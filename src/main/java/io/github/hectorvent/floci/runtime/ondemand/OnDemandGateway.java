package io.github.hectorvent.floci.runtime.ondemand;

import io.github.hectorvent.floci.core.common.SsrfProtection;
import io.github.hectorvent.floci.runtime.ondemand.ActivationCoordinator.RequestLease;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.BackendProtocol;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.HealthResponse;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.SocketAddress;
import org.jboss.logging.Logger;

import java.net.InetAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Application reverse proxy on its own listener; never participates in AWS API routing. */
final class OnDemandGateway implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(OnDemandGateway.class);
    private static final Set<String> HOP_HEADERS = Set.of("connection", "keep-alive", "proxy-authenticate",
            "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade");

    private final Vertx vertx;
    private final Map<String, WorkloadDefinition> routes = new HashMap<>();
    private final Supplier<ActivationCoordinator> coordinator;
    private final HttpClient client;
    private final HttpClient http2Client;
    private final Set<Exchange> exchanges = new HashSet<>();
    private HttpServer server;

    OnDemandGateway(Vertx vertx, List<WorkloadDefinition> definitions, Supplier<ActivationCoordinator> coordinator) {
        this.vertx = vertx;
        this.coordinator = coordinator;
        definitions.stream().filter(definition -> definition.host() != null)
                .forEach(definition -> routes.put(definition.host(), definition));
        client = vertx.createHttpClient(new HttpClientOptions().setConnectTimeout(5000).setMaxPoolSize(100));
        http2Client = vertx.createHttpClient(new HttpClientOptions().setConnectTimeout(5000)
                .setProtocolVersion(HttpVersion.HTTP_2).setHttp2ClearTextUpgrade(false)
                .setUseAlpn(true).setAlpnVersions(List.of(HttpVersion.HTTP_2)).setHttp2MaxPoolSize(100));
    }

    Future<HttpServer> start(String host, int port) {
        server = vertx.createHttpServer(new HttpServerOptions().setHandle100ContinueAutomatically(true)
                .setHttp2ClearTextEnabled(true));
        return server.requestHandler(this::handle).listen(port, host);
    }

    int port() {
        return server.actualPort();
    }

    private void handle(HttpServerRequest request) {
        request.pause();
        String host;
        try {
            host = URI.create("http://" + request.host()).getHost();
        } catch (IllegalArgumentException e) {
            reject(request, 400, "Invalid application host");
            return;
        }
        WorkloadDefinition definition = host == null ? null : routes.get(host.toLowerCase(Locale.ROOT));
        if (definition == null) {
            reject(request, 404, "Unknown application host");
            return;
        }
        ActivationCoordinator current = coordinator.get();
        if (current == null || !definition.enabled()) {
            reject(request, 503, "Application runtime unavailable");
            return;
        }
        boolean health = definition.isHealthRequest(request.method().name(), request.path());
        RequestLease lease;
        try {
            if (health) {
                Optional<RequestLease> probe = current.acquireHealth(definition.id());
                if (probe.isEmpty()) {
                    if (current.canReportSleepingHealth(definition.id())) {
                        HealthResponse response = definition.sleepingHealth();
                        request.response().setStatusCode(response.status()).putHeader("Content-Type", response.contentType())
                                .putHeader("Cache-Control", "no-store")
                                .end(request.method() == HttpMethod.HEAD ? "" : response.body());
                        drain(request);
                    } else {
                        reject(request, 503, "Application is not ready");
                    }
                    return;
                }
                lease = probe.get();
            } else {
                lease = current.acquire(definition.id());
            }
        } catch (IllegalStateException e) {
            reject(request, 503, e instanceof ActivationCoordinator.CapacityException
                    ? "Application startup queue is full" : "Application runtime unavailable");
            return;
        }
        Exchange exchange = new Exchange(request, lease, definition.backendProtocol());
        synchronized (exchanges) {
            exchanges.add(exchange);
        }
        request.exceptionHandler(ignored -> exchange.cancel());
        request.response().closeHandler(ignored -> {
            if (!exchange.upgraded) {
                exchange.cancel();
            }
        });
        request.response().endHandler(ignored -> {
            if (!exchange.upgraded) {
                exchange.finish();
            }
        });
        exchange.timer = vertx.setTimer(definition.startupTimeout().toMillis(),
                ignored -> exchange.fail(504, "Application startup timed out"));
        Context context = Vertx.currentContext();
        lease.ready().whenComplete((backend, failure) -> context.runOnContext(ignored -> {
            if (exchange.done.get()) {
                return;
            }
            if (failure != null) {
                exchange.fail(503, "Application could not become ready");
                return;
            }
            vertx.<String>executeBlocking(() -> {
                InetAddress[] addresses = SsrfProtection.rejectMetadataAddresses(
                        InetAddress.getAllByName(backend.getHost()), backend.getHost());
                int backendPort = effectivePort(backend);
                if (backendPort == port() && addresses[0].isLoopbackAddress()) {
                    throw new IllegalArgumentException("Application backend points to its gateway");
                }
                return addresses[0].getHostAddress();
            }).onSuccess(address -> proxy(exchange, backend, address))
                    .onFailure(error -> exchange.fail(502, "Application backend is unreachable"));
        }));
    }

    private void proxy(Exchange exchange, URI backend, String address) {
        if (exchange.done.get()) {
            return;
        }
        vertx.cancelTimer(exchange.timer);
        HttpServerRequest request = exchange.request;
        boolean upgrade = "websocket".equalsIgnoreCase(request.getHeader("Upgrade"))
                && request.method() == HttpMethod.GET;
        RequestOptions options = new RequestOptions().setHost(backend.getHost()).setPort(effectivePort(backend))
                .setServer(SocketAddress.inetSocketAddress(effectivePort(backend), address))
                .setSsl("https".equals(backend.getScheme())).setMethod(request.method()).setURI(request.uri());
        HttpClient backendClient = exchange.protocol == BackendProtocol.HTTP2 && !upgrade ? http2Client : client;
        backendClient.request(options).onSuccess(upstream -> {
            exchange.upstream = upstream;
            if (exchange.done.get()) {
                upstream.reset();
                return;
            }
            if (exchange.protocol == BackendProtocol.HTTP2 && !upgrade && upstream.version() != HttpVersion.HTTP_2) {
                upstream.reset();
                exchange.fail(502, "Application backend did not negotiate HTTP/2");
                return;
            }
            copyHeaders(request.headers(), upstream.headers());
            upstream.authority(request.authority());
            if (upstream.version() == HttpVersion.HTTP_2 && "trailers".equalsIgnoreCase(request.getHeader("TE"))) {
                upstream.putHeader("TE", "trailers");
            }
            upstream.exceptionHandler(error -> exchange.fail(502, "Application request failed", error));
            if (upgrade) {
                upstream.putHeader("Connection", "Upgrade").putHeader("Upgrade", "websocket");
                upstream.connect().onSuccess(response -> relayResponse(exchange, response, true))
                        .onFailure(error -> exchange.fail(502, "Application upgrade failed", error));
            } else {
                if (request.getHeader("Content-Length") == null) {
                    upstream.setChunked(true);
                }
                upstream.response().onSuccess(response -> relayResponse(exchange, response, false))
                        .onFailure(error -> exchange.fail(502, "Application request failed", error));
                upstream.sendHead().onSuccess(ignored -> request.pipe().endOnFailure(false).to(upstream)
                        .onFailure(error -> exchange.fail(502, "Application upload failed", error)))
                        .onFailure(error -> exchange.fail(502, "Application request failed", error));
            }
        }).onFailure(error -> exchange.fail(502, "Application connection failed", error));
    }

    private void relayResponse(Exchange exchange, HttpClientResponse response, boolean upgrade) {
        if (exchange.done.get()) {
            response.request().reset();
            return;
        }
        HttpServerRequest request = exchange.request;
        request.response().setStatusCode(response.statusCode());
        copyHeaders(response.headers(), request.response().headers());
        if (upgrade && response.statusCode() == 101) {
            exchange.upgraded = true;
            request.response().putHeader("Connection", "Upgrade").putHeader("Upgrade", "websocket");
            exchange.targetSocket = response.netSocket();
            exchange.targetSocket.pause();
            request.toNetSocket().onSuccess(socket -> {
                exchange.clientSocket = socket;
                socket.closeHandler(ignored -> exchange.cancel());
                exchange.targetSocket.closeHandler(ignored -> exchange.cancel());
                socket.exceptionHandler(ignored -> exchange.cancel());
                exchange.targetSocket.exceptionHandler(ignored -> exchange.cancel());
                socket.pipeTo(exchange.targetSocket).onComplete(ignored -> exchange.cancel());
                exchange.targetSocket.pipeTo(socket).onComplete(ignored -> exchange.cancel());
            }).onFailure(error -> exchange.cancel());
            return;
        }
        if (response.getHeader("Content-Length") == null && request.method() != HttpMethod.HEAD
                && response.statusCode() != 204 && response.statusCode() != 304) {
            request.response().setChunked(true);
        }
        response.pipe().endOnComplete(false).to(request.response()).onSuccess(ignored -> {
            copyHeaders(response.trailers(), request.response().trailers());
            request.response().end().onFailure(error -> exchange.fail(502, "Application response failed", error));
        }).onFailure(error -> exchange.fail(502, "Application response failed", error));
    }

    private static void copyHeaders(MultiMap source, MultiMap target) {
        Set<String> excluded = new HashSet<>(HOP_HEADERS);
        for (String connection : source.getAll("Connection")) {
            for (String token : connection.split(",")) {
                excluded.add(token.strip().toLowerCase(Locale.ROOT));
            }
        }
        for (Map.Entry<String, String> header : source) {
            if (!header.getKey().startsWith(":") && !excluded.contains(header.getKey().toLowerCase(Locale.ROOT))) {
                target.add(header.getKey(), header.getValue());
            }
        }
    }

    private static int effectivePort(URI backend) {
        return backend.getPort() >= 0 ? backend.getPort() : "https".equals(backend.getScheme()) ? 443 : 80;
    }

    private static void reject(HttpServerRequest request, int status, String message) {
        String contentType = request.getHeader("Content-Type");
        if (request.version() == HttpVersion.HTTP_2 && contentType != null
                && (contentType.equals("application/grpc") || contentType.startsWith("application/grpc+"))) {
            request.response().setStatusCode(200).putHeader("Content-Type", "application/grpc")
                    .putHeader("grpc-status", status == 504 ? "4" : "14")
                    .putHeader("grpc-message", message).putHeader("Cache-Control", "no-store").end();
        } else {
            request.response().setStatusCode(status).putHeader("Cache-Control", "no-store").end(message);
        }
        drain(request);
    }

    private static void drain(HttpServerRequest request) {
        if (!request.isEnded()) {
            request.resume();
        }
    }

    @Override
    public void close() {
        List<Exchange> current;
        synchronized (exchanges) {
            current = List.copyOf(exchanges);
        }
        for (Exchange exchange : current) {
            exchange.context.runOnContext(ignored -> exchange.cancel());
        }
        if (server != null) {
            server.close();
        }
        client.close();
        http2Client.close();
    }

    private final class Exchange {
        private final HttpServerRequest request;
        private final RequestLease lease;
        private final BackendProtocol protocol;
        private final Context context = Vertx.currentContext();
        private final AtomicBoolean done = new AtomicBoolean();
        private long timer;
        private boolean upgraded;
        private HttpClientRequest upstream;
        private NetSocket clientSocket;
        private NetSocket targetSocket;

        private Exchange(HttpServerRequest request, RequestLease lease, BackendProtocol protocol) {
            this.request = request;
            this.lease = lease;
            this.protocol = protocol;
        }

        private void finish() {
            if (done.compareAndSet(false, true)) {
                vertx.cancelTimer(timer);
                lease.close();
                synchronized (exchanges) {
                    exchanges.remove(this);
                }
            }
        }

        private void cancel() {
            finish();
            if (upstream != null) {
                upstream.reset();
            }
            if (clientSocket != null) {
                clientSocket.close();
            }
            if (targetSocket != null) {
                targetSocket.close();
            }
        }

        private void fail(int status, String message) {
            if (done.get()) {
                return;
            }
            LOG.debugv("On-demand proxy failure: {0}", message);
            if (request.response().headWritten()) {
                if (request.version() == HttpVersion.HTTP_2) {
                    request.response().reset(2);
                } else {
                    request.response().close();
                }
            } else {
                reject(request, status, message);
            }
            drain(request);
            cancel();
        }

        private void fail(int status, String message, Throwable cause) {
            LOG.debugv(cause, "On-demand proxy failure for {0} {1}: {2}",
                    request.method(), request.uri(), message);
            fail(status, message);
        }
    }
}
