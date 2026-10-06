package io.github.hectorvent.floci.core.common.http;

import io.vertx.core.Context;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.net.HostAndPort;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.core.net.SocketAddress;
import org.jboss.logging.Logger;

import java.net.URI;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/** Streams HTTP and WebSocket exchanges between clients and backends. */
public final class HttpReverseProxy implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(HttpReverseProxy.class);
    private static final Set<String> HOP_HEADERS = Set.of("connection", "keep-alive", "proxy-authenticate",
            "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade");

    private final Vertx vertx;
    private final HttpClient client;
    private final HttpClient http2Client;
    private final Set<Exchange> exchanges = new HashSet<>();
    private final Function<URI, String> backendResolver;

    public HttpReverseProxy(Vertx vertx, PemTrustOptions trust, Function<URI, String> backendResolver) {
        this.vertx = vertx;
        this.backendResolver = backendResolver;
        HttpClientOptions httpOptions = new HttpClientOptions().setConnectTimeout(5000).setMaxPoolSize(100);
        if (trust != null) {
            httpOptions.setTrustOptions(trust);
        }
        client = vertx.createHttpClient(httpOptions);
        http2Client = vertx.createHttpClient(new HttpClientOptions(httpOptions)
                .setProtocolVersion(HttpVersion.HTTP_2).setHttp2ClearTextUpgrade(false)
                .setUseAlpn(true).setAlpnVersions(List.of(HttpVersion.HTTP_2)).setHttp2MaxPoolSize(100));
    }

    public void forward(HttpServerRequest request, URI backend, boolean preserveHost) {
        forward(request, CompletableFuture.completedFuture(backend), HttpVersion.HTTP_1_1,
                Duration.ofSeconds(30), () -> {}, preserveHost);
    }

    public void forward(HttpServerRequest request, CompletableFuture<URI> ready, HttpVersion protocol,
                        Duration timeout, Runnable onComplete, boolean preserveHost) {
        request.pause();
        Exchange exchange = new Exchange(request, onComplete, protocol, preserveHost);
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
        exchange.timer = vertx.setTimer(timeout.toMillis(),
                ignored -> exchange.fail(504, "Application startup timed out"));
        Context context = Vertx.currentContext();
        ready.whenComplete((backend, failure) -> context.runOnContext(ignored -> {
            if (exchange.done.get()) {
                return;
            }
            if (failure != null) {
                exchange.fail(503, "Application could not become ready");
                return;
            }
            vertx.<String>executeBlocking(() -> backendResolver.apply(backend)).onSuccess(address -> proxy(exchange, backend, address))
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
        HttpClient backendClient = exchange.protocol == HttpVersion.HTTP_2 && !upgrade ? http2Client : client;
        backendClient.request(options).onSuccess(upstream -> {
            exchange.upstream = upstream;
            if (exchange.done.get()) {
                upstream.reset();
                return;
            }
            if (exchange.protocol == HttpVersion.HTTP_2 && !upgrade && upstream.version() != HttpVersion.HTTP_2) {
                upstream.reset();
                exchange.fail(502, "Application backend did not negotiate HTTP/2");
                return;
            }
            copyHeaders(request.headers(), upstream.headers());
            if (exchange.preserveHost) {
                upstream.authority(request.authority());
            } else {
                upstream.headers().remove("Host");
                upstream.authority(HostAndPort.create(backend.getHost(), effectivePort(backend)));
                upstream.putHeader("X-Forwarded-Host", request.host());
            }
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

    public static void reject(HttpServerRequest request, int status, String message) {
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
        client.close();
        http2Client.close();
    }

    private final class Exchange {
        private final HttpServerRequest request;
        private final Runnable onComplete;
        private final boolean preserveHost;
        private final HttpVersion protocol;
        private final Context context = Vertx.currentContext();
        private final AtomicBoolean done = new AtomicBoolean();
        private long timer;
        private boolean upgraded;
        private HttpClientRequest upstream;
        private NetSocket clientSocket;
        private NetSocket targetSocket;

        private Exchange(HttpServerRequest request, Runnable onComplete, HttpVersion protocol, boolean preserveHost) {
            this.request = request;
            this.onComplete = onComplete;
            this.preserveHost = preserveHost;
            this.protocol = protocol;
        }

        private void finish() {
            if (done.compareAndSet(false, true)) {
                vertx.cancelTimer(timer);
                onComplete.run();
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
            LOG.debugv("HTTP proxy failure: {0}", message);
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
            LOG.debugv(cause, "HTTP proxy failure for {0} {1}: {2}",
                    request.method(), request.uri(), message);
            fail(status, message);
        }
    }
}
