package io.github.hectorvent.floci.runtime.ondemand;

import io.github.hectorvent.floci.core.common.SsrfProtection;
import io.github.hectorvent.floci.core.common.http.HttpReverseProxy;
import io.github.hectorvent.floci.runtime.ondemand.ActivationCoordinator.RequestLease;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.BackendProtocol;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.HealthResponse;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpVersion;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static io.github.hectorvent.floci.core.common.http.HttpReverseProxy.reject;

/** Hostname routing and activation on the on-demand application listener. */
final class OnDemandGateway implements AutoCloseable {
    private final Vertx vertx;
    private final Map<String, WorkloadDefinition> routes = new HashMap<>();
    private final Supplier<ActivationCoordinator> coordinator;
    private final HttpReverseProxy proxy;
    private HttpServer server;

    OnDemandGateway(Vertx vertx, List<WorkloadDefinition> definitions, Supplier<ActivationCoordinator> coordinator) {
        this.vertx = vertx;
        this.coordinator = coordinator;
        definitions.stream().filter(definition -> definition.host() != null)
                .forEach(definition -> routes.put(definition.host(), definition));
        proxy = new HttpReverseProxy(vertx, null, this::resolveBackend);
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
        handleRoute(request, definition);
    }

    private void handleRoute(HttpServerRequest request, WorkloadDefinition definition) {
        request.pause();
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
        proxy.forward(request, lease.ready(), definition.backendProtocol() == BackendProtocol.HTTP2
                ? HttpVersion.HTTP_2 : HttpVersion.HTTP_1_1, definition.startupTimeout(), lease::close, true);
    }

    private String resolveBackend(URI backend) {
        try {
            InetAddress[] addresses = SsrfProtection.rejectMetadataAddresses(
                    InetAddress.getAllByName(backend.getHost()), backend.getHost());
            int backendPort = backend.getPort() >= 0 ? backend.getPort() : "https".equals(backend.getScheme()) ? 443 : 80;
            if (backendPort == port() && addresses[0].isLoopbackAddress()) {
                throw new IllegalArgumentException("Application backend points to its gateway");
            }
            return addresses[0].getHostAddress();
        } catch (IOException e) {
            throw new IllegalArgumentException("Application backend is unreachable", e);
        }
    }

    private static void drain(HttpServerRequest request) {
        if (!request.isEnded()) {
            request.resume();
        }
    }

    @Override
    public void close() {
        proxy.close();
        if (server != null) {
            server.close();
        }
    }
}
