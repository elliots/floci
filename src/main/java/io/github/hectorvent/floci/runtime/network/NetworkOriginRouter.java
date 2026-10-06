package io.github.hectorvent.floci.runtime.network;

import io.github.hectorvent.floci.core.common.http.HttpReverseProxy;
import io.vertx.core.http.HttpServerRequest;
import org.jboss.logging.Logger;

import java.net.URI;
import java.util.Locale;
import java.util.Optional;

/** Matches the original authority before delegating streaming to the shared HTTP proxy. */
public final class NetworkOriginRouter {
    private static final Logger LOG = Logger.getLogger(NetworkOriginRouter.class);
    private final NetworkDefinition network;
    private final HttpReverseProxy proxy;

    public NetworkOriginRouter(NetworkDefinition network, HttpReverseProxy proxy) {
        this.network = network;
        this.proxy = proxy;
    }

    public void route(HttpServerRequest request, String scheme, int port) {
        URI origin;
        try {
            URI authority = URI.create(scheme + "://" + request.host());
            if (authority.getHost() == null || authority.getUserInfo() != null
                    || !authority.getPath().isEmpty() || authority.getQuery() != null || authority.getFragment() != null
                    || NetworkDefinition.port(authority) != port) {
                throw new IllegalArgumentException("Invalid authority");
            }
            origin = URI.create(scheme + "://" + authority.getHost().toLowerCase(Locale.ROOT) + ":" + port);
        } catch (IllegalArgumentException e) {
            deny(request, "invalid-authority");
            return;
        }
        Optional<NetworkDefinition.Route> match = network.routes().stream()
                .filter(route -> route.origins().contains(origin)).findFirst();
        if (match.isEmpty()) {
            deny(request, origin.toString());
            return;
        }
        NetworkDefinition.Route route = match.orElseThrow();
        LOG.infov("Network connection source={0} origin={1} route={2} outcome=routed", request.remoteAddress(), origin, route.name());
        proxy.forward(request, route.backendUrl(), route.preserveHost());
    }

    private static void deny(HttpServerRequest request, String origin) {
        LOG.infov("Network connection source={0} origin={1} outcome=denied", request.remoteAddress(), origin);
        request.response().setStatusCode(502).putHeader("Cache-Control", "no-store").end("Unmapped network origin");
        request.resume();
    }

}
