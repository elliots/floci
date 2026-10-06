package io.github.hectorvent.floci.runtime.network;

import java.net.URI;
import java.util.List;

/** Network isolation policy and origin routes. */
public record NetworkDefinition(boolean isolated, List<Route> routes) {
    public static final NetworkDefinition DISABLED = new NetworkDefinition(false, List.of());

    public NetworkDefinition {
        routes = List.copyOf(routes);
    }

    public record Route(String name, List<URI> origins, URI backendUrl, boolean preserveHost) {
        public Route {
            origins = List.copyOf(origins);
        }
    }

    public boolean owns(String hostname) {
        return routes.stream().flatMap(route -> route.origins().stream())
                .anyMatch(origin -> origin.getHost().equalsIgnoreCase(hostname));
    }

    public static int port(URI origin) {
        return origin.getPort() >= 0 ? origin.getPort() : "https".equals(origin.getScheme()) ? 443 : 80;
    }
}
