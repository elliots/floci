package io.github.hectorvent.floci.runtime.network;

import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/** Network isolation policy and origin routes. */
public record NetworkDefinition(boolean isolated, List<Route> routes) {
    public static final NetworkDefinition DISABLED = new NetworkDefinition(false, List.of());

    public NetworkDefinition {
        routes = List.copyOf(routes);
    }

    /**
     * One backend and the origins it serves. A wildcard origin (https://*.example.test) is kept in {@code wildcards} as the
     * domain it covers (https://example.test:443): it takes any name below that domain, at any depth, on its scheme and port.
     */
    public record Route(String name, List<URI> origins, List<URI> wildcards, URI backendUrl, boolean preserveHost) {
        public Route {
            origins = List.copyOf(origins);
            wildcards = List.copyOf(wildcards);
        }

        public Route(String name, List<URI> origins, URI backendUrl, boolean preserveHost) {
            this(name, origins, List.of(), backendUrl, preserveHost);
        }

        /** The wildcard domain of this route that covers origin, if any: the longest, when several do. */
        Optional<URI> covering(URI origin) {
            return wildcards.stream().filter(w -> covers(w, origin))
                    .max(Comparator.comparingInt(w -> w.getHost().length()));
        }
    }

    /** Whether a wildcard domain covers origin: the same scheme and port, and a name below the domain. */
    static boolean covers(URI wildcard, URI origin) {
        return wildcard.getScheme().equals(origin.getScheme()) && wildcard.getPort() == origin.getPort()
                && below(origin.getHost(), wildcard.getHost());
    }

    private static boolean below(String host, String domain) {
        return host.toLowerCase(Locale.ROOT).endsWith("." + domain);
    }

    /** The route for an origin: the one that names it, else the one whose wildcard covers it most closely. */
    public Optional<Route> routeFor(URI origin) {
        Optional<Route> exact = routes.stream().filter(route -> route.origins().contains(origin)).findFirst();
        if (exact.isPresent()) {
            return exact;
        }
        return routes.stream().filter(route -> route.covering(origin).isPresent())
                .max(Comparator.comparingInt(route -> route.covering(origin).orElseThrow().getHost().length()));
    }

    public boolean owns(String hostname) {
        return routes.stream().anyMatch(route -> route.origins().stream().anyMatch(o -> o.getHost().equalsIgnoreCase(hostname))
                || route.wildcards().stream().anyMatch(w -> below(hostname, w.getHost())));
    }

    /** Whether a name is served only through a wildcard origin, so the gateway issues it a certificate of its own. */
    public boolean coveredByWildcard(String hostname) {
        return routes.stream().noneMatch(route -> route.origins().stream().anyMatch(o -> o.getHost().equalsIgnoreCase(hostname)))
                && routes.stream().anyMatch(route -> route.wildcards().stream().anyMatch(w -> below(hostname, w.getHost())));
    }

    public static int port(URI origin) {
        return origin.getPort() >= 0 ? origin.getPort() : "https".equals(origin.getScheme()) ? 443 : 80;
    }
}
