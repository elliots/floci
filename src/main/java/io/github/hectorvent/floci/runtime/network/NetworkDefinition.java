package io.github.hectorvent.floci.runtime.network;

import io.github.hectorvent.floci.core.common.CidrCanonicalizer;

import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/** Network isolation policy and origin routes. */
public record NetworkDefinition(boolean isolated, List<Route> routes, List<Egress> allowedEgress) {
    public static final NetworkDefinition DISABLED = new NetworkDefinition(false, List.of());

    public NetworkDefinition {
        routes = List.copyOf(routes);
        allowedEgress = List.copyOf(allowedEgress);
    }

    public NetworkDefinition(boolean isolated, List<Route> routes) {
        this(isolated, routes, List.of());
    }

    /** An explicit address and transport exception, without authorizing public DNS resolution. */
    public record Egress(String destination, String protocol, String ports) {
        public Egress {
            if (destination == null || !destination.matches("[0-9]{1,3}(\\.[0-9]{1,3}){3}/[0-9]{1,2}")
                    || !destination.equals(CidrCanonicalizer.canonicalize(destination).orElse(""))
                    || destination.endsWith("/0")) {
                throw new IllegalArgumentException("Egress destination must be a canonical IPv4 CIDR with a nonzero prefix");
            }
            String address = destination.substring(0, destination.indexOf('/'));
            for (String excluded : List.of("0.0.0.0/8", "127.0.0.0/8", "169.254.0.0/16", "224.0.0.0/4", "240.0.0.0/4")) {
                if (CidrCanonicalizer.contains(excluded, address)
                        || CidrCanonicalizer.contains(destination, excluded.substring(0, excluded.indexOf('/')))) {
                    throw new IllegalArgumentException("Egress destination overlaps an unspecified, loopback, link-local or reserved range");
                }
            }
            if (!"tcp".equals(protocol) && !"udp".equals(protocol)) {
                throw new IllegalArgumentException("Egress protocol must be tcp or udp");
            }
            if (ports == null || !ports.matches("[1-9][0-9]{0,4}(-[1-9][0-9]{0,4})?")) {
                throw new IllegalArgumentException("Egress ports must be a port or ordered port range");
            }
            String[] range = ports.split("-", 2);
            int first = Integer.parseInt(range[0]);
            int last = Integer.parseInt(range[range.length - 1]);
            if (first > last || last > 65535) {
                throw new IllegalArgumentException("Egress ports must be between 1 and 65535 in ascending order");
            }
            ports = first == last ? Integer.toString(first) : first + "-" + last;
        }
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
