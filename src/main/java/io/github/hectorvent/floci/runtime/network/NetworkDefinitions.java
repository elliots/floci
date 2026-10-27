package io.github.hectorvent.floci.runtime.network;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * The runtime file's {@code network}: whether the nodes' pods are isolated, Floci's extra ports,
 * how containers trust Floci's CA, the outside addresses pods may reach, and the third-party
 * origins routed to simulators.
 */
public record NetworkDefinitions(boolean isolation, List<Integer> extraPorts, TrustMode trustMode,
                                 List<Allow> allow, List<Route> routes) {

    public static final NetworkDefinitions NONE =
            new NetworkDefinitions(false, List.of(), TrustMode.SYSTEM, List.of(), List.of());

    public NetworkDefinitions {
        extraPorts = List.copyOf(extraPorts);
        Objects.requireNonNull(trustMode, "trustMode");
        allow = List.copyOf(allow);
        routes = List.copyOf(routes);
    }

    public enum TrustMode {
        /** Floci's CA in the image's bundles and Java stores, and in the variables clients read. */
        SYSTEM,
        /** Only Floci's CA files and the variables. */
        ENV,
        /** Nothing. */
        OFF;

        public String wire() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public enum Protocol {
        TCP, UDP, ANY;

        public String wire() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /** An outside network that pods may reach: every port when {@code ports} is empty. */
    public record Allow(String cidr, Protocol protocol, List<Integer> ports) {

        public Allow {
            Objects.requireNonNull(cidr, "cidr");
            Objects.requireNonNull(protocol, "protocol");
            ports = List.copyOf(ports);
        }
    }

    /**
     * A third-party origin, such as {@code https://api.vendor.test:443}, or a wildcard for every
     * name below a domain, such as {@code https://*.vendor.test:443}.
     */
    public record Origin(boolean tls, String host, boolean wildcard, int port) {

        /** {@code SCHEME://HOST:PORT}, with {@code *.} before a wildcard's domain. */
        public String wire() {
            return (tls ? "https" : "http") + "://" + (wildcard ? "*." : "") + host + ":" + port;
        }
    }

    public enum BackendProtocol {
        HTTP1, H2C, AUTO;

        public String wire() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * Origins served by a simulator: {@code backendHost} is a Service name in the cluster, an
     * address in the cluster's Service range, or {@code host.floci.internal} for the Floci host.
     */
    public record Route(String name, List<Origin> origins, String backendHost, int backendPort,
                        BackendProtocol protocol) {

        public Route {
            Objects.requireNonNull(name, "name");
            origins = List.copyOf(origins);
            Objects.requireNonNull(backendHost, "backendHost");
            Objects.requireNonNull(protocol, "protocol");
        }
    }

    /** Every routed host name, exact or a wildcard's domain, in the order the file lists them. */
    public List<Origin> origins() {
        return routes.stream().flatMap(route -> route.origins().stream()).toList();
    }
}
