package io.github.hectorvent.floci.runtime.network;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.dns.EmbeddedDnsServer;
import io.github.hectorvent.floci.runtime.network.NetworkDefinitions.Allow;
import io.github.hectorvent.floci.runtime.network.NetworkDefinitions.BackendProtocol;
import io.github.hectorvent.floci.runtime.network.NetworkDefinitions.Origin;
import io.github.hectorvent.floci.runtime.network.NetworkDefinitions.Protocol;
import io.github.hectorvent.floci.runtime.network.NetworkDefinitions.Route;
import io.github.hectorvent.floci.runtime.network.NetworkDefinitions.TrustMode;
import io.github.hectorvent.floci.runtime.node.RuntimeFileException;
import io.github.hectorvent.floci.runtime.node.RuntimeFileFields;
import io.github.hectorvent.floci.runtime.node.RuntimeFileSection;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The runtime file's {@code network}: isolation of the nodes' pods, routes from third-party
 * origins to simulators, the allow list, Floci's extra ports and the trust mode.
 *
 * <pre>
 * network:
 *   isolation:
 *     enabled: true
 *   routes:
 *     payments:
 *       origins: [https://api.vendor.test, https://*.vendor.test]
 *       target:
 *         backend-url: http://payments-simulator.simulators.svc.cluster.local:8080
 * </pre>
 */
@ApplicationScoped
public class NetworkSection implements RuntimeFileSection<NetworkDefinitions> {

    public static final String NAME = "network";

    /** The host of a backend on the Floci host. */
    public static final String FLOCI_HOST = "host.floci.internal";

    private static final List<String> FIELDS = List.of("isolation", "floci", "trust", "egress", "routes");
    private static final List<String> ROUTE_FIELDS = List.of("origins", "target");
    private static final List<String> TARGET_FIELDS = List.of("backend-url", "protocol");
    private static final List<String> ALLOW_FIELDS = List.of("cidr", "protocol", "ports");
    private static final Pattern ROUTE_NAME = Pattern.compile("[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?");
    private static final Pattern DNS_NAME =
            Pattern.compile("([a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?\\.)*[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?");
    private static final Pattern IPV4 = Pattern.compile("(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})");
    private static final String SERVICE_SUFFIX = ".svc.cluster.local";

    private final FlociPortInventory ports;
    private final List<String> flociSuffixes;

    @Inject
    public NetworkSection(FlociPortInventory ports, EmulatorConfig config) {
        this(ports, flociSuffixes(config.hostname(), config.dns().extraSuffixes()));
    }

    NetworkSection(FlociPortInventory ports, List<String> flociSuffixes) {
        this.ports = ports;
        this.flociSuffixes = List.copyOf(flociSuffixes);
    }

    private static List<String> flociSuffixes(Optional<String> hostname, Optional<List<String>> extra) {
        List<String> suffixes = new ArrayList<>(EmbeddedDnsServer.BUILTIN_SUFFIXES);
        suffixes.add("host.docker.internal");
        suffixes.add("cluster.local");
        hostname.ifPresent(name -> suffixes.add(name.toLowerCase(Locale.ROOT)));
        extra.ifPresent(list -> list.forEach(name -> suffixes.add(name.toLowerCase(Locale.ROOT))));
        return suffixes;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public NetworkDefinitions empty() {
        return NetworkDefinitions.NONE;
    }

    @Override
    public NetworkDefinitions read(JsonNode node, Path file) {
        RuntimeFileFields.mapping(node, NAME, FIELDS);
        boolean isolation = isolation(node);
        List<Integer> extraPorts = extraPorts(node);
        TrustMode trust = trust(node);
        List<Allow> allow = allow(node);
        List<Route> routes = routes(node, extraPorts);
        return new NetworkDefinitions(isolation, extraPorts, trust, allow, routes);
    }

    /** Whether {@code host} is one of Floci's names, or below one. */
    boolean isFlociName(String host) {
        return flociSuffixes.stream().anyMatch(suffix -> host.equals(suffix) || host.endsWith("." + suffix));
    }

    private static boolean isolation(JsonNode node) {
        JsonNode isolation = node.get("isolation");
        if (isolation == null) {
            return false;
        }
        String path = RuntimeFileFields.path(NAME, "isolation");
        RuntimeFileFields.mapping(isolation, path, List.of("enabled"));
        JsonNode enabled = isolation.get("enabled");
        if (enabled == null) {
            return false;
        }
        if (!enabled.isBoolean()) {
            throw new RuntimeFileException(RuntimeFileFields.path(path, "enabled"), "must be true or false");
        }
        return enabled.booleanValue();
    }

    private List<Integer> extraPorts(JsonNode node) {
        JsonNode floci = node.get("floci");
        if (floci == null) {
            return List.of();
        }
        String path = RuntimeFileFields.path(NAME, "floci");
        RuntimeFileFields.mapping(floci, path, List.of("extra-ports"));
        return portList(floci.get("extra-ports"), RuntimeFileFields.path(path, "extra-ports"));
    }

    private static TrustMode trust(JsonNode node) {
        JsonNode trust = node.get("trust");
        if (trust == null) {
            return TrustMode.SYSTEM;
        }
        String path = RuntimeFileFields.path(NAME, "trust");
        RuntimeFileFields.mapping(trust, path, List.of("mode"));
        String mode = RuntimeFileFields.optionalText(trust, path, "mode");
        if (mode == null) {
            return TrustMode.SYSTEM;
        }
        return switch (mode) {
            case "system" -> TrustMode.SYSTEM;
            case "env" -> TrustMode.ENV;
            case "off" -> TrustMode.OFF;
            default -> throw new RuntimeFileException(RuntimeFileFields.path(path, "mode"), "must be system, env or off");
        };
    }

    private static List<Allow> allow(JsonNode node) {
        JsonNode egress = node.get("egress");
        if (egress == null) {
            return List.of();
        }
        String path = RuntimeFileFields.path(NAME, "egress");
        RuntimeFileFields.mapping(egress, path, List.of("allow"));
        JsonNode list = egress.get("allow");
        if (list == null) {
            return List.of();
        }
        String listPath = RuntimeFileFields.path(path, "allow");
        if (!list.isArray()) {
            throw new RuntimeFileException(listPath, "must be a list");
        }
        List<Allow> allow = new ArrayList<>();
        for (JsonNode entry : list) {
            String entryPath = RuntimeFileFields.path(listPath, String.valueOf(allow.size()));
            RuntimeFileFields.mapping(entry, entryPath, ALLOW_FIELDS);
            String cidr = cidr(RuntimeFileFields.text(entry, entryPath, "cidr"), RuntimeFileFields.path(entryPath, "cidr"));
            String protocolText = RuntimeFileFields.optionalText(entry, entryPath, "protocol");
            Protocol protocol = switch (protocolText == null ? "tcp" : protocolText) {
                case "tcp" -> Protocol.TCP;
                case "udp" -> Protocol.UDP;
                case "any" -> Protocol.ANY;
                default -> throw new RuntimeFileException(RuntimeFileFields.path(entryPath, "protocol"),
                        "must be tcp, udp or any");
            };
            List<Integer> entryPorts = portList(entry.get("ports"), RuntimeFileFields.path(entryPath, "ports"));
            allow.add(new Allow(cidr, protocol, entryPorts));
        }
        return allow;
    }

    private List<Route> routes(JsonNode node, List<Integer> extraPorts) {
        JsonNode routes = node.get("routes");
        if (routes == null) {
            return List.of();
        }
        String path = RuntimeFileFields.path(NAME, "routes");
        RuntimeFileFields.mapping(routes, path);
        List<FlociPortInventory.PortRange> flociPorts = ports.ports(extraPorts);
        Map<String, String> seen = new HashMap<>();
        Map<Integer, Boolean> schemeOfPort = new HashMap<>();
        List<Route> out = new ArrayList<>();
        for (Map.Entry<String, JsonNode> entry : routes.properties()) {
            String name = entry.getKey();
            String routePath = RuntimeFileFields.path(path, name);
            if (!ROUTE_NAME.matcher(name).matches()) {
                throw new RuntimeFileException(routePath, "a route name is 1 to 63 of a-z, 0-9 and -");
            }
            JsonNode route = entry.getValue();
            RuntimeFileFields.mapping(route, routePath, ROUTE_FIELDS);
            JsonNode originList = route.get("origins");
            String originsPath = RuntimeFileFields.path(routePath, "origins");
            if (originList == null || !originList.isArray() || originList.isEmpty()) {
                throw new RuntimeFileException(originsPath, "must be a list of at least one origin");
            }
            List<Origin> origins = new ArrayList<>();
            for (JsonNode originNode : originList) {
                String originPath = RuntimeFileFields.path(originsPath, String.valueOf(origins.size()));
                if (!originNode.isTextual()) {
                    throw new RuntimeFileException(originPath, "must be an origin such as https://api.vendor.test");
                }
                Origin origin = origin(originNode.textValue(), originPath);
                for (FlociPortInventory.PortRange range : flociPorts) {
                    if (range.contains(origin.port())) {
                        throw new RuntimeFileException(routePath, "origin port " + origin.port()
                                + " is a Floci port (" + range.key() + ")");
                    }
                }
                String owner = seen.putIfAbsent(origin.wire(), name);
                if (owner != null) {
                    throw new RuntimeFileException(originPath, origin.wire() + " is routed by "
                            + RuntimeFileFields.path(path, owner) + " too");
                }
                Boolean tls = schemeOfPort.putIfAbsent(origin.port(), origin.tls());
                if (tls != null && tls != origin.tls()) {
                    throw new RuntimeFileException(originPath, "port " + origin.port()
                            + " cannot serve both http and https origins");
                }
                origins.add(origin);
            }
            out.add(target(name, route.get("target"), routePath, origins));
        }
        return out;
    }

    private Origin origin(String text, String path) {
        URI uri;
        try {
            uri = new URI(text);
        } catch (URISyntaxException e) {
            throw new RuntimeFileException(path, "is not an origin: " + e.getMessage());
        }
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if (!scheme.equals("http") && !scheme.equals("https")) {
            throw new RuntimeFileException(path, "must start with http:// or https://");
        }
        if (uri.getRawUserInfo() != null) {
            throw new RuntimeFileException(path, "must not hold credentials");
        }
        String rawPath = uri.getRawPath();
        if ((rawPath != null && !rawPath.isEmpty() && !rawPath.equals("/")) || uri.getRawQuery() != null
                || uri.getRawFragment() != null) {
            throw new RuntimeFileException(path, "must have no path, query or fragment");
        }
        String authority = uri.getRawAuthority();
        if (authority == null) {
            throw new RuntimeFileException(path, "has no host");
        }
        int colon = authority.lastIndexOf(':');
        String host = (colon < 0 ? authority : authority.substring(0, colon)).toLowerCase(Locale.ROOT);
        boolean tls = scheme.equals("https");
        int port = tls ? 443 : 80;
        if (colon >= 0) {
            port = port(authority.substring(colon + 1), path);
        }
        boolean wildcard = host.startsWith("*.");
        String name = wildcard ? host.substring(2) : host;
        if (name.contains("*") || !DNS_NAME.matcher(name).matches() || IPV4.matcher(name).matches()
                || !name.contains(".")) {
            throw new RuntimeFileException(path, "must name a DNS host, with *. only at its start");
        }
        if (port == 53) {
            throw new RuntimeFileException(path, "port 53 is DNS");
        }
        if (isFlociName(name)) {
            throw new RuntimeFileException(path, "host " + name + " is a Floci or cluster name");
        }
        return new Origin(tls, name, wildcard, port);
    }

    private Route target(String name, JsonNode target, String routePath, List<Origin> origins) {
        String path = RuntimeFileFields.path(routePath, "target");
        if (target == null) {
            throw new RuntimeFileException(path, "is required");
        }
        RuntimeFileFields.mapping(target, path, TARGET_FIELDS);
        String url = RuntimeFileFields.text(target, path, "backend-url");
        String urlPath = RuntimeFileFields.path(path, "backend-url");
        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            throw new RuntimeFileException(urlPath, "is not a URL: " + e.getMessage());
        }
        if (!"http".equalsIgnoreCase(uri.getScheme())) {
            throw new RuntimeFileException(urlPath, "backend-url must use http");
        }
        String host = uri.getHost() == null ? "" : uri.getHost().toLowerCase(Locale.ROOT);
        if (uri.getPort() <= 0) {
            throw new RuntimeFileException(urlPath, "must name the backend's port");
        }
        boolean service = host.endsWith(SERVICE_SUFFIX) && DNS_NAME.matcher(host).matches();
        boolean floci = host.equals(FLOCI_HOST);
        boolean address = IPV4.matcher(host).matches() && isPrivate(host);
        if (!service && !floci && !address) {
            if (isFlociName(host) && !host.endsWith(SERVICE_SUFFIX)) {
                throw new RuntimeFileException(urlPath, "backend-url host " + host + " is a Floci name");
            }
            throw new RuntimeFileException(urlPath, "backend-url host must be a Service name (NAME.NAMESPACE"
                    + SERVICE_SUFFIX + "), a private IPv4 address or " + FLOCI_HOST);
        }
        for (Origin origin : origins) {
            if (origin.host().equals(host)) {
                throw new RuntimeFileException(urlPath, "backend-url host " + host + " is routed");
            }
        }
        String protocolText = RuntimeFileFields.optionalText(target, path, "protocol");
        BackendProtocol protocol = switch (protocolText == null ? "http1" : protocolText) {
            case "http1" -> BackendProtocol.HTTP1;
            case "h2c" -> BackendProtocol.H2C;
            case "auto" -> BackendProtocol.AUTO;
            default -> throw new RuntimeFileException(RuntimeFileFields.path(path, "protocol"),
                    "must be http1, h2c or auto");
        };
        return new Route(name, origins, host, uri.getPort(), protocol);
    }

    private static boolean isPrivate(String address) {
        Matcher m = IPV4.matcher(address);
        if (!m.matches()) {
            return false;
        }
        int a = Integer.parseInt(m.group(1));
        int b = Integer.parseInt(m.group(2));
        return a == 10 || (a == 172 && b >= 16 && b <= 31) || (a == 192 && b == 168);
    }

    private static String cidr(String text, String path) {
        String[] parts = text.split("/", -1);
        if (parts.length > 2 || !IPV4.matcher(parts[0]).matches()) {
            throw new RuntimeFileException(path, "must be an IPv4 address or CIDR");
        }
        for (String octet : parts[0].split("\\.")) {
            if (Integer.parseInt(octet) > 255) {
                throw new RuntimeFileException(path, "must be an IPv4 address or CIDR");
            }
        }
        int length = 32;
        if (parts.length == 2) {
            try {
                length = Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                throw new RuntimeFileException(path, "has a bad prefix length");
            }
            if (length < 0 || length > 32) {
                throw new RuntimeFileException(path, "has a bad prefix length");
            }
        }
        return parts[0] + "/" + length;
    }

    private static List<Integer> portList(JsonNode list, String path) {
        if (list == null) {
            return List.of();
        }
        if (!list.isArray()) {
            throw new RuntimeFileException(path, "must be a list of ports");
        }
        List<Integer> out = new ArrayList<>();
        for (JsonNode port : list) {
            String portPath = RuntimeFileFields.path(path, String.valueOf(out.size()));
            if (!port.isInt() || port.intValue() < 1 || port.intValue() > 65535) {
                throw new RuntimeFileException(portPath, "must be a port from 1 to 65535");
            }
            out.add(port.intValue());
        }
        return out;
    }

    private static int port(String text, String path) {
        try {
            int port = Integer.parseInt(text);
            if (port >= 1 && port <= 65535) {
                return port;
            }
        } catch (NumberFormatException e) {
            throw new RuntimeFileException(path, "has a bad port");
        }
        throw new RuntimeFileException(path, "has a bad port");
    }
}
