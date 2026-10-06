package io.github.hectorvent.floci.runtime.network;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Reads and validates network isolation and origin-routing configuration. */
public final class NetworkConfigLoader {
    private NetworkConfigLoader() {}

    public static NetworkDefinition load(Path file) {
        try {
            return parse(new ObjectMapper(new YAMLFactory().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION))
                    .readTree(file.toFile()));
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot read network configuration: " + file, e);
        }
    }

    static NetworkDefinition parse(JsonNode root) {
        fields(root, "configuration", Set.of("network"));
        JsonNode node = root.path("network");
        fields(node, "network", Set.of("isolation", "routes"));
        JsonNode isolation = node.path("isolation");
        if (!isolation.isMissingNode()) {
            fields(isolation, "network.isolation", Set.of("enabled"));
        }
        boolean enabled = bool(isolation, "enabled", false);
        JsonNode routes = node.path("routes");
        if (routes.isMissingNode()) {
            return new NetworkDefinition(enabled, List.of());
        }
        object(routes, "network.routes");
        Set<URI> seen = new HashSet<>();
        Set<String> hosts = new HashSet<>();
        Map<Integer, String> protocols = new HashMap<>();
        List<NetworkDefinition.Route> result = new ArrayList<>();
        for (Map.Entry<String, JsonNode> entry : routes.properties()) {
            String name = entry.getKey();
            JsonNode route = entry.getValue();
            fields(route, name, Set.of("origins", "target"));
            List<URI> origins = new ArrayList<>();
            for (String value : strings(route, "origins")) {
                URI origin = networkOrigin(value, name);
                if (origin.getHost().matches("[0-9.]+")) {
                    throw invalid(name, "origin must use a DNS hostname so Floci can redirect it locally");
                }
                if (origin.getPort() == NetworkPodAdmission.PORT || NetworkPodAdmission.HOST.equals(origin.getHost())) {
                    throw invalid(name, "origin uses the reserved Kubernetes trust admission host or port");
                }
                if (!seen.add(origin)) {
                    throw invalid(name, "origin is already assigned: " + origin);
                }
                String protocol = protocols.putIfAbsent(origin.getPort(), origin.getScheme());
                if (protocol != null && !protocol.equals(origin.getScheme())) {
                    throw invalid(name, "HTTP and HTTPS origins cannot share a listener port");
                }
                hosts.add(origin.getHost());
                origins.add(origin);
            }
            if (origins.isEmpty()) {
                throw invalid(name, "origins must not be empty");
            }
            JsonNode target = route.path("target");
            fields(target, name + ".target", Set.of("backend-url", "preserve-host"));
            URI backend = networkOrigin(text(target, "backend-url"), name);
            result.add(new NetworkDefinition.Route(name, origins, backend, bool(target, "preserve-host", false)));
        }
        for (NetworkDefinition.Route route : result) {
            if (hosts.contains(route.backendUrl().getHost())) {
                throw invalid(route.name(), "backend-url resolves to a network gateway hostname");
            }
        }
        return new NetworkDefinition(enabled, result);
    }

    private static URI networkOrigin(String value, String name) {
        URI uri = URI.create(value);
        if (!Set.of("http", "https").contains(uri.getScheme()) || uri.getHost() == null
                || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null
                || !Set.of("", "/").contains(uri.getPath()) || uri.getPort() == 0 || uri.getPort() > 65535
                || !uri.getHost().matches("(?i)[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?")) {
            throw invalid(name, "expected an HTTP(S) origin with a DNS hostname and no path or credentials");
        }
        return URI.create(uri.getScheme() + "://" + uri.getHost().toLowerCase(Locale.ROOT)
                + ":" + NetworkDefinition.port(uri));
    }

    private static List<String> strings(JsonNode node, String key) {
        JsonNode values = node.path(key);
        if (values.isMissingNode()) {
            return List.of();
        }
        if (!values.isArray()) {
            throw invalid(key, "must be an array");
        }
        List<String> result = new ArrayList<>();
        for (JsonNode value : values) {
            if (!value.isTextual() || value.asText().isBlank()) {
                throw invalid(key, "must contain nonempty strings");
            }
            result.add(value.asText());
        }
        return result;
    }

    private static String text(JsonNode node, String key) {
        JsonNode value = node.path(key);
        if (!value.isTextual() || value.asText().isBlank()) {
            throw invalid(key, "must be a nonempty string");
        }
        return value.asText();
    }

    private static boolean bool(JsonNode node, String key, boolean fallback) {
        JsonNode value = node.path(key);
        if (value.isMissingNode()) {
            return fallback;
        }
        if (!value.isBoolean()) {
            throw invalid(key, "must be boolean");
        }
        return value.booleanValue();
    }

    private static void fields(JsonNode node, String name, Set<String> allowed) {
        object(node, name);
        for (String key : node.propertyStream().map(Map.Entry::getKey).toList()) {
            if (!allowed.contains(key)) {
                throw invalid(name, "unknown field: " + key);
            }
        }
    }

    private static void object(JsonNode node, String name) {
        if (node == null || !node.isObject()) {
            throw invalid(name, "must be an object");
        }
    }

    private static IllegalArgumentException invalid(String name, String reason) {
        return new IllegalArgumentException("Invalid network " + name + ": " + reason);
    }
}
