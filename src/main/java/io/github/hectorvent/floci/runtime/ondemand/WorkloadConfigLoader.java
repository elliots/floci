package io.github.hectorvent.floci.runtime.ondemand;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.hectorvent.floci.core.common.AwsRegions;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.BackendProtocol;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.HealthRequest;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.HealthResponse;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.RuntimeType;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Reads configuration as a tree so workload records need no native-image reflection. */
public final class WorkloadConfigLoader {
    private WorkloadConfigLoader() {}

    public static List<WorkloadDefinition> load(Path file, String defaultAccount, String defaultRegion) {
        try {
            return parse(new ObjectMapper(new YAMLFactory().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION))
                    .readTree(file.toFile()), defaultAccount, defaultRegion);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot read on-demand configuration: " + file, e);
        }
    }

    static List<WorkloadDefinition> parse(JsonNode root, String defaultAccount, String defaultRegion) {
        fields(root, "configuration", Set.of("workloads"));
        JsonNode workloads = root.path("workloads");
        object(workloads, "workloads");
        List<WorkloadDefinition> definitions = new ArrayList<>();
        Set<String> hosts = new HashSet<>();
        Set<String> targets = new HashSet<>();
        for (Map.Entry<String, JsonNode> entry : workloads.properties()) {
            String id = entry.getKey();
            JsonNode node = entry.getValue();
            fields(node, id, Set.of("enabled", "account-id", "region", "host", "runtime", "queues",
                    "idle-timeout-seconds", "startup-timeout-seconds", "health", "readiness"));
            JsonNode runtime = node.path("runtime");
            object(runtime, id + ".runtime");
            RuntimeType type = switch (text(runtime, "type", null)) {
                case "kubernetes" -> RuntimeType.KUBERNETES;
                case "ec2" -> RuntimeType.EC2;
                case "ecs" -> RuntimeType.ECS;
                default -> throw invalid(id, "runtime.type must be kubernetes, ec2 or ecs");
            };
            fields(runtime, id + ".runtime", switch (type) {
                case KUBERNETES -> Set.of("type", "deployment", "namespace", "replicas", "backend-url",
                        "cluster-name", "backend-protocol");
                case EC2 -> Set.of("type", "instance-id", "backend-url", "backend-port", "backend-protocol");
                case ECS -> Set.of("type", "service", "cluster-name", "replicas", "backend-url", "backend-protocol");
            });
            BackendProtocol protocol = switch (text(runtime, "backend-protocol", "http1")) {
                case "http1" -> BackendProtocol.HTTP1;
                case "http2" -> BackendProtocol.HTTP2;
                default -> throw invalid(id, "backend-protocol must be http1 or http2");
            };
            String account = text(node, "account-id", defaultAccount);
            if (!account.matches("[0-9]{12}")) {
                throw invalid(id, "account-id must contain 12 digits");
            }
            String region = text(node, "region", defaultRegion);
            if (!AwsRegions.isRegionId(region)) {
                throw invalid(id, "unknown region: " + region);
            }
            String host = node.has("host") ? text(node, "host", null).toLowerCase(Locale.ROOT) : null;
            if (host != null && (!host.matches("[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?") || !hosts.add(host))) {
                throw invalid(id, "host must be a unique DNS hostname without a port");
            }
            String target = text(runtime, switch (type) {
                case EC2 -> "instance-id";
                case KUBERNETES -> "deployment";
                case ECS -> "service";
            }, null);
            String namespace = text(runtime, "namespace", "default");
            String clusterName = runtime.has("cluster-name") ? text(runtime, "cluster-name", null) : null;
            if (clusterName != null && !clusterName.matches(type == RuntimeType.ECS
                    ? "[A-Za-z0-9_-]{1,255}" : "[A-Za-z0-9][A-Za-z0-9_-]{0,99}")) {
                throw invalid(id, "invalid cluster-name");
            }
            if (!(type == RuntimeType.ECS ? target.matches("[A-Za-z0-9_-]{1,255}")
                    : target.matches("[a-z0-9][a-z0-9.-]*")) || !namespace.matches("[a-z0-9][a-z0-9-]*")) {
                throw invalid(id, "invalid runtime target or namespace");
            }
            if (type == RuntimeType.ECS && clusterName == null) {
                clusterName = "default";
            }
            String identity = type == RuntimeType.KUBERNETES
                    ? type + "/" + (clusterName == null ? "current-context"
                            : account + "/" + region + "/" + clusterName) + "/" + namespace + "/" + target
                    : type + "/" + account + "/" + region + "/"
                            + (type == RuntimeType.ECS ? clusterName + "/" : "") + target;
            if (!targets.add(identity)) {
                throw invalid(id, "runtime target is already owned by another workload");
            }
            URI backend = runtime.has("backend-url") ? URI.create(text(runtime, "backend-url", null)) : null;
            if (backend != null && (!Set.of("http", "https").contains(backend.getScheme())
                    || backend.getHost() == null || backend.getUserInfo() != null
                    || backend.getPort() == 0 || backend.getPort() > 65535
                    || backend.getQuery() != null || backend.getFragment() != null
                    || (backend.getPath() != null && !Set.of("", "/").contains(backend.getPath())))) {
                throw invalid(id, "backend-url must be an HTTP(S) origin without credentials, path, query or fragment");
            }
            if (backend != null && host != null && backend.getHost().equalsIgnoreCase(host)) {
                throw invalid(id, "backend-url must bypass the gateway hostname");
            }
            if (type != RuntimeType.EC2 && host != null && backend == null) {
                throw invalid(id, "Kubernetes and ECS HTTP workloads require a reachable backend-url");
            }
            List<String> queues = strings(node, "queues");
            if (host == null && queues.isEmpty()) {
                throw invalid(id, "a workload needs a host or at least one queue");
            }
            for (String queue : queues) {
                if (!queue.matches("[a-zA-Z0-9_-]{1,80}(?:\\.fifo)?") || queue.length() > 80) {
                    throw invalid(id, "queues must contain queue names, not URLs or ARNs");
                }
            }
            List<HealthRequest> healthRequests = new ArrayList<>();
            HealthResponse sleeping = new HealthResponse(200, "", "text/plain");
            if (node.has("health")) {
                JsonNode health = node.path("health");
                fields(health, id + ".health", Set.of("requests", "sleeping-response"));
                JsonNode requests = health.path("requests");
                if (!requests.isArray()) {
                    throw invalid(id, "health.requests must be an array");
                }
                for (JsonNode request : requests) {
                    fields(request, "health request", Set.of("method", "path"));
                    String method = text(request, "method", "GET");
                    if (!Set.of("GET", "HEAD").contains(method)) {
                        throw invalid(id, "health requests must use GET or HEAD");
                    }
                    healthRequests.add(new HealthRequest(method, path(request, "path", null)));
                }
                if (health.has("sleeping-response")) {
                    JsonNode response = health.path("sleeping-response");
                    fields(response, "sleeping response", Set.of("status", "body", "content-type"));
                    String body = text(response, "body", "");
                    String contentType = text(response, "content-type", "text/plain");
                    if (body.length() > 65536 || contentType.contains("\r") || contentType.contains("\n")) {
                        throw invalid(id, "invalid sleeping health response");
                    }
                    sleeping = new HealthResponse(number(response, "status", 200, 200, 299), body, contentType);
                }
            }
            JsonNode readiness = node.path("readiness");
            if (!readiness.isMissingNode()) {
                fields(readiness, "readiness", Set.of("enabled", "path", "status"));
            }
            boolean httpReadiness = bool(readiness, "enabled", host != null || readiness.has("path"));
            if (httpReadiness && type != RuntimeType.EC2 && backend == null) {
                throw invalid(id, "HTTP readiness requires a backend-url");
            }
            definitions.add(new WorkloadDefinition(id, bool(node, "enabled", true), account, region,
                    host, type, target, namespace, number(runtime, "replicas", 1, 1, 1000), backend,
                    number(runtime, "backend-port", 80, 1, 65535),
                    Duration.ofSeconds(number(node, "idle-timeout-seconds", 120, 1, 86400)),
                    Duration.ofSeconds(number(node, "startup-timeout-seconds", 90, 1, 3600)),
                    List.copyOf(queues), List.copyOf(healthRequests), sleeping,
                    httpReadiness ? path(readiness, "path", "/ready") : null,
                    number(readiness, "status", 200, 200, 299), clusterName, protocol));
        }
        for (WorkloadDefinition definition : definitions) {
            if (definition.backendUrl() != null
                    && hosts.contains(definition.backendUrl().getHost().toLowerCase(Locale.ROOT))) {
                throw invalid(definition.id(), "backend-url resolves to a configured gateway hostname");
            }
        }
        return List.copyOf(definitions);
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

    private static String path(JsonNode node, String key, String fallback) {
        String path = text(node, key, fallback);
        if (!path.startsWith("/") || path.startsWith("//") || path.contains("?") || path.contains("#")) {
            throw invalid(key, "must be an absolute path without query or fragment");
        }
        return path;
    }

    private static String text(JsonNode node, String key, String fallback) {
        JsonNode value = node.path(key);
        if (value.isMissingNode() && fallback != null) {
            return fallback;
        }
        if (!value.isTextual() || (value.asText().isBlank() && !"body".equals(key))) {
            throw invalid(key, "must be a nonempty string");
        }
        return value.asText();
    }

    private static int number(JsonNode node, String key, int fallback, int min, int max) {
        JsonNode value = node.path(key);
        if (value.isMissingNode()) {
            return fallback;
        }
        if (!value.isIntegralNumber() || !value.canConvertToInt() || value.intValue() < min || value.intValue() > max) {
            throw invalid(key, "must be an integer between " + min + " and " + max);
        }
        return value.intValue();
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
        return new IllegalArgumentException("Invalid on-demand " + name + ": " + reason);
    }
}
