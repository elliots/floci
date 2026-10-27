package io.github.hectorvent.floci.services.eks;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.core.common.AwsArnUtils;
import io.github.hectorvent.floci.core.common.CsvParser;
import io.github.hectorvent.floci.services.eks.model.Cluster;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * ServiceAccount annotation admission (IRSA), using the same projected-token contract as the EKS
 * webhook. The ServiceAccount is read through {@link EksClusterManager#serviceAccountReader}, from
 * a k3s container or a smolvm-pods node.
 */
@ApplicationScoped
public class EksIrsaWebhook {

    static final String ANNOTATION_PREFIX = "eks.amazonaws.com/"; // partition-literal: Kubernetes annotation namespace defined by the EKS webhook
    static final String VOLUME_NAME = "aws-iam-token";
    static final String MOUNT_PATH = "/var/run/secrets/eks.amazonaws.com/serviceaccount"; // partition-literal: EKS webhook projected-token path
    private static final long DEFAULT_EXPIRATION = 86400;

    private final EksClusterManager clusters;
    private final ObjectMapper mapper;

    @Inject
    public EksIrsaWebhook(EksClusterManager clusters, ObjectMapper mapper) {
        this.clusters = clusters;
        this.mapper = mapper;
    }

    /**
     * The patch that gives {@code pod} the identity of the IAM role that its ServiceAccount names
     * in {@code eks.amazonaws.com/role-arn}, read now from the cluster, or none.
     *
     * @throws IllegalStateException when the ServiceAccount cannot be read; the message never holds
     *                               what Kubernetes answered
     */
    List<Map<String, Object>> patch(Cluster cluster, String namespace, String serviceAccount,
                                    Map<String, Object> pod) {
        if (!validName(namespace, 63) || !validName(serviceAccount, 253)) {
            return List.of();
        }
        Optional<String> account = clusters.serviceAccountReader(cluster).read(namespace, serviceAccount);
        if (account.isEmpty()) {
            return List.of();
        }
        try {
            JsonNode metadata = mapper.readTree(account.get()).path("metadata");
            if (!namespace.equals(metadata.path("namespace").asText())
                    || !serviceAccount.equals(metadata.path("name").asText())) {
                throw new IllegalStateException("EKS service account lookup returned a different identity");
            }
            Map<String, String> annotations = new HashMap<>();
            metadata.path("annotations").properties().forEach(entry -> {
                if (entry.getValue().isTextual()) {
                    annotations.put(entry.getKey(), entry.getValue().asText());
                }
            });
            return buildPatch(pod, annotations, AwsArnUtils.regionOrDefault(cluster.getArn(), ""));
        } catch (JsonProcessingException expected) {
            // Kubernetes diagnostics and resource contents must not reach admission logs.
            throw new IllegalStateException("EKS service account lookup returned invalid JSON");
        }
    }

    static List<Map<String, Object>> buildPatch(Map<String, Object> pod, Map<String, String> annotations,
                                               String region) {
        String role = annotations.get(ANNOTATION_PREFIX + "role-arn");
        Map<String, Object> spec = map(pod.get("spec"));
        if (role == null || role.isBlank() || spec.isEmpty()) {
            return List.of();
        }
        Map<String, Object> podAnnotations = map(map(pod.get("metadata")).get("annotations"));
        long expiration = expiration(annotations.get(ANNOTATION_PREFIX + "token-expiration"), DEFAULT_EXPIRATION);
        expiration = expiration(podAnnotations.get(ANNOTATION_PREFIX + "token-expiration"), expiration);
        String audience = annotations.getOrDefault(ANNOTATION_PREFIX + "audience", EksOidcService.STS_AUDIENCE);
        boolean regional = "true".equalsIgnoreCase(annotations.get(ANNOTATION_PREFIX + "sts-regional-endpoints"));
        Set<String> skip = skippedContainers(podAnnotations.get(ANNOTATION_PREFIX + "skip-containers"));
        Map<String, Object> volume = Map.of("name", VOLUME_NAME, "projected", Map.of("defaultMode", 420,
                "sources", List.of(Map.of("serviceAccountToken", Map.of("audience", audience,
                        "expirationSeconds", expiration, "path", "token")))));
        List<Map<String, Object>> patch = new ArrayList<>();
        List<?> volumes = list(spec.get("volumes"));
        if (!containsNamed(volumes, VOLUME_NAME)) {
            append(patch, "/spec/volumes", spec.get("volumes"), List.of(volume));
        }
        for (String field : List.of("initContainers", "containers")) {
            List<?> containers = list(spec.get(field));
            for (int i = 0; i < containers.size(); i++) {
                Map<String, Object> container = map(containers.get(i));
                if (skip.contains(container.get("name"))) {
                    continue;
                }
                String path = "/spec/" + field + "/" + i;
                List<?> env = list(container.get("env"));
                List<Map<String, Object>> missing = new ArrayList<>();
                if (regional && !containsNamed(env, "AWS_STS_REGIONAL_ENDPOINTS")) {
                    missing.add(variable("AWS_STS_REGIONAL_ENDPOINTS", "regional"));
                }
                if (!region.isBlank() && !containsNamed(env, "AWS_REGION")
                        && !containsNamed(env, "AWS_DEFAULT_REGION")) {
                    missing.add(variable("AWS_DEFAULT_REGION", region));
                    missing.add(variable("AWS_REGION", region));
                }
                if (!containsNamed(env, "AWS_ROLE_ARN") && !containsNamed(env, "AWS_WEB_IDENTITY_TOKEN_FILE")) {
                    missing.add(variable("AWS_ROLE_ARN", role));
                    missing.add(variable("AWS_WEB_IDENTITY_TOKEN_FILE", MOUNT_PATH + "/token"));
                }
                append(patch, path + "/env", container.get("env"), missing);
                if (!containsNamed(list(container.get("volumeMounts")), VOLUME_NAME)) {
                    append(patch, path + "/volumeMounts", container.get("volumeMounts"), List.of(
                            Map.of("name", VOLUME_NAME, "mountPath", MOUNT_PATH, "readOnly", true)));
                }
            }
        }
        return patch;
    }

    private static void append(List<Map<String, Object>> patch, String path, Object existing,
                               List<Map<String, Object>> values) {
        if (values.isEmpty()) {
            return;
        }
        if (existing == null) {
            patch.add(add(path, values));
        } else {
            values.forEach(value -> patch.add(add(path + "/-", value)));
        }
    }

    private static long expiration(Object value, long fallback) {
        if (!(value instanceof String text)) {
            return fallback;
        }
        try {
            return Math.max(600, Long.parseLong(text));
        } catch (NumberFormatException expected) {
            return fallback;
        }
    }

    private static Set<String> skippedContainers(Object value) {
        if (!(value instanceof String text)) {
            return Set.of();
        }
        return Set.copyOf(CsvParser.parseLine(text));
    }

    private static boolean validName(String value, int maxLength) {
        return value != null && value.length() <= maxLength
                && value.matches("[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?");
    }

    private static boolean containsNamed(List<?> values, String name) {
        return values.stream().anyMatch(value -> name.equals(map(value).get("name")));
    }

    private static Map<String, Object> variable(String name, String value) {
        return Map.of("name", name, "value", value);
    }

    private static Map<String, Object> add(String path, Object value) {
        return Map.of("op", "add", "path", path, "value", value);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object value) {
        return value instanceof Map<?, ?> map ? (Map<String, Object>) map : Map.of();
    }

    private static List<?> list(Object value) {
        return value instanceof List<?> list ? list : List.of();
    }
}
