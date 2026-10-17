package io.github.hectorvent.floci.core.common.microsandbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/** Authenticated, internal lifecycle contract of the native Kubernetes pod controller. */
public class PodControllerClient {
    private static final Set<String> STATES = Set.of("COLD", "WAITINGVOLUMES", "STARTING", "INITSTARTING",
            "INITIALIZING", "RUNNING", "READY",
            "HIBERNATING", "CHECKPOINTED", "SLEEPING", "RESUMING", "RESTARTING", "TERMINATED", "FAILED");
    private final String url;
    private final String token;
    private final ObjectMapper mapper;
    private final LongSupplier nanoTime;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private Map<String, JsonNode> observedDeployments = Map.of();
    private long observationsExpire;
    private String observationsFailure;
    private boolean supportsBatch = true;

    public PodControllerClient(String url, String token, ObjectMapper mapper) {
        this(url, token, mapper, System::nanoTime);
    }

    PodControllerClient(String url, String token, ObjectMapper mapper, LongSupplier nanoTime) {
        URI endpoint = URI.create(url);
        if ((!"http".equals(endpoint.getScheme()) && !"https".equals(endpoint.getScheme())) || endpoint.getHost() == null
                || endpoint.getUserInfo() != null || endpoint.getQuery() != null || endpoint.getFragment() != null) {
            throw new IllegalArgumentException("Pod controller URL must be an HTTP or HTTPS endpoint");
        }
        if (token == null || token.length() < 32 || token.chars().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException("Pod controller token must contain at least 32 characters and no controls");
        }
        this.url = url.replaceAll("/+$", "");
        this.token = token;
        this.mapper = mapper;
        this.nanoTime = nanoTime;
    }

    public record Pod(String uid, String namespace, String name, String state, long generation,
                      boolean proxyAvailability, String idleOwner, int activeConnections, int activeRequests,
                      long activitySequence, boolean activitySequenceKnown) {
        public Pod(String uid, String namespace, String name, String state, long generation) {
            this(uid, namespace, name, state, generation, false, "external", 0, 0, 0, false);
        }

        public Pod(String uid, String namespace, String name, String state, long generation,
                   boolean proxyAvailability, String idleOwner, int activeConnections) {
            this(uid, namespace, name, state, generation, proxyAvailability, idleOwner, activeConnections, 0, 0, false);
        }

        public Pod(String uid, String namespace, String name, String state, long generation,
                   boolean proxyAvailability, String idleOwner, int activeConnections, int activeRequests) {
            this(uid, namespace, name, state, generation, proxyAvailability, idleOwner, activeConnections, activeRequests, 0, false);
        }

        public Pod(String uid, String namespace, String name, String state, long generation,
                   boolean proxyAvailability, String idleOwner, int activeConnections, int activeRequests,
                   long activitySequence) {
            this(uid, namespace, name, state, generation, proxyAvailability, idleOwner, activeConnections,
                    activeRequests, activitySequence, true);
        }
    }

    public record Deployment(String uid, String namespace, String name, String clusterName,
                             long generation, int replicas, List<Pod> pods) {
        public Deployment {
            pods = List.copyOf(pods);
        }
    }

    public void checkAvailable(Duration timeout) {
        JsonNode health = request("GET", "/v1/health", null, timeout);
        if (!"ok".equals(health.path("status").asText())
                || !"microsandbox-pods".equals(health.path("name").asText())
                || health.path("contract").asInt(-1) != 1) {
            throw new IllegalStateException("Expected healthy microsandbox pod controller contract 1");
        }
    }

    public Deployment deployment(String namespace, String name, Duration timeout) {
        JsonNode value = request("GET", "/v1/deployments/" + segment(namespace) + "/" + segment(name), null, timeout);
        return deployment(value, namespace, name);
    }

    /** Shares one read of controller metadata across all workloads, without querying sleeping guests. */
    public synchronized Deployment observedDeployment(String namespace, String name, Duration timeout) {
        if (!supportsBatch) {
            return deployment(namespace, name, timeout);
        }
        if (observationsExpire == 0 || nanoTime.getAsLong() - observationsExpire >= 0) {
            try {
                refreshObservations(timeout);
                observationsFailure = null;
            } catch (ControllerHttpException e) {
                if (e.status == 404) {
                    supportsBatch = false;
                    return deployment(namespace, name, timeout);
                }
                observationsFailure = "Pod controller observation failed with HTTP " + e.status;
            } catch (RuntimeException e) {
                observationsFailure = "Pod controller deployment observation is unavailable";
            } finally {
                observationsExpire = nanoTime.getAsLong() + Duration.ofSeconds(1).toNanos();
            }
        }
        if (observationsFailure != null) {
            throw new IllegalStateException(observationsFailure);
        }
        JsonNode observed = observedDeployments.get(namespace + "/" + name);
        if (observed == null || observed.has("error")) {
            throw new IllegalStateException("Deployment is unavailable in the pod controller observation");
        }
        return deployment(observed, namespace, name);
    }

    private void refreshObservations(Duration timeout) {
        JsonNode batch = request("GET", "/v1/deployments", null, timeout);
        if (!batch.path("deployments").isArray()) {
            throw new IllegalStateException("Pod controller returned an invalid deployment snapshot");
        }
        Map<String, JsonNode> observations = new LinkedHashMap<>();
        for (JsonNode value : batch.path("deployments")) {
            String key = requiredText(value, "namespace") + "/" + requiredText(value, "name");
            if (observations.putIfAbsent(key, value) != null) {
                throw new IllegalStateException("Pod controller returned duplicate deployment identities");
            }
        }
        observedDeployments = Map.copyOf(observations);
    }

    private static Deployment deployment(JsonNode value, String namespace, String name) {
        String uid = requiredText(value, "uid");
        if (!namespace.equals(requiredText(value, "namespace")) || !name.equals(requiredText(value, "name"))) {
            throw new IllegalStateException("Pod controller returned a different deployment");
        }
        long generation = generation(value);
        int replicas = value.path("replicas").asInt(-1);
        if (replicas < 1 || !value.path("pods").isArray()) {
            throw new IllegalStateException("Deployment must declare positive replicas and controller-owned pods");
        }
        List<Pod> pods = new ArrayList<>();
        Set<String> identities = new HashSet<>();
        for (JsonNode child : value.path("pods")) {
            Pod pod = pod(child);
            if (!namespace.equals(pod.namespace()) || !identities.add(pod.uid())) {
                throw new IllegalStateException("Pod controller returned inconsistent deployment membership");
            }
            pods.add(pod);
        }
        if (pods.size() != replicas) {
            throw new IllegalStateException("Deployment is changing membership; wait for its rollout before hibernating");
        }
        return new Deployment(uid, namespace, name, value.path("clusterName").asText(), generation, replicas, pods);
    }

    public Pod resume(Pod observed, Duration timeout) {
        return transition(observed, "resume", timeout);
    }

    public Pod hibernate(Pod observed, Duration timeout) {
        return transition(observed, "hibernate", timeout);
    }

    private Pod transition(Pod observed, String operation, Duration timeout) {
        invalidateObservations();
        ObjectNode body = mapper.createObjectNode().put("expectedGeneration", observed.generation());
        if ("hibernate".equals(operation) && observed.activitySequenceKnown()) {
            body.put("expectedActivitySequence", observed.activitySequence());
        }
        Pod result;
        try {
            result = pod(request("POST", "/v1/pods/" + segment(observed.uid()) + "/" + operation, body.toString(), timeout));
        } finally {
            invalidateObservations();
        }
        if (!observed.uid().equals(result.uid()) || !observed.namespace().equals(result.namespace())
                || !observed.name().equals(result.name()) || result.generation() < observed.generation()) {
            throw new IllegalStateException("Pod controller changed lifecycle identity");
        }
        return result;
    }

    private synchronized void invalidateObservations() {
        observationsExpire = 0;
    }

    private static Pod pod(JsonNode value) {
        String state = requiredText(value, "state");
        if (!STATES.contains(state)) {
            throw new IllegalStateException("Pod controller returned an unsupported lifecycle state");
        }
        if (value.has("proxyAvailability") && !value.path("proxyAvailability").isBoolean()) {
            throw new IllegalStateException("Pod controller returned invalid proxy availability");
        }
        boolean proxy = value.path("proxyAvailability").asBoolean(false);
        String idleOwner = value.path("idleOwner").asText(proxy ? "" : "external");
        JsonNode connectionsValue = value.path("activeConnections");
        if (!connectionsValue.isMissingNode()
                && (!connectionsValue.isIntegralNumber() || !connectionsValue.canConvertToInt())) {
            throw new IllegalStateException("Pod controller returned an invalid active connection count");
        }
        int connections = connectionsValue.asInt(0);
        JsonNode requestsValue = value.path("activeRequests");
        if (!requestsValue.isMissingNode()
                && (!requestsValue.isIntegralNumber() || !requestsValue.canConvertToInt() || requestsValue.asInt() < 0)) {
            throw new IllegalStateException("Pod controller returned an invalid active request count");
        }
        JsonNode sequenceValue = value.path("activitySequence");
        if (!sequenceValue.isMissingNode()
                && (!sequenceValue.isIntegralNumber() || !sequenceValue.canConvertToLong() || sequenceValue.asLong() < 0)) {
            throw new IllegalStateException("Pod controller returned an invalid activity sequence");
        }
        if (connections < 0 || proxy && !Set.of("controller", "external").contains(idleOwner)) {
            throw new IllegalStateException("Pod controller returned invalid proxy lifecycle ownership");
        }
        return new Pod(requiredText(value, "uid"), requiredText(value, "namespace"), requiredText(value, "name"),
                state, generation(value), proxy, idleOwner, connections, requestsValue.asInt(0), sequenceValue.asLong(0),
                !sequenceValue.isMissingNode());
    }

    private static long generation(JsonNode value) {
        JsonNode generation = value.path("generation");
        if (!generation.isIntegralNumber() || !generation.canConvertToLong() || generation.asLong() < 0) {
            throw new IllegalStateException("Pod controller did not provide a valid lifecycle generation");
        }
        return generation.asLong();
    }

    private static String requiredText(JsonNode value, String name) {
        JsonNode field = value.path(name);
        if (!field.isTextual() || field.asText().isBlank()) {
            throw new IllegalStateException("Pod controller response is missing " + name);
        }
        return field.asText();
    }

    private static String segment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private JsonNode request(String method, String path, String body, Duration timeout) {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url + path)).timeout(timeout)
                .header("Authorization", "Bearer " + token).header("Accept", "application/json");
        if (body == null) {
            request.GET();
        } else {
            request.header("Content-Type", "application/json").method(method, HttpRequest.BodyPublishers.ofString(body));
        }
        try {
            HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 409) {
                throw new IllegalStateException("Pod lifecycle generation changed or its operation failed; inspect before retrying");
            }
            if (response.statusCode() != 200) {
                throw new ControllerHttpException(response.statusCode());
            }
            JsonNode parsed = mapper.readTree(response.body());
            if (parsed == null || !parsed.isObject()) {
                throw new IllegalStateException("Pod controller returned an invalid response");
            }
            return parsed;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Pod controller request interrupted; lifecycle outcome may be unknown");
        } catch (IOException e) {
            // A controller's error body may contain application details. Do not attach it to errors.
            throw new IllegalStateException("Pod controller request failed; lifecycle outcome may be unknown");
        }
    }

    private static final class ControllerHttpException extends IllegalStateException {
        private final int status;

        private ControllerHttpException(int status) {
            super("Pod controller request failed with HTTP " + status);
            this.status = status;
        }
    }
}
