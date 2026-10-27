package io.github.hectorvent.floci.runtime.node;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * A pod record of a node service: a POD in tools/smolvm-pods/docs/configuration.md.
 *
 * @param ip         the pod's IP, or null before the VM has booted
 * @param phase      while {@code PAUSING}, {@code saving} or {@code stopping}; else null
 * @param hibernate  whether the pod may sleep
 * @param labels     the pod's Kubernetes labels
 * @param controller the Deployment, StatefulSet or other controller that owns the pod, or null
 */
public record NodePod(String sandboxId, String namespace, String name, String uid, String ip, State state,
                      String phase, long generation, String incarnation, boolean hibernate, List<Lease> leases,
                      Map<String, String> labels, Controller controller) {

    public enum State {
        CREATING, AWAKE, PAUSING, ASLEEP, WAKING, FAILED, DELETING;

        public static State of(String wire) {
            for (State state : values()) {
                if (state.name().toLowerCase(Locale.ROOT).equals(wire)) {
                    return state;
                }
            }
            throw new IllegalArgumentException("unknown pod state " + wire);
        }
    }

    /**
     * A lease on the pod: {@code api} for one held through the API, with its holder and expiry,
     * or {@code exec}, {@code create} or {@code waiter} for an implicit one, without.
     */
    public record Lease(String id, String kind, String holder, Instant expiresAt) {
    }

    /** The pod's controller, such as {@code Deployment orders}. */
    public record Controller(String kind, String name) {
    }

    public NodePod {
        leases = List.copyOf(leases);
        labels = Map.copyOf(labels);
    }

    static NodePod of(JsonNode pod) {
        List<Lease> leases = new ArrayList<>();
        for (JsonNode lease : pod.path("leases")) {
            String expires = text(lease, "expires_at");
            leases.add(new Lease(required(lease, "id"), required(lease, "kind"), text(lease, "holder"),
                    expires == null ? null : Instant.parse(expires)));
        }
        Map<String, String> labels = new LinkedHashMap<>();
        for (Map.Entry<String, JsonNode> label : pod.path("labels").properties()) {
            labels.put(label.getKey(), label.getValue().asText());
        }
        JsonNode controller = pod.path("controller");
        return new NodePod(required(pod, "sandbox_id"), required(pod, "namespace"), required(pod, "name"),
                required(pod, "uid"), text(pod, "ip"), State.of(required(pod, "state")), text(pod, "phase"),
                pod.path("generation").asLong(), text(pod, "incarnation"), pod.path("hibernate").asBoolean(true),
                leases, labels,
                controller.isObject() ? new Controller(required(controller, "kind"), required(controller, "name")) : null);
    }

    static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }

    static String required(JsonNode node, String field) {
        String value = text(node, field);
        if (value == null) {
            throw new IllegalArgumentException("no " + field);
        }
        return value;
    }
}
