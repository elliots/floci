package io.github.hectorvent.floci.runtime.node;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A state change of a pod on a node: an EVENT in tools/smolvm-pods/docs/configuration.md.
 *
 * @param seq          rises with each event, also across restarts of the node service
 * @param pod          the pod's sandbox id
 * @param from         the state before, {@code none} for a new record
 * @param to           the state after, {@code deleted} for a removed record
 * @param reason       the reason's kind, such as {@code traffic} or {@code idle}
 * @param reasonDetail the whole reason of a {@code traffic} or {@code aws} event, such as its
 *                     {@code from} and {@code port}; else empty
 * @param error        why the change failed, was refused or did not happen, or null
 */
public record NodeEvent(long seq, Instant time, String pod, String incarnation, String from, String to,
                        long generation, String reason, Map<String, String> reasonDetail, String source,
                        String error, String warning) {

    public NodeEvent {
        reasonDetail = Map.copyOf(reasonDetail);
    }

    static NodeEvent of(JsonNode event) {
        Map<String, String> detail = new LinkedHashMap<>();
        for (Map.Entry<String, JsonNode> field : event.path("reason_detail").properties()) {
            if (!field.getValue().isNull()) {
                detail.put(field.getKey(), field.getValue().asText());
            }
        }
        JsonNode seq = event.get("seq");
        if (seq == null || !seq.canConvertToLong()) {
            throw new IllegalArgumentException("no seq");
        }
        return new NodeEvent(seq.asLong(), Instant.parse(NodePod.required(event, "time")),
                NodePod.required(event, "pod"), NodePod.text(event, "incarnation"), NodePod.required(event, "from"),
                NodePod.required(event, "to"), event.path("generation").asLong(), NodePod.required(event, "reason"),
                detail, NodePod.text(event, "source"), NodePod.text(event, "error"), NodePod.text(event, "warning"));
    }
}
