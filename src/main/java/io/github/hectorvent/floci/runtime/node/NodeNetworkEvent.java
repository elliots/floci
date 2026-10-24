package io.github.hectorvent.floci.runtime.node;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * The attempts of one pod to one destination, port and protocol over a minute: a NETWORK_EVENT of
 * {@code GET /v1/network/events} in tools/smolvm-pods/docs/configuration.md.
 *
 * @param seq         rises with each event, also across restarts of the node service
 * @param kind        {@code blocked}, {@code relayed} or {@code relay_failed}
 * @param pod         the pod's {@code NAMESPACE/NAME}, or its address when the node has no name for it
 * @param destination the TLS server name or HTTP {@code Host} that a relayed attempt sent, else the address
 * @param protocol    such as {@code tcp}, {@code udp}, {@code https} or {@code http}
 * @param count       the attempts in the event
 * @param names       the TLS server names or HTTP {@code Host} values that the attempts sent
 * @param route       the route of a relayed attempt, or null
 * @param error       why the relay could not serve it, or null
 * @param bytes       the bytes that the relay copied
 */
public record NodeNetworkEvent(long seq, String kind, String pod, String destination, int port, String protocol,
                               long count, Instant first, Instant last, List<String> names, String route,
                               String error, long bytes) {

    public NodeNetworkEvent {
        names = List.copyOf(names);
    }

    static NodeNetworkEvent of(JsonNode event) {
        JsonNode seq = event.get("seq");
        if (seq == null || !seq.canConvertToLong()) {
            throw new IllegalArgumentException("no seq");
        }
        List<String> names = new ArrayList<>();
        for (JsonNode name : event.path("names")) {
            names.add(name.asText());
        }
        return new NodeNetworkEvent(seq.asLong(), NodePod.required(event, "kind"), NodePod.required(event, "pod"),
                NodePod.required(event, "destination"), event.path("port").asInt(),
                NodePod.required(event, "protocol"), event.path("count").asLong(),
                Instant.parse(NodePod.required(event, "first")), Instant.parse(NodePod.required(event, "last")),
                names, NodePod.text(event, "route"), NodePod.text(event, "error"), event.path("bytes").asLong());
    }
}
