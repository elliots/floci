package io.github.hectorvent.floci.runtime.wakes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A message of a broker's firehose, {@code amq.rabbitmq.trace}: a publish, with the queues it was
 * routed to, or a delivery from a queue to a consumer or a {@code basic.get}.
 *
 * @param name         the exchange of a publish ({@code ""} for the default exchange), or the queue of a delivery
 * @param routedQueues the queues a publish went to; empty for a delivery
 */
record RabbitMqTrace(Kind kind, String name, List<String> routedQueues) {

    static final String PUBLISH = "publish.";
    static final String DELIVER = "deliver.";
    static final String ROUTED_QUEUES = "routed_queues";

    enum Kind { PUBLISH, DELIVER }

    RabbitMqTrace {
        routedQueues = List.copyOf(routedQueues);
    }

    /** The trace a firehose message carries, from its routing key and headers; null for any other message. */
    static RabbitMqTrace of(String routingKey, Map<String, Object> headers) {
        if (routingKey == null) {
            return null;
        }
        if (routingKey.startsWith(PUBLISH)) {
            List<String> routed = new ArrayList<>();
            if (headers != null && headers.get(ROUTED_QUEUES) instanceof List<?> queues) {
                for (Object queue : queues) {
                    if (queue != null) {
                        routed.add(queue.toString());
                    }
                }
            }
            return new RabbitMqTrace(Kind.PUBLISH, routingKey.substring(PUBLISH.length()), routed);
        }
        if (routingKey.startsWith(DELIVER)) {
            return new RabbitMqTrace(Kind.DELIVER, routingKey.substring(DELIVER.length()), List.of());
        }
        return null;
    }
}
