package io.github.hectorvent.floci.runtime.wakes;

import io.github.hectorvent.floci.runtime.node.NodePod;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The runtime file's {@code workloads}: the message consumers that Floci wakes, by name, in file
 * order. Every record is immutable and compares by value.
 */
public record WorkloadDefinitions(Map<String, Workload> workloads) {

    public static final WorkloadDefinitions NONE = new WorkloadDefinitions(Map.of());

    public WorkloadDefinitions {
        workloads = Collections.unmodifiableMap(new LinkedHashMap<>(workloads));
    }

    /** The workloads that are enabled, in file order. */
    public List<Workload> enabled() {
        return workloads.values().stream().filter(Workload::enabled).toList();
    }

    /**
     * A message consumer: the queues whose messages it takes, and its pods.
     *
     * @param idleTimeout    how long Floci keeps the pods' leases after the last message
     * @param startupTimeout the limit of one wake call
     * @param queues         SQS queue names in the workload's account and region
     */
    public record Workload(String name, boolean enabled, String accountId, String region, Duration idleTimeout,
                           Duration startupTimeout, List<String> queues, List<RabbitMqQueue> rabbitMqQueues,
                           Target target) {

        public Workload {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(target, "target");
            queues = List.copyOf(queues);
            rabbitMqQueues = List.copyOf(rabbitMqQueues);
        }
    }

    /**
     * A queue on one of Floci's Amazon MQ for RabbitMQ brokers, which Floci follows as its own
     * broker user.
     *
     * @param usernameEnv an environment variable that the runtime file may name for a user of the
     *                    broker; accepted and not used
     * @param passwordEnv that user's password, null when {@code usernameEnv} is
     */
    public record RabbitMqQueue(String brokerId, String virtualHost, String queue, String usernameEnv,
                                String passwordEnv) {
    }

    /**
     * Where a workload's pods run.
     *
     * @param node the node's name in the node directory: {@code node:NAME} for a node of the
     *             runtime file's {@code nodes}, {@code cluster:ACCOUNT/REGION/NAME} for the node
     *             of a Floci EKS cluster
     */
    public record Target(String node, String namespace, PodSelector pods) {

        /** Whether {@code pod} is one of the workload's pods. */
        public boolean selects(NodePod pod) {
            return namespace.equals(pod.namespace()) && pods.matches(pod);
        }
    }

    /** Which pods of a namespace are a workload's, from the labels and controller in their node records. */
    public sealed interface PodSelector {

        boolean matches(NodePod pod);

        /** The pods that one Deployment or StatefulSet controls. */
        record Controller(String kind, String name) implements PodSelector {

            public static Controller deployment(String name) {
                return new Controller("Deployment", name);
            }

            public static Controller statefulSet(String name) {
                return new Controller("StatefulSet", name);
            }

            @Override
            public boolean matches(NodePod pod) {
                NodePod.Controller controller = pod.controller();
                return controller != null && kind.equals(controller.kind()) && name.equals(controller.name());
            }
        }

        /** The pods that have every one of these labels. */
        record Labels(Map<String, String> labels) implements PodSelector {

            public Labels {
                labels = Collections.unmodifiableMap(new LinkedHashMap<>(labels));
            }

            @Override
            public boolean matches(NodePod pod) {
                return labels.entrySet().stream()
                        .allMatch(label -> label.getValue().equals(pod.labels().get(label.getKey())));
            }
        }
    }
}
