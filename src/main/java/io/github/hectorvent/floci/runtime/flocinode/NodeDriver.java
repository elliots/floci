package io.github.hectorvent.floci.runtime.flocinode;


import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Runs the smolvm-pods node VM that Floci creates: creates, starts, stops and deletes it, changes
 * the allow list of an isolated one, reads the boot id of a running one, and lists the machines.
 * {@link NodeScriptDriver} does it with {@code node-vm.sh}.
 */
public interface NodeDriver {

    /** Who created a machine: Floci's resource namespace, empty when none is set. */
    String LABEL_OWNER = "floci.node.owner";
    /** Which Floci data a machine belongs to: 12 hex digits of the SHA-256 of the node data path. */
    String LABEL_DATA = "floci.node.data";
    // node-vm.sh's labels: a machine's Mac ports, and isolated=true for an isolated machine.
    String LABEL_API_PORT = "api-port";
    String LABEL_SERVICE_PORT = "service-port";
    String LABEL_NODE_PORTS = "node-ports";
    String LABEL_ISOLATED = "isolated";
    /** The {@code node-ports} value of a machine that publishes no NodePorts. */
    String NO_NODE_PORTS = "none";

    /**
     * Creates {@code node}'s machine with {@code seed} mounted, labelled with this Floci's owner
     * and data labels, and starts it without waiting for Kubernetes. When smolvm cannot publish one
     * of its Mac ports, deletes the machine, takes other ports from {@code reallocator} and tries
     * again, at most three times.
     *
     * @return the node as it was started, with the ports it publishes
     * @throws NodeCommandException when a command fails or ports keep clashing
     */
    NodeRecord createAndStart(NodeRecord node, Path seed, PortReallocator reallocator);

    /** Starts a created or stopped machine without waiting for Kubernetes. */
    void start(String nodeId);

    /** Stops a running machine; its disks stay. */
    void stop(String nodeId);

    /**
     * Deletes a machine and its disks.
     *
     * @return false when there is no such machine
     */
    boolean delete(String nodeId);

    /**
     * Adds {@code add} to the allow list of an isolated machine and removes {@code remove}, also
     * while it runs. Does nothing when both are empty.
     */
    void updateAllowList(String nodeId, List<String> add, List<String> remove);

    /**
     * The boot id of a running machine's kernel, {@code /proc/sys/kernel/random/boot_id}, which is
     * new at each start. Kubernetes reports it as the node's {@code status.nodeInfo.bootID}.
     */
    String bootId(String nodeId);

    /** Every {@code node-vm.sh} machine on the Mac, Floci's and others, with its state and labels. */
    List<Machine> status();

    /** Whether this Floci created {@code machine}: its owner and data labels are both this Floci's. */
    boolean owned(Machine machine);

    /** Other Mac ports for a node whose machine could not publish one. */
    @FunctionalInterface
    interface PortReallocator {

        /**
         * @param node        the node as it was created
         * @param clashedPort the Mac port that smolvm could not publish, or 0 when it did not say
         * @return the node with other ports
         */
        NodeRecord reallocate(NodeRecord node, int clashedPort);
    }

    /**
     * A {@code node-vm.sh} machine.
     *
     * @param name   the machine's name, a node id for the machines Floci creates
     * @param state  smolvm's state, such as {@code created}, {@code running} or {@code stopped}
     * @param labels the machine's labels
     */
    record Machine(String name, String state, Map<String, String> labels) {

        public Machine {
            labels = labels == null ? Map.of() : Map.copyOf(labels);
        }

        public boolean running() {
            return "running".equals(state);
        }

        /** The Mac port of the Kubernetes API, or 0 when the labels name none. */
        public int apiPort() {
            return port(LABEL_API_PORT);
        }

        /** The Mac port of the node service's API, or 0 when it is not published. */
        public int servicePort() {
            return port(LABEL_SERVICE_PORT);
        }

        /** The NodePorts, {@code FIRST-LAST} or {@code PORT}, or {@code none}. */
        public String nodePorts() {
            return labels.getOrDefault(LABEL_NODE_PORTS, NO_NODE_PORTS);
        }

        public boolean isolated() {
            return "true".equals(labels.get(LABEL_ISOLATED));
        }

        private int port(String key) {
            String value = labels.get(key);
            if (value == null || !value.matches("\\d{1,5}")) {
                return 0;
            }
            int port = Integer.parseInt(value);
            return port <= 65535 ? port : 0;
        }
    }

    /** A {@code node-vm.sh} command that failed, did not finish in time, or could not run. */
    class NodeCommandException extends RuntimeException {

        /** smolvm's message when another process holds a Mac port that a machine publishes. */
        private static final Pattern PUBLISH_FAILED =
                Pattern.compile("cannot publish\\s+host\\s+TCP\\s+\\S+:(\\d{1,5})\\s+to\\s+guest");

        private final String output;
        private final boolean ran;

        public NodeCommandException(String message, String output, Throwable cause) {
            this(message, output, cause, true);
        }

        private NodeCommandException(String message, String output, Throwable cause, boolean ran) {
            super(message, cause);
            this.output = output == null ? "" : output;
            this.ran = ran;
        }

        /** A command that Floci could not run at all: the script is not set or cannot be started. */
        public static NodeCommandException notRun(String message, Throwable cause) {
            return new NodeCommandException(message, "", cause, false);
        }

        /** Whether the script ran; false when it is not set or cannot be started. */
        public boolean ran() {
            return ran;
        }

        /** What the command printed, its error output last. */
        public String output() {
            return output;
        }

        /**
         * Whether smolvm could not publish a Mac port of the machine because another process holds
         * it, which other ports avoid.
         */
        public boolean portClash() {
            String lower = output.toLowerCase(Locale.ROOT);
            return PUBLISH_FAILED.matcher(output).find()
                    || (lower.contains("address already in use") && lower.contains("virtio"));
        }

        /** The Mac port that smolvm could not publish, or 0 when it did not say. */
        public int clashedPort() {
            Matcher matcher = PUBLISH_FAILED.matcher(output);
            if (!matcher.find()) {
                return 0;
            }
            int port = Integer.parseInt(matcher.group(1));
            return port <= 65535 ? port : 0;
        }
    }
}
