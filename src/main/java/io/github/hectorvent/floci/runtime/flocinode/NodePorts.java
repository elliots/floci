package io.github.hectorvent.floci.runtime.flocinode;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.RuntimeConfig.NodeConfig;
import io.github.hectorvent.floci.core.common.docker.PortAllocator;
import io.github.hectorvent.floci.runtime.flocinode.NodeDriver.Machine;
import io.github.hectorvent.floci.runtime.flocinode.NodeSettings.PortRange;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ProtocolFamily;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntPredicate;

/**
 * The Mac ports of the node that Floci creates: the Kubernetes API, in
 * {@code floci.runtime.node.api-server-base-port} to {@code api-server-max-port}, through the
 * {@link PortAllocator} that k3s clusters share; the node service's API, in
 * {@code floci.runtime.node.node-service-base-port} to {@code node-service-max-port}; and
 * {@code floci.runtime.node.node-ports}, when no other machine holds them.
 *
 * <p>smolvm publishes a machine's ports on {@code 127.0.0.1} and {@code ::1}, where a bind of the
 * wildcard address does not see them on macOS. A port is free here only when both loopback
 * addresses can be bound with {@code SO_REUSEADDR} off. An allocated port stays reserved until it
 * is released, and so do the ports in the labels of every {@code node-vm.sh} machine, running or
 * not, since a stopped machine publishes them again when it starts.
 */
@ApplicationScoped
public class NodePorts {

    private static final Logger LOG = Logger.getLogger(NodePorts.class);

    private final EmulatorConfig config;
    private final PortAllocator portAllocator;
    private final IntPredicate free;
    private final Set<Integer> reservedNodePorts = new HashSet<>();

    @Inject
    public NodePorts(EmulatorConfig config, PortAllocator portAllocator) {
        this(config, portAllocator, NodePorts::loopbackFree);
    }

    NodePorts(EmulatorConfig config, PortAllocator portAllocator, IntPredicate free) {
        this.config = config;
        this.portAllocator = portAllocator;
        this.free = free;
    }

    /**
     * The Mac ports of one node.
     *
     * @param nodePorts {@code FIRST-LAST}, {@code PORT} or {@code none}
     */
    record Ports(int apiPort, int servicePort, String nodePorts) {
    }

    /**
     * Reserves the ports of a new node: a Kubernetes API port, a node service port, and
     * {@code node-ports} when none of them is reserved or taken, else none.
     *
     * @throws IllegalStateException when the API or the node service range has no free port
     */
    synchronized Ports allocate() {
        int apiPort = allocateApiPort();
        int servicePort;
        try {
            servicePort = allocateServicePort();
        } catch (IllegalStateException e) {
            portAllocator.release(apiPort);
            throw e;
        }
        return new Ports(apiPort, servicePort, allocateNodePorts());
    }

    /**
     * Other ports for {@code node}, whose machine could not publish {@code clashedPort} because a
     * process that the probe does not see holds it. That port stays reserved. A clashed API or node
     * service port is replaced; a clashed NodePort leaves the node with none and releases the rest
     * of its range. When smolvm named no port of the node, the API and node service ports are
     * replaced and the old ones released.
     *
     * @throws IllegalStateException when a range has no other free port
     */
    synchronized NodeRecord reallocate(NodeRecord node, int clashedPort) {
        Optional<PortRange> nodePorts = range(node.nodePorts());
        boolean nodePortClashed = nodePorts.filter(range -> contains(range, clashedPort)).isPresent();
        boolean named = clashedPort == node.apiPort() || clashedPort == node.servicePort() || nodePortClashed;
        int apiPort = node.apiPort();
        int servicePort = node.servicePort();
        if (!named || clashedPort == apiPort) {
            apiPort = allocateApiPort();
        }
        if (!named || clashedPort == servicePort) {
            try {
                servicePort = allocateServicePort();
            } catch (IllegalStateException e) {
                if (apiPort != node.apiPort()) {
                    portAllocator.release(apiPort);
                }
                throw e;
            }
        }
        if (!named) {
            portAllocator.release(node.apiPort());
            portAllocator.release(node.servicePort());
        }
        String nodePortsValue = node.nodePorts();
        if (nodePortClashed) {
            nodePorts.ifPresent(this::releaseNodePorts);
            reservedNodePorts.add(clashedPort);
            nodePortsValue = NodeDriver.NO_NODE_PORTS;
        }
        String clashed = named ? String.valueOf(clashedPort) : "unnamed";
        LOG.infov("Node {0} takes API port {1}, node service port {2} and NodePorts {3}, since smolvm could not "
                + "publish one of its Mac ports ({4})", node.nodeId(), String.valueOf(apiPort),
                String.valueOf(servicePort), nodePortsValue, clashed);
        return node.withPorts(apiPort, servicePort, nodePortsValue);
    }

    /** Releases {@code node}'s ports, once its machine is gone. */
    synchronized void release(NodeRecord node) {
        portAllocator.release(node.apiPort());
        portAllocator.release(node.servicePort());
        range(node.nodePorts()).ifPresent(this::releaseNodePorts);
    }

    /** Reserves the ports of a node from its record. */
    synchronized void reserve(NodeRecord node) {
        reserve(node.apiPort(), node.servicePort(), node.nodePorts());
    }

    /**
     * Moves a node's reservation from the ports of {@code from}, its record, to those of {@code to},
     * its machine's labels: reserves {@code to}'s ports and releases those that only {@code from} has.
     */
    synchronized void move(NodeRecord from, NodeRecord to) {
        reserve(to);
        Set<Integer> kept = Set.of(to.apiPort(), to.servicePort());
        for (int port : List.of(from.apiPort(), from.servicePort())) {
            if (port > 0 && !kept.contains(port)) {
                portAllocator.release(port);
            }
        }
        Optional<PortRange> released = range(from.nodePorts());
        Optional<PortRange> keptNodePorts = range(to.nodePorts());
        if (released.isEmpty()) {
            return;
        }
        for (int port = released.get().first(); port <= released.get().last(); port++) {
            if (keptNodePorts.isEmpty() || !contains(keptNodePorts.get(), port)) {
                reservedNodePorts.remove(port);
            }
        }
    }

    /** Reserves the ports in the labels of {@code machines}, Floci's and others. */
    synchronized void reserve(List<Machine> machines) {
        for (Machine machine : machines) {
            reserve(machine.apiPort(), machine.servicePort(), machine.nodePorts());
        }
    }

    /**
     * Whether {@code port} is free on the Mac's loopback: {@code 127.0.0.1}, and {@code ::1} when
     * the Mac has it, can be bound with {@code SO_REUSEADDR} off. Also false for a listener on the
     * IPv4 wildcard address.
     */
    static boolean loopbackFree(int port) {
        return bindable(StandardProtocolFamily.INET, "127.0.0.1", port)
                && bindable(StandardProtocolFamily.INET6, "::1", port);
    }

    private static boolean bindable(ProtocolFamily family, String address, int port) {
        try (ServerSocketChannel channel = ServerSocketChannel.open(family)) {
            channel.setOption(StandardSocketOptions.SO_REUSEADDR, false);
            channel.bind(new InetSocketAddress(address, port));
            return true;
        } catch (BindException e) {
            return family == StandardProtocolFamily.INET6 && !inUse(e);
        } catch (IOException | UnsupportedOperationException e) {
            // Without IPv6, nothing can hold ::1.
            return family == StandardProtocolFamily.INET6;
        }
    }

    private static boolean inUse(BindException e) {
        return e.getMessage() != null && e.getMessage().toLowerCase(Locale.ROOT).contains("in use");
    }

    private int allocateApiPort() {
        NodeConfig node = config.runtime().node();
        try {
            return portAllocator.allocate(node.apiServerBasePort(), node.apiServerMaxPort(), free);
        } catch (RuntimeException e) {
            throw new IllegalStateException("No free Mac port for the Kubernetes API of a smolvm-pods node in "
                    + node.apiServerBasePort() + "-" + node.apiServerMaxPort() + ": widen "
                    + NodeSettings.key("api-server-base-port") + " to " + NodeSettings.key("api-server-max-port"), e);
        }
    }

    private int allocateServicePort() {
        NodeConfig node = config.runtime().node();
        try {
            return portAllocator.allocate(node.nodeServiceBasePort(), node.nodeServiceMaxPort(), free);
        } catch (RuntimeException e) {
            throw new IllegalStateException("No free Mac port for the node service of a smolvm-pods node in "
                    + node.nodeServiceBasePort() + "-" + node.nodeServiceMaxPort() + ": widen "
                    + NodeSettings.key("node-service-base-port") + " to "
                    + NodeSettings.key("node-service-max-port"), e);
        }
    }

    private String allocateNodePorts() {
        String configured = config.runtime().node().nodePorts();
        Optional<PortRange> range = range(configured);
        if (range.isEmpty()) {
            return NodeDriver.NO_NODE_PORTS;
        }
        for (int port = range.get().first(); port <= range.get().last(); port++) {
            if (reservedNodePorts.contains(port) || !free.test(port)) {
                LOG.infov("The new node gets no NodePorts: port {0} of {1} is held by another node or process",
                        String.valueOf(port), configured);
                return NodeDriver.NO_NODE_PORTS;
            }
        }
        addNodePorts(range.get());
        return configured;
    }

    private void reserve(int apiPort, int servicePort, String nodePorts) {
        if (apiPort > 0) {
            portAllocator.markReserved(apiPort);
        }
        if (servicePort > 0) {
            portAllocator.markReserved(servicePort);
        }
        range(nodePorts).ifPresent(this::addNodePorts);
    }

    private void addNodePorts(PortRange range) {
        for (int port = range.first(); port <= range.last(); port++) {
            reservedNodePorts.add(port);
        }
    }

    private void releaseNodePorts(PortRange range) {
        for (int port = range.first(); port <= range.last(); port++) {
            reservedNodePorts.remove(port);
        }
    }

    private static boolean contains(PortRange range, int port) {
        return range.first() <= port && port <= range.last();
    }

    /** The ports of a {@code node-ports} value, or empty for {@code none} or a value that is not one. */
    private static Optional<PortRange> range(String nodePorts) {
        try {
            return NodeSettings.nodePorts(nodePorts);
        } catch (IllegalArgumentException e) {
            LOG.debugv("Ignoring NodePorts {0}: {1}", nodePorts, e.getMessage());
            return Optional.empty();
        }
    }
}
