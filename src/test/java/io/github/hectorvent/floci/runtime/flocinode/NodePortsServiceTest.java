package io.github.hectorvent.floci.runtime.flocinode;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.RuntimeConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.RuntimeConfig.NodeConfig;
import io.github.hectorvent.floci.core.common.docker.HostBlindPortAllocator;
import io.github.hectorvent.floci.core.common.docker.PortAllocator;
import io.github.hectorvent.floci.runtime.flocinode.NodeDriver.Machine;
import io.github.hectorvent.floci.runtime.flocinode.NodePorts.Ports;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.StandardProtocolFamily;
import java.nio.channels.ServerSocketChannel;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NodePortsServiceTest {

    private static final String NODE_ID = "floci-node-0123456789abcdef";

    private final Set<Integer> taken = new HashSet<>();
    private NodeConfig smolvm;
    private EmulatorConfig config;
    private PortAllocator portAllocator;
    private NodePorts ports;

    @BeforeEach
    void setUp() {
        config = mock(EmulatorConfig.class);
        RuntimeConfig runtime = mock(RuntimeConfig.class);
        smolvm = mock(NodeConfig.class);
        when(config.runtime()).thenReturn(runtime);
        when(runtime.node()).thenReturn(smolvm);
        when(smolvm.apiServerBasePort()).thenReturn(6500);
        when(smolvm.apiServerMaxPort()).thenReturn(6502);
        when(smolvm.nodeServiceBasePort()).thenReturn(6600);
        when(smolvm.nodeServiceMaxPort()).thenReturn(6602);
        when(smolvm.nodePorts()).thenReturn("32080-32082");
        portAllocator = new HostBlindPortAllocator();
        ports = new NodePorts(config, portAllocator, port -> !taken.contains(port));
    }

    private static NodeRecord node(Ports allocated) {
        return TestNodeRecords.record(NODE_ID, allocated.apiPort(), allocated.servicePort(), allocated.nodePorts(),
                "ab".repeat(32), false, List.of());
    }

    @Test
    void allocate_givesEachNodeItsOwnPortsAndTheNodePortsToOneNode() {
        Ports first = ports.allocate();
        Ports second = ports.allocate();

        assertEquals(new Ports(6500, 6600, "32080-32082"), first);
        assertEquals(new Ports(6501, 6601, "none"), second);

        ports.release(node(first));

        assertEquals(new Ports(6500, 6600, "32080-32082"), ports.allocate());
    }

    @Test
    void allocate_skipsPortsTakenOnTheLoopback() {
        taken.addAll(List.of(6500, 6600, 32081));

        assertEquals(new Ports(6501, 6601, "none"), ports.allocate());

        taken.clear();
        assertEquals(new Ports(6500, 6600, "32080-32082"), ports.allocate());
    }

    @Test
    void allocate_withNoNodePortsConfigured_givesNone() {
        when(smolvm.nodePorts()).thenReturn("none");

        assertEquals(new Ports(6500, 6600, "none"), ports.allocate());
    }

    @Test
    void allocate_sharesTheApiRangeWithK3sClusters() {
        int k3sPort = portAllocator.allocate(6500, 6502);

        Ports smolvmNode = ports.allocate();

        assertEquals(6500, k3sPort);
        assertEquals(6501, smolvmNode.apiPort());
        assertEquals(6502, portAllocator.allocate(6500, 6502));
        assertThrows(RuntimeException.class, () -> portAllocator.allocate(6501, 6501));
    }

    @Test
    void allocate_whenARangeRunsOut_namesItsSettingsAndKeepsNothing() {
        taken.addAll(List.of(6500, 6501, 6502));
        IllegalStateException noApiPort = assertThrows(IllegalStateException.class, ports::allocate);
        assertThat(noApiPort.getMessage(), containsString("Kubernetes API of a smolvm-pods node in 6500-6502"));
        assertThat(noApiPort.getMessage(), containsString("floci.runtime.node.api-server-base-port"));

        taken.clear();
        taken.addAll(List.of(6600, 6601, 6602));
        IllegalStateException noServicePort = assertThrows(IllegalStateException.class, ports::allocate);
        assertThat(noServicePort.getMessage(), containsString("node service of a smolvm-pods node in 6600-6602"));
        assertThat(noServicePort.getMessage(), containsString("FLOCI_RUNTIME_NODE_NODE_SERVICE_BASE_PORT"));

        taken.clear();
        assertEquals(new Ports(6500, 6600, "32080-32082"), ports.allocate());
    }

    @Test
    void reserve_portsInMachineLabels_areNotGivenToNewNodes() {
        ports.reserve(List.of(
                new Machine("smolvm-pods-node", "stopped",
                        Map.of("api-port", "6500", "node-ports", "32080-32099")),
                new Machine("floci-node-fedcba9876543210", "running",
                        Map.of("api-port", "6501", "service-port", "6600", "node-ports", "none")),
                new Machine("broken", "created", Map.of("api-port", "x", "node-ports", "32099-32080"))));

        assertEquals(new Ports(6502, 6601, "none"), ports.allocate());
        assertThrows(RuntimeException.class, () -> portAllocator.allocate(6500, 6501),
                "k3s clusters skip them too");
    }

    @Test
    void reserve_aNodesRecord_keepsItsPorts() {
        ports.reserve(TestNodeRecords.record(NODE_ID, 6500, 6600, "32080-32082", "ab".repeat(32), true, List.of()));

        assertEquals(new Ports(6501, 6601, "none"), ports.allocate());
    }

    @Test
    void move_toTheMachineLabels_releasesThePortsOnlyTheRecordHas() {
        NodeRecord record = TestNodeRecords.record(NODE_ID, 6500, 6600, "32080-32082", "ab".repeat(32), false,
                List.of());
        NodeRecord labelled = record.withPorts(6502, 6600, "none");
        ports.reserve(record);

        ports.move(record, labelled);

        assertEquals(new Ports(6500, 6601, "32080-32082"), ports.allocate());
        assertEquals(new Ports(6501, 6602, "none"), ports.allocate());
    }

    @Test
    void reallocate_aClashedApiPort_isReplacedAndStaysReserved() {
        NodeRecord node = node(ports.allocate());

        NodeRecord moved = ports.reallocate(node, 6500);

        assertEquals(TestNodeRecords.record(NODE_ID, 6501, 6600, "32080-32082", "ab".repeat(32), false, List.of()), moved);
        ports.release(moved);
        assertEquals(new Ports(6501, 6600, "32080-32082"), ports.allocate());
    }

    @Test
    void reallocate_aClashedServicePort_isReplaced() {
        NodeRecord node = node(ports.allocate());

        NodeRecord moved = ports.reallocate(node, 6600);

        assertEquals(6500, moved.apiPort());
        assertEquals(6601, moved.servicePort());
        assertEquals("32080-32082", moved.nodePorts());
    }

    @Test
    void reallocate_aClashedNodePort_leavesTheNodeWithoutNodePorts() {
        NodeRecord node = node(ports.allocate());

        NodeRecord moved = ports.reallocate(node, 32081);

        assertEquals(6500, moved.apiPort());
        assertEquals(6600, moved.servicePort());
        assertEquals("none", moved.nodePorts());
        when(smolvm.nodePorts()).thenReturn("32080");
        assertEquals("32080", ports.allocate().nodePorts(), "the rest of the range is released");
        when(smolvm.nodePorts()).thenReturn("32081-32082");
        assertEquals("none", ports.allocate().nodePorts(), "the clashed port stays reserved");
    }

    @Test
    void reallocate_anUnnamedPort_replacesTheApiAndServicePortsAndReleasesTheOldOnes() {
        NodeRecord node = node(ports.allocate());

        NodeRecord moved = ports.reallocate(node, 0);

        assertEquals(6501, moved.apiPort());
        assertEquals(6601, moved.servicePort());
        assertEquals("32080-32082", moved.nodePorts());
        assertEquals(new Ports(6500, 6600, "none"), ports.allocate());
    }

    @Test
    void reallocate_whenTheServiceRangeRunsOut_releasesTheNewApiPort() {
        NodeRecord node = node(ports.allocate());
        taken.addAll(List.of(6601, 6602));

        assertThrows(IllegalStateException.class, () -> ports.reallocate(node, 0));

        assertEquals(6501, portAllocator.allocate(6501, 6501));
    }

    @Test
    void loopbackFree_aListenerOnIpv4LoopbackOnly_isTaken() throws IOException {
        int port;
        try (ServerSocket listener = new ServerSocket()) {
            listener.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
            port = listener.getLocalPort();

            assertFalse(NodePorts.loopbackFree(port));
            when(smolvm.apiServerBasePort()).thenReturn(port);
            when(smolvm.apiServerMaxPort()).thenReturn(port);
            NodePorts probing = new NodePorts(config, new HostBlindPortAllocator());
            assertThrows(IllegalStateException.class, probing::allocate);
        }
        assertTrue(NodePorts.loopbackFree(port));
    }

    @Test
    void loopbackFree_aListenerOnTheIpv4Wildcard_isTaken() throws IOException {
        try (ServerSocket listener = new ServerSocket()) {
            listener.bind(new InetSocketAddress(InetAddress.getByName("0.0.0.0"), 0));

            assertFalse(NodePorts.loopbackFree(listener.getLocalPort()));
        }
    }

    @Test
    void loopbackFree_aListenerOnIpv6LoopbackOnly_isTaken() throws IOException {
        try (ServerSocketChannel listener = ServerSocketChannel.open(StandardProtocolFamily.INET6)) {
            listener.bind(new InetSocketAddress(InetAddress.getByName("::1"), 0));
            int port = ((InetSocketAddress) listener.getLocalAddress()).getPort();

            assertFalse(NodePorts.loopbackFree(port));
        }
    }
}
