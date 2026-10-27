package io.github.hectorvent.floci.services.rds.container;

import com.github.dockerjava.api.model.Mount;
import com.github.dockerjava.api.model.MountType;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.common.ServiceConfigAccess;
import io.github.hectorvent.floci.core.common.dns.EmbeddedDnsServer;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLiveness;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.core.common.docker.ContainerSpec;
import io.github.hectorvent.floci.core.common.docker.DockerHostResolver;
import io.github.hectorvent.floci.runtime.node.NodeDocker;
import io.github.hectorvent.floci.runtime.node.NodeToken;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.services.rds.model.DatabaseEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * The database container that RDS creates on a smolvm-pods node: through the node's dockerd,
 * with its data in a named volume there, reached at its address on the node's network, and
 * with no Docker-pause auto-pause, since the node puts it to sleep.
 */
class RdsContainerManagerSmolvmTest {

    private static final String RUNTIME_ID = "arn:aws:rds:us-east-1:000000000000:db:db1";
    private static final String CONTAINER = "node-container";
    private static final String IP = "172.17.0.5";

    @TempDir
    Path tempDir;

    private final ContainerLifecycleManager local = mock(ContainerLifecycleManager.class);
    private final ContainerLifecycleManager onNode = mock(ContainerLifecycleManager.class);
    private final ContainerLogStreamer nodeLogs = mock(ContainerLogStreamer.class);
    private final RdsSmolvmBackend smolvm = mock(RdsSmolvmBackend.class);
    private RdsSmolvmBackend.Target target;
    private RdsContainerManager manager;

    @BeforeEach
    void setUp() {
        EmulatorConfig config = config(tempDir.resolve("host-root"));
        SmolvmNode node = new SmolvmNode("node:dev", URI.create("http://127.0.0.1:17070"),
                NodeToken.file(Path.of("token")), SmolvmNode.Kind.DECLARED, List.of());
        target = new RdsSmolvmBackend.Target(node, new NodeDocker.Clients(node.baseUrl(), node.token(), null, null),
                onNode, nodeLogs);
        when(smolvm.enabled()).thenReturn(true);
        when(smolvm.target()).thenReturn(target);
        when(onNode.create(any())).thenReturn(CONTAINER);
        when(onNode.startCreated(any(), any())).thenReturn(new ContainerLifecycleManager.ContainerInfo(
                CONTAINER, Map.of(3306, new ContainerLifecycleManager.EndpointInfo("localhost", 3306))));
        when(onNode.resolveContainerNetworkIp(CONTAINER, null)).thenReturn(IP);
        when(nodeLogs.generateLogStreamName(any())).thenReturn("log-stream");
        manager = new RdsContainerManager(
                new ContainerBuilder(config, mock(DockerHostResolver.class), mock(EmbeddedDnsServer.class)),
                local, mock(ContainerLogStreamer.class), mock(ContainerDetector.class), config,
                new RegionResolver("us-east-1", "000000000000"), mock(ServiceConfigAccess.class), smolvm);
    }

    @Test
    void theDatabaseRunsOnTheNodeWithItsDataInANamedVolumeThere() {
        RdsContainerHandle handle = manager.start(RUNTIME_ID, "db1", "db1", "floci-rds-db1",
                DatabaseEngine.MYSQL, "mysql:8.0.36", "root", "password", "db", Map.of());

        ArgumentCaptor<ContainerSpec> captured = ArgumentCaptor.forClass(ContainerSpec.class);
        InOrder order = inOrder(smolvm, onNode);
        order.verify(onNode).ensureVolume("floci-rds-db1");
        order.verify(smolvm).ensureImage(target, "mysql:8.0.36");
        order.verify(onNode).create(captured.capture());
        ContainerSpec spec = captured.getValue();
        assertThat(spec.exposedPorts(), contains(3306));
        assertThat(spec.portBindings().isEmpty(), is(true));
        assertThat(spec.networkMode(), nullValue());
        assertThat(spec.binds(), empty());
        Mount volume = spec.mounts().getFirst();
        assertEquals(MountType.VOLUME, volume.getType());
        assertEquals("floci-rds-db1", volume.getSource());
        assertEquals("/var/lib/mysql", volume.getTarget());
        assertEquals(IP, handle.getHost());
        assertEquals(3306, handle.getPort());
        manager.exportLogs(RUNTIME_ID, new RdsLogExport("000000000000", "us-east-1", List.of()));
        verify(nodeLogs).attachToDestinations(any(), anyString(), anyList(), anyString(), anyString(), any());
        verifyNoInteractions(local);
    }

    @Test
    void aDatabaseOnTheNodeHasNoDockerPause() throws InterruptedException {
        manager.start(RUNTIME_ID, "db1", "db1", "floci-rds-db1",
                DatabaseEngine.MYSQL, "mysql:8.0.36", "root", "password", "db", Map.of());

        manager.configureAutoPause(RUNTIME_ID, 300, mock(AutoPauseListener.class));

        assertFalse(manager.pauseIfIdle(RUNTIME_ID));
        assertSame(RdsBackendGate.Lease.NONE, manager.enterRuntime(RUNTIME_ID));
    }

    @Test
    void theProxyReachesTheDatabaseAtItsAddressOnTheNode() {
        manager.start(RUNTIME_ID, "db1", "db1", "floci-rds-db1",
                DatabaseEngine.MYSQL, "mysql:8.0.36", "root", "password", "db", Map.of());

        assertEquals(new ContainerLifecycleManager.EndpointInfo(IP, 3306),
                manager.resolveBackendEndpoint(RUNTIME_ID, DatabaseEngine.MYSQL));
    }

    @Test
    void livenessComesFromTheNode() {
        when(smolvm.probe(target, CONTAINER)).thenReturn(ContainerLiveness.RUNNING);

        assertEquals(ContainerLiveness.RUNNING, manager.probeContainer(CONTAINER));
        verifyNoInteractions(local);
    }

    @Test
    void withoutANodeACreateFailsWithTheOperationsError() {
        when(smolvm.place("RDS cluster c1", "000000000000", "us-east-1"))
                .thenThrow(new RdsSmolvmBackend.NoNodeException("Floci cannot create the smolvm-pods node"));

        AwsException e = assertThrows(AwsException.class,
                () -> manager.place("InsufficientDBClusterCapacityFault", "RDS cluster c1", "000000000000", "us-east-1"));

        assertEquals("InsufficientDBClusterCapacityFault", e.getErrorCode());
        assertEquals(400, e.getHttpStatus());
    }

    @Test
    void aCreateWaitsForANodeThatIsBeingCreated() {
        when(smolvm.place("RDS instance db1", "000000000000", "us-east-1")).thenReturn(false);
        when(smolvm.whenReady()).thenReturn(CompletableFuture.completedFuture(null));
        CountDownLatch ready = new CountDownLatch(1);

        assertFalse(manager.place("InsufficientDBInstanceCapacity", "RDS instance db1", "000000000000", "us-east-1"));
        manager.whenNodeReady(ready::countDown, failure -> { });

        assertDoesNotThrow(() -> assertTrue(ready.await(5, TimeUnit.SECONDS)));
    }

    @Test
    void aNodeThatFailsTellsWhyToThoseWhoWait() throws InterruptedException {
        when(smolvm.whenReady()).thenReturn(CompletableFuture.failedFuture(
                new IllegalStateException("node-vm.sh create failed")));
        AtomicReference<String> reason = new AtomicReference<>();
        CountDownLatch failed = new CountDownLatch(1);

        manager.whenNodeReady(() -> { }, failure -> {
            reason.set(failure);
            failed.countDown();
        });

        assertTrue(failed.await(5, TimeUnit.SECONDS));
        assertEquals("node-vm.sh create failed", reason.get());
    }

    @Test
    void withoutTheNodeEveryCreateStartsAtOnce() {
        when(smolvm.enabled()).thenReturn(false);

        assertTrue(manager.place("InsufficientDBInstanceCapacity", "RDS instance db1", "000000000000", "us-east-1"));
        verify(smolvm, never()).place(anyString(), anyString(), anyString());
    }

    @Test
    void aStartWithoutANodeFailsWithInsufficientCapacity() {
        when(smolvm.target()).thenAnswer(invocation -> {
            throw new RdsSmolvmBackend.NoNodeException("Floci knows no node");
        });

        AwsException e = assertThrows(AwsException.class, () -> manager.tryStart(RUNTIME_ID, "db1", "db1",
                "floci-rds-db1", DatabaseEngine.MYSQL, "mysql:8.0.36", "root", "password", "db", Map.of()));

        assertEquals("InsufficientDBInstanceCapacity", e.getErrorCode());
        assertThat(manager.getActiveHandle(RUNTIME_ID), nullValue());
        verifyNoInteractions(onNode, local);
    }

    private static EmulatorConfig config(Path hostRoot) {
        EmulatorConfig config = mock(EmulatorConfig.class);
        EmulatorConfig.ServicesConfig services = mock(EmulatorConfig.ServicesConfig.class);
        EmulatorConfig.RdsServiceConfig rds = mock(EmulatorConfig.RdsServiceConfig.class);
        EmulatorConfig.DockerConfig docker = mock(EmulatorConfig.DockerConfig.class);
        EmulatorConfig.StorageConfig storage = mock(EmulatorConfig.StorageConfig.class);
        when(config.services()).thenReturn(services);
        when(services.rds()).thenReturn(rds);
        when(services.dockerNetwork()).thenReturn(Optional.empty());
        when(rds.dockerNetwork()).thenReturn(Optional.empty());
        when(config.docker()).thenReturn(docker);
        when(docker.resourceNamespace()).thenReturn(Optional.empty());
        when(docker.logMaxSize()).thenReturn("10m");
        when(docker.logMaxFile()).thenReturn("3");
        when(config.storage()).thenReturn(storage);
        when(storage.hostPersistentPath()).thenReturn(hostRoot.toString());
        when(storage.mode()).thenReturn("persistent");
        return config;
    }
}
