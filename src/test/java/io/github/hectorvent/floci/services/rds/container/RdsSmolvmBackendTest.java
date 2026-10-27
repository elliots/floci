package io.github.hectorvent.floci.services.rds.container;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectImageCmd;
import com.github.dockerjava.api.command.LoadImageCmd;
import com.github.dockerjava.api.command.SaveImageCmd;
import com.github.dockerjava.api.exception.NotFoundException;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLiveness;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.core.common.docker.OciRuntime;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode.State;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode.Status;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode.Use;
import io.github.hectorvent.floci.runtime.node.NodeApiException;
import io.github.hectorvent.floci.runtime.node.NodeDocker;
import io.github.hectorvent.floci.runtime.node.NodePod;
import io.github.hectorvent.floci.runtime.node.NodeToken;
import io.github.hectorvent.floci.runtime.node.NodeTunnel;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import io.github.hectorvent.floci.services.cloudwatch.logs.CloudWatchLogsService;
import io.github.hectorvent.floci.services.lambda.launcher.ImageCacheService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class RdsSmolvmBackendTest {

    private static final String CONTAINER = "c0ffee";
    private static final String IMAGE = "postgres:16-alpine";

    private final EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
    private final FlociNode flociNode = mock(FlociNode.class);
    private final SmolvmNodeClient nodeClient = mock(SmolvmNodeClient.class);
    private final DockerClient localDocker = mock(DockerClient.class, RETURNS_DEEP_STUBS);
    private final ImageCacheService localImages = mock(ImageCacheService.class);
    private final DockerClient nodeDocker = mock(DockerClient.class);
    private final ContainerLifecycleManager lifecycle = mock(ContainerLifecycleManager.class);
    private final NodeDocker docker = mock(NodeDocker.class);
    private RdsSmolvmBackend backend;
    private RdsSmolvmBackend.Target target;

    @BeforeEach
    void setUp() {
        when(config.services().rds().smolvm().enabled()).thenReturn(true);
        when(config.services().rds().smolvm().idleTimeout()).thenReturn(Optional.empty());
        backend = new RdsSmolvmBackend(config, flociNode, nodeClient, docker, mock(NodeTunnel.class),
                localDocker, localImages, mock(ContainerDetector.class), mock(CloudWatchLogsService.class));
        SmolvmNode node = node("node:dev");
        target = new RdsSmolvmBackend.Target(node,
                new NodeDocker.Clients(node.baseUrl(), node.token(), nodeDocker, nodeDocker),
                lifecycle, mock(ContainerLogStreamer.class));
    }

    private static SmolvmNode node(String name) {
        return new SmolvmNode(name, URI.create("http://127.0.0.1:17070"), NodeToken.file(Path.of("token")),
                name.startsWith("node:") ? SmolvmNode.Kind.DECLARED : SmolvmNode.Kind.CREATED, List.of());
    }

    @Test
    void theNodeIsTheFlociNodeOnceItIsReady() {
        when(flociNode.status()).thenReturn(new Status(State.NONE, null, null, null, false));
        assertThat(assertThrows(RdsSmolvmBackend.NoNodeException.class, backend::node).getMessage(),
                containsString("Floci has no smolvm-pods node for RDS's databases yet"));

        when(flociNode.status()).thenReturn(new Status(State.CREATING, node("floci"), null, null, false));
        assertThat(assertThrows(RdsSmolvmBackend.NoNodeException.class, backend::node).getMessage(),
                containsString("still being created"));

        when(flociNode.status()).thenReturn(new Status(State.FAILED, null, null, "node-vm.sh create failed", false));
        assertThat(assertThrows(RdsSmolvmBackend.NoNodeException.class, backend::node).getMessage(),
                containsString("failed: node-vm.sh create failed"));

        when(flociNode.status()).thenReturn(new Status(State.READY, node("node:dev"), null, null, true));
        assertEquals("node:dev", backend.node().name());
    }

    @Test
    void aNewDatabaseHasFlociCreateTheNodeAndWaitsWhileItIsCreated() {
        when(flociNode.acquire(any())).thenReturn(new Status(State.CREATING, node("floci"), null, null, false));

        assertFalse(backend.place("RDS instance orders", "123456789012", "eu-west-1"));

        ArgumentCaptor<Use> use = ArgumentCaptor.forClass(Use.class);
        verify(flociNode).acquire(use.capture());
        assertEquals(Use.of("RDS instance orders", "123456789012", "eu-west-1"), use.getValue());
        when(flociNode.acquire(any())).thenReturn(new Status(State.READY, node("floci"), null, null, false));
        assertFalse(backend.place("RDS instance billing", "123456789012", "eu-west-1"), "dockerd does not answer");

        SmolvmNode ready = node("floci");
        DockerClient answering = mock(DockerClient.class, RETURNS_DEEP_STUBS);
        when(docker.of(ready)).thenReturn(new NodeDocker.Clients(ready.baseUrl(), ready.token(), answering, answering));
        assertTrue(backend.place("RDS instance billing", "123456789012", "eu-west-1"));
    }

    @Test
    void aNodeThatCannotBeCreatedFailsThePlacementWithItsReason() {
        when(flociNode.acquire(any())).thenThrow(new FlociNode.NodeUnavailableException(
                "Floci cannot create the smolvm-pods node for RDS instance orders: floci.runtime.node.image is not set"));

        RdsSmolvmBackend.NoNodeException none = assertThrows(RdsSmolvmBackend.NoNodeException.class,
                () -> backend.place("RDS instance orders", "123456789012", "eu-west-1"));

        assertThat(none.getMessage(), containsString("floci.runtime.node.image is not set"));
    }

    @Test
    void everyDatabaseHibernatesWithTheIdleTimeoutOfTheSetting() {
        assertEquals(new OciRuntime("io.containerd.smolvm-pods.v2", Map.of("smolvm-pods.floci.io/hibernate", "true")),
                backend.runtime());

        when(config.services().rds().smolvm().idleTimeout()).thenReturn(Optional.of("2m"));
        assertEquals(Map.of("smolvm-pods.floci.io/hibernate", "true", "smolvm-pods.floci.io/idle-timeout", "2m"),
                backend.runtime().annotations());

        when(config.services().rds().smolvm().idleTimeout()).thenReturn(Optional.of("soon"));
        RdsSmolvmBackend.NoNodeException e = assertThrows(RdsSmolvmBackend.NoNodeException.class, backend::runtime);
        assertThat(e.getMessage(), containsString("FLOCI_SERVICES_RDS_SMOLVM_IDLE_TIMEOUT"));
    }

    @Test
    void aSleepingDatabaseIsRunningAndDockerIsNotAsked() {
        when(nodeClient.getPod(target.node(), CONTAINER)).thenReturn(pod(NodePod.State.ASLEEP));

        assertEquals(ContainerLiveness.RUNNING, backend.probe(target, CONTAINER));
        verifyNoInteractions(lifecycle);
    }

    @Test
    void aFailedPodIsNotRunning() {
        when(nodeClient.getPod(target.node(), CONTAINER)).thenReturn(pod(NodePod.State.FAILED));

        assertEquals(ContainerLiveness.NOT_RUNNING, backend.probe(target, CONTAINER));
    }

    @Test
    void dockerAnswersForAContainerWithoutAPod() {
        when(nodeClient.getPod(target.node(), CONTAINER)).thenThrow(
                new NodeApiException(NodeApiException.Code.NOT_FOUND, 404, "no such pod", List.of()));
        when(lifecycle.probeContainer(CONTAINER)).thenReturn(ContainerLiveness.NOT_RUNNING);

        assertEquals(ContainerLiveness.NOT_RUNNING, backend.probe(target, CONTAINER));
    }

    @Test
    void anUnreachableNodeLeavesTheStateUnknown() {
        when(nodeClient.getPod(target.node(), CONTAINER)).thenThrow(
                new NodeApiException(NodeApiException.Code.UNREACHABLE, "no answer", null));

        assertEquals(ContainerLiveness.UNKNOWN, backend.probe(target, CONTAINER));
        verifyNoInteractions(lifecycle);
    }

    @Test
    void anImageTheNodeHasIsNotLoaded() {
        when(nodeDocker.inspectImageCmd(IMAGE)).thenReturn(mock(InspectImageCmd.class));

        backend.ensureImage(target, IMAGE);

        verifyNoInteractions(localImages, localDocker);
    }

    @Test
    void anImageTheNodeLacksIsLoadedFromFlocisDocker() {
        InspectImageCmd inspect = mock(InspectImageCmd.class);
        when(inspect.exec()).thenThrow(new NotFoundException("no such image"));
        when(nodeDocker.inspectImageCmd(IMAGE)).thenReturn(inspect);
        InputStream archive = new ByteArrayInputStream(new byte[] {1, 2, 3});
        SaveImageCmd save = mock(SaveImageCmd.class);
        when(save.exec()).thenReturn(archive);
        when(localDocker.saveImageCmd(IMAGE)).thenReturn(save);
        LoadImageCmd load = mock(LoadImageCmd.class);
        when(nodeDocker.loadImageCmd(archive)).thenReturn(load);

        backend.ensureImage(target, IMAGE);

        verify(localImages).ensureImageExists(IMAGE);
        verify(load).exec();
    }

    @Test
    void anImageFlocisDockerCannotGiveIsLeftToTheNodesPull() {
        InspectImageCmd inspect = mock(InspectImageCmd.class);
        when(inspect.exec()).thenThrow(new NotFoundException("no such image"));
        when(nodeDocker.inspectImageCmd(IMAGE)).thenReturn(inspect);
        when(localImages.ensureImageExists(IMAGE)).thenThrow(new IllegalStateException("no Docker daemon"));

        backend.ensureImage(target, IMAGE);

        verify(nodeDocker, never()).loadImageCmd(any(InputStream.class));
    }

    private static NodePod pod(NodePod.State state) {
        return new NodePod(CONTAINER, "", CONTAINER, CONTAINER, "172.17.0.2", state, null, 3, "inc", true,
                List.of(), Map.of(), null);
    }
}
