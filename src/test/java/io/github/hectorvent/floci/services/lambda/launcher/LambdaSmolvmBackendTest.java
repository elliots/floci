package io.github.hectorvent.floci.services.lambda.launcher;

import com.github.dockerjava.api.DockerClient;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.core.common.docker.ContainerReachableEndpoint;
import io.github.hectorvent.floci.core.common.docker.OciRuntime;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode;
import io.github.hectorvent.floci.runtime.node.NodeApiException;
import io.github.hectorvent.floci.runtime.node.NodeDocker;
import io.github.hectorvent.floci.runtime.node.NodeToken;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient.WakeRequest;
import io.github.hectorvent.floci.services.cloudwatch.logs.CloudWatchLogsService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LambdaSmolvmBackendTest {

    private static final String CONTAINER = "c0ffee";

    private final EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
    private final SmolvmNodeClient nodeClient = mock(SmolvmNodeClient.class);
    private final ContainerReachableEndpoint endpoint = mock(ContainerReachableEndpoint.class);
    private LambdaSmolvmBackend backend;
    private LambdaSmolvmBackend.Target target;

    @BeforeEach
    void setUp() {
        when(config.services().lambda().smolvm().idleTimeout()).thenReturn(Optional.empty());
        when(config.hostname()).thenReturn(Optional.empty());
        when(endpoint.port()).thenReturn(4566);
        backend = new LambdaSmolvmBackend(config, mock(FlociNode.class), nodeClient, mock(NodeDocker.class),
                mock(DockerClient.class), mock(ImageCacheService.class), mock(ContainerDetector.class),
                mock(CloudWatchLogsService.class), endpoint);
        SmolvmNode node = new SmolvmNode("floci", URI.create("http://127.0.0.1:6600"), NodeToken.file(Path.of("token")),
                SmolvmNode.Kind.CREATED, List.of());
        DockerClient docker = mock(DockerClient.class);
        target = new LambdaSmolvmBackend.Target(node, new NodeDocker.Clients(node.baseUrl(), node.token(), docker, docker),
                mock(ContainerLifecycleManager.class), mock(ContainerLogStreamer.class));
    }

    @AfterEach
    void tearDown() {
        backend.close();
    }

    @Test
    void everyEnvironmentHibernatesWithItsConnectionsAndTheConfiguredIdleTimeout() {
        // reset-on-pause false: the runtime's wait on /runtime/invocation/next lasts through the pause.
        assertEquals(new OciRuntime("io.containerd.smolvm-pods.v2", Map.of(
                        "smolvm-pods.floci.io/hibernate", "true",
                        "smolvm-pods.floci.io/reset-on-pause", "false")),
                backend.runtime());

        when(config.services().lambda().smolvm().idleTimeout()).thenReturn(Optional.of(" 1m "));
        assertEquals(Map.of("smolvm-pods.floci.io/hibernate", "true", "smolvm-pods.floci.io/reset-on-pause", "false",
                        "smolvm-pods.floci.io/idle-timeout", "1m"),
                backend.runtime().annotations());

        when(config.services().lambda().smolvm().idleTimeout()).thenReturn(Optional.of("soon"));
        IllegalStateException refused = assertThrows(IllegalStateException.class, backend::runtime);
        assertThat(refused.getMessage(), containsString("FLOCI_SERVICES_LAMBDA_SMOLVM_IDLE_TIMEOUT"));
    }

    @Test
    void environmentsReachFlociByTheNodeAddressAndANameItsDnsAnswers() {
        assertThat(backend.runtimeApiHost(), is("169.254.53.1"));
        assertThat(backend.flociEndpoint(), is("http://localhost.floci.io:4566"));
        when(config.hostname()).thenReturn(Optional.of("floci.test"));
        assertThat(backend.flociEndpoint(), is("http://floci.test:4566"));
    }

    @Test
    void aHoldLeasesTheEnvironmentWakesItAndReleasesTheLease() {
        LambdaRuntimeLauncher.Hold hold = backend.hold(target, CONTAINER, "orders");

        ArgumentCaptor<String> lease = ArgumentCaptor.forClass(String.class);
        InOrder order = inOrder(nodeClient);
        order.verify(nodeClient).putLease(eq(target.node()), eq(CONTAINER), lease.capture(), eq(30),
                eq("floci lambda orders"), isNull());
        ArgumentCaptor<WakeRequest> wake = ArgumentCaptor.forClass(WakeRequest.class);
        order.verify(nodeClient).wake(eq(target.node()), eq(CONTAINER), wake.capture(), eq(Duration.ofSeconds(40)));
        assertThat(lease.getValue(), startsWith("floci-lambda-"));
        assertThat(wake.getValue().reason().kind(), is("api"));
        assertThat(wake.getValue().source(), is("lambda orders"));

        hold.close();
        order.verify(nodeClient).deleteLease(target.node(), CONTAINER, lease.getValue());
    }

    @Test
    void aFailedWakeReleasesTheLease() {
        when(nodeClient.wake(any(), anyString(), any(), any()))
                .thenThrow(new NodeApiException(NodeApiException.Code.FAILED, 409, "the pod failed", List.of()));

        assertThrows(NodeApiException.class, () -> backend.hold(target, CONTAINER, "orders"));

        verify(nodeClient).deleteLease(eq(target.node()), eq(CONTAINER), leaseId());
    }

    private static String leaseId() {
        return argThat(id -> id.startsWith("floci-lambda-"));
    }

    @Test
    void anExpiredLeaseIsHeldAgainAndTheEnvironmentWoken() {
        backend.renew(target.node(), CONTAINER, "floci-lambda-1", "floci lambda orders");
        verify(nodeClient).patchLease(target.node(), CONTAINER, "floci-lambda-1", 30, null);
        verify(nodeClient, never()).putLease(any(), anyString(), anyString(), anyInt(), any(), any());

        when(nodeClient.patchLease(any(), anyString(), anyString(), anyInt(), any()))
                .thenThrow(new NodeApiException(NodeApiException.Code.LEASE_NOT_FOUND, 404, "expired", List.of()));
        backend.renew(target.node(), CONTAINER, "floci-lambda-1", "floci lambda orders");

        verify(nodeClient).putLease(target.node(), CONTAINER, "floci-lambda-1", 30, "floci lambda orders", null);
        verify(nodeClient).wake(eq(target.node()), eq(CONTAINER), any(), any());
    }
}
