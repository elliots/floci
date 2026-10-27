package io.github.hectorvent.floci.services.elasticache.container;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectImageCmd;
import com.github.dockerjava.api.command.LoadImageCmd;
import com.github.dockerjava.api.command.SaveImageCmd;
import com.github.dockerjava.api.exception.NotFoundException;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode;
import io.github.hectorvent.floci.runtime.node.NodeDocker;
import io.github.hectorvent.floci.runtime.node.NodeToken;
import io.github.hectorvent.floci.runtime.node.NodeTunnel;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.services.cloudwatch.logs.CloudWatchLogsService;
import io.github.hectorvent.floci.services.lambda.launcher.ImageCacheService;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ElastiCacheSmolvmBackendTest {

    private final EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
    private final DockerClient host = mock(DockerClient.class);
    private final DockerClient nodeDocker = mock(DockerClient.class);
    private final ImageCacheService hostImages = mock(ImageCacheService.class);
    private final ElastiCacheSmolvmBackend backend = new ElastiCacheSmolvmBackend(config, mock(FlociNode.class),
            mock(NodeDocker.class), mock(NodeTunnel.class), host, hostImages, mock(ContainerDetector.class),
            mock(CloudWatchLogsService.class));
    private final SmolvmNode node = new SmolvmNode("node", URI.create("http://127.0.0.1:1234"),
            NodeToken.file(Path.of("token")), SmolvmNode.Kind.DECLARED, List.of());
    private final ElastiCacheSmolvmBackend.Target target = new ElastiCacheSmolvmBackend.Target(node,
            new NodeDocker.Clients(node.baseUrl(), node.token(), nodeDocker, nodeDocker),
            mock(ContainerLifecycleManager.class), mock(ContainerLogStreamer.class));

    @Test
    void anImageAlreadyOnTheNodeDoesNotNeedHostDocker() {
        InspectImageCmd inspect = mock(InspectImageCmd.class);
        when(nodeDocker.inspectImageCmd("valkey:8")).thenReturn(inspect);

        backend.ensureImage(target, "valkey:8");

        verifyNoInteractions(host, hostImages);
    }

    @Test
    void aMissingImageCanBeTransferredFromHostDocker() {
        InspectImageCmd inspect = mock(InspectImageCmd.class);
        when(nodeDocker.inspectImageCmd("valkey:8")).thenReturn(inspect);
        when(inspect.exec()).thenThrow(new NotFoundException("missing"));
        SaveImageCmd save = mock(SaveImageCmd.class);
        LoadImageCmd load = mock(LoadImageCmd.class);
        ByteArrayInputStream archive = new ByteArrayInputStream(new byte[] {1, 2, 3});
        when(host.saveImageCmd("valkey:8")).thenReturn(save);
        when(save.exec()).thenReturn(archive);
        when(nodeDocker.loadImageCmd(any())).thenReturn(load);

        backend.ensureImage(target, "valkey:8");

        verify(hostImages).ensureImageExists("valkey:8");
        verify(nodeDocker).loadImageCmd(archive);
        verify(load).exec();
    }

    @Test
    void podRuntimeUsesTheConfiguredIdleTimeout() {
        when(config.services().elasticache().smolvm().idleTimeout()).thenReturn(Optional.of("2m"));
        assertEquals(Map.of("smolvm-pods.floci.io/hibernate", "true",
                "smolvm-pods.floci.io/idle-timeout", "2m"), backend.runtime().annotations());
        when(config.services().elasticache().smolvm().idleTimeout()).thenReturn(Optional.of("bad"));
        assertThrows(IllegalArgumentException.class, backend::runtime);
    }
}
