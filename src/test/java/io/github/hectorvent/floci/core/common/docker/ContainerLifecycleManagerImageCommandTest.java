package io.github.hectorvent.floci.core.common.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectImageCmd;
import com.github.dockerjava.api.command.InspectImageResponse;
import com.github.dockerjava.api.model.ContainerConfig;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.services.lambda.launcher.ImageCacheService;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ContainerLifecycleManagerImageCommandTest {

    @Test
    void preparesTheRequestedPlatformBeforeInspectingTheResolvedImage() {
        DockerClient docker = mock(DockerClient.class);
        ImageCacheService images = mock(ImageCacheService.class);
        InspectImageCmd inspect = mock(InspectImageCmd.class);
        InspectImageResponse response = mock(InspectImageResponse.class);
        when(images.ensureImageExists("example/function:1", "linux/arm64")).thenReturn("sha256:resolved");
        when(docker.inspectImageCmd("sha256:resolved")).thenReturn(inspect);
        when(inspect.exec()).thenReturn(response);
        when(response.getConfig()).thenReturn(new ContainerConfig().withCmd(new String[]{"app.handler"}));
        ContainerLifecycleManager lifecycle = new ContainerLifecycleManager(docker, images,
                mock(ContainerDetector.class), mock(EmulatorConfig.class));

        assertEquals(List.of("app.handler"), lifecycle.imageCommand("example/function:1", "linux/arm64"));

        InOrder order = inOrder(images, docker);
        order.verify(images).ensureImageExists("example/function:1", "linux/arm64");
        order.verify(docker).inspectImageCmd("sha256:resolved");
    }
}
