package io.github.hectorvent.floci.core.common.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.HealthCheck;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.dns.EmbeddedDnsServer;
import io.github.hectorvent.floci.services.lambda.launcher.ImageCacheService;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ContainerLifecycleManagerHealthCheckServiceTest {
    @Test
    void declaredHealthCheckReachesTheDockerCreateContract() {
        EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.docker().resourceNamespace()).thenReturn(Optional.empty());
        ContainerBuilder builder = new ContainerBuilder(config, mock(DockerHostResolver.class), mock(EmbeddedDnsServer.class));
        HealthCheck health = new HealthCheck().withTest(List.of("CMD-SHELL", "curl -f localhost:8080/ready"))
                .withInterval(Duration.ofSeconds(5).toNanos()).withTimeout(Duration.ofSeconds(2).toNanos())
                .withRetries(3).withStartPeriod(Duration.ofSeconds(10).toNanos());
        ContainerSpec spec = builder.newContainer("app:1").withHealthCheck(health).build();
        assertSame(health, spec.healthCheck());

        DockerClient docker = mock(DockerClient.class);
        CreateContainerCmd create = mock(CreateContainerCmd.class, RETURNS_SELF);
        CreateContainerResponse response = mock(CreateContainerResponse.class);
        when(response.getId()).thenReturn("container-id");
        when(create.exec()).thenReturn(response);
        when(docker.createContainerCmd("app:1")).thenReturn(create);
        ImageCacheService images = mock(ImageCacheService.class);
        when(images.ensureImageExists(any())).thenAnswer(invocation -> invocation.getArgument(0));
        ContainerLifecycleManager manager = new ContainerLifecycleManager(docker, images,
                mock(ContainerDetector.class), mock(PortAllocator.class), config);
        manager.create(spec);
        verify(create).withHealthcheck(health);
        manager.create(new ContainerSpec("app:1"));
        verify(create, times(1)).withHealthcheck(any());
    }
}
