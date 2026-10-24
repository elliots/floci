package io.github.hectorvent.floci.core.common.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.HostConfig;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.services.lambda.launcher.ImageCacheService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A manager built with an {@link OciRuntime} creates every container with that runtime and its
 * annotations; the CDI manager leaves both to the daemon.
 */
@ExtendWith(MockitoExtension.class)
class ContainerLifecycleManagerRuntimeTest {

    @Mock
    DockerClient dockerClient;

    @Mock
    ImageCacheService imageCacheService;

    @Mock
    ContainerDetector containerDetector;

    @Mock
    EmulatorConfig config;

    @Mock
    EmulatorConfig.DockerConfig dockerConfig;

    @Mock
    EmulatorConfig.TlsConfig tlsConfig;

    @BeforeEach
    void setUp() {
        lenient().when(config.docker()).thenReturn(dockerConfig);
        lenient().when(config.tls()).thenReturn(tlsConfig);
        lenient().when(dockerConfig.resourceNamespace()).thenReturn(Optional.empty());
        lenient().when(imageCacheService.ensureImageExists(any()))
                .thenAnswer(invocation -> invocation.getArgument(0));
    }

    @Test
    void aManagerWithARuntimeCreatesEveryContainerWithIt() {
        CreateContainerCmd createCmd = stubCreateContainer();
        OciRuntime runtime = new OciRuntime("io.containerd.smolvm-pods.v2",
                Map.of("smolvm-pods.floci.io/hibernate", "true", "smolvm-pods.floci.io/idle-timeout", "2m"));

        new ContainerLifecycleManager(dockerClient, imageCacheService, containerDetector, config, runtime)
                .create(new ContainerSpec("postgres:16-alpine"));

        HostConfig hostConfig = capturedHostConfig(createCmd);
        assertEquals("io.containerd.smolvm-pods.v2", hostConfig.getRuntime());
        assertEquals(Map.of("smolvm-pods.floci.io/hibernate", "true", "smolvm-pods.floci.io/idle-timeout", "2m"),
                hostConfig.getAnnotations());
    }

    @Test
    void theCdiManagerLeavesTheRuntimeToTheDaemon() {
        CreateContainerCmd createCmd = stubCreateContainer();

        new ContainerLifecycleManager(dockerClient, imageCacheService, containerDetector, config)
                .create(new ContainerSpec("postgres:16-alpine"));

        HostConfig hostConfig = capturedHostConfig(createCmd);
        assertNull(hostConfig.getRuntime());
        assertNull(hostConfig.getAnnotations());
    }

    private CreateContainerCmd stubCreateContainer() {
        CreateContainerCmd createCmd = mock(CreateContainerCmd.class, RETURNS_SELF);
        when(dockerClient.createContainerCmd("postgres:16-alpine")).thenReturn(createCmd);
        CreateContainerResponse response = mock(CreateContainerResponse.class);
        when(response.getId()).thenReturn("container-id");
        when(createCmd.exec()).thenReturn(response);
        return createCmd;
    }

    private static HostConfig capturedHostConfig(CreateContainerCmd createCmd) {
        ArgumentCaptor<HostConfig> hostConfig = ArgumentCaptor.forClass(HostConfig.class);
        verify(createCmd).withHostConfig(hostConfig.capture());
        return hostConfig.getValue();
    }
}
