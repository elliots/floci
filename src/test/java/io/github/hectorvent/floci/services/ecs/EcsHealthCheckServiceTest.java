package io.github.hectorvent.floci.services.ecs;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.services.ecs.container.EcsContainerManager;
import io.github.hectorvent.floci.services.ecs.container.EcsTaskHandle;
import io.github.hectorvent.floci.services.ecs.model.Container;
import io.github.hectorvent.floci.services.ecs.model.ContainerDefinition;
import io.github.hectorvent.floci.services.ecs.model.EcsTask;
import io.github.hectorvent.floci.services.ecs.model.HealthCheck;
import io.github.hectorvent.floci.services.ecs.model.LaunchType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class EcsHealthCheckServiceTest {
    @Test
    void essentialExitStopsARunningSidecarButNonessentialExitDoesNot() {
        EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        EcsContainerManager containers = mock(EcsContainerManager.class);
        EcsService service = new EcsService(new RegionResolver("us-east-1", "000000000000"),
                containers, config, mock(EcsLoadBalancerRegistrar.class), null, null);
        service.createCluster("essential", "us-east-1");
        ContainerDefinition app = new ContainerDefinition();
        app.setName("app");
        app.setImage("busybox:stable");
        ContainerDefinition sidecar = new ContainerDefinition();
        sidecar.setName("sidecar");
        sidecar.setImage("busybox:stable");
        sidecar.setEssential(false);
        service.registerTaskDefinition("essential", List.of(app, sidecar), null, null, null, null, null, List.of(), "us-east-1");
        when(containers.startTask(any(), any(), any(), anyString())).thenAnswer(invocation -> {
            EcsTask task = invocation.getArgument(0);
            return new EcsTaskHandle(task.getTaskArn(), Map.of("app", "app-id", "sidecar", "sidecar-id"), Map.of());
        });
        when(containers.stopTaskAndCollectExitCodes(any())).thenAnswer(invocation -> {
            EcsTaskHandle handle = invocation.getArgument(0);
            handle.getContainerIds().keySet().forEach(handle::recordContainerRemoved);
            return Map.of("app", 1, "sidecar", 143);
        });
        EcsTask task = service.runTask("essential", "essential", 1, LaunchType.EC2,
                null, null, null, null, "us-east-1").getFirst();
        when(containers.getExitCodeIfStopped(anyString())).thenReturn(null);
        when(containers.getExitCodeIfStopped("sidecar-id")).thenReturn(0);
        service.reconcile();
        assertEquals("RUNNING", task.getLastStatus());
        when(containers.getExitCodeIfStopped("sidecar-id")).thenReturn(null);
        when(containers.getExitCodeIfStopped("app-id")).thenReturn(1);
        service.reconcile();
        assertEquals("STOPPED", task.getLastStatus());
        assertEquals("EssentialContainerExited", task.getStopCode());
        verify(containers).stopTaskAndCollectExitCodes(any());
    }

    @Test
    void taskHealthAggregatesOnlyEssentialDeclaredChecksAndWaitsForEveryCheck() {
        EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.services().ecs().mock()).thenReturn(false);
        EcsContainerManager containers = mock(EcsContainerManager.class);
        EcsService service = new EcsService(new RegionResolver("us-east-1", "000000000000"),
                containers, config, mock(EcsLoadBalancerRegistrar.class), null, null);
        service.createCluster("health", "us-east-1");
        List<ContainerDefinition> definitions = new ArrayList<>();
        for (String name : List.of("first", "second", "optional", "image-only")) {
            ContainerDefinition definition = new ContainerDefinition();
            definition.setName(name);
            definition.setImage("app:1");
            definition.setEssential(!"optional".equals(name));
            if (!"image-only".equals(name)) {
                definition.setHealthCheck(new HealthCheck(List.of("CMD", "true"), 5, 2, 3, null));
            }
            definitions.add(definition);
        }
        service.registerTaskDefinition("health", definitions, null, null, null, null, null, List.of(), "us-east-1");
        when(containers.startTask(any(), any(), any(), anyString())).thenAnswer(invocation -> {
            EcsTask task = invocation.getArgument(0);
            List<Container> models = new ArrayList<>();
            for (ContainerDefinition definition : definitions) {
                Container model = new Container();
                model.setName(definition.getName());
                models.add(model);
            }
            task.setContainers(models);
            return new EcsTaskHandle(task.getTaskArn(), Map.of("first", "first-id", "second", "second-id",
                    "optional", "optional-id", "image-only", "image-id"), Map.of());
        });
        when(containers.getExitCodeIfStopped(anyString())).thenReturn(null);
        when(containers.ecsHealthStatus("first-id")).thenReturn("HEALTHY");
        when(containers.ecsHealthStatus("second-id")).thenReturn("UNKNOWN");
        when(containers.ecsHealthStatus("optional-id")).thenReturn("UNHEALTHY");
        EcsTask task = service.runTask("health", "health", 1, LaunchType.EC2,
                null, null, null, null, "us-east-1").getFirst();
        service.reconcile();
        assertEquals("UNKNOWN", task.getHealthStatus());
        when(containers.ecsHealthStatus("second-id")).thenReturn("HEALTHY");
        service.reconcile();
        assertEquals("HEALTHY", task.getHealthStatus());
        assertNull(task.getContainers().stream().filter(container -> "image-only".equals(container.getName()))
                .findFirst().orElseThrow().getHealthStatus());
        verify(containers, never()).ecsHealthStatus("image-id");
        when(containers.ecsHealthStatus("first-id")).thenReturn("UNHEALTHY");
        service.reconcile();
        assertEquals("UNHEALTHY", task.getHealthStatus());
    }
}
