package io.github.hectorvent.floci.runtime.ondemand;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotFoundException;
import io.github.hectorvent.floci.services.ecs.EcsService;
import io.github.hectorvent.floci.services.ecs.model.AwsVpcConfiguration;
import io.github.hectorvent.floci.services.ecs.model.ContainerDefinition;
import io.github.hectorvent.floci.services.ecs.model.EcsServiceModel;
import io.github.hectorvent.floci.services.ecs.model.EcsTask;
import io.github.hectorvent.floci.services.ecs.model.LaunchType;
import io.github.hectorvent.floci.services.ecs.model.NetworkConfiguration;
import io.github.hectorvent.floci.services.ecs.model.NetworkMode;
import io.github.hectorvent.floci.services.ecs.model.RegisterTaskDefinitionRequest;
import io.github.hectorvent.floci.services.ecs.model.TaskDefinition;
import io.github.hectorvent.floci.testing.TestImages;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(OnDemandEcsDockerIntegrationTest.Profile.class)
class OnDemandEcsDockerIntegrationTest {
    private static final Logger LOG = Logger.getLogger(OnDemandEcsDockerIntegrationTest.class);
    private static final String REGION = "us-east-1";

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("floci.services.ecs.mock", "false",
                    "floci.network.security-group-enforcement.enabled", "false",
                    "floci.docker.resource-namespace", "on-demand-ecs-test");
        }
    }

    @Inject
    DockerClient docker;

    @Inject
    EcsService ecs;

    @Inject
    WorkloadRuntimeFactory factory;

    private String cluster;
    private TaskDefinition definition;
    private EcsServiceModel service;

    @BeforeEach
    void requireDocker() {
        boolean available;
        try {
            docker.pingCmd().exec();
            available = true;
        } catch (RuntimeException e) {
            available = false;
            LOG.warnv("Docker daemon unavailable for on-demand ECS test: {0}", e.getMessage());
        }
        Assumptions.assumeTrue(available, "Docker daemon must be available for on-demand ECS tests");
    }

    @AfterEach
    void tearDown() {
        if (service != null) {
            ecs.deleteService(cluster, "worker", true, REGION);
            for (EcsTask task : ecs.tasksForService(service)) {
                ecs.stopTask(cluster, task.getTaskArn(), "test cleanup", REGION);
            }
        }
        if (definition != null) {
            ecs.deregisterTaskDefinition(definition.getTaskDefinitionArn(), REGION);
        }
        if (cluster != null) {
            ecs.deleteCluster(cluster, REGION);
        }
    }

    @ParameterizedTest
    @EnumSource(value = LaunchType.class, names = {"EC2", "FARGATE"})
    void ecsServiceStartsRealContainersDrainsThemAndCreatesNewTasksAfterIdle(LaunchType launchType,
                                                                          @TempDir Path directory) throws Exception {
        cluster = "on-demand-" + UUID.randomUUID().toString().substring(0, 8);
        ecs.createCluster(cluster, REGION);
        ContainerDefinition container = new ContainerDefinition();
        container.setName("worker");
        container.setImage(TestImages.BUSYBOX);
        container.setCommand(List.of("sleep", "3600"));
        NetworkConfiguration network = null;
        if (launchType == LaunchType.FARGATE) {
            RegisterTaskDefinitionRequest request = new RegisterTaskDefinitionRequest();
            request.setFamily(cluster);
            request.setContainerDefinitions(List.of(container));
            request.setNetworkMode(NetworkMode.awsvpc);
            request.setCpu("256");
            request.setMemory("512");
            request.setRequiresCompatibilities(List.of("FARGATE"));
            definition = ecs.registerTaskDefinition(request, REGION);
            AwsVpcConfiguration vpc = new AwsVpcConfiguration();
            vpc.setSubnets(List.of("subnet-default-us-east-1-a"));
            network = new NetworkConfiguration();
            network.setAwsvpcConfiguration(vpc);
        } else {
            definition = ecs.registerTaskDefinition(cluster, List.of(container), null, null,
                    null, null, null, List.of(), REGION);
        }
        service = ecs.createService(cluster, "worker", definition.getTaskDefinitionArn(), 0,
                launchType, List.of(), network, REGION);
        Path file = directory.resolve("on-demand.yaml");
        Files.writeString(file, """
                workloads:
                  worker:
                    queues: [events]
                    runtime:
                      type: ecs
                      cluster-name: %s
                      service: worker
                """.formatted(cluster));
        WorkloadRuntime runtime = factory.create(OnDemandConfigLoader.load(file, "000000000000", REGION).getFirst());
        assertFalse(runtime.isRunning());
        runtime.start(Duration.ofSeconds(60));
        runtime.awaitReady(Duration.ofSeconds(60));
        EcsTask first = runningTask();
        String firstContainer = first.getContainers().getFirst().getRuntimeId();
        assertTrue(docker.inspectContainerCmd(firstContainer).exec().getState().getRunning());
        runtime.stop(Duration.ofSeconds(60));
        assertFalse(runtime.isRunning());
        assertEquals("STOPPED", first.getLastStatus());
        assertThrows(NotFoundException.class,
                () -> docker.inspectContainerCmd(firstContainer).exec());
        runtime.start(Duration.ofSeconds(60));
        runtime.awaitReady(Duration.ofSeconds(60));
        EcsTask second = runningTask();
        assertNotEquals(first.getTaskArn(), second.getTaskArn());
        assertNotEquals(firstContainer, second.getContainers().getFirst().getRuntimeId());
        runtime.stop(Duration.ofSeconds(60));
        assertEquals("STOPPED", second.getLastStatus());
        assertEquals(0, service.getDesiredCount());
    }

    private EcsTask runningTask() {
        return ecs.tasksForService(service).stream().filter(task -> "RUNNING".equals(task.getLastStatus()))
                .findFirst().orElseThrow();
    }
}
