package io.github.hectorvent.floci.services.ecs;

import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.ecs.model.InvalidParameterException;
import software.amazon.awssdk.services.ecs.model.LaunchType;
import software.amazon.awssdk.services.ecs.model.NetworkMode;
import software.amazon.awssdk.services.ecs.model.ProtectedTask;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class EcsTaskProtectionIntegrationTest {
    @TestHTTPResource
    URI endpoint;

    @Inject
    EcsService service;

    @Test
    void sdkProtectionDefaultsExpiresAndPreventsScaleInUntilCleared() {
        String cluster = "protection-" + UUID.randomUUID();
        try (EcsClient ecs = EcsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build()) {
            ecs.createCluster(request -> request.clusterName(cluster));
            String definition = ecs.registerTaskDefinition(request -> request.family(cluster)
                    .networkMode(NetworkMode.BRIDGE).containerDefinitions(container -> container
                            .name("worker").image("busybox:1.37").memory(128).essential(true)))
                    .taskDefinition().taskDefinitionArn();
            try {
                ecs.createService(request -> request.cluster(cluster).serviceName("worker")
                        .taskDefinition(definition).desiredCount(1).launchType(LaunchType.EC2));
                service.reconcileServices();
                String task = ecs.listTasks(request -> request.cluster(cluster).serviceName("worker"))
                        .taskArns().getFirst();
                Instant before = Instant.now();
                ProtectedTask protectedTask = ecs.updateTaskProtection(request -> request.cluster(cluster)
                        .tasks(task).protectionEnabled(true)).protectedTasks().getFirst();
                assertTrue(protectedTask.protectionEnabled());
                assertTrue(protectedTask.expirationDate().isAfter(before.plusSeconds(7199)));
                assertTrue(protectedTask.expirationDate().isBefore(Instant.now().plusSeconds(7201)));
                assertThrows(InvalidParameterException.class, () -> ecs.updateTaskProtection(request -> request
                        .cluster(cluster).tasks(task).protectionEnabled(true).expiresInMinutes(2881)));
                ecs.updateService(request -> request.cluster(cluster).service("worker").desiredCount(0));
                service.reconcileServices();
                assertEquals("RUNNING", ecs.describeTasks(request -> request.cluster(cluster).tasks(task))
                        .tasks().getFirst().lastStatus());
                service.describeTasks(cluster, List.of(task), "us-east-1").getFirst()
                        .setProtectedUntil(Instant.now().minusSeconds(1));
                ProtectedTask expired = ecs.getTaskProtection(request -> request.cluster(cluster).tasks(task))
                        .protectedTasks().getFirst();
                assertFalse(expired.protectionEnabled());
                assertNull(expired.expirationDate());
                service.reconcileServices();
                assertEquals("STOPPED", ecs.describeTasks(request -> request.cluster(cluster).tasks(task))
                        .tasks().getFirst().lastStatus());
            } finally {
                ecs.deleteService(request -> request.cluster(cluster).service("worker").force(true));
                ecs.deregisterTaskDefinition(request -> request.taskDefinition(definition));
                ecs.deleteCluster(request -> request.cluster(cluster));
            }
        }
    }
}
