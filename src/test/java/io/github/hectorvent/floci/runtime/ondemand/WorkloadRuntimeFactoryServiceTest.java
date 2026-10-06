package io.github.hectorvent.floci.runtime.ondemand;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.BackendProtocol;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.RuntimeType;
import io.github.hectorvent.floci.services.ec2.Ec2Service;
import io.github.hectorvent.floci.services.ec2.model.Instance;
import io.github.hectorvent.floci.services.ec2.model.InstanceState;
import io.github.hectorvent.floci.services.ecs.EcsService;
import io.github.hectorvent.floci.services.ecs.model.Container;
import io.github.hectorvent.floci.services.ecs.model.EcsServiceModel;
import io.github.hectorvent.floci.services.ecs.model.EcsTask;
import io.github.hectorvent.floci.services.ecs.model.UpdateServiceRequest;
import io.github.hectorvent.floci.services.eks.EksClusterManager;
import io.github.hectorvent.floci.services.eks.EksService;
import io.github.hectorvent.floci.services.eks.model.Cluster;
import io.github.hectorvent.floci.services.eks.model.ClusterStatus;
import io.github.hectorvent.floci.services.lambda.launcher.kubernetes.KubernetesApiClient;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class WorkloadRuntimeFactoryServiceTest {
    private final Ec2Service ec2 = mock(Ec2Service.class);
    private final KubernetesApiClient kubernetes = mock(KubernetesApiClient.class);
    private final WorkloadRuntimeFactory factory = new WorkloadRuntimeFactory(ec2, kubernetes);

    @Test
    void ecsScalesOnlyDesiredCountAndWaitsForStoppingTasksToDrain() throws Exception {
        EcsService ecs = mock(EcsService.class);
        EcsServiceModel service = new EcsServiceModel();
        service.setStatus("ACTIVE");
        service.setSchedulingStrategy("REPLICA");
        service.setDeploymentController("ECS");
        service.setDeploymentId("deployment-1");
        EcsTask task = new EcsTask();
        task.setLastStatus("STOPPED");
        task.setDesiredStatus("STOPPED");
        task.setDeploymentId("deployment-1");
        when(ecs.describeServices("cluster", List.of("Orders_API"), "cn-north-1")).thenReturn(List.of(service));
        when(ecs.tasksForService(service)).thenAnswer(ignored -> List.of(task));
        when(ecs.updateService(any(UpdateServiceRequest.class), eq("cn-north-1"))).thenAnswer(invocation -> {
            UpdateServiceRequest update = invocation.getArgument(0);
            assertEquals("cluster", update.getCluster());
            assertEquals("Orders_API", update.getService());
            assertNull(update.getTaskDefinition());
            assertNull(update.getNetworkConfiguration());
            assertNull(update.getCapacityProviderStrategy());
            assertFalse(update.isForceNewDeployment());
            service.setDesiredCount(update.getDesiredCount());
            task.setLastStatus(update.getDesiredCount() == 0 ? "STOPPING" : "RUNNING");
            task.setDesiredStatus(update.getDesiredCount() == 0 ? "STOPPED" : "RUNNING");
            return service;
        });
        WorkloadDefinition definition = WorkloadConfigLoader.parse(new ObjectMapper().readTree("""
                {"workloads":{"orders":{"account-id":"123456789012","region":"cn-north-1",
                "host":"orders.test","runtime":{"type":"ecs","cluster-name":"cluster",
                "service":"Orders_API","backend-url":"http://stable-backend:9000"}}}}
                """), "000000000000", "us-east-1").getFirst();
        WorkloadRuntime runtime = new WorkloadRuntimeFactory(ec2, kubernetes, null, null, ecs).create(definition);
        assertFalse(runtime.isRunning());
        Container container = new Container();
        container.setHealthStatus("UNKNOWN");
        task.setContainers(List.of(container));
        assertThrows(IllegalStateException.class, () -> runtime.start(Duration.ofMillis(100)),
                "RUNNING alone is insufficient while a configured container health check is unknown");
        container.setHealthStatus("HEALTHY");
        runtime.start(Duration.ofSeconds(1));
        assertTrue(runtime.isRunning());
        assertEquals(definition.backendUrl(), runtime.backend());
        task.setProtectionEnabled(true);
        task.setProtectedUntil(Instant.now().plusSeconds(60));
        assertTrue(runtime.hasActivity());
        task.setProtectedUntil(Instant.now().minusSeconds(1));
        assertFalse(runtime.hasActivity());
        CountDownLatch draining = new CountDownLatch(1);
        when(ecs.tasksForService(service)).thenAnswer(ignored -> {
            if ("STOPPING".equals(task.getLastStatus())) {
                draining.countDown();
            }
            return List.of(task);
        });
        Thread stopping = Thread.startVirtualThread(() -> {
            try {
                runtime.stop(Duration.ofSeconds(5));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        try {
            assertTrue(draining.await(2, TimeUnit.SECONDS));
            assertTrue(stopping.isAlive(), "desiredCount=0 alone does not prove containers have stopped");
            task.setLastStatus("STOPPED");
            stopping.join(2000);
            assertFalse(stopping.isAlive());
            assertFalse(runtime.isRunning());
            runtime.start(Duration.ofSeconds(1));
            verify(ecs, times(4)).updateService(any(UpdateServiceRequest.class), eq("cn-north-1"));
        } finally {
            stopping.interrupt();
            stopping.join(2000);
        }
    }

    @Test
    void ecsRejectsMissingInactiveDaemonAndExternalServices() throws Exception {
        EcsService ecs = mock(EcsService.class);
        WorkloadDefinition definition = WorkloadConfigLoader.parse(new ObjectMapper().readTree("""
                {"workloads":{"worker":{"queues":["events"],"runtime":{"type":"ecs","service":"worker"}}}}
                """), "000000000000", "us-east-1").getFirst();
        WorkloadRuntime runtime = new WorkloadRuntimeFactory(ec2, kubernetes, null, null, ecs).create(definition);
        when(ecs.describeServices("default", List.of("worker"), "us-east-1")).thenReturn(List.of());
        assertThrows(IllegalStateException.class, runtime::isRunning);
        EcsServiceModel service = new EcsServiceModel();
        when(ecs.describeServices("default", List.of("worker"), "us-east-1")).thenReturn(List.of(service));
        service.setStatus("INACTIVE");
        assertThrows(IllegalStateException.class, runtime::isRunning);
        service.setStatus("ACTIVE");
        service.setSchedulingStrategy("DAEMON");
        service.setDeploymentController("ECS");
        assertThrows(IllegalStateException.class, runtime::isRunning);
        service.setSchedulingStrategy("REPLICA");
        service.setDeploymentController("EXTERNAL");
        assertThrows(IllegalStateException.class, runtime::isRunning);
        verify(ecs, never()).updateService(any(UpdateServiceRequest.class), anyString());
    }

    @Test
    void ec2UsesExactAccountAndRegionAndRefreshesAddressAfterRestart() throws Exception {
        Instance instance = new Instance();
        instance.setState(InstanceState.stopped());
        instance.setContainerBridgeIp("172.18.0.10");
        when(ec2.findInstanceForAccount("123456789012", "cn-north-1", "i-1234")).thenReturn(Optional.of(instance));
        doAnswer(invocation -> {
            instance.setState(InstanceState.running());
            return List.of();
        }).when(ec2).startInstances("cn-north-1", List.of("i-1234"));
        doAnswer(invocation -> {
            instance.setState(InstanceState.stopped());
            return List.of();
        }).when(ec2).stopInstances("cn-north-1", List.of("i-1234"));
        WorkloadDefinition base = OnDemandTestSupport.definition("legacy", null, List.of());
        WorkloadDefinition definition = new WorkloadDefinition(base.id(), true, "123456789012", "cn-north-1",
                base.host(), RuntimeType.EC2, "i-1234", "default", 1, null, 9000,
                base.idleTimeout(), base.startupTimeout(), List.of(), List.of(), base.sleepingHealth(), "/ready", 200, null, BackendProtocol.HTTP1);
        WorkloadRuntime runtime = factory.create(definition);
        assertFalse(runtime.isRunning());
        runtime.start(Duration.ofSeconds(1));
        assertEquals(URI.create("http://172.18.0.10:9000"), runtime.backend());
        runtime.stop(Duration.ofSeconds(1));
        instance.setContainerBridgeIp("172.18.0.20");
        runtime.start(Duration.ofSeconds(1));
        assertEquals(URI.create("http://172.18.0.20:9000"), runtime.backend());
        verify(ec2, times(2)).startInstances("cn-north-1", List.of("i-1234"));
        verify(ec2).stopInstances("cn-north-1", List.of("i-1234"));
    }

    @Test
    void kubernetesWaitsForObservedGenerationAndAvailabilityThenScalesToZero() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode stopped = mapper.readTree("""
                {"metadata":{"generation":3},"spec":{"replicas":0},
                 "status":{"observedGeneration":3,"replicas":0}}
                """);
        JsonNode ready = mapper.readTree("""
                {"metadata":{"generation":4},"spec":{"replicas":1},
                 "status":{"observedGeneration":4,"replicas":1,"availableReplicas":1}}
                """);
        when(kubernetes.getDeployment("default", "orders")).thenReturn(Optional.of(stopped));
        doAnswer(invocation -> {
            when(kubernetes.getDeployment("default", "orders")).thenReturn(Optional.of(ready));
            return null;
        }).when(kubernetes).scaleDeployment("default", "orders", 1);
        doAnswer(invocation -> {
            when(kubernetes.getDeployment("default", "orders")).thenReturn(Optional.of(stopped));
            return null;
        }).when(kubernetes).scaleDeployment("default", "orders", 0);
        WorkloadDefinition definition = OnDemandTestSupport.definition("orders", URI.create("http://orders-backend"), List.of());
        WorkloadRuntime runtime = factory.create(definition);
        assertFalse(runtime.isRunning());
        runtime.start(Duration.ofSeconds(1));
        assertTrue(runtime.isRunning());
        assertEquals(definition.backendUrl(), runtime.backend());
        runtime.stop(Duration.ofSeconds(1));
        assertFalse(runtime.isRunning());
        verify(kubernetes).scaleDeployment("default", "orders", 1);
        verify(kubernetes).scaleDeployment("default", "orders", 0);
    }

    @Test
    void kubernetesStopWaitsForTerminatingPodsEvenWithoutTheNewReplicaStatusField() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode stopped = mapper.readTree("""
                {"metadata":{"generation":3},"spec":{"replicas":0,"selector":{
                  "matchLabels":{"app":"orders"},"matchExpressions":[
                    {"key":"environment","operator":"In","values":["test"]},
                    {"key":"tier","operator":"NotIn","values":["production"]},
                    {"key":"worker","operator":"Exists"},
                    {"key":"excluded","operator":"DoesNotExist"}]}},
                 "status":{"observedGeneration":3,"replicas":0}}
                """);
        JsonNode terminating = mapper.readTree("""
                {"metadata":{"deletionTimestamp":"2026-10-06T00:00:00Z",
                  "labels":{"app":"orders","environment":"test","worker":"true"}}}
                """);
        JsonNode unrelated = mapper.readTree("""
                {"metadata":{"labels":{"app":"orders","environment":"production","worker":"true"}}}
                """);
        when(kubernetes.getDeployment("default", "orders")).thenReturn(Optional.of(stopped));
        when(kubernetes.listPods("default", Map.of("app", "orders")))
                .thenReturn(List.of(terminating, unrelated), List.of(unrelated));
        WorkloadRuntime runtime = factory.create(OnDemandTestSupport.definition("orders", URI.create("http://backend"), List.of()));
        runtime.stop(Duration.ofSeconds(1));
        verify(kubernetes, times(2)).listPods("default", Map.of("app", "orders"));
    }

    @Test
    void kubernetesStopWaitsForTheTerminatingReplicaCountToReachZero() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode draining = mapper.readTree("""
                {"metadata":{"generation":3},"spec":{"replicas":0},
                 "status":{"observedGeneration":3,"replicas":0,"terminatingReplicas":1}}
                """);
        JsonNode stopped = mapper.readTree("""
                {"metadata":{"generation":3},"spec":{"replicas":0},
                 "status":{"observedGeneration":3,"replicas":0,"terminatingReplicas":0}}
                """);
        when(kubernetes.getDeployment("default", "orders"))
                .thenReturn(Optional.of(draining), Optional.of(draining), Optional.of(stopped));
        WorkloadRuntime runtime = factory.create(OnDemandTestSupport.definition("orders", URI.create("http://backend"), List.of()));
        runtime.stop(Duration.ofSeconds(1));
        verify(kubernetes, times(3)).getDeployment("default", "orders");
        verify(kubernetes).listPods("default", Map.of());
    }

    @Test
    void missingTargetsFailRatherThanReportingAHealthyDormantWorkload() {
        when(kubernetes.getDeployment("default", "orders")).thenReturn(Optional.empty());
        WorkloadRuntime runtime = factory.create(OnDemandTestSupport.definition("orders", URI.create("http://backend"), List.of()));
        assertThrows(IllegalStateException.class, runtime::isRunning);
    }

    @Test
    void namedClustersRejectOtherRegionsBeforeReadingCredentialsOrUsingAmbientKubernetes() {
        EksService eks = mock(EksService.class);
        EksClusterManager clusters = mock(EksClusterManager.class);
        Cluster cluster = new Cluster();
        cluster.setArn("arn:aws:eks:eu-west-1:000000000000:cluster/managed");
        cluster.setStatus(ClusterStatus.ACTIVE);
        when(eks.findAuthenticationCluster("000000000000", "managed")).thenReturn(Optional.of(cluster));
        WorkloadDefinition base = OnDemandTestSupport.definition("worker", null, List.of("events"));
        WorkloadDefinition definition = new WorkloadDefinition(base.id(), true, base.accountId(), base.region(), null,
                RuntimeType.KUBERNETES, base.target(), "default", 1, null, 80, base.idleTimeout(), base.startupTimeout(),
                base.queues(), List.of(), base.sleepingHealth(), null, 200, "managed", BackendProtocol.HTTP1);
        WorkloadRuntime runtime = new WorkloadRuntimeFactory(ec2, kubernetes, eks, clusters).create(definition);
        assertThrows(IllegalStateException.class, runtime::isRunning);
        verifyNoInteractions(kubernetes, clusters);
    }

    @Test
    void interruptedEc2ActivationDrainsTheAcceptedBackgroundStartBeforeReturning() throws Exception {
        Instance instance = new Instance();
        instance.setState(InstanceState.stopped());
        when(ec2.findInstanceForAccount("000000000000", "us-east-1", "i-drain")).thenReturn(Optional.of(instance));
        CountDownLatch started = new CountDownLatch(1);
        doAnswer(invocation -> {
            instance.setState(InstanceState.pending());
            started.countDown();
            return List.of();
        }).when(ec2).startInstances("us-east-1", List.of("i-drain"));
        WorkloadDefinition base = OnDemandTestSupport.definition("legacy", null, List.of());
        WorkloadRuntime runtime = factory.create(new WorkloadDefinition(base.id(), true, base.accountId(), base.region(),
                base.host(), RuntimeType.EC2, "i-drain", "default", 1, null, 80, base.idleTimeout(),
                base.startupTimeout(), List.of(), List.of(), base.sleepingHealth(), null, 200, null, BackendProtocol.HTTP1));
        AtomicBoolean cancelled = new AtomicBoolean();
        Thread task = Thread.startVirtualThread(() -> {
            try {
                runtime.start(Duration.ofSeconds(5));
            } catch (InterruptedException expected) {
                cancelled.set(true);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        try {
            assertTrue(started.await(2, TimeUnit.SECONDS));
            task.interrupt();
            await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(1)).until(() -> !cancelled.get());
            instance.setState(InstanceState.running());
            task.join(2000);
            assertFalse(task.isAlive());
            assertTrue(cancelled.get());
        } finally {
            instance.setState(InstanceState.running());
            task.interrupt();
            task.join(2000);
        }
    }
}
