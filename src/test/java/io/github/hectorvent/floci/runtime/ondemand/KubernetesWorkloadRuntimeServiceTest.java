package io.github.hectorvent.floci.runtime.ondemand;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.core.common.microsandbox.PodControllerClient;
import io.github.hectorvent.floci.core.common.microsandbox.PodControllerClient.Deployment;
import io.github.hectorvent.floci.core.common.microsandbox.PodControllerClient.Pod;
import io.github.hectorvent.floci.runtime.ondemand.ActivationCoordinator.State;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.BackendProtocol;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.KubernetesRouting;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.KubernetesSpec;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.RuntimeType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class KubernetesWorkloadRuntimeServiceTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(2);
    private final FakeController controller = new FakeController();
    private WorkloadDefinition definition;
    private KubernetesWorkloadRuntime runtime;

    @BeforeEach
    void setup() {
        controller.pods.put("pod-a", pod("pod-a", "SLEEPING", 1));
        controller.pods.put("pod-b", pod("pod-b", "SLEEPING", 4));
        definition = new WorkloadDefinition("workers", true, "000000000000", "us-east-1",
                "workers.local", RuntimeType.KUBERNETES, "workers", "apps", 1,
                URI.create("http://workers-service.local:8080"), 8080, Duration.ofSeconds(1), TIMEOUT,
                List.of("jobs"), List.of(), null, "/health", 200, "dev", BackendProtocol.HTTP1);
        runtime = new KubernetesWorkloadRuntime(definition, controller);
    }

    @Test
    void wakesOnePodAndLeavesOtherReplicasSleepingWithoutChangingKubernetesIdentities() throws Exception {
        assertFalse(runtime.isRunning());
        runtime.start(TIMEOUT);
        runtime.awaitReady(TIMEOUT);
        assertTrue(runtime.isRunning());
        assertEquals(URI.create("http://127.0.0.1:31001"), runtime.backend());
        assertEquals(List.of("resume:pod-a:1"), controller.operations);
        runtime.stop(TIMEOUT);
        assertFalse(runtime.isRunning());
        assertEquals(List.of("pod-a", "pod-b"), List.copyOf(controller.pods.keySet()));
        assertEquals(2, controller.deployment("apps", "workers", TIMEOUT).replicas());
        assertEquals(List.of("resume:pod-a:1", "hibernate:pod-a:2"),
                controller.operations);
    }

    @Test
    void replacementDuringActivationFailsInsteadOfAdoptingADifferentDeployment() throws Exception {
        runtime.start(TIMEOUT);
        controller.deploymentUid = "new-deployment";
        assertThrows(IllegalStateException.class, () -> runtime.awaitReady(TIMEOUT));
        assertEquals(1, controller.operations.size());
    }

    @Test
    void partialHibernateFailureReusesTheStillRunningPodWithoutWakingTheOther() throws Exception {
        controller.pods.replaceAll((uid, current) -> pod(uid, "READY", current.generation()));
        runtime.start(TIMEOUT);
        runtime.awaitReady(TIMEOUT);
        controller.failSecondHibernate = true;
        assertThrows(IllegalStateException.class, () -> runtime.stop(TIMEOUT));
        assertEquals("SLEEPING", controller.pods.get("pod-a").state());
        assertEquals("READY", controller.pods.get("pod-b").state());
        controller.operations.clear();
        runtime.start(TIMEOUT);
        runtime.awaitReady(TIMEOUT);
        assertTrue(controller.operations.isEmpty());
        assertEquals(URI.create("http://127.0.0.1:31002"), runtime.backend());
    }

    @Test
    void rejectsWrongClusterAndFailedPodsBeforeMutatingAnything() {
        controller.cluster = "another-cluster";
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        controller.cluster = "dev";
        controller.pods.put("pod-a", pod("pod-a", "FAILED", 1));
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        assertTrue(controller.operations.isEmpty());
    }

    @Test
    void restoredRunningProcessMustPassPodReadinessBeforeGatewayActivation() throws Exception {
        controller.deferReadiness = true;
        runtime.start(TIMEOUT);
        assertEquals("RUNNING", controller.pods.get("pod-a").state());
        controller.readyOnSecondObservation = true;
        runtime.awaitReady(TIMEOUT);
        assertEquals("READY", controller.pods.get("pod-a").state());
        assertTrue(controller.observations >= 2);
    }

    @Test
    void pendingVolumeInitCheckpointAndRestartTransitionsSettleBeforeActivation() throws Exception {
        for (String state : List.of("WAITINGVOLUMES", "INITSTARTING", "INITIALIZING", "CHECKPOINTED", "RESTARTING")) {
            controller.pods.replaceAll((uid, current) -> pod(uid, state, current.generation()));
            controller.observations = 0;
            controller.readyOnSecondObservation = true;
            runtime.start(TIMEOUT);
            runtime.awaitReady(TIMEOUT);
            assertTrue(controller.observations >= 2);
            assertTrue(controller.operations.isEmpty());
        }
    }

    @Test
    void initializingPodsCannotCompleteActivationOrHibernate() {
        for (String state : List.of("INITSTARTING", "INITIALIZING")) {
            controller.pods.replaceAll((uid, current) -> pod(uid, state, current.generation()));
            Duration timeout = Duration.ofMillis(10);
            assertTrue(runtime.isRunning());
            assertThrows(IllegalStateException.class, () -> runtime.start(timeout));
            assertThrows(IllegalStateException.class, () -> runtime.stop(timeout));
            assertTrue(controller.operations.isEmpty());
            assertTrue(controller.pods.values().stream().allMatch(pod -> state.equals(pod.state())));
        }
    }

    @Test
    void podsWaitingForVolumesCannotCompleteActivationAndRemainUntouchedOnIdleStop() throws Exception {
        controller.pods.replaceAll((uid, current) -> pod(uid, "WAITINGVOLUMES", current.generation()));
        Duration timeout = Duration.ofMillis(10);
        assertThrows(IllegalStateException.class, () -> runtime.start(timeout));
        assertFalse(runtime.isRunning());
        runtime.stop(timeout);
        assertTrue(controller.operations.isEmpty());
        assertTrue(controller.pods.values().stream().allMatch(pod -> "WAITINGVOLUMES".equals(pod.state())));
    }

    @Test
    void coldPodsRemainUnstartedDuringDiscoveryAndIdleStop() throws Exception {
        controller.pods.replaceAll((uid, current) -> pod(uid, "COLD", current.generation()));
        assertFalse(runtime.isRunning());
        runtime.stop(TIMEOUT);
        assertFalse(runtime.isRunning());
        assertTrue(controller.pods.values().stream().allMatch(pod -> "COLD".equals(pod.state())));
        assertTrue(controller.operations.isEmpty());
    }

    @Test
    void explicitParallelConsumerPolicyResumesColdAndHibernatedPods() throws Exception {
        configure(2, KubernetesRouting.POD);
        controller.pods.put("pod-a", pod("pod-a", "COLD", 1));
        assertFalse(runtime.isRunning());
        runtime.start(TIMEOUT);
        runtime.awaitReady(TIMEOUT);
        assertEquals(List.of("resume:pod-a:1", "resume:pod-b:4"), controller.operations);
        assertTrue(controller.pods.values().stream().allMatch(pod -> "READY".equals(pod.state())));
    }

    @Test
    void idleStopLeavesColdPodsUntouchedWhileHibernatingRunningPods() throws Exception {
        controller.pods.put("pod-a", pod("pod-a", "COLD", 1));
        controller.pods.put("pod-b", pod("pod-b", "READY", 4));
        assertTrue(runtime.isRunning());
        runtime.stop(TIMEOUT);
        assertFalse(runtime.isRunning());
        assertEquals("COLD", controller.pods.get("pod-a").state());
        assertEquals(List.of("hibernate:pod-b:4"), controller.operations);
    }

    @Test
    void readinessFailsIfActivatedPodReturnsToCold() throws Exception {
        runtime.start(TIMEOUT);
        controller.pods.put("pod-a", pod("pod-a", "COLD", 3));
        assertThrows(IllegalStateException.class, () -> runtime.awaitReady(TIMEOUT));
    }

    @Test
    void proxyAvailabilityDoesNotMeanTheColdApplicationIsRunning() {
        controller.pods.replaceAll((uid, current) -> new Pod(uid, "apps", uid, "COLD", current.generation(),
                true, "external", 0));
        assertTrue(runtime.observesExternalStarts());
        assertFalse(runtime.isRunning());
        assertFalse(runtime.hasActivity());
        assertTrue(controller.operations.isEmpty());
    }

    @Test
    void legacyTcpProxyStreamsCountAsActivityOutsideTheFlociGateway() {
        controller.pods.put("pod-a", new Pod("pod-a", "apps", "pod-a", "READY", 2, true, "external", 1));
        assertTrue(runtime.isRunning());
        assertTrue(runtime.hasActivity());
        controller.pods.put("pod-a", new Pod("pod-a", "apps", "pod-a", "READY", 2, true, "external", 0));
        assertFalse(runtime.hasActivity());
    }

    @Test
    void directHttpRequestsCountAsActivityWithoutLegacyTcpConnections() {
        controller.pods.put("pod-a", new Pod("pod-a", "apps", "pod-a", "READY", 2, true, "external", 0, 1));
        assertTrue(runtime.hasActivity());
        controller.pods.put("pod-a", new Pod("pod-a", "apps", "pod-a", "READY", 2, true, "external", 0, 0));
        assertFalse(runtime.hasActivity());
    }

    @Test
    void directHttpRequestPinsDeploymentUntilCompletionAndIdleTimeout() {
        controller.pods.replaceAll((uid, current) -> pod(uid, "READY", current.generation()));
        controller.pods.put("pod-a", new Pod("pod-a", "apps", "pod-a", "READY", 1, true, "external", 0, 1, 1));
        AtomicLong time = new AtomicLong(1);
        try (ActivationCoordinator coordinator = new ActivationCoordinator(Executors.newSingleThreadExecutor(),
                (workload, backend, timeout) -> {}, 1, 2, 2, time::get)) {
            coordinator.register(definition, runtime);
            coordinator.reconcile("workers", false, true);
            await().atMost(TIMEOUT).until(() -> coordinator.state("workers") == State.READY);
            time.addAndGet(Duration.ofSeconds(5).toNanos());
            coordinator.reconcile("workers", false, true);
            assertEquals(State.READY, coordinator.state("workers"));
            assertTrue(controller.operations.isEmpty(), "The active direct HTTP request must prevent hibernation");

            // An idle HTTP keep-alive socket contributes to neither activity count.
            controller.pods.put("pod-a", new Pod("pod-a", "apps", "pod-a", "READY", 1, true, "external", 0, 0, 2));
            coordinator.reconcile("workers", false, true);
            time.addAndGet(Duration.ofMillis(999).toNanos());
            coordinator.reconcile("workers", false, true);
            assertEquals(State.READY, coordinator.state("workers"));
            time.addAndGet(Duration.ofMillis(1).toNanos());
            coordinator.reconcile("workers", false, true);
            await().atMost(TIMEOUT).until(() -> coordinator.state("workers") == State.SLEEPING);
            assertEquals(List.of("hibernate:pod-a:1", "hibernate:pod-b:4"), controller.operations);
        }
    }

    @Test
    void completedDirectHttpRequestBetweenPollsRestartsTheIdleTimer() {
        controller.pods.replaceAll((uid, current) -> pod(uid, "READY", current.generation()));
        AtomicLong time = new AtomicLong(1);
        try (ActivationCoordinator coordinator = new ActivationCoordinator(Executors.newSingleThreadExecutor(),
                (workload, backend, timeout) -> {}, 1, 2, 2, time::get)) {
            coordinator.register(definition, runtime);
            coordinator.reconcile("workers", false, true);
            await().atMost(TIMEOUT).until(() -> coordinator.state("workers") == State.READY);
            time.addAndGet(Duration.ofMillis(900).toNanos());
            controller.pods.put("pod-a", new Pod("pod-a", "apps", "pod-a", "READY", 1, true, "external", 0, 0, 2));
            coordinator.reconcile("workers", false, true);
            time.addAndGet(Duration.ofMillis(999).toNanos());
            coordinator.reconcile("workers", false, true);
            assertEquals(State.READY, coordinator.state("workers"), "The completed request starts a fresh idle interval");
            assertTrue(controller.operations.isEmpty());
            time.addAndGet(Duration.ofMillis(1).toNanos());
            coordinator.reconcile("workers", false, true);
            await().atMost(TIMEOUT).until(() -> coordinator.state("workers") == State.SLEEPING);
            assertEquals(List.of("hibernate:pod-a:1", "hibernate:pod-b:4"), controller.operations);
        }
    }

    @Test
    void failedRequestActivityObservationDoesNotPermitIdleHibernation() {
        controller.pods.replaceAll((uid, current) -> pod(uid, "READY", current.generation()));
        AtomicLong time = new AtomicLong(1);
        try (ActivationCoordinator coordinator = new ActivationCoordinator(Executors.newSingleThreadExecutor(),
                (workload, backend, timeout) -> {}, 1, 2, 2, time::get)) {
            coordinator.register(definition, runtime);
            coordinator.reconcile("workers", false, true);
            await().atMost(TIMEOUT).until(() -> coordinator.state("workers") == State.READY);
            controller.failObservation = true;
            time.addAndGet(Duration.ofSeconds(5).toNanos());
            coordinator.tick("workers");
            assertEquals(State.READY, coordinator.state("workers"));
            assertTrue(controller.operations.isEmpty());
        }
    }

    @Test
    void requestCompletedAfterIdleObservationPreventsAnyHibernateMutation() throws Exception {
        runtime.start(TIMEOUT);
        runtime.awaitReady(TIMEOUT);
        assertFalse(runtime.hasActivity());
        controller.operations.clear();
        controller.pods.put("pod-b", new Pod("pod-b", "apps", "pod-b", "READY", 5, true, "external", 0, 0, 2));
        IllegalStateException failure = assertThrows(IllegalStateException.class, () -> runtime.stop(TIMEOUT));
        assertTrue(failure.getMessage().contains("activity changed"));
        assertTrue(controller.operations.isEmpty(), "Activity must be checked before hibernating any deployment member");
        assertTrue(runtime.hasActivity());
        assertFalse(runtime.hasActivity());
        runtime.stop(TIMEOUT);
        assertFalse(runtime.isRunning());
    }

    @Test
    void activityBetweenObservationAndStopDefersIdleWithoutFailingTheWorkload() {
        AtomicLong time = new AtomicLong(1);
        try (ActivationCoordinator coordinator = new ActivationCoordinator(Executors.newSingleThreadExecutor(),
                (workload, endpoint, timeout) -> {}, 1, 2, 2, time::get)) {
            coordinator.register(definition, runtime);
            try (ActivationCoordinator.RequestLease request = coordinator.acquire("workers")) {
                request.ready().join();
            }
            coordinator.reconcile("workers", false, true);
            time.addAndGet(Duration.ofSeconds(1).toNanos());
            controller.activityAfterObservation = true;
            coordinator.tick("workers");
            await().atMost(TIMEOUT).until(() -> coordinator.state("workers") == State.READY);
            assertEquals(URI.create("http://127.0.0.1:31001"), coordinator.backend("workers"));
            assertEquals(List.of("resume:pod-a:1"), controller.operations, "The deferred stop must mutate no pod");
            coordinator.tick("workers");
            time.addAndGet(Duration.ofMillis(999).toNanos());
            coordinator.tick("workers");
            assertEquals(State.READY, coordinator.state("workers"), "The activity deferral starts a new idle interval");
            time.addAndGet(Duration.ofMillis(1).toNanos());
            coordinator.tick("workers");
            await().atMost(TIMEOUT).until(() -> coordinator.state("workers") == State.SLEEPING);
            assertEquals(List.of("resume:pod-a:1", "hibernate:pod-a:2"), controller.operations);
        }
    }

    @Test
    void refusesTwoIndependentIdleOwnersBeforeMutatingPods() {
        controller.pods.put("pod-a", new Pod("pod-a", "apps", "pod-a", "COLD", 1, true, "controller", 0));
        assertThrows(IllegalStateException.class, runtime::isRunning);
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        assertThrows(IllegalStateException.class, () -> runtime.stop(TIMEOUT));
        assertTrue(controller.operations.isEmpty());
    }

    @Test
    void repeatedGatewayHealthChecksUseDirectEndpointAndDoNotRenewIdleTime() {
        AtomicLong time = new AtomicLong(1);
        try (ActivationCoordinator coordinator = new ActivationCoordinator(Executors.newSingleThreadExecutor(),
                (workload, endpoint, timeout) -> assertEquals(URI.create("http://127.0.0.1:32001"), endpoint),
                1, 2, 2, time::get)) {
            coordinator.register(definition, runtime);
            try (ActivationCoordinator.RequestLease request = coordinator.acquire("workers")) {
                assertEquals(URI.create("http://127.0.0.1:31001"), request.ready().join());
            }
            coordinator.reconcile("workers", false, true);
            for (int probe = 0; probe < 5; probe++) {
                time.addAndGet(Duration.ofMillis(250).toNanos());
                try (ActivationCoordinator.RequestLease health = coordinator.acquireHealth("workers").orElseThrow()) {
                    assertEquals(URI.create("http://127.0.0.1:32001"), health.ready().join());
                    coordinator.tick("workers");
                    assertEquals(State.READY, coordinator.state("workers"), "An in-flight health check holds the pod");
                }
            }
            coordinator.tick("workers");
            await().atMost(TIMEOUT).until(() -> coordinator.state("workers") == State.SLEEPING);
            assertEquals(List.of("resume:pod-a:1", "hibernate:pod-a:2"), controller.operations);
        }
    }

    @Test
    void deploymentReplacementBetweenParallelWakesStopsBeforeMutatingAnotherPod() {
        configure(2, KubernetesRouting.POD);
        controller.replaceAfterResume = true;
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        assertEquals(List.of("resume:pod-a:1"), controller.operations);
        assertEquals("SLEEPING", controller.pods.get("pod-b").state());
    }

    @Test
    void queueOnlyConsumersDoNotRequireAnyPublishedHttpEndpoint() throws Exception {
        definition = new WorkloadDefinition(definition.id(), definition.enabled(), definition.accountId(),
                definition.region(), null, definition.type(), definition.target(), definition.namespace(),
                definition.replicas(), null, 80, definition.idleTimeout(), definition.startupTimeout(),
                definition.queues(), List.of(), null, null, 200, definition.clusterName(), definition.backendProtocol());
        controller.failEndpoint = true;
        runtime = new KubernetesWorkloadRuntime(definition, controller);
        runtime.start(TIMEOUT);
        runtime.awaitReady(TIMEOUT);
        assertEquals(List.of("resume:pod-a:1"), controller.operations);
        assertNull(runtime.backend());
        assertNull(runtime.healthBackend());
    }

    @Test
    void missingHttpEndpointFailsBeforeAnyPodIsWoken() {
        controller.failEndpoint = true;
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        assertTrue(controller.operations.isEmpty());
    }

    @Test
    void aReplicaWithMissingVolumeSourcesDoesNotBlockAnAvailableColdReplica() throws Exception {
        controller.pods.put("pod-a", pod("pod-a", "WAITINGVOLUMES", 1));
        controller.pods.put("pod-b", pod("pod-b", "COLD", 4));
        runtime.start(TIMEOUT);
        runtime.awaitReady(TIMEOUT);
        assertEquals(List.of("resume:pod-b:4"), controller.operations);
        assertEquals("WAITINGVOLUMES", controller.pods.get("pod-a").state());
        runtime.stop(TIMEOUT);
        assertEquals("SLEEPING", controller.pods.get("pod-b").state());
        assertEquals("WAITINGVOLUMES", controller.pods.get("pod-a").state());
    }

    @Test
    void defaultActivationOfFiveHundredReplicasStartsOnlyOne() throws Exception {
        controller.pods.clear();
        for (int index = 0; index < 500; index++) {
            String uid = "pod-" + String.format("%03d", index);
            controller.pods.put(uid, pod(uid, "COLD", 1));
        }
        runtime.start(TIMEOUT);
        runtime.awaitReady(TIMEOUT);
        assertEquals(List.of("resume:pod-000:1"), controller.operations);
        assertEquals(499, controller.pods.values().stream().filter(pod -> "COLD".equals(pod.state())).count());
        assertEquals(500, controller.deployment("apps", "workers", TIMEOUT).replicas());
        runtime.stop(TIMEOUT);
        assertEquals(499, controller.pods.values().stream().filter(pod -> "COLD".equals(pod.state())).count());
    }

    @Test
    void activationReusesReadyReplicaWithoutWaitingForAnUnselectedInitContainer() throws Exception {
        controller.pods.put("pod-a", pod("pod-a", "INITIALIZING", 1));
        controller.pods.put("pod-b", pod("pod-b", "READY", 4));
        runtime.start(TIMEOUT);
        runtime.awaitReady(TIMEOUT);
        assertTrue(controller.operations.isEmpty());
        assertEquals(URI.create("http://127.0.0.1:31002"), runtime.backend());
    }

    @Test
    void serviceRoutingPreservesExplicitHttpsEndpointAndCapsWakeCountAtDesiredReplicas() throws Exception {
        configure(20, KubernetesRouting.SERVICE);
        runtime.start(TIMEOUT);
        runtime.awaitReady(TIMEOUT);
        assertEquals(definition.backendUrl(), runtime.backend());
        assertEquals(2, controller.operations.size());
    }

    @Test
    void deploymentReplacementAfterActivationInvalidatesCachedBackend() throws Exception {
        runtime.start(TIMEOUT);
        runtime.awaitReady(TIMEOUT);
        controller.deploymentUid = "replacement-deployment";
        assertFalse(runtime.isRunning());
    }

    @Test
    void externallyWokenReplicaIsAdoptedWithoutBootingAnyColdPeer() throws Exception {
        controller.pods.put("pod-a", pod("pod-a", "COLD", 1));
        controller.pods.put("pod-b", pod("pod-b", "RUNNING", 4));
        controller.readyOnSecondObservation = true;
        runtime.start(TIMEOUT);
        runtime.awaitReady(TIMEOUT);
        assertTrue(controller.operations.isEmpty());
        assertEquals(URI.create("http://127.0.0.1:31002"), runtime.backend());
    }

    private void configure(int replicas, KubernetesRouting routing) {
        definition = new WorkloadDefinition(definition.id(), definition.enabled(), definition.accountId(),
                definition.region(), definition.host(), definition.type(), definition.target(), definition.namespace(),
                definition.replicas(), URI.create("https://workers-service.local:8443/"), definition.backendPort(),
                definition.idleTimeout(), definition.startupTimeout(), definition.queues(), definition.healthRequests(),
                definition.sleepingHealth(), definition.readinessPath(), definition.readinessStatus(), definition.clusterName(),
                definition.backendProtocol(), definition.rabbitMqQueues(), null, new KubernetesSpec(replicas, routing));
        runtime = new KubernetesWorkloadRuntime(definition, controller);
    }

    private static Pod pod(String uid, String state, long generation) {
        return new Pod(uid, "apps", uid, state, generation);
    }

    private static final class FakeController extends PodControllerClient {
        private final Map<String, Pod> pods = new LinkedHashMap<>();
        private final List<String> operations = new ArrayList<>();
        private String deploymentUid = "deployment-uid";
        private String cluster = "dev";
        private boolean failSecondHibernate;
        private boolean replaceAfterResume;
        private boolean failEndpoint;
        private boolean deferReadiness;
        private boolean readyOnSecondObservation;
        private boolean failObservation;
        private boolean activityAfterObservation;
        private int observations;

        private FakeController() {
            super("http://localhost:18187", "controller-token-at-least-thirty-two-characters", new ObjectMapper());
        }

        @Override
        public URI applicationEndpoint(Pod observed) {
            if (failEndpoint) {
                throw new IllegalStateException("No HTTP endpoint");
            }
            return URI.create("http://127.0.0.1:" + (observed.uid().equals("pod-b") ? 31002 : 31001));
        }

        @Override
        public URI healthEndpoint(Pod observed) {
            if (failEndpoint) {
                throw new IllegalStateException("No HTTP endpoint");
            }
            return URI.create("http://127.0.0.1:" + (observed.uid().equals("pod-b") ? 32002 : 32001));
        }

        @Override
        public void checkAvailable(Duration timeout) {
            // HTTP authentication and contract validation are covered by PodControllerClientServiceTest.
        }

        @Override
        public Deployment observedDeployment(String namespace, String name, Duration timeout) {
            if (failObservation) {
                throw new IllegalStateException("Controller observation is unavailable");
            }
            Deployment observed = deployment(namespace, name, timeout);
            if (activityAfterObservation) {
                activityAfterObservation = false;
                Pod current = pods.get("pod-a");
                pods.put("pod-a", new Pod(current.uid(), current.namespace(), current.name(), current.state(),
                        current.generation(), true, "external", 0, 0, 2));
            }
            return observed;
        }

        @Override
        public Deployment deployment(String namespace, String name, Duration timeout) {
            if (readyOnSecondObservation && ++observations >= 2) {
                pods.replaceAll((uid, current) -> pod(uid, "READY", current.generation()));
            }
            return new Deployment(deploymentUid, namespace, name, cluster, 1, pods.size(), List.copyOf(pods.values()));
        }

        @Override
        public Pod resume(Pod observed, Duration timeout) {
            assertEquals(observed.generation(), pods.get(observed.uid()).generation());
            operations.add("resume:" + observed.uid() + ":" + observed.generation());
            Pod resumed = pod(observed.uid(), deferReadiness ? "RUNNING" : "READY", observed.generation() + 1);
            pods.put(observed.uid(), resumed);
            if (replaceAfterResume) {
                deploymentUid = "replacement-deployment";
            }
            return resumed;
        }

        @Override
        public Pod hibernate(Pod observed, Duration timeout) {
            assertEquals(observed.generation(), pods.get(observed.uid()).generation());
            if (failSecondHibernate && observed.uid().equals("pod-b")) {
                throw new IllegalStateException("Lifecycle generation changed");
            }
            operations.add("hibernate:" + observed.uid() + ":" + observed.generation());
            Pod stopped = pod(observed.uid(), "SLEEPING", observed.generation() + 1);
            pods.put(observed.uid(), stopped);
            return stopped;
        }
    }
}
