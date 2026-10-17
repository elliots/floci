package io.github.hectorvent.floci.runtime.ondemand;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.core.common.microsandbox.PodControllerClient;
import io.github.hectorvent.floci.core.common.microsandbox.PodControllerClient.Deployment;
import io.github.hectorvent.floci.core.common.microsandbox.PodControllerClient.Pod;
import io.github.hectorvent.floci.runtime.ondemand.ActivationCoordinator.State;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.BackendProtocol;
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
    void wakesAndHibernatesAllPodsWithoutChangingReplicaCountOrPodUids() throws Exception {
        assertFalse(runtime.isRunning());
        runtime.start(TIMEOUT);
        runtime.awaitReady(TIMEOUT);
        assertTrue(runtime.isRunning());
        assertEquals(URI.create("http://workers-service.local:8080"), runtime.backend());
        assertEquals(List.of("resume:pod-a:1", "resume:pod-b:4"), controller.operations);
        runtime.stop(TIMEOUT);
        assertFalse(runtime.isRunning());
        assertEquals(List.of("pod-a", "pod-b"), List.copyOf(controller.pods.keySet()));
        assertEquals(2, controller.deployment("apps", "workers", TIMEOUT).replicas());
        assertEquals(List.of("resume:pod-a:1", "resume:pod-b:4", "hibernate:pod-a:2", "hibernate:pod-b:5"),
                controller.operations);
    }

    @Test
    void replacementDuringActivationFailsInsteadOfAdoptingADifferentDeployment() throws Exception {
        runtime.start(TIMEOUT);
        controller.deploymentUid = "new-deployment";
        assertThrows(IllegalStateException.class, () -> runtime.awaitReady(TIMEOUT));
        assertEquals(2, controller.operations.size());
    }

    @Test
    void partialHibernateFailureCanWakeTheAlreadySleepingPodWithoutReplayingTheOther() throws Exception {
        runtime.start(TIMEOUT);
        runtime.awaitReady(TIMEOUT);
        controller.failSecondHibernate = true;
        assertThrows(IllegalStateException.class, () -> runtime.stop(TIMEOUT));
        assertEquals("SLEEPING", controller.pods.get("pod-a").state());
        assertEquals("READY", controller.pods.get("pod-b").state());
        controller.operations.clear();
        runtime.start(TIMEOUT);
        runtime.awaitReady(TIMEOUT);
        assertEquals(List.of("resume:pod-a:3"), controller.operations);
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
    void podsWaitingForVolumesCannotCompleteActivationOrHibernate() {
        controller.pods.replaceAll((uid, current) -> pod(uid, "WAITINGVOLUMES", current.generation()));
        Duration timeout = Duration.ofMillis(10);
        assertThrows(IllegalStateException.class, () -> runtime.start(timeout));
        assertThrows(IllegalStateException.class, () -> runtime.stop(timeout));
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
    void activationResumesBothColdAndHibernatedPods() throws Exception {
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
    void refusesTwoIndependentIdleOwnersBeforeMutatingPods() {
        controller.pods.put("pod-a", new Pod("pod-a", "apps", "pod-a", "COLD", 1, true, "controller", 0));
        assertThrows(IllegalStateException.class, runtime::isRunning);
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        assertThrows(IllegalStateException.class, () -> runtime.stop(TIMEOUT));
        assertTrue(controller.operations.isEmpty());
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
        private boolean deferReadiness;
        private boolean readyOnSecondObservation;
        private boolean failObservation;
        private int observations;

        private FakeController() {
            super("http://localhost:18187", "controller-token-at-least-thirty-two-characters", new ObjectMapper());
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
            return deployment(namespace, name, timeout);
        }

        @Override
        public Deployment deployment(String namespace, String name, Duration timeout) {
            if (readyOnSecondObservation && ++observations >= 2) {
                pods.replaceAll((uid, current) -> pod(uid, "READY", current.generation()));
            }
            return new Deployment(deploymentUid, namespace, name, cluster, 1, 2, List.copyOf(pods.values()));
        }

        @Override
        public Pod resume(Pod observed, Duration timeout) {
            assertEquals(observed.generation(), pods.get(observed.uid()).generation());
            operations.add("resume:" + observed.uid() + ":" + observed.generation());
            Pod resumed = pod(observed.uid(), deferReadiness ? "RUNNING" : "READY", observed.generation() + 1);
            pods.put(observed.uid(), resumed);
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
