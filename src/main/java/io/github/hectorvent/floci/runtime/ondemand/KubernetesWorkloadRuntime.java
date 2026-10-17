package io.github.hectorvent.floci.runtime.ondemand;

import io.github.hectorvent.floci.core.common.microsandbox.PodControllerClient;
import io.github.hectorvent.floci.core.common.microsandbox.PodControllerClient.Deployment;
import io.github.hectorvent.floci.core.common.microsandbox.PodControllerClient.Pod;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Hibernates controller-owned pods without scaling their Deployment or replacing Kubernetes identities. */
final class KubernetesWorkloadRuntime implements WorkloadRuntime {
    private final WorkloadDefinition definition;
    private final PodControllerClient controller;
    private Map<String, Long> activitySequences = Map.of();
    private Deployment starting;

    KubernetesWorkloadRuntime(WorkloadDefinition definition, PodControllerClient controller) {
        this.definition = definition;
        this.controller = controller;
    }

    @Override
    public synchronized boolean isRunning() {
        Deployment observed = validateDeployment(controller.observedDeployment(definition.namespace(),
                definition.target(), Duration.ofSeconds(10)));
        validateStates(observed);
        return observed.pods().stream().anyMatch(pod -> !inactive(pod));
    }

    @Override
    public boolean observesExternalStarts() {
        return true;
    }

    @Override
    public synchronized boolean hasActivity() {
        Deployment observed = validateDeployment(controller.observedDeployment(definition.namespace(),
                definition.target(), Duration.ofSeconds(10)));
        boolean active = observed.pods().stream().anyMatch(pod -> pod.activeRequests() > 0 || pod.activeConnections() > 0
                || activitySequences.getOrDefault(pod.uid(), 0L) != pod.activitySequence());
        activitySequences = observed.pods().stream().collect(Collectors.toUnmodifiableMap(Pod::uid, Pod::activitySequence));
        return active;
    }

    @Override
    public synchronized void start(Duration timeout) throws InterruptedException {
        long deadline = deadline(timeout);
        controller.checkAvailable(remaining(deadline));
        Deployment observed = stable(deployment(remaining(deadline)), deadline);
        starting = observed;
        for (Pod pod : observed.pods()) {
            if (inactive(pod)) {
                controller.resume(pod, remaining(deadline));
            }
        }
    }

    @Override
    public synchronized void awaitReady(Duration timeout) throws InterruptedException {
        if (starting == null) {
            throw new IllegalStateException("Kubernetes workload has no activation in progress");
        }
        await(starting, "READY", deadline(timeout));
    }

    @Override
    public synchronized void stop(Duration timeout) throws InterruptedException {
        long deadline = deadline(timeout);
        controller.checkAvailable(remaining(deadline));
        Deployment observed = stable(deployment(remaining(deadline)), deadline);
        if (!activitySequences.isEmpty() && observed.pods().stream()
                .anyMatch(pod -> activitySequences.getOrDefault(pod.uid(), -1L) != pod.activitySequence())) {
            throw new IllegalStateException("Kubernetes workload activity changed before hibernation");
        }
        for (Pod pod : observed.pods()) {
            if (!inactive(pod)) {
                controller.hibernate(pod, remaining(deadline));
            }
        }
        await(observed, "SLEEPING", deadline);
        starting = null;
    }

    private Deployment stable(Deployment original, long deadline) throws InterruptedException {
        Deployment observed = original;
        while (true) {
            validateStates(observed);
            if (observed.pods().stream().allMatch(pod -> Set.of("READY", "RUNNING", "SLEEPING", "COLD")
                    .contains(pod.state()))) {
                return observed;
            }
            pause(deadline);
            observed = sameMembership(original, deployment(remaining(deadline)));
        }
    }

    private void await(Deployment expected, String state, long deadline) throws InterruptedException {
        while (true) {
            Deployment observed = sameMembership(expected, deployment(remaining(deadline)));
            validateStates(observed);
            if (observed.pods().stream().allMatch(pod -> state.equals(pod.state())
                    || "SLEEPING".equals(state) && inactive(pod))) {
                return;
            }
            if ("READY".equals(state) && observed.pods().stream().anyMatch(KubernetesWorkloadRuntime::inactive)) {
                throw new IllegalStateException("A Kubernetes pod became inactive during activation");
            }
            pause(deadline);
        }
    }

    private Deployment deployment(Duration timeout) {
        return validateDeployment(controller.deployment(definition.namespace(), definition.target(), timeout));
    }

    private Deployment validateDeployment(Deployment observed) {
        if (definition.clusterName() != null && !definition.clusterName().equals(observed.clusterName())) {
            throw new IllegalStateException("Pod controller does not manage the configured Kubernetes cluster");
        }
        if (observed.pods().stream().anyMatch(pod -> pod.proxyAvailability() && !"external".equals(pod.idleOwner()))) {
            throw new IllegalStateException("Floci-managed proxy pods must select the external idle owner");
        }
        return observed;
    }

    private static Deployment sameMembership(Deployment expected, Deployment actual) {
        Map<String, String> identities = expected.pods().stream().collect(Collectors.toMap(Pod::uid, Pod::name));
        Map<String, String> current = actual.pods().stream().collect(Collectors.toMap(Pod::uid, Pod::name));
        if (!expected.uid().equals(actual.uid()) || expected.generation() != actual.generation()
                || expected.replicas() != actual.replicas() || !identities.equals(current)) {
            throw new IllegalStateException("Kubernetes deployment changed during its lifecycle transition");
        }
        return actual;
    }

    private static void validateStates(Deployment observed) {
        for (Pod pod : observed.pods()) {
            if ("FAILED".equals(pod.state()) || "TERMINATED".equals(pod.state())) {
                throw new IllegalStateException("Kubernetes pod is not resumable: " + pod.uid());
            }
        }
    }

    private static boolean inactive(Pod pod) {
        return "COLD".equals(pod.state()) || "SLEEPING".equals(pod.state());
    }

    private static void pause(long deadline) throws InterruptedException {
        Thread.sleep(Math.min(100, Math.max(1, remaining(deadline).toMillis())));
    }

    private static long deadline(Duration timeout) {
        return System.nanoTime() + timeout.toNanos();
    }

    private static Duration remaining(long deadline) {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) {
            throw new IllegalStateException("Timed out waiting for Kubernetes pod lifecycle");
        }
        return Duration.ofNanos(nanos);
    }

    @Override
    public URI backend() {
        return definition.backendUrl();
    }
}
