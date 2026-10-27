package io.github.hectorvent.floci.services.lambda.launcher;

import io.github.hectorvent.floci.services.lambda.model.LambdaFunction;

/**
 * Starts and stops Lambda execution environments. Implementations back each
 * environment with a concrete runtime: a Docker container ({@link ContainerLauncher})
 * or a Kubernetes pod ({@code KubernetesPodLauncher}). Selected at startup by
 * {@code floci.services.lambda.executor}.
 */
public interface LambdaRuntimeLauncher {

    ContainerHandle launch(LambdaFunction fn);

    default boolean supportsProvisionedConcurrency() {
        return false;
    }

    default ContainerHandle launchProvisioned(LambdaFunction fn) {
        throw new UnsupportedOperationException("Provisioned concurrency is not supported by this Lambda executor");
    }

    /**
     * Keeps the environment awake for one invocation, until the returned hold closes. An
     * environment that sleeps between invocations is woken first.
     */
    default Hold hold(ContainerHandle handle) {
        return () -> { };
    }

    void stop(ContainerHandle handle);

    boolean isAlive(ContainerHandle handle);

    /** What {@link #hold} returns. */
    interface Hold extends AutoCloseable {

        @Override
        void close();
    }
}
