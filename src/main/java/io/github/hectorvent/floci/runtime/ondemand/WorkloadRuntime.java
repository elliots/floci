package io.github.hectorvent.floci.runtime.ondemand;

import java.net.URI;
import java.time.Duration;

public interface WorkloadRuntime {
    boolean isRunning();

    /** Whether an available external request gate can start this runtime independently. */
    default boolean observesExternalStarts() {
        return false;
    }

    default boolean hasActivity() {
        return false;
    }

    /** Waits for any previous lifecycle operation before acquiring a launch permit. */
    default void prepareStart(Duration timeout) throws Exception {}

    /** Submits the start or scale command without waiting for runtime or application readiness. */
    void start(Duration timeout) throws Exception;

    /** Waits for runtime readiness after the launch permit has been released. */
    void awaitReady(Duration timeout) throws Exception;

    void stop(Duration timeout) throws Exception;

    /** Drains accepted asynchronous lifecycle operations before reset can remove their resource records. */
    default void awaitQuiescence(Duration timeout) throws Exception {}

    URI backend();

    /** Running health probes bypass application-activity accounting while their coordinator lease prevents sleep. */
    default URI healthBackend() {
        return backend();
    }
}
