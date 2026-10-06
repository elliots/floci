package io.github.hectorvent.floci.runtime.ondemand;

import java.net.URI;
import java.time.Duration;

public interface WorkloadRuntime {
    boolean isRunning();

    default boolean hasActivity() {
        return false;
    }

    void start(Duration timeout) throws Exception;

    void stop(Duration timeout) throws Exception;

    URI backend();
}
