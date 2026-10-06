package io.github.hectorvent.floci.runtime.ondemand;

import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.HealthRequest;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.HealthResponse;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.BackendProtocol;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.RuntimeType;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

final class OnDemandTestSupport {
    private OnDemandTestSupport() {}

    static WorkloadDefinition definition(String id, URI backend, List<String> queues) {
        return new WorkloadDefinition(id, true, "000000000000", "us-east-1", id + ".test",
                RuntimeType.KUBERNETES, id, "default", 1, backend, 8080,
                Duration.ofSeconds(120), Duration.ofSeconds(5), queues,
                List.of(new HealthRequest("GET", "/health"), new HealthRequest("HEAD", "/health")),
                new HealthResponse(200, "{\"status\":\"UP\"}", "application/json"), "/ready", 200, null, BackendProtocol.HTTP1);
    }

    static class FakeRuntime implements WorkloadRuntime {
        final AtomicInteger starts = new AtomicInteger();
        final AtomicInteger stops = new AtomicInteger();
        volatile boolean running;
        volatile boolean failStart;
        volatile boolean activity;
        volatile boolean failActivity;
        volatile Runnable startAction = () -> {};
        volatile URI backend = URI.create("http://localhost:8081");

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public boolean hasActivity() {
            if (failActivity) {
                throw new IllegalStateException("activity inspection unavailable");
            }
            return activity;
        }

        @Override
        public void start(Duration timeout) {
            starts.incrementAndGet();
            startAction.run();
            if (failStart) {
                throw new IllegalStateException("backend failed");
            }
            running = true;
        }

        @Override
        public void awaitReady(Duration timeout) throws Exception {}

        @Override
        public void stop(Duration timeout) {
            stops.incrementAndGet();
            running = false;
        }

        @Override
        public URI backend() {
            return backend;
        }
    }
}
