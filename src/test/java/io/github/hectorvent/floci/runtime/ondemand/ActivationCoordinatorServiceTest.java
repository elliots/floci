package io.github.hectorvent.floci.runtime.ondemand;

import io.github.hectorvent.floci.runtime.ondemand.ActivationCoordinator.RequestLease;
import io.github.hectorvent.floci.runtime.ondemand.ActivationCoordinator.State;
import io.github.hectorvent.floci.runtime.ondemand.OnDemandTestSupport.FakeRuntime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class ActivationCoordinatorServiceTest {
    private final AtomicLong time = new AtomicLong();
    private final ManualExecutor executor = new ManualExecutor();
    private final FakeRuntime runtime = new FakeRuntime();
    private final ActivationCoordinator coordinator = new ActivationCoordinator(executor,
            (definition, backend, timeout) -> {}, 1, 2, 2, time::get);

    @AfterEach
    void close() {
        coordinator.close();
    }

    @Test
    void protectedRuntimeWorkAndFailedActivityInspectionPreventIdleStop() {
        register(List.of());
        RequestLease request = coordinator.acquire("orders");
        executor.runNext();
        request.close();
        runtime.activity = true;
        advance(600);
        coordinator.tick("orders");
        assertEquals(State.READY, coordinator.state("orders"));
        runtime.activity = false;
        runtime.failActivity = true;
        advance(120);
        coordinator.tick("orders");
        assertEquals(State.READY, coordinator.state("orders"));
        runtime.failActivity = false;
        coordinator.tick("orders");
        executor.runNext();
        assertEquals(State.SLEEPING, coordinator.state("orders"));
    }

    @Test
    void concurrentRequestsShareStartupAndIdleBeginsAfterTheLastCompletion() {
        register(List.of());
        RequestLease first = coordinator.acquire("orders");
        RequestLease second = coordinator.acquire("orders");
        assertEquals(1, executor.tasks.size());
        executor.runNext();
        assertEquals(runtime.backend, first.ready().join());
        assertEquals(runtime.backend, second.ready().join());
        assertEquals(1, runtime.starts.get());
        advance(600);
        first.close();
        coordinator.tick("orders");
        assertEquals(State.READY, coordinator.state("orders"));
        second.close();
        advance(119);
        coordinator.tick("orders");
        assertEquals(State.READY, coordinator.state("orders"));
        advance(1);
        coordinator.tick("orders");
        assertEquals(State.STOPPING, coordinator.state("orders"));
        executor.runNext();
        assertEquals(State.SLEEPING, coordinator.state("orders"));
        assertEquals(1, runtime.stops.get());
    }

    @Test
    void healthNeverStartsSleepingWorkloadsAndDoesNotRefreshIdleTime() {
        register(List.of());
        coordinator.reconcile("orders", false, true);
        assertTrue(coordinator.acquireHealth("orders").isEmpty());
        RequestLease request = coordinator.acquire("orders");
        executor.runNext();
        request.close();
        advance(90);
        RequestLease health = coordinator.acquireHealth("orders").orElseThrow();
        advance(40);
        coordinator.tick("orders");
        assertEquals(State.READY, coordinator.state("orders"), "a probe in progress must finish first");
        health.close();
        coordinator.tick("orders");
        executor.runNext();
        assertEquals(State.SLEEPING, coordinator.state("orders"));
        assertEquals(1, runtime.starts.get());
    }

    @Test
    void requestDuringStopWaitsForStopThenStartsAgain() {
        register(List.of());
        RequestLease initial = coordinator.acquire("orders");
        executor.runNext();
        initial.close();
        advance(120);
        coordinator.tick("orders");
        RequestLease next = coordinator.acquire("orders");
        assertFalse(next.ready().isDone());
        executor.runNext();
        assertFalse(next.ready().isDone());
        assertEquals(State.STARTING, coordinator.state("orders"));
        executor.runNext();
        assertEquals(runtime.backend, next.ready().join());
        assertEquals(2, runtime.starts.get());
        next.close();
    }

    @Test
    void queueWorkActivatesAndUnknownQueueStatePreventsShutdown() {
        register(List.of("orders-incoming"));
        coordinator.reconcile("orders", true, true);
        executor.runNext();
        advance(600);
        coordinator.tick("orders");
        assertEquals(State.READY, coordinator.state("orders"));
        coordinator.reconcile("orders", false, false);
        advance(600);
        coordinator.tick("orders");
        assertEquals(State.READY, coordinator.state("orders"));
        coordinator.reconcile("orders", false, true);
        advance(120);
        coordinator.tick("orders");
        executor.runNext();
        assertEquals(State.SLEEPING, coordinator.state("orders"));
    }

    @Test
    void emptyQueueStartsIdleIntervalAfterProcessingFinishes() {
        register(List.of("orders-incoming"));
        coordinator.reconcile("orders", true, true);
        executor.runNext();
        advance(600);
        coordinator.reconcile("orders", false, true);
        advance(119);
        coordinator.tick("orders");
        assertEquals(State.READY, coordinator.state("orders"));
        advance(1);
        coordinator.tick("orders");
        executor.runNext();
        assertEquals(State.SLEEPING, coordinator.state("orders"));
    }

    @Test
    void failedStartupReleasesCapacityAndAllowsRetry() {
        register(List.of());
        runtime.failStart = true;
        RequestLease failed = coordinator.acquire("orders");
        executor.runNext();
        assertThrows(CompletionException.class, () -> failed.ready().join());
        assertEquals(State.FAILED, coordinator.state("orders"));
        assertTrue(coordinator.acquireHealth("orders").isEmpty());
        runtime.failStart = false;
        RequestLease retry = coordinator.acquire("orders");
        executor.runNext();
        assertEquals(runtime.backend, retry.ready().join());
        retry.close();
    }

    @Test
    void cleaningUpFailedStartupDoesNotMakeHealthAppearSuccessful() {
        register(List.of());
        runtime.failStart = true;
        RequestLease failed = coordinator.acquire("orders");
        executor.runNext();
        assertTrue(failed.ready().isCompletedExceptionally());
        advance(120);
        coordinator.tick("orders");
        assertFalse(coordinator.canReportSleepingHealth("orders"));
        executor.runNext();
        assertEquals(State.FAILED, coordinator.state("orders"));
        assertFalse(coordinator.canReportSleepingHealth("orders"));
        coordinator.tick("orders");
        assertEquals(0, executor.tasks.size());
    }

    @Test
    void cancellationAndRepeatedCloseReleaseWaitingCapacityExactlyOnce() {
        register(List.of());
        RequestLease first = coordinator.acquire("orders");
        RequestLease second = coordinator.acquire("orders");
        assertThrows(ActivationCoordinator.CapacityException.class, () -> coordinator.acquire("orders"));
        first.close();
        first.close();
        RequestLease replacement = coordinator.acquire("orders");
        executor.runNext();
        second.close();
        replacement.close();
        advance(120);
        coordinator.tick("orders");
        executor.runNext();
        assertEquals(State.SLEEPING, coordinator.state("orders"));
    }

    @Test
    void restartAdoptsRunningWorkloadsAndLeavesSleepingOnesAlone() {
        register(List.of());
        runtime.running = true;
        coordinator.reconcile("orders", false, true);
        assertEquals(State.STARTING, coordinator.state("orders"));
        executor.runNext();
        assertEquals(State.READY, coordinator.state("orders"));
        FakeRuntime sleeping = new FakeRuntime();
        coordinator.register(OnDemandTestSupport.definition("other", sleeping.backend, List.of()), sleeping);
        coordinator.reconcile("other", false, true);
        assertEquals(State.SLEEPING, coordinator.state("other"));
        assertEquals(0, sleeping.starts.get());
    }

    @Test
    void inspectionRetriesUntilAConfiguredResourceIsProvisionedWithoutStartingIt() {
        boolean[] provisioned = {false};
        FakeRuntime delayed = new FakeRuntime() {
            @Override
            public boolean isRunning() {
                if (!provisioned[0]) {
                    throw new IllegalStateException("Deployment does not exist yet");
                }
                return super.isRunning();
            }
        };
        coordinator.register(OnDemandTestSupport.definition("orders", delayed.backend, List.of()), delayed);
        coordinator.reconcile("orders", false, true);
        assertEquals(State.UNKNOWN, coordinator.state("orders"));
        advance(120);
        coordinator.reconcile("orders", false, true);
        assertEquals(State.UNKNOWN, coordinator.state("orders"));
        assertEquals(0, delayed.starts.get());
        assertEquals(0, delayed.stops.get());
        provisioned[0] = true;
        coordinator.reconcile("orders", false, true);
        assertEquals(State.SLEEPING, coordinator.state("orders"));
        assertEquals(0, delayed.starts.get());
        assertEquals(0, delayed.stops.get());
    }

    @Test
    void closingFailsPendingRequestsAndRejectsNewOnes() {
        register(List.of());
        RequestLease lease = coordinator.acquire("orders");
        coordinator.close();
        assertThrows(CompletionException.class, () -> lease.ready().join());
        assertThrows(IllegalStateException.class, () -> coordinator.acquire("orders"));
        lease.close();
    }

    @Test
    void rejectedStartupCompletesTheRequestAndReleasesPendingCapacity() {
        register(List.of());
        executor.shutdown();
        RequestLease first = coordinator.acquire("orders");
        assertThrows(CompletionException.class, () -> first.ready().join());
        assertEquals(State.FAILED, coordinator.state("orders"));
        RequestLease second = coordinator.acquire("orders");
        assertThrows(CompletionException.class, () -> second.ready().join());
        RequestLease third = coordinator.acquire("orders");
        assertThrows(CompletionException.class, () -> third.ready().join());
    }

    private void register(List<String> queues) {
        coordinator.register(OnDemandTestSupport.definition("orders", runtime.backend, queues), runtime);
    }

    private void advance(long seconds) {
        time.addAndGet(TimeUnit.SECONDS.toNanos(seconds));
    }

    private static class ManualExecutor extends AbstractExecutorService {
        private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
        private boolean shutdown;

        void runNext() {
            tasks.remove().run();
        }

        @Override
        public void execute(Runnable task) {
            if (shutdown) {
                throw new RejectedExecutionException();
            }
            tasks.add(task);
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            List<Runnable> remaining = List.copyOf(tasks);
            tasks.clear();
            return remaining;
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return shutdown;
        }
    }
}
