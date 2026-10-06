package io.github.hectorvent.floci.runtime.ondemand;

import io.github.hectorvent.floci.runtime.ondemand.ActivationCoordinator.RequestLease;
import io.github.hectorvent.floci.runtime.ondemand.ActivationCoordinator.State;
import io.github.hectorvent.floci.runtime.ondemand.OnDemandTestSupport.FakeRuntime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class ActivationStartupPermitsServiceTest {
    private ActivationCoordinator coordinator;

    @AfterEach
    void close() {
        if (coordinator != null) {
            coordinator.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void fiveDependenciesStartWithOnePermitDuringEitherReadinessPhase(boolean runtimeReadiness) throws Exception {
        coordinator = new ActivationCoordinator(Executors.newVirtualThreadPerTaskExecutor(),
                (definition, backend, timeout) -> {
                    if (!runtimeReadiness) {
                        awaitDependency(Integer.parseInt(definition.id()), timeout);
                    }
                }, 1, 16, 4, System::nanoTime);
        List<FakeRuntime> runtimes = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            int index = i;
            FakeRuntime runtime = new FakeRuntime() {
                @Override
                public void awaitReady(Duration timeout) throws Exception {
                    if (runtimeReadiness) {
                        awaitDependency(index, timeout);
                    }
                }
            };
            runtimes.add(runtime);
            register(Integer.toString(i), runtime);
        }
        try (RequestLease first = coordinator.acquire("0"); RequestLease second = coordinator.acquire("0")) {
            first.close();
            assertEquals(runtimes.getFirst().backend, second.ready().get(5, TimeUnit.SECONDS));
            for (int i = 0; i < runtimes.size(); i++) {
                assertEquals(1, runtimes.get(i).starts.get(), "Each dependency must share one activation");
                assertEquals(State.READY, coordinator.state(Integer.toString(i)));
            }
        }
    }

    private void awaitDependency(int index, Duration timeout) throws Exception {
        if (index < 4) {
            try (RequestLease dependency = coordinator.acquire(Integer.toString(index + 1))) {
                dependency.ready().get(timeout.toNanos(), TimeUnit.NANOSECONDS);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void cancellationDoesNotReleaseAnInUsePermitAndLaunchFailureDoesNotLeakIt(boolean failLaunch) throws Exception {
        coordinator = new ActivationCoordinator(Executors.newVirtualThreadPerTaskExecutor(),
                (definition, backend, timeout) -> {}, 1, 8, 4, System::nanoTime);
        CountDownLatch launching = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch secondPrepared = new CountDownLatch(1);
        CountDownLatch secondLaunching = new CountDownLatch(1);
        FakeRuntime first = new FakeRuntime();
        first.failStart = failLaunch;
        first.startAction = () -> {
            launching.countDown();
            await(release);
        };
        FakeRuntime second = new FakeRuntime() {
            @Override
            public void prepareStart(Duration timeout) {
                secondPrepared.countDown();
            }
        };
        second.startAction = secondLaunching::countDown;
        register("first", first);
        register("second", second);
        try (RequestLease firstRequest = coordinator.acquire("first")) {
            assertTrue(launching.await(2, TimeUnit.SECONDS));
            firstRequest.close();
            try (RequestLease secondRequest = coordinator.acquire("second")) {
                assertTrue(secondPrepared.await(2, TimeUnit.SECONDS));
                assertFalse(secondLaunching.await(200, TimeUnit.MILLISECONDS), "The first command still owns its permit");
                release.countDown();
                assertEquals(second.backend, secondRequest.ready().get(3, TimeUnit.SECONDS));
                if (failLaunch) {
                    assertThrows(ExecutionException.class, () -> firstRequest.ready().get(3, TimeUnit.SECONDS));
                } else {
                    assertEquals(first.backend, firstRequest.ready().get(3, TimeUnit.SECONDS));
                }
            }
        } finally {
            release.countDown();
        }
    }

    @Test
    void lifecyclePreparationDoesNotHoldALaunchPermit() throws Exception {
        coordinator = new ActivationCoordinator(Executors.newVirtualThreadPerTaskExecutor(),
                (definition, backend, timeout) -> {}, 1, 8, 4, System::nanoTime);
        CountDownLatch preparing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        FakeRuntime previousStop = new FakeRuntime() {
            @Override
            public void prepareStart(Duration timeout) {
                preparing.countDown();
                await(release);
            }
        };
        FakeRuntime other = new FakeRuntime();
        register("stopping", previousStop);
        register("other", other);
        try (RequestLease first = coordinator.acquire("stopping")) {
            assertTrue(preparing.await(2, TimeUnit.SECONDS));
            try (RequestLease second = coordinator.acquire("other")) {
                assertEquals(other.backend, second.ready().get(2, TimeUnit.SECONDS));
                assertFalse(first.ready().isDone());
            }
            release.countDown();
            assertEquals(previousStop.backend, first.ready().get(2, TimeUnit.SECONDS));
        } finally {
            release.countDown();
        }
    }

    @Test
    void startupPhasesShareOneDeadlineAndCannotReportReadyAfterItExpires() throws Exception {
        AtomicLong time = new AtomicLong();
        coordinator = new ActivationCoordinator(Executors.newVirtualThreadPerTaskExecutor(),
                (definition, backend, timeout) -> {
                    assertEquals(Duration.ofSeconds(2), timeout);
                    time.addAndGet(TimeUnit.SECONDS.toNanos(3));
                }, 1, 8, 4, time::get);
        FakeRuntime runtime = new FakeRuntime() {
            @Override
            public void prepareStart(Duration timeout) {
                assertEquals(Duration.ofSeconds(5), timeout);
                time.addAndGet(TimeUnit.SECONDS.toNanos(1));
            }

            @Override
            public void start(Duration timeout) {
                assertEquals(Duration.ofSeconds(4), timeout);
                time.addAndGet(TimeUnit.SECONDS.toNanos(1));
                super.start(timeout);
            }

            @Override
            public void awaitReady(Duration timeout) {
                assertEquals(Duration.ofSeconds(3), timeout);
                time.addAndGet(TimeUnit.SECONDS.toNanos(1));
            }
        };
        register("deadline", runtime);
        try (RequestLease request = coordinator.acquire("deadline")) {
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> request.ready().get(2, TimeUnit.SECONDS));
            assertEquals("On-demand readiness deadline exceeded", failure.getCause().getMessage());
            assertEquals(State.FAILED, coordinator.state("deadline"));
        }
    }

    private void register(String id, FakeRuntime runtime) {
        coordinator.register(OnDemandTestSupport.definition(id, runtime.backend, List.of()), runtime);
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
