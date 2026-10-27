package io.github.hectorvent.floci.runtime.wakes;

import io.github.hectorvent.floci.runtime.node.RuntimeFile;
import io.github.hectorvent.floci.runtime.node.RuntimeFileWatcher;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeDirectory;
import io.github.hectorvent.floci.runtime.wakes.WorkloadActivity.Read;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.PodSelector;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Target;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Workload;
import io.github.hectorvent.floci.testing.InlineMocks;
import io.github.hectorvent.floci.testing.MutableClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The wake loop's parks and its reset, with a clock and a parker of the test's own. Each park
 * waits for the test to let the loop take its next turn.
 */
class MessageWakeServiceTest {

    private static final long UNBOUNDED = -1;

    private final RuntimeFileWatcher runtimeFile = mock(RuntimeFileWatcher.class);
    private final WorkloadsSection workloads = mock(WorkloadsSection.class);
    private final SqsQueueWatch sqs = mock(SqsQueueWatch.class);
    private final RabbitMqQueueWatch rabbitMq = mock(RabbitMqQueueWatch.class);
    private final LeaseKeeper leases = mock(LeaseKeeper.class);
    private final NodeEventWatch nodeEvents = mock(NodeEventWatch.class);
    private final SmolvmNodeDirectory nodes = mock(SmolvmNodeDirectory.class);
    private final MutableClock clock = new MutableClock();
    private final StepParker parker = new StepParker();
    private final Workload orders = new Workload("orders", true, "000000000000", "us-east-1",
            Duration.ofSeconds(120), Duration.ofSeconds(30), List.of("orders-incoming"), List.of(),
            new Target("node:dev", "shop", PodSelector.Controller.deployment("orders")));
    private MessageWakeService service;

    @BeforeEach
    void setUp() {
        RuntimeFile file = InlineMocks.inlineMock(RuntimeFile.class);
        when(file.section(workloads)).thenReturn(new WorkloadDefinitions(Map.of("orders", orders)));
        when(file.instanceId()).thenReturn(Optional.empty());
        when(runtimeFile.configured()).thenReturn(true);
        when(runtimeFile.current()).thenReturn(file);
        when(sqs.read(any())).thenReturn(Read.none(null));
        when(leases.nextDue()).thenReturn(Optional.empty());
        service = new MessageWakeService(runtimeFile, workloads, sqs, rabbitMq, leases, nodeEvents, nodes, clock,
                parker);
    }

    @AfterEach
    void tearDown() {
        service.onStop(null);
        parker.steps.release(1_000);
    }

    @Test
    void withNoTimerTheLoopParksUntilAnUnpark() throws InterruptedException {
        service.onStart(null);

        assertThat(parker.next(), is(UNBOUNDED));
        verify(sqs).read(orders);
    }

    @Test
    void aParkWithATimerPendingLastsAtMostFiveSeconds() throws InterruptedException {
        when(leases.nextDue()).thenAnswer(invocation -> Optional.of(clock.instant().plus(Duration.ofMinutes(1))));

        service.onStart(null);

        assertThat(parker.next(), is(MessageWakeService.MAX_TIMED_PARK.toNanos()));
    }

    @Test
    void aParkWithATimerSoonerThanTheCapLastsUntilTheTimer() throws InterruptedException {
        when(leases.nextDue()).thenReturn(Optional.of(clock.instant().plus(Duration.ofSeconds(2))));

        service.onStart(null);

        long nanos = parker.next();
        assertThat(nanos > Duration.ofMillis(1_900).toNanos() && nanos <= Duration.ofSeconds(2).toNanos(), is(true));
    }

    @Test
    void aResetRunsOnTheLoopAndReturnsOnceItIsDone() throws Exception {
        service.onStart(null);
        assertThat(parker.next(), is(UNBOUNDED));
        AtomicReference<String> releasedOn = new AtomicReference<>();
        doAnswer(invocation -> {
            releasedOn.set(Thread.currentThread().getName());
            return null;
        }).when(leases).releaseAll();

        CompletableFuture<Void> reset = CompletableFuture.runAsync(service::clear);
        Thread.sleep(200);
        assertThat(reset.isDone(), is(false));

        parker.step();
        reset.get(5, TimeUnit.SECONDS);

        assertThat(releasedOn.get(), is("message-wakes"));
        verify(rabbitMq).clear();
    }

    @Test
    void afterAResetEachWatchedQueueIsReadOnce() throws InterruptedException {
        service.onStart(null);
        assertThat(parker.next(), is(UNBOUNDED));

        service.afterReset();
        parker.step();

        assertThat(parker.next(), is(UNBOUNDED));
        verify(sqs, times(2)).read(orders);
        verify(rabbitMq, times(2)).watch(any());
    }

    /** Records each park, and holds the loop there until the test steps it. */
    private static final class StepParker implements MessageWakeService.Parker {

        private final BlockingQueue<Long> parks = new LinkedBlockingQueue<>();
        private final Semaphore steps = new Semaphore(0);

        @Override
        public void park(Object blocker) {
            parks.add(UNBOUNDED);
            steps.acquireUninterruptibly();
        }

        @Override
        public void parkNanos(Object blocker, long nanos) {
            parks.add(nanos);
            steps.acquireUninterruptibly();
        }

        /** The next park's time in nanoseconds, or {@link #UNBOUNDED}. */
        long next() throws InterruptedException {
            Long nanos = parks.poll(5, TimeUnit.SECONDS);
            if (nanos == null) {
                throw new AssertionError("the loop did not park within 5 s");
            }
            return nanos;
        }

        void step() {
            steps.release();
        }
    }
}
