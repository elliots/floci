package io.github.hectorvent.floci.runtime.wakes;

import io.github.hectorvent.floci.runtime.wakes.LeaseKeeper.Work;
import io.github.hectorvent.floci.runtime.wakes.WorkloadActivity.Read;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.PodSelector;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.RabbitMqQueue;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Target;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Workload;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/** The lease rule of one workload, with the clock as plain milliseconds. */
class WorkloadActivityServiceTest {

    private static final String QUEUE = "arn:aws:sqs:us-east-1:000000000000:orders-incoming";
    private static final String AMQP = "amqp://b-1/%2F/jobs";
    private static final long T0 = 1_000_000;

    private final Reads reads = new Reads();

    @Test
    void workMakesAnIdleWorkloadBusyAndSignalsOnlyWhileIdle() {
        WorkloadActivity activity = activity(10);

        assertThat(activity.work(T0, QUEUE), is(true));
        assertThat(activity.due(T0), is(T0));
        assertThat(activity.turn(T0, reads), is(true));

        assertThat(activity.demand().work(), is(Work.WAITING));
        assertThat(activity.demand().resource(), is(QUEUE));
        assertThat(activity.work(T0 + 1, QUEUE), is(false));
        assertThat(reads.count, is(0));
    }

    @Test
    void theLingerRunsFromTheLastEventAndAnEmptyReadReleases() {
        WorkloadActivity activity = busy(10);

        assertThat(activity.activity(T0 + 5_000), is(false));
        assertThat(activity.due(T0 + 5_000), is(T0 + 15_000));
        activity.turn(T0 + 10_000, reads);
        assertThat(reads.count, is(0));

        reads.next(Read.none(null));
        assertThat(activity.turn(T0 + 15_000, reads), is(true));

        assertThat(reads.count, is(1));
        assertThat(activity.demand().work(), is(Work.NONE));
        assertThat(activity.due(T0 + 15_000), is(Long.MAX_VALUE));
    }

    @Test
    void aSteadyStreamOfEventsKeepsTheWorkloadBusyWithNoRead() {
        WorkloadActivity activity = busy(2);

        for (long t = T0; t < T0 + 60_000; t += 1_000) {
            activity.work(t, QUEUE);
            activity.activity(t + 100);
            activity.activity(t + 200);
            activity.turn(t + 500, reads);
        }

        assertThat(reads.count, is(0));
        assertThat(activity.demand().work(), is(Work.WAITING));
    }

    @Test
    void workFoundAtTheDecisionKeepsTheLeasesUntilTheNextEvent() {
        WorkloadActivity activity = busy(10);
        reads.next(Read.work(QUEUE, null));

        activity.turn(T0 + 10_000, reads);

        assertThat(activity.demand().work(), is(Work.WAITING));
        assertThat(activity.due(T0 + 10_000), is(Long.MAX_VALUE));
        activity.turn(T0 + 100_000, reads);
        assertThat(reads.count, is(1));

        assertThat(activity.activity(T0 + 120_000), is(true));
        assertThat(activity.due(T0 + 120_000), is(T0 + 120_000));
        activity.turn(T0 + 120_000, reads);
        assertThat(activity.due(T0 + 120_000), is(T0 + 130_000));

        reads.next(Read.none(null));
        activity.turn(T0 + 130_000, reads);
        assertThat(activity.demand().work(), is(Work.NONE));
    }

    @Test
    void decisionsAreAtLeastOneSecondApartAlsoWithNoLinger() {
        WorkloadActivity activity = activity(0);
        activity.work(T0, QUEUE);
        activity.turn(T0, reads);

        assertThat(activity.due(T0), is(T0 + 1_000));
        activity.turn(T0 + 500, reads);
        assertThat(reads.count, is(0));

        reads.next(Read.work(QUEUE, null));
        activity.turn(T0 + 1_000, reads);
        activity.activity(T0 + 1_100);
        reads.next(Read.none(null));
        activity.turn(T0 + 1_100, reads);

        assertThat(reads.count, is(1));
        assertThat(activity.due(T0 + 1_100), is(T0 + 2_000));
        activity.turn(T0 + 2_000, reads);
        assertThat(activity.demand().work(), is(Work.NONE));
    }

    @Test
    void aWorkloadWithRabbitMqQueuesDecidesAtTheEndOfItsLinger() {
        Workload workload = new Workload("orders", true, "000000000000", "us-east-1", Duration.ofSeconds(2),
                Duration.ofSeconds(30), List.of(), List.of(new RabbitMqQueue("b-1", "/", "orders", null, null)),
                new Target("node:dev", "shop", PodSelector.Controller.deployment("orders")));
        WorkloadActivity activity = new WorkloadActivity(workload);
        activity.work(T0, "amqp://b-1/%2F/orders");
        activity.turn(T0, reads);

        assertThat(activity.due(T0), is(T0 + 2_000));
    }

    @Test
    void anEventDuringTheReadThatFindsNothingKeepsTheWorkloadBusy() {
        WorkloadActivity activity = busy(10);
        reads.next(() -> {
            activity.work(T0 + 10_000, QUEUE);
            return Read.none(null);
        });

        activity.turn(T0 + 10_000, reads);

        assertThat(activity.demand().work(), is(Work.WAITING));
        assertThat(activity.due(T0 + 10_000), is(T0 + 20_000));
    }

    @Test
    void anEventAfterTheReleaseMakesTheWorkloadBusyAgain() {
        WorkloadActivity activity = busy(10);
        reads.next(Read.none(null));
        activity.turn(T0 + 10_000, reads);
        assertThat(activity.demand().work(), is(Work.NONE));

        assertThat(activity.work(T0 + 10_000, QUEUE), is(true));
        activity.turn(T0 + 10_000, reads);

        assertThat(activity.demand().work(), is(Work.WAITING));
    }

    @Test
    void aDelayedMessageSetsTheTimerOfAnIdleWorkload() {
        WorkloadActivity activity = activity(10);

        assertThat(activity.delayed(T0 + 30_000), is(true));
        assertThat(activity.delayed(T0 + 40_000), is(false));
        assertThat(activity.due(T0), is(T0 + 30_000));
        activity.turn(T0 + 29_999, reads);
        assertThat(reads.count, is(0));

        reads.next(Read.work(QUEUE, Instant.ofEpochMilli(T0 + 40_000)));
        activity.turn(T0 + 30_000, reads);

        assertThat(activity.demand().work(), is(Work.WAITING));
        assertThat(activity.nextDelayed(), is(T0 + 40_000));
    }

    @Test
    void aDelayedSendDuringTheTimersReadIsKept() {
        WorkloadActivity activity = activity(10);
        activity.delayed(T0 + 30_000);
        reads.next(() -> {
            activity.delayed(T0 + 50_000);
            return Read.none(Instant.ofEpochMilli(T0 + 60_000));
        });

        activity.turn(T0 + 30_000, reads);

        assertThat(activity.demand().work(), is(Work.NONE));
        assertThat(activity.due(T0 + 30_000), is(T0 + 50_000));
    }

    @Test
    void aDelayedSendDuringTheDecisionSetsTheTimerAtTheRelease() {
        WorkloadActivity activity = busy(10);
        reads.next(() -> {
            activity.delayed(T0 + 50_000);
            return Read.none(null);
        });

        activity.turn(T0 + 10_000, reads);

        assertThat(activity.demand().work(), is(Work.NONE));
        assertThat(activity.due(T0 + 10_000), is(T0 + 50_000));
    }

    @Test
    void aDelayedMessageDoesNotMoveTheLinger() {
        WorkloadActivity activity = busy(10);

        assertThat(activity.delayed(T0 + 5_000), is(false));

        assertThat(activity.due(T0 + 1_000), is(T0 + 10_000));
    }

    @Test
    void aFailedReadKeepsTheLeasesAndIsTriedAgainLater() {
        WorkloadActivity activity = busy(10);
        reads.next(Read.failed());
        reads.next(Read.failed());
        reads.next(Read.none(null));

        activity.turn(T0 + 10_000, reads);
        assertThat(activity.demand().work(), is(Work.UNKNOWN));
        assertThat(activity.due(T0 + 10_000), is(T0 + 11_000));

        activity.turn(T0 + 11_000, reads);
        assertThat(activity.due(T0 + 11_000), is(T0 + 13_000));

        activity.turn(T0 + 13_000, reads);
        assertThat(activity.demand().work(), is(Work.NONE));
        assertThat(reads.count, is(3));
    }

    @Test
    void theFirstReadFindsTheMessagesThatWaitAlready() {
        WorkloadActivity activity = activity(10);
        activity.readOnce();
        reads.next(Read.work(QUEUE, null));

        assertThat(activity.due(T0), is(0L));
        activity.turn(T0, reads);

        assertThat(activity.demand().work(), is(Work.WAITING));
    }

    @Test
    void aFailedFirstReadIsTriedAgain() {
        WorkloadActivity activity = activity(10);
        activity.readOnce();
        reads.next(Read.failed());
        reads.next(Read.none(null));

        activity.turn(T0, reads);
        assertThat(activity.due(T0), is(T0 + 1_000));
        activity.turn(T0 + 1_000, reads);

        assertThat(activity.due(T0 + 1_000), is(Long.MAX_VALUE));
        assertThat(activity.demand().work(), is(Work.NONE));
    }

    @Test
    void aReloadReadNeverReleasesAndANewLingerMovesTheDecision() {
        WorkloadActivity activity = busy(10);
        activity.readOnce();
        activity.turn(T0 + 1_000, reads);

        assertThat(reads.count, is(0));
        assertThat(activity.demand().work(), is(Work.WAITING));

        activity.define(workload(30));

        assertThat(activity.due(T0 + 1_000), is(T0 + 30_000));
    }

    @Test
    void aReloadThatChangesTheQueuesEndsTheWaitForTheNextEvent() {
        WorkloadActivity activity = busy(10);
        reads.next(Read.work(QUEUE, null));
        activity.turn(T0 + 10_000, reads);
        assertThat(activity.due(T0 + 10_000), is(Long.MAX_VALUE));

        activity.define(new Workload("orders", true, "000000000000", "us-east-1", Duration.ofSeconds(10),
                Duration.ofSeconds(30), List.of("orders-retry"), List.of(),
                new Target("node:dev", "shop", PodSelector.Controller.deployment("orders"))));
        activity.readOnce();
        assertThat(activity.due(T0 + 10_500), is(T0 + 10_500));
        activity.turn(T0 + 10_500, reads);

        assertThat(reads.count, is(1));
        assertThat(activity.demand().work(), is(Work.WAITING));
        assertThat(activity.due(T0 + 10_500), is(T0 + 11_000));
        activity.turn(T0 + 11_000, reads);

        assertThat(reads.count, is(2));
        assertThat(activity.demand().work(), is(Work.NONE));
    }

    @Test
    void aSignalOfAnEventThatKeptTheWorkloadBusyAtItsReleaseIsDroppedAtTheNextRelease() {
        WorkloadActivity activity = busy(10);
        activity.releasing(() -> activity.work(T0 + 10_000, QUEUE));
        activity.turn(T0 + 10_000, reads);
        activity.releasing(() -> { });
        assertThat(activity.demand().work(), is(Work.WAITING));

        activity.turn(T0 + 20_000, reads);
        assertThat(activity.demand().work(), is(Work.NONE));
        assertThat(activity.turn(T0 + 20_000, reads), is(false));

        assertThat(activity.demand().work(), is(Work.NONE));
        assertThat(activity.due(T0 + 20_000), is(Long.MAX_VALUE));
        assertThat(reads.count, is(2));
    }

    @Test
    void anEventAfterAReleaseThatFollowedARaceMakesTheWorkloadBusy() {
        WorkloadActivity activity = busy(10);
        activity.releasing(() -> activity.work(T0 + 10_000, QUEUE));
        activity.turn(T0 + 10_000, reads);
        activity.releasing(() -> { });
        activity.turn(T0 + 20_000, reads);
        assertThat(activity.demand().work(), is(Work.NONE));

        assertThat(activity.work(T0 + 30_000, QUEUE), is(true));
        activity.turn(T0 + 30_000, reads);

        assertThat(activity.demand().work(), is(Work.WAITING));
    }

    @Test
    void heldMessagesKeepTheLeasesAndAreReadAgainOneLingerLater() {
        WorkloadActivity activity = rabbitMq(10);
        reads.next(Read.held(AMQP, null));

        activity.turn(T0 + 10_000, reads);

        assertThat(activity.demand().work(), is(Work.WAITING));
        assertThat(activity.demand().resource(), is(AMQP));
        assertThat(activity.due(T0 + 10_000), is(T0 + 20_000));

        reads.next(Read.held(AMQP, null));
        activity.turn(T0 + 20_000, reads);
        assertThat(activity.due(T0 + 20_000), is(T0 + 30_000));

        reads.next(Read.none(null));
        activity.turn(T0 + 30_000, reads);
        assertThat(activity.demand().work(), is(Work.NONE));
        assertThat(reads.count, is(3));
    }

    @Test
    void anEventWhileMessagesAreHeldMovesTheNextRead() {
        WorkloadActivity activity = rabbitMq(10);
        reads.next(Read.held(AMQP, null));
        activity.turn(T0 + 10_000, reads);

        activity.activity(T0 + 15_000);

        assertThat(activity.due(T0 + 15_000), is(T0 + 25_000));
    }

    @Test
    void aPendingDecisionWaitsForItsAnswerWithTheSameTicket() {
        WorkloadActivity activity = busy(10);
        reads.next(Read.pending());

        assertThat(activity.turn(T0 + 10_000, reads), is(false));
        assertThat(activity.demand().work(), is(Work.WAITING));
        assertThat(activity.due(T0 + 10_000), is(Long.MAX_VALUE));

        reads.next(Read.none(null));
        activity.turn(T0 + 10_200, reads);

        assertThat(activity.demand().work(), is(Work.NONE));
        assertThat(reads.tickets, is(List.of(1L, 1L)));
    }

    @Test
    void anEventWhileADecisionIsPendingKeepsTheWorkloadBusy() {
        WorkloadActivity activity = busy(10);
        reads.next(Read.pending());
        activity.turn(T0 + 10_000, reads);

        activity.activity(T0 + 10_100);
        reads.next(Read.none(null));
        activity.turn(T0 + 10_200, reads);

        assertThat(activity.demand().work(), is(Work.WAITING));
        assertThat(activity.due(T0 + 10_200), is(T0 + 20_100));
    }

    @Test
    void aSignalWhilePendingDropsTheIdleReadAndTheNextReadTakesANewTicket() {
        WorkloadActivity activity = activity(10);
        activity.readOnce();
        reads.next(Read.pending());
        activity.turn(T0, reads);

        activity.work(T0 + 100, QUEUE);
        activity.turn(T0 + 100, reads);
        assertThat(activity.demand().work(), is(Work.WAITING));

        reads.next(Read.none(null));
        activity.turn(T0 + 10_100, reads);

        assertThat(activity.demand().work(), is(Work.NONE));
        assertThat(reads.tickets, is(List.of(1L, 2L)));
    }

    @Test
    void aRequestedReadOfAnIdleWorkloadReadsAtItsTime() {
        WorkloadActivity activity = activity(10);

        assertThat(activity.requestRead(T0 + 6_000), is(true));
        assertThat(activity.requestRead(T0 + 7_000), is(false));
        assertThat(activity.due(T0), is(T0 + 6_000));
        activity.turn(T0 + 5_999, reads);
        assertThat(reads.count, is(0));

        reads.next(Read.held(AMQP, null));
        activity.turn(T0 + 6_000, reads);

        assertThat(reads.count, is(1));
        assertThat(activity.demand().work(), is(Work.WAITING));
    }

    @Test
    void aRequestedReadEndsTheWaitForTheNextEventOfABusyWorkload() {
        WorkloadActivity activity = busy(10);
        reads.next(Read.work(QUEUE, null));
        activity.turn(T0 + 10_000, reads);
        assertThat(activity.due(T0 + 10_000), is(Long.MAX_VALUE));

        activity.requestRead(T0 + 20_000);
        assertThat(activity.due(T0 + 10_000), is(T0 + 20_000));
        activity.turn(T0 + 20_000, reads);

        assertThat(reads.count, is(2));
        assertThat(activity.demand().work(), is(Work.NONE));
    }

    @Test
    void aResetMakesTheWorkloadIdleWithNoTimer() {
        WorkloadActivity activity = busy(10);
        activity.delayed(T0 + 50_000);

        activity.reset();

        assertThat(activity.demand().work(), is(Work.NONE));
        assertThat(activity.due(T0), is(Long.MAX_VALUE));
    }

    private WorkloadActivity busy(int idleTimeoutSeconds) {
        WorkloadActivity activity = activity(idleTimeoutSeconds);
        activity.work(T0, QUEUE);
        activity.turn(T0, reads);
        assertThat(activity.demand().work(), is(Work.WAITING));
        return activity;
    }

    private WorkloadActivity rabbitMq(int idleTimeoutSeconds) {
        Workload workload = new Workload("jobs", true, "000000000000", "us-east-1",
                Duration.ofSeconds(idleTimeoutSeconds), Duration.ofSeconds(30), List.of(),
                List.of(new RabbitMqQueue("b-1", "/", "jobs", null, null)),
                new Target("node:dev", "shop", PodSelector.Controller.deployment("jobs")));
        WorkloadActivity activity = new WorkloadActivity(workload);
        activity.work(T0, AMQP);
        activity.turn(T0, reads);
        return activity;
    }

    private static WorkloadActivity activity(int idleTimeoutSeconds) {
        return new WorkloadActivity(workload(idleTimeoutSeconds));
    }

    private static Workload workload(int idleTimeoutSeconds) {
        return new Workload("orders", true, "000000000000", "us-east-1", Duration.ofSeconds(idleTimeoutSeconds),
                Duration.ofSeconds(30), List.of("orders-incoming"), List.of(),
                new Target("node:dev", "shop", PodSelector.Controller.deployment("orders")));
    }

    /** Answers each read with the next scripted answer, or none. */
    private static final class Reads implements WorkloadActivity.Reader {

        private final Deque<Supplier<Read>> answers = new ArrayDeque<>();
        private final List<Long> tickets = new ArrayList<>();
        int count;

        void next(Read read) {
            answers.add(() -> read);
        }

        void next(Supplier<Read> read) {
            answers.add(read);
        }

        @Override
        public Read read(Workload workload, long ticket) {
            count++;
            tickets.add(ticket);
            Supplier<Read> answer = answers.poll();
            return answer == null ? Read.none(null) : answer.get();
        }
    }
}
