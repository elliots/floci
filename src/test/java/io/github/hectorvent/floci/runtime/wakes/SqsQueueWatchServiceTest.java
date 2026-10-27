package io.github.hectorvent.floci.runtime.wakes;

import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.runtime.wakes.LeaseKeeper.Work;
import io.github.hectorvent.floci.runtime.wakes.WorkloadActivity.Found;
import io.github.hectorvent.floci.runtime.wakes.WorkloadActivity.Read;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.PodSelector;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Target;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Workload;
import io.github.hectorvent.floci.services.sqs.SqsQueueId;
import io.github.hectorvent.floci.services.sqs.SqsService;
import io.github.hectorvent.floci.services.sqs.SqsService.QueueWork;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SqsQueueWatchServiceTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final SqsQueueId ORDERS = new SqsQueueId("000000000000", "us-east-1", "orders-incoming");
    private static final SqsQueueId AUDIT = new SqsQueueId("000000000000", "us-east-1", "audit-incoming");

    private final SqsService sqs = mock(SqsService.class);
    private final SqsQueueWatch watch = new SqsQueueWatch(sqs, new RegionResolver("us-east-1", "000000000000"),
            InstantSource.fixed(NOW));
    private int looks;

    @BeforeEach
    void start() {
        watch.start(() -> looks++);
    }

    @Test
    void startingListensToSqsOnce() {
        watch.start(() -> looks++);

        verify(sqs).addQueueListener(watch);
    }

    @Test
    void aMessageWakesEveryWorkloadThatReadsTheQueue() {
        WorkloadActivity orders = activity("orders", "orders-incoming");
        WorkloadActivity billing = activity("billing", "orders-incoming", "audit-incoming");
        watch.watch(List.of(orders, billing));

        watch.messageAvailable(ORDERS, NOW);

        assertThat(looks, is(1));
        orders.turn(NOW.toEpochMilli(), (workload, ticket) -> Read.none(null));
        billing.turn(NOW.toEpochMilli(), (workload, ticket) -> Read.none(null));
        assertThat(orders.demand().work(), is(Work.WAITING));
        assertThat(orders.demand().resource(), is("arn:aws:sqs:us-east-1:000000000000:orders-incoming"));
        assertThat(billing.demand().work(), is(Work.WAITING));
    }

    @Test
    void aQueueNoWorkloadReadsChangesNothing() {
        WorkloadActivity orders = activity("orders", "orders-incoming");
        watch.watch(List.of(orders));

        watch.messageAvailable(AUDIT, NOW);
        watch.messagesChanged(AUDIT);
        watch.messageAvailable(new SqsQueueId("111111111111", "us-east-1", "orders-incoming"), NOW);

        assertThat(looks, is(0));
        assertThat(orders.due(NOW.toEpochMilli()), is(Long.MAX_VALUE));
    }

    @Test
    void aDelayedMessageSetsATimerAndAChangeIsActivity() {
        WorkloadActivity orders = activity("orders", "orders-incoming");
        watch.watch(List.of(orders));

        watch.messageAvailable(ORDERS, NOW.plusSeconds(30));
        watch.messagesChanged(ORDERS);

        assertThat(looks, is(1));
        assertThat(orders.due(NOW.toEpochMilli()), is(NOW.plusSeconds(30).toEpochMilli()));
        assertThat(orders.demand().work(), is(Work.NONE));
    }

    @Test
    void aReloadSwapsTheWatchedQueues() {
        WorkloadActivity orders = activity("orders", "orders-incoming");
        watch.watch(List.of(orders));
        WorkloadActivity audit = activity("audit", "audit-incoming");

        watch.watch(List.of(audit));
        watch.messageAvailable(ORDERS, NOW);
        assertThat(looks, is(0));

        watch.messageAvailable(AUDIT, NOW);
        assertThat(looks, is(1));
    }

    @Test
    void aReadFindsWorkInTheFirstQueueWithMessagesAndTheEarliestDelayedTime() {
        Workload workload = workload("billing", "orders-incoming", "audit-incoming");
        when(sqs.queueWork(ORDERS)).thenReturn(Optional.of(new QueueWork(0, 0, NOW.plusSeconds(40))));
        when(sqs.queueWork(AUDIT)).thenReturn(Optional.of(new QueueWork(0, 1, NOW.plusSeconds(20))));

        Read read = watch.read(workload);

        assertThat(read.found(), is(Found.WORK));
        assertThat(read.resource(), is("arn:aws:sqs:us-east-1:000000000000:audit-incoming"));
        assertThat(read.nextVisible(), is(NOW.plusSeconds(20)));
        assertThat(watch.reads("billing"), is(1L));
    }

    @Test
    void aMissingQueueHasNoWork() {
        when(sqs.queueWork(ORDERS)).thenReturn(Optional.empty());

        Read read = watch.read(workload("orders", "orders-incoming"));

        assertThat(read.found(), is(Found.NONE));
        assertThat(read.nextVisible(), is(nullValue()));
    }

    @Test
    void aQueueThatCannotBeReadFailsTheRead() {
        when(sqs.queueWork(ORDERS)).thenThrow(new IllegalStateException("the disk is gone"));
        when(sqs.queueWork(AUDIT)).thenReturn(Optional.of(new QueueWork(0, 0, null)));

        Read read = watch.read(workload("billing", "orders-incoming", "audit-incoming"));

        assertThat(read.found(), is(Found.FAILED));
    }

    private static WorkloadActivity activity(String name, String... queues) {
        return new WorkloadActivity(workload(name, queues));
    }

    private static Workload workload(String name, String... queues) {
        return new Workload(name, true, "000000000000", "us-east-1", Duration.ofSeconds(10), Duration.ofSeconds(30),
                List.of(queues), List.of(), new Target("node:dev", "shop", PodSelector.Controller.deployment(name)));
    }
}
