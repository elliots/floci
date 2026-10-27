package io.github.hectorvent.floci.services.sqs;

import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.storage.InMemoryStorage;
import io.github.hectorvent.floci.services.sqs.model.Message;
import io.github.hectorvent.floci.services.sqs.model.Queue;
import io.github.hectorvent.floci.testing.MutableClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Every change to a queue's messages reaches the queue listeners once, after the queue's lock is
 * released, and {@link SqsService#queueWork} reads a queue without changing it.
 */
class SqsQueueEventsServiceTest {

    private static final String BASE_URL = "http://localhost:4566";
    private static final String REGION = "us-east-1";
    private static final String ACCOUNT = "000000000000";

    private final FailingStore messageStore = new FailingStore();
    private final InMemoryStorage<String, Queue> queueStore = new InMemoryStorage<>();
    private SqsService sqs;
    private Recorder events;

    @BeforeEach
    void setUp() {
        sqs = new SqsService(queueStore, messageStore, new InMemoryStorage<>(), new InMemoryStorage<>(), 30, 1048576,
                BASE_URL, new RegionResolver(REGION, ACCOUNT), false, null, new MutableClock());
        events = new Recorder();
        sqs.addQueueListener(events);
    }

    @Test
    void aSendReportsAMessageAvailableNow() {
        String url = queue("orders");

        Instant before = Instant.now();
        sqs.sendMessage(url, "order", null, REGION);

        assertThat(events.kinds(), contains("available orders"));
        Instant at = events.events.getFirst().at();
        assertThat(at, greaterThanOrEqualTo(before));
        assertThat(at, lessThanOrEqualTo(Instant.now()));
    }

    @Test
    void aDelayedSendReportsWhenTheMessageBecomesVisible() {
        String url = queue("orders");

        Instant before = Instant.now();
        sqs.sendMessage(url, "later", 30, REGION);

        assertThat(events.kinds(), contains("available orders"));
        assertThat(events.events.getFirst().at(), greaterThanOrEqualTo(before.plusSeconds(30)));
    }

    @Test
    void eachEntryOfABatchIsReported() {
        String url = queue("orders");

        sqs.sendMessage(url, "one", null, REGION);
        sqs.sendMessage(url, "two", null, REGION);

        assertThat(events.kinds(), contains("available orders", "available orders"));
    }

    @Test
    void aFifoDuplicateReportsNothingAndAFifoSendReportsWithTheQueueUnlocked() {
        String url = sqs.createQueue("orders.fifo", Map.of("FifoQueue", "true"), REGION).getQueueUrl();

        sqs.sendMessage(url, "order", null, "group", "dedup-1", REGION);
        sqs.sendMessage(url, "order", null, "group", "dedup-1", REGION);

        assertThat(events.kinds(), contains("available orders.fifo"));
        assertThat(events.events.getFirst().locked(), is(false));
    }

    @Test
    void aReceiveThatClaimsReportsAChangeAndAnEmptyReceiveNothing() {
        String url = queue("orders");

        sqs.receiveMessage(url, 1, 30, 0, REGION);
        assertThat(events.kinds(), is(empty()));

        sqs.sendMessage(url, "order", null, REGION);
        sqs.receiveMessage(url, 1, 30, 0, REGION);

        assertThat(events.kinds(), contains("available orders", "changed orders"));
        assertThat(events.events.stream().anyMatch(Event::locked), is(false));
    }

    @Test
    void aDeleteReportsAChangeAndADeleteOfAMessageAlreadyGoneNothing() {
        String url = queue("orders");
        sqs.sendMessage(url, "order", null, REGION);
        String handle = sqs.receiveMessage(url, 1, 30, 0, REGION).getFirst().getReceiptHandle();
        events.events.clear();

        sqs.deleteMessage(url, handle, REGION);
        sqs.deleteMessage(url, handle, REGION);

        assertThat(events.kinds(), contains("changed orders"));
    }

    @Test
    void aVisibilityChangeReportsAChange() {
        String url = queue("orders");
        sqs.sendMessage(url, "order", null, REGION);
        String handle = sqs.receiveMessage(url, 1, 30, 0, REGION).getFirst().getReceiptHandle();
        events.events.clear();

        sqs.changeMessageVisibility(url, handle, 0, REGION);
        sqs.changeMessageVisibilityBatch(url,
                List.of(new SqsService.ChangeVisibilityBatchEntry("1", handle, 10)), REGION);

        assertThat(events.kinds(), contains("changed orders", "changed orders"));
    }

    @Test
    void aReceivePastTheMaximumMovesTheMessageAndReportsBothQueues() {
        queue("orders-dlq");
        String url = sqs.createQueue("orders", Map.of("RedrivePolicy",
                "{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:orders-dlq\",\"maxReceiveCount\":\"1\"}"),
                REGION).getQueueUrl();
        sqs.sendMessage(url, "poison", null, REGION);
        String handle = sqs.receiveMessage(url, 1, 30, 0, REGION).getFirst().getReceiptHandle();
        sqs.changeMessageVisibility(url, handle, 0, REGION);
        events.events.clear();

        assertThat(sqs.receiveMessage(url, 1, 30, 0, REGION), is(empty()));

        assertThat(events.kinds(), hasItem("available orders-dlq"));
        assertThat(events.kinds().getFirst(), is("changed orders"));
        assertThat(events.kinds().getLast(), is("available orders-dlq"));
        assertThat(sqs.queueWork(id("orders-dlq")).orElseThrow().visible(), is(1L));
        assertThat(sqs.queueWork(id("orders")).orElseThrow().any(), is(false));
    }

    @Test
    void aPurgeAndADeletedQueueReportChanges() {
        String url = queue("orders");
        sqs.sendMessage(url, "order", null, REGION);
        events.events.clear();

        sqs.purgeQueue(url, REGION);
        sqs.deleteQueue(url, REGION);

        assertThat(events.kinds(), contains("changed orders", "changed orders"));
    }

    @Test
    void anExpiredMessageRemovedByARead() {
        String url = sqs.createQueue("orders", Map.of("MessageRetentionPeriod", "60"), REGION).getQueueUrl();
        sqs.sendMessage(url, "old", null, REGION);
        sqs.peekMessages(url, REGION).getFirst().setSentTimestamp(Instant.now().minusSeconds(120));
        events.events.clear();

        assertThat(sqs.queueWork(id("orders")).orElseThrow().any(), is(false));
        assertThat(events.kinds(), is(empty()));

        sqs.getQueueAttributes(url, List.of("All"), REGION);

        assertThat(events.kinds(), contains("changed orders"));
    }

    @Test
    void aMoveTaskReportsTheSourceAndTheDestination() {
        String dlq = queue("orders-dlq");
        sqs.createQueue("orders", Map.of("RedrivePolicy",
                "{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:orders-dlq\",\"maxReceiveCount\":\"1\"}"),
                REGION);
        queue("orders-replay");
        sqs.sendMessage(dlq, "poison", null, REGION);
        events.events.clear();

        sqs.startMessageMoveTask("arn:aws:sqs:us-east-1:000000000000:orders-dlq",
                "arn:aws:sqs:us-east-1:000000000000:orders-replay", REGION);

        await().atMost(Duration.ofSeconds(5)).until(() -> events.kinds().contains("available orders-replay"));
        assertThat(events.kinds(), contains("changed orders-dlq", "available orders-replay"));
        assertThat(sqs.queueWork(id("orders-replay")).orElseThrow().visible(), is(1L));
    }

    @Test
    void aFailedPersistReportsNothing() {
        String url = queue("orders");
        messageStore.failing = true;

        assertThrows(RuntimeException.class, () -> sqs.sendMessage(url, "order", null, REGION));

        assertThat(events.kinds(), is(empty()));
    }

    @Test
    void aListenerThatThrowsNeverFailsTheRequest() {
        String url = queue("orders");
        sqs.addQueueListener(new SqsQueueListener() {
            @Override
            public void messageAvailable(SqsQueueId queue, Instant at) {
                throw new IllegalStateException("broken listener");
            }

            @Override
            public void messagesChanged(SqsQueueId queue) {
                throw new IllegalStateException("broken listener");
            }
        });

        sqs.sendMessage(url, "order", null, REGION);
        sqs.receiveMessage(url, 1, 30, 0, REGION);

        assertThat(events.kinds(), contains("available orders", "changed orders"));
    }

    @Test
    void queueWorkCountsVisibleInFlightAndTheFirstDelayedTime() {
        String url = queue("orders");
        sqs.sendMessage(url, "now", null, REGION);
        sqs.sendMessage(url, "in flight", null, REGION);
        sqs.receiveMessage(url, 1, 30, 0, REGION);
        Instant before = Instant.now();
        sqs.sendMessage(url, "later", 60, REGION);
        sqs.sendMessage(url, "soon", 20, REGION);
        events.events.clear();

        SqsService.QueueWork work = sqs.queueWork(id("orders")).orElseThrow();

        assertThat(work.visible(), is(1L));
        assertThat(work.inFlight(), is(1L));
        assertThat(work.nextVisible(), greaterThanOrEqualTo(before.plusSeconds(20)));
        assertThat(work.nextVisible().isBefore(before.plusSeconds(59)), is(true));
        assertThat(events.kinds(), is(empty()));
    }

    @Test
    void queueWorkOfAQueueWithNoMessagesIsEmpty() {
        queue("orders");

        SqsService.QueueWork work = sqs.queueWork(id("orders")).orElseThrow();

        assertThat(work.any(), is(false));
        assertThat(work.nextVisible(), is(nullValue()));
    }

    @Test
    void queueWorkOfAMissingQueueIsEmptyAndCreatesNothing() {
        assertThat(sqs.queueWork(id("missing")), is(Optional.empty()));
        assertThat(sqs.queueWork(new SqsQueueId("111111111111", REGION, "orders")), is(Optional.empty()));
        assertThat(queueStore.keys(), is(empty()));
        assertThat(messageStore.keys(), is(empty()));
    }

    @Test
    void aMessageAddedToADeletedQueueReportsForTheSameQueueAndTheNextReadIsEmpty() {
        String dlq = queue("orders-dlq");
        String url = sqs.createQueue("orders", Map.of("RedrivePolicy",
                "{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:orders-dlq\",\"maxReceiveCount\":\"1\"}"),
                REGION).getQueueUrl();
        sqs.sendMessage(url, "poison", null, REGION);
        String handle = sqs.receiveMessage(url, 1, 30, 0, REGION).getFirst().getReceiptHandle();
        sqs.changeMessageVisibility(url, handle, 0, REGION);
        sqs.deleteQueue(dlq, REGION);
        events.events.clear();

        sqs.receiveMessage(url, 1, 30, 0, REGION);

        assertThat(events.kinds().getLast(), is("available orders-dlq"));
        assertThat(events.events.getLast().queue(), is(id("orders-dlq")));
        assertThat(sqs.queueWork(id("orders-dlq")), is(Optional.empty()));
        assertThrows(AwsException.class, () -> sqs.getQueueUrl("orders-dlq", REGION));
    }

    @Test
    void storedMessagesReportNothingWhenLoaded() {
        String url = queue("orders");
        sqs.sendMessage(url, "stored", null, REGION);

        SqsService restarted = new SqsService(queueStore, messageStore, new InMemoryStorage<>(),
                new InMemoryStorage<>(), 30, 1048576, BASE_URL, new RegionResolver(REGION, ACCOUNT), false, null,
                new MutableClock());
        Recorder after = new Recorder();
        restarted.addQueueListener(after);

        assertThat(restarted.queueWork(id("orders")).orElseThrow().visible(), is(1L));
        restarted.receiveMessage(url, 1, 30, 0, REGION);
        assertThat(after.kinds(), contains("changed orders"));
    }

    @Test
    void aQueueIdIsReadFromItsStorageKey() {
        assertThat(SqsQueueId.ofStorageKey("eu-west-1::/123456789012/jobs"),
                is(new SqsQueueId("123456789012", "eu-west-1", "jobs")));
        assertThat(new SqsQueueId("123456789012", "eu-west-1", "jobs").storageKey(),
                is("eu-west-1::/123456789012/jobs"));
        assertThat(SqsQueueId.ofStorageKey("jobs"), is(nullValue()));
        assertThat(SqsQueueId.ofStorageKey("eu-west-1::/jobs"), is(nullValue()));
    }

    private String queue(String name) {
        return sqs.createQueue(name, null, REGION).getQueueUrl();
    }

    private static SqsQueueId id(String name) {
        return new SqsQueueId(ACCOUNT, REGION, name);
    }

    private record Event(String kind, SqsQueueId queue, Instant at, boolean locked) {
    }

    /** Records each event and whether the queue's lock was held while it was reported. */
    private final class Recorder implements SqsQueueListener {

        final List<Event> events = new ArrayList<>();

        @Override
        public synchronized void messageAvailable(SqsQueueId queue, Instant at) {
            events.add(new Event("available", queue, at, sqs.holdsQueueLock(queue)));
        }

        @Override
        public synchronized void messagesChanged(SqsQueueId queue) {
            events.add(new Event("changed", queue, null, sqs.holdsQueueLock(queue)));
        }

        synchronized List<String> kinds() {
            return events.stream().map(event -> event.kind() + " " + event.queue().name()).toList();
        }
    }

    private static final class FailingStore extends InMemoryStorage<String, List<Message>> {

        volatile boolean failing;

        @Override
        public void put(String key, List<Message> value) {
            if (failing) {
                throw new IllegalStateException("the disk is full");
            }
            super.put(key, value);
        }
    }
}
