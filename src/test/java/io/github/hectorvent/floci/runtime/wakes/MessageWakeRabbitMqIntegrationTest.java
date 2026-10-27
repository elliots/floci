package io.github.hectorvent.floci.runtime.wakes;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.hectorvent.floci.runtime.wakes.RabbitMqTraceClient.Handler;
import io.github.hectorvent.floci.runtime.wakes.RabbitMqTraceClient.TraceSubscription;
import io.github.hectorvent.floci.services.amazonmq.AmazonMqService;
import io.github.hectorvent.floci.services.amazonmq.container.RabbitMqManager;
import io.github.hectorvent.floci.services.amazonmq.container.RabbitMqManager.QueueCounts;
import io.github.hectorvent.floci.services.amazonmq.container.RabbitMqManager.VirtualHost;
import io.github.hectorvent.floci.services.amazonmq.container.RabbitMqManager.VirtualHostQueues;
import io.github.hectorvent.floci.services.amazonmq.model.Broker;
import io.github.hectorvent.floci.services.amazonmq.model.BrokerInstance;
import io.github.hectorvent.floci.services.amazonmq.model.BrokerState;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Publishes and deliveries in a workload's RabbitMQ queues wake its pods on a node and keep them
 * awake. The broker is a fake: its firehose is the handler each subscription hands to Floci's
 * AMQP client, and the counts that a decision reads come from {@code RabbitMqManager}. Every
 * workload has a virtual host of its own, so that the reads of each can be counted.
 */
@QuarkusTest
@TestProfile(MessageWakeRabbitMqIntegrationTest.Profile.class)
class MessageWakeRabbitMqIntegrationTest {

    private static final WakeTestNode NODE = WakeTestNode.start("message-wake-rabbitmq-it");
    private static final FakeBroker BROKER = new FakeBroker();
    private static final List<String> VIRTUAL_HOSTS = List.of("/", "stream", "held", "backlog", "restart",
            "billing", "mixed");
    private static final String WORKLOADS = """
            jobs:
              idle-timeout-seconds: 2
              rabbitmq-queues: [{broker-id: b-jobs, queue: background-jobs}]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: jobs}
            stream:
              idle-timeout-seconds: 2
              rabbitmq-queues: [{broker-id: b-jobs, virtual-host: stream, queue: events}]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: stream}
            held:
              idle-timeout-seconds: 2
              rabbitmq-queues: [{broker-id: b-jobs, virtual-host: held, queue: slow-jobs}]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: held}
            backlog:
              idle-timeout-seconds: 2
              rabbitmq-queues: [{broker-id: b-jobs, virtual-host: backlog, queue: waiting-jobs}]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: backlog}
            restart:
              idle-timeout-seconds: 2
              rabbitmq-queues: [{broker-id: b-jobs, virtual-host: restart, queue: restart-jobs}]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: restart}
            billing:
              idle-timeout-seconds: 1
              rabbitmq-queues: [{broker-id: b-jobs, virtual-host: billing, queue: invoices}]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: billing}
            archive:
              region: eu-west-1
              rabbitmq-queues: [{broker-id: b-jobs, queue: archive-jobs}]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: archive}
            mixed:
              idle-timeout-seconds: 1
              queues: [mixed-incoming]
              rabbitmq-queues: [{broker-id: b-jobs, virtual-host: mixed, queue: mixed-jobs}]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: mixed}
            """;

    @InjectMock
    AmazonMqService brokers;

    @InjectMock
    RabbitMqManager containers;

    @InjectMock
    RabbitMqTraceClient client;

    @Inject
    RabbitMqQueueWatch watch;

    @TestHTTPResource
    URI endpoint;

    @BeforeEach
    void broker() throws Exception {
        Broker broker = new Broker();
        broker.setBrokerId("b-jobs");
        broker.setBrokerArn("arn:aws:mq:us-east-1:000000000000:broker:jobs:b-jobs");
        broker.setEngineType("RABBITMQ");
        broker.setBrokerState(BrokerState.RUNNING);
        broker.setBrokerInstances(List.of(new BrokerInstance("http://127.0.0.1:15672",
                List.of("amqp://127.0.0.1:5672"), "127.0.0.1")));
        when(brokers.describeBroker("b-jobs")).thenReturn(broker);
        when(containers.running("b-jobs")).thenReturn(true);
        List<VirtualHost> hosts = new ArrayList<>();
        for (String host : VIRTUAL_HOSTS) {
            hosts.add(new VirtualHost(host, false));
        }
        when(containers.virtualHosts("b-jobs")).thenReturn(hosts);
        when(containers.grantWakeUser(eq("b-jobs"), anyString(), anyString())).thenReturn(true);
        when(containers.queueCounts(eq("b-jobs"), anyString()))
                .thenAnswer(invocation -> BROKER.counts(invocation.getArgument(1)));
        when(client.subscribe(any(), any(), any())).thenAnswer(invocation -> {
            RabbitMqTraceClient.Endpoint subscribed = invocation.getArgument(0);
            return BROKER.subscribed(subscribed.virtualHost(), invocation.getArgument(1), invocation.getArgument(2));
        });
        watch.brokerRunning(broker);
        await().atMost(Duration.ofSeconds(10)).until(() -> VIRTUAL_HOSTS.stream().allMatch(BROKER::subscribed));
        await().atMost(Duration.ofSeconds(15)).until(() -> VIRTUAL_HOSTS.stream()
                .noneMatch(host -> BROKER.calls(host).isEmpty()));
    }

    @AfterAll
    static void stop() {
        NODE.close();
    }

    @Test
    void aPublishWakesThePodsAtOnceAndTheLeaseEndsOneLingerAfterTheLastEvent() throws Exception {
        NODE.pod("jobs-1", "shop", "jobs", "asleep");
        NODE.pod("jobs-2", "shop", "jobs", "asleep");
        BROKER.queue("/", "background-jobs", 0, 0);

        Instant published = Instant.now();
        BROKER.publish("/", "background-jobs");

        await().atMost(Duration.ofSeconds(1)).until(() -> NODE.state("jobs-1").equals("awake")
                && NODE.state("jobs-2").equals("awake"));
        assertThat(NODE.leases("jobs-1"), contains("floci:jobs"));
        assertThat(NODE.leases("jobs-2"), contains("floci:jobs"));
        JsonNode wake = NODE.wakes("jobs-1").getFirst().body();
        assertThat(wake.path("reason").path("kind").asText(), is("aws"));
        assertThat(wake.path("reason").path("queue").asText(), is("amqp://b-jobs/%2F/background-jobs"));
        assertThat(wake.path("source").asText(), is("floci"));

        BROKER.deliver("/", "background-jobs");
        Instant delivered = Instant.now();

        await().atMost(Duration.ofSeconds(15)).until(() -> NODE.leases("jobs-1").isEmpty()
                && NODE.leases("jobs-2").isEmpty());
        assertThat(Duration.between(delivered, Instant.now()), greaterThanOrEqualTo(Duration.ofMillis(1500)));
        assertThat(NODE.wakes("jobs-1"), hasSize(1));
        assertThat(BROKER.calls("/").size(), lessThanOrEqualTo(2));
        assertThat(published.isBefore(delivered), is(true));
    }

    @Test
    void aSteadyStreamOfQuicklyAcknowledgedMessagesKeepsOneLeaseWithNoRead() throws Exception {
        NODE.pod("stream-1", "shop", "stream", "asleep");
        int readsBefore = BROKER.calls("stream").size();

        for (int i = 0; i < 16; i++) {
            BROKER.publish("stream", "events");
            Thread.sleep(200);
            BROKER.deliver("stream", "events");
            Thread.sleep(800);
            assertThat("message " + i, NODE.leases("stream-1"), contains("floci:stream"));
        }

        assertThat(NODE.wakes("stream-1"), hasSize(1));
        assertThat(BROKER.calls("stream").size() - readsBefore, is(0));
        assertThat(NODE.requests("DELETE", WakeTestNode.leasePath("stream-1", "floci:stream")), is(empty()));

        await().atMost(Duration.ofSeconds(15)).until(() -> NODE.leases("stream-1").isEmpty());
        assertThat(BROKER.calls("stream").size() - readsBefore, is(1));
    }

    @Test
    void unacknowledgedDeliveriesKeepTheLeaseAndAreReadAgainOnceALinger() throws Exception {
        NODE.pod("held-1", "shop", "held", "asleep");
        BROKER.queue("held", "slow-jobs", 0, 1);

        BROKER.publish("held", "slow-jobs");
        BROKER.deliver("held", "slow-jobs");
        await().atMost(Duration.ofSeconds(1)).until(() -> NODE.state("held-1").equals("awake"));
        int readsBefore = BROKER.calls("held").size();

        Thread.sleep(Duration.ofSeconds(10));

        assertThat(NODE.leases("held-1"), contains("floci:held"));
        int reads = BROKER.calls("held").size() - readsBefore;
        assertThat(reads, greaterThanOrEqualTo(3));
        assertThat(reads, lessThanOrEqualTo(6));

        BROKER.queue("held", "slow-jobs", 0, 0);
        await().atMost(Duration.ofSeconds(15)).until(() -> NODE.leases("held-1").isEmpty());
        assertThat(NODE.wakes("held-1"), hasSize(1));
    }

    @Test
    void readyMessagesKeepTheLeaseAndAreReadAgainOnceALingerUntilTheyLeaveWithNoEvent() throws Exception {
        NODE.pod("backlog-1", "shop", "backlog", "asleep");
        BROKER.queue("backlog", "waiting-jobs", 3, 0);

        BROKER.publish("backlog", "waiting-jobs");
        await().atMost(Duration.ofSeconds(1)).until(() -> NODE.state("backlog-1").equals("awake"));
        int readsBefore = BROKER.calls("backlog").size();

        Thread.sleep(Duration.ofSeconds(10));

        assertThat(NODE.leases("backlog-1"), contains("floci:backlog"));
        int reads = BROKER.calls("backlog").size() - readsBefore;
        assertThat(reads, greaterThanOrEqualTo(3));
        assertThat(reads, lessThanOrEqualTo(6));

        BROKER.queue("backlog", "waiting-jobs", 0, 0);
        await().atMost(Duration.ofSeconds(15)).until(() -> NODE.leases("backlog-1").isEmpty());
        assertThat(NODE.wakes("backlog-1"), hasSize(1));
    }

    @Test
    void aBrokerRestartSubscribesAgainAndItsMessagesWakeThePods() throws Exception {
        NODE.pod("restart-1", "shop", "restart", "asleep");
        int subscriptions = BROKER.subscriptions("restart");

        BROKER.drop("restart", "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'");

        await().atMost(Duration.ofSeconds(10)).until(() -> BROKER.subscriptions("restart") == subscriptions + 1);
        verify(containers, atLeast(1)).traceOn("b-jobs", "restart");
        assertThat(NODE.wakes("restart-1"), is(empty()));

        BROKER.publish("restart", "restart-jobs");

        await().atMost(Duration.ofSeconds(1)).until(() -> NODE.state("restart-1").equals("awake"));
        BROKER.deliver("restart", "restart-jobs");
        await().atMost(Duration.ofSeconds(15)).until(() -> NODE.leases("restart-1").isEmpty());
    }

    @Test
    void aBrokerThatCannotBeReadKeepsTheLeases() throws InterruptedException {
        NODE.pod("billing-1", "shop", "billing", "asleep");
        BROKER.publish("billing", "invoices");
        await().atMost(Duration.ofSeconds(1)).until(() -> NODE.state("billing-1").equals("awake"));

        BROKER.unreadable("billing", true);
        Thread.sleep(Duration.ofSeconds(9));

        assertThat(NODE.leases("billing-1"), contains("floci:billing"));

        BROKER.unreadable("billing", false);
        await().atMost(Duration.ofSeconds(40)).until(() -> NODE.leases("billing-1").isEmpty());
        assertThat(NODE.wakes("billing-1"), hasSize(1));
    }

    @Test
    void aBrokerInAnotherRegionWakesNoPod() throws InterruptedException {
        NODE.pod("archive-1", "shop", "archive", "asleep");

        BROKER.publish("/", "archive-jobs");
        Thread.sleep(Duration.ofSeconds(3));

        assertThat(NODE.wakes("archive-1"), is(empty()));
        assertThat(NODE.leases("archive-1"), is(empty()));
    }

    @Test
    void messagesInEitherSourceWakeAWorkloadWithBoth() {
        NODE.pod("mixed-1", "shop", "mixed", "asleep");
        try (SqsClient sqs = WakeTestNode.sqs(endpoint, Region.US_EAST_1)) {
            String queueUrl = sqs.createQueue(request -> request.queueName("mixed-incoming")).queueUrl();

            BROKER.publish("mixed", "mixed-jobs");
            await().atMost(Duration.ofSeconds(1)).until(() -> NODE.state("mixed-1").equals("awake"));
            assertThat(NODE.wakes("mixed-1").getFirst().body().path("reason").path("queue").asText(),
                    is("amqp://b-jobs/mixed/mixed-jobs"));
            BROKER.deliver("mixed", "mixed-jobs");
            await().atMost(Duration.ofSeconds(15)).until(() -> NODE.leases("mixed-1").isEmpty());
            NODE.fake().state("mixed-1", "asleep");

            BROKER.unreadable("mixed", true);
            sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("order"));

            await().atMost(Duration.ofSeconds(2)).until(() -> NODE.state("mixed-1").equals("awake"));
            assertThat(NODE.wakes("mixed-1").getLast().body().path("reason").path("queue").asText(),
                    is("arn:aws:sqs:us-east-1:000000000000:mixed-incoming"));

            Message message = sqs.receiveMessage(request -> request.queueUrl(queueUrl)).messages().getFirst();
            sqs.deleteMessage(request -> request.queueUrl(queueUrl).receiptHandle(message.receiptHandle()));
            BROKER.unreadable("mixed", false);
            await().atMost(Duration.ofSeconds(40)).until(() -> NODE.leases("mixed-1").isEmpty());
        }
    }

    public static class Profile implements QuarkusTestProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            return NODE.config(WORKLOADS);
        }
    }

    /**
     * A RabbitMQ broker as Floci sees it: the firehose handler of each virtual host's
     * subscription, and the counts of each virtual host's queues.
     */
    static final class FakeBroker {

        private final Map<String, Handler> handlers = new ConcurrentHashMap<>();
        private final Map<String, Integer> subscriptions = new ConcurrentHashMap<>();
        private final Map<String, Map<String, QueueCounts>> queues = new ConcurrentHashMap<>();
        private final Set<String> unreadable = ConcurrentHashMap.newKeySet();
        private final List<String> calls = new CopyOnWriteArrayList<>();

        TraceSubscription subscribed(String virtualHost, Set<String> keys, Handler handler) {
            handlers.put(virtualHost, handler);
            subscriptions.merge(virtualHost, 1, Integer::sum);
            return new TraceSubscription() {
                @Override
                public void bind(String key) {
                }

                @Override
                public void unbind(String key) {
                }

                @Override
                public void close() {
                    handlers.remove(virtualHost, handler);
                }
            };
        }

        boolean subscribed(String virtualHost) {
            return handlers.containsKey(virtualHost);
        }

        int subscriptions(String virtualHost) {
            return subscriptions.getOrDefault(virtualHost, 0);
        }

        void publish(String virtualHost, String queue) {
            handlers.get(virtualHost).trace(new RabbitMqTrace(RabbitMqTrace.Kind.PUBLISH, "", List.of(queue)));
        }

        void deliver(String virtualHost, String queue) {
            handlers.get(virtualHost).trace(new RabbitMqTrace(RabbitMqTrace.Kind.DELIVER, queue, List.of()));
        }

        /** The broker closes the subscription's connection, as at a restart. */
        void drop(String virtualHost, String why) {
            handlers.remove(virtualHost).closed(why);
        }

        void queue(String virtualHost, String queue, long ready, long unacknowledged) {
            queues.computeIfAbsent(virtualHost, ignored -> new ConcurrentHashMap<>())
                    .put(queue, new QueueCounts(ready, unacknowledged));
        }

        void unreadable(String virtualHost, boolean failing) {
            if (failing) {
                unreadable.add(virtualHost);
            } else {
                unreadable.remove(virtualHost);
            }
        }

        List<String> calls(String virtualHost) {
            return calls.stream().filter(virtualHost::equals).toList();
        }

        VirtualHostQueues counts(String virtualHost) {
            calls.add(virtualHost);
            if (unreadable.contains(virtualHost)) {
                throw new IllegalStateException("list_queues exited with 69: the node is down");
            }
            return new VirtualHostQueues(Map.copyOf(queues.getOrDefault(virtualHost, Map.of())), Set.of());
        }
    }
}
