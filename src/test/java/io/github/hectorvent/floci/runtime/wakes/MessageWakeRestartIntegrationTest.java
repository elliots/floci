package io.github.hectorvent.floci.runtime.wakes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.core.storage.PersistentStorage;
import io.github.hectorvent.floci.runtime.node.NodeToken;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import io.github.hectorvent.floci.services.sqs.model.Message;
import io.github.hectorvent.floci.services.sqs.model.Queue;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Floci started again with persistent storage, while a message of the last run waits in a queue
 * and its consumer sleeps on a node that still holds the last run's leases: the read of each
 * watched queue at startup wakes the consumer under the same lease id, and its listing deletes the
 * leases that no workload wants. A message that was delayed in the last run is visible after the
 * restart and wakes its consumer too. An emulator reset then releases Floci's leases, and a
 * message sent after it wakes the consumer again.
 */
@QuarkusTest
@TestProfile(MessageWakeRestartIntegrationTest.Profile.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MessageWakeRestartIntegrationTest {

    private static final WakeTestNode NODE = WakeTestNode.start("message-wake-restart-it");
    private static final String ACCOUNT = "000000000000";
    private static final String WORKLOADS = """
            orders:
              queues: [orders-incoming]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: orders}
            later:
              queues: [later-incoming]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: later}
            """;

    @TestHTTPResource
    URI endpoint;

    @AfterAll
    static void stopNode() {
        NODE.close();
    }

    @Test
    @Order(1)
    void theMessagesOfTheLastRunWakeTheConsumerUnderItsLease() {
        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("orders-1").equals("awake"));
        await().atMost(Duration.ofSeconds(10)).until(() -> !NODE.leases("orders-1").contains("floci:retired"));

        assertThat(NODE.leases("orders-1"), containsInAnyOrder("floci:orders", "floci:other:orders"));
        assertThat(NODE.requests("PUT", WakeTestNode.leasePath("orders-1", "floci:orders")), hasSize(2));
        assertThat(NODE.requests("DELETE", WakeTestNode.leasePath("orders-1", "floci:retired")), hasSize(1));
        assertThat(NODE.wakes("orders-1"), hasSize(1));
        try (SqsClient sqs = WakeTestNode.sqs(endpoint, Region.US_EAST_1)) {
            String queueUrl = sqs.getQueueUrl(request -> request.queueName("orders-incoming")).queueUrl();
            String body = sqs.receiveMessage(request -> request.queueUrl(queueUrl)).messages().getFirst().body();
            assertThat(body, is("order of the last run"));
        }
    }

    @Test
    @Order(2)
    void aMessageDelayedInTheLastRunWakesItsConsumerAtStartup() {
        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("later-1").equals("awake"));

        assertThat(NODE.leases("later-1"), containsInAnyOrder("floci:later"));
    }

    @Test
    @Order(3)
    void anEmulatorResetReleasesFlocisLeases() {
        assertThat(NODE.leases("orders-1"), containsInAnyOrder("floci:orders", "floci:other:orders"));

        given().when().post("/_floci/state/reset").then().statusCode(200);

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("orders-1").equals(List.of(
                "floci:other:orders")));
        assertThat(NODE.requests("DELETE", WakeTestNode.leasePath("orders-1", "floci:orders")), hasSize(1));
        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("later-1").isEmpty());
    }

    @Test
    @Order(4)
    void aMessageSentAfterTheResetWakesTheConsumer() {
        NODE.fake().state("orders-1", "asleep");
        try (SqsClient sqs = WakeTestNode.sqs(endpoint, Region.US_EAST_1)) {
            String queueUrl = sqs.createQueue(request -> request.queueName("orders-incoming")).queueUrl();
            sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("order after the reset"));
        }

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("orders-1").equals("awake"));
        assertThat(NODE.leases("orders-1"), containsInAnyOrder("floci:orders", "floci:other:orders"));
    }

    public static class Profile implements QuarkusTestProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            Path data = NODE.dir().resolve("data");
            lastRun(data);
            Map<String, String> config = new HashMap<>(NODE.config(WORKLOADS));
            config.put("floci.storage.mode", "persistent");
            config.put("floci.storage.persistent-path", data.toString());
            return config;
        }

        /**
         * What the last run left: the queue and its message in SQS's store files, keyed as SQS
         * keys them, and the sleeping consumer with the last run's leases on the node.
         */
        private static void lastRun(Path data) {
            String key = ACCOUNT + "/us-east-1::/" + ACCOUNT + "/orders-incoming";
            Queue queue = new Queue("orders-incoming", "http://localhost:4566/" + ACCOUNT + "/orders-incoming");
            queue.setAccountId(ACCOUNT);
            String laterKey = ACCOUNT + "/us-east-1::/" + ACCOUNT + "/later-incoming";
            Queue later = new Queue("later-incoming", "http://localhost:4566/" + ACCOUNT + "/later-incoming");
            later.setAccountId(ACCOUNT);
            PersistentStorage<String, Queue> queues = new PersistentStorage<>(data.resolve("sqs-queues.json"),
                    new TypeReference<Map<String, Queue>>() { });
            queues.put(key, queue);
            queues.put(laterKey, later);
            Message delayed = new Message("delayed in the last run");
            delayed.setVisibleAt(Instant.now().plus(Duration.ofHours(1)));
            PersistentStorage<String, List<Message>> messages = new PersistentStorage<>(
                    data.resolve("sqs-messages.json"), new TypeReference<Map<String, List<Message>>>() { });
            messages.put(key, List.of(new Message("order of the last run")));
            messages.put(laterKey, List.of(delayed));

            NODE.pod("orders-1", "shop", "orders", "asleep");
            NODE.pod("later-1", "shop", "later", "asleep");
            SmolvmNode node = NODE.fake().node("dev", NodeToken.file(NODE.dir().resolve("node-token")));
            SmolvmNodeClient client = new SmolvmNodeClient(new ObjectMapper());
            for (String lease : List.of("floci:orders", "floci:retired", "floci:other:orders")) {
                client.putLease(node, "orders-1", lease, 60, "floci wakes: last run", null);
            }
        }
    }
}
