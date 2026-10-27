package io.github.hectorvent.floci.runtime.wakes;

import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

/**
 * Changes of the runtime file's workloads while Floci runs: each applies as Floci reads the file,
 * the queues of an added workload are read once, the leases of a workload that leaves, or that
 * is disabled, are released, and a busy workload takes its new queues and pods. Each test starts
 * from the file that the one before it left.
 */
@QuarkusTest
@TestProfile(MessageWakeReloadIntegrationTest.Profile.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MessageWakeReloadIntegrationTest {

    private static final WakeTestNode NODE = WakeTestNode.start("message-wake-reload-it");
    private static final String ORDERS = """
            orders:
              queues: [orders-incoming]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: orders}
            """;
    private static final String LATE = """
            late:
              queues: [late-incoming]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: late}
            """;

    @Inject
    MessageWakeService wakes;

    @TestHTTPResource
    URI endpoint;

    private SqsClient sqs;

    @BeforeEach
    void client() {
        sqs = WakeTestNode.sqs(endpoint, Region.US_EAST_1);
    }

    @AfterEach
    void closeClient() {
        sqs.close();
    }

    @AfterAll
    static void stopNode() {
        NODE.close();
    }

    @Test
    @Order(1)
    void aWorkloadAddedByAReloadWakesItsPods() throws InterruptedException {
        NODE.pod("late-1", "shop", "late", "asleep");
        send("late-incoming");
        Thread.sleep(Duration.ofSeconds(1));

        assertThat(NODE.wakes("late-1"), is(empty()));

        NODE.write(ORDERS + LATE);

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("late-1").equals("awake"));
        assertThat(NODE.leases("late-1"), contains("floci:late"));
    }

    @Test
    @Order(2)
    void anInvalidReloadKeepsTheWorkloadsInForce() throws InterruptedException {
        NODE.pod("orders-1", "shop", "orders", "asleep");
        NODE.writeFile("version: 1\nworkloads:\n  late:\n    replicas: 2\n");
        Thread.sleep(Duration.ofSeconds(3));

        send("orders-incoming");

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("orders-1").equals("awake"));
        assertThat(NODE.leases("late-1"), contains("floci:late"));
    }

    @Test
    @Order(3)
    void aWorkloadRemovedByAReloadLosesItsLeases() {
        NODE.write(ORDERS);

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("late-1").isEmpty());
        assertThat(NODE.leases("orders-1"), contains("floci:orders"));
    }

    @Test
    @Order(4)
    void aDisabledWorkloadLosesItsLeasesAndTheLoopStops() {
        NODE.write(ORDERS.replace("orders:\n", "orders:\n  enabled: false\n"));

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("orders-1").isEmpty());
        await().atMost(Duration.ofSeconds(5)).until(() -> !wakes.running());
    }

    @Test
    @Order(5)
    void aNewInstanceIdStartsTheLoopAndMovesTheLeases() {
        NODE.write(ORDERS);
        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("orders-1").equals(List.of("floci:orders")));

        NODE.writeFile("version: 1\ninstance-id: laptop\nnodes:\n  dev:\n    url: " + NODE.fake().url()
                + "\n    token-file: node-token\nworkloads:\n" + ORDERS.indent(2));

        await().atMost(Duration.ofSeconds(10))
                .until(() -> NODE.leases("orders-1").equals(List.of("floci:laptop:orders")));
        assertThat(wakes.running(), is(true));
    }

    @Test
    @Order(6)
    void aNewIdleTimeoutKeepsTheLeaseAndMovesItsRelease() {
        NODE.write(ORDERS.replace("orders:\n", "orders:\n  idle-timeout-seconds: 1\n"));
        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("orders-1").equals(List.of("floci:orders")));

        String queueUrl = sqs.getQueueUrl(request -> request.queueName("orders-incoming")).queueUrl();
        Message message = sqs.receiveMessage(request -> request.queueUrl(queueUrl)).messages().getFirst();
        sqs.deleteMessage(request -> request.queueUrl(queueUrl).receiptHandle(message.receiptHandle()));
        Instant deleted = Instant.now();

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("orders-1").isEmpty());
        assertThat(Duration.between(deleted, Instant.now()), lessThan(Duration.ofSeconds(5)));
    }

    @Test
    @Order(7)
    void aBusyWorkloadThatWaitsForAnEventOfAQueueItNoLongerReadsReleases() throws InterruptedException {
        String fast = ORDERS.replace("orders:\n", "orders:\n  idle-timeout-seconds: 1\n");
        send("orders-incoming");
        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("orders-1").equals(List.of("floci:orders")));
        Thread.sleep(Duration.ofSeconds(3));
        assertThat(NODE.leases("orders-1"), contains("floci:orders"));

        NODE.write(fast.replace("[orders-incoming]", "[orders-retry]"));

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("orders-1").isEmpty());
    }

    @Test
    @Order(8)
    void aNewNamespaceOfABusyWorkloadMovesItsLeases() {
        String fast = ORDERS.replace("orders:\n", "orders:\n  idle-timeout-seconds: 1\n");
        NODE.pod("orders-2", "stock", "orders", "asleep");
        NODE.write(fast);
        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("orders-1").equals(List.of("floci:orders")));

        NODE.write(fast.replace("namespace: shop", "namespace: stock"));

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("orders-2").equals(List.of("floci:orders")));
        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("orders-1").isEmpty());
        assertThat(NODE.state("orders-2"), is("awake"));
    }

    private void send(String queue) {
        String queueUrl = sqs.createQueue(request -> request.queueName(queue)).queueUrl();
        sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("message for " + queue));
    }

    public static class Profile implements QuarkusTestProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            return NODE.config(ORDERS);
        }
    }
}
