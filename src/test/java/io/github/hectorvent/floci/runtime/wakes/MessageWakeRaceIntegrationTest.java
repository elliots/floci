package io.github.hectorvent.floci.runtime.wakes;

import io.github.hectorvent.floci.runtime.node.FakeNodeApi;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

/**
 * Messages that meet a consumer at the edges of its sleep: claimed by a poll it parked before it
 * slept, sent while it pauses, while a wake is under way, while its lease lingers, or as its lease
 * is released.
 */
@QuarkusTest
@TestProfile(MessageWakeRaceIntegrationTest.Profile.class)
class MessageWakeRaceIntegrationTest {

    private static final WakeTestNode NODE = WakeTestNode.start("message-wake-race-it");
    private static final String WORKLOADS = """
            parked:
              idle-timeout-seconds: 1
              queues: [parked-incoming]
              runtime: {type: kubernetes, node: dev, namespace: jobs, deployment: parked}
            pausing:
              queues: [pausing-incoming]
              runtime: {type: kubernetes, node: dev, namespace: jobs, deployment: pausing}
            slow:
              queues: [slow-incoming]
              runtime: {type: kubernetes, node: dev, namespace: jobs, deployment: slow}
            burst:
              idle-timeout-seconds: 3
              queues: [burst-incoming]
              runtime: {type: kubernetes, node: dev, namespace: jobs, deployment: burst}
            edge:
              idle-timeout-seconds: 1
              queues: [edge-incoming]
              runtime: {type: kubernetes, node: dev, namespace: jobs, deployment: edge}
            delayed:
              idle-timeout-seconds: 1
              queues: [delayed-incoming]
              runtime: {type: kubernetes, node: dev, namespace: jobs, deployment: delayed}
            """;

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
    void aMessageThatAParkedPollClaimsWakesThePod() throws Exception {
        NODE.pod("parked-1", "jobs", "parked", "asleep");
        String queueUrl = sqs.createQueue(request -> request.queueName("parked-incoming")
                .attributes(Map.of(QueueAttributeName.VISIBILITY_TIMEOUT, "30"))).queueUrl();
        CompletableFuture<List<Message>> parked = CompletableFuture.supplyAsync(() ->
                sqs.receiveMessage(request -> request.queueUrl(queueUrl).waitTimeSeconds(20)
                        .messageSystemAttributeNames(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT))
                        .messages());
        Thread.sleep(Duration.ofMillis(500));

        sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("job 1"));
        Message message = parked.get(5, TimeUnit.SECONDS).getFirst();

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("parked-1").equals("awake"));
        assertThat(NODE.leases("parked-1"), contains("floci:parked"));
        assertThat(message.attributes().get(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT), is("1"));

        sqs.deleteMessage(request -> request.queueUrl(queueUrl).receiptHandle(message.receiptHandle()));

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("parked-1").isEmpty());
        assertThat(NODE.wakes("parked-1"), hasSize(1));
    }

    @Test
    void podsAreWokenAfterTheirLeaseWhetherAwakeOrPausing() {
        NODE.pod("pausing-1", "jobs", "pausing", "awake");
        NODE.pod("pausing-2", "jobs", "pausing", "pausing");
        String queueUrl = sqs.createQueue(request -> request.queueName("pausing-incoming")).queueUrl();

        sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("job 2"));

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.wakes("pausing-1").size() == 1
                && NODE.wakes("pausing-2").size() == 1);
        for (String sandbox : List.of("pausing-1", "pausing-2")) {
            List<FakeNodeApi.Request> requests = NODE.fake().requests();
            int lease = requests.indexOf(NODE.requests("PUT", WakeTestNode.leasePath(sandbox, "floci:pausing"))
                    .getFirst());
            int wake = requests.indexOf(NODE.wakes(sandbox).getFirst());
            assertThat(lease, lessThan(wake));
            assertThat(NODE.state(sandbox), is("awake"));
        }
    }

    @Test
    void aPodGetsOneWakeAtATime() throws InterruptedException {
        NODE.pod("slow-1", "jobs", "slow", "asleep");
        NODE.fake().delayNext("POST", "/v1/pods/slow-1/wake", Duration.ofSeconds(3));
        String queueUrl = sqs.createQueue(request -> request.queueName("slow-incoming")).queueUrl();

        sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("job 3"));
        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.wakes("slow-1").size() == 1);
        sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("job 4"));
        Thread.sleep(Duration.ofSeconds(2));

        assertThat(NODE.wakes("slow-1"), hasSize(1));

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("slow-1").equals("awake"));
        Thread.sleep(Duration.ofSeconds(1));
        assertThat(NODE.wakes("slow-1"), hasSize(1));
    }

    @Test
    void messagesWhileTheLeaseLingersNeedNoNewWake() throws InterruptedException {
        NODE.pod("burst-1", "jobs", "burst", "asleep");
        String queueUrl = sqs.createQueue(request -> request.queueName("burst-incoming")).queueUrl();

        for (int i = 0; i < 3; i++) {
            String body = "burst " + i;
            sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody(body));
            await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("burst-1").equals("awake"));
            Message message = sqs.receiveMessage(request -> request.queueUrl(queueUrl).waitTimeSeconds(5))
                    .messages().getFirst();
            sqs.deleteMessage(request -> request.queueUrl(queueUrl).receiptHandle(message.receiptHandle()));
            Thread.sleep(Duration.ofSeconds(1));
        }

        assertThat(NODE.leases("burst-1"), contains("floci:burst"));
        assertThat(NODE.wakes("burst-1"), hasSize(1));
        assertThat(NODE.requests("DELETE", WakeTestNode.leasePath("burst-1", "floci:burst")), hasSize(0));
        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("burst-1").isEmpty());
    }

    @Test
    void aMessageSentAsTheLeaseIsReleasedIsNeverLeftWithoutALease() throws InterruptedException {
        NODE.pod("edge-1", "jobs", "edge", "asleep");
        String queueUrl = sqs.createQueue(request -> request.queueName("edge-incoming")).queueUrl();
        sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("edge"));

        for (int i = 0; i < 8; i++) {
            await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("edge-1").contains("floci:edge"));
            receiveAndDelete(queueUrl);
            Thread.sleep(Duration.ofMillis(850 + 40L * i));
            String body = "edge " + i;
            sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody(body));
            Thread.sleep(Duration.ofMillis(2500));

            assertThat(NODE.leases("edge-1"), contains("floci:edge"));
        }
        receiveAndDelete(queueUrl);

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("edge-1").isEmpty());
    }

    @Test
    void aDelayedMessageSentAsTheLeaseIsReleasedWakesThePod() throws InterruptedException {
        NODE.pod("delayed-1", "jobs", "delayed", "asleep");
        String queueUrl = sqs.createQueue(request -> request.queueName("delayed-incoming")).queueUrl();
        sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("delayed"));

        for (int i = 0; i < 5; i++) {
            await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("delayed-1").contains("floci:delayed"));
            receiveAndDelete(queueUrl);
            Thread.sleep(Duration.ofMillis(850 + 60L * i));
            String body = "delayed " + i;
            sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody(body).delaySeconds(1));
            Thread.sleep(Duration.ofMillis(3500));

            assertThat(NODE.leases("delayed-1"), contains("floci:delayed"));
        }
        receiveAndDelete(queueUrl);

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("delayed-1").isEmpty());
    }

    private void receiveAndDelete(String queueUrl) {
        Message message = sqs.receiveMessage(request -> request.queueUrl(queueUrl).waitTimeSeconds(5))
                .messages().getFirst();
        sqs.deleteMessage(request -> request.queueUrl(queueUrl).receiptHandle(message.receiptHandle()));
    }

    public static class Profile implements QuarkusTestProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            return NODE.config(WORKLOADS);
        }
    }
}
