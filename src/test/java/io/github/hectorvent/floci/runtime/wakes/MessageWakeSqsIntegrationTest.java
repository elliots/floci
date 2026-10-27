package io.github.hectorvent.floci.runtime.wakes;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.hectorvent.floci.runtime.node.FakeNodeApi;
import io.github.hectorvent.floci.testing.RestAssuredJsonUtils;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iot.IotClient;
import software.amazon.awssdk.services.iotdataplane.IotDataPlaneClient;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

/**
 * Messages in a workload's SQS queues wake its pods on a node, through the node service's API,
 * whoever sends them: an SQS client, an SNS subscription, an EventBridge rule, an IoT rule or a
 * receive past a queue's maximum. The leases last while the consumer works and for the
 * workload's idle timeout after its queues' last event.
 */
@QuarkusTest
@TestProfile(MessageWakeSqsIntegrationTest.Profile.class)
class MessageWakeSqsIntegrationTest {

    private static final WakeTestNode NODE = WakeTestNode.start("message-wake-sqs-it");
    private static final String EVENTS_JSON = "application/x-amz-json-1.1";
    private static final String WORKLOADS = """
            orders:
              idle-timeout-seconds: 3
              queues: [orders-incoming]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: orders}
            reports:
              queues: [reports-incoming]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: reports}
            audit:
              idle-timeout-seconds: 1
              queues: [audit-incoming]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: audit}
            retried:
              idle-timeout-seconds: 1
              queues: [retried-incoming]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: retried}
            stream:
              idle-timeout-seconds: 2
              queues: [stream-incoming]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: stream}
            purged:
              idle-timeout-seconds: 2
              queues: [purged-incoming]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: purged}
            alerts:
              queues: [alerts-incoming]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: alerts}
            events:
              queues: [events-incoming]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: events}
            telemetry:
              queues: [telemetry-incoming]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: telemetry}
            drainer:
              queues: [drain-dlq]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: drainer}
            quiet:
              queues: [quiet-incoming]
              runtime: {type: kubernetes, node: dev, namespace: quiet, deployment: quiet}
            billing:
              region: eu-west-1
              queues: [billing-incoming]
              runtime: {type: kubernetes, node: dev, namespace: shop, selector: {app: billing}}
            """;

    @TestHTTPResource
    URI endpoint;

    @Inject
    SqsQueueWatch watch;

    private SqsClient sqs;

    @BeforeAll
    static void configureRestAssured() {
        RestAssuredJsonUtils.configureAwsContentTypes();
    }

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
    void aMessageWakesEveryPodOfTheWorkloadAtOnceAndTheLeaseLingers() {
        NODE.pod("orders-1", "shop", "orders", "asleep");
        NODE.pod("orders-2", "shop", "orders", "asleep");
        NODE.pod("invoices-1", "shop", "invoices", "asleep");
        String queueUrl = sqs.createQueue(request -> request.queueName("orders-incoming")).queueUrl();

        Instant sent = Instant.now();
        sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("order 1"));

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("orders-1").equals("awake")
                && NODE.state("orders-2").equals("awake"));
        assertThat(Duration.between(sent, Instant.now()), lessThan(Duration.ofSeconds(1)));
        assertThat(NODE.leases("orders-1"), contains("floci:orders"));
        assertThat(NODE.leases("orders-2"), contains("floci:orders"));
        assertThat(NODE.state("invoices-1"), is("asleep"));
        assertThat(NODE.leases("invoices-1"), is(empty()));
        JsonNode wake = NODE.wakes("orders-1").getFirst().body();
        assertThat(wake.path("reason").path("kind").asText(), is("aws"));
        assertThat(wake.path("reason").path("queue").asText(),
                is("arn:aws:sqs:us-east-1:000000000000:orders-incoming"));
        assertThat(wake.path("source").asText(), is("floci"));
        JsonNode lease = NODE.requests("PUT", WakeTestNode.leasePath("orders-1", "floci:orders")).getFirst().body();
        assertThat(lease.path("holder").asText(), is("floci wakes: orders"));

        Message message = sqs.receiveMessage(request -> request.queueUrl(queueUrl)).messages().getFirst();
        sqs.deleteMessage(request -> request.queueUrl(queueUrl).receiptHandle(message.receiptHandle()));
        Instant emptied = Instant.now();

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("orders-1").isEmpty()
                && NODE.leases("orders-2").isEmpty());
        assertThat(Duration.between(emptied, Instant.now()), greaterThanOrEqualTo(Duration.ofMillis(2500)));
        assertThat(NODE.wakes("orders-1"), hasSize(1));
    }

    @Test
    void aDelayedMessageWakesThePodsWhenItBecomesVisibleAndNotBefore() throws InterruptedException {
        NODE.pod("reports-1", "shop", "reports", "asleep");
        String queueUrl = sqs.createQueue(request -> request.queueName("reports-incoming")).queueUrl();

        Instant sent = Instant.now();
        sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("report").delaySeconds(3));
        Thread.sleep(Duration.between(Instant.now(), sent.plusMillis(2500)));

        assertThat(NODE.wakes("reports-1"), is(empty()));

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("reports-1").equals("awake"));
        Duration woken = Duration.between(sent, Instant.now());
        assertThat(woken, greaterThanOrEqualTo(Duration.ofMillis(2900)));
        assertThat(woken, lessThan(Duration.ofSeconds(4)));
        assertThat(NODE.leases("reports-1"), contains("floci:reports"));
    }

    @Test
    void aMessageInFlightKeepsTheLeasePastTheIdleTimeoutUntilItIsDeleted() throws InterruptedException {
        NODE.pod("audit-1", "shop", "audit", "asleep");
        String queueUrl = sqs.createQueue(request -> request.queueName("audit-incoming")
                .attributes(Map.of(QueueAttributeName.VISIBILITY_TIMEOUT, "30"))).queueUrl();
        sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("audit entry"));
        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("audit-1").equals("awake"));

        Message message = sqs.receiveMessage(request -> request.queueUrl(queueUrl)).messages().getFirst();
        Thread.sleep(Duration.ofSeconds(3));

        assertThat(NODE.leases("audit-1"), contains("floci:audit"));

        Instant deleted = Instant.now();
        sqs.deleteMessage(request -> request.queueUrl(queueUrl).receiptHandle(message.receiptHandle()));

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("audit-1").isEmpty());
        assertThat(Duration.between(deleted, Instant.now()), greaterThanOrEqualTo(Duration.ofMillis(900)));
        assertThat(Duration.between(deleted, Instant.now()), lessThan(Duration.ofSeconds(3)));
    }

    @Test
    void aMessageWhoseVisibilityTimeoutEndsKeepsTheLeaseUntilItIsHandled() throws InterruptedException {
        NODE.pod("retried-1", "shop", "retried", "asleep");
        String queueUrl = sqs.createQueue(request -> request.queueName("retried-incoming")).queueUrl();
        sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("job"));
        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("retried-1").equals("awake"));

        sqs.receiveMessage(request -> request.queueUrl(queueUrl).visibilityTimeout(1));
        Thread.sleep(Duration.ofSeconds(4));

        assertThat(NODE.leases("retried-1"), contains("floci:retried"));

        Message again = sqs.receiveMessage(request -> request.queueUrl(queueUrl)).messages().getFirst();
        sqs.deleteMessage(request -> request.queueUrl(queueUrl).receiptHandle(again.receiptHandle()));

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("retried-1").isEmpty());
        assertThat(NODE.wakes("retried-1"), hasSize(1));
    }

    @Test
    void aSteadyStreamOfQuicklyHandledMessagesKeepsOneLeaseWithNoRelease() throws InterruptedException {
        NODE.pod("stream-1", "shop", "stream", "asleep");
        String queueUrl = sqs.createQueue(request -> request.queueName("stream-incoming")).queueUrl();
        String lease = WakeTestNode.leasePath("stream-1", "floci:stream");

        Instant start = Instant.now();
        for (int i = 0; i < 7; i++) {
            String body = "message " + i;
            sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody(body));
            Message message = sqs.receiveMessage(request -> request.queueUrl(queueUrl).waitTimeSeconds(5))
                    .messages().getFirst();
            sqs.deleteMessage(request -> request.queueUrl(queueUrl).receiptHandle(message.receiptHandle()));
            Thread.sleep(Duration.between(Instant.now(), start.plusSeconds(i + 1)));
        }
        Instant lastDelete = Instant.now().minusSeconds(1);

        assertThat(NODE.requests("PUT", lease), hasSize(1));
        assertThat(NODE.requests("DELETE", lease), is(empty()));
        assertThat(NODE.wakes("stream-1"), hasSize(1));
        assertThat(NODE.leases("stream-1"), contains("floci:stream"));
        assertThat(watch.reads("stream"), lessThan(3L));

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("stream-1").isEmpty());
        assertThat(Duration.between(lastDelete, Instant.now()), greaterThanOrEqualTo(Duration.ofMillis(1900)));
        assertThat(NODE.requests("DELETE", lease), hasSize(1));
    }

    @Test
    void aPurgeIsTheLastEventOfTheLinger() {
        NODE.pod("purged-1", "shop", "purged", "asleep");
        String queueUrl = sqs.createQueue(request -> request.queueName("purged-incoming")).queueUrl();
        sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("stale"));
        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("purged-1").equals("awake"));

        Instant purged = Instant.now();
        sqs.purgeQueue(request -> request.queueUrl(queueUrl));

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("purged-1").isEmpty());
        assertThat(Duration.between(purged, Instant.now()), greaterThanOrEqualTo(Duration.ofMillis(1900)));
    }

    @Test
    void anSnsMessageDeliveredToTheQueueWakesThePods() {
        NODE.pod("alerts-1", "shop", "alerts", "asleep");
        String queueUrl = sqs.createQueue(request -> request.queueName("alerts-incoming")).queueUrl();
        String queueArn = arn(queueUrl);
        try (SnsClient sns = SnsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .credentialsProvider(WakeTestNode.credentials()).build()) {
            String topicArn = sns.createTopic(request -> request.name("alerts")).topicArn();
            sns.subscribe(request -> request.topicArn(topicArn).protocol("sqs").endpoint(queueArn));

            sns.publish(request -> request.topicArn(topicArn).message("disk full"));
        }

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("alerts-1").equals("awake"));
        assertThat(NODE.wakes("alerts-1").getFirst().body().path("reason").path("queue").asText(), is(queueArn));
    }

    @Test
    void anEventBridgeRuleThatSendsToTheQueueWakesThePods() {
        NODE.pod("events-1", "shop", "events", "asleep");
        String queueUrl = sqs.createQueue(request -> request.queueName("events-incoming")).queueUrl();
        String queueArn = arn(queueUrl);
        given().contentType(EVENTS_JSON).header("X-Amz-Target", "AWSEvents.PutRule")
                .body("{\"Name\":\"orders-to-sqs\",\"EventPattern\":\"{\\\"source\\\":[\\\"shop.orders\\\"]}\"}")
                .when().post("/").then().statusCode(200);
        given().contentType(EVENTS_JSON).header("X-Amz-Target", "AWSEvents.PutTargets")
                .body("{\"Rule\":\"orders-to-sqs\",\"Targets\":[{\"Id\":\"queue\",\"Arn\":\"" + queueArn + "\"}]}")
                .when().post("/").then().statusCode(200);

        given().contentType(EVENTS_JSON).header("X-Amz-Target", "AWSEvents.PutEvents")
                .body("{\"Entries\":[{\"Source\":\"shop.orders\",\"DetailType\":\"OrderPlaced\","
                        + "\"Detail\":\"{\\\"id\\\":1}\"}]}")
                .when().post("/").then().statusCode(200);

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("events-1").equals("awake"));
        assertThat(NODE.wakes("events-1").getFirst().body().path("reason").path("queue").asText(), is(queueArn));
    }

    @Test
    void anIotRuleThatSendsToTheQueueWakesThePods() {
        NODE.pod("telemetry-1", "shop", "telemetry", "asleep");
        String queueUrl = sqs.createQueue(request -> request.queueName("telemetry-incoming")).queueUrl();
        try (IotClient iot = IotClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .credentialsProvider(WakeTestNode.credentials()).build();
             IotDataPlaneClient data = IotDataPlaneClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                     .credentialsProvider(WakeTestNode.credentials()).build()) {
            iot.createTopicRule(request -> request.ruleName("telemetryToSqs").topicRulePayload(rule -> rule
                    .sql("SELECT * FROM 'devices/telemetry'")
                    .actions(action -> action.sqs(target -> target.queueUrl(queueUrl)
                            .roleArn("arn:aws:iam::000000000000:role/iot-rules")))));

            data.publish(request -> request.topic("devices/telemetry").payload(SdkBytes.fromUtf8String("{\"t\":21}")));
        }

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("telemetry-1").equals("awake"));
        assertThat(NODE.leases("telemetry-1"), contains("floci:telemetry"));
    }

    @Test
    void aMessageMovedToTheDeadLetterQueueWakesItsDrainer() {
        NODE.pod("drainer-1", "shop", "drainer", "asleep");
        String dlqUrl = sqs.createQueue(request -> request.queueName("drain-dlq")).queueUrl();
        String dlqArn = arn(dlqUrl);
        String queueUrl = sqs.createQueue(request -> request.queueName("drain-incoming").attributes(Map.of(
                QueueAttributeName.REDRIVE_POLICY, "{\"deadLetterTargetArn\":\"" + dlqArn + "\",\"maxReceiveCount\":\"1\"}",
                QueueAttributeName.VISIBILITY_TIMEOUT, "0"))).queueUrl();
        sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("poison"));
        sqs.receiveMessage(request -> request.queueUrl(queueUrl));

        assertThat(NODE.wakes("drainer-1"), is(empty()));

        sqs.receiveMessage(request -> request.queueUrl(queueUrl));

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("drainer-1").equals("awake"));
        assertThat(NODE.wakes("drainer-1").getFirst().body().path("reason").path("queue").asText(), is(dlqArn));
    }

    @Test
    void anIdleWorkloadCostsNoReadAndNoNodeCall() throws InterruptedException {
        NODE.pod("quiet-1", "quiet", "quiet", "asleep");
        String queueUrl = sqs.createQueue(request -> request.queueName("quiet-incoming")).queueUrl();
        sqs.getQueueAttributes(request -> request.queueUrl(queueUrl).attributeNames(QueueAttributeName.ALL));
        Thread.sleep(Duration.ofMillis(500));
        long reads = watch.reads("quiet");

        Thread.sleep(Duration.ofSeconds(5));

        assertThat(watch.reads("quiet"), is(reads));
        assertThat(NODE.fake().requests().stream()
                .filter(request -> (request.query() != null && request.query().contains("namespace=quiet"))
                        || request.path().startsWith("/v1/pods/quiet-1"))
                .toList(), is(empty()));
    }

    @Test
    void aWorkloadReadsItsQueuesInItsOwnRegion() throws InterruptedException {
        NODE.pod("billing-1", "shop", "billing", "asleep");
        String here = sqs.createQueue(request -> request.queueName("billing-incoming")).queueUrl();
        sqs.sendMessage(request -> request.queueUrl(here).messageBody("not for billing"));
        Thread.sleep(Duration.ofSeconds(1));

        assertThat(NODE.wakes("billing-1"), is(empty()));

        try (SqsClient ireland = WakeTestNode.sqs(endpoint, Region.EU_WEST_1)) {
            String queueUrl = ireland.createQueue(request -> request.queueName("billing-incoming")).queueUrl();
            ireland.sendMessage(request -> request.queueUrl(queueUrl).messageBody("invoice"));
        }

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("billing-1").equals("awake"));
        List<FakeNodeApi.Request> wakes = NODE.wakes("billing-1");
        assertThat(wakes.getFirst().body().path("reason").path("queue").asText(),
                is("arn:aws:sqs:eu-west-1:000000000000:billing-incoming"));
    }

    private String arn(String queueUrl) {
        return sqs.getQueueAttributes(request -> request.queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.QUEUE_ARN)).attributes().get(QueueAttributeName.QUEUE_ARN);
    }

    public static class Profile implements QuarkusTestProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            return NODE.config(WORKLOADS);
        }
    }
}
