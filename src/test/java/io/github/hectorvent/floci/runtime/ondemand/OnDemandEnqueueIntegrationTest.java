package io.github.hectorvent.floci.runtime.ondemand;

import io.github.hectorvent.floci.runtime.ondemand.ActivationCoordinator.State;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import java.net.URI;
import java.time.Duration;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@QuarkusTest
@TestProfile(OnDemandQueueTestProfile.class)
@QuarkusTestResource(value = OnDemandEnqueueIntegrationTest.SlowPollingResource.class, restrictToAnnotatedClass = true)
class OnDemandEnqueueIntegrationTest {
    @TestHTTPResource
    URI endpoint;

    @Inject
    OnDemandService service;

    @Test
    void sdkBatchAndQueryWakeTheirOwnConsumersWithoutWaitingForTheNextPoll() {
        await().atMost(Duration.ofSeconds(5)).until(() -> service.coordinator().state("orders") == State.SLEEPING
                && service.coordinator().state("other-account") == State.SLEEPING
                && service.coordinator().state("china") == State.SLEEPING);
        try (SqsClient first = client("000000000000"); SqsClient second = client("111111111111")) {
            String firstQueue = first.createQueue(request -> request.queueName("on-demand-incoming")).queueUrl();
            String secondQueue = second.createQueue(request -> request.queueName("on-demand-incoming")).queueUrl();
            try {
                assertFalse(first.sendMessageBatch(request -> request.queueUrl(firstQueue).entries(
                        SendMessageBatchRequestEntry.builder().id("batch-event").messageBody("wake-now").build()))
                        .successful().isEmpty());
                await().atMost(Duration.ofSeconds(5)).until(() -> service.coordinator().state("orders") == State.READY);
                assertEquals(State.SLEEPING, service.coordinator().state("other-account"));
                assertEquals(State.SLEEPING, service.coordinator().state("china"));

                given().contentType("application/x-www-form-urlencoded")
                        .header("Authorization", "AWS4-HMAC-SHA256 Credential=111111111111/20261016/us-east-1/sqs/aws4_request")
                        .formParam("Action", "SendMessage").formParam("QueueUrl", secondQueue)
                        .formParam("MessageBody", "query-wake").post(endpoint).then().statusCode(200);
                await().atMost(Duration.ofSeconds(5))
                        .until(() -> service.coordinator().state("other-account") == State.READY);
                assertEquals(State.SLEEPING, service.coordinator().state("china"));
                assertEquals("wake-now", first.receiveMessage(request -> request.queueUrl(firstQueue))
                        .messages().getFirst().body());
                assertEquals("query-wake", second.receiveMessage(request -> request.queueUrl(secondQueue))
                        .messages().getFirst().body());
            } finally {
                first.deleteQueue(request -> request.queueUrl(firstQueue));
                second.deleteQueue(request -> request.queueUrl(secondQueue));
            }
        }
    }

    private SqsClient client(String accountId) {
        return SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accountId, "test")))
                .build();
    }

    public static class SlowPollingResource extends OnDemandQueueTestResource {
        @Override
        protected String reconcileIntervalMillis() {
            return "600000";
        }
    }
}
