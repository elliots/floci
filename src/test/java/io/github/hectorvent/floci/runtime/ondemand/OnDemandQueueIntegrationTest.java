package io.github.hectorvent.floci.runtime.ondemand;

import io.github.hectorvent.floci.runtime.ondemand.ActivationCoordinator.State;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(OnDemandQueueTestProfile.class)
@QuarkusTestResource(value = OnDemandQueueTestResource.class, restrictToAnnotatedClass = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OnDemandQueueIntegrationTest {
    @TestHTTPResource
    URI endpoint;

    @Inject
    OnDemandService service;

    @Inject
    QueueRuntimeTestFactory factory;

    @Test
    @Order(1)
    void sdkMessagesWakeConsumersAndInFlightMessagesHoldThemAwake() {
        try (SqsClient client = client("000000000000", "us-east-1")) {
            String url = create(client);
            try {
                awaitSleeping("orders");
                int starts = factory.runtime("orders").starts.get();
                String id = client.sendMessage(request -> request.queueUrl(url).messageBody("customer-event")).messageId();
                assertFalse(id.isBlank());
                awaitReady("orders");
                List<Message> messages = client.receiveMessage(request -> request.queueUrl(url).visibilityTimeout(10)).messages();
                assertEquals(1, messages.size());
                assertEquals("customer-event", messages.getFirst().body());
                await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(4))
                        .until(() -> service.coordinator().state("orders") == State.READY);
                client.deleteMessage(request -> request.queueUrl(url).receiptHandle(messages.getFirst().receiptHandle()));
                awaitSleeping("orders");
                assertEquals(starts + 1, factory.runtime("orders").starts.get());
            } finally {
                client.deleteQueue(request -> request.queueUrl(url));
            }
        }
    }

    @Test
    @Order(2)
    void delayedMessagesActivateOnlyWhenVisibleAndDoNotGetConsumedByTheScaler() {
        try (SqsClient client = client("000000000000", "us-east-1")) {
            String url = create(client);
            try {
                awaitSleeping("orders");
                int starts = factory.runtime("orders").starts.get();
                client.sendMessage(request -> request.queueUrl(url).messageBody("delayed-event").delaySeconds(2));
                await().during(Duration.ofMillis(700)).atMost(Duration.ofSeconds(1))
                        .until(() -> factory.runtime("orders").starts.get() == starts);
                awaitReady("orders");
                assertEquals("1", client.getQueueAttributes(request -> request.queueUrl(url)
                        .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES))
                        .attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));
            } finally {
                client.deleteQueue(request -> request.queueUrl(url));
                awaitSleeping("orders");
            }
        }
    }

    @Test
    @Order(3)
    void queryMessagesUseTheSameActivationPathAndAccountRegionIsolation() {
        try (SqsClient client = client("111111111111", "us-east-1")) {
            String url = create(client);
            try {
                awaitSleeping("orders");
                awaitSleeping("china");
                given().contentType("application/x-www-form-urlencoded")
                        .header("Authorization", "AWS4-HMAC-SHA256 Credential=111111111111/20261006/us-east-1/sqs/aws4_request")
                        .formParam("Action", "SendMessage").formParam("QueueUrl", url).formParam("MessageBody", "query-event")
                        .when().post("/").then().statusCode(200).body(containsString("<MessageId>"));
                awaitReady("other-account");
                assertEquals(State.SLEEPING, service.coordinator().state("orders"));
                assertEquals(State.SLEEPING, service.coordinator().state("china"));
            } finally {
                client.deleteQueue(request -> request.queueUrl(url));
                awaitSleeping("other-account");
            }
        }
    }

    @Test
    @Order(4)
    void activationFailureDoesNotChangeSuccessfulSdkSendOrLoseTheMessage() {
        try (SqsClient client = client("000000000000", "cn-north-1")) {
            String url = create(client);
            try {
                factory.runtime("china").failStart = true;
                String id = client.sendMessage(request -> request.queueUrl(url).messageBody("keep-me")).messageId();
                assertFalse(id.isBlank());
                await().atMost(Duration.ofSeconds(5)).until(() -> factory.runtime("china").starts.get() > 0);
                List<Message> messages = client.receiveMessage(request -> request.queueUrl(url)).messages();
                assertEquals("keep-me", messages.getFirst().body());
                client.deleteMessage(request -> request.queueUrl(url).receiptHandle(messages.getFirst().receiptHandle()));
            } finally {
                factory.runtime("china").failStart = false;
                client.deleteQueue(request -> request.queueUrl(url));
            }
        }
    }

    @Test
    @Order(5)
    void emulatorResetRebuildsTheCoordinatorAndNewQueueMessagesCanActivateAgain() {
        try (SqsClient client = client("000000000000", "us-east-1")) {
            String url = create(client);
            client.sendMessage(request -> request.queueUrl(url).messageBody("before-reset"));
            awaitReady("orders");
            ActivationCoordinator previous = service.coordinator();
            given().when().post("/_floci/state/reset").then().statusCode(200);
            assertNotSame(previous, service.coordinator());
            assertThrows(IllegalStateException.class, () -> previous.acquire("orders"));
            assertThrows(QueueDoesNotExistException.class, () -> client.getQueueUrl(request
                    -> request.queueName("on-demand-incoming")));
            String recreated = create(client);
            try {
                client.sendMessage(request -> request.queueUrl(recreated).messageBody("after-reset"));
                awaitReady("orders");
                assertEquals("after-reset", client.receiveMessage(request -> request.queueUrl(recreated))
                        .messages().getFirst().body());
            } finally {
                client.deleteQueue(request -> request.queueUrl(recreated));
            }
        }
    }

    private SqsClient client(String account, String region) {
        return SqsClient.builder().endpointOverride(endpoint).region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(account, "test"))).build();
    }

    private String create(SqsClient client) {
        String returned = client.createQueue(request -> request.queueName("on-demand-incoming")
                .attributes(Map.of(QueueAttributeName.VISIBILITY_TIMEOUT, "10"))).queueUrl();
        return endpoint.resolve(URI.create(returned).getPath()).toString();
    }

    private void awaitReady(String id) {
        await().atMost(Duration.ofSeconds(5)).until(() -> service.coordinator().state(id) == State.READY);
    }

    private void awaitSleeping(String id) {
        await().atMost(Duration.ofSeconds(5)).until(() -> service.coordinator().state(id) == State.SLEEPING);
    }
}
