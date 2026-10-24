package io.github.hectorvent.floci.runtime.wakes;

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

import java.net.URI;
import java.time.Duration;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/** Wakes that a node refuses or fails, and what Floci does until the pods can be woken. */
@QuarkusTest
@TestProfile(MessageWakeRefusalIntegrationTest.Profile.class)
class MessageWakeRefusalIntegrationTest {

    private static final WakeTestNode NODE = WakeTestNode.start("message-wake-refusal-it");
    private static final String WORKLOADS = """
            crowded:
              queues: [crowded-incoming]
              runtime: {type: kubernetes, node: dev, namespace: crowded, deployment: crowded}
            fresh:
              queues: [fresh-incoming]
              runtime: {type: kubernetes, node: dev, namespace: ops, deployment: fresh}
            broken:
              queues: [broken-incoming]
              runtime: {type: kubernetes, node: dev, namespace: ops, deployment: broken}
            moved:
              queues: [moved-incoming]
              runtime: {type: kubernetes, node: dev, namespace: ops, deployment: moved}
            locked:
              queues: [locked-incoming]
              runtime: {type: kubernetes, node: dev, namespace: ops, deployment: locked}
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
    void aWakeRefusedForWantOfRoomWaitsUntilAPodSleeps() throws InterruptedException {
        NODE.pod("crowded-busy", "crowded", "web", "awake");
        NODE.pod("crowded-1", "crowded", "crowded", "asleep");
        NODE.fake().failNext("POST", "/v1/pods/crowded-1/wake", 409, "refused", "wake refused: awake pods 1/1",
                "wake refused");

        send("crowded-incoming");
        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.wakes("crowded-1").size() == 1);
        Thread.sleep(Duration.ofSeconds(3));

        assertThat(NODE.wakes("crowded-1"), hasSize(1));
        assertThat(NODE.leases("crowded-1"), contains("floci:crowded"));
        assertThat(NODE.state("crowded-1"), is("asleep"));

        NODE.fake().state("crowded-busy", "asleep");

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("crowded-1").equals("awake"));
        assertThat(NODE.wakes("crowded-1"), hasSize(2));
    }

    @Test
    void aCreatingPodIsLeasedAndWokenOnceItRuns() throws InterruptedException {
        NODE.pod("fresh-1", "ops", "fresh", "creating");

        send("fresh-incoming");
        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases("fresh-1").contains("floci:fresh"));
        Thread.sleep(Duration.ofSeconds(1));

        assertThat(NODE.wakes("fresh-1"), is(empty()));

        NODE.fake().state("fresh-1", "asleep");

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("fresh-1").equals("awake"));
    }

    @Test
    void aFailedPodIsLeftAlone() {
        NODE.pod("broken-1", "ops", "broken", "failed");
        NODE.pod("broken-2", "ops", "broken", "asleep");

        send("broken-incoming");

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("broken-2").equals("awake"));
        assertThat(NODE.wakes("broken-1"), is(empty()));
        assertThat(NODE.leases("broken-1"), is(empty()));
    }

    @Test
    void aPodThatIsGoneIsListedAgainAndItsReplacementWoken() {
        NODE.pod("moved-1", "ops", "moved", "asleep");
        NODE.fake().failNext("POST", "/v1/pods/moved-1/wake", 404, "not_found", "no pod moved-1");

        send("moved-incoming");
        await().atMost(Duration.ofSeconds(10)).until(() -> !NODE.wakes("moved-1").isEmpty());
        NODE.fake().removePod("moved-1");
        NODE.pod("moved-2", "ops", "moved", "asleep");

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("moved-2").equals("awake"));
        assertThat(NODE.leases("moved-2"), contains("floci:moved"));
    }

    @Test
    void aNodeThatRefusesTheTokenIsCalledAgainWithTheNewTokenFile() {
        String token = "rotated-token-0123456789";
        NODE.pod("locked-1", "ops", "locked", "asleep");
        NODE.fake().token(token);
        try {
            send("locked-incoming");
            await().atMost(Duration.ofSeconds(10)).until(() -> NODE.fake().requests().stream()
                    .anyMatch(request -> request.query() != null && request.query().contains("namespace=ops")
                            && !("Bearer " + token).equals(request.authorization())));

            NODE.token(token);

            await().atMost(Duration.ofSeconds(40)).until(() -> NODE.state("locked-1").equals("awake"));
            assertThat(NODE.wakes("locked-1").getLast().authorization(), is("Bearer " + token));
        } finally {
            NODE.token(WakeTestNode.TOKEN);
            NODE.fake().token(WakeTestNode.TOKEN);
        }
    }

    private void send(String queue) {
        String queueUrl = sqs.createQueue(request -> request.queueName(queue)).queueUrl();
        sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("message for " + queue));
    }

    public static class Profile implements QuarkusTestProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            return NODE.config(WORKLOADS);
        }
    }
}
