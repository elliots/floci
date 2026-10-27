package io.github.hectorvent.floci.runtime.wakes;

import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

/**
 * The node's pod events while a workload is busy: a pod created then is leased and woken from
 * its events, and the leases are renewed with no listing of the namespace.
 */
@QuarkusTest
@TestProfile(MessageWakeNodeEventsIntegrationTest.Profile.class)
class MessageWakeNodeEventsIntegrationTest {

    private static final WakeTestNode NODE = WakeTestNode.start("message-wake-node-events-it");
    private static final String ORDERS = """
            orders:
              queues: [orders-incoming]
              runtime: {type: kubernetes, node: dev, namespace: shop, deployment: orders}
            """;

    @TestHTTPResource
    URI endpoint;

    @AfterAll
    static void stopNode() {
        NODE.close();
    }

    @Test
    void aPodCreatedWhileTheWorkloadIsBusyIsLeasedFromItsEventsAndRenewedWithNoListing() {
        NODE.pod("orders-1", "shop", "orders", "asleep");
        try (SqsClient sqs = WakeTestNode.sqs(endpoint, Region.US_EAST_1)) {
            String queueUrl = sqs.createQueue(request -> request.queueName("orders-incoming")).queueUrl();
            sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("waits"));
        }
        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("orders-1").equals("awake"));
        int listings = NODE.requests("GET", "/v1/pods").size();

        NODE.pod("orders-2", "shop", "orders", "asleep");

        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.state("orders-2").equals("awake"));
        assertThat(NODE.leases("orders-2"), is(List.of("floci:orders")));
        int afterEvent = NODE.requests("GET", "/v1/pods").size();
        assertThat(afterEvent, greaterThan(listings));

        await().atMost(Duration.ofSeconds(40)).until(() -> renewals("orders-1") > 0 && renewals("orders-2") > 0);
        assertThat(NODE.requests("GET", "/v1/pods").size(), is(afterEvent));
        assertThat(NODE.leases("orders-1"), is(List.of("floci:orders")));
    }

    private static int renewals(String sandbox) {
        return NODE.requests("PATCH", WakeTestNode.leasePath(sandbox, "floci:orders")).size();
    }

    public static class Profile implements QuarkusTestProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            return NODE.config(ORDERS);
        }
    }
}
