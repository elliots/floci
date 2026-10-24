package io.github.hectorvent.floci.services.eks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.runtime.flocinode.FakeFlociNodes;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode;
import io.github.hectorvent.floci.runtime.node.FakeNodeApi;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeDirectory;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eks.EksClient;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A runtime file workload that names an EKS cluster with {@code runtime.cluster-name}: an SQS
 * message wakes its pods on the smolvm-pods node that the cluster runs on, found in
 * {@link SmolvmNodeDirectory} as {@code cluster:ACCOUNT/REGION/NAME} while the cluster runs. The
 * node runs on the fakes of {@link SmolvmPodsProfile}.
 */
@QuarkusTest
@TestProfile(EksSmolvmMessageWakeIntegrationTest.WorkloadsProfile.class)
class EksSmolvmMessageWakeIntegrationTest {

    private static final String ACCOUNT = "000000000000";
    private static final String OTHER_ACCOUNT = "111111111111";
    private static final String REGION = "us-east-1";
    private static final String CLUSTER = "wake-cluster";
    private static final String QUEUE = "orders-incoming";
    private static final String LEASE = "floci:orders";
    private static final String WORKLOADS = """
            version: 1
            workloads:
              orders:
                queues: [orders-incoming]
                runtime: {type: kubernetes, cluster-name: wake-cluster, namespace: shop, deployment: orders}
              ireland:
                region: eu-west-1
                queues: [orders-incoming]
                runtime: {type: kubernetes, cluster-name: wake-cluster, namespace: shop, deployment: ireland}
              partner:
                account-id: '111111111111'
                queues: [orders-incoming]
                runtime: {type: kubernetes, cluster-name: wake-cluster, namespace: shop, deployment: partner}
            """;

    /** {@link SmolvmPodsProfile} with a runtime file whose workloads name {@link #CLUSTER}. */
    public static class WorkloadsProfile extends SmolvmPodsProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> overrides = overrides();
            overrides.put("floci.runtime.config-file", runtimeFile().toString());
            return overrides;
        }

        private static Path runtimeFile() {
            try {
                Path file = Files.createTempDirectory("floci-eks-wakes-").resolve("runtime.yaml");
                Files.writeString(file, WORKLOADS);
                return file;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @TestHTTPResource
    URI endpoint;

    @Inject
    FakeFlociNodes driver;

    @Inject
    SmolvmNodeDirectory directory;

    @Test
    void aMessageWakesTheWorkloadsPodsOnTheNodeOfItsClusterWhileTheClusterRuns() {
        given().baseUri(endpoint.toString()).when().post("/_floci/state/reset").then().statusCode(200);
        try (EksClient eks = eks();
             SqsClient sqs = sqs(Region.US_EAST_1, ACCOUNT);
             SqsClient ireland = sqs(Region.EU_WEST_1, ACCOUNT);
             SqsClient partner = sqs(Region.US_EAST_1, OTHER_ACCOUNT)) {
            FakeNodeApi node = createCluster(eks);
            for (SqsClient client : List.of(ireland, partner)) {
                String queueUrl = client.createQueue(request -> request.queueName(QUEUE)).queueUrl();
                client.sendMessage(request -> request.queueUrl(queueUrl).messageBody("not for orders"));
            }
            String queueUrl = sqs.createQueue(request -> request.queueName(QUEUE)).queueUrl();

            sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("order 1"));

            await().atMost(10, TimeUnit.SECONDS).until(() -> state(node, "orders-1").equals("awake"));
            assertThat(leases(node, "orders-1"), contains(LEASE));
            JsonNode wake = wakes(node, "orders-1").getFirst().body();
            assertEquals("arn:aws:sqs:us-east-1:000000000000:orders-incoming",
                    wake.path("reason").path("queue").asText());
            assertEquals("Bearer " + driver.serviceToken(), wakes(node, "orders-1").getFirst().authorization());

            assertTrue(directory.find(SmolvmNode.clusterName(ACCOUNT, "eu-west-1", CLUSTER)).isEmpty());
            assertTrue(directory.find(SmolvmNode.clusterName(OTHER_ACCOUNT, REGION, CLUSTER)).isEmpty());
            await().during(2, TimeUnit.SECONDS).atMost(3, TimeUnit.SECONDS).until(() ->
                    wakes(node, "ireland-1").isEmpty() && wakes(node, "partner-1").isEmpty());
            for (String sandbox : List.of("ireland-1", "partner-1")) {
                assertThat(state(node, sandbox), is("asleep"));
                assertThat(leases(node, sandbox), is(empty()));
            }

            eks.deleteCluster(request -> request.name(CLUSTER));

            assertTrue(directory.find(SmolvmNode.clusterName(ACCOUNT, REGION, CLUSTER)).isEmpty());
            assertTrue(directory.find(FlociNode.NAME).isPresent(), "the node stays");
            node.state("orders-1", "asleep");
            int woken = wakes(node, "orders-1").size();
            sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("order 2"));
            await().during(2, TimeUnit.SECONDS).atMost(3, TimeUnit.SECONDS).until(() ->
                    wakes(node, "orders-1").size() == woken);

            FakeNodeApi adopted = createCluster(eks);
            assertSame(node, adopted, "a cluster created again adopts the node");
            await().atMost(10, TimeUnit.SECONDS).until(() -> state(adopted, "orders-1").equals("awake"));
            assertThat(leases(adopted, "orders-1"), contains(LEASE));
            eks.deleteCluster(request -> request.name(CLUSTER));
        }
    }

    @Test
    void anEmulatorResetDeletesTheNodeSoNoMessageWakesItsPods() {
        given().baseUri(endpoint.toString()).when().post("/_floci/state/reset").then().statusCode(200);
        try (EksClient eks = eks(); SqsClient sqs = sqs(Region.US_EAST_1, ACCOUNT)) {
            FakeNodeApi node = createCluster(eks);
            String nodeId = driver.calls("create").getLast().nodeId();

            given().baseUri(endpoint.toString()).when().post("/_floci/state/reset").then().statusCode(200);

            assertTrue(directory.find(SmolvmNode.clusterName(ACCOUNT, REGION, CLUSTER)).isEmpty());
            assertTrue(directory.find(FlociNode.NAME).isEmpty());
            assertTrue(driver.machine(nodeId).isEmpty(), "the reset deleted the node's machine");
            assertNull(driver.nodeApi(), "the node's service is gone");
            String queueUrl = sqs.createQueue(request -> request.queueName(QUEUE)).queueUrl();
            sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("order after the reset"));
            await().during(1, TimeUnit.SECONDS).atMost(2, TimeUnit.SECONDS).until(() ->
                    wakes(node, "orders-1").isEmpty());

            FakeNodeApi recreated = createCluster(eks);
            assertNotSame(node, recreated);
            await().atMost(10, TimeUnit.SECONDS).until(() -> state(recreated, "orders-1").equals("awake"));
            eks.deleteCluster(request -> request.name(CLUSTER));
        }
    }

    /**
     * Creates {@link #CLUSTER}, waits until it is ACTIVE and gives its node an asleep pod of each
     * workload, unless the node has them.
     */
    private FakeNodeApi createCluster(EksClient eks) {
        eks.createCluster(request -> request.name(CLUSTER).roleArn("arn:aws:iam::" + ACCOUNT + ":role/eks-cluster"));
        await().atMost(30, TimeUnit.SECONDS).pollInterval(Duration.ofMillis(250)).until(() ->
                eks.describeCluster(request -> request.name(CLUSTER)).cluster().statusAsString().equals("ACTIVE"));
        assertTrue(directory.find(SmolvmNode.clusterName(ACCOUNT, REGION, CLUSTER)).isPresent());
        FakeNodeApi node = driver.nodeApi();
        for (String deployment : List.of("orders", "ireland", "partner")) {
            String sandbox = deployment + "-1";
            if (node.hasPod(sandbox)) {
                continue;
            }
            ObjectNode pod = node.addPod(sandbox, "shop", sandbox);
            synchronized (node) {
                pod.putObject("controller").put("kind", "Deployment").put("name", deployment);
                pod.putObject("labels").put("app", deployment);
            }
            node.state(sandbox, "asleep");
        }
        return node;
    }

    private static String state(FakeNodeApi node, String sandbox) {
        return node.pod(sandbox).path("state").asText();
    }

    /** The ids of the API leases on the pod. */
    private static List<String> leases(FakeNodeApi node, String sandbox) {
        List<String> ids = new ArrayList<>();
        for (JsonNode lease : node.pod(sandbox).path("leases")) {
            if (lease.path("kind").asText().equals("api")) {
                ids.add(lease.path("id").asText());
            }
        }
        return ids;
    }

    private static List<FakeNodeApi.Request> wakes(FakeNodeApi node, String sandbox) {
        return node.requests().stream()
                .filter(request -> request.method().equals("POST")
                        && request.path().equals("/v1/pods/" + sandbox + "/wake"))
                .toList();
    }

    private static List<FakeNodeApi.Request> leaseCalls(FakeNodeApi node, String sandbox) {
        String path = "/v1/pods/" + sandbox + "/leases/" + LEASE.replace(":", "%3A");
        return node.requests().stream().filter(request -> request.path().equals(path)).toList();
    }

    private EksClient eks() {
        return EksClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder()).build();
    }

    /** An SQS client in {@code region}, of {@code account} through its access key. */
    private SqsClient sqs(Region region, String account) {
        String accessKey = account.equals(ACCOUNT) ? "test" : account;
        return SqsClient.builder().endpointOverride(endpoint).region(region)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder()).build();
    }
}
