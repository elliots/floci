package io.github.hectorvent.floci.services.eks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.Base64Url;
import io.github.hectorvent.floci.core.common.RequestScopes;
import io.github.hectorvent.floci.core.common.docker.UserDataPipeline;
import io.github.hectorvent.floci.runtime.flocinode.FakeFlociNodes;
import io.github.hectorvent.floci.runtime.flocinode.FakeKubeApi;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode;
import io.github.hectorvent.floci.runtime.flocinode.NodeReadyRule;
import io.github.hectorvent.floci.runtime.flocinode.NodeRecord;
import io.github.hectorvent.floci.runtime.flocinode.NodeSeed;
import io.github.hectorvent.floci.runtime.node.FakeNodeApi;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeDirectory;
import io.github.hectorvent.floci.services.ec2.model.Instance;
import io.github.hectorvent.floci.services.eks.model.Cluster;
import io.github.hectorvent.floci.services.iam.IamService;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.response.ValidatableResponse;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;
import software.amazon.awssdk.services.eks.EksClient;
import software.amazon.awssdk.services.eks.model.ClusterStatus;
import software.amazon.awssdk.services.eks.model.CreateClusterResponse;
import software.amazon.awssdk.services.eks.model.InvalidParameterException;
import software.amazon.awssdk.services.eks.model.LogType;
import software.amazon.awssdk.services.eks.model.ResourceLimitExceededException;
import software.amazon.awssdk.services.eks.model.ResourceNotFoundException;
import software.amazon.awssdk.services.eks.model.TaintEffect;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * EKS on the {@code smolvm-pods} provider through the AWS SDK, with {@code node-vm.sh}, the node's
 * Kubernetes API and its node service replaced by fakes ({@link SmolvmPodsProfile}). Each test
 * starts from an emulator reset, which deletes the node, so each creates its own.
 */
@QuarkusTest
@TestProfile(SmolvmPodsProfile.class)
class EksSmolvmIntegrationTest {

    private static final String ACCOUNT = "000000000000";
    private static final String REGION = "us-east-1";
    private static final String ROLE = "arn:aws:iam::" + ACCOUNT + ":role/eks-cluster";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String POD_IDENTITY_TRUST = """
            {"Version":"2012-10-17","Statement":[{"Effect":"Allow",
              "Principal":{"Service":"pods.eks.amazonaws.com"},"Action":["sts:AssumeRole","sts:TagSession"]}]}
            """;

    @TestHTTPResource
    URI endpoint;

    @Inject
    FakeFlociNodes driver;

    @Inject
    SmolvmNodeDirectory directory;

    @Inject
    EksClusterManager clusterManager;

    @Inject
    FlociNode flociNode;

    @Inject
    EksService eksService;

    @Inject
    NodeSeed seeds;

    @Inject
    IamService iamService;

    private EksClient eks;

    @BeforeEach
    void freshNode() {
        given().when().post("/_floci/state/reset").then().statusCode(200);
        driver.reset();
        eks = client(Region.US_EAST_1);
    }

    @AfterEach
    void closeClient() {
        eks.close();
    }

    private EksClient client(Region region) {
        return EksClient.builder().endpointOverride(endpoint).region(region)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder()).build();
    }

    private static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private CreateClusterResponse create(String name) {
        return eks.createCluster(request -> request.name(name).roleArn(ROLE));
    }

    private ClusterStatus status(String name) {
        return eks.describeCluster(request -> request.name(name)).cluster().status();
    }

    private void awaitStatus(String name, ClusterStatus status) {
        await().atMost(30, TimeUnit.SECONDS).pollInterval(Duration.ofMillis(250)).until(() -> status(name) == status);
    }

    /** Floci's record of cluster {@code name}. */
    private Cluster record(String name) {
        return eksService.findAuthenticationCluster(ACCOUNT, name).orElseThrow();
    }

    private String nodeName() {
        return driver.node().nodeName();
    }

    /** Waits until the ready rule ran {@code count} once more, which tells that it made a pass. */
    private static void awaitPass(Supplier<Integer> count) {
        int before = count.get();
        await().atMost(15, TimeUnit.SECONDS).pollInterval(Duration.ofMillis(100)).until(() -> count.get() > before);
    }

    private static int podListings(FakeNodeApi nodeApi) {
        return (int) nodeApi.requests().stream().filter(request -> request.path().equals("/v1/pods")).count();
    }

    @Test
    void createClusterAnswersCreatingAtOnceAndListsTheNodeWhileItIsCreated() {
        String name = unique("create");
        CountDownLatch nodeCreate = driver.holdCreates();

        long started = System.nanoTime();
        CreateClusterResponse created = create(name);
        Duration took = Duration.ofNanos(System.nanoTime() - started);

        assertTrue(took.compareTo(Duration.ofSeconds(2)) < 0, "CreateCluster took " + took);
        assertEquals(ClusterStatus.CREATING, created.cluster().status());
        assertEquals("1.35", created.cluster().version());
        assertThat(created.cluster().endpoint(), containsString("https://localhost:465"));
        SmolvmNode node = directory.find(FlociNode.NAME).orElseThrow();
        assertEquals(SmolvmNode.Kind.CREATED, node.kind());
        NodeRecord record = flociNode.record().orElseThrow();
        assertEquals(record.nodeId(), record(name).getSmolvmNodeId());
        assertEquals(URI.create("http://127.0.0.1:" + record.servicePort()), node.baseUrl());
        await().atMost(5, TimeUnit.SECONDS).until(() -> driver.calls("create", record.nodeId()).size() == 1);
        await().during(4, TimeUnit.SECONDS).atMost(6, TimeUnit.SECONDS)
                .until(() -> status(name) == ClusterStatus.CREATING);

        nodeCreate.countDown();
        awaitStatus(name, ClusterStatus.ACTIVE);

        // The SDK's Cluster; Floci's model Cluster is imported.
        software.amazon.awssdk.services.eks.model.Cluster active =
                eks.describeCluster(request -> request.name(name)).cluster();
        FakeKubeApi kube = driver.kubeApi();
        assertEquals("https://localhost:" + kube.port(), active.endpoint());
        assertEquals(Base64.getEncoder().encodeToString(kube.caPem().getBytes(StandardCharsets.US_ASCII)),
                active.certificateAuthority().data());
        assertNotNull(clusterManager.getRegisteredClusterNodeInstance(record(name)));
        assertEquals(record.instanceId(), clusterManager.getRegisteredClusterNodeInstance(record(name))
                .getInstanceId());
    }

    @Test
    void theClusterStaysCreatingUntilEveryConditionOfTheReadyRuleHolds() {
        String name = unique("ready");
        driver.holdReadiness();
        create(name);
        await().atMost(10, TimeUnit.SECONDS).until(() -> driver.nodeApi() != null);
        FakeKubeApi kube = driver.kubeApi();
        FakeNodeApi nodeApi = driver.nodeApi();
        String nodePath = "/api/v1/nodes/" + nodeName();

        kube.readyz(500, "[-]poststarthook/rbac/bootstrap-roles failed: not finished");
        awaitPass(() -> kube.requests("GET", "/readyz").size());
        assertEquals(ClusterStatus.CREATING, status(name));

        kube.readyz(200, "ok");
        awaitPass(() -> kube.requests("GET", nodePath).size());
        assertEquals(ClusterStatus.CREATING, status(name), "the node has not registered");

        kube.addNode(nodeName(), "False");
        awaitPass(() -> kube.requests("GET", nodePath).size());
        assertEquals(ClusterStatus.CREATING, status(name), "the node is not Ready");

        kube.changeNode(nodeName(), n -> ((ObjectNode) n.path("status").path("conditions").get(0))
                .put("status", "True"));
        String runtimeClass = "/apis/node.k8s.io/v1/runtimeclasses/" + NodeReadyRule.RUNTIME_CLASS;
        awaitPass(() -> kube.requests("GET", runtimeClass).size());
        assertEquals(ClusterStatus.CREATING, status(name), "RuntimeClass smolvm-pods is missing");

        nodeApi.token("a-token-that-the-seed-does-not-hold");
        kube.addRuntimeClass(NodeReadyRule.RUNTIME_CLASS);
        awaitPass(() -> podListings(nodeApi));
        assertEquals(ClusterStatus.CREATING, status(name), "the node service refuses Floci's token");

        nodeApi.token(driver.serviceToken());
        awaitStatus(name, ClusterStatus.ACTIVE);
    }

    @Test
    void aNodeThatFailsMakesTheClusterFailedAndTheNextClusterCreatesTheNodeAgain() {
        String name = unique("failed");
        driver.failCreates("smolvm machine create failed: the image is not a node image");

        create(name);

        awaitStatus(name, ClusterStatus.FAILED);
        assertThat(clusterManager.creationFailure(record(name)).orElseThrow(), containsString("not a node image"));
        String failedNode = record(name).getSmolvmNodeId();
        eks.deleteCluster(request -> request.name(name));
        assertThrows(ResourceNotFoundException.class, () -> status(name));

        driver.failCreates(null);
        String next = unique("next");
        create(next);
        awaitStatus(next, ClusterStatus.ACTIVE);
        assertFalse(record(next).getSmolvmNodeId().equals(failedNode), "a new node");
        assertTrue(driver.machine(failedNode).isEmpty());
    }

    @Test
    void describeClusterShowsNoInternalField() throws Exception {
        String name = unique("fields");
        create(name);
        awaitStatus(name, ClusterStatus.ACTIVE);

        String body = given()
                .header("Authorization", "AWS4-HMAC-SHA256 Credential=test/20261021/us-east-1/eks/aws4_request, "
                        + "SignedHeaders=host, Signature=abc")
                .get("/clusters/" + name)
                .then().statusCode(200).extract().asString();

        JsonNode cluster = JSON.readTree(body).path("cluster");
        assertEquals(name, cluster.path("name").asText());
        for (String internal : List.of("provider", "smolvmNodeId", "explicitVersion", "nodeInstanceType",
                "clusterArgs", "defaultStorageClass", "joinedInstanceIds")) {
            assertTrue(cluster.path(internal).isMissingNode(), internal + " is in " + body);
        }
        assertThat(body, not(containsString("floci-node-")));
    }

    @Test
    void aSecondCreateClusterIsRefusedWhileAClusterHoldsTheNode() {
        String first = unique("first");
        create(first);
        awaitStatus(first, ClusterStatus.ACTIVE);
        String nodeId = record(first).getSmolvmNodeId();

        String second = unique("second");
        ResourceLimitExceededException refused = assertThrows(ResourceLimitExceededException.class,
                () -> create(second));

        assertEquals(400, refused.statusCode());
        assertThat(refused.awsErrorDetails().errorMessage(), containsString("Floci runs one EKS cluster per "
                + "smolvm-pods node, and cluster " + first));
        assertThat(refused.awsErrorDetails().errorMessage(), containsString("FLOCI_SERVICES_EKS_PROVIDER=k3s"));
        assertThat(eks.listClusters().clusters(), not(hasItem(second)));
        try (EksClient ireland = client(Region.EU_WEST_1)) {
            assertThrows(ResourceLimitExceededException.class,
                    () -> ireland.createCluster(request -> request.name(second).roleArn(ROLE)));
        }

        eks.deleteCluster(request -> request.name(first));
        create(second);
        awaitStatus(second, ClusterStatus.ACTIVE);
        assertEquals(nodeId, record(second).getSmolvmNodeId(), "the next cluster adopts the node");
        assertEquals(1, driver.calls("create").size());
    }

    @Test
    void deleteClusterRemovesItsKubernetesObjectsAndKeepsTheNode() {
        String name = unique("delete");
        create(name);
        awaitStatus(name, ClusterStatus.ACTIVE);
        Cluster active = record(name);
        NodeRecord node = flociNode.record().orElseThrow();
        eks.createNodegroup(request -> request.clusterName(name).nodegroupName("workers")
                .nodeRole("arn:aws:iam::" + ACCOUNT + ":role/eks-node").subnets("subnet-0123456789abcdef0")
                .instanceTypes("t3.medium").labels(Map.of("team", "orders"))
                .taints(taint -> taint.key("dedicated").value("orders").effect(TaintEffect.NO_SCHEDULE)));
        FakeKubeApi kube = driver.kubeApi();
        await().atMost(30, TimeUnit.SECONDS).until(() -> kube.kubeNode(nodeName()).path("metadata")
                .path("labels").path("team").asText("").equals("orders"));
        kube.addNamespace("shop");
        kube.addService("web");

        eks.deleteNodegroup(request -> request.clusterName(name).nodegroupName("workers"));
        eks.deleteCluster(request -> request.name(name));

        assertThrows(ResourceNotFoundException.class, () -> status(name));
        assertTrue(driver.calls("delete").isEmpty(), "the node stays");
        assertTrue(driver.machine(node.nodeId()).orElseThrow().running());
        assertTrue(Files.isDirectory(seeds.seedDir(node.nodeId())));
        assertTrue(directory.find(FlociNode.NAME).isPresent());
        assertEquals(FlociNode.State.READY, flociNode.status().state());
        assertEquals(Set.of("default", "kube-system", "kube-public", "kube-node-lease"), kube.namespaces());
        assertEquals(Set.of("kubernetes"), kube.services());
        assertFalse(kube.requests("DELETE", "/apis/apps/v1/namespaces/default/deployments").isEmpty());
        JsonNode metadata = kube.kubeNode(nodeName()).path("metadata");
        assertTrue(metadata.path("labels").path("team").isMissingNode(), "the node group's label goes");
        assertTrue(kube.kubeNode(nodeName()).path("spec").path("taints").isEmpty(), "the node group's taint goes");
        assertNull(clusterManager.getRegisteredClusterNodeInstance(active));
    }

    @Test
    void aClusterDeletedWhileTheNodeIsCreatedLeavesTheNodeToTheNextCluster() {
        String name = unique("early");
        CountDownLatch nodeCreate = driver.holdCreates();
        create(name);
        String nodeId = record(name).getSmolvmNodeId();
        await().atMost(5, TimeUnit.SECONDS).until(() -> driver.calls("create", nodeId).size() == 1);

        eks.deleteCluster(request -> request.name(name));

        assertThrows(ResourceNotFoundException.class, () -> status(name));
        nodeCreate.countDown();
        await().atMost(10, TimeUnit.SECONDS).until(() -> flociNode.status().state() == FlociNode.State.READY);
        String next = unique("next");
        create(next);
        awaitStatus(next, ClusterStatus.ACTIVE);
        assertEquals(nodeId, record(next).getSmolvmNodeId());
        assertTrue(driver.calls("delete").isEmpty());
    }

    @Test
    void theGuardsKeepASmolvmClusterOnItsNode() {
        String name = unique("guards");
        eks.createCluster(request -> request.name(name).roleArn(ROLE)
                .logging(logging -> logging.clusterLogging(setup -> setup.enabled(true).types(LogType.API))));
        awaitStatus(name, ClusterStatus.ACTIVE);
        Cluster cluster = record(name);
        String nodeId = cluster.getSmolvmNodeId();
        Instance instance = new Instance();
        instance.setInstanceId("i-0123456789abcdef0");
        instance.setRegion(REGION);

        eks.createNodegroup(request -> request.clusterName(name).nodegroupName("workers")
                .nodeRole("arn:aws:iam::" + ACCOUNT + ":role/eks-node").subnets("subnet-0123456789abcdef0")
                .instanceTypes("t3.medium").labels(Map.of("team", "orders"))
                .taints(taint -> taint.key("dedicated").value("orders").effect(TaintEffect.NO_SCHEDULE)));

        FakeKubeApi kube = driver.kubeApi();
        await().atMost(30, TimeUnit.SECONDS).until(() -> kube.kubeNode(nodeName()).path("metadata")
                .path("labels").path("team").asText("").equals("orders"));
        JsonNode node = kube.kubeNode(nodeName());
        assertEquals("t3.medium", node.path("metadata").path("labels").path("node.kubernetes.io/instance-type")
                .asText());
        assertEquals("dedicated", node.path("spec").path("taints").path(0).path("key").asText());
        assertEquals("NoSchedule", node.path("spec").path("taints").path(0).path("effect").asText());
        assertEquals(ClusterStatus.ACTIVE, status(name), "a node group does not restart the node");

        assertTrue(clusterManager.restartForNodeCapacity(cluster, "m5.large"));
        UserDataPipeline.ExecutionResult userData = clusterManager.executeUserData(cluster, "workers",
                "#!/bin/sh\necho hi\n");
        assertTrue(userData.isSuccess());
        assertThat(userData.getOutputSummary(), containsString("smolvm-pods"));
        AwsException join = assertThrows(AwsException.class, () -> clusterManager.joinInstance(cluster, instance));
        assertEquals("InvalidParameterException", join.getErrorCode());
        clusterManager.recordJoinedInstanceId(cluster, instance);
        assertNull(cluster.getJoinedInstanceIds());
        assertFalse(clusterManager.isInstanceJoined(cluster, instance.getInstanceId()));
        clusterManager.attachClusterLogs(cluster);
        assertNull(clusterManager.getLogHandle(cluster));
        assertEquals(Optional.empty(), clusterManager.creationFailure(cluster));

        clusterManager.detachCluster(cluster);
        clusterManager.shutdownCluster(cluster);
        assertEquals("running", driver.machine(nodeId).orElseThrow().state(), "the node is Floci's, not the cluster's");
        clusterManager.restoreCluster(cluster);
        assertTrue(clusterManager.isReady(cluster));
        assertEquals(1, driver.calls("create", nodeId).size(), "a restored cluster takes the node as it is");
        assertTrue(driver.calls("start", nodeId).isEmpty());
    }

    /** The JSONPatch that Floci's admission webhook at {@code path} gives a pod of {@code namespace/serviceAccount}. */
    private JsonNode admit(String path, String namespace, String serviceAccount) throws IOException {
        Map<String, Object> pod = Map.of("kind", "Pod", "spec", Map.of("serviceAccountName", serviceAccount,
                "containers", List.of(Map.of("name", "app"))));
        Map<String, Object> review = Map.of("apiVersion", "admission.k8s.io/v1", "kind", "AdmissionReview",
                "request", Map.of("uid", "request-1", "namespace", namespace, "object", pod));
        String patch = given().contentType("application/json").body(review)
                .post(path)
                .then().statusCode(200).body("response.allowed", equalTo(true)).extract().path("response.patch");
        return patch == null ? JSON.createArrayNode() : JSON.readTree(Base64.getDecoder().decode(patch));
    }

    private static String clusterWebhook(String name) {
        return "/_floci/eks/clusters/" + name + "/pod-identity-webhook/scope/" + ACCOUNT;
    }

    /** The node's own route, which the node's seed names. */
    private static final String NODE_WEBHOOK = "/_floci/eks/smolvm-node/pod-identity-webhook";

    /** The value that {@code patch} adds at {@code path}. */
    private static JsonNode added(JsonNode patch, String path) {
        for (JsonNode operation : patch) {
            if (operation.path("op").asText().equals("add") && operation.path("path").asText().equals(path)) {
                return operation.path("value");
            }
        }
        throw new AssertionError("the patch adds nothing at " + path + ": " + patch);
    }

    /** The value of variable {@code name} in {@code env}, or null. */
    private static String variable(JsonNode env, String name) {
        for (JsonNode entry : env) {
            if (entry.path("name").asText().equals(name)) {
                return entry.path("value").asText();
            }
        }
        return null;
    }

    @Test
    void aPodGetsIrsaFromItsServiceAccountThroughTheNodesRouteToo() throws Exception {
        String name = unique("irsa");
        create(name);
        awaitStatus(name, ClusterStatus.ACTIVE);
        String roleArn = RequestScopes.callAs(ACCOUNT, () -> iamService.createRole(unique("pod-role"), "/",
                POD_IDENTITY_TRUST, null, 3600, null).getArn());
        eks.createAddon(request -> request.clusterName(name).addonName(EksPodIdentityWebhook.POD_IDENTITY_ADDON));
        eks.createPodIdentityAssociation(request -> request.clusterName(name).namespace("team")
                .serviceAccount("app").roleArn(roleArn));
        FakeKubeApi kube = driver.kubeApi();
        kube.addServiceAccount("team", "app", Map.of("eks.amazonaws.com/role-arn", roleArn));
        kube.addServiceAccount("team", "plain", Map.of());

        for (String route : List.of(clusterWebhook(name), NODE_WEBHOOK)) {
            JsonNode patch = admit(route, "team", "app");

            JsonNode volume = added(patch, "/spec/volumes").path(0);
            assertEquals(EksIrsaWebhook.VOLUME_NAME, volume.path("name").asText());
            JsonNode token = volume.at("/projected/sources/0/serviceAccountToken");
            assertEquals(EksOidcService.STS_AUDIENCE, token.path("audience").asText());
            JsonNode env = added(patch, "/spec/containers/0/env");
            assertEquals(roleArn, variable(env, "AWS_ROLE_ARN"), route);
            assertEquals(REGION, variable(env, "AWS_REGION"));
            assertNull(variable(env, EksPodIdentityWebhook.ENV_CREDENTIALS_FULL_URI));
            assertEquals(0, admit(route, "team", "plain").size(), "a ServiceAccount that names no role");
        }
        List<FakeKubeApi.Request> reads = kube.requests("GET", "/api/v1/namespaces/team/serviceaccounts/app");
        assertFalse(reads.isEmpty());
        assertEquals("Bearer " + seeds.adminToken(record(name).getSmolvmNodeId()), reads.getFirst().authorization());

        eks.deleteCluster(request -> request.name(name));
        assertEquals(0, admit(NODE_WEBHOOK, "team", "app").size(), "a node without a cluster admits pods as they are");
    }

    @Test
    void theNodesTokenWebhookAuthenticatesNobodyWithoutACluster() {
        given().contentType("application/json")
                .body(Map.of("apiVersion", "authentication.k8s.io/v1", "kind", "TokenReview",
                        "spec", Map.of("token", "k8s-aws-v1.not-a-token")))
                .post("/_floci/eks/smolvm-node/token-webhook")
                .then().statusCode(200)
                .body("apiVersion", equalTo("authentication.k8s.io/v1"))
                .body("status.authenticated", equalTo(false));
    }

    @Test
    void aTokenThatTheNodeSignsWithItsSeedKeyAssumesTheRoleOfItsServiceAccount() throws Exception {
        String name = unique("sts");
        create(name);
        awaitStatus(name, ClusterStatus.ACTIVE);
        String issuer = eks.describeCluster(request -> request.name(name)).cluster().identity().oidc().issuer();
        String provider = issuer.replaceFirst("^https://", "");
        String roleName = unique("irsa-role");
        String trust = """
                {"Version":"2012-10-17","Statement":[{"Effect":"Allow",
                  "Principal":{"Federated":"arn:aws:iam::%1$s:oidc-provider/%2$s"},
                  "Action":"sts:AssumeRoleWithWebIdentity",
                  "Condition":{"StringEquals":{"%2$s:sub":"system:serviceaccount:team:app",
                                               "%2$s:aud":"sts.amazonaws.com"}}}]}
                """.formatted(ACCOUNT, provider);
        String roleArn = RequestScopes.callAs(ACCOUNT,
                () -> iamService.createRole(roleName, "/", trust, null, 3600, null).getArn());
        driver.kubeApi().addServiceAccount("team", "app", Map.of("eks.amazonaws.com/role-arn", roleArn));
        JsonNode patch = admit(clusterWebhook(name), "team", "app");
        String audience = added(patch, "/spec/volumes").at("/0/projected/sources/0/serviceAccountToken/audience")
                .asText();
        String role = variable(added(patch, "/spec/containers/0/env"), "AWS_ROLE_ARN");
        PrivateKey nodeKey = signingKey(seeds.seedDir(record(name).getSmolvmNodeId()));
        String result = "AssumeRoleWithWebIdentityResponse.AssumeRoleWithWebIdentityResult.";

        assumeRoleWithWebIdentity(role, projectedToken(nodeKey, issuer, "app", audience))
                .statusCode(200)
                .body(result + "SubjectFromWebIdentityToken", equalTo("system:serviceaccount:team:app"))
                .body(result + "Provider", equalTo(issuer))
                .body(result + "AssumedRoleUser.Arn", equalTo("arn:aws:sts::" + ACCOUNT + ":assumed-role/"
                        + roleName + "/irsa-session"))
                .body(result + "Credentials.AccessKeyId", startsWith("ASIA"));
        assumeRoleWithWebIdentity(role, projectedToken(nodeKey, issuer, "other", audience))
                .statusCode(403)
                .body(containsString("AccessDenied"));
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        assumeRoleWithWebIdentity(role, projectedToken(generator.generateKeyPair().getPrivate(), issuer, "app",
                audience))
                .statusCode(400)
                .body(containsString("InvalidIdentityToken"));
    }

    /** The service account signing key in a node's seed, which its API server signs projected tokens with. */
    private static PrivateKey signingKey(Path seed) throws Exception {
        String pem = Files.readString(seed.resolve(NodeSeed.SIGNING_KEY_FILE), StandardCharsets.US_ASCII);
        String body = pem.replaceAll("-----[A-Z ]+-----", "").replaceAll("\\s", "");
        return KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(body)));
    }

    /** A projected token of ServiceAccount {@code team/serviceAccount}, as a Kubernetes API server signs it. */
    private static String projectedToken(PrivateKey key, String issuer, String serviceAccount, String audience)
            throws Exception {
        long now = Instant.now().getEpochSecond();
        ObjectNode header = JSON.createObjectNode().put("alg", "RS256").put("kid", "node");
        ObjectNode claims = JSON.createObjectNode();
        claims.putArray("aud").add(audience);
        claims.put("exp", now + 3600).put("iat", now).put("nbf", now).put("iss", issuer)
                .put("sub", "system:serviceaccount:team:" + serviceAccount);
        claims.putObject("kubernetes.io").put("namespace", "team").putObject("serviceaccount")
                .put("name", serviceAccount);
        String input = Base64Url.encode(JSON.writeValueAsBytes(header)) + "."
                + Base64Url.encode(JSON.writeValueAsBytes(claims));
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(key);
        signature.update(input.getBytes(StandardCharsets.US_ASCII));
        return input + "." + Base64Url.encode(signature.sign());
    }

    private static ValidatableResponse assumeRoleWithWebIdentity(String roleArn, String token) {
        return given().contentType("application/x-www-form-urlencoded")
                .formParam("Action", "AssumeRoleWithWebIdentity")
                .formParam("Version", "2011-06-15")
                .formParam("RoleArn", roleArn)
                .formParam("RoleSessionName", "irsa-session")
                .formParam("WebIdentityToken", token)
                .post("/")
                .then();
    }

    @Test
    void controlPlaneAndContainerInsightsLogsShipFromTheNode() throws Exception {
        String name = unique("logs");
        eks.createCluster(request -> request.name(name).roleArn(ROLE)
                .logging(logging -> logging.clusterLogging(setup -> setup.enabled(true).types(LogType.API))));
        awaitStatus(name, ClusterStatus.ACTIVE);
        FakeNodeApi nodeApi = driver.nodeApi();
        String group = "/aws/eks/" + name + "/cluster";
        String id = EksSmolvmControlPlaneLogs.streamId(record(name).getSmolvmNodeId());
        await().atMost(10, TimeUnit.SECONDS).until(() -> logFollows(nodeApi, "k3s") == 1);
        Thread.sleep(300);
        nodeApi.appendLog("k3s", "I1025 apiserver is ready");
        await().atMost(10, TimeUnit.SECONDS).until(() -> events(group, "kube-apiserver-" + id)
                .contains("I1025 apiserver is ready"));

        eks.updateClusterConfig(request -> request.name(name)
                .logging(logging -> logging.clusterLogging(setup -> setup.enabled(true).types(LogType.AUDIT))));
        await().atMost(10, TimeUnit.SECONDS).until(() -> logFollows(nodeApi, "kube-apiserver-audit") == 1);
        Thread.sleep(300);
        nodeApi.appendLog("kube-apiserver-audit", "{\"kind\":\"Event\",\"verb\":\"get\"}");
        await().atMost(10, TimeUnit.SECONDS).until(() -> events(group, "kube-apiserver-audit-" + id)
                .contains("{\"kind\":\"Event\",\"verb\":\"get\"}"));

        FakeKubeApi kube = driver.kubeApi();
        kube.addPod("shop", "web-0", "web", "abc123", nodeName());
        kube.podLog("shop", "web-0", Instant.now(), "before the add-on");
        eks.createAddon(request -> request.clusterName(name).addonName("amazon-cloudwatch-observability"));
        String logPath = "/api/v1/namespaces/shop/pods/web-0/log";
        await().atMost(30, TimeUnit.SECONDS).until(() -> kube.requests("GET", logPath).size() == 1);
        Thread.sleep(300);
        kube.podLog("shop", "web-0", Instant.now(), "GET / 200");
        String stream = nodeName() + "-application.var.log.containers.web-0_shop_web-abc123.log";
        String application = "/aws/containerinsights/" + name + "/application";
        await().atMost(10, TimeUnit.SECONDS).until(() -> !events(application, stream).isEmpty());

        List<String> shipped = events(application, stream);
        assertEquals(1, shipped.size(), shipped.toString());
        JsonNode event = JSON.readTree(shipped.getFirst());
        assertEquals("GET / 200", event.path("log").asText());
        assertEquals("web-0", event.path("kubernetes").path("pod_name").asText());
        assertEquals("abc123", event.path("kubernetes").path("docker_id").asText());
    }

    private static long logFollows(FakeNodeApi nodeApi, String log) {
        return nodeApi.requests().stream().filter(request -> request.path().equals("/v1/logs/" + log)).count();
    }

    private List<String> events(String group, String stream) {
        try (CloudWatchLogsClient logs = CloudWatchLogsClient.builder().endpointOverride(endpoint)
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder()).build()) {
            return logs.getLogEvents(request -> request.logGroupName(group).logStreamName(stream).startFromHead(true))
                    .events().stream().map(OutputLogEvent::message).toList();
        } catch (software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException e) {
            // Named in full: EKS's ResourceNotFoundException is the one imported. No stream yet is no event.
            return List.of();
        }
    }

    @Test
    void callerArgumentsThatTheNodeSetsAreRefusedWith400() {
        Map<String, String> refused = Map.of(
                "floci:kubelet-arg:resolv-conf", "/run/systemd/resolve/resolv.conf",
                "floci:kube-apiserver-arg:token-auth-file", "/etc/tokens.csv",
                "floci:kubelet-arg:cpu-manager-policy", "static",
                "floci:kubelet-arg:cluster-cidr", "10.244.0.0/16");
        for (Map.Entry<String, String> tag : refused.entrySet()) {
            String name = unique("refused");

            InvalidParameterException e = assertThrows(InvalidParameterException.class,
                    () -> eks.createCluster(request -> request.name(name).roleArn(ROLE)
                            .tags(Map.of(tag.getKey(), tag.getValue()))));

            assertEquals(400, e.statusCode(), tag.getKey());
            assertThat(eks.listClusters().clusters(), not(hasItem(name)));
        }
        String allowed = unique("allowed");
        create(allowed);
        assertEquals(ClusterStatus.CREATING, status(allowed), "a refused create holds no node");
    }
}
