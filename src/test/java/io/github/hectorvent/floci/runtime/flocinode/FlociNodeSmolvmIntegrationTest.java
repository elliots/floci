package io.github.hectorvent.floci.runtime.flocinode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.runtime.node.NodePod;
import io.github.hectorvent.floci.runtime.node.NodeToken;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import io.github.hectorvent.floci.services.eks.FlociProcess;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterOverrideConfiguration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException;
import software.amazon.awssdk.services.eks.EksClient;
import software.amazon.awssdk.services.eks.model.LogType;
import software.amazon.awssdk.services.eks.model.ResourceLimitExceededException;
import software.amazon.awssdk.services.eks.waiters.EksWaiter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RDS and EKS on the one smolvm-pods node of a real Floci, with no node in the runtime file: a
 * PostgreSQL instance created first has Floci create the node; an EKS cluster created next adopts
 * the node and its k3s, where a VM pod runs; a second cluster is refused; the RDS Data API reaches
 * the sleeping database through the node, which wakes it; the database's exported postgresql log,
 * the cluster's api log and the database's AWS/RDS metrics reach CloudWatch, and a minute of
 * metrics does not wake the sleeping database; deleting the cluster keeps the database,
 * and deleting the database keeps the node. Floci runs as a child process ({@link FlociProcess}),
 * and an emulator reset deletes the node at the end.
 *
 * <p>It needs an Apple silicon Mac of M3 or later, the node image of {@code node-vm.sh build},
 * Docker with {@code postgres:16-alpine} and busybox or access to Docker Hub, and the AWS CLI and
 * kubectl on the PATH, and runs only with {@code FLOCI_NODE_SMOLVM_E2E=1}:
 *
 * <pre>
 * ./mvnw package -DskipTests
 * SMOLVM_SRC=/path/to/smolvm FLOCI_NODE_SMOLVM_E2E=1 ./mvnw test -Dtest=FlociNodeSmolvmIntegrationTest
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "FLOCI_NODE_SMOLVM_E2E", matches = "1")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Timeout(value = 30, unit = TimeUnit.MINUTES)
class FlociNodeSmolvmIntegrationTest {

    private static final Logger LOG = Logger.getLogger(FlociNodeSmolvmIntegrationTest.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String ACCOUNT = "000000000000";
    private static final String REGION = "us-east-1";
    private static final String DATABASE = "node-e2e-pg";
    private static final String CLUSTER = "node-e2e";
    private static final String SECOND = "node-e2e-second";
    private static final String CLUSTER_ROLE = "arn:aws:iam::" + ACCOUNT + ":role/node-e2e-cluster";
    private static final String NAMESPACE = "node-e2e";
    private static final String USER = "admin";
    private static final String PASSWORD = "password123";
    /** The node's shortest idle timeout; the node reads conntrack every 15 s on top. */
    private static final String IDLE_TIMEOUT = "10s";
    /** busybox 1.37 for linux/arm64. */
    private static final String BUSYBOX_IMAGE =
            "busybox@sha256:bdf57e528e45e4433820e045b29b4597825a1c9e38353532d90a01445013f82e";
    /** What a cluster leaves outside its namespaces: a CRD, a webhook that refuses, and roles. */
    private static final String LEFTOVERS = """
            apiVersion: apiextensions.k8s.io/v1
            kind: CustomResourceDefinition
            metadata:
              name: widgets.e2e.floci.io
            spec:
              group: e2e.floci.io
              names:
                kind: Widget
                plural: widgets
                singular: widget
              scope: Namespaced
              versions:
                - name: v1
                  served: true
                  storage: true
                  schema:
                    openAPIV3Schema:
                      type: object
                      x-kubernetes-preserve-unknown-fields: true
            ---
            apiVersion: admissionregistration.k8s.io/v1
            kind: ValidatingWebhookConfiguration
            metadata:
              name: node-e2e-refuse
            webhooks:
              - name: refuse.e2e.floci.io
                admissionReviewVersions: ["v1"]
                sideEffects: None
                failurePolicy: Fail
                clientConfig:
                  url: "https://127.0.0.1:1/refuse"
                rules:
                  - operations: ["CREATE"]
                    apiGroups: ["e2e.floci.io"]
                    apiVersions: ["v1"]
                    resources: ["widgets"]
            ---
            apiVersion: rbac.authorization.k8s.io/v1
            kind: ClusterRole
            metadata:
              name: node-e2e-reader
            rules:
              - apiGroups: [""]
                resources: ["pods"]
                verbs: ["get"]
            ---
            apiVersion: rbac.authorization.k8s.io/v1
            kind: Role
            metadata:
              name: node-e2e-reader
              namespace: default
            rules:
              - apiGroups: [""]
                resources: ["pods"]
                verbs: ["get"]
            """;
    private static final Duration READY_TIMEOUT = Duration.ofMinutes(10);
    private static final Duration COMMAND_TIMEOUT = Duration.ofMinutes(6);

    private final Path moduleDir = Path.of("").toAbsolutePath();
    private final String runId = UUID.randomUUID().toString().substring(0, 6);
    private final String resourceNamespace = "node-e2e-" + runId;
    private final Path work = moduleDir.resolve("target/node-smolvm-e2e").resolve(runId);
    private final Path data = work.resolve("data");
    private final Path kubeconfig = work.resolve("kube/config.yaml");
    private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    private final SmolvmNodeClient nodeClient = new SmolvmNodeClient(JSON);
    private final List<String> pushedImages = new ArrayList<>();
    private final List<String> pulledImages = new ArrayList<>();
    private Path nodeVmScript;
    private String smolvmSrc;
    private FlociProcess floci;
    private RdsClient rds;
    private EksClient eks;
    private EksWaiter waiter;
    private String accessKeyId;
    private String secretAccessKey;
    private String instanceArn;
    private String secretArn;
    private int endpointPort;
    private String nodeId;

    @BeforeAll
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void startFloci() throws IOException {
        Files.createDirectories(work.resolve("aws"));
        Files.createDirectories(kubeconfig.getParent());
        Path pods = moduleDir.resolve("tools/smolvm-pods");
        nodeVmScript = pods.resolve("scripts/node-vm.sh");
        smolvmSrc = firstSet(System.getenv("SMOLVM_SRC"), moduleDir.resolveSibling("smolvm").toString());
        assertTrue(Files.isDirectory(Path.of(smolvmSrc)), "No smolvm checkout at " + smolvmSrc + ": export SMOLVM_SRC");
        Path image = Path.of(firstSet(System.getenv("FLOCI_RUNTIME_NODE_IMAGE"),
                pods.resolve("target/node/node-image.tar").toString()));
        assertTrue(Files.isRegularFile(image), "No node image at " + image + ": run node-vm.sh build");
        Path runtimeFile = work.resolve("runtime.yaml");
        Files.writeString(runtimeFile, "version: 1\n");

        Map<String, String> env = new LinkedHashMap<>();
        env.put("FLOCI_STORAGE_MODE", "memory");
        env.put("FLOCI_DOCKER_RESOURCE_NAMESPACE", resourceNamespace);
        env.put("FLOCI_TLS_ENABLED", "true");
        env.put("FLOCI_TLS_AWS_HTTPS_PORT", "0");
        env.put("FLOCI_SERVICES_UI_ENABLED", "false");
        env.put("FLOCI_SERVICES_ECR_URI_STYLE", "path");
        env.put("FLOCI_SERVICES_ECR_DATA_PATH", data.resolve("ecr").toString());
        env.put("FLOCI_SERVICES_EKS_DATA_PATH", data.resolve("eks").toString());
        env.put("FLOCI_SERVICES_EKS_PROVIDER", "smolvm-pods");
        env.put("FLOCI_SERVICES_RDS_SMOLVM_ENABLED", "true");
        env.put("FLOCI_SERVICES_RDS_SMOLVM_IDLE_TIMEOUT", IDLE_TIMEOUT);
        env.put("FLOCI_RUNTIME_NODE_NODE_VM_SCRIPT", nodeVmScript.toString());
        env.put("FLOCI_RUNTIME_NODE_SMOLVM_SRC", smolvmSrc);
        env.put("FLOCI_RUNTIME_NODE_IMAGE", image.toString());
        env.put("FLOCI_RUNTIME_NODE_IMAGE_SHA256", sha256(image));
        env.put("FLOCI_RUNTIME_NODE_DATA_PATH", data.resolve("node").toString());
        env.put("FLOCI_RUNTIME_CONFIG_FILE", runtimeFile.toString());
        env.put("FLOCI_RUNTIME_DNS_LISTEN", "127.0.0.1:" + freePort());
        floci = FlociProcess.of(moduleDir, work, freePort());
        floci.start(env);
        rds = RdsClient.builder().endpointOverride(floci.endpoint()).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder().socketTimeout(Duration.ofMinutes(5))).build();
        eks = EksClient.builder().endpointOverride(floci.endpoint()).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder()).build();
        waiter = EksWaiter.builder().client(eks).overrideConfiguration(WaiterOverrideConfiguration.builder()
                .waitTimeout(READY_TIMEOUT).backoffStrategyV2(BackoffStrategy.fixedDelay(Duration.ofSeconds(1)))
                .build()).build();
        LOG.infov("Floci for the node end-to-end test: resource namespace {0}, data {1}", resourceNamespace, data);
    }

    @AfterAll
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void removeEverythingTheTestCreated() {
        if (floci != null) {
            tolerate(() -> assertEquals(200, post("/_floci/state/reset", "")));
            tolerate(floci::stop);
        }
        if (nodeVmScript != null) {
            tolerate(() -> {
                for (JsonNode machine : ownMachines()) {
                    String name = machine.path("name").asText();
                    LOG.infov("Deleting smolvm-pods node {0}, which the test left", name);
                    tolerate(() -> nodeVm("delete", "--name", name));
                }
            });
        }
        for (String image : pushedImages) {
            tolerate(() -> run(List.of("docker", "rmi", image), Map.of(), null));
        }
        for (String image : pulledImages) {
            tolerate(() -> run(List.of("docker", "rmi", image), Map.of(), null));
        }
        if (rds != null) {
            rds.close();
        }
        if (eks != null) {
            eks.close();
        }
    }

    @Test
    @Order(1)
    void aPostgresInstanceCreatedFirstHasFlociCreateTheNode() throws SQLException {
        assertTrue(ownMachines().isEmpty(), "no node before the first use");
        secretArn = json(aws("secretsmanager", "create-secret", "--name", "node-e2e-db", "--secret-string",
                "{\"username\":\"" + USER + "\",\"password\":\"" + PASSWORD + "\"}"), "/ARN").asText();

        Instant started = Instant.now();
        DBInstance created = rds.createDBInstance(request -> request.dbInstanceIdentifier(DATABASE)
                .engine("postgres").masterUsername(USER).masterUserPassword(PASSWORD).dbName("appdb")
                .dbInstanceClass("db.t3.micro").allocatedStorage(20)
                .enableCloudwatchLogsExports("postgresql")).dbInstance();
        LOG.infov("CreateDBInstance answered {0} after {1} ms", created.dbInstanceStatus(),
                Duration.between(started, Instant.now()).toMillis());
        assertEquals("creating", created.dbInstanceStatus(), "the instance waits for the node");
        instanceArn = created.dbInstanceArn();
        endpointPort = created.endpoint().port();
        await().atMost(READY_TIMEOUT).pollInterval(Duration.ofSeconds(2)).until(() -> "available".equals(
                rds.describeDBInstances(request -> request.dbInstanceIdentifier(DATABASE)).dbInstances().getFirst()
                        .dbInstanceStatus()));
        LOG.infov("The database was available {0} s after CreateDBInstance",
                Duration.between(started, Instant.now()).toSeconds());

        List<JsonNode> machines = ownMachines();
        assertEquals(1, machines.size(), "the node Floci created");
        assertEquals("running", machines.getFirst().path("state").asText());
        nodeId = machines.getFirst().path("name").asText();
        assertThat(nodeId, startsWith("floci-node-"));
        assertEquals("awake", databasePod().path("state").asText());
        try (Connection connection = connect(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE marker (v text)");
            statement.execute("INSERT INTO marker VALUES ('before-the-cluster')");
        }
        await().atMost(Duration.ofMinutes(1)).pollInterval(Duration.ofSeconds(2)).until(() ->
                events("/aws/rds/instance/" + DATABASE + "/postgresql", DATABASE).stream()
                        .anyMatch(message -> message.contains("database system is ready to accept connections")));
    }

    @Test
    @Order(2)
    void anEksClusterCreatedNextAdoptsTheNodeAndRunsAVmPod() {
        JsonNode key = json(aws("iam", "create-access-key", "--user-name",
                json(aws("iam", "create-user", "--user-name", "node-e2e"), "/User/UserName").asText()), "/AccessKey");
        accessKeyId = key.path("AccessKeyId").asText();
        secretAccessKey = key.path("SecretAccessKey").asText();

        Instant started = Instant.now();
        eks.createCluster(request -> request.name(CLUSTER).roleArn(CLUSTER_ROLE)
                .logging(logging -> logging.clusterLogging(setup -> setup.enabled(true).types(LogType.API))));
        waiter.waitUntilClusterActive(request -> request.name(CLUSTER)).matched().response()
                .orElseThrow(() -> new AssertionError(CLUSTER + " did not become ACTIVE:\n" + floci.logTail(60)));
        LOG.infov("The cluster was ACTIVE {0} s after CreateCluster on the node",
                Duration.between(started, Instant.now()).toSeconds());

        List<JsonNode> machines = ownMachines();
        assertEquals(1, machines.size(), "one node for RDS and EKS");
        assertEquals(nodeId, machines.getFirst().path("name").asText(), "the cluster adopted the node");
        assertEquals("https://localhost:" + label(machines.getFirst(), "api-port"),
                eks.describeCluster(request -> request.name(CLUSTER)).cluster().endpoint());

        aws("eks", "update-kubeconfig", "--name", CLUSTER, "--kubeconfig", kubeconfig.toString());
        JsonNode nodes = json(kubectl("get", "nodes", "-o", "json"), "/items");
        assertEquals(1, nodes.size(), "kubectl get nodes: " + nodes);
        assertTrue(ready(nodes.get(0)), "the node is Ready");

        String image = push(BUSYBOX_IMAGE, "linux/arm64", "node-e2e/busybox", "1.37");
        kubectlApply("""
                apiVersion: v1
                kind: Namespace
                metadata:
                  name: %1$s
                ---
                apiVersion: v1
                kind: Pod
                metadata:
                  name: vm
                  namespace: %1$s
                spec:
                  runtimeClassName: smolvm-pods
                  containers:
                    - name: busybox
                      image: %2$s
                      command: ["sh", "-c", "exec sleep 3600"]
                """.formatted(NAMESPACE, image));
        await().atMost(Duration.ofMinutes(1)).pollInterval(Duration.ofSeconds(1)).until(() ->
                run(kubectlCommand("get", "pod", "vm", "-n", NAMESPACE), kubectlEnv(), null).exit() == 0);
        kubectl("wait", "--for=condition=Ready", "pod/vm", "-n", NAMESPACE, "--timeout=300s");
        List<String> vmPods = nodeClient.listPods(node(), SmolvmNodeClient.PodQuery.namespace(NAMESPACE)).stream()
                .map(NodePod::name).toList();
        assertThat("VM pods of the node", vmPods, hasItem("vm"));
        String apiStream = "kube-apiserver-" + controlPlaneStreamId();
        await().atMost(Duration.ofMinutes(1)).pollInterval(Duration.ofSeconds(2)).until(() ->
                !events("/aws/eks/" + CLUSTER + "/cluster", apiStream).isEmpty());
    }

    @Test
    @Order(3)
    void theDatabaseSendsItsAwsRdsMetricsEachMinute() {
        await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofSeconds(5)).until(() ->
                !datapoints("FreeableMemory").isEmpty());

        JsonNode memory = datapoints("FreeableMemory").get(0);
        assertTrue(memory.path("Maximum").asDouble() > 0, "FreeableMemory " + memory);
        assertFalse(datapoints("CPUUtilization").isEmpty(), "CPUUtilization");
        assertFalse(datapoints("DatabaseConnections").isEmpty(), "DatabaseConnections");
    }

    @Test
    @Order(4)
    void aSecondCreateClusterIsRefused() {
        ResourceLimitExceededException refused = assertThrows(ResourceLimitExceededException.class,
                () -> eks.createCluster(request -> request.name(SECOND).roleArn(CLUSTER_ROLE)));

        assertThat(refused.awsErrorDetails().errorMessage(), containsString("one EKS cluster per smolvm-pods node"));
        assertThat(refused.awsErrorDetails().errorMessage(), containsString("FLOCI_SERVICES_EKS_PROVIDER=k3s"));
        assertFalse(eks.listClusters().clusters().contains(SECOND));
        assertEquals(1, ownMachines().size());
    }

    @Test
    @Order(5)
    void theDataApiReachesTheSleepingDatabaseThroughTheNodeAndWakesIt() {
        String pod = databasePod().path("sandbox_id").asText();
        await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofSeconds(2))
                .until(() -> "asleep".equals(nodeGet("/v1/pods/" + pod).path("state").asText()));
        int samples = datapoints("CPUUtilization").size();
        await().atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofSeconds(5))
                .until(() -> datapoints("CPUUtilization").size() > samples);
        assertEquals("asleep", nodeGet("/v1/pods/" + pod).path("state").asText(), "a minute of metrics woke it");

        JsonNode result = execute("SELECT v FROM marker");

        assertEquals("before-the-cluster", result.at("/records/0/0/stringValue").asText(), result.toString());
        assertEquals("awake", nodeGet("/v1/pods/" + pod).path("state").asText(), "the Data API woke the database");
    }

    @Test
    @Order(6)
    void deletingTheClusterRemovesItsClusterScopedObjectsAndKeepsTheDatabase() throws SQLException {
        kubectlApply(LEFTOVERS);
        assertEquals(Set.of("customresourcedefinitions widgets.e2e.floci.io",
                "validatingwebhookconfigurations node-e2e-refuse", "clusterroles node-e2e-reader",
                "default roles node-e2e-reader"), removable(),
                "what DeleteCluster removes is what the cluster made, not what k3s, Kubernetes or Floci made");

        eks.deleteCluster(request -> request.name(CLUSTER));
        waiter.waitUntilClusterDeleted(request -> request.name(CLUSTER));

        assertEquals(Set.of(), removable(), "DeleteCluster removed the cluster's leftovers");
        assertEquals("running", ownMachines().getFirst().path("state").asText(), "the node outlives the cluster");
        try (Connection connection = connect(); Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT v FROM marker")) {
            assertTrue(rows.next());
            assertEquals("before-the-cluster", rows.getString(1));
        }
        assertEquals("before-the-cluster", execute("SELECT v FROM marker").at("/records/0/0/stringValue").asText());
    }

    @Test
    @Order(7)
    void deletingTheDatabaseKeepsTheNode() {
        rds.deleteDBInstance(request -> request.dbInstanceIdentifier(DATABASE).skipFinalSnapshot(true));

        await().atMost(Duration.ofMinutes(1)).until(() -> standalonePods().isEmpty());
        List<JsonNode> machines = ownMachines();
        assertEquals(1, machines.size(), "the node outlives its databases");
        assertEquals(nodeId, machines.getFirst().path("name").asText());
        assertEquals("running", machines.getFirst().path("state").asText());
        assertTrue(Files.isDirectory(seedDir()), "the seed of the node");
    }

    @Test
    @Order(8)
    void aResetDeletesTheNode() {
        assertEquals(200, post("/_floci/state/reset", ""));

        assertTrue(ownMachines().isEmpty(), "the reset deleted the node");
        assertFalse(Files.exists(seedDir().getParent()), "the files of the node");
    }

    /**
     * {@code KIND NAME}, with {@code default } before a namespaced kind, of each object of the
     * collections that DeleteCluster empties and that k3s, Kubernetes and Floci did not make, read
     * as the node's admin.
     */
    private Set<String> removable() {
        Set<String> names = new TreeSet<>();
        for (String collection : NodeKubeApi.OWNED_COLLECTIONS) {
            Result list = run(List.of(nodeVmScript.toString(), "kubectl", "--name", nodeId, "--", "get", "--raw",
                    collection), Map.of("SMOLVM_SRC", smolvmSrc), null);
            if (list.exit() != 0) {
                LOG.infov("The node serves no {0}: {1}", collection, list.stderr().strip());
                continue;
            }
            String kind = collection.substring(collection.lastIndexOf('/') + 1);
            for (JsonNode item : json(list.stdout(), "/items")) {
                if (!NodeKubeApi.madeForTheNode(kind, item)) {
                    names.add((collection.contains("/namespaces/default/") ? "default " : "") + kind + " "
                            + item.path("metadata").path("name").asText());
                }
            }
        }
        return names;
    }

    /** The messages of a log stream, read with the CloudWatch Logs SDK; none while it does not exist. */
    private List<String> events(String group, String stream) {
        try (CloudWatchLogsClient logs = CloudWatchLogsClient.builder().endpointOverride(floci.endpoint())
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder()).build()) {
            return logs.getLogEvents(request -> request.logGroupName(group).logStreamName(stream).startFromHead(true))
                    .events().stream().map(OutputLogEvent::message).toList();
        } catch (ResourceNotFoundException e) {
            return List.of();
        }
    }

    /** The one-minute Maximum datapoints of the database's AWS/RDS {@code metric} in the last 15 minutes. */
    private JsonNode datapoints(String metric) {
        Instant now = Instant.now();
        return json(aws("cloudwatch", "get-metric-statistics", "--namespace", "AWS/RDS", "--metric-name", metric,
                "--dimensions", "Name=DBInstanceIdentifier,Value=" + DATABASE, "--statistics", "Maximum",
                "--period", "60", "--start-time", now.minus(Duration.ofMinutes(15)).toString(),
                "--end-time", now.plus(Duration.ofMinutes(1)).toString()), "/Datapoints");
    }

    /** The id that names the node's control plane log streams, from the node id. */
    private String controlPlaneStreamId() {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(nodeId.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest).substring(0, 32);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** {@code POST /Execute} of the RDS Data API on the database. */
    private JsonNode execute(String sql) {
        String body;
        try {
            body = JSON.writeValueAsString(Map.of("resourceArn", instanceArn, "secretArn", secretArn,
                    "database", "appdb", "sql", sql));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        HttpRequest request = HttpRequest.newBuilder(floci.endpoint().resolve("/Execute"))
                .timeout(Duration.ofMinutes(2)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        HttpResponse<String> response = send(request);
        assertEquals(200, response.statusCode(), "POST /Execute: " + response.body());
        return json(response.body(), "");
    }

    private int post(String path, String body) {
        HttpRequest request = HttpRequest.newBuilder(floci.endpoint().resolve(path)).timeout(Duration.ofMinutes(3))
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        return send(request).statusCode();
    }

    private HttpResponse<String> send(HttpRequest request) {
        try {
            return http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted", e);
        }
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://localhost:" + endpointPort + "/appdb", USER, PASSWORD);
    }

    /** The node's service, as Floci calls it. */
    private SmolvmNode node() {
        JsonNode machine = ownMachines().getFirst();
        return new SmolvmNode(FlociNode.NAME, URI.create("http://127.0.0.1:" + label(machine, "service-port")),
                NodeToken.file(seedDir().resolve("api-token")), SmolvmNode.Kind.CREATED, List.of());
    }

    private JsonNode nodeGet(String path) {
        SmolvmNode node = node();
        HttpRequest request = HttpRequest.newBuilder(node.baseUrl().resolve(path)).timeout(Duration.ofSeconds(10))
                .header("Authorization", "Bearer " + node.token().read()).GET().build();
        HttpResponse<String> response = send(request);
        assertEquals(200, response.statusCode(), "GET " + path + ": " + response.body());
        return json(response.body(), "");
    }

    /** The one standalone pod on the node: the database's. */
    private JsonNode databasePod() {
        List<JsonNode> pods = standalonePods();
        assertEquals(1, pods.size(), "standalone pods: " + pods);
        return pods.getFirst();
    }

    private List<JsonNode> standalonePods() {
        List<JsonNode> pods = new ArrayList<>();
        for (JsonNode pod : nodeGet("/v1/pods").path("pods")) {
            if (pod.path("standalone").asBoolean()) {
                pods.add(pod);
            }
        }
        return pods;
    }

    private Path seedDir() {
        return data.resolve("node").resolve(nodeId).resolve("seed");
    }

    /** Every {@code node-vm.sh} machine with this Floci's owner label. */
    private List<JsonNode> ownMachines() {
        List<JsonNode> own = new ArrayList<>();
        for (JsonNode machine : json(nodeVm("status", "--json"), "")) {
            if (resourceNamespace.equals(label(machine, NodeDriver.LABEL_OWNER))) {
                own.add(machine);
            }
        }
        return own;
    }

    private static String label(JsonNode machine, String key) {
        return machine.path("labels").path(key).asText("");
    }

    private static boolean ready(JsonNode node) {
        for (JsonNode condition : node.path("status").path("conditions")) {
            if ("Ready".equals(condition.path("type").asText())) {
                return "True".equals(condition.path("status").asText());
            }
        }
        return false;
    }

    /** Pushes {@code image}'s {@code platform} manifest to Floci ECR as {@code repository:tag}. */
    private String push(String image, String platform, String repository, String tag) {
        String uri = json(aws("ecr", "create-repository", "--repository-name", repository), "/repository")
                .path("repositoryUri").asText() + ":" + tag;
        if (run(List.of("docker", "image", "inspect", image), Map.of(), null).exit() != 0) {
            succeed(List.of("docker", "pull", "--platform", platform, image), Map.of(), null);
            pulledImages.add(image);
        }
        succeed(List.of("docker", "tag", image, uri), Map.of(), null);
        pushedImages.add(uri);
        succeed(List.of("docker", "push", uri), Map.of(), null);
        return uri;
    }

    private String aws(String... args) {
        List<String> command = new ArrayList<>(List.of("aws", "--endpoint-url", floci.endpoint().toString(),
                "--region", REGION, "--output", "json"));
        command.addAll(List.of(args));
        return succeed(command, awsEnv("test", "test"), null);
    }

    private String kubectl(String... args) {
        return succeed(kubectlCommand(args), kubectlEnv(), null);
    }

    private void kubectlApply(String yaml) {
        succeed(kubectlCommand("apply", "-f", "-"), kubectlEnv(), yaml);
    }

    private List<String> kubectlCommand(String... args) {
        List<String> command = new ArrayList<>(List.of("kubectl", "--kubeconfig", kubeconfig.toString()));
        command.addAll(List.of(args));
        return command;
    }

    /** kubectl's environment: the IAM user's key, which {@code aws eks get-token} signs with. */
    private Map<String, String> kubectlEnv() {
        return awsEnv(accessKeyId, secretAccessKey);
    }

    private Map<String, String> awsEnv(String keyId, String secret) {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("AWS_ACCESS_KEY_ID", keyId);
        env.put("AWS_SECRET_ACCESS_KEY", secret);
        env.put("AWS_REGION", REGION);
        env.put("AWS_CONFIG_FILE", work.resolve("aws/config").toString());
        env.put("AWS_SHARED_CREDENTIALS_FILE", work.resolve("aws/credentials").toString());
        env.put("AWS_PAGER", "");
        return env;
    }

    private String nodeVm(String... args) {
        List<String> command = new ArrayList<>(List.of(nodeVmScript.toString()));
        command.addAll(List.of(args));
        return succeed(command, Map.of("SMOLVM_SRC", smolvmSrc), null);
    }

    private String succeed(List<String> command, Map<String, String> env, String stdin) {
        Result result = run(command, env, stdin);
        if (result.exit() != 0) {
            throw new AssertionError(String.join(" ", command) + " exited with " + result.exit() + ":\n"
                    + result.stderr() + result.stdout());
        }
        return result.stdout();
    }

    /** Runs {@code command} with {@code env} and without the test's own {@code AWS_*} and {@code KUBECONFIG}. */
    private Result run(List<String> command, Map<String, String> env, String stdin) {
        try {
            Path out = Files.createTempFile(work, "command", ".out");
            Path err = Files.createTempFile(work, "command", ".err");
            try {
                ProcessBuilder builder = new ProcessBuilder(command).redirectOutput(out.toFile())
                        .redirectError(err.toFile());
                builder.environment().keySet().removeIf(name -> name.startsWith("AWS_") || name.equals("KUBECONFIG"));
                builder.environment().putAll(env);
                if (stdin == null) {
                    builder.redirectInput(new File("/dev/null"));
                }
                Process process = builder.start();
                if (stdin != null) {
                    try (OutputStream input = process.getOutputStream()) {
                        input.write(stdin.getBytes(StandardCharsets.UTF_8));
                    }
                }
                if (!process.waitFor(COMMAND_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
                    process.descendants().forEach(ProcessHandle::destroyForcibly);
                    process.destroyForcibly();
                    throw new AssertionError(String.join(" ", command) + " did not finish within "
                            + COMMAND_TIMEOUT.toSeconds() + " s");
                }
                return new Result(process.exitValue(), Files.readString(out), Files.readString(err));
            } finally {
                Files.deleteIfExists(out);
                Files.deleteIfExists(err);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while running " + command, e);
        }
    }

    private static JsonNode json(String text, String pointer) {
        try {
            return JSON.readTree(text).at(pointer);
        } catch (IOException e) {
            throw new AssertionError("Not JSON: " + text, e);
        }
    }

    private static String firstSet(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    private static void tolerate(Runnable cleanup) {
        try {
            cleanup.run();
        } catch (RuntimeException | AssertionError e) {
            LOG.warnv("Cleanup step failed: {0}", e.getMessage());
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }

    private static String sha256(Path file) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        try (InputStream in = new DigestInputStream(Files.newInputStream(file), digest)) {
            in.transferTo(OutputStream.nullOutputStream());
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private record Result(int exit, String stdout, String stderr) {
    }
}
