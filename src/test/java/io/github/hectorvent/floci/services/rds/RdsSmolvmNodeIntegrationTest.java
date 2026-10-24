package io.github.hectorvent.floci.services.rds;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A PostgreSQL instance as a hibernating microVM on a real smolvm-pods node: the test creates a
 * node VM with {@code node-vm.sh}, declares it in Floci's runtime file, and runs Floci as a child
 * process ({@link FlociProcess}) with {@code FLOCI_SERVICES_RDS_SMOLVM_ENABLED}. Through the AWS
 * SDK and JDBC on the RDS endpoint it writes a row, waits until the node puts the database to
 * sleep, connects again, which wakes it, reads the row, and deletes the instance. It needs an
 * Apple silicon Mac of M3 or later, the node image of {@code node-vm.sh build}, and Docker with
 * {@code postgres:16-alpine} or access to Docker Hub, and runs only with
 * {@code FLOCI_RDS_SMOLVM_E2E=1}:
 *
 * <pre>
 * ./mvnw package -DskipTests
 * SMOLVM_SRC=/path/to/smolvm FLOCI_RDS_SMOLVM_E2E=1 ./mvnw test -Dtest=RdsSmolvmNodeIntegrationTest
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "FLOCI_RDS_SMOLVM_E2E", matches = "1")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Timeout(value = 20, unit = TimeUnit.MINUTES)
class RdsSmolvmNodeIntegrationTest {

    private static final Logger LOG = Logger.getLogger(RdsSmolvmNodeIntegrationTest.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String INSTANCE = "smolvm-e2e-pg";
    private static final String RESTORED = "smolvm-e2e-restored";
    private static final String SNAPSHOT = "smolvm-e2e-snap";
    private static final String CLUSTER = "smolvm-e2e-aurora";
    private static final String MEMBER = "smolvm-e2e-aurora-1";
    private static final String MYSQL = "smolvm-e2e-mysql";
    private static final String USER = "admin";
    private static final String PASSWORD = "password123";
    private static final String NEW_PASSWORD = "rotated456";
    /** The node's shortest idle timeout; the node reads conntrack every 15 s on top. */
    private static final String IDLE_TIMEOUT = "10s";
    private static final Duration COMMAND_TIMEOUT = Duration.ofMinutes(6);

    private final Path moduleDir = Path.of("").toAbsolutePath();
    private final String runId = UUID.randomUUID().toString().substring(0, 6);
    private final String nodeName = "floci-rds-e2e-" + runId;
    private final Path work = moduleDir.resolve("target/rds-smolvm-e2e").resolve(runId);
    private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    private Path nodeVmScript;
    private String smolvmSrc;
    private boolean nodeCreated;
    private URI nodeApi;
    private String token;
    private FlociProcess floci;
    private RdsClient rds;
    private int endpointPort;
    private String password = PASSWORD;

    @BeforeAll
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void startNodeAndFloci() throws IOException {
        Files.createDirectories(work);
        Path pods = moduleDir.resolve("tools/smolvm-pods");
        nodeVmScript = pods.resolve("scripts/node-vm.sh");
        smolvmSrc = firstSet(System.getenv("SMOLVM_SRC"), moduleDir.resolveSibling("smolvm").toString());
        assertTrue(Files.isDirectory(Path.of(smolvmSrc)), "No smolvm checkout at " + smolvmSrc + ": export SMOLVM_SRC");
        Path image = Path.of(firstSet(System.getenv("FLOCI_RDS_SMOLVM_E2E_IMAGE"),
                pods.resolve("target/node/node-image.tar").toString()));
        assertTrue(Files.isRegularFile(image), "No node image at " + image + ": run node-vm.sh build");

        int servicePort = freePort();
        LOG.infov("Creating smolvm-pods node {0} with its API on port {1}", nodeName, String.valueOf(servicePort));
        nodeCreated = true;
        nodeVm("create", "--name", nodeName, "--image", image.toString(), "--service-port",
                String.valueOf(servicePort), "--api-port", String.valueOf(freePort()), "--node-ports", "none");
        nodeVm("start", "--name", nodeName);
        nodeApi = URI.create("http://127.0.0.1:" + servicePort);
        token = nodeVm("token", "--name", nodeName).strip();
        Path tokenFile = work.resolve("node-token");
        Files.writeString(tokenFile, token);
        Files.setPosixFilePermissions(tokenFile, PosixFilePermissions.fromString("rw-------"));
        await().atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofSeconds(1)).ignoreExceptions()
                .until(() -> ping() == 200);
        Path runtimeFile = work.resolve("runtime.yaml");
        Files.writeString(runtimeFile, "version: 1\nnodes:\n  rds:\n    url: " + nodeApi
                + "\n    token-file: node-token\n");

        Map<String, String> env = new LinkedHashMap<>();
        env.put("FLOCI_STORAGE_MODE", "memory");
        env.put("FLOCI_DOCKER_RESOURCE_NAMESPACE", "rds-e2e-" + runId);
        env.put("FLOCI_SERVICES_UI_ENABLED", "false");
        env.put("FLOCI_RUNTIME_CONFIG_FILE", runtimeFile.toString());
        env.put("FLOCI_RUNTIME_DNS_LISTEN", "127.0.0.1:" + freePort());
        env.put("FLOCI_SERVICES_RDS_SMOLVM_ENABLED", "true");
        env.put("FLOCI_SERVICES_RDS_SMOLVM_IDLE_TIMEOUT", IDLE_TIMEOUT);
        floci = FlociProcess.of(moduleDir, work, freePort());
        floci.start(env);
        rds = RdsClient.builder().endpointOverride(floci.endpoint()).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder().socketTimeout(Duration.ofMinutes(5)))
                .build();
    }

    @AfterAll
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void removeEverythingTheTestCreated() {
        if (rds != null) {
            for (String id : List.of(INSTANCE, RESTORED, MYSQL, MEMBER)) {
                tolerate(() -> rds.deleteDBInstance(request -> request.dbInstanceIdentifier(id).skipFinalSnapshot(true)));
            }
            tolerate(() -> rds.deleteDBCluster(request -> request.dbClusterIdentifier(CLUSTER).skipFinalSnapshot(true)));
            rds.close();
        }
        if (floci != null) {
            tolerate(floci::stop);
        }
        if (nodeCreated) {
            tolerate(() -> nodeVm("delete", "--name", nodeName));
        }
    }

    @Test
    @Order(1)
    void aPostgresInstanceRunsAsAMicroVmOnTheNode() throws SQLException {
        Instant started = Instant.now();
        DBInstance created = rds.createDBInstance(request -> request.dbInstanceIdentifier(INSTANCE)
                .engine("postgres").masterUsername(USER).masterUserPassword(PASSWORD).dbName("appdb")
                .dbInstanceClass("db.t3.micro").allocatedStorage(20)).dbInstance();
        endpointPort = created.endpoint().port();
        LOG.infov("CreateDBInstance answered after {0} ms", Duration.between(started, Instant.now()).toMillis());
        assertEquals("available", describe().dbInstanceStatus());

        JsonNode pod = databasePod();
        assertEquals("awake", pod.path("state").asText(), pod.toString());
        assertTrue(pod.path("hibernate").asBoolean(), pod.toString());
        JsonNode inspect = get("/docker/containers/" + pod.path("sandbox_id").asText() + "/json");
        assertEquals("io.containerd.smolvm-pods.v2", inspect.path("HostConfig").path("Runtime").asText());

        try (Connection connection = connect(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE marker (v text)");
            statement.execute("INSERT INTO marker VALUES ('before-sleep')");
        }
    }

    @Test
    @Order(2)
    void theIdleDatabaseSleepsAndAConnectionWakesIt() throws SQLException {
        String pod = databasePod().path("sandbox_id").asText();
        Instant idleSince = Instant.now();
        await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofSeconds(2))
                .until(() -> "asleep".equals(get("/v1/pods/" + pod).path("state").asText()));
        LOG.infov("The database slept {0} s after its last connection",
                Duration.between(idleSince, Instant.now()).toSeconds());
        assertEquals("available", describe().dbInstanceStatus(), "a describe of the sleeping database");
        assertEquals("asleep", get("/v1/pods/" + pod).path("state").asText(), "the describe woke the database");

        long start = System.nanoTime();
        try (Connection connection = connect()) {
            long connected = (System.nanoTime() - start) / 1_000_000;
            try (Statement statement = connection.createStatement();
                 ResultSet rows = statement.executeQuery("SELECT v FROM marker")) {
                assertTrue(rows.next(), "the row written before the sleep");
                assertEquals("before-sleep", rows.getString(1));
            }
            long read = (System.nanoTime() - start) / 1_000_000;
            JsonNode woken = get("/v1/pods/" + pod);
            LOG.infov("The first connection to the sleeping database took {0} ms, and its first query answered "
                    + "after {1} ms; the node''s wake took {2} ms", connected, read, woken.path("last_wake_ms").asText());
            assertEquals("awake", woken.path("state").asText());
        }
        JsonNode events = get("/v1/events?pod=" + pod);
        List<String> wakes = new ArrayList<>();
        for (JsonNode event : events.path("events")) {
            if ("waking".equals(event.path("from").asText()) && "awake".equals(event.path("to").asText())) {
                wakes.add(event.path("duration_ms").asText());
            }
        }
        LOG.infov("Wakes of the database, in ms: {0}", wakes);
        assertFalse(wakes.isEmpty(), "no wake in " + events);
    }

    @Test
    @Order(3)
    void passwordRotationSnapshotsAndLogsWorkOnTheNode() throws SQLException {
        rds.modifyDBInstance(request -> request.dbInstanceIdentifier(INSTANCE).masterUserPassword(NEW_PASSWORD)
                .applyImmediately(true));
        password = NEW_PASSWORD;
        assertEquals("before-sleep", marker(endpointPort));

        rds.createDBSnapshot(request -> request.dbInstanceIdentifier(INSTANCE).dbSnapshotIdentifier(SNAPSHOT));
        await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofSeconds(2)).until(() -> "available".equals(
                rds.describeDBSnapshots(request -> request.dbSnapshotIdentifier(SNAPSHOT)).dbSnapshots().getFirst()
                        .status()));
        DBInstance restored = rds.restoreDBInstanceFromDBSnapshot(request -> request
                .dbInstanceIdentifier(RESTORED).dbSnapshotIdentifier(SNAPSHOT)).dbInstance();
        await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofSeconds(2)).until(() -> "available".equals(
                rds.describeDBInstances(request -> request.dbInstanceIdentifier(RESTORED)).dbInstances().getFirst()
                        .dbInstanceStatus()));
        assertEquals("before-sleep", marker(restored.endpoint().port()));
        assertEquals(2, standalonePods().size(), "the restored database's microVM");

        List<String> logs = rds.describeDBLogFiles(request -> request.dbInstanceIdentifier(INSTANCE))
                .describeDBLogFiles().stream().map(file -> file.logFileName()).toList();
        assertFalse(logs.isEmpty(), "log files");
        StringBuilder data = new StringBuilder();
        for (String name : logs) {
            data.append(rds.downloadDBLogFilePortion(request -> request.dbInstanceIdentifier(INSTANCE)
                    .logFileName(name).marker("0")).logFileData());
        }
        assertTrue(data.toString().contains("ready to accept connections"), data.toString());
    }

    @Test
    @Order(4)
    void aMySqlInstanceRunsOnTheNodeWithItsMasterGrants() throws SQLException {
        int port = rds.createDBInstance(request -> request.dbInstanceIdentifier(MYSQL).engine("mysql")
                .masterUsername(USER).masterUserPassword(PASSWORD).dbName("appdb")
                .dbInstanceClass("db.t3.micro").allocatedStorage(20)).dbInstance().endpoint().port();
        String url = "jdbc:mysql://localhost:" + port + "/appdb?allowPublicKeyRetrieval=true&useSSL=false";
        // CreateDBInstance returns while MySQL initializes its data directory, as without a node.
        await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofSeconds(2)).ignoreExceptions()
                .until(() -> {
                    try (Connection connection = DriverManager.getConnection(url, USER, PASSWORD)) {
                        return connection.isValid(5);
                    }
                });

        try (Connection connection = DriverManager.getConnection(url, USER, PASSWORD);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE granted");
            try (ResultSet rows = statement.executeQuery("SELECT 41 + 1")) {
                assertTrue(rows.next());
                assertEquals(42, rows.getInt(1));
            }
        }
        assertEquals(3, standalonePods().size(), "the MySQL database's microVM");
    }

    @Test
    @Order(5)
    void anAuroraClusterRunsOnTheNode() throws SQLException {
        int port = rds.createDBCluster(request -> request.dbClusterIdentifier(CLUSTER).engine("aurora-postgresql")
                .masterUsername(USER).masterUserPassword(PASSWORD).databaseName("appdb")).dbCluster().port();
        rds.createDBInstance(request -> request.dbInstanceIdentifier(MEMBER).dbClusterIdentifier(CLUSTER)
                .engine("aurora-postgresql").dbInstanceClass("db.r6g.large"));

        try (Connection connection = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + port + "/appdb", USER, PASSWORD);
             Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT 41 + 1")) {
            assertTrue(rows.next());
            assertEquals(42, rows.getInt(1));
        }
        assertEquals(4, standalonePods().size(), "the cluster's microVM");
    }

    @Test
    @Order(6)
    void aDeleteRemovesTheDatabasesFromTheNode() {
        for (String id : List.of(INSTANCE, RESTORED, MYSQL, MEMBER)) {
            rds.deleteDBInstance(request -> request.dbInstanceIdentifier(id).skipFinalSnapshot(true));
        }
        rds.deleteDBCluster(request -> request.dbClusterIdentifier(CLUSTER).skipFinalSnapshot(true));

        await().atMost(Duration.ofMinutes(1)).until(() -> standalonePods().isEmpty());
        assertThat(standalonePods(), empty());
    }

    private DBInstance describe() {
        return rds.describeDBInstances(request -> request.dbInstanceIdentifier(INSTANCE)).dbInstances().getFirst();
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://localhost:" + endpointPort + "/appdb", USER, password);
    }

    /** The row of {@code marker} through the endpoint on {@code port}. */
    private String marker(int port) throws SQLException {
        try (Connection connection = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + port + "/appdb", USER, password);
             Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT v FROM marker")) {
            assertTrue(rows.next(), "the marker row");
            return rows.getString(1);
        }
    }

    /** The one standalone pod on the node: the database's. */
    private JsonNode databasePod() {
        List<JsonNode> pods = standalonePods();
        assertEquals(1, pods.size(), "standalone pods: " + pods);
        return pods.getFirst();
    }

    private List<JsonNode> standalonePods() {
        List<JsonNode> pods = new ArrayList<>();
        for (JsonNode pod : get("/v1/pods").path("pods")) {
            if (pod.path("standalone").asBoolean()) {
                pods.add(pod);
            }
        }
        return pods;
    }

    /** The status of dockerd's ping through the node's Docker proxy. */
    private int ping() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(nodeApi.resolve("/docker/_ping")).timeout(Duration.ofSeconds(5))
                .header("Authorization", "Bearer " + token).GET().build();
        return http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    private JsonNode get(String path) {
        HttpRequest request = HttpRequest.newBuilder(nodeApi.resolve(path)).timeout(Duration.ofSeconds(10))
                .header("Authorization", "Bearer " + token).GET().build();
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode(), "GET " + path + ": " + response.body());
            return JSON.readTree(response.body());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted", e);
        }
    }

    private String nodeVm(String... args) {
        List<String> command = new ArrayList<>(List.of(nodeVmScript.toString()));
        command.addAll(List.of(args));
        try {
            Path out = Files.createTempFile(work, "node-vm", ".out");
            Path err = Files.createTempFile(work, "node-vm", ".err");
            ProcessBuilder builder = new ProcessBuilder(command).redirectOutput(out.toFile()).redirectError(err.toFile())
                    .redirectInput(new File("/dev/null"));
            builder.environment().put("SMOLVM_SRC", smolvmSrc);
            Process process = builder.start();
            if (!process.waitFor(COMMAND_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new AssertionError(String.join(" ", command) + " did not finish in time");
            }
            String stdout = Files.readString(out);
            if (process.exitValue() != 0) {
                throw new AssertionError(String.join(" ", command) + " exited with " + process.exitValue() + ":\n"
                        + Files.readString(err) + stdout);
            }
            return stdout;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while running " + command, e);
        }
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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
}
