package io.github.hectorvent.floci.services.rds;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.runtime.node.FakeDockerNode;
import io.github.hectorvent.floci.services.cloudwatch.metrics.CloudWatchMetricsService;
import io.github.hectorvent.floci.services.cloudwatch.metrics.model.Dimension;
import io.github.hectorvent.floci.services.rds.model.DbInstance;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static io.github.hectorvent.floci.testing.RdsRequests.rdsClient;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RDS with its databases on a smolvm-pods node, against a fake node service
 * ({@link FakeDockerNode}) that the runtime file declares: the database containers are created
 * through the node's Docker proxy with the node's runtime and annotations, their data in a named
 * volume on the node; a describe reads a sleeping database's state from the node's pod record,
 * which does not wake it; and a client connection to the endpoint reaches the database through
 * the node's tunnel.
 */
@QuarkusTest
@TestProfile(RdsSmolvmFakeNodeIntegrationTest.SmolvmNodeProfile.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RdsSmolvmFakeNodeIntegrationTest {

    private static final String TOKEN = "fake-node-token-0123456789";
    private static final Path DIR = Path.of("target", "rds-smolvm-fake-node-it").toAbsolutePath();
    private static final int PORT = freePort();
    private static final String POSTGRES = "smolvm-pg";
    private static final String SQLSERVER = "smolvm-mssql";

    private static FakeDockerNode node;

    @Inject
    RdsService rdsService;

    @Inject
    RdsMetricsPublisher metricsPublisher;

    @Inject
    CloudWatchMetricsService metricsService;

    @BeforeAll
    static void startNode() throws IOException {
        node = new FakeDockerNode(TOKEN, PORT);
    }

    @AfterAll
    static void stopNode() {
        node.close();
    }

    @Test
    @Order(1)
    void aDatabaseContainerIsCreatedOnTheNodeAsAHibernatingMicroVm() {
        try (RdsClient rds = rdsClient("us-east-1", "000000000000")) {
            DBInstance created = rds.createDBInstance(request -> request.dbInstanceIdentifier(POSTGRES)
                    .engine("postgres").masterUsername("admin").masterUserPassword("password123")
                    .dbName("appdb").dbInstanceClass("db.t3.micro").allocatedStorage(20)).dbInstance();
            assertEquals(POSTGRES, created.dbInstanceIdentifier());
        }
        DbInstance instance = rdsService.getDbInstance(POSTGRES);
        ObjectNode container = node.containers().get(instance.getContainerId());

        JsonNode hostConfig = container.path("HostConfig");
        assertEquals("io.containerd.smolvm-pods.v2", hostConfig.path("Runtime").asText());
        assertEquals("true", hostConfig.path("Annotations").path("smolvm-pods.floci.io/hibernate").asText());
        assertEquals("2m", hostConfig.path("Annotations").path("smolvm-pods.floci.io/idle-timeout").asText());
        JsonNode mount = hostConfig.path("Mounts").path(0);
        assertEquals("volume", mount.path("Type").asText());
        assertEquals("/var/lib/postgresql/data", mount.path("Target").asText());
        assertTrue(node.volumes().containsKey(mount.path("Source").asText()), "the volume is on the node");
        assertTrue(hostConfig.path("PortBindings").isMissingNode() || hostConfig.path("PortBindings").isEmpty()
                || hostConfig.path("PortBindings").isNull(), "a port is published: " + hostConfig);
        assertThat(container.path("Env").toString(), is(not("")));
        assertTrue(container.path("Env").toString().contains("POSTGRES_DB=appdb"), container.path("Env").toString());
        assertEquals(container.path("Ip").asText(), instance.getContainerHost());
        assertEquals(5432, instance.getContainerPort());

        List<String> paths = node.requests().stream().map(FakeDockerNode.Request::path).toList();
        assertThat(paths, hasItem("/docker/containers/" + instance.getContainerId() + "/exec"));
        assertThat(paths, everyItem(not(startsWith("/docker/containers/" + instance.getContainerId() + "/pause"))));
        assertThat(node.requests().stream().map(r -> r.headers().get("authorization")).toList(),
                everyItem(is("Bearer " + TOKEN)));
    }

    @Test
    @Order(2)
    void aDescribeOfASleepingDatabaseAsksTheNodeAndDoesNotWakeIt() {
        DbInstance instance = rdsService.getDbInstance(POSTGRES);
        node.podState(instance.getContainerId(), "asleep");
        int before = node.requests().size();

        try (RdsClient rds = rdsClient("us-east-1", "000000000000")) {
            DBInstance described = rds.describeDBInstances(request -> request.dbInstanceIdentifier(POSTGRES))
                    .dbInstances().getFirst();
            assertEquals("available", described.dbInstanceStatus());
        }

        List<FakeDockerNode.Request> during = node.requests().subList(before, node.requests().size());
        assertThat(during.stream().map(FakeDockerNode.Request::path).toList(),
                hasItem("/v1/pods/" + instance.getContainerId()));
        assertThat(during.stream().filter(r -> r.path().contains("/exec") || r.path().equals("/v1/tunnel")).toList(),
                empty());
    }

    @Test
    @Order(3)
    void aClientConnectionReachesTheDatabaseThroughTheNodesTunnel() throws Exception {
        try (ServerSocket database = new ServerSocket(0, 5, InetAddress.getLoopbackAddress());
             RdsClient rds = rdsClient("us-east-1", "000000000000")) {
            Thread.ofVirtual().start(() -> echo(database));
            DBInstance created = rds.createDBInstance(request -> request.dbInstanceIdentifier(SQLSERVER)
                    .engine("sqlserver-se").masterUsername("admin").masterUserPassword("Password123!")
                    .dbInstanceClass("db.t3.micro").allocatedStorage(20)).dbInstance();
            DbInstance instance = rdsService.getDbInstance(SQLSERVER);
            node.mapTarget(instance.getContainerHost(), 1433, database.getLocalPort());

            try (Socket client = new Socket("localhost", created.endpoint().port())) {
                client.setSoTimeout(10_000);
                OutputStream output = client.getOutputStream();
                output.write("prelogin".getBytes(StandardCharsets.ISO_8859_1));
                output.flush();
                byte[] answer = client.getInputStream().readNBytes(8);
                assertEquals("PRELOGIN", new String(answer, StandardCharsets.ISO_8859_1));
            }
            FakeDockerNode.Request tunnel = node.requests().stream()
                    .filter(r -> r.path().equals("/v1/tunnel")).findFirst().orElseThrow();
            assertEquals("/v1/tunnel?to=" + instance.getContainerHost() + "%3A1433", tunnel.target());
            assertEquals("Upgrade", tunnel.headers().get("connection"));
        }
    }

    @Test
    @Order(4)
    void anExportedLogShipsFromTheNodeAndFollowsAgainAfterADrop() throws Exception {
        String container = rdsService.getDbInstance(POSTGRES).getContainerId();
        try (RdsClient rds = rdsClient("us-east-1", "000000000000")) {
            rds.modifyDBInstance(request -> request.dbInstanceIdentifier(POSTGRES).cloudwatchLogsExportConfiguration(
                    exports -> exports.enableLogTypes("postgresql")));
        }
        await().atMost(Duration.ofSeconds(10)).until(() -> follows(container).stream()
                .anyMatch(target -> target.contains("since=")));
        Thread.sleep(500);
        Instant first = Instant.now();
        node.logLine(container, first, "LOG:  database system is ready to accept connections");
        await().atMost(Duration.ofSeconds(10)).until(() -> shipped().contains(
                "LOG:  database system is ready to accept connections"));

        int before = follows(container).size();
        node.dropLogFollows();
        await().atMost(Duration.ofSeconds(10)).until(() -> follows(container).size() > before
                && node.logFollows(container) > 0);
        node.logLine(container, first, "LOG:  database system is ready to accept connections");
        node.logLine(container, first.plusSeconds(1), "LOG:  checkpoint starting: time");
        await().atMost(Duration.ofSeconds(10)).until(() -> shipped().contains("LOG:  checkpoint starting: time"));

        assertEquals(List.of("LOG:  database system is ready to accept connections", "LOG:  checkpoint starting: time"),
                shipped());
        assertThat(follows(container).getLast(), containsString("since=" + first.getEpochSecond()));
    }

    @Test
    @Order(5)
    void aMinuteOfMetricsComesFromTheNodeWithoutWakingTheDatabase() {
        DbInstance instance = rdsService.getDbInstance(POSTGRES);
        int before = node.requests().size();
        Instant now = Instant.now();

        metricsPublisher.publish(now);

        Map<String, Double> values = new TreeMap<>();
        for (String metric : List.of("DatabaseConnections", "CPUUtilization", "FreeableMemory")) {
            List<CloudWatchMetricsService.Datapoint> points = metricsService.getMetricStatistics("AWS/RDS", metric,
                    List.of(new Dimension("DBInstanceIdentifier", POSTGRES)), now.minusSeconds(600), now.plusSeconds(60),
                    60, List.of("Maximum"), null, "us-east-1");
            values.put(metric, points.getFirst().maximum());
        }
        assertEquals(Map.of("CPUUtilization", 10.0, "DatabaseConnections", 0.0, "FreeableMemory",
                (double) (768L << 20)), values);
        assertEquals(List.of(), metricsService.listMetrics("AWS/RDS", "FreeStorageSpace", List.of(), "us-east-1"),
                "the node's microVM writes to its own copy of the volume");
        List<String> paths = node.requests().subList(before, node.requests().size()).stream()
                .map(FakeDockerNode.Request::path).toList();
        assertThat(paths, hasItem("/docker/containers/" + instance.getContainerId() + "/stats"));
        assertThat(paths.stream().filter(path -> path.contains("/exec") || path.equals("/v1/tunnel")
                || path.endsWith("/wake")).toList(), empty());
    }

    private static List<String> follows(String container) {
        return node.requests().stream()
                .filter(r -> r.path().equals("/docker/containers/" + container + "/logs")
                        && r.target().contains("follow="))
                .map(FakeDockerNode.Request::target).toList();
    }

    private static List<String> shipped() {
        try (CloudWatchLogsClient logs = CloudWatchLogsClient.builder()
                .endpointOverride(URI.create("http://localhost:" + RestAssured.port))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("000000000000", "test")))
                .build()) {
            return logs.getLogEvents(request -> request.logGroupName("/aws/rds/instance/" + POSTGRES + "/postgresql")
                    .logStreamName(POSTGRES).startFromHead(true)).events().stream()
                    .map(OutputLogEvent::message).toList();
        } catch (ResourceNotFoundException e) {
            return List.of();
        }
    }

    @Test
    @Order(9)
    void aDeleteRemovesTheContainersFromTheNode() {
        String postgres = rdsService.getDbInstance(POSTGRES).getContainerId();
        try (RdsClient rds = rdsClient("us-east-1", "000000000000")) {
            for (String id : List.of(POSTGRES, SQLSERVER)) {
                rds.deleteDBInstance(request -> request.dbInstanceIdentifier(id).skipFinalSnapshot(true));
            }
        }

        assertThat(node.containers().keySet(), empty());
        assertThat(node.requests().stream().map(r -> r.method() + " " + r.path()).toList(),
                hasItem("DELETE /docker/containers/" + postgres));
    }

    private static void echo(ServerSocket server) {
        try (Socket socket = server.accept()) {
            InputStream input = socket.getInputStream();
            byte[] question = input.readNBytes(8);
            socket.getOutputStream().write(new String(question, StandardCharsets.ISO_8859_1).toUpperCase()
                    .getBytes(StandardCharsets.ISO_8859_1));
            socket.getOutputStream().flush();
            input.read();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class SmolvmNodeProfile implements QuarkusTestProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            try {
                Files.createDirectories(DIR);
                Path token = DIR.resolve("node-token");
                Files.writeString(token, TOKEN);
                Files.setPosixFilePermissions(token, PosixFilePermissions.fromString("rw-------"));
                Path file = DIR.resolve("runtime.yaml");
                Files.writeString(file, "version: 1\nnodes:\n  fake:\n    url: http://127.0.0.1:" + PORT
                        + "\n    token-file: node-token\n");
                return Map.of(
                        "floci.runtime.config-file", file.toString(),
                        "floci.services.rds.smolvm.enabled", "true",
                        "floci.services.rds.smolvm.idle-timeout", "2m");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
