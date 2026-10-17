package io.github.hectorvent.floci.runtime.ondemand;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.services.rds.container.RdsRuntimeTestSupport;
import io.github.hectorvent.floci.testing.RdsRuntimeTestProfile;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Supplies real AWS services to the shared Docker/native application graph fixture. */
@QuarkusTest
@TestProfile(NativeApplicationGraphIntegrationTest.Profile.class)
@EnabledIfEnvironmentVariable(named = "FLOCI_NATIVE_GRAPH_TEST", matches = "true")
class NativeApplicationGraphIntegrationTest extends RdsRuntimeTestSupport {
    private static final String USER = "master";
    private static final String PASSWORD = "GraphFixture123!";
    private static final String DATABASE = "appdb";

    public static final class Profile extends RdsRuntimeTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> overrides = super.getConfigOverrides();
            Path directory = fixtureDirectory();
            try {
                ObjectMapper json = new ObjectMapper();
                JsonNode fixture = json.readTree(directory.resolve("fixture.json").toFile());
                Map<String, Object> worker = Map.of("idle-timeout-seconds", 3,
                        "startup-timeout-seconds", 240, "queues", List.of(queueName(fixture)),
                        "runtime", Map.of("type", "kubernetes", "namespace", fixture.path("namespace").asText("default"),
                                "deployment", fixture.path("workerDeployment").asText(), "replicas", 1));
                Path definitions = directory.resolve("workloads.json");
                json.writeValue(definitions.toFile(), Map.of("workloads", Map.of("graph-worker", worker)));
                overrides.put("floci.on-demand.enabled", "true");
                overrides.put("floci.on-demand.config-file", definitions.toString());
                overrides.put("floci.on-demand.gateway-port", "0");
                overrides.put("floci.on-demand.reconcile-interval-millis", "100");
                overrides.put("floci.on-demand.pod-controller-url", fixture.path("controllerURL").asText());
                overrides.put("floci.on-demand.pod-controller-token", fixture.path("controllerToken").asText());
            } catch (IOException exception) {
                throw new IllegalStateException("Cannot prepare the application graph fixture", exception);
            }
            overrides.put("quarkus.http.host", "0.0.0.0");
            overrides.put("quarkus.http.test-port", "0");
            overrides.put("quarkus.http.test-ssl-port", "0");
            overrides.put("quarkus.test.hang-detection-timeout", "21m");
            overrides.put("floci.services.rds.microsandbox-memory-mib", "768");
            overrides.put("floci.microsandbox.max-active-memory-mib", "1536");
            return overrides;
        }
    }

    @Inject
    ObjectMapper mapper;
    @Inject
    OnDemandService workloads;
    @TestHTTPResource
    URI endpoint;

    @Test
    @Timeout(value = 20, unit = TimeUnit.MINUTES)
    void httpAndSqsWorkloadsShareAuroraAcrossSleepAndResume() throws Exception {
        Path directory = fixtureDirectory();
        JsonNode fixture = mapper.readTree(directory.resolve("fixture.json").toFile());
        String id = "graph-" + fixture.path("owner").asText();
        StaticCredentialsProvider credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
        try (RdsClient rds = RdsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .credentialsProvider(credentials).httpClientBuilder(UrlConnectionHttpClient.builder()
                        .socketTimeout(Duration.ofMinutes(4))).build();
             SqsClient sqs = SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                     .credentialsProvider(credentials).httpClientBuilder(UrlConnectionHttpClient.builder()).build()) {
            String queue = sqs.createQueue(request -> request.queueName(queueName(fixture))).queueUrl();
            String localQueue = endpoint.resolve(URI.create(queue).getPath()).toString();
            try {
                DBCluster cluster = rds.createDBCluster(request -> request.dbClusterIdentifier(id)
                        .engine("aurora-postgresql").masterUsername(USER).masterUserPassword(PASSWORD)
                        .databaseName(DATABASE).serverlessV2ScalingConfiguration(
                                scaling -> scaling.minCapacity(0.0).maxCapacity(2.0).secondsUntilAutoPause(300))).dbCluster();
                rds.createDBInstance(request -> request.dbInstanceIdentifier(id + "-member")
                        .dbClusterIdentifier(id).engine("aurora-postgresql").dbInstanceClass("db.serverless"));
                String started;
                try (Connection connection = postgres(cluster.port(), DATABASE, USER, PASSWORD);
                     Statement statement = connection.createStatement()) {
                    statement.execute("CREATE TABLE graph_writes(id text PRIMARY KEY, source text NOT NULL, marker text NOT NULL)");
                    try (ResultSet rows = statement.executeQuery("SELECT pg_postmaster_start_time()::text")) {
                        assertTrue(rows.next());
                        started = rows.getString(1);
                    }
                }
                pause(cluster.dbClusterArn());
                Map<String, Object> ready = new LinkedHashMap<>();
                ready.put("localEndpoint", endpoint.toString());
                ready.put("endpoint", "http://" + fixture.path("hostIP").asText() + ":" + endpoint.getPort());
                ready.put("queueURL", localQueue);
                ready.put("queueName", queueName(fixture));
                ready.put("dbPort", cluster.port());
                ready.put("dbUser", USER);
                ready.put("dbPassword", PASSWORD);
                ready.put("dbName", DATABASE);
                ready.put("runtimeId", cluster.dbClusterArn());
                write(directory.resolve("ready.json"), ready);
                AtomicInteger sequence = new AtomicInteger(1);
                await().pollInterval(Duration.ofMillis(100)).atMost(Duration.ofMinutes(18)).until(() -> {
                    Path command = directory.resolve("command-" + sequence.get() + ".json");
                    if (Files.isRegularFile(command)) {
                        JsonNode request = mapper.readTree(command.toFile());
                        Map<String, Object> receipt = command(request, sqs, localQueue, cluster.dbClusterArn());
                        write(directory.resolve("receipt-" + sequence.getAndIncrement() + ".json"), receipt);
                    }
                    return Files.isRegularFile(directory.resolve("result.json"));
                });
                JsonNode result = mapper.readTree(directory.resolve("result.json").toFile());
                assertTrue(result.path("success").asBoolean(), result.path("error").asText());
                try (Connection connection = postgres(cluster.port(), DATABASE, USER, PASSWORD);
                     Statement statement = connection.createStatement()) {
                    try (ResultSet rows = statement.executeQuery("SELECT pg_postmaster_start_time()::text")) {
                        assertTrue(rows.next());
                        assertEquals(started, rows.getString(1), "Aurora must resume the same PostgreSQL process");
                    }
                    Map<String, String> writes = new LinkedHashMap<>();
                    try (ResultSet rows = statement.executeQuery("SELECT id, source FROM graph_writes")) {
                        while (rows.next()) {
                            writes.put(rows.getString(1), rows.getString(2));
                        }
                    }
                    for (String backend : List.of("docker", "native")) {
                        assertEquals("http", writes.get(backend + "-http-1"));
                        assertEquals("sqs", writes.get(backend + "-sqs-1"));
                    }
                    assertEquals("http", writes.get("native-http-2"));
                    assertEquals("sqs", writes.get("native-sqs-2"));
                }
            } finally {
                workloads.stop(null);
                sqs.deleteQueue(request -> request.queueUrl(localQueue));
            }
        }
    }

    private Map<String, Object> command(JsonNode request, SqsClient sqs, String queue, String runtimeId) {
        return switch (request.path("operation").asText()) {
            case "enqueue" -> Map.of("success", true, "messageId", sqs.sendMessage(
                    message -> message.queueUrl(queue).messageBody(request.path("body").asText())).messageId());
            case "pauseAurora" -> {
                pause(runtimeId);
                yield Map.of("success", true, "state", "SLEEPING");
            }
            case "status" -> Map.of("success", true, "workerState", workloads.coordinator().state("graph-worker").name());
            default -> throw new IllegalArgumentException("Unknown application graph fixture command");
        };
    }

    private void pause(String runtimeId) {
        await().pollInterval(Duration.ofMillis(200)).atMost(Duration.ofSeconds(30)).until(() -> pauseIfIdle(runtimeId));
        assertBackendPaused(runtimeId);
    }

    private void write(Path destination, Object value) throws IOException {
        Path pending = destination.resolveSibling(destination.getFileName() + ".pending");
        mapper.writeValue(pending.toFile(), value);
        Files.move(pending, destination, StandardCopyOption.ATOMIC_MOVE);
    }

    private static String queueName(JsonNode fixture) {
        return "graph-" + fixture.path("owner").asText();
    }

    private static Path fixtureDirectory() {
        return Path.of(System.getenv("FLOCI_NATIVE_GRAPH_DIRECTORY"));
    }
}
