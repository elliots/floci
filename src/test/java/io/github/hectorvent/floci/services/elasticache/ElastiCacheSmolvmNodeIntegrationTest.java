package io.github.hectorvent.floci.services.elasticache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.services.eks.FlociProcess;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.elasticache.ElastiCacheClient;
import software.amazon.awssdk.services.elasticache.model.ReplicationGroup;
import software.amazon.awssdk.services.elasticache.model.Tag;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Real node: SDK provisioning, tunneled Redis, DNS and a sleep/wake round trip. */
@EnabledIfEnvironmentVariable(named = "FLOCI_ELASTICACHE_SMOLVM_E2E", matches = "1")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(value = 15, unit = TimeUnit.MINUTES)
class ElastiCacheSmolvmNodeIntegrationTest {
    private static final Logger LOG = Logger.getLogger(ElastiCacheSmolvmNodeIntegrationTest.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Duration COMMAND_TIMEOUT = Duration.ofMinutes(6);
    private final Path moduleDir = Path.of("").toAbsolutePath();
    private final String runId = UUID.randomUUID().toString().substring(0, 6);
    private final String nodeName = "floci-cache-e2e-" + runId;
    private final Path work = moduleDir.resolve("target/elasticache-smolvm-e2e").resolve(runId);
    private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    private Path nodeVmScript;
    private String smolvmSrc;
    private boolean nodeCreated;
    private URI nodeApi;
    private String token;
    private FlociProcess floci;
    private ElastiCacheClient caches;

    @BeforeAll
    void startNodeAndFloci() throws IOException {
        Files.createDirectories(work);
        Path pods = moduleDir.resolve("tools/smolvm-pods");
        nodeVmScript = pods.resolve("scripts/node-vm.sh");
        smolvmSrc = firstSet(System.getenv("SMOLVM_SRC"), moduleDir.resolveSibling("smolvm").toString());
        Path image = pods.resolve("target/node/node-image.tar");
        assertTrue(Files.isRegularFile(image), "Run node-vm.sh build first");
        int servicePort = freePort();
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
        Path runtime = work.resolve("runtime.yaml");
        Files.writeString(runtime, "version: 1\nnodes:\n  cache:\n    url: " + nodeApi
                + "\n    token-file: node-token\n");
        Map<String, String> env = new LinkedHashMap<>();
        env.put("FLOCI_STORAGE_MODE", "memory");
        env.put("FLOCI_DOCKER_RESOURCE_NAMESPACE", "cache-e2e-" + runId);
        env.put("FLOCI_SERVICES_UI_ENABLED", "false");
        env.put("FLOCI_RUNTIME_CONFIG_FILE", runtime.toString());
        env.put("FLOCI_RUNTIME_DNS_LISTEN", "127.0.0.1:" + freePort());
        env.put("FLOCI_HOSTNAME", "localhost.floci.io");
        env.put("FLOCI_SERVICES_ELASTICACHE_SMOLVM_ENABLED", "true");
        env.put("FLOCI_SERVICES_ELASTICACHE_SMOLVM_IDLE_TIMEOUT", "10s");
        floci = FlociProcess.of(moduleDir, work, freePort());
        floci.start(env);
        caches = ElastiCacheClient.builder().endpointOverride(floci.endpoint()).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder().socketTimeout(Duration.ofMinutes(5))).build();
    }

    @AfterAll
    void cleanup() {
        if (caches != null) {
            tolerate(() -> caches.deleteReplicationGroup(request -> request.replicationGroupId("node-cache")));
            caches.close();
        }
        if (floci != null) {
            tolerate(floci::stop);
        }
        if (nodeCreated) {
            tolerate(() -> nodeVm("delete", "--name", nodeName));
        }
    }

    @Test
    void aCacheSleepsAndWakesWithItsDataAndResolvesFromTheNode() throws IOException {
        ReplicationGroup group = caches.createReplicationGroup(request -> request.replicationGroupId("node-cache")
                .replicationGroupDescription("Node cache").engine("valkey").cacheNodeType("cache.t3.micro")
                .numCacheClusters(1).tags(Tag.builder().key("floci:tls-hostnames").value("cache.example.test").build()))
                .replicationGroup();
        int port = group.nodeGroups().getFirst().primaryEndpoint().port();
        assertEquals("+OK\r\n", command(port, "SET marker retained", 5));
        JsonNode pod = get("/v1/pods").path("pods").get(0);
        String podId = pod.path("sandbox_id").asText();
        assertTrue(!podId.isBlank(), pod.toString());
        await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofSeconds(2))
                .until(() -> "asleep".equals(get("/v1/pods/" + podId).path("state").asText()));
        assertEquals("$8\r\nretained\r\n", command(port, "GET marker", 14));
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(100))
                .until(() -> "awake".equals(get("/v1/pods/" + podId).path("state").asText()));
        String dns = nodeVm("exec", "--name", nodeName, "--", "getent", "ahostsv4", "cache.example.test");
        assertTrue(dns.contains("169.254.53.1"), dns);
    }

    private static String command(int port, String command, int replyBytes) throws IOException {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(45_000);
            String[] args = command.split(" ");
            StringBuilder resp = new StringBuilder("*" + args.length + "\r\n");
            for (String arg : args) {
                resp.append('$').append(arg.length()).append("\r\n").append(arg).append("\r\n");
            }
            socket.getOutputStream().write(resp.toString().getBytes(StandardCharsets.US_ASCII));
            return new String(socket.getInputStream().readNBytes(replyBytes), StandardCharsets.US_ASCII);
        }
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
