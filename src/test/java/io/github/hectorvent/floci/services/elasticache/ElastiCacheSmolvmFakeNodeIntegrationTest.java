package io.github.hectorvent.floci.services.elasticache;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.hectorvent.floci.runtime.node.FakeDockerNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.elasticache.ElastiCacheClient;
import software.amazon.awssdk.services.elasticache.model.ReplicationGroup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(ElastiCacheSmolvmFakeNodeIntegrationTest.Profile.class)
class ElastiCacheSmolvmFakeNodeIntegrationTest {

    private static final Logger LOG = Logger.getLogger(ElastiCacheSmolvmFakeNodeIntegrationTest.class);
    private static final String TOKEN = "fake-cache-node-token-0123456789";
    private static final Path DIR = Path.of("target", "elasticache-smolvm-fake-node-it").toAbsolutePath();
    private static final int PORT = freePort();
    private static FakeDockerNode node;
    private static ServerSocket redis;

    @BeforeAll
    static void startNode() throws IOException {
        node = new FakeDockerNode(TOKEN, PORT);
        redis = new ServerSocket(0);
        for (int i = 2; i < 100; i++) {
            node.mapTarget("172.17.0." + i, 6379, redis.getLocalPort());
        }
        Thread.ofVirtual().start(() -> {
            while (!redis.isClosed()) {
                try {
                    Socket socket = redis.accept();
                    Thread.ofVirtual().start(() -> pong(socket));
                } catch (IOException e) {
                    LOG.debugv("Fake Redis accept loop stopped: {0}", e.getMessage());
                }
            }
        });
    }

    private static void pong(Socket socket) {
        try (socket) {
            socket.setSoTimeout(10_000);
            while (socket.getInputStream().readNBytes(14).length == 14) {
                socket.getOutputStream().write("+PONG\r\n".getBytes(StandardCharsets.US_ASCII));
                socket.getOutputStream().flush();
            }
        } catch (IOException e) {
            LOG.debugv("Fake Redis client disconnected: {0}", e.getMessage());
        }
    }

    @AfterAll
    static void stopNode() throws IOException {
        redis.close();
        node.close();
    }

    @Test
    void sdkCreateRunsOnTheNodeAndProxyTrafficUsesItsTunnel() throws IOException {
        try (ElastiCacheClient caches = ElastiCacheClient.builder()
                .endpointOverride(URI.create("http://localhost:" + RestAssured.port))
                .httpClientBuilder(UrlConnectionHttpClient.builder()).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("000000000000", "test"))).build()) {
            ReplicationGroup group = caches.createReplicationGroup(request -> request
                    .replicationGroupId("node-cache").replicationGroupDescription("Node cache")
                    .engine("valkey").cacheNodeType("cache.t3.micro").numCacheClusters(1)).replicationGroup();
            try {
                assertEquals("available", group.status());
                assertEquals(1, node.containers().size());
                JsonNode container = node.containers().values().iterator().next();
                JsonNode host = container.path("HostConfig");
                assertEquals("io.containerd.smolvm-pods.v2", host.path("Runtime").asText());
                assertEquals("true", host.path("Annotations").path("smolvm-pods.floci.io/hibernate").asText());
                assertEquals("2m", host.path("Annotations").path("smolvm-pods.floci.io/idle-timeout").asText());
                assertTrue(host.path("PortBindings").isMissingNode() || host.path("PortBindings").isNull()
                        || host.path("PortBindings").isEmpty());
                long before = tunnels();
                try (Socket client = new Socket("localhost", group.nodeGroups().getFirst().primaryEndpoint().port())) {
                    client.setSoTimeout(10_000);
                    client.getOutputStream().write("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII));
                    assertEquals("+PONG\r\n", new String(client.getInputStream().readNBytes(7), StandardCharsets.US_ASCII));
                }
                assertTrue(tunnels() > before, "the auth proxy must open a node tunnel for the client");
            } finally {
                caches.deleteReplicationGroup(request -> request.replicationGroupId("node-cache"));
            }
            assertTrue(node.containers().isEmpty(), "delete must remove the container from the node");
        }
    }

    private static long tunnels() {
        return node.requests().stream().filter(request -> request.path().equals("/v1/tunnel")).count();
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            try {
                Files.createDirectories(DIR);
                Path token = DIR.resolve("node-token");
                Files.writeString(token, TOKEN);
                Files.setPosixFilePermissions(token, PosixFilePermissions.fromString("rw-------"));
                Path runtime = DIR.resolve("runtime.yaml");
                Files.writeString(runtime, "version: 1\nnodes:\n  fake:\n    url: http://127.0.0.1:" + PORT
                        + "\n    token-file: node-token\n");
                return Map.of("floci.runtime.config-file", runtime.toString(),
                        "floci.services.elasticache.smolvm.enabled", "true",
                        "floci.services.elasticache.smolvm.idle-timeout", "2m");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
