package io.github.hectorvent.floci.runtime.node;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

/** The runtime file, the node directory and the client as Floci wires them at startup. */
@QuarkusTest
@TestProfile(SmolvmNodeDirectoryIntegrationTest.RuntimeFileProfile.class)
class SmolvmNodeDirectoryIntegrationTest {

    private static final String TOKEN = "integration-token-0123456789";
    private static final Path DIR = Path.of("target", "smolvm-node-directory-it").toAbsolutePath();
    private static final Path FILE = DIR.resolve("runtime.yaml");

    private static FakeNodeApi fake;

    @Inject
    SmolvmNodeDirectory directory;

    @Inject
    SmolvmNodeClient client;

    @BeforeAll
    static void startFake() throws IOException {
        fake = new FakeNodeApi(TOKEN);
    }

    @AfterAll
    static void stopFake() {
        fake.close();
    }

    @Test
    void theRuntimeFileIsReadAtStartup() {
        SmolvmNode node = directory.find("node:dev").orElseThrow();

        assertThat(node.kind(), is(SmolvmNode.Kind.DECLARED));
        assertThat(node.token(), is(NodeToken.file(DIR.resolve("node-token"))));
    }

    @Test
    void aChangedRuntimeFileReachesTheDirectoryAndTheClient() throws IOException {
        fake.addPod("sandbox-orders", "shop", "orders");

        Files.writeString(FILE, runtimeFile(fake.url(), "dev", "prod"));
        Files.setLastModifiedTime(FILE, FileTime.from(Instant.now().plusSeconds(60)));
        await().atMost(Duration.ofSeconds(10)).until(() -> directory.find("node:prod").isPresent());
        SmolvmNode node = directory.find("node:dev").orElseThrow();
        List<NodePod> pods = client.listPods(node, SmolvmNodeClient.PodQuery.namespace("shop"));

        assertThat(directory.all().stream().map(SmolvmNode::name).toList(), contains("node:dev", "node:prod"));
        assertThat(node.baseUrl(), is(fake.url()));
        assertThat(pods.stream().map(NodePod::name).toList(), contains("orders"));
    }

    private static String runtimeFile(URI url, String... keys) {
        StringBuilder content = new StringBuilder("version: 1\nnodes:\n");
        for (String key : keys) {
            content.append("  ").append(key).append(":\n    url: ").append(url).append("\n    token-file: node-token\n");
        }
        return content.toString();
    }

    public static class RuntimeFileProfile implements QuarkusTestProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            try {
                Files.createDirectories(DIR);
                Path token = DIR.resolve("node-token");
                Files.writeString(token, TOKEN);
                Files.setPosixFilePermissions(token, PosixFilePermissions.fromString("rw-------"));
                Files.writeString(FILE, runtimeFile(URI.create("http://127.0.0.1:1"), "dev"));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return Map.of("floci.runtime.config-file", FILE.toString());
        }
    }
}
