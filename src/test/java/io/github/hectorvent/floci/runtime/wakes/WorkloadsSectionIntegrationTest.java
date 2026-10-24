package io.github.hectorvent.floci.runtime.wakes;

import io.github.hectorvent.floci.runtime.node.RuntimeFileWatcher;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.PodSelector;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Target;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

/** The runtime file's workloads as Floci reads them at startup and after a change. */
@QuarkusTest
@TestProfile(WorkloadsSectionIntegrationTest.WorkloadsProfile.class)
class WorkloadsSectionIntegrationTest {

    private static final Path DIR = Path.of("target", "workloads-section-it").toAbsolutePath();
    private static final Path FILE = DIR.resolve("runtime.yaml");

    @Inject
    RuntimeFileWatcher runtimeFile;

    @Inject
    WorkloadsSection workloads;

    @Test
    void theWorkloadsAreReadAtStartup() {
        WorkloadDefinitions definitions = runtimeFile.current().section(workloads);

        assertThat(definitions.workloads().get("orders").target(),
                is(new Target("node:dev", "shop", PodSelector.Controller.deployment("orders"))));
    }

    @Test
    void aChangedWorkloadsSectionIsReadAgain() throws IOException {
        Files.writeString(FILE, runtimeFile("orders", "payments"));
        Files.setLastModifiedTime(FILE, FileTime.from(Instant.now().plusSeconds(60)));

        await().atMost(Duration.ofSeconds(10))
                .until(() -> runtimeFile.current().section(workloads).workloads().containsKey("payments"));

        assertThat(runtimeFile.current().section(workloads).workloads().keySet(), contains("orders", "payments"));
    }

    private static String runtimeFile(String... names) {
        StringBuilder content = new StringBuilder("""
                version: 1
                nodes:
                  dev:
                    url: http://127.0.0.1:1
                    token-file: node-token
                workloads:
                """);
        for (String name : names) {
            content.append("  ").append(name).append(":\n    queues: [").append(name).append("-incoming]\n")
                    .append("    runtime: {type: kubernetes, node: dev, namespace: shop, deployment: ")
                    .append(name).append("}\n");
        }
        return content.toString();
    }

    public static class WorkloadsProfile implements QuarkusTestProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            try {
                Files.createDirectories(DIR);
                Path token = DIR.resolve("node-token");
                Files.writeString(token, "integration-token-0123456789");
                Files.setPosixFilePermissions(token, PosixFilePermissions.fromString("rw-------"));
                Files.writeString(FILE, runtimeFile("orders"));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return Map.of("floci.runtime.config-file", FILE.toString());
        }
    }
}
