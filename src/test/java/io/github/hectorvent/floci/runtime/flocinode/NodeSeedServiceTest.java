package io.github.hectorvent.floci.runtime.flocinode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.RuntimeConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.RuntimeConfig.NodeConfig;
import io.github.hectorvent.floci.runtime.flocinode.NodeSeedContributor.Contribution;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** The node's own seed, with no service's additions or with one that a fake contributor makes. */
class NodeSeedServiceTest {

    private static final String NODE_ID = "floci-node-0123456789abcdef";
    private static final ObjectMapper YAML = new YAMLMapper();

    @TempDir
    Path dataPath;

    private NodeConfig node;
    private final List<NodeSeedContributor> contributors = new ArrayList<>();
    private NodeSeed seed;

    @BeforeEach
    void setUp() {
        EmulatorConfig config = mock(EmulatorConfig.class);
        RuntimeConfig runtime = mock(RuntimeConfig.class);
        node = mock(NodeConfig.class);
        when(config.runtime()).thenReturn(runtime);
        when(runtime.node()).thenReturn(node);
        when(config.port()).thenReturn(4566);
        when(node.dataPath()).thenReturn(dataPath.toString());
        NodeSettings settings = new NodeSettings(config, () -> false, () -> false, "Mac OS X", "aarch64");
        seed = new NodeSeed(config, settings, contributors);
    }

    private static NodeRecord record(Map<String, List<String>> k3sArgs) {
        return new NodeRecord(NODE_ID, 6500, 6600, "32080-32099", "a".repeat(64), false, List.of(), "123456789012",
                "eu-west-1", "i-0123456789abcdef0.eu-west-1.compute.internal", FlociNode.DEFAULT_SERVICE_CIDR,
                FlociNode.DEFAULT_POD_CIDR, k3sArgs, List.of());
    }

    private Path seedDir() {
        return dataPath.resolve(NODE_ID).resolve("seed");
    }

    private String read(String name) throws IOException {
        return Files.readString(seedDir().resolve(name), StandardCharsets.UTF_8);
    }

    private static List<String> texts(JsonNode list) {
        List<String> texts = new ArrayList<>();
        list.forEach(item -> texts.add(item.asText()));
        return texts;
    }

    @Test
    void aNodeWithoutAServiceGetsItsOwnFilesOnly() throws IOException {
        seed.write(record(Map.of()));

        try (Stream<Path> files = Files.walk(seedDir())) {
            List<String> names = files.filter(Files::isRegularFile)
                    .map(file -> seedDir().relativize(file).toString()).toList();
            assertThat(names, containsInAnyOrder("version", "env", "k3s.yaml", "tokens.csv", "api-token", "hosts"));
        }
        JsonNode k3s = YAML.readTree(read("k3s.yaml"));
        assertEquals("i-0123456789abcdef0.eu-west-1.compute.internal", k3s.path("node-name").asText());
        assertEquals("10.100.0.0/16", k3s.path("service-cidr").asText());
        assertEquals("10.42.0.0/16", k3s.path("cluster-cidr").asText());
        assertThat(texts(k3s.path("kubelet-arg+")), contains("provider-id=aws:///eu-west-1a/i-0123456789abcdef0",
                "node-labels=topology.kubernetes.io/zone=eu-west-1a,topology.kubernetes.io/region=eu-west-1"));
        assertThat(texts(k3s.path("kube-apiserver-arg+")), contains("token-auth-file=/etc/rancher/k3s/floci/tokens.csv"));
        assertEquals("1\n", read("version"));
        assertEquals(seedDir(), seed.seedDir(NODE_ID));
    }

    @Test
    void contributionsAndTheFirstUsesArgumentsReachTheDropIn() throws IOException {
        contributors.add(new NodeSeedContributor() {
            @Override
            public String name() {
                return "feature";
            }

            @Override
            public Contribution contribute(NodeRecord record) {
                return new Contribution(Map.of(NodeSeed.REGISTRIES_FILE, "mirrors: {}\n"),
                        Map.of(NodeSeed.API_SERVER_ARGS, List.of("audit-log-maxage=1")));
            }

            @Override
            public List<SmolvmNode.Endpoint> endpoints(NodeRecord record) {
                return List.of(new SmolvmNode.Endpoint("169.254.170.23", 80, 4566));
            }
        });
        contributors.add(new NodeSeedContributor() {
            @Override
            public String name() {
                return "idle";
            }

            @Override
            public Contribution contribute(NodeRecord record) {
                return Contribution.NONE;
            }
        });

        List<String> seeded = seed.write(record(Map.of(NodeSeed.KUBELET_ARGS, List.of("max-pods=250"),
                NodeSeed.SCHEDULER_ARGS, List.of("v=4"))));

        assertEquals(List.of("feature"), seeded, "the contributors whose additions the seed has");
        JsonNode k3s = YAML.readTree(read("k3s.yaml"));
        assertThat(texts(k3s.path("kubelet-arg+")), contains(
                "provider-id=aws:///eu-west-1a/i-0123456789abcdef0",
                "node-labels=topology.kubernetes.io/zone=eu-west-1a,topology.kubernetes.io/region=eu-west-1",
                "max-pods=250"));
        assertThat(texts(k3s.path("kube-apiserver-arg+")), contains(
                "token-auth-file=/etc/rancher/k3s/floci/tokens.csv", "audit-log-maxage=1"));
        assertThat(texts(k3s.path("kube-scheduler-arg+")), contains("v=4"));
        assertFalse(k3s.has("kube-controller-manager-arg+"));
        assertEquals("mirrors: {}\n", read("registries.yaml"));
        assertEquals(List.of(new SmolvmNode.Endpoint("169.254.170.23", 80, 4566)), seed.endpoints(record(Map.of())));
    }

    @Test
    void seedIsPrivateToItsOwner() throws IOException {
        seed.write(record(Map.of()));

        for (Path dir : List.of(dataPath, seedDir().getParent(), seedDir())) {
            assertEquals("rwx------", PosixFilePermissions.toString(Files.getPosixFilePermissions(dir)), dir.toString());
        }
        try (Stream<Path> files = Files.walk(seedDir())) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                assertEquals("rw-------", PosixFilePermissions.toString(Files.getPosixFilePermissions(file)),
                        file.toString());
                assertFalse(file.getFileName().toString().startsWith("."), "a temporary file stayed: " + file);
            }
        }
    }

    @Test
    void envHasEveryNodeSettingInOrderAndNeverIsolation() throws IOException {
        when(node.idleTimeout()).thenReturn(Optional.of("7m"));
        when(node.maxAwakePods()).thenReturn(OptionalInt.of(4));
        when(node.maxAwakeMemoryMib()).thenReturn(OptionalInt.of(4096));
        when(node.reportedMemoryGib()).thenReturn(OptionalInt.of(200));
        when(node.rustLog()).thenReturn(Optional.of("smolvm_pods=debug"));

        seed.write(record(Map.of()));

        String env = read("env");
        assertEquals("""
                FLOCI_NODE_IDLE_TIMEOUT=7m
                FLOCI_NODE_CPU_THRESHOLD=
                FLOCI_NODE_CPU_WINDOW=
                FLOCI_NODE_MAX_AWAKE_PODS=4
                FLOCI_NODE_MAX_AWAKE_MEMORY=4096
                FLOCI_NODE_ADMISSION_WAIT=
                FLOCI_NODE_MEMORY_GIB=200
                FLOCI_NODE_CPUS=
                RUST_LOG=smolvm_pods=debug
                """, env);
        assertThat(env, not(containsString("FLOCI_NODE_ISOLATION")));
    }

    @Test
    void writeEnvChangesOnlyTheEnvAndOnlyWhenTheSettingsChanged() throws IOException {
        seed.write(record(Map.of()));
        String k3s = read("k3s.yaml");
        String version = read("version");

        assertFalse(seed.writeEnv(NODE_ID));

        when(node.idleTimeout()).thenReturn(Optional.of("2m"));
        assertTrue(seed.writeEnv(NODE_ID));

        assertThat(read("env"), containsString("FLOCI_NODE_IDLE_TIMEOUT=2m\n"));
        assertEquals(k3s, read("k3s.yaml"));
        assertEquals(version, read("version"));
    }

    @Test
    void tokensAreGeneratedOnceAndKeptAcrossWrites() throws IOException {
        seed.write(record(Map.of()));
        String adminToken = seed.adminToken(NODE_ID);
        String apiToken = seed.apiToken(NODE_ID).read();

        seed.write(record(Map.of()));

        assertThat(adminToken, matchesPattern("[0-9a-f]{64}"));
        assertThat(apiToken, matchesPattern("[0-9a-f]{64}"));
        assertFalse(adminToken.equals(apiToken));
        assertEquals(adminToken, seed.adminToken(NODE_ID));
        assertEquals(apiToken, seed.apiToken(NODE_ID).read());
        assertEquals(adminToken + ",floci-admin,floci-admin,system:masters\n", read("tokens.csv"));
        assertEquals(apiToken + "\n", read("api-token"));
    }

    @Test
    void aWriteThatFailsPartWayLeavesNoVersion() throws IOException {
        Files.createDirectories(seedDir().resolve("hosts").resolve("in-the-way"));

        assertThrows(UncheckedIOException.class, () -> seed.write(record(Map.of())));

        assertFalse(Files.exists(seedDir().resolve("version")));
    }

    @Test
    void deleteRemovesTheNodesFiles() {
        seed.write(record(Map.of()));

        seed.delete(NODE_ID);

        assertFalse(Files.exists(seedDir().getParent()));
        assertTrue(Files.isDirectory(dataPath));
    }

    @Test
    void nodeIdsAreRandomAndNameNoOtherDirectory() {
        String first = seed.newNodeId();

        assertThat(first, matchesPattern(NodeRecord.NODE_ID));
        assertFalse(first.equals(seed.newNodeId()));
        assertThat(seed.seedDir(first).toString(), endsWith("/" + first + "/seed"));
        for (String invalid : List.of("../keys", NODE_ID + "/..", "smolvm-pods-node", "floci-eks-0123456789abcdef")) {
            assertThrows(IllegalArgumentException.class, () -> seed.seedDir(invalid), invalid);
        }
    }

    @Test
    void envNamesAreSettingsThatFlociNodeTakesFromTheSeed() throws IOException {
        seed.write(record(Map.of()));

        for (String line : read("env").lines().toList()) {
            String name = line.substring(0, line.indexOf('='));
            assertThat(name, matchesPattern("FLOCI_NODE_[A-Z0-9_]+|RUST_LOG"));
            assertThat(name, not("FLOCI_NODE_ISOLATION"));
        }
    }
}
