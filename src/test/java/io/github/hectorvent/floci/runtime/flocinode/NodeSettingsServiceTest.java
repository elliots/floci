package io.github.hectorvent.floci.runtime.flocinode;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.EksServiceConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.RdsServiceConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.RdsServiceConfig.RdsSmolvmConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.RuntimeConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.RuntimeConfig.NodeConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NodeSettingsServiceTest {

    @TempDir
    Path dir;

    private EmulatorConfig config;
    private EksServiceConfig eks;
    private RdsSmolvmConfig rdsSmolvm;
    private NodeConfig smolvm;
    private final AtomicBoolean inContainer = new AtomicBoolean();
    private final AtomicBoolean runtimeFileHasNodes = new AtomicBoolean();
    private Path image;
    private NodeSettings settings;

    private EmulatorConfig.LambdaServiceConfig lambda;

    @BeforeEach
    void setUp() throws IOException {
        config = mock(EmulatorConfig.class);
        EmulatorConfig.ServicesConfig services = mock(EmulatorConfig.ServicesConfig.class);
        eks = mock(EksServiceConfig.class);
        RdsServiceConfig rds = mock(RdsServiceConfig.class);
        rdsSmolvm = mock(RdsSmolvmConfig.class);
        RuntimeConfig runtime = mock(RuntimeConfig.class);
        smolvm = mock(NodeConfig.class);
        when(config.services()).thenReturn(services);
        when(config.runtime()).thenReturn(runtime);
        when(runtime.node()).thenReturn(smolvm);
        when(config.port()).thenReturn(4566);
        when(services.eks()).thenReturn(eks);
        when(services.rds()).thenReturn(rds);
        lambda = mock(EmulatorConfig.LambdaServiceConfig.class, RETURNS_DEEP_STUBS);
        when(services.lambda()).thenReturn(lambda);
        when(rds.smolvm()).thenReturn(rdsSmolvm);
        when(rds.enabled()).thenReturn(true);
        when(eks.enabled()).thenReturn(true);
        when(eks.provider()).thenReturn("smolvm-pods");
        when(smolvm.apiServerBasePort()).thenReturn(6500);
        when(smolvm.apiServerMaxPort()).thenReturn(6599);
        when(smolvm.readyTimeoutSeconds()).thenReturn(600);

        Path script = Files.writeString(dir.resolve("node-vm.sh"), "#!/bin/sh\n");
        Files.setPosixFilePermissions(script, PosixFilePermissions.fromString("rwxr-xr-x"));
        Path smolvmSrc = Files.createDirectory(dir.resolve("smolvm"));
        image = Files.writeString(dir.resolve("node-image.tar"), "node image");
        when(smolvm.nodeVmScript()).thenReturn(Optional.of(script.toString()));
        when(smolvm.smolvmSrc()).thenReturn(Optional.of(smolvmSrc.toString()));
        when(smolvm.image()).thenReturn(Optional.of(image.toString()));
        when(smolvm.imageSha256()).thenReturn(Optional.of(sha256(image)));
        when(smolvm.cpus()).thenReturn(4);
        when(smolvm.memoryMib()).thenReturn(6144);
        when(smolvm.storageGib()).thenReturn(40);
        when(smolvm.overlayGib()).thenReturn(8);
        when(smolvm.nodeServiceBasePort()).thenReturn(6600);
        when(smolvm.nodeServiceMaxPort()).thenReturn(6699);
        when(smolvm.nodePorts()).thenReturn("32080-32099");
        settings = new NodeSettings(config, inContainer::get, runtimeFileHasNodes::get, "Mac OS X", "aarch64");
    }

    private static String sha256(Path file) throws IOException {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void theDefaultsOnAnAppleSiliconMacPass() {
        assertThat(settings.problems(), empty());
        assertDoesNotThrow(settings::requireValid);
    }

    @Test
    void extraPortsValidateShapeConflictsAndTheTotalMappingBudget() {
        when(smolvm.extraPorts()).thenReturn(Optional.of(List.of("443:30443", "8080:30080")));
        assertThat(settings.problems(), empty());
        for (String invalid : List.of("0:80", "443:65536", "443", "*:443:30443", "443:30443/udp", "6501:80", "32080:80")) {
            when(smolvm.extraPorts()).thenReturn(Optional.of(List.of(invalid)));
            assertThat(invalid, settings.problems(), not(empty()));
        }
        when(smolvm.extraPorts()).thenReturn(Optional.of(List.of("443:30443", "443:30080")));
        assertThat(settings.problems(), hasItem(containsString("overlaps")));
        when(smolvm.extraPorts()).thenReturn(Optional.of(List.of("443:30443")));
        when(smolvm.nodePorts()).thenReturn("32000-32061");
        assertThat(settings.problems(), hasItem(containsString("64 mappings")));
    }

    @Test
    void volumesRequireExistingHostDirectoriesAndDistinctGuestPaths() {
        String host = dir.toString();
        when(smolvm.volumes()).thenReturn(Optional.of(List.of(host + ":/workspace:ro")));
        assertThat(settings.problems(), empty());
        for (String invalid : List.of("relative:/workspace", host + ":/", host + ":relative",
                host + ":/workspace:rw", host + ":/workspace/../etc", host + "/missing:/workspace")) {
            when(smolvm.volumes()).thenReturn(Optional.of(List.of(invalid)));
            assertThat(invalid, settings.problems(), not(empty()));
        }
        when(smolvm.volumes()).thenReturn(Optional.of(List.of(host + ":/workspace", host + ":/workspace:ro")));
        assertThat(settings.problems(), hasItem(containsString("unique absolute directory")));
    }

    @Test
    void flociMustRunNativelyOnAnAppleSiliconMac() {
        inContainer.set(true);
        assertThat(settings.problems(), contains(
                "Floci runs in a container, and must run natively on the Mac, as a JVM or the native binary"));

        inContainer.set(false);
        NodeSettings linux = new NodeSettings(config, inContainer::get, runtimeFileHasNodes::get, "Linux", "aarch64");
        assertThat(linux.problems(), contains("Floci must run on an Apple silicon Mac, not Linux on aarch64"));
        NodeSettings intelMac = new NodeSettings(config, inContainer::get, runtimeFileHasNodes::get, "Mac OS X", "x86_64");
        assertThat(intelMac.problems(), hasSize(1));
    }

    @Test
    void startupRefusesEveryProblemAtOnce() {
        inContainer.set(true);
        when(smolvm.nodeVmScript()).thenReturn(Optional.empty());

        IllegalStateException refused = assertThrows(IllegalStateException.class, settings::requireValid);

        assertThat(refused.getMessage(), containsString("Floci cannot create the smolvm-pods node that EKS use: "));
        assertThat(refused.getMessage(), containsString("Floci runs in a container"));
        assertThat(refused.getMessage(), containsString(
                "floci.runtime.node.node-vm-script (FLOCI_RUNTIME_NODE_NODE_VM_SCRIPT) is not set"));
    }

    @Test
    void k3sMockAndDisabledEksCheckNothingOfSmolvm() {
        inContainer.set(true);
        when(smolvm.image()).thenReturn(Optional.empty());

        when(eks.provider()).thenReturn("k3s");
        assertDoesNotThrow(settings::requireValid);

        when(eks.provider()).thenReturn("smolvm-pods");
        when(eks.mock()).thenReturn(true);
        assertDoesNotThrow(settings::requireValid);

        when(eks.mock()).thenReturn(false);
        when(eks.enabled()).thenReturn(false);
        assertDoesNotThrow(settings::requireValid);
    }

    @Test
    void rdsOnTheNodeChecksTheSettingsToo() {
        inContainer.set(true);
        when(eks.provider()).thenReturn("k3s");
        when(rdsSmolvm.enabled()).thenReturn(true);

        IllegalStateException refused = assertThrows(IllegalStateException.class, settings::requireValid);
        assertThat(refused.getMessage(), containsString("Floci cannot create the smolvm-pods node that RDS use: "));

        when(eks.provider()).thenReturn("smolvm-pods");
        refused = assertThrows(IllegalStateException.class, settings::requireValid);
        assertThat(refused.getMessage(), containsString("that RDS and EKS use: "));
    }

    @Test
    void lambdaOnTheNodeChecksTheSettingsToo() {
        inContainer.set(true);
        when(eks.provider()).thenReturn("k3s");
        when(lambda.enabled()).thenReturn(true);
        when(lambda.smolvm().enabled()).thenReturn(true);

        IllegalStateException refused = assertThrows(IllegalStateException.class, settings::requireValid);
        assertThat(refused.getMessage(), containsString("the smolvm-pods node that Lambda use: "));

        when(rdsSmolvm.enabled()).thenReturn(true);
        when(eks.provider()).thenReturn("smolvm-pods");
        refused = assertThrows(IllegalStateException.class, settings::requireValid);
        assertThat(refused.getMessage(), containsString("that RDS, EKS and Lambda use: "));
    }

    @Test
    void aNodeInTheRuntimeFileNeedsNoSettingsToCreateOne() {
        inContainer.set(true);
        runtimeFileHasNodes.set(true);

        assertDoesNotThrow(settings::requireValid);
        assertThat(settings.problems(), hasSize(1));
    }

    @Test
    void pathsMustBeAbsoluteAndPresent() throws IOException {
        when(smolvm.nodeVmScript()).thenReturn(Optional.of("scripts/node-vm.sh"));
        when(smolvm.smolvmSrc()).thenReturn(Optional.of(dir.resolve("missing").toString()));
        when(smolvm.image()).thenReturn(Optional.of(""));

        List<String> problems = settings.problems();

        assertThat(problems, hasItem("floci.runtime.node.node-vm-script "
                + "(FLOCI_RUNTIME_NODE_NODE_VM_SCRIPT) must be an absolute path, not scripts/node-vm.sh"));
        assertThat(problems, hasItem("floci.runtime.node.smolvm-src (FLOCI_RUNTIME_NODE_SMOLVM_SRC) "
                + "is not a directory: " + dir.resolve("missing")));
        assertThat(problems, hasItem("floci.runtime.node.image (FLOCI_RUNTIME_NODE_IMAGE) is not set"));

        Path notExecutable = Files.writeString(dir.resolve("plain.sh"), "#!/bin/sh\n");
        when(smolvm.nodeVmScript()).thenReturn(Optional.of(notExecutable.toString()));
        assertThat(settings.problems(), hasItem(containsString("node-vm-script "
                + "(FLOCI_RUNTIME_NODE_NODE_VM_SCRIPT) is not an executable file")));
    }

    @Test
    void theNodeImageMustHaveItsConfiguredSha256() throws IOException {
        when(smolvm.imageSha256()).thenReturn(Optional.of("0".repeat(64)));
        assertThat(settings.problems(), contains("the SHA-256 of " + image + " is " + sha256(image)
                + ", not floci.runtime.node.image-sha256 (FLOCI_RUNTIME_NODE_IMAGE_SHA256) "
                + "0".repeat(64)));

        when(smolvm.imageSha256()).thenReturn(Optional.of(sha256(image).toUpperCase(Locale.ROOT)));
        assertThat(settings.problems(), empty());

        Files.writeString(image, "a rebuilt node image", StandardCharsets.UTF_8);
        assertThat(settings.problems(), hasSize(1));

        when(smolvm.imageSha256()).thenReturn(Optional.of("not-a-digest"));
        assertThat(settings.problems(), contains("floci.runtime.node.image-sha256 "
                + "(FLOCI_RUNTIME_NODE_IMAGE_SHA256) must be 64 hex digits"));
    }

    @Test
    void nodeServiceSettingsMustBeWhatTheNodeServiceReads() {
        for (String duration : List.of("30s", "1m30s", "1.5h", "500ms", ".5s")) {
            when(smolvm.idleTimeout()).thenReturn(Optional.of(duration));
            assertThat(duration, settings.problems(), empty());
        }
        for (String threshold : List.of("off", "OFF", "0", "0.5", "2")) {
            when(smolvm.cpuThreshold()).thenReturn(Optional.of(threshold));
            assertThat(threshold, settings.problems(), empty());
        }
        for (String duration : List.of("5", "0s", "5 m", "-1s", "1d")) {
            when(smolvm.cpuWindow()).thenReturn(Optional.of(duration));
            assertThat(duration, settings.problems(), contains("floci.runtime.node.cpu-window "
                    + "(FLOCI_RUNTIME_NODE_CPU_WINDOW) must be a duration above 0, such as 30s, 5m or 1m30s, "
                    + "not \"" + duration + "\""));
        }
        when(smolvm.cpuWindow()).thenReturn(Optional.empty());
        for (String threshold : List.of("-1", "busy", "1e3")) {
            when(smolvm.cpuThreshold()).thenReturn(Optional.of(threshold));
            assertThat(threshold, settings.problems(), hasSize(1));
        }
        when(smolvm.cpuThreshold()).thenReturn(Optional.empty());
        when(smolvm.maxAwakePods()).thenReturn(OptionalInt.of(-1));
        when(smolvm.rustLog()).thenReturn(Optional.of("smolvm_pods=debug info"));
        assertThat(settings.problems(), contains(
                "floci.runtime.node.max-awake-pods (FLOCI_RUNTIME_NODE_MAX_AWAKE_PODS) must be 0 or "
                        + "more, not -1",
                "floci.runtime.node.rust-log (FLOCI_RUNTIME_NODE_RUST_LOG) must be one word of visible "
                        + "ASCII characters"));
    }

    @Test
    void portRangesMustBeValidAndApart() {
        for (String nodePorts : List.of("none", "32080", "32080-32099")) {
            when(smolvm.nodePorts()).thenReturn(nodePorts);
            assertThat(nodePorts, settings.problems(), empty());
        }
        for (String nodePorts : List.of("32099-32080", "70000", "a-b", "")) {
            when(smolvm.nodePorts()).thenReturn(nodePorts);
            assertThat(nodePorts, settings.problems(), contains("floci.runtime.node.node-ports "
                    + "(FLOCI_RUNTIME_NODE_NODE_PORTS) must be FIRST-LAST, PORT or none, with ports from 1 to "
                    + "65535, not \"" + nodePorts + "\""));
        }
        when(smolvm.nodePorts()).thenReturn("6650-6660");
        assertThat(settings.problems(), contains(containsString("node-ports")));

        when(smolvm.nodePorts()).thenReturn("none");
        when(smolvm.nodeServiceBasePort()).thenReturn(6550);
        assertThat(settings.problems(), contains(containsString("overlaps floci.runtime.node.api-server-base-port")));

        when(smolvm.nodeServiceBasePort()).thenReturn(6700);
        assertThat(settings.problems(), contains(containsString("must be a range of ports from 1 to 65535")));
    }

    @Test
    void theFlociEndpointIsHostAndPort() {
        assertEquals("host.docker.internal:4566", settings.flociEndpoint());
        assertEquals("host.docker.internal", settings.flociEndpointHost());

        for (String endpoint : List.of("localhost.floci.io:4566", "100.96.0.1:4567", "floci:4566")) {
            when(smolvm.flociEndpoint()).thenReturn(Optional.of(endpoint));
            assertThat(endpoint, settings.problems(), empty());
        }
        assertEquals("floci", settings.flociEndpointHost());
        for (String endpoint : List.of("4566", "floci", "1.2.3.400:80", "floci:0", "http://floci:4566")) {
            when(smolvm.flociEndpoint()).thenReturn(Optional.of(endpoint));
            assertThat(endpoint, settings.problems(), contains(containsString("floci-endpoint")));
        }
    }

    @Test
    void nodeEnvHasEverySettingWithEmptyForTheNodesDefault() {
        when(smolvm.cpuThreshold()).thenReturn(Optional.of("off"));
        when(smolvm.reportedCpus()).thenReturn(OptionalInt.of(64));

        Map<String, String> env = settings.nodeEnv();

        assertThat(List.copyOf(env.keySet()), contains("FLOCI_NODE_IDLE_TIMEOUT", "FLOCI_NODE_CPU_THRESHOLD",
                "FLOCI_NODE_CPU_WINDOW", "FLOCI_NODE_MAX_AWAKE_PODS", "FLOCI_NODE_MAX_AWAKE_MEMORY",
                "FLOCI_NODE_ADMISSION_WAIT", "FLOCI_NODE_MEMORY_GIB", "FLOCI_NODE_CPUS", "RUST_LOG"));
        assertEquals("off", env.get("FLOCI_NODE_CPU_THRESHOLD"));
        assertEquals("64", env.get("FLOCI_NODE_CPUS"));
        assertEquals("", env.get("FLOCI_NODE_IDLE_TIMEOUT"));
        assertThat(env.keySet(), not(hasItem("FLOCI_NODE_ISOLATION")));
    }
}
