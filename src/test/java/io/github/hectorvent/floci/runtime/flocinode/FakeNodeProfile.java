package io.github.hectorvent.floci.runtime.flocinode;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.runtime.node.NodesSection;
import io.github.hectorvent.floci.runtime.node.RuntimeFileWatcher;
import io.quarkus.test.junit.QuarkusTestProfile;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Floci with the node that it creates and no VM: {@link FakeFlociNodes} stands for
 * {@code node-vm.sh}, and {@link MacSettings} passes the platform check on any host, so a class
 * with this profile also runs on Linux. The script, smolvm checkout and image that the settings
 * name exist only for the checks; nothing runs them. A profile adds the services that use the
 * node.
 */
public class FakeNodeProfile implements QuarkusTestProfile {

    public static final int API_BASE_PORT = 46500;
    public static final int SERVICE_BASE_PORT = 46600;

    private static final Path ROOT = createRoot();

    @Override
    public Map<String, String> getConfigOverrides() {
        return overrides();
    }

    @Override
    public Set<Class<?>> getEnabledAlternatives() {
        return Set.of(FakeFlociNodes.class, MacSettings.class);
    }

    /** The settings of the node that Floci creates, on the fakes. */
    public static Map<String, String> overrides() {
        Map<String, String> overrides = new LinkedHashMap<>();
        overrides.put("floci.runtime.node.data-path", ROOT.resolve("node").toString());
        overrides.put("floci.runtime.node.api-server-base-port", String.valueOf(API_BASE_PORT));
        overrides.put("floci.runtime.node.api-server-max-port", String.valueOf(API_BASE_PORT + 19));
        overrides.put("floci.runtime.node.node-vm-script", ROOT.resolve("node-vm.sh").toString());
        overrides.put("floci.runtime.node.smolvm-src", ROOT.resolve("smolvm").toString());
        overrides.put("floci.runtime.node.image", ROOT.resolve("node.tar").toString());
        overrides.put("floci.runtime.node.image-sha256", sha256(ROOT.resolve("node.tar")));
        overrides.put("floci.runtime.node.node-service-base-port", String.valueOf(SERVICE_BASE_PORT));
        overrides.put("floci.runtime.node.node-service-max-port", String.valueOf(SERVICE_BASE_PORT + 19));
        overrides.put("floci.runtime.node.node-ports", "none");
        // MacSettings takes the place of the settings bean, whose checks would see the host.
        overrides.put("quarkus.arc.exclude-types", NodeSettings.class.getName());
        return overrides;
    }

    private static Path createRoot() {
        try {
            Path root = Files.createTempDirectory("floci-node-");
            Path script = root.resolve("node-vm.sh");
            Files.writeString(script, "#!/bin/sh\necho 'node-vm.sh is not run in these tests' >&2\nexit 1\n");
            Files.setPosixFilePermissions(script, PosixFilePermissions.fromString("rwx------"));
            Files.createDirectories(root.resolve("smolvm"));
            Files.writeString(root.resolve("node.tar"), "not a node image");
            return root;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String sha256(Path file) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The node settings as on an Apple silicon Mac, natively. */
    @Alternative
    @Singleton
    public static class MacSettings extends NodeSettings {

        @Inject
        public MacSettings(EmulatorConfig config, RuntimeFileWatcher runtimeFile, NodesSection nodesSection) {
            super(config, () -> false, () -> !runtimeFile.current().section(nodesSection).isEmpty(), "Mac OS X",
                    "aarch64");
        }
    }
}
