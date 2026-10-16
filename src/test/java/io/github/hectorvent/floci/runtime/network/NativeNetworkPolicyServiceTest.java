package io.github.hectorvent.floci.runtime.network;

import io.github.hectorvent.floci.config.EmulatorConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class NativeNetworkPolicyServiceTest {
    @TempDir Path directory;

    @Test
    void nativeIsolationFingerprintUsesExactSharedFileBytes() throws Exception {
        String content = """
                network:
                  isolation:
                    enabled: true
                  native:
                    dns-nameservers: ["127.0.0.1:5300"]
                    allowed-egress:
                      - destination: host
                        protocol: tcp
                        ports: "4566"
                """;
        Path file = directory.resolve("network.yaml");
        Files.writeString(file, content);
        EmulatorConfig config = config(file);
        assertEquals(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(content.getBytes(StandardCharsets.UTF_8))), NativeNetworkPolicy.expectedFingerprint(config));
        String prior = NativeNetworkPolicy.expectedFingerprint(config);
        Files.writeString(file, content + "# policy revision\n");
        assertNotEquals(prior, NativeNetworkPolicy.expectedFingerprint(config));
    }

    @Test
    void isolationCannotSilentlyUseAnUnconfiguredNativeBoundary() throws Exception {
        Path file = directory.resolve("network.yaml");
        Files.writeString(file, "network: {isolation: {enabled: true}}\n");
        assertThrows(IllegalArgumentException.class, () -> NativeNetworkPolicy.expectedFingerprint(config(file)));
        Files.writeString(file, "network: {isolation: {enabled: false}}\n");
        assertEquals("", NativeNetworkPolicy.expectedFingerprint(config(file)));
    }

    private static EmulatorConfig config(Path file) {
        EmulatorConfig config = mock(EmulatorConfig.class);
        EmulatorConfig.NetworkConfig network = mock(EmulatorConfig.NetworkConfig.class);
        when(config.network()).thenReturn(network);
        when(network.configFile()).thenReturn(Optional.of(file.toString()));
        return config;
    }
}
