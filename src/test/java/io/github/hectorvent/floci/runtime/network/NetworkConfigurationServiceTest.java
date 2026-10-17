package io.github.hectorvent.floci.runtime.network;

import io.github.hectorvent.floci.config.EmulatorConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class NetworkConfigurationServiceTest {
    @TempDir Path directory;

    @Test
    void doesNotReadOrFallBackToTheOnDemandFile() {
        EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.network().configFile()).thenReturn(Optional.empty());
        NetworkConfiguration network = new NetworkConfiguration(config);
        assertFalse(network.configured());
        assertFalse(network.network().isolated());
        assertTrue(network.network().routes().isEmpty());
        verify(config, never()).onDemand();
    }

    @Test
    void networkFileNeedsNoOnDemandConfiguration() throws Exception {
        Path file = directory.resolve("network.yaml");
        Files.writeString(file, """
                network:
                  isolation:
                    enabled: true
                  routes:
                    vendor:
                      origins: [https://api.vendor.test]
                      target:
                        backend-url: http://simulator.localhost.floci.io:8080
                """);
        EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.network().configFile()).thenReturn(Optional.of(file.toString()));
        NetworkConfiguration network = new NetworkConfiguration(config);
        assertTrue(network.configured());
        assertTrue(network.network().isolated());
        assertEquals("simulator.localhost.floci.io", network.network().routes().getFirst().backendUrl().getHost());
        assertFalse(network.network().routes().getFirst().preserveHost());
        verify(config, never()).onDemand();
    }

    @Test
    void explicitDisabledFileCanRemoveAnExistingPolicy() throws Exception {
        Path file = directory.resolve("network.yaml");
        Files.writeString(file, "network: {isolation: {enabled: false}}");
        EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.network().configFile()).thenReturn(Optional.of(file.toString()));
        NetworkConfiguration network = new NetworkConfiguration(config);
        assertTrue(network.configured());
        assertFalse(network.network().isolated());
    }

    @Test
    void fingerprintUsesTheExactParsedBytesWithoutRequiringNativeConfiguration() throws Exception {
        Path file = directory.resolve("network.yaml");
        Files.writeString(file, "network: {isolation: {enabled: true}}\n");
        String fingerprint = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
        EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.network().configFile()).thenReturn(Optional.of(file.toString()));
        NetworkConfiguration network = new NetworkConfiguration(config);
        assertTrue(network.network().isolated());
        assertEquals(fingerprint, network.fingerprint());
        Files.writeString(file, "network: {isolation: {enabled: false}}\n");
        assertTrue(network.network().isolated());
        assertEquals(fingerprint, network.fingerprint());
        assertNotEquals(fingerprint, new NetworkConfiguration(config).fingerprint());
    }
}
