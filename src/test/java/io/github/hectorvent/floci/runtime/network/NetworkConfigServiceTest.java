package io.github.hectorvent.floci.runtime.network;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class NetworkConfigServiceTest {
    private static final String CONFIG = """
            network:
              isolation:
                enabled: true
              routes:
                payments:
                  origins: [https://API.vendor.test, https://uploads.vendor.test:443, http://api.vendor.test]
                  target:
                    backend-url: http://payments:8080
            """;

    @Test
    void multipleOriginsShareOneBackendAndNormalizeDefaultPorts() throws Exception {
        NetworkDefinition network = parse(CONFIG);
        assertTrue(network.isolated());
        assertEquals(List.of(URI.create("https://api.vendor.test:443"), URI.create("https://uploads.vendor.test:443"),
                URI.create("http://api.vendor.test:80")), network.routes().getFirst().origins());
        assertEquals(URI.create("http://payments:8080"), network.routes().getFirst().backendUrl());
        assertThrows(IllegalArgumentException.class, () -> parse("workloads: {}"));
        assertThrows(IllegalArgumentException.class, () -> parse(CONFIG + "workloads: {}\n"));
        assertFalse(parse("network: {}").isolated());
        assertFalse(parse(CONFIG.replace("enabled: true", "enabled: false")).isolated());
    }

    @Test
    void rejectsUnknownSettingsAndAmbiguousTargets() {
        for (String input : List.of(CONFIG.replace("enabled: true", "enabeld: true"),
                CONFIG.replace("origins:", "origin:"), CONFIG.replace("target:", "targets:"),
                CONFIG.replace("backend-url: http://payments:8080", "workload: missing"),
                CONFIG.replace("backend-url: http://payments:8080", "backend-url: http://payments:8080\n        workload: missing"))) {
            assertThrows(IllegalArgumentException.class, () -> parse(input), input);
        }
    }

    @Test
    void rejectsDuplicateOriginsAfterNormalizationAndBackendLoops() {
        assertThrows(IllegalArgumentException.class,
                () -> parse(CONFIG.replace("https://uploads.vendor.test:443", "https://api.vendor.test:443")));
        assertThrows(IllegalArgumentException.class,
                () -> parse(CONFIG.replace("http://payments:8080", "http://api.vendor.test:8080")));
        assertThrows(IllegalArgumentException.class,
                () -> parse(CONFIG.replace("http://api.vendor.test]", "http://api.vendor.test:443]")));
    }

    @Test
    void rejectsPathsCredentialsWildcardsAndUnsupportedSchemes() {
        for (String origin : List.of("https://api.vendor.test/path", "https://user:password@api.vendor.test",
                "https://*.vendor.test", "https://api.vendor.test?key=secret", "tcp://api.vendor.test:443",
                "https://api.vendor.test:0", "https://api.vendor.test:65536",
                "https://network.floci.internal", "https://api.vendor.test:9443", "https://192.0.2.1")) {
            assertThrows(IllegalArgumentException.class, () -> parse(CONFIG.replace("https://API.vendor.test", "\"" + origin + "\"")), origin);
        }
    }

    private static NetworkDefinition parse(String yaml) throws Exception {
        return NetworkConfigLoader.parse(new ObjectMapper(new YAMLFactory()).readTree(yaml));
    }
}
