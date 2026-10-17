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
    void wildcardOriginsCoverEveryNameBelowTheirDomainAndExactOnesWin() throws Exception {
        NetworkDefinition network = parse(CONFIG + """
                    front:
                      origins: ["https://*.env.test", "http://*.ENV.test"]
                      target:
                        backend-url: http://front:80
                        preserve-host: true
                    inbound:
                      origins: [https://pay.inbound.env.test]
                      target:
                        backend-url: http://sim:4201
                    deeper:
                      origins: ["https://*.connect.env.test"]
                      target:
                        backend-url: http://connect:80
                """);
        NetworkDefinition.Route front = network.routes().get(1);
        assertEquals(List.of(), front.origins());
        assertEquals(List.of(URI.create("https://env.test:443"), URI.create("http://env.test:80")), front.wildcards());
        assertEquals("front", network.routeFor(URI.create("https://api.env.test:443")).orElseThrow().name());
        assertEquals("front", network.routeFor(URI.create("http://a.b.c.env.test:80")).orElseThrow().name());
        assertEquals("inbound", network.routeFor(URI.create("https://pay.inbound.env.test:443")).orElseThrow().name());
        assertEquals("deeper", network.routeFor(URI.create("https://pay.connect.env.test:443")).orElseThrow().name());
        assertTrue(network.routeFor(URI.create("https://env.test:443")).isEmpty());
        assertTrue(network.routeFor(URI.create("https://api.env.test:8443")).isEmpty());
        assertTrue(network.owns("x.env.test") && !network.owns("env.test") && !network.owns("xenv.test"));
        assertTrue(network.coveredByWildcard("api.env.test"));
        assertFalse(network.coveredByWildcard("pay.inbound.env.test"));
        assertFalse(network.coveredByWildcard("api.vendor.test"));
    }

    @Test
    void rejectsWildcardsThatCoverTooMuch() {
        for (String origin : List.of("https://*.test", "https://*.floci.internal", "https://*.*.vendor.test", "https://a.*.vendor.test")) {
            assertThrows(IllegalArgumentException.class, () -> parse(CONFIG.replace("https://API.vendor.test", "\"" + origin + "\"")), origin);
        }
        assertThrows(IllegalArgumentException.class, () -> parse(CONFIG.replace("http://payments:8080", "http://x.vendor.test:8080")
                .replace("https://API.vendor.test", "\"https://*.vendor.test\"")), "a backend below a wildcard origin loops");
    }

    @Test
    void rejectsPathsCredentialsAndUnsupportedSchemes() {
        for (String origin : List.of("https://api.vendor.test/path", "https://user:password@api.vendor.test",
                "https://api.vendor.test?key=secret", "tcp://api.vendor.test:443",
                "https://api.vendor.test:0", "https://api.vendor.test:65536",
                "https://network.floci.internal", "https://api.vendor.test:9443", "https://192.0.2.1")) {
            assertThrows(IllegalArgumentException.class, () -> parse(CONFIG.replace("https://API.vendor.test", "\"" + origin + "\"")), origin);
        }
    }

    @Test
    void explicitPrivateAndPublicEgressPreserveDefaultDenyAndSimulatorRoutes() throws Exception {
        NetworkDefinition network = parse(CONFIG + """
                  allowed-egress:
                    - destination: 203.0.113.17/32
                      protocol: tcp
                      ports: "443"
                    - destination: 10.20.0.0/16
                      protocol: udp
                      ports: "4000-4010"
                """);
        assertTrue(network.isolated());
        assertEquals(1, network.routes().size());
        assertEquals(List.of(new NetworkDefinition.Egress("203.0.113.17/32", "tcp", "443"),
                new NetworkDefinition.Egress("10.20.0.0/16", "udp", "4000-4010")), network.allowedEgress());
        assertFalse(network.owns("unlisted.public.test"));
        assertEquals("53", new NetworkDefinition.Egress("203.0.113.17/32", "udp", "53").ports());
        assertEquals("853", new NetworkDefinition.Egress("203.0.113.17/32", "tcp", "853-853").ports());
        assertEquals(1, parse("network: {allowed-egress: [{destination: 198.51.100.0/24, protocol: tcp, ports: '80-443'}]}")
                .allowedEgress().size());
    }

    @Test
    void egressRejectsAmbiguousDestinationsAndInjectionBeforeRuleCompilation() {
        for (String destination : List.of("203.0.113.17", "203.0.113.17/24", "203.0.113.256/32", "203.0.113.17/33",
                "0.0.0.0/0", "0.0.0.0/8", "127.0.0.1/32", "126.0.0.0/7", "169.254.0.0/16",
                "224.0.0.0/4", "240.0.0.0/4", "255.255.255.255/32", "::/1", "host", "public.test", "203.0.113.17/32; accept")) {
            assertThrows(IllegalArgumentException.class, () -> new NetworkDefinition.Egress(destination, "tcp", "443"), destination);
        }
        for (String ports : List.of("", "0", "65536", "443-80", "1-65536", "80,443", "443 accept", "0443")) {
            assertThrows(IllegalArgumentException.class, () -> new NetworkDefinition.Egress("203.0.113.17/32", "tcp", ports), ports);
        }
        for (String protocol : List.of("all", "icmp", "TCP", "tcp;accept")) {
            assertThrows(IllegalArgumentException.class, () -> new NetworkDefinition.Egress("203.0.113.17/32", protocol, "443"), protocol);
        }
        assertThrows(IllegalArgumentException.class, () -> parse("network: {allowed-egress: {destination: 203.0.113.17/32}}"));
        assertThrows(IllegalArgumentException.class,
                () -> parse("network: {allowed-egress: [{destination: 203.0.113.17/32, protocol: tcp, ports: '443', hostname: test}]}"));
    }

    private static NetworkDefinition parse(String yaml) throws Exception {
        return NetworkConfigLoader.parse(new ObjectMapper(new YAMLFactory()).readTree(yaml));
    }
}
