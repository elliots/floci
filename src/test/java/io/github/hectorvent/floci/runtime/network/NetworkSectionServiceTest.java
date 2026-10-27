package io.github.hectorvent.floci.runtime.network;

import io.github.hectorvent.floci.runtime.network.NetworkDefinitions.Allow;
import io.github.hectorvent.floci.runtime.network.NetworkDefinitions.BackendProtocol;
import io.github.hectorvent.floci.runtime.network.NetworkDefinitions.Origin;
import io.github.hectorvent.floci.runtime.network.NetworkDefinitions.Protocol;
import io.github.hectorvent.floci.runtime.network.NetworkDefinitions.Route;
import io.github.hectorvent.floci.runtime.network.NetworkDefinitions.TrustMode;
import io.github.hectorvent.floci.runtime.node.RuntimeFile;
import io.github.hectorvent.floci.runtime.node.RuntimeFileException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NetworkSectionServiceTest {

    @TempDir
    Path dir;

    private final FlociPortInventory ports = new FlociPortInventory(List.of(
            new FlociPortInventory.PortRange(4566, 4566, "port"),
            new FlociPortInventory.PortRange(5672, 5699, "services.amazonmq.amqp-host-port-base")));
    private final NetworkSection section = new NetworkSection(ports, List.of("localhost.floci.io", "cluster.local",
            "host.docker.internal", "floci.example"));

    @Test
    void readsEveryField() throws IOException {
        NetworkDefinitions network = read("""
                version: 1
                network:
                  isolation:
                    enabled: true
                  floci:
                    extra-ports: [9000]
                  trust:
                    mode: env
                  egress:
                    allow:
                      - cidr: 203.0.113.10
                        protocol: tcp
                        ports: [443]
                      - cidr: 198.51.100.0/24
                        protocol: any
                  routes:
                    payments:
                      origins:
                        - https://api.vendor.test
                        - https://*.vendor.test
                        - http://api.vendor.test:8000/
                      target:
                        backend-url: http://payments-simulator.simulators.svc.cluster.local:8080
                        protocol: h2c
                    mac-sim:
                      origins: [https://maps.other.test:8443]
                      target:
                        backend-url: http://host.floci.internal:9090
                """);

        assertThat(network, is(new NetworkDefinitions(true, List.of(9000), TrustMode.ENV,
                List.of(new Allow("203.0.113.10/32", Protocol.TCP, List.of(443)),
                        new Allow("198.51.100.0/24", Protocol.ANY, List.of())),
                List.of(new Route("payments", List.of(new Origin(true, "api.vendor.test", false, 443),
                                new Origin(true, "vendor.test", true, 443),
                                new Origin(false, "api.vendor.test", false, 8000)),
                                "payments-simulator.simulators.svc.cluster.local", 8080, BackendProtocol.H2C),
                        new Route("mac-sim", List.of(new Origin(true, "maps.other.test", false, 8443)),
                                "host.floci.internal", 9090, BackendProtocol.HTTP1)))));
        assertThat(network.origins().get(1).wire(), is("https://*.vendor.test:443"));
        assertThat(NodeNetworkProfile.permittedNames(network), is(List.of("api.vendor.test", "maps.other.test",
                "vendor.test")));
    }

    @Test
    void aFileWithoutTheSectionIsOpen() throws IOException {
        assertThat(read("version: 1\n"), is(NetworkDefinitions.NONE));
    }

    @Test
    void eachBadFieldIsNamed() {
        assertProblem("isolation: {enabled: yes-please}", "network.isolation.enabled: must be true or false");
        assertProblem("gateway: {}", "network.gateway: unknown field");
        assertProblem("trust: {mode: always}", "network.trust.mode: must be system, env or off");
        assertProblem("egress: {allow: [{cidr: 203.0.113.0/40}]}", "network.egress.allow.0.cidr: has a bad prefix length");
        assertProblem("egress: {allow: [{cidr: 203.0.113.1, protocol: icmp}]}",
                "network.egress.allow.0.protocol: must be tcp, udp or any");
        assertProblem("floci: {extra-ports: [70000]}", "network.floci.extra-ports.0: must be a port from 1 to 65535");
        assertRoute("origins: [ftp://a.vendor.test]", "network.routes.r.origins.0: must start with http:// or https://");
        assertRoute("origins: [https://a.vendor.test:53]", "network.routes.r.origins.0: port 53 is DNS");
        assertRoute("origins: [https://a.vendor.test:5680]",
                "network.routes.r: origin port 5680 is a Floci port (services.amazonmq.amqp-host-port-base)");
        assertRoute("origins: [https://a.*.vendor.test]", "network.routes.r.origins.0: must name a DNS host");
        assertRoute("origins: [https://10.0.0.1]", "network.routes.r.origins.0: must name a DNS host");
        assertRoute("origins: [https://user:pw@a.vendor.test]", "network.routes.r.origins.0: must not hold credentials");
        assertRoute("origins: [https://a.vendor.test/pay]", "network.routes.r.origins.0: must have no path");
        assertRoute("origins: [https://orders.localhost.floci.io]",
                "network.routes.r.origins.0: host orders.localhost.floci.io is a Floci or cluster name");
        assertRoute("origins: [https://a.vendor.test:8000, http://b.vendor.test:8000]",
                "network.routes.r.origins.1: port 8000 cannot serve both http and https origins");
        assertRoute("origins: [https://a.vendor.test, https://a.vendor.test:443]",
                "network.routes.r.origins.1: https://a.vendor.test:443 is routed by network.routes.r too");
    }

    @Test
    void aBackendMustBeAServiceAnAddressOrTheFlociHost() {
        assertTarget("backend-url: https://sim.sims.svc.cluster.local:8443", "backend-url must use http");
        assertTarget("backend-url: http://orders.localhost.floci.io:8080", "backend-url host orders.localhost.floci.io "
                + "is a Floci name");
        assertTarget("backend-url: http://simulator:8080", "backend-url host must be a Service name");
        assertTarget("backend-url: http://8.8.8.8:80", "backend-url host must be a Service name");
        assertTarget("backend-url: http://sim.sims.svc.cluster.local", "must name the backend's port");
    }

    @Test
    void aPrivateAddressIsABackend() throws IOException {
        NetworkDefinitions network = read("""
                version: 1
                network:
                  routes:
                    r:
                      origins: [https://a.vendor.test]
                      target: {backend-url: 'http://10.100.20.30:8080'}
                """);
        assertThat(network.routes().get(0).backendHost(), is("10.100.20.30"));
    }

    private void assertRoute(String route, String message) {
        RuntimeFileException e = assertThrows(RuntimeFileException.class, () -> read("version: 1\nnetwork:\n"
                + "  routes:\n    r:\n      " + route + "\n      target: {backend-url: "
                + "'http://sim.sims.svc.cluster.local:8080'}\n"));
        assertThat(e.getMessage(), e.getMessage().startsWith(message), is(true));
    }

    private void assertTarget(String target, String message) {
        RuntimeFileException e = assertThrows(RuntimeFileException.class, () -> read("version: 1\nnetwork:\n"
                + "  routes:\n    r:\n      origins: [https://a.vendor.test]\n      target: "
                + (target.startsWith("{") ? target : "{" + target + "}") + "\n"));
        assertThat(e.getMessage(), e.getMessage().contains(message), is(true));
    }

    private void assertProblem(String body, String message) {
        RuntimeFileException e = assertThrows(RuntimeFileException.class,
                () -> read("version: 1\nnetwork:\n  " + body + "\n"));
        assertThat(e.getMessage(), e.getMessage().startsWith(message), is(true));
    }

    private NetworkDefinitions read(String content) throws IOException {
        Path file = dir.resolve("runtime.yaml");
        Files.writeString(file, content);
        return RuntimeFile.read(file, List.of(section)).section(section);
    }
}
