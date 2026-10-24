package io.github.hectorvent.floci.runtime.network;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.core.common.Pem;
import io.github.hectorvent.floci.core.common.dns.NodeDnsServer;
import io.github.hectorvent.floci.runtime.node.FakeNodeApi;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/** The publisher, the node DNS, the network event follow and the runtime file's network section as Floci wires them. */
@QuarkusTest
@TestProfile(NodeNetworkPublisherIntegrationTest.NetworkProfile.class)
class NodeNetworkPublisherIntegrationTest {

    private static final String TOKEN = "integration-token-0123456789";
    private static final Path DIR = Path.of("target", "node-network-publisher-it").toAbsolutePath();
    private static final Path FILE = DIR.resolve("runtime.yaml");
    private static final Path FIXTURE = Path.of("tools/smolvm-pods/tests/fixtures/network-profile.json");
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static FakeNodeApi fake;

    @Inject
    NodeNetworkPublisher publisher;

    @Inject
    NodeDnsServer dns;

    @Inject
    NodeNetworkEvents networkEvents;

    @Test
    void eachNodeGetsAProfileOfTheContractsShape() throws Exception {
        await().atMost(Duration.ofSeconds(15)).until(() -> fake.network() != null && publisher.isApplied("node:dev"));
        JsonNode profile = fake.network();
        JsonNode fixture = MAPPER.readTree(Files.readString(FIXTURE));

        assertThat(fields(profile), is(fields(fixture)));
        assertThat(fields(profile.path("floci")), is(fields(fixture.path("floci"))));
        assertThat(fields(profile.path("routes").get(0)), is(fields(fixture.path("routes").get(0))));
        assertThat(fields(profile.path("issuer")), is(fields(fixture.path("issuer"))));
        assertThat(fields(profile.path("trust")), is(fields(fixture.path("trust"))));
        assertThat(fields(profile.path("allow").get(0)), is(fields(fixture.path("allow").get(0))));

        assertThat(profile.path("isolation").asBoolean(), is(true));
        assertThat(profile.path("floci").path("dns_port").asInt(), is(dns.port()));
        assertThat(texts(profile.path("floci").path("ports")), hasItem("4566"));
        assertThat(texts(profile.path("routes").get(0).path("origins")),
                is(List.of("https://api.vendor.test:443", "https://*.vendor.test:443")));
        assertThat(profile.path("routes").get(0).path("backend").asText(),
                is("payments-simulator.simulators.svc.cluster.local:8080"));
        X509Certificate issuer = Pem.parseCertificate(profile.path("issuer").path("certificate").asText());
        assertThat(issuer.getBasicConstraints(), is(0));
        assertThat(profile.path("issuer").path("subject").asText(), is("Floci route CA"));
        assertThat(profile.path("trust").path("mode").asText(), is("system"));
    }

    @Test
    void aChangedNetworkSectionIsPublishedAgain() throws Exception {
        await().atMost(Duration.ofSeconds(15)).until(() -> fake.network() != null);
        String first = fake.network().path("generation").asText();

        Files.writeString(FILE, runtimeFile(false));
        Files.setLastModifiedTime(FILE, FileTime.from(Instant.now().plusSeconds(120)));
        await().atMost(Duration.ofSeconds(15)).until(() -> !fake.network().path("generation").asText().equals(first));

        assertThat(fake.network().path("isolation").asBoolean(), is(false));
        assertThat(fake.network().path("generation").asText(), is(not(first)));
        Files.writeString(FILE, runtimeFile(true));
        Files.setLastModifiedTime(FILE, FileTime.from(Instant.now().plusSeconds(240)));
        await().atMost(Duration.ofSeconds(15)).until(() -> fake.network().path("isolation").asBoolean());
    }

    @Test
    void eachNodesNetworkEventsAreFollowed() {
        await().atMost(Duration.ofSeconds(15)).until(() -> networkEvents.following("node:dev"));
        long head = networkEvents.cursor("node:dev");

        fake.addNetworkEvent("blocked", "shop/orders", "1.1.1.1", 443, "tcp", 1);

        await().atMost(Duration.ofSeconds(15)).until(() -> networkEvents.cursor("node:dev") == head + 1);
    }

    @Test
    void theNodeDnsAnswersFlociAndRoutedNames() throws Exception {
        await().atMost(Duration.ofSeconds(15)).until(() -> fake.network() != null);
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(3000);
            for (String name : List.of("localhost.floci.io", "pay.vendor.test")) {
                byte[] query = query(name);
                socket.send(new DatagramPacket(query, query.length, InetAddress.getLoopbackAddress(), dns.port()));
                byte[] buffer = new byte[512];
                DatagramPacket answer = new DatagramPacket(buffer, buffer.length);
                socket.receive(answer);
                int end = answer.getLength();
                String address = (buffer[end - 4] & 0xff) + "." + (buffer[end - 3] & 0xff) + "."
                        + (buffer[end - 2] & 0xff) + "." + (buffer[end - 1] & 0xff);
                assertThat(name, address, is(NodeDnsServer.NODE_ADDRESS));
            }
        }
    }

    private static byte[] query(String name) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(new byte[] {0x00, 0x07, 0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0});
        for (String label : name.split("\\.")) {
            out.write(label.length());
            out.writeBytes(label.getBytes(StandardCharsets.US_ASCII));
        }
        out.writeBytes(new byte[] {0, 0, 1, 0, 1});
        return out.toByteArray();
    }

    private static TreeSet<String> fields(JsonNode node) {
        TreeSet<String> names = new TreeSet<>();
        node.properties().forEach(field -> names.add(field.getKey()));
        return names;
    }

    private static List<String> texts(JsonNode list) {
        List<String> out = new ArrayList<>();
        list.forEach(item -> out.add(item.asText()));
        return out;
    }

    private static String runtimeFile(boolean isolation) {
        return """
                version: 1
                nodes:
                  dev:
                    url: %s
                    token-file: node-token
                network:
                  isolation:
                    enabled: %s
                  egress:
                    allow:
                      - cidr: 203.0.113.10/32
                        protocol: tcp
                        ports: [443]
                  routes:
                    payments:
                      origins: [https://api.vendor.test, https://*.vendor.test]
                      target:
                        backend-url: http://payments-simulator.simulators.svc.cluster.local:8080
                """.formatted(fake.url(), isolation);
    }

    public static class NetworkProfile implements QuarkusTestProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            try {
                if (fake == null) {
                    fake = new FakeNodeApi(TOKEN);
                }
                Files.createDirectories(DIR);
                Path token = DIR.resolve("node-token");
                Files.writeString(token, TOKEN);
                Files.setPosixFilePermissions(token, PosixFilePermissions.fromString("rw-------"));
                Files.writeString(FILE, runtimeFile(true));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return Map.of("floci.runtime.config-file", FILE.toString(),
                    "floci.storage.persistent-path", DIR.resolve("data").toString());
        }
    }
}
