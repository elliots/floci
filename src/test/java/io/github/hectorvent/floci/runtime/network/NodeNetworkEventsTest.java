package io.github.hectorvent.floci.runtime.network;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.runtime.node.FakeNodeApi;
import io.github.hectorvent.floci.runtime.node.NodeToken;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import io.github.hectorvent.floci.testutil.LogCapture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

class NodeNetworkEventsTest {

    private static final String TOKEN = "network-events-token-0123456789";

    @TempDir
    Path dir;

    private FakeNodeApi fake;
    private SmolvmNode node;
    private SmolvmNodeClient client;
    private NodeNetworkEvents events;

    @BeforeEach
    void start() throws IOException {
        fake = new FakeNodeApi(TOKEN);
        fake.keepalive(Duration.ofMillis(50));
        Path token = dir.resolve("node-token");
        Files.writeString(token, TOKEN);
        Files.setPosixFilePermissions(token, PosixFilePermissions.fromString("rw-------"));
        node = fake.node("dev", NodeToken.file(token));
        client = new SmolvmNodeClient(new ObjectMapper());
        events = new NodeNetworkEvents(null, client, Executors.newSingleThreadScheduledExecutor());
    }

    @AfterEach
    void stop() {
        events.close();
        fake.close();
    }

    @Test
    void eachBlockedAttemptIsLoggedOnceFromTheHeadOn() {
        fake.addNetworkEvent("blocked", "shop/orders", "8.8.8.8", 53, "udp", 1);

        List<LogRecord> records = LogCapture.capture(NodeNetworkEvents.class, () -> {
            events.follow(List.of(node));
            await().atMost(Duration.ofSeconds(5)).until(() -> events.following(node.name()));
            assertThat(events.cursor(node.name()), is(1L));

            fake.addNetworkEvent("blocked", "shop/orders", "93.184.215.14", 443, "tcp", 3, "api.example.com");
            fake.addNetworkEvent("relayed", "shop/orders", "api.vendor.test", 443, "https", 1);
            await().atMost(Duration.ofSeconds(5)).until(() -> events.cursor(node.name()) == 3);

            fake.endFollows();
            fake.addNetworkEvent("blocked", "shop/web", "1.1.1.1", 80, "tcp", 1);
            await().atMost(Duration.ofSeconds(10)).until(() -> events.cursor(node.name()) == 4);
        });

        List<String> blocked = records.stream()
                .filter(record -> record.getLevel().equals(Level.INFO))
                .map(NodeNetworkEventsTest::text)
                .filter(text -> text.contains(" blocked "))
                .toList();
        assertThat(blocked, contains(
                "node:dev blocked shop/orders: tcp to 93.184.215.14:443, count 3, names api.example.com",
                "node:dev blocked shop/web: tcp to 1.1.1.1:80, count 1"));
        List<String> follows = fake.requests().stream()
                .filter(request -> request.query() != null && request.query().startsWith("follow=true"))
                .map(FakeNodeApi.Request::query)
                .toList();
        assertThat(follows, contains("follow=true&cursor=1", "follow=true&cursor=3"));
    }

    @Test
    void aNodeThatLeavesIsNoLongerFollowed() {
        events.follow(List.of(node));
        await().atMost(Duration.ofSeconds(5)).until(() -> events.following(node.name()));

        events.follow(List.of());

        assertThat(events.following(node.name()), is(false));
        assertThat(events.cursor(node.name()), is(-1L));
    }

    private static String text(LogRecord record) {
        Object[] parameters = record.getParameters();
        return parameters == null ? record.getMessage() : MessageFormat.format(record.getMessage(), parameters);
    }
}
