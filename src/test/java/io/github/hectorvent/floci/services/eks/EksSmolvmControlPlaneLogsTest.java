package io.github.hectorvent.floci.services.eks;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.runtime.node.FakeNodeApi;
import io.github.hectorvent.floci.runtime.node.NodeToken;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class EksSmolvmControlPlaneLogsTest {

    private static final String TOKEN = "node-token-0123456789abcdef";
    private static final String NODE_ID = "floci-node-0123456789abcdef";

    @TempDir
    Path dir;

    private FakeNodeApi fake;
    private SmolvmNodeClient client;
    private SmolvmNode node;
    private final ContainerLogStreamer writer = mock(ContainerLogStreamer.class);
    private final List<String> shipped = new CopyOnWriteArrayList<>();
    private EksSmolvmControlPlaneLogs logs;

    @BeforeEach
    void start() throws IOException {
        fake = new FakeNodeApi(TOKEN);
        fake.keepalive(Duration.ofMillis(50));
        Path token = dir.resolve("token");
        Files.writeString(token, TOKEN + "\n");
        Files.setPosixFilePermissions(token, PosixFilePermissions.fromString("rw-------"));
        node = fake.node("floci", NodeToken.file(token));
        client = new SmolvmNodeClient(new ObjectMapper());
        doAnswer(call -> shipped.add(call.getArgument(2) + " " + call.getArgument(4)))
                .when(writer).streamToCloudWatchLogsForAccount(eq("123456789012"), eq("/aws/eks/orders/cluster"),
                        anyString(), eq("us-west-2"), anyString());
        logs = new EksSmolvmControlPlaneLogs(client, writer, Duration.ofMillis(50));
    }

    @AfterEach
    void stop() {
        logs.stop();
        fake.close();
    }

    @Test
    void shipsTheApiAndAuditLogsToTheirStreamsAndFollowsAgainAfterTheNodeEndsAFollow() throws Exception {
        String id = EksSmolvmControlPlaneLogs.streamId(NODE_ID);
        logs.attach("key", "orders", "123456789012", "us-west-2", node, NODE_ID, List.of("api", "audit"));
        await().atMost(Duration.ofSeconds(5)).until(() -> fake.requests().size() == 2);
        Thread.sleep(200);

        fake.appendLog("k3s", "I1025 apiserver started");
        fake.appendLog("kube-apiserver-audit", "{\"kind\":\"Event\"}");
        await().atMost(Duration.ofSeconds(5)).until(() -> shipped.size() == 2);

        fake.endFollows();
        await().atMost(Duration.ofSeconds(5)).until(() -> fake.requests().size() == 4);
        Thread.sleep(200);
        fake.appendLog("k3s", "I1025 after the drop");
        await().atMost(Duration.ofSeconds(5)).until(() -> shipped.size() == 3);

        assertThat(shipped, containsInAnyOrder("kube-apiserver-" + id + " I1025 apiserver started",
                "kube-apiserver-audit-" + id + " {\"kind\":\"Event\"}",
                "kube-apiserver-" + id + " I1025 after the drop"));
        verify(writer, atLeastOnce()).ensureLogGroupAndStreamForAccount(any(), eq("/aws/eks/orders/cluster"),
                eq("kube-apiserver-audit-" + id), any());
        assertEquals(32, id.length());
    }

    @Test
    void closingStopsShippingAndAnAttachWithoutTypesFollowsNothing() throws Exception {
        logs.attach("key", "orders", "123456789012", "us-west-2", node, NODE_ID, List.of("api"));
        await().atMost(Duration.ofSeconds(5)).until(() -> fake.requests().size() == 1);

        logs.attach("key", "orders", "123456789012", "us-west-2", node, NODE_ID, List.of());
        Thread.sleep(300);
        fake.appendLog("k3s", "not shipped");
        Thread.sleep(300);

        assertEquals(List.of(), shipped);
        assertEquals(1, fake.requests().size());
    }
}
