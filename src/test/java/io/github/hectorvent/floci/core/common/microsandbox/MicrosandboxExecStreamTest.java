package io.github.hectorvent.floci.core.common.microsandbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MicrosandboxExecStreamTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final MicrosandboxOperationJournal journal = mock(MicrosandboxOperationJournal.class);
    private final BlockingQueue<JsonNode> frames = new LinkedBlockingQueue<>();
    private final CompletableFuture<ServerWebSocket> connection = new CompletableFuture<>();
    private Vertx vertx;
    private HttpServer server;
    private MicrosandboxClient client;

    @BeforeEach
    void setup() throws Exception {
        vertx = Vertx.vertx();
        server = vertx.createHttpServer().requestHandler(request -> request.toWebSocket().onSuccess(socket -> {
            socket.textMessageHandler(text -> {
                try {
                    frames.add(mapper.readTree(text));
                } catch (Exception failure) {
                    connection.completeExceptionally(failure);
                }
            });
            connection.complete(socket);
        }).onFailure(connection::completeExceptionally)).listen(0, "127.0.0.1")
                .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        when(journal.begin(anyString())).thenReturn("operation");
        client = new MicrosandboxClient("http://127.0.0.1:" + server.actualPort(), "private-token", mapper, journal);
    }

    @AfterEach
    void close() throws Exception {
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    void authenticatesStreamsBinaryInputAndOutputAndResizesBeforeConfirmedExit() throws Exception {
        CompletableFuture<byte[]> output = new CompletableFuture<>();
        CompletableFuture<Integer> exit = new CompletableFuture<>();
        CompletableFuture<Throwable> failure = new CompletableFuture<>();
        client.requireNetworkPolicy("policy-sha256");
        MicrosandboxClient.ExecStream stream = client.streamExec(Duration.ofSeconds(5),
                List.of("exec", "guest", "--", "sh"), true, output::complete, exit::complete, failure::complete);
        ServerWebSocket socket = connection.get(5, TimeUnit.SECONDS);
        assertEquals("Bearer private-token", socket.headers().get("Authorization"));
        assertEquals("policy-sha256", socket.headers().get("X-Floci-Network-Policy"));
        JsonNode start = frames.poll(5, TimeUnit.SECONDS);
        assertNotNull(start);
        assertTrue(start.path("tty").asBoolean());
        byte[] bytes = new byte[]{0, 1, 2, (byte) 255};
        stream.write(bytes);
        assertArrayEquals(bytes, Base64.getDecoder().decode(frames.poll(5, TimeUnit.SECONDS).path("data").asText()));
        stream.resize(40, 100);
        assertEquals(40, frames.poll(5, TimeUnit.SECONDS).path("rows").asInt());
        stream.endInput();
        assertEquals("eof", frames.poll(5, TimeUnit.SECONDS).path("type").asText());
        ObjectNode data = mapper.createObjectNode().put("type", "stdout").put("data", Base64.getEncoder().encodeToString(bytes));
        socket.writeTextMessage(data.toString());
        assertArrayEquals(bytes, output.get(5, TimeUnit.SECONDS));
        socket.writeTextMessage("{\"type\":\"exit\",\"exitCode\":0}");
        assertEquals(0, exit.get(5, TimeUnit.SECONDS));
        client.awaitQuiescence(Duration.ofSeconds(1));
        verify(journal).complete("operation");
        assertFalse(failure.isDone());
        stream.close();
    }

    @Test
    void disconnectNeverInventsAnExitStatusOrClearsTheMutationFence() throws Exception {
        CompletableFuture<Integer> exit = new CompletableFuture<>();
        CompletableFuture<Throwable> failure = new CompletableFuture<>();
        client.streamExec(Duration.ofSeconds(5), List.of("exec", "guest", "--", "sh"), false,
                ignored -> {}, exit::complete, failure::complete);
        assertNotNull(frames.poll(5, TimeUnit.SECONDS));
        connection.get().close();
        assertNotNull(failure.get(5, TimeUnit.SECONDS));
        assertFalse(exit.isDone());
        assertThrows(IllegalStateException.class, () -> client.awaitQuiescence(Duration.ofMillis(10)));
        verify(journal).lost("operation");
        verify(journal, never()).complete(anyString());
    }

    @Test
    void cancellationWaitsForTheHelpersExitAcknowledgement() throws Exception {
        CompletableFuture<Integer> exit = new CompletableFuture<>();
        MicrosandboxClient.ExecStream stream = client.streamExec(Duration.ofSeconds(5),
                List.of("exec", "guest", "--", "sh"), false, ignored -> {}, exit::complete, ignored -> {});
        assertNotNull(frames.poll(5, TimeUnit.SECONDS));
        stream.close();
        assertEquals("cancel", frames.poll(5, TimeUnit.SECONDS).path("type").asText());
        verify(journal, never()).complete(anyString());
        connection.get().writeTextMessage("{\"type\":\"exit\",\"exitCode\":137}");
        assertEquals(137, exit.get(5, TimeUnit.SECONDS));
        verify(journal).complete("operation");
    }
}
