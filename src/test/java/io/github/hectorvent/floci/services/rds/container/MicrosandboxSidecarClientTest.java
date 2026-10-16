package io.github.hectorvent.floci.services.rds.container;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.github.hectorvent.floci.config.EmulatorConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MicrosandboxSidecarClientTest {
    private HttpServer server;
    private MicrosandboxSidecarManager manager;
    private MicrosandboxSidecarClient client;
    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicReference<String> response = new AtomicReference<>(
            "{\"exitCode\":0,\"stdout\":\"ok\",\"stderr\":\"\"}");
    private final AtomicReference<JsonNode> request = new AtomicReference<>();
    private final AtomicReference<String> authorization = new AtomicReference<>();

    @BeforeEach
    void setup() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            byte[] body = exchange.getRequestBody().readAllBytes();
            if (body.length > 0) {
                request.set(mapper.readTree(body));
            }
            byte[] bytes = response.get().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (exchange) {
                exchange.getResponseBody().write(bytes);
            }
        });
        server.start();
        EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.services().rds().microsandboxUrl()).thenReturn(Optional.of(
                "http://127.0.0.1:" + server.getAddress().getPort()));
        when(config.services().rds().microsandboxToken()).thenReturn(Optional.of("secret-token"));
        manager = new MicrosandboxSidecarManager(config, mapper);
        client = new MicrosandboxSidecarClient(manager, mapper);
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    @Test
    void sendsArgumentsAndInputSeparatelyWithAuthentication() {
        assertEquals("ok", client.execute(List.of("exec", "db", "--", "psql"), "SELECT 1;\n"));
        assertEquals("Bearer secret-token", authorization.get());
        assertEquals("SELECT 1;\n", request.get().path("input").asText());
        assertEquals("psql", request.get().path("arguments").get(3).asText());
    }

    @Test
    void guestErrorsDoNotExposeCredentials() {
        response.set("{\"exitCode\":1,\"stderr\":\"password=secret\"}");
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> client.command("run", "POSTGRES_PASSWORD=secret"));
        assertFalse(error.getMessage().contains("secret"));
    }

    @Test
    void requiresAnExplicitSuccessfulExitCode() {
        response.set("{\"stdout\":\"apparently successful\"}");
        assertThrows(IllegalStateException.class, () -> client.command("inspect", "db"));
    }

    @Test
    void rejectsWrongHelperAndContract() {
        response.set("{\"status\":\"ok\",\"name\":\"microsandbox\",\"contract\":\"2\"}");
        assertThrows(IllegalStateException.class, manager::isAvailable);
        response.set("{\"status\":\"ok\",\"name\":\"cedar\",\"contract\":\"1\"}");
        assertThrows(IllegalStateException.class, manager::isAvailable);
        response.set("{\"status\":\"ok\",\"name\":\"microsandbox\",\"contract\":\"1\"}");
        assertTrue(manager.isAvailable());
    }
}
