package io.github.hectorvent.floci.services.lambda.launcher.kubernetes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class KubernetesDeploymentScaleTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private HttpServer server;
    private KubernetesApiClient client;
    private final AtomicInteger reads = new AtomicInteger();
    private final AtomicInteger writes = new AtomicInteger();
    private final AtomicReference<JsonNode> scale = new AtomicReference<>();
    private boolean conflict;

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/apis/apps/v1/namespaces/integrations/deployments/orders", exchange -> {
            assertEquals("Bearer rotating-token", exchange.getRequestHeaders().getFirst("Authorization"));
            int status = 200;
            String response;
            if (exchange.getRequestURI().getPath().endsWith("/scale")) {
                if ("PUT".equals(exchange.getRequestMethod())) {
                    scale.set(mapper.readTree(exchange.getRequestBody()));
                    int attempt = writes.incrementAndGet();
                    status = conflict && attempt == 1 ? 409 : 200;
                    response = scale.get().toString();
                } else {
                    int version = reads.incrementAndGet();
                    response = """
                            {"apiVersion":"autoscaling/v1","kind":"Scale",
                             "metadata":{"name":"orders","namespace":"integrations","resourceVersion":"%s"},
                             "spec":{"replicas":0},"status":{"replicas":0}}
                            """.formatted(version);
                }
            } else {
                response = "{\"metadata\":{\"name\":\"orders\"},\"spec\":{\"replicas\":0}}";
            }
            byte[] body = response.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        client = new KubernetesApiClient(URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
                HttpClient.newHttpClient(), () -> "rotating-token");
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    @Test
    void scalesUsingTheScaleSubresourceAndRetainsResourceVersion() {
        client.scaleDeployment("integrations", "orders", 2);
        assertEquals(2, scale.get().path("spec").path("replicas").asInt());
        assertEquals("1", scale.get().path("metadata").path("resourceVersion").asText());
        assertEquals("autoscaling/v1", scale.get().path("apiVersion").asText());
        assertEquals(1, writes.get());
        assertEquals("orders", client.getDeployment("integrations", "orders").orElseThrow()
                .path("metadata").path("name").asText());
    }

    @Test
    void retriesVersionConflictsWithAFreshScaleRecord() {
        conflict = true;
        client.scaleDeployment("integrations", "orders", 0);
        assertEquals(2, reads.get());
        assertEquals(2, writes.get());
        assertEquals("2", scale.get().path("metadata").path("resourceVersion").asText());
    }
}
