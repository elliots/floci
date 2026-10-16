package io.github.hectorvent.floci.runtime.ondemand;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class HttpReadinessServiceTest {
    private HttpServer server;
    private URI backend;
    private final AtomicInteger probes = new AtomicInteger();
    private boolean alwaysFail;

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/ready", exchange -> {
            int count = probes.incrementAndGet();
            exchange.sendResponseHeaders(alwaysFail || count == 1 ? 503 : 200, -1);
            exchange.close();
        });
        server.start();
        backend = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    @Test
    void waitsForActualBackendReadinessRatherThanSyntheticHealth() throws Exception {
        try (HttpReadiness readiness = new HttpReadiness()) {
            readiness.await(OnDemandTestSupport.definition("orders", backend, List.of()), backend, Duration.ofSeconds(2));
        }
        assertEquals(2, probes.get());
    }

    @Test
    void readinessFailureIncludesTheLastProbeResult() {
        alwaysFail = true;
        try (HttpReadiness readiness = new HttpReadiness()) {
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> readiness.await(OnDemandTestSupport.definition("orders", backend, List.of()),
                        backend, Duration.ofMillis(300)));
            assertTrue(failure.getMessage().contains("orders"));
            assertTrue(failure.getCause().getMessage().contains("503"));
        }
    }

    @Test
    void successfulProbeClosesItsKeepAliveConnectionBeforeTheReadinessServiceCloses() throws Exception {
        try (ServerSocket listener = new ServerSocket(0, 1, new InetSocketAddress("127.0.0.1", 0).getAddress());
                ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
                HttpReadiness readiness = new HttpReadiness()) {
            CompletableFuture<Boolean> disconnected = new CompletableFuture<>();
            workers.execute(() -> {
                try (Socket socket = listener.accept()) {
                    socket.setSoTimeout(3000);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(),
                            StandardCharsets.US_ASCII));
                    String line;
                    while ((line = reader.readLine()) != null && !line.isEmpty()) {
                        // Consume the request headers before returning a persistent HTTP response.
                    }
                    socket.getOutputStream().write(("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n"
                            + "Connection: keep-alive\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
                    socket.getOutputStream().flush();
                    disconnected.complete(socket.getInputStream().read() == -1);
                } catch (Exception e) {
                    disconnected.completeExceptionally(e);
                }
            });
            URI endpoint = URI.create("http://127.0.0.1:" + listener.getLocalPort());
            readiness.await(OnDemandTestSupport.definition("orders", endpoint, List.of()), endpoint,
                    Duration.ofSeconds(2));
            assertTrue(disconnected.get(5, TimeUnit.SECONDS), "Idle readiness socket held the raw proxy lease");
        }
    }
}
