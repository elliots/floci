package io.github.hectorvent.floci.runtime.ondemand;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.List;
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
}
