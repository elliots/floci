package io.github.hectorvent.floci.runtime.node;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TunnelRelayTest {

    /** Answers each line it reads in upper case, until the client closes. */
    private static void echo(ServerSocket server) {
        try (Socket socket = server.accept()) {
            InputStream input = socket.getInputStream();
            byte[] buffer = new byte[64];
            int read;
            while ((read = input.read(buffer)) >= 0) {
                socket.getOutputStream().write(new String(buffer, 0, read, StandardCharsets.US_ASCII).toUpperCase()
                        .getBytes(StandardCharsets.US_ASCII));
            }
        } catch (IOException ignored) {
            // The test closed the far end; nothing is left to answer.
        }
    }

    @Test
    void theFirstConnectionGoesThroughTheTunnelAndNoOtherIsTaken() throws Exception {
        AtomicInteger opened = new AtomicInteger();
        try (ServerSocket far = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            Thread.ofVirtual().start(() -> echo(far));
            TunnelRelay relay = TunnelRelay.open("the database", Duration.ofSeconds(5), () -> {
                opened.incrementAndGet();
                return new Socket(InetAddress.getLoopbackAddress(), far.getLocalPort());
            });

            try (Socket client = new Socket(InetAddress.getLoopbackAddress(), relay.port())) {
                client.setSoTimeout(5000);
                client.getOutputStream().write("select 1".getBytes(StandardCharsets.US_ASCII));
                assertEquals("SELECT 1", new String(client.getInputStream().readNBytes(8), StandardCharsets.US_ASCII));

                assertThrows(ConnectException.class, () -> new Socket(InetAddress.getLoopbackAddress(), relay.port()));
            }
            assertEquals(1, opened.get());
        }
    }

    @Test
    void aRelayThatNoClientUsesCloses() throws Exception {
        AtomicInteger opened = new AtomicInteger();
        TunnelRelay relay = TunnelRelay.open("the database", Duration.ofMillis(100), () -> {
            opened.incrementAndGet();
            return new Socket();
        });
        int port = relay.port();

        await().pollDelay(Duration.ofMillis(500)).atMost(5, TimeUnit.SECONDS).until(() -> {
            try (Socket ignored = new Socket(InetAddress.getLoopbackAddress(), port)) {
                return false;
            } catch (ConnectException closed) {
                return true;
            }
        });
        assertEquals(0, opened.get());
    }

    @Test
    void aTunnelThatCannotOpenClosesTheClient() throws Exception {
        TunnelRelay relay = TunnelRelay.open("the database", Duration.ofSeconds(5), () -> {
            throw new IOException("the node refused the tunnel");
        });

        try (Socket client = new Socket(TunnelRelay.HOST, relay.port())) {
            client.setSoTimeout(5000);
            assertEquals(-1, client.getInputStream().read());
        }
        assertEquals("the node refused the tunnel", relay.failure().getMessage());
    }

    @Test
    void aTunnelThatFailsAtTheNodeTellsWhy() throws Exception {
        TunnelRelay relay = TunnelRelay.open("the database", Duration.ofSeconds(5), () -> {
            throw new IllegalStateException("the smolvm-pods node is still being created");
        });

        try (Socket client = new Socket(TunnelRelay.HOST, relay.port())) {
            client.setSoTimeout(5000);
            assertEquals(-1, client.getInputStream().read());
        }
        assertInstanceOf(IllegalStateException.class, relay.failure());
    }
}
