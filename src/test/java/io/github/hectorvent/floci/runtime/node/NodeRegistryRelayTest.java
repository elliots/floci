package io.github.hectorvent.floci.runtime.node;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NodeRegistryRelayTest {
    @Test
    void concurrentClientsEachGetTheirOwnTunnelAndClosingTheRelayStopsIt() throws Exception {
        try (ServerSocket backend = new ServerSocket(0, 10, InetAddress.getLoopbackAddress())) {
            Thread acceptor = Thread.ofVirtual().start(() -> {
                try {
                    for (int i = 0; i < 2; i++) {
                        Socket socket = backend.accept();
                        Thread.ofVirtual().start(() -> echo(socket));
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            NodeTunnel tunnel = mock(NodeTunnel.class);
            when(tunnel.open(any(), anyString(), anyInt(), any(), any()))
                    .thenAnswer(invocation -> new Socket(InetAddress.getLoopbackAddress(), backend.getLocalPort()));
            SmolvmNode node = new SmolvmNode("fake", URI.create("http://127.0.0.1:1"),
                    NodeToken.file(Path.of("token")), SmolvmNode.Kind.DECLARED, List.of());
            NodeRegistryRelay relay = new NodeRegistryRelay(tunnel, node, "172.17.0.2", 5000);
            int port = relay.port();
            try (relay) {
                CompletableFuture<String> first = CompletableFuture.supplyAsync(() -> exchange(port, "first"));
                CompletableFuture<String> second = CompletableFuture.supplyAsync(() -> exchange(port, "second"));
                assertEquals("first", first.get(5, TimeUnit.SECONDS));
                assertEquals("second", second.get(5, TimeUnit.SECONDS));
            }
            acceptor.join();
            assertThrows(IOException.class, () -> new Socket("127.0.0.1", port));
        }
    }

    private static void echo(Socket socket) {
        try (socket) {
            socket.getInputStream().transferTo(socket.getOutputStream());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String exchange(int port, String message) {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(5_000);
            socket.getOutputStream().write(message.getBytes(StandardCharsets.UTF_8));
            socket.shutdownOutput();
            return new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
