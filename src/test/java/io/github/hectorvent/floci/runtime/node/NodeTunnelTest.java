package io.github.hectorvent.floci.runtime.node;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NodeTunnelTest {

    private static final String TOKEN = "tunnel-token-0123456789";
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final NodeTunnel tunnel = new NodeTunnel();
    private final BlockingQueue<String> requests = new LinkedBlockingQueue<>();
    private ServerSocket server;
    private volatile BiConsumer<Socket, String> answer;

    @BeforeEach
    void startFakeNode() throws IOException {
        server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread.ofVirtual().start(() -> {
            while (!server.isClosed()) {
                try {
                    Socket socket = server.accept();
                    Thread.ofVirtual().start(() -> serve(socket));
                } catch (IOException e) {
                    return;
                }
            }
        });
    }

    @AfterEach
    void stopFakeNode() throws IOException {
        server.close();
    }

    private void serve(Socket socket) {
        try (socket) {
            InputStream input = socket.getInputStream();
            StringBuilder head = new StringBuilder();
            while (!head.toString().endsWith("\r\n\r\n")) {
                int b = input.read();
                if (b < 0) {
                    return;
                }
                head.append((char) b);
            }
            requests.add(head.toString());
            answer.accept(socket, head.toString());
        } catch (IOException e) {
            requests.add("failed: " + e.getMessage());
        }
    }

    private SmolvmNode node() {
        NodeToken token = new NodeToken.TokenVariable("NODE_TOKEN", name -> TOKEN);
        return new SmolvmNode("node:dev", URI.create("http://127.0.0.1:" + server.getLocalPort()), token,
                SmolvmNode.Kind.DECLARED, List.of());
    }

    @Test
    void anUpgradedTunnelCarriesTheBytesTheTargetSendsFirst() throws Exception {
        answer = (socket, head) -> {
            try {
                OutputStream output = socket.getOutputStream();
                output.write(("HTTP/1.1 101 Switching Protocols\r\nconnection: Upgrade\r\nupgrade: tcp\r\n\r\n"
                        + "greeting").getBytes(StandardCharsets.ISO_8859_1));
                output.flush();
                byte[] question = socket.getInputStream().readNBytes(4);
                output.write(new String(question, StandardCharsets.ISO_8859_1).toUpperCase()
                        .getBytes(StandardCharsets.ISO_8859_1));
                output.flush();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        };

        try (Socket socket = tunnel.open(node(), "172.17.0.2", 5432, TIMEOUT, TIMEOUT)) {
            assertEquals("greeting", new String(socket.getInputStream().readNBytes(8), StandardCharsets.ISO_8859_1));
            socket.getOutputStream().write("ping".getBytes(StandardCharsets.ISO_8859_1));
            assertEquals("PING", new String(socket.getInputStream().readNBytes(4), StandardCharsets.ISO_8859_1));
            assertThat(socket.getSoTimeout(), is(0));
        }
        String request = requests.poll(5, TimeUnit.SECONDS);
        assertThat(request, containsString("GET /v1/tunnel?to=172.17.0.2%3A5432 HTTP/1.1\r\n"));
        assertThat(request, containsString("Authorization: Bearer " + TOKEN + "\r\n"));
        assertThat(request, containsString("Connection: Upgrade\r\n"));
        assertThat(request, containsString("Upgrade: tcp\r\n"));
    }

    @Test
    void anIpv6AddressGoesInBrackets() throws Exception {
        answer = (socket, head) -> write(socket, "HTTP/1.1 101 Switching Protocols\r\n\r\n");

        tunnel.open(node(), "fd00:1::5", 3306, TIMEOUT, TIMEOUT).close();

        assertThat(requests.poll(5, TimeUnit.SECONDS), containsString("to=%5Bfd00%3A1%3A%3A5%5D%3A3306 "));
    }

    @Test
    void aRefusedTunnelFailsWithTheNodesErrorButNotTheToken() {
        String body = "{\"error\":{\"code\":\"forbidden\",\"message\":\"10.1.2.3 is not a container address\","
                + "\"reasons\":[]}}";
        answer = (socket, head) -> write(socket, "HTTP/1.1 403 Forbidden\r\ncontent-type: application/json\r\n"
                + "content-length: " + body.length() + "\r\n\r\n" + body);

        IOException e = assertThrows(IOException.class,
                () -> tunnel.open(node(), "10.1.2.3", 5432, TIMEOUT, TIMEOUT));

        assertThat(e.getMessage(), containsString("node:dev: GET /v1/tunnel?to=10.1.2.3%3A5432: 403 forbidden: "
                + "10.1.2.3 is not a container address"));
        assertThat(e.getMessage(), not(containsString(TOKEN)));
    }

    @Test
    void aNodeThatClosesBeforeItAnswersFails() {
        answer = (socket, head) -> {
        };

        IOException e = assertThrows(IOException.class,
                () -> tunnel.open(node(), "172.17.0.2", 5432, TIMEOUT, TIMEOUT));

        assertThat(e.getMessage(), containsString("closed the tunnel before it answered"));
    }

    @Test
    void aNodeThatDoesNotAnswerInTimeFails() {
        answer = (socket, head) -> sleep(Duration.ofSeconds(2));

        assertThrows(IOException.class,
                () -> tunnel.open(node(), "172.17.0.2", 5432, TIMEOUT, Duration.ofMillis(200)));
    }

    @Test
    void theHeadIsReadUpToItsEndAndNoFurther() throws IOException {
        InputStream input = new ByteArrayInputStream(
                "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 7\r\n\r\npayload".getBytes(StandardCharsets.ISO_8859_1));

        NodeTunnel.Head head = NodeTunnel.readHead(input);

        assertEquals(new NodeTunnel.Head(502, 7), head);
        assertEquals("payload", new String(input.readAllBytes(), StandardCharsets.ISO_8859_1));
        assertThrows(IOException.class, () -> NodeTunnel.readHead(
                new ByteArrayInputStream("SSH-2.0\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1))));
    }

    private static void write(Socket socket, String text) {
        try {
            socket.getOutputStream().write(text.getBytes(StandardCharsets.ISO_8859_1));
            socket.getOutputStream().flush();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
