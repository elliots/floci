package io.github.hectorvent.floci.runtime.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import javax.net.ssl.SSLSocketFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;

/**
 * TCP connections to the containers on a node's Docker networks, through its node service's
 * tunnel ({@code GET /v1/tunnel?to=IP:PORT}, tools/smolvm-pods/docs/configuration.md): one
 * connection to the API that the node service upgrades to raw TCP once it has connected to the
 * container. The node makes that connection, so it wakes a sleeping container; the answer comes
 * once the container is awake.
 */
@ApplicationScoped
public class NodeTunnel {

    private static final Logger LOG = Logger.getLogger(NodeTunnel.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    /** The longest response head the tunnel reads before it gives up. */
    static final int MAX_HEAD_BYTES = 16 * 1024;

    /** The longest error body the tunnel reads. */
    private static final int MAX_ERROR_BYTES = 16 * 1024;

    /**
     * Opens a TCP connection to {@code host:port} in the node.
     *
     * @param connectTimeout how long the connection to the node service may take
     * @param answerTimeout  how long the node service may take to answer, which includes the wake
     *                       of a sleeping container
     * @return the socket, which from now on carries the container's bytes; it has no read timeout
     * @throws IOException when the node service cannot be reached or refuses the tunnel; the
     *                     message names the node's error code and never holds the token
     */
    public Socket open(SmolvmNode node, String host, int port, Duration connectTimeout, Duration answerTimeout)
            throws IOException {
        URI base = node.baseUrl();
        boolean tls = "https".equalsIgnoreCase(base.getScheme());
        int apiPort = base.getPort() > 0 ? base.getPort() : tls ? 443 : 80;
        String to = (host.indexOf(':') >= 0 ? "[" + host + "]" : host) + ":" + port;
        String path = "/v1/tunnel?to=" + SmolvmNodeClient.encode(to);
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(base.getHost(), apiPort), (int) connectTimeout.toMillis());
            socket.setTcpNoDelay(true);
            Socket stream = tls
                    ? ((SSLSocketFactory) SSLSocketFactory.getDefault()).createSocket(socket, base.getHost(), apiPort, true)
                    : socket;
            stream.setSoTimeout((int) Math.max(1, answerTimeout.toMillis()));
            String request = "GET " + path + " HTTP/1.1\r\n"
                    + "Host: " + base.getHost() + ":" + apiPort + "\r\n"
                    + "Authorization: Bearer " + token(node) + "\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Upgrade: tcp\r\n\r\n";
            OutputStream output = stream.getOutputStream();
            output.write(request.getBytes(StandardCharsets.ISO_8859_1));
            output.flush();
            Head head = readHead(stream.getInputStream());
            if (head.status() == 101) {
                stream.setSoTimeout(0);
                LOG.debugv("Opened a tunnel to {0} on node {1}", to, node.name());
                return stream;
            }
            throw new IOException(node.name() + ": GET " + path + ": " + head.status()
                    + error(stream.getInputStream(), head));
        } catch (IOException | RuntimeException e) {
            closeQuietly(socket);
            throw e;
        }
    }

    private static String token(SmolvmNode node) throws IOException {
        try {
            return node.token().read();
        } catch (IllegalStateException e) {
            throw new IOException(node.name() + ": " + e.getMessage(), e);
        }
    }

    /** A response head: its status and its {@code Content-Length}, or -1 without one. */
    record Head(int status, long contentLength) {
    }

    /**
     * Reads a response head one byte at a time, so that the bytes after it, which belong to the
     * container once the connection is upgraded, stay in the stream.
     */
    static Head readHead(InputStream input) throws IOException {
        ByteArrayOutputStream head = new ByteArrayOutputStream();
        int matched = 0;
        while (matched < 4) {
            int b = input.read();
            if (b < 0) {
                throw new IOException("the node service closed the tunnel before it answered");
            }
            head.write(b);
            if (head.size() > MAX_HEAD_BYTES) {
                throw new IOException("the node service's answer has a head over " + MAX_HEAD_BYTES + " bytes");
            }
            matched = switch (matched) {
                case 0, 2 -> b == '\r' ? matched + 1 : 0;
                case 1, 3 -> b == '\n' ? matched + 1 : b == '\r' ? 1 : 0;
                default -> 0;
            };
        }
        String[] lines = head.toString(StandardCharsets.ISO_8859_1).split("\r\n");
        String[] statusLine = lines[0].split(" ", 3);
        if (statusLine.length < 2 || !statusLine[0].startsWith("HTTP/1.")) {
            throw new IOException("the node service's answer is not HTTP: " + lines[0]);
        }
        int status;
        try {
            status = Integer.parseInt(statusLine[1]);
        } catch (NumberFormatException e) {
            throw new IOException("the node service's answer has no status: " + lines[0], e);
        }
        long contentLength = -1;
        for (int i = 1; i < lines.length; i++) {
            int colon = lines[i].indexOf(':');
            if (colon > 0 && lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT).equals("content-length")) {
                try {
                    contentLength = Long.parseLong(lines[i].substring(colon + 1).trim());
                } catch (NumberFormatException e) {
                    LOG.debugv("The node service's answer has a Content-Length that is not a number: {0}", lines[i]);
                }
            }
        }
        return new Head(status, contentLength);
    }

    /** The node's error code and message from the body of an error answer, as " CODE: MESSAGE". */
    private static String error(InputStream input, Head head) {
        if (head.contentLength() <= 0) {
            return "";
        }
        try {
            byte[] body = input.readNBytes((int) Math.min(head.contentLength(), MAX_ERROR_BYTES));
            JsonNode error = JSON.readTree(body).path("error");
            String code = NodePod.text(error, "code");
            String message = NodePod.text(error, "message");
            return (code == null ? "" : " " + code) + (message == null ? "" : ": " + message);
        } catch (IOException e) {
            LOG.debugv("The node service's error answer to a tunnel cannot be read: {0}", e.getMessage());
            return "";
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            LOG.debugv("Closing a failed tunnel: {0}", e.getMessage());
        }
    }
}
