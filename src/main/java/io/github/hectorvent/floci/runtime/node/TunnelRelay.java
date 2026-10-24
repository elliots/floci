package io.github.hectorvent.floci.runtime.node;

import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;

/**
 * A port on {@code 127.0.0.1} that carries one connection through a tunnel, for a client that can
 * only connect to a host and a port, such as a JDBC driver: the first connection to the port gets
 * the socket that {@link Opener} opens, such as one through {@link NodeTunnel}, and the bytes go
 * both ways until either side closes. The port takes no other connection, and closes when no
 * connection comes within the accept timeout.
 */
public final class TunnelRelay implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(TunnelRelay.class);
    private static final int BUFFER = 16 * 1024;

    /** Opens the far end of the relay. */
    @FunctionalInterface
    public interface Opener {
        Socket open() throws IOException;
    }

    /** The address the relay listens on. */
    public static final String HOST = "127.0.0.1";

    private final ServerSocket server;
    private final Opener opener;
    private final String name;
    private volatile Exception failure;

    private TunnelRelay(ServerSocket server, Opener opener, String name) {
        this.server = server;
        this.opener = opener;
        this.name = name;
    }

    /**
     * Opens a relay on a free port of {@link #HOST}, which waits up to {@code acceptTimeout} for
     * its connection.
     *
     * @param name what the relay leads to, for the logs
     */
    public static TunnelRelay open(String name, Duration acceptTimeout, Opener opener) throws IOException {
        ServerSocket server = new ServerSocket();
        server.bind(new InetSocketAddress(InetAddress.getByName(HOST), 0), 1);
        server.setSoTimeout((int) Math.max(1, acceptTimeout.toMillis()));
        TunnelRelay relay = new TunnelRelay(server, opener, name);
        Thread.ofVirtual().name("tunnel-relay").start(relay::serve);
        return relay;
    }

    /** The port on {@link #HOST} that leads through the tunnel. */
    public int port() {
        return server.getLocalPort();
    }

    /**
     * Why the tunnel did not open, once the relay closed its connection for it; null while it has
     * not failed.
     */
    public Exception failure() {
        return failure;
    }

    @Override
    public void close() {
        try {
            server.close();
        } catch (IOException e) {
            LOG.debugv("Closing the relay to {0} failed: {1}", name, e.getMessage());
        }
    }

    private void serve() {
        try (Socket client = server.accept()) {
            close();
            Socket opened;
            try {
                opened = opener.open();
            } catch (IOException | RuntimeException e) {
                failure = e;
                LOG.warnv("Could not open the tunnel to {0}: {1}", name, e.getMessage());
                return;
            }
            try (Socket far = opened) {
                Thread up = Thread.ofVirtual().name("tunnel-relay-up").start(() -> pump(client, far));
                pump(far, client);
                up.join();
            }
        } catch (SocketTimeoutException e) {
            LOG.debugv("No connection came to the relay to {0}", name);
        } catch (IOException e) {
            LOG.debugv("The relay to {0} ended: {1}", name, e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            close();
        }
    }

    /** Copies {@code from} to {@code to} until {@code from} ends, then ends {@code to}'s output. */
    private void pump(Socket from, Socket to) {
        byte[] buffer = new byte[BUFFER];
        try {
            InputStream input = from.getInputStream();
            OutputStream output = to.getOutputStream();
            int read;
            while ((read = input.read(buffer)) >= 0) {
                output.write(buffer, 0, read);
                output.flush();
            }
            try {
                to.shutdownOutput();
            } catch (UnsupportedOperationException expected) {
                // A TLS socket cannot half-close; its side ends when the other side closes.
                LOG.tracev("A TLS side of the relay to {0} stays open until the other closes", name);
            }
        } catch (IOException e) {
            LOG.tracev("A side of the relay to {0} closed: {1}", name, e.getMessage());
            closeQuietly(to);
            closeQuietly(from);
        }
    }

    private void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            LOG.tracev("Closing a side of the relay to {0} failed: {1}", name, e.getMessage());
        }
    }
}
