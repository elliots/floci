package io.github.hectorvent.floci.runtime.node;

import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Loopback-only, multi-connection relay from Floci to a node container. */
public final class NodeTcpRelay implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(NodeTcpRelay.class);
    private final ServerSocket listener;
    private final NodeTunnel tunnel;
    private final SmolvmNode node;
    private final String host;
    private final int port;
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();

    public NodeTcpRelay(NodeTunnel tunnel, SmolvmNode node, String host, int port) throws IOException {
        this(tunnel, node, host, port, 0);
    }

    public NodeTcpRelay(NodeTunnel tunnel, SmolvmNode node, String host, int port, int listenPort) throws IOException {
        this.tunnel = tunnel;
        this.node = node;
        this.host = host;
        this.port = port;
        listener = new ServerSocket();
        try {
            listener.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), listenPort));
        } catch (IOException e) {
            listener.close();
            throw e;
        }
        Thread.ofVirtual().name("node-relay-accept").start(this::accept);
    }

    public int port() {
        return listener.getLocalPort();
    }

    private void accept() {
        while (!listener.isClosed()) {
            try {
                Socket client = listener.accept();
                sockets.add(client);
                Thread.ofVirtual().name("node-relay-connection").start(() -> relay(client));
            } catch (IOException e) {
                if (!listener.isClosed()) {
                    LOG.warnv("Node relay could not accept a connection: {0}", e.getMessage());
                }
            }
        }
    }

    private void relay(Socket client) {
        Socket upstream = null;
        try (client) {
            upstream = tunnel.open(node, host, port, Duration.ofSeconds(5), Duration.ofSeconds(30));
            sockets.add(upstream);
            Socket far = upstream;
            Thread sender = Thread.ofVirtual().start(() -> copy(client, far));
            copy(far, client);
            sender.join();
        } catch (IOException e) {
            LOG.debugv("Node relay connection ended: {0}", e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            sockets.remove(client);
            if (upstream != null) {
                sockets.remove(upstream);
                try {
                    upstream.close();
                } catch (IOException e) {
                    LOG.debugv("Closing node relay upstream failed: {0}", e.getMessage());
                }
            }
        }
    }

    private static void copy(Socket from, Socket to) {
        try {
            from.getInputStream().transferTo(to.getOutputStream());
            try {
                to.shutdownOutput();
            } catch (UnsupportedOperationException expected) {
                // TLS sockets cannot half-close; their other direction ends the relay.
                LOG.trace("A TLS node relay socket does not support half-close");
            }
        } catch (IOException e) {
            try {
                from.close();
                to.close();
            } catch (IOException closeFailure) {
                LOG.debugv("Closing a node relay connection failed: {0}", closeFailure.getMessage());
            }
        }
    }

    @Override
    public void close() {
        try {
            listener.close();
        } catch (IOException e) {
            LOG.debugv("Closing node relay listener failed: {0}", e.getMessage());
        }
        for (Socket socket : sockets) {
            try {
                socket.close();
            } catch (IOException e) {
                LOG.debugv("Closing node relay socket failed: {0}", e.getMessage());
            }
        }
        sockets.clear();
    }
}
