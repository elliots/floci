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

/** Loopback-only, multi-connection relay from Floci's registry clients to a node container. */
public final class NodeRegistryRelay implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(NodeRegistryRelay.class);
    private final ServerSocket listener;
    private final NodeTunnel tunnel;
    private final SmolvmNode node;
    private final String host;
    private final int port;
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();

    public NodeRegistryRelay(NodeTunnel tunnel, SmolvmNode node, String host, int port) throws IOException {
        this.tunnel = tunnel;
        this.node = node;
        this.host = host;
        this.port = port;
        listener = new ServerSocket();
        listener.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
        Thread.ofVirtual().name("node-registry-accept").start(this::accept);
    }

    public int port() {
        return listener.getLocalPort();
    }

    private void accept() {
        while (!listener.isClosed()) {
            try {
                Socket client = listener.accept();
                sockets.add(client);
                Thread.ofVirtual().name("node-registry-connection").start(() -> relay(client));
            } catch (IOException e) {
                if (!listener.isClosed()) {
                    LOG.warnv("Node registry relay could not accept a connection: {0}", e.getMessage());
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
            LOG.debugv("Node registry connection ended: {0}", e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            sockets.remove(client);
            if (upstream != null) {
                sockets.remove(upstream);
                try {
                    upstream.close();
                } catch (IOException e) {
                    LOG.debugv("Closing node registry upstream failed: {0}", e.getMessage());
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
                LOG.trace("A TLS node registry socket does not support half-close");
            }
        } catch (IOException e) {
            try {
                from.close();
                to.close();
            } catch (IOException closeFailure) {
                LOG.debugv("Closing a node registry connection failed: {0}", closeFailure.getMessage());
            }
        }
    }

    @Override
    public void close() {
        try {
            listener.close();
        } catch (IOException e) {
            LOG.debugv("Closing node registry listener failed: {0}", e.getMessage());
        }
        for (Socket socket : sockets) {
            try {
                socket.close();
            } catch (IOException e) {
                LOG.debugv("Closing node registry socket failed: {0}", e.getMessage());
            }
        }
        sockets.clear();
    }
}
