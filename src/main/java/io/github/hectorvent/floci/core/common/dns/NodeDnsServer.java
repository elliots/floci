package io.github.hectorvent.floci.core.common.dns;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.NetworkExposureGuard;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.quarkus.runtime.ShutdownEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.SequencedSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Floci's DNS for smolvm-pods nodes, which each node's DNS forwarder asks over TCP for every name
 * the cluster does not answer. Floci's own names, such as {@code localhost.floci.io} and
 * {@code host.docker.internal}, and every routed origin's host get the node address, which the
 * node sends on to Floci or to its relay; a name of a record source such as Cloud Map gets its
 * records; any other name is forwarded to Floci's resolvers, or gets NXDOMAIN while the runtime
 * file isolates pods.
 */
@ApplicationScoped
public class NodeDnsServer {

    /** Where pods of every node reach Floci and the relay. */
    public static final String NODE_ADDRESS = "169.254.53.1";
    static final int DEFAULT_PORT = 15353;

    private static final Logger LOG = Logger.getLogger(NodeDnsServer.class);
    private static final int TYPE_A = 1;
    private static final long DENIAL_LOG_EVERY_MS = 60_000;

    private final EmulatorConfig config;
    private final ContainerDetector containers;
    private final EmbeddedDnsServer answers;
    private final SequencedSet<String> flociSuffixes = new LinkedHashSet<>();
    private final Map<String, Long> denialsLogged = new ConcurrentHashMap<>();
    private volatile Policy policy = new Policy(Set.of(), Set.of(), false);
    private volatile List<String> upstreams = List.of();
    private volatile DatagramSocket udp;
    private volatile ServerSocket tcp;
    private volatile int port = -1;

    /**
     * What the runtime file adds: routed hosts, exact and wildcard domains, and whether names
     * Floci does not own get NXDOMAIN.
     */
    public record Policy(Set<String> exact, Set<String> wildcards, boolean isolation) {

        public Policy {
            exact = Set.copyOf(exact);
            wildcards = Set.copyOf(wildcards);
        }
    }

    @Inject
    public NodeDnsServer(EmulatorConfig config, ContainerDetector containers,
                         Instance<DnsRecordSource> recordSources) {
        this(config, containers, recordSources, suffixes(config));
    }

    /** Answers only, without listeners, for tests. */
    NodeDnsServer(List<String> extraSuffixes, Iterable<DnsRecordSource> recordSources) {
        this(null, null, recordSources, extraSuffixes);
    }

    private NodeDnsServer(EmulatorConfig config, ContainerDetector containers, Iterable<DnsRecordSource> recordSources,
                          List<String> extraSuffixes) {
        this.config = config;
        this.containers = containers;
        this.answers = new EmbeddedDnsServer(List.of(), recordSources);
        flociSuffixes.addAll(EmbeddedDnsServer.BUILTIN_SUFFIXES);
        flociSuffixes.add("host.docker.internal");
        extraSuffixes.forEach(name -> flociSuffixes.add(name.toLowerCase(Locale.ROOT)));
    }

    private static List<String> suffixes(EmulatorConfig config) {
        List<String> suffixes = new ArrayList<>();
        config.hostname().ifPresent(suffixes::add);
        config.dns().extraSuffixes().ifPresent(suffixes::addAll);
        return suffixes;
    }

    /** Applies the runtime file's routed hosts and isolation. */
    public void update(Policy policy) {
        this.policy = policy;
    }

    /** The port it listens on, once started; the configured one before. */
    public int port() {
        int bound = port;
        return bound > 0 ? bound : listen().getPort();
    }

    private InetSocketAddress listen() {
        Optional<String> setting = config.runtime().dnsListen();
        String host = containers.isRunningInContainer() ? "0.0.0.0" : "127.0.0.1";
        int listenPort = DEFAULT_PORT;
        if (setting.isPresent()) {
            String value = setting.get().trim();
            int colon = value.lastIndexOf(':');
            if (colon <= 0) {
                throw new IllegalStateException("floci.runtime.dns-listen must be HOST:PORT, not " + value);
            }
            host = value.substring(0, colon);
            listenPort = Integer.parseInt(value.substring(colon + 1));
        }
        return new InetSocketAddress(host, listenPort);
    }

    /** Starts the listeners, once. */
    public synchronized void start() {
        if (tcp != null) {
            return;
        }
        InetSocketAddress address = listen();
        NetworkExposureGuard.requireConsent(address.getHostString(), config.security());
        upstreams = EmbeddedDnsServer.composeUpstreams(EmbeddedDnsServer.readResolvConfNameservers(),
                config.dns().containerFallbackServers());
        try {
            ServerSocket server = new ServerSocket();
            server.setReuseAddress(true);
            server.bind(address);
            DatagramSocket datagrams = new DatagramSocket(new InetSocketAddress(address.getAddress(),
                    server.getLocalPort()));
            tcp = server;
            udp = datagrams;
            port = server.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("The smolvm-pods nodes' DNS cannot listen on " + address + ": "
                    + e.getMessage(), e);
        }
        Thread.ofPlatform().daemon().name("node-dns-tcp").start(this::serveTcp);
        Thread.ofPlatform().daemon().name("node-dns-udp").start(this::serveUdp);
        LOG.infov("Answering the smolvm-pods nodes'' DNS on {0}:{1}, {2} at {3}",
                address.getHostString(), Integer.toString(port), flociSuffixes, NODE_ADDRESS);
    }

    void onStop(@Observes ShutdownEvent ignored) {
        try {
            ServerSocket server = tcp;
            if (server != null) {
                server.close();
            }
        } catch (IOException e) {
            LOG.debugv("Closing the node DNS listener: {0}", e.getMessage());
        }
        DatagramSocket datagrams = udp;
        if (datagrams != null) {
            datagrams.close();
        }
    }

    private void serveTcp() {
        ServerSocket server = tcp;
        while (!server.isClosed()) {
            Socket socket;
            try {
                socket = server.accept();
            } catch (IOException e) {
                if (!server.isClosed()) {
                    LOG.warnv("The node DNS listener failed: {0}", e.getMessage());
                }
                return;
            }
            Thread.ofVirtual().name("node-dns-connection").start(() -> serveConnection(socket));
        }
    }

    private void serveConnection(Socket socket) {
        try (socket; DataInputStream in = new DataInputStream(socket.getInputStream());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            socket.setTcpNoDelay(true);
            while (true) {
                int length = in.readUnsignedShort();
                byte[] query = in.readNBytes(length);
                if (query.length < length) {
                    return;
                }
                Thread.ofVirtual().name("node-dns-query").start(() -> {
                    byte[] answer = answer(query);
                    if (answer == null) {
                        return;
                    }
                    synchronized (out) {
                        try {
                            out.writeShort(answer.length);
                            out.write(answer);
                            out.flush();
                        } catch (IOException e) {
                            LOG.debugv("A node DNS connection closed: {0}", e.getMessage());
                        }
                    }
                });
            }
        } catch (IOException e) {
            LOG.debugv("A node DNS connection ended: {0}", e.getMessage());
        }
    }

    private void serveUdp() {
        DatagramSocket socket = udp;
        byte[] buffer = new byte[4096];
        while (!socket.isClosed()) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);
            } catch (IOException e) {
                if (!socket.isClosed()) {
                    LOG.warnv("The node DNS UDP socket failed: {0}", e.getMessage());
                }
                return;
            }
            byte[] query = Arrays.copyOf(packet.getData(), packet.getLength());
            InetAddress sender = packet.getAddress();
            int senderPort = packet.getPort();
            Thread.ofVirtual().name("node-dns-query").start(() -> {
                byte[] answer = answer(query);
                if (answer != null) {
                    try {
                        socket.send(new DatagramPacket(answer, answer.length, sender, senderPort));
                    } catch (IOException e) {
                        LOG.debugv("A node DNS answer was not sent: {0}", e.getMessage());
                    }
                }
            });
        }
    }

    /** Whether {@code name} is Floci's: one of its suffixes, or below one. */
    boolean isFlociName(String name) {
        for (String suffix : flociSuffixes) {
            if (name.equals(suffix) || name.endsWith("." + suffix)) {
                return true;
            }
        }
        return false;
    }

    /** Whether {@code name} is a routed origin's host. */
    boolean isRouted(String name) {
        Policy current = policy;
        if (current.exact().contains(name)) {
            return true;
        }
        for (String base : current.wildcards()) {
            if (name.endsWith("." + base)) {
                return true;
            }
        }
        return false;
    }

    /** The answer to {@code query}, or none for a message that is not a query. */
    byte[] answer(byte[] query) {
        if (query.length < 12 || (query[2] & 0x80) != 0) {
            return null;
        }
        int questionEnd = EmbeddedDnsServer.questionEnd(query);
        if (questionEnd < 0) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(query);
        short txId = buffer.getShort(0);
        buffer.position(12);
        String name = EmbeddedDnsServer.readName(buffer, query).toLowerCase(Locale.ROOT);
        if (name.endsWith(".")) {
            name = name.substring(0, name.length() - 1);
        }
        int type = buffer.getShort(questionEnd - 4) & 0xFFFF;
        if (isFlociName(name) || isRouted(name)) {
            if (type == TYPE_A) {
                return answers.buildAResponse(query, txId, 12, questionEnd,
                        DnsAnswer.records(List.of(NODE_ADDRESS), DnsAnswer.DEFAULT_TTL_SECONDS));
            }
            return answers.buildEmptyResponse(query, txId, 12, questionEnd, 0);
        }
        Optional<DnsAnswer> owned = answers.resolveFromRecordSources(name, type);
        if (owned.isPresent()) {
            DnsAnswer answer = owned.get();
            return answer.isEmpty()
                    ? answers.buildEmptyResponse(query, txId, 12, questionEnd, answer.nameExists() ? 0 : 3)
                    : answers.buildAResponse(query, txId, 12, questionEnd, answer);
        }
        if (policy.isolation()) {
            logDenial(name);
            return answers.buildEmptyResponse(query, txId, 12, questionEnd, 3);
        }
        try {
            return EmbeddedDnsServer.forwardToUpstreams(query, upstreams, EmbeddedDnsServer.DNS_PORT);
        } catch (Exception e) {
            LOG.debugv("No resolver answered {0} for a smolvm-pods node: {1}", name, e.getMessage());
            return answers.buildServerFailureResponse(query, txId, 12, questionEnd);
        }
    }

    private void logDenial(String name) {
        long now = System.currentTimeMillis();
        Long last = denialsLogged.get(name);
        if (last == null || now - last >= DENIAL_LOG_EVERY_MS) {
            denialsLogged.put(name, now);
            if (denialsLogged.size() > 10_000) {
                denialsLogged.clear();
            }
            LOG.infov("Refused {0} to a smolvm-pods node: pods are isolated and it is not a Floci or routed name",
                    name);
        }
    }
}
