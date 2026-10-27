package io.github.hectorvent.floci.runtime.network;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.config.FlociCertificateAuthority.RouteIssuer;
import io.github.hectorvent.floci.core.common.dns.NodeDnsServer;
import io.github.hectorvent.floci.runtime.RetryBackoff;
import io.github.hectorvent.floci.runtime.node.NodeApiException;
import io.github.hectorvent.floci.runtime.node.RuntimeFile;
import io.github.hectorvent.floci.runtime.node.RuntimeFileWatcher;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeDirectory;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;

/**
 * Publishes the network profile to every smolvm-pods node of {@link SmolvmNodeDirectory}: when
 * a node joins, when the runtime file's {@code network} changes, again after a failed call, and
 * when the node reports another generation, which a check finds every
 * {@link #CHECK_INTERVAL}. Each node gets Floci's ports, its own endpoints, the routes with a route
 * intermediate CA, the allow list and the trust material.
 */
@ApplicationScoped
public class NodeNetworkPublisher {

    static final Duration CHECK_INTERVAL = Duration.ofSeconds(30);
    /** A route intermediate's validity, and the time before its end at which a new one is issued. */
    static final Duration ISSUER_VALIDITY = Duration.ofDays(90);
    static final Duration ISSUER_RENEW_BEFORE = Duration.ofDays(30);

    private static final Logger LOG = Logger.getLogger(NodeNetworkPublisher.class);

    private final SmolvmNodeDirectory directory;
    private final SmolvmNodeClient client;
    private final RuntimeFileWatcher runtimeFile;
    private final NetworkSection section;
    private final FlociPortInventory ports;
    private final NetworkTrustMaterial trust;
    private final NodeDnsServer dns;
    private final ObjectMapper mapper;

    /** Per node: the generation it should have, the one it applied, and when to call it after a failure. */
    private final Map<String, String> expected = new ConcurrentHashMap<>();
    private final Map<String, String> applied = new ConcurrentHashMap<>();
    private final Map<String, Instant> retryAt = new HashMap<>();
    private final Map<String, Duration> backoff = new HashMap<>();
    private final Set<String> warnedOpen = new HashSet<>();
    private volatile RouteIssuer issuer;
    private volatile List<String> issuerNames = List.of();
    private volatile boolean dirty = true;
    private volatile boolean stopped;
    private volatile Thread loop;

    @Inject
    public NodeNetworkPublisher(SmolvmNodeDirectory directory, SmolvmNodeClient client,
                                RuntimeFileWatcher runtimeFile, NetworkSection section, FlociPortInventory ports,
                                NetworkTrustMaterial trust, NodeDnsServer dns, ObjectMapper mapper) {
        this.directory = directory;
        this.client = client;
        this.runtimeFile = runtimeFile;
        this.section = section;
        this.ports = ports;
        this.trust = trust;
        this.dns = dns;
        this.mapper = mapper;
    }

    void onStart(@Observes @Priority(Interceptor.Priority.APPLICATION + 610) StartupEvent ignored) {
        directory.addListener(nodes -> changed());
        runtimeFile.addListener(file -> changed());
        if (!directory.all().isEmpty()) {
            changed();
        }
    }

    void onStop(@Observes ShutdownEvent ignored) {
        stopped = true;
        Thread thread = loop;
        if (thread != null) {
            LockSupport.unpark(thread);
        }
    }

    /** Whether node {@code name} applied the profile that Floci publishes to it now. */
    public boolean isApplied(String name) {
        String generation = applied.get(name);
        return generation != null && generation.equals(expected.get(name));
    }

    /**
     * Whether node {@code name} applied a profile that sends {@code address:port} to Floci, so
     * that its pods reach that endpoint.
     */
    public boolean reaches(String name, String address, int port) {
        Optional<SmolvmNode> node = directory.find(name);
        return node.isPresent() && isApplied(name) && node.get().endpoints().stream()
                .anyMatch(e -> e.address().equals(address) && e.port() == port);
    }

    private synchronized void changed() {
        dirty = true;
        if (stopped) {
            return;
        }
        if (loop == null) {
            dns.start();
            loop = Thread.ofPlatform().daemon().name("node-network").start(this::run);
            return;
        }
        LockSupport.unpark(loop);
    }

    private void run() {
        Instant nextCheck = Instant.now();
        while (!stopped) {
            Instant now = Instant.now();
            boolean check = !now.isBefore(nextCheck);
            if (dirty || check || retryDue(now)) {
                dirty = false;
                try {
                    publishAll(check);
                } catch (RuntimeException e) {
                    LOG.warnv(e, "Publishing the network profile to the smolvm-pods nodes failed");
                }
                if (check) {
                    nextCheck = now.plus(CHECK_INTERVAL);
                }
            }
            Instant wake = nextCheck;
            for (Instant at : retryAt.values()) {
                if (at.isBefore(wake)) {
                    wake = at;
                }
            }
            long nanos = Duration.between(Instant.now(), wake).toNanos();
            if (nanos > 0 && !dirty) {
                LockSupport.parkNanos(this, nanos);
            }
        }
    }

    private boolean retryDue(Instant now) {
        return retryAt.values().stream().anyMatch(at -> !now.isBefore(at));
    }

    private NetworkDefinitions network() {
        RuntimeFile file = runtimeFile.current();
        return file.section(section);
    }

    private void publishAll(boolean check) {
        NetworkDefinitions network = network();
        Set<String> exact = new HashSet<>();
        Set<String> wildcards = new HashSet<>();
        for (NetworkDefinitions.Origin origin : network.origins()) {
            (origin.wildcard() ? wildcards : exact).add(origin.host());
        }
        dns.update(new NodeDnsServer.Policy(exact, wildcards, network.isolation()));
        List<SmolvmNode> nodes = directory.all();
        Set<String> names = new HashSet<>();
        for (SmolvmNode node : nodes) {
            names.add(node.name());
            Instant at = retryAt.get(node.name());
            if (at != null && Instant.now().isBefore(at) && !dirty) {
                continue;
            }
            publish(node, network, check);
        }
        applied.keySet().retainAll(names);
        expected.keySet().retainAll(names);
        retryAt.keySet().retainAll(names);
        backoff.keySet().retainAll(names);
    }

    private void publish(SmolvmNode node, NetworkDefinitions network, boolean check) {
        ObjectNode profile = profile(node, network);
        String generation = profile.path("generation").asText();
        expected.put(node.name(), generation);
        try {
            if (generation.equals(applied.get(node.name()))) {
                if (!check) {
                    return;
                }
                JsonNode status = client.network(node);
                if (generation.equals(status.path("generation").asText())) {
                    return;
                }
            }
            JsonNode status = client.putNetwork(node, profile);
            applied.put(node.name(), status.path("generation").asText(generation));
            retryAt.remove(node.name());
            backoff.remove(node.name());
            LOG.infov("Published network profile {0} to {1}: isolation {2}, {3} routes", shortGeneration(generation),
                    node.name(), status.path("effective_isolation").asBoolean(), network.routes().size());
            if (network.isolation() && !status.path("node_isolation").asBoolean()
                    && warnedOpen.add(node.name())) {
                LOG.warnv("{0} was created without isolation, so only its own table blocks pods' egress; on a Mac, "
                        + "create it again isolated for smolvm's boundary too", node.name());
            }
        } catch (NodeApiException e) {
            applied.remove(node.name());
            Duration wait = backoff.compute(node.name(), (name, previous) -> RetryBackoff.next(previous));
            retryAt.put(node.name(), Instant.now().plus(wait));
            LOG.debugv("The network profile did not reach {0}, trying again in {1} s: {2}", node.name(),
                    wait.toSeconds(), e.getMessage());
        }
    }

    private ObjectNode profile(SmolvmNode node, NetworkDefinitions network) {
        return NodeNetworkProfile.build(mapper, node, network, ports.ports(network.extraPorts()), dns.port(),
                issuer(network), network.trustMode() == NetworkDefinitions.TrustMode.OFF ? null : trust.material());
    }

    /** The route intermediate for the routed names, issued again when they change or it nears its end. */
    private synchronized RouteIssuer issuer(NetworkDefinitions network) {
        if (network.routes().isEmpty()) {
            return null;
        }
        List<String> names = NodeNetworkProfile.permittedNames(network);
        RouteIssuer current = issuer;
        if (current == null || !names.equals(issuerNames)
                || Instant.now().plus(ISSUER_RENEW_BEFORE).isAfter(current.notAfter())) {
            current = trust.authority().issueConstrainedIntermediate("Floci route CA", names,
                    Instant.now().plus(ISSUER_VALIDITY));
            issuer = current;
            issuerNames = names;
            LOG.infov("Issued a route intermediate CA for {0}, valid until {1}", names, current.notAfter());
        }
        return current;
    }

    private static String shortGeneration(String generation) {
        return generation.length() > 12 ? generation.substring(0, 12) : generation;
    }
}
