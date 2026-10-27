package io.github.hectorvent.floci.runtime.wakes;

import io.github.hectorvent.floci.runtime.node.NodeApiException;
import io.github.hectorvent.floci.runtime.node.NodePod;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient.PodQuery;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeDirectory;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Target;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * The pod records of the nodes that workloads name, from the node service's pod listing. A
 * listing of one namespace, or of every pod of a node, serves every caller for
 * {@link #MAX_AGE}; a workload's pods are those its selector picks by their records' labels or
 * controller, so Floci needs no credentials for the Kubernetes API.
 */
@ApplicationScoped
public class PodView {

    static final Duration MAX_AGE = Duration.ofSeconds(2);

    private final Function<String, Optional<SmolvmNode>> nodes;
    private final SmolvmNodeClient client;
    private final InstantSource clock;
    private final Map<Key, Listing> listings = new ConcurrentHashMap<>();

    @Inject
    public PodView(SmolvmNodeDirectory directory, SmolvmNodeClient client) {
        this(directory::find, client, InstantSource.system());
    }

    /**
     * @param nodes finds a node by its name in the node directory
     */
    PodView(Function<String, Optional<SmolvmNode>> nodes, SmolvmNodeClient client, InstantSource clock) {
        this.nodes = nodes;
        this.client = client;
        this.clock = clock;
    }

    /**
     * The pods of one namespace of a node, or of every namespace when {@code namespace} is null,
     * as the node listed them at {@code listedAt}.
     */
    public record Listing(SmolvmNode node, String namespace, List<NodePod> pods, Instant listedAt) {

        public Listing {
            pods = List.copyOf(pods);
        }

        /** The pods of the listing that {@code target} selects. */
        public List<NodePod> select(Target target) {
            return pods.stream().filter(target::selects).toList();
        }
    }

    private record Key(SmolvmNode node, String namespace) {
    }

    /** The node named {@code name} in the node directory, such as {@code node:dev}. */
    public Optional<SmolvmNode> node(String name) {
        return nodes.apply(name);
    }

    /**
     * The pods of the namespace that {@code target} names, on its node, from a listing at most
     * {@link #MAX_AGE} old.
     *
     * @return empty when the node directory has no such node
     * @throws NodeApiException when the node does not answer the listing
     */
    public Optional<Listing> namespace(Target target) {
        return node(target.node()).map(node -> namespace(node, target.namespace()));
    }

    /**
     * The pods of {@code namespace} on {@code node}, from a listing at most {@link #MAX_AGE} old.
     *
     * @throws NodeApiException when the node does not answer the listing
     */
    public Listing namespace(SmolvmNode node, String namespace) {
        return listing(new Key(node, namespace));
    }

    /**
     * Every pod of {@code node}, from a listing at most {@link #MAX_AGE} old.
     *
     * @throws NodeApiException when the node does not answer the listing
     */
    public Listing all(SmolvmNode node) {
        return listing(new Key(node, null));
    }

    /**
     * Lists the namespace again at the next call: after the node answered that a pod is gone or
     * has a new incarnation. It also drops the node's listing of every pod.
     */
    public void forget(String node, String namespace) {
        listings.keySet().removeIf(key -> key.node().name().equals(node)
                && (key.namespace() == null || key.namespace().equals(namespace)));
    }

    /** Lists every namespace of {@code node} again at the next call: its pods changed. */
    public void forget(String node) {
        listings.keySet().removeIf(key -> key.node().name().equals(node));
    }

    /** Drops every listing. */
    public void clear() {
        listings.clear();
    }

    private Listing listing(Key key) {
        Instant now = clock.instant();
        Listing cached = listings.get(key);
        if (cached != null && now.isBefore(cached.listedAt().plus(MAX_AGE)) && !now.isBefore(cached.listedAt())) {
            return cached;
        }
        PodQuery query = key.namespace() == null ? PodQuery.all() : PodQuery.namespace(key.namespace());
        Listing listing = new Listing(key.node(), key.namespace(), client.listPods(key.node(), query), now);
        listings.keySet().removeIf(other -> other.node().name().equals(key.node().name())
                && !other.node().equals(key.node()));
        listings.put(key, listing);
        return listing;
    }
}
