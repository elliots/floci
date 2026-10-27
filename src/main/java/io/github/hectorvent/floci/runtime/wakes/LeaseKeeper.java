package io.github.hectorvent.floci.runtime.wakes;

import io.github.hectorvent.floci.runtime.RetryBackoff;
import io.github.hectorvent.floci.runtime.node.NodeApiException;
import io.github.hectorvent.floci.runtime.node.NodeEvent;
import io.github.hectorvent.floci.runtime.node.NodePod;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient.LeaseHeld;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient.Wake;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient.WakeReason;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient.WakeRequest;
import io.github.hectorvent.floci.runtime.wakes.PodView.Listing;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Target;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Workload;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The leases Floci holds on workloads' pods, and the wakes that follow them. While a workload is
 * busy, each of its pods gets a lease, {@code floci:WORKLOAD} or {@code floci:INSTANCE:WORKLOAD}
 * with the runtime file's {@code instance-id}, and is woken; the lease is renewed every
 * {@link #RENEW_INTERVAL} until the workload is idle, then released. A node that refuses a wake
 * for want of room keeps the lease, and
 * gets no wake until one of its pods stops using room or {@link #ROOM_WAIT} passes; then its
 * oldest refused pod is woken, one at a time. A node that does not answer is called again after
 * {@link RetryBackoff#FIRST}, doubling up to {@link RetryBackoff#MAX}.
 *
 * <p>A workload's pods come from a listing of its namespace when it begins to wake, when its
 * target changes, and again when the node's events show a pod that Floci has not listed or a
 * follow of those events begins ({@link #nodeEvent}, {@link #followStarted}). The events also give a held pod its state, end the
 * leases of a deleted pod, and free room when a pod stops using it.
 *
 * <p>Each {@link #tick} makes the lease calls on the caller's thread, and {@link #nextDue} says
 * when the next tick has calls to make. Wakes, which last up to the workload's startup timeout,
 * run on their own threads, one at a time per pod; their answers take effect at the next tick.
 */
@ApplicationScoped
public class LeaseKeeper {

    static final int LEASE_TTL_SECONDS = 60;
    static final Duration RENEW_INTERVAL = Duration.ofSeconds(20);
    static final Duration ROOM_WAIT = Duration.ofMinutes(5);
    static final Duration UNAUTHORIZED_LOG_INTERVAL = Duration.ofSeconds(30);
    static final String LEASE_PREFIX = "floci:";
    static final String SOURCE = "floci";

    private static final Logger LOG = Logger.getLogger(LeaseKeeper.class);
    private static final Set<NodePod.State> USING_ROOM = EnumSet.of(NodePod.State.CREATING, NodePod.State.AWAKE,
            NodePod.State.PAUSING, NodePod.State.WAKING);
    private static final String ADMISSION_REFUSAL = "wake refused";
    private static final String CREATING = "state creating";
    private static final String NEW_RECORD = "none";
    private static final String REMOVED_RECORD = "deleted";

    /** Whether a workload is busy at one tick. */
    public enum Work {
        /** Busy: its pods are leased and woken. */
        WAITING,
        /** Idle: its leases are released. */
        NONE,
        /** A queue could not be read: leases are kept, and no pod is woken. */
        UNKNOWN
    }

    /**
     * A workload and whether it is busy at one tick.
     *
     * @param resource the queue whose messages wait, as the wake's reason names it: an SQS queue
     *                 ARN, or {@code amqp://BROKER-ID/VHOST/QUEUE}; null unless {@code WAITING}
     */
    public record Demand(Workload workload, Work work, String resource) {

        public Demand {
            Objects.requireNonNull(workload, "workload");
            Objects.requireNonNull(work, "work");
            if (work == Work.WAITING) {
                Objects.requireNonNull(resource, "resource");
            }
        }

        public static Demand waiting(Workload workload, String resource) {
            return new Demand(workload, Work.WAITING, resource);
        }

        public static Demand none(Workload workload) {
            return new Demand(workload, Work.NONE, null);
        }

        public static Demand unknown(Workload workload) {
            return new Demand(workload, Work.UNKNOWN, null);
        }
    }

    /** What a tick does for a workload: take leases and wake, keep the leases held, or release them. */
    private enum Mode { WAKE, KEEP, RELEASE }

    private record Key(String node, String sandbox, String leaseId) {
    }

    private record PodKey(String node, String sandbox) {
    }

    private record ListingKey(String node, String namespace) {
    }

    private record WorkloadNode(String workload, String node) {
    }

    /**
     * A lease a tick wants held. {@code node} and {@code pod} come from this tick's listing, and
     * are null for a lease that is only kept.
     */
    private record Want(Workload workload, Mode mode, String resource, SmolvmNode node, NodePod pod) {
    }

    private record WakeResult(Held held, boolean retry, Wake wake, NodeApiException failure) {
    }

    /** One lease on one pod, from the first tick that wants it until it is released or the pod is gone. */
    private static final class Held {
        final Key key;
        final String workload;
        final String namespace;
        final String name;
        final SmolvmNode node;
        String incarnation;
        NodePod.State state;
        long generation;
        Instant renewed;
        boolean woken;
        boolean waking;
        Instant queued;
        Instant retryAt;
        Duration retry;
        String resource;
        Duration startupTimeout;

        Held(Key key, String workload, SmolvmNode node, NodePod pod) {
            this.key = key;
            this.workload = workload;
            this.namespace = pod.namespace();
            this.name = pod.name();
            this.node = node;
            this.incarnation = pod.incarnation();
            this.state = pod.state();
            this.generation = pod.generation();
        }

        PodKey pod() {
            return new PodKey(key.node(), key.sandbox());
        }

        /** Takes the pod's state from a record newer than the last one seen. */
        void saw(NodePod pod) {
            saw(pod.state(), pod.generation());
        }

        /** Takes the pod's state from a record or event newer than the last one seen. */
        void saw(NodePod.State newState, long newGeneration) {
            if (newGeneration > generation) {
                state = newState;
                generation = newGeneration;
            }
        }

        @Override
        public String toString() {
            return namespace + "/" + name + " on " + key.node();
        }
    }

    /** What Floci knows of one node: its failures, and whether its refused wakes wait for room. */
    private static final class NodeState {
        Instant retryAt;
        Duration retry;
        boolean failing;
        Instant unauthorizedLogged;
        boolean waiting;
        boolean warned;
        boolean retrying;
        boolean roomFreed;
        Instant lastRefusal;
    }

    /** A pod event of a node, or, with no event, the start of a follow of the node's events. */
    private record NodeUpdate(String node, NodeEvent event) {
    }

    private final SmolvmNodeClient client;
    private final PodView pods;
    private final InstantSource clock;
    private final Executor wakes;
    private final Queue<WakeResult> results = new ConcurrentLinkedQueue<>();
    private final Map<Key, Held> held = new LinkedHashMap<>();
    private final Map<String, NodeState> nodes = new HashMap<>();
    private final Set<PodKey> wakingPods = new HashSet<>();
    private final Map<ListingKey, Instant> swept = new HashMap<>();
    private final Set<WorkloadNode> missingNodes = new HashSet<>();
    private final Queue<NodeUpdate> nodeUpdates = new ConcurrentLinkedQueue<>();
    private final Map<String, Target> listed = new HashMap<>();
    private final Set<String> relist = new HashSet<>();
    private final Map<String, Set<String>> known = new HashMap<>();
    private volatile Runnable changes = () -> { };
    private Instant lastTick;

    @Inject
    public LeaseKeeper(SmolvmNodeClient client, PodView pods) {
        this(client, pods, InstantSource.system(), Executors.newVirtualThreadPerTaskExecutor());
    }

    /** @param wakes runs each wake call */
    LeaseKeeper(SmolvmNodeClient client, PodView pods, InstantSource clock, Executor wakes) {
        this.client = client;
        this.pods = pods;
        this.clock = clock;
        this.wakes = wakes;
    }

    @PreDestroy
    void close() {
        if (wakes instanceof ExecutorService service) {
            service.shutdownNow();
        }
    }

    /** The id of a workload's leases: {@code floci:WORKLOAD}, or {@code floci:INSTANCE:WORKLOAD}. */
    public static String leaseId(String instanceId, String workload) {
        return prefix(instanceId) + workload;
    }

    /** Runs {@code listener} on the thread of each change that the next tick should take: a wake's answer, a node event. */
    public void onChange(Runnable listener) {
        changes = listener;
    }

    /** Takes a pod event of {@code node} at the next tick. Returns at once. */
    public void nodeEvent(String node, NodeEvent event) {
        nodeUpdates.add(new NodeUpdate(node, event));
        changes.run();
    }

    /** Lists the pods of the busy workloads on {@code node} at the next tick: a follow of its events began. */
    public void followStarted(String node) {
        nodeUpdates.add(new NodeUpdate(node, null));
        changes.run();
    }

    /**
     * Brings the leases in line with {@code demands}: takes and renews the leases of busy
     * workloads, wakes their pods, and releases every other lease this keeper holds. A workload
     * that is not in {@code demands} has no leases.
     *
     * @param instanceId the runtime file's {@code instance-id}, or null
     */
    public synchronized void tick(String instanceId, List<Demand> demands) {
        Instant now = clock.instant();
        drain(now);
        drainNodeUpdates();
        String prefix = prefix(instanceId);
        Map<String, Optional<SmolvmNode>> found = new HashMap<>();
        Map<Key, Want> wanted = new LinkedHashMap<>();
        List<Listing> listings = new ArrayList<>();
        Set<String> leaseIds = new HashSet<>();
        for (Demand demand : demands) {
            Workload workload = demand.workload();
            String leaseId = prefix + workload.name();
            leaseIds.add(leaseId);
            Mode mode = switch (demand.work()) {
                case WAITING -> Mode.WAKE;
                case UNKNOWN -> Mode.KEEP;
                case NONE -> Mode.RELEASE;
            };
            if (mode != Mode.WAKE) {
                listed.remove(leaseId);
            }
            switch (mode) {
                case RELEASE -> {
                }
                case KEEP -> keepHeld(workload, leaseId, Mode.KEEP, null, wanted);
                case WAKE -> {
                    if (relist.contains(workload.target().node())
                            || !workload.target().equals(listed.get(leaseId))) {
                        listed.remove(leaseId);
                    }
                    Listing listing = listed.containsKey(leaseId) ? null : list(workload, found, now);
                    if (listing == null) {
                        keepHeld(workload, leaseId, Mode.WAKE, demand.resource(), wanted);
                        continue;
                    }
                    listed.put(leaseId, workload.target());
                    listings.add(listing);
                    for (NodePod pod : listing.select(workload.target())) {
                        if (pod.state() != NodePod.State.FAILED && pod.state() != NodePod.State.DELETING) {
                            wanted.put(new Key(listing.node().name(), pod.sandboxId(), leaseId),
                                    new Want(workload, Mode.WAKE, demand.resource(), listing.node(), pod));
                        }
                    }
                }
            }
        }
        listed.keySet().retainAll(leaseIds);
        relist.clear();
        releaseUnwanted(wanted, found, now);
        for (Listing listing : listings) {
            sweep(listing, prefix, wanted, now);
        }
        for (Map.Entry<Key, Want> want : wanted.entrySet()) {
            keep(want.getKey(), want.getValue(), now);
        }
        retryRefused(wanted, now);
        drain(now);
        endRetries(now);
        lastTick = now;
    }

    /**
     * When the next {@link #tick} has calls to make: the first renewal of a held lease, a call to
     * try again, or the end of a wait for room. Empty when no tick needs to come before the next
     * change.
     */
    public synchronized Optional<Instant> nextDue() {
        Instant due = null;
        for (Held lease : held.values()) {
            if (lease.renewed != null) {
                due = after(due, lease.renewed.plus(RENEW_INTERVAL));
            }
            due = after(due, lease.retryAt);
        }
        for (NodeState node : nodes.values()) {
            due = after(due, node.retryAt);
            if (node.waiting && node.lastRefusal != null) {
                due = after(due, node.lastRefusal.plus(ROOM_WAIT));
            }
        }
        return Optional.ofNullable(due);
    }

    /**
     * The earlier of {@code due} and {@code time} when {@code time} comes after the last tick: a
     * time the last tick passed had its calls made then, or waits for a retry time of its own.
     */
    private Instant after(Instant due, Instant time) {
        if (time == null || (lastTick != null && !time.isAfter(lastTick))) {
            return due;
        }
        return earliest(due, time);
    }

    /** Releases every lease this keeper holds and forgets every workload's work. */
    public synchronized void releaseAll() {
        Instant now = clock.instant();
        drain(now);
        for (Held lease : held.values()) {
            release(lease, now);
        }
        held.clear();
        nodes.clear();
        wakingPods.clear();
        swept.clear();
        missingNodes.clear();
        listed.clear();
        relist.clear();
        known.clear();
        nodeUpdates.clear();
        pods.clear();
    }

    /** The ids of the leases this keeper holds on {@code sandbox}, for tests. */
    synchronized List<String> heldOn(String sandbox) {
        return held.values().stream()
                .filter(lease -> lease.key.sandbox().equals(sandbox) && lease.renewed != null)
                .map(lease -> lease.key.leaseId())
                .toList();
    }

    /** Whether {@code node} holds back wakes until it has room, for tests. */
    synchronized boolean waitingForRoom(String node) {
        NodeState state = nodes.get(node);
        return state != null && state.waiting;
    }

    /** Wants the leases already held for {@code workload}, without taking new ones. */
    private void keepHeld(Workload workload, String leaseId, Mode mode, String resource, Map<Key, Want> wanted) {
        for (Held lease : held.values()) {
            if (lease.workload.equals(workload.name()) && lease.key.leaseId().equals(leaseId)) {
                wanted.put(lease.key, new Want(workload, mode, resource, null, null));
            }
        }
    }

    /** The listing of the workload's namespace, or null when its node is unknown, backs off or failed. */
    private Listing list(Workload workload, Map<String, Optional<SmolvmNode>> found, Instant now) {
        String name = workload.target().node();
        Optional<SmolvmNode> node = found.computeIfAbsent(name, pods::node);
        WorkloadNode missing = new WorkloadNode(workload.name(), name);
        if (node.isEmpty()) {
            if (missingNodes.add(missing)) {
                LOG.infov("Workload {0} has messages for its pods on {1}, which Floci does not know; it wakes them "
                        + "once the node exists", workload.name(), name);
            }
            return null;
        }
        missingNodes.remove(missing);
        NodeState state = node(name);
        if (backsOff(state, now)) {
            return null;
        }
        try {
            Listing listing = pods.namespace(node.get(), workload.target().namespace());
            answered(name, state);
            Set<String> sandboxes = known.computeIfAbsent(name, ignored -> new HashSet<>());
            for (NodePod pod : listing.pods()) {
                sandboxes.add(pod.sandboxId());
            }
            return listing;
        } catch (NodeApiException e) {
            nodeFailed(name, state, e, now);
            return null;
        }
    }

    /**
     * Releases the leases no tick wants, and forgets those on nodes that left the directory or that
     * another node of the same name replaced, such as the new node of an EKS cluster created again.
     */
    private void releaseUnwanted(Map<Key, Want> wanted, Map<String, Optional<SmolvmNode>> found, Instant now) {
        Iterator<Held> leases = held.values().iterator();
        while (leases.hasNext()) {
            Held lease = leases.next();
            Optional<SmolvmNode> node = found.computeIfAbsent(lease.key.node(), pods::node);
            if (node.isEmpty()) {
                LOG.debugv("Forgetting lease {0} of pod {1}: the node left the node directory", lease.key.leaseId(),
                        lease);
                leases.remove();
                continue;
            }
            if (!node.get().equals(lease.node)) {
                LOG.debugv("Forgetting lease {0} of pod {1}: another node replaced its node", lease.key.leaseId(),
                        lease);
                leases.remove();
                continue;
            }
            if (!wanted.containsKey(lease.key)) {
                release(lease, now);
                leases.remove();
            }
        }
    }

    /** Deletes the leases of this Floci in a new listing that no workload wants, such as a previous run's. */
    private void sweep(Listing listing, String prefix, Map<Key, Want> wanted, Instant now) {
        String node = listing.node().name();
        if (listing.listedAt().equals(swept.put(new ListingKey(node, listing.namespace()), listing.listedAt()))) {
            return;
        }
        for (NodePod pod : listing.pods()) {
            for (NodePod.Lease lease : pod.leases()) {
                Key leaseKey = new Key(node, pod.sandboxId(), lease.id());
                if ("api".equals(lease.kind()) && own(prefix, lease.id()) && !wanted.containsKey(leaseKey)
                        && !held.containsKey(leaseKey) && !backsOff(node(node), now)) {
                    try {
                        client.deleteLease(listing.node(), pod.sandboxId(), lease.id());
                        LOG.debugv("Deleted lease {0} of pod {1}/{2} on {3}, which no workload wants", lease.id(),
                                pod.namespace(), pod.name(), node);
                    } catch (NodeApiException e) {
                        LOG.debugv("Deleting lease {0} of pod {1}/{2} on {3} failed: {4}", lease.id(),
                                pod.namespace(), pod.name(), node, e.getMessage());
                    }
                }
            }
        }
    }

    /** Takes, renews and wakes for one wanted lease. */
    private void keep(Key key, Want want, Instant now) {
        NodePod pod = want.pod();
        Held lease = held.get(key);
        if (lease != null && pod != null && !Objects.equals(lease.incarnation, pod.incarnation())) {
            held.remove(key);
            lease = null;
        }
        if (lease == null) {
            if (pod == null) {
                return;
            }
            lease = new Held(key, want.workload().name(), want.node(), pod);
            held.put(key, lease);
        } else if (pod != null) {
            lease.saw(pod);
        }
        if (want.mode() == Mode.WAKE) {
            lease.resource = want.resource();
            lease.startupTimeout = want.workload().startupTimeout();
        }
        NodeState node = node(key.node());
        if (lease.renewed == null) {
            if ((pod != null || want.mode() == Mode.WAKE) && callable(node, lease, now)) {
                hold(lease, node, now);
            }
        } else if (!now.isBefore(lease.renewed.plus(RENEW_INTERVAL)) && callable(node, lease, now)) {
            renew(lease, node, now);
        }
        if (want.mode() == Mode.WAKE && held.get(key) == lease && lease.renewed != null) {
            wakeIfNeeded(lease, node, now);
        }
    }

    private void hold(Held lease, NodeState node, Instant now) {
        try {
            LeaseHeld answer = client.putLease(lease.node, lease.key.sandbox(), lease.key.leaseId(),
                    LEASE_TTL_SECONDS, "floci wakes: " + lease.workload, lease.incarnation);
            answered(lease.key.node(), node);
            lease.renewed = now;
            lease.woken = false;
            lease.retry = null;
            lease.retryAt = null;
            lease.saw(answer.pod());
            LOG.debugv("Holding lease {0} on pod {1}", lease.key.leaseId(), lease);
        } catch (NodeApiException e) {
            callFailed(lease, node, e, now, "holding lease " + lease.key.leaseId());
        }
    }

    private void renew(Held lease, NodeState node, Instant now) {
        try {
            LeaseHeld answer = client.patchLease(lease.node, lease.key.sandbox(), lease.key.leaseId(),
                    LEASE_TTL_SECONDS, lease.incarnation);
            answered(lease.key.node(), node);
            lease.renewed = now;
            lease.saw(answer.pod());
        } catch (NodeApiException e) {
            if (e.code() == NodeApiException.Code.LEASE_NOT_FOUND) {
                LOG.debugv("Lease {0} on pod {1} expired; holding it again", lease.key.leaseId(), lease);
                answered(lease.key.node(), node);
                hold(lease, node, now);
            } else {
                callFailed(lease, node, e, now, "renewing lease " + lease.key.leaseId());
            }
        }
    }

    private void release(Held lease, Instant now) {
        NodeState node = node(lease.key.node());
        if (lease.renewed == null || backsOff(node, now)) {
            return;
        }
        try {
            client.deleteLease(lease.node, lease.key.sandbox(), lease.key.leaseId());
            answered(lease.key.node(), node);
            LOG.debugv("Released lease {0} on pod {1}", lease.key.leaseId(), lease);
        } catch (NodeApiException e) {
            if (e.code() == NodeApiException.Code.UNREACHABLE || e.code() == NodeApiException.Code.UNAUTHORIZED) {
                nodeFailed(lease.key.node(), node, e, now);
            }
            LOG.debugv("Releasing lease {0} on pod {1} failed; it expires by itself: {2}", lease.key.leaseId(),
                    lease, e.getMessage());
        }
    }

    /** Whether the pod of {@code lease} needs a wake that may be sent now. */
    private boolean wakeable(Held lease, NodeState node, Instant now) {
        return !lease.woken && !lease.waking && lease.state != NodePod.State.CREATING
                && !wakingPods.contains(lease.pod()) && callable(node, lease, now);
    }

    private void wakeIfNeeded(Held lease, NodeState node, Instant now) {
        if (!wakeable(lease, node, now)) {
            return;
        }
        if (node.waiting) {
            if (lease.queued == null) {
                lease.queued = now;
            }
            return;
        }
        wake(lease, false);
    }

    private void wake(Held lease, boolean retry) {
        lease.waking = true;
        wakingPods.add(lease.pod());
        SmolvmNode node = lease.node;
        String sandbox = lease.key.sandbox();
        WakeRequest request = new WakeRequest(WakeReason.aws(lease.resource), SOURCE, lease.incarnation);
        Duration timeout = lease.startupTimeout;
        wakes.execute(() -> {
            try {
                results.add(new WakeResult(lease, retry, client.wake(node, sandbox, request, timeout), null));
            } catch (NodeApiException e) {
                results.add(new WakeResult(lease, retry, null, e));
            } catch (RuntimeException e) {
                results.add(new WakeResult(lease, retry, null, new NodeApiException(NodeApiException.Code.UNEXPECTED,
                        node.name() + ": the wake of " + sandbox + " failed: " + e.getMessage(), e)));
            }
            changes.run();
        });
    }

    /**
     * Applies the node events that came since the last call: a pod that is new or not yet listed
     * has its node listed again for busy workloads; a held pod takes its new state; a deleted pod
     * loses its leases; a pod that stops using room frees room on a node that waits for it.
     */
    private void drainNodeUpdates() {
        NodeUpdate update;
        while ((update = nodeUpdates.poll()) != null) {
            String node = update.node();
            NodeEvent event = update.event();
            if (event == null) {
                relist(node);
                continue;
            }
            Set<String> sandboxes = known.computeIfAbsent(node, ignored -> new HashSet<>());
            boolean unknown = sandboxes.add(event.pod());
            if (unknown || NEW_RECORD.equals(event.from())) {
                relist(node);
            }
            NodePod.State to = state(event.to());
            boolean gone = REMOVED_RECORD.equals(event.to()) || to == NodePod.State.DELETING;
            Iterator<Held> leases = held.values().iterator();
            while (leases.hasNext()) {
                Held lease = leases.next();
                if (!lease.key.node().equals(node) || !lease.key.sandbox().equals(event.pod())) {
                    continue;
                }
                if (gone) {
                    LOG.debugv("Forgetting lease {0} of pod {1}: the pod is {2}", lease.key.leaseId(), lease,
                            event.to());
                    leases.remove();
                    listed.remove(lease.key.leaseId());
                } else if (to != null) {
                    lease.saw(to, event.generation());
                }
            }
            if (event.to().equals(REMOVED_RECORD)) {
                sandboxes.remove(event.pod());
            }
            NodeState state = nodes.get(node);
            NodePod.State from = state(event.from());
            if (state != null && state.waiting && from != null && USING_ROOM.contains(from)
                    && (to == null || !USING_ROOM.contains(to))) {
                LOG.debugv("Pod {0} on {1} went from {2} to {3}, which frees room", event.pod(), node, event.from(),
                        event.to());
                state.roomFreed = true;
            }
        }
    }

    /** Lists the node's pods for busy workloads at this tick, and for others when they begin to wake. */
    private void relist(String node) {
        relist.add(node);
        pods.forget(node);
    }

    /** The state an event names, or null for none, a removed record or a state this Floci does not know. */
    private static NodePod.State state(String wire) {
        if (NEW_RECORD.equals(wire) || REMOVED_RECORD.equals(wire)) {
            return null;
        }
        try {
            return NodePod.State.of(wire);
        } catch (IllegalArgumentException e) {
            LOG.debugv("A node event names the pod state {0}, which Floci does not know", wire);
            return null;
        }
    }

    /** Applies the answers of the wakes that ended since the last call. */
    private void drain(Instant now) {
        WakeResult result;
        while ((result = results.poll()) != null) {
            Held lease = result.held();
            lease.waking = false;
            wakingPods.remove(lease.pod());
            NodeState node = node(lease.key.node());
            if (result.retry()) {
                node.retrying = false;
            }
            if (held.get(lease.key) != lease) {
                continue;
            }
            if (result.wake() != null) {
                woke(lease, result.wake(), result.retry(), node);
            } else {
                wakeFailed(lease, result.failure(), node, now);
                if (result.retry() && !admissionRefusal(result.failure())) {
                    node.roomFreed = true;
                }
            }
        }
    }

    private static boolean admissionRefusal(NodeApiException e) {
        return e.code() == NodeApiException.Code.REFUSED && e.reasons().contains(ADMISSION_REFUSAL);
    }

    private void woke(Held lease, Wake wake, boolean retry, NodeState node) {
        answered(lease.key.node(), node);
        lease.retry = null;
        lease.retryAt = null;
        boolean queued = lease.queued != null;
        for (Held other : held.values()) {
            if (other.pod().equals(lease.pod()) && Objects.equals(other.incarnation, wake.pod().incarnation())) {
                other.woken = true;
                other.queued = null;
                other.saw(wake.pod());
            }
        }
        if (retry) {
            node.roomFreed = true;
        }
        if (wake.resumed() || queued) {
            LOG.infov("Woke pod {0} for workload {1}: messages wait in {2}", lease, lease.workload, lease.resource);
        } else {
            LOG.debugv("Pod {0} of workload {1} is awake", lease, lease.workload);
        }
    }

    private void wakeFailed(Held lease, NodeApiException e, NodeState node, Instant now) {
        switch (e.code()) {
            case REFUSED -> {
                if (admissionRefusal(e)) {
                    refused(lease, node, e, now);
                } else if (e.reasons().contains(CREATING)) {
                    lease.state = NodePod.State.CREATING;
                } else {
                    LOG.debugv("The wake of pod {0} was refused; trying again: {1}", lease, e.getMessage());
                }
            }
            case NOT_FOUND, GONE, STALE_INCARNATION, FAILED -> forget(lease, e);
            case UNAUTHORIZED -> nodeFailed(lease.key.node(), node, e, now);
            default -> retryLater(lease, e, now, "waking it");
        }
    }

    private void refused(Held lease, NodeState node, NodeApiException e, Instant now) {
        if (lease.queued == null) {
            lease.queued = now;
        }
        node.waiting = true;
        node.lastRefusal = now;
        node.roomFreed = false;
        if (!node.warned) {
            node.warned = true;
            LOG.warnv("Pod {0} was not woken for workload {1}: {2}. Floci keeps its lease, and wakes it once "
                    + "another pod on the node sleeps or is deleted, or within {3} minutes", lease, lease.workload,
                    e.getMessage(), ROOM_WAIT.toMinutes());
        }
    }

    /** On each node that waits for room, wakes its oldest refused pod once room frees or the wait ends. */
    private void retryRefused(Map<Key, Want> wanted, Instant now) {
        for (Map.Entry<String, NodeState> entry : nodes.entrySet()) {
            String name = entry.getKey();
            NodeState node = entry.getValue();
            if (!node.waiting) {
                continue;
            }
            List<Held> queued = held.values().stream()
                    .filter(lease -> lease.key.node().equals(name) && lease.queued != null)
                    .sorted(Comparator.comparing(lease -> lease.queued))
                    .toList();
            if (queued.isEmpty()) {
                LOG.debugv("{0} has no refused wakes left", name);
                node.waiting = false;
                node.warned = false;
                node.roomFreed = false;
                continue;
            }
            Held first = queued.stream()
                    .filter(lease -> wanted.get(lease.key) != null && wanted.get(lease.key).mode() == Mode.WAKE)
                    .filter(lease -> wakeable(lease, node, now))
                    .findFirst().orElse(null);
            if (first == null || node.retrying) {
                continue;
            }
            if (node.roomFreed || !now.isBefore(node.lastRefusal.plus(ROOM_WAIT))) {
                node.retrying = true;
                node.roomFreed = false;
                LOG.debugv("Waking pod {0} again, which {1} refused for want of room", first, name);
                wake(first, true);
            }
        }
    }

    /** Handles a failed lease call. */
    private void callFailed(Held lease, NodeState node, NodeApiException e, Instant now, String call) {
        switch (e.code()) {
            case NOT_FOUND, GONE, STALE_INCARNATION, FAILED -> forget(lease, e);
            case UNREACHABLE, UNAUTHORIZED -> nodeFailed(lease.key.node(), node, e, now);
            default -> retryLater(lease, e, now, call);
        }
    }

    /** Forgets a pod that failed, is gone or has a new incarnation, and lists its namespace again. */
    private void forget(Held lease, NodeApiException e) {
        LOG.debugv("Forgetting pod {0}: {1}", lease, e.getMessage());
        held.remove(lease.key);
        listed.remove(lease.key.leaseId());
        pods.forget(lease.key.node(), lease.namespace);
    }

    private void retryLater(Held lease, NodeApiException e, Instant now, String call) {
        lease.retry = RetryBackoff.next(lease.retry);
        lease.retryAt = now.plus(lease.retry);
        if (e.code() == NodeApiException.Code.INVALID) {
            LOG.errorv("{0} refused Floci''s call {1} on pod {2} as invalid; Floci tries again in {3} s: {4}",
                    lease.key.node(), call, lease, lease.retry.toSeconds(), e.getMessage());
        } else if (lease.retry.equals(RetryBackoff.FIRST)) {
            LOG.warnv("{0} on pod {1} failed; Floci tries again: {2}", call, lease, e.getMessage());
        } else {
            LOG.debugv("{0} on pod {1} failed again; Floci tries again in {2} s: {3}", call, lease,
                    lease.retry.toSeconds(), e.getMessage());
        }
    }

    private void nodeFailed(String name, NodeState node, NodeApiException e, Instant now) {
        node.retry = RetryBackoff.next(node.retry);
        node.retryAt = now.plus(node.retry);
        if (e.code() == NodeApiException.Code.UNAUTHORIZED) {
            if (node.unauthorizedLogged == null
                    || !now.isBefore(node.unauthorizedLogged.plus(UNAUTHORIZED_LOG_INTERVAL))) {
                node.unauthorizedLogged = now;
                LOG.errorv("The node service of {0} refuses Floci''s token, so Floci cannot wake its pods: {1}",
                        name, e.getMessage());
            }
        } else if (!node.failing) {
            LOG.warnv("Floci cannot call the node service of {0}, and tries again within {1} s: {2}", name,
                    RetryBackoff.MAX.toSeconds(), e.getMessage());
        } else {
            LOG.debugv("Calling the node service of {0} failed again: {1}", name, e.getMessage());
        }
        node.failing = true;
    }

    private void answered(String name, NodeState node) {
        if (node.failing) {
            LOG.infov("The node service of {0} answers again", name);
        }
        node.failing = false;
        node.retry = null;
        node.retryAt = null;
    }

    private NodeState node(String name) {
        return nodes.computeIfAbsent(name, ignored -> new NodeState());
    }

    private static boolean callable(NodeState node, Held lease, Instant now) {
        return !backsOff(node, now) && (lease.retryAt == null || !now.isBefore(lease.retryAt));
    }

    private static boolean backsOff(NodeState node, Instant now) {
        return node.retryAt != null && now.isBefore(node.retryAt);
    }

    /** Forgets the retry times that have passed: the calls they held back were made, or are no longer wanted. */
    private void endRetries(Instant now) {
        for (Held lease : held.values()) {
            if (lease.retryAt != null && !now.isBefore(lease.retryAt)) {
                lease.retryAt = null;
            }
        }
        for (NodeState node : nodes.values()) {
            if (node.retryAt != null && !now.isBefore(node.retryAt)) {
                node.retryAt = null;
            }
        }
    }

    private static Instant earliest(Instant a, Instant b) {
        if (a == null) {
            return b;
        }
        return b == null || a.isBefore(b) ? a : b;
    }

    private static String prefix(String instanceId) {
        return instanceId == null ? LEASE_PREFIX : LEASE_PREFIX + instanceId + ":";
    }

    /** Whether {@code leaseId} is the lease of a workload of the Floci whose leases start with {@code prefix}. */
    private static boolean own(String prefix, String leaseId) {
        return leaseId.startsWith(prefix) && leaseId.length() > prefix.length()
                && leaseId.indexOf(':', prefix.length()) < 0;
    }
}
