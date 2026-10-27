package io.github.hectorvent.floci.runtime.network;

import io.github.hectorvent.floci.runtime.RetryBackoff;
import io.github.hectorvent.floci.runtime.node.NodeNetworkEvent;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient.EventFollow;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeDirectory;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Follows the network events of every node of {@link SmolvmNodeDirectory} and logs each blocked
 * attempt at INFO, once per event, with the pod, the destination, port and protocol, the count
 * and the names it sent. The first follow of a node starts at the head of its events; a follow
 * that ends or fails starts again after {@link RetryBackoff#FIRST}, doubling up to {@link RetryBackoff#MAX},
 * after the last event it read.
 */
@ApplicationScoped
public class NodeNetworkEvents {


    private static final Logger LOG = Logger.getLogger(NodeNetworkEvents.class);

    /** One node's follow, from its first start until the node leaves the directory. */
    private static final class Watch {
        final SmolvmNode node;
        EventFollow follow;
        ScheduledFuture<?> pending;
        Duration retry;
        long startedNanos;
        /** The last event read, or -1 before the first follow. */
        long cursor = -1;
        boolean failing;
        boolean closed;

        Watch(SmolvmNode node) {
            this.node = node;
        }
    }

    private final SmolvmNodeDirectory directory;
    private final SmolvmNodeClient client;
    private final ScheduledExecutorService starts;
    private final Map<String, Watch> watches = new HashMap<>();

    @Inject
    public NodeNetworkEvents(SmolvmNodeDirectory directory, SmolvmNodeClient client) {
        this(directory, client, Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon()
                .name("smolvm-node-network-events").factory()));
    }

    NodeNetworkEvents(SmolvmNodeDirectory directory, SmolvmNodeClient client, ScheduledExecutorService starts) {
        this.directory = directory;
        this.client = client;
        this.starts = starts;
    }

    void onStart(@Observes @Priority(Interceptor.Priority.APPLICATION + 620) StartupEvent ignored) {
        directory.addListener(this::follow);
        follow(directory.all());
    }

    @PreDestroy
    void close() {
        synchronized (this) {
            for (Watch watch : watches.values()) {
                stop(watch);
            }
            watches.clear();
        }
        starts.shutdownNow();
    }

    /** Follows exactly {@code nodes}, starting again for a node whose address or token changed. */
    synchronized void follow(Collection<SmolvmNode> nodes) {
        Map<String, SmolvmNode> wanted = new HashMap<>();
        for (SmolvmNode node : nodes) {
            wanted.put(node.name(), node);
        }
        Iterator<Map.Entry<String, Watch>> current = watches.entrySet().iterator();
        while (current.hasNext()) {
            Watch watch = current.next().getValue();
            if (!watch.node.equals(wanted.get(watch.node.name()))) {
                stop(watch);
                current.remove();
            }
        }
        for (SmolvmNode node : wanted.values()) {
            if (!watches.containsKey(node.name())) {
                Watch watch = new Watch(node);
                watches.put(node.name(), watch);
                schedule(watch, Duration.ZERO);
            }
        }
    }

    /** Whether a follow of {@code node} runs, for tests. */
    synchronized boolean following(String node) {
        Watch watch = watches.get(node);
        return watch != null && watch.follow != null;
    }

    /** The last network event of {@code node} that a follow read, or -1, for tests. */
    synchronized long cursor(String node) {
        Watch watch = watches.get(node);
        return watch == null ? -1 : watch.cursor;
    }

    private void stop(Watch watch) {
        watch.closed = true;
        if (watch.pending != null) {
            watch.pending.cancel(false);
        }
        if (watch.follow != null) {
            watch.follow.close();
            watch.follow = null;
        }
    }

    private void schedule(Watch watch, Duration delay) {
        try {
            watch.pending = starts.schedule(() -> start(watch), delay.toMillis(), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException closed) {
            LOG.debugv("Not following the network events of {0}: Floci stops", watch.node.name());
        }
    }

    private void start(Watch watch) {
        String name = watch.node.name();
        long cursor;
        synchronized (this) {
            if (watch.closed) {
                return;
            }
            cursor = watch.cursor;
        }
        EventFollow follow;
        try {
            if (cursor < 0) {
                cursor = client.networkEventsHead(watch.node);
                synchronized (this) {
                    watch.cursor = cursor;
                }
            }
            follow = client.followNetwork(watch.node, cursor, event -> read(watch, event));
        } catch (RuntimeException e) {
            failed(watch, e);
            return;
        }
        synchronized (this) {
            if (watch.closed) {
                follow.close();
                return;
            }
            watch.follow = follow;
            watch.startedNanos = System.nanoTime();
            if (watch.failing) {
                LOG.infov("Floci follows the network events of {0} again", name);
                watch.failing = false;
            } else {
                LOG.debugv("Following the network events of {0}", name);
            }
        }
        follow.done().whenComplete((ignored, failure) -> ended(watch, follow, failure));
    }

    private void read(Watch watch, NodeNetworkEvent event) {
        synchronized (this) {
            if (event.seq() <= watch.cursor) {
                return;
            }
            watch.cursor = event.seq();
        }
        log(watch.node.name(), event);
    }

    static void log(String node, NodeNetworkEvent event) {
        String names = event.names().isEmpty() ? "" : ", names " + String.join(", ", event.names());
        switch (event.kind()) {
            case "blocked" -> LOG.infov("{0} blocked {1}: {2} to {3}:{4,number,#}, count {5,number,#}{6}", node,
                    event.pod(), event.protocol(), event.destination(), event.port(), event.count(), names);
            case "relay_failed" -> LOG.debugv("{0} could not relay {1}: {2} to {3}:{4,number,#}, count "
                    + "{5,number,#}, route {6}: {7}", node, event.pod(), event.protocol(), event.destination(),
                    event.port(), event.count(), event.route(), event.error());
            default -> LOG.debugv("{0} relayed {1}: {2} to {3}:{4,number,#}, count {5,number,#}, route {6}", node,
                    event.pod(), event.protocol(), event.destination(), event.port(), event.count(), event.route());
        }
    }

    private synchronized void ended(Watch watch, EventFollow follow, Throwable failure) {
        if (watch.closed || watch.follow != follow) {
            return;
        }
        watch.follow = null;
        if (System.nanoTime() - watch.startedNanos >= RetryBackoff.MAX.toNanos()) {
            watch.retry = null;
        }
        if (failure == null) {
            LOG.debugv("The node service of {0} ended the follow of its network events; following again",
                    watch.node.name());
            retry(watch);
        } else {
            failed(watch, failure);
        }
    }

    private synchronized void failed(Watch watch, Throwable failure) {
        if (watch.closed) {
            return;
        }
        if (!watch.failing) {
            watch.failing = true;
            LOG.infov("Floci cannot follow the network events of {0}, and tries again within {1} s: {2}",
                    watch.node.name(), RetryBackoff.MAX.toSeconds(), failure.getMessage());
        } else {
            LOG.debugv("Following the network events of {0} failed again: {1}", watch.node.name(),
                    failure.getMessage());
        }
        retry(watch);
    }

    private void retry(Watch watch) {
        watch.retry = RetryBackoff.next(watch.retry);
        schedule(watch, watch.retry);
    }
}
