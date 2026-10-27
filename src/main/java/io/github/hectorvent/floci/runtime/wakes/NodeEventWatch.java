package io.github.hectorvent.floci.runtime.wakes;

import io.github.hectorvent.floci.runtime.RetryBackoff;
import io.github.hectorvent.floci.runtime.node.NodeEvent;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient.EventFollow;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
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
 * Follows the pod events of the nodes that workloads name, so that Floci learns of new, changed
 * and deleted pods as they happen. Each follow starts at the head of the node's event log; once
 * it runs, the listener hears {@link Listener#started}, so that it lists the node's pods after
 * the follow began and misses no change in between. A follow that ends or fails starts again
 * after {@link RetryBackoff#FIRST}, doubling up to {@link RetryBackoff#MAX}, from a new head.
 */
@ApplicationScoped
public class NodeEventWatch {


    private static final Logger LOG = Logger.getLogger(NodeEventWatch.class);

    /** Hears the events of the followed nodes, on the threads of the follows. Returns at once. */
    public interface Listener {

        /** A pod event of {@code node}, such as {@code node:dev}. */
        void event(String node, NodeEvent event);

        /** A follow of {@code node} began: events after the moment of the call come through it. */
        void started(String node);
    }

    /** One node's follow, from its first start until the node is no longer watched. */
    private final class Watch {
        final SmolvmNode node;
        EventFollow follow;
        ScheduledFuture<?> pending;
        Duration retry;
        long startedNanos;
        boolean failing;
        boolean closed;

        Watch(SmolvmNode node) {
            this.node = node;
        }
    }

    private final SmolvmNodeClient client;
    private final ScheduledExecutorService starts;
    private final Map<String, Watch> watches = new HashMap<>();
    private volatile Listener listener;

    @Inject
    public NodeEventWatch(SmolvmNodeClient client) {
        this(client, Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon()
                .name("smolvm-node-events").factory()));
    }

    /** @param starts runs each start of a follow: the head query and the follow's request */
    NodeEventWatch(SmolvmNodeClient client, ScheduledExecutorService starts) {
        this.client = client;
        this.starts = starts;
    }

    @PreDestroy
    void close() {
        stopAll();
        starts.shutdownNow();
    }

    /**
     * Follows exactly {@code nodes}: starts a follow for each new node or node whose address or
     * token changed, and closes the follows of the others.
     */
    public synchronized void watch(Collection<SmolvmNode> nodes, Listener listener) {
        this.listener = listener;
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

    /** Closes every follow. */
    public synchronized void stopAll() {
        for (Watch watch : watches.values()) {
            stop(watch);
        }
        watches.clear();
    }

    /** Whether a follow of {@code node} runs, for tests. */
    synchronized boolean following(String node) {
        Watch watch = watches.get(node);
        return watch != null && watch.follow != null;
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
            LOG.debugv("Not following the events of {0}: Floci stops", watch.node.name());
        }
    }

    /** Reads the head of the node's events and follows them from there. */
    private void start(Watch watch) {
        String name = watch.node.name();
        synchronized (this) {
            if (watch.closed) {
                return;
            }
        }
        EventFollow follow;
        try {
            long head = client.eventsHead(watch.node);
            follow = client.follow(watch.node, head, null, event -> listener.event(name, event));
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
                LOG.infov("Floci follows the pod events of {0} again", name);
                watch.failing = false;
            } else {
                LOG.debugv("Following the pod events of {0}", name);
            }
        }
        listener.started(name);
        follow.done().whenComplete((ignored, failure) -> ended(watch, follow, failure));
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
            LOG.debugv("The node service of {0} ended the follow of its pod events; following again",
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
            LOG.warnv("Floci cannot follow the pod events of {0}, and tries again within {1} s: {2}",
                    watch.node.name(), RetryBackoff.MAX.toSeconds(), failure.getMessage());
        } else {
            LOG.debugv("Following the pod events of {0} failed again: {1}", watch.node.name(), failure.getMessage());
        }
        retry(watch);
    }

    private void retry(Watch watch) {
        watch.retry = RetryBackoff.next(watch.retry);
        schedule(watch, watch.retry);
    }
}
