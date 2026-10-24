package io.github.hectorvent.floci.runtime.wakes;

import io.github.hectorvent.floci.core.common.Resettable;
import io.github.hectorvent.floci.runtime.RetryBackoff;
import io.github.hectorvent.floci.runtime.node.NodeEvent;
import io.github.hectorvent.floci.runtime.node.RuntimeFile;
import io.github.hectorvent.floci.runtime.node.RuntimeFileWatcher;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeDirectory;
import io.github.hectorvent.floci.runtime.wakes.LeaseKeeper.Demand;
import io.github.hectorvent.floci.runtime.wakes.WorkloadActivity.Found;
import io.github.hectorvent.floci.runtime.wakes.WorkloadActivity.Read;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Workload;
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
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

/**
 * Wakes the pods of the runtime file's workloads from the events of their queues, and keeps them
 * awake while their consumers work. SQS reports every change to a watched queue
 * ({@link SqsQueueWatch}); the node reports its pods ({@link NodeEventWatch}). One thread,
 * {@code message-wakes}, applies the lease rule of each workload ({@link WorkloadActivity}) and
 * hands the result to {@link LeaseKeeper}. It parks until an event, a wake's answer, a node event
 * or the first due time: the end of a workload's linger, a delayed message's time, a lease's
 * renewal or a retry. No queue is read while nothing happens.
 *
 * <p>RabbitMQ queues report through their broker's firehose ({@link RabbitMqQueueWatch}). Each
 * watched queue is read once when Floci starts, after a reset, when a reload adds it and when a
 * RabbitMQ subscription starts. The thread runs while the runtime file has enabled workloads.
 */
@ApplicationScoped
public class MessageWakeService implements Resettable {

    /** A park with a timer pending lasts at most this long, so that a timer stays right after the host sleeps. */
    static final Duration MAX_TIMED_PARK = Duration.ofSeconds(5);
    static final Duration RESET_WAIT = Duration.ofSeconds(10);

    private static final Logger LOG = Logger.getLogger(MessageWakeService.class);
    private static final long NONE = Long.MAX_VALUE;

    /** Parks the wake loop's thread until an unpark, or for at most a time. */
    interface Parker {

        Parker SYSTEM = new Parker() {
            @Override
            public void park(Object blocker) {
                LockSupport.park(blocker);
            }

            @Override
            public void parkNanos(Object blocker, long nanos) {
                LockSupport.parkNanos(blocker, nanos);
            }
        };

        void park(Object blocker);

        void parkNanos(Object blocker, long nanos);
    }

    private final RuntimeFileWatcher runtimeFile;
    private final WorkloadsSection workloads;
    private final SqsQueueWatch sqs;
    private final RabbitMqQueueWatch rabbitMq;
    private final LeaseKeeper leases;
    private final NodeEventWatch nodeEvents;
    private final SmolvmNodeDirectory nodes;
    private final InstantSource clock;
    private final Parker parker;
    private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    private final Map<String, WorkloadActivity> activities = new LinkedHashMap<>();
    private final NodeEventWatch.Listener nodeListener = new NodeEventWatch.Listener() {
        @Override
        public void event(String node, NodeEvent event) {
            leases.nodeEvent(node, event);
        }

        @Override
        public void started(String node) {
            leases.followStarted(node);
        }
    };
    private volatile Thread loop;
    private volatile boolean leasesChanged;
    private volatile RuntimeFile latest;
    private volatile boolean stopped;
    private RuntimeFile applied;
    private String instanceId;
    private volatile boolean nodesChanged;

    @Inject
    public MessageWakeService(RuntimeFileWatcher runtimeFile, WorkloadsSection workloads, SqsQueueWatch sqs,
                              RabbitMqQueueWatch rabbitMq, LeaseKeeper leases, NodeEventWatch nodeEvents,
                              SmolvmNodeDirectory nodes) {
        this(runtimeFile, workloads, sqs, rabbitMq, leases, nodeEvents, nodes, InstantSource.system(), Parker.SYSTEM);
    }

    MessageWakeService(RuntimeFileWatcher runtimeFile, WorkloadsSection workloads, SqsQueueWatch sqs,
                       RabbitMqQueueWatch rabbitMq, LeaseKeeper leases, NodeEventWatch nodeEvents,
                       SmolvmNodeDirectory nodes, InstantSource clock, Parker parker) {
        this.runtimeFile = runtimeFile;
        this.workloads = workloads;
        this.sqs = sqs;
        this.rabbitMq = rabbitMq;
        this.leases = leases;
        this.nodeEvents = nodeEvents;
        this.nodes = nodes;
        this.clock = clock;
        this.parker = parker;
    }

    /** After {@code EmulatorLifecycle} has loaded storage, so the first reads see the queues. */
    void onStart(@Observes @Priority(Interceptor.Priority.APPLICATION + 600) StartupEvent ignored) {
        if (!runtimeFile.configured()) {
            return;
        }
        leases.onChange(() -> {
            leasesChanged = true;
            unpark();
        });
        sqs.start(this::unpark);
        rabbitMq.start(this::unpark);
        nodes.addListener(changed -> {
            nodesChanged = true;
            unpark();
        });
        runtimeFile.addListener(this::changed);
        changed(runtimeFile.current());
    }

    void onStop(@Observes ShutdownEvent ignored) {
        Thread thread;
        synchronized (this) {
            stopped = true;
            thread = loop;
        }
        if (thread != null) {
            LockSupport.unpark(thread);
        }
        nodeEvents.stopAll();
    }

    /** Releases every lease, makes every workload idle, and ends the RabbitMQ subscriptions. */
    @Override
    public void clear() {
        run(() -> {
            leases.releaseAll();
            rabbitMq.clear();
            for (WorkloadActivity activity : activities.values()) {
                activity.reset();
            }
        });
    }

    /**
     * Reads each watched queue once and subscribes to the watched brokers again, as at startup:
     * the reset assumed nothing about them.
     */
    @Override
    public void afterReset() {
        post(() -> {
            for (WorkloadActivity activity : activities.values()) {
                activity.readOnce();
            }
            rabbitMq.watch(activities.values());
        });
    }

    /** Whether the wake loop runs, for tests. */
    boolean running() {
        return loop != null;
    }

    /** Takes a new runtime file: starts the loop when it has enabled workloads, and tells a running loop. */
    private synchronized void changed(RuntimeFile file) {
        latest = file;
        if (stopped) {
            return;
        }
        if (loop == null) {
            if (file.section(workloads).enabled().isEmpty()) {
                return;
            }
            loop = Thread.ofPlatform().daemon().name("message-wakes").start(this::runLoop);
            return;
        }
        unpark();
    }

    private void runLoop() {
        while (true) {
            try {
                if (!turn()) {
                    return;
                }
            } catch (RuntimeException e) {
                LOG.errorv(e, "Waking the pods of the runtime file''s workloads failed; Floci tries again in {0} s",
                        RetryBackoff.FIRST.toSeconds());
                LockSupport.parkNanos(this, RetryBackoff.FIRST.toNanos());
            }
        }
    }

    /** One turn of the loop, then its park. Returns false once the loop has ended. */
    private boolean turn() {
        if (stopped) {
            end();
            return false;
        }
        Runnable task;
        while ((task = tasks.poll()) != null) {
            task.run();
        }
        RuntimeFile file = latest;
        boolean reloaded = file != applied;
        if (reloaded && !reload(file)) {
            return false;
        }
        if (nodesChanged) {
            nodesChanged = false;
            reloaded = true;
            followNodes();
        }
        long now = clock.millis();
        boolean changed = reloaded;
        List<Demand> demands = new ArrayList<>();
        for (WorkloadActivity activity : activities.values()) {
            changed |= activity.turn(now, this::read);
            demands.add(activity.demand());
        }
        Instant leaseDue = leases.nextDue().orElse(null);
        if (changed || leasesChanged || (leaseDue != null && !leaseDue.isAfter(clock.instant()))) {
            leasesChanged = false;
            leases.tick(instanceId, demands);
        }
        park(clock.millis());
        return true;
    }

    /**
     * Applies a runtime file: keeps each workload's activity by its name, reads the queues of a
     * new workload or of one whose queues changed once, and watches exactly the enabled
     * workloads' queues. With no enabled workload it releases every lease and ends the loop;
     * returns false then.
     */
    private boolean reload(RuntimeFile file) {
        applied = file;
        instanceId = file.instanceId().orElse(null);
        List<Workload> enabled = file.section(workloads).enabled();
        if (enabled.isEmpty()) {
            leases.tick(instanceId, List.of());
            activities.clear();
            sqs.watch(List.of());
            rabbitMq.watch(List.of());
            nodeEvents.stopAll();
            synchronized (this) {
                if (latest == applied || stopped) {
                    end();
                    LOG.info("The runtime file has no enabled workloads, so Floci wakes no pods for them");
                    return false;
                }
            }
            return true;
        }
        boolean starting = activities.isEmpty();
        Map<String, WorkloadActivity> next = new LinkedHashMap<>();
        for (Workload workload : enabled) {
            WorkloadActivity activity = activities.get(workload.name());
            if (activity == null) {
                activity = new WorkloadActivity(workload);
                activity.readOnce();
            } else if (!activity.workload().queues().equals(workload.queues())
                    || !Objects.equals(activity.workload().accountId(), workload.accountId())
                    || !Objects.equals(activity.workload().region(), workload.region())) {
                activity.define(workload);
                activity.readOnce();
            } else {
                activity.define(workload);
            }
            next.put(workload.name(), activity);
        }
        activities.clear();
        activities.putAll(next);
        sqs.watch(activities.values());
        rabbitMq.watch(activities.values());
        followNodes();
        if (starting) {
            LOG.infov("Floci wakes the pods of the runtime file''s workloads from the events of their queues: {0}",
                    String.join(", ", activities.keySet()));
        }
        return true;
    }

    /** Follows the pod events of each node that an enabled workload names. */
    private void followNodes() {
        Set<SmolvmNode> named = new HashSet<>();
        for (WorkloadActivity activity : activities.values()) {
            nodes.find(activity.workload().target().node()).ifPresent(named::add);
        }
        nodeEvents.watch(named, nodeListener);
    }

    /** One read of a workload's SQS queues and, unless they have work, of its RabbitMQ queues. */
    private Read read(Workload workload, long ticket) {
        LOG.debugv("Floci reads the queues of workload {0}", workload.name());
        Read sqsRead = workload.queues().isEmpty() ? Read.none(null) : sqs.read(workload);
        if (sqsRead.found() == Found.WORK || workload.rabbitMqQueues().isEmpty()) {
            return sqsRead;
        }
        Read rabbitMqRead = rabbitMq.read(workload, ticket);
        return switch (rabbitMqRead.found()) {
            case WORK, HELD -> Read.held(rabbitMqRead.resource(), sqsRead.nextVisible());
            case FAILED, PENDING -> rabbitMqRead;
            case NONE -> sqsRead;
        };
    }

    /** Parks until the first due time, or an unpark. */
    private void park(long now) {
        if (!tasks.isEmpty() || latest != applied || nodesChanged || leasesChanged) {
            return;
        }
        long due = NONE;
        for (WorkloadActivity activity : activities.values()) {
            due = Math.min(due, activity.due(now));
        }
        Instant leaseDue = leases.nextDue().orElse(null);
        if (leaseDue != null) {
            due = Math.min(due, leaseDue.toEpochMilli());
        }
        if (stopped) {
            return;
        }
        if (due == NONE) {
            parker.park(this);
        } else if (due > now) {
            parker.parkNanos(this, TimeUnit.MILLISECONDS.toNanos(Math.min(due - now, MAX_TIMED_PARK.toMillis())));
        }
    }

    /** Ends the loop, and runs the tasks that came for it, such as a reset, on its last turn. */
    private synchronized void end() {
        loop = null;
        Runnable task;
        while ((task = tasks.poll()) != null) {
            task.run();
        }
    }

    /** Runs {@code task} at the loop's next turn. */
    private void post(Runnable task) {
        tasks.add(task);
        unpark();
    }

    /** Runs {@code task} on the loop and waits for it, or runs it here when the loop does not run. */
    private void run(Runnable task) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        post(() -> {
            try {
                task.run();
                done.complete(null);
            } catch (RuntimeException e) {
                done.completeExceptionally(e);
            }
        });
        synchronized (this) {
            if (loop == null) {
                Runnable queued;
                while ((queued = tasks.poll()) != null) {
                    queued.run();
                }
            }
        }
        try {
            done.get(RESET_WAIT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            LOG.warnv("The message wake loop did not take the reset within {0} s", RESET_WAIT.toSeconds());
        } catch (ExecutionException e) {
            LOG.errorv(e.getCause(), "Resetting message wakes failed");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void unpark() {
        Thread thread = loop;
        if (thread != null) {
            LockSupport.unpark(thread);
        }
    }
}
