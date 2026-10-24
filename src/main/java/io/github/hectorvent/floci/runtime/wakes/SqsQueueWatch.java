package io.github.hectorvent.floci.runtime.wakes;

import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.runtime.wakes.WorkloadActivity.Read;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Workload;
import io.github.hectorvent.floci.services.sqs.SqsQueueId;
import io.github.hectorvent.floci.services.sqs.SqsQueueListener;
import io.github.hectorvent.floci.services.sqs.SqsService;
import io.github.hectorvent.floci.services.sqs.SqsService.QueueWork;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The SQS side of message wakes: hears every change to SQS queues from {@link SqsService}, hands
 * the changes of the queues that workloads name to their {@link WorkloadActivity}, and reads a
 * workload's queues when the wake loop decides. A message that can be received now is work; a
 * delayed one sets a timer; every other change is activity. A queue that does not exist has no
 * work.
 */
@ApplicationScoped
public class SqsQueueWatch implements SqsQueueListener {

    private static final Logger LOG = Logger.getLogger(SqsQueueWatch.class);

    /** A watched queue: its ARN, as a wake names it, and the workloads that read it. */
    private record Watched(String arn, List<WorkloadActivity> activities) {
    }

    private final SqsService sqs;
    private final RegionResolver regionResolver;
    private final InstantSource clock;
    private final Map<String, AtomicLong> reads = new ConcurrentHashMap<>();
    private final Set<SqsQueueId> missing = new HashSet<>();
    private final Set<SqsQueueId> unreadable = new HashSet<>();
    private volatile Map<SqsQueueId, Watched> watched = Map.of();
    private volatile Runnable loop = () -> { };
    private boolean listening;

    @Inject
    public SqsQueueWatch(SqsService sqs, RegionResolver regionResolver) {
        this(sqs, regionResolver, InstantSource.system());
    }

    SqsQueueWatch(SqsService sqs, RegionResolver regionResolver, InstantSource clock) {
        this.sqs = sqs;
        this.regionResolver = regionResolver;
        this.clock = clock;
    }

    /** Listens to SQS from now on, and runs {@code loop} when the wake loop must look at a workload. */
    public synchronized void start(Runnable loop) {
        this.loop = loop;
        if (!listening) {
            listening = true;
            sqs.addQueueListener(this);
        }
    }

    /** Watches the SQS queues of exactly these workloads. */
    void watch(Collection<WorkloadActivity> activities) {
        Map<SqsQueueId, Watched> next = new HashMap<>();
        for (WorkloadActivity activity : activities) {
            Workload workload = activity.workload();
            for (String queue : workload.queues()) {
                SqsQueueId id = id(workload, queue);
                next.computeIfAbsent(id, key -> new Watched(regionResolver.buildArn("sqs", key.region(),
                        key.accountId(), key.name()), new ArrayList<>())).activities().add(activity);
            }
        }
        Map<SqsQueueId, Watched> frozen = new HashMap<>();
        next.forEach((id, entry) -> frozen.put(id, new Watched(entry.arn(), List.copyOf(entry.activities()))));
        watched = Map.copyOf(frozen);
        missing.retainAll(frozen.keySet());
        unreadable.retainAll(frozen.keySet());
    }

    @Override
    public void messageAvailable(SqsQueueId queue, Instant at) {
        Watched entry = watched.get(queue);
        if (entry == null) {
            return;
        }
        long now = clock.millis();
        long from = at.toEpochMilli();
        boolean look = false;
        for (WorkloadActivity activity : entry.activities()) {
            look |= from <= now ? activity.work(now, entry.arn()) : activity.delayed(from);
        }
        if (look) {
            loop.run();
        }
    }

    @Override
    public void messagesChanged(SqsQueueId queue) {
        Watched entry = watched.get(queue);
        if (entry == null) {
            return;
        }
        long now = clock.millis();
        boolean look = false;
        for (WorkloadActivity activity : entry.activities()) {
            look |= activity.activity(now);
        }
        if (look) {
            loop.run();
        }
    }

    /**
     * Reads the workload's SQS queues once: work for the first queue whose messages are visible
     * or in flight; else failed when a queue could not be read; else none. Either way with the
     * earliest time a delayed message becomes visible.
     */
    Read read(Workload workload) {
        reads.computeIfAbsent(workload.name(), name -> new AtomicLong()).incrementAndGet();
        boolean failed = false;
        String busy = null;
        Instant nextVisible = null;
        for (String queue : workload.queues()) {
            SqsQueueId id = id(workload, queue);
            Optional<QueueWork> work;
            try {
                work = sqs.queueWork(id);
            } catch (RuntimeException e) {
                if (unreadable.add(id)) {
                    LOG.warnv(e, "Reading the SQS queue {0} of workload {1} failed; Floci keeps the leases on its "
                            + "pods and tries again", queue, workload.name());
                }
                failed = true;
                continue;
            }
            if (unreadable.remove(id)) {
                LOG.infov("Floci reads the SQS queue {0} in account {1} and region {2} again", queue, id.accountId(),
                        id.region());
            }
            if (work.isEmpty()) {
                if (missing.add(id)) {
                    LOG.infov("Workload {0} reads the SQS queue {1} in account {2} and region {3}, which does not "
                            + "exist; its messages wake the workload''s pods once it does", workload.name(), queue,
                            id.accountId(), id.region());
                }
                continue;
            }
            missing.remove(id);
            if (busy == null && work.get().any()) {
                busy = regionResolver.buildArn("sqs", id.region(), id.accountId(), queue);
            }
            Instant visible = work.get().nextVisible();
            if (visible != null && (nextVisible == null || visible.isBefore(nextVisible))) {
                nextVisible = visible;
            }
        }
        if (busy != null) {
            return Read.work(busy, nextVisible);
        }
        return failed ? Read.failed() : Read.none(nextVisible);
    }

    /** How many reads of the workload's queues were made, for tests. */
    long reads(String workload) {
        AtomicLong count = reads.get(workload);
        return count == null ? 0 : count.get();
    }

    private static SqsQueueId id(Workload workload, String queue) {
        return new SqsQueueId(workload.accountId(), workload.region(), queue);
    }
}
