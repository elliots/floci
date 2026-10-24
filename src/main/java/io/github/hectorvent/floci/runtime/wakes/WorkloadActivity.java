package io.github.hectorvent.floci.runtime.wakes;

import io.github.hectorvent.floci.runtime.RetryBackoff;
import io.github.hectorvent.floci.runtime.wakes.LeaseKeeper.Demand;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Workload;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What the events of one workload's queues say, and the lease rule that follows from them. An
 * event that shows work makes an idle workload busy, so its pods are leased and woken. Every
 * event of its queues restarts the linger. Once the workload's idle timeout passes with no event,
 * one read of its queues decides: no work releases the leases; SQS work keeps them until the next
 * event. Messages in RabbitMQ queues keep the leases and are read again one linger later, since
 * the broker tells nobody of an acknowledgement, nor of a message that leaves a queue undelivered.
 * A delayed message is no work until it is visible; its time is a timer of an idle workload.
 *
 * <p>Events come from the threads that change the queues and touch only atomic and volatile
 * fields: {@link #work}, {@link #activity}, {@link #delayed} and {@link #requestRead}, which
 * return whether the wake loop must look at the workload. Everything else runs on the wake loop's
 * thread. A read may answer later ({@link Found#PENDING}); the loop asks again, with the same
 * ticket, once the reader tells it the answer is there. Times are epoch milliseconds.
 */
final class WorkloadActivity {

    /** Decisions for one workload are at least this far apart. */
    static final Duration MIN_DECISION_GAP = Duration.ofSeconds(1);

    private static final long NONE = Long.MAX_VALUE;

    /** What a read of a workload's queues found. */
    enum Found {
        /** Messages wait or are in flight. */
        WORK,
        /** RabbitMQ messages are ready or unacknowledged: they are read again one linger later. */
        HELD,
        /** Every queue is empty, or missing. */
        NONE,
        /** A queue could not be read. */
        FAILED,
        /** The answer comes later: the reader tells the loop when, and the loop asks again. */
        PENDING
    }

    /**
     * One read of a workload's queues.
     *
     * @param resource    the queue with work, as a wake names it; null unless {@code WORK} or {@code HELD}
     * @param nextVisible when the first delayed SQS message becomes visible, or null
     */
    record Read(Found found, String resource, Instant nextVisible) {

        Read {
            Objects.requireNonNull(found, "found");
            if (found == Found.WORK || found == Found.HELD) {
                Objects.requireNonNull(resource, "resource");
            }
        }

        static Read work(String resource, Instant nextVisible) {
            return new Read(Found.WORK, resource, nextVisible);
        }

        static Read held(String resource, Instant nextVisible) {
            return new Read(Found.HELD, resource, nextVisible);
        }

        static Read none(Instant nextVisible) {
            return new Read(Found.NONE, null, nextVisible);
        }

        static Read failed() {
            return new Read(Found.FAILED, null, null);
        }

        static Read pending() {
            return new Read(Found.PENDING, null, null);
        }
    }

    /** Reads a workload's queues, on the wake loop's thread. */
    @FunctionalInterface
    interface Reader {

        /**
         * Reads the workload's queues, or answers {@link Found#PENDING} and tells the loop once
         * the answer is there. {@code ticket} names one read: the loop asks again with the same
         * ticket for its answer, and a new ticket starts a new read.
         */
        Read read(Workload workload, long ticket);
    }

    private volatile boolean busy;
    private final AtomicLong events = new AtomicLong();
    private volatile long lastEvent;
    private final AtomicLong nextDelayed = new AtomicLong(NONE);
    private final AtomicLong readAt = new AtomicLong(NONE);
    private volatile boolean signal;
    private volatile String signalled;
    private volatile long signalledEvents;
    private volatile boolean waitingForEvent;

    private Workload workload;
    private boolean readPending;
    private long busySince;
    private long lastDecision = Long.MIN_VALUE;
    private long eventsAtKeep;
    private boolean unknown;
    private long retryAt;
    private Duration retry;
    private String resource;
    private long tickets;
    private boolean reading;
    private long eventsAtRead;
    private long heldAt = Long.MIN_VALUE;
    private long eventsAtRelease = -1;
    private Runnable releasing = () -> { };

    WorkloadActivity(Workload workload) {
        this.workload = workload;
    }

    Workload workload() {
        return workload;
    }

    /**
     * An event that shows work, such as a message that can be received now. Returns whether the
     * loop must look at the workload: it was idle and is now busy, or the loop waits for its next
     * event.
     *
     * @param resource the queue, as a wake names it
     */
    boolean work(long now, String resource) {
        long count = events.incrementAndGet();
        lastEvent = now;
        if (!busy) {
            signalled = resource;
            signalledEvents = count;
            signal = true;
            return true;
        }
        return waitingForEvent;
    }

    /**
     * Any other event of the workload's queues: a receive, a delete, a visibility change. Returns
     * whether the loop waits for it, after a read that found work.
     */
    boolean activity(long now) {
        events.incrementAndGet();
        lastEvent = now;
        return waitingForEvent;
    }

    /**
     * A message that can be received from {@code at}, later than now. Returns whether it is the
     * earliest of an idle workload, so that the loop sets its timer.
     */
    boolean delayed(long at) {
        return lower(at) && !busy;
    }

    /**
     * Reads the workload's queues at its next turn, as at startup, after a reset or after a
     * reload. A busy workload that waits for its next event decides at the end of its linger
     * instead, since the events it waits for may be of queues it does not read now.
     */
    void readOnce() {
        readPending = true;
    }

    /**
     * Reads the workload's queues once at {@code at}: after a subscription to a queue's events
     * starts, or when some of its events were lost. A busy workload that waits for its next event
     * decides at the end of its linger instead. Returns whether the loop must look at the
     * workload sooner than it planned.
     */
    boolean requestRead(long at) {
        long current = readAt.get();
        while (at < current) {
            if (readAt.compareAndSet(current, at)) {
                return true;
            }
            current = readAt.get();
        }
        return false;
    }

    /** Takes a new definition from a reload; a new idle timeout moves the next decision. */
    void define(Workload definition) {
        if (!definition.equals(workload)) {
            reading = false;
        }
        workload = definition;
    }

    /** Idle, with no timer and no read to make. */
    void reset() {
        busy = false;
        signal = false;
        signalled = null;
        waitingForEvent = false;
        nextDelayed.set(NONE);
        readAt.set(NONE);
        readPending = false;
        reading = false;
        heldAt = Long.MIN_VALUE;
        unknown = false;
        retry = null;
        retryAt = 0;
        resource = null;
    }

    /** What the leases of the workload's pods should do. */
    Demand demand() {
        if (!busy) {
            return Demand.none(workload);
        }
        if (unknown) {
            return Demand.unknown(workload);
        }
        return Demand.waiting(workload, resource);
    }

    boolean busy() {
        return busy;
    }

    /**
     * The loop's turn for the workload at {@code now}: makes it busy on a signal or a read that
     * finds work, and decides at the end of its linger. Returns whether its demand changed.
     */
    boolean turn(long now, Reader reader) {
        Demand before = demand();
        long requested = readAt.get();
        if (requested <= now && readAt.compareAndSet(requested, NONE)) {
            if (busy) {
                waitingForEvent = false;
            } else {
                readPending = true;
            }
        }
        if (!busy) {
            idleTurn(now, reader);
        }
        if (busy) {
            if (readPending) {
                readPending = false;
                waitingForEvent = false;
            }
            if (waitingForEvent && events.get() != eventsAtKeep) {
                waitingForEvent = false;
            }
            if (reading || now >= decisionTime()) {
                decide(now, reader);
            }
        }
        return !before.equals(demand());
    }

    /** When the loop must next look at the workload, or {@link Long#MAX_VALUE} for no time. */
    long due(long now) {
        long requested = readAt.get();
        if (reading) {
            return requested;
        }
        if (!busy) {
            if (signal) {
                return now;
            }
            long due = Math.min(requested, readPending ? retryAt : NONE);
            long delayed = nextDelayed.get();
            if (delayed != NONE) {
                due = Math.min(due, Math.max(delayed, retryAt));
            }
            return due;
        }
        if (readPending) {
            return now;
        }
        if (waitingForEvent && events.get() != eventsAtKeep) {
            return now;
        }
        return Math.min(requested, decisionTime());
    }

    private void idleTurn(long now, Reader reader) {
        if (signal) {
            signal = false;
            if (signalledEvents > eventsAtRelease) {
                becomeBusy(now, signalled);
                return;
            }
        }
        boolean timer = nextDelayed.get() <= now;
        if (reading || ((readPending || timer) && now >= retryAt)) {
            if (!reading) {
                tickets++;
            }
            long previous = nextDelayed.getAndSet(NONE);
            Read read = reader.read(workload, tickets);
            reading = read.found() == Found.PENDING;
            switch (read.found()) {
                case WORK, HELD -> {
                    readPending = false;
                    succeeded();
                    becomeBusy(now, read.resource());
                    if (read.found() == Found.HELD) {
                        heldAt = now;
                    }
                    lower(read.nextVisible());
                }
                case NONE -> {
                    readPending = false;
                    succeeded();
                    lower(read.nextVisible());
                }
                case FAILED -> {
                    lower(previous);
                    failed(now);
                }
                case PENDING -> lower(previous);
            }
        }
    }

    /** The read at the end of the linger: releases when the queues hold nothing and no event came during it. */
    private void decide(long now, Reader reader) {
        if (!reading) {
            lastDecision = now;
            eventsAtRead = events.get();
            tickets++;
        }
        long before = eventsAtRead;
        long previous = nextDelayed.getAndSet(NONE);
        Read read = reader.read(workload, tickets);
        reading = read.found() == Found.PENDING;
        switch (read.found()) {
            case WORK -> {
                unknown = false;
                succeeded();
                resource = read.resource();
                eventsAtKeep = before;
                waitingForEvent = true;
                lower(read.nextVisible());
            }
            case HELD -> {
                unknown = false;
                succeeded();
                resource = read.resource();
                heldAt = now;
                lower(read.nextVisible());
            }
            case NONE -> {
                unknown = false;
                succeeded();
                busy = false;
                releasing.run();
                if (events.get() != before) {
                    busy = true;
                } else {
                    eventsAtRelease = before;
                }
                lower(read.nextVisible());
            }
            case FAILED -> {
                lower(previous);
                unknown = true;
                failed(now);
            }
            case PENDING -> lower(previous);
        }
    }

    private long decisionTime() {
        if (unknown) {
            return retryAt;
        }
        if (waitingForEvent) {
            return NONE;
        }
        long last = Math.max(Math.max(lastEvent, busySince), heldAt);
        long time = last + workload.idleTimeout().toMillis();
        return Math.max(time, Math.max(busySince, lastDecision) + MIN_DECISION_GAP.toMillis());
    }

    private void becomeBusy(long now, String queue) {
        busy = true;
        busySince = now;
        waitingForEvent = false;
        unknown = false;
        reading = false;
        heldAt = Long.MIN_VALUE;
        if (queue != null) {
            resource = queue;
        }
    }

    private void succeeded() {
        retry = null;
        retryAt = 0;
    }

    private void failed(long now) {
        retry = RetryBackoff.next(retry);
        retryAt = now + retry.toMillis();
    }

    private boolean lower(Instant at) {
        return at != null && lower(at.toEpochMilli());
    }

    /** Lowers the earliest delayed time to {@code at}; returns whether it did. */
    private boolean lower(long at) {
        long current = nextDelayed.get();
        while (at < current) {
            if (nextDelayed.compareAndSet(current, at)) {
                return true;
            }
            current = nextDelayed.get();
        }
        return false;
    }

    /** Runs {@code step} between a release and its check for events that came during the read, for tests. */
    void releasing(Runnable step) {
        releasing = step;
    }

    /** The earliest delayed time not yet read, for tests. */
    long nextDelayed() {
        return nextDelayed.get();
    }

    /** The failed reads' retry delay, for tests. */
    Duration retry() {
        return retry;
    }
}
