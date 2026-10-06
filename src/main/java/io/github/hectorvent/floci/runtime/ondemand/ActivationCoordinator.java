package io.github.hectorvent.floci.runtime.ondemand;

import org.jboss.logging.Logger;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/** Serializes each workload's transitions, without holding its monitor during runtime I/O. */
public final class ActivationCoordinator implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(ActivationCoordinator.class);

    public enum State { UNKNOWN, SLEEPING, STARTING, READY, STOPPING, FAILED, DISABLED }

    @FunctionalInterface
    public interface Readiness {
        void await(WorkloadDefinition definition, URI backend, Duration timeout) throws Exception;
    }

    private static final class Entry {
        private final WorkloadDefinition definition;
        private final WorkloadRuntime runtime;
        private State state;
        private URI backend;
        private CompletableFuture<URI> transition;
        private int active;
        private int pending;
        private boolean queueActive;
        private boolean queuesKnown;
        private boolean stopFromFailure;
        private boolean failedCleanup;
        private long idleSince;

        private Entry(WorkloadDefinition definition, WorkloadRuntime runtime, long now) {
            this.definition = definition;
            this.runtime = runtime;
            state = definition.enabled() ? State.UNKNOWN : State.DISABLED;
            queuesKnown = definition.queues().isEmpty();
            idleSince = now;
        }
    }

    private final Map<String, Entry> entries = new ConcurrentHashMap<>();
    private final ExecutorService workers;
    private final Readiness readiness;
    private final LongSupplier nanoTime;
    private final Semaphore starts;
    private final AtomicInteger pending = new AtomicInteger();
    private final int maxPending;
    private final int maxPendingPerWorkload;
    private volatile boolean closed;

    public ActivationCoordinator(ExecutorService workers, Readiness readiness, int maxStarts,
                                 int maxPending, int maxPendingPerWorkload, LongSupplier nanoTime) {
        this.workers = workers;
        this.readiness = readiness;
        this.starts = new Semaphore(maxStarts, true);
        this.maxPending = maxPending;
        this.maxPendingPerWorkload = maxPendingPerWorkload;
        this.nanoTime = nanoTime;
    }

    public void register(WorkloadDefinition definition, WorkloadRuntime runtime) {
        if (entries.putIfAbsent(definition.id(), new Entry(definition, runtime, nanoTime.getAsLong())) != null) {
            throw new IllegalArgumentException("Duplicate on-demand workload: " + definition.id());
        }
    }

    public State state(String id) {
        Entry entry = entry(id);
        synchronized (entry) {
            return entry.state;
        }
    }

    public URI backend(String id) {
        Entry entry = entry(id);
        synchronized (entry) {
            return entry.backend;
        }
    }

    public boolean canReportSleepingHealth(String id) {
        Entry entry = entry(id);
        synchronized (entry) {
            return entry.state == State.SLEEPING || (entry.state == State.STOPPING && !entry.stopFromFailure);
        }
    }

    public RequestLease acquire(String id) {
        Entry entry = entry(id);
        synchronized (entry) {
            if (closed || entry.state == State.DISABLED) {
                throw new IllegalStateException("On-demand workload is disabled: " + id);
            }
            if (entry.pending >= maxPendingPerWorkload) {
                throw new CapacityException();
            }
            if (pending.incrementAndGet() > maxPending) {
                pending.decrementAndGet();
                throw new CapacityException();
            }
            entry.active++;
            entry.pending++;
        }
        RequestLease lease = new RequestLease(entry, true);
        ensureStarted(entry).whenComplete((backend, failure) -> {
            lease.releasePending();
            if (failure != null) {
                lease.ready.completeExceptionally(failure);
                lease.close();
            } else {
                lease.ready.complete(backend);
            }
        });
        return lease;
    }

    /** A running probe holds the backend only for the probe, without refreshing its idle clock. */
    public Optional<RequestLease> acquireHealth(String id) {
        Entry entry = entry(id);
        synchronized (entry) {
            if (closed || entry.state != State.READY) {
                return Optional.empty();
            }
            entry.active++;
            RequestLease lease = new RequestLease(entry, false);
            lease.pendingReleased.set(true);
            lease.ready.complete(entry.backend);
            return Optional.of(lease);
        }
    }

    /** Initial adoption never starts a sleeping runtime just to inspect it. */
    public void reconcile(String id, boolean queueActive, boolean queuesKnown) {
        Entry entry = entry(id);
        synchronized (entry) {
            if (closed || entry.state == State.DISABLED) {
                return;
            }
            boolean wasActive = entry.queueActive;
            boolean wasKnown = entry.queuesKnown;
            entry.queueActive = queueActive;
            entry.queuesKnown = queuesKnown;
            if ((wasActive || !wasKnown) && !queueActive && queuesKnown && entry.active == 0) {
                entry.idleSince = nanoTime.getAsLong();
            }
        }
        if (state(id) == State.UNKNOWN) {
            try {
                boolean running = entry.runtime.isRunning();
                synchronized (entry) {
                    if (entry.state == State.UNKNOWN && !running) {
                        entry.state = State.SLEEPING;
                    }
                }
                if (running) {
                    ensureStarted(entry);
                }
            } catch (RuntimeException e) {
                LOG.debugv("Cannot inspect on-demand workload {0}: {1}", id, e.getMessage());
            }
        }
        if (queueActive) {
            ensureStarted(entry);
        }
        tick(id);
    }

    public void tick(String id) {
        Entry entry = entry(id);
        CompletableFuture<URI> stopping;
        if (state(id) != State.READY && state(id) != State.FAILED) {
            return;
        }
        try {
            if (entry.runtime.hasActivity()) {
                synchronized (entry) {
                    entry.idleSince = nanoTime.getAsLong();
                }
                return;
            }
        } catch (RuntimeException e) {
            LOG.debugv(e, "Cannot inspect activity for on-demand workload {0}", id);
            return;
        }
        synchronized (entry) {
            if (closed || (entry.state != State.READY && entry.state != State.FAILED)
                    || (entry.state == State.FAILED && entry.failedCleanup)
                    || entry.active != 0 || entry.queueActive || !entry.queuesKnown
                    || nanoTime.getAsLong() - entry.idleSince < entry.definition.idleTimeout().toNanos()) {
                return;
            }
            entry.stopFromFailure = entry.state == State.FAILED;
            entry.state = State.STOPPING;
            stopping = new CompletableFuture<>();
            entry.transition = stopping;
        }
        workers.execute(() -> {
            try {
                entry.runtime.stop(entry.definition.startupTimeout());
                synchronized (entry) {
                    entry.state = entry.stopFromFailure ? State.FAILED : State.SLEEPING;
                    entry.failedCleanup = entry.stopFromFailure;
                    entry.backend = null;
                }
                stopping.complete(null);
                LOG.debugv("Stopped idle on-demand workload {0}", id);
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                synchronized (entry) {
                    entry.state = State.FAILED;
                    entry.idleSince = nanoTime.getAsLong();
                }
                stopping.completeExceptionally(e);
                LOG.warnv(e, "Could not stop on-demand workload {0}", id);
            }
        });
    }

    private CompletableFuture<URI> ensureStarted(Entry entry) {
        CompletableFuture<URI> starting;
        synchronized (entry) {
            if (closed) {
                return CompletableFuture.failedFuture(new IllegalStateException("On-demand runtime is shutting down"));
            }
            if (entry.state == State.READY) {
                return CompletableFuture.completedFuture(entry.backend);
            }
            if (entry.state == State.STARTING) {
                return entry.transition;
            }
            if (entry.state == State.STOPPING) {
                return entry.transition.thenCompose(ignored -> ensureStarted(entry));
            }
            if (entry.state == State.DISABLED) {
                return CompletableFuture.failedFuture(new IllegalStateException("Workload is disabled"));
            }
            entry.state = State.STARTING;
            entry.failedCleanup = false;
            starting = new CompletableFuture<>();
            entry.transition = starting;
        }
        long deadline = nanoTime.getAsLong() + entry.definition.startupTimeout().toNanos();
        try {
            workers.execute(() -> start(entry, starting, deadline));
        } catch (RuntimeException e) {
            synchronized (entry) {
                entry.state = State.FAILED;
                entry.idleSince = nanoTime.getAsLong();
            }
            starting.completeExceptionally(e);
        }
        return starting;
    }

    private void start(Entry entry, CompletableFuture<URI> starting, long deadline) {
        boolean permit = false;
        try {
            permit = starts.tryAcquire(remaining(deadline).toNanos(), TimeUnit.NANOSECONDS);
            if (!permit || closed) {
                throw new IllegalStateException("On-demand startup capacity timeout");
            }
            entry.runtime.start(remaining(deadline));
            URI backend = entry.runtime.backend();
            readiness.await(entry.definition, backend, remaining(deadline));
            synchronized (entry) {
                if (closed) {
                    throw new IllegalStateException("On-demand runtime is shutting down");
                }
                entry.backend = backend;
                entry.state = State.READY;
                entry.idleSince = nanoTime.getAsLong();
            }
            starting.complete(backend);
            LOG.debugv("Activated on-demand workload {0}", entry.definition.id());
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            synchronized (entry) {
                entry.state = State.FAILED;
                entry.idleSince = nanoTime.getAsLong();
            }
            starting.completeExceptionally(e);
            LOG.warnv(e, "Could not activate on-demand workload {0}", entry.definition.id());
        } finally {
            if (permit) {
                starts.release();
            }
        }
    }

    private Duration remaining(long deadline) {
        long nanos = deadline - nanoTime.getAsLong();
        if (nanos <= 0) {
            throw new IllegalStateException("On-demand readiness deadline exceeded");
        }
        return Duration.ofNanos(nanos);
    }

    private Entry entry(String id) {
        Entry entry = entries.get(id);
        if (entry == null) {
            throw new IllegalArgumentException("Unknown on-demand workload: " + id);
        }
        return entry;
    }

    @Override
    public void close() {
        closed = true;
        for (Entry entry : entries.values()) {
            CompletableFuture<URI> transition;
            synchronized (entry) {
                transition = entry.transition;
            }
            if (transition != null) {
                transition.completeExceptionally(new IllegalStateException("On-demand runtime is shutting down"));
            }
        }
        workers.shutdownNow();
        if (readiness instanceof AutoCloseable resource) {
            try {
                resource.close();
            } catch (Exception e) {
                LOG.warnv(e, "Could not close on-demand readiness client");
            }
        }
        try {
            if (!workers.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("On-demand operations did not stop before resource reset");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted stopping on-demand operations", e);
        }
    }

    public final class RequestLease implements AutoCloseable {
        private final Entry entry;
        private final CompletableFuture<URI> ready = new CompletableFuture<>();
        private final boolean application;
        private final AtomicBoolean released = new AtomicBoolean();
        private final AtomicBoolean pendingReleased = new AtomicBoolean();

        private RequestLease(Entry entry, boolean application) {
            this.entry = entry;
            this.application = application;
        }

        public CompletableFuture<URI> ready() {
            return ready;
        }

        private void releasePending() {
            if (pendingReleased.compareAndSet(false, true)) {
                synchronized (entry) {
                    entry.pending--;
                }
                pending.decrementAndGet();
            }
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                releasePending();
                synchronized (entry) {
                    entry.active--;
                    if (application && !entry.queueActive) {
                        entry.idleSince = nanoTime.getAsLong();
                    }
                }
            }
        }
    }

    public static final class CapacityException extends IllegalStateException {
        private CapacityException() {
            super("On-demand pending request capacity exceeded");
        }
    }
}
