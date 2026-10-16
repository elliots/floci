package io.github.hectorvent.floci.core.common.microsandbox;

import io.github.hectorvent.floci.config.EmulatorConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Admission accounting for guest RAM across providers in one Floci process. */
@ApplicationScoped
public class MicrosandboxCapacity {
    private final long limit;
    private final Map<String, Integer> reservations = new HashMap<>();
    private long reserved;

    @Inject
    public MicrosandboxCapacity(EmulatorConfig config) {
        this(config.microsandbox().maxActiveMemoryMib());
    }

    public MicrosandboxCapacity(int limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("Microsandbox memory budget cannot be negative");
        }
        this.limit = limit == 0 ? Long.MAX_VALUE : limit;
    }

    /** Reserve before a create/restore call; retain the reservation when its outcome is uncertain. */
    public synchronized void reserve(String id, int memoryMib, Duration timeout) {
        validate(id, memoryMib);
        if (reservations.containsKey(id)) {
            record(id, memoryMib);
            return;
        }
        if (memoryMib > limit) {
            throw new CapacityUnavailableException("Guest RAM exceeds FLOCI_MICROSANDBOX_MAX_ACTIVE_MEMORY_MIB");
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!reservations.containsKey(id) && reserved + memoryMib > limit) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw new CapacityUnavailableException("Timed out waiting for microsandbox guest RAM capacity");
            }
            try {
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CapacityUnavailableException("Interrupted waiting for microsandbox guest RAM capacity");
            }
        }
        record(id, memoryMib);
    }

    /** Count an existing live guest even if a newly lowered budget is already exceeded. */
    public synchronized void observeRunning(String id, int memoryMib) {
        validate(id, memoryMib);
        record(id, memoryMib);
    }

    /** Release only once the hypervisor has confirmed the guest no longer consumes RAM. */
    public synchronized void release(String id) {
        Integer memory = reservations.remove(id);
        if (memory != null) {
            reserved -= memory;
            notifyAll();
        }
    }

    public synchronized long reservedMemoryMib() {
        return reserved;
    }

    private void record(String id, int memoryMib) {
        Integer existing = reservations.putIfAbsent(id, memoryMib);
        if (existing == null) {
            reserved += memoryMib;
        } else if (existing != memoryMib) {
            throw new IllegalStateException("Microsandbox reservation changed while its guest may still be running");
        }
    }

    private static void validate(String id, int memoryMib) {
        if (id == null || id.isBlank() || memoryMib < 1) {
            throw new IllegalArgumentException("Microsandbox capacity needs a guest identity and positive memory size");
        }
    }

    public static class CapacityUnavailableException extends IllegalStateException {
        public CapacityUnavailableException(String message) {
            super(message);
        }
    }
}
