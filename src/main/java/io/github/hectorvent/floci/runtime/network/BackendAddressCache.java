package io.github.hectorvent.floci.runtime.network;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Briefly retains successful local lookups without serving expired addresses when discovery fails. */
final class BackendAddressCache {
    private record Address(String value, long expiresAt) {}

    private final LinkedHashMap<String, Address> addresses = new LinkedHashMap<>(16, 0.75f, true);
    private final Object refreshLock = new Object();
    private final long ttlNanos;
    private final int capacity;
    private final LongSupplier nanoTime;

    BackendAddressCache(Duration ttl, int capacity, LongSupplier nanoTime) {
        this.ttlNanos = ttl.toNanos();
        this.capacity = capacity;
        this.nanoTime = nanoTime;
    }

    String resolve(String host, Supplier<String> lookup) {
        String cached = cached(host);
        if (cached != null) {
            return cached;
        }
        synchronized (refreshLock) {
            cached = cached(host);
            if (cached != null) {
                return cached;
            }
            String resolved = lookup.get();
            synchronized (addresses) {
                addresses.put(host, new Address(resolved, nanoTime.getAsLong() + ttlNanos));
                if (addresses.size() > capacity) {
                    addresses.pollFirstEntry();
                }
            }
            return resolved;
        }
    }

    private String cached(String host) {
        synchronized (addresses) {
            Address address = addresses.get(host);
            if (address == null) {
                return null;
            }
            if (nanoTime.getAsLong() - address.expiresAt() >= 0) {
                addresses.remove(host);
                return null;
            }
            return address.value();
        }
    }
}
