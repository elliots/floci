package io.github.hectorvent.floci.runtime.network;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class BackendAddressCacheServiceTest {
    private final AtomicLong time = new AtomicLong();
    private final BackendAddressCache cache = new BackendAddressCache(Duration.ofSeconds(1), 2, time::get);

    @Test
    void repeatedRequestsReuseAnAddressUntilExpiryThenDiscoverItsReplacement() {
        AtomicInteger lookups = new AtomicInteger();
        Supplier<String> lookup = () -> "172.22.0." + lookups.incrementAndGet();
        assertEquals("172.22.0.1", cache.resolve("simulator", lookup));
        time.set(TimeUnit.MILLISECONDS.toNanos(999));
        assertEquals("172.22.0.1", cache.resolve("simulator", lookup));
        assertEquals(1, lookups.get());
        time.set(TimeUnit.SECONDS.toNanos(1));
        assertEquals("172.22.0.2", cache.resolve("simulator", lookup));
        assertEquals(2, lookups.get());
    }

    @Test
    void expiredAddressesAreNotServedWhenMembershipValidationFailsAndFailuresAreNotCached() {
        cache.resolve("simulator", () -> "172.22.0.2");
        time.set(TimeUnit.SECONDS.toNanos(1));
        assertThrows(IllegalArgumentException.class, () -> cache.resolve("simulator", () -> {
            throw new IllegalArgumentException("Backend is no longer on a protected network");
        }));
        assertEquals("172.22.0.3", cache.resolve("simulator", () -> "172.22.0.3"));
    }

    @Test
    void capacityEvictsTheLeastRecentlyUsedHostname() {
        cache.resolve("first", () -> "172.22.0.1");
        cache.resolve("second", () -> "172.22.0.2");
        assertEquals("172.22.0.1", cache.resolve("first", () -> fail("Unexpected lookup")));
        cache.resolve("third", () -> "172.22.0.3");
        assertEquals("172.22.0.1", cache.resolve("first", () -> fail("Unexpected eviction")));
        assertEquals("172.22.0.4", cache.resolve("second", () -> "172.22.0.4"));
    }

    @Test
    void concurrentMissesShareDiscoveryAndDoNotBlockCachedHosts() throws Exception {
        cache.resolve("warm", () -> "172.22.0.1");
        CountDownLatch discovering = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger lookups = new AtomicInteger();
        Supplier<String> lookup = () -> {
            lookups.incrementAndGet();
            discovering.countDown();
            try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            return "172.22.0.2";
        };
        try (ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<String> first = workers.submit(() -> cache.resolve("cold", lookup));
            try {
                assertTrue(discovering.await(5, TimeUnit.SECONDS));
                Future<String> second = workers.submit(() -> cache.resolve("cold", lookup));
                Future<String> warm = workers.submit(() -> cache.resolve("warm", () -> fail("Unexpected lookup")));
                assertEquals("172.22.0.1", warm.get(2, TimeUnit.SECONDS));
                release.countDown();
                assertEquals("172.22.0.2", first.get(5, TimeUnit.SECONDS));
                assertEquals("172.22.0.2", second.get(5, TimeUnit.SECONDS));
                assertEquals(1, lookups.get());
            } finally {
                release.countDown();
            }
        }
    }
}
