package io.github.hectorvent.floci.core.common.microsandbox;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MicrosandboxCapacityTest {
    @Test
    void hundredsOfDormantIdentitiesDoNotConsumeCapacity() {
        MicrosandboxCapacity capacity = new MicrosandboxCapacity(1024);
        for (int i = 0; i < 500; i++) {
            String id = "guest-" + i;
            capacity.reserve(id, 512, Duration.ZERO);
            capacity.release(id);
        }
        assertEquals(0, capacity.reservedMemoryMib());
    }

    @Test
    void blockedWakeProceedsOnlyAfterConfirmedStopReleasesMemory() throws Exception {
        MicrosandboxCapacity capacity = new MicrosandboxCapacity(1024);
        capacity.reserve("ec2", 768, Duration.ZERO);
        CountDownLatch entered = new CountDownLatch(1);
        try (ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<?> next = workers.submit(() -> {
                entered.countDown();
                capacity.reserve("pod", 512, Duration.ofSeconds(5));
            });
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            assertFalse(next.isDone());
            capacity.release("ec2");
            next.get(1, TimeUnit.SECONDS);
            assertEquals(512, capacity.reservedMemoryMib());
        }
    }

    @Test
    void existingGuestsAreCountedAboveALoweredBudgetWithoutBeingKilled() {
        MicrosandboxCapacity capacity = new MicrosandboxCapacity(512);
        capacity.observeRunning("old", 1024);
        capacity.observeRunning("old", 1024);
        assertEquals(1024, capacity.reservedMemoryMib());
        assertThrows(MicrosandboxCapacity.CapacityUnavailableException.class,
                () -> capacity.reserve("new", 128, Duration.ZERO));
        capacity.release("old");
        capacity.reserve("new", 128, Duration.ZERO);
        assertEquals(128, capacity.reservedMemoryMib());
    }

    @Test
    void duplicateActivationCannotDoubleCountOrChangeAnExistingReservation() {
        MicrosandboxCapacity capacity = new MicrosandboxCapacity(512);
        capacity.reserve("guest", 512, Duration.ZERO);
        capacity.reserve("guest", 512, Duration.ZERO);
        assertEquals(512, capacity.reservedMemoryMib());
        assertThrows(IllegalStateException.class, () -> capacity.observeRunning("guest", 256));
        capacity.release("guest");
        capacity.release("guest");
        assertEquals(0, capacity.reservedMemoryMib());
    }

    @Test
    void oversizedGuestFailsImmediatelyAndZeroDisablesAdmissionLimit() {
        MicrosandboxCapacity limited = new MicrosandboxCapacity(512);
        assertThrows(MicrosandboxCapacity.CapacityUnavailableException.class,
                () -> limited.reserve("large", 1024, Duration.ofMinutes(1)));
        MicrosandboxCapacity unlimited = new MicrosandboxCapacity(0);
        unlimited.reserve("large", 65536, Duration.ZERO);
        assertEquals(65536, unlimited.reservedMemoryMib());
    }
}
