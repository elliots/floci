package io.github.hectorvent.floci.runtime;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RetryBackoffTest {

    @Test
    void startsAtOneSecondAndDoublesUpToThirty() {
        Duration delay = RetryBackoff.next(null);
        assertEquals(Duration.ofSeconds(1), delay);
        long[] expected = {2, 4, 8, 16, 30, 30};
        for (long seconds : expected) {
            delay = RetryBackoff.next(delay);
            assertEquals(Duration.ofSeconds(seconds), delay);
        }
    }
}
