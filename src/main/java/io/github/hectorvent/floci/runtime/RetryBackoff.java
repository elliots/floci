package io.github.hectorvent.floci.runtime;

import java.time.Duration;

/**
 * The delays between the retries of the runtime's calls and follows: {@link #FIRST}, doubling up
 * to {@link #MAX}.
 */
public final class RetryBackoff {

    public static final Duration FIRST = Duration.ofSeconds(1);
    public static final Duration MAX = Duration.ofSeconds(30);

    private RetryBackoff() {
    }

    /** The delay that follows {@code previous}, or {@link #FIRST} when there was none. */
    public static Duration next(Duration previous) {
        if (previous == null) {
            return FIRST;
        }
        Duration doubled = previous.multipliedBy(2);
        return doubled.compareTo(MAX) > 0 ? MAX : doubled;
    }
}
