package io.github.hectorvent.floci.services.sqs;

import java.time.Instant;

/**
 * Hears every change to the messages of SQS queues, on the thread that made it, after the queue's
 * lock is released. A FIFO send still holds its deduplication monitor while it reports, so an
 * implementation returns at once: no I/O, no locks.
 */
public interface SqsQueueListener {

    /** A message was added to {@code queue}; it can be received from {@code at}, which may be past. */
    void messageAvailable(SqsQueueId queue, Instant at);

    /**
     * Messages of {@code queue} were received, deleted, moved out, expired or purged, or had their
     * visibility changed, or the queue was deleted.
     */
    void messagesChanged(SqsQueueId queue);
}
