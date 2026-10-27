package io.github.hectorvent.floci.services.sqs;

import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageBackend;
import io.github.hectorvent.floci.services.sqs.model.Message;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Thread-safe wrapper around a per-queue message list. All operations acquire a
 * {@link ReentrantLock} so that compound read-modify-write sequences (e.g., claim
 * visible messages) are atomic with respect to each other. Each change that succeeds is
 * reported to the queue's {@link SqsQueueListener} after the lock is released.
 */
class GuardedMessageQueue {

    static final SqsQueueListener NO_LISTENER = new SqsQueueListener() {
        @Override
        public void messageAvailable(SqsQueueId queue, Instant at) {
        }

        @Override
        public void messagesChanged(SqsQueueId queue) {
        }
    };

    private final ReentrantLock lock = new ReentrantLock();
    private final List<Message> messages;
    private final StorageBackend<String, List<Message>> messageStore;
    private final String storageKey;
    private final String receiptHandleSecret;
    private final SqsQueueActivity activity = new SqsQueueActivity();
    private final SqsQueueId id;
    private final SqsQueueListener listener;
    private volatile boolean closed;

    @FunctionalInterface
    private interface Guard extends AutoCloseable {
        @Override
        void close(); // no checked exception
    }

    private Guard hold() {
        lock.lock();
        return lock::unlock;
    }

    GuardedMessageQueue(StorageBackend<String, List<Message>> messageStore, String storageKey) {
        this(new ArrayList<>(), messageStore, storageKey, ReceiptHandle.DEFAULT_SECRET);
    }

    GuardedMessageQueue(List<Message> initial, StorageBackend<String, List<Message>> messageStore, String storageKey) {
        this(initial, messageStore, storageKey, ReceiptHandle.DEFAULT_SECRET);
    }

    GuardedMessageQueue(List<Message> initial, StorageBackend<String, List<Message>> messageStore, String storageKey,
                        String receiptHandleSecret) {
        this(initial, messageStore, storageKey, receiptHandleSecret, NO_LISTENER);
    }

    /** @param listener hears each change when the storage key names a queue */
    GuardedMessageQueue(List<Message> initial, StorageBackend<String, List<Message>> messageStore, String storageKey,
                        String receiptHandleSecret, SqsQueueListener listener) {
        this.messages = new ArrayList<>(initial);
        this.messageStore = messageStore;
        this.storageKey = storageKey;
        this.receiptHandleSecret = receiptHandleSecret;
        this.id = SqsQueueId.ofStorageKey(storageKey);
        this.listener = listener;
    }

    record ClaimResult(List<Message> claimed, List<Message> dlqCandidates) {
    }

    void addMessage(Message message) {
        try (Guard _ = hold()) {
            int mark = messages.size();
            messages.add(message);
            try {
                persist();
            } catch (RuntimeException e) {
                messages.remove(mark);
                throw e;
            }
        }
        available(List.of(message));
    }

    /** If persisting fails the in-memory add is rolled back and the exception propagates, so a
     *  caller that compensates on the source side does not leave a duplicate here. */
    void addAll(List<Message> toAdd) {
        try (Guard _ = hold()) {
            int mark = messages.size();
            messages.addAll(toAdd);
            try {
                persist();
            } catch (RuntimeException e) {
                messages.subList(mark, messages.size()).clear();
                throw e;
            }
        }
        available(toAdd);
    }

    ClaimResult claimVisibleMessages(int maxMessages, int effectiveTimeout,
                                     boolean fifo, int maxReceiveCount,
                                     String deadLetterTargetArn) {
        ClaimResult result;
        try (Guard _ = hold()) {
            List<Message> claimed = new ArrayList<>();
            List<Message> dlqCandidates = new ArrayList<>();

            if (fifo) {
                claimFifo(maxMessages, effectiveTimeout, maxReceiveCount, deadLetterTargetArn,
                        claimed, dlqCandidates);
            } else {
                claimStandard(maxMessages, effectiveTimeout, maxReceiveCount, deadLetterTargetArn,
                        claimed, dlqCandidates);
            }

            if (!claimed.isEmpty() || !dlqCandidates.isEmpty()) {
                persist();
            }

            result = new ClaimResult(claimed, dlqCandidates);
        }
        if (!result.claimed().isEmpty() || !result.dlqCandidates().isEmpty()) {
            changed();
        }
        return result;
    }

    private boolean tryClaim(Message msg, int effectiveTimeout, int maxReceiveCount,
                             String deadLetterTargetArn, List<Message> claimed,
                             List<Message> dlqCandidates) {
        msg.setReceiveCount(msg.getReceiveCount() + 1);
        if (msg.getFirstReceiveTimestamp() == null) {
            msg.setFirstReceiveTimestamp(Instant.now());
        }

        if (maxReceiveCount > 0 && deadLetterTargetArn != null
                && msg.getReceiveCount() > maxReceiveCount) {
            dlqCandidates.add(msg);
            return false;
        }

        msg.setReceiptHandle(ReceiptHandle.issue(storageKey, msg.getMessageId(), receiptHandleSecret).encode());
        msg.setVisibleAt(Instant.now().plusSeconds(effectiveTimeout));
        claimed.add(msg);
        return true;
    }

    private void claimStandard(int maxMessages, int effectiveTimeout,
                               int maxReceiveCount, String deadLetterTargetArn,
                               List<Message> claimed, List<Message> dlqCandidates) {
        for (Message msg : messages) {
            if (claimed.size() >= maxMessages) break;
            if (!msg.isVisible()) continue;
            tryClaim(msg, effectiveTimeout, maxReceiveCount, deadLetterTargetArn, claimed, dlqCandidates);
        }
    }

    private void claimFifo(int maxMessages, int effectiveTimeout,
                           int maxReceiveCount, String deadLetterTargetArn,
                           List<Message> claimed, List<Message> dlqCandidates) {
        // Cross-call group locking: a group that already has an in-flight
        // message from a previous ReceiveMessage call is blocked until that
        // message is deleted or its visibility expires. Within a single call
        // we may return multiple messages from the same group (preserving
        // insertion order), up to MaxNumberOfMessages. A not-visible message
        // that was never claimed — no receipt handle — is only waiting out its
        // DelaySeconds and must not lock its group.
        Set<String> groupsWithInFlight =
                messages.stream()
                        .filter(msg -> !msg.isVisible() && msg.getReceiptHandle() != null
                                && msg.getMessageGroupId() != null)
                        .map(Message::getMessageGroupId).collect(Collectors.toSet());

        for (Message msg : messages) {
            if (claimed.size() >= maxMessages) break;
            if (!msg.isVisible()) continue;

            String groupId = msg.getMessageGroupId();
            if (groupId != null && groupsWithInFlight.contains(groupId)) continue;

            tryClaim(msg, effectiveTimeout, maxReceiveCount, deadLetterTargetArn, claimed, dlqCandidates);
        }
    }

    enum HandleResult { APPLIED, MESSAGE_GONE, HANDLE_EXPIRED }

    record Removal(HandleResult result, Message message) {
    }

    Removal removeByReceiptHandle(ReceiptHandle handle, boolean fifo) {
        Message removed;
        try (Guard _ = hold()) {
            Message msg = findByMessageId(handle.messageId());
            HandleResult result = checkHandle(msg, handle, fifo);
            if (result != HandleResult.APPLIED) {
                return new Removal(result, null);
            }
            messages.remove(msg);
            persist();
            removed = msg;
        }
        changed();
        return new Removal(HandleResult.APPLIED, removed);
    }

    HandleResult changeVisibility(ReceiptHandle handle, int visibilityTimeout, boolean fifo) {
        HandleResult result;
        try (Guard _ = hold()) {
            Message msg = findByMessageId(handle.messageId());
            result = checkHandle(msg, handle, fifo);
            if (result == HandleResult.APPLIED) {
                msg.setVisibleAt(Instant.now().plusSeconds(visibilityTimeout));
                persist();
            }
        }
        if (result == HandleResult.APPLIED) {
            changed();
        }
        return result;
    }

    private Message findByMessageId(String messageId) {
        for (Message msg : messages) {
            if (messageId.equals(msg.getMessageId())) {
                return msg;
            }
        }
        return null;
    }

    /**
     * On a standard queue an older handle of the message still works. On a FIFO queue only the
     * handle of the receive still in flight does.
     */
    private static HandleResult checkHandle(Message msg, ReceiptHandle handle, boolean fifo) {
        if (msg == null) {
            return HandleResult.MESSAGE_GONE;
        }
        if (fifo && (!handle.encode().equals(msg.getReceiptHandle()) || msg.isVisible())) {
            return HandleResult.HANDLE_EXPIRED;
        }
        return HandleResult.APPLIED;
    }

    void removeMessages(List<Message> toRemove) {
        try (Guard _ = hold()) {
            messages.removeAll(toRemove);
            persist();
        }
        changed();
    }

    void removeExpired(Instant cutoff) {
        boolean removed;
        try (Guard _ = hold()) {
            removed = messages.removeIf(msg -> msg.isExpired(cutoff));
            if (removed) {
                persist();
            }
        }
        if (removed) {
            changed();
        }
    }

    void purge() {
        try (Guard _ = hold()) {
            messages.clear();
            persist();
        }
        changed();
    }

    List<Message> drainAll() {
        List<Message> drained;
        try (Guard _ = hold()) {
            drained = new ArrayList<>(messages);
            messages.clear();
            persist();
        }
        changed();
        return drained;
    }

    /** Remove and return the head message if {@code eligible} accepts it; otherwise leave the
     *  queue untouched and return {@code null}. The check and the removal happen under one lock
     *  hold so nothing can slip in between. Used by the message-move-task worker so the source
     *  queue stays observably populated for the duration of a rate-limited move. */
    Message drainFirstIf(Predicate<Message> eligible) {
        Message head;
        try (Guard _ = hold()) {
            if (messages.isEmpty() || !eligible.test(messages.getFirst())) {
                return null;
            }
            head = messages.removeFirst();
            persist();
        }
        changed();
        return head;
    }

    /** Put a message that could not be delivered back at the head, preserving queue order. */
    void restoreFirst(Message message) {
        try (Guard _ = hold()) {
            messages.addFirst(message);
            persist();
        }
        available(List.of(message));
    }

    record MessageCounts(long visible, long inFlight, long delayed) {
    }

    /**
     * Splits the queue contents the way AWS SQS reports them in
     * GetQueueAttributes: visible (ApproximateNumberOfMessages), in flight
     * (ApproximateNumberOfMessagesNotVisible) and delayed
     * (ApproximateNumberOfMessagesDelayed). A message that is not visible and
     * was never claimed — no receipt handle — can only be waiting out its
     * DelaySeconds, so it counts as delayed rather than in flight.
     */
    MessageCounts messageCounts() {
        try (Guard _ = hold()) {
            long visible = 0;
            long inFlight = 0;
            long delayed = 0;
            for (Message m : messages) {
                if (m.isVisible()) {
                    visible++;
                } else if (m.getReceiptHandle() == null) {
                    delayed++;
                } else {
                    inFlight++;
                }
            }
            return new MessageCounts(visible, inFlight, delayed);
        }
    }

    record Gauges(MessageCounts counts, long ageOfOldestMessageSeconds, long groupsWithInflightMessages) {
        boolean isEmpty() {
            return counts.visible() + counts.inFlight() + counts.delayed() == 0;
        }
    }

    /**
     * The queue contents AWS/SQS reports each minute. The age is that of the oldest message by the
     * time its retention started, so a message moved to a FIFO dead-letter queue or by a move task
     * counts from the move.
     */
    Gauges gauges(Instant now) {
        try (Guard _ = hold()) {
            Instant oldest = null;
            Set<String> groupsInFlight = new HashSet<>();
            for (Message m : messages) {
                if (!m.isVisible() && m.getReceiptHandle() != null && m.getMessageGroupId() != null) {
                    groupsInFlight.add(m.getMessageGroupId());
                }
                // ponytail: AWS also leaves out a standard-queue message received 3+ times in this queue;
                // that needs a per-queue receive count, since a redriven message keeps its total.
                Instant enqueued = m.getRetentionStartTimestamp() != null
                        ? m.getRetentionStartTimestamp() : m.getSentTimestamp();
                if (enqueued != null && (oldest == null || enqueued.isBefore(oldest))) {
                    oldest = enqueued;
                }
            }
            long age = oldest == null ? 0 : Math.max(0, Duration.between(oldest, now).toSeconds());
            return new Gauges(messageCounts(), age, groupsInFlight.size());
        }
    }

    SqsQueueActivity activity() {
        return activity;
    }

    /**
     * The messages a consumer has to handle, in one pass that changes nothing: those visible and
     * those in flight, and the earliest time a delayed one becomes visible. Messages sent before
     * {@code cutoff} have expired and are skipped.
     */
    SqsService.QueueWork work(Instant cutoff) {
        try (Guard _ = hold()) {
            long visible = 0;
            long inFlight = 0;
            Instant nextVisible = null;
            for (Message m : messages) {
                if (m.isExpired(cutoff)) {
                    continue;
                }
                if (m.isVisible()) {
                    visible++;
                } else if (m.getReceiptHandle() != null) {
                    inFlight++;
                } else if (nextVisible == null || m.getVisibleAt().isBefore(nextVisible)) {
                    nextVisible = m.getVisibleAt();
                }
            }
            return new SqsService.QueueWork(visible, inFlight, nextVisible);
        }
    }

    List<Message> peekAll() {
        try (Guard _ = hold()) {
            return new ArrayList<>(messages);
        }
    }

    boolean isEmpty() {
        try (Guard _ = hold()) {
            return messages.isEmpty();
        }
    }

    Message findByDeduplicationId(String dedupId) {
        return findByDeduplicationId(dedupId, null);
    }

    Message findByDeduplicationId(String dedupId, String messageGroupId) {
        try (Guard _ = hold()) {
            return messages.stream()
                    .filter(msg -> dedupId.equals(msg.getMessageDeduplicationId()))
                    .filter(msg -> messageGroupId == null
                            || messageGroupId.equals(msg.getMessageGroupId()))
                    .findFirst().orElse(null);
        }
    }

    void close() {
        closed = true;
    }

    /** Whether the calling thread holds the queue's lock, for tests. */
    boolean heldByCurrentThread() {
        return lock.isHeldByCurrentThread();
    }

    /** Reports that messages of this queue changed. */
    void changed() {
        if (id != null) {
            listener.messagesChanged(id);
        }
    }

    /** Reports added messages, once, with the earliest time one of them can be received. */
    private void available(List<Message> added) {
        if (id == null || added.isEmpty()) {
            return;
        }
        Instant at = null;
        for (Message message : added) {
            Instant from = message.getVisibleAt() != null ? message.getVisibleAt() : message.getSentTimestamp();
            if (from == null) {
                from = Instant.now();
            }
            if (at == null || from.isBefore(at)) {
                at = from;
            }
        }
        listener.messageAvailable(id, at);
    }

    private void persist() {
        if (closed || messageStore == null || storageKey == null) {
            return;
        }
        if (messageStore instanceof AccountAwareStorageBackend<List<Message>> aware) {
            String accountId = extractAccountFromStorageKey(storageKey);
            if (accountId != null) {
                aware.putForAccount(accountId, storageKey, new ArrayList<>(messages));
                return;
            }
        }
        messageStore.put(storageKey, new ArrayList<>(messages));
    }

    /**
     * Extracts the 12-digit account ID from a storage key of the form
     * {@code region::/accountId/queueName}.
     */
    private static String extractAccountFromStorageKey(String storageKey) {
        if (storageKey == null) {
            return null;
        }
        // storageKey format: "us-east-1::/000000000001/my-queue"
        int separator = storageKey.indexOf("::");
        if (separator < 0) {
            return null;
        }
        String path = storageKey.substring(separator + 2); // "/000000000001/my-queue"
        String trimmed = path.startsWith("/") ? path.substring(1) : path;
        int slash = trimmed.indexOf('/');
        String candidate = slash > 0 ? trimmed.substring(0, slash) : trimmed;
        return candidate.matches("\\d{12}") ? candidate : null;
    }
}
