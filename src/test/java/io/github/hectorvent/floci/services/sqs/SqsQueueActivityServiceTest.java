package io.github.hectorvent.floci.services.sqs;

import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.storage.InMemoryStorage;
import io.github.hectorvent.floci.services.sqs.model.Message;
import io.github.hectorvent.floci.services.sqs.model.Queue;
import io.github.hectorvent.floci.testing.MutableClock;
import jakarta.enterprise.event.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class SqsQueueActivityServiceTest {
    private Event<QueueActivity> events;
    private SqsService service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        events = mock(Event.class);
        service = new SqsService(new InMemoryStorage<>(), new InMemoryStorage<>(), new InMemoryStorage<>(),
                new InMemoryStorage<>(), 30, 1048576, "http://localhost:4566",
                new RegionResolver("us-east-1", "000000000000"), false,
                null, new MutableClock(), "activity-test", null, null, events);
    }

    @Test
    void newMessagesPublishTheOwningQueueScopeButDuplicatesDoNot() {
        Queue queue = service.createQueue("events.fifo", Map.of("FifoQueue", "true"), "cn-north-1");
        Message first = service.sendMessage(queue.getQueueUrl(), "event", null, "group", "dedup", "cn-north-1");
        Message duplicate = service.sendMessage(queue.getQueueUrl(), "event", null, "group", "dedup", "cn-north-1");
        assertEquals(first.getMessageId(), duplicate.getMessageId());
        verify(events).fire(new QueueActivity("000000000000", "cn-north-1", "events.fifo"));
        verifyNoMoreInteractions(events);
    }

    @Test
    void rejectedMessageDoesNotPublishActivity() {
        Queue queue = service.createQueue("events.fifo", Map.of("FifoQueue", "true"), "us-east-1");
        assertThrows(AwsException.class, () -> service.sendMessage(queue.getQueueUrl(), "event", null, "us-east-1"));
        verifyNoInteractions(events);
    }

    @Test
    void observerFailureCannotChangeSuccessfulEnqueueOrConsumeTheMessage() {
        Queue queue = service.createQueue("events", null, "us-east-1");
        doThrow(new IllegalStateException("observer unavailable")).when(events).fire(any());
        Message sent = service.sendMessage(queue.getQueueUrl(), "keep-me", null, "us-east-1");
        List<Message> received = service.receiveMessage(queue.getQueueUrl(), 1, 30, 0, "us-east-1");
        assertEquals(sent.getMessageId(), received.getFirst().getMessageId());
        assertEquals("keep-me", received.getFirst().getBody());
    }
}
