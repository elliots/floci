package io.github.hectorvent.floci.runtime.wakes;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownSignalException;
import io.github.hectorvent.floci.runtime.wakes.RabbitMqTraceClient.Ending;
import io.github.hectorvent.floci.runtime.wakes.RabbitMqTraceClient.EndingConsumer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

/** The end of a subscription: a consumer's cancel or its channel's close, reported once. */
class RabbitMqTraceClientServiceTest {

    private final List<String> ends = new CopyOnWriteArrayList<>();
    private final Ending ending = new Ending(ends::add);

    @Test
    void theBrokersCancelOfAConsumerEndsTheSubscriptionOnce() {
        EndingConsumer trace = new EndingConsumer(mock(Channel.class), ending);
        EndingConsumer lost = new EndingConsumer(mock(Channel.class), ending);

        trace.handleCancel("amq.ctag-1");
        lost.handleCancel("amq.ctag-2");
        ending.report("connection reset");

        assertThat(ends, hasSize(1));
        assertThat(ends.getFirst().contains("amq.ctag-1"), is(true));
    }

    @Test
    void aClosedChannelEndsTheSubscription() {
        EndingConsumer trace = new EndingConsumer(mock(Channel.class), ending);

        trace.handleShutdownSignal("amq.ctag-1", new ShutdownSignalException(false, false, null, null));

        assertThat(ends, hasSize(1));
    }

    @Test
    void nothingIsReportedOnceFlociCloses() {
        EndingConsumer trace = new EndingConsumer(mock(Channel.class), ending);

        ending.close();
        trace.handleCancel("amq.ctag-1");
        trace.handleShutdownSignal("amq.ctag-1", new ShutdownSignalException(false, true, null, null));

        assertThat(ends, is(empty()));
    }

    @Test
    void theFirstEndIsTheOneReported() {
        ending.report("CONNECTION_FORCED - broker forced connection closure");
        ending.report("connection reset");

        assertThat(ends, contains("CONNECTION_FORCED - broker forced connection closure"));
    }
}
