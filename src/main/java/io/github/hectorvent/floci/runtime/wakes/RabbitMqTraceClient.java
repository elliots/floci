package io.github.hectorvent.floci.runtime.wakes;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.PossibleAuthenticationFailureException;
import com.rabbitmq.client.ShutdownSignalException;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Floci's AMQP connections to its brokers for message wakes. A subscription consumes a virtual
 * host's firehose: its publishes ({@code publish.#}) and the deliveries of the queues it binds
 * ({@code deliver.QUEUE}). It goes through a server-named, exclusive trace queue of at most
 * {@link #TRACE_QUEUE_LENGTH} messages, which drops its oldest message when full and dead-letters
 * it to a lost queue of one message, so that a dropped trace is noticed. Trace messages are
 * acknowledged in batches with a prefetch of {@link #PREFETCH}, so a backlog waits in the trace
 * queue. Each connection is named {@value #CONNECTION_NAME}, has a 30 s heartbeat and no
 * automatic recovery: its owner subscribes again. The close of the connection or its channel, and
 * the broker's cancel of a consumer, such as when a user deletes one of Floci's queues, each end
 * the subscription.
 */
@ApplicationScoped
public class RabbitMqTraceClient {

    static final String CONNECTION_NAME = "floci message wakes";
    static final String TRACE_EXCHANGE = "amq.rabbitmq.trace";
    static final String EVENT_EXCHANGE = "amq.rabbitmq.event";
    static final String VIRTUAL_HOST_CREATED = "vhost.created";
    static final int PREFETCH = 500;
    static final int ACK_BATCH = PREFETCH / 2;
    static final Duration ACK_DELAY = Duration.ofMillis(200);
    static final int TRACE_QUEUE_LENGTH = 10_000;
    static final int HEARTBEAT_SECONDS = 30;
    static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);

    private static final Logger LOG = Logger.getLogger(RabbitMqTraceClient.class);
    private static final int NOT_ALLOWED = 530;
    private static final int ACCESS_REFUSED = 403;

    /** Where and as whom to connect. */
    public record Endpoint(URI amqp, String virtualHost, String username, String password) {
    }

    /** Hears a subscription, on the AMQP client's threads. Implementations return at once. */
    public interface Handler {

        void trace(RabbitMqTrace trace);

        /** The trace queue dropped a trace. */
        void lost();

        /**
         * The subscription ended, once, not by {@link Subscription#close}: the connection or its
         * channel closed, or the broker cancelled one of its consumers.
         */
        void closed(String why);
    }

    /** An open connection. */
    public interface Subscription extends AutoCloseable {

        /** Closes the connection, and tells its listeners nothing. */
        @Override
        void close();
    }

    /** An open subscription to a firehose. */
    public interface TraceSubscription extends Subscription {

        /** Binds the trace queue to the firehose with this key as well. */
        void bind(String key) throws IOException;

        void unbind(String key) throws IOException;
    }

    /** The broker refused Floci's user, or its permissions. */
    public static final class RefusedException extends Exception {

        RefusedException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** The virtual host does not exist. */
    public static final class NoVirtualHostException extends Exception {

        NoVirtualHostException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private final ScheduledExecutorService acks = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("rabbitmq-wakes-acks").factory());

    @PreDestroy
    void close() {
        acks.shutdownNow();
    }

    /**
     * Subscribes to the firehose of the endpoint's virtual host: every publish, and the deliveries
     * bound with {@code deliverKeys}.
     */
    public TraceSubscription subscribe(Endpoint endpoint, Set<String> deliverKeys, Handler handler)
            throws IOException, RefusedException, NoVirtualHostException {
        Connection connection = connect(endpoint);
        Ending ending = new Ending(handler::closed);
        try {
            Channel channel = connection.createChannel();
            String lost = channel.queueDeclare("", false, true, true, Map.of(
                    "x-max-length", 1,
                    "x-overflow", "drop-head")).getQueue();
            Map<String, Object> traceArguments = new HashMap<>();
            traceArguments.put("x-max-length", TRACE_QUEUE_LENGTH);
            traceArguments.put("x-overflow", "drop-head");
            traceArguments.put("x-dead-letter-exchange", "");
            traceArguments.put("x-dead-letter-routing-key", lost);
            String trace = channel.queueDeclare("", false, true, true, traceArguments).getQueue();
            channel.queueBind(trace, TRACE_EXCHANGE, RabbitMqTrace.PUBLISH + "#");
            for (String key : deliverKeys) {
                channel.queueBind(trace, TRACE_EXCHANGE, key);
            }
            channel.basicQos(PREFETCH);
            channel.basicConsume(lost, true, new EndingConsumer(channel, ending) {
                @Override
                public void handleDelivery(String tag, Envelope envelope, AMQP.BasicProperties properties,
                                           byte[] body) {
                    handler.lost();
                }
            });
            channel.basicConsume(trace, false, new TraceConsumer(channel, handler, ending));
            connection.addShutdownListener(cause -> ending.report(describe(cause)));
            return new TraceSubscription() {
                @Override
                public void bind(String key) throws IOException {
                    channel.queueBind(trace, TRACE_EXCHANGE, key);
                }

                @Override
                public void unbind(String key) throws IOException {
                    channel.queueUnbind(trace, TRACE_EXCHANGE, key);
                }

                @Override
                public void close() {
                    ending.close();
                    closeQuietly(connection, endpoint);
                }
            };
        } catch (IOException | RuntimeException e) {
            ending.close();
            closeQuietly(connection, endpoint);
            throw classify(e, endpoint);
        }
    }

    /**
     * Follows the broker's {@code vhost.created} events in the endpoint's virtual host, which the
     * event exchange plugin publishes on {@code /}, and tells {@code created} each new virtual
     * host's name.
     */
    public Subscription followVirtualHosts(Endpoint endpoint, Consumer<String> created, Consumer<String> closed)
            throws IOException, RefusedException, NoVirtualHostException {
        Connection connection = connect(endpoint);
        Ending ending = new Ending(closed);
        try {
            Channel channel = connection.createChannel();
            String events = channel.queueDeclare("", false, true, true, Map.of()).getQueue();
            channel.queueBind(events, EVENT_EXCHANGE, VIRTUAL_HOST_CREATED);
            channel.basicConsume(events, true, new EndingConsumer(channel, ending) {
                @Override
                public void handleDelivery(String tag, Envelope envelope, AMQP.BasicProperties properties,
                                           byte[] body) {
                    Map<String, Object> headers = properties.getHeaders();
                    Object name = headers == null ? null : headers.get("name");
                    if (name != null) {
                        created.accept(name.toString());
                    }
                }
            });
            connection.addShutdownListener(cause -> ending.report(describe(cause)));
            return () -> {
                ending.close();
                closeQuietly(connection, endpoint);
            };
        } catch (IOException | RuntimeException e) {
            ending.close();
            closeQuietly(connection, endpoint);
            throw classify(e, endpoint);
        }
    }

    private static Connection connect(Endpoint endpoint) throws IOException, RefusedException,
            NoVirtualHostException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(endpoint.amqp().getHost());
        factory.setPort(endpoint.amqp().getPort() < 0 ? ConnectionFactory.DEFAULT_AMQP_PORT : endpoint.amqp().getPort());
        factory.setVirtualHost(endpoint.virtualHost());
        factory.setUsername(endpoint.username());
        factory.setPassword(endpoint.password());
        factory.setRequestedHeartbeat(HEARTBEAT_SECONDS);
        factory.setConnectionTimeout((int) CONNECT_TIMEOUT.toMillis());
        factory.setHandshakeTimeout((int) CONNECT_TIMEOUT.toMillis());
        factory.setAutomaticRecoveryEnabled(false);
        factory.setTopologyRecoveryEnabled(false);
        factory.setThreadFactory(Thread.ofPlatform().daemon().name("rabbitmq-wakes-amqp-", 0).factory());
        try {
            return factory.newConnection(CONNECTION_NAME);
        } catch (TimeoutException e) {
            throw new IOException("no answer within " + CONNECT_TIMEOUT.toSeconds() + " s", e);
        } catch (IOException | RuntimeException e) {
            throw classify(e, endpoint);
        }
    }

    /**
     * Throws a refusal or a missing virtual host as such, and returns any other failure of a
     * connection or a declaration to throw.
     */
    private static IOException classify(Exception failure, Endpoint endpoint)
            throws RefusedException, NoVirtualHostException {
        if (failure instanceof PossibleAuthenticationFailureException) {
            throw new RefusedException("the broker refused the user " + endpoint.username(), failure);
        }
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof ShutdownSignalException signal) {
                int code = replyCode(signal);
                if (code == NOT_ALLOWED) {
                    throw new NoVirtualHostException("the virtual host " + endpoint.virtualHost()
                            + " does not exist", failure);
                }
                if (code == ACCESS_REFUSED) {
                    throw new RefusedException(describe(signal), failure);
                }
            }
        }
        if (failure instanceof IOException io) {
            return io;
        }
        return new IOException(failure.getMessage(), failure);
    }

    private static int replyCode(ShutdownSignalException signal) {
        return switch (signal.getReason()) {
            case AMQP.Connection.Close close -> close.getReplyCode();
            case AMQP.Channel.Close close -> close.getReplyCode();
            case null, default -> 0;
        };
    }

    static String describe(ShutdownSignalException cause) {
        return switch (cause.getReason()) {
            case AMQP.Connection.Close close -> close.getReplyText();
            case AMQP.Channel.Close close -> close.getReplyText();
            case null, default -> cause.getMessage();
        };
    }

    private static void closeQuietly(Connection connection, Endpoint endpoint) {
        try {
            if (connection.isOpen()) {
                connection.close((int) CONNECT_TIMEOUT.toMillis());
            }
        } catch (IOException | RuntimeException e) {
            LOG.debugv(e, "Closing Floci''s connection to virtual host {0} at {1} failed", endpoint.virtualHost(),
                    endpoint.amqp());
        }
    }

    /** Reports the end of a subscription once, unless Floci closes it. */
    static final class Ending {

        private final AtomicBoolean closing = new AtomicBoolean();
        private final AtomicBoolean reported = new AtomicBoolean();
        private final Consumer<String> listener;

        Ending(Consumer<String> listener) {
            this.listener = listener;
        }

        /** Floci closes the subscription: nothing is reported from now on. */
        void close() {
            closing.set(true);
        }

        void report(String why) {
            if (!closing.get() && reported.compareAndSet(false, true)) {
                listener.accept(why);
            }
        }
    }

    /** A consumer whose cancel by the broker, or the close of its channel, ends its subscription. */
    static class EndingConsumer extends DefaultConsumer {

        private final Ending ending;

        EndingConsumer(Channel channel, Ending ending) {
            super(channel);
            this.ending = ending;
        }

        @Override
        public void handleCancel(String tag) {
            ending.report("the broker cancelled Floci's consumer " + tag + ", as when its queue is deleted");
        }

        @Override
        public void handleShutdownSignal(String tag, ShutdownSignalException signal) {
            ending.report(describe(signal));
        }
    }

    /** Hands each trace to the handler, and acknowledges them in batches once handled. */
    private final class TraceConsumer extends EndingConsumer {

        private final Handler handler;
        private long lastTag;
        private int unacknowledged;
        private ScheduledFuture<?> flush;

        TraceConsumer(Channel channel, Handler handler, Ending ending) {
            super(channel, ending);
            this.handler = handler;
        }

        @Override
        public void handleDelivery(String tag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
            RabbitMqTrace trace = RabbitMqTrace.of(envelope.getRoutingKey(), properties.getHeaders());
            if (trace != null) {
                try {
                    handler.trace(trace);
                } catch (RuntimeException e) {
                    LOG.errorv(e, "Handling the trace {0} failed", envelope.getRoutingKey());
                }
            }
            synchronized (this) {
                lastTag = envelope.getDeliveryTag();
                unacknowledged++;
                if (unacknowledged >= ACK_BATCH) {
                    acknowledge();
                } else if (flush == null) {
                    flush = acks.schedule(this::flush, ACK_DELAY.toMillis(), TimeUnit.MILLISECONDS);
                }
            }
        }

        private synchronized void flush() {
            flush = null;
            acknowledge();
        }

        private void acknowledge() {
            if (unacknowledged == 0) {
                return;
            }
            unacknowledged = 0;
            try {
                getChannel().basicAck(lastTag, true);
            } catch (IOException | RuntimeException e) {
                LOG.debugv(e, "Acknowledging traces up to {0} failed", lastTag);
            }
        }
    }
}
