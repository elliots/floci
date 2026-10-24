package io.github.hectorvent.floci.runtime.wakes;

import io.github.hectorvent.floci.core.common.AwsArnUtils;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RequestScopes;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager.ContainerPresence;
import io.github.hectorvent.floci.runtime.RetryBackoff;
import io.github.hectorvent.floci.runtime.wakes.RabbitMqTraceClient.Endpoint;
import io.github.hectorvent.floci.runtime.wakes.RabbitMqTraceClient.NoVirtualHostException;
import io.github.hectorvent.floci.runtime.wakes.RabbitMqTraceClient.RefusedException;
import io.github.hectorvent.floci.runtime.wakes.RabbitMqTraceClient.Subscription;
import io.github.hectorvent.floci.runtime.wakes.RabbitMqTraceClient.TraceSubscription;
import io.github.hectorvent.floci.runtime.wakes.WorkloadActivity.Read;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.RabbitMqQueue;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Workload;
import io.github.hectorvent.floci.services.amazonmq.AmazonMqService;
import io.github.hectorvent.floci.services.amazonmq.BrokerListener;
import io.github.hectorvent.floci.services.amazonmq.container.RabbitMqManager;
import io.github.hectorvent.floci.services.amazonmq.container.RabbitMqManager.QueueCounts;
import io.github.hectorvent.floci.services.amazonmq.container.RabbitMqManager.VirtualHost;
import io.github.hectorvent.floci.services.amazonmq.container.RabbitMqManager.VirtualHostQueues;
import io.github.hectorvent.floci.services.amazonmq.model.Broker;
import io.github.hectorvent.floci.services.amazonmq.model.BrokerInstance;
import io.github.hectorvent.floci.services.amazonmq.model.BrokerState;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The RabbitMQ side of message wakes, for queues on the Amazon MQ brokers that Floci runs in
 * Docker. For each virtual host that a workload watches, Floci turns the broker's firehose on
 * ({@code rabbitmqctl trace_on}) unless it is on already, and follows it as its own broker user,
 * {@code floci-wakes-RANDOM}: a publish routed to a watched queue, and a delivery from one, are
 * events of the workloads that read the queue. The broker publishes no event for an
 * acknowledgement, nor for a message that leaves a queue undelivered, such as one that expires or
 * is purged, so a decision reads the queues' counts with {@code rabbitmqctl list_queues}: ready
 * or unacknowledged messages keep the leases until a read one linger later. A virtual host that
 * does not exist yet is followed from the broker's {@code vhost.created} event.
 *
 * <p>Subscriptions and their {@code rabbitmqctl} commands run on one thread,
 * {@code rabbitmq-wakes}, and reads on virtual threads; traces arrive on the AMQP client's
 * threads and touch only the workloads' activities. A subscription that fails is made again after
 * 1 s, doubling to 30 s, while the broker's container is there, and a new subscription reads its
 * queues once, so that nothing published before it is missed. While the container is stopped,
 * its queues have no messages, so they hold no lease, and the subscription is still made again.
 */
@ApplicationScoped
public class RabbitMqQueueWatch implements BrokerListener {

    /** A queue name longer than this makes a {@code deliver.QUEUE} key past AMQP's 255 bytes. */
    static final int MAX_KEYED_QUEUE_BYTES = 255 - RabbitMqTrace.DELIVER.length();

    private static final Logger LOG = Logger.getLogger(RabbitMqQueueWatch.class);
    private static final String NOT_FOUND = "NotFoundException";
    private static final String ROOT = "/";

    private record BrokerKey(String accountId, String region, String brokerId) {
    }

    private record HostKey(BrokerKey broker, String virtualHost) {
    }

    /** A workload that reads a watched queue, and the queue's name in its wakes. */
    private record Target(WorkloadActivity activity, String resource) {
    }

    /** Where a virtual host's subscription stands. */
    enum State {
        /** Floci is setting the subscription up. */
        SUBSCRIBING,
        SUBSCRIBED,
        /** The subscription failed, and is made again after a delay. */
        RETRYING,
        /** The broker's container is stopped; the subscription is made again after a delay. */
        STOPPED,
        /** The virtual host does not exist; its {@code vhost.created} event subscribes it. */
        MISSING,
        /** The broker refused Floci's user. */
        REFUSED,
        /** The broker's container is gone. */
        ENDED
    }

    /** One read of a workload's RabbitMQ queues, by its ticket. */
    private record Call(long ticket, CompletableFuture<Read> answer) {
    }

    /** The counts of the queues of a virtual host and the queues that are down, or why there are none. */
    private record Counts(Map<String, QueueCounts> queues, Set<String> down, boolean missing, String failure) {

        static Counts of(VirtualHostQueues queues) {
            return new Counts(queues.counts(), queues.down(), false, null);
        }

        static Counts missingHost() {
            return new Counts(Map.of(), Set.of(), true, null);
        }

        static Counts failed(String why) {
            return new Counts(Map.of(), Set.of(), false, why);
        }
    }

    /** A watched broker; touched only on the control thread. */
    private final class BrokerWatch {
        final BrokerKey key;
        final Map<String, HostWatch> hosts = new LinkedHashMap<>();
        URI amqp;
        boolean userReady;
        boolean eventExchange;
        Subscription virtualHostEvents;
        Duration eventsRetry;

        BrokerWatch(BrokerKey key) {
            this.key = key;
        }

        String id() {
            return key.brokerId();
        }
    }

    /** A watched virtual host; touched only on the control thread, except its targets. */
    private final class HostWatch {
        final HostKey key;
        volatile Map<String, List<Target>> targets = Map.of();
        Set<String> keys = Set.of();
        TraceSubscription subscription;
        boolean tracingOurs;
        State state = State.SUBSCRIBING;
        Duration retry;
        ScheduledFuture<?> retryTask;
        long generation;

        HostWatch(HostKey key) {
            this.key = key;
        }

        String name() {
            return key.virtualHost();
        }
    }

    private final AmazonMqService brokers;
    private final RabbitMqManager containers;
    private final RabbitMqTraceClient client;
    private final ExecutorService readers;
    private final InstantSource clock;
    private final ScheduledExecutorService control;
    private final String username;
    private final String password;
    private final Map<BrokerKey, BrokerWatch> watches = new LinkedHashMap<>();
    private final Map<BrokerKey, String> unwatchable = new HashMap<>();
    private final Map<String, Call> calls = new HashMap<>();
    private final Set<String> logged = ConcurrentHashMap.newKeySet();
    private volatile List<WorkloadActivity> watched = List.of();
    private volatile Map<HostKey, State> views = Map.of();
    private volatile Map<BrokerKey, String> unwatched = Map.of();
    private volatile Runnable loop = () -> { };
    private boolean listening;

    @Inject
    public RabbitMqQueueWatch(AmazonMqService brokers, RabbitMqManager containers, RabbitMqTraceClient client) {
        this(brokers, containers, client, InstantSource.system(), Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().daemon().name("rabbitmq-wakes").factory()),
                Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("rabbitmq-wakes-read-", 0).factory()),
                "floci-wakes-" + random(4), random(24));
    }

    RabbitMqQueueWatch(AmazonMqService brokers, RabbitMqManager containers, RabbitMqTraceClient client,
                       InstantSource clock, ScheduledExecutorService control, ExecutorService readers,
                       String username, String password) {
        this.brokers = brokers;
        this.containers = containers;
        this.client = client;
        this.clock = clock;
        this.control = control;
        this.readers = readers;
        this.username = username;
        this.password = password;
    }

    /** Hears Amazon MQ's brokers from now on, and runs {@code loop} when the wake loop must look at a workload. */
    public synchronized void start(Runnable loop) {
        this.loop = loop;
        if (!listening) {
            listening = true;
            brokers.addBrokerListener(this);
        }
    }

    /** Watches the RabbitMQ queues of exactly these workloads. */
    void watch(Collection<WorkloadActivity> activities) {
        watched = List.copyOf(activities);
        post(this::reconcile);
    }

    /** Ends every subscription and forgets every read, as after a reset, which watches again. */
    void clear() {
        calls.clear();
        logged.clear();
        watched = List.of();
        post(() -> {
            unwatchable.clear();
            reconcile();
        });
    }

    /** Closes every connection, with no command in the brokers: Floci stops, and its brokers with it. */
    @PreDestroy
    void stop() {
        post(() -> {
            for (BrokerWatch broker : watches.values()) {
                for (HostWatch host : broker.hosts.values()) {
                    close(host);
                }
                closeVirtualHostEvents(broker);
            }
            watches.clear();
        });
        control.shutdown();
        try {
            if (!control.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.warn("Closing the RabbitMQ subscriptions of message wakes took longer than 5 s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        readers.shutdownNow();
    }

    @Override
    public void brokerRunning(Broker broker) {
        post(this::reconcile);
    }

    @Override
    public void brokerDeleted(Broker broker) {
        post(this::reconcile);
    }

    /** The name of a RabbitMQ queue in a wake's reason. */
    static String resource(RabbitMqQueue queue) {
        return "amqp://" + queue.brokerId() + "/" + segment(queue.virtualHost()) + "/" + segment(queue.queue());
    }

    /** The {@code deliver} keys that bind exactly these queues, or every delivery when a name cannot be a key. */
    static Set<String> deliverKeys(Collection<String> queues) {
        Set<String> keys = new LinkedHashSet<>();
        for (String queue : queues) {
            if (!keyable(queue)) {
                return Set.of(RabbitMqTrace.DELIVER + "#");
            }
            keys.add(RabbitMqTrace.DELIVER + queue);
        }
        return keys;
    }

    private static boolean keyable(String queue) {
        if (queue.getBytes(StandardCharsets.UTF_8).length > MAX_KEYED_QUEUE_BYTES) {
            return false;
        }
        for (String word : queue.split("\\.", -1)) {
            if (word.equals("*") || word.equals("#")) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reads the workload's RabbitMQ queues, on the wake loop's thread: held for the first queue
     * with ready messages, else for the first with unacknowledged ones; else failed when a queue
     * could not be read; else none. The calls to the brokers run beside the loop: the read
     * answers pending, tells the loop when its answer is there, and gives it when asked again
     * with the same ticket.
     */
    Read read(Workload workload, long ticket) {
        Call call = calls.get(workload.name());
        if (call != null && call.ticket() == ticket) {
            if (!call.answer().isDone()) {
                return Read.pending();
            }
            calls.remove(workload.name());
            return call.answer().join();
        }
        CompletableFuture<Read> answer = call(workload);
        if (answer.isDone()) {
            calls.remove(workload.name());
            return answer.join();
        }
        calls.put(workload.name(), new Call(ticket, answer));
        answer.whenComplete((read, error) -> loop.run());
        return Read.pending();
    }

    private CompletableFuture<Read> call(Workload workload) {
        Map<HostKey, CompletableFuture<Counts>> hosts = new LinkedHashMap<>();
        for (RabbitMqQueue queue : workload.rabbitMqQueues()) {
            HostKey host = hostKey(workload, queue);
            hosts.computeIfAbsent(host, key -> counts(workload, key));
        }
        return CompletableFuture.allOf(hosts.values().toArray(CompletableFuture[]::new))
                .thenApply(ignored -> {
                    Map<HostKey, Counts> answers = new HashMap<>();
                    hosts.forEach((key, counts) -> answers.put(key, counts.join()));
                    return read(workload, answers);
                });
    }

    private Read read(Workload workload, Map<HostKey, Counts> answers) {
        String held = null;
        boolean failed = false;
        for (RabbitMqQueue queue : workload.rabbitMqQueues()) {
            Counts counts = answers.get(hostKey(workload, queue));
            if (counts.failure() != null) {
                failed = true;
                continue;
            }
            if (counts.missing()) {
                continue;
            }
            String downKey = "down " + workload.name() + " " + resource(queue);
            if (counts.down().contains(queue.queue())) {
                once(downKey, () -> LOG.warnv("The RabbitMQ queue {0} of broker {1} and virtual host {2} has no "
                        + "counts, so it may be down; Floci keeps the leases of workload {3} and reads it again",
                        queue.queue(), queue.brokerId(), queue.virtualHost(), workload.name()));
                failed = true;
                continue;
            }
            logged.remove(downKey);
            QueueCounts queueCounts = counts.queues().get(queue.queue());
            if (queueCounts == null) {
                once("missing " + workload.name() + " " + resource(queue), () -> LOG.infov(
                        "Workload {0} reads the RabbitMQ queue {1} of broker {2} and virtual host {3}, which does not "
                        + "exist; its messages wake the workload''s pods once it does", workload.name(), queue.queue(),
                        queue.brokerId(), queue.virtualHost()));
                continue;
            }
            logged.remove("missing " + workload.name() + " " + resource(queue));
            if (queueCounts.ready() > 0) {
                return Read.held(resource(queue), null);
            } else if (queueCounts.unacknowledged() > 0 && held == null) {
                held = resource(queue);
            }
        }
        if (held != null) {
            return Read.held(held, null);
        }
        return failed ? Read.failed() : Read.none(null);
    }

    /** Reads the counts of a virtual host's queues in the broker's container. */
    private CompletableFuture<Counts> counts(Workload workload, HostKey key) {
        State state = views.get(key);
        if (state == null) {
            String why = unwatched.get(key.broker());
            if (why == null) {
                return CompletableFuture.completedFuture(Counts.failed("Floci has not subscribed yet"));
            }
            once("unwatched " + workload.name() + " " + key + " " + why, () -> LOG.infov(
                    "Workload {0} reads RabbitMQ queues of broker {1} in account {2} and region {3}, but {4}, so "
                    + "they have no messages for the workload''s pods", workload.name(), key.broker().brokerId(),
                    key.broker().accountId(), key.broker().region(), why));
            return CompletableFuture.completedFuture(Counts.missingHost());
        }
        return switch (state) {
            case SUBSCRIBING, RETRYING -> CompletableFuture.completedFuture(Counts.failed("not subscribed"));
            case MISSING, REFUSED, STOPPED, ENDED -> CompletableFuture.completedFuture(Counts.missingHost());
            case SUBSCRIBED -> list(key);
        };
    }

    private CompletableFuture<Counts> list(HostKey key) {
        CompletableFuture<Counts> answer = new CompletableFuture<>();
        try {
            readers.execute(() -> {
                Counts counts;
                try {
                    counts = Counts.of(containers.queueCounts(key.broker().brokerId(), key.virtualHost()));
                } catch (RuntimeException e) {
                    counts = Counts.failed(e.getMessage() == null ? e.toString() : e.getMessage());
                }
                String problem = "unreadable " + key;
                if (counts.failure() != null) {
                    String why = counts.failure();
                    once(problem, () -> LOG.warnv("Reading the RabbitMQ queues of broker {0} and virtual host {1} "
                            + "failed, so Floci keeps the leases of the workloads that read them and tries again: {2}",
                            key.broker().brokerId(), key.virtualHost(), why));
                } else if (logged.remove(problem)) {
                    LOG.infov("Floci reads the RabbitMQ queues of broker {0} and virtual host {1} again",
                            key.broker().brokerId(), key.virtualHost());
                }
                answer.complete(counts);
            });
        } catch (RejectedExecutionException e) {
            answer.complete(Counts.failed("Floci stops"));
        }
        return answer;
    }

    /** Matches the watched virtual hosts and their subscriptions to the watched workloads, on the control thread. */
    private void reconcile() {
        Map<BrokerKey, Map<String, Map<String, List<Target>>>> wanted = new LinkedHashMap<>();
        for (WorkloadActivity activity : watched) {
            Workload workload = activity.workload();
            for (RabbitMqQueue queue : workload.rabbitMqQueues()) {
                HostKey host = hostKey(workload, queue);
                wanted.computeIfAbsent(host.broker(), key -> new LinkedHashMap<>())
                        .computeIfAbsent(host.virtualHost(), key -> new LinkedHashMap<>())
                        .computeIfAbsent(queue.queue(), key -> new ArrayList<>())
                        .add(new Target(activity, resource(queue)));
            }
        }
        for (BrokerWatch broker : List.copyOf(watches.values())) {
            if (!wanted.containsKey(broker.key)) {
                end(broker);
            }
        }
        unwatchable.keySet().retainAll(wanted.keySet());
        wanted.forEach((key, hosts) -> {
            Broker broker = runningBroker(key);
            if (broker == null) {
                return;
            }
            BrokerWatch watch = watches.computeIfAbsent(key, BrokerWatch::new);
            BrokerInstance instance = broker.getBrokerInstances().getFirst();
            watch.amqp = URI.create(instance.getEndpoints().getFirst());
            for (HostWatch host : List.copyOf(watch.hosts.values())) {
                if (!hosts.containsKey(host.name())) {
                    unwatch(watch, host);
                }
            }
            hosts.forEach((name, queues) -> {
                HostWatch host = watch.hosts.get(name);
                boolean added = host == null;
                if (added) {
                    host = new HostWatch(new HostKey(key, name));
                    watch.hosts.put(name, host);
                }
                Map<String, List<Target>> targets = new HashMap<>();
                queues.forEach((queue, list) -> targets.put(queue, List.copyOf(list)));
                host.targets = Map.copyOf(targets);
                if (added || host.state == State.ENDED || host.state == State.REFUSED) {
                    subscribe(watch, host);
                } else if (host.state == State.SUBSCRIBED) {
                    rebind(watch, host);
                }
            });
        });
        publish();
    }

    /** The broker, when it runs in a container of this Floci run; else null, with the reason logged once. */
    private Broker runningBroker(BrokerKey key) {
        String why;
        Broker broker = null;
        try {
            broker = RequestScopes.callAs(key.accountId(), key.region(), () -> brokers.describeBroker(key.brokerId()));
            why = problem(key, broker);
        } catch (AwsException e) {
            why = NOT_FOUND.equals(e.getErrorCode()) ? "the broker does not exist"
                    : "Amazon MQ answered " + e.getErrorCode() + ": " + e.getMessage();
        } catch (RuntimeException e) {
            why = "finding the broker failed: " + e;
        }
        if (why == null) {
            unwatchable.remove(key);
            return broker;
        }
        BrokerWatch existing = watches.get(key);
        if (existing != null) {
            end(existing);
        }
        if (!why.equals(unwatchable.put(key, why))) {
            LOG.infov("Floci does not follow the RabbitMQ queues of broker {0} in account {1} and region {2}: {3}",
                    key.brokerId(), key.accountId(), key.region(), why);
        }
        return null;
    }

    private String problem(BrokerKey key, Broker broker) {
        AwsArnUtils.Arn arn = AwsArnUtils.parse(broker.getBrokerArn());
        if (!key.accountId().equals(arn.accountId()) || !key.region().equals(arn.region())) {
            return "the broker does not exist";
        }
        if (broker.getBrokerState() != BrokerState.RUNNING) {
            return "the broker is " + broker.getBrokerState();
        }
        if (!containers.running(key.brokerId())) {
            return "Floci does not run the broker's container, as for a broker of an earlier run or with "
                    + "floci.services.amazonmq.mock";
        }
        List<BrokerInstance> instances = broker.getBrokerInstances();
        if (instances == null || instances.isEmpty() || instances.getFirst().getEndpoints() == null
                || instances.getFirst().getEndpoints().isEmpty()) {
            return "the broker has no endpoints";
        }
        return null;
    }

    /**
     * Turns the virtual host's firehose on, unless it is on, and subscribes to it. A virtual host
     * of a stopped container stays stopped until the subscription is made.
     */
    private void subscribe(BrokerWatch broker, HostWatch host) {
        cancelRetry(host);
        close(host);
        if (host.state != State.STOPPED) {
            host.state = State.SUBSCRIBING;
            publish();
        }
        try {
            if (!broker.userReady) {
                containers.addWakeUser(broker.id(), username, password);
                broker.userReady = true;
            }
            VirtualHost found = containers.virtualHosts(broker.id()).stream()
                    .filter(candidate -> candidate.name().equals(host.name()))
                    .findFirst()
                    .orElse(null);
            if (found == null || !containers.grantWakeUser(broker.id(), host.name(), username)) {
                missing(broker, host);
                return;
            }
            if (!found.tracing()) {
                containers.traceOn(broker.id(), host.name());
                host.tracingOurs = true;
            }
            host.keys = deliverKeys(host.targets.keySet());
            long generation = ++host.generation;
            host.subscription = client.subscribe(new Endpoint(broker.amqp, host.name(), username, password),
                    host.keys, new HostHandler(host, generation));
            host.state = State.SUBSCRIBED;
            host.retry = null;
            logged.remove("stopped " + host.key);
            LOG.infov("Floci follows the firehose of virtual host {0} of broker {1} for message wakes, as the "
                    + "broker user {2}", host.name(), broker.id(), username);
            requestReads(host);
            closeVirtualHostEventsWhenUnneeded(broker);
        } catch (RefusedException e) {
            host.state = State.REFUSED;
            LOG.warnv("Broker {0} refused Floci''s user {1} on virtual host {2}, so Floci follows no queue there "
                    + "until the runtime file changes or the broker runs again: {3}", broker.id(), username,
                    host.name(), e.getMessage());
        } catch (NoVirtualHostException e) {
            missing(broker, host);
        } catch (IOException | RuntimeException e) {
            failed(broker, host, e);
        } finally {
            publish();
        }
    }

    private void rebind(BrokerWatch broker, HostWatch host) {
        Set<String> keys = deliverKeys(host.targets.keySet());
        if (keys.equals(host.keys)) {
            return;
        }
        try {
            for (String key : keys) {
                if (!host.keys.contains(key)) {
                    host.subscription.bind(key);
                }
            }
            for (String key : host.keys) {
                if (!keys.contains(key)) {
                    host.subscription.unbind(key);
                }
            }
            host.keys = keys;
        } catch (IOException | RuntimeException e) {
            failed(broker, host, e);
        }
    }

    private void failed(BrokerWatch broker, HostWatch host, Exception failure) {
        close(host);
        ContainerPresence presence = containers.containerPresence(broker.id());
        if (presence == ContainerPresence.ABSENT) {
            host.state = State.ENDED;
            broker.userReady = false;
            LOG.warnv("The container of broker {0} is gone, so Floci follows no queue of virtual host {1} until the "
                    + "broker runs again", broker.id(), host.name());
            return;
        }
        host.retry = RetryBackoff.next(host.retry);
        if (presence == ContainerPresence.STOPPED) {
            host.state = State.STOPPED;
            broker.userReady = false;
            once("stopped " + host.key, () -> LOG.warnv("The container of broker {0} is stopped, so the queues of "
                    + "virtual host {1} have no messages and Floci holds no lease for them; it follows them again "
                    + "once the container runs", broker.id(), host.name()));
            LOG.debugv("Following the firehose of virtual host {0} of broker {1} failed: {2}", host.name(),
                    broker.id(), failure.toString());
        } else if (host.retry.equals(RetryBackoff.FIRST)) {
            host.state = State.RETRYING;
            LOG.warnv("Following the firehose of virtual host {0} of broker {1} failed, so Floci keeps the leases of "
                    + "the workloads that read its queues and tries again: {2}", host.name(), broker.id(),
                    failure.toString());
            LOG.debugv(failure, "Following the firehose of virtual host {0} of broker {1} failed", host.name(),
                    broker.id());
        } else {
            host.state = State.RETRYING;
            LOG.debugv("Following the firehose of virtual host {0} of broker {1} failed again: {2}", host.name(),
                    broker.id(), failure.toString());
        }
        host.retryTask = control.schedule(() -> {
            host.retryTask = null;
            if (watches.get(broker.key) == broker && broker.hosts.get(host.name()) == host
                    && (host.state == State.RETRYING || host.state == State.STOPPED)) {
                subscribe(broker, host);
            }
        }, host.retry.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** The subscription of a virtual host closed, by the broker or a failure. */
    private void connectionLost(HostWatch host, long generation, String why) {
        BrokerWatch broker = watches.get(host.key.broker());
        if (broker == null || broker.hosts.get(host.name()) != host || host.generation != generation) {
            return;
        }
        host.subscription = null;
        failed(broker, host, new IOException("the broker closed Floci's connection: " + why));
        publish();
    }

    /**
     * The virtual host does not exist: Floci follows the broker's {@code vhost.created} events,
     * with the event exchange plugin enabled for it, and subscribes when the virtual host appears.
     */
    private void missing(BrokerWatch broker, HostWatch host) {
        close(host);
        if (host.state != State.MISSING) {
            LOG.infov("Virtual host {0} of broker {1} does not exist; Floci follows its queues once it does",
                    host.name(), broker.id());
        }
        host.state = State.MISSING;
        followVirtualHosts(broker);
    }

    private void followVirtualHosts(BrokerWatch broker) {
        if (broker.virtualHostEvents != null) {
            return;
        }
        try {
            if (!broker.eventExchange) {
                containers.enableEventExchange(broker.id());
                broker.eventExchange = true;
                LOG.infov("Floci enabled the plugin rabbitmq_event_exchange on broker {0}, which Amazon MQ does not "
                        + "offer, to learn when the missing virtual hosts of its workloads are created", broker.id());
            }
            if (!containers.grantWakeUser(broker.id(), ROOT, username)) {
                throw new IllegalStateException("the virtual host / does not exist");
            }
            broker.virtualHostEvents = client.followVirtualHosts(new Endpoint(broker.amqp, ROOT, username, password),
                    name -> post(() -> created(broker, name)),
                    why -> post(() -> virtualHostEventsLost(broker, why)));
            broker.eventsRetry = null;
            Set<String> existing = new LinkedHashSet<>();
            for (VirtualHost host : containers.virtualHosts(broker.id())) {
                existing.add(host.name());
            }
            for (HostWatch host : List.copyOf(broker.hosts.values())) {
                if (host.state == State.MISSING && existing.contains(host.name())) {
                    subscribe(broker, host);
                }
            }
        } catch (IOException | RefusedException | NoVirtualHostException | RuntimeException e) {
            closeVirtualHostEvents(broker);
            broker.eventsRetry = RetryBackoff.next(broker.eventsRetry);
            LOG.warnv("Following the virtual hosts of broker {0} failed; Floci tries again in {1} s: {2}", broker.id(),
                    broker.eventsRetry.toSeconds(), e.toString());
            control.schedule(() -> {
                if (watches.get(broker.key) == broker && broker.virtualHostEvents == null
                        && broker.hosts.values().stream().anyMatch(host -> host.state == State.MISSING)) {
                    followVirtualHosts(broker);
                    publish();
                }
            }, broker.eventsRetry.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private void created(BrokerWatch broker, String name) {
        if (watches.get(broker.key) != broker) {
            return;
        }
        HostWatch host = broker.hosts.get(name);
        if (host != null && host.state == State.MISSING) {
            subscribe(broker, host);
        }
    }

    private void virtualHostEventsLost(BrokerWatch broker, String why) {
        if (watches.get(broker.key) != broker || broker.virtualHostEvents == null) {
            return;
        }
        LOG.debugv("Floci's connection for the virtual hosts of broker {0} closed: {1}", broker.id(), why);
        broker.virtualHostEvents = null;
        if (broker.hosts.values().stream().anyMatch(host -> host.state == State.MISSING)) {
            followVirtualHosts(broker);
            publish();
        }
    }

    private void closeVirtualHostEventsWhenUnneeded(BrokerWatch broker) {
        if (broker.hosts.values().stream().noneMatch(host -> host.state == State.MISSING)) {
            closeVirtualHostEvents(broker);
        }
    }

    private static void closeVirtualHostEvents(BrokerWatch broker) {
        if (broker.virtualHostEvents != null) {
            broker.virtualHostEvents.close();
            broker.virtualHostEvents = null;
        }
    }

    /** Stops following a virtual host, and turns its firehose off where Floci turned it on. */
    private void unwatch(BrokerWatch broker, HostWatch host) {
        cancelRetry(host);
        close(host);
        broker.hosts.remove(host.name());
        if (host.tracingOurs && containers.running(broker.id())) {
            try {
                containers.traceOff(broker.id(), host.name());
                LOG.infov("Floci turned the firehose of virtual host {0} of broker {1} off again", host.name(),
                        broker.id());
            } catch (RuntimeException e) {
                LOG.warnv("Turning the firehose of virtual host {0} of broker {1} off failed: {2}", host.name(),
                        broker.id(), e.getMessage());
            }
        }
    }

    /** Stops following a broker, and deletes Floci's user there. */
    private void end(BrokerWatch broker) {
        for (HostWatch host : List.copyOf(broker.hosts.values())) {
            unwatch(broker, host);
        }
        closeVirtualHostEvents(broker);
        watches.remove(broker.key);
        if (broker.userReady && containers.running(broker.id())) {
            try {
                containers.deleteWakeUser(broker.id(), username);
            } catch (RuntimeException e) {
                LOG.warnv("Deleting Floci''s user {0} from broker {1} failed: {2}", username, broker.id(),
                        e.getMessage());
            }
        }
    }

    private void close(HostWatch host) {
        host.generation++;
        if (host.subscription != null) {
            host.subscription.close();
            host.subscription = null;
        }
    }

    private static void cancelRetry(HostWatch host) {
        if (host.retryTask != null) {
            host.retryTask.cancel(false);
            host.retryTask = null;
        }
    }

    /** Reads the queues of the workloads of a new subscription, or of one that lost a trace, once. */
    private void requestReads(HostWatch host) {
        long at = clock.millis();
        for (List<Target> targets : host.targets.values()) {
            for (Target target : targets) {
                target.activity().requestRead(at);
            }
        }
        loop.run();
    }

    private void publish() {
        Map<HostKey, State> next = new HashMap<>();
        for (BrokerWatch broker : watches.values()) {
            for (HostWatch host : broker.hosts.values()) {
                next.put(host.key, host.state);
            }
        }
        views = Map.copyOf(next);
        unwatched = Map.copyOf(unwatchable);
    }

    private void post(Runnable task) {
        try {
            control.execute(() -> {
                try {
                    task.run();
                } catch (RuntimeException e) {
                    LOG.errorv(e, "Following the RabbitMQ queues of the runtime file''s workloads failed");
                }
            });
        } catch (RejectedExecutionException e) {
            LOG.debugv("Floci stops, so it follows no RabbitMQ queue any more: {0}", e.getMessage());
        }
    }

    private void once(String key, Runnable log) {
        if (logged.add(key)) {
            log.run();
        }
    }

    private static HostKey hostKey(Workload workload, RabbitMqQueue queue) {
        return new HostKey(new BrokerKey(workload.accountId(), workload.region(), queue.brokerId()),
                queue.virtualHost());
    }

    private static String segment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static String random(int bytes) {
        byte[] value = new byte[bytes];
        new SecureRandom().nextBytes(value);
        return HexFormat.of().formatHex(value);
    }

    /** Hands a subscription's traces to the workloads that read its queues. */
    private final class HostHandler implements RabbitMqTraceClient.Handler {

        private final HostWatch host;
        private final long generation;

        HostHandler(HostWatch host, long generation) {
            this.host = host;
            this.generation = generation;
        }

        @Override
        public void trace(RabbitMqTrace trace) {
            Map<String, List<Target>> targets = host.targets;
            long now = clock.millis();
            boolean look = false;
            if (trace.kind() == RabbitMqTrace.Kind.PUBLISH) {
                for (String queue : trace.routedQueues()) {
                    look |= work(targets.get(queue), now);
                }
            } else {
                look = work(targets.get(trace.name()), now);
            }
            if (look) {
                loop.run();
            }
        }

        private static boolean work(List<Target> targets, long now) {
            boolean look = false;
            if (targets != null) {
                for (Target target : targets) {
                    look |= target.activity().work(now, target.resource());
                }
            }
            return look;
        }

        @Override
        public void lost() {
            LOG.debugv("The trace queue of virtual host {0} of broker {1} dropped a trace; Floci reads its queues",
                    host.name(), host.key.broker().brokerId());
            requestReads(host);
        }

        @Override
        public void closed(String why) {
            post(() -> connectionLost(host, generation, why));
        }
    }
}
