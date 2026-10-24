package io.github.hectorvent.floci.runtime.wakes;

import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager.ContainerPresence;
import io.github.hectorvent.floci.runtime.wakes.LeaseKeeper.Work;
import io.github.hectorvent.floci.runtime.wakes.RabbitMqTraceClient.Endpoint;
import io.github.hectorvent.floci.runtime.wakes.RabbitMqTraceClient.Handler;
import io.github.hectorvent.floci.runtime.wakes.RabbitMqTraceClient.RefusedException;
import io.github.hectorvent.floci.runtime.wakes.RabbitMqTraceClient.TraceSubscription;
import io.github.hectorvent.floci.runtime.wakes.WorkloadActivity.Found;
import io.github.hectorvent.floci.runtime.wakes.WorkloadActivity.Read;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.PodSelector;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.RabbitMqQueue;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Target;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Workload;
import io.github.hectorvent.floci.services.amazonmq.AmazonMqService;
import io.github.hectorvent.floci.services.amazonmq.container.RabbitMqManager;
import io.github.hectorvent.floci.services.amazonmq.container.RabbitMqManager.QueueCounts;
import io.github.hectorvent.floci.services.amazonmq.container.RabbitMqManager.VirtualHost;
import io.github.hectorvent.floci.services.amazonmq.container.RabbitMqManager.VirtualHostQueues;
import io.github.hectorvent.floci.services.amazonmq.model.Broker;
import io.github.hectorvent.floci.services.amazonmq.model.BrokerInstance;
import io.github.hectorvent.floci.services.amazonmq.model.BrokerState;
import io.github.hectorvent.floci.testing.MutableClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RabbitMqQueueWatchServiceTest {

    private static final String USER = "floci-wakes-test";
    private static final String PASSWORD = "secret";
    private static final String JOBS = "amqp://b-1/%2F/jobs";

    /** A subscription the fake client opened. */
    private record Opened(Endpoint endpoint, Set<String> keys, Handler handler, FakeSubscription subscription) {
    }

    private static final class FakeSubscription implements TraceSubscription {
        final List<String> bound = new CopyOnWriteArrayList<>();
        final List<String> unbound = new CopyOnWriteArrayList<>();
        volatile boolean closed;

        @Override
        public void bind(String key) {
            bound.add(key);
        }

        @Override
        public void unbind(String key) {
            unbound.add(key);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private final AmazonMqService brokers = mock(AmazonMqService.class);
    private final RabbitMqManager containers = mock(RabbitMqManager.class);
    private final RabbitMqTraceClient client = mock(RabbitMqTraceClient.class);
    private final MutableClock clock = new MutableClock();
    private final ScheduledExecutorService control = Executors.newSingleThreadScheduledExecutor();
    private final List<Opened> opened = new CopyOnWriteArrayList<>();
    private final AtomicInteger looks = new AtomicInteger();
    private final AtomicReference<Map<String, QueueCounts>> queues = new AtomicReference<>(Map.of());
    private final AtomicReference<Set<String>> down = new AtomicReference<>(Set.of());
    private final AtomicReference<RuntimeException> unreadable = new AtomicReference<>();
    private RabbitMqQueueWatch watch;

    @BeforeEach
    void setUp() throws Exception {
        Broker broker = new Broker();
        broker.setBrokerId("b-1");
        broker.setBrokerArn("arn:aws:mq:us-east-1:000000000000:broker:jobs:b-1");
        broker.setBrokerState(BrokerState.RUNNING);
        broker.setBrokerInstances(List.of(new BrokerInstance(
                "http://127.0.0.1:15672", List.of("amqp://127.0.0.1:5672"),
                "127.0.0.1")));
        when(brokers.describeBroker("b-1")).thenReturn(broker);
        when(containers.running("b-1")).thenReturn(true);
        when(containers.virtualHosts("b-1")).thenReturn(List.of(new VirtualHost("/", false),
                new VirtualHost("billing", true)));
        when(containers.grantWakeUser(eq("b-1"), anyString(), eq(USER))).thenReturn(true);
        when(client.subscribe(any(), any(), any())).thenAnswer(invocation -> {
            FakeSubscription subscription = new FakeSubscription();
            opened.add(new Opened(invocation.getArgument(0), Set.copyOf(invocation.<Set<String>>getArgument(1)),
                    invocation.getArgument(2), subscription));
            return subscription;
        });
        when(containers.queueCounts(eq("b-1"), anyString())).thenAnswer(invocation -> {
            RuntimeException failure = unreadable.get();
            if (failure != null) {
                throw failure;
            }
            return new VirtualHostQueues(queues.get(), down.get());
        });
        watch = new RabbitMqQueueWatch(brokers, containers, client, clock, control,
                Executors.newVirtualThreadPerTaskExecutor(), USER, PASSWORD);
        watch.start(looks::incrementAndGet);
    }

    @AfterEach
    void tearDown() {
        watch.stop();
    }

    @Test
    void aWatchedVirtualHostIsTracedAsFlocisOwnUser() throws Exception {
        WorkloadActivity jobs = activity("jobs", queue("/", "jobs"), queue("/", "reports"));

        watch.watch(List.of(jobs));

        await().until(() -> opened.size() == 1);
        Opened subscription = opened.getFirst();
        assertThat(subscription.endpoint(), is(new Endpoint(URI.create("amqp://127.0.0.1:5672"), "/", USER,
                PASSWORD)));
        assertThat(subscription.keys(), is(Set.of("deliver.jobs", "deliver.reports")));
        verify(containers).addWakeUser("b-1", USER, PASSWORD);
        verify(containers).grantWakeUser("b-1", "/", USER);
        verify(containers).traceOn("b-1", "/");
    }

    @Test
    void tracingThatIsOnAlreadyIsLeftOnAfterTheWatch() throws Exception {
        WorkloadActivity billing = activity("billing", queue("billing", "invoices"));
        watch.watch(List.of(billing));
        await().until(() -> opened.size() == 1);

        watch.watch(List.of());

        verify(containers, timeout(5000)).deleteWakeUser("b-1", USER);
        verify(containers, never()).traceOn(eq("b-1"), anyString());
        verify(containers, never()).traceOff(eq("b-1"), anyString());
        assertThat(opened.getFirst().subscription().closed, is(true));
    }

    @Test
    void tracingFlociTurnedOnIsTurnedOffWhenNoWorkloadWatchesTheVirtualHost() {
        WorkloadActivity jobs = activity("jobs", queue("/", "jobs"));
        watch.watch(List.of(jobs));
        await().until(() -> opened.size() == 1);

        watch.watch(List.of());

        verify(containers, timeout(5000)).traceOff("b-1", "/");
        verify(containers, timeout(5000)).deleteWakeUser("b-1", USER);
    }

    @Test
    void aPublishRoutedToAWatchedQueueIsWorkOfEveryWorkloadThatReadsIt() {
        WorkloadActivity jobs = activity("jobs", queue("/", "jobs"));
        WorkloadActivity audit = activity("audit", queue("/", "jobs"), queue("/", "audit"));
        WorkloadActivity reports = activity("reports", queue("/", "reports"));
        watch.watch(List.of(jobs, audit, reports));
        await().until(() -> opened.size() == 1);
        int before = looks.get();

        opened.getFirst().handler().trace(RabbitMqTrace.of("publish.orders", Map.of("routed_queues",
                List.of("jobs", "unwatched"))));

        assertThat(looks.get(), is(before + 1));
        jobs.turn(clock.millis(), (workload, ticket) -> Read.none(null));
        audit.turn(clock.millis(), (workload, ticket) -> Read.none(null));
        reports.turn(clock.millis(), (workload, ticket) -> Read.none(null));
        assertThat(jobs.demand().work(), is(Work.WAITING));
        assertThat(jobs.demand().resource(), is(JOBS));
        assertThat(audit.demand().work(), is(Work.WAITING));
        assertThat(reports.demand().work(), is(Work.NONE));
    }

    @Test
    void aDeliveryWakesAnIdleWorkloadAndIsActivityOfABusyOne() {
        WorkloadActivity jobs = activity("jobs", queue("/", "jobs"));
        watch.watch(List.of(jobs));
        await().until(() -> opened.size() == 1);
        Handler handler = opened.getFirst().handler();
        jobs.reset();

        handler.trace(RabbitMqTrace.of("deliver.jobs", Map.of()));
        long woken = clock.millis();
        jobs.turn(woken, (workload, ticket) -> Read.none(null));
        assertThat(jobs.demand().work(), is(Work.WAITING));

        clock.advance(Duration.ofSeconds(5));
        handler.trace(RabbitMqTrace.of("deliver.jobs", Map.of()));
        handler.trace(RabbitMqTrace.of("deliver.other", Map.of()));

        assertThat(jobs.due(clock.millis()) >= woken + 5_000 + 10_000, is(true));
    }

    @Test
    void namesThatCannotBeKeysBindEveryDeliveryAndMatchExactly() {
        String star = "jobs.*.retry";
        String longName = "q".repeat(250);
        assertThat(RabbitMqQueueWatch.deliverKeys(List.of("jobs", star)), is(Set.of("deliver.#")));
        assertThat(RabbitMqQueueWatch.deliverKeys(List.of(longName)), is(Set.of("deliver.#")));
        assertThat(RabbitMqQueueWatch.deliverKeys(List.of("jobs", "a.b")), is(Set.of("deliver.jobs", "deliver.a.b")));

        WorkloadActivity jobs = activity("jobs", queue("/", star));
        watch.watch(List.of(jobs));
        await().until(() -> opened.size() == 1);
        Handler handler = opened.getFirst().handler();
        int before = looks.get();

        handler.trace(RabbitMqTrace.of("deliver.jobs.a.retry", Map.of()));
        assertThat(looks.get(), is(before));

        handler.trace(RabbitMqTrace.of("deliver." + star, Map.of()));
        assertThat(looks.get(), is(before + 1));
    }

    @Test
    void aTraceOfTheDefaultExchangeAndAnyOtherMessageAreRead() {
        assertThat(RabbitMqTrace.of("publish.", Map.of("routed_queues", List.of("jobs"))),
                is(new RabbitMqTrace(RabbitMqTrace.Kind.PUBLISH, "", List.of("jobs"))));
        assertThat(RabbitMqTrace.of("publish.orders", null),
                is(new RabbitMqTrace(RabbitMqTrace.Kind.PUBLISH, "orders", List.of())));
        assertThat(RabbitMqTrace.of("deliver.jobs.v2", Map.of()),
                is(new RabbitMqTrace(RabbitMqTrace.Kind.DELIVER, "jobs.v2", List.of())));
        assertThat(RabbitMqTrace.of("something", Map.of()) == null, is(true));
    }

    @Test
    void aNewSubscriptionAndALostTraceReadTheQueuesOnce() {
        WorkloadActivity jobs = activity("jobs", queue("/", "jobs"));
        long start = clock.millis();
        watch.watch(List.of(jobs));
        await().until(() -> opened.size() == 1);

        long due = jobs.due(clock.millis());
        assertThat(due >= start && due <= clock.millis(), is(true));
        jobs.reset();

        int before = looks.get();
        opened.getFirst().handler().lost();
        assertThat(looks.get(), is(before + 1));
        assertThat(jobs.due(clock.millis()) <= clock.millis(), is(true));
    }

    @Test
    void aReloadBindsAddedQueuesAndUnbindsRemovedOnes() {
        WorkloadActivity jobs = activity("jobs", queue("/", "jobs"), queue("/", "reports"));
        watch.watch(List.of(jobs));
        await().until(() -> opened.size() == 1);

        jobs.define(activity("jobs", queue("/", "jobs"), queue("/", "audit")).workload());
        watch.watch(List.of(jobs));

        FakeSubscription subscription = opened.getFirst().subscription();
        await().until(() -> subscription.unbound.size() == 1);
        assertThat(subscription.bound, contains("deliver.audit"));
        assertThat(subscription.unbound, contains("deliver.reports"));
        assertThat(opened, hasSize(1));
    }

    @Test
    void aClosedConnectionSubscribesAgainWhileTheContainerIsThere() {
        WorkloadActivity jobs = activity("jobs", queue("/", "jobs"));
        watch.watch(List.of(jobs));
        await().until(() -> opened.size() == 1);
        when(containers.virtualHosts("b-1")).thenReturn(List.of(new VirtualHost("/", false)));

        opened.getFirst().handler().closed("CONNECTION_FORCED - broker forced connection closure");

        await().atMost(Duration.ofSeconds(5)).until(() -> opened.size() == 2);
        verify(containers, times(2)).traceOn("b-1", "/");
        verify(containers, times(1)).addWakeUser("b-1", USER, PASSWORD);
    }

    @Test
    void aClosedConnectionOfAGoneContainerEndsTheWatch() {
        WorkloadActivity jobs = activity("jobs", queue("/", "jobs"));
        watch.watch(List.of(jobs));
        await().until(() -> opened.size() == 1);
        when(containers.containerPresence("b-1")).thenReturn(ContainerPresence.ABSENT);

        opened.getFirst().handler().closed("connection reset");

        verify(containers, after(2500).times(1)).virtualHosts("b-1");
        assertThat(opened, hasSize(1));
        assertThat(read(jobs).found(), is(Found.NONE));
    }

    @Test
    void aStoppedContainerHasNoWorkAndIsSubscribedAgainOnceItRuns() {
        WorkloadActivity jobs = activity("jobs", queue("/", "jobs"));
        watch.watch(List.of(jobs));
        await().until(() -> opened.size() == 1);
        when(containers.containerPresence("b-1")).thenReturn(ContainerPresence.STOPPED);
        doThrow(new IllegalStateException("rabbitmqctl exited with 1")).when(containers)
                .addWakeUser("b-1", USER, PASSWORD);

        opened.getFirst().handler().closed("CONNECTION_FORCED - broker forced connection closure");

        verify(containers, timeout(5000).times(2)).addWakeUser("b-1", USER, PASSWORD);
        assertThat(read(jobs).found(), is(Found.NONE));
        assertThat(opened, hasSize(1));

        when(containers.containerPresence("b-1")).thenReturn(ContainerPresence.RUNNING);
        doNothing().when(containers).addWakeUser("b-1", USER, PASSWORD);

        await().atMost(Duration.ofSeconds(10)).until(() -> opened.size() == 2);
        queues.set(Map.of("jobs", new QueueCounts(1, 0)));
        assertThat(read(jobs), is(Read.held(JOBS, null)));
    }

    @Test
    void aQueueWithNoCountsFailsOnlyTheReadsOfItsWorkloads() {
        WorkloadActivity jobs = activity("jobs", queue("/", "jobs"));
        WorkloadActivity reports = activity("reports", queue("/", "reports"));
        watch.watch(List.of(jobs, reports));
        await().until(() -> opened.size() == 1);

        queues.set(Map.of("jobs", new QueueCounts(0, 0)));
        down.set(Set.of("reports"));

        assertThat(read(jobs), is(Read.none(null)));
        assertThat(read(reports), is(Read.failed()));
    }

    @Test
    void aRefusedUserIsNotRetriedUntilTheWorkloadsChange() throws Exception {
        doThrow(new RefusedException("ACCESS_REFUSED", null)).when(client).subscribe(any(), any(), any());
        WorkloadActivity jobs = activity("jobs", queue("/", "jobs"));
        watch.watch(List.of(jobs));
        verify(client, timeout(5000)).subscribe(any(), any(), any());

        verify(client, after(2500).times(1)).subscribe(any(), any(), any());
        assertThat(read(jobs).found(), is(Found.NONE));

        watch.watch(List.of(jobs));
        verify(client, timeout(5000).times(2)).subscribe(any(), any(), any());
    }

    @Test
    void aMissingVirtualHostIsSubscribedOnceItIsCreated() throws Exception {
        List<Consumer<String>> created = new CopyOnWriteArrayList<>();
        when(client.followVirtualHosts(any(), any(), any())).thenAnswer(invocation -> {
            created.add(invocation.getArgument(1));
            return (RabbitMqTraceClient.Subscription) () -> { };
        });
        WorkloadActivity later = activity("later", queue("later", "jobs"));
        watch.watch(List.of(later));

        await().until(() -> created.size() == 1);
        verify(containers).enableEventExchange("b-1");
        verify(containers).grantWakeUser("b-1", "/", USER);
        assertThat(opened, is(empty()));

        when(containers.virtualHosts("b-1")).thenReturn(List.of(new VirtualHost("/", false),
                new VirtualHost("later", false)));
        created.getFirst().accept("later");

        await().until(() -> opened.size() == 1);
        assertThat(opened.getFirst().endpoint().virtualHost(), is("later"));
    }

    @Test
    void aBrokerThatFlociDoesNotRunIsNotWatchedAndHasNoWork() {
        when(containers.running("b-1")).thenReturn(false);
        WorkloadActivity jobs = activity("jobs", queue("/", "jobs"));
        watch.watch(List.of(jobs));

        await().until(() -> read(jobs).found() == Found.NONE);
        assertThat(opened, is(empty()));
    }

    @Test
    void aMissingBrokerHasNoWork() {
        when(brokers.describeBroker("b-1")).thenThrow(new AwsException("NotFoundException", "no", 404));
        WorkloadActivity jobs = activity("jobs", queue("/", "jobs"));
        watch.watch(List.of(jobs));

        await().until(() -> read(jobs).found() == Found.NONE);
    }

    @Test
    void theDecisionReadCountsReadyAndUnacknowledgedMessages() {
        WorkloadActivity jobs = activity("jobs", queue("/", "jobs"), queue("/", "reports"));
        watch.watch(List.of(jobs));
        await().until(() -> opened.size() == 1);

        queues.set(Map.of("jobs", new QueueCounts(0, 0), "reports", new QueueCounts(2, 1)));
        assertThat(read(jobs), is(Read.held("amqp://b-1/%2F/reports", null)));

        queues.set(Map.of("jobs", new QueueCounts(0, 1), "reports", new QueueCounts(0, 0)));
        assertThat(read(jobs), is(Read.held(JOBS, null)));

        queues.set(Map.of("jobs", new QueueCounts(0, 0)));
        assertThat(read(jobs), is(Read.none(null)));

        queues.set(Map.of());
        assertThat(read(jobs), is(Read.none(null)));

        unreadable.set(new IllegalStateException("list_queues exited with 69"));
        assertThat(read(jobs), is(Read.failed()));

        verify(containers, atLeast(5)).queueCounts("b-1", "/");
    }

    @Test
    void aReadAnswersPendingAndGivesItsAnswerForTheSameTicket() {
        WorkloadActivity jobs = activity("jobs", queue("/", "jobs"));
        watch.watch(List.of(jobs));
        await().until(() -> opened.size() == 1);
        queues.set(Map.of("jobs", new QueueCounts(1, 0)));
        int before = looks.get();

        assertThat(watch.read(jobs.workload(), 7).found(), is(Found.PENDING));
        await().until(() -> looks.get() > before);

        assertThat(watch.read(jobs.workload(), 7), is(Read.held(JOBS, null)));
        assertThat(watch.read(jobs.workload(), 8).found(), is(Found.PENDING));
    }

    @Test
    void aVirtualHostThatIsNotSubscribedYetCannotBeRead() {
        when(containers.virtualHosts("b-1")).thenAnswer(invocation -> {
            Thread.sleep(1000);
            return List.of(new VirtualHost("/", false));
        });
        WorkloadActivity jobs = activity("jobs", queue("/", "jobs"));
        watch.watch(List.of(jobs));

        assertThat(read(jobs).found(), is(Found.FAILED));
    }

    /** One read, waited for: the first answers pending, and its answer comes for the same ticket. */
    private Read read(WorkloadActivity activity) {
        long ticket = System.nanoTime();
        AtomicReference<Read> answer = new AtomicReference<>(watch.read(activity.workload(), ticket));
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            if (answer.get().found() == Found.PENDING) {
                answer.set(watch.read(activity.workload(), ticket));
            }
            return answer.get().found() != Found.PENDING;
        });
        return answer.get();
    }

    private static RabbitMqQueue queue(String virtualHost, String name) {
        return new RabbitMqQueue("b-1", virtualHost, name, null, null);
    }

    private static WorkloadActivity activity(String name, RabbitMqQueue... queues) {
        List<RabbitMqQueue> list = new ArrayList<>(List.of(queues));
        return new WorkloadActivity(new Workload(name, true, "000000000000", "us-east-1", Duration.ofSeconds(10),
                Duration.ofSeconds(30), List.of(), list,
                new Target("node:dev", "shop", PodSelector.Controller.deployment(name))));
    }
}
