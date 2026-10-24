package io.github.hectorvent.floci.runtime.wakes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.runtime.RetryBackoff;
import io.github.hectorvent.floci.runtime.node.FakeNodeApi;
import io.github.hectorvent.floci.runtime.node.NodeEvent;
import io.github.hectorvent.floci.runtime.node.NodeToken;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import io.github.hectorvent.floci.runtime.wakes.LeaseKeeper.Demand;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.PodSelector;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Target;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Workload;
import io.github.hectorvent.floci.testing.MutableClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

class LeaseKeeperServiceTest {

    private static final String TOKEN = "0123456789abcdef0123456789abcdef";
    private static final String QUEUE = "arn:aws:sqs:us-east-1:000000000000:orders-incoming";

    private final MutableClock clock = new MutableClock();
    private final ManualExecutor wakes = new ManualExecutor();
    private final SmolvmNodeClient client = new SmolvmNodeClient(new ObjectMapper());
    private FakeNodeApi fake;
    private SmolvmNode node;
    private LeaseKeeper keeper;
    private long cursor;

    private final Workload orders = workload("orders", PodSelector.Controller.deployment("orders"));

    @BeforeEach
    void startFake() throws IOException {
        fake = new FakeNodeApi(TOKEN);
        node = fake.node("dev", new NodeToken.TokenVariable("NODE_TOKEN", name -> TOKEN));
        PodView pods = new PodView(name -> name.equals(node.name()) ? Optional.of(node) : Optional.empty(), client,
                clock);
        keeper = new LeaseKeeper(client, pods, clock, wakes);
    }

    @AfterEach
    void stopFake() {
        fake.close();
    }

    @Test
    void messagesLeaseEveryPodOfTheWorkloadAndWakeIt() {
        pod("sb-1", "orders", "asleep");
        pod("sb-2", "orders", "awake");
        pod("sb-jobs", "jobs", "asleep");

        keeper.tick(null, List.of(Demand.waiting(orders, QUEUE)));

        assertThat(leases("sb-1"), contains("floci:orders"));
        assertThat(leases("sb-2"), contains("floci:orders"));
        assertThat(leases("sb-jobs"), is(empty()));
        JsonNode put = only("PUT", "/v1/pods/sb-1/leases/floci%3Aorders").body();
        assertThat(put.path("ttl_secs").asInt(), is(60));
        assertThat(put.path("holder").asText(), is("floci wakes: orders"));
        assertThat(put.path("incarnation").asText(), is(fake.pod("sb-1").path("incarnation").asText()));
        assertThat(wakes.pending(), is(2));

        wakes.runAll();

        JsonNode wake = only("POST", "/v1/pods/sb-1/wake").body();
        assertThat(wake.path("reason").path("kind").asText(), is("aws"));
        assertThat(wake.path("reason").path("queue").asText(), is(QUEUE));
        assertThat(wake.path("source").asText(), is("floci"));
        assertThat(wake.path("incarnation").asText(), is(fake.pod("sb-1").path("incarnation").asText()));
        assertThat(fake.pod("sb-1").path("state").asText(), is("awake"));
        assertThat(calls("POST", "/v1/pods/sb-jobs/wake"), is(0));
    }

    @Test
    void aWokenPodIsNotWokenAgainWhileItsLeaseLasts() {
        pod("sb-1", "orders", "asleep");

        keeper.tick(null, List.of(Demand.waiting(orders, QUEUE)));
        wakes.runAll();
        for (int i = 0; i < 5; i++) {
            clock.advance(Duration.ofSeconds(3));
            keeper.tick(null, List.of(Demand.waiting(orders, QUEUE)));
            wakes.runAll();
        }

        assertThat(calls("PUT", "/v1/pods/sb-1/leases/"), is(1));
        assertThat(calls("POST", "/v1/pods/sb-1/wake"), is(1));
    }

    @Test
    void aCreatingPodIsLeasedThenWokenOnceItRuns() {
        pod("sb-1", "orders", "awake");
        fake.state("sb-1", "creating");

        keeper.tick(null, List.of(Demand.waiting(orders, QUEUE)));
        skipEvents();

        assertThat(leases("sb-1"), contains("floci:orders"));
        assertThat(wakes.pending(), is(0));

        keeper.tick(null, List.of(Demand.waiting(orders, QUEUE)));
        assertThat(wakes.pending(), is(0));

        fake.state("sb-1", "awake");
        events();
        keeper.tick(null, List.of(Demand.waiting(orders, QUEUE)));
        wakes.runAll();

        assertThat(calls("POST", "/v1/pods/sb-1/wake"), is(1));
        assertThat(calls("PUT", "/v1/pods/sb-1/leases/"), is(1));
    }

    @Test
    void aWakeRefusedForACreatingPodWaitsForItToRun() {
        pod("sb-1", "orders", "asleep");
        fake.failNext("POST", "/v1/pods/sb-1/wake", 409, "refused", "pod sandbox sb-1 is creating", "state creating");

        tick(Demand.waiting(orders, QUEUE));
        skipEvents();
        clock.advance(Duration.ofSeconds(2));
        tick(Demand.waiting(orders, QUEUE));

        assertThat(calls("POST", "/v1/pods/sb-1/wake"), is(1));

        fake.state("sb-1", "awake");
        events();
        tick(Demand.waiting(orders, QUEUE));

        assertThat(calls("POST", "/v1/pods/sb-1/wake"), is(2));
    }

    @Test
    void leasesAreRenewedEveryTwentySeconds() {
        pod("sb-1", "orders", "asleep");
        tick(Demand.waiting(orders, QUEUE));

        clock.advance(Duration.ofSeconds(19));
        tick(Demand.waiting(orders, QUEUE));
        assertThat(calls("PATCH", "/v1/pods/sb-1/leases/floci%3Aorders"), is(0));

        clock.advance(Duration.ofSeconds(1));
        tick(Demand.waiting(orders, QUEUE));
        assertThat(only("PATCH", "/v1/pods/sb-1/leases/floci%3Aorders").body().path("ttl_secs").asInt(), is(60));
    }

    @Test
    void anIdleWorkloadReleasesItsLeasesAtOnce() {
        pod("sb-1", "orders", "asleep");
        tick(Demand.waiting(orders, QUEUE));

        clock.advance(Duration.ofMillis(500));
        tick(Demand.none(orders));

        assertThat(leases("sb-1"), is(empty()));
        assertThat(calls("DELETE", "/v1/pods/sb-1/leases/floci%3Aorders"), is(1));
        assertThat(keeper.heldOn("sb-1"), is(empty()));
        assertThat(keeper.nextDue(), is(Optional.empty()));
    }

    @Test
    void theNextTickIsDueAtTheFirstRenewal() {
        assertThat(keeper.nextDue(), is(Optional.empty()));
        pod("sb-1", "orders", "asleep");
        Instant held = clock.instant();

        tick(Demand.waiting(orders, QUEUE));

        assertDue(held.plus(LeaseKeeper.RENEW_INTERVAL));

        clock.advance(LeaseKeeper.RENEW_INTERVAL);
        tick(Demand.waiting(orders, QUEUE));

        assertDue(clock.instant().plus(LeaseKeeper.RENEW_INTERVAL));
    }

    @Test
    void theNextTickIsDueWhenAFailedCallIsTriedAgain() {
        pod("sb-1", "orders", "asleep");
        fake.failNext("PUT", "/v1/pods/sb-1/leases/", 500, "internal", "the disk is full");

        keeper.tick(null, List.of(Demand.waiting(orders, QUEUE)));

        assertDue(clock.instant().plus(RetryBackoff.FIRST));

        clock.advance(RetryBackoff.FIRST);
        tick(Demand.waiting(orders, QUEUE));

        assertThat(leases("sb-1"), contains("floci:orders"));
        assertDue(clock.instant().plus(LeaseKeeper.RENEW_INTERVAL));
    }

    @Test
    void theNextTickIsDueWhenTheWaitForRoomEnds() {
        pod("sb-1", "orders", "asleep");
        fake.failNext("POST", "/v1/pods/sb-1/wake", 409, "refused", "wake refused: no room", "wake refused");

        tick(Demand.waiting(orders, QUEUE));

        Instant refused = clock.instant();
        clock.advance(Duration.ofSeconds(290));
        tick(Demand.waiting(orders, QUEUE));
        assertDue(refused.plus(LeaseKeeper.ROOM_WAIT));
    }

    @Test
    void anUnreadableQueueKeepsTheLeasesAndWakesNothingNew() {
        pod("sb-1", "orders", "asleep");
        tick(Demand.waiting(orders, QUEUE));

        pod("sb-2", "orders", "asleep");
        for (int i = 0; i < 30; i++) {
            clock.advance(Duration.ofSeconds(10));
            tick(Demand.unknown(orders));
        }

        assertThat(leases("sb-1"), contains("floci:orders"));
        assertThat(leases("sb-2"), is(empty()));
        assertThat(calls("POST", "/v1/pods/sb-2/wake"), is(0));
    }

    @Test
    void aWorkloadThatLeavesReleasesItsLeases() {
        pod("sb-1", "orders", "asleep");
        tick(Demand.waiting(orders, QUEUE));

        tick();

        assertThat(leases("sb-1"), is(empty()));
    }

    @Test
    void releaseAllDeletesEveryLease() {
        pod("sb-1", "orders", "asleep");
        tick(Demand.waiting(orders, QUEUE));

        keeper.releaseAll();

        assertThat(leases("sb-1"), is(empty()));
        assertThat(keeper.heldOn("sb-1"), is(empty()));
    }

    @Test
    void anExpiredLeaseIsHeldAgainAndThePodWoken() {
        pod("sb-1", "orders", "asleep");
        tick(Demand.waiting(orders, QUEUE));
        fake.failNext("PATCH", "/v1/pods/sb-1/leases/", 404, "lease_not_found", "pod sandbox sb-1 has no lease");

        clock.advance(Duration.ofSeconds(20));
        tick(Demand.waiting(orders, QUEUE));

        assertThat(calls("PUT", "/v1/pods/sb-1/leases/floci%3Aorders"), is(2));
        assertThat(calls("POST", "/v1/pods/sb-1/wake"), is(2));
    }

    @Test
    void anExpiredLeaseThatIsOnlyKeptIsHeldAgainWithoutAWake() {
        pod("sb-1", "orders", "asleep");
        tick(Demand.waiting(orders, QUEUE));
        fake.failNext("PATCH", "/v1/pods/sb-1/leases/", 404, "lease_not_found", "pod sandbox sb-1 has no lease");

        clock.advance(Duration.ofSeconds(20));
        tick(Demand.unknown(orders));

        assertThat(calls("PUT", "/v1/pods/sb-1/leases/floci%3Aorders"), is(2));
        assertThat(calls("POST", "/v1/pods/sb-1/wake"), is(1));
    }

    @Test
    void onlyOneWakeAtATimeReachesAPod() {
        Workload labelled = workload("orders-labelled", new PodSelector.Labels(Map.of("app", "orders")));
        pod("sb-1", "orders", "asleep");

        keeper.tick(null, List.of(Demand.waiting(orders, QUEUE), Demand.waiting(labelled, QUEUE)));
        keeper.tick(null, List.of(Demand.waiting(orders, QUEUE), Demand.waiting(labelled, QUEUE)));

        assertThat(leases("sb-1"), containsInAnyOrder("floci:orders", "floci:orders-labelled"));
        assertThat(wakes.pending(), is(1));

        wakes.runAll();
        keeper.tick(null, List.of(Demand.waiting(orders, QUEUE), Demand.waiting(labelled, QUEUE)));

        assertThat(wakes.pending(), is(0));
        assertThat(calls("POST", "/v1/pods/sb-1/wake"), is(1));
    }

    @Test
    void anAdmissionRefusalHoldsWakesOnTheNodeUntilAPodStopsUsingRoom() {
        pod("sb-1", "orders", "asleep");
        pod("sb-2", "orders", "asleep");
        fake.addPod("sb-busy", "other", "busy");
        fake.failNext("POST", "/v1/pods/sb-1/wake", 409, "refused",
                "wake refused: 4 pods are awake, the most allowed", "wake refused");

        tick(Demand.waiting(orders, QUEUE));
        skipEvents();
        tick(Demand.waiting(orders, QUEUE));

        assertThat(keeper.waitingForRoom(node.name()), is(true));
        assertThat(leases("sb-1"), contains("floci:orders"));
        assertThat(fake.pod("sb-2").path("state").asText(), is("awake"));

        pod("sb-3", "orders", "asleep");
        skipEvents();
        keeper.followStarted(node.name());
        clock.advance(Duration.ofSeconds(2));
        tick(Demand.waiting(orders, QUEUE));
        clock.advance(Duration.ofSeconds(25));
        tick(Demand.waiting(orders, QUEUE));

        assertThat(leases("sb-3"), contains("floci:orders"));
        assertThat(calls("POST", "/v1/pods/sb-1/wake"), is(1));
        assertThat(calls("POST", "/v1/pods/sb-3/wake"), is(0));
        assertThat(calls("PATCH", "/v1/pods/sb-1/leases/"), is(1));

        fake.state("sb-busy", "pausing");
        tick(Demand.waiting(orders, QUEUE));
        assertThat(calls("POST", "/v1/pods/sb-1/wake"), is(1));

        fake.state("sb-busy", "asleep");
        events();
        tick(Demand.waiting(orders, QUEUE));

        assertThat(calls("POST", "/v1/pods/sb-1/wake"), is(2));
        assertThat(calls("POST", "/v1/pods/sb-3/wake"), is(0));

        tick(Demand.waiting(orders, QUEUE));
        tick(Demand.waiting(orders, QUEUE));

        assertThat(calls("POST", "/v1/pods/sb-3/wake"), is(1));
        assertThat(fake.pod("sb-3").path("state").asText(), is("awake"));
        assertThat(keeper.waitingForRoom(node.name()), is(false));
    }

    @Test
    void aRefusedWakeIsTriedAgainAfterFiveMinutes() {
        pod("sb-1", "orders", "asleep");
        fake.addPod("sb-busy", "other", "busy");
        fake.failNext("POST", "/v1/pods/sb-1/wake", 409, "refused", "wake refused: no room", "wake refused");
        tick(Demand.waiting(orders, QUEUE));
        tick(Demand.waiting(orders, QUEUE));

        for (int i = 0; i < 29; i++) {
            clock.advance(Duration.ofSeconds(10));
            tick(Demand.waiting(orders, QUEUE));
        }
        assertThat(calls("POST", "/v1/pods/sb-1/wake"), is(1));

        clock.advance(Duration.ofSeconds(10));
        tick(Demand.waiting(orders, QUEUE));
        tick(Demand.waiting(orders, QUEUE));

        assertThat(calls("POST", "/v1/pods/sb-1/wake"), is(2));
        assertThat(fake.pod("sb-1").path("state").asText(), is("awake"));
        assertThat(keeper.waitingForRoom(node.name()), is(false));
    }

    @Test
    void aFailedPodIsForgotten() {
        pod("sb-1", "orders", "asleep");
        fake.failNext("POST", "/v1/pods/sb-1/wake", 409, "failed", "pod sandbox sb-1 failed");
        keeper.tick(null, List.of(Demand.waiting(orders, QUEUE)));
        fake.state("sb-1", "failed");
        wakes.runAll();

        keeper.tick(null, List.of(Demand.waiting(orders, QUEUE)));
        clock.advance(Duration.ofSeconds(30));
        tick(Demand.waiting(orders, QUEUE));

        assertThat(keeper.heldOn("sb-1"), is(empty()));
        assertThat(calls("PUT", "/v1/pods/sb-1/leases/"), is(1));
        assertThat(calls("PATCH", "/v1/pods/sb-1/leases/"), is(0));
    }

    @Test
    void aNodeThatReplacesOneOfTheSameNameIsLeasedAndWokenAgain() throws IOException {
        pod("sb-1", "orders", "asleep");
        tick(Demand.waiting(orders, QUEUE));
        String incarnation = fake.pod("sb-1").path("incarnation").asText();
        FakeNodeApi replaced = fake;
        try {
            fake = new FakeNodeApi(TOKEN);
            node = fake.node("dev", new NodeToken.TokenVariable("NODE_TOKEN", name -> TOKEN));
            ObjectNode pod = pod("sb-1", "orders", "asleep");
            synchronized (fake) {
                pod.put("incarnation", incarnation);
            }

            keeper.followStarted(node.name());
            tick(Demand.waiting(orders, QUEUE));

            assertThat(calls("PUT", "/v1/pods/sb-1/leases/"), is(1));
            assertThat(calls("PATCH", "/v1/pods/sb-1/leases/"), is(0));
            assertThat(calls("POST", "/v1/pods/sb-1/wake"), is(1));
            assertThat(fake.pod("sb-1").path("state").asText(), is("awake"));
        } finally {
            replaced.close();
        }
    }

    @Test
    void aStaleIncarnationListsTheNamespaceAgain() {
        pod("sb-1", "orders", "asleep");
        fake.failNext("PUT", "/v1/pods/sb-1/leases/", 409, "stale_incarnation", "another incarnation");

        keeper.tick(null, List.of(Demand.waiting(orders, QUEUE)));
        keeper.tick(null, List.of(Demand.waiting(orders, QUEUE)));

        assertThat(calls("GET", "/v1/pods"), is(2));
        assertThat(leases("sb-1"), contains("floci:orders"));
    }

    @Test
    void aNodeThatRefusesTheTokenIsCalledAgainAfterABackoff() {
        pod("sb-1", "orders", "asleep");
        fake.token("another-token-0123456789");

        keeper.tick(null, List.of(Demand.waiting(orders, QUEUE)));
        clock.advance(Duration.ofMillis(500));
        keeper.tick(null, List.of(Demand.waiting(orders, QUEUE)));
        assertThat(calls("GET", "/v1/pods"), is(1));

        clock.advance(Duration.ofMillis(500));
        keeper.tick(null, List.of(Demand.waiting(orders, QUEUE)));
        clock.advance(Duration.ofMillis(1500));
        keeper.tick(null, List.of(Demand.waiting(orders, QUEUE)));
        assertThat(calls("GET", "/v1/pods"), is(2));

        clock.advance(Duration.ofMillis(500));
        keeper.tick(null, List.of(Demand.waiting(orders, QUEUE)));
        assertThat(calls("GET", "/v1/pods"), is(3));

        fake.token(TOKEN);
        clock.advance(Duration.ofSeconds(4));
        tick(Demand.waiting(orders, QUEUE));

        assertThat(leases("sb-1"), contains("floci:orders"));
        assertThat(fake.pod("sb-1").path("state").asText(), is("awake"));
    }

    @Test
    void anUnreachableNodeLeavesTheOtherNodesAlone() throws IOException {
        FakeNodeApi gone = new FakeNodeApi(TOKEN);
        SmolvmNode down = gone.node("down", new NodeToken.TokenVariable("NODE_TOKEN", name -> TOKEN));
        gone.close();
        PodView pods = new PodView(name -> Optional.ofNullable(Map.of(node.name(), node, down.name(), down).get(name)),
                client, clock);
        keeper = new LeaseKeeper(client, pods, clock, wakes);
        Workload elsewhere = new Workload("elsewhere", true, "000000000000", "us-east-1", Duration.ofSeconds(120),
                Duration.ofSeconds(30), List.of("jobs"), List.of(),
                new Target(down.name(), "shop", PodSelector.Controller.deployment("jobs")));
        pod("sb-1", "orders", "asleep");

        keeper.tick(null, List.of(Demand.waiting(elsewhere, QUEUE), Demand.waiting(orders, QUEUE)));
        wakes.runAll();

        assertThat(leases("sb-1"), contains("floci:orders"));
        assertThat(fake.pod("sb-1").path("state").asText(), is("awake"));
    }

    @Test
    void staleLeasesOfThisFlociAreDeleted() {
        pod("sb-1", "orders", "asleep");
        client.putLease(node, "sb-1", "floci:removed-workload", 60, "floci wakes: removed-workload", null);
        client.putLease(node, "sb-1", "floci:laptop:orders", 60, "floci wakes: orders", null);
        client.putLease(node, "sb-1", "debugging", 60, "elliot", null);

        tick(Demand.waiting(orders, QUEUE));

        assertThat(leases("sb-1"), containsInAnyOrder("floci:orders", "floci:laptop:orders", "debugging"));
        assertThat(calls("DELETE", "/v1/pods/sb-1/leases/floci%3Aremoved-workload"), is(1));
    }

    @Test
    void anInstanceKeepsToItsOwnLeases() {
        pod("sb-1", "orders", "asleep");
        client.putLease(node, "sb-1", "floci:orders", 60, "floci wakes: orders", null);
        client.putLease(node, "sb-1", "floci:laptop:removed", 60, "floci wakes: removed", null);
        client.putLease(node, "sb-1", "floci:laptop-2:orders", 60, "floci wakes: orders", null);

        keeper.tick("laptop", List.of(Demand.waiting(orders, QUEUE)));

        assertThat(leases("sb-1"), containsInAnyOrder("floci:orders", "floci:laptop:orders",
                "floci:laptop-2:orders"));
    }

    @Test
    void aLeaseIdNamesTheWorkloadAndTheInstance() {
        assertThat(LeaseKeeper.leaseId(null, "orders"), is("floci:orders"));
        assertThat(LeaseKeeper.leaseId("laptop", "orders.v2"), is("floci:laptop:orders.v2"));
    }

    @Test
    void aNewInstanceIdMovesTheLeases() {
        pod("sb-1", "orders", "asleep");
        tick(Demand.waiting(orders, QUEUE));

        keeper.tick("laptop", List.of(Demand.waiting(orders, QUEUE)));

        assertThat(leases("sb-1"), contains("floci:laptop:orders"));
        assertThat(keeper.heldOn("sb-1"), contains("floci:laptop:orders"));
    }

    @Test
    void aPodThatLeavesTheWorkloadLosesItsLease() {
        pod("sb-1", "orders", "asleep");
        tick(Demand.waiting(orders, QUEUE));

        assertThat(leases("sb-1"), contains("floci:orders"));
        skipEvents();
        fake.removePod("sb-1");
        events();
        tick(Demand.waiting(orders, QUEUE));

        assertThat(keeper.heldOn("sb-1"), is(empty()));
    }

    @Test
    void aWakeTickListsOnlyWhenTheWorkloadBeginsToWake() {
        pod("sb-1", "orders", "asleep");

        tick(Demand.waiting(orders, QUEUE));
        for (int i = 0; i < 5; i++) {
            clock.advance(Duration.ofSeconds(5));
            tick(Demand.waiting(orders, QUEUE));
        }

        assertThat(calls("GET", "/v1/pods"), is(1));

        clock.advance(Duration.ofSeconds(5));
        tick(Demand.none(orders));
        tick(Demand.waiting(orders, QUEUE));

        assertThat(calls("GET", "/v1/pods"), is(2));
    }

    @Test
    void aNewTargetOfABusyWorkloadLeasesItsPodsAndReleasesTheOldOnes() {
        pod("sb-1", "orders", "asleep");
        pod("sb-2", "billing", "asleep");
        tick(Demand.waiting(orders, QUEUE));
        assertThat(leases("sb-1"), contains("floci:orders"));

        Workload retargeted = workload("orders", PodSelector.Controller.deployment("billing"));
        tick(Demand.waiting(retargeted, QUEUE));

        assertThat(leases("sb-1"), is(empty()));
        assertThat(leases("sb-2"), contains("floci:orders"));
        assertThat(fake.pod("sb-2").path("state").asText(), is("awake"));
        int listings = calls("GET", "/v1/pods");

        clock.advance(Duration.ofSeconds(5));
        tick(Demand.waiting(retargeted, QUEUE));
        assertThat(calls("GET", "/v1/pods"), is(listings));
    }

    @Test
    void aPodCreatedWhileTheWorkloadWakesIsLeasedAndWokenFromItsEvent() {
        pod("sb-1", "orders", "asleep");
        tick(Demand.waiting(orders, QUEUE));
        skipEvents();

        pod("sb-2", "orders", "asleep");
        tick(Demand.waiting(orders, QUEUE));
        assertThat(leases("sb-2"), is(empty()));

        events();
        tick(Demand.waiting(orders, QUEUE));

        assertThat(leases("sb-2"), contains("floci:orders"));
        assertThat(fake.pod("sb-2").path("state").asText(), is("awake"));
        assertThat(calls("GET", "/v1/pods"), is(2));
    }

    @Test
    void eventsOfListedPodsListNothing() {
        pod("sb-1", "orders", "asleep");
        tick(Demand.waiting(orders, QUEUE));
        skipEvents();

        fake.state("sb-1", "pausing");
        fake.state("sb-1", "asleep");
        events();
        tick(Demand.waiting(orders, QUEUE));

        assertThat(calls("GET", "/v1/pods"), is(1));
    }

    @Test
    void aFollowThatStartsListsTheNodeAgain() {
        pod("sb-1", "orders", "asleep");
        tick(Demand.waiting(orders, QUEUE));
        pod("sb-2", "orders", "asleep");

        keeper.followStarted(node.name());
        tick(Demand.waiting(orders, QUEUE));

        assertThat(calls("GET", "/v1/pods"), is(2));
        assertThat(leases("sb-2"), contains("floci:orders"));
    }

    @Test
    void aWakeAnswerAndANodeEventTellTheListener() {
        int[] told = new int[1];
        keeper.onChange(() -> told[0]++);
        pod("sb-1", "orders", "asleep");

        keeper.tick(null, List.of(Demand.waiting(orders, QUEUE)));
        wakes.runAll();
        assertThat(told[0], is(1));

        keeper.followStarted(node.name());
        assertThat(told[0], is(2));
    }

    /** The next tick is due at {@code expected}, give or take the milliseconds the test clock adds on each read. */
    private void assertDue(Instant expected) {
        Instant due = keeper.nextDue().orElseThrow();
        assertThat(Duration.between(expected, due).abs().toMillis() < 20, is(true));
    }

    /** Hands the keeper the node's events since the last call. */
    private void events() {
        SmolvmNodeClient.EventPage page = client.events(node, cursor, 500, null);
        for (NodeEvent event : page.events()) {
            keeper.nodeEvent(node.name(), event);
        }
        cursor = page.cursor();
    }

    /** Skips the node's events so far, as a follow that starts now does. */
    private void skipEvents() {
        cursor = client.eventsHead(node);
    }

    /** One tick with no instance id, its wakes run, and their answers applied by a second tick. */
    private void tick(Demand... demands) {
        keeper.tick(null, List.of(demands));
        wakes.runAll();
        keeper.tick(null, List.of(demands));
    }

    private ObjectNode pod(String sandbox, String deployment, String state) {
        ObjectNode pod = fake.addPod(sandbox, "shop", deployment + "-" + sandbox);
        pod.putObject("labels").put("app", deployment);
        pod.putObject("controller").put("kind", "Deployment").put("name", deployment);
        if (!state.equals("awake")) {
            fake.state(sandbox, state);
        }
        return pod;
    }

    private List<String> leases(String sandbox) {
        List<String> ids = new ArrayList<>();
        for (JsonNode lease : fake.pod(sandbox).path("leases")) {
            ids.add(lease.path("id").asText());
        }
        return ids;
    }

    private int calls(String method, String pathPrefix) {
        return (int) fake.requests().stream()
                .filter(request -> request.method().equals(method) && request.path().startsWith(pathPrefix))
                .count();
    }

    private FakeNodeApi.Request only(String method, String path) {
        List<FakeNodeApi.Request> matching = fake.requests().stream()
                .filter(request -> request.method().equals(method) && request.path().equals(path))
                .toList();
        assertThat(matching, hasSize(1));
        return matching.getFirst();
    }

    private static Workload workload(String name, PodSelector pods) {
        return new Workload(name, true, "000000000000", "us-east-1", Duration.ofSeconds(120), Duration.ofSeconds(30),
                List.of("orders-incoming"), List.of(), new Target("node:dev", "shop", pods));
    }

    /** Holds each wake until the test runs it. */
    private static final class ManualExecutor implements Executor {

        private final List<Runnable> tasks = new ArrayList<>();

        @Override
        public synchronized void execute(Runnable task) {
            tasks.add(task);
        }

        synchronized int pending() {
            return tasks.size();
        }

        void runAll() {
            List<Runnable> run;
            synchronized (this) {
                run = List.copyOf(tasks);
                tasks.clear();
            }
            run.forEach(Runnable::run);
        }
    }
}
