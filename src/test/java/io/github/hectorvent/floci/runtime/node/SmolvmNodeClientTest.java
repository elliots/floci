package io.github.hectorvent.floci.runtime.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.testutil.LogCapture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogRecord;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SmolvmNodeClientTest {

    private static final String TOKEN = "first-token-0123456789abcdef";
    private static final String NEW_TOKEN = "second-token-0123456789abcdef";
    private static final String QUEUE = "arn:aws:sqs:us-east-1:000000000000:orders";

    @TempDir
    Path dir;

    private final ObjectMapper mapper = new ObjectMapper();
    private FakeNodeApi fake;
    private Path tokenFile;
    private SmolvmNode node;
    private SmolvmNodeClient client;

    @BeforeEach
    void start() throws IOException {
        fake = new FakeNodeApi(TOKEN);
        tokenFile = dir.resolve("node-token");
        writeToken(TOKEN);
        node = fake.node("dev", NodeToken.file(tokenFile));
        client = new SmolvmNodeClient(mapper);
    }

    @AfterEach
    void stop() {
        client.close();
        fake.close();
    }

    @Test
    void listsPodsWithTheirLabelsAndController() {
        ObjectNode orders = fake.addPod("sandbox-orders", "shop", "orders-7d9f-abcde");
        orders.putObject("labels").put("app", "orders").put("pod-template-hash", "7d9f");
        orders.putObject("controller").put("kind", "Deployment").put("name", "orders");
        fake.addPod("sandbox-dns", "kube-system", "coredns");

        List<NodePod> shop = client.listPods(node, SmolvmNodeClient.PodQuery.namespace("shop"));
        List<NodePod> all = client.listPods(node, SmolvmNodeClient.PodQuery.all());

        assertThat(shop, hasSize(1));
        NodePod pod = shop.getFirst();
        assertThat(pod.sandboxId(), is("sandbox-orders"));
        assertThat(pod.namespace(), is("shop"));
        assertThat(pod.name(), is("orders-7d9f-abcde"));
        assertThat(pod.state(), is(NodePod.State.AWAKE));
        assertThat(pod.incarnation(), is(orders.path("incarnation").asText()));
        assertThat(pod.ip(), is(orders.path("ip").asText()));
        assertThat(pod.hibernate(), is(true));
        assertThat(pod.labels(), is(Map.of("app", "orders", "pod-template-hash", "7d9f")));
        assertThat(pod.controller(), is(new NodePod.Controller("Deployment", "orders")));
        assertThat(all.stream().map(NodePod::sandboxId).toList(), contains("sandbox-orders", "sandbox-dns"));
        assertThat(all.get(1).controller(), is(nullValue()));
        assertThat(all.get(1).labels(), is(Map.of()));
        FakeNodeApi.Request request = fake.requests().getFirst();
        assertThat(request.method(), is("GET"));
        assertThat(request.path(), is("/v1/pods"));
        assertThat(request.query(), is("namespace=shop"));
        assertThat(request.authorization(), is("Bearer " + TOKEN));
    }

    @Test
    void listsPodsByEveryFilter() {
        ObjectNode pod = fake.addPod("sandbox-web", "shop", "web");

        List<NodePod> found = client.listPods(node, new SmolvmNodeClient.PodQuery("shop", "web",
                pod.path("uid").asText(), pod.path("ip").asText(), pod.path("netns").asText()));

        assertThat(found, hasSize(1));
        assertThat(fake.requests().getFirst().query(), is("namespace=shop&name=web&uid=" + pod.path("uid").asText()
                + "&ip=" + pod.path("ip").asText() + "&netns=%2Fvar%2Frun%2Fnetns%2Fcni-sandbox-web"));
    }

    @Test
    void getsAPodByEachKindOfId() {
        ObjectNode web = fake.addPod("sandbox-web", "shop", "web");
        fake.state("sandbox-web", "asleep");

        assertThat(client.getPod(node, "sandbox-web").state(), is(NodePod.State.ASLEEP));
        assertThat(client.getPod(node, "shop/web").sandboxId(), is("sandbox-web"));
        assertThat(client.getPod(node, "uid:" + web.path("uid").asText()).sandboxId(), is("sandbox-web"));
        assertThat(client.getPod(node, "ip:" + web.path("ip").asText()).sandboxId(), is("sandbox-web"));
        assertThat(fake.requests().get(1).path(), is("/v1/pods/shop%2Fweb"));
        assertThat(fake.requests().get(2).path(), containsString("/v1/pods/uid%3A"));
    }

    @Test
    void anUnknownPodIsNotFound() {
        NodeApiException e = assertThrows(NodeApiException.class, () -> client.getPod(node, "shop/absent"));

        assertThat(e.code(), is(NodeApiException.Code.NOT_FOUND));
        assertThat(e.status(), is(404));
        assertThat(e.getMessage(), is("node:dev: GET /v1/pods/shop%2Fabsent: 404 not_found: no pod shop/absent"));
    }

    @Test
    void holdsRenewsAndReleasesALease() {
        ObjectNode pod = fake.addPod("sandbox-orders", "shop", "orders");
        String incarnation = pod.path("incarnation").asText();

        SmolvmNodeClient.LeaseHeld held = client.putLease(node, "sandbox-orders", "floci-orders", 60,
                "floci wakes: orders", incarnation);
        SmolvmNodeClient.LeaseHeld renewed = client.patchLease(node, "sandbox-orders", "floci-orders", 60, incarnation);
        NodePod released = client.deleteLease(node, "sandbox-orders", "floci-orders");

        assertThat(held.lease().id(), is("floci-orders"));
        assertThat(held.lease().kind(), is("api"));
        assertThat(held.lease().holder(), is("floci wakes: orders"));
        assertThat(held.lease().expiresAt(), is(notNullValue()));
        assertThat(held.pod().leases(), contains(held.lease()));
        assertThat(renewed.lease().expiresAt().isBefore(held.lease().expiresAt()), is(false));
        assertThat(released.leases(), is(empty()));
        List<FakeNodeApi.Request> requests = fake.requests();
        assertThat(requests.get(0).method(), is("PUT"));
        assertThat(requests.get(0).path(), is("/v1/pods/sandbox-orders/leases/floci-orders"));
        assertThat(requests.get(0).body(), is(json("{\"ttl_secs\":60,\"holder\":\"floci wakes: orders\","
                + "\"incarnation\":\"" + incarnation + "\"}")));
        assertThat(requests.get(0).headers().get("content-type"), is("application/json"));
        assertThat(requests.get(1).method(), is("PATCH"));
        assertThat(requests.get(1).body(), is(json("{\"ttl_secs\":60,\"incarnation\":\"" + incarnation + "\"}")));
        assertThat(requests.get(2).method(), is("DELETE"));
        assertThat(requests.get(2).body(), is(nullValue()));
    }

    @Test
    void renewingAnExpiredLeaseIsLeaseNotFound() {
        fake.addPod("sandbox-orders", "shop", "orders");

        NodeApiException e = assertThrows(NodeApiException.class,
                () -> client.patchLease(node, "sandbox-orders", "floci-orders", 30, null));

        assertThat(e.code(), is(NodeApiException.Code.LEASE_NOT_FOUND));
        assertThat(e.status(), is(404));
    }

    @Test
    void aLeaseForAnotherIncarnationIsStale() {
        fake.addPod("sandbox-orders", "shop", "orders");

        NodeApiException e = assertThrows(NodeApiException.class,
                () -> client.putLease(node, "sandbox-orders", "floci-orders", 30, null, "an-older-incarnation"));

        assertThat(e.code(), is(NodeApiException.Code.STALE_INCARNATION));
        assertThat(e.status(), is(409));
    }

    @Test
    void wakesAPodWithTheQueueThatWaits() {
        ObjectNode pod = fake.addPod("sandbox-orders", "shop", "orders");
        fake.state("sandbox-orders", "asleep");
        String incarnation = pod.path("incarnation").asText();

        SmolvmNodeClient.Wake wake = client.wake(node, "sandbox-orders", new SmolvmNodeClient.WakeRequest(
                SmolvmNodeClient.WakeReason.aws(QUEUE), "floci", incarnation), Duration.ofSeconds(30));

        assertThat(wake.resumed(), is(true));
        assertThat(wake.cancelledPause(), is(false));
        assertThat(wake.pod().state(), is(NodePod.State.AWAKE));
        assertThat(wake.generation(), is(wake.pod().generation()));
        FakeNodeApi.Request request = fake.requests().getFirst();
        assertThat(request.method(), is("POST"));
        assertThat(request.path(), is("/v1/pods/sandbox-orders/wake"));
        assertThat(request.body(), is(json("{\"reason\":{\"kind\":\"aws\",\"queue\":\"" + QUEUE + "\"},"
                + "\"source\":\"floci\",\"incarnation\":\"" + incarnation + "\"}")));
    }

    @Test
    void wakingAnAwakePodReturnsAtOnce() {
        fake.addPod("sandbox-orders", "shop", "orders");

        SmolvmNodeClient.Wake wake = client.wake(node, "shop/orders", new SmolvmNodeClient.WakeRequest(
                SmolvmNodeClient.WakeReason.api(), null, null), Duration.ofSeconds(30));

        assertThat(wake.resumed(), is(false));
        assertThat(fake.requests().getFirst().body(), is(json("{\"reason\":\"api\"}")));
    }

    @Test
    void wakingACreatingPodIsRefusedWithItsState() {
        fake.addPod("sandbox-orders", "shop", "orders");
        fake.state("sandbox-orders", "creating");

        NodeApiException e = assertThrows(NodeApiException.class, () -> client.wake(node, "sandbox-orders",
                new SmolvmNodeClient.WakeRequest(SmolvmNodeClient.WakeReason.api(), null, null), Duration.ofSeconds(5)));

        assertThat(e.code(), is(NodeApiException.Code.REFUSED));
        assertThat(e.status(), is(409));
        assertThat(e.reasons(), contains("state creating"));
    }

    @Test
    void aWakeWaitsOnlyAsLongAsItsTimeout() {
        fake.addPod("sandbox-orders", "shop", "orders");
        fake.delayNext("POST", "/v1/pods/sandbox-orders/wake", Duration.ofSeconds(3));

        NodeApiException e = assertThrows(NodeApiException.class, () -> client.wake(node, "sandbox-orders",
                new SmolvmNodeClient.WakeRequest(SmolvmNodeClient.WakeReason.api(), null, null),
                Duration.ofMillis(300)));

        assertThat(e.code(), is(NodeApiException.Code.UNREACHABLE));
        assertThat(e.status(), is(0));
    }

    @ParameterizedTest
    @CsvSource({
        "400, invalid, INVALID",
        "404, not_found, NOT_FOUND",
        "404, lease_not_found, LEASE_NOT_FOUND",
        "409, refused, REFUSED",
        "409, cancelled, CANCELLED",
        "409, failed, FAILED",
        "409, stale_incarnation, STALE_INCARNATION",
        "410, gone, GONE",
        "500, vm_error, VM_ERROR",
        "500, store_error, STORE_ERROR",
        "409, something_new, UNEXPECTED",
    })
    void errorCodesMapToTheirType(int status, String code, NodeApiException.Code expected) {
        fake.addPod("sandbox-orders", "shop", "orders");
        fake.failNext("POST", "/v1/pods/sandbox-orders/wake", status, code, "it went wrong", "wake refused");

        NodeApiException e = assertThrows(NodeApiException.class, () -> client.wake(node, "sandbox-orders",
                new SmolvmNodeClient.WakeRequest(null, null, null), Duration.ofSeconds(5)));

        assertThat(e.code(), is(expected));
        assertThat(e.status(), is(status));
        assertThat(e.reasons(), contains("wake refused"));
        assertThat(e.getMessage(), is("node:dev: POST /v1/pods/sandbox-orders/wake: " + status + " " + code
                + ": it went wrong"));
    }

    @Test
    void anErrorWithoutABodyMapsByItsStatus() {
        fake.answerNext("GET", "/v1/pods", 404, "");

        NodeApiException e = assertThrows(NodeApiException.class,
                () -> client.listPods(node, SmolvmNodeClient.PodQuery.all()));

        assertThat(e.code(), is(NodeApiException.Code.NOT_FOUND));
        assertThat(e.getMessage(), is("node:dev: GET /v1/pods: 404"));
    }

    @Test
    void anAnswerThatCannotBeReadIsUnexpected() {
        fake.answerNext("GET", "/v1/pods", 200, "not json");
        NodeApiException notJson = assertThrows(NodeApiException.class,
                () -> client.listPods(node, SmolvmNodeClient.PodQuery.all()));
        fake.answerNext("GET", "/v1/pods", 200,
                "{\"pods\": [{\"sandbox_id\": \"s\", \"namespace\": \"n\", \"name\": \"p\", \"uid\": \"u\", "
                        + "\"state\": \"dreaming\"}]}");
        NodeApiException unknownState = assertThrows(NodeApiException.class,
                () -> client.listPods(node, SmolvmNodeClient.PodQuery.all()));

        assertThat(notJson.code(), is(NodeApiException.Code.UNEXPECTED));
        assertThat(unknownState.code(), is(NodeApiException.Code.UNEXPECTED));
        assertThat(unknownState.getMessage(), containsString("unknown pod state dreaming"));
    }

    @Test
    void aNodeThatDoesNotAnswerIsUnreachable() throws IOException {
        int port;
        try (ServerSocket socket = new ServerSocket()) {
            socket.bind(new InetSocketAddress("127.0.0.1", 0));
            port = socket.getLocalPort();
        }
        SmolvmNode gone = new SmolvmNode("node:gone", URI.create("http://127.0.0.1:" + port),
                NodeToken.file(tokenFile), SmolvmNode.Kind.DECLARED, List.of());

        NodeApiException e = assertThrows(NodeApiException.class,
                () -> client.listPods(gone, SmolvmNodeClient.PodQuery.all()));

        assertThat(e.code(), is(NodeApiException.Code.UNREACHABLE));
        assertThat(e.getMessage(), containsString("node:gone: GET /v1/pods: no answer"));
    }

    @Test
    void aRefusedTokenIsReadAgainOnce() throws IOException {
        fake.addPod("sandbox-orders", "shop", "orders");
        client.listPods(node, SmolvmNodeClient.PodQuery.all());
        fake.token(NEW_TOKEN);
        writeToken(NEW_TOKEN);

        List<LogRecord> records = LogCapture.capture(SmolvmNodeClient.class,
                () -> assertThat(client.listPods(node, SmolvmNodeClient.PodQuery.all()), hasSize(1)));
        client.listPods(node, SmolvmNodeClient.PodQuery.all());

        assertThat(fake.requests().stream().map(FakeNodeApi.Request::authorization).toList(),
                contains("Bearer " + TOKEN, "Bearer " + TOKEN, "Bearer " + NEW_TOKEN, "Bearer " + NEW_TOKEN));
        assertThat(records, not(empty()));
        for (LogRecord record : records) {
            String text = record.getMessage() + " " + List.of(record.getParameters() == null
                    ? new Object[0] : record.getParameters());
            assertThat(text, not(containsString(TOKEN)));
            assertThat(text, not(containsString(NEW_TOKEN)));
        }
    }

    @Test
    void aRefusedTokenThatDidNotChangeIsUnauthorized() {
        fake.token(NEW_TOKEN);

        NodeApiException e = assertThrows(NodeApiException.class,
                () -> client.listPods(node, SmolvmNodeClient.PodQuery.all()));

        assertThat(e.code(), is(NodeApiException.Code.UNAUTHORIZED));
        assertThat(e.status(), is(401));
        assertThat(e.getMessage(), not(containsString(TOKEN)));
        assertThat(fake.requests(), hasSize(1));
    }

    @Test
    void aTokenThatCannotBeReadIsUnauthorizedWithoutACall() throws IOException {
        Files.delete(tokenFile);

        NodeApiException e = assertThrows(NodeApiException.class,
                () -> client.listPods(node, SmolvmNodeClient.PodQuery.all()));

        assertThat(e.code(), is(NodeApiException.Code.UNAUTHORIZED));
        assertThat(e.getMessage(), is("node:dev: the token file " + tokenFile + " does not exist"));
        assertThat(fake.requests(), is(empty()));
    }

    @Test
    void callsUseHttp11WithoutAnUpgrade() {
        client.listPods(node, SmolvmNodeClient.PodQuery.all());

        Map<String, String> headers = fake.requests().getFirst().headers();
        assertThat(headers, not(hasKey("upgrade")));
        assertThat(headers, not(hasKey("http2-settings")));
    }

    @Test
    void readsAPageOfEvents() {
        fake.addPod("sandbox-a", "shop", "a");
        fake.addPod("sandbox-b", "shop", "b");
        fake.state("sandbox-a", "asleep");

        SmolvmNodeClient.EventPage first = client.events(node, 0, 2, null);
        SmolvmNodeClient.EventPage rest = client.events(node, first.cursor(), 500, null);
        SmolvmNodeClient.EventPage ofA = client.events(node, 0, 500, "sandbox-a");

        assertThat(first.events().stream().map(NodeEvent::seq).toList(), contains(1L, 2L));
        assertThat(first.cursor(), is(2L));
        assertThat(first.gap(), is(false));
        assertThat(rest.events().stream().map(NodeEvent::seq).toList(), contains(3L, 4L, 5L));
        assertThat(ofA.events().stream().map(NodeEvent::to).toList(), contains("creating", "awake", "asleep"));
        NodeEvent sleep = ofA.events().getLast();
        assertThat(sleep.pod(), is("sandbox-a"));
        assertThat(sleep.from(), is("awake"));
        assertThat(sleep.reason(), is("api"));
        assertThat(sleep.time(), is(notNullValue()));
        assertThat(fake.requests().get(2).query(), is("cursor=0&limit=500&pod=sandbox-a"));
    }

    @Test
    void theHeadOfTheEventsIsTheLastEventOfTheLog() {
        assertThat(client.eventsHead(node), is(0L));

        fake.addPod("sandbox-a", "shop", "a");
        fake.state("sandbox-a", "asleep");

        assertThat(client.eventsHead(node), is(3L));
        assertThat(fake.requests().getLast().query(), is("cursor=0&limit=1&pod=floci-head"));
    }

    @Test
    void readsTheDetailOfAReason() {
        fake.addPod("sandbox-a", "shop", "a");
        ObjectNode event = fake.addEvent("sandbox-a", "asleep", "waking", "traffic");
        event.putObject("reason_detail").put("kind", "traffic").put("from", "shop/web").put("port", 8080);

        NodeEvent read = client.events(node, 2, 500, null).events().getFirst();

        assertThat(read.reasonDetail(), is(Map.of("kind", "traffic", "from", "shop/web", "port", "8080")));
    }

    @Test
    void followsTheEventsAfterTheCursorAndThenNewOnes() throws Exception {
        fake.keepalive(Duration.ofMillis(50));
        fake.addPod("sandbox-a", "shop", "a");
        List<NodeEvent> events = new CopyOnWriteArrayList<>();

        try (SmolvmNodeClient.EventFollow follow = client.follow(node, 1, null, events::add)) {
            await().atMost(Duration.ofSeconds(5)).until(() -> events.size() == 1);
            Thread.sleep(200);
            fake.state("sandbox-a", "asleep");
            await().atMost(Duration.ofSeconds(5)).until(() -> events.size() == 2);
            assertThat(follow.done().isDone(), is(false));
        }

        assertThat(events.stream().map(NodeEvent::seq).toList(), contains(2L, 3L));
        assertThat(events.getLast().to(), is("asleep"));
        assertThat(fake.requests().getFirst().query(), is("follow=true&cursor=1"));
    }

    @Test
    void theHeadOfTheNetworkEventsIsTheirLastEvent() {
        assertThat(client.networkEventsHead(node), is(0L));

        fake.addNetworkEvent("blocked", "shop/orders", "1.1.1.1", 443, "tcp", 2);
        fake.addNetworkEvent("relayed", "shop/orders", "api.vendor.test", 443, "https", 1);

        assertThat(client.networkEventsHead(node), is(2L));
        assertThat(fake.requests().getLast().path(), is("/v1/network/events"));
        assertThat(fake.requests().getLast().query(), is("cursor=0&limit=500"));
    }

    @Test
    void followsTheNetworkEventsAfterTheCursorAndThenNewOnes() {
        fake.keepalive(Duration.ofMillis(50));
        fake.addNetworkEvent("blocked", "shop/orders", "1.1.1.1", 443, "tcp", 2);
        List<NodeNetworkEvent> events = new CopyOnWriteArrayList<>();

        try (SmolvmNodeClient.EventFollow follow = client.followNetwork(node, 1, events::add)) {
            fake.addNetworkEvent("blocked", "shop/orders", "93.184.215.14", 443, "tcp", 3, "api.example.com");
            await().atMost(Duration.ofSeconds(5)).until(() -> events.size() == 1);
            synchronized (fake) {
                fake.addNetworkEvent("relay_failed", "shop/orders", "api.vendor.test", 443, "https", 1)
                        .put("route", "payments").put("error", "no route");
            }
            await().atMost(Duration.ofSeconds(5)).until(() -> events.size() == 2);
            assertThat(follow.done().isDone(), is(false));
        }

        NodeNetworkEvent blocked = events.getFirst();
        assertThat(blocked.seq(), is(2L));
        assertThat(blocked.kind(), is("blocked"));
        assertThat(blocked.pod(), is("shop/orders"));
        assertThat(blocked.destination(), is("93.184.215.14"));
        assertThat(blocked.port(), is(443));
        assertThat(blocked.protocol(), is("tcp"));
        assertThat(blocked.count(), is(3L));
        assertThat(blocked.names(), contains("api.example.com"));
        assertThat(blocked.first().isBefore(blocked.last()), is(true));
        assertThat(events.getLast().route(), is("payments"));
        assertThat(events.getLast().error(), is("no route"));
        assertThat(fake.requests().getFirst().path(), is("/v1/network/events"));
        assertThat(fake.requests().getFirst().query(), is("follow=true&cursor=1"));
    }

    @Test
    void followsOnePod() {
        fake.addPod("sandbox-a", "shop", "a");
        fake.addPod("sandbox-b", "shop", "b");
        List<NodeEvent> events = new CopyOnWriteArrayList<>();

        try (SmolvmNodeClient.EventFollow follow = client.follow(node, 0, "sandbox-b", events::add)) {
            await().atMost(Duration.ofSeconds(5)).until(() -> events.size() == 2);
            fake.state("sandbox-a", "asleep");
            fake.state("sandbox-b", "asleep");
            await().atMost(Duration.ofSeconds(5)).until(() -> events.size() == 3);
            assertThat(follow.done().isDone(), is(false));
        }

        assertThat(events.stream().map(NodeEvent::pod).distinct().toList(), contains("sandbox-b"));
    }

    @Test
    void followsTheLinesOfANodeLogWithoutItsKeepalives() throws Exception {
        fake.keepalive(Duration.ofMillis(50));
        fake.appendLog("k3s", "before the follow");
        List<String> lines = new CopyOnWriteArrayList<>();

        try (SmolvmNodeClient.EventFollow follow = client.followLog(node, SmolvmNodeClient.K3S_LOG, lines::add)) {
            await().atMost(Duration.ofSeconds(5)).until(() -> fake.requests().size() == 1);
            Thread.sleep(200);
            fake.appendLog("k3s", "I1025 apiserver started");
            fake.appendLog("kube-apiserver-audit", "{\"kind\":\"Event\"}");
            await().atMost(Duration.ofSeconds(5)).until(() -> lines.size() == 1);
            Thread.sleep(200);
            assertThat(follow.done().isDone(), is(false));
        }

        assertThat(lines, contains("I1025 apiserver started"));
        assertThat(fake.requests().getFirst().path(), is("/v1/logs/k3s"));
        assertThat(fake.requests().getFirst().authorization(), is("Bearer " + TOKEN));
    }

    @Test
    void aFollowEndsWhenTheNodeEndsIt() throws Exception {
        fake.addPod("sandbox-a", "shop", "a");
        List<NodeEvent> events = new CopyOnWriteArrayList<>();
        SmolvmNodeClient.EventFollow follow = client.follow(node, 0, null, events::add);
        await().atMost(Duration.ofSeconds(5)).until(() -> events.size() == 2);

        fake.endFollows();

        assertThat(follow.done().get(5, TimeUnit.SECONDS), is(nullValue()));
    }

    @Test
    void aFollowWithoutLinesForTheIdleLimitFailsAsUnreachable() throws Exception {
        fake.keepalive(Duration.ofMillis(100));
        fake.addPod("sandbox-a", "shop", "a");
        List<NodeEvent> events = new CopyOnWriteArrayList<>();
        SmolvmNodeClient quick = quickClient();
        try {
            SmolvmNodeClient.EventFollow follow = quick.follow(node, 0, null, events::add);
            await().atMost(Duration.ofSeconds(5)).until(() -> events.size() == 2);
            Thread.sleep(1500);
            assertThat(follow.done().isDone(), is(false));

            fake.silenceFollows();

            ExecutionException e = assertThrows(ExecutionException.class,
                    () -> follow.done().get(5, TimeUnit.SECONDS));
            NodeApiException failure = (NodeApiException) e.getCause();
            assertThat(failure.code(), is(NodeApiException.Code.UNREACHABLE));
            assertThat(failure.getMessage(), is("node:dev: GET /v1/events?follow=true&cursor=0: no event or keepalive "
                    + "line in 1 s"));
        } finally {
            quick.close();
        }
    }

    @Test
    void aFollowThatGetsNoAnswerFailsAsUnreachable() throws Exception {
        fake.silenceFollows();
        SmolvmNodeClient quick = quickClient();
        try {
            CompletableFuture<Void> done = quick.follow(node, 0, null, event -> { }).done();

            ExecutionException e = assertThrows(ExecutionException.class, () -> done.get(5, TimeUnit.SECONDS));
            assertThat(((NodeApiException) e.getCause()).code(), is(NodeApiException.Code.UNREACHABLE));
        } finally {
            quick.close();
        }
    }

    @Test
    void aClosedFollowIsDone() {
        SmolvmNodeClient.EventFollow follow = client.follow(node, 0, null, event -> { });

        follow.close();

        assertThat(follow.done().isDone(), is(true));
        assertThat(follow.done().isCompletedExceptionally(), is(false));
    }

    @Test
    void aFollowReadsARefusedTokenAgain() throws IOException {
        fake.addPod("sandbox-a", "shop", "a");
        client.listPods(node, SmolvmNodeClient.PodQuery.all());
        fake.token(NEW_TOKEN);
        writeToken(NEW_TOKEN);
        List<NodeEvent> events = new CopyOnWriteArrayList<>();

        try (SmolvmNodeClient.EventFollow follow = client.follow(node, 0, null, events::add)) {
            await().atMost(Duration.ofSeconds(5)).until(() -> events.size() == 2);
            assertThat(follow.done().isDone(), is(false));
        }

        assertThat(fake.requests().stream().map(FakeNodeApi.Request::authorization).toList(),
                contains("Bearer " + TOKEN, "Bearer " + TOKEN, "Bearer " + NEW_TOKEN));
    }

    @Test
    void aFollowThatTheNodeRefusesFails() {
        fake.failNext("GET", "/v1/events", 400, "invalid", "bad cursor");

        SmolvmNodeClient.EventFollow follow = client.follow(node, 0, null, event -> { });

        ExecutionException e = assertThrows(ExecutionException.class, () -> follow.done().get(5, TimeUnit.SECONDS));
        assertThat(e.getCause(), instanceOf(NodeApiException.class));
        assertThat(((NodeApiException) e.getCause()).code(), is(NodeApiException.Code.INVALID));
    }

    @Test
    void aFollowEndsWithItsListenersFailure() {
        fake.addPod("sandbox-a", "shop", "a");
        IllegalStateException failure = new IllegalStateException("listener failed");

        SmolvmNodeClient.EventFollow follow = client.follow(node, 0, null, event -> {
            throw failure;
        });

        ExecutionException e = assertThrows(ExecutionException.class, () -> follow.done().get(5, TimeUnit.SECONDS));
        assertThat(e.getCause(), sameInstance(failure));
    }

    @Test
    void aFollowEndsOnAnEventItCannotRead() {
        fake.answerNext("GET", "/v1/events", 200, "{\"seq\": \"one\"}\n");

        CompletableFuture<Void> done = client.follow(node, 0, null, event -> { }).done();

        ExecutionException e = assertThrows(ExecutionException.class, () -> done.get(5, TimeUnit.SECONDS));
        assertThat(((NodeApiException) e.getCause()).code(), is(NodeApiException.Code.UNEXPECTED));
    }

    @Test
    void segmentsAndValuesArePercentEncoded() {
        assertThat(SmolvmNodeClient.encode("shop/web"), is("shop%2Fweb"));
        assertThat(SmolvmNodeClient.encode("uid:a b+c"), is("uid%3Aa%20b%2Bc"));
        assertThat(SmolvmNodeClient.encode("floci-laptop-orders_1.v2~"), is("floci-laptop-orders_1.v2~"));
        assertThat(SmolvmNodeClient.encode("é"), is("%C3%A9"));
    }

    /** A client whose follows fail after 1 s without a line. */
    private SmolvmNodeClient quickClient() {
        return new SmolvmNodeClient(mapper, HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(SmolvmNodeClient.CONNECT_TIMEOUT)
                .build(), Duration.ofSeconds(1));
    }

    private JsonNode json(String text) {
        try {
            return mapper.readTree(text);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private void writeToken(String token) throws IOException {
        Files.writeString(tokenFile, token + "\n");
        Files.setPosixFilePermissions(tokenFile, PosixFilePermissions.fromString("rw-------"));
    }
}
