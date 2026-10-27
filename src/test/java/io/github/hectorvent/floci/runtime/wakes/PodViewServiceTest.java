package io.github.hectorvent.floci.runtime.wakes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.runtime.node.FakeNodeApi;
import io.github.hectorvent.floci.runtime.node.NodeApiException;
import io.github.hectorvent.floci.runtime.node.NodePod;
import io.github.hectorvent.floci.runtime.node.NodeToken;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import io.github.hectorvent.floci.runtime.wakes.PodView.Listing;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.PodSelector;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Target;
import io.github.hectorvent.floci.testing.MutableClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PodViewServiceTest {

    private static final String TOKEN = "0123456789abcdef0123456789abcdef";

    private final MutableClock clock = new MutableClock();
    private final Map<String, SmolvmNode> nodes = new HashMap<>();
    private FakeNodeApi fake;
    private SmolvmNode node;
    private PodView view;

    @BeforeEach
    void startFake() throws IOException {
        fake = new FakeNodeApi(TOKEN);
        node = fake.node("dev", new NodeToken.TokenVariable("NODE_TOKEN", name -> TOKEN));
        nodes.put(node.name(), node);
        view = new PodView(name -> Optional.ofNullable(nodes.get(name)), new SmolvmNodeClient(new ObjectMapper()),
                clock);
    }

    @AfterEach
    void stopFake() {
        fake.close();
    }

    @Test
    void listsTheTargetsNamespaceOnItsNode() {
        pod("sb-orders", "shop", "orders", Map.of("app", "orders"), "Deployment", "orders");
        pod("sb-other", "other", "orders", Map.of("app", "orders"), "Deployment", "orders");

        Listing listing = view.namespace(target("shop", PodSelector.Controller.deployment("orders"))).orElseThrow();

        assertThat(listing.node(), is(node));
        assertThat(listing.namespace(), is("shop"));
        assertThat(sandboxes(listing.pods()), contains("sb-orders"));
        assertThat(listGets(), contains("namespace=shop"));
    }

    @Test
    void selectsPodsByTheirControllerOrLabels() {
        pod("sb-a", "shop", "orders-a", Map.of("app", "orders", "tier", "back"), "Deployment", "orders");
        pod("sb-b", "shop", "orders-0", Map.of("app", "orders"), "StatefulSet", "orders");
        pod("sb-c", "shop", "jobs-c", Map.of("app", "jobs"), "Job", "orders");
        pod("sb-d", "shop", "bare", Map.of(), null, null);

        assertThat(selected(PodSelector.Controller.deployment("orders")), contains("sb-a"));
        assertThat(selected(PodSelector.Controller.statefulSet("orders")), contains("sb-b"));
        assertThat(selected(new PodSelector.Labels(Map.of("app", "orders"))), contains("sb-a", "sb-b"));
        assertThat(selected(new PodSelector.Labels(Map.of("app", "orders", "tier", "back"))), contains("sb-a"));
        assertThat(selected(new PodSelector.Labels(Map.of("app", "payments"))), is(empty()));
        assertThat(selected(PodSelector.Controller.deployment("payments")), is(empty()));
    }

    @Test
    void aListingServesForTwoSeconds() {
        pod("sb-orders", "shop", "orders", Map.of("app", "orders"), "Deployment", "orders");
        Target orders = target("shop", PodSelector.Controller.deployment("orders"));
        Target labelled = target("shop", new PodSelector.Labels(Map.of("app", "orders")));

        view.namespace(orders);
        clock.advance(Duration.ofMillis(1900));
        view.namespace(labelled);
        assertThat(listGets(), contains("namespace=shop"));

        clock.advance(Duration.ofMillis(100));
        fake.state("sb-orders", "asleep");
        Listing listing = view.namespace(orders).orElseThrow();

        assertThat(listGets(), contains("namespace=shop", "namespace=shop"));
        assertThat(listing.pods().getFirst().state(), is(NodePod.State.ASLEEP));
    }

    @Test
    void aForgottenNamespaceIsListedAgain() {
        pod("sb-orders", "shop", "orders", Map.of("app", "orders"), "Deployment", "orders");
        Target orders = target("shop", PodSelector.Controller.deployment("orders"));
        view.namespace(orders);
        view.all(node);

        view.forget(node.name(), "other");
        view.namespace(orders);
        view.forget(node.name(), "shop");
        view.namespace(orders);
        view.all(node);

        assertThat(listGets(), contains("namespace=shop", null, "namespace=shop", null));
    }

    @Test
    void allListsEveryPodOfTheNode() {
        pod("sb-a", "shop", "orders", Map.of(), "Deployment", "orders");
        pod("sb-b", "other", "jobs", Map.of(), "Deployment", "jobs");

        Listing listing = view.all(node);

        assertThat(listing.namespace(), is(nullValue()));
        assertThat(sandboxes(listing.pods()), contains("sb-a", "sb-b"));
        assertThat(listGets(), contains((String) null));
    }

    @Test
    void aNodeOutsideTheDirectoryHasNoListing() {
        Target target = new Target("cluster:000000000000/us-east-1/absent", "shop",
                PodSelector.Controller.deployment("orders"));

        assertThat(view.namespace(target).isPresent(), is(false));
        assertThat(fake.requests(), is(empty()));
    }

    @Test
    void aFailedListingIsNotKept() {
        pod("sb-orders", "shop", "orders", Map.of("app", "orders"), "Deployment", "orders");
        Target orders = target("shop", PodSelector.Controller.deployment("orders"));
        fake.failNext("GET", "/v1/pods", 500, "store_error", "the store failed");

        NodeApiException e = assertThrows(NodeApiException.class, () -> view.namespace(orders));
        Listing listing = view.namespace(orders).orElseThrow();

        assertThat(e.code(), is(NodeApiException.Code.STORE_ERROR));
        assertThat(sandboxes(listing.pods()), contains("sb-orders"));
    }

    @Test
    void aNodeThatChangesIsListedAgain() {
        pod("sb-orders", "shop", "orders", Map.of("app", "orders"), "Deployment", "orders");
        Target orders = target("shop", PodSelector.Controller.deployment("orders"));
        view.namespace(orders);

        SmolvmNode moved = new SmolvmNode(node.name(), node.baseUrl(), node.token(), node.kind(),
                List.of(new SmolvmNode.Endpoint("169.254.53.1", 4566, 4566)));
        nodes.put(moved.name(), moved);
        Listing listing = view.namespace(orders).orElseThrow();

        assertThat(listing.node(), is(moved));
        assertThat(listGets(), contains("namespace=shop", "namespace=shop"));
    }

    private List<String> selected(PodSelector selector) {
        Target target = target("shop", selector);
        return sandboxes(view.namespace(target).orElseThrow().select(target));
    }

    private Target target(String namespace, PodSelector selector) {
        return new Target(node.name(), namespace, selector);
    }

    /** The queries of the fake's pod listings, in order; null for a listing of every pod. */
    private List<String> listGets() {
        return fake.requests().stream()
                .filter(request -> request.method().equals("GET") && request.path().equals("/v1/pods"))
                .map(FakeNodeApi.Request::query)
                .toList();
    }

    private void pod(String sandbox, String namespace, String name, Map<String, String> labels, String kind,
                     String controller) {
        ObjectNode pod = fake.addPod(sandbox, namespace, name);
        ObjectNode podLabels = pod.putObject("labels");
        labels.forEach(podLabels::put);
        if (kind != null) {
            pod.putObject("controller").put("kind", kind).put("name", controller);
        }
    }

    private static List<String> sandboxes(List<NodePod> pods) {
        return pods.stream().map(NodePod::sandboxId).toList();
    }
}
