package io.github.hectorvent.floci.runtime.wakes;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.runtime.node.FakeNodeApi;
import io.github.hectorvent.floci.runtime.node.NodeEvent;
import io.github.hectorvent.floci.runtime.node.NodeToken;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

class NodeEventWatchServiceTest {

    private static final String TOKEN = "0123456789abcdef0123456789abcdef";

    private final SmolvmNodeClient client = new SmolvmNodeClient(new ObjectMapper());
    private final Recorder recorder = new Recorder();
    private FakeNodeApi fake;
    private SmolvmNode node;
    private NodeEventWatch watch;

    @BeforeEach
    void start() throws IOException {
        fake = new FakeNodeApi(TOKEN);
        node = fake.node("dev", new NodeToken.TokenVariable("NODE_TOKEN", name -> TOKEN));
        watch = new NodeEventWatch(client);
    }

    @AfterEach
    void stop() {
        watch.close();
        fake.close();
    }

    @Test
    void aFollowStartsAtTheHeadAndTellsTheListenerOnceItRuns() {
        fake.addPod("sb-old", "shop", "old");

        watch.watch(Set.of(node), recorder);
        await().atMost(Duration.ofSeconds(5)).until(() -> recorder.started().size() == 1);
        fake.addPod("sb-new", "shop", "new");

        await().atMost(Duration.ofSeconds(5)).until(() -> recorder.events().size() == 2);
        assertThat(recorder.events(), contains("sb-new none->creating", "sb-new creating->awake"));
        List<FakeNodeApi.Request> requests = fake.requests();
        assertThat(requests.get(0).query(), is("cursor=0&limit=1&pod=floci-head"));
        assertThat(requests.get(1).query(), is("follow=true&cursor=2"));
    }

    @Test
    void aFollowThatEndsStartsAgainFromANewHead() {
        watch.watch(Set.of(node), recorder);
        await().atMost(Duration.ofSeconds(5)).until(() -> watch.following(node.name()));
        fake.addPod("sb-1", "shop", "one");
        await().atMost(Duration.ofSeconds(5)).until(() -> recorder.events().size() == 2);

        fake.endFollows();
        fake.state("sb-1", "asleep");

        await().atMost(Duration.ofSeconds(5)).until(() -> recorder.started().size() == 2);
        List<String> follows = fake.requests().stream()
                .filter(request -> request.query() != null && request.query().startsWith("follow=true"))
                .map(FakeNodeApi.Request::query)
                .toList();
        assertThat(follows.getLast(), startsWith("follow=true&cursor="));
        assertThat(follows.size(), is(2));
    }

    @Test
    void aNodeThatDoesNotAnswerIsFollowedOnceItDoes() throws IOException {
        FakeNodeApi down = new FakeNodeApi(TOKEN);
        SmolvmNode gone = down.node("down", new NodeToken.TokenVariable("NODE_TOKEN", name -> TOKEN));
        down.close();

        watch.watch(Set.of(gone, node), recorder);

        await().atMost(Duration.ofSeconds(5)).until(() -> watch.following(node.name()));
        assertThat(watch.following(gone.name()), is(false));
        assertThat(recorder.started(), contains(node.name()));
    }

    @Test
    void aNodeNoLongerWatchedIsNoLongerFollowed() throws InterruptedException {
        watch.watch(Set.of(node), recorder);
        await().atMost(Duration.ofSeconds(5)).until(() -> watch.following(node.name()));

        watch.watch(Set.of(), recorder);
        fake.addPod("sb-1", "shop", "one");
        Thread.sleep(Duration.ofMillis(500));

        assertThat(watch.following(node.name()), is(false));
        assertThat(recorder.events().isEmpty(), is(true));
    }

    private static final class Recorder implements NodeEventWatch.Listener {

        private final List<String> events = new ArrayList<>();
        private final List<String> started = new ArrayList<>();

        @Override
        public synchronized void event(String node, NodeEvent event) {
            events.add(event.pod() + " " + event.from() + "->" + event.to());
        }

        @Override
        public synchronized void started(String node) {
            started.add(node);
        }

        synchronized List<String> events() {
            return List.copyOf(events);
        }

        synchronized List<String> started() {
            return List.copyOf(started);
        }
    }
}
