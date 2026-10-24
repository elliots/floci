package io.github.hectorvent.floci.runtime.node;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

class SmolvmNodeDirectoryTest {

    private static final String TOKEN = "0123456789abcdef0123456789abcdef";

    @TempDir
    Path dir;

    private final NodesSection nodesSection = new NodesSection(Map.of("T", TOKEN)::get);
    private final List<SmolvmNode> created = new CopyOnWriteArrayList<>();
    private final SmolvmNodeSource source = () -> List.copyOf(created);
    private final List<List<SmolvmNode>> told = new CopyOnWriteArrayList<>();
    private int rewrites;

    @Test
    void listsTheRuntimeFilesNodesThenEachSources() throws IOException {
        SmolvmNode cluster = cluster("integration-test");
        created.add(cluster);
        SmolvmNodeDirectory directory = directory(write(nodes("dev", "prod")));

        assertThat(directory.all().stream().map(SmolvmNode::name).toList(),
                contains("node:dev", "node:prod", "cluster:000000000000/us-east-1/integration-test"));
        assertThat(directory.find("node:prod").orElseThrow().baseUrl(), is(URI.create("http://127.0.0.1:2")));
        assertThat(directory.find(SmolvmNode.clusterName("000000000000", "us-east-1", "integration-test"))
                .orElseThrow(), is(cluster));
        assertThat(directory.find("node:absent").isPresent(), is(false));
    }

    @Test
    void withoutARuntimeFileOnlyTheSourcesNodesAreListed() {
        created.add(cluster("integration-test"));
        SmolvmNodeDirectory directory = new SmolvmNodeDirectory(new RuntimeFileWatcher(null, List.of(nodesSection)),
                nodesSection, List.of(source));

        assertThat(directory.all(), contains(created.getFirst()));
    }

    @Test
    void theFirstOfTwoNodesWithOneNameIsKept() throws IOException {
        SmolvmNode impostor = new SmolvmNode("node:dev", URI.create("http://127.0.0.1:9"), NodeToken.variable("T"),
                SmolvmNode.Kind.CREATED, List.of());
        created.add(impostor);
        SmolvmNodeDirectory directory = directory(write(nodes("dev")));

        assertThat(directory.all(), hasSize(1));
        assertThat(directory.find("node:dev").orElseThrow().kind(), is(SmolvmNode.Kind.DECLARED));
    }

    @Test
    void aChangedRuntimeFileTellsTheListeners() throws IOException {
        Path file = write(nodes("dev"));
        RuntimeFileWatcher watcher = new RuntimeFileWatcher(file, List.of(nodesSection));
        SmolvmNodeDirectory directory = new SmolvmNodeDirectory(watcher, nodesSection, List.of(source));
        directory.addListener(told::add);

        rewrite(file, nodes("dev", "prod"));
        watcher.check();

        assertThat(told, hasSize(1));
        assertThat(told.getFirst().stream().map(SmolvmNode::name).toList(), contains("node:dev", "node:prod"));
    }

    @Test
    void aSourcesChangeTellsTheListenersOnRefresh() throws IOException {
        SmolvmNodeDirectory directory = directory(write(nodes("dev")));
        directory.addListener(told::add);

        created.add(cluster("integration-test"));
        directory.refresh();
        directory.refresh();
        created.clear();
        directory.refresh();

        assertThat(told, hasSize(2));
        assertThat(told.get(0).stream().map(SmolvmNode::name).toList(),
                contains("node:dev", "cluster:000000000000/us-east-1/integration-test"));
        assertThat(told.get(1).stream().map(SmolvmNode::name).toList(), contains("node:dev"));
    }

    @Test
    void aRefreshWithoutAChangeTellsNoOne() throws IOException {
        created.add(cluster("integration-test"));
        SmolvmNodeDirectory directory = directory(write(nodes("dev")));
        directory.addListener(told::add);

        directory.refresh();

        assertThat(told, is(empty()));
    }

    @Test
    void aRemovedListenerIsNotTold() throws IOException {
        SmolvmNodeDirectory directory = directory(write(nodes("dev")));
        Consumer<List<SmolvmNode>> listener = told::add;
        directory.addListener(listener);
        directory.removeListener(listener);

        created.add(cluster("integration-test"));
        directory.refresh();

        assertThat(told, is(empty()));
    }

    @Test
    void aFailingListenerDoesNotStopTheOthers() throws IOException {
        SmolvmNodeDirectory directory = directory(write(nodes("dev")));
        directory.addListener(nodes -> {
            throw new IllegalStateException("listener failed");
        });
        directory.addListener(told::add);

        created.add(cluster("integration-test"));
        directory.refresh();

        assertThat(told, hasSize(1));
    }

    private SmolvmNodeDirectory directory(Path file) {
        RuntimeFileWatcher watcher = new RuntimeFileWatcher(file, List.of(nodesSection));
        return new SmolvmNodeDirectory(watcher, nodesSection, List.of(source));
    }

    private static SmolvmNode cluster(String name) {
        return new SmolvmNode(SmolvmNode.clusterName("000000000000", "us-east-1", name),
                URI.create("http://127.0.0.1:6600"), NodeToken.variable("T"), SmolvmNode.Kind.CREATED,
                List.of(new SmolvmNode.Endpoint("169.254.170.23", 80, 4566)));
    }

    private static String nodes(String... keys) {
        StringBuilder content = new StringBuilder("version: 1\nnodes:\n");
        for (int i = 0; i < keys.length; i++) {
            content.append("  ").append(keys[i]).append(": {url: \"http://127.0.0.1:").append(i + 1)
                    .append("\", token-env: T}\n");
        }
        return content.toString();
    }

    private Path write(String content) throws IOException {
        Path file = dir.resolve("runtime.yaml");
        Files.writeString(file, content);
        return file;
    }

    private void rewrite(Path file, String content) throws IOException {
        Files.writeString(file, content);
        rewrites++;
        Files.setLastModifiedTime(file, FileTime.from(Instant.now().plusSeconds(60L * rewrites)));
    }
}
