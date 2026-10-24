package io.github.hectorvent.floci.runtime.node;

import io.github.hectorvent.floci.testutil.LogCapture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RuntimeFileWatcherTest {

    private static final String TOKEN = "0123456789abcdef0123456789abcdef";

    @TempDir
    Path dir;

    private final NodesSection nodes = new NodesSection(Map.of("T", TOKEN)::get);
    private final List<RuntimeFile> told = new CopyOnWriteArrayList<>();
    private RuntimeFileWatcher watcher;
    private int rewrites;

    @AfterEach
    void stop() {
        if (watcher != null) {
            watcher.onStop(null);
        }
    }

    @Test
    void withoutAFileEverySectionIsEmpty() {
        watcher = new RuntimeFileWatcher(null, List.of(nodes));

        watcher.onStart(null);
        watcher.check();

        assertThat(watcher.current().section(nodes), is(anEmptyMap()));
        assertThat(watcher.current().instanceId().isPresent(), is(false));
    }

    @Test
    void startupFailsWhenTheFileIsMissing() {
        Path file = dir.resolve("runtime.yaml");
        watcher = new RuntimeFileWatcher(file, List.of(nodes));

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> watcher.onStart(null));

        assertThat(e.getMessage(), is("The runtime file " + file
                + " (FLOCI_RUNTIME_CONFIG_FILE) is invalid: the file does not exist"));
    }

    @Test
    void startupFailsWhenTheFileIsInvalid() throws IOException {
        Path file = write(nodeFile("dev", "ftp://127.0.0.1:1"));
        watcher = new RuntimeFileWatcher(file, List.of(nodes));

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> watcher.onStart(null));

        assertThat(e.getMessage(), containsString("nodes.dev.url: must be http:// or https://"));
    }

    @Test
    void aValidChangeReplacesTheDefinitionsAndTellsTheListeners() throws IOException {
        Path file = write(nodeFile("dev", "http://127.0.0.1:1"));
        watcher = started(file);

        rewrite(file, nodeFile("dev", "http://127.0.0.1:2") + "  prod: {url: \"http://127.0.0.1:3\", token-env: T}\n");
        watcher.check();

        assertThat(watcher.current().section(nodes).keySet(), contains("dev", "prod"));
        assertThat(watcher.current().section(nodes).get("dev").baseUrl().getPort(), is(2));
        assertThat(told, hasSize(1));
        assertThat(told.getFirst(), sameInstance(watcher.current()));
    }

    @Test
    void aChangeOfTheModificationTimeAloneIsReadToo() throws IOException {
        Path file = write(nodeFile("dev", "http://127.0.0.1:1"));
        watcher = started(file);

        rewrite(file, nodeFile("dev", "http://127.0.0.1:2"));
        watcher.check();

        assertThat(watcher.current().section(nodes).get("dev").baseUrl().getPort(), is(2));
        assertThat(told, hasSize(1));
    }

    @Test
    void anInvalidChangeKeepsTheDefinitionsInForceAndLogsTheProblem() throws IOException {
        Path file = write(nodeFile("dev", "http://127.0.0.1:1"));
        watcher = started(file);
        RuntimeFile before = watcher.current();

        rewrite(file, nodeFile("dev", "http://127.0.0.1"));
        List<LogRecord> records = errors(LogCapture.capture(RuntimeFileWatcher.class, watcher::check));

        assertThat(watcher.current(), sameInstance(before));
        assertThat(told, is(empty()));
        assertThat(records, hasSize(1));
        assertThat(String.valueOf(records.getFirst().getParameters()[1]),
                is("nodes.dev.url: must be http:// or https:// with a host and a port, and nothing else"));
    }

    @Test
    void anInvalidFileIsLoggedOnceUntilItChangesAgain() throws IOException {
        Path file = write(nodeFile("dev", "http://127.0.0.1:1"));
        watcher = started(file);

        rewrite(file, "version: 2\n");
        List<LogRecord> first = errors(LogCapture.capture(RuntimeFileWatcher.class, watcher::check));
        List<LogRecord> second = errors(LogCapture.capture(RuntimeFileWatcher.class, watcher::check));
        rewrite(file, "version: 3\n");
        List<LogRecord> third = errors(LogCapture.capture(RuntimeFileWatcher.class, watcher::check));

        assertThat(first, hasSize(1));
        assertThat(second, is(empty()));
        assertThat(third, hasSize(1));
    }

    @Test
    void aFixedTokenFileTakesEffectWithoutAChangeToTheRuntimeFile() throws IOException {
        Path file = write(nodeFile("dev", "http://127.0.0.1:1"));
        watcher = started(file);
        Path token = dir.resolve("prod-token");

        rewrite(file, nodeFile("dev", "http://127.0.0.1:1")
                + "  prod: {url: \"http://127.0.0.1:2\", token-file: prod-token}\n");
        List<LogRecord> missing = errors(LogCapture.capture(RuntimeFileWatcher.class, watcher::check));
        Files.writeString(token, TOKEN + "\n");
        Files.setPosixFilePermissions(token, PosixFilePermissions.fromString("rw-r--r--"));
        List<LogRecord> open = errors(LogCapture.capture(RuntimeFileWatcher.class, watcher::check));
        List<LogRecord> openAgain = errors(LogCapture.capture(RuntimeFileWatcher.class, watcher::check));
        assertThat(watcher.current().section(nodes).keySet(), contains("dev"));

        Files.setPosixFilePermissions(token, PosixFilePermissions.fromString("rw-------"));
        watcher.check();

        assertThat(missing, hasSize(1));
        assertThat(String.valueOf(missing.getFirst().getParameters()[1]), containsString("does not exist"));
        assertThat(open, hasSize(1));
        assertThat(String.valueOf(open.getFirst().getParameters()[1]), containsString("chmod 600"));
        assertThat(openAgain, is(empty()));
        assertThat(watcher.current().section(nodes).keySet(), contains("dev", "prod"));
        assertThat(told, hasSize(1));
    }

    @Test
    void aFileFixedBackToTheDefinitionsInForceTellsNoOne() throws IOException {
        Path file = write(nodeFile("dev", "http://127.0.0.1:1"));
        watcher = started(file);
        RuntimeFile before = watcher.current();

        rewrite(file, "version: 2\n");
        watcher.check();
        rewrite(file, nodeFile("dev", "http://127.0.0.1:1"));
        List<LogRecord> records = LogCapture.capture(RuntimeFileWatcher.class, watcher::check);
        List<LogRecord> later = LogCapture.capture(RuntimeFileWatcher.class, watcher::check);

        assertThat(watcher.current(), sameInstance(before));
        assertThat(told, is(empty()));
        assertThat(records.stream().map(LogRecord::getMessage).toList(),
                contains("The runtime file {0} is valid again, with the definitions in force"));
        assertThat(later, is(empty()));
    }

    @Test
    void aRemovedFileKeepsTheDefinitionsUntilItComesBack() throws IOException {
        Path file = write(nodeFile("dev", "http://127.0.0.1:1"));
        watcher = started(file);
        RuntimeFile before = watcher.current();

        Files.delete(file);
        watcher.check();
        assertThat(watcher.current(), sameInstance(before));

        rewrite(file, nodeFile("prod", "http://127.0.0.1:1"));
        watcher.check();
        assertThat(watcher.current().section(nodes).keySet(), contains("prod"));
        assertThat(told, hasSize(1));
    }

    @Test
    void aChangeWithTheSameDefinitionsTellsNoOne() throws IOException {
        Path file = write(nodeFile("dev", "http://127.0.0.1:1"));
        watcher = started(file);
        RuntimeFile before = watcher.current();

        rewrite(file, "# the same nodes\n" + nodeFile("dev", "http://127.0.0.1:1"));
        watcher.check();

        assertThat(watcher.current(), sameInstance(before));
        assertThat(told, is(empty()));
    }

    @Test
    void aFailingListenerDoesNotStopTheOthers() throws IOException {
        Path file = write(nodeFile("dev", "http://127.0.0.1:1"));
        watcher = new RuntimeFileWatcher(file, List.of(nodes));
        watcher.addListener(changed -> {
            throw new IllegalStateException("listener failed");
        });
        watcher.addListener(told::add);
        watcher.current();

        rewrite(file, nodeFile("dev", "http://127.0.0.1:2"));
        watcher.check();

        assertThat(told, hasSize(1));
    }

    @Test
    void aRemovedListenerIsNotTold() throws IOException {
        Path file = write(nodeFile("dev", "http://127.0.0.1:1"));
        watcher = new RuntimeFileWatcher(file, List.of(nodes));
        Consumer<RuntimeFile> listener = told::add;
        watcher.addListener(listener);
        watcher.current();
        watcher.removeListener(listener);

        rewrite(file, nodeFile("dev", "http://127.0.0.1:2"));
        watcher.check();

        assertThat(watcher.current().section(nodes).get("dev").baseUrl().getPort(), is(2));
        assertThat(told, is(empty()));
    }

    @Test
    void theFileIsCheckedEveryTwoSeconds() throws IOException {
        Path file = write(nodeFile("dev", "http://127.0.0.1:1"));
        watcher = started(file);

        rewrite(file, nodeFile("dev", "http://127.0.0.1:2"));

        await().atMost(Duration.ofSeconds(6)).until(() -> told.size() == 1);
        assertThat(watcher.current().section(nodes).get("dev").baseUrl().getPort(), is(2));
    }

    @Test
    void sectionsAreRegisteredOnceEach() {
        IllegalStateException twice = assertThrows(IllegalStateException.class,
                () -> new RuntimeFileWatcher(null, List.of(nodes, nodes)));
        assertThat(twice.getMessage(), containsString("nodes is registered twice"));
    }

    private static List<LogRecord> errors(List<LogRecord> records) {
        return records.stream().filter(record -> record.getLevel().intValue() >= Level.SEVERE.intValue()).toList();
    }

    private RuntimeFileWatcher started(Path file) {
        RuntimeFileWatcher started = new RuntimeFileWatcher(file, List.of(nodes));
        started.addListener(told::add);
        started.onStart(null);
        return started;
    }

    private static String nodeFile(String key, String url) {
        return "version: 1\nnodes:\n  " + key + ": {url: \"" + url + "\", token-env: T}\n";
    }

    private Path write(String content) throws IOException {
        Path file = dir.resolve("runtime.yaml");
        Files.writeString(file, content);
        return file;
    }

    /** Writes {@code content} with a modification time the watcher has not seen. */
    private void rewrite(Path file, String content) throws IOException {
        Files.writeString(file, content);
        rewrites++;
        Files.setLastModifiedTime(file, FileTime.from(Instant.now().plusSeconds(60L * rewrites)));
    }
}
