package io.github.hectorvent.floci.runtime.flocinode;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.RuntimeConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.RuntimeConfig.NodeConfig;
import io.github.hectorvent.floci.runtime.flocinode.FakeNodeDriver.Call;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode.NodeUnavailableException;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode.ShutdownAction;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode.State;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode.Status;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode.Use;
import io.github.hectorvent.floci.runtime.flocinode.NodeDriver.Machine;
import io.github.hectorvent.floci.runtime.flocinode.NodeIsolation.Isolation;
import io.github.hectorvent.floci.runtime.node.NodeToken;
import io.github.hectorvent.floci.runtime.node.NodesSection;
import io.github.hectorvent.floci.runtime.node.RuntimeFile;
import io.github.hectorvent.floci.runtime.node.RuntimeFileWatcher;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FlociNodeServiceTest {

    private static final String DIGEST = "a".repeat(64);
    private static final Map<String, String> OWNER_LABELS = Map.of(
            NodeDriver.LABEL_OWNER, "", NodeDriver.LABEL_DATA, "0123456789ab");
    private static final Use RDS = Use.of("RDS instance orders", "123456789012", "eu-west-1");
    private static final Use EKS = new Use("EKS cluster shop", "123456789012", "us-east-1", "172.20.0.0/16",
            "10.42.0.0/16", Map.of(NodeSeed.KUBELET_ARGS, List.of("max-pods=250")));

    @TempDir
    Path dataPath;

    private NodeConfig node;
    private EmulatorConfig config;
    private NodeSettings settings;
    private NodeSeed seed;
    private NodePorts ports;
    private FakeNodeDriver driver;
    private NodeKubeApi kubeApi;
    private NodeReadyRule readyRule;
    private RuntimeFileWatcher runtimeFile;
    private Consumer<RuntimeFile> runtimeFileListener;
    private ExecutorService executor;
    private MemoryStore store;
    private final AtomicInteger nodeIds = new AtomicInteger();
    private final AtomicInteger allocations = new AtomicInteger();
    private final AtomicInteger changes = new AtomicInteger();
    private volatile Isolation isolation = Isolation.OPEN;
    private volatile String storageMode = "memory";
    private volatile String runtimeYaml = "version: 1\n";
    private String readYaml;
    private RuntimeFile read;
    private FlociNode flociNode;

    /** The record in memory, as storage keeps it. */
    static final class MemoryStore implements FlociNode.RecordStore {

        final AtomicReference<NodeRecord> record = new AtomicReference<>();

        @Override
        public Optional<NodeRecord> load() {
            return Optional.ofNullable(record.get());
        }

        @Override
        public void save(NodeRecord saved) {
            record.set(saved);
        }

        @Override
        public void delete() {
            record.set(null);
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        config = mock(EmulatorConfig.class);
        RuntimeConfig runtime = mock(RuntimeConfig.class);
        node = mock(NodeConfig.class);
        when(config.runtime()).thenReturn(runtime);
        when(runtime.node()).thenReturn(node);
        when(config.defaultRegion()).thenReturn("us-east-1");
        when(config.defaultAccountId()).thenReturn("000000000000");
        when(node.nodeVmScript()).thenReturn(Optional.of("/opt/smolvm-pods/scripts/node-vm.sh"));
        when(node.imageSha256()).thenReturn(Optional.of(DIGEST.toUpperCase()));
        when(node.keepRunningOnShutdown()).thenReturn(true);
        when(node.readyTimeoutSeconds()).thenReturn(600);
        when(node.name()).thenReturn(Optional.empty());

        settings = mock(NodeSettings.class);
        when(settings.problems()).thenReturn(List.of());
        when(settings.wanted()).thenReturn(true);
        seed = mock(NodeSeed.class);
        when(seed.newNodeId()).thenAnswer(call -> nodeId(nodeIds.incrementAndGet()));
        when(seed.seedDir(anyString())).thenAnswer(call -> dataPath.resolve(call.<String>getArgument(0)).resolve("seed"));
        when(seed.apiToken(anyString())).thenAnswer(call -> NodeToken.file(
                dataPath.resolve(call.<String>getArgument(0)).resolve("seed").resolve("api-token")));
        when(seed.endpoints(any())).thenReturn(List.of());
        ports = mock(NodePorts.class);
        when(ports.allocate()).thenAnswer(call -> {
            int n = allocations.incrementAndGet();
            return new NodePorts.Ports(6500 + n, 6600 + n, n == 1 ? "32080-32099" : "none");
        });
        driver = new FakeNodeDriver(OWNER_LABELS);
        kubeApi = mock(NodeKubeApi.class);
        readyRule = mock(NodeReadyRule.class);
        when(readyRule.unmetCondition(any(), anyString(), any(), any())).thenAnswer(call -> {
            try {
                call.<NodeReadyRule.BootId>getArgument(3).read();
                return Optional.empty();
            } catch (NodeDriver.NodeCommandException e) {
                return Optional.of(e.getMessage());
            }
        });
        runtimeFile = mock(RuntimeFileWatcher.class);
        when(runtimeFile.current()).thenAnswer(call -> runtimeFile());
        executor = Executors.newVirtualThreadPerTaskExecutor();
        store = new MemoryStore();
        flociNode = flociNode(Duration.ofSeconds(5));
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @SuppressWarnings("unchecked")
    private FlociNode flociNode(Duration queueWait) {
        FlociNode created = new FlociNode(config, settings, seed, ports, driver, kubeApi, readyRule, runtimeFile,
                new NodesSection(), file -> isolation, changes::incrementAndGet, () -> storageMode, store, executor,
                queueWait, Duration.ofMillis(20));
        ArgumentCaptor<Consumer<RuntimeFile>> listener = ArgumentCaptor.forClass(Consumer.class);
        verify(runtimeFile, atLeastOnce()).addListener(listener.capture());
        runtimeFileListener = listener.getValue();
        return created;
    }

    /** The runtime file, with the test's nodes; the test's isolation stands for its network section. */
    private synchronized RuntimeFile runtimeFile() {
        if (readYaml != null && readYaml.equals(runtimeYaml)) {
            return read;
        }
        Path file = dataPath.resolve("runtime.yaml");
        try {
            Files.writeString(file, runtimeYaml);
            read = RuntimeFile.read(file, List.of(new NodesSection()));
            readYaml = runtimeYaml;
            return read;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Declares the runtime file's nodes {@code keys}, each with a token file. */
    private void declare(String... keys) throws IOException {
        StringBuilder yaml = new StringBuilder("version: 1\nnodes:\n");
        int port = 17070;
        for (String key : keys) {
            Path token = dataPath.resolve(key + "-token");
            Files.writeString(token, "token-of-the-node-" + key);
            Files.setPosixFilePermissions(token, PosixFilePermissions.fromString("rw-------"));
            yaml.append("  ").append(key).append(":\n    url: http://127.0.0.1:").append(port++)
                    .append("\n    token-file: ").append(token).append('\n');
        }
        runtimeYaml = yaml.toString();
    }

    private static String nodeId(int n) {
        return "floci-node-" + String.format("%016x", n);
    }

    private static NodeRecord persisted(int n) {
        return new NodeRecord(nodeId(0x100 + n), 6550 + n, 6650 + n, "none", DIGEST, false, List.of(),
                "123456789012", "eu-west-1", FlociNode.nodeName(nodeId(0x100 + n), "eu-west-1"),
                FlociNode.DEFAULT_SERVICE_CIDR, FlociNode.DEFAULT_POD_CIDR, Map.of(), List.of());
    }

    private static Machine machine(NodeRecord record, String state, Map<String, String> owner) {
        Map<String, String> labels = new LinkedHashMap<>(owner);
        labels.put(NodeDriver.LABEL_API_PORT, String.valueOf(record.apiPort()));
        labels.put(NodeDriver.LABEL_SERVICE_PORT, String.valueOf(record.servicePort()));
        labels.put(NodeDriver.LABEL_NODE_PORTS, record.nodePorts());
        return new Machine(record.nodeId(), state, labels);
    }

    private void awaitReady() {
        await().atMost(5, TimeUnit.SECONDS).until(() -> flociNode.status().state() == State.READY);
    }

    private List<String> commands() {
        return driver.calls().stream().map(Call::command).filter(command -> !command.equals("status")).toList();
    }

    @Test
    void aFirstUseCreatesTheNodeAndReturnsAtOnce() throws Exception {
        CountDownLatch create = driver.holdCreates();

        Status status = flociNode.acquire(RDS);

        assertEquals(State.CREATING, status.state());
        NodeRecord record = status.record();
        assertEquals(nodeId(1), record.nodeId());
        assertEquals(6501, record.apiPort());
        assertEquals(6601, record.servicePort());
        assertEquals("32080-32099", record.nodePorts());
        assertEquals(DIGEST, record.imageSha256());
        assertEquals("123456789012", record.accountId());
        assertEquals("eu-west-1", record.region());
        assertThat(record.nodeName(), matchesPattern(
                "i-[0-9a-f]{17}\\.eu-west-1\\.compute\\.internal"));
        assertEquals("10.100.0.0/16", record.serviceCidr());
        assertEquals("10.42.0.0/16", record.podCidr());
        verify(seed).write(record);
        assertEquals(record, store.record.get(), "the record persists");
        List<SmolvmNode> listed = flociNode.nodes();
        assertEquals(1, listed.size());
        assertEquals(FlociNode.NAME, listed.getFirst().name());
        assertEquals(URI.create("http://127.0.0.1:6601"), listed.getFirst().baseUrl());
        assertEquals(SmolvmNode.Kind.CREATED, listed.getFirst().kind());
        CompletableFuture<SmolvmNode> ready = flociNode.whenReady();
        assertFalse(ready.isDone());
        assertTrue(changes.get() > 0, "the directory was told");

        create.countDown();

        assertEquals(FlociNode.NAME, ready.get(5, TimeUnit.SECONDS).name());
        awaitReady();
        assertEquals(List.of("create"), commands());
        assertEquals(Optional.of(flociNode.nodes().getFirst()), flociNode.node());
        verify(readyRule, atLeastOnce()).unmetCondition(any(), eq(record.nodeName()),
                any(), any());
    }

    @Test
    void anEksClusterAsTheFirstUseGivesTheNodeItsRangesAndArguments() {
        NodeRecord record = flociNode.acquire(EKS).record();

        assertEquals("us-east-1", record.region());
        assertThat(record.nodeName(), endsWith(".ec2.internal"));
        assertEquals("172.20.0.0/16", record.serviceCidr());
        assertEquals(Map.of(NodeSeed.KUBELET_ARGS, List.of("max-pods=250")), record.k3sArgs());
    }

    @Test
    void aLaterUseTakesTheNodeAsItIs() {
        NodeRecord created = flociNode.acquire(RDS).record();
        awaitReady();

        Status later = flociNode.acquire(EKS);

        assertEquals(State.READY, later.state());
        assertEquals(created, later.record());
        assertEquals("10.100.0.0/16", later.record().serviceCidr(), "the first use made the node's k3s");
        assertEquals(1, driver.calls("create").size());
        verify(ports, times(1)).allocate();
    }

    @Test
    void concurrentFirstUsesCreateOneNode() throws Exception {
        CountDownLatch create = driver.holdCreates();
        ExecutorService callers = Executors.newFixedThreadPool(8);
        try {
            CountDownLatch go = new CountDownLatch(1);
            List<CompletableFuture<Status>> uses = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                Use use = i % 2 == 0 ? RDS : EKS;
                uses.add(CompletableFuture.supplyAsync(() -> {
                    try {
                        go.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return flociNode.acquire(use);
                }, callers));
            }
            go.countDown();
            for (CompletableFuture<Status> use : uses) {
                assertEquals(nodeId(1), use.get(5, TimeUnit.SECONDS).record().nodeId());
            }
        } finally {
            callers.shutdownNow();
        }
        create.countDown();
        awaitReady();
        assertEquals(1, driver.calls("create").size());
        assertEquals(1, allocations.get());
    }

    @Test
    void theNodeOutlivesItsUsers() {
        flociNode.acquire(RDS);
        awaitReady();
        flociNode.acquire(EKS);

        flociNode.reconcile();

        assertEquals(State.READY, flociNode.status().state());
        assertTrue(driver.machine(nodeId(1)).orElseThrow().running());
        assertEquals(List.of("create"), commands());
        verify(seed, never()).delete(anyString());
    }

    @Test
    void theRuntimeFilesOnlyNodeIsTheNodeAndFlociCreatesNone() throws IOException {
        declare("dev");

        Status status = flociNode.acquire(RDS);

        assertEquals(State.READY, status.state());
        assertTrue(status.declared());
        assertEquals("node:dev", status.node().name());
        assertNull(status.record());
        assertTrue(flociNode.whenReady().isDone());
        assertTrue(driver.calls().isEmpty());
        verify(ports, never()).allocate();
        assertTrue(flociNode.nodes().isEmpty(), "the directory lists the runtime file's node itself");
    }

    @Test
    void theSettingNamesOneOfSeveralRuntimeFileNodes() throws IOException {
        declare("dev", "ci");

        NodeUnavailableException several = assertThrows(NodeUnavailableException.class, () -> flociNode.acquire(RDS));
        assertThat(several.getMessage(), containsString("lists 2 nodes (dev, ci)"));
        assertThat(several.getMessage(), containsString("floci.runtime.node.name (FLOCI_RUNTIME_NODE_NAME)"));

        when(node.name()).thenReturn(Optional.of("ci"));
        assertEquals("node:ci", flociNode.acquire(RDS).node().name());
        when(node.name()).thenReturn(Optional.of("node:dev"));
        assertEquals("node:dev", flociNode.acquire(EKS).node().name());

        when(node.name()).thenReturn(Optional.of("gone"));
        NodeUnavailableException unknown = assertThrows(NodeUnavailableException.class, () -> flociNode.acquire(RDS));
        assertThat(unknown.getMessage(), startsWith("The runtime file has no node gone"));
        assertEquals(State.FAILED, flociNode.status().state());
        assertTrue(driver.calls().isEmpty());
    }

    @Test
    void aFirstUseRefusesWhenTheSettingsHaveProblems() {
        when(settings.problems()).thenReturn(List.of("floci.runtime.node.image is not set"));

        NodeUnavailableException refused = assertThrows(NodeUnavailableException.class, () -> flociNode.acquire(RDS));

        assertThat(refused.getMessage(), containsString("for RDS instance orders: floci.runtime.node.image is not set"));
        assertEquals(State.NONE, flociNode.status().state());
        verify(ports, never()).allocate();
        assertTrue(commands().isEmpty());
        assertInstanceOf(NodeUnavailableException.class,
                assertThrows(ExecutionException.class, () -> flociNode.whenReady().get()).getCause());
    }

    @Test
    void aSeedThatCannotBeWrittenReleasesThePorts() {
        doThrow(new UncheckedIOException(new IOException("disk full"))).when(seed).write(any(NodeRecord.class));

        assertThrows(UncheckedIOException.class, () -> flociNode.acquire(RDS));

        verify(ports).release(any(NodeRecord.class));
        assertEquals(State.NONE, flociNode.status().state());
        assertNull(store.record.get());
        assertTrue(driver.calls("create").isEmpty());
    }

    @Test
    void aFailedCreationFailsTheWaitersAndTheNextUseCreatesTheNodeAgain() throws Exception {
        driver.failCreates("cannot publish host TCP 127.0.0.1:6501 to guest TCP 6443");

        flociNode.acquire(RDS);
        CompletableFuture<SmolvmNode> ready = flociNode.whenReady();

        ExecutionException failed = assertThrows(ExecutionException.class, () -> ready.get(5, TimeUnit.SECONDS));
        assertThat(failed.getCause().getMessage(), startsWith("Could not create smolvm-pods node " + nodeId(1)
                + ": node-vm.sh create"));
        Status status = flociNode.status();
        assertEquals(State.FAILED, status.state());
        assertThat(status.failure(), startsWith("Could not create smolvm-pods node"));

        driver.failCreates(null);
        Status again = flociNode.acquire(EKS);

        assertEquals(nodeId(2), again.record().nodeId());
        awaitReady();
        verify(ports).release(any(NodeRecord.class));
        verify(seed).delete(nodeId(1));
        assertEquals(nodeId(2), store.record.get().nodeId());
    }

    @Test
    void theNodeIsReadyOnlyWhenTheReadyRuleHolds() {
        AtomicReference<String> unmet = new AtomicReference<>("node i-1 has not registered");
        doAnswer(call -> Optional.ofNullable(unmet.get())).when(readyRule)
                .unmetCondition(any(), anyString(), any(), any());

        flociNode.acquire(RDS);

        await().during(200, TimeUnit.MILLISECONDS).atMost(2, TimeUnit.SECONDS)
                .until(() -> flociNode.status().state() == State.CREATING);
        unmet.set(null);
        awaitReady();
    }

    @Test
    void aNodeThatIsNotReadyInTimeFails() {
        when(node.readyTimeoutSeconds()).thenReturn(0);
        doReturn(Optional.of("RuntimeClass smolvm-pods does not exist")).when(readyRule)
                .unmetCondition(any(), anyString(), any(), any());

        flociNode.acquire(RDS);

        await().atMost(5, TimeUnit.SECONDS).until(() -> flociNode.status().state() == State.FAILED);
        assertThat(flociNode.status().failure(), containsString("was not ready within 0 s: RuntimeClass smolvm-pods "
                + "does not exist"));
    }

    @Test
    void anIsolatedNodeGetsTheAllowListOfTheRuntimeFile() {
        isolation = new Isolation(true, List.of("203.0.113.10/32"));
        NodeRecord record = flociNode.acquire(RDS).record();
        assertTrue(record.isolated());
        assertEquals(List.of("203.0.113.10/32"), record.allowCidrs());
        awaitReady();

        isolation = new Isolation(true, List.of("198.51.100.0/24"));
        runtimeFileListener.accept(runtimeFile());

        await().atMost(5, TimeUnit.SECONDS).until(() -> driver.calls("update").size() == 1);
        assertEquals(List.of("allow-cidr=198.51.100.0/24", "remove-allow-cidr=203.0.113.10/32"),
                driver.calls("update").getFirst().args());
        await().atMost(5, TimeUnit.SECONDS).until(() -> flociNode.record().orElseThrow().allowCidrs()
                .equals(List.of("198.51.100.0/24")));
        assertEquals(List.of("198.51.100.0/24"), store.record.get().allowCidrs());
    }

    @Test
    void restoreAdoptsARunningNode() {
        NodeRecord record = persisted(1);
        store.save(record);
        driver.addMachine(machine(record, "running", OWNER_LABELS));

        flociNode.onStart(null);

        awaitReady();
        verify(ports).reserve(record);
        verify(seed).writeEnv(record.nodeId());
        verify(seed, never()).write(any(NodeRecord.class));
        assertEquals(List.of(), commands());
        assertEquals(record, flociNode.acquire(RDS).record());
    }

    @Test
    void restoreStartsAStoppedNode() {
        NodeRecord record = persisted(1);
        store.save(record);
        driver.addMachine(machine(record, "stopped", OWNER_LABELS));

        flociNode.onStart(null);

        awaitReady();
        verify(seed).write(record);
        assertEquals(List.of("start"), commands());
        assertTrue(driver.machine(record.nodeId()).orElseThrow().running());
    }

    @Test
    void theRecordNamesTheContributorsOfTheSeedThatTheNodeBootedWith() {
        when(seed.write(any(NodeRecord.class))).thenReturn(List.of("eks"));
        assertEquals(List.of("eks"), flociNode.acquire(RDS).record().seeded());
        assertEquals(List.of("eks"), store.record.get().seeded());

        NodeRecord running = persisted(1);
        store.save(running);
        driver.addMachine(machine(running, "running", OWNER_LABELS));
        FlociNode adopting = flociNode(Duration.ofSeconds(5));
        adopting.onStart(null);
        await().atMost(5, TimeUnit.SECONDS).until(() -> adopting.status().state() == State.READY);
        assertEquals(List.of(), adopting.record().orElseThrow().seeded(), "a running node keeps the seed it booted with");

        NodeRecord stopped = persisted(2);
        store.save(stopped);
        driver.addMachine(machine(stopped, "stopped", OWNER_LABELS));
        FlociNode starting = flociNode(Duration.ofSeconds(5));
        starting.onStart(null);
        await().atMost(5, TimeUnit.SECONDS).until(() -> starting.status().state() == State.READY);
        assertEquals(List.of("eks"), starting.record().orElseThrow().seeded(), "a stopped node boots with the seed");
        assertEquals(List.of("eks"), store.record.get().seeded());
    }

    @Test
    void restoreCreatesAMissingNodeWithItsPorts() {
        NodeRecord record = persisted(1);
        store.save(record);

        flociNode.onStart(null);

        awaitReady();
        verify(kubeApi).forget(record.nodeId());
        assertEquals(List.of("create"), commands());
        assertThat(String.join(" ", driver.calls("create").getFirst().args()), containsString("api-port=6551"));
    }

    @Test
    void aFirstUseBeforeTheStartRestoresThePersistedNode() {
        NodeRecord record = persisted(1);
        store.save(record);
        driver.addMachine(machine(record, "running", OWNER_LABELS));

        assertEquals(record, flociNode.acquire(RDS).record());

        awaitReady();
        assertEquals(List.of(), commands());
        verify(ports, never()).allocate();
    }

    @Test
    void orphansAreDeletedOnlyWhenBothLabelsMatchAndTheRecordDoesNotNameThem() {
        NodeRecord named = persisted(1);
        NodeRecord orphan = persisted(2);
        NodeRecord otherOwner = persisted(3);
        NodeRecord otherData = persisted(4);
        store.save(named);
        driver.addMachine(machine(named, "running", OWNER_LABELS));
        driver.addMachine(machine(orphan, "stopped", OWNER_LABELS));
        driver.addMachine(machine(otherOwner, "running", Map.of(NodeDriver.LABEL_OWNER, "ci",
                NodeDriver.LABEL_DATA, "0123456789ab")));
        driver.addMachine(machine(otherData, "running", Map.of(NodeDriver.LABEL_OWNER, "",
                NodeDriver.LABEL_DATA, "ba9876543210")));
        driver.addMachine(new Machine("smolvm-pods-node", "running", OWNER_LABELS));

        flociNode.onStart(null);

        await().atMost(5, TimeUnit.SECONDS).until(() -> !driver.calls("delete").isEmpty());
        awaitReady();
        assertEquals(List.of(new Call("delete", orphan.nodeId(), List.of())), driver.calls("delete"));
        verify(seed).delete(orphan.nodeId());
        verify(seed, never()).delete(named.nodeId());
    }

    @Test
    void withoutARecordTheStartDeletesTheMachinesThisFlociCreated() {
        NodeRecord unrecorded = persisted(1);
        driver.addMachine(machine(unrecorded, "running", OWNER_LABELS));

        flociNode.onStart(null);

        await().atMost(5, TimeUnit.SECONDS).until(() -> !driver.calls("delete").isEmpty());
        assertEquals(List.of(new Call("delete", unrecorded.nodeId(), List.of())), driver.calls("delete"));
        verify(seed).delete(unrecorded.nodeId());
        assertEquals(State.NONE, flociNode.status().state());
    }

    @Test
    void aResetDuringTheCreationDeletesTheNodeAfterItAndTheCreationStopsEarly() {
        flociNode = flociNode(Duration.ofMillis(100));
        CountDownLatch create = driver.holdCreates();
        NodeRecord record = flociNode.acquire(RDS).record();
        await().atMost(5, TimeUnit.SECONDS).until(() -> driver.calls("create").size() == 1);

        flociNode.clear();

        assertTrue(flociNode.nodes().isEmpty());
        assertTrue(driver.calls("delete").isEmpty(), "the deletion waits for the creation");
        verify(ports, never()).release(any());
        create.countDown();
        await().atMost(5, TimeUnit.SECONDS).until(() -> driver.machine(record.nodeId()).isEmpty());
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(seed).delete(record.nodeId()));
        verify(ports).release(record);
        verify(readyRule, never()).unmetCondition(any(), anyString(), any(), any());
    }

    @Test
    void aStopThatDeletesTheNodeDeletesItAfterItsCreation() {
        flociNode = flociNode(Duration.ofMillis(100));
        CountDownLatch create = driver.holdCreates();
        NodeRecord record = flociNode.acquire(RDS).record();
        await().atMost(5, TimeUnit.SECONDS).until(() -> driver.calls("create").size() == 1);
        storageMode = "memory";

        flociNode.onStop(null);

        assertTrue(driver.calls("delete").isEmpty(), "the deletion waits for the creation");
        create.countDown();
        await().atMost(5, TimeUnit.SECONDS).until(() -> driver.machine(record.nodeId()).isEmpty());
        await().atMost(5, TimeUnit.SECONDS).until(() -> store.record.get() == null);
        verify(readyRule, never()).unmetCondition(any(), anyString(), any(), any());
    }

    @Test
    void withNoServiceOnTheNodeTheStartTouchesNothing() {
        when(settings.wanted()).thenReturn(false);
        store.save(persisted(1));

        flociNode.onStart(null);

        assertTrue(driver.calls().isEmpty());
        assertEquals(State.NONE, flociNode.status().state());
    }

    @Test
    void aResetDeletesTheNodeOnceItsWorkIsDone() throws Exception {
        CountDownLatch create = driver.holdCreates();
        NodeRecord record = flociNode.acquire(RDS).record();
        CompletableFuture<SmolvmNode> ready = flociNode.whenReady();
        await().atMost(5, TimeUnit.SECONDS).until(() -> driver.calls("create").size() == 1);

        CompletableFuture<Void> reset = CompletableFuture.runAsync(flociNode::clear);

        await().atMost(2, TimeUnit.SECONDS).until(() -> flociNode.nodes().isEmpty());
        create.countDown();
        reset.get(5, TimeUnit.SECONDS);
        assertTrue(driver.machine(record.nodeId()).isEmpty());
        verify(kubeApi).forget(record.nodeId());
        verify(ports).release(record);
        verify(seed).delete(record.nodeId());
        assertNull(store.record.get());
        assertEquals(State.NONE, flociNode.status().state());
        assertTrue(ready.isDone());

        assertEquals(nodeId(2), flociNode.acquire(RDS).record().nodeId(), "the next use creates a node");
    }

    @Test
    void aResetThatCannotDeleteTheMachineKeepsItsPortsAndSeedForTheNextStart() {
        NodeRecord record = flociNode.acquire(RDS).record();
        awaitReady();
        driver.failDeletes(record.nodeId(), "smolvm could not stop the machine");

        flociNode.clear();

        assertTrue(flociNode.nodes().isEmpty());
        assertTrue(driver.machine(record.nodeId()).isPresent());
        verify(ports, never()).release(record);
        verify(seed, never()).delete(record.nodeId());
    }

    @Test
    void theShutdownActionFollowsTheStorageModeAndTheKeepRunningSetting() {
        storageMode = "memory";
        assertEquals(ShutdownAction.DELETE, flociNode.shutdownAction());

        storageMode = "persistent";
        assertEquals(ShutdownAction.LEAVE, flociNode.shutdownAction());

        when(node.keepRunningOnShutdown()).thenReturn(false);
        assertEquals(ShutdownAction.STOP, flociNode.shutdownAction());
    }

    @Test
    void flociStopLeavesStopsOrDeletesTheNode() {
        NodeRecord record = flociNode.acquire(RDS).record();
        awaitReady();

        storageMode = "persistent";
        flociNode.onStop(null);
        assertEquals("running", driver.machine(record.nodeId()).orElseThrow().state());

        when(node.keepRunningOnShutdown()).thenReturn(false);
        flociNode.onStop(null);
        assertEquals("stopped", driver.machine(record.nodeId()).orElseThrow().state());

        storageMode = "memory";
        flociNode.onStop(null);
        assertTrue(driver.machine(record.nodeId()).isEmpty());
        verify(seed).delete(record.nodeId());
        assertNull(store.record.get());
    }

    @Test
    void aNodeStoppedFromOutsideIsLeftSo() {
        NodeRecord record = flociNode.acquire(RDS).record();
        awaitReady();

        driver.state(record.nodeId(), "stopped");
        flociNode.reconcile();
        driver.removeMachine(record.nodeId());
        flociNode.reconcile();

        assertEquals(List.of("create"), commands());
        assertSame(State.READY, flociNode.status().state());
    }

    @Test
    void theNodeNameComesFromTheNodeIdAndTheRegion() {
        assertEquals(FlociNode.nodeName(nodeId(1), "us-east-1"), FlociNode.nodeName(nodeId(1), "us-east-1"));
        assertFalse(FlociNode.nodeName(nodeId(1), "us-east-1").equals(FlociNode.nodeName(nodeId(2), "us-east-1")));
        assertThat(FlociNode.nodeName(nodeId(1), "us-east-1"),
                matchesPattern("i-[0-9a-f]{17}\\.ec2\\.internal"));
    }

    @Test
    void theReadyMessageSaysWhereTheNodeIsAndHowToReachItsDocker() {
        NodeRecord record = TestNodeRecords.record(nodeId(1), 6500, 6600, "32080-32099", DIGEST, false, List.of());

        String message = FlociNode.readyMessage(record, Path.of("/data/runtime-node", nodeId(1), "seed"),
                "/src/floci/tools/smolvm-pods/scripts/node-vm.sh", "/src/my smolvm");

        assertEquals("smolvm-pods node " + nodeId(1) + " is ready: Kubernetes API https://127.0.0.1:6500, "
                + "node API http://127.0.0.1:6600, admin UI http://127.0.0.1:6600/ui, seed and token in "
                + "/data/runtime-node/" + nodeId(1) + "/seed. Docker CLI: eval \"$(SMOLVM_SRC='/src/my smolvm' "
                + "/src/floci/tools/smolvm-pods/scripts/node-vm.sh docker-env --name " + nodeId(1) + ")\"", message);
    }
}
