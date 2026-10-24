package io.github.hectorvent.floci.runtime.flocinode;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.RuntimeConfig.NodeConfig;
import io.github.hectorvent.floci.core.common.AwsRegions;
import io.github.hectorvent.floci.core.common.Resettable;
import io.github.hectorvent.floci.core.common.ServiceConfigAccess;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.runtime.flocinode.NodeDriver.Machine;
import io.github.hectorvent.floci.runtime.flocinode.NodeDriver.NodeCommandException;
import io.github.hectorvent.floci.runtime.flocinode.NodeIsolation.Isolation;
import io.github.hectorvent.floci.runtime.node.NodesSection;
import io.github.hectorvent.floci.runtime.node.RuntimeFile;
import io.github.hectorvent.floci.runtime.node.RuntimeFileWatcher;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeDirectory;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeSource;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import org.jboss.logging.Logger;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The one smolvm-pods node of Floci, which RDS and EKS share. When the runtime file lists nodes,
 * the one {@code floci.runtime.node.name} names, or the only one, is it, and Floci neither creates
 * nor deletes it. Otherwise Floci creates a node on the first use, by whichever comes first: an RDS
 * create or an EKS {@code CreateCluster}, each with smolvm on. Concurrent first uses create one
 * node. The node is listed in {@link SmolvmNodeDirectory} as {@value #NAME} while it is created
 * and runs.
 *
 * <p>The first use checks the settings, allocates the node's Mac ports, writes its seed and
 * returns at once; a worker creates and starts the node with {@code node-vm.sh} and waits for
 * {@link NodeReadyRule}. The work on the node runs in order on its queue. Users wait for
 * {@link #whenReady()}, and read {@link #status()}.
 *
 * <p>The node outlives its users: deleting the last database or the EKS cluster keeps it, since
 * creating one is the slow part. Floci persists the node's record. At a start, the node is adopted
 * when it runs, started when it is stopped, and created again with the same ports when it is gone;
 * a machine that this Floci created, by its owner and data labels, and that the record does not
 * name is deleted: nothing of Floci's state reaches it any more. An emulator
 * reset deletes the node, and so does Floci's stop with {@code memory} storage, since nothing of
 * Floci's state then names it; both run after the work queued on the node.
 */
@ApplicationScoped
public class FlociNode implements SmolvmNodeSource, Resettable {

    private static final Logger LOG = Logger.getLogger(FlociNode.class);

    /** The node's name in the node directory, for a node that Floci created. */
    public static final String NAME = "floci";

    /** The storage key of the node's record, whose storage mode decides what Floci's stop does. */
    static final String STORAGE_KEY = "runtime-node";
    private static final String RECORD_KEY = "node";

    /** How long a reset and Floci's stop wait for the work queued on the node, such as its creation. */
    static final Duration QUEUE_WAIT = Duration.ofSeconds(15);

    /** How often the running node is checked against the runtime file and its machine. */
    static final Duration RECONCILE_INTERVAL = Duration.ofSeconds(10);

    /** How often the ready rule is checked while the node becomes ready. */
    static final Duration READY_POLL = Duration.ofSeconds(2);

    /** k3s's own Service and pod ranges, which a node gets when its first use names none. */
    public static final String DEFAULT_SERVICE_CIDR = "10.100.0.0/16";
    public static final String DEFAULT_POD_CIDR = "10.42.0.0/16";

    /** What happens to the node when Floci stops. */
    enum ShutdownAction {
        /** It keeps running, and its pods keep their memory. */
        LEAVE,
        /** It stops; its disks stay, and every pod starts again cold. */
        STOP,
        /** It is deleted, with its seed. */
        DELETE
    }

    /** Where the node is. */
    public enum State {
        /** Floci has created none, and the runtime file lists none. */
        NONE,
        /** Floci creates, starts or adopts it, and it is not ready yet. */
        CREATING,
        /** It is ready for RDS's databases and an EKS cluster. */
        READY,
        /** Its creation or restoration failed; the next use creates it again. */
        FAILED
    }

    /**
     * The node as it is now.
     *
     * @param node     the node's service, when it is listed
     * @param record   the record of a node that Floci created, else null
     * @param failure  why it failed, when it did
     * @param declared whether the runtime file declares it
     */
    public record Status(State state, SmolvmNode node, NodeRecord record, String failure, boolean declared) {
    }

    /**
     * What a first use asks of the node it creates; a later use takes the node as it is.
     *
     * @param user        who uses it, for the logs, such as {@code RDS instance orders}
     * @param accountId   the account that names the node's EC2 identity
     * @param region      the region that names the node in Kubernetes
     * @param serviceCidr k3s's Service range, or null for {@link #DEFAULT_SERVICE_CIDR}
     * @param podCidr     k3s's pod range, or null for {@link #DEFAULT_POD_CIDR}
     * @param k3sArgs     more k3s settings, by setting, such as {@code kubelet-arg+}
     */
    public record Use(String user, String accountId, String region, String serviceCidr, String podCidr,
                      Map<String, List<String>> k3sArgs) {

        public Use {
            k3sArgs = k3sArgs == null ? Map.of() : Map.copyOf(k3sArgs);
        }

        public static Use of(String user, String accountId, String region) {
            return new Use(user, accountId, region, null, null, Map.of());
        }
    }

    /** The node cannot be used now; the message says why and what to set. */
    public static final class NodeUnavailableException extends IllegalStateException {

        public NodeUnavailableException(String message) {
            super(message);
        }
    }

    /** Where the node's record persists. */
    interface RecordStore {

        Optional<NodeRecord> load();

        void save(NodeRecord record);

        void delete();
    }

    private final EmulatorConfig config;
    private final NodeSettings settings;
    private final NodeSeed seed;
    private final NodePorts ports;
    private final NodeDriver driver;
    private final NodeKubeApi kubeApi;
    private final NodeReadyRule readyRule;
    private final RuntimeFileWatcher runtimeFile;
    private final NodesSection nodesSection;
    private final Function<RuntimeFile, Isolation> isolation;
    private final Runnable nodesChanged;
    private final Supplier<String> storageMode;
    private final RecordStore store;
    private final Executor executor;
    private final Duration queueWait;
    private final Duration readyPoll;
    private final AtomicBoolean loggedIsolation = new AtomicBoolean();
    private volatile Node current;
    private volatile boolean machinesLoaded;
    private boolean restored;
    private ScheduledExecutorService reconciler;

    @Inject
    public FlociNode(EmulatorConfig config, NodeSettings settings, NodeSeed seed, NodePorts ports, NodeDriver driver,
                     NodeKubeApi kubeApi, NodeReadyRule readyRule, RuntimeFileWatcher runtimeFile,
                     NodesSection nodesSection, NodeIsolation isolation, SmolvmNodeDirectory directory,
                     ServiceConfigAccess serviceConfig, StorageFactory storageFactory) {
        this(config, settings, seed, ports, driver, kubeApi, readyRule, runtimeFile, nodesSection, isolation::of,
                directory::refresh, () -> serviceConfig.storageMode(STORAGE_KEY),
                store(storageFactory.create(STORAGE_KEY, "runtime-node.json",
                        new TypeReference<Map<String, NodeRecord>>() { }), config.defaultAccountId()),
                Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("floci-node-", 0).factory()),
                QUEUE_WAIT, READY_POLL);
    }

    /**
     * @param isolation    the isolation of a new node and the allow list of an isolated one, from a runtime file
     * @param nodesChanged tells the node directory that the node changed
     * @param storageMode  the storage mode of the node's record
     * @param executor     runs the work queued on the node
     * @param queueWait    how long a reset or a stop waits for the node's queue
     * @param readyPoll    how often the ready rule is checked
     */
    FlociNode(EmulatorConfig config, NodeSettings settings, NodeSeed seed, NodePorts ports, NodeDriver driver,
              NodeKubeApi kubeApi, NodeReadyRule readyRule, RuntimeFileWatcher runtimeFile, NodesSection nodesSection,
              Function<RuntimeFile, Isolation> isolation, Runnable nodesChanged, Supplier<String> storageMode,
              RecordStore store, Executor executor, Duration queueWait, Duration readyPoll) {
        this.config = config;
        this.settings = settings;
        this.seed = seed;
        this.ports = ports;
        this.driver = driver;
        this.kubeApi = kubeApi;
        this.readyRule = readyRule;
        this.runtimeFile = runtimeFile;
        this.nodesSection = nodesSection;
        this.isolation = isolation;
        this.nodesChanged = nodesChanged;
        this.storageMode = storageMode;
        this.store = store;
        this.executor = executor;
        this.queueWait = queueWait;
        this.readyPoll = readyPoll;
        runtimeFile.addListener(this::syncAllowList);
    }

    private static RecordStore store(AccountAwareStorageBackend<NodeRecord> storage, String accountId) {
        return new RecordStore() {
            @Override
            public Optional<NodeRecord> load() {
                return storage.getForAccount(accountId, RECORD_KEY);
            }

            @Override
            public void save(NodeRecord record) {
                storage.putForAccount(accountId, RECORD_KEY, record);
            }

            @Override
            public void delete() {
                storage.deleteForAccount(accountId, RECORD_KEY);
            }
        };
    }

    /**
     * At Floci's start, when a service runs on the node and Floci can run {@code node-vm.sh}:
     * restores the persisted node and deletes the machines that this Floci created and the record
     * does not name.
     */
    void onStart(@Observes @Priority(Interceptor.Priority.APPLICATION + 550) StartupEvent ignored) {
        if (settings.wanted()) {
            restore();
        }
    }

    /** Late, so that RDS and EKS stop their containers on the node before it goes. */
    void onStop(@Observes @Priority(Interceptor.Priority.LIBRARY_AFTER + 900) ShutdownEvent ignored) {
        stopReconciler();
        Node node = current;
        if (node != null) {
            shutdown(node, shutdownAction());
        }
    }

    /**
     * The node for a use: the runtime file's, or the one Floci created. Without either, starts
     * the creation of a node for {@code use}, and returns at once. A node that failed is created
     * again.
     *
     * @throws NodeUnavailableException when the runtime file names no usable node, or the
     *         settings or the Mac keep Floci from creating one
     */
    public Status acquire(Use use) {
        Optional<SmolvmNode> declared = declared();
        if (declared.isPresent()) {
            return new Status(State.READY, declared.get(), null, null, true);
        }
        synchronized (this) {
            restore();
            Node existing = current;
            if (existing != null && existing.failure == null && !existing.deleting) {
                return status(existing);
            }
            List<String> problems = settings.problems();
            if (!problems.isEmpty()) {
                throw new NodeUnavailableException("Floci cannot create the smolvm-pods node for " + use.user() + ": "
                        + String.join("; ", problems));
            }
            loadMachines();
            Isolation wanted = isolation.apply(runtimeFile.current());
            NodePorts.Ports allocated = ports.allocate();
            String nodeId = seed.newNodeId();
            String region = use.region() == null || use.region().isBlank() ? config.defaultRegion() : use.region();
            String accountId = use.accountId() == null || use.accountId().isBlank()
                    ? config.defaultAccountId() : use.accountId();
            NodeRecord allocatedRecord = new NodeRecord(nodeId, allocated.apiPort(), allocated.servicePort(),
                    allocated.nodePorts(), imageSha256(), wanted.enabled(),
                    wanted.enabled() ? wanted.allowCidrs() : List.of(), accountId, region, nodeName(nodeId, region),
                    blankTo(use.serviceCidr(), DEFAULT_SERVICE_CIDR), blankTo(use.podCidr(), DEFAULT_POD_CIDR),
                    use.k3sArgs(), List.of());
            NodeRecord record;
            try {
                record = allocatedRecord.withSeeded(seed.write(allocatedRecord));
            } catch (RuntimeException e) {
                ports.release(allocatedRecord);
                throw e;
            }
            store.save(record);
            Node node = new Node(record, new NodeQueue(executor));
            current = node;
            if (existing != null) {
                node.queue.submit(() -> retire(existing));
            }
            startReconciler();
            nodesChanged.run();
            LOG.infov("Creating smolvm-pods node {0} for {1}: API port {2}, node service port {3}, NodePorts {4}, "
                    + "isolated {5}", nodeId, use.user(), String.valueOf(record.apiPort()),
                    String.valueOf(record.servicePort()), record.nodePorts(), record.isolated());
            node.queue.submit(() -> createNode(node));
            return status(node);
        }
    }

    /** The node as it is now; never creates one. */
    public Status status() {
        Optional<SmolvmNode> declared;
        try {
            declared = declared();
        } catch (NodeUnavailableException e) {
            return new Status(State.FAILED, null, null, e.getMessage(), true);
        }
        if (declared.isPresent()) {
            return new Status(State.READY, declared.get(), null, null, true);
        }
        Node node = current;
        return node == null ? new Status(State.NONE, null, null, null, false) : status(node);
    }

    private Status status(Node node) {
        if (node.failure != null) {
            return new Status(State.FAILED, null, node.record, node.failure, false);
        }
        if (node.deleting) {
            return new Status(State.NONE, null, null, null, false);
        }
        return new Status(node.ready ? State.READY : State.CREATING, smolvmNode(node.record), node.record, null,
                false);
    }

    /** The node, once it is ready. */
    public Optional<SmolvmNode> node() {
        Status status = status();
        return status.state() == State.READY ? Optional.ofNullable(status.node()) : Optional.empty();
    }

    /** The record of the node that Floci created, in any state. */
    public Optional<NodeRecord> record() {
        Node node = current;
        return node == null || node.deleting ? Optional.empty() : Optional.of(node.record);
    }

    /**
     * Completes once the node is ready, with the node; exceptionally with a
     * {@link NodeUnavailableException} when it fails or goes before. Without a node, fails at once.
     */
    public CompletableFuture<SmolvmNode> whenReady() {
        Optional<SmolvmNode> declared;
        try {
            declared = declared();
        } catch (NodeUnavailableException e) {
            return CompletableFuture.failedFuture(e);
        }
        if (declared.isPresent()) {
            return CompletableFuture.completedFuture(declared.get());
        }
        Node node = current;
        if (node == null) {
            return CompletableFuture.failedFuture(new NodeUnavailableException("Floci has no smolvm-pods node"));
        }
        return node.readiness.copy();
    }

    /**
     * The runtime file's node: the one {@code floci.runtime.node.name} names, or the only one; empty
     * when the file lists none.
     *
     * @throws NodeUnavailableException when the file names no such node, or lists several and the
     *         setting names none
     */
    Optional<SmolvmNode> declared() {
        Map<String, SmolvmNode> nodes = runtimeFile.current().section(nodesSection);
        if (nodes.isEmpty()) {
            return Optional.empty();
        }
        Optional<String> name = config.runtime().node().name().map(String::strip).filter(value -> !value.isEmpty());
        if (name.isPresent()) {
            String key = name.get().startsWith("node:") ? name.get().substring("node:".length()) : name.get();
            SmolvmNode node = nodes.get(key);
            if (node == null) {
                throw new NodeUnavailableException("The runtime file has no node " + key + " that "
                        + NodeSettings.key("name") + " names; it lists " + String.join(", ", nodes.keySet()));
            }
            return Optional.of(node);
        }
        if (nodes.size() > 1) {
            throw new NodeUnavailableException("The runtime file lists " + nodes.size() + " nodes ("
                    + String.join(", ", nodes.keySet()) + "); name the one for RDS and EKS with "
                    + NodeSettings.key("name"));
        }
        return Optional.of(nodes.values().iterator().next());
    }

    /** Whether Floci can run {@code node-vm.sh}: the script is set. */
    boolean managesMachines() {
        return config.runtime().node().nodeVmScript().filter(path -> !path.isBlank()).isPresent();
    }

    /**
     * Restores the persisted node, once: queues its adoption when it runs, its start when it is
     * stopped, and its creation with the same ports when it is gone, and deletes in the background
     * every machine that this Floci created and the record does not name.
     */
    private synchronized void restore() {
        if (restored) {
            return;
        }
        restored = true;
        if (!managesMachines()) {
            return;
        }
        try {
            loadMachines();
        } catch (RuntimeException e) {
            LOG.warnv("Could not list the smolvm-pods nodes on this Mac: {0}", e.getMessage());
        }
        Optional<NodeRecord> record = store.load();
        if (record.isPresent()) {
            ports.reserve(record.get());
            Node node = new Node(record.get(), new NodeQueue(executor));
            current = node;
            startReconciler();
            nodesChanged.run();
            node.queue.submit(() -> restoreNode(node));
        }
        String named = record.map(NodeRecord::nodeId).orElse(null);
        executor.execute(() -> deleteOrphans(named));
    }

    /**
     * Deletes the node on an emulator reset, on its queue after the work queued before, such as
     * its creation, which stops early; waits for the deletion up to the queue wait. The machine,
     * its seed, its ports and its record go. A machine that {@code node-vm.sh} fails to delete
     * keeps its ports and its seed, and Floci deletes it as an orphan at its next start.
     */
    @Override
    public void clear() {
        Node node;
        synchronized (this) {
            node = current;
            current = null;
            store.delete();
            stopReconciler();
        }
        if (node == null) {
            return;
        }
        node.deleting = true;
        node.readiness.completeExceptionally(new NodeUnavailableException("The emulator reset deleted the smolvm-pods "
                + "node"));
        nodesChanged.run();
        node.queue.submit(() -> {
            deleteNode(node, "on the reset");
            nodesChanged.run();
        });
        if (!awaitQueue(node, queueWait)) {
            LOG.warnv("The work on smolvm-pods node {0} did not finish within {1} s of the reset; Floci deletes the "
                    + "node once it does", node.record.nodeId(), String.valueOf(queueWait.toSeconds()));
        }
    }

    /** Deletes the machine of a node that failed and that a new one replaces. */
    private void retire(Node failed) {
        failed.deleting = true;
        deleteNode(failed, "after it failed");
    }

    /**
     * Deletes {@code node}'s machine, then forgets its CA and releases its ports and its seed. A
     * machine that {@code node-vm.sh} fails to delete keeps them.
     *
     * @param when when the node is deleted, for the logs, such as {@code on the reset}
     * @return whether the node was deleted
     */
    private boolean deleteNode(Node node, String when) {
        NodeRecord record = node.record;
        try {
            deleteMachine(record);
            kubeApi.forget(record.nodeId());
            ports.release(record);
            seed.delete(record.nodeId());
            return true;
        } catch (RuntimeException e) {
            LOG.warnv("Could not delete smolvm-pods node {0} {1}; delete it with node-vm.sh delete --name {0}: {2}",
                    record.nodeId(), when, e.getMessage());
            return false;
        }
    }

    /**
     * Deletes the machine of {@code record}, or leaves it with a warning when Floci cannot run
     * {@code node-vm.sh}.
     *
     * @throws NodeCommandException when {@code node-vm.sh} fails to delete it
     */
    private void deleteMachine(NodeRecord record) {
        if (managesMachines()) {
            try {
                if (driver.delete(record.nodeId())) {
                    LOG.infov("Deleted smolvm-pods node {0}", record.nodeId());
                } else {
                    LOG.infov("smolvm-pods node {0} was already gone", record.nodeId());
                }
                return;
            } catch (NodeCommandException e) {
                if (e.ran()) {
                    throw e;
                }
                LOG.warnv("Floci cannot run node-vm.sh to delete smolvm-pods node {0}: {1}", record.nodeId(),
                        e.getMessage());
            }
        }
        LOG.warnv("Leaving smolvm-pods node {0}, since Floci cannot run node-vm.sh: delete it with node-vm.sh delete "
                + "--name {0}, or set {1}", record.nodeId(), NodeSettings.key("node-vm-script"));
    }

    /** What happens to the node when Floci stops, from the storage mode and the keep-running setting. */
    ShutdownAction shutdownAction() {
        if ("memory".equals(storageMode.get())) {
            return ShutdownAction.DELETE;
        }
        return config.runtime().node().keepRunningOnShutdown() ? ShutdownAction.LEAVE : ShutdownAction.STOP;
    }

    /**
     * Leaves, stops or deletes the node as Floci stops. The deletion runs on the node's queue after
     * the work queued before, such as its creation, which stops early.
     */
    void shutdown(Node node, ShutdownAction action) {
        if (action == ShutdownAction.DELETE) {
            node.deleting = true;
            node.queue.submit(() -> {
                if (deleteNode(node, "as Floci stops")) {
                    store.delete();
                    LOG.infov("Deleted smolvm-pods node {0}, since Floci''s state goes with it",
                            node.record.nodeId());
                }
            });
        }
        NodeRecord record = node.record;
        if (!awaitQueue(node, queueWait)) {
            LOG.warnv("The work on smolvm-pods node {0} did not finish within {1} s", record.nodeId(),
                    String.valueOf(queueWait.toSeconds()));
        }
        if (action == ShutdownAction.LEAVE) {
            LOG.infov("Leaving smolvm-pods node {0} running", record.nodeId());
        } else if (action == ShutdownAction.STOP) {
            try {
                driver.stop(record.nodeId());
                LOG.infov("Stopped smolvm-pods node {0}", record.nodeId());
            } catch (RuntimeException e) {
                LOG.warnv("Could not stop smolvm-pods node {0} as Floci stops: {1}", record.nodeId(), e.getMessage());
            }
        }
    }

    /**
     * Deletes every machine that this Floci created, by its owner and data labels, and that is not
     * {@code named}, with its seed. Never any other machine.
     */
    private void deleteOrphans(String named) {
        List<Machine> machines;
        try {
            machines = driver.status();
        } catch (RuntimeException e) {
            LOG.warnv("Could not list the smolvm-pods nodes on this Mac to remove the ones Floci no longer names: {0}",
                    e.getMessage());
            return;
        }
        for (Machine machine : machines) {
            Node node = current;
            if (machine.name().equals(named) || (node != null && machine.name().equals(node.record.nodeId()))
                    || !driver.owned(machine) || !NodeRecord.NODE_ID.matcher(machine.name()).matches()) {
                continue;
            }
            LOG.infov("Deleting smolvm-pods node {0}, which this Floci created and no longer names", machine.name());
            try {
                driver.delete(machine.name());
                ports.release(new NodeRecord(machine.name(), machine.apiPort(), machine.servicePort(),
                        machine.nodePorts(), null, machine.isolated(), List.of(), null, null, null, null, null,
                        Map.of(), List.of()));
                seed.delete(machine.name());
            } catch (RuntimeException e) {
                LOG.warnv("Could not delete smolvm-pods node {0}: {1}", machine.name(), e.getMessage());
            }
        }
    }

    @Override
    public List<SmolvmNode> nodes() {
        Node node = current;
        if (node == null || node.deleting) {
            return List.of();
        }
        return List.of(smolvmNode(node.record));
    }

    /**
     * Checks the node: logs once that it stopped or disappeared, and leaves it so; gives a running
     * isolated node the allow list of the runtime file.
     */
    void reconcile() {
        Node node = current;
        if (node == null || !node.settled() || !managesMachines()) {
            return;
        }
        Optional<Machine> machine;
        try {
            machine = machine(node.record.nodeId());
        } catch (RuntimeException e) {
            LOG.debugv("Could not list the smolvm-pods nodes: {0}", e.getMessage());
            return;
        }
        if (machine.isEmpty() || !machine.get().running()) {
            reportNotRunning(node, machine.orElse(null));
            return;
        }
        if (node.notRunning != null) {
            LOG.infov("smolvm-pods node {0} runs again", node.record.nodeId());
            node.notRunning = null;
        }
        syncAllowList(node, isolation.apply(runtimeFile.current()));
    }

    private void reportNotRunning(Node node, Machine machine) {
        String state = machine == null ? "gone" : machine.state();
        if (state.equals(node.notRunning)) {
            return;
        }
        node.notRunning = state;
        if (machine == null) {
            LOG.warnv("smolvm-pods node {0} is gone, and Floci leaves it so. Reset Floci, or restart it, which "
                    + "creates the node again", node.record.nodeId());
        } else {
            LOG.warnv("smolvm-pods node {0} is {1}, and Floci leaves it so. Start it with node-vm.sh start --name {0}, "
                    + "or restart Floci", node.record.nodeId(), state);
        }
    }

    private void syncAllowList(RuntimeFile file) {
        Node node = current;
        if (node != null && node.settled()) {
            syncAllowList(node, isolation.apply(file));
        }
    }

    /**
     * Queues the change of an isolated node's allow list to {@code wanted}'s CIDRs. A node created
     * open stays open, since an allow list would restrict it; a node keeps the isolation it was
     * created with, which is logged once when the runtime file asks for another.
     */
    private void syncAllowList(Node node, Isolation wanted) {
        NodeRecord record = node.record;
        if (record.isolated() != wanted.enabled() && loggedIsolation.compareAndSet(false, true)) {
            LOG.warnv("smolvm-pods node {0} was created {1}, and the runtime file asks for nodes {2}; the node keeps "
                    + "its boundary until it is created again", record.nodeId(), record.isolated() ? "isolated" : "open",
                    wanted.enabled() ? "isolated" : "open");
        }
        if (!record.isolated() || record.allowCidrs().equals(wanted.allowCidrs())) {
            return;
        }
        if (node.syncing.compareAndSet(false, true)) {
            node.queue.submit(() -> {
                try {
                    updateAllowList(node);
                } finally {
                    node.syncing.set(false);
                }
            });
        }
    }

    private void updateAllowList(Node node) {
        NodeRecord record = node.record;
        if (node.deleting || !record.isolated()) {
            return;
        }
        List<String> wanted = isolation.apply(runtimeFile.current()).allowCidrs();
        List<String> add = missingFrom(record.allowCidrs(), wanted);
        List<String> remove = missingFrom(wanted, record.allowCidrs());
        if (add.isEmpty() && remove.isEmpty()) {
            if (!record.allowCidrs().equals(wanted)) {
                record(node, record.withAllowCidrs(wanted));
            }
            return;
        }
        try {
            driver.updateAllowList(record.nodeId(), add, remove);
            record(node, record.withAllowCidrs(wanted));
            LOG.infov("Changed the allow list of smolvm-pods node {0}: added {1}, removed {2}", record.nodeId(), add,
                    remove);
        } catch (RuntimeException e) {
            LOG.warnv("Could not change the allow list of smolvm-pods node {0}: {1}", record.nodeId(), e.getMessage());
        }
    }

    /** The entries of {@code wanted} that {@code present} lacks, in order. */
    private static List<String> missingFrom(List<String> present, List<String> wanted) {
        List<String> missing = new ArrayList<>();
        for (String entry : wanted) {
            if (!present.contains(entry) && !missing.contains(entry)) {
                missing.add(entry);
            }
        }
        return missing;
    }

    private void createNode(Node node) {
        NodeRecord record = node.record;
        if (node.deleting) {
            return;
        }
        try {
            loadMachines();
            NodeRecord started = driver.createAndStart(record, seed.seedDir(record.nodeId()),
                    (clashed, port) -> reallocate(node, clashed, port));
            record(node, started);
            if (node.deleting) {
                LOG.infov("smolvm-pods node {0} started as Floci deletes it", started.nodeId());
                return;
            }
            node.started = true;
            LOG.infov("Started smolvm-pods node {0}", started.nodeId());
            awaitReady(node);
        } catch (RuntimeException e) {
            fail(node, "Could not create smolvm-pods node " + record.nodeId() + ": " + e.getMessage(), e);
        }
    }

    private void restoreNode(Node node) {
        NodeRecord record = node.record;
        if (node.deleting) {
            return;
        }
        try {
            loadMachines();
            Optional<Machine> machine = machine(record.nodeId());
            if (machine.isEmpty()) {
                LOG.warnv("smolvm-pods node {0} is gone; creating it again. The new node has a new cluster CA and none "
                        + "of the old workloads or databases", record.nodeId());
                kubeApi.forget(record.nodeId());
                NodeRecord seeded = record.withSeeded(seed.write(record));
                record(node, driver.createAndStart(seeded, seed.seedDir(record.nodeId()),
                        (clashed, port) -> reallocate(node, clashed, port)));
            } else {
                NodeRecord labelled = fromLabels(record, machine.get());
                if (!labelled.equals(record)) {
                    ports.move(record, labelled);
                    record(node, labelled);
                }
                if (machine.get().running()) {
                    seed.writeEnv(record.nodeId());
                    LOG.infov("Adopted the running smolvm-pods node {0}", record.nodeId());
                } else {
                    record(node, node.record.withSeeded(seed.write(node.record)));
                    driver.start(record.nodeId());
                    LOG.infov("Started the {0} smolvm-pods node {1}", machine.get().state(), record.nodeId());
                }
            }
            node.started = true;
            awaitReady(node);
        } catch (RuntimeException e) {
            fail(node, "Could not restore smolvm-pods node " + record.nodeId() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Checks the ready rule until it holds, the node goes, or {@code ready-timeout-seconds} pass.
     * Runs on the node's queue.
     */
    private void awaitReady(Node node) {
        Instant deadline = Instant.now().plusSeconds(config.runtime().node().readyTimeoutSeconds());
        String unmet = null;
        while (!node.deleting) {
            NodeRecord record = node.record;
            Optional<String> condition = readyRule.unmetCondition(record, record.nodeName(), smolvmNode(record),
                    () -> bootId(node));
            if (condition.isEmpty()) {
                node.ready = true;
                node.readiness.complete(smolvmNode(record));
                LOG.info(readyMessage(record, seed.seedDir(record.nodeId()), text(config.runtime().node().nodeVmScript()),
                        text(config.runtime().node().smolvmSrc())));
                return;
            }
            if (!condition.get().equals(unmet)) {
                unmet = condition.get();
                LOG.infov("Floci waits for smolvm-pods node {0}: {1}", record.nodeId(), unmet);
            }
            if (Instant.now().isAfter(deadline)) {
                fail(node, "smolvm-pods node " + record.nodeId() + " was not ready within "
                        + config.runtime().node().readyTimeoutSeconds() + " s: " + unmet, null);
                return;
            }
            try {
                Thread.sleep(readyPoll);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail(node, "Floci stopped waiting for smolvm-pods node " + record.nodeId(), null);
                return;
            }
        }
    }

    /** The boot id of the node VM, read once after Floci created, adopted or started it. */
    private String bootId(Node node) {
        String bootId = node.bootId;
        if (bootId == null) {
            bootId = driver.bootId(node.record.nodeId());
            node.bootId = bootId;
        }
        return bootId;
    }

    /** {@code record} with the ports and isolation of its machine's labels, where they name them. */
    private static NodeRecord fromLabels(NodeRecord record, Machine machine) {
        int apiPort = machine.apiPort() > 0 ? machine.apiPort() : record.apiPort();
        int servicePort = machine.servicePort() > 0 ? machine.servicePort() : record.servicePort();
        String nodePorts = machine.labels().containsKey(NodeDriver.LABEL_NODE_PORTS)
                ? machine.nodePorts() : record.nodePorts();
        return record.withPorts(apiPort, servicePort, nodePorts).withIsolated(machine.isolated());
    }

    /** Other ports for a node whose machine could not publish one, in its record and its seed too. */
    private NodeRecord reallocate(Node node, NodeRecord clashed, int clashedPort) {
        NodeRecord next = ports.reallocate(clashed, clashedPort);
        record(node, next);
        seed.write(next);
        return next;
    }

    private void record(Node node, NodeRecord record) {
        NodeRecord previous = node.record;
        node.record = record;
        if (!record.equals(previous)) {
            if (current == node) {
                store.save(record);
            }
            nodesChanged.run();
        }
    }

    /** Records why {@code node} failed, and tells those who wait for it. */
    private void fail(Node node, String message, RuntimeException cause) {
        node.failure = message;
        node.readiness.completeExceptionally(new NodeUnavailableException(message));
        LOG.errorv(cause, "The smolvm-pods node failed: {0}", message);
    }

    /** Reserves the ports in the labels of every {@code node-vm.sh} machine, once. */
    private void loadMachines() {
        if (machinesLoaded) {
            return;
        }
        synchronized (this) {
            if (!machinesLoaded) {
                List<Machine> machines = driver.status();
                ports.reserve(machines);
                machinesLoaded = true;
                LOG.debugv("Reserved the Mac ports of {0} node-vm.sh machines", String.valueOf(machines.size()));
            }
        }
    }

    private Optional<Machine> machine(String nodeId) {
        return driver.status().stream().filter(machine -> machine.name().equals(nodeId)).findFirst();
    }

    private synchronized void startReconciler() {
        if (reconciler != null || !managesMachines()) {
            return;
        }
        reconciler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().daemon().name("floci-node-reconcile").factory());
        reconciler.scheduleWithFixedDelay(() -> {
            try {
                reconcile();
            } catch (RuntimeException e) {
                LOG.errorv(e, "Checking the smolvm-pods node failed");
            }
        }, RECONCILE_INTERVAL.toMillis(), RECONCILE_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    }

    private synchronized void stopReconciler() {
        if (reconciler != null) {
            reconciler.shutdownNow();
            reconciler = null;
        }
    }

    private boolean awaitQueue(Node node, Duration timeout) {
        try {
            node.queue.tail().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            return true;
        } catch (TimeoutException e) {
            return false;
        } catch (ExecutionException failed) {
            // The queue's last task failed and recorded why; nothing more runs on the node.
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private SmolvmNode smolvmNode(NodeRecord record) {
        return new SmolvmNode(NAME, URI.create("http://127.0.0.1:" + record.servicePort()),
                seed.apiToken(record.nodeId()), SmolvmNode.Kind.CREATED, seed.endpoints(record));
    }

    private String imageSha256() {
        NodeConfig node = config.runtime().node();
        return node.imageSha256().map(value -> value.strip().toLowerCase(Locale.ROOT)).orElse("");
    }

    /**
     * The node's name in Kubernetes: an EC2 private DNS name in {@code region}, whose instance id
     * comes from the node id.
     */
    static String nodeName(String nodeId, String region) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(nodeId.getBytes(StandardCharsets.UTF_8));
            String instanceId = "i-" + HexFormat.of().formatHex(digest).substring(0, 17);
            return instanceId + "." + AwsRegions.ec2PrivateDnsDomain(region);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    /**
     * What Floci logs once the node is ready: where a developer on the Mac finds it, and the
     * command that points the Docker CLI at its dockerd.
     */
    static String readyMessage(NodeRecord record, Path seedDir, String nodeVmScript, String smolvmSrc) {
        String script = nodeVmScript.isEmpty() ? "node-vm.sh" : shellWord(nodeVmScript);
        String env = smolvmSrc.isEmpty() ? "" : "SMOLVM_SRC=" + shellWord(smolvmSrc) + " ";
        return "smolvm-pods node " + record.nodeId() + " is ready: Kubernetes API https://127.0.0.1:"
                + record.apiPort() + ", node API http://127.0.0.1:" + record.servicePort()
                + ", admin UI http://127.0.0.1:" + record.servicePort() + "/ui, seed and token in " + seedDir
                + ". Docker CLI: eval \"$(" + env + script + " docker-env --name " + record.nodeId() + ")\"";
    }

    private static String shellWord(String value) {
        return value.matches("[A-Za-z0-9_./:@%+=-]+") ? value : "'" + value.replace("'", "'\\''") + "'";
    }

    private static String text(Optional<String> value) {
        return value.map(String::strip).orElse("");
    }

    private static String blankTo(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    /** The node Floci created, while it has a record. */
    static final class Node {

        final NodeQueue queue;
        final AtomicBoolean syncing = new AtomicBoolean();
        /** Completes once the node is ready; exceptionally when it fails or goes. */
        final CompletableFuture<SmolvmNode> readiness = new CompletableFuture<>();
        volatile NodeRecord record;
        /** The node was created, adopted or started, so the ready rule may hold. */
        volatile boolean started;
        volatile boolean ready;
        /** The boot id of the node VM since it was created, adopted or started, once read. */
        volatile String bootId;
        volatile boolean deleting;
        volatile String failure;
        /** The state last logged for a node that does not run, or null while it runs. */
        volatile String notRunning;

        Node(NodeRecord record, NodeQueue queue) {
            this.record = record;
            this.queue = queue;
        }

        /** Started, not failed and not being deleted. */
        boolean settled() {
            return started && failure == null && !deleting;
        }
    }

    /** The work on the node, run in order. */
    static final class NodeQueue {

        private final Executor executor;
        private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);

        NodeQueue(Executor executor) {
            this.executor = executor;
        }

        synchronized void submit(Runnable task) {
            tail = tail.handle((done, error) -> done).thenRunAsync(task, executor);
        }

        synchronized CompletableFuture<Void> tail() {
            return tail;
        }
    }
}
