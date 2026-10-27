package io.github.hectorvent.floci.runtime.flocinode;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A {@link NodeDriver} for tests that keeps its machines in memory, labelled as
 * {@code node-vm.sh} labels them, and records every call. A test can hold or fail the creation of
 * nodes, and add machines that {@code status} lists.
 */
public class FakeNodeDriver implements NodeDriver {

    /** A call to the driver: the command, the machine it names, if any, and its other arguments. */
    public record Call(String command, String nodeId, List<String> args) {
    }

    private final Map<String, String> ownerLabels;
    private final Map<String, Machine> machines = new LinkedHashMap<>();
    private final List<Call> calls = new CopyOnWriteArrayList<>();
    private final Map<String, String> deleteFailures = new ConcurrentHashMap<>();
    private final Map<String, String> bootIds = new ConcurrentHashMap<>();
    private final Map<String, Integer> bootIdReads = new ConcurrentHashMap<>();
    private volatile CountDownLatch gate;
    private volatile String failure;
    private volatile boolean runnable = true;

    /** @param ownerLabels the owner and data labels of the machines this driver creates */
    public FakeNodeDriver(Map<String, String> ownerLabels) {
        this.ownerLabels = Map.copyOf(ownerLabels);
    }

    @Override
    public NodeRecord createAndStart(NodeRecord node, Path seed, PortReallocator reallocator) {
        List<String> args = new ArrayList<>(List.of(seed.toString(), "api-port=" + node.apiPort(),
                "service-port=" + node.servicePort(), "node-ports=" + node.nodePorts()));
        if (node.isolated()) {
            args.add("isolate");
            node.allowCidrs().forEach(cidr -> args.add("allow-cidr=" + cidr));
        }
        calls.add(new Call("create", node.nodeId(), List.copyOf(args)));
        CountDownLatch held = gate;
        if (held != null) {
            try {
                if (!held.await(30, TimeUnit.SECONDS)) {
                    throw new NodeCommandException("the test never let the create of " + node.nodeId() + " go on", "",
                            null);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new NodeCommandException("interrupted", "", e);
            }
        }
        String error = failure;
        if (error != null) {
            throw new NodeCommandException("node-vm.sh create --name " + node.nodeId() + " failed with exit code 1: "
                    + error, error, null);
        }
        Map<String, String> labels = new LinkedHashMap<>(ownerLabels);
        labels.put(LABEL_API_PORT, String.valueOf(node.apiPort()));
        labels.put(LABEL_SERVICE_PORT, String.valueOf(node.servicePort()));
        labels.put(LABEL_NODE_PORTS, node.nodePorts());
        if (node.isolated()) {
            labels.put(LABEL_ISOLATED, "true");
        }
        synchronized (this) {
            machines.put(node.nodeId(), new Machine(node.nodeId(), "running", labels));
        }
        started(node, seed);
        return node;
    }

    /** Called once a node's machine runs, after a create. */
    protected void started(NodeRecord node, Path seed) {
    }

    /** Called once a node's machine is deleted. */
    protected void deleted(String nodeId) {
    }

    @Override
    public synchronized void start(String nodeId) {
        calls.add(new Call("start", nodeId, List.of()));
        Machine machine = machines.get(nodeId);
        if (machine == null) {
            throw new NodeCommandException("node-vm.sh start --name " + nodeId + " failed: machine " + nodeId
                    + " does not exist", "", null);
        }
        machines.put(nodeId, new Machine(nodeId, "running", machine.labels()));
    }

    @Override
    public synchronized void stop(String nodeId) {
        calls.add(new Call("stop", nodeId, List.of()));
        Machine machine = machines.get(nodeId);
        if (machine != null) {
            machines.put(nodeId, new Machine(nodeId, "stopped", machine.labels()));
        }
    }

    @Override
    public boolean delete(String nodeId) {
        if (!runnable) {
            throw NodeCommandException.notRun("Cannot run node-vm.sh delete --name " + nodeId + ": No such file or "
                    + "directory", null);
        }
        calls.add(new Call("delete", nodeId, List.of()));
        String error = deleteFailures.get(nodeId);
        if (error != null) {
            throw new NodeCommandException("node-vm.sh delete --name " + nodeId + " failed with exit code 1: "
                    + error, error, null);
        }
        Machine removed;
        synchronized (this) {
            removed = machines.remove(nodeId);
        }
        deleted(nodeId);
        return removed != null;
    }

    @Override
    public void updateAllowList(String nodeId, List<String> add, List<String> remove) {
        List<String> args = new ArrayList<>();
        add.forEach(cidr -> args.add("allow-cidr=" + cidr));
        remove.forEach(cidr -> args.add("remove-allow-cidr=" + cidr));
        calls.add(new Call("update", nodeId, List.copyOf(args)));
    }

    /** The boot id set for a running machine, else {@link FakeKubeApi#BOOT_ID}; not one of the {@link #calls()}. */
    @Override
    public String bootId(String nodeId) {
        bootIdReads.merge(nodeId, 1, Integer::sum);
        Optional<Machine> machine = machine(nodeId);
        if (machine.isEmpty() || !machine.get().running()) {
            throw new NodeCommandException("node-vm.sh exec --name " + nodeId + " failed: machine " + nodeId
                    + " is not running", "", null);
        }
        return bootIds.getOrDefault(nodeId, FakeKubeApi.BOOT_ID);
    }

    /** Gives machine {@code nodeId} boot id {@code bootId}, as a start of its VM does. */
    public void bootId(String nodeId, String bootId) {
        bootIds.put(nodeId, bootId);
    }

    /** How often the boot id of machine {@code nodeId} was read. */
    public int bootIdReads(String nodeId) {
        return bootIdReads.getOrDefault(nodeId, 0);
    }

    @Override
    public synchronized List<Machine> status() {
        calls.add(new Call("status", null, List.of()));
        return List.copyOf(machines.values());
    }

    @Override
    public boolean owned(Machine machine) {
        for (Map.Entry<String, String> label : ownerLabels.entrySet()) {
            if (!label.getValue().equals(machine.labels().get(label.getKey()))) {
                return false;
            }
        }
        return true;
    }

    /** The owner and data labels of the machines this driver creates. */
    public Map<String, String> ownerLabels() {
        return ownerLabels;
    }

    /** Lists {@code machine} from now on, as a machine that another create or another tool made. */
    public synchronized void addMachine(Machine machine) {
        machines.put(machine.name(), machine);
    }

    /** Sets the state of machine {@code nodeId}, as a change made outside Floci would. */
    public synchronized void state(String nodeId, String state) {
        Machine machine = machines.get(nodeId);
        machines.put(nodeId, new Machine(nodeId, state, machine.labels()));
    }

    /** Removes machine {@code nodeId}, as a delete made outside Floci would. */
    public synchronized void removeMachine(String nodeId) {
        machines.remove(nodeId);
    }

    public synchronized Optional<Machine> machine(String nodeId) {
        return Optional.ofNullable(machines.get(nodeId));
    }

    /** Every machine, as {@code status} lists them, without recording a call. */
    public synchronized List<Machine> machines() {
        return List.copyOf(machines.values());
    }

    public List<Call> calls() {
        return List.copyOf(calls);
    }

    /** Forgets the calls so far. */
    public void clearCalls() {
        calls.clear();
    }

    /** The calls of {@code command}, in order. */
    public List<Call> calls(String command) {
        return calls.stream().filter(call -> call.command().equals(command)).toList();
    }

    /** The calls of {@code command} that name machine {@code nodeId}. */
    public List<Call> calls(String command, String nodeId) {
        return calls(command).stream().filter(call -> nodeId.equals(call.nodeId())).toList();
    }

    /** Holds every create until the latch counts down. */
    public CountDownLatch holdCreates() {
        CountDownLatch held = new CountDownLatch(1);
        gate = held;
        return held;
    }

    /** Fails every create with {@code error}; a null error lets them succeed. */
    public void failCreates(String error) {
        failure = error;
    }

    /** Whether a delete can run node-vm.sh; when it cannot, it fails before it runs, as a missing script does. */
    public void runnable(boolean runnable) {
        this.runnable = runnable;
    }

    /** Fails every delete of machine {@code nodeId} with {@code error}; a null error lets them succeed. */
    public void failDeletes(String nodeId, String error) {
        if (error == null) {
            deleteFailures.remove(nodeId);
        } else {
            deleteFailures.put(nodeId, error);
        }
    }
}
