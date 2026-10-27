package io.github.hectorvent.floci.runtime.flocinode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.RuntimeConfig.NodeConfig;
import io.github.hectorvent.floci.core.common.docker.ContainerStorageHelper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/**
 * Drives smolvm-pods node VMs with {@code node-vm.sh}, {@code floci.runtime.node.node-vm-script},
 * run as a child process with {@code SMOLVM_SRC} set to {@code floci.runtime.node.smolvm-src}.
 * Each command has a time limit, after which it and its children are stopped, and writes its
 * output to temporary files rather than pipes, so a process that outlives the script, such as a VM
 * that {@code start} launched, cannot hold the call open.
 *
 * <p>Floci runs commands only on the machines it names, {@code floci-node-} and 16 hex digits,
 * and lists every {@code node-vm.sh} machine.
 */
@ApplicationScoped
public class NodeScriptDriver implements NodeDriver {

    private static final Logger LOG = Logger.getLogger(NodeScriptDriver.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    static final Duration COMMAND_TIMEOUT = Duration.ofMinutes(2);
    static final int PORT_CLASH_RETRIES = 3;
    private static final long STOP_GRACE_SECONDS = 5;
    private static final Pattern BOOT_ID = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

    private final EmulatorConfig config;
    private final Duration timeout;

    @Inject
    public NodeScriptDriver(EmulatorConfig config) {
        this(config, COMMAND_TIMEOUT);
    }

    NodeScriptDriver(EmulatorConfig config, Duration timeout) {
        this.config = config;
        this.timeout = timeout;
    }

    @Override
    public NodeRecord createAndStart(NodeRecord node, Path seed, PortReallocator reallocator) {
        NodeRecord current = node;
        for (int retry = 0; ; retry++) {
            requireNodeId(current.nodeId());
            LOG.infov("Creating smolvm-pods node {0}: API port {1}, node service port {2}, NodePorts {3}, "
                    + "isolated {4}", current.nodeId(), String.valueOf(current.apiPort()),
                    String.valueOf(current.servicePort()), current.nodePorts(), current.isolated());
            try {
                run(createArgs(current, seed));
                start(current.nodeId());
                return current;
            } catch (NodeCommandException e) {
                if (!e.portClash() || retry == PORT_CLASH_RETRIES) {
                    throw e;
                }
                LOG.warnv("smolvm could not publish Mac port {0} of node {1}; deleting the node and creating it "
                        + "again with other ports", e.clashedPort() > 0 ? String.valueOf(e.clashedPort()) : "(unnamed)",
                        current.nodeId());
                delete(current.nodeId());
                current = reallocator.reallocate(current, e.clashedPort());
            }
        }
    }

    @Override
    public void start(String nodeId) {
        run(List.of("start", "--name", requireNodeId(nodeId), "--timeout", "0"));
    }

    @Override
    public void stop(String nodeId) {
        run(List.of("stop", "--name", requireNodeId(nodeId)));
    }

    @Override
    public boolean delete(String nodeId) {
        try {
            run(List.of("delete", "--name", requireNodeId(nodeId)));
            return true;
        } catch (NodeCommandException e) {
            if (e.output().contains("machine " + nodeId + " does not exist")) {
                return false;
            }
            throw e;
        }
    }

    @Override
    public void updateAllowList(String nodeId, List<String> add, List<String> remove) {
        List<String> args = new ArrayList<>(List.of("update", "--name", requireNodeId(nodeId)));
        for (String cidr : add) {
            args.add("--allow-cidr");
            args.add(cidr);
        }
        for (String cidr : remove) {
            args.add("--remove-allow-cidr");
            args.add(cidr);
        }
        if (args.size() > 3) {
            run(args);
        }
    }

    @Override
    public String bootId(String nodeId) {
        Output output = run(List.of("exec", "--name", requireNodeId(nodeId), "--", "cat",
                "/proc/sys/kernel/random/boot_id"));
        String bootId = output.stdout().strip();
        if (!BOOT_ID.matcher(bootId).matches()) {
            throw new NodeCommandException("node-vm.sh exec --name " + nodeId + " printed no boot id", output.all(),
                    null);
        }
        return bootId;
    }

    @Override
    public List<Machine> status() {
        Output output = run(List.of("status", "--json"));
        JsonNode root;
        try {
            root = JSON.readTree(output.stdout());
        } catch (JsonProcessingException e) {
            throw new NodeCommandException("node-vm.sh status --json printed no JSON: " + e.getOriginalMessage(),
                    output.all(), e);
        }
        if (root == null || !root.isArray()) {
            throw new NodeCommandException("node-vm.sh status --json printed no JSON array", output.all(), null);
        }
        List<Machine> machines = new ArrayList<>();
        for (JsonNode item : root) {
            String name = item.path("name").textValue();
            if (name == null) {
                continue;
            }
            Map<String, String> labels = new LinkedHashMap<>();
            for (Map.Entry<String, JsonNode> label : item.path("labels").properties()) {
                if (label.getValue().isValueNode() && !label.getValue().isNull()) {
                    labels.put(label.getKey(), label.getValue().asText());
                }
            }
            machines.add(new Machine(name, item.path("state").asText(""), labels));
        }
        return machines;
    }

    @Override
    public boolean owned(Machine machine) {
        for (Map.Entry<String, String> label : ownerLabels().entrySet()) {
            if (!label.getValue().equals(machine.labels().get(label.getKey()))) {
                return false;
            }
        }
        return true;
    }

    /** The owner and data labels of the machines this Floci creates. */
    public Map<String, String> ownerLabels() {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put(LABEL_OWNER, ContainerStorageHelper.resourceNamespace(config));
        labels.put(LABEL_DATA, dataId());
        return labels;
    }

    private String dataId() {
        String dataPath = Path.of(config.runtime().node().dataPath()).toAbsolutePath().normalize().toString();
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(dataPath.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest).substring(0, 12);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private List<String> createArgs(NodeRecord node, Path seed) {
        NodeConfig smolvm = config.runtime().node();
        List<String> args = new ArrayList<>(List.of("create", "--name", node.nodeId(),
                "--image", text(smolvm.image()),
                "--cpus", String.valueOf(smolvm.cpus()),
                "--mem", String.valueOf(smolvm.memoryMib()),
                "--storage", String.valueOf(smolvm.storageGib()),
                "--overlay", String.valueOf(smolvm.overlayGib()),
                "--api-port", String.valueOf(node.apiPort())));
        if (node.servicePort() > 0) {
            args.add("--service-port");
            args.add(String.valueOf(node.servicePort()));
        }
        args.add("--node-ports");
        args.add(node.nodePorts() == null ? NO_NODE_PORTS : node.nodePorts());
        args.add("--seed");
        args.add(seed.toString());
        if (node.isolated()) {
            args.add("--isolate");
            for (String cidr : node.allowCidrs()) {
                args.add("--allow-cidr");
                args.add(cidr);
            }
        }
        if (!smolvm.rosetta()) {
            args.add("--no-rosetta");
        }
        Map<String, String> labels = ownerLabels();
        args.add("--");
        for (Map.Entry<String, String> label : labels.entrySet()) {
            args.add("--label");
            args.add(label.getKey() + "=" + label.getValue());
        }
        return args;
    }

    /**
     * Runs {@code node-vm.sh} with {@code args}.
     *
     * @throws NodeCommandException when it cannot run, exits with an error or does not finish in time
     */
    private Output run(List<String> args) {
        NodeConfig smolvm = config.runtime().node();
        String shown = "node-vm.sh " + String.join(" ", args);
        String script = text(smolvm.nodeVmScript());
        if (script.isEmpty()) {
            throw NodeCommandException.notRun("Cannot run " + shown + ": " + NodeSettings.key("node-vm-script")
                    + " is not set", null);
        }
        List<String> command = new ArrayList<>();
        command.add(script);
        command.addAll(args);
        Path stdout = null;
        Path stderr = null;
        try {
            stdout = Files.createTempFile("floci-node-vm-", ".out");
            stderr = Files.createTempFile("floci-node-vm-", ".err");
            ProcessBuilder builder = new ProcessBuilder(command)
                    .redirectOutput(stdout.toFile())
                    .redirectError(stderr.toFile());
            String smolvmSrc = text(smolvm.smolvmSrc());
            if (!smolvmSrc.isEmpty()) {
                builder.environment().put("SMOLVM_SRC", smolvmSrc);
            }
            LOG.debugv("Running {0}", shown);
            Process process;
            try {
                process = builder.start();
            } catch (IOException e) {
                throw NodeCommandException.notRun("Cannot run " + shown + ": " + e.getMessage(), e);
            }
            process.getOutputStream().close();
            if (!finishes(process, shown)) {
                throw new NodeCommandException(shown + " did not finish within " + timeout.toSeconds()
                        + " s and was stopped", read(stdout) + read(stderr), null);
            }
            Output output = new Output(read(stdout), read(stderr));
            if (process.exitValue() != 0) {
                LOG.warnv("{0} failed with exit code {1}:\n{2}", shown, String.valueOf(process.exitValue()),
                        output.all().strip());
                throw new NodeCommandException(shown + " failed with exit code " + process.exitValue() + ": "
                        + output.reason(), output.all(), null);
            }
            LOG.debugv("{0} finished:\n{1}", shown, output.stderr().strip());
            return output;
        } catch (IOException e) {
            throw new NodeCommandException("Cannot run " + shown + ": " + e.getMessage(), "", e);
        } finally {
            deleteTemporary(stdout);
            deleteTemporary(stderr);
        }
    }

    /** Waits for {@code process}; stops it and its children and returns false when it runs out of time. */
    private boolean finishes(Process process, String shown) {
        try {
            if (process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                return true;
            }
            LOG.warnv("{0} did not finish within {1} s; stopping it", shown, String.valueOf(timeout.toSeconds()));
            stopTree(process);
            return false;
        } catch (InterruptedException e) {
            stopTree(process);
            Thread.currentThread().interrupt();
            throw new NodeCommandException("Interrupted while running " + shown, "", e);
        }
    }

    /** Asks {@code process} and its children to stop, and kills those still running after a grace period. */
    private static void stopTree(Process process) {
        List<ProcessHandle> tree = new ArrayList<>(process.descendants().toList());
        tree.add(process.toHandle());
        tree.forEach(ProcessHandle::destroy);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(STOP_GRACE_SECONDS);
        try {
            for (ProcessHandle handle : tree) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                handle.onExit().get(remaining, TimeUnit.NANOSECONDS);
            }
        } catch (TimeoutException | ExecutionException e) {
            LOG.debugv("A process of node-vm.sh did not stop within {0} s: {1}", String.valueOf(STOP_GRACE_SECONDS),
                    e.toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        tree.stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroyForcibly);
    }

    private static String read(Path file) throws IOException {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }

    private static void deleteTemporary(Path file) {
        if (file == null) {
            return;
        }
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            LOG.debugv("Could not remove the temporary file {0}: {1}", file, e.getMessage());
        }
    }

    private static String requireNodeId(String nodeId) {
        if (nodeId == null || !NodeRecord.NODE_ID.matcher(nodeId).matches()) {
            throw new IllegalArgumentException("not a smolvm node id: " + nodeId);
        }
        return nodeId;
    }

    private static String text(Optional<String> value) {
        return value.map(String::strip).orElse("");
    }

    /** What a command printed. */
    private record Output(String stdout, String stderr) {

        String all() {
            return stdout + stderr;
        }

        /** The last line of the error output, or of the output when there is none. */
        String reason() {
            String line = lastLine(stderr);
            if (line.isEmpty()) {
                line = lastLine(stdout);
            }
            return line.isEmpty() ? "no output" : line;
        }

        private static String lastLine(String text) {
            List<String> lines = text.strip().lines().toList();
            return lines.isEmpty() ? "" : lines.getLast().strip();
        }
    }
}
