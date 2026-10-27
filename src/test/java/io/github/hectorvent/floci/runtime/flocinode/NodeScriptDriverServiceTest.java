package io.github.hectorvent.floci.runtime.flocinode;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.DockerConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.RuntimeConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.RuntimeConfig.NodeConfig;
import io.github.hectorvent.floci.runtime.flocinode.NodeDriver.Machine;
import io.github.hectorvent.floci.runtime.flocinode.NodeDriver.NodeCommandException;
import io.github.hectorvent.floci.runtime.flocinode.NodeDriver.PortReallocator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NodeScriptDriverServiceTest {

    private static final String NODE_ID = "floci-node-0123456789abcdef";
    private static final String CLASH = "Error: agent operation failed: configure virtio-net: failed to start virtio "
            + "network runtime: cannot publish host TCP 127.0.0.1:6500 to guest TCP 6443: Address already in use "
            + "(os error 48)";
    private static final PortReallocator NO_REALLOCATION = (node, port) -> {
        throw new AssertionError("no port clash expected");
    };

    /**
     * Records each call as its argument count and its arguments, one per line, and SMOLVM_SRC.
     * {@code COMMAND-failures} holds one error per line: each call of COMMAND prints the next one
     * and fails. {@code hang-COMMAND} makes COMMAND sleep. {@code status} prints {@code status.json},
     * and {@code exec} prints {@code exec-output}.
     */
    private static final String FAKE_NODE_VM = """
            #!/bin/sh
            dir=$(dirname "$0")
            printf '%s\\n' "$#" "$@" >> "$dir/calls"
            printf '%s\\n' "${SMOLVM_SRC-}" > "$dir/smolvm-src"
            command=$1
            if [ -f "$dir/hang-$command" ]; then
                sleep 60 &
                echo $! > "$dir/sleep-pid"
                wait
            fi
            failures="$dir/$command-failures"
            if [ -s "$failures" ]; then
                echo "==> Working on it" >&2
                head -n 1 "$failures" >&2
                tail -n +2 "$failures" > "$failures.rest"
                mv "$failures.rest" "$failures"
                exit 1
            fi
            echo "==> Done" >&2
            if [ "$command" = status ]; then
                cat "$dir/status.json"
            fi
            if [ "$command" = exec ]; then
                cat "$dir/exec-output"
            fi
            """;

    @TempDir
    Path dir;

    private Path scripts;
    private Path dataPath;
    private Path seed;
    private EmulatorConfig config;
    private NodeConfig smolvm;
    private DockerConfig docker;
    private NodeScriptDriver driver;

    @BeforeEach
    void setUp() throws IOException {
        scripts = Files.createDirectory(dir.resolve("scripts"));
        Path script = Files.writeString(scripts.resolve("node-vm.sh"), FAKE_NODE_VM);
        Files.setPosixFilePermissions(script, PosixFilePermissions.fromString("rwxr-xr-x"));
        dataPath = Files.createDirectory(dir.resolve("runtime-node"));
        seed = Files.createDirectories(dataPath.resolve(NODE_ID).resolve("seed"));

        config = mock(EmulatorConfig.class);
        RuntimeConfig runtime = mock(RuntimeConfig.class);
        smolvm = mock(NodeConfig.class);
        docker = mock(DockerConfig.class);
        when(config.runtime()).thenReturn(runtime);
        when(config.docker()).thenReturn(docker);
        when(docker.resourceNamespace()).thenReturn(Optional.empty());
        when(runtime.node()).thenReturn(smolvm);
        when(smolvm.dataPath()).thenReturn(dataPath.toString());
        when(smolvm.nodeVmScript()).thenReturn(Optional.of(script.toString()));
        when(smolvm.smolvmSrc()).thenReturn(Optional.of("/Users/dev/smolvm"));
        when(smolvm.image()).thenReturn(Optional.of("/Users/dev/node-image.tar"));
        when(smolvm.cpus()).thenReturn(4);
        when(smolvm.memoryMib()).thenReturn(6144);
        when(smolvm.storageGib()).thenReturn(40);
        when(smolvm.overlayGib()).thenReturn(8);
        when(smolvm.rosetta()).thenReturn(true);
        driver = new NodeScriptDriver(config, Duration.ofSeconds(30));
    }

    private static NodeRecord node(boolean isolated, List<String> allowCidrs) {
        return TestNodeRecords.record(NODE_ID, 6500, 6600, "32080-32099", "ab".repeat(32), isolated, allowCidrs);
    }

    private List<List<String>> calls() throws IOException {
        Path file = scripts.resolve("calls");
        List<List<String>> calls = new ArrayList<>();
        if (!Files.exists(file)) {
            return calls;
        }
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        int line = 0;
        while (line < lines.size()) {
            int count = Integer.parseInt(lines.get(line));
            calls.add(List.copyOf(lines.subList(line + 1, line + 1 + count)));
            line += count + 1;
        }
        return calls;
    }

    private void failNext(String command, String... errors) throws IOException {
        Files.writeString(scripts.resolve(command + "-failures"), String.join("\n", errors) + "\n");
    }

    private String dataId() {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(dataPath.toAbsolutePath().normalize().toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest).substring(0, 12);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private List<String> createCall(int apiPort, String... middle) {
        List<String> call = new ArrayList<>(List.of("create", "--name", NODE_ID, "--image", "/Users/dev/node-image.tar",
                "--cpus", "4", "--mem", "6144", "--storage", "40", "--overlay", "8",
                "--api-port", String.valueOf(apiPort), "--service-port", "6600", "--node-ports", "32080-32099",
                "--seed", seed.toString()));
        call.addAll(List.of(middle));
        call.addAll(List.of("--", "--label", "floci.node.owner=", "--label", "floci.node.data=" + dataId()));
        return call;
    }

    private static List<String> startCall() {
        return List.of("start", "--name", NODE_ID, "--timeout", "0");
    }

    private static List<String> deleteCall() {
        return List.of("delete", "--name", NODE_ID);
    }

    @Test
    void createAndStart_notIsolated_createsTheNodeThenStartsItWithoutWaiting() throws IOException {
        NodeRecord started = driver.createAndStart(node(false, List.of()), seed, NO_REALLOCATION);

        assertEquals(node(false, List.of()), started);
        assertThat(calls(), contains(createCall(6500), startCall()));
        assertEquals("/Users/dev/smolvm", Files.readString(scripts.resolve("smolvm-src")).strip());
    }

    @Test
    void createAndStart_isolated_passesIsolateAndTheAllowList() throws IOException {
        when(docker.resourceNamespace()).thenReturn(Optional.of("team-a"));
        when(smolvm.rosetta()).thenReturn(false);

        driver.createAndStart(node(true, List.of("10.20.0.0/16", "192.0.2.0/24")), seed, NO_REALLOCATION);

        List<String> create = calls().getFirst();
        assertEquals(List.of("--seed", seed.toString(), "--isolate", "--allow-cidr", "10.20.0.0/16",
                "--allow-cidr", "192.0.2.0/24", "--no-rosetta", "--", "--label", "floci.node.owner=team-a"),
                create.subList(create.indexOf("--seed"), create.indexOf("--seed") + 11));
    }

    @Test
    void createAndStart_notIsolated_neverPassesAnAllowList() throws IOException {
        driver.createAndStart(node(false, List.of("10.20.0.0/16")), seed, NO_REALLOCATION);

        List<String> create = calls().getFirst();
        assertThat(create, not(hasItem("--isolate")));
        assertThat(create, not(hasItem("--allow-cidr")));
    }

    @Test
    void createAndStart_portClash_deletesTheNodeAndCreatesItAgainWithOtherPorts() throws IOException {
        failNext("start", CLASH);
        List<Integer> clashedPorts = new ArrayList<>();

        NodeRecord started = driver.createAndStart(node(false, List.of()), seed, (node, port) -> {
            clashedPorts.add(port);
            return node.withPorts(6501, node.servicePort(), node.nodePorts());
        });

        assertEquals(6501, started.apiPort());
        assertEquals(List.of(6500), clashedPorts);
        assertThat(calls(), contains(createCall(6500), startCall(), deleteCall(), createCall(6501), startCall()));
    }

    @Test
    void createAndStart_portsThatKeepClashing_failAfterThreeRetries() throws IOException {
        failNext("start", CLASH, CLASH, CLASH, CLASH);
        List<Integer> clashedPorts = new ArrayList<>();

        NodeCommandException failed = assertThrows(NodeCommandException.class,
                () -> driver.createAndStart(node(false, List.of()), seed, (node, port) -> {
                    clashedPorts.add(port);
                    return node;
                }));

        assertTrue(failed.portClash());
        assertEquals(List.of(6500, 6500, 6500), clashedPorts);
        List<List<String>> calls = calls();
        assertEquals(4, calls.stream().filter(call -> call.getFirst().equals("create")).count());
        assertEquals(4, calls.stream().filter(call -> call.getFirst().equals("start")).count());
        assertEquals(3, calls.stream().filter(call -> call.getFirst().equals("delete")).count());
    }

    @Test
    void createAndStart_anotherStartFailure_isNotRetried() throws IOException {
        failNext("start", "node-vm.sh: smolvm CLI not found: /Users/dev/smolvm/target/floci/kernel/bin/smolvm-floci");

        NodeCommandException failed = assertThrows(NodeCommandException.class,
                () -> driver.createAndStart(node(false, List.of()), seed, NO_REALLOCATION));

        assertEquals("node-vm.sh start --name " + NODE_ID + " --timeout 0 failed with exit code 1: "
                + "node-vm.sh: smolvm CLI not found: /Users/dev/smolvm/target/floci/kernel/bin/smolvm-floci",
                failed.getMessage());
        assertFalse(failed.portClash());
        assertThat(calls(), contains(createCall(6500), startCall()));
    }

    @Test
    void startStopAndDelete_nameTheNode() throws IOException {
        driver.start(NODE_ID);
        driver.stop(NODE_ID);
        assertTrue(driver.delete(NODE_ID));

        assertThat(calls(), contains(startCall(), List.of("stop", "--name", NODE_ID), deleteCall()));
    }

    @Test
    void delete_aMissingMachine_returnsFalse() throws IOException {
        failNext("delete", "node-vm.sh: machine " + NODE_ID + " does not exist (run node-vm.sh create)",
                "Error: machine is busy");

        assertFalse(driver.delete(NODE_ID));
        NodeCommandException failed = assertThrows(NodeCommandException.class, () -> driver.delete(NODE_ID));
        assertThat(failed.getMessage(), containsString("Error: machine is busy"));
    }

    @Test
    void bootId_readsTheKernelBootIdInTheNode() throws IOException {
        Files.writeString(scripts.resolve("exec-output"), "6f1c2d3e-4b5a-4c6d-8e7f-0a1b2c3d4e5f\n");

        assertEquals("6f1c2d3e-4b5a-4c6d-8e7f-0a1b2c3d4e5f", driver.bootId(NODE_ID));
        assertThat(calls(), contains(List.of("exec", "--name", NODE_ID, "--", "cat",
                "/proc/sys/kernel/random/boot_id")));

        Files.writeString(scripts.resolve("exec-output"), "cat: can't open '/proc/sys/kernel/random/boot_id'\n");
        NodeCommandException failed = assertThrows(NodeCommandException.class, () -> driver.bootId(NODE_ID));
        assertEquals("node-vm.sh exec --name " + NODE_ID + " printed no boot id", failed.getMessage());
    }

    @Test
    void aScriptThatIsNotSetOrCannotStart_didNotRun() {
        Optional<String> script = smolvm.nodeVmScript();
        when(smolvm.nodeVmScript()).thenReturn(Optional.empty());
        NodeCommandException unset = assertThrows(NodeCommandException.class, () -> driver.delete(NODE_ID));
        assertFalse(unset.ran());
        assertThat(unset.getMessage(), containsString("FLOCI_RUNTIME_NODE_NODE_VM_SCRIPT) is not set"));

        when(smolvm.nodeVmScript()).thenReturn(Optional.of(dir.resolve("missing").resolve("node-vm.sh").toString()));
        NodeCommandException missing = assertThrows(NodeCommandException.class, () -> driver.delete(NODE_ID));
        assertFalse(missing.ran());

        when(smolvm.nodeVmScript()).thenReturn(script);
        assertTrue(new NodeCommandException("failed", "Error: machine is busy", null).ran());
    }

    @Test
    void commands_runOnlyOnFlociNodeIds() throws IOException {
        assertThrows(IllegalArgumentException.class, () -> driver.delete("smolvm-pods-node"));
        assertThrows(IllegalArgumentException.class, () -> driver.stop("floci-node-x"));
        assertThrows(IllegalArgumentException.class,
                () -> driver.updateAllowList("smolvm-pods-node", List.of("10.0.0.0/8"), List.of()));

        assertThat(calls(), empty());
    }

    @Test
    void updateAllowList_addsAndRemovesCidrs() throws IOException {
        driver.updateAllowList(NODE_ID, List.of("10.20.0.0/16", "192.0.2.0/24"), List.of("198.51.100.0/24"));
        driver.updateAllowList(NODE_ID, List.of(), List.of("10.20.0.0/16"));
        driver.updateAllowList(NODE_ID, List.of(), List.of());

        assertThat(calls(), contains(
                List.of("update", "--name", NODE_ID, "--allow-cidr", "10.20.0.0/16", "--allow-cidr", "192.0.2.0/24",
                        "--remove-allow-cidr", "198.51.100.0/24"),
                List.of("update", "--name", NODE_ID, "--remove-allow-cidr", "10.20.0.0/16")));
    }

    @Test
    void status_parsesEveryMachineWithItsLabels() throws IOException {
        Files.writeString(scripts.resolve("status.json"), """
                [
                  {"name": "%s", "state": "running", "labels": {"smolvm-pods-node": "true",
                    "api-port": "6500", "service-port": "6600", "node-ports": "32080-32099", "isolated": "true",
                    "floci.node.owner": "", "floci.node.data": "%s"}},
                  {"name": "smolvm-pods-node", "state": "stopped", "labels": {"smolvm-pods-node": "true",
                    "api-port": "6443", "node-ports": "none"}},
                  {"name": "floci-node-fedcba9876543210", "state": "created", "labels": {"smolvm-pods-node": "true",
                    "api-port": "6501", "floci.node.owner": "", "floci.node.data": "000000000000"}}
                ]
                """.formatted(NODE_ID, dataId()));

        List<Machine> machines = driver.status();

        assertThat(machines.stream().map(Machine::name).toList(),
                contains(NODE_ID, "smolvm-pods-node", "floci-node-fedcba9876543210"));
        Machine created = machines.get(0);
        assertTrue(created.running());
        assertEquals(6500, created.apiPort());
        assertEquals(6600, created.servicePort());
        assertEquals("32080-32099", created.nodePorts());
        assertTrue(created.isolated());
        assertTrue(driver.owned(created));

        Machine byHand = machines.get(1);
        assertFalse(byHand.running());
        assertEquals("stopped", byHand.state());
        assertEquals(6443, byHand.apiPort());
        assertEquals(0, byHand.servicePort());
        assertEquals("none", byHand.nodePorts());
        assertFalse(byHand.isolated());
        assertFalse(driver.owned(byHand));

        assertFalse(driver.owned(machines.get(2)), "another Floci data path");
        assertEquals(List.of("status", "--json"), calls().getFirst());
    }

    @Test
    void status_ownerLabelsFollowTheResourceNamespace() throws IOException {
        Files.writeString(scripts.resolve("status.json"), """
                [{"name": "%s", "state": "running", "labels": {"floci.node.owner": "", "floci.node.data": "%s"}}]
                """.formatted(NODE_ID, dataId()));
        Machine machine = driver.status().getFirst();
        assertTrue(driver.owned(machine));

        when(docker.resourceNamespace()).thenReturn(Optional.of("team-a"));

        assertFalse(driver.owned(machine));
        assertEquals(Map.of("floci.node.owner", "team-a", "floci.node.data", dataId()), driver.ownerLabels());
    }

    @Test
    void status_refusesOutputThatIsNotAJsonArray() throws IOException {
        Files.writeString(scripts.resolve("status.json"), "{\"name\": \"x\"}\n");
        NodeCommandException notArray = assertThrows(NodeCommandException.class, driver::status);
        assertEquals("node-vm.sh status --json printed no JSON array", notArray.getMessage());

        Files.writeString(scripts.resolve("status.json"), "NAME STATE\n");
        NodeCommandException notJson = assertThrows(NodeCommandException.class, driver::status);
        assertThat(notJson.getMessage(), containsString("node-vm.sh status --json printed no JSON"));
    }

    @Test
    void aCommandThatRunsTooLong_isStoppedWithItsChildren() throws Exception {
        Files.writeString(scripts.resolve("hang-stop"), "");
        NodeScriptDriver impatient = new NodeScriptDriver(config, Duration.ofSeconds(1));

        NodeCommandException failed = assertThrows(NodeCommandException.class, () -> impatient.stop(NODE_ID));

        assertEquals("node-vm.sh stop --name " + NODE_ID + " did not finish within 1 s and was stopped",
                failed.getMessage());
        long sleepPid = Long.parseLong(Files.readString(scripts.resolve("sleep-pid")).strip());
        Optional<ProcessHandle> sleep = ProcessHandle.of(sleepPid);
        if (sleep.isPresent()) {
            sleep.get().onExit().get(10, TimeUnit.SECONDS);
        }
        assertFalse(ProcessHandle.of(sleepPid).map(ProcessHandle::isAlive).orElse(false), "the child was stopped");
    }

    @Test
    void portClash_recognisesSmolvmsPublishFailures() {
        NodeCommandException named = new NodeCommandException("failed", "==> Starting\n" + CLASH + "\n", null);
        assertTrue(named.portClash());
        assertEquals(6500, named.clashedPort());

        NodeCommandException ipv6 = new NodeCommandException("failed",
                "cannot publish host TCP [::1]:32085 to guest TCP 32085: Address already in use", null);
        assertTrue(ipv6.portClash());
        assertEquals(32085, ipv6.clashedPort());

        NodeCommandException wrapped = new NodeCommandException("failed", "cannot publish\n    host TCP "
                + "127.0.0.1:6600 to guest TCP 7070: Address already in use (os error 48)", null);
        assertEquals(6600, wrapped.clashedPort());

        NodeCommandException unnamed = new NodeCommandException("failed", "configure virtio-net: failed to start "
                + "virtio network runtime: Address already in use (os error 48)", null);
        assertTrue(unnamed.portClash());
        assertEquals(0, unnamed.clashedPort());

        NodeCommandException vsock = new NodeCommandException("failed", "bind vsock: Address already in use", null);
        assertFalse(vsock.portClash());
        assertFalse(new NodeCommandException("failed", null, null).portClash());
    }
}
