package io.github.hectorvent.floci.runtime.ondemand;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.microsandbox.MicrosandboxCapacity;
import io.github.hectorvent.floci.core.common.microsandbox.MicrosandboxClient;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.BackendProtocol;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.MicrosandboxSpec;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.RuntimeType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MicrosandboxWorkloadProviderTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final String ACCOUNT = "000000000000";
    private final ObjectMapper mapper = new ObjectMapper();
    private final AccountAwareStorageBackend<MicrosandboxWorkloadMachine> journal =
            AccountAwareStorageBackend.inMemory(ACCOUNT);
    private final FakeClient client = new FakeClient();
    private EmulatorConfig config;
    private StorageFactory storageFactory;
    private MicrosandboxWorkloadProvider provider;
    private MicrosandboxCapacity capacity;
    private WorkloadRuntime runtime;

    @BeforeEach
    void setup() {
        config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.docker().resourceNamespace()).thenReturn(Optional.of("workload-test"));
        when(config.onDemand().microsandboxBackendHost()).thenReturn("127.0.0.1");
        when(config.onDemand().microsandboxBindHost()).thenReturn("127.0.0.1");
        when(config.microsandbox().maxActiveMemoryMib()).thenReturn(8192);
        capacity = new MicrosandboxCapacity(config);
        storageFactory = mock(StorageFactory.class);
        when(storageFactory.create(eq("on-demand"), eq("microsandbox-workloads.json"), any(TypeReference.class)))
                .thenAnswer(ignored -> journal);
        provider = new MicrosandboxWorkloadProvider(config, mapper, storageFactory, client, capacity);
        runtime = provider.create(definition("image:1"));
    }

    @Test
    void twoCyclesPreserveBackendAndConsumeSourceBeforeResumingConsumers() throws Exception {
        runtime.start(TIMEOUT);
        URI backend = runtime.backend();
        assertTrue(runtime.isRunning());
        for (int cycle = 1; cycle <= 2; cycle++) {
            String source = machine().vm;
            runtime.stop(TIMEOUT);
            assertFalse(runtime.isRunning());
            assertEquals("Stopped", client.guests.get(source));
            runtime.start(TIMEOUT);
            runtime.awaitReady(TIMEOUT);
            assertEquals(backend, runtime.backend());
            assertEquals(cycle, machine().generation);
            assertEquals(1, client.guests.size());
            assertFalse(client.guests.containsKey(source));
            assertTrue(client.checkpoints.isEmpty());
        }
        assertEquals(1, client.runs);
        assertEquals(2, client.restores);
    }

    @Test
    void failedVerificationResumesOriginalWithoutDestroyingItsState() throws Exception {
        runtime.start(TIMEOUT);
        String source = machine().vm;
        client.failVerify = true;
        assertThrows(IllegalStateException.class, () -> runtime.stop(TIMEOUT));
        assertEquals("RUNNING", machine().phase);
        assertEquals("Running", client.guests.get(source));
        assertEquals(0, client.restores);
        client.failVerify = false;
        runtime.start(TIMEOUT);
        assertTrue(client.checkpoints.isEmpty());
        assertEquals(1, client.guests.size());
    }

    @Test
    void uncertainRestoreNeverReplaysCheckpointWhenTargetIsMissing() throws Exception {
        runtime.start(TIMEOUT);
        runtime.stop(TIMEOUT);
        client.failRestore = true;
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        assertEquals("RESTORING", machine().phase);
        assertTrue(client.guests.isEmpty());
        client.failRestore = false;
        restart();
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        assertEquals(1, client.restores);
        assertEquals(1, client.checkpoints.size(), "Keep the checkpoint for deliberate operator recovery");
    }

    @Test
    void lostRestoreResponseAdoptsAlreadyRunningGenerationAfterRestart() throws Exception {
        runtime.start(TIMEOUT);
        runtime.stop(TIMEOUT);
        client.loseRestoreResponse = true;
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        assertEquals(1, client.guests.size());
        restart();
        runtime.start(TIMEOUT);
        assertTrue(runtime.isRunning());
        assertEquals(1, client.restores);
        assertTrue(client.checkpoints.isEmpty());
    }

    @Test
    void staleHibernateJournalCannotReplayAcknowledgedWorkerState() throws Exception {
        runtime.start(TIMEOUT);
        runtime.stop(TIMEOUT);
        byte[] stale = mapper.writeValueAsBytes(machine());
        runtime.start(TIMEOUT);
        client.guests.clear();
        MicrosandboxWorkloadMachine saved = mapper.readValue(stale, MicrosandboxWorkloadMachine.class);
        journal.putForAccount(saved.account, saved.id, saved);
        restart();
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        assertEquals(1, client.restores);
    }

    @Test
    void lostSourceRemovalResponseFailsClosedWithoutIssuingRestore() throws Exception {
        runtime.start(TIMEOUT);
        runtime.stop(TIMEOUT);
        client.loseRemoveResponse = true;
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        restart();
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        assertEquals(0, client.restores);
        assertTrue(client.guests.isEmpty());
    }

    @Test
    void failedSourceRemovalCanRetryBeforeAnyWorkerResumes() throws Exception {
        runtime.start(TIMEOUT);
        runtime.stop(TIMEOUT);
        client.failRemove = true;
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        assertEquals(0, client.restores);
        client.failRemove = false;
        runtime.start(TIMEOUT);
        assertEquals(1, client.restores);
        assertTrue(runtime.isRunning());
    }

    @Test
    void concurrentActivationsRestoreOnlyOneGeneration() throws Exception {
        runtime.start(TIMEOUT);
        runtime.stop(TIMEOUT);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> activations = new ArrayList<>();
            for (int i = 0; i < 24; i++) {
                activations.add(executor.submit(() -> {
                    try {
                        runtime.start(TIMEOUT);
                        runtime.awaitReady(TIMEOUT);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }));
            }
            for (Future<?> activation : activations) {
                activation.get(10, TimeUnit.SECONDS);
            }
        }
        assertEquals(1, client.restores);
        assertEquals(1, client.guests.size());
    }

    @Test
    void missingOrStoppedRunningGuestIsNeverRecreated() throws Exception {
        runtime.start(TIMEOUT);
        client.guests.put(machine().vm, "Stopped");
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        client.guests.clear();
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        assertEquals(1, client.runs);
        assertEquals(0, client.restores);
    }

    @Test
    void lostInitialRunResponseDoesNotLaunchAnotherConsumer() throws Exception {
        client.loseRunResponse = true;
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        restart();
        runtime.start(TIMEOUT);
        assertTrue(runtime.isRunning());
        assertEquals(1, client.runs);
    }

    @Test
    void missingJournalWithVmOrCheckpointRefusesFreshCreation() throws Exception {
        runtime.start(TIMEOUT);
        journal.clear();
        restart();
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        String orphan = client.guests.keySet().iterator().next();
        client.guests.clear();
        client.checkpoints.add(orphan + ":orphan");
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        assertEquals(1, client.runs);
    }

    @Test
    void persistedWorkloadRejectsImageAndEnvironmentChanges() throws Exception {
        runtime.start(TIMEOUT);
        assertThrows(IllegalStateException.class, () -> provider.create(definition("image:2")));
        assertFalse(mapper.writeValueAsString(machine()).contains("secret-value"));
        assertEquals(1, client.runs);
    }

    @Test
    void clearRemovesOnlyOwnedVmsAndCheckpointsIncludingConsumedSource() throws Exception {
        runtime.start(TIMEOUT);
        runtime.stop(TIMEOUT);
        client.failRestore = true;
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        client.guests.put("foreign-vm", "Running");
        client.guests.put(machine().id + "-foreign", "Running");
        client.checkpoints.add("foreign-vm:checkpoint");
        provider.clear();
        assertEquals(Set.of("foreign-vm", machineId() + "-foreign"), client.guests.keySet());
        assertEquals(Set.of("foreign-vm:checkpoint"), client.checkpoints);
        assertTrue(journal.scanAllAccounts().isEmpty());
    }

    @Test
    void failedResetRetainsJournalForRetry() throws Exception {
        runtime.start(TIMEOUT);
        client.failRemove = true;
        assertThrows(IllegalStateException.class, provider::clear);
        assertEquals(1, journal.scanAllAccounts().size());
        client.failRemove = false;
        provider.clear();
        assertTrue(journal.scanAllAccounts().isEmpty());
        assertTrue(client.guests.isEmpty());
    }

    @Test
    void foreignReplacementCannotBeAdoptedOrRemoved() throws Exception {
        runtime.start(TIMEOUT);
        client.configurations.get(machine().vm).withObject("labels").put("io.floci.owner", "another-deployment");
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        assertThrows(IllegalStateException.class, provider::clear);
        assertEquals(1, client.guests.size());
        assertEquals(1, journal.scanAllAccounts().size());
    }

    @Test
    void restoredGuestRequiresMatchingSnapshotLineageInBothConfigurations() throws Exception {
        runtime.start(TIMEOUT);
        runtime.stop(TIMEOUT);
        runtime.start(TIMEOUT);
        client.activeOverride = mapper.createObjectNode().put("snapshot_parent", "foreign-checkpoint");
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        assertThrows(IllegalStateException.class, provider::clear);
        assertEquals(1, client.guests.size());
    }

    @Test
    void differentApiPortsDoNotAdoptEachOthersWorkload() throws Exception {
        runtime.start(TIMEOUT);
        String first = machine().vm;
        when(config.port()).thenReturn(4567);
        MicrosandboxWorkloadProvider other = new MicrosandboxWorkloadProvider(config, mapper, storageFactory, client);
        WorkloadRuntime second = other.create(definition("image:1"));
        second.start(TIMEOUT);
        assertEquals(2, client.guests.size());
        assertTrue(client.guests.containsKey(first));
    }

    @Test
    void capacityTracksLiveMemoryAndRetainsUncertainRestoreReservation() throws Exception {
        runtime.start(TIMEOUT);
        assertEquals(512, capacity.reservedMemoryMib());
        runtime.stop(TIMEOUT);
        assertEquals(0, capacity.reservedMemoryMib());
        client.failRestore = true;
        assertThrows(IllegalStateException.class, () -> runtime.start(TIMEOUT));
        assertEquals(512, capacity.reservedMemoryMib());
        provider.clear();
        assertEquals(0, capacity.reservedMemoryMib());
    }

    @Test
    void capacityRecountsJournalOnRestartUntilStoppedSourceIsVerified() throws Exception {
        runtime.start(TIMEOUT);
        runtime.stop(TIMEOUT);
        capacity = new MicrosandboxCapacity(config);
        restart();
        assertEquals(512, capacity.reservedMemoryMib());
        assertFalse(runtime.isRunning());
        assertEquals(0, capacity.reservedMemoryMib());
    }

    @Test
    void exhaustedCapacityCannotLaunchANewGuest() {
        capacity.reserve("another-provider", 8192, TIMEOUT);
        assertThrows(MicrosandboxCapacity.CapacityUnavailableException.class,
                () -> runtime.start(Duration.ofMillis(10)));
        assertEquals(0, client.runs);
        assertTrue(client.guests.isEmpty());
    }

    private String machineId() {
        return client.commands.stream().filter(command -> command.getFirst().equals("run"))
                .findFirst().orElseThrow().get(3).replaceFirst("-0$", "");
    }

    private MicrosandboxWorkloadMachine machine() {
        return journal.scanAllAccounts().getFirst();
    }

    private void restart() throws Exception {
        for (MicrosandboxWorkloadMachine machine : journal.scanAllAccounts()) {
            MicrosandboxWorkloadMachine saved = mapper.readValue(mapper.writeValueAsBytes(machine),
                    MicrosandboxWorkloadMachine.class);
            journal.putForAccount(saved.account, saved.id, saved);
        }
        provider = new MicrosandboxWorkloadProvider(config, mapper, storageFactory, client, capacity);
        runtime = provider.create(definition("image:1"));
    }

    private WorkloadDefinition definition(String image) {
        return new WorkloadDefinition("app", true, ACCOUNT, "us-east-1", "app.local", RuntimeType.MICROSANDBOX,
                "app", "default", 1, null, 8080, Duration.ofSeconds(1), TIMEOUT,
                List.of("work"), List.of(), null, "/health", 200, null, BackendProtocol.HTTP1,
                List.of(), new MicrosandboxSpec(image, List.of("serve"), Map.of("PASSWORD", "secret-value"), 1, 512, 4096));
    }

    private final class FakeClient extends MicrosandboxClient {
        private final Map<String, String> guests = new LinkedHashMap<>();
        private final Set<String> checkpoints = new LinkedHashSet<>();
        private final Map<String, String> checkpointIds = new LinkedHashMap<>();
        private final Map<String, ObjectNode> configurations = new LinkedHashMap<>();
        private final List<List<String>> commands = new ArrayList<>();
        private ObjectNode activeOverride;
        private int runs;
        private int restores;
        private boolean failVerify;
        private boolean failRestore;
        private boolean failRemove;
        private boolean loseRestoreResponse;
        private boolean loseRemoveResponse;
        private boolean loseRunResponse;

        private FakeClient() {
            super("http://localhost:8186", "token", mapper);
        }

        @Override
        public void checkAvailable(Duration timeout) {
            // This fake exercises lifecycle outcomes independently of the HTTP transport contract.
        }

        @Override
        public String prepareAdmission(Duration timeout, List<String> arguments, String source) {
            assertEquals("Stopped", guests.get(source));
            return "test-permit";
        }

        @Override
        public String commandAdmitted(Duration timeout, String permit, List<String> arguments) {
            assertEquals("test-permit", permit);
            return command(timeout, arguments.toArray(String[]::new));
        }

        @Override
        public int port(Duration timeout) {
            return 18080;
        }

        @Override
        public String command(Duration timeout, String... arguments) {
            List<String> args = List.of(arguments);
            commands.add(args);
            switch (args.getFirst()) {
                case "run" -> {
                    runs++;
                    String vm = args.get(args.indexOf("--name") + 1);
                    guests.put(vm, "Running");
                    ObjectNode configuration = mapper.createObjectNode();
                    ObjectNode labels = configuration.putObject("labels");
                    for (int i = 0; i < args.size() - 1; i++) {
                        if (args.get(i).equals("--label")) {
                            String[] label = args.get(i + 1).split("=", 2);
                            labels.put(label[0], label[1]);
                        }
                    }
                    configurations.put(vm, configuration);
                    if (loseRunResponse) {
                        throw new IllegalStateException("Lost run response");
                    }
                }
                case "pause" -> guests.put(args.getLast(), "Paused");
                case "resume" -> guests.put(args.getLast(), "Running");
                case "stop" -> guests.put(args.getLast(), "Stopped");
                case "rm" -> {
                    if (failRemove) {
                        throw new IllegalStateException("Remove failed");
                    }
                    guests.remove(args.getLast());
                    configurations.remove(args.getLast());
                    if (loseRemoveResponse) {
                        throw new IllegalStateException("Lost remove response");
                    }
                }
                case "snap" -> {
                    switch (args.get(1)) {
                        case "create" -> {
                            String checkpoint = args.get(args.indexOf("--sandbox") + 1) + ":" + args.get(2);
                            checkpoints.add(checkpoint);
                            checkpointIds.put(checkpoint, "snap-" + checkpointIds.size());
                        }
                        case "verify" -> {
                            if (failVerify) {
                                throw new IllegalStateException("Verification failed");
                            }
                        }
                        case "restore" -> {
                            restores++;
                            String source = args.get(2).substring(0, args.get(2).indexOf(':'));
                            assertFalse(guests.containsKey(source), "Source fence must be consumed before any worker resumes");
                            if (failRestore) {
                                throw new IllegalStateException("Restore failed without a target");
                            }
                            String vm = args.get(args.indexOf("--name") + 1);
                            guests.put(vm, "Running");
                            configurations.put(vm, mapper.createObjectNode()
                                    .put("snapshot_parent", checkpointIds.get(args.get(2))));
                            if (loseRestoreResponse) {
                                throw new IllegalStateException("Lost restore response");
                            }
                        }
                        case "rm" -> checkpoints.remove(args.getLast());
                        default -> throw new IllegalArgumentException("Unsupported snapshot operation");
                    }
                }
                default -> throw new IllegalArgumentException("Unsupported command");
            }
            return "";
        }

        @Override
        public JsonNode json(Duration timeout, String... arguments) {
            if (arguments[0].equals("inspect")) {
                ObjectNode result = mapper.createObjectNode().put("status", guests.get(arguments[3]));
                result.set("config", configurations.get(arguments[3]));
                result.set("active_config", activeOverride == null ? configurations.get(arguments[3]) : activeOverride);
                return result;
            }
            ArrayNode result = mapper.createArrayNode();
            if (arguments[0].equals("ls")) {
                guests.forEach((name, status) -> result.addObject().put("name", name).put("status", status));
            } else {
                checkpoints.forEach(checkpoint -> {
                    String[] parts = checkpoint.split(":", 2);
                    result.addObject().put("group", parts[0]).put("name", parts[1])
                            .put("snapshot_id", checkpointIds.getOrDefault(checkpoint, "snap-orphan"));
                });
            }
            return result;
        }
    }
}
