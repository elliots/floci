package io.github.hectorvent.floci.services.rds.container;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.docker.ContainerLiveness;
import io.github.hectorvent.floci.core.common.microsandbox.MicrosandboxCapacity;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.services.rds.model.DatabaseEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InOrder;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MicrosandboxRdsRuntimeTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, String> guests = new LinkedHashMap<>();
    private final Map<String, String> nativeIds = new LinkedHashMap<>();
    private final AccountAwareStorageBackend<MicrosandboxMachine> journal =
            AccountAwareStorageBackend.inMemory("000000000000");
    private final String arn = "arn:aws:rds:us-east-1:000000000000:cluster:db";
    private MicrosandboxSidecarClient client;
    private MicrosandboxRdsRuntime runtime;
    private RdsContainerHandle handle;
    private boolean failVerify;
    private boolean failRestore;
    private boolean refuseRestore;
    private boolean failRemove;
    private int restores;
    private EmulatorConfig config;
    private MicrosandboxSidecarManager manager;
    private StorageFactory storageFactory;
    private MicrosandboxCapacity capacity;

    @BeforeEach
    void setup() {
        config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.defaultAccountId()).thenReturn("000000000000");
        when(config.services().rds().microsandboxBackendHost()).thenReturn("localhost");
        when(config.services().rds().microsandboxBindHost()).thenReturn("127.0.0.1");
        when(config.services().rds().microsandboxMemoryMib()).thenReturn(512);
        when(config.services().rds().microsandboxRootDiskMib()).thenReturn(4096);
        when(config.microsandbox().maxActiveMemoryMib()).thenReturn(8192);
        when(config.services().rds().auroraAutoPauseEnabled()).thenReturn(true);
        manager = mock(MicrosandboxSidecarManager.class);
        when(manager.isAvailable()).thenReturn(true);
        storageFactory = mock(StorageFactory.class);
        when(storageFactory.create(eq("rds"), anyString(), any(TypeReference.class)))
                .thenAnswer(invocation -> invocation.getArgument(1).equals("rds-microsandbox.json")
                        ? journal : AccountAwareStorageBackend.inMemory("000000000000"));
        client = mock(MicrosandboxSidecarClient.class);
        when(client.port()).thenReturn(15432);
        when(client.prepareAdmission(anyList())).thenReturn("snapshot-permit");
        when(client.command(any(String[].class))).thenAnswer(invocation -> {
            List<String> args = List.of(invocation.getArguments()).stream().map(String.class::cast).toList();
            switch (args.getFirst()) {
                case "--version" -> {
                    return "msb 0.7.8";
                }
                case "run" -> guests.put(args.get(args.indexOf("--name") + 1), "Running");
                case "stop" -> guests.put(args.getLast(), "Stopped");
                case "start", "resume" -> guests.put(args.getLast(), "Running");
                case "rm" -> {
                    if (failRemove) {
                        throw new IllegalStateException("remove failed");
                    }
                    guests.remove(args.getLast());
                }
                case "snap" -> {
                    if (args.get(1).equals("verify") && failVerify) {
                        throw new IllegalStateException("invalid checkpoint");
                    }
                    if (args.get(1).equals("restore")) {
                        if (refuseRestore) {
                            throw new MicrosandboxCapacity.CapacityUnavailableException("shared native budget full");
                        }
                        if (failRestore) {
                            throw new IllegalStateException("restore failed");
                        }
                        restores++;
                        guests.put(args.get(args.indexOf("--name") + 1), "Running");
                    }
                }
                default -> {
                }
            }
            return "";
        });
        when(client.json(any(String[].class))).thenAnswer(invocation -> {
            String command = invocation.getArgument(0);
            if (command.equals("inspect")) {
                ObjectNode inspected = mapper.createObjectNode()
                        .put("status", guests.get((String) invocation.getArguments()[3]));
                inspected.putObject("config").putObject("resources").put("memory_mib", 512);
                return inspected;
            }
            ArrayNode list = mapper.createArrayNode();
            if (command.equals("ls")) {
                guests.forEach((name, status) -> list.addObject().put("name", name).put("status", status)
                        .put("id", nativeIds.getOrDefault(name, name + "-native-id")));
            }
            return list;
        });
        capacity = new MicrosandboxCapacity(config);
        runtime = new MicrosandboxRdsRuntime(config, manager, client, storageFactory, capacity);
        handle = runtime.tryStart(arn, "db", "volume", "volume", DatabaseEngine.POSTGRES,
                "postgres:17.11-alpine", "master", "secret", "appdb");
        AutoPauseListener listener = mock(AutoPauseListener.class);
        when(listener.mayPause()).thenReturn(true);
        runtime.configureAutoPause(arn, 300, listener);
    }

    @AfterEach
    void shutdown() {
        runtime.shutdown();
    }

    @Test
    void snapshotAdmissionPrecedesFreezingAndUsesTheExactPreparedCommand() {
        String source = machine().vm;
        assertTrue(runtime.pauseIfIdle(arn));
        List<String> capture = List.of("snap", "create", machine().snapshot.split(":", 2)[1],
                "--sandbox", source, "--full", "--integrity");
        InOrder order = inOrder(client);
        order.verify(client).prepareAdmission(capture);
        order.verify(client).command("pause", source);
        order.verify(client).commandAdmitted("snapshot-permit", capture);
        verify(client, never()).cancelAdmission(anyString());
    }

    @Test
    void snapshotCapacityRefusalLeavesTheDatabaseRunningAndUnfrozen() {
        when(client.prepareAdmission(anyList())).thenThrow(
                new MicrosandboxCapacity.CapacityUnavailableException("checkpoint disk budget full"));
        String source = machine().vm;
        assertFalse(runtime.pauseIfIdle(arn));
        assertEquals("RUNNING", machine().phase);
        assertNull(machine().snapshot);
        assertEquals("Running", guests.get(source));
        verify(client, never()).command("pause", source);
        verify(client, never()).commandAdmitted(anyString(), anyList());
    }

    @Test
    void failedFreezeCancelsTheUndispatchedSnapshotPermit() {
        doThrow(new IllegalStateException("pause failed")).when(client).command("pause", machine().vm);
        assertFalse(runtime.pauseIfIdle(arn));
        verify(client).cancelAdmission("snapshot-permit");
        verify(client, never()).commandAdmitted(anyString(), anyList());
        assertEquals("RUNNING", machine().phase);
    }

    @Test
    void uncertainSnapshotCaptureRetainsItsPermit() {
        when(client.commandAdmitted(anyString(), anyList())).thenThrow(new IllegalStateException("response lost"));
        assertFalse(runtime.pauseIfIdle(arn));
        verify(client, never()).cancelAdmission(anyString());
    }

    @Test
    void explicitSnapshotDispatchRefusalCancelsItsPermitAndRecoversTheDatabase() {
        when(client.commandAdmitted(anyString(), anyList())).thenThrow(
                new MicrosandboxCapacity.CapacityUnavailableException("disk capacity changed"));
        assertFalse(runtime.pauseIfIdle(arn));
        verify(client).cancelAdmission("snapshot-permit");
        assertEquals("RUNNING", machine().phase);
        assertEquals("Running", guests.get(machine().vm));
    }

    @Test
    void failedVerificationDoesNotDestroySource() {
        failVerify = true;
        assertFalse(runtime.pauseIfIdle(arn));
        assertEquals("RUNNING", machine().phase);
        assertEquals(1, guests.size());
        assertEquals("Running", guests.get(machine().vm));
        assertEquals(0, restores);
    }

    @ParameterizedTest
    @EnumSource(value = DatabaseEngine.class, names = {"POSTGRES", "MYSQL"})
    void restartKeepsHibernatedDatabaseColdUntilFirstConnection(DatabaseEngine engine) throws Exception {
        String runtimeId = arn;
        if (engine == DatabaseEngine.MYSQL) {
            runtime.stop(handle);
            runtimeId = "arn:aws:rds:us-east-1:000000000000:cluster:mysql";
            handle = runtime.tryStart(runtimeId, "mysql", "mysql-volume", "mysql-volume",
                    engine, "mysql:8.0.45", "master", "secret", "appdb");
            AutoPauseListener listener = mock(AutoPauseListener.class);
            when(listener.mayPause()).thenReturn(true);
            runtime.configureAutoPause(runtimeId, 300, listener);
        }
        assertTrue(runtime.pauseIfIdle(runtimeId));
        runtime.stopAll();
        assertEquals(0, restores, "Host shutdown must preserve a sleeping database checkpoint");
        restart();
        clearInvocations(client);
        RdsContainerHandle recovered = runtime.recover(runtimeId);
        runtime.configureAutoPause(runtimeId, 300, AutoPauseListener.NONE);
        assertEquals(handle.getContainerId(), recovered.getContainerId());
        assertEquals(handle.getPort(), recovered.getPort());
        assertEquals(ContainerLiveness.RUNNING, runtime.probeContainer(recovered.getContainerId()));
        assertEquals(0, capacity.reservedMemoryMib());
        verify(client, never()).command(any(String[].class));
        try (RdsBackendGate.Lease first = runtime.enter(recovered.getHost(), recovered.getPort());
             RdsBackendGate.Lease second = runtime.enter(recovered.getHost(), recovered.getPort())) {
            assertEquals(1, restores);
            assertEquals(512, capacity.reservedMemoryMib());
        }
    }

    @Test
    void restartRegistersColdDatabaseWithoutStartingIt() throws Exception {
        runtime.stop(handle);
        restart();
        clearInvocations(client);
        RdsContainerHandle recovered = runtime.recover(arn);
        runtime.configureAutoPause(arn, null, AutoPauseListener.NONE);
        assertEquals(ContainerLiveness.RUNNING, runtime.probeContainer(handle.getContainerId()));
        assertEquals("STOPPED", machine().phase);
        verify(client, never()).command(any(String[].class));
        try (RdsBackendGate.Lease ignored = runtime.enter(recovered.getHost(), recovered.getPort())) {
            assertEquals("RUNNING", machine().phase);
            assertEquals(0, restores);
            verify(client).command("start", machine().vm);
        }
    }

    @Test
    void recoveryNeverCreatesMissingDatabaseStorage() throws Exception {
        runtime.stop(handle);
        restart();
        guests.clear();
        clearInvocations(client);
        assertThrows(IllegalStateException.class, () -> runtime.recover(arn));
        assertThrows(IllegalStateException.class, () -> runtime.recover(arn + "-missing"));
        verify(client, never()).command(any(String[].class));
    }

    @Test
    void recoveryRejectsAnUnrecordedPause() throws Exception {
        guests.put(machine().vm, "Paused");
        restart();
        clearInvocations(client);
        assertThrows(IllegalStateException.class, () -> runtime.recover(arn));
        verify(client, never()).command(any(String[].class));
    }

    @Test
    void recoveryRetainsCapacityForAStillActiveRestoreSource() throws Exception {
        MicrosandboxMachine machine = machine();
        machine.retiredVms.add(machine.vm);
        machine.vm = machine.id + "-1";
        machine.generation = 1;
        machine.phase = "RESTORING";
        machine.snapshot = machine.id + "-0:sleep";
        restart();
        clearInvocations(client);
        assertNotNull(runtime.recover(arn));
        assertEquals(512, capacity.reservedMemoryMib());
        verify(client, never()).command(any(String[].class));
    }

    @Test
    void nativeDiskSizeIsPersistedForNewDatabasesAndNeverResizesExistingOnes() throws Exception {
        when(config.services().rds().microsandboxRootDiskMib()).thenReturn(8192);
        String mysqlArn = "arn:aws:rds:us-east-1:000000000000:db:mysql";
        RdsContainerHandle mysql = runtime.tryStart(mysqlArn, "mysql", "mysql-volume", "mysql-volume",
                DatabaseEngine.MYSQL, "mysql:8.0.45", "master", "secret", "appdb");
        MicrosandboxMachine saved = journal.scanAllAccounts().stream()
                .filter(candidate -> candidate.runtimeId.equals(mysqlArn)).findFirst().orElseThrow();
        assertEquals(8192, saved.rootDiskMib);
        assertTrue(mockingDetails(client).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("command"))
                .map(invocation -> List.of(invocation.getArguments()))
                .anyMatch(args -> args.contains("--root-disk") && args.contains("8192M")));
        runtime.stop(mysql);
        when(config.services().rds().microsandboxRootDiskMib()).thenReturn(16384);
        restart();
        clearInvocations(client);
        runtime.recover(mysqlArn);
        assertEquals(8192, journal.scanAllAccounts().stream()
                .filter(candidate -> candidate.runtimeId.equals(mysqlArn)).findFirst().orElseThrow().rootDiskMib);
        verify(client, never()).command(any(String[].class));
        ObjectNode legacy = mapper.valueToTree(machine());
        legacy.remove("rootDiskMib");
        assertEquals(4096, mapper.treeToValue(legacy, MicrosandboxMachine.class).rootDiskMib);
    }

    @Test
    void sharedCapacityRefusalReleasesLocalBudgetAndRetainsDatabaseCheckpoint() throws Exception {
        assertTrue(runtime.pauseIfIdle(arn));
        String source = machine().vm;
        String checkpoint = machine().snapshot;
        refuseRestore = true;
        IllegalStateException refused = assertThrows(IllegalStateException.class,
                () -> runtime.enter(handle.getHost(), handle.getPort()));
        assertInstanceOf(MicrosandboxCapacity.CapacityUnavailableException.class, refused.getCause());
        assertEquals(0, capacity.reservedMemoryMib());
        assertEquals("Stopped", guests.get(source));
        assertEquals(checkpoint, machine().snapshot);
        assertEquals(0, restores);
        refuseRestore = false;
        try (RdsBackendGate.Lease ignored = runtime.enter(handle.getHost(), handle.getPort())) {
            assertEquals("RUNNING", machine().phase);
            assertEquals(1, restores);
        }
    }

    @Test
    void failedRestoreKeepsGateClosedAndCanBeRetried() throws Exception {
        assertTrue(runtime.pauseIfIdle(arn));
        assertEquals(0, capacity.reservedMemoryMib());
        failRestore = true;
        assertThrows(IllegalStateException.class, () -> runtime.enter(handle.getHost(), handle.getPort()));
        assertEquals("RESTORING", machine().phase);
        assertEquals(512, capacity.reservedMemoryMib(), "An uncertain restore must retain its RAM reservation");
        failRestore = false;
        try (RdsBackendGate.Lease ignored = runtime.enter(handle.getHost(), handle.getPort())) {
            assertEquals("RUNNING", machine().phase);
            assertEquals(1, restores);
            assertFalse(runtime.pauseIfIdle(arn));
        }
    }

    @Test
    void sourceRemovalMustSucceedBeforeResumedClientsCanWrite() throws Exception {
        assertTrue(runtime.pauseIfIdle(arn));
        failRemove = true;
        assertThrows(IllegalStateException.class, () -> runtime.enter(handle.getHost(), handle.getPort()));
        assertEquals("RESTORING", machine().phase);
        assertEquals(1, restores);
        failRemove = false;
        try (RdsBackendGate.Lease ignored = runtime.enter(handle.getHost(), handle.getPort())) {
            assertEquals(1, restores, "Retry adopts the already restored guest instead of replaying memory");
            assertEquals(1, guests.size());
        }
    }

    @Test
    void aStaleJournalCannotReplayMemoryWhenBothGenerationsAreGone() {
        assertTrue(runtime.pauseIfIdle(arn));
        guests.clear();
        assertThrows(IllegalStateException.class, () -> runtime.enter(handle.getHost(), handle.getPort()));
        assertEquals(0, restores);
    }

    @Test
    void activeStorageCannotBeDeleted() {
        assertThrows(IllegalStateException.class, () -> runtime.removeVolume(arn, "volume", "volume"));
        assertEquals(1, guests.size());
    }

    @Test
    void restartAdoptsRestoredGuestAfterLostSourceRemovalResponse() throws Exception {
        assertTrue(runtime.pauseIfIdle(arn));
        failRemove = true;
        assertThrows(IllegalStateException.class, () -> runtime.enter(handle.getHost(), handle.getPort()));
        restart();
        failRemove = false;
        runtime.tryStart(arn, "db", "volume", "volume", DatabaseEngine.POSTGRES,
                "postgres:17.11-alpine", "master", "secret", "appdb");
        assertEquals(1, restores);
        assertEquals(1, guests.size());
        assertEquals("RUNNING", machine().phase);
    }

    @Test
    void staleRestoringJournalCannotReplayMemoryAfterBothGuestsDisappear() throws Exception {
        assertTrue(runtime.pauseIfIdle(arn));
        failRemove = true;
        assertThrows(IllegalStateException.class, () -> runtime.enter(handle.getHost(), handle.getPort()));
        restart();
        guests.clear();
        assertThrows(IllegalStateException.class, () -> runtime.tryStart(arn, "db", "volume", "volume",
                DatabaseEngine.POSTGRES, "postgres:17.11-alpine", "master", "secret", "appdb"));
        assertEquals(1, restores);
    }

    @Test
    void staleCreatingJournalCannotRecreateALostDatabase() throws Exception {
        machine().phase = "CREATING";
        restart();
        guests.clear();
        assertThrows(IllegalStateException.class, () -> runtime.tryStart(arn, "db", "volume", "volume",
                DatabaseEngine.POSTGRES, "postgres:17.11-alpine", "master", "secret", "appdb"));
        assertTrue(guests.isEmpty());
    }

    @Test
    void legacyJournalRemainsPostgresWithoutEngineOrServerOptions() throws Exception {
        ObjectNode legacy = mapper.valueToTree(machine());
        legacy.remove(List.of("engine", "serverArguments"));
        MicrosandboxMachine saved = mapper.treeToValue(legacy, MicrosandboxMachine.class);
        journal.putForAccount(saved.account, saved.id, saved);
        restart();
        assertEquals(DatabaseEngine.POSTGRES, machine().engine);
        assertTrue(machine().serverArguments.isEmpty());
        runtime.tryStart(arn, "db", "volume", "volume", DatabaseEngine.POSTGRES,
                "postgres:17.11-alpine", "master", "secret", "appdb");
        assertEquals(1, guests.size());
    }

    @Test
    void capacityFollowsConfirmedHibernateResumeAndStop() throws Exception {
        assertEquals(512, capacity.reservedMemoryMib());
        assertEquals(512, machine().memoryMib);
        assertTrue(runtime.pauseIfIdle(arn));
        assertEquals(0, capacity.reservedMemoryMib());
        try (RdsBackendGate.Lease ignored = runtime.enter(handle.getHost(), handle.getPort())) {
            assertEquals(512, capacity.reservedMemoryMib());
        }
        runtime.stop(handle);
        assertEquals(0, capacity.reservedMemoryMib());
    }

    @Test
    void legacyJournalMigratesActualGuestMemoryInsteadOfChangedConfiguration() throws Exception {
        ObjectNode legacy = mapper.valueToTree(machine());
        legacy.remove("memoryMib");
        MicrosandboxMachine saved = mapper.treeToValue(legacy, MicrosandboxMachine.class);
        journal.putForAccount(saved.account, saved.id, saved);
        when(config.services().rds().microsandboxMemoryMib()).thenReturn(128);
        restart();
        assertEquals(512, machine().memoryMib);
        assertEquals(512, capacity.reservedMemoryMib());
    }

    @Test
    void legacyJournalWithoutGuestMetadataFailsClosed() throws Exception {
        ObjectNode legacy = mapper.valueToTree(machine());
        legacy.remove("memoryMib");
        MicrosandboxMachine saved = mapper.treeToValue(legacy, MicrosandboxMachine.class);
        journal.putForAccount(saved.account, saved.id, saved);
        guests.clear();
        IllegalStateException failure = assertThrows(IllegalStateException.class, this::restart);
        assertTrue(failure.getMessage().contains("Cannot establish RAM size"));
        assertTrue(guests.isEmpty());
        assertEquals(0, journal.scanAllAccounts().getFirst().memoryMib);
    }

    @Test
    void hibernatedJournalRetainsCapacityUntilGuestStopIsObserved() throws Exception {
        assertTrue(runtime.pauseIfIdle(arn));
        restart();
        assertEquals(512, capacity.reservedMemoryMib());
        assertEquals(ContainerLiveness.RUNNING, runtime.probeContainer(handle.getContainerId()));
        assertEquals(0, capacity.reservedMemoryMib());
    }

    @Test
    void failedHibernateKeepsGuestCapacityReserved() {
        failVerify = true;
        assertFalse(runtime.pauseIfIdle(arn));
        assertEquals(512, capacity.reservedMemoryMib());
        assertEquals("Running", guests.get(machine().vm));
    }

    @Test
    void existingStorageCannotChangeEngine() {
        assertThrows(IllegalStateException.class, () -> runtime.tryStart(arn, "db", "volume", "volume",
                DatabaseEngine.MYSQL, "mysql:8.0.45", "master", "secret", "appdb"));
        assertEquals(DatabaseEngine.POSTGRES, machine().engine);
        assertEquals(1, guests.size());
    }

    @Test
    void mysqlColdStartKeepsPersistedAuthenticationOptionsAfterImageConfigurationChanges() throws Exception {
        String mysqlArn = "arn:aws:rds:us-east-1:000000000000:db:mysql";
        RdsContainerHandle mysql = runtime.tryStart(mysqlArn, "mysql", "mysql-volume", "mysql-volume",
                DatabaseEngine.MYSQL, "mysql:8.0.45", "master", "secret", "appdb");
        runtime.stop(mysql);
        restart();
        clearInvocations(client);
        runtime.tryStart(mysqlArn, "mysql", "mysql-volume", "mysql-volume",
                DatabaseEngine.MYSQL, "mysql:8.4.8", "master", "rotated", "appdb");
        List<String> startup = mockingDetails(client).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("command"))
                .map(invocation -> List.of(invocation.getArguments()).stream().map(String.class::cast).toList())
                .filter(args -> args.getFirst().equals("exec")
                        && args.stream().anyMatch(argument -> argument.contains("floci-mysql-start")))
                .findFirst().orElseThrow();
        assertFalse(startup.contains("-d"), "A managed database job would block later RAM capture");
        assertTrue(startup.contains("--default-authentication-plugin=mysql_native_password"));
        assertFalse(startup.contains("--mysql-native-password=ON"));
        assertEquals(2, guests.size());
    }

    @Test
    void shutdownCompletionRequiresTheSameStoppedNativeGuest() {
        String name = machine().vm;
        when(client.commandWithVerifiedCompletion(any(BooleanSupplier.class), any(String[].class)))
                .thenAnswer(invocation -> {
                    BooleanSupplier completed = invocation.getArgument(0);
                    assertFalse(completed.getAsBoolean(), "A running guest cannot prove shutdown");
                    guests.put(name, "Stopped");
                    assertTrue(completed.getAsBoolean());
                    return "";
                });
        runtime.stop(handle);
        assertEquals("STOPPED", machine().phase);
        assertEquals(0, capacity.reservedMemoryMib());
    }

    @Test
    void shutdownCompletionRejectsReplacementAndMissingNativeGuests() {
        String name = machine().vm;
        when(client.commandWithVerifiedCompletion(any(BooleanSupplier.class), any(String[].class)))
                .thenAnswer(invocation -> {
                    BooleanSupplier completed = invocation.getArgument(0);
                    guests.put(name, "Stopped");
                    nativeIds.put(name, "replacement-native-id");
                    assertFalse(completed.getAsBoolean(), "A replacement cannot prove the owned guest stopped");
                    guests.remove(name);
                    assertFalse(completed.getAsBoolean(), "An absent guest cannot prove shutdown");
                    throw new IllegalStateException("Unverified shutdown");
                });
        assertThrows(IllegalStateException.class, () -> runtime.stop(handle));
        assertEquals("STOPPING", machine().phase);
        assertEquals(512, capacity.reservedMemoryMib());
    }

    @Test
    void rootRotationKeepsSqlWordsInsidePasswordUnchanged() {
        String mysqlArn = "arn:aws:rds:us-east-1:000000000000:db:mysql";
        RdsContainerHandle mysql = runtime.tryStart(mysqlArn, "mysql", "mysql-volume", "mysql-volume",
                DatabaseEngine.MYSQL, "mysql:8.0.45", "root", "secret", "appdb");
        runtime.rotateMasterPassword("mysql", mysql.getContainerId(), DatabaseEngine.MYSQL,
                "root", "secret", "SET PASSWORD = 'quoted'");
        verify(client).execute(eq(List.of("exec", "--no-tty",
                        journal.scanAllAccounts().stream().filter(machine -> machine.runtimeId.equals(mysqlArn))
                                .findFirst().orElseThrow().vm,
                        "--", "mysql", "-uroot", "-psecret")),
                eq("SET PASSWORD FOR 'root'@'%' = 'SET PASSWORD = \\'quoted\\'';"
                        + "SET PASSWORD = 'SET PASSWORD = \\'quoted\\'';"));
    }

    @Test
    void postgresMaintenanceCannotRunAgainstMysql() {
        RdsContainerHandle mysql = runtime.tryStart("arn:aws:rds:us-east-1:000000000000:db:mysql",
                "mysql", "mysql-volume", "mysql-volume",
                DatabaseEngine.MYSQL, "mysql:8.0.45", "root", "secret", "appdb");
        clearInvocations(client);
        assertThrows(IllegalStateException.class, () -> runtime.createPostgresSnapshot(mysql.getContainerId(), "root"));
        assertThrows(IllegalStateException.class, () -> runtime.restorePostgresSnapshot(mysql.getContainerId(), "root", "SQL"));
        assertThrows(IllegalStateException.class, () -> runtime.renamePostgresDatabase(mysql.getContainerId(), "root", "a", "b"));
        verifyNoInteractions(client);
    }

    private void restart() throws Exception {
        runtime.shutdown();
        for (MicrosandboxMachine machine : journal.scanAllAccounts()) {
            MicrosandboxMachine saved = mapper.readValue(mapper.writeValueAsBytes(machine), MicrosandboxMachine.class);
            journal.putForAccount(saved.account, saved.id, saved);
        }
        capacity = new MicrosandboxCapacity(config);
        runtime = new MicrosandboxRdsRuntime(config, manager, client, storageFactory, capacity);
    }

    private MicrosandboxMachine machine() {
        return journal.scanAllAccounts().getFirst();
    }
}
