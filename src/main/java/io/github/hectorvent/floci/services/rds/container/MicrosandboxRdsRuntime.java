package io.github.hectorvent.floci.services.rds.container;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsArnUtils;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.docker.ContainerLiveness;
import io.github.hectorvent.floci.core.common.microsandbox.MicrosandboxCapacity;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.services.rds.model.DatabaseEngine;
import io.github.hectorvent.floci.services.rds.model.DbCluster;
import io.github.hectorvent.floci.services.rds.model.DbInstance;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/** Database provider using native host microVMs and the existing Aurora activity gate. */
@ApplicationScoped
@Typed(MicrosandboxRdsRuntime.class)
public class MicrosandboxRdsRuntime implements RdsBackendRuntime {
    private static final Logger LOG = Logger.getLogger(MicrosandboxRdsRuntime.class);
    private final EmulatorConfig config;
    private final MicrosandboxSidecarManager manager;
    private final MicrosandboxSidecarClient client;
    private final MicrosandboxCapacity capacity;
    private final AccountAwareStorageBackend<MicrosandboxMachine> journal;
    private final Map<String, MicrosandboxMachine> machines = new ConcurrentHashMap<>();
    private final Map<String, AutoPauseController> controllers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("rds-microsandbox-timer").factory());
    private final ExecutorService events = Executors.newSingleThreadExecutor(
            Thread.ofVirtual().name("rds-microsandbox-events").factory());

    public MicrosandboxRdsRuntime(EmulatorConfig config, MicrosandboxSidecarManager manager,
                                  MicrosandboxSidecarClient client, StorageFactory storageFactory) {
        this(config, manager, client, storageFactory, new MicrosandboxCapacity(config));
    }

    @Inject
    public MicrosandboxRdsRuntime(EmulatorConfig config, MicrosandboxSidecarManager manager,
                                  MicrosandboxSidecarClient client, StorageFactory storageFactory,
                                  MicrosandboxCapacity capacity) {
        this.config = config;
        this.manager = manager;
        this.client = client;
        this.capacity = capacity;
        journal = storageFactory.create("rds", "rds-microsandbox.json",
                new TypeReference<Map<String, MicrosandboxMachine>>() {});
        for (MicrosandboxMachine machine : journal.scanAllAccounts()) {
            if (machine.memoryMib < 1) {
                machine.memoryMib = discoverMemory(machine);
                save(machine);
            }
            machines.put(machine.id, machine);
            if (!"STOPPED".equals(machine.phase) && !"WAITING_CAPACITY".equals(machine.phase)) {
                capacity.observeRunning(machine.id, machine.memoryMib);
            }
        }
        AccountAwareStorageBackend<DbInstance> instances = storageFactory.create("rds", "rds-instances.json",
                new TypeReference<Map<String, DbInstance>>() {});
        AccountAwareStorageBackend<DbCluster> clusters = storageFactory.create("rds", "rds-clusters.json",
                new TypeReference<Map<String, DbCluster>>() {});
        for (DbCluster cluster : clusters.scanAllAccounts()) {
            requireOwnedResource(cluster.getDbClusterArn());
        }
        for (DbInstance instance : instances.scanAllAccounts()) {
            if (instance.getDbClusterIdentifier() == null || instance.getDbClusterIdentifier().isBlank()) {
                requireOwnedResource(instance.getDbInstanceArn());
            }
        }
    }

    private int discoverMemory(MicrosandboxMachine machine) {
        List<String> candidates = new ArrayList<>();
        candidates.add(machine.vm);
        candidates.addAll(machine.retiredVms);
        for (String vm : candidates) {
            if (exists(vm)) {
                JsonNode guest = inspect(vm);
                int memory = Math.max(guest.path("config").path("resources").path("memory_mib").asInt(),
                        guest.path("active_config").path("resources").path("memory_mib").asInt());
                if (memory > 0) {
                    return memory;
                }
            }
        }
        throw new IllegalStateException("Cannot establish RAM size for legacy microsandbox database journal "
                + machine.id + "; restore its owned guest metadata before migrating the runtime");
    }

    private void requireOwnedResource(String runtimeId) {
        if (machines.values().stream().noneMatch(machine -> runtimeId.equals(machine.runtimeId))) {
            throw new IllegalStateException("Existing RDS resource has no microsandbox storage journal: "
                    + runtimeId + ". Export and migrate its data before switching providers.");
        }
    }

    @Override
    public RdsContainerHandle recover(String runtimeId) {
        List<MicrosandboxMachine> owned = machines.values().stream()
                .filter(candidate -> runtimeId.equals(candidate.runtimeId)).toList();
        if (owned.size() != 1) {
            throw new IllegalStateException("Persisted native database requires exactly one owned storage journal");
        }
        MicrosandboxMachine machine = owned.getFirst();
        if (!manager.isAvailable()) {
            return null;
        }
        synchronized (machine) {
            if (!List.of("RUNNING", "STOPPED", "STARTING", "STOPPING", "CAPTURING", "HIBERNATED", "RESTORING")
                    .contains(machine.phase)) {
                throw new IllegalStateException("Persisted native database has an incomplete or unknown lifecycle state");
            }
            boolean present = exists(machine.vm);
            String recoveredTarget = machine.id + "-" + (machine.generation + 1);
            boolean hibernatedTarget = !present && "HIBERNATED".equals(machine.phase) && exists(recoveredTarget);
            if (!present && !hibernatedTarget && !"RESTORING".equals(machine.phase)) {
                throw new IllegalStateException("Persisted native database guest is missing; refusing empty recovery");
            }
            if (!present && !hibernatedTarget && machine.retiredVms.stream().noneMatch(this::exists)) {
                throw new IllegalStateException("Native database source and restored guest are both missing");
            }
            String status = present ? inspect(machine.vm).path("status").asText()
                    : hibernatedTarget ? inspect(recoveredTarget).path("status").asText() : "Stopped";
            if (!List.of("Running", "Paused", "Stopped").contains(status)) {
                throw new IllegalStateException("Native database guest has an uncertain runtime state");
            }
            if ("Paused".equals(status) && !List.of("CAPTURING", "HIBERNATED").contains(machine.phase)) {
                throw new IllegalStateException("Native database is paused outside a recorded capture");
            }
            boolean sourcesActive = false;
            for (String source : machine.retiredVms) {
                if (exists(source)) {
                    String sourceStatus = inspect(source).path("status").asText();
                    if (!List.of("Running", "Paused", "Stopped").contains(sourceStatus)) {
                        throw new IllegalStateException("Native database source has an uncertain runtime state");
                    }
                    sourcesActive |= !"Stopped".equals(sourceStatus);
                }
            }
            if (!"Stopped".equals(status) || sourcesActive) {
                capacity.observeRunning(machine.id, machine.memoryMib);
            } else {
                capacity.release(machine.id);
            }
            boolean needsWake = !"RUNNING".equals(machine.phase) || !"Running".equals(status);
            controllers.computeIfAbsent(machine.id, ignored -> controller(machine, needsWake));
            return handle(machine);
        }
    }

    @Override
    public RdsContainerHandle tryStart(String runtimeId, String instanceId, String storageResourceId,
                                       String storageName, DatabaseEngine engine, String image,
                                       String username, String password, String dbName) {
        if (engine != DatabaseEngine.POSTGRES && engine != DatabaseEngine.MYSQL) {
            throw new AwsException("InvalidParameterValue",
                    "The microsandbox runtime currently supports PostgreSQL and MySQL only", 400);
        }
        if (!manager.isAvailable()) {
            return null;
        }
        if (!"msb 0.7.8".equals(client.command("--version"))) {
            throw new IllegalStateException("The microsandbox provider requires runtime 0.7.8");
        }
        String id = "msb-" + UUID.nameUUIDFromBytes(storageName.getBytes(StandardCharsets.UTF_8));
        AtomicBoolean fresh = new AtomicBoolean();
        MicrosandboxMachine machine = machines.computeIfAbsent(id, ignored -> {
            MicrosandboxMachine created = new MicrosandboxMachine();
            created.id = id;
            created.account = AwsArnUtils.isArn(runtimeId)
                    ? AwsArnUtils.parse(runtimeId).accountId() : config.defaultAccountId();
            created.runtimeId = runtimeId;
            created.instanceId = instanceId;
            created.storageName = storageName;
            created.engine = engine;
            created.username = username == null || username.isBlank()
                    ? (engine == DatabaseEngine.MYSQL ? "root" : "postgres") : username;
            created.serverArguments.addAll(RdsContainerManager.buildContainerCmd(engine, image));
            if (engine == DatabaseEngine.MYSQL) {
                created.serverArguments.add("--pid-file=/var/run/mysqld/floci.pid");
            }
            created.host = config.services().rds().microsandboxBackendHost();
            created.port = client.port();
            created.memoryMib = config.services().rds().microsandboxMemoryMib();
            created.rootDiskMib = config.services().rds().microsandboxRootDiskMib();
            if (created.rootDiskMib < 1024) {
                throw new IllegalArgumentException("floci.services.rds.microsandbox-root-disk-mib must be at least 1024");
            }
            created.vm = id + "-0";
            created.phase = "CREATING";
            capacity.reserve(id, created.memoryMib, Duration.ofSeconds(90));
            save(created);
            fresh.set(true);
            return created;
        });
        synchronized (machine) {
            if (!runtimeId.equals(machine.runtimeId)) {
                throw new IllegalStateException("Microsandbox storage is owned by another runtime");
            }
            if (engine != machine.engine) {
                throw new IllegalStateException("Microsandbox storage belongs to a different database engine");
            }
            if ("WAITING_CAPACITY".equals(machine.phase)) {
                capacity.reserve(machine.id, machine.memoryMib, Duration.ofSeconds(90));
                machine.phase = "CREATING";
                save(machine);
                fresh.set(true);
            }
            if ("CREATING".equals(machine.phase)) {
                if (!exists(machine.vm)) {
                    if (!fresh.get()) {
                        throw new IllegalStateException("Previous guest creation has an unknown outcome; "
                                + "refusing to recreate a potentially lost database");
                    }
                    if (machine.port < 1 || machine.port > 65535) {
                        throw new IllegalStateException("Helper returned an invalid database port");
                    }
                    try {
                        createGuest(machine, image, password, dbName);
                    } catch (MicrosandboxCapacity.CapacityUnavailableException refused) {
                        capacity.release(machine.id);
                        machine.phase = "WAITING_CAPACITY";
                        save(machine);
                        throw refused;
                    }
                }
                ready(machine);
                if (machine.engine == DatabaseEngine.POSTGRES) {
                    sql(machine, RdsContainerManager.postgresIamRoleInitSql());
                } else {
                    client.command("exec", "--no-tty", machine.vm, "--", "mysql", "--protocol=tcp",
                            "-h127.0.0.1", "-u" + machine.username, "-p" + password, "-e", "SELECT 1");
                }
                machine.phase = "RUNNING";
                save(machine);
            } else {
                wake(machine);
            }
        }
        controllers.computeIfAbsent(id, ignored -> controller(machine));
        return handle(machine);
    }

    private void createGuest(MicrosandboxMachine machine, String image, String password, String dbName) {
        String database = dbName == null || dbName.isBlank() ? machine.username : dbName;
        List<String> command = new ArrayList<>(List.of("run", "-d", "--name", machine.vm, "--cpus", "1", "--memory",
                machine.memoryMib + "M", "--root-disk", machine.rootDiskMib + "M"));
        if (machine.engine == DatabaseEngine.POSTGRES) {
            command.addAll(List.of("-e", "POSTGRES_USER=" + machine.username,
                    "-e", "POSTGRES_PASSWORD=" + password, "-e", "POSTGRES_DB=" + database));
        } else {
            command.addAll(List.of("-e", "MYSQL_ROOT_PASSWORD=" + password, "-e", "MYSQL_DATABASE=" + database));
            if (!"root".equals(machine.username)) {
                command.addAll(List.of("-e", "MYSQL_USER=" + machine.username, "-e", "MYSQL_PASSWORD=" + password));
            }
        }
        command.addAll(List.of("-p", binding(machine), image));
        if (machine.engine == DatabaseEngine.MYSQL) {
            // The OCI entrypoint delegates sh unchanged. Install grants before its first database
            // initialization, then run it normally. Arguments never pass through a host shell.
            command.addAll(List.of("--", "sh", "-c",
                    "set -e; printf '%s' \"$1\" > /docker-entrypoint-initdb.d/floci-master.sql; "
                            + "shift; exec docker-entrypoint.sh \"$@\"", "sh",
                    RdsContainerManager.needsMasterGrant(machine.engine, machine.username)
                            ? RdsContainerManager.mysqlMasterGrantSql(machine.username) : "", "mysqld"));
            command.addAll(machine.serverArguments);
        }
        client.command(command.toArray(String[]::new));
    }

    private AutoPauseController controller(MicrosandboxMachine machine) {
        return controller(machine, false);
    }

    private AutoPauseController controller(MicrosandboxMachine machine, boolean initiallyPaused) {
        AutoPauseController.Freezer freezer = new AutoPauseController.Freezer() {
            @Override
            public void pause(String id) {
                hibernate(machine);
            }

            @Override
            public void unpause(String id) {
                synchronized (machine) {
                    wake(machine);
                }
            }
        };
        return new AutoPauseController(machine.id, machine.host, machine.port, freezer,
                (task, delay) -> timer.schedule(() -> Thread.ofVirtual().start(task),
                        delay.toMillis(), TimeUnit.MILLISECONDS),
                events, Clock.systemUTC(), Duration.ofMillis(config.services().rds().auroraResumeDelayMillis()),
                initiallyPaused);
    }

    private void hibernate(MicrosandboxMachine machine) {
        synchronized (machine) {
            String snapshotName = "sleep-" + UUID.randomUUID();
            List<String> capture = List.of("snap", "create", snapshotName,
                    "--sandbox", machine.vm, "--full", "--integrity");
            String permit = client.prepareAdmission(capture);
            boolean captureDispatched = false;
            try {
                machine.phase = "CAPTURING";
                machine.snapshot = machine.vm + ":" + snapshotName;
                save(machine);
                client.command("pause", machine.vm);
                captureDispatched = true;
                client.commandAdmitted(permit, capture);
                client.command("snap", "verify", machine.snapshot);
                // Journal the verified recovery point before the source can disappear.
                machine.phase = "HIBERNATED";
                save(machine);
                client.command("stop", "--force", machine.vm);
                if (!"Stopped".equals(inspect(machine.vm).path("status").asText())) {
                    throw new IllegalStateException("Microsandbox source did not stop");
                }
                capacity.release(machine.id);
            } catch (RuntimeException failure) {
                if (!captureDispatched || failure instanceof MicrosandboxCapacity.CapacityUnavailableException) {
                    try {
                        client.cancelAdmission(permit);
                    } catch (RuntimeException cancelFailure) {
                        failure.addSuppressed(cancelFailure);
                    }
                }
                try {
                    wake(machine);
                } catch (RuntimeException recoveryFailure) {
                    failure.addSuppressed(recoveryFailure);
                }
                throw failure;
            }
        }
    }

    /** Reconciles a lost helper response without ever replaying a checkpoint over acknowledged writes. */
    private void wake(MicrosandboxMachine machine) {
        if ("STOPPING".equals(machine.phase)) {
            stopGuest(machine);
        }
        if ("CAPTURING".equals(machine.phase)) {
            client.command("resume", machine.vm);
            if (machine.snapshot != null) {
                machine.retiredSnapshots.add(machine.snapshot);
                machine.snapshot = null;
            }
            machine.phase = "RUNNING";
            save(machine);
        }
        if ("HIBERNATED".equals(machine.phase)) {
            String target = machine.id + "-" + (machine.generation + 1);
            boolean sourceExists = exists(machine.vm);
            if (!sourceExists && !exists(target)) {
                throw new IllegalStateException("Neither checkpoint source nor resumed generation exists; refusing stale recovery");
            }
            if (sourceExists && !"Stopped".equals(inspect(machine.vm).path("status").asText())) {
                client.command("stop", "--force", machine.vm);
                if (!"Stopped".equals(inspect(machine.vm).path("status").asText())) {
                    throw new IllegalStateException("Microsandbox database checkpoint source did not stop");
                }
            }
            machine.retiredVms.add(machine.vm);
            machine.vm = machine.id + "-" + ++machine.generation;
            machine.phase = "RESTORING";
            save(machine);
        }
        if ("RESTORING".equals(machine.phase)) {
            if (!exists(machine.vm)) {
                if (machine.retiredVms.stream().noneMatch(this::exists)) {
                    throw new IllegalStateException("Checkpoint source and target are missing; refusing stale recovery");
                }
                capacity.reserve(machine.id, machine.memoryMib, Duration.ofSeconds(90));
                try {
                    client.command("snap", "restore", machine.snapshot, "--name", machine.vm, "-p", binding(machine));
                } catch (MicrosandboxCapacity.CapacityUnavailableException refused) {
                    capacity.release(machine.id);
                    throw refused;
                }
            } else {
                capacity.observeRunning(machine.id, machine.memoryMib);
            }
            ready(machine);
            // Removing the stopped source is a recovery fence: even if the local journal write
            // fails, an old HIBERNATED record cannot replay its checkpoint after this target has
            // accepted writes and subsequently disappeared. A lost rm response is reconciled by ls.
            for (String source : List.copyOf(machine.retiredVms)) {
                if (exists(source)) {
                    client.command("rm", source);
                }
                machine.retiredVms.remove(source);
            }
            machine.retiredSnapshots.add(machine.snapshot);
            machine.snapshot = null;
            machine.phase = "RUNNING";
            // Publish the new generation before exposing SQL readiness.
            save(machine);
        } else if ("STOPPED".equals(machine.phase) || "STARTING".equals(machine.phase)
                || "RUNNING".equals(machine.phase)) {
            if (!exists(machine.vm)) {
                throw new IllegalStateException("Microsandbox durable guest disk is missing; refusing to create an empty database");
            }
            boolean stopped = "Stopped".equals(inspect(machine.vm).path("status").asText());
            boolean coldStart = stopped || !"RUNNING".equals(machine.phase);
            if (coldStart) {
                machine.phase = "STARTING";
                save(machine);
            }
            if (stopped) {
                capacity.reserve(machine.id, machine.memoryMib, Duration.ofSeconds(90));
                try {
                    client.command("start", machine.vm);
                } catch (MicrosandboxCapacity.CapacityUnavailableException refused) {
                    capacity.release(machine.id);
                    machine.phase = "STOPPED";
                    save(machine);
                    throw refused;
                }
            } else {
                capacity.observeRunning(machine.id, machine.memoryMib);
            }
            if (coldStart) {
                // start boots the agent, but does not rerun the OCI image's entrypoint.
                // A tmpfs lock makes a retry after a lost exec response safe. It disappears
                // with the cold guest, unlike the crash-left postmaster PID on its data disk.
                coldStart(machine);
            }
            ready(machine);
            machine.phase = "RUNNING";
            save(machine);
        } else if (!"RUNNING".equals(machine.phase)) {
            throw new IllegalStateException("Microsandbox runtime is not resumable: " + machine.phase);
        }
        cleanupRetired(machine);
    }

    private void coldStart(MicrosandboxMachine machine) {
        // A long-lived managed exec job prevents full RAM capture. An attached
        // bootstrap exits after launching a child with independent standard I/O.
        // The child keeps the tmpfs lock until the database exits.
        if (machine.engine == DatabaseEngine.POSTGRES) {
            client.command("exec", "--no-tty", machine.vm, "--", "sh", "-c",
                    "mkdir /dev/shm/floci-pg-start 2>/dev/null || exit 0; "
                            + "umask 077; (trap '' HUP; trap 'rmdir /dev/shm/floci-pg-start' EXIT; "
                            + "rm -f \"$PGDATA/postmaster.pid\"; docker-entrypoint.sh postgres) "
                            + "</dev/null >/tmp/floci-pg-start.log 2>&1 &");
        } else {
            List<String> command = new ArrayList<>(List.of("exec", "--no-tty", machine.vm, "--", "sh", "-c",
                    "mkdir /dev/shm/floci-mysql-start 2>/dev/null || exit 0; "
                            + "umask 077; (trap '' HUP; trap 'rmdir /dev/shm/floci-mysql-start' EXIT; "
                            + "rm -f /var/run/mysqld/floci.pid; docker-entrypoint.sh mysqld \"$@\") "
                            + "</dev/null >/tmp/floci-mysql-start.log 2>&1 &", "sh"));
            command.addAll(machine.serverArguments);
            client.command(command.toArray(String[]::new));
        }
    }

    private void cleanupRetired(MicrosandboxMachine machine) {
        for (String vm : List.copyOf(machine.retiredVms)) {
            try {
                if (exists(vm)) {
                    client.command("rm", vm);
                }
                machine.retiredVms.remove(vm);
                save(machine);
            } catch (RuntimeException e) {
                LOG.warnv("Retaining retired microsandbox VM {0} for cleanup: {1}", vm, e.getMessage());
            }
        }
        for (String snapshot : List.copyOf(machine.retiredSnapshots)) {
            try {
                // A failed capture may not have created an indexed snapshot.
                boolean exists = false;
                for (JsonNode entry : client.json("snap", "ls", "--format", "json")) {
                    exists |= snapshot.equals(entry.path("group").asText() + ":" + entry.path("name").asText());
                }
                if (exists) {
                    client.command("snap", "rm", "--force", snapshot);
                }
                machine.retiredSnapshots.remove(snapshot);
                save(machine);
            } catch (RuntimeException e) {
                LOG.warnv("Retaining microsandbox checkpoint for cleanup: {0}", e.getMessage());
            }
        }
    }

    private String binding(MicrosandboxMachine machine) {
        return config.services().rds().microsandboxBindHost() + ":" + machine.port + ":" + machine.engine.defaultPort();
    }

    private boolean exists(String name) {
        for (JsonNode entry : client.json("ls", "--format", "json")) {
            if (name.equals(entry.path("name").asText())) {
                return true;
            }
        }
        return false;
    }

    private JsonNode inspect(String name) {
        return client.json("inspect", "--format", "json", name);
    }

    private void ready(MicrosandboxMachine machine) {
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (true) {
            try {
                if (machine.engine == DatabaseEngine.POSTGRES) {
                    client.command("exec", "--no-tty", machine.vm, "--", "pg_isready",
                            "-h", "127.0.0.1", "-p", "5432", "-U", machine.username);
                    sql(machine, "SELECT 1;");
                } else {
                    // TCP excludes the image's temporary socket-only initialization server.
                    // mysqladmin ping reports availability even after credentials have rotated;
                    // it deliberately succeeds on Access denied without storing stale passwords.
                    client.command("exec", "--no-tty", machine.vm, "--", "mysqladmin",
                            "--protocol=tcp", "-h127.0.0.1", "--connect-timeout=2", "ping");
                }
                return;
            } catch (RuntimeException e) {
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException(machine.engine + " did not become ready", e);
                }
                try {
                    Thread.sleep(250);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted waiting for database readiness", interrupted);
                }
            }
        }
    }

    private String sql(MicrosandboxMachine machine, String sql) {
        return client.execute(List.of("exec", "--no-tty", machine.vm, "--", "psql", "-v", "ON_ERROR_STOP=1",
                "-U", machine.username, "-d", "postgres", "-At"), sql);
    }

    private void save(MicrosandboxMachine machine) {
        journal.putForAccount(machine.account, machine.id, machine);
        journal.flush();
    }

    private RdsContainerHandle handle(MicrosandboxMachine machine) {
        return new RdsContainerHandle(machine.id, machine.runtimeId, machine.instanceId,
                machine.host, machine.port, machine.storageName);
    }

    @Override
    public boolean isDockerReachable() {
        return manager.isAvailable();
    }

    @Override
    public void stop(RdsContainerHandle handle) {
        if (handle == null) {
            return;
        }
        MicrosandboxMachine machine = machines.get(handle.getContainerId());
        if (machine == null) {
            throw new IllegalStateException("Unknown microsandbox backend");
        }
        AutoPauseController controller = controllers.remove(machine.id);
        if (controller != null) {
            controller.close();
        }
        synchronized (machine) {
            if ("CREATING".equals(machine.phase) || "WAITING_CAPACITY".equals(machine.phase)) {
                if (exists(machine.vm)) {
                    client.command("stop", "--force", machine.vm);
                    if (!"Stopped".equals(inspect(machine.vm).path("status").asText())) {
                        throw new IllegalStateException("Microsandbox database guest did not stop");
                    }
                }
                capacity.release(machine.id);
                machine.phase = "STOPPED";
                save(machine);
                return;
            }
            if (!"STOPPED".equals(machine.phase)) {
                if (!"STOPPING".equals(machine.phase)) {
                    wake(machine);
                    machine.phase = "STOPPING";
                    save(machine);
                }
                stopGuest(machine);
            }
        }
    }

    private void stopGuest(MicrosandboxMachine machine) {
        if (exists(machine.vm) && !"Stopped".equals(inspect(machine.vm).path("status").asText())) {
            String vm = machine.vm;
            String nativeId = nativeGuestId(vm);
            BooleanSupplier stopped = () -> sameGuestStopped(vm, nativeId);
            if (machine.engine == DatabaseEngine.POSTGRES) {
                client.commandWithVerifiedCompletion(stopped,
                        "exec", "--no-tty", "--user", "postgres", vm, "--", "sh", "-c",
                        "if pg_ctl -D \"$PGDATA\" status >/dev/null 2>&1; then "
                                + "pg_ctl -D \"$PGDATA\" -m fast -w stop; fi");
            } else {
                client.commandWithVerifiedCompletion(stopped,
                        "exec", "--no-tty", vm, "--", "sh", "-c", mysqlShutdownScript());
            }
            if (!"Stopped".equals(inspect(machine.vm).path("status").asText())) {
                client.command("stop", "-t", "30", machine.vm);
            }
        }
        if (exists(machine.vm) && !"Stopped".equals(inspect(machine.vm).path("status").asText())) {
            throw new IllegalStateException("Microsandbox database guest did not stop");
        }
        capacity.release(machine.id);
        machine.phase = "STOPPED";
        save(machine);
    }

    private String nativeGuestId(String name) {
        for (JsonNode guest : client.json("ls", "--format", "json")) {
            if (name.equals(guest.path("name").asText()) && !guest.path("id").asText().isBlank()) {
                return guest.path("id").asText();
            }
        }
        throw new IllegalStateException("Cannot identify the native database guest; rebuild the Go host helper");
    }

    private boolean sameGuestStopped(String name, String nativeId) {
        for (JsonNode guest : client.json("ls", "--format", "json")) {
            if (name.equals(guest.path("name").asText())) {
                return nativeId.equals(guest.path("id").asText())
                        && "Stopped".equals(guest.path("status").asText());
            }
        }
        return false;
    }

    private static String mysqlShutdownScript() {
        // SIGTERM is MySQL's clean shutdown path and does not depend on rotated credentials.
        // Verify the process name before signalling a PID retained in the guest-owned disk.
        return "if [ -f /var/run/mysqld/floci.pid ]; then "
                + "pid=$(cat /var/run/mysqld/floci.pid); "
                + "case \"$pid\" in ''|*[!0-9]*) exit 1;; esac; "
                + "if [ -e \"/proc/$pid/comm\" ]; then "
                + "[ \"$(cat /proc/$pid/comm)\" = mysqld ] || exit 1; "
                + "kill -TERM \"$pid\" || exit 1; "
                + "for attempt in $(seq 1 120); do "
                + "kill -0 \"$pid\" 2>/dev/null || exit 0; sleep 0.25; done; exit 1; fi; fi";
    }

    @Override
    public void stopByRuntimeId(String runtimeId) {
        RdsContainerHandle handle = getActiveHandle(runtimeId);
        if (handle != null) {
            stop(handle);
        }
    }

    @Override
    public ContainerLiveness probeContainer(String backendId) {
        MicrosandboxMachine machine = machines.get(backendId);
        if (machine == null) {
            return ContainerLiveness.NOT_RUNNING;
        }
        synchronized (machine) {
            AutoPauseController controller = controllers.get(machine.id);
            if (controller != null && controller.state() != AutoPauseController.State.RUNNING) {
                return ContainerLiveness.RUNNING;
            }
            if ("HIBERNATED".equals(machine.phase)) {
                try {
                    if (exists(machine.vm) && "Stopped".equals(inspect(machine.vm).path("status").asText())) {
                        capacity.release(machine.id);
                    }
                } catch (RuntimeException e) {
                    LOG.debugv("Cannot reconcile paused database RAM for {0}: {1}", machine.id, e.getMessage());
                }
            }
            if ("HIBERNATED".equals(machine.phase) || "RESTORING".equals(machine.phase)
                    || "CAPTURING".equals(machine.phase)) {
                return ContainerLiveness.RUNNING;
            }
            try {
                return exists(machine.vm) && "Running".equals(inspect(machine.vm).path("status").asText())
                        ? ContainerLiveness.RUNNING : ContainerLiveness.NOT_RUNNING;
            } catch (RuntimeException e) {
                return ContainerLiveness.UNKNOWN;
            }
        }
    }

    @Override
    public RdsContainerHandle getActiveHandle(String runtimeId) {
        return machines.values().stream().filter(machine -> machine.runtimeId.equals(runtimeId))
                .findFirst().map(this::handle).orElse(null);
    }

    @Override
    public void stopAll() {
        for (MicrosandboxMachine machine : machines.values()) {
            try {
                AutoPauseController controller = controllers.remove(machine.id);
                if (controller != null) {
                    controller.closeWithoutResume();
                }
                synchronized (machine) {
                    if ("HIBERNATED".equals(machine.phase) && exists(machine.vm)
                            && "Stopped".equals(inspect(machine.vm).path("status").asText())) {
                        capacity.release(machine.id);
                    } else {
                        stop(handle(machine));
                    }
                }
            } catch (RuntimeException e) {
                LOG.warnv("Could not stop microsandbox backend {0}: {1}", machine.id, e.getMessage());
            }
        }
    }

    @Override
    public void configureAutoPause(String runtimeId, Integer seconds, AutoPauseListener listener) {
        RdsContainerHandle handle = getActiveHandle(runtimeId);
        AutoPauseController controller = handle == null ? null : controllers.get(handle.getContainerId());
        if (controller != null) {
            controller.configure(config.services().rds().auroraAutoPauseEnabled() ? seconds : null, listener);
        }
    }

    boolean pauseIfIdle(String runtimeId) {
        RdsContainerHandle handle = getActiveHandle(runtimeId);
        AutoPauseController controller = handle == null ? null : controllers.get(handle.getContainerId());
        return controller != null && controller.pauseIfIdle();
    }

    @Override
    public Lease enter(String host, int port) throws InterruptedException {
        return enter(host, port, true);
    }

    @Override
    public Lease enter(String host, int port, boolean waitForResume) throws InterruptedException {
        for (AutoPauseController controller : controllers.values()) {
            if (controller.serves(host, port)) {
                return controller.acquire(waitForResume);
            }
        }
        return Lease.NONE;
    }

    private Lease hold(MicrosandboxMachine machine) {
        try {
            return enter(machine.host, machine.port);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted acquiring microsandbox backend", e);
        }
    }

    private MicrosandboxMachine machine(String id) {
        MicrosandboxMachine machine = machines.get(id);
        if (machine == null) {
            throw new IllegalStateException("Unknown microsandbox backend");
        }
        return machine;
    }

    @Override
    public void rotateMasterPassword(String name, String id, DatabaseEngine engine,
                                     String username, String oldPassword, String newPassword) {
        MicrosandboxMachine machine = machine(id);
        try (Lease ignored = hold(machine)) {
            if (machine.engine != engine) {
                throw new IllegalStateException("Password rotation engine does not match microsandbox storage");
            }
            if (machine.engine == DatabaseEngine.POSTGRES) {
                sql(machine, RdsContainerManager.postgresPasswordRotationSql(machine.username, newPassword));
            } else {
                String sql = RdsContainerManager.mysqlPasswordRotationSql(engine, machine.username, newPassword);
                client.execute(List.of("exec", "--no-tty", machine.vm, "--", "mysql",
                        "-u" + machine.username, "-p" + oldPassword), sql);
            }
        }
    }

    @Override
    public String createPostgresSnapshot(String id, String username) {
        MicrosandboxMachine machine = postgresMachine(id);
        try (Lease ignored = hold(machine)) {
            return client.command("exec", "--no-tty", machine.vm, "--", "pg_dumpall", "-U", machine.username);
        }
    }

    @Override
    public void restorePostgresSnapshot(String id, String username, String sqlDump) {
        MicrosandboxMachine machine = postgresMachine(id);
        try (Lease ignored = hold(machine)) {
            client.execute(List.of("exec", "--no-tty", machine.vm, "--", "sh", "-c", "cat > /tmp/dump.sql"), sqlDump);
            client.execute(List.of("exec", "--no-tty", machine.vm, "--", "sh", "-s", "--", machine.username),
                    RdsContainerManager.postgresRestoreScript());
        }
    }

    @Override
    public void renamePostgresDatabase(String id, String username, String from, String to) {
        MicrosandboxMachine machine = postgresMachine(id);
        try (Lease ignored = hold(machine)) {
            client.execute(List.of("exec", "--no-tty", machine.vm, "--", "psql", "-v", "ON_ERROR_STOP=1",
                    "-U", machine.username, "-d", "template1"),
                    "ALTER DATABASE " + RdsContainerManager.quotePostgresIdentifier(from)
                            + " RENAME TO " + RdsContainerManager.quotePostgresIdentifier(to) + ";");
        }
    }

    private MicrosandboxMachine postgresMachine(String id) {
        MicrosandboxMachine machine = machine(id);
        if (machine.engine != DatabaseEngine.POSTGRES) {
            throw new IllegalStateException("This operation requires a PostgreSQL backend");
        }
        return machine;
    }

    @Override
    public void removeVolume(String runtimeId, String resourceId, String storageName) {
        RdsContainerHandle handle = getActiveHandle(runtimeId);
        if (handle == null) {
            return;
        }
        MicrosandboxMachine machine = machine(handle.getContainerId());
        synchronized (machine) {
            if (!"STOPPED".equals(machine.phase) || !storageName.equals(machine.storageName)) {
                throw new IllegalStateException("Refusing to remove active or mismatched microsandbox storage");
            }
            client.awaitQuiescence(Duration.ofSeconds(10));
            cleanupRetired(machine);
            if (!machine.retiredVms.isEmpty() || !machine.retiredSnapshots.isEmpty()) {
                throw new IllegalStateException("Microsandbox retired storage cleanup is pending");
            }
            if (exists(machine.vm)) {
                client.command("rm", machine.vm);
                if (exists(machine.vm)) {
                    throw new IllegalStateException("Microsandbox database guest was not removed");
                }
            }
            capacity.release(machine.id);
            journal.deleteForAccount(machine.account, machine.id);
            journal.flush();
            machines.remove(machine.id, machine);
        }
    }

    @Override
    public void clear() {
        controllers.values().forEach(AutoPauseController::close);
        controllers.clear();
    }

    @PreDestroy
    void shutdown() {
        timer.shutdownNow();
        events.shutdownNow();
    }
}
