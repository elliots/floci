package io.github.hectorvent.floci.runtime.ondemand;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.docker.ContainerStorageHelper;
import io.github.hectorvent.floci.core.common.microsandbox.MicrosandboxCapacity;
import io.github.hectorvent.floci.core.common.microsandbox.MicrosandboxClient;
import io.github.hectorvent.floci.core.common.microsandbox.MicrosandboxOperationJournal;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.runtime.network.NativeNetworkPolicy;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.MicrosandboxSpec;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Optional application VM provider. AWS resource lifecycle policy stays in the service adapters. */
@ApplicationScoped
public class MicrosandboxWorkloadProvider {
    private static final Logger LOG = Logger.getLogger(MicrosandboxWorkloadProvider.class);
    private static final Duration INSPECT_TIMEOUT = Duration.ofSeconds(10);
    private final EmulatorConfig config;
    private final ObjectMapper mapper;
    private final AccountAwareStorageBackend<MicrosandboxWorkloadMachine> journal;
    private final MicrosandboxCapacity capacity;
    private final MicrosandboxOperationJournal operations;
    private final Map<String, MicrosandboxWorkloadMachine> machines = new ConcurrentHashMap<>();
    private final Map<String, WorkloadRuntime> adapters = new ConcurrentHashMap<>();
    private volatile MicrosandboxClient client;

    @Inject
    public MicrosandboxWorkloadProvider(EmulatorConfig config, ObjectMapper mapper, StorageFactory storageFactory,
                                         MicrosandboxCapacity capacity, MicrosandboxOperationJournal operations) {
        this(config, mapper, storageFactory, null, capacity, operations);
    }

    MicrosandboxWorkloadProvider(EmulatorConfig config, ObjectMapper mapper, StorageFactory storageFactory,
                                 MicrosandboxClient client) {
        this(config, mapper, storageFactory, client, new MicrosandboxCapacity(config));
    }

    MicrosandboxWorkloadProvider(EmulatorConfig config, ObjectMapper mapper, StorageFactory storageFactory,
                                 MicrosandboxClient client, MicrosandboxCapacity capacity) {
        this(config, mapper, storageFactory, client, capacity, null);
    }

    private MicrosandboxWorkloadProvider(EmulatorConfig config, ObjectMapper mapper, StorageFactory storageFactory,
                                         MicrosandboxClient client, MicrosandboxCapacity capacity,
                                         MicrosandboxOperationJournal operations) {
        this.config = config;
        this.mapper = mapper;
        this.client = client;
        this.capacity = capacity;
        this.operations = operations;
        journal = storageFactory.create("on-demand", "microsandbox-workloads.json",
                new TypeReference<Map<String, MicrosandboxWorkloadMachine>>() {});
        for (MicrosandboxWorkloadMachine machine : journal.scanAllAccounts()) {
            machines.put(machine.id, machine);
            if (!"NEW".equals(machine.phase)) {
                capacity.observeRunning(machine.id, machine.memoryMib);
            }
        }
    }

    public WorkloadRuntime create(WorkloadDefinition definition) {
        if (definition.type() != WorkloadDefinition.RuntimeType.MICROSANDBOX || definition.microsandbox() == null) {
            throw new IllegalArgumentException("Microsandbox workload configuration is required");
        }
        String namespace = ContainerStorageHelper.ownerIdentity(config);
        String identity = namespace + "/" + definition.accountId() + "/" + definition.region() + "/" + definition.id();
        String id = "floci-workload-" + UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8));
        String specification = fingerprint(definition);
        MicrosandboxWorkloadMachine machine = machines.computeIfAbsent(id, ignored -> {
            MicrosandboxWorkloadMachine fresh = new MicrosandboxWorkloadMachine();
            fresh.id = id;
            fresh.account = definition.accountId();
            fresh.workload = definition.id();
            fresh.specification = specification;
            fresh.host = config.onDemand().microsandboxBackendHost();
            fresh.bindHost = config.onDemand().microsandboxBindHost();
            fresh.guestPort = definition.backendPort();
            fresh.memoryMib = definition.microsandbox().memoryMib();
            fresh.vm = id + "-0";
            return fresh;
        });
        synchronized (machine) {
            if (!specification.equals(machine.specification)) {
                throw new IllegalStateException("Existing microsandbox workload configuration changed: "
                        + definition.id() + ". Reset or migrate the workload before replacing its image or configuration.");
            }
        }
        return adapters.computeIfAbsent(id, ignored -> new RuntimeAdapter(definition, machine));
    }

    private String fingerprint(WorkloadDefinition definition) {
        MicrosandboxSpec spec = definition.microsandbox();
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("image", spec.image());
        value.put("command", spec.command());
        value.put("environment", new TreeMap<>(spec.environment()));
        value.put("cpus", spec.cpus());
        value.put("memoryMib", spec.memoryMib());
        value.put("rootDiskMib", spec.rootDiskMib());
        value.put("guestPort", definition.backendPort());
        value.put("host", config.onDemand().microsandboxBackendHost());
        value.put("bindHost", config.onDemand().microsandboxBindHost());
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(mapper.writeValueAsBytes(value)));
        } catch (JsonProcessingException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Cannot identify microsandbox workload configuration", e);
        }
    }

    private synchronized MicrosandboxClient client() {
        if (client == null) {
            String url = config.onDemand().microsandboxUrl().orElseThrow(() ->
                    new IllegalStateException("Set FLOCI_ON_DEMAND_MICROSANDBOX_URL"));
            String token = config.onDemand().microsandboxToken().orElseThrow(() ->
                    new IllegalStateException("Set FLOCI_ON_DEMAND_MICROSANDBOX_TOKEN"));
            client = new MicrosandboxClient(url, token, mapper, operations);
            client.requireNetworkPolicy(NativeNetworkPolicy.expectedFingerprint(config));
        }
        return client;
    }

    private final class RuntimeAdapter implements WorkloadRuntime {
        private final WorkloadDefinition definition;
        private final MicrosandboxWorkloadMachine machine;

        private RuntimeAdapter(WorkloadDefinition definition, MicrosandboxWorkloadMachine machine) {
            this.definition = definition;
            this.machine = machine;
        }

        @Override
        public boolean isRunning() {
            synchronized (machine) {
                if ("HIBERNATED".equals(machine.phase) && exists(machine.vm, deadline(INSPECT_TIMEOUT))
                        && "Stopped".equals(status(machine, machine.vm, deadline(INSPECT_TIMEOUT)))) {
                    capacity.release(machine.id);
                }
                return "RUNNING".equals(machine.phase)
                        && "Running".equals(status(machine, machine.vm, deadline(INSPECT_TIMEOUT)));
            }
        }

        @Override
        public void start(Duration timeout) {
            long deadline = deadline(timeout);
            synchronized (machine) {
                remaining(deadline);
                client().checkAvailable(remaining(deadline));
                if ("NEW".equals(machine.phase)) {
                    createGuest(definition, machine, deadline);
                } else if ("CREATING".equals(machine.phase)) {
                    requireRunning(machine, machine.vm, deadline);
                    machine.phase = "RUNNING";
                    save(machine);
                } else {
                    wake(machine, deadline);
                }
            }
        }

        @Override
        public void awaitReady(Duration timeout) {
            long deadline = deadline(timeout);
            synchronized (machine) {
                requireRunning(machine, machine.vm, deadline);
                if (!"RUNNING".equals(machine.phase)) {
                    throw new IllegalStateException("Microsandbox workload transition is incomplete");
                }
            }
        }

        @Override
        public void stop(Duration timeout) {
            long deadline = deadline(timeout);
            synchronized (machine) {
                hibernate(machine, deadline);
            }
        }

        @Override
        public void awaitQuiescence(Duration timeout) {
            MicrosandboxClient current = client;
            if (current != null) {
                current.awaitQuiescence(timeout);
            }
        }

        @Override
        public URI backend() {
            synchronized (machine) {
                if (machine.hostPort == 0) {
                    throw new IllegalStateException("Microsandbox workload has no published backend");
                }
                String host = machine.host.contains(":") ? "[" + machine.host + "]" : machine.host;
                return URI.create("http://" + host + ":" + machine.hostPort);
            }
        }
    }

    private void createGuest(WorkloadDefinition definition, MicrosandboxWorkloadMachine machine, long deadline) {
        requireNoRemnants(machine, deadline);
        machine.hostPort = client().port(remaining(deadline));
        capacity.reserve(machine.id, machine.memoryMib, remaining(deadline));
        machine.phase = "CREATING";
        machine.generationParents.put(machine.vm, "");
        save(machine);
        MicrosandboxSpec spec = definition.microsandbox();
        List<String> command = new ArrayList<>(List.of("run", "-d", "--name", machine.vm,
                "--label", "floci.workload=" + machine.id,
                "--label", ContainerStorageHelper.OWNER_LABEL + "=" + ContainerStorageHelper.ownerIdentity(config),
                "--cpus", Integer.toString(spec.cpus()), "--memory", spec.memoryMib() + "M",
                "--root-disk", spec.rootDiskMib() + "M", "--net", "host,private,public"));
        spec.environment().forEach((key, value) -> command.addAll(List.of("-e", key + "=" + value)));
        command.addAll(List.of("-p", binding(machine), spec.image()));
        if (!spec.command().isEmpty()) {
            command.add("--");
            command.addAll(spec.command());
        }
        try {
            client().command(remaining(deadline), command.toArray(String[]::new));
        } catch (MicrosandboxCapacity.CapacityUnavailableException refused) {
            capacity.release(machine.id);
            machine.phase = "NEW";
            save(machine);
            throw refused;
        }
        requireRunning(machine, machine.vm, deadline);
        machine.phase = "RUNNING";
        save(machine);
    }

    private void requireNoRemnants(MicrosandboxWorkloadMachine machine, long deadline) {
        for (JsonNode vm : client().json(remaining(deadline), "ls", "--format", "json")) {
            if (ownedName(machine, vm.path("name").asText())) {
                throw new IllegalStateException("Microsandbox workload has VM state without a recovery journal; refusing replacement");
            }
        }
        for (JsonNode snapshot : client().json(remaining(deadline), "snap", "ls", "--format", "json")) {
            if (ownedName(machine, snapshot.path("group").asText())) {
                throw new IllegalStateException("Microsandbox workload has checkpoints without a recovery journal; refusing replacement");
            }
        }
    }

    private void hibernate(MicrosandboxWorkloadMachine machine, long deadline) {
        if ("NEW".equals(machine.phase)) {
            return;
        }
        if ("HIBERNATED".equals(machine.phase)) {
            stopSource(machine, machine.vm, deadline);
            return;
        }
        if ("CAPTURING".equals(machine.phase)) {
            recoverCapture(machine, deadline);
        }
        if (!"RUNNING".equals(machine.phase)) {
            throw new IllegalStateException("Microsandbox workload cannot hibernate during " + machine.phase);
        }
        requireRunning(machine, machine.vm, deadline);
        machine.phase = "CAPTURING";
        String name = "sleep-" + UUID.randomUUID();
        machine.checkpoint = machine.vm + ":" + name;
        save(machine);
        try {
            client().command(remaining(deadline), "pause", machine.vm);
            if (!"Paused".equals(status(machine, machine.vm, deadline))) {
                throw new IllegalStateException("Microsandbox workload did not pause before capture");
            }
            client().command(remaining(deadline), "snap", "create", name,
                    "--sandbox", machine.vm, "--full", "--integrity");
            client().command(remaining(deadline), "snap", "verify", machine.checkpoint);
            rememberCheckpoint(machine, deadline);
            if (!machine.checkpointIds.containsKey(machine.checkpoint)) {
                throw new IllegalStateException("Microsandbox checkpoint identity is missing");
            }
            machine.phase = "HIBERNATED";
            save(machine);
            stopSource(machine, machine.vm, deadline);
        } catch (RuntimeException failure) {
            if ("CAPTURING".equals(machine.phase)) {
                try {
                    recoverCapture(machine, deadline);
                } catch (RuntimeException recoveryFailure) {
                    failure.addSuppressed(recoveryFailure);
                }
            }
            throw failure;
        }
    }

    private void wake(MicrosandboxWorkloadMachine machine, long deadline) {
        if ("CAPTURING".equals(machine.phase)) {
            recoverCapture(machine, deadline);
        }
        if ("HIBERNATED".equals(machine.phase)) {
            String parent = machine.checkpointIds.get(machine.checkpoint);
            if (parent == null || parent.isBlank()) {
                throw new IllegalStateException("Microsandbox checkpoint has no ownership record");
            }
            machine.source = machine.vm;
            machine.vm = machine.id + "-" + ++machine.generation;
            machine.generationParents.put(machine.vm, parent);
            machine.phase = "RESTORING";
            save(machine);
        }
        if ("RESTORING".equals(machine.phase)) {
            restore(machine, deadline);
        } else if (!"RUNNING".equals(machine.phase)) {
            throw new IllegalStateException("Microsandbox workload is not resumable: " + machine.phase);
        }
        requireRunning(machine, machine.vm, deadline);
        cleanupCheckpoints(machine, deadline);
    }

    private void restore(MicrosandboxWorkloadMachine machine, long deadline) {
        boolean targetExists = exists(machine.vm, deadline);
        boolean sourceExists = exists(machine.source, deadline);
        if (!targetExists) {
            if (!sourceExists) {
                throw new IllegalStateException("Microsandbox restore outcome is unknown and its source was consumed; "
                        + "refusing to replay a checkpoint");
            }
            if (!checkpointExists(machine, machine.checkpoint, deadline)) {
                throw new IllegalStateException("Microsandbox checkpoint is missing");
            }
            stopSource(machine, machine.source, deadline);
            capacity.reserve(machine.id, machine.memoryMib, remaining(deadline));
            List<String> command = List.of("snap", "restore", machine.checkpoint,
                    "--name", machine.vm, "-p", binding(machine), "--net-default", "deny",
                    "--net-rule", "allow:egress@host,allow:egress@private,allow:egress@public",
                    "--net-rule", "allow:ingress@any");
            String permit;
            try {
                permit = client().prepareAdmission(remaining(deadline), command, machine.source);
            } catch (MicrosandboxCapacity.CapacityUnavailableException refused) {
                capacity.release(machine.id);
                throw refused;
            }
            // Consumers resume immediately, before any HTTP readiness check. Remove this one-use
            // recovery fence before restore, so no stale journal can replay acknowledged work.
            client().command(remaining(deadline), "rm", machine.source);
            if (exists(machine.source, deadline)) {
                throw new IllegalStateException("Microsandbox checkpoint source was not removed");
            }
            client().commandAdmitted(remaining(deadline), permit, command);
        } else if (sourceExists) {
            throw new IllegalStateException("Both microsandbox generations exist after source consumption; "
                    + "refusing an ambiguous restore");
        }
        requireRunning(machine, machine.vm, deadline);
        capacity.observeRunning(machine.id, machine.memoryMib);
        machine.generationParents.remove(machine.source);
        machine.retiredCheckpoints.add(machine.checkpoint);
        machine.checkpoint = null;
        machine.source = null;
        machine.phase = "RUNNING";
        save(machine);
    }

    private void recoverCapture(MicrosandboxWorkloadMachine machine, long deadline) {
        String status = status(machine, machine.vm, deadline);
        if ("Paused".equals(status)) {
            client().command(remaining(deadline), "resume", machine.vm);
        } else if (!"Running".equals(status)) {
            throw new IllegalStateException("Microsandbox capture source is not recoverable; refusing stale checkpoint replay");
        }
        requireRunning(machine, machine.vm, deadline);
        if (machine.checkpoint != null) {
            rememberCheckpoint(machine, deadline);
            machine.retiredCheckpoints.add(machine.checkpoint);
            machine.checkpoint = null;
        }
        machine.phase = "RUNNING";
        save(machine);
    }

    private void stopSource(MicrosandboxWorkloadMachine machine, String vm, long deadline) {
        if (!exists(vm, deadline)) {
            throw new IllegalStateException("Microsandbox source is missing; refusing to replace its state");
        }
        if (!"Stopped".equals(status(machine, vm, deadline))) {
            client().command(remaining(deadline), "stop", "--force", vm);
        }
        if (!"Stopped".equals(status(machine, vm, deadline))) {
            throw new IllegalStateException("Microsandbox source did not stop");
        }
        capacity.release(machine.id);
    }

    private void cleanupCheckpoints(MicrosandboxWorkloadMachine machine, long deadline) {
        for (String checkpoint : List.copyOf(machine.retiredCheckpoints)) {
            try {
                if (checkpointExists(machine, checkpoint, deadline)) {
                    client().command(remaining(deadline), "snap", "rm", "--force", checkpoint);
                }
                machine.retiredCheckpoints.remove(checkpoint);
                machine.checkpointIds.remove(checkpoint);
                save(machine);
            } catch (RuntimeException e) {
                LOG.warnv("Retaining microsandbox workload checkpoint for cleanup: {0}", e.getMessage());
            }
        }
    }

    private void rememberCheckpoint(MicrosandboxWorkloadMachine machine, long deadline) {
        for (JsonNode snapshot : client().json(remaining(deadline), "snap", "ls", "--format", "json")) {
            String selector = snapshot.path("group").asText() + ":" + snapshot.path("name").asText();
            if (selector.equals(machine.checkpoint)) {
                String id = snapshot.path("snapshot_id").asText();
                if (id.isBlank()) {
                    throw new IllegalStateException("Microsandbox checkpoint has no identity");
                }
                machine.checkpointIds.put(selector, id);
                return;
            }
        }
    }

    private boolean checkpointExists(MicrosandboxWorkloadMachine machine, String checkpoint, long deadline) {
        for (JsonNode snapshot : client().json(remaining(deadline), "snap", "ls", "--format", "json")) {
            if (checkpoint.equals(snapshot.path("group").asText() + ":" + snapshot.path("name").asText())) {
                if (!snapshot.path("snapshot_id").asText().equals(machine.checkpointIds.get(checkpoint))) {
                    throw new IllegalStateException("Microsandbox checkpoint ownership changed; refusing to use it");
                }
                return true;
            }
        }
        return false;
    }

    private boolean exists(String vm, long deadline) {
        if (vm == null) {
            return false;
        }
        for (JsonNode entry : client().json(remaining(deadline), "ls", "--format", "json")) {
            if (vm.equals(entry.path("name").asText())) {
                return true;
            }
        }
        return false;
    }

    private String status(MicrosandboxWorkloadMachine machine, String vm, long deadline) {
        JsonNode inspect = client().json(remaining(deadline), "inspect", "--format", "json", vm);
        String parent = machine.generationParents.get(vm);
        if (parent == null) {
            throw new IllegalStateException("Microsandbox generation has no ownership record");
        }
        requireOwnership(machine, inspect.path("config"), parent);
        if (inspect.hasNonNull("active_config")) {
            requireOwnership(machine, inspect.path("active_config"), parent);
        }
        return inspect.path("status").asText();
    }

    private void requireOwnership(MicrosandboxWorkloadMachine machine, JsonNode configuration, String parent) {
        JsonNode labels = configuration.path("labels");
        if (parent.isEmpty()) {
            if (!machine.id.equals(labels.path("floci.workload").asText())
                    || !ContainerStorageHelper.ownerIdentity(config).equals(labels.path(ContainerStorageHelper.OWNER_LABEL).asText())) {
                throw new IllegalStateException("Microsandbox guest ownership labels changed; refusing to use it");
            }
        } else if (!parent.equals(configuration.path("snapshot_parent").asText())
                || (labels.has("floci.workload") && !machine.id.equals(labels.path("floci.workload").asText()))
                || (labels.has(ContainerStorageHelper.OWNER_LABEL)
                && !ContainerStorageHelper.ownerIdentity(config).equals(labels.path(ContainerStorageHelper.OWNER_LABEL).asText()))) {
            // Full restore drops labels in 0.7.8. Its immutable snapshot lineage identifies the child.
            throw new IllegalStateException("Microsandbox guest checkpoint lineage changed; refusing to use it");
        }
    }

    private void requireRunning(MicrosandboxWorkloadMachine machine, String vm, long deadline) {
        if (!exists(vm, deadline) || !"Running".equals(status(machine, vm, deadline))) {
            throw new IllegalStateException("Microsandbox workload guest is missing or stopped; refusing a fresh process launch");
        }
    }

    private static String binding(MicrosandboxWorkloadMachine machine) {
        String host = machine.bindHost.contains(":") ? "[" + machine.bindHost + "]" : machine.bindHost;
        return host + ":" + machine.hostPort + ":" + machine.guestPort;
    }

    private void save(MicrosandboxWorkloadMachine machine) {
        journal.putForAccount(machine.account, machine.id, machine);
        journal.flush();
    }

    private static long deadline(Duration timeout) {
        return System.nanoTime() + timeout.toNanos();
    }

    private static Duration remaining(long deadline) {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) {
            throw new IllegalStateException("Microsandbox workload operation timed out; inspect state before retrying");
        }
        return Duration.ofNanos(nanos);
    }

    private static boolean ownedName(MicrosandboxWorkloadMachine machine, String name) {
        if (!name.startsWith(machine.id + "-")) {
            return false;
        }
        String generation = name.substring(machine.id.length() + 1);
        return !generation.isEmpty() && generation.chars().allMatch(Character::isDigit);
    }

    /** Destructive reset, called only after all activity and lifecycle operations have drained. */
    public void clear() {
        MicrosandboxClient current = client;
        if (current != null) {
            current.awaitQuiescence(Duration.ofSeconds(10));
        }
        for (MicrosandboxWorkloadMachine machine : List.copyOf(machines.values())) {
            synchronized (machine) {
                long deadline = deadline(Duration.ofSeconds(180));
                for (JsonNode entry : client().json(remaining(deadline), "ls", "--format", "json")) {
                    String name = entry.path("name").asText();
                    if (ownedName(machine, name)) {
                        status(machine, name, deadline);
                        client().command(remaining(deadline), "rm", "--force", name);
                    }
                }
                for (JsonNode entry : client().json(remaining(deadline), "snap", "ls", "--format", "json")) {
                    if (ownedName(machine, entry.path("group").asText())) {
                        String checkpoint = entry.path("group").asText() + ":" + entry.path("name").asText();
                        if (!entry.path("snapshot_id").asText().equals(machine.checkpointIds.get(checkpoint))) {
                            throw new IllegalStateException("Microsandbox checkpoint has no matching ownership journal");
                        }
                        client().command(remaining(deadline), "snap", "rm", "--force", checkpoint);
                    }
                }
                journal.deleteForAccount(machine.account, machine.id);
                journal.flush();
                capacity.release(machine.id);
                machines.remove(machine.id, machine);
                adapters.remove(machine.id);
            }
        }
    }
}
