package io.github.hectorvent.floci.runtime.network;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Capability;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Info;
import com.github.dockerjava.api.model.LogConfig;
import com.github.dockerjava.api.model.Mount;
import com.github.dockerjava.api.model.MountType;
import com.github.dockerjava.api.model.Network;
import com.github.dockerjava.api.model.RestartPolicy;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.FlociCertificateAuthority;
import io.github.hectorvent.floci.core.common.dns.EmbeddedDnsServer;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerExec;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.core.common.docker.ContainerSpec;
import io.github.hectorvent.floci.core.common.docker.ContainerStorageHelper;
import io.github.hectorvent.floci.core.common.docker.CurrentContainerNetworkResolver;
import io.github.hectorvent.floci.core.common.docker.LocallyBuiltHelperImage;
import io.github.hectorvent.floci.core.common.docker.RetryingTarCopier;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Installs a default-deny boundary before any managed container starts on a protected bridge. */
@ApplicationScoped
public class NetworkIsolationManager {
    private static final Logger LOG = Logger.getLogger(NetworkIsolationManager.class);
    public static final String CA_PATH = "/var/run/floci/certs/ca.crt";
    public static final String CA_ENV = "FLOCI_CA_BUNDLE";
    private static final String CA_DIRECTORY = "/var/run/floci/certs";
    private static final String OWNER_LABEL = "floci.network.owner";
    private static final String TABLE_LABEL = "floci.network.table";

    private final NetworkConfiguration configuration;
    private final EmulatorConfig config;
    private final DockerClient docker;
    private final ContainerBuilder builder;
    private final ContainerDetector detector;
    private final CurrentContainerNetworkResolver current;
    private final Instance<FlociCertificateAuthority> authority;
    private final ContainerLogStreamer logs;
    private final Instance<EmbeddedDnsServer> dns;
    private final Map<String, String> bridges = new LinkedHashMap<>();
    private final BackendAddressCache backendAddresses = new BackendAddressCache(
            Duration.ofSeconds(1), 1024, System::nanoTime);
    private String helper;
    private String table;
    private String volume;
    private String networkName;
    private Closeable logStream;
    private boolean initialized;

    @Inject
    public NetworkIsolationManager(NetworkConfiguration configuration, EmulatorConfig config,
                                    DockerClient docker, ContainerBuilder builder, ContainerDetector detector,
                                    CurrentContainerNetworkResolver current, Instance<FlociCertificateAuthority> authority,
                                    ContainerLogStreamer logs, Instance<EmbeddedDnsServer> dns) {
        this.configuration = configuration;
        this.config = config;
        this.docker = docker;
        this.builder = builder;
        this.detector = detector;
        this.current = current;
        this.authority = authority;
        this.logs = logs;
        this.dns = dns;
    }

    public boolean active() {
        return configuration.network().isolated() || !configuration.network().routes().isEmpty();
    }

    public synchronized void initialize() {
        if (!active() || initialized) {
            return;
        }
        if (!detector.isRunningInContainer()) {
            throw new IllegalStateException("Workload network routing requires Floci in Docker on a dedicated bridge network");
        }
        Info info = docker.infoCmd().exec();
        if (!"linux".equals(info.getOsType()) || info.getSecurityOptions() != null
                && info.getSecurityOptions().stream().anyMatch(option -> option.toLowerCase(Locale.ROOT).contains("rootless"))) {
            throw new IllegalStateException("Workload isolation requires a rootful Linux Docker daemon");
        }
        networkName = current.resolveNetworkName().orElseThrow(() ->
                new IllegalStateException("Cannot resolve Floci's Docker network"));
        Network network = requireBridge(networkName);
        if (configuration.network().isolated()) {
            InspectContainerResponse self = docker.inspectContainerCmd(current.resolveContainerId().orElseThrow()).exec();
            String[] resolvers = self.getHostConfig().getDns();
            if (resolvers == null || resolvers.length == 0
                    || List.of(resolvers).stream().anyMatch(server -> !"127.0.0.1".equals(server))) {
                throw new IllegalStateException("Start the Floci container with --dns 127.0.0.1 when isolation is enabled, "
                        + "so Docker cannot forward Floci's own lookups to public DNS");
            }
            for (String attached : self.getNetworkSettings().getNetworks().keySet()) {
                remember(requireBridge(attached));
            }
        }
        String owner = ContainerStorageHelper.ownerIdentity(config);
        table = "floci_iso_" + UUID.nameUUIDFromBytes(owner.getBytes(StandardCharsets.UTF_8))
                .toString().replace("-", "").substring(0, 12);
        volume = ContainerStorageHelper.resourceName(config, "network-ca", null, "public");
        String name = ContainerStorageHelper.resourceName(config, "network-policy", null, "monitor");
        String image = config.network().securityGroupEnforcement().helperImage();
        LocallyBuiltHelperImage.ensureBuilt(docker, builder, image, "floci/network-helper:local",
                "/docker/network-helper.Dockerfile");
        docker.createVolumeCmd().withName(volume).withLabels(Map.of(OWNER_LABEL, owner)).exec();
        try {
            InspectContainerResponse existing = docker.inspectContainerCmd(name).exec();
            if (!owner.equals(existing.getConfig().getLabels().get(OWNER_LABEL))) {
                throw new IllegalStateException("Network helper belongs to another Floci environment: " + name);
            }
            helper = existing.getId();
            String existingTable = existing.getConfig().getLabels().get(TABLE_LABEL);
            if (existingTable == null || !existingTable.matches("floci_iso_[a-z0-9]+")) {
                throw new IllegalStateException("Cannot identify the existing isolation table for " + name);
            }
            table = existingTable;
            if (!Boolean.TRUE.equals(existing.getState().getRunning())) {
                docker.startContainerCmd(helper).exec();
            }
        } catch (NotFoundException missing) {
            helper = docker.createContainerCmd(builder.resolveImage(image)).withName(name)
                    .withLabels(Map.of(OWNER_LABEL, owner, TABLE_LABEL, table))
                    .withHostConfig(HostConfig.newHostConfig().withNetworkMode("host")
                            .withCapAdd(Capability.NET_ADMIN).withRestartPolicy(RestartPolicy.alwaysRestart())
                            .withLogConfig(new LogConfig(LogConfig.LoggingType.JSON_FILE, Map.of(
                                    "max-size", config.docker().logMaxSize(), "max-file", config.docker().logMaxFile())))
                            .withMounts(List.of(new Mount().withType(MountType.VOLUME).withSource(volume)
                                    .withTarget(CA_DIRECTORY))))
                    .withCmd("sh", "-c", "nft monitor trace | awk '$5 == \"" + table
                            + "\" && /packet:|verdict (drop|accept)/ { print; fflush(); }'").exec().getId();
            docker.startContainerCmd(helper).exec();
        }
        RetryingTarCopier.copyBytes(docker, helper, CA_DIRECTORY, "ca.crt",
                authority.get().caPem().getBytes(StandardCharsets.US_ASCII), 0444);
        logStream = logs.attachConsoleOnly(helper, "network-isolation");
        if (configuration.network().isolated()) {
            for (Container container : docker.listContainersCmd().withShowAll(true).exec()) {
                if (owner.equals(ContainerStorageHelper.labelValue(container.getLabels(), ContainerStorageHelper.OWNER_LABEL))) {
                    InspectContainerResponse inspect = docker.inspectContainerCmd(container.getId()).exec();
                    for (String attached : inspect.getNetworkSettings().getNetworks().keySet()) {
                        remember(requireBridge(attached));
                    }
                }
            }
            protect(network);
        } else {
            removePolicy(helper, table);
        }
        initialized = true;
    }

    /** Removes this environment's network policy when isolation is explicitly disabled. */
    public synchronized void disableExistingPolicy() {
        if (!configuration.configured() || !detector.isRunningInContainer()) {
            return;
        }
        String name = ContainerStorageHelper.resourceName(config, "network-policy", null, "monitor");
        try {
            InspectContainerResponse existing = docker.inspectContainerCmd(name).exec();
            if (!ContainerStorageHelper.ownerIdentity(config).equals(existing.getConfig().getLabels().get(OWNER_LABEL))) {
                throw new IllegalStateException("Network helper belongs to another environment: " + name);
            }
            String existingTable = existing.getConfig().getLabels().get(TABLE_LABEL);
            if (existingTable == null || !existingTable.matches("floci_iso_[a-z0-9]+")) {
                throw new IllegalStateException("Cannot identify the existing isolation table for " + name);
            }
            if (!Boolean.TRUE.equals(existing.getState().getRunning())) {
                docker.startContainerCmd(existing.getId()).exec();
            }
            removePolicy(existing.getId(), existingTable);
            docker.removeContainerCmd(existing.getId()).withForce(true).exec();
        } catch (NotFoundException missing) {
            LOG.debugv("No prior network policy helper {0}", name);
        }
    }

    private void removePolicy(String container, String name) {
        ContainerExec.Result listed = ContainerExec.run(docker, container,
                new String[]{"nft", "list", "tables"}, 15);
        if (listed.exitCode() != 0) {
            throw new IllegalStateException("Cannot inspect existing network policy: " + listed.summary());
        }
        if (listed.stdout().lines().anyMatch(line -> line.equals("table inet " + name))) {
            ContainerExec.Result removed = ContainerExec.run(docker, container,
                    new String[]{"nft", "delete", "table", "inet", name}, 15);
            if (removed.exitCode() != 0) {
                throw new IllegalStateException("Cannot disable existing network policy: " + removed.summary());
            }
            LOG.infov("Network isolation disabled table={0}", name);
        }
    }

    public synchronized void prepare(ContainerSpec spec, HostConfig hostConfig) {
        if (!active()) {
            return;
        }
        initialize();
        String mode = spec.networkMode();
        if (mode == null || mode.isBlank() || Set.of("host", "bridge", "default", "none").contains(mode)) {
            throw new IllegalArgumentException("Workload network policy requires a dedicated Docker bridge, got " + mode);
        }
        if (mode.startsWith("container:")) {
            InspectContainerResponse parent = docker.inspectContainerCmd(mode.substring("container:".length())).exec();
            if (parent.getNetworkSettings().getNetworks().isEmpty()) {
                throw new IllegalArgumentException("Shared network namespace has no protected Docker bridge");
            }
            for (String name : parent.getNetworkSettings().getNetworks().keySet()) {
                protect(requireBridge(name));
            }
        } else {
            protect(requireBridge(mode));
            hostConfig.withNetworkMode(mode);
            // Overwrite image/user DNS fallbacks. Docker's embedded resolver forwards only here.
            String flociAddress = flociAddress();
            hostConfig.withDns(flociAddress);
        }
        List<Mount> mounts = new ArrayList<>(hostConfig.getMounts() == null ? List.of() : hostConfig.getMounts());
        if (mounts.stream().anyMatch(mount -> CA_DIRECTORY.equals(mount.getTarget()))) {
            throw new IllegalArgumentException("Container already mounts the reserved Floci CA directory");
        }
        mounts.add(new Mount().withType(MountType.VOLUME).withSource(volume).withTarget(CA_DIRECTORY).withReadOnly(true));
        hostConfig.withMounts(mounts);
    }

    public void created(String containerId, ContainerSpec spec) {
        if (active() && "eks".equals(ContainerStorageHelper.labelValue(spec.labels(), ContainerStorageHelper.SERVICE_LABEL))) {
            if (configuration.gatewayAddress().isEmpty()) {
                throw new IllegalStateException("Network gateway must be ready before an EKS node starts");
            }
            RetryingTarCopier.copyBytes(docker, containerId, "/etc", "floci-resolv.conf",
                    ("nameserver " + flociAddress() + "\noptions ndots:0\n").getBytes(StandardCharsets.US_ASCII), 0644);
            RetryingTarCopier.copyBytes(docker, containerId, "/var/lib/rancher/k3s",
                    "server/manifests/floci-network-trust.yaml",
                    NetworkPodAdmission.manifest(authority.get().caPem()).getBytes(StandardCharsets.UTF_8), 0644);
        }
    }

    public boolean hasPendingPodAdmission(String containerId) {
        if (!active()) {
            return false;
        }
        ContainerExec.Result result = ContainerExec.run(docker, containerId,
                new String[]{"kubectl", "get", "mutatingwebhookconfiguration", "floci-network-trust", "-o", "name"}, 10);
        return result.exitCode() != 0 || result.timedOut();
    }

    public synchronized void beforeStart(String id) {
        if (!active()) {
            return;
        }
        initialize();
        if (!Boolean.TRUE.equals(docker.inspectContainerCmd(helper).exec().getState().getRunning())) {
            throw new IllegalStateException("Network policy monitor is unavailable");
        }
        InspectContainerResponse container = docker.inspectContainerCmd(id).exec();
        String[] env = container.getConfig().getEnv();
        if (env == null || !List.of(env).contains(CA_ENV + "=" + CA_PATH)
                || container.getMounts() == null || container.getMounts().stream().noneMatch(mount ->
                        CA_DIRECTORY.equals(mount.getDestination().getPath()) && volume.equals(mount.getName())
                                && !Boolean.TRUE.equals(mount.getRW()))) {
            throw new IllegalStateException("Recreate the existing container to apply network isolation and CA delivery: " + id);
        }
        String mode = container.getHostConfig().getNetworkMode();
        if (mode != null && mode.startsWith("container:")) {
            beforeStart(mode.substring("container:".length()));
        } else {
            protect(requireBridge(mode));
            for (String name : container.getNetworkSettings().getNetworks().keySet()) {
                protect(requireBridge(name));
            }
        }
    }

    public List<String> environment(ContainerSpec spec) {
        List<String> env = new ArrayList<>(spec.env() == null ? List.of() : spec.env());
        if (active()) {
            env.removeIf(value -> value.equals(CA_ENV) || value.startsWith(CA_ENV + "="));
            env.add(CA_ENV + "=" + CA_PATH);
            if ("eks".equals(ContainerStorageHelper.labelValue(spec.labels(), ContainerStorageHelper.SERVICE_LABEL))) {
                env.removeIf(value -> value.equals("K3S_RESOLV_CONF") || value.startsWith("K3S_RESOLV_CONF="));
                env.add("K3S_RESOLV_CONF=/etc/floci-resolv.conf");
            }
        }
        return env;
    }

    public synchronized void protect(Network network) {
        if (!configuration.network().isolated()) {
            return;
        }
        boolean added = !bridges.containsKey(network.getId());
        remember(network);
        String script = NetworkIsolationRules.compile(table, bridges.values());
        RetryingTarCopier.copyBytes(docker, helper, "/tmp", "floci-isolation.nft",
                script.getBytes(StandardCharsets.UTF_8), 0600);
        ContainerExec.Result result = ContainerExec.run(docker, helper,
                new String[]{"nft", "-f", "/tmp/floci-isolation.nft"}, 15);
        if (result.exitCode() != 0 || result.timedOut()) {
            throw new IllegalStateException("Cannot enforce workload isolation: " + result.summary());
        }
        if (added || !initialized) {
            LOG.infov("Network isolation protected network={0} bridge={1}", network.getName(), bridges.get(network.getId()));
        }
    }

    private void remember(Network network) {
        String bridge = network.getOptions() == null ? null : network.getOptions().get("com.docker.network.bridge.name");
        bridges.put(network.getId(), bridge == null ? "br-" + network.getId().substring(0, 12) : bridge);
    }

    private Network requireBridge(String name) {
        Network network = docker.inspectNetworkCmd().withNetworkId(name).exec();
        if (!"bridge".equals(network.getDriver()) || "bridge".equals(network.getName())) {
            throw new IllegalArgumentException("Workload isolation requires a user-defined bridge network: " + name);
        }
        return network;
    }

    public String networkName() {
        initialize();
        return networkName;
    }

    public String flociAddress() {
        String id = current.resolveContainerId().orElseThrow();
        ContainerNetwork endpoint = docker.inspectContainerCmd(id).exec().getNetworkSettings().getNetworks().get(networkName);
        if (endpoint == null || endpoint.getIpAddress() == null || endpoint.getIpAddress().isBlank()) {
            throw new IllegalStateException("Floci has no address on its isolated network");
        }
        return endpoint.getIpAddress();
    }

    /** Resolve only Docker-owned addresses, never public DNS, even for a misspelled backend. */
    public String resolveBackend(URI backend) {
        String address = backendAddresses.resolve(backend.getHost(), () -> discoverBackend(backend));
        if (configuration.gatewayAddress().filter(address::equals).isPresent()) {
            throw new IllegalArgumentException("Backend points to the network gateway");
        }
        return address;
    }

    private synchronized String discoverBackend(URI backend) {
        Set<String> networks = new LinkedHashSet<>(bridges.keySet());
        networks.add(networkName());
        Set<String> addresses = new LinkedHashSet<>();
        for (String name : networks) {
            Network network = docker.inspectNetworkCmd().withNetworkId(name).exec();
            if (network.getContainers() == null) {
                continue;
            }
            for (String id : network.getContainers().keySet()) {
                InspectContainerResponse container;
                try {
                    container = docker.inspectContainerCmd(id).exec();
                } catch (NotFoundException missing) {
                    // Containers can disappear between network discovery and inspection.
                    LOG.debugv("Network backend discovery skipped removed container {0}", id);
                    continue;
                }
                for (Map.Entry<String, ContainerNetwork> entry : container.getNetworkSettings().getNetworks().entrySet()) {
                    if (!network.getName().equals(entry.getKey())) {
                        continue;
                    }
                    ContainerNetwork endpoint = entry.getValue();
                    addresses.add(endpoint.getIpAddress());
                    if (backend.getHost().equals(endpoint.getIpAddress())
                            || backend.getHost().equals(container.getName().replaceFirst("^/", ""))
                            || endpoint.getAliases() != null && endpoint.getAliases().contains(backend.getHost())) {
                        if (configuration.gatewayAddress().filter(endpoint.getIpAddress()::equals).isPresent()) {
                            throw new IllegalArgumentException("Backend points to the network gateway");
                        }
                        return endpoint.getIpAddress();
                    }
                }
            }
        }
        for (String address : dns.get().resolveARecord(backend.getHost(), flociAddress())) {
            if (addresses.contains(address) && configuration.gatewayAddress().filter(address::equals).isEmpty()) {
                return address;
            }
        }
        throw new IllegalArgumentException("Network backend is not on a protected Docker network: " + backend.getHost());
    }

    @PreDestroy
    void close() {
        if (logStream != null) {
            try {
                logStream.close();
            } catch (IOException e) {
                LOG.warnv(e, "Cannot close isolation log stream");
            }
        }
        // The monitor and kernel rules outlive Floci: surviving workloads must remain isolated.
    }
}
