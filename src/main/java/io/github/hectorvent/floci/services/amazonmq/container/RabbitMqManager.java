package io.github.hectorvent.floci.services.amazonmq.container;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsArnUtils;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerExec;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager.ContainerInfo;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager.ContainerPresence;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager.EndpointInfo;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.core.common.docker.ContainerSpec;
import io.github.hectorvent.floci.core.common.docker.ContainerStorageHelper;
import io.github.hectorvent.floci.core.common.docker.PortAllocator;
import io.github.hectorvent.floci.services.amazonmq.model.Broker;
import io.github.hectorvent.floci.services.amazonmq.model.BrokerInstance;
import io.github.hectorvent.floci.services.amazonmq.model.MqUser;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.Closeable;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the backing RabbitMQ Docker container for an Amazon MQ broker.
 * The AMQP (5672) and management (15672) ports are each published to a host
 * port allocated from the configured ranges, in both topologies. Unlike the
 * proxy-fronted services (RDS, ElastiCache), nothing inside Floci relays AMQP
 * traffic, so when Floci itself runs in Docker the host-port binding is the only
 * way a client on the host can reach the broker (#3240), the same reasoning as
 * {@code OpenSearchDomainManager}. Sibling containers keep reaching the broker
 * over the docker network via the endpoints reported by {@code DescribeBroker}.
 *
 * <p>Unlike {@code RedpandaManager}, RabbitMQ does not advertise a self-perceived
 * address back to clients, so the host port does not need to be passed to the
 * container as a startup argument.
 */
@ApplicationScoped
public class RabbitMqManager {

    private static final Logger LOG = Logger.getLogger(RabbitMqManager.class);
    private static final int AMQP_PORT = 5672;
    private static final int MGMT_PORT = 15672;
    private static final int COMMAND_TIMEOUT_SECONDS = 10;
    private static final long EXIT_ALREADY_EXISTS = 70;
    private static final long EXIT_NO_SUCH_VIRTUAL_HOST = 69;
    /** Configure on its own server-named queues. */
    private static final String WAKE_USER_CONFIGURE = "^amq\\.gen-.*";
    /** Write on its own server-named queues, and on the default exchange that their dead letters go through. */
    private static final String WAKE_USER_WRITE = "^(amq\\.gen-.*|amq\\.default)$";
    /** Read on its own server-named queues and on the trace and event exchanges. */
    private static final String WAKE_USER_READ = "^(amq\\.rabbitmq\\.(trace|event)|amq\\.gen-.*)$";
    private static final ObjectMapper JSON = new ObjectMapper();

    private final ContainerBuilder containerBuilder;
    private final ContainerLifecycleManager lifecycleManager;
    private final ContainerLogStreamer logStreamer;
    private final ContainerDetector containerDetector;
    private final PortAllocator portAllocator;
    private final EmulatorConfig config;
    private final RegionResolver regionResolver;
    private final Map<String, Closeable> logStreams = new ConcurrentHashMap<>();
    private final Map<String, String> containerIds = new ConcurrentHashMap<>();
    // Host ports reserved for each running broker (in-memory only, like containerIds):
    // released when the container goes away so repeated create/delete cycles do not
    // exhaust the configured ranges.
    private final Map<String, HostPorts> hostPorts = new ConcurrentHashMap<>();

    /** The pair of host ports a broker's container is published on. */
    record HostPorts(int amqp, int console) {}

    /** A virtual host of a broker, and whether the broker traces it to {@code amq.rabbitmq.trace}. */
    public record VirtualHost(String name, boolean tracing) {}

    /** A queue's messages that wait for a consumer, and those delivered and not yet acknowledged. */
    public record QueueCounts(long ready, long unacknowledged) {}

    /**
     * The counts of a virtual host's queues, by name, and the names of the queues that have none,
     * such as a crashed classic queue or a quorum queue with no leader.
     */
    public record VirtualHostQueues(Map<String, QueueCounts> counts, Set<String> down) {

        public VirtualHostQueues {
            counts = Collections.unmodifiableMap(new LinkedHashMap<>(counts));
            down = Collections.unmodifiableSet(new LinkedHashSet<>(down));
        }
    }

    @Inject
    public RabbitMqManager(ContainerBuilder containerBuilder,
                           ContainerLifecycleManager lifecycleManager,
                           ContainerLogStreamer logStreamer,
                           ContainerDetector containerDetector,
                           PortAllocator portAllocator,
                           EmulatorConfig config,
                           RegionResolver regionResolver) {
        this.containerBuilder = containerBuilder;
        this.lifecycleManager = lifecycleManager;
        this.logStreamer = logStreamer;
        this.containerDetector = containerDetector;
        this.portAllocator = portAllocator;
        this.config = config;
        this.regionResolver = regionResolver;
    }

    /** Deterministic container name for a broker, stable across emulator restarts. */
    private String containerName(String brokerId) {
        return ContainerStorageHelper.dockerName(config, "amazonmq-" + brokerId);
    }

    public void startContainer(Broker broker) {
        String image = config.services().amazonmq().defaultImage();
        String containerName = containerName(broker.getBrokerId());
        LOG.infov("Starting RabbitMQ container for broker {0} using image {1}",
                broker.getBrokerName(), image);

        // Remove any stale container with the same name (e.g. leftover from a crash).
        ContainerStorageHelper.removeStaleContainer(config, lifecycleManager, containerName);

        ContainerBuilder.Builder specBuilder = containerBuilder.newContainer(image)
                .withName(containerName)
                .withDockerNetwork(config.services().dockerNetwork())
                .withLogRotation()
                .withLabels(ContainerStorageHelper.resourceIdentityLabels(
                        "amazonmq", broker.getBrokerId(), regionResolver.getAccountId(),
                        brokerRegion(broker)));

        // Seed the broker's admin user. RabbitMQ's built-in `guest` user is
        // loopback-only, so it cannot authenticate over the mapped host port; a user
        // created via RABBITMQ_DEFAULT_USER/PASS is not loopback-restricted and can.
        if (!broker.getUsers().isEmpty()) {
            MqUser admin = broker.getUsers().get(0);
            if (admin.getPassword() == null) {
                // The password is in-memory only (a secret we do not persist). It is
                // present when CreateBroker provisions the container but null for a
                // broker reloaded from persistent storage. Fail loudly rather than
                // create a container seeded with a null credential.
                throw new IllegalStateException("Admin password unavailable for broker "
                        + broker.getBrokerId() + " (secrets are not persisted); cannot "
                        + "provision the RabbitMQ container");
            }
            specBuilder.withEnv("RABBITMQ_DEFAULT_USER", admin.getUsername());
            specBuilder.withEnv("RABBITMQ_DEFAULT_PASS", admin.getPassword());
        }

        // A re-provision of the same broker has just removed its old container, so any
        // reservation held for it is stale; hand the ports back before allocating anew.
        releaseHostPorts(broker.getBrokerId());
        HostPorts ports = allocateHostPorts();

        // Publish both ports on the host in every topology (see class javadoc). The
        // endpoints resolved below stay topology-aware: localhost:<hostPort> natively,
        // the container's network IP when Floci runs in Docker.
        specBuilder.withPortBinding(AMQP_PORT, ports.amqp())
                .withPortBinding(MGMT_PORT, ports.console());

        ContainerInfo info;
        try {
            if (ContainerStorageHelper.isNamedVolumeMode(config)) {
                ContainerStorageHelper.applyNamedVolume(specBuilder, lifecycleManager,
                        resolveVolumeName(broker), "/var/lib/rabbitmq");
            } else {
                String hostDataPath = ContainerStorageHelper.hostResourcePath(config, "amazonmq", broker.getBrokerId())
                        .toAbsolutePath().toString();
                if (!containerDetector.isRunningInContainer()) {
                    ContainerStorageHelper.ensureHostDir(hostDataPath);
                }
                specBuilder.withBind(hostDataPath, "/var/lib/rabbitmq");
            }

            ContainerSpec spec = specBuilder.build();
            info = lifecycleManager.createAndStart(spec);
        } catch (RuntimeException e) {
            // Roll back a partially-created container so a failed CreateBroker
            // does not leave an orphaned container behind, and hand the reserved
            // ports back so the failure does not leak them.
            lifecycleManager.removeIfExists(containerName);
            portAllocator.release(ports.amqp());
            portAllocator.release(ports.console());
            throw e;
        }
        broker.setContainerId(info.containerId());
        containerIds.put(broker.getBrokerId(), info.containerId());
        hostPorts.put(broker.getBrokerId(), ports);

        EndpointInfo amqp = info.getEndpoint(AMQP_PORT);
        EndpointInfo mgmt = info.getEndpoint(MGMT_PORT);
        BrokerInstance instance = new BrokerInstance(
                "http://" + mgmt.host() + ":" + mgmt.port(),
                List.of("amqp://" + amqp.host() + ":" + amqp.port()),
                amqp.host());
        broker.setBrokerInstances(new java.util.ArrayList<>(List.of(instance)));
        LOG.infov("RabbitMQ container {0} started for broker {1}: amqp={2} (host ports amqp={3}, console={4})",
                info.containerId(), broker.getBrokerName(), instance.getEndpoints().get(0),
                String.valueOf(ports.amqp()), String.valueOf(ports.console()));

        String shortId = info.containerId().length() >= 8
                ? info.containerId().substring(0, 8)
                : info.containerId();
        String logGroup = broker.generalLogGroup();
        String logStream = logStreamer.generateLogStreamName(shortId);
        String region = brokerRegion(broker);
        Closeable logHandle = logStreamer.attach(
                info.containerId(), logGroup, logStream, region, "amazonmq:" + broker.getBrokerId());
        if (logHandle != null) {
            logStreams.put(broker.getBrokerId(), logHandle);
        }
    }

    /** The region in the broker's ARN; a broker recorded without one counts as the default region's. */
    private String brokerRegion(Broker broker) {
        return AwsArnUtils.regionOrDefault(broker.getBrokerArn(), regionResolver.getDefaultRegion());
    }

    /**
     * Ready once the RabbitMQ management UI answers on its port. The management
     * plugin starts after the broker core, so a 200 here implies AMQP is also up.
     * The root path needs no auth, sidestepping RabbitMQ's loopback-only guest user.
     */
    public boolean isReady(Broker broker) {
        if (broker.getBrokerInstances().isEmpty()) {
            return false;
        }
        String consoleUrl = broker.getBrokerInstances().get(0).getConsoleURL();
        if (consoleUrl == null) {
            return false;
        }
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) URI.create(consoleUrl).toURL().openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(1000);
            conn.setReadTimeout(1000);
            return conn.getResponseCode() == 200;
        } catch (Exception e) {
            // Expected while the broker is still booting (connection refused/timeout).
            // Logged at debug so a genuinely stuck probe is diagnosable without
            // spamming this 2s-interval hot path (AGENTS.md: no empty catch).
            LOG.debugf("Readiness probe for broker %s at %s not ready: %s",
                    broker.getBrokerId(), consoleUrl, e.toString());
            return false;
        } finally {
            // Release the socket; isReady() is polled every 2s per pending broker.
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    /** Whether this Floci run started the broker's container and has not stopped it. */
    public boolean running(String brokerId) {
        return containerIds.containsKey(brokerId);
    }

    /**
     * Where the broker's container stands: absent when Floci stopped it or Docker no longer has
     * it, stopped when it exited or was stopped and is still there. One inspect.
     */
    public ContainerPresence containerPresence(String brokerId) {
        String containerId = containerIds.get(brokerId);
        return containerId == null ? ContainerPresence.ABSENT : lifecycleManager.presenceOf(containerId);
    }

    /** The broker's virtual hosts and their tracing, from {@code rabbitmqctl list_vhosts}. */
    public List<VirtualHost> virtualHosts(String brokerId) {
        ContainerExec.Result result = rabbitmqctl(brokerId, "list_vhosts", "--silent", "--formatter", "json",
                "name", "tracing");
        try {
            JsonNode list = JSON.readTree(result.stdout());
            if (list == null || !list.isArray()) {
                throw new IllegalStateException("rabbitmqctl list_vhosts printed no list: " + result.summary());
            }
            List<VirtualHost> hosts = new ArrayList<>();
            for (JsonNode host : list) {
                hosts.add(new VirtualHost(host.path("name").asText(), host.path("tracing").asBoolean()));
            }
            return hosts;
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("rabbitmqctl list_vhosts printed invalid JSON: " + result.summary(), e);
        }
    }

    /**
     * The counts of every queue of a virtual host from {@code rabbitmqctl list_queues}: the
     * queues' own counts at this moment, unlike the management API's statistics, which lag them
     * by up to two statistics intervals. Empty for a virtual host that does not exist.
     */
    public VirtualHostQueues queueCounts(String brokerId, String virtualHost) {
        ContainerExec.Result result = rabbitmqctl(brokerId, "list_queues", "-p", virtualHost, "--silent",
                "--formatter", "json", "name", "messages_ready", "messages_unacknowledged");
        return virtualHostQueues(result.stdout(), result.summary());
    }

    /** Reads what {@code rabbitmqctl list_queues --formatter json} printed; {@code summary} names it in a failure. */
    static VirtualHostQueues virtualHostQueues(String stdout, String summary) {
        try {
            JsonNode list = JSON.readTree(stdout);
            if (list == null || !list.isArray()) {
                throw new IllegalStateException("rabbitmqctl list_queues printed no list: " + summary);
            }
            Map<String, QueueCounts> counts = new LinkedHashMap<>();
            Set<String> down = new LinkedHashSet<>();
            for (JsonNode queue : list) {
                String name = queue.path("name").asText();
                JsonNode ready = queue.path("messages_ready");
                JsonNode unacknowledged = queue.path("messages_unacknowledged");
                if (ready.canConvertToLong() && unacknowledged.canConvertToLong()) {
                    counts.put(name, new QueueCounts(ready.asLong(), unacknowledged.asLong()));
                } else {
                    down.add(name);
                }
            }
            return new VirtualHostQueues(counts, down);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("rabbitmqctl list_queues printed invalid JSON: " + summary, e);
        }
    }

    /** Turns the firehose on for a virtual host: the broker copies its publishes and deliveries to {@code amq.rabbitmq.trace}. */
    public void traceOn(String brokerId, String virtualHost) {
        rabbitmqctl(brokerId, "trace_on", "-p", virtualHost);
    }

    public void traceOff(String brokerId, String virtualHost) {
        rabbitmqctl(brokerId, "trace_off", "-p", virtualHost);
    }

    /** Adds the user that wakes consumers, with no tags and this password, or sets the password of the user that is there. */
    public void addWakeUser(String brokerId, String username, String password) {
        ContainerExec.Result added = exec(brokerId, "rabbitmqctl", "add_user", username, password);
        if (added.exitCode() == EXIT_ALREADY_EXISTS) {
            rabbitmqctl(brokerId, "change_password", username, password);
        } else {
            check(added, "add_user");
        }
    }

    /**
     * Lets the user that wakes consumers declare and consume its own server-named queues in a
     * virtual host, and bind them to the trace and event exchanges. Returns false when the
     * virtual host does not exist.
     */
    public boolean grantWakeUser(String brokerId, String virtualHost, String username) {
        ContainerExec.Result result = exec(brokerId, "rabbitmqctl", "set_permissions", "-p", virtualHost, username,
                WAKE_USER_CONFIGURE, WAKE_USER_WRITE, WAKE_USER_READ);
        if (result.exitCode() == EXIT_NO_SUCH_VIRTUAL_HOST) {
            return false;
        }
        check(result, "set_permissions");
        return true;
    }

    public void deleteWakeUser(String brokerId, String username) {
        rabbitmqctl(brokerId, "delete_user", username);
    }

    /** Enables {@code rabbitmq_event_exchange}, which publishes the broker's events, such as {@code vhost.created}, to {@code amq.rabbitmq.event}. */
    public void enableEventExchange(String brokerId) {
        check(exec(brokerId, "rabbitmq-plugins", "enable", "rabbitmq_event_exchange"), "rabbitmq-plugins");
    }

    private ContainerExec.Result rabbitmqctl(String brokerId, String... args) {
        String[] command = new String[args.length + 1];
        command[0] = "rabbitmqctl";
        System.arraycopy(args, 0, command, 1, args.length);
        return check(exec(brokerId, command), args[0]);
    }

    private ContainerExec.Result exec(String brokerId, String... command) {
        String containerId = containerIds.get(brokerId);
        if (containerId == null) {
            throw new IllegalStateException("Floci runs no container for broker " + brokerId);
        }
        return ContainerExec.run(lifecycleManager.getDockerClient(), containerId, command, COMMAND_TIMEOUT_SECONDS)
                .throwIfTimedOut(containerId);
    }

    private static ContainerExec.Result check(ContainerExec.Result result, String command) {
        if (result.exitCode() != 0) {
            throw new IllegalStateException(command + " exited with " + result.exitCode() + ": " + result.summary());
        }
        return result;
    }

    public void stopContainer(Broker broker) {
        containerIds.remove(broker.getBrokerId());
        releaseHostPorts(broker.getBrokerId());
        Closeable logHandle = logStreams.remove(broker.getBrokerId());
        String containerId = broker.getContainerId();
        if (containerId != null) {
            lifecycleManager.stopAndRemove(containerId, logHandle);
            LOG.infov("RabbitMQ container {0} stopped and removed", containerId);
        } else {
            // containerId is in-memory bookkeeping and is null after an emulator
            // restart (it is intentionally not persisted; see Broker). Fall back to
            // the deterministic container name so an explicit DeleteBroker still
            // removes a container left running from a previous run.
            ContainerStorageHelper.removeStaleContainer(config, lifecycleManager, containerName(broker.getBrokerId()));
        }
    }

    /**
     * Stops and removes every running broker container. Wired into
     * {@code EmulatorLifecycle.onStop()} so containers are torn down on shutdown
     * alongside the other container managers.
     */
    public void stopAll() {
        if (!containerIds.isEmpty()) {
            LOG.infov("Stopping {0} RabbitMQ container(s) on shutdown", containerIds.size());
        }
        for (String brokerId : new ArrayList<>(containerIds.keySet())) {
            String containerId = containerIds.remove(brokerId);
            releaseHostPorts(brokerId);
            if (containerId == null) {
                continue;
            }
            Closeable logHandle = logStreams.remove(brokerId);
            lifecycleManager.stopAndRemove(containerId, logHandle);
        }
    }

    private HostPorts allocateHostPorts() {
        EmulatorConfig.AmazonMqServiceConfig mq = config.services().amazonmq();
        int amqp = portAllocator.allocate(mq.amqpHostPortBase(), mq.amqpHostPortMax());
        int console;
        try {
            console = portAllocator.allocate(mq.consoleHostPortBase(), mq.consoleHostPortMax());
        } catch (RuntimeException e) {
            portAllocator.release(amqp);
            throw e;
        }
        return new HostPorts(amqp, console);
    }

    private void releaseHostPorts(String brokerId) {
        HostPorts ports = hostPorts.remove(brokerId);
        if (ports != null) {
            portAllocator.release(ports.amqp());
            portAllocator.release(ports.console());
        }
    }

    public void removeBrokerStorage(Broker broker) {
        ContainerStorageHelper.removeNamedVolume(config, lifecycleManager, resolveVolumeName(broker));
    }

    /**
     * The broker's Docker volume name, backfilled once for records written before the field existed:
     * those predate the {@code floci-aws-} migration, so their data is in the legacy-named volume
     * and must keep resolving there. Never use the live helper here, which would strand that data
     * under a freshly created volume.
     */
    private String resolveVolumeName(Broker broker) {
        if (broker.getDockerVolumeName() == null || broker.getDockerVolumeName().isBlank()) {
            broker.setDockerVolumeName(ContainerStorageHelper.legacyResourceName(
                    config, "amazonmq", broker.getVolumeId(), broker.getBrokerId()));
        }
        return broker.getDockerVolumeName();
    }

}
