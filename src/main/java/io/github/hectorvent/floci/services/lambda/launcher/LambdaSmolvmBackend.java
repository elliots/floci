package io.github.hectorvent.floci.services.lambda.launcher;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Container;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.dns.EmbeddedDnsServer;
import io.github.hectorvent.floci.core.common.dns.NodeDnsServer;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.core.common.docker.ContainerReachableEndpoint;
import io.github.hectorvent.floci.core.common.docker.ContainerStorageHelper;
import io.github.hectorvent.floci.core.common.docker.OciRuntime;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode.NodeUnavailableException;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode.State;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode.Status;
import io.github.hectorvent.floci.runtime.node.NodeApiException;
import io.github.hectorvent.floci.runtime.node.NodeDocker;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient.WakeReason;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient.WakeRequest;
import io.github.hectorvent.floci.services.cloudwatch.logs.CloudWatchLogsService;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/**
 * Lambda's environments on the smolvm-pods node ({@code floci.services.lambda.smolvm.*}), the node
 * that Lambda shares with RDS and EKS ({@link FlociNode}): each runs through the node's dockerd in
 * a pod VM of its own, with the runtime {@code io.containerd.smolvm-pods.v2}, on dockerd's default
 * bridge, where it gets the network of the node's pods: DNS through Floci, Floci's ports and the
 * Runtime API at the node address, routed simulators and Floci's CA. An environment sleeps between
 * invocations; {@link #hold} wakes it and keeps it awake for one. The first cold start has Floci
 * create the node when there is none.
 */
@ApplicationScoped
public class LambdaSmolvmBackend {

    static final String RUNTIME = "io.containerd.smolvm-pods.v2";
    static final String HIBERNATE_ANNOTATION = "smolvm-pods.floci.io/hibernate";
    static final String IDLE_TIMEOUT_ANNOTATION = "smolvm-pods.floci.io/idle-timeout";
    /**
     * Keeps the Runtime API connection over a pause. The runtime waits on
     * {@code GET /runtime/invocation/next} while the environment sleeps, as on AWS, and a reset at the
     * pause would fail that request at the wake, which the runtime takes as fatal.
     */
    static final String KEEP_TCP_ON_PAUSE_ANNOTATION = "smolvm-pods.floci.io/keep-tcp-on-pause";
    static final String RESET_ON_PAUSE_ANNOTATION = "smolvm-pods.floci.io/reset-on-pause";

    /** A lease's time to live; it is renewed every {@link #LEASE_RENEW} while an invocation runs. */
    static final int LEASE_TTL_SECONDS = 30;
    static final Duration LEASE_RENEW = Duration.ofSeconds(10);
    /** A wake takes well under a second, but waits up to the node's admission wait when a cap is full. */
    static final Duration WAKE_TIMEOUT = Duration.ofSeconds(40);
    static final Duration DOCKER_READY_TIMEOUT = Duration.ofMinutes(3);
    private static final Duration DOCKER_POLL = Duration.ofSeconds(1);

    private static final Logger LOG = Logger.getLogger(LambdaSmolvmBackend.class);
    private static final Pattern DURATION = Pattern.compile("(\\d+(\\.\\d+)?(ns|us|µs|ms|s|m|h))+");

    private final EmulatorConfig config;
    private final FlociNode flociNode;
    private final SmolvmNodeClient nodeClient;
    private final NodeDocker nodeDocker;
    private final DockerClient localDocker;
    private final ImageCacheService localImages;
    private final ContainerDetector containerDetector;
    private final CloudWatchLogsService cloudWatchLogs;
    private final ContainerReachableEndpoint reachableEndpoint;
    private final Map<String, Target> targets = new ConcurrentHashMap<>();
    private final Map<String, Object> imageLocks = new ConcurrentHashMap<>();
    private final ScheduledExecutorService renewer = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("lambda-smolvm-lease").factory());

    /**
     * The node that runs the environments, and what Floci drives its dockerd with: the clients, a
     * lifecycle manager that creates every container with {@link #RUNTIME}, and a log streamer.
     */
    public record Target(SmolvmNode node, NodeDocker.Clients clients, ContainerLifecycleManager lifecycle,
                         ContainerLogStreamer logs) {
    }

    @Inject
    public LambdaSmolvmBackend(EmulatorConfig config, FlociNode flociNode, SmolvmNodeClient nodeClient,
                               NodeDocker nodeDocker, DockerClient localDocker, ImageCacheService localImages,
                               ContainerDetector containerDetector, CloudWatchLogsService cloudWatchLogs,
                               ContainerReachableEndpoint reachableEndpoint) {
        this.config = config;
        this.flociNode = flociNode;
        this.nodeClient = nodeClient;
        this.nodeDocker = nodeDocker;
        this.localDocker = localDocker;
        this.localImages = localImages;
        this.containerDetector = containerDetector;
        this.cloudWatchLogs = cloudWatchLogs;
        this.reachableEndpoint = reachableEndpoint;
    }

    @PreDestroy
    void close() {
        renewer.shutdownNow();
    }

    /** Whether Lambda runs its environments on a smolvm-pods node. */
    public boolean enabled() {
        return config.services().lambda().smolvm().enabled();
    }

    /**
     * The node's dockerd for a cold start in {@code accountId} and {@code region}: the runtime
     * file's node, or the one Floci created, or one Floci creates now and waits for.
     *
     * @throws IllegalStateException when there is no node and Floci cannot create one, or it does
     *         not become ready
     */
    public Target target(String accountId, String region) {
        OciRuntime runtime = runtime();
        Status status;
        try {
            status = flociNode.acquire(FlociNode.Use.of("Lambda", accountId, region));
        } catch (NodeUnavailableException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        SmolvmNode node = status.state() == State.READY ? status.node() : awaitNode();
        awaitDocker(node);
        NodeDocker.Clients clients = nodeDocker.of(node);
        return targets.compute(node.name(), (name, existing) -> {
            if (existing != null && existing.clients() == clients) {
                return existing;
            }
            LOG.infov("Lambda runs its environments on smolvm node {0} at {1}", node.name(), node.baseUrl());
            ContainerLifecycleManager lifecycle = new ContainerLifecycleManager(clients.control(),
                    new ImageCacheService(clients.control(), config), containerDetector, config, runtime);
            Target target = new Target(node, clients, lifecycle,
                    ContainerLogStreamer.reattaching(clients.streaming(), cloudWatchLogs));
            removeLeftovers(target);
            return target;
        });
    }

    private SmolvmNode awaitNode() {
        int seconds = config.runtime().node().readyTimeoutSeconds();
        try {
            return flociNode.whenReady().get(seconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the smolvm-pods node", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("The smolvm-pods node that Lambda runs on is not available: "
                    + e.getCause().getMessage(), e.getCause());
        } catch (TimeoutException e) {
            throw new IllegalStateException("The smolvm-pods node that Lambda runs on was not ready within "
                    + seconds + " s", e);
        }
    }

    private void awaitDocker(SmolvmNode node) {
        long deadline = System.nanoTime() + DOCKER_READY_TIMEOUT.toNanos();
        while (true) {
            try {
                nodeDocker.of(node).control().pingCmd().exec();
                return;
            } catch (RuntimeException e) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("The dockerd of smolvm-pods node " + node.name()
                            + " did not answer within " + DOCKER_READY_TIMEOUT.toSeconds() + " s: " + e.getMessage(), e);
                }
                LOG.debugv("The dockerd of smolvm-pods node {0} does not answer yet: {1}", node.name(), e.getMessage());
            }
            try {
                Thread.sleep(DOCKER_POLL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for the dockerd of smolvm-pods node "
                        + node.name(), e);
            }
        }
    }

    /**
     * Removes the environments that an earlier run of this Floci left on the node: their runtimes
     * poll Runtime API ports that went with it.
     */
    private void removeLeftovers(Target target) {
        try {
            List<Container> leftovers = target.clients().control().listContainersCmd().withShowAll(true)
                    .withLabelFilter(ownerLabels()).exec();
            for (Container container : leftovers) {
                LOG.infov("Removing Lambda container {0} that an earlier Floci left on smolvm node {1}",
                        container.getId(), target.node().name());
                target.lifecycle().stopAndRemove(container.getId(), null);
            }
        } catch (RuntimeException e) {
            LOG.warnv("Could not remove the Lambda containers that an earlier Floci left on smolvm node {0}: {1}",
                    target.node().name(), e.getMessage());
        }
    }

    /** The labels that mark a Lambda environment of this Floci on the node. */
    Map<String, String> ownerLabels() {
        return Map.of(ContainerStorageHelper.SERVICE_LABEL, "lambda",
                ContainerStorageHelper.OWNER_LABEL, ContainerStorageHelper.ownerIdentity(config));
    }

    /** The runtime and annotations of every Lambda container on the node. */
    OciRuntime runtime() {
        Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put(HIBERNATE_ANNOTATION, "true");
        // Older nodes preserve all connections; new nodes honor the narrower keep list.
        annotations.put(RESET_ON_PAUSE_ANNOTATION, "false");
        annotations.put(KEEP_TCP_ON_PAUSE_ANNOTATION, runtimeApiHost() + ":"
                + config.services().lambda().runtimeApiBasePort() + "-" + config.services().lambda().runtimeApiMaxPort());
        Optional<String> idleTimeout = config.services().lambda().smolvm().idleTimeout()
                .map(String::strip).filter(value -> !value.isEmpty());
        if (idleTimeout.isPresent()) {
            if (!DURATION.matcher(idleTimeout.get()).matches()) {
                throw new IllegalStateException("floci.services.lambda.smolvm.idle-timeout "
                        + "(FLOCI_SERVICES_LAMBDA_SMOLVM_IDLE_TIMEOUT) must be a duration from 10s to 24h, such as 1m, not "
                        + idleTimeout.get());
            }
            annotations.put(IDLE_TIMEOUT_ANNOTATION, idleTimeout.get());
        }
        return new OciRuntime(RUNTIME, annotations);
    }

    /** Where an environment reaches its Runtime API server: the node address, which leads to Floci. */
    public String runtimeApiHost() {
        return NodeDnsServer.NODE_ADDRESS;
    }

    /** Floci's endpoint for the SDKs in an environment, by a name that Floci's node DNS answers. */
    public String flociEndpoint() {
        return "http://" + config.hostname().orElse(EmbeddedDnsServer.DEFAULT_SUFFIX) + ":" + reachableEndpoint.port();
    }

    /**
     * Puts {@code image} in the node's dockerd when it is not there: Floci's own Docker pulls it and
     * saves it, and the node's dockerd loads it, so the node needs no registry. When Floci's Docker
     * cannot give it, the node's dockerd pulls it itself as the container is created.
     *
     * @param platform such as {@code linux/arm64}, or null for the daemon's own
     */
    public void ensureImage(Target target, String image, String platform) {
        if (config.services().ecr().smolvmEnabled() && image.startsWith("localhost:5000/")) {
            return; // The node's dockerd pulls from its local registry during container creation.
        }
        DockerClient node = target.clients().control();
        if (hasImage(node, image)) {
            return;
        }
        Object lock = imageLocks.computeIfAbsent(target.node().name() + "|" + image, key -> new Object());
        synchronized (lock) {
            if (hasImage(node, image)) {
                return;
            }
            long started = System.nanoTime();
            try {
                localImages.ensureImageExists(image, platform);
                try (InputStream archive = localDocker.saveImageCmd(image).exec()) {
                    node.loadImageCmd(archive).exec();
                }
                LOG.infov("Loaded image {0} from Floci''s Docker into the dockerd of smolvm node {1} in {2} ms",
                        image, target.node().name(), (System.nanoTime() - started) / 1_000_000);
            } catch (IOException | RuntimeException e) {
                LOG.warnv("Could not load image {0} from Floci''s Docker into smolvm node {1} ({2}); the node''s "
                        + "dockerd pulls it instead", image, target.node().name(), e.getMessage());
            }
        }
    }

    private static boolean hasImage(DockerClient docker, String image) {
        try {
            docker.inspectImageCmd(image).exec();
            return true;
        } catch (NotFoundException e) {
            return false;
        }
    }

    /**
     * Wakes the environment in {@code containerId} and keeps it awake with a lease, renewed while
     * the invocation runs, until the hold closes.
     */
    public LambdaRuntimeLauncher.Hold hold(Target target, String containerId, String functionName) {
        SmolvmNode node = target.node();
        String lease = "floci-lambda-" + UUID.randomUUID();
        String holder = "floci lambda " + functionName;
        nodeClient.putLease(node, containerId, lease, LEASE_TTL_SECONDS, holder, null);
        try {
            nodeClient.wake(node, containerId, new WakeRequest(WakeReason.api(), "lambda " + functionName, null),
                    WAKE_TIMEOUT);
        } catch (RuntimeException e) {
            release(node, containerId, lease);
            throw e;
        }
        ScheduledFuture<?> renewal = renewer.scheduleAtFixedRate(() -> renew(node, containerId, lease, holder),
                LEASE_RENEW.toMillis(), LEASE_RENEW.toMillis(), TimeUnit.MILLISECONDS);
        return () -> {
            renewal.cancel(false);
            release(node, containerId, lease);
        };
    }

    void renew(SmolvmNode node, String containerId, String lease, String holder) {
        try {
            nodeClient.patchLease(node, containerId, lease, LEASE_TTL_SECONDS, null);
        } catch (NodeApiException e) {
            if (e.code() != NodeApiException.Code.LEASE_NOT_FOUND) {
                LOG.warnv("Could not renew the lease of Lambda container {0} on smolvm node {1}: {2}",
                        containerId, node.name(), e.getMessage());
                return;
            }
            LOG.warnv("The lease of Lambda container {0} on smolvm node {1} expired during an invocation; "
                    + "holding it again", containerId, node.name());
            try {
                nodeClient.putLease(node, containerId, lease, LEASE_TTL_SECONDS, holder, null);
                nodeClient.wake(node, containerId, new WakeRequest(WakeReason.api(), holder, null), WAKE_TIMEOUT);
            } catch (RuntimeException again) {
                LOG.warnv("Could not hold Lambda container {0} on smolvm node {1} again: {2}",
                        containerId, node.name(), again.getMessage());
            }
        } catch (RuntimeException e) {
            LOG.warnv("Could not renew the lease of Lambda container {0} on smolvm node {1}: {2}",
                    containerId, node.name(), e.getMessage());
        }
    }

    private void release(SmolvmNode node, String containerId, String lease) {
        try {
            nodeClient.deleteLease(node, containerId, lease);
        } catch (RuntimeException e) {
            LOG.debugv("Could not release the lease of Lambda container {0} on smolvm node {1}; it expires: {2}",
                    containerId, node.name(), e.getMessage());
        }
    }
}
