package io.github.hectorvent.floci.services.rds.container;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotFoundException;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLiveness;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.core.common.docker.OciRuntime;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode.NodeUnavailableException;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode.State;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode.Status;
import io.github.hectorvent.floci.runtime.node.NodeApiException;
import io.github.hectorvent.floci.runtime.node.NodeDocker;
import io.github.hectorvent.floci.runtime.node.NodePod;
import io.github.hectorvent.floci.runtime.node.NodeTunnel;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import io.github.hectorvent.floci.services.cloudwatch.logs.CloudWatchLogsService;
import io.github.hectorvent.floci.services.lambda.launcher.ImageCacheService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * RDS's database containers on the smolvm-pods node ({@code floci.services.rds.smolvm.*}), the
 * node that RDS shares with EKS ({@link FlociNode}): each runs through the node's dockerd in a pod
 * VM of its own, with the runtime {@code io.containerd.smolvm-pods.v2}, and sleeps when no client
 * has used it for the idle timeout. Floci reaches the dockerd through the node service's Docker
 * proxy and the database through its tunnels, so a client connection to a sleeping database wakes
 * it. The first database has Floci create the node when there is none.
 */
@ApplicationScoped
public class RdsSmolvmBackend {

    /** The runtime that puts a Docker container in a pod VM of its own. */
    public static final String RUNTIME = "io.containerd.smolvm-pods.v2";
    /** The annotation that lets a Docker container on the node sleep. */
    public static final String HIBERNATE_ANNOTATION = "smolvm-pods.floci.io/hibernate";
    /** The annotation of a pod's idle timeout. */
    public static final String IDLE_TIMEOUT_ANNOTATION = "smolvm-pods.floci.io/idle-timeout";

    /**
     * How long a tunnel waits for the node to connect to the database: a wake takes well under a
     * second, but waits up to the node's admission wait (10 s by default) when a cap is full.
     */
    static final Duration TUNNEL_ANSWER_TIMEOUT = Duration.ofSeconds(40);

    /** How long a database waits for the dockerd of a node that is ready. */
    static final Duration DOCKER_READY_TIMEOUT = Duration.ofMinutes(3);
    private static final Duration DOCKER_POLL = Duration.ofSeconds(1);

    private static final Logger LOG = Logger.getLogger(RdsSmolvmBackend.class);
    private static final Pattern DURATION = Pattern.compile("(\\d+(\\.\\d+)?(ns|us|µs|ms|s|m|h))+");

    private final EmulatorConfig config;
    private final FlociNode flociNode;
    private final SmolvmNodeClient nodeClient;
    private final NodeDocker nodeDocker;
    private final NodeTunnel tunnel;
    private final DockerClient localDocker;
    private final ImageCacheService localImages;
    private final ContainerDetector containerDetector;
    private final CloudWatchLogsService cloudWatchLogs;
    private final Map<String, Target> targets = new ConcurrentHashMap<>();
    private final Map<String, Object> imageLocks = new ConcurrentHashMap<>();

    /**
     * The node that RDS runs its databases on, and what Floci drives its dockerd with: a lifecycle
     * manager that creates every container with {@link #RUNTIME}, and a log streamer.
     */
    public record Target(SmolvmNode node, NodeDocker.Clients clients, ContainerLifecycleManager lifecycle,
                         ContainerLogStreamer logs) {
    }

    /** No node can run RDS's databases; the message says why and what to set. */
    public static final class NoNodeException extends IllegalStateException {
        NoNodeException(String message) {
            super(message);
        }
    }

    @Inject
    public RdsSmolvmBackend(EmulatorConfig config, FlociNode flociNode, SmolvmNodeClient nodeClient,
                            NodeDocker nodeDocker, NodeTunnel tunnel, DockerClient localDocker,
                            ImageCacheService localImages, ContainerDetector containerDetector,
                            CloudWatchLogsService cloudWatchLogs) {
        this.config = config;
        this.flociNode = flociNode;
        this.nodeClient = nodeClient;
        this.nodeDocker = nodeDocker;
        this.tunnel = tunnel;
        this.localDocker = localDocker;
        this.localImages = localImages;
        this.containerDetector = containerDetector;
        this.cloudWatchLogs = cloudWatchLogs;
    }

    /** Whether RDS runs its databases on a smolvm-pods node. */
    public boolean enabled() {
        return config.services().rds().smolvm().enabled();
    }

    /**
     * The node that runs the databases now, once it is ready.
     *
     * @throws NoNodeException when there is none, or it is not ready
     */
    public SmolvmNode node() {
        runtime();
        Status status = flociNode.status();
        return switch (status.state()) {
            case READY -> status.node();
            case CREATING -> throw new NoNodeException("The smolvm-pods node that RDS runs its databases on is "
                    + "still being created; try again once it is ready");
            case FAILED -> throw new NoNodeException("The smolvm-pods node that RDS runs its databases on failed: "
                    + status.failure());
            case NONE -> throw new NoNodeException("Floci has no smolvm-pods node for RDS's databases yet");
        };
    }

    /**
     * The node for a new database: the runtime file's, or the one Floci created, or one Floci
     * starts to create now for {@code user}.
     *
     * @return whether the node and its dockerd are ready; a database waits for
     *         {@link #whenReady()} when they are not
     * @throws NoNodeException when the runtime file names no usable node, or Floci cannot create one
     */
    public boolean place(String user, String accountId, String region) {
        runtime();
        Status status;
        try {
            status = flociNode.acquire(FlociNode.Use.of(user, accountId, region));
        } catch (NodeUnavailableException e) {
            throw new NoNodeException(e.getMessage());
        }
        return status.state() == State.READY && dockerAnswers(status.node());
    }

    /**
     * Completes once the node is ready and its dockerd answers, which on a node that Floci just
     * created comes a few seconds after Kubernetes; exceptionally when either does not.
     */
    public CompletableFuture<SmolvmNode> whenReady() {
        return flociNode.whenReady().thenApplyAsync(node -> {
            awaitDocker(node);
            return node;
        }, task -> Thread.ofVirtual().name("rds-smolvm-docker-ready").start(task));
    }

    private void awaitDocker(SmolvmNode node) {
        long deadline = System.nanoTime() + DOCKER_READY_TIMEOUT.toNanos();
        while (!dockerAnswers(node)) {
            if (System.nanoTime() > deadline) {
                throw new NoNodeException("The dockerd of smolvm-pods node " + node.name() + " did not answer within "
                        + DOCKER_READY_TIMEOUT.toSeconds() + " s");
            }
            try {
                Thread.sleep(DOCKER_POLL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new NoNodeException("Floci stopped waiting for the dockerd of smolvm-pods node " + node.name());
            }
        }
    }

    /** Whether the node's dockerd answers a ping through the node service's Docker proxy. */
    private boolean dockerAnswers(SmolvmNode node) {
        try {
            nodeDocker.of(node).control().pingCmd().exec();
            return true;
        } catch (RuntimeException e) {
            LOG.debugv("The dockerd of smolvm-pods node {0} does not answer yet: {1}", node.name(), e.getMessage());
            return false;
        }
    }

    /** The node's dockerd, as {@link #node()} chooses the node. */
    public Target target() {
        SmolvmNode node = node();
        NodeDocker.Clients clients = nodeDocker.of(node);
        return targets.compute(node.name(), (name, existing) -> {
            if (existing != null && existing.clients() == clients) {
                return existing;
            }
            LOG.infov("RDS runs its databases on smolvm node {0} at {1}", node.name(), node.baseUrl());
            ContainerLifecycleManager lifecycle = new ContainerLifecycleManager(clients.control(),
                    new ImageCacheService(clients.control(), config), containerDetector, config, runtime());
            return new Target(node, clients, lifecycle,
                    ContainerLogStreamer.reattaching(clients.streaming(), cloudWatchLogs));
        });
    }

    /** The runtime and annotations of every database container on the node. */
    OciRuntime runtime() {
        Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put(HIBERNATE_ANNOTATION, "true");
        Optional<String> idleTimeout = config.services().rds().smolvm().idleTimeout()
                .map(String::strip).filter(value -> !value.isEmpty());
        if (idleTimeout.isPresent()) {
            if (!DURATION.matcher(idleTimeout.get()).matches()) {
                throw new NoNodeException("floci.services.rds.smolvm.idle-timeout (FLOCI_SERVICES_RDS_SMOLVM_IDLE_TIMEOUT) "
                        + "must be a duration from 10s to 24h, such as 2m, not " + idleTimeout.get());
            }
            annotations.put(IDLE_TIMEOUT_ANNOTATION, idleTimeout.get());
        }
        return new OciRuntime(RUNTIME, annotations);
    }

    /**
     * Puts {@code image} in the node's dockerd when it is not there: Floci's own Docker pulls it,
     * as for any container, and saves it, and the node's dockerd loads it, so the node needs no
     * registry. When Floci's Docker cannot give it, the node's dockerd pulls it itself as the
     * container is created.
     */
    public void ensureImage(Target target, String image) {
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
                localImages.ensureImageExists(image);
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
     * Whether the container runs, from the node's record of its pod VM: a sleeping database is
     * running, and asking about it does not wake it. Docker answers for a container with no pod,
     * such as one stopped or still being created.
     */
    public ContainerLiveness probe(Target target, String containerId) {
        try {
            NodePod pod = nodeClient.getPod(target.node(), containerId);
            return switch (pod.state()) {
                case AWAKE, PAUSING, ASLEEP, WAKING -> ContainerLiveness.RUNNING;
                case FAILED -> ContainerLiveness.NOT_RUNNING;
                case CREATING, DELETING -> target.lifecycle().probeContainer(containerId);
            };
        } catch (NodeApiException e) {
            if (e.code() == NodeApiException.Code.NOT_FOUND) {
                return target.lifecycle().probeContainer(containerId);
            }
            LOG.warnv("The pod of RDS container {0} on smolvm node {1} cannot be read; state unknown: {2}",
                    containerId, target.node().name(), e.getMessage());
            return ContainerLiveness.UNKNOWN;
        }
    }

    /** A connection to {@code host:port} in the node, through the node service's tunnel. */
    public Socket connect(String host, int port, int connectTimeoutMillis) throws IOException {
        SmolvmNode node;
        try {
            node = node();
        } catch (NoNodeException e) {
            throw new IOException(e.getMessage(), e);
        }
        return tunnel.open(node, host, port, Duration.ofMillis(connectTimeoutMillis), TUNNEL_ANSWER_TIMEOUT);
    }
}
