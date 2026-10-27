package io.github.hectorvent.floci.services.elasticache.container;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotFoundException;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.core.common.docker.OciRuntime;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode;
import io.github.hectorvent.floci.runtime.node.NodeDocker;
import io.github.hectorvent.floci.runtime.node.NodeTunnel;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/** Redis and Valkey containers on the shared node, reached through its wake-on-connect tunnel. */
@ApplicationScoped
public class ElastiCacheSmolvmBackend {

    private static final Logger LOG = Logger.getLogger(ElastiCacheSmolvmBackend.class);
    private static final Duration DOCKER_READY_TIMEOUT = Duration.ofMinutes(3);
    private static final Pattern DURATION = Pattern.compile("(\\d+(\\.\\d+)?(ns|us|µs|ms|s|m|h))+");

    private final EmulatorConfig config;
    private final FlociNode flociNode;
    private final NodeDocker nodeDocker;
    private final NodeTunnel tunnel;
    private final DockerClient localDocker;
    private final ImageCacheService localImages;
    private final ContainerDetector detector;
    private final CloudWatchLogsService cloudWatchLogs;
    private final Map<String, Target> targets = new ConcurrentHashMap<>();
    private final Map<String, Object> imageLocks = new ConcurrentHashMap<>();

    public record Target(SmolvmNode node, NodeDocker.Clients clients, ContainerLifecycleManager lifecycle,
                         ContainerLogStreamer logs) {}

    @Inject
    public ElastiCacheSmolvmBackend(EmulatorConfig config, FlociNode flociNode, NodeDocker nodeDocker,
                                   NodeTunnel tunnel, DockerClient localDocker, ImageCacheService localImages,
                                   ContainerDetector detector, CloudWatchLogsService cloudWatchLogs) {
        this.config = config;
        this.flociNode = flociNode;
        this.nodeDocker = nodeDocker;
        this.tunnel = tunnel;
        this.localDocker = localDocker;
        this.localImages = localImages;
        this.detector = detector;
        this.cloudWatchLogs = cloudWatchLogs;
    }

    public boolean enabled() {
        return config.services().elasticache().smolvm().enabled();
    }

    /** Acquires the node for a cache create or restore, waiting for its dockerd to answer. */
    public Target target(String accountId, String region) {
        OciRuntime runtime = runtime();
        FlociNode.Status status = flociNode.acquire(FlociNode.Use.of("ElastiCache", accountId, region));
        SmolvmNode node = status.state() == FlociNode.State.READY ? status.node() : awaitNode();
        awaitDocker(node);
        return target(node, runtime);
    }

    /** Uses the existing node for cleanup without creating a new one. */
    public Target target() {
        return target(node(), runtime());
    }

    private Target target(SmolvmNode node, OciRuntime runtime) {
        NodeDocker.Clients clients = nodeDocker.of(node);
        return targets.compute(node.name(), (name, previous) -> {
            if (previous != null && previous.clients() == clients) {
                return previous;
            }
            ContainerLifecycleManager lifecycle = new ContainerLifecycleManager(clients.control(),
                    new ImageCacheService(clients.control(), config), detector, config, runtime);
            return new Target(node, clients, lifecycle,
                    ContainerLogStreamer.reattaching(clients.streaming(), cloudWatchLogs));
        });
    }

    private SmolvmNode node() {
        FlociNode.Status status = flociNode.status();
        if (status.state() != FlociNode.State.READY) {
            throw new IllegalStateException("The ElastiCache smolvm-pods node is not ready: " + status.state());
        }
        return status.node();
    }

    private SmolvmNode awaitNode() {
        try {
            return flociNode.whenReady().get(config.runtime().node().readyTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the ElastiCache smolvm-pods node", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("The ElastiCache smolvm-pods node did not become ready", e);
        }
    }

    private void awaitDocker(SmolvmNode node) {
        long deadline = System.nanoTime() + DOCKER_READY_TIMEOUT.toNanos();
        while (true) {
            try {
                nodeDocker.of(node).control().pingCmd().exec();
                return;
            } catch (RuntimeException e) {
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException("ElastiCache node dockerd did not become ready", e);
                }
                LOG.debugv("Waiting for dockerd on node {0}: {1}", node.name(), e.getMessage());
            }
            try {
                Thread.sleep(Duration.ofSeconds(1));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for ElastiCache node dockerd", e);
            }
        }
    }

    OciRuntime runtime() {
        Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put("smolvm-pods.floci.io/hibernate", "true");
        config.services().elasticache().smolvm().idleTimeout().map(String::strip)
                .filter(value -> !value.isEmpty()).ifPresent(value -> {
                    if (!DURATION.matcher(value).matches()) {
                        throw new IllegalArgumentException("FLOCI_SERVICES_ELASTICACHE_SMOLVM_IDLE_TIMEOUT "
                                + "must be a duration from 10s to 24h, such as 2m");
                    }
                    annotations.put("smolvm-pods.floci.io/idle-timeout", value);
                });
        return new OciRuntime("io.containerd.smolvm-pods.v2", annotations);
    }

    /** Host Docker supplies the archive; an isolated node cannot pull external images itself. */
    public void ensureImage(Target target, String image) {
        if (config.services().ecr().smolvmEnabled() && image.startsWith("localhost:5000/")) {
            return; // The node's dockerd pulls from its local registry during container creation.
        }
        DockerClient docker = target.clients().control();
        Object lock = imageLocks.computeIfAbsent(target.node().name() + "|" + image, key -> new Object());
        synchronized (lock) {
            try {
                docker.inspectImageCmd(image).exec();
                return;
            } catch (NotFoundException expected) {
                // The host supplies the missing image below.
            }
            localImages.ensureImageExists(image);
            try (InputStream archive = localDocker.saveImageCmd(image).exec()) {
                docker.loadImageCmd(archive).exec();
            } catch (IOException e) {
                throw new IllegalStateException("Could not transfer ElastiCache image " + image + " to the node", e);
            }
        }
    }

    public Socket connect(String host, int port, int timeoutMillis) throws IOException {
        try {
            return tunnel.open(node(), host, port, Duration.ofMillis(timeoutMillis), Duration.ofSeconds(40));
        } catch (IllegalStateException e) {
            throw new IOException(e.getMessage(), e);
        }
    }
}
