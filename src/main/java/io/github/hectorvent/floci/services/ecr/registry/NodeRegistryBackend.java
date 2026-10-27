package io.github.hectorvent.floci.services.ecr.registry;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Container;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.EcsServiceConfig.ImagePullBehavior;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager.ContainerInfo;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager.ContainerPresence;
import io.github.hectorvent.floci.core.common.docker.ContainerSpec;
import io.github.hectorvent.floci.core.common.docker.ContainerStorageHelper;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode;
import io.github.hectorvent.floci.runtime.node.NodeDocker;
import io.github.hectorvent.floci.runtime.node.NodeRegistryRelay;
import io.github.hectorvent.floci.runtime.node.NodeTunnel;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.services.lambda.launcher.ImageCacheService;
import io.github.hectorvent.floci.services.lambda.launcher.ImageCacheService.LaunchImage;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** ECR's registry on the node, with a loopback relay for Floci's existing registry client. */
@ApplicationScoped
public class NodeRegistryBackend {
    private static final Logger LOG = Logger.getLogger(NodeRegistryBackend.class);
    public static final int REGISTRY_PORT = 5000;
    private static final Duration DOCKER_TIMEOUT = Duration.ofMinutes(3);
    private final EmulatorConfig config;
    private final FlociNode flociNode;
    private final NodeDocker nodeDocker;
    private final NodeTunnel tunnel;
    private final ContainerBuilder builder;
    private final ContainerDetector detector;
    private final RegionResolver region;
    private ContainerLifecycleManager lifecycle;
    private String containerId;
    private NodeRegistryRelay relay;

    @Inject
    public NodeRegistryBackend(EmulatorConfig config, FlociNode flociNode, NodeDocker nodeDocker,
                               NodeTunnel tunnel, ContainerBuilder builder, ContainerDetector detector,
                               RegionResolver region) {
        this.config = config;
        this.flociNode = flociNode;
        this.nodeDocker = nodeDocker;
        this.tunnel = tunnel;
        this.builder = builder;
        this.detector = detector;
        this.region = region;
    }

    public synchronized void ensureStarted() {
        if (relay != null && lifecycle != null && containerId != null) {
            ContainerPresence presence = lifecycle.presenceOf(containerId);
            if (presence == ContainerPresence.RUNNING || presence == ContainerPresence.UNKNOWN) {
                return;
            }
            closeRelay();
        }
        FlociNode.Status status = flociNode.acquire(FlociNode.Use.of("ECR", region.getAccountId(), region.getRegion()));
        SmolvmNode node = status.state() == FlociNode.State.READY ? status.node() : awaitNode();
        NodeDocker.Clients clients = nodeDocker.of(node);
        awaitDocker(node, clients.control());
        lifecycle = new ContainerLifecycleManager(clients.control(), new ImageCacheService(clients.control(), config),
                detector, config);
        String name = ContainerStorageHelper.dockerName(config, config.services().ecr().registryContainerName());
        String volume = volumeName();
        lifecycle.ensureVolume(volume);
        Optional<Container> existing = lifecycle.findByName(name);
        ContainerInfo info;
        if (existing.isPresent()) {
            if (!EcrRegistryManager.hasLoopbackBinding(existing.get())) {
                throw new IllegalStateException("The existing node registry " + name
                        + " does not have a loopback-only binding");
            }
            info = lifecycle.adopt(existing.get().getId(), List.of(REGISTRY_PORT));
            if (info.publishedHostPort(REGISTRY_PORT).orElse(0) != REGISTRY_PORT) {
                throw new IllegalStateException("The node registry " + name + " must publish 127.0.0.1:5000");
            }
        } else {
            awaitBundledImage(clients.control());
            ContainerSpec spec = builder.newContainerResolved(registryImage())
                    .withName(name)
                    .withEnv(List.of("REGISTRY_STORAGE_DELETE_ENABLED=true", "REGISTRY_HTTP_ADDR=0.0.0.0:5000",
                            "REGISTRY_HTTP_RELATIVEURLS=true"))
                    .withLoopbackPortBinding(REGISTRY_PORT, REGISTRY_PORT)
                    .withNamedVolume(volume, "/var/lib/registry")
                    .withLogRotation()
                    .withLabels(ContainerStorageHelper.resourceIdentityLabels(
                            "ecr", null, region.getAccountId(), region.getDefaultRegion()))
                    .build();
            info = lifecycle.createAndStart(spec);
        }
        containerId = info.containerId();
        String ip = lifecycle.resolveContainerNetworkIp(containerId, null);
        try {
            relay = new NodeRegistryRelay(tunnel, node, ip, REGISTRY_PORT);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot open the node registry relay", e);
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!httpClient().ping()) {
            if (System.nanoTime() >= deadline) {
                closeRelay();
                throw new IllegalStateException("The registry on node " + node.name() + " did not become ready");
            }
            sleep();
        }
        LOG.infov("ECR registry {0} runs on node {1}", containerId, node.name());
    }

    private SmolvmNode awaitNode() {
        try {
            return flociNode.whenReady().get(config.runtime().node().readyTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the ECR node", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("The ECR node did not become ready", e);
        }
    }

    private static void awaitDocker(SmolvmNode node, DockerClient docker) {
        long deadline = System.nanoTime() + DOCKER_TIMEOUT.toNanos();
        while (true) {
            try {
                docker.pingCmd().exec();
                return;
            } catch (RuntimeException e) {
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException("Docker on the ECR node " + node.name() + " did not answer", e);
                }
                LOG.debugv("Waiting for Docker on node {0}: {1}", node.name(), e.getMessage());
                sleep();
            }
        }
    }

    private void awaitBundledImage(DockerClient docker) {
        String image = registryImage();
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (true) {
            try {
                docker.inspectImageCmd(image).exec();
                return;
            } catch (NotFoundException e) {
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException("Registry image " + image + " is absent from the node. "
                            + "Rebuild the node image or load the image into the node's dockerd.", e);
                }
                sleep();
            }
        }
    }

    private String registryImage() {
        String image = config.services().ecr().registryImage();
        return "registry:2".equals(image) ? image : builder.resolveImage(image);
    }

    private static void sleep() {
        try {
            Thread.sleep(Duration.ofSeconds(1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the node registry", e);
        }
    }

    public RegistryHttpClient httpClient() {
        if (relay == null) {
            throw new IllegalStateException("Node registry has not started");
        }
        return new RegistryHttpClient("http://127.0.0.1:" + relay.port());
    }

    public DockerClient docker() {
        return lifecycle.getDockerClient();
    }

    public String containerId() {
        return containerId;
    }

    public record ImageMetadata(List<String> command, String manifestDigest) {}

    /** Pull by reference, then return its immutable manifest and that image's command. */
    public ImageMetadata imageMetadata(String image) {
        ensureStarted();
        LaunchImage pulled = lifecycle.resolveImageForLaunch(image, ImagePullBehavior.ALWAYS);
        String digest = pulled.manifestDigest();
        if (digest == null && image.contains("@sha256:")) {
            digest = image.substring(image.indexOf('@') + 1);
        }
        if (digest == null) {
            throw new IllegalStateException("The node registry did not return an immutable digest for " + image);
        }
        return new ImageMetadata(lifecycle.imageCommand(pulled.imageId(), null), digest);
    }

    private String volumeName() {
        return ContainerStorageHelper.dockerName(config, "ecr-registry-data");
    }

    /** Close the relay and optionally remove the container and its node-owned data volume. */
    public synchronized void close(boolean removeContainer, boolean removeVolume) {
        closeRelay();
        if (removeContainer && lifecycle != null && containerId != null) {
            lifecycle.stopAndRemove(containerId, null);
            containerId = null;
        }
        if (removeVolume && lifecycle != null) {
            ContainerStorageHelper.removeNamedVolume(config, lifecycle, volumeName());
        }
    }

    private void closeRelay() {
        if (relay != null) {
            relay.close();
            relay = null;
        }
    }
}
