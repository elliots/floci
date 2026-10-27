package io.github.hectorvent.floci.services.elasticache.container;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager.ContainerInfo;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager.EndpointInfo;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.core.common.docker.ContainerSpec;
import io.github.hectorvent.floci.core.common.docker.ContainerStorageHelper;
import io.github.hectorvent.floci.runtime.node.NodeTcpRelay;
import io.github.hectorvent.floci.services.elasticache.ElastiCachePortReservations;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages backend Docker container lifecycle for ElastiCache Memcached clusters.
 * In native (dev) mode, binds container port 11211 to a random host port.
 * In Docker mode, uses the container's internal network IP directly.
 */
@ApplicationScoped
public class ElastiCacheMemcachedContainerManager {

    private static final Logger LOG = Logger.getLogger(ElastiCacheMemcachedContainerManager.class);
    private static final int BACKEND_PORT = 11211;

    private static final int BACKEND_READY_DEADLINE_MS = 60_000;
    private static final int BACKEND_READY_RETRY_MS = 100;
    private static final int BACKEND_PROBE_CONNECT_MS = 2_000;
    private static final byte[] TEXT_VERSION = "version\r\n".getBytes(StandardCharsets.UTF_8);

    private final ContainerBuilder containerBuilder;
    private final ContainerLifecycleManager lifecycleManager;
    private final ContainerLogStreamer logStreamer;
    private final ContainerDetector containerDetector;
    private final EmulatorConfig config;
    private final RegionResolver regionResolver;
    private final ElastiCacheSmolvmBackend smolvm;
    private final ElastiCachePortReservations ports;
    private final Map<String, ElastiCacheContainerHandle> activeContainers = new ConcurrentHashMap<>();
    private final Map<String, NodeContainer> nodeContainers = new ConcurrentHashMap<>();
    private volatile boolean dockerUnavailableLogged;

    private record NodeContainer(ContainerLifecycleManager lifecycle, NodeTcpRelay relay) {}

    @Inject
    public ElastiCacheMemcachedContainerManager(ContainerBuilder containerBuilder,
                                                ContainerLifecycleManager lifecycleManager,
                                                ContainerLogStreamer logStreamer,
                                                ContainerDetector containerDetector,
                                                EmulatorConfig config,
                                                RegionResolver regionResolver,
                                                ElastiCacheSmolvmBackend smolvm,
                                                ElastiCachePortReservations ports) {
        this.containerBuilder = containerBuilder;
        this.lifecycleManager = lifecycleManager;
        this.logStreamer = logStreamer;
        this.containerDetector = containerDetector;
        this.config = config;
        this.regionResolver = regionResolver;
        this.smolvm = smolvm;
        this.ports = ports;
    }

    ElastiCacheMemcachedContainerManager(ContainerBuilder containerBuilder,
                                        ContainerLifecycleManager lifecycleManager,
                                        ContainerLogStreamer logStreamer,
                                        ContainerDetector containerDetector,
                                        EmulatorConfig config,
                                        RegionResolver regionResolver) {
        this(containerBuilder, lifecycleManager, logStreamer, containerDetector, config, regionResolver,
                null, new ElastiCachePortReservations());
    }

    public boolean onSmolvmNode() {
        return smolvm != null && smolvm.enabled();
    }

    private ContainerLifecycleManager lifecycle() {
        return onSmolvmNode() ? smolvm.target().lifecycle() : lifecycleManager;
    }

    /**
     * Attempts {@link #start} and reports the backend as unavailable instead of propagating the
     * failure, when the cause is that no Docker daemon is reachable from Floci: Floci running
     * inside Docker without a mounted socket, or a stopped daemon on the host. A failure raised
     * while the daemon <em>is</em> reachable is a genuine container problem and still propagates,
     * so nothing changes for a Floci that can start Memcached containers.
     *
     * @return the container handle, or {@code null} when no Docker daemon is reachable and no
     *         container was created
     */
    public ElastiCacheContainerHandle tryStart(String clusterId, String image, String region) {
        try {
            ElastiCacheContainerHandle handle = start(clusterId, image, region);
            dockerUnavailableLogged = false;
            return handle;
        } catch (RuntimeException e) {
            if (onSmolvmNode()) {
                throw e;
            }
            if (isDockerReachable()) {
                throw e;
            }
            ElastiCacheContainerHandle partial = activeContainers.get(clusterId);
            if (partial != null) {
                stop(partial);
                throw e;
            }
            if (!dockerUnavailableLogged) {
                dockerUnavailableLogged = true;
                LOG.warnv("No Docker daemon is reachable from Floci ({0}). ElastiCache metadata "
                        + "operations keep working and cache clusters still reach 'available', but "
                        + "they have no backing Memcached container until a daemon becomes "
                        + "reachable.", e.getMessage());
            }
            return null;
        }
    }

    /**
     * Probes the configured Docker endpoint, which is how a missing daemon is told apart from a
     * container that failed for its own reasons.
     */
    public boolean isDockerReachable() {
        try {
            lifecycleManager.getDockerClient().pingCmd().exec();
            return true;
        } catch (Exception e) {
            LOG.debugv("Docker daemon is not reachable: {0}", e.getMessage());
            return false;
        }
    }

    /** {@code region} is null for a cluster recorded without one, which then counts as the default region's. */
    public ElastiCacheContainerHandle start(String clusterId, String image, String region) {
        String effectiveRegion = region != null ? region : regionResolver.getDefaultRegion();
        LOG.infov("Starting Memcached container for cluster: {0}", clusterId);

        String containerName = ContainerStorageHelper.resourceName(config, "memcached", null, clusterId);
        ElastiCacheSmolvmBackend.Target node = onSmolvmNode()
                ? smolvm.target(regionResolver.getAccountId(), effectiveRegion) : null;
        ContainerLifecycleManager lifecycle = node == null ? lifecycleManager : node.lifecycle();
        ContainerLogStreamer logs = node == null ? logStreamer : node.logs();
        lifecycle.removeIfExists(containerName);

        ContainerBuilder.Builder specBuilder = containerBuilder.newContainer(image)
                .withName(containerName)
                .withLogRotation()
                .withLabels(ContainerStorageHelper.resourceIdentityLabels(
                        "elasticache", clusterId, regionResolver.getAccountId(), effectiveRegion));

        if (node != null) {
            specBuilder.withExposedPort(BACKEND_PORT);
        } else {
            specBuilder.withDockerNetwork(config.services().elasticache().dockerNetwork());
            if (!containerDetector.isRunningInContainer()) {
                specBuilder.withDynamicPort(BACKEND_PORT);
            } else {
                specBuilder.withExposedPort(BACKEND_PORT);
            }
        }

        ContainerSpec spec = specBuilder.build();
        if (node != null) {
            smolvm.ensureImage(node, spec.image());
        }
        ContainerInfo info = lifecycle.createAndStart(spec);
        try {
            EndpointInfo endpoint;
            if (node == null) {
                endpoint = info.getEndpoint(BACKEND_PORT);
            } else {
                String ip = lifecycle.resolveContainerNetworkIp(info.containerId(), null);
                try {
                    NodeTcpRelay relay = openRelay(node, ip);
                    nodeContainers.put(info.containerId(), new NodeContainer(lifecycle, relay));
                    endpoint = new EndpointInfo("127.0.0.1", relay.port());
                } catch (IOException e) {
                    throw new IllegalStateException("Could not expose Memcached on the node", e);
                }
            }

            LOG.infov("Memcached backend for cluster {0}: {1}", clusterId, endpoint);

            ElastiCacheContainerHandle handle = new ElastiCacheContainerHandle(
                    info.containerId(), clusterId, endpoint.host(), endpoint.port());
            activeContainers.put(clusterId, handle);

            String shortId = info.containerId().length() >= 8
                    ? info.containerId().substring(0, 8)
                    : info.containerId();
            String logGroup = "/aws/elasticache/cluster/" + clusterId + "/engine-log";
            String logStream = logs.generateLogStreamName(shortId);

            Closeable logHandle = logs.attach(
                    info.containerId(), logGroup, logStream, effectiveRegion, "elasticache-memcached:" + clusterId);
            handle.setLogStream(logHandle);

            waitForBackendReady(clusterId, endpoint.host(), endpoint.port());

            return handle;
        } catch (RuntimeException e) {
            if (node != null) {
                ElastiCacheContainerHandle partial = activeContainers.get(clusterId);
                Closeable logHandle = null;
                if (partial != null && info.containerId().equals(partial.getContainerId())) {
                    activeContainers.remove(clusterId, partial);
                    logHandle = partial.getLogStream();
                }
                closeRelay(info.containerId());
                try {
                    lifecycle.stopAndRemove(info.containerId(), logHandle);
                } catch (RuntimeException cleanupFailure) {
                    e.addSuppressed(cleanupFailure);
                }
            }
            throw e;
        }
    }

    private static void waitForBackendReady(String clusterId, String host, int port) {
        long deadline = System.currentTimeMillis() + BACKEND_READY_DEADLINE_MS;
        int attempt = 0;
        while (System.currentTimeMillis() < deadline) {
            attempt++;
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(host, port), BACKEND_PROBE_CONNECT_MS);
                s.setSoTimeout(BACKEND_PROBE_CONNECT_MS);
                OutputStream out = s.getOutputStream();
                out.write(TEXT_VERSION);
                out.flush();
                String line = readLine(s.getInputStream());
                if (line.startsWith("VERSION")) {
                    if (attempt > 1) {
                        LOG.infov("Memcached backend ready for cluster {0} after {1} probe attempt(s)", clusterId, attempt);
                    }
                    return;
                }
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debugv("Memcached probe for cluster {0} attempt {1}: {2}", clusterId, attempt, e.getMessage());
                }
            }
            try {
                Thread.sleep(BACKEND_READY_RETRY_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for Memcached backend " + clusterId, ie);
            }
        }
        throw new RuntimeException(
                "Memcached backend for cluster " + clusterId + " did not become ready on " + host + ":" + port
                        + " within " + BACKEND_READY_DEADLINE_MS + "ms");
    }

    private static String readLine(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                in.read(); // consume \n
                break;
            }
            sb.append((char) b);
        }
        return sb.toString();
    }

    public void stop(ElastiCacheContainerHandle handle) {
        if (handle == null) {
            return;
        }
        ElastiCacheContainerHandle existing = activeContainers.get(handle.getGroupId());
        if (existing != null && existing.getContainerId().equals(handle.getContainerId())) {
            activeContainers.remove(handle.getGroupId(), existing);
        } else {
            existing = null;
        }
        Closeable logStream = handle.getLogStream() != null ? handle.getLogStream()
                : (existing != null ? existing.getLogStream() : null);
        NodeContainer node = closeRelay(handle.getContainerId());
        ContainerLifecycleManager lifecycle = node == null ? lifecycle() : node.lifecycle();
        lifecycle.stopAndRemove(handle.getContainerId(), logStream);
    }

    private NodeTcpRelay openRelay(ElastiCacheSmolvmBackend.Target node, String ip) throws IOException {
        int base = config.services().elasticache().proxyBasePort();
        int max = config.services().elasticache().proxyMaxPort();
        for (int port = base; port <= max; port++) {
            if (!ports.claim(port)) {
                continue;
            }
            try {
                return smolvm.relay(node, ip, BACKEND_PORT, port);
            } catch (BindException e) {
                ports.release(port);
                LOG.debugv("Memcached relay port {0} is unavailable: {1}", port, e.getMessage());
            } catch (IOException | RuntimeException e) {
                ports.release(port);
                throw e;
            }
        }
        LOG.warnv("ElastiCache proxy port range {0}-{1} exhausted while starting Memcached", base, max);
        throw new AwsException("InsufficientCacheClusterCapacity",
                "The requested cache node type is not available in the specified Availability Zone.", 400);
    }

    private NodeContainer closeRelay(String containerId) {
        NodeContainer node = nodeContainers.remove(containerId);
        if (node != null) {
            int port = node.relay().port();
            node.relay().close();
            ports.release(port);
        }
        return node;
    }

    public void stopAll() {
        List<ElastiCacheContainerHandle> handles = new ArrayList<>(activeContainers.values());
        if (!handles.isEmpty()) {
            LOG.infov("Stopping {0} Memcached container(s) on shutdown", handles.size());
        }
        for (ElastiCacheContainerHandle handle : handles) {
            stop(handle);
        }
    }
}
