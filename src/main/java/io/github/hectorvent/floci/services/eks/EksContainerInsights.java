package io.github.hectorvent.floci.services.eks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.runtime.flocinode.NodeKubeApi;
import io.github.hectorvent.floci.runtime.flocinode.NodeKubeApi.KubeApiException;
import io.github.hectorvent.floci.runtime.flocinode.NodeKubeApi.PodContainer;
import io.github.hectorvent.floci.runtime.flocinode.NodeRecord;
import io.github.hectorvent.floci.services.eks.model.Cluster;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Container Insights application logs of the EKS cluster on the smolvm-pods node, while the
 * {@code amazon-cloudwatch-observability} add-on is on it: each running container's log goes to
 * {@code /aws/containerinsights/CLUSTER/application}, in a stream named as the add-on's Fluent Bit
 * names it, {@code HOST-application.var.log.containers.POD_NAMESPACE_CONTAINER-ID.log}, one JSON
 * event per line with the {@code log}, its {@code time} and the {@code kubernetes} metadata. The
 * logs come from the Kubernetes API's pod logs, which the kubelet reads from the node's files, so
 * they do not wake a sleeping pod. The reconciler finds new containers every 10 s. Those running
 * when the add-on is created, or when Floci starts, ship from then on; a later one ships its whole
 * log. Floci sends no Container Insights metrics.
 */
@ApplicationScoped
public class EksContainerInsights {

    static final String ADDON = "amazon-cloudwatch-observability";
    private static final Logger LOG = Logger.getLogger(EksContainerInsights.class);

    private final NodeKubeApi kubeApi;
    private final ContainerLogStreamer writer;
    private final EksAddonService addons;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, Map<String, Follow>> follows = new ConcurrentHashMap<>();

    @Inject
    public EksContainerInsights(NodeKubeApi kubeApi, ContainerLogStreamer writer, EksAddonService addons) {
        this.kubeApi = kubeApi;
        this.writer = writer;
        this.addons = addons;
    }

    /**
     * Follows the log of each container that runs now on {@code record}'s node, and stops following
     * those that ended, while {@code cluster} has the add-on; else stops following them all.
     *
     * @param key the cluster's key, by which {@link #close} stops it
     */
    void reconcile(String key, Cluster cluster, String accountId, String region, NodeRecord record) {
        if (!addons.isInstalled(cluster, ADDON)) {
            close(key);
            return;
        }
        boolean adopting = !follows.containsKey(key);
        List<PodContainer> running;
        try {
            running = kubeApi.runningContainers(record);
        } catch (KubeApiException e) {
            LOG.debugv("Could not list the containers of EKS cluster {0}: {1}", cluster.getName(), e.getMessage());
            return;
        }
        Map<String, Follow> current = follows.computeIfAbsent(key, ignored -> new ConcurrentHashMap<>());
        String logGroup = "/aws/containerinsights/" + cluster.getName() + "/application";
        Set<String> live = new HashSet<>();
        for (PodContainer container : running) {
            live.add(container.containerId());
            Follow follow = current.get(container.containerId());
            if (follow == null || follow.ended) {
                Follow started = new Follow(container, accountId, region, logGroup, record);
                current.put(container.containerId(), started);
                started.start(adopting || follow != null);
            }
        }
        current.entrySet().removeIf(entry -> {
            if (live.contains(entry.getKey())) {
                return false;
            }
            entry.getValue().close();
            return true;
        });
    }

    /** Stops following the containers of the cluster {@code key}. */
    void close(String key) {
        Map<String, Follow> stopped = follows.remove(key);
        if (stopped != null) {
            stopped.values().forEach(Follow::close);
        }
    }

    @PreDestroy
    void closeAll() {
        for (String key : List.copyOf(follows.keySet())) {
            close(key);
        }
    }

    /** The stream that the add-on's Fluent Bit writes a container's log to. */
    static String logStream(PodContainer container) {
        return container.host() + "-application.var.log.containers." + container.pod() + "_"
                + container.namespace() + "_" + container.container() + "-" + container.containerId() + ".log";
    }

    /** A CloudWatch Logs event of a stamped pod log line, shaped as the add-on's Fluent Bit sends it. */
    String event(PodContainer container, String stamped) {
        int space = stamped.indexOf(' ');
        ObjectNode event = mapper.createObjectNode();
        event.put("time", space > 0 ? stamped.substring(0, space) : null);
        event.put("log", space > 0 ? stamped.substring(space + 1) : stamped);
        ObjectNode kubernetes = event.putObject("kubernetes");
        kubernetes.put("pod_name", container.pod());
        kubernetes.put("namespace_name", container.namespace());
        kubernetes.put("pod_id", container.podUid());
        kubernetes.put("host", container.host());
        kubernetes.put("container_name", container.container());
        kubernetes.put("docker_id", container.containerId());
        kubernetes.put("container_image", container.image());
        return event.toString();
    }

    /** One container's log, read on a virtual thread until it ends or is closed. */
    private final class Follow {

        private final PodContainer container;
        private final String accountId;
        private final String region;
        private final String logGroup;
        private final String logStream;
        private final NodeRecord record;
        private volatile InputStream body;
        private volatile boolean closed;
        private volatile boolean ended;

        private Follow(PodContainer container, String accountId, String region, String logGroup,
                       NodeRecord record) {
            this.container = container;
            this.accountId = accountId;
            this.region = region;
            this.logGroup = logGroup;
            this.logStream = logStream(container);
            this.record = record;
        }

        void start(boolean fromNow) {
            Thread.ofVirtual().name("eks-container-insights").start(() -> {
                try {
                    writer.ensureLogGroupAndStreamForAccount(accountId, logGroup, logStream, region);
                    body = kubeApi.followPodLog(record, container, fromNow);
                    if (closed) {
                        body.close();
                        return;
                    }
                    try (BufferedReader lines = new BufferedReader(new InputStreamReader(body,
                            StandardCharsets.UTF_8))) {
                        String line;
                        while ((line = lines.readLine()) != null) {
                            String trimmed = line.stripTrailing();
                            if (!trimmed.isEmpty()) {
                                writer.streamToCloudWatchLogsForAccount(accountId, logGroup, logStream, region,
                                        event(container, trimmed));
                            }
                        }
                    }
                } catch (IOException | RuntimeException e) {
                    if (!closed) {
                        LOG.debugv("The log of container {0} of pod {1}/{2} ended: {3}", container.container(),
                                container.namespace(), container.pod(), e.getMessage());
                    }
                } finally {
                    ended = true;
                }
            });
        }

        void close() {
            closed = true;
            InputStream current = body;
            if (current != null) {
                try {
                    current.close();
                } catch (IOException e) {
                    LOG.debugv("Could not close the log of container {0}: {1}", container.containerId(),
                            e.getMessage());
                }
            }
        }
    }
}
