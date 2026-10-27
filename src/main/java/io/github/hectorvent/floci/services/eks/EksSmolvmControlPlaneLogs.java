package io.github.hectorvent.floci.services.eks;

import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient.EventFollow;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Ships the control plane logs of the EKS cluster on the smolvm-pods node to CloudWatch Logs, as
 * the k3s container of a Docker cluster does: {@code api} from the node's k3s log to
 * {@code /aws/eks/NAME/cluster} stream {@code kube-apiserver-ID}, and {@code audit} from the API
 * server's audit log to stream {@code kube-apiserver-audit-ID}, through the node API's log follows
 * ({@link SmolvmNodeClient#followLog}). They ship the lines written from the attach on, and a
 * follow that ends while the cluster still ships it follows again, after 1 s and up to 30 s.
 */
@ApplicationScoped
public class EksSmolvmControlPlaneLogs {

    private static final Logger LOG = Logger.getLogger(EksSmolvmControlPlaneLogs.class);
    static final Duration FIRST_DELAY = Duration.ofSeconds(1);
    static final Duration MAX_DELAY = Duration.ofSeconds(30);
    /** A follow that lasted this long ended for a reason of its own, so the next waits the first delay. */
    private static final Duration STEADY = Duration.ofSeconds(10);

    private final SmolvmNodeClient client;
    private final ContainerLogStreamer writer;
    private final Duration firstDelay;
    private final Map<String, List<Shipping>> shipping = new ConcurrentHashMap<>();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("eks-smolvm-logs").factory());

    @Inject
    public EksSmolvmControlPlaneLogs(SmolvmNodeClient client, ContainerLogStreamer writer) {
        this(client, writer, FIRST_DELAY);
    }

    EksSmolvmControlPlaneLogs(SmolvmNodeClient client, ContainerLogStreamer writer, Duration firstDelay) {
        this.client = client;
        this.writer = writer;
        this.firstDelay = firstDelay;
    }

    /**
     * Ships the types {@code types} (of {@code api} and {@code audit}) of the cluster {@code key}
     * from {@code node}, and stops shipping what it shipped before.
     *
     * @param nodeId the node's id, which names the log streams
     */
    void attach(String key, String clusterName, String accountId, String region, SmolvmNode node, String nodeId,
                List<String> types) {
        close(key);
        String logGroup = "/aws/eks/" + clusterName + "/cluster";
        String id = streamId(nodeId);
        List<Shipping> started = new ArrayList<>();
        if (types.contains("api")) {
            started.add(new Shipping(node, SmolvmNodeClient.K3S_LOG, accountId, region, logGroup,
                    "kube-apiserver-" + id));
        }
        if (types.contains("audit")) {
            started.add(new Shipping(node, SmolvmNodeClient.KUBE_APISERVER_AUDIT_LOG, accountId, region, logGroup,
                    "kube-apiserver-audit-" + id));
        }
        if (started.isEmpty()) {
            return;
        }
        shipping.put(key, started);
        started.forEach(Shipping::start);
        LOG.infov("EKS cluster {0} ships its control plane logs {1} from smolvm-pods node {2} to {3}",
                clusterName, types, nodeId, logGroup);
    }

    /** Stops shipping the logs of the cluster {@code key}. */
    void close(String key) {
        List<Shipping> stopped = shipping.remove(key);
        if (stopped != null) {
            stopped.forEach(Shipping::close);
        }
    }

    /** Stops shipping the logs of every cluster. */
    void closeAll() {
        for (String key : List.copyOf(shipping.keySet())) {
            close(key);
        }
    }

    @PreDestroy
    void stop() {
        closeAll();
        timer.shutdownNow();
    }

    /** 32 hex digits of the node id's SHA-256, as AWS names the API server's streams by a 32-digit id. */
    static String streamId(String nodeId) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(nodeId.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest).substring(0, 32);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    /** One log of the node shipped to one stream, followed again whenever its follow ends. */
    private final class Shipping {

        private final SmolvmNode node;
        private final String log;
        private final String accountId;
        private final String region;
        private final String logGroup;
        private final String logStream;
        private volatile boolean closed;
        private volatile EventFollow follow;
        private volatile long startedNanos;
        private volatile Duration delay = firstDelay;

        private Shipping(SmolvmNode node, String log, String accountId, String region, String logGroup,
                         String logStream) {
            this.node = node;
            this.log = log;
            this.accountId = accountId;
            this.region = region;
            this.logGroup = logGroup;
            this.logStream = logStream;
        }

        void start() {
            if (closed) {
                return;
            }
            writer.ensureLogGroupAndStreamForAccount(accountId, logGroup, logStream, region);
            startedNanos = System.nanoTime();
            EventFollow started;
            try {
                started = client.followLog(node, log, this::line);
            } catch (RuntimeException e) {
                LOG.debugv("Could not follow the {0} log of smolvm-pods node {1}: {2}", log, node.name(),
                        e.getMessage());
                again();
                return;
            }
            follow = started;
            if (closed) {
                started.close();
                return;
            }
            started.done().whenComplete((ignored, failure) -> {
                if (!closed) {
                    LOG.debugv("The follow of the {0} log of smolvm-pods node {1} ended: {2}", log, node.name(),
                            failure == null ? "the node ended it" : failure.getMessage());
                    again();
                }
            });
        }

        private void line(String line) {
            String trimmed = line.stripTrailing();
            if (!trimmed.isEmpty()) {
                writer.streamToCloudWatchLogsForAccount(accountId, logGroup, logStream, region, trimmed);
            }
        }

        private void again() {
            if (System.nanoTime() - startedNanos >= STEADY.toNanos()) {
                delay = firstDelay;
            }
            Duration wait = delay;
            delay = delay.multipliedBy(2).compareTo(MAX_DELAY) > 0 ? MAX_DELAY : delay.multipliedBy(2);
            try {
                timer.schedule(this::start, wait.toMillis(), TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                LOG.debugv("Not following the {0} log of smolvm-pods node {1} again: Floci is stopping", log,
                        node.name());
            }
        }

        void close() {
            closed = true;
            EventFollow current = follow;
            if (current != null) {
                current.close();
            }
        }
    }
}
