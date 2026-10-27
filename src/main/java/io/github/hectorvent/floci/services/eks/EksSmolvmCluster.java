package io.github.hectorvent.floci.services.eks;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.AwsRegions;
import io.github.hectorvent.floci.core.common.Resettable;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode.State;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode.Status;
import io.github.hectorvent.floci.runtime.flocinode.NodeKubeApi;
import io.github.hectorvent.floci.runtime.flocinode.NodeKubeApi.KubeApiException;
import io.github.hectorvent.floci.runtime.flocinode.NodeKubeApi.Taint;
import io.github.hectorvent.floci.runtime.flocinode.NodeRecord;
import io.github.hectorvent.floci.runtime.network.NodeNetworkPublisher;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeDirectory;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeSource;
import io.github.hectorvent.floci.services.eks.model.CertificateAuthority;
import io.github.hectorvent.floci.services.eks.model.Cluster;
import io.github.hectorvent.floci.services.eks.model.ClusterIdentity;
import io.github.hectorvent.floci.services.eks.model.ClusterOidcKey;
import io.github.hectorvent.floci.services.eks.model.ClusterStatus;
import io.github.hectorvent.floci.services.eks.model.KubernetesNetworkConfig;
import io.github.hectorvent.floci.services.eks.model.Nodegroup;
import io.github.hectorvent.floci.services.eks.model.OidcIdentity;
import io.quarkus.runtime.ShutdownEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The one EKS cluster that runs on the smolvm-pods node ({@link FlociNode}), with the
 * {@code smolvm-pods} provider: the cluster is the node's k3s. A second {@code CreateCluster}
 * fails with {@code ResourceLimitExceededException} while a cluster holds the node.
 *
 * <p>{@code CreateCluster} claims the node, and uses it: a cluster created first has Floci create
 * the node with its Service range, pod range and k3s arguments; a cluster created after RDS
 * adopts the node and its k3s as they are. Either way the cluster takes the node's service
 * account issuer, and its endpoint is the node's Kubernetes API. The cluster is CREATING until the
 * node is ready, and FAILED when the node fails.
 *
 * <p>{@code DeleteCluster} removes what the cluster put in Kubernetes, its namespaces, workloads
 * and cluster-scoped objects ({@link NodeKubeApi#removeWorkloads}) and the labels and taints of its
 * node groups, and keeps the node, its k3s and RDS's containers on it.
 */
@ApplicationScoped
public class EksSmolvmCluster implements Resettable, SmolvmNodeSource {

    private static final Logger LOG = Logger.getLogger(EksSmolvmCluster.class);

    /** How often the cluster's node is given the labels and taints of its node group. */
    static final Duration RECONCILE_INTERVAL = Duration.ofSeconds(10);

    private final EmulatorConfig config;
    private final FlociNode node;
    private final NodeKubeApi kubeApi;
    private final EksClusterManager clusterManager;
    private final EksOidcService oidcService;
    private final EndpointReach reach;
    private final Runnable aliasesChanged;
    private final EksSmolvmControlPlaneLogs logs;
    private final EksContainerInsights insights;
    private final Set<String> logged = ConcurrentHashMap.newKeySet();
    private final Map<String, String> failures = new ConcurrentHashMap<>();
    private volatile Cluster claimed;
    private volatile String metadataError;
    private ScheduledExecutorService reconciler;

    @Inject
    public EksSmolvmCluster(EmulatorConfig config, FlociNode node, NodeKubeApi kubeApi,
                            EksClusterManager clusterManager, EksOidcService oidcService,
                            NodeNetworkPublisher network, SmolvmNodeDirectory directory,
                            EksSmolvmControlPlaneLogs logs, EksContainerInsights insights) {
        this(config, node, kubeApi, clusterManager, oidcService, network::reaches, directory::refresh, logs,
                insights);
    }

    /** @param aliasesChanged tells the node directory that the cluster that finds the node changed */
    EksSmolvmCluster(EmulatorConfig config, FlociNode node, NodeKubeApi kubeApi, EksClusterManager clusterManager,
                     EksOidcService oidcService, EndpointReach reach, Runnable aliasesChanged,
                     EksSmolvmControlPlaneLogs logs, EksContainerInsights insights) {
        this.config = config;
        this.node = node;
        this.kubeApi = kubeApi;
        this.clusterManager = clusterManager;
        this.oidcService = oidcService;
        this.reach = reach;
        this.aliasesChanged = aliasesChanged;
        this.logs = logs;
        this.insights = insights;
    }

    /** Whether the pods of a node, by its directory name, reach a link-local endpoint. */
    @FunctionalInterface
    interface EndpointReach {
        boolean reaches(String node, String address, int port);
    }

    void onStop(@Observes ShutdownEvent ignored) {
        stopReconciler();
    }

    /**
     * Claims the node for {@code cluster}.
     *
     * @throws AwsException {@code ResourceLimitExceededException} when another cluster holds the
     *         node, and {@code InvalidRequestException} when the node is the runtime file's, whose
     *         k3s Floci does not manage
     */
    void claim(Cluster cluster) {
        boolean claimedNow;
        synchronized (this) {
            claimedNow = claimLocked(cluster);
        }
        if (claimedNow) {
            aliasesChanged.run();
        }
    }

    private boolean claimLocked(Cluster cluster) {
        Cluster holder = claimed;
        if (holder != null && !key(holder).equals(key(cluster))) {
            throw new AwsException("ResourceLimitExceededException", "Floci runs one EKS cluster per smolvm-pods "
                    + "node, and cluster " + holder.getName() + " (account " + clusterManager.resolveClusterAccountId(holder)
                    + ", region " + region(holder) + ") runs on it. Delete that cluster first, or set "
                    + "FLOCI_SERVICES_EKS_PROVIDER=k3s to run each cluster in a k3s container instead.", 400);
        }
        Status status = node.status();
        if (status.declared()) {
            throw new AwsException("InvalidRequestException", "The smolvm-pods node of Floci is "
                    + (status.node() != null ? status.node().name() : "the runtime file's")
                    + ", which Floci did not create, so it holds no EKS cluster. Remove it from the runtime file's "
                    + "nodes to have Floci create a node, or set FLOCI_SERVICES_EKS_PROVIDER=k3s.", 400);
        }
        if (status.record() != null) {
            requireUsable(cluster, status.record());
        }
        boolean claimedNow = holder == null;
        claimed = cluster;
        return claimedNow;
    }

    /** Releases the node that {@code cluster} claimed, if it did. */
    void release(Cluster cluster) {
        boolean released = false;
        synchronized (this) {
            Cluster holder = claimed;
            if (holder != null && key(holder).equals(key(cluster))) {
                claimed = null;
                released = true;
            }
        }
        failures.remove(key(cluster));
        if (released) {
            aliasesChanged.run();
        }
    }

    /** No node of its own: the cluster's node is the Floci node. */
    @Override
    public List<SmolvmNode> nodes() {
        return List.of();
    }

    /** The name of the cluster on the node, {@code cluster:ACCOUNT/REGION/NAME}, finds the node. */
    @Override
    public Map<String, String> aliases() {
        Cluster holder = claimed;
        if (holder == null) {
            return Map.of();
        }
        return Map.of(SmolvmNode.clusterName(clusterManager.resolveClusterAccountId(holder), region(holder),
                holder.getName()), FlociNode.NAME);
    }

    /** The cluster that runs on the node now. */
    Optional<Cluster> cluster() {
        return Optional.ofNullable(claimed);
    }

    /**
     * Uses the node for {@code cluster}, which claimed it: records the provider, the version when
     * none was requested, the node and its endpoint, and the node's issuer, and has Floci create the
     * node when there is none.
     *
     * @return true, since the cluster then has a node that becomes ready
     * @throws IllegalStateException when the settings or the Mac keep Floci from creating the node
     */
    boolean create(Cluster cluster) {
        cluster.setProvider(EksSmolvmSettings.PROVIDER_SMOLVM_PODS);
        if (!cluster.isExplicitVersion()) {
            cluster.setVersion(config.services().eks().smolvm().kubernetesVersion());
        }
        Status status = node.acquire(new FlociNode.Use("EKS cluster " + cluster.getName(),
                clusterManager.resolveClusterAccountId(cluster), region(cluster), serviceCidr(cluster),
                cluster.getPodCidr(), EksSmolvmSeedContributor.k3sArgs(clusterManager.resolveCallerArgs(cluster))));
        adopt(cluster, status.record());
        startReconciler();
        return true;
    }

    /**
     * Uses the node again for {@code cluster} after a Floci start. Never throws; a failure makes
     * the cluster FAILED.
     */
    void restore(Cluster cluster) {
        try {
            claim(cluster);
            create(cluster);
        } catch (AwsException | IllegalStateException e) {
            failures.put(key(cluster), e.getMessage());
            LOG.warnv("EKS cluster {0} cannot run on the smolvm-pods node: {1}", cluster.getName(), e.getMessage());
        }
    }

    /**
     * Makes {@code cluster} the cluster of node {@code record}: its endpoint, its node, its Service
     * and pod ranges, and the node's service account issuer and key. A node created for another
     * use keeps its k3s, so the cluster reports the node's ranges, and a request for others or for
     * k3s arguments is logged.
     */
    private void adopt(Cluster cluster, NodeRecord record) {
        if (record == null) {
            throw new IllegalStateException("The smolvm-pods node has no record");
        }
        requireUsable(cluster, record);
        cluster.setSmolvmNodeId(record.nodeId());
        cluster.setEndpoint(endpoint(record));
        String requested = serviceCidr(cluster);
        if (requested != null && !requested.equals(record.serviceCidr())) {
            LOG.warnv("EKS cluster {0} asked for Service range {1}, and the smolvm-pods node {2} it runs on has {3}",
                    cluster.getName(), requested, record.nodeId(), record.serviceCidr());
        }
        if (cluster.getKubernetesNetworkConfig() == null) {
            cluster.setKubernetesNetworkConfig(new KubernetesNetworkConfig());
        }
        cluster.getKubernetesNetworkConfig().setServiceIpv4Cidr(record.serviceCidr());
        cluster.setPodCidr(record.podCidr());
        Map<String, List<String>> args = EksSmolvmSeedContributor.k3sArgs(clusterManager.resolveCallerArgs(cluster));
        if (!args.isEmpty() && !args.equals(record.k3sArgs())) {
            LOG.warnv("EKS cluster {0} asked for k3s arguments {1}, and the smolvm-pods node {2} it runs on keeps "
                    + "{3}", cluster.getName(), args, record.nodeId(), record.k3sArgs());
        }
        if (config.services().eks().irsaSigningKey()) {
            ClusterOidcKey key = oidcService.nodeKey(record.accountId(), record.nodeId(), record.region());
            cluster.setIdentity(new ClusterIdentity(new OidcIdentity(key.getIssuer())));
            oidcService.adoptKey(clusterManager.resolveClusterAccountId(cluster), cluster.getName(), key);
        }
        LOG.infov("EKS cluster {0} runs on smolvm-pods node {1}, API https://localhost:{2}", cluster.getName(),
                record.nodeId(), String.valueOf(record.apiPort()));
    }

    /**
     * Refuses a node that cannot hold {@code cluster}: one whose k3s booted without EKS's seed,
     * since it was created while EKS clusters did not run on it, and one in another partition,
     * whose name, service account issuer and k3s are of that partition.
     *
     * @throws AwsException {@code InvalidRequestException} for a node without EKS's seed, and
     *         {@code InvalidParameterException} naming both partitions
     */
    private void requireUsable(Cluster cluster, NodeRecord record) {
        if (!record.seeded().contains(EksSmolvmSeedContributor.NAME)) {
            throw new AwsException("InvalidRequestException", "Floci's smolvm-pods node " + record.nodeId()
                    + " was created while EKS clusters did not run on it (FLOCI_SERVICES_EKS_PROVIDER was not "
                    + "smolvm-pods), so its k3s has none of EKS's seed: no token webhook, service account signing key "
                    + "or ECR mirror, and IRSA would fail. Stop the node with node-vm.sh stop --name "
                    + record.nodeId() + " and restart Floci, which starts it with EKS's seed, or reset Floci, which "
                    + "deletes the node.", 400);
        }
        String partition = AwsRegions.partitionFor(region(cluster));
        String nodePartition = AwsRegions.partitionFor(record.region());
        if (!partition.equals(nodePartition)) {
            throw new AwsException("InvalidParameterException", "Cluster " + cluster.getName() + " is in partition "
                    + partition + " (region " + region(cluster) + "), and Floci's smolvm-pods node " + record.nodeId()
                    + " runs in partition " + nodePartition + " (region " + record.region() + "), whose node name "
                    + "and service account issuer it keeps. Create the cluster in a region of " + nodePartition
                    + ", or reset Floci, which deletes the node, to have the next use create one in " + partition
                    + ".", 400);
        }
    }

    /** The error that keeps {@code cluster} from running on the node, if one does. */
    Optional<String> creationFailure(Cluster cluster) {
        String failure = failures.get(key(cluster));
        if (failure != null) {
            return Optional.of(failure);
        }
        if (!holds(cluster)) {
            return Optional.of("EKS cluster " + cluster.getName() + " does not hold the smolvm-pods node");
        }
        Status status = node.status();
        return status.state() == State.FAILED ? Optional.ofNullable(status.failure()) : Optional.empty();
    }

    /** Whether the node of {@code cluster} is ready for the cluster to be ACTIVE. */
    boolean isReady(Cluster cluster) {
        if (!holds(cluster)) {
            return false;
        }
        Status status = node.status();
        if (status.state() != State.READY || status.record() == null) {
            return false;
        }
        if (!status.record().nodeId().equals(cluster.getSmolvmNodeId())) {
            try {
                adopt(cluster, status.record());
            } catch (AwsException e) {
                failures.put(key(cluster), e.getMessage());
                return false;
            }
        }
        return true;
    }

    /**
     * Completes a cluster whose node is ready: its endpoint and certificate authority from the
     * node, its EC2 node instance, the labels and taints of its first node group, and a check of
     * the version the node runs.
     */
    void finalizeCluster(Cluster cluster) {
        Optional<NodeRecord> found = node.record();
        if (found.isEmpty()) {
            return;
        }
        NodeRecord record = found.get();
        cluster.setEndpoint(endpoint(record));
        try {
            String caPem = kubeApi.caPem(record);
            cluster.setCertificateAuthority(new CertificateAuthority(
                    Base64.getEncoder().encodeToString(caPem.getBytes(StandardCharsets.US_ASCII))));
        } catch (KubeApiException e) {
            LOG.warnv("Could not read the cluster CA of EKS cluster {0} from smolvm-pods node {1}: {2}",
                    cluster.getName(), record.nodeId(), e.getMessage());
        }
        clusterManager.registerSmolvmNodeInstance(cluster);
        applyNodeGroup(cluster);
        checkVersion(cluster, record);
        attachLogs(cluster);
        LOG.infov("Finalized EKS cluster {0} on smolvm-pods node {1}, API https://localhost:{2}", cluster.getName(),
                record.nodeId(), String.valueOf(record.apiPort()));
    }

    /**
     * Removes what {@code cluster} put on the node and releases the node: the namespaces and
     * workloads, and the labels and taints of its node groups. The node, its k3s and RDS's
     * containers stay. A node that does not answer keeps them, which is logged.
     */
    void delete(Cluster cluster) {
        if (holds(cluster)) {
            Optional<NodeRecord> record = node.record();
            if (record.isPresent() && node.status().state() == State.READY) {
                try {
                    kubeApi.removeWorkloads(record.get());
                    kubeApi.applyNodeMetadata(record.get(), record.get().nodeName(), Map.of(), List.of());
                } catch (KubeApiException e) {
                    LOG.warnv("Could not remove the Kubernetes objects of deleted EKS cluster {0} from smolvm-pods "
                            + "node {1}; they stay for the next cluster: {2}", cluster.getName(),
                            record.get().nodeId(), e.getMessage());
                }
            }
            LOG.infov("EKS cluster {0} is deleted, and its smolvm-pods node {1} keeps running", cluster.getName(),
                    cluster.getSmolvmNodeId());
        }
        release(cluster);
        String key = key(cluster);
        logs.close(key);
        insights.close(key);
        logged.removeIf(entry -> entry.startsWith(key + "|"));
        metadataError = null;
    }

    @Override
    public void clear() {
        synchronized (this) {
            claimed = null;
        }
        failures.clear();
        logged.clear();
        metadataError = null;
        logs.closeAll();
        insights.closeAll();
        aliasesChanged.run();
    }

    /**
     * Ships the control plane log types that {@code cluster} enables, {@code api} and {@code audit},
     * from its node to CloudWatch Logs, from now on, and stops shipping the others.
     */
    void attachLogs(Cluster cluster) {
        Optional<SmolvmNode> smolvmNode = node.node();
        Optional<NodeRecord> record = node.record();
        if (!holds(cluster) || smolvmNode.isEmpty() || record.isEmpty()) {
            logs.close(key(cluster));
            return;
        }
        List<String> types = new ArrayList<>();
        for (String type : List.of("api", "audit")) {
            if (EksClusterManager.hasLoggingEnabled(cluster, type)) {
                types.add(type);
            }
        }
        logs.attach(key(cluster), cluster.getName(), clusterManager.resolveClusterAccountId(cluster), region(cluster),
                smolvmNode.get(), record.get().nodeId(), types);
    }

    /** The node's name in Kubernetes, when Floci created the node. */
    Optional<String> nodeName() {
        return node.record().map(NodeRecord::nodeName);
    }

    /**
     * Whether the pods of {@code cluster}'s node reach Floci's Pod Identity credentials endpoint,
     * {@code 169.254.170.23:80}: the node applied a network profile that sends it to Floci. Until
     * it does, admission gives the cluster's pods IRSA where their service account names a role,
     * and says so once per cluster.
     */
    boolean supportsPodIdentity(Cluster cluster) {
        LinkLocalEndpoint podIdentity = EksPodNetworkRouting.POD_IDENTITY_ENDPOINT;
        if (reach.reaches(FlociNode.NAME, podIdentity.ip(), podIdentity.port())) {
            return true;
        }
        logUnsupported(cluster, "its Pod Identity associations until its node applies a network profile with their "
                + "endpoint; their pods get IRSA where the service account names a role");
        return false;
    }

    /** Reads the ServiceAccounts of {@code cluster} from its node's Kubernetes API. */
    EksServiceAccountReader serviceAccountReader(Cluster cluster) {
        return (namespace, name) -> {
            Optional<NodeRecord> record = node.record();
            if (record.isEmpty() || !holds(cluster)) {
                return Optional.empty();
            }
            try {
                return kubeApi.serviceAccount(record.get(), namespace, name, EksServiceAccountReader.TIMEOUT);
            } catch (KubeApiException e) {
                if (e.getCause() instanceof HttpTimeoutException) {
                    throw new IllegalStateException("Reading ServiceAccount " + namespace + "/" + name
                            + " timed out after " + EksServiceAccountReader.TIMEOUT.toSeconds() + " s");
                }
                // What the API server answered stays out of admission logs.
                throw new IllegalStateException("Cannot read EKS service account annotations");
            }
        };
    }

    /** Logs once per cluster that Floci skips {@code what}, which a smolvm-pods node does not support. */
    void logUnsupported(Cluster cluster, String what) {
        if (logged.add(key(cluster) + "|" + what)) {
            LOG.infov("EKS cluster {0} runs on a smolvm-pods node, so Floci skips {1}", cluster.getName(), what);
        }
    }

    /**
     * Gives the node the labels and taints of its ACTIVE cluster's first node group, and follows the
     * logs of its containers for Container Insights.
     */
    void reconcile() {
        Cluster cluster = claimed;
        if (cluster != null && cluster.getStatus() == ClusterStatus.ACTIVE && node.status().state() == State.READY) {
            applyNodeGroup(cluster);
            node.record().ifPresent(record -> insights.reconcile(key(cluster), cluster,
                    clusterManager.resolveClusterAccountId(cluster), region(cluster), record));
        }
    }

    /** Gives the node the labels and taints of {@code cluster}'s first active node group. */
    private void applyNodeGroup(Cluster cluster) {
        Optional<NodeRecord> found = node.record();
        if (found.isEmpty()) {
            return;
        }
        NodeRecord record = found.get();
        String region = region(cluster);
        Nodegroup group = clusterManager.listActiveNodegroups(cluster).stream().findFirst().orElse(null);
        Map<String, String> labels = clusterManager.nodeLabels(cluster, group,
                clusterManager.deriveClusterNodeAvailabilityZone(cluster, region), region);
        try {
            kubeApi.applyNodeMetadata(record, record.nodeName(), labels, taints(group));
            metadataError = null;
        } catch (KubeApiException e) {
            if (!e.getMessage().equals(metadataError)) {
                metadataError = e.getMessage();
                LOG.warnv("Could not give the node of EKS cluster {0} the labels and taints of its node group: {1}",
                        cluster.getName(), e.getMessage());
            }
        }
    }

    /** The taints of {@code group}, as maps of key, value and effect or as {@code key=value:Effect}. */
    static List<Taint> taints(Nodegroup group) {
        List<Taint> taints = new ArrayList<>();
        if (group == null || group.getTaints() == null) {
            return taints;
        }
        for (Object taint : group.getTaints()) {
            if (taint instanceof Map<?, ?> map && map.get("key") != null && !map.get("key").toString().isBlank()) {
                Object value = map.get("value");
                Object effect = map.get("effect");
                taints.add(new Taint(map.get("key").toString().strip(),
                        value == null || value.toString().isBlank() ? null : value.toString().strip(),
                        EksClusterManager.mapTaintEffect(effect == null ? null : effect.toString())));
            } else if (taint instanceof String text && !text.isBlank()) {
                int colon = text.lastIndexOf(':');
                String keyValue = colon > 0 ? text.substring(0, colon) : text;
                String effect = EksClusterManager.mapTaintEffect(colon > 0 ? text.substring(colon + 1) : null);
                int equals = keyValue.indexOf('=');
                taints.add(new Taint(equals > 0 ? keyValue.substring(0, equals).strip() : keyValue.strip(),
                        equals > 0 ? keyValue.substring(equals + 1).strip() : null, effect));
            }
        }
        return taints;
    }

    private void checkVersion(Cluster cluster, NodeRecord record) {
        try {
            NodeKubeApi.Version version = kubeApi.version(record);
            String minor = version.minor() == null ? "" : version.minor().replaceAll("\\D", "");
            String running = version.major() + "." + minor;
            if (!running.equals(cluster.getVersion())) {
                LOG.warnv("EKS cluster {0} reports Kubernetes {1}, and its smolvm-pods node runs {2} ({3})",
                        cluster.getName(), cluster.getVersion(), running, version.gitVersion());
            }
        } catch (KubeApiException e) {
            LOG.debugv("Could not read the Kubernetes version of smolvm-pods node {0}: {1}", record.nodeId(),
                    e.getMessage());
        }
    }

    private boolean holds(Cluster cluster) {
        Cluster holder = claimed;
        return holder != null && key(holder).equals(key(cluster));
    }

    private synchronized void startReconciler() {
        if (reconciler != null) {
            return;
        }
        reconciler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().daemon().name("eks-smolvm-reconcile").factory());
        reconciler.scheduleWithFixedDelay(() -> {
            try {
                reconcile();
            } catch (RuntimeException e) {
                LOG.errorv(e, "Giving the smolvm-pods node its node group's labels and taints failed");
            }
        }, RECONCILE_INTERVAL.toMillis(), RECONCILE_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    }

    private synchronized void stopReconciler() {
        if (reconciler != null) {
            reconciler.shutdownNow();
            reconciler = null;
        }
    }

    private static String serviceCidr(Cluster cluster) {
        return cluster.getKubernetesNetworkConfig() != null
                ? cluster.getKubernetesNetworkConfig().getServiceIpv4Cidr() : null;
    }

    String region(Cluster cluster) {
        return clusterManager.clusterRegion(cluster);
    }

    private String key(Cluster cluster) {
        return clusterManager.resolveClusterAccountId(cluster) + "/" + region(cluster) + "/" + cluster.getName();
    }

    private static String endpoint(NodeRecord record) {
        return "https://" + NodeKubeApi.HOST + ":" + record.apiPort();
    }
}
