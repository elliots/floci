package io.github.hectorvent.floci.services.eks;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.EksServiceConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.EksServiceConfig.SmolvmConfig;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.DockerHostResolver;
import io.github.hectorvent.floci.core.common.docker.PortAllocator;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode.State;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode.Status;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode.Use;
import io.github.hectorvent.floci.runtime.flocinode.NodeKubeApi;
import io.github.hectorvent.floci.runtime.flocinode.NodeKubeApi.KubeApiException;
import io.github.hectorvent.floci.runtime.flocinode.NodeKubeApi.Taint;
import io.github.hectorvent.floci.runtime.flocinode.NodeRecord;
import io.github.hectorvent.floci.runtime.flocinode.NodeSeed;
import io.github.hectorvent.floci.runtime.node.NodeToken;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.services.ecr.registry.EcrRegistryManager;
import io.github.hectorvent.floci.services.eks.model.Cluster;
import io.github.hectorvent.floci.services.eks.model.ClusterStatus;
import io.github.hectorvent.floci.services.eks.model.KubernetesNetworkConfig;
import io.github.hectorvent.floci.services.eks.model.LogSetup;
import io.github.hectorvent.floci.services.eks.model.Logging;
import io.github.hectorvent.floci.services.eks.model.Nodegroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.URI;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EksSmolvmClusterServiceTest {

    private static final String ACCOUNT = "123456789012";
    private static final String REGION = "us-east-1";
    private static final NodeRecord NODE = new NodeRecord("floci-node-0123456789abcdef", 6501, 6601, "none",
            "a".repeat(64), false, List.of(), ACCOUNT, "eu-west-1", "i-0123456789abcdef0.eu-west-1.compute.internal",
            "10.100.0.0/16", "10.42.0.0/16", Map.of(), List.of(EksSmolvmSeedContributor.NAME));

    private EksServiceConfig eks;
    private FlociNode flociNode;
    private NodeKubeApi kubeApi;
    private EksClusterManager clusterManager;
    private EksOidcService oidcService;
    private final AtomicInteger aliasChanges = new AtomicInteger();
    private EksSmolvmCluster smolvm;
    private final EksSmolvmControlPlaneLogs logs = mock(EksSmolvmControlPlaneLogs.class);

    @BeforeEach
    void setUp() {
        EmulatorConfig config = mock(EmulatorConfig.class);
        EmulatorConfig.ServicesConfig services = mock(EmulatorConfig.ServicesConfig.class);
        eks = mock(EksServiceConfig.class);
        SmolvmConfig smolvmConfig = mock(SmolvmConfig.class);
        when(config.services()).thenReturn(services);
        when(config.defaultRegion()).thenReturn(REGION);
        when(config.defaultAccountId()).thenReturn("000000000000");
        when(services.eks()).thenReturn(eks);
        when(eks.smolvm()).thenReturn(smolvmConfig);
        when(eks.provider()).thenReturn(EksSmolvmSettings.PROVIDER_SMOLVM_PODS);
        when(eks.irsaSigningKey()).thenReturn(true);
        when(smolvmConfig.kubernetesVersion()).thenReturn("1.35");
        flociNode = mock(FlociNode.class);
        when(flociNode.status()).thenReturn(new Status(State.NONE, null, null, null, false));
        when(flociNode.acquire(any())).thenReturn(new Status(State.CREATING, null, NODE, null, false));
        when(flociNode.record()).thenReturn(Optional.of(NODE));
        kubeApi = mock(NodeKubeApi.class);
        oidcService = new EksOidcService(new StorageFactory(null, null) {
            @Override
            public <V> AccountAwareStorageBackend<V> create(String serviceName, String fileName,
                    TypeReference<Map<String, V>> typeReference) {
                return AccountAwareStorageBackend.inMemory("000000000000");
            }
        }, new ObjectMapper());
        clusterManager = new EksClusterManager(mock(ContainerBuilder.class), mock(ContainerLifecycleManager.class),
                mock(ContainerDetector.class), mock(PortAllocator.class), mock(DockerHostResolver.class),
                mock(EcrRegistryManager.class), config, new RegionResolver(REGION, "000000000000"), null, oidcService);
        smolvm = new EksSmolvmCluster(config, flociNode, kubeApi, clusterManager, oidcService, (node, ip, port) -> false,
                aliasChanges::incrementAndGet, logs, mock(EksContainerInsights.class));
        clusterManager.setSmolvmCluster(smolvm);
    }

    private static Cluster cluster(String name) {
        return cluster(name, REGION);
    }

    private static Cluster cluster(String name, String region) {
        Cluster cluster = new Cluster();
        cluster.setName(name);
        cluster.setAccountId(ACCOUNT);
        cluster.setArn("arn:aws:eks:" + region + ":" + ACCOUNT + ":cluster/" + name);
        cluster.setCreatedAt(Instant.parse("2026-10-21T00:00:00Z"));
        cluster.setVersion("1.29");
        cluster.setStatus(ClusterStatus.CREATING);
        KubernetesNetworkConfig network = new KubernetesNetworkConfig();
        network.setServiceIpv4Cidr("10.100.0.0/16");
        cluster.setKubernetesNetworkConfig(network);
        cluster.setPodCidr("10.42.0.0/16");
        return cluster;
    }

    private static SmolvmNode service() {
        return new SmolvmNode(FlociNode.NAME, URI.create("http://127.0.0.1:6601"), NodeToken.file(Path.of("token")),
                SmolvmNode.Kind.CREATED, List.of());
    }

    /** Claims the node for {@code cluster} and creates it there, as {@code CreateCluster} does. */
    private boolean claimAndCreate(Cluster cluster) {
        smolvm.claim(cluster);
        return smolvm.create(cluster);
    }

    @Test
    void aSecondClusterIsRefusedWhileOneHoldsTheNode() {
        Cluster orders = cluster("orders");
        smolvm.claim(orders);
        smolvm.claim(cluster("orders"));

        AwsException refused = assertThrows(AwsException.class, () -> smolvm.claim(cluster("billing", "eu-west-1")));

        assertEquals("ResourceLimitExceededException", refused.getErrorCode());
        assertEquals(400, refused.getHttpStatus());
        assertEquals("Floci runs one EKS cluster per smolvm-pods node, and cluster orders (account " + ACCOUNT
                + ", region us-east-1) runs on it. Delete that cluster first, or set FLOCI_SERVICES_EKS_PROVIDER=k3s "
                + "to run each cluster in a k3s container instead.", refused.getMessage());

        smolvm.release(orders);
        assertDoesNotThrow(() -> smolvm.claim(cluster("billing", "eu-west-1")));
    }

    @Test
    void aClusterOnTheRuntimeFilesNodeIsRefused() {
        when(flociNode.status()).thenReturn(new Status(State.READY, new SmolvmNode("node:dev",
                URI.create("http://127.0.0.1:17070"), NodeToken.file(Path.of("token")), SmolvmNode.Kind.DECLARED,
                List.of()), null, null, true));

        AwsException refused = assertThrows(AwsException.class, () -> smolvm.claim(cluster("orders")));

        assertEquals("InvalidRequestException", refused.getErrorCode());
        assertThat(refused.getMessage(), containsString("node:dev, which Floci did not create"));
        assertEquals(Optional.empty(), smolvm.cluster());
    }

    @Test
    void aClusterInAnotherPartitionThanTheNodeIsRefused() {
        when(flociNode.status()).thenReturn(new Status(State.READY, service(), NODE, null, false));

        AwsException refused = assertThrows(AwsException.class, () -> smolvm.claim(cluster("orders", "cn-north-1")));

        assertEquals("InvalidParameterException", refused.getErrorCode());
        assertThat(refused.getMessage(), containsString("is in partition aws-cn (region cn-north-1)"));
        assertThat(refused.getMessage(), containsString("runs in partition aws (region eu-west-1)"));
        assertEquals(Optional.empty(), smolvm.cluster());
        assertDoesNotThrow(() -> smolvm.claim(cluster("orders", "ap-southeast-2")));
    }

    @Test
    void aNodeCreatedWithoutTheSeedOfEksIsRefused() {
        when(flociNode.status()).thenReturn(new Status(State.READY, service(), NODE.withSeeded(List.of()), null,
                false));

        AwsException refused = assertThrows(AwsException.class, () -> smolvm.claim(cluster("orders")));

        assertEquals("InvalidRequestException", refused.getErrorCode());
        assertThat(refused.getMessage(), containsString("was created while EKS clusters did not run on it"));
        assertThat(refused.getMessage(), containsString("node-vm.sh stop --name " + NODE.nodeId()));
        assertEquals(Optional.empty(), smolvm.cluster());
    }

    @Test
    void createUsesTheNodeAndTakesItsEndpointAndIssuer() {
        Cluster cluster = cluster("orders");
        cluster.setClusterArgs(List.of("--kubelet-arg=max-pods=250"));

        assertTrue(claimAndCreate(cluster));

        ArgumentCaptor<Use> use = ArgumentCaptor.forClass(Use.class);
        verify(flociNode).acquire(use.capture());
        assertEquals("EKS cluster orders", use.getValue().user());
        assertEquals(ACCOUNT, use.getValue().accountId());
        assertEquals(REGION, use.getValue().region());
        assertEquals("10.100.0.0/16", use.getValue().serviceCidr());
        assertEquals(Map.of(NodeSeed.KUBELET_ARGS, List.of("max-pods=250")), use.getValue().k3sArgs());
        assertEquals(EksSmolvmSettings.PROVIDER_SMOLVM_PODS, cluster.getProvider());
        assertEquals("1.35", cluster.getVersion());
        assertEquals(NODE.nodeId(), cluster.getSmolvmNodeId());
        assertEquals("https://localhost:6501", cluster.getEndpoint());
        String issuer = oidcService.nodeKey(NODE.accountId(), NODE.nodeId(), NODE.region()).getIssuer();
        assertEquals(issuer, cluster.getIdentity().getOidc().getIssuer());
        assertTrue(oidcService.findVerificationKey(issuer).isPresent(), "STS verifies tokens of the node's issuer");
        assertEquals(Optional.of(cluster), smolvm.cluster());
    }

    @Test
    void aClusterCreatedAfterRdsReportsTheRangesOfTheNodesK3s() {
        Cluster cluster = cluster("orders");
        cluster.getKubernetesNetworkConfig().setServiceIpv4Cidr("172.20.0.0/16");
        cluster.setPodCidr("10.244.0.0/16");

        claimAndCreate(cluster);

        assertEquals("10.100.0.0/16", cluster.getKubernetesNetworkConfig().getServiceIpv4Cidr());
        assertEquals("10.42.0.0/16", cluster.getPodCidr());
    }

    @Test
    void theClusterIsReadyWithTheNodeAndFailsWithIt() {
        Cluster cluster = cluster("orders");
        claimAndCreate(cluster);

        assertFalse(smolvm.isReady(cluster));
        assertEquals(Optional.empty(), smolvm.creationFailure(cluster));

        when(flociNode.status()).thenReturn(new Status(State.READY, service(), NODE, null, false));
        assertTrue(smolvm.isReady(cluster));
        assertFalse(smolvm.isReady(cluster("other")), "a cluster that does not hold the node");

        when(flociNode.status()).thenReturn(new Status(State.FAILED, null, NODE, "node-vm.sh create failed", false));
        assertFalse(smolvm.isReady(cluster));
        assertEquals(Optional.of("node-vm.sh create failed"), smolvm.creationFailure(cluster));
    }

    @Test
    void aRestoredSecondClusterFailsAndTheFirstKeepsTheNode() {
        Cluster first = cluster("orders");
        Cluster second = cluster("billing");
        smolvm.restore(first);

        smolvm.restore(second);

        assertThat(smolvm.creationFailure(second).orElseThrow(), containsString("one EKS cluster per smolvm-pods"));
        assertEquals(Optional.of(first), smolvm.cluster());
    }

    @Test
    void deleteRemovesTheClustersKubernetesObjectsAndKeepsTheNode() {
        Cluster cluster = cluster("orders");
        claimAndCreate(cluster);
        when(flociNode.status()).thenReturn(new Status(State.READY, service(), NODE, null, false));

        smolvm.delete(cluster);

        verify(kubeApi).removeWorkloads(NODE);
        verify(kubeApi).applyNodeMetadata(NODE, NODE.nodeName(), Map.of(), List.of());
        verify(flociNode, never()).clear();
        assertEquals(Optional.empty(), smolvm.cluster());
        assertDoesNotThrow(() -> smolvm.claim(cluster("billing")));
    }

    @Test
    void theClusterShipsItsEnabledControlPlaneLogsFromTheNodeAndStopsOnDelete() {
        Cluster cluster = cluster("orders");
        cluster.setLogging(new Logging(List.of(new LogSetup(List.of("api", "audit", "scheduler"), true),
                new LogSetup(List.of("authenticator"), false))));
        smolvm.claim(cluster);
        smolvm.create(cluster);
        SmolvmNode node = service();
        when(flociNode.node()).thenReturn(Optional.of(node));
        when(flociNode.status()).thenReturn(new Status(State.READY, node, NODE, null, false));

        smolvm.attachLogs(cluster);
        verify(logs).attach(ACCOUNT + "/" + REGION + "/orders", "orders", ACCOUNT, REGION, node, NODE.nodeId(),
                List.of("api", "audit"));

        cluster.setLogging(new Logging(List.of(new LogSetup(List.of("api"), false))));
        clusterManager.attachClusterLogs(cluster);
        verify(logs).attach(ACCOUNT + "/" + REGION + "/orders", "orders", ACCOUNT, REGION, node, NODE.nodeId(),
                List.of());

        smolvm.delete(cluster);
        verify(logs).close(ACCOUNT + "/" + REGION + "/orders");
    }

    @Test
    void aNodeThatDoesNotAnswerStillLetsTheClusterGo() {
        Cluster cluster = cluster("orders");
        claimAndCreate(cluster);
        when(flociNode.status()).thenReturn(new Status(State.READY, service(), NODE, null, false));
        when(kubeApi.removeWorkloads(NODE)).thenThrow(new KubeApiException("no answer", null));

        assertDoesNotThrow(() -> smolvm.delete(cluster));

        assertEquals(Optional.empty(), smolvm.cluster());
    }

    @Test
    void aClusterDeletedBeforeTheNodeIsReadyTouchesNoKubernetes() {
        Cluster cluster = cluster("orders");
        claimAndCreate(cluster);

        smolvm.delete(cluster);

        verify(kubeApi, never()).removeWorkloads(any());
        assertEquals(Optional.empty(), smolvm.cluster());
    }

    @Test
    void theClusterOnTheNodeFindsTheNodeByItsName() {
        Cluster cluster = cluster("orders");
        assertEquals(Map.of(), smolvm.aliases());

        claimAndCreate(cluster);

        assertEquals(Map.of("cluster:" + ACCOUNT + "/us-east-1/orders", FlociNode.NAME), smolvm.aliases());
        assertEquals(List.of(), smolvm.nodes());
        int changes = aliasChanges.get();
        smolvm.delete(cluster);
        assertEquals(Map.of(), smolvm.aliases());
        assertTrue(aliasChanges.get() > changes);
    }

    @Test
    void theClustersInstanceIsTheNodes() {
        Cluster cluster = cluster("orders");
        claimAndCreate(cluster);

        assertEquals(NODE.nodeName(), clusterManager.deriveClusterNodePrivateDnsName(cluster));
        assertEquals("i-0123456789abcdef0", clusterManager.deriveClusterNodeInstanceId(cluster));
    }

    @Test
    void nodeGroupTaintsBecomeKubernetesTaints() {
        Nodegroup group = new Nodegroup();
        group.setTaints(List.<Object>of(Map.of("key", "dedicated", "value", "gpu", "effect", "NO_SCHEDULE"),
                Map.of("key", "spot", "effect", "PREFER_NO_SCHEDULE"), "batch=true:NoExecute"));

        assertEquals(List.of(new Taint("dedicated", "gpu", "NoSchedule"), new Taint("spot", null, "PreferNoSchedule"),
                new Taint("batch", "true", "NoExecute")), EksSmolvmCluster.taints(group));
    }
}
