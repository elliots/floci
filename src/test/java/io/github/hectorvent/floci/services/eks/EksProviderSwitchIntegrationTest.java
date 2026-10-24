package io.github.hectorvent.floci.services.eks;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.PersistentStorage;
import io.github.hectorvent.floci.runtime.flocinode.FakeFlociNodes;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeDirectory;
import io.github.hectorvent.floci.services.eks.model.Cluster;
import io.github.hectorvent.floci.services.eks.model.ClusterIdentity;
import io.github.hectorvent.floci.services.eks.model.ClusterStatus;
import io.github.hectorvent.floci.services.eks.model.OidcIdentity;
import io.github.hectorvent.floci.testing.DockerAvailability;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eks.EksClient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A Floci whose provider changed from {@code k3s} to {@code smolvm-pods}: the cluster it persisted
 * as a k3s container comes back as one, and a new cluster gets a smolvm-pods node. The k3s cluster
 * needs Docker; the smolvm-pods node runs on the fakes of {@link SmolvmPodsProfile}.
 */
@QuarkusTest
@TestProfile(EksProviderSwitchIntegrationTest.SwitchedProvider.class)
class EksProviderSwitchIntegrationTest {

    private static final String ACCOUNT = "000000000000";
    private static final String REGION = "us-east-1";
    private static final String K3S_CLUSTER = "persisted-k3s";
    private static final String K3S_VERSION = "1.35";
    private static final String ACTIVE = "ACTIVE";

    /**
     * {@link SmolvmPodsProfile} with persistent storage that already holds {@link #K3S_CLUSTER}, as a
     * Floci on the {@code k3s} provider left it, and a resource namespace of its own for the k3s
     * container, which DeleteCluster or the shutdown removes.
     */
    public static class SwitchedProvider extends SmolvmPodsProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> overrides = overrides();
            overrides.put("floci.storage.mode", "persistent");
            overrides.put("floci.storage.persistent-path", persistedK3sCluster().toString());
            overrides.put("floci.docker.resource-namespace", "floci-eks-switch-it");
            overrides.put("floci.services.eks.keep-running-on-shutdown", "false");
            // Without the mirror the k3s cluster starts no ECR registry container.
            overrides.put("floci.services.eks.ecr-registry-mirror", "false");
            return overrides;
        }

        private static Path persistedK3sCluster() {
            try {
                Path data = Files.createTempDirectory("floci-eks-switch-");
                AccountAwareStorageBackend<Cluster> clusters = new AccountAwareStorageBackend<>(
                        new PersistentStorage<>(data.resolve("eks-clusters.json"),
                                new TypeReference<Map<String, Cluster>>() {
                                }), null, ACCOUNT);
                Cluster cluster = new Cluster();
                cluster.setName(K3S_CLUSTER);
                cluster.setArn("arn:aws:eks:" + REGION + ":" + ACCOUNT + ":cluster/" + K3S_CLUSTER);
                cluster.setRoleArn("arn:aws:iam::" + ACCOUNT + ":role/eks-cluster");
                cluster.setCreatedAt(Instant.now());
                cluster.setVersion(K3S_VERSION);
                cluster.setExplicitVersion(true);
                cluster.setStatus(ClusterStatus.ACTIVE);
                cluster.setPlatformVersion("eks.1");
                cluster.setIdentity(new ClusterIdentity(new OidcIdentity("https://oidc.eks." + REGION
                        + ".amazonaws.com/id/" + UUID.randomUUID().toString().replace("-", "").toUpperCase())));
                clusters.putForAccount(ACCOUNT, K3S_CLUSTER, cluster);
                clusters.flush();
                return data;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @TestHTTPResource
    URI endpoint;

    @Inject
    EksService eksService;

    @Inject
    FakeFlociNodes driver;

    @Inject
    SmolvmNodeDirectory directory;

    private EksClient client() {
        return EksClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder()).build();
    }

    private Cluster record(String name) {
        return eksService.findAuthenticationCluster(ACCOUNT, name).orElseThrow();
    }

    @Test
    void aPersistedK3sClusterStaysK3sAndANewClusterGetsASmolvmPodsNode() throws Exception {
        Assumptions.assumeTrue(DockerAvailability.isAvailable(), "the persisted k3s cluster needs Docker");
        String smolvmCluster = "switched-" + UUID.randomUUID().toString().substring(0, 8);
        try (EksClient eks = client()) {
            try {
                await().atMost(3, TimeUnit.MINUTES).pollInterval(Duration.ofSeconds(2)).until(() ->
                        eks.describeCluster(request -> request.name(K3S_CLUSTER)).cluster().statusAsString()
                                .equals(ACTIVE));
                Cluster k3s = record(K3S_CLUSTER);
                assertNull(k3s.getProvider());
                assertNull(k3s.getSmolvmNodeId());
                assertNotNull(k3s.getContainerId(), "the k3s cluster runs in a container again");
                assertEquals(K3S_VERSION, eks.describeCluster(request -> request.name(K3S_CLUSTER)).cluster()
                        .version());

                eks.createCluster(request -> request.name(smolvmCluster)
                        .roleArn("arn:aws:iam::" + ACCOUNT + ":role/eks-cluster"));
                await().atMost(30, TimeUnit.SECONDS).pollInterval(Duration.ofMillis(250)).until(() ->
                        eks.describeCluster(request -> request.name(smolvmCluster)).cluster().statusAsString()
                                .equals(ACTIVE));
                Cluster created = record(smolvmCluster);
                assertEquals(EksSmolvmSettings.PROVIDER_SMOLVM_PODS, created.getProvider());
                assertNotNull(created.getSmolvmNodeId());
                assertNull(created.getContainerId());
                assertEquals(1, driver.calls("create", created.getSmolvmNodeId()).size());
                assertTrue(directory.find(FlociNode.NAME).isPresent());
                assertEquals(ACTIVE, eks.describeCluster(request -> request.name(K3S_CLUSTER)).cluster()
                        .statusAsString());
            } finally {
                for (String name : new String[]{smolvmCluster, K3S_CLUSTER}) {
                    if (eks.listClusters().clusters().contains(name)) {
                        eks.deleteCluster(request -> request.name(name));
                    }
                }
            }
        }
    }
}
