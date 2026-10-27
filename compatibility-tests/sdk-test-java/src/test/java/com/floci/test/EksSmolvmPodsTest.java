package com.floci.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.services.eks.EksClient;
import software.amazon.awssdk.services.eks.model.Cluster;
import software.amazon.awssdk.services.eks.model.ClusterStatus;
import software.amazon.awssdk.services.eks.model.ResourceInUseException;
import software.amazon.awssdk.services.eks.model.ResourceNotFoundException;
import software.amazon.awssdk.services.eks.model.VpcConfigRequest;
import software.amazon.awssdk.services.eks.waiters.EksWaiter;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * EKS clusters on the smolvm-pods node, against a Floci started natively on an Apple silicon Mac
 * with {@code FLOCI_SERVICES_EKS_PROVIDER=smolvm-pods} and the {@code FLOCI_RUNTIME_NODE_*}
 * settings. Runs only with {@code FLOCI_EKS_SMOLVM_E2E=1}.
 */
@DisplayName("EKS on smolvm-pods nodes")
@EnabledIfEnvironmentVariable(named = "FLOCI_EKS_SMOLVM_E2E", matches = "1")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class EksSmolvmPodsTest {

    private static EksClient eks;
    private static EksWaiter waiter;
    private static String clusterName;

    @BeforeAll
    static void setup() {
        eks = TestFixtures.eksClient();
        waiter = EksWaiter.builder().client(eks)
                .overrideConfiguration(override -> override.waitTimeout(Duration.ofMinutes(10))
                        .backoffStrategyV2(BackoffStrategy.fixedDelay(Duration.ofSeconds(2))))
                .build();
        clusterName = TestFixtures.uniqueName("smolvm");
    }

    @AfterAll
    static void cleanup() {
        try {
            eks.deleteCluster(request -> request.name(clusterName));
        } catch (ResourceNotFoundException expected) {
            // The last test deleted it.
        }
        eks.close();
    }

    @Test
    @Order(1)
    @DisplayName("CreateCluster answers CREATING with the node's Kubernetes API endpoint")
    void createCluster() {
        Cluster cluster = eks.createCluster(request -> request.name(clusterName)
                .roleArn(TestFixtures.globalArn("iam", "000000000000", "role/eks-role"))
                .resourcesVpcConfig(VpcConfigRequest.builder().build())).cluster();

        assertThat(cluster.status()).isEqualTo(ClusterStatus.CREATING);
        assertThat(cluster.endpoint()).startsWith("https://localhost:");
        assertThat(cluster.version()).matches("1\\.\\d+");
    }

    @Test
    @Order(2)
    @DisplayName("The cluster becomes ACTIVE with the node's certificate authority")
    void clusterBecomesActive() {
        Cluster cluster = waiter.waitUntilClusterActive(request -> request.name(clusterName)).matched().response()
                .orElseThrow().cluster();

        assertThat(cluster.status()).isEqualTo(ClusterStatus.ACTIVE);
        String ca = new String(Base64.getDecoder().decode(cluster.certificateAuthority().data()),
                StandardCharsets.US_ASCII);
        assertThat(ca).startsWith("-----BEGIN CERTIFICATE-----");
        assertThat(cluster.identity().oidc().issuer()).startsWith("https://");
    }

    @Test
    @Order(3)
    @DisplayName("A second CreateCluster with the same name is refused")
    void createDuplicateClusterFails() {
        assertThatThrownBy(() -> eks.createCluster(request -> request.name(clusterName)
                        .roleArn(TestFixtures.globalArn("iam", "000000000000", "role/eks-role"))
                        .resourcesVpcConfig(VpcConfigRequest.builder().build())))
                .isInstanceOf(ResourceInUseException.class);
    }

    @Test
    @Order(4)
    @DisplayName("DeleteCluster removes the cluster and its node")
    void deleteCluster() {
        eks.deleteCluster(request -> request.name(clusterName));
        waiter.waitUntilClusterDeleted(request -> request.name(clusterName));

        assertThatThrownBy(() -> eks.describeCluster(request -> request.name(clusterName)))
                .isInstanceOf(ResourceNotFoundException.class);
    }
}
