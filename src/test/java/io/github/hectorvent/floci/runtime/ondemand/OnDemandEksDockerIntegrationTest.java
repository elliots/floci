package io.github.hectorvent.floci.runtime.ondemand;

import com.github.dockerjava.api.DockerClient;
import io.github.hectorvent.floci.core.common.docker.ContainerExec;
import io.github.hectorvent.floci.services.eks.EksService;
import io.github.hectorvent.floci.services.eks.model.Cluster;
import io.github.hectorvent.floci.services.eks.model.ClusterStatus;
import io.github.hectorvent.floci.services.eks.model.CreateClusterRequest;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(OnDemandEksDockerIntegrationTest.Profile.class)
class OnDemandEksDockerIntegrationTest {
    private static final Logger LOG = Logger.getLogger(OnDemandEksDockerIntegrationTest.class);
    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("floci.services.eks.mock", "false",
                    "floci.services.eks.default-image", "rancher/k3s:v1.35.0-k3s1",
                    "floci.network.security-group-enforcement.enabled", "false",
                    "floci.docker.resource-namespace", "on-demand-eks-test");
        }
    }

    @Inject
    DockerClient docker;

    @Inject
    EksService eks;

    @Inject
    WorkloadRuntimeFactory factory;

    private Cluster cluster;

    @BeforeEach
    void requireDocker() {
        boolean available;
        try {
            docker.pingCmd().exec();
            available = true;
        } catch (RuntimeException e) {
            available = false;
            LOG.warnv("Docker daemon unavailable for on-demand runtime test: {0}", e.getMessage());
        }
        Assumptions.assumeTrue(available, "Docker daemon must be available for on-demand runtime integration tests");
    }

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            try {
                eks.deleteCluster(cluster.getName());
            } catch (RuntimeException e) {
                LOG.warnv(e, "Failed to clean up on-demand EKS test cluster {0}", cluster.getName());
            }
        }
    }

    @Test
    void namedFlociClusterScalesDeploymentsUsingItsOwnCredentials(@TempDir Path directory) throws Exception {
        String name = "on-demand-" + UUID.randomUUID().toString().substring(0, 8);
        CreateClusterRequest request = new CreateClusterRequest();
        request.setName(name);
        request.setRoleArn("arn:aws:iam::000000000000:role/on-demand-test");
        cluster = eks.createCluster(request);
        assertNotNull(cluster.getContainerId(), "EKS must start a real k3s container");
        await().atMost(Duration.ofMinutes(3)).until(() -> cluster.getStatus() == ClusterStatus.ACTIVE);
        String manifest = """
                apiVersion: apps/v1
                kind: Deployment
                metadata:
                  name: worker
                spec:
                  replicas: 0
                  selector:
                    matchLabels: {app: worker}
                  template:
                    metadata:
                      labels: {app: worker}
                    spec:
                      containers:
                        - name: worker
                          image: alpine:3.22.1
                          command: [sleep, '3600']
                """;
        ContainerExec.Result applied = ContainerExec.runMerged(docker, cluster.getContainerId(),
                new String[]{"sh", "-c", "cat <<'FLOCI_MANIFEST' | kubectl apply -f -\n"
                        + manifest + "FLOCI_MANIFEST\n"}, 30);
        assertEquals(0, applied.exitCode(), applied.summary());
        Path file = directory.resolve("on-demand.yaml");
        Files.writeString(file, """
                workloads:
                  worker:
                    queues: [events]
                    runtime:
                      type: kubernetes
                      cluster-name: %s
                      deployment: worker
                """.formatted(name));
        WorkloadRuntime runtime = factory.create(OnDemandConfigLoader.load(file, "000000000000", "us-east-1").getFirst());
        assertFalse(runtime.isRunning());
        runtime.start(Duration.ofMinutes(2));
        runtime.awaitReady(Duration.ofMinutes(2));
        assertTrue(runtime.isRunning());
        runtime.stop(Duration.ofMinutes(1));
        assertFalse(runtime.isRunning());
        assertTrue(docker.inspectContainerCmd(cluster.getContainerId()).exec().getState().getRunning(),
                "Scaling workloads must keep the Kubernetes control plane running");
    }
}
