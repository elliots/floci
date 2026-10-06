package io.github.hectorvent.floci.services.ec2;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.ExposedPort;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerSpec;
import io.github.hectorvent.floci.runtime.ondemand.ActivationCoordinator;
import io.github.hectorvent.floci.runtime.ondemand.ActivationCoordinator.RequestLease;
import io.github.hectorvent.floci.runtime.ondemand.ActivationCoordinator.State;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadConfigLoader;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadRuntime;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadRuntimeFactory;
import io.github.hectorvent.floci.services.ec2.model.Instance;
import io.github.hectorvent.floci.services.ec2.model.InstanceState;
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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(OnDemandEc2DockerIntegrationTest.Profile.class)
class OnDemandEc2DockerIntegrationTest {
    private static final Logger LOG = Logger.getLogger(OnDemandEc2DockerIntegrationTest.class);
    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("floci.services.ec2.mock", "false",
                    "floci.network.security-group-enforcement.enabled", "false",
                    "floci.docker.resource-namespace", "on-demand-ec2-test");
        }
    }

    @Inject
    DockerClient docker;

    @Inject
    ContainerBuilder containers;

    @Inject
    ContainerLifecycleManager lifecycle;

    @Inject
    Ec2Service ec2;

    @Inject
    WorkloadRuntimeFactory factory;

    private String instanceId;
    private String containerId;

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
        if (instanceId != null) {
            ec2.deleteInstanceForTest("us-east-1", instanceId);
        }
        if (containerId != null) {
            try {
                docker.removeContainerCmd(containerId).withForce(true).exec();
            } catch (RuntimeException e) {
                LOG.warnv(e, "Failed to clean up on-demand EC2 test container {0}", containerId);
            }
        }
    }

    @Test
    void idleStopAndReactivationRestartTheRealContainerAndPreserveItsFilesystem(@TempDir Path directory) throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        instanceId = "i-" + suffix;
        String script = """
                from http.server import BaseHTTPRequestHandler, HTTPServer
                from pathlib import Path
                import signal, sys
                signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(0))
                marker = Path('/tmp/boots')
                boots = int(marker.read_text()) + 1 if marker.exists() else 1
                marker.write_text(str(boots))
                class Handler(BaseHTTPRequestHandler):
                    def do_GET(self):
                        self.send_response(200)
                        self.end_headers()
                        self.wfile.write(str(boots).encode())
                HTTPServer(('0.0.0.0', 8080), Handler).serve_forever()
                """;
        ContainerSpec spec = containers.newContainer("python:3.11-alpine")
                .withName("floci-on-demand-smoke-" + suffix)
                .withEntrypoint(List.of("python", "-u", "-c", script))
                .withLoopbackPortBinding(8080, 0).build();
        containerId = lifecycle.createAndStart(spec).containerId();
        try (HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            String port = docker.inspectContainerCmd(containerId).exec().getNetworkSettings().getPorts()
                    .getBindings().get(ExposedPort.tcp(8080))[0].getHostPortSpec();
            URI backend = URI.create("http://127.0.0.1:" + port);
            await().ignoreExceptions().atMost(Duration.ofSeconds(10)).until(() -> get(http, backend).equals("1"));
            Instance instance = new Instance();
            instance.setInstanceId(instanceId);
            instance.setRegion("us-east-1");
            instance.setDockerContainerId(containerId);
            instance.setState(InstanceState.running());
            ec2.putInstanceForTest(instance);
            Path file = directory.resolve("workloads.yaml");
            Files.writeString(file, """
                    workloads:
                      smoke:
                        host: smoke.test
                        runtime:
                          type: ec2
                          instance-id: %s
                          backend-url: %s
                    """.formatted(instanceId, backend));
            WorkloadDefinition definition = WorkloadConfigLoader.load(file, "000000000000", "us-east-1").getFirst();
            WorkloadRuntime runtime = factory.create(definition);
            runtime.stop(Duration.ofSeconds(40));
            AtomicLong time = new AtomicLong();
            try (ActivationCoordinator coordinator = new ActivationCoordinator(Executors.newVirtualThreadPerTaskExecutor(),
                    (workload, origin, timeout) -> await().ignoreExceptions().atMost(timeout)
                            .until(() -> Integer.parseInt(get(http, origin.resolve(workload.readinessPath()))) >= 2),
                    1, 16, 16, time::get)) {
                coordinator.register(definition, runtime);
                coordinator.reconcile("smoke", false, true);
                assertEquals(State.SLEEPING, coordinator.state("smoke"));
                try (RequestLease request = coordinator.acquire("smoke")) {
                    URI ready = request.ready().get(40, TimeUnit.SECONDS);
                    assertEquals("2", get(http, ready));
                    assertEquals("running", instance.getState().getName());
                    assertNotNull(instance.getContainerBridgeIp());
                }
                time.addAndGet(Duration.ofSeconds(121).toNanos());
                coordinator.tick("smoke");
                await().atMost(Duration.ofSeconds(40)).until(() -> coordinator.state("smoke") == State.SLEEPING);
                assertEquals("stopped", instance.getState().getName());
                assertFalse(docker.inspectContainerCmd(containerId).exec().getState().getRunning());
                try (RequestLease request = coordinator.acquire("smoke")) {
                    assertEquals("3", get(http, request.ready().get(40, TimeUnit.SECONDS)));
                }
                assertEquals("running", instance.getState().getName());
                assertEquals(containerId, instance.getDockerContainerId());
            }
        }
    }

    private static String get(HttpClient client, URI uri) throws Exception {
        return client.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(2)).GET().build(),
                HttpResponse.BodyHandlers.ofString()).body();
    }
}
