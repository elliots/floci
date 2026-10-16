package io.github.hectorvent.floci.runtime.ondemand;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.core.common.microsandbox.PodControllerClient;
import io.github.hectorvent.floci.core.common.microsandbox.PodControllerClient.Deployment;
import io.github.hectorvent.floci.core.common.microsandbox.PodControllerClient.Pod;
import io.github.hectorvent.floci.runtime.ondemand.ActivationCoordinator.State;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.BackendProtocol;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.RuntimeType;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/** Run against the owned Deployment kept alive by the native controller qualification script. */
@EnabledIfEnvironmentVariable(named = "FLOCI_NATIVE_POD_TEST", matches = "true")
class NativePodWorkloadIntegrationTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(180);

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void httpGatewayHibernatesAndResumesTheSameDeploymentAndPodIdentities() throws Exception {
        PodControllerClient controller = new PodControllerClient(required("FLOCI_NATIVE_POD_CONTROLLER_URL"),
                required("FLOCI_NATIVE_POD_CONTROLLER_TOKEN"), new ObjectMapper());
        String namespace = required("FLOCI_NATIVE_POD_NAMESPACE");
        String deploymentName = required("FLOCI_NATIVE_POD_DEPLOYMENT");
        URI backend = URI.create(required("FLOCI_NATIVE_POD_BACKEND_URL"));
        String httpPath = System.getenv().getOrDefault("FLOCI_NATIVE_POD_HTTP_PATH", "/");
        controller.checkAvailable(TIMEOUT);
        Deployment original = controller.deployment(namespace, deploymentName, TIMEOUT);
        if (Boolean.parseBoolean(System.getenv("FLOCI_NATIVE_POD_EXPECT_COLD"))) {
            assertTrue(original.pods().stream().allMatch(pod -> "COLD".equals(pod.state())),
                    "First gateway request must boot the untouched cold Deployment");
        }
        Map<String, String> identities = identities(original);
        WorkloadDefinition definition = new WorkloadDefinition("native-pods", true, "000000000000", "us-east-1",
                "native-pods.test", RuntimeType.KUBERNETES, deploymentName, namespace, original.replicas(), backend,
                backend.getPort() > 0 ? backend.getPort() : 80, Duration.ofSeconds(1), TIMEOUT,
                List.of(), List.of(), null, httpPath, 200, original.clusterName().isBlank() ? null : original.clusterName(),
                BackendProtocol.HTTP1);
        KubernetesWorkloadRuntime runtime = new KubernetesWorkloadRuntime(definition, controller);
        AtomicLong clock = new AtomicLong();
        Vertx vertx = Vertx.vertx();
        HttpClient http = vertx.createHttpClient();
        try (HttpReadiness readiness = new HttpReadiness();
             ActivationCoordinator coordinator = new ActivationCoordinator(Executors.newVirtualThreadPerTaskExecutor(),
                     readiness, 1, 16, 16, clock::get);
             OnDemandGateway gateway = new OnDemandGateway(vertx, List.of(definition), () -> coordinator)) {
            coordinator.register(definition, runtime);
            gateway.start("127.0.0.1", 0).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            coordinator.reconcile(definition.id(), false, true);
            String proxyUrl = System.getenv("FLOCI_NATIVE_POD_PROXY_URL");
            if (proxyUrl != null && !proxyUrl.isBlank()) {
                assertTrue(original.pods().stream().allMatch(pod -> pod.proxyAvailability()
                        && "external".equals(pod.idleOwner())));
                assertEquals(State.SLEEPING, coordinator.state(definition.id()));
                Reply direct = invokeProxy(http, URI.create(proxyUrl), httpPath);
                assertEquals(200, direct.status(), direct.body());
                assertEquals(State.SLEEPING, coordinator.state(definition.id()),
                        "The controller proxy can wake an app before Java observes the external activation");
                coordinator.reconcile(definition.id(), false, true);
                await().atMost(TIMEOUT).until(() -> coordinator.state(definition.id()) == State.READY);
            }
            Reply initial = invoke(http, gateway.port(), httpPath);
            assertEquals(200, initial.status(), initial.body());
            assertFalse(initial.body().isBlank());
            for (int cycle = 0; cycle < 2; cycle++) {
                await().atMost(TIMEOUT).until(() -> coordinator.state(definition.id()) == State.READY);
                clock.addAndGet(Duration.ofSeconds(2).toNanos());
                await().atMost(TIMEOUT).until(() -> {
                    coordinator.tick(definition.id());
                    return coordinator.state(definition.id()) == State.SLEEPING;
                });
                Deployment sleeping = controller.deployment(namespace, deploymentName, TIMEOUT);
                assertEquals(original.uid(), sleeping.uid());
                assertEquals(original.replicas(), sleeping.replicas());
                assertEquals(identities, identities(sleeping));
                assertTrue(sleeping.pods().stream().allMatch(pod -> "SLEEPING".equals(pod.state())));

                Reply resumed = invoke(http, gateway.port(), httpPath);
                assertEquals(200, resumed.status());
                assertEquals(initial.body(), resumed.body());
                Deployment ready = controller.deployment(namespace, deploymentName, TIMEOUT);
                assertEquals(original.uid(), ready.uid());
                assertEquals(original.generation(), ready.generation());
                assertEquals(original.replicas(), ready.replicas());
                assertEquals(identities, identities(ready));
                assertTrue(ready.pods().stream().allMatch(pod -> "READY".equals(pod.state())));
            }
        } finally {
            http.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    private static Reply invoke(HttpClient client, int port, String path) throws Exception {
        return client.request(HttpMethod.GET, port, "127.0.0.1", path)
                .compose(request -> request.putHeader("Host", "native-pods.test").send())
                .compose(response -> response.body().map(body -> new Reply(response.statusCode(), body.toString())))
                .toCompletionStage().toCompletableFuture().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    }

    private static Reply invokeProxy(HttpClient client, URI endpoint, String path) throws Exception {
        assertEquals("http", endpoint.getScheme(), "The native qualification fixture exposes plaintext HTTP");
        return client.request(HttpMethod.GET, endpoint.getPort() > 0 ? endpoint.getPort() : 80, endpoint.getHost(), path)
                .compose(request -> request.putHeader("Connection", "close").send())
                .compose(response -> response.body().map(body -> new Reply(response.statusCode(), body.toString())))
                .toCompletionStage().toCompletableFuture().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    }

    private static Map<String, String> identities(Deployment deployment) {
        return deployment.pods().stream().collect(Collectors.toMap(Pod::uid, Pod::name));
    }

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Native pod integration test requires " + name);
        }
        return value;
    }

    private record Reply(int status, String body) {}
}
