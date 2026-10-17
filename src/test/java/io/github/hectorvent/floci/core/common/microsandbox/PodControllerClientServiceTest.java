package io.github.hectorvent.floci.core.common.microsandbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.hectorvent.floci.core.common.microsandbox.PodControllerClient.Deployment;
import io.github.hectorvent.floci.core.common.microsandbox.PodControllerClient.Pod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class PodControllerClientServiceTest {
    private static final String TOKEN = "controller-token-at-least-thirty-two-characters";
    private static final Duration TIMEOUT = Duration.ofSeconds(2);
    private final ObjectMapper mapper = new ObjectMapper();
    private final List<String> calls = new CopyOnWriteArrayList<>();
    private final AtomicLong time = new AtomicLong(1);
    private HttpServer server;
    private PodControllerClient client;
    private volatile String deployment;
    private volatile String transition;
    private volatile String health;
    private volatile String batch;
    private volatile int batchStatus;
    private volatile int transitionStatus;
    private volatile JsonNode transitionBody;

    @BeforeEach
    void setup() throws IOException {
        deployment = """
                {"uid":"deployment-uid","namespace":"apps","name":"worker","clusterName":"dev",
                 "generation":7,"replicas":1,"pods":[{"uid":"pod-uid","namespace":"apps","name":"worker-pod",
                 "generation":4,"state":"SLEEPING"}]}
                """;
        transition = """
                {"uid":"pod-uid","namespace":"apps","name":"worker-pod","generation":5,"state":"READY"}
                """;
        health = "{\"status\":\"ok\",\"name\":\"microsandbox-pods\",\"contract\":1}";
        transitionStatus = 200;
        batchStatus = 200;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::respond);
        server.start();
        client = new PodControllerClient("http://127.0.0.1:" + server.getAddress().getPort(), TOKEN, mapper, time::get);
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private void respond(HttpExchange exchange) throws IOException {
        assertEquals("Bearer " + TOKEN, exchange.getRequestHeaders().getFirst("Authorization"));
        String route = exchange.getRequestURI().getPath();
        calls.add(exchange.getRequestMethod() + " " + route);
        String response;
        int status = 200;
        if (route.equals("/v1/health")) {
            response = health;
        } else if (route.equals("/v1/deployments")) {
            response = batch == null ? "{\"deployments\":[" + deployment + "]}" : batch;
            status = batchStatus;
        } else if (route.equals("/v1/deployments/apps/worker")) {
            response = deployment;
        } else {
            assertEquals("POST", exchange.getRequestMethod());
            transitionBody = mapper.readTree(exchange.getRequestBody());
            response = transition;
            status = transitionStatus;
        }
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    @Test
    void routesProxyHttpToTheSelectedPodAndKeepsStrictBackendSeparate() {
        deployment = deployment.replace("\"state\":\"SLEEPING\"", """
                "state":"SLEEPING","proxyAvailability":true,"idleOwner":"external","proxyProtocol":"http",
                "proxyUrl":"http://127.0.0.1:31001/","backendUrl":"http://127.0.0.1:31002"
                """);
        Pod proxy = client.deployment("apps", "worker", TIMEOUT).pods().getFirst();
        assertEquals(URI.create("http://127.0.0.1:31001/"), client.applicationEndpoint(proxy));
        PodControllerClient remote = new PodControllerClient("http://host.docker.internal:18187", TOKEN, mapper);
        assertEquals(URI.create("http://host.docker.internal:31001/"), remote.applicationEndpoint(proxy));
        deployment = deployment.replace("\"proxyAvailability\":true", "\"proxyAvailability\":false");
        Pod strict = client.deployment("apps", "worker", TIMEOUT).pods().getFirst();
        assertEquals(URI.create("http://127.0.0.1:31002"), client.applicationEndpoint(strict));
    }

    @Test
    void refusesOpaqueProxyAndUnknownOrForeignEndpointsWithoutExposingTheirContents() {
        String original = deployment;
        for (String protocol : List.of("tcp", "unknown", "")) {
            deployment = original.replace("\"state\":\"SLEEPING\"", """
                    "state":"SLEEPING","proxyAvailability":true,"idleOwner":"external","proxyProtocol":"%s",
                    "proxyUrl":"http://127.0.0.1:31001"
                    """.formatted(protocol));
            Pod observed = client.deployment("apps", "worker", TIMEOUT).pods().getFirst();
            assertThrows(IllegalStateException.class, () -> client.applicationEndpoint(observed));
        }
        deployment = original.replace("\"state\":\"SLEEPING\"", """
                "state":"SLEEPING","backendUrl":"http://another-host:31001"
                """);
        Pod foreign = client.deployment("apps", "worker", TIMEOUT).pods().getFirst();
        assertThrows(IllegalStateException.class, () -> client.applicationEndpoint(foreign));
        for (String address : List.of("http://secret:private@127.0.0.1:31001", "https://127.0.0.1:31001",
                "http://127.0.0.1:31001/path", "http://127.0.0.1:31001?secret=private", "http://127.0.0.1")) {
            deployment = original.replace("\"state\":\"SLEEPING\"", "\"state\":\"SLEEPING\",\"backendUrl\":\"" + address + "\"");
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> client.deployment("apps", "worker", TIMEOUT));
            assertFalse(failure.getMessage().contains("private"));
        }
        deployment = original;
        Pod legacy = client.deployment("apps", "worker", TIMEOUT).pods().getFirst();
        assertThrows(IllegalStateException.class, () -> client.applicationEndpoint(legacy));
    }

    @Test
    void discoversDeploymentAndUsesPodUidWithObservedGeneration() {
        client.checkAvailable(TIMEOUT);
        Deployment observed = client.deployment("apps", "worker", TIMEOUT);
        assertEquals(1, observed.replicas());
        assertEquals("dev", observed.clusterName());
        assertEquals("READY", client.resume(observed.pods().getFirst(), TIMEOUT).state());
        assertEquals(4, transitionBody.path("expectedGeneration").asLong());
        assertEquals(List.of("GET /v1/health", "GET /v1/deployments/apps/worker", "POST /v1/pods/pod-uid/resume"), calls);
    }

    @Test
    void generationConflictIsNotRetriedAndDoesNotExposeControllerBody() {
        Deployment observed = client.deployment("apps", "worker", TIMEOUT);
        transitionStatus = 409;
        transition = "{\"error\":\"private-application-secret\"}";
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> client.hibernate(observed.pods().getFirst(), TIMEOUT));
        assertFalse(error.getMessage().contains("private-application-secret"));
        assertEquals(2, calls.size());
        assertEquals("POST /v1/pods/pod-uid/hibernate", calls.getLast());
    }

    @Test
    void refusesIncompleteRolloutsAndForeignPodMembership() {
        deployment = deployment.replace("\"replicas\":1", "\"replicas\":2");
        assertThrows(IllegalStateException.class, () -> client.deployment("apps", "worker", TIMEOUT));
        deployment = deployment.replace("\"replicas\":2", "\"replicas\":0");
        assertThrows(IllegalStateException.class, () -> client.deployment("apps", "worker", TIMEOUT));
        deployment = deployment.replace("\"replicas\":0", "\"replicas\":1")
                .replace("\"uid\":\"pod-uid\",\"namespace\":\"apps\"", "\"uid\":\"pod-uid\",\"namespace\":\"other\"");
        assertThrows(IllegalStateException.class, () -> client.deployment("apps", "worker", TIMEOUT));
    }

    @Test
    void validatesContractAndPodIdentityAfterMutation() {
        health = health.replace("\"contract\":1", "\"contract\":2");
        assertThrows(IllegalStateException.class, () -> client.checkAvailable(TIMEOUT));
        Deployment observed = client.deployment("apps", "worker", TIMEOUT);
        transition = transition.replace("pod-uid", "replacement-uid");
        assertThrows(IllegalStateException.class, () -> client.resume(observed.pods().getFirst(), TIMEOUT));
    }

    @Test
    void recognizesCheckpointAndRestartTransitionsWithoutTreatingThemAsReady() {
        deployment = deployment.replace("\"state\":\"SLEEPING\"", "\"state\":\"CHECKPOINTED\"");
        assertEquals("CHECKPOINTED", client.deployment("apps", "worker", TIMEOUT).pods().getFirst().state());
        deployment = deployment.replace("\"state\":\"CHECKPOINTED\"", "\"state\":\"RESTARTING\"");
        assertEquals("RESTARTING", client.deployment("apps", "worker", TIMEOUT).pods().getFirst().state());
    }

    @Test
    void recognizesInitTransitionsWithoutTreatingThemAsReady() {
        deployment = deployment.replace("\"state\":\"SLEEPING\"", "\"state\":\"INITSTARTING\"");
        assertEquals("INITSTARTING", client.deployment("apps", "worker", TIMEOUT).pods().getFirst().state());
        deployment = deployment.replace("\"state\":\"INITSTARTING\"", "\"state\":\"INITIALIZING\"");
        assertEquals("INITIALIZING", client.deployment("apps", "worker", TIMEOUT).pods().getFirst().state());
    }

    @Test
    void discoversColdPodWithoutStartingIt() {
        deployment = deployment.replace("\"state\":\"SLEEPING\"", "\"state\":\"COLD\"");
        assertEquals("COLD", client.deployment("apps", "worker", TIMEOUT).pods().getFirst().state());
        assertEquals(List.of("GET /v1/deployments/apps/worker"), calls);
    }

    @Test
    void discoversPodsWaitingForVolumesWithoutStartingThem() {
        deployment = deployment.replace("\"state\":\"SLEEPING\"", "\"state\":\"WAITINGVOLUMES\"");
        Pod observed = client.observedDeployment("apps", "worker", TIMEOUT).pods().getFirst();
        assertEquals("WAITINGVOLUMES", observed.state());
        assertEquals("pod-uid", observed.uid());
        assertEquals(4, observed.generation());
        assertEquals("WAITINGVOLUMES", client.deployment("apps", "worker", TIMEOUT).pods().getFirst().state());
        assertEquals(List.of("GET /v1/deployments", "GET /v1/deployments/apps/worker"), calls);
    }

    @Test
    void sharesOneControllerObservationAcrossFiveHundredWorkloads() {
        StringBuilder document = new StringBuilder("{\"deployments\":[");
        for (int index = 0; index < 500; index++) {
            if (index > 0) {
                document.append(',');
            }
            document.append(deployment.replace("deployment-uid", "deployment-" + index)
                    .replace("pod-uid", "pod-" + index).replace("worker", "worker-" + index));
        }
        batch = document.append("]}").toString();
        for (int index = 0; index < 500; index++) {
            assertEquals("SLEEPING", client.observedDeployment("apps", "worker-" + index, TIMEOUT)
                    .pods().getFirst().state());
        }
        assertEquals(List.of("GET /v1/deployments"), calls);
        time.addAndGet(Duration.ofSeconds(1).toNanos());
        client.observedDeployment("apps", "worker-0", TIMEOUT);
        assertEquals(2, calls.size());
    }

    @Test
    void invalidatesObservationsAfterLifecycleChangesAndRefusesExpiredDataOnFailure() {
        Deployment observed = client.observedDeployment("apps", "worker", TIMEOUT);
        client.resume(observed.pods().getFirst(), TIMEOUT);
        deployment = deployment.replace("SLEEPING", "READY");
        assertEquals("READY", client.observedDeployment("apps", "worker", TIMEOUT).pods().getFirst().state());
        assertEquals(3, calls.size());
        time.addAndGet(Duration.ofSeconds(1).toNanos());
        batchStatus = 503;
        assertThrows(IllegalStateException.class, () -> client.observedDeployment("apps", "worker", TIMEOUT));
    }

    @Test
    void fiveHundredObserversShareAFailedRefreshAndRecoverAfterItsRetryInterval() {
        client.observedDeployment("apps", "worker", TIMEOUT);
        time.addAndGet(Duration.ofSeconds(1).toNanos());
        batchStatus = 503;
        for (int index = 0; index < 500; index++) {
            assertThrows(IllegalStateException.class, () -> client.observedDeployment("apps", "worker", TIMEOUT));
        }
        assertEquals(2, calls.size(), "An outage must not make every workload retry the same failed batch");
        time.addAndGet(Duration.ofSeconds(1).toNanos());
        batchStatus = 200;
        deployment = deployment.replace("SLEEPING", "READY");
        assertEquals("READY", client.observedDeployment("apps", "worker", TIMEOUT).pods().getFirst().state());
        assertEquals(3, calls.size());
    }

    @Test
    void oneUnavailableDeploymentDoesNotHideOtherCachedObservations() {
        batch = "{\"deployments\":[" + deployment
                + ", {\"namespace\":\"apps\",\"name\":\"changing\",\"error\":\"private-detail\"}]}";
        assertEquals("worker", client.observedDeployment("apps", "worker", TIMEOUT).name());
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> client.observedDeployment("apps", "changing", TIMEOUT));
        assertFalse(error.getMessage().contains("private-detail"));
        assertEquals(1, calls.size());
    }

    @Test
    void supportsEarlierControllersWithoutTheBatchObservationEndpoint() {
        batchStatus = 404;
        assertEquals("worker", client.observedDeployment("apps", "worker", TIMEOUT).name());
        client.observedDeployment("apps", "worker", TIMEOUT);
        assertEquals(List.of("GET /v1/deployments", "GET /v1/deployments/apps/worker",
                "GET /v1/deployments/apps/worker"), calls);
    }

    @Test
    void readsProxyActivitySeparatelyFromApplicationReadinessAndRejectsInvalidOwnership() {
        deployment = deployment.replace("\"state\":\"SLEEPING\"",
                "\"state\":\"COLD\",\"proxyAvailability\":true,\"idleOwner\":\"external\",\"activeConnections\":2,\"activeRequests\":3,\"activitySequence\":9");
        Pod observed = client.deployment("apps", "worker", TIMEOUT).pods().getFirst();
        assertEquals("COLD", observed.state());
        assertTrue(observed.proxyAvailability());
        assertEquals(2, observed.activeConnections());
        assertEquals(3, observed.activeRequests());
        assertEquals(9, observed.activitySequence());
        assertTrue(observed.activitySequenceKnown());
        deployment = deployment.replace("\"idleOwner\":\"external\"", "\"idleOwner\":\"unknown\"");
        assertThrows(IllegalStateException.class, () -> client.deployment("apps", "worker", TIMEOUT));
    }

    @Test
    void earlierControllersKeepLegacyConnectionActivityWithoutRequestCounts() {
        deployment = deployment.replace("\"state\":\"SLEEPING\"",
                "\"state\":\"READY\",\"proxyAvailability\":true,\"idleOwner\":\"external\",\"activeConnections\":2");
        Pod observed = client.deployment("apps", "worker", TIMEOUT).pods().getFirst();
        assertEquals(2, observed.activeConnections());
        assertEquals(0, observed.activeRequests());
        assertEquals(0, observed.activitySequence());
        assertFalse(observed.activitySequenceKnown());
        client.hibernate(observed, TIMEOUT);
        assertFalse(transitionBody.has("expectedActivitySequence"), "Earlier controllers reject unknown request fields");
    }

    @Test
    void hibernateUsesTheObservedActivitySequenceIncludingZero() {
        String original = deployment;
        for (long sequence : List.of(0L, 9L)) {
            deployment = original.replace("\"state\":\"SLEEPING\"", "\"state\":\"READY\",\"activitySequence\":" + sequence);
            Pod observed = client.deployment("apps", "worker", TIMEOUT).pods().getFirst();
            client.hibernate(observed, TIMEOUT);
            assertTrue(transitionBody.has("expectedActivitySequence"));
            assertEquals(sequence, transitionBody.path("expectedActivitySequence").asLong());
            assertEquals(observed.generation(), transitionBody.path("expectedGeneration").asLong());
            client.resume(observed, TIMEOUT);
            assertFalse(transitionBody.has("expectedActivitySequence"));
        }
    }

    @Test
    void rejectsInvalidRequestCountsInsteadOfTreatingUnknownActivityAsIdle() {
        String original = deployment;
        for (String value : List.of("-1", "1.5", "\"1\"", "true", "null", "2147483648")) {
            deployment = original.replace("\"state\":\"SLEEPING\"", "\"state\":\"READY\",\"activeRequests\":" + value);
            assertThrows(IllegalStateException.class, () -> client.deployment("apps", "worker", TIMEOUT), value);
        }
    }

    @Test
    void rejectsInvalidActivitySequencesInsteadOfLosingCompletedRequests() {
        String original = deployment;
        for (String value : List.of("-1", "1.5", "\"1\"", "true", "null", "9223372036854775808")) {
            deployment = original.replace("\"state\":\"SLEEPING\"", "\"state\":\"READY\",\"activitySequence\":" + value);
            assertThrows(IllegalStateException.class, () -> client.deployment("apps", "worker", TIMEOUT), value);
        }
    }
}
