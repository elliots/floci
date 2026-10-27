package io.github.hectorvent.floci.services.lambda;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.github.hectorvent.floci.services.eks.FlociProcess;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A Node.js function as a hibernating microVM on a real, isolated smolvm-pods node: the test
 * creates the node with {@code node-vm.sh create --isolate}, declares it in Floci's runtime file
 * with a route of {@code https://api.vendor.test} to a simulator on the Mac, and runs Floci as a
 * child process ({@link FlociProcess}) with {@code FLOCI_SERVICES_LAMBDA_SMOLVM_ENABLED}, which it
 * calls through Lambda's REST API. The
 * function resolves Floci by name, calls STS through the AWS SDK, calls the routed vendor over
 * HTTPS with Floci's CA, and cannot resolve a public name. The test waits until the node puts the
 * environment to sleep, invokes again, which wakes it with its memory, and deletes the function.
 * It needs an Apple silicon Mac of M3 or later, the node image of {@code node-vm.sh build}, and
 * Docker with {@code public.ecr.aws/lambda/nodejs:20}, and runs only with
 * {@code FLOCI_LAMBDA_SMOLVM_E2E=1}:
 *
 * <pre>
 * ./mvnw package -DskipTests
 * SMOLVM_SRC=/path/to/smolvm FLOCI_LAMBDA_SMOLVM_E2E=1 ./mvnw test -Dtest=LambdaSmolvmNodeIntegrationTest
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "FLOCI_LAMBDA_SMOLVM_E2E", matches = "1")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Timeout(value = 20, unit = TimeUnit.MINUTES)
class LambdaSmolvmNodeIntegrationTest {

    private static final Logger LOG = Logger.getLogger(LambdaSmolvmNodeIntegrationTest.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String FUNCTION = "smolvm-e2e-fn";
    /** The node's shortest idle timeout; the node reads conntrack every 15 s on top. */
    private static final String IDLE_TIMEOUT = "10s";
    private static final Duration COMMAND_TIMEOUT = Duration.ofMinutes(6);
    private static final String HANDLER = """
            import dns from 'node:dns/promises';
            import http from 'node:http';
            import https from 'node:https';
            import { STSClient, GetCallerIdentityCommand } from '@aws-sdk/client-sts';

            let count = 0;

            const get = (url) => new Promise((resolve, reject) => {
              (url.startsWith('https:') ? https : http).get(url, (res) => {
                let body = '';
                res.on('data', (chunk) => { body += chunk; });
                res.on('end', () => resolve({ status: res.statusCode, body }));
              }).on('error', reject);
            });

            const attempt = async (work) => {
              try { return await work(); } catch (e) { return { error: e.code || e.name || String(e) }; }
            };

            export const handler = async () => {
              count += 1;
              return {
                count,
                floci: await attempt(async () => (await dns.lookup('localhost.floci.io')).address),
                health: await attempt(async () => (await get(process.env.AWS_ENDPOINT_URL + '/_floci/health')).status),
                account: await attempt(async () => (await new STSClient({}).send(new GetCallerIdentityCommand({}))).Account),
                vendor: await attempt(() => get('https://api.vendor.test/hello')),
                outside: await attempt(async () => (await dns.lookup('example.com')).address),
              };
            };
            """;

    private final Path moduleDir = Path.of("").toAbsolutePath();
    private final String runId = UUID.randomUUID().toString().substring(0, 6);
    private final String nodeName = "floci-lambda-e2e-" + runId;
    private final Path work = moduleDir.resolve("target/lambda-smolvm-e2e").resolve(runId);
    private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    private Path nodeVmScript;
    private String smolvmSrc;
    private boolean nodeCreated;
    private URI nodeApi;
    private String token;
    private HttpServer simulator;
    private FlociProcess floci;

    @BeforeAll
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void startNodeSimulatorAndFloci() throws IOException {
        Files.createDirectories(work);
        Path pods = moduleDir.resolve("tools/smolvm-pods");
        nodeVmScript = pods.resolve("scripts/node-vm.sh");
        smolvmSrc = firstSet(System.getenv("SMOLVM_SRC"), moduleDir.resolveSibling("smolvm").toString());
        assertTrue(Files.isDirectory(Path.of(smolvmSrc)), "No smolvm checkout at " + smolvmSrc + ": export SMOLVM_SRC");
        Path image = Path.of(firstSet(System.getenv("FLOCI_LAMBDA_SMOLVM_E2E_IMAGE"),
                pods.resolve("target/node/node-image.tar").toString()));
        assertTrue(Files.isRegularFile(image), "No node image at " + image + ": run node-vm.sh build");

        simulator = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        simulator.createContext("/", exchange -> {
            byte[] body = ("hello from the simulator for " + exchange.getRequestHeaders().getFirst("Host"))
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        simulator.start();

        int servicePort = freePort();
        LOG.infov("Creating isolated smolvm-pods node {0} with its API on port {1}", nodeName,
                String.valueOf(servicePort));
        nodeCreated = true;
        nodeVm("create", "--name", nodeName, "--image", image.toString(), "--service-port",
                String.valueOf(servicePort), "--api-port", String.valueOf(freePort()), "--node-ports", "none",
                "--isolate");
        nodeVm("start", "--name", nodeName);
        nodeApi = URI.create("http://127.0.0.1:" + servicePort);
        token = nodeVm("token", "--name", nodeName).strip();
        Path tokenFile = work.resolve("node-token");
        Files.writeString(tokenFile, token);
        Files.setPosixFilePermissions(tokenFile, PosixFilePermissions.fromString("rw-------"));
        await().atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofSeconds(1)).ignoreExceptions()
                .until(() -> ping() == 200);
        Files.writeString(work.resolve("runtime.yaml"), """
                version: 1
                nodes:
                  lambda:
                    url: %s
                    token-file: node-token
                network:
                  isolation:
                    enabled: true
                  routes:
                    vendor:
                      origins: [https://api.vendor.test]
                      target:
                        backend-url: http://host.floci.internal:%d
                """.formatted(nodeApi, simulator.getAddress().getPort()));

        Map<String, String> env = new LinkedHashMap<>();
        env.put("FLOCI_STORAGE_MODE", "memory");
        env.put("FLOCI_DOCKER_RESOURCE_NAMESPACE", "lambda-e2e-" + runId);
        env.put("FLOCI_SERVICES_UI_ENABLED", "false");
        env.put("FLOCI_RUNTIME_CONFIG_FILE", work.resolve("runtime.yaml").toString());
        env.put("FLOCI_RUNTIME_DNS_LISTEN", "127.0.0.1:" + freePort());
        env.put("FLOCI_SERVICES_LAMBDA_SMOLVM_ENABLED", "true");
        env.put("FLOCI_SERVICES_LAMBDA_SMOLVM_IDLE_TIMEOUT", IDLE_TIMEOUT);
        env.put("FLOCI_SERVICES_LAMBDA_CONTAINER_IDLE_TIMEOUT_SECONDS", "900");
        floci = FlociProcess.of(moduleDir, work, freePort());
        floci.start(env);
    }

    @AfterAll
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void removeEverythingTheTestCreated() {
        if (floci != null) {
            tolerate(this::deleteFunction);
            tolerate(floci::stop);
        }
        if (simulator != null) {
            simulator.stop(0);
        }
        if (nodeCreated) {
            tolerate(() -> nodeVm("delete", "--name", nodeName));
        }
    }

    @Test
    @Order(1)
    void aFunctionRunsAsAMicroVmWithTheNetworkOfThePods() throws IOException {
        String create = JSON.writeValueAsString(Map.of("FunctionName", FUNCTION, "Runtime", "nodejs20.x",
                "Role", "arn:aws:iam::000000000000:role/lambda", "Handler", "index.handler", "Timeout", 60,
                "Code", Map.of("ZipFile", Base64.getEncoder().encodeToString(zip("index.mjs", HANDLER)))));
        HttpResponse<String> created = lambdaApi("POST", "/2015-03-31/functions", create);
        assertEquals(201, created.statusCode(), created.body());

        JsonNode answer = invoke();
        LOG.infov("The first invocation answered {0}", answer);
        assertEquals(1, answer.path("count").asInt(), answer.toString());
        assertEquals("169.254.53.1", answer.path("floci").asText(), answer.toString());
        assertEquals(200, answer.path("health").asInt(), answer.toString());
        assertEquals("000000000000", answer.path("account").asText(), answer.toString());
        assertEquals(200, answer.path("vendor").path("status").asInt(), answer.toString());
        assertThat(answer.path("vendor").path("body").asText(), containsString("api.vendor.test"));
        assertFalse(answer.path("outside").path("error").isMissingNode(), "an isolated function resolved "
                + "example.com: " + answer);

        JsonNode pod = functionPod();
        assertEquals("awake", pod.path("state").asText(), pod.toString());
        assertTrue(pod.path("hibernate").asBoolean(), pod.toString());
        JsonNode inspect = get("/docker/containers/" + pod.path("sandbox_id").asText() + "/json");
        assertEquals("io.containerd.smolvm-pods.v2", inspect.path("HostConfig").path("Runtime").asText());
        assertEquals("lambda", inspect.path("Config").path("Labels").path("io.floci.service").asText());
    }

    @Test
    @Order(2)
    void theIdleEnvironmentSleepsAndAnInvocationWakesItWithItsMemory() throws IOException {
        String pod = functionPod().path("sandbox_id").asText();
        await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofSeconds(2))
                .until(() -> "asleep".equals(get("/v1/pods/" + pod).path("state").asText()));

        long start = System.nanoTime();
        JsonNode answer = invoke();
        LOG.infov("The invocation of the sleeping function answered after {0} ms: {1}",
                (System.nanoTime() - start) / 1_000_000, answer);
        assertEquals(2, answer.path("count").asInt(), "the environment's memory: " + answer);
        assertEquals(200, answer.path("vendor").path("status").asInt(), answer.toString());
        assertEquals(pod, functionPod().path("sandbox_id").asText(), "the same environment");

        List<String> wakes = new ArrayList<>();
        for (JsonNode event : get("/v1/events?pod=" + pod).path("events")) {
            if ("awake".equals(event.path("to").asText()) && "waking".equals(event.path("from").asText())) {
                wakes.add(event.path("reason").toString() + " " + event.path("duration_ms").asText() + " ms");
            }
        }
        LOG.infov("Wakes of the function: {0}", wakes);
        assertFalse(wakes.isEmpty(), "no wake of " + pod);
    }

    @Test
    @Order(3)
    void aDeleteRemovesTheEnvironmentFromTheNode() {
        deleteFunction();

        await().atMost(Duration.ofMinutes(1)).until(() -> standalonePods().isEmpty());
        assertThat(standalonePods(), empty());
    }

    private JsonNode invoke() throws IOException {
        HttpResponse<String> response = lambdaApi("POST", "/2015-03-31/functions/" + FUNCTION + "/invocations", "{}");
        assertEquals(200, response.statusCode(), response.body());
        assertTrue(response.headers().firstValue("X-Amz-Function-Error").isEmpty(), response.body());
        return JSON.readTree(response.body());
    }

    private void deleteFunction() {
        HttpResponse<String> response = lambdaApi("DELETE", "/2015-03-31/functions/" + FUNCTION, null);
        assertTrue(response.statusCode() == 204 || response.statusCode() == 404, response.body());
    }

    /** A call of Lambda's REST API on Floci. */
    private HttpResponse<String> lambdaApi(String method, String path, String body) {
        HttpRequest request = HttpRequest.newBuilder(floci.endpoint().resolve(path)).timeout(Duration.ofMinutes(5))
                .header("Content-Type", "application/json")
                .header("Authorization", "AWS4-HMAC-SHA256 Credential=test/20261027/us-east-1/lambda/aws4_request, "
                        + "SignedHeaders=host, Signature=0")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(body))
                .build();
        try {
            return http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted", e);
        }
    }

    /** The one standalone pod on the node: the function's environment. */
    private JsonNode functionPod() {
        List<JsonNode> pods = standalonePods();
        assertEquals(1, pods.size(), "standalone pods: " + pods);
        return pods.getFirst();
    }

    private List<JsonNode> standalonePods() {
        List<JsonNode> pods = new ArrayList<>();
        for (JsonNode pod : get("/v1/pods").path("pods")) {
            if (pod.path("standalone").asBoolean()) {
                pods.add(pod);
            }
        }
        return pods;
    }

    private static byte[] zip(String name, String content) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
            zip.putNextEntry(new ZipEntry(name));
            zip.write(content.getBytes(StandardCharsets.UTF_8));
            zip.closeEntry();
        }
        return bytes.toByteArray();
    }

    /** The status of dockerd's ping through the node's Docker proxy. */
    private int ping() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(nodeApi.resolve("/docker/_ping")).timeout(Duration.ofSeconds(5))
                .header("Authorization", "Bearer " + token).GET().build();
        return http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    private JsonNode get(String path) {
        HttpRequest request = HttpRequest.newBuilder(nodeApi.resolve(path)).timeout(Duration.ofSeconds(10))
                .header("Authorization", "Bearer " + token).GET().build();
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode(), "GET " + path + ": " + response.body());
            return JSON.readTree(response.body());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted", e);
        }
    }

    private String nodeVm(String... args) {
        List<String> command = new ArrayList<>(List.of(nodeVmScript.toString()));
        command.addAll(List.of(args));
        try {
            Path out = Files.createTempFile(work, "node-vm", ".out");
            Path err = Files.createTempFile(work, "node-vm", ".err");
            ProcessBuilder builder = new ProcessBuilder(command).redirectOutput(out.toFile()).redirectError(err.toFile())
                    .redirectInput(new File("/dev/null"));
            builder.environment().put("SMOLVM_SRC", smolvmSrc);
            Process process = builder.start();
            if (!process.waitFor(COMMAND_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new AssertionError(String.join(" ", command) + " did not finish in time");
            }
            String stdout = Files.readString(out);
            if (process.exitValue() != 0) {
                throw new AssertionError(String.join(" ", command) + " exited with " + process.exitValue() + ":\n"
                        + Files.readString(err) + stdout);
            }
            return stdout;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while running " + command, e);
        }
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String firstSet(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    private static void tolerate(Runnable cleanup) {
        try {
            cleanup.run();
        } catch (RuntimeException | AssertionError e) {
            LOG.warnv("Cleanup step failed: {0}", e.getMessage());
        }
    }
}
