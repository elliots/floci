package io.github.hectorvent.floci.runtime.ondemand;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.runtime.ondemand.ActivationCoordinator.State;
import io.github.hectorvent.floci.services.amazonmq.AmazonMqService;
import io.github.hectorvent.floci.services.amazonmq.CreateBrokerParams;
import io.github.hectorvent.floci.services.amazonmq.model.Broker;
import io.github.hectorvent.floci.services.amazonmq.model.BrokerState;
import io.github.hectorvent.floci.services.amazonmq.model.MqUser;
import io.github.hectorvent.floci.services.sqs.SqsService;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.config.HttpClientConfig;
import io.restassured.config.RestAssuredConfig;
import io.vertx.core.Vertx;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@QuarkusTest
@TestProfile(MicrosandboxOnDemandIntegrationTest.Profile.class)
@EnabledIfEnvironmentVariable(named = "FLOCI_TEST_MICROSANDBOX_URL", matches = ".+")
class MicrosandboxOnDemandIntegrationTest {
    private static final String RABBIT_USER = "nativeobserver";
    private static final String RABBIT_PASSWORD = "NativeRabbitPassword123";
    private static final String RABBIT_QUEUE = "native-jobs";

    @TestHTTPResource
    URI endpoint;

    @Inject
    EmulatorConfig config;

    @Inject
    Instance<SqsService> sqsServices;

    @Inject
    Instance<RabbitMqQueueProbe> rabbitProbes;

    @Inject
    AmazonMqService brokers;

    @Inject
    WorkloadRuntimeFactory runtimeFactory;

    @Inject
    Vertx vertx;

    @Inject
    ObjectMapper mapper;

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.ofEntries(
                    Map.entry("floci.on-demand.enabled", "false"),
                    Map.entry("floci.on-demand.microsandbox-url",
                            System.getenv().getOrDefault("FLOCI_TEST_MICROSANDBOX_URL", "http://127.0.0.1:1")),
                    Map.entry("floci.on-demand.microsandbox-token",
                            System.getenv().getOrDefault("FLOCI_TEST_MICROSANDBOX_TOKEN", "unused")),
                    Map.entry("floci.services.amazonmq.mock", "false"),
                    Map.entry("floci.docker.resource-namespace", "microsandbox-wake-test"),
                    Map.entry("floci.services.iot.mqtt.enabled", "true"),
                    Map.entry("floci.services.iot.mqtt.host", "127.0.0.1"),
                    Map.entry("floci.services.iot.mqtt.port", Integer.toString(freePort())));
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    void httpSqsRabbitMqAndIotWakeTheSameProcessMemoryAfterHibernation() throws Exception {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String workload = "native-" + suffix;
        String queueName = "native-incoming-" + suffix;
        String ruleName = "native_wake_" + suffix;
        try (SqsClient sqs = sqsClient(); HttpClient http = HttpClient.newHttpClient(); Cleanup cleanup = new Cleanup()) {
            String returnedUrl = sqs.createQueue(request -> request.queueName(queueName)).queueUrl();
            String queueUrl = endpoint.resolve(URI.create(returnedUrl).getPath()).toString();
            cleanup.add(() -> sqs.deleteQueue(request -> request.queueUrl(queueUrl)));

            Broker broker = brokers.createBroker(new CreateBrokerParams("native-wake-" + suffix, "RABBITMQ",
                    "3.13", "SINGLE_INSTANCE", "mq.t3.micro", false, false,
                    List.of(new MqUser(RABBIT_USER, RABBIT_PASSWORD, true, List.of())), Map.of()));
            cleanup.add(() -> brokers.deleteBroker(broker.getBrokerId()));
            await().atMost(Duration.ofSeconds(90)).until(() -> broker.getBrokerState() == BrokerState.RUNNING);

            createTopicRule(ruleName, queueUrl);
            cleanup.add(() -> given().when().delete("/rules/" + ruleName));
            given().when().get("/endpoint").then().statusCode(200);

            Path definitions = writeDefinition(workload, queueName, queueUrl, broker);
            cleanup.add(() -> Files.deleteIfExists(definitions));
            cleanup.add(runtimeFactory::clear);
            int gatewayPort = freePort();
            OnDemandService service = new OnDemandService(activationConfig(definitions, gatewayPort),
                    sqsServices, runtimeFactory, vertx, rabbitProbes);
            cleanup.add(() -> service.stop(null));

            ConnectionFactory connections = new ConnectionFactory();
            connections.setUri(broker.getBrokerInstances().getFirst().getEndpoints().getFirst());
            connections.setUsername(RABBIT_USER);
            connections.setPassword(RABBIT_PASSWORD);
            connections.setAutomaticRecoveryEnabled(false);
            try (Connection connection = connections.newConnection(); Channel channel = connection.createChannel()) {
                channel.queueDeclare(RABBIT_QUEUE, true, false, false, Map.of());
                channel.confirmSelect();
                service.start(null);
                awaitState(service, workload, State.SLEEPING);

                JsonNode first = invoke(gatewayPort);
                String boot = first.path("boot").asText();
                assertFalse(boot.isBlank());
                assertEquals(1, first.path("requests").asInt());
                sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("preflight-" + suffix));
                awaitPayload(service, workload, http, "sqs", "preflight-" + suffix, boot, 1);
                awaitState(service, workload, State.SLEEPING);
                JsonNode second = invoke(gatewayPort);
                assertEquals(boot, second.path("boot").asText(), "HTTP wake must restore the original process");
                assertEquals(2, second.path("requests").asInt(), "The HTTP counter lives only in guest RAM");
                awaitState(service, workload, State.SLEEPING);

                sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("sqs-wake-" + suffix));
                awaitState(service, workload, State.READY);
                awaitPayload(service, workload, http, "sqs", "sqs-wake-" + suffix, boot);
                awaitState(service, workload, State.SLEEPING);

                String mqttPayload = "{\"sensor\":\"" + suffix + "\",\"temperature\":21}";
                try (MqttClient mqtt = new MqttClient("tcp://127.0.0.1:" + config.services().iot().mqtt().port(),
                        "native-device-" + suffix, new MemoryPersistence())) {
                    MqttConnectOptions options = new MqttConnectOptions();
                    options.setCleanSession(true);
                    mqtt.connect(options);
                    mqtt.publish("native/devices/sensor", mqttPayload.getBytes(StandardCharsets.UTF_8), 1, false);
                    awaitState(service, workload, State.READY);
                    awaitPayload(service, workload, http, "sqs", mqttPayload, boot);
                    mqtt.disconnect();
                }
                awaitState(service, workload, State.SLEEPING);

                String rabbitPayload = "rabbit-wake-" + suffix;
                channel.basicPublish("", RABBIT_QUEUE, new AMQP.BasicProperties.Builder().deliveryMode(2).build(),
                        rabbitPayload.getBytes(StandardCharsets.UTF_8));
                channel.waitForConfirmsOrDie(5000);
                awaitState(service, workload, State.READY);
                awaitPayload(service, workload, http, "rabbitmq", rabbitPayload, boot);
                JsonNode last = readState(service, workload, http);
                assertTrue(contains(last.path("sqs"), "sqs-wake-" + suffix));
                assertTrue(contains(last.path("sqs"), mqttPayload), "Earlier messages must survive later snapshots");
                awaitState(service, workload, State.SLEEPING);
            }
        }
    }

    private EmulatorConfig activationConfig(Path definitions, int gatewayPort) {
        EmulatorConfig override = mock(EmulatorConfig.class, delegatesTo(config));
        EmulatorConfig.OnDemandConfig settings = mock(EmulatorConfig.OnDemandConfig.class, delegatesTo(config.onDemand()));
        doReturn(true).when(settings).enabled();
        doReturn(Optional.of(definitions.toString())).when(settings).configFile();
        doReturn("127.0.0.1").when(settings).gatewayHost();
        doReturn(gatewayPort).when(settings).gatewayPort();
        doReturn(100L).when(settings).reconcileIntervalMillis();
        doReturn(settings).when(override).onDemand();
        return override;
    }

    private Path writeDefinition(String workload, String queueName, String queueUrl, Broker broker) throws IOException {
        String script;
        try (InputStream resource = getClass().getResourceAsStream("/ondemand/native-worker.py")) {
            assertNotNull(resource);
            script = new String(resource.readAllBytes(), StandardCharsets.UTF_8);
        }
        URI console = URI.create(broker.getBrokerInstances().getFirst().getConsoleURL());
        Map<String, Object> runtime = Map.of("type", "microsandbox", "image", "python:3.12-alpine",
                "command", List.of("python", "-u", "-c", script), "memory-mib", 512, "backend-port", 8080,
                "environment", Map.of("SQS_ENDPOINT", "http://host.microsandbox.internal:" + endpoint.getPort(),
                        "SQS_QUEUE_URL", queueUrl,
                        "RABBIT_URL", "http://host.microsandbox.internal:" + console.getPort(),
                        "RABBIT_USER", RABBIT_USER, "RABBIT_PASSWORD", RABBIT_PASSWORD));
        Map<String, Object> definition = new LinkedHashMap<>();
        definition.put("host", "native.test");
        definition.put("idle-timeout-seconds", 2);
        definition.put("startup-timeout-seconds", 240);
        definition.put("queues", List.of(queueName));
        definition.put("rabbitmq-queues", List.of(Map.of("broker-id", broker.getBrokerId(), "queue", RABBIT_QUEUE)));
        definition.put("runtime", runtime);
        Path file = Files.createTempFile("floci-native-wake-", ".yaml");
        try {
            new ObjectMapper(new YAMLFactory()).writeValue(file.toFile(), Map.of("workloads", Map.of(workload, definition)));
        } catch (IOException e) {
            Files.deleteIfExists(file);
            throw e;
        }
        return file;
    }

    private void createTopicRule(String name, String queueUrl) {
        given().contentType("application/json").body(Map.of(
                "sql", "SELECT * FROM 'native/devices/+'", "awsIotSqlVersion", "2016-03-23",
                "actions", List.of(Map.of("sqs", Map.of("queueUrl", queueUrl, "useBase64", false,
                        "roleArn", "arn:aws:iam::000000000000:role/iot-to-sqs")))))
                .when().post("/rules/" + name).then().statusCode(200);
    }

    private JsonNode invoke(int gatewayPort) throws IOException {
        String response = given().baseUri("http://127.0.0.1").port(gatewayPort).header("Host", "native.test")
                .config(RestAssuredConfig.config().httpClient(HttpClientConfig.httpClientConfig()
                        .setParam("http.socket.timeout", 300000).setParam("http.connection.timeout", 10000)))
                .when().get("/invoke").then().statusCode(200).extract().asString();
        return mapper.readTree(response);
    }

    private void awaitPayload(OnDemandService service, String workload, HttpClient http,
                              String source, String payload, String boot) {
        awaitPayload(service, workload, http, source, payload, boot, 2);
    }

    private void awaitPayload(OnDemandService service, String workload, HttpClient http,
                              String source, String payload, String boot, int requests) {
        await().ignoreExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            JsonNode state = readState(service, workload, http);
            assertEquals(boot, state.path("boot").asText());
            assertEquals(requests, state.path("requests").asInt());
            assertTrue(contains(state.path(source), payload),
                    () -> "The original wake message must reach the restored worker: " + state);
        });
    }

    private JsonNode readState(OnDemandService service, String workload, HttpClient http) throws Exception {
        assertEquals(State.READY, service.coordinator().state(workload));
        URI backend = service.coordinator().backend(workload);
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(backend.resolve("/state"))
                .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        return mapper.readTree(response.body());
    }

    private static boolean contains(JsonNode values, String expected) {
        for (JsonNode value : values) {
            if (expected.equals(value.asText())) {
                return true;
            }
        }
        return false;
    }

    private static void awaitState(OnDemandService service, String workload, State state) {
        await().atMost(Duration.ofMinutes(4)).until(() -> service.coordinator().state(workload) == state);
    }

    private SqsClient sqsClient() {
        return SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .httpClientBuilder(UrlConnectionHttpClient.builder().socketTimeout(Duration.ofSeconds(30)))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("000000000000", "test"))).build();
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static final class Cleanup implements AutoCloseable {
        private final List<AutoCloseable> resources = new ArrayList<>();

        void add(AutoCloseable resource) {
            resources.add(resource);
        }

        @Override
        public void close() throws Exception {
            Exception failure = null;
            for (AutoCloseable resource : resources.reversed()) {
                try {
                    resource.close();
                } catch (Exception e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}
