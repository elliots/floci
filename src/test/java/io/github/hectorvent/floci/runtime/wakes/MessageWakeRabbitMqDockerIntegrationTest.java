package io.github.hectorvent.floci.runtime.wakes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import io.github.hectorvent.floci.core.common.RequestScopes;
import io.github.hectorvent.floci.core.common.docker.ContainerExec;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.services.amazonmq.AmazonMqService;
import io.github.hectorvent.floci.testing.SidecarContainersProfile;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.mq.MqClient;
import software.amazon.awssdk.services.mq.model.BrokerState;
import software.amazon.awssdk.services.mq.model.DeploymentMode;
import software.amazon.awssdk.services.mq.model.EngineType;
import software.amazon.awssdk.services.mq.model.User;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Message wakes from the firehose of a RabbitMQ broker that Floci's Amazon MQ runs in Docker:
 * Floci's broker user follows the broker's publishes and deliveries, a publish wakes the
 * workload's pods at once, every publish and delivery keeps their lease, an unacknowledged
 * delivery keeps it, the lease ends one decision after the last acknowledgement, a backlog keeps
 * it until a purge, and Floci follows the broker again after it restarts, after its trace queue
 * is deleted and after its container is stopped and started. A virtual host created after the
 * watch is followed from the broker's event. Skipped without Docker.
 */
@QuarkusTest
@TestProfile(MessageWakeRabbitMqDockerIntegrationTest.Profile.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MessageWakeRabbitMqDockerIntegrationTest {

    private static final WakeTestNode NODE = WakeTestNode.start("message-wake-rabbitmq-docker-it");
    private static final String USER = "observer";
    private static final String PASSWORD = "ObserverPass123";
    private static final String QUEUE = "wake-jobs";
    private static final String OWN_HOST = "traced-by-user";
    private static final String OWN_QUEUE = "own-jobs";
    private static final String POD = "worker-1";
    private static final String LATE_HOST = "created-later";
    private static final String LATE_QUEUE = "late-jobs";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    private static String brokerId;
    private static String amqp;
    private static String console;
    private static URI flociEndpoint;

    @TestHTTPResource
    URI endpoint;

    @Inject
    AmazonMqService brokers;

    @Inject
    ContainerLifecycleManager containers;

    @BeforeEach
    void broker() throws Exception {
        SidecarContainersProfile.requireDocker();
        if (brokerId != null) {
            return;
        }
        flociEndpoint = endpoint;
        NODE.pod(POD, "jobs", "worker", "asleep");
        try (MqClient mq = mq()) {
            brokerId = mq.createBroker(request -> request.brokerName("wake-" + UUID.randomUUID())
                    .engineType(EngineType.RABBITMQ)
                    .engineVersion("3.13")
                    .deploymentMode(DeploymentMode.SINGLE_INSTANCE)
                    .hostInstanceType("mq.t3.micro")
                    .publiclyAccessible(false)
                    .autoMinorVersionUpgrade(false)
                    .users(User.builder().username(USER).password(PASSWORD).consoleAccess(true).build()))
                    .brokerId();
            await().atMost(Duration.ofMinutes(2)).until(() ->
                    mq.describeBroker(request -> request.brokerId(brokerId)).brokerState() == BrokerState.RUNNING);
            amqp = mq.describeBroker(request -> request.brokerId(brokerId)).brokerInstances().getFirst()
                    .endpoints().getFirst();
            console = mq.describeBroker(request -> request.brokerId(brokerId)).brokerInstances().getFirst()
                    .consoleURL();
        }
        api("PUT", "/api/vhosts/" + OWN_HOST, "{}");
        api("PUT", "/api/permissions/" + OWN_HOST + "/" + USER, "{\"configure\":\".*\",\"write\":\".*\",\"read\":\".*\"}");
        exec("rabbitmqctl", "trace_on", "-p", OWN_HOST);
        try (Connection connection = connection("/"); Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE, true, false, false, Map.of());
        }
        try (Connection connection = connection(OWN_HOST); Channel channel = connection.createChannel()) {
            channel.queueDeclare(OWN_QUEUE, true, false, false, Map.of());
        }
        NODE.write("""
                worker:
                  idle-timeout-seconds: 2
                  rabbitmq-queues:
                    - {broker-id: %s, queue: %s}
                    - {broker-id: %s, virtual-host: %s, queue: %s}
                  runtime: {type: kubernetes, node: dev, namespace: jobs, deployment: worker}
                """.formatted(brokerId, QUEUE, brokerId, OWN_HOST, OWN_QUEUE));
        await().atMost(Duration.ofSeconds(60)).until(() -> tracing("/") && flociConnections() == 2);
        Thread.sleep(Duration.ofSeconds(2));
    }

    @AfterAll
    static void deleteBroker() {
        try {
            if (brokerId != null) {
                try (MqClient mq = MqClient.builder().endpointOverride(flociEndpoint).region(Region.US_EAST_1)
                        .credentialsProvider(WakeTestNode.credentials()).build()) {
                    mq.deleteBroker(request -> request.brokerId(brokerId));
                }
            }
        } finally {
            NODE.close();
        }
    }

    @Test
    @Order(1)
    void flocisUserFollowsTheFirehoseOfEachWatchedVirtualHost() throws Exception {
        JsonNode users = api("GET", "/api/users", null);
        JsonNode wakeUser = null;
        for (JsonNode user : users) {
            if (user.path("name").asText().startsWith("floci-wakes-")) {
                wakeUser = user;
            }
        }
        assertThat(wakeUser, notNullValue());
        assertThat(wakeUser.path("tags").size(), is(0));
        String name = wakeUser.path("name").asText();
        JsonNode permissions = api("GET", "/api/permissions/%2F/" + name, null);
        assertThat(permissions.path("configure").asText(), is("^amq\\.gen-.*"));
        assertThat(permissions.path("write").asText(), is("^(amq\\.gen-.*|amq\\.default)$"));
        assertThat(permissions.path("read").asText(), is("^(amq\\.rabbitmq\\.(trace|event)|amq\\.gen-.*)$"));
        assertThat(tracing("/"), is(true));
        assertThat(tracing(OWN_HOST), is(true));
        assertThat(flociConnections(), is(2));
        assertThat(NODE.wakes(POD), hasSize(0));
    }

    @Test
    @Order(2)
    void aPublishWakesThePodsAtOnceAndTheLeaseEndsOneLingerAfterTheAcknowledgement() throws Exception {
        try (Connection connection = connection("/"); Channel channel = connection.createChannel()) {
            Instant published = Instant.now();
            publish(channel, QUEUE, "wake and work");

            await().atMost(Duration.ofSeconds(1)).until(() -> NODE.state(POD).equals("awake"));
            assertThat(NODE.leases(POD), contains("floci:worker"));
            assertThat(NODE.wakes(POD).getFirst().body().path("reason").path("queue").asText(),
                    is("amqp://" + brokerId + "/%2F/" + QUEUE));

            GetResponse delivery = channel.basicGet(QUEUE, false);
            assertThat(new String(delivery.getBody(), StandardCharsets.UTF_8), is("wake and work"));
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            Instant acknowledged = Instant.now();

            await().atMost(Duration.ofSeconds(20)).until(() -> NODE.leases(POD).isEmpty());
            assertThat(Duration.between(acknowledged, Instant.now()), greaterThanOrEqualTo(Duration.ofMillis(1500)));
            assertThat(NODE.wakes(POD), hasSize(1));
            assertThat(published.isBefore(acknowledged), is(true));
        }
        NODE.fake().state(POD, "asleep");
    }

    @Test
    @Order(3)
    void aPublishThroughTheManagementApiWakesThePods() throws Exception {
        int wakes = NODE.wakes(POD).size();

        api("POST", "/api/exchanges/%2F/amq.default/publish",
                "{\"properties\":{},\"routing_key\":\"" + QUEUE + "\",\"payload\":\"m\",\"payload_encoding\":\"string\"}");

        await().atMost(Duration.ofSeconds(1)).until(() -> NODE.state(POD).equals("awake"));
        assertThat(NODE.wakes(POD), hasSize(wakes + 1));
        try (Connection connection = connection("/"); Channel channel = connection.createChannel()) {
            GetResponse delivery = channel.basicGet(QUEUE, true);
            assertThat(delivery, notNullValue());
        }
        await().atMost(Duration.ofSeconds(20)).until(() -> NODE.leases(POD).isEmpty());
        NODE.fake().state(POD, "asleep");
    }

    @Test
    @Order(4)
    void aSteadyStreamOfQuicklyAcknowledgedMessagesKeepsOneLease() throws Exception {
        int wakes = NODE.wakes(POD).size();
        int deletes = NODE.requests("DELETE", WakeTestNode.leasePath(POD, "floci:worker")).size();
        try (Connection consumerConnection = connection("/"); Channel consumer = consumerConnection.createChannel();
             Connection publisherConnection = connection("/"); Channel publisher = publisherConnection.createChannel()) {
            consumer.basicQos(1);
            consumer.basicConsume(QUEUE, false, new DefaultConsumer(consumer) {
                @Override
                public void handleDelivery(String tag, Envelope envelope, AMQP.BasicProperties properties,
                                           byte[] body) throws IOException {
                    getChannel().basicAck(envelope.getDeliveryTag(), false);
                }
            });
            publish(publisher, QUEUE, "first");
            await().atMost(Duration.ofSeconds(1)).until(() -> NODE.state(POD).equals("awake"));

            for (int i = 0; i < 20; i++) {
                Thread.sleep(1000);
                publish(publisher, QUEUE, "message " + i);
                assertThat("message " + i, NODE.leases(POD), contains("floci:worker"));
            }
            assertThat(NODE.wakes(POD), hasSize(wakes + 1));
            assertThat(NODE.requests("DELETE", WakeTestNode.leasePath(POD, "floci:worker")), hasSize(deletes));

            await().atMost(Duration.ofSeconds(20)).until(() -> NODE.leases(POD).isEmpty());
        }
        NODE.fake().state(POD, "asleep");
    }

    @Test
    @Order(5)
    void anUnacknowledgedDeliveryKeepsTheLeaseUntilItsAcknowledgement() throws Exception {
        try (Connection connection = connection("/"); Channel channel = connection.createChannel()) {
            publish(channel, QUEUE, "slow work");
            await().atMost(Duration.ofSeconds(1)).until(() -> NODE.state(POD).equals("awake"));
            GetResponse delivery = channel.basicGet(QUEUE, false);
            assertThat(delivery, notNullValue());

            Thread.sleep(Duration.ofSeconds(16));
            assertThat(NODE.leases(POD), contains("floci:worker"));

            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            await().atMost(Duration.ofSeconds(25)).until(() -> NODE.leases(POD).isEmpty());
        }
        NODE.fake().state(POD, "asleep");
    }

    @Test
    @Order(6)
    void aPublishToAVirtualHostTheUserTracesWakesAndItsTracingStaysOnAfterTheWatch() throws Exception {
        try (Connection connection = connection(OWN_HOST); Channel channel = connection.createChannel()) {
            publish(channel, OWN_QUEUE, "own");
            await().atMost(Duration.ofSeconds(1)).until(() -> NODE.state(POD).equals("awake"));
            assertThat(NODE.wakes(POD).getLast().body().path("reason").path("queue").asText(),
                    is("amqp://" + brokerId + "/" + OWN_HOST + "/" + OWN_QUEUE));
            assertThat(channel.basicGet(OWN_QUEUE, true), notNullValue());
        }
        await().atMost(Duration.ofSeconds(20)).until(() -> NODE.leases(POD).isEmpty());
        NODE.fake().state(POD, "asleep");

        NODE.write("""
                worker:
                  idle-timeout-seconds: 2
                  rabbitmq-queues: [{broker-id: %s, queue: %s}]
                  runtime: {type: kubernetes, node: dev, namespace: jobs, deployment: worker}
                """.formatted(brokerId, QUEUE));

        await().atMost(Duration.ofSeconds(60)).until(() -> flociConnections() == 1);
        assertThat(tracing(OWN_HOST), is(true));
        assertThat(tracing("/"), is(true));
    }

    @Test
    @Order(7)
    void flociFollowsTheBrokerAgainAfterItRestarts() throws Exception {
        int wakes = NODE.wakes(POD).size();
        String container = RequestScopes.callAs("000000000000", "us-east-1",
                () -> brokers.describeBroker(brokerId)).getContainerId();

        containers.getDockerClient().restartContainerCmd(container).withTimeout(10).exec();

        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(1)).until(() -> {
            try {
                return tracing("/") && flociConnections() == 1;
            } catch (IOException | RuntimeException e) {
                return false;
            }
        });
        assertThat(NODE.wakes(POD), hasSize(wakes));

        try (Connection connection = connection("/"); Channel channel = connection.createChannel()) {
            publish(channel, QUEUE, "after the restart");
            await().atMost(Duration.ofSeconds(1)).until(() -> NODE.state(POD).equals("awake"));
            assertThat(channel.basicGet(QUEUE, true), notNullValue());
        }
        await().atMost(Duration.ofSeconds(20)).until(() -> NODE.leases(POD).isEmpty());
        NODE.fake().state(POD, "asleep");
    }

    @Test
    @Order(8)
    void aBacklogWithNoConsumerKeepsTheLeaseUntilAPurgeWithNoEvent() throws Exception {
        try (Connection connection = connection("/"); Channel channel = connection.createChannel()) {
            publish(channel, QUEUE, "nobody takes this");
        }
        await().atMost(Duration.ofSeconds(1)).until(() -> NODE.state(POD).equals("awake"));

        Thread.sleep(Duration.ofSeconds(10));
        assertThat(NODE.leases(POD), contains("floci:worker"));

        api("DELETE", "/api/queues/%2F/" + QUEUE + "/contents", null);

        await().atMost(Duration.ofSeconds(20)).until(() -> NODE.leases(POD).isEmpty());
        NODE.fake().state(POD, "asleep");
    }

    @Test
    @Order(9)
    void aDeletedTraceQueueIsFollowedAgain() throws Exception {
        String trace = traceQueue();
        assertThat(trace, notNullValue());

        exec("rabbitmqctl", "delete_queue", "-p", "/", trace);

        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).until(() -> {
            String next = traceQueue();
            return next != null && !next.equals(trace);
        });
        Thread.sleep(Duration.ofSeconds(1));
        try (Connection connection = connection("/"); Channel channel = connection.createChannel()) {
            publish(channel, QUEUE, "after the delete");
            await().atMost(Duration.ofSeconds(1)).until(() -> NODE.state(POD).equals("awake"));
            assertThat(channel.basicGet(QUEUE, true), notNullValue());
        }
        await().atMost(Duration.ofSeconds(20)).until(() -> NODE.leases(POD).isEmpty());
        NODE.fake().state(POD, "asleep");
    }

    @Test
    @Order(10)
    void aStoppedBrokerContainerReleasesTheLeaseAndIsFollowedOnceItRunsAgain() throws Exception {
        try (Connection connection = connection("/"); Channel channel = connection.createChannel()) {
            publish(channel, QUEUE, "kept across the stop");
        }
        await().atMost(Duration.ofSeconds(1)).until(() -> NODE.state(POD).equals("awake"));
        String container = RequestScopes.callAs("000000000000", "us-east-1",
                () -> brokers.describeBroker(brokerId)).getContainerId();

        containers.getDockerClient().stopContainerCmd(container).withTimeout(10).exec();

        await().atMost(Duration.ofSeconds(60)).until(() -> NODE.leases(POD).isEmpty());
        NODE.fake().state(POD, "asleep");

        containers.getDockerClient().startContainerCmd(container).exec();

        await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofSeconds(1)).until(() -> {
            try {
                return tracing("/") && flociConnections() == 1;
            } catch (IOException | RuntimeException e) {
                return false;
            }
        });
        await().atMost(Duration.ofSeconds(10)).until(() -> NODE.leases(POD).equals(List.of("floci:worker")));
        api("DELETE", "/api/queues/%2F/" + QUEUE + "/contents", null);
        await().atMost(Duration.ofSeconds(20)).until(() -> NODE.leases(POD).isEmpty());
        NODE.fake().state(POD, "asleep");
    }

    @Test
    @Order(11)
    void aVirtualHostCreatedAfterTheWatchIsFollowedFromTheBrokersEvent() throws Exception {
        NODE.write("""
                worker:
                  idle-timeout-seconds: 2
                  rabbitmq-queues:
                    - {broker-id: %s, queue: %s}
                    - {broker-id: %s, virtual-host: %s, queue: %s}
                  runtime: {type: kubernetes, node: dev, namespace: jobs, deployment: worker}
                """.formatted(brokerId, QUEUE, brokerId, LATE_HOST, LATE_QUEUE));
        await().atMost(Duration.ofSeconds(60)).until(() -> flociConnections() == 2);
        assertThat(flociConnections(LATE_HOST), is(0));

        api("PUT", "/api/vhosts/" + LATE_HOST, "{}");
        api("PUT", "/api/permissions/" + LATE_HOST + "/" + USER,
                "{\"configure\":\".*\",\"write\":\".*\",\"read\":\".*\"}");

        await().atMost(Duration.ofSeconds(30)).until(() -> tracing(LATE_HOST) && flociConnections(LATE_HOST) == 1);
        try (Connection connection = connection(LATE_HOST); Channel channel = connection.createChannel()) {
            channel.queueDeclare(LATE_QUEUE, true, false, false, Map.of());
            publish(channel, LATE_QUEUE, "late");
            await().atMost(Duration.ofSeconds(1)).until(() -> NODE.state(POD).equals("awake"));
            assertThat(NODE.wakes(POD).getLast().body().path("reason").path("queue").asText(),
                    is("amqp://" + brokerId + "/" + LATE_HOST + "/" + LATE_QUEUE));
            assertThat(channel.basicGet(LATE_QUEUE, true), notNullValue());
        }
        await().atMost(Duration.ofSeconds(20)).until(() -> NODE.leases(POD).isEmpty());
        NODE.fake().state(POD, "asleep");
    }

    @Test
    @Order(12)
    void withNoWorkloadFlociTurnsItsTracingOffAndDeletesItsUser() throws Exception {
        NODE.write("");

        await().atMost(Duration.ofSeconds(60)).until(() -> !tracing("/") && flociConnections() == 0
                && users().stream().noneMatch(user -> user.startsWith("floci-wakes-")));
        assertThat(users(), hasItem(USER));
    }

    private static List<String> users() throws IOException, InterruptedException {
        List<String> users = new ArrayList<>();
        for (JsonNode user : api("GET", "/api/users", null)) {
            users.add(user.path("name").asText());
        }
        return users;
    }

    private static MqClient mq() {
        return MqClient.builder().endpointOverride(flociEndpoint).region(Region.US_EAST_1)
                .credentialsProvider(WakeTestNode.credentials()).build();
    }

    private static Connection connection(String virtualHost) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(amqp);
        factory.setVirtualHost(virtualHost);
        factory.setUsername(USER);
        factory.setPassword(PASSWORD);
        factory.setAutomaticRecoveryEnabled(false);
        return factory.newConnection();
    }

    private static void publish(Channel channel, String queue, String body) throws Exception {
        channel.confirmSelect();
        channel.basicPublish("", queue, new AMQP.BasicProperties.Builder().deliveryMode(2).build(),
                body.getBytes(StandardCharsets.UTF_8));
        channel.waitForConfirmsOrDie(5000);
    }

    private static boolean tracing(String virtualHost) throws IOException, InterruptedException {
        return api("GET", "/api/vhosts/" + URLEncoder.encode(virtualHost, StandardCharsets.UTF_8), null)
                .path("tracing").asBoolean();
    }

    /** Floci's open connections to the broker, by their name. */
    private static int flociConnections() throws IOException, InterruptedException {
        return flociConnections(null);
    }

    /** Floci's open connections to the broker's {@code virtualHost}, or to any with null. */
    private static int flociConnections(String virtualHost) throws IOException, InterruptedException {
        int count = 0;
        for (JsonNode connection : api("GET", "/api/connections", null)) {
            if (RabbitMqTraceClient.CONNECTION_NAME.equals(connection.path("client_properties")
                    .path("connection_name").asText())
                    && (virtualHost == null || virtualHost.equals(connection.path("vhost").asText()))) {
                count++;
            }
        }
        return count;
    }

    /** The name of Floci's trace queue in {@code /}, the server-named queue of 10,000 traces, or null. */
    private static String traceQueue() throws IOException, InterruptedException {
        for (JsonNode queue : api("GET", "/api/queues/%2F", null)) {
            if (queue.path("name").asText().startsWith("amq.gen-")
                    && queue.path("arguments").path("x-max-length").asInt() == RabbitMqTraceClient.TRACE_QUEUE_LENGTH) {
                return queue.path("name").asText();
            }
        }
        return null;
    }

    private static JsonNode api(String method, String path, String body) throws IOException, InterruptedException {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(console + path))
                .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(
                        (USER + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8)))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(body));
        HttpResponse<String> response = HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 300) {
            throw new IOException(method + " " + path + " answered " + response.statusCode() + ": " + response.body());
        }
        return response.body().isBlank() ? JSON.createObjectNode() : JSON.readTree(response.body());
    }

    private void exec(String... command) {
        String container = RequestScopes.callAs("000000000000", "us-east-1",
                () -> brokers.describeBroker(brokerId)).getContainerId();
        ContainerExec.Result result = ContainerExec.run(containers.getDockerClient(), container, command, 30);
        assertThat(result.summary(), result.exitCode(), is(0L));
    }

    /**
     * Runs real brokers in this test run's container namespace, and starts with no workloads: the
     * test names its broker in the runtime file once the broker exists.
     */
    public static class Profile implements QuarkusTestProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> config = new HashMap<>(NODE.config(""));
            config.put("floci.services.amazonmq.mock", "false");
            config.put("floci.docker.resource-namespace", SidecarContainersProfile.namespace());
            return config;
        }
    }
}
