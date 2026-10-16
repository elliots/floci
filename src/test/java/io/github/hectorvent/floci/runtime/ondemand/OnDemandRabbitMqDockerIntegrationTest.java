package io.github.hectorvent.floci.runtime.ondemand;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.dockerjava.api.DockerClient;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.RabbitMqQueue;
import io.github.hectorvent.floci.services.amazonmq.AmazonMqService;
import io.github.hectorvent.floci.services.amazonmq.CreateBrokerParams;
import io.github.hectorvent.floci.services.amazonmq.model.Broker;
import io.github.hectorvent.floci.services.amazonmq.model.BrokerState;
import io.github.hectorvent.floci.services.amazonmq.model.MqUser;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(OnDemandRabbitMqDockerIntegrationTest.Profile.class)
class OnDemandRabbitMqDockerIntegrationTest {
    private static final Logger LOG = Logger.getLogger(OnDemandRabbitMqDockerIntegrationTest.class);

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("floci.services.amazonmq.mock", "false",
                    "floci.docker.resource-namespace", "on-demand-rabbitmq-test");
        }
    }

    @Inject
    DockerClient docker;

    @Inject
    AmazonMqService brokers;

    @Inject
    RabbitMqQueueProbe probe;

    @Test
    void amqpMessagesRemainAvailableAndUnacknowledgedDeliveriesKeepQueueActivityAlive() throws Exception {
        boolean available;
        try {
            docker.pingCmd().exec();
            available = true;
        } catch (RuntimeException e) {
            available = false;
            LOG.warnv("Docker unavailable for RabbitMQ wake-up test: {0}", e.getMessage());
        }
        Assumptions.assumeTrue(available, "Docker is required for the RabbitMQ protocol test");
        String password = "RabbitProbePassword123";
        Broker broker = brokers.createBroker(new CreateBrokerParams("wake-" + UUID.randomUUID(), "RABBITMQ",
                "3.13", "SINGLE_INSTANCE", "mq.t3.micro", false, false,
                List.of(new MqUser("observer", password, true, List.of())), Map.of()));
        try {
            await().atMost(Duration.ofSeconds(60)).until(() -> broker.getBrokerState() == BrokerState.RUNNING);
            WorkloadDefinition definition = OnDemandConfigLoader.parse(new ObjectMapper(new YAMLFactory()).readTree("""
                    workloads:
                      consumer:
                        rabbitmq-queues: [{broker-id: %s, queue: wake-jobs}]
                        runtime: {type: kubernetes, deployment: worker}
                    """.formatted(broker.getBrokerId())), "000000000000", "us-east-1").getFirst();
            RabbitMqQueue queue = definition.rabbitMqQueues().getFirst();
            ConnectionFactory connections = new ConnectionFactory();
            connections.setUri(broker.getBrokerInstances().getFirst().getEndpoints().getFirst());
            connections.setUsername("observer");
            connections.setPassword(password);
            connections.setAutomaticRecoveryEnabled(false);
            try (Connection connection = connections.newConnection(); Channel channel = connection.createChannel()) {
                channel.queueDeclare("wake-jobs", true, false, false, Map.of());
                channel.confirmSelect();
                channel.basicPublish("", "wake-jobs", new AMQP.BasicProperties.Builder().deliveryMode(2).build(),
                        "wake-and-process".getBytes(StandardCharsets.UTF_8));
                channel.waitForConfirmsOrDie(5000);
                await().ignoreExceptions().atMost(Duration.ofSeconds(30)).until(() -> probe.hasWork(definition, queue));
                GetResponse message = channel.basicGet("wake-jobs", false);
                assertNotNull(message, "The probe must not consume the triggering message");
                assertFalse(message.getEnvelope().isRedeliver(), "The probe must not receive and requeue messages");
                assertEquals("wake-and-process", new String(message.getBody(), StandardCharsets.UTF_8));
                assertEquals(0, channel.queueDeclarePassive("wake-jobs").getMessageCount());
                await().during(Duration.ofSeconds(7)).atMost(Duration.ofSeconds(12))
                        .until(() -> probe.hasWork(definition, queue));
                channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
                await().atMost(Duration.ofSeconds(30)).until(() -> !probe.hasWork(definition, queue));
                channel.queueDelete("wake-jobs");
            }
        } finally {
            brokers.deleteBroker(broker.getBrokerId());
        }
    }
}
