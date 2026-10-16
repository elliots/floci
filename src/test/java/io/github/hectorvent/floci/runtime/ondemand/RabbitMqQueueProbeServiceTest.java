package io.github.hectorvent.floci.runtime.ondemand;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.sun.net.httpserver.HttpServer;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.RabbitMqQueue;
import io.github.hectorvent.floci.services.amazonmq.AmazonMqService;
import io.github.hectorvent.floci.services.amazonmq.model.Broker;
import io.github.hectorvent.floci.services.amazonmq.model.BrokerInstance;
import io.github.hectorvent.floci.services.amazonmq.model.BrokerState;
import io.github.hectorvent.floci.services.amazonmq.model.MqUser;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RabbitMqQueueProbeServiceTest {
    private final AmazonMqService brokers = mock(AmazonMqService.class);
    private final AtomicInteger status = new AtomicInteger(200);
    private final AtomicReference<String> response = new AtomicReference<>();
    private final AtomicReference<String> path = new AtomicReference<>();
    private final AtomicReference<String> auth = new AtomicReference<>();
    private final AtomicReference<String> method = new AtomicReference<>();
    private HttpServer server;
    private RabbitMqQueueProbe probe;
    private Broker broker;
    private WorkloadDefinition definition;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            method.set(exchange.getRequestMethod());
            path.set(exchange.getRequestURI().getRawPath());
            auth.set(exchange.getRequestHeaders().getFirst("Authorization"));
            byte[] body = response.get().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status.get(), body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        broker = new Broker();
        broker.setBrokerArn("arn:aws:mq:us-east-1:000000000000:broker:jobs:b-example");
        broker.setEngineType("RABBITMQ");
        broker.setBrokerState(BrokerState.RUNNING);
        broker.setBrokerInstances(List.of(new BrokerInstance("http://127.0.0.1:" + server.getAddress().getPort(),
                List.of(), "127.0.0.1")));
        broker.setUsers(List.of(new MqUser("admin", "password", true, List.of())));
        when(brokers.describeBroker("b-example")).thenReturn(broker);
        Instance<AmazonMqService> instance = mock(Instance.class);
        when(instance.get()).thenReturn(brokers);
        probe = new RabbitMqQueueProbe(instance, new ObjectMapper(), HttpClient.newHttpClient(),
                Map.of("MQ_USER", "observer", "MQ_PASSWORD", "read-only-password")::get);
        definition = OnDemandConfigLoader.parse(new ObjectMapper(new YAMLFactory()).readTree("""
                workloads:
                  worker:
                    rabbitmq-queues: [{broker-id: b-example, queue: jobs}]
                    runtime: {type: kubernetes, deployment: worker}
                """), "000000000000", "us-east-1").getFirst();
        response.set("{\"messages_ready\":0,\"messages_unacknowledged\":0}");
    }

    @AfterEach
    void tearDown() {
        if (probe != null) {
            probe.close();
        }
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void readyAndUnacknowledgedMessagesCountAsWorkWithoutConsuming() {
        RabbitMqQueue queue = definition.rabbitMqQueues().getFirst();
        assertFalse(probe.hasWork(definition, queue));
        response.set("{\"messages_ready\":1,\"messages_unacknowledged\":0}");
        assertTrue(probe.hasWork(definition, queue));
        response.set("{\"messages_ready\":0,\"messages_unacknowledged\":1}");
        assertTrue(probe.hasWork(definition, queue));
        assertEquals("GET", method.get());
        assertEquals("/api/queues/%2F/jobs", path.get());
        assertEquals(basic("admin:password"), auth.get());
    }

    @Test
    void encodesVirtualHostsAndQueueNamesAndUsesExplicitCredentialsAfterRestore() {
        broker.getUsers().getFirst().setPassword(null);
        RabbitMqQueue queue = new RabbitMqQueue("b-example", "team/blue", "jobs #+?", "MQ_USER", "MQ_PASSWORD");
        assertFalse(probe.hasWork(definition, queue));
        assertEquals("/api/queues/team%2Fblue/jobs%20%23%2B%3F", path.get());
        assertEquals(basic("observer:read-only-password"), auth.get());
        assertThrows(IllegalStateException.class,
                () -> probe.hasWork(definition, definition.rabbitMqQueues().getFirst()));
    }

    @Test
    void missingResourcesAreIdleButUnavailableOrMalformedStatisticsAreUnknown() {
        RabbitMqQueue queue = definition.rabbitMqQueues().getFirst();
        status.set(404);
        assertFalse(probe.hasWork(definition, queue));
        for (int code : List.of(401, 403, 429, 500, 503)) {
            status.set(code);
            assertThrows(IllegalStateException.class, () -> probe.hasWork(definition, queue));
        }
        status.set(200);
        for (String body : List.of("{}", "null", "not json", "{\"messages_ready\":0}",
                "{\"messages_ready\":-1,\"messages_unacknowledged\":0}",
                "{\"messages_ready\":\"0\",\"messages_unacknowledged\":0}")) {
            response.set(body);
            assertThrows(IllegalStateException.class, () -> probe.hasWork(definition, queue));
        }
        when(brokers.describeBroker("b-example")).thenThrow(new AwsException("NotFoundException", "missing", 404));
        assertFalse(probe.hasWork(definition, queue));
    }

    @Test
    void neverQueriesABrokerInAnotherAccountOrRegionOrAnUnavailableBroker() {
        for (String arn : List.of("arn:aws:mq:us-east-1:111111111111:broker:jobs:b-example",
                "arn:aws:mq:us-west-2:000000000000:broker:jobs:b-example")) {
            broker.setBrokerArn(arn);
            assertThrows(IllegalStateException.class,
                    () -> probe.hasWork(definition, definition.rabbitMqQueues().getFirst()));
            assertNull(path.get());
        }
        broker.setBrokerArn("arn:aws:mq:us-east-1:000000000000:broker:jobs:b-example");
        broker.setBrokerState(BrokerState.CREATION_IN_PROGRESS);
        assertThrows(IllegalStateException.class,
                () -> probe.hasWork(definition, definition.rabbitMqQueues().getFirst()));
        assertNull(path.get());
    }

    private static String basic(String value) {
        return "Basic " + Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}
