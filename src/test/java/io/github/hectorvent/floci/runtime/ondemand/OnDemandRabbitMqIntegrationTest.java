package io.github.hectorvent.floci.runtime.ondemand;

import com.sun.net.httpserver.HttpServer;
import io.github.hectorvent.floci.runtime.ondemand.ActivationCoordinator.State;
import io.github.hectorvent.floci.services.amazonmq.AmazonMqService;
import io.github.hectorvent.floci.services.amazonmq.model.Broker;
import io.github.hectorvent.floci.services.amazonmq.model.BrokerInstance;
import io.github.hectorvent.floci.services.amazonmq.model.BrokerState;
import io.github.hectorvent.floci.services.amazonmq.model.MqUser;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@QuarkusTest
@TestProfile(OnDemandQueueTestProfile.class)
@QuarkusTestResource(value = OnDemandRabbitMqTestResource.class, restrictToAnnotatedClass = true)
class OnDemandRabbitMqIntegrationTest {
    @TestHTTPResource
    URI endpoint;

    @Inject
    OnDemandService service;

    @Inject
    QueueRuntimeTestFactory factory;

    @InjectMock
    AmazonMqService brokers;

    @Test
    void queueActivityWakesWorkersAndUnknownOrUnacknowledgedWorkPreventsShutdown() throws Exception {
        AtomicInteger status = new AtomicInteger(503);
        AtomicReference<String> body = new AtomicReference<>("{\"messages_ready\":0,\"messages_unacknowledged\":0}");
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            byte[] response = body.get().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status.get(), response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        Broker broker = new Broker();
        broker.setBrokerArn("arn:aws:mq:us-east-1:000000000000:broker:jobs:b-probe");
        broker.setEngineType("RABBITMQ");
        broker.setBrokerState(BrokerState.RUNNING);
        broker.setBrokerInstances(List.of(new BrokerInstance("http://127.0.0.1:" + server.getAddress().getPort(),
                List.of(), "127.0.0.1")));
        broker.setUsers(List.of(new MqUser("admin", "password", true, List.of())));
        when(brokers.describeBroker("b-probe")).thenReturn(broker);
        try (SqsClient sqs = SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("000000000000", "test")))
                .build()) {
            waitFor(State.SLEEPING);
            int starts = factory.runtime("consumer").starts.get();
            String url = sqs.createQueue(request -> request.queueName("fallback-incoming")).queueUrl();
            String queueUrl = endpoint.resolve(URI.create(url).getPath()).toString();
            try {
                sqs.sendMessage(request -> request.queueUrl(queueUrl).messageBody("wake despite unavailable RabbitMQ"));
                waitFor(State.READY);
            } finally {
                sqs.deleteQueue(request -> request.queueUrl(queueUrl));
            }
            staysReady();
            status.set(200);
            waitFor(State.SLEEPING);
            body.set("{\"messages_ready\":1,\"messages_unacknowledged\":0}");
            waitFor(State.READY);
            body.set("{\"messages_ready\":0,\"messages_unacknowledged\":1}");
            staysReady();
            status.set(401);
            body.set("{\"messages_ready\":0,\"messages_unacknowledged\":0}");
            staysReady();
            status.set(200);
            waitFor(State.SLEEPING);
            assertEquals(starts + 2, factory.runtime("consumer").starts.get());
            assertEquals(0, factory.runtime("other-account").starts.get());
            assertEquals(0, factory.runtime("other-region").starts.get());
        } finally {
            server.stop(0);
        }
    }

    private void waitFor(State state) {
        await().atMost(Duration.ofSeconds(5)).until(() -> service.coordinator().state("consumer") == state);
    }

    private void staysReady() {
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(4))
                .until(() -> service.coordinator().state("consumer") == State.READY);
    }
}
