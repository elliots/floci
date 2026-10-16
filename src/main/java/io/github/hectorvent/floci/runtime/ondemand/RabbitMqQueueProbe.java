package io.github.hectorvent.floci.runtime.ondemand;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.core.common.AwsArnUtils;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.RabbitMqQueue;
import io.github.hectorvent.floci.services.amazonmq.AmazonMqService;
import io.github.hectorvent.floci.services.amazonmq.model.Broker;
import io.github.hectorvent.floci.services.amazonmq.model.BrokerState;
import io.github.hectorvent.floci.services.amazonmq.model.MqUser;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.function.Function;

/** Reads queue depth without receiving, acknowledging or otherwise modifying messages. */
@ApplicationScoped
public class RabbitMqQueueProbe {
    private final Instance<AmazonMqService> brokers;
    private final ObjectMapper mapper;
    private final HttpClient http;
    private final Function<String, String> environment;

    @Inject
    public RabbitMqQueueProbe(Instance<AmazonMqService> brokers, ObjectMapper mapper) {
        this(brokers, mapper, HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build(), System::getenv);
    }

    RabbitMqQueueProbe(Instance<AmazonMqService> brokers, ObjectMapper mapper, HttpClient http,
                       Function<String, String> environment) {
        this.brokers = brokers;
        this.mapper = mapper;
        this.http = http;
        this.environment = environment;
    }

    public boolean hasWork(WorkloadDefinition definition, RabbitMqQueue queue) {
        Broker broker;
        try {
            broker = brokers.get().describeBroker(queue.brokerId());
        } catch (AwsException e) {
            if ("NotFoundException".equals(e.getErrorCode())) {
                return false;
            }
            throw e;
        }
        AwsArnUtils.Arn arn = AwsArnUtils.parse(broker.getBrokerArn());
        if (!definition.accountId().equals(arn.accountId()) || !definition.region().equals(arn.region())
                || !"mq".equals(arn.service())) {
            throw new IllegalStateException("RabbitMQ broker does not belong to the workload's account and region");
        }
        if (!"RABBITMQ".equalsIgnoreCase(broker.getEngineType()) || broker.getBrokerState() != BrokerState.RUNNING
                || broker.getBrokerInstances().isEmpty()) {
            throw new IllegalStateException("RabbitMQ broker is not running");
        }
        String username;
        String password;
        if (queue.usernameEnv() != null) {
            username = environment.apply(queue.usernameEnv());
            password = environment.apply(queue.passwordEnv());
        } else {
            MqUser user = broker.getUsers().isEmpty() ? null : broker.getUsers().getFirst();
            username = user == null ? null : user.getUsername();
            password = user == null ? null : user.getPassword();
        }
        if (username == null || username.isBlank() || username.contains(":") || password == null || password.isEmpty()) {
            throw new IllegalStateException("RabbitMQ management credentials unavailable; configure username-env and password-env");
        }
        URI console = URI.create(broker.getBrokerInstances().getFirst().getConsoleURL());
        URI endpoint = console.resolve("/api/queues/" + segment(queue.virtualHost()) + "/" + segment(queue.queue())
                + "?columns=messages_ready,messages_unacknowledged");
        String credentials = Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
        HttpRequest request = HttpRequest.newBuilder(endpoint).timeout(Duration.ofSeconds(2))
                .header("Authorization", "Basic " + credentials).header("Accept", "application/json").GET().build();
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 404) {
                return false;
            }
            if (response.statusCode() != 200) {
                throw new IllegalStateException("RabbitMQ queue inspection returned HTTP " + response.statusCode());
            }
            JsonNode counts = mapper.readTree(response.body());
            long ready = count(counts, "messages_ready");
            long unacknowledged = count(counts, "messages_unacknowledged");
            return ready > 0 || unacknowledged > 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("RabbitMQ queue inspection interrupted", e);
        } catch (IOException e) {
            throw new IllegalStateException("RabbitMQ queue inspection failed", e);
        }
    }

    private static long count(JsonNode response, String field) {
        JsonNode count = response == null ? null : response.get(field);
        if (count == null || !count.isIntegralNumber() || !count.canConvertToLong() || count.longValue() < 0) {
            throw new IllegalStateException("RabbitMQ queue statistics are unavailable");
        }
        return count.longValue();
    }

    private static String segment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    @PreDestroy
    public void close() {
        http.shutdownNow();
    }
}
