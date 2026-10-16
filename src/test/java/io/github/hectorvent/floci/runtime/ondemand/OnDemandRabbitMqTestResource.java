package io.github.hectorvent.floci.runtime.ondemand;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class OnDemandRabbitMqTestResource implements QuarkusTestResourceLifecycleManager {
    private Path file;

    @Override
    public Map<String, String> start() {
        try {
            file = Files.createTempFile("floci-rabbitmq-workers-", ".yaml");
            Files.writeString(file, "workloads:\n" + worker("consumer", "000000000000", "us-east-1")
                    + worker("other-account", "111111111111", "us-east-1")
                    + worker("other-region", "000000000000", "us-west-2"));
            return Map.of("floci.on-demand.enabled", "true", "floci.on-demand.config-file", file.toString(),
                    "floci.on-demand.gateway-port", "0", "floci.on-demand.reconcile-interval-millis", "100");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String worker(String id, String account, String region) {
        return """
                  %s:
                    account-id: '%s'
                    region: %s
                    idle-timeout-seconds: 1
                    startup-timeout-seconds: 5
                    queues: [fallback-incoming]
                    rabbitmq-queues:
                      - broker-id: b-probe
                        queue: jobs
                    runtime:
                      type: kubernetes
                      deployment: %s
                """.formatted(id, account, region, id);
    }

    @Override
    public void stop() {
        if (file != null) {
            try {
                Files.deleteIfExists(file);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
