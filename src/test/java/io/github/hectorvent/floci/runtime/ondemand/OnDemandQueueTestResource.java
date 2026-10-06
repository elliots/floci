package io.github.hectorvent.floci.runtime.ondemand;

import com.sun.net.httpserver.HttpServer;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class OnDemandQueueTestResource implements QuarkusTestResourceLifecycleManager {
    private HttpServer backend;
    private Path config;

    @Override
    public Map<String, String> start() {
        try {
            backend = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            backend.createContext("/ready", exchange -> {
                exchange.sendResponseHeaders(200, -1);
                exchange.close();
            });
            backend.start();
            config = Files.createTempFile("floci-on-demand-queues-", ".yaml");
            String origin = "http://127.0.0.1:" + backend.getAddress().getPort();
            Files.writeString(config, "workloads:\n" + workload("orders", "000000000000", "us-east-1", origin)
                    + workload("other-account", "111111111111", "us-east-1", origin)
                    + workload("china", "000000000000", "cn-north-1", origin));
            return Map.of("floci.on-demand.enabled", "true", "floci.on-demand.config-file", config.toString(),
                    "floci.on-demand.gateway-port", "0", "floci.on-demand.reconcile-interval-millis", "100");
        } catch (IOException e) {
            stop();
            throw new UncheckedIOException(e);
        }
    }

    private static String workload(String id, String account, String region, String origin) {
        return """
                  %s:
                    host: %s.test
                    account-id: '%s'
                    region: %s
                    idle-timeout-seconds: 1
                    startup-timeout-seconds: 5
                    queues: [on-demand-incoming]
                    runtime:
                      type: kubernetes
                      deployment: %s
                      backend-url: %s
                """.formatted(id, id, account, region, id, origin);
    }

    @Override
    public void stop() {
        if (backend != null) {
            backend.stop(0);
        }
        if (config != null) {
            try {
                Files.deleteIfExists(config);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
