package io.github.hectorvent.floci.runtime.ondemand;

import java.net.URI;
import java.time.Duration;
import java.util.List;

public record WorkloadDefinition(String id, boolean enabled, String accountId, String region,
                                 String host, RuntimeType type, String target, String namespace,
                                 int replicas, URI backendUrl, int backendPort, Duration idleTimeout,
                                 Duration startupTimeout, List<String> queues, List<HealthRequest> healthRequests,
                                 HealthResponse sleepingHealth, String readinessPath, int readinessStatus,
                                 String clusterName, BackendProtocol backendProtocol) {
    public enum RuntimeType { KUBERNETES, EC2, ECS }

    public enum BackendProtocol { HTTP1, HTTP2 }

    public record HealthRequest(String method, String path) {}

    public record HealthResponse(int status, String body, String contentType) {}

    public boolean isHealthRequest(String method, String path) {
        return healthRequests.contains(new HealthRequest(method, path));
    }
}
