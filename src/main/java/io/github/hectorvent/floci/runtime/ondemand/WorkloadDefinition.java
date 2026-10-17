package io.github.hectorvent.floci.runtime.ondemand;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public record WorkloadDefinition(String id, boolean enabled, String accountId, String region,
                                 String host, RuntimeType type, String target, String namespace,
                                 int replicas, URI backendUrl, int backendPort, Duration idleTimeout,
                                 Duration startupTimeout, List<String> queues, List<HealthRequest> healthRequests,
                                 HealthResponse sleepingHealth, String readinessPath, int readinessStatus,
                                 String clusterName, BackendProtocol backendProtocol,
                                 List<RabbitMqQueue> rabbitMqQueues, MicrosandboxSpec microsandbox, KubernetesSpec kubernetes) {
    public WorkloadDefinition(String id, boolean enabled, String accountId, String region,
                              String host, RuntimeType type, String target, String namespace,
                              int replicas, URI backendUrl, int backendPort, Duration idleTimeout,
                              Duration startupTimeout, List<String> queues, List<HealthRequest> healthRequests,
                              HealthResponse sleepingHealth, String readinessPath, int readinessStatus,
                              String clusterName, BackendProtocol backendProtocol, List<RabbitMqQueue> rabbitMqQueues,
                              MicrosandboxSpec microsandbox) {
        this(id, enabled, accountId, region, host, type, target, namespace, replicas, backendUrl, backendPort,
                idleTimeout, startupTimeout, queues, healthRequests, sleepingHealth, readinessPath,
                readinessStatus, clusterName, backendProtocol, rabbitMqQueues, microsandbox,
                new KubernetesSpec(1, KubernetesRouting.POD));
    }
    public WorkloadDefinition(String id, boolean enabled, String accountId, String region,
                              String host, RuntimeType type, String target, String namespace,
                              int replicas, URI backendUrl, int backendPort, Duration idleTimeout,
                              Duration startupTimeout, List<String> queues, List<HealthRequest> healthRequests,
                              HealthResponse sleepingHealth, String readinessPath, int readinessStatus,
                              String clusterName, BackendProtocol backendProtocol, List<RabbitMqQueue> rabbitMqQueues) {
        this(id, enabled, accountId, region, host, type, target, namespace, replicas, backendUrl, backendPort,
                idleTimeout, startupTimeout, queues, healthRequests, sleepingHealth, readinessPath,
                readinessStatus, clusterName, backendProtocol, rabbitMqQueues, null);
    }

    public WorkloadDefinition(String id, boolean enabled, String accountId, String region,
                              String host, RuntimeType type, String target, String namespace,
                              int replicas, URI backendUrl, int backendPort, Duration idleTimeout,
                              Duration startupTimeout, List<String> queues, List<HealthRequest> healthRequests,
                              HealthResponse sleepingHealth, String readinessPath, int readinessStatus,
                              String clusterName, BackendProtocol backendProtocol) {
        this(id, enabled, accountId, region, host, type, target, namespace, replicas, backendUrl, backendPort,
                idleTimeout, startupTimeout, queues, healthRequests, sleepingHealth, readinessPath,
                readinessStatus, clusterName, backendProtocol, List.of());
    }

    public enum KubernetesRouting { POD, SERVICE }

    public record KubernetesSpec(int wakeReplicas, KubernetesRouting backendRouting) {
        public KubernetesSpec {
            if (wakeReplicas < 1 || wakeReplicas > 1000 || backendRouting == null) {
                throw new IllegalArgumentException("Invalid Kubernetes activation policy");
            }
        }
    }

    public record MicrosandboxSpec(String image, List<String> command, Map<String, String> environment,
                                    int cpus, int memoryMib, int rootDiskMib) {
        public MicrosandboxSpec {
            command = List.copyOf(command);
            environment = Map.copyOf(environment);
        }

        @Override
        public String toString() {
            return "MicrosandboxSpec[image=" + image + ", command=<redacted>, environment=<redacted>, cpus="
                    + cpus + ", memoryMib=" + memoryMib + ", rootDiskMib=" + rootDiskMib + "]";
        }
    }

    public record RabbitMqQueue(String brokerId, String virtualHost, String queue,
                                String usernameEnv, String passwordEnv) {}

    public boolean hasQueueTriggers() {
        return !queues.isEmpty() || !rabbitMqQueues.isEmpty();
    }

    public enum RuntimeType { KUBERNETES, ECS, MICROSANDBOX }

    public enum BackendProtocol { HTTP1, HTTP2 }

    public record HealthRequest(String method, String path) {}

    public record HealthResponse(int status, String body, String contentType) {}

    public boolean isHealthRequest(String method, String path) {
        return healthRequests.contains(new HealthRequest(method, path));
    }
}
