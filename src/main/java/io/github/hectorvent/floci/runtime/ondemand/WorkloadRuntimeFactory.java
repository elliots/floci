package io.github.hectorvent.floci.runtime.ondemand;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.hectorvent.floci.core.common.AwsArnUtils;
import io.github.hectorvent.floci.core.common.RequestScopes;
import io.github.hectorvent.floci.services.ec2.Ec2Service;
import io.github.hectorvent.floci.services.ec2.model.Instance;
import io.github.hectorvent.floci.services.ecs.EcsService;
import io.github.hectorvent.floci.services.ecs.model.EcsServiceModel;
import io.github.hectorvent.floci.services.ecs.model.EcsTask;
import io.github.hectorvent.floci.services.ecs.model.TaskStatus;
import io.github.hectorvent.floci.services.ecs.model.UpdateServiceRequest;
import io.github.hectorvent.floci.services.eks.EksClusterManager;
import io.github.hectorvent.floci.services.eks.EksService;
import io.github.hectorvent.floci.services.eks.model.Cluster;
import io.github.hectorvent.floci.services.eks.model.ClusterStatus;
import io.github.hectorvent.floci.services.lambda.launcher.kubernetes.KubernetesApiClient;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

@ApplicationScoped
public class WorkloadRuntimeFactory {
    private final Ec2Service ec2;
    private final KubernetesApiClient kubernetes;
    private final EcsService ecs;
    private final EksService eks;
    private final EksClusterManager clusters;
    private record ClusterClient(String containerId, KubernetesApiClient client) {}

    private final Map<String, ClusterClient> clusterClients = new ConcurrentHashMap<>();

    public WorkloadRuntimeFactory(Ec2Service ec2, KubernetesApiClient kubernetes) {
        this(ec2, kubernetes, null, null, null);
    }

    WorkloadRuntimeFactory(Ec2Service ec2, KubernetesApiClient kubernetes, EksService eks, EksClusterManager clusters) {
        this(ec2, kubernetes, eks, clusters, null);
    }

    @Inject
    public WorkloadRuntimeFactory(Ec2Service ec2, KubernetesApiClient kubernetes, EksService eks,
                                  EksClusterManager clusters, EcsService ecs) {
        this.ec2 = ec2;
        this.ecs = ecs;
        this.kubernetes = kubernetes;
        this.eks = eks;
        this.clusters = clusters;
    }

    @PreDestroy
    public void closeManagedClients() {
        clusterClients.values().forEach(cached -> cached.client().close());
        clusterClients.clear();
    }

    public WorkloadRuntime create(WorkloadDefinition definition) {
        return switch (definition.type()) {
            case EC2 -> new Ec2Runtime(definition);
            case ECS -> new EcsRuntime(definition);
            case KUBERNETES -> new KubernetesRuntime(definition);
        };
    }

    private final class Ec2Runtime implements WorkloadRuntime {
        private final WorkloadDefinition definition;

        private Ec2Runtime(WorkloadDefinition definition) {
            this.definition = definition;
        }

        private Instance instance() {
            return ec2.findInstanceForAccount(definition.accountId(), definition.region(), definition.target())
                    .orElseThrow(() -> new IllegalStateException("EC2 instance does not exist: " + definition.target()));
        }

        @Override
        public boolean isRunning() {
            return "running".equals(instance().getState().getName());
        }

        @Override
        public void start(Duration timeout) throws Exception {
            String state = instance().getState().getName();
            if ("pending".equals(state)) {
                awaitState("running", timeout);
            } else if ("stopping".equals(state)) {
                awaitState("stopped", timeout);
            }
            if (!isRunning()) {
                RequestScopes.runAs(definition.accountId(), definition.region(),
                        () -> ec2.startInstances(definition.region(), List.of(definition.target())));
            }
            awaitState("running", timeout);
        }

        @Override
        public void stop(Duration timeout) throws Exception {
            String state = instance().getState().getName();
            if ("pending".equals(state)) {
                awaitState("running", timeout);
            } else if ("stopping".equals(state)) {
                awaitState("stopped", timeout);
            }
            if ("stopped".equals(instance().getState().getName())) {
                return;
            }
            RequestScopes.runAs(definition.accountId(), definition.region(),
                    () -> ec2.stopInstances(definition.region(), List.of(definition.target())));
            awaitState("stopped", timeout);
        }

        private void awaitState(String desired, Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            boolean interrupted = false;
            while (!desired.equals(instance().getState().getName())) {
                String state = instance().getState().getName();
                if (interrupted && !"pending".equals(state) && !"stopping".equals(state)) {
                    break;
                }
                if (System.nanoTime() >= deadline) {
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                    throw new IllegalStateException("Timed out waiting for EC2 state " + desired);
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException expected) {
                    // The AWS lifecycle already accepted the operation. Drain it before reset can clear its record.
                    interrupted = true;
                }
            }
            if (interrupted) {
                throw new InterruptedException("Interrupted waiting for EC2 lifecycle completion");
            }
        }

        @Override
        public URI backend() {
            if (definition.host() == null && definition.readinessPath() == null) {
                return definition.backendUrl();
            }
            if (definition.backendUrl() != null) {
                return definition.backendUrl();
            }
            String address = instance().getContainerBridgeIp();
            if (address == null || address.isBlank()) {
                throw new IllegalStateException("EC2 instance has no reachable container address: " + definition.target());
            }
            return URI.create("http://" + (address.contains(":") ? "[" + address + "]" : address)
                    + ":" + definition.backendPort());
        }
    }

    private final class EcsRuntime implements WorkloadRuntime {
        private final WorkloadDefinition definition;

        private EcsRuntime(WorkloadDefinition definition) {
            this.definition = definition;
        }

        private EcsServiceModel service() {
            EcsServiceModel service = RequestScopes.callAs(definition.accountId(), definition.region(),
                    () -> ecs.describeServices(definition.clusterName(), List.of(definition.target()), definition.region()))
                    .stream().findFirst().orElseThrow(() -> new IllegalStateException(
                            "ECS service does not exist: " + definition.target()));
            if (!EcsService.STATUS_ACTIVE.equals(service.getStatus())
                    || !EcsService.DEFAULT_SCHEDULING_STRATEGY.equals(service.getSchedulingStrategy())
                    || !EcsService.DEFAULT_DEPLOYMENT_CONTROLLER.equals(service.getDeploymentController())) {
                throw new IllegalStateException("On-demand ECS requires an active REPLICA service with ECS deployments");
            }
            return service;
        }

        private List<EcsTask> tasks(EcsServiceModel service) {
            return RequestScopes.callAs(definition.accountId(), definition.region(), () -> ecs.tasksForService(service));
        }

        private void scale(int count) {
            service();
            UpdateServiceRequest request = new UpdateServiceRequest();
            request.setCluster(definition.clusterName());
            request.setService(definition.target());
            request.setDesiredCount(count);
            RequestScopes.runAs(definition.accountId(), definition.region(), () -> ecs.updateService(request, definition.region()));
        }

        @Override
        public boolean isRunning() {
            EcsServiceModel service = service();
            return service.getDesiredCount() > 0 || tasks(service).stream()
                    .anyMatch(task -> !TaskStatus.STOPPED.name().equals(task.getLastStatus()));
        }

        @Override
        public boolean hasActivity() {
            return tasks(service()).stream().filter(task -> !TaskStatus.STOPPED.name().equals(task.getLastStatus()))
                    .anyMatch(EcsTask::hasActiveProtection);
        }

        @Override
        public void start(Duration timeout) throws Exception {
            scale(definition.replicas());
            await(() -> {
                EcsServiceModel service = service();
                return tasks(service).stream().filter(task -> TaskStatus.RUNNING.name().equals(task.getLastStatus()))
                        .filter(task -> TaskStatus.RUNNING.name().equals(task.getDesiredStatus()))
                        .filter(task -> service.getDeploymentId().equals(task.getDeploymentId()))
                        .filter(task -> !EcsService.HEALTH_STATUS_UNHEALTHY.equals(task.getHealthStatus()))
                        .filter(task -> task.getContainers() == null || task.getContainers().stream()
                                .allMatch(container -> container.getHealthStatus() == null
                                        || EcsService.HEALTH_STATUS_HEALTHY.equals(container.getHealthStatus())))
                        .count() >= definition.replicas();
            }, timeout, "starting ECS service " + definition.target());
        }

        @Override
        public void stop(Duration timeout) throws Exception {
            scale(0);
            await(() -> {
                EcsServiceModel service = service();
                return service.getDesiredCount() == 0 && tasks(service).stream()
                        .allMatch(task -> TaskStatus.STOPPED.name().equals(task.getLastStatus()));
            }, timeout, "stopping ECS service " + definition.target());
        }

        @Override
        public URI backend() {
            return definition.backendUrl();
        }
    }

    private final class KubernetesRuntime implements WorkloadRuntime {
        private final WorkloadDefinition definition;

        private KubernetesRuntime(WorkloadDefinition definition) {
            this.definition = definition;
        }

        private JsonNode deployment() {
            return client().getDeployment(definition.namespace(), definition.target())
                    .orElseThrow(() -> new IllegalStateException("Kubernetes deployment does not exist: " + definition.target()));
        }

        private KubernetesApiClient client() {
            if (definition.clusterName() == null) {
                return kubernetes;
            }
            Cluster cluster = eks.findAuthenticationCluster(definition.accountId(), definition.clusterName())
                    .orElseThrow(() -> new IllegalStateException("EKS cluster does not exist: " + definition.clusterName()));
            if (!definition.region().equals(AwsArnUtils.parse(cluster.getArn()).region())
                    || cluster.getStatus() != ClusterStatus.ACTIVE || cluster.getInternalEndpoint() == null
                    || cluster.getContainerId() == null) {
                throw new IllegalStateException("EKS cluster is not active in the configured region: " + definition.clusterName());
            }
            String key = definition.accountId() + "/" + definition.region() + "/" + definition.clusterName();
            return clusterClients.compute(key, (ignored, cached) -> {
                if (cached != null && cached.containerId().equals(cluster.getContainerId())) {
                    return cached;
                }
                KubernetesApiClient client = KubernetesApiClient.fromKubeconfig(
                        clusters.readKubeconfig(cluster), URI.create(cluster.getInternalEndpoint()));
                if (cached != null) {
                    cached.client().close();
                }
                return new ClusterClient(cluster.getContainerId(), client);
            }).client();
        }

        @Override
        public boolean isRunning() {
            return deployment().path("spec").path("replicas").asInt(1) > 0;
        }

        @Override
        public void start(Duration timeout) throws Exception {
            client().scaleDeployment(definition.namespace(), definition.target(), definition.replicas());
            await(() -> {
                JsonNode node = deployment();
                return node.path("status").path("observedGeneration").asLong()
                        >= node.path("metadata").path("generation").asLong()
                        && node.path("status").path("availableReplicas").asInt() >= definition.replicas();
            }, timeout, "starting deployment " + definition.target());
        }

        @Override
        public void stop(Duration timeout) throws Exception {
            JsonNode selector = deployment().path("spec").path("selector");
            Map<String, String> labels = new LinkedHashMap<>();
            for (Map.Entry<String, JsonNode> label : selector.path("matchLabels").properties()) {
                labels.put(label.getKey(), label.getValue().asText());
            }
            client().scaleDeployment(definition.namespace(), definition.target(), 0);
            await(() -> {
                JsonNode node = deployment();
                return node.path("spec").path("replicas").asInt(1) == 0
                        && node.path("status").path("observedGeneration").asLong()
                        >= node.path("metadata").path("generation").asLong()
                        && node.path("status").path("replicas").asInt() == 0
                        && node.path("status").path("terminatingReplicas").asInt() == 0
                        && client().listPods(definition.namespace(), labels).stream()
                        .noneMatch(pod -> matchesExpressions(selector, pod.path("metadata").path("labels")));
            }, timeout, "stopping deployment " + definition.target());
        }

        private static boolean matchesExpressions(JsonNode selector, JsonNode labels) {
            for (JsonNode expression : selector.path("matchExpressions")) {
                String key = expression.path("key").asText();
                boolean present = labels.has(key);
                boolean listed = false;
                for (JsonNode value : expression.path("values")) {
                    listed |= present && value.asText().equals(labels.path(key).asText());
                }
                boolean matches = switch (expression.path("operator").asText()) {
                    case "In" -> listed;
                    case "NotIn" -> !listed;
                    case "Exists" -> present;
                    case "DoesNotExist" -> !present;
                    default -> throw new IllegalStateException("Unsupported deployment label selector operator");
                };
                if (!matches) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public URI backend() {
            return definition.backendUrl();
        }
    }

    private static void await(BooleanSupplier condition, Duration timeout, String action) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("Timed out " + action);
            }
            Thread.sleep(100);
        }
    }
}
