package io.github.hectorvent.floci.runtime.ondemand;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.RuntimeType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OnDemandConfigLoaderServiceTest {
    @Test
    void nativeWorkloadsKeepWakePolicyAndUseTheAllocatedBackend() throws Exception {
        WorkloadDefinition definition = parse("""
                workloads:
                  worker:
                    host: worker.test
                    queues: [events]
                    idle-timeout-seconds: 30
                    runtime:
                      type: microsandbox
                      image: python:3.13-alpine
                      command: [python, -c, '', 'literal $VALUE']
                      environment:
                        SECRET: private-value
                        EMPTY: ''
                      backend-port: 8090
                      memory-mib: 768
                    health:
                      requests: [{method: GET, path: /health}]
                    readiness:
                      path: /ready
                """).getFirst();
        assertEquals(RuntimeType.MICROSANDBOX, definition.type());
        assertEquals("worker", definition.target());
        assertNull(definition.backendUrl());
        assertEquals(8090, definition.backendPort());
        assertEquals("/ready", definition.readinessPath());
        assertEquals(Duration.ofSeconds(30), definition.idleTimeout());
        assertTrue(definition.isHealthRequest("GET", "/health"));
        assertEquals(List.of("events"), definition.queues());
        assertEquals(List.of("python", "-c", "", "literal $VALUE"), definition.microsandbox().command());
        assertEquals(Map.of("SECRET", "private-value", "EMPTY", ""), definition.microsandbox().environment());
        assertEquals(1, definition.microsandbox().cpus());
        assertEquals(768, definition.microsandbox().memoryMib());
        assertEquals(4096, definition.microsandbox().rootDiskMib());
        assertFalse(definition.toString().contains("private-value"));
        assertFalse(definition.toString().contains("literal $VALUE"));
    }

    @Test
    void nativeQueueConsumersNeedNoHttpListenerOrReadinessEndpoint() throws Exception {
        WorkloadDefinition definition = parse("""
                workloads:
                  worker:
                    queues: [events]
                    runtime: {type: microsandbox, image: python:3.13-alpine}
                """).getFirst();
        assertNull(definition.host());
        assertNull(definition.readinessPath());
        assertEquals(List.of(), definition.microsandbox().command());
        assertEquals(Map.of(), definition.microsandbox().environment());
    }

    @Test
    void nativeRuntimeRequiresImageAndRejectsMalformedGuestConfiguration() {
        String template = """
                workloads:
                  worker:
                    queues: [events]
                    runtime: {type: microsandbox, %s}
                """;
        for (String fields : List.of("command: [python]", "image: ''", "image: 'bad image'",
                "image: app:1, command: command-string", "image: app:1, command: [true]",
                "image: app:1, environment: [TOKEN]", "image: app:1, environment: {TOKEN: 12}",
                "image: app:1, environment: {bad-name: secret}", "image: app:1, cpus: 0",
                "image: app:1, memory-mib: 0", "image: app:1, root-disk-mib: 0",
                "image: app:1, backend-url: 'http://external:8080'")) {
            assertThrows(IllegalArgumentException.class, () -> parse(template.formatted(fields)));
        }
    }

    @Test
    void malformedYamlDoesNotExposeEnvironmentSecretsInTheException(@TempDir Path directory) throws Exception {
        Path file = directory.resolve("invalid.yaml");
        Files.writeString(file, "workloads: {worker: {runtime: {type: microsandbox, environment: [secret-value}}\n");
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> OnDemandConfigLoader.load(file, "000000000000", "us-east-1"));
        assertFalse(failure.toString().contains("secret-value"));
        assertNull(failure.getCause());
    }

    @Test
    void rejectsNetworkConfigurationInTheOnDemandFile() throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        assertThrows(IllegalArgumentException.class, () -> OnDemandConfigLoader.parse(
                mapper.readTree("workloads: {}\nnetwork: {}"), "000000000000", "us-east-1"));
    }

    @Test
    void acceptsRabbitMqOnlyKubernetesWorkersAndCredentialEnvironmentNames() throws Exception {
        WorkloadDefinition definition = parse("""
                workloads:
                  consumer:
                    rabbitmq-queues:
                      - broker-id: b-example
                        queue: jobs/priority
                        username-env: MQ_USER
                        password-env: MQ_PASSWORD
                    runtime:
                      type: kubernetes
                      deployment: worker
                """).getFirst();
        assertEquals(RuntimeType.KUBERNETES, definition.type());
        assertNull(definition.host());
        assertNull(definition.readinessPath());
        assertTrue(definition.hasQueueTriggers());
        assertEquals("/", definition.rabbitMqQueues().getFirst().virtualHost());
        assertEquals("jobs/priority", definition.rabbitMqQueues().getFirst().queue());
        assertEquals("MQ_PASSWORD", definition.rabbitMqQueues().getFirst().passwordEnv());
    }

    @Test
    void rejectsInvalidRabbitMqTriggers() {
        for (String trigger : List.of("{}", "[{}]", "[{broker-id: b-example, queue: jobs, username-env: MQ_USER}]",
                "[{broker-id: b-example, queue: jobs, password: secret}]",
                "[{broker-id: b-example, queue: jobs, username-env: bad-name, password-env: MQ_PASSWORD}]")) {
            assertThrows(IllegalArgumentException.class, () -> parse("""
                    workloads:
                      consumer:
                        runtime: {type: kubernetes, deployment: worker}
                        rabbitmq-queues: %s
                    """.formatted(trigger)), trigger);
        }
    }

    private static final String CONFIG = """
            workloads:
              orders:
                host: orders.test
                runtime:
                  type: kubernetes
                  namespace: integrations
                  deployment: orders
                  backend-url: http://orders-backend:8080
                queues: [orders-incoming]
                health:
                  requests:
                    - method: GET
                      path: /health
                  sleeping-response:
                    status: 200
                    body: '{"status":"UP"}'
                    content-type: application/json
            """;

    @Test
    void loadsExplicitWorkloadsAndDefaultsWithoutReflectingRecords(@TempDir Path directory) throws Exception {
        Path file = directory.resolve("on-demand.yaml");
        Files.writeString(file, CONFIG);
        WorkloadDefinition definition = OnDemandConfigLoader.load(file, "123456789012", "cn-north-1").getFirst();
        assertEquals("123456789012", definition.accountId());
        assertEquals("cn-north-1", definition.region());
        assertEquals(Duration.ofSeconds(120), definition.idleTimeout());
        assertEquals(List.of("orders-incoming"), definition.queues());
        assertTrue(definition.isHealthRequest("GET", "/health"));
        assertFalse(definition.isHealthRequest("POST", "/health"));
        assertEquals("{\"status\":\"UP\"}", definition.sleepingHealth().body());
    }

    @Test
    void rejectsUnknownFieldsRatherThanSilentlyIgnoringPolicyTypos() {
        assertThrows(IllegalArgumentException.class, () -> parse(CONFIG.replace("queues:", "queuez:")));
    }

    @Test
    void rejectsBackendLoopsAndMissingKubernetesBackends() {
        assertThrows(IllegalArgumentException.class,
                () -> parse(CONFIG.replace("http://orders-backend:8080", "http://orders.test:8080")));
        assertThrows(IllegalArgumentException.class,
                () -> parse(CONFIG.replace("      backend-url: http://orders-backend:8080\n", "")));
    }

    @Test
    void rejectsUnsafeOriginsAndInvalidHealthRules() {
        for (String backend : List.of("file:///tmp/app", "http://user:pass@app", "http://app/path", "http://app?x=1")) {
            assertThrows(IllegalArgumentException.class,
                    () -> parse(CONFIG.replace("http://orders-backend:8080", backend)));
        }
        assertThrows(IllegalArgumentException.class, () -> parse(CONFIG.replace("method: GET", "method: POST")));
        assertThrows(IllegalArgumentException.class, () -> parse(CONFIG.replace("path: /health", "path: /health?deep=1")));
    }

    @Test
    void acceptsDisabledWorkloadsAndRejectsInvalidQueueNames() throws Exception {
        List<WorkloadDefinition> definitions = parse(CONFIG.replace("host: orders.test", "host: orders.test\n    enabled: false"));
        assertFalse(definitions.getFirst().enabled());
        assertThrows(IllegalArgumentException.class, () -> parse(CONFIG.replace("orders-incoming", "http://queue")));
    }

    @Test
    void queueOnlyWorkersNeedNeitherAnHttpRouteNorAnHttpReadinessEndpoint() throws Exception {
        WorkloadDefinition definition = parse("""
                workloads:
                  worker:
                    queues: [events]
                    runtime:
                      type: kubernetes
                      deployment: worker
                """).getFirst();
        assertNull(definition.host());
        assertNull(definition.backendUrl());
        assertNull(definition.readinessPath());
        assertThrows(IllegalArgumentException.class, () -> parse("""
                workloads:
                  worker:
                    runtime:
                      type: kubernetes
                      deployment: worker
                """));
    }

    @Test
    void readinessCannotResolveAnAlternateOriginThroughADoubleSlashPath() {
        assertThrows(IllegalArgumentException.class, () -> parse(CONFIG + "    readiness:\n      path: //other.test/ready\n"));
    }

    @Test
    void kubernetesTargetsCannotBeOwnedTwiceThroughDifferentAwsScopes() {
        assertThrows(IllegalArgumentException.class, () -> parse(CONFIG + """
                  duplicate:
                    account-id: '111111111111'
                    region: cn-north-1
                    queues: [other-events]
                    runtime:
                      type: kubernetes
                      namespace: integrations
                      deployment: orders
                """));
    }

    @Test
    void rejectsFieldsThatBelongToAnotherRuntimeType() {
        assertThrows(IllegalArgumentException.class, () -> parse(CONFIG
                .replace("      deployment: orders", "      deployment: orders\n      image: worker:1")));
    }

    @Test
    void namedKubernetesTargetsCannotBeOwnedTwiceThroughDifferentAwsScopes() {
        String first = CONFIG.replace("      deployment: orders", "      cluster-name: managed\n      deployment: orders");
        for (String scope : List.of("account-id: '111111111111'", "region: cn-north-1")) {
            assertThrows(IllegalArgumentException.class, () -> parse(first + """
                      other:
                        %s
                        queues: [other-events]
                        runtime:
                          type: kubernetes
                          cluster-name: managed
                          namespace: integrations
                          deployment: orders
                    """.formatted(scope)), scope);
        }
    }

    @Test
    void kubernetesClusterHintsCannotAliasTheSameControllerDeployment() {
        for (String hint : List.of("managed", "another-cluster")) {
            assertThrows(IllegalArgumentException.class, () -> parse(CONFIG + """
                      other:
                        queues: [other-events]
                        runtime:
                          type: kubernetes
                          cluster-name: %s
                          namespace: integrations
                          deployment: orders
                    """.formatted(hint)), hint);
        }
    }

    @Test
    void kubernetesNamespacesCanOwnTheSameDeploymentName() throws Exception {
        List<WorkloadDefinition> definitions = parse(CONFIG + """
                  other:
                    queues: [other-events]
                    runtime:
                      type: kubernetes
                      cluster-name: managed
                      namespace: another-namespace
                      deployment: orders
                """);
        assertEquals(2, definitions.size());
        assertEquals("managed", definitions.getLast().clusterName());
    }

    @Test
    void ecsSupportsQueueOnlyServicesAndExplicitHttp2Backends() throws Exception {
        List<WorkloadDefinition> definitions = parse("""
                workloads:
                  api:
                    host: api.test
                    runtime:
                      type: ecs
                      service: My_Service
                      cluster-name: My_Cluster
                      replicas: 2
                      backend-url: http://backend.test:50051
                      backend-protocol: http2
                    readiness:
                      enabled: false
                  worker:
                    queues: [events]
                    runtime:
                      type: ecs
                      service: worker
                """);
        assertEquals(RuntimeType.ECS, definitions.getFirst().type());
        assertEquals("My_Cluster", definitions.getFirst().clusterName());
        assertEquals("HTTP2", definitions.getFirst().backendProtocol().name());
        assertEquals("default", definitions.getLast().clusterName());
        assertNull(definitions.getLast().backendUrl());
        assertNull(definitions.getFirst().readinessPath());
    }

    @Test
    void ecsTargetsAreScopedByClusterAndRejectMissingStableHttpBackends() throws Exception {
        String yaml = """
                workloads:
                  first:
                    queues: [events]
                    runtime: {type: ecs, service: worker, cluster-name: one}
                  second:
                    queues: [events]
                    runtime: {type: ecs, service: worker, cluster-name: two}
                """;
        assertEquals(2, parse(yaml).size());
        assertThrows(IllegalArgumentException.class, () -> parse(yaml.replace("cluster-name: two", "cluster-name: one")));
        assertThrows(IllegalArgumentException.class, () -> parse(yaml.replace("queues: [events]", "host: app.test")));
        assertThrows(IllegalArgumentException.class, () -> parse(CONFIG.replace("type: kubernetes", "type: kubernetes\n      backend-protocol: h2c")));
    }

    private List<WorkloadDefinition> parse(String yaml) throws Exception {
        return OnDemandConfigLoader.parse(new ObjectMapper(new YAMLFactory()).readTree(yaml), "000000000000", "us-east-1");
    }
}
