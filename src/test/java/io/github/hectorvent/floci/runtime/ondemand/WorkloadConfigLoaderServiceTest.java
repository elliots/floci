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

import static org.junit.jupiter.api.Assertions.*;

class WorkloadConfigLoaderServiceTest {
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
        Path file = directory.resolve("workloads.yaml");
        Files.writeString(file, CONFIG);
        WorkloadDefinition definition = WorkloadConfigLoader.load(file, "123456789012", "cn-north-1").getFirst();
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
    void acceptsEc2WithDynamicBackendAndRejectsInvalidQueueNames() throws Exception {
        List<WorkloadDefinition> definitions = parse("""
                workloads:
                  legacy:
                    host: legacy.test
                    enabled: false
                    runtime:
                      type: ec2
                      instance-id: i-12345678
                      backend-port: 9000
                """);
        assertNull(definitions.getFirst().backendUrl());
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
        assertThrows(IllegalArgumentException.class, () -> parse(CONFIG.replace("replicas:", "backend-port:")
                .replace("      deployment: orders", "      deployment: orders\n      instance-id: i-12345678")));
    }

    @Test
    void namedFlociClustersCanOwnTheSameDeploymentNameInDifferentAccounts() throws Exception {
        String first = CONFIG.replace("      deployment: orders", "      cluster-name: managed\n      deployment: orders");
        List<WorkloadDefinition> definitions = parse(first + """
                  other:
                    account-id: '111111111111'
                    queues: [other-events]
                    runtime:
                      type: kubernetes
                      cluster-name: managed
                      namespace: integrations
                      deployment: orders
                """);
        assertEquals(2, definitions.size());
        assertEquals("managed", definitions.getFirst().clusterName());
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
        return WorkloadConfigLoader.parse(new ObjectMapper(new YAMLFactory()).readTree(yaml), "000000000000", "us-east-1");
    }
}
