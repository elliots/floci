package io.github.hectorvent.floci.services.eks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.core.common.docker.ContainerExec;
import io.github.hectorvent.floci.services.eks.model.Cluster;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class EksIrsaWebhookTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ROLE = "arn:aws:iam::123456789012:role/workloads/payments";
    private static final String PREFIX = EksIrsaWebhook.ANNOTATION_PREFIX;

    @Test
    void projectsStsIdentityForApplicationsAndInitContainers() {
        Map<String, Object> pod = Map.of("spec", Map.of("containers", List.of(Map.of("name", "app")),
                "initContainers", List.of(Map.of("name", "init", "restartPolicy", "Always"))));
        List<Map<String, Object>> patch = EksIrsaWebhook.buildPatch(pod,
                Map.of(PREFIX + "role-arn", ROLE, PREFIX + "sts-regional-endpoints", "true"), "us-west-2");
        JsonNode volume = value(patch, "/spec/volumes").get(0);
        assertEquals("aws-iam-token", volume.path("name").asText());
        JsonNode token = volume.path("projected").path("sources").get(0).path("serviceAccountToken");
        assertEquals("sts.amazonaws.com", token.path("audience").asText());
        assertEquals(86400, token.path("expirationSeconds").asInt());
        assertEquals("token", token.path("path").asText());
        for (String field : List.of("containers", "initContainers")) {
            JsonNode env = value(patch, "/spec/" + field + "/0/env");
            assertEquals(ROLE, named(env, "AWS_ROLE_ARN").path("value").asText());
            assertEquals(EksIrsaWebhook.MOUNT_PATH + "/token",
                    named(env, "AWS_WEB_IDENTITY_TOKEN_FILE").path("value").asText());
            assertEquals("us-west-2", named(env, "AWS_REGION").path("value").asText());
            assertEquals("regional", named(env, "AWS_STS_REGIONAL_ENDPOINTS").path("value").asText());
            assertTrue(value(patch, "/spec/" + field + "/0/volumeMounts").get(0).path("readOnly").asBoolean());
            assertNull(named(env, "AWS_ACCESS_KEY_ID"));
            assertNull(named(env, "AWS_SECRET_ACCESS_KEY"));
        }
    }

    @Test
    void preservesExplicitCredentialAndRegionPairsAndExistingMounts() {
        Map<String, Object> container = Map.of("name", "app", "env", List.of(
                Map.of("name", "AWS_WEB_IDENTITY_TOKEN_FILE", "value", "/custom/token"),
                Map.of("name", "AWS_DEFAULT_REGION", "valueFrom", Map.of("fieldRef", Map.of("fieldPath", "metadata.name"))),
                Map.of("name", "AWS_STS_REGIONAL_ENDPOINTS", "value", "legacy")),
                "volumeMounts", List.of(Map.of("name", "aws-iam-token", "mountPath", "/custom")));
        Map<String, Object> pod = Map.of("spec", Map.of("containers", List.of(container),
                "volumes", List.of(Map.of("name", "aws-iam-token", "emptyDir", Map.of()))));
        assertTrue(EksIrsaWebhook.buildPatch(pod,
                Map.of(PREFIX + "role-arn", ROLE, PREFIX + "sts-regional-endpoints", "true"), "us-west-2").isEmpty());
    }

    @Test
    void appliesServiceAccountAudienceAndPodExpirationAndContainerOptOut() {
        Map<String, Object> pod = Map.of("metadata", Map.of("annotations", Map.of(
                PREFIX + "token-expiration", "1200", PREFIX + "skip-containers", "\"skip\",init")),
                "spec", Map.of("containers", List.of(Map.of("name", "skip"), Map.of("name", "app")),
                        "initContainers", List.of(Map.of("name", "init"))));
        List<Map<String, Object>> patch = EksIrsaWebhook.buildPatch(pod,
                Map.of(PREFIX + "role-arn", ROLE, PREFIX + "audience", "custom-audience",
                        PREFIX + "token-expiration", "3600"), "us-east-1");
        JsonNode token = value(patch, "/spec/volumes").get(0).path("projected").path("sources").get(0)
                .path("serviceAccountToken");
        assertEquals("custom-audience", token.path("audience").asText());
        assertEquals(1200, token.path("expirationSeconds").asInt());
        assertTrue(patch.stream().noneMatch(op -> op.get("path").toString().startsWith("/spec/initContainers/")));
        assertTrue(patch.stream().noneMatch(op -> op.get("path").toString().startsWith("/spec/containers/0/")));
        assertNotNull(value(patch, "/spec/containers/1/env"));
    }

    @Test
    void invalidExpirationFallsBackAndShortExpirationUsesKubernetesMinimum() {
        Map<String, Object> pod = Map.of("spec", Map.of("containers", List.of(Map.of("name", "app"))));
        for (Map.Entry<String, Long> sample : Map.of("invalid", 86400L, "0", 600L, "7200", 7200L).entrySet()) {
            List<Map<String, Object>> patch = EksIrsaWebhook.buildPatch(pod,
                    Map.of(PREFIX + "role-arn", ROLE, PREFIX + "token-expiration", sample.getKey()), "");
            assertEquals(sample.getValue(), value(patch, "/spec/volumes").get(0).path("projected")
                    .path("sources").get(0).path("serviceAccountToken").path("expirationSeconds").asLong());
        }
        assertTrue(EksIrsaWebhook.buildPatch(pod, Map.of(), "us-east-1").isEmpty());
    }

    @Test
    void readsTheCurrentNamespacedServiceAccountFromTheOwnedCluster() {
        EksClusterManager manager = mock(EksClusterManager.class);
        EksIrsaWebhook webhook = new EksIrsaWebhook(manager, MAPPER);
        Cluster cluster = new Cluster();
        cluster.setContainerId("owned-cluster");
        cluster.setArn("arn:aws-cn:eks:cn-north-1:123456789012:cluster/example");
        when(manager.execInContainerForResult(eq("owned-cluster"), any(), eq(7))).thenReturn(
                new ContainerExec.Result(0, """
                        {"metadata":{"name":"app","namespace":"team","annotations":{
                          "eks.amazonaws.com/role-arn":"arn:aws-cn:iam::123456789012:role/first"}}}
                        """, "", false),
                new ContainerExec.Result(0, "{\"metadata\":{\"name\":\"app\",\"namespace\":\"team\"}}", "", false));
        Map<String, Object> pod = Map.of("spec", Map.of("containers", List.of(Map.of("name", "app"))));
        assertEquals("cn-north-1", named(value(webhook.patch(cluster, "team", "app", pod),
                "/spec/containers/0/env"), "AWS_REGION").path("value").asText());
        assertTrue(webhook.patch(cluster, "team", "app", pod).isEmpty(), "Removed annotations must not remain cached");
        ArgumentCaptor<String[]> command = ArgumentCaptor.forClass(String[].class);
        verify(manager, times(2)).execInContainerForResult(eq("owned-cluster"), command.capture(), eq(7));
        assertArrayEquals(new String[]{"kubectl", "--request-timeout=5s", "get", "serviceaccount", "app",
                "--namespace", "team", "--output=json", "--ignore-not-found"}, command.getValue());
        assertTrue(webhook.patch(cluster, "team", "--all", pod).isEmpty());
        verifyNoMoreInteractions(manager);
    }

    @Test
    void rejectsMismatchedLookupAndNeverLogsKubernetesDiagnostics() {
        EksClusterManager manager = mock(EksClusterManager.class);
        EksIrsaWebhook webhook = new EksIrsaWebhook(manager, MAPPER);
        Cluster cluster = new Cluster();
        cluster.setContainerId("owned-cluster");
        for (ContainerExec.Result result : List.of(
                new ContainerExec.Result(0, "{\"metadata\":{\"name\":\"other\",\"namespace\":\"team\"}}", "", false),
                new ContainerExec.Result(1, "private-output", "private-error", false),
                new ContainerExec.Result(0, "private-invalid-json", "", false),
                new ContainerExec.Result(-1, "", "private-timeout", true))) {
            when(manager.execInContainerForResult(any(), any(), anyInt())).thenReturn(result);
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> webhook.patch(cluster, "team", "app", Map.of()));
            assertFalse(failure.toString().contains("private"));
            assertNull(failure.getCause());
        }
    }

    private static JsonNode value(List<Map<String, Object>> patch, String path) {
        return MAPPER.valueToTree(patch.stream().filter(op -> path.equals(op.get("path")))
                .findFirst().orElseThrow().get("value"));
    }

    private static JsonNode named(JsonNode values, String name) {
        for (JsonNode value : values) {
            if (name.equals(value.path("name").asText())) {
                return value;
            }
        }
        return null;
    }
}
