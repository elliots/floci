package io.github.hectorvent.floci.runtime.flocinode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.runtime.flocinode.NodeKubeApi.KubeApiException;
import io.github.hectorvent.floci.runtime.flocinode.NodeKubeApi.KubeNode;
import io.github.hectorvent.floci.runtime.flocinode.NodeKubeApi.Taint;
import io.github.hectorvent.floci.services.acm.CertificateGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeKubeApiServiceTest {

    private static final String TOKEN = "4f1e0c6a9d2b7e8f3a5c1d0e9b8a7f6e5d4c3b2a19087f6e5d4c3b2a1908f7e6";
    private static final String NODE_ID = "floci-node-0123456789abcdef";
    private static final String NODE_NAME = "ip-192-168-0-10.ec2.internal";
    private static final String UNSCHEDULABLE = "node.kubernetes.io/unschedulable";

    private final ObjectMapper mapper = new ObjectMapper();
    private final List<FakeKubeApi> fakes = new ArrayList<>();
    private FakeKubeApi kube;
    private NodeRecord node;
    private NodeKubeApi api;

    @BeforeEach
    void setUp() throws IOException {
        kube = fake(new FakeKubeApi(TOKEN));
        node = kube.nodeRef(NODE_ID);
        api = new NodeKubeApi(nodeId -> TOKEN, mapper);
    }

    @AfterEach
    void tearDown() {
        api.close();
        fakes.forEach(FakeKubeApi::close);
    }

    private FakeKubeApi fake(FakeKubeApi fake) {
        fakes.add(fake);
        return fake;
    }

    @Test
    void removeWorkloadsDeletesWhatAClusterPutThereAndKeepsWhatK3sMakes() {
        kube.addNamespace("shop");
        kube.addNamespace("monitoring");
        kube.addService("web");

        List<String> removed = api.removeWorkloads(node);

        assertEquals(Set.of("default", "kube-system", "kube-public", "kube-node-lease"), kube.namespaces());
        assertEquals(Set.of("kubernetes"), kube.services());
        assertTrue(removed.contains("namespace shop"));
        assertTrue(removed.contains("default service web"));
        for (String collection : List.of("/apis/apps/v1/namespaces/default/deployments",
                "/apis/apps/v1/namespaces/default/statefulsets", "/apis/batch/v1/namespaces/default/jobs",
                "/api/v1/namespaces/default/pods", "/api/v1/namespaces/default/configmaps",
                "/api/v1/namespaces/default/serviceaccounts")) {
            List<FakeKubeApi.Request> deletes = kube.requests("DELETE", collection);
            assertEquals(1, deletes.size(), collection);
            assertEquals("Bearer " + TOKEN, deletes.getFirst().authorization());
        }
        assertTrue(kube.requests("DELETE", "/api/v1/namespaces/default").isEmpty());
        assertTrue(kube.requests("DELETE", "/api/v1/namespaces/default/services/kubernetes").isEmpty());
    }

    @Test
    void removeWorkloadsDeletesTheClustersLeftoversAndKeepsWhatK3sKubernetesAndFlociMade() {
        String webhooks = "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations";
        String validating = "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations";
        String crds = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions";
        String clusterRoles = "/apis/rbac.authorization.k8s.io/v1/clusterroles";
        String volumes = "/api/v1/persistentvolumes";
        String roles = "/apis/rbac.authorization.k8s.io/v1/namespaces/default/roles";
        kube.addObject(webhooks, "floci-eks-pod-identity", Map.of("objectset.rio.cattle.io/hash", "abc"));
        kube.addObject(webhooks, "cert-manager-webhook", Map.of());
        kube.addObject(validating, "policy-enforcer", Map.of());
        kube.addObject(crds, "helmcharts.helm.cattle.io", Map.of());
        kube.addObject(crds, "certificates.cert-manager.io", Map.of());
        kube.addObject(clusterRoles, "system:coredns", Map.of());
        kube.addObject(clusterRoles, "admin", Map.of("kubernetes.io/bootstrapping", "rbac-defaults"));
        kube.addObject(clusterRoles, "cert-manager-controller", Map.of());
        kube.addObject(volumes, "pvc-shop", Map.of()).putObject("spec").putObject("claimRef")
                .put("namespace", "shop");
        kube.addObject(volumes, "pvc-system", Map.of()).putObject("spec").putObject("claimRef")
                .put("namespace", "kube-system");
        kube.addObject(roles, "app-reader", Map.of());

        List<String> removed = api.removeWorkloads(node);

        assertEquals(Set.of("floci-eks-pod-identity"), kube.objects(webhooks));
        assertEquals(Set.of(), kube.objects(validating));
        assertEquals(Set.of("helmcharts.helm.cattle.io"), kube.objects(crds));
        assertEquals(Set.of("system:coredns", "admin"), kube.objects(clusterRoles));
        assertEquals(Set.of("pvc-system"), kube.objects(volumes));
        assertEquals(Set.of(), kube.objects(roles));
        assertTrue(removed.contains("mutatingwebhookconfigurations cert-manager-webhook"), removed.toString());
        assertTrue(removed.contains("default roles app-reader"), removed.toString());
    }

    @Test
    void readsTheCaFromCacertsOnceAndSendsTheAdminToken() {
        assertEquals("ok", api.readyz(node));
        assertEquals("ok", api.readyz(node));

        assertEquals(kube.caPem(), api.caPem(node));
        List<FakeKubeApi.Request> caCerts = kube.requests("GET", "/cacerts");
        assertEquals(1, caCerts.size());
        assertNull(caCerts.getFirst().authorization());
        List<FakeKubeApi.Request> readyz = kube.requests("GET", "/readyz");
        assertEquals(2, readyz.size());
        for (FakeKubeApi.Request request : readyz) {
            assertEquals("Bearer " + TOKEN, request.authorization());
        }
    }

    @Test
    void readsAServiceAccountOrNoneWhenItDoesNotExist() throws IOException {
        kube.addServiceAccount("team", "app", Map.of("eks.amazonaws.com/role-arn", "arn:aws:iam::123456789012:role/app"));

        JsonNode account = mapper.readTree(api.serviceAccount(node, "team", "app", Duration.ofSeconds(2))
                .orElseThrow());

        assertEquals("app", account.path("metadata").path("name").asText());
        assertEquals("arn:aws:iam::123456789012:role/app",
                account.path("metadata").path("annotations").path("eks.amazonaws.com/role-arn").asText());
        assertEquals(Optional.empty(), api.serviceAccount(node, "team", "other", Duration.ofSeconds(2)));
        assertEquals("Bearer " + TOKEN,
                kube.requests("GET", "/api/v1/namespaces/team/serviceaccounts/app").getFirst().authorization());
        kube.forbidNext("GET", "/api/v1/namespaces/team/serviceaccounts/app");
        KubeApiException forbidden = assertThrows(KubeApiException.class,
                () -> api.serviceAccount(node, "team", "app", Duration.ofSeconds(2)));
        assertEquals(403, forbidden.status());
    }

    @Test
    void aServiceAccountReadThatOutlastsItsTimeoutFails() {
        kube.addServiceAccount("team", "app", Map.of());
        api.readyz(node);
        kube.delayNext("GET", "/api/v1/namespaces/team/serviceaccounts/app", Duration.ofSeconds(3));

        KubeApiException late = assertThrows(KubeApiException.class,
                () -> api.serviceAccount(node, "team", "app", Duration.ofMillis(300)));

        assertInstanceOf(HttpTimeoutException.class, late.getCause());
    }

    @Test
    void keepsTheCaPinnedWhenTheServerComesBackWithAnotherCa() throws IOException {
        String first = api.caPem(node);
        api.readyz(node);

        kube.newCertificateAuthority();

        KubeApiException refused = assertThrows(KubeApiException.class, () -> api.readyz(node));
        assertEquals(0, refused.status());
        assertThat(refused.getMessage(), containsString("GET /readyz: TLS failed"));
        assertEquals(1, kube.requests("GET", "/cacerts").size());
        assertEquals(1, kube.requests("GET", "/readyz").size());
        assertEquals(first, api.caPem(node));

        api.forget(NODE_ID);

        assertEquals("ok", api.readyz(node));
        assertEquals(kube.caPem(), api.caPem(node));
        assertThat(api.caPem(node), not(first));
    }

    @Test
    void refusesAServerWhoseCertificateTheCaOfCacertsDidNotIssue() {
        String otherCa = new CertificateGenerator().generateCaCertificate("k3s-server-ca").certificatePem();
        kube.caCerts(otherCa);

        KubeApiException refused = assertThrows(KubeApiException.class, () -> api.readyz(node));

        assertEquals(0, refused.status());
        assertThat(refused.getMessage(), containsString("TLS failed"));
        assertEquals(otherCa, api.caPem(node));
        assertTrue(kube.requests("GET", "/readyz").isEmpty());
    }

    @Test
    void refusesACertificateWithoutTheNameLocalhost() throws IOException {
        FakeKubeApi other = fake(new FakeKubeApi(TOKEN, List.of("kubernetes", "kubernetes.default")));

        KubeApiException refused = assertThrows(KubeApiException.class, () -> api.readyz(other.nodeRef(NODE_ID)));

        assertThat(refused.getMessage(), containsString("TLS failed"));
        assertTrue(other.requests("GET", "/readyz").isEmpty());
    }

    @Test
    void refusesCacertsWithoutACertificate() {
        kube.caCerts("not a certificate\n");

        KubeApiException refused = assertThrows(KubeApiException.class, () -> api.readyz(node));

        assertThat(refused.getMessage(), containsString("/cacerts"));
        assertTrue(kube.requests("GET", "/readyz").isEmpty());
    }

    @Test
    void aRefusedTokenIsA401WhoseMessageDoesNotHoldTheToken() {
        kube.token("a-different-token-of-the-node");

        KubeApiException refused = assertThrows(KubeApiException.class, () -> api.readyz(node));

        assertEquals(401, refused.status());
        assertThat(refused.getMessage(), containsString(NODE_ID + ": GET /readyz: 401: Unauthorized"));
        assertThat(refused.getMessage(), not(containsString(TOKEN)));
    }

    @Test
    void noAdminTokenFailsBeforeAnyCallWithTheToken() {
        NodeKubeApi withoutToken = new NodeKubeApi(nodeId -> {
            throw new IllegalStateException("The seed of node " + nodeId + " has no admin token");
        }, mapper);
        try {
            KubeApiException refused = assertThrows(KubeApiException.class, () -> withoutToken.readyz(node));

            assertEquals(0, refused.status());
            assertThat(refused.getMessage(), containsString("no admin token"));
            assertTrue(kube.requests("GET", "/readyz").isEmpty());
        } finally {
            withoutToken.close();
        }
    }

    @Test
    void noApiServerIsAFailureWithoutAStatus() {
        int port = kube.port();
        kube.close();

        KubeApiException refused = assertThrows(KubeApiException.class,
                () -> api.readyz(TestNodeRecords.record(NODE_ID, port, 6600, "none", "0".repeat(64), false, List.of())));

        assertEquals(0, refused.status());
        assertThat(refused.getMessage(), containsString("GET /cacerts: no answer"));
    }

    @Test
    void aReadyzThatIsNotOkIsItsStatusAndBody() {
        kube.readyz(500, "[-]etcd failed: reason withheld\nreadyz check failed");

        KubeApiException refused = assertThrows(KubeApiException.class, () -> api.readyz(node));

        assertEquals(500, refused.status());
        assertThat(refused.getMessage(), containsString("GET /readyz: 500: [-]etcd failed"));
    }

    @Test
    void readsTheVersion() {
        NodeKubeApi.Version version = api.version(node);

        assertEquals("1", version.major());
        assertEquals("35", version.minor());
        assertEquals("v1.35.0+k3s1", version.gitVersion());
    }

    @Test
    void readsANodeWithItsReadyCondition() {
        kube.addNode(NODE_NAME, "True");
        kube.changeNode(NODE_NAME, n -> ((ObjectNode) n.path("spec")).putArray("taints").addObject()
                .put("key", UNSCHEDULABLE).put("effect", "NoSchedule").put("timeAdded", "2026-10-21T00:00:00Z"));

        KubeNode read = api.node(node, NODE_NAME).orElseThrow();

        assertEquals(NODE_NAME, read.name());
        assertTrue(read.ready());
        assertEquals(FakeKubeApi.BOOT_ID, read.bootId());
        assertEquals("linux", read.labels().get("kubernetes.io/os"));
        assertEquals(List.of(new Taint(UNSCHEDULABLE, null, "NoSchedule", "2026-10-21T00:00:00Z")), read.taints());
        assertEquals(kube.kubeNode(NODE_NAME).path("metadata").path("resourceVersion").asText(), read.resourceVersion());
    }

    @Test
    void aNodeThatIsNotReadyOrMissing() {
        kube.addNode(NODE_NAME, "False");

        assertFalse(api.node(node, NODE_NAME).orElseThrow().ready());
        assertEquals(Optional.empty(), api.node(node, "ip-192-168-0-11.ec2.internal"));
    }

    @Test
    void findsARuntimeClass() {
        kube.addRuntimeClass("smolvm-pods");

        assertTrue(api.runtimeClassExists(node, "smolvm-pods"));
        assertFalse(api.runtimeClassExists(node, "kata"));
    }

    @Test
    void setsLabelsAndRecordsTheirKeysInOneMergePatch() {
        kube.addNode(NODE_NAME, "True");
        String version = kube.kubeNode(NODE_NAME).path("metadata").path("resourceVersion").asText();

        assertTrue(api.applyNodeMetadata(node, NODE_NAME, labels("team", "orders", "eks.amazonaws.com/nodegroup", "workers"),
                List.of()));

        List<FakeKubeApi.Request> patches = kube.requests("PATCH", "/api/v1/nodes/" + NODE_NAME);
        assertEquals(1, patches.size());
        assertEquals("application/merge-patch+json", patches.getFirst().headers().get("content-type"));
        assertEquals("Bearer " + TOKEN, patches.getFirst().authorization());
        JsonNode body = patches.getFirst().body();
        assertEquals(version, body.path("metadata").path("resourceVersion").asText());
        assertEquals("orders", body.path("metadata").path("labels").path("team").asText());
        assertEquals("eks.amazonaws.com/nodegroup,team",
                body.path("metadata").path("annotations").path(NodeKubeApi.LABELS_ANNOTATION).asText());
        assertFalse(body.has("spec"));

        JsonNode patched = kube.kubeNode(NODE_NAME).path("metadata");
        assertEquals("orders", patched.path("labels").path("team").asText());
        assertEquals("workers", patched.path("labels").path("eks.amazonaws.com/nodegroup").asText());
        assertEquals(NODE_NAME, patched.path("labels").path("kubernetes.io/hostname").asText());
        assertEquals("[]", patched.path("annotations").path("k3s.io/node-args").asText());
    }

    @Test
    void sendsNothingWhenTheNodeHasTheLabelsAndTaints() {
        kube.addNode(NODE_NAME, "True");
        Map<String, String> labels = labels("team", "orders");
        List<Taint> taints = List.of(new Taint("dedicated", "orders", "NoSchedule"));
        api.applyNodeMetadata(node, NODE_NAME, labels, taints);

        assertFalse(api.applyNodeMetadata(node, NODE_NAME, labels, taints));

        assertEquals(1, kube.requests("PATCH", "/api/v1/nodes/" + NODE_NAME).size());
        assertEquals(2, kube.requests("GET", "/api/v1/nodes/" + NODE_NAME).size());
    }

    @Test
    void removesOnlyTheLabelsThatFlociSet() {
        kube.addNode(NODE_NAME, "True");
        api.applyNodeMetadata(node, NODE_NAME, labels("team", "orders", "tier", "backend"), List.of());
        kube.changeNode(NODE_NAME, n -> ((ObjectNode) n.path("metadata").path("labels")).put("owner", "elliot"));

        assertTrue(api.applyNodeMetadata(node, NODE_NAME, labels("team", "payments"), List.of()));

        JsonNode body = kube.requests("PATCH", "/api/v1/nodes/" + NODE_NAME).getLast().body();
        assertEquals("payments", body.path("metadata").path("labels").path("team").asText());
        assertTrue(body.path("metadata").path("labels").path("tier").isNull());
        assertFalse(body.path("metadata").path("labels").has("owner"));
        assertEquals("team", body.path("metadata").path("annotations").path(NodeKubeApi.LABELS_ANNOTATION).asText());

        Map<String, String> after = api.node(node, NODE_NAME).orElseThrow().labels();
        assertEquals("payments", after.get("team"));
        assertEquals("elliot", after.get("owner"));
        assertThat(after, not(hasKey("tier")));

        assertTrue(api.applyNodeMetadata(node, NODE_NAME, Map.of(), List.of()));

        KubeNode cleared = api.node(node, NODE_NAME).orElseThrow();
        assertThat(cleared.labels(), not(hasKey("team")));
        assertEquals("elliot", cleared.labels().get("owner"));
        assertThat(cleared.annotations(), not(hasKey(NodeKubeApi.LABELS_ANNOTATION)));
    }

    @Test
    void setsTaintsBesideTheOnesFlociDidNotSet() {
        kube.addNode(NODE_NAME, "True");
        kube.changeNode(NODE_NAME, n -> ((ObjectNode) n.path("spec")).putArray("taints").addObject()
                .put("key", UNSCHEDULABLE).put("effect", "NoSchedule").put("timeAdded", "2026-10-21T00:00:00Z"));

        assertTrue(api.applyNodeMetadata(node, NODE_NAME, Map.of(),
                List.of(new Taint("dedicated", "orders", "NoSchedule"), new Taint("spot", null, "PreferNoSchedule"))));

        JsonNode body = kube.requests("PATCH", "/api/v1/nodes/" + NODE_NAME).getLast().body();
        assertEquals("dedicated:NoSchedule,spot:PreferNoSchedule",
                body.path("metadata").path("annotations").path(NodeKubeApi.TAINTS_ANNOTATION).asText());
        assertFalse(body.path("metadata").has("labels"));
        assertThat(taintIds(body.path("spec").path("taints")), contains(UNSCHEDULABLE + ":NoSchedule",
                "dedicated:NoSchedule", "spot:PreferNoSchedule"));
        assertEquals("2026-10-21T00:00:00Z", body.path("spec").path("taints").get(0).path("timeAdded").asText());
        assertFalse(body.path("spec").path("taints").get(2).has("value"));

        assertEquals(List.of(new Taint(UNSCHEDULABLE, null, "NoSchedule", "2026-10-21T00:00:00Z"),
                new Taint("dedicated", "orders", "NoSchedule"), new Taint("spot", null, "PreferNoSchedule")),
                api.node(node, NODE_NAME).orElseThrow().taints());
    }

    @Test
    void changesAndRemovesOnlyTheTaintsThatFlociSet() {
        kube.addNode(NODE_NAME, "True");
        api.applyNodeMetadata(node, NODE_NAME, Map.of(), List.of(new Taint("dedicated", "orders", "NoSchedule"),
                new Taint("spot", null, "PreferNoSchedule")));
        kube.changeNode(NODE_NAME, n -> ((ArrayNode) n.path("spec").path("taints")).addObject()
                .put("key", UNSCHEDULABLE).put("effect", "NoSchedule"));

        assertTrue(api.applyNodeMetadata(node, NODE_NAME, Map.of(),
                List.of(new Taint("dedicated", "payments", "NoSchedule"))));

        assertEquals(List.of(new Taint("dedicated", "payments", "NoSchedule"),
                new Taint(UNSCHEDULABLE, null, "NoSchedule")), api.node(node, NODE_NAME).orElseThrow().taints());
        assertEquals("dedicated:NoSchedule",
                api.node(node, NODE_NAME).orElseThrow().annotations().get(NodeKubeApi.TAINTS_ANNOTATION));

        assertTrue(api.applyNodeMetadata(node, NODE_NAME, Map.of(), List.of()));

        KubeNode cleared = api.node(node, NODE_NAME).orElseThrow();
        assertEquals(List.of(new Taint(UNSCHEDULABLE, null, "NoSchedule")), cleared.taints());
        assertThat(cleared.annotations(), not(hasKey(NodeKubeApi.TAINTS_ANNOTATION)));
    }

    @Test
    void readsTheNodeAgainWhenItChangedBeforeThePatch() {
        kube.addNode(NODE_NAME, "True");
        kube.conflictNextPatches(1);

        assertTrue(api.applyNodeMetadata(node, NODE_NAME, labels("team", "orders"), List.of()));

        List<FakeKubeApi.Request> patches = kube.requests("PATCH", "/api/v1/nodes/" + NODE_NAME);
        assertEquals(2, patches.size());
        assertThat(patches.get(1).body().path("metadata").path("resourceVersion").asText(),
                not(patches.get(0).body().path("metadata").path("resourceVersion").asText()));
        assertEquals("orders", api.node(node, NODE_NAME).orElseThrow().labels().get("team"));
    }

    @Test
    void aMissingNodeCannotBePatched() {
        KubeApiException refused = assertThrows(KubeApiException.class,
                () -> api.applyNodeMetadata(node, NODE_NAME, labels("team", "orders"), List.of()));

        assertEquals(404, refused.status());
        assertThat(refused.getMessage(), containsString("node " + NODE_NAME + " does not exist"));
    }

    @Test
    void givesUpAfterThreeConflicts() {
        kube.addNode(NODE_NAME, "True");
        kube.conflictNextPatches(3);

        KubeApiException refused = assertThrows(KubeApiException.class,
                () -> api.applyNodeMetadata(node, NODE_NAME, labels("team", "orders"), List.of()));

        assertEquals(409, refused.status());
        assertThat(refused.getMessage(), containsString("PATCH /api/v1/nodes/" + NODE_NAME
                + ": 409: Operation cannot be fulfilled"));
        assertEquals(3, kube.requests("PATCH", "/api/v1/nodes/" + NODE_NAME).size());
    }

    private static Map<String, String> labels(String... keysAndValues) {
        Map<String, String> labels = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            labels.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return labels;
    }

    private static List<String> taintIds(JsonNode taints) {
        List<String> ids = new ArrayList<>();
        for (JsonNode taint : taints) {
            ids.add(taint.path("key").asText() + ":" + taint.path("effect").asText());
        }
        return ids;
    }
}
