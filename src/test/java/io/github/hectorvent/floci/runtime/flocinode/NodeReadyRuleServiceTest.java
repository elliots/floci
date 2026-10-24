package io.github.hectorvent.floci.runtime.flocinode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.runtime.flocinode.NodeDriver.NodeCommandException;
import io.github.hectorvent.floci.runtime.node.FakeNodeApi;
import io.github.hectorvent.floci.runtime.node.NodeToken;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeReadyRuleServiceTest {

    private static final String ADMIN_TOKEN = "4f1e0c6a9d2b7e8f3a5c1d0e9b8a7f6e5d4c3b2a19087f6e5d4c3b2a1908f7e6";
    private static final String SERVICE_TOKEN = "9a8b7c6d5e4f30211203f4e5d6c7b8a99a8b7c6d5e4f30211203f4e5d6c7b8a9";
    private static final String NODE_ID = "floci-node-0123456789abcdef";
    private static final String NODE_NAME = "ip-192-168-0-10.ec2.internal";

    private static final NodeReadyRule.BootId BOOT = () -> FakeKubeApi.BOOT_ID;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final SmolvmNodeClient NODE_CLIENT = new SmolvmNodeClient(MAPPER);

    private FakeKubeApi kube;
    private FakeNodeApi nodeApi;
    private NodeKubeApi kubeApi;
    private NodeReadyRule rule;
    private final Set<String> applied = new HashSet<>();
    private NodeRecord node;
    private SmolvmNode service;

    @BeforeEach
    void setUp() throws IOException {
        kube = new FakeKubeApi(ADMIN_TOKEN);
        nodeApi = new FakeNodeApi(SERVICE_TOKEN);
        kubeApi = new NodeKubeApi(nodeId -> ADMIN_TOKEN, MAPPER);
        rule = new NodeReadyRule(kubeApi, NODE_CLIENT, applied::contains);
        node = kube.nodeRef(NODE_ID);
        service = new SmolvmNode(FlociNode.NAME, nodeApi.url(),
                new NodeToken.TokenVariable("FLOCI_TEST_NODE_TOKEN", name -> SERVICE_TOKEN), SmolvmNode.Kind.CREATED,
                List.of());
        kube.addNode(NODE_NAME, "True");
        kube.addRuntimeClass(NodeReadyRule.RUNTIME_CLASS);
        applied.add(service.name());
    }

    @AfterEach
    void tearDown() {
        kubeApi.close();
        kube.close();
        nodeApi.close();
    }

    @Test
    void readyWhenEveryConditionHolds() {
        assertEquals(Optional.empty(), rule.unmetCondition(node, NODE_NAME, service, BOOT));

        assertEquals(1, kube.requests("GET", "/readyz").size());
        assertEquals(1, kube.requests("GET", "/api/v1/nodes/" + NODE_NAME).size());
        assertEquals(1, kube.requests("GET", "/apis/node.k8s.io/v1/runtimeclasses/smolvm-pods").size());
        List<FakeNodeApi.Request> pods = nodeApi.requests();
        assertEquals(1, pods.size());
        assertEquals("/v1/pods", pods.getFirst().path());
        assertEquals("Bearer " + SERVICE_TOKEN, pods.getFirst().authorization());
    }

    @Test
    void waitsForTheKubernetesApiToBeReady() {
        kube.readyz(500, "[-]poststarthook/rbac/bootstrap-roles failed: not finished");

        String unmet = rule.unmetCondition(node, NODE_NAME, service, BOOT).orElseThrow();

        assertThat(unmet, startsWith("the Kubernetes API is not ready: "));
        assertThat(unmet, containsString("GET /readyz: 500"));
        assertTrue(kube.requests("GET", "/api/v1/nodes/" + NODE_NAME).isEmpty());
        assertTrue(nodeApi.requests().isEmpty());
    }

    @Test
    void waitsForAKubernetesApiThatDoesNotAnswer() {
        kube.close();

        String unmet = rule.unmetCondition(node, NODE_NAME, service, BOOT).orElseThrow();

        assertThat(unmet, startsWith("the Kubernetes API is not ready: "));
        assertThat(unmet, containsString("no answer"));
        assertTrue(nodeApi.requests().isEmpty());
    }

    @Test
    void aRefusedAdminTokenIsNotReady() {
        kube.token("a-different-token-of-the-node");

        String unmet = rule.unmetCondition(node, NODE_NAME, service, BOOT).orElseThrow();

        assertThat(unmet, containsString("GET /readyz: 401"));
    }

    @Test
    void waitsForTheNodeToRegister() {
        String other = "ip-192-168-0-11.ec2.internal";

        assertEquals(Optional.of("node " + other + " has not registered"), rule.unmetCondition(node, other, service, BOOT));
        assertTrue(nodeApi.requests().isEmpty());
    }

    @Test
    void waitsForTheNodeToBeReady() {
        kube.changeNode(NODE_NAME, n -> ((ObjectNode) n.path("status").path("conditions").get(0)).put("status", "False"));

        assertEquals(Optional.of("node " + NODE_NAME + " is not Ready: its Ready condition is False"),
                rule.unmetCondition(node, NODE_NAME, service, BOOT));

        kube.changeNode(NODE_NAME, n -> ((ArrayNode) n.path("status").path("conditions")).removeAll());

        assertEquals(Optional.of("node " + NODE_NAME + " is not Ready: its Ready condition is missing"),
                rule.unmetCondition(node, NODE_NAME, service, BOOT));
        assertTrue(kube.requests("GET", "/apis/node.k8s.io/v1/runtimeclasses/smolvm-pods").isEmpty());
        assertTrue(nodeApi.requests().isEmpty());
    }

    @Test
    void aReadyStatusFromAnEarlierBootIsNotReady() {
        String restarted = "8e1f2a3b-4c5d-4e6f-8a9b-0c1d2e3f4a5b";

        assertEquals(Optional.of("node " + NODE_NAME + " is Ready from an earlier boot (boot id " + FakeKubeApi.BOOT_ID
                + "), and kubelet has not reported since the node VM started with boot id " + restarted),
                rule.unmetCondition(node, NODE_NAME, service, () -> restarted));
        assertTrue(kube.requests("GET", "/apis/node.k8s.io/v1/runtimeclasses/smolvm-pods").isEmpty());
        assertTrue(nodeApi.requests().isEmpty());

        kube.changeNode(NODE_NAME, n -> ((ObjectNode) n.path("status").path("nodeInfo")).put("bootID", restarted));

        assertEquals(Optional.empty(), rule.unmetCondition(node, NODE_NAME, service, () -> restarted));
    }

    @Test
    void aBootIdThatCannotBeReadIsNotReady() {
        assertEquals(Optional.of("could not read the boot id of the node VM: node-vm.sh exec failed"),
                rule.unmetCondition(node, NODE_NAME, service, () -> {
                    throw new NodeCommandException("node-vm.sh exec failed", "", null);
                }));
        assertTrue(nodeApi.requests().isEmpty());
    }

    @Test
    void waitsForTheRuntimeClass() throws IOException {
        try (FakeKubeApi withoutRuntimeClass = new FakeKubeApi(ADMIN_TOKEN)) {
            withoutRuntimeClass.addNode(NODE_NAME, "True");

            assertEquals(Optional.of("RuntimeClass smolvm-pods does not exist"),
                    rule.unmetCondition(withoutRuntimeClass.nodeRef("floci-node-fedcba9876543210"), NODE_NAME, service,
                            BOOT));
        }
        assertTrue(nodeApi.requests().isEmpty());
    }

    @Test
    void aFailedReadOfTheNodeOrRuntimeClassIsNotReady() {
        kube.forbidNext("GET", "/api/v1/nodes/" + NODE_NAME);

        assertEquals(Optional.of("a call to the Kubernetes API failed: " + NODE_ID + ": GET /api/v1/nodes/" + NODE_NAME
                + ": 403: forbidden: GET /api/v1/nodes/" + NODE_NAME), rule.unmetCondition(node, NODE_NAME, service, BOOT));

        kube.forbidNext("GET", "/apis/node.k8s.io/v1/runtimeclasses/smolvm-pods");

        assertThat(rule.unmetCondition(node, NODE_NAME, service, BOOT).orElseThrow(),
                startsWith("a call to the Kubernetes API failed: " + NODE_ID
                        + ": GET /apis/node.k8s.io/v1/runtimeclasses/smolvm-pods: 403"));
        assertTrue(nodeApi.requests().isEmpty());
        assertEquals(Optional.empty(), rule.unmetCondition(node, NODE_NAME, service, BOOT));
    }

    @Test
    void waitsForTheNodeServiceToAcceptItsToken() {
        nodeApi.token("a-token-that-floci-does-not-have");

        String unmet = rule.unmetCondition(node, NODE_NAME, service, BOOT).orElseThrow();

        assertThat(unmet, startsWith("the node service is not ready: "));
        assertThat(unmet, containsString("GET /v1/pods: 401"));
    }

    @Test
    void waitsForTheNodeServiceToAnswer() {
        nodeApi.close();

        String unmet = rule.unmetCondition(node, NODE_NAME, service, BOOT).orElseThrow();

        assertThat(unmet, startsWith("the node service is not ready: "));
        assertThat(unmet, containsString("no answer"));
    }

    @Test
    void aNodeWithoutItsNetworkProfileIsNotReady() {
        applied.clear();
        assertEquals(Optional.of("the node has not applied Floci's network profile yet"),
                rule.unmetCondition(node, NODE_NAME, service, BOOT));
        applied.add(service.name());
        assertEquals(Optional.empty(), rule.unmetCondition(node, NODE_NAME, service, BOOT));
    }
}
