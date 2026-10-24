package io.github.hectorvent.floci.runtime.flocinode;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.runtime.node.FakeDockerNode;
import io.github.hectorvent.floci.runtime.node.FakeNodeApi;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.ConfigProvider;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * {@code node-vm.sh} on fakes, for Quarkus tests: a node it creates answers with a
 * {@link FakeKubeApi} on its API port, Ready and with RuntimeClass {@code smolvm-pods} unless a
 * test holds its readiness, and with a node service on its node service port: a
 * {@link FakeNodeApi}, or a {@link FakeDockerNode} when {@value #SERVICE_PROPERTY} is
 * {@code docker}. Both take the tokens of the node's seed.
 */
@Alternative
@Singleton
public class FakeFlociNodes extends FakeNodeDriver {

    /** Which node service the fake runs: {@code pods}, the default, or {@code docker}. */
    public static final String SERVICE_PROPERTY = "floci-test.fake-node-service";

    private final boolean docker;
    private volatile boolean held;
    private volatile NodeRecord node;
    private volatile FakeKubeApi kubeApi;
    private volatile FakeNodeApi nodeApi;
    private volatile FakeDockerNode dockerNode;
    private volatile String serviceToken;

    @Inject
    public FakeFlociNodes(EmulatorConfig config) {
        super(new NodeScriptDriver(config).ownerLabels());
        docker = "docker".equals(ConfigProvider.getConfig().getOptionalValue(SERVICE_PROPERTY, String.class)
                .orElse("pods"));
    }

    /** The next node starts with no registered node and no RuntimeClass. */
    public void holdReadiness() {
        held = true;
    }

    /** Back to nodes that are ready at once, which create as asked, with no call recorded. */
    public void reset() {
        held = false;
        failCreates(null);
        clearCalls();
    }

    /** The node that runs now, or null. */
    public NodeRecord node() {
        return node;
    }

    /** The Kubernetes API of the node, or null before it runs. */
    public FakeKubeApi kubeApi() {
        return kubeApi;
    }

    /** The node service of the node, a {@link FakeNodeApi}, or null before it runs. */
    public FakeNodeApi nodeApi() {
        return nodeApi;
    }

    /** The node service of the node, a {@link FakeDockerNode}, or null before it runs. */
    public FakeDockerNode dockerNode() {
        return dockerNode;
    }

    /** The token in the seed of the node service. */
    public String serviceToken() {
        return serviceToken;
    }

    @Override
    protected void started(NodeRecord started, Path seed) {
        try {
            String adminToken = Files.readString(seed.resolve(NodeSeed.TOKENS_FILE), StandardCharsets.UTF_8)
                    .split(",", 2)[0];
            String token = Files.readString(seed.resolve(NodeSeed.API_TOKEN_FILE), StandardCharsets.UTF_8).strip();
            closeFakes();
            FakeKubeApi kube = new FakeKubeApi(adminToken, started.apiPort());
            if (!held) {
                kube.addNode(started.nodeName(), "True");
                kube.addRuntimeClass(NodeReadyRule.RUNTIME_CLASS);
            }
            if (docker) {
                dockerNode = new FakeDockerNode(token, started.servicePort());
            } else {
                nodeApi = new FakeNodeApi(token, started.servicePort());
            }
            serviceToken = token;
            kubeApi = kube;
            node = started;
        } catch (IOException e) {
            throw new NodeCommandException("the fake APIs of node " + started.nodeId() + " did not start: "
                    + e.getMessage(), "", e);
        }
    }

    @Override
    protected void deleted(String nodeId) {
        NodeRecord current = node;
        if (current != null && current.nodeId().equals(nodeId)) {
            closeFakes();
            node = null;
        }
    }

    private synchronized void closeFakes() {
        if (kubeApi != null) {
            kubeApi.close();
            kubeApi = null;
        }
        if (nodeApi != null) {
            nodeApi.close();
            nodeApi = null;
        }
        if (dockerNode != null) {
            dockerNode.close();
            dockerNode = null;
        }
    }
}
