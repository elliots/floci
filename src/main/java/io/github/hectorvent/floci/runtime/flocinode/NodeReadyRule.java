package io.github.hectorvent.floci.runtime.flocinode;

import io.github.hectorvent.floci.runtime.flocinode.NodeDriver.NodeCommandException;
import io.github.hectorvent.floci.runtime.flocinode.NodeKubeApi.KubeApiException;
import io.github.hectorvent.floci.runtime.flocinode.NodeKubeApi.KubeNode;
import io.github.hectorvent.floci.runtime.network.NodeNetworkPublisher;
import io.github.hectorvent.floci.runtime.node.NodeApiException;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.Optional;
import java.util.function.Predicate;

/**
 * When the node that Floci created is ready for RDS and EKS, checked in this order:
 *
 * <ol>
 *   <li>the Kubernetes API answers {@code GET /readyz} with 200 to the admin token;</li>
 *   <li>the node's {@code Ready} condition is {@code True}, and kubelet reported it in the node VM's
 *       current boot: the node's {@code status.nodeInfo.bootID} is the VM's boot id. After a stop
 *       and a start, the node keeps the {@code Ready} status of its previous boot until kubelet
 *       reports again;</li>
 *   <li>RuntimeClass {@value #RUNTIME_CLASS} exists;</li>
 *   <li>the node service answers {@code GET /v1/pods} with 200 to its token;</li>
 *   <li>the node applied the network profile that Floci publishes to it.</li>
 * </ol>
 */
@ApplicationScoped
public class NodeReadyRule {

    /** The RuntimeClass that the node image installs for pods in VMs. */
    public static final String RUNTIME_CLASS = "smolvm-pods";

    /** Reads the boot id of a node VM's kernel. */
    @FunctionalInterface
    interface BootId {

        /** @throws NodeCommandException when it cannot be read */
        String read();
    }

    private final NodeKubeApi kubeApi;
    private final SmolvmNodeClient nodeClient;
    private final Predicate<String> profileApplied;

    @Inject
    public NodeReadyRule(NodeKubeApi kubeApi, SmolvmNodeClient nodeClient, NodeNetworkPublisher network) {
        this(kubeApi, nodeClient, network::isApplied);
    }

    /** @param profileApplied whether the node of a directory name applied its network profile */
    NodeReadyRule(NodeKubeApi kubeApi, SmolvmNodeClient nodeClient, Predicate<String> profileApplied) {
        this.kubeApi = kubeApi;
        this.nodeClient = nodeClient;
        this.profileApplied = profileApplied;
    }

    /**
     * The first condition that does not hold, or empty when every one holds.
     *
     * @param node     the cluster's node
     * @param nodeName the node's name in Kubernetes
     * @param service  the node's node service
     * @param bootId   reads the boot id of the node VM's kernel
     */
    Optional<String> unmetCondition(NodeRecord node, String nodeName, SmolvmNode service, BootId bootId) {
        try {
            kubeApi.readyz(node);
        } catch (KubeApiException e) {
            return Optional.of("the Kubernetes API is not ready: " + e.getMessage());
        }
        try {
            Optional<KubeNode> kubeNode = kubeApi.node(node, nodeName);
            if (kubeNode.isEmpty()) {
                return Optional.of("node " + nodeName + " has not registered");
            }
            if (!kubeNode.get().ready()) {
                return Optional.of("node " + nodeName + " is not Ready: its Ready condition is "
                        + (kubeNode.get().readyStatus() == null ? "missing" : kubeNode.get().readyStatus()));
            }
            Optional<String> earlierBoot = earlierBoot(kubeNode.get(), bootId);
            if (earlierBoot.isPresent()) {
                return earlierBoot;
            }
            if (!kubeApi.runtimeClassExists(node, RUNTIME_CLASS)) {
                return Optional.of("RuntimeClass " + RUNTIME_CLASS + " does not exist");
            }
        } catch (KubeApiException e) {
            return Optional.of("a call to the Kubernetes API failed: " + e.getMessage());
        }
        try {
            nodeClient.listPods(service, SmolvmNodeClient.PodQuery.all());
        } catch (NodeApiException e) {
            return Optional.of("the node service is not ready: " + e.getMessage());
        }
        if (!profileApplied.test(service.name())) {
            return Optional.of("the node has not applied Floci's network profile yet");
        }
        return Optional.empty();
    }

    /** Why {@code kubeNode}'s Ready status is not from the node VM's current boot, or empty when it is. */
    private static Optional<String> earlierBoot(KubeNode kubeNode, BootId bootId) {
        String current;
        try {
            current = bootId.read();
        } catch (NodeCommandException e) {
            return Optional.of("could not read the boot id of the node VM: " + e.getMessage());
        }
        if (!current.equals(kubeNode.bootId())) {
            return Optional.of("node " + kubeNode.name() + " is Ready from an earlier boot ("
                    + (kubeNode.bootId() == null ? "no boot id" : "boot id " + kubeNode.bootId())
                    + "), and kubelet has not reported since the node VM started with boot id " + current);
        }
        return Optional.empty();
    }
}
