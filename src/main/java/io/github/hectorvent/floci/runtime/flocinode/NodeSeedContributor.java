package io.github.hectorvent.floci.runtime.flocinode;

import io.github.hectorvent.floci.runtime.node.SmolvmNode;

import java.util.List;
import java.util.Map;

/**
 * What a Floci feature adds to the node that Floci creates: seed files, k3s settings, and
 * link-local endpoints that the node's pods reach Floci through. An implementation is a bean; EKS
 * adds the token and admission webhooks, the service account signing key and the ECR mirror.
 */
public interface NodeSeedContributor {

    /** The feature's name, which {@link NodeRecord#seeded()} lists while its additions are in the seed. */
    String name();

    /** What this feature adds to the seed of {@code node}, or {@link Contribution#NONE}. */
    Contribution contribute(NodeRecord node);

    /** The link-local endpoints of {@code node}'s pods that lead to Floci. */
    default List<SmolvmNode.Endpoint> endpoints(NodeRecord node) {
        return List.of();
    }

    /**
     * @param files   seed files by name, among {@link NodeSeed#FILES}
     * @param k3sArgs k3s settings by name, such as {@code kube-apiserver-arg+}, each a list of values
     */
    record Contribution(Map<String, String> files, Map<String, List<String>> k3sArgs) {

        public static final Contribution NONE = new Contribution(Map.of(), Map.of());

        public Contribution {
            files = Map.copyOf(files);
            k3sArgs = Map.copyOf(k3sArgs);
        }
    }
}
