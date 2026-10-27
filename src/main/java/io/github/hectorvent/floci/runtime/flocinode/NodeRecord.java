package io.github.hectorvent.floci.runtime.flocinode;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The smolvm-pods node VM that Floci created, as Floci persists it. The node's secrets are not
 * here: they live only in its seed directory.
 *
 * @param nodeId      the machine's name, {@code floci-node-} and 16 hex digits
 * @param apiPort     the Mac port of the Kubernetes API
 * @param servicePort the Mac port of the node service's API
 * @param nodePorts   the NodePorts published on the same Mac ports, as {@code node-vm.sh create
 *                    --node-ports} takes them: {@code FIRST-LAST}, {@code PORT} or {@code none}
 * @param imageSha256 the SHA-256 of the node image the machine was created from
 * @param isolated    whether the machine was created with {@code --isolate}, fixed for its life
 * @param allowCidrs  the CIDRs of the allow list at the machine's boundary, when isolated
 * @param accountId   the account of the first use, which created the node
 * @param region      the region of the first use, which names the node in Kubernetes
 * @param nodeName    the node's name in Kubernetes, an EC2 private DNS name
 * @param serviceCidr k3s's Service range
 * @param podCidr     k3s's pod range
 * @param k3sArgs     more k3s settings that the first use asked for, by k3s setting, such as
 *                    {@code kubelet-arg+}
 * @param seeded      the {@link NodeSeedContributor}s, by name, whose additions the seed had when
 *                    Floci last wrote it whole, which the node's k3s took at its next boot
 */
@RegisterForReflection
@JsonIgnoreProperties(ignoreUnknown = true)
public record NodeRecord(String nodeId, int apiPort, int servicePort, String nodePorts, String imageSha256,
                         boolean isolated, List<String> allowCidrs, String accountId, String region,
                         String nodeName, String serviceCidr, String podCidr, Map<String, List<String>> k3sArgs,
                         List<String> seeded) {

    /** What {@code nodeId} is: smolvm takes letters, digits, {@code -} and {@code _}, without {@code --}. */
    public static final Pattern NODE_ID = Pattern.compile("floci-node-[0-9a-f]{16}");

    public NodeRecord {
        allowCidrs = allowCidrs == null ? List.of() : List.copyOf(allowCidrs);
        Map<String, List<String>> args = new LinkedHashMap<>();
        if (k3sArgs != null) {
            k3sArgs.forEach((key, values) -> args.put(key, List.copyOf(values)));
        }
        k3sArgs = Collections.unmodifiableMap(args);
        seeded = seeded == null ? List.of() : List.copyOf(seeded);
    }

    /** The EC2 instance id in the node's name. */
    public String instanceId() {
        int dot = nodeName == null ? -1 : nodeName.indexOf('.');
        return dot < 0 ? nodeName : nodeName.substring(0, dot);
    }

    /** The availability zone that the node reports. */
    public String availabilityZone() {
        return region + "a";
    }

    /** This node with other Mac ports. */
    public NodeRecord withPorts(int apiPort, int servicePort, String nodePorts) {
        return new NodeRecord(nodeId, apiPort, servicePort, nodePorts, imageSha256, isolated, allowCidrs, accountId,
                region, nodeName, serviceCidr, podCidr, k3sArgs, seeded);
    }

    /** This node with the allow list {@code allowCidrs}. */
    public NodeRecord withAllowCidrs(List<String> allowCidrs) {
        return new NodeRecord(nodeId, apiPort, servicePort, nodePorts, imageSha256, isolated, allowCidrs, accountId,
                region, nodeName, serviceCidr, podCidr, k3sArgs, seeded);
    }

    /** This node with the isolation of its machine. */
    public NodeRecord withIsolated(boolean isolated) {
        return new NodeRecord(nodeId, apiPort, servicePort, nodePorts, imageSha256, isolated, allowCidrs, accountId,
                region, nodeName, serviceCidr, podCidr, k3sArgs, seeded);
    }

    /** This node with the contributors of the seed that Floci wrote whole. */
    public NodeRecord withSeeded(List<String> seeded) {
        return new NodeRecord(nodeId, apiPort, servicePort, nodePorts, imageSha256, isolated, allowCidrs, accountId,
                region, nodeName, serviceCidr, podCidr, k3sArgs, seeded);
    }
}
