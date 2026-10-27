package io.github.hectorvent.floci.runtime.flocinode;

import java.util.List;
import java.util.Map;

/** Node records for tests, with the identity of a node that Floci created in {@code us-east-1}. */
public final class TestNodeRecords {

    public static final String NODE_NAME = "i-0123456789abcdef0.ec2.internal";

    private TestNodeRecords() {
    }

    public static NodeRecord record(String nodeId, int apiPort, int servicePort, String nodePorts, String imageSha256,
                                    boolean isolated, List<String> allowCidrs) {
        return new NodeRecord(nodeId, apiPort, servicePort, nodePorts, imageSha256, isolated, allowCidrs,
                "000000000000", "us-east-1", NODE_NAME, FlociNode.DEFAULT_SERVICE_CIDR, FlociNode.DEFAULT_POD_CIDR,
                Map.of(), List.of());
    }
}
