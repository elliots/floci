package io.github.hectorvent.floci.runtime.node;

import java.net.URI;
import java.util.List;
import java.util.Objects;

/**
 * A smolvm-pods node that Floci calls: its name in {@link SmolvmNodeDirectory}, the base URL and
 * token of its node service's API, and the link-local endpoints its pods reach Floci through.
 *
 * @param name      {@code node:NAME} for a node in the runtime file, {@code floci} for the node that
 *                  Floci created, or {@code cluster:ACCOUNT/REGION/NAME} when it is found by the name of the
 *                  EKS cluster that runs on it
 * @param baseUrl   {@code http} or {@code https}, host and port, without a path
 * @param endpoints link-local addresses in the node that lead to a port of Floci
 */
public record SmolvmNode(String name, URI baseUrl, NodeToken token, Kind kind, List<Endpoint> endpoints) {

    public enum Kind {
        /** Created by Floci, for RDS and EKS. */
        CREATED,
        /** Declared in the runtime file's {@code nodes}. */
        DECLARED
    }

    /** {@code address:port} in the node, which leads to Floci's {@code targetPort}. */
    public record Endpoint(String address, int port, int targetPort) {
    }

    public SmolvmNode {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(baseUrl, "baseUrl");
        Objects.requireNonNull(token, "token");
        Objects.requireNonNull(kind, "kind");
        endpoints = List.copyOf(endpoints);
    }

    /** The directory name of the node that the runtime file declares as {@code nodes.KEY}. */
    public static String declaredName(String key) {
        return "node:" + key;
    }

    /** The name that finds the node of an EKS cluster in the directory. */
    public static String clusterName(String accountId, String region, String clusterName) {
        return "cluster:" + accountId + "/" + region + "/" + clusterName;
    }

    /** This node under another name, as an alias finds it. */
    public SmolvmNode named(String alias) {
        return new SmolvmNode(alias, baseUrl, token, kind, endpoints);
    }
}
