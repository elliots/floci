package io.github.hectorvent.floci.services.eks;

import java.time.Duration;
import java.util.Optional;

/**
 * Reads one Kubernetes ServiceAccount of a cluster: with {@code kubectl} in a k3s container, or
 * from the Kubernetes API of a smolvm-pods node. A failure never carries what Kubernetes said, so
 * that admission logs hold no resource contents.
 */
@FunctionalInterface
public interface EksServiceAccountReader {

    /**
     * How long a read may take: less than the 3 seconds that the API server gives Floci's admission
     * webhook, so that a slow read fails, and is logged, before the API server admits the pod unchanged.
     */
    Duration TIMEOUT = Duration.ofSeconds(2);

    /**
     * The ServiceAccount {@code namespace/name} as Kubernetes returns it, JSON, or empty when it
     * does not exist or the cluster runs no Kubernetes API.
     *
     * @throws IllegalStateException when Kubernetes cannot be asked or does not answer within
     *                               {@link #TIMEOUT}
     */
    Optional<String> read(String namespace, String name);
}
