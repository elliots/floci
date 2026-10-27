package io.github.hectorvent.floci.core.common.docker;

import java.util.Map;
import java.util.Objects;

/**
 * The OCI runtime that a daemon runs a container with, {@code HostConfig.Runtime}, and the
 * annotations it writes into the container's OCI spec, {@code HostConfig.Annotations}, which
 * Docker 24 and later pass to the runtime. A {@link ContainerLifecycleManager} built with one
 * creates every container with it.
 *
 * @param name        the runtime's name in the daemon, such as {@code io.containerd.smolvm-pods.v2}
 * @param annotations the OCI annotations, empty for none
 */
public record OciRuntime(String name, Map<String, String> annotations) {

    public OciRuntime {
        Objects.requireNonNull(name, "name");
        annotations = Map.copyOf(annotations);
    }
}
