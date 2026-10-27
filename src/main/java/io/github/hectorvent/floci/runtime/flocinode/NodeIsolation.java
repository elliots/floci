package io.github.hectorvent.floci.runtime.flocinode;

import io.github.hectorvent.floci.runtime.network.NetworkDefinitions;
import io.github.hectorvent.floci.runtime.network.NetworkSection;
import io.github.hectorvent.floci.runtime.node.RuntimeFile;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;

/**
 * Whether Floci creates a smolvm-pods node isolated, and the CIDRs of the allow list at the
 * boundary of an isolated one, from the runtime file's {@code network} in force. A node keeps the
 * isolation it was created with; only its allow list follows the file.
 */
@ApplicationScoped
public class NodeIsolation {

    /**
     * @param enabled    whether a node created now is isolated
     * @param allowCidrs the CIDRs that an isolated node may reach besides Floci
     */
    public record Isolation(boolean enabled, List<String> allowCidrs) {

        public static final Isolation OPEN = new Isolation(false, List.of());

        public Isolation {
            allowCidrs = allowCidrs == null ? List.of() : List.copyOf(allowCidrs);
        }
    }

    private final NetworkSection network;

    @Inject
    public NodeIsolation(NetworkSection network) {
        this.network = network;
    }

    /** The isolation that {@code file} gives the nodes Floci creates. */
    public Isolation of(RuntimeFile file) {
        NetworkDefinitions definitions = file.section(network);
        if (!definitions.isolation()) {
            return Isolation.OPEN;
        }
        return new Isolation(true, definitions.allow().stream().map(NetworkDefinitions.Allow::cidr).distinct().toList());
    }
}
