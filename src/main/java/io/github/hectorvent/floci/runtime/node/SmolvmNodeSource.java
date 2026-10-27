package io.github.hectorvent.floci.runtime.node;

import java.util.List;
import java.util.Map;

/**
 * Nodes that a Floci feature creates or names, listed by {@link SmolvmNodeDirectory} beside the
 * runtime file's. An implementation is a bean; it names its nodes apart from the runtime file's,
 * such as {@code floci}, and calls {@link SmolvmNodeDirectory#refresh()} when they change.
 */
public interface SmolvmNodeSource {

    /** The nodes as they are now. */
    List<SmolvmNode> nodes();

    /**
     * Other names that find nodes of the directory, each with the name of the node it finds, such
     * as {@code cluster:ACCOUNT/REGION/NAME} for the node of an EKS cluster. The directory lists a
     * node once, by its own name.
     */
    default Map<String, String> aliases() {
        return Map.of();
    }
}
