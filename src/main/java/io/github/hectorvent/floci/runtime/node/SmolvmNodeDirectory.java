package io.github.hectorvent.floci.runtime.node;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Every smolvm-pods node that Floci knows: the runtime file's {@code nodes}, then the nodes of each
 * {@link SmolvmNodeSource}. Names are unique; when two nodes share one, the first is kept. A
 * source's aliases find a node under another name, such as the name of the EKS cluster that runs
 * on it; {@link #all()} lists each node once, by its own name.
 */
@ApplicationScoped
public class SmolvmNodeDirectory {

    private static final Logger LOG = Logger.getLogger(SmolvmNodeDirectory.class);

    private final RuntimeFileWatcher runtimeFile;
    private final NodesSection nodesSection;
    private final Iterable<SmolvmNodeSource> sources;
    private final List<Consumer<List<SmolvmNode>>> listeners = new CopyOnWriteArrayList<>();
    private View told;

    /** What the listeners were told: the nodes, and the aliases that find them. */
    private record View(List<SmolvmNode> nodes, Map<String, String> aliases) {
    }

    @Inject
    public SmolvmNodeDirectory(RuntimeFileWatcher runtimeFile, NodesSection nodesSection,
                               Instance<SmolvmNodeSource> sources) {
        this(runtimeFile, nodesSection, (Iterable<SmolvmNodeSource>) sources);
    }

    SmolvmNodeDirectory(RuntimeFileWatcher runtimeFile, NodesSection nodesSection, Iterable<SmolvmNodeSource> sources) {
        this.runtimeFile = runtimeFile;
        this.nodesSection = nodesSection;
        this.sources = sources;
        runtimeFile.addListener(changed -> refresh());
    }

    /** Every node, the runtime file's first. */
    public List<SmolvmNode> all() {
        Map<String, SmolvmNode> nodes = new LinkedHashMap<>();
        for (SmolvmNode node : runtimeFile.current().section(nodesSection).values()) {
            nodes.putIfAbsent(node.name(), node);
        }
        for (SmolvmNodeSource source : sources) {
            for (SmolvmNode node : source.nodes()) {
                if (nodes.putIfAbsent(node.name(), node) != null) {
                    LOG.debugv("Two smolvm nodes are named {0}; the first is kept", node.name());
                }
            }
        }
        return List.copyOf(nodes.values());
    }

    /**
     * The node named {@code name}, such as {@code node:dev}, or the node that alias {@code name}
     * finds, under the alias.
     */
    public Optional<SmolvmNode> find(String name) {
        List<SmolvmNode> nodes = all();
        Optional<SmolvmNode> found = nodes.stream().filter(node -> node.name().equals(name)).findFirst();
        if (found.isPresent()) {
            return found;
        }
        String target = aliases().get(name);
        return target == null ? Optional.empty()
                : nodes.stream().filter(node -> node.name().equals(target)).findFirst().map(node -> node.named(name));
    }

    /** Every alias of the sources, with the name of the node it finds. */
    private Map<String, String> aliases() {
        Map<String, String> aliases = new LinkedHashMap<>();
        for (SmolvmNodeSource source : sources) {
            source.aliases().forEach(aliases::putIfAbsent);
        }
        return aliases;
    }

    /**
     * Tells {@code listener} every node each time the nodes or their aliases change from now on, on
     * the thread of the change: the runtime file's watcher, or the caller of {@link #refresh()}.
     */
    public synchronized void addListener(Consumer<List<SmolvmNode>> listener) {
        if (told == null) {
            told = new View(all(), aliases());
        }
        listeners.add(listener);
    }

    public void removeListener(Consumer<List<SmolvmNode>> listener) {
        listeners.remove(listener);
    }

    /** Lists the nodes again and tells the listeners when they changed. */
    public synchronized void refresh() {
        List<SmolvmNode> nodes = all();
        View view = new View(nodes, aliases());
        if (told == null || view.equals(told)) {
            told = view;
            return;
        }
        told = view;
        LOG.infov("The smolvm nodes changed; they are now {0}", nodes.stream().map(SmolvmNode::name).toList());
        for (Consumer<List<SmolvmNode>> listener : listeners) {
            try {
                listener.accept(nodes);
            } catch (RuntimeException e) {
                LOG.errorv(e, "A listener of the smolvm node directory failed");
            }
        }
    }
}
