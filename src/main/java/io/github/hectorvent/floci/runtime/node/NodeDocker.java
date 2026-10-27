package io.github.hectorvent.floci.runtime.node;

import com.github.dockerjava.api.DockerClient;
import io.github.hectorvent.floci.core.common.docker.DockerClientProducer;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DockerClients for the dockerd of each smolvm-pods node, through its node service's Docker
 * proxy ({@code /docker}, tools/smolvm-pods/docs/configuration.md), with the node's bearer token.
 * A node's clients are made once and made again when its URL or token source changes.
 */
@ApplicationScoped
public class NodeDocker {

    /** Where the node service serves dockerd's Engine API. */
    public static final String PATH_PREFIX = "/docker";

    private static final Logger LOG = Logger.getLogger(NodeDocker.class);

    private final DockerClientProducer producer;
    private final Map<String, Clients> clients = new ConcurrentHashMap<>();

    /**
     * The clients of one node: {@code control} for short calls, {@code streaming} for log follows.
     */
    public record Clients(URI baseUrl, NodeToken token, DockerClient control, DockerClient streaming) {

        boolean serves(SmolvmNode node) {
            return baseUrl.equals(node.baseUrl()) && token.equals(node.token());
        }
    }

    @Inject
    public NodeDocker(DockerClientProducer producer) {
        this.producer = producer;
    }

    /** The clients of {@code node}'s dockerd. */
    public Clients of(SmolvmNode node) {
        return clients.compute(node.name(), (name, existing) -> {
            if (existing != null && existing.serves(node)) {
                return existing;
            }
            if (existing != null) {
                close(name, existing);
            }
            NodeToken token = node.token();
            return new Clients(node.baseUrl(), token,
                    producer.remoteDockerClient(node.baseUrl(), PATH_PREFIX, () -> "Bearer " + token.read(), false),
                    producer.remoteDockerClient(node.baseUrl(), PATH_PREFIX, () -> "Bearer " + token.read(), true));
        });
    }

    @PreDestroy
    void closeAll() {
        for (Map.Entry<String, Clients> entry : List.copyOf(clients.entrySet())) {
            close(entry.getKey(), entry.getValue());
        }
        clients.clear();
    }

    private static void close(String name, Clients clients) {
        for (DockerClient client : List.of(clients.control(), clients.streaming())) {
            try {
                client.close();
            } catch (IOException e) {
                LOG.debugv("Closing a DockerClient of node {0}: {1}", name, e.getMessage());
            }
        }
    }
}
