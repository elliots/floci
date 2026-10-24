package io.github.hectorvent.floci.runtime.wakes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.runtime.node.FakeNodeApi;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A smolvm-pods node for the message wake integration tests: a {@link FakeNodeApi} that the
 * runtime file declares as {@code nodes.dev}, with its token in a file beside the runtime file.
 * The fake starts with the test's profile, before Floci, so Floci's first follow reaches it.
 */
final class WakeTestNode implements AutoCloseable {

    static final String TOKEN = "integration-token-0123456789";
    static final String NODE = "node:dev";

    private final Path dir;
    private final Path file;
    private final FakeNodeApi fake;

    private WakeTestNode(Path dir, FakeNodeApi fake) {
        this.dir = dir;
        this.file = dir.resolve("runtime.yaml");
        this.fake = fake;
    }

    /** Starts the fake and keeps the test's files in {@code target/NAME}, emptied first. */
    static WakeTestNode start(String name) {
        Path dir = Path.of("target", name).toAbsolutePath();
        try {
            if (Files.exists(dir)) {
                try (Stream<Path> paths = Files.walk(dir)) {
                    for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                        Files.delete(path);
                    }
                }
            }
            Files.createDirectories(dir);
            WakeTestNode node = new WakeTestNode(dir, new FakeNodeApi(TOKEN));
            node.token(TOKEN);
            return node;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    FakeNodeApi fake() {
        return fake;
    }

    Path dir() {
        return dir;
    }

    /**
     * Writes the runtime file with this node and {@code workloads}, the YAML under
     * {@code workloads:}, and returns the configuration that names it.
     */
    Map<String, String> config(String workloads) {
        write(workloads);
        return Map.of("floci.runtime.config-file", file.toString());
    }

    /** Writes the runtime file again, with a modification time that Floci's next check sees as new. */
    void write(String workloads) {
        writeFile("version: 1\nnodes:\n  dev:\n    url: " + fake.url() + "\n    token-file: node-token\n"
                + "workloads:\n" + workloads.stripIndent().indent(2));
    }

    /** Writes {@code content} as the runtime file, valid or not. */
    void writeFile(String content) {
        try {
            Files.writeString(file, content);
            Files.setLastModifiedTime(file, FileTime.from(Instant.now().plusSeconds(60 + content.length())));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Writes the token file that the runtime file names. */
    void token(String token) {
        try {
            Path path = dir.resolve("node-token");
            Files.writeString(path, token);
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Adds a pod of the Deployment {@code deployment} in {@code state}. The node sends its events
     * once the pod has its labels and controller, as a node lists a pod with them from the start.
     */
    ObjectNode pod(String sandbox, String namespace, String deployment, String state) {
        synchronized (fake) {
            ObjectNode pod = fake.addPod(sandbox, namespace, sandbox);
            pod.putObject("controller").put("kind", "Deployment").put("name", deployment);
            pod.putObject("labels").put("app", deployment);
            if (!state.equals("awake")) {
                fake.state(sandbox, state);
            }
            return pod;
        }
    }

    String state(String sandbox) {
        return fake.pod(sandbox).path("state").asText();
    }

    /** The ids of the API leases on the pod. */
    List<String> leases(String sandbox) {
        List<String> ids = new ArrayList<>();
        for (JsonNode lease : fake.pod(sandbox).path("leases")) {
            if (lease.path("kind").asText().equals("api")) {
                ids.add(lease.path("id").asText());
            }
        }
        return ids;
    }

    /** The wakes the pod got, in order. */
    List<FakeNodeApi.Request> wakes(String sandbox) {
        return requests("POST", "/v1/pods/" + sandbox + "/wake");
    }

    /** The requests of {@code method} to {@code path}, in order. */
    List<FakeNodeApi.Request> requests(String method, String path) {
        return fake.requests().stream()
                .filter(request -> request.method().equals(method) && request.path().equals(path))
                .toList();
    }

    /** An SQS client of Floci at {@code endpoint}, in {@code region}. */
    static SqsClient sqs(URI endpoint, Region region) {
        return SqsClient.builder().endpointOverride(endpoint).region(region).credentialsProvider(credentials())
                .build();
    }

    static StaticCredentialsProvider credentials() {
        return StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
    }

    /** The path of a lease of a pod, as Floci calls it. */
    static String leasePath(String sandbox, String lease) {
        return "/v1/pods/" + sandbox + "/leases/" + lease.replace(":", "%3A");
    }

    @Override
    public void close() {
        fake.close();
    }
}
