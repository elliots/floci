package io.github.hectorvent.floci.runtime.flocinode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.runtime.flocinode.NodeSeedContributor.Contribution;
import io.github.hectorvent.floci.runtime.node.NodeToken;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Writes the seed directory of the node that Floci creates,
 * {@code <floci.runtime.node.data-path>/<nodeId>/seed}. {@code node-vm.sh create --seed} mounts it
 * read-only at {@code /etc/floci-node/seed}, and {@code floci-node} installs it at every boot
 * before k3s starts, and applies a changed {@code env} to the running node service
 * (tools/smolvm-pods/node/README.md, "The seed").
 *
 * <p>The seed belongs to the node, not to a service: the node's name, its k3s ranges and the k3s
 * settings of its first use, and what each {@link NodeSeedContributor} adds, such as EKS's
 * webhooks. A cluster created later finds the node's k3s as the seed set it up.
 *
 * <p>The directory is mode 0700 and each file 0600. Each file is written to a temporary name and
 * renamed over the old one, never the directory itself, which a running node keeps mounted, and
 * {@code version} comes last, since {@code floci-node} waits until it reads {@code 1}. The node's
 * secrets, its Kubernetes admin token, its node service token and its service account signing
 * key, live only here.
 */
@ApplicationScoped
public class NodeSeed {

    private static final Logger LOG = Logger.getLogger(NodeSeed.class);

    static final String SEED_DIR = "seed";
    static final String VERSION = "1";

    public static final String VERSION_FILE = "version";
    public static final String ENV_FILE = "env";
    public static final String K3S_FILE = "k3s.yaml";
    public static final String TOKEN_WEBHOOK_FILE = "token-webhook.yaml";
    public static final String TOKENS_FILE = "tokens.csv";
    public static final String SIGNING_KEY_FILE = "sa-signing-key.pem";
    public static final String PUBLIC_KEY_FILE = "sa-public-key.pem";
    public static final String REGISTRIES_FILE = "registries.yaml";
    public static final String ADMISSION_FILE = "manifests/floci-admission.yaml";
    public static final String API_TOKEN_FILE = "api-token";
    public static final String HOSTS_FILE = "hosts";
    public static final String AUDIT_POLICY_FILE = "audit-policy.yaml";

    /** Every seed file but {@code version}, in the order Floci writes them. */
    public static final List<String> FILES = List.of(ENV_FILE, K3S_FILE, TOKEN_WEBHOOK_FILE, TOKENS_FILE,
            SIGNING_KEY_FILE, PUBLIC_KEY_FILE, REGISTRIES_FILE, ADMISSION_FILE, API_TOKEN_FILE, HOSTS_FILE,
            AUDIT_POLICY_FILE);

    /**
     * Where the node mounts the seed. k3s reads the audit policy there, as {@code floci-node} reads
     * {@code env} and {@code hosts}, so that a node image that does not install it still starts.
     */
    public static final String NODE_SEED_DIR = "/etc/floci-node/seed";
    public static final String NODE_AUDIT_POLICY = NODE_SEED_DIR + "/" + AUDIT_POLICY_FILE;
    /** Where kube-apiserver writes its audit log, which the node API's {@code kube-apiserver-audit} log follows. */
    public static final String NODE_AUDIT_LOG = "/var/log/kube-apiserver-audit.log";

    /** Where {@code floci-node} installs Floci's k3s files in the node. */
    public static final String NODE_FLOCI_DIR = "/etc/rancher/k3s/floci";
    public static final String NODE_TOKEN_WEBHOOK = NODE_FLOCI_DIR + "/" + TOKEN_WEBHOOK_FILE;
    public static final String NODE_TOKENS = NODE_FLOCI_DIR + "/" + TOKENS_FILE;
    public static final String NODE_SIGNING_KEY = NODE_FLOCI_DIR + "/" + SIGNING_KEY_FILE;
    public static final String NODE_PUBLIC_KEY = NODE_FLOCI_DIR + "/" + PUBLIC_KEY_FILE;

    /** The k3s settings that take lists, in the order the drop-in names them. */
    public static final String KUBELET_ARGS = "kubelet-arg+";
    public static final String API_SERVER_ARGS = "kube-apiserver-arg+";
    public static final String CONTROLLER_MANAGER_ARGS = "kube-controller-manager-arg+";
    public static final String SCHEDULER_ARGS = "kube-scheduler-arg+";
    public static final String CLOUD_CONTROLLER_MANAGER_ARGS = "kube-cloud-controller-manager-arg+";
    static final List<String> ARG_SETTINGS = List.of(KUBELET_ARGS, API_SERVER_ARGS, CONTROLLER_MANAGER_ARGS,
            SCHEDULER_ARGS, CLOUD_CONTROLLER_MANAGER_ARGS);

    /** The user and group of the static admin token. */
    static final String ADMIN_USER = "floci-admin";
    static final String ADMIN_GROUP = "system:masters";

    private static final Set<PosixFilePermission> DIRECTORY_MODE = PosixFilePermissions.fromString("rwx------");
    private static final Set<PosixFilePermission> FILE_MODE = PosixFilePermissions.fromString("rw-------");
    private static final Pattern TOKEN = Pattern.compile("[0-9a-f]{64}");
    private static final YAMLMapper YAML = YAMLMapper.builder()
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
            .build();

    private final EmulatorConfig config;
    private final NodeSettings settings;
    private final Iterable<NodeSeedContributor> contributors;
    private final SecureRandom random = new SecureRandom();

    @Inject
    public NodeSeed(EmulatorConfig config, NodeSettings settings, Instance<NodeSeedContributor> contributors) {
        this(config, settings, (Iterable<NodeSeedContributor>) contributors);
    }

    public NodeSeed(EmulatorConfig config, NodeSettings settings, Iterable<NodeSeedContributor> contributors) {
        this.config = config;
        this.settings = settings;
        this.contributors = contributors;
    }

    /** A new node id: {@code floci-node-} and 16 random hex digits. */
    public String newNodeId() {
        byte[] bytes = new byte[8];
        random.nextBytes(bytes);
        return "floci-node-" + HexFormat.of().formatHex(bytes);
    }

    /** The directory of node {@code nodeId}'s files, {@code <data-path>/<nodeId>}. */
    Path nodeDir(String nodeId) {
        if (nodeId == null || !NodeRecord.NODE_ID.matcher(nodeId).matches()) {
            throw new IllegalArgumentException("not a smolvm node id: " + nodeId);
        }
        return dataDir().resolve(nodeId);
    }

    private Path dataDir() {
        return Path.of(config.runtime().node().dataPath()).toAbsolutePath().normalize();
    }

    /** The seed directory of node {@code nodeId}, which {@code node-vm.sh create --seed} takes. */
    public Path seedDir(String nodeId) {
        return nodeDir(nodeId).resolve(SEED_DIR);
    }

    /**
     * Writes the whole seed of {@code node}, every file and then {@code version}, and removes the
     * files that this seed does not have. The tokens already in the seed stay; a missing one is
     * generated.
     *
     * @return the names of the contributors whose additions the seed has, for {@link NodeRecord#seeded()}
     * @throws UncheckedIOException when a file cannot be written
     */
    public synchronized List<String> write(NodeRecord node) {
        String nodeId = node.nodeId();
        Path seed = seedDir(nodeId);
        List<String> seeded = new ArrayList<>();
        Map<String, String> files = files(node, seed, seeded);
        try {
            createPrivateDirectories(seed);
            for (Map.Entry<String, String> file : files.entrySet()) {
                writeFile(seed, file.getKey(), file.getValue());
            }
            for (String name : FILES) {
                if (!files.containsKey(name)) {
                    Files.deleteIfExists(seed.resolve(name));
                }
            }
            writeFile(seed, VERSION_FILE, VERSION + "\n");
        } catch (IOException e) {
            throw new UncheckedIOException("Could not write the seed of node " + nodeId + " in " + seed, e);
        }
        LOG.infov("Wrote the seed of node {0} in {1}: {2}", nodeId, seed, String.join(", ", files.keySet()));
        return List.copyOf(seeded);
    }

    /**
     * Writes the seed's {@code env} from Floci's settings, which {@code floci-node} applies to the
     * running node service within about 5 s.
     *
     * @return whether the file changed
     * @throws UncheckedIOException when it cannot be written
     */
    public synchronized boolean writeEnv(String nodeId) {
        Path seed = seedDir(nodeId);
        String env = env();
        try {
            if (env.equals(readIfExists(seed.resolve(ENV_FILE)))) {
                return false;
            }
            createPrivateDirectories(seed);
            writeFile(seed, ENV_FILE, env);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not write the env of node " + nodeId + " in " + seed, e);
        }
        LOG.infov("Wrote the node settings of node {0} in {1}", nodeId, seed);
        return true;
    }

    /**
     * The static token of node {@code nodeId}'s Kubernetes API, in {@code system:masters}.
     *
     * @throws IllegalStateException when the seed has none
     */
    public String adminToken(String nodeId) {
        Path tokens = seedDir(nodeId).resolve(TOKENS_FILE);
        String token = existingAdminToken(tokens);
        if (token == null) {
            throw new IllegalStateException("The seed of node " + nodeId + " has no admin token in " + tokens);
        }
        return token;
    }

    /** The bearer token of node {@code nodeId}'s node service, read from its seed. */
    public NodeToken apiToken(String nodeId) {
        return NodeToken.file(seedDir(nodeId).resolve(API_TOKEN_FILE));
    }

    /** The link-local endpoints of {@code node}'s pods that lead to Floci, from the contributors. */
    List<SmolvmNode.Endpoint> endpoints(NodeRecord node) {
        List<SmolvmNode.Endpoint> endpoints = new ArrayList<>();
        for (NodeSeedContributor contributor : contributors) {
            endpoints.addAll(contributor.endpoints(node));
        }
        return endpoints;
    }

    /**
     * Removes node {@code nodeId}'s directory, its seed with it.
     *
     * @throws UncheckedIOException when it cannot be removed
     */
    public synchronized void delete(String nodeId) {
        Path dir = nodeDir(nodeId);
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(dir)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Could not remove the files of node " + nodeId + " in " + dir, e);
        }
        LOG.infov("Removed the files of node {0} in {1}", nodeId, dir);
    }

    /**
     * Every file of the seed but {@code version}, by name, in the order Floci writes them; adds the
     * names of the contributors that add to it to {@code seeded}.
     */
    private Map<String, String> files(NodeRecord node, Path seed, List<String> seeded) {
        Map<String, String> files = new LinkedHashMap<>();
        files.put(ENV_FILE, env());
        Map<String, List<String>> k3sArgs = new LinkedHashMap<>();
        for (String setting : ARG_SETTINGS) {
            k3sArgs.put(setting, new ArrayList<>());
        }
        k3sArgs.get(KUBELET_ARGS).add("provider-id=aws:///" + node.availabilityZone() + "/" + node.instanceId());
        k3sArgs.get(KUBELET_ARGS).add("node-labels=topology.kubernetes.io/zone=" + node.availabilityZone()
                + ",topology.kubernetes.io/region=" + node.region());
        k3sArgs.get(API_SERVER_ARGS).add("token-auth-file=" + NODE_TOKENS);
        for (NodeSeedContributor contributor : contributors) {
            Contribution contribution = contributor.contribute(node);
            if (contribution != Contribution.NONE) {
                seeded.add(contributor.name());
            }
            files.putAll(contribution.files());
            addArgs(k3sArgs, contribution.k3sArgs());
        }
        addArgs(k3sArgs, node.k3sArgs());
        files.put(K3S_FILE, k3sConfig(node, k3sArgs));
        String adminToken = existingAdminToken(seed.resolve(TOKENS_FILE));
        files.put(TOKENS_FILE, String.join(",", adminToken != null ? adminToken : newToken(), ADMIN_USER,
                ADMIN_USER, ADMIN_GROUP) + "\n");
        String apiToken = existingToken(seed.resolve(API_TOKEN_FILE));
        files.put(API_TOKEN_FILE, (apiToken != null ? apiToken : newToken()) + "\n");
        files.put(HOSTS_FILE, String.join("\n", hostNames()) + "\n");

        Map<String, String> ordered = new LinkedHashMap<>();
        for (String name : FILES) {
            if (files.containsKey(name)) {
                ordered.put(name, files.get(name));
            }
        }
        return ordered;
    }

    private static void addArgs(Map<String, List<String>> into, Map<String, List<String>> args) {
        for (Map.Entry<String, List<String>> setting : args.entrySet()) {
            if (!into.containsKey(setting.getKey())) {
                throw new IllegalArgumentException("not a k3s setting of the seed: " + setting.getKey());
            }
            into.get(setting.getKey()).addAll(setting.getValue());
        }
    }

    private String env() {
        StringBuilder env = new StringBuilder();
        for (Map.Entry<String, String> setting : settings.nodeEnv().entrySet()) {
            env.append(setting.getKey()).append('=').append(setting.getValue()).append('\n');
        }
        return env.toString();
    }

    /**
     * The k3s drop-in: the node's name, Floci's names in the API server's certificate, the node's
     * Service and pod ranges, and the argument lists of kubelet and the control plane.
     */
    private static String k3sConfig(NodeRecord node, Map<String, List<String>> k3sArgs) {
        Map<String, Object> k3s = new LinkedHashMap<>();
        k3s.put("node-name", node.nodeName());
        k3s.put("tls-san+", List.of(NodeSettings.DEFAULT_FLOCI_HOST));
        k3s.put("service-cidr", node.serviceCidr());
        k3s.put("cluster-cidr", node.podCidr());
        for (Map.Entry<String, List<String>> setting : k3sArgs.entrySet()) {
            boolean required = setting.getKey().equals(KUBELET_ARGS) || setting.getKey().equals(API_SERVER_ARGS);
            if (required || !setting.getValue().isEmpty()) {
                k3s.put(setting.getKey(), setting.getValue());
            }
        }
        try {
            return YAML.writeValueAsString(k3s);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not write the k3s settings of node " + node.nodeId(), e);
        }
    }

    /** The names that {@code floci-node} maps to the node's gateway, which reaches the Mac's loopback. */
    private Set<String> hostNames() {
        Set<String> names = new LinkedHashSet<>();
        names.add(NodeSettings.DEFAULT_FLOCI_HOST);
        String endpointHost = settings.flociEndpointHost();
        if (!NodeSettings.isIpv4(endpointHost)) {
            names.add(endpointHost);
        }
        return names;
    }

    private String newToken() {
        byte[] bytes = new byte[32];
        random.nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }

    private static String existingAdminToken(Path tokens) {
        String text = readIfExists(tokens);
        if (text == null) {
            return null;
        }
        String token = text.lines().findFirst().orElse("").split(",", 2)[0];
        return TOKEN.matcher(token).matches() ? token : null;
    }

    private static String existingToken(Path file) {
        String text = readIfExists(file);
        String token = text == null ? null : text.strip();
        return token != null && TOKEN.matcher(token).matches() ? token : null;
    }

    private static String readIfExists(Path file) {
        try {
            return Files.readString(file, StandardCharsets.UTF_8);
        } catch (NoSuchFileException e) {
            return null;
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read " + file, e);
        }
    }

    /** Creates {@code seed} and the directories above it up to the data path, each mode 0700. */
    private void createPrivateDirectories(Path seed) throws IOException {
        Path dataDir = seed.getParent().getParent();
        Files.createDirectories(dataDir.getParent());
        for (Path dir : List.of(dataDir, seed.getParent(), seed)) {
            createPrivateDirectory(dir);
        }
    }

    private static void createPrivateDirectory(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            Files.createDirectory(dir, PosixFilePermissions.asFileAttribute(DIRECTORY_MODE));
        }
        Files.setPosixFilePermissions(dir, DIRECTORY_MODE);
    }

    /** Writes {@code content} to a temporary file beside {@code name}, mode 0600, and renames it to {@code name}. */
    private static void writeFile(Path seed, String name, String content) throws IOException {
        Path target = seed.resolve(name);
        Path dir = target.getParent();
        if (!dir.equals(seed)) {
            createPrivateDirectory(dir);
        }
        Path temporary = dir.resolve("." + target.getFileName() + ".new");
        Files.deleteIfExists(temporary);
        Files.createFile(temporary, PosixFilePermissions.asFileAttribute(FILE_MODE));
        Files.writeString(temporary, content, StandardCharsets.UTF_8);
        Files.setPosixFilePermissions(temporary, FILE_MODE);
        Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }
}
