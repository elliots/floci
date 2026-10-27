package io.github.hectorvent.floci.runtime.flocinode;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.RuntimeConfig.NodeConfig;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.runtime.node.NodesSection;
import io.github.hectorvent.floci.runtime.node.RuntimeFileWatcher;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The settings of the node that Floci creates, {@code floci.runtime.node.*}, and the checks they
 * must pass. Floci creates a node only natively on an Apple silicon Mac, not in a container, with
 * {@code node-vm.sh}, the smolvm checkout and the node image in place and every setting valid.
 * When RDS or EKS would create the node and the runtime file lists none, Floci refuses to start
 * on a problem.
 */
@ApplicationScoped
public class NodeSettings {

    /** {@code host.docker.internal} reaches the Mac's loopback from a node's processes. */
    public static final String DEFAULT_FLOCI_HOST = "host.docker.internal";

    private static final String KEY_PREFIX = "floci.runtime.node.";
    private static final Pattern SHA256 = Pattern.compile("[0-9a-fA-F]{64}");
    private static final Pattern HOST_NAME = Pattern.compile("[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?");
    private static final Pattern IPV4 = Pattern.compile("(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})");
    private static final Pattern PORT_RANGE = Pattern.compile("(\\d{1,5})(?:-(\\d{1,5}))?");
    /** A Go duration, as the node service reads one: numbers with units, such as {@code 1m30s}. */
    private static final Pattern GO_DURATION =
            Pattern.compile("(?:(?:\\d+\\.?\\d*|\\.\\d+)(?:ns|us|µs|μs|ms|s|m|h))+");
    private static final Pattern VCPUS = Pattern.compile("\\d+(?:\\.\\d+)?|\\.\\d+");
    private static final Pattern VISIBLE_ASCII = Pattern.compile("[\\x21-\\x7e]+");

    private final EmulatorConfig config;
    private final BooleanSupplier inContainer;
    private final BooleanSupplier runtimeFileHasNodes;
    private final String osName;
    private final String osArch;
    private volatile ImageDigest imageDigest;

    @Inject
    public NodeSettings(EmulatorConfig config, ContainerDetector containerDetector, RuntimeFileWatcher runtimeFile,
                        NodesSection nodesSection) {
        this(config, containerDetector::isRunningInContainer,
                () -> !runtimeFile.current().section(nodesSection).isEmpty(), System.getProperty("os.name", ""),
                System.getProperty("os.arch", ""));
    }

    public NodeSettings(EmulatorConfig config, BooleanSupplier inContainer, BooleanSupplier runtimeFileHasNodes,
                           String osName, String osArch) {
        this.config = config;
        this.inContainer = inContainer;
        this.runtimeFileHasNodes = runtimeFileHasNodes;
        this.osName = osName;
        this.osArch = osArch;
    }

    void onStart(@Observes @Priority(Interceptor.Priority.APPLICATION + 100) StartupEvent ignored) {
        requireValid();
    }

    /**
     * Refuses every problem of {@link #problems()} when RDS or EKS would have Floci create the node
     * and the runtime file lists none.
     *
     * @throws IllegalStateException naming each problem
     */
    public void requireValid() {
        if (!wanted() || runtimeFileHasNodes.getAsBoolean()) {
            return;
        }
        List<String> problems = problems();
        if (!problems.isEmpty()) {
            throw new IllegalStateException("Floci cannot create the smolvm-pods node that " + users() + " use: "
                    + String.join("; ", problems));
        }
    }

    /**
     * Whether a service runs on the node: RDS or Lambda with smolvm on, or EKS with the smolvm-pods
     * provider, each real.
     */
    public boolean wanted() {
        return rdsWants() || eksWants() || lambdaWants() || elasticacheWants() || ecrWants();
    }

    private boolean ecrWants() {
        EmulatorConfig.EcrServiceConfig ecr = config.services().ecr();
        return ecr != null && ecr.enabled() && ecr.smolvmEnabled();
    }

    private boolean elasticacheWants() {
        EmulatorConfig.ElastiCacheServiceConfig cache = config.services().elasticache();
        return cache != null && cache.enabled() && cache.smolvm().enabled();
    }

    private boolean rdsWants() {
        EmulatorConfig.RdsServiceConfig rds = config.services().rds();
        return rds.enabled() && !rds.mock() && rds.smolvm().enabled();
    }

    private boolean eksWants() {
        EmulatorConfig.EksServiceConfig eks = config.services().eks();
        return eks.enabled() && !eks.mock() && "smolvm-pods".equals(eks.provider());
    }

    private boolean lambdaWants() {
        EmulatorConfig.LambdaServiceConfig lambda = config.services().lambda();
        return lambda.enabled() && lambda.smolvm().enabled();
    }

    private String users() {
        List<String> users = new ArrayList<>();
        if (rdsWants()) {
            users.add("RDS");
        }
        if (eksWants()) {
            users.add("EKS");
        }
        if (lambdaWants()) {
            users.add("Lambda");
        }
        if (elasticacheWants()) {
            users.add("ElastiCache");
        }
        if (ecrWants()) {
            users.add("ECR");
        }
        return users.size() < 2 ? String.join("", users)
                : String.join(", ", users.subList(0, users.size() - 1)) + " and " + users.getLast();
    }

    /**
     * What keeps Floci from creating the node: where it runs, a missing or wrong path, a node
     * image whose SHA-256 is not {@code image-sha256}, or a setting out of range. Empty when
     * nothing does. The node image's SHA-256 is computed again only when its size or modification
     * time changed.
     */
    public List<String> problems() {
        NodeConfig node = node();
        List<String> problems = new ArrayList<>();
        if (!osName.startsWith("Mac") || !("aarch64".equals(osArch) || "arm64".equals(osArch))) {
            problems.add("Floci must run on an Apple silicon Mac, not " + osName + " on " + osArch);
        }
        if (inContainer.getAsBoolean()) {
            problems.add("Floci runs in a container, and must run natively on the Mac, as a JVM or the native binary");
        }
        Path script = absolutePath(problems, "node-vm-script", text(node.nodeVmScript()));
        if (script != null && !(Files.isRegularFile(script) && Files.isExecutable(script))) {
            problems.add(key("node-vm-script") + " is not an executable file: " + script);
        }
        Path smolvmSrc = absolutePath(problems, "smolvm-src", text(node.smolvmSrc()));
        if (smolvmSrc != null && !Files.isDirectory(smolvmSrc)) {
            problems.add(key("smolvm-src") + " is not a directory: " + smolvmSrc);
        }
        checkImage(problems, text(node.image()), text(node.imageSha256()));
        positive(problems, "cpus", node.cpus());
        positive(problems, "memory-mib", node.memoryMib());
        positive(problems, "storage-gib", node.storageGib());
        positive(problems, "overlay-gib", node.overlayGib());
        positive(problems, "ready-timeout-seconds", node.readyTimeoutSeconds());
        checkPorts(problems, node);
        checkVolumes(problems, node.volumes().orElse(List.of()));
        checkFlociEndpoint(problems, text(node.flociEndpoint()));
        duration(problems, "idle-timeout", text(node.idleTimeout()));
        duration(problems, "cpu-window", text(node.cpuWindow()));
        duration(problems, "admission-wait", text(node.admissionWait()));
        String cpuThreshold = text(node.cpuThreshold());
        if (!cpuThreshold.isEmpty() && !cpuThreshold.equalsIgnoreCase("off") && !VCPUS.matcher(cpuThreshold).matches()) {
            problems.add(key("cpu-threshold") + " must be vCPUs, such as 0.5, or off, not \"" + cpuThreshold + "\"");
        }
        notNegative(problems, "max-awake-pods", node.maxAwakePods());
        notNegative(problems, "max-awake-memory-mib", node.maxAwakeMemoryMib());
        notNegative(problems, "reported-memory-gib", node.reportedMemoryGib());
        notNegative(problems, "reported-cpus", node.reportedCpus());
        String rustLog = text(node.rustLog());
        if (!rustLog.isEmpty() && !VISIBLE_ASCII.matcher(rustLog).matches()) {
            problems.add(key("rust-log") + " must be one word of visible ASCII characters");
        }
        return problems;
    }

    /** Where k3s and containerd in the node reach Floci, {@code HOST:PORT}. */
    public String flociEndpoint() {
        String endpoint = text(node().flociEndpoint());
        return endpoint.isEmpty() ? DEFAULT_FLOCI_HOST + ":" + config.port() : endpoint;
    }

    /** The host of {@link #flociEndpoint()}. */
    public String flociEndpointHost() {
        String endpoint = flociEndpoint();
        return endpoint.substring(0, endpoint.lastIndexOf(':'));
    }

    /**
     * The settings of the node's seed {@code env}, in order: every one present, with an empty value
     * where the node keeps its own default.
     */
    Map<String, String> nodeEnv() {
        NodeConfig node = node();
        Map<String, String> env = new LinkedHashMap<>();
        env.put("FLOCI_NODE_IDLE_TIMEOUT", text(node.idleTimeout()));
        env.put("FLOCI_NODE_CPU_THRESHOLD", text(node.cpuThreshold()));
        env.put("FLOCI_NODE_CPU_WINDOW", text(node.cpuWindow()));
        env.put("FLOCI_NODE_MAX_AWAKE_PODS", text(node.maxAwakePods()));
        env.put("FLOCI_NODE_MAX_AWAKE_MEMORY", text(node.maxAwakeMemoryMib()));
        env.put("FLOCI_NODE_ADMISSION_WAIT", text(node.admissionWait()));
        env.put("FLOCI_NODE_MEMORY_GIB", text(node.reportedMemoryGib()));
        env.put("FLOCI_NODE_CPUS", text(node.reportedCpus()));
        env.put("RUST_LOG", text(node.rustLog()));
        return env;
    }

    private NodeConfig node() {
        return config.runtime().node();
    }

    /**
     * The first and last port of a {@code node-ports} value, or empty for {@code none}.
     *
     * @throws IllegalArgumentException when the value is not {@code FIRST-LAST}, {@code PORT} or {@code none}
     */
    static Optional<PortRange> nodePorts(String value) {
        if ("none".equals(value)) {
            return Optional.empty();
        }
        Matcher matcher = PORT_RANGE.matcher(value == null ? "" : value);
        boolean matches = matcher.matches();
        int first = matches ? Integer.parseInt(matcher.group(1)) : 0;
        int last = matches && matcher.group(2) != null ? Integer.parseInt(matcher.group(2)) : first;
        if (first < 1 || last > 65535 || first > last) {
            throw new IllegalArgumentException("must be FIRST-LAST, PORT or none, with ports from 1 to 65535, not \""
                    + value + "\"");
        }
        return Optional.of(new PortRange(first, last));
    }

    /** Whether {@code host} is an IPv4 address rather than a name. */
    public static boolean isIpv4(String host) {
        Matcher matcher = IPV4.matcher(host);
        if (!matcher.matches()) {
            return false;
        }
        for (int group = 1; group <= 4; group++) {
            if (Integer.parseInt(matcher.group(group)) > 255) {
                return false;
            }
        }
        return true;
    }

    /** {@code floci.runtime.node.NAME (FLOCI_RUNTIME_NODE_NAME)}. */
    public static String key(String name) {
        return KEY_PREFIX + name + " (FLOCI_RUNTIME_NODE_" + name.toUpperCase(Locale.ROOT).replace('-', '_') + ")";
    }

    /** A setting's value, or empty when it is not set. */
    private static String text(Optional<String> value) {
        return value.map(String::strip).orElse("");
    }

    private static String text(OptionalInt value) {
        return value.isPresent() ? Integer.toString(value.getAsInt()) : "";
    }

    /** The path that setting {@code name} holds, or null, with the problem, when it is unset or relative. */
    private static Path absolutePath(List<String> problems, String name, String value) {
        if (value.isEmpty()) {
            problems.add(key(name) + " is not set");
            return null;
        }
        Path path = Path.of(value);
        if (!path.isAbsolute()) {
            problems.add(key(name) + " must be an absolute path, not " + value);
            return null;
        }
        return path;
    }

    private void checkImage(List<String> problems, String imageSetting, String expected) {
        Path image = absolutePath(problems, "image", imageSetting);
        boolean validDigest = SHA256.matcher(expected).matches();
        if (expected.isEmpty()) {
            problems.add(key("image-sha256") + " is not set");
        } else if (!validDigest) {
            problems.add(key("image-sha256") + " must be 64 hex digits");
        }
        if (image == null) {
            return;
        }
        if (!Files.isRegularFile(image)) {
            problems.add(key("image") + " is not a file: " + image);
            return;
        }
        if (!validDigest) {
            return;
        }
        try {
            String actual = sha256(image);
            if (!actual.equalsIgnoreCase(expected)) {
                problems.add("the SHA-256 of " + image + " is " + actual + ", not " + key("image-sha256") + " "
                        + expected.toLowerCase(Locale.ROOT));
            }
        } catch (IOException e) {
            problems.add(key("image") + " cannot be read: " + e.getMessage());
        }
    }

    private String sha256(Path image) throws IOException {
        BasicFileAttributes attributes = Files.readAttributes(image, BasicFileAttributes.class);
        ImageDigest known = imageDigest;
        if (known != null && known.matches(image, attributes)) {
            return known.sha256();
        }
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
        try (InputStream in = new DigestInputStream(Files.newInputStream(image), digest)) {
            in.transferTo(OutputStream.nullOutputStream());
        }
        String sha256 = HexFormat.of().formatHex(digest.digest());
        imageDigest = new ImageDigest(image, attributes.size(), attributes.lastModifiedTime(), sha256);
        return sha256;
    }

    private static void positive(List<String> problems, String name, int value) {
        if (value < 1) {
            problems.add(key(name) + " must be at least 1, not " + value);
        }
    }

    private static void notNegative(List<String> problems, String name, OptionalInt value) {
        if (value.isPresent() && value.getAsInt() < 0) {
            problems.add(key(name) + " must be 0 or more, not " + value.getAsInt());
        }
    }

    private static void duration(List<String> problems, String name, String value) {
        if (!value.isEmpty() && (!GO_DURATION.matcher(value).matches() || !value.matches(".*[1-9].*"))) {
            problems.add(key(name) + " must be a duration above 0, such as 30s, 5m or 1m30s, not \"" + value + "\"");
        }
    }

    private static void checkPorts(List<String> problems, NodeConfig node) {
        PortRange api = new PortRange(node.apiServerBasePort(), node.apiServerMaxPort());
        if (!api.valid()) {
            problems.add(key("api-server-base-port") + " and " + key("api-server-max-port")
                    + " must be a range of ports from 1 to 65535, not " + api.first() + " to " + api.last());
            return;
        }
        PortRange service = new PortRange(node.nodeServiceBasePort(), node.nodeServiceMaxPort());
        if (!service.valid()) {
            problems.add(key("node-service-base-port") + " and " + key("node-service-max-port")
                    + " must be a range of ports from 1 to 65535, not " + service.first() + " to " + service.last());
            return;
        }
        Optional<PortRange> nodePorts;
        try {
            nodePorts = nodePorts(node.nodePorts());
        } catch (IllegalArgumentException e) {
            problems.add(key("node-ports") + " " + e.getMessage());
            return;
        }
        if (service.overlaps(api)) {
            problems.add(key("node-service-base-port") + " to " + service.last() + " overlaps "
                    + key("api-server-base-port") + " to " + api.last());
        }
        if (nodePorts.filter(ports -> ports.overlaps(api) || ports.overlaps(service)).isPresent()) {
            problems.add(key("node-ports") + " " + node.nodePorts()
                    + " overlaps the Kubernetes API or node service ports");
        }
        int mappings = 2 + nodePorts.map(ports -> ports.last() - ports.first() + 1).orElse(0);
        Set<Integer> hosts = new HashSet<>();
        for (String mapping : node.extraPorts().orElse(List.of())) {
            String[] ports = mapping.split(":", -1);
            int host;
            int guest;
            try {
                if (ports.length != 2 || !ports[0].matches("[0-9]{1,5}") || !ports[1].matches("[0-9]{1,5}")) {
                    throw new IllegalArgumentException();
                }
                host = Integer.parseInt(ports[0]);
                guest = Integer.parseInt(ports[1]);
                if (host < 1 || host > 65535 || guest < 1 || guest > 65535) {
                    throw new IllegalArgumentException();
                }
            } catch (IllegalArgumentException e) {
                problems.add(key("extra-ports") + " must contain HOST_PORT:GUEST_PORT with ports from 1 to 65535: " + mapping);
                continue;
            }
            PortRange extra = new PortRange(host, host);
            if (!hosts.add(host) || extra.overlaps(api) || extra.overlaps(service)
                    || nodePorts.filter(extra::overlaps).isPresent()) {
                problems.add(key("extra-ports") + " host port overlaps another node mapping or API allocation range: " + host);
            }
            mappings++;
        }
        if (mappings > 64) {
            problems.add(key("extra-ports") + " and " + key("node-ports")
                    + " exceed smolvm's 64 mappings, including the two API ports: " + mappings);
        }
    }

    private static void checkVolumes(List<String> problems, List<String> volumes) {
        Set<String> targets = new HashSet<>();
        for (String volume : volumes) {
            String[] parts = volume.split(":", -1);
            if (parts.length < 2 || parts.length > 3 || (parts.length == 3 && !parts[2].equals("ro"))) {
                problems.add(key("volumes") + " must contain HOST:GUEST[:ro]: " + volume);
                continue;
            }
            try {
                Path host = Path.of(parts[0]);
                Path guest = Path.of(parts[1]);
                if (!host.isAbsolute() || !Files.isDirectory(host)) {
                    problems.add(key("volumes") + " host must be an existing absolute directory: " + parts[0]);
                }
                if (!guest.isAbsolute() || guest.normalize().getNameCount() == 0
                        || !guest.equals(guest.normalize()) || !targets.add(guest.toString())) {
                    problems.add(key("volumes") + " guest must be a unique absolute directory other than /: " + parts[1]);
                }
            } catch (IllegalArgumentException e) {
                problems.add(key("volumes") + " contains an invalid path: " + volume);
            }
        }
    }

    private static void checkFlociEndpoint(List<String> problems, String endpoint) {
        if (endpoint.isEmpty()) {
            return;
        }
        int colon = endpoint.lastIndexOf(':');
        String host = colon > 0 ? endpoint.substring(0, colon) : "";
        String port = colon > 0 ? endpoint.substring(colon + 1) : "";
        boolean validPort = port.matches("\\d{1,5}") && Integer.parseInt(port) >= 1 && Integer.parseInt(port) <= 65535;
        boolean validHost = isIpv4(host) || (HOST_NAME.matcher(host).matches() && !host.matches("[0-9.]+"));
        if (!validPort || !validHost) {
            problems.add(key("floci-endpoint") + " must be HOST:PORT, with a host name or an IPv4 address, not \""
                    + endpoint + "\"");
        }
    }

    /** Ports {@code first} to {@code last}, both included. */
    record PortRange(int first, int last) {

        boolean overlaps(PortRange other) {
            return first <= other.last && other.first <= last;
        }

        boolean valid() {
            return first >= 1 && last <= 65535 && first <= last;
        }
    }

    private record ImageDigest(Path path, long size, FileTime modified, String sha256) {

        boolean matches(Path image, BasicFileAttributes attributes) {
            return path.equals(image) && size == attributes.size() && modified.equals(attributes.lastModifiedTime());
        }
    }
}
