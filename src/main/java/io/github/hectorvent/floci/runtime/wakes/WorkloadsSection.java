package io.github.hectorvent.floci.runtime.wakes;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsRegions;
import io.github.hectorvent.floci.runtime.node.NodesSection;
import io.github.hectorvent.floci.runtime.node.RuntimeFile;
import io.github.hectorvent.floci.runtime.node.RuntimeFileException;
import io.github.hectorvent.floci.runtime.node.RuntimeFileFields;
import io.github.hectorvent.floci.runtime.node.RuntimeFileSection;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.PodSelector;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.RabbitMqQueue;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Target;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Workload;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The runtime file's {@code workloads}: the message consumers that Floci wakes when messages
 * arrive in their queues.
 *
 * <pre>
 * workloads:
 *   orders:
 *     queues: [orders-incoming]
 *     runtime:
 *       type: kubernetes
 *       cluster-name: integration-test
 *       namespace: integration-test
 *       deployment: orders
 * </pre>
 */
@ApplicationScoped
public class WorkloadsSection implements RuntimeFileSection<WorkloadDefinitions> {

    public static final String NAME = "workloads";

    static final int DEFAULT_IDLE_TIMEOUT_SECONDS = 120;
    static final int MAX_IDLE_TIMEOUT_SECONDS = 86_400;
    static final int DEFAULT_STARTUP_TIMEOUT_SECONDS = 30;
    static final int MAX_STARTUP_TIMEOUT_SECONDS = 120;

    private static final List<String> FIELDS = List.of("enabled", "account-id", "region", "idle-timeout-seconds",
            "startup-timeout-seconds", "queues", "rabbitmq-queues", "runtime");
    private static final List<String> RABBITMQ_FIELDS = List.of("broker-id", "virtual-host", "queue", "username-env",
            "password-env");
    private static final List<String> RUNTIME_FIELDS = List.of("type", "cluster-name", "node", "namespace",
            "deployment", "statefulset", "selector");

    private static final Pattern WORKLOAD_NAME = Pattern.compile("[A-Za-z0-9_.-]{1,64}");
    private static final Pattern ACCOUNT_ID = Pattern.compile("[0-9]{12}");
    private static final Pattern SQS_QUEUE = Pattern.compile("[A-Za-z0-9_-]{1,80}|[A-Za-z0-9_-]{1,75}\\.fifo");
    private static final Pattern BROKER_ID = Pattern.compile("[A-Za-z0-9-]{1,128}");
    private static final Pattern VARIABLE = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern CLUSTER_NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_-]{0,99}");
    private static final Pattern DNS_LABEL = Pattern.compile("[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?");
    private static final Pattern DNS_SUBDOMAIN =
            Pattern.compile("[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*");
    private static final Pattern LABEL_NAME = Pattern.compile("[A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?");
    private static final Pattern LABEL_VALUE = Pattern.compile("([A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?)?");
    private static final int MAX_SUBDOMAIN = 253;

    private final String defaultAccountId;
    private final String defaultRegion;
    private final NodesSection nodes;

    @Inject
    public WorkloadsSection(EmulatorConfig config, NodesSection nodes) {
        this(config.defaultAccountId(), config.defaultRegion(), nodes);
    }

    WorkloadsSection(String defaultAccountId, String defaultRegion, NodesSection nodes) {
        this.defaultAccountId = defaultAccountId;
        this.defaultRegion = defaultRegion;
        this.nodes = nodes;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public WorkloadDefinitions empty() {
        return WorkloadDefinitions.NONE;
    }

    @Override
    public WorkloadDefinitions read(JsonNode node, Path file) {
        RuntimeFileFields.mapping(node, NAME);
        Map<String, Workload> workloads = new LinkedHashMap<>();
        Map<Target, String> owners = new HashMap<>();
        for (Map.Entry<String, JsonNode> entry : node.properties()) {
            String name = entry.getKey();
            String path = RuntimeFileFields.path(NAME, name);
            if (!WORKLOAD_NAME.matcher(name).matches()) {
                throw new RuntimeFileException(path, "a workload name is 1 to 64 of A-Z, a-z, 0-9, _, . and -");
            }
            Workload workload = workload(name, entry.getValue(), path);
            String owner = owners.putIfAbsent(workload.target(), name);
            if (owner != null) {
                throw new RuntimeFileException(RuntimeFileFields.path(path, "runtime"),
                        "selects the same pods as " + RuntimeFileFields.path(NAME, owner));
            }
            workloads.put(name, workload);
        }
        return new WorkloadDefinitions(workloads);
    }

    /** Checks that each {@code runtime.node} names a node of the same file's {@code nodes}. */
    @Override
    public void check(WorkloadDefinitions definitions, RuntimeFile file) {
        Set<String> declared = new HashSet<>();
        for (SmolvmNode node : file.section(nodes).values()) {
            declared.add(node.name());
        }
        String prefix = SmolvmNode.declaredName("");
        for (Workload workload : definitions.workloads().values()) {
            String node = workload.target().node();
            if (node.startsWith(prefix) && !declared.contains(node)) {
                throw new RuntimeFileException(path(NAME, workload.name(), "runtime", "node"),
                        "names no node in nodes");
            }
        }
    }

    private Workload workload(String name, JsonNode node, String path) {
        RuntimeFileFields.mapping(node, path, FIELDS);
        boolean enabled = bool(node, path, "enabled", true);
        String accountId = accountId(node, path);
        String region = region(node, path);
        int idleTimeout = number(node, path, "idle-timeout-seconds", DEFAULT_IDLE_TIMEOUT_SECONDS, 0,
                MAX_IDLE_TIMEOUT_SECONDS);
        int startupTimeout = number(node, path, "startup-timeout-seconds", DEFAULT_STARTUP_TIMEOUT_SECONDS, 1,
                MAX_STARTUP_TIMEOUT_SECONDS);
        List<String> queues = queues(node, path);
        List<RabbitMqQueue> rabbitMqQueues = rabbitMqQueues(node, path);
        if (queues.isEmpty() && rabbitMqQueues.isEmpty()) {
            throw new RuntimeFileException(path, "needs at least one queue in queues or rabbitmq-queues");
        }
        JsonNode runtime = node.get("runtime");
        String runtimePath = RuntimeFileFields.path(path, "runtime");
        if (runtime == null) {
            throw new RuntimeFileException(runtimePath, "is required");
        }
        Target target = target(runtime, runtimePath, accountId, region);
        return new Workload(name, enabled, accountId, region, Duration.ofSeconds(idleTimeout),
                Duration.ofSeconds(startupTimeout), queues, rabbitMqQueues, target);
    }

    private String accountId(JsonNode node, String path) {
        JsonNode value = node.get("account-id");
        if (value == null) {
            return defaultAccountId;
        }
        if (!value.isTextual() || !ACCOUNT_ID.matcher(value.textValue()).matches()) {
            throw new RuntimeFileException(RuntimeFileFields.path(path, "account-id"),
                    "must be 12 digits in quotes, such as '000000000000'");
        }
        return value.textValue();
    }

    private String region(JsonNode node, String path) {
        String region = RuntimeFileFields.optionalText(node, path, "region");
        if (region == null) {
            return defaultRegion;
        }
        if (!region.equals(region.toLowerCase(Locale.ROOT)) || !AwsRegions.isRegionId(region)) {
            throw new RuntimeFileException(RuntimeFileFields.path(path, "region"),
                    "must be the code of an AWS region, in lower case");
        }
        return region;
    }

    private static List<String> queues(JsonNode node, String path) {
        String queuesPath = RuntimeFileFields.path(path, "queues");
        List<String> queues = new ArrayList<>();
        for (JsonNode queue : list(node, queuesPath, "queues")) {
            String queuePath = RuntimeFileFields.path(queuesPath, String.valueOf(queues.size()));
            if (!queue.isTextual() || !SQS_QUEUE.matcher(queue.textValue()).matches()) {
                throw new RuntimeFileException(queuePath, "must be an SQS queue name: 1 to 80 of A-Z, a-z, 0-9, _ "
                        + "and -, ending in .fifo for a FIFO queue");
            }
            if (queues.contains(queue.textValue())) {
                throw new RuntimeFileException(queuePath, queue.textValue() + " is listed twice");
            }
            queues.add(queue.textValue());
        }
        return queues;
    }

    private static List<RabbitMqQueue> rabbitMqQueues(JsonNode node, String path) {
        String listPath = RuntimeFileFields.path(path, "rabbitmq-queues");
        List<RabbitMqQueue> queues = new ArrayList<>();
        for (JsonNode entry : list(node, listPath, "rabbitmq-queues")) {
            String entryPath = RuntimeFileFields.path(listPath, String.valueOf(queues.size()));
            RuntimeFileFields.mapping(entry, entryPath, RABBITMQ_FIELDS);
            String brokerId = RuntimeFileFields.text(entry, entryPath, "broker-id");
            if (!BROKER_ID.matcher(brokerId).matches()) {
                throw new RuntimeFileException(RuntimeFileFields.path(entryPath, "broker-id"),
                        "must be an Amazon MQ broker id: 1 to 128 of A-Z, a-z, 0-9 and -");
            }
            String virtualHost = RuntimeFileFields.optionalText(entry, entryPath, "virtual-host");
            String queue = RuntimeFileFields.text(entry, entryPath, "queue");
            String usernameEnv = variable(entry, entryPath, "username-env");
            String passwordEnv = variable(entry, entryPath, "password-env");
            if ((usernameEnv == null) != (passwordEnv == null)) {
                throw new RuntimeFileException(entryPath, "needs both username-env and password-env, or neither");
            }
            RabbitMqQueue rabbitMqQueue = new RabbitMqQueue(brokerId, virtualHost == null ? "/" : virtualHost, queue,
                    usernameEnv, passwordEnv);
            for (RabbitMqQueue listed : queues) {
                if (listed.brokerId().equals(brokerId) && listed.virtualHost().equals(rabbitMqQueue.virtualHost())
                        && listed.queue().equals(queue)) {
                    throw new RuntimeFileException(entryPath, "the queue " + queue + " of broker " + brokerId
                            + " and virtual host " + rabbitMqQueue.virtualHost() + " is listed twice");
                }
            }
            queues.add(rabbitMqQueue);
        }
        return queues;
    }

    private static String variable(JsonNode node, String path, String key) {
        String name = RuntimeFileFields.optionalText(node, path, key);
        if (name != null && !VARIABLE.matcher(name).matches()) {
            throw new RuntimeFileException(RuntimeFileFields.path(path, key),
                    "must be an environment variable name: A-Z, a-z, 0-9 and _, not starting with a digit");
        }
        return name;
    }

    private static Target target(JsonNode runtime, String path, String accountId, String region) {
        RuntimeFileFields.mapping(runtime, path, RUNTIME_FIELDS);
        String type = RuntimeFileFields.text(runtime, path, "type");
        if (!type.equals("kubernetes")) {
            throw new RuntimeFileException(RuntimeFileFields.path(path, "type"), "must be kubernetes");
        }
        String clusterName = RuntimeFileFields.optionalText(runtime, path, "cluster-name");
        String node = RuntimeFileFields.optionalText(runtime, path, "node");
        if ((clusterName == null) == (node == null)) {
            throw new RuntimeFileException(path, "needs exactly one of cluster-name and node");
        }
        if (clusterName != null && !CLUSTER_NAME.matcher(clusterName).matches()) {
            throw new RuntimeFileException(RuntimeFileFields.path(path, "cluster-name"),
                    "must be an EKS cluster name: 1 to 100 of A-Z, a-z, 0-9, _ and -, starting with a letter or digit");
        }
        String namespace = RuntimeFileFields.optionalText(runtime, path, "namespace");
        if (namespace == null) {
            namespace = "default";
        } else if (!DNS_LABEL.matcher(namespace).matches()) {
            throw new RuntimeFileException(RuntimeFileFields.path(path, "namespace"), "must be a Kubernetes "
                    + "namespace: 1 to 63 of a-z, 0-9 and -, starting and ending with a letter or digit");
        }
        long selectors = Stream.of("deployment", "statefulset", "selector").filter(runtime::has).count();
        if (selectors != 1) {
            throw new RuntimeFileException(path, "needs exactly one of deployment, statefulset and selector");
        }
        PodSelector pods;
        if (runtime.has("deployment")) {
            pods = PodSelector.Controller.deployment(objectName(runtime, path, "deployment"));
        } else if (runtime.has("statefulset")) {
            pods = PodSelector.Controller.statefulSet(objectName(runtime, path, "statefulset"));
        } else {
            pods = labels(runtime.get("selector"), RuntimeFileFields.path(path, "selector"));
        }
        String nodeName = clusterName != null
                ? SmolvmNode.clusterName(accountId, region, clusterName)
                : SmolvmNode.declaredName(node);
        return new Target(nodeName, namespace, pods);
    }

    private static String objectName(JsonNode runtime, String path, String key) {
        String name = RuntimeFileFields.text(runtime, path, key);
        if (name.length() > MAX_SUBDOMAIN || !DNS_SUBDOMAIN.matcher(name).matches()) {
            throw new RuntimeFileException(RuntimeFileFields.path(path, key), "must be a Kubernetes object name: "
                    + "at most 253 of a-z, 0-9, - and ., starting and ending with a letter or digit");
        }
        return name;
    }

    private static PodSelector labels(JsonNode selector, String path) {
        RuntimeFileFields.mapping(selector, path);
        if (selector.isEmpty()) {
            throw new RuntimeFileException(path, "needs at least one label");
        }
        Map<String, String> labels = new LinkedHashMap<>();
        for (Map.Entry<String, JsonNode> label : selector.properties()) {
            String key = label.getKey();
            String labelPath = RuntimeFileFields.path(path, key);
            if (!labelName(key)) {
                throw new RuntimeFileException(labelPath, "must be a Kubernetes label name, such as app or "
                        + "app.kubernetes.io/name");
            }
            String text = labelValue(label.getValue());
            if (text == null || !LABEL_VALUE.matcher(text).matches()) {
                throw new RuntimeFileException(labelPath, "must be a Kubernetes label value in quotes: at most 63 of "
                        + "A-Z, a-z, 0-9, -, _ and ., starting and ending with a letter or digit");
            }
            labels.put(key, text);
        }
        return new PodSelector.Labels(labels);
    }

    /** The label value of a selector entry; YAML reads an empty one as null. */
    private static String labelValue(JsonNode value) {
        if (value.isNull()) {
            return "";
        }
        return value.isTextual() ? value.textValue() : null;
    }

    private static boolean labelName(String key) {
        int slash = key.indexOf('/');
        String name = slash < 0 ? key : key.substring(slash + 1);
        if (slash >= 0) {
            String prefix = key.substring(0, slash);
            if (prefix.isEmpty() || prefix.length() > MAX_SUBDOMAIN || !DNS_SUBDOMAIN.matcher(prefix).matches()) {
                return false;
            }
        }
        return LABEL_NAME.matcher(name).matches();
    }

    private static String path(String... segments) {
        String path = "";
        for (String segment : segments) {
            path = RuntimeFileFields.path(path, segment);
        }
        return path;
    }

    private static Iterable<JsonNode> list(JsonNode node, String path, String key) {
        JsonNode value = node.get(key);
        if (value == null) {
            return List.of();
        }
        if (!value.isArray()) {
            throw new RuntimeFileException(path, "must be a list");
        }
        return value;
    }

    private static boolean bool(JsonNode node, String path, String key, boolean fallback) {
        JsonNode value = node.get(key);
        if (value == null) {
            return fallback;
        }
        if (!value.isBoolean()) {
            throw new RuntimeFileException(RuntimeFileFields.path(path, key), "must be true or false");
        }
        return value.booleanValue();
    }

    private static int number(JsonNode node, String path, String key, int fallback, int min, int max) {
        JsonNode value = node.get(key);
        if (value == null) {
            return fallback;
        }
        if (!value.isIntegralNumber() || !value.canConvertToInt() || value.intValue() < min
                || value.intValue() > max) {
            throw new RuntimeFileException(RuntimeFileFields.path(path, key),
                    "must be a whole number from " + min + " to " + max);
        }
        return value.intValue();
    }
}
