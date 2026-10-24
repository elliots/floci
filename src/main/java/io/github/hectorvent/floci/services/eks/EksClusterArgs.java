package io.github.hectorvent.floci.services.eks;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.core.common.ReservedTags;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Handles caller-supplied k3s arguments configured via reserved tags on cluster creation.
 * Supported components include kubelet, kube-apiserver, kube-controller-manager, and kube-scheduler.
 */
public final class EksClusterArgs {

    private static final Logger LOG = Logger.getLogger(EksClusterArgs.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public enum Component {
        KUBELET("--kubelet-arg="),
        KUBE_APISERVER("--kube-apiserver-arg="),
        KUBE_CONTROLLER_MANAGER("--kube-controller-manager-arg="),
        KUBE_SCHEDULER("--kube-scheduler-arg="),
        KUBE_CLOUD_CONTROLLER_MANAGER("--kube-cloud-controller-manager-arg=");

        private final String flagPrefix;

        Component(String flagPrefix) {
            this.flagPrefix = flagPrefix;
        }

        public String flagPrefix() {
            return flagPrefix;
        }
    }

    private record ComponentTagMapping(Component component, String name) {}

    private static final List<ComponentTagMapping> MAPPINGS = List.of(
            new ComponentTagMapping(Component.KUBELET, "kubelet"),
            new ComponentTagMapping(Component.KUBE_APISERVER, "kube-apiserver"),
            new ComponentTagMapping(Component.KUBE_APISERVER, "apiserver"),
            new ComponentTagMapping(Component.KUBE_CONTROLLER_MANAGER, "kube-controller-manager"),
            new ComponentTagMapping(Component.KUBE_CONTROLLER_MANAGER, "controller-manager"),
            new ComponentTagMapping(Component.KUBE_SCHEDULER, "kube-scheduler"),
            new ComponentTagMapping(Component.KUBE_SCHEDULER, "scheduler"),
            new ComponentTagMapping(Component.KUBE_CLOUD_CONTROLLER_MANAGER, "kube-cloud-controller-manager")
    );

    /**
     * Arguments Floci sets for kubelet to manage node identity, topology and node capacity budget.
     * Normalized without hyphens for case/hyphen-insensitive collision matching.
     */
    static final Set<String> FLOCI_MANAGED_KUBELET_FLAGS = Set.of(
            "providerid",
            "nodelabels",
            "systemreserved",
            "kubereserved",
            "evictionhard",
            "registerwithtaints"
    );

    /**
     * Arguments Floci sets for kube-apiserver for IAM token webhook authentication, IRSA signing,
     * and audit logging. Normalized without hyphens.
     */
    static final Set<String> FLOCI_MANAGED_APISERVER_FLAGS = Set.of(
            "authenticationtokenwebhookconfigfile",
            "authenticationtokenwebhookversion",
            "authenticationtokenwebhookcachettl",
            "serviceaccountissuer",
            "serviceaccountkeyfile",
            "serviceaccountsigningkeyfile",
            "auditpolicyfile",
            "auditlogpath",
            "auditlogmaxage",
            "auditlogmaxbackup",
            "auditlogmaxsize"
    );

    /**
     * Flags refused outright because they conflict with the k3s embedded SQLite (kine) engine
     * or bypass API server RBAC authorization.
     */
    static final Set<String> REFUSED_APISERVER_FLAGS = Set.of(
            "storagebackend",
            "etcdservers",
            "authorizationmode"
    );

    /**
     * Flags that a smolvm-pods node takes from its seed or its image, in any component: the node's
     * name, its Service and pod ranges and the static token file, which Floci sets, and the
     * {@code resolv-conf} of the node image. Normalized without hyphens.
     */
    static final Set<String> SMOLVM_NODE_FLAGS = Set.of(
            "tokenauthfile",
            "nodename",
            "hostnameoverride",
            "servicecidr",
            "serviceclusteriprange",
            "clustercidr",
            "resolvconf"
    );

    private static final String SMOLVM_CPU_MANAGER_POLICY = "none";
    private static final String SMOLVM_MEMORY_MANAGER_POLICY = "None";

    private static final TypeReference<List<String>> STRING_LIST_TYPE = new TypeReference<>() {};

    private EksClusterArgs() {
    }

    public static class CollisionException extends IllegalArgumentException {
        public CollisionException(String message) {
            super(message);
        }
    }

    /**
     * Parses and validates cluster arguments supplied in tags, returning a list of k3s argument flags.
     * Malformed entries log a warning and are skipped so the cluster still starts.
     * Collisions with Floci-managed arguments throw {@link CollisionException}.
     */
    public static List<String> parseAndValidateClusterArgs(Map<String, String> tags, String clusterName) {
        if (tags == null || tags.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> result = new ArrayList<>();

        for (Map.Entry<String, String> entry : tags.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (key == null || !key.startsWith(ReservedTags.RESERVED_PREFIX)) {
                continue;
            }

            ParsedTag parsedTag = matchComponent(key);
            if (parsedTag == null) {
                continue;
            }

            try {
                List<String> rawFlags = extractRawFlags(parsedTag, key, value);
                for (String rawFlag : rawFlags) {
                    if (rawFlag == null || rawFlag.isBlank()) {
                        LOG.warnv("Malformed EKS cluster argument {0}={1} for cluster {2}: empty argument flag",
                                key, value, clusterName);
                        continue;
                    }
                    if (hasControlCharacters(rawFlag)) {
                        LOG.warnv("Malformed EKS cluster argument {0}={1} for cluster {2}: argument contains control characters",
                                key, value, clusterName);
                        continue;
                    }

                    String normalized = normalizeFlag(parsedTag.component(), rawFlag);
                    validateCollision(parsedTag.component(), normalized);
                    result.add(parsedTag.component().flagPrefix() + normalized);
                }
            } catch (CollisionException e) {
                // Collisions and refused arguments must be propagated to reject the request
                throw e;
            } catch (Exception e) {
                LOG.warnv("Malformed EKS cluster argument {0}={1} for cluster {2}: {3}",
                        key, value, clusterName, e.getMessage());
            }
        }

        return result;
    }

    /**
     * Validates a list of already-assembled server arguments to guarantee no Floci-managed or
     * refused argument collides.
     */
    public static void validateClusterArgs(List<String> serverArgs) {
        if (serverArgs == null || serverArgs.isEmpty()) {
            return;
        }
        for (String arg : serverArgs) {
            if (arg == null || arg.isBlank()) {
                continue;
            }
            for (Component component : Component.values()) {
                if (arg.startsWith(component.flagPrefix())) {
                    String flag = arg.substring(component.flagPrefix().length());
                    validateCollision(component, flag);
                    break;
                }
            }
        }
    }

    /**
     * Refuses the arguments that clusters of {@code provider} cannot take. A {@code smolvm-pods}
     * node refuses {@link #SMOLVM_NODE_FLAGS}, a {@code memory.available} threshold in
     * {@code eviction-hard} or {@code eviction-soft}, and a CPU or memory manager policy other than
     * {@code none} and {@code None}, which its capacity view does not allow. A k3s container takes
     * every argument that {@link #validateClusterArgs} accepts.
     *
     * @throws CollisionException naming the first refused argument
     */
    public static void validateProviderArgs(String provider, List<String> serverArgs) {
        if (!EksSmolvmSettings.PROVIDER_SMOLVM_PODS.equals(provider) || serverArgs == null) {
            return;
        }
        for (String arg : serverArgs) {
            if (arg == null || arg.isBlank()) {
                continue;
            }
            for (Component component : Component.values()) {
                if (arg.startsWith(component.flagPrefix())) {
                    validateSmolvmArg(component, arg.substring(component.flagPrefix().length()));
                    break;
                }
            }
        }
    }

    private static void validateSmolvmArg(Component component, String flag) {
        String rawName = extractFlagName(flag);
        String flagName = rawName.replace("-", "").toLowerCase(Locale.ROOT);
        int eqIndex = flag.indexOf('=');
        String value = eqIndex >= 0 ? flag.substring(eqIndex + 1).trim() : null;
        if (SMOLVM_NODE_FLAGS.contains(flagName)) {
            throw new CollisionException("Cluster argument '" + rawName
                    + "' is set by the smolvm-pods node and cannot be overridden on a smolvm-pods cluster.");
        }
        if (component != Component.KUBELET) {
            return;
        }
        boolean memoryEviction = ("evictionhard".equals(flagName) || "evictionsoft".equals(flagName))
                && value != null && value.toLowerCase(Locale.ROOT).contains("memory.available");
        boolean cpuManager = "cpumanagerpolicy".equals(flagName) && !SMOLVM_CPU_MANAGER_POLICY.equals(value);
        boolean memoryManager = "memorymanagerpolicy".equals(flagName)
                && !SMOLVM_MEMORY_MANAGER_POLICY.equals(value);
        if (memoryEviction || cpuManager || memoryManager) {
            throw new CollisionException("Cluster argument '" + flag.trim()
                    + "' is refused on a smolvm-pods cluster: the node reports a larger capacity than it has, "
                    + "so it allows no memory.available eviction threshold, and no CPU or memory manager.");
        }
    }

    private record ParsedTag(Component component, TagKind kind, String prefixMatched) {}

    private enum TagKind {
        PREFIXED,
        SINGLE,
        PLURAL
    }

    static ParsedTag matchComponent(String key) {
        for (ComponentTagMapping m : MAPPINGS) {
            String prefix = ReservedTags.RESERVED_PREFIX + m.name() + "-arg:";
            if (key.startsWith(prefix)) {
                return new ParsedTag(m.component(), TagKind.PREFIXED, prefix);
            }
            String single = ReservedTags.RESERVED_PREFIX + m.name() + "-arg";
            if (key.equals(single)) {
                return new ParsedTag(m.component(), TagKind.SINGLE, single);
            }
            String plural = ReservedTags.RESERVED_PREFIX + m.name() + "-args";
            if (key.equals(plural)) {
                return new ParsedTag(m.component(), TagKind.PLURAL, plural);
            }
        }
        return null;
    }

    private static List<String> extractRawFlags(ParsedTag parsedTag, String key, String value) throws Exception {
        List<String> flags = new ArrayList<>();
        switch (parsedTag.kind()) {
            case PREFIXED -> {
                String flagSuffix = key.substring(parsedTag.prefixMatched().length()).trim();
                if (flagSuffix.isBlank()) {
                    if (value == null || value.isBlank()) {
                        throw new IllegalArgumentException("flag name is blank");
                    }
                    flags.add(value.trim());
                } else if (flagSuffix.contains("=")) {
                    flags.add(flagSuffix);
                } else if (value != null && !value.isBlank() && !"true".equalsIgnoreCase(value.trim())) {
                    flags.add(flagSuffix + "=" + value.trim());
                } else {
                    flags.add(flagSuffix);
                }
            }
            case SINGLE -> {
                if (value == null || value.isBlank()) {
                    throw new IllegalArgumentException("tag value is blank");
                }
                flags.add(value.trim());
            }
            case PLURAL -> {
                if (value == null || value.isBlank()) {
                    throw new IllegalArgumentException("tag value is blank");
                }
                String trimmedValue = value.trim();
                if (trimmedValue.startsWith("[")) {
                    List<String> list = MAPPER.readValue(trimmedValue, STRING_LIST_TYPE);
                    if (list != null) {
                        for (String item : list) {
                            if (item != null && !item.isBlank()) {
                                flags.add(item.trim());
                            }
                        }
                    }
                } else if (trimmedValue.contains("\n")) {
                    for (String line : trimmedValue.split("\n")) {
                        String lineTrimmed = line.trim();
                        if (!lineTrimmed.isBlank()) {
                            flags.add(lineTrimmed);
                        }
                    }
                } else if (trimmedValue.contains(",") && !trimmedValue.contains("=")) {
                    for (String item : trimmedValue.split(",")) {
                        String itemTrimmed = item.trim();
                        if (!itemTrimmed.isBlank()) {
                            flags.add(itemTrimmed);
                        }
                    }
                } else {
                    flags.add(trimmedValue);
                }
            }
        }
        return flags;
    }

    private static String normalizeFlag(Component component, String rawFlag) {
        String flag = rawFlag.trim();
        if (flag.startsWith(component.flagPrefix())) {
            flag = flag.substring(component.flagPrefix().length()).trim();
        }
        while (flag.startsWith("-")) {
            flag = flag.substring(1).trim();
        }
        return flag;
    }

    private static void validateCollision(Component component, String flag) {
        String rawName = extractFlagName(flag);
        String flagName = rawName.replace("-", "").toLowerCase(Locale.ROOT);

        if (component == Component.KUBELET) {
            if (FLOCI_MANAGED_KUBELET_FLAGS.contains(flagName)) {
                throw new CollisionException("Cluster argument '" + rawName
                        + "' collides with Floci-managed kubelet argument. "
                        + "Floci manages this argument for cluster operation and it cannot be overridden.");
            }
        } else if (component == Component.KUBE_APISERVER) {
            if (FLOCI_MANAGED_APISERVER_FLAGS.contains(flagName)) {
                throw new CollisionException("Cluster argument '" + rawName
                        + "' collides with Floci-managed kube-apiserver argument. "
                        + "Floci manages this argument for cluster operation and it cannot be overridden.");
            }
            if (REFUSED_APISERVER_FLAGS.contains(flagName)) {
                throw new CollisionException("Cluster argument '" + rawName
                        + "' is refused outright and cannot be configured.");
            }
        }
    }

    private static String extractFlagName(String flag) {
        int eqIndex = flag.indexOf('=');
        String name = (eqIndex >= 0) ? flag.substring(0, eqIndex).trim() : flag.trim();
        while (name.startsWith("-")) {
            name = name.substring(1).trim();
        }
        while (name.endsWith("+") || name.endsWith("-")) {
            name = name.substring(0, name.length() - 1).trim();
        }
        return name;
    }

    private static boolean hasControlCharacters(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isISOControl(c) && c != '\t' && c != '\n' && c != '\r') {
                return true;
            }
        }
        return false;
    }
}
