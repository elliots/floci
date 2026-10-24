package io.github.hectorvent.floci.runtime.flocinode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * The Kubernetes API of the node that Floci created, {@code https://localhost:<apiPort>}, called as the
 * static admin user whose token is in the node's seed. Floci never runs a command in the node.
 *
 * <p>The first call to a node reads the cluster CA from k3s's {@code /cacerts} without
 * verification and pins it: every later call verifies the API server's certificate, and the name
 * {@code localhost} in it, against that CA alone. The CA stays pinned until {@link #forget} or
 * until the node's API port changes, so whoever creates a node again under the same id, which
 * gives it a new CA, calls {@link #forget} first.
 */
@ApplicationScoped
public class NodeKubeApi {

    private static final Logger LOG = Logger.getLogger(NodeKubeApi.class);

    /** The host of every node's API: smolvm publishes it on the Mac's loopback. */
    public static final String HOST = "localhost";

    /** The keys of the labels that Floci set on a node, comma separated. */
    static final String LABELS_ANNOTATION = "floci.io/eks-node-labels";

    /** The taints that Floci set on a node, as {@code key:effect}, comma separated. */
    static final String TAINTS_ANNOTATION = "floci.io/eks-node-taints";

    static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(2);
    static final Duration CALL_TIMEOUT = Duration.ofSeconds(5);

    private static final String MERGE_PATCH = "application/merge-patch+json";
    private static final int PATCH_ATTEMPTS = 3;
    private static final int MAX_DETAIL = 200;

    /** Accepts any certificate and any name; only for reading {@code /cacerts}. */
    private static final X509ExtendedTrustManager ANY_CERTIFICATE = new X509ExtendedTrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
            // Never a client.
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) {
            // Never a client.
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
            // Never a client.
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
            // The CA is not known before /cacerts answers.
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {
            // The CA is not known before /cacerts answers.
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
            // The CA is not known before /cacerts answers.
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    };

    private final Function<String, String> adminTokens;
    private final ObjectMapper mapper;
    private final Map<String, Pinned> pinned = new ConcurrentHashMap<>();

    @Inject
    public NodeKubeApi(NodeSeed seed, ObjectMapper mapper) {
        this(seed::adminToken, mapper);
    }

    /** @param adminTokens the admin token of a node, by node id */
    NodeKubeApi(Function<String, String> adminTokens, ObjectMapper mapper) {
        this.adminTokens = adminTokens;
        this.mapper = mapper;
    }

    @PreDestroy
    void close() {
        for (Pinned client : pinned.values()) {
            client.http().shutdownNow();
        }
        pinned.clear();
    }

    /** A Kubernetes taint; {@code value} and {@code timeAdded} may be null. */
    public record Taint(String key, String value, String effect, String timeAdded) {

        public Taint(String key, String value, String effect) {
            this(key, value, effect, null);
        }

        /** What tells taints apart on a node: {@code key:effect}. */
        public String id() {
            return key + ":" + effect;
        }

        boolean sameValue(Taint other) {
            return id().equals(other.id()) && (value == null ? other.value == null : value.equals(other.value));
        }
    }

    /**
     * What Floci reads of a node.
     *
     * @param readyStatus the status of its {@code Ready} condition, or null when it has none
     * @param bootId      the boot id that kubelet last reported, {@code status.nodeInfo.bootID}, or null
     */
    public record KubeNode(String name, String resourceVersion, Map<String, String> labels,
                    Map<String, String> annotations, List<Taint> taints, String readyStatus, String bootId) {

        public KubeNode {
            labels = Map.copyOf(labels);
            annotations = Map.copyOf(annotations);
            taints = List.copyOf(taints);
        }

        public boolean ready() {
            return "True".equals(readyStatus);
        }
    }

    /** The API server's {@code /version}. */
    public record Version(String major, String minor, String gitVersion) {
    }

    /**
     * A call that failed. {@link #status()} is the HTTP status of an unexpected answer, or 0 when
     * there was none: no connection, a certificate that the pinned CA did not issue, or no admin
     * token. The message never holds the token.
     */
    public static final class KubeApiException extends RuntimeException {

        private final int status;

        public KubeApiException(int status, String message) {
            super(message);
            this.status = status;
        }

        public KubeApiException(String message, Throwable cause) {
            super(message, cause);
            this.status = 0;
        }

        public int status() {
            return status;
        }
    }

    private record Pinned(int apiPort, String caPem, HttpClient http) {
    }

    /** The cluster CA, as k3s's {@code /cacerts} gave it, that every call to {@code node} verifies against. */
    public String caPem(NodeRecord node) {
        return pin(node).caPem();
    }

    /** Unpins node {@code nodeId}'s CA, so that the next call reads {@code /cacerts} again. */
    public void forget(String nodeId) {
        Pinned client = pinned.remove(nodeId);
        if (client != null) {
            client.http().shutdownNow();
        }
    }

    /**
     * {@code GET /readyz}.
     *
     * @return the answer's body, once the API server answers 200
     * @throws KubeApiException for any other answer, or none
     */
    public String readyz(NodeRecord node) {
        return expect(node, "GET", "/readyz", send(node, "GET", "/readyz", null), 200).body();
    }

    /** {@code GET /version}. */
    public Version version(NodeRecord node) {
        JsonNode answer = json(node, "GET", "/version", expect(node, "GET", "/version",
                send(node, "GET", "/version", null), 200));
        return new Version(text(answer, "major"), text(answer, "minor"), text(answer, "gitVersion"));
    }

    /** {@code GET /api/v1/nodes/NAME}, or empty when there is no such node. */
    public Optional<KubeNode> node(NodeRecord node, String name) {
        String path = nodePath(name);
        HttpResponse<String> response = send(node, "GET", path, null);
        if (response.statusCode() == 404) {
            return Optional.empty();
        }
        return Optional.of(kubeNode(json(node, "GET", path, expect(node, "GET", path, response, 200))));
    }

    /**
     * {@code GET /api/v1/namespaces/NAMESPACE/serviceaccounts/NAME}: the ServiceAccount as JSON, or
     * empty when there is no such ServiceAccount.
     *
     * @param timeout how long the call may take
     */
    public Optional<String> serviceAccount(NodeRecord node, String namespace, String name, Duration timeout) {
        String path = "/api/v1/namespaces/" + encode(namespace) + "/serviceaccounts/" + encode(name);
        HttpResponse<String> response = send(node, "GET", path, null, timeout);
        if (response.statusCode() == 404) {
            return Optional.empty();
        }
        return Optional.of(expect(node, "GET", path, response, 200).body());
    }

    /** Whether RuntimeClass {@code name} exists. */
    public boolean runtimeClassExists(NodeRecord node, String name) {
        String path = "/apis/node.k8s.io/v1/runtimeclasses/" + encode(name);
        HttpResponse<String> response = send(node, "GET", path, null);
        if (response.statusCode() == 404) {
            return false;
        }
        expect(node, "GET", path, response, 200);
        return true;
    }

    /**
     * Gives node {@code name} {@code labels} and {@code taints} among those Floci set: it sets
     * the ones that differ, removes the ones Floci set before and no longer gives, and leaves
     * every other label and taint alone. The node's annotations {@value #LABELS_ANNOTATION} and
     * {@value #TAINTS_ANNOTATION} record what Floci set. The change is one JSON merge patch with
     * the node's {@code resourceVersion}, read again and sent again when the node changed in
     * between.
     *
     * @return whether the node was patched
     * @throws KubeApiException when the node does not exist or a call fails
     */
    public boolean applyNodeMetadata(NodeRecord node, String name, Map<String, String> labels, List<Taint> taints) {
        String path = nodePath(name);
        for (int attempt = 1; ; attempt++) {
            KubeNode current = node(node, name).orElseThrow(() -> new KubeApiException(404,
                    node.nodeId() + ": node " + name + " does not exist"));
            ObjectNode patch = metadataPatch(current, labels, taints);
            if (patch == null) {
                return false;
            }
            HttpResponse<String> response = send(node, "PATCH", path, patch.toString());
            if (response.statusCode() == 409 && attempt < PATCH_ATTEMPTS) {
                LOG.debugv("Node {0} of {1} changed while Floci patched it; reading it again", name, node.nodeId());
                continue;
            }
            expect(node, "PATCH", path, response, 200);
            LOG.infov("Set the labels {0} and the taints {1} that Floci gives node {2} of {3}",
                    new TreeSet<>(labels.keySet()), ids(taints), name, node.nodeId());
            return true;
        }
    }

    /** The namespaces that k3s makes, which stay when Floci removes a cluster's workloads. */
    public static final Set<String> SYSTEM_NAMESPACES = Set.of("default", "kube-system", "kube-public",
            "kube-node-lease");

    /** The collections of namespace {@code default} that Floci empties, but for what k3s makes there. */
    private static final List<String> DEFAULT_COLLECTIONS = List.of(
            "/apis/apps/v1/namespaces/default/deployments",
            "/apis/apps/v1/namespaces/default/statefulsets",
            "/apis/apps/v1/namespaces/default/daemonsets",
            "/apis/apps/v1/namespaces/default/replicasets",
            "/apis/batch/v1/namespaces/default/cronjobs",
            "/apis/batch/v1/namespaces/default/jobs",
            "/api/v1/namespaces/default/pods",
            "/api/v1/namespaces/default/persistentvolumeclaims",
            "/api/v1/namespaces/default/secrets",
            "/api/v1/namespaces/default/configmaps?fieldSelector=metadata.name%21%3Dkube-root-ca.crt",
            "/api/v1/namespaces/default/serviceaccounts?fieldSelector=metadata.name%21%3Ddefault");

    /**
     * The collections whose objects Floci removes one by one, but for those that k3s, Kubernetes
     * or Floci's own manifests made ({@link #madeForTheNode}): the cluster-scoped kinds a cluster
     * leaves behind, such as a webhook that would refuse the next cluster's pods, and the other
     * namespaced kinds of {@code default}.
     */
    static final List<String> OWNED_COLLECTIONS = List.of(
            "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations",
            "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
            "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings",
            "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies",
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
            "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings",
            "/apis/rbac.authorization.k8s.io/v1/clusterroles",
            "/apis/storage.k8s.io/v1/storageclasses",
            "/apis/scheduling.k8s.io/v1/priorityclasses",
            "/api/v1/persistentvolumes",
            "/apis/rbac.authorization.k8s.io/v1/namespaces/default/roles",
            "/apis/rbac.authorization.k8s.io/v1/namespaces/default/rolebindings",
            "/apis/networking.k8s.io/v1/namespaces/default/ingresses",
            "/apis/networking.k8s.io/v1/namespaces/default/networkpolicies",
            "/apis/autoscaling/v2/namespaces/default/horizontalpodautoscalers",
            "/apis/policy/v1/namespaces/default/poddisruptionbudgets",
            "/api/v1/namespaces/default/resourcequotas",
            "/api/v1/namespaces/default/limitranges");

    /** The prefix of the labels and annotations that k3s's deploy controller puts on what it applies. */
    private static final String K3S_APPLIED = "objectset.rio.cattle.io/";

    /** The label of Kubernetes's own default RBAC objects. */
    private static final String BOOTSTRAPPING = "kubernetes.io/bootstrapping";

    /**
     * Removes what an EKS cluster put on the node's Kubernetes: every namespace but
     * {@link #SYSTEM_NAMESPACES}; in {@code default} its workloads, Services, claims, secrets,
     * config maps, service accounts, roles and their bindings, ingresses, network policies,
     * autoscalers, disruption budgets, quotas and limit ranges; and its webhook configurations,
     * admission policies, CRDs, cluster roles and their bindings, storage classes, priority
     * classes and persistent volumes; each but for those that k3s, Kubernetes or Floci's own
     * manifests made. What the cluster put in {@code kube-system}, {@code kube-public} and
     * {@code kube-node-lease} stays. Kubernetes deletes in the background, so namespaces may still
     * be terminating when this returns.
     *
     * @return what it asked Kubernetes to delete
     * @throws KubeApiException when a call fails
     */
    /** A running container of a pod: what Container Insights names its log by and tells of it. */
    public record PodContainer(String namespace, String pod, String podUid, String host, String container,
                               String containerId, String image) {
    }

    /** {@code GET /api/v1/pods}: the running containers of every pod, with their container ids. */
    public List<PodContainer> runningContainers(NodeRecord node) {
        String path = "/api/v1/pods";
        JsonNode answer = json(node, "GET", path, expect(node, "GET", path, send(node, "GET", path, null), 200));
        List<PodContainer> containers = new ArrayList<>();
        for (JsonNode pod : answer.path("items")) {
            JsonNode metadata = pod.path("metadata");
            for (JsonNode status : pod.path("status").path("containerStatuses")) {
                String id = text(status, "containerID");
                if (id == null || !status.path("state").has("running")) {
                    continue;
                }
                containers.add(new PodContainer(text(metadata, "namespace"), text(metadata, "name"),
                        text(metadata, "uid"), text(pod.path("spec"), "nodeName"), text(status, "name"),
                        id.substring(id.indexOf("://") + 3), text(status, "image")));
            }
        }
        return containers;
    }

    /**
     * {@code GET /api/v1/namespaces/NAMESPACE/pods/POD/log?follow=true&timestamps=true}: each line
     * of the container's log, stamped, as it is written, until the container stops or the stream
     * is closed. The kubelet reads the log from the node's files, so a sleeping pod stays asleep.
     *
     * @param fromNow only the lines written from now on, else the whole log first
     */
    public InputStream followPodLog(NodeRecord node, PodContainer container, boolean fromNow) {
        String path = "/api/v1/namespaces/" + encode(container.namespace()) + "/pods/" + encode(container.pod())
                + "/log?follow=true&timestamps=true&container=" + encode(container.container())
                + (fromNow ? "&tailLines=0" : "");
        HttpRequest request = HttpRequest.newBuilder(uri(node, path))
                .header("Authorization", "Bearer " + adminToken(node))
                .GET().build();
        try {
            HttpResponse<InputStream> response = pin(node).http().send(request,
                    HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() != 200) {
                String body;
                try (InputStream answer = response.body()) {
                    body = new String(answer.readNBytes(4096), StandardCharsets.UTF_8);
                }
                throw new KubeApiException(response.statusCode(), node.nodeId() + ": GET " + path + ": "
                        + response.statusCode() + detail(body));
            }
            return response.body();
        } catch (IOException e) {
            throw new KubeApiException(node.nodeId() + ": GET " + path + ": no answer: " + reason(e), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KubeApiException(node.nodeId() + ": GET " + path + ": interrupted", e);
        }
    }

    public List<String> removeWorkloads(NodeRecord node) {
        List<String> removed = new ArrayList<>();
        // The webhooks go first, so that none of them refuses the deletions below.
        for (String collection : OWNED_COLLECTIONS) {
            HttpResponse<String> response = send(node, "GET", collection, null);
            if (response.statusCode() == 404) {
                continue;
            }
            JsonNode items = json(node, "GET", collection, expect(node, "GET", collection, response, 200));
            String kind = collection.substring(collection.lastIndexOf('/') + 1);
            for (JsonNode item : items.path("items")) {
                String name = text(item.path("metadata"), "name");
                if (name != null && !madeForTheNode(kind, item)) {
                    deleteIfPresent(node, collection + "/" + encode(name));
                    removed.add((collection.contains("/namespaces/default/") ? "default " : "") + kind + " " + name);
                }
            }
        }
        JsonNode namespaces = json(node, "GET", "/api/v1/namespaces", expect(node, "GET", "/api/v1/namespaces",
                send(node, "GET", "/api/v1/namespaces", null), 200));
        for (JsonNode namespace : namespaces.path("items")) {
            String name = text(namespace.path("metadata"), "name");
            if (name == null || SYSTEM_NAMESPACES.contains(name)
                    || "Terminating".equals(text(namespace.path("status"), "phase"))) {
                continue;
            }
            deleteIfPresent(node, "/api/v1/namespaces/" + encode(name));
            removed.add("namespace " + name);
        }
        for (String collection : DEFAULT_COLLECTIONS) {
            deleteIfPresent(node, collection + (collection.contains("?") ? "&" : "?") + "propagationPolicy=Background");
            removed.add("default " + collection.substring(collection.lastIndexOf('/') + 1).replaceAll("\\?.*", ""));
        }
        String services = "/api/v1/namespaces/default/services";
        JsonNode list = json(node, "GET", services, expect(node, "GET", services, send(node, "GET", services, null),
                200));
        for (JsonNode service : list.path("items")) {
            String name = text(service.path("metadata"), "name");
            if (name != null && !"kubernetes".equals(name)) {
                deleteIfPresent(node, services + "/" + encode(name));
                removed.add("default service " + name);
            }
        }
        LOG.infov("Removed the workloads of the EKS cluster from node {0}: {1}", node.nodeId(), removed);
        return removed;
    }

    /**
     * Whether k3s, Kubernetes or Floci's own manifests made {@code item}, of the collection
     * {@code kind}: its name is a {@code system:} or {@code system-} one, it carries Kubernetes's
     * bootstrapping label or the marks of k3s's deploy controller, which applies k3s's manifests
     * and the seed's, it is one of k3s's {@code *.cattle.io} CRDs, or it is a persistent volume
     * of a claim in a system namespace that stays.
     */
    static boolean madeForTheNode(String kind, JsonNode item) {
        JsonNode metadata = item.path("metadata");
        String name = text(metadata, "name");
        if (name == null || name.startsWith("system:") || name.startsWith("system-")
                || metadata.path("labels").has(BOOTSTRAPPING)) {
            return true;
        }
        for (String field : List.of("labels", "annotations")) {
            for (Map.Entry<String, JsonNode> entry : metadata.path(field).properties()) {
                if (entry.getKey().startsWith(K3S_APPLIED)) {
                    return true;
                }
            }
        }
        if ("customresourcedefinitions".equals(kind)) {
            return name.endsWith(".cattle.io");
        }
        if ("persistentvolumes".equals(kind)) {
            String claimNamespace = text(item.path("spec").path("claimRef"), "namespace");
            return claimNamespace != null && !"default".equals(claimNamespace)
                    && SYSTEM_NAMESPACES.contains(claimNamespace);
        }
        return false;
    }

    private void deleteIfPresent(NodeRecord node, String path) {
        HttpResponse<String> response = send(node, "DELETE", path, null);
        int status = response.statusCode();
        if (status != 200 && status != 202 && status != 404) {
            expect(node, "DELETE", path, response, 200);
        }
    }

    /**
     * The JSON merge patch that gives {@code current} {@code labels} and {@code taints} among
     * those Floci set, or null when it has them.
     */
    static ObjectNode metadataPatch(KubeNode current, Map<String, String> labels, List<Taint> taints) {
        JsonNodeFactory json = JsonNodeFactory.instance;
        ObjectNode labelPatch = json.objectNode();
        for (Map.Entry<String, String> label : labels.entrySet()) {
            if (!label.getValue().equals(current.labels().get(label.getKey()))) {
                labelPatch.put(label.getKey(), label.getValue());
            }
        }
        for (String key : split(current.annotations().get(LABELS_ANNOTATION))) {
            if (!labels.containsKey(key) && current.labels().containsKey(key)) {
                labelPatch.putNull(key);
            }
        }

        Map<String, Taint> wanted = new LinkedHashMap<>();
        for (Taint taint : taints) {
            wanted.put(taint.id(), taint);
        }
        Set<String> owned = split(current.annotations().get(TAINTS_ANNOTATION));
        List<Taint> next = new ArrayList<>();
        Set<String> placed = new LinkedHashSet<>();
        for (Taint taint : current.taints()) {
            Taint want = wanted.get(taint.id());
            if (want != null) {
                if (placed.add(taint.id())) {
                    next.add(taint.sameValue(want) ? taint : want);
                }
            } else if (!owned.contains(taint.id())) {
                next.add(taint);
            }
        }
        for (Taint want : wanted.values()) {
            if (!placed.contains(want.id())) {
                next.add(want);
            }
        }

        ObjectNode annotationPatch = json.objectNode();
        recordOwned(annotationPatch, current, LABELS_ANNOTATION, new TreeSet<>(labels.keySet()));
        recordOwned(annotationPatch, current, TAINTS_ANNOTATION, new TreeSet<>(wanted.keySet()));

        boolean taintsChanged = !next.equals(current.taints());
        if (labelPatch.isEmpty() && annotationPatch.isEmpty() && !taintsChanged) {
            return null;
        }
        ObjectNode patch = json.objectNode();
        ObjectNode metadata = patch.putObject("metadata");
        metadata.put("resourceVersion", current.resourceVersion());
        if (!labelPatch.isEmpty()) {
            metadata.set("labels", labelPatch);
        }
        if (!annotationPatch.isEmpty()) {
            metadata.set("annotations", annotationPatch);
        }
        if (taintsChanged) {
            ArrayNode list = patch.putObject("spec").putArray("taints");
            for (Taint taint : next) {
                ObjectNode entry = list.addObject().put("key", taint.key());
                putIfPresent(entry, "value", taint.value());
                entry.put("effect", taint.effect());
                putIfPresent(entry, "timeAdded", taint.timeAdded());
            }
        }
        return patch;
    }

    private static void recordOwned(ObjectNode annotationPatch, KubeNode current, String annotation, Set<String> keys) {
        String value = String.join(",", keys);
        String present = current.annotations().get(annotation);
        if (value.isEmpty()) {
            if (present != null) {
                annotationPatch.putNull(annotation);
            }
        } else if (!value.equals(present)) {
            annotationPatch.put(annotation, value);
        }
    }

    static KubeNode kubeNode(JsonNode answer) {
        JsonNode metadata = answer.path("metadata");
        List<Taint> taints = new ArrayList<>();
        for (JsonNode taint : answer.path("spec").path("taints")) {
            taints.add(new Taint(text(taint, "key"), text(taint, "value"), text(taint, "effect"),
                    text(taint, "timeAdded")));
        }
        String ready = null;
        for (JsonNode condition : answer.path("status").path("conditions")) {
            if ("Ready".equals(text(condition, "type"))) {
                ready = text(condition, "status");
            }
        }
        return new KubeNode(text(metadata, "name"), text(metadata, "resourceVersion"), strings(metadata.path("labels")),
                strings(metadata.path("annotations")), taints, ready,
                text(answer.path("status").path("nodeInfo"), "bootID"));
    }

    private HttpResponse<String> send(NodeRecord node, String method, String path, String mergePatch) {
        return send(node, method, path, mergePatch, CALL_TIMEOUT);
    }

    private HttpResponse<String> send(NodeRecord node, String method, String path, String mergePatch,
                                      Duration timeout) {
        HttpClient http = pin(node).http();
        HttpRequest.Builder request = HttpRequest.newBuilder(uri(node, path))
                .timeout(timeout)
                .header("Authorization", "Bearer " + adminToken(node))
                .header("Accept", "application/json");
        if (mergePatch == null) {
            request.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            request.header("Content-Type", MERGE_PATCH)
                    .method(method, HttpRequest.BodyPublishers.ofString(mergePatch, StandardCharsets.UTF_8));
        }
        return exchange(node, method, path, http, request.build());
    }

    private String adminToken(NodeRecord node) {
        try {
            return adminTokens.apply(node.nodeId());
        } catch (IllegalStateException | UncheckedIOException e) {
            throw new KubeApiException(node.nodeId() + ": no admin token: " + e.getMessage(), e);
        }
    }

    /** The pinned client of {@code node}, after reading and pinning its CA if needed. */
    private Pinned pin(NodeRecord node) {
        Pinned current = pinned.get(node.nodeId());
        if (current != null && current.apiPort() == node.apiPort()) {
            return current;
        }
        String caPem = readCaCerts(node);
        Pinned fresh = new Pinned(node.apiPort(), caPem, pinnedClient(node, caPem));
        Pinned kept = pinned.merge(node.nodeId(), fresh,
                (existing, candidate) -> existing.apiPort() == candidate.apiPort() ? existing : candidate);
        if (kept != fresh) {
            fresh.http().shutdownNow();
            return kept;
        }
        if (current != null) {
            current.http().shutdownNow();
        }
        LOG.infov("Pinned the cluster CA of node {0} from {1}, SHA-256 {2}", node.nodeId(), uri(node, "/cacerts"),
                fingerprint(certificates(node, caPem).getFirst()));
        return fresh;
    }

    /** k3s's {@code /cacerts}, read without verification. */
    private String readCaCerts(NodeRecord node) {
        HttpClient http = HttpClient.newBuilder()
                .connectTimeout(CONNECT_TIMEOUT)
                .sslContext(sslContext(new TrustManager[]{ANY_CERTIFICATE}))
                .build();
        try {
            HttpRequest request = HttpRequest.newBuilder(uri(node, "/cacerts")).timeout(CALL_TIMEOUT).GET().build();
            String pem = expect(node, "GET", "/cacerts", exchange(node, "GET", "/cacerts", http, request), 200).body();
            certificates(node, pem);
            return pem;
        } finally {
            http.shutdownNow();
        }
    }

    private static HttpClient pinnedClient(NodeRecord node, String caPem) {
        try {
            KeyStore anchors = KeyStore.getInstance(KeyStore.getDefaultType());
            anchors.load(null, null);
            int index = 0;
            for (X509Certificate certificate : certificates(node, caPem)) {
                anchors.setCertificateEntry("ca-" + index++, certificate);
            }
            TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trust.init(anchors);
            return HttpClient.newBuilder()
                    .connectTimeout(CONNECT_TIMEOUT)
                    .sslContext(sslContext(trust.getTrustManagers()))
                    .build();
        } catch (GeneralSecurityException | IOException e) {
            throw new KubeApiException(node.nodeId() + ": the CA of /cacerts cannot be trusted: " + e.getMessage(), e);
        }
    }

    private static SSLContext sslContext(TrustManager[] trustManagers) {
        try {
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, trustManagers, null);
            return context;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("TLS cannot be set up: " + e.getMessage(), e);
        }
    }

    private static List<X509Certificate> certificates(NodeRecord node, String pem) {
        try {
            Collection<? extends Certificate> parsed = CertificateFactory.getInstance("X.509")
                    .generateCertificates(new ByteArrayInputStream(pem.getBytes(StandardCharsets.US_ASCII)));
            List<X509Certificate> certificates = new ArrayList<>();
            for (Certificate certificate : parsed) {
                if (certificate instanceof X509Certificate x509) {
                    certificates.add(x509);
                }
            }
            if (certificates.isEmpty()) {
                throw new KubeApiException(0, node.nodeId() + ": GET /cacerts has no certificate");
            }
            return certificates;
        } catch (GeneralSecurityException e) {
            throw new KubeApiException(node.nodeId() + ": GET /cacerts has no readable certificate: "
                    + e.getMessage(), e);
        }
    }

    private static String fingerprint(X509Certificate certificate) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded()));
        } catch (GeneralSecurityException e) {
            return "unknown: " + e.getMessage();
        }
    }

    private static HttpResponse<String> exchange(NodeRecord node, String method, String path, HttpClient http,
                                                 HttpRequest request) {
        try {
            return http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (SSLException e) {
            throw new KubeApiException(node.nodeId() + ": " + method + " " + path + ": TLS failed: "
                    + reason(e), e);
        } catch (IOException e) {
            throw new KubeApiException(node.nodeId() + ": " + method + " " + path + ": no answer: " + reason(e), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KubeApiException(node.nodeId() + ": " + method + " " + path + ": interrupted", e);
        }
    }

    private HttpResponse<String> expect(NodeRecord node, String method, String path,
                                        HttpResponse<String> response, int status) {
        if (response.statusCode() != status) {
            throw new KubeApiException(response.statusCode(), node.nodeId() + ": " + method + " " + path + ": "
                    + response.statusCode() + detail(response.body()));
        }
        return response;
    }

    /** The {@code message} of a Kubernetes {@code Status} answer, or the start of any other body. */
    private String detail(String body) {
        if (body == null || body.isBlank()) {
            return "";
        }
        try {
            String message = text(mapper.readTree(body), "message");
            if (message != null) {
                return ": " + message;
            }
        } catch (IOException notJson) {
            LOG.tracev("An error answer is not JSON: {0}", notJson.getMessage());
        }
        String text = body.strip();
        return ": " + (text.length() > MAX_DETAIL ? text.substring(0, MAX_DETAIL) + "..." : text);
    }

    private JsonNode json(NodeRecord node, String method, String path, HttpResponse<String> response) {
        try {
            return mapper.readTree(response.body());
        } catch (IOException e) {
            throw new KubeApiException(response.statusCode(), node.nodeId() + ": " + method + " " + path
                    + ": the answer is not JSON: " + e.getMessage());
        }
    }

    private static URI uri(NodeRecord node, String path) {
        return URI.create("https://" + HOST + ":" + node.apiPort() + path);
    }

    private static String nodePath(String name) {
        return "/api/v1/nodes/" + encode(name);
    }

    private static String encode(String segment) {
        return URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static Map<String, String> strings(JsonNode object) {
        Map<String, String> values = new LinkedHashMap<>();
        object.properties().forEach(field -> values.put(field.getKey(), field.getValue().asText()));
        return values;
    }

    private static Set<String> split(String list) {
        Set<String> values = new LinkedHashSet<>();
        if (list != null) {
            for (String value : list.split(",")) {
                if (!value.isBlank()) {
                    values.add(value.strip());
                }
            }
        }
        return values;
    }

    private static List<String> ids(List<Taint> taints) {
        List<String> ids = new ArrayList<>();
        for (Taint taint : taints) {
            ids.add(taint.id());
        }
        return ids;
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isValueNode() && !value.isNull() ? value.asText() : null;
    }

    private static void putIfPresent(ObjectNode node, String field, String value) {
        if (value != null) {
            node.put(field, value);
        }
    }

    private static String reason(Exception e) {
        return e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    }
}
