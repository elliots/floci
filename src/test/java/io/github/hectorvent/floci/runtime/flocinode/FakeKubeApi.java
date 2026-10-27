package io.github.hectorvent.floci.runtime.flocinode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import io.github.hectorvent.floci.core.common.Pem;
import io.github.hectorvent.floci.services.acm.CertificateGenerator;
import io.github.hectorvent.floci.services.acm.CertificateGenerator.GeneratedCertificate;
import io.github.hectorvent.floci.services.acm.model.KeyAlgorithm;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * A k3s Kubernetes API for tests, on a JDK {@link HttpsServer} on 127.0.0.1: {@code /cacerts}
 * without authentication, and with the admin token {@code /readyz}, {@code /version}, nodes with
 * JSON merge patches and their {@code resourceVersion} check, RuntimeClasses and ServiceAccounts.
 * Its certificate is issued by its own CA for {@code localhost} and {@code 127.0.0.1}, unless a
 * test names others.
 * Every answer closes its connection, so that each call makes a TLS handshake.
 */
public final class FakeKubeApi implements AutoCloseable {

    /** A request as the fake received it, with its headers by lower-case name. */
    public record Request(String method, String path, Map<String, String> headers, JsonNode body) {

        public String authorization() {
            return headers.get("authorization");
        }
    }

    public static final List<String> LOCALHOST_NAMES = List.of("localhost", "127.0.0.1");
    /** The boot id that kubelet reports for every node the fake adds. */
    public static final String BOOT_ID = "3c9b8f1e-2d4a-4e6b-9f70-1a2b3c4d5e6f";

    private static final CertificateGenerator CERTIFICATES = new CertificateGenerator();

    private final ObjectMapper mapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<String, ObjectNode> nodes = new LinkedHashMap<>();
    private final Set<String> runtimeClasses = new LinkedHashSet<>();
    private final Map<String, ObjectNode> serviceAccounts = new LinkedHashMap<>();
    private final Set<String> namespaces = new LinkedHashSet<>(List.of("default", "kube-system", "kube-public",
            "kube-node-lease"));
    private final Set<String> services = new LinkedHashSet<>(List.of("kubernetes"));
    private final Map<String, Map<String, ObjectNode>> objects = new LinkedHashMap<>();
    private final List<ObjectNode> pods = new ArrayList<>();
    private final Map<String, List<String>> podLogs = new LinkedHashMap<>();
    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private final Set<String> forbidden = ConcurrentHashMap.newKeySet();
    private final Map<String, Duration> delays = new ConcurrentHashMap<>();
    private final int port;
    private volatile String token;
    private volatile String caPem;
    private volatile String caCertsBody;
    private volatile int readyzStatus = 200;
    private volatile String readyzBody = "ok";
    private int conflictingPatches;
    private boolean closed;
    private long resourceVersion = 1;
    private HttpsServer server;

    /** Starts the fake on a free port, with a certificate for {@code localhost}, accepting {@code token}. */
    public FakeKubeApi(String token) throws IOException {
        this(token, LOCALHOST_NAMES);
    }

    /** Starts the fake on a free port, with a certificate for {@code serverNames}, accepting {@code token}. */
    public FakeKubeApi(String token, List<String> serverNames) throws IOException {
        this(token, 0, serverNames);
    }

    /** Starts the fake on {@code port}, as a node's published API port, with a certificate for {@code localhost}. */
    public FakeKubeApi(String token, int port) throws IOException {
        this(token, port, LOCALHOST_NAMES);
    }

    private FakeKubeApi(String token, int port, List<String> serverNames) throws IOException {
        this.token = token;
        start(port, serverNames);
        this.port = server.getAddress().getPort();
    }

    /** A node whose Kubernetes API is this fake. */
    public NodeRecord nodeRef(String nodeId) {
        return new NodeRecord(nodeId, port, 6600, "none", "0".repeat(64), false, List.of(), "000000000000",
                "us-east-1", "i-0123456789abcdef0.ec2.internal", "10.100.0.0/16", "10.42.0.0/16", Map.of(),
                List.of());
    }

    public int port() {
        return port;
    }

    /** The CA that issued the fake's certificate, as {@code /cacerts} gives it. */
    public String caPem() {
        return caPem;
    }

    /** From now on, accepts only {@code token}. */
    public void token(String token) {
        this.token = token;
    }

    /** From now on, {@code /cacerts} answers {@code pem}, whoever issued the fake's certificate. */
    public void caCerts(String pem) {
        this.caCertsBody = pem;
    }

    /** From now on, {@code /readyz} answers {@code status} with {@code body}. */
    public void readyz(int status, String body) {
        this.readyzStatus = status;
        this.readyzBody = body;
    }

    /**
     * Starts again on the same port with a new CA and a certificate it issued, as a node that
     * was created again does.
     */
    public synchronized void newCertificateAuthority() throws IOException {
        server.stop(0);
        caCertsBody = null;
        start(port, LOCALHOST_NAMES);
    }

    /**
     * Adds a node with the labels kubelet sets, no taints, the given {@code Ready} status and boot id
     * {@link #BOOT_ID}, and returns it.
     */
    public synchronized ObjectNode addNode(String name, String readyStatus) {
        ObjectNode node = mapper.createObjectNode();
        node.put("apiVersion", "v1").put("kind", "Node");
        ObjectNode metadata = node.putObject("metadata");
        metadata.put("name", name);
        metadata.put("resourceVersion", String.valueOf(resourceVersion++));
        metadata.putObject("labels").put("kubernetes.io/hostname", name).put("kubernetes.io/os", "linux");
        metadata.putObject("annotations").put("k3s.io/node-args", "[]");
        node.putObject("spec").put("providerID", "aws:///us-east-1a/i-0123456789abcdef0");
        ObjectNode status = node.putObject("status");
        status.putArray("conditions").addObject().put("type", "Ready").put("status", readyStatus);
        status.putObject("nodeInfo").put("bootID", BOOT_ID);
        nodes.put(name, node);
        return node;
    }

    /** A copy of node {@code name}. */
    public synchronized ObjectNode kubeNode(String name) {
        return nodes.get(name).deepCopy();
    }

    /** Changes node {@code name} as another client would, which gives it a new {@code resourceVersion}. */
    public synchronized void changeNode(String name, Consumer<ObjectNode> change) {
        ObjectNode node = nodes.get(name);
        change.accept(node);
        ((ObjectNode) node.path("metadata")).put("resourceVersion", String.valueOf(resourceVersion++));
    }

    /** Answers the next {@code method} request of {@code path} with 403. */
    public void forbidNext(String method, String path) {
        forbidden.add(method + " " + path);
    }

    /** Answers the next {@code method} request of {@code path} after {@code delay}. */
    public void delayNext(String method, String path, Duration delay) {
        delays.put(method + " " + path, delay);
    }

    /** The next {@code count} PATCHes find that the node changed since the caller read it. */
    public synchronized void conflictNextPatches(int count) {
        conflictingPatches = count;
    }

    public synchronized void addRuntimeClass(String name) {
        runtimeClasses.add(name);
    }

    /** Adds ServiceAccount {@code namespace/name} with {@code annotations}. */
    public synchronized void addServiceAccount(String namespace, String name, Map<String, String> annotations) {
        ObjectNode account = mapper.createObjectNode();
        account.put("apiVersion", "v1").put("kind", "ServiceAccount");
        ObjectNode metadata = account.putObject("metadata");
        metadata.put("name", name).put("namespace", namespace);
        ObjectNode values = metadata.putObject("annotations");
        annotations.forEach(values::put);
        serviceAccounts.put(namespace + "/" + name, account);
    }

    /** Adds namespace {@code name}, as a cluster's workload would. */
    /** Adds a pod with one running container, whose log {@link #podLog} writes to. */
    public synchronized void addPod(String namespace, String name, String container, String containerId,
                                    String nodeName) {
        ObjectNode pod = mapper.createObjectNode();
        pod.putObject("metadata").put("namespace", namespace).put("name", name).put("uid", "uid-" + name);
        pod.putObject("spec").put("nodeName", nodeName);
        ObjectNode status = pod.putObject("status").putArray("containerStatuses").addObject()
                .put("name", container).put("containerID", "containerd://" + containerId).put("image", "busybox:1.36");
        status.putObject("state").putObject("running").put("startedAt", "2026-10-25T00:00:00Z");
        pods.add(pod);
        podLogs.put(namespace + "/" + name, new ArrayList<>());
    }

    /** Writes a line, stamped with {@code at}, to the log of pod {@code namespace/name}'s container. */
    public synchronized void podLog(String namespace, String name, Instant at, String line) {
        podLogs.get(namespace + "/" + name).add(at + " " + line);
        notifyAll();
    }

    public synchronized void addNamespace(String name) {
        namespaces.add(name);
    }

    public synchronized Set<String> namespaces() {
        return Set.copyOf(namespaces);
    }

    /**
     * Adds an object named {@code name} to the list at {@code collection}, such as
     * {@code /apis/apiextensions.k8s.io/v1/customresourcedefinitions}, with {@code labels}; a
     * collection that has an object answers its list, and a DELETE of the object removes it.
     */
    public synchronized ObjectNode addObject(String collection, String name, Map<String, String> labels) {
        ObjectNode object = mapper.createObjectNode();
        ObjectNode metadata = object.putObject("metadata").put("name", name);
        ObjectNode values = metadata.putObject("labels");
        labels.forEach(values::put);
        objects.computeIfAbsent(collection, ignored -> new LinkedHashMap<>()).put(name, object);
        return object;
    }

    /** The names of the objects in the list at {@code collection}. */
    public synchronized Set<String> objects(String collection) {
        return Set.copyOf(objects.getOrDefault(collection, Map.of()).keySet());
    }

    /** Adds Service {@code name} in namespace {@code default}. */
    public synchronized void addService(String name) {
        services.add(name);
    }

    public synchronized Set<String> services() {
        return Set.copyOf(services);
    }

    public List<Request> requests() {
        return List.copyOf(requests);
    }

    public List<Request> requests(String method, String path) {
        return requests.stream().filter(r -> r.method().equals(method) && r.path().equals(path)).toList();
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            server.stop(0);
            executor.shutdownNow();
        }
    }

    private void start(int bindPort, List<String> serverNames) throws IOException {
        GeneratedCertificate ca = CERTIFICATES.generateCaCertificate("k3s-server-ca");
        GeneratedCertificate leaf = CERTIFICATES.generateIssuedCertificate("kube-apiserver", serverNames,
                KeyAlgorithm.RSA_2048, null, new CertificateGenerator.Issuer(Pem.parseCertificate(ca.certificatePem()),
                        Pem.parsePrivateKey(ca.privateKeyPem())), CertificateGenerator.LeafUsage.SERVER);
        caPem = ca.certificatePem();
        HttpsServer started = HttpsServer.create(new InetSocketAddress("127.0.0.1", bindPort), 0);
        started.setHttpsConfigurator(new HttpsConfigurator(sslContext(leaf, ca)));
        started.setExecutor(executor);
        started.createContext("/", this::handle);
        started.start();
        server = started;
    }

    private static SSLContext sslContext(GeneratedCertificate leaf, GeneratedCertificate ca) {
        try {
            KeyStore keys = KeyStore.getInstance(KeyStore.getDefaultType());
            keys.load(null, null);
            keys.setKeyEntry("server", Pem.parsePrivateKey(leaf.privateKeyPem()), new char[0], new Certificate[]{
                Pem.parseCertificate(leaf.certificatePem()), Pem.parseCertificate(ca.certificatePem())});
            KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            factory.init(keys, new char[0]);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(factory.getKeyManagers(), null, null);
            return context;
        } catch (GeneralSecurityException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            String path = exchange.getRequestURI().getRawPath();
            byte[] raw = exchange.getRequestBody().readAllBytes();
            Map<String, String> headers = new LinkedHashMap<>();
            exchange.getRequestHeaders().forEach((name, values) ->
                    headers.put(name.toLowerCase(Locale.ROOT), String.join(",", values)));
            JsonNode body = raw.length == 0 ? null : mapper.readTree(raw);
            requests.add(new Request(method, path, Map.copyOf(headers), body));
            exchange.getResponseHeaders().add("Connection", "close");

            if (method.equals("GET") && path.equals("/cacerts")) {
                respond(exchange, 200, "text/plain", caCertsBody != null ? caCertsBody : caPem);
                return;
            }
            if (!("Bearer " + token).equals(headers.get("authorization"))) {
                status(exchange, 401, "Unauthorized", "Unauthorized");
                return;
            }
            Duration delay = delays.remove(method + " " + path);
            if (delay != null) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            if (forbidden.remove(method + " " + path)) {
                status(exchange, 403, "Forbidden", "forbidden: " + method + " " + path);
                return;
            }
            if (method.equals("GET") && path.equals("/readyz")) {
                respond(exchange, readyzStatus, "text/plain", readyzBody);
                return;
            }
            if (method.equals("GET") && path.equals("/version")) {
                respond(exchange, 200, "application/json",
                        "{\"major\":\"1\",\"minor\":\"35\",\"gitVersion\":\"v1.35.0+k3s1\"}");
                return;
            }
            if (method.equals("GET") && path.equals("/api/v1/pods")) {
                synchronized (this) {
                    ObjectNode list = mapper.createObjectNode();
                    list.putArray("items").addAll(pods);
                    respond(exchange, 200, "application/json", list.toString());
                }
                return;
            }
            if (method.equals("GET") && path.startsWith("/api/v1/namespaces/") && path.endsWith("/log")) {
                String[] parts = path.substring("/api/v1/namespaces/".length()).split("/");
                followPodLog(exchange, decode(parts[0]) + "/" + decode(parts[2]));
                return;
            }
            if (method.equals("GET") && path.equals("/api/v1/namespaces")) {
                respond(exchange, 200, "application/json", list(namespaces));
                return;
            }
            if (method.equals("GET") && path.equals("/api/v1/namespaces/default/services")) {
                respond(exchange, 200, "application/json", list(services));
                return;
            }
            synchronized (this) {
                if (method.equals("GET") && objects.containsKey(path)) {
                    ObjectNode answer = mapper.createObjectNode();
                    answer.putArray("items").addAll(objects.get(path).values());
                    respond(exchange, 200, "application/json", answer.toString());
                    return;
                }
            }
            if (method.equals("DELETE")) {
                synchronized (this) {
                    int slash = path.lastIndexOf('/');
                    Map<String, ObjectNode> collection = objects.get(path.substring(0, slash));
                    if (collection != null) {
                        collection.remove(decode(path.substring(slash + 1)));
                    }
                    if (path.startsWith("/api/v1/namespaces/") && path.indexOf('/', "/api/v1/namespaces/".length()) < 0) {
                        namespaces.remove(decode(path.substring("/api/v1/namespaces/".length())));
                    } else if (path.startsWith("/api/v1/namespaces/default/services/")) {
                        services.remove(decode(path.substring("/api/v1/namespaces/default/services/".length())));
                    }
                }
                respond(exchange, 200, "application/json", "{\"kind\":\"Status\",\"status\":\"Success\"}");
                return;
            }
            if (path.startsWith("/api/v1/nodes/")) {
                node(exchange, method, decode(path.substring("/api/v1/nodes/".length())), headers, body);
                return;
            }
            if (method.equals("GET") && path.startsWith("/api/v1/namespaces/") && path.contains("/serviceaccounts/")) {
                String[] parts = path.substring("/api/v1/namespaces/".length()).split("/serviceaccounts/", 2);
                String key = decode(parts[0]) + "/" + decode(parts[1]);
                synchronized (this) {
                    ObjectNode account = serviceAccounts.get(key);
                    if (account != null) {
                        respond(exchange, 200, "application/json", account.toString());
                    } else {
                        status(exchange, 404, "NotFound", "serviceaccounts \"" + decode(parts[1]) + "\" not found");
                    }
                }
                return;
            }
            if (method.equals("GET") && path.startsWith("/apis/node.k8s.io/v1/runtimeclasses/")) {
                String name = decode(path.substring("/apis/node.k8s.io/v1/runtimeclasses/".length()));
                synchronized (this) {
                    if (runtimeClasses.contains(name)) {
                        respond(exchange, 200, "application/json", "{\"apiVersion\":\"node.k8s.io/v1\","
                                + "\"kind\":\"RuntimeClass\",\"metadata\":{\"name\":\"" + name + "\"},\"handler\":\""
                                + name + "\"}");
                    } else {
                        status(exchange, 404, "NotFound", "runtimeclasses.node.k8s.io \"" + name + "\" not found");
                    }
                }
                return;
            }
            status(exchange, 404, "NotFound", "the server could not find the requested resource");
        }
    }

    private synchronized String list(Set<String> names) {
        ObjectNode answer = mapper.createObjectNode();
        for (String name : names) {
            answer.withArray("items").addObject().putObject("metadata").put("name", name);
        }
        if (names.isEmpty()) {
            answer.putArray("items");
        }
        return answer.toString();
    }

    private synchronized void node(HttpExchange exchange, String method, String name, Map<String, String> headers,
                                   JsonNode body) throws IOException {
        ObjectNode node = nodes.get(name);
        if (node == null) {
            status(exchange, 404, "NotFound", "nodes \"" + name + "\" not found");
            return;
        }
        if (method.equals("GET")) {
            respond(exchange, 200, "application/json", node.toString());
            return;
        }
        if (!method.equals("PATCH")) {
            status(exchange, 405, "MethodNotAllowed", method + " is not allowed");
            return;
        }
        if (!"application/merge-patch+json".equals(headers.get("content-type"))) {
            status(exchange, 415, "UnsupportedMediaType", "the body of the request was in an unknown format");
            return;
        }
        if (conflictingPatches > 0) {
            conflictingPatches--;
            ((ObjectNode) node.path("metadata")).put("resourceVersion", String.valueOf(resourceVersion++));
        }
        String expected = body.path("metadata").path("resourceVersion").asText(null);
        String current = node.path("metadata").path("resourceVersion").asText();
        if (expected != null && !expected.equals(current)) {
            status(exchange, 409, "Conflict", "Operation cannot be fulfilled on nodes \"" + name
                    + "\": the object has been modified; please apply your changes to the latest version and try again");
            return;
        }
        ObjectNode patched = (ObjectNode) mergePatch(node, body);
        ((ObjectNode) patched.path("metadata")).put("resourceVersion", String.valueOf(resourceVersion++));
        nodes.put(name, patched);
        respond(exchange, 200, "application/json", patched.toString());
    }

    /** RFC 7386. */
    private JsonNode mergePatch(JsonNode target, JsonNode patch) {
        if (!patch.isObject()) {
            return patch.deepCopy();
        }
        ObjectNode result = target != null && target.isObject() ? ((ObjectNode) target).deepCopy() : mapper.createObjectNode();
        patch.properties().forEach(field -> {
            if (field.getValue().isNull()) {
                result.remove(field.getKey());
            } else {
                result.set(field.getKey(), mergePatch(result.get(field.getKey()), field.getValue()));
            }
        });
        return result;
    }

    private void status(HttpExchange exchange, int code, String reason, String message) throws IOException {
        ObjectNode status = mapper.createObjectNode();
        status.put("kind", "Status").put("apiVersion", "v1");
        status.putObject("metadata");
        status.put("status", "Failure").put("message", message).put("reason", reason).put("code", code);
        respond(exchange, code, "application/json", status.toString());
    }

    /** The pod's log, from its start or with {@code tailLines=0} from now, then each new line, until the fake closes. */
    private void followPodLog(HttpExchange exchange, String pod) throws IOException {
        List<String> log;
        int sent;
        synchronized (this) {
            log = podLogs.get(pod);
            if (log == null) {
                status(exchange, 404, "NotFound", "pods \"" + pod + "\" not found");
                return;
            }
            String query = exchange.getRequestURI().getRawQuery();
            sent = query != null && query.contains("tailLines=0") ? log.size() : 0;
        }
        exchange.getResponseHeaders().add("Content-Type", "text/plain");
        exchange.sendResponseHeaders(200, 0);
        OutputStream output = exchange.getResponseBody();
        while (true) {
            List<String> next;
            synchronized (this) {
                while (!closed && log.size() == sent) {
                    try {
                        wait(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                if (closed) {
                    return;
                }
                next = new ArrayList<>(log.subList(sent, log.size()));
                sent = log.size();
            }
            for (String line : next) {
                output.write((line + "\n").getBytes(StandardCharsets.UTF_8));
            }
            output.flush();
        }
    }

    private static void respond(HttpExchange exchange, int status, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        if (bytes.length > 0) {
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        }
    }

    private static String decode(String value) {
        return URLDecoder.decode(value.replace("+", "%2B"), StandardCharsets.UTF_8);
    }
}
