package io.github.hectorvent.floci.runtime.node;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A smolvm-pods node service's API over TCP, on a JDK {@link HttpServer}, for tests: the routes of
 * {@link SmolvmNodeClient} with the node's bearer token check, error answers and state rules
 * (tools/smolvm-pods/docs/configuration.md). It logs every request, and a test can script the
 * answer to the next matching one.
 */
public final class FakeNodeApi implements AutoCloseable {

    /**
     * A request as the fake received it: its raw path and query, its headers by lower-case name,
     * and its body, or null when it had none.
     */
    public record Request(String method, String path, String query, Map<String, String> headers, JsonNode body) {

        public String authorization() {
            return headers.get("authorization");
        }
    }

    private record Scripted(String method, String pathPrefix, Duration delay, int status, String body) {
    }

    private static final Set<String> POD_FILTERS = Set.of("ip", "uid", "namespace", "name", "netns");
    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private final ObjectMapper mapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final HttpServer server;
    private final Map<String, ObjectNode> pods = new LinkedHashMap<>();
    private final List<ObjectNode> events = new ArrayList<>();
    private final List<ObjectNode> networkEvents = new ArrayList<>();
    private final Map<String, List<String>> logs = new LinkedHashMap<>(Map.of(
            "k3s", new ArrayList<>(), "kube-apiserver-audit", new ArrayList<>()));
    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private final Deque<Scripted> scripted = new ArrayDeque<>();
    private volatile String token;
    private volatile JsonNode network;
    private Duration keepalive = Duration.ofSeconds(30);
    private long followEpoch;
    private boolean silent;
    private boolean closed;

    /** Starts the fake on a free port of 127.0.0.1, accepting {@code token}. */
    public FakeNodeApi(String token) throws IOException {
        this(token, 0);
    }

    /** Starts the fake on {@code port} of 127.0.0.1, accepting {@code token}. */
    public FakeNodeApi(String token, int port) throws IOException {
        this.token = token;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.setExecutor(executor);
        server.createContext("/", this::handle);
        server.start();
    }

    public URI url() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    /** A declared node named {@code node:KEY} at this fake, with {@code token}. */
    public SmolvmNode node(String key, NodeToken token) {
        return new SmolvmNode(SmolvmNode.declaredName(key), url(), token, SmolvmNode.Kind.DECLARED, List.of());
    }

    /** From now on, accepts only {@code token}. */
    public void token(String token) {
        this.token = token;
    }

    /**
     * Adds an awake pod and returns its record, which a test may change, such as its
     * {@code labels} or {@code controller}, before the calls that read it.
     */
    public synchronized ObjectNode addPod(String sandboxId, String namespace, String name) {
        ObjectNode pod = mapper.createObjectNode();
        pod.put("sandbox_id", sandboxId);
        pod.put("namespace", namespace);
        pod.put("name", name);
        pod.put("uid", UUID.nameUUIDFromBytes(sandboxId.getBytes(StandardCharsets.UTF_8)).toString());
        pod.put("ip", "10.42.0." + (pods.size() + 2));
        pod.put("mac", "02:00:00:00:00:" + String.format("%02x", pods.size() + 2));
        pod.put("netns", "/var/run/netns/cni-" + sandboxId);
        pod.put("machine", "pod-" + sandboxId);
        pod.put("state", "awake");
        pod.putNull("phase");
        pod.put("generation", 1);
        pod.put("incarnation", UUID.randomUUID().toString());
        pod.put("rosetta", false);
        pod.put("hibernate", true);
        pod.put("vm_pid", 100 + pods.size());
        pod.putArray("leases");
        pod.put("created", time(Instant.now()));
        pod.put("state_since", time(Instant.now()));
        pod.putNull("last_wake_ms");
        pod.putNull("last_pause_ms");
        pod.putNull("checkpoint_bytes");
        pod.put("wake_failures", 0);
        pod.putNull("failure");
        pod.putObject("labels");
        pod.putNull("controller");
        pods.put(sandboxId, pod);
        addEvent(sandboxId, "none", "creating", "container-create");
        addEvent(sandboxId, "creating", "awake", "container-create");
        return pod;
    }

    /** Sets a pod's state, as the node would, with its event. */
    public synchronized void state(String sandboxId, String state) {
        transition(pods.get(sandboxId), state, "api");
    }

    public synchronized void removePod(String sandboxId) {
        String from = pods.remove(sandboxId).path("state").asText();
        addEvent(sandboxId, from, "deleted", "api");
    }

    public synchronized boolean hasPod(String sandboxId) {
        return pods.containsKey(sandboxId);
    }

    /** A copy of a pod's record. */
    public synchronized ObjectNode pod(String sandboxId) {
        return pods.get(sandboxId).deepCopy();
    }

    /** Writes a line to the node log {@code log}, which every follow of it then sends. */
    public synchronized void appendLog(String log, String line) {
        logs.get(log).add(line);
        notifyAll();
    }

    /** Appends an event with the next sequence number, which every follow then sends. */
    public synchronized ObjectNode addEvent(String sandboxId, String from, String to, String reason) {
        ObjectNode pod = pods.get(sandboxId);
        ObjectNode event = mapper.createObjectNode();
        event.put("seq", events.size() + 1);
        event.put("time", time(Instant.now()));
        event.put("pod", sandboxId);
        event.put("incarnation", pod == null ? null : pod.path("incarnation").asText());
        event.put("from", from);
        event.put("to", to);
        event.put("generation", pod == null ? 0 : pod.path("generation").asLong());
        event.put("reason", reason);
        event.putNull("reason_detail");
        event.putNull("source");
        event.putObject("steps_ms");
        event.putNull("duration_ms");
        event.putNull("error");
        event.putNull("warning");
        events.add(event);
        notifyAll();
        return event;
    }

    /**
     * Appends a network event with the next sequence number, as the node writes one when its
     * minute ends, which every follow of the network events then sends.
     */
    public synchronized ObjectNode addNetworkEvent(String kind, String pod, String destination, int port,
                                                   String protocol, long count, String... names) {
        ObjectNode event = mapper.createObjectNode();
        event.put("seq", networkEvents.size() + 1);
        event.put("kind", kind);
        event.put("pod", pod);
        event.put("destination", destination);
        event.put("port", port);
        event.put("protocol", protocol);
        event.put("count", count);
        Instant now = Instant.now();
        event.put("first", time(now.minusSeconds(60)));
        event.put("last", time(now));
        if (names.length > 0) {
            ArrayNode list = event.putArray("names");
            for (String name : names) {
                list.add(name);
            }
        }
        networkEvents.add(event);
        notifyAll();
        return event;
    }

    /** Answers the next request of {@code method} whose path starts with {@code pathPrefix} with this. */
    public synchronized void answerNext(String method, String pathPrefix, int status, String body) {
        scripted.add(new Scripted(method, pathPrefix, Duration.ZERO, status, body));
    }

    /** Answers the next matching request with the node's error answer. */
    public void failNext(String method, String pathPrefix, int status, String code, String message,
                         String... reasons) {
        ObjectNode error = mapper.createObjectNode();
        ObjectNode fields = error.putObject("error");
        fields.put("code", code);
        fields.put("message", message);
        ArrayNode list = fields.putArray("reasons");
        for (String reason : reasons) {
            list.add(reason);
        }
        answerNext(method, pathPrefix, status, error.toString());
    }

    /** Holds the next matching request for {@code delay}, then serves it as usual. */
    public synchronized void delayNext(String method, String pathPrefix, Duration delay) {
        scripted.add(new Scripted(method, pathPrefix, delay, 0, null));
    }

    /** How long a follow stays quiet before it sends a blank line; 30 s, as on the node. */
    public synchronized void keepalive(Duration interval) {
        keepalive = interval;
    }

    /** Ends every follow that is open, as a node service that stops does. */
    public synchronized void endFollows() {
        followEpoch++;
        notifyAll();
    }

    /**
     * From now on, every follow sends nothing, neither events nor keepalives, and keeps its
     * connection open, as a follow does over a connection that died without closing.
     */
    public synchronized void silenceFollows() {
        silent = true;
        notifyAll();
    }

    public List<Request> requests() {
        return List.copyOf(requests);
    }

    /** The network profile that the last {@code PUT /v1/network} published, or null. */
    public JsonNode network() {
        return network;
    }

    private ObjectNode networkStatus() {
        JsonNode applied = network;
        ObjectNode status = mapper.createObjectNode();
        if (applied == null) {
            status.putNull("generation");
            status.put("effective_isolation", false);
        } else {
            status.put("generation", applied.path("generation").asText());
            status.put("effective_isolation", applied.path("isolation").asBoolean());
        }
        status.put("node_isolation", false);
        status.put("address", "169.254.53.1");
        return status;
    }

    @Override
    public void close() {
        synchronized (this) {
            closed = true;
            notifyAll();
        }
        server.stop(0);
        executor.shutdownNow();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            String path = exchange.getRequestURI().getRawPath();
            String query = exchange.getRequestURI().getRawQuery();
            byte[] raw = exchange.getRequestBody().readAllBytes();
            String authorization = exchange.getRequestHeaders().getFirst("Authorization");
            Map<String, String> headers = new LinkedHashMap<>();
            exchange.getRequestHeaders().forEach((name, values) ->
                    headers.put(name.toLowerCase(Locale.ROOT), String.join(",", values)));
            JsonNode body;
            try {
                body = raw.length == 0 ? null : mapper.readTree(raw);
            } catch (JsonProcessingException e) {
                requests.add(new Request(method, path, query, Map.copyOf(headers), null));
                error(exchange, 400, "invalid", e.getOriginalMessage());
                return;
            }
            requests.add(new Request(method, path, query, Map.copyOf(headers), body));
            if (!("Bearer " + token).equals(authorization)) {
                exchange.getResponseHeaders().add("WWW-Authenticate", "Bearer");
                error(exchange, 401, "unauthorized", "a request needs Authorization: Bearer TOKEN, with the token "
                        + "of the node service's --api-token-file");
                return;
            }
            Scripted answer = scripted(method, path);
            if (answer != null) {
                sleep(answer.delay());
                if (answer.body() != null) {
                    respond(exchange, answer.status(), answer.body());
                    return;
                }
            }
            route(exchange, method, segments(path), query(query), body == null ? mapper.createObjectNode() : body);
        }
    }

    private void route(HttpExchange exchange, String method, List<String> path, Map<String, String> query,
                       JsonNode body) throws IOException {
        int size = path.size();
        if (size >= 2 && path.get(0).equals("v1") && path.get(1).equals("pods")) {
            if (size == 2 && method.equals("GET")) {
                listPods(exchange, query);
                return;
            }
            if (size == 3 && method.equals("GET")) {
                ObjectNode pod = find(path.get(2));
                if (pod == null) {
                    notFound(exchange, path.get(2));
                } else {
                    respond(exchange, 200, copy(pod).toString());
                }
                return;
            }
            if (size == 4 && path.get(3).equals("wake") && method.equals("POST")) {
                wake(exchange, path.get(2), body);
                return;
            }
            if (size == 5 && path.get(3).equals("leases")) {
                lease(exchange, method, path.get(2), path.get(4), body);
                return;
            }
        }
        if (size == 2 && path.get(0).equals("v1") && path.get(1).equals("network")) {
            if (method.equals("PUT")) {
                network = body;
            }
            if (method.equals("PUT") || method.equals("GET")) {
                respond(exchange, 200, networkStatus().toString());
                return;
            }
        }
        if (size == 2 && path.get(0).equals("v1") && path.get(1).equals("events") && method.equals("GET")) {
            if ("true".equals(query.get("follow"))) {
                follow(exchange, Long.parseLong(query.getOrDefault("cursor", "0")), query.get("pod"), events);
            } else {
                eventPage(exchange, query);
            }
            return;
        }
        if (size == 3 && path.get(0).equals("v1") && path.get(1).equals("network") && path.get(2).equals("events")
                && method.equals("GET")) {
            if ("true".equals(query.get("follow"))) {
                follow(exchange, Long.parseLong(query.getOrDefault("cursor", "0")), null, networkEvents);
            } else {
                networkEventPage(exchange, query);
            }
            return;
        }
        if (size == 3 && path.get(0).equals("v1") && path.get(1).equals("logs") && method.equals("GET")) {
            List<String> log;
            synchronized (this) {
                log = logs.get(path.get(2));
            }
            if (log == null) {
                error(exchange, 404, "not_found", "no node log " + path.get(2));
            } else {
                followLog(exchange, log);
            }
            return;
        }
        respond(exchange, 404, "");
    }

    /** The lines written to {@code log} from now on, and a blank line after each quiet keepalive. */
    private void followLog(HttpExchange exchange, List<String> log) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(200, 0);
        OutputStream output = exchange.getResponseBody();
        long epoch;
        int sent;
        synchronized (this) {
            epoch = followEpoch;
            sent = log.size();
        }
        while (true) {
            List<String> next;
            boolean quiet = false;
            synchronized (this) {
                if (closed || followEpoch != epoch) {
                    return;
                }
                if (silent) {
                    waitFor(keepalive);
                    continue;
                }
                if (log.size() == sent) {
                    waitFor(keepalive);
                    quiet = log.size() == sent && !closed && followEpoch == epoch;
                }
                next = new ArrayList<>(log.subList(sent, log.size()));
                sent = log.size();
            }
            if (quiet) {
                output.write('\n');
            }
            for (String line : next) {
                output.write((line + "\n").getBytes(StandardCharsets.UTF_8));
            }
            output.flush();
        }
    }

    private void listPods(HttpExchange exchange, Map<String, String> query) throws IOException {
        for (String filter : query.keySet()) {
            if (!POD_FILTERS.contains(filter)) {
                error(exchange, 400, "invalid", "unknown field `" + filter + "`");
                return;
            }
        }
        ObjectNode answer = mapper.createObjectNode();
        ArrayNode list = answer.putArray("pods");
        synchronized (this) {
            for (ObjectNode pod : pods.values()) {
                boolean matches = query.entrySet().stream()
                        .allMatch(filter -> filter.getValue().equals(pod.path(filter.getKey()).asText()));
                if (matches) {
                    list.add(copy(pod));
                }
            }
        }
        respond(exchange, 200, answer.toString());
    }

    private void wake(HttpExchange exchange, String id, JsonNode body) throws IOException {
        ObjectNode answer;
        synchronized (this) {
            ObjectNode pod = find(id);
            if (pod == null) {
                notFound(exchange, id);
                return;
            }
            if (staleIncarnation(exchange, pod, body)) {
                return;
            }
            String state = pod.path("state").asText();
            String sandbox = pod.path("sandbox_id").asText();
            if (state.equals("creating") || state.equals("deleting")) {
                error(exchange, 409, "refused", "pod sandbox " + sandbox + " is " + state, "state " + state);
                return;
            }
            if (state.equals("failed")) {
                error(exchange, 409, "failed", "pod sandbox " + sandbox + " failed");
                return;
            }
            boolean resumed = !state.equals("awake");
            if (resumed) {
                JsonNode reason = body.path("reason");
                String kind = reason.isObject() ? reason.path("kind").asText() : reason.asText("api");
                transition(pod, "waking", kind);
                transition(pod, "awake", kind);
            }
            answer = mapper.createObjectNode();
            answer.set("pod", copy(pod));
            answer.put("resumed", resumed);
            answer.put("cancelled_pause", false);
            answer.put("generation", pod.path("generation").asLong());
            answer.put("duration_ms", resumed ? 150.0 : 0.0);
            answer.putObject("steps_ms");
            answer.putNull("clock");
            answer.putNull("lease");
        }
        respond(exchange, 200, answer.toString());
    }

    private void lease(HttpExchange exchange, String method, String id, String leaseId, JsonNode body)
            throws IOException {
        ObjectNode answer;
        synchronized (this) {
            ObjectNode pod = find(id);
            if (pod == null) {
                notFound(exchange, id);
                return;
            }
            if (!method.equals("DELETE") && staleIncarnation(exchange, pod, body)) {
                return;
            }
            String sandbox = pod.path("sandbox_id").asText();
            ArrayNode leases = (ArrayNode) pod.path("leases");
            ObjectNode held = null;
            for (JsonNode lease : leases) {
                if (lease.path("id").asText().equals(leaseId)) {
                    held = (ObjectNode) lease;
                }
            }
            answer = mapper.createObjectNode();
            if (method.equals("DELETE")) {
                if (held != null) {
                    leases.remove(indexOf(leases, held));
                }
                answer.set("pod", copy(pod));
                respond(exchange, 200, answer.toString());
                return;
            }
            double ttl = body.path("ttl_secs").asDouble(30);
            if (ttl <= 0 || ttl > 60 || leaseId.isEmpty() || leaseId.length() > 128) {
                error(exchange, 400, "invalid", "a lease lasts above 0 and at most 60 s, and its id has 1 to 128 "
                        + "characters");
                return;
            }
            Instant now = Instant.now();
            boolean live = held != null && Instant.parse(held.path("expires_at").asText()).isAfter(now);
            if (method.equals("PATCH") && !live) {
                error(exchange, 404, "lease_not_found", "pod sandbox " + sandbox + " has no lease " + leaseId
                        + ", or it expired");
                return;
            }
            if (!method.equals("PUT") && !method.equals("PATCH")) {
                respond(exchange, 405, "");
                return;
            }
            if (held == null) {
                held = leases.addObject();
                held.put("id", leaseId);
                held.put("kind", "api");
                held.put("holder", body.path("holder").isTextual() ? body.path("holder").asText() : null);
            }
            held.put("expires_at", time(now.plusMillis((long) (ttl * 1000))));
            answer.set("pod", copy(pod));
            answer.set("lease", held.deepCopy());
        }
        respond(exchange, 200, answer.toString());
    }

    private void eventPage(HttpExchange exchange, Map<String, String> query) throws IOException {
        long cursor = Long.parseLong(query.getOrDefault("cursor", "0"));
        int limit = Math.clamp(Long.parseLong(query.getOrDefault("limit", "500")), 1, 500);
        String pod = query.get("pod");
        ObjectNode answer = mapper.createObjectNode();
        ArrayNode list = answer.putArray("events");
        long last = cursor;
        synchronized (this) {
            for (ObjectNode event : events) {
                long seq = event.path("seq").asLong();
                if (seq > cursor && list.size() < limit && (pod == null || pod.equals(event.path("pod").asText()))) {
                    list.add(event.deepCopy());
                    last = seq;
                }
            }
            if (list.size() < limit) {
                last = Math.max(cursor, events.size());
            }
        }
        answer.put("cursor", last);
        answer.put("gap", false);
        respond(exchange, 200, answer.toString());
    }

    /** A page as the node serves it: the cursor is the last event's, or the log's head after the last page. */
    private void networkEventPage(HttpExchange exchange, Map<String, String> query) throws IOException {
        long cursor = Long.parseLong(query.getOrDefault("cursor", "0"));
        int limit = Math.clamp(Long.parseLong(query.getOrDefault("limit", "500")), 1, 500);
        ObjectNode answer = mapper.createObjectNode();
        ArrayNode list = answer.putArray("events");
        long last;
        synchronized (this) {
            last = Math.max(cursor, networkEvents.size());
            for (ObjectNode event : networkEvents) {
                long seq = event.path("seq").asLong();
                if (seq > cursor && list.size() < limit) {
                    list.add(event.deepCopy());
                }
            }
            if (!list.isEmpty()) {
                last = list.get(list.size() - 1).path("seq").asLong();
            }
        }
        answer.put("cursor", last);
        respond(exchange, 200, answer.toString());
    }

    private void follow(HttpExchange exchange, long cursor, String pod, List<ObjectNode> log) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", "application/x-ndjson");
        exchange.sendResponseHeaders(200, 0);
        OutputStream output = exchange.getResponseBody();
        long epoch;
        synchronized (this) {
            epoch = followEpoch;
        }
        long sent = cursor;
        while (true) {
            List<ObjectNode> next = new ArrayList<>();
            boolean quiet = false;
            synchronized (this) {
                if (closed || followEpoch != epoch) {
                    return;
                }
                if (silent) {
                    waitFor(keepalive);
                    continue;
                }
                for (ObjectNode event : log) {
                    if (event.path("seq").asLong() > sent) {
                        next.add(event.deepCopy());
                    }
                }
                if (next.isEmpty()) {
                    long before = log.size();
                    waitFor(keepalive);
                    quiet = log.size() == before && !closed && followEpoch == epoch;
                }
            }
            if (quiet) {
                output.write('\n');
                output.flush();
            }
            for (ObjectNode event : next) {
                sent = event.path("seq").asLong();
                if (pod == null || pod.equals(event.path("pod").asText())) {
                    output.write((event + "\n").getBytes(StandardCharsets.UTF_8));
                }
            }
            output.flush();
        }
    }

    private void transition(ObjectNode pod, String state, String reason) {
        String from = pod.path("state").asText();
        pod.put("state", state);
        pod.put("generation", pod.path("generation").asLong() + 1);
        pod.put("state_since", time(Instant.now()));
        addEvent(pod.path("sandbox_id").asText(), from, state, reason);
    }

    private boolean staleIncarnation(HttpExchange exchange, ObjectNode pod, JsonNode body) throws IOException {
        JsonNode incarnation = body.path("incarnation");
        if (incarnation.isTextual() && !incarnation.asText().equals(pod.path("incarnation").asText())) {
            error(exchange, 409, "stale_incarnation", "pod sandbox " + pod.path("sandbox_id").asText()
                    + " has another incarnation");
            return true;
        }
        return false;
    }

    private synchronized ObjectNode find(String id) {
        int colon = id.indexOf(':');
        String prefix = colon < 0 ? "" : id.substring(0, colon);
        for (ObjectNode pod : pods.values()) {
            boolean matches = switch (prefix) {
                case "uid", "ip", "netns" -> pod.path(prefix).asText().equals(id.substring(colon + 1));
                default -> id.contains("/")
                        ? id.equals(pod.path("namespace").asText() + "/" + pod.path("name").asText())
                        : id.equals(pod.path("sandbox_id").asText());
            };
            if (matches) {
                return pod;
            }
        }
        return null;
    }

    private synchronized Scripted scripted(String method, String path) {
        Iterator<Scripted> answers = scripted.iterator();
        while (answers.hasNext()) {
            Scripted answer = answers.next();
            if (answer.method().equals(method) && path.startsWith(answer.pathPrefix())) {
                answers.remove();
                return answer;
            }
        }
        return null;
    }

    private void waitFor(Duration duration) {
        try {
            wait(Math.max(1, duration.toMillis()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            closed = true;
        }
    }

    private static void sleep(Duration delay) {
        if (delay.isZero()) {
            return;
        }
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private ObjectNode copy(ObjectNode pod) {
        return pod.deepCopy();
    }

    private static int indexOf(ArrayNode array, JsonNode element) {
        for (int i = 0; i < array.size(); i++) {
            if (array.get(i) == element) {
                return i;
            }
        }
        return -1;
    }

    private void notFound(HttpExchange exchange, String id) throws IOException {
        error(exchange, 404, "not_found", "no pod " + id);
    }

    private void error(HttpExchange exchange, int status, String code, String message, String... reasons)
            throws IOException {
        ObjectNode answer = mapper.createObjectNode();
        ObjectNode error = answer.putObject("error");
        error.put("code", code);
        error.put("message", message);
        ArrayNode list = error.putArray("reasons");
        for (String reason : reasons) {
            list.add(reason);
        }
        respond(exchange, status, answer.toString());
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > 0) {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
        }
        exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        if (bytes.length > 0) {
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        }
    }

    private static List<String> segments(String rawPath) {
        List<String> segments = new ArrayList<>();
        for (String segment : rawPath.split("/")) {
            if (!segment.isEmpty()) {
                segments.add(decode(segment));
            }
        }
        return segments;
    }

    private static Map<String, String> query(String rawQuery) {
        Map<String, String> query = new LinkedHashMap<>();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return query;
        }
        for (String pair : rawQuery.split("&")) {
            int equals = pair.indexOf('=');
            query.put(decode(equals < 0 ? pair : pair.substring(0, equals)),
                    equals < 0 ? "" : decode(pair.substring(equals + 1)));
        }
        return query;
    }

    private static String decode(String value) {
        return URLDecoder.decode(value.replace("+", "%2B"), StandardCharsets.UTF_8);
    }

    private static String time(Instant instant) {
        return TIME.format(instant);
    }
}
