package io.github.hectorvent.floci.runtime.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The client of a node service's API over TCP, as tools/smolvm-pods/docs/configuration.md
 * describes it: pods, leases, wakes, events, the network profile and network events. Every call sends the node's bearer token, which
 * is read once and read again after a 401, and never logged. Calls block; each failure is a
 * {@link NodeApiException} with the node's error code.
 */
@ApplicationScoped
public class SmolvmNodeClient {

    /** The limit of every call except a wake, which takes its own, and a follow. */
    public static final Duration CALL_TIMEOUT = Duration.ofSeconds(10);

    /**
     * How long a follow waits for its next line before it fails as
     * {@link NodeApiException.Code#UNREACHABLE}: two and a half times the 30 s after which the
     * node sends a blank keepalive line when it has no event, so that a follow over a connection
     * that died without closing ends.
     */
    public static final Duration FOLLOW_IDLE_LIMIT = Duration.ofSeconds(75);

    /** The node's k3s log, for {@link #followLog}. */
    public static final String K3S_LOG = "k3s";

    /** The node's kube-apiserver audit log, for {@link #followLog}. */
    public static final String KUBE_APISERVER_AUDIT_LOG = "kube-apiserver-audit";

    /** A sandbox id that no pod has: CRI sandbox ids are hexadecimal. */
    static final String HEAD_QUERY_POD = "floci-head";

    static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);

    /** The most network events a page holds. */
    static final int NETWORK_PAGE = 500;

    private static final Logger LOG = Logger.getLogger(SmolvmNodeClient.class);
    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private final ObjectMapper mapper;
    private final HttpClient http;
    private final Duration followIdleLimit;
    private final ScheduledThreadPoolExecutor followTimer;
    private final Map<NodeToken, String> tokens = new ConcurrentHashMap<>();

    @Inject
    public SmolvmNodeClient(ObjectMapper mapper) {
        // HTTP/1.1 only: on an http URL the JDK client otherwise asks for an h2c upgrade.
        this(mapper, HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(CONNECT_TIMEOUT)
                .build());
    }

    SmolvmNodeClient(ObjectMapper mapper, HttpClient http) {
        this(mapper, http, FOLLOW_IDLE_LIMIT);
    }

    SmolvmNodeClient(ObjectMapper mapper, HttpClient http, Duration followIdleLimit) {
        this.mapper = mapper;
        this.http = http;
        this.followIdleLimit = followIdleLimit;
        // Its thread starts with the first follow.
        followTimer = new ScheduledThreadPoolExecutor(1,
                Thread.ofPlatform().daemon().name("smolvm-node-follow-timer").factory());
        followTimer.setRemoveOnCancelPolicy(true);
    }

    @PreDestroy
    void close() {
        followTimer.shutdownNow();
        http.shutdownNow();
    }

    /** Which pods {@link #listPods} returns: those that match every field that is not null. */
    public record PodQuery(String namespace, String name, String uid, String ip, String netns) {

        public static PodQuery all() {
            return new PodQuery(null, null, null, null, null);
        }

        public static PodQuery namespace(String namespace) {
            return new PodQuery(namespace, null, null, null, null);
        }
    }

    /** Why a wake is asked for: a kind such as {@code api}, or {@code aws} with the queue. */
    public record WakeReason(String kind, String queue) {

        public static WakeReason api() {
            return new WakeReason("api", null);
        }

        /** A message waits in {@code queue}, a queue ARN or {@code amqp://BROKER/VHOST/QUEUE}. */
        public static WakeReason aws(String queue) {
            return new WakeReason("aws", queue);
        }
    }

    /**
     * @param source      free text the node's events carry, such as {@code floci}; or null
     * @param incarnation the pod's incarnation the caller saw, or null for any
     */
    public record WakeRequest(WakeReason reason, String source, String incarnation) {
    }

    /** The answer of a wake, once the pod is awake. */
    public record Wake(NodePod pod, boolean resumed, boolean cancelledPause, long generation, double durationMs) {
    }

    /** The answer of a lease call: the pod, and the lease held or renewed. */
    public record LeaseHeld(NodePod pod, NodePod.Lease lease) {
    }

    /**
     * A page of events.
     *
     * @param cursor the last sequence number read, the {@code cursor} of the next page
     * @param gap    events after the requested cursor were rotated away
     */
    public record EventPage(List<NodeEvent> events, long cursor, boolean gap) {

        public EventPage {
            events = List.copyOf(events);
        }
    }

    /**
     * A page of network events.
     *
     * @param cursor the last sequence number read, the {@code cursor} of the next page
     */
    public record NetworkEventPage(List<NodeNetworkEvent> events, long cursor) {

        public NetworkEventPage {
            events = List.copyOf(events);
        }
    }

    /** A follow of a node's pod or network events; {@link #close()} ends it. */
    public interface EventFollow extends AutoCloseable {

        /**
         * Completes when the follow ends: normally when the node ends the stream or the follow
         * is closed, exceptionally with a {@link NodeApiException}, such as
         * {@link NodeApiException.Code#UNREACHABLE} after
         * {@link SmolvmNodeClient#FOLLOW_IDLE_LIMIT} without a line, or with the listener's
         * exception.
         */
        CompletableFuture<Void> done();

        @Override
        void close();
    }

    /** {@code GET /v1/pods}. */
    public List<NodePod> listPods(SmolvmNode node, PodQuery query) {
        StringBuilder path = new StringBuilder("/v1/pods");
        char separator = '?';
        String[][] filters = {
            {"namespace", query.namespace()}, {"name", query.name()}, {"uid", query.uid()},
            {"ip", query.ip()}, {"netns", query.netns()},
        };
        for (String[] filter : filters) {
            if (filter[1] != null) {
                path.append(separator).append(filter[0]).append('=').append(encode(filter[1]));
                separator = '&';
            }
        }
        JsonNode answer = call(node, "GET", path.toString(), null, CALL_TIMEOUT);
        return read(node, "GET", path.toString(), () -> {
            List<NodePod> pods = new ArrayList<>();
            for (JsonNode pod : answer.path("pods")) {
                pods.add(NodePod.of(pod));
            }
            return pods;
        });
    }

    /**
     * {@code GET /v1/pods/ID}.
     *
     * @param pod the sandbox id, {@code uid:UID}, {@code ip:IP}, {@code netns:PATH} or {@code NAMESPACE/NAME}
     */
    public NodePod getPod(SmolvmNode node, String pod) {
        String path = podPath(pod);
        JsonNode answer = call(node, "GET", path, null, CALL_TIMEOUT);
        return read(node, "GET", path, () -> NodePod.of(answer));
    }

    /**
     * {@code PUT /v1/pods/ID/leases/LEASE}: holds the lease, or renews it if it is held, without
     * waking the pod.
     *
     * @param ttlSeconds  above 0 and at most 60
     * @param holder      free text naming the holder, or null
     * @param incarnation the pod's incarnation the caller saw, or null for any
     */
    public LeaseHeld putLease(SmolvmNode node, String pod, String lease, int ttlSeconds, String holder,
                              String incarnation) {
        ObjectNode body = mapper.createObjectNode().put("ttl_secs", ttlSeconds);
        putIfPresent(body, "holder", holder);
        putIfPresent(body, "incarnation", incarnation);
        return lease(node, "PUT", leasePath(pod, lease), body);
    }

    /**
     * {@code PATCH /v1/pods/ID/leases/LEASE}: renews a lease that has not expired; a
     * {@link NodeApiException.Code#LEASE_NOT_FOUND} once it has.
     */
    public LeaseHeld patchLease(SmolvmNode node, String pod, String lease, int ttlSeconds, String incarnation) {
        ObjectNode body = mapper.createObjectNode().put("ttl_secs", ttlSeconds);
        putIfPresent(body, "incarnation", incarnation);
        return lease(node, "PATCH", leasePath(pod, lease), body);
    }

    /** {@code DELETE /v1/pods/ID/leases/LEASE}: releases the lease; an unknown one is no error. */
    public NodePod deleteLease(SmolvmNode node, String pod, String lease) {
        String path = leasePath(pod, lease);
        JsonNode answer = call(node, "DELETE", path, null, CALL_TIMEOUT);
        return read(node, "DELETE", path, () -> NodePod.of(answer.path("pod")));
    }

    /**
     * {@code POST /v1/pods/ID/wake}, which answers once the pod is awake.
     *
     * @param timeout how long to wait for the answer, which covers the node's admission wait
     */
    public Wake wake(SmolvmNode node, String pod, WakeRequest request, Duration timeout) {
        ObjectNode body = mapper.createObjectNode();
        WakeReason reason = request.reason();
        if (reason != null && reason.queue() != null) {
            body.putObject("reason").put("kind", reason.kind()).put("queue", reason.queue());
        } else if (reason != null) {
            body.put("reason", reason.kind());
        }
        putIfPresent(body, "source", request.source());
        putIfPresent(body, "incarnation", request.incarnation());
        String path = podPath(pod) + "/wake";
        JsonNode answer = call(node, "POST", path, body, timeout);
        return read(node, "POST", path, () -> new Wake(NodePod.of(answer.path("pod")),
                answer.path("resumed").asBoolean(), answer.path("cancelled_pause").asBoolean(),
                answer.path("generation").asLong(), answer.path("duration_ms").asDouble()));
    }

    /** {@code PUT /v1/network}: publishes the node's network profile, and returns its status. */
    public JsonNode putNetwork(SmolvmNode node, JsonNode profile) {
        return call(node, "PUT", "/v1/network", profile, CALL_TIMEOUT);
    }

    /** {@code GET /v1/network}: the node's network status, with the generation it applied. */
    public JsonNode network(SmolvmNode node) {
        return call(node, "GET", "/v1/network", null, CALL_TIMEOUT);
    }

    /**
     * {@code GET /v1/events}: the events after {@code cursor}.
     *
     * @param limit at most this many, from 1 to 500
     * @param pod   the sandbox id of the one pod whose events to read, or null for every pod
     */
    public EventPage events(SmolvmNode node, long cursor, int limit, String pod) {
        String path = "/v1/events?cursor=" + cursor + "&limit=" + limit + (pod == null ? "" : "&pod=" + encode(pod));
        JsonNode answer = call(node, "GET", path, null, CALL_TIMEOUT);
        return read(node, "GET", path, () -> {
            List<NodeEvent> events = new ArrayList<>();
            for (JsonNode event : answer.path("events")) {
                events.add(NodeEvent.of(event));
            }
            return new EventPage(events, answer.path("cursor").asLong(), answer.path("gap").asBoolean());
        });
    }

    /**
     * The sequence number of the node's last event, or 0 when it has none: the cursor of a page
     * read for a sandbox id that no pod has, which holds no event.
     */
    public long eventsHead(SmolvmNode node) {
        return events(node, 0, 1, HEAD_QUERY_POD).cursor();
    }

    /**
     * {@code GET /v1/events?follow=true}: every event after {@code cursor}, then each new one,
     * until the follow is closed, the node ends it, or no line comes for
     * {@link #FOLLOW_IDLE_LIMIT}. {@code listener} gets them one at a time, in order, on a thread
     * of the HTTP client.
     *
     * @param pod the sandbox id of the one pod whose events to follow, or null for every pod
     */
    public EventFollow follow(SmolvmNode node, long cursor, String pod, Consumer<NodeEvent> listener) {
        Follow<NodeEvent> follow = new Follow<>(node, "/v1/events?follow=true&cursor=" + cursor
                + (pod == null ? "" : "&pod=" + encode(pod)), line -> NodeEvent.of(json(line)), listener);
        follow.start(token(node), true);
        return follow;
    }

    /**
     * {@code GET /v1/network/events}: the network events after {@code cursor}.
     *
     * @param limit at most this many, from 1 to 500
     */
    public NetworkEventPage networkEvents(SmolvmNode node, long cursor, int limit) {
        String path = "/v1/network/events?cursor=" + cursor + "&limit=" + limit;
        JsonNode answer = call(node, "GET", path, null, CALL_TIMEOUT);
        return read(node, "GET", path, () -> {
            List<NodeNetworkEvent> events = new ArrayList<>();
            for (JsonNode event : answer.path("events")) {
                events.add(NodeNetworkEvent.of(event));
            }
            return new NetworkEventPage(events, answer.path("cursor").asLong());
        });
    }

    /**
     * The sequence number of the node's last network event, or 0 when it has none: the cursor
     * after the last page of the events that the node keeps.
     */
    public long networkEventsHead(SmolvmNode node) {
        long cursor = 0;
        while (true) {
            NetworkEventPage page = networkEvents(node, cursor, NETWORK_PAGE);
            if (page.events().size() < NETWORK_PAGE || page.cursor() <= cursor) {
                return page.cursor();
            }
            cursor = page.cursor();
        }
    }

    /**
     * {@code GET /v1/network/events?follow=true}: every network event after {@code cursor}, then
     * each new one, as {@link #follow} follows the pod events.
     */
    public EventFollow followNetwork(SmolvmNode node, long cursor, Consumer<NodeNetworkEvent> listener) {
        Follow<NodeNetworkEvent> follow = new Follow<>(node, "/v1/network/events?follow=true&cursor=" + cursor,
                line -> NodeNetworkEvent.of(json(line)), listener);
        follow.start(token(node), true);
        return follow;
    }

    /**
     * {@code GET /v1/logs/LOG}: each line written to one of the node's log files from now on, such
     * as {@link #K3S_LOG} or {@link #KUBE_APISERVER_AUDIT_LOG}, as {@link #follow} follows the
     * events. The node's blank keepalive lines do not reach {@code listener}.
     */
    public EventFollow followLog(SmolvmNode node, String log, Consumer<String> listener) {
        Follow<String> follow = new Follow<>(node, "/v1/logs/" + encode(log), line -> line, listener);
        follow.start(token(node), true);
        return follow;
    }

    private JsonNode json(String line) {
        try {
            return mapper.readTree(line);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private LeaseHeld lease(SmolvmNode node, String method, String path, ObjectNode body) {
        JsonNode answer = call(node, method, path, body, CALL_TIMEOUT);
        return read(node, method, path, () -> {
            JsonNode lease = answer.path("lease");
            String expires = NodePod.text(lease, "expires_at");
            return new LeaseHeld(NodePod.of(answer.path("pod")), new NodePod.Lease(NodePod.required(lease, "id"),
                    NodePod.required(lease, "kind"), NodePod.text(lease, "holder"),
                    expires == null ? null : Instant.parse(expires)));
        });
    }

    private JsonNode call(SmolvmNode node, String method, String path, JsonNode body, Duration timeout) {
        String token = token(node);
        HttpResponse<String> response = send(node, method, path, body, timeout, token);
        if (response.statusCode() == 401) {
            String fresh = readTokenAgain(node);
            if (!fresh.equals(token)) {
                LOG.infov("The node service of {0} refused its token; sending the new one in {1}", node.name(),
                        node.token());
                response = send(node, method, path, body, timeout, fresh);
            }
        }
        if (response.statusCode() / 100 != 2) {
            throw error(node, method, path, response.statusCode(), response.body());
        }
        String text = response.body();
        if (text == null || text.isBlank()) {
            return mapper.createObjectNode();
        }
        return read(node, method, path, () -> {
            try {
                return mapper.readTree(text);
            } catch (IOException e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        });
    }

    private HttpResponse<String> send(SmolvmNode node, String method, String path, JsonNode body, Duration timeout,
                                      String token) {
        HttpRequest.Builder request = request(node, path, token).timeout(timeout);
        if (body == null) {
            request.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            request.header("Content-Type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8));
        }
        try {
            return http.send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw unreachable(node, method, path, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw unreachable(node, method, path, e);
        }
    }

    private HttpRequest.Builder request(SmolvmNode node, String path, String token) {
        return HttpRequest.newBuilder(URI.create(node.baseUrl() + path))
                .header("Authorization", "Bearer " + token)
                .header("Accept", "application/json");
    }

    private String token(SmolvmNode node) {
        return tokens.computeIfAbsent(node.token(), source -> readToken(node));
    }

    private String readTokenAgain(SmolvmNode node) {
        String token = readToken(node);
        tokens.put(node.token(), token);
        return token;
    }

    private static String readToken(SmolvmNode node) {
        try {
            return node.token().read();
        } catch (IllegalStateException e) {
            throw new NodeApiException(NodeApiException.Code.UNAUTHORIZED, 0, node.name() + ": " + e.getMessage(),
                    List.of());
        }
    }

    private <T> T read(SmolvmNode node, String method, String path, Reader<T> reader) {
        try {
            return reader.read();
        } catch (RuntimeException e) {
            throw new NodeApiException(NodeApiException.Code.UNEXPECTED, node.name() + ": " + method + " " + path
                    + ": the answer cannot be read: " + e.getMessage(), e);
        }
    }

    @FunctionalInterface
    private interface Reader<T> {
        T read();
    }

    private NodeApiException error(SmolvmNode node, String method, String path, int status, String body) {
        JsonNode error = mapper.createObjectNode();
        try {
            if (body != null && !body.isBlank()) {
                error = mapper.readTree(body).path("error");
            }
        } catch (IOException unreadable) {
            LOG.debugv("The error answer of {0} to {1} {2} is not JSON", node.name(), method, path);
        }
        String code = NodePod.text(error, "code");
        NodeApiException.Code type = code == null ? byStatus(status) : NodeApiException.Code.of(code);
        List<String> reasons = new ArrayList<>();
        for (JsonNode reason : error.path("reasons")) {
            reasons.add(reason.asText());
        }
        String message = NodePod.text(error, "message");
        return new NodeApiException(type, status, node.name() + ": " + method + " " + path + ": " + status
                + (code == null ? "" : " " + code) + (message == null ? "" : ": " + message), reasons);
    }

    private static NodeApiException.Code byStatus(int status) {
        return switch (status) {
            case 400 -> NodeApiException.Code.INVALID;
            case 401 -> NodeApiException.Code.UNAUTHORIZED;
            case 404 -> NodeApiException.Code.NOT_FOUND;
            case 410 -> NodeApiException.Code.GONE;
            default -> NodeApiException.Code.UNEXPECTED;
        };
    }

    private static NodeApiException unreachable(SmolvmNode node, String method, String path, Exception e) {
        String reason = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        return new NodeApiException(NodeApiException.Code.UNREACHABLE,
                node.name() + ": " + method + " " + path + ": no answer: " + reason, e);
    }

    private static void putIfPresent(ObjectNode body, String field, String value) {
        if (value != null) {
            body.put(field, value);
        }
    }

    private static String podPath(String pod) {
        return "/v1/pods/" + encode(pod);
    }

    private static String leasePath(String pod, String lease) {
        return podPath(pod) + "/leases/" + encode(lease);
    }

    /** Percent-encodes everything but the unreserved characters, for one path segment or query value. */
    static String encode(String value) {
        StringBuilder encoded = new StringBuilder();
        for (byte b : value.getBytes(StandardCharsets.UTF_8)) {
            int c = b & 0xff;
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                    || c == '-' || c == '.' || c == '_' || c == '~') {
                encoded.append((char) c);
            } else {
                encoded.append('%').append(HEX[c >> 4]).append(HEX[c & 0xf]);
            }
        }
        return encoded.toString();
    }

    /**
     * One follow; a 401 reads the token again and starts once more. A timer fails it when no
     * line comes for the idle limit, counted from the request and from each line.
     */
    private final class Follow<T> implements EventFollow {

        private final SmolvmNode node;
        private final String path;
        private final Function<String, T> parse;
        private final Consumer<T> listener;
        private final CompletableFuture<Void> done = new CompletableFuture<>();
        private volatile boolean closed;
        private volatile Flow.Subscription subscription;
        private volatile CompletableFuture<HttpResponse<String>> sent;
        private volatile long lastLine;
        private volatile ScheduledFuture<?> idleCheck;

        Follow(SmolvmNode node, String path, Function<String, T> parse, Consumer<T> listener) {
            this.node = node;
            this.path = path;
            this.parse = parse;
            this.listener = listener;
            done.whenComplete((ignored, failure) -> {
                ScheduledFuture<?> check = idleCheck;
                if (check != null) {
                    check.cancel(false);
                }
            });
        }

        void start(String token, boolean retryOnUnauthorized) {
            if (closed) {
                return;
            }
            lastLine = System.nanoTime();
            HttpRequest request = request(node, path, token).GET().build();
            CompletableFuture<HttpResponse<String>> response = http.sendAsync(request, info -> info.statusCode() == 200
                    ? HttpResponse.BodySubscribers.fromLineSubscriber(new Lines(), lines -> "", StandardCharsets.UTF_8, null)
                    : HttpResponse.BodySubscribers.ofString(StandardCharsets.UTF_8));
            sent = response;
            response.whenComplete((answer, failure) -> ended(token, retryOnUnauthorized, answer, failure));
            if (idleCheck == null) {
                checkIdleIn(followIdleLimit.toNanos());
            }
        }

        private void checkIdleIn(long nanos) {
            if (done.isDone()) {
                return;
            }
            try {
                idleCheck = followTimer.schedule(this::checkIdle, nanos, TimeUnit.NANOSECONDS);
            } catch (RejectedExecutionException closedClient) {
                // The client is closed, which also ends the follow's request.
                LOG.debugv("No idle check for the follow of {0}: the client is closed", node.name());
            }
        }

        private void checkIdle() {
            long left = followIdleLimit.toNanos() - (System.nanoTime() - lastLine);
            if (left > 0) {
                checkIdleIn(left);
                return;
            }
            fail(new NodeApiException(NodeApiException.Code.UNREACHABLE, node.name() + ": GET " + path
                    + ": no event or keepalive line in " + followIdleLimit.toSeconds() + " s", null));
        }

        private void ended(String token, boolean retryOnUnauthorized, HttpResponse<String> answer, Throwable failure) {
            if (closed || done.isDone()) {
                done.complete(null);
                return;
            }
            if (failure != null) {
                Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                        ? failure.getCause() : failure;
                done.completeExceptionally(unreachable(node, "GET", path,
                        cause instanceof Exception exception ? exception : new IOException(cause)));
                return;
            }
            if (answer.statusCode() == 200) {
                done.complete(null);
                return;
            }
            if (answer.statusCode() == 401 && retryOnUnauthorized) {
                try {
                    String fresh = readTokenAgain(node);
                    if (!fresh.equals(token)) {
                        LOG.infov("The node service of {0} refused its token; following again with the new one in {1}",
                                node.name(), node.token());
                        start(fresh, false);
                        return;
                    }
                } catch (NodeApiException e) {
                    done.completeExceptionally(e);
                    return;
                }
            }
            done.completeExceptionally(error(node, "GET", path, answer.statusCode(), answer.body()));
        }

        private void fail(Throwable failure) {
            done.completeExceptionally(failure);
            cancel();
        }

        private void cancel() {
            Flow.Subscription current = subscription;
            if (current != null) {
                current.cancel();
            }
            CompletableFuture<HttpResponse<String>> response = sent;
            if (response != null) {
                response.cancel(true);
            }
        }

        @Override
        public CompletableFuture<Void> done() {
            return done;
        }

        @Override
        public void close() {
            closed = true;
            cancel();
            done.complete(null);
        }

        /** Each line of the stream: an event, or a blank keepalive line. */
        private final class Lines implements Flow.Subscriber<String> {

            @Override
            public void onSubscribe(Flow.Subscription lines) {
                subscription = lines;
                if (closed) {
                    lines.cancel();
                } else {
                    lines.request(1);
                }
            }

            @Override
            public void onNext(String line) {
                lastLine = System.nanoTime();
                if (!line.isBlank()) {
                    T event;
                    try {
                        event = parse.apply(line);
                    } catch (RuntimeException e) {
                        fail(new NodeApiException(NodeApiException.Code.UNEXPECTED, node.name() + ": GET " + path
                                + ": an event cannot be read: " + e.getMessage(), e));
                        return;
                    }
                    try {
                        listener.accept(event);
                    } catch (RuntimeException e) {
                        fail(e);
                        return;
                    }
                }
                lastLine = System.nanoTime();
                subscription.request(1);
            }

            @Override
            public void onError(Throwable failure) {
                // The response future fails with the same error, and ended() reports it.
            }

            @Override
            public void onComplete() {
                // The response future completes too, and ended() reports it.
            }
        }
    }
}
