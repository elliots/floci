package io.github.hectorvent.floci.core.common.microsandbox;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Authenticated transport to the generic native host helper, with no workload policy. */
public class MicrosandboxClient {
    private static final Pattern PLATFORM = Pattern.compile(
            "(?m)^\\s*[^A-Za-z\\r\\n]*Platform\\s+(?:macOS|Linux)\\s+(aarch64|arm64|x86_64|amd64)\\s*$");
    private final String url;
    private final String token;
    private final ObjectMapper mapper;
    private final MicrosandboxOperationJournal journal;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final Set<CompletableFuture<JsonNode>> operations = ConcurrentHashMap.newKeySet();
    private volatile String networkPolicy = "";

    public MicrosandboxClient(String url, String token, ObjectMapper mapper) {
        this(url, token, mapper, null);
    }

    public MicrosandboxClient(String url, String token, ObjectMapper mapper, MicrosandboxOperationJournal journal) {
        URI endpoint = URI.create(url);
        if ((!"http".equals(endpoint.getScheme()) && !"https".equals(endpoint.getScheme()))
                || endpoint.getHost() == null || endpoint.getUserInfo() != null
                || endpoint.getQuery() != null || endpoint.getFragment() != null) {
            throw new IllegalArgumentException("Microsandbox helper URL must be an HTTP or HTTPS endpoint");
        }
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("Microsandbox helper token is required");
        }
        this.url = url.replaceAll("/+$", "");
        this.token = token;
        this.mapper = mapper;
        this.journal = journal;
    }

    public void checkAvailable(Duration timeout) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url + "/health"))
                .timeout(timeout).GET().build();
        JsonNode health = send(request);
        if (!networkPolicy.isBlank() && !networkPolicy.equals(health.path("networkPolicy").asText())) {
            throw new IllegalStateException("Microsandbox helper network policy does not match Floci");
        }
        if (!"1".equals(health.path("contract").asText())
                || !"microsandbox".equals(health.path("name").asText())
                || !"ok".equals(health.path("status").asText())) {
            throw new IllegalStateException("Expected healthy microsandbox helper contract 1");
        }
        if (!"msb 0.7.8".equals(command(timeout, "--version"))) {
            throw new IllegalStateException("The microsandbox provider requires runtime 0.7.8");
        }
    }

    public void requireNetworkPolicy(String fingerprint) {
        networkPolicy = fingerprint == null ? "" : fingerprint;
    }

    public int port(Duration timeout) {
        int port = post("port", mapper.createObjectNode(), timeout).path("port").asInt();
        if (port < 1 || port > 65535) {
            throw new IllegalStateException("Microsandbox helper returned an invalid host port");
        }
        return port;
    }

    /** Runtime 0.7.8's doctor reports its own platform, which can differ from the Floci JVM. */
    public String hostArchitecture(Duration timeout) {
        JsonNode diagnostic = invoke(timeout, List.of("doctor"), "");
        Matcher platform = PLATFORM.matcher(diagnostic.path("stdout").asText() + "\n"
                + diagnostic.path("stderr").asText());
        if (!platform.find()) {
            throw new IllegalStateException("Microsandbox did not identify a supported Linux or macOS host architecture");
        }
        return switch (platform.group(1)) {
            case "aarch64", "arm64" -> "arm64";
            case "x86_64", "amd64" -> "x86_64";
            default -> throw new IllegalStateException("Unsupported microsandbox host architecture");
        };
    }

    public String command(Duration timeout, String... arguments) {
        return execute(timeout, List.of(arguments), "");
    }

    /** Reserve shared native capacity before removing a stopped restore source. */
    public String prepareAdmission(Duration timeout, List<String> arguments, String sourceName) {
        ObjectNode body = mapper.createObjectNode().put("sourceName", sourceName);
        body.set("arguments", mapper.valueToTree(arguments));
        String permit = post("capacity/prepare", body, timeout).path("token").asText();
        if (permit.isBlank()) {
            throw new IllegalStateException("Native admission permit is missing");
        }
        return permit;
    }

    public void cancelAdmission(Duration timeout, String permit) {
        post("capacity/cancel", mapper.createObjectNode().put("token", permit), timeout);
    }

    public String commandAdmitted(Duration timeout, String permit, List<String> arguments) {
        return invoke(timeout, arguments, "", null, permit).path("stdout").asText().strip();
    }

    /**
     * Resolves one lifecycle command after a completed helper error, when a read-only check
     * proves its intended postcondition on the same owned resource. This does not infer a
     * guest exit status. Lost responses, caller timeout and interruption never invoke the check.
     * A verified lifecycle completion returns empty output.
     */
    public String commandWithVerifiedCompletion(Duration timeout, BooleanSupplier completed, String... arguments) {
        return invoke(timeout, List.of(arguments), "", Objects.requireNonNull(completed))
                .path("stdout").asText().strip();
    }

    public String execute(Duration timeout, List<String> arguments, String input) {
        return executeRaw(timeout, arguments, input).strip();
    }

    /** Preserves application output, including partial log lines and terminal whitespace. */
    public String executeRaw(Duration timeout, List<String> arguments, String input) {
        return invoke(timeout, arguments, input).path("stdout").asText();
    }

    /** Read-only command planning; execution still passes through the ordinary operation journal. */
    public GuestTaskPlan guestTaskPlan(Duration timeout, JsonNode specification) {
        JsonNode response = post("guest-task/plan", specification, timeout);
        JsonNode arguments = response.path("arguments");
        if (!arguments.isArray() || arguments.isEmpty() || !response.path("stdin").isTextual()) {
            throw new IllegalStateException("Invalid microsandbox guest task plan");
        }
        List<String> command = new ArrayList<>();
        for (JsonNode argument : arguments) {
            if (!argument.isTextual() || argument.asText().indexOf('\0') >= 0) {
                throw new IllegalStateException("Invalid microsandbox guest task argument");
            }
            command.add(argument.asText());
        }
        if (command.getFirst().isEmpty()) {
            throw new IllegalStateException("Microsandbox guest task plan has no executable");
        }
        return new GuestTaskPlan(List.copyOf(command), response.path("stdin").asText());
    }

    public String guestTask(Duration timeout, String sandbox, JsonNode specification) {
        GuestTaskPlan plan = guestTaskPlan(timeout, specification);
        List<String> arguments = new ArrayList<>(List.of("exec", "--stream", sandbox, "--"));
        arguments.addAll(plan.arguments());
        return executeRaw(timeout, arguments, plan.stdin());
    }

    public record GuestTaskPlan(List<String> arguments, String stdin) {}

    public ExecStream streamExec(Duration timeout, List<String> arguments, boolean tty,
                                 Consumer<byte[]> output, Consumer<Integer> exited, Consumer<Throwable> failed) {
        if (arguments.size() < 4 || !"exec".equals(arguments.getFirst())) {
            throw new IllegalArgumentException("Native exec requires an attached guest command");
        }
        ExecStream stream = new ExecStream(output, exited, failed);
        try {
            URI endpoint = URI.create(url.replaceFirst("^http", "ws") + "/v1/exec");
            WebSocket.Builder builder = http.newWebSocketBuilder().connectTimeout(timeout)
                    .header("Authorization", "Bearer " + token);
            if (!networkPolicy.isBlank()) {
                builder.header("X-Floci-Network-Policy", networkPolicy);
            }
            stream.socket = builder.buildAsync(endpoint, stream)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            stream.operationId = journal == null ? null : journal.begin(url);
            operations.add(stream.completion);
            ObjectNode start = mapper.createObjectNode().put("type", "start").put("stdin", true)
                    .put("tty", tty).put("rows", 24).put("cols", 80);
            start.set("arguments", mapper.valueToTree(arguments));
            stream.send(start);
            return stream;
        } catch (Exception e) {
            stream.fail();
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("Cannot connect native exec stream", e);
        }
    }

    public final class ExecStream implements WebSocket.Listener, AutoCloseable {
        private final Consumer<byte[]> output;
        private final Consumer<Integer> exited;
        private final Consumer<Throwable> failed;
        private final CompletableFuture<JsonNode> completion = new CompletableFuture<>();
        private final StringBuilder fragment = new StringBuilder();
        private WebSocket socket;
        private String operationId;
        private CompletableFuture<WebSocket> sent;

        private ExecStream(Consumer<byte[]> output, Consumer<Integer> exited, Consumer<Throwable> failed) {
            this.output = output;
            this.exited = exited;
            this.failed = failed;
        }

        private synchronized void send(ObjectNode frame) {
            if (completion.isDone()) {
                return;
            }
            String text = frame.toString();
            sent = sent == null ? socket.sendText(text, true)
                    : sent.thenCompose(ignored -> socket.sendText(text, true));
            sent.exceptionally(failure -> { fail(); return null; });
        }

        public void write(byte[] bytes) {
            for (int offset = 0; offset < bytes.length; offset += 65536) {
                int length = Math.min(65536, bytes.length - offset);
                byte[] chunk = new byte[length];
                System.arraycopy(bytes, offset, chunk, 0, length);
                send(mapper.createObjectNode().put("type", "stdin").put("data", Base64.getEncoder().encodeToString(chunk)));
            }
        }

        public void resize(int rows, int columns) {
            if (rows > 0 && columns > 0 && rows <= 65535 && columns <= 65535) {
                send(mapper.createObjectNode().put("type", "resize").put("rows", rows).put("cols", columns));
            }
        }

        public void endInput() { send(mapper.createObjectNode().put("type", "eof")); }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence text, boolean last) {
            fragment.append(text);
            if (fragment.length() > 2 * 1024 * 1024) {
                fail();
                webSocket.abort();
                return null;
            }
            if (last) {
                try {
                    JsonNode frame = mapper.readTree(fragment.toString());
                    fragment.setLength(0);
                    switch (frame.path("type").asText()) {
                        case "stdout", "stderr" -> output.accept(Base64.getDecoder().decode(frame.path("data").asText()));
                        case "exit" -> {
                            if (!frame.path("exitCode").isIntegralNumber()) {
                                throw new IllegalStateException("Native exec exit status is missing");
                            }
                            if (journal != null) {
                                journal.complete(operationId);
                            }
                            completion.complete(frame);
                            operations.remove(completion);
                            exited.accept(frame.path("exitCode").asInt());
                            webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "");
                        }
                        default -> fail();
                    }
                } catch (Exception e) {
                    fail();
                }
            }
            webSocket.request(1);
            return null;
        }

        private void fail() {
            IllegalStateException failure = new IllegalStateException("Native exec stream ended without confirmed completion");
            if (completion.completeExceptionally(failure)) {
                if (journal != null && operationId != null) {
                    journal.lost(operationId);
                }
                failed.accept(failure);
            }
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int status, String reason) {
            if (!completion.isDone()) {
                fail();
            }
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable failure) { fail(); }

        @Override
        public void close() { send(mapper.createObjectNode().put("type", "cancel")); }
    }

    private JsonNode invoke(Duration timeout, List<String> arguments, String input) {
        return invoke(timeout, arguments, input, null);
    }

    private JsonNode invoke(Duration timeout, List<String> arguments, String input, BooleanSupplier completed) {
        return invoke(timeout, arguments, input, completed, "");
    }

    private JsonNode invoke(Duration timeout, List<String> arguments, String input, BooleanSupplier completed, String admission) {
        if (arguments.isEmpty()) {
            throw new IllegalArgumentException("A microsandbox command is required");
        }
        ObjectNode body = mapper.createObjectNode();
        body.set("arguments", mapper.valueToTree(arguments));
        body.put("input", input);
        if (!admission.isEmpty()) {
            body.put("admission", admission);
        }
        JsonNode result = post("command", body, timeout, mutates(arguments), completed);
        if (!result.has("exitCode") || result.path("exitCode").asInt(-1) != 0) {
            // Output and arguments can contain application credentials. Do not put them in errors.
            throw new IllegalStateException("Microsandbox operation failed; inspect the native runtime");
        }
        return result;
    }

    public JsonNode json(Duration timeout, String... arguments) {
        try {
            JsonNode response = mapper.readTree(command(timeout, arguments));
            if (response == null) {
                throw new IllegalStateException("Empty microsandbox runtime response");
            }
            return response;
        } catch (IOException e) {
            throw new IllegalStateException("Invalid microsandbox runtime response", e);
        }
    }

    private JsonNode post(String path, JsonNode body, Duration timeout) {
        return post(path, body, timeout, false, null);
    }

    private JsonNode post(String path, JsonNode body, Duration timeout, boolean mutating, BooleanSupplier completed) {
        try {
            HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url + "/v1/" + path))
                    .header("Authorization", "Bearer " + token)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)));
            if (!networkPolicy.isBlank()) {
                request.header("X-Floci-Network-Policy", networkPolicy);
            }
            if (!mutating) {
                return send(request.timeout(timeout).build());
            }
            String operationId = journal == null ? null : journal.begin(url);
            CompletableFuture<JsonNode> operation;
            try {
                operation = http.sendAsync(request.build(), HttpResponse.BodyHandlers.ofString())
                        .thenApply(response -> {
                            if (admissionRejected(response)) {
                                if (journal != null) {
                                    journal.complete(operationId);
                                }
                                throw new MicrosandboxCapacity.CapacityUnavailableException(
                                        "Shared native capacity unavailable; operation was not started");
                            }
                            if (completed != null && completedHelperFailure(response)) {
                                throw new CompletedHelperFailure();
                            }
                            JsonNode result = decode(response);
                            if (!result.path("exitCode").isIntegralNumber()) {
                                throw new IllegalStateException("Microsandbox command completion is missing");
                            }
                            if (journal != null) {
                                journal.complete(operationId);
                            }
                            return result;
                        }).whenComplete((result, failure) -> {
                            if (failure != null && !capacityRefusal(failure) && journal != null) {
                                journal.lost(operationId);
                            }
                        });
            } catch (RuntimeException e) {
                if (journal != null) {
                    journal.lost(operationId);
                }
                throw e;
            }
            operations.add(operation);
            operation.whenComplete((result, failure) -> {
                if (failure == null || capacityRefusal(failure)) {
                    operations.remove(operation);
                }
            });
            try {
                return await(operation, timeout);
            } catch (IllegalStateException failure) {
                if (completed == null || Thread.currentThread().isInterrupted()
                        || !(failure.getCause() instanceof ExecutionException execution)
                        || !(execution.getCause() instanceof CompletedHelperFailure)) {
                    throw failure;
                }
                boolean verified;
                try {
                    verified = completed.getAsBoolean();
                } catch (RuntimeException ignored) {
                    // A failed check proves nothing; its details may contain workload credentials.
                    throw failure;
                }
                if (!verified) {
                    throw failure;
                }
                if (journal != null) {
                    journal.complete(operationId);
                }
                operations.remove(operation);
                return mapper.createObjectNode().put("exitCode", 0).put("stdout", "").put("stderr", "");
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot encode microsandbox helper request", e);
        }
    }

    private boolean completedHelperFailure(HttpResponse<String> response) {
        if (response.statusCode() != 500 || !response.headers().firstValue("Content-Type")
                .map(value -> value.split(";", 2)[0].strip().equalsIgnoreCase("application/json")).orElse(false)) {
            return false;
        }
        try {
            JsonNode body = mapper.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .readTree(response.body());
            return body != null && body.isObject() && body.size() == 1 && body.path("error").isTextual()
                    && !body.path("error").asText().isBlank();
        } catch (IOException ignored) {
            // Malformed error responses cannot establish that the helper completed its operation.
            return false;
        }
    }

    private boolean admissionRejected(HttpResponse<String> response) {
        if (response.statusCode() != 409 || !response.headers().firstValue("Content-Type")
                .map(value -> value.split(";", 2)[0].strip().equalsIgnoreCase("application/json")).orElse(false)) {
            return false;
        }
        try {
            JsonNode body = mapper.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .readTree(response.body());
            return body != null && body.isObject() && body.size() == 3
                    && "CapacityUnavailable".equals(body.path("code").asText())
                    && body.path("operationStarted").isBoolean() && !body.path("operationStarted").asBoolean()
                    && body.path("error").isTextual();
        } catch (IOException ignored) {
            // A malformed refusal cannot prove that a native mutation did not start.
            return false;
        }
    }

    private static boolean capacityRefusal(Throwable failure) {
        while (failure instanceof CompletionException || failure instanceof ExecutionException) {
            failure = failure.getCause();
        }
        return failure instanceof MicrosandboxCapacity.CapacityUnavailableException;
    }

    private static final class CompletedHelperFailure extends IllegalStateException {
        private CompletedHelperFailure() {
            super("Microsandbox helper returned a completed operation error");
        }
    }

    private static boolean mutates(List<String> arguments) {
        return switch (arguments.getFirst()) {
            case "--version", "doctor", "inspect", "ls", "metrics" -> false;
            case "snap" -> arguments.size() < 2 || !Set.of("ls", "inspect", "verify").contains(arguments.get(1));
            default -> true;
        };
    }

    /** Caller cancellation does not cancel an accepted helper command or prove that it has finished. */
    public void awaitQuiescence(Duration timeout) {
        if (journal != null) {
            journal.requireRecovered(url);
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        for (CompletableFuture<JsonNode> operation : List.copyOf(operations)) {
            await(operation, Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
        }
    }

    private static JsonNode await(CompletableFuture<JsonNode> operation, Duration timeout) {
        try {
            return operation.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted calling microsandbox helper; operation outcome is unknown", e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("Microsandbox helper command is still pending; operation outcome is unknown", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof MicrosandboxCapacity.CapacityUnavailableException refused) {
                throw refused;
            }
            throw new IllegalStateException("Microsandbox helper command completion was lost; operation outcome is unknown", e);
        }
    }

    private JsonNode send(HttpRequest request) {
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            return decode(response);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted calling microsandbox helper; operation outcome is unknown", e);
        } catch (IOException e) {
            throw new IllegalStateException("Microsandbox helper call failed; operation outcome is unknown", e);
        }
    }

    private JsonNode decode(HttpResponse<String> response) {
        if (admissionRejected(response)) {
            throw new MicrosandboxCapacity.CapacityUnavailableException(
                    "Shared native capacity unavailable; operation was not started");
        }
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Microsandbox helper returned HTTP " + response.statusCode());
        }
        try {
            JsonNode body = mapper.readTree(response.body());
            if (body == null) {
                throw new IllegalStateException("Empty microsandbox helper response");
            }
            return body;
        } catch (IOException e) {
            throw new IllegalStateException("Invalid microsandbox helper response", e);
        }
    }
}
