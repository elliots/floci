package io.github.hectorvent.floci.core.common.microsandbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MicrosandboxClientTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(2);
    private final ObjectMapper mapper = new ObjectMapper();
    private HttpServer server;
    private ExecutorService executor;
    private MicrosandboxClient client;
    private volatile String authorization;
    private volatile JsonNode request;
    private volatile String contract = "1";
    private volatile int port = 18080;
    private volatile int exitCode;
    private volatile long delayMillis;
    private volatile boolean dropResponse;
    private volatile int responseStatus = 200;
    private volatile String responseBody;
    private volatile CountDownLatch entered;
    private volatile CountDownLatch finish;
    private volatile String doctor = "  • Platform            macOS aarch64\n  ✓ Host setup is ready.\n";
    private volatile ObjectNode plan;
    private volatile String policyHeader;
    private volatile String policyFingerprint = "";

    @BeforeEach
    void setup() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        executor = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(executor);
        server.createContext("/health", exchange -> respond(exchange,
                mapper.createObjectNode().put("name", "microsandbox").put("status", "ok").put("contract", contract)
                        .put("networkPolicy", policyFingerprint)));
        server.createContext("/v1/guest-task/plan", exchange -> {
            authorization = exchange.getRequestHeaders().getFirst("Authorization");
            policyHeader = exchange.getRequestHeaders().getFirst("X-Floci-Network-Policy");
            respond(exchange, plan);
        });
        server.createContext("/v1/port", exchange -> {
            authorization = exchange.getRequestHeaders().getFirst("Authorization");
            respond(exchange, mapper.createObjectNode().put("port", port));
        });
        server.createContext("/v1/command", exchange -> {
            authorization = exchange.getRequestHeaders().getFirst("Authorization");
            policyHeader = exchange.getRequestHeaders().getFirst("X-Floci-Network-Policy");
            request = mapper.readTree(exchange.getRequestBody());
            if (entered != null) {
                entered.countDown();
            }
            if (finish != null) {
                try {
                    finish.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    exchange.close();
                    return;
                }
            }
            if (dropResponse) {
                exchange.close();
                return;
            }
            if (delayMillis > 0) {
                try {
                    Thread.sleep(delayMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    exchange.close();
                    return;
                }
            }
            if (responseBody != null) {
                respond(exchange, responseStatus, responseBody);
                return;
            }
            String stdout = switch (request.path("arguments").get(0).asText()) {
                case "--version" -> "msb 0.7.8\n";
                case "doctor" -> "";
                default -> "secret process output";
            };
            String stderr = "doctor".equals(request.path("arguments").get(0).asText()) ? doctor : "secret process error";
            respond(exchange, mapper.createObjectNode().put("exitCode", exitCode)
                    .put("stdout", stdout).put("stderr", stderr));
        });
        server.start();
        client = new MicrosandboxClient("http://127.0.0.1:" + server.getAddress().getPort() + "/", "private-token", mapper);
    }

    @Test
    void capacityRefusalCompletesJournalAndAllowsRetry() {
        MicrosandboxOperationJournal journal = journal();
        client = client(journal);
        responseStatus = 409;
        responseBody = "{\"code\":\"CapacityUnavailable\",\"error\":\"full\",\"operationStarted\":false}";
        assertThrows(MicrosandboxCapacity.CapacityUnavailableException.class,
                () -> client.command(Duration.ofSeconds(2), "start", "guest"));
        assertDoesNotThrow(journal::beforeReset);
        assertDoesNotThrow(() -> client.awaitQuiescence(Duration.ofSeconds(1)));
        responseStatus = 200;
        responseBody = null;
        assertDoesNotThrow(() -> client.command(Duration.ofSeconds(2), "start", "guest"));
    }

    @Test
    void malformedCapacityRefusalCannotClearUncertainMutation() {
        MicrosandboxOperationJournal journal = journal();
        client = client(journal);
        responseStatus = 409;
        responseBody = "{\"code\":\"CapacityUnavailable\",\"error\":\"full\",\"operationStarted\":true}";
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> client.command(Duration.ofSeconds(2), "start", "guest"));
        assertFalse(failure instanceof MicrosandboxCapacity.CapacityUnavailableException);
        assertThrows(IllegalStateException.class, journal::beforeReset);
    }

    @Test
    void admittedCommandKeepsPermitOutsideRuntimeArguments() {
        client.commandAdmitted(Duration.ofSeconds(2), "opaque-permit", List.of("start", "guest"));
        assertEquals("opaque-permit", request.path("admission").asText());
        assertEquals(mapper.valueToTree(List.of("start", "guest")), request.path("arguments"));
    }

    @Test
    void guestPlansAreReadOnlyButTheirExecutionKeepsTheMutationFenceAndRawLogs() {
        MicrosandboxOperationJournal journal = journal();
        client = client(journal);
        ObjectNode specification = mapper.createObjectNode().put("action", "logs").put("id", "container-0");
        ObjectNode response = mapper.createObjectNode().put("stdin", "private\ninput");
        response.putArray("arguments").add("/bin/sh").add("-se");
        plan = response;
        assertEquals(List.of("/bin/sh", "-se"), client.guestTaskPlan(TIMEOUT, specification).arguments());
        assertDoesNotThrow(journal::beforeReset);
        responseBody = "{\"exitCode\":0,\"stdout\":\"  line\\n\",\"stderr\":\"\"}";
        assertEquals("  line\n", client.guestTask(TIMEOUT, "guest", specification));
        assertEquals(mapper.valueToTree(List.of("exec", "--stream", "guest", "--", "/bin/sh", "-se")), request.path("arguments"));
        assertEquals("private\ninput", request.path("input").asText());
        assertDoesNotThrow(journal::beforeReset);
        delayMillis = 200;
        assertThrows(IllegalStateException.class,
                () -> client.guestTask(Duration.ofMillis(30), "guest", specification));
        assertThrows(IllegalStateException.class, journal::beforeReset);
        client.awaitQuiescence(TIMEOUT);
    }

    @Test
    void malformedPlansFailBeforeAnyGuestCommand() {
        ObjectNode response = mapper.createObjectNode().put("stdin", "");
        response.putArray("arguments").add(1);
        plan = response;
        assertThrows(IllegalStateException.class, () -> client.guestTaskPlan(TIMEOUT, mapper.createObjectNode()));
        assertNull(request);
    }

    @Test
    void networkPolicyIsVerifiedAndSentOnAuthenticatedCommands() {
        client.requireNetworkPolicy("sha256:policy");
        assertThrows(IllegalStateException.class, () -> client.checkAvailable(TIMEOUT));
        assertNull(request);
        policyFingerprint = "sha256:policy";
        client.checkAvailable(TIMEOUT);
        assertEquals("sha256:policy", policyHeader);
    }

    @AfterEach
    void shutdown() {
        server.stop(0);
        executor.shutdownNow();
        executor.close();
    }

    @Test
    void authenticatesTransportAndPreservesCommandArgumentsAndInput() {
        client.checkAvailable(TIMEOUT);
        assertEquals("Bearer private-token", authorization);
        assertEquals(18080, client.port(TIMEOUT));
        List<String> arguments = List.of("exec", "guest", "--", "sh", "-c", "echo '$HOME'; echo literal");
        client.execute(TIMEOUT, arguments, "payload\nsecond line");
        assertEquals(mapper.valueToTree(arguments), request.path("arguments"));
        assertEquals("payload\nsecond line", request.path("input").asText());
    }

    @Test
    void rejectsContractMismatchAndInvalidPort() {
        contract = "2";
        assertThrows(IllegalStateException.class, () -> client.checkAvailable(TIMEOUT));
        port = 65536;
        assertThrows(IllegalStateException.class, () -> client.port(TIMEOUT));
    }

    @Test
    void commandErrorsDoNotExposeCredentialsOrGuestOutput() {
        exitCode = 1;
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> client.command(TIMEOUT, "run", "-e", "PASSWORD=private-password"));
        assertFalse(failure.toString().contains("private-password"));
        assertFalse(failure.toString().contains("secret process"));
        assertFalse(failure.toString().contains("private-token"));
    }

    @Test
    void identifiesTheRuntimeHostInsteadOfTheCallingJvmAndRejectsUnknownOutput() {
        assertEquals("arm64", client.hostArchitecture(TIMEOUT));
        doctor = "  • Platform            Linux x86_64\n";
        assertEquals("x86_64", client.hostArchitecture(TIMEOUT));
        doctor = "No supported platform diagnostic";
        assertThrows(IllegalStateException.class, () -> client.hostArchitecture(TIMEOUT));
    }

    @Test
    void timedOutCommandHasUnknownOutcome() {
        delayMillis = 500;
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> client.command(Duration.ofMillis(50), "snap", "restore", "saved"));
        assertTrue(failure.getMessage().contains("outcome is unknown"));
    }

    @Test
    void timedOutMutationRetainsResetFenceUntilTheHelperActuallyFinishes() {
        MicrosandboxOperationJournal journal = journal();
        client = client(journal);
        delayMillis = 200;
        assertThrows(IllegalStateException.class, () -> client.command(Duration.ofMillis(30), "run", "guest"));
        assertThrows(IllegalStateException.class, journal::beforeReset);
        assertDoesNotThrow(journal::checkReset, "A live command can still drain during reset preparation");
        assertThrows(IllegalStateException.class, () -> client.awaitQuiescence(Duration.ZERO));
        client.awaitQuiescence(TIMEOUT);
        assertDoesNotThrow(journal::beforeReset);
    }

    @Test
    void interruptionDoesNotCancelTheAcceptedHelperRequest() throws Exception {
        MicrosandboxOperationJournal journal = journal();
        client = client(journal);
        entered = new CountDownLatch(1);
        finish = new CountDownLatch(1);
        AtomicReference<Thread> caller = new AtomicReference<>();
        Future<?> request = executor.submit(() -> {
            caller.set(Thread.currentThread());
            assertThrows(IllegalStateException.class, () -> client.command(TIMEOUT, "snap", "restore", "saved"));
            assertTrue(Thread.currentThread().isInterrupted());
        });
        try {
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            caller.get().interrupt();
            request.get(1, TimeUnit.SECONDS);
            assertThrows(IllegalStateException.class, journal::beforeReset);
            assertThrows(IllegalStateException.class, () -> client.awaitQuiescence(Duration.ZERO));
        } finally {
            finish.countDown();
        }
        client.awaitQuiescence(TIMEOUT);
        assertDoesNotThrow(journal::beforeReset);
    }

    @Test
    void lostCompletionRemainsARecoveryFenceAcrossClientAndJournalRestart() {
        AccountAwareStorageBackend<MicrosandboxOperationJournal.PendingCommand> storage =
                AccountAwareStorageBackend.inMemory("000000000000");
        MicrosandboxOperationJournal journal = journal(storage);
        client = client(journal);
        dropResponse = true;
        assertThrows(IllegalStateException.class, () -> client.command(TIMEOUT, "run", "PASSWORD=private-password"));
        assertThrows(IllegalStateException.class, () -> client.awaitQuiescence(Duration.ZERO));
        assertThrows(IllegalStateException.class, journal::checkReset);
        assertEquals(1, storage.scanAllAccounts().size());
        assertFalse(storage.scanAllAccounts().getFirst().toString().contains("private-password"));
        MicrosandboxOperationJournal restarted = journal(storage);
        MicrosandboxClient freshClient = client(restarted);
        assertThrows(IllegalStateException.class, restarted::checkReset);
        assertThrows(IllegalStateException.class, restarted::beforeReset);
        assertThrows(IllegalStateException.class, () -> freshClient.awaitQuiescence(TIMEOUT));
        dropResponse = false;
        assertThrows(IllegalStateException.class, () -> client.command(TIMEOUT, "run", "same-process-replacement"));
        assertThrows(IllegalStateException.class, () -> freshClient.command(TIMEOUT, "run", "replacement"));
        assertEquals(1, storage.scanAllAccounts().size());
    }

    @Test
    void readOnlyFailuresNeverCreateMutationRecoveryRecords() {
        MicrosandboxOperationJournal journal = journal();
        client = client(journal);
        dropResponse = true;
        for (List<String> command : List.of(List.of("ls"), List.of("inspect", "guest"), List.of("doctor"),
                List.of("--version"), List.of("snap", "ls"), List.of("snap", "inspect", "saved"),
                List.of("snap", "verify", "saved"))) {
            assertThrows(IllegalStateException.class, () -> client.execute(TIMEOUT, command, ""));
        }
        assertDoesNotThrow(journal::beforeReset);
        assertDoesNotThrow(() -> client.awaitQuiescence(Duration.ZERO));
    }

    @Test
    void completedHelperFailureCanResolveOnlyItsVerifiedLifecycleCommand() {
        MicrosandboxOperationJournal journal = journal();
        client = client(journal);
        responseStatus = 500;
        responseBody = "{\"error\":\"secret runtime diagnostic\"}";
        AtomicInteger checks = new AtomicInteger();

        assertEquals("", client.commandWithVerifiedCompletion(TIMEOUT, () -> {
            checks.incrementAndGet();
            return true;
        }, "exec", "owned-guest", "--", "shutdown"));

        assertEquals(1, checks.get());
        assertDoesNotThrow(journal::beforeReset);
        assertDoesNotThrow(() -> client.awaitQuiescence(Duration.ZERO));
        responseBody = null;
        assertEquals("secret process output", client.command(TIMEOUT, "start", "owned-guest"));
    }

    @Test
    void failedOrThrowingCompletionProofRetainsTheRecoveryFence() {
        responseStatus = 500;
        responseBody = "{\"error\":\"secret runtime diagnostic\"}";
        for (boolean throwsFailure : List.of(false, true)) {
            MicrosandboxOperationJournal journal = journal();
            client = client(journal);
            AtomicInteger checks = new AtomicInteger();
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> client.commandWithVerifiedCompletion(TIMEOUT, () -> {
                        checks.incrementAndGet();
                        if (throwsFailure) {
                            throw new IllegalStateException("secret verification credential");
                        }
                        return false;
                    }, "exec", "guest", "--", "shutdown"));
            assertEquals(1, checks.get());
            assertFalse(failure.toString().contains("secret"));
            assertThrows(IllegalStateException.class, journal::beforeReset);
            assertThrows(IllegalStateException.class, () -> client.awaitQuiescence(Duration.ZERO));
            assertThrows(IllegalStateException.class, () -> client.command(TIMEOUT, "start", "guest"));
        }
    }

    @Test
    void defaultCommandNeverResolvesACompletedHelperFailure() {
        MicrosandboxOperationJournal journal = journal();
        client = client(journal);
        responseStatus = 500;
        responseBody = "{\"error\":\"runtime failure\"}";
        assertThrows(IllegalStateException.class, () -> client.command(TIMEOUT, "exec", "guest", "--", "shutdown"));
        assertThrows(IllegalStateException.class, journal::beforeReset);
        assertThrows(IllegalStateException.class, () -> client.awaitQuiescence(Duration.ZERO));
    }

    @Test
    void malformedErrorsAndSuccessesNeverInvokeCompletionProof() {
        AtomicInteger checks = new AtomicInteger();
        for (String body : List.of("not json", "{}", "{\"error\":null}", "{\"error\":\"\"}",
                "{\"error\":\"failure\",\"exitCode\":0}", "{\"error\":\"failure\"} {}")) {
            responseBody = body;
            responseStatus = 500;
            client = client(journal());
            assertThrows(IllegalStateException.class, () -> client.commandWithVerifiedCompletion(TIMEOUT,
                    () -> checks.incrementAndGet() > 0, "exec", "guest", "--", "shutdown"));
        }
        for (int status : List.of(200, 400, 502, 503)) {
            responseStatus = status;
            responseBody = "{\"error\":\"runtime failure\"}";
            client = client(journal());
            assertThrows(IllegalStateException.class, () -> client.commandWithVerifiedCompletion(TIMEOUT,
                    () -> checks.incrementAndGet() > 0, "exec", "guest", "--", "shutdown"));
        }
        assertEquals(0, checks.get());
    }

    @Test
    void knownGuestExitNeverInvokesCompletionProof() {
        AtomicInteger checks = new AtomicInteger();
        assertEquals("secret process output", client.commandWithVerifiedCompletion(TIMEOUT,
                () -> checks.incrementAndGet() > 0, "exec", "guest", "--", "true"));
        exitCode = 7;
        assertThrows(IllegalStateException.class, () -> client.commandWithVerifiedCompletion(TIMEOUT,
                () -> checks.incrementAndGet() > 0, "exec", "guest", "--", "false"));
        assertEquals(0, checks.get());
    }

    @Test
    void timeoutDoesNotInvokeProofEvenWhenAnErrorResponseArrivesLater() throws Exception {
        MicrosandboxOperationJournal journal = journal();
        client = client(journal);
        responseStatus = 500;
        responseBody = "{\"error\":\"runtime failure\"}";
        entered = new CountDownLatch(1);
        finish = new CountDownLatch(1);
        AtomicInteger checks = new AtomicInteger();
        try {
            assertThrows(IllegalStateException.class, () -> client.commandWithVerifiedCompletion(Duration.ofMillis(50),
                    () -> checks.incrementAndGet() > 0, "exec", "guest", "--", "shutdown"));
            assertTrue(entered.await(1, TimeUnit.SECONDS));
        } finally {
            finish.countDown();
        }
        assertThrows(IllegalStateException.class, () -> client.awaitQuiescence(TIMEOUT));
        assertEquals(0, checks.get());
        assertThrows(IllegalStateException.class, journal::beforeReset);
    }

    @Test
    void lostResponseNeverInvokesCompletionProof() {
        MicrosandboxOperationJournal journal = journal();
        client = client(journal);
        dropResponse = true;
        AtomicInteger checks = new AtomicInteger();
        assertThrows(IllegalStateException.class, () -> client.commandWithVerifiedCompletion(TIMEOUT,
                () -> checks.incrementAndGet() > 0, "exec", "guest", "--", "shutdown"));
        assertEquals(0, checks.get());
        assertThrows(IllegalStateException.class, journal::beforeReset);
    }

    @Test
    void interruptedCallerNeverInvokesCompletionProof() throws Exception {
        MicrosandboxOperationJournal journal = journal();
        client = client(journal);
        responseStatus = 500;
        responseBody = "{\"error\":\"runtime failure\"}";
        entered = new CountDownLatch(1);
        finish = new CountDownLatch(1);
        AtomicInteger checks = new AtomicInteger();
        AtomicReference<Thread> caller = new AtomicReference<>();
        Future<?> pending = executor.submit(() -> {
            caller.set(Thread.currentThread());
            assertThrows(IllegalStateException.class, () -> client.commandWithVerifiedCompletion(TIMEOUT,
                    () -> checks.incrementAndGet() > 0, "exec", "guest", "--", "shutdown"));
            assertTrue(Thread.currentThread().isInterrupted());
        });
        try {
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            caller.get().interrupt();
            pending.get(1, TimeUnit.SECONDS);
        } finally {
            finish.countDown();
        }
        assertThrows(IllegalStateException.class, () -> client.awaitQuiescence(TIMEOUT));
        assertEquals(0, checks.get());
        assertThrows(IllegalStateException.class, journal::beforeReset);
    }

    @Test
    void verifiedCompletionDoesNotClearAnotherUnknownCommand() throws Exception {
        AccountAwareStorageBackend<MicrosandboxOperationJournal.PendingCommand> storage =
                AccountAwareStorageBackend.inMemory("000000000000");
        MicrosandboxOperationJournal journal = journal(storage);
        client = client(journal);
        CountDownLatch verifyEntered = new CountDownLatch(1);
        CountDownLatch releaseError = new CountDownLatch(1);
        server.removeContext("/v1/command");
        server.createContext("/v1/command", exchange -> {
            JsonNode body = mapper.readTree(exchange.getRequestBody());
            if (body.path("arguments").get(1).asText().equals("verified")) {
                verifyEntered.countDown();
                try {
                    releaseError.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    exchange.close();
                    return;
                }
                respond(exchange, 500, "{\"error\":\"runtime failure\"}");
            } else {
                respond(exchange, 200, "{}");
            }
        });
        Future<String> verified = executor.submit(() -> client.commandWithVerifiedCompletion(TIMEOUT,
                () -> true, "exec", "verified", "--", "shutdown"));
        String unknownId;
        try {
            assertTrue(verifyEntered.await(1, TimeUnit.SECONDS));
            String verifiedId = storage.scanAllAccounts().getFirst().id();
            assertThrows(IllegalStateException.class, () -> client.command(TIMEOUT, "start", "unknown"));
            assertEquals(2, storage.scanAllAccounts().size());
            unknownId = storage.scanAllAccounts().stream().map(MicrosandboxOperationJournal.PendingCommand::id)
                    .filter(id -> !id.equals(verifiedId)).findFirst().orElseThrow();
        } finally {
            releaseError.countDown();
        }
        assertEquals("", verified.get(1, TimeUnit.SECONDS));
        assertEquals(1, storage.scanAllAccounts().size());
        assertEquals(unknownId, storage.scanAllAccounts().getFirst().id());
        assertThrows(IllegalStateException.class, journal::beforeReset);
        assertThrows(IllegalStateException.class, () -> client.awaitQuiescence(Duration.ZERO));
        assertThrows(IllegalStateException.class, () -> client.command(TIMEOUT, "start", "another"));
    }

    private MicrosandboxClient client(MicrosandboxOperationJournal journal) {
        return new MicrosandboxClient("http://127.0.0.1:" + server.getAddress().getPort(), "private-token", mapper, journal);
    }

    private MicrosandboxOperationJournal journal() {
        return journal(AccountAwareStorageBackend.inMemory("000000000000"));
    }

    private MicrosandboxOperationJournal journal(AccountAwareStorageBackend<MicrosandboxOperationJournal.PendingCommand> storage) {
        StorageFactory factory = mock(StorageFactory.class);
        EmulatorConfig config = mock(EmulatorConfig.class);
        when(config.defaultAccountId()).thenReturn("000000000000");
        when(factory.<MicrosandboxOperationJournal.PendingCommand>create(eq("microsandbox"),
                eq("microsandbox-commands.json"), any())).thenReturn(storage);
        return new MicrosandboxOperationJournal(factory, config);
    }

    private void respond(HttpExchange exchange, ObjectNode response) throws IOException {
        respond(exchange, 200, mapper.writeValueAsString(response));
    }

    private void respond(HttpExchange exchange, int status, String response) throws IOException {
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (exchange) {
            exchange.getResponseBody().write(bytes);
        }
    }
}
