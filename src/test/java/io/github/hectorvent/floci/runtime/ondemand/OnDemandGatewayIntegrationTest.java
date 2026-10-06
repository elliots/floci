package io.github.hectorvent.floci.runtime.ondemand;

import io.github.hectorvent.floci.runtime.ondemand.ActivationCoordinator.State;
import io.github.hectorvent.floci.runtime.ondemand.OnDemandTestSupport.FakeRuntime;
import io.github.hectorvent.floci.runtime.ondemand.WorkloadDefinition.BackendProtocol;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.net.HostAndPort;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class OnDemandGatewayIntegrationTest {
    private Vertx vertx;
    private HttpClient client;
    private HttpServer backend;
    private OnDemandGateway gateway;
    private ActivationCoordinator coordinator;
    private final FakeRuntime runtime = new FakeRuntime();
    private final List<Throwable> unhandled = new CopyOnWriteArrayList<>();
    private final AtomicLong time = new AtomicLong();
    private volatile Consumer<HttpServerRequest> backendHandler = request -> request.response().end("ok");

    @BeforeEach
    void start() throws Exception {
        vertx = Vertx.vertx().exceptionHandler(unhandled::add);
        client = vertx.createHttpClient();
        backend = waitFor(vertx.createHttpServer().requestHandler(request -> backendHandler.accept(request))
                .listen(0, "127.0.0.1"));
        runtime.backend = URI.create("http://127.0.0.1:" + backend.actualPort());
        coordinator = new ActivationCoordinator(Executors.newVirtualThreadPerTaskExecutor(),
                (definition, uri, timeout) -> {}, 2, 128, 64, time::get);
        WorkloadDefinition definition = OnDemandTestSupport.definition("orders", runtime.backend, List.of());
        coordinator.register(definition, runtime);
        coordinator.reconcile("orders", false, true);
        gateway = new OnDemandGateway(vertx, List.of(definition), () -> coordinator);
        waitFor(gateway.start("127.0.0.1", 0));
    }

    @AfterEach
    void close() throws Exception {
        gateway.close();
        coordinator.close();
        waitFor(client.close());
        waitFor(backend.close());
        waitFor(vertx.close());
        assertEquals(List.of(), unhandled, "No unhandled event-loop exceptions are allowed");
    }

    @Test
    void coldBinaryPostPreservesMethodQueryHeadersStatusAndDuplicateCookies() throws Exception {
        byte[] bytes = {0, 1, 2, (byte) 255};
        CompletableFuture<String> received = new CompletableFuture<>();
        backendHandler = request -> request.bodyHandler(body -> {
            received.complete(request.method() + " " + request.uri() + " " + request.getHeader("Host")
                    + " " + request.getHeader("Authorization") + " " + request.getHeader("X-Hop"));
            request.response().setStatusCode(201).headers().add("Set-Cookie", "a=1").add("Set-Cookie", "b=2");
            request.response().end(body);
        });
        Reply response = waitFor(client.request(HttpMethod.POST, gateway.port(), "127.0.0.1", "/create?a=%2F&a=2")
                .compose(request -> request.putHeader("Host", "orders.test")
                        .putHeader("Authorization", "Bearer customer-token")
                        .putHeader("Connection", "X-Hop").putHeader("X-Hop", "remove-me").send(Buffer.buffer(bytes)))
                .compose(this::readReply));
        assertEquals(201, response.status());
        assertArrayEquals(bytes, response.body().getBytes());
        assertEquals(List.of("a=1", "b=2"), response.headers().getAll("Set-Cookie"));
        assertEquals("POST /create?a=%2F&a=2 orders.test Bearer customer-token null", received.get(5, TimeUnit.SECONDS));
        assertEquals(1, runtime.starts.get());
    }

    @Test
    void exactHealthRulesReturnConfiguredResponsesWithoutStarting() throws Exception {
        Reply health = waitFor(send(HttpMethod.GET, "/health"));
        assertEquals(200, health.status());
        assertEquals("{\"status\":\"UP\"}", health.body().toString());
        assertEquals("no-store", health.headers().get("Cache-Control"));
        Reply head = waitFor(send(HttpMethod.HEAD, "/health"));
        assertEquals(0, head.body().length());
        assertEquals(0, runtime.starts.get());
        assertEquals(200, waitFor(send(HttpMethod.POST, "/health")).status());
        assertEquals(1, runtime.starts.get(), "POST to a health path is real application traffic");
    }

    @Test
    void runningHealthFailureIsForwardedAndDoesNotExtendIdleTime() throws Exception {
        assertEquals(200, waitFor(send(HttpMethod.GET, "/orders")).status());
        backendHandler = request -> request.response().setStatusCode(503).end("database unavailable");
        time.addAndGet(TimeUnit.SECONDS.toNanos(90));
        Reply health = waitFor(send(HttpMethod.GET, "/health"));
        assertEquals(503, health.status());
        assertEquals("database unavailable", health.body().toString());
        time.addAndGet(TimeUnit.SECONDS.toNanos(31));
        coordinator.tick("orders");
        await().atMost(Duration.ofSeconds(5)).until(() -> coordinator.state("orders") == State.SLEEPING);
    }

    @Test
    void simultaneousColdRequestsWaitForOneStartup() throws Exception {
        CountDownLatch starting = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        runtime.startAction = () -> {
            starting.countDown();
            try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        };
        List<Future<Reply>> requests = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            requests.add(send(HttpMethod.GET, "/orders"));
        }
        assertTrue(starting.await(5, TimeUnit.SECONDS));
        assertEquals(1, runtime.starts.get());
        release.countDown();
        for (Future<Reply> request : requests) {
            assertEquals(200, waitFor(request).status());
        }
        assertEquals(1, runtime.starts.get());
    }

    @Test
    void responseStreamsKeepTheWorkloadAwakeUntilCompletion() throws Exception {
        AtomicReference<HttpServerResponse> stream = new AtomicReference<>();
        backendHandler = request -> {
            stream.set(request.response());
            request.response().setChunked(true).write("first");
        };
        HttpClientResponse response = waitForResponse(client.request(HttpMethod.GET, gateway.port(), "127.0.0.1", "/events")
                .compose(request -> request.putHeader("Host", "orders.test").send()));
        Future<Buffer> body = readBody(response);
        time.addAndGet(TimeUnit.SECONDS.toNanos(600));
        coordinator.tick("orders");
        assertEquals(State.READY, coordinator.state("orders"));
        assertFalse(body.isComplete());
        waitFor(stream.get().end("last"));
        assertEquals("firstlast", waitFor(body).toString());
        time.addAndGet(TimeUnit.SECONDS.toNanos(121));
        coordinator.tick("orders");
        await().atMost(Duration.ofSeconds(5)).until(() -> coordinator.state("orders") == State.SLEEPING);
    }

    @Test
    void disconnectedStreamsReleaseTheWorkloadLease() throws Exception {
        backendHandler = request -> request.response().setChunked(true).write("first");
        HttpClientResponse response = waitForResponse(client.request(HttpMethod.GET, gateway.port(), "127.0.0.1", "/events")
                .compose(request -> request.putHeader("Host", "orders.test").send()));
        response.request().reset();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            time.addAndGet(TimeUnit.SECONDS.toNanos(121));
            coordinator.tick("orders");
            assertEquals(State.SLEEPING, coordinator.state("orders"));
        });
    }

    @Test
    void websocketFramesAreRelayedAndConnectionHoldsAWorkloadLease() throws Exception {
        backendHandler = request -> request.toWebSocket().onSuccess(socket ->
                socket.textMessageHandler(text -> socket.writeTextMessage("echo:" + text)));
        WebSocket socket = waitFor(client.webSocket(new WebSocketConnectOptions()
                .setHost("127.0.0.1").setPort(gateway.port()).setURI("/socket?session=1")
                .addHeader("Host", "orders.test")));
        CompletableFuture<String> echoed = new CompletableFuture<>();
        socket.textMessageHandler(echoed::complete);
        waitFor(socket.writeTextMessage("hello"));
        assertEquals("echo:hello", echoed.get(5, TimeUnit.SECONDS));
        time.addAndGet(TimeUnit.SECONDS.toNanos(600));
        coordinator.tick("orders");
        assertEquals(State.READY, coordinator.state("orders"));
        waitFor(socket.close());
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            time.addAndGet(TimeUnit.SECONDS.toNanos(121));
            coordinator.tick("orders");
            assertEquals(State.SLEEPING, coordinator.state("orders"));
        });
    }

    @Test
    void startupFailureIsNotReportedAsHealthyAndSubsequentRequestCanRetry() throws Exception {
        runtime.failStart = true;
        assertEquals(503, waitFor(send(HttpMethod.GET, "/orders")).status());
        assertEquals(503, waitFor(send(HttpMethod.GET, "/health")).status());
        runtime.failStart = false;
        assertEquals(200, waitFor(send(HttpMethod.GET, "/orders")).status());
        assertEquals(2, runtime.starts.get());
    }

    @Test
    void unknownHostsNeverActivateAnyWorkload() throws Exception {
        Reply reply = waitFor(client.request(HttpMethod.GET, gateway.port(), "127.0.0.1", "/orders")
                .compose(request -> request.putHeader("Host", "unknown.test").send()).compose(this::readReply));
        assertEquals(404, reply.status());
        assertEquals(0, runtime.starts.get());
    }

    @Test
    void http2GrpcPreservesFramesAuthorityMetadataAndTrailers() throws Exception {
        useHttp2();
        Buffer frame = Buffer.buffer().appendByte((byte) 0).appendInt(4).appendBytes(new byte[]{0, 1, 2, (byte) 255});
        CompletableFuture<String> received = new CompletableFuture<>();
        backendHandler = request -> request.bodyHandler(body -> {
            received.complete(request.version() + " " + request.host() + " " + request.uri() + " "
                    + request.getHeader("TE") + " " + request.getHeader("Authorization"));
            request.response().putHeader("Content-Type", "application/grpc")
                    .putHeader("initial-bin", "AP8=").putTrailer("grpc-status", "0")
                    .putTrailer("grpc-status-details-bin", "AQI=")
                    .putTrailer("x-result", List.<String>of("one", "two")).end(body);
        });
        HttpClientResponse response = waitForResponse(grpcRequest("/test.Echo/Unary").compose(request ->
                request.putHeader("Authorization", "Bearer customer").send(frame)));
        assertEquals(HttpVersion.HTTP_2, response.version());
        assertEquals(frame, waitFor(readBody(response)));
        assertEquals("AP8=", response.getHeader("initial-bin"));
        assertEquals("0", response.getTrailer("grpc-status"));
        assertEquals("AQI=", response.getTrailer("grpc-status-details-bin"));
        assertEquals(List.of("one", "two"), response.trailers().getAll("x-result"));
        assertEquals("HTTP_2 orders.test /test.Echo/Unary trailers Bearer customer", received.get(5, TimeUnit.SECONDS));
        assertEquals(1, runtime.starts.get());
    }

    @Test
    void bidirectionalHttp2StreamsRelayBeforeUploadEndsAndHoldTheLease() throws Exception {
        useHttp2();
        backendHandler = request -> {
            HttpServerResponse response = request.response().putHeader("Content-Type", "application/grpc");
            request.handler(response::write);
            request.endHandler(ignored -> response.putTrailer("grpc-status", "0").end());
        };
        HttpClientRequest request = waitFor(grpcRequest("/test.Echo/Bidi"));
        Future<HttpClientResponse> responseFuture = request.response().map(HttpClientResponse::pause);
        waitFor(request.write("first"));
        HttpClientResponse response = waitForResponse(responseFuture);
        CompletableFuture<String> first = new CompletableFuture<>();
        CompletableFuture<Void> ended = new CompletableFuture<>();
        StringBuilder received = new StringBuilder();
        response.handler(buffer -> {
            received.append(buffer);
            first.complete(received.toString());
        });
        response.endHandler(ignored -> ended.complete(null));
        response.resume();
        assertEquals("first", first.get(5, TimeUnit.SECONDS));
        time.addAndGet(TimeUnit.SECONDS.toNanos(600));
        coordinator.tick("orders");
        assertEquals(State.READY, coordinator.state("orders"));
        waitFor(request.end("last"));
        ended.get(5, TimeUnit.SECONDS);
        assertEquals("firstlast", received.toString());
        assertEquals("0", response.getTrailer("grpc-status"));
        awaitSleeping();
    }

    @Test
    void cancellingOneHttp2StreamDoesNotCancelAnotherOnTheSameConnection() throws Exception {
        useHttp2();
        CompletableFuture<Void> cancelledBackend = new CompletableFuture<>();
        backendHandler = request -> {
            if (request.path().endsWith("/Cancel")) {
                request.response().closeHandler(ignored -> cancelledBackend.complete(null));
                request.response().putHeader("Content-Type", "application/grpc").write("open");
            } else {
                request.response().putHeader("Content-Type", "application/grpc").putTrailer("grpc-status", "0").end("ok");
            }
        };
        HttpClientResponse cancelled = waitForResponse(grpcRequest("/test.Echo/Cancel").compose(HttpClientRequest::send));
        HttpClientResponse sibling = waitForResponse(grpcRequest("/test.Echo/Unary").compose(HttpClientRequest::send));
        cancelled.request().reset(8);
        assertEquals("ok", waitFor(readBody(sibling)).toString());
        assertEquals("0", sibling.getTrailer("grpc-status"));
        cancelledBackend.get(5, TimeUnit.SECONDS);
        awaitSleeping();
    }

    @Test
    void cancelledOpenHttp2UploadReleasesItsLeaseWithoutAnEventLoopError() throws Exception {
        useHttp2();
        backendHandler = request -> {
            request.handler(buffer -> request.response().putHeader("Content-Type", "application/grpc").write(buffer));
            request.endHandler(ignored -> request.response().putTrailer("grpc-status", "0").end());
        };
        HttpClientRequest upload = waitFor(grpcRequest("/test.Echo/Bidi"));
        Future<HttpClientResponse> reply = upload.response().map(HttpClientResponse::pause);
        waitFor(upload.write("open"));
        HttpClientResponse response = waitForResponse(reply);
        Future<Buffer> body = readBody(response);
        upload.reset(8);
        assertThrows(ExecutionException.class, () -> waitFor(body));
        awaitSleeping();
    }

    @Test
    void backendStreamFailureDoesNotCloseAnActiveSiblingHttp2Stream() throws Exception {
        useHttp2();
        AtomicReference<HttpServerResponse> failedBackend = new AtomicReference<>();
        AtomicReference<HttpServerResponse> siblingBackend = new AtomicReference<>();
        backendHandler = request -> {
            HttpServerResponse response = request.response().putHeader("Content-Type", "application/grpc");
            if (request.path().endsWith("/Fail")) {
                failedBackend.set(response);
            } else {
                siblingBackend.set(response);
            }
            response.write("first");
        };
        HttpClientResponse failed = waitForResponse(grpcRequest("/test.Echo/Fail").compose(HttpClientRequest::send)
                .map(HttpClientResponse::pause));
        HttpClientResponse sibling = waitForResponse(grpcRequest("/test.Echo/Sibling").compose(HttpClientRequest::send)
                .map(HttpClientResponse::pause));
        Future<Buffer> failedBody = readBody(failed);
        Future<Buffer> siblingBody = readBody(sibling);
        vertx.runOnContext(ignored -> failedBackend.get().reset(2));
        assertThrows(ExecutionException.class, () -> waitFor(failedBody));
        assertFalse(siblingBody.isComplete());
        waitFor(siblingBackend.get().putTrailer("grpc-status", "0").end("last"));
        assertEquals("firstlast", waitFor(siblingBody).toString());
        assertEquals("0", sibling.getTrailer("grpc-status"));
        awaitSleeping();
    }

    @Test
    void grpcTrailersOnlyErrorsAndStartupFailuresRemainGrpcErrors() throws Exception {
        useHttp2();
        runtime.failStart = true;
        HttpClientResponse failed = waitForResponse(grpcRequest("/test.Echo/Unary").compose(HttpClientRequest::send));
        waitFor(readBody(failed));
        assertEquals(200, failed.statusCode());
        assertEquals("14", failed.getHeader("grpc-status"));
        runtime.failStart = false;
        backendHandler = request -> request.response().putHeader("Content-Type", "application/grpc")
                .putHeader("grpc-status", "7").putHeader("grpc-message", "denied%20here").end();
        HttpClientResponse denied = waitForResponse(grpcRequest("/test.Echo/Unary").compose(HttpClientRequest::send));
        assertEquals(0, waitFor(readBody(denied)).length());
        assertEquals("7", denied.getHeader("grpc-status"));
        assertEquals("denied%20here", denied.getHeader("grpc-message"));
    }

    @Test
    void http2ClientsCanReachHttp1BackendsAndReceiveResponseTrailers() throws Exception {
        waitFor(client.close());
        client = vertx.createHttpClient(new HttpClientOptions().setProtocolVersion(HttpVersion.HTTP_2)
                .setHttp2ClearTextUpgrade(false));
        backendHandler = request -> {
            assertEquals(HttpVersion.HTTP_1_1, request.version());
            request.response().setChunked(true).putHeader("Trailer", "x-checksum")
                    .putTrailer("x-checksum", "1234").end("data");
        };
        HttpClientResponse response = waitForResponse(client.request(HttpMethod.GET, gateway.port(), "127.0.0.1", "/data")
                .compose(request -> request.authority(HostAndPort.create("orders.test", -1)).send()));
        assertEquals(HttpVersion.HTTP_2, response.version());
        assertEquals("data", waitFor(readBody(response)).toString());
        assertEquals("1234", response.getTrailer("x-checksum"));
    }

    @Test
    void http2UpgradeAndLargeBinaryFramesAreRelayed() throws Exception {
        useHttp2();
        waitFor(client.close());
        client = vertx.createHttpClient(new HttpClientOptions().setProtocolVersion(HttpVersion.HTTP_2)
                .setHttp2ClearTextUpgrade(true));
        byte[] payload = new byte[1024 * 1024];
        for (int index = 0; index < payload.length; index++) {
            payload[index] = (byte) index;
        }
        Buffer frame = Buffer.buffer().appendByte((byte) 0).appendInt(payload.length).appendBytes(payload);
        backendHandler = request -> request.bodyHandler(body -> request.response()
                .putHeader("Content-Type", "application/grpc").putTrailer("grpc-status", "0").end(body));
        HttpClientResponse response = waitForResponse(grpcRequest("/test.Echo/Unary").compose(request -> request.send(frame)));
        assertEquals(HttpVersion.HTTP_2, response.version());
        assertEquals(frame, waitFor(readBody(response)));
        assertEquals("0", response.getTrailer("grpc-status"));
    }

    private void useHttp2() throws Exception {
        gateway.close();
        waitFor(client.close());
        client = vertx.createHttpClient(new HttpClientOptions().setProtocolVersion(HttpVersion.HTTP_2)
                .setHttp2ClearTextUpgrade(false).setHttp2MaxPoolSize(1));
        WorkloadDefinition base = OnDemandTestSupport.definition("orders", runtime.backend, List.of());
        WorkloadDefinition definition = new WorkloadDefinition(base.id(), base.enabled(), base.accountId(), base.region(),
                base.host(), base.type(), base.target(), base.namespace(), base.replicas(), base.backendUrl(), base.backendPort(),
                base.idleTimeout(), base.startupTimeout(), base.queues(), base.healthRequests(), base.sleepingHealth(),
                base.readinessPath(), base.readinessStatus(), base.clusterName(), BackendProtocol.HTTP2);
        gateway = new OnDemandGateway(vertx, List.of(definition), () -> coordinator);
        waitFor(gateway.start("127.0.0.1", 0));
    }

    private Future<HttpClientRequest> grpcRequest(String path) {
        return client.request(HttpMethod.POST, gateway.port(), "127.0.0.1", path)
                .map(request -> request.authority(HostAndPort.create("orders.test", -1)).setChunked(true)
                        .putHeader("Content-Type", "application/grpc").putHeader("TE", "trailers"));
    }

    private void awaitSleeping() {
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            time.addAndGet(TimeUnit.SECONDS.toNanos(121));
            coordinator.tick("orders");
            assertEquals(State.SLEEPING, coordinator.state("orders"));
        });
    }

    private Future<Reply> send(HttpMethod method, String path) {
        return client.request(method, gateway.port(), "127.0.0.1", path)
                .compose(request -> request.putHeader("Host", "orders.test").send()).compose(this::readReply);
    }

    private Future<Reply> readReply(HttpClientResponse response) {
        return readBody(response).map(body -> new Reply(response.statusCode(), response.headers(), body));
    }

    private static HttpClientResponse waitForResponse(Future<HttpClientResponse> future) throws Exception {
        return waitFor(future.map(HttpClientResponse::pause));
    }

    private static Future<Buffer> readBody(HttpClientResponse response) {
        Future<Buffer> body = response.body();
        response.resume();
        return body;
    }

    private static <T> T waitFor(Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private record Reply(int status, MultiMap headers, Buffer body) {}
}
