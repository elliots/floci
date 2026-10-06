package io.github.hectorvent.floci.runtime.network;

import io.github.hectorvent.floci.config.FlociCertificateAuthority;
import io.github.hectorvent.floci.core.common.http.HttpReverseProxy;
import io.github.hectorvent.floci.services.acm.CertificateGenerator.GeneratedCertificate;
import io.github.hectorvent.floci.services.acm.model.KeyAlgorithm;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.core.net.SocketAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class NetworkOriginGatewayIntegrationTest {
    @TempDir Path directory;
    private Vertx vertx;
    private HttpServer backend;
    private HttpServer gateway;
    private HttpClient trusted;
    private HttpClient untrusted;
    private HttpReverseProxy proxy;

    @BeforeEach
    void start() throws Exception {
        vertx = Vertx.vertx();
        backend = waitFor(vertx.createHttpServer().requestHandler(request -> request.bodyHandler(body ->
                request.response().putHeader("X-Backend-Host", request.host())
                        .putHeader("X-Original-Host", request.getHeader("X-Forwarded-Host") == null
                                ? request.host() : request.getHeader("X-Forwarded-Host"))
                        .setStatusCode(201).end(body)))
                .listen(0, "127.0.0.1"));
        URI destination = URI.create("http://127.0.0.1:" + backend.actualPort());
        FlociCertificateAuthority ca = FlociCertificateAuthority.loadOrCreate(directory);
        GeneratedCertificate certificate = ca.issueServerCertificate("api.vendor.test",
                List.of("api.vendor.test", "uploads.vendor.test", "cold.vendor.test", "unknown.vendor.test"),
                KeyAlgorithm.RSA_2048, null);
        trusted = vertx.createHttpClient(new HttpClientOptions().setTrustOptions(new PemTrustOptions()
                .addCertValue(Buffer.buffer(ca.caPem()))));
        untrusted = vertx.createHttpClient();
        proxy = new HttpReverseProxy(vertx, null, uri -> "127.0.0.1");
        NetworkDefinition network = new NetworkDefinition(true, List.of(
                new NetworkDefinition.Route("payments", List.of(URI.create("https://api.vendor.test:443"),
                        URI.create("https://uploads.vendor.test:443")), destination, false),
                new NetworkDefinition.Route("preserved", List.of(URI.create("https://cold.vendor.test:443")),
                        destination, true)));
        NetworkOriginRouter router = new NetworkOriginRouter(network, proxy);
        gateway = waitFor(vertx.createHttpServer(new HttpServerOptions().setSsl(true).setKeyCertOptions(new PemKeyCertOptions()
                        .setCertValue(Buffer.buffer(certificate.certificatePem())).setKeyValue(Buffer.buffer(certificate.privateKeyPem()))))
                .requestHandler(request -> router.route(request, "https", 443)).listen(0, "127.0.0.1"));
    }

    @AfterEach
    void close() throws Exception {
        if (proxy != null) {
            proxy.close();
        }
        if (vertx != null) {
            waitFor(vertx.close());
        }
    }

    @Test
    void trustedHttpsOriginsPreserveAuthorityAndBinaryBodies() throws Exception {
        byte[] body = {0, 1, (byte) 255, 2};
        for (String host : List.of("api.vendor.test", "uploads.vendor.test")) {
            Reply response = waitFor(send(trusted, host, Buffer.buffer(body)));
            assertEquals(201, response.status());
            assertEquals(host, response.host());
            assertEquals("127.0.0.1:" + backend.actualPort(), response.backendHost());
            assertArrayEquals(body, response.body().getBytes());
        }
    }

    @Test
    void clientsMustTrustTheCaAndValidateTheHostname() {
        assertThrows(ExecutionException.class, () -> waitFor(send(untrusted, "api.vendor.test", Buffer.buffer())));
        assertThrows(ExecutionException.class, () -> waitFor(send(trusted, "wrong.vendor.test", Buffer.buffer())));
    }

    @Test
    void unknownOriginsNeverFallThroughToABackend() throws Exception {
        Reply response = waitFor(send(trusted, "unknown.vendor.test", Buffer.buffer()));
        assertEquals(502, response.status());
        assertEquals("Unmapped network origin", response.body().toString());
    }

    @Test
    void preservesHostOnlyWhenRequested() throws Exception {
        Reply response = waitFor(send(trusted, "cold.vendor.test", Buffer.buffer("request")));
        assertEquals(201, response.status());
        assertEquals("cold.vendor.test", response.backendHost());
        assertEquals("request", response.body().toString());
    }

    private Future<Reply> send(HttpClient client, String host, Buffer body) {
        return client.request(new RequestOptions().setHost(host).setPort(443).setSsl(true).setMethod(HttpMethod.POST)
                        .setURI("/v1/customers?a=%2F&a=2")
                        .setServer(SocketAddress.inetSocketAddress(gateway.actualPort(), "127.0.0.1")))
                .compose(request -> request.send(body)).compose(response -> response.body()
                        .map(bytes -> new Reply(response.statusCode(), response.getHeader("X-Original-Host"),
                                response.getHeader("X-Backend-Host"), bytes)));
    }

    private record Reply(int status, String host, String backendHost, Buffer body) {}

    private static <T> T waitFor(Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
}
