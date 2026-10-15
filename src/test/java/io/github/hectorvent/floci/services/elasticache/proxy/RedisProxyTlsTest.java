package io.github.hectorvent.floci.services.elasticache.proxy;

import io.github.hectorvent.floci.config.FlociCertificateAuthority;
import io.github.hectorvent.floci.services.elasticache.model.AuthMode;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class RedisProxyTlsTest {
    private static final byte[] PING = "*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.UTF_8);

    @TempDir
    Path tempDir;

    private static final String REAL_ENDPOINT = "master.connect-cluster-feast-redis.vlooa2.apse2.cache.amazonaws.com";

    private volatile Set<String> hostnames = Set.of();
    private FlociCertificateAuthority authority;
    private ServerSocket backend;
    private ElastiCacheAuthProxy proxy;
    private int proxyPort;

    @BeforeAll
    static void bouncyCastle() {
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    @BeforeEach
    void start() throws Exception {
        authority = FlociCertificateAuthority.loadOrCreate(tempDir);
        // a Redis that answers PONG to anything
        backend = new ServerSocket(0);
        Thread.ofVirtual().start(() -> {
            while (!backend.isClosed()) {
                try {
                    Socket s = backend.accept();
                    Thread.ofVirtual().start(() -> {
                        try (s) {
                            s.getInputStream().read(new byte[64]);
                            s.getOutputStream().write("+PONG\r\n".getBytes(StandardCharsets.UTF_8));
                            s.getOutputStream().flush();
                            s.getInputStream().read();
                        } catch (Exception ignored) {
                        }
                    });
                } catch (Exception ignored) {
                }
            }
        });
        try (ServerSocket free = new ServerSocket(0)) {
            proxyPort = free.getLocalPort();
        }
        proxy = new ElastiCacheAuthProxy("grp", AuthMode.NO_AUTH, "127.0.0.1", backend.getLocalPort(),
                (username, password) -> true, mock(SigV4Validator.class));
        proxy.serveTls(new RedisTlsContext(authority, List.of("cache.amazonaws.com"), "floci", () -> hostnames).context());
        proxy.start(proxyPort);
    }

    @AfterEach
    void stop() throws Exception {
        proxy.stop();
        backend.close();
    }

    private SSLContext trustingTheAuthority() throws Exception {
        X509Certificate ca = (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(authority.caPem().getBytes(StandardCharsets.UTF_8)));
        KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
        store.load(null);
        store.setCertificateEntry("floci", ca);
        TrustManagerFactory tm = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tm.init(store);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, tm.getTrustManagers(), null);
        return context;
    }

    private String ping(Socket s) throws Exception {
        s.getOutputStream().write(PING);
        s.getOutputStream().flush();
        InputStream in = s.getInputStream();
        byte[] buf = new byte[7];
        int n = in.readNBytes(buf, 0, buf.length);
        return new String(buf, 0, n, StandardCharsets.UTF_8);
    }

    private SSLSocket tlsClient(String serverName) throws Exception {
        SSLSocket s = (SSLSocket) trustingTheAuthority().getSocketFactory().createSocket("127.0.0.1", proxyPort);
        SSLParameters params = s.getSSLParameters();
        params.setServerNames(List.of(new SNIHostName(serverName)));
        params.setEndpointIdentificationAlgorithm("HTTPS");
        s.setSSLParameters(params);
        s.setSoTimeout(5000);
        return s;
    }

    @Test
    void aClientOfAnElastiCacheEndpointNameVerifiesTheCertificateAndIsBridged() throws Exception {
        try (SSLSocket s = tlsClient("master.group.abc123.apse2.cache.amazonaws.com")) {
            assertEquals("+PONG\r\n", ping(s));
        }
    }

    @Test
    void aNameOutsideTheConfiguredDomainsDoesNotVerify() throws Exception {
        try (SSLSocket s = tlsClient("redis.example.com")) {
            assertThrows(Exception.class, () -> ping(s));
        }
    }

    @Test
    void anEndpointNameLongerThanASubjectAllowsStillGetsACertificate() throws Exception {
        try (SSLSocket s = tlsClient(REAL_ENDPOINT)) {
            assertEquals("+PONG\r\n", ping(s));
        }
    }

    @Test
    void aClientThatSendsNoServerNameGetsTheNamesTheGroupAnswersTo() throws Exception {
        hostnames = Set.of(REAL_ENDPOINT);
        SSLSocket s = (SSLSocket) trustingTheAuthority().getSocketFactory().createSocket("127.0.0.1", proxyPort);
        SSLParameters params = s.getSSLParameters();
        params.setServerNames(List.of());
        params.setEndpointIdentificationAlgorithm(null);
        s.setSSLParameters(params);
        s.setSoTimeout(5000);
        try (s) {
            s.startHandshake();
            X509Certificate cert = (X509Certificate) s.getSession().getPeerCertificates()[0];
            Set<String> names = new HashSet<>();
            for (List<?> entry : cert.getSubjectAlternativeNames()) {
                names.add(String.valueOf(entry.get(1)));
            }
            assertEquals(Set.of("floci", REAL_ENDPOINT), names);
            assertEquals("+PONG\r\n", ping(s));
        }
    }

    @Test
    void aPlainClientIsStillServed() throws Exception {
        try (Socket s = new Socket("127.0.0.1", proxyPort)) {
            s.setSoTimeout(5000);
            assertEquals("+PONG\r\n", ping(s));
        }
    }
}
