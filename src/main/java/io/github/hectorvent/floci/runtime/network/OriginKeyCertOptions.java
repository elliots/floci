package io.github.hectorvent.floci.runtime.network;

import io.github.hectorvent.floci.config.FlociCertificateAuthority;
import io.github.hectorvent.floci.services.acm.CertificateGenerator.GeneratedCertificate;
import io.github.hectorvent.floci.services.acm.model.KeyAlgorithm;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.KeyCertOptions;
import io.vertx.core.net.PemKeyCertOptions;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.X509KeyManager;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * The gateway's certificates: the one issued at start for the origins it names, and, for a name only a wildcard origin
 * covers, one issued on the first handshake that asks for it (by SNI). A certificate's own wildcard covers one label, and
 * an origin's covers any number, so each such name gets a certificate of its own.
 */
final class OriginKeyCertOptions implements KeyCertOptions {
    private final PemKeyCertOptions fixed;
    private final NetworkDefinition network;
    private final FlociCertificateAuthority authority;
    private final Map<String, KeyManagerFactory> issued;

    OriginKeyCertOptions(PemKeyCertOptions fixed, NetworkDefinition network, FlociCertificateAuthority authority) {
        this(fixed, network, authority, new ConcurrentHashMap<>());
    }

    private OriginKeyCertOptions(PemKeyCertOptions fixed, NetworkDefinition network, FlociCertificateAuthority authority,
                                 Map<String, KeyManagerFactory> issued) {
        this.fixed = fixed;
        this.network = network;
        this.authority = authority;
        this.issued = issued;
    }

    @Override
    public KeyCertOptions copy() {
        return new OriginKeyCertOptions(fixed, network, authority, issued);
    }

    @Override
    public KeyManagerFactory getKeyManagerFactory(Vertx vertx) throws Exception {
        return fixed.getKeyManagerFactory(vertx);
    }

    @Override
    public Function<String, KeyManagerFactory> keyManagerFactoryMapper(Vertx vertx) {
        // null keeps the certificate issued at start
        return serverName -> serverName == null || !network.coveredByWildcard(serverName) ? null
                : issued.computeIfAbsent(serverName.toLowerCase(Locale.ROOT), name -> issue(vertx, name));
    }

    @Override
    public Function<String, X509KeyManager> keyManagerMapper(Vertx vertx) {
        Function<String, KeyManagerFactory> factories = keyManagerFactoryMapper(vertx);
        return serverName -> {
            KeyManagerFactory factory = factories.apply(serverName);
            return factory == null ? null : (X509KeyManager) factory.getKeyManagers()[0];
        };
    }

    private KeyManagerFactory issue(Vertx vertx, String name) {
        GeneratedCertificate certificate = authority.issueServerCertificate(name, List.of(name), KeyAlgorithm.RSA_2048, null);
        try {
            return new PemKeyCertOptions().setCertValue(Buffer.buffer(certificate.certificatePem()))
                    .setKeyValue(Buffer.buffer(certificate.privateKeyPem())).getKeyManagerFactory(vertx);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot issue a certificate for " + name, e);
        }
    }
}
