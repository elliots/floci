package io.github.hectorvent.floci.services.elasticache.proxy;

import io.github.hectorvent.floci.config.FlociCertificateAuthority;
import io.github.hectorvent.floci.core.common.Pem;
import io.github.hectorvent.floci.services.acm.CertificateGenerator.GeneratedCertificate;
import io.github.hectorvent.floci.services.acm.model.KeyAlgorithm;

import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.X509ExtendedKeyManager;

import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * The TLS side of a Redis proxy, with certificates from Floci's CA.
 *
 * <p>A client that names the host it connects to (SNI) is given a certificate for that name when it is under one of the
 * configured domains. A client that does not (Node's TLS client sends none unless it is told to) is given the default
 * certificate, which covers {@code defaultHost} and the {@code hostnames} the group was told it answers to: that is
 * how a client given an address from its own configuration, rather than the one Floci reports, can verify it. Every
 * certificate carries {@code defaultHost} as its subject, since a subject is limited to 64 characters and a real
 * endpoint name is longer; the names are in the subject alternative names, which is what clients check.
 */
public final class RedisTlsContext {
    private final SSLContext context;

    public RedisTlsContext(FlociCertificateAuthority authority, List<String> suffixes, String defaultHost,
                           Supplier<Set<String>> hostnames) {
        List<String> domains = suffixes.stream().map(s -> s.toLowerCase(Locale.ROOT)).toList();
        try {
            context = SSLContext.getInstance("TLS");
            context.init(new KeyManager[] {new Keys(authority, domains, defaultHost, hostnames)}, null, null);
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new IllegalStateException("Cannot set up TLS for the Redis proxy", e);
        }
    }

    public SSLContext context() {
        return context;
    }

    private static final class Keys extends X509ExtendedKeyManager {
        private static final String DEFAULT = "default";

        private record Issued(X509Certificate[] chain, PrivateKey key) {
        }

        private record Default(Set<String> names, Issued issued) {
        }

        private final FlociCertificateAuthority authority;
        private final List<String> domains;
        private final String defaultHost;
        private final Supplier<Set<String>> hostnames;
        private final Map<String, Issued> issued = new ConcurrentHashMap<>();
        private volatile Default current;

        Keys(FlociCertificateAuthority authority, List<String> domains, String defaultHost,
             Supplier<Set<String>> hostnames) {
            this.authority = authority;
            this.domains = domains;
            this.defaultHost = defaultHost.toLowerCase(Locale.ROOT);
            this.hostnames = hostnames;
        }

        @Override
        public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
            String name = requestedName(socket);
            return name != null && covered(name) ? name : DEFAULT;
        }

        @Override
        public X509Certificate[] getCertificateChain(String alias) {
            return issuedFor(alias).chain();
        }

        @Override
        public PrivateKey getPrivateKey(String alias) {
            return issuedFor(alias).key();
        }

        private Issued issuedFor(String alias) {
            return DEFAULT.equals(alias) ? defaultCertificate() : issued.computeIfAbsent(alias, n -> issue(List.of(n)));
        }

        /** Reissued when the names the group answers to change. */
        private synchronized Issued defaultCertificate() {
            Set<String> names = hostnames.get();
            Default have = current;
            if (have == null || !have.names().equals(names)) {
                List<String> sans = new ArrayList<>(List.of(defaultHost));
                names.stream().filter(n -> !sans.contains(n)).forEach(sans::add);
                have = new Default(Set.copyOf(names), issue(sans));
                current = have;
            }
            return have.issued();
        }

        private Issued issue(List<String> sans) {
            GeneratedCertificate cert = authority.issueServerCertificate(defaultHost, sans, KeyAlgorithm.RSA_2048, null);
            return new Issued(new X509Certificate[] {Pem.parseCertificate(cert.certificatePem())},
                    Pem.parsePrivateKey(cert.privateKeyPem()));
        }

        private boolean covered(String name) {
            for (String d : domains) {
                if (name.equals(d) || name.endsWith("." + d)) {
                    return true;
                }
            }
            return false;
        }

        private static String requestedName(Socket socket) {
            if (socket instanceof SSLSocket s && s.getHandshakeSession() instanceof ExtendedSSLSession session) {
                for (SNIServerName n : session.getRequestedServerNames()) {
                    if (n instanceof SNIHostName h) {
                        return h.getAsciiName().toLowerCase(Locale.ROOT);
                    }
                }
            }
            return null;
        }

        @Override
        public String[] getServerAliases(String keyType, Principal[] issuers) {
            return new String[] {DEFAULT};
        }

        @Override
        public String[] getClientAliases(String keyType, Principal[] issuers) {
            return null;
        }

        @Override
        public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
            return null;
        }
    }
}
