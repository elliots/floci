package io.github.hectorvent.floci.services.rds.proxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.config.CertificateMetadata;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.FlociCertificateAuthority;
import io.github.hectorvent.floci.config.FlociVersion;
import io.github.hectorvent.floci.core.common.Pem;
import io.github.hectorvent.floci.services.acm.CertificateGenerator;
import io.github.hectorvent.floci.services.acm.model.KeyAlgorithm;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Owns the RDS auth proxy's in-memory server certificate signed by Floci's shared root CA.
 * The server certificate's SANs cover every advertised Postgres/MySQL host and the names
 * configured in {@code services.rds.tls-hostnames}. The bean is created at startup, so a
 * malformed configured name stops Floci from starting.
 *
 * <p>The shared CA persists at {@code {persistent-path}/tls/floci-root-ca.{crt,key}}. The
 * {@code rds-ca.crt} compatibility copy preserves existing client paths, and
 * {@code rds-ca.metadata.json} records the current SAN list. A client trusts the shared CA once
 * (e.g. {@code sslmode=verify-full sslrootcert=<path>}) to keep working across every local
 * database and every restart, including when advertised hosts change.
 */
@ApplicationScoped
@Startup
public class RdsProxyTlsCertificates {

    private static final Logger LOG = Logger.getLogger(RdsProxyTlsCertificates.class);

    private static final List<String> DEFAULT_SANS = List.of("localhost", "127.0.0.1");
    private static final String METADATA_NAME = "rds-ca.metadata.json";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Pattern DNS_NAME = Pattern.compile("[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?");

    private final FlociCertificateAuthority certificateAuthority;
    private final CertificateGenerator certificateGenerator;
    private final Set<String> configuredSans;
    private final Clock clock;
    private final Object lock = new Object();

    private volatile ServerCertificate current;

    private record ServerCertificate(SSLContext context, Set<String> sans, Instant notAfter) {
    }

    @Inject
    public RdsProxyTlsCertificates(FlociCertificateAuthority certificateAuthority, CertificateGenerator certificateGenerator,
                                   EmulatorConfig config) {
        this(certificateAuthority, certificateGenerator, config.services().rds().tlsHostnames().orElse(List.of()),
                Clock.systemUTC());
    }

    RdsProxyTlsCertificates(FlociCertificateAuthority certificateAuthority, CertificateGenerator certificateGenerator,
                            List<String> configuredHostnames, Clock clock) {
        this.certificateAuthority = certificateAuthority;
        this.certificateGenerator = certificateGenerator;
        this.configuredSans = configuredSans(configuredHostnames);
        this.clock = clock;
    }

    /**
     * Ensures {@code host} is covered by the server certificate, issuing a new leaf when needed.
     * The persisted CA is unchanged, so existing client trust stores continue to work.
     */
    public void ensureHost(String host) {
        if (host == null || host.isBlank()) {
            return;
        }
        ensureInitialized();
        if (current.sans().contains(host)) {
            return;
        }
        synchronized (lock) {
            if (current.sans().contains(host)) {
                return;
            }
            Set<String> updated = new LinkedHashSet<>(current.sans());
            updated.add(host);
            regenerate(updated);
        }
    }

    /**
     * Returns the current TLS server context. Handlers should call this per-handshake rather than
     * caching it themselves; {@link #ensureHost(String)} can swap it out at any time.
     */
    public SSLContext sslContext() {
        ensureInitialized();
        return current.context();
    }

    private void ensureInitialized() {
        if (!needsRenewal()) {
            return;
        }
        synchronized (lock) {
            if (!needsRenewal()) {
                return;
            }
            if (current != null) {
                regenerate(current.sans());
                return;
            }
            Path tlsDir = tlsDir();
            Set<String> sans = new LinkedHashSet<>(DEFAULT_SANS);
            Path metadataFile = tlsDir.resolve(METADATA_NAME);
            if (Files.exists(metadataFile)) {
                try {
                    sans.addAll(readMetadata(metadataFile));
                } catch (IOException e) {
                    LOG.warnv(e, "RDS proxy TLS: invalid SAN metadata at {0}; keeping the CA and using default hosts",
                            metadataFile);
                }
            }
            sans.addAll(configuredSans);
            refreshLegacyCaFiles();
            regenerate(sans);
            logTrustHint(certificateAuthority.certificatePath());
        }
    }

    /** The configured names, validated before anything is written. */
    private static Set<String> configuredSans(List<String> configuredHostnames) {
        Set<String> names = new LinkedHashSet<>();
        for (String configured : configuredHostnames) {
            String name = configured.trim().toLowerCase(Locale.ROOT);
            if (name.length() > 253 || !DNS_NAME.matcher(name).matches() || name.contains("..")) {
                throw new IllegalArgumentException("Invalid RDS TLS hostname '" + configured
                        + "': use a DNS name without a scheme or port");
            }
            names.add(name);
        }
        return names;
    }

    private void refreshLegacyCaFiles() {
        Path temporary = null;
        try {
            Path tlsDir = tlsDir();
            temporary = Files.createTempFile(tlsDir, "rds-ca-", ".tmp");
            Files.writeString(temporary, certificateAuthority.caPem());
            // Replace the path itself, never a legacy symlink's target or a shared inode.
            Files.move(temporary, tlsDir.resolve("rds-ca.crt"),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            Files.deleteIfExists(tlsDir.resolve("rds-ca.key"));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to refresh RDS proxy TLS CA compatibility files", e);
        } finally {
            if (temporary != null) {
                try {
                    Files.deleteIfExists(temporary);
                } catch (IOException e) {
                    LOG.warnv(e, "RDS proxy TLS: could not remove temporary CA file {0}", temporary);
                }
            }
        }
    }

    private void regenerate(Set<String> sans) {
        try {
            Path tlsDir = tlsDir();
            List<String> sanList = new ArrayList<>(sans);
            Instant now = clock.instant();
            certificateAuthority.certificate().checkValidity(Date.from(now));
            Instant notAfter = now.plus(365, ChronoUnit.DAYS);
            if (certificateAuthority.certificate().getNotAfter().toInstant().isBefore(notAfter)) {
                notAfter = certificateAuthority.certificate().getNotAfter().toInstant();
            }
            CertificateGenerator.GeneratedCertificate generated = certificateGenerator.generateIssuedCertificate(
                    "localhost", sanList, KeyAlgorithm.RSA_2048, null, certificateAuthority.issuer(),
                    CertificateGenerator.LeafUsage.SERVER, notAfter);
            X509Certificate certificate = Pem.parseCertificate(generated.certificatePem());
            PrivateKey privateKey = Pem.parsePrivateKey(generated.privateKeyPem());
            SSLContext context = certificateAuthority.serverSslContext(certificate, privateKey);
            writeMetadata(tlsDir.resolve(METADATA_NAME), sanList);
            current = new ServerCertificate(context, Set.copyOf(sans), generated.notAfter());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to generate RDS proxy TLS certificate", e);
        }
    }

    private boolean needsRenewal() {
        ServerCertificate certificate = current;
        if (certificate == null) {
            return true;
        }
        Instant renewAt = certificate.notAfter();
        // A leaf capped at CA expiry cannot be extended without replacing the trust anchor.
        if (renewAt.isBefore(certificateAuthority.certificate().getNotAfter().toInstant())) {
            renewAt = renewAt.minusSeconds(60);
        }
        return !renewAt.isAfter(clock.instant());
    }

    private Path tlsDir() {
        return certificateAuthority.certificatePath().getParent();
    }

    private void logTrustHint(Path certFile) {
        LOG.infov("RDS proxy TLS: for sslmode=verify-full set PGSSLROOTCERT={0}",
                certFile.toAbsolutePath());
    }

    private List<String> readMetadata(Path metadataFile) throws IOException {
        CertificateMetadata metadata = OBJECT_MAPPER.readValue(metadataFile.toFile(), CertificateMetadata.class);
        List<String> hostnames = metadata != null ? metadata.getHostnames() : null;
        if (hostnames == null || hostnames.isEmpty()
                || hostnames.stream().anyMatch(host -> host == null || host.isBlank())) {
            throw new IOException("RDS proxy TLS metadata file has invalid hostnames: " + metadataFile);
        }
        return hostnames;
    }

    private void writeMetadata(Path metadataFile, List<String> sans) throws IOException {
        CertificateMetadata metadata = CertificateMetadata.create(sans, FlociVersion.current());
        String json = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(metadata);
        Files.writeString(metadataFile, json);
    }
}
