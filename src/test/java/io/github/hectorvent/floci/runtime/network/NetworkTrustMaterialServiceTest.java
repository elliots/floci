package io.github.hectorvent.floci.runtime.network;

import io.github.hectorvent.floci.config.FlociCertificateAuthority;
import io.github.hectorvent.floci.config.FlociCertificateAuthority.RouteIssuer;
import io.github.hectorvent.floci.core.common.Pem;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.sec.ECPrivateKey;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.math.BigInteger;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.CertPath;
import java.security.cert.CertPathValidator;
import java.security.cert.CertPathValidatorException;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NetworkTrustMaterialServiceTest {

    @TempDir
    Path dir;

    @Test
    void theJavaStoresHoldTheCaAsATrustAnchor() throws Exception {
        FlociCertificateAuthority ca = FlociCertificateAuthority.loadOrCreate(dir.resolve("tls"));
        NetworkTrustMaterial.Material material = NetworkTrustMaterial.build(ca);

        KeyStore pkcs12 = KeyStore.getInstance("PKCS12");
        pkcs12.load(new ByteArrayInputStream(Base64.getDecoder().decode(material.pkcs12())), null);
        assertThat(pkcs12.getCertificateAlias(ca.certificate()) != null, is(true));
        assertThat(pkcs12.size(), greaterThan(10));

        KeyStore jks = KeyStore.getInstance("JKS");
        jks.load(new ByteArrayInputStream(Base64.getDecoder().decode(material.jks())), "changeit".toCharArray());
        String alias = jks.getCertificateAlias(ca.certificate());
        assertThat(jks.isCertificateEntry(alias), is(true));
        assertThat(jks.size(), is(pkcs12.size()));

        assertThat(material.bundle().endsWith(ca.caPem()) || material.bundle().contains(ca.caPem().trim()), is(true));
        assertThat(material.caSha256().length(), is(64));
    }

    @Test
    void theRouteIntermediateSignsOnlyItsNames() throws Exception {
        FlociCertificateAuthority ca = FlociCertificateAuthority.loadOrCreate(dir.resolve("tls"));
        RouteIssuer issuer = ca.issueConstrainedIntermediate("Floci route CA", List.of("api.other.test", "vendor.test"),
                Instant.now().plus(Duration.ofDays(90)));
        X509Certificate intermediate = Pem.parseCertificate(issuer.certificatePem());
        PrivateKey key = Pem.parsePrivateKey(issuer.keyPem());

        assertThat(intermediate.getBasicConstraints(), is(0));
        assertThat(intermediate.getCriticalExtensionOIDs().contains(Extension.nameConstraints.getId()), is(true));
        assertThat(issuer.keyId().length(), is(40));
        String der = issuer.keyPem().replaceAll("-----[A-Z ]+-----|\\s", "");
        ECPrivateKey inner = ECPrivateKey.getInstance(
                PrivateKeyInfo.getInstance(Base64.getDecoder().decode(der)).parsePrivateKey());
        assertThat("the key carries its public key, which ring requires", inner.getPublicKey() != null, is(true));

        validate(ca, intermediate, leaf(intermediate, key, "pay.vendor.test"));
        validate(ca, intermediate, leaf(intermediate, key, "x.y.vendor.test"));
        validate(ca, intermediate, leaf(intermediate, key, "api.other.test"));
        assertThrows(CertPathValidatorException.class,
                () -> validate(ca, intermediate, leaf(intermediate, key, "bank.example")));
        assertThrows(CertPathValidatorException.class,
                () -> validate(ca, intermediate, leaf(intermediate, key, "other.test")));
    }

    private static X509Certificate leaf(X509Certificate issuer, PrivateKey issuerKey, String name) throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        KeyPair pair = generator.generateKeyPair();
        JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                X500Name.getInstance(issuer.getSubjectX500Principal().getEncoded()), BigInteger.TWO,
                Date.from(Instant.now().minus(Duration.ofHours(1))), Date.from(Instant.now().plus(Duration.ofDays(1))),
                new X500Name("CN=" + name), pair.getPublic());
        builder.addExtension(Extension.subjectAlternativeName, false,
                new GeneralNames(new GeneralName(GeneralName.dNSName, name)));
        return new JcaX509CertificateConverter()
                .getCertificate(builder.build(new JcaContentSignerBuilder("SHA256withECDSA").build(issuerKey)));
    }

    private static void validate(FlociCertificateAuthority ca, X509Certificate intermediate, X509Certificate leaf)
            throws Exception {
        CertPath path = CertificateFactory.getInstance("X.509").generateCertPath(List.of(leaf, intermediate));
        PKIXParameters parameters = new PKIXParameters(Set.of(new TrustAnchor(ca.certificate(), null)));
        parameters.setRevocationEnabled(false);
        CertPathValidator.getInstance("PKIX").validate(path, parameters);
    }
}
