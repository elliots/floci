package io.github.hectorvent.floci.runtime.network;

import io.github.hectorvent.floci.config.ContainerCaBundle;
import io.github.hectorvent.floci.config.FlociCertificateAuthority;
import io.github.hectorvent.floci.core.common.Digests;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.bouncycastle.asn1.ASN1Encoding;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.DERBMPString;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.pkcs.PKCS12PfxPduBuilder;
import org.bouncycastle.pkcs.PKCS12SafeBagBuilder;
import org.bouncycastle.pkcs.bc.BcPKCS12MacCalculatorBuilder;
import org.bouncycastle.pkcs.jcajce.JcaPKCS12SafeBagBuilder;

import java.io.ByteArrayOutputStream;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;

/**
 * What a smolvm-pods node writes into each container so that it trusts Floci's CA: the CA in
 * PEM, a bundle of the JVM's public roots and the CA for a root file system without one, and
 * Java trust stores holding the same roots, in PKCS12 that the JVM reads without a password and
 * in JKS with {@code changeit}.
 */
@ApplicationScoped
public class NetworkTrustMaterial {

    /** The attribute that marks a PKCS12 certificate as a trust anchor for the JDK. */
    private static final ASN1ObjectIdentifier ORACLE_TRUSTED_KEY_USAGE =
            new ASN1ObjectIdentifier("2.16.840.1.113894.746875.1.1");
    private static final char[] JAVA_STORE_PASSWORD = "changeit".toCharArray();

    /** The material, in the profile's form. */
    public record Material(String ca, String caSha256, String bundle, String pkcs12, String jks) {
    }

    private final Instance<FlociCertificateAuthority> authority;
    private volatile Material material;

    @Inject
    public NetworkTrustMaterial(Instance<FlociCertificateAuthority> authority) {
        this.authority = authority;
    }

    /** The CA that signs the route intermediates. */
    public FlociCertificateAuthority authority() {
        return authority.get();
    }

    /** The material for Floci's CA, built once. */
    public Material material() {
        Material current = material;
        if (current == null) {
            synchronized (this) {
                current = material;
                if (current == null) {
                    current = build(authority.get());
                    material = current;
                }
            }
        }
        return current;
    }

    static Material build(FlociCertificateAuthority ca) {
        try {
            X509Certificate anchor = ca.certificate();
            List<X509Certificate> roots = new ArrayList<>(ContainerCaBundle.systemRoots());
            roots.add(anchor);
            StringBuilder bundle = new StringBuilder();
            for (X509Certificate root : roots) {
                bundle.append(ContainerCaBundle.toPem(root));
            }
            Base64.Encoder base64 = Base64.getEncoder();
            return new Material(ca.caPem(), HexFormat.of().formatHex(Digests.sha256(anchor.getEncoded())),
                    bundle.toString(), base64.encodeToString(pkcs12(roots)), base64.encodeToString(jks(roots)));
        } catch (Exception e) {
            throw new IllegalStateException("Cannot build the trust material for containers: " + e.getMessage(), e);
        }
    }

    /** A PKCS12 store of trusted certificates, unencrypted, which the JDK reads with no password. */
    static byte[] pkcs12(List<X509Certificate> roots) throws Exception {
        PKCS12PfxPduBuilder pfx = new PKCS12PfxPduBuilder();
        int n = 0;
        for (X509Certificate root : roots) {
            PKCS12SafeBagBuilder bag = new JcaPKCS12SafeBagBuilder(root);
            bag.addBagAttribute(PKCSObjectIdentifiers.pkcs_9_at_friendlyName, new DERBMPString(alias(root, n++)));
            bag.addBagAttribute(ORACLE_TRUSTED_KEY_USAGE, KeyPurposeId.anyExtendedKeyUsage.toOID());
            pfx.addData(bag.build());
        }
        return pfx.build(new BcPKCS12MacCalculatorBuilder(), JAVA_STORE_PASSWORD).getEncoded(ASN1Encoding.DER);
    }

    static byte[] jks(List<X509Certificate> roots) throws Exception {
        KeyStore store = KeyStore.getInstance("JKS");
        store.load(null, JAVA_STORE_PASSWORD);
        int n = 0;
        for (X509Certificate root : roots) {
            store.setCertificateEntry(alias(root, n++), root);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        store.store(out, JAVA_STORE_PASSWORD);
        return out.toByteArray();
    }

    private static String alias(X509Certificate root, int n) {
        return "floci-" + n + "-" + Integer.toHexString(root.getSubjectX500Principal().getName().hashCode());
    }
}
