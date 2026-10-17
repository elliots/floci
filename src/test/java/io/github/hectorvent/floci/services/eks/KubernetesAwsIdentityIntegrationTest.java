package io.github.hectorvent.floci.services.eks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.services.eks.model.Cluster;
import io.github.hectorvent.floci.services.eks.model.ClusterIdentity;
import io.github.hectorvent.floci.services.eks.model.ClusterOidcKey;
import io.github.hectorvent.floci.services.eks.model.ClusterStatus;
import io.github.hectorvent.floci.services.eks.model.CreateAddonRequest;
import io.github.hectorvent.floci.services.eks.model.CreatePodIdentityAssociationRequest;
import io.github.hectorvent.floci.services.eks.model.OidcIdentity;
import io.github.hectorvent.floci.services.iam.IamService;
import io.github.hectorvent.floci.services.ssm.SsmService;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.math.BigInteger;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyFactory;
import java.security.spec.RSAPublicKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Serves real AWS identity APIs while one shared probe exercises both Kubernetes worker backends. */
@QuarkusTest
@TestProfile(KubernetesAwsIdentityIntegrationTest.Profile.class)
@EnabledIfEnvironmentVariable(named = "FLOCI_KUBERNETES_AWS_IDENTITY_TEST", matches = "true")
class KubernetesAwsIdentityIntegrationTest {
    public static final class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("floci.services.iam.enforcement-enabled", "true", "quarkus.http.host", "0.0.0.0",
                    "quarkus.test.hang-detection-timeout", "21m");
        }
    }

    private static final String ACCOUNT = "000000000000";
    private static final String REGION = "us-east-1";

    @Inject
    ObjectMapper mapper;
    @Inject
    EksService clusters;
    @Inject
    EksOidcService oidc;
    @Inject
    EksAddonService addons;
    @Inject
    EksPodIdentityAssociationService associations;
    @Inject
    IamService iam;
    @Inject
    SsmService ssm;
    @TestHTTPResource
    URI endpoint;

    @Test
    @Timeout(value = 20, unit = TimeUnit.MINUTES)
    void sharedSdkCredentialProvidersAuthenticateAcrossWorkerBackendsAndNativeSleep() throws Exception {
        Path directory = Path.of(System.getenv("FLOCI_KUBERNETES_AWS_IDENTITY_DIRECTORY"));
        JsonNode fixture = mapper.readTree(directory.resolve("fixture.json").toFile());
        String name = fixture.path("cluster").asText();
        String issuer = fixture.path("issuer").asText();
        String irsa = name + "-irsa";
        String podIdentity = name + "-pod";
        String allowed = "/" + name + "/allowed";
        String denied = "/" + name + "/denied";
        Cluster cluster = new Cluster();
        cluster.setName(name);
        cluster.setAccountId(ACCOUNT);
        cluster.setArn("arn:aws:eks:" + REGION + ":" + ACCOUNT + ":cluster/" + name);
        cluster.setStatus(ClusterStatus.ACTIVE);
        cluster.setCreatedAt(Instant.now());
        cluster.setContainerId(fixture.path("peer").asText());
        cluster.setIdentity(new ClusterIdentity(new OidcIdentity(issuer)));
        clusters.putClusterForAccount(ACCOUNT, cluster);
        // The disposable k3s API minted real Pod-bound tokens. Register its public
        // verification key only; never export or replace its signing private key.
        ClusterOidcKey key = oidc.ensureKeyForAccount(ACCOUNT, name, issuer);
        JsonNode jwk = fixture.path("jwk");
        RSAPublicKeySpec publicKey = new RSAPublicKeySpec(
                new BigInteger(1, Base64.getUrlDecoder().decode(jwk.path("n").asText())),
                new BigInteger(1, Base64.getUrlDecoder().decode(jwk.path("e").asText())));
        key.setPublicKey(Base64.getEncoder().encodeToString(KeyFactory.getInstance("RSA").generatePublic(publicKey).getEncoded()));
        String issuerKey = issuer.replaceFirst("^https://", "");
        String irsaTrust = """
                {"Version":"2012-10-17","Statement":[{"Effect":"Allow",
                "Principal":{"Federated":"arn:aws:iam::%s:oidc-provider/%s"},
                "Action":"sts:AssumeRoleWithWebIdentity","Condition":{"StringEquals":{
                "%s:sub":"system:serviceaccount:default:aws-irsa","%s:aud":"sts.amazonaws.com"}}}]}
                """.formatted(ACCOUNT, issuerKey, issuerKey, issuerKey);
        String podTrust = """
                {"Version":"2012-10-17","Statement":[{"Effect":"Allow",
                "Principal":{"Service":"pods.eks.amazonaws.com"},
                "Action":["sts:AssumeRole","sts:TagSession"]}]}
                """;
        String policy = """
                {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"ssm:GetParameter",
                "Resource":"arn:aws:ssm:%s:%s:parameter%s"}]}
                """.formatted(REGION, ACCOUNT, allowed);
        try {
            iam.createRole(irsa, "/", irsaTrust, null, 3600, null);
            iam.createRole(podIdentity, "/", podTrust, null, 3600, null);
            iam.putRolePolicy(irsa, "read-one", policy);
            iam.putRolePolicy(podIdentity, "read-one", policy);
            ssm.putParameter(allowed, "authorized", "String", null, false, REGION);
            ssm.putParameter(denied, "must-not-be-readable", "String", null, false, REGION);
            addons.create(cluster, new CreateAddonRequest("eks-pod-identity-agent", null, null,
                    null, null, null, null, null));
            associations.create(cluster, new CreatePodIdentityAssociationRequest(name, "default", "aws-pod",
                    "arn:aws:iam::" + ACCOUNT + ":role/" + podIdentity, null, null, null, null, null));
            Map<String, Object> ready = Map.of("endpoint", "http://" + fixture.path("hostIP").asText() + ":" + endpoint.getPort(),
                    "localEndpoint", endpoint.toString(), "irsaRole", "arn:aws:iam::" + ACCOUNT + ":role/" + irsa,
                    "podRole", "arn:aws:iam::" + ACCOUNT + ":role/" + podIdentity,
                    "allowed", allowed, "denied", denied);
            Path pending = directory.resolve("ready.pending");
            mapper.writeValue(pending.toFile(), ready);
            Files.move(pending, directory.resolve("ready.json"), StandardCopyOption.ATOMIC_MOVE);
            await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofMinutes(18))
                    .until(() -> Files.isRegularFile(directory.resolve("result.json")));
            JsonNode result = mapper.readTree(directory.resolve("result.json").toFile());
            assertTrue(result.path("success").asBoolean(), result.path("error").asText());
            assertEquals(4, result.path("providers").size(), "IRSA and Pod Identity must run on both worker backends");
        } finally {
            cluster.setContainerId(null);
            clusters.deleteClusterForAccount(ACCOUNT, name);
            oidc.deleteKey(name);
            for (String role : new String[]{irsa, podIdentity}) {
                if (iam.findRole(ACCOUNT, role).isPresent()) {
                    iam.deleteRolePolicy(role, "read-one");
                    iam.deleteRole(role);
                }
            }
            ssm.deleteParameters(List.of(allowed, denied), REGION);
        }
    }
}
