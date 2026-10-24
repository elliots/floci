package io.github.hectorvent.floci.services.eks;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.EksServiceConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.RuntimeConfig;
import io.github.hectorvent.floci.config.EmulatorConfig.RuntimeConfig.NodeConfig;
import io.github.hectorvent.floci.config.FlociCertificateAuthority;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.runtime.flocinode.NodeRecord;
import io.github.hectorvent.floci.runtime.flocinode.NodeSeed;
import io.github.hectorvent.floci.runtime.flocinode.NodeSettings;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.services.eks.model.ClusterOidcKey;
import io.github.hectorvent.floci.testutil.LogCapture;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.LogRecord;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** The seed of the smolvm-pods node with what EKS adds to it ({@link EksSmolvmSeedContributor}). */
class EksSmolvmSeedServiceTest {

    private static final String ACCOUNT = "123456789012";
    private static final String NODE_ID = "floci-node-0123456789abcdef";
    private static final String NODE_NAME = "i-0123456789abcdef0.ec2.internal";
    private static final String SEED_ENDPOINT = "host.docker.internal:4566";
    private static final ObjectMapper YAML = new YAMLMapper();

    private static FlociCertificateAuthority certificateAuthority;

    @TempDir
    Path dataPath;

    private EksServiceConfig eks;
    private NodeConfig node;
    private EmulatorConfig.TlsConfig tls;
    private EmulatorConfig.EcrServiceConfig ecr;
    private EksOidcService oidcService;
    private EksSmolvmSeedContributor contributor;
    private NodeSeed seed;

    @BeforeAll
    static void createCertificateAuthority(@TempDir Path tlsDir) {
        certificateAuthority = FlociCertificateAuthority.loadOrCreate(tlsDir);
    }

    @BeforeEach
    void setUp() {
        EmulatorConfig config = mock(EmulatorConfig.class);
        EmulatorConfig.ServicesConfig services = mock(EmulatorConfig.ServicesConfig.class);
        RuntimeConfig runtime = mock(RuntimeConfig.class);
        eks = mock(EksServiceConfig.class);
        node = mock(NodeConfig.class);
        tls = mock(EmulatorConfig.TlsConfig.class);
        ecr = mock(EmulatorConfig.EcrServiceConfig.class);
        when(config.services()).thenReturn(services);
        when(config.runtime()).thenReturn(runtime);
        when(runtime.node()).thenReturn(node);
        when(config.tls()).thenReturn(tls);
        when(config.port()).thenReturn(4566);
        when(config.defaultAccountId()).thenReturn("000000000000");
        when(config.defaultRegion()).thenReturn("us-east-1");
        when(services.eks()).thenReturn(eks);
        when(services.ecr()).thenReturn(ecr);
        when(node.dataPath()).thenReturn(dataPath.toString());
        when(eks.enabled()).thenReturn(true);
        when(eks.provider()).thenReturn(EksSmolvmSettings.PROVIDER_SMOLVM_PODS);
        when(eks.iamAuthWebhook()).thenReturn(true);
        when(eks.irsaSigningKey()).thenReturn(true);
        when(eks.ecrRegistryMirror()).thenReturn(true);
        when(eks.podIdentityWebhook()).thenReturn(true);
        when(ecr.enabled()).thenReturn(true);
        when(tls.enabled()).thenReturn(true);

        oidcService = new EksOidcService(new StorageFactory(null, null) {
            @Override
            public <V> AccountAwareStorageBackend<V> create(String serviceName, String fileName,
                    TypeReference<Map<String, V>> typeReference) {
                return AccountAwareStorageBackend.inMemory("000000000000");
            }
        }, new ObjectMapper());
        NodeSettings settings = new NodeSettings(config, () -> false, () -> false, "Mac OS X", "aarch64");
        contributor = new EksSmolvmSeedContributor(config, settings, oidcService, certificateAuthority);
        seed = new NodeSeed(config, settings, List.of(contributor));
    }

    private static NodeRecord record(Map<String, List<String>> k3sArgs) {
        return new NodeRecord(NODE_ID, 6500, 6600, "32080-32099", "a".repeat(64), false, List.of(), ACCOUNT,
                "us-east-1", NODE_NAME, "172.20.0.0/16", "10.42.0.0/16", k3sArgs, List.of());
    }

    private static NodeRecord record() {
        return record(Map.of());
    }

    private Path seedDir() {
        return dataPath.resolve(NODE_ID).resolve("seed");
    }

    private String read(String name) throws IOException {
        return Files.readString(seedDir().resolve(name), StandardCharsets.UTF_8);
    }

    private static List<String> texts(JsonNode list) {
        List<String> texts = new ArrayList<>();
        list.forEach(item -> texts.add(item.asText()));
        return texts;
    }

    @Test
    void writesEverySeedFileThatFlociNodeInstalls() throws IOException {
        seed.write(record());

        try (Stream<Path> files = Files.walk(seedDir())) {
            List<String> names = files.filter(Files::isRegularFile)
                    .map(file -> seedDir().relativize(file).toString()).toList();
            assertThat(names, containsInAnyOrder("version", "env", "k3s.yaml", "token-webhook.yaml", "tokens.csv",
                    "sa-signing-key.pem", "sa-public-key.pem", "registries.yaml", "manifests/floci-admission.yaml",
                    "api-token", "hosts", "audit-policy.yaml"));
        }
        assertEquals(EksClusterManager.buildAuditPolicy(), read("audit-policy.yaml"));
        assertEquals("1\n", read("version"));
    }

    @Test
    void dropInNamesTheNodeAndConfiguresKubeletAndTheApiServerForTheNodesIssuer() throws IOException {
        seed.write(record(EksSmolvmSeedContributor.k3sArgs(List.of("--kubelet-arg=max-pods=250",
                "--kube-apiserver-arg=feature-gates=CSIStorageCapacity=true",
                "--kube-controller-manager-arg=leader-elect=false"))));

        JsonNode k3s = YAML.readTree(read("k3s.yaml"));
        assertEquals(NODE_NAME, k3s.path("node-name").asText());
        assertThat(texts(k3s.path("tls-san+")), contains("host.docker.internal"));
        assertEquals("172.20.0.0/16", k3s.path("service-cidr").asText());
        assertEquals("10.42.0.0/16", k3s.path("cluster-cidr").asText());
        assertThat(texts(k3s.path("kubelet-arg+")), contains("provider-id=aws:///us-east-1a/i-0123456789abcdef0",
                "node-labels=topology.kubernetes.io/zone=us-east-1a,topology.kubernetes.io/region=us-east-1",
                "max-pods=250"));
        String issuer = oidcService.nodeKey(ACCOUNT, NODE_ID, "us-east-1").getIssuer();
        assertThat(issuer, containsString("https://oidc.eks.us-east-1."));
        assertThat(texts(k3s.path("kube-apiserver-arg+")), contains(
                "token-auth-file=/etc/rancher/k3s/floci/tokens.csv",
                "authentication-token-webhook-config-file=/etc/rancher/k3s/floci/token-webhook.yaml",
                "authentication-token-webhook-version=v1",
                "authentication-token-webhook-cache-ttl=30s",
                "service-account-signing-key-file=/etc/rancher/k3s/floci/sa-signing-key.pem",
                "service-account-key-file=/etc/rancher/k3s/floci/sa-public-key.pem",
                "service-account-issuer=" + issuer,
                "service-account-issuer=https://kubernetes.default.svc.cluster.local",
                "api-audiences=https://kubernetes.default.svc.cluster.local,sts.amazonaws.com",
                "audit-policy-file=/etc/floci-node/seed/audit-policy.yaml",
                "audit-log-path=/var/log/kube-apiserver-audit.log",
                "audit-log-maxsize=50",
                "audit-log-maxbackup=2",
                "feature-gates=CSIStorageCapacity=true"));
        assertThat(texts(k3s.path("kube-controller-manager-arg+")), contains("leader-elect=false"));
        assertFalse(k3s.has("kube-scheduler-arg+"));
    }

    @Test
    void registriesMirrorEveryRegistryToFlociSoAClusterInAnotherAccountPullsItsEcr() throws IOException {
        seed.write(record());

        // containerd names the registry of a mirrored pull in ns, and Floci's ECR serves any account's.
        JsonNode mirrors = YAML.readTree(read("registries.yaml")).path("mirrors");
        List<String> hosts = new ArrayList<>();
        mirrors.fieldNames().forEachRemaining(hosts::add);
        assertEquals(List.of("*"), hosts);
        assertThat(texts(mirrors.path("*").path("endpoint")), contains("http://host.docker.internal:4566"));
    }

    @Test
    void webhooksCallTheRoutesOfTheNodeAtTheNodeEndpoint() throws IOException {
        seed.write(record());

        assertThat(read("token-webhook.yaml"), containsString("server: http://" + SEED_ENDPOINT
                + "/_floci/eks/smolvm-node/token-webhook\n"));
        JsonNode admission = YAML.readTree(read("manifests/floci-admission.yaml"));
        assertEquals("MutatingWebhookConfiguration", admission.path("kind").asText());
        JsonNode clientConfig = admission.path("webhooks").path(0).path("clientConfig");
        assertEquals("https://" + SEED_ENDPOINT + "/_floci/eks/smolvm-node/pod-identity-webhook",
                clientConfig.path("url").asText());
        assertEquals(certificateAuthority.caPem(), new String(Base64.getDecoder()
                .decode(clientConfig.path("caBundle").asText()), StandardCharsets.UTF_8));
    }

    @Test
    void hostsMapFlociNamesToTheGateway() throws IOException {
        seed.write(record());
        assertEquals("host.docker.internal\n", read("hosts"));

        when(node.flociEndpoint()).thenReturn(Optional.of("localhost.floci.io:4566"));
        seed.write(record());
        assertEquals("host.docker.internal\nlocalhost.floci.io\n", read("hosts"));
        assertThat(read("token-webhook.yaml"), containsString("server: http://localhost.floci.io:4566/"));

        when(node.flociEndpoint()).thenReturn(Optional.of("100.96.0.1:4566"));
        seed.write(record());
        assertEquals("host.docker.internal\n", read("hosts"));
    }

    @Test
    void seedHasNoCoreDnsConfiguration() throws IOException {
        seed.write(record());

        try (Stream<Path> files = Files.walk(seedDir())) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                assertThat(file.toString(), not(containsString("coredns")));
                assertThat(Files.readString(file).toLowerCase(Locale.ROOT), not(containsString("coredns")));
            }
        }
        try (Stream<Path> manifests = Files.list(seedDir().resolve("manifests"))) {
            assertThat(manifests.map(path -> path.getFileName().toString()).toList(), contains("floci-admission.yaml"));
        }
    }

    @Test
    void serviceAccountKeysAreTheNodesSigningKeyWhichTheClusterAdopts() throws IOException {
        seed.write(record());

        ClusterOidcKey key = oidcService.nodeKey(ACCOUNT, NODE_ID, "us-east-1");
        assertEquals(oidcService.exportSigningKeyPem(key), read("sa-signing-key.pem"));
        assertEquals(oidcService.exportPublicKeyPem(key), read("sa-public-key.pem"));
        seed.write(record());
        assertEquals(oidcService.exportSigningKeyPem(key), read("sa-signing-key.pem"), "the key is kept");

        oidcService.adoptKey(ACCOUNT, "orders", key);
        assertTrue(oidcService.findVerificationKey(key.getIssuer()).isPresent());
    }

    @Test
    void aRewriteRemovesTheFilesThatTheSeedNoLongerHas() throws IOException {
        seed.write(record());
        when(tls.enabled()).thenReturn(false);
        when(eks.irsaSigningKey()).thenReturn(false);
        when(eks.iamAuthWebhook()).thenReturn(false);
        when(eks.ecrRegistryMirror()).thenReturn(false);

        seed.write(record());

        for (String gone : List.of("manifests/floci-admission.yaml", "sa-signing-key.pem", "sa-public-key.pem",
                "token-webhook.yaml", "registries.yaml")) {
            assertFalse(Files.exists(seedDir().resolve(gone)), gone);
        }
        List<String> apiServer = texts(YAML.readTree(read("k3s.yaml")).path("kube-apiserver-arg+"));
        assertThat(apiServer, contains(Stream.concat(Stream.of("token-auth-file=/etc/rancher/k3s/floci/tokens.csv"),
                EksSmolvmSeedContributor.AUDIT_ARGS.stream()).toArray(String[]::new)));
        assertEquals("1\n", read("version"));
    }

    @Test
    void withoutTheSmolvmProviderEksAddsNothing() throws IOException {
        when(eks.provider()).thenReturn(EksSmolvmSettings.PROVIDER_K3S);

        seed.write(record());

        try (Stream<Path> files = Files.walk(seedDir())) {
            List<String> names = files.filter(Files::isRegularFile)
                    .map(file -> seedDir().relativize(file).toString()).toList();
            assertThat(names, containsInAnyOrder("version", "env", "k3s.yaml", "tokens.csv", "api-token", "hosts"));
        }
        assertEquals(List.of(), contributor.endpoints(record()));
    }

    @Test
    void podIdentityNeedsTlsForItsEndpoint() {
        assertEquals(List.of(new SmolvmNode.Endpoint("169.254.170.23", 80, 4566)), contributor.endpoints(record()));

        when(tls.enabled()).thenReturn(false);

        assertEquals(List.of(), contributor.endpoints(record()));
    }

    @Test
    void secretsNeverReachTheLog() throws IOException {
        List<LogRecord> records = LogCapture.capture(NodeSeed.class, () -> {
            seed.write(record());
            when(node.idleTimeout()).thenReturn(Optional.of("9m"));
            seed.writeEnv(NODE_ID);
            seed.write(record());
        });

        assertFalse(records.isEmpty());
        List<String> secrets = List.of(seed.adminToken(NODE_ID), seed.apiToken(NODE_ID).read(),
                read("sa-signing-key.pem").lines().skip(1).findFirst().orElseThrow());
        for (LogRecord record : records) {
            String logged = record.getMessage() + " " + Arrays.toString(record.getParameters());
            for (String secret : secrets) {
                assertThat(logged, not(containsString(secret)));
            }
        }
    }

    @Test
    void seedFilesMatchWhatFlociNodeInstalls() throws IOException {
        String script = Files.readString(Path.of("tools/smolvm-pods/node/rootfs/usr/local/bin/floci-node"));
        Matcher table = Pattern.compile("(?ms)^seed_files=\\((.*?)^\\)").matcher(script);
        assertTrue(table.find(), "floci-node has no seed_files table");
        Set<String> installed = new LinkedHashSet<>();
        Matcher entry = Pattern.compile("\"(\\S+) (\\S+) (\\d+)\"").matcher(table.group(1));
        while (entry.find()) {
            installed.add(entry.group(1) + " " + entry.group(2));
        }

        assertThat(installed, containsInAnyOrder(
                "k3s.yaml /etc/rancher/k3s/config.yaml.d/50-floci.yaml",
                "token-webhook.yaml " + NodeSeed.NODE_TOKEN_WEBHOOK,
                "tokens.csv " + NodeSeed.NODE_TOKENS,
                "sa-signing-key.pem " + NodeSeed.NODE_SIGNING_KEY,
                "sa-public-key.pem " + NodeSeed.NODE_PUBLIC_KEY,
                "registries.yaml /etc/rancher/k3s/registries.yaml",
                "manifests/floci-admission.yaml /var/lib/rancher/k3s/server/manifests/floci-admission.yaml",
                "api-token $api_token_file"));
        Set<String> sources = new LinkedHashSet<>();
        installed.forEach(line -> sources.add(line.split(" ")[0]));
        sources.add(NodeSeed.ENV_FILE);
        sources.add(NodeSeed.HOSTS_FILE);
        sources.add(NodeSeed.AUDIT_POLICY_FILE);
        assertEquals(sources, new LinkedHashSet<>(NodeSeed.FILES));
        assertThat(script, containsString("seed_dir=/etc/floci-node/seed\n"));
        assertThat(script, containsString("if [ \"$version\" != 1 ]; then"));
    }
}
