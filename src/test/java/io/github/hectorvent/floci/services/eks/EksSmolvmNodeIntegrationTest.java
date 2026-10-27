package io.github.hectorvent.floci.services.eks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.runtime.flocinode.FlociNode;
import io.github.hectorvent.floci.runtime.node.NodeApiException;
import io.github.hectorvent.floci.runtime.node.NodePod;
import io.github.hectorvent.floci.runtime.node.NodeToken;
import io.github.hectorvent.floci.runtime.node.SmolvmNode;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient.Wake;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient.WakeReason;
import io.github.hectorvent.floci.runtime.node.SmolvmNodeClient.WakeRequest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterOverrideConfiguration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.services.eks.EksClient;
import software.amazon.awssdk.services.eks.model.Cluster;
import software.amazon.awssdk.services.eks.model.ClusterStatus;
import software.amazon.awssdk.services.eks.model.CreateClusterResponse;
import software.amazon.awssdk.services.eks.model.ResourceLimitExceededException;
import software.amazon.awssdk.services.eks.waiters.EksWaiter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An EKS cluster on a real smolvm-pods node: Floci, run as a child process ({@link FlociProcess}),
 * creates the node VM with {@code node-vm.sh} for the cluster, refuses a second cluster, and keeps
 * the node when the cluster goes; the test stops and starts Floci between steps, as a user would.
 * It needs an Apple silicon Mac of M3 or later, the node image of {@code node-vm.sh build}, Docker
 * with access to Docker Hub, and the AWS CLI and kubectl on the PATH, and runs only with
 * {@code FLOCI_EKS_SMOLVM_E2E=1}:
 *
 * <pre>
 * ./mvnw package -DskipTests
 * SMOLVM_SRC=/path/to/smolvm FLOCI_EKS_SMOLVM_E2E=1 ./mvnw test -Dtest=EksSmolvmNodeIntegrationTest
 * </pre>
 *
 * <p>Floci gets a resource namespace and a data directory of its own under
 * {@code target/eks-smolvm-e2e}, so the test touches only the machines, containers and volumes
 * it created, and removes them at the end.
 */
@EnabledIfEnvironmentVariable(named = "FLOCI_EKS_SMOLVM_E2E", matches = "1")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Timeout(value = 20, unit = TimeUnit.MINUTES)
class EksSmolvmNodeIntegrationTest {

    private static final Logger LOG = Logger.getLogger(EksSmolvmNodeIntegrationTest.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String ACCOUNT = "000000000000";
    private static final String REGION = "us-east-1";
    private static final String CLUSTER_ROLE = "arn:aws:iam::" + ACCOUNT + ":role/eks-e2e-cluster";
    private static final String FIRST = "eks-e2e-first";
    private static final String SECOND = "eks-e2e-second";
    private static final String NAMESPACE = "e2e";
    private static final String IRSA_ROLE = "eks-e2e-irsa";
    private static final String NETWORK_NAMESPACE = "e2e-net";

    /** aws-cli 2.35.14 for linux/arm64, the node's own architecture. */
    private static final String AWS_CLI_IMAGE =
            "amazon/aws-cli@sha256:dba20c4c5cf7a07ab4b5e311a230dba64b612526d1bfd8a15a192e399f29995b";
    /** node 24 on alpine for linux/arm64, which checks TLS with its own roots and NODE_EXTRA_CA_CERTS. */
    private static final String NODE_IMAGE =
            "node@sha256:44fce696f38ba8c987c4db89954051000bbd05ffd8d8659ba15ed4a8602e937f";
    /** busybox 1.37 for linux/arm64, whose httpd stands in for a vendor's simulator. */
    private static final String BUSYBOX_IMAGE =
            "busybox@sha256:bdf57e528e45e4433820e045b29b4597825a1c9e38353532d90a01445013f82e";
    /** alpine 3.22.1 for linux/amd64, which runs under Rosetta. */
    private static final String AMD64_IMAGE =
            "alpine@sha256:eafc1edb577d2e9b458664a15f23ea1c370214193226069eb22921169fc7e43f";

    private static final Duration ACTIVE_TIMEOUT = Duration.ofMinutes(10);
    private static final Duration COMMAND_TIMEOUT = Duration.ofMinutes(6);

    private final Path moduleDir = Path.of("").toAbsolutePath();
    private final String runId = UUID.randomUUID().toString().substring(0, 6);
    private final String resourceNamespace = "eks-e2e-" + runId;
    private final Path work = moduleDir.resolve("target/eks-smolvm-e2e").resolve(runId);
    private final Path data = work.resolve("data");
    private final Map<String, Path> kubeconfigs = new LinkedHashMap<>();
    private final List<String> pushedImages = new ArrayList<>();
    private final List<String> pulledImages = new ArrayList<>();
    private final SmolvmNodeClient nodeClient = new SmolvmNodeClient(JSON);

    private final Path runtimeFile = work.resolve("runtime.yaml");
    private Path nodeVmScript;
    private String smolvmSrc;
    private Map<String, String> flociEnv;
    private FlociProcess floci;
    private EksClient eks;
    private EksWaiter waiter;
    private String accessKeyId;
    private String secretAccessKey;
    private String gateway;
    private String awsCliUri;
    private String amd64Uri;

    @BeforeAll
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void startFloci() throws IOException {
        Files.createDirectories(work.resolve("aws"));
        Files.createDirectories(work.resolve("kube"));
        Path pods = moduleDir.resolve("tools/smolvm-pods");
        nodeVmScript = pods.resolve("scripts/node-vm.sh");
        smolvmSrc = firstSet(System.getenv("SMOLVM_SRC"), moduleDir.resolveSibling("smolvm").toString());
        assertTrue(Files.isDirectory(Path.of(smolvmSrc)),
                "No smolvm checkout at " + smolvmSrc + ": export SMOLVM_SRC");
        Path image = Path.of(firstSet(System.getenv("FLOCI_RUNTIME_NODE_IMAGE"),
                pods.resolve("target/node/node-image.tar").toString()));
        assertTrue(Files.isRegularFile(image), "No node image at " + image + ": run node-vm.sh build");

        int port = freePort();
        flociEnv = new LinkedHashMap<>();
        flociEnv.put("FLOCI_STORAGE_MODE", "persistent");
        flociEnv.put("FLOCI_STORAGE_PERSISTENT_PATH", data.toString());
        flociEnv.put("FLOCI_SERVICES_EKS_DATA_PATH", data.resolve("eks").toString());
        flociEnv.put("FLOCI_SERVICES_ECR_DATA_PATH", data.resolve("ecr").toString());
        flociEnv.put("FLOCI_DOCKER_RESOURCE_NAMESPACE", resourceNamespace);
        flociEnv.put("FLOCI_TLS_ENABLED", "true");
        flociEnv.put("FLOCI_TLS_AWS_HTTPS_PORT", "0");
        flociEnv.put("FLOCI_SERVICES_UI_ENABLED", "false");
        flociEnv.put("FLOCI_SERVICES_ECR_URI_STYLE", "path");
        flociEnv.put("FLOCI_SERVICES_EKS_PROVIDER", "smolvm-pods");
        flociEnv.put("FLOCI_RUNTIME_NODE_NODE_VM_SCRIPT", nodeVmScript.toString());
        flociEnv.put("FLOCI_RUNTIME_NODE_SMOLVM_SRC", smolvmSrc);
        flociEnv.put("FLOCI_RUNTIME_NODE_IMAGE", image.toString());
        flociEnv.put("FLOCI_RUNTIME_NODE_IMAGE_SHA256", sha256(image));
        flociEnv.put("FLOCI_RUNTIME_NODE_DATA_PATH", data.resolve("node").toString());
        flociEnv.put("FLOCI_RUNTIME_DNS_LISTEN", "127.0.0.1:" + freePort());
        Files.writeString(runtimeFile, "version: 1\n");
        flociEnv.put("FLOCI_RUNTIME_CONFIG_FILE", runtimeFile.toString());

        floci = FlociProcess.of(moduleDir, work, port);
        floci.start(flociEnv);
        eks = EksClient.builder().endpointOverride(floci.endpoint()).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder()).build();
        waiter = EksWaiter.builder().client(eks).overrideConfiguration(WaiterOverrideConfiguration.builder()
                .waitTimeout(ACTIVE_TIMEOUT).backoffStrategyV2(BackoffStrategy.fixedDelay(Duration.ofSeconds(1)))
                .build()).build();
        LOG.infov("Floci for the smolvm-pods end-to-end test: port {0}, resource namespace {1}, data {2}",
                String.valueOf(port), resourceNamespace, data);
    }

    @AfterAll
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void removeEverythingTheTestCreated() {
        if (floci != null) {
            try {
                floci.stop();
            } catch (RuntimeException e) {
                LOG.warnv("Could not stop Floci: {0}", e.getMessage());
            }
        }
        if (nodeVmScript != null) {
            tolerate(() -> {
                for (JsonNode machine : ownMachines()) {
                    String name = machine.path("name").asText();
                    LOG.infov("Deleting smolvm-pods node {0}, which the test left", name);
                    tolerate(() -> nodeVm("delete", "--name", name));
                }
            });
        }
        for (String kind : List.of("container", "volume")) {
            tolerate(() -> {
                List<String> listing = new ArrayList<>(List.of("docker", kind, "ls", "-q", "--filter",
                        "name=" + resourceNamespace));
                if ("container".equals(kind)) {
                    listing.add("-a");
                }
                for (String id : run(listing, Map.of(), null, COMMAND_TIMEOUT).stdout().strip().lines()
                        .filter(line -> !line.isBlank()).toList()) {
                    tolerate(() -> docker(kind, "rm", "-f", id));
                }
            });
        }
        for (String image : pushedImages) {
            tolerate(() -> docker("rmi", image));
        }
        for (String image : pulledImages) {
            tolerate(() -> docker("rmi", image));
        }
        if (eks != null) {
            eks.close();
        }
    }

    @Test
    @Order(1)
    void createClusterGivesARunningNodeThatKubectlReaches() {
        JsonNode user = json(aws("iam", "create-user", "--user-name", "eks-e2e"), "/User");
        assertEquals("eks-e2e", user.path("UserName").asText());
        JsonNode key = json(aws("iam", "create-access-key", "--user-name", "eks-e2e"), "/AccessKey");
        accessKeyId = key.path("AccessKeyId").asText();
        secretAccessKey = key.path("SecretAccessKey").asText();

        Duration took = createActiveCluster(FIRST);
        assertThat("CreateCluster to ACTIVE", took, lessThan(Duration.ofMinutes(5)));

        Cluster cluster = describe(FIRST);
        JsonNode machine = machine(FIRST).orElseThrow();
        assertEquals("running", machine.path("state").asText());
        assertEquals("https://localhost:" + label(machine, "api-port"), cluster.endpoint());
        assertEquals("1.35", cluster.version());

        updateKubeconfig(FIRST);
        JsonNode nodes = json(kubectl(FIRST, "get", "nodes", "-o", "json"), "/items");
        assertEquals(1, nodes.size(), "nodes of " + FIRST);
        assertThat(nodes.get(0).path("metadata").path("name").asText(), endsWith(".ec2.internal"));
        assertTrue(ready(nodes.get(0)), "the node of " + FIRST + " is Ready");
        assertThat(kubectl(FIRST, "get", "runtimeclass", "smolvm-pods", "-o", "name"),
                containsString("runtimeclass.node.k8s.io/smolvm-pods"));
    }

    @Test
    @Order(2)
    void hostDockerInternalResolvesInTheNodeToItsGateway() {
        String nodeId = machine(FIRST).orElseThrow().path("name").asText();
        String route = nodeVm("exec", "--name", nodeId, "--", "ip", "-4", "route", "show", "default");
        String[] words = route.strip().split("\\s+");
        assertTrue(words.length > 2 && "via".equals(words[1]), "default route: " + route);
        gateway = words[2];

        String hosts = nodeVm("exec", "--name", nodeId, "--", "getent", "hosts", "host.docker.internal");
        assertEquals(gateway, hosts.strip().split("\\s+")[0], "host.docker.internal in the node");
        LOG.infov("The node of {0} reaches the Mac at {1}", FIRST, gateway);
    }

    @Test
    @Order(3)
    void imagesPushedToFlociEcrRunInVmPods() {
        awsCliUri = push(AWS_CLI_IMAGE, "linux/arm64", "eks-e2e/aws-cli", "2.35.14");
        amd64Uri = push(AMD64_IMAGE, "linux/amd64", "eks-e2e/alpine", "3.22.1-amd64");

        String issuer = describe(FIRST).identity().oidc().issuer();
        String provider = issuer.substring("https://".length());
        String trust = """
                {"Version":"2012-10-17","Statement":[{"Effect":"Allow",
                  "Principal":{"Federated":"arn:aws:iam::%s:oidc-provider/%s"},
                  "Action":"sts:AssumeRoleWithWebIdentity",
                  "Condition":{"StringEquals":{"%s:sub":"system:serviceaccount:%s:irsa",
                                               "%s:aud":"sts.amazonaws.com"}}}]}
                """.formatted(ACCOUNT, provider, provider, NAMESPACE, provider);
        String roleArn = json(aws("iam", "create-role", "--role-name", IRSA_ROLE,
                "--assume-role-policy-document", trust), "/Role").path("Arn").asText();

        kubectlApply(FIRST, """
                apiVersion: v1
                kind: Namespace
                metadata:
                  name: %s
                """.formatted(NAMESPACE));
        await().atMost(1, TimeUnit.MINUTES).pollInterval(Duration.ofSeconds(1)).until(() ->
                run(kubectlCommand(FIRST, "get", "serviceaccount", "default", "-n", NAMESPACE), kubectlEnv(),
                        null, COMMAND_TIMEOUT).exit() == 0);
        kubectlApply(FIRST, manifests(roleArn));
        kubectl(FIRST, "wait", "--for=condition=Ready", "pod", "--all", "-n", NAMESPACE, "--timeout=300s");

        SmolvmNode node = node(FIRST);
        List<String> vmPods = nodeClient.listPods(node, SmolvmNodeClient.PodQuery.namespace(NAMESPACE)).stream()
                .map(NodePod::name).toList();
        for (String pod : List.of("irsa", "steady", "amd64")) {
            assertThat("VM pods of the node", vmPods, hasItem(pod));
        }
        assertThat(kubectl(FIRST, "logs", "-n", NAMESPACE, "amd64"), startsWith("x86_64"));
    }

    @Test
    @Order(4)
    void theMacReachesAPodThroughANodePortOnTheSameMacPort() {
        String nodePorts = label(machine(FIRST).orElseThrow(), "node-ports");
        if ("none".equals(nodePorts)) {
            LOG.warnv("The node of {0} has no NodePorts, since another process holds one of them, so the "
                    + "NodePort path is not checked", FIRST);
            return;
        }
        int nodePort = Integer.parseInt(nodePorts.split("-")[0]);
        kubectlApply(FIRST, """
                apiVersion: v1
                kind: Service
                metadata:
                  name: amd64
                  namespace: %s
                spec:
                  type: NodePort
                  selector:
                    app: amd64
                  ports:
                    - port: 8080
                      nodePort: %d
                """.formatted(NAMESPACE, nodePort));

        HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + nodePort + "/"))
                .timeout(Duration.ofSeconds(10)).build();
        await().atMost(1, TimeUnit.MINUTES).pollInterval(Duration.ofSeconds(1)).ignoreExceptions()
                .untilAsserted(() -> assertEquals("x86_64\n",
                        http.send(request, HttpResponse.BodyHandlers.ofString()).body()));
    }

    @Test
    @Order(5)
    void anIrsaPodGetsItsRoleBeforeAndAfterASleepAndAWake() {
        JsonNode container = json(kubectl(FIRST, "get", "pod", "irsa", "-n", NAMESPACE, "-o", "json"),
                "/spec/containers/0");
        assertThat(container.path("env").toString(), containsString("\"AWS_ROLE_ARN\""));
        assertThat(container.path("volumeMounts").toString(), containsString("aws-iam-token"));
        assertCallerIsTheRole();
        String memory = memory("irsa");

        SmolvmNode node = node(FIRST);
        String uid = nodeClient.getPod(node, NAMESPACE + "/irsa").uid();
        awaitState(node, "irsa", NodePod.State.ASLEEP, Duration.ofMinutes(3));

        Wake wake = nodeClient.wake(node, NAMESPACE + "/irsa",
                new WakeRequest(WakeReason.api(), "eks-smolvm-e2e", null), Duration.ofMinutes(1));
        assertTrue(wake.resumed(), "the wake resumed the pod");
        assertEquals(NodePod.State.AWAKE, wake.pod().state());
        assertEquals(uid, wake.pod().uid());

        assertCallerIsTheRole();
        assertEquals(memory, memory("irsa"), "the pod's memory after the wake");
        assertEquals(0, restarts("irsa"));
    }

    @Test
    @Order(6)
    void anIsolatingNetworkSendsFlociNamesAndRoutesToTheNodeAndBlocksTheRest() throws IOException {
        String nodeUri = push(NODE_IMAGE, "linux/arm64", "eks-e2e/node", "24-alpine");
        String busyboxUri = push(BUSYBOX_IMAGE, "linux/arm64", "eks-e2e/busybox", "1.37");
        SmolvmNode node = node(FIRST);
        Files.writeString(runtimeFile, network(true));
        await().atMost(1, TimeUnit.MINUTES).pollInterval(Duration.ofSeconds(1)).ignoreExceptions().until(() -> {
            JsonNode status = nodeClient.network(node);
            return status.path("effective_isolation").asBoolean() && status.path("routes").asInt() == 1;
        });

        kubectlApply(FIRST, """
                apiVersion: v1
                kind: Namespace
                metadata:
                  name: %1$s
                ---
                apiVersion: v1
                kind: Pod
                metadata:
                  name: simulator
                  namespace: %1$s
                  labels:
                    app: simulator
                spec:
                  runtimeClassName: smolvm-pods
                  containers:
                    - name: httpd
                      image: %2$s
                      command: ["sh", "-c", "mkdir -p /www && echo SIMULATOR > /www/index.html && exec httpd -f -p 8080 -h /www"]
                ---
                apiVersion: v1
                kind: Service
                metadata:
                  name: simulator
                  namespace: %1$s
                spec:
                  selector:
                    app: simulator
                  ports:
                    - port: 8080
                ---
                apiVersion: v1
                kind: Pod
                metadata:
                  name: client
                  namespace: %1$s
                spec:
                  runtimeClassName: smolvm-pods
                  containers:
                    - name: node
                      image: %3$s
                      command: ["sleep", "infinity"]
                """.formatted(NETWORK_NAMESPACE, busyboxUri, nodeUri));
        kubectl(FIRST, "wait", "--for=condition=Ready", "pod", "--all", "-n", NETWORK_NAMESPACE, "--timeout=300s");

        String script = """
                const dns = require('dns').promises, https = require('https'), http = require('http');
                const get = (mod, url) => new Promise(done => {
                  const req = mod.get(url, {timeout: 15000}, res => {
                    let body = ''; res.on('data', d => body += d); res.on('end', () => done(res.statusCode + ' ' + body.trim()));
                  });
                  req.on('error', e => done('error ' + (e.code || e.message)));
                  req.on('timeout', () => { req.destroy(); done('error timeout'); });
                });
                (async () => {
                  for (const name of ['localhost.floci.io', 'host.docker.internal', 'api.vendor.test', 'eu.pay.vendor.test']) {
                    console.log('dns ' + name + ' ' + (await dns.lookup(name)).address);
                  }
                  console.log('name ' + await dns.lookup('example.com').then(a => a.address, e => e.code));
                  console.log('floci ' + await get(http, 'http://localhost.floci.io:%d/_floci/health'));
                  console.log('route ' + await get(https, 'https://api.vendor.test/'));
                  console.log('wildcard ' + await get(https, 'https://eu.pay.vendor.test/'));
                  console.log('address ' + await get(http, 'http://1.1.1.1/'));
                })();
                """.formatted(floci.port());
        String out = kubectl(FIRST, "exec", "-n", NETWORK_NAMESPACE, "client", "--", "node", "-e", script);
        LOG.infov("Network checks from a VM pod:\n{0}", out);
        for (String name : List.of("localhost.floci.io", "host.docker.internal", "api.vendor.test",
                "eu.pay.vendor.test")) {
            assertThat(out, containsString("dns " + name + " 169.254.53.1"));
        }
        assertThat(out, containsString("name ENOTFOUND"));
        assertThat(out, containsString("floci 200"));
        assertThat(out, containsString("route 200 SIMULATOR"));
        assertThat(out, containsString("wildcard 200 SIMULATOR"));
        assertThat(out, containsString("address error ECONNRESET"));

        Files.writeString(runtimeFile, network(false));
        await().atMost(1, TimeUnit.MINUTES).pollInterval(Duration.ofSeconds(1)).ignoreExceptions()
                .until(() -> !nodeClient.network(node).path("effective_isolation").asBoolean());
        kubectl(FIRST, "delete", "namespace", NETWORK_NAMESPACE, "--wait=false");
    }

    /** A runtime file whose network routes {@code api.vendor.test} and {@code *.vendor.test} to the simulator. */
    private static String network(boolean isolation) {
        return """
                version: 1
                network:
                  isolation:
                    enabled: %s
                  routes:
                    vendor:
                      origins: [https://api.vendor.test, https://*.vendor.test]
                      target:
                        backend-url: http://simulator.%s.svc.cluster.local:8080
                """.formatted(isolation, NETWORK_NAMESPACE);
    }

    @Test
    @Order(7)
    void aSecondClusterIsRefusedWhileTheFirstHoldsTheNode() {
        ResourceLimitExceededException refused = assertThrows(ResourceLimitExceededException.class,
                () -> eks.createCluster(request -> request.name(SECOND).roleArn(CLUSTER_ROLE)));

        assertThat(refused.awsErrorDetails().errorMessage(), containsString("one EKS cluster per smolvm-pods node"));
        assertEquals(1, ownMachines().size(), "machines of this Floci");
        assertFalse(eks.listClusters().clusters().contains(SECOND));
    }

    @Test
    @Order(8)
    void nodesKeepRunningAndPodsKeepTheirMemoryAcrossARestartWithTheDefaults() {
        Map<String, String> uids = podUids();
        Map<String, String> memory = memoryOfEveryPod();
        String ca = describe(FIRST).certificateAuthority().data();

        floci.stop();
        assertEquals("running", machine(FIRST).orElseThrow().path("state").asText());

        restartAndAwaitActive(flociEnv);
        assertEquals(ca, describe(FIRST).certificateAuthority().data(), "the cluster CA of an adopted node");
        assertEquals(uids, podUids());
        assertEquals(memory, memoryOfEveryPod());
        for (String pod : uids.keySet()) {
            assertEquals(0, restarts(pod), "restarts of " + pod);
        }
    }

    @Test
    @Order(9)
    void aChangedIdleTimeoutReachesTheNodeWithItsPodsKept() {
        SmolvmNode node = node(FIRST);
        String memory = memory("steady");
        assertEquals(NodePod.State.AWAKE, nodeClient.getPod(node, NAMESPACE + "/steady").state(),
                "the steady pod, which the exec woke");

        floci.stop();
        Map<String, String> env = new LinkedHashMap<>(flociEnv);
        env.put("FLOCI_RUNTIME_NODE_IDLE_TIMEOUT", "20s");
        env.put("FLOCI_RUNTIME_NODE_CPU_WINDOW", "5s");
        restartAndAwaitActive(env);

        String nodeId = machine(FIRST).orElseThrow().path("name").asText();
        assertThat(read(seedDir(nodeId).resolve("env")), containsString("FLOCI_NODE_IDLE_TIMEOUT=20s"));
        awaitState(node, "steady", NodePod.State.ASLEEP, Duration.ofMinutes(3));
        assertEquals(memory, memory("steady"), "the steady pod's memory");
        assertEquals(0, restarts("steady"));
    }

    @Test
    @Order(10)
    void flociStartsAStoppedNode() {
        String memory = memory("steady");

        floci.stop();
        String nodeId = machine(FIRST).orElseThrow().path("name").asText();
        nodeVm("stop", "--name", nodeId);
        assertEquals("stopped", machine(FIRST).orElseThrow().path("state").asText());

        // This Floci stops the node when it stops, which the next step checks.
        Map<String, String> env = new LinkedHashMap<>(flociEnv);
        env.put("FLOCI_RUNTIME_NODE_KEEP_RUNNING_ON_SHUTDOWN", "false");
        restartAndAwaitActive(env);

        assertEquals("running", machine(FIRST).orElseThrow().path("state").asText());
        kubectl(FIRST, "wait", "--for=condition=Ready", "pod", "--all", "-n", NAMESPACE, "--timeout=300s");
        assertNotEquals(memory, memory("steady"), "a node started again starts its pods cold");
    }

    @Test
    @Order(11)
    void withKeepRunningOffFlociStopsTheNodeAndStartsItAgain() {
        floci.stop();
        assertEquals("stopped", machine(FIRST).orElseThrow().path("state").asText());

        restartAndAwaitActive(flociEnv);
        assertEquals("running", machine(FIRST).orElseThrow().path("state").asText());
        kubectl(FIRST, "wait", "--for=condition=Ready", "pod", "--all", "-n", NAMESPACE, "--timeout=300s");
        assertCallerIsTheRole();
    }

    @Test
    @Order(12)
    void flociCreatesADeletedNodeAgainWithItsPorts() {
        String ca = describe(FIRST).certificateAuthority().data();
        JsonNode node = machine(FIRST).orElseThrow();

        floci.stop();
        nodeVm("delete", "--name", node.path("name").asText());
        assertFalse(machine(FIRST).isPresent(), "the node is gone");
        restartAndAwaitActive(flociEnv);

        JsonNode created = machine(FIRST).orElseThrow();
        assertEquals(node.path("name").asText(), created.path("name").asText());
        assertEquals(label(node, "api-port"), label(created, "api-port"));
        assertEquals(label(node, "service-port"), label(created, "service-port"));
        assertNotEquals(ca, describe(FIRST).certificateAuthority().data(), "the CA of a node created again");
        assertFalse(floci.logLines("is gone; creating it again").isEmpty(), "Floci logs that it creates the node again");
        updateKubeconfig(FIRST);
        assertTrue(ready(json(kubectl(FIRST, "get", "nodes", "-o", "json"), "/items/0")),
                "the node created again is Ready");
    }

    @Test
    @Order(13)
    void deleteClusterKeepsTheNodeAndAResetDeletesIt() {
        JsonNode node = machine(FIRST).orElseThrow();
        String nodeId = node.path("name").asText();
        eks.deleteCluster(request -> request.name(FIRST));
        waiter.waitUntilClusterDeleted(request -> request.name(FIRST));

        assertEquals("running", machine(FIRST).orElseThrow().path("state").asText(), "the node outlives its cluster");
        assertTrue(Files.isDirectory(seedDir(nodeId)), "the seed of node " + nodeId);

        HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        HttpRequest reset = HttpRequest.newBuilder(floci.endpoint().resolve("/_floci/state/reset"))
                .POST(HttpRequest.BodyPublishers.noBody()).timeout(Duration.ofMinutes(3)).build();
        assertEquals(200, sendQuietly(http, reset));

        assertEquals(List.of(), ownMachines(), "machines of this Floci");
        assertFalse(Files.exists(seedDir(nodeId).getParent()), "the files of node " + nodeId);
        for (int port : ports(node)) {
            // smolvm closes a machine's forwards just after node-vm.sh delete returns.
            await().atMost(30, TimeUnit.SECONDS).pollInterval(Duration.ofMillis(500))
                    .untilAsserted(() -> assertTrue(free(port), "Mac port " + port + " of node " + nodeId
                            + " is free"));
        }
    }

    private static int sendQuietly(HttpClient http, HttpRequest request) {
        try {
            return http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted", e);
        }
    }

    private Duration createActiveCluster(String name) {
        long started = System.nanoTime();
        CreateClusterResponse created = eks.createCluster(request -> request.name(name).roleArn(CLUSTER_ROLE));
        Duration answered = Duration.ofNanos(System.nanoTime() - started);
        assertEquals(ClusterStatus.CREATING, created.cluster().status());
        assertThat(answered, lessThan(Duration.ofSeconds(2)));
        waiter.waitUntilClusterActive(request -> request.name(name)).matched().response()
                .orElseThrow(() -> new AssertionError(name + " did not become ACTIVE"));
        Duration took = Duration.ofNanos(System.nanoTime() - started);
        LOG.infov("EKS cluster {0} was ACTIVE {1} ms after CreateCluster, which answered in {2} ms", name,
                String.valueOf(took.toMillis()), String.valueOf(answered.toMillis()));
        return took;
    }

    /** Starts Floci again with {@code env} and waits until the cluster is ACTIVE. */
    private void restartAndAwaitActive(Map<String, String> env) {
        long started = System.nanoTime();
        floci.start(env);
        waiter.waitUntilClusterActive(request -> request.name(FIRST)).matched().response()
                .orElseThrow(() -> new AssertionError(FIRST + " did not become ACTIVE after a restart:\n"
                        + floci.logTail(60)));
        LOG.infov("The cluster was ACTIVE {0} ms after Floci started again",
                String.valueOf(Duration.ofNanos(System.nanoTime() - started).toMillis()));
    }

    private Cluster describe(String name) {
        return eks.describeCluster(request -> request.name(name)).cluster();
    }

    private void updateKubeconfig(String cluster) {
        Path kubeconfig = work.resolve("kube").resolve(cluster + ".yaml");
        kubeconfigs.put(cluster, kubeconfig);
        aws("eks", "update-kubeconfig", "--name", cluster, "--kubeconfig", kubeconfig.toString());
    }

    /** Pushes {@code image}'s {@code platform} manifest to Floci ECR as {@code repository:tag}. */
    private String push(String image, String platform, String repository, String tag) {
        String uri = json(aws("ecr", "create-repository", "--repository-name", repository), "/repository")
                .path("repositoryUri").asText() + ":" + tag;
        if (run(List.of("docker", "image", "inspect", image), Map.of(), null, COMMAND_TIMEOUT).exit() != 0) {
            docker("pull", "--platform", platform, image);
            pulledImages.add(image);
        }
        docker("tag", image, uri);
        pushedImages.add(uri);
        docker("push", uri);
        return uri;
    }

    private String manifests(String roleArn) {
        String endpoint = "http://" + gateway + ":" + floci.port();
        return """
                apiVersion: v1
                kind: ServiceAccount
                metadata:
                  name: irsa
                  namespace: %1$s
                  annotations:
                    eks.amazonaws.com/role-arn: %2$s
                ---
                apiVersion: v1
                kind: Pod
                metadata:
                  name: irsa
                  namespace: %1$s
                  annotations:
                    smolvm-pods.floci.io/idle-timeout: "10s"
                    smolvm-pods.floci.io/cpu-window: "5s"
                spec:
                  runtimeClassName: smolvm-pods
                  serviceAccountName: irsa
                  containers:
                    - name: aws
                      image: %3$s
                      command: ["sh", "-c", "cat /proc/sys/kernel/random/uuid > /tmp/memory && exec sleep infinity"]
                      env:
                        - name: AWS_ENDPOINT_URL
                          value: %4$s
                ---
                apiVersion: v1
                kind: Pod
                metadata:
                  name: steady
                  namespace: %1$s
                spec:
                  runtimeClassName: smolvm-pods
                  containers:
                    - name: aws
                      image: %3$s
                      command: ["sh", "-c", "cat /proc/sys/kernel/random/uuid > /tmp/memory && exec sleep infinity"]
                ---
                apiVersion: v1
                kind: Pod
                metadata:
                  name: amd64
                  namespace: %1$s
                  labels:
                    app: amd64
                spec:
                  runtimeClassName: smolvm-pods
                  containers:
                    - name: alpine
                      image: %5$s
                      command: ["sh", "-c"]
                      args:
                        - |
                          uname -m
                          cat /proc/sys/kernel/random/uuid > /tmp/memory
                          arch=$(uname -m)
                          while true; do
                            printf 'HTTP/1.0 200 OK\\r\\nContent-Length: %%d\\r\\nConnection: close\\r\\n\\r\\n%%s\\n' \\
                                $((${#arch} + 1)) "$arch" | nc -l -p 8080
                          done
                      ports:
                        - containerPort: 8080
                """.formatted(NAMESPACE, roleArn, awsCliUri, endpoint, amd64Uri);
    }

    private void assertCallerIsTheRole() {
        JsonNode identity = json(kubectl(FIRST, "exec", "-n", NAMESPACE, "irsa", "--", "aws", "sts",
                "get-caller-identity", "--output", "json"), "");
        assertThat(identity.path("Arn").asText(),
                startsWith("arn:aws:sts::" + ACCOUNT + ":assumed-role/" + IRSA_ROLE + "/"));
    }

    private String memory(String pod) {
        return kubectl(FIRST, "exec", "-n", NAMESPACE, pod, "--", "cat", "/tmp/memory").strip();
    }

    private Map<String, String> memoryOfEveryPod() {
        Map<String, String> memory = new LinkedHashMap<>();
        for (String pod : List.of("irsa", "steady", "amd64")) {
            memory.put(pod, memory(pod));
        }
        return memory;
    }

    private Map<String, String> podUids() {
        Map<String, String> uids = new LinkedHashMap<>();
        for (JsonNode pod : json(kubectl(FIRST, "get", "pods", "-n", NAMESPACE, "-o", "json"), "/items")) {
            uids.put(pod.path("metadata").path("name").asText(), pod.path("metadata").path("uid").asText());
        }
        return uids;
    }

    private int restarts(String pod) {
        return json(kubectl(FIRST, "get", "pod", pod, "-n", NAMESPACE, "-o", "json"),
                "/status/containerStatuses/0").path("restartCount").asInt(-1);
    }

    /** Waits for {@code pod}'s state, also while the node service restarts to apply a changed setting. */
    private void awaitState(SmolvmNode node, String pod, NodePod.State state, Duration timeout) {
        await().atMost(timeout.toSeconds(), TimeUnit.SECONDS).pollInterval(Duration.ofSeconds(2))
                .ignoreException(NodeApiException.class).untilAsserted(() ->
                        assertThat("the state of " + pod, nodeClient.getPod(node, NAMESPACE + "/" + pod).state(),
                                is(state)));
    }

    private static boolean ready(JsonNode node) {
        for (JsonNode condition : node.path("status").path("conditions")) {
            if ("Ready".equals(condition.path("type").asText())) {
                return "True".equals(condition.path("status").asText());
            }
        }
        return false;
    }

    /** The node service of {@code cluster}'s node, as Floci calls it. */
    private SmolvmNode node(String cluster) {
        JsonNode machine = machine(cluster).orElseThrow();
        String nodeId = machine.path("name").asText();
        return new SmolvmNode(FlociNode.NAME,
                URI.create("http://127.0.0.1:" + label(machine, "service-port")),
                NodeToken.file(seedDir(nodeId).resolve("api-token")), SmolvmNode.Kind.CREATED, List.of());
    }

    private Path seedDir(String nodeId) {
        return data.resolve("node").resolve(nodeId).resolve("seed");
    }

    /** The machine that this Floci created, which {@code cluster} runs on. */
    private Optional<JsonNode> machine(String cluster) {
        return ownMachines().stream().findFirst();
    }

    /** Every {@code node-vm.sh} machine with this Floci's owner label. */
    private List<JsonNode> ownMachines() {
        List<JsonNode> own = new ArrayList<>();
        for (JsonNode machine : json(nodeVm("status", "--json"), "")) {
            if (resourceNamespace.equals(label(machine, "floci.node.owner"))) {
                own.add(machine);
            }
        }
        return own;
    }

    private static String label(JsonNode machine, String key) {
        return machine.path("labels").path(key).asText("");
    }

    /** The Mac ports in a machine's labels. */
    private static List<Integer> ports(JsonNode machine) {
        List<Integer> ports = new ArrayList<>();
        ports.add(Integer.parseInt(label(machine, "api-port")));
        ports.add(Integer.parseInt(label(machine, "service-port")));
        String nodePorts = label(machine, "node-ports");
        if (!nodePorts.isEmpty() && !"none".equals(nodePorts)) {
            String[] range = nodePorts.split("-");
            int last = Integer.parseInt(range[range.length - 1]);
            for (int port = Integer.parseInt(range[0]); port <= last; port++) {
                ports.add(port);
            }
        }
        return ports;
    }

    private String aws(String... args) {
        List<String> command = new ArrayList<>(List.of("aws", "--endpoint-url", floci.endpoint().toString(),
                "--region", REGION, "--output", "json"));
        command.addAll(List.of(args));
        return succeed(command, awsEnv("test", "test"), null);
    }

    private String kubectl(String cluster, String... args) {
        return succeed(kubectlCommand(cluster, args), kubectlEnv(), null);
    }

    private void kubectlApply(String cluster, String yaml) {
        succeed(kubectlCommand(cluster, "apply", "-f", "-"), kubectlEnv(), yaml);
    }

    private List<String> kubectlCommand(String cluster, String... args) {
        List<String> command = new ArrayList<>(List.of("kubectl", "--kubeconfig", kubeconfigs.get(cluster).toString()));
        command.addAll(List.of(args));
        return command;
    }

    /** kubectl's environment: the IAM user's key, which {@code aws eks get-token} signs with. */
    private Map<String, String> kubectlEnv() {
        return awsEnv(accessKeyId, secretAccessKey);
    }

    private Map<String, String> awsEnv(String keyId, String secret) {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("AWS_ACCESS_KEY_ID", keyId);
        env.put("AWS_SECRET_ACCESS_KEY", secret);
        env.put("AWS_REGION", REGION);
        env.put("AWS_CONFIG_FILE", work.resolve("aws/config").toString());
        env.put("AWS_SHARED_CREDENTIALS_FILE", work.resolve("aws/credentials").toString());
        env.put("AWS_PAGER", "");
        return env;
    }

    private String nodeVm(String... args) {
        List<String> command = new ArrayList<>(List.of(nodeVmScript.toString()));
        command.addAll(List.of(args));
        return succeed(command, Map.of("SMOLVM_SRC", smolvmSrc), null);
    }

    private String docker(String... args) {
        List<String> command = new ArrayList<>(List.of("docker"));
        command.addAll(List.of(args));
        return succeed(command, Map.of(), null);
    }

    private String succeed(List<String> command, Map<String, String> env, String stdin) {
        Result result = run(command, env, stdin, COMMAND_TIMEOUT);
        if (result.exit() != 0) {
            throw new AssertionError(String.join(" ", command) + " exited with " + result.exit() + ":\n"
                    + result.stderr() + result.stdout());
        }
        return result.stdout();
    }

    /**
     * Runs {@code command} with {@code env} and without the test's own {@code AWS_*} and
     * {@code KUBECONFIG} variables. Its output goes to files, so a process it leaves running
     * cannot hold the call open.
     */
    private Result run(List<String> command, Map<String, String> env, String stdin, Duration timeout) {
        try {
            Path out = Files.createTempFile(work, "command", ".out");
            Path err = Files.createTempFile(work, "command", ".err");
            try {
                ProcessBuilder builder = new ProcessBuilder(command).redirectOutput(out.toFile())
                        .redirectError(err.toFile());
                builder.environment().keySet().removeIf(name -> name.startsWith("AWS_") || name.equals("KUBECONFIG"));
                builder.environment().putAll(env);
                if (stdin == null) {
                    builder.redirectInput(new File("/dev/null"));
                }
                Process process = builder.start();
                if (stdin != null) {
                    try (OutputStream input = process.getOutputStream()) {
                        input.write(stdin.getBytes(StandardCharsets.UTF_8));
                    }
                }
                if (!process.waitFor(timeout.toSeconds(), TimeUnit.SECONDS)) {
                    process.descendants().forEach(ProcessHandle::destroyForcibly);
                    process.destroyForcibly();
                    throw new AssertionError(String.join(" ", command) + " did not finish within "
                            + timeout.toSeconds() + " s");
                }
                return new Result(process.exitValue(), Files.readString(out), Files.readString(err));
            } finally {
                Files.deleteIfExists(out);
                Files.deleteIfExists(err);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while running " + command, e);
        }
    }

    private static JsonNode json(String text, String pointer) {
        try {
            return JSON.readTree(text).at(pointer);
        } catch (IOException e) {
            throw new AssertionError("Not JSON: " + text, e);
        }
    }

    private static String read(Path file) {
        try {
            return Files.readString(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String firstSet(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    private static void tolerate(Runnable cleanup) {
        try {
            cleanup.run();
        } catch (RuntimeException | AssertionError e) {
            LOG.warnv("Cleanup step failed: {0}", e.getMessage());
        }
    }

    private static boolean free(int port) {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(false);
            socket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }

    private static String sha256(Path file) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        try (InputStream in = new DigestInputStream(Files.newInputStream(file), digest)) {
            in.transferTo(OutputStream.nullOutputStream());
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private record Result(int exit, String stdout, String stderr) {
    }
}
