package io.github.hectorvent.floci.runtime.wakes;

import io.github.hectorvent.floci.runtime.node.NodesSection;
import io.github.hectorvent.floci.runtime.node.RuntimeFile;
import io.github.hectorvent.floci.runtime.node.RuntimeFileException;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.PodSelector;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.RabbitMqQueue;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Target;
import io.github.hectorvent.floci.runtime.wakes.WorkloadDefinitions.Workload;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WorkloadsSectionServiceTest {

    private static final String TOKEN = "0123456789abcdef0123456789abcdef";

    @TempDir
    Path dir;

    private final NodesSection nodes = new NodesSection();
    private final WorkloadsSection workloads =
            new WorkloadsSection("000000000000", "us-east-1", nodes);

    @Test
    void readsAWorkloadWithEveryField() throws IOException {
        WorkloadDefinitions definitions = read("""
                version: 1
                workloads:
                  orders:
                    enabled: true
                    account-id: '123456789012'
                    region: eu-west-1
                    idle-timeout-seconds: 300
                    startup-timeout-seconds: 60
                    queues: [orders-incoming, orders-priority.fifo]
                    rabbitmq-queues:
                      - broker-id: b-0b1c2d3e-0000-4000-8000-000000000001
                        virtual-host: shop
                        queue: background-jobs
                        username-env: RABBITMQ_OBSERVER_USER
                        password-env: RABBITMQ_OBSERVER_PASSWORD
                    runtime:
                      type: kubernetes
                      cluster-name: integration-test
                      namespace: integration-test
                      deployment: orders
                """);

        Workload orders = definitions.workloads().get("orders");
        assertThat(orders, is(new Workload("orders", true, "123456789012", "eu-west-1", Duration.ofSeconds(300),
                Duration.ofSeconds(60), List.of("orders-incoming", "orders-priority.fifo"),
                List.of(new RabbitMqQueue("b-0b1c2d3e-0000-4000-8000-000000000001", "shop", "background-jobs",
                        "RABBITMQ_OBSERVER_USER", "RABBITMQ_OBSERVER_PASSWORD")),
                new Target("cluster:123456789012/eu-west-1/integration-test", "integration-test",
                        PodSelector.Controller.deployment("orders")))));
    }

    @Test
    void appliesTheDefaults() throws IOException {
        WorkloadDefinitions definitions = read("""
                version: 1
                workloads:
                  jobs:
                    rabbitmq-queues:
                      - broker-id: b-1
                        queue: background-jobs
                    runtime:
                      type: kubernetes
                      cluster-name: integration-test
                      statefulset: jobs
                """);

        Workload jobs = definitions.workloads().get("jobs");
        assertThat(jobs.enabled(), is(true));
        assertThat(jobs.accountId(), is("000000000000"));
        assertThat(jobs.region(), is("us-east-1"));
        assertThat(jobs.idleTimeout(), is(Duration.ofSeconds(120)));
        assertThat(jobs.startupTimeout(), is(Duration.ofSeconds(30)));
        assertThat(jobs.queues(), is(empty()));
        assertThat(jobs.rabbitMqQueues(), contains(new RabbitMqQueue("b-1", "/", "background-jobs", null, null)));
        assertThat(jobs.target(), is(new Target("cluster:000000000000/us-east-1/integration-test", "default",
                PodSelector.Controller.statefulSet("jobs"))));
    }

    @Test
    void aWorkloadOnADeclaredNodeNamesItInTheDirectory() throws IOException {
        WorkloadDefinitions definitions = read(withNode("""
                workloads:
                  orders:
                    queues: [orders-incoming]
                    runtime:
                      type: kubernetes
                      node: dev
                      namespace: shop
                      selector:
                        app: orders
                        app.kubernetes.io/part-of: shop
                        canary: ""
                """));

        Target target = definitions.workloads().get("orders").target();
        assertThat(target.node(), is("node:dev"));
        assertThat(target.namespace(), is("shop"));
        assertThat(target.pods(), is(new PodSelector.Labels(Map.of("app", "orders",
                "app.kubernetes.io/part-of", "shop", "canary", ""))));
    }

    @Test
    void aWorkloadNamesANodeOfTheSameFile() throws IOException {
        Path file = write(withNode("""
                workloads:
                  orders:
                    queues: [orders-incoming]
                    runtime: {type: kubernetes, node: prod, deployment: orders}
                """));

        RuntimeFileException e = assertThrows(RuntimeFileException.class,
                () -> RuntimeFile.read(file, List.of(nodes, workloads)));

        assertThat(e.getMessage(), is("workloads.orders.runtime.node: names no node in nodes"));
        assertThat(e.path(), is("workloads.orders.runtime.node"));
    }

    @Test
    void aNodeOutsideTheFileIsUnknown() {
        assertProblem("""
                workloads:
                  orders:
                    queues: [orders-incoming]
                    runtime: {type: kubernetes, node: dev, deployment: orders}
                """, "workloads.orders.runtime.node: names no node in nodes");
    }

    @Test
    void aWorkloadNamesExactlyOneOfAClusterAndANode() {
        assertProblem(workload("runtime: {type: kubernetes, deployment: orders}"),
                "workloads.orders.runtime: needs exactly one of cluster-name and node");
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: c, node: dev, deployment: orders}"),
                "workloads.orders.runtime: needs exactly one of cluster-name and node");
    }

    @Test
    void aWorkloadSelectsItsPodsOneWay() {
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: c}"),
                "workloads.orders.runtime: needs exactly one of deployment, statefulset and selector");
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: c, deployment: a, statefulset: b}"),
                "workloads.orders.runtime: needs exactly one of deployment, statefulset and selector");
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: c, deployment: a, selector: {app: a}}"),
                "workloads.orders.runtime: needs exactly one of deployment, statefulset and selector");
    }

    @ParameterizedTest
    @ValueSource(strings = {"ecs", "ec2", "Kubernetes"})
    void onlyKubernetesWorkloadsAreRead(String type) {
        assertProblem(workload("runtime: {type: " + type + ", cluster-name: c, deployment: orders}"),
                "workloads.orders.runtime.type: must be kubernetes");
    }

    @Test
    void theRuntimeAndItsTypeAreRequired() {
        assertProblem("workloads:\n  orders:\n    queues: [q]\n", "workloads.orders.runtime: is required");
        assertProblem(workload("runtime: {cluster-name: c, deployment: orders}"),
                "workloads.orders.runtime.type: is required");
        assertProblem(workload("runtime: kubernetes"), "workloads.orders.runtime: must be a mapping");
    }

    @Test
    void unknownFieldsAreRefused() {
        String fields = "enabled, account-id, region, idle-timeout-seconds, startup-timeout-seconds, queues, "
                + "rabbitmq-queues, runtime";
        assertProblem(workload("host: orders.dev.test\nruntime: {type: kubernetes, cluster-name: c, deployment: o}"),
                "workloads.orders.host: unknown field; workloads.orders has " + fields);
        assertProblem(workload("health: {}\nruntime: {type: kubernetes, cluster-name: c, deployment: o}"),
                "workloads.orders.health: unknown field; workloads.orders has " + fields);
        String runtimeFields = "type, cluster-name, node, namespace, deployment, statefulset, selector";
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: c, deployment: o, replicas: 2}"),
                "workloads.orders.runtime.replicas: unknown field; workloads.orders.runtime has " + runtimeFields);
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: c, deployment: o, "
                        + "backend-url: \"http://orders:8080\"}"),
                "workloads.orders.runtime.backend-url: unknown field; workloads.orders.runtime has " + runtimeFields);
        assertProblem("workloads:\n  orders:\n    queues: [q]\n    rabbitmq-queues: [{broker-id: b, queue: q, "
                        + "vhost: /}]\n    runtime: {type: kubernetes, cluster-name: c, deployment: o}\n",
                "workloads.orders.rabbitmq-queues.0.vhost: unknown field; workloads.orders.rabbitmq-queues.0 has "
                        + "broker-id, virtual-host, queue, username-env, password-env");
    }

    @Test
    void aWorkloadNameIsShortAndPlain() throws IOException {
        String name = "a".repeat(64);
        assertThat(read("version: 1\n" + workload(clusterRuntime()).replace("orders:", name + ":"))
                .workloads().keySet(), contains(name));
        assertThat(read("version: 1\n" + workload(clusterRuntime()).replace("orders:", "orders_v2.blue-1:"))
                .workloads().keySet(), contains("orders_v2.blue-1"));
        assertProblem(workload(clusterRuntime()).replace("orders:", "a".repeat(65) + ":"),
                "workloads." + "a".repeat(65) + ": a workload name is 1 to 64 of A-Z, a-z, 0-9, _, . and -");
        assertProblem(workload(clusterRuntime()).replace("orders:", "\"orders/eu\":"),
                "workloads.orders/eu: a workload name is 1 to 64 of A-Z, a-z, 0-9, _, . and -");
    }

    @Test
    void theAccountIsTwelveDigitsInQuotes() {
        String rule = "workloads.orders.account-id: must be 12 digits in quotes, such as '000000000000'";
        assertProblem(workload("account-id: 123456789012\n" + clusterRuntime()), rule);
        assertProblem(workload("account-id: '12345678901'\n" + clusterRuntime()), rule);
        assertProblem(workload("account-id: 'abcdefghijkl'\n" + clusterRuntime()), rule);
    }

    @Test
    void theRegionIsAnAwsRegion() {
        String rule = "workloads.orders.region: must be the code of an AWS region, in lower case";
        assertProblem(workload("region: mars-1\n" + clusterRuntime()), rule);
        assertProblem(workload("region: US-EAST-1\n" + clusterRuntime()), rule);
        assertProblem(workload("region: 7\n" + clusterRuntime()), "workloads.orders.region: must be a nonempty string");
    }

    @Test
    void theClusterNodeIsInTheWorkloadsAccountAndRegion() throws IOException {
        WorkloadDefinitions definitions = read("version: 1\n" + workload("account-id: '111111111111'\n"
                + "region: ap-southeast-2\n" + clusterRuntime()));

        assertThat(definitions.workloads().get("orders").target().node(),
                is("cluster:111111111111/ap-southeast-2/integration-test"));
    }

    @Test
    void enabledIsTrueOrFalse() throws IOException {
        WorkloadDefinitions definitions = read("version: 1\n" + workload("enabled: false\n" + clusterRuntime()));

        assertThat(definitions.workloads().get("orders").enabled(), is(false));
        assertThat(definitions.enabled(), is(empty()));
        assertProblem(workload("enabled: \"yes\"\n" + clusterRuntime()),
                "workloads.orders.enabled: must be true or false");
    }

    @Test
    void theTimeoutsAreWholeSecondsInRange() throws IOException {
        Workload linger = read("version: 1\n" + workload("idle-timeout-seconds: 0\nstartup-timeout-seconds: 120\n"
                + clusterRuntime())).workloads().get("orders");
        assertThat(linger.idleTimeout(), is(Duration.ZERO));
        assertThat(linger.startupTimeout(), is(Duration.ofSeconds(120)));

        assertProblem(workload("idle-timeout-seconds: 86401\n" + clusterRuntime()),
                "workloads.orders.idle-timeout-seconds: must be a whole number from 0 to 86400");
        assertProblem(workload("idle-timeout-seconds: -1\n" + clusterRuntime()),
                "workloads.orders.idle-timeout-seconds: must be a whole number from 0 to 86400");
        assertProblem(workload("idle-timeout-seconds: \"120\"\n" + clusterRuntime()),
                "workloads.orders.idle-timeout-seconds: must be a whole number from 0 to 86400");
        assertProblem(workload("startup-timeout-seconds: 0\n" + clusterRuntime()),
                "workloads.orders.startup-timeout-seconds: must be a whole number from 1 to 120");
        assertProblem(workload("startup-timeout-seconds: 121\n" + clusterRuntime()),
                "workloads.orders.startup-timeout-seconds: must be a whole number from 1 to 120");
        assertProblem(workload("startup-timeout-seconds: 2.5\n" + clusterRuntime()),
                "workloads.orders.startup-timeout-seconds: must be a whole number from 1 to 120");
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "\"https://sqs.us-east-1.amazonaws.com/000000000000/orders\"", "\"arn:aws:sqs:us-east-1:000000000000:orders\"",
        "orders.incoming", "orders.fifo.fifo", "7",
    })
    void queuesAreSqsQueueNames(String queue) {
        assertProblem("workloads:\n  orders:\n    queues: [" + queue + "]\n    " + clusterRuntime() + "\n",
                "workloads.orders.queues.0: must be an SQS queue name: 1 to 80 of A-Z, a-z, 0-9, _ and -, ending in "
                        + ".fifo for a FIFO queue");
    }

    @Test
    void aQueueNameHasAtMostEightyCharacters() throws IOException {
        String standard = "q".repeat(80);
        String fifo = "q".repeat(75) + ".fifo";
        assertThat(read("version: 1\nworkloads:\n  orders:\n    queues: [" + standard + ", " + fifo + "]\n    "
                + clusterRuntime() + "\n").workloads().get("orders").queues(), contains(standard, fifo));
        assertProblem("workloads:\n  orders:\n    queues: [" + "q".repeat(81) + "]\n    " + clusterRuntime() + "\n",
                "workloads.orders.queues.0: must be an SQS queue name: 1 to 80 of A-Z, a-z, 0-9, _ and -, ending in "
                        + ".fifo for a FIFO queue");
        assertProblem("workloads:\n  orders:\n    queues: [" + "q".repeat(76) + ".fifo]\n    " + clusterRuntime()
                        + "\n",
                "workloads.orders.queues.0: must be an SQS queue name: 1 to 80 of A-Z, a-z, 0-9, _ and -, ending in "
                        + ".fifo for a FIFO queue");
    }

    @Test
    void aQueueIsListedOnce() {
        assertProblem("workloads:\n  orders:\n    queues: [a, b, a]\n    " + clusterRuntime() + "\n",
                "workloads.orders.queues.2: a is listed twice");
        assertProblem("workloads:\n  orders:\n    queues: a\n    " + clusterRuntime() + "\n",
                "workloads.orders.queues: must be a list");
    }

    @Test
    void aWorkloadHasAQueue() {
        assertProblem("workloads:\n  orders:\n    queues: []\n    " + clusterRuntime() + "\n",
                "workloads.orders: needs at least one queue in queues or rabbitmq-queues");
        assertProblem("workloads:\n  orders:\n    " + clusterRuntime() + "\n",
                "workloads.orders: needs at least one queue in queues or rabbitmq-queues");
    }

    @Test
    void aRabbitMqQueueNamesItsBrokerAndQueue() {
        assertProblem(rabbitMq("{queue: jobs}"), "workloads.jobs.rabbitmq-queues.0.broker-id: is required");
        assertProblem(rabbitMq("{broker-id: b-1}"), "workloads.jobs.rabbitmq-queues.0.queue: is required");
        assertProblem(rabbitMq("{broker-id: \"b_1\", queue: jobs}"),
                "workloads.jobs.rabbitmq-queues.0.broker-id: must be an Amazon MQ broker id: 1 to 128 of A-Z, a-z, "
                        + "0-9 and -");
        assertProblem(rabbitMq("{broker-id: b-1, queue: jobs}, {broker-id: b-1, virtual-host: /, queue: jobs}"),
                "workloads.jobs.rabbitmq-queues.1: the queue jobs of broker b-1 and virtual host / is listed twice");
        assertProblem("workloads:\n  jobs:\n    rabbitmq-queues: {broker-id: b-1, queue: jobs}\n    "
                + clusterRuntime() + "\n", "workloads.jobs.rabbitmq-queues: must be a list");
    }

    @Test
    void aRabbitMqQueueNamesBothCredentialVariablesOrNeither() {
        assertProblem(rabbitMq("{broker-id: b-1, queue: jobs, username-env: USER}"),
                "workloads.jobs.rabbitmq-queues.0: needs both username-env and password-env, or neither");
        assertProblem(rabbitMq("{broker-id: b-1, queue: jobs, password-env: PASSWORD}"),
                "workloads.jobs.rabbitmq-queues.0: needs both username-env and password-env, or neither");
        assertProblem(rabbitMq("{broker-id: b-1, queue: jobs, username-env: 1USER, password-env: PASSWORD}"),
                "workloads.jobs.rabbitmq-queues.0.username-env: must be an environment variable name: A-Z, a-z, 0-9 "
                        + "and _, not starting with a digit");
    }

    @Test
    void theNamespaceIsAKubernetesNamespace() {
        String rule = "workloads.orders.runtime.namespace: must be a Kubernetes namespace: 1 to 63 of a-z, 0-9 and -, "
                + "starting and ending with a letter or digit";
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: c, namespace: Shop, deployment: o}"), rule);
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: c, namespace: shop-, deployment: o}"), rule);
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: c, namespace: " + "n".repeat(64)
                + ", deployment: o}"), rule);
    }

    @Test
    void theControllerIsAKubernetesObjectName() throws IOException {
        assertThat(read("version: 1\n" + workload("runtime: {type: kubernetes, cluster-name: c, deployment: "
                + "orders.v2-blue}")).workloads().get("orders").target().pods(),
                is(PodSelector.Controller.deployment("orders.v2-blue")));
        String rule = "must be a Kubernetes object name: at most 253 of a-z, 0-9, - and ., starting and ending with a "
                + "letter or digit";
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: c, deployment: Orders}"),
                "workloads.orders.runtime.deployment: " + rule);
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: c, statefulset: -jobs}"),
                "workloads.orders.runtime.statefulset: " + rule);
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: c, deployment: " + "d".repeat(254) + "}"),
                "workloads.orders.runtime.deployment: " + rule);
    }

    @Test
    void theClusterNameIsAnEksClusterName() {
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: \"-test\", deployment: o}"),
                "workloads.orders.runtime.cluster-name: must be an EKS cluster name: 1 to 100 of A-Z, a-z, 0-9, _ and "
                        + "-, starting with a letter or digit");
    }

    @Test
    void aSelectorHasLabels() {
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: c, selector: {}}"),
                "workloads.orders.runtime.selector: needs at least one label");
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: c, selector: [app]}"),
                "workloads.orders.runtime.selector: must be a mapping");
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: c, selector: {\"-app\": orders}}"),
                "workloads.orders.runtime.selector.-app: must be a Kubernetes label name, such as app or "
                        + "app.kubernetes.io/name");
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: c, selector: {\"Shop.io/app\": orders}}"),
                "workloads.orders.runtime.selector.Shop.io/app: must be a Kubernetes label name, such as app or "
                        + "app.kubernetes.io/name");
        String valueRule = "must be a Kubernetes label value in quotes: at most 63 of A-Z, a-z, 0-9, -, _ and ., "
                + "starting and ending with a letter or digit";
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: c, selector: {version: 2}}"),
                "workloads.orders.runtime.selector.version: " + valueRule);
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: c, selector: {app: \"orders \"}}"),
                "workloads.orders.runtime.selector.app: " + valueRule);
        assertProblem(workload("runtime: {type: kubernetes, cluster-name: c, selector: {app: " + "v".repeat(64)
                + "}}"), "workloads.orders.runtime.selector.app: " + valueRule);
    }

    @Test
    void twoWorkloadsCannotSelectTheSamePods() throws IOException {
        assertProblem("""
                workloads:
                  orders:
                    queues: [a]
                    runtime: {type: kubernetes, cluster-name: c, namespace: shop, deployment: orders}
                  orders-again:
                    queues: [b]
                    runtime: {type: kubernetes, cluster-name: c, namespace: shop, deployment: orders}
                """, "workloads.orders-again.runtime: selects the same pods as workloads.orders");
        assertProblem("""
                workloads:
                  a:
                    queues: [a]
                    runtime: {type: kubernetes, cluster-name: c, selector: {app: orders, tier: back}}
                  b:
                    queues: [b]
                    runtime: {type: kubernetes, cluster-name: c, selector: {tier: back, app: orders}}
                """, "workloads.b.runtime: selects the same pods as workloads.a");

        WorkloadDefinitions apart = read("""
                version: 1
                workloads:
                  orders:
                    queues: [a]
                    runtime: {type: kubernetes, cluster-name: c, namespace: shop, deployment: orders}
                  orders-eu:
                    queues: [b]
                    region: eu-west-1
                    runtime: {type: kubernetes, cluster-name: c, namespace: shop, deployment: orders}
                  orders-jobs:
                    queues: [c]
                    runtime: {type: kubernetes, cluster-name: c, namespace: shop, statefulset: orders}
                """);
        assertThat(apart.workloads().keySet(), contains("orders", "orders-eu", "orders-jobs"));
    }

    @Test
    void theSameFileGivesEqualDefinitions() throws IOException {
        String content = "version: 1\n" + workload("runtime: {type: kubernetes, cluster-name: c, "
                + "selector: {app: orders}}");

        assertThat(read(content), equalTo(read(content)));
        assertThat(read(content), not(equalTo(read(content.replace("app: orders", "app: payments")))));
    }

    @Test
    void aMissingOrEmptySectionHasNoWorkloads() throws IOException {
        assertThat(read("version: 1\n"), is(WorkloadDefinitions.NONE));
        assertThat(read("version: 1\nworkloads:\n"), is(WorkloadDefinitions.NONE));
        assertThat(read("version: 1\nworkloads: {}\n").workloads().isEmpty(), is(true));
        assertProblem("workloads: [orders]\n", "workloads: must be a mapping");
    }

    private static String clusterRuntime() {
        return "runtime: {type: kubernetes, cluster-name: integration-test, deployment: orders}";
    }

    /** A file's workloads section with one workload, orders, with one queue and these lines. */
    private static String workload(String lines) {
        return "workloads:\n  orders:\n    queues: [orders-incoming]\n    " + lines.replace("\n", "\n    ") + "\n";
    }

    private static String rabbitMq(String queues) {
        return "workloads:\n  jobs:\n    rabbitmq-queues: [" + queues + "]\n    " + clusterRuntime() + "\n";
    }

    private String withNode(String workloadsSection) throws IOException {
        Path token = dir.resolve("dev-token");
        Files.writeString(token, TOKEN);
        Files.setPosixFilePermissions(token, PosixFilePermissions.fromString("rw-------"));
        return "version: 1\nnodes:\n  dev: {url: \"http://127.0.0.1:17070\", token-file: dev-token}\n"
                + workloadsSection;
    }

    private void assertProblem(String section, String message) {
        RuntimeFileException e = assertThrows(RuntimeFileException.class, () -> read("version: 1\n" + section));
        assertThat(e.getMessage(), is(message));
    }

    private WorkloadDefinitions read(String content) throws IOException {
        return RuntimeFile.read(write(content), List.of(nodes, workloads)).section(workloads);
    }

    private Path write(String content) throws IOException {
        Path file = dir.resolve("runtime.yaml");
        Files.writeString(file, content);
        return file;
    }
}
