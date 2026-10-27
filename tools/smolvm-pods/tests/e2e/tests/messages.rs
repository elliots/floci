//! Wakes from Floci. Message consumers in smolvm-pods sleep while their
//! queues are empty, and Floci wakes them while messages wait. Floci runs on
//! the Mac from its JVM build, with persistent storage, and its runtime file
//! declares the test's node and the consumers' workloads. The consumers are
//! the Node.js apps of `workloads/consumer`: SQS consumers on the AWS SDK for
//! JavaScript v3, and an AMQP consumer on amqplib with its automatic
//! recovery, which reach Floci and its broker at the node's gateway. Every
//! call to the runtime goes through `smolvm_pods_e2e::runtime` and
//! `smolvm_pods_e2e::wake`, and every call to Floci through
//! `smolvm_pods_e2e::floci`. tests/e2e/README.md lists the checks.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::hash::{BuildHasher, RandomState};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use smolvm_pods_e2e::floci::{self, Floci, ACCOUNT, REGION};
use smolvm_pods_e2e::json::Json;
use smolvm_pods_e2e::runtime::{Api, Event, PodState};
use smolvm_pods_e2e::wake::{self, ADMISSION_WAIT, IDLE_TIMEOUT, MAX_AWAKE_PODS, POLL_INTERVAL};
use smolvm_pods_e2e::{
    expect, run_checks, step, App, Arch, Cluster, Node, NodeSetup, PodSpec, Runtime,
    COMMAND_TIMEOUT, RUNTIME_CLASS,
};

/// The memory of the test's node.
const NODE_MEMORY_MIB: u32 = 8192;

/// The idle timeout of every VM pod of the test.
const IDLE_ANNOTATION: &str = "10s";
const IDLE: Duration = Duration::from_secs(10);
/// A pod's pause starts at most this long after its last activity: the idle
/// timeout, one poll interval of the node service, and 2 s for the test.
const SLEEP_START: Duration = Duration::from_secs(IDLE.as_secs() + POLL_INTERVAL.as_secs() + 2);
/// How long a pause may take once it started.
const PAUSE_TIME: Duration = Duration::from_secs(30);

/// The `idle-timeout-seconds` of every workload: how long Floci keeps a
/// workload's leases after the last event of the workload's queues.
const LINGER: Duration = Duration::from_secs(60);
/// Once the linger ends, the pause starts within Floci's next decision (1 s)
/// and its read of the queues (3 s), one poll interval of the node service,
/// and 3 s for the clocks.
const AFTER_LINGER: Duration =
    Duration::from_millis(1000 + 3000 + POLL_INTERVAL.as_millis() as u64 + 3000);
/// The clocks of the Mac, the node and the pods agree within this.
const CLOCKS_MS: i64 = 2000;
/// A message starts the wake of its sleeping consumer within this of the
/// start of its send, which includes the start of the AWS CLI or of curl.
const WAKE: Duration = Duration::from_secs(5);
/// A message reaches a sleeping consumer within this: the wake, and the
/// consumer's next poll or its reconnection.
const DELIVERY: Duration = Duration::from_secs(10);
/// The long poll of the SQS consumers: an answer that Floci gives a sleeping
/// consumer's poll waits at most this long in Floci.
const WAIT_TIME: Duration = Duration::from_secs(20);
/// The reset poll check sends its message at most this long after the
/// consumer's poll started, and holds the pod with [`POLL_LEASE`] while it
/// waits for that poll.
const PARKED: Duration = Duration::from_secs(12);
const POLL_LEASE: &str = "e2e-poll";

/// How long the consumer of the first check is watched asleep, and how often.
const QUIET: Duration = Duration::from_secs(180);
const QUIET_LOOK: Duration = Duration::from_secs(10);
/// The work after an acknowledgement: longer than an idle pod takes to
/// start sleeping, shorter than the linger.
const POST_ACK_WORK: Duration = Duration::from_secs(30);
/// The steady stream: a message this often, each handled at once, for
/// longer than two lingers.
const STREAM_GAP: Duration = Duration::from_secs(5);
const STREAM: usize = (2 * LINGER.as_secs() / STREAM_GAP.as_secs()) as usize + 3;
/// The burst: messages this far apart, which end within the linger of the
/// first.
const BURST: usize = 5;
const BURST_GAP: Duration = Duration::from_secs(10);
/// The delay of the delayed message.
const DELAY: Duration = Duration::from_secs(30);
/// The work of the long message, and the visibility timeout of its queue.
const LONG_WORK: Duration = Duration::from_secs(90);
const ORDERS_VISIBILITY: u64 = 150;
/// The replicas of the shared queue's workload and the messages they share.
const SHARED_REPLICAS: usize = 3;
const SHARED_MESSAGES: usize = 6;
const SHARED_WORK_MS: u64 = 3000;
/// The receives after which the queue of `orders` moves a message to its
/// dead-letter queue.
const MAX_RECEIVE_COUNT: usize = 3;
/// The refusal check: how long admission waits for room, and the lease that
/// holds the awake pods.
const ADMISSION_WAIT_VALUE: &str = "10s";
const ROOM_LEASE: &str = "e2e-room";
const LEASE_MAX: Duration = Duration::from_secs(60);
const MAX_REFUSALS: usize = 2;
/// How long the reload check waits with a message and no workload for it.
const UNLISTED: Duration = Duration::from_secs(15);
/// Floci checks the runtime file every 2 s, then reads the queue of the
/// added workload and wakes its consumer.
const RELOADED: Duration = Duration::from_secs(15);

/// The node's name in the runtime file.
const NODE_KEY: &str = "e2e";
/// The workloads, each a Deployment of the consumers' image.
const ORDERS: &str = "orders";
const DRAINER: &str = "drainer";
const SHARED: &str = "shared";
const AMQP: &str = "amqp";
const LATE: &str = "late";
/// The SQS queues: `orders`, its dead-letter queue, `shared` and `late`.
const ORDERS_QUEUE: &str = "orders";
const DLQ: &str = "orders-dlq";
const SHARED_QUEUE: &str = "shared";
const LATE_QUEUE: &str = "late";
/// The RabbitMQ queue of `amqp`, on a broker of Floci's Amazon MQ, and the
/// broker's user, which the consumer and the test connect as.
const AMQP_QUEUE: &str = "tasks";
const RABBITMQ_USER: &str = "e2e";
const USER_ENV: &str = "E2E_RABBITMQ_USER";
const PASSWORD_ENV: &str = "E2E_RABBITMQ_PASSWORD";
/// The SNS topic and the IoT rule that deliver into the queue of `orders`.
const TOPIC: &str = "orders-events";
const RULE: &str = "orders_from_devices";
const DEVICE_TOPIC: &str = "devices/e2e/events";
/// A VM pod that takes the node's room in the refusal check.
const FILLER: &str = "filler";

/// What a workload's consumers read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Source {
    Sqs(&'static str),
    RabbitMq(&'static str),
}

/// A workload of the runtime file, and the Deployment of its consumers.
#[derive(Clone, Copy, Debug)]
struct Workload {
    name: &'static str,
    replicas: usize,
    source: Source,
    /// Floci finds the pods by their labels rather than their Deployment.
    by_labels: bool,
}

const WORKLOADS: [Workload; 5] = [
    Workload {
        name: ORDERS,
        replicas: 1,
        source: Source::Sqs(ORDERS_QUEUE),
        by_labels: false,
    },
    Workload {
        name: DRAINER,
        replicas: 1,
        source: Source::Sqs(DLQ),
        by_labels: false,
    },
    Workload {
        name: SHARED,
        replicas: SHARED_REPLICAS,
        source: Source::Sqs(SHARED_QUEUE),
        by_labels: true,
    },
    Workload {
        name: AMQP,
        replicas: 1,
        source: Source::RabbitMq(AMQP_QUEUE),
        by_labels: false,
    },
    Workload {
        name: LATE,
        replicas: 1,
        source: Source::Sqs(LATE_QUEUE),
        by_labels: false,
    },
];

fn workload(name: &str) -> Workload {
    *WORKLOADS
        .iter()
        .find(|w| w.name == name)
        .unwrap_or_else(|| panic!("no workload {name}"))
}

#[test]
#[ignore = "needs a Mac with smolvm, Docker and Floci's build: SMOLVM_PODS_E2E=1 cargo test -- --ignored"]
fn messages() {
    let cluster = Cluster::start_with(
        "messages",
        NodeSetup {
            memory_mib: Some(NODE_MEMORY_MIB),
            service_port: true,
            ..NodeSetup::default()
        },
    );
    let suite = setup(&cluster);
    run_checks(&[
        ("asleep with an empty queue", &|| {
            expect(asleep_with_an_empty_queue(&suite))
        }),
        ("a message wakes the consumer", &|| {
            expect(a_message_wakes_the_consumer(&suite))
        }),
        ("post-ack work, a stream and a burst", &|| {
            expect(post_ack_work_a_stream_and_a_burst(&suite))
        }),
        ("a delayed message", &|| expect(a_delayed_message(&suite))),
        ("a long message", &|| expect(a_long_message(&suite))),
        ("a dead-letter queue's drainer", &|| {
            expect(a_dead_letter_queues_drainer(&suite))
        }),
        ("SNS and MQTT into SQS", &|| expect(sns_and_mqtt(&suite))),
        ("a poll reset by the sleep", &|| {
            expect(a_reset_poll(&suite))
        }),
        ("replicas share a queue", &|| expect(replicas(&suite))),
        ("a refused wake", &|| a_refused_wake(&suite)),
        ("an AMQP consumer reconnects", &|| {
            expect(an_amqp_consumer_reconnects(&suite))
        }),
        ("an AMQP stream and a burst", &|| {
            expect(an_amqp_stream_and_a_burst(&suite))
        }),
        ("restarts", &|| expect(restarts(&suite))),
        ("a workload added by reload", &|| {
            expect(a_workload_added_by_reload(&suite))
        }),
    ]);
}

/// A consumer pod of a workload.
#[derive(Clone, Debug)]
struct Consumer {
    name: String,
    sandbox: String,
}

/// The broker of the AMQP consumer.
struct Broker {
    id: String,
    password: String,
    /// The Mac ports of its AMQP listener and its management API.
    amqp_port: u16,
    console_port: u16,
}

/// The runtime file, which Floci reads again when it changes.
struct RuntimeFile {
    path: PathBuf,
    service_port: u16,
    namespace: String,
}

impl RuntimeFile {
    /// Writes the file with `workloads`, to a temporary name and then
    /// renamed, so that Floci never reads half of it.
    fn write(&self, workloads: &[Workload], broker: Option<&str>) -> Result<(), String> {
        let yaml = runtime_yaml(self.service_port, &self.namespace, workloads, broker);
        let temp = self.path.with_extension("yaml.new");
        fs::write(&temp, yaml).map_err(|e| format!("{}: {e}", temp.display()))?;
        fs::rename(&temp, &self.path).map_err(|e| format!("{}: {e}", self.path.display()))
    }
}

/// The runtime file: the node, with its token in `node-token` beside the
/// file, and `workloads`, each with the test's linger. A RabbitMQ workload is
/// left out until there is a broker.
fn runtime_yaml(
    service_port: u16,
    namespace: &str,
    workloads: &[Workload],
    broker: Option<&str>,
) -> String {
    let mut yaml = format!(
        "version: 1\nnodes:\n  {NODE_KEY}:\n    url: http://127.0.0.1:{service_port}\n    \
         token-file: node-token\nworkloads:\n"
    );
    for workload in workloads {
        let source = match (workload.source, broker) {
            (Source::Sqs(queue), _) => format!("    queues: [{queue}]\n"),
            (Source::RabbitMq(queue), Some(broker)) => format!(
                "    rabbitmq-queues:\n      - broker-id: {broker}\n        queue: {queue}\n        \
                 username-env: {USER_ENV}\n        password-env: {PASSWORD_ENV}\n"
            ),
            (Source::RabbitMq(_), None) => continue,
        };
        let pods = if workload.by_labels {
            format!("selector: {{app: {}}}", workload.name)
        } else {
            format!("deployment: {}", workload.name)
        };
        yaml += &format!(
            "  {name}:\n    idle-timeout-seconds: {linger}\n{source}    runtime:\n      \
             type: kubernetes\n      node: {NODE_KEY}\n      namespace: {namespace}\n      {pods}\n",
            name = workload.name,
            linger = LINGER.as_secs(),
        );
    }
    yaml
}

/// The Deployment of a workload's consumers, with `env`.
fn deployment(workload: &Workload, namespace: &str, image: &str, env: &[(&str, String)]) -> String {
    let env: String = env
        .iter()
        .map(|(name, value)| format!("        - {{name: {name}, value: {value:?}}}\n"))
        .collect();
    format!(
        r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: {name}
  namespace: {namespace}
spec:
  replicas: {replicas}
  selector:
    matchLabels: {{app: {name}}}
  template:
    metadata:
      labels: {{app: {name}}}
      annotations: {{"{IDLE_TIMEOUT}": "{IDLE_ANNOTATION}"}}
    spec:
      runtimeClassName: {RUNTIME_CLASS}
      automountServiceAccountToken: false
      terminationGracePeriodSeconds: 5
      containers:
      - name: consumer
        image: {image}
        imagePullPolicy: Never
        env:
{env}"#,
        name = workload.name,
        replicas = workload.replicas,
    )
}

/// A body with `marker` and the members `more`, such as `"work_ms":1000`.
fn body(marker: &str, more: &[&str]) -> String {
    let more: String = more.iter().map(|m| format!(",{m}")).collect();
    format!("{{\"marker\":\"{marker}\"{more}}}")
}

/// One line that a consumer printed: `EVENT time=ISO key=value ...`.
#[derive(Clone, Debug, PartialEq)]
struct Line {
    event: String,
    /// When the consumer printed it, in ms since the epoch by the pod's clock.
    time_ms: i64,
    fields: Vec<(String, String)>,
}

impl Line {
    fn parse(text: &str) -> Option<Line> {
        let mut words = text.split_whitespace();
        let event = words.next()?.to_string();
        let time = words.next()?.strip_prefix("time=")?;
        let fields = words
            .filter_map(|word| word.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Some(Line {
            event,
            time_ms: iso_ms(time)?,
            fields,
        })
    }

    fn get(&self, key: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    fn is(&self, event: &str, marker: &str) -> bool {
        self.event == event && self.get("marker") == Some(marker)
    }
}

impl std::fmt::Display for Line {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.event)?;
        for (key, value) in &self.fields {
            write!(f, " {key}={value}")?;
        }
        Ok(())
    }
}

/// Parses the lines of a consumer's log, skipping any that are not its own.
fn parse_lines(text: &str) -> Vec<Line> {
    text.lines().filter_map(Line::parse).collect()
}

/// Ms since the epoch of a UTC time such as `2026-10-19T12:00:00.300Z`.
fn iso_ms(text: &str) -> Option<i64> {
    let (date, time) = text.strip_suffix('Z')?.split_once('T')?;
    let numbers = |text: &str, sep: char| -> Option<Vec<i64>> {
        text.split(sep).map(|n| n.parse().ok()).collect()
    };
    let [year, month, day] = numbers(date, '-')?[..] else {
        return None;
    };
    let (clock, fraction) = time.split_once('.').unwrap_or((time, "0"));
    let [hour, minute, second] = numbers(clock, ':')?[..] else {
        return None;
    };
    let digits: String = fraction.chars().chain("000".chars()).take(3).collect();
    let ms: i64 = digits.parse().ok()?;
    // Days since 1970-01-01 of a proleptic Gregorian date (H. Hinnant).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(((days * 24 + hour) * 60 + minute) * 60_000 + second * 1000 + ms)
}

/// The Mac's clock, in ms since the epoch.
fn mac_ms() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock after 1970");
    i64::try_from(now.as_millis()).expect("ms since the epoch")
}

fn ms(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).expect("a short duration")
}

/// The node's clock less the Mac's, in ms, from the reading with the
/// shortest round trip.
fn clock_offset(node: &Node) -> Result<i64, String> {
    let mut best: Option<(i64, i64)> = None;
    for _ in 0..5 {
        let before = mac_ms();
        let text = node.try_sh("date +%s%3N")?;
        let after = mac_ms();
        let node_ms: i64 = text
            .trim()
            .parse()
            .map_err(|e| format!("the node's clock {text:?}: {e}"))?;
        let offset = node_ms - (before + after) / 2;
        if best.is_none_or(|(rtt, _)| after - before < rtt) {
            best = Some((after - before, offset));
        }
    }
    Ok(best.expect("five readings").1)
}

fn random_hex() -> String {
    format!("{:016x}", RandomState::new().hash_one(0))
}

struct Suite<'a> {
    cluster: &'a Cluster,
    api: Api<'a>,
    floci: RefCell<Floci>,
    runtime: RuntimeFile,
    pods: HashMap<&'static str, Vec<Consumer>>,
    filler: String,
    broker: Broker,
    /// The node's clock less the Mac's, in ms.
    offset_ms: i64,
    /// The event cursor from before the consumers were made.
    start: u64,
}

/// Starts Floci with the runtime file, makes the queues, the topic, the
/// rule and the broker, adds the AMQP workload, and runs the consumers and
/// the filler pod until each consumer has started.
fn setup(cluster: &Cluster) -> Suite<'_> {
    let node = &cluster.node;
    let testapp = cluster.node_path("testapp");
    node.copy_in(&cluster.workload(Arch::Arm64, "testapp"), &testapp);
    node.sh(&format!("chmod +x {testapp}"));
    let api = Api::connect(node, &testapp, Duration::from_secs(60));
    cluster.import_consumer_image();

    let files = cluster.work_dir().join("floci");
    fs::create_dir_all(&files).unwrap_or_else(|e| panic!("{}: {e}", files.display()));
    let token = files.join("node-token");
    let node_token = node.script(&["token"], COMMAND_TIMEOUT);
    fs::write(&token, node_token.trim()).unwrap_or_else(|e| panic!("{}: {e}", token.display()));
    fs::set_permissions(&token, fs::Permissions::from_mode(0o600))
        .unwrap_or_else(|e| panic!("{}: {e}", token.display()));
    let runtime = RuntimeFile {
        path: files.join("runtime.yaml"),
        service_port: cluster
            .service_port()
            .expect("the node publishes its node service"),
        namespace: cluster.namespace.clone(),
    };
    expect(runtime.write(
        &[workload(ORDERS), workload(DRAINER), workload(SHARED)],
        None,
    ));
    let password = format!("{}{}", random_hex(), random_hex());
    let runtime_path = runtime.path.display().to_string();
    let floci = Floci::start(
        &cluster.namespace,
        &[
            ("FLOCI_RUNTIME_CONFIG_FILE", &runtime_path),
            (USER_ENV, RABBITMQ_USER),
            (PASSWORD_ENV, &password),
        ],
    );
    let broker = expect(make_resources(&floci, &password));
    expect(runtime.write(
        &[
            workload(ORDERS),
            workload(DRAINER),
            workload(SHARED),
            workload(AMQP),
        ],
        Some(&broker.id),
    ));

    let start = expect(api.cursor().map_err(|e| e.to_string()));
    let gateway = node.gateway();
    let endpoint = format!("http://{gateway}:{}", floci.port());
    let amqp_url = format!(
        "amqp://{RABBITMQ_USER}:{}@{gateway}:{}",
        broker.password, broker.amqp_port
    );
    let mut manifests: Vec<String> = WORKLOADS
        .iter()
        .map(|workload| {
            let env = match workload.source {
                Source::Sqs(queue) => vec![
                    ("MODE", "sqs".to_string()),
                    ("QUEUE_NAME", queue.to_string()),
                    ("FLOCI_ENDPOINT", endpoint.clone()),
                    ("AWS_REGION", REGION.to_string()),
                    ("AWS_ACCESS_KEY_ID", "test".to_string()),
                    ("AWS_SECRET_ACCESS_KEY", "test".to_string()),
                ],
                Source::RabbitMq(queue) => vec![
                    ("MODE", "amqp".to_string()),
                    ("AMQP_QUEUE", queue.to_string()),
                    ("AMQP_URL", amqp_url.clone()),
                ],
            };
            deployment(
                workload,
                &cluster.namespace,
                &cluster.consumer_image(),
                &env,
            )
        })
        .collect();
    let filler = PodSpec {
        name: FILLER,
        arch: Arch::Arm64,
        runtime: Runtime::SmolvmPods,
        app: App::Hold { mib: 16 },
        containers: &[("app", 8080)],
        annotations: &[(IDLE_TIMEOUT, IDLE_ANNOTATION)],
    };
    manifests.push(filler.manifest(&cluster.namespace, &cluster.image(Arch::Arm64)));
    step("creating the consumers and the filler pod");
    cluster.apply("consumers.yaml", &manifests.join("---\n"));
    let ns = cluster.namespace.as_str();
    for workload in &WORKLOADS {
        let rollout = format!("deployment/{}", workload.name);
        node.kubectl_ok(&["rollout", "status", "-n", ns, &rollout, "--timeout=300s"]);
    }
    node.kubectl_ok(&[
        "wait",
        "-n",
        ns,
        "--for=condition=Ready",
        "pod/filler",
        "--timeout=300s",
    ]);
    let mut pods = HashMap::new();
    for workload in &WORKLOADS {
        let selector = format!("app={}", workload.name);
        let names = node.kubectl_ok(&[
            "get",
            "pods",
            "-n",
            ns,
            "-l",
            &selector,
            "-o",
            "jsonpath={.items[*].metadata.name}",
        ]);
        let consumers: Vec<Consumer> = names
            .split_whitespace()
            .map(|name| Consumer {
                name: name.to_string(),
                sandbox: expect(cluster.ready_sandbox(name)),
            })
            .collect();
        assert_eq!(
            consumers.len(),
            workload.replicas,
            "pods of {}: {names}",
            workload.name
        );
        pods.insert(workload.name, consumers);
    }
    let filler = expect(cluster.ready_sandbox(FILLER));
    let suite = Suite {
        cluster,
        api,
        floci: RefCell::new(floci),
        runtime,
        pods,
        filler,
        broker,
        offset_ms: 0,
        start,
    };
    for workload in &WORKLOADS {
        let started = match workload.source {
            Source::Sqs(_) => "ready",
            Source::RabbitMq(_) => "connected",
        };
        for pod in suite.pods_of(workload.name) {
            expect(suite.wait_for(pod, Duration::from_secs(120), |l| l.event == started));
            step(&format!("consumer {} started", pod.name));
        }
    }
    let offset_ms = expect(clock_offset(node));
    step(&format!(
        "the node's clock is {offset_ms} ms ahead of the Mac's"
    ));
    Suite { offset_ms, ..suite }
}

/// Makes the queues, the topic and its subscription, the IoT rule, and the
/// broker with its user, and waits until the broker runs.
fn make_resources(floci: &Floci, password: &str) -> Result<Broker, String> {
    step("making the queues, the topic, the IoT rule and the broker in Floci");
    let dlq_arn = arn("sqs", DLQ);
    let redrive = format!(
        "{{\"deadLetterTargetArn\":\"{dlq_arn}\",\"maxReceiveCount\":\"{MAX_RECEIVE_COUNT}\"}}"
    );
    let orders_attributes =
        format!("{{\"VisibilityTimeout\":\"{ORDERS_VISIBILITY}\",\"RedrivePolicy\":{redrive:?}}}");
    for (queue, attributes) in [
        (DLQ, None),
        (ORDERS_QUEUE, Some(orders_attributes.as_str())),
        (SHARED_QUEUE, Some("{\"VisibilityTimeout\":\"60\"}")),
        (LATE_QUEUE, Some("{\"VisibilityTimeout\":\"60\"}")),
    ] {
        let mut args = vec!["sqs", "create-queue", "--queue-name", queue];
        if let Some(attributes) = attributes {
            args.extend(["--attributes", attributes]);
        }
        floci.aws(&args)?;
    }
    let topic = floci.aws(&["sns", "create-topic", "--name", TOPIC])?;
    let topic = topic
        .get("TopicArn")
        .as_str()
        .ok_or("no TopicArn")?
        .to_string();
    floci.aws(&[
        "sns",
        "subscribe",
        "--topic-arn",
        &topic,
        "--protocol",
        "sqs",
        "--notification-endpoint",
        &arn("sqs", ORDERS_QUEUE),
    ])?;
    floci.aws(&[
        "iot",
        "describe-endpoint",
        "--endpoint-type",
        "iot:Data-ATS",
    ])?;
    let rule = format!(
        "{{\"sql\":\"SELECT * FROM '{DEVICE_TOPIC}'\",\"awsIotSqlVersion\":\"2016-03-23\",\
         \"ruleDisabled\":false,\"actions\":[{{\"sqs\":{{\"queueUrl\":\"{}\",\
         \"roleArn\":\"arn:aws:iam::{ACCOUNT}:role/iot-to-sqs\",\"useBase64\":false}}}}]}}",
        queue_url(floci, ORDERS_QUEUE)
    );
    floci.aws(&[
        "iot",
        "create-topic-rule",
        "--rule-name",
        RULE,
        "--topic-rule-payload",
        &rule,
    ])?;
    let users = format!("[{{\"Username\":\"{RABBITMQ_USER}\",\"Password\":\"{password}\"}}]");
    let created = floci.aws(&[
        "mq",
        "create-broker",
        "--broker-name",
        "e2e",
        "--engine-type",
        "RABBITMQ",
        "--engine-version",
        "3.13",
        "--deployment-mode",
        "SINGLE_INSTANCE",
        "--host-instance-type",
        "mq.t3.micro",
        "--no-publicly-accessible",
        "--auto-minor-version-upgrade",
        "--users",
        &users,
    ])?;
    let id = created
        .get("BrokerId")
        .as_str()
        .ok_or("no BrokerId")?
        .to_string();
    let deadline = Instant::now() + Duration::from_secs(300);
    let described = loop {
        let described = floci.aws(&["mq", "describe-broker", "--broker-id", &id])?;
        match described.get("BrokerState").as_str() {
            Some("RUNNING") => break described,
            state if Instant::now() >= deadline => {
                return Err(format!("broker {id} is {state:?} after 300 s"))
            }
            _ => thread::sleep(Duration::from_secs(2)),
        }
    };
    let instance = &described
        .get("BrokerInstances")
        .as_array()
        .ok_or("no BrokerInstances")?[0];
    let port_of = |url: Option<&str>| -> Result<u16, String> {
        url.and_then(|url| url.rsplit(':').next())
            .and_then(|port| port.trim_end_matches('/').parse().ok())
            .ok_or_else(|| format!("no port in the broker's endpoints: {instance}"))
    };
    let amqp_port = port_of(
        instance
            .get("Endpoints")
            .as_array()
            .and_then(|e| e[0].as_str()),
    )?;
    let console_port = port_of(instance.get("ConsoleURL").as_str())?;
    step(&format!(
        "broker {id} runs: AMQP on port {amqp_port}, management on port {console_port}"
    ));
    Ok(Broker {
        id,
        password: password.to_string(),
        amqp_port,
        console_port,
    })
}

fn arn(service: &str, name: &str) -> String {
    format!("arn:aws:{service}:{REGION}:{ACCOUNT}:{name}")
}

fn queue_url(floci: &Floci, queue: &str) -> String {
    format!("{}/{ACCOUNT}/{queue}", floci.url())
}

impl Suite<'_> {
    fn node(&self) -> &Node {
        &self.cluster.node
    }

    fn pods_of(&self, workload: &str) -> &[Consumer] {
        &self.pods[workload]
    }

    /// The one pod of a workload.
    fn pod(&self, workload: &str) -> &Consumer {
        &self.pods_of(workload)[0]
    }

    fn aws(&self, args: &[&str]) -> Result<Json, String> {
        self.floci.borrow().aws(args)
    }

    /// Sends `body` to the SQS queue, delayed by `delay`, and returns when
    /// the send started, by the node's clock.
    fn send(&self, queue: &str, body: &str, delay: Option<Duration>) -> Result<i64, String> {
        let started = self.now_ms();
        let url = queue_url(&self.floci.borrow(), queue);
        let delay = delay.map(|d| d.as_secs().to_string());
        let mut args = vec![
            "sqs",
            "send-message",
            "--queue-url",
            &url,
            "--message-body",
            body,
        ];
        if let Some(delay) = &delay {
            args.extend(["--delay-seconds", delay]);
        }
        self.aws(&args)?;
        step(&format!("sent {body} to {queue}"));
        Ok(started)
    }

    /// The queue's visible, in-flight and delayed messages.
    fn counts(&self, queue: &str) -> Result<[u64; 3], String> {
        let url = queue_url(&self.floci.borrow(), queue);
        let names = [
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ];
        let mut args = vec!["sqs", "get-queue-attributes", "--queue-url", &url];
        args.push("--attribute-names");
        args.extend(names);
        let attributes = self.aws(&args)?;
        let count = |name: &str| -> Result<u64, String> {
            attributes
                .get("Attributes")
                .get(name)
                .as_str()
                .and_then(|n| n.parse().ok())
                .ok_or_else(|| format!("{name} of {queue}: {attributes}"))
        };
        Ok([count(names[0])?, count(names[1])?, count(names[2])?])
    }

    /// Fails unless the queue has no message, visible, in flight or delayed.
    fn check_empty(&self, queue: &str) -> Result<(), String> {
        match self.counts(queue)? {
            [0, 0, 0] => Ok(()),
            counts => Err(format!(
                "queue {queue} has {} visible, {} in flight and {} delayed messages",
                counts[0], counts[1], counts[2]
            )),
        }
    }

    /// The node's clock now, in ms since the epoch.
    fn now_ms(&self) -> i64 {
        mac_ms() + self.offset_ms
    }

    fn cursor(&self) -> Result<u64, String> {
        self.api.cursor().map_err(|e| e.to_string())
    }

    /// The events of the pod after `cursor`.
    fn events(&self, cursor: u64, pod: &str) -> Result<Vec<Event>, String> {
        self.api.events_of(cursor, pod)
    }

    fn state(&self, pod: &Consumer) -> Result<PodState, String> {
        self.api.state(&pod.sandbox)
    }

    /// The ids of the API leases on the pod.
    fn leases(&self, sandbox: &str) -> Result<Vec<String>, String> {
        let pod = self.api.get(sandbox).map_err(|e| e.to_string())?;
        Ok(pod
            .leases
            .iter()
            .filter(|lease| lease.kind == "api")
            .map(|lease| lease.id.clone())
            .collect())
    }

    /// Fails unless Floci holds the workload's lease on the pod.
    fn check_floci_lease(&self, pod: &Consumer, workload: &str) -> Result<(), String> {
        let leases = self.leases(&pod.sandbox)?;
        let lease = format!("floci:{workload}");
        if leases.contains(&lease) {
            Ok(())
        } else {
            Err(format!(
                "pod {} has the leases {leases:?}, not {lease}",
                pod.name
            ))
        }
    }

    /// The consumer's lines so far.
    fn lines(&self, pod: &Consumer) -> Result<Vec<Line>, String> {
        let ns = &self.cluster.namespace;
        let logs = ["logs", "-n", ns, &pod.name, "-c", "consumer"];
        let text = self.node().try_kubectl(&logs, COMMAND_TIMEOUT)?;
        Ok(parse_lines(&text))
    }

    /// The consumer's lines of `event` for the message `marker`.
    fn lines_of(&self, pod: &Consumer, event: &str, marker: &str) -> Result<Vec<Line>, String> {
        let lines = self.lines(pod)?;
        Ok(lines.into_iter().filter(|l| l.is(event, marker)).collect())
    }

    /// Waits up to `timeout` for the consumer to print a line that `found`
    /// accepts, and returns the first.
    fn wait_for(
        &self,
        pod: &Consumer,
        timeout: Duration,
        found: impl Fn(&Line) -> bool,
    ) -> Result<Line, String> {
        let deadline = Instant::now() + timeout;
        loop {
            let lines = self.lines(pod)?;
            if let Some(line) = lines.iter().find(|l| found(l)) {
                return Ok(line.clone());
            }
            if Instant::now() >= deadline {
                let last: Vec<String> = lines.iter().rev().take(8).map(Line::to_string).collect();
                return Err(format!(
                    "consumer {} printed no such line within {timeout:?}; its last lines, newest \
                     first: {last:?}",
                    pod.name
                ));
            }
            thread::sleep(Duration::from_secs(1));
        }
    }

    /// Waits for the consumer's line of `event` for the message `marker`.
    fn wait_for_message(
        &self,
        pod: &Consumer,
        event: &str,
        marker: &str,
        timeout: Duration,
    ) -> Result<Line, String> {
        self.wait_for(pod, timeout, |l| l.is(event, marker))
            .map_err(|e| format!("{event} {marker}: {e}"))
    }

    /// Fails unless the consumer received the message `marker` once, as its
    /// first receive, and returns that line.
    fn received_once(&self, pod: &Consumer, marker: &str) -> Result<Line, String> {
        let received = self.lines_of(pod, "received", marker)?;
        match &received[..] {
            [line] if line.get("count") == Some("1") => Ok(line.clone()),
            _ => Err(format!(
                "consumer {} should have received {marker} once, with the receive count 1: {}",
                pod.name,
                lines_text(&received)
            )),
        }
    }

    /// The pod's one resume after `cursor`, which must be Floci's wake for
    /// messages in `resource`.
    fn woken_by_floci(&self, cursor: u64, pod: &Consumer, resource: &str) -> Result<Event, String> {
        let events = self.events(cursor, &pod.sandbox)?;
        let resumes: Vec<&Event> = events.iter().filter(|e| e.is_resume()).collect();
        let [resume] = resumes[..] else {
            return Err(format!(
                "pod {} should have resumed once since event {cursor}: {}",
                pod.name,
                events_text(&events)
            ));
        };
        let queue = resume.raw.get("reason_detail").get("queue").as_str();
        if resume.reason != "aws" || resume.source != "floci" || queue != Some(resource) {
            return Err(format!(
                "pod {} should have been woken by Floci for {resource} (reason aws, source \
                 floci): {}",
                pod.name, resume.raw
            ));
        }
        Ok(resume.clone())
    }

    /// The pod's one resume after `cursor`, Floci's wake for messages in
    /// `resource`, which must start within [`WAKE`] of `sent_ms`; returns
    /// how long after `sent_ms` it started.
    fn woken_within(
        &self,
        cursor: u64,
        pod: &Consumer,
        resource: &str,
        sent_ms: i64,
    ) -> Result<i64, String> {
        let resume = self.woken_by_floci(cursor, pod, resource)?;
        let woke = event_ms(&resume)? - sent_ms;
        if woke > ms(WAKE) {
            return Err(format!(
                "pod {} started to wake {woke} ms after the send, more than {WAKE:?}",
                pod.name
            ));
        }
        step(&format!(
            "pod {} started to wake {woke} ms after the send",
            pod.name
        ));
        Ok(woke)
    }

    /// Fails unless no pause of the pod started after `cursor` and before
    /// `until_ms` by the node's clock.
    fn check_no_pause_before(
        &self,
        cursor: u64,
        pod: &Consumer,
        until_ms: i64,
    ) -> Result<(), String> {
        let events = self.events(cursor, &pod.sandbox)?;
        let mut pauses = Vec::new();
        for event in events.iter().filter(|e| wake::is_pause(e)) {
            if event_ms(event)? < until_ms {
                pauses.push(event.raw.to_string());
            }
        }
        if pauses.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "pod {} paused while it had work: {pauses:?}",
                pod.name
            ))
        }
    }

    /// Waits until the pod is asleep: it sleeps by itself once no lease of
    /// Floci keeps it awake, or else the test hibernates it.
    fn ensure_asleep(&self, pod: &Consumer) -> Result<(), String> {
        if self.state(pod)? == PodState::Asleep {
            return Ok(());
        }
        let wait = LINGER + AFTER_LINGER + PAUSE_TIME;
        if wake::watch_sleep(self.node(), &pod.sandbox, wait).is_ok() {
            return Ok(());
        }
        self.api.hibernate(&pod.sandbox).map(drop).map_err(|e| {
            format!(
                "pod {} did not sleep within {wait:?}, and a hibernate failed: {e}",
                pod.name
            )
        })
    }

    /// Waits until the pod sleeps by itself once Floci's lease ends, and
    /// fails unless its last pause after `cursor` was for being idle and
    /// started the linger after `last_ms`, the time by the node's clock of
    /// the last event of the workload's queues, within [`AFTER_LINGER`].
    fn sleeps_after_linger(&self, pod: &Consumer, cursor: u64, last_ms: i64) -> Result<(), String> {
        let latest = last_ms + ms(LINGER + AFTER_LINGER);
        let wait =
            Duration::from_millis(u64::try_from(latest - self.now_ms()).unwrap_or(0)) + PAUSE_TIME;
        wake::watch_sleep(self.node(), &pod.sandbox, wait)
            .map_err(|e| format!("pod {} did not sleep within {wait:?}: {e}", pod.name))?;
        let events = self.events(cursor, &pod.sandbox)?;
        let pause = events
            .iter()
            .rev()
            .find(|e| wake::is_pause(e))
            .ok_or_else(|| {
                format!(
                    "pod {} is asleep, but no pause started since event {cursor}",
                    pod.name
                )
            })?;
        if pause.reason != "idle" {
            return Err(format!(
                "pod {} paused with the reason {}, not idle: {}",
                pod.name, pause.reason, pause.raw
            ));
        }
        let paused = event_ms(pause)?;
        let after = paused - last_ms;
        if paused < last_ms + ms(LINGER) - CLOCKS_MS || paused > latest {
            return Err(format!(
                "pod {} started to sleep {after} ms after the last event of its queue, not within \
                 the linger of {LINGER:?} and {AFTER_LINGER:?} more",
                pod.name
            ));
        }
        if !self.leases(&pod.sandbox)?.is_empty() {
            return Err(format!("pod {} still has leases while it sleeps", pod.name));
        }
        step(&format!(
            "pod {} started to sleep {:.1} s after the last event of its queue",
            pod.name,
            after as f64 / 1000.0
        ));
        Ok(())
    }

    /// The RabbitMQ management API of the broker, through curl.
    fn rabbitmq(&self, method: &str, path: &str, body: Option<&str>) -> Result<Json, String> {
        let url = format!("http://127.0.0.1:{}{path}", self.broker.console_port);
        let user = format!("{RABBITMQ_USER}:{}", self.broker.password);
        let mut curl = Command::new("curl");
        curl.args(["-sS", "-f", "-m", "10", "-u", &user, "-X", method])
            .args(["-H", "content-type: application/json"]);
        if let Some(body) = body {
            curl.args(["-d", body]);
        }
        let output = curl.arg(&url).output().map_err(|e| format!("curl: {e}"))?;
        if !output.status.success() {
            return Err(format!(
                "{method} {path}: {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        Json::parse(&String::from_utf8_lossy(&output.stdout))
            .map_err(|e| format!("{method} {path}: {e}"))
    }

    /// The consumers, ready and unacknowledged messages of the AMQP queue.
    fn amqp_queue(&self) -> Result<[u64; 3], String> {
        let queue = self.rabbitmq("GET", &format!("/api/queues/%2F/{AMQP_QUEUE}"), None)?;
        let count = |name: &str| {
            queue
                .get(name)
                .as_u64()
                .ok_or_else(|| format!("{name} of queue {AMQP_QUEUE}: {queue}"))
        };
        Ok([
            count("consumers")?,
            count("messages_ready")?,
            count("messages_unacknowledged")?,
        ])
    }

    /// Publishes a persistent `body` to the AMQP queue through the default
    /// exchange, and returns when the publish started, by the node's clock.
    fn publish(&self, body: &str) -> Result<i64, String> {
        let started = self.now_ms();
        let publish = format!(
            "{{\"properties\":{{\"delivery_mode\":2}},\"routing_key\":\"{AMQP_QUEUE}\",\
             \"payload\":{body:?},\"payload_encoding\":\"string\"}}"
        );
        let routed = self.rabbitmq(
            "POST",
            "/api/exchanges/%2F/amq.default/publish",
            Some(&publish),
        )?;
        if routed.get("routed").as_bool() != Some(true) {
            return Err(format!(
                "the broker did not route the message to {AMQP_QUEUE}: {routed}"
            ));
        }
        step(&format!("published {body} to {AMQP_QUEUE}"));
        Ok(started)
    }

    /// Waits until the AMQP queue has no ready and no unacknowledged message.
    fn amqp_empty(&self) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match self.amqp_queue()? {
                [_, 0, 0] => return Ok(()),
                counts if Instant::now() >= deadline => {
                    return Err(format!("queue {AMQP_QUEUE} still has messages: {counts:?}"))
                }
                _ => thread::sleep(Duration::from_secs(2)),
            }
        }
    }
}

/// How a check gives messages to a workload's one consumer.
struct Feed<'a> {
    pod: &'a Consumer,
    /// The queue as Floci's wakes name it.
    resource: String,
    /// Sends a body and returns when the send started, by the node's clock.
    send: &'a dyn Fn(&str) -> Result<i64, String>,
    /// The consumer's line when it is done with a message, and its line at
    /// the last event of the queue for the message: the delete of an SQS
    /// message, or the delivery of an AMQP one, since RabbitMQ tells nobody
    /// of an acknowledgement.
    done: &'static str,
    last: &'static str,
}

impl Feed<'_> {
    /// Sends `count` messages `gap` apart, named `prefix-N`, and waits for the
    /// consumer to be done with each before the next. Returns when the first
    /// send started and the time of the last event for each message.
    fn feed(
        &self,
        suite: &Suite,
        prefix: &str,
        count: usize,
        gap: Duration,
    ) -> Result<(i64, Vec<i64>), String> {
        let start = Instant::now();
        let mut first = None;
        let mut last = Vec::new();
        for i in 1..=count {
            let at = gap * u32::try_from(i - 1).expect("a few messages");
            thread::sleep(at.saturating_sub(start.elapsed()));
            let marker = format!("{prefix}-{i}");
            let sent = (self.send)(&body(&marker, &[]))?;
            first.get_or_insert(sent);
            suite.wait_for_message(
                self.pod,
                self.done,
                &marker,
                DELIVERY + Duration::from_secs(20),
            )?;
            let line = suite.wait_for_message(self.pod, self.last, &marker, Duration::ZERO)?;
            last.push(line.time_ms);
        }
        Ok((first.expect("a message"), last))
    }

    /// Fails unless the pod resumed once and paused once after `cursor`.
    fn one_wake_one_pause(&self, suite: &Suite, cursor: u64, what: &str) -> Result<(), String> {
        let events = suite.events(cursor, &self.pod.sandbox)?;
        let resumes = events.iter().filter(|e| e.is_resume()).count();
        let pauses = events.iter().filter(|e| wake::is_pause(e)).count();
        if resumes != 1 || pauses != 1 {
            return Err(format!(
                "{what} should cost one wake and one pause, not {resumes} and {pauses}: {}",
                events_text(&events)
            ));
        }
        Ok(())
    }

    /// A steady stream of [`STREAM`] messages [`STREAM_GAP`] apart, which the
    /// consumer handles at once, keeps it awake with no pause, and it sleeps
    /// once the linger after the last ends. Then a burst of [`BURST`]
    /// messages [`BURST_GAP`] apart costs one wake and one pause, the pause
    /// the linger after the last message. Returns how long after their first
    /// send the stream and the burst started to wake the pod.
    fn stream_and_burst(&self, suite: &Suite) -> Result<[i64; 2], String> {
        let pod = self.pod;
        suite.ensure_asleep(pod)?;
        let cursor = suite.cursor()?;
        let (sent, last) = self.feed(suite, "stream", STREAM, STREAM_GAP)?;
        let stream_wake = suite.woken_within(cursor, pod, &self.resource, sent)?;
        let last = *last.last().expect("messages");
        suite.check_no_pause_before(cursor, pod, last)?;
        if suite.state(pod)? != PodState::Awake {
            return Err(format!("pod {} is not awake after the stream", pod.name));
        }
        step(&format!(
            "{STREAM} messages {STREAM_GAP:?} apart kept pod {} awake for {:.1} s with no pause",
            pod.name,
            (last - sent) as f64 / 1000.0
        ));
        suite.sleeps_after_linger(pod, cursor, last)?;
        self.one_wake_one_pause(suite, cursor, "the stream")?;

        let cursor = suite.cursor()?;
        let (sent, last) = self.feed(suite, "burst", BURST, BURST_GAP)?;
        let burst_wake = suite.woken_within(cursor, pod, &self.resource, sent)?;
        suite.sleeps_after_linger(pod, cursor, *last.last().expect("messages"))?;
        self.one_wake_one_pause(
            suite,
            cursor,
            &format!("{BURST} messages {BURST_GAP:?} apart"),
        )?;
        Ok([stream_wake, burst_wake])
    }
}

/// When an event happened, by the node's clock, in ms since the epoch.
fn event_ms(event: &Event) -> Result<i64, String> {
    event
        .raw
        .get("time")
        .as_str()
        .and_then(iso_ms)
        .ok_or_else(|| format!("event without a time: {}", event.raw))
}

fn events_text(events: &[Event]) -> String {
    let events: Vec<String> = events.iter().map(|e| e.raw.to_string()).collect();
    format!("{events:?}")
}

fn lines_text(lines: &[Line]) -> String {
    let lines: Vec<String> = lines.iter().map(Line::to_string).collect();
    format!("{lines:?}")
}

/// 1. With its queue empty, the SQS consumer, which long-polls, sleeps by
///    itself and stays asleep for 3 minutes, while Floci makes no read of
///    its queue.
fn asleep_with_an_empty_queue(suite: &Suite) -> Result<(), String> {
    let pod = suite.pod(ORDERS);
    suite.check_empty(ORDERS_QUEUE)?;
    let limit = SLEEP_START + PAUSE_TIME;
    wake::watch_sleep(suite.node(), &pod.sandbox, limit).map_err(|e| {
        format!(
            "pod {} did not sleep by itself within {limit:?}: {e}",
            pod.name
        )
    })?;
    let events = suite.events(suite.start, &pod.sandbox)?;
    match events.iter().rev().find(|e| e.to == "asleep") {
        Some(e) if wake::is_idle_sleep(e) => {}
        _ => {
            return Err(format!(
                "pod {} should have slept by itself: {}",
                pod.name,
                events_text(&events)
            ))
        }
    }
    let cursor = suite.cursor()?;
    let polls = suite
        .lines(pod)?
        .iter()
        .filter(|l| l.event == "poll")
        .count();
    let log_start = suite.floci.borrow().log().len();
    let start = Instant::now();
    while start.elapsed() < QUIET {
        thread::sleep(QUIET_LOOK);
        let status = suite.api.get(&pod.sandbox).map_err(|e| e.to_string())?;
        if status.state != PodState::Asleep || status.vm_pid.is_some() {
            return Err(format!(
                "pod {} woke after {:?} asleep: {} with VM process {:?}",
                pod.name,
                start.elapsed(),
                status.state,
                status.vm_pid
            ));
        }
    }
    let events = suite.events(cursor, &pod.sandbox)?;
    if !events.is_empty() {
        return Err(format!(
            "pod {} changed state while its queue was empty: {}",
            pod.name,
            events_text(&events)
        ));
    }
    let read = format!("Floci reads the queues of workload {ORDERS}");
    let log = suite.floci.borrow().log();
    if let Some(line) = log
        .get(log_start..)
        .unwrap_or_default()
        .lines()
        .find(|l| l.trim_end().ends_with(&read))
    {
        return Err(format!(
            "Floci read the queues of {ORDERS} while its pod slept with its queue empty: {line}"
        ));
    }
    let lines = suite.lines(pod)?;
    if polls == 0
        || lines
            .iter()
            .any(|l| l.event != "poll" && l.event != "polled" && l.event != "ready")
    {
        return Err(format!(
            "consumer {} should only have polled its empty queue: {}",
            pod.name,
            lines_text(&lines)
        ));
    }
    if !suite.leases(&pod.sandbox)?.is_empty() {
        return Err(format!("pod {} has leases with its queue empty", pod.name));
    }
    step(&format!(
        "pod {} slept for {QUIET:?} after {polls} polls of its empty queue, with no read by Floci",
        pod.name
    ));
    Ok(())
}

/// 2. A message wakes the sleeping consumer with Floci's lease within a
///    few seconds of its send, and the consumer sleeps by itself once the
///    linger after the message ends.
fn a_message_wakes_the_consumer(suite: &Suite) -> Result<(), String> {
    let pod = suite.pod(ORDERS);
    suite.ensure_asleep(pod)?;
    let cursor = suite.cursor()?;
    let sent = suite.send(ORDERS_QUEUE, &body("one", &[]), None)?;
    let deleted =
        suite.wait_for_message(pod, "deleted", "one", DELIVERY + Duration::from_secs(20))?;
    suite.check_floci_lease(pod, ORDERS)?;
    let received = suite.received_once(pod, "one")?;
    suite.woken_within(cursor, pod, &arn("sqs", ORDERS_QUEUE), sent)?;
    let took = received.time_ms - sent;
    if took > ms(DELIVERY) {
        return Err(format!(
            "the message reached the sleeping consumer after {took} ms, more than {DELIVERY:?}"
        ));
    }
    step(&format!(
        "the message reached the sleeping consumer after {took} ms"
    ));
    suite.sleeps_after_linger(pod, cursor, deleted.time_ms)?;
    suite.check_empty(ORDERS_QUEUE)
}

/// 3. Work after the acknowledgement finishes while the linger keeps the pod
///    awake; a steady stream of messages keeps the consumer awake with no
///    pause; and a burst costs one wake and one pause.
fn post_ack_work_a_stream_and_a_burst(suite: &Suite) -> Result<(), String> {
    let pod = suite.pod(ORDERS);
    suite.ensure_asleep(pod)?;
    let cursor = suite.cursor()?;
    let after_ms = format!("\"after_ms\":{}", POST_ACK_WORK.as_millis());
    let sent = suite.send(ORDERS_QUEUE, &body("post-ack", &[&after_ms]), None)?;
    let done = suite.wait_for_message(
        pod,
        "after-done",
        "post-ack",
        DELIVERY + POST_ACK_WORK + Duration::from_secs(20),
    )?;
    let deleted = suite.wait_for_message(pod, "deleted", "post-ack", Duration::ZERO)?;
    if done.time_ms - deleted.time_ms < ms(POST_ACK_WORK) {
        return Err(format!(
            "the work after the acknowledgement took {} ms",
            done.time_ms - deleted.time_ms
        ));
    }
    suite.check_no_pause_before(cursor, pod, done.time_ms)?;
    step("the work after the acknowledgement finished awake");
    suite.woken_within(cursor, pod, &arn("sqs", ORDERS_QUEUE), sent)?;
    suite.sleeps_after_linger(pod, cursor, deleted.time_ms)?;

    let send = |body: &str| suite.send(ORDERS_QUEUE, body, None);
    let feed = Feed {
        pod,
        resource: arn("sqs", ORDERS_QUEUE),
        send: &send,
        done: "deleted",
        last: "deleted",
    };
    feed.stream_and_burst(suite).map(drop)?;
    suite.check_empty(ORDERS_QUEUE)
}

/// 4. A delayed message wakes the consumer once its delay ends, not before.
fn a_delayed_message(suite: &Suite) -> Result<(), String> {
    let pod = suite.pod(ORDERS);
    suite.ensure_asleep(pod)?;
    let cursor = suite.cursor()?;
    let sent = suite.send(ORDERS_QUEUE, &body("delayed", &[]), Some(DELAY))?;
    let deleted = suite.wait_for_message(
        pod,
        "deleted",
        "delayed",
        DELAY + DELIVERY + Duration::from_secs(20),
    )?;
    let resume = suite.woken_by_floci(cursor, pod, &arn("sqs", ORDERS_QUEUE))?;
    let woke = event_ms(&resume)? - sent;
    if woke < ms(DELAY) - CLOCKS_MS || woke > ms(DELAY + WAKE) {
        return Err(format!(
            "the delayed message woke the consumer {woke} ms after it was sent, with a delay of {DELAY:?}"
        ));
    }
    step(&format!(
        "the message delayed by {DELAY:?} woke the consumer after {woke} ms"
    ));
    suite.received_once(pod, "delayed")?;
    suite.sleeps_after_linger(pod, cursor, deleted.time_ms)
}

/// 5. A message whose work takes 90 s keeps the consumer awake until it is
///    done, and is received once.
fn a_long_message(suite: &Suite) -> Result<(), String> {
    let pod = suite.pod(ORDERS);
    suite.ensure_asleep(pod)?;
    let cursor = suite.cursor()?;
    let work_ms = format!("\"work_ms\":{}", LONG_WORK.as_millis());
    suite.send(ORDERS_QUEUE, &body("long", &[&work_ms]), None)?;
    suite.wait_for_message(pod, "working", "long", DELIVERY + Duration::from_secs(20))?;
    thread::sleep(LONG_WORK / 2);
    if suite.state(pod)? != PodState::Awake {
        return Err(format!(
            "pod {} is not awake halfway through its work",
            pod.name
        ));
    }
    suite.check_floci_lease(pod, ORDERS)?;
    let deleted =
        suite.wait_for_message(pod, "deleted", "long", LONG_WORK + Duration::from_secs(30))?;
    let received = suite.received_once(pod, "long")?;
    if deleted.time_ms - received.time_ms < ms(LONG_WORK) {
        return Err(format!(
            "the work took {} ms",
            deleted.time_ms - received.time_ms
        ));
    }
    suite.woken_by_floci(cursor, pod, &arn("sqs", ORDERS_QUEUE))?;
    suite.check_no_pause_before(cursor, pod, deleted.time_ms)?;
    suite.sleeps_after_linger(pod, cursor, deleted.time_ms)
}

/// 6. A message that the consumer fails three times moves to the dead-letter
///    queue, whose drainer Floci wakes.
fn a_dead_letter_queues_drainer(suite: &Suite) -> Result<(), String> {
    let orders = suite.pod(ORDERS);
    let drainer = suite.pod(DRAINER);
    suite.ensure_asleep(orders)?;
    suite.ensure_asleep(drainer)?;
    let cursor = suite.cursor()?;
    let fail_in = format!("\"fail_in\":\"{ORDERS_QUEUE}\"");
    suite.send(ORDERS_QUEUE, &body("dead", &[&fail_in]), None)?;
    let deleted = suite.wait_for_message(drainer, "deleted", "dead", Duration::from_secs(120))?;
    let failed = suite.lines_of(orders, "failed", "dead")?;
    let counts: Vec<&str> = failed.iter().filter_map(|l| l.get("count")).collect();
    let expected: Vec<String> = (1..=MAX_RECEIVE_COUNT).map(|n| n.to_string()).collect();
    if counts != expected {
        return Err(format!(
            "consumer {} should have failed the message {MAX_RECEIVE_COUNT} times: {}",
            orders.name,
            lines_text(&failed)
        ));
    }
    if !suite.lines_of(orders, "deleted", "dead")?.is_empty() {
        return Err(format!(
            "consumer {} deleted the failed message",
            orders.name
        ));
    }
    let drained = suite.lines_of(drainer, "received", "dead")?;
    if drained.len() != 1 {
        return Err(format!(
            "the drainer should have received the message once: {}",
            lines_text(&drained)
        ));
    }
    suite.woken_by_floci(cursor, orders, &arn("sqs", ORDERS_QUEUE))?;
    suite.woken_by_floci(cursor, drainer, &arn("sqs", DLQ))?;
    let last_failure = failed.last().expect("failures").time_ms;
    suite.sleeps_after_linger(orders, cursor, last_failure)?;
    suite.sleeps_after_linger(drainer, cursor, deleted.time_ms)?;
    suite.check_empty(DLQ)?;
    suite.check_empty(ORDERS_QUEUE)
}

/// 7. A message published to the SNS topic, and one published over MQTT to
///    a topic of the IoT rule, each wake the consumer of the queue that they
///    reach.
fn sns_and_mqtt(suite: &Suite) -> Result<(), String> {
    let pod = suite.pod(ORDERS);
    suite.ensure_asleep(pod)?;
    let cursor = suite.cursor()?;
    let topic = arn("sns", TOPIC);
    let sent = suite.now_ms();
    suite.aws(&[
        "sns",
        "publish",
        "--topic-arn",
        &topic,
        "--message",
        &body("sns", &[]),
    ])?;
    let deleted =
        suite.wait_for_message(pod, "deleted", "sns", DELIVERY + Duration::from_secs(20))?;
    suite.woken_within(cursor, pod, &arn("sqs", ORDERS_QUEUE), sent)?;
    suite.sleeps_after_linger(pod, cursor, deleted.time_ms)?;
    step("a message published to the SNS topic woke the consumer");

    let cursor = suite.cursor()?;
    let port = suite.floci.borrow().mqtt_port();
    let sent = suite.now_ms();
    floci::mqtt_publish(
        port,
        "e2e-device",
        DEVICE_TOPIC,
        body("mqtt", &[]).as_bytes(),
    )?;
    let deleted =
        suite.wait_for_message(pod, "deleted", "mqtt", DELIVERY + Duration::from_secs(20))?;
    suite.woken_within(cursor, pod, &arn("sqs", ORDERS_QUEUE), sent)?;
    suite.sleeps_after_linger(pod, cursor, deleted.time_ms)?;
    step("a message published over MQTT woke the consumer through the IoT rule");
    suite.check_empty(ORDERS_QUEUE)
}

/// 8. A message that comes while the sleeping consumer's last long poll
///    would still wait: the pod reset the poll's connection as it paused, so
///    Floci ended the poll and the message stays visible. Floci wakes the
///    consumer for it, the reset poll fails, and the next poll gets the
///    message at once, received once.
fn a_reset_poll(suite: &Suite) -> Result<(), String> {
    let pod = suite.pod(ORDERS);
    suite.ensure_asleep(pod)?;
    let poll = parked_poll(suite, pod)?;
    let cursor = suite.cursor()?;
    let sent = suite.now_ms();
    suite.send(ORDERS_QUEUE, &body("parked", &[]), None)?;
    let deleted = suite.wait_for_message(pod, "deleted", "parked", DELIVERY)?;
    suite.received_once(pod, "parked")?;
    let n = poll.get("n").unwrap_or_default();
    let lines = suite.lines(pod)?;
    if lines
        .iter()
        .any(|l| l.event == "polled" && l.get("n") == Some(n) && l.get("messages") != Some("0"))
    {
        return Err(format!(
            "the poll that the sleep reset should have claimed nothing: {}",
            lines_text(&lines)
        ));
    }
    suite.woken_by_floci(cursor, pod, &arn("sqs", ORDERS_QUEUE))?;
    step(&format!(
        "poll {n} ended with the sleep; the next poll got the message {} ms after it was sent",
        deleted.time_ms - sent
    ));
    suite.sleeps_after_linger(pod, cursor, deleted.time_ms)
}

/// A poll of the sleeping consumer that was in flight when it slept and
/// started at most [`PARKED`] ago: its last poll when it is that young and unanswered, or
/// else the next one, which the consumer sends once the test wakes it and
/// the last poll's answer comes. The test then hibernates the pod.
fn parked_poll(suite: &Suite, pod: &Consumer) -> Result<Line, String> {
    let number = |line: &Line| line.get("n").and_then(|n| n.parse::<u64>().ok());
    for _ in 0..3 {
        let lines = suite.lines(pod)?;
        let last = lines
            .iter()
            .rfind(|l| l.event == "poll")
            .ok_or_else(|| format!("consumer {} has not polled", pod.name))?
            .clone();
        let answered = lines
            .iter()
            .any(|l| l.event == "polled" && number(l) == number(&last));
        if !answered
            && suite.now_ms() - last.time_ms < ms(PARKED)
            && suite.state(pod)? == PodState::Asleep
        {
            return Ok(last);
        }
        suite
            .api
            .wake(&pod.sandbox)
            .map_err(|e| format!("wake pod {}: {e}", pod.name))?;
        suite
            .api
            .hold(&pod.sandbox, POLL_LEASE, LEASE_MAX)
            .map_err(|e| format!("hold pod {}: {e}", pod.name))?;
        let next = suite.wait_for(pod, WAIT_TIME + DELIVERY, |l| {
            l.event == "poll" && number(l) > number(&last)
        });
        suite
            .api
            .release(&pod.sandbox, POLL_LEASE)
            .map_err(|e| format!("release pod {}: {e}", pod.name))?;
        next?;
        suite
            .api
            .hibernate(&pod.sandbox)
            .map_err(|e| format!("hibernate pod {}: {e}", pod.name))?;
    }
    Err(format!(
        "no poll of consumer {} waited in Floci while its pod slept",
        pod.name
    ))
}

/// 9. Messages in a queue that a workload of three replicas reads wake every
///    replica, and each message is processed once.
fn replicas(suite: &Suite) -> Result<(), String> {
    let pods = suite.pods_of(SHARED);
    for pod in pods {
        suite.ensure_asleep(pod)?;
    }
    let cursor = suite.cursor()?;
    let work_ms = format!("\"work_ms\":{SHARED_WORK_MS}");
    let markers: Vec<String> = (1..=SHARED_MESSAGES)
        .map(|i| format!("shared-{i}"))
        .collect();
    let entries: Vec<String> = markers
        .iter()
        .enumerate()
        .map(|(i, marker)| {
            format!(
                "{{\"Id\":\"m{i}\",\"MessageBody\":{:?}}}",
                body(marker, &[&work_ms])
            )
        })
        .collect();
    let url = queue_url(&suite.floci.borrow(), SHARED_QUEUE);
    suite.aws(&[
        "sqs",
        "send-message-batch",
        "--queue-url",
        &url,
        "--entries",
        &format!("[{}]", entries.join(",")),
    ])?;
    let deadline = Instant::now() + Duration::from_secs(120);
    let (received, deleted) = loop {
        let mut received: HashMap<String, Vec<String>> = HashMap::new();
        let mut deleted: HashMap<String, Vec<i64>> = HashMap::new();
        for pod in pods {
            for line in suite.lines(pod)? {
                let Some(marker) = line.get("marker") else {
                    continue;
                };
                match line.event.as_str() {
                    "received" => received
                        .entry(marker.to_string())
                        .or_default()
                        .push(pod.name.clone()),
                    "deleted" => deleted
                        .entry(marker.to_string())
                        .or_default()
                        .push(line.time_ms),
                    _ => {}
                }
            }
        }
        if markers.iter().all(|m| deleted.contains_key(m)) {
            break (received, deleted);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "not every message was processed within 120 s: received {received:?}"
            ));
        }
        thread::sleep(Duration::from_secs(1));
    };
    for marker in &markers {
        let receivers = received.get(marker).cloned().unwrap_or_default();
        if receivers.len() != 1 || deleted.get(marker).map_or(0, Vec::len) != 1 {
            return Err(format!(
                "message {marker} should have been received and deleted once: received by \
                 {receivers:?}"
            ));
        }
    }
    for pod in pods {
        suite.check_floci_lease(pod, SHARED)?;
        suite.woken_by_floci(cursor, pod, &arn("sqs", SHARED_QUEUE))?;
    }
    let mut by_pod: HashMap<&str, usize> = HashMap::new();
    for names in received.values() {
        *by_pod.entry(names[0].as_str()).or_default() += 1;
    }
    step(&format!("the replicas processed the messages: {by_pod:?}"));
    let last = deleted
        .values()
        .flatten()
        .copied()
        .max()
        .expect("deleted messages");
    for pod in pods {
        suite.sleeps_after_linger(pod, cursor, last)?;
    }
    suite.check_empty(SHARED_QUEUE)
}

/// 10. With the node's room taken, the wake for a message is refused: the
///     answer to the consumer's last poll, held at the pod, is reset, Floci
///     keeps its lease, and once room frees it wakes the consumer, which gets
///     the message once. The node refuses the wake at most twice.
fn a_refused_wake(suite: &Suite) {
    let pod = suite.pod(ORDERS);
    expect(suite.ensure_asleep(pod));
    let held = RefCell::new(Vec::new());
    let check = || -> Result<(), String> {
        suite
            .api
            .wake(&suite.filler)
            .map_err(|e| format!("wake {FILLER}: {e}"))?;
        hold_awake_pods(suite, &mut held.borrow_mut())?;
        let cap = held.borrow().len().to_string();
        wake::set_args(
            &suite.api,
            &[MAX_AWAKE_PODS, &cap, ADMISSION_WAIT, ADMISSION_WAIT_VALUE],
        )?;
        hold_awake_pods(suite, &mut held.borrow_mut())?;
        // The answer to the poll that the consumer sent before it slept must
        // be held at the pod: Floci answers it within the poll's wait.
        let slept = suite
            .events(suite.start, &pod.sandbox)?
            .iter()
            .rev()
            .find(|e| e.to == "asleep")
            .map(event_ms)
            .ok_or_else(|| format!("pod {} has no sleep event", pod.name))??;
        let answered = slept + ms(WAIT_TIME) + CLOCKS_MS;
        if let Ok(wait) = u64::try_from(answered - suite.now_ms()) {
            thread::sleep(Duration::from_millis(wait));
        }
        let cursor = suite.cursor()?;
        suite.send(ORDERS_QUEUE, &body("refused", &[]), None)?;
        let deadline = Instant::now() + Duration::from_secs(30);
        let refused_ms = loop {
            let events = suite.events(cursor, &pod.sandbox)?;
            if let Some(event) = events.iter().find(|e| wake::wake_refusal(e).is_some()) {
                step(&format!("the wake was refused: {}", event.raw));
                break event_ms(event)?;
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "no wake of pod {} was refused within 30 s: {}",
                    pod.name,
                    events_text(&events)
                ));
            }
            thread::sleep(Duration::from_secs(1));
        };
        thread::sleep(Duration::from_secs(5));
        if suite.state(pod)? != PodState::Asleep {
            return Err(format!(
                "pod {} should still sleep after the refusal",
                pod.name
            ));
        }
        suite.check_floci_lease(pod, ORDERS)?;
        if !suite.lines_of(pod, "received", "refused")?.is_empty() {
            return Err("the consumer received the message while it slept".to_string());
        }
        suite
            .api
            .release(&suite.filler, ROOM_LEASE)
            .map_err(|e| e.to_string())?;
        held.borrow_mut().retain(|s| *s != suite.filler);
        suite
            .api
            .hibernate(&suite.filler)
            .map_err(|e| format!("hibernate {FILLER}: {e}"))?;
        let deleted = suite.wait_for_message(pod, "deleted", "refused", Duration::from_secs(90))?;
        let received = suite.received_once(pod, "refused")?;
        let events = suite.events(cursor, &pod.sandbox)?;
        let refusals = events
            .iter()
            .filter(|e| wake::wake_refusal(e).is_some())
            .count();
        if refusals > MAX_REFUSALS {
            return Err(format!(
                "the node refused {refusals} wakes of pod {}: {}",
                pod.name,
                events_text(&events)
            ));
        }
        let resume = events
            .iter()
            .find(|e| e.is_resume())
            .ok_or_else(|| format!("pod {} did not resume: {}", pod.name, events_text(&events)))?;
        let queue = resume.raw.get("reason_detail").get("queue").as_str();
        if resume.reason != "aws"
            || resume.source != "floci"
            || queue != Some(arn("sqs", ORDERS_QUEUE).as_str())
        {
            return Err(format!(
                "pod {} was not woken by Floci: {}",
                pod.name, resume.raw
            ));
        }
        // The node answered the held answer to the poll in flight with a RST.
        // The poll then fails, is sent again by the SDK, or gets its answer
        // after all.
        let lines = suite.lines(pod)?;
        let last_poll = lines
            .iter()
            .rfind(|l| l.event == "poll" && l.time_ms < refused_ms)
            .and_then(|l| l.get("n"))
            .ok_or("no poll before the refusal")?;
        let ended = lines
            .iter()
            .find(|l| {
                l.get("n") == Some(last_poll) && (l.event == "polled" || l.event == "poll-error")
            })
            .ok_or_else(|| {
                format!(
                    "poll {last_poll}, in flight when the wake was refused, did not end: {}",
                    lines_text(&lines[lines.len().saturating_sub(10)..])
                )
            })?;
        step(&format!(
            "{refusals} refused wakes; poll {last_poll}, in flight when the wake was refused, \
             ended with {ended}; the consumer got the message {} ms after the first refusal",
            received.time_ms - refused_ms
        ));
        suite.sleeps_after_linger(pod, cursor, deleted.time_ms)
    };
    let result = check();
    let cleared = wake::clear_args(&suite.api).map(drop);
    for sandbox in held.borrow().iter() {
        if let Err(e) = suite.api.release(sandbox, ROOM_LEASE) {
            eprintln!("release {sandbox}: {e}");
        }
    }
    expect(result);
    expect(cleared.map_err(|e| format!("cleanup: {e}")));
}

/// Holds every awake pod with [`ROOM_LEASE`], so that none makes room by
/// sleeping, and adds them to `held`.
fn hold_awake_pods(suite: &Suite, held: &mut Vec<String>) -> Result<(), String> {
    let pods = suite.api.list().map_err(|e| e.to_string())?;
    for pod in pods
        .iter()
        .filter(|p| matches!(p.state, PodState::Awake | PodState::Waking))
    {
        suite
            .api
            .hold(&pod.sandbox_id, ROOM_LEASE, LEASE_MAX)
            .map_err(|e| format!("hold {}: {e}", pod.sandbox_id))?;
        if !held.contains(&pod.sandbox_id) {
            held.push(pod.sandbox_id.clone());
        }
    }
    Ok(())
}

/// 11. The AMQP consumer's pod resets its connection to the broker as it
///     sleeps, so the broker drops the consumer. A message in its durable queue wakes it, and it reconnects
///     by itself and gets the message.
fn an_amqp_consumer_reconnects(suite: &Suite) -> Result<(), String> {
    let pod = suite.pod(AMQP);
    suite.ensure_asleep(pod)?;
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let [consumers, ..] = suite.amqp_queue()?;
        if consumers == 0 {
            break;
        }
        if Instant::now() >= deadline {
            return Err("the broker kept the sleeping consumer for 120 s".to_string());
        }
        thread::sleep(Duration::from_secs(2));
    }
    if suite.state(pod)? != PodState::Asleep {
        return Err(format!(
            "pod {} woke when the broker closed its connection",
            pod.name
        ));
    }
    step("the broker closed the sleeping consumer's connection");
    let cursor = suite.cursor()?;
    let connections = suite
        .lines(pod)?
        .iter()
        .filter(|l| l.event == "connected")
        .count();
    let sent = suite.publish(&body("amqp", &[]))?;
    suite.wait_for_message(pod, "acked", "amqp", Duration::from_secs(60))?;
    suite.woken_within(cursor, pod, &amqp_resource(suite), sent)?;
    let lines = suite.lines(pod)?;
    let reconnected = lines.iter().filter(|l| l.event == "connected").count();
    if reconnected <= connections || !lines.iter().any(|l| l.event == "disconnected") {
        return Err(format!(
            "the consumer should have reconnected: {}",
            lines_text(&lines)
        ));
    }
    let received: Vec<&Line> = lines.iter().filter(|l| l.is("received", "amqp")).collect();
    let [received] = received[..] else {
        return Err(format!(
            "the consumer received the message {} times",
            received.len()
        ));
    };
    suite.amqp_empty()?;
    step("the consumer reconnected and got the message");
    suite.sleeps_after_linger(pod, cursor, received.time_ms)
}

/// 12. A steady stream of AMQP messages keeps the consumer awake with no
///     pause, and a burst costs one wake and one pause.
fn an_amqp_stream_and_a_burst(suite: &Suite) -> Result<(), String> {
    let publish = |body: &str| suite.publish(body);
    let feed = Feed {
        pod: suite.pod(AMQP),
        resource: amqp_resource(suite),
        send: &publish,
        done: "acked",
        last: "received",
    };
    feed.stream_and_burst(suite)?;
    suite.amqp_empty()
}

/// The AMQP queue as Floci's wakes name it.
fn amqp_resource(suite: &Suite) -> String {
    format!("amqp://{}/%2F/{AMQP_QUEUE}", suite.broker.id)
}

/// 13. Floci and the node service restart while the consumers sleep: no
///     consumer wakes, and a message then wakes the consumer of its queue.
fn restarts(suite: &Suite) -> Result<(), String> {
    let pod = suite.pod(ORDERS);
    suite.ensure_asleep(pod)?;
    let cursor = suite.cursor()?;
    suite.floci.borrow_mut().restart()?;
    suite.api.restart_node_service()?;
    thread::sleep(Duration::from_secs(20));
    for workload in &WORKLOADS {
        for consumer in suite.pods_of(workload.name) {
            let events = suite.events(cursor, &consumer.sandbox)?;
            if events.iter().any(Event::is_resume) {
                return Err(format!(
                    "pod {} woke when Floci and the node service restarted: {}",
                    consumer.name,
                    events_text(&events)
                ));
            }
        }
    }
    let cursor = suite.cursor()?;
    let sent = suite.send(ORDERS_QUEUE, &body("restarted", &[]), None)?;
    let deleted = suite.wait_for_message(pod, "deleted", "restarted", Duration::from_secs(60))?;
    suite.received_once(pod, "restarted")?;
    suite.woken_within(cursor, pod, &arn("sqs", ORDERS_QUEUE), sent)?;
    suite.sleeps_after_linger(pod, cursor, deleted.time_ms)
}

/// 14. A message in a queue that no workload lists leaves its consumer
///     asleep; once the runtime file lists it, Floci wakes the consumer.
fn a_workload_added_by_reload(suite: &Suite) -> Result<(), String> {
    let pod = suite.pod(LATE);
    suite.ensure_asleep(pod)?;
    let cursor = suite.cursor()?;
    suite.send(LATE_QUEUE, &body("late", &[]), None)?;
    thread::sleep(UNLISTED);
    let events = suite.events(cursor, &pod.sandbox)?;
    if !events.is_empty() || suite.state(pod)? != PodState::Asleep {
        return Err(format!(
            "pod {} woke with no workload for it: {}",
            pod.name,
            events_text(&events)
        ));
    }
    if suite.counts(LATE_QUEUE)?[0] != 1 {
        return Err(format!("queue {LATE_QUEUE} should hold its message"));
    }
    let reloaded = suite.now_ms();
    suite.runtime.write(&WORKLOADS, Some(&suite.broker.id))?;
    step("the runtime file lists the workload late");
    let deleted =
        suite.wait_for_message(pod, "deleted", "late", RELOADED + Duration::from_secs(20))?;
    let resume = suite.woken_by_floci(cursor, pod, &arn("sqs", LATE_QUEUE))?;
    let woke = event_ms(&resume)? - reloaded;
    if woke > ms(RELOADED) {
        return Err(format!(
            "Floci woke the consumer {woke} ms after the runtime file listed it"
        ));
    }
    step(&format!(
        "Floci woke the consumer {woke} ms after the runtime file listed it"
    ));
    suite.sleeps_after_linger(pod, cursor, deleted.time_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_utc_times() {
        assert_eq!(iso_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(iso_ms("1970-01-01T00:00:01.5Z"), Some(1500));
        assert_eq!(iso_ms("2000-03-01T00:00:00.000Z"), Some(951_868_800_000));
        assert_eq!(iso_ms("2026-10-20T16:34:13.041Z"), Some(1_792_514_053_041));
        assert_eq!(
            iso_ms("2026-10-20T16:34:13.041123456Z"),
            Some(1_792_514_053_041)
        );
        assert_eq!(iso_ms("2026-10-20 16:34:13Z"), None);
        assert_eq!(iso_ms("2026-10-20T16:34:13"), None);
    }

    #[test]
    fn parses_consumer_lines() {
        let lines = parse_lines(
            "ready time=2026-10-20T16:34:13.041Z mode=sqs queue=orders\n\
             (node:1) a warning from Node.js\n\
             received time=2026-10-20T16:34:13.053Z marker=one id=cb66 count=2\n",
        );
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1].event, "received");
        assert_eq!(lines[1].time_ms, 1_792_514_053_053);
        assert!(lines[1].is("received", "one"));
        assert_eq!(lines[1].get("count"), Some("2"));
        assert_eq!(lines[1].get("other"), None);
        assert_eq!(lines[1].to_string(), "received marker=one id=cb66 count=2");
    }

    #[test]
    fn writes_the_runtime_file() {
        let yaml = runtime_yaml(17070, "ns", &WORKLOADS[2..4], Some("b-1"));
        assert_eq!(
            yaml,
            "version: 1\n\
             nodes:\n  e2e:\n    url: http://127.0.0.1:17070\n    token-file: node-token\n\
             workloads:\n  shared:\n    idle-timeout-seconds: 60\n    queues: [shared]\n    \
             runtime:\n      type: kubernetes\n      node: e2e\n      namespace: ns\n      \
             selector: {app: shared}\n  amqp:\n    idle-timeout-seconds: 60\n    \
             rabbitmq-queues:\n      - broker-id: b-1\n        queue: tasks\n        \
             username-env: E2E_RABBITMQ_USER\n        password-env: E2E_RABBITMQ_PASSWORD\n    \
             runtime:\n      type: kubernetes\n      node: e2e\n      namespace: ns\n      \
             deployment: amqp\n"
        );
        let without_broker = runtime_yaml(17070, "ns", &WORKLOADS[3..4], None);
        assert!(without_broker.ends_with("workloads:\n"), "{without_broker}");
    }

    #[test]
    fn writes_a_deployment() {
        let env = [("MODE", "sqs".to_string())];
        let yaml = deployment(&workload(SHARED), "ns", "docker.io/x/consumer:1", &env);
        for expected in [
            "  name: shared\n  namespace: ns\n",
            "  replicas: 3\n",
            "      labels: {app: shared}\n",
            "      annotations: {\"smolvm-pods.floci.io/idle-timeout\": \"10s\"}\n",
            "      runtimeClassName: smolvm-pods\n",
            "        image: docker.io/x/consumer:1\n",
            "        - {name: MODE, value: \"sqs\"}\n",
        ] {
            assert!(yaml.contains(expected), "no {expected:?} in:\n{yaml}");
        }
    }

    #[test]
    fn writes_message_bodies() {
        assert_eq!(body("one", &[]), r#"{"marker":"one"}"#);
        assert_eq!(
            body("long", &["\"work_ms\":90000", "\"after_ms\":1000"]),
            r#"{"marker":"long","work_ms":90000,"after_ms":1000}"#
        );
    }
}
