//! Wake on traffic and idle sleep. Pods sleep by themselves after their idle
//! timeout, while the node service answers their probes, and any TCP
//! connection to them wakes them. Every call to the runtime goes through
//! `smolvm_pods_e2e::runtime` and `smolvm_pods_e2e::wake`. Traffic comes from
//! the runc client pod's network namespace, through
//! `smolvm_pods_e2e::traffic`. tests/e2e/README.md lists the checks and
//! documents the runtime's contract.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, RandomState};
use std::thread;
use std::time::{Duration, Instant};

use smolvm_pods_e2e::runtime::{
    self, Api, Event, FaultPoint, PodState, NODE_SERVICE_LOG, NODE_SERVICE_RESTART,
};
use smolvm_pods_e2e::traffic::{pairs, parse_fetch, Client, Fetched};
use smolvm_pods_e2e::wake::{
    self, Slept, TcpApi, Traffic, ADMISSION_WAIT, API_LISTEN, API_TOKEN_FILE, IDLE_TIMEOUT,
    MAX_AWAKE_PODS, POLL_INTERVAL, PROBE_USER_AGENT,
};
use smolvm_pods_e2e::{
    describe, eventually, expect, http_get, quantile, run_checks, step, summary, Arch, Cluster,
    Node, Pod, Pusher, Runtime, COMMAND_TIMEOUT, RUNTIME_CLASS,
};

const PORT: u16 = 8080;
const HTTPS_PORT: u16 = 8443;
const GRPC_PORT: u16 = 9090;
const UDP_PORT: u16 = 8081;

/// The pod that most checks wake and watch sleep, with a ClusterIP Service.
const WEB: &str = "web";
/// The pods of the pod-to-pod check, each with a ClusterIP Service.
const CALLER: &str = "caller";
const CALLEE: &str = "callee";
/// The probe pods, one per kind of probe. The exec pod's image is amd64, so
/// that its probe runs under Rosetta.
const PROBES_HTTP: &str = "probes-http";
const PROBES_TCP: &str = "probes-tcp";
const PROBES_GRPC: &str = "probes-grpc";
const PROBES_EXEC: &str = "probes-exec";
const PROBES_HTTPS: &str = "probes-https";
const PROBE_PODS: [&str; 5] = [
    PROBES_HTTP,
    PROBES_TCP,
    PROBES_GRPC,
    PROBES_EXEC,
    PROBES_HTTPS,
];
/// The pods of the failing-app check: one whose health is switched to
/// failing, one whose resume fails, and one that never becomes ready.
const UNHEALTHY: &str = "unhealthy";
const DOOMED: &str = "doomed";
const UNREADY: &str = "unready";
/// The pod of the outbound check, which follows streams that the client pod
/// and the Mac push, with a ClusterIP Service.
const FOLLOWER: &str = "follower";
/// The pod of the CPU check with the CPU rule off, which sleeps while busy.
const CPU_OFF: &str = "cpu-off";
/// The annotation that turns the CPU rule off for a pod.
const CPU_RULE_OFF: (&str, &str) = ("smolvm-pods.floci.io/cpu-threshold", "off");
/// The runc pod in whose network namespace the test's clients run, with a
/// ClusterIP Service.
const CLIENT: &str = "client";
const VM_PODS: [&str; 13] = [
    WEB,
    CALLER,
    CALLEE,
    PROBES_HTTP,
    PROBES_TCP,
    PROBES_GRPC,
    PROBES_EXEC,
    PROBES_HTTPS,
    UNHEALTHY,
    DOOMED,
    UNREADY,
    FOLLOWER,
    CPU_OFF,
];

/// The exec probe's command, which the exec probe check also runs through
/// kubectl exec.
const EXEC_PROBE: [&str; 3] = ["/testapp", "get", "http://127.0.0.1:8080/health"];

/// The idle timeout of every VM pod of the test.
const IDLE: Duration = Duration::from_secs(10);
const IDLE_ANNOTATION: &str = "10s";
/// A pod's pause starts at most this long after its last activity: the idle
/// timeout, one poll interval, and 2 s for the test's polling.
const SLEEP_START: Duration = Duration::from_secs(IDLE.as_secs() + POLL_INTERVAL.as_secs() + 2);
/// How long a pause may take once it started.
const PAUSE_TIME: Duration = Duration::from_secs(30);

/// MiB that the app fills: `web` hashes 64 MiB for the cycles, the others
/// hold little so that they boot and pause quickly.
const WEB_MIB: u32 = 64;
const SMALL_MIB: u32 = 16;

/// The memory of a node that the test creates.
const NODE_MEMORY_MIB: u32 = 8192;

/// How long one request may take, a wake included.
const REQUEST: Duration = Duration::from_secs(30);

/// The automatic cycles, and the limits on their first answers.
const CYCLES: usize = 20;
const MEDIAN_MS: f64 = 300.0;
const P95_MS: f64 = 600.0;
/// A connection that waited for a SYN retransmit took at least this long.
const RETRANSMIT_MS: f64 = 900.0;
/// How long the probe pods are watched asleep.
const PROBE_WINDOW: Duration = Duration::from_secs(180);
/// The stream and the WebSocket of the streams check last 3 idle timeouts.
const STREAM_SECONDS: u64 = 3 * IDLE.as_secs();
const WS_PING_SECONDS: u64 = 5;
/// The keepalive check waits this long asleep, for two of the client's
/// keepalive probes (Go: after 15 s idle, then every 15 s).
const KEEPALIVE_WAIT: Duration = Duration::from_secs(40);
/// After a restart of the node service, probes are answered within this time.
const PROBES_BACK: Duration = Duration::from_secs(5);
/// The probes' timeout, after which a probe that started during an outage
/// has failed.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
/// The TCP API's listener, and the lease of the wake through it.
const API_ADDR: &str = "127.0.0.1:17070";
const LEASE_TTL: Duration = Duration::from_secs(40);
/// A wake that admission refuses gets its RST within this time.
const REFUSED_WITHIN_MS: f64 = 11_000.0;
/// The longest lease the API grants.
const LEASE_MAX: Duration = Duration::from_secs(60);
/// The small scale check's pods, how they are made and watched, and its
/// limits.
const SCALE_PODS: usize = 40;
const SCALE_BATCH: usize = 10;
const SCALE_WINDOW: Duration = Duration::from_secs(600);
const SCALE_WOKEN: usize = 10;
const SCALE_CPU_SHARE: f64 = 0.10;
const SCALE_KIB_PER_POD: u64 = 200;
/// The CPU check's busy period, and how long after it the pod may sleep: the
/// CPU window (60 s) and [`SLEEP_START`].
const BUSY_SECONDS: u64 = 45;
const AFTER_BUSY: Duration = Duration::from_secs(60 + SLEEP_START.as_secs());
/// The outbound check: the client pod and a server on the Mac push a line
/// every few seconds to the follower, which sleeps through some of them.
const PUSH_EVERY: u64 = 3;
const PUSH_SECONDS: u64 = 300;
const ASLEEP_WITH_PUSHES: Duration = Duration::from_secs(20);

#[test]
#[ignore = "needs a Mac with smolvm and Docker: SMOLVM_PODS_E2E=1 cargo test -- --ignored"]
fn wake() {
    let cluster = Cluster::start_with_memory("wake", Some(NODE_MEMORY_MIB));
    let testapp = cluster.node_path("testapp");
    cluster
        .node
        .copy_in(&cluster.workload(Arch::Arm64, "testapp"), &testapp);
    cluster.node.sh(&format!("chmod +x {testapp}"));
    let api = Api::connect(&cluster.node, &testapp, Duration::from_secs(60));
    let start = api.cursor().unwrap_or(0);
    let pods = start_pods(&cluster);
    let suite = Suite::new(&cluster, api, testapp, pods, start);
    run_checks(&[
        ("the test app's endpoints", &|| endpoints(&suite)),
        ("probes leave pods asleep", &|| {
            probes_leave_pods_asleep(&suite)
        }),
        ("exec probes under Rosetta", &|| exec_probes(&suite)),
        ("NodePort to a probe port", &|| node_port(&suite)),
        ("SIGKILL of the node service", &|| node_service_kill(&suite)),
        ("20 automatic cycles through a Service", &|| cycles(&suite)),
        ("first-request latency", &|| latency(&suite)),
        ("10 concurrent requests", &|| concurrent_requests(&suite)),
        ("client IP preserved", &|| client_ip(&suite)),
        ("pod-to-pod calls wake the callee", &|| pod_to_pod(&suite)),
        ("streams and WebSockets", &|| streams(&suite)),
        ("keepalive and quiet connections", &|| keepalive(&suite)),
        ("UDP and ICMP", &|| udp_and_icmp(&suite)),
        ("wake during a pause", &|| wake_during_pause(&suite)),
        ("the recheck", &|| recheck(&suite)),
        ("CPU keeps a pod awake", &|| busy_cpu(&suite)),
        ("outbound connections", &|| outbound(&suite)),
        ("a failing app still fails", &|| failing_app(&suite)),
        ("TCP API", &|| tcp_api(&suite)),
        ("admission", &|| admission(&suite)),
        ("small scale", &|| small_scale(&suite)),
    ]);
}

/// The probes of a pod of the test. Http, Tcp, Grpc, Https and Exec are
/// readiness and liveness of that kind, every 2 s (exec: every 10 s).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Probes {
    None,
    Http,
    Tcp,
    Grpc,
    Https,
    Exec,
    /// HTTP readiness that fails at once, and HTTP liveness that restarts
    /// the container only after 30 s, longer than the pod would need to
    /// sleep.
    Unhealthy,
    /// HTTP readiness only, against an app that answers 500 (`-unready`).
    Unready,
}

impl Probes {
    fn yaml(self) -> String {
        let http = |port: u16, scheme: &str| {
            format!("httpGet: {{path: /health, port: {port}, scheme: {scheme}}}")
        };
        let probe = |kind: &str, every: u32, timeout: u32, failures: u32| {
            format!(
                "{{{kind}, periodSeconds: {every}, timeoutSeconds: {timeout}, \
                 failureThreshold: {failures}}}"
            )
        };
        let both = |kind: String, every: u32, timeout: u32| {
            let probe = probe(&kind, every, timeout, 3);
            format!("    readinessProbe: {probe}\n    livenessProbe: {probe}\n")
        };
        let exec = format!(
            "exec: {{command: [{}]}}",
            EXEC_PROBE.map(|a| format!("{a:?}")).join(", ")
        );
        match self {
            Probes::None => String::new(),
            Probes::Http => both(http(PORT, "HTTP"), 2, 2),
            Probes::Tcp => both(format!("tcpSocket: {{port: {PORT}}}"), 2, 2),
            Probes::Grpc => both(format!("grpc: {{port: {GRPC_PORT}}}"), 2, 2),
            Probes::Https => both(http(HTTPS_PORT, "HTTPS"), 2, 2),
            Probes::Exec => both(exec, 10, 10),
            Probes::Unhealthy => format!(
                "    readinessProbe: {}\n    livenessProbe: {}\n",
                probe(&http(PORT, "HTTP"), 2, 2, 1),
                probe(&http(PORT, "HTTP"), 2, 2, 15)
            ),
            Probes::Unready => format!(
                "    readinessProbe: {}\n",
                probe(&http(PORT, "HTTP"), 2, 2, 1)
            ),
        }
    }
}

/// A pod of the test. VM pods run `testapp hold` with every server and the
/// test's idle timeout; the runc pod runs `testapp serve`.
struct TestPod {
    name: &'static str,
    arch: Arch,
    runtime: Runtime,
    mib: u32,
    probes: Probes,
    /// The value of the label `group`: `main`, or `scale` for the small
    /// scale check's pods.
    group: &'static str,
    /// Annotations besides the idle timeout.
    annotations: &'static [(&'static str, &'static str)],
}

impl TestPod {
    fn vm(name: &'static str, mib: u32, probes: Probes) -> TestPod {
        TestPod {
            name,
            arch: Arch::Arm64,
            runtime: Runtime::SmolvmPods,
            mib,
            probes,
            group: "main",
            annotations: &[],
        }
    }

    fn manifest(&self, namespace: &str, image: &str) -> String {
        let (runtime_class, annotations, args) = match self.runtime {
            Runtime::SmolvmPods => {
                let mut args = vec![
                    "hold".to_string(),
                    "-mib".into(),
                    format!("{:?}", self.mib.to_string()),
                    "-https".into(),
                    format!("\":{HTTPS_PORT}\""),
                    "-grpc".into(),
                    format!("\":{GRPC_PORT}\""),
                    "-udp".into(),
                    format!("\":{UDP_PORT}\""),
                ];
                if self.probes == Probes::Unready {
                    args.push("-unready".into());
                }
                args.extend([self.name.to_string(), format!("\":{PORT}\"")]);
                (
                    format!("  runtimeClassName: {RUNTIME_CLASS}\n"),
                    [(IDLE_TIMEOUT, IDLE_ANNOTATION)]
                        .iter()
                        .chain(self.annotations)
                        .map(|(key, value)| format!("{key:?}: {value:?}"))
                        .collect::<Vec<_>>()
                        .join(", "),
                    args,
                )
            }
            Runtime::Runc => (
                String::new(),
                String::new(),
                vec!["serve".into(), self.name.into(), format!("\":{PORT}\"")],
            ),
        };
        format!(
            r#"apiVersion: v1
kind: Pod
metadata:
  name: {name}
  namespace: {namespace}
  labels: {{app: {name}, group: {group}}}
  annotations: {{{annotations}}}
spec:
{runtime_class}  automountServiceAccountToken: false
  terminationGracePeriodSeconds: 5
  containers:
  - name: app
    image: {image}
    imagePullPolicy: Never
    args: [{args}]
    ports:
    - {{name: http, containerPort: {PORT}}}
    - {{name: https, containerPort: {HTTPS_PORT}}}
    - {{name: grpc, containerPort: {GRPC_PORT}}}
    - {{name: udp, containerPort: {UDP_PORT}, protocol: UDP}}
{probes}"#,
            name = self.name,
            group = self.group,
            args = args.join(", "),
            probes = self.probes.yaml(),
        )
    }
}

/// The test's pods, created at the start.
fn test_pods() -> Vec<TestPod> {
    let mut exec = TestPod::vm(PROBES_EXEC, SMALL_MIB, Probes::Exec);
    exec.arch = Arch::Amd64;
    vec![
        TestPod::vm(WEB, WEB_MIB, Probes::None),
        TestPod::vm(CALLER, SMALL_MIB, Probes::None),
        TestPod::vm(CALLEE, SMALL_MIB, Probes::None),
        TestPod::vm(PROBES_HTTP, SMALL_MIB, Probes::Http),
        TestPod::vm(PROBES_TCP, SMALL_MIB, Probes::Tcp),
        TestPod::vm(PROBES_GRPC, SMALL_MIB, Probes::Grpc),
        exec,
        TestPod::vm(PROBES_HTTPS, SMALL_MIB, Probes::Https),
        TestPod::vm(UNHEALTHY, SMALL_MIB, Probes::Unhealthy),
        TestPod::vm(DOOMED, SMALL_MIB, Probes::Http),
        TestPod::vm(UNREADY, SMALL_MIB, Probes::Unready),
        TestPod::vm(FOLLOWER, SMALL_MIB, Probes::None),
        TestPod {
            annotations: &[CPU_RULE_OFF],
            ..TestPod::vm(CPU_OFF, SMALL_MIB, Probes::None)
        },
        TestPod {
            name: CLIENT,
            arch: Arch::Arm64,
            runtime: Runtime::Runc,
            mib: 0,
            probes: Probes::None,
            group: "main",
            annotations: &[],
        },
    ]
}

/// ClusterIP Services for `web`, `caller`, `callee`, `follower` and `client`,
/// and a NodePort Service on `node_port` for `probes-http`'s probe port.
fn services(namespace: &str, node_port: u16) -> String {
    let cluster_ip = |name: &str| {
        format!(
            "apiVersion: v1\nkind: Service\nmetadata: {{name: {name}, namespace: {namespace}}}\n\
             spec:\n  selector: {{app: {name}}}\n  ports: [{{port: {PORT}, targetPort: {PORT}}}]\n"
        )
    };
    let node = format!(
        "apiVersion: v1\nkind: Service\nmetadata: {{name: {PROBES_HTTP}, namespace: {namespace}}}\n\
         spec:\n  type: NodePort\n  selector: {{app: {PROBES_HTTP}}}\n  \
         ports: [{{port: {PORT}, targetPort: {PORT}, nodePort: {node_port}}}]\n"
    );
    [
        cluster_ip(WEB),
        cluster_ip(CALLER),
        cluster_ip(CALLEE),
        cluster_ip(FOLLOWER),
        cluster_ip(CLIENT),
        node,
    ]
    .join("---\n")
}

/// Creates the test's pods and Services, and waits until every pod but
/// `unready` is Ready, and `unready` runs.
fn start_pods(cluster: &Cluster) -> Vec<Pod<'_>> {
    step("creating the test pods and Services");
    let pods = test_pods();
    let manifests: Vec<String> = pods
        .iter()
        .map(|p| p.manifest(&cluster.namespace, &cluster.image(p.arch)))
        .collect();
    cluster.apply("pods.yaml", &manifests.join("---\n"));
    cluster.apply(
        "services.yaml",
        &services(&cluster.namespace, cluster.node_port()),
    );
    let ready: Vec<&str> = pods
        .iter()
        .map(|p| p.name)
        .filter(|&name| name != UNREADY)
        .collect();
    expect(wait_ready(cluster, &ready));
    eventually(Duration::from_secs(300), "pod unready to run", || {
        let args = [
            "get",
            "pod",
            "-n",
            &cluster.namespace,
            UNREADY,
            "-o",
            "jsonpath={.status.phase}",
        ];
        match cluster.node.try_kubectl(&args, COMMAND_TIMEOUT)?.as_str() {
            "Running" => Ok(()),
            phase => Err(format!("phase {phase:?}")),
        }
    });
    pods.iter().map(|p| cluster.ready_pod(p.name)).collect()
}

/// Waits until the pods are Ready, describing them if they do not become so.
fn wait_ready(cluster: &Cluster, names: &[&str]) -> Result<(), String> {
    let timeout = "--timeout=300s";
    let pods: Vec<String> = names.iter().map(|name| format!("pod/{name}")).collect();
    let mut wait = vec![
        "wait",
        "-n",
        &cluster.namespace,
        "--for=condition=Ready",
        timeout,
    ];
    wait.extend(pods.iter().map(String::as_str));
    cluster
        .node
        .try_kubectl(&wait, Duration::from_secs(300) + COMMAND_TIMEOUT)
        .map(drop)
        .map_err(|e| {
            let details = cluster.node.try_kubectl(
                &["describe", "pods", "-n", &cluster.namespace],
                COMMAND_TIMEOUT,
            );
            format!(
                "the pods did not become Ready: {e}\n{}",
                details.unwrap_or_else(|e| e)
            )
        })
}

/// What a pod's app reports of its process, which a wake must not change.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Identity {
    pid: String,
    start_ticks: String,
    boot_id: String,
}

impl Identity {
    fn of(fetched: &Fetched) -> Result<Identity, String> {
        Ok(Identity {
            pid: fetched.value("pid")?.to_string(),
            start_ticks: fetched.value("start_ticks")?.to_string(),
            boot_id: fetched.value("boot_id")?.to_string(),
        })
    }
}

/// A pod as kubectl shows it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PodRow {
    phase: String,
    ready: String,
    ready_since: String,
    restarts: u64,
    ip: String,
    last_exit: Option<i64>,
}

/// One line per pod: name, phase, Ready and its last change, the first
/// container's restarts and last exit code, and the pod IP, by tabs.
const POD_ROWS: &str = r#"jsonpath={range .items[*]}{.metadata.name}{"\t"}{.status.phase}{"\t"}{.status.conditions[?(@.type=="Ready")].status}{"\t"}{.status.conditions[?(@.type=="Ready")].lastTransitionTime}{"\t"}{.status.containerStatuses[0].restartCount}{"\t"}{.status.podIP}{"\t"}{.status.containerStatuses[0].lastState.terminated.exitCode}{"\n"}{end}"#;

fn parse_pod_rows(text: &str) -> HashMap<String, PodRow> {
    text.lines()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split('\t').collect();
            let field = |i: usize| fields.get(i).copied().unwrap_or_default().to_string();
            let name = field(0);
            (!name.is_empty()).then(|| {
                let row = PodRow {
                    phase: field(1),
                    ready: field(2),
                    ready_since: field(3),
                    restarts: field(4).parse().unwrap_or_default(),
                    ip: field(5),
                    last_exit: field(6).parse().ok(),
                };
                (name, row)
            })
        })
        .collect()
}

/// kubelet's `Unhealthy` events, as `NAME COUNT` lines.
const UNHEALTHY_EVENTS: &str =
    r#"jsonpath={range .items[*]}{.involvedObject.name}{"\t"}{.count}{"\n"}{end}"#;

/// The number of `Unhealthy` events per pod: an event without a count is one.
fn parse_unhealthy(text: &str) -> HashMap<String, u64> {
    let mut counts = HashMap::new();
    for line in text.lines() {
        let (name, count) = line.split_once('\t').unwrap_or((line, ""));
        if !name.is_empty() {
            *counts.entry(name.to_string()).or_default() += count.trim().parse().unwrap_or(1);
        }
    }
    counts
}

/// One automatic cycle, for the latency check.
struct Cycle {
    first_byte_ms: f64,
    connect_ms: f64,
    capture_to_release_ms: Option<f64>,
    steps: Vec<(String, f64)>,
    from: Option<Traffic>,
}

/// What checks record for later checks.
#[derive(Default)]
struct Records {
    cycles: Vec<Cycle>,
    /// The probe check's outcome for the exec probe pod, for the exec probe
    /// check.
    exec_window: Option<Result<(), String>>,
}

/// The test's pods and what it measured.
struct Suite<'a> {
    cluster: &'a Cluster,
    api: Api<'a>,
    /// The arm64 test app in the node.
    testapp: String,
    /// The test app in the client pod's network namespace.
    client: Client<'a>,
    pods: Vec<Pod<'a>>,
    ips: HashMap<&'static str, String>,
    services: HashMap<&'static str, String>,
    identities: HashMap<&'static str, Identity>,
    web_hash: String,
    /// The event cursor before the pods were created.
    start: u64,
    /// The last event cursor seen, from which the next is read.
    last_cursor: Cell<u64>,
    records: RefCell<Records>,
}

impl<'a> Suite<'a> {
    /// Finds the client pod's network namespace, waits for each app's first
    /// answer and records what the checks compare with.
    fn new(
        cluster: &'a Cluster,
        api: Api<'a>,
        testapp: String,
        pods: Vec<Pod<'a>>,
        start: u64,
    ) -> Suite<'a> {
        let client_pod = pods
            .iter()
            .find(|p| p.name == CLIENT)
            .expect("the client pod");
        let container = cluster
            .node
            .sh(&format!("crictl ps -q --pod {}", client_pod.sandbox));
        let container = container
            .split_whitespace()
            .next()
            .expect("the client's container");
        let pid = cluster
            .node
            .sh(&format!("crictl inspect {container} | jq -r .info.pid"));
        let dir = cluster.node_path("clients");
        cluster.node.sh(&format!("mkdir -p {dir}"));
        let client = Client::new(&cluster.node, &testapp, pid.trim(), &dir);
        let ips = pods.iter().map(|p| (p.name, p.ip())).collect();
        let services = [WEB, CALLER, CALLEE, FOLLOWER, CLIENT]
            .into_iter()
            .map(|name| (name, cluster.service_ip(name)))
            .collect();
        let mut suite = Suite {
            cluster,
            api,
            testapp,
            client,
            pods,
            ips,
            services,
            identities: HashMap::new(),
            web_hash: String::new(),
            start,
            last_cursor: Cell::new(start),
            records: RefCell::default(),
        };
        for name in VM_PODS {
            let url = suite.url(name, "/id");
            let fetched = eventually(Duration::from_secs(180), "the app's first answer", || {
                suite.client.get(&url, REQUEST)
            });
            let identity = expect(Identity::of(&fetched));
            step(&format!(
                "pod {name}: IP {}, pid {}, boot id {}",
                suite.ips[name], identity.pid, identity.boot_id
            ));
            suite.identities.insert(name, identity);
        }
        let hash = expect(suite.client.get(&suite.url(WEB, "/hash"), REQUEST));
        suite.web_hash = expect(hash.value("sha256")).to_string();
        suite
    }

    #[track_caller]
    fn pod(&self, name: &str) -> &Pod<'a> {
        match self.pods.iter().find(|pod| pod.name == name) {
            Some(pod) => pod,
            None => panic!("no pod {name}"),
        }
    }

    fn node(&self) -> &Node {
        &self.cluster.node
    }

    fn sandbox(&self, name: &str) -> &str {
        &self.pod(name).sandbox
    }

    fn url(&self, name: &str, path: &str) -> String {
        format!("http://{}:{PORT}{path}", self.ips[name])
    }

    fn service_url(&self, name: &str, path: &str) -> String {
        format!("http://{}:{PORT}{path}", self.services[name])
    }

    /// `namespace/name` of a pod, as a traffic wake names its source.
    fn qualified(&self, name: &str) -> String {
        format!("{}/{name}", self.cluster.namespace)
    }

    /// The cursor after the last event so far.
    #[track_caller]
    fn cursor(&self) -> u64 {
        let last = self.last_cursor.get();
        let events = expect(self.api.events(last).map_err(|e| e.to_string()));
        let cursor = events.last().map_or(last, |event| event.seq);
        self.last_cursor.set(cursor);
        cursor
    }

    /// The events after `cursor` about the pod.
    fn events_since(&self, cursor: u64, name: &str) -> Result<Vec<Event>, String> {
        self.api.events_of(cursor, self.sandbox(name))
    }

    fn state(&self, name: &str) -> Result<PodState, String> {
        self.api.state(self.sandbox(name))
    }

    fn vm_count(&self, name: &str) -> usize {
        self.pod(name).vm_processes().len()
    }

    fn vm_pid(&self, name: &str) -> Result<u32, String> {
        match &self.pod(name).vm_processes()[..] {
            [vm] => Ok(vm.pid),
            vms => Err(format!(
                "pod {name} should have one VM process, has {vms:?}"
            )),
        }
    }

    /// Puts the pod to sleep through the API unless it is asleep.
    fn ensure_asleep(&self, name: &str) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let state = self.state(name)?;
            match state {
                PodState::Asleep if self.vm_count(name) == 0 => return Ok(()),
                PodState::Awake => match self.api.hibernate(self.sandbox(name)) {
                    Ok(_) => continue,
                    Err(e) if e.code == "refused" || e.code == "cancelled" => {
                        step(&format!("hibernate pod {name}: {e}; trying again"))
                    }
                    Err(e) => return Err(format!("hibernate pod {name}: {e}")),
                },
                _ => {}
            }
            if Instant::now() >= deadline {
                return Err(format!("pod {name} did not go to sleep: {state}"));
            }
            thread::sleep(Duration::from_secs(1));
        }
    }

    /// Wakes the pod through the API unless it is awake.
    fn ensure_awake(&self, name: &str) -> Result<(), String> {
        if self.state(name)? != PodState::Awake {
            self.api
                .wake(self.sandbox(name))
                .map_err(|e| format!("wake pod {name}: {e}"))?;
        }
        match self.state(name)? {
            PodState::Awake => Ok(()),
            state => Err(format!("pod {name} is {state} after a wake")),
        }
    }

    /// Waits until the pod sleeps by itself, and fails unless the last sleep
    /// since `cursor` had the reason `idle`.
    fn wait_for_idle_sleep(&self, name: &str, cursor: u64) -> Result<Slept, String> {
        self.wait_for_idle_sleep_within(name, cursor, SLEEP_START)
    }

    /// Like [`Suite::wait_for_idle_sleep`], for a pause that may start up to
    /// `start` from now.
    fn wait_for_idle_sleep_within(
        &self,
        name: &str,
        cursor: u64,
        start: Duration,
    ) -> Result<Slept, String> {
        let limit = start + PAUSE_TIME;
        let slept = wake::watch_sleep(self.node(), self.sandbox(name), limit).map_err(|e| {
            format!("pod {name} did not sleep by itself within {limit:?} (idle sleep): {e}")
        })?;
        if self.vm_count(name) != 0 {
            return Err(format!("pod {name} is asleep, but its VM process runs"));
        }
        let events = self.events_since(cursor, name)?;
        match events.iter().rev().find(|e| e.to == "asleep") {
            Some(e) if wake::is_idle_sleep(e) => Ok(slept),
            Some(e) => Err(format!(
                "pod {name} went to sleep with the reason {}, not idle: {}",
                e.reason, e.raw
            )),
            None => Err(format!(
                "pod {name} is asleep, but no event since {cursor} put it to sleep"
            )),
        }
    }

    /// Like [`Suite::wait_for_idle_sleep`] from the end of some activity, and
    /// fails unless the pause started within `limit` of it.
    fn sleeps_within(&self, name: &str, cursor: u64, limit: Duration) -> Result<Slept, String> {
        let slept = self.wait_for_idle_sleep_within(name, cursor, limit)?;
        if slept.pausing > limit {
            return Err(format!(
                "pod {name} started to sleep {:?} after the end of its activity, more than {limit:?}",
                slept.pausing
            ));
        }
        step(&format!(
            "pod {name} slept by itself: pausing after {:.1} s, asleep after {:.1} s",
            slept.pausing.as_secs_f64(),
            slept.asleep.as_secs_f64()
        ));
        Ok(slept)
    }

    /// Fails unless no pause of the pod started after `cursor`.
    fn check_no_pause(&self, cursor: u64, name: &str) -> Result<(), String> {
        let pauses: Vec<String> = self
            .events_since(cursor, name)?
            .iter()
            .filter(|e| wake::is_pause(e))
            .map(|e| e.raw.to_string())
            .collect();
        if pauses.is_empty() {
            Ok(())
        } else {
            Err(format!("pod {name} started to pause: {pauses:?}"))
        }
    }

    /// The traffic wakes of the pod after `cursor`.
    fn traffic_wakes(&self, cursor: u64, name: &str) -> Result<Vec<Event>, String> {
        let events = self.events_since(cursor, name)?;
        let resumes: Vec<&Event> = events.iter().filter(|e| e.is_resume()).collect();
        if let Some(other) = resumes.iter().find(|e| !wake::is_traffic_wake(e)) {
            return Err(format!(
                "pod {name} woke with the reason {}, not traffic: {}",
                other.reason, other.raw
            ));
        }
        Ok(resumes.into_iter().cloned().collect())
    }

    /// Fails unless the pod woke once after `cursor`, from `from`'s traffic
    /// to its port.
    fn check_woken_by(&self, cursor: u64, name: &str, from: &str) -> Result<(), String> {
        let wakes = self.traffic_wakes(cursor, name)?;
        let [wake] = &wakes[..] else {
            return Err(format!(
                "pod {name} should wake once from traffic, woke {} times (wake on traffic)",
                wakes.len()
            ));
        };
        let want = Traffic {
            from: from.to_string(),
            port: PORT,
        };
        match Traffic::of(wake) {
            Some(traffic) if traffic == want => Ok(()),
            traffic => Err(format!(
                "pod {name} woke from {traffic:?}, expected {want:?}: {}",
                wake.raw
            )),
        }
    }

    /// Every pod of the namespace as kubectl shows it.
    fn rows(&self) -> Result<HashMap<String, PodRow>, String> {
        let args = ["get", "pods", "-n", &self.cluster.namespace, "-o", POD_ROWS];
        Ok(parse_pod_rows(
            &self.node().try_kubectl(&args, COMMAND_TIMEOUT)?,
        ))
    }

    fn row(&self, name: &str) -> Result<PodRow, String> {
        self.rows()?
            .remove(name)
            .ok_or_else(|| format!("kubectl shows no pod {name}"))
    }

    fn unhealthy(&self) -> Result<HashMap<String, u64>, String> {
        let args = [
            "get",
            "events",
            "-n",
            &self.cluster.namespace,
            "--field-selector",
            "reason=Unhealthy",
            "-o",
            UNHEALTHY_EVENTS,
        ];
        Ok(parse_unhealthy(
            &self.node().try_kubectl(&args, COMMAND_TIMEOUT)?,
        ))
    }

    /// Fails unless the pod has its IP and sandbox, and no container
    /// restarted.
    fn check_unchanged(&self, name: &str) -> Result<(), String> {
        let row = self.row(name)?;
        let sandbox = self.cluster.ready_sandbox(name)?;
        let mut changed = Vec::new();
        if row.ip != self.ips[name] {
            changed.push(format!("pod IP {}, was {}", row.ip, self.ips[name]));
        }
        if sandbox != self.sandbox(name) {
            changed.push(format!("sandbox {sandbox}, was {}", self.sandbox(name)));
        }
        if row.restarts != 0 {
            changed.push(format!("{} restarts", row.restarts));
        }
        if changed.is_empty() {
            Ok(())
        } else {
            Err(format!("pod {name} changed: {}", changed.join("; ")))
        }
    }

    /// Fails unless `/id` answers with the pod's identity.
    fn check_identity(&self, name: &str, fetched: &Fetched) -> Result<(), String> {
        let identity = Identity::of(fetched)?;
        if identity == self.identities[name] {
            Ok(())
        } else {
            Err(format!(
                "pod {name} answers as {identity:?}, was {:?}",
                self.identities[name]
            ))
        }
    }
}

/// Fails with every error in `failed`.
#[track_caller]
fn assert_none(failed: &[String]) {
    assert!(failed.is_empty(), "{}", failed.join("\n"));
}

/// Runs `body`, then `cleanup` whatever happened, and fails with the body's
/// error, or else the cleanup's.
#[track_caller]
fn with_cleanup(
    body: impl FnOnce() -> Result<(), String>,
    cleanup: impl FnOnce() -> Result<(), String>,
) {
    let result = body();
    let cleaned = cleanup();
    expect(result);
    expect(cleaned.map_err(|e| format!("cleanup: {e}")));
}

/// The test app's endpoints and clients work against awake pods. This needs
/// no sleep or wake, so it tells a broken test app from a broken runtime. The
/// pods may sleep between the steps; each request wakes them, so UDP comes
/// right after one.
fn endpoints(suite: &Suite) {
    let mut failed = Vec::new();
    let mut check = |what: &str, result: Result<(), String>| match result {
        Ok(()) => step(&format!("{what}: ok")),
        Err(e) => failed.push(format!("{what}: {e}")),
    };
    let client = &suite.client;
    let web_ip = suite.ips[WEB].clone();
    let get = |url: &str| client.get(url, REQUEST);
    check(
        "/remote",
        get(&suite.url(WEB, "/remote")).and_then(|f| {
            let seen = f.value("remote_ip")?;
            (seen == suite.ips[CLIENT]).then_some(()).ok_or(format!(
                "saw {seen}, not the client's {}",
                suite.ips[CLIENT]
            ))
        }),
    );
    check(
        "/health switched to failing and back",
        (|| {
            get(&suite.url(WEB, "/health/fail"))?;
            let failing = client.fetch_one(&suite.url(WEB, "/health"), REQUEST)?;
            let grpc =
                pairs(&client.run(&["grpchealth", &format!("{web_ip}:{GRPC_PORT}")], REQUEST)?);
            get(&suite.url(WEB, "/health/ok"))?;
            let ok = get(&suite.url(WEB, "/health"))?;
            match (
                failing.status,
                grpc.get("serving").map(String::as_str),
                ok.lines.first(),
            ) {
                (Some(500), Some("NOT_SERVING"), Some(line)) if line == "ok" => Ok(()),
                other => Err(format!("{other:?}")),
            }
        })(),
    );
    check(
        "HTTPS /health",
        get(&format!("https://{web_ip}:{HTTPS_PORT}/health")).map(drop),
    );
    check(
        "gRPC health",
        client
            .run(&["grpchealth", &format!("{web_ip}:{GRPC_PORT}")], REQUEST)
            .and_then(|out| match pairs(&out).get("serving").map(String::as_str) {
                Some("SERVING") => Ok(()),
                _ => Err(out),
            }),
    );
    check(
        "/stream",
        client
            .fetch_one(&suite.url(WEB, "/stream?seconds=2&every=1"), REQUEST)
            .and_then(|f| match f.value("chunks") {
                Ok("3") if f.ok() && f.total_ms >= 1900.0 => Ok(()),
                _ => Err(f.to_string()),
            }),
    );
    check(
        "/ws with pings",
        client
            .run(
                &[
                    "ws",
                    "-hold",
                    "3s",
                    &format!("ws://{web_ip}:{PORT}/ws?ping=1"),
                ],
                REQUEST,
            )
            .and_then(|out| {
                let ws = pairs(&out);
                let pings: u32 = ws.get("pings").and_then(|p| p.parse().ok()).unwrap_or(0);
                (ws.contains_key("first_conn") && pings >= 2)
                    .then_some(())
                    .ok_or(out)
            }),
    );
    check(
        "/call to another pod",
        get(&suite.url(WEB, &format!("/call?url={}", suite.url(CALLEE, "/remote")))).and_then(
            |f| match (f.value("call_status"), f.value("callee.remote_ip")) {
                (Ok("200"), Ok(ip)) if ip == web_ip => Ok(()),
                _ => Err(format!("{f}: {:?}", f.body)),
            },
        ),
    );
    check(
        "/busy",
        get(&suite.url(WEB, "/busy?seconds=1")).and_then(|f| f.value("busy_seconds").map(drop)),
    );
    check(
        "UDP echo",
        get(&suite.url(WEB, "/ping")).and_then(|_| {
            let out = client.run(
                &["udp", "-n", "3", &format!("{web_ip}:{UDP_PORT}")],
                REQUEST,
            )?;
            (pairs(&out).get("received").map(String::as_str) == Some("3"))
                .then_some(())
                .ok_or(out)
        }),
    );
    check(
        "ICMP echo",
        client
            .run(&["ping", "-count", "2", &web_ip], REQUEST)
            .and_then(|out| {
                (pairs(&out).get("received").map(String::as_str) == Some("2"))
                    .then_some(())
                    .ok_or(out)
            }),
    );
    check(
        "a kept connection",
        client
            .run(&["pool", &suite.url(WEB, "/id")], REQUEST)
            .and_then(|out| {
                (pairs(&out).get("first.status").map(String::as_str) == Some("200"))
                    .then_some(())
                    .ok_or(out)
            }),
    );
    check(
        "TCP connect",
        client
            .run(&["dial", &format!("{web_ip}:{PORT}")], REQUEST)
            .and_then(|out| {
                pairs(&out)
                    .contains_key("connect_ms")
                    .then_some(())
                    .ok_or(out)
            }),
    );
    check(
        "the unready app",
        client
            .fetch_one(&suite.url(UNREADY, "/health"), REQUEST)
            .and_then(|f| (f.status == Some(500)).then_some(()).ok_or(f.to_string())),
    );
    check(
        "kubelet's probes of every kind",
        suite.rows().and_then(|rows| {
            let unready: Vec<&str> = PROBE_PODS
                .into_iter()
                .chain([UNHEALTHY, DOOMED])
                .filter(|name| rows.get(*name).is_none_or(|row| row.ready != "True"))
                .collect();
            unready
                .is_empty()
                .then_some(())
                .ok_or(format!("not Ready: {unready:?}"))
        }),
    );
    assert_none(&failed);
}

/// The five probe pods sleep by themselves, and stay asleep for 3 minutes
/// while kubelet probes them: no wake, no VM process, Ready the whole time,
/// no `Unhealthy` event and no restart.
fn probes_leave_pods_asleep(suite: &Suite) {
    for name in PROBE_PODS {
        expect(suite.wait_for_idle_sleep(name, suite.start));
    }
    let cursor = suite.cursor();
    let rows = expect(suite.rows());
    let unhealthy = expect(suite.unhealthy());
    step(&format!(
        "the probe pods sleep; watching them for {PROBE_WINDOW:?}"
    ));
    let mut seen = Vec::new();
    let held = wake::holds_for(PROBE_WINDOW, Duration::from_secs(10), || {
        let now = suite.rows()?;
        for name in PROBE_PODS {
            let state = suite.state(name)?;
            let ready = now
                .get(name)
                .map(|row| row.ready.clone())
                .unwrap_or_default();
            if state != PodState::Asleep || suite.vm_count(name) != 0 || ready != "True" {
                seen.push(format!(
                    "pod {name}: {state}, {} VM processes, Ready {ready:?}",
                    suite.vm_count(name)
                ));
            }
        }
        Ok(seen.is_empty())
    });
    let mut failed = Vec::new();
    match held {
        Ok(None) => {}
        Ok(Some(after)) => failed.push(format!("after {after:?}: {}", seen.join("; "))),
        Err(e) => failed.push(e),
    }
    let after = expect(suite.rows());
    let unhealthy_after = expect(suite.unhealthy());
    for name in PROBE_PODS {
        let mut problems = Vec::new();
        match suite.events_since(cursor, name) {
            Ok(events) => {
                let resumes: Vec<String> = events
                    .iter()
                    .filter(|e| e.is_resume())
                    .map(|e| e.raw.to_string())
                    .collect();
                if !resumes.is_empty() {
                    problems.push(format!("woke: {resumes:?}"));
                }
            }
            Err(e) => problems.push(e),
        }
        let (before, now) = (rows.get(name), after.get(name));
        if before.map(|r| &r.ready_since) != now.map(|r| &r.ready_since) {
            problems.push(format!("Ready changed: {before:?} to {now:?}"));
        }
        if now.is_none_or(|r| r.restarts != 0) {
            problems.push(format!("restarts: {now:?}"));
        }
        let count = |map: &HashMap<String, u64>| map.get(name).copied().unwrap_or(0);
        if count(&unhealthy_after) != count(&unhealthy) {
            problems.push(format!(
                "Unhealthy events: {} before, {} after",
                count(&unhealthy),
                count(&unhealthy_after)
            ));
        }
        let outcome = if problems.is_empty() && failed.is_empty() {
            Ok(())
        } else {
            Err(problems.join("; "))
        };
        if name == PROBES_EXEC {
            suite.records.borrow_mut().exec_window = Some(outcome.clone());
        }
        if !problems.is_empty() {
            failed.push(format!("pod {name}: {}", problems.join("; ")));
        }
    }
    assert_none(&failed);
    step(&format!(
        "the five probe pods slept {PROBE_WINDOW:?} under kubelet's probes"
    ));
}

/// The exec probe pod, whose probe runs under Rosetta, slept through the
/// probe check without a wake; an exec with the probe's command gets the
/// cached answer without a wake; an exec of another command wakes it.
fn exec_probes(suite: &Suite) {
    let mut failed = Vec::new();
    match suite.records.borrow().exec_window.clone() {
        Some(Ok(())) => {}
        Some(Err(e)) => failed.push(format!("the probe check: {e}")),
        None => failed.push("the probe check did not watch the exec probe pod".into()),
    }
    let pod = suite.pod(PROBES_EXEC);
    expect(suite.wait_for_idle_sleep(PROBES_EXEC, suite.start));
    let cursor = suite.cursor();
    let cached = pod.exec("app", &EXEC_PROBE);
    let resumes = expect(suite.events_since(cursor, PROBES_EXEC))
        .into_iter()
        .filter(|e| e.is_resume())
        .count();
    if !cached.success() || cached.stdout.trim() != "ok" || resumes != 0 {
        failed.push(format!(
            "an exec of the probe's command should get the cached answer without a wake \
             (exec probe cache), got {resumes} resumes and {}",
            describe(&cached)
        ));
    }
    let cursor = suite.cursor();
    let other = pod.exec("app", &["/testapp", "echo", "woken"]);
    let resumes: Vec<Event> = expect(suite.events_since(cursor, PROBES_EXEC))
        .into_iter()
        .filter(|e| e.is_resume())
        .collect();
    let exec_wake = matches!(&resumes[..], [wake] if wake.reason == "exec");
    if other.stdout.trim() != "woken" || !exec_wake {
        failed.push(format!(
            "an exec of another command should wake the pod once with the reason exec: {}; \
             resumes {:?}",
            describe(&other),
            resumes
                .iter()
                .map(|e| e.raw.to_string())
                .collect::<Vec<_>>()
        ));
    }
    assert_none(&failed);
}

/// A request from the Mac through a NodePort to the probe port of a sleeping
/// pod wakes it and gets the app's answer, not a probe answer.
fn node_port(suite: &Suite) {
    expect(suite.wait_for_idle_sleep(PROBES_HTTP, suite.start));
    let cursor = suite.cursor();
    let addr = ([127, 0, 0, 1], suite.cluster.node_port()).into();
    let answer = http_get(addr, "/id", REQUEST);
    let wakes = expect(suite.traffic_wakes(cursor, PROBES_HTTP));
    let (status, body) = expect(answer.map_err(|e| {
        format!("the NodePort did not answer for the sleeping pod (wake on traffic): {e}")
    }));
    assert_eq!(status, 200, "the NodePort's answer: {body:?}");
    assert!(
        body.contains(&format!("pid={}", suite.identities[PROBES_HTTP].pid)),
        "the NodePort's answer is not the app's /id: {body:?}"
    );
    assert_eq!(wakes.len(), 1, "traffic wakes of {PROBES_HTTP}");
    step(&format!(
        "the NodePort woke {PROBES_HTTP}, from {:?}",
        Traffic::of(&wakes[0])
    ));
}

/// Waits until the node service answers probes of the three pods as kubelet
/// would, from the node, and prints how long that took.
const PROBES_ANSWERED: &str = r#"t=$1 http=$2 grpc=$3 tcp=$4 ua=$5
start=$(date +%s%N)
while :; do
  ms=$(( ($(date +%s%N) - start) / 1000000 ))
  if "$t" fetch -timeout 1s -user-agent "$ua" "$http" | grep -qx '0.status=200' &&
     "$t" grpchealth -timeout 1s "$grpc" | grep -qx 'serving=SERVING' &&
     "$t" dial -timeout 1s "$tcp" | grep -q '^connect_ms='; then
    echo "answered_ms=$ms"; exit 0
  fi
  if [ "$ms" -ge 30000 ]; then echo "answered_ms="; exit 0; fi
  sleep 0.1
done"#;

/// How long the node took to answer the probes of [`PROBES_ANSWERED`].
fn probes_answered(suite: &Suite) -> Result<Option<Duration>, String> {
    let http = format!("{}/health", suite.url(PROBES_HTTP, ""));
    let grpc = format!("{}:{GRPC_PORT}", suite.ips[PROBES_GRPC]);
    let tcp = format!("{}:{PORT}", suite.ips[PROBES_TCP]);
    let output = suite.node().exec(
        &[
            "sh",
            "-c",
            PROBES_ANSWERED,
            "sh",
            &suite.testapp,
            &http,
            &grpc,
            &tcp,
            PROBE_USER_AGENT,
        ],
        Duration::from_secs(60),
    );
    if !output.success() {
        return Err(describe(&output));
    }
    Ok(pairs(&output.stdout)
        .get("answered_ms")
        .and_then(|ms| ms.parse().ok())
        .map(Duration::from_millis))
}

/// A SIGKILL of the node service with three probe pods asleep. After the
/// restart the node answers their probes again within 5 s; once the probes'
/// timeout has passed, kubelet records no new failure and the pods stay
/// Ready without a restart; a request sent during the outage connects after
/// a SYN retransmit.
fn node_service_kill(suite: &Suite) {
    let pods = [PROBES_HTTP, PROBES_TCP, PROBES_GRPC];
    for name in pods {
        expect(suite.wait_for_idle_sleep(name, suite.start));
    }
    match expect(probes_answered(suite)) {
        Some(took) if took < PROBES_BACK => {}
        other => panic!(
            "the node should answer the sleeping pods' probes at once (probe answers): {other:?}"
        ),
    }
    let unhealthy = expect(suite.unhealthy());
    let cursor = suite.cursor();
    let old = expect(suite.api.kill_node_service());
    let outage = expect(suite.client.spawn(
        "outage",
        &["fetch", "-timeout", "30s", &suite.url(PROBES_TCP, "/id")],
    ));
    expect(
        suite
            .api
            .wait_for_node_service(old, Duration::from_secs(60)),
    );
    let answered = expect(probes_answered(suite));
    let request = expect(
        outage
            .wait(Duration::from_secs(60))
            .and_then(|out| parse_fetch(&out)),
    )
    .remove(0);
    let mut failed = Vec::new();
    match answered {
        Some(took) if took <= PROBES_BACK => {
            step(&format!("probes answered {took:?} after the restart"))
        }
        other => failed.push(format!(
            "the node did not answer the probes within {PROBES_BACK:?} of the restart: {other:?}"
        )),
    }
    if !request.ok() || request.connect_ms.unwrap_or_default() < RETRANSMIT_MS {
        failed.push(format!(
            "the request during the outage should connect after a retransmit and be answered: \
             {request}"
        ));
    }
    // A probe whose SYN and first retransmit fell in the outage times out, as
    // probes do while the node service is down, and kubelet records an
    // Unhealthy event for each failed probe. None may fail once the node
    // service is back and its probes' timeout passed.
    thread::sleep(PROBE_TIMEOUT);
    let unhealthy_back = expect(suite.unhealthy());
    thread::sleep(Duration::from_secs(20));
    let rows = expect(suite.rows());
    let unhealthy_after = expect(suite.unhealthy());
    for name in pods {
        let row = rows.get(name).cloned().unwrap_or_default();
        let count = |map: &HashMap<String, u64>| map.get(name).copied().unwrap_or(0);
        let (before, back, after) = (
            count(&unhealthy),
            count(&unhealthy_back),
            count(&unhealthy_after),
        );
        if row.ready != "True" || row.restarts != 0 || after != back {
            failed.push(format!(
                "pod {name} after the restart: {row:?}, Unhealthy events {before} before the \
                 kill, {back} once the node service was back, {after} 20 s later"
            ));
        } else if back != before {
            step(&format!(
                "pod {name}: {} probes failed during the outage, and kubelet kept it Ready",
                back - before
            ));
        }
    }
    for name in [PROBES_HTTP, PROBES_GRPC] {
        match suite.events_since(cursor, name) {
            Ok(events) if events.iter().all(|e| !e.is_resume()) => {}
            Ok(events) => failed.push(format!(
                "pod {name} woke: {:?}",
                events.iter().map(|e| e.raw.to_string()).collect::<Vec<_>>()
            )),
            Err(e) => failed.push(e),
        }
    }
    assert_none(&failed);
}

/// 20 automatic cycles through `web`'s Service. The pod sleeps by itself
/// after its idle timeout, and the client's next call wakes it. Every call
/// answers with the same `/id` and `/hash`; the pod keeps its IP and sandbox
/// and restarts nothing; there are 20 sleeps with the reason `idle` and 20
/// wakes with the reason `traffic`.
fn cycles(suite: &Suite) {
    expect(suite.ensure_awake(WEB));
    let start = suite.cursor();
    let id_url = suite.service_url(WEB, "/id");
    let hash_url = suite.service_url(WEB, "/hash");
    let mut since = start;
    for cycle in 1..=CYCLES {
        let context = |e: String| format!("cycle {cycle}: {e}");
        expect(suite.wait_for_idle_sleep(WEB, since).map_err(context));
        let cursor = suite.cursor();
        let id = expect(suite.client.get(&id_url, REQUEST).map_err(context));
        expect(suite.check_identity(WEB, &id).map_err(context));
        let hash = expect(suite.client.get(&hash_url, REQUEST).map_err(context));
        assert_eq!(
            expect(hash.value("sha256")),
            suite.web_hash,
            "cycle {cycle}: /hash"
        );
        expect(suite.check_unchanged(WEB).map_err(context));
        let events = expect(suite.events_since(cursor, WEB));
        let wakes: Vec<&Event> = events.iter().filter(|e| e.is_resume()).collect();
        let [wake] = wakes[..] else {
            panic!("cycle {cycle}: {} resumes for the call", wakes.len());
        };
        assert!(
            wake::is_traffic_wake(wake),
            "cycle {cycle}: the wake's reason is {}, not traffic: {}",
            wake.reason,
            wake.raw
        );
        let awake = events
            .iter()
            .find(|e| e.from == "waking" && e.to == "awake");
        suite.records.borrow_mut().cycles.push(Cycle {
            first_byte_ms: id.first_byte_ms.unwrap_or(id.total_ms),
            connect_ms: id.connect_ms.unwrap_or_default(),
            capture_to_release_ms: awake.and_then(wake::capture_to_release_ms),
            steps: awake.map(|e| e.steps_ms.clone()).unwrap_or_default(),
            from: Traffic::of(wake),
        });
        step(&format!("cycle {cycle} of {CYCLES}: {id}"));
        since = cursor;
    }
    let events = expect(suite.events_since(start, WEB));
    let sleeps = events.iter().filter(|e| wake::is_idle_sleep(e)).count();
    let wakes = events.iter().filter(|e| wake::is_traffic_wake(e)).count();
    assert_eq!(
        (sleeps, wakes),
        (CYCLES, CYCLES),
        "idle sleeps and traffic wakes"
    );
    let sources: HashSet<String> = suite
        .records
        .borrow()
        .cycles
        .iter()
        .map(|c| format!("{:?}", c.from))
        .collect();
    step(&format!("the wakes came from {sources:?}"));
}

/// The first answer after each wake of the automatic cycles, from the
/// client's connect to the first byte, has a median under 0.3 s and a 95th
/// percentile under 0.6 s, and each wake's event has its step times and the
/// time from capture to release.
fn latency(suite: &Suite) {
    let records = suite.records.borrow();
    assert_eq!(
        records.cycles.len(),
        CYCLES,
        "the cycle check completed {} of {CYCLES} cycles",
        records.cycles.len()
    );
    let first: Vec<f64> = records.cycles.iter().map(|c| c.first_byte_ms).collect();
    let connect: Vec<f64> = records.cycles.iter().map(|c| c.connect_ms).collect();
    let capture: Vec<f64> = records
        .cycles
        .iter()
        .filter_map(|c| c.capture_to_release_ms)
        .collect();
    step(&format!(
        "first answer {}; connect {}; capture to release {}",
        summary(&first, "ms"),
        summary(&connect, "ms"),
        summary(&capture, "ms")
    ));
    let mut names: Vec<String> = Vec::new();
    for cycle in records.cycles.iter() {
        for (name, _) in &cycle.steps {
            if !names.contains(name) {
                names.push(name.clone());
            }
        }
    }
    for name in &names {
        let values: Vec<f64> = records
            .cycles
            .iter()
            .filter_map(|c| c.steps.iter().find(|(n, _)| n == name).map(|s| s.1))
            .collect();
        step(&format!("wake step {name}: {}", summary(&values, "ms")));
    }
    let mut failed = Vec::new();
    let (median, p95) = (
        quantile(&first, 0.5).unwrap(),
        quantile(&first, 0.95).unwrap(),
    );
    if median >= MEDIAN_MS || p95 >= P95_MS {
        failed.push(format!(
            "first answer: median {median:.1} ms, p95 {p95:.1} ms; the limits are {MEDIAN_MS} \
             and {P95_MS} ms"
        ));
    }
    let incomplete = records
        .cycles
        .iter()
        .filter(|c| c.steps.is_empty() || c.capture_to_release_ms.is_none())
        .count();
    if incomplete > 0 {
        failed.push(format!(
            "{incomplete} wake events lack steps_ms or capture_to_release_ms"
        ));
    }
    assert_none(&failed);
}

/// 10 concurrent requests to a sleeping pod cause one resume, all are
/// answered, and all connect within 1 s, so no SYN was retransmitted.
fn concurrent_requests(suite: &Suite) {
    expect(suite.ensure_asleep(WEB));
    let cursor = suite.cursor();
    let answers = expect(suite.client.fetch(&suite.url(WEB, "/id"), 10, REQUEST));
    let mut failed = Vec::new();
    for (i, answer) in answers.iter().enumerate() {
        let identity = suite.check_identity(WEB, answer);
        if !answer.ok() || identity.is_err() || answer.connect_ms.is_none_or(|ms| ms >= 1000.0) {
            failed.push(format!("request {i}: {answer}; {identity:?}"));
        }
    }
    match suite.traffic_wakes(cursor, WEB) {
        Ok(wakes) if wakes.len() == 1 => {}
        Ok(wakes) => failed.push(format!(
            "{} resumes for 10 concurrent requests (merged traffic wakes)",
            wakes.len()
        )),
        Err(e) => failed.push(e),
    }
    assert_none(&failed);
    let connects: Vec<f64> = answers.iter().filter_map(|a| a.connect_ms).collect();
    step(&format!(
        "10 concurrent requests: connect {}",
        summary(&connects, "ms")
    ));
}

/// `/remote` shows the client pod's IP, awake and on a wake, at the pod IP
/// and through the Service.
fn client_ip(suite: &Suite) {
    let mut failed = Vec::new();
    let want = &suite.ips[CLIENT];
    for (what, url) in [
        ("the pod IP", suite.url(WEB, "/remote")),
        ("the Service", suite.service_url(WEB, "/remote")),
    ] {
        for asleep in [false, true] {
            let prepared = if asleep {
                suite.ensure_asleep(WEB)
            } else {
                suite.ensure_awake(WEB)
            };
            let cursor = suite.cursor();
            let seen = prepared
                .and_then(|()| suite.client.get(&url, REQUEST))
                .and_then(|f| f.value("remote_ip").map(str::to_string));
            let woke = suite.traffic_wakes(cursor, WEB).map(|w| w.len());
            match (seen, woke) {
                (Ok(ip), Ok(wakes)) if ip == *want && wakes == usize::from(asleep) => {}
                (seen, woke) => failed.push(format!(
                    "through {what}, {}: saw {seen:?}, expected {want}; traffic wakes {woke:?}",
                    if asleep { "on a wake" } else { "awake" }
                )),
            }
        }
    }
    assert_none(&failed);
}

/// A call from `caller` wakes the sleeping `callee`, through its Service and
/// at its pod IP, and the wake names the caller and the port; a call to the
/// sleeping caller that calls the sleeping callee wakes both.
fn pod_to_pod(suite: &Suite) {
    let mut failed = Vec::new();
    let caller = suite.qualified(CALLER);
    for (what, target) in [
        ("its Service", suite.service_url(CALLEE, "/remote")),
        ("its pod IP", suite.url(CALLEE, "/remote")),
    ] {
        let result = (|| -> Result<(), String> {
            suite.ensure_awake(CALLER)?;
            suite.ensure_asleep(CALLEE)?;
            let cursor = suite.cursor();
            let call = suite
                .client
                .get(&suite.url(CALLER, &format!("/call?url={target}")), REQUEST)?;
            check_call(suite, &call)?;
            suite.check_woken_by(cursor, CALLEE, &caller)
        })();
        if let Err(e) = result {
            failed.push(format!("a call to the callee through {what}: {e}"));
        }
    }
    let chain = (|| -> Result<(), String> {
        suite.ensure_asleep(CALLER)?;
        suite.ensure_asleep(CALLEE)?;
        let cursor = suite.cursor();
        let target = suite.service_url(CALLEE, "/remote");
        let call = suite.client.get(
            &suite.service_url(CALLER, &format!("/call?url={target}")),
            REQUEST,
        )?;
        check_call(suite, &call)?;
        suite.check_woken_by(cursor, CALLER, &suite.qualified(CLIENT))?;
        suite.check_woken_by(cursor, CALLEE, &caller)
    })();
    if let Err(e) = chain {
        failed.push(format!("a chain of two sleeping pods: {e}"));
    }
    assert_none(&failed);
}

/// Fails unless the caller's `/call` reached the callee, which saw the
/// caller's pod IP.
fn check_call(suite: &Suite, call: &Fetched) -> Result<(), String> {
    match (call.value("call_status"), call.value("callee.remote_ip")) {
        (Ok("200"), Ok(ip)) if ip == suite.ips[CALLER] => Ok(()),
        _ => Err(format!(
            "the call failed or came from elsewhere: {call}: {:?}",
            call.body
        )),
    }
}

/// A stream and a WebSocket with pings, each 3 idle timeouts long, keep `web`
/// awake, and it sleeps within the idle timeout and 15 s after each ends.
/// Then a quiet WebSocket lets it sleep, and the next message wakes it on the
/// same connection.
fn streams(suite: &Suite) {
    let mut failed = Vec::new();
    let mut report = |what: &str, result: Result<(), String>| {
        if let Err(e) = result {
            failed.push(format!("{what}: {e}"));
        }
    };
    let ip = suite.ips[WEB].clone();
    report(
        "a stream",
        (|| {
            suite.ensure_awake(WEB)?;
            let cursor = suite.cursor();
            let url = suite.url(WEB, &format!("/stream?seconds={STREAM_SECONDS}&every=1"));
            let stream = suite
                .client
                .fetch_one(&url, REQUEST + Duration::from_secs(STREAM_SECONDS))?;
            if !stream.ok() || stream.value("chunks")? != (STREAM_SECONDS + 1).to_string() {
                return Err(format!("the stream: {stream}"));
            }
            suite.check_no_pause(cursor, WEB)?;
            suite.sleeps_within(WEB, cursor, SLEEP_START).map(drop)
        })(),
    );
    report(
        "a WebSocket with pings",
        (|| {
            suite.ensure_awake(WEB)?;
            let cursor = suite.cursor();
            let hold = format!("{STREAM_SECONDS}s");
            let url = format!("ws://{ip}:{PORT}/ws?ping={WS_PING_SECONDS}");
            let out = suite.client.run(
                &["ws", "-hold", &hold, &url],
                REQUEST + Duration::from_secs(STREAM_SECONDS),
            )?;
            let pings: u64 = pairs(&out)
                .get("pings")
                .and_then(|p| p.parse().ok())
                .unwrap_or(0);
            if pings < STREAM_SECONDS / WS_PING_SECONDS - 1 {
                return Err(format!("the client answered {pings} pings: {out:?}"));
            }
            suite.check_no_pause(cursor, WEB)?;
            suite.sleeps_within(WEB, cursor, SLEEP_START).map(drop)
        })(),
    );
    report(
        "a quiet WebSocket",
        (|| {
            suite.ensure_awake(WEB)?;
            let url = format!("ws://{ip}:{PORT}/ws");
            let trigger = suite.client.trigger_file("ws-quiet");
            let ws = suite.client.spawn(
                "ws-quiet",
                &["ws", "-timeout", "5m", "-then-file", &trigger, &url],
            )?;
            ws.wait_for("pings=", REQUEST)?;
            let cursor = suite.cursor();
            suite.sleeps_within(WEB, cursor, SLEEP_START)?;
            let cursor = suite.cursor();
            ws.trigger()?;
            let out = pairs(&ws.wait(Duration::from_secs(60))?);
            let (first, second) = (out.get("first_conn"), out.get("second_conn"));
            if first.is_none() || first != second {
                return Err(format!(
                    "the second message came on another connection: {out:?}"
                ));
            }
            let ms: f64 = out
                .get("second_ms")
                .and_then(|ms| ms.parse().ok())
                .unwrap_or(f64::MAX);
            if ms >= 5000.0 {
                return Err(format!("the second message took {ms} ms"));
            }
            suite.check_woken_by(cursor, WEB, &suite.qualified(CLIENT))
        })(),
    );
    assert_none(&failed);
}

/// Only connections opened to a pod keep it awake and wake it. `follower`
/// opens three connections, each with a stream that its server pushes a line
/// every few seconds on: to the client pod at its IP, with no NAT; to the
/// client pod through its Service, whose pushes reach `follower` from the
/// Service's address after DNAT; and to a server on the Mac at the node's
/// default gateway, after SNAT. Each stream reaches the pod, which sleeps by
/// itself after its idle timeout anyway, and the pushes do not wake it while
/// the node holds them. A request to it through its Service then wakes it
/// once, from the client, and the test reports what became of each stream:
/// it goes on, it stalled, or the app saw it end.
fn outbound(suite: &Suite) {
    expect(suite.ensure_awake(FOLLOWER));
    let mac = Pusher::start(
        Duration::from_secs(PUSH_EVERY),
        Duration::from_secs(PUSH_SECONDS),
    );
    let stream = format!("/stream%3Fseconds%3D{PUSH_SECONDS}%26every%3D{PUSH_EVERY}");
    let streams = [
        ("pod", format!("{}:{PORT}", suite.ips[CLIENT])),
        ("service", format!("{}:{PORT}", suite.services[CLIENT])),
        ("mac", format!("{}:{}", suite.node().gateway(), mac.port())),
    ];
    let followed = |name: &str| {
        let url = suite.url(FOLLOWER, &format!("/followed?name={name}"));
        expect(suite.client.get(&url, REQUEST))
    };
    let lines = |f: &Fetched| f.value("lines").unwrap_or("?").to_string();

    let cursor = suite.cursor();
    for (name, server) in &streams {
        let url = format!("/follow?name={name}&url=http://{server}{stream}");
        let started = expect(suite.client.get(&suite.url(FOLLOWER, &url), REQUEST));
        assert_eq!(expect(started.value("follow")), "started", "{started}");
    }
    thread::sleep(Duration::from_secs(PUSH_EVERY + 1));
    for (name, server) in &streams {
        let before = followed(name);
        let received: u64 = expect(before.value("lines")).parse().unwrap_or_default();
        assert!(
            received > 0 && before.value("ended") == Ok("false"),
            "the stream {name} from {server} did not reach {FOLLOWER}: {before}, {:?}",
            before.body
        );
    }
    expect(suite.sleeps_within(FOLLOWER, cursor, SLEEP_START));

    let cursor = suite.cursor();
    thread::sleep(ASLEEP_WITH_PUSHES);
    let resumes: Vec<String> = expect(suite.events_since(cursor, FOLLOWER))
        .iter()
        .filter(|e| e.is_resume())
        .map(|e| e.raw.to_string())
        .collect();
    assert!(
        resumes.is_empty() && suite.vm_count(FOLLOWER) == 0,
        "the pushes on the pod's own connections woke it: {resumes:?}"
    );
    step(&format!(
        "{FOLLOWER} slept through {} s of pushes every {PUSH_EVERY} s from {}",
        ASLEEP_WITH_PUSHES.as_secs(),
        streams
            .iter()
            .map(|(name, server)| format!("{name} ({server})"))
            .collect::<Vec<_>>()
            .join(", ")
    ));

    let cursor = suite.cursor();
    let woken: Vec<Fetched> = streams
        .iter()
        .map(|(name, _)| {
            let url = suite.service_url(FOLLOWER, &format!("/followed?name={name}"));
            expect(suite.client.get(&url, REQUEST))
        })
        .collect();
    expect(suite.check_woken_by(cursor, FOLLOWER, &suite.qualified(CLIENT)));
    let held_ms = expect(suite.events_since(cursor, FOLLOWER))
        .iter()
        .find(|e| e.from == "waking" && e.to == "awake")
        .and_then(wake::capture_to_release_ms);
    // A push came within PUSH_EVERY of the sleep and waited for the wake.
    let held_at_least_ms = (ASLEEP_WITH_PUSHES.as_secs() - PUSH_EVERY) as f64 * 1000.0;
    assert!(
        held_ms.is_some_and(|ms| ms >= held_at_least_ms),
        "the pushes should have reached {FOLLOWER} while it slept: its first held frame \
         waited {held_ms:?} ms for the release, less than {held_at_least_ms} ms"
    );
    step(&format!(
        "the first frame held while {FOLLOWER} slept waited {:.1} s",
        held_ms.unwrap_or_default() / 1000.0
    ));
    thread::sleep(Duration::from_secs(2 * PUSH_EVERY + 1));
    for ((name, _), woken) in streams.iter().zip(&woken) {
        let later = followed(name);
        let outcome = match (later.value("ended"), later.value("error")) {
            (Ok("false"), _) if lines(&later) != lines(woken) => "it went on".to_string(),
            (Ok("true"), Ok(error)) if !error.is_empty() => {
                format!("the app saw it end: {error}")
            }
            _ => "it stalled".to_string(),
        };
        step(&format!(
            "the stream {name} after the wake: {outcome}; lines {} at the wake, {} {} s later, \
             the last {:?} {} ms before",
            lines(woken),
            lines(&later),
            2 * PUSH_EVERY + 1,
            later.value("last").unwrap_or("?"),
            later.value("last_ago_ms").unwrap_or("?"),
        ));
    }
}

/// An idle connection of a keep-alive pool with Go's default TCP keepalive to
/// a sleeping pod: its keepalive probes reach the node unanswered and wake
/// nothing, and the next request on the same connection wakes the pod and is
/// answered.
fn keepalive(suite: &Suite) {
    expect(suite.ensure_awake(WEB));
    let trigger = suite.client.trigger_file("pool");
    let pool = expect(suite.client.spawn(
        "pool",
        &[
            "pool",
            "-timeout",
            "5m",
            "-then-file",
            &trigger,
            &suite.url(WEB, "/id"),
        ],
    ));
    let first = expect(pool.wait_for("first.status=", REQUEST));
    assert_eq!(
        pairs(&first).get("first.status").map(String::as_str),
        Some("200"),
        "{first:?}"
    );
    expect(suite.ensure_asleep(WEB));
    let cursor = suite.cursor();
    thread::sleep(KEEPALIVE_WAIT);
    let resumes = expect(suite.events_since(cursor, WEB))
        .into_iter()
        .filter(|e| e.is_resume())
        .count();
    assert_eq!(resumes, 0, "the keepalive probes woke the pod");
    assert_eq!(suite.vm_count(WEB), 0, "the pod has a VM process");
    expect(pool.trigger());
    let out = pairs(&expect(pool.wait(Duration::from_secs(90))));
    let probes: u64 = out
        .get("keepalive_probes")
        .and_then(|p| p.parse().ok())
        .unwrap_or(0);
    let mut failed = Vec::new();
    if probes == 0 {
        failed.push(format!(
            "the connection has no unanswered keepalive probes after {KEEPALIVE_WAIT:?} asleep: {out:?}"
        ));
    }
    let same =
        out.contains_key("second.local") && out.get("second.local") == out.get("first.local");
    if out.get("second.status").map(String::as_str) != Some("200")
        || out.get("second.reused").map(String::as_str) != Some("true")
        || !same
    {
        failed.push(format!(
            "the second request should be answered on the same connection (held data): {out:?}"
        ));
    }
    if let Err(e) = suite.check_woken_by(cursor, WEB, &suite.qualified(CLIENT)) {
        failed.push(e);
    }
    assert_none(&failed);
    step(&format!(
        "{probes} keepalive probes unanswered while asleep; the pool's next request was answered"
    ));
}

/// UDP to a sleeping pod is dropped and wakes nothing, `ping` is answered,
/// the node service logs no error, and UDP works after a TCP wake.
fn udp_and_icmp(suite: &Suite) {
    expect(suite.ensure_asleep(WEB));
    let mark = expect(suite.api.log_mark());
    let cursor = suite.cursor();
    let udp_addr = format!("{}:{UDP_PORT}", suite.ips[WEB]);
    let udp = pairs(&expect(
        suite
            .client
            .run(&["udp", "-n", "3", "-timeout", "1s", &udp_addr], REQUEST),
    ));
    let ping = pairs(&expect(
        suite
            .client
            .run(&["ping", "-count", "3", &suite.ips[WEB]], REQUEST),
    ));
    let mut failed = Vec::new();
    if udp.get("received").map(String::as_str) != Some("0") {
        failed.push(format!("UDP to the sleeping pod was answered: {udp:?}"));
    }
    if ping.get("received").map(String::as_str) != Some("3") {
        failed.push(format!(
            "ping of the sleeping pod (answers ICMP echo): {ping:?}"
        ));
    }
    match (suite.state(WEB), suite.events_since(cursor, WEB)) {
        (Ok(PodState::Asleep), Ok(events)) if events.iter().all(|e| !e.is_resume()) => {}
        (state, events) => failed.push(format!(
            "after UDP and ping the pod is {state:?}, with events {:?}",
            events.map(|e| e.iter().map(|e| e.raw.to_string()).collect::<Vec<_>>())
        )),
    }
    match suite.api.log_since(mark) {
        Ok(log) => {
            let errors = wake::error_lines(&log);
            if !errors.is_empty() {
                failed.push(format!("{NODE_SERVICE_LOG} has errors: {errors:?}"));
            }
        }
        Err(e) => failed.push(e),
    }
    let after = suite
        .client
        .get(&suite.url(WEB, "/ping"), REQUEST)
        .and_then(|_| suite.client.run(&["udp", "-n", "3", &udp_addr], REQUEST));
    match after.map(|out| pairs(&out)) {
        Ok(udp) if udp.get("received").map(String::as_str) == Some("3") => {}
        other => failed.push(format!("UDP after a TCP wake: {other:?}")),
    }
    assert_none(&failed);
}

/// A request as soon as a pause shows is answered in under 1 s without a SYN
/// retransmit, and cancels the pause: no resume follows. A delta pause of an
/// idle pod can end before the request comes; that attempt is repeated, at
/// most 3 times.
fn wake_during_pause(suite: &Suite) {
    for _ in 0..3 {
        if wake_during_pause_once(suite) {
            return;
        }
        eprintln!("[e2e] the pause ended before the request came");
    }
    panic!("the pause ended before the request came in 3 attempts");
}

/// One attempt of [`wake_during_pause`]: false when the pause was already
/// asleep when the request came, so that nothing was checked.
fn wake_during_pause_once(suite: &Suite) -> bool {
    expect(suite.ensure_awake(WEB));
    let vm = expect(suite.vm_pid(WEB));
    let sandbox = suite.sandbox(WEB);
    let dir = suite.node().sh("mktemp -d");
    let dir = dir.trim();
    let cursor = suite.cursor();
    let script = format!(
        r#"d={dir}
({hibernate} >$d/h.out 2>$d/h.err; echo $? >$d/h.status) &
state=none
for _ in $(seq 2000); do
  state=$({get} 2>/dev/null | jq -r .state 2>/dev/null)
  case $state in pausing | asleep) break ;; esac
  sleep 0.005
done
echo "state_before=$state" >$d/f.out
{fetch} >>$d/f.out 2>&1
wait
cat $d/f.out
"#,
        hibernate = runtime::ctl_command(&runtime::hibernate_args(sandbox)),
        get = runtime::ctl_command(&["get", sandbox]),
        fetch = suite
            .client
            .shell(&["fetch", "-timeout", "5s", &suite.url(WEB, "/id")]),
    );
    let output = suite.node().sh(&script);
    let _ = suite.node().try_sh(&format!("rm -rf {dir}"));
    let state = pairs(&output)
        .get("state_before")
        .cloned()
        .unwrap_or_default();
    assert_eq!(state, "pausing", "the pod's state before the request");
    let request = expect(parse_fetch(&output)).remove(0);
    let events = expect(suite.events_since(cursor, WEB));
    let cancelled = events
        .iter()
        .any(|e| e.from == "pausing" && e.to == "awake" && e.cancelled);
    let slept = events
        .iter()
        .any(|e| e.from == "pausing" && e.to == "asleep");
    if slept && !cancelled {
        return false;
    }
    let resumes = events.iter().filter(|e| e.is_resume()).count();
    let mut failed = Vec::new();
    if !request.ok()
        || request.total_ms >= 1000.0
        || request.connect_ms.is_none_or(|ms| ms >= 1000.0)
    {
        failed.push(format!(
            "the request during the pause should be answered within 1 s (hold and \
             cancel): {request}"
        ));
    }
    if !cancelled || resumes != 0 {
        failed.push(format!(
            "the request should cancel the pause with no resume: {:?}",
            events.iter().map(|e| e.raw.to_string()).collect::<Vec<_>>()
        ));
    }
    match suite.vm_pid(WEB) {
        Ok(pid) if pid == vm => {}
        other => failed.push(format!("the VM process is {other:?}, was {vm}")),
    }
    assert_none(&failed);
    true
}

/// Waits until the node service's log after byte `$1` has the line `$2`, for
/// at most `$3` s, then runs `$4...`. Prints `reached=1` or `reached=0`.
const AFTER_LOG_LINE: &str = r#"mark=$1 line=$2 limit=$3 log=$4
shift 4
end=$(( $(date +%s) + limit ))
until tail -c +$((mark + 1)) "$log" | grep -qF -- "$line"; do
  if [ "$(date +%s)" -ge "$end" ]; then echo reached=0; exit 0; fi
  sleep 0.05
done
echo reached=1
"$@""#;

/// A request in the window between the idle decision and the hold (fault
/// point `idle:decided`, which waits) aborts the pause, and is answered.
fn recheck(suite: &Suite) {
    expect(suite.ensure_awake(WEB));
    let vm = expect(suite.vm_pid(WEB));
    let sandbox = suite.sandbox(WEB);
    with_cleanup(
        || {
            let mark = suite.api.log_mark()?;
            suite
                .api
                .arm_fault(FaultPoint::IdleDecided, Some(sandbox), NODE_SERVICE_RESTART)?;
            let cursor = suite.cursor();
            let line = FaultPoint::IdleDecided.log_line(Some(sandbox));
            let limit = (SLEEP_START + PAUSE_TIME).as_secs().to_string();
            let mark = mark.to_string();
            let fetch = suite
                .client
                .command(&["fetch", "-timeout", "10s", &suite.url(WEB, "/id")]);
            let mut args = vec![
                "sh",
                "-c",
                AFTER_LOG_LINE,
                "sh",
                &mark,
                &line,
                &limit,
                NODE_SERVICE_LOG,
            ];
            args.extend(fetch.iter().map(String::as_str));
            let output = suite.node().exec(&args, Duration::from_secs(120));
            if pairs(&output.stdout).get("reached").map(String::as_str) != Some("1") {
                return Err(format!(
                    "the node service did not reach fault point idle:decided for {WEB} within \
                     {limit} s (idle decision): {}",
                    describe(&output)
                ));
            }
            let request = parse_fetch(&output.stdout)?.remove(0);
            if !request.ok() {
                return Err(format!("the request in the window: {request}"));
            }
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let events = suite.events_since(cursor, WEB)?;
                let outcome = events
                    .iter()
                    .find(|e| e.to == "asleep" || wake::is_aborted_pause(e));
                match outcome {
                    Some(e) if e.to == "asleep" => {
                        return Err(format!(
                            "the pod went to sleep after the request: {:?}",
                            events.iter().map(|e| e.raw.to_string()).collect::<Vec<_>>()
                        ))
                    }
                    Some(_) => break,
                    None => {}
                }
                if Instant::now() >= deadline {
                    return Err(format!(
                        "no aborted pause after the request (recheck): {:?}",
                        events.iter().map(|e| e.raw.to_string()).collect::<Vec<_>>()
                    ));
                }
                thread::sleep(Duration::from_millis(500));
            }
            match suite.vm_pid(WEB)? {
                pid if pid == vm => Ok(()),
                pid => Err(format!("the VM process is {pid}, was {vm}")),
            }
        },
        || suite.api.restart_node_service().map(drop),
    );
}

/// A pod whose VM keeps a vCPU busy stays awake past its idle timeout, and
/// sleeps once the CPU window has passed. A busy pod with the CPU rule off
/// sleeps after its idle timeout all the same.
fn busy_cpu(suite: &Suite) {
    let busy = |name: &str| -> Result<u64, String> {
        suite.ensure_awake(name)?;
        let cursor = suite.cursor();
        let busy = suite.client.get(
            &suite.url(name, &format!("/busy?seconds={BUSY_SECONDS}")),
            REQUEST,
        )?;
        busy.value("busy_seconds")?;
        Ok(cursor)
    };
    let cursor = expect(busy(WEB));
    thread::sleep(Duration::from_secs(BUSY_SECONDS));
    let paused = suite.check_no_pause(cursor, WEB);
    expect(suite.sleeps_within(WEB, cursor, AFTER_BUSY).map(drop));
    expect(paused.map_err(|e| format!("while its vCPU was busy: {e}")));
    let cursor = expect(busy(CPU_OFF));
    expect(
        suite
            .sleeps_within(CPU_OFF, cursor, SLEEP_START)
            .map(drop)
            .map_err(|e| format!("with {CPU_RULE_OFF:?}: {e}")),
    );
}

/// An app that starts failing while awake never sleeps, and kubelet restarts
/// it; a pod that never becomes ready never sleeps, and an event says why; a
/// pod whose resume fails shows failing probes, fails after three tries with
/// exit 255, and is recreated.
fn failing_app(suite: &Suite) {
    let mut failed = Vec::new();
    for (what, result) in [
        ("an app that starts failing", failing_while_awake(suite)),
        ("a pod that never becomes ready", never_ready(suite)),
        ("a resume that fails", failed_restore(suite)),
    ] {
        match result {
            Ok(()) => step(&format!("{what}: ok")),
            Err(e) => failed.push(format!("{what}: {e}")),
        }
    }
    assert_none(&failed);
}

/// The pod sleeps while healthy, so that its staying awake later shows that
/// the failing probe kept it awake. The request that fails its health wakes
/// it.
fn failing_while_awake(suite: &Suite) -> Result<(), String> {
    suite.wait_for_idle_sleep(UNHEALTHY, suite.start)?;
    let before = suite.row(UNHEALTHY)?;
    let cursor = suite.cursor();
    suite
        .client
        .get(&suite.url(UNHEALTHY, "/health/fail"), REQUEST)?;
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let row = suite.row(UNHEALTHY)?;
        if row.restarts > before.restarts {
            break;
        }
        if Instant::now() >= deadline {
            return Err(format!("kubelet did not restart the failing app: {row:?}"));
        }
        thread::sleep(Duration::from_secs(1));
    }
    suite.check_no_pause(cursor, UNHEALTHY)
}

fn never_ready(suite: &Suite) -> Result<(), String> {
    let row = suite.row(UNREADY)?;
    if row.phase != "Running" || row.ready == "True" {
        return Err(format!(
            "pod {UNREADY} should run and not be Ready: {row:?}"
        ));
    }
    suite.check_no_pause(suite.start, UNREADY)?;
    if suite.state(UNREADY)? != PodState::Awake {
        return Err(format!("pod {UNREADY} is not awake"));
    }
    let events = suite.events_since(suite.start, UNREADY)?;
    match events.iter().find_map(wake::sleep_refusal) {
        Some(why) => {
            step(&format!("pod {UNREADY} does not sleep: {why}"));
            Ok(())
        }
        None => Err(format!(
            "no event says why pod {UNREADY} does not sleep (an event per change of the \
             reason)"
        )),
    }
}

fn failed_restore(suite: &Suite) -> Result<(), String> {
    let old = suite.sandbox(DOOMED).to_string();
    suite.wait_for_idle_sleep(DOOMED, suite.start)?;
    let unhealthy = suite.unhealthy()?.get(DOOMED).copied().unwrap_or(0);
    let mark = suite.api.log_mark()?;
    let outcome = failing_resumes(suite, &old, unhealthy, mark);
    let restarted = suite.api.restart_node_service();
    outcome?;
    restarted?;
    Ok(())
}

/// Arms `wake:resume-error` for the sleeping pod and sends it traffic, then
/// waits for three failed resumes, the failed pod, failing probes, and the
/// recreated pod with a new kernel.
fn failing_resumes(suite: &Suite, old: &str, unhealthy: u64, mark: u64) -> Result<(), String> {
    suite
        .api
        .arm_fault(FaultPoint::WakeResumeError, Some(old), NODE_SERVICE_RESTART)?;
    let cursor = suite.cursor();
    let request = suite.client.fetch_one(&suite.url(DOOMED, "/id"), REQUEST)?;
    step(&format!(
        "the request that started the failing wakes: {request}"
    ));
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        let events = suite.api.events_of(cursor, old)?;
        let resumes = events.iter().filter(|e| e.is_resume()).count();
        if resumes >= 3 && events.iter().any(|e| e.to == "failed") {
            break;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "the pod should fail after three failed resumes: {:?}",
                events.iter().map(|e| e.raw.to_string()).collect::<Vec<_>>()
            ));
        }
        thread::sleep(Duration::from_secs(2));
    }
    let log = suite.api.log_since(mark)?;
    if !log.contains(&FaultPoint::WakeResumeError.log_line(Some(old))) {
        return Err("the node service did not log fault point wake:resume-error".into());
    }
    let now = suite.unhealthy()?.get(DOOMED).copied().unwrap_or(0);
    if now <= unhealthy {
        return Err(format!(
            "kubelet saw no failing probe while the pod could not wake: {unhealthy} \
             Unhealthy events before, {now} after"
        ));
    }
    let deadline = Instant::now() + Duration::from_secs(300);
    let sandbox = loop {
        match suite.cluster.ready_sandbox(DOOMED) {
            Ok(sandbox) if sandbox != old => break sandbox,
            last if Instant::now() >= deadline => {
                return Err(format!("kubelet did not recreate the pod: {last:?}"));
            }
            _ => thread::sleep(Duration::from_secs(2)),
        }
    };
    let row = suite.row(DOOMED)?;
    if row.last_exit != Some(255) {
        return Err(format!(
            "the failed container's exit should be 255: {row:?}"
        ));
    }
    let url = format!("http://{}:{PORT}/id", row.ip);
    let id = eventually(Duration::from_secs(180), "the recreated app", || {
        suite.client.get(&url, REQUEST)
    });
    if id.value("boot_id")? == suite.identities[DOOMED].boot_id {
        return Err(format!("the recreated pod {sandbox} has its old kernel"));
    }
    step(&format!("pod {DOOMED} was recreated in sandbox {sandbox}"));
    Ok(())
}

/// The TCP API refuses a missing or wrong token with 401, looks pods up by
/// IP, streams events, and a wake with a lease keeps the pod awake past its
/// idle timeout until the lease ends.
fn tcp_api(suite: &Suite) {
    let token = format!("{:032x}", RandomState::new().hash_one("token"));
    let token_file = suite.cluster.node_path("api-token");
    suite
        .node()
        .sh(&format!("umask 077 && printf %s {token} > {token_file}"));
    let tcp = TcpApi::new(suite.node(), API_ADDR, &token);
    let follow = suite.cluster.node_path("follow.jsonl");
    with_cleanup(
        || {
            wake::set_args(
                &suite.api,
                &[API_LISTEN, API_ADDR, API_TOKEN_FILE, &token_file],
            )?;
            for (what, token) in [("no token", None), ("a wrong token", Some("wrong"))] {
                let (status, body) = tcp.request("GET", "/v1/pods", token, None)?;
                if status != 401 {
                    return Err(format!("{what} got {status}, not 401: {body}"));
                }
            }
            let sandbox = suite.sandbox(WEB);
            let ip = &suite.ips[WEB];
            let (status, pods) = tcp.call("GET", &format!("/v1/pods?ip={ip}"), None)?;
            let found = pods.get("pods").as_array().unwrap_or_default();
            if status != 200
                || found.len() != 1
                || found[0].get("sandbox_id").as_str() != Some(sandbox)
            {
                return Err(format!("the lookup by IP got {status}: {pods}"));
            }
            let (status, pod) = tcp.call("GET", &format!("/v1/pods/ip:{ip}"), None)?;
            if status != 200 || pod.get("sandbox_id").as_str() != Some(sandbox) {
                return Err(format!("GET /v1/pods/ip:{ip} got {status}: {pod}"));
            }
            suite.ensure_asleep(WEB)?;
            let cursor = suite.cursor();
            tcp.follow(cursor, 120, &follow)?;
            let body = format!(
                r#"{{"reason": "api", "source": "{}", "lease_ttl": {}}}"#,
                runtime::SOURCE,
                LEASE_TTL.as_secs()
            );
            let (status, woke) =
                tcp.call("POST", &format!("/v1/pods/{sandbox}/wake"), Some(&body))?;
            let woke_at = Instant::now();
            if status != 200 || woke.get("pod").get("state").as_str() != Some("awake") {
                return Err(format!("the wake with a lease got {status}: {woke}"));
            }
            let held = LEASE_TTL - Duration::from_secs(2);
            let left = wake::holds_for(held, Duration::from_secs(2), || {
                Ok(suite.state(WEB)? == PodState::Awake)
            })?;
            if let Some(after) = left {
                return Err(format!(
                    "the pod stopped being awake {after:?} into its {LEASE_TTL:?} lease"
                ));
            }
            thread::sleep(LEASE_TTL.saturating_sub(woke_at.elapsed()));
            suite.sleeps_within(WEB, cursor, SLEEP_START)?;
            let followed = tcp.followed(&follow)?;
            if !followed
                .iter()
                .any(|e| e.pod == sandbox && e.is_resume() && e.seq > cursor)
            {
                return Err(format!(
                    "the followed event stream has no wake of {WEB}: {:?}",
                    followed
                        .iter()
                        .map(|e| e.raw.to_string())
                        .collect::<Vec<_>>()
                ));
            }
            Ok(())
        },
        || wake::clear_args(&suite.api).map(drop),
    );
}

/// Holds a lease on every awake pod until none is awake without one, and
/// returns their sandboxes.
fn hold_awake_pods(suite: &Suite, lease: &str) -> Result<HashSet<String>, String> {
    let mut held = HashSet::new();
    loop {
        let awake: Vec<String> = suite
            .api
            .list()
            .map_err(|e| e.to_string())?
            .into_iter()
            .filter(|p| matches!(p.state, PodState::Awake | PodState::Waking))
            .map(|p| p.sandbox_id)
            .filter(|id| !held.contains(id))
            .collect();
        if awake.is_empty() {
            return Ok(held);
        }
        for sandbox in awake {
            suite
                .api
                .hold(&sandbox, lease, LEASE_MAX)
                .map_err(|e| format!("hold {sandbox}: {e}"))?;
            held.insert(sandbox);
        }
    }
}

/// The pods in `waking` or `awake`.
fn awake_pods(suite: &Suite) -> Result<usize, String> {
    let pods = suite.api.list().map_err(|e| e.to_string())?;
    Ok(pods
        .iter()
        .filter(|p| matches!(p.state, PodState::Awake | PodState::Waking))
        .count())
}

/// With the cap on awake pods reached, a wake of another pod waits; a pod put
/// to sleep within the wait lets it in; without one, its held SYN gets a RST
/// within 11 s and the event says why.
fn admission(suite: &Suite) {
    let lease = "admission";
    expect(suite.ensure_awake(WEB));
    expect(suite.ensure_awake(CALLEE));
    expect(suite.ensure_asleep(CALLER));
    let mut held = expect(hold_awake_pods(suite, lease));
    let cap = held.len().to_string();
    let caller = suite.sandbox(CALLER).to_string();
    held.insert(caller.clone());
    let body = || -> Result<(), String> {
        wake::set_args(&suite.api, &[MAX_AWAKE_PODS, &cap, ADMISSION_WAIT, "10s"])?;
        let awake = awake_pods(suite)?;
        if awake.to_string() != cap {
            return Err(format!(
                "{awake} pods are awake after the restart, not {cap}"
            ));
        }
        let cursor = suite.cursor();
        let waiting = suite.client.spawn(
            "admitted",
            &["fetch", "-timeout", "40s", &suite.url(CALLER, "/id")],
        )?;
        thread::sleep(Duration::from_secs(3));
        if suite.vm_count(CALLER) != 0 || waiting.status()?.is_some() {
            return Err(format!(
                "the wake past the cap of {cap} did not wait: {:?}",
                waiting.output()?
            ));
        }
        suite
            .api
            .release(suite.sandbox(WEB), lease)
            .map_err(|e| e.to_string())?;
        suite.ensure_asleep(WEB)?;
        let admitted = parse_fetch(&waiting.wait(Duration::from_secs(60))?)?.remove(0);
        if !admitted.ok() {
            return Err(format!(
                "the waiting wake was not admitted once a pod slept (admission): {admitted}"
            ));
        }
        suite.check_woken_by(cursor, CALLER, &suite.qualified(CLIENT))?;
        // The leases run out after a minute: renew them, and hold the woken
        // caller, for the refusal.
        for sandbox in held.iter().filter(|s| *s != suite.sandbox(WEB)) {
            suite
                .api
                .hold(sandbox, lease, LEASE_MAX)
                .map_err(|e| format!("hold {sandbox}: {e}"))?;
        }
        let cursor = suite.cursor();
        let refused = suite.client.fetch_one(&suite.url(WEB, "/id"), REQUEST)?;
        let reset = refused
            .error
            .as_deref()
            .is_some_and(|e| e.contains("connection refused") || e.contains("connection reset"));
        if !reset || refused.total_ms > REFUSED_WITHIN_MS {
            return Err(format!(
                "a refused wake should reset the held connection within {REFUSED_WITHIN_MS} ms: \
                 {refused}"
            ));
        }
        let events = suite.events_since(cursor, WEB)?;
        match events.iter().find_map(wake::wake_refusal) {
            Some(why) => step(&format!("the wake of {WEB} was refused: {why}")),
            None => {
                return Err(format!(
                    "no event says that the wake was refused: {:?}",
                    events.iter().map(|e| e.raw.to_string()).collect::<Vec<_>>()
                ))
            }
        }
        if suite.state(WEB)? != PodState::Asleep || suite.vm_count(WEB) != 0 {
            return Err(format!("pod {WEB} should still sleep"));
        }
        Ok(())
    };
    with_cleanup(body, || {
        let cleared = wake::clear_args(&suite.api).map(drop);
        for sandbox in &held {
            if let Err(e) = suite.api.release(sandbox, lease) {
                eprintln!("release {sandbox}: {e}");
            }
        }
        cleared
    });
}

/// 40 pods with 2 s HTTP probes sleep for 10 minutes; the node service uses
/// under 10% of one core and grows by under 200 KiB per pod; then 10 of them
/// are woken at once, with 10 resumes and every request answered.
fn small_scale(suite: &Suite) {
    let names: Vec<&'static str> = (1..=SCALE_PODS)
        .map(|i| &*Box::leak(format!("scale-{i:02}").into_boxed_str()))
        .collect();
    let result = scale(suite, &names);
    let delete = [
        "delete",
        "pods",
        "-n",
        &suite.cluster.namespace,
        "-l",
        "group=scale",
        "--ignore-not-found",
        "--timeout=600s",
    ];
    let deleted = suite
        .node()
        .try_kubectl(&delete, Duration::from_secs(700))
        .map(drop);
    expect(result);
    expect(deleted.map_err(|e| format!("delete the scale pods: {e}")));
}

fn scale(suite: &Suite, names: &[&'static str]) -> Result<(), String> {
    let before = wake::usage(&suite.api)?;
    let disk_before = suite.api.disk_used()?;
    let mut sandboxes = HashMap::new();
    for (batch, chunk) in names.chunks(SCALE_BATCH).enumerate() {
        step(&format!("creating scale pods, batch {}", batch + 1));
        let cursor = suite.cursor();
        let manifests: Vec<String> = chunk
            .iter()
            .map(|&name| {
                let pod = TestPod {
                    group: "scale",
                    ..TestPod::vm(name, SMALL_MIB, Probes::Http)
                };
                pod.manifest(&suite.cluster.namespace, &suite.cluster.image(Arch::Arm64))
            })
            .collect();
        suite
            .cluster
            .apply(&format!("scale-{batch}.yaml"), &manifests.join("---\n"));
        wait_ready(suite.cluster, chunk)?;
        for &name in chunk {
            let sandbox = suite.cluster.ready_sandbox(name)?;
            wake::watch_sleep(
                suite.node(),
                &sandbox,
                SLEEP_START + PAUSE_TIME + Duration::from_secs(60),
            )
            .map_err(|e| format!("scale pod {name} did not sleep by itself (idle sleep): {e}"))?;
            let events = suite.api.events_of(cursor, &sandbox)?;
            if !events.iter().any(wake::is_idle_sleep) {
                return Err(format!("scale pod {name} slept, but not for being idle"));
            }
            sandboxes.insert(name, sandbox);
        }
    }
    let start = wake::usage(&suite.api)?;
    let started = Instant::now();
    let cursor = suite.cursor();
    step(&format!(
        "{} scale pods asleep; watching them for {SCALE_WINDOW:?}",
        names.len()
    ));
    let mut awake = Vec::new();
    let held = wake::holds_for(SCALE_WINDOW, Duration::from_secs(30), || {
        let pods = suite.api.list().map_err(|e| e.to_string())?;
        awake = pods
            .iter()
            .filter(|p| sandboxes.values().any(|s| *s == p.sandbox_id))
            .filter(|p| p.state != PodState::Asleep)
            .map(|p| format!("{} {}", p.sandbox_id, p.state))
            .collect();
        Ok(awake.is_empty())
    })?;
    if let Some(after) = held {
        return Err(format!(
            "after {after:?} scale pods are not asleep: {awake:?}"
        ));
    }
    let end = wake::usage(&suite.api)?;
    let disk = suite.api.disk_used()?;
    step(&format!(
        "the smolvm volume holds {} MiB more with the scale pods asleep: {} MiB per pod",
        disk.saturating_sub(disk_before) >> 20,
        (disk.saturating_sub(disk_before) / names.len() as u64) >> 20
    ));
    let share = start.cpu_share(&end, started.elapsed());
    let growth = end.uss_kib.saturating_sub(before.uss_kib) / names.len() as u64;
    step(&format!(
        "node service: {:.1}% of one core over {:?}; USS {} KiB before the scale pods, {} KiB \
         once all slept, {} KiB after: {growth} KiB per pod",
        share * 100.0,
        started.elapsed(),
        before.uss_kib,
        start.uss_kib,
        end.uss_kib
    ));
    if end.pid != start.pid || start.pid != before.pid {
        return Err("the node service restarted during the check".into());
    }
    if share >= SCALE_CPU_SHARE || growth >= SCALE_KIB_PER_POD {
        return Err(format!(
            "the node service used {:.1}% of one core and grew {growth} KiB per pod; the \
             limits are {}% and {SCALE_KIB_PER_POD} KiB",
            share * 100.0,
            SCALE_CPU_SHARE * 100.0
        ));
    }
    for (name, sandbox) in &sandboxes {
        if suite
            .api
            .events_of(cursor, sandbox)?
            .iter()
            .any(|e| e.is_resume())
        {
            return Err(format!("scale pod {name} woke while it should sleep"));
        }
    }
    let woken = &names[..SCALE_WOKEN];
    let rows = suite.rows()?;
    let urls: Vec<String> = woken
        .iter()
        .map(|name| {
            format!(
                "http://{}:{PORT}/id",
                rows.get(*name).map(|r| r.ip.as_str()).unwrap_or_default()
            )
        })
        .collect();
    let cursor = suite.cursor();
    let answers = suite.client.fetch_all(
        &urls.iter().map(String::as_str).collect::<Vec<_>>(),
        REQUEST,
    )?;
    let unanswered: Vec<String> = answers
        .iter()
        .zip(woken)
        .filter(|(answer, _)| !answer.ok())
        .map(|(answer, name)| format!("{name}: {answer}"))
        .collect();
    if !unanswered.is_empty() {
        return Err(format!("requests to sleeping scale pods: {unanswered:?}"));
    }
    let mut resumes = 0;
    for name in woken {
        resumes += suite
            .api
            .events_of(cursor, &sandboxes[name])?
            .iter()
            .filter(|e| e.is_resume())
            .count();
    }
    if resumes != SCALE_WOKEN {
        return Err(format!("{resumes} resumes for {SCALE_WOKEN} requests"));
    }
    let firsts: Vec<f64> = answers.iter().filter_map(|a| a.first_byte_ms).collect();
    step(&format!(
        "{SCALE_WOKEN} woken at once: first answer {}",
        summary(&firsts, "ms")
    ));
    Ok(())
}

#[test]
fn writes_the_pods() {
    let pods = test_pods();
    let names: Vec<&str> = pods.iter().map(|p| p.name).collect();
    assert_eq!(&names[..VM_PODS.len()], VM_PODS);
    assert_eq!(names.last(), Some(&CLIENT));
    let web = pods[0].manifest("ns", "docker.io/test:1");
    let expected = r#"apiVersion: v1
kind: Pod
metadata:
  name: web
  namespace: ns
  labels: {app: web, group: main}
  annotations: {"smolvm-pods.floci.io/idle-timeout": "10s"}
spec:
  runtimeClassName: smolvm-pods
  automountServiceAccountToken: false
  terminationGracePeriodSeconds: 5
  containers:
  - name: app
    image: docker.io/test:1
    imagePullPolicy: Never
    args: [hold, -mib, "64", -https, ":8443", -grpc, ":9090", -udp, ":8081", web, ":8080"]
    ports:
    - {name: http, containerPort: 8080}
    - {name: https, containerPort: 8443}
    - {name: grpc, containerPort: 9090}
    - {name: udp, containerPort: 8081, protocol: UDP}
"#;
    assert_eq!(web, expected);
    let exec = pods.iter().find(|p| p.name == PROBES_EXEC).unwrap();
    assert_eq!(exec.arch, Arch::Amd64);
    let manifest = exec.manifest("ns", "docker.io/test:1");
    assert!(
        manifest.contains(
            r#"    readinessProbe: {exec: {command: ["/testapp", "get", "http://127.0.0.1:8080/health"]}, periodSeconds: 10, timeoutSeconds: 10, failureThreshold: 3}"#
        ),
        "{manifest}"
    );
    let cpu_off = pods.iter().find(|p| p.name == CPU_OFF).unwrap();
    assert!(cpu_off.manifest("ns", "docker.io/test:1").contains(
        r#"annotations: {"smolvm-pods.floci.io/idle-timeout": "10s", "smolvm-pods.floci.io/cpu-threshold": "off"}"#
    ));
    let unready = pods.iter().find(|p| p.name == UNREADY).unwrap();
    let manifest = unready.manifest("ns", "docker.io/test:1");
    assert!(
        manifest.contains(r#"-unready, unready, ":8080"]"#),
        "{manifest}"
    );
    assert!(!manifest.contains("livenessProbe"), "{manifest}");
    let unhealthy = pods.iter().find(|p| p.name == UNHEALTHY).unwrap();
    let manifest = unhealthy.manifest("ns", "docker.io/test:1");
    assert!(manifest.contains("failureThreshold: 15}"), "{manifest}");
    let client = pods.last().unwrap().manifest("ns", "docker.io/test:1");
    assert!(
        client.contains("args: [serve, client, \":8080\"]"),
        "{client}"
    );
    assert!(client.contains("annotations: {}") && !client.contains("runtimeClassName"));
}

#[test]
fn writes_each_kind_of_probe() {
    assert_eq!(Probes::None.yaml(), "");
    let grpc = Probes::Grpc.yaml();
    assert!(
        grpc.contains("readinessProbe: {grpc: {port: 9090}, periodSeconds: 2"),
        "{grpc}"
    );
    assert!(
        grpc.contains("livenessProbe: {grpc: {port: 9090}"),
        "{grpc}"
    );
    let https = Probes::Https.yaml();
    assert!(
        https.contains("httpGet: {path: /health, port: 8443, scheme: HTTPS}"),
        "{https}"
    );
    assert!(Probes::Tcp.yaml().contains("tcpSocket: {port: 8080}"));
}

#[test]
fn writes_the_services() {
    let yaml = services("ns", 30123);
    assert_eq!(yaml.matches("kind: Service").count(), 6);
    for name in [CALLEE, FOLLOWER, CLIENT] {
        assert!(
            yaml.contains(&format!("metadata: {{name: {name}, namespace: ns}}")),
            "{yaml}"
        );
    }
    assert!(
        yaml.contains("type: NodePort\n  selector: {app: probes-http}\n  ports: [{port: 8080, targetPort: 8080, nodePort: 30123}]"),
        "{yaml}"
    );
}

#[test]
fn parses_pod_rows() {
    let text = "web\tRunning\tTrue\t2026-10-19T12:00:00Z\t0\t10.42.0.9\t\n\
                doomed\tRunning\tFalse\t2026-10-19T12:05:00Z\t1\t10.42.0.12\t255\n\
                pending\tPending\t\t\t\t\t\n";
    let rows = parse_pod_rows(text);
    assert_eq!(
        rows["web"],
        PodRow {
            phase: "Running".into(),
            ready: "True".into(),
            ready_since: "2026-10-19T12:00:00Z".into(),
            restarts: 0,
            ip: "10.42.0.9".into(),
            last_exit: None,
        }
    );
    assert_eq!(
        (rows["doomed"].restarts, rows["doomed"].last_exit),
        (1, Some(255))
    );
    assert_eq!(rows["pending"].ip, "");
    assert_eq!(rows.len(), 3);
}

#[test]
fn counts_unhealthy_events() {
    let counts = parse_unhealthy("doomed\t3\ndoomed\t\nunready\t40\n\n");
    assert_eq!(counts["doomed"], 4);
    assert_eq!(counts["unready"], 40);
    assert_eq!(counts.len(), 2);
}

#[test]
fn the_sleep_limits_follow_the_idle_timeout() {
    assert_eq!(SLEEP_START, Duration::from_secs(27));
    assert_eq!(AFTER_BUSY, Duration::from_secs(87));
    assert!(
        LEASE_TTL > SLEEP_START,
        "the lease must outlast an idle sleep"
    );
    assert!(
        Duration::from_secs(BUSY_SECONDS) > SLEEP_START,
        "the busy period must outlast an idle sleep"
    );
    assert!(Duration::from_secs(STREAM_SECONDS) > SLEEP_START - Duration::from_secs(2));
}
