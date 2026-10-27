//! A node configured through a seed directory, as Floci configures the nodes
//! it creates. `node-vm.sh create --seed DIR` mounts the directory read-only
//! at /etc/floci-node/seed, and floci-node waits for the seed's version,
//! installs its files, maps its host names and applies changes to its env.
//! The node is isolated (`--isolate`), as Floci makes it when isolation is
//! on. tests/e2e/README.md lists the checks.

use std::cell::RefCell;
use std::env;
use std::fs;
use std::hash::{BuildHasher, RandomState};
use std::net::{Ipv4Addr, SocketAddr, TcpStream, ToSocketAddrs};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use smolvm_pods_e2e::json::Json;
use smolvm_pods_e2e::runtime::{Api, Role};
use smolvm_pods_e2e::wake::has_args;
use smolvm_pods_e2e::{
    assert_running_without_restarts, describe, eventually, expect, http_get_with, run_checks, step,
    App, Arch, Cluster, Node, NodeSetup, Pod, PodSpec, Runtime, StandIn, State, COMMAND_TIMEOUT,
};

/// The node's name, from the seed's k3s.yaml, which also gives it the label
/// `LABEL=<the seed's id>`.
const NODE_NAME: &str = "floci-seed-node";
const LABEL: &str = "floci.io/seed-test";

/// The memory that kubelet reports, from the seed's env. The CPUs it reports
/// stay the node's own setting, as the seed leaves them empty.
const MEMORY_GIB: u32 = 200;
const CPUS: &str = "128";

/// The idle timeout of the seed's env, and the one that the env change sets.
const IDLE_TIMEOUT: &str = "7m";
const CHANGED_IDLE_TIMEOUT: &str = "9m";

/// How long the test watches the node while the seed has no version.
const VERSION_WAIT: Duration = Duration::from_secs(40);
/// How often it looks, and how many looks must find floci-node waiting.
const VERSION_LOOK: Duration = Duration::from_secs(2);
const WAITING_LOOKS: usize = 5;
/// floci-node checks the seed at most 30 s apart; the rest is for the check.
const VERSION_APPLIED: Duration = Duration::from_secs(45);
/// floci-node reads the seed's env every 5 s and starts the node service 1 s
/// after it stops; the rest is for the node service's exit and the test.
const ENV_APPLIED: Duration = Duration::from_secs(30);
/// How long the test makes sure that one env change restarts the node
/// service once: two more of floci-node's reads of the env.
const ONE_RESTART: Duration = Duration::from_secs(12);

/// A host on the internet that answers plain HTTP. The node is isolated, so
/// it reaches the host only while its allow list names the host's address.
const EXTERNAL_HOST: &str = "example.com";
/// How long a change of the allow list may take to reach new connections.
const ALLOW_LIST_APPLIED: Duration = Duration::from_secs(15);

/// Where floci-node installs the seed's files, with their owners and modes.
const INSTALLED: [(&str, &str, &str); 4] = [
    (
        "k3s.yaml",
        "/etc/rancher/k3s/config.yaml.d/50-floci.yaml",
        "root:root 644",
    ),
    (
        "tokens.csv",
        "/etc/rancher/k3s/floci/tokens.csv",
        "root:root 600",
    ),
    (
        "registries.yaml",
        "/etc/rancher/k3s/registries.yaml",
        "root:root 644",
    ),
    ("api-token", "/etc/floci-node/api-token", "root:root 600"),
];

/// The image's own registries.yaml, which comes back when the seed's goes.
const IMAGE_REGISTRIES: &str = include_str!("../../../node/rootfs/etc/rancher/k3s/registries.yaml");

/// A registry that the seed's registries.yaml mirrors to the Mac, and the
/// file in which k3s gives containerd its endpoint.
const SEED_REGISTRY: &str = "registry.seed.test";
const SEED_REGISTRY_HOSTS: &str =
    "/var/lib/rancher/k3s/agent/etc/containerd/certs.d/registry.seed.test/hosts.toml";

/// Prints, one `key=value` per line, how many k3s, node service and
/// `floci-node run` processes the node has, and which of the files that only
/// an applied seed, a started k3s or a started node service makes exist.
const BEFORE_VERSION: &str = r#"k3s=0 service=0 waiting=0
for p in /proc/[0-9]*; do
  [ "${p#/proc/}" = "$$" ] && continue
  set -- $(tr '\0' ' ' < "$p/cmdline" 2>/dev/null)
  case "${1##*/} ${2:-} ${3:-}" in
    k3s*) k3s=$((k3s + 1)) ;;
    "smolvm-pods node-service"*) service=$((service + 1)) ;;
    *"floci-node run") waiting=$((waiting + 1)) ;;
  esac
done
echo "k3s=$k3s"
echo "service=$service"
echo "waiting=$waiting"
for f in /etc/rancher/k3s/config.yaml.d/50-floci.yaml /var/log/k3s.log /run/smolvm-pods/api.sock; do
  if [ -e "$f" ]; then echo "exists=$f"; fi
done"#;

/// Prints the pid of `k3s server`.
const K3S_SERVER: &str = r#"for p in /proc/[0-9]*; do
  [ "${p#/proc/}" = "$$" ] && continue
  set -- $(tr '\0' ' ' < "$p/cmdline" 2>/dev/null)
  case "${1##*/} ${2:-}" in
    "k3s server") echo "${p#/proc/}" ;;
  esac
done"#;

/// Prints the HTTP status of `GET /api/v1/nodes` on the node's Kubernetes
/// API, with the bearer token `$1`, or none when it is empty.
const API_STATUS: &str = r#"if [ -n "$1" ]; then set -- -H "Authorization: Bearer $1"; else set --; fi
curl -sk -o /dev/null -m 10 -w '%{http_code}' "$@" https://127.0.0.1:6443/api/v1/nodes"#;

#[test]
#[ignore = "needs a Mac with smolvm and Docker: SMOLVM_PODS_E2E=1 cargo test -- --ignored"]
fn seed() {
    let floci = StandIn::start();
    let seed = Seed::new(floci.port());
    let waited = RefCell::new(vec!["the node never started".to_string()]);
    let started = |node: &Node| *waited.borrow_mut() = wait_for_version(node, &seed);
    let create = ["--seed", seed.path(), "--isolate"];
    let cluster = Cluster::start_with(
        "seed",
        NodeSetup {
            service_port: true,
            create: &create,
            started: Some(&started),
            ..NodeSetup::default()
        },
    );
    let testapp = cluster.node_path("testapp");
    cluster
        .node
        .copy_in(&cluster.workload(Arch::Arm64, "testapp"), &testapp);
    cluster.node.sh(&format!("chmod +x {testapp}"));
    let api = Api::connect(&cluster.node, &testapp, Duration::from_secs(60));
    let suite = Suite {
        cluster: &cluster,
        api,
        seed: &seed,
        floci: &floci,
    };
    let waited = waited.take();
    run_checks(&[
        ("k3s waits for the version", &|| {
            assert!(waited.is_empty(), "{}", waited.join("\n"))
        }),
        ("installed files", &|| installed_files(&suite)),
        ("host names at the gateway", &|| host_names(&suite)),
        ("isolation", &|| isolation(&suite)),
        ("node name and label", &|| node_name_and_label(&suite)),
        ("Kubernetes token", &|| kubernetes_token(&suite, 200)),
        ("registry mirror", &|| registry_mirror(&suite)),
        ("node service token", &|| node_service_token(&suite)),
        ("settings from the seed", &|| settings(&suite, IDLE_TIMEOUT)),
        ("env change", &|| env_change(&suite)),
        ("dropped files", &|| dropped_files(&suite)),
    ]);
}

/// The seed directory on the Mac, written as Floci writes it: mode 0700, each
/// file 0600, written to a temporary name and renamed, and `version` last.
/// Dropping it removes it.
struct Seed {
    dir: PathBuf,
    path: String,
    /// The value of the node's label.
    id: String,
    /// The static token of the Kubernetes API, in `system:masters`.
    admin_token: String,
    /// The node service's bearer token.
    api_token: String,
}

impl Seed {
    /// Writes every file but `version`. `floci_port` is the port of the
    /// stand-in for Floci, to which the seed's registry mirror goes.
    fn new(floci_port: u16) -> Seed {
        let id = random_hex();
        let dir = env::temp_dir().join(format!("floci-seed-e2e-{}-{id}", std::process::id()));
        fs::create_dir(&dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display()));
        let seed = Seed {
            path: dir.display().to_string(),
            dir,
            admin_token: format!("{}{}", random_hex(), random_hex()),
            api_token: format!("{}{}", random_hex(), random_hex()),
            id,
        };
        fs::set_permissions(&seed.dir, fs::Permissions::from_mode(0o700))
            .unwrap_or_else(|e| panic!("{}: {e}", seed.path));
        seed.write("env", &env_file(IDLE_TIMEOUT));
        seed.write("k3s.yaml", &k3s_yaml(&seed.id, true));
        seed.write(
            "tokens.csv",
            &format!(
                "{},floci-admin,floci-admin,system:masters\n",
                seed.admin_token
            ),
        );
        seed.write("api-token", &seed.api_token);
        seed.write("hosts", "host.docker.internal\n");
        seed.write("registries.yaml", &registries_yaml(floci_port));
        seed
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn read(&self, name: &str) -> String {
        let path = self.dir.join(name);
        fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
    }

    /// Writes `name` as Floci does: to a temporary file, then renamed.
    fn write(&self, name: &str, contents: &str) {
        let path = self.dir.join(name);
        let temporary = self.dir.join(format!(".{name}.new"));
        let written = fs::write(&temporary, contents)
            .and_then(|()| fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600)))
            .and_then(|()| fs::rename(&temporary, &path));
        written.unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    }

    fn remove(&self, name: &str) {
        let path = self.dir.join(name);
        fs::remove_file(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    }
}

impl Drop for Seed {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

/// 16 random hex digits.
fn random_hex() -> String {
    format!("{:016x}", RandomState::new().hash_one(0))
}

/// The seed's env as Floci writes it: every setting it owns, empty for the
/// node's own, with `idle_timeout` and the reported memory set.
fn env_file(idle_timeout: &str) -> String {
    format!(
        "FLOCI_NODE_IDLE_TIMEOUT={idle_timeout}\n\
         FLOCI_NODE_CPU_THRESHOLD=\n\
         FLOCI_NODE_CPU_WINDOW=\n\
         FLOCI_NODE_MAX_AWAKE_PODS=\n\
         FLOCI_NODE_MAX_AWAKE_MEMORY=\n\
         FLOCI_NODE_ADMISSION_WAIT=\n\
         FLOCI_NODE_MEMORY_GIB={MEMORY_GIB}\n\
         FLOCI_NODE_CPUS=\n\
         RUST_LOG=\n"
    )
}

/// The seed's k3s settings: the node's name and label, and with `tokens` the
/// static token file.
fn k3s_yaml(id: &str, tokens: bool) -> String {
    let mut yaml = format!("node-name: {NODE_NAME}\nkubelet-arg+:\n  - node-labels={LABEL}={id}\n");
    if tokens {
        yaml += "kube-apiserver-arg+:\n  - token-auth-file=/etc/rancher/k3s/floci/tokens.csv\n";
    }
    yaml
}

/// The seed's registries: every registry to the endpoint that refuses
/// connections, as the image's, and [`SEED_REGISTRY`] to the Mac.
fn registries_yaml(floci_port: u16) -> String {
    format!(
        "mirrors:\n  \"*\":\n    endpoint:\n      - \"http://127.0.0.1:1\"\n  \
         {SEED_REGISTRY}:\n    endpoint:\n      - \"http://host.docker.internal:{floci_port}\"\n"
    )
}

/// Watches the node while the seed has no version, then writes the version
/// and waits until floci-node has installed the seed. Returns what went
/// wrong: k3s, the node service or a seed file before the version, too few
/// looks that found floci-node waiting, or a seed applied too late.
fn wait_for_version(node: &Node, seed: &Seed) -> Vec<String> {
    let mut problems = Vec::new();
    let mut waiting = 0;
    let start = Instant::now();
    step("watching the node while the seed has no version");
    while start.elapsed() < VERSION_WAIT {
        // The node's agent may not answer during the first seconds of the boot.
        if let Ok(output) = node.try_sh(BEFORE_VERSION) {
            let look = Look::parse(&output);
            if look.waiting > 0 {
                waiting += 1;
            }
            if let Some(problem) = look.problem() {
                problems.push(format!(
                    "{:.0?} after the start: {problem}",
                    start.elapsed()
                ));
            }
        }
        thread::sleep(VERSION_LOOK);
    }
    if waiting < WAITING_LOOKS {
        problems.push(format!(
            "only {waiting} looks found floci-node run waiting, fewer than {WAITING_LOOKS}"
        ));
    }
    step("writing the seed's version");
    seed.write("version", "1\n");
    let written = Instant::now();
    let applied = format!("test -e {}", INSTALLED[0].1);
    loop {
        if node.try_sh(&applied).is_ok() {
            step(&format!(
                "floci-node applied the seed {:.1?} after its version",
                written.elapsed()
            ));
            break;
        }
        if written.elapsed() > VERSION_APPLIED {
            problems.push(format!(
                "floci-node did not apply the seed within {VERSION_APPLIED:?} of its version"
            ));
            break;
        }
        thread::sleep(Duration::from_secs(1));
    }
    problems
}

/// What one look at the node found while the seed had no version.
#[derive(Debug, PartialEq)]
struct Look {
    k3s: usize,
    service: usize,
    waiting: usize,
    existing: Vec<String>,
}

impl Look {
    /// Parses the output of [`BEFORE_VERSION`].
    fn parse(text: &str) -> Look {
        let count = |key: &str| {
            text.lines()
                .find_map(|line| line.strip_prefix(key)?.strip_prefix('='))
                .and_then(|n| n.trim().parse().ok())
                .unwrap_or(0)
        };
        Look {
            k3s: count("k3s"),
            service: count("service"),
            waiting: count("waiting"),
            existing: text
                .lines()
                .filter_map(|line| line.strip_prefix("exists="))
                .map(str::to_string)
                .collect(),
        }
    }

    /// What should not be there before the version.
    fn problem(&self) -> Option<String> {
        let mut found = Vec::new();
        if self.k3s > 0 {
            found.push(format!("{} k3s processes", self.k3s));
        }
        if self.service > 0 {
            found.push(format!("{} node service processes", self.service));
        }
        found.extend(self.existing.iter().cloned());
        (!found.is_empty()).then(|| found.join(", "))
    }
}

struct Suite<'a> {
    cluster: &'a Cluster,
    api: Api<'a>,
    seed: &'a Seed,
    floci: &'a StandIn,
}

impl Suite<'_> {
    fn node(&self) -> &Node {
        &self.cluster.node
    }
}

/// Each installed file has the seed's content, root as its owner and its
/// mode; the seed's mount is read-only.
fn installed_files(suite: &Suite) {
    let node = suite.node();
    let mut wrong = Vec::new();
    for (name, path, owner_and_mode) in INSTALLED {
        let found = node.sh(&format!("stat -c '%U:%G %a' {path}"));
        if found.trim() != owner_and_mode {
            wrong.push(format!("{path} is {}, not {owner_and_mode}", found.trim()));
        }
        if node.sh(&format!("cat {path}")) != suite.seed.read(name) {
            wrong.push(format!("{path} is not the seed's {name}"));
        }
    }
    if node.try_sh("touch /etc/floci-node/seed/probe").is_ok() {
        wrong.push("the node could write in the seed's mount".to_string());
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// /etc/hosts maps the seed's host name to the default route's gateway
/// between floci-node's marker comments, and the node reaches the stand-in
/// for Floci on the Mac's loopback by that name.
fn host_names(suite: &Suite) {
    let node = suite.node();
    let gateway = node.gateway();
    let block = format!(
        "# floci-node seed: begin\n{gateway}\thost.docker.internal\n# floci-node seed: end\n"
    );
    let hosts = node.sh("cat /etc/hosts");
    assert!(
        hosts.contains(&block),
        "/etc/hosts has no {block:?}:\n{hosts}"
    );
    let resolved = node.sh("getent hosts host.docker.internal | awk '{ print $1 }'");
    assert_eq!(resolved.trim(), gateway, "host.docker.internal in the node");
    let answer = node.sh(&format!(
        "curl -sS -m 10 http://host.docker.internal:{}/",
        suite.floci.port()
    ));
    assert_eq!(
        &State::parse(&answer)["token"],
        suite.floci.token(),
        "the stand-in for Floci at host.docker.internal"
    );
}

/// The node is isolated: `node-vm.sh status` labels it so, it resolves no
/// name and reaches no address on the internet, and the stand-in for Floci
/// answers at the gateway and at the seed's host name. `node-vm.sh update`
/// opens an address of [`EXTERNAL_HOST`] to the running node, and closes it.
fn isolation(suite: &Suite) {
    let node = suite.node();
    let status = expect(Json::parse(
        &node.script(&["status", "--json"], COMMAND_TIMEOUT),
    ));
    let isolated = status
        .as_array()
        .into_iter()
        .flatten()
        .find(|entry| entry.get("name").as_str() == Some(node.name()))
        .map(|entry| entry.get("labels").get("isolated").as_str() == Some("true"));
    assert_eq!(isolated, Some(true), "node-vm.sh status: {status}");
    let lookup = node.exec(&["getent", "hosts", EXTERNAL_HOST], COMMAND_TIMEOUT);
    assert!(
        !lookup.success(),
        "{EXTERNAL_HOST} resolves in the node: {}",
        describe(&lookup)
    );
    let gateway = node.gateway();
    for host in [gateway.as_str(), "host.docker.internal"] {
        let answer = node.sh(&format!(
            "curl -sS -m 10 http://{host}:{}/",
            suite.floci.port()
        ));
        assert_eq!(
            &State::parse(&answer)["token"],
            suite.floci.token(),
            "the stand-in for Floci at {host}"
        );
    }
    let address = external_address();
    let reach = || {
        node.try_sh(&format!(
            "curl -sS -o /dev/null -m 5 -H 'Host: {EXTERNAL_HOST}' http://{address}/"
        ))
    };
    assert!(
        reach().is_err(),
        "the node reached {address} before its allow list named it"
    );
    let cidr = format!("{address}/32");
    step(&format!("allowing {cidr} on the running node"));
    node.script(&["update", "--allow-cidr", &cidr], COMMAND_TIMEOUT);
    eventually(
        ALLOW_LIST_APPLIED,
        &format!("the node to reach {address}"),
        reach,
    );
    step(&format!("removing {cidr} from the allow list"));
    node.script(&["update", "--remove-allow-cidr", &cidr], COMMAND_TIMEOUT);
    eventually(
        ALLOW_LIST_APPLIED,
        &format!("the node to no longer reach {address}"),
        || match reach() {
            Ok(_) => Err(format!("it reaches {address}")),
            Err(_) => Ok(()),
        },
    );
}

/// An IPv4 address of [`EXTERNAL_HOST`] that the Mac reaches on port 80.
fn external_address() -> Ipv4Addr {
    let addrs: Vec<SocketAddr> = (EXTERNAL_HOST, 80)
        .to_socket_addrs()
        .unwrap_or_else(|e| panic!("the Mac cannot resolve {EXTERNAL_HOST}: {e}"))
        .collect();
    addrs
        .iter()
        .find_map(|addr| match addr {
            SocketAddr::V4(v4)
                if TcpStream::connect_timeout(addr, Duration::from_secs(10)).is_ok() =>
            {
                Some(*v4.ip())
            }
            _ => None,
        })
        .unwrap_or_else(|| {
            panic!("the Mac reaches no IPv4 address of {EXTERNAL_HOST} on port 80: {addrs:?}")
        })
}

/// The node registered with the seed's name and label.
fn node_name_and_label(suite: &Suite) {
    let node = suite.node();
    let expected = format!("node/{NODE_NAME}");
    let nodes = node.kubectl_ok(&["get", "nodes", "-o", "name"]);
    assert_eq!(nodes.trim(), expected, "the nodes");
    let selector = format!("{LABEL}={}", suite.seed.id);
    let labelled = node.kubectl_ok(&["get", "nodes", "-l", &selector, "-o", "name"]);
    assert_eq!(labelled.trim(), expected, "the nodes labelled {selector}");
}

/// The Kubernetes API answers `GET /api/v1/nodes` with `status` for the
/// seed's static token, and with 401 without a token.
fn kubernetes_token(suite: &Suite, status: u16) {
    let node = suite.node();
    let status_with = |token: &str| {
        let output = node.exec(&["sh", "-c", API_STATUS, "sh", token], COMMAND_TIMEOUT);
        output.stdout.trim().to_string()
    };
    assert_eq!(
        status_with(&suite.seed.admin_token),
        status.to_string(),
        "the status with the seed's static token"
    );
    assert_eq!(status_with(""), "401", "the status without a token");
}

/// k3s gives containerd the seed's mirror of [`SEED_REGISTRY`].
fn registry_mirror(suite: &Suite) {
    let hosts = suite.node().sh(&format!("cat {SEED_REGISTRY_HOSTS}"));
    let endpoint = format!("http://host.docker.internal:{}", suite.floci.port());
    assert!(
        hosts.contains(&endpoint),
        "{SEED_REGISTRY_HOSTS} does not name {endpoint}:\n{hosts}"
    );
}

/// From the Mac, the node service's API takes the seed's token and refuses
/// none or another; `node-vm.sh token` prints the seed's token.
fn node_service_token(suite: &Suite) {
    let port = suite
        .cluster
        .service_port()
        .expect("the node publishes the node service's API");
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let get = |token: Option<&str>| {
        let auth = token.map(|token| format!("Bearer {token}"));
        let headers: Vec<(&str, &str)> = auth
            .iter()
            .map(|auth| ("Authorization", auth.as_str()))
            .collect();
        expect(http_get_with(addr, "/v1/pods", &headers, COMMAND_TIMEOUT))
    };
    let (status, body) = get(Some(suite.seed.api_token.as_str()));
    assert_eq!(status, 200, "GET /v1/pods with the seed's token: {body}");
    let pods = expect(Json::parse(&body));
    assert!(pods.get("pods").as_array().is_some(), "{body}");
    let (status, _) = get(None);
    assert_eq!(status, 401, "GET /v1/pods without a token");
    let (status, _) = get(Some(random_hex().as_str()));
    assert_eq!(status, 401, "GET /v1/pods with another token");
    let token = suite.node().script(&["token"], COMMAND_TIMEOUT);
    assert_eq!(token.trim(), suite.seed.api_token, "node-vm.sh token");
}

/// The node service runs with the seed's idle timeout and the node's own CPU
/// rule, which the seed leaves empty, and kubelet reports the seed's memory
/// and the node's own CPUs.
fn settings(suite: &Suite, idle_timeout: &str) {
    let service = expect(suite.api.only(Role::NodeService));
    for flag in [
        ["--idle-timeout", idle_timeout],
        ["--cpu-threshold", "0.5"],
        ["--cpu-window", "60s"],
    ] {
        assert!(
            has_args(&service.args, &flag),
            "the node service runs without {}: {}",
            flag.join(" "),
            service.args.join(" ")
        );
    }
    let capacity = suite.node().kubectl_ok(&[
        "get",
        "node",
        NODE_NAME,
        "-o",
        "jsonpath={.status.capacity.memory} {.status.capacity.cpu}",
    ]);
    assert_eq!(
        capacity.trim(),
        format!("{MEMORY_GIB}Gi {CPUS}"),
        "the memory and CPUs that kubelet reports"
    );
}

/// What a change of the seed's env must leave as it was.
#[derive(Debug, PartialEq)]
struct Running {
    boot_id: String,
    k3s: String,
    vm_pid: Vec<u32>,
    restarts: String,
}

impl Running {
    fn now(suite: &Suite, pod: &Pod) -> Running {
        let node = suite.node();
        Running {
            boot_id: node
                .sh("cat /proc/sys/kernel/random/boot_id")
                .trim()
                .to_string(),
            k3s: node.sh(K3S_SERVER).trim().to_string(),
            vm_pid: pod.vm_processes().iter().map(|vm| vm.pid).collect(),
            restarts: pod.field("{.status.containerStatuses[*].restartCount}"),
        }
    }
}

/// With a VM pod running, a new env in the seed restarts the node service
/// once, with the new idle timeout, within [`ENV_APPLIED`]; k3s, the pod's VM
/// and its restart count stay.
fn env_change(suite: &Suite) {
    let pods = suite.cluster.run_pods(&[PodSpec {
        name: "app",
        arch: Arch::Arm64,
        runtime: Runtime::SmolvmPods,
        app: App::Hold { mib: 64 },
        containers: &[("app", 8080)],
        annotations: &[],
    }]);
    let pod = &pods[0];
    let api = &suite.api;
    let before = Running::now(suite, pod);
    assert_eq!(before.vm_pid.len(), 1, "the pod's VM processes: {before:?}");
    assert!(
        before.k3s.parse::<u32>().is_ok(),
        "one k3s server process: {before:?}"
    );
    let old = expect(api.only(Role::NodeService)).pid;
    let mark = expect(api.log_mark());
    step(&format!(
        "changing the seed's idle timeout to {CHANGED_IDLE_TIMEOUT}; node service {old}, {before:?}"
    ));
    suite.seed.write("env", &env_file(CHANGED_IDLE_TIMEOUT));
    let changed = Instant::now();
    let new = eventually(ENV_APPLIED, "the node service with the new env", || {
        let service = api.only(Role::NodeService)?;
        if service.pid != old && has_args(&service.args, &["--idle-timeout", CHANGED_IDLE_TIMEOUT])
        {
            Ok(service.pid)
        } else {
            Err(format!(
                "it runs as {}: {}",
                service.pid,
                service.args.join(" ")
            ))
        }
    });
    step(&format!(
        "node service {new} runs with the new env {:.1?} after the change",
        changed.elapsed()
    ));
    expect(api.wait_for_node_service(old, ENV_APPLIED));
    let log = expect(api.log_since(mark));
    assert!(
        log.contains("floci-node: the seed's env changed: restarting the node service"),
        "the node service's log has no line about the change:\n{log}"
    );
    thread::sleep(ONE_RESTART);
    assert_eq!(
        expect(api.only(Role::NodeService)).pid,
        new,
        "the node service restarted again after the change"
    );
    let after = Running::now(suite, pod);
    assert_eq!(after, before, "what the env change must leave as it was");
    assert_running_without_restarts(pod);
    settings(suite, CHANGED_IDLE_TIMEOUT);
}

/// After the seed loses tokens.csv, registries.yaml and hosts and the node
/// restarts, floci-node has removed the token file, put back the image's
/// registries.yaml and removed the host names; the static token gets 401,
/// and the node keeps its name and the node service's token.
fn dropped_files(suite: &Suite) {
    let seed = suite.seed;
    for name in ["tokens.csv", "registries.yaml", "hosts"] {
        seed.remove(name);
    }
    seed.write("k3s.yaml", &k3s_yaml(&seed.id, false));
    suite.cluster.restart_node();
    let node = suite.node();
    let tokens = INSTALLED[1].1;
    assert!(
        node.try_sh(&format!("test ! -e {tokens}")).is_ok(),
        "{tokens} is still there"
    );
    let registries = node.sh(&format!("cat {}", INSTALLED[2].1));
    assert_eq!(
        registries, IMAGE_REGISTRIES,
        "registries.yaml is not the image's"
    );
    let hosts = node.sh("cat /etc/hosts");
    assert!(
        !hosts.contains("floci-node seed"),
        "/etc/hosts still has the seed's names:\n{hosts}"
    );
    assert!(
        node.try_sh("getent hosts host.docker.internal").is_err(),
        "host.docker.internal still resolves"
    );
    kubernetes_token(suite, 401);
    node_name_and_label(suite);
    node_service_token(suite);
}

#[test]
fn reads_looks_at_the_node() {
    let waiting = Look::parse("k3s=0\nservice=0\nwaiting=1\n");
    assert_eq!(
        waiting,
        Look {
            k3s: 0,
            service: 0,
            waiting: 1,
            existing: vec![],
        }
    );
    assert_eq!(waiting.problem(), None);
    let started = Look::parse(
        "k3s=2\nservice=1\nwaiting=1\nexists=/var/log/k3s.log\nexists=/run/smolvm-pods/api.sock\n",
    );
    assert_eq!(
        started.problem().as_deref(),
        Some(
            "2 k3s processes, 1 node service processes, /var/log/k3s.log, \
             /run/smolvm-pods/api.sock"
        )
    );
}

#[test]
fn writes_the_seed_files() {
    assert_eq!(
        k3s_yaml("abc", true),
        "node-name: floci-seed-node\nkubelet-arg+:\n  - node-labels=floci.io/seed-test=abc\n\
         kube-apiserver-arg+:\n  - token-auth-file=/etc/rancher/k3s/floci/tokens.csv\n"
    );
    assert_eq!(
        k3s_yaml("abc", false),
        "node-name: floci-seed-node\nkubelet-arg+:\n  - node-labels=floci.io/seed-test=abc\n"
    );
    let env = env_file("7m");
    assert_eq!(env.lines().count(), 9, "{env}");
    assert!(env.starts_with("FLOCI_NODE_IDLE_TIMEOUT=7m\n"), "{env}");
    assert!(env.contains("\nFLOCI_NODE_MEMORY_GIB=200\n"), "{env}");
    assert!(!env.contains("FLOCI_NODE_ISOLATION"), "{env}");
    assert_eq!(
        registries_yaml(8080),
        "mirrors:\n  \"*\":\n    endpoint:\n      - \"http://127.0.0.1:1\"\n  \
         registry.seed.test:\n    endpoint:\n      - \"http://host.docker.internal:8080\"\n"
    );
}
