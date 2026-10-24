//! The scale test: one node runs 500 runc pods and 5 VM pods whose requests
//! add up to about 10 times its memory and CPUs, because kubelet reports the
//! capacity that /etc/floci-node/env configures (node/README.md,
//! "Capacity"). The VM pods hibernate and wake while the runc pods run, keep
//! their share of the CPU under contention, and every pod survives a restart
//! of k3s.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::thread;
use std::time::{Duration, Instant};

use smolvm_pods_e2e::runtime::{Api, Hibernation, PodState, Wake};
use smolvm_pods_e2e::{
    describe, eventually, expect, run_checks, step, summary, Arch, Cluster, Pod, State,
    COMMAND_TIMEOUT, NO_IDLE_SLEEP,
};

/// The node that the test creates. It has 8 vCPUs (`Cluster`).
const NODE_MEMORY_MIB: u32 = 16384;

/// The runc pods, a Deployment of pause containers, and their requests.
const RUNC_PODS: u32 = 500;
const RUNC_CPU_MILLI: u64 = 160;
const RUNC_MEMORY_MIB: u64 = 320;

/// The VM pods and their requests. Each runs the hold app.
const VM_PODS: [&str; 5] = ["vm-1", "vm-2", "vm-3", "vm-4", "vm-5"];
const VM_CPU_MILLI: u64 = 500;
const VM_MEMORY_MIB: u64 = 1024;
const HOLD_MIB: u32 = 64;
const PORT: u16 = 8080;

/// The pods that the k3s restart check adds after the restart.
const NEW_VM: &str = "vm-6";
const NEW_RUNC_PODS: u32 = 5;

/// The pods together must request at least this many times the node's
/// memory and online CPUs.
const OVERCOMMIT: f64 = 10.0;

/// What floci-node sets (node/README.md).
const MAX_PODS: &str = "600";
const POD_CIDR_MASK: &str = "/22";
const SYSCTLS: [(&str, &str); 5] = [
    ("fs.inotify.max_user_instances", "8192"),
    ("fs.inotify.max_user_watches", "1048576"),
    ("net.ipv4.neigh.default.gc_thresh1", "1024"),
    ("net.ipv4.neigh.default.gc_thresh2", "4096"),
    ("net.ipv4.neigh.default.gc_thresh3", "8192"),
];
const RUNTIME_CGROUP: &str = "/smolvm-pods";

/// The hibernate and wake cycles of every VM pod under load.
const CYCLES: u32 = 3;

/// Threads that burn CPU in each VM pod (its vCPUs) and in the runc pod
/// under contention, for how long, and the window measured within it.
const VM_BURN_THREADS: u32 = 2;
const RUNC_BURN_THREADS: u32 = 8;
const BURN_SECONDS: u32 = 40;
const SAMPLE_AFTER_SECONDS: u32 = 10;
const SAMPLE_SECONDS: u32 = 20;
/// The least share of the CPU that the VM pods and the runc pods use together
/// that the VM pods must get. Equal weights give half. In the root cgroup the
/// VMs' 10 vCPU threads would weigh 1000 against kubepods' 5000, a sixth.
const VM_SHARE_MIN: f64 = 0.35;
const BURNER: &str = "burn";

const RUNNING_TIMEOUT: Duration = Duration::from_secs(900);
const RESTART_TIMEOUT: Duration = Duration::from_secs(300);

/// Events that would show the capacity view failing: a pod refused for its
/// requests or the pod count, kubelet failing to apply kubepods' limits, or
/// kubelet evicting.
const BAD_EVENTS: [&str; 8] = [
    "FailedScheduling",
    "OutOfmemory",
    "OutOfcpu",
    "OutOfpods",
    "UnexpectedAdmissionError",
    "FailedNodeAllocatableEnforcement",
    "Evicted",
    "EvictionThresholdMet",
];

/// Log lines that would show the node running out of a kernel limit.
const BAD_LOG_LINES: &str = "too many open files|neighbour table overflow|no space left on device";

/// The node's memory, what uses it, and the `memory.current` of kubepods and
/// of the runtime's cgroup, which includes page cache, in MiB, each as
/// `key=value`.
const MEMORY: &str = r#"m() { awk -v k="$1:" '$1 == k { printf "%d", $2 / 1024 }' /proc/meminfo; }
sum() { t=0; for p in $(pgrep -f "$1"); do
  v=$(awk -v k="$2:" '$1 == k { print $2 }' "/proc/$p/smaps_rollup" 2>/dev/null); t=$((t + ${v:-0}))
done; echo $((t / 1024)); }
cg() { echo $(($(cat "/sys/fs/cgroup/$1/memory.current") / 1048576)); }
echo "total=$(m MemTotal) available=$(m MemAvailable) anon=$(m AnonPages) slab=$(m Slab)"
echo "page_tables=$(m PageTables) kernel_stack=$(m KernelStack) shmem=$(m Shmem)"
echo "k3s_rss=$(sum '^k3s server' Rss) containerd_rss=$(sum '^containerd ' Rss)"
echo "runc_shims=$(pgrep -fc containerd-shim-runc) runc_shims_pss=$(sum containerd-shim-runc Pss)"
echo "node_service_rss=$(sum 'smolvm-pods node-service' Rss) vm_rss=$(sum ' _boot-vm ' Rss)"
echo "kubepods=$(cg kubepods) smolvm_pods=$(cg smolvm-pods)""#;

/// One line per process that the cgroup check looks at: its role, its
/// cgroup and its pid.
const PROCESS_CGROUPS: &str = r#"for p in /proc/[0-9]*; do
  set -- $(tr '\0' ' ' < "$p/cmdline" 2>/dev/null)
  role=
  case "$1 $2" in
    "smolvm-pods node-service") role=node-service ;;
    *" _boot-vm") role=vm ;;
    "k3s server") role=k3s ;;
    "containerd ") role=containerd ;;
    "/pause ") role=pause ;;
    *containerd-shim-smolvm-pods-v2*) case " $* " in *" -id "*) role=front-shim ;; esac ;;
  esac
  if [ -n "$role" ]; then echo "$role $(sed -n 's/^0:://p' "$p/cgroup") ${p#/proc/}"; fi
done"#;

/// Burns CPU in the VM pods (`$3...`) and the runc pod `$2` of namespace `$1`
/// at once, and prints the CPU time of the runtime's cgroup, of kubepods and
/// of the whole node over a window within it, then what each burner ran.
const CONTENTION: &str = r#"ns=$1 burner=$2 vm_threads=$3 runc_threads=$4 seconds=$5 after=$6 window=$7
shift 7
out=$(mktemp -d)
usage() { awk '$1 == "usage_usec" { print $2 }' "/sys/fs/cgroup/$1/cpu.stat"; }
for pod in "$@"; do
  kubectl -n "$ns" exec "$pod" -c app -- /testapp burn "$vm_threads" "$seconds" >"$out/$pod" 2>&1 &
done
kubectl -n "$ns" exec "$burner" -c app -- /testapp burn "$runc_threads" "$seconds" >"$out/$burner" 2>&1 &
sleep "$after"
vm=$(usage smolvm-pods) runc=$(usage kubepods) all=$(usage .) start=$(date +%s%N)
sleep "$window"
echo "vm_usec=$(($(usage smolvm-pods) - vm)) runc_usec=$(($(usage kubepods) - runc))"
echo "all_usec=$(($(usage .) - all)) wall_usec=$((($(date +%s%N) - start) / 1000))"
wait
for f in "$out"/*; do echo "loops_${f##*/}=$(sed -n 's/^loops=//p' "$f")"; done
rm -rf "$out""#;

/// kubelet's and k3s's warnings and errors in k3s.log, grouped: the time, the
/// pid and CPU numbers are left out, and each line is cut at 160 characters.
const WARNINGS: &str = r#"{ grep -E '^[WE][0-9]{4} ' /var/log/k3s.log |
    sed -E 's/^([WE])[0-9]{4} [0-9:.]+ +[0-9]+ /\1 /'
  grep -E 'level=(warning|error)' /var/log/k3s.log | sed -E 's/^time="[^"]*" //'
} | sed -E 's/cpu[0-9]+/cpuN/g' | cut -c1-160 | sort | uniq -c | sort -rn"#;

/// What the node reports and what it has.
struct Facts {
    /// The capacity that /etc/floci-node/env configures.
    memory_gib: u64,
    cpus: u64,
    /// The node's own memory and online CPUs.
    real_memory_kib: u64,
    online_cpus: u64,
    online: String,
}

/// The test's state, which the checks share.
struct Scale<'a> {
    cluster: &'a Cluster,
    api: Api<'a>,
    testapp: String,
    facts: Facts,
    vms: Vec<Pod<'a>>,
    /// Each VM pod's memory hash at the start, which no wake may change.
    hashes: HashMap<&'static str, String>,
    /// Durations of the hibernates and wakes, in ms, as ctl reports them.
    hibernate_ms: RefCell<Vec<f64>>,
    wake_ms: RefCell<Vec<f64>>,
    /// Memory figures at each point, in MiB.
    memory: RefCell<Vec<(String, State)>>,
}

#[test]
#[ignore = "needs a Mac with smolvm and Docker: SMOLVM_PODS_E2E=1 cargo test -- --ignored"]
fn scale() {
    let cluster = Cluster::start_with_memory("scale", Some(NODE_MEMORY_MIB));
    let testapp = cluster.node_path("testapp");
    cluster
        .node
        .copy_in(&cluster.workload(Arch::Arm64, "testapp"), &testapp);
    cluster.node.sh(&format!("chmod +x {testapp}"));
    let api = Api::connect(&cluster.node, &testapp, Duration::from_secs(60));
    let facts = read_facts(&cluster);
    let memory = RefCell::new(vec![("before the pods".to_string(), read_memory(&cluster))]);

    step(&format!(
        "creating {RUNC_PODS} runc pods ({RUNC_CPU_MILLI}m, {RUNC_MEMORY_MIB}Mi) and {} VM pods \
         ({VM_CPU_MILLI}m, {VM_MEMORY_MIB}Mi)",
        VM_PODS.len()
    ));
    let start = Instant::now();
    let ns = cluster.namespace.as_str();
    let image = cluster.image(Arch::Arm64);
    cluster.apply("pause.yaml", &pause_deployment(ns, RUNC_PODS));
    let vm_manifests: Vec<String> = VM_PODS
        .iter()
        .map(|name| vm_pod(ns, &image, name))
        .collect();
    cluster.apply("vms.yaml", &vm_manifests.join("---\n"));
    wait_running(
        &cluster,
        RUNC_PODS as usize + VM_PODS.len(),
        RUNNING_TIMEOUT,
    );
    step(&format!(
        "all {} pods Running {:.0} s after the apply",
        RUNC_PODS as usize + VM_PODS.len(),
        start.elapsed().as_secs_f64()
    ));
    let vms: Vec<Pod> = VM_PODS.iter().map(|name| cluster.ready_pod(name)).collect();
    let mut scale = Scale {
        cluster: &cluster,
        api,
        testapp,
        facts,
        vms,
        hashes: HashMap::new(),
        hibernate_ms: RefCell::default(),
        wake_ms: RefCell::default(),
        memory,
    };
    for name in VM_PODS {
        let hash = eventually(Duration::from_secs(120), "the app's first answer", || {
            scale.hash(name)
        });
        scale.hashes.insert(name, hash);
    }
    scale.record_memory("all pods running");
    run_checks(&[
        ("node settings", &|| node_settings(&scale)),
        ("cgroups", &|| cgroups(&scale)),
        ("requests and pods", &|| requests_and_pods(&scale)),
        ("memory", &|| memory_use(&scale)),
        ("hibernate and wake under load", &|| cycles(&scale)),
        ("CPU share under contention", &|| contention(&scale)),
        ("k3s restart", &|| k3s_restart(&scale)),
        ("warnings", &|| warnings(&scale)),
    ]);
}

impl<'a> Scale<'a> {
    fn sh(&self, script: &str) -> Result<String, String> {
        self.cluster.node.try_sh(script)
    }

    /// Runs `script` in the node with `args` as `$1...`.
    fn sh_with(&self, script: &str, args: &[&str], timeout: Duration) -> Result<String, String> {
        let output = self
            .cluster
            .node
            .exec(&[&["sh", "-c", script, "sh"], args].concat(), timeout);
        if output.success() {
            Ok(output.stdout)
        } else {
            Err(format!("a script in the node: {}", describe(&output)))
        }
    }

    fn vm(&self, name: &str) -> &Pod<'a> {
        let found = self.vms.iter().find(|pod| pod.name == name);
        found.unwrap_or_else(|| panic!("no VM pod {name}"))
    }

    /// The memory hash that the VM pod's app reports, asked from the node.
    fn hash(&self, name: &str) -> Result<String, String> {
        let url = format!("http://{}:{PORT}/hash", self.vm(name).ip());
        let output = self.cluster.node.exec(
            &[self.testapp.as_str(), "get", "-timeout", "60s", &url],
            COMMAND_TIMEOUT,
        );
        if !output.success() {
            return Err(format!("GET {url} from the node: {}", describe(&output)));
        }
        Ok(State::parse(&output.stdout)["sha256"].to_string())
    }

    fn check_hash(&self, name: &'static str) -> Result<(), String> {
        let hash = self.hash(name)?;
        if hash == self.hashes[name] {
            Ok(())
        } else {
            Err(format!(
                "pod {name}: memory hash {hash}, was {}",
                self.hashes[name]
            ))
        }
    }

    fn hibernate(&self, name: &str) -> Result<Hibernation, String> {
        let pod = self.vm(name);
        let (done, _) = self
            .api
            .hibernate(&pod.sandbox)
            .map_err(|e| format!("hibernate pod {name}: {e}"))?;
        if done.pod.state != PodState::Asleep || !pod.vm_processes().is_empty() {
            return Err(format!(
                "pod {name} after hibernate: {}, VM processes {:?}",
                done.pod.state,
                pod.vm_processes()
            ));
        }
        self.hibernate_ms.borrow_mut().push(done.duration_ms);
        Ok(done)
    }

    fn wake(&self, name: &str) -> Result<Wake, String> {
        let pod = self.vm(name);
        let (wake, _) = self
            .api
            .wake(&pod.sandbox)
            .map_err(|e| format!("wake pod {name}: {e}"))?;
        if wake.pod.state != PodState::Awake || pod.vm_processes().len() != 1 {
            return Err(format!(
                "pod {name} after wake: {}, VM processes {:?}",
                wake.pod.state,
                pod.vm_processes()
            ));
        }
        self.wake_ms.borrow_mut().push(wake.duration_ms);
        Ok(wake)
    }

    #[track_caller]
    fn record_memory(&self, when: &str) {
        let memory = read_memory(self.cluster);
        self.memory.borrow_mut().push((when.to_string(), memory));
    }
}

/// Reads the configured capacity and the node's own memory and CPUs.
#[track_caller]
fn read_facts(cluster: &Cluster) -> Facts {
    let script = r#". /etc/floci-node/env
echo "memory_gib=$FLOCI_NODE_MEMORY_GIB"
echo "cpus=$FLOCI_NODE_CPUS"
echo "real_memory_kib=$(awk '$1 == "MemTotal:" { print $2 }' /proc/meminfo)"
echo "online=$(cat /sys/devices/system/cpu/online)"
echo "online_cpus=$(getconf _NPROCESSORS_ONLN)""#;
    let state = State::parse(&cluster.node.sh(script));
    let number = |key: &str| -> u64 {
        state[key]
            .parse()
            .unwrap_or_else(|e| panic!("{key} of the node: {e}"))
    };
    let facts = Facts {
        memory_gib: number("memory_gib"),
        cpus: number("cpus"),
        real_memory_kib: number("real_memory_kib"),
        online_cpus: number("online_cpus"),
        online: state["online"].to_string(),
    };
    step(&format!(
        "node: configured {} GiB and {} CPUs; has {} MiB and {} online CPUs ({})",
        facts.memory_gib,
        facts.cpus,
        facts.real_memory_kib >> 10,
        facts.online_cpus,
        facts.online
    ));
    facts
}

#[track_caller]
fn read_memory(cluster: &Cluster) -> State {
    State::parse(&cluster.node.sh(MEMORY).replace(' ', "\n"))
}

fn pause_deployment(ns: &str, replicas: u32) -> String {
    format!(
        r#"apiVersion: apps/v1
kind: Deployment
metadata: {{name: pause, namespace: {ns}}}
spec:
  replicas: {replicas}
  selector: {{matchLabels: {{app: pause}}}}
  template:
    metadata: {{labels: {{app: pause}}}}
    spec:
      automountServiceAccountToken: false
      enableServiceLinks: false
      terminationGracePeriodSeconds: 0
      containers:
      - name: pause
        image: docker.io/rancher/mirrored-pause:3.6
        imagePullPolicy: Never
        resources: {{requests: {{cpu: {RUNC_CPU_MILLI}m, memory: {RUNC_MEMORY_MIB}Mi}}}}
"#
    )
}

/// A VM pod, which stays awake until the test hibernates it.
fn vm_pod(ns: &str, image: &str, name: &str) -> String {
    let (idle, timeout) = NO_IDLE_SLEEP;
    format!(
        r#"apiVersion: v1
kind: Pod
metadata: {{name: {name}, namespace: {ns}, labels: {{app: vm}}, annotations: {{{idle:?}: {timeout:?}}}}}
spec:
  runtimeClassName: smolvm-pods
  automountServiceAccountToken: false
  enableServiceLinks: false
  terminationGracePeriodSeconds: 5
  containers:
  - name: app
    image: {image}
    imagePullPolicy: Never
    args: [hold, -mib, "{HOLD_MIB}", app, ":{PORT}"]
    resources: {{requests: {{cpu: {VM_CPU_MILLI}m, memory: {VM_MEMORY_MIB}Mi}}}}
"#
    )
}

/// The runc pod of the contention check, which waits until an exec burns CPU
/// in it.
fn burner_pod(ns: &str, image: &str) -> String {
    format!(
        r#"apiVersion: v1
kind: Pod
metadata: {{name: {BURNER}, namespace: {ns}}}
spec:
  automountServiceAccountToken: false
  enableServiceLinks: false
  terminationGracePeriodSeconds: 0
  containers:
  - name: app
    image: {image}
    imagePullPolicy: Never
    args: [sleep, "3600"]
    resources: {{requests: {{cpu: 100m, memory: 64Mi}}}}
"#
    )
}

/// The number of the namespace's pods in each phase.
fn phases(cluster: &Cluster) -> Result<BTreeMap<String, usize>, String> {
    let args = [
        "get",
        "pods",
        "-n",
        &cluster.namespace,
        "-o",
        "jsonpath={range .items[*]}{.status.phase}{\"\\n\"}{end}",
    ];
    let out = cluster.node.try_kubectl(&args, COMMAND_TIMEOUT)?;
    let mut phases = BTreeMap::new();
    for phase in out.lines().filter(|line| !line.is_empty()) {
        *phases.entry(phase.to_string()).or_insert(0) += 1;
    }
    Ok(phases)
}

/// Waits until `count` pods of the namespace are Running, printing the phases
/// as they change.
#[track_caller]
fn wait_running(cluster: &Cluster, count: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let mut last = BTreeMap::new();
    loop {
        let now = phases(cluster).unwrap_or_default();
        if now != last {
            step(&format!("pods: {now:?}"));
            last = now;
        }
        if last.get("Running").copied().unwrap_or(0) >= count {
            return;
        }
        if Instant::now() >= deadline {
            let pending = cluster.node.try_sh(&format!(
                "kubectl get pods -n {} --no-headers | grep -v Running | head -n 5; \
                 kubectl get events -n {} --field-selector type=Warning | tail -n 10",
                cluster.namespace, cluster.namespace
            ));
            panic!(
                "{count} pods were not Running after {timeout:?}: {last:?}\n{}",
                pending.unwrap_or_else(|e| e)
            );
        }
        thread::sleep(Duration::from_secs(5));
    }
}

/// The cpu.weight that kubelet gives kubepods for `cpus` CPUs, and that
/// floci-node gives the runtime's cgroup.
fn kubepods_cpu_weight(cpus: u64) -> u64 {
    (1 + (cpus * 1024 - 2) * 9999 / 262_142).min(10_000)
}

/// kubelet reports the configured capacity and pod count, the node has a /22
/// of pod IPs and the kernel limits, kubelet runs without memory eviction or
/// a CPU or memory manager, and k3s's namespace has the view.
fn node_settings(scale: &Scale) {
    let facts = &scale.facts;
    let node = scale.cluster.node.kubectl_ok(&[
        "get",
        "nodes",
        "-o",
        "jsonpath={.items[0].metadata.name} {.items[0].status.capacity.cpu} \
         {.items[0].status.capacity.memory} {.items[0].status.capacity.pods} \
         {.items[0].spec.podCIDR}",
    ]);
    step(&format!("node name, capacity and pod CIDR: {node}"));
    let fields: Vec<&str> = node.split_whitespace().collect();
    let [name, cpu, memory, pods, cidr] = fields[..] else {
        panic!("node fields: {node:?}");
    };
    let mut wrong = Vec::new();
    if cpu != facts.cpus.to_string() {
        wrong.push(format!("capacity cpu {cpu}, expected {}", facts.cpus));
    }
    if memory != format!("{}Gi", facts.memory_gib) {
        wrong.push(format!(
            "capacity memory {memory}, expected {}Gi",
            facts.memory_gib
        ));
    }
    if pods != MAX_PODS {
        wrong.push(format!("capacity pods {pods}, expected {MAX_PODS}"));
    }
    if !cidr.ends_with(POD_CIDR_MASK) {
        wrong.push(format!("pod CIDR {cidr}, expected a {POD_CIDR_MASK}"));
    }
    let names: Vec<&str> = SYSCTLS.iter().map(|(name, _)| *name).collect();
    let values = scale
        .cluster
        .node
        .sh(&format!("sysctl -n {}", names.join(" ")));
    for ((name, want), got) in SYSCTLS.iter().zip(values.lines()) {
        if got.trim() != *want {
            wrong.push(format!("{name} is {}, expected {want}", got.trim()));
        }
    }
    // kubelet's configz answers through the API a few seconds after k3s does.
    let script = format!(
        r#"config=$(kubectl get --raw /api/v1/nodes/{name}/proxy/configz) || exit 1
printf '%s' "$config" | jq -er '.kubeletconfig |
  "eviction_hard=\(.evictionHard // {{}} | keys | join(","))",
  "eviction_soft=\(.evictionSoft // {{}} | keys | join(","))",
  "cpu_manager=\(.cpuManagerPolicy)", "memory_manager=\(.memoryManagerPolicy)",
  "max_pods=\(.maxPods)"'"#
    );
    let config = eventually(RESTART_TIMEOUT, "kubelet's configuration", || {
        scale.sh(&script)
    });
    step(&format!(
        "kubelet's configuration: {}",
        config.trim().replace('\n', ", ")
    ));
    let config = State::parse(&config);
    for key in ["eviction_hard", "eviction_soft"] {
        if config[key].to_lowercase().contains("memory") {
            wrong.push(format!(
                "kubelet's {key} has a memory threshold: {}",
                &config[key]
            ));
        }
    }
    for (key, want) in [
        ("cpu_manager", "none"),
        ("memory_manager", "None"),
        ("max_pods", MAX_PODS),
    ] {
        if &config[key] != want {
            wrong.push(format!(
                "kubelet's {key} is {}, expected {want}",
                &config[key]
            ));
        }
    }
    let view = scale.cluster.node.sh(r#"k3s=$(pgrep -f '^k3s server')
nsenter -t "$k3s" -m awk '$1 == "MemTotal:" { print "memory_kib=" $2 }' /proc/meminfo
echo "cpu_dirs=$(nsenter -t "$k3s" -m sh -c 'ls -d /sys/devices/system/cpu/cpu[0-9]*' | wc -l)"
echo "online=$(nsenter -t "$k3s" -m cat /sys/devices/system/cpu/online)""#);
    let view = State::parse(&view);
    let expected_kib = (facts.memory_gib << 20).max(facts.real_memory_kib);
    if view["memory_kib"] != expected_kib.to_string() {
        wrong.push(format!(
            "k3s sees MemTotal {} kB, expected {expected_kib}",
            &view["memory_kib"]
        ));
    }
    if view["cpu_dirs"] != facts.cpus.to_string() || view["online"] != facts.online {
        wrong.push(format!(
            "k3s sees {} CPUs, {} online; expected {}, {} online",
            &view["cpu_dirs"], &view["online"], facts.cpus, facts.online
        ));
    }
    assert!(wrong.is_empty(), "node settings:\n{}", wrong.join("\n"));
}

/// The node service and every VM process are in the runtime's cgroup, with
/// kubepods' cpu.weight; k3s, containerd and the front shim are not; the runc
/// pods are in kubepods, whose cpuset is the online CPUs.
fn cgroups(scale: &Scale) {
    let lines = scale.cluster.node.sh(PROCESS_CGROUPS);
    let mut by_role: BTreeMap<&str, Vec<(&str, &str)>> = BTreeMap::new();
    for line in lines.lines() {
        let mut fields = line.split_whitespace();
        if let (Some(role), Some(cgroup), Some(pid)) = (fields.next(), fields.next(), fields.next())
        {
            by_role.entry(role).or_default().push((cgroup, pid));
        }
    }
    let mut wrong = Vec::new();
    for (role, processes) in &by_role {
        let mut cgroups: BTreeMap<&str, usize> = BTreeMap::new();
        for (cgroup, _) in processes {
            let key = if cgroup.starts_with("/kubepods") {
                "/kubepods/..."
            } else {
                cgroup
            };
            *cgroups.entry(key).or_insert(0) += 1;
        }
        step(&format!("{role}: {cgroups:?}"));
        let in_runtime = processes
            .iter()
            .all(|(cgroup, _)| *cgroup == RUNTIME_CGROUP);
        let in_kubepods = processes
            .iter()
            .all(|(cgroup, _)| cgroup.starts_with("/kubepods/"));
        let outside_runtime = processes
            .iter()
            .all(|(cgroup, _)| *cgroup != RUNTIME_CGROUP);
        let ok = match *role {
            "node-service" | "vm" => in_runtime,
            "pause" => in_kubepods,
            _ => outside_runtime,
        };
        if !ok {
            wrong.push(format!("{role} processes in {processes:?}"));
        }
    }
    let vms = by_role.get("vm").map_or(0, Vec::len);
    if vms != VM_PODS.len() {
        wrong.push(format!("{vms} VM processes, expected {}", VM_PODS.len()));
    }
    for role in ["node-service", "k3s", "containerd", "front-shim", "pause"] {
        if !by_role.contains_key(role) {
            wrong.push(format!("no {role} process found"));
        }
    }
    let weights = State::parse(&scale.cluster.node.sh(
        r#"echo "kubepods=$(cat /sys/fs/cgroup/kubepods/cpu.weight)"
echo "runtime=$(cat /sys/fs/cgroup/smolvm-pods/cpu.weight)"
echo "cpuset=$(cat /sys/fs/cgroup/kubepods/cpuset.cpus.effective)""#,
    ));
    let expected = kubepods_cpu_weight(scale.facts.cpus).to_string();
    step(&format!(
        "cpu.weight: kubepods {}, smolvm-pods {}, expected {expected}; kubepods cpuset {}",
        &weights["kubepods"], &weights["runtime"], &weights["cpuset"]
    ));
    if weights["kubepods"] != expected || weights["runtime"] != expected {
        wrong.push(format!(
            "cpu.weight of kubepods {} and smolvm-pods {}, expected {expected} for both",
            &weights["kubepods"], &weights["runtime"]
        ));
    }
    if weights["cpuset"] != scale.facts.online {
        wrong.push(format!(
            "kubepods' cpuset is {}, expected the online CPUs {}",
            &weights["cpuset"], scale.facts.online
        ));
    }
    assert!(wrong.is_empty(), "cgroups:\n{}", wrong.join("\n"));
}

/// Every pod runs without a restart, and they request at least 10 times the
/// node's memory and online CPUs.
fn requests_and_pods(scale: &Scale) {
    let ns = &scale.cluster.namespace;
    let restarts = scale.cluster.node.kubectl_ok(&[
        "get",
        "pods",
        "-n",
        ns,
        "-o",
        "jsonpath={range .items[*]}{.status.phase} {.status.containerStatuses[0].restartCount}{\"\\n\"}{end}",
    ]);
    let running = restarts
        .lines()
        .filter(|l| l.starts_with("Running "))
        .count();
    let restarted = restarts.lines().filter(|l| !l.ends_with(" 0")).count();
    let pods = RUNC_PODS as usize + VM_PODS.len();
    let cpu_milli = u64::from(RUNC_PODS) * RUNC_CPU_MILLI + VM_PODS.len() as u64 * VM_CPU_MILLI;
    let memory_mib = u64::from(RUNC_PODS) * RUNC_MEMORY_MIB + VM_PODS.len() as u64 * VM_MEMORY_MIB;
    let cpu_ratio = cpu_milli as f64 / (scale.facts.online_cpus * 1000) as f64;
    let memory_ratio = memory_mib as f64 / (scale.facts.real_memory_kib >> 10) as f64;
    let allocated = scale
        .cluster
        .node
        .sh("kubectl describe node | sed -n '/^Allocated resources:/,/^Events:/p' | sed -n '5,6p'");
    step(&format!(
        "{running} of {pods} pods Running, {restarted} with restarts; the test's pods request \
         {cpu_milli}m CPU ({cpu_ratio:.1} times the online CPUs) and {memory_mib} MiB \
         ({memory_ratio:.1} times the node's memory); the node's allocated: {}",
        allocated.split_whitespace().collect::<Vec<_>>().join(" ")
    ));
    assert_eq!(running, pods, "Running pods");
    assert_eq!(restarted, 0, "pods with restarts");
    assert!(
        cpu_ratio >= OVERCOMMIT && memory_ratio >= OVERCOMMIT,
        "the pods request {cpu_ratio:.1} times the CPUs and {memory_ratio:.1} times the memory, \
         expected {OVERCOMMIT} at least"
    );
}

/// The node has memory left with every pod running. It prints what the pods
/// cost.
fn memory_use(scale: &Scale) {
    let memory = scale.memory.borrow();
    let mib = |state: &State, key: &str| -> i64 { state[key].parse().unwrap_or(-1) };
    let before = &memory[0].1;
    let running = &memory[1].1;
    let used = |state: &State| mib(state, "total") - mib(state, "available");
    let per_pod_kib = (used(running) - used(before)) * 1024 / i64::from(RUNC_PODS);
    step(&format!(
        "node memory used {} MiB before the pods, {} MiB with them: about {per_pod_kib} KiB a \
         pod; {} MiB available; runc shims {} with {} MiB PSS",
        used(before),
        used(running),
        mib(running, "available"),
        mib(running, "runc_shims"),
        mib(running, "runc_shims_pss"),
    ));
    assert!(
        mib(running, "available") > 1024,
        "less than 1 GiB available with every pod running"
    );
}

/// While the runc pods run, each VM pod hibernates and wakes through the API,
/// three times, and its memory hash stays the same.
fn cycles(scale: &Scale) {
    for cycle in 1..=CYCLES {
        for name in VM_PODS {
            let done = expect(scale.hibernate(name));
            step(&format!(
                "cycle {cycle}: {name} asleep in {:.0} ms, checkpoint {} MiB",
                done.duration_ms,
                done.checkpoint_bytes.unwrap_or_default() >> 20
            ));
        }
        if cycle == 1 {
            scale.record_memory("VM pods asleep");
        }
        for name in VM_PODS {
            let wake = expect(scale.wake(name));
            expect(scale.check_hash(name));
            step(&format!(
                "cycle {cycle}: {name} awake in {:.0} ms (resumed {})",
                wake.duration_ms, wake.resumed
            ));
        }
    }
    step(&format!(
        "under load: hibernate {}, wake {}",
        summary(&scale.hibernate_ms.borrow(), "ms"),
        summary(&scale.wake_ms.borrow(), "ms")
    ));
}

/// The VM pods and a runc pod burn every CPU they can, and the VM pods get at
/// least VM_SHARE_MIN of what both use.
fn contention(scale: &Scale) {
    let cluster = scale.cluster;
    let ns = cluster.namespace.as_str();
    cluster.apply("burner.yaml", &burner_pod(ns, &cluster.image(Arch::Arm64)));
    let ready = [
        "wait",
        "-n",
        ns,
        "--for=condition=Ready",
        "pod/burn",
        "--timeout=120s",
    ];
    expect(cluster.node.try_kubectl(&ready, Duration::from_secs(180)));
    let mut args: Vec<String> = [
        ns.to_string(),
        BURNER.to_string(),
        VM_BURN_THREADS.to_string(),
        RUNC_BURN_THREADS.to_string(),
        BURN_SECONDS.to_string(),
        SAMPLE_AFTER_SECONDS.to_string(),
        SAMPLE_SECONDS.to_string(),
    ]
    .into();
    args.extend(VM_PODS.iter().map(|name| name.to_string()));
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let timeout = Duration::from_secs(u64::from(BURN_SECONDS) + 120);
    let result = scale.sh_with(CONTENTION, &args, timeout);
    cluster.delete_pod(BURNER);
    let usage = State::parse(&expect(result).replace(' ', "\n"));
    let usec = |key: &str| -> f64 { usage[key].parse().unwrap_or(0.0) };
    let wall = usec("wall_usec");
    let (vm, runc, all) = (usec("vm_usec"), usec("runc_usec"), usec("all_usec"));
    let share = vm / (vm + runc);
    step(&format!(
        "under contention: VM pods {:.2} CPUs, runc pods {:.2} CPUs, the rest {:.2} CPUs; the \
         VM pods' share {share:.2}; loops: {}",
        vm / wall,
        runc / wall,
        (all - vm - runc) / wall,
        VM_PODS
            .iter()
            .chain([&BURNER])
            .map(|name| format!("{name} {}", &usage[format!("loops_{name}").as_str()]))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    assert!(
        share >= VM_SHARE_MIN,
        "the VM pods got {share:.2} of the CPU that they and the runc pods used, \
         expected {VM_SHARE_MIN} at least"
    );
}

/// What identifies the namespace's pods: name, uid, first container and
/// restart count, one line each, sorted.
fn pod_identities(scale: &Scale) -> Result<Vec<String>, String> {
    let args = [
        "get",
        "pods",
        "-n",
        &scale.cluster.namespace,
        "-o",
        "jsonpath={range .items[*]}{.metadata.name} {.metadata.uid} \
         {.status.containerStatuses[0].containerID} {.status.containerStatuses[0].restartCount} \
         {.status.phase}{\"\\n\"}{end}",
    ];
    let out = scale.cluster.node.try_kubectl(&args, COMMAND_TIMEOUT)?;
    let mut lines: Vec<String> = out
        .lines()
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect();
    lines.sort();
    Ok(lines)
}

fn sandboxes(scale: &Scale) -> Result<Vec<String>, String> {
    let out = scale.sh(&format!(
        "crictl pods -q --state ready --namespace {} | sort",
        scale.cluster.namespace
    ))?;
    Ok(out.lines().map(str::to_string).collect())
}

/// After k3s restarts, kubelet reports the same capacity, every pod runs on
/// in the same sandbox without a restart, the VM pods keep their memory and
/// hibernate and wake, and new runc and VM pods start.
fn k3s_restart(scale: &Scale) {
    let before = expect(pod_identities(scale));
    let sandboxes_before = expect(sandboxes(scale));
    let old = scale.cluster.node.sh("pgrep -f '^k3s server'");
    let killed = scale
        .cluster
        .node
        .sh("pids=$(pgrep -f '^k3s init'); kill -TERM $pids; echo $pids");
    step(&format!("sent SIGTERM to k3s ({})", killed.trim()));
    let restart = Instant::now();
    eventually(RESTART_TIMEOUT, "k3s to restart", || {
        let new = scale.sh("pgrep -f '^k3s server'")?;
        if new == old {
            return Err("k3s server has its old pid".to_string());
        }
        scale
            .cluster
            .node
            .try_kubectl(&["get", "--raw", "/readyz"], COMMAND_TIMEOUT)
    });
    let ready = r#"jsonpath={.items[*].status.conditions[?(@.type=="Ready")].status}"#;
    eventually(RESTART_TIMEOUT, "the node to be Ready", || {
        let status = scale
            .cluster
            .node
            .try_kubectl(&["get", "nodes", "-o", ready], COMMAND_TIMEOUT)?;
        match status.trim() {
            "True" => Ok(()),
            other => Err(format!("node Ready is {other:?}")),
        }
    });
    step(&format!(
        "k3s answers and the node is Ready {:.0} s after the SIGTERM",
        restart.elapsed().as_secs_f64()
    ));
    eventually(RESTART_TIMEOUT, "the pods to be as before", || {
        let after = pod_identities(scale)?;
        if after == before {
            Ok(())
        } else {
            let changed: Vec<&String> = after.iter().filter(|l| !before.contains(l)).collect();
            Err(format!(
                "{} pods changed, for example {:?}",
                changed.len(),
                changed.iter().take(3).collect::<Vec<_>>()
            ))
        }
    });
    step(&format!(
        "all {} pods Running in the same sandboxes and containers {:.0} s after the SIGTERM",
        before.len(),
        restart.elapsed().as_secs_f64()
    ));
    eventually(
        RESTART_TIMEOUT,
        "containerd to list the same sandboxes",
        || {
            let after = sandboxes(scale)?;
            if after == sandboxes_before {
                Ok(())
            } else {
                Err(format!(
                    "{} ready sandboxes, {} before",
                    after.len(),
                    sandboxes_before.len()
                ))
            }
        },
    );
    step(&format!(
        "containerd lists the same {} ready sandboxes {:.0} s after the SIGTERM",
        sandboxes_before.len(),
        restart.elapsed().as_secs_f64()
    ));
    node_settings(scale);
    scale.record_memory("after the k3s restart");
    for name in VM_PODS {
        expect(scale.check_hash(name));
        expect(scale.hibernate(name));
        expect(scale.wake(name));
        expect(scale.check_hash(name));
    }
    step("the VM pods kept their memory, and hibernate and wake");
    let cluster = scale.cluster;
    let ns = cluster.namespace.as_str();
    let replicas = RUNC_PODS + NEW_RUNC_PODS;
    cluster.apply("pause.yaml", &pause_deployment(ns, replicas));
    cluster.apply(
        "vm-6.yaml",
        &vm_pod(ns, &cluster.image(Arch::Arm64), NEW_VM),
    );
    let pods = replicas as usize + VM_PODS.len() + 1;
    wait_running(cluster, pods, RESTART_TIMEOUT);
    let new_vm = cluster.ready_pod(NEW_VM);
    assert_eq!(new_vm.vm_processes().len(), 1, "VM processes of {NEW_VM}");
    step(&format!(
        "{NEW_RUNC_PODS} new runc pods and {NEW_VM} run after the restart"
    ));
}

/// Prints kubelet's and k3s's warnings and errors, the warning events and the
/// node service's; fails on an event or log line that shows the capacity view
/// failing.
fn warnings(scale: &Scale) {
    let log = expect(scale.sh(WARNINGS));
    step(&format!(
        "k3s.log warnings and errors, grouped:\n{}",
        log.lines().take(40).collect::<Vec<_>>().join("\n")
    ));
    let events = scale.cluster.node.kubectl_ok(&[
        "get",
        "events",
        "-A",
        "--field-selector",
        "type=Warning",
        "-o",
        "jsonpath={range .items[*]}{.reason} {.count} {.involvedObject.kind}/{.involvedObject.name}: \
         {.message}{\"\\n\"}{end}",
    ]);
    step(&format!("warning events:\n{}", events.trim_end()));
    let service = expect(scale.sh(
        "grep -cE ' (WARN|ERROR) ' /var/log/smolvm-pods/node-service.log; \
         grep -E ' (WARN|ERROR) ' /var/log/smolvm-pods/node-service.log | \
         sed -E 's/^[^ ]+ //' | cut -c1-160 | sort | uniq -c | sort -rn | head -n 10",
    ));
    step(&format!("node service warnings and errors: {service}"));
    let bad_logs = expect(scale.sh(&format!(
        "grep -ciE '{BAD_LOG_LINES}' /var/log/k3s.log /var/log/smolvm-pods/node-service.log || true"
    )));
    let mut wrong: Vec<String> = events
        .lines()
        .filter(|line| {
            BAD_EVENTS
                .iter()
                .any(|bad| line.starts_with(&format!("{bad} ")))
        })
        .map(str::to_string)
        .collect();
    for line in bad_logs.lines() {
        if !line.ends_with(":0") {
            wrong.push(format!("log lines matching {BAD_LOG_LINES}: {line}"));
        }
    }
    let memory = scale.memory.borrow();
    for (when, state) in memory.iter() {
        step(&format!(
            "memory {when}: {}",
            [
                "total",
                "available",
                "anon",
                "slab",
                "page_tables",
                "kernel_stack",
                "k3s_rss",
                "containerd_rss",
                "runc_shims",
                "runc_shims_pss",
                "node_service_rss",
                "vm_rss",
                "kubepods",
                "smolvm_pods"
            ]
            .map(|key| format!("{key} {}", &state[key]))
            .join(", ")
        ));
    }
    assert!(wrong.is_empty(), "warnings:\n{}", wrong.join("\n"));
}

#[test]
fn computes_kubepods_cpu_weight() {
    assert_eq!(kubepods_cpu_weight(16), 625);
    assert_eq!(kubepods_cpu_weight(128), 5000);
    assert_eq!(kubepods_cpu_weight(512), 10_000);
}

#[test]
fn writes_the_scale_pods() {
    let pause = pause_deployment("ns", 500);
    assert!(pause.contains("  replicas: 500\n"), "{pause}");
    assert!(
        pause.contains("resources: {requests: {cpu: 160m, memory: 320Mi}}"),
        "{pause}"
    );
    assert!(!pause.contains("runtimeClassName"), "{pause}");
    let vm = vm_pod("ns", "docker.io/test:1", "vm-1");
    assert!(
        vm.contains(
            r#"metadata: {name: vm-1, namespace: ns, labels: {app: vm}, annotations: {"smolvm-pods.floci.io/idle-timeout": "24h"}}"#
        ),
        "{vm}"
    );
    assert!(vm.contains("  runtimeClassName: smolvm-pods\n"), "{vm}");
    assert!(
        vm.contains(r#"args: [hold, -mib, "64", app, ":8080"]"#),
        "{vm}"
    );
    assert!(
        vm.contains("resources: {requests: {cpu: 500m, memory: 1024Mi}}"),
        "{vm}"
    );
    let burner = burner_pod("ns", "docker.io/test:1");
    assert!(burner.contains(r#"args: [sleep, "3600"]"#), "{burner}");
    assert!(!burner.contains("runtimeClassName"), "{burner}");
}
