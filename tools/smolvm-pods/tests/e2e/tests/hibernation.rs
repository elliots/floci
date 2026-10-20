//! Transparent hibernation. The node service hibernates and wakes pods
//! through its API, and containerd and kubelet keep seeing them run. Every
//! call to the runtime goes through `smolvm_pods_e2e::runtime`.
//! tests/e2e/README.md lists the checks and documents the runtime's contract.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::thread;
use std::time::{Duration, Instant};

use smolvm_pods_e2e::checkers::{check_monotonic, settled, ClockSample, Ticks};
use smolvm_pods_e2e::recorder::Recorder;
use smolvm_pods_e2e::runtime::{
    self, AgentStats, Api, CtlError, FaultPoint, PodState, Role, Wake, Window,
};
use smolvm_pods_e2e::{
    describe, eventually, expect, run_checks, step, summary, App, Arch, Cluster, NetnsHolder, Node,
    Pod, PodSpec, Runtime, State, VmProcess, COMMAND_TIMEOUT, NO_ROSETTA,
};

const PORT: u16 = 8080;

/// The pods of most checks: Rosetta by default, an amd64 image, and Rosetta
/// turned off. None has an annotation that turns Rosetta on.
const ARM64: &str = "arm64";
const AMD64: &str = "amd64";
const PLAIN: &str = "arm64-plain";
const CYCLED: [&str; 3] = [ARM64, AMD64, PLAIN];
/// The pods of the resume-through-the-shim check, with Rosetta by default
/// and without.
const RESUME: &str = "resume";
const RESUME_PLAIN: &str = "resume-plain";
/// A runc pod, from which the resume-through-the-shim check reaches the VM
/// pods.
const RUNC: &str = "runc";
/// The extra pods of the check for no process per sleeping pod.
const IDLE: [&str; 5] = ["idle-1", "idle-2", "idle-3", "idle-4", "idle-5"];

/// MiB that the app fills and hashes, in each kind of pod. A pod VM has 1 GiB.
const HOLD_MIB: u32 = 256;
const RESUME_MIB: u32 = 512;
const IDLE_MIB: u32 = 16;

/// The memory of a node that the test creates.
const NODE_MEMORY_MIB: u32 = 8192;

/// The hibernation cycles, and how long the pods sleep in them.
const CYCLES: u32 = 20;
const SHORT_SLEEP: Duration = Duration::from_secs(5);
const LONG_SLEEP: Duration = Duration::from_secs(60);
/// The resume-through-the-shim check's cycles, and how long the pods sleep
/// in them.
const RESUME_CYCLES: u32 = 20;
const RESUME_SLEEP: Duration = Duration::from_secs(2);

/// The guest's realtime after a wake may be this far from the node's.
const REALTIME_SKEW_NS: i64 = 100_000_000;
/// The guest's monotonic clock may be this far outside its expected range.
const MONOTONIC_TOLERANCE_NS: i64 = 50_000_000;
/// After a hibernate, the node's anonymous memory must have dropped by this
/// share of the VM process's at least.
const FREED_SHARE: f64 = 0.75;
/// The resume-through-the-shim check's limits.
const FIRST_ANSWER_MS: f64 = 500.0;
const HUGE_SHARE: f64 = 0.95;
const IPV6_PROBE_MS: f64 = 5.0;
/// The front shim's largest growth per sleeping pod.
const USS_PER_POD_KIB: u64 = 50;
/// How long crictl may take to answer for a sleeping pod.
const CRI_CALL_MS: f64 = 1000.0;
/// How many samples of the guest agent make one record, and the time between
/// them.
const AGENT_SAMPLES: usize = 5;
const AGENT_SAMPLE_GAP: Duration = Duration::from_millis(200);

#[test]
#[ignore = "needs a Mac with smolvm and Docker: SMOLVM_PODS_E2E=1 cargo test -- --ignored"]
fn hibernation() {
    let cluster = Cluster::start_with_memory("hibernation", Some(NODE_MEMORY_MIB));
    let testapp = cluster.node_path("testapp");
    cluster
        .node
        .copy_in(&cluster.workload(Arch::Arm64, "testapp"), &testapp);
    cluster.node.sh(&format!("chmod +x {testapp}"));
    let recorder = Recorder::start(&cluster.node, &cluster.node_path("events.log"));
    let pods = cluster.run_pods(&specs());
    let api = Api::connect(&cluster.node, &testapp, Duration::from_secs(60));
    let suite = Suite::new(&cluster, api, recorder, testapp, pods);
    run_checks(&[
        ("20 cycles", &|| cycles(&suite)),
        ("logs", &|| logs(&suite)),
        ("clock", &|| clock(&suite)),
        ("TSO", &|| tso(&suite)),
        ("exec wakes", &|| exec_wakes(&suite)),
        ("merged wakes", &|| merged_wakes(&suite)),
        ("wake during a pause", &|| wake_during_pause(&suite)),
        ("calls while asleep", &|| calls_while_asleep(&suite)),
        ("resume through the shim", &|| {
            resume_through_the_shim(&suite)
        }),
        ("no process per sleeping pod", &|| {
            no_process_per_pod(&suite)
        }),
        ("SIGKILL of the node service", &|| node_service_kill(&suite)),
        ("containerd restart", &|| containerd_restart(&suite)),
        ("delete while asleep", &|| delete_while_asleep(&suite)),
        ("no false exits", &|| no_false_exits(&suite)),
        ("node settings and guest health", &|| node_health(&suite)),
        ("SIGKILL of the front shim", &|| front_shim_kill(&suite)),
    ]);
}

/// The test's pods.
fn specs() -> Vec<PodSpec> {
    let hold = |name, arch, mib, annotations| PodSpec {
        name,
        arch,
        runtime: Runtime::SmolvmPods,
        app: App::Hold { mib },
        containers: &[("app", PORT)],
        annotations,
    };
    vec![
        hold(ARM64, Arch::Arm64, HOLD_MIB, &[]),
        hold(AMD64, Arch::Amd64, HOLD_MIB, &[]),
        hold(PLAIN, Arch::Arm64, HOLD_MIB, &[NO_ROSETTA]),
        hold(RESUME, Arch::Arm64, RESUME_MIB, &[]),
        hold(RESUME_PLAIN, Arch::Arm64, RESUME_MIB, &[NO_ROSETTA]),
        PodSpec {
            name: RUNC,
            arch: Arch::Arm64,
            runtime: Runtime::Runc,
            app: App::Serve,
            containers: &[("app", PORT)],
            annotations: &[],
        },
    ]
}

/// What a pod's app reports of its process, which a wake must not change.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Identity {
    pid: String,
    start_ticks: String,
    boot_id: String,
}

/// What a wake must leave unchanged in a pod.
struct Baseline {
    hash: String,
    identity: Identity,
    ip: String,
    vm_dir: String,
    /// The CRI ids of the pod's containers.
    containers: Vec<String>,
}

/// The guest agent's settled reading after a cycle, or why it could not be
/// measured.
type AgentSample = (u32, Result<AgentStats, String>);

/// What checks record for later checks.
#[derive(Default)]
struct Records {
    /// The hibernation cycles that completed.
    cycles: u32,
    /// The event cursors at the start and the end of the cycles.
    cycle_events: (Option<u64>, Option<u64>),
    clock_failures: Vec<String>,
    clock_samples: u32,
    tso_failures: Vec<String>,
    tso_checks: u32,
    /// Where the Rosetta default did not hold at setup.
    default_rosetta: Vec<String>,
    /// The guest agent after each cycle that measured it.
    agent: HashMap<&'static str, Vec<AgentSample>>,
    /// The delete of the sleeping pod: the recorder's line at the delete,
    /// the pod's CRI ids, and kubelet's event reasons for it before the
    /// delete.
    delete_mark: Option<usize>,
    deleted: Vec<String>,
    deleted_reasons: Option<String>,
}

/// The test's pods and what it measured.
struct Suite<'a> {
    cluster: &'a Cluster,
    api: Api<'a>,
    recorder: Recorder<'a>,
    /// The arm64 test app in the node, which makes requests and reads the
    /// node's clocks.
    testapp: String,
    pods: Vec<Pod<'a>>,
    baseline: HashMap<&'static str, Baseline>,
    node_boot_id: String,
    records: RefCell<Records>,
}

impl<'a> Suite<'a> {
    /// Waits for each app's first answer and records what the checks
    /// compare with: this proves the app, its logs and the node's requests.
    fn new(
        cluster: &'a Cluster,
        api: Api<'a>,
        recorder: Recorder<'a>,
        testapp: String,
        pods: Vec<Pod<'a>>,
    ) -> Suite<'a> {
        let mut suite = Suite {
            cluster,
            api,
            recorder,
            testapp,
            pods,
            baseline: HashMap::new(),
            node_boot_id: cluster.node_kernel().boot_id,
            records: RefCell::default(),
        };
        for name in [ARM64, AMD64, PLAIN, RESUME, RESUME_PLAIN] {
            // An amd64 app fills its memory under Rosetta in a nested VM.
            eventually(Duration::from_secs(180), "the app's first answer", || {
                suite.request(name, "/ping")
            });
            let pod = suite.pod(name);
            let vm = expect(suite.vm(pod));
            let baseline = Baseline {
                hash: expect(suite.hash(name)),
                identity: expect(suite.identity(name)),
                ip: pod.ip(),
                vm_dir: vm.dir.clone(),
                containers: suite.containers(pod),
            };
            let sample = expect(suite.clock_sample(name));
            let rosetta = !matches!(name, PLAIN | RESUME_PLAIN);
            let tso = expect(suite.tso_marked(&vm));
            if tso != rosetta {
                suite.records.borrow_mut().default_rosetta.push(format!(
                    "pod {name}: TSO is {} in its VM process, expected {}",
                    on_off(tso),
                    on_off(rosetta)
                ));
            }
            let ticks = eventually(Duration::from_secs(30), "the app's counter lines", || {
                let ticks = Ticks::parse(&pod.logs("app"));
                ticks.check(0).map(|()| ticks)
            });
            step(&format!(
                "pod {name}: sha256 {}, pid {}, boot id {}, IP {}, VM process {} with TSO {}, \
                 realtime skew {:.1} ms, {ticks}",
                &baseline.hash[..16],
                baseline.identity.pid,
                baseline.identity.boot_id,
                baseline.ip,
                vm.pid,
                on_off(tso),
                sample.realtime_skew() as f64 / 1e6,
            ));
            suite.baseline.insert(name, baseline);
        }
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

    /// GETs `path` from a pod's app at its IP, from the node.
    fn request(&self, name: &str, path: &str) -> Result<State, String> {
        self.request_with(name, path, &[])
    }

    fn request_with(&self, name: &str, path: &str, flags: &[&str]) -> Result<State, String> {
        let ip = match self.baseline.get(name) {
            Some(base) => base.ip.clone(),
            None => self.pod(name).ip(),
        };
        let url = format!("http://{ip}:{PORT}{path}");
        let args = [
            &[self.testapp.as_str(), "get", "-timeout", "60s"],
            flags,
            &[&url],
        ]
        .concat();
        let output = self.node().exec(&args, COMMAND_TIMEOUT);
        if output.success() {
            Ok(State::parse(&output.stdout))
        } else {
            Err(format!("GET {url} from the node: {}", describe(&output)))
        }
    }

    fn hash(&self, name: &str) -> Result<String, String> {
        Ok(self.request(name, "/hash")?["sha256"].to_string())
    }

    fn identity(&self, name: &str) -> Result<Identity, String> {
        let id = self.request(name, "/id")?;
        Ok(Identity {
            pid: id["pid"].to_string(),
            start_ticks: id["start_ticks"].to_string(),
            boot_id: id["boot_id"].to_string(),
        })
    }

    /// Reads the guest's clocks, with the node's clocks around the request.
    fn clock_sample(&self, name: &str) -> Result<ClockSample, String> {
        let state = self.request_with(name, "/clock", &["-times"])?;
        let ns = |key: &str| -> Result<i64, String> {
            state[key]
                .parse()
                .map_err(|e| format!("{key} of /clock: {e}"))
        };
        Ok(ClockSample {
            guest_realtime: ns("realtime_ns")?,
            guest_monotonic: ns("monotonic_ns")?,
            node_realtime: (
                ns("request_start_realtime_ns")?,
                ns("request_end_realtime_ns")?,
            ),
            node_monotonic: (
                ns("request_start_monotonic_ns")?,
                ns("request_end_monotonic_ns")?,
            ),
        })
    }

    /// The pod's one VM process.
    fn vm(&self, pod: &Pod) -> Result<VmProcess, String> {
        match &pod.vm_processes()[..] {
            [vm] => Ok(vm.clone()),
            vms => Err(format!(
                "the node should run one VM process for pod {} (sandbox {}), runs {vms:?}",
                pod.name, pod.sandbox
            )),
        }
    }

    /// Whether the TSO marker beside the VM's boot config names this VM
    /// process, which then turned TSO on.
    fn tso_marked(&self, vm: &VmProcess) -> Result<bool, String> {
        let script = format!(
            r#"marker=$(cat "$1/{}" 2>/dev/null)
start=$(sed 's/.*) //' "/proc/$2/stat" | cut -d ' ' -f 20)
if [ "$marker" = "$2 $start" ]; then echo on; else echo off; fi"#,
            runtime::TSO_MARKER
        );
        let pid = vm.pid.to_string();
        let output = self
            .node()
            .exec(&["sh", "-c", &script, "sh", &vm.dir, &pid], COMMAND_TIMEOUT);
        if output.success() {
            Ok(output.stdout.trim() == "on")
        } else {
            Err(format!("the TSO marker of {vm:?}: {}", describe(&output)))
        }
    }

    /// The node's anonymous memory and, given a pid, that process's, in KiB.
    fn memory(&self, pid: Option<u32>) -> Result<(u64, u64), String> {
        let script = r#"awk '/^AnonPages:/ { print $2 }' /proc/meminfo
if [ -n "$1" ]; then awk '/^Anonymous:/ { print $2 }' "/proc/$1/smaps_rollup"; else echo 0; fi"#;
        let pid = pid.map(|pid| pid.to_string()).unwrap_or_default();
        let output = self
            .node()
            .exec(&["sh", "-c", script, "sh", &pid], COMMAND_TIMEOUT);
        let values: Vec<u64> = output
            .stdout
            .split_whitespace()
            .filter_map(|v| v.parse().ok())
            .collect();
        match values[..] {
            [node, process] if output.success() => Ok((node, process)),
            _ => Err(format!("memory of the node: {}", describe(&output))),
        }
    }

    /// The CRI ids of a pod's containers.
    #[track_caller]
    fn containers(&self, pod: &Pod) -> Vec<String> {
        let ids = self
            .node()
            .sh(&format!("crictl ps -a -q --pod {}", pod.sandbox));
        ids.split_whitespace().map(str::to_string).collect()
    }

    /// The CRI state of every container and sandbox, by id.
    fn cri_states(&self) -> Result<HashMap<String, String>, String> {
        let script = r#"crictl ps -a -o json | jq -r '.containers[] | "\(.id) \(.state)"'
crictl pods -o json | jq -r '.items[] | "\(.id) \(.state)"'"#;
        let lines = self.node().try_sh(script)?;
        Ok(lines
            .lines()
            .filter_map(|line| line.split_once(' '))
            .map(|(id, state)| (id.to_string(), state.to_string()))
            .collect())
    }

    /// Fails unless the pods' sandboxes are ready and their containers run.
    fn check_running(&self, names: &[&str]) -> Result<(), String> {
        let states = self.cri_states()?;
        let mut wrong = Vec::new();
        for &name in names {
            let pod = self.pod(name);
            let mut expected = vec![(pod.sandbox.clone(), "SANDBOX_READY")];
            for id in &self.baseline[name].containers {
                expected.push((id.clone(), "CONTAINER_RUNNING"));
            }
            for (id, want) in expected {
                let state = states.get(&id).map_or("absent", String::as_str);
                if state != want {
                    wrong.push(format!("pod {name}: {id} is {state}, not {want}"));
                }
            }
        }
        if wrong.is_empty() {
            Ok(())
        } else {
            Err(wrong.join("\n"))
        }
    }

    /// Fails unless the pod's app, IP, sandbox, restart counts and VM are
    /// those of the baseline.
    fn check_unchanged(&self, name: &'static str) -> Result<(), String> {
        let pod = self.pod(name);
        let base = &self.baseline[name];
        let mut changed = Vec::new();
        let identity = self.identity(name)?;
        if identity != base.identity {
            changed.push(format!("/id {identity:?}, was {:?}", base.identity));
        }
        let hash = self.hash(name)?;
        if hash != base.hash {
            changed.push(format!("/hash {hash}, was {}", base.hash));
        }
        let fields = self.cluster.node.try_kubectl(
            &[
                "get",
                "pod",
                "-n",
                &self.cluster.namespace,
                name,
                "-o",
                "jsonpath={.status.podIP} {.status.containerStatuses[*].restartCount}",
            ],
            COMMAND_TIMEOUT,
        )?;
        let mut fields = fields.split_whitespace();
        let ip = fields.next().unwrap_or_default();
        if ip != base.ip {
            changed.push(format!("pod IP {ip}, was {}", base.ip));
        }
        let restarts: Vec<&str> = fields.collect();
        if restarts.is_empty() || restarts.iter().any(|n| *n != "0") {
            changed.push(format!("restart counts {restarts:?}"));
        }
        let sandbox = self.cluster.ready_sandbox(name)?;
        if sandbox != pod.sandbox {
            changed.push(format!("sandbox {sandbox}, was {}", pod.sandbox));
        }
        let vm = self.vm(pod)?;
        if vm.dir != base.vm_dir {
            changed.push(format!("VM directory {}, was {}", vm.dir, base.vm_dir));
        }
        if changed.is_empty() {
            Ok(())
        } else {
            Err(format!("pod {name} changed: {}", changed.join("; ")))
        }
    }

    /// Hibernates a pod through the API, and checks that its VM process is
    /// gone and its memory freed.
    fn hibernate(&self, pod: &Pod) -> Result<Window, String> {
        let vm = self.vm(pod)?;
        let (before, vm_anonymous) = self.memory(Some(vm.pid))?;
        let (done, window) = self
            .api
            .hibernate(&pod.sandbox)
            .map_err(|e| format!("hibernate pod {}: {e}", pod.name))?;
        if done.pod.state != PodState::Asleep || done.pod.vm_pid.is_some() {
            return Err(format!(
                "pod {} after hibernate: {} with VM process {:?}",
                pod.name, done.pod.state, done.pod.vm_pid
            ));
        }
        let left = pod.vm_processes();
        if !left.is_empty() {
            return Err(format!(
                "pod {} is asleep, but its VM process runs: {left:?}",
                pod.name
            ));
        }
        let (after, _) = self.memory(None)?;
        let freed = before.saturating_sub(after);
        if (freed as f64) < FREED_SHARE * vm_anonymous as f64 {
            return Err(format!(
                "hibernating pod {} freed {} MiB of the node's anonymous memory, \
                 but its VM process had {} MiB",
                pod.name,
                freed >> 10,
                vm_anonymous >> 10
            ));
        }
        Ok(window)
    }

    /// Wakes a pod through the API, and checks that one VM process runs and
    /// the API names it.
    fn wake(&self, pod: &Pod) -> Result<(Wake, Window), String> {
        let (wake, window) = self
            .api
            .wake(&pod.sandbox)
            .map_err(|e| format!("wake pod {}: {e}", pod.name))?;
        if wake.pod.state != PodState::Awake {
            return Err(format!("pod {} after wake: {}", pod.name, wake.pod.state));
        }
        let vm = self.vm(pod)?;
        if wake.pod.vm_pid != Some(vm.pid) {
            return Err(format!(
                "pod {}: the API names VM process {:?}, the node runs {}",
                pod.name, wake.pod.vm_pid, vm.pid
            ));
        }
        Ok((wake, window))
    }

    /// The pod's state through the API.
    fn state(&self, pod: &Pod) -> Result<PodState, String> {
        self.api.state(&pod.sandbox)
    }

    /// Hibernates the pod unless it is asleep.
    fn ensure_asleep(&self, pod: &Pod) -> Result<(), String> {
        if self.state(pod)? != PodState::Asleep {
            self.hibernate(pod)?;
        }
        Ok(())
    }

    /// Wakes the pod unless it is awake.
    fn ensure_awake(&self, pod: &Pod) -> Result<(), String> {
        if self.state(pod)? != PodState::Awake {
            self.wake(pod)?;
        }
        Ok(())
    }

    /// Waits `duration` while the pods sleep, and checks every 10 s that
    /// crictl shows them running and the API asleep.
    fn sleep_watching(&self, names: &[&str], duration: Duration) -> Result<(), String> {
        let end = Instant::now() + duration;
        loop {
            self.check_running(names)?;
            for &name in names {
                let state = self.state(self.pod(name))?;
                if state != PodState::Asleep {
                    return Err(format!("pod {name} is {state} while it should sleep"));
                }
            }
            let left = end.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(());
            }
            thread::sleep(left.min(Duration::from_secs(10)));
        }
    }

    /// The highest counter in the pod's log.
    #[track_caller]
    fn last_tick(&self, name: &str) -> u64 {
        Ticks::parse(&self.pod(name).logs("app"))
            .last
            .unwrap_or_default()
    }

    /// Fails unless the pod's log gets counters past `after`, without a gap.
    fn check_logs_continue(&self, name: &str, after: u64) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let ticks = Ticks::parse(&self.pod(name).logs("app"));
            if !ticks.gaps.is_empty() || !ticks.out_of_order.is_empty() {
                return Err(format!(
                    "pod {name}: {ticks}, gaps {:?}, out of order {:?}",
                    ticks.gaps, ticks.out_of_order
                ));
            }
            if ticks.last.unwrap_or_default() > after + 20 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "pod {name}: its log stopped: {ticks}, expected counters past {after}"
                ));
            }
            thread::sleep(Duration::from_secs(1));
        }
    }

    /// The events after `cursor` about `pod`.
    #[track_caller]
    fn events_since(&self, cursor: u64, pod: &Pod) -> Vec<runtime::Event> {
        expect(self.api.events_of(cursor, &pod.sandbox))
    }

    fn record_clock(
        &self,
        name: &str,
        cycle: u32,
        before: &ClockSample,
        after: &ClockSample,
        hibernate: Window,
        wake: Window,
    ) {
        let mut records = self.records.borrow_mut();
        records.clock_samples += 1;
        let skew = after.realtime_skew();
        if skew.abs() > REALTIME_SKEW_NS {
            records.clock_failures.push(format!(
                "pod {name} cycle {cycle}: the guest's realtime is {:.1} ms off the node's",
                skew as f64 / 1e6
            ));
        }
        let monotonic = check_monotonic(
            before,
            after,
            (hibernate.start_ns, hibernate.end_ns),
            (wake.start_ns, wake.end_ns),
            MONOTONIC_TOLERANCE_NS,
        );
        if let Err(e) = monotonic {
            records
                .clock_failures
                .push(format!("pod {name} cycle {cycle}: {e}"));
        }
    }

    fn record_tso(&self, name: &str, cycle: u32) {
        let rosetta = name != PLAIN;
        let marked = self.vm(self.pod(name)).and_then(|vm| self.tso_marked(&vm));
        let mut records = self.records.borrow_mut();
        records.tso_checks += 1;
        match marked {
            Ok(marked) if marked == rosetta => {}
            Ok(marked) => records.tso_failures.push(format!(
                "pod {name} cycle {cycle}: TSO is {} in the resumed VM process",
                on_off(marked)
            )),
            Err(e) => records
                .tso_failures
                .push(format!("pod {name} cycle {cycle}: {e}")),
        }
    }

    /// Records the settled reading of `AGENT_SAMPLES` samples of the guest
    /// agent. A connection that the agent serves for a moment, such as
    /// kubelet's stats poll, adds a thread and descriptors to one sample at
    /// most, while a leak shows in every sample.
    fn record_agent(&self, name: &'static str, cycle: u32) {
        let mut samples = Vec::with_capacity(AGENT_SAMPLES);
        for i in 0..AGENT_SAMPLES {
            if i > 0 {
                thread::sleep(AGENT_SAMPLE_GAP);
            }
            samples.push(
                self.api
                    .agent(&self.pod(name).sandbox)
                    .map_err(|e| e.to_string()),
            );
        }
        if samples.iter().any(|s| *s != samples[0]) {
            step(&format!(
                "pod {name} cycle {cycle}: guest agent samples {samples:?}"
            ));
        }
        let stats = settled(&samples);
        let mut records = self.records.borrow_mut();
        records.agent.entry(name).or_default().push((cycle, stats));
    }
}

fn on_off(on: bool) -> &'static str {
    if on {
        "on"
    } else {
        "off"
    }
}

/// 20 cycles of the three pods: hibernate through the API, the VM process
/// gone and its memory freed, a sleep (60 s in every fifth cycle, 5 s in the
/// others) while crictl shows every task running, and a wake through the
/// API, after which the app's hash and identity, the pod IP, the sandbox and
/// the restart counts are unchanged. It also records what the clock, TSO,
/// and node settings and guest health checks need.
fn cycles(suite: &Suite) {
    let start = expect(suite.api.cursor().map_err(|e| e.to_string()));
    suite.records.borrow_mut().cycle_events.0 = Some(start);
    for cycle in 1..=CYCLES {
        let sleep = if cycle % 5 == 0 {
            LONG_SLEEP
        } else {
            SHORT_SLEEP
        };
        step(&format!("cycle {cycle} of {CYCLES}, sleeping {sleep:?}"));
        let mut before = HashMap::new();
        let mut hibernated = HashMap::new();
        for name in CYCLED {
            before.insert(name, expect(suite.clock_sample(name)));
            hibernated.insert(name, expect(suite.hibernate(suite.pod(name))));
        }
        expect(suite.sleep_watching(&CYCLED, sleep));
        for name in CYCLED {
            let (_, window) = expect(suite.wake(suite.pod(name)));
            let after = expect(suite.clock_sample(name));
            suite.record_clock(name, cycle, &before[name], &after, hibernated[name], window);
            expect(suite.check_unchanged(name));
            suite.record_tso(name, cycle);
            if cycle == 1 || cycle == CYCLES {
                suite.record_agent(name, cycle);
            }
        }
        expect(suite.check_running(&CYCLED));
        suite.records.borrow_mut().cycles = cycle;
    }
    let end = expect(suite.api.cursor().map_err(|e| e.to_string()));
    suite.records.borrow_mut().cycle_events.1 = Some(end);
}

/// Fails unless the cycle check completed every cycle, for the checks that
/// judge what the cycles did.
#[track_caller]
fn require_cycles(suite: &Suite) {
    let done = suite.records.borrow().cycles;
    assert_eq!(
        done, CYCLES,
        "the cycle check completed {done} of {CYCLES} hibernation cycles"
    );
}

/// Each pod's counter lines in kubectl logs run from 1 to the app's latest
/// counter, with no gap and no repeat, across all the cycles.
fn logs(suite: &Suite) {
    let mut failed = Vec::new();
    for name in CYCLED {
        let latest: u64 = expect(suite.request(name, "/ticks"))["tick"]
            .parse()
            .expect("the app's counter");
        let deadline = Instant::now() + Duration::from_secs(20);
        let ticks = loop {
            let ticks = Ticks::parse(&suite.pod(name).logs("app"));
            if ticks.last >= Some(latest) || Instant::now() >= deadline {
                break ticks;
            }
            thread::sleep(Duration::from_secs(1));
        };
        let result = ticks.check(0).and_then(|()| match ticks.last {
            Some(last) if last >= latest => Ok(()),
            _ => Err(format!(
                "{ticks}, but the app printed counter {latest} already"
            )),
        });
        match result {
            Ok(()) => step(&format!("pod {name}: {ticks}")),
            Err(e) => failed.push(format!("pod {name}: {e}")),
        }
    }
    assert!(failed.is_empty(), "{}", failed.join("\n"));
    require_cycles(suite);
}

/// After each wake of the cycles, the guest's realtime was within 100 ms of
/// the node's, and its monotonic clock had advanced by about the time it ran;
/// each wake's event records the clock step.
fn clock(suite: &Suite) {
    require_cycles(suite);
    let records = suite.records.borrow();
    assert!(
        records.clock_failures.is_empty(),
        "{}",
        records.clock_failures.join("\n")
    );
    assert_eq!(records.clock_samples, CYCLES * CYCLED.len() as u32);
    let (Some(start), Some(end)) = records.cycle_events else {
        panic!("the cycle check recorded no event cursors");
    };
    let events = expect(suite.api.events(start).map_err(|e| e.to_string()));
    let sandboxes: Vec<&str> = CYCLED.map(|name| suite.pod(name).sandbox.as_str()).to_vec();
    let wakes: Vec<_> = events
        .iter()
        .filter(|e| e.seq <= end && sandboxes.contains(&e.pod.as_str()))
        .filter(|e| e.from == "waking" && e.to == "awake")
        .collect();
    assert_eq!(
        wakes.len(),
        CYCLES as usize * CYCLED.len(),
        "wake events during the cycles"
    );
    let skews: Vec<f64> = wakes
        .iter()
        .map(|e| match e.clock {
            Some(clock) => clock.skew_ns as f64 / 1e6,
            None => panic!("a wake event without the clock step: {}", e.raw),
        })
        .collect();
    step(&format!(
        "{} clock samples within {} ms; skews before the step: {}",
        records.clock_samples,
        REALTIME_SKEW_NS / 1_000_000,
        summary(&skews, "ms")
    ));
}

/// The Rosetta default held at setup, every resume of a Rosetta pod in the
/// cycles turned TSO on in its new VM process and none of the pod without
/// Rosetta did, and after the last cycle the litmus test finds no violation
/// in the amd64 pod.
fn tso(suite: &Suite) {
    {
        let records = suite.records.borrow();
        let failures: Vec<&String> = records
            .default_rosetta
            .iter()
            .chain(&records.tso_failures)
            .collect();
        assert!(
            failures.is_empty(),
            "{}",
            failures
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join("\n")
        );
        require_cycles(suite);
        assert_eq!(records.tso_checks, CYCLES * CYCLED.len() as u32);
    }
    let pod = suite.pod(AMD64);
    let mut output = pod.exec("app", &["/litmus"]);
    for _ in 1..3 {
        if output.code() != Some(4) {
            break;
        }
        output = pod.exec("app", &["/litmus"]);
    }
    assert!(
        output.success(),
        "litmus in pod {AMD64}: {}",
        describe(&output)
    );
    step(&format!(
        "pod {AMD64}: {}",
        output.stdout.lines().last().unwrap_or_default()
    ));
}

/// Starts `kubectl exec` of `args` in the pod's app container in the
/// background in the node, writing its output to `out` and its exit status
/// to `out.status`.
#[track_caller]
fn exec_in_background(suite: &Suite, pod: &Pod, out: &str, args: &[&str]) {
    let script = r#"out=$1
shift
rm -f "$out" "$out.status"
setsid -f sh -c 'out=$1; shift; "$@" >"$out" 2>&1; echo $? >"$out.status"' sh "$out" "$@" \
  </dev/null >/dev/null 2>&1"#;
    let exec = [
        "kubectl",
        "exec",
        "-n",
        &suite.cluster.namespace,
        pod.name,
        "-c",
        "app",
        "--",
    ];
    let command = [&["sh", "-c", script, "sh", out][..], &exec[..], args].concat();
    let output = suite.node().exec(&command, COMMAND_TIMEOUT);
    assert!(output.success(), "{}", describe(&output));
}

/// Fails unless the call was refused with `reason`.
#[track_caller]
fn assert_refused<T: std::fmt::Debug>(result: Result<T, CtlError>, reason: &str) {
    match result {
        Err(e) if e.code == "refused" && e.reasons.iter().any(|r| r == reason) => {
            step(&format!("refused: {e}"));
        }
        other => panic!("expected a refusal with the reason {reason:?}, got {other:?}"),
    }
}

/// kubectl exec into a sleeping pod wakes it and returns stdout, stderr and
/// the exit code; while a 30 s exec runs, a hibernate is refused with
/// "exec running", and while a lease is held, with "lease held".
fn exec_wakes(suite: &Suite) {
    let pod = suite.pod(PLAIN);
    expect(suite.ensure_awake(pod));
    expect(suite.hibernate(pod));
    let cursor = expect(suite.api.cursor().map_err(|e| e.to_string()));
    let output = pod.exec("app", &["/testapp", "exit", "7"]);
    assert_eq!(output.code(), Some(7), "{}", describe(&output));
    assert!(
        output.stdout.contains("stdout before exit 7")
            && output.stderr.contains("stderr before exit 7"),
        "{}",
        describe(&output)
    );
    assert_eq!(expect(suite.state(pod)), PodState::Awake);
    let resumes: Vec<_> = suite
        .events_since(cursor, pod)
        .into_iter()
        .filter(|e| e.is_resume())
        .collect();
    assert!(
        resumes.len() == 1 && resumes[0].reason == "exec",
        "the exec should wake the pod once, with the reason exec: {:?}",
        resumes
            .iter()
            .map(|e| e.raw.to_string())
            .collect::<Vec<_>>()
    );
    step("kubectl exec woke the sleeping pod");

    let out = suite.cluster.node_path("long-exec");
    exec_in_background(suite, pod, &out, &["/testapp", "sleep", "30"]);
    eventually(Duration::from_secs(20), "the exec's lease", || {
        let status = suite.api.get(&pod.sandbox).map_err(|e| e.to_string())?;
        if status.has_lease("exec") {
            Ok(())
        } else {
            Err(format!("leases {:?}", status.leases))
        }
    });
    assert_refused(suite.api.hibernate(&pod.sandbox), "exec running");
    let status = eventually(Duration::from_secs(60), "the exec to end", || {
        suite.node().try_sh(&format!("cat {out}.status"))
    });
    let printed = suite.node().sh(&format!("cat {out}"));
    assert!(
        status.trim() == "0" && printed.contains("slept"),
        "the 30 s exec: status {status:?}, output {printed:?}"
    );

    let lease = "exec-wakes";
    expect(
        suite
            .api
            .hold(&pod.sandbox, lease, Duration::from_secs(30))
            .map(drop)
            .map_err(|e| e.to_string()),
    );
    assert_refused(suite.api.hibernate(&pod.sandbox), "lease held");
    expect(
        suite
            .api
            .release(&pod.sandbox, lease)
            .map(drop)
            .map_err(|e| e.to_string()),
    );
    expect(suite.hibernate(pod));
    expect(suite.wake(pod));
    expect(suite.check_unchanged(PLAIN));
}

/// 12 concurrent wakes of a sleeping pod cause one resume.
fn merged_wakes(suite: &Suite) {
    let pod = suite.pod(ARM64);
    expect(suite.ensure_awake(pod));
    expect(suite.hibernate(pod));
    let cursor = expect(suite.api.cursor().map_err(|e| e.to_string()));
    let calls: Vec<Vec<&str>> = (0..12).map(|_| runtime::wake_args(&pod.sandbox)).collect();
    let outcomes = expect(suite.api.concurrent(&calls));
    let mut resumed = 0;
    for outcome in outcomes {
        let wake = match outcome.map(|json| Wake::parse(&json)) {
            Ok(Ok(wake)) => wake,
            other => panic!("one of 12 wakes: {other:?}"),
        };
        assert_eq!(wake.pod.state, PodState::Awake, "{:?}", wake.pod.raw);
        resumed += usize::from(wake.resumed);
    }
    let resumes = suite
        .events_since(cursor, pod)
        .into_iter()
        .filter(|e| e.is_resume())
        .count();
    assert_eq!(resumes, 1, "resumes for 12 concurrent wakes");
    assert_eq!(resumed, 1, "wakes that report they resumed the pod");
    expect(suite.vm(pod));
    expect(suite.check_unchanged(ARM64));
    step("12 concurrent wakes: one resume");
}

/// A wake 100 ms into a pause leaves the pod awake. A wake cancels a pause
/// that is not durable, so the VM process must be the same, and no resume may
/// follow.
fn wake_during_pause(suite: &Suite) {
    let pod = suite.pod(ARM64);
    expect(suite.ensure_awake(pod));
    let vm = expect(suite.vm(pod));
    let cursor = expect(suite.api.cursor().map_err(|e| e.to_string()));
    let seen = expect(
        suite
            .api
            .wake_during_pause(&pod.sandbox, Duration::from_millis(100)),
    );
    assert_eq!(
        seen.state_before_wake, "pausing",
        "the pod's state before the wake"
    );
    let wake = match seen.wake.map(|json| Wake::parse(&json)) {
        Ok(Ok(wake)) => wake,
        other => panic!("the wake during the pause: {other:?}"),
    };
    assert_eq!(wake.pod.state, PodState::Awake);
    step(&format!(
        "the hibernate during which the wake came: {:?}",
        seen.hibernate.as_ref().map(|json| json.to_string())
    ));
    let events = suite.events_since(cursor, pod);
    let resumes = events.iter().filter(|e| e.is_resume()).count();
    let cancelled = events
        .iter()
        .any(|e| e.from == "pausing" && e.to == "awake" && e.cancelled);
    assert!(
        cancelled && resumes == 0,
        "the pause was not cancelled: {resumes} resumes followed; events: {:?}",
        events.iter().map(|e| e.raw.to_string()).collect::<Vec<_>>()
    );
    assert_eq!(expect(suite.vm(pod)).pid, vm.pid, "the VM process");
    expect(suite.check_unchanged(ARM64));
    step("a wake 100 ms into the pause cancelled it");
}

/// Times crictl's answers for a sleeping pod and returns, for each call, its
/// time in ms and what it printed of the state.
const CRI_CALLS: &str = r#"timed() {
  t0=$(date +%s%N); out=$("$@" 2>&1); s=$?; t1=$(date +%s%N)
  echo "$(( (t1 - t0) / 1000 )) $s $*"
  [ $s -eq 0 ] || echo "  $out"
}
sandbox=$1; shift
timed crictl statsp --id "$sandbox" -o json
timed crictl stats --pod "$sandbox" -o json
echo "state $(crictl inspectp "$sandbox" | jq -r .status.state) $sandbox"
for c in "$@"; do
  timed crictl stats --id "$c" -o json
  echo "state $(crictl inspect "$c" | jq -r .status.state) $c"
done"#;

/// For a sleeping pod, crictl stats and statsp answer within 1 s, crictl
/// inspect shows it running, and the pod stays asleep; a hold does not wake it
/// either.
fn calls_while_asleep(suite: &Suite) {
    let mut failed = Vec::new();
    for name in CYCLED {
        let pod = suite.pod(name);
        expect(suite.ensure_asleep(pod));
        let args = [
            &["sh", "-c", CRI_CALLS, "sh", pod.sandbox.as_str()][..],
            &suite.baseline[name]
                .containers
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()[..],
        ]
        .concat();
        let output = suite.node().exec(&args, COMMAND_TIMEOUT);
        step(&format!("pod {name} asleep:\n{}", output.stdout.trim_end()));
        for line in output.stdout.lines() {
            let fields: Vec<&str> = line.split_whitespace().collect();
            match fields[..] {
                ["state", state, id] if !matches!(state, "SANDBOX_READY" | "CONTAINER_RUNNING") => {
                    failed.push(format!("pod {name}: {id} is {state}"))
                }
                [us, status, ..] if us.parse::<f64>().is_ok() => {
                    let ms = us.parse::<f64>().unwrap() / 1000.0;
                    if status != "0" || ms > CRI_CALL_MS {
                        failed.push(format!("pod {name}: {line}"));
                    }
                }
                _ => {}
            }
        }
        let lease = "calls-while-asleep";
        let held = suite
            .api
            .hold(&pod.sandbox, lease, Duration::from_secs(30))
            .map_err(|e| e.to_string())
            .and_then(|_| {
                suite
                    .api
                    .release(&pod.sandbox, lease)
                    .map_err(|e| e.to_string())
            });
        if let Err(e) = held {
            failed.push(format!("pod {name}: hold and release: {e}"));
        }
        match (suite.state(pod), pod.vm_processes().len()) {
            (Ok(PodState::Asleep), 0) => {}
            (state, vms) => failed.push(format!(
                "pod {name} after the calls: {state:?} with {vms} VM processes"
            )),
        }
    }
    for name in CYCLED {
        expect(suite.wake(suite.pod(name)));
        expect(suite.check_unchanged(name));
    }
    assert!(failed.is_empty(), "{}", failed.join("\n"));
}

/// Runs `$3 firstanswer` in the network namespace of process `$1`: it starts
/// the wake `$5...` and polls URL `$4` from just before it. Then it prints
/// the VM process's RSS and AnonHugePages from the VM in directory `$2`, and
/// the time of smolvm's IPv6 probe in that process's network namespace. The
/// wake's output goes to `$2.wake`, which must not be in the VM directory.
const FIRST_ANSWER: &str = r#"holder=$1 dir=$2 testapp=$3 url=$4 out=$5
shift 5
nsenter -t "$holder" -n "$testapp" firstanswer -timeout 30s -out "$out" "$url" "$@"
vm=
for p in /proc/[0-9]*; do
  case $(tr '\0' ' ' < "$p/cmdline" 2>/dev/null) in
    *" _boot-vm $dir/"*) vm=${p#/proc/} ;;
  esac
done
[ -n "$vm" ] || { echo "no VM process in $dir" >&2; exit 1; }
awk '/^Rss:/ { print "rss_kib=" $2 } /^AnonHugePages:/ { print "huge_kib=" $2 }' "/proc/$vm/smaps_rollup"
nsenter --net="/proc/$vm/ns/net" "$testapp" ipv6probe | sed 's/^/ipv6_/'"#;

/// What [`FIRST_ANSWER`] measured.
struct Answer {
    /// From the start of the command to the first answer, and from its end.
    first_answer_ms: f64,
    after_return_ms: f64,
    attempts: u64,
    command_exit: Option<i32>,
    /// AnonHugePages over RSS of the VM process at the first answer.
    huge_share: f64,
    ipv6_ms: f64,
}

/// Runs `command` in the node while the runc pod polls `url`, with
/// [`FIRST_ANSWER`], and returns when the VM pod first answered and its
/// memory at that answer. The command's stdout goes to `out`.
fn first_answer(
    suite: &Suite,
    pod: &Pod,
    holder: &str,
    url: &str,
    out: &str,
    command: &[&str],
) -> Result<Answer, String> {
    let dir = &suite.baseline[pod.name].vm_dir;
    let script = [
        "sh",
        "-c",
        FIRST_ANSWER,
        "sh",
        holder,
        dir,
        &suite.testapp,
        url,
        out,
    ];
    let output = suite
        .node()
        .exec(&[&script[..], command].concat(), Duration::from_secs(120));
    if !output.success() {
        return Err(format!(
            "{command:?} and the first answer: {}",
            describe(&output)
        ));
    }
    let state = State::parse(&output.stdout);
    let number = |key: &str| -> Result<f64, String> {
        state[key].parse::<f64>().map_err(|e| format!("{key}: {e}"))
    };
    let start = number("command_start_monotonic_ns")?;
    let end = number("command_end_monotonic_ns")?;
    let first = number("first_answer_monotonic_ns")?;
    Ok(Answer {
        first_answer_ms: (first - start) / 1e6,
        after_return_ms: (first - end) / 1e6,
        attempts: number("attempts")? as u64,
        command_exit: state["command_exit"].parse().ok(),
        huge_share: number("huge_kib")? / number("rss_kib")?.max(1.0),
        ipv6_ms: number("ipv6_ms")?,
    })
}

/// One cycle of the resume-through-the-shim check.
struct ResumeCycle {
    answer: Answer,
    steps: Vec<(String, f64)>,
}

/// One wake of the resume-through-the-shim check, with the first answer
/// from the runc pod.
fn resume_cycle(
    suite: &Suite,
    pod: &Pod,
    holder: &str,
    url: &str,
    out: &str,
) -> Result<ResumeCycle, String> {
    let wake = runtime::wake_args(&pod.sandbox);
    let command = [&runtime::CTL[..], &wake[..]].concat();
    let cursor = suite.api.cursor().map_err(|e| e.to_string())?;
    let answer = first_answer(suite, pod, holder, url, out, &command)?;
    let printed = suite.node().sh(&format!("cat {out}"));
    let json = runtime::parse_outcome(&wake, answer.command_exit, &printed, "")
        .map_err(|e| e.to_string())?;
    let wake = Wake::parse(&json)?;
    // The poll's first SYN, sent just before the wake call, can start the
    // resume as a traffic wake, which the call then joins.
    let events = suite.api.events(cursor).map_err(|e| e.to_string())?;
    let resumes: Vec<&str> = events
        .iter()
        .filter(|e| e.pod == pod.sandbox && e.is_resume())
        .map(|e| e.reason.as_str())
        .collect();
    let joined = !wake.resumed && resumes == ["traffic"];
    if !(wake.resumed || joined) || resumes.len() != 1 {
        return Err(format!(
            "the wake did not resume the pod once: {json}; resumes {resumes:?}"
        ));
    }
    Ok(ResumeCycle {
        answer,
        steps: wake.steps_ms,
    })
}

/// Resume through the shim and pod networking. For a pod with Rosetta by
/// default and one without, 20 cycles of a hibernate and a wake through
/// the API while the runc pod polls the VM pod's IP; the poll can start the
/// resume first, as a traffic wake. Each cycle's first answer comes within
/// 0.5 s of the wake call, the VM process has its RSS in huge pages at that
/// answer, smolvm's IPv6 probe costs at most 5 ms in its namespace, and the
/// memory hash is unchanged. Prints each wake step's duration, and the same
/// measurement of the awake pod before the cycles for comparison.
fn resume_through_the_shim(suite: &Suite) {
    let runc = suite.pod(RUNC);
    let runc_container = suite.containers(runc);
    let holder = suite.node().sh(&format!(
        "crictl inspect {} | jq -r .info.pid",
        runc_container.first().expect("the runc pod's container")
    ));
    let holder = holder.trim();
    let mut failed = Vec::new();
    let mut report = String::new();
    for name in [RESUME, RESUME_PLAIN] {
        let pod = suite.pod(name);
        let base = &suite.baseline[name];
        let url = format!("http://{}:{PORT}/ping", base.ip);
        let out = suite.cluster.node_path(&format!("{name}.wake"));
        expect(suite.ensure_awake(pod).or_else(|e| {
            // If the API call fails, a pod with one VM process is still
            // awake: the pods start awake.
            if pod.vm_processes().len() == 1 {
                Ok(())
            } else {
                Err(e)
            }
        }));
        // The runc pod learns the VM pod's MAC, as a client of a pod would
        // have it, and the awake pod is measured as the cycles will be.
        let awake = expect(first_answer(suite, pod, holder, &url, &out, &["true"]));
        let _ = writeln!(
            report,
            "pod {name} awake: AnonHugePages / RSS {:.3}, IPv6 probe {:.3} ms",
            awake.huge_share, awake.ipv6_ms
        );
        let mut rows = Vec::new();
        for cycle in 1..=RESUME_CYCLES {
            let result = suite.hibernate(pod).and_then(|_| {
                thread::sleep(RESUME_SLEEP);
                resume_cycle(suite, pod, holder, &url, &out)
            });
            match result {
                Ok(row) => rows.push(row),
                Err(e) => {
                    failed.push(format!("pod {name} cycle {cycle}: {e}"));
                    break;
                }
            }
            match suite.hash(name) {
                Ok(hash) if hash == base.hash => {}
                other => failed.push(format!("pod {name} cycle {cycle}: /hash {other:?}")),
            }
        }
        for (cycle, row) in rows.iter().enumerate() {
            let (cycle, answer) = (cycle + 1, &row.answer);
            if answer.first_answer_ms >= FIRST_ANSWER_MS {
                failed.push(format!(
                    "pod {name} cycle {cycle}: first answer {:.0} ms after the wake call",
                    answer.first_answer_ms
                ));
            }
            if answer.huge_share < HUGE_SHARE {
                failed.push(format!(
                    "pod {name} cycle {cycle}: AnonHugePages {:.3} of RSS",
                    answer.huge_share
                ));
            }
            if answer.ipv6_ms > IPV6_PROBE_MS {
                failed.push(format!(
                    "pod {name} cycle {cycle}: the IPv6 probe took {:.1} ms",
                    answer.ipv6_ms
                ));
            }
        }
        if rows.len() < RESUME_CYCLES as usize {
            failed.push(format!(
                "pod {name}: {} of {RESUME_CYCLES} cycles",
                rows.len()
            ));
        }
        let column = |f: fn(&Answer) -> f64| rows.iter().map(|r| f(&r.answer)).collect::<Vec<_>>();
        let _ = writeln!(report, "pod {name}, {} cycles:", rows.len());
        for (what, values, unit) in [
            (
                "wake call to first answer",
                column(|a| a.first_answer_ms),
                "ms",
            ),
            (
                "wake's return to first answer",
                column(|a| a.after_return_ms),
                "ms",
            ),
            ("attempts", column(|a| a.attempts as f64), ""),
            (
                "AnonHugePages / RSS at first answer",
                column(|a| a.huge_share),
                "",
            ),
            ("IPv6 probe", column(|a| a.ipv6_ms), "ms"),
        ] {
            let _ = writeln!(report, "  {what}: {}", summary(&values, unit));
        }
        let steps: Vec<String> = rows
            .first()
            .map(|r| r.steps.iter().map(|(n, _)| n.clone()).collect())
            .unwrap_or_default();
        for step_name in steps {
            let values: Vec<f64> = rows
                .iter()
                .filter_map(|r| r.steps.iter().find(|(n, _)| *n == step_name).map(|s| s.1))
                .collect();
            let _ = writeln!(
                report,
                "  wake step {step_name}: {}",
                summary(&values, "ms")
            );
        }
    }
    step(&format!("resume through the shim:\n{}", report.trim_end()));
    assert!(failed.is_empty(), "{}", failed.join("\n"));
}

/// The lasting processes of the runtime, which must be exactly one front
/// shim and one node service.
fn only_shim_and_service(suite: &Suite) -> Result<runtime::Process, String> {
    let processes = suite.api.processes()?;
    let lasting: Vec<&runtime::Process> = processes.iter().filter(|p| p.role.lasting()).collect();
    let roles: Vec<Role> = lasting.iter().map(|p| p.role).collect();
    let shims = roles.iter().filter(|r| **r == Role::FrontShim).count();
    let services = roles.iter().filter(|r| **r == Role::NodeService).count();
    if shims != 1 || services != 1 || lasting.len() != 2 {
        return Err(format!(
            "expected only the front shim and the node service, found:\n{}",
            runtime::list(&processes)
        ));
    }
    suite.api.only(Role::FrontShim)
}

/// With every pod asleep, the only processes of the runtime are the front
/// shim and the node service, and five more sleeping pods grow the front
/// shim's USS by less than 50 KiB each.
fn no_process_per_pod(suite: &Suite) {
    let processes = expect(suite.api.processes());
    step(&format!(
        "the runtime's processes:\n{}",
        runtime::list(&processes)
    ));
    for name in [ARM64, AMD64, PLAIN, RESUME, RESUME_PLAIN] {
        expect(suite.ensure_asleep(suite.pod(name)));
    }
    let before = expect(only_shim_and_service(suite));
    let idle: Vec<PodSpec> = IDLE
        .iter()
        .map(|&name| PodSpec {
            name,
            arch: Arch::Arm64,
            runtime: Runtime::SmolvmPods,
            app: App::Hold { mib: IDLE_MIB },
            containers: &[("app", PORT)],
            annotations: &[],
        })
        .collect();
    let idle_pods = suite.cluster.run_pods(&idle);
    for pod in &idle_pods {
        let url = format!("http://{}:{PORT}/ping", pod.ip());
        eventually(Duration::from_secs(120), "the idle app", || {
            suite.node().try_script(
                &["exec", "--", &suite.testapp, "get", &url],
                COMMAND_TIMEOUT,
            )
        });
        expect(suite.hibernate(pod));
    }
    let after = expect(only_shim_and_service(suite));
    assert_eq!(after.pid, before.pid, "the front shim's pid");
    let growth = after.uss_kib.saturating_sub(before.uss_kib) / IDLE.len() as u64;
    step(&format!(
        "front shim USS {} KiB with {} pods asleep, {} KiB with {} more: {growth} KiB per pod",
        before.uss_kib,
        5,
        after.uss_kib,
        IDLE.len()
    ));
    for pod in &idle_pods {
        suite.cluster.delete_pod(pod.name);
    }
    assert!(
        growth < USS_PER_POD_KIB,
        "the front shim grows by {growth} KiB per sleeping pod"
    );
}

/// A SIGKILL of the node service with two pods asleep and one awake: after
/// the restart the states are the same, the awake pod's VM process is the
/// same, wakes work, and the awake pod's log repeats at most one frame and
/// loses none. Then, at each fault point that kills the node service, the
/// outcome that recovery gives, with the pod unchanged.
fn node_service_kill(suite: &Suite) {
    let (arm64, amd64, plain) = (suite.pod(ARM64), suite.pod(AMD64), suite.pod(PLAIN));
    expect(suite.ensure_asleep(arm64));
    expect(suite.ensure_asleep(amd64));
    expect(suite.ensure_awake(plain));
    let vm = expect(suite.vm(plain));
    let ticks = suite.last_tick(PLAIN);
    let old = expect(suite.api.kill_node_service());
    expect(
        suite
            .api
            .wait_for_node_service(old, Duration::from_secs(60)),
    );
    for pod in [arm64, amd64] {
        assert_eq!(
            expect(suite.state(pod)),
            PodState::Asleep,
            "pod {}",
            pod.name
        );
        assert!(
            pod.vm_processes().is_empty(),
            "pod {} has a VM process",
            pod.name
        );
    }
    let status = expect(suite.api.get(&plain.sandbox).map_err(|e| e.to_string()));
    assert_eq!(status.state, PodState::Awake, "pod {PLAIN}");
    assert_eq!(status.vm_pid, Some(vm.pid), "pod {PLAIN}'s VM process");
    for pod in [arm64, amd64] {
        expect(suite.wake(pod));
    }
    for name in CYCLED {
        expect(suite.check_unchanged(name));
    }
    expect(suite.check_logs_continue(PLAIN, ticks));
    let replayed = Ticks::parse(&plain.logs("app"));
    expect(replayed.check(1));
    step(&format!("pod {PLAIN} across the restart: {replayed}"));

    let mut failed = Vec::new();
    for point in FaultPoint::KILLING {
        match fault(suite, point) {
            Ok(()) => step(&format!("fault point {}: recovered", point.name())),
            Err(e) => failed.push(format!("fault point {}: {e}", point.name())),
        }
    }
    assert!(failed.is_empty(), "{}", failed.join("\n"));
}

/// Kills the node service at `point` during a hibernate or a wake of the
/// arm64 pod, and checks the outcome after the restart.
fn fault(suite: &Suite, point: FaultPoint) -> Result<(), String> {
    let pod = suite.pod(ARM64);
    let pausing = matches!(point, FaultPoint::PauseSaving | FaultPoint::PauseDurable);
    if pausing {
        suite.ensure_awake(pod)?;
    } else {
        suite.ensure_asleep(pod)?;
    }
    let vm = if pausing { Some(suite.vm(pod)?) } else { None };
    let mark = suite.api.log_mark()?;
    let armed = suite.api.arm_fault(point, None, Duration::from_secs(30))?;
    let call = if pausing {
        suite.api.hibernate(&pod.sandbox).map(drop)
    } else {
        suite.api.wake(&pod.sandbox).map(drop)
    };
    if call.is_ok() {
        return Err("the call succeeded, so the node service did not stop".into());
    }
    suite
        .api
        .wait_for_node_service(armed, Duration::from_secs(60))?;
    suite.api.check_fault_reached(point, mark)?;
    let want = point.recovered_state();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let state = suite.state(pod)?;
        if state == want {
            break;
        }
        if Instant::now() >= deadline {
            return Err(format!("the pod is {state} after the recovery, not {want}"));
        }
        thread::sleep(Duration::from_millis(500));
    }
    match point {
        FaultPoint::PauseSaving => {
            let now = suite.vm(pod)?;
            if Some(&now) != vm.as_ref() {
                return Err(format!("the VM process is {now:?}, was {vm:?}"));
            }
        }
        FaultPoint::PauseDurable => {
            if !pod.vm_processes().is_empty() {
                return Err("the pod is asleep, but a VM process runs".into());
            }
            suite.wake(pod)?;
        }
        FaultPoint::WakeResumed | FaultPoint::WakeClock => {
            let skew = suite.clock_sample(ARM64)?.realtime_skew();
            if skew.abs() > REALTIME_SKEW_NS {
                return Err(format!(
                    "the guest's realtime is {:.1} ms off the node's",
                    skew as f64 / 1e6
                ));
            }
            if !suite.tso_marked(&suite.vm(pod)?)? {
                return Err("TSO is off in the resumed VM process".into());
            }
        }
        FaultPoint::IdleDecided | FaultPoint::WakeResumeError => {
            unreachable!(
                "fault point {} does not kill the node service",
                point.name()
            )
        }
    }
    suite.check_unchanged(ARM64)
}

/// Kills k3s, its server and containerd with SIGKILL. floci-node starts k3s
/// again after 5 s.
const KILL_K3S: &str = r#"pids=$(pgrep -f '^k3s (init|server)'; pgrep -x containerd)
[ -n "$pids" ] || { echo "no k3s processes" >&2; exit 1; }
kill -KILL $pids
echo $pids"#;

/// A containerd restart while the pods sleep. Their tasks reload running with
/// no exits, they stay asleep, and their logs continue after the next wake.
fn containerd_restart(suite: &Suite) {
    for name in CYCLED {
        expect(suite.ensure_asleep(suite.pod(name)));
    }
    let ticks: Vec<u64> = CYCLED.iter().map(|name| suite.last_tick(name)).collect();
    let mark = suite.recorder.mark();
    let old_containerd = suite.node().sh("pgrep -x containerd");
    let killed = suite.node().sh(KILL_K3S);
    step(&format!("killed k3s: {}", killed.trim()));
    eventually(Duration::from_secs(300), "k3s to restart", || {
        let containerd = suite.node().try_sh("pgrep -x containerd")?;
        if containerd == old_containerd {
            return Err("containerd has its old pid".to_string());
        }
        suite
            .node()
            .try_kubectl(&["get", "--raw", "/readyz"], COMMAND_TIMEOUT)
    });
    eventually(Duration::from_secs(120), "the tasks to reload", || {
        suite.check_running(&CYCLED)
    });
    for name in CYCLED {
        let pod = suite.pod(name);
        assert_eq!(expect(suite.state(pod)), PodState::Asleep, "pod {name}");
        assert!(pod.vm_processes().is_empty(), "pod {name} has a VM process");
    }
    let ids = cri_ids(suite, &CYCLED);
    let exits: Vec<String> = suite
        .recorder
        .events()
        .into_iter()
        .filter(|e| e.line > mark && e.is_exit_or_delete() && ids.contains(&e.container_id))
        .map(|e| format!("{} {} {}", e.time, e.topic, e.container_id))
        .collect();
    assert!(
        exits.is_empty(),
        "exits after the restart:\n{}",
        exits.join("\n")
    );
    step(&format!(
        "the recorder's ctr events restarts:\n{}",
        suite.recorder.restarts().trim_end()
    ));
    for (name, ticks) in CYCLED.iter().zip(ticks) {
        expect(suite.wake(suite.pod(name)));
        expect(suite.check_unchanged(name));
        expect(suite.check_logs_continue(name, ticks));
    }
}

/// The CRI ids of the pods' sandboxes and containers.
fn cri_ids(suite: &Suite, names: &[&str]) -> HashSet<String> {
    names
        .iter()
        .flat_map(|&name| {
            let pod = suite.pod(name);
            std::iter::once(pod.sandbox.clone()).chain(suite.baseline[name].containers.clone())
        })
        .collect()
}

/// kubelet's event reasons for a pod, one per line.
#[track_caller]
fn event_reasons(suite: &Suite, name: &str) -> String {
    let selector = format!("involvedObject.name={name}");
    suite.cluster.node.kubectl_ok(&[
        "get",
        "events",
        "-n",
        &suite.cluster.namespace,
        "--field-selector",
        &selector,
        "-o",
        r#"jsonpath={range .items[*]}{.reason}{"\n"}{end}"#,
    ])
}

/// Prints what is left of a deleted pod VM: its VM process, VM directory,
/// checkpoints, smolvm's locks and ready marker, pod share, record and
/// network namespace. The arguments are the sandbox id, the VM directory and
/// the namespace's name.
const LEFTOVERS: &str = r#"sandbox=$1 dir=$2 netns=$3
vms=${dir%/*}
for p in /proc/[0-9]*; do
  case $(tr '\0' ' ' < "$p/cmdline" 2>/dev/null) in
    *" _boot-vm $dir/"*) echo "process ${p#/proc/}" ;;
  esac
done
for f in "$dir" "$dir"/pause-*.smolcheckpoint "$vms/.$sandbox.fork-operation.lock" \
    "$vms/.$sandbox.fork-operation.pause-operation.lock" \
    "$SMOLVM_AGENT_ROOTFS/.smolvm-ready.${dir##*/}" \
    "/var/lib/containerd-shim-smolvm-pods/$sandbox" "$RECORDS/$sandbox.json" \
    ${netns:+"/var/run/netns/$netns"}; do
  if [ -e "$f" ]; then echo "file $f"; fi
done"#;

/// What is left of a deleted pod VM, one item per line.
fn leftovers(suite: &Suite, sandbox: &str, dir: &str, netns: &str) -> Result<String, String> {
    let script = format!("RECORDS={}\n{LEFTOVERS}", runtime::RECORDS);
    let output = suite.node().exec(
        &["sh", "-c", &script, "sh", sandbox, dir, netns],
        COMMAND_TIMEOUT,
    );
    if output.success() {
        Ok(output.stdout.trim().to_string())
    } else {
        Err(describe(&output))
    }
}

/// The name of the pod's network namespace.
#[track_caller]
fn netns_of(suite: &Suite, pod: &Pod) -> String {
    let path = suite.node().sh(&format!(
        "crictl inspectp {} | jq -r '.info.runtimeSpec.linux.namespaces[] | \
         select(.type == \"network\") | .path'",
        pod.sandbox
    ));
    path.trim()
        .rsplit('/')
        .next()
        .unwrap_or_default()
        .to_string()
}

/// Deleting a sleeping pod does not wake it, and removes its smolvm machine,
/// checkpoint, pod share, podbr0 and record, and gives back the disk space.
fn delete_while_asleep(suite: &Suite) {
    let pod = suite.pod(PLAIN);
    expect(suite.ensure_awake(pod));
    let used_awake = expect(suite.api.disk_used());
    expect(suite.hibernate(pod));
    let used_asleep = expect(suite.api.disk_used());
    let dir = suite.baseline[PLAIN].vm_dir.clone();
    let netns = netns_of(suite, pod);
    let before = expect(leftovers(suite, &pod.sandbox, &dir, &netns));
    for expected in [
        format!("file {dir}/pause-"),
        format!("file {}/{}.json", runtime::RECORDS, pod.sandbox),
        format!("file /var/lib/containerd-shim-smolvm-pods/{}", pod.sandbox),
    ] {
        assert!(
            before.contains(&expected),
            "the sleeping pod should have {expected:?}:\n{before}"
        );
    }
    let links = suite.node().sh(&format!("ip -n {netns} -o link show"));
    assert!(
        links.contains(": podbr0:"),
        "no podbr0 while asleep:\n{links}"
    );
    let holder = NetnsHolder::start(suite.cluster, &netns);
    let cursor = expect(suite.api.cursor().map_err(|e| e.to_string()));
    {
        let mut records = suite.records.borrow_mut();
        records.deleted_reasons = Some(event_reasons(suite, PLAIN));
        records.delete_mark = Some(suite.recorder.mark());
        records.deleted = cri_ids(suite, &[PLAIN]).into_iter().collect();
    }
    suite.cluster.delete_pod(PLAIN);
    let result = (|| -> Result<(), String> {
        let wakes = suite
            .events_since(cursor, pod)
            .into_iter()
            .filter(|e| e.is_resume())
            .count();
        if wakes != 0 {
            return Err(format!("the delete woke the pod {wakes} times"));
        }
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let left = leftovers(suite, &pod.sandbox, &dir, "")?;
            if left.is_empty() {
                break;
            }
            if Instant::now() >= deadline {
                return Err(format!("left in the node:\n{left}"));
            }
            thread::sleep(Duration::from_secs(1));
        }
        let links = holder.links()?;
        if links.contains(": podbr0:") || links.contains(": tap0:") {
            return Err(format!("the held namespace keeps the bridge:\n{links}"));
        }
        match suite.api.get(&pod.sandbox) {
            Err(e) if e.code == "not_found" => {}
            other => return Err(format!("ctl get of the deleted pod: {other:?}")),
        }
        let used = suite.api.disk_used()?;
        step(&format!(
            "disk used: {} MiB awake, {} MiB asleep, {} MiB after the delete",
            used_awake >> 20,
            used_asleep >> 20,
            used >> 20
        ));
        if used > used_awake + (32 << 20) {
            return Err(format!(
                "the delete gave back the disk space only down to {} MiB, from {} MiB awake",
                used >> 20,
                used_awake >> 20
            ));
        }
        Ok(())
    })();
    drop(holder);
    expect(result);
    let gone = suite.node().sh(&format!(
        "if [ -e /var/run/netns/{netns} ]; then echo left; fi"
    ));
    assert_eq!(
        gone.trim(),
        "",
        "the network namespace {netns} after the delete"
    );
}

/// The recorder saw no task exit or delete for the test's pods, other than
/// the delete of the sleeping pod, and kubelet logged no SandboxChanged or
/// Killing for them. The recorder ran: it saw each container start.
fn no_false_exits(suite: &Suite) {
    expect(suite.recorder.check_alive());
    let events = suite.recorder.events();
    let records = suite.records.borrow();
    let all = [ARM64, AMD64, PLAIN, RESUME, RESUME_PLAIN];
    let ids = cri_ids(suite, &all);
    for name in all {
        for id in &suite.baseline[name].containers {
            assert!(
                events
                    .iter()
                    .any(|e| e.topic == "/tasks/start" && &e.container_id == id),
                "the recorder did not see container {id} of pod {name} start"
            );
        }
    }
    let deleted = |e: &smolvm_pods_e2e::recorder::TaskEvent| {
        records.delete_mark.is_some_and(|mark| e.line > mark)
            && records.deleted.contains(&e.container_id)
    };
    let exits: Vec<String> = events
        .iter()
        .filter(|e| e.is_exit_or_delete() && ids.contains(&e.container_id) && !deleted(e))
        .map(|e| {
            format!(
                "{} {} {} {:?}",
                e.time, e.topic, e.container_id, e.exit_status
            )
        })
        .collect();
    let mut kubelet = Vec::new();
    for name in all {
        let reasons = match (name, &records.deleted_reasons) {
            (PLAIN, Some(reasons)) => reasons.clone(),
            _ => event_reasons(suite, name),
        };
        for reason in reasons.lines() {
            if matches!(reason, "SandboxChanged" | "Killing") {
                kubelet.push(format!("pod {name}: {reason}"));
            }
        }
    }
    assert!(
        exits.is_empty() && kubelet.is_empty(),
        "false exits:\n{}\nkubelet events:\n{}",
        exits.join("\n"),
        kubelet.join("\n")
    );
    step(&format!("{} events recorded, no false exit", events.len()));
    drop(records);
    require_cycles(suite);
}

/// The node's khugepaged sleeps are set, the node VM did not restart, and the
/// guest agent has the same threads and descriptors after cycle 20 as after
/// cycle 1. Each count is the settled reading of several samples (see
/// `Suite::record_agent`).
fn node_health(suite: &Suite) {
    let khugepaged = suite
        .node()
        .sh("cd /sys/kernel/mm/transparent_hugepage/khugepaged && \
         cat scan_sleep_millisecs alloc_sleep_millisecs");
    let sleeps: Vec<&str> = khugepaged.split_whitespace().collect();
    assert_eq!(
        sleeps,
        [runtime::KHUGEPAGED_SLEEP_MS; 2],
        "khugepaged scan_sleep_millisecs and alloc_sleep_millisecs"
    );
    assert_eq!(
        suite.cluster.node_kernel().boot_id,
        suite.node_boot_id,
        "the node's boot id: the node VM restarted"
    );
    require_cycles(suite);
    let records = suite.records.borrow();
    let mut failed = Vec::new();
    for name in CYCLED {
        let agent = records
            .agent
            .get(name)
            .map(Vec::as_slice)
            .unwrap_or_default();
        match agent {
            [(1, Ok(first)), (CYCLES, Ok(last))] => {
                step(&format!("pod {name}: guest agent {first:?}, then {last:?}"));
                if (first.threads, first.fds) != (last.threads, last.fds) {
                    failed.push(format!(
                        "pod {name}: the guest agent had {first:?} after cycle 1, {last:?} after \
                         cycle {CYCLES}"
                    ));
                }
            }
            other => failed.push(format!("pod {name}: guest agent stats {other:?}")),
        }
    }
    assert!(failed.is_empty(), "{}", failed.join("\n"));
}

/// A SIGKILL of the front shim, the one failure that pods do not survive:
/// kubelet recreates every pod; no VM, checkpoint, record or bridge of the
/// old pods is left; the new pods run.
fn front_shim_kill(suite: &Suite) {
    let names = [ARM64, AMD64, RESUME, RESUME_PLAIN];
    expect(suite.ensure_asleep(suite.pod(ARM64)));
    expect(suite.ensure_asleep(suite.pod(RESUME)));
    expect(suite.ensure_awake(suite.pod(AMD64)));
    expect(suite.ensure_awake(suite.pod(RESUME_PLAIN)));
    let old: Vec<_> = names
        .iter()
        .map(|&name| {
            let pod = suite.pod(name);
            (
                name,
                pod.sandbox.clone(),
                suite.baseline[name].vm_dir.clone(),
                netns_of(suite, pod),
            )
        })
        .collect();
    let shim = expect(suite.api.only(Role::FrontShim));
    expect(suite.node().try_sh(&format!("kill -KILL {}", shim.pid)));
    step(&format!("killed the front shim, pid {}", shim.pid));
    for (name, sandbox, _, _) in &old {
        eventually(
            Duration::from_secs(300),
            "kubelet to recreate the pod",
            || match suite.cluster.ready_sandbox(name)? {
                new if new != *sandbox => Ok(()),
                _ => Err(format!("pod {name} still has sandbox {sandbox}")),
            },
        );
    }
    for (name, sandbox, dir, netns) in &old {
        eventually(
            Duration::from_secs(120),
            "the old pod to leave nothing",
            || match leftovers(suite, sandbox, dir, netns)?.as_str() {
                "" => Ok(()),
                left => Err(format!("pod {name} left:\n{left}")),
            },
        );
    }
    let pods = expect(suite.api.list().map_err(|e| e.to_string()));
    let kept: Vec<&str> = pods
        .iter()
        .map(|p| p.sandbox_id.as_str())
        .filter(|id| old.iter().any(|(_, sandbox, _, _)| sandbox == id))
        .collect();
    assert!(
        kept.is_empty(),
        "the node service keeps records of {kept:?}"
    );
    for (name, _, _, _) in &old {
        let pod = suite.cluster.ready_pod(name);
        let url = format!("http://{}:{PORT}/id", pod.ip());
        let id = eventually(Duration::from_secs(180), "the new pod's app", || {
            suite.node().try_script(
                &["exec", "--", &suite.testapp, "get", &url],
                COMMAND_TIMEOUT,
            )
        });
        let boot_id = State::parse(&id)["boot_id"].to_string();
        assert_ne!(
            boot_id, suite.baseline[name].identity.boot_id,
            "pod {name} has its old kernel"
        );
        step(&format!("pod {name} runs again in sandbox {}", pod.sandbox));
    }
}

#[test]
fn writes_the_pods() {
    let pods = specs();
    let names: Vec<&str> = pods.iter().map(|s| s.name).collect();
    assert_eq!(names, [ARM64, AMD64, PLAIN, RESUME, RESUME_PLAIN, RUNC]);
    let manifest = pods[1].manifest("ns", "docker.io/test:1");
    assert!(
        manifest.contains(r#"annotations: {"smolvm-pods.floci.io/idle-timeout": "24h"}"#),
        "{manifest}"
    );
    assert!(manifest.contains(r#"args: [hold, -mib, "256", app, ":8080"]"#));
    let manifest = pods[2].manifest("ns", "docker.io/test:1");
    assert!(
        manifest.contains(
            r#"annotations: {"smolvm-pods.floci.io/rosetta": "false", "smolvm-pods.floci.io/idle-timeout": "24h"}"#
        ),
        "{manifest}"
    );
}
