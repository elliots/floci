//! The tests' adapter to the runtime in the node: the `smolvm-pods ctl`
//! commands and the JSON they print, the fault points, the node service's
//! files and log, and how the tests tell the runtime's processes apart.
//! tests/e2e/README.md ("Runtime contract") documents the same contract for
//! the runtime's side; a change here must change it too.

use std::cell::RefCell;
use std::fmt;
use std::thread;
use std::time::{Duration, Instant};

use crate::json::Json;
use crate::node::Node;
use crate::step;

/// The command that the tests run in the node for every API call.
pub const CTL: [&str; 2] = ["smolvm-pods", "ctl"];

/// The runtime's binary in the node. Every process of the runtime runs it:
/// `smolvm-pods` is a link to it.
pub const BINARY: &str = "/usr/local/bin/containerd-shim-smolvm-pods-v2";

/// floci-node runs the node service with `SMOLVM_PODS_FAULT` set to the
/// contents of this file when it exists, and removes it.
pub const FAULT_FILE: &str = "/run/smolvm-pods/fault";

/// The node service's stderr, which floci-node appends to this file.
pub const NODE_SERVICE_LOG: &str = "/var/log/smolvm-pods/node-service.log";

/// The node service's durable record of each pod: `RECORDS/SANDBOX.json`.
pub const RECORDS: &str = "/var/lib/containerd-shim-smolvm-pods/.node/pods";

/// The file beside a VM's boot config in which the VM process records its pid
/// and start time when it turns TSO on (the runtime's `src/boot.rs`).
pub const TSO_MARKER: &str = "smolvm-pods-tso";

/// What floci-node writes to khugepaged's scan and alloc sleeps on a Mac node.
pub const KHUGEPAGED_SLEEP_MS: &str = "86400000";

/// The exit statuses of ctl: the API answered with an error, or the API
/// could not be reached.
pub const EXIT_ERROR: i32 = 1;
pub const EXIT_UNREACHABLE: i32 = 3;

/// The `--source` and `--holder` of the tests' calls.
pub const SOURCE: &str = "e2e";

/// How long floci-node may take to start the next node service after a
/// SIGKILL, until its API answers.
pub const NODE_SERVICE_RESTART: Duration = Duration::from_secs(60);

/// The node's smolvm volume, which holds the VMs and their checkpoints.
const SMOLVM_VOLUME: &str = "/var/lib/smolvm";

/// How long a wake or a hibernate may take, and any other call.
const SLOW_CALL: Duration = Duration::from_secs(120);
const CALL: Duration = Duration::from_secs(30);

/// A point at which the node service acts when `SMOLVM_PODS_FAULT` names it.
/// At the [`FaultPoint::KILLING`] points it kills itself with SIGKILL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultPoint {
    /// The record says `pausing` and smolvm's pause is saving: the VM is
    /// frozen and the checkpoint is not durable.
    PauseSaving,
    /// The pause is durable (smolvm's record says Paused), before the node
    /// service checked that the VM process exited and wrote `asleep`.
    PauseDurable,
    /// The resume returned and the new VM process runs, before the TSO check,
    /// the agent ping and the clock step.
    WakeResumed,
    /// The agent answered SetClock, before the deferred kills and `awake`.
    WakeClock,
    /// A pause for being idle, after the idle decision and before the
    /// `pausing` hook holds the pod's traffic: the node service waits 5 s,
    /// once, so that a request can arrive before the hold.
    IdleDecided,
    /// Every resume fails before the VM starts and leaves the checkpoint
    /// intact, as a checkpoint that cannot be restored makes it, until the
    /// node service restarts.
    WakeResumeError,
}

impl FaultPoint {
    /// The points that kill the node service.
    pub const KILLING: [FaultPoint; 4] = [
        FaultPoint::PauseSaving,
        FaultPoint::PauseDurable,
        FaultPoint::WakeResumed,
        FaultPoint::WakeClock,
    ];

    pub fn name(self) -> &'static str {
        match self {
            FaultPoint::PauseSaving => "pause:saving",
            FaultPoint::PauseDurable => "pause:durable",
            FaultPoint::WakeResumed => "wake:resumed",
            FaultPoint::WakeClock => "wake:clock",
            FaultPoint::IdleDecided => "idle:decided",
            FaultPoint::WakeResumeError => "wake:resume-error",
        }
    }

    /// The line of the fault file that arms the point, for every pod or for
    /// one pod sandbox.
    pub fn armed_for(self, sandbox: Option<&str>) -> String {
        match sandbox {
            Some(sandbox) => format!("{}@{sandbox}", self.name()),
            None => self.name().to_string(),
        }
    }

    /// The line that the node service logs when it reaches the point during a
    /// transition of the pod sandbox `sandbox`; with no sandbox, the start of
    /// the line, which every firing logs.
    pub fn log_line(self, sandbox: Option<&str>) -> String {
        let reached = format!("fault point {} reached", self.name());
        match sandbox {
            Some(sandbox) => format!("{reached} for pod sandbox {sandbox}"),
            None => reached,
        }
    }

    /// The state that recovery leaves the pod in after a killing point.
    pub fn recovered_state(self) -> PodState {
        match self {
            FaultPoint::PauseDurable => PodState::Asleep,
            _ => PodState::Awake,
        }
    }
}

/// A pod's state in the node service.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PodState {
    Creating,
    Awake,
    Pausing,
    Asleep,
    Waking,
    Failed,
    Deleting,
}

impl PodState {
    pub fn parse(name: &str) -> Option<PodState> {
        Some(match name {
            "creating" => PodState::Creating,
            "awake" => PodState::Awake,
            "pausing" => PodState::Pausing,
            "asleep" => PodState::Asleep,
            "waking" => PodState::Waking,
            "failed" => PodState::Failed,
            "deleting" => PodState::Deleting,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            PodState::Creating => "creating",
            PodState::Awake => "awake",
            PodState::Pausing => "pausing",
            PodState::Asleep => "asleep",
            PodState::Waking => "waking",
            PodState::Failed => "failed",
            PodState::Deleting => "deleting",
        }
    }
}

impl fmt::Display for PodState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A pod as `ctl get` and `ctl list` describe it.
#[derive(Clone, Debug)]
pub struct PodStatus {
    pub sandbox_id: String,
    pub state: PodState,
    pub generation: u64,
    /// The VM process, while one runs.
    pub vm_pid: Option<u32>,
    pub rosetta: Option<bool>,
    pub leases: Vec<Lease>,
    /// The pod's labels, as the node service read them from the API server.
    pub labels: Vec<(String, String)>,
    /// The kind and name of the workload that controls the pod.
    pub controller: Option<(String, String)>,
    pub raw: Json,
}

impl PodStatus {
    pub fn parse(json: &Json) -> Result<PodStatus, String> {
        let field = |name: &str| format!("pod field {name:?} in {json}");
        let state = json.get("state").as_str().ok_or_else(|| field("state"))?;
        let labels = match json.get("labels") {
            Json::Null => Vec::new(),
            Json::Object(members) => members
                .iter()
                .map(|(key, value)| {
                    let value = value.as_str().ok_or_else(|| field("labels"))?;
                    Ok((key.clone(), value.to_string()))
                })
                .collect::<Result<_, String>>()?,
            _ => return Err(field("labels")),
        };
        let controller = match json.get("controller") {
            Json::Null => None,
            controller => {
                let text = |name| {
                    controller
                        .get(name)
                        .as_str()
                        .map(str::to_string)
                        .ok_or_else(|| field("controller"))
                };
                Some((text("kind")?, text("name")?))
            }
        };
        let leases = match json.get("leases") {
            Json::Null => Vec::new(),
            leases => leases
                .as_array()
                .ok_or_else(|| field("leases"))?
                .iter()
                .map(Lease::parse)
                .collect::<Result<_, _>>()?,
        };
        Ok(PodStatus {
            sandbox_id: json
                .get("sandbox_id")
                .as_str()
                .ok_or_else(|| field("sandbox_id"))?
                .to_string(),
            state: PodState::parse(state).ok_or_else(|| field("state"))?,
            generation: json
                .get("generation")
                .as_u64()
                .ok_or_else(|| field("generation"))?,
            vm_pid: match json.get("vm_pid") {
                Json::Null => None,
                pid => Some(
                    pid.as_u64()
                        .and_then(|pid| u32::try_from(pid).ok())
                        .ok_or_else(|| field("vm_pid"))?,
                ),
            },
            rosetta: json.get("rosetta").as_bool(),
            leases,
            labels,
            controller,
            raw: json.clone(),
        })
    }

    /// Whether a lease of `kind` is held.
    pub fn has_lease(&self, kind: &str) -> bool {
        self.leases.iter().any(|lease| lease.kind == kind)
    }
}

/// A lease on a pod: `api` from hold, or an implicit one: `exec`, `create`
/// or `waiter`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    pub id: String,
    pub kind: String,
}

impl Lease {
    fn parse(json: &Json) -> Result<Lease, String> {
        let text = |name: &str| {
            json.get(name)
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| format!("lease field {name:?} in {json}"))
        };
        Ok(Lease {
            id: text("id")?,
            kind: text("kind")?,
        })
    }
}

/// The guest clock step of a wake: the guest's realtime minus the node's,
/// before the step, and whether the agent stepped it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClockStep {
    pub skew_ns: i64,
    pub stepped: bool,
}

impl ClockStep {
    fn parse(json: &Json) -> Result<Option<ClockStep>, String> {
        if json.is_null() {
            return Ok(None);
        }
        let skew_ns = json.get("skew_ns").as_i64();
        let stepped = json.get("stepped").as_bool();
        match (skew_ns, stepped) {
            (Some(skew_ns), Some(stepped)) => Ok(Some(ClockStep { skew_ns, stepped })),
            _ => Err(format!("clock step without skew_ns and stepped: {json}")),
        }
    }
}

/// Durations by name, in ms, such as the steps of a wake.
fn durations(json: &Json) -> Result<Vec<(String, f64)>, String> {
    match json {
        Json::Null => Ok(Vec::new()),
        Json::Object(members) => members
            .iter()
            .map(|(name, ms)| {
                ms.as_f64()
                    .map(|ms| (name.clone(), ms))
                    .ok_or_else(|| format!("duration {name:?} is not a number: {json}"))
            })
            .collect(),
        _ => Err(format!("durations are not an object: {json}")),
    }
}

/// What `ctl wake` returns once the pod is awake.
#[derive(Clone, Debug)]
pub struct Wake {
    pub pod: PodStatus,
    /// Whether this call ran the resume, rather than finding the pod awake or
    /// joining another call's resume.
    pub resumed: bool,
    pub duration_ms: f64,
    pub steps_ms: Vec<(String, f64)>,
    pub clock: Option<ClockStep>,
}

impl Wake {
    pub fn parse(json: &Json) -> Result<Wake, String> {
        Ok(Wake {
            pod: PodStatus::parse(json.get("pod"))?,
            resumed: json
                .get("resumed")
                .as_bool()
                .ok_or_else(|| format!("wake without resumed: {json}"))?,
            duration_ms: json
                .get("duration_ms")
                .as_f64()
                .ok_or_else(|| format!("wake without duration_ms: {json}"))?,
            steps_ms: durations(json.get("steps_ms"))?,
            clock: ClockStep::parse(json.get("clock"))?,
        })
    }
}

/// What `ctl hibernate` returns once the pod is asleep.
#[derive(Clone, Debug)]
pub struct Hibernation {
    pub pod: PodStatus,
    pub duration_ms: f64,
    pub checkpoint_bytes: Option<u64>,
}

impl Hibernation {
    pub fn parse(json: &Json) -> Result<Hibernation, String> {
        Ok(Hibernation {
            pod: PodStatus::parse(json.get("pod"))?,
            duration_ms: json
                .get("duration_ms")
                .as_f64()
                .ok_or_else(|| format!("hibernation without duration_ms: {json}"))?,
            checkpoint_bytes: json.get("checkpoint_bytes").as_u64(),
        })
    }
}

/// One state change, as `ctl events` prints it.
#[derive(Clone, Debug)]
pub struct Event {
    pub seq: u64,
    pub pod: String,
    pub from: String,
    pub to: String,
    pub reason: String,
    pub source: String,
    /// On `pausing` to `awake`: a wake cancelled the pause.
    pub cancelled: bool,
    pub error: Option<String>,
    pub steps_ms: Vec<(String, f64)>,
    pub clock: Option<ClockStep>,
    pub raw: Json,
}

impl Event {
    pub fn parse(json: &Json) -> Result<Event, String> {
        let text = |name: &str| {
            json.get(name)
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| format!("event field {name:?} in {json}"))
        };
        Ok(Event {
            seq: json
                .get("seq")
                .as_u64()
                .ok_or_else(|| format!("event field \"seq\" in {json}"))?,
            pod: text("pod")?,
            from: text("from")?,
            to: text("to")?,
            reason: text("reason")?,
            source: json.get("source").as_str().unwrap_or_default().to_string(),
            cancelled: json.get("cancelled").as_bool().unwrap_or(false),
            error: json.get("error").as_str().map(str::to_string),
            steps_ms: durations(json.get("steps_ms"))?,
            clock: ClockStep::parse(json.get("clock"))?,
            raw: json.clone(),
        })
    }

    /// Whether this event starts a resume.
    pub fn is_resume(&self) -> bool {
        self.to == "waking"
    }
}

/// The guest agent's process in a pod's VM, counted in the guest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AgentStats {
    pub pid: u64,
    pub threads: u64,
    pub fds: u64,
}

impl AgentStats {
    pub fn parse(json: &Json) -> Result<AgentStats, String> {
        let number = |name: &str| {
            json.get(name)
                .as_u64()
                .ok_or_else(|| format!("agent field {name:?} in {json}"))
        };
        Ok(AgentStats {
            pid: number("pid")?,
            threads: number("threads")?,
            fds: number("fds")?,
        })
    }
}

/// How a ctl call failed.
#[derive(Clone, Debug)]
pub struct CtlError {
    /// The exit status, if ctl ran to its end.
    pub exit: Option<i32>,
    /// The error code from the API, such as `refused`, or empty.
    pub code: String,
    pub message: String,
    /// Why a hibernate was refused, such as `exec running`.
    pub reasons: Vec<String>,
}

impl CtlError {
    fn other(message: String) -> CtlError {
        CtlError {
            exit: None,
            code: String::new(),
            message,
            reasons: Vec::new(),
        }
    }

    /// Whether the node service could not be reached.
    pub fn unreachable(&self) -> bool {
        self.exit == Some(EXIT_UNREACHABLE)
    }
}

impl fmt::Display for CtlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)?;
        if !self.code.is_empty() {
            write!(f, " (code {}", self.code)?;
            if !self.reasons.is_empty() {
                write!(f, ", reasons {:?}", self.reasons)?;
            }
            write!(f, ")")?;
        }
        Ok(())
    }
}

/// Reads the outcome of a ctl call from its exit status and output.
pub fn parse_outcome(
    args: &[&str],
    exit: Option<i32>,
    stdout: &str,
    stderr: &str,
) -> Result<Json, CtlError> {
    let call = format!("{} {}", CTL.join(" "), args.join(" "));
    let described = || format!("{call} exited with {exit:?}: stdout {stdout:?}, stderr {stderr:?}");
    match exit {
        Some(0) => Json::parse(stdout)
            .map_err(|e| CtlError::other(format!("{call}: {e}: stdout {stdout:?}"))),
        Some(EXIT_ERROR) => {
            let error = Json::parse(stdout).map(|json| json.get("error").clone());
            match error {
                Ok(error @ Json::Object(_)) => Err(CtlError {
                    exit,
                    code: error.get("code").as_str().unwrap_or_default().to_string(),
                    message: format!(
                        "{call}: {}",
                        error.get("message").as_str().unwrap_or_default()
                    ),
                    reasons: error
                        .get("reasons")
                        .as_array()
                        .unwrap_or_default()
                        .iter()
                        .filter_map(|r| r.as_str().map(str::to_string))
                        .collect(),
                }),
                _ => Err(CtlError {
                    exit,
                    ..CtlError::other(format!("{} with no JSON error", described()))
                }),
            }
        }
        _ => Err(CtlError {
            exit,
            ..CtlError::other(described())
        }),
    }
}

/// Quotes `arg` for sh.
pub fn quote(arg: &str) -> String {
    format!("'{}'", arg.replace('\'', r"'\''"))
}

/// The shell command of a ctl call, for scripts that run several at once.
pub fn ctl_command(args: &[&str]) -> String {
    CTL.iter()
        .map(|a| a.to_string())
        .chain(args.iter().map(|a| quote(a)))
        .collect::<Vec<_>>()
        .join(" ")
}

/// The arguments of `ctl wake POD`, as the tests call it.
pub fn wake_args(pod: &str) -> Vec<&str> {
    vec!["wake", pod, "--reason", "api", "--source", SOURCE]
}

/// The arguments of `ctl hibernate POD`, as the tests call it.
pub fn hibernate_args(pod: &str) -> Vec<&str> {
    vec!["hibernate", pod, "--reason", "api"]
}

/// What a runtime process is, from its arguments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// The front shim: `containerd-shim-smolvm-pods-v2 -namespace NS -id ID
    /// -address ADDRESS -socket SOCKET [-debug]`.
    FrontShim,
    /// `smolvm-pods node-service`, which floci-node runs.
    NodeService,
    /// A VM process: `EXE _boot-vm DIR/boot-config.json`.
    Vm,
    /// A ctl call, which ends.
    Ctl,
    /// The shim's `start` or `delete` action, which ends.
    ShimAction,
    /// Anything else, such as the `ip` and `tc` stand-ins, which end.
    Other,
}

impl Role {
    pub fn of(args: &[&str]) -> Role {
        match args.get(1).copied() {
            Some("_boot-vm") => Role::Vm,
            Some("node-service") => Role::NodeService,
            Some("ctl") => Role::Ctl,
            _ if matches!(args.last().copied(), Some("start" | "delete")) => Role::ShimAction,
            _ if args
                .first()
                .is_some_and(|a| a.ends_with("containerd-shim-smolvm-pods-v2"))
                && args.contains(&"-id") =>
            {
                Role::FrontShim
            }
            _ => Role::Other,
        }
    }

    /// Whether processes of this role live on between calls.
    pub fn lasting(self) -> bool {
        matches!(self, Role::FrontShim | Role::NodeService | Role::Vm)
    }
}

/// A process of the runtime's binary in the node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Process {
    pub pid: u32,
    pub role: Role,
    /// Unique set size: private clean and dirty memory.
    pub uss_kib: u64,
    pub rss_kib: u64,
    pub threads: u64,
    pub args: Vec<String>,
}

/// Prints `PID USS_KIB RSS_KIB THREADS ARG...` for each process whose
/// executable is `$1`, the arguments separated by tabs.
const PROCESSES: &str = r#"for p in /proc/[0-9]*; do
  [ "$(readlink "$p/exe" 2>/dev/null)" = "$1" ] || continue
  set -- "$1" $(awk '/^(Private_Clean|Private_Dirty|Rss):/ { if ($1 == "Rss:") r += $2; else u += $2 }
    END { print u + 0, r + 0 }' "$p/smaps_rollup" 2>/dev/null) \
    $(awk '/^Threads:/ { print $2 }' "$p/status" 2>/dev/null)
  [ $# -eq 4 ] || { set -- "$1"; continue; }
  printf '%s %s %s %s %s\n' "${p#/proc/}" "$2" "$3" "$4" "$(tr '\0' '\t' < "$p/cmdline")"
  set -- "$1"
done"#;

impl Process {
    /// Parses a line of [`PROCESSES`].
    pub fn parse(line: &str) -> Option<Process> {
        let mut fields = line.splitn(5, ' ');
        let pid = fields.next()?.parse().ok()?;
        let uss_kib = fields.next()?.parse().ok()?;
        let rss_kib = fields.next()?.parse().ok()?;
        let threads = fields.next()?.parse().ok()?;
        let args: Vec<String> = fields
            .next()?
            .trim_end_matches(['\t', '\n'])
            .split('\t')
            .map(str::to_string)
            .collect();
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        Some(Process {
            pid,
            role: Role::of(&refs),
            uss_kib,
            rss_kib,
            threads,
            args,
        })
    }
}

impl fmt::Display for Process {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {:?} USS {} KiB RSS {} KiB {} threads: {}",
            self.pid,
            self.role,
            self.uss_kib,
            self.rss_kib,
            self.threads,
            self.args.join(" ")
        )
    }
}

/// Runs `$3...`, a ctl call, with the limit of `$2` seconds, and prints a
/// line `ctl START END STATUS` and then its stdout. START and END are the
/// node's monotonic clock before and after it, read with `$1 now`.
const TIMED: &str = r#"clock=$1 limit=$2
shift 2
now() { "$clock" now 2>/dev/null | sed -n 's/^monotonic_ns=//p'; }
start=$(now)
out=$(timeout "$limit" "$@")
status=$?
end=$(now)
printf 'ctl %s %s %s\n%s\n' "${start:-0}" "${end:-0}" "$status" "$out"
"#;

/// When a call ran, on the node's monotonic clock, in ns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Window {
    pub start_ns: i64,
    pub end_ns: i64,
}

/// A finished ctl call.
struct Ran {
    exit: Option<i32>,
    stdout: String,
    stderr: String,
    window: Window,
}

/// What [`Api::wake_during_pause`] saw.
pub struct PauseAndWake {
    /// The pod's state when the wait for `pausing` ended.
    pub state_before_wake: String,
    pub hibernate: Result<Json, CtlError>,
    pub wake: Result<Json, CtlError>,
}

/// The runtime in one node, as the tests drive it.
pub struct Api<'a> {
    node: &'a Node,
    /// A program in the node whose `now` prints `monotonic_ns=N`, which
    /// times the calls.
    clock: String,
    /// Why ctl cannot be used, once the first call found that out, so that
    /// later calls fail at once.
    unavailable: RefCell<Option<String>>,
}

impl<'a> Api<'a> {
    /// Waits up to `timeout` for the API to answer `ctl list`. If ctl fails
    /// in another way, or the API does not answer in time, every later call
    /// fails at once with that reason. `clock` is the program that times the
    /// calls (`testapp now`).
    pub fn connect(node: &'a Node, clock: &str, timeout: Duration) -> Api<'a> {
        let api = Api {
            node,
            clock: clock.to_string(),
            unavailable: RefCell::new(None),
        };
        let deadline = Instant::now() + timeout;
        loop {
            match api.call(&["list"], CALL) {
                Ok(_) => {
                    step("smolvm-pods ctl answers");
                    break;
                }
                Err(e) if e.unreachable() && Instant::now() < deadline => {
                    thread::sleep(Duration::from_secs(1))
                }
                Err(e) => {
                    step(&format!("smolvm-pods ctl is not available: {e}"));
                    *api.unavailable.borrow_mut() = Some(e.to_string());
                    break;
                }
            }
        }
        api
    }

    pub fn node(&self) -> &'a Node {
        self.node
    }

    fn run(&self, args: &[&str], timeout: Duration) -> Result<Ran, CtlError> {
        if let Some(reason) = self.unavailable.borrow().as_ref() {
            return Err(CtlError::other(format!(
                "smolvm-pods ctl is not available: {reason}"
            )));
        }
        let limit = timeout.as_secs().to_string();
        let script = [
            &["exec", "--", "sh", "-c", TIMED, "sh", &self.clock, &limit][..],
            &CTL[..],
            args,
        ]
        .concat();
        let output = crate::cmd::run(&mut self.node.command(&script), timeout + CALL)
            .map_err(CtlError::other)?;
        let (head, stdout) = output
            .stdout
            .split_once('\n')
            .unwrap_or((&output.stdout, ""));
        let fields: Vec<i64> = head
            .strip_prefix("ctl ")
            .unwrap_or_default()
            .split(' ')
            .filter_map(|f| f.parse().ok())
            .collect();
        let [start_ns, end_ns, status] = fields[..] else {
            return Err(CtlError::other(format!(
                "{} did not run: {}",
                ctl_command(args),
                crate::describe(&output)
            )));
        };
        if status == 124 {
            return Err(CtlError::other(format!(
                "{} did not finish within {timeout:?}",
                ctl_command(args)
            )));
        }
        Ok(Ran {
            exit: i32::try_from(status).ok(),
            stdout: stdout.to_string(),
            stderr: output.stderr,
            window: Window { start_ns, end_ns },
        })
    }

    /// Runs `ctl ARGS` in the node and returns what it printed as JSON.
    pub fn call(&self, args: &[&str], timeout: Duration) -> Result<Json, CtlError> {
        let ran = self.run(args, timeout)?;
        parse_outcome(args, ran.exit, &ran.stdout, &ran.stderr)
    }

    /// Like [`Api::call`], with when the call ran.
    fn timed(&self, args: &[&str], timeout: Duration) -> Result<(Json, Window), CtlError> {
        let ran = self.run(args, timeout)?;
        let json = parse_outcome(args, ran.exit, &ran.stdout, &ran.stderr)?;
        Ok((json, ran.window))
    }

    pub fn get(&self, pod: &str) -> Result<PodStatus, CtlError> {
        let json = self.call(&["get", pod], CALL)?;
        PodStatus::parse(&json).map_err(CtlError::other)
    }

    /// The pod's state.
    pub fn state(&self, pod: &str) -> Result<PodState, String> {
        self.get(pod)
            .map(|status| status.state)
            .map_err(|e| e.to_string())
    }

    pub fn list(&self) -> Result<Vec<PodStatus>, CtlError> {
        let json = self.call(&["list"], CALL)?;
        json.get("pods")
            .as_array()
            .ok_or_else(|| CtlError::other(format!("ctl list printed no pods: {json}")))?
            .iter()
            .map(|pod| PodStatus::parse(pod).map_err(CtlError::other))
            .collect()
    }

    /// Wakes the pod, and returns when the call ran.
    pub fn wake(&self, pod: &str) -> Result<(Wake, Window), CtlError> {
        let (json, window) = self.timed(&wake_args(pod), SLOW_CALL)?;
        Ok((Wake::parse(&json).map_err(CtlError::other)?, window))
    }

    /// Hibernates the pod, and returns when the call ran.
    pub fn hibernate(&self, pod: &str) -> Result<(Hibernation, Window), CtlError> {
        let (json, window) = self.timed(&hibernate_args(pod), SLOW_CALL)?;
        Ok((Hibernation::parse(&json).map_err(CtlError::other)?, window))
    }

    pub fn hold(&self, pod: &str, lease: &str, ttl: Duration) -> Result<PodStatus, CtlError> {
        let ttl = ttl.as_secs().to_string();
        let args = [
            "hold", pod, "--lease", lease, "--ttl", &ttl, "--holder", SOURCE,
        ];
        let json = self.call(&args, CALL)?;
        PodStatus::parse(json.get("pod")).map_err(CtlError::other)
    }

    pub fn release(&self, pod: &str, lease: &str) -> Result<PodStatus, CtlError> {
        let json = self.call(&["release", pod, "--lease", lease], CALL)?;
        PodStatus::parse(json.get("pod")).map_err(CtlError::other)
    }

    pub fn agent(&self, pod: &str) -> Result<AgentStats, CtlError> {
        let json = self.call(&["agent", pod], CALL)?;
        AgentStats::parse(&json).map_err(CtlError::other)
    }

    /// The events after `cursor`, in order.
    pub fn events(&self, cursor: u64) -> Result<Vec<Event>, CtlError> {
        let cursor = cursor.to_string();
        let args = ["events", "--cursor", cursor.as_str()];
        let ran = self.run(&args, CALL)?;
        if ran.exit != Some(0) {
            return parse_outcome(&args, ran.exit, &ran.stdout, &ran.stderr).map(|_| Vec::new());
        }
        parse_events(&ran.stdout).map_err(CtlError::other)
    }

    /// The events after `cursor` about the pod.
    pub fn events_of(&self, cursor: u64, pod: &str) -> Result<Vec<Event>, String> {
        let events = self.events(cursor).map_err(|e| e.to_string())?;
        Ok(events.into_iter().filter(|e| e.pod == pod).collect())
    }

    /// Runs the ctl calls `calls` at once, and returns each one's outcome.
    pub fn concurrent(&self, calls: &[Vec<&str>]) -> Result<Vec<Result<Json, CtlError>>, String> {
        if let Some(reason) = self.unavailable.borrow().as_ref() {
            return Err(format!("smolvm-pods ctl is not available: {reason}"));
        }
        let dir = self.node.try_sh("mktemp -d")?;
        let dir = dir.trim();
        let mut script = String::new();
        for (i, args) in calls.iter().enumerate() {
            script += &format!(
                "({} >{dir}/{i}.out 2>{dir}/{i}.err; echo $? >{dir}/{i}.status) &\n",
                ctl_command(args)
            );
        }
        script += "wait\n";
        self.node.try_sh(&script)?;
        let outcomes = calls
            .iter()
            .enumerate()
            .map(|(i, args)| {
                let read = |ext: &str| self.node.try_sh(&format!("cat {dir}/{i}.{ext}"));
                let status = read("status").map_err(CtlError::other)?;
                let exit = status.trim().parse().ok();
                let stdout = read("out").map_err(CtlError::other)?;
                let stderr = read("err").map_err(CtlError::other)?;
                parse_outcome(args, exit, &stdout, &stderr)
            })
            .collect();
        self.node.try_sh(&format!("rm -rf {dir}"))?;
        Ok(outcomes)
    }

    /// Starts a hibernate of `pod`, waits until `ctl get` shows it
    /// `pausing`, then `delay` more, and wakes it. Returns the state seen
    /// before the wake and the outcomes of the hibernate and the wake.
    pub fn wake_during_pause(&self, pod: &str, delay: Duration) -> Result<PauseAndWake, String> {
        if let Some(reason) = self.unavailable.borrow().as_ref() {
            return Err(format!("smolvm-pods ctl is not available: {reason}"));
        }
        let hibernate = hibernate_args(pod);
        let wake = wake_args(pod);
        let get = ["get", pod];
        let dir = self.node.try_sh("mktemp -d")?;
        let dir = dir.trim();
        let script = format!(
            r#"d={dir}
({hibernate} >$d/h.out 2>$d/h.err; echo $? >$d/h.status) &
state=none
for _ in $(seq 2000); do
  state=$({get} 2>/dev/null | jq -r .state 2>/dev/null)
  case $state in pausing | asleep) break ;; esac
  sleep 0.005
done
echo "$state" >$d/state
sleep {delay:.3}
{wake} >$d/w.out 2>$d/w.err; echo $? >$d/w.status
wait
"#,
            hibernate = ctl_command(&hibernate),
            get = ctl_command(&get),
            wake = ctl_command(&wake),
            delay = delay.as_secs_f64(),
        );
        self.node.try_sh(&script)?;
        let read = |file: &str| self.node.try_sh(&format!("cat {dir}/{file}"));
        let outcome = |name: &str, args: &[&str]| -> Result<Result<Json, CtlError>, String> {
            let exit = read(&format!("{name}.status"))?.trim().parse().ok();
            Ok(parse_outcome(
                args,
                exit,
                &read(&format!("{name}.out"))?,
                &read(&format!("{name}.err"))?,
            ))
        };
        let result = PauseAndWake {
            state_before_wake: read("state")?.trim().to_string(),
            hibernate: outcome("h", &hibernate)?,
            wake: outcome("w", &wake)?,
        };
        self.node.try_sh(&format!("rm -rf {dir}"))?;
        Ok(result)
    }

    /// The cursor after the last event so far.
    pub fn cursor(&self) -> Result<u64, CtlError> {
        Ok(self.events(0)?.last().map_or(0, |event| event.seq))
    }

    /// The runtime's processes in the node.
    pub fn processes(&self) -> Result<Vec<Process>, String> {
        let script = format!("set -- {BINARY}\n{PROCESSES}");
        let lines = self.node.try_sh(&script)?;
        Ok(lines.lines().filter_map(Process::parse).collect())
    }

    /// The one process of `role`.
    pub fn only(&self, role: Role) -> Result<Process, String> {
        let processes = self.processes()?;
        let found: Vec<&Process> = processes.iter().filter(|p| p.role == role).collect();
        match found[..] {
            [process] => Ok(process.clone()),
            _ => Err(format!(
                "expected one {role:?} process, found {}:\n{}",
                found.len(),
                list(&processes)
            )),
        }
    }

    /// Kills the node service with SIGKILL and returns its pid.
    pub fn kill_node_service(&self) -> Result<u32, String> {
        let service = self.only(Role::NodeService)?;
        self.node.try_sh(&format!("kill -KILL {}", service.pid))?;
        step(&format!("killed the node service, pid {}", service.pid));
        Ok(service.pid)
    }

    /// Waits until a node service other than `old` runs and the API answers,
    /// and returns its pid.
    pub fn wait_for_node_service(&self, old: u32, timeout: Duration) -> Result<u32, String> {
        let deadline = Instant::now() + timeout;
        loop {
            let last = match self.only(Role::NodeService) {
                Ok(service) if service.pid != old => match self.call(&["list"], CALL) {
                    Ok(_) => return Ok(service.pid),
                    Err(e) => e.to_string(),
                },
                Ok(service) => format!("the node service still has pid {}", service.pid),
                Err(e) => e,
            };
            if Instant::now() >= deadline {
                return Err(format!(
                    "no new node service answered within {timeout:?}: {last}"
                ));
            }
            thread::sleep(Duration::from_millis(250));
        }
    }

    /// Kills the node service with SIGKILL and waits until floci-node has
    /// started the next one and its API answers. Returns the new pid.
    pub fn restart_node_service(&self) -> Result<u32, String> {
        let old = self.kill_node_service()?;
        self.wait_for_node_service(old, NODE_SERVICE_RESTART)
    }

    /// Arms `point` for the next node service, for every pod or for the pod
    /// sandbox `sandbox` only: writes it to [`FAULT_FILE`], kills the node
    /// service, waits up to `restart` for the next one, and fails unless it
    /// took the file and knows the point. Returns the armed node service's
    /// pid.
    pub fn arm_fault(
        &self,
        point: FaultPoint,
        sandbox: Option<&str>,
        restart: Duration,
    ) -> Result<u32, String> {
        let mark = self.log_mark()?;
        let line = point.armed_for(sandbox);
        self.node.try_sh(&format!(
            "mkdir -p {dir} && printf '%s\\n' {quoted} > {FAULT_FILE}.new && \
             mv {FAULT_FILE}.new {FAULT_FILE}",
            dir = parent(FAULT_FILE),
            quoted = quote(&line)
        ))?;
        let old = self.kill_node_service()?;
        let armed = self.wait_for_node_service(old, restart)?;
        let left = self
            .node
            .try_sh(&format!("if [ -e {FAULT_FILE} ]; then echo left; fi"))?;
        if left.trim() == "left" {
            return Err(format!(
                "the node service started without taking {FAULT_FILE}"
            ));
        }
        let log = self.log_since(mark)?;
        if let Some(unknown) = log.lines().find(|l| l.contains("unknown fault point")) {
            return Err(format!(
                "the node service does not know fault point {}: {unknown}",
                point.name()
            ));
        }
        step(&format!("node service {armed} has fault point {line}"));
        Ok(armed)
    }

    /// Fails unless the node service logged after `mark` that it reached
    /// `point`.
    pub fn check_fault_reached(&self, point: FaultPoint, mark: u64) -> Result<(), String> {
        let line = point.log_line(None);
        if self.log_since(mark)?.contains(&line) {
            Ok(())
        } else {
            Err(format!(
                "{NODE_SERVICE_LOG} has no {line:?} after byte {mark}"
            ))
        }
    }

    /// The size of the node service's log, which marks a point in it.
    pub fn log_mark(&self) -> Result<u64, String> {
        let size = self.node.try_sh(&format!(
            "stat -c %s {NODE_SERVICE_LOG} 2>/dev/null || echo 0"
        ))?;
        size.trim()
            .parse()
            .map_err(|e| format!("size of {NODE_SERVICE_LOG}: {e}"))
    }

    /// What the node service logged after `mark`.
    pub fn log_since(&self, mark: u64) -> Result<String, String> {
        self.node
            .try_sh(&format!("tail -c +{} {NODE_SERVICE_LOG}", mark + 1))
    }

    /// The bytes used on the node's smolvm volume.
    pub fn disk_used(&self) -> Result<u64, String> {
        let used = self
            .node
            .try_sh(&format!("df -B1 --output=used {SMOLVM_VOLUME} | tail -n 1"))?;
        used.trim()
            .parse()
            .map_err(|e| format!("bytes used on {SMOLVM_VOLUME}: {e}: {used:?}"))
    }
}

/// The directory of `path`.
pub(crate) fn parent(path: &str) -> &str {
    path.rsplit_once('/').map_or(".", |(dir, _)| dir)
}

/// Parses the JSON lines of `ctl events`.
pub fn parse_events(text: &str) -> Result<Vec<Event>, String> {
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| Json::parse(line).and_then(|json| Event::parse(&json)))
        .collect()
}

/// The processes, one per line.
pub fn list(processes: &[Process]) -> String {
    processes
        .iter()
        .map(|p| format!("  {p}"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    const POD: &str = r#"{"sandbox_id": "5f3c", "namespace": "ns", "name": "orders-7c9d8f6b5-x2x9q",
        "uid": "u-1", "labels": {"app": "orders", "pod-template-hash": "7c9d8f6b5"},
        "controller": {"kind": "Deployment", "name": "orders"},
        "ip": "10.42.0.9", "netns": "/var/run/netns/cni-1", "machine": "m-5f3c",
        "state": "awake", "generation": 41, "incarnation": "i-1", "rosetta": true,
        "vm_pid": 4242,
        "leases": [{"id": "test", "kind": "api", "holder": "e2e",
                    "expires_at": "2026-10-19T12:00:30Z"}],
        "state_since": "2026-10-19T12:00:00Z", "last_wake_ms": 183.2, "last_pause_ms": 950.2}"#;

    #[test]
    fn parses_a_pod() {
        let pod = PodStatus::parse(&Json::parse(POD).unwrap()).unwrap();
        assert_eq!(pod.sandbox_id, "5f3c");
        assert_eq!(pod.state, PodState::Awake);
        assert_eq!(pod.generation, 41);
        assert_eq!(pod.vm_pid, Some(4242));
        assert_eq!(pod.rosetta, Some(true));
        assert_eq!(
            pod.leases,
            [Lease {
                id: "test".into(),
                kind: "api".into()
            }]
        );
        assert!(pod.has_lease("api") && !pod.has_lease("exec"));
        assert_eq!(
            pod.labels,
            [
                ("app".to_string(), "orders".to_string()),
                ("pod-template-hash".to_string(), "7c9d8f6b5".to_string())
            ]
        );
        assert_eq!(
            pod.controller,
            Some(("Deployment".to_string(), "orders".to_string()))
        );
        let bad = r#"{"sandbox_id": "5f3c", "state": "awake", "generation": 1,
            "controller": {"kind": "Deployment"}}"#;
        assert!(PodStatus::parse(&Json::parse(bad).unwrap()).is_err());
        let bad = r#"{"sandbox_id": "5f3c", "state": "awake", "generation": 1,
            "labels": {"app": 1}}"#;
        assert!(PodStatus::parse(&Json::parse(bad).unwrap()).is_err());
    }

    #[test]
    fn a_sleeping_pod_has_no_vm_process() {
        let json = r#"{"sandbox_id": "5f3c", "state": "asleep", "generation": 42,
            "vm_pid": null, "leases": [], "labels": {}, "controller": null}"#;
        let pod = PodStatus::parse(&Json::parse(json).unwrap()).unwrap();
        assert_eq!((pod.state, pod.vm_pid), (PodState::Asleep, None));
        assert!(pod.labels.is_empty() && pod.controller.is_none());
        let json = r#"{"sandbox_id": "5f3c", "state": "dozing", "generation": 1}"#;
        assert!(PodStatus::parse(&Json::parse(json).unwrap()).is_err());
    }

    #[test]
    fn parses_a_wake() {
        let json = format!(
            r#"{{"pod": {POD}, "resumed": true, "duration_ms": 183.2,
            "steps_ms": {{"admit": 0.1, "resume": 140.2, "tso": 0.3, "ping": 20.1,
                         "clock": 1.2, "deferred_kills": 0.0, "release": 0.4}},
            "clock": {{"skew_ns": -5012345678, "stepped": true}}}}"#
        );
        let wake = Wake::parse(&Json::parse(&json).unwrap()).unwrap();
        assert!(wake.resumed);
        assert_eq!(wake.steps_ms.len(), 7);
        assert_eq!(wake.steps_ms[1], ("resume".to_string(), 140.2));
        assert_eq!(
            wake.clock,
            Some(ClockStep {
                skew_ns: -5_012_345_678,
                stepped: true
            })
        );
        let merged = format!(r#"{{"pod": {POD}, "resumed": false, "duration_ms": 90.0}}"#);
        let wake = Wake::parse(&Json::parse(&merged).unwrap()).unwrap();
        assert!(!wake.resumed && wake.steps_ms.is_empty() && wake.clock.is_none());
    }

    #[test]
    fn parses_a_hibernation() {
        let json = format!(
            r#"{{"pod": {}, "duration_ms": 950.2, "checkpoint_bytes": 1234567}}"#,
            POD.replace("\"awake\"", "\"asleep\"")
        );
        let done = Hibernation::parse(&Json::parse(&json).unwrap()).unwrap();
        assert_eq!(done.pod.state, PodState::Asleep);
        assert_eq!(done.checkpoint_bytes, Some(1_234_567));
    }

    #[test]
    fn reads_errors_and_refusals() {
        let refused = r#"{"error": {"code": "refused", "message": "hibernate refused",
            "reasons": ["exec running", "lease held"]}}"#;
        let error = parse_outcome(&["hibernate", "p"], Some(1), refused, "").unwrap_err();
        assert_eq!(error.code, "refused");
        assert_eq!(error.reasons, ["exec running", "lease held"]);
        assert!(error.to_string().contains("hibernate refused"));

        let error = parse_outcome(&["list"], Some(3), "", "connect: no such file").unwrap_err();
        assert!(error.unreachable());
        assert!(error.to_string().contains("no such file"));

        let error = parse_outcome(&["list"], Some(1), "not json", "").unwrap_err();
        assert!(error.to_string().contains("no JSON error"));
        let error = parse_outcome(&["list"], Some(0), "{", "").unwrap_err();
        assert!(error.to_string().contains("invalid JSON"));
        assert!(parse_outcome(&["list"], Some(0), r#"{"pods": []}"#, "").is_ok());
    }

    #[test]
    fn parses_events() {
        let text = r#"{"seq": 811, "time": "2026-10-19T12:00:00.1Z", "pod": "5f3c", "from": "asleep", "to": "waking", "reason": "api", "source": "e2e"}

{"seq": 812, "time": "2026-10-19T12:00:00.3Z", "pod": "5f3c", "from": "waking", "to": "awake", "reason": "api", "source": "e2e", "steps_ms": {"resume": 140.2}, "clock": {"skew_ns": -61000000000, "stepped": true}}
{"seq": 813, "time": "2026-10-19T12:00:01Z", "pod": "5f3c", "from": "pausing", "to": "awake", "reason": "api", "cancelled": true, "error": null}
"#;
        let events = parse_events(text).unwrap();
        assert_eq!(events.len(), 3);
        assert!(events[0].is_resume() && !events[1].is_resume());
        assert_eq!(events[1].clock.unwrap().skew_ns, -61_000_000_000);
        assert!(events[2].cancelled && events[2].error.is_none());
        assert!(parse_events(r#"{"seq": 1}"#).is_err());
    }

    #[test]
    fn parses_agent_stats() {
        let json = Json::parse(r#"{"pid": 1, "threads": 9, "fds": 23}"#).unwrap();
        assert_eq!(
            AgentStats::parse(&json).unwrap(),
            AgentStats {
                pid: 1,
                threads: 9,
                fds: 23
            }
        );
    }

    #[test]
    fn tells_the_processes_apart() {
        let role = |args: &str| Role::of(&args.split(' ').collect::<Vec<_>>());
        let shim = "/usr/local/bin/containerd-shim-smolvm-pods-v2";
        assert_eq!(
            role(&format!(
                "{shim} -namespace k8s.io -id 5f3c -address /run/k3s/containerd/containerd.sock"
            )),
            Role::FrontShim
        );
        assert_eq!(
            role(&format!(
                "{shim} -namespace k8s.io -id 5f3c -address /a start"
            )),
            Role::ShimAction
        );
        assert_eq!(
            role(&format!("{shim} -namespace k8s.io -id 5f3c delete")),
            Role::ShimAction
        );
        assert_eq!(role("smolvm-pods node-service"), Role::NodeService);
        assert_eq!(role("smolvm-pods ctl list"), Role::Ctl);
        assert_eq!(
            role(&format!(
                "{shim} _boot-vm /var/lib/smolvm/vms/ab/boot-config.json"
            )),
            Role::Vm
        );
        assert_eq!(role("ip link show"), Role::Other);
        assert!(Role::Vm.lasting() && !Role::Ctl.lasting());
    }

    #[test]
    fn parses_process_lines() {
        let line = "77 1200 8000 12 /usr/local/bin/containerd-shim-smolvm-pods-v2\t-namespace\
                    \tk8s.io\t-id\t5f3c\t-address\t/run/c.sock\t";
        let process = Process::parse(line).unwrap();
        assert_eq!(process.pid, 77);
        assert_eq!(
            (process.uss_kib, process.rss_kib, process.threads),
            (1200, 8000, 12)
        );
        assert_eq!(process.role, Role::FrontShim);
        assert_eq!(process.args.len(), 7);
        assert_eq!(Process::parse("x 1 2 3 a"), None);
    }

    #[test]
    fn quotes_ctl_commands() {
        assert_eq!(
            ctl_command(&wake_args("5f3c")),
            "smolvm-pods ctl 'wake' '5f3c' '--reason' 'api' '--source' 'e2e'"
        );
        assert_eq!(quote("it's"), r"'it'\''s'");
    }

    #[test]
    fn names_the_fault_points() {
        let names: Vec<&str> = FaultPoint::KILLING.iter().map(|p| p.name()).collect();
        assert_eq!(
            names,
            [
                "pause:saving",
                "pause:durable",
                "wake:resumed",
                "wake:clock"
            ]
        );
        assert_eq!(
            FaultPoint::PauseSaving.log_line(None),
            "fault point pause:saving reached"
        );
        assert_eq!(FaultPoint::PauseDurable.recovered_state(), PodState::Asleep);
        assert_eq!(FaultPoint::WakeClock.recovered_state(), PodState::Awake);
        assert_eq!(FaultPoint::PauseSaving.armed_for(None), "pause:saving");
        assert_eq!(
            FaultPoint::IdleDecided.armed_for(Some("5f3c")),
            "idle:decided@5f3c"
        );
        assert_eq!(
            FaultPoint::WakeResumeError.log_line(Some("5f3c")),
            "fault point wake:resume-error reached for pod sandbox 5f3c"
        );
        assert_eq!(parent(FAULT_FILE), "/run/smolvm-pods");
    }
}
