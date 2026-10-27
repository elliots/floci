//! The durable record of a pod.

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::state::{PodState, Reason};
use super::vm::VmProcess;
use crate::bundle;
use crate::kube::{Controller, Labels};
use crate::probe::PodProbes;

/// Pod annotation that keeps a pod from ever sleeping. Only an explicit
/// `"false"` or `"0"` does.
pub const ANN_HIBERNATE: &str = "smolvm-pods.floci.io/hibernate";

/// Pod annotation that makes SIGTERM wake a sleeping pod. Only an explicit
/// `"true"` or `"1"` does.
pub const ANN_WAKE_ON_TERMINATE: &str = "smolvm-pods.floci.io/wake-on-terminate";

/// Pod annotation that sets how long the pod may be idle before it sleeps, as
/// a Go duration such as `"30s"` or `"10m"`.
pub const ANN_IDLE_TIMEOUT: &str = "smolvm-pods.floci.io/idle-timeout";

/// The idle timeouts that [`ANN_IDLE_TIMEOUT`] may set.
pub const IDLE_TIMEOUT_MIN: Duration = Duration::from_secs(10);
pub const IDLE_TIMEOUT_MAX: Duration = Duration::from_secs(24 * 3600);

/// Pod annotation that sets the VM CPU, in vCPUs, above which the pod is busy
/// and does not sleep, such as `"0.5"` or `"2"`; `"off"` or `"0"` turns that
/// rule off for the pod.
pub const ANN_CPU_THRESHOLD: &str = "smolvm-pods.floci.io/cpu-threshold";

/// Pod annotation that sets the time over which the pod's VM CPU is
/// averaged, as a Go duration such as `"60s"`.
pub const ANN_CPU_WINDOW: &str = "smolvm-pods.floci.io/cpu-window";

/// The CPU windows that [`ANN_CPU_WINDOW`] may set.
pub const CPU_WINDOW_MIN: Duration = Duration::from_secs(5);
pub const CPU_WINDOW_MAX: Duration = Duration::from_secs(3600);

/// The version of the record format.
pub const RECORD_VERSION: u32 = 1;

/// The annotation `key` of the sandbox OCI spec, trimmed and in lower case.
fn annotation(spec: Option<&serde_json::Value>, key: &str) -> Option<String> {
    bundle::annotation(spec, key).map(|v| v.trim().to_ascii_lowercase())
}

/// Whether the sandbox OCI spec opts the pod out of hibernation. A CRI pod
/// hibernates unless it says `"false"`; a standalone pod only when it says
/// `"true"`.
pub fn hibernation_disabled(spec: Option<&serde_json::Value>, standalone: bool) -> bool {
    let flag = bundle::annotation(spec, ANN_HIBERNATE).and_then(bundle::parse_flag);
    if standalone {
        flag != Some(true)
    } else {
        flag == Some(false)
    }
}

/// Whether the sandbox OCI spec asks SIGTERM to wake the pod.
pub fn wakes_on_terminate(spec: Option<&serde_json::Value>) -> bool {
    bundle::annotation(spec, ANN_WAKE_ON_TERMINATE).and_then(bundle::parse_flag) == Some(true)
}

/// The duration that the sandbox OCI spec sets with the annotation `key`, if
/// any. A value that is not a duration, or is outside `min` to `max`, which
/// `range` names, is an error.
fn duration_annotation(
    spec: Option<&serde_json::Value>,
    key: &str,
    min: Duration,
    max: Duration,
    range: &str,
) -> Result<Option<Duration>, String> {
    let Some(value) = annotation(spec, key) else {
        return Ok(None);
    };
    let duration = parse_duration(&value).map_err(|e| format!("{key}: {e}"))?;
    if !(min..=max).contains(&duration) {
        return Err(format!("{key}: {value:?} is not between {range}"));
    }
    Ok(Some(duration))
}

/// The idle timeout that the sandbox OCI spec sets with [`ANN_IDLE_TIMEOUT`],
/// if any. A value that is not a duration, or is outside 10 s to 24 h, is an
/// error.
pub fn idle_timeout(spec: Option<&serde_json::Value>) -> Result<Option<Duration>, String> {
    duration_annotation(
        spec,
        ANN_IDLE_TIMEOUT,
        IDLE_TIMEOUT_MIN,
        IDLE_TIMEOUT_MAX,
        "10s and 24h",
    )
}

/// A CPU threshold in vCPUs, as the node service's `--cpu-threshold` and the
/// annotation [`ANN_CPU_THRESHOLD`] write it: a number above 0, or `off` or
/// `0` for no CPU rule (none).
pub fn parse_cpu_threshold(text: &str) -> Result<Option<f64>, String> {
    let text = text.trim();
    if text.eq_ignore_ascii_case("off") {
        return Ok(None);
    }
    match text.parse::<f64>() {
        Ok(0.0) => Ok(None),
        Ok(v) if v.is_finite() && v > 0.0 => Ok(Some(v)),
        _ => Err(format!("{text:?} is not a number of vCPUs above 0, or off")),
    }
}

/// The CPU threshold that the sandbox OCI spec sets with
/// [`ANN_CPU_THRESHOLD`], if any, in thousandths of a vCPU: 0 turns the CPU
/// rule off for the pod, and a threshold above 0 is at least 1.
pub fn cpu_threshold(spec: Option<&serde_json::Value>) -> Result<Option<u32>, String> {
    let Some(value) = annotation(spec, ANN_CPU_THRESHOLD) else {
        return Ok(None);
    };
    let threshold = parse_cpu_threshold(&value).map_err(|e| format!("{ANN_CPU_THRESHOLD}: {e}"))?;
    Ok(Some(threshold.map_or(0, |v| {
        (v * 1000.0).ceil().clamp(1.0, f64::from(u32::MAX)) as u32
    })))
}

/// The CPU window that the sandbox OCI spec sets with [`ANN_CPU_WINDOW`], if
/// any. A value that is not a duration, or is outside 5 s to 1 h, is an
/// error.
pub fn cpu_window(spec: Option<&serde_json::Value>) -> Result<Option<Duration>, String> {
    duration_annotation(
        spec,
        ANN_CPU_WINDOW,
        CPU_WINDOW_MIN,
        CPU_WINDOW_MAX,
        "5s and 1h",
    )
}

/// A Go duration, as Kubernetes annotations and the node service's flags
/// write them: `"0"`, or numbers with units, such as `"30s"`, `"1.5h"` or
/// `"1m30s"`. The units are `ns`, `us` (or `µs`), `ms`, `s`, `m` and `h`.
pub fn parse_duration(text: &str) -> Result<Duration, String> {
    let invalid = || format!("{text:?} is not a duration such as 30s, 10m or 1h30m");
    let mut rest = text.trim();
    if rest == "0" {
        return Ok(Duration::ZERO);
    }
    if rest.is_empty() {
        return Err(invalid());
    }
    let mut nanos = 0f64;
    while !rest.is_empty() {
        let number_end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        let (number, after) = rest.split_at(number_end);
        let unit_end = after
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(after.len());
        let (unit, next) = after.split_at(unit_end);
        let value: f64 = number.parse().map_err(|_| invalid())?;
        let scale = match unit {
            "ns" => 1.0,
            "us" | "µs" | "μs" => 1e3,
            "ms" => 1e6,
            "s" => 1e9,
            "m" => 60e9,
            "h" => 3600e9,
            _ => return Err(invalid()),
        };
        nanos += value * scale;
        rest = next;
    }
    if !nanos.is_finite() || nanos >= u64::MAX as f64 {
        return Err(invalid());
    }
    Ok(Duration::from_nanos(nanos.round() as u64))
}

/// Who a pod is. The lifecycle sets `incarnation` when it creates the record.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodIdentity {
    pub sandbox_id: String,
    #[serde(default)]
    pub incarnation: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub uid: String,
    /// The pod's labels and the workload that controls it, as the pod watch
    /// last saw them.
    #[serde(default, skip_serializing_if = "Labels::is_empty")]
    pub labels: Labels,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller: Option<Controller>,
    /// The smolvm machine of the pod's VM.
    pub machine: String,
    #[serde(default)]
    pub netns: Option<String>,
    #[serde(default)]
    pub rosetta: bool,
    /// Set by [`ANN_HIBERNATE`].
    #[serde(default)]
    pub hibernate_disabled: bool,
    /// Set by [`ANN_WAKE_ON_TERMINATE`].
    #[serde(default)]
    pub wake_on_terminate: bool,
    /// Set by [`ANN_IDLE_TIMEOUT`]; without it the node's default applies.
    #[serde(default)]
    pub idle_timeout_ms: Option<u64>,
    /// Set by [`ANN_CPU_THRESHOLD`], in thousandths of a vCPU, 0 for no CPU
    /// rule; without it the node's default applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_threshold_milli: Option<u32>,
    /// Set by [`ANN_CPU_WINDOW`]; without it the node's default applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_window_ms: Option<u64>,
    #[serde(default)]
    pub ip: Option<String>,
    #[serde(default)]
    pub mac: Option<String>,
    /// The containerd namespace of the pod's tasks.
    #[serde(default)]
    pub task_namespace: String,
    /// A container that did not come from CRI, such as Docker's, as a pod
    /// of its own ([`crate::standalone`]): it has no sandbox task, and it
    /// sleeps on the idle and CPU rules alone.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub standalone: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub expires_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub holder: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskKind {
    Sandbox,
    Container,
}

/// containerd's stdio paths for a process.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StdioPaths {
    #[serde(default)]
    pub stdin: String,
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
    #[serde(default)]
    pub terminal: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRecord {
    pub id: String,
    pub kind: TaskKind,
    /// The synthetic pid that containerd sees.
    pub pid: u32,
    #[serde(default)]
    pub bundle: String,
    #[serde(default)]
    pub stdio: StdioPaths,
    /// The task was started: its output is pumped while the pod is awake.
    #[serde(default)]
    pub started: bool,
    #[serde(default)]
    pub exit_status: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExecStatus {
    Created,
    Running,
    Exited,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecRecord {
    pub container: String,
    pub exec: String,
    pub pid: u32,
    #[serde(default)]
    pub stdio: StdioPaths,
    pub status: ExecStatus,
    #[serde(default)]
    pub exit_status: Option<u32>,
}

impl ExecRecord {
    pub fn key(&self) -> String {
        exec_key(&self.container, &self.exec)
    }
}

pub fn exec_key(container: &str, exec: &str) -> String {
    format!("{container}/{exec}")
}

/// A kill recorded while the pod slept. The next wake sends SIGKILL to the
/// guest process before it releases anything.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeferredKill {
    pub container: String,
    #[serde(default)]
    pub exec: Option<String>,
    /// The signal containerd asked for, which sets the exit status reported.
    pub signal: u32,
    #[serde(default)]
    pub all: bool,
}

/// A container (`exec` none) or an exec that containerd deleted while its
/// pod was not awake. The guest still has it, and the next wake deletes it
/// there.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuestDelete {
    pub container: String,
    #[serde(default)]
    pub exec: Option<String>,
}

/// When something ended, and how long it took.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Timing {
    pub at_ms: u64,
    pub ms: f64,
}

/// `pods/<sandbox-id>.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PodRecord {
    pub version: u32,
    pub identity: PodIdentity,
    pub state: PodState,
    /// Increases with every state change.
    pub generation: u64,
    /// The node boot the record belongs to.
    pub boot_id: String,
    pub created_ms: u64,
    /// When the pod entered its state.
    pub since_ms: u64,
    /// The reason of the last transition, or of the one in flight, which
    /// recovery reports again.
    #[serde(default)]
    pub reason: Option<Reason>,
    #[serde(default)]
    pub leases: BTreeMap<String, Lease>,
    #[serde(default)]
    pub tasks: BTreeMap<String, TaskRecord>,
    /// By `container/exec`.
    #[serde(default)]
    pub execs: BTreeMap<String, ExecRecord>,
    #[serde(default)]
    pub deferred_kills: Vec<DeferredKill>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub guest_deletes: Vec<GuestDelete>,
    /// The VM process, while there is one.
    #[serde(default)]
    pub vm: Option<VmProcess>,
    /// Resumes that failed in a row before the VM started.
    #[serde(default)]
    pub wake_failures: u32,
    /// Why the pod failed, or, while it sleeps after a failed resume, why
    /// that resume failed.
    #[serde(default)]
    pub failure: Option<String>,
    #[serde(default)]
    pub last_wake: Option<Timing>,
    #[serde(default)]
    pub last_sleep: Option<Timing>,
    #[serde(default)]
    pub checkpoint_bytes: Option<u64>,
    /// The pod's kubelet probes and their last results, once the API server
    /// told us about them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probes: Option<PodProbes>,
}

impl PodRecord {
    pub fn new(identity: PodIdentity, boot_id: String, now_ms: u64) -> PodRecord {
        PodRecord {
            version: RECORD_VERSION,
            identity,
            state: PodState::Creating,
            generation: 1,
            boot_id,
            created_ms: now_ms,
            since_ms: now_ms,
            reason: Some(Reason::Api),
            leases: BTreeMap::new(),
            tasks: BTreeMap::new(),
            execs: BTreeMap::new(),
            deferred_kills: Vec::new(),
            guest_deletes: Vec::new(),
            vm: None,
            wake_failures: 0,
            failure: None,
            last_wake: None,
            last_sleep: None,
            checkpoint_bytes: None,
            probes: None,
        }
    }
}

/// 32 random hex digits, for incarnations and lease ids.
pub fn random_id() -> String {
    let mut bytes = [0u8; 16];
    let read = std::fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut bytes));
    if read.is_err() {
        // Without /dev/urandom: the time, the pid and a counter, which differ
        // between calls and between processes.
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let count = COUNTER.fetch_add(1, Ordering::Relaxed);
        bytes[..8].copy_from_slice(&nanos.to_be_bytes());
        bytes[8..12].copy_from_slice(&std::process::id().to_be_bytes());
        bytes[12..].copy_from_slice(&(count as u32).to_be_bytes());
    }
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(annotations: serde_json::Value) -> serde_json::Value {
        serde_json::json!({ "annotations": annotations })
    }

    #[test]
    fn only_an_explicit_false_disables_hibernation() {
        let with = |value: &str| spec(serde_json::json!({ ANN_HIBERNATE: value }));
        for value in ["false", "0", " FALSE "] {
            assert!(hibernation_disabled(Some(&with(value)), false), "{value:?}");
        }
        for value in ["true", "1", "no", ""] {
            assert!(
                !hibernation_disabled(Some(&with(value)), false),
                "{value:?}"
            );
        }
        assert!(!hibernation_disabled(None, false));
        assert!(!hibernation_disabled(Some(&serde_json::json!({})), false));
    }

    #[test]
    fn a_standalone_pod_hibernates_only_with_an_explicit_true() {
        let with = |value: &str| spec(serde_json::json!({ ANN_HIBERNATE: value }));
        for value in ["true", "1", " TRUE "] {
            assert!(!hibernation_disabled(Some(&with(value)), true), "{value:?}");
        }
        for value in ["false", "0", "yes", ""] {
            assert!(hibernation_disabled(Some(&with(value)), true), "{value:?}");
        }
        assert!(hibernation_disabled(None, true));
        assert!(hibernation_disabled(Some(&serde_json::json!({})), true));
    }

    #[test]
    fn only_an_explicit_true_wakes_on_terminate() {
        let on = spec(serde_json::json!({ ANN_WAKE_ON_TERMINATE: "true" }));
        let off = spec(serde_json::json!({ ANN_WAKE_ON_TERMINATE: "yes" }));
        assert!(wakes_on_terminate(Some(&on)));
        assert!(!wakes_on_terminate(Some(&off)));
        assert!(!wakes_on_terminate(None));
    }

    #[test]
    fn go_durations_parse() {
        let cases = [
            ("0", Duration::ZERO),
            ("30s", Duration::from_secs(30)),
            ("10m", Duration::from_secs(600)),
            ("1h30m", Duration::from_secs(5400)),
            ("1.5h", Duration::from_secs(5400)),
            (".5s", Duration::from_millis(500)),
            ("250ms", Duration::from_millis(250)),
            ("2us", Duration::from_micros(2)),
            ("3µs", Duration::from_micros(3)),
            ("7ns", Duration::from_nanos(7)),
            (" 1m ", Duration::from_secs(60)),
        ];
        for (text, want) in cases {
            assert_eq!(parse_duration(text), Ok(want), "{text:?}");
        }
        for bad in ["", "30", "s", "10x", "-5s", "1.2.3s", "5 m", "1h 30m", "."] {
            assert!(parse_duration(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn the_idle_timeout_annotation_is_a_duration_from_10s_to_24h() {
        let with = |value: &str| spec(serde_json::json!({ ANN_IDLE_TIMEOUT: value }));
        assert_eq!(idle_timeout(None), Ok(None));
        assert_eq!(idle_timeout(Some(&spec(serde_json::json!({})))), Ok(None));
        assert_eq!(
            idle_timeout(Some(&with("10s"))),
            Ok(Some(Duration::from_secs(10)))
        );
        assert_eq!(
            idle_timeout(Some(&with("24h"))),
            Ok(Some(Duration::from_secs(86400)))
        );
        for bad in ["9s", "25h", "0", "soon"] {
            let e = idle_timeout(Some(&with(bad))).unwrap_err();
            assert!(e.starts_with(ANN_IDLE_TIMEOUT), "{bad:?}: {e}");
        }
    }

    #[test]
    fn the_cpu_annotations_set_a_threshold_or_off_and_a_window() {
        let with = |key: &str, value: &str| spec(serde_json::json!({ key: value }));
        assert_eq!(cpu_threshold(None), Ok(None));
        let threshold = |value| cpu_threshold(Some(&with(ANN_CPU_THRESHOLD, value)));
        assert_eq!(threshold("0.5"), Ok(Some(500)));
        assert_eq!(threshold("2"), Ok(Some(2000)));
        assert_eq!(threshold("0.0001"), Ok(Some(1)));
        assert_eq!(threshold("off"), Ok(Some(0)));
        assert_eq!(threshold("OFF"), Ok(Some(0)));
        assert_eq!(threshold("0"), Ok(Some(0)));
        for bad in ["-1", "half", "", "inf", "NaN"] {
            let e = threshold(bad).unwrap_err();
            assert!(e.starts_with(ANN_CPU_THRESHOLD), "{bad:?}: {e}");
        }
        let window = |value| cpu_window(Some(&with(ANN_CPU_WINDOW, value)));
        assert_eq!(cpu_window(None), Ok(None));
        assert_eq!(window("30s"), Ok(Some(Duration::from_secs(30))));
        assert_eq!(window("1h"), Ok(Some(Duration::from_secs(3600))));
        for bad in ["4s", "2h", "0", "soon"] {
            let e = window(bad).unwrap_err();
            assert!(e.starts_with(ANN_CPU_WINDOW), "{bad:?}: {e}");
        }
        assert_eq!(parse_cpu_threshold(" 1.5 "), Ok(Some(1.5)));
        assert_eq!(parse_cpu_threshold("off"), Ok(None));
    }

    #[test]
    fn ids_are_32_hex_digits_and_differ() {
        let (a, b) = (random_id(), random_id());
        assert_eq!(a.len(), 32);
        assert!(a.bytes().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
    }

    #[test]
    fn a_record_without_optional_fields_loads() {
        let json = r#"{"version":1,"identity":{"sandbox_id":"pod","machine":"pod"},
            "state":"asleep","generation":4,"boot_id":"b","created_ms":1,"since_ms":2}"#;
        let record: PodRecord = serde_json::from_str(json).unwrap();
        assert_eq!(record.state, PodState::Asleep);
        assert!(record.leases.is_empty() && record.vm.is_none());
    }
}
