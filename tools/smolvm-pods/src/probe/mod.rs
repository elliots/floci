//! Kubelet probes of a pod that is pausing, asleep or waking.
//!
//! - `spec.rs` keeps each pod's probes from the API server in the node's
//!   [`NodeProbeBook`], and `check.rs` runs the pre-sleep checks.
//! - `responder.rs` answers ICMP echo, and TCP, HTTP, HTTPS and gRPC probes
//!   from the last results, in userspace, for the catcher of
//!   [`crate::sleepnet`] ([`Responder`], with `http.rs`, `grpc.rs` and
//!   `tls.rs`).
//! - `exec.rs` answers exec probes from the cached results.

mod check;
mod exec;
mod grpc;
mod http;
mod responder;
mod spec;
mod tls;

use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::sleepnet::{Frame, PodNet};

pub use exec::{
    cached, container_name, probe_exec, result as exec_result, write_output, CachedAnswer,
    ExecCommand, OutputCapture, ProbeExec,
};
pub use responder::ProbeResponders;
pub use spec::NodeProbeBook;

/// Kubelet keeps at most this much of an exec probe's output, and so does
/// the cache.
pub const EXEC_OUTPUT_MAX: usize = 10 * 1024;

/// Which of a container's probes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProbeRole {
    Startup,
    Liveness,
    Readiness,
}

/// What a probe does, as kubelet runs it: named ports resolved, and `$(VAR)`
/// in exec commands expanded from the container's static environment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ProbeAction {
    Tcp {
        port: u16,
    },
    Http {
        port: u16,
        /// The request target, with any query, as kubelet sends it.
        path: String,
        https: bool,
        /// The probe's own headers, which kubelet sends too. A `Host` or
        /// `User-Agent` here replaces kubelet's.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        headers: Vec<(String, String)>,
    },
    Grpc {
        port: u16,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        service: Option<String>,
    },
    Exec {
        command: Vec<String>,
    },
}

impl ProbeAction {
    /// The port of a TCP, HTTP or gRPC probe.
    pub fn port(&self) -> Option<u16> {
        match self {
            ProbeAction::Tcp { port }
            | ProbeAction::Http { port, .. }
            | ProbeAction::Grpc { port, .. } => Some(*port),
            ProbeAction::Exec { .. } => None,
        }
    }
}

/// The outcome of one run of a probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeResult {
    pub passed: bool,
    /// The HTTP status, the gRPC status, or an exec's exit code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<i32>,
    /// An exec's output, at most [`EXEC_OUTPUT_MAX`] bytes.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub output: String,
    pub at_ms: u64,
}

/// One probe of one container, with its last result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Probe {
    pub container: String,
    pub role: ProbeRole,
    pub action: ProbeAction,
    pub period_secs: u32,
    pub timeout_secs: u32,
    /// For a TCP, HTTP or gRPC probe, its pre-sleep run; for an exec probe,
    /// the last real exec of its command while the pod was awake.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last: Option<ProbeResult>,
}

impl Probe {
    /// Whether this is an exec probe of `container` with exactly `command`.
    fn runs(&self, container: &str, command: &[String]) -> bool {
        self.container == container
            && matches!(&self.action, ProbeAction::Exec { command: c } if c == command)
    }
}

/// A pod's probes, kept in its record.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodProbes {
    pub probes: Vec<Probe>,
    /// The pod is Ready, and every container ready and started, by the API
    /// server.
    #[serde(default)]
    pub ready: bool,
    /// When the API server last told us about the pod.
    #[serde(default)]
    pub at_ms: u64,
}

impl PodProbes {
    /// Whether `port` is the port of a TCP, HTTP or gRPC probe.
    pub fn is_probe_port(&self, port: u16) -> bool {
        self.probes.iter().any(|p| p.action.port() == Some(port))
    }

    /// The cached result of the exec probe of `container` whose command is
    /// exactly `command`.
    pub fn exec(&self, container: &str, command: &[String]) -> Option<&ProbeResult> {
        self.probes
            .iter()
            .filter(|p| p.runs(container, command))
            .find_map(|p| p.last.as_ref())
    }

    /// Caches the result of a real exec of a probe's command. Returns false
    /// if no exec probe of `container` has exactly that command.
    pub fn record_exec(
        &mut self,
        container: &str,
        command: &[String],
        result: ProbeResult,
    ) -> bool {
        let mut found = false;
        for probe in self
            .probes
            .iter_mut()
            .filter(|p| p.runs(container, command))
        {
            probe.last = Some(result.clone());
            found = true;
        }
        found
    }
}

/// The record of every pod's probes. The responder and the idle
/// [`crate::activity::Monitor`] read it; the node service runs the pre-sleep
/// check through it and caches exec results in it.
pub trait ProbeBook: Send + Sync {
    /// The pod's probes, or none until they are recorded. A pod without them
    /// does not sleep.
    fn probes(&self, sandbox_id: &str) -> Option<Arc<PodProbes>>;

    /// The pre-sleep check: runs the pod's TCP, HTTP and gRPC liveness and
    /// readiness probes from the node with their timeouts, records the
    /// results, and checks the last exec results (within twice their period)
    /// and the pod's readiness. An error says why the pod must not sleep.
    /// Blocking.
    fn check_before_sleep(&self, sandbox_id: &str) -> Result<(), String>;

    /// Caches the result of a real exec of a probe's command in the awake pod.
    fn record_exec(
        &self,
        sandbox_id: &str,
        container: &str,
        command: &[String],
        result: ProbeResult,
    );
}

/// A pod's probe responder, which its catcher drives on the netloop thread. It
/// answers ICMP echo, and TCP, HTTP, HTTPS and gRPC probes from
/// [`ProbeBook::probes`].
pub trait Responder: Send {
    /// Offers a frame to the pod. The responder takes a kubelet probe's SYN, a
    /// segment of a connection it has open, and an ICMP echo request, and
    /// returns false for any other frame, which the catcher handles. It
    /// completes partial checksums, and never takes a segmented (GSO) frame.
    fn offer(&mut self, frame: &Frame<'_>, now: Instant) -> bool;

    /// Moves the responder on: appends the frames to send out of the pod's
    /// eth0 to `out`, and returns when to poll it again.
    fn poll(&mut self, now: Instant, out: &mut Vec<Vec<u8>>) -> Option<Instant>;

    /// Whether no connection is open, for the drain before a release.
    fn is_idle(&self) -> bool;

    /// After a failed restore every probe fails, so that kubelet sees it.
    fn set_failing(&mut self, failing: bool);
}

/// Makes the responder of a pod, on its first probe or echo request.
pub trait ResponderFactory: Send + Sync {
    fn responder(&self, pod: &PodNet) -> Box<dyn Responder>;
}

/// A responder for tests that takes nothing: every probe SYN is then held,
/// and wakes the pod.
#[cfg(test)]
pub struct NoResponder;

#[cfg(test)]
impl Responder for NoResponder {
    fn offer(&mut self, _frame: &Frame<'_>, _now: Instant) -> bool {
        false
    }

    fn poll(&mut self, _now: Instant, _out: &mut Vec<Vec<u8>>) -> Option<Instant> {
        None
    }

    fn is_idle(&self) -> bool {
        true
    }

    fn set_failing(&mut self, _failing: bool) {}
}

#[cfg(test)]
impl ResponderFactory for NoResponder {
    fn responder(&self, _pod: &PodNet) -> Box<dyn Responder> {
        Box::new(NoResponder)
    }
}

/// A probe book for tests: probes set by the test, and a pre-sleep check
/// that passes unless the test sets a refusal.
#[cfg(test)]
#[derive(Default)]
pub struct MockProbeBook {
    pub pods: std::sync::Mutex<std::collections::HashMap<String, PodProbes>>,
    pub refusal: std::sync::Mutex<Option<String>>,
    /// The pods whose pre-sleep check ran, in order.
    pub checked: std::sync::Mutex<Vec<String>>,
}

#[cfg(test)]
impl ProbeBook for MockProbeBook {
    fn probes(&self, sandbox_id: &str) -> Option<Arc<PodProbes>> {
        crate::sync::lock(&self.pods)
            .get(sandbox_id)
            .cloned()
            .map(Arc::new)
    }

    fn check_before_sleep(&self, sandbox_id: &str) -> Result<(), String> {
        crate::sync::lock(&self.checked).push(sandbox_id.to_string());
        if !crate::sync::lock(&self.pods).contains_key(sandbox_id) {
            return Err("the pod's probes are not recorded yet".into());
        }
        crate::sync::lock(&self.refusal).clone().map_or(Ok(()), Err)
    }

    fn record_exec(
        &self,
        sandbox_id: &str,
        container: &str,
        command: &[String],
        result: ProbeResult,
    ) {
        if let Some(probes) = crate::sync::lock(&self.pods).get_mut(sandbox_id) {
            probes.record_exec(container, command, result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn passed(code: i32, at_ms: u64) -> ProbeResult {
        ProbeResult {
            passed: true,
            code: Some(code),
            output: String::new(),
            at_ms,
        }
    }

    fn probe(role: ProbeRole, action: ProbeAction, last: Option<ProbeResult>) -> Probe {
        Probe {
            container: "app".into(),
            role,
            action,
            period_secs: 10,
            timeout_secs: 1,
            last,
        }
    }

    fn command(argv: &[&str]) -> Vec<String> {
        argv.iter().map(|a| a.to_string()).collect()
    }

    fn probes() -> PodProbes {
        let http = |path: &str| ProbeAction::Http {
            port: 8080,
            path: path.into(),
            https: false,
            headers: Vec::new(),
        };
        PodProbes {
            probes: vec![
                probe(ProbeRole::Startup, ProbeAction::Tcp { port: 7070 }, None),
                probe(ProbeRole::Liveness, http("/livez"), Some(passed(200, 1))),
                probe(ProbeRole::Readiness, http("/readyz"), Some(passed(204, 2))),
                probe(
                    ProbeRole::Startup,
                    ProbeAction::Grpc {
                        port: 9090,
                        service: None,
                    },
                    None,
                ),
                probe(
                    ProbeRole::Liveness,
                    ProbeAction::Exec {
                        command: command(&["cat", "/tmp/healthy"]),
                    },
                    None,
                ),
            ],
            ready: true,
            at_ms: 3,
        }
    }

    #[test]
    fn probe_ports_are_those_of_tcp_http_and_grpc_probes() {
        let probes = probes();
        assert!(probes.is_probe_port(7070));
        assert!(probes.is_probe_port(8080) && probes.is_probe_port(9090));
        assert!(!probes.is_probe_port(80));
    }

    #[test]
    fn exec_results_are_cached_by_container_and_exact_command() {
        let mut probes = probes();
        let argv = command(&["cat", "/tmp/healthy"]);
        assert!(probes.exec("app", &argv).is_none());
        let result = ProbeResult {
            passed: false,
            code: Some(1),
            output: "cat: /tmp/healthy: No such file or directory\n".into(),
            at_ms: 9,
        };
        assert!(probes.record_exec("app", &argv, result.clone()));
        assert_eq!(probes.exec("app", &argv), Some(&result));
        assert!(probes.exec("sidecar", &argv).is_none());
        let other = command(&["cat", "/tmp/healthy", "-"]);
        assert!(!probes.record_exec("app", &other, result));
        assert!(probes.exec("app", &other).is_none());
    }

    #[test]
    fn probes_round_trip_through_json() {
        let probes = probes();
        let json = serde_json::to_string(&probes).unwrap();
        assert!(
            json.contains(r#""kind":"http","port":8080,"path":"/livez""#),
            "{json}"
        );
        assert!(json.contains(r#""role":"readiness""#), "{json}");
        assert_eq!(serde_json::from_str::<PodProbes>(&json).unwrap(), probes);
    }
}
