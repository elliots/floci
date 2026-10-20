//! Harness for the smolvm-pods end-to-end tests.
//!
//! The tests run on the Mac. They create a node VM with `scripts/node-vm.sh`,
//! or use a running one, build the test image with Docker, import it into the
//! node's containerd and run kubectl inside the node. They are ignored unless
//! cargo test gets `--ignored`, and then fail unless `SMOLVM_PODS_E2E=1`; a
//! missing prerequisite fails them too.
//!
//! - `SMOLVM_PODS_E2E_NODE=NAME` uses that running node and leaves it running.
//!   Without it, each test creates a node named `floci-SUITE-e2e-ID`, SUITE
//!   being the name of its file in `tests/`, and deletes it.
//! - `SMOLVM_PODS_E2E_PREFIX=PREFIX` names what the test creates `PREFIX-ID`.
//! - `SMOLVM_PODS_E2E_NODE_PORT=PORT` is a NodePort that the running node
//!   publishes on the Mac (default: 30080, the first that `node-vm.sh create`
//!   publishes). A node that a test creates publishes one of its own.
//! - The node's shim must be the build under test: the file at
//!   `SMOLVM_PODS_SHIM` (default: the release-small build), or the SHA-256 in
//!   `SMOLVM_PODS_E2E_SHIM_SHA256`.
//! - `SMOLVM_PODS_E2E_NETNS_TEST` is the shim's unit test binary, whose
//!   network test the pods test runs in the node.
//! - `SMOLVM_PODS_E2E_KEEP=1` keeps the node and the test's namespace, for
//!   debugging.

pub mod checkers;
mod cluster;
mod cmd;
mod http;
mod images;
pub mod json;
mod netns;
mod node;
mod pod;
pub mod recorder;
pub mod runtime;
pub mod traffic;
pub mod wake;

use std::panic::{self, AssertUnwindSafe};
use std::thread;
use std::time::{Duration, Instant};

pub use cluster::{Cluster, Kernel};
pub use cmd::{describe, Output};
pub use http::{http_get, StandIn};
pub use netns::NetnsHolder;
pub use node::{Node, COMMAND_TIMEOUT};
pub use pod::{App, Arch, Pod, PodSpec, Runtime, State, VmProcess, NO_IDLE_SLEEP};

/// The RuntimeClass, and its handler, under test.
pub const RUNTIME_CLASS: &str = "smolvm-pods";

/// The pod annotation that requires Rosetta for a pod's VM, and with it TSO in
/// the VM process: the pod is refused where the node cannot give it Rosetta.
/// An arm64 node gives every pod Rosetta by default where it can.
pub const ROSETTA: (&str, &str) = ("smolvm-pods.floci.io/rosetta", "true");

/// The same annotation, turning Rosetta off for a pod that would get it by
/// default.
pub const NO_ROSETTA: (&str, &str) = ("smolvm-pods.floci.io/rosetta", "false");

/// Calls `check` until it succeeds or `timeout` passes, then panics with its
/// last error.
#[track_caller]
pub fn eventually<T>(
    timeout: Duration,
    what: &str,
    mut check: impl FnMut() -> Result<T, String>,
) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        match check() {
            Ok(value) => return value,
            Err(e) if Instant::now() >= deadline => {
                panic!("timed out after {timeout:?} waiting for {what}: {e}")
            }
            Err(_) => thread::sleep(Duration::from_secs(1)),
        }
    }
}

/// Runs every check, also after one fails, and then fails if any did.
pub fn run_checks(checks: &[(&str, &dyn Fn())]) {
    let mut failed = Vec::new();
    for &(name, check) in checks {
        step(&format!("check {name}"));
        if panic::catch_unwind(AssertUnwindSafe(check)).is_err() {
            failed.push(name);
        }
    }
    assert!(failed.is_empty(), "failed checks:\n{}", failed.join("\n"));
}

/// The pod is Running and Ready, and no container restarted.
#[track_caller]
pub fn assert_running_without_restarts(pod: &Pod) {
    assert_eq!(
        pod.field("{.status.phase}"),
        "Running",
        "phase of pod {}",
        pod.name
    );
    let ready = pod.field(r#"{.status.conditions[?(@.type=="Ready")].status}"#);
    assert_eq!(ready, "True", "Ready condition of pod {}", pod.name);
    let restarts = pod.field("{.status.containerStatuses[*].restartCount}");
    assert!(
        restarts.split_whitespace().all(|n| n == "0"),
        "pod {} restarted containers: {restarts}",
        pod.name
    );
}

/// Each pod is Running and Ready, and no container restarted.
#[track_caller]
pub fn assert_all_running_without_restarts(pods: &[Pod]) {
    for pod in pods {
        assert_running_without_restarts(pod);
    }
}

/// Prints a progress line.
pub fn step(message: &str) {
    eprintln!("[e2e] {message}");
}

/// Returns the value of `result`, or panics with its error at the caller.
#[track_caller]
pub fn expect<T>(result: Result<T, String>) -> T {
    match result {
        Ok(value) => value,
        Err(e) => panic!("{e}"),
    }
}

/// The value at quantile `q` (0 to 1) of `values`, by the nearest rank.
pub fn quantile(values: &[f64], q: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = (q * sorted.len() as f64).ceil() as usize;
    Some(sorted[rank.clamp(1, sorted.len()) - 1])
}

/// The median, the 95th percentile and the range of `values`, with `unit`
/// after the median and the percentile unless it is empty.
pub fn summary(values: &[f64], unit: &str) -> String {
    let (Some(median), Some(p95)) = (quantile(values, 0.5), quantile(values, 0.95)) else {
        return "none".to_string();
    };
    let unit = if unit.is_empty() {
        String::new()
    } else {
        format!(" {unit}")
    };
    let min = values.iter().copied().fold(f64::INFINITY, f64::min);
    let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    format!(
        "median {median:.2}{unit}, p95 {p95:.2}{unit} ({min:.2} to {max:.2}, {} values)",
        values.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_quantiles() {
        let values: Vec<f64> = (1..=20).map(f64::from).collect();
        assert_eq!(quantile(&values, 0.5), Some(10.0));
        assert_eq!(quantile(&values, 0.95), Some(19.0));
        assert_eq!(quantile(&values, 1.0), Some(20.0));
        assert_eq!(quantile(&values, 0.0), Some(1.0));
        assert_eq!(quantile(&[3.0], 0.95), Some(3.0));
        assert_eq!(quantile(&[], 0.5), None);
    }

    #[test]
    fn summarizes_values() {
        assert_eq!(
            summary(&[300.0, 100.0, 200.0], "ms"),
            "median 200.00 ms, p95 300.00 ms (100.00 to 300.00, 3 values)"
        );
        assert_eq!(
            summary(&[0.5, 0.25], ""),
            "median 0.25, p95 0.50 (0.25 to 0.50, 2 values)"
        );
        assert_eq!(summary(&[], "ms"), "none");
    }
}
