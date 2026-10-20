//! The seam between the Task API and the pod VMs: the node service drives
//! each pod's processes through a [`PodBackend`].
//!
//! The Task API's state transitions, event order and exits live above this
//! trait, in the front shim and the node service. [`MockBackend`] runs them
//! without VMs, in unit tests and with `SMOLVM_SHIM_MOCK=1`. The engine
//! backend ([`crate::engine`]) boots a smolvm microVM for each pod sandbox
//! and drives its containers through the guest agent over vsock.
//!
//! Derived from smolvm's crates/smolvm-shim (Apache-2.0), with changes.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{watch, Mutex, RwLock};

/// Stdio wiring for a process: containerd-side fifo paths (empty = not wired).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stdio {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stdin: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stdout: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stderr: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub terminal: bool,
}

/// What the pod backend needs to know to create a container or exec process.
#[derive(Debug, Clone)]
pub struct ProcessSpec {
    /// OCI bundle dir on the host (config.json + rootfs mount target).
    pub bundle: String,
    /// Host rootfs path, already mounted from containerd's mounts.
    pub rootfs: String,
    pub stdio: Stdio,
    /// For exec processes: the serialized OCI process spec from containerd.
    pub exec_spec: Option<Vec<u8>>,
    /// For an exec of a probe's command: keeps the first output.
    pub capture: Option<crate::probe::OutputCapture>,
}

/// How a process exited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitInfo {
    pub status: u32,
    /// Unix nanos; 0 = unknown.
    pub exited_at_ns: i64,
    /// The container's init was killed by the guest cgroup OOM killer. Drives a
    /// TaskOOM event so the CRI reports `reason=OOMKilled`. Only the init exit
    /// ever sets this.
    #[serde(default)]
    pub oom: bool,
}

impl ExitInfo {
    /// Sets an unknown exit time to now.
    pub fn stamp(&mut self) {
        if self.exited_at_ns <= 0 {
            self.exited_at_ns = now_ns();
        }
    }
}

/// The time now, in Unix nanoseconds.
pub fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

/// A container's resource usage, as the guest reports it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatsSample {
    pub cpu_usage_ns: u64,
    pub memory_bytes: u64,
}

/// Resolves when the process exits. The node service's exit watcher
/// subscribes to it, and other calls read the exit it already holds.
pub type ExitWatch = watch::Receiver<Option<ExitInfo>>;

/// What the boot of a sandbox made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sandboxed {
    /// The sandbox task's pid. It is synthetic: the VM process changes at
    /// every wake.
    pub pid: u32,
    /// The VM has the Rosetta share, so its VM process must turn TSO on.
    pub rosetta: bool,
    /// Why the pod runs without something it would have had, such as Rosetta
    /// when the node's TSO canary failed.
    pub warning: Option<String>,
}

/// The guest agent's process, counted in the guest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentStats {
    pub pid: u32,
    pub threads: u32,
    pub fds: u32,
}

/// A process of a pod that a backend adopts: one that containerd has, made by
/// a node service before this one.
#[derive(Debug, Clone)]
pub struct Adopted {
    pub id: String,
    pub exec_id: Option<String>,
    pub stdio: Stdio,
    pub pid: u32,
    /// Started, and not known to have exited: its output is pumped again.
    pub running: bool,
    /// The exit that containerd already has.
    pub exit: Option<ExitInfo>,
}

/// Backend operations for one pod (sandbox VM + its containers).
///
/// Process addressing follows the Task API: `(container_id, exec_id)`, where
/// `None` (or an empty exec_id) means the container's init process.
#[async_trait]
pub trait PodBackend: Send + Sync + 'static {
    /// Boot the sandbox VM for this pod. Called once, from the sandbox
    /// container's Create.
    async fn create_sandbox(
        &self,
        id: &str,
        bundle: &str,
        netns: Option<&str>,
    ) -> Result<Sandboxed, String>;

    /// Take over sandbox `id` and its processes `procs`, the sandbox's own
    /// among them, after a restart of the node service, from the node
    /// service's records. It does not touch the VM, which may sleep: the
    /// pumps start with [`PodBackend::resume_pumps`] once the pod is awake.
    async fn adopt(
        &self,
        id: &str,
        bundle: &str,
        netns: Option<&str>,
        procs: Vec<Adopted>,
    ) -> Result<(), String>;

    /// Record the exit of a process that the node service knows without the
    /// guest, such as a kill deferred while the pod sleeps, or 255 when the
    /// VM is lost. An exit already known stays.
    async fn set_exit(&self, id: &str, exec_id: Option<&str>, exit: ExitInfo);

    /// Before a pause: stop every output pump, so that the snapshot holds no
    /// agent connection. Blocking.
    fn pause_pumps(&self) -> Result<(), String>;

    /// Once the pod is awake again, or adopted awake: let every output pump
    /// go on at its cursor, starting one for each process that runs and has
    /// none. Blocking.
    fn resume_pumps(&self);

    /// While the pod sleeps: start a paused pump for each process that runs
    /// and has none, as after a restart of the node service, so that it
    /// attaches at its cursor at the next resume. A paused pump parks
    /// without a thread. Blocking.
    fn park_pumps(&self);

    /// End the output pump of a process whose exit the node service recorded
    /// while the pod sleeps, once it holds no agent connection, so that it
    /// closes containerd's fifos: CRI deletes the task only after their end.
    async fn end_pump(&self, id: &str, exec_id: Option<&str>);

    /// End every output pump: the pod's VM is lost.
    fn end_pumps(&self);

    /// The guest agent's process, counted in the guest.
    async fn agent_stats(&self) -> Result<AgentStats, String>;

    /// Create (but do not start) a workload container in the sandbox VM.
    async fn create_container(&self, id: &str, spec: ProcessSpec) -> Result<u32, String>;

    /// Start a created container's init process, or an exec process.
    async fn start(&self, id: &str, exec_id: Option<&str>) -> Result<u32, String>;

    /// Register an exec process (started later via `start`).
    async fn create_exec(&self, id: &str, exec_id: &str, spec: ProcessSpec) -> Result<(), String>;

    /// Deliver a signal. `all` = process group / whole container.
    async fn kill(
        &self,
        id: &str,
        exec_id: Option<&str>,
        signal: u32,
        all: bool,
    ) -> Result<(), String>;

    /// Watch for a process exit.
    async fn wait_channel(&self, id: &str, exec_id: Option<&str>) -> Result<ExitWatch, String>;

    /// Resize a terminal process's PTY.
    async fn resize_pty(
        &self,
        id: &str,
        exec_id: Option<&str>,
        w: u32,
        h: u32,
    ) -> Result<(), String>;

    /// Close the process's stdin.
    async fn close_io(&self, id: &str, exec_id: Option<&str>) -> Result<(), String>;

    /// Remove a container's resources (after exit), in the guest too. For the
    /// sandbox, forget the pod: the node service deletes its VM.
    async fn delete(&self, id: &str, exec_id: Option<&str>) -> Result<(), String>;

    /// PIDs visible in the container (guest view).
    async fn pids(&self, id: &str) -> Result<Vec<u32>, String>;

    /// The container's resource usage, if available.
    async fn stats(&self, id: &str) -> Result<Option<StatsSample>, String>;
}

// ============================== MockBackend ==============================

/// Pods without VMs, for unit tests and `SMOLVM_SHIM_MOCK=1`: each process is
/// an entry in a map. It exits on SIGKILL or SIGTERM, or when a test finishes
/// it.
#[derive(Default)]
pub struct MockBackend {
    inner: Mutex<HashMap<String, MockProc>>,
    start_gate: RwLock<()>,
    stats: std::sync::Mutex<Option<StatsSample>>,
}

struct MockProc {
    running: bool,
    tx: watch::Sender<Option<ExitInfo>>,
    rx: ExitWatch,
    next_pid: u32,
}

/// The name of process `id`/`exec_id`: the container id, followed by
/// `/exec_id` for an exec process.
pub(crate) fn key(id: &str, exec_id: Option<&str>) -> String {
    match exec_id {
        Some(e) if !e.is_empty() => format!("{id}/{e}"),
        _ => id.to_string(),
    }
}

impl MockBackend {
    /// Whether the mock has the process.
    pub async fn has(&self, id: &str, exec_id: Option<&str>) -> bool {
        self.inner.lock().await.contains_key(&key(id, exec_id))
    }

    async fn insert(&self, k: String, pid: u32) {
        let (tx, rx) = watch::channel(None);
        self.inner.lock().await.insert(
            k,
            MockProc {
                running: false,
                tx,
                rx,
                next_pid: pid,
            },
        );
    }
}

#[cfg(test)]
impl MockBackend {
    /// Test hook: mark a process exited.
    pub async fn finish(&self, id: &str, exec_id: Option<&str>, status: u32) {
        self.finish_at(id, exec_id, status, 1_700_000_000_000_000_000)
            .await;
    }

    /// Test hook: mark a process exited at `exited_at_ns` (0 = unknown).
    pub async fn finish_at(&self, id: &str, exec_id: Option<&str>, status: u32, exited_at_ns: i64) {
        let map = self.inner.lock().await;
        if let Some(p) = map.get(&key(id, exec_id)) {
            let _ = p.tx.send(Some(ExitInfo {
                status,
                exited_at_ns,
                oom: false,
            }));
        }
    }

    /// Test hook: what `stats` reports.
    pub fn set_stats(&self, sample: Option<StatsSample>) {
        *self.stats.lock().unwrap() = sample;
    }

    /// Test hook: `start` waits until the returned guard is dropped.
    pub async fn hold_start(&self) -> tokio::sync::RwLockWriteGuard<'_, ()> {
        self.start_gate.write().await
    }
}

#[async_trait]
impl PodBackend for MockBackend {
    async fn create_sandbox(
        &self,
        id: &str,
        _bundle: &str,
        _netns: Option<&str>,
    ) -> Result<Sandboxed, String> {
        self.insert(key(id, None), 1).await;
        Ok(Sandboxed {
            pid: 1,
            rosetta: false,
            warning: None,
        })
    }

    /// A mock pod outlives the node service only in the same process, which
    /// keeps its processes.
    async fn adopt(
        &self,
        _id: &str,
        _bundle: &str,
        _netns: Option<&str>,
        _procs: Vec<Adopted>,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn set_exit(&self, id: &str, exec_id: Option<&str>, exit: ExitInfo) {
        if let Some(p) = self.inner.lock().await.get(&key(id, exec_id)) {
            p.tx.send_if_modified(|known| {
                let unknown = known.is_none();
                if unknown {
                    *known = Some(exit);
                }
                unknown
            });
        }
    }

    fn pause_pumps(&self) -> Result<(), String> {
        Ok(())
    }

    fn resume_pumps(&self) {}

    fn park_pumps(&self) {}

    async fn end_pump(&self, _id: &str, _exec_id: Option<&str>) {}

    fn end_pumps(&self) {}

    async fn agent_stats(&self) -> Result<AgentStats, String> {
        Err("a mock pod has no guest agent".into())
    }

    async fn create_container(&self, id: &str, _spec: ProcessSpec) -> Result<u32, String> {
        self.insert(key(id, None), 100).await;
        Ok(100)
    }

    async fn start(&self, id: &str, exec_id: Option<&str>) -> Result<u32, String> {
        let _gate = self.start_gate.read().await;
        let mut map = self.inner.lock().await;
        let p = map
            .get_mut(&key(id, exec_id))
            .ok_or_else(|| format!("no such process {}", key(id, exec_id)))?;
        p.running = true;
        Ok(p.next_pid)
    }

    async fn create_exec(&self, id: &str, exec_id: &str, _spec: ProcessSpec) -> Result<(), String> {
        self.insert(key(id, Some(exec_id)), 200).await;
        Ok(())
    }

    async fn kill(
        &self,
        id: &str,
        exec_id: Option<&str>,
        signal: u32,
        _all: bool,
    ) -> Result<(), String> {
        let map = self.inner.lock().await;
        let p = map
            .get(&key(id, exec_id))
            .ok_or_else(|| "no such process".to_string())?;
        // SIGKILL/SIGTERM end the mock process immediately.
        if signal == 9 || signal == 15 {
            let _ = p.tx.send(Some(ExitInfo {
                status: 137,
                exited_at_ns: 1_700_000_000_000_000_000,
                oom: false,
            }));
        }
        Ok(())
    }

    async fn wait_channel(&self, id: &str, exec_id: Option<&str>) -> Result<ExitWatch, String> {
        let map = self.inner.lock().await;
        map.get(&key(id, exec_id))
            .map(|p| p.rx.clone())
            .ok_or_else(|| "no such process".to_string())
    }

    async fn resize_pty(
        &self,
        _id: &str,
        _exec_id: Option<&str>,
        _w: u32,
        _h: u32,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn close_io(&self, _id: &str, _exec_id: Option<&str>) -> Result<(), String> {
        Ok(())
    }

    async fn delete(&self, id: &str, exec_id: Option<&str>) -> Result<(), String> {
        self.inner.lock().await.remove(&key(id, exec_id));
        Ok(())
    }

    async fn pids(&self, _id: &str) -> Result<Vec<u32>, String> {
        Ok(vec![1])
    }

    async fn stats(&self, _id: &str) -> Result<Option<StatsSample>, String> {
        Ok(*self.stats.lock().unwrap())
    }
}
