//! The node service's sessions with the front shim, its requests, and its
//! part in the pods' hibernation.
//!
//! A front shim opens one session for its containerd namespace. The node
//! service keeps, for each namespace, the exits it reported and the front
//! shim has not acked, and repeats them when the session opens again.
//! Requests run concurrently. A mutating request that arrives again while the
//! first is still running waits for the first's outcome.
//!
//! The lifecycle ([`crate::lifecycle`]) owns each pod's state and its durable
//! record, which holds the pod's tasks and execs. A node service that restarts
//! takes its pods from the records, not from the front shim: recovery adopts
//! the VMs that still run and fails those that are gone, whose processes each
//! exit with 255. The session's Hello then claims the pods of its namespace;
//! a pod that it does not have is abandoned, and deleted after
//! [`ABANDONED_AFTER`] unless containerd's delete actions delete it first.
//!
//! A lost pod's sandbox and execs exit at once. Its containers exit when
//! containerd kills them, or after [`HOLD_LIMIT`]: CRI reports the sandbox
//! ready until containerd has deleted the sandbox task, and kubelet restarts
//! a container whose exit it sees in a ready sandbox. That restart fails, and
//! its exit, 128, would become the container's last state in place of 255.
//! kubelet kills the containers once it sees the sandbox not ready, as when
//! a pause container dies under runc, so their exits come after that.
//!
//! A request that needs the guest wakes a sleeping pod first: an exec, a
//! container's create or start, and a signal other than SIGKILL or SIGTERM.
//! The others are answered without a wake.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use log::{debug, info, warn};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, watch};

use super::settings::Settings;
use super::sleep::Sleep;
use super::Backends;
use crate::activity::Flow;
use crate::backend::{now_ns, Adopted, AgentStats, PodBackend, ProcessSpec};
use crate::bundle;
use crate::engine::sandbox_memory;
use crate::lifecycle::{
    cpu_threshold, cpu_window, hibernation_disabled, idle_timeout, keeps_connections,
    kept_tcp_peers, wakes_on_terminate, Activity, AwakeCause, Config, ExecRecord, ExecStatus,
    Hooks, KillOutcome, KillRequest, Lifecycle, LifecycleError, PodIdentity, PodInfo, PodRef,
    PodState, Reason, StdioPaths, TaskKind, TaskRecord, VmExit, VmProcess,
};
use crate::metrics::Text;
use crate::probe::{self, CachedAnswer, ExecCommand, OutputCapture, ProbeExec};
use crate::proto::{
    read_frame, write_frame, Code, Exit, Failure, FromNode, Kind, Mount, Op, OpKey, Outcome,
    ProcId, Reply, StatsSample, Status, Stdio, TaskInfo, ToNode, MIN_VERSION, VERSION,
};
use crate::sync::{lock, wait_timeout};
use crate::tombstone::{self, Tombstone};

/// How long a pod that no front shim claims stays.
pub(super) const ABANDONED_AFTER: Duration = Duration::from_secs(300);

/// How long a node service waits for the state directory's lock, which the
/// one before it may still hold for a moment.
const LOCK_WAIT: Duration = Duration::from_secs(10);

/// How long a hook that recovery calls waits for the node service to finish
/// opening.
const READY_WAIT: Duration = Duration::from_secs(60);

/// How long a lost pod's container exits wait for a kill. kubelet's kill
/// comes within about a second.
const HOLD_LIMIT: Duration = Duration::from_secs(10);

/// CRI annotations on a sandbox: the pod's namespace, name and uid.
const ANN_POD_NAMESPACE: &str = "io.kubernetes.cri.sandbox-namespace";
const ANN_POD_NAME: &str = "io.kubernetes.cri.sandbox-name";
const ANN_POD_UID: &str = "io.kubernetes.cri.sandbox-uid";

/// Starts the parts of a node service that wake pods on traffic and put
/// idle pods to sleep, once its lifecycle is open.
pub type StartSleep = Box<dyn FnOnce(Lifecycle) -> Result<Arc<Sleep>, String> + Send>;

/// The node service. Cloning it gives another handle to the same service.
#[derive(Clone)]
pub struct Node {
    inner: Arc<Inner>,
}

struct Inner {
    backends: Arc<dyn Backends>,
    lifecycle: Lifecycle,
    /// Wakes on traffic and idle sleep; none for mock pods and tests.
    sleep: Option<Arc<Sleep>>,
    tombstones: PathBuf,
    abandon_after: Duration,
    pods: Mutex<HashMap<String, Arc<Pod>>>,
    namespaces: Mutex<HashMap<String, Namespace>>,
    /// The container exits of lost pods, by sandbox id, until containerd
    /// kills the containers.
    held: Mutex<HashMap<String, Held>>,
    inflight: Mutex<HashMap<OpKey, watch::Receiver<Option<Outcome>>>>,
    sessions: AtomicU64,
}

/// A lost pod's container exits, not yet reported.
struct Held {
    namespace: String,
    exits: BTreeMap<ProcId, Exit>,
}

#[derive(Default)]
struct Namespace {
    /// The open sessions' outgoing frames, by session number. There is one,
    /// but for a moment there can be two: the `start` actions of two tasks
    /// that start together can each start a front shim daemon, and one of
    /// them ends.
    sessions: BTreeMap<u64, mpsc::UnboundedSender<FromNode>>,
    /// Exits reported but not acked.
    unacked: BTreeMap<(String, ProcId), Exit>,
}

/// A pod: its sandbox VM's backend and the processes containerd made in it.
struct Pod {
    id: String,
    namespace: String,
    netns: Option<String>,
    backend: Arc<dyn PodBackend>,
    procs: Mutex<HashMap<ProcId, NodeProc>>,
    /// Why the pod's VM is lost. Every process of the pod has exited with 255.
    lost: Mutex<Option<String>>,
    /// Since when no front shim has claimed the pod.
    abandoned_at: Mutex<Option<Instant>>,
    /// The VM process that a watcher watches.
    watched_vm: Mutex<Option<VmProcess>>,
    /// A standalone pod ([`crate::standalone`]): no sandbox task, and its
    /// container has the pod's id.
    standalone: bool,
}

#[derive(Debug, Clone)]
struct NodeProc {
    kind: Kind,
    /// The container's bundle, whose rootfs this service mounted.
    bundle: String,
    stdio: Stdio,
    pid: u32,
    started: bool,
    /// An exit watcher runs for it.
    watched: bool,
    exit: Option<Exit>,
    /// An exec of a probe's command: its result is cached when it exits.
    probe: Option<(ProbeExec, OutputCapture)>,
    /// An exec answered from the probe cache, with no process in the guest.
    cached: Option<CachedAnswer>,
}

impl NodeProc {
    fn new(kind: Kind, bundle: String, stdio: Stdio, pid: u32) -> NodeProc {
        NodeProc {
            kind,
            bundle,
            stdio,
            pid,
            started: false,
            watched: false,
            exit: None,
            probe: None,
            cached: None,
        }
    }
}

impl Pod {
    fn procs(&self) -> MutexGuard<'_, HashMap<ProcId, NodeProc>> {
        lock(&self.procs)
    }

    fn proc(&self, proc: &ProcId) -> Option<NodeProc> {
        self.procs().get(proc).cloned()
    }

    fn proc_or_not_found(&self, proc: &ProcId) -> Result<NodeProc, Failure> {
        self.proc(proc)
            .ok_or_else(|| Failure::new(Code::NotFound, format!("process {proc}")))
    }

    fn is_lost(&self) -> bool {
        lock(&self.lost).is_some()
    }

    /// Fail unless the pod's VM is still there.
    fn require_vm(&self) -> Result<(), Failure> {
        match lock(&self.lost).as_ref() {
            None => Ok(()),
            Some(why) => Err(Failure::new(
                Code::FailedPrecondition,
                format!("the VM of pod sandbox {} is gone: {why}", self.id),
            )),
        }
    }

    fn is_sandbox(&self, proc: &ProcId) -> bool {
        !self.standalone && proc.id == self.id && proc.exec.is_empty()
    }

    /// The container of a standalone pod.
    fn is_standalone_container(&self, proc: &ProcId) -> bool {
        self.standalone && proc.id == self.id && proc.exec.is_empty()
    }
}

fn unknown(message: impl std::fmt::Display) -> Failure {
    Failure::unknown(message)
}

/// A lifecycle error as the front shim gets it.
fn life_failure(e: LifecycleError) -> Failure {
    match e {
        LifecycleError::NotFound(_) => Failure::new(Code::NotFound, e),
        LifecycleError::Failed(_) => Failure::new(Code::FailedPrecondition, e),
        e => unknown(e),
    }
}

fn stdio_paths(stdio: &Stdio) -> StdioPaths {
    StdioPaths {
        stdin: stdio.stdin.clone(),
        stdout: stdio.stdout.clone(),
        stderr: stdio.stderr.clone(),
        terminal: stdio.terminal,
    }
}

fn stdio_of(paths: &StdioPaths) -> Stdio {
    Stdio {
        stdin: paths.stdin.clone(),
        stdout: paths.stdout.clone(),
        stderr: paths.stderr.clone(),
        terminal: paths.terminal,
    }
}

fn exit_of(status: u32) -> Exit {
    Exit {
        status,
        exited_at_ns: 0,
        oom: false,
    }
}

/// Removes a request from the requests in flight when it ends, also when it
/// is cancelled.
struct InflightGuard<'a> {
    node: &'a Node,
    key: OpKey,
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        lock(&self.node.inner.inflight).remove(&self.key);
    }
}

/// The lifecycle's hooks into the node service. Recovery calls them as soon
/// as the lifecycle opens, so they wait until the node service is ready.
struct NodeHooks {
    runtime: tokio::runtime::Handle,
    node: Mutex<Option<Weak<Inner>>>,
    ready: Condvar,
}

impl NodeHooks {
    fn new() -> NodeHooks {
        NodeHooks {
            runtime: tokio::runtime::Handle::current(),
            node: Mutex::new(None),
            ready: Condvar::new(),
        }
    }

    fn set(&self, node: &Node) {
        *lock(&self.node) = Some(Arc::downgrade(&node.inner));
        self.ready.notify_all();
    }

    fn node(&self) -> Option<Node> {
        let deadline = Instant::now() + READY_WAIT;
        let mut node = lock(&self.node);
        while node.is_none() {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                warn!("a lifecycle hook found no node service");
                return None;
            }
            node = wait_timeout(&self.ready, node, left);
        }
        node.as_ref()?.upgrade().map(|inner| Node { inner })
    }

    fn sleep(&self) -> Option<Arc<Sleep>> {
        self.node().and_then(|node| node.inner.sleep.clone())
    }
}

impl Hooks for NodeHooks {
    fn may_wake(&self, pod: &PodInfo, waiting: &dyn Fn(&str)) -> Result<(), String> {
        match self.sleep() {
            Some(sleep) => sleep.may_wake(pod, waiting),
            None => Ok(()),
        }
    }

    fn pausing(&self, pod: &PodInfo, reason: &Reason) -> Result<(), String> {
        match self.sleep() {
            Some(sleep) => sleep.pausing(pod, reason),
            None => Ok(()),
        }
    }

    fn release(&self, pod: &PodInfo) -> Result<Option<Duration>, String> {
        match self.sleep() {
            Some(sleep) => sleep.release(pod),
            None => Ok(None),
        }
    }

    fn stop_pumps(&self, pod: &PodInfo) -> Result<(), String> {
        let node = self.node().ok_or("the node service is gone")?;
        match node.pod(&pod.identity.sandbox_id) {
            Ok(pod) => pod.backend.pause_pumps(),
            Err(_) => Ok(()),
        }
    }

    fn before_release(&self, pod: &PodInfo) -> Result<Option<String>, String> {
        let node = self.node().ok_or("the node service is gone")?;
        let id = pod.identity.sandbox_id.clone();
        self.runtime.block_on(node.delete_in_guest(&id))?;
        Ok(node.refresh_trust(&id))
    }

    fn asleep(&self, pod: &PodInfo) {
        if let Some(node) = self.node() {
            if let Some(sleep) = &node.inner.sleep {
                sleep.asleep(pod);
            }
            if let Ok(pod) = node.pod(&pod.identity.sandbox_id) {
                pod.backend.park_pumps();
            }
        }
        // A pause's large buffers are mapped and unmapped (see
        // `node::tune_allocator`). Give back the free pages between the
        // smaller blocks that it freed.
        // SAFETY: malloc_trim has no preconditions.
        unsafe { libc::malloc_trim(0) };
    }

    fn awake(&self, pod: &PodInfo, cause: AwakeCause) {
        if let Some(node) = self.node() {
            self.runtime.block_on(node.pod_awake(pod, cause));
        }
    }

    fn failed(&self, pod: &PodInfo, reason: &str) {
        if let Some(node) = self.node() {
            if let Some(sleep) = &node.inner.sleep {
                sleep.gone(pod);
            }
            let id = pod.identity.sandbox_id.clone();
            self.runtime.block_on(node.pod_failed(&id, reason));
        }
    }

    fn deleted(&self, pod: &PodInfo) {
        if let Some(node) = self.node() {
            node.pod_deleted(pod);
        }
    }
}

impl Node {
    /// Opens the node service with its state in `state_dir`: the lifecycle,
    /// whose recovery starts at once, and a pod for each record.
    pub async fn open(
        backends: Arc<dyn Backends>,
        state_dir: PathBuf,
        abandon_after: Duration,
    ) -> Result<Node, String> {
        Node::open_with(backends, state_dir, abandon_after, None).await
    }

    /// [`Node::open`], with the wakes on traffic and the idle sleep that
    /// `sleep` starts.
    pub async fn open_with(
        backends: Arc<dyn Backends>,
        state_dir: PathBuf,
        abandon_after: Duration,
        sleep: Option<StartSleep>,
    ) -> Result<Node, String> {
        let hooks = Arc::new(NodeHooks::new());
        let config = Config::new(&state_dir)?;
        let lifecycle = open_lifecycle(config, backends.vm(), hooks.clone()).await?;
        let sleep = match sleep {
            Some(start) => Some(start(lifecycle.clone())?),
            None => None,
        };
        let node = Node {
            inner: Arc::new(Inner {
                backends,
                lifecycle,
                sleep,
                tombstones: tombstone::dir(&state_dir),
                abandon_after,
                pods: Mutex::new(HashMap::new()),
                namespaces: Mutex::new(HashMap::new()),
                held: Mutex::new(HashMap::new()),
                inflight: Mutex::new(HashMap::new()),
                sessions: AtomicU64::new(1),
            }),
        };
        node.restore().await;
        hooks.set(&node);
        Ok(node)
    }

    /// The node service: [`Node::open`] with the default time before an
    /// abandoned pod is deleted, and wakes on traffic and idle sleep with
    /// `settings`.
    pub async fn open_default(
        backends: Arc<dyn Backends>,
        state_dir: PathBuf,
        settings: &Settings,
    ) -> Result<Node, String> {
        let settings = settings.clone();
        let start: StartSleep = Box::new(move |lifecycle| Sleep::start(&settings, lifecycle));
        Node::open_with(backends, state_dir, ABANDONED_AFTER, Some(start)).await
    }

    pub fn lifecycle(&self) -> &Lifecycle {
        &self.inner.lifecycle
    }

    /// The traffic between pods, from the idle loop's conntrack dumps.
    pub fn flows(&self) -> Vec<Flow> {
        self.inner
            .sleep
            .as_ref()
            .map(|sleep| sleep.flows())
            .unwrap_or_default()
    }

    /// The metrics of the parts that wake pods and put them to sleep.
    pub fn render_metrics(&self, text: &mut Text) {
        if let Some(sleep) = &self.inner.sleep {
            sleep.render(text);
        }
    }

    /// Ends the lifecycle at once, as a crash would, and unlocks the state
    /// directory.
    #[cfg(test)]
    pub fn shutdown(&self) {
        self.inner.lifecycle.shutdown();
    }

    /// The end of the process, at a SIGTERM: closes the catchers' sockets at
    /// once, and leaves every pod's hold and record as a crash would.
    /// Blocking.
    pub fn stop(&self) {
        if let Some(sleep) = &self.inner.sleep {
            sleep.stop();
        }
    }

    /// Waits until recovery has settled every pod, deletes the sandboxes of
    /// the tombstones left from before, then accepts connections until the
    /// listener fails for good.
    pub async fn serve(&self, listener: UnixListener) {
        self.inner.lifecycle.settled().await;
        if let Some(sleep) = &self.inner.sleep {
            sleep.start_idle_loop();
        }
        self.drop_tombstoned();
        tokio::spawn(self.clone().sweep_abandoned());
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let node = self.clone();
                    tokio::spawn(async move { node.connection(stream).await });
                }
                Err(e) => {
                    warn!("accept a connection: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    /// A pod for each record, adopted without touching its VM, and its
    /// catcher by its state.
    async fn restore(&self) {
        let mut highest = 0;
        let mut caught = Vec::new();
        for info in self.inner.lifecycle.list() {
            let id = info.identity.sandbox_id.clone();
            let Some(record) = self.inner.lifecycle.record(&id) else {
                continue;
            };
            // Recovery drops the record of a boot that a crash cut short,
            // and finishes a delete.
            if matches!(
                record.state,
                PodState::Creating | PodState::Deleting | PodState::Deleted
            ) {
                continue;
            }
            let mut procs = HashMap::new();
            for task in record.tasks.values() {
                let kind = match task.kind {
                    TaskKind::Sandbox => Kind::Sandbox,
                    TaskKind::Container => Kind::Container,
                };
                let bundle = task.bundle.clone();
                let mut proc = NodeProc::new(kind, bundle, stdio_of(&task.stdio), task.pid);
                proc.started = task.started;
                proc.exit = task.exit_status.map(exit_of);
                procs.insert(ProcId::init(&task.id), proc);
            }
            for exec in record.execs.values() {
                let mut proc = NodeProc::new(Kind::Exec, String::new(), stdio_of(&exec.stdio), 0);
                proc.pid = exec.pid;
                proc.started = exec.status != ExecStatus::Created;
                proc.exit = match exec.status {
                    ExecStatus::Exited => Some(exit_of(exec.exit_status.unwrap_or(255))),
                    _ => None,
                };
                procs.insert(ProcId::new(&exec.container, &exec.exec), proc);
            }
            highest = procs.values().map(|p| p.pid).fold(highest, u32::max);
            let bundle = record
                .tasks
                .get(&id)
                .map(|t| t.bundle.clone())
                .unwrap_or_default();
            let backend = self.inner.backends.pod(&id);
            let adopted = procs
                .iter()
                .map(|(proc, p)| Adopted {
                    id: proc.id.clone(),
                    exec_id: proc.exec_opt().map(str::to_string),
                    stdio: p.stdio.clone(),
                    pid: p.pid,
                    running: p.started && p.exit.is_none(),
                    exit: p.exit,
                })
                .collect();
            let netns = record.identity.netns.clone();
            let standalone = record.identity.standalone;
            caught.push((record.identity.clone(), record.state, record.wake_failures));
            let adopting = backend.adopt(&id, &bundle, netns.as_deref(), standalone, adopted);
            if let Err(e) = adopting.await {
                warn!("adopt pod sandbox {id}: {e}");
            }
            let lost = (record.state == PodState::Failed)
                .then(|| record.failure.clone().unwrap_or_default());
            let pod = Arc::new(Pod {
                id: id.clone(),
                namespace: record.identity.task_namespace.clone(),
                netns,
                backend,
                procs: Mutex::new(procs),
                lost: Mutex::new(lost),
                abandoned_at: Mutex::new(None),
                watched_vm: Mutex::new(None),
                standalone,
            });
            let running: Vec<ProcId> = pod
                .procs()
                .iter()
                .filter(|(_, p)| p.started && p.exit.is_none())
                .map(|(proc, _)| proc.clone())
                .collect();
            lock(&self.inner.pods).insert(id.clone(), pod.clone());
            for proc in running {
                self.watch_exit(&pod, proc);
            }
            info!("restored pod sandbox {id}, {} in its record", record.state);
        }
        if highest > 0 {
            self.inner.backends.reserve_pids(highest);
        }
        if let Some(sleep) = self.inner.sleep.clone() {
            let lifecycle = self.inner.lifecycle.clone();
            let restored = tokio::task::spawn_blocking(move || {
                for (mut identity, state, wake_failures) in caught {
                    let known = identity.ip.is_some() && identity.mac.is_some();
                    if let (false, Some(netns)) = (known, identity.netns.clone()) {
                        if let Some((ip, mac)) = record_network(&lifecycle, &identity, &netns) {
                            (identity.ip, identity.mac) = (Some(ip), Some(mac));
                        }
                    }
                    sleep.restore(&identity, state, wake_failures);
                }
            })
            .await;
            if let Err(e) = restored {
                warn!("restore the catchers: {e}");
            }
        }
    }

    async fn connection(&self, stream: UnixStream) {
        let (mut rd, mut wr) = stream.into_split();
        let first = match read_frame::<_, ToNode>(&mut rd).await {
            Ok(Some(first)) => first,
            Ok(None) => return,
            Err(e) => {
                warn!("read the first frame of a connection: {e}");
                return;
            }
        };
        match first {
            ToNode::Call { version, id, op } => {
                let outcome = if accepts(version) {
                    self.handle(None, op).await
                } else {
                    Err(Failure::new(
                        Code::FailedPrecondition,
                        version_refusal(version),
                    ))
                };
                let response = FromNode::Response {
                    id,
                    answer: outcome.into(),
                };
                if let Err(e) = write_frame(&mut wr, &response).await {
                    debug!("answer a call: {e}");
                }
            }
            ToNode::Hello {
                version,
                namespace,
                tasks,
            } => self.session(rd, wr, version, namespace, tasks).await,
            other => warn!("a connection began with {other:?}, not with Hello or Call"),
        }
    }

    async fn session(
        &self,
        mut rd: OwnedReadHalf,
        mut wr: OwnedWriteHalf,
        version: u32,
        namespace: String,
        tasks: Vec<TaskInfo>,
    ) {
        if !accepts(version) {
            let refused = FromNode::Refused {
                message: version_refusal(version),
            };
            let _ = write_frame(&mut wr, &refused).await;
            return;
        }
        let (tx, mut rx) = mpsc::unbounded_channel::<FromNode>();
        let writer = tokio::spawn(async move {
            while let Some(frame) = rx.recv().await {
                if let Err(e) = write_frame(&mut wr, &frame).await {
                    debug!("write to the front shim: {e}");
                    break;
                }
            }
        });
        let _ = tx.send(FromNode::Welcome { version });
        let number = self.inner.sessions.fetch_add(1, Ordering::Relaxed);
        info!(
            "session {number} of namespace {namespace} opened with {} tasks",
            tasks.len()
        );
        self.resync(&namespace, &tasks, number, &tx);
        loop {
            match read_frame::<_, ToNode>(&mut rd).await {
                Ok(Some(ToNode::Request { id, op })) => {
                    let node = self.clone();
                    let tx = tx.clone();
                    let namespace = namespace.clone();
                    tokio::spawn(async move {
                        let outcome = node.handle(Some(&namespace), op).await;
                        let _ = tx.send(FromNode::Response {
                            id,
                            answer: outcome.into(),
                        });
                    });
                }
                Ok(Some(ToNode::Ack { pod, proc })) => {
                    if let Some(ns) = lock(&self.inner.namespaces).get_mut(&namespace) {
                        ns.unacked.remove(&(pod, proc));
                    }
                }
                Ok(Some(other)) => {
                    warn!("session {number}: unexpected frame {other:?}");
                    break;
                }
                Ok(None) => break,
                Err(e) => {
                    warn!("session {number}: {e}");
                    break;
                }
            }
        }
        info!("session {number} of namespace {namespace} closed");
        if let Some(ns) = lock(&self.inner.namespaces).get_mut(&namespace) {
            ns.sessions.remove(&number);
        }
        writer.abort();
    }

    /// Bring the namespace's pods in line with the front shim's tasks, and
    /// repeat the exits it has not recorded.
    fn resync(
        &self,
        namespace: &str,
        tasks: &[TaskInfo],
        number: u64,
        tx: &mpsc::UnboundedSender<FromNode>,
    ) {
        if let Some(highest) = tasks.iter().map(|t| t.pid).max() {
            self.inner.backends.reserve_pids(highest);
        }
        let mut claimed = HashSet::new();
        let mut known = Vec::new();
        let mut lost = Vec::new();
        for task in tasks {
            let pod = self.pod(&task.pod).ok();
            if let Some(pod) = &pod {
                if claimed.insert(pod.id.clone()) {
                    *lock(&pod.abandoned_at) = None;
                }
            }
            if task.status == Status::Stopped || self.is_held(&task.pod, &task.proc) {
                continue;
            }
            let key = (task.pod.clone(), task.proc.clone());
            match pod {
                Some(pod) => {
                    if let Some(exit) = pod.proc(&task.proc).and_then(|p| p.exit) {
                        known.push((key, exit));
                    }
                }
                // No record names the pod: its VM is gone.
                None => lost.push(key),
            }
        }
        for pod in lock(&self.inner.pods).values() {
            if pod.namespace == namespace && !claimed.contains(&pod.id) {
                let mut abandoned_at = lock(&pod.abandoned_at);
                if abandoned_at.is_none() {
                    info!("pod sandbox {} has no task in the front shim", pod.id);
                    *abandoned_at = Some(Instant::now());
                }
            }
        }

        let waiting: HashSet<(String, ProcId)> = tasks
            .iter()
            .filter(|t| t.status != Status::Stopped)
            .map(|t| (t.pod.clone(), t.proc.clone()))
            .collect();
        let mut namespaces = lock(&self.inner.namespaces);
        let ns = namespaces.entry(namespace.to_string()).or_default();
        // An exit that the front shim has, or of a task it no longer has, is done.
        ns.unacked.retain(|key, _| waiting.contains(key));
        for (key, exit) in known {
            ns.unacked.entry(key).or_insert(exit);
        }
        let unknown = Exit {
            status: 255,
            exited_at_ns: now_ns(),
            oom: false,
        };
        for key in lost {
            ns.unacked.entry(key).or_insert(unknown);
        }
        for ((pod, proc), exit) in &ns.unacked {
            let _ = tx.send(FromNode::Exit {
                pod: pod.clone(),
                proc: proc.clone(),
                exit: *exit,
            });
        }
        ns.sessions.insert(number, tx.clone());
    }

    /// Handle a request. `namespace` is that of the session, if it came in one.
    pub async fn handle(&self, namespace: Option<&str>, op: Op) -> Outcome {
        let Some(key) = op.key() else {
            return self.guarded(namespace, op).await;
        };
        let (leader, mut rx) = {
            let mut inflight = lock(&self.inner.inflight);
            match inflight.get(&key) {
                Some(rx) => (None, rx.clone()),
                None => {
                    let (tx, rx) = watch::channel(None);
                    inflight.insert(key.clone(), rx.clone());
                    (Some(tx), rx)
                }
            }
        };
        let Some(tx) = leader else {
            loop {
                if let Some(outcome) = rx.borrow_and_update().clone() {
                    return outcome;
                }
                if rx.changed().await.is_err() {
                    return Err(Failure::new(
                        Code::Internal,
                        "the same request, running already, ended without an answer",
                    ));
                }
            }
        };
        let guard = InflightGuard { node: self, key };
        let outcome = self.guarded(namespace, op).await;
        drop(guard);
        tx.send_replace(Some(outcome.clone()));
        outcome
    }

    async fn guarded(&self, namespace: Option<&str>, op: Op) -> Outcome {
        let name = op.name();
        match crate::panics::catch(self.dispatch(namespace, op)).await {
            Ok(outcome) => outcome,
            Err(panic) => Err(Failure::new(
                Code::Internal,
                format!("the node service failed in {name}: {panic}"),
            )),
        }
    }

    async fn dispatch(&self, namespace: Option<&str>, op: Op) -> Outcome {
        // A request in a session shows that the front shim has the pod.
        let mut abandoned = false;
        if namespace.is_some() {
            if let Some(pod) = lock(&self.inner.pods).get(op.pod()) {
                abandoned = lock(&pod.abandoned_at).take().is_some();
            }
        }
        match op {
            Op::CreateSandbox { pod, bundle, netns } => {
                let namespace = namespace.ok_or_else(|| {
                    Failure::new(Code::FailedPrecondition, "a sandbox needs a session")
                })?;
                self.create_sandbox(namespace, pod, bundle, netns).await
            }
            Op::CreateStandalone {
                pod,
                bundle,
                rootfs,
                stdio,
            } => {
                let namespace = namespace.ok_or_else(|| {
                    Failure::new(Code::FailedPrecondition, "a container needs a session")
                })?;
                self.create_standalone(namespace, pod, bundle, rootfs, stdio, abandoned)
                    .await
            }
            Op::CreateContainer {
                pod,
                id,
                bundle,
                rootfs,
                stdio,
            } => self.create_container(&pod, id, bundle, rootfs, stdio).await,
            Op::Exec {
                pod,
                id,
                exec,
                spec,
                stdio,
            } => self.exec(&pod, id, exec, spec, stdio).await,
            Op::Start { pod, proc } => self.start(&pod, proc).await,
            Op::Kill {
                pod,
                proc,
                signal,
                all,
            } => self.kill(&pod, proc, signal, all).await,
            Op::CloseIo { pod, proc } => {
                let pod = self.pod(&pod)?;
                if !pod.is_lost() && self.is_awake(&pod.id) && !is_cached(&pod, &proc) {
                    pod.backend
                        .close_io(&proc.id, proc.exec_opt())
                        .await
                        .map_err(unknown)?;
                }
                Ok(Reply::Done)
            }
            Op::ResizePty {
                pod,
                proc,
                width,
                height,
            } => {
                let pod = self.pod(&pod)?;
                if !pod.is_lost() && self.is_awake(&pod.id) && !is_cached(&pod, &proc) {
                    pod.backend
                        .resize_pty(&proc.id, proc.exec_opt(), width, height)
                        .await
                        .map_err(unknown)?;
                }
                Ok(Reply::Done)
            }
            Op::Delete { pod, proc } => self.delete(&pod, proc).await,
            Op::Pids { pod, id } => {
                let pod = self.pod(&pod)?;
                pod.require_vm()?;
                if !self.is_awake(&pod.id) {
                    return Err(Failure::new(
                        Code::FailedPrecondition,
                        format!("pod sandbox {} is not awake", pod.id),
                    ));
                }
                let pids = pod.backend.pids(&id).await.map_err(unknown)?;
                Ok(Reply::Pids { pids })
            }
            Op::Stats { pod, id } => {
                let pod = self.pod(&pod)?;
                pod.require_vm()?;
                let memory_mib = self
                    .inner
                    .lifecycle
                    .get(&PodRef::sandbox(&pod.id))
                    .and_then(|info| info.identity.memory_mib)
                    .unwrap_or(crate::engine::DEFAULT_SANDBOX_MEMORY_MIB);
                // While the pod sleeps: memory 0, and the front shim keeps
                // the CPU total, which never goes back.
                let mut sample = if !self.is_awake(&pod.id) {
                    let container = id != pod.id || pod.standalone;
                    container.then_some(StatsSample::default())
                } else {
                    pod.backend.stats(&id).await.map_err(unknown)?
                };
                if let Some(sample) = &mut sample {
                    sample.memory_limit_bytes = Some(u64::from(memory_mib) * 1024 * 1024);
                }
                Ok(Reply::Stats { sample })
            }
            Op::DropSandbox { pod, netns } => self.drop_sandbox(&pod, netns).await,
        }
    }

    fn pod(&self, id: &str) -> Result<Arc<Pod>, Failure> {
        lock(&self.inner.pods)
            .get(id)
            .cloned()
            .ok_or_else(|| Failure::new(Code::NotFound, "no sandbox created for this pod"))
    }

    fn state(&self, id: &str) -> Option<PodState> {
        self.inner
            .lifecycle
            .get(&PodRef::sandbox(id))
            .map(|info| info.state)
    }

    fn is_awake(&self, id: &str) -> bool {
        self.state(id) == Some(PodState::Awake)
    }

    /// Records a task in the pod's record. A failure leaves the request
    /// done, as containerd sees it.
    fn put_task(&self, pod: &Pod, proc: &ProcId, p: &NodeProc) {
        let kind = match p.kind {
            Kind::Sandbox => TaskKind::Sandbox,
            _ => TaskKind::Container,
        };
        let task = TaskRecord {
            id: proc.id.clone(),
            kind,
            pid: p.pid,
            // The sandbox's bundle holds the spec that an adoption reads.
            bundle: p.bundle.clone(),
            stdio: stdio_paths(&p.stdio),
            started: p.started,
            exit_status: p.exit.map(|e| e.status),
        };
        if let Err(e) = self.inner.lifecycle.put_task(&pod.id, task) {
            warn!("record task {proc} of pod sandbox {}: {e}", pod.id);
        }
    }

    fn put_exec(&self, pod: &Pod, proc: &ProcId, p: &NodeProc) {
        let status = match (p.exit, p.started) {
            (Some(_), _) => ExecStatus::Exited,
            (None, true) => ExecStatus::Running,
            (None, false) => ExecStatus::Created,
        };
        let exec = ExecRecord {
            container: proc.id.clone(),
            exec: proc.exec.clone(),
            pid: p.pid,
            stdio: stdio_paths(&p.stdio),
            status,
            exit_status: p.exit.map(|e| e.status),
        };
        if let Err(e) = self.inner.lifecycle.put_exec(&pod.id, exec) {
            warn!("record exec {proc} of pod sandbox {}: {e}", pod.id);
        }
    }

    fn record(&self, pod: &Pod, proc: &ProcId) {
        if let Some(p) = pod.proc(proc) {
            match p.kind {
                Kind::Exec => self.put_exec(pod, proc, &p),
                _ => self.put_task(pod, proc, &p),
            }
        }
    }

    async fn create_sandbox(
        &self,
        namespace: &str,
        id: String,
        bundle: String,
        netns: Option<String>,
    ) -> Outcome {
        if let Ok(pod) = self.pod(&id) {
            pod.require_vm()?;
            let pid = pod.proc(&ProcId::init(&id)).map_or(0, |p| p.pid);
            return Ok(Reply::Pid { pid });
        }
        let (_, pid) = self.create_pod(namespace, id, bundle, netns, false).await?;
        Ok(Reply::Pid { pid })
    }

    /// Creates a standalone pod ([`crate::standalone`]): its network
    /// namespace and network hooks, its VM, and its container, which has the
    /// pod's id. A create of a pod that no front shim claims replaces it:
    /// containerd gives a new task the id of one whose front shim died.
    async fn create_standalone(
        &self,
        namespace: &str,
        id: String,
        bundle: String,
        rootfs: Vec<Mount>,
        stdio: Stdio,
        abandoned: bool,
    ) -> Outcome {
        if let Ok(pod) = self.pod(&id) {
            if !abandoned {
                pod.require_vm()?;
                let pid = pod.proc(&ProcId::init(&id)).map_or(0, |p| p.pid);
                return Ok(Reply::Pid { pid });
            }
            info!("container {id} is created again: deleting the pod that no front shim claims");
            self.delete_sandbox(&pod).await?;
        }
        let spec = {
            let bundle = bundle.clone();
            tokio::task::spawn_blocking(move || bundle::read_spec(&bundle))
                .await
                .map_err(unknown)?
        }
        .ok_or_else(|| unknown(format!("container {id} has no readable OCI spec")))?;
        crate::standalone::check_network(&spec)
            .map_err(|e| Failure::new(Code::FailedPrecondition, e))?;
        let netns = {
            let (backends, owned, bundle) =
                (self.inner.backends.clone(), id.clone(), bundle.clone());
            tokio::task::spawn_blocking(move || backends.standalone_netns(&owned, &bundle, &spec))
                .await
                .map_err(unknown)?
                .map_err(|e| unknown(format!("container {id}: {e}")))?
        };
        let created = self
            .create_pod(namespace, id.clone(), bundle.clone(), netns.clone(), true)
            .await;
        let pod = match created {
            Ok((pod, _)) => pod,
            Err(e) => {
                let backends = self.inner.backends.clone();
                let owned = id.clone();
                let cleaned = tokio::task::spawn_blocking(move || {
                    backends.after_delete(&owned, netns.as_deref())
                });
                if let Err(join) = cleaned.await {
                    warn!("remove the network of container {id}: {join}");
                }
                return Err(e);
            }
        };
        match self
            .create_container(&id, id.clone(), bundle, rootfs, stdio)
            .await
        {
            Ok(reply) => Ok(reply),
            Err(e) => {
                if let Err(deleted) = self.delete_sandbox(&pod).await {
                    warn!("delete the pod of container {id}, whose create failed: {deleted}");
                }
                Err(e)
            }
        }
    }

    /// Boots a pod's VM through the lifecycle, and gives the node service the
    /// pod, with its sandbox task unless it is standalone. Returns the pod and
    /// the sandbox's pid.
    async fn create_pod(
        &self,
        namespace: &str,
        id: String,
        bundle: String,
        netns: Option<String>,
        standalone: bool,
    ) -> Result<(Arc<Pod>, u32), Failure> {
        let identity = {
            let (namespace, id, bundle, netns) = (
                namespace.to_string(),
                id.clone(),
                bundle.clone(),
                netns.clone(),
            );
            tokio::task::spawn_blocking(move || {
                pod_identity(&namespace, &id, &bundle, netns, standalone)
            })
            .await
            .map_err(unknown)?
        };
        let backend = self.inner.backends.pod(&id);
        let (booted, made) = oneshot::channel();
        let networked = self.inner.sleep.is_some();
        let boot = {
            let (backend, lifecycle) = (backend.clone(), self.inner.lifecycle.clone());
            let (id, bundle, netns) = (id.clone(), bundle.clone(), netns.clone());
            async move {
                let sandboxed = backend
                    .create_sandbox(&id, &bundle, netns.as_deref(), standalone)
                    .await?;
                lifecycle
                    .set_rosetta(&id, sandboxed.rosetta)
                    .map_err(|e| e.to_string())?;
                if let (true, Some(netns)) = (networked, netns.clone()) {
                    let (lifecycle, sandbox) = (lifecycle.clone(), id.clone());
                    let recorded = tokio::task::spawn_blocking(move || {
                        let identity = lifecycle.record(&sandbox).map(|r| r.identity);
                        identity.and_then(|i| record_network(&lifecycle, &i, &netns))
                    })
                    .await;
                    if let Err(e) = recorded {
                        warn!("record the network of pod sandbox {id}: {e}");
                    }
                }
                let warning = sandboxed.warning.clone();
                let _ = booted.send(sandboxed.pid);
                Ok(warning)
            }
        };
        let info = self
            .inner
            .lifecycle
            .create(identity, boot)
            .await
            .map_err(life_failure)?;
        let pid = made.await.map_err(|_| {
            unknown(format!(
                "pod sandbox {id} has a record, but no node service task"
            ))
        })?;
        // A standalone pod has no sandbox task.
        let proc = ProcId::init(&id);
        let sandbox =
            (!standalone).then(|| NodeProc::new(Kind::Sandbox, bundle, Stdio::default(), pid));
        let procs = sandbox.iter().map(|s| (proc.clone(), s.clone())).collect();
        let pod = Arc::new(Pod {
            id: id.clone(),
            namespace: namespace.to_string(),
            netns,
            backend,
            procs: Mutex::new(procs),
            lost: Mutex::new(None),
            abandoned_at: Mutex::new(None),
            watched_vm: Mutex::new(None),
            standalone,
        });
        lock(&self.inner.pods).insert(id, pod.clone());
        if let Some(sandbox) = &sandbox {
            self.put_task(&pod, &proc, sandbox);
        }
        if let Some(vm) = info.vm {
            self.watch_vm(&pod, vm);
        }
        Ok((pod, pid))
    }

    async fn create_container(
        &self,
        pod: &str,
        id: String,
        bundle: String,
        rootfs: Vec<Mount>,
        stdio: Stdio,
    ) -> Outcome {
        let pod = self.pod(pod)?;
        let proc = ProcId::init(&id);
        if let Some(existing) = pod.proc(&proc) {
            return Ok(Reply::Pid { pid: existing.pid });
        }
        pod.require_vm()?;
        let (_creating, _) = self
            .inner
            .lifecycle
            .wake_for(&pod.id, Activity::ContainerCreate)
            .await
            .map_err(life_failure)?;
        // Mount containerd's rootfs mounts at bundle/rootfs, or take the
        // spec's own root, then hand the host path to the backend for guest
        // sharing.
        let rootfs = bundle::container_rootfs(&bundle, &rootfs)
            .await
            .map_err(unknown)?;
        let spec = ProcessSpec {
            bundle: bundle.clone(),
            rootfs,
            stdio: stdio.clone(),
            exec_spec: None,
            capture: None,
        };
        let pid = pod
            .backend
            .create_container(&id, spec)
            .await
            .map_err(unknown)?;
        let container = NodeProc::new(Kind::Container, bundle, stdio, pid);
        pod.procs().insert(proc.clone(), container.clone());
        self.put_task(&pod, &proc, &container);
        Ok(Reply::Pid { pid })
    }

    async fn exec(
        &self,
        pod: &str,
        id: String,
        exec: String,
        spec: String,
        stdio: Stdio,
    ) -> Outcome {
        let pod = self.pod(pod)?;
        let proc = ProcId::new(&id, &exec);
        if pod.proc(&proc).is_some() {
            return Ok(Reply::Done);
        }
        pod.require_vm()?;
        let container = pod.proc_or_not_found(&ProcId::init(&id))?;
        let probe = self
            .probe_exec(&pod.id, &container.bundle, &spec, &stdio)
            .await;
        if let Some(answer) = probe.as_ref().and_then(|p| self.cached_answer(&pod.id, p)) {
            let mut cached = NodeProc::new(Kind::Exec, String::new(), stdio, 0);
            cached.cached = Some(answer);
            pod.procs().insert(proc.clone(), cached);
            info!(
                "exec {proc} in pod {} is answered from the probe cache",
                pod.id
            );
            return Ok(Reply::Done);
        }
        // The wake's guard blocks a pause until the exec is recorded, which
        // then blocks it until the exec exits.
        let (_starting, _) = self
            .inner
            .lifecycle
            .wake_for(&pod.id, Activity::Exec)
            .await
            .map_err(life_failure)?;
        let capture = probe.as_ref().map(|_| OutputCapture::default());
        let spec = ProcessSpec {
            bundle: String::new(),
            rootfs: String::new(),
            stdio: stdio.clone(),
            exec_spec: Some(spec.into_bytes()),
            capture: capture.clone(),
        };
        pod.backend
            .create_exec(&id, &exec, spec)
            .await
            .map_err(unknown)?;
        let mut created = NodeProc::new(Kind::Exec, String::new(), stdio, 0);
        created.probe = probe.zip(capture);
        pod.procs().insert(proc.clone(), created.clone());
        self.put_exec(&pod, &proc, &created);
        info!("exec {proc} in pod {} created", pod.id);
        Ok(Reply::Done)
    }

    /// The probe exec that an exec in the container of `bundle` is: one of
    /// an exec probe's command, by the container's name.
    async fn probe_exec(
        &self,
        sandbox: &str,
        bundle: &str,
        spec: &str,
        stdio: &Stdio,
    ) -> Option<ProbeExec> {
        let sleep = self.inner.sleep.as_ref()?;
        let command = ExecCommand::parse(spec)?;
        let probes = sleep.book().probes(sandbox)?;
        let bundle = bundle.to_string();
        let name = tokio::task::spawn_blocking(move || probe::container_name(&bundle))
            .await
            .ok()??;
        let stdin = !stdio.stdin.is_empty() || stdio.terminal;
        probe::probe_exec(&probes, &name, &command, stdin)
    }

    /// The cached answer to a probe exec while the pod pauses, sleeps or
    /// wakes, so that it does not wake. After a failed restore, exit 1 with
    /// why.
    fn cached_answer(&self, sandbox: &str, exec: &ProbeExec) -> Option<CachedAnswer> {
        let sleep = self.inner.sleep.as_ref()?;
        let info = self.inner.lifecycle.get(&PodRef::sandbox(sandbox))?;
        if !matches!(
            info.state,
            PodState::Pausing | PodState::Asleep | PodState::Waking
        ) {
            return None;
        }
        let failed = (info.state == PodState::Asleep && info.wake_failures > 0)
            .then(|| info.failure.unwrap_or_else(|| "its resume failed".into()));
        let probes = sleep.book().probes(sandbox)?;
        probe::cached(&probes, exec, failed.as_deref())
    }

    /// Starts an exec answered from the probe cache: its output goes to its
    /// stdout fifo, and it exits at once.
    async fn start_cached(&self, pod: &Arc<Pod>, proc: ProcId, existing: NodeProc) -> Outcome {
        let Some(answer) = existing.cached else {
            return Err(unknown(format!("exec {proc} has no cached answer")));
        };
        let pid = self.inner.backends.next_pid();
        let (stdout, output) = (existing.stdio.stdout.clone(), answer.output);
        match tokio::task::spawn_blocking(move || probe::write_output(&stdout, &output)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!("exec {proc} in pod {}: {e}", pod.id),
            Err(e) => warn!("exec {proc} in pod {}: {e}", pod.id),
        }
        if let Some(p) = pod.procs().get_mut(&proc) {
            p.started = true;
            p.pid = pid;
        }
        let exit = Exit {
            status: answer.exit_status,
            exited_at_ns: now_ns(),
            oom: false,
        };
        let (node, pod) = (self.clone(), pod.clone());
        tokio::spawn(async move { node.exited(&pod, proc, exit) });
        Ok(Reply::Pid { pid })
    }

    async fn start(&self, pod: &str, proc: ProcId) -> Outcome {
        let pod = self.pod(pod)?;
        let existing = pod.proc_or_not_found(&proc)?;
        if existing.started {
            return Ok(Reply::Pid { pid: existing.pid });
        }
        if existing.cached.is_some() {
            return self.start_cached(&pod, proc, existing).await;
        }
        pod.require_vm()?;
        // The sandbox's process is nominal: the VM runs already.
        let _awake = match existing.kind {
            Kind::Sandbox => None,
            kind => {
                let activity = match kind {
                    Kind::Exec => Activity::Exec,
                    _ => Activity::ContainerCreate,
                };
                let woke = self.inner.lifecycle.wake_for(&pod.id, activity).await;
                Some(woke.map_err(life_failure)?.0)
            }
        };
        let pid = pod
            .backend
            .start(&proc.id, proc.exec_opt())
            .await
            .map_err(unknown)?;
        if let Some(p) = pod.procs().get_mut(&proc) {
            p.started = true;
            p.pid = pid;
        }
        if !proc.exec.is_empty() {
            info!("exec {proc} in pod {} started", pod.id);
        }
        self.record(&pod, &proc);
        if pod.is_standalone_container(&proc) {
            let vm = self
                .inner
                .lifecycle
                .get(&PodRef::sandbox(&pod.id))
                .and_then(|i| i.vm);
            let pid = vm.and_then(|v| u32::try_from(v.pid).ok());
            self.standalone_hooks(&pod, &existing.bundle, "poststart", "running", pid)
                .await;
        }
        self.watch_exit(&pod, proc);
        Ok(Reply::Pid { pid })
    }

    /// Every kill of a guest process goes through the lifecycle, which
    /// defers SIGKILL, and SIGTERM unless the pod wakes on terminate, while
    /// the pod sleeps. A kill of a lost pod's container answers its held
    /// exit.
    async fn kill(&self, pod: &str, proc: ProcId, signal: u32, all: bool) -> Outcome {
        if let Some(exit) = self.release_exit(pod, &proc) {
            return Ok(Reply::Exit { exit: Some(exit) });
        }
        let pod = self.pod(pod)?;
        // Every process of a lost pod has exited with 255.
        if pod.is_lost() {
            return Ok(Reply::Exit { exit: None });
        }
        // An exec answered from the cache has no guest process.
        if let Some(p) = pod.proc(&proc).filter(|p| p.cached.is_some()) {
            return Ok(Reply::Exit { exit: p.exit });
        }
        // The sandbox's process is nominal: no guest process to signal.
        if pod.is_sandbox(&proc) {
            pod.backend
                .kill(&proc.id, None, signal, all)
                .await
                .map_err(unknown)?;
            return Ok(Reply::Exit {
                exit: known_exit(&pod, &proc).await,
            });
        }
        let request = KillRequest {
            container: proc.id.clone(),
            exec: proc.exec_opt().map(str::to_string),
            signal,
            all,
        };
        let outcome = self.inner.lifecycle.kill(&pod.id, request).await;
        info!("signal {signal} to {proc} in pod {}: {outcome:?}", pod.id);
        match outcome {
            Ok(KillOutcome::Delivered) => Ok(Reply::Exit {
                exit: known_exit(&pod, &proc).await,
            }),
            Ok(KillOutcome::Deferred { exit_status }) => {
                let exit = Exit {
                    status: exit_status,
                    exited_at_ns: now_ns(),
                    oom: false,
                };
                pod.backend.set_exit(&proc.id, proc.exec_opt(), exit).await;
                pod.backend.end_pump(&proc.id, proc.exec_opt()).await;
                let exit = known_exit(&pod, &proc).await.unwrap_or(exit);
                self.exited(&pod, proc, exit);
                Ok(Reply::Exit { exit: Some(exit) })
            }
            Err(LifecycleError::Failed(_)) => Ok(Reply::Exit { exit: None }),
            Err(e) => Err(life_failure(e)),
        }
    }

    async fn delete(&self, pod_id: &str, proc: ProcId) -> Outcome {
        let Ok(pod) = self.pod(pod_id) else {
            return Ok(Reply::Exit { exit: None });
        };
        let Some(existing) = pod.proc(&proc) else {
            return Ok(Reply::Exit { exit: None });
        };
        if existing.kind == Kind::Sandbox {
            return self.delete_sandbox(&pod).await;
        }
        // A standalone pod's container goes with its VM.
        if pod.is_standalone_container(&proc) {
            let deleted = self.delete_sandbox(&pod).await?;
            bundle::unmount_rootfs(&existing.bundle).await;
            self.standalone_hooks(&pod, &existing.bundle, "poststop", "stopped", None)
                .await;
            return Ok(deleted);
        }
        let exit = known_exit(&pod, &proc).await.or(existing.exit);
        let mut in_guest = false;
        if !pod.is_lost() && existing.cached.is_none() {
            if self.is_awake(&pod.id) {
                pod.backend
                    .delete(&proc.id, proc.exec_opt())
                    .await
                    .map_err(unknown)?;
            } else {
                in_guest = true;
            }
        }
        if existing.kind == Kind::Container && !existing.bundle.is_empty() {
            bundle::unmount_rootfs(&existing.bundle).await;
        }
        pod.procs().remove(&proc);
        let exec = (existing.kind == Kind::Exec).then_some(proc.exec.as_str());
        let lifecycle = &self.inner.lifecycle;
        let removed = match (in_guest, exec) {
            (true, exec) => lifecycle.remove_for_guest(&pod.id, &proc.id, exec),
            (false, Some(exec)) => lifecycle.remove_exec(&pod.id, &proc.id, exec),
            (false, None) => lifecycle.remove_task(&pod.id, &proc.id),
        };
        if let Err(e) = removed {
            warn!(
                "remove {proc} from the record of pod sandbox {}: {e}",
                pod.id
            );
        }
        self.forget_exits(&pod.namespace, |(p, q)| p == pod_id && *q == proc);
        Ok(Reply::Exit { exit })
    }

    /// Deletes the sandbox and its VM without a wake.
    async fn delete_sandbox(&self, pod: &Arc<Pod>) -> Outcome {
        let proc = ProcId::init(&pod.id);
        let exit = known_exit(pod, &proc)
            .await
            .or_else(|| pod.proc(&proc).and_then(|p| p.exit));
        self.inner
            .lifecycle
            .delete(&PodRef::sandbox(&pod.id), None)
            .await
            .map_err(life_failure)?;
        if let Err(e) = pod.backend.delete(&pod.id, None).await {
            warn!("forget pod sandbox {}: {e}", pod.id);
        }
        self.forget_pod(&pod.id);
        Ok(Reply::Exit { exit })
    }

    /// Runs a standalone container's `poststart` or `poststop` hooks, as
    /// runc does after the container starts and after its delete. A failure
    /// is logged.
    async fn standalone_hooks(
        &self,
        pod: &Pod,
        bundle: &str,
        kind: &'static str,
        status: &'static str,
        pid: Option<u32>,
    ) {
        let (id, bundle) = (pod.id.clone(), bundle.to_string());
        let ran = tokio::task::spawn_blocking(move || {
            let Some(spec) = bundle::read_spec(&bundle) else {
                return Ok(());
            };
            let state = crate::standalone::state(&spec, &id, &bundle, status, pid);
            crate::standalone::run_hooks(&spec, kind, &state)
        })
        .await;
        match ran {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!("container {}: {e}", pod.id),
            Err(e) => warn!("container {}: the {kind} hooks ended: {e}", pod.id),
        }
    }

    fn forget_pod(&self, id: &str) {
        let removed = lock(&self.inner.pods).remove(id);
        if let Some(pod) = removed {
            self.forget_exits(&pod.namespace, |(p, _)| p == id);
        }
    }

    /// Delete sandbox `id` and its VM, whether this service knows it or not.
    /// A tombstone keeps the delete across a restart of this service.
    async fn drop_sandbox(&self, id: &str, netns: Option<String>) -> Outcome {
        if self.claimed_standalone(id) {
            debug!(
                "container {id}: a live front shim has it, so its delete action deletes nothing"
            );
            return Ok(Reply::Done);
        }
        let grave = Tombstone {
            pod: id.to_string(),
            netns: netns.clone(),
        };
        let dir = self.inner.tombstones.clone();
        match tokio::task::spawn_blocking(move || tombstone::write(&dir, &grave)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!("write the tombstone of sandbox {id}: {e}"),
            Err(e) => warn!("write the tombstone of sandbox {id}: {e}"),
        }
        let node = self.clone();
        let id = id.to_string();
        tokio::spawn(async move { node.finish_drop(&id, netns).await });
        Ok(Reply::Done)
    }

    /// Whether standalone pod `id` belongs to a live front shim: it is being
    /// created, or a session of its namespace is open and has not left it
    /// abandoned. containerd runs the delete action after every Delete, and
    /// reuses a container's id, as Docker does at each start, so a delete
    /// action may come for a task that is already new. The front shim's own
    /// Delete deleted the old pod.
    fn claimed_standalone(&self, id: &str) -> bool {
        let creating = OpKey {
            pod: id.to_string(),
            proc: ProcId::init(id),
            op: "create_standalone",
        };
        if lock(&self.inner.inflight).contains_key(&creating) {
            return true;
        }
        let Some(record) = self.inner.lifecycle.record(id) else {
            return false;
        };
        if !record.identity.standalone {
            return false;
        }
        let session = lock(&self.inner.namespaces)
            .get(&record.identity.task_namespace)
            .is_some_and(|ns| !ns.sessions.is_empty());
        let abandoned = lock(&self.inner.pods)
            .get(id)
            .is_some_and(|pod| lock(&pod.abandoned_at).is_some());
        session && !abandoned
    }

    async fn finish_drop(&self, id: &str, netns: Option<String>) {
        let deleted = match self.pod(id) {
            Ok(pod) => {
                info!("deleting pod sandbox {id}, which containerd ended");
                self.delete_sandbox(&pod).await.map(drop)
            }
            Err(_) if self.state(id).is_some() => self
                .inner
                .lifecycle
                .delete(&PodRef::sandbox(id), None)
                .await
                .map_err(life_failure),
            Err(_) => {
                let backends = self.inner.backends.clone();
                let owned = id.to_string();
                tokio::task::spawn_blocking(move || backends.reap(&owned, netns.as_deref()))
                    .await
                    .map_err(unknown)
            }
        };
        if let Err(e) = deleted {
            warn!("delete sandbox {id}: {e}");
            return;
        }
        if let Err(e) = tombstone::remove(&self.inner.tombstones, id) {
            warn!("remove the tombstone of sandbox {id}: {e}");
        }
    }

    /// Delete the sandboxes whose tombstones a node service before this one,
    /// or the shim's `delete` action, left.
    fn drop_tombstoned(&self) {
        let (graves, bad) = tombstone::list(&self.inner.tombstones);
        for path in bad {
            warn!("removing {}, which is not a tombstone", path.display());
            let _ = std::fs::remove_file(path);
        }
        for grave in graves {
            let node = self.clone();
            tokio::spawn(async move { node.finish_drop(&grave.pod, grave.netns).await });
        }
    }

    /// Delete the pods that no front shim has claimed for a while.
    async fn sweep_abandoned(self) {
        let every = (self.inner.abandon_after / 4)
            .clamp(Duration::from_millis(10), Duration::from_secs(30));
        loop {
            tokio::time::sleep(every).await;
            let expired: Vec<(String, Option<String>)> = lock(&self.inner.pods)
                .values()
                .filter(|pod| {
                    lock(&pod.abandoned_at)
                        .is_some_and(|since| since.elapsed() >= self.inner.abandon_after)
                })
                .map(|pod| (pod.id.clone(), pod.netns.clone()))
                .collect();
            for (pod, netns) in expired {
                info!("deleting pod sandbox {pod}, which no front shim claims");
                let drop = Op::DropSandbox { pod, netns };
                if let Err(e) = self.handle(None, drop).await {
                    warn!("delete an abandoned pod: {e}");
                }
            }
        }
    }

    fn forget_exits(&self, namespace: &str, mut which: impl FnMut(&(String, ProcId)) -> bool) {
        if let Some(ns) = lock(&self.inner.namespaces).get_mut(namespace) {
            ns.unacked.retain(|key, _| !which(key));
        }
    }

    /// Report the exit of a started process once it is known.
    fn watch_exit(&self, pod: &Arc<Pod>, proc: ProcId) {
        match pod.procs().get_mut(&proc) {
            Some(p) if !p.watched => p.watched = true,
            _ => return,
        }
        let node = self.clone();
        let pod = pod.clone();
        tokio::spawn(async move {
            let mut rx = match pod.backend.wait_channel(&proc.id, proc.exec_opt()).await {
                Ok(rx) => rx,
                Err(e) => {
                    warn!("watch the exit of {proc} in pod {}: {e}", pod.id);
                    return;
                }
            };
            let exit = loop {
                if let Some(exit) = *rx.borrow_and_update() {
                    break Some(exit);
                }
                if rx.changed().await.is_err() {
                    break None;
                }
            };
            let mut exit = match exit {
                Some(exit) => exit,
                // The backend dropped the process: an exit it does not know.
                None => Exit {
                    status: 255,
                    exited_at_ns: 0,
                    oom: false,
                },
            };
            exit.stamp();
            node.exited(&pod, proc, exit);
        });
    }

    fn exited(&self, pod: &Arc<Pod>, proc: ProcId, exit: Exit) {
        let current = lock(&self.inner.pods)
            .get(&pod.id)
            .is_some_and(|p| Arc::ptr_eq(p, pod));
        if !current {
            return;
        }
        let (kind, probe) = {
            let mut procs = pod.procs();
            let Some(p) = procs.get_mut(&proc) else {
                return;
            };
            p.exit.get_or_insert(exit);
            (p.kind, p.probe.take())
        };
        if let Some((exec, capture)) = probe {
            self.record_probe_exec(pod, &exec, &capture, exit.status);
        }
        if proc.exec.is_empty() {
            debug!("{proc} in pod {} exited with {}", pod.id, exit.status);
        } else {
            info!("exec {proc} in pod {} exited with {}", pod.id, exit.status);
        }
        let recorded = match proc.exec_opt() {
            Some(exec) => self
                .inner
                .lifecycle
                .exec_exited(&pod.id, &proc.id, exec, exit.status),
            None => self
                .inner
                .lifecycle
                .task_exited(&pod.id, &proc.id, exit.status),
        };
        if let Err(e) = recorded {
            debug!("record the exit of {proc} in pod {}: {e}", pod.id);
        }
        if kind == Kind::Container {
            if let Some(held) = lock(&self.inner.held).get_mut(&pod.id) {
                held.exits.entry(proc).or_insert(exit);
                return;
            }
        }
        self.report_exit(&pod.namespace, &pod.id, proc, exit);
    }

    /// Sends an exit to the namespace's front shims, and keeps it until one
    /// acks it.
    fn report_exit(&self, namespace: &str, pod: &str, proc: ProcId, exit: Exit) {
        let mut namespaces = lock(&self.inner.namespaces);
        let ns = namespaces.entry(namespace.to_string()).or_default();
        ns.unacked.insert((pod.to_string(), proc.clone()), exit);
        // Each front shim records the exits of its own tasks and acks every
        // exit.
        for tx in ns.sessions.values() {
            let _ = tx.send(FromNode::Exit {
                pod: pod.to_string(),
                proc: proc.clone(),
                exit,
            });
        }
    }

    /// Holds the exits of the lost pod's containers, those known and those
    /// to come, each until containerd kills the container, or for
    /// [`HOLD_LIMIT`].
    fn hold_exits(&self, pod: &Pod) {
        // A standalone pod has no sandbox for kubelet to see first.
        if pod.standalone {
            return;
        }
        let known: BTreeMap<ProcId, Exit> = pod
            .procs()
            .iter()
            .filter(|(_, p)| p.kind == Kind::Container)
            .filter_map(|(proc, p)| Some((proc.clone(), p.exit?)))
            .collect();
        let mut held = lock(&self.inner.held);
        if held.contains_key(&pod.id) {
            return;
        }
        held.insert(
            pod.id.clone(),
            Held {
                namespace: pod.namespace.clone(),
                exits: known,
            },
        );
        let node = self.clone();
        let id = pod.id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(HOLD_LIMIT).await;
            let Some(held) = lock(&node.inner.held).remove(&id) else {
                return;
            };
            if !held.exits.is_empty() {
                info!(
                    "pod sandbox {id}: no kill within {HOLD_LIMIT:?}, reporting the exits of {} \
                     containers",
                    held.exits.len()
                );
            }
            for (proc, exit) in held.exits {
                node.report_exit(&held.namespace, &id, proc, exit);
            }
        });
    }

    /// Reports the held exit of `proc` in pod `pod`, if it has one.
    fn release_exit(&self, pod: &str, proc: &ProcId) -> Option<Exit> {
        let (namespace, exit) = {
            let mut held = lock(&self.inner.held);
            let held = held.get_mut(pod)?;
            (held.namespace.clone(), held.exits.remove(proc)?)
        };
        self.report_exit(&namespace, pod, proc.clone(), exit);
        Some(exit)
    }

    fn is_held(&self, pod: &str, proc: &ProcId) -> bool {
        lock(&self.inner.held)
            .get(pod)
            .is_some_and(|held| held.exits.contains_key(proc))
    }

    /// Caches the result of a probe exec that ran in the awake pod. An exit
    /// that the node service made, of a kill while the pod sleeps or of a
    /// lost VM, is not the probe's result.
    fn record_probe_exec(&self, pod: &Pod, exec: &ProbeExec, capture: &OutputCapture, status: u32) {
        let Some(sleep) = &self.inner.sleep else {
            return;
        };
        if pod.is_lost() || !self.is_awake(&pod.id) {
            return;
        }
        let at_ms = crate::lifecycle::Clock::system().now_ms();
        let result = probe::exec_result(status, &capture.bytes(), at_ms);
        debug!(
            "pod sandbox {}: exec probe {:?} of {} exited with {status}",
            pod.id, exec.command, exec.container
        );
        sleep
            .book()
            .record_exec(&pod.id, &exec.container, &exec.command, result);
    }

    /// Watches the pod's VM process, once, and asks the lifecycle what its
    /// exit means: a hibernation, or a crash that fails the pod.
    fn watch_vm(&self, pod: &Arc<Pod>, process: VmProcess) {
        if !self.inner.backends.watches_vm_processes() {
            return;
        }
        {
            let mut watched = lock(&pod.watched_vm);
            if *watched == Some(process) {
                return;
            }
            *watched = Some(process);
        }
        let node = self.clone();
        let id = pod.id.clone();
        tokio::spawn(async move {
            super::vm::exited(process).await;
            match node.inner.lifecycle.vm_exited(&id, process).await {
                VmExit::Crash => warn!(
                    "VM process {} of pod sandbox {id} exited outside a pause: the pod failed",
                    process.pid
                ),
                exit => debug!("VM process {} of pod sandbox {id}: {exit:?}", process.pid),
            }
        });
    }

    /// The `awake` hook: the pumps go on, and the VM process is watched.
    async fn pod_awake(&self, info: &PodInfo, cause: AwakeCause) {
        // A new pod has no pumps yet, and is watched once it is created.
        if cause == AwakeCause::Created {
            return;
        }
        let Ok(pod) = self.pod(&info.identity.sandbox_id) else {
            return;
        };
        pod.backend.resume_pumps();
        if let Some(vm) = info.vm {
            self.watch_vm(&pod, vm);
        }
    }

    /// The `failed` hook: every process of the pod exits with 255, each
    /// container's exit at its kill ([`Node::hold_exits`]).
    async fn pod_failed(&self, id: &str, reason: &str) {
        let Ok(pod) = self.pod(id) else {
            return;
        };
        warn!("pod sandbox {id} is lost: {reason}");
        self.hold_exits(&pod);
        *lock(&pod.lost) = Some(reason.to_string());
        pod.backend.end_pumps();
        let exit = Exit {
            status: 255,
            exited_at_ns: now_ns(),
            oom: false,
        };
        let open: Vec<ProcId> = pod
            .procs()
            .iter()
            .filter(|(_, p)| p.exit.is_none())
            .map(|(proc, _)| proc.clone())
            .collect();
        for proc in open {
            pod.backend.set_exit(&proc.id, proc.exec_opt(), exit).await;
            self.exited(&pod, proc, exit);
        }
    }

    /// The `deleted` hook: the pod is gone, and with it its bridge in the
    /// pod's network namespace. Blocking.
    fn pod_deleted(&self, info: &PodInfo) {
        let id = &info.identity.sandbox_id;
        // Before the bridge goes, which the catcher's hold is on.
        if let Some(sleep) = &self.inner.sleep {
            sleep.gone(info);
        }
        self.forget_pod(id);
        self.inner
            .backends
            .after_delete(id, info.identity.netns.as_deref());
    }

    /// The `before_release` hook: delete in the guest what containerd
    /// deleted while the pod slept, from the pod's record.
    async fn delete_in_guest(&self, id: &str) -> Result<(), String> {
        let Ok(pod) = self.pod(id) else {
            return Ok(());
        };
        let lifecycle = &self.inner.lifecycle;
        let deletes = lifecycle
            .record(id)
            .map(|r| r.guest_deletes)
            .unwrap_or_default();
        if deletes.is_empty() {
            return Ok(());
        }
        for delete in &deletes {
            let exec = delete.exec.as_deref();
            if let Err(e) = pod.backend.delete(&delete.container, exec).await {
                let proc = ProcId::new(&delete.container, exec.unwrap_or_default());
                warn!("delete {proc} in the guest of pod sandbox {id}: {e}");
            }
        }
        if let Err(e) = lifecycle.guest_deleted(id, &deletes) {
            warn!("pod sandbox {id}: record its guest deletes as done: {e}");
        }
        Ok(())
    }

    /// The `before_release` hook: give each container the CA of the profile
    /// when Floci's CA changed since it was written, and return the wake's
    /// warning. Blocking.
    fn refresh_trust(&self, id: &str) -> Option<String> {
        let trust = crate::network::trust()?;
        let pod = self.pod(id).ok()?;
        crate::trust::refresh_all(&pod.backend.container_roots(), &trust)
    }

    /// The guest agent's process of an awake pod, without a wake.
    pub async fn agent_stats(&self, id: &str) -> Result<AgentStats, LifecycleError> {
        let pod = self
            .pod(id)
            .map_err(|_| LifecycleError::NotFound(format!("with sandbox id {id}")))?;
        // Blocks a pause while the guest answers.
        let _busy = self.inner.lifecycle.begin(id, Activity::Exec)?;
        match self.state(id) {
            Some(PodState::Awake) => pod.backend.agent_stats().await.map_err(LifecycleError::Vm),
            Some(state) => Err(LifecycleError::State {
                state,
                op: "count the agent of",
            }),
            None => Err(LifecycleError::NotFound(format!("with sandbox id {id}"))),
        }
    }
}

/// Opens the lifecycle, waiting a moment for the lock on the state directory,
/// which a node service that just ended may still hold.
async fn open_lifecycle(
    config: Config,
    vm: Arc<dyn crate::lifecycle::VmOps>,
    hooks: Arc<dyn Hooks>,
) -> Result<Lifecycle, String> {
    let deadline = Instant::now() + LOCK_WAIT;
    loop {
        match Lifecycle::open(config.clone(), vm.clone(), hooks.clone()).await {
            Ok(lifecycle) => return Ok(lifecycle),
            Err(LifecycleError::Store(e)) if Instant::now() < deadline => {
                debug!("open the lifecycle: {e}; trying again");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => return Err(format!("open the lifecycle: {e}")),
        }
    }
}

/// Reads the pod's IP and the guest's MAC from its network namespace and
/// records them, for the pod's catcher and lookups by IP. Blocking.
fn record_network(
    lifecycle: &Lifecycle,
    identity: &PodIdentity,
    netns: &str,
) -> Option<(String, String)> {
    let id = &identity.sandbox_id;
    let (ip, mac) = match crate::net::pod_network(netns) {
        Ok(found) => found,
        Err(e) => {
            warn!("pod sandbox {id}: read its IP and MAC: {e}");
            return None;
        }
    };
    let ip = ip.to_string();
    match lifecycle.set_network(id, Some(ip.clone()), Some(mac.clone())) {
        Ok(()) => {
            info!("pod sandbox {id} has the IP {ip} and the guest MAC {mac}");
            Some((ip, mac))
        }
        Err(e) => {
            warn!("pod sandbox {id}: record its IP and MAC: {e}");
            None
        }
    }
}

/// Who a pod is, from its sandbox's bundle, or a standalone pod's
/// container's.
fn pod_identity(
    namespace: &str,
    id: &str,
    bundle: &str,
    netns: Option<String>,
    standalone: bool,
) -> PodIdentity {
    let spec = bundle::read_spec(bundle);
    let annotation = |key: &str| {
        bundle::annotation(spec.as_ref(), key)
            .unwrap_or_default()
            .to_string()
    };
    let idle = idle_timeout(spec.as_ref()).unwrap_or_else(|e| {
        warn!("pod sandbox {id} gets the node's idle timeout: {e}");
        None
    });
    let cpu_threshold = cpu_threshold(spec.as_ref()).unwrap_or_else(|e| {
        warn!("pod sandbox {id} gets the node's CPU threshold: {e}");
        None
    });
    let cpu_window = cpu_window(spec.as_ref()).unwrap_or_else(|e| {
        warn!("pod sandbox {id} gets the node's CPU window: {e}");
        None
    });
    let memory = sandbox_memory(spec.as_ref());
    for warning in &memory.warnings {
        warn!("pod sandbox {id}: {warning}");
    }
    let keep_connections = keeps_connections(spec.as_ref()).unwrap_or_else(|e| {
        warn!("pod sandbox {id} resets its connections at a pause: {e}");
        false
    });
    let kept_tcp_peers = kept_tcp_peers(spec.as_ref()).unwrap_or_else(|e| {
        warn!("pod sandbox {id} ignores its invalid TCP keep list: {e}");
        Vec::new()
    });
    PodIdentity {
        sandbox_id: id.to_string(),
        namespace: annotation(ANN_POD_NAMESPACE),
        name: annotation(ANN_POD_NAME),
        uid: annotation(ANN_POD_UID),
        machine: id.to_string(),
        netns,
        memory_mib: Some(memory.mib),
        hibernate_disabled: hibernation_disabled(spec.as_ref(), standalone),
        wake_on_terminate: wakes_on_terminate(spec.as_ref()),
        keep_connections,
        kept_tcp_peers,
        idle_timeout_ms: idle.map(|t| t.as_millis() as u64),
        cpu_threshold_milli: cpu_threshold,
        cpu_window_ms: cpu_window.map(|t| t.as_millis() as u64),
        task_namespace: namespace.to_string(),
        standalone,
        ..PodIdentity::default()
    }
}

/// The exit of `proc`, if its backend knows it already.
async fn known_exit(pod: &Pod, proc: &ProcId) -> Option<Exit> {
    let rx = pod
        .backend
        .wait_channel(&proc.id, proc.exec_opt())
        .await
        .ok()?;
    let mut exit = (*rx.borrow())?;
    exit.stamp();
    Some(exit)
}

/// Whether `proc` is an exec answered from the probe cache.
fn is_cached(pod: &Pod, proc: &ProcId) -> bool {
    pod.proc(proc).is_some_and(|p| p.cached.is_some())
}

fn accepts(version: u32) -> bool {
    (MIN_VERSION..=VERSION).contains(&version)
}

fn version_refusal(version: u32) -> String {
    format!("the node service speaks protocol versions {MIN_VERSION} to {VERSION}, not {version}")
}
