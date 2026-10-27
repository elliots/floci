//! The Task API that containerd calls: the task table of every pod on the
//! node, which forwards to the node service what needs a VM.
//!
//! The table enforces the lifecycle containerd and critest expect (Created,
//! Running, Stopped; exactly one exit event per process; Wait blocking until
//! the exit; Delete returning the exit). State, Wait, Connect and Pids are
//! answered from it alone, so that they never wait for the node service:
//! containerd's Pids at its start has no deadline. Stats waits for the node
//! service for 500 ms at most, then answers the last sample, or fails if it
//! has none. Every other call waits for the node service until shortly before
//! its deadline, or for 30 seconds if it has none, then answers Unavailable.
//!
//! While a process runs, the table holds a descriptor open for reading and
//! writing on its stdout and stderr fifos. CRI stops logging a container for
//! good when it reads the end of a fifo, which it would when the node
//! service, whose pumps write the fifos, restarts.
//!
//! A panic in a handler fails that call alone ([`crate::panics`]).
//!
//! The Task API's Pause and Resume are not implemented: hibernation goes
//! through the node service.
//!
//! Derived from smolvm's crates/smolvm-shim (Apache-2.0), with changes.

use std::collections::HashMap;
use std::fs::File;
use std::future::Future;
use std::os::unix::fs::{FileTypeExt, OpenOptionsExt};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use containerd_shim::asynchronous::ExitSignal;
use containerd_shim::publisher::RemotePublisher;
use containerd_shim::util::{convert_to_any, timestamp};
use containerd_shim::TtrpcResult;
use containerd_shim_protos::api;
use containerd_shim_protos::events::task::{
    TaskCreate, TaskDelete, TaskExecAdded, TaskExecStarted, TaskExit, TaskOOM, TaskStart,
};
use containerd_shim_protos::protobuf::well_known_types::timestamp::Timestamp;
use containerd_shim_protos::protobuf::MessageDyn;
use containerd_shim_protos::shim_async::Task;
use containerd_shim_protos::ttrpc::r#async::TtrpcContext;
use containerd_shim_protos::ttrpc::{self, Code as RpcCode};
use log::{debug, warn};
use tokio::sync::{watch, Mutex};
use tokio::time::Instant;

use super::client::{Front, NodeClient, Wait};
use crate::backend::now_ns;
use crate::bundle;
use crate::proto::{
    Code, Exit, Failure, Kind, Mount, Op, ProcId, Reply, StatsSample, Status, Stdio, TaskInfo,
};

/// How long a call waits for the node service when containerd gave it no
/// deadline.
const OUTAGE_WAIT: Duration = Duration::from_secs(30);

/// How long Stats, and the refresh of Pids, wait for the node service.
const QUICK_WAIT: Duration = Duration::from_millis(500);

/// How long the front shim waits, after a Shutdown found the table empty,
/// before it exits: containerd may be about to create a task, after a `start`
/// action gave it this shim's address.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(1);

fn rpc_error(code: RpcCode, msg: impl std::fmt::Display) -> ttrpc::Error {
    ttrpc::Error::RpcStatus(ttrpc::get_status(code, msg.to_string()))
}

fn err(msg: impl std::fmt::Display) -> ttrpc::Error {
    rpc_error(RpcCode::UNKNOWN, msg)
}

fn not_found(msg: impl std::fmt::Display) -> ttrpc::Error {
    rpc_error(RpcCode::NOT_FOUND, msg)
}

fn from_failure(failure: Failure) -> ttrpc::Error {
    let code = match failure.code {
        Code::NotFound => RpcCode::NOT_FOUND,
        Code::AlreadyExists => RpcCode::ALREADY_EXISTS,
        Code::FailedPrecondition => RpcCode::FAILED_PRECONDITION,
        Code::Unavailable => RpcCode::UNAVAILABLE,
        Code::Internal => RpcCode::INTERNAL,
        Code::Unknown => RpcCode::UNKNOWN,
    };
    rpc_error(code, failure.message)
}

fn unexpected(op: &str, reply: Reply) -> ttrpc::Error {
    err(format!("the node service answered {op} with {reply:?}"))
}

/// Run a handler, turning a panic in it into an error for this call alone.
async fn guard<T>(method: &str, handler: impl Future<Output = TtrpcResult<T>>) -> TtrpcResult<T> {
    match crate::panics::catch(handler).await {
        Ok(result) => result,
        Err(panic) => Err(rpc_error(
            RpcCode::INTERNAL,
            format!("the front shim failed in {method}: {panic}"),
        )),
    }
}

/// Where a test makes a handler panic.
#[cfg(test)]
fn fail_point(method: &str, id: &str) {
    tests::fail_point(method, id);
}

#[cfg(not(test))]
fn fail_point(_method: &str, _id: &str) {}

/// Where task events go: containerd, or a test.
#[async_trait]
pub trait Events: Send + Sync + 'static {
    async fn publish(&self, topic: &str, event: Box<dyn MessageDyn>);
}

/// containerd's event service, for one namespace.
pub struct Containerd {
    pub publisher: RemotePublisher,
    pub namespace: String,
}

#[async_trait]
impl Events for Containerd {
    async fn publish(&self, topic: &str, event: Box<dyn MessageDyn>) {
        let ctx = ttrpc::context::Context::default();
        if let Err(e) = self
            .publisher
            .publish(ctx, topic, &self.namespace, event)
            .await
        {
            warn!("publish {topic} failed: {e}");
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcState {
    Created,
    Running,
    Stopped,
}

struct Proc {
    /// The sandbox id of the process's pod.
    pod: String,
    kind: Kind,
    state: ProcState,
    pid: u32,
    stdio: Stdio,
    bundle: String,
    /// The pod's network namespace, for the sandbox.
    netns: Option<String>,
    /// Set with `state` by [`Proc::stop`]. Wait listens here, so State and
    /// Delete already see the exit when Wait returns.
    exit: watch::Sender<Option<Exit>>,
    /// A Start of the process is waiting for the node service.
    starting: bool,
    /// An exit reported while the process was starting, which the Start
    /// records after its TaskStart.
    early_exit: Option<Exit>,
    /// The process's stdout and stderr fifos, held open until it exits.
    fifos: Vec<File>,
    /// The guest's pids of a container, as last reported.
    guest_pids: Vec<u32>,
    refreshing_pids: bool,
    /// A container's last stats.
    stats: Option<StatsSample>,
}

impl Proc {
    fn new(pod: String, kind: Kind, pid: u32, stdio: Stdio, bundle: String) -> Proc {
        let fifos = if kind == Kind::Sandbox {
            Vec::new()
        } else {
            keep_fifos_open(&stdio)
        };
        Proc {
            pod,
            kind,
            state: ProcState::Created,
            pid,
            stdio,
            bundle,
            netns: None,
            exit: watch::Sender::new(None),
            starting: false,
            early_exit: None,
            fifos,
            guest_pids: Vec::new(),
            refreshing_pids: false,
            stats: None,
        }
    }

    /// Mark the process stopped, then record its exit, which wakes Wait.
    fn stop(&mut self, info: Exit) {
        self.state = ProcState::Stopped;
        // Once the pump has closed its ends too, CRI reads the end of the
        // output and finishes the container's log.
        self.fifos.clear();
        self.exit.send_replace(Some(info));
    }

    fn info(&self, key: &ProcKey) -> TaskInfo {
        TaskInfo {
            pod: self.pod.clone(),
            proc: ProcId::new(&key.0, &key.1),
            kind: self.kind,
            bundle: self.bundle.clone(),
            netns: self.netns.clone(),
            stdio: self.stdio.clone(),
            pid: self.pid,
            status: match self.state {
                ProcState::Created => Status::Created,
                ProcState::Running => Status::Running,
                ProcState::Stopped => Status::Stopped,
            },
            exit: *self.exit.borrow(),
        }
    }
}

/// Open the stdout and stderr fifos of `stdio` for reading and writing, which
/// never blocks. Such a descriptor is a writer, so the reader never sees the
/// end of the fifo, and a reader, so the writer never gets EPIPE while
/// containerd restarts.
fn keep_fifos_open(stdio: &Stdio) -> Vec<File> {
    [&stdio.stdout, &stdio.stderr]
        .into_iter()
        .filter(|path| !path.is_empty())
        .filter_map(|path| {
            let opened = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(path);
            match opened {
                Ok(file) if file.metadata().is_ok_and(|m| m.file_type().is_fifo()) => Some(file),
                Ok(_) => None,
                Err(e) => {
                    warn!("keep fifo {path} open: {e}");
                    None
                }
            }
        })
        .collect()
}

/// `(container_id, exec_id)` → process entry. Empty exec_id = init process.
type ProcKey = (String, String);

/// The front shim's Task service. Cloning it gives another handle to the same
/// table.
#[derive(Clone)]
pub struct TaskService {
    shared: Arc<Shared>,
}

struct Shared {
    procs: Mutex<HashMap<ProcKey, Proc>>,
    node: NodeClient,
    events: Option<Arc<dyn Events>>,
    exit: Arc<ExitSignal>,
    /// Creates that wait for the node service: the table is not empty for
    /// Shutdown while one runs.
    creating: AtomicUsize,
}

/// Counts a Create while it runs.
struct Creating<'a>(&'a AtomicUsize);

impl<'a> Creating<'a> {
    fn new(count: &'a AtomicUsize) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Creating(count)
    }
}

impl Drop for Creating<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Front for Shared {
    async fn tasks(&self) -> Vec<TaskInfo> {
        let procs = self.procs.lock().await;
        let mut tasks: Vec<TaskInfo> = procs.iter().map(|(k, p)| p.info(k)).collect();
        tasks.sort_by(|a, b| (&a.pod, &a.proc).cmp(&(&b.pod, &b.proc)));
        tasks
    }

    async fn exited(&self, _pod: String, proc: ProcId, exit: Exit) {
        self.record_exit(&(proc.id, proc.exec), exit).await;
    }
}

impl TaskService {
    /// A Task service whose node service listens on `socket`, for containerd
    /// namespace `namespace`.
    pub fn new(
        socket: PathBuf,
        namespace: String,
        events: Option<Arc<dyn Events>>,
        exit: Arc<ExitSignal>,
    ) -> Self {
        let shared = Arc::new(Shared {
            procs: Mutex::new(HashMap::new()),
            node: NodeClient::new(socket, namespace),
            events,
            exit,
            creating: AtomicUsize::new(0),
        });
        let front: Arc<dyn Front> = shared.clone();
        shared.node.run(Arc::downgrade(&front));
        TaskService { shared }
    }

    async fn pod_of(&self, id: &str) -> TtrpcResult<String> {
        let procs = self.shared.procs.lock().await;
        procs
            .get(&(id.to_string(), String::new()))
            .map(|p| p.pod.clone())
            .ok_or_else(|| not_found(format!("container {id}")))
    }

    async fn call(&self, ctx: &TtrpcContext, op: Op) -> TtrpcResult<Reply> {
        self.shared
            .node
            .call(op, wait_for(ctx))
            .await
            .map_err(from_failure)
    }

    /// Ask the node service for a container's guest pids, for the next Pids.
    fn refresh_pids(&self, pod: String, id: String) {
        let shared = self.shared.clone();
        tokio::spawn(async move {
            let op = Op::Pids {
                pod,
                id: id.clone(),
            };
            let reply = shared
                .node
                .call(op, Wait::until(Instant::now() + QUICK_WAIT))
                .await;
            let mut procs = shared.procs.lock().await;
            if let Some(p) = procs.get_mut(&(id, String::new())) {
                p.refreshing_pids = false;
                if let Ok(Reply::Pids { pids }) = reply {
                    p.guest_pids = pids;
                }
            }
        });
    }
}

/// How long a call may wait: until shortly before containerd's deadline, so
/// that containerd gets the answer, or for an outage of [`OUTAGE_WAIT`].
fn wait_for(ctx: &TtrpcContext) -> Wait {
    if ctx.timeout_nano <= 0 {
        return Wait::outage(OUTAGE_WAIT);
    }
    let timeout = Duration::from_nanos(ctx.timeout_nano as u64);
    let margin = (timeout / 10).min(Duration::from_millis(100));
    Wait::until(Instant::now() + timeout - margin)
}

impl Shared {
    async fn publish(&self, topic: &str, event: Box<dyn MessageDyn>) {
        if let Some(events) = &self.events {
            events.publish(topic, event).await;
        }
    }

    /// Record a process's exit and publish it, once.
    async fn record_exit(&self, key: &ProcKey, mut exit: Exit) {
        exit.stamp();
        let pid = {
            let mut procs = self.procs.lock().await;
            let Some(p) = procs.get_mut(key) else {
                return;
            };
            match p.state {
                ProcState::Stopped => return,
                ProcState::Created if p.starting => {
                    p.early_exit.get_or_insert(exit);
                    return;
                }
                _ => {}
            }
            p.stop(exit);
            p.pid
        };
        self.publish_exit(&key.0, &key.1, pid, exit).await;
    }

    async fn publish_exit(&self, id: &str, exec_id: &str, pid: u32, info: Exit) {
        // A cgroup OOM kill must be announced before the exit so the CRI
        // has marked the container OOMKilled by the time it reads the exit
        // status. Only the init process (empty exec_id) carries this.
        if info.oom && exec_id.is_empty() {
            let mut oom = TaskOOM::new();
            oom.container_id = id.to_string();
            self.publish("/tasks/oom", Box::new(oom)).await;
        }
        let mut ev = TaskExit::new();
        ev.container_id = id.to_string();
        ev.id = if exec_id.is_empty() { id } else { exec_id }.to_string();
        ev.pid = pid;
        ev.exit_status = info.status;
        ev.exited_at = Some(protobuf_ts(info.exited_at_ns)).into();
        self.publish("/tasks/exit", Box::new(ev)).await;
    }

    async fn idle(&self) -> bool {
        self.procs.lock().await.is_empty() && self.creating.load(Ordering::SeqCst) == 0
    }
}

fn protobuf_ts(ns: i64) -> Timestamp {
    if ns <= 0 {
        return timestamp().unwrap_or_default();
    }
    let mut t = Timestamp::new();
    t.seconds = ns / 1_000_000_000;
    t.nanos = (ns % 1_000_000_000) as i32;
    t
}

fn state_to_api(s: ProcState) -> api::Status {
    match s {
        ProcState::Created => api::Status::CREATED,
        ProcState::Running => api::Status::RUNNING,
        ProcState::Stopped => api::Status::STOPPED,
    }
}

/// Build a cgroups v1 `Metrics` message (the format containerd's CRI decodes for
/// `ContainerStats`) from a guest usage sample. crun runs cgroup-less in the
/// guest, so only CPU total and memory usage are populated, which is what the
/// CRI cpu/memory stats surface, and the memory limit, which `docker stats`
/// reports, is the size of this pod VM. Old nodes omit the size and used the
/// default for every pod. The CPU throttling is there, empty, because
/// dockerd reads it without a check.
fn cgroup_metrics(sample: StatsSample) -> containerd_shim_protos::cgroups::metrics::Metrics {
    use containerd_shim_protos::cgroups::metrics::{
        CPUStat, CPUUsage, MemoryEntry, MemoryStat, Metrics, Throttle,
    };
    use containerd_shim_protos::protobuf::MessageField;

    Metrics {
        cpu: MessageField::some(CPUStat {
            usage: MessageField::some(CPUUsage {
                total: sample.cpu_usage_ns,
                ..Default::default()
            }),
            throttling: MessageField::some(Throttle::default()),
            ..Default::default()
        }),
        memory: MessageField::some(MemoryStat {
            usage: MessageField::some(MemoryEntry {
                usage: sample.memory_bytes,
                limit: sample
                    .memory_limit_bytes
                    .unwrap_or(u64::from(crate::engine::DEFAULT_SANDBOX_MEMORY_MIB) * 1024 * 1024),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The handlers of the Task API, each called through [`guard`].
impl TaskService {
    async fn handle_create(
        &self,
        ctx: &TtrpcContext,
        req: api::CreateTaskRequest,
    ) -> TtrpcResult<api::CreateTaskResponse> {
        let id = req.id.clone();
        debug!("create task id={id} bundle={}", req.bundle);
        let key: ProcKey = (id.clone(), String::new());
        if self.shared.procs.lock().await.contains_key(&key) {
            return Err(err(format!("task {id} already exists")));
        }
        let _creating = Creating::new(&self.shared.creating);
        let bundle_dir = req.bundle.clone();
        let info = tokio::task::spawn_blocking(move || bundle::load(&bundle_dir))
            .await
            .map_err(err)?
            .map_err(err)?;

        let stdio = Stdio {
            stdin: req.stdin.clone(),
            stdout: req.stdout.clone(),
            stderr: req.stderr.clone(),
            terminal: req.terminal,
        };
        let rootfs: Vec<Mount> = req
            .rootfs
            .iter()
            .map(|m| Mount {
                kind: m.type_.clone(),
                source: m.source.clone(),
                target: m.target.clone(),
                options: m.options.to_vec(),
            })
            .collect();
        let (pod, kind, op) = if info.standalone {
            // A pod of its own, whose container has the task's id.
            let op = Op::CreateStandalone {
                pod: id.clone(),
                bundle: req.bundle.clone(),
                rootfs,
                stdio: stdio.clone(),
            };
            (id.clone(), Kind::Container, op)
        } else if info.is_sandbox {
            let op = Op::CreateSandbox {
                pod: id.clone(),
                bundle: req.bundle.clone(),
                netns: info.netns.clone(),
            };
            (id.clone(), Kind::Sandbox, op)
        } else {
            let pod = info.sandbox_id.clone().unwrap_or_else(|| id.clone());
            let op = Op::CreateContainer {
                pod: pod.clone(),
                id: id.clone(),
                bundle: req.bundle.clone(),
                rootfs,
                stdio: stdio.clone(),
            };
            (pod, Kind::Container, op)
        };
        let pid = match self.call(ctx, op).await? {
            Reply::Pid { pid } => pid,
            other => return Err(unexpected("create", other)),
        };

        let mut procs = self.shared.procs.lock().await;
        if procs.contains_key(&key) {
            return Err(err(format!("task {id} already exists")));
        }
        let mut proc = Proc::new(pod, kind, pid, stdio, req.bundle.clone());
        proc.netns = info.netns.filter(|_| kind == Kind::Sandbox);
        procs.insert(key, proc);
        drop(procs);

        let mut ev = TaskCreate::new();
        ev.container_id = id.clone();
        ev.bundle = req.bundle.clone();
        ev.pid = pid;
        self.shared.publish("/tasks/create", Box::new(ev)).await;

        Ok(api::CreateTaskResponse {
            pid,
            ..Default::default()
        })
    }

    async fn handle_start(
        &self,
        ctx: &TtrpcContext,
        req: api::StartRequest,
    ) -> TtrpcResult<api::StartResponse> {
        let (id, exec_id) = (req.id.clone(), req.exec_id.clone());
        let key: ProcKey = (id.clone(), exec_id.clone());
        let proc = ProcId::new(&id, &exec_id);
        // Only a created process starts, so a killed one stays stopped.
        let pod = {
            let mut procs = self.shared.procs.lock().await;
            match procs.get_mut(&key) {
                Some(p) if p.state == ProcState::Created && !p.starting => {
                    p.starting = true;
                    p.pod.clone()
                }
                Some(p) => {
                    let state = p.state;
                    return Err(err(format!(
                        "cannot start process {id}/{exec_id} in state {state:?}"
                    )));
                }
                None => return Err(not_found(format!("process {id}/{exec_id}"))),
            }
        };
        let started = self
            .call(
                ctx,
                Op::Start {
                    pod: pod.clone(),
                    proc: proc.clone(),
                },
            )
            .await;
        let (pid, early_exit) = {
            let mut procs = self.shared.procs.lock().await;
            let p = procs
                .get_mut(&key)
                .ok_or_else(|| not_found(format!("process {id}/{exec_id}")))?;
            p.starting = false;
            let early_exit = p.early_exit.take();
            let pid = match started {
                Ok(Reply::Pid { pid }) => pid,
                Ok(other) => return Err(unexpected("start", other)),
                Err(e) => return Err(e),
            };
            if p.state == ProcState::Stopped {
                // Killed while starting. Its exit is already reported, so end
                // the process instead of reopening it.
                drop(procs);
                let kill = Op::Kill {
                    pod,
                    proc,
                    signal: 9,
                    all: true,
                };
                if let Err(e) = self.call(ctx, kill).await {
                    warn!("kill {id}/{exec_id}, which was killed while starting: {e}");
                }
                return Err(err(format!(
                    "process {id}/{exec_id} was killed while starting"
                )));
            }
            p.state = ProcState::Running;
            p.pid = pid;
            (pid, early_exit)
        };

        if exec_id.is_empty() {
            let mut ev = TaskStart::new();
            ev.container_id = id.clone();
            ev.pid = pid;
            self.shared.publish("/tasks/start", Box::new(ev)).await;
        } else {
            let mut ev = TaskExecStarted::new();
            ev.container_id = id.clone();
            ev.exec_id = exec_id.clone();
            ev.pid = pid;
            self.shared
                .publish("/tasks/exec-started", Box::new(ev))
                .await;
        }
        if let Some(exit) = early_exit {
            self.shared.record_exit(&key, exit).await;
        }
        Ok(api::StartResponse {
            pid,
            ..Default::default()
        })
    }

    async fn handle_state(&self, req: api::StateRequest) -> TtrpcResult<api::StateResponse> {
        let procs = self.shared.procs.lock().await;
        fail_point("state", &req.id);
        let p = procs
            .get(&(req.id.clone(), req.exec_id.clone()))
            .ok_or_else(|| not_found(format!("process {}/{}", req.id, req.exec_id)))?;
        let exit = *p.exit.borrow();
        let mut resp = api::StateResponse {
            id: req.id.clone(),
            pid: p.pid,
            bundle: p.bundle.clone(),
            stdin: p.stdio.stdin.clone(),
            stdout: p.stdio.stdout.clone(),
            stderr: p.stdio.stderr.clone(),
            terminal: p.stdio.terminal,
            exit_status: exit.map(|e| e.status).unwrap_or_default(),
            ..Default::default()
        };
        resp.set_status(state_to_api(p.state));
        if let Some(e) = exit {
            resp.exited_at = Some(protobuf_ts(e.exited_at_ns)).into();
        }
        Ok(resp)
    }

    async fn handle_wait(&self, req: api::WaitRequest) -> TtrpcResult<api::WaitResponse> {
        let mut rx = self
            .shared
            .procs
            .lock()
            .await
            .get(&(req.id.clone(), req.exec_id.clone()))
            .map(|p| p.exit.subscribe())
            .ok_or_else(|| not_found(format!("process {}/{}", req.id, req.exec_id)))?;
        let info = loop {
            if let Some(info) = *rx.borrow() {
                break info;
            }
            if rx.changed().await.is_err() {
                // Deleted before it exited.
                break Exit {
                    status: 255,
                    exited_at_ns: 0,
                    oom: false,
                };
            }
        };
        Ok(api::WaitResponse {
            exit_status: info.status,
            exited_at: Some(protobuf_ts(info.exited_at_ns)).into(),
            ..Default::default()
        })
    }

    async fn handle_kill(&self, ctx: &TtrpcContext, req: api::KillRequest) -> TtrpcResult<()> {
        let key: ProcKey = (req.id.clone(), req.exec_id.clone());
        let pod = {
            let mut procs = self.shared.procs.lock().await;
            fail_point("kill", &req.id);
            let p = procs
                .get_mut(&key)
                .ok_or_else(|| not_found(format!("process {}/{}", req.id, req.exec_id)))?;
            if p.state == ProcState::Stopped {
                // Killing an exited process is a no-op (matches runc shim).
                return Ok(());
            }
            // A created process has not run yet, so there is nothing to
            // signal. Record an exit, as runc does for a killed created
            // container, so that Wait returns and Delete can proceed.
            if p.state == ProcState::Created && matches!(req.signal, 9 | 15) {
                let info = Exit {
                    status: 128 + req.signal,
                    exited_at_ns: now_ns(),
                    oom: false,
                };
                p.stop(info);
                let pid = p.pid;
                drop(procs);
                self.shared
                    .publish_exit(&req.id, &req.exec_id, pid, info)
                    .await;
                return Ok(());
            }
            p.pod.clone()
        };
        let op = Op::Kill {
            pod,
            proc: ProcId::new(&req.id, &req.exec_id),
            signal: req.signal,
            all: req.all,
        };
        match self.call(ctx, op).await? {
            Reply::Exit { exit: Some(exit) } => self.shared.record_exit(&key, exit).await,
            Reply::Exit { exit: None } | Reply::Done => {}
            other => return Err(unexpected("kill", other)),
        }
        Ok(())
    }

    async fn handle_exec(
        &self,
        ctx: &TtrpcContext,
        req: api::ExecProcessRequest,
    ) -> TtrpcResult<()> {
        let stdio = Stdio {
            stdin: req.stdin.clone(),
            stdout: req.stdout.clone(),
            stderr: req.stderr.clone(),
            terminal: req.terminal,
        };
        let pod = {
            let procs = self.shared.procs.lock().await;
            let Some(container) = procs.get(&(req.id.clone(), String::new())) else {
                return Err(not_found(format!("container {}", req.id)));
            };
            if procs.contains_key(&(req.id.clone(), req.exec_id.clone())) {
                return Err(err(format!("exec {} already exists", req.exec_id)));
            }
            container.pod.clone()
        };
        // containerd's ExecProcessRequest.spec is an Any whose value is the
        // OCI Process serialized as JSON; pass it through verbatim.
        let spec = String::from_utf8(req.spec.value.clone())
            .map_err(|e| err(format!("exec spec is not UTF-8 JSON: {e}")))?;
        let op = Op::Exec {
            pod: pod.clone(),
            id: req.id.clone(),
            exec: req.exec_id.clone(),
            spec,
            stdio: stdio.clone(),
        };
        match self.call(ctx, op).await? {
            Reply::Done => {}
            other => return Err(unexpected("exec", other)),
        }
        self.shared.procs.lock().await.insert(
            (req.id.clone(), req.exec_id.clone()),
            Proc::new(pod, Kind::Exec, 0, stdio, String::new()),
        );
        let mut ev = TaskExecAdded::new();
        ev.container_id = req.id.clone();
        ev.exec_id = req.exec_id.clone();
        self.shared.publish("/tasks/exec-added", Box::new(ev)).await;
        Ok(())
    }

    async fn handle_resize_pty(
        &self,
        ctx: &TtrpcContext,
        req: api::ResizePtyRequest,
    ) -> TtrpcResult<()> {
        let op = Op::ResizePty {
            pod: self.pod_of(&req.id).await?,
            proc: ProcId::new(&req.id, &req.exec_id),
            width: req.width,
            height: req.height,
        };
        self.call(ctx, op).await.map(|_| ())
    }

    async fn handle_close_io(
        &self,
        ctx: &TtrpcContext,
        req: api::CloseIORequest,
    ) -> TtrpcResult<()> {
        let op = Op::CloseIo {
            pod: self.pod_of(&req.id).await?,
            proc: ProcId::new(&req.id, &req.exec_id),
        };
        self.call(ctx, op).await.map(|_| ())
    }

    async fn handle_delete(
        &self,
        ctx: &TtrpcContext,
        req: api::DeleteRequest,
    ) -> TtrpcResult<api::DeleteResponse> {
        let key: ProcKey = (req.id.clone(), req.exec_id.clone());
        let (pod, pid, mut exit) = {
            let procs = self.shared.procs.lock().await;
            let p = procs
                .get(&key)
                .ok_or_else(|| not_found(format!("process {}/{}", req.id, req.exec_id)))?;
            let exit = *p.exit.borrow();
            (p.pod.clone(), p.pid, exit)
        };
        let op = Op::Delete {
            pod,
            proc: ProcId::new(&req.id, &req.exec_id),
        };
        // An exit the node service knows, and this table not yet, is the real
        // status.
        match self.call(ctx, op).await? {
            Reply::Exit { exit: known } => exit = exit.or(known),
            Reply::Done => {}
            other => return Err(unexpected("delete", other)),
        }
        self.shared.procs.lock().await.remove(&key);

        let exited_at = exit
            .map(|e| protobuf_ts(e.exited_at_ns))
            .unwrap_or_default();
        if req.exec_id.is_empty() {
            let mut ev = TaskDelete::new();
            ev.container_id = req.id.clone();
            ev.pid = pid;
            ev.exit_status = exit.map(|e| e.status).unwrap_or_default();
            ev.exited_at = Some(exited_at.clone()).into();
            self.shared.publish("/tasks/delete", Box::new(ev)).await;
        }
        Ok(api::DeleteResponse {
            pid,
            exit_status: exit.map(|e| e.status).unwrap_or_default(),
            exited_at: Some(exited_at).into(),
            ..Default::default()
        })
    }

    /// The task's pid and its guest's pids as last reported, never empty.
    async fn handle_pids(&self, req: api::PidsRequest) -> TtrpcResult<api::PidsResponse> {
        let (pids, refresh) = {
            let mut procs = self.shared.procs.lock().await;
            let p = procs
                .get_mut(&(req.id.clone(), String::new()))
                .ok_or_else(|| not_found(format!("container {}", req.id)))?;
            let mut pids = vec![p.pid.max(1)];
            for pid in &p.guest_pids {
                if !pids.contains(pid) {
                    pids.push(*pid);
                }
            }
            let refresh =
                p.kind == Kind::Container && p.state == ProcState::Running && !p.refreshing_pids;
            p.refreshing_pids |= refresh;
            (pids, refresh.then(|| p.pod.clone()))
        };
        if let Some(pod) = refresh {
            self.refresh_pids(pod, req.id.clone());
        }
        let processes = pids
            .into_iter()
            .map(|pid| containerd_shim_protos::types::task::ProcessInfo {
                pid,
                ..Default::default()
            })
            .collect();
        Ok(api::PidsResponse {
            processes,
            ..Default::default()
        })
    }

    async fn handle_stats(&self, req: api::StatsRequest) -> TtrpcResult<api::StatsResponse> {
        let key: ProcKey = (req.id.clone(), String::new());
        let op = Op::Stats {
            pod: self.pod_of(&req.id).await?,
            id: req.id.clone(),
        };
        let fresh = self
            .shared
            .node
            .call(op, Wait::until(Instant::now() + QUICK_WAIT))
            .await;
        let sample = {
            let mut procs = self.shared.procs.lock().await;
            let p = procs
                .get_mut(&key)
                .ok_or_else(|| not_found(format!("container {}", req.id)))?;
            match fresh {
                Ok(Reply::Stats {
                    sample: Some(mut sample),
                }) => {
                    // A CPU total never goes back.
                    if let Some(last) = p.stats {
                        sample.cpu_usage_ns = sample.cpu_usage_ns.max(last.cpu_usage_ns);
                    }
                    p.stats = Some(sample);
                    Some(sample)
                }
                Ok(Reply::Stats { sample: None }) => p.stats,
                Ok(other) => return Err(unexpected("stats", other)),
                // containerd leaves out a task whose Stats fails, but CRI fails
                // a whole listing for a container without stats.
                Err(e) => match p.stats {
                    Some(last) => {
                        debug!("stats of {}: {e}; answering the last ones", req.id);
                        Some(last)
                    }
                    None => return Err(from_failure(e)),
                },
            }
        };
        let mut resp = api::StatsResponse::default();
        if let Some(sample) = sample {
            let metrics = cgroup_metrics(sample);
            resp.stats = Some(convert_to_any(Box::new(metrics)).map_err(err)?).into();
        }
        Ok(resp)
    }

    async fn handle_connect(&self, req: api::ConnectRequest) -> TtrpcResult<api::ConnectResponse> {
        let procs = self.shared.procs.lock().await;
        let pid = procs
            .get(&(req.id.clone(), String::new()))
            .map(|p| p.pid)
            .unwrap_or_default();
        Ok(api::ConnectResponse {
            shim_pid: std::process::id(),
            task_pid: pid,
            ..Default::default()
        })
    }

    /// Exit once the table stays empty.
    async fn handle_shutdown(&self) -> TtrpcResult<()> {
        if self.shared.idle().await {
            let shared = self.shared.clone();
            tokio::spawn(async move {
                tokio::time::sleep(SHUTDOWN_GRACE).await;
                if shared.idle().await {
                    shared.exit.signal();
                }
            });
        }
        Ok(())
    }
}

#[async_trait]
impl Task for TaskService {
    async fn create(
        &self,
        ctx: &TtrpcContext,
        req: api::CreateTaskRequest,
    ) -> TtrpcResult<api::CreateTaskResponse> {
        guard("create", self.handle_create(ctx, req)).await
    }

    async fn start(
        &self,
        ctx: &TtrpcContext,
        req: api::StartRequest,
    ) -> TtrpcResult<api::StartResponse> {
        guard("start", self.handle_start(ctx, req)).await
    }

    async fn state(
        &self,
        _ctx: &TtrpcContext,
        req: api::StateRequest,
    ) -> TtrpcResult<api::StateResponse> {
        guard("state", self.handle_state(req)).await
    }

    async fn wait(
        &self,
        _ctx: &TtrpcContext,
        req: api::WaitRequest,
    ) -> TtrpcResult<api::WaitResponse> {
        guard("wait", self.handle_wait(req)).await
    }

    async fn kill(&self, ctx: &TtrpcContext, req: api::KillRequest) -> TtrpcResult<api::Empty> {
        guard("kill", self.handle_kill(ctx, req)).await?;
        Ok(api::Empty::default())
    }

    async fn exec(
        &self,
        ctx: &TtrpcContext,
        req: api::ExecProcessRequest,
    ) -> TtrpcResult<api::Empty> {
        guard("exec", self.handle_exec(ctx, req)).await?;
        Ok(api::Empty::default())
    }

    async fn resize_pty(
        &self,
        ctx: &TtrpcContext,
        req: api::ResizePtyRequest,
    ) -> TtrpcResult<api::Empty> {
        guard("resize_pty", self.handle_resize_pty(ctx, req)).await?;
        Ok(api::Empty::default())
    }

    async fn close_io(
        &self,
        ctx: &TtrpcContext,
        req: api::CloseIORequest,
    ) -> TtrpcResult<api::Empty> {
        guard("close_io", self.handle_close_io(ctx, req)).await?;
        Ok(api::Empty::default())
    }

    async fn delete(
        &self,
        ctx: &TtrpcContext,
        req: api::DeleteRequest,
    ) -> TtrpcResult<api::DeleteResponse> {
        guard("delete", self.handle_delete(ctx, req)).await
    }

    async fn pids(
        &self,
        _ctx: &TtrpcContext,
        req: api::PidsRequest,
    ) -> TtrpcResult<api::PidsResponse> {
        guard("pids", self.handle_pids(req)).await
    }

    async fn stats(
        &self,
        _ctx: &TtrpcContext,
        req: api::StatsRequest,
    ) -> TtrpcResult<api::StatsResponse> {
        guard("stats", self.handle_stats(req)).await
    }

    async fn connect(
        &self,
        _ctx: &TtrpcContext,
        req: api::ConnectRequest,
    ) -> TtrpcResult<api::ConnectResponse> {
        guard("connect", self.handle_connect(req)).await
    }

    async fn shutdown(
        &self,
        _ctx: &TtrpcContext,
        _req: api::ShutdownRequest,
    ) -> TtrpcResult<api::Empty> {
        guard("shutdown", self.handle_shutdown()).await?;
        Ok(api::Empty::default())
    }

    async fn update(
        &self,
        _ctx: &TtrpcContext,
        _req: api::UpdateTaskRequest,
    ) -> TtrpcResult<api::Empty> {
        // Accepted as a no-op: the guest cgroup follows the spec set at create.
        Ok(api::Empty::default())
    }
}

#[cfg(test)]
mod tests;
