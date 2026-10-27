//! The Task API against a node service with mock pods, in this process.

use std::collections::BTreeMap;
use std::io::Read as _;
use std::path::Path;

use containerd_shim_protos::events::task::TaskExit;

use super::*;
use crate::backend::MockBackend;
use crate::node::{MockBackends, Node};
use crate::sync::lock;

/// Handlers that panic, as `(method, task id)`.
static ARMED: std::sync::Mutex<Vec<(String, String)>> = std::sync::Mutex::new(Vec::new());

pub(super) fn fail_point(method: &str, id: &str) {
    let armed = lock(&ARMED).iter().any(|(m, i)| m == method && i == id);
    if armed {
        panic!("fail point {method} for {id}");
    }
}

fn arm(method: &str, id: &str, on: bool) {
    let mut armed = lock(&ARMED);
    armed.retain(|(m, i)| !(m == method && i == id));
    if on {
        armed.push((method.to_string(), id.to_string()));
    }
}

/// A node service on its own runtime, so that a test can kill it: dropping
/// the runtime ends every task and connection of the service at once.
struct NodeProcess {
    runtime: Option<tokio::runtime::Runtime>,
    node: Arc<std::sync::Mutex<Option<Node>>>,
}

impl NodeProcess {
    fn start(
        socket: &Path,
        backends: Arc<MockBackends>,
        state: PathBuf,
        abandon_after: Duration,
    ) -> NodeProcess {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let _ = std::fs::remove_file(socket);
        let listener = {
            let _entered = runtime.enter();
            tokio::net::UnixListener::bind(socket).unwrap()
        };
        let slot = Arc::new(std::sync::Mutex::new(None));
        let opened = slot.clone();
        runtime.spawn(async move {
            let node = Node::open(backends, state, abandon_after).await.unwrap();
            *opened.lock().unwrap() = Some(node.clone());
            node.serve(listener).await
        });
        NodeProcess {
            runtime: Some(runtime),
            node: slot,
        }
    }
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        // As at a SIGKILL, the state directory's lock goes with the process.
        if let Some(node) = self.node.lock().unwrap().take() {
            node.shutdown();
        }
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

/// A TaskExit event: container id, process id and status.
pub(super) type ExitEvent = (String, String, u32);

/// The events the Task service published.
#[derive(Default)]
pub(super) struct Recorder {
    events: std::sync::Mutex<Vec<(String, Option<ExitEvent>)>>,
}

#[async_trait]
impl Events for Recorder {
    async fn publish(&self, topic: &str, event: Box<dyn MessageDyn>) {
        let exit = event
            .downcast_ref::<TaskExit>()
            .map(|e| (e.container_id.clone(), e.id.clone(), e.exit_status));
        self.events.lock().unwrap().push((topic.to_string(), exit));
    }
}

impl Recorder {
    pub(super) fn exits(&self) -> Vec<ExitEvent> {
        let events = self.events.lock().unwrap();
        events.iter().filter_map(|(_, exit)| exit.clone()).collect()
    }

    pub(super) fn topics(&self) -> Vec<String> {
        let events = self.events.lock().unwrap();
        events.iter().map(|(topic, _)| topic.clone()).collect()
    }
}

pub(super) struct Harness {
    _dir: tempfile::TempDir,
    pub dir: PathBuf,
    socket: PathBuf,
    pub backends: Arc<MockBackends>,
    node: Option<NodeProcess>,
    pub svc: TaskService,
    pub events: Arc<Recorder>,
}

impl Harness {
    /// A Task service and its node service.
    pub(super) fn new() -> Harness {
        let mut h = Harness::without_node();
        h.start_node();
        h
    }

    /// A Task service whose node service is not running.
    pub(super) fn without_node() -> Harness {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let socket = path.join("node.sock");
        let events = Arc::new(Recorder::default());
        let svc = TaskService::new(
            socket.clone(),
            "k8s.io".into(),
            Some(events.clone()),
            Arc::new(ExitSignal::default()),
        );
        Harness {
            _dir: dir,
            dir: path,
            socket,
            backends: Arc::new(MockBackends::default()),
            node: None,
            svc,
            events,
        }
    }

    /// Another front shim of the same namespace, with an empty table.
    pub(super) fn another_front(&self) -> TaskService {
        TaskService::new(
            self.socket.clone(),
            "k8s.io".into(),
            None,
            Arc::new(ExitSignal::default()),
        )
    }

    pub(super) fn start_node(&mut self) {
        self.start_node_with(Duration::from_secs(300));
    }

    /// Start the node service, which deletes a pod no front shim claims
    /// after `abandon_after`.
    pub(super) fn start_node_with(&mut self, abandon_after: Duration) {
        let state = self.dir.join("state");
        let backends = self.backends.clone();
        self.node = Some(NodeProcess::start(
            &self.socket,
            backends,
            state,
            abandon_after,
        ));
    }

    /// End the node service at once, as SIGKILL would.
    pub(super) fn kill_node(&mut self) {
        self.node = None;
    }

    pub(super) fn node_up(&self) -> bool {
        self.node.is_some()
    }

    pub(super) fn mock(&self, pod: &str) -> Arc<MockBackend> {
        self.backends.get(pod).expect("no such mock pod")
    }

    /// Wait until the Task service has had `n` sessions with the node service.
    pub(super) async fn sessions(&self, n: u64) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.svc.shared.node.sessions() < n || !self.svc.shared.node.is_connected() {
            assert!(
                Instant::now() < deadline,
                "no session {n} with the node service"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Write the bundle of a task whose CRI container type is `kind`, in the
    /// pod of sandbox `sandbox`. With no kind, the task is a standalone
    /// container, as Docker's are, with a new network namespace.
    pub(super) fn bundle(&self, id: &str, kind: &str, sandbox: &str) -> String {
        let bundle = self.dir.join("bundles").join(id);
        std::fs::create_dir_all(bundle.join("rootfs")).unwrap();
        let mut annotations = serde_json::json!({"io.kubernetes.cri.container-type": kind});
        if kind.is_empty() {
            annotations = serde_json::json!({});
        }
        if kind == "container" {
            annotations["io.kubernetes.cri.sandbox-id"] = sandbox.into();
        }
        std::fs::write(
            bundle.join("config.json"),
            serde_json::json!({
                "ociVersion": "1.0.2",
                "process": {"args": ["sleep", "1"], "cwd": "/"},
                "root": {"path": "rootfs"},
                "linux": {"namespaces": [{"type": "pid"}, {"type": "network"}]},
                "annotations": annotations,
            })
            .to_string(),
        )
        .unwrap();
        bundle.to_string_lossy().into_owned()
    }

    pub(super) async fn create_with(
        &self,
        ctx: &TtrpcContext,
        id: &str,
        kind: &str,
        sandbox: &str,
        stdout: &str,
    ) -> TtrpcResult<api::CreateTaskResponse> {
        let req = api::CreateTaskRequest {
            id: id.to_string(),
            bundle: self.bundle(id, kind, sandbox),
            stdout: stdout.to_string(),
            ..Default::default()
        };
        self.svc.create(ctx, req).await
    }

    pub(super) async fn create(
        &self,
        id: &str,
        kind: &str,
        sandbox: &str,
    ) -> TtrpcResult<api::CreateTaskResponse> {
        self.create_with(&ttctx(), id, kind, sandbox, "").await
    }

    /// Create and start sandbox `sb`.
    pub(super) async fn sandbox(&self, sb: &str) {
        self.create(sb, "sandbox", sb).await.unwrap();
        self.start(sb, "").await.unwrap();
    }

    /// Create and start container `id` in sandbox `sb`.
    pub(super) async fn container(&self, id: &str, sb: &str) {
        self.create(id, "container", sb).await.unwrap();
        self.start(id, "").await.unwrap();
    }

    pub(super) async fn start_with(
        &self,
        ctx: &TtrpcContext,
        id: &str,
        exec_id: &str,
    ) -> TtrpcResult<api::StartResponse> {
        let req = api::StartRequest {
            id: id.into(),
            exec_id: exec_id.into(),
            ..Default::default()
        };
        self.svc.start(ctx, req).await
    }

    pub(super) async fn start(&self, id: &str, exec_id: &str) -> TtrpcResult<api::StartResponse> {
        self.start_with(&ttctx(), id, exec_id).await
    }

    pub(super) async fn state(&self, id: &str, exec_id: &str) -> TtrpcResult<api::StateResponse> {
        let req = api::StateRequest {
            id: id.into(),
            exec_id: exec_id.into(),
            ..Default::default()
        };
        self.svc.state(&ttctx(), req).await
    }

    pub(super) async fn status(&self, id: &str, exec_id: &str) -> api::Status {
        self.state(id, exec_id).await.unwrap().status.unwrap()
    }

    pub(super) async fn wait(&self, id: &str, exec_id: &str) -> TtrpcResult<api::WaitResponse> {
        let req = api::WaitRequest {
            id: id.into(),
            exec_id: exec_id.into(),
            ..Default::default()
        };
        self.svc.wait(&ttctx(), req).await
    }

    pub(super) async fn kill_with(
        &self,
        ctx: &TtrpcContext,
        id: &str,
        exec_id: &str,
        signal: u32,
    ) -> TtrpcResult<()> {
        let req = api::KillRequest {
            id: id.into(),
            exec_id: exec_id.into(),
            signal,
            all: true,
            ..Default::default()
        };
        self.svc.kill(ctx, req).await.map(|_| ())
    }

    pub(super) async fn kill(&self, id: &str, exec_id: &str, signal: u32) -> TtrpcResult<()> {
        self.kill_with(&ttctx(), id, exec_id, signal).await
    }

    pub(super) async fn exec_with(
        &self,
        ctx: &TtrpcContext,
        id: &str,
        exec_id: &str,
    ) -> TtrpcResult<()> {
        let mut req = api::ExecProcessRequest {
            id: id.into(),
            exec_id: exec_id.into(),
            ..Default::default()
        };
        req.spec =
            Some(containerd_shim_protos::protobuf::well_known_types::any::Any::default()).into();
        self.svc.exec(ctx, req).await.map(|_| ())
    }

    pub(super) async fn exec(&self, id: &str, exec_id: &str) -> TtrpcResult<()> {
        self.exec_with(&ttctx(), id, exec_id).await
    }

    pub(super) async fn delete_with(
        &self,
        ctx: &TtrpcContext,
        id: &str,
        exec_id: &str,
    ) -> TtrpcResult<api::DeleteResponse> {
        let req = api::DeleteRequest {
            id: id.into(),
            exec_id: exec_id.into(),
            ..Default::default()
        };
        self.svc.delete(ctx, req).await
    }

    pub(super) async fn delete(&self, id: &str, exec_id: &str) -> TtrpcResult<api::DeleteResponse> {
        self.delete_with(&ttctx(), id, exec_id).await
    }

    pub(super) async fn pids(&self, id: &str) -> TtrpcResult<Vec<u32>> {
        let req = api::PidsRequest {
            id: id.into(),
            ..Default::default()
        };
        let resp = self.svc.pids(&ttctx(), req).await?;
        Ok(resp.processes.iter().map(|p| p.pid).collect())
    }

    /// The CPU total and memory of the container's stats, if any.
    pub(super) async fn stats(&self, id: &str) -> TtrpcResult<Option<(u64, u64)>> {
        let req = api::StatsRequest {
            id: id.into(),
            ..Default::default()
        };
        let resp = self.svc.stats(&ttctx(), req).await?;
        Ok(resp.stats.into_option().map(|any| {
            let metrics =
                containerd_shim_protos::cgroups::metrics::Metrics::parse_from_bytes(&any.value)
                    .unwrap();
            (metrics.cpu.usage.total, metrics.memory.usage.usage)
        }))
    }

    /// The shim's `delete` action for sandbox `sb`, which containerd runs
    /// once it has deleted the sandbox task and closed its connection.
    pub(super) async fn delete_action(&self, sb: &str) {
        let graves = crate::tombstone::dir(&self.dir.join("state"));
        crate::front::service::drop_sandbox(&self.socket, sb, None, &graves).await;
    }

    /// Wait for the process's exit, failing after 5 seconds.
    pub(super) async fn exit_of(&self, id: &str, exec_id: &str) -> api::WaitResponse {
        tokio::time::timeout(Duration::from_secs(5), self.wait(id, exec_id))
            .await
            .unwrap_or_else(|_| panic!("{id}/{exec_id} did not exit"))
            .unwrap()
    }

    /// Whether a Wait for the process stays blocked for `time`.
    pub(super) async fn still_waiting(&self, id: &str, exec_id: &str, time: Duration) -> bool {
        tokio::time::timeout(time, self.wait(id, exec_id))
            .await
            .is_err()
    }
}

use containerd_shim_protos::protobuf::Message as _;

pub(super) fn ttctx() -> TtrpcContext {
    TtrpcContext {
        mh: Default::default(),
        metadata: Default::default(),
        timeout_nano: 0,
    }
}

/// A context whose deadline is `ms` milliseconds away.
pub(super) fn deadline(ms: u64) -> TtrpcContext {
    TtrpcContext {
        timeout_nano: Duration::from_millis(ms).as_nanos() as i64,
        ..ttctx()
    }
}

fn code_of<T: std::fmt::Debug>(result: TtrpcResult<T>) -> RpcCode {
    match result {
        Err(ttrpc::Error::RpcStatus(status)) => status.code(),
        other => panic!("expected an RPC error, not {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn lifecycle_create_start_kill_wait_delete() {
    let h = Harness::new();
    h.sandbox("sb").await;
    let created = h.create("c1", "container", "sb").await.unwrap();
    assert_eq!(created.pid, 100);
    assert_eq!(h.status("c1", "").await, api::Status::CREATED);

    h.start("c1", "").await.unwrap();
    assert_eq!(h.status("c1", "").await, api::Status::RUNNING);

    // Kill(SIGKILL) → exit 137; Wait resolves.
    h.kill("c1", "", 9).await.unwrap();
    let w = h.exit_of("c1", "").await;
    assert_eq!(w.exit_status, 137);

    // Delete returns the same exit info.
    let d = h.delete("c1", "").await.unwrap();
    assert_eq!(d.exit_status, 137);
    assert!(h.state("c1", "").await.is_err());
    assert_eq!(h.events.exits(), [("c1".into(), "c1".into(), 137)]);
}

#[tokio::test(flavor = "current_thread")]
async fn exec_lifecycle() {
    let h = Harness::new();
    h.sandbox("sb").await;
    h.container("c2", "sb").await;
    h.exec("c2", "e1").await.unwrap();
    let started = h.start("c2", "e1").await.unwrap();
    assert_eq!(started.pid, 200);

    h.mock("sb").finish("c2", Some("e1"), 0).await;
    assert_eq!(h.exit_of("c2", "e1").await.exit_status, 0);
    assert_eq!(h.status("c2", "").await, api::Status::RUNNING);
}

#[tokio::test(flavor = "current_thread")]
async fn wait_after_exit_returns_immediately() {
    let h = Harness::new();
    h.sandbox("sb").await;
    h.container("c3", "sb").await;
    h.mock("sb").finish("c3", None, 7).await;
    assert_eq!(h.exit_of("c3", "").await.exit_status, 7);
    let w = tokio::time::timeout(Duration::from_millis(100), h.wait("c3", ""))
        .await
        .expect("a second Wait returns at once")
        .unwrap();
    assert_eq!(w.exit_status, 7);
    assert_eq!(h.status("c3", "").await, api::Status::STOPPED);
}

#[tokio::test(flavor = "current_thread")]
async fn state_and_delete_match_wait() {
    let h = Harness::new();
    h.sandbox("sb").await;
    h.container("c4", "sb").await;
    // An unknown exit time, which a backend may report for a killed sandbox.
    h.mock("sb").finish_at("c4", None, 3, 0).await;

    let w = h.exit_of("c4", "").await;
    assert_eq!(w.exit_status, 3);
    // containerd's client checks State before it calls Delete.
    let st = h.state("c4", "").await.unwrap();
    assert_eq!(st.status.unwrap(), api::Status::STOPPED);
    assert_eq!(st.exit_status, 3);
    assert_eq!(st.exited_at, w.exited_at);

    // An exit time computed on each read would now differ from Wait's.
    tokio::time::sleep(Duration::from_millis(10)).await;
    let d = h.delete("c4", "").await.unwrap();
    assert_eq!(d.exit_status, 3);
    assert_eq!(d.exited_at, w.exited_at);
}

// containerd kills a sandbox whose setup failed, or a container whose Start
// failed, then waits for the task and deletes it.
#[tokio::test(flavor = "current_thread")]
async fn kill_before_start_stops_the_task() {
    let h = Harness::new();
    for (id, kind) in [("sb", "sandbox"), ("c5", "container")] {
        h.create(id, kind, "sb").await.unwrap();
        let waiting = tokio::spawn({
            let svc = h.svc.clone();
            let id = id.to_string();
            async move {
                let req = api::WaitRequest {
                    id,
                    ..Default::default()
                };
                svc.wait(&ttctx(), req).await
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        h.kill(id, "", 9).await.unwrap();
        let w = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .unwrap_or_else(|_| panic!("Wait for the killed {kind} did not return"))
            .unwrap()
            .unwrap();
        assert_eq!(w.exit_status, 137);
        let st = h.state(id, "").await.unwrap();
        assert_eq!(st.status.unwrap(), api::Status::STOPPED);
        assert_eq!(st.exit_status, 137);
        assert_eq!(st.exited_at, w.exited_at);

        // A late Start cannot reopen it.
        assert!(h.start(id, "").await.is_err(), "the killed {kind} started");
        if kind == "container" {
            let d = h.delete(id, "").await.unwrap();
            assert_eq!(d.exit_status, 137);
            assert_eq!(d.exited_at, w.exited_at);
        }
    }
    let d = h.delete("sb", "").await.unwrap();
    assert_eq!(d.exit_status, 137);
}

#[tokio::test(flavor = "current_thread")]
async fn kill_during_start_keeps_the_exit() {
    let h = Harness::new();
    h.sandbox("sb").await;
    h.create("c6", "container", "sb").await.unwrap();
    let mock = h.mock("sb");
    let held = mock.hold_start().await;
    let starting = tokio::spawn({
        let svc = h.svc.clone();
        async move {
            let req = api::StartRequest {
                id: "c6".into(),
                ..Default::default()
            };
            svc.start(&ttctx(), req).await
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    h.kill("c6", "", 9).await.unwrap();
    drop(held);

    let Err(ttrpc::Error::RpcStatus(status)) = starting.await.unwrap() else {
        panic!("Start of a task killed while starting succeeded");
    };
    assert!(
        status.message.contains("killed while starting"),
        "{}",
        status.message
    );
    let st = h.state("c6", "").await.unwrap();
    assert_eq!(st.status.unwrap(), api::Status::STOPPED);
    assert_eq!(st.exit_status, 137);
    assert_eq!(h.events.exits(), [("c6".into(), "c6".into(), 137)]);
}

/// A sandbox's kill ends its nominal process at once, and its exit is
/// reported once, though both the answer to the kill and the node service's
/// exit report carry it.
#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_exit_is_reported_once() {
    let h = Harness::new();
    h.sandbox("sb").await;
    h.kill("sb", "", 15).await.unwrap();
    assert_eq!(h.exit_of("sb", "").await.exit_status, 137);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(h.events.exits(), [("sb".into(), "sb".into(), 137)]);
    let topics = h.events.topics();
    assert_eq!(
        topics,
        ["/tasks/create", "/tasks/start", "/tasks/exit"].map(String::from)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_container_needs_its_sandbox() {
    let h = Harness::new();
    let Err(ttrpc::Error::RpcStatus(status)) = h.create("c", "container", "sb").await else {
        panic!("a container without its sandbox was created");
    };
    assert_eq!(status.code(), RpcCode::NOT_FOUND);
    assert!(status.message.contains("no sandbox"), "{}", status.message);
}

/// While the node service is down, State, Connect, Pids and Wait answer from
/// the table at once, Stats answers its last sample within its limit, and
/// every call that needs the node service answers Unavailable at its
/// deadline.
#[tokio::test(flavor = "current_thread")]
async fn every_call_answers_while_the_node_service_is_down() {
    let mut h = Harness::new();
    h.sandbox("sb").await;
    h.container("c", "sb").await;
    h.create("idle", "container", "sb").await.unwrap();
    h.mock("sb").set_stats(Some(StatsSample {
        cpu_usage_ns: 500,
        memory_bytes: 4096,
        ..Default::default()
    }));
    assert_eq!(h.stats("c").await.unwrap(), Some((500, 4096)));
    h.sessions(1).await;
    h.kill_node();

    let quick = Duration::from_millis(100);
    let at_once = |started: Instant| started.elapsed() < quick;
    let started = Instant::now();
    assert_eq!(h.status("c", "").await, api::Status::RUNNING);
    assert_eq!(h.status("sb", "").await, api::Status::RUNNING);
    let pids = h.pids("c").await.unwrap();
    assert!(!pids.is_empty() && pids[0] == 100, "{pids:?}");
    assert_eq!(h.pids("sb").await.unwrap(), [1]);
    let connect = api::ConnectRequest {
        id: "c".into(),
        ..Default::default()
    };
    assert_eq!(
        h.svc.connect(&ttctx(), connect).await.unwrap().task_pid,
        100
    );
    let none = ttctx();
    let shutdown = h.svc.shutdown(&none, api::ShutdownRequest::default());
    shutdown.await.unwrap();
    let update = h.svc.update(&none, api::UpdateTaskRequest::default());
    update.await.unwrap();
    assert!(at_once(started), "{:?}", started.elapsed());
    assert!(h.still_waiting("c", "", Duration::from_millis(200)).await);

    let started = Instant::now();
    assert_eq!(h.stats("c").await.unwrap(), Some((500, 4096)));
    let took = started.elapsed();
    assert!(took < Duration::from_millis(900), "Stats took {took:?}");
    // With no sample to answer, an error, which containerd skips, and not an
    // empty answer, which fails CRI's whole listing.
    assert_eq!(code_of(h.stats("idle").await), RpcCode::UNAVAILABLE);

    let unavailable = |what: &str, started: Instant, code: RpcCode| {
        let took = started.elapsed();
        assert_eq!(code, RpcCode::UNAVAILABLE, "{what}");
        assert!(
            took >= Duration::from_millis(200) && took < Duration::from_millis(900),
            "{what} took {took:?}"
        );
    };
    let ctx = deadline(300);
    let t = Instant::now();
    let created = h.create_with(&ctx, "c2", "container", "sb", "").await;
    unavailable("create", t, code_of(created));
    let t = Instant::now();
    unavailable("start", t, code_of(h.start_with(&ctx, "idle", "").await));
    let t = Instant::now();
    unavailable("exec", t, code_of(h.exec_with(&ctx, "c", "e").await));
    let t = Instant::now();
    unavailable("kill", t, code_of(h.kill_with(&ctx, "c", "", 9).await));
    let t = Instant::now();
    unavailable("delete", t, code_of(h.delete_with(&ctx, "c", "").await));
    let resize = api::ResizePtyRequest {
        id: "c".into(),
        ..Default::default()
    };
    let t = Instant::now();
    unavailable("resize", t, code_of(h.svc.resize_pty(&ctx, resize).await));
    let close = api::CloseIORequest {
        id: "c".into(),
        ..Default::default()
    };
    let t = Instant::now();
    unavailable("close_io", t, code_of(h.svc.close_io(&ctx, close).await));

    // Nothing changed, and a call without a deadline waits for the node
    // service to come back.
    assert_eq!(h.status("c", "").await, api::Status::RUNNING);
    assert_eq!(h.status("idle", "").await, api::Status::CREATED);
    let killing = tokio::spawn({
        let svc = h.svc.clone();
        async move {
            let req = api::KillRequest {
                id: "c".into(),
                signal: 9,
                ..Default::default()
            };
            svc.kill(&ttctx(), req).await
        }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!killing.is_finished());
    h.start_node();
    tokio::time::timeout(Duration::from_secs(5), killing)
        .await
        .expect("the kill waited for the node service")
        .unwrap()
        .unwrap();
    assert_eq!(h.exit_of("c", "").await.exit_status, 137);
    assert_eq!(h.events.exits(), [("c".into(), "c".into(), 137)]);
}

/// The node service repeats an exit until the front shim acks it, and the
/// front shim reports it once.
#[tokio::test(flavor = "current_thread")]
async fn an_exit_is_reported_once_across_sessions() {
    let mut h = Harness::new();
    h.sandbox("sb").await;
    h.container("c", "sb").await;
    h.mock("sb").finish("c", None, 4).await;
    assert_eq!(h.exit_of("c", "").await.exit_status, 4);
    h.sessions(1).await;
    h.kill_node();
    h.start_node();
    h.sessions(2).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(h.events.exits(), [("c".into(), "c".into(), 4)]);
}

/// Two front shims of a namespace can run for a moment, when the `start`
/// actions of two tasks race: the exits of each one's tasks reach it, also
/// after the other's session opened.
#[tokio::test(flavor = "current_thread")]
async fn exits_reach_their_front_shim_while_another_has_a_session() {
    let h = Harness::new();
    h.sandbox("sb").await;
    h.container("c", "sb").await;
    h.sessions(1).await;
    let other = h.another_front();
    let deadline = Instant::now() + Duration::from_secs(5);
    while other.shared.node.sessions() == 0 {
        assert!(
            Instant::now() < deadline,
            "no session of the other front shim"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    h.mock("sb").finish("c", None, 3).await;
    assert_eq!(h.exit_of("c", "").await.exit_status, 3);
    drop(other);
}

/// A restarted node service adopts the pods: their processes stay running
/// with no exit, an exit during the outage is reported once, and the pods
/// work as before.
#[tokio::test(flavor = "current_thread")]
async fn pods_outlive_a_restart_of_the_node_service() {
    let mut h = Harness::new();
    h.sandbox("sb").await;
    h.container("c1", "sb").await;
    h.container("c2", "sb").await;
    h.exec("c1", "e").await.unwrap();
    h.start("c1", "e").await.unwrap();
    h.create("c3", "container", "sb").await.unwrap();
    h.sessions(1).await;

    h.kill_node();
    h.mock("sb").finish("c2", None, 3).await;
    h.start_node();
    h.sessions(2).await;

    assert_eq!(h.exit_of("c2", "").await.exit_status, 3);
    for (id, exec_id) in [("sb", ""), ("c1", ""), ("c1", "e")] {
        assert_eq!(h.status(id, exec_id).await, api::Status::RUNNING);
        assert!(
            h.still_waiting(id, exec_id, Duration::from_millis(50))
                .await
        );
    }
    // The pod works: a container created before starts, a kill reaches its
    // process, a new exec runs.
    h.start("c3", "").await.unwrap();
    h.kill("c1", "e", 9).await.unwrap();
    assert_eq!(h.exit_of("c1", "e").await.exit_status, 137);
    h.exec("c1", "e2").await.unwrap();
    h.start("c1", "e2").await.unwrap();
    h.mock("sb").finish("c1", Some("e2"), 0).await;
    assert_eq!(h.exit_of("c1", "e2").await.exit_status, 0);
    let mut exits = h.events.exits();
    exits.sort();
    let expected = [("c1", "e", 137), ("c1", "e2", 0), ("c2", "c2", 3)];
    assert_eq!(exits, expected.map(|(c, p, s)| (c.into(), p.into(), s)));
    assert_eq!(h.delete("c2", "").await.unwrap().exit_status, 3);
}

/// A pod whose VM ended while no node service ran ends every process with
/// 255, in the order CRI needs. The sandbox and the exec exit at once, and
/// containerd deletes the sandbox; each container exits only when killed, as
/// kubelet does once CRI reports the sandbox not ready. A container exit
/// before that would make kubelet restart the container in the old sandbox.
#[tokio::test(flavor = "current_thread")]
async fn a_pod_whose_vm_is_gone_ends_each_container_at_its_kill() {
    let mut h = Harness::new();
    h.sandbox("sb").await;
    h.container("c1", "sb").await;
    h.container("c2", "sb").await;
    h.exec("c1", "e").await.unwrap();
    h.start("c1", "e").await.unwrap();
    h.sessions(1).await;
    h.kill_node();
    h.backends.crash("sb");
    h.start_node();
    for (id, exec_id) in [("sb", ""), ("c1", "e")] {
        assert_eq!(h.exit_of(id, exec_id).await.exit_status, 255, "{id}");
    }
    assert_eq!(h.delete("sb", "").await.unwrap().exit_status, 255);
    assert_eq!(*h.backends.reaped().lock().unwrap(), ["sb"]);
    h.delete_action("sb").await;
    for id in ["c1", "c2"] {
        assert!(
            h.still_waiting(id, "", Duration::from_millis(200)).await,
            "{id}"
        );
    }

    h.kill("c1", "", 15).await.unwrap();
    assert_eq!(h.exit_of("c1", "").await.exit_status, 255);
    assert!(h.still_waiting("c2", "", Duration::from_millis(200)).await);
    h.kill("c2", "", 9).await.unwrap();
    assert_eq!(h.exit_of("c2", "").await.exit_status, 255);
    let mut exits = h.events.exits();
    exits[..2].sort();
    let lost = |c: &str, p: &str| (c.to_string(), p.to_string(), 255);
    let order = [
        lost("c1", "e"),
        lost("sb", "sb"),
        lost("c1", "c1"),
        lost("c2", "c2"),
    ];
    assert_eq!(exits, order);
    for id in ["c1", "c2"] {
        assert_eq!(h.delete(id, "").await.unwrap().exit_status, 255, "{id}");
    }
}

/// A pod that no front shim claims after a restart of the front shim is
/// deleted, and one that is claimed is not.
#[tokio::test(flavor = "current_thread")]
async fn a_pod_no_front_shim_claims_is_deleted() {
    let mut h = Harness::without_node();
    h.start_node_with(Duration::from_millis(300));
    h.sandbox("sb").await;
    h.container("c", "sb").await;
    h.sessions(1).await;
    let mock = h.mock("sb");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(mock.has("sb", None).await, "a claimed pod was deleted");

    // A new front shim, as after a crash of the old one, has no task.
    let other = h.another_front();
    let deadline = Instant::now() + Duration::from_secs(5);
    while other.shared.node.sessions() == 0 {
        assert!(
            Instant::now() < deadline,
            "no session of the new front shim"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while mock.has("sb", None).await {
        assert!(Instant::now() < deadline, "the abandoned pod stayed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(other);
}

/// A container with no CRI annotations, as Docker's, is a pod of its own:
/// its task is the container, with execs, stats and pids, and its delete
/// deletes the pod.
#[tokio::test(flavor = "current_thread")]
async fn a_standalone_container_is_a_pod_of_its_own() {
    let h = Harness::new();
    let created = h.create("web", "", "").await.unwrap();
    assert_eq!(created.pid, 100);
    let mock = h.mock("web");
    assert_eq!(h.status("web", "").await, api::Status::CREATED);
    h.start("web", "").await.unwrap();
    assert_eq!(h.status("web", "").await, api::Status::RUNNING);

    h.exec("web", "e1").await.unwrap();
    assert_eq!(h.start("web", "e1").await.unwrap().pid, 200);
    mock.finish("web", Some("e1"), 0).await;
    assert_eq!(h.exit_of("web", "e1").await.exit_status, 0);
    h.delete("web", "e1").await.unwrap();

    mock.set_stats(Some(StatsSample {
        cpu_usage_ns: 5,
        memory_bytes: 7,
        ..Default::default()
    }));
    assert_eq!(h.stats("web").await.unwrap(), Some((5, 7)));
    assert!(h.pids("web").await.unwrap().contains(&100));

    h.kill("web", "", 15).await.unwrap();
    assert_eq!(h.exit_of("web", "").await.exit_status, 137);
    assert_eq!(h.delete("web", "").await.unwrap().exit_status, 137);
    assert!(
        !mock.has("web", None).await,
        "the pod outlived its container"
    );
    let exits = h.events.exits();
    assert_eq!(exits.last(), Some(&("web".into(), "web".into(), 137)));
}

/// dockerd reads the CPU throttling of a container's stats without a check,
/// and reports the memory limit, which is the pod VM's memory.
#[test]
fn old_nodes_report_the_original_vm_memory() {
    let metrics = cgroup_metrics(StatsSample {
        cpu_usage_ns: 5,
        memory_bytes: 7,
        ..Default::default()
    });
    assert!(metrics.cpu.throttling.is_some());
    assert_eq!(metrics.cpu.usage.total, 5);
    assert_eq!(metrics.memory.usage.usage, 7);
    assert_eq!(metrics.memory.usage.limit, 1024 * 1024 * 1024);
}

/// containerd runs the delete action after every Delete and gives a new
/// task the old one's id, as Docker's restarts do: an action for a
/// standalone container that a live front shim has deletes nothing.
#[tokio::test(flavor = "current_thread")]
async fn a_delete_action_leaves_a_standalone_container_that_was_created_again() {
    let h = Harness::new();
    h.create("web", "", "").await.unwrap();
    h.start("web", "").await.unwrap();
    h.kill("web", "", 9).await.unwrap();
    h.exit_of("web", "").await;
    h.delete("web", "").await.unwrap();

    h.create("web", "", "").await.unwrap();
    h.start("web", "").await.unwrap();
    h.delete_action("web").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        h.mock("web").has("web", None).await,
        "the new container was deleted"
    );
    assert_eq!(h.status("web", "").await, api::Status::RUNNING);
}

/// A standalone pod whose VM is gone ends its container at once: no sandbox
/// comes first.
#[tokio::test(flavor = "current_thread")]
async fn a_standalone_container_whose_vm_is_gone_exits_at_once() {
    let mut h = Harness::new();
    h.create("web", "", "").await.unwrap();
    h.start("web", "").await.unwrap();
    h.sessions(1).await;
    h.kill_node();
    h.backends.crash("web");
    h.start_node();
    assert_eq!(h.exit_of("web", "").await.exit_status, 255);
    assert_eq!(h.delete("web", "").await.unwrap().exit_status, 255);
}

/// The table holds a container's stdout fifo open while it runs, so that its
/// reader sees no end of the output however its writers come and go.
#[tokio::test(flavor = "current_thread")]
async fn a_running_container_keeps_its_fifo_open() {
    let h = Harness::new();
    h.sandbox("sb").await;
    let fifo = h.dir.join("c-stdout");
    nix::unistd::mkfifo(&fifo, nix::sys::stat::Mode::S_IRWXU).unwrap();
    let mut reader = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(&fifo)
        .unwrap();
    let mut buf = [0u8; 16];
    let at_end = |reader: &mut File, buf: &mut [u8]| match reader.read(buf) {
        Ok(0) => true,
        Ok(n) => panic!("read {n} bytes"),
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => false,
        Err(e) => panic!("read the fifo: {e}"),
    };
    let fifo_path = fifo.to_string_lossy().into_owned();
    h.create_with(&ttctx(), "c", "container", "sb", &fifo_path)
        .await
        .unwrap();
    h.start("c", "").await.unwrap();
    // A writer that comes and goes, as a pump of a restarting node service.
    let pump = std::fs::OpenOptions::new().write(true).open(&fifo).unwrap();
    drop(pump);
    assert!(!at_end(&mut reader, &mut buf), "the reader saw the end");
    h.mock("sb").finish("c", None, 0).await;
    h.exit_of("c", "").await;
    assert!(at_end(&mut reader, &mut buf), "the output did not end");
}

/// Stats answers the last sample when the node service has none, and a CPU
/// total never goes back.
#[tokio::test(flavor = "current_thread")]
async fn stats_are_cached() {
    let h = Harness::new();
    h.sandbox("sb").await;
    h.container("c", "sb").await;
    assert_eq!(h.stats("c").await.unwrap(), None);
    let mock = h.mock("sb");
    mock.set_stats(Some(StatsSample {
        cpu_usage_ns: 900,
        memory_bytes: 10,
        ..Default::default()
    }));
    assert_eq!(h.stats("c").await.unwrap(), Some((900, 10)));
    mock.set_stats(Some(StatsSample {
        cpu_usage_ns: 800,
        memory_bytes: 20,
        ..Default::default()
    }));
    assert_eq!(h.stats("c").await.unwrap(), Some((900, 20)));
    mock.set_stats(None);
    assert_eq!(h.stats("c").await.unwrap(), Some((900, 20)));
}

/// Pids answers from the table and asks the node service for the guest's
/// pids for the next time.
#[tokio::test(flavor = "current_thread")]
async fn pids_come_from_the_table() {
    let h = Harness::new();
    h.sandbox("sb").await;
    h.container("c", "sb").await;
    assert_eq!(h.pids("c").await.unwrap(), [100]);
    let deadline = Instant::now() + Duration::from_secs(5);
    while h.pids("c").await.unwrap() != [100, 1] {
        assert!(Instant::now() < deadline, "the guest's pids never came");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A panic in a handler fails that call alone: the call answers an error,
/// other calls are answered, and the process goes on. Through containerd's
/// own path, a ttrpc server.
#[tokio::test(flavor = "current_thread")]
async fn a_panic_fails_only_its_call() {
    use containerd_shim_protos::shim_async::{create_task, TaskClient};
    use containerd_shim_protos::ttrpc::r#async::{Client, Server};

    let h = Harness::new();
    h.sandbox("panicky").await;
    h.container("panicky-c", "panicky").await;
    let address = format!("unix://{}", h.dir.join("shim.sock").display());
    let task = Arc::new(h.svc.clone()) as Arc<dyn Task + Send + Sync>;
    let mut server = Server::new()
        .bind(&address)
        .unwrap()
        .register_service(create_task(task));
    server.start().await.unwrap();
    let client = TaskClient::new(Client::connect(&address).await.unwrap());
    let ctx = || ttrpc::context::with_timeout(Duration::from_secs(5).as_nanos() as i64);

    let waiting = tokio::spawn({
        let client = client.clone();
        async move {
            let req = api::WaitRequest {
                id: "panicky-c".into(),
                ..Default::default()
            };
            client.wait(ctx(), &req).await
        }
    });
    arm("kill", "panicky-c", true);
    let kill = api::KillRequest {
        id: "panicky-c".into(),
        signal: 9,
        ..Default::default()
    };
    let Err(ttrpc::Error::RpcStatus(status)) = client.kill(ctx(), &kill).await else {
        panic!("a kill that panicked succeeded");
    };
    assert_eq!(status.code(), RpcCode::INTERNAL);
    assert!(
        status.message.contains("failed in kill"),
        "{}",
        status.message
    );

    // The table is not locked up, and the waiter still waits.
    let state = api::StateRequest {
        id: "panicky-c".into(),
        ..Default::default()
    };
    let st = client.state(ctx(), &state).await.unwrap();
    assert_eq!(st.status.unwrap(), api::Status::RUNNING);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiting.is_finished());

    arm("kill", "panicky-c", false);
    client.kill(ctx(), &kill).await.unwrap();
    let w = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(w.exit_status, 137);
    server.shutdown().await.unwrap();
}

/// A deterministic random sequence of calls, with the node service killed and
/// restarted among them, and processes exiting at random. Every call answers
/// within its limit, no handler panics, every exit is reported once, and
/// State, Wait and Delete agree.
#[tokio::test(flavor = "current_thread")]
async fn a_random_sequence_of_calls_keeps_the_table_consistent() {
    let mut h = Harness::new();
    let mut rng = Rng(0x5eed_1234_9876_abcd);
    let mut sandboxes: Vec<String> = Vec::new();
    // Process → its sandbox, for the processes not deleted.
    let mut procs: BTreeMap<(String, String), String> = BTreeMap::new();
    let mut next = 0;
    let mut down_for = 0;
    let quick = Duration::from_millis(150);
    let ctx = || deadline(150);
    let limit = Duration::from_millis(700);

    for step in 0..300 {
        if !h.node_up() {
            if down_for == 0 {
                h.start_node();
            } else {
                down_for -= 1;
            }
        }
        let pick = |rng: &mut Rng, procs: &BTreeMap<(String, String), String>| {
            let keys: Vec<_> = procs.keys().cloned().collect();
            (!keys.is_empty()).then(|| keys[rng.below(keys.len())].clone())
        };
        let started = Instant::now();
        let what = match rng.below(14) {
            0 => {
                next += 1;
                let id = format!("sb{next}");
                let created = h.create_with(&ctx(), &id, "sandbox", &id, "").await;
                if no_panic(created).is_ok() {
                    sandboxes.push(id.clone());
                    procs.insert((id.clone(), String::new()), id);
                }
                "create sandbox"
            }
            1 | 2 if !sandboxes.is_empty() => {
                next += 1;
                let id = format!("c{next}");
                let sb = sandboxes[rng.below(sandboxes.len())].clone();
                let created = h.create_with(&ctx(), &id, "container", &sb, "").await;
                if no_panic(created).is_ok() {
                    procs.insert((id, String::new()), sb);
                }
                "create container"
            }
            3 | 4 => {
                if let Some((id, exec_id)) = pick(&mut rng, &procs) {
                    no_panic(h.start_with(&ctx(), &id, &exec_id).await).ok();
                }
                "start"
            }
            5 => {
                let containers: Vec<_> = procs
                    .iter()
                    .filter(|((id, e), sb)| e.is_empty() && *id != **sb)
                    .map(|((id, _), sb)| (id.clone(), sb.clone()))
                    .collect();
                if !containers.is_empty() {
                    next += 1;
                    let (id, sb) = containers[rng.below(containers.len())].clone();
                    let exec_id = format!("e{next}");
                    if no_panic(h.exec_with(&ctx(), &id, &exec_id).await).is_ok() {
                        procs.insert((id, exec_id), sb);
                    }
                }
                "exec"
            }
            6 => {
                if let Some((id, exec_id)) = pick(&mut rng, &procs) {
                    let signal = [9, 15, 2][rng.below(3)];
                    no_panic(h.kill_with(&ctx(), &id, &exec_id, signal).await).ok();
                }
                "kill"
            }
            7 => {
                if let Some((id, exec_id)) = pick(&mut rng, &procs) {
                    let sb = procs[&(id.clone(), exec_id.clone())].clone();
                    let st = h.state(&id, &exec_id).await.unwrap();
                    let is_sandbox = id == sb && exec_id.is_empty();
                    let has_procs = procs.iter().any(|((i, _), s)| *s == sb && *i != sb);
                    let deletable = st.status.unwrap() == api::Status::STOPPED
                        && !(is_sandbox && has_procs)
                        && !(exec_id.is_empty()
                            && procs.keys().any(|(i, e)| *i == id && !e.is_empty()));
                    if deletable {
                        let deleted = no_panic(h.delete_with(&ctx(), &id, &exec_id).await);
                        if let Ok(d) = deleted {
                            assert_eq!(d.exit_status, st.exit_status, "{id}/{exec_id}");
                            procs.remove(&(id.clone(), exec_id));
                            if is_sandbox {
                                sandboxes.retain(|s| *s != id);
                            }
                        }
                    }
                }
                "delete"
            }
            8 => {
                if let Some((id, exec_id)) = pick(&mut rng, &procs) {
                    let st = h.state(&id, &exec_id).await.unwrap();
                    assert!(
                        started.elapsed() < quick,
                        "State took {:?}",
                        started.elapsed()
                    );
                    if st.status.unwrap() == api::Status::STOPPED {
                        let w = h.exit_of(&id, &exec_id).await;
                        assert_eq!(w.exit_status, st.exit_status, "{id}/{exec_id}");
                        assert_eq!(w.exited_at, st.exited_at, "{id}/{exec_id}");
                    }
                }
                "state"
            }
            9 => {
                if let Some((id, _)) = pick(&mut rng, &procs) {
                    assert!(!h.pids(&id).await.unwrap().is_empty());
                    assert!(
                        started.elapsed() < quick,
                        "Pids took {:?}",
                        started.elapsed()
                    );
                    let stats = h.stats(&id).await;
                    if stats.is_err() {
                        assert_eq!(code_of(stats), RpcCode::UNAVAILABLE);
                    }
                }
                "pids and stats"
            }
            10 | 11 => {
                if let Some((id, exec_id)) = pick(&mut rng, &procs) {
                    let sb = &procs[&(id.clone(), exec_id.clone())];
                    if let Some(mock) = h.backends.get(sb) {
                        let exec = (!exec_id.is_empty()).then_some(exec_id.as_str());
                        mock.finish(&id, exec, rng.below(4) as u32).await;
                    }
                }
                "finish"
            }
            12 if h.node_up() => {
                h.kill_node();
                down_for = rng.below(4);
                "kill the node service"
            }
            _ => "nothing",
        };
        assert!(
            started.elapsed() < limit,
            "step {step} ({what}) took {:?}",
            started.elapsed()
        );
    }

    if !h.node_up() {
        h.start_node();
    }
    // Exits that the node service holds come once a session is open.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut seen = BTreeMap::new();
        for exit in h.events.exits() {
            *seen.entry((exit.0, exit.1)).or_insert(0) += 1;
        }
        for (process, count) in &seen {
            assert_eq!(*count, 1, "{process:?} exited {count} times");
        }
        let mut inconsistent = Vec::new();
        for (id, exec_id) in procs.keys() {
            let st = h.state(id, exec_id).await.unwrap();
            let process = if exec_id.is_empty() { id } else { exec_id };
            let reported = seen.contains_key(&(id.clone(), process.clone()));
            let stopped = st.status.unwrap() == api::Status::STOPPED;
            if reported != stopped {
                inconsistent.push(format!("{id}/{exec_id}: {st:?}"));
            }
        }
        if inconsistent.is_empty() && h.svc.shared.node.is_connected() {
            break;
        }
        assert!(Instant::now() < deadline, "{inconsistent:#?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Fail on an error that says a handler panicked.
fn no_panic<T>(result: TtrpcResult<T>) -> TtrpcResult<T> {
    if let Err(ttrpc::Error::RpcStatus(status)) = &result {
        assert_ne!(status.code(), RpcCode::INTERNAL, "{}", status.message);
    }
    result
}

/// xorshift64*, for a sequence that is the same on every run.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 33) as usize % n
    }
}

#[test]
fn stats_report_the_memory_of_small_and_large_vms() {
    for mib in [512, 768, 2048, 16384] {
        let limit = mib * 1024 * 1024;
        let metrics = cgroup_metrics(StatsSample {
            memory_limit_bytes: Some(limit),
            ..Default::default()
        });
        assert_eq!(metrics.memory.usage.limit, limit);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn stats_keep_the_sandbox_memory_limit_after_node_recovery() {
    let mut h = Harness::new();
    let bundle = h.bundle("sb", "sandbox", "");
    let config = Path::new(&bundle).join("config.json");
    let mut spec: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&config).unwrap()).unwrap();
    spec["annotations"]["smolvm.io/memory-mib"] = "768".into();
    std::fs::write(config, spec.to_string()).unwrap();
    h.svc
        .create(
            &ttctx(),
            api::CreateTaskRequest {
                id: "sb".into(),
                bundle,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    h.start("sb", "").await.unwrap();
    h.container("c", "sb").await;
    h.mock("sb").set_stats(Some(StatsSample {
        memory_bytes: 4096,
        ..Default::default()
    }));
    for restarted in [false, true] {
        if restarted {
            h.kill_node();
            h.start_node();
            h.sessions(2).await;
        }
        let response = h
            .svc
            .stats(
                &ttctx(),
                api::StatsRequest {
                    id: "c".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let metrics = containerd_shim_protos::cgroups::metrics::Metrics::parse_from_bytes(
            &response.stats.unwrap().value,
        )
        .unwrap();
        assert_eq!(metrics.memory.usage.limit, 768 * 1024 * 1024);
        assert_eq!(metrics.memory.usage.usage, 4096);
    }
}
