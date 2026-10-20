//! The shim v2 process lifecycle: what containerd invokes to start and stop
//! the front shim. One front shim serves every task of a containerd namespace.
//!
//! Derived from smolvm's crates/smolvm-shim (Apache-2.0), with changes.

use std::path::{Path, PathBuf};
use std::process::Stdio as ChildStdio;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use containerd_shim::asynchronous::{run, ExitSignal, Shim};
use containerd_shim::publisher::RemotePublisher;
use containerd_shim::util::timestamp;
use containerd_shim::{Config, Error, Flags, StartOpts};
use containerd_shim_protos::api::DeleteResponse;
use log::{info, warn};
use tokio::net::UnixStream;

use super::task::{Containerd, TaskService};
use crate::files;
use crate::proto::{read_frame, write_frame, FromNode, Op, Outcome, ToNode, NODE_SOCKET, VERSION};
use crate::tombstone::{self, Tombstone};

/// containerd runs `containerd-shim-smolvm-pods-v2` for this runtime type.
pub const RUNTIME_ID: &str = "io.containerd.smolvm-pods.v2";

/// The `start` action groups every task of a namespace under this id, so
/// that one front shim serves them all.
const GROUPING: &str = "smolvm-pods";

/// The front shim's log. containerd's log of a shim follows the bundle of its
/// first task, which a front shim outlives.
const LOG: &str = "/var/log/smolvm-pods/front-shim.log";

/// The front shim's OOM score: the kernel ends nearly anything else first,
/// because its end recreates every pod.
const OOM_SCORE_ADJ: i32 = -998;

/// How long the `delete` action tries the node service before it leaves a
/// tombstone. containerd gives the action 5 seconds.
const DROP_WAIT: Duration = Duration::from_millis(2500);

pub struct Service {
    exit: Arc<ExitSignal>,
    namespace: String,
    /// Task id containerd invoked us with. For a sandbox this is the sandbox
    /// id, which is also its VM's machine name.
    id: String,
    /// The task's bundle, if containerd passed it. Its actions otherwise run
    /// in the bundle directory.
    bundle: String,
    /// The daemon's socket, which it removes when it exits.
    socket: String,
}

#[async_trait]
impl Shim for Service {
    type T = TaskService;

    async fn new(_runtime_id: &str, args: &Flags, _config: &mut Config) -> Self {
        Service {
            exit: Arc::new(ExitSignal::default()),
            namespace: args.namespace.clone(),
            id: args.id.clone(),
            bundle: args.bundle.clone(),
            socket: args.socket.clone(),
        }
    }

    async fn start_shim(&mut self, opts: StartOpts) -> Result<String, Error> {
        let address = containerd_shim::socket_address(&opts.address, &opts.namespace, GROUPING);
        let _starting = lock_start(&address).await?;
        if serving(&address).await {
            return Ok(address);
        }
        spawn_front(opts, &address).await?;
        Ok(address)
    }

    /// containerd runs this action whenever a task's connection closes: after
    /// every delete, and for every task after the front shim died. For a
    /// sandbox it tells the node service to delete the VM, or leaves a
    /// tombstone for the node service to find when it starts.
    async fn delete_shim(&mut self) -> Result<DeleteResponse, Error> {
        let bundle = if self.bundle.is_empty() {
            "."
        } else {
            self.bundle.as_str()
        };
        // An unreadable bundle may be a sandbox whose front shim died.
        let info = crate::bundle::load(bundle);
        let is_sandbox = info.as_ref().map(|b| b.is_sandbox).unwrap_or(true);
        if is_sandbox {
            let netns = info.ok().and_then(|b| b.netns);
            drop_sandbox(Path::new(NODE_SOCKET), &self.id, netns, &tombstones()).await;
        }
        Ok(DeleteResponse {
            exit_status: 137,
            exited_at: Some(timestamp()?).into(),
            ..Default::default()
        })
    }

    async fn wait(&mut self) {
        self.exit.wait().await;
        // A `start` action must find no front shim at this address now.
        let socket = self.socket.trim_start_matches("unix://");
        if !socket.is_empty() {
            let _ = std::fs::remove_file(socket);
        }
        info!("front shim of namespace {} exits", self.namespace);
    }

    async fn create_task_service(&self, publisher: RemotePublisher) -> Self::T {
        info!(
            "front shim of namespace {} serves at {}",
            self.namespace, self.socket
        );
        let events = Containerd {
            publisher,
            namespace: self.namespace.clone(),
        };
        TaskService::new(
            PathBuf::from(NODE_SOCKET),
            self.namespace.clone(),
            Some(Arc::new(events)),
            self.exit.clone(),
        )
    }
}

fn tombstones() -> PathBuf {
    tombstone::dir(&crate::node::state_dir())
}

/// Serializes the `start` actions of a namespace, until the daemon that one
/// starts serves: the actions of tasks that start together would otherwise
/// each find no front shim and start one. Closing the file unlocks it, and
/// the daemon does not inherit it.
async fn lock_start(address: &str) -> Result<std::fs::File, Error> {
    let path = format!("{}.lock", address.trim_start_matches("unix://"));
    let locked = tokio::task::spawn_blocking(move || {
        let path = Path::new(&path);
        files::lock(path, 0o666, true).map_err(|e| Error::Other(e.message(path)))
    });
    locked.await.map_err(|e| Error::Other(e.to_string()))?
}

/// Whether a front shim answers at `address`.
async fn serving(address: &str) -> bool {
    let path = address.trim_start_matches("unix://");
    matches!(
        tokio::time::timeout(Duration::from_secs(1), UnixStream::connect(path)).await,
        Ok(Ok(_))
    )
}

/// Start the front shim daemon at `address`, as containerd-shim's `spawn`
/// does, but with the front shim's OOM score and in containerd's cgroup.
async fn spawn_front(opts: StartOpts, address: &str) -> Result<(), Error> {
    let exe = std::env::current_exe().map_err(|e| Error::Other(format!("current exe: {e}")))?;
    let cwd = std::env::current_dir().map_err(|e| Error::Other(format!("current dir: {e}")))?;
    let mut command = tokio::process::Command::new(exe);
    command
        .current_dir(cwd)
        .stdout(ChildStdio::piped())
        .stdin(ChildStdio::null())
        .stderr(ChildStdio::null())
        .args(["-namespace", &opts.namespace, "-id", &opts.id])
        .args(["-address", &opts.address, "-socket", address]);
    if opts.debug {
        command.arg("-debug");
    }
    let mut child = command
        .spawn()
        .map_err(|e| Error::Other(format!("spawn the front shim: {e}")))?;
    // The front shim stays in containerd's cgroup: the shim cgroup that
    // containerd may pass on stdin is that of the first task's pod, which a
    // front shim outlives.
    if let Some(pid) = child.id() {
        let score = format!("/proc/{pid}/oom_score_adj");
        if let Err(e) = std::fs::write(&score, OOM_SCORE_ADJ.to_string()) {
            warn!("write {score}: {e}");
        }
    }
    // The daemon closes its stdout once it serves.
    if let Some(mut out) = child.stdout.take() {
        let _ = tokio::io::copy(&mut out, &mut tokio::io::stderr()).await;
    }
    Ok(())
}

/// Ask the node service to delete sandbox `id`; if it does not answer in
/// time, leave a tombstone in `tombstones` for it to find when it starts.
async fn drop_sandbox(socket: &Path, id: &str, netns: Option<String>, tombstones: &Path) {
    let op = Op::DropSandbox {
        pod: id.to_string(),
        netns: netns.clone(),
    };
    match tokio::time::timeout(DROP_WAIT, call(socket, op)).await {
        Ok(Ok(Ok(_))) => return,
        Ok(Ok(Err(failure))) => warn!("the node service could not delete sandbox {id}: {failure}"),
        Ok(Err(e)) => warn!("ask the node service to delete sandbox {id}: {e}"),
        Err(_) => warn!("the node service did not answer the delete of sandbox {id}"),
    }
    let grave = Tombstone {
        pod: id.to_string(),
        netns,
    };
    let dir = tombstones.to_path_buf();
    match tokio::task::spawn_blocking(move || tombstone::write(&dir, &grave)).await {
        Ok(Ok(())) => info!("left a tombstone for sandbox {id}"),
        Ok(Err(e)) => warn!("leave a tombstone for sandbox {id}: {e}"),
        Err(e) => warn!("leave a tombstone for sandbox {id}: {e}"),
    }
}

/// One request on a connection of its own.
async fn call(socket: &Path, op: Op) -> std::io::Result<Outcome> {
    let mut stream = UnixStream::connect(socket).await?;
    let request = ToNode::Call {
        version: VERSION,
        id: 1,
        op,
    };
    write_frame(&mut stream, &request).await?;
    match read_frame::<_, FromNode>(&mut stream).await? {
        Some(FromNode::Response { answer, .. }) => Ok(answer.into()),
        other => Err(std::io::Error::other(format!(
            "the node service answered {other:?}"
        ))),
    }
}

/// The front shim: containerd's `start` and `delete` actions, and the daemon.
pub fn run_shim() {
    crate::logging::init(Some(Path::new(LOG)));
    crate::panics::log_panics();
    // The front shim starts no processes but its daemon, so it reaps none,
    // and it logs to its own file.
    let config = Config {
        no_reaper: true,
        no_sub_reaper: true,
        no_setup_logger: true,
        ..Default::default()
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("{RUNTIME_ID}: tokio runtime: {e}");
            std::process::exit(1);
        }
    };
    runtime.block_on(run::<Service>(RUNTIME_ID, Some(config)));
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::node::{MockBackends, Node};

    /// With no node service, the `delete` action of a sandbox leaves a
    /// tombstone within containerd's limit, and the node service deletes the
    /// sandbox when it starts.
    #[tokio::test(flavor = "current_thread")]
    async fn a_sandbox_deleted_while_the_node_service_is_down_is_deleted_later() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("node.sock");
        let state = dir.path().join("state");
        let graves = tombstone::dir(&state);
        let netns = Some("/var/run/netns/cni-1".to_string());

        let started = std::time::Instant::now();
        drop_sandbox(&socket, "sb", netns.clone(), &graves).await;
        assert!(started.elapsed() < Duration::from_secs(5));
        let left = tombstone::list(&graves).0;
        let expected = Tombstone {
            pod: "sb".into(),
            netns,
        };
        assert_eq!(left, [expected]);

        let backends = Arc::new(MockBackends::default());
        let reaped = backends.reaped();
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let node = Node::open(backends, state, Duration::from_secs(300))
            .await
            .unwrap();
        tokio::spawn(async move { node.serve(listener).await });
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !tombstone::list(&graves).0.is_empty() {
            assert!(std::time::Instant::now() < deadline, "the tombstone stayed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(*reaped.lock().unwrap(), ["sb"]);

        // With the node service up, the action leaves nothing once the node
        // service deleted the sandbox.
        drop_sandbox(&socket, "sb2", None, &graves).await;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while reaped.lock().unwrap().len() < 2 || !tombstone::list(&graves).0.is_empty() {
            assert!(std::time::Instant::now() < deadline, "sb2 was not reaped");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(*reaped.lock().unwrap(), ["sb", "sb2"]);
    }
}
