//! The engine backend: one smolvm microVM for each pod sandbox.
//!
//! It boots the sandbox VM with smolvm's embeddable engine API
//! (`smolvm::embedded`), and runs the pod's containers in it with the guest
//! agent's Pod* requests (see `smolvm_protocol::AgentRequest`) over vsock:
//!
//! - `create_sandbox` creates and starts a persistent machine named after the
//!   sandbox id, with one host mount, the pod share. On an arm64 node the VM
//!   also gets the Rosetta share, unless the pod turns it off or the node has
//!   no working Rosetta (see [`crate::rosetta`]). virtiofs shares are fixed at
//!   boot and containers come later, so each container is copied into the pod
//!   share when it is created.
//! - `create_container` copies the container's rootfs and bind mounts into
//!   the pod share and sends `PodCreate`, and `create_exec` sends `PodExec`,
//!   each on a short-lived agent connection.
//! - `start` gives each process an agent connection of its own: `PodStart`
//!   streams `Started`, the process's `Stdout` and `Stderr`, then `Exited`,
//!   which a pump thread copies to containerd's fifos (see
//!   [`crate::pump::run_pump`]). The thread ends while the pod sleeps.
//! - `kill`, `pids` and `delete` send `PodSignal`, `PodPids` and `PodDelete`.
//!
//! The node service's lifecycle (see [`crate::lifecycle`]) owns the machine
//! itself: it pauses, resumes, stops and deletes it through
//! [`crate::node::vm::SmolvmOps`], and watches its VM process. A backend asks
//! nothing of a VM that sleeps: the node service wakes the pod first.
//!
//! smolvm's engine API is synchronous, so every smolvm and agent call runs in
//! `tokio::task::spawn_blocking`.
//!
//! Derived from smolvm's crates/smolvm-shim (Apache-2.0), with changes.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use log::{debug, info, warn};
use tokio::sync::watch;

use smolvm::agent::{AgentClient, HostMount, VmResources};
use smolvm::embedded::{EmbeddedRuntime, MachineSpec};
use smolvm::network::NetworkBackend;
use smolvm_protocol::{AgentRequest, AgentResponse, ROSETTA_GUEST_PATH};

use crate::backend::{
    key, now_ns, Adopted, AgentStats, ExitInfo, ExitWatch, PodBackend, ProcessSpec, Sandboxed,
    StatsSample, Stdio,
};
use crate::bundle;
use crate::probe::OutputCapture;
use crate::proto::valid_id;
use crate::pump::{pump_channel, run_pump, Begin, Inbox, PumpCmd, PumpHandle, PumpSpec};

/// Host root for pod shares: a dedicated, non-protected location (the OCI
/// bundle lives under /run/containerd, which smolvm's mount validation
/// rejects). One `<id>/podshare` subdir per sandbox. It also holds the node
/// service's state and the TSO canary's and the disk templates' files, as dot
/// files, which the node's sweep of pod shares at boot keeps.
pub(crate) const POD_SHARE_HOST_ROOT: &str = "/var/lib/containerd-shim-smolvm-pods";

/// Reap the sandbox's host share after its VM has been deleted. The sandbox id
/// must be one path component: containerd supplies it, but it must never
/// escape [`POD_SHARE_HOST_ROOT`], which also holds the node service's state.
fn remove_sandbox_share_at(root: &Path, id: &str) -> std::io::Result<()> {
    if !valid_id(id) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid sandbox id for share cleanup",
        ));
    }
    match crate::mounts::remove_tree(&root.join(id)) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

fn remove_sandbox_share(id: &str) -> std::io::Result<()> {
    remove_sandbox_share_at(Path::new(POD_SHARE_HOST_ROOT), id)
}

/// Deletes the VM of sandbox `id`, then its pod share.
pub(crate) fn delete_sandbox_vm(rt: &EmbeddedRuntime, id: &str) -> smolvm::Result<()> {
    rt.delete_machine(id)?;
    if let Err(e) = remove_sandbox_share(id) {
        warn!("remove the share of sandbox {id}: {e}");
    }
    Ok(())
}

/// Nominal guest target recorded on the pod share's [`HostMount`]. The pods
/// do not use it: [`mount_pod_share_in_guest`] mounts the share at
/// [`POD_SHARE_GUEST_MOUNT`].
const POD_SHARE_GUEST_PATH: &str = "/podshare";

/// Where the agent's pod handlers resolve `PodCreate.rootfs_rel` from:
/// `paths::VIRTIOFS_MOUNT_ROOT` + `pod::POD_SHARE_TAG` in smolvm-agent. Must
/// stay in lockstep with those constants.
const POD_SHARE_GUEST_MOUNT: &str = "/run/smolvm/virtiofs/podshare";

/// vCPUs of every sandbox VM.
const SANDBOX_CPUS: u8 = 2;

/// Memory of every sandbox VM, in MiB, which admission also counts for each
/// pod.
pub(crate) const SANDBOX_MEMORY_MIB: u32 = 1024;

/// Pod annotation opting the sandbox VM into a virtio-gpu device (Venus/Vulkan).
///
/// The GPU is decided at sandbox creation: libkrun takes it with
/// `krun_set_gpu_options2` before the VM starts and has no hot-plug, and
/// kubelet reports a container's devices only once the sandbox runs. So the
/// request is a pod annotation, which containerd copies onto the sandbox's OCI
/// spec.
const ANN_GPU: &str = "smolvm.io/gpu";
/// Pod annotation sizing the GPU's shared-memory region, in MiB. If it is
/// absent, smolvm's default size applies. Ignored unless [`ANN_GPU`] is set.
const ANN_GPU_VRAM_MIB: &str = "smolvm.io/gpu-vram-mib";

/// A sandbox's GPU request, read off the pod's annotations.
#[derive(Debug, Default, PartialEq, Eq)]
struct GpuRequest {
    enabled: bool,
    vram_mib: Option<u32>,
}

/// Read the GPU request from a sandbox OCI spec.
///
/// Unparseable or out-of-range sizes fall back to smolvm's default rather
/// than failing the pod: the workload asked for a GPU, and a bad size hint is
/// not a reason to deny it one.
fn sandbox_gpu_request(spec: Option<&serde_json::Value>) -> GpuRequest {
    let Some(annotations) = spec.and_then(|v| v.get("annotations")) else {
        return GpuRequest::default();
    };
    // Only an explicit yes enables it: any other value, a typo too, leaves the
    // pod without a GPU.
    let flag = annotations.get(ANN_GPU).and_then(|v| v.as_str());
    if flag.and_then(bundle::parse_flag) != Some(true) {
        return GpuRequest::default();
    }
    let vram_mib = annotations
        .get(ANN_GPU_VRAM_MIB)
        .and_then(|v| v.as_str())
        .and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|mib| *mib > 0);
    if annotations.get(ANN_GPU_VRAM_MIB).is_some() && vram_mib.is_none() {
        warn!("{ANN_GPU_VRAM_MIB} is not a positive integer; using the default VRAM size");
    }
    GpuRequest {
        enabled: true,
        vram_mib,
    }
}

/// How long `start` waits for the agent's `Started` frame.
const START_TIMEOUT: Duration = Duration::from_secs(60);

/// How long a pause waits for each pump to close its agent connection: a
/// pump inside an attach handshake answers when the agent does, within 30 s.
const PUMP_PAUSE_TIMEOUT: Duration = Duration::from_secs(35);

/// Synthetic guest-process pids reported to containerd. The agent does not
/// report real guest pids on `Started`, and containerd only needs a stable,
/// non-zero identifier per process.
static NEXT_PID: AtomicU32 = AtomicU32::new(1000);

fn next_pid() -> u32 {
    NEXT_PID.fetch_add(1, Ordering::Relaxed)
}

/// Hand out no pid up to `above`: those of the tasks containerd has from a
/// node service before this one.
fn reserve_pids(above: u32) {
    NEXT_PID.fetch_max(above.saturating_add(1), Ordering::Relaxed);
}

/// The pumps' cursor files, on tmpfs, one directory for each sandbox.
pub(crate) const CURSOR_ROOT: &str = "/run/smolvm-pods/cursors";

/// The cursor file of the pump of process `id`/`exec_id` in sandbox `sandbox`.
fn cursor_path(sandbox: &str, id: &str, exec_id: Option<&str>) -> Option<PathBuf> {
    if !valid_id(sandbox) || !valid_id(id) || !exec_id.is_none_or(valid_id) {
        return None;
    }
    let name = match exec_id {
        Some(exec_id) => format!("{id}@{exec_id}"),
        None => id.to_string(),
    };
    Some(Path::new(CURSOR_ROOT).join(sandbox).join(name))
}

fn spec_hostname(spec: Option<&serde_json::Value>) -> Option<String> {
    spec.and_then(|v| {
        v.get("hostname")
            .and_then(|h| h.as_str())
            .filter(|s| !s.is_empty())
            .map(String::from)
    })
}

fn spec_sysctls(spec: Option<&serde_json::Value>) -> Option<serde_json::Value> {
    spec.and_then(|v| v.pointer("/linux/sysctl"))
        .filter(|v| v.as_object().map(|o| !o.is_empty()).unwrap_or(false))
        .cloned()
}

struct Sandbox {
    /// Sandbox task id == machine name.
    id: String,
    /// The pod share: `<POD_SHARE_HOST_ROOT>/<id>/podshare`, shared into the
    /// guest.
    share_dir: PathBuf,
    /// Host-side unix socket bridging to the guest agent's vsock port.
    socket: PathBuf,
    /// Pod UTS hostname from the sandbox OCI spec. Injected into each container's
    /// spec (containerd puts the pod hostname on the sandbox, not the container).
    hostname: Option<String>,
    /// Pod sysctls (`linux.sysctl` on the sandbox OCI spec). The IPC and UTS
    /// ones go into every container's spec ([`materialize_bind_mounts`]). The
    /// `net.*` ones are applied guest-wide at sandbox creation
    /// ([`apply_guest_net_sysctls`]).
    sysctls: Option<serde_json::Value>,
    /// virtiofs tag of the VM's Rosetta share, mounted into every container.
    rosetta_tag: Option<String>,
    /// A standalone pod, with no sandbox task: the process with the pod's
    /// id is its container.
    standalone: bool,
}

impl Sandbox {
    /// What a pump of process `id`/`exec_id` in this sandbox needs.
    fn pump_spec(
        &self,
        id: &str,
        exec_id: Option<&str>,
        stdio: Stdio,
        exit_tx: Arc<watch::Sender<Option<ExitInfo>>>,
        begin: Begin,
    ) -> PumpSpec {
        PumpSpec {
            socket: self.socket.clone(),
            id: id.to_string(),
            exec_id: exec_id.map(str::to_string),
            stdio,
            exit_tx,
            begin,
            cursor_file: cursor_path(&self.id, id, exec_id),
        }
    }
}

/// Records exit status 255, which means the real status is unknown, unless the
/// process's exit is already known.
pub(crate) fn publish_unknown_exit(exit_tx: &watch::Sender<Option<ExitInfo>>) {
    exit_tx.send_if_modified(|exit| {
        let unknown = exit.is_none();
        if unknown {
            *exit = Some(ExitInfo {
                status: 255,
                exited_at_ns: now_ns(),
                oom: false,
            });
        }
        unknown
    });
}

/// The guest agent's pod sessions take no input: input on a session ends it,
/// and it refuses a terminal. So a process that would need either is refused
/// before it starts, instead of losing its input.
fn refuse_input(stdio: &Stdio) -> Result<(), String> {
    if stdio.terminal {
        return Err("smolvm-pods does not give VM pod processes a terminal \
                    (tty: true, kubectl exec -t)"
            .into());
    }
    if !stdio.stdin.is_empty() {
        return Err("smolvm-pods does not pass stdin to VM pod processes \
                    (stdin: true, kubectl exec -i, kubectl attach, kubectl cp into a pod)"
            .into());
    }
    Ok(())
}

struct ProcEntry {
    stdio: Stdio,
    pid: u32,
    exit_tx: Arc<watch::Sender<Option<ExitInfo>>>,
    exit_rx: ExitWatch,
    /// The process was started in the guest.
    started: bool,
    /// Present while the process has a pump, on its thread or parked.
    pump: Option<PumpHandle>,
    /// Keeps the first output of an exec of a probe's command.
    capture: Option<OutputCapture>,
}

impl ProcEntry {
    fn new(stdio: Stdio, pid: u32) -> Self {
        let (tx, rx) = watch::channel(None);
        Self {
            stdio,
            pid,
            exit_tx: Arc::new(tx),
            exit_rx: rx,
            started: false,
            pump: None,
            capture: None,
        }
    }

    fn exited(&self) -> bool {
        self.exit_rx.borrow().is_some()
    }
}

pub struct EnginePodBackend {
    sandbox: Mutex<Option<Arc<Sandbox>>>,
    procs: Arc<Mutex<HashMap<String, ProcEntry>>>,
}

impl EnginePodBackend {
    pub fn new() -> Self {
        Self {
            sandbox: Mutex::new(None),
            procs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn sandbox(&self) -> Result<Arc<Sandbox>, String> {
        self.sandbox
            .lock()
            .map_err(|e| e.to_string())?
            .clone()
            .ok_or_else(|| "no sandbox created for this pod".to_string())
    }

    fn is_sandbox_task(&self, id: &str, exec_id: Option<&str>) -> bool {
        exec_id.is_none()
            && self
                .sandbox
                .lock()
                .ok()
                .and_then(|s| s.as_ref().map(|s| !s.standalone && s.id == id))
                .unwrap_or(false)
    }

    /// Calls `with` on the pump of each running process. A process with no
    /// pump, or whose pump `with` finds ended (it returns false), gets a new
    /// pump that begins as `begin`.
    fn each_pump(&self, begin: Begin, with: impl Fn(&PumpHandle) -> bool) {
        let Ok(sandbox) = self.sandbox() else {
            return;
        };
        let Ok(mut procs) = self.procs.lock() else {
            return;
        };
        for (k, p) in procs.iter_mut() {
            if !p.started || p.exited() || p.pump.as_ref().is_some_and(&with) {
                continue;
            }
            // No pump runs for it, as after a restart of the node service.
            let (id, exec_id) = match k.split_once('/') {
                Some((id, exec_id)) => (id.to_string(), Some(exec_id.to_string())),
                None => (k.clone(), None),
            };
            let (handle, inbox) = pump_channel();
            let (started_tx, _) = std::sync::mpsc::sync_channel(1);
            let pump = sandbox.pump_spec(
                &id,
                exec_id.as_deref(),
                p.stdio.clone(),
                p.exit_tx.clone(),
                begin,
            );
            match spawn_pump(pump, inbox, started_tx) {
                Ok(()) => p.pump = Some(handle),
                Err(e) => warn!("pod sandbox {}: a pump for {k}: {e}", sandbox.id),
            }
        }
    }

    fn insert_proc(&self, k: String, entry: ProcEntry) -> Result<(), String> {
        self.procs
            .lock()
            .map_err(|e| e.to_string())?
            .insert(k, entry);
        Ok(())
    }
}

// ========================== blocking agent helpers ==========================

fn connect(socket: &Path) -> Result<AgentClient, String> {
    AgentClient::connect_with_retry(socket).map_err(|e| format!("connect to sandbox agent: {e}"))
}

/// One-shot request/response on a fresh agent connection. All Pod* control
/// requests (create/exec/signal/pids/delete) are single-frame exchanges, so a
/// short-lived connection per request keeps them independent of the per-process
/// streaming connections.
pub(crate) fn pod_request(socket: &Path, req: &AgentRequest) -> Result<AgentResponse, String> {
    let mut client = connect(socket)?;
    client
        .send_raw(req)
        .map_err(|e| format!("send {}: {e}", req.log_summary()))?;
    // Skip Progress-style frames; return the first terminal response.
    loop {
        match client
            .recv_raw()
            .map_err(|e| format!("recv {}: {e}", req.log_summary()))?
        {
            AgentResponse::Progress { .. } => continue,
            resp => return Ok(resp),
        }
    }
}

pub(crate) fn expect_ok(resp: AgentResponse, op: &str) -> Result<(), String> {
    match resp {
        AgentResponse::Ok { .. } => Ok(()),
        AgentResponse::Error { message, .. } => Err(format!("{op}: {message}")),
        other => Err(format!("{op}: unexpected agent response {other:?}")),
    }
}

/// Run `script` with the guest agent's shell, for at most 10 s.
fn vm_exec(socket: &Path, script: String) -> Result<AgentResponse, String> {
    pod_request(
        socket,
        &AgentRequest::VmExec {
            command: vec!["/bin/sh".into(), "-c".into(), script],
            env: Vec::new(),
            workdir: None,
            timeout_ms: Some(10_000),
            interactive: false,
            tty: false,
            background: false,
            stdin_data: None,
        },
    )
}

/// Apply the pod's `net.*` sysctls to the guest kernel. Pod containers declare
/// no network namespace of their own, so they share the guest's; writing these
/// under the guest's `/proc/sys/net` is what every container in the pod observes.
/// Best-effort: a sysctl the guest kernel doesn't expose is skipped, never fatal
/// to sandbox creation.
fn apply_guest_net_sysctls(
    socket: &Path,
    sysctls: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    let mut cmds = Vec::new();
    for (k, v) in sysctls {
        if !k.starts_with("net.") {
            continue;
        }
        let Some(val) = v.as_str() else { continue };
        // Skip values with a single quote, which would end the quoted value.
        // Sysctl values are numbers or space/tab-separated number ranges.
        if val.contains('\'') {
            continue;
        }
        let path = format!("/proc/sys/{}", k.replace('.', "/"));
        cmds.push(format!("printf '%s' '{val}' > {path} 2>/dev/null || true"));
    }
    if cmds.is_empty() {
        return Ok(());
    }
    match vm_exec(socket, cmds.join("; "))? {
        AgentResponse::Completed { .. } => Ok(()),
        other => Err(format!(
            "apply net sysctls: unexpected agent response {other:?}"
        )),
    }
}

/// Mount the pod share where the agent's pod handlers expect it,
/// [`POD_SHARE_GUEST_MOUNT`].
///
/// The launcher tags virtiofs devices `smolvm{index}` (`HostMount::mount_tag`),
/// and the pod share is the first mount, so its tag is `smolvm0`. The agent
/// mounts each device only at its own guest path, not where its pod handlers
/// look, so the backend mounts the device itself right after boot.
fn mount_pod_share_in_guest(socket: &Path) -> Result<(), String> {
    let share_tag = HostMount::mount_tag(0);
    let script = format!(
        "mkdir -p {POD_SHARE_GUEST_MOUNT} && \
         (mountpoint -q {POD_SHARE_GUEST_MOUNT} || \
          mount -t virtiofs {share_tag} {POD_SHARE_GUEST_MOUNT})"
    );
    run_in_guest(socket, "mount pod share in guest", script)
}

/// Run `script` with the guest agent's shell and fail unless it exits 0.
fn run_in_guest(socket: &Path, op: &str, script: String) -> Result<(), String> {
    match vm_exec(socket, script)? {
        AgentResponse::Completed { exit_code: 0, .. } => Ok(()),
        AgentResponse::Completed {
            exit_code, stderr, ..
        } => Err(format!(
            "{op} failed (exit {exit_code}): {}",
            String::from_utf8_lossy(&stderr).trim()
        )),
        AgentResponse::Error { message, .. } => Err(format!("{op}: {message}")),
        other => Err(format!("{op}: unexpected agent response {other:?}")),
    }
}

/// Prints `PID THREADS FDS` of the guest agent, a child of libkrun's init
/// that runs from a memfd, so that the kernel names it `memfd:agent`. A
/// process that the agent forks has the same name until it execs, so the
/// agent is the one whose parent is pid 1. Only shell builtins, so that the
/// script spawns no process.
const AGENT_STATS: &str = r#"pid=
for p in /proc/[0-9]*; do
  read -r comm 2>/dev/null <"$p/comm" || continue
  case $comm in
    memfd:agent | smolvm-agent) ;;
    *) continue ;;
  esac
  read -r stat 2>/dev/null <"$p/stat" || continue
  set -- $stat
  if [ "$4" = 1 ]; then pid=${p#/proc/}; break; fi
done
[ -n "$pid" ] || { echo "no smolvm-agent process" >&2; exit 1; }
threads=
while read -r key value _; do
  if [ "$key" = Threads: ]; then threads=$value; fi
done <"/proc/$pid/status"
set -- "/proc/$pid/fd"/*
echo "$pid $threads $#""#;

/// The guest agent's process, counted in the guest.
fn agent_stats(socket: &Path) -> Result<AgentStats, String> {
    let stdout = match vm_exec(socket, AGENT_STATS.into())? {
        AgentResponse::Completed {
            exit_code: 0,
            stdout,
            ..
        } => stdout,
        AgentResponse::Completed {
            exit_code, stderr, ..
        } => {
            return Err(format!(
                "count the agent's threads (exit {exit_code}): {}",
                String::from_utf8_lossy(&stderr).trim()
            ))
        }
        other => return Err(format!("count the agent's threads: {other:?}")),
    };
    let text = String::from_utf8_lossy(&stdout);
    let numbers: Vec<u32> = text
        .split_whitespace()
        .filter_map(|n| n.parse().ok())
        .collect();
    match numbers[..] {
        [pid, threads, fds] => Ok(AgentStats { pid, threads, fds }),
        _ => Err(format!("count the agent's threads: {text:?}")),
    }
}

/// Start a pump on a thread of its own: it owns the process's streaming
/// connection while the pod is awake.
fn spawn_pump(
    pump: PumpSpec,
    inbox: Inbox,
    started_tx: std::sync::mpsc::SyncSender<Result<(), String>>,
) -> Result<(), String> {
    std::thread::Builder::new()
        .name(format!("pod-io-{}", key(&pump.id, pump.exec_id.as_deref())))
        .spawn(move || run_pump(pump, inbox, started_tx))
        .map(|_| ())
        .map_err(|e| format!("spawn pump thread: {e}"))
}

// ============================== trait impl ==================================

#[async_trait]
impl PodBackend for EnginePodBackend {
    async fn create_sandbox(
        &self,
        id: &str,
        bundle: &str,
        netns: Option<&str>,
        standalone: bool,
    ) -> Result<Sandboxed, String> {
        let id_owned = id.to_string();
        let bundle = bundle.to_string();
        let netns = netns.map(str::to_string);

        let (sandbox, warning) = tokio::task::spawn_blocking(move || {
            let sandbox_spec = bundle::read_spec(&bundle);
            let hostname = spec_hostname(sandbox_spec.as_ref());
            let sysctls = spec_sysctls(sandbox_spec.as_ref());
            let gpu = sandbox_gpu_request(sandbox_spec.as_ref());
            if gpu.enabled {
                info!(
                    "sandbox {id_owned}: GPU requested (vram_mib={})",
                    gpu.vram_mib
                        .map(|m| m.to_string())
                        .unwrap_or_else(|| "default".into())
                );
            }
            // A pod sandbox without a network namespace of its own is a
            // hostNetwork pod, which a VM cannot join. A standalone pod
            // without one gets a VM without networking, so that nothing
            // leaves the node outside the pod network.
            if !standalone && netns.is_none() {
                let refusal = "the pod has no network namespace of its own (hostNetwork), \
                               and smolvm-pods does not run such pods";
                return Err(refusal.to_string());
            }
            // Rosetta and TSO by default on an arm64 node. The decision's
            // warning, why a pod runs without Rosetta, is for the pod's event.
            let rosetta_decision = crate::rosetta::decide(&id_owned, sandbox_spec.as_ref())?;
            let rosetta = rosetta_decision.enabled;

            let share_dir = Path::new(POD_SHARE_HOST_ROOT)
                .join(&id_owned)
                .join("podshare");
            std::fs::create_dir_all(&share_dir)
                .map_err(|e| format!("mkdir {}: {e}", share_dir.display()))?;
            for template in crate::templates::expand()? {
                warn!("no disk template {template}, so smolvm formats each new VM disk");
            }

            let rt = smolvm::embedded::runtime().map_err(|e| e.to_string())?;
            let mount = HostMount::new(&share_dir, POD_SHARE_GUEST_PATH, false)
                .map_err(|e| format!("pod share mount: {e}"))?;
            let mut mounts = vec![mount];
            let rosetta_tag = if rosetta {
                mounts.push(crate::rosetta::share()?);
                Some(HostMount::mount_tag(mounts.len() - 1))
            } else {
                None
            };
            let spec = MachineSpec {
                name: id_owned.clone(),
                mounts,
                ports: Vec::new(),
                // A pod sandbox is a bare VM; the pod's containers bring their
                // own images through the CRI, not through the machine record.
                image: None,
                resources: VmResources {
                    cpus: SANDBOX_CPUS,
                    memory_mib: SANDBOX_MEMORY_MIB,
                    network: netns.is_some(),
                    // virtio-net gives the sandbox a real L2 NIC, which the pod
                    // network bridges into the pod's network namespace, so that
                    // the pod has its CNI-assigned IP.
                    network_backend: netns.is_some().then_some(NetworkBackend::VirtioNet),
                    gpu: gpu.enabled,
                    gpu_vram_mib: gpu.vram_mib,
                    ..VmResources::default()
                },
                persistent: true,
                runtime_managed: true,
                // smolvm's defaults for the rest, such as no remote volumes.
                ..Default::default()
            };
            match rt.create_machine(spec.clone()) {
                Ok(()) => {}
                // A stale record with this sandbox id, as a crash or an
                // unclean node shutdown leaves, cannot be reused: replace it.
                Err(smolvm::Error::Agent {
                    kind: smolvm::error::AgentErrorKind::Conflict,
                    ..
                }) => {
                    warn!("sandbox machine {id_owned} already exists; recreating");
                    let _ = rt.delete_machine(&id_owned);
                    rt.create_machine(spec).map_err(|e| e.to_string())?;
                }
                Err(e) => return Err(e.to_string()),
            }
            match netns.as_deref() {
                Some(ns) => rt
                    .start_machine_with_netns(&id_owned, std::path::PathBuf::from(ns))
                    .map_err(|e| format!("start sandbox VM (netns {ns}): {e}"))?,
                None => rt
                    .start_machine(&id_owned)
                    .map_err(|e| format!("start sandbox VM: {e}"))?,
            }

            let vm_pid = rt.pid(&id_owned);
            let vm_dir = smolvm::agent::vm_data_dir(&id_owned);
            // Only this binary's VM process entry point turns TSO on.
            if rosetta && !vm_pid.is_some_and(|p| crate::boot::has_tso(&vm_dir, p)) {
                if let Err(e) = rt.stop_machine(&id_owned) {
                    warn!("stop sandbox VM {id_owned}: {e}");
                }
                return Err(format!(
                    "the VM process of sandbox {id_owned} did not turn TSO on for Rosetta"
                ));
            }
            let socket = vm_dir.join("agent.sock");
            mount_pod_share_in_guest(&socket)?;
            if let Some(ns) = netns.as_deref() {
                run_in_guest(
                    &socket,
                    "set up the pod network in guest",
                    crate::net::guest_setup_script(ns)?,
                )?;
            }
            if rosetta_tag.is_some() {
                run_in_guest(
                    &socket,
                    "set up Rosetta in guest",
                    crate::rosetta::guest_setup_script(),
                )?;
            }
            if let Some(serde_json::Value::Object(map)) = sysctls.as_ref() {
                apply_guest_net_sysctls(&socket, map)?;
            }
            let sandbox = Sandbox {
                id: id_owned,
                share_dir,
                socket,
                hostname,
                sysctls,
                rosetta_tag,
                standalone,
            };
            Ok::<_, String>((sandbox, rosetta_decision.warning))
        })
        .await
        .map_err(|e| e.to_string())??;

        let rosetta = sandbox.rosetta_tag.is_some();
        *self.sandbox.lock().map_err(|e| e.to_string())? = Some(Arc::new(sandbox));
        let pid = next_pid();
        if !standalone {
            self.insert_proc(key(id, None), ProcEntry::new(Stdio::default(), pid))?;
        }
        Ok(Sandboxed {
            pid,
            rosetta,
            warning,
        })
    }

    async fn adopt(
        &self,
        id: &str,
        bundle: &str,
        _netns: Option<&str>,
        standalone: bool,
        procs: Vec<Adopted>,
    ) -> Result<(), String> {
        if self.sandbox.lock().map_err(|e| e.to_string())?.is_some() {
            return Ok(());
        }
        let id_owned = id.to_string();
        let bundle = bundle.to_string();
        let sandbox = tokio::task::spawn_blocking(move || {
            let spec = bundle::read_spec(&bundle);
            // smolvm's record only: the VM may sleep, or be gone.
            let record = smolvm::embedded::runtime()
                .and_then(|rt| rt.machine_record(&id_owned))
                .map_err(|e| e.to_string())?;
            let rosetta_tag = record.and_then(|record| {
                record
                    .mounts
                    .iter()
                    .position(|(_, guest, _)| guest == ROSETTA_GUEST_PATH)
                    .map(HostMount::mount_tag)
            });
            Ok::<_, String>(Sandbox {
                share_dir: Path::new(POD_SHARE_HOST_ROOT)
                    .join(&id_owned)
                    .join("podshare"),
                socket: smolvm::agent::vm_data_dir(&id_owned).join("agent.sock"),
                hostname: spec_hostname(spec.as_ref()),
                sysctls: spec_sysctls(spec.as_ref()),
                rosetta_tag,
                standalone,
                id: id_owned,
            })
        })
        .await
        .map_err(|e| e.to_string())??;
        for p in procs {
            let mut entry = ProcEntry::new(p.stdio.clone(), p.pid);
            if let Some(exit) = p.exit {
                entry.exit_tx.send_replace(Some(exit));
            }
            let is_sandbox = !standalone && p.id == sandbox.id && p.exec_id.is_none();
            entry.started = p.running && !is_sandbox;
            self.insert_proc(key(&p.id, p.exec_id.as_deref()), entry)?;
        }
        info!("adopted sandbox {id}");
        *self.sandbox.lock().map_err(|e| e.to_string())? = Some(Arc::new(sandbox));
        Ok(())
    }

    async fn create_container(&self, id: &str, spec: ProcessSpec) -> Result<u32, String> {
        refuse_input(&spec.stdio)?;
        let sandbox = self.sandbox()?;
        let id_owned = id.to_string();
        let stdio = spec.stdio.clone();
        let pid = next_pid();

        tokio::task::spawn_blocking(move || {
            // Nothing is copied for a VM whose agent does not answer.
            connect(&sandbox.socket)?
                .ping()
                .map_err(|e| format!("ping sandbox agent: {e}"))?;
            let config = Path::new(&spec.bundle).join("config.json");
            let spec_json = std::fs::read_to_string(&config)
                .map_err(|e| format!("read {}: {e}", config.display()))?;
            if sandbox.rosetta_tag.is_none() && cfg!(target_arch = "aarch64") {
                crate::rosetta::refuse_amd64_entrypoint(Path::new(&spec.rootfs), &spec_json)?;
            }
            // A failed create removes what it copied: kubelet retries with a
            // new container id, and each try copies the whole image.
            let created = copy_and_create(&sandbox, &id_owned, &spec, spec_json);
            if created.is_err() {
                let dir = sandbox.share_dir.join(&id_owned);
                if let Err(e) = crate::mounts::remove_tree(&dir) {
                    debug!("rm {}: {e}", dir.display());
                }
            }
            created
        })
        .await
        .map_err(|e| e.to_string())??;

        self.insert_proc(key(id, None), ProcEntry::new(stdio, pid))?;
        Ok(pid)
    }

    async fn create_exec(&self, id: &str, exec_id: &str, spec: ProcessSpec) -> Result<(), String> {
        refuse_input(&spec.stdio)?;
        let sandbox = self.sandbox()?;
        let id_owned = id.to_string();
        let exec_owned = exec_id.to_string();
        let stdio = spec.stdio.clone();

        // containerd's ExecProcessRequest.spec is an Any whose value is the
        // OCI Process serialized as JSON; pass it through verbatim.
        let process_json = String::from_utf8(spec.exec_spec.unwrap_or_default())
            .map_err(|e| format!("exec spec is not UTF-8 JSON: {e}"))?;
        let process_json = match crate::network::trust() {
            Some(trust) => {
                let root = sandbox.share_dir.join(id).join("rootfs");
                crate::trust::exec_process(&root, &process_json, trust.mode)
            }
            None => process_json,
        };

        tokio::task::spawn_blocking(move || {
            let resp = pod_request(
                &sandbox.socket,
                &AgentRequest::PodExec {
                    id: id_owned,
                    exec_id: exec_owned,
                    process_json,
                    tty: false,
                },
            )?;
            expect_ok(resp, "PodExec")
        })
        .await
        .map_err(|e| e.to_string())??;

        let mut entry = ProcEntry::new(stdio, next_pid());
        entry.capture = spec.capture;
        self.insert_proc(key(id, Some(exec_id)), entry)?;
        Ok(())
    }

    async fn start(&self, id: &str, exec_id: Option<&str>) -> Result<u32, String> {
        let k = key(id, exec_id);
        let (stdio, pid, exit_tx, capture) = {
            let map = self.procs.lock().map_err(|e| e.to_string())?;
            let p = map.get(&k).ok_or_else(|| format!("no such process {k}"))?;
            (p.stdio.clone(), p.pid, p.exit_tx.clone(), p.capture.clone())
        };

        // The sandbox task is nominal. The VM already runs; there is no guest
        // process to start.
        if self.is_sandbox_task(id, exec_id) {
            return Ok(pid);
        }

        let sandbox = self.sandbox()?;
        let (handle, inbox) = pump_channel();
        if let Some(capture) = capture {
            handle.send(PumpCmd::Capture(capture));
        }
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let pump = sandbox.pump_spec(id, exec_id, stdio, exit_tx, Begin::Start);
        spawn_pump(pump, inbox, started_tx)?;

        // Block (off the async runtime) until the agent confirms Started.
        tokio::task::spawn_blocking(move || match started_rx.recv_timeout(START_TIMEOUT) {
            Ok(res) => res,
            Err(e) => Err(format!("timed out waiting for Started: {e}")),
        })
        .await
        .map_err(|e| e.to_string())??;

        if let Some(p) = self.procs.lock().map_err(|e| e.to_string())?.get_mut(&k) {
            p.started = true;
            p.pump = Some(handle);
        }
        Ok(pid)
    }

    async fn kill(
        &self,
        id: &str,
        exec_id: Option<&str>,
        signal: u32,
        all: bool,
    ) -> Result<(), String> {
        // Killing the sandbox task ends the nominal pause process: publish its
        // exit so Wait resolves. The node service deletes the VM itself.
        if self.is_sandbox_task(id, exec_id) {
            if signal == 9 || signal == 15 {
                let k = key(id, None);
                if let Some(p) = self.procs.lock().map_err(|e| e.to_string())?.get(&k) {
                    let _ = p.exit_tx.send(Some(ExitInfo {
                        status: 128 + signal,
                        exited_at_ns: now_ns(),
                        oom: false,
                    }));
                }
            }
            return Ok(());
        }

        let sandbox = self.sandbox()?;
        let req = AgentRequest::PodSignal {
            id: id.to_string(),
            exec_id: exec_id.map(str::to_string),
            signal,
            all,
        };
        tokio::task::spawn_blocking(move || {
            expect_ok(pod_request(&sandbox.socket, &req)?, "PodSignal")
        })
        .await
        .map_err(|e| e.to_string())?
    }

    async fn wait_channel(&self, id: &str, exec_id: Option<&str>) -> Result<ExitWatch, String> {
        let map = self.procs.lock().map_err(|e| e.to_string())?;
        map.get(&key(id, exec_id))
            .map(|p| p.exit_rx.clone())
            .ok_or_else(|| format!("no such process {}", key(id, exec_id)))
    }

    async fn resize_pty(
        &self,
        id: &str,
        exec_id: Option<&str>,
        w: u32,
        h: u32,
    ) -> Result<(), String> {
        let pump = {
            let map = self.procs.lock().map_err(|e| e.to_string())?;
            map.get(&key(id, exec_id)).and_then(|p| p.pump.clone())
        };
        // Not started (or already exited): resize is a no-op.
        if let Some(pump) = pump {
            pump.send(PumpCmd::Resize {
                cols: w as u16,
                rows: h as u16,
            });
        }
        Ok(())
    }

    /// No process has stdin to close: `refuse_input` refuses one that would.
    async fn close_io(&self, _id: &str, _exec_id: Option<&str>) -> Result<(), String> {
        Ok(())
    }

    async fn delete(&self, id: &str, exec_id: Option<&str>) -> Result<(), String> {
        let k = key(id, exec_id);

        // A standalone pod's container goes with its VM.
        let standalone =
            exec_id.is_none() && self.sandbox().is_ok_and(|s| s.standalone && s.id == id);
        if standalone || self.is_sandbox_task(id, exec_id) {
            // The lifecycle deleted the VM (`SmolvmOps::delete`).
            *self.sandbox.lock().map_err(|e| e.to_string())? = None;
            self.procs.lock().map_err(|e| e.to_string())?.clear();
            return Ok(());
        }

        let sandbox = self.sandbox()?;
        let id_owned = id.to_string();
        let exec_owned = exec_id.map(str::to_string);
        tokio::task::spawn_blocking(move || {
            // Best-effort guest-side cleanup: the VM may already be gone when
            // containerd deletes exited tasks during pod teardown, and a
            // failing Delete would wedge containerd's teardown loop.
            match pod_request(
                &sandbox.socket,
                &AgentRequest::PodDelete {
                    id: id_owned.clone(),
                    exec_id: exec_owned.clone(),
                },
            )
            .and_then(|r| expect_ok(r, "PodDelete"))
            {
                Ok(()) => {}
                Err(e) => warn!("PodDelete {id_owned}: {e}"),
            }

            if let Some(cursor) = cursor_path(&sandbox.id, &id_owned, exec_owned.as_deref()) {
                let _ = std::fs::remove_file(cursor);
            }
            // Container (not exec) delete removes the materialized rootfs copy
            // under the pod share (the container filesystem is ephemeral).
            if exec_owned.is_none() {
                let cdir = sandbox.share_dir.join(&id_owned);
                if let Err(e) = crate::mounts::remove_tree(&cdir) {
                    debug!("rm {}: {e}", cdir.display());
                }
            }
            Ok::<(), String>(())
        })
        .await
        .map_err(|e| e.to_string())??;

        self.procs.lock().map_err(|e| e.to_string())?.remove(&k);
        Ok(())
    }

    async fn pids(&self, id: &str) -> Result<Vec<u32>, String> {
        if self.is_sandbox_task(id, None) {
            let map = self.procs.lock().map_err(|e| e.to_string())?;
            let pid = map.get(&key(id, None)).map(|p| p.pid).unwrap_or(1);
            return Ok(vec![pid]);
        }
        let sandbox = self.sandbox()?;
        let req = AgentRequest::PodPids { id: id.to_string() };
        tokio::task::spawn_blocking(move || match pod_request(&sandbox.socket, &req)? {
            AgentResponse::Pids { pids } => Ok(pids),
            AgentResponse::Error { message, .. } => Err(format!("PodPids: {message}")),
            other => Err(format!("PodPids: unexpected agent response {other:?}")),
        })
        .await
        .map_err(|e| e.to_string())?
    }

    async fn set_exit(&self, id: &str, exec_id: Option<&str>, exit: ExitInfo) {
        let Ok(procs) = self.procs.lock() else {
            return;
        };
        if let Some(p) = procs.get(&key(id, exec_id)) {
            p.exit_tx.send_if_modified(|known| {
                let unknown = known.is_none();
                if unknown {
                    *known = Some(exit);
                }
                unknown
            });
        }
    }

    fn pause_pumps(&self) -> Result<(), String> {
        let acks: Vec<(String, std::sync::mpsc::Receiver<Result<(), String>>)> = {
            let procs = self.procs.lock().map_err(|e| e.to_string())?;
            procs
                .iter()
                .filter(|(_, p)| !p.exited())
                .filter_map(|(k, p)| {
                    let (ack, acked) = std::sync::mpsc::sync_channel(1);
                    // A pump that ended holds no connection.
                    let pump = p.pump.as_ref()?;
                    pump.send(PumpCmd::Pause { ack }).then_some(())?;
                    Some((k.clone(), acked))
                })
                .collect()
        };
        let mut failed = Vec::new();
        for (k, acked) in acks {
            match acked.recv_timeout(PUMP_PAUSE_TIMEOUT) {
                Ok(Ok(())) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {}
                Ok(Err(e)) => failed.push(format!("{k}: {e}")),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    failed.push(format!("{k}: no answer within {PUMP_PAUSE_TIMEOUT:?}"))
                }
            }
        }
        if failed.is_empty() {
            Ok(())
        } else {
            Err(failed.join("; "))
        }
    }

    fn resume_pumps(&self) {
        self.each_pump(Begin::Attach, |pump| pump.send(PumpCmd::Resume));
    }

    fn park_pumps(&self) {
        self.each_pump(Begin::Paused, |_| true);
    }

    async fn end_pump(&self, id: &str, exec_id: Option<&str>) {
        let k = key(id, exec_id);
        let pump = match self.procs.lock() {
            Ok(mut procs) => procs.get_mut(&k).and_then(|p| p.pump.take()),
            Err(_) => None,
        };
        let Some(pump) = pump else {
            return;
        };
        // A pump of a pod still pausing may hold its connection: it pauses
        // first. A paused pump whose handle is dropped ends.
        let paused = tokio::task::spawn_blocking(move || {
            let (ack, acked) = std::sync::mpsc::sync_channel(1);
            if pump.send(PumpCmd::Pause { ack }) {
                let _ = acked.recv_timeout(PUMP_PAUSE_TIMEOUT);
            }
        })
        .await;
        if let Err(e) = paused {
            warn!("end the pump of {k}: {e}");
        }
    }

    fn end_pumps(&self) {
        if let Ok(mut procs) = self.procs.lock() {
            for p in procs.values_mut() {
                p.pump = None;
            }
        }
    }

    async fn agent_stats(&self) -> Result<AgentStats, String> {
        let sandbox = self.sandbox()?;
        tokio::task::spawn_blocking(move || agent_stats(&sandbox.socket))
            .await
            .map_err(|e| e.to_string())?
    }

    async fn stats(&self, id: &str) -> Result<Option<StatsSample>, String> {
        // The pod sandbox "task" has no container process tree to sample.
        if self.is_sandbox_task(id, None) {
            return Ok(None);
        }
        let sandbox = self.sandbox()?;
        let req = AgentRequest::PodStats { id: id.to_string() };
        tokio::task::spawn_blocking(move || match pod_request(&sandbox.socket, &req)? {
            AgentResponse::Stats {
                cpu_usage_ns,
                memory_bytes,
            } => Ok(Some(StatsSample {
                cpu_usage_ns,
                memory_bytes,
            })),
            AgentResponse::Error { message, .. } => Err(format!("PodStats: {message}")),
            other => Err(format!("PodStats: unexpected agent response {other:?}")),
        })
        .await
        .map_err(|e| e.to_string())?
    }

    fn container_roots(&self) -> Vec<(String, PathBuf)> {
        let Ok(sandbox) = self.sandbox() else {
            return Vec::new();
        };
        let Ok(entries) = std::fs::read_dir(&sandbox.share_dir) else {
            return Vec::new();
        };
        let mut roots: Vec<(String, PathBuf)> = entries
            .flatten()
            .filter_map(|entry| {
                let root = entry.path().join("rootfs");
                root.is_dir()
                    .then(|| (entry.file_name().to_string_lossy().into_owned(), root))
            })
            .collect();
        roots.sort();
        roots
    }
}

/// Copy a container's rootfs and bind mounts into the pod share, then create
/// the container in the guest with `PodCreate`.
fn copy_and_create(
    sandbox: &Sandbox,
    id: &str,
    spec: &ProcessSpec,
    spec_json: String,
) -> Result<(), String> {
    // The rootfs is copied as plain files, which the guest sees through the
    // pod share: an overlayfs re-exported through virtiofs cannot serve the
    // guest's copy-up writes (crun's `mkdir /proc` fails with EACCES). The
    // copy lives as long as the container, as a container's filesystem does.
    // `cp -a` keeps modes, owners and symlinks, and a reflink makes the copy
    // nearly free when the share and the snapshot are on one filesystem.
    let target = sandbox.share_dir.join(id).join("rootfs");
    std::fs::create_dir_all(&target).map_err(|e| format!("mkdir {}: {e}", target.display()))?;
    let src_glob = format!("{}/.", spec.rootfs);
    let status = std::process::Command::new("cp")
        .args(["-a", "--reflink=auto", &src_glob])
        .arg(&target)
        .status()
        .map_err(|e| format!("spawn cp for rootfs: {e}"))?;
    if !status.success() {
        return Err(format!(
            "copy rootfs {} -> {}: cp exited {status}",
            spec.rootfs,
            target.display()
        ));
    }

    let spec_json = materialize_bind_mounts(
        &spec_json,
        &sandbox.share_dir,
        id,
        sandbox.standalone,
        sandbox.hostname.as_deref(),
        sandbox.sysctls.as_ref(),
    )?;
    let spec_json = match &sandbox.rosetta_tag {
        Some(tag) => crate::rosetta::add_to_container(&spec_json, tag)?,
        None => spec_json,
    };
    let spec_json = match crate::network::trust() {
        Some(trust) => crate::trust::apply(&target, &spec_json, &trust)
            .map_err(|e| format!("write Floci's CA into the container: {e}"))?,
        None if crate::network::needs_trust() => {
            return Err(
                "the node's network profile routes hosts or isolates pods, but has no trust \
                 material for containers"
                    .into(),
            )
        }
        None => spec_json,
    };

    let resp = pod_request(
        &sandbox.socket,
        &AgentRequest::PodCreate {
            id: id.to_string(),
            rootfs_rel: format!("{id}/rootfs"),
            spec_json,
            tty: false,
        },
    )?;
    expect_ok(resp, "PodCreate")
}

/// Place the bind mounts referenced by `config.json` in the pod share, so the
/// guest agent can bind them into the container, and rewrite each such
/// mount's `source` to the guest-visible path under [`POD_SHARE_GUEST_MOUNT`].
/// With `in_place`, as for a standalone container, each source is bound in
/// place there ([`crate::mounts::bind_in_place`]), read-only when the mount
/// says `ro`, so the container writes to the source itself; otherwise, as
/// for a CRI container, each is copied. The container's spec also gets the
/// pod's hostname and its IPC and UTS sysctls, which containerd puts on the
/// sandbox's spec only.
///
/// Only real host-path bind mounts are placed ([`share_mounts`]): volumes and
/// the generated `/etc/resolv.conf`, `/etc/hosts`, `/etc/hostname`,
/// termination-log, etc. The virtual mounts in the spec (proc/sysfs/tmpfs/
/// devpts/mqueue/cgroup) are left untouched; the guest's default OCI spec
/// provides those, and the agent keeps one at a path the default does not
/// have. Returns the rewritten spec JSON.
fn materialize_bind_mounts(
    spec_json: &str,
    share_dir: &Path,
    id: &str,
    in_place: bool,
    sandbox_hostname: Option<&str>,
    sandbox_sysctls: Option<&serde_json::Value>,
) -> Result<String, String> {
    let mut spec: serde_json::Value =
        serde_json::from_str(spec_json).map_err(|e| format!("parse config.json: {e}"))?;

    // The pod VMs' kernel has no io cgroup controller, and crun fails on any
    // block IO setting, even the empty one that Docker writes.
    if let Some(resources) = spec
        .pointer_mut("/linux/resources")
        .and_then(|r| r.as_object_mut())
    {
        resources.remove("blockIO");
    }
    // The container shares the guest's network namespace, so crun refuses a
    // net.* sysctl of its own. The sandbox's are applied guest-wide, and a
    // standalone container's spec is its sandbox's.
    if let Some(sysctls) = spec
        .pointer_mut("/linux/sysctl")
        .and_then(|s| s.as_object_mut())
    {
        sysctls.retain(|key, _| !key.starts_with("net."));
    }

    // Inject the pod hostname if the container spec doesn't carry one.
    if let Some(h) = sandbox_hostname {
        let empty = spec
            .get("hostname")
            .and_then(|v| v.as_str())
            .map(|s| s.is_empty())
            .unwrap_or(true);
        if empty {
            spec["hostname"] = serde_json::Value::String(h.to_string());
        }
    }

    // Merge the pod's IPC/UTS sysctls into the container spec (a container-specific
    // sysctl already present wins). crun sets them inside the container's own IPC
    // and UTS namespaces, which each pod container unshares. net.* sysctls are
    // left out: the pod container declares no network namespace, so crun would
    // reject them. `apply_guest_net_sysctls` applies them guest-wide at sandbox
    // setup, where the shared net namespace makes them visible to every container.
    if let Some(serde_json::Value::Object(pod)) = sandbox_sysctls {
        let namespaced: Vec<(&String, &serde_json::Value)> =
            pod.iter().filter(|(k, _)| !k.starts_with("net.")).collect();
        if !namespaced.is_empty() {
            let target = spec
                .pointer_mut("/linux/sysctl")
                .and_then(|v| v.as_object_mut());
            match target {
                Some(existing) => {
                    for (k, v) in namespaced {
                        existing.entry(k.clone()).or_insert_with(|| v.clone());
                    }
                }
                None => {
                    let obj = namespaced
                        .into_iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    spec["linux"]["sysctl"] = serde_json::Value::Object(obj);
                }
            }
        }
    }

    let Some(mounts) = spec.get_mut("mounts").and_then(|m| m.as_array_mut()) else {
        return Ok(serde_json::to_string(&spec).unwrap_or_else(|_| spec_json.to_string()));
    };

    let mounts_dir = share_dir.join(id).join("mounts");
    for (n, mount) in share_mounts(mounts).into_iter().enumerate() {
        std::fs::create_dir_all(&mounts_dir)
            .map_err(|e| format!("mkdir {}: {e}", mounts_dir.display()))?;
        let dst = mounts_dir.join(n.to_string());
        if in_place {
            crate::mounts::bind_in_place(Path::new(&mount.source), &dst, mount.read_only)?;
        } else {
            // `cp -aT` makes `dst` a copy of the source, file or directory,
            // with its modes, owners and symlinks; a reflink keeps it cheap.
            let status = std::process::Command::new("cp")
                .args(["-aT", "--reflink=auto", &mount.source])
                .arg(&dst)
                .status()
                .map_err(|e| format!("spawn cp for mount {}: {e}", mount.source))?;
            if !status.success() {
                return Err(format!("copy mount {}: cp exited {status}", mount.source));
            }
        }
        mounts[mount.index]["source"] =
            serde_json::Value::String(format!("{POD_SHARE_GUEST_MOUNT}/{id}/mounts/{n}"));
    }

    serde_json::to_string(&spec).map_err(|e| format!("reserialize spec: {e}"))
}

/// A bind mount of a container's spec that the pod share serves the guest.
#[derive(Debug, PartialEq)]
struct ShareMount {
    /// Its index in the spec's `mounts`.
    index: usize,
    source: String,
    read_only: bool,
}

/// The mounts in `mounts` whose source is an existing absolute host path, in
/// their order. Some specs omit the `bind` type and rely on options, so any
/// such mount counts, except the virtual ones, whose sources are no paths. A
/// source that does not exist is left as it is.
fn share_mounts(mounts: &[serde_json::Value]) -> Vec<ShareMount> {
    mounts
        .iter()
        .enumerate()
        .filter_map(|(index, m)| {
            let mtype = m.get("type").and_then(|t| t.as_str()).unwrap_or_default();
            let source = m.get("source").and_then(|s| s.as_str()).unwrap_or_default();
            let is_virtual = matches!(
                mtype,
                "proc"
                    | "sysfs"
                    | "tmpfs"
                    | "cgroup"
                    | "cgroup2"
                    | "mqueue"
                    | "devpts"
                    | "devtmpfs"
            );
            if is_virtual || !source.starts_with('/') || !Path::new(source).exists() {
                return None;
            }
            let read_only = m
                .get("options")
                .and_then(|o| o.as_array())
                .is_some_and(|options| options.iter().any(|o| o.as_str() == Some("ro")));
            Some(ShareMount {
                index,
                source: source.to_string(),
                read_only,
            })
        })
        .collect()
}

// ============================ node service backends =========================

/// The backends of the node service: a new [`EnginePodBackend`] for each pod.
pub struct EngineBackends;

impl crate::node::Backends for EngineBackends {
    fn pod(&self, _id: &str) -> Arc<dyn PodBackend> {
        Arc::new(EnginePodBackend::new())
    }

    fn vm(&self) -> Arc<dyn crate::lifecycle::VmOps> {
        Arc::new(crate::node::vm::SmolvmOps)
    }

    fn watches_vm_processes(&self) -> bool {
        true
    }

    fn after_delete(&self, id: &str, netns: Option<&str>) {
        crate::cleanup::remove_bridge(id, netns);
        crate::standalone::remove_netns(id);
    }

    fn standalone_netns(
        &self,
        id: &str,
        bundle: &str,
        spec: &serde_json::Value,
    ) -> Result<Option<String>, String> {
        crate::standalone::create_netns(id, bundle, spec)
    }

    fn reserve_pids(&self, above: u32) {
        reserve_pids(above);
    }

    fn next_pid(&self) -> u32 {
        next_pid()
    }

    /// Delete the VM of sandbox `id` that no backend has, such as one whose
    /// node service ended. A missing machine is one already deleted.
    fn reap(&self, id: &str, netns: Option<&str>) {
        let rt = match smolvm::embedded::runtime() {
            Ok(rt) => rt,
            Err(e) => {
                warn!("runtime unavailable, cannot reap the VM of sandbox {id}: {e}");
                return;
            }
        };
        // After every delete of a sandbox, containerd's delete action asks
        // for this again, when its network namespace may be on its way out.
        match rt.machine_record(id) {
            Ok(Some(_)) => {}
            Ok(None) => {
                debug!("sandbox {id} has no VM to reap");
                return;
            }
            Err(e) => warn!("read the record of sandbox {id}: {e}"),
        }
        match delete_sandbox_vm(&rt, id) {
            Ok(()) => {
                crate::cleanup::remove_bridge(id, netns);
                crate::standalone::remove_netns(id);
                crate::cleanup::sweep();
            }
            Err(e) => warn!("reap the VM of sandbox {id}: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec_with(annotations: serde_json::Value) -> serde_json::Value {
        serde_json::json!({ "annotations": annotations })
    }

    #[test]
    fn deleting_sandbox_share_reclaims_container_copies_and_is_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let deleted = root.path().join("deleted-pod");
        let live = root.path().join("live-pod");
        std::fs::create_dir_all(deleted.join("podshare/ctr/rootfs")).unwrap();
        std::fs::write(deleted.join("podshare/ctr/rootfs/data"), b"reclaimed").unwrap();
        std::fs::create_dir(&live).unwrap();

        remove_sandbox_share_at(root.path(), "deleted-pod").unwrap();
        remove_sandbox_share_at(root.path(), "deleted-pod").unwrap();
        assert!(!deleted.exists());
        assert!(live.exists());
        assert!(remove_sandbox_share_at(root.path(), "../live-pod").is_err());
        assert!(live.exists());
    }

    #[test]
    fn no_annotations_means_no_gpu() {
        assert_eq!(sandbox_gpu_request(None), GpuRequest::default());
        assert_eq!(
            sandbox_gpu_request(Some(&serde_json::json!({}))),
            GpuRequest::default()
        );
        assert_eq!(
            sandbox_gpu_request(Some(&spec_with(serde_json::json!({})))),
            GpuRequest::default()
        );
    }

    #[test]
    fn affirmative_values_enable_the_gpu() {
        for v in ["true", "1", "TRUE", " true "] {
            assert_eq!(
                sandbox_gpu_request(Some(&spec_with(serde_json::json!({ ANN_GPU: v })))),
                GpuRequest {
                    enabled: true,
                    vram_mib: None
                },
                "expected {v:?} to enable the GPU"
            );
        }
    }

    /// A pod that did not clearly ask for a GPU keeps the CPU-only VM it would
    /// otherwise have had: a 4 GiB VRAM reservation is not a safe default.
    #[test]
    fn non_affirmative_values_leave_the_gpu_off() {
        for v in ["false", "0", "", "yes", "maybe"] {
            assert_eq!(
                sandbox_gpu_request(Some(&spec_with(serde_json::json!({ ANN_GPU: v })))),
                GpuRequest::default(),
                "expected {v:?} to leave the GPU off"
            );
        }
    }

    #[test]
    fn vram_size_is_read_when_the_gpu_is_on() {
        assert_eq!(
            sandbox_gpu_request(Some(&spec_with(
                serde_json::json!({ ANN_GPU: "true", ANN_GPU_VRAM_MIB: "2048" })
            ))),
            GpuRequest {
                enabled: true,
                vram_mib: Some(2048)
            }
        );
    }

    #[test]
    fn a_bad_vram_size_falls_back_to_the_default_without_denying_the_gpu() {
        for v in ["0", "-1", "lots", ""] {
            assert_eq!(
                sandbox_gpu_request(Some(&spec_with(
                    serde_json::json!({ ANN_GPU: "true", ANN_GPU_VRAM_MIB: v })
                ))),
                GpuRequest {
                    enabled: true,
                    vram_mib: None
                },
                "expected {v:?} to fall back to the default size"
            );
        }
    }

    #[test]
    fn vram_size_alone_does_not_enable_the_gpu() {
        assert_eq!(
            sandbox_gpu_request(Some(&spec_with(
                serde_json::json!({ ANN_GPU_VRAM_MIB: "2048" })
            ))),
            GpuRequest::default()
        );
    }

    #[test]
    fn processes_that_need_input_are_refused() {
        let output = Stdio {
            stdout: "/run/stdout".into(),
            stderr: "/run/stderr".into(),
            ..Stdio::default()
        };
        refuse_input(&output).unwrap();
        let stdin = Stdio {
            stdin: "/run/stdin".into(),
            ..Stdio::default()
        };
        assert!(refuse_input(&stdin).unwrap_err().contains("stdin"));
        let tty = Stdio {
            terminal: true,
            ..Stdio::default()
        };
        assert!(refuse_input(&tty).unwrap_err().contains("terminal"));
    }

    /// Writes the /proc entry of process `pid` under `proc`, named `comm`.
    fn fake_process(proc: &Path, pid: u32, comm: &str, ppid: u32, threads: u32, fds: u32) {
        let dir = proc.join(pid.to_string());
        std::fs::create_dir_all(dir.join("fd")).unwrap();
        std::fs::write(dir.join("comm"), format!("{comm}\n")).unwrap();
        std::fs::write(
            dir.join("stat"),
            format!("{pid} ({comm}) S {ppid} {pid} {pid} 0 -1 4194560\n"),
        )
        .unwrap();
        std::fs::write(
            dir.join("status"),
            format!("Name:\t{comm}\nState:\tS (sleeping)\nThreads:\t{threads}\n"),
        )
        .unwrap();
        for fd in 0..fds {
            std::fs::write(dir.join("fd").join(fd.to_string()), "").unwrap();
        }
    }

    /// Runs the agent stats script against the /proc tree at `proc`.
    fn run_agent_stats(proc: &Path) -> std::process::Output {
        let script = AGENT_STATS.replace("/proc", proc.to_str().unwrap());
        std::process::Command::new("/bin/sh")
            .args(["-c", &script])
            .output()
            .unwrap()
    }

    #[test]
    fn agent_stats_count_the_agent_and_not_a_child_it_forked() {
        let proc = tempfile::tempdir().unwrap();
        fake_process(proc.path(), 1, "init", 0, 1, 4);
        fake_process(proc.path(), 44, "memfd:agent", 1, 11, 18);
        // The glob of /proc lists 19882 before 44.
        fake_process(proc.path(), 19882, "memfd:agent", 44, 1, 9);
        fake_process(proc.path(), 88, "crun", 44, 1, 6);
        let output = run_agent_stats(proc.path());
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(String::from_utf8_lossy(&output.stdout), "44 11 18\n");
    }

    #[test]
    fn agent_stats_fail_without_the_agent() {
        let proc = tempfile::tempdir().unwrap();
        fake_process(proc.path(), 1, "init", 0, 1, 4);
        fake_process(proc.path(), 19882, "memfd:agent", 44, 1, 9);
        let output = run_agent_stats(proc.path());
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("no smolvm-agent process"));
    }

    /// Docker's spec sets an empty block IO and net.* sysctls, which crun
    /// refuses in the guest.
    #[test]
    fn block_io_and_net_sysctls_are_left_out_of_a_container() {
        let share = tempfile::tempdir().unwrap();
        let spec = serde_json::json!({
            "linux": {
                "resources": {"blockIO": {}, "pids": {"limit": 10}},
                "sysctl": {
                    "net.ipv4.ip_unprivileged_port_start": "0",
                    "kernel.shm_rmid_forced": "1",
                },
            },
        });
        let out = materialize_bind_mounts(&spec.to_string(), share.path(), "c1", false, None, None)
            .unwrap();
        let out: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(
            out["linux"]["resources"],
            serde_json::json!({"pids": {"limit": 10}})
        );
        assert_eq!(
            out["linux"]["sysctl"],
            serde_json::json!({"kernel.shm_rmid_forced": "1"})
        );
    }

    /// Docker's mounts for `-v pv:/vol -v /dir:/ro:ro --tmpfs /tmpf
    /// --mount type=volume,...` and its generated /etc files.
    #[test]
    fn host_path_mounts_go_through_the_share_with_their_read_only_flag() {
        let node = tempfile::tempdir().unwrap();
        let volume = node.path().join("volumes/pv/_data");
        let dir = node.path().join("dir");
        let hosts = node.path().join("containers/c1/hosts");
        std::fs::create_dir_all(&volume).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::create_dir_all(hosts.parent().unwrap()).unwrap();
        std::fs::write(&hosts, "127.0.0.1 localhost\n").unwrap();
        let path = |p: &Path| p.to_string_lossy().into_owned();
        let mounts = serde_json::json!([
            {"destination": "/proc", "type": "proc", "source": "proc"},
            {"destination": "/dev/shm", "type": "tmpfs", "source": "shm"},
            {"destination": "/tmpf", "type": "tmpfs", "source": "tmpfs",
             "options": ["noexec", "rprivate"]},
            {"destination": "/vol", "type": "bind", "source": path(&volume),
             "options": ["rbind"]},
            {"destination": "/ro", "type": "bind", "source": path(&dir),
             "options": ["rbind", "ro", "rprivate"]},
            {"destination": "/gone", "type": "bind",
             "source": path(&node.path().join("gone")), "options": ["rbind"]},
            {"destination": "/untyped", "source": path(&dir), "options": ["rbind"]},
            {"destination": "/etc/hosts", "type": "bind", "source": path(&hosts),
             "options": ["rbind", "rprivate"]},
        ]);
        let mounts = mounts.as_array().unwrap();

        let placed = share_mounts(mounts);

        let mount = |index, source: &Path, read_only| ShareMount {
            index,
            source: path(source),
            read_only,
        };
        assert_eq!(
            placed,
            [
                mount(3, &volume, false),
                mount(4, &dir, true),
                mount(6, &dir, false),
                mount(7, &hosts, false),
            ]
        );
    }

    /// A CRI container's mounts are copies under the pod share, and the spec
    /// points at them in the guest.
    #[test]
    fn a_cri_container_gets_copies_of_its_mounts() {
        let node = tempfile::tempdir().unwrap();
        let share = node.path().join("podshare");
        let volume = node.path().join("volume");
        std::fs::create_dir_all(&volume).unwrap();
        std::fs::write(volume.join("data"), "kept").unwrap();
        let spec = serde_json::json!({
            "mounts": [
                {"destination": "/tmp", "type": "tmpfs", "source": "tmpfs"},
                {"destination": "/data", "type": "bind",
                 "source": volume.to_string_lossy(), "options": ["rbind", "ro"]},
            ],
        });

        let out =
            materialize_bind_mounts(&spec.to_string(), &share, "c1", false, None, None).unwrap();

        let out: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(out["mounts"][0]["source"], "tmpfs");
        assert_eq!(
            out["mounts"][1]["source"],
            format!("{POD_SHARE_GUEST_MOUNT}/c1/mounts/0")
        );
        assert_eq!(
            out["mounts"][1]["options"],
            serde_json::json!(["rbind", "ro"])
        );
        let copy = share.join("c1/mounts/0/data");
        assert_eq!(std::fs::read_to_string(&copy).unwrap(), "kept");
        std::fs::write(&copy, "changed").unwrap();
        assert_eq!(
            std::fs::read_to_string(volume.join("data")).unwrap(),
            "kept"
        );
    }
}
