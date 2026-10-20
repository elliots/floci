//! The node service (`smolvm-pods node-service`): the one process of a node
//! that owns the pod VMs.
//!
//! containerd talks only to the front shim ([`crate::front`]), which forwards
//! every call that needs a VM to this service over [`crate::proto`]. The
//! service hosts a [`PodBackend`] for each pod, and runs the pods' hibernation
//! through [`crate::lifecycle`], over smolvm ([`vm::SmolvmOps`]). It may
//! restart while pods run or sleep: the VMs outlive it, its records keep the
//! pods, and the front shim keeps containerd's view of the tasks. Its API
//! ([`crate::api`]) serves `smolvm-pods ctl` on a Unix socket, and Floci on
//! a TCP listener with a bearer token when `--api-listen` is given. It wakes
//! pods on traffic and puts idle pods to sleep ([`sleep`]).

mod admission;
mod server;
pub mod settings;
mod sleep;
pub mod vm;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use log::{info, warn};
use tokio::signal::unix::{signal, SignalKind};

use crate::backend::{MockBackend, PodBackend};
use crate::engine::{EngineBackends, POD_SHARE_HOST_ROOT};
use crate::files::{self, LockError};
use crate::lifecycle::{ClockStep, PauseControl, PauseOutcome, VmOps, VmProcess, VmState, VmView};
use crate::sync::lock;

pub use server::Node;
pub use settings::Settings;

/// The node service's state on disk, beside the pod shares. The leading dot
/// keeps it out of `floci-node`'s sweep of the pod shares at boot.
pub fn state_dir() -> PathBuf {
    Path::new(POD_SHARE_HOST_ROOT).join(".node")
}

/// Where the node service gets a backend for each pod, and the VM operations
/// of its lifecycle.
pub trait Backends: Send + Sync + 'static {
    /// The backend of pod `id`, new or to adopt.
    fn pod(&self, id: &str) -> Arc<dyn PodBackend>;

    /// The lifecycle's VM operations.
    fn vm(&self) -> Arc<dyn VmOps>;

    /// Whether the pods' VM processes are real processes to watch.
    fn watches_vm_processes(&self) -> bool {
        false
    }

    /// Remove the bridge of a deleted pod VM in its pod network namespace
    /// `netns`. Blocking.
    fn after_delete(&self, _id: &str, _netns: Option<&str>) {}

    /// Delete the VM of sandbox `id` that no record names. Blocking.
    fn reap(&self, id: &str, netns: Option<&str>);

    /// Hand out no pid up to `above`, after a restart.
    fn reserve_pids(&self, _above: u32) {}

    /// A pid for a process that the node service answers itself, such as an
    /// exec answered from the probe cache.
    fn next_pid(&self) -> u32;
}

/// In-process mock pods (`SMOLVM_SHIM_MOCK=1`, and the unit tests). A pod's
/// mock outlives the node service that created it, as a VM would, so that a
/// test can restart the service while pods run. Mock pods do not hibernate.
#[derive(Default)]
pub struct MockBackends {
    vms: Arc<MockVms>,
}

#[derive(Default)]
struct MockVms {
    pods: Mutex<HashMap<String, Arc<MockBackend>>>,
    /// The sandboxes reaped, in order.
    reaped: Arc<Mutex<Vec<String>>>,
    /// The nominal VM process of each pod.
    processes: Mutex<HashMap<String, VmProcess>>,
    next_pid: AtomicI32,
    next_task_pid: AtomicU32,
}

#[cfg(test)]
impl MockBackends {
    pub fn get(&self, id: &str) -> Option<Arc<MockBackend>> {
        lock(&self.vms.pods).get(id).cloned()
    }

    pub fn reaped(&self) -> Arc<Mutex<Vec<String>>> {
        self.vms.reaped.clone()
    }

    /// End a mock VM, as a crash would while no node service ran.
    pub fn crash(&self, id: &str) {
        lock(&self.vms.pods).remove(id);
    }
}

impl MockVms {
    /// The mock of a pod whose sandbox exists. Blocking.
    fn running(&self, id: &str) -> Option<Arc<MockBackend>> {
        let mock = lock(&self.pods).get(id).cloned()?;
        let runtime = tokio::runtime::Handle::current();
        runtime.block_on(mock.has(id, None)).then_some(mock)
    }

    fn process(&self, id: &str) -> VmProcess {
        *lock(&self.processes)
            .entry(id.to_string())
            .or_insert_with(|| VmProcess {
                pid: 1_000_000 + self.next_pid.fetch_add(1, Ordering::Relaxed),
                start_time: Some(1),
            })
    }

    fn remove(&self, id: &str) {
        let mock = lock(&self.pods).remove(id);
        if let Some(mock) = mock {
            let runtime = tokio::runtime::Handle::current();
            if let Err(e) = runtime.block_on(mock.delete(id, None)) {
                warn!("delete mock sandbox {id}: {e}");
            }
        }
        lock(&self.processes).remove(id);
        lock(&self.reaped).push(id.to_string());
    }
}

impl VmOps for MockVms {
    fn inspect(&self, machine: &str) -> Result<VmView, String> {
        if self.running(machine).is_none() {
            return Ok(VmView::missing());
        }
        Ok(VmView {
            state: VmState::Running,
            process: Some(self.process(machine)),
            alive: true,
            checkpoint: false,
            checkpoint_bytes: None,
        })
    }

    fn machines(&self) -> Result<Vec<String>, String> {
        let ids: Vec<String> = lock(&self.pods).keys().cloned().collect();
        Ok(ids
            .into_iter()
            .filter(|id| self.running(id).is_some())
            .collect())
    }

    fn pause(&self, _machine: &str, _control: &Arc<PauseControl>) -> Result<PauseOutcome, String> {
        Err("mock pods do not hibernate".into())
    }

    fn resume(&self, _machine: &str) -> Result<(), String> {
        Err("mock pods do not hibernate".into())
    }

    fn stop_paused(&self, _machine: &str) -> Result<(), String> {
        Err("mock pods do not hibernate".into())
    }

    fn stop(&self, machine: &str) -> Result<(), String> {
        self.remove(machine);
        Ok(())
    }

    fn delete(&self, machine: &str) -> Result<(), String> {
        self.remove(machine);
        Ok(())
    }

    fn is_alive(&self, process: &VmProcess) -> bool {
        let owner = lock(&self.processes)
            .iter()
            .find(|(_, p)| *p == process)
            .map(|(id, _)| id.clone());
        owner.is_some_and(|id| self.running(&id).is_some())
    }

    fn has_tso(&self, _machine: &str, _process: &VmProcess) -> bool {
        true
    }

    fn ping(&self, machine: &str) -> Result<(), String> {
        self.running(machine)
            .map(drop)
            .ok_or_else(|| format!("no mock VM {machine}"))
    }

    fn set_clock(&self, _machine: &str, _host_realtime_ns: i64) -> Result<ClockStep, String> {
        Ok(ClockStep {
            skew_ns: 0,
            stepped: false,
        })
    }

    fn signal(
        &self,
        machine: &str,
        container: &str,
        exec: Option<&str>,
        signal: u32,
        all: bool,
    ) -> Result<(), String> {
        let mock = self
            .running(machine)
            .ok_or_else(|| format!("no mock VM {machine}"))?;
        let runtime = tokio::runtime::Handle::current();
        runtime.block_on(mock.kill(container, exec, signal, all))
    }
}

impl Backends for MockBackends {
    fn pod(&self, id: &str) -> Arc<dyn PodBackend> {
        lock(&self.vms.pods)
            .entry(id.to_string())
            .or_default()
            .clone()
    }

    fn vm(&self) -> Arc<dyn VmOps> {
        self.vms.clone()
    }

    fn reap(&self, id: &str, _netns: Option<&str>) {
        lock(&self.vms.pods).remove(id);
        lock(&self.vms.reaped).push(id.to_string());
    }

    fn next_pid(&self) -> u32 {
        50_000 + self.vms.next_task_pid.fetch_add(1, Ordering::Relaxed)
    }
}

/// How often to reap VM processes without a SIGCHLD: a VM process can exit
/// before smolvm registers it.
const REAP_INTERVAL: Duration = Duration::from_secs(10);

/// The allocator arenas that the node service's threads share. With one
/// arena for each thread of the blocking pool, as glibc gives by default,
/// each keeps what it freed, and every pause leaves a few MiB in the arena
/// of the thread that ran it.
const MALLOC_ARENAS: libc::c_int = 2;

/// The size from which a block gets a mapping of its own, and the free top
/// above which a heap shrinks. Setting them turns off glibc's dynamic
/// thresholds, which rise to the largest mapped block freed so far. With
/// those, the few MiB that a pause allocates come from a heap once a pause
/// has freed them, and stay resident there: glibc shrinks a heap only when
/// its free top reaches twice that size, and malloc_trim shrinks only the
/// main heap's top.
const MALLOC_THRESHOLD: libc::c_int = 128 * 1024;

/// Keeps the node service's heaps from holding what it freed. Call it before
/// any other thread starts.
fn tune_allocator() {
    // SAFETY: mallopt only sets the allocator's parameters.
    unsafe {
        libc::mallopt(libc::M_ARENA_MAX, MALLOC_ARENAS);
        libc::mallopt(libc::M_MMAP_THRESHOLD, MALLOC_THRESHOLD);
        libc::mallopt(libc::M_TRIM_THRESHOLD, MALLOC_THRESHOLD);
    }
}

/// `smolvm-pods node-service ARGS`.
pub fn run(args: &[String]) -> i32 {
    tune_allocator();
    let settings = match Settings::parse(args) {
        Ok(settings) => settings,
        Err(e) => {
            eprintln!("smolvm-pods node-service: {e}");
            return 2;
        }
    };
    crate::logging::init(None);
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("node service: tokio runtime: {e}");
            return 1;
        }
    };
    let node_socket = Path::new(crate::proto::NODE_SOCKET);
    let api_socket = Path::new(crate::api::API_SOCKET);
    match runtime.block_on(serve(node_socket, api_socket, settings)) {
        Ok(()) => 0,
        Err(e) => {
            log::error!("node service: {e}");
            1
        }
    }
}

async fn serve(socket: &Path, api_socket: &Path, settings: Settings) -> Result<(), String> {
    log_settings(&settings);
    let dir = socket.parent().ok_or("the socket has no directory")?;
    std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    let lock_path = dir.join("node.sock.lock");
    let _lock = lock_once(&lock_path)?;
    // Before the pods are loaded, so that a wrong token file or a port in
    // use stops the node service at once.
    let tcp_api = match settings.api_listen {
        Some(addr) => {
            let token = crate::api::Token::read(&settings.api_token_file)?;
            Some((crate::api::listen_tcp(addr).await?, token, addr))
        }
        None => None,
    };
    // The lock is ours, so a socket left there belongs to a node service that
    // is gone.
    let _ = std::fs::remove_file(socket);
    let listener = tokio::net::UnixListener::bind(socket)
        .map_err(|e| format!("listen on {}: {e}", socket.display()))?;
    let mock = std::env::var("SMOLVM_SHIM_MOCK").as_deref() == Ok("1");
    let node = if mock {
        warn!("SMOLVM_SHIM_MOCK=1: using mock backends (no VMs will boot)");
        let backends = Arc::new(MockBackends::default());
        Node::open(backends, state_dir(), server::ABANDONED_AFTER).await?
    } else {
        // The VM processes of this service are its children.
        tokio::spawn(reap_vm_processes());
        Node::open_default(Arc::new(EngineBackends), state_dir(), &settings).await?
    };
    let api = crate::api::listen(api_socket)?;
    tokio::spawn(crate::api::serve(node.clone(), api));
    info!(
        "node service listening on {} and {}",
        socket.display(),
        api_socket.display()
    );
    if let Some((listener, token, addr)) = tcp_api {
        tokio::spawn(crate::api::serve_tcp(node.clone(), listener, token));
        info!(
            "the API also listens on {addr}, with the token of {}",
            settings.api_token_file.display()
        );
    }
    node.serve(listener).await;
    Ok(())
}

fn log_settings(s: &Settings) {
    let cap = |n: u64, unit: &str| match n {
        0 => "none".to_string(),
        n => format!("{n}{unit}"),
    };
    let cpu = match s.cpu_threshold {
        Some(vcpus) => format!("busy above {vcpus} vCPU over {:?}", s.cpu_window),
        None => "no CPU rule".to_string(),
    };
    info!(
        "idle timeout {:?}, activity every {:?}, {cpu}, \
         awake pods cap {}, awake memory cap {}, admission wait {:?}, \
         API TCP listener {}, token file {}",
        s.idle_timeout,
        s.poll_interval,
        cap(u64::from(s.max_awake_pods), ""),
        cap(s.max_awake_memory_mib, " MiB"),
        s.admission_wait,
        s.api_listen.map_or("off".to_string(), |a| a.to_string()),
        s.api_token_file.display(),
    );
}

/// Lock `path` so that one node service runs at a time. Closing the file
/// unlocks it.
fn lock_once(path: &Path) -> Result<std::fs::File, String> {
    files::lock(path, 0o666, false).map_err(|e| match e {
        LockError::Lock(e) => format!("another node service holds {}: {e}", path.display()),
        open => open.message(path),
    })
}

/// Reap the VM processes that smolvm started, which it never waits for. The
/// lifecycle learns of an exit from its watch of the VM process.
async fn reap_vm_processes() {
    let mut exits = match signal(SignalKind::child()) {
        Ok(exits) => exits,
        Err(e) => {
            warn!("cannot watch for exited VM processes: {e}");
            return;
        }
    };
    loop {
        if let Ok(None) = tokio::time::timeout(REAP_INTERVAL, exits.recv()).await {
            return;
        }
        smolvm::process::reap_vm_children();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Set in the process of its own in which
    /// `a_freed_block_of_a_few_mib_leaves_no_resident_memory` measures.
    const MEASURE: &str = "SMOLVM_PODS_TEST_ALLOCATOR";

    fn resident_kib() -> u64 {
        let statm = std::fs::read_to_string("/proc/self/statm").unwrap();
        let pages: u64 = statm.split_whitespace().nth(1).unwrap().parse().unwrap();
        // SAFETY: sysconf has no preconditions.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        pages * page as u64 / 1024
    }

    /// Allocates a block of `kib` KiB, writes to all of it, and frees it.
    fn use_block(kib: usize) {
        let block = vec![1u8; kib << 10];
        std::hint::black_box(&block);
    }

    /// A block of a few MiB that a thread other than the main one allocates
    /// and frees again, as a pause does with its buffers, leaves nothing
    /// resident, also without a malloc_trim. With glibc's dynamic thresholds,
    /// the first free raises them, so the second block comes from a heap and
    /// stays resident there. The allocator's settings are the process's, so
    /// the test measures in a process of its own.
    #[test]
    fn a_freed_block_of_a_few_mib_leaves_no_resident_memory() {
        if std::env::var_os(MEASURE).is_some() {
            tune_allocator();
            let grew = std::thread::spawn(|| {
                use_block(4096);
                let before = resident_kib();
                use_block(4096);
                resident_kib().saturating_sub(before)
            });
            println!("resident growth: {} KiB", grew.join().unwrap());
            return;
        }
        let name = "node::tests::a_freed_block_of_a_few_mib_leaves_no_resident_memory";
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture", "--test-threads=1"])
            .env(MEASURE, "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(out.status.success(), "{stdout}{stderr}");
        let grew: u64 = stdout
            .lines()
            .find_map(|line| line.split("resident growth: ").nth(1)?.strip_suffix(" KiB"))
            .and_then(|kib| kib.parse().ok())
            .unwrap_or_else(|| panic!("no measure in {stdout}{stderr}"));
        assert!(grew < 1024, "the resident set grew by {grew} KiB");
    }
}
