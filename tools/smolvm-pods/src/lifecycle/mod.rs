//! The hibernation lifecycle of pod VMs.
//!
//! Each pod has a small actor, a tokio task that runs the pod's transitions
//! one at a time through the states `creating`, `awake`, `pausing`, `asleep`,
//! `waking`, `failed` and `deleting`. Commands that arrive during a
//! transition wait for it, except that wakes merge into a running wake, and a
//! wake during a pause cancels the pause if it is not yet durable. Leases,
//! execs, container creates and recent wake waiters block a pause.
//!
//! Every transition is written to the pod's record before and after it runs
//! (see [`store`]), with an event (see [`events`]). After a crash, recovery
//! classifies each record against smolvm's view of its VM (see [`recovery`]).
//!
//! The VM work goes through [`VmOps`], and the work of other parts of the
//! node service, such as admission, traffic holds, the catchers and the output
//! pumps, through [`Hooks`]. The node service, and its API, act on pods
//! through the methods of [`Lifecycle`].

mod events;
mod fault;
mod pod;
mod record;
mod recovery;
mod state;
mod store;
mod vm;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use log::{info, warn};
use tokio::sync::{broadcast, oneshot, Semaphore};
use tokio::task::JoinHandle;

pub use events::{Change, Event, EventPage, EventQuery, StepTime};
pub use fault::Faults;
#[cfg(test)]
pub use mock::{clock_at, RunningVms};
pub use record::{
    cpu_threshold, cpu_window, exec_key, hibernation_disabled, idle_timeout, parse_cpu_threshold,
    parse_duration, wakes_on_terminate, ExecRecord, ExecStatus, GuestDelete, PodIdentity,
    PodRecord, StdioPaths, TaskKind, TaskRecord, Timing,
};
pub use state::{Blocker, PausePhase, PodState, Reason};
#[cfg(test)]
pub use vm::NoHooks;
pub use vm::{
    AwakeCause, ClockStep, Hooks, PauseControl, PauseOutcome, Save, SaveMode, VmOps, VmProcess,
    VmState, VmView,
};

use crate::files;
use crate::kube::{Controller, Labels};
use crate::metrics::Transitions;
use crate::probe::PodProbes;
use crate::proto::valid_id;
use crate::sync::{lock, read, write};
use pod::{Command, Pod, WakeWaiter};
use store::Store;

/// Wall-clock time, which tests replace.
#[derive(Clone)]
pub struct Clock(Arc<dyn Fn() -> SystemTime + Send + Sync>);

impl Clock {
    pub fn system() -> Clock {
        Clock(Arc::new(SystemTime::now))
    }

    #[cfg(test)]
    pub fn from_fn(now: impl Fn() -> SystemTime + Send + Sync + 'static) -> Clock {
        Clock(Arc::new(now))
    }

    pub fn now(&self) -> SystemTime {
        (self.0)()
    }

    pub fn now_ms(&self) -> u64 {
        self.since_epoch().as_millis() as u64
    }

    pub fn now_ns(&self) -> i64 {
        self.since_epoch().as_nanos() as i64
    }

    fn since_epoch(&self) -> Duration {
        self.now().duration_since(UNIX_EPOCH).unwrap_or_default()
    }
}

#[derive(Clone)]
pub struct Config {
    /// The state directory.
    pub dir: PathBuf,
    /// The node's boot id. Records of another boot are deleted at start.
    pub boot_id: String,
    /// Pauses that run at once, across pods.
    pub pause_slots: usize,
    /// Resumes that run at once, across pods.
    pub resume_slots: usize,
    /// How long a wake pings the guest agent before the pod fails.
    pub agent_timeout: Duration,
    pub ping_interval: Duration,
    /// How long a completed wake keeps blocking a pause for its waiters.
    pub wake_grace: Duration,
    /// Resumes that fail in a row before the pod fails.
    pub max_wake_failures: u32,
    /// The size at which the event log rotates.
    pub events_max_bytes: u64,
    pub faults: Faults,
    pub clock: Clock,
}

impl Config {
    /// The node service's configuration: the defaults, the node's boot id,
    /// and the fault points of `SMOLVM_PODS_FAULT`.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Config, String> {
        let mut config = Config::defaults(dir);
        config.faults = Faults::from_env(&config.dir)?;
        config.boot_id = node_boot_id();
        Ok(config)
    }

    /// The defaults, with no fault points and an empty boot id.
    pub fn defaults(dir: impl Into<PathBuf>) -> Config {
        Config {
            dir: dir.into(),
            boot_id: String::new(),
            pause_slots: 2,
            resume_slots: 4,
            agent_timeout: Duration::from_secs(30),
            ping_interval: Duration::from_millis(100),
            wake_grace: Duration::from_secs(5),
            max_wake_failures: 3,
            events_max_bytes: 8 << 20,
            faults: Faults::none(),
            clock: Clock::system(),
        }
    }
}

fn node_boot_id() -> String {
    files::boot_id().unwrap_or_else(|e| {
        warn!("read {}: {e}", files::BOOT_ID);
        String::new()
    })
}

#[derive(Debug, Clone, PartialEq)]
pub enum LifecycleError {
    NotFound(String),
    /// The call names an incarnation that the pod no longer has.
    StaleIncarnation(String),
    Invalid(String),
    /// The pod cannot pause now, or the admission hook refused a wake.
    Refused(Vec<Blocker>),
    State {
        state: PodState,
        op: &'static str,
    },
    /// A wake cancelled the pause.
    Cancelled,
    /// The `pausing` hook refused the pause, with this reason.
    PauseRefused(String),
    Failed(String),
    Vm(String),
    Store(String),
    /// The pod was deleted, or the lifecycle shut down, before it answered.
    Gone,
}

impl fmt::Display for LifecycleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LifecycleError::NotFound(what) => write!(f, "no pod {what}"),
            LifecycleError::StaleIncarnation(id) => {
                write!(f, "pod sandbox {id} has another incarnation")
            }
            LifecycleError::Invalid(why) => f.write_str(why),
            LifecycleError::Refused(blockers) => {
                let reasons: Vec<_> = blockers.iter().map(|b| b.to_string()).collect();
                write!(f, "refused: {}", reasons.join("; "))
            }
            LifecycleError::State { state, op } => write!(f, "cannot {op} a pod that is {state}"),
            LifecycleError::Cancelled => f.write_str("a wake cancelled the pause"),
            LifecycleError::PauseRefused(why) => write!(f, "pausing: {why}"),
            LifecycleError::Failed(why) => write!(f, "the pod failed: {why}"),
            LifecycleError::Vm(why) => f.write_str(why),
            LifecycleError::Store(why) => write!(f, "durable state: {why}"),
            LifecycleError::Gone => f.write_str("the pod is gone"),
        }
    }
}

impl std::error::Error for LifecycleError {}

/// How a call names a pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodRef {
    Sandbox(String),
    /// Of the pods with this IP, one that has not failed and is not being
    /// deleted, then the newest.
    Ip(String),
}

impl PodRef {
    pub fn sandbox(id: &str) -> PodRef {
        PodRef::Sandbox(id.to_string())
    }
}

impl fmt::Display for PodRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PodRef::Sandbox(id) => write!(f, "with sandbox id {id}"),
            PodRef::Ip(ip) => write!(f, "with IP {ip}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WakeRequest {
    pub reason: Reason,
    pub source: Option<String>,
    /// Also hold a lease with this TTL in seconds, to keep the pod awake.
    pub lease_ttl: Option<f64>,
    pub incarnation: Option<String>,
}

impl WakeRequest {
    pub fn new(reason: Reason) -> WakeRequest {
        WakeRequest {
            reason,
            source: None,
            lease_ttl: None,
            incarnation: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HibernateRequest {
    pub reason: Reason,
    pub source: Option<String>,
    pub incarnation: Option<String>,
}

impl HibernateRequest {
    pub fn new(reason: Reason) -> HibernateRequest {
        HibernateRequest {
            reason,
            source: None,
            incarnation: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct HoldRequest {
    /// The lease's id, which holding again renews. A new random id if unset.
    pub id: Option<String>,
    /// Seconds, more than 0 and at most 60; 30 if unset.
    pub ttl_secs: Option<f64>,
    /// Without an id, a holder that holds again renews its lease instead of
    /// adding one.
    pub holder: Option<String>,
    pub incarnation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KillRequest {
    pub container: String,
    pub exec: Option<String>,
    pub signal: u32,
    /// containerd's `all`: every process of the container.
    pub all: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WakeReport {
    pub generation: u64,
    /// The pod was resumed from its checkpoint.
    pub resumed: bool,
    /// The wake cancelled a pause, and the VM went on in place.
    pub cancelled_pause: bool,
    pub steps: Vec<StepTime>,
    pub duration_ms: f64,
    pub clock: Option<ClockStep>,
    /// The lease of [`WakeRequest::lease_ttl`].
    pub lease: Option<LeaseInfo>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SleepReport {
    pub generation: u64,
    pub already_asleep: bool,
    pub steps: Vec<StepTime>,
    pub duration_ms: f64,
    pub checkpoint_bytes: Option<u64>,
    pub save: Option<vm::Save>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseInfo {
    pub id: String,
    pub holder: Option<String>,
    pub expires_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillOutcome {
    Delivered,
    /// Recorded without a wake: report this exit status now, and the next
    /// wake kills the guest process.
    Deferred {
        exit_status: u32,
    },
}

/// What the exit of a VM process means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VmExit {
    /// The process of a durable pause.
    Hibernation,
    /// A crash: the pod failed.
    Crash,
    Deleted,
    /// Not the pod's current VM process.
    Stale,
}

/// Work that blocks a pause until its guard is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Activity {
    /// An exec being created, until it is recorded with [`Lifecycle::put_exec`].
    Exec,
    ContainerCreate,
}

pub struct ActivityGuard {
    pod: Arc<Pod>,
    activity: Activity,
}

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        self.pod.end(self.activity);
    }
}

/// The API view of a pod.
#[derive(Debug, Clone, PartialEq)]
pub struct PodInfo {
    pub identity: PodIdentity,
    pub state: PodState,
    pub phase: Option<PausePhase>,
    pub generation: u64,
    pub created_ms: u64,
    pub since_ms: u64,
    /// The leases that have not expired.
    pub leases: Vec<LeaseInfo>,
    pub waiters: usize,
    /// Execs being created, which are not recorded yet.
    pub starting_execs: usize,
    /// Containers being created.
    pub creates: usize,
    /// The recorded execs that have not exited, as `container/exec`.
    pub execs: Vec<String>,
    pub last_wake: Option<Timing>,
    pub last_sleep: Option<Timing>,
    pub checkpoint_bytes: Option<u64>,
    pub wake_failures: u32,
    /// Why the pod failed, or why its last resume failed.
    pub failure: Option<String>,
    pub vm: Option<VmProcess>,
}

/// Runs `f` in `spawn_blocking`.
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| format!("a blocking call ended: {e}"))?
}

/// What the lifecycle and every pod actor share.
pub(crate) struct Shared {
    store: Store,
    vm: Arc<dyn VmOps>,
    hooks: Arc<dyn Hooks>,
    config: Config,
    pause_slots: Semaphore,
    resume_slots: Semaphore,
    events: broadcast::Sender<Event>,
    transitions: Transitions,
    pods: RwLock<HashMap<String, Arc<Pod>>>,
    /// Every actor and transition task, which [`Lifecycle::shutdown`] ends.
    #[cfg(test)]
    tasks: Mutex<Vec<tokio::task::AbortHandle>>,
    recovering: Mutex<Vec<oneshot::Receiver<()>>>,
}

impl Shared {
    fn spawn<T: Send + 'static>(
        &self,
        future: impl Future<Output = T> + Send + 'static,
    ) -> JoinHandle<T> {
        let handle = tokio::spawn(future);
        #[cfg(test)]
        {
            let mut tasks = lock(&self.tasks);
            tasks.retain(|t| !t.is_finished());
            tasks.push(handle.abort_handle());
        }
        handle
    }

    /// Calls `f` with the VM operations in `spawn_blocking`.
    async fn vm_call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&dyn VmOps) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String> {
        let vm = self.vm.clone();
        blocking(move || f(vm.as_ref())).await
    }

    fn emit(&self, mut event: Event) {
        if let Err(e) = self.store.append(&mut event) {
            warn!("record an event of pod sandbox {}: {e}", event.sandbox_id);
        }
        self.transitions.observe(&event);
        let _ = self.events.send(event);
    }

    fn now_ms(&self) -> u64 {
        self.config.clock.now_ms()
    }
}

/// The lifecycle of every pod of the node. Cloning shares it.
#[derive(Clone)]
pub struct Lifecycle {
    shared: Arc<Shared>,
}

impl Lifecycle {
    /// Locks the state directory, loads the records, deletes smolvm machines
    /// that no record names, and starts the recovery of every pod. Calls on a
    /// pod wait for its recovery; [`Lifecycle::settled`] waits for all.
    pub async fn open(
        config: Config,
        vm: Arc<dyn VmOps>,
        hooks: Arc<dyn Hooks>,
    ) -> Result<Lifecycle, LifecycleError> {
        let store =
            Store::open(&config.dir, config.events_max_bytes).map_err(LifecycleError::Store)?;
        let records = match store.load() {
            Ok(records) => records,
            Err(e) => {
                store.unlock();
                return Err(LifecycleError::Store(e));
            }
        };
        let shared = Arc::new(Shared {
            pause_slots: Semaphore::new(config.pause_slots.max(1)),
            resume_slots: Semaphore::new(config.resume_slots.max(1)),
            store,
            vm,
            hooks,
            config,
            events: broadcast::channel(1024).0,
            transitions: Transitions::default(),
            pods: RwLock::new(HashMap::new()),
            #[cfg(test)]
            tasks: Mutex::new(Vec::new()),
            recovering: Mutex::new(Vec::new()),
        });
        let named: HashSet<String> = records.iter().map(|r| r.identity.machine.clone()).collect();
        match shared.vm_call(|vm| vm.machines()).await {
            Ok(machines) => {
                for machine in machines.into_iter().filter(|m| !named.contains(m)) {
                    info!("delete smolvm machine {machine}, which no pod record names");
                    let name = machine.clone();
                    if let Err(e) = shared.vm_call(move |vm| vm.delete(&name)).await {
                        warn!("delete smolvm machine {machine}: {e}");
                    }
                }
            }
            Err(e) => warn!("list the smolvm machines: {e}"),
        }
        let mut pods = write(&shared.pods);
        for record in records {
            let pod = Pod::start(&shared, record);
            let (done, recovered) = oneshot::channel();
            let _ = pod.send(Command::Recover { done });
            lock(&shared.recovering).push(recovered);
            pods.insert(pod.id.clone(), pod);
        }
        drop(pods);
        Ok(Lifecycle { shared })
    }

    /// Waits until the recovery of every pod loaded at open has finished.
    pub async fn settled(&self) {
        let pending: Vec<_> = lock(&self.shared.recovering).drain(..).collect();
        for recovered in pending {
            let _ = recovered.await;
        }
    }

    /// Ends every actor and transition at once, as a crash would, and unlocks
    /// the state directory. Calls on any clone fail afterwards.
    #[cfg(test)]
    pub fn shutdown(&self) {
        for task in lock(&self.shared.tasks).drain(..) {
            task.abort();
        }
        write(&self.shared.pods).clear();
        self.shared.store.unlock();
    }

    /// Creates a pod: writes its record as `creating`, runs `boot`, which
    /// boots the VM and sets up the guest, then records the VM process and
    /// the pod is awake. A warning that `boot` returns goes in the event of
    /// the pod becoming awake. If `boot` fails, the machine is deleted and the
    /// record dropped. A create of an existing pod returns it.
    pub async fn create<F>(
        &self,
        mut identity: PodIdentity,
        boot: F,
    ) -> Result<PodInfo, LifecycleError>
    where
        F: Future<Output = Result<Option<String>, String>> + Send + 'static,
    {
        let id = identity.sandbox_id.clone();
        if !valid_id(&id) {
            return Err(LifecycleError::Invalid(format!(
                "invalid sandbox id {id:?}"
            )));
        }
        let now = self.shared.now_ms();
        let (reply, answer) = oneshot::channel();
        {
            let mut pods = write(&self.shared.pods);
            if let Some(existing) = pods.get(&id) {
                let record = existing.record();
                let known = &record.identity;
                if (&known.uid, &known.namespace, &known.name, &known.machine)
                    != (
                        &identity.uid,
                        &identity.namespace,
                        &identity.name,
                        &identity.machine,
                    )
                {
                    return Err(LifecycleError::Invalid(format!(
                        "sandbox {id} already belongs to another pod"
                    )));
                }
                return match record.state {
                    PodState::Failed | PodState::Deleting => Err(LifecycleError::State {
                        state: record.state,
                        op: "create",
                    }),
                    _ => Ok(existing.info(now)),
                };
            }
            identity.incarnation = record::random_id();
            let record = PodRecord::new(identity, self.shared.config.boot_id.clone(), now);
            self.shared
                .store
                .save(&record)
                .map_err(LifecycleError::Store)?;
            let pod = Pod::start(&self.shared, record);
            pod.created(&self.shared);
            // Before any other command can reach the pod, which would find
            // it `creating` and settle it.
            pod.send(Command::Create {
                boot: Mutex::new(Box::pin(boot)),
                reply,
            })?;
            pods.insert(id, pod);
        }
        answer.await.map_err(|_| LifecycleError::Gone)?
    }

    /// Wakes a pod and returns when it is awake. Wakes merge, and a wake
    /// during a pause cancels it if it can.
    pub async fn wake(
        &self,
        pod: &PodRef,
        request: WakeRequest,
    ) -> Result<WakeReport, LifecycleError> {
        let pod = self.resolve(pod, request.incarnation.as_deref())?;
        let lease = match request.lease_ttl {
            Some(ttl) => Some(pod.hold(&self.shared, None, Some(ttl), None)?),
            None => None,
        };
        let _waiter = pod.waiter();
        let (reply, answer) = oneshot::channel();
        pod.send(Command::Wake(WakeWaiter {
            reply,
            reason: request.reason,
            source: request.source,
        }))?;
        let mut report = answer.await.map_err(|_| LifecycleError::Gone)??;
        report.lease = lease;
        Ok(report)
    }

    /// Pauses an awake pod to its checkpoint. It is refused, with the
    /// reasons, while anything blocks a pause.
    pub async fn hibernate(
        &self,
        pod: &PodRef,
        request: HibernateRequest,
    ) -> Result<SleepReport, LifecycleError> {
        let pod = self.resolve(pod, request.incarnation.as_deref())?;
        let (reply, answer) = oneshot::channel();
        pod.send(Command::Hibernate {
            reason: request.reason,
            source: request.source,
            reply,
        })?;
        answer.await.map_err(|_| LifecycleError::Gone)?
    }

    /// Adds a lease, which blocks a pause until it expires or is released.
    /// It does not wake the pod.
    pub fn hold(&self, pod: &PodRef, request: HoldRequest) -> Result<LeaseInfo, LifecycleError> {
        let pod = self.resolve(pod, request.incarnation.as_deref())?;
        pod.hold(&self.shared, request.id, request.ttl_secs, request.holder)
    }

    pub fn renew(
        &self,
        pod: &PodRef,
        lease: &str,
        ttl_secs: Option<f64>,
        incarnation: Option<&str>,
    ) -> Result<LeaseInfo, LifecycleError> {
        self.resolve(pod, incarnation)?
            .renew(&self.shared, lease, ttl_secs)
    }

    /// Removes a lease. An unknown lease is not an error.
    pub fn release(
        &self,
        pod: &PodRef,
        lease: &str,
        incarnation: Option<&str>,
    ) -> Result<(), LifecycleError> {
        self.resolve(pod, incarnation)?.release(&self.shared, lease)
    }

    pub fn get(&self, pod: &PodRef) -> Option<PodInfo> {
        self.find(pod).map(|p| p.info(self.shared.now_ms()))
    }

    /// Every pod, by sandbox id.
    pub fn list(&self) -> Vec<PodInfo> {
        let now = self.shared.now_ms();
        let mut pods: Vec<_> = read(&self.shared.pods)
            .values()
            .map(|p| p.info(now))
            .collect();
        pods.sort_by(|a, b| a.identity.sandbox_id.cmp(&b.identity.sandbox_id));
        pods
    }

    /// The record of a pod, with its tasks and execs.
    pub fn record(&self, sandbox_id: &str) -> Option<PodRecord> {
        self.find(&PodRef::sandbox(sandbox_id)).map(|p| p.record())
    }

    pub fn events(&self, query: &EventQuery) -> Result<EventPage, LifecycleError> {
        self.shared
            .store
            .events(query)
            .map_err(LifecycleError::Store)
    }

    /// The transitions counted from the events since the node service
    /// started.
    pub fn transitions(&self) -> &Transitions {
        &self.shared.transitions
    }

    /// Every event from now on.
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.shared.events.subscribe()
    }

    /// Deletes a pod without waking it. An unknown pod is not an error.
    pub async fn delete(
        &self,
        pod: &PodRef,
        incarnation: Option<&str>,
    ) -> Result<(), LifecycleError> {
        let pod = match self.resolve(pod, incarnation) {
            Err(LifecycleError::NotFound(_)) => return Ok(()),
            result => result?,
        };
        let (reply, answer) = oneshot::channel();
        if pod.send(Command::Delete { reply }).is_err() {
            return Ok(());
        }
        match answer.await {
            Ok(result) => result,
            Err(_) => Ok(()),
        }
    }

    /// A kill from containerd. SIGKILL, and SIGTERM unless the pod wakes on
    /// terminate, are deferred while the pod sleeps; other signals wake it.
    pub async fn kill(
        &self,
        sandbox_id: &str,
        request: KillRequest,
    ) -> Result<KillOutcome, LifecycleError> {
        let pod = self.resolve(&PodRef::sandbox(sandbox_id), None)?;
        let (reply, answer) = oneshot::channel();
        pod.send(Command::Kill { request, reply })?;
        answer.await.map_err(|_| LifecycleError::Gone)?
    }

    /// The VM watcher asks what an exit of the pod's VM process means. A
    /// crash fails the pod.
    pub async fn vm_exited(&self, sandbox_id: &str, process: VmProcess) -> VmExit {
        let Some(pod) = self.find(&PodRef::sandbox(sandbox_id)) else {
            return VmExit::Deleted;
        };
        let (reply, answer) = oneshot::channel();
        if pod.send(Command::VmExited { process, reply }).is_err() {
            return VmExit::Deleted;
        }
        answer.await.unwrap_or(VmExit::Deleted)
    }

    /// Blocks a pause until the guard is dropped.
    pub fn begin(
        &self,
        sandbox_id: &str,
        activity: Activity,
    ) -> Result<ActivityGuard, LifecycleError> {
        let pod = self.resolve(&PodRef::sandbox(sandbox_id), None)?;
        pod.begin(activity);
        Ok(ActivityGuard { pod, activity })
    }

    /// Blocks a pause and wakes the pod for an exec or a container create.
    pub async fn wake_for(
        &self,
        sandbox_id: &str,
        activity: Activity,
    ) -> Result<(ActivityGuard, WakeReport), LifecycleError> {
        let guard = self.begin(sandbox_id, activity)?;
        let reason = match activity {
            Activity::Exec => Reason::Exec,
            Activity::ContainerCreate => Reason::ContainerCreate,
        };
        let report = self
            .wake(&PodRef::sandbox(sandbox_id), WakeRequest::new(reason))
            .await?;
        Ok((guard, report))
    }

    pub fn put_task(&self, sandbox_id: &str, task: TaskRecord) -> Result<(), LifecycleError> {
        self.update(sandbox_id, |r| {
            r.tasks.insert(task.id.clone(), task);
        })
    }

    pub fn remove_task(&self, sandbox_id: &str, task_id: &str) -> Result<(), LifecycleError> {
        self.update(sandbox_id, |r| {
            r.tasks.remove(task_id);
        })
    }

    /// Records an exec. One that has not exited blocks a pause.
    pub fn put_exec(&self, sandbox_id: &str, exec: ExecRecord) -> Result<(), LifecycleError> {
        self.update(sandbox_id, |r| {
            r.execs.insert(exec.key(), exec);
        })
    }

    pub fn exec_exited(
        &self,
        sandbox_id: &str,
        container: &str,
        exec: &str,
        exit_status: u32,
    ) -> Result<(), LifecycleError> {
        self.update(sandbox_id, |r| {
            if let Some(e) = r.execs.get_mut(&exec_key(container, exec)) {
                e.status = ExecStatus::Exited;
                e.exit_status = Some(exit_status);
            }
        })
    }

    /// Records the exit of a task's process.
    pub fn task_exited(
        &self,
        sandbox_id: &str,
        task_id: &str,
        exit_status: u32,
    ) -> Result<(), LifecycleError> {
        self.update(sandbox_id, |r| {
            if let Some(t) = r.tasks.get_mut(task_id) {
                t.exit_status = Some(exit_status);
            }
        })
    }

    pub fn remove_exec(
        &self,
        sandbox_id: &str,
        container: &str,
        exec: &str,
    ) -> Result<(), LifecycleError> {
        self.update(sandbox_id, |r| {
            r.execs.remove(&exec_key(container, exec));
        })
    }

    /// Removes a task, or with `exec` an exec, that containerd deleted while
    /// the pod was not awake, and records in the same write that the guest
    /// still has it: the next wake deletes it there, also after a restart.
    pub fn remove_for_guest(
        &self,
        sandbox_id: &str,
        container: &str,
        exec: Option<&str>,
    ) -> Result<(), LifecycleError> {
        let delete = GuestDelete {
            container: container.to_string(),
            exec: exec.map(str::to_string),
        };
        self.update(sandbox_id, |r| {
            match exec {
                Some(exec) => {
                    r.execs.remove(&exec_key(container, exec));
                }
                None => {
                    r.tasks.remove(container);
                }
            }
            if !r.guest_deletes.contains(&delete) {
                r.guest_deletes.push(delete);
            }
        })
    }

    /// The guest deleted `done`: they leave the record.
    pub fn guest_deleted(
        &self,
        sandbox_id: &str,
        done: &[GuestDelete],
    ) -> Result<(), LifecycleError> {
        self.update(sandbox_id, |r| {
            r.guest_deletes.retain(|d| !done.contains(d))
        })
    }

    /// Records whether the pod's VM has the Rosetta share, which makes every
    /// resume check the TSO marker.
    pub fn set_rosetta(&self, sandbox_id: &str, rosetta: bool) -> Result<(), LifecycleError> {
        self.update(sandbox_id, |r| r.identity.rosetta = rosetta)
    }

    /// Records the pod's IP and the guest's MAC, for lookups and the catcher.
    pub fn set_network(
        &self,
        sandbox_id: &str,
        ip: Option<String>,
        mac: Option<String>,
    ) -> Result<(), LifecycleError> {
        self.update(sandbox_id, |r| {
            r.identity.ip = ip;
            r.identity.mac = mac;
        })
    }

    /// Records the pod's labels and controller, from the pod watch.
    pub fn set_labels(
        &self,
        sandbox_id: &str,
        labels: Labels,
        controller: Option<Controller>,
    ) -> Result<(), LifecycleError> {
        self.update(sandbox_id, |r| {
            r.identity.labels = labels;
            r.identity.controller = controller;
        })
    }

    /// Records an event that changes nothing, from `state` to `state`, with
    /// `error` saying why the pod stays there, such as an idle pod that does
    /// not sleep because of its probes. Returns false, with no event,
    /// unless the pod is in `state`.
    pub fn note(
        &self,
        sandbox_id: &str,
        state: PodState,
        reason: Reason,
        error: String,
    ) -> Result<bool, LifecycleError> {
        let pod = self.resolve(&PodRef::sandbox(sandbox_id), None)?;
        Ok(pod.note(&self.shared, state, reason, error))
    }

    /// Records the pod's probes and their last results, which survive a
    /// restart.
    pub fn set_probes(
        &self,
        sandbox_id: &str,
        probes: Option<PodProbes>,
    ) -> Result<(), LifecycleError> {
        self.update(sandbox_id, |r| r.probes = probes)
    }

    fn update(
        &self,
        sandbox_id: &str,
        edit: impl FnOnce(&mut PodRecord),
    ) -> Result<(), LifecycleError> {
        let pod = self.resolve(&PodRef::sandbox(sandbox_id), None)?;
        pod.commit(&self.shared, edit)
            .map_err(LifecycleError::Store)
    }

    fn find(&self, pod: &PodRef) -> Option<Arc<Pod>> {
        let pods = read(&self.shared.pods);
        match pod {
            PodRef::Sandbox(id) => pods.get(id).cloned(),
            PodRef::Ip(ip) => pods
                .values()
                .filter_map(|p| {
                    let record = p.record();
                    let key = (record.state.is_live(), record.created_ms, &p.id);
                    (record.identity.ip.as_deref() == Some(ip.as_str())).then_some((key, p))
                })
                .max_by_key(|(key, _)| *key)
                .map(|(_, p)| p.clone()),
        }
    }

    fn resolve(&self, pod: &PodRef, incarnation: Option<&str>) -> Result<Arc<Pod>, LifecycleError> {
        let found = self
            .find(pod)
            .ok_or_else(|| LifecycleError::NotFound(pod.to_string()))?;
        match incarnation {
            Some(incarnation) if found.incarnation() != incarnation => {
                Err(LifecycleError::StaleIncarnation(found.id.clone()))
            }
            _ => Ok(found),
        }
    }
}

#[cfg(test)]
mod mock;
#[cfg(test)]
mod tests;
