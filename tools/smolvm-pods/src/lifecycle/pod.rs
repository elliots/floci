//! The actor of one pod. It runs the pod's transitions one at a time, and
//! decides what a command that arrives during a transition does: a wake
//! merges into it, or cancels a pause; a hibernate during a pause waits for
//! its outcome; a deferrable kill during a pause is recorded at once; every
//! other command waits until the transition ends.

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use log::{info, warn};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinError, JoinHandle};

use super::events::{ms, Event, Steps};
use super::fault::FaultPoint;
use super::record::{random_id, DeferredKill, ExecStatus, Lease, PodRecord, Timing};
use super::recovery::{classify, Plan};
use super::state::{Blocker, PausePhase, PodState, Reason};
use super::vm::{AwakeCause, ClockStep, Hooks, PauseControl, PauseOutcome, VmProcess, VmView};
use super::{
    blocking, Activity, KillOutcome, KillRequest, LeaseInfo, LifecycleError, PodInfo, Shared,
    SleepReport, VmExit, WakeReport,
};
use crate::sync::{lock, write};

const SIGKILL: u32 = 9;
const SIGTERM: u32 = 15;

pub(super) type Boot = Pin<Box<dyn Future<Output = Result<Option<String>, String>> + Send>>;

type Reply<T> = oneshot::Sender<Result<T, LifecycleError>>;

pub(super) struct WakeWaiter {
    pub reply: Reply<WakeReport>,
    pub reason: Reason,
    pub source: Option<String>,
}

pub(super) enum Command {
    Recover {
        done: oneshot::Sender<()>,
    },
    /// The mutex makes the command `Sync`, as the actor's `&self` futures
    /// need; it is never contended.
    Create {
        boot: Mutex<Boot>,
        reply: Reply<PodInfo>,
    },
    Wake(WakeWaiter),
    Hibernate {
        reason: Reason,
        source: Option<String>,
        reply: Reply<SleepReport>,
    },
    Kill {
        request: KillRequest,
        reply: Reply<KillOutcome>,
    },
    VmExited {
        process: VmProcess,
        reply: oneshot::Sender<VmExit>,
    },
    Delete {
        reply: Reply<()>,
    },
}

/// The implicit leases that live only in memory. Execs that were recorded
/// are in the record.
#[derive(Default)]
struct Activities {
    execs: usize,
    creates: usize,
    waiters: usize,
    grace_until_ms: u64,
}

/// A pod's shared state. Locks are taken in the order record, then pause or
/// activity, and never held across an await.
pub(super) struct Pod {
    pub id: String,
    record: Mutex<PodRecord>,
    activity: Mutex<Activities>,
    pause: Mutex<Option<Arc<PauseControl>>>,
    commands: mpsc::UnboundedSender<Command>,
}

pub(super) struct WaiterGuard(Arc<Pod>);

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        let mut activity = lock(&self.0.activity);
        activity.waiters = activity.waiters.saturating_sub(1);
    }
}

impl Pod {
    /// Starts the pod's actor.
    pub fn start(shared: &Arc<Shared>, record: PodRecord) -> Arc<Pod> {
        let (commands, rx) = mpsc::unbounded_channel();
        let pod = Arc::new(Pod {
            id: record.identity.sandbox_id.clone(),
            record: Mutex::new(record),
            activity: Mutex::default(),
            pause: Mutex::new(None),
            commands,
        });
        let actor = Actor {
            shared: shared.clone(),
            pod: pod.clone(),
            rx,
            backlog: VecDeque::new(),
            gone: false,
        };
        shared.spawn(actor.run());
        pod
    }

    pub fn send(&self, command: Command) -> Result<(), LifecycleError> {
        self.commands
            .send(command)
            .map_err(|_| LifecycleError::Gone)
    }

    pub fn record(&self) -> PodRecord {
        lock(&self.record).clone()
    }

    pub fn state(&self) -> PodState {
        lock(&self.record).state
    }

    pub fn incarnation(&self) -> String {
        lock(&self.record).identity.incarnation.clone()
    }

    /// Applies `edit` to a copy of the record and writes the copy, which
    /// replaces the record in memory only once it is durable.
    pub fn commit<T>(
        &self,
        shared: &Shared,
        edit: impl FnOnce(&mut PodRecord) -> T,
    ) -> Result<T, String> {
        let mut record = lock(&self.record);
        let mut next = record.clone();
        let out = edit(&mut next);
        if next != *record {
            shared.store.save(&next)?;
            *record = next;
        }
        Ok(out)
    }

    pub fn info(&self, now_ms: u64) -> PodInfo {
        let record = lock(&self.record);
        let phase = match record.state {
            PodState::Pausing => lock(&self.pause).as_ref().map(|c| {
                if c.is_durable() {
                    PausePhase::Stopping
                } else {
                    PausePhase::Saving
                }
            }),
            _ => None,
        };
        let leases = record
            .leases
            .iter()
            .filter(|(_, l)| l.expires_ms > now_ms)
            .map(|(id, l)| LeaseInfo {
                id: id.clone(),
                holder: l.holder.clone(),
                expires_ms: l.expires_ms,
            })
            .collect();
        let execs = record
            .execs
            .iter()
            .filter(|(_, e)| e.status != ExecStatus::Exited)
            .map(|(key, _)| key.clone())
            .collect();
        let activity = lock(&self.activity);
        PodInfo {
            identity: record.identity.clone(),
            state: record.state,
            phase,
            generation: record.generation,
            created_ms: record.created_ms,
            since_ms: record.since_ms,
            leases,
            waiters: activity.waiters,
            starting_execs: activity.execs,
            creates: activity.creates,
            execs,
            last_wake: record.last_wake,
            last_sleep: record.last_sleep,
            checkpoint_bytes: record.checkpoint_bytes,
            wake_failures: record.wake_failures,
            failure: record.failure.clone(),
            vm: record.vm,
        }
    }

    /// Emits an event that changes nothing: why the pod stays in `state`.
    /// Nothing happens unless the pod is in `state`.
    pub fn note(&self, shared: &Shared, state: PodState, reason: Reason, error: String) -> bool {
        let record = self.record();
        if record.state != state {
            return false;
        }
        let mut t = Transition::new(reason);
        t.error = Some(error);
        let now = shared.now_ms();
        shared.emit(event_of(&record, Some(state), &t, now));
        true
    }

    /// Emits the event of a new record.
    pub fn created(&self, shared: &Shared) {
        let record = self.record();
        let now = shared.now_ms();
        shared.emit(event_of(&record, None, &Transition::new(Reason::Api), now));
    }

    pub fn hold(
        &self,
        shared: &Shared,
        id: Option<String>,
        ttl_secs: Option<f64>,
        holder: Option<String>,
    ) -> Result<LeaseInfo, LifecycleError> {
        if id
            .as_deref()
            .is_some_and(|id| id.is_empty() || id.len() > 128)
        {
            return Err(LifecycleError::Invalid(
                "a lease id has 1 to 128 characters".into(),
            ));
        }
        let expires_ms = shared.now_ms() + lease_ttl_ms(ttl_secs)?;
        let fresh = random_id();
        let id = self
            .commit(shared, |r| {
                let held = holder.as_ref().and_then(|h| {
                    r.leases
                        .iter()
                        .find(|(_, l)| l.holder.as_ref() == Some(h))
                        .map(|(id, _)| id.clone())
                });
                let id = id.or(held).unwrap_or(fresh);
                r.leases.insert(
                    id.clone(),
                    Lease {
                        expires_ms,
                        holder: holder.clone(),
                    },
                );
                id
            })
            .map_err(LifecycleError::Store)?;
        Ok(LeaseInfo {
            id,
            holder,
            expires_ms,
        })
    }

    pub fn renew(
        &self,
        shared: &Shared,
        lease: &str,
        ttl_secs: Option<f64>,
    ) -> Result<LeaseInfo, LifecycleError> {
        let now = shared.now_ms();
        let expires_ms = now + lease_ttl_ms(ttl_secs)?;
        let holder = self
            .commit(shared, |r| match r.leases.get_mut(lease) {
                Some(l) if l.expires_ms > now => {
                    l.expires_ms = expires_ms;
                    Some(l.holder.clone())
                }
                _ => None,
            })
            .map_err(LifecycleError::Store)?
            .ok_or_else(|| LifecycleError::NotFound(format!("lease {lease}")))?;
        Ok(LeaseInfo {
            id: lease.to_string(),
            holder,
            expires_ms,
        })
    }

    pub fn release(&self, shared: &Shared, lease: &str) -> Result<(), LifecycleError> {
        self.commit(shared, |r| {
            r.leases.remove(lease);
        })
        .map_err(LifecycleError::Store)
    }

    /// What blocks a pause now. Expired leases are dropped from the record.
    /// The grace time after a wake holds off only a pause for being idle:
    /// a client that asks for one means it.
    pub fn blockers(&self, shared: &Shared, idle: bool) -> Result<Vec<Blocker>, String> {
        let now = shared.now_ms();
        self.commit(shared, |r| r.leases.retain(|_, l| l.expires_ms > now))?;
        let record = self.record();
        let mut blockers = Vec::new();
        if record.identity.hibernate_disabled {
            blockers.push(Blocker::Disabled);
        }
        for (id, lease) in &record.leases {
            blockers.push(Blocker::Lease {
                id: id.clone(),
                holder: lease.holder.clone(),
                expires_ms: lease.expires_ms,
            });
        }
        for exec in record.execs.values() {
            if exec.status != ExecStatus::Exited {
                blockers.push(Blocker::Exec {
                    container: exec.container.clone(),
                    exec: exec.exec.clone(),
                    status: exec.status,
                });
            }
        }
        let activity = lock(&self.activity);
        if activity.execs > 0 {
            blockers.push(Blocker::ExecStarting);
        }
        if activity.creates > 0 {
            blockers.push(Blocker::ContainerCreating);
        }
        if activity.waiters > 0 || (idle && now < activity.grace_until_ms) {
            blockers.push(Blocker::WakeWaiter {
                until_ms: activity.grace_until_ms,
            });
        }
        Ok(blockers)
    }

    pub fn waiter(self: &Arc<Self>) -> WaiterGuard {
        lock(&self.activity).waiters += 1;
        WaiterGuard(self.clone())
    }

    pub fn begin(&self, activity: Activity) {
        let mut a = lock(&self.activity);
        match activity {
            Activity::Exec => a.execs += 1,
            Activity::ContainerCreate => a.creates += 1,
        }
    }

    pub fn end(&self, activity: Activity) {
        let mut a = lock(&self.activity);
        match activity {
            Activity::Exec => a.execs = a.execs.saturating_sub(1),
            Activity::ContainerCreate => a.creates = a.creates.saturating_sub(1),
        }
    }
}

fn lease_ttl_ms(ttl_secs: Option<f64>) -> Result<u64, LifecycleError> {
    let ttl = ttl_secs.unwrap_or(30.0);
    let valid = ttl > 0.0 && ttl <= 60.0;
    if !valid {
        return Err(LifecycleError::Invalid(
            "lease TTL must be between 0 and 60 seconds".into(),
        ));
    }
    Ok((ttl * 1000.0).ceil() as u64)
}

/// What an event says about a transition, gathered as it runs.
#[derive(Clone)]
struct Transition {
    reason: Reason,
    source: Option<String>,
    steps: Steps,
    duration_ms: Option<f64>,
    checkpoint_bytes: Option<u64>,
    clock: Option<ClockStep>,
    error: Option<String>,
    warning: Option<String>,
    cancelled: bool,
    capture_to_release_ms: Option<f64>,
    recovered: bool,
}

impl Transition {
    fn new(reason: Reason) -> Transition {
        Transition {
            reason,
            source: None,
            steps: Steps::default(),
            duration_ms: None,
            checkpoint_bytes: None,
            clock: None,
            error: None,
            warning: None,
            cancelled: false,
            capture_to_release_ms: None,
            recovered: false,
        }
    }
}

fn event_of(record: &PodRecord, from: Option<PodState>, t: &Transition, now_ms: u64) -> Event {
    Event {
        seq: 0,
        time_ms: now_ms,
        sandbox_id: record.identity.sandbox_id.clone(),
        incarnation: record.identity.incarnation.clone(),
        from,
        state: record.state,
        generation: record.generation,
        reason: t.reason.clone(),
        source: t.source.clone(),
        steps: t.steps.0.clone(),
        duration_ms: t.duration_ms,
        checkpoint_bytes: t.checkpoint_bytes,
        clock: t.clock,
        error: t.error.clone(),
        warning: t.warning.clone(),
        cancelled: t.cancelled,
        capture_to_release_ms: t.capture_to_release_ms,
        recovered: t.recovered,
    }
}

/// The waiters a transition collects.
struct During {
    state: PodState,
    pause: Option<Arc<PauseControl>>,
    wakes: Vec<WakeWaiter>,
    sleeps: Vec<Reply<SleepReport>>,
    /// The answer to `sleeps` once a pause that did not happen is undone.
    refusal: Option<LifecycleError>,
    /// The first of `wakes` started the resume; the others merged into it.
    first_wakes: bool,
}

impl During {
    fn new(state: PodState) -> During {
        During {
            state,
            pause: None,
            wakes: Vec::new(),
            sleeps: Vec::new(),
            refusal: None,
            first_wakes: false,
        }
    }

    /// Answers every wake. Only the one that started the resume says that it
    /// resumed the pod.
    fn answer_wakes(&mut self, result: Result<WakeReport, LifecycleError>) {
        let mut result = result;
        if !self.first_wakes {
            result = result.map(|report| WakeReport {
                resumed: false,
                ..report
            });
        }
        for waiter in self.wakes.drain(..) {
            let _ = waiter.reply.send(result.clone());
            result = result.map(|report| WakeReport {
                resumed: false,
                ..report
            });
        }
    }

    fn answer_sleeps(&mut self, result: Result<SleepReport, LifecycleError>) {
        for reply in self.sleeps.drain(..) {
            let _ = reply.send(result.clone());
        }
    }

    fn fail_all(&mut self, error: LifecycleError) {
        self.answer_wakes(Err(error.clone()));
        self.answer_sleeps(Err(error));
    }
}

/// A spawned task that is aborted when dropped, so that a shut down actor
/// leaves no transition running.
struct Abortable<T>(JoinHandle<T>);

impl<T> Drop for Abortable<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> Future for Abortable<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

/// Why a pause did not happen.
enum PauseError {
    /// The `pausing` hook refused the pause, with this reason.
    Refused(String),
    Failed(String),
}

impl fmt::Display for PauseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PauseError::Refused(why) => {
                fmt::Display::fmt(&LifecycleError::PauseRefused(why.clone()), f)
            }
            PauseError::Failed(why) => f.write_str(why),
        }
    }
}

struct PostResume {
    steps: Steps,
    clock: ClockStep,
    process: VmProcess,
    warning: Option<String>,
    /// From the release hook: how long the first held frame waited.
    held: Option<Duration>,
}

struct Actor {
    shared: Arc<Shared>,
    pod: Arc<Pod>,
    rx: mpsc::UnboundedReceiver<Command>,
    backlog: VecDeque<Command>,
    gone: bool,
}

impl Actor {
    async fn run(mut self) {
        while !self.gone {
            let command = match self.backlog.pop_front() {
                Some(command) => command,
                None => match self.rx.recv().await {
                    Some(command) => command,
                    None => return,
                },
            };
            self.handle(command).await;
        }
    }

    async fn handle(&mut self, command: Command) {
        let transient = matches!(
            self.pod.state(),
            PodState::Creating | PodState::Pausing | PodState::Waking
        );
        if transient && !matches!(command, Command::Recover { .. } | Command::Create { .. }) {
            // A transition stopped half way, for example on a failed write.
            self.settle(false).await;
            if self.gone {
                return;
            }
        }
        match command {
            Command::Recover { done } => {
                self.settle(true).await;
                let _ = done.send(());
            }
            Command::Create { boot, reply } => {
                let boot = boot.into_inner().unwrap_or_else(|e| e.into_inner());
                self.create(boot, reply).await
            }
            Command::Wake(waiter) => match self.pod.state() {
                PodState::Awake => {
                    let _ = waiter.reply.send(Ok(self.awake_report()));
                }
                PodState::Asleep => {
                    let (reason, source) = (waiter.reason.clone(), waiter.source.clone());
                    self.wake(reason, source, vec![waiter]).await;
                }
                state => {
                    let _ = waiter.reply.send(Err(self.unavailable(state, "wake")));
                }
            },
            Command::Hibernate {
                reason,
                source,
                reply,
            } => self.hibernate(reason, source, reply).await,
            Command::Kill { request, reply } => {
                let result = self.kill(request).await;
                let _ = reply.send(result);
            }
            Command::VmExited { process, reply } => {
                let exit = self.vm_exited(process).await;
                let _ = reply.send(exit);
            }
            Command::Delete { reply } => {
                let result = self.delete(Transition::new(Reason::Api)).await;
                let _ = reply.send(result);
            }
        }
    }

    /// Runs `work` as its own task, handling commands until it ends.
    async fn drive<T: Send + 'static>(
        &mut self,
        during: &mut During,
        work: impl Future<Output = T> + Send + 'static,
    ) -> Result<T, String> {
        let mut work = Abortable(self.shared.spawn(work));
        loop {
            tokio::select! {
                biased;
                command = self.rx.recv() => match command {
                    Some(command) => self.during(during, command),
                    None => {
                        return (&mut work)
                            .await
                            .map_err(|e| format!("a transition task ended: {e}"))
                    }
                },
                done = &mut work => {
                    return done.map_err(|e| format!("a transition task ended: {e}"))
                }
            }
        }
    }

    fn during(&mut self, during: &mut During, command: Command) {
        match command {
            Command::Wake(waiter) => {
                if let Some(control) = &during.pause {
                    if control.cancel() {
                        info!("a wake cancels the pause of pod sandbox {}", self.pod.id);
                    }
                }
                during.wakes.push(waiter);
            }
            Command::Hibernate { reply, .. } => match during.state {
                PodState::Pausing => during.sleeps.push(reply),
                PodState::Waking => {
                    let _ = reply.send(Err(LifecycleError::Refused(vec![Blocker::Waking])));
                }
                state => {
                    let _ = reply.send(Err(LifecycleError::State {
                        state,
                        op: "hibernate",
                    }));
                }
            },
            Command::Kill { request, reply }
                if during.state == PodState::Pausing && self.deferrable(request.signal) =>
            {
                let _ = reply.send(self.defer(&request));
            }
            command => self.backlog.push_back(command),
        }
    }

    fn info(&self) -> PodInfo {
        self.pod.info(self.shared.now_ms())
    }

    fn machine(&self) -> String {
        lock(&self.pod.record).identity.machine.clone()
    }

    async fn inspect(&self) -> Result<VmView, String> {
        let machine = self.machine();
        self.shared.vm_call(move |vm| vm.inspect(&machine)).await
    }

    /// Whether `process` runs. An error counts as alive, which is the
    /// cautious answer for a process that must be gone.
    async fn alive(&self, process: VmProcess) -> bool {
        self.shared
            .vm_call(move |vm| Ok(vm.is_alive(&process)))
            .await
            .unwrap_or(true)
    }

    async fn plan_for(&self, state: PodState) -> Plan {
        match self.inspect().await {
            Ok(view) => classify(state, &view),
            Err(e) => Plan::Fail(format!("inspect the VM: {e}")),
        }
    }

    async fn hook(&self, call: impl FnOnce(&dyn Hooks, &PodInfo) + Send + 'static) {
        let hooks = self.shared.hooks.clone();
        let info = self.info();
        let called = blocking(move || {
            call(hooks.as_ref(), &info);
            Ok(())
        })
        .await;
        if let Err(e) = called {
            warn!("a hook of pod sandbox {} failed: {e}", self.pod.id);
        }
    }

    /// Writes the record in its new state, then the event.
    fn transition(
        &self,
        to: PodState,
        t: &Transition,
        edit: impl FnOnce(&mut PodRecord),
    ) -> Result<PodRecord, String> {
        let now = self.shared.now_ms();
        let reason = t.reason.clone();
        let (from, record) = self.pod.commit(&self.shared, |r| {
            let from = r.state;
            r.state = to;
            r.generation += 1;
            r.since_ms = now;
            r.reason = Some(reason);
            edit(r);
            (from, r.clone())
        })?;
        self.shared.emit(event_of(&record, Some(from), t, now));
        Ok(record)
    }

    fn unavailable(&self, state: PodState, op: &'static str) -> LifecycleError {
        match state {
            PodState::Failed => {
                LifecycleError::Failed(lock(&self.pod.record).failure.clone().unwrap_or_default())
            }
            state => LifecycleError::State { state, op },
        }
    }

    fn awake_report(&self) -> WakeReport {
        WakeReport {
            generation: lock(&self.pod.record).generation,
            resumed: false,
            cancelled_pause: false,
            steps: Vec::new(),
            duration_ms: 0.0,
            clock: None,
            lease: None,
        }
    }

    async fn create(&mut self, boot: Boot, reply: Reply<PodInfo>) {
        let started = Instant::now();
        let mut during = During::new(PodState::Creating);
        let mut t = Transition::new(Reason::Api);
        let booted = self.drive(&mut during, boot).await.and_then(|r| r);
        t.steps.push("boot", started.elapsed());
        let process = match booted {
            Ok(warning) => {
                t.warning = warning;
                self.inspect().await.and_then(|view| {
                    view.running()
                        .ok_or_else(|| "the VM is not running after the boot".to_string())
                })
            }
            Err(e) => Err(e),
        };
        let process = match process {
            Ok(process) => process,
            Err(e) => {
                warn!("the boot of pod sandbox {} failed: {e}", self.pod.id);
                t.error = Some(e.clone());
                self.drop_record(&mut t).await;
                let error = LifecycleError::Vm(e);
                let _ = reply.send(Err(error.clone()));
                during.fail_all(error);
                return;
            }
        };
        t.duration_ms = Some(ms(started.elapsed()));
        if let Err(e) = self.transition(PodState::Awake, &t, |r| r.vm = Some(process)) {
            let error = LifecycleError::Store(e);
            let _ = reply.send(Err(error.clone()));
            during.fail_all(error);
            return;
        }
        self.hook(|h, info| h.awake(info, AwakeCause::Created))
            .await;
        let _ = reply.send(Ok(self.info()));
        during.answer_wakes(Ok(self.awake_report()));
    }

    /// Deletes the machine and drops the record, for a boot that failed.
    async fn drop_record(&mut self, t: &mut Transition) {
        let started = Instant::now();
        let machine = self.machine();
        if let Err(e) = self.shared.vm_call(move |vm| vm.delete(&machine)).await {
            warn!("delete the machine of pod sandbox {}: {e}", self.pod.id);
        }
        t.steps.push("delete", started.elapsed());
        self.hook(|h, info| h.deleted(info)).await;
        if let Err(e) = self.remove(t) {
            warn!("remove the record of pod sandbox {}: {e}", self.pod.id);
        }
    }

    /// Removes the record and ends the actor.
    fn remove(&mut self, t: &Transition) -> Result<(), String> {
        self.shared.store.remove(&self.pod.id)?;
        let now = self.shared.now_ms();
        let (from, record) = {
            let mut record = lock(&self.pod.record);
            let from = record.state;
            record.state = PodState::Deleted;
            record.generation += 1;
            (from, record.clone())
        };
        {
            let mut pods = write(&self.shared.pods);
            if pods
                .get(&self.pod.id)
                .is_some_and(|p| Arc::ptr_eq(p, &self.pod))
            {
                pods.remove(&self.pod.id);
            }
        }
        self.shared.emit(event_of(&record, Some(from), t, now));
        self.gone = true;
        Ok(())
    }

    async fn hibernate(
        &mut self,
        reason: Reason,
        source: Option<String>,
        reply: Reply<SleepReport>,
    ) {
        match self.pod.state() {
            PodState::Awake => {}
            PodState::Asleep => {
                let record = self.pod.record();
                let _ = reply.send(Ok(SleepReport {
                    generation: record.generation,
                    already_asleep: true,
                    steps: Vec::new(),
                    duration_ms: 0.0,
                    checkpoint_bytes: record.checkpoint_bytes,
                }));
                return;
            }
            state => {
                let _ = reply.send(Err(self.unavailable(state, "hibernate")));
                return;
            }
        }
        match self.pod.blockers(&self.shared, reason == Reason::Idle) {
            Ok(blockers) if blockers.is_empty() => {}
            Ok(blockers) => {
                let _ = reply.send(Err(LifecycleError::Refused(blockers)));
                return;
            }
            Err(e) => {
                let _ = reply.send(Err(LifecycleError::Store(e)));
                return;
            }
        }
        let paused = match self.inspect().await.map(|view| view.running()) {
            Ok(Some(process)) => process,
            Ok(None) => {
                let _ = reply.send(Err(LifecycleError::Vm(format!(
                    "smolvm reports no running VM process for {}",
                    self.machine()
                ))));
                return;
            }
            Err(e) => {
                let _ = reply.send(Err(LifecycleError::Vm(e)));
                return;
            }
        };
        let started = Instant::now();
        let mut t = Transition::new(reason.clone());
        t.source = source;
        let faults = self.shared.config.faults.for_pod(&self.pod.id);
        let control = Arc::new(PauseControl::new(faults));
        *lock(&self.pod.pause) = Some(control.clone());
        if let Err(e) = self.transition(PodState::Pausing, &t, |_| {}) {
            *lock(&self.pod.pause) = None;
            let _ = reply.send(Err(LifecycleError::Store(e)));
            return;
        }
        let mut during = During::new(PodState::Pausing);
        during.pause = Some(control.clone());
        during.sleeps.push(reply);
        let work = pause(self.shared.clone(), self.info(), reason, control);
        let done = self.drive(&mut during, work).await;
        *lock(&self.pod.pause) = None;
        let (outcome, steps) =
            done.unwrap_or_else(|e| (Err(PauseError::Failed(e)), Steps::default()));
        t.steps = steps;
        self.after_pause(t, started, paused, outcome, during).await;
    }

    /// Settles a pause by smolvm's view, whatever the pause returned. After a
    /// durable pause, the VM process that was paused must be gone.
    async fn after_pause(
        &mut self,
        mut t: Transition,
        started: Instant,
        paused: VmProcess,
        outcome: Result<PauseOutcome, PauseError>,
        mut during: During,
    ) {
        let cancelled = matches!(outcome, Ok(PauseOutcome::Cancelled));
        let bytes = match &outcome {
            Ok(PauseOutcome::Paused { checkpoint_bytes }) => *checkpoint_bytes,
            _ => None,
        };
        if let Err(e) = &outcome {
            warn!("the pause of pod sandbox {} failed: {e}", self.pod.id);
            t.error = Some(e.to_string());
        }
        let mut plan = self.plan_for(PodState::Pausing).await;
        if plan == Plan::Asleep && self.alive(paused).await {
            plan = Plan::StopPaused;
        }
        if plan == Plan::StopPaused {
            plan = self.stop_paused(&mut t, Some(paused)).await;
        }
        match plan {
            Plan::Asleep => self.fall_asleep(t, started, bytes, during).await,
            Plan::FinishWake | Plan::ResumeInPlace => {
                during.refusal = Some(match outcome {
                    Ok(PauseOutcome::Cancelled) => LifecycleError::Cancelled,
                    Err(PauseError::Refused(why)) => LifecycleError::PauseRefused(why),
                    _ => LifecycleError::Vm(
                        t.error
                            .clone()
                            .unwrap_or_else(|| "the pause did not happen".into()),
                    ),
                });
                if plan == Plan::ResumeInPlace {
                    if let Err(e) = self.resume_in_place(&mut t).await {
                        return self.fail(t, e, during).await;
                    }
                }
                let cause = if cancelled {
                    AwakeCause::PauseCancelled
                } else {
                    AwakeCause::PauseFailed
                };
                self.finish_wake(t, started, during, cause, false).await;
            }
            Plan::Fail(why) => self.fail(t, why, during).await,
            other => {
                let why = format!("no way to settle a pause from {other:?}");
                self.fail(t, why, during).await
            }
        }
    }

    /// Stops the VM process of a durable pause and checks that the pod is
    /// asleep.
    async fn stop_paused(&self, t: &mut Transition, process: Option<VmProcess>) -> Plan {
        let started = Instant::now();
        let machine = self.machine();
        if let Err(e) = self
            .shared
            .vm_call(move |vm| vm.stop_paused(&machine))
            .await
        {
            warn!("stop the paused VM of pod sandbox {}: {e}", self.pod.id);
            t.error.get_or_insert(e);
        }
        t.steps.push("stop-paused", started.elapsed());
        if let Some(process) = process {
            if self.alive(process).await {
                return Plan::Fail("the VM process survived the pause".into());
            }
        }
        match self.plan_for(PodState::Pausing).await {
            Plan::StopPaused => Plan::Fail("the paused VM process did not stop".into()),
            plan => plan,
        }
    }

    async fn fall_asleep(
        &mut self,
        mut t: Transition,
        started: Instant,
        bytes: Option<u64>,
        mut during: During,
    ) {
        let duration = ms(started.elapsed());
        let bytes = match bytes {
            Some(bytes) => Some(bytes),
            None => self.inspect().await.ok().and_then(|v| v.checkpoint_bytes),
        };
        t.duration_ms = Some(duration);
        t.checkpoint_bytes = bytes;
        let now = self.shared.now_ms();
        let record = match self.transition(PodState::Asleep, &t, |r| {
            r.vm = None;
            r.last_sleep = Some(Timing {
                at_ms: now,
                ms: duration,
            });
            r.checkpoint_bytes = bytes;
        }) {
            Ok(record) => record,
            Err(e) => return during.fail_all(LifecycleError::Store(e)),
        };
        self.hook(|h, info| h.asleep(info)).await;
        during.answer_sleeps(Ok(SleepReport {
            generation: record.generation,
            already_asleep: false,
            steps: t.steps.0.clone(),
            duration_ms: duration,
            checkpoint_bytes: bytes,
        }));
        // A wake that came after the pause was durable resumes now.
        if let Some(first) = during.wakes.first() {
            let (reason, source) = (first.reason.clone(), first.source.clone());
            let waiters = std::mem::take(&mut during.wakes);
            self.wake(reason, source, waiters).await;
        }
    }

    /// Admits the wake, resumes the VM, then runs the post-resume steps.
    async fn wake(&mut self, reason: Reason, source: Option<String>, waiters: Vec<WakeWaiter>) {
        let started = Instant::now();
        let mut t = Transition::new(reason);
        t.source = source;
        let mut during = During::new(PodState::Waking);
        during.first_wakes = !waiters.is_empty();
        during.wakes = waiters;
        let hooks = self.shared.hooks.clone();
        let info = self.info();
        let admitted = blocking(move || hooks.may_wake(&info)).await;
        t.steps.push("admit", started.elapsed());
        if let Err(reason) = admitted {
            let refusal = Blocker::Admission { reason };
            info!("pod sandbox {}: {refusal}", self.pod.id);
            t.error = Some(refusal.to_string());
            t.duration_ms = Some(ms(started.elapsed()));
            let record = self.pod.record();
            let now = self.shared.now_ms();
            self.shared
                .emit(event_of(&record, Some(record.state), &t, now));
            during.answer_wakes(Err(LifecycleError::Refused(vec![refusal])));
            return;
        }
        if let Err(e) = self.transition(PodState::Waking, &t, |_| {}) {
            return during.fail_all(LifecycleError::Store(e));
        }
        self.resume_and_finish(t, started, during).await;
    }

    async fn resume_and_finish(&mut self, mut t: Transition, started: Instant, mut during: During) {
        let work = resume(self.shared.clone(), self.pod.id.clone(), self.machine());
        let resumed = self.drive(&mut during, work).await.and_then(|r| r);
        let e = match resumed {
            Ok(steps) => {
                t.steps.extend(steps);
                return self
                    .finish_wake(t, started, during, AwakeCause::Woke, true)
                    .await;
            }
            Err(e) => e,
        };
        warn!("the resume of pod sandbox {} failed: {e}", self.pod.id);
        t.error = Some(e.clone());
        match self.plan_for(PodState::Waking).await {
            Plan::Rewake => self.wake_failed(t, e, during).await,
            Plan::FinishWake => {
                self.finish_wake(t, started, during, AwakeCause::Woke, true)
                    .await
            }
            Plan::Fail(why) => self.fail(t, format!("{why}: {e}"), during).await,
            other => {
                let why = format!("no way to settle a resume from {other:?}: {e}");
                self.fail(t, why, during).await
            }
        }
    }

    /// The resume failed before the VM started, and the checkpoint is intact.
    async fn wake_failed(&mut self, t: Transition, error: String, mut during: During) {
        let failures = lock(&self.pod.record).wake_failures + 1;
        if failures >= self.shared.config.max_wake_failures {
            let why = format!("the resume failed {failures} times in a row: {error}");
            return self.fail(t, why, during).await;
        }
        let why = error.clone();
        let asleep = self.transition(PodState::Asleep, &t, |r| {
            r.wake_failures = failures;
            r.failure = Some(why);
        });
        if let Err(e) = asleep {
            return during.fail_all(LifecycleError::Store(e));
        }
        self.hook(|h, info| h.asleep(info)).await;
        during.answer_wakes(Err(LifecycleError::Vm(error)));
    }

    /// The post-resume steps, which every way back to awake runs.
    async fn finish_wake(
        &mut self,
        mut t: Transition,
        started: Instant,
        mut during: During,
        cause: AwakeCause,
        resumed: bool,
    ) {
        during.state = PodState::Waking;
        during.pause = None;
        let work = post_resume(self.shared.clone(), self.pod.clone());
        let post = match self.drive(&mut during, work).await.and_then(|r| r) {
            Ok(post) => post,
            Err(e) => return self.fail(t, e, during).await,
        };
        t.steps.extend(post.steps);
        t.clock = Some(post.clock);
        t.capture_to_release_ms = post.held.map(ms);
        t.cancelled = cause == AwakeCause::PauseCancelled;
        if t.error.is_none() {
            t.error = post.warning;
        }
        let duration = ms(started.elapsed());
        t.duration_ms = Some(duration);
        let now = self.shared.now_ms();
        let record = match self.transition(PodState::Awake, &t, |r| {
            r.vm = Some(post.process);
            r.wake_failures = 0;
            r.failure = None;
            if resumed {
                r.last_wake = Some(Timing {
                    at_ms: now,
                    ms: duration,
                });
            }
        }) {
            Ok(record) => record,
            Err(e) => return during.fail_all(LifecycleError::Store(e)),
        };
        if !during.wakes.is_empty() {
            let grace = self.shared.config.wake_grace.as_millis() as u64;
            lock(&self.pod.activity).grace_until_ms = now + grace;
        }
        self.hook(move |h, info| h.awake(info, cause)).await;
        during.answer_wakes(Ok(WakeReport {
            generation: record.generation,
            resumed,
            cancelled_pause: cause == AwakeCause::PauseCancelled,
            steps: t.steps.0.clone(),
            duration_ms: duration,
            clock: t.clock,
            lease: None,
        }));
        if let Some(refusal) = during.refusal.take() {
            during.answer_sleeps(Err(refusal));
        }
    }

    async fn resume_in_place(&self, t: &mut Transition) -> Result<(), String> {
        let started = Instant::now();
        let machine = self.machine();
        let resumed = self.shared.vm_call(move |vm| vm.resume(&machine)).await;
        t.steps.push("resume", started.elapsed());
        resumed
    }

    /// The VM is lost: stop it if it still runs, record the failure, and let
    /// the hook report exit 255 for every task.
    async fn fail(&mut self, mut t: Transition, why: String, mut during: During) {
        warn!("pod sandbox {} failed: {why}", self.pod.id);
        if self.inspect().await.is_ok_and(|view| view.alive) {
            let machine = self.machine();
            if let Err(e) = self.shared.vm_call(move |vm| vm.stop(&machine)).await {
                warn!("stop the VM of failed pod sandbox {}: {e}", self.pod.id);
            }
        }
        t.reason = Reason::Error;
        t.error = Some(why.clone());
        let failure = why.clone();
        if let Err(e) = self.transition(PodState::Failed, &t, |r| {
            r.vm = None;
            r.failure = Some(failure);
        }) {
            warn!("record the failure of pod sandbox {}: {e}", self.pod.id);
        }
        let reason = why.clone();
        self.hook(move |h, info| h.failed(info, &reason)).await;
        during.fail_all(LifecycleError::Failed(why));
    }

    /// Classifies the record against smolvm and acts on it: recovery after a
    /// restart, or a transition that stopped half way.
    async fn settle(&mut self, recovered: bool) {
        let record = self.pod.record();
        let reason = match recovered {
            true => Reason::Recovery,
            false => record.reason.clone().unwrap_or(Reason::Error),
        };
        let mut t = Transition::new(reason);
        t.recovered = recovered;
        let started = Instant::now();
        let view = self.inspect().await;
        let plan = match &view {
            _ if record.boot_id != self.shared.config.boot_id => Plan::DropRecord,
            Ok(view) => classify(record.state, view),
            Err(_) if record.state == PodState::Creating => Plan::DropRecord,
            Err(_) if record.state == PodState::Deleting => Plan::FinishDelete,
            Err(e) => Plan::Fail(format!("inspect the VM: {e}")),
        };
        if !matches!(plan, Plan::Keep | Plan::Adopt) {
            info!(
                "pod sandbox {} is {} in its record: {plan:?}",
                self.pod.id, record.state
            );
        }
        let process = view.ok().and_then(|v| v.process);
        let during = During::new(record.state);
        match plan {
            Plan::Keep => match record.state {
                PodState::Asleep => self.hook(|h, info| h.asleep(info)).await,
                PodState::Failed => {
                    let why = record.failure.unwrap_or_default();
                    self.hook(move |h, info| h.failed(info, &why)).await
                }
                _ => {}
            },
            Plan::Adopt => {
                if let Err(e) = self.pod.commit(&self.shared, |r| r.vm = process) {
                    warn!("record the VM of pod sandbox {}: {e}", self.pod.id);
                }
                self.hook(|h, info| h.awake(info, AwakeCause::Recovered))
                    .await;
            }
            Plan::ResumeInPlace => match self.resume_in_place(&mut t).await {
                Ok(()) => {
                    self.finish_wake(t, started, during, AwakeCause::Recovered, false)
                        .await
                }
                Err(e) => self.fail(t, e, during).await,
            },
            Plan::StopPaused => match self.stop_paused(&mut t, process).await {
                Plan::Asleep => self.fall_asleep(t, started, None, during).await,
                Plan::Fail(why) => self.fail(t, why, during).await,
                other => {
                    let why = format!("no way to settle a stopped pause from {other:?}");
                    self.fail(t, why, during).await
                }
            },
            Plan::Asleep => self.fall_asleep(t, started, None, during).await,
            Plan::Rewake => self.resume_and_finish(t, started, during).await,
            Plan::FinishWake => {
                let resumed = record.state == PodState::Waking;
                self.finish_wake(t, started, during, AwakeCause::Recovered, resumed)
                    .await
            }
            Plan::Fail(why) => self.fail(t, why, during).await,
            Plan::DropRecord => self.drop_record(&mut t).await,
            Plan::FinishDelete => {
                if let Err(e) = self.delete(t).await {
                    warn!("finish the delete of pod sandbox {}: {e}", self.pod.id);
                }
            }
        }
    }

    /// Deletes the machine and its checkpoint without a wake, then the
    /// record. A failed delete leaves the pod `deleting`, for a retry.
    async fn delete(&mut self, mut t: Transition) -> Result<(), LifecycleError> {
        if self.pod.state() != PodState::Deleting {
            self.transition(PodState::Deleting, &t, |_| {})
                .map_err(LifecycleError::Store)?;
        }
        let started = Instant::now();
        let machine = self.machine();
        self.shared
            .vm_call(move |vm| vm.delete(&machine))
            .await
            .map_err(LifecycleError::Vm)?;
        t.steps.push("delete", started.elapsed());
        self.hook(|h, info| h.deleted(info)).await;
        self.remove(&t).map_err(LifecycleError::Store)
    }

    async fn kill(&mut self, request: KillRequest) -> Result<KillOutcome, LifecycleError> {
        match self.pod.state() {
            PodState::Awake => self.deliver(&request).await,
            PodState::Asleep if self.deferrable(request.signal) => self.defer(&request),
            PodState::Asleep => {
                self.wake(Reason::Signal, None, Vec::new()).await;
                match self.pod.state() {
                    PodState::Awake => self.deliver(&request).await,
                    state => Err(self.unavailable(state, "signal")),
                }
            }
            state => Err(self.unavailable(state, "signal")),
        }
    }

    /// SIGKILL and SIGTERM do not wake a sleeping pod.
    fn deferrable(&self, signal: u32) -> bool {
        signal == SIGKILL
            || (signal == SIGTERM && !lock(&self.pod.record).identity.wake_on_terminate)
    }

    fn defer(&self, request: &KillRequest) -> Result<KillOutcome, LifecycleError> {
        let kill = DeferredKill {
            container: request.container.clone(),
            exec: request.exec.clone(),
            signal: request.signal,
            all: request.all,
        };
        self.pod
            .commit(&self.shared, |r| {
                if !r.deferred_kills.contains(&kill) {
                    r.deferred_kills.push(kill);
                }
            })
            .map_err(LifecycleError::Store)?;
        Ok(KillOutcome::Deferred {
            exit_status: 128 + request.signal,
        })
    }

    async fn deliver(&self, request: &KillRequest) -> Result<KillOutcome, LifecycleError> {
        let (machine, r) = (self.machine(), request.clone());
        self.shared
            .vm_call(move |vm| {
                vm.signal(&machine, &r.container, r.exec.as_deref(), r.signal, r.all)
            })
            .await
            .map_err(LifecycleError::Vm)?;
        Ok(KillOutcome::Delivered)
    }

    async fn vm_exited(&mut self, process: VmProcess) -> VmExit {
        let record = self.pod.record();
        match record.state {
            PodState::Asleep => VmExit::Hibernation,
            PodState::Failed => VmExit::Crash,
            PodState::Deleting | PodState::Deleted => VmExit::Deleted,
            PodState::Awake if record.vm.is_none_or(|vm| vm == process) => {
                if self.alive(process).await {
                    return VmExit::Stale;
                }
                let during = During::new(PodState::Awake);
                let t = Transition::new(Reason::Error);
                self.fail(t, "the VM process exited".into(), during).await;
                VmExit::Crash
            }
            _ => VmExit::Stale,
        }
    }
}

/// The `pausing` hook, the stop of the output pumps and the VM pause, in
/// their own task. The slot limits pauses across pods.
async fn pause(
    shared: Arc<Shared>,
    info: PodInfo,
    reason: Reason,
    control: Arc<PauseControl>,
) -> (Result<PauseOutcome, PauseError>, Steps) {
    let mut steps = Steps::default();
    let started = Instant::now();
    let _slot = match shared.pause_slots.acquire().await {
        Ok(slot) => slot,
        Err(e) => return (Err(PauseError::Failed(e.to_string())), steps),
    };
    steps.push("slot", started.elapsed());
    if control.is_cancelled() {
        return (Ok(PauseOutcome::Cancelled), steps);
    }
    let (hooks, vm) = (shared.hooks.clone(), shared.vm.clone());
    let faults = shared.config.faults.for_pod(&info.identity.sandbox_id);
    let paused = blocking(move || {
        let mut steps = Steps::default();
        if reason == Reason::Idle {
            faults.hit(FaultPoint::IdleDecided);
        }
        let started = Instant::now();
        if let Err(refusal) = hooks.pausing(&info, &reason) {
            return Ok(Err(PauseError::Refused(refusal)));
        }
        steps.push("pausing", started.elapsed());
        let started = Instant::now();
        hooks
            .stop_pumps(&info)
            .map_err(|e| format!("stop the output pumps: {e}"))?;
        steps.push("stop-pumps", started.elapsed());
        if control.is_cancelled() {
            return Ok(Ok((PauseOutcome::Cancelled, steps)));
        }
        let started = Instant::now();
        let outcome = vm.pause(&info.identity.machine, &control)?;
        steps.push("pause", started.elapsed());
        Ok(Ok((outcome, steps)))
    })
    .await;
    match paused {
        Ok(Ok((outcome, more))) => {
            steps.extend(more);
            (Ok(outcome), steps)
        }
        Ok(Err(refusal)) => (Err(refusal), steps),
        Err(e) => (Err(PauseError::Failed(e)), steps),
    }
}

/// The VM resume, in its own task. The slot limits resumes across pods.
async fn resume(shared: Arc<Shared>, id: String, machine: String) -> Result<Steps, String> {
    let mut steps = Steps::default();
    let started = Instant::now();
    let _slot = shared
        .resume_slots
        .acquire()
        .await
        .map_err(|e| e.to_string())?;
    steps.push("slot", started.elapsed());
    let faults = shared.config.faults.for_pod(&id);
    let took = shared
        .vm_call(move |vm| {
            faults.check(FaultPoint::WakeResumeError)?;
            let started = Instant::now();
            vm.resume(&machine)?;
            let took = started.elapsed();
            faults.hit(FaultPoint::WakeResumed);
            Ok(took)
        })
        .await?;
    steps.push("resume", took);
    Ok(steps)
}

/// The post-resume steps (`tso`, `ping`, `clock`, `deferred_kills`,
/// `before_release` and `release`), in their own task. Each is idempotent, so
/// recovery runs them again after a crash.
async fn post_resume(shared: Arc<Shared>, pod: Arc<Pod>) -> Result<PostResume, String> {
    let identity = pod.record().identity;
    let machine = identity.machine.clone();
    let mut steps = Steps::default();
    let mut warning = None;

    // The VM process runs, and turned TSO on if the VM has Rosetta.
    let started = Instant::now();
    let m = machine.clone();
    let view = shared.vm_call(move |vm| vm.inspect(&m)).await?;
    let process = view
        .running()
        .ok_or("the VM is not running after the resume")?;
    pod.commit(&shared, |r| r.vm = Some(process))?;
    if identity.rosetta {
        let m = machine.clone();
        let tso = shared
            .vm_call(move |vm| Ok(vm.has_tso(&m, &process)))
            .await?;
        if !tso {
            let m = machine.clone();
            if let Err(e) = shared.vm_call(move |vm| vm.stop(&m)).await {
                warn!("stop the VM of pod sandbox {}: {e}", pod.id);
            }
            return Err("the VM process has no TSO marker, so it was stopped".into());
        }
    }
    steps.push("tso", started.elapsed());

    // The guest agent answers.
    let started = Instant::now();
    let deadline = started + shared.config.agent_timeout;
    loop {
        let m = machine.clone();
        let Err(e) = shared.vm_call(move |vm| vm.ping(&m)).await else {
            break;
        };
        if !shared.vm_call(move |vm| Ok(vm.is_alive(&process))).await? {
            return Err(format!("the VM process exited after the resume: {e}"));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "the guest agent did not answer within {} s: {e}",
                shared.config.agent_timeout.as_secs_f64()
            ));
        }
        tokio::time::sleep(shared.config.ping_interval).await;
    }
    steps.push("ping", started.elapsed());

    // The guest clock.
    let started = Instant::now();
    let (m, clock, faults) = (
        machine.clone(),
        shared.config.clock.clone(),
        shared.config.faults.for_pod(&pod.id),
    );
    let step = shared
        .vm_call(move |vm| {
            let step = vm.set_clock(&m, clock.now_ns())?;
            faults.hit(FaultPoint::WakeClock);
            Ok(step)
        })
        .await?;
    steps.push("clock", started.elapsed());

    // The deferred kills, then the before_release hook.
    let started = Instant::now();
    let kills = pod.record().deferred_kills;
    for kill in &kills {
        let (m, k) = (machine.clone(), kill.clone());
        let target = match &kill.exec {
            Some(exec) => format!("{}/{exec}", kill.container),
            None => kill.container.clone(),
        };
        let killed = shared
            .vm_call(move |vm| vm.signal(&m, &k.container, k.exec.as_deref(), SIGKILL, k.all))
            .await;
        if let Err(e) = killed {
            warn!(
                "deliver the deferred kill of {target} in pod sandbox {}: {e}",
                pod.id
            );
            warning = Some(format!("deliver the deferred kill of {target}: {e}"));
        }
    }
    if !kills.is_empty() {
        pod.commit(&shared, |r| r.deferred_kills.retain(|k| !kills.contains(k)))?;
    }
    steps.push("deferred_kills", started.elapsed());
    let started = Instant::now();
    let hooks = shared.hooks.clone();
    let info = pod.info(shared.now_ms());
    if let Err(e) = blocking(move || hooks.before_release(&info)).await {
        warn!(
            "the before_release hook of pod sandbox {} failed: {e}",
            pod.id
        );
        warning = Some(format!("before release: {e}"));
    }
    steps.push("before_release", started.elapsed());

    // Release the traffic held since the pause, the last step before the pod
    // is awake.
    let started = Instant::now();
    let hooks = shared.hooks.clone();
    let info = pod.info(shared.now_ms());
    let held = match blocking(move || hooks.release(&info)).await {
        Ok(held) => held,
        Err(e) => {
            warn!("the release hook of pod sandbox {} failed: {e}", pod.id);
            warning = Some(format!("release: {e}"));
            None
        }
    };
    steps.push("release", started.elapsed());
    Ok(PostResume {
        steps,
        clock: step,
        process,
        warning,
        held,
    })
}
