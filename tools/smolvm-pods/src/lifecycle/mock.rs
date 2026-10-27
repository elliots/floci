//! A mock of smolvm for the lifecycle's tests, which follows smolvm's
//! records through a pause and a resume, and hooks that record their calls.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, UNIX_EPOCH};

use tokio::sync::watch;

use super::vm::{
    AwakeCause, ClockStep, Hooks, PauseControl, PauseOutcome, Save, SaveMode, VmOps, VmProcess,
    VmState, VmView,
};
use super::{Clock, PodInfo, PodState, Reason};
use crate::sync::{lock, wait_timeout};

pub const CHECKPOINT_BYTES: u64 = 1 << 30;

/// Holds the mock operations that pass it while it is held.
pub struct Gate {
    held: Mutex<bool>,
    wake: Condvar,
    entered: watch::Sender<usize>,
}

impl Gate {
    fn new() -> Gate {
        Gate {
            held: Mutex::new(false),
            wake: Condvar::new(),
            entered: watch::channel(0).0,
        }
    }

    pub fn hold(&self) {
        *lock(&self.held) = true;
    }

    pub fn release(&self) {
        *lock(&self.held) = false;
        self.wake.notify_all();
    }

    /// How many operations reached the gate.
    pub fn count(&self) -> usize {
        *self.entered.borrow()
    }

    /// Waits until `n` operations reached the gate.
    pub async fn entered(&self, n: usize) {
        let mut rx = self.entered.subscribe();
        let reached = tokio::time::timeout(Duration::from_secs(10), rx.wait_for(|&c| c >= n)).await;
        assert!(
            reached.is_ok_and(|r| r.is_ok()),
            "{n} operations did not reach the gate"
        );
    }

    /// Waits while the gate is held, unless `stop` says otherwise.
    fn pass(&self, stop: impl Fn() -> bool) {
        self.entered.send_modify(|c| *c += 1);
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut held = lock(&self.held);
        while *held && !stop() && Instant::now() < deadline {
            held = wait_timeout(&self.wake, held, Duration::from_millis(2));
        }
    }
}

/// smolvm's record of a machine, and how the mock should misbehave.
#[derive(Debug, Clone)]
pub struct Machine {
    pub state: VmState,
    pub process: Option<VmProcess>,
    pub checkpoint: Option<u64>,
    /// The VM process wrote its TSO marker.
    pub tso: bool,
    /// A VM process that outlived its pause, which `stop_paused` ends.
    pub leftover: Option<VmProcess>,
    pub tso_on_resume: bool,
    /// Pings that fail after each resume before the agent answers.
    pub ping_failures: u32,
    pub pings: u32,
    /// Resumes that fail before the VM starts.
    pub resume_failures: u32,
    pub fail_pause: Option<String>,
    pub survive_pause: bool,
    /// `stop_paused` leaves the VM process running.
    pub stuck: bool,
    pub skew_ns: i64,
}

impl Machine {
    fn new(state: VmState) -> Machine {
        Machine {
            state,
            process: None,
            checkpoint: None,
            tso: true,
            leftover: None,
            tso_on_resume: true,
            ping_failures: 0,
            pings: 0,
            resume_failures: 0,
            fail_pause: None,
            survive_pause: false,
            stuck: false,
            skew_ns: -192_000_000_000,
        }
    }
}

#[derive(Default)]
struct Inner {
    machines: HashMap<String, Machine>,
    alive: HashSet<VmProcess>,
    next_pid: i32,
    calls: Vec<String>,
    pausing: usize,
    max_pausing: usize,
}

impl Inner {
    fn spawn(&mut self) -> VmProcess {
        self.next_pid += 1;
        let process = VmProcess {
            pid: 1000 + self.next_pid,
            start_time: Some(7 * self.next_pid as u64),
        };
        self.alive.insert(process);
        process
    }

    fn end(&mut self, name: &str) {
        let Some(m) = self.machines.get_mut(name) else {
            return;
        };
        for process in [m.process.take(), m.leftover.take()].into_iter().flatten() {
            self.alive.remove(&process);
        }
    }
}

pub struct MockVm {
    inner: Mutex<Inner>,
    /// After the freeze, before the pause is durable.
    pub pause_gate: Gate,
    /// After the pause is durable, before the VM process stops.
    pub stop_gate: Gate,
    pub resume_gate: Gate,
    /// Whether a resumed VM runs the guest before the resume returns, as
    /// `wait_running` sees it.
    pub runs_early: AtomicBool,
}

impl MockVm {
    pub fn new() -> MockVm {
        MockVm {
            inner: Mutex::new(Inner::default()),
            pause_gate: Gate::new(),
            stop_gate: Gate::new(),
            resume_gate: Gate::new(),
            runs_early: AtomicBool::new(false),
        }
    }

    /// Adds a running machine whose VM process has a TSO marker.
    pub fn add_running(&self, name: &str) -> VmProcess {
        self.set(name, VmState::Running, true, false)
    }

    /// Sets smolvm's record of `name`, with a new VM process.
    pub fn set(&self, name: &str, state: VmState, alive: bool, checkpoint: bool) -> VmProcess {
        let mut inner = lock(&self.inner);
        inner.end(name);
        let process = inner.spawn();
        if !alive {
            inner.alive.remove(&process);
        }
        let mut machine = Machine::new(state);
        machine.process = Some(process);
        machine.checkpoint = checkpoint.then_some(CHECKPOINT_BYTES);
        inner.machines.insert(name.to_string(), machine);
        process
    }

    pub fn with<T>(&self, name: &str, edit: impl FnOnce(&mut Machine) -> T) -> T {
        let mut inner = lock(&self.inner);
        edit(inner.machines.get_mut(name).expect("a mock machine"))
    }

    pub fn machine(&self, name: &str) -> Option<Machine> {
        lock(&self.inner).machines.get(name).cloned()
    }

    /// The VM process of `name` dies.
    pub fn kill(&self, name: &str) {
        let mut inner = lock(&self.inner);
        if let Some(process) = inner.machines.get(name).and_then(|m| m.process) {
            inner.alive.remove(&process);
        }
    }

    /// Every call that may change a machine.
    pub fn calls(&self) -> Vec<String> {
        lock(&self.inner).calls.clone()
    }

    pub fn clear_calls(&self) {
        lock(&self.inner).calls.clear();
    }

    pub fn max_pausing(&self) -> usize {
        lock(&self.inner).max_pausing
    }

    fn call(&self, call: String) {
        lock(&self.inner).calls.push(call);
    }

    fn pause_steps(&self, machine: &str, control: &PauseControl) -> Result<PauseOutcome, String> {
        {
            let mut guard = lock(&self.inner);
            let inner = &mut *guard;
            let m = inner.machines.get_mut(machine).ok_or("no such machine")?;
            let alive = m.process.is_some_and(|p| inner.alive.contains(&p));
            if m.state != VmState::Running || !alive {
                return Err("the machine is not running".into());
            }
            m.state = VmState::Pausing;
        }
        control.saving();
        self.pause_gate.pass(|| control.is_cancelled());
        {
            let mut inner = lock(&self.inner);
            let m = inner.machines.get_mut(machine).ok_or("no such machine")?;
            if control.is_cancelled() {
                m.state = VmState::Running;
                return Ok(PauseOutcome::Cancelled);
            }
            if let Some(e) = m.fail_pause.clone() {
                m.state = VmState::Running;
                return Err(e);
            }
            m.state = VmState::Paused;
            m.checkpoint = Some(CHECKPOINT_BYTES);
        }
        control.durable();
        self.stop_gate.pass(|| false);
        let mut guard = lock(&self.inner);
        let inner = &mut *guard;
        let m = inner.machines.get_mut(machine).ok_or("no such machine")?;
        if let Some(process) = m.process.take() {
            if m.survive_pause {
                m.leftover = Some(process);
            } else {
                inner.alive.remove(&process);
            }
        }
        Ok(PauseOutcome::Paused {
            checkpoint_bytes: Some(CHECKPOINT_BYTES),
            save: Some(Save {
                mode: SaveMode::Full,
                written_bytes: CHECKPOINT_BYTES,
            }),
        })
    }
}

impl VmOps for MockVm {
    fn inspect(&self, machine: &str) -> Result<VmView, String> {
        let inner = lock(&self.inner);
        let Some(m) = inner.machines.get(machine) else {
            return Ok(VmView::missing());
        };
        Ok(VmView {
            state: m.state,
            process: m.process,
            alive: m.process.is_some_and(|p| inner.alive.contains(&p)),
            checkpoint: m.checkpoint.is_some(),
            checkpoint_bytes: m.checkpoint,
        })
    }

    fn machines(&self) -> Result<Vec<String>, String> {
        let mut names: Vec<_> = lock(&self.inner).machines.keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    fn pause(&self, machine: &str, control: &Arc<PauseControl>) -> Result<PauseOutcome, String> {
        self.call(format!("pause {machine}"));
        {
            let mut inner = lock(&self.inner);
            inner.pausing += 1;
            inner.max_pausing = inner.max_pausing.max(inner.pausing);
        }
        let outcome = self.pause_steps(machine, control);
        lock(&self.inner).pausing -= 1;
        outcome
    }

    fn resume(&self, machine: &str) -> Result<(), String> {
        self.call(format!("resume {machine}"));
        self.resume_gate.pass(|| false);
        let mut guard = lock(&self.inner);
        let inner = &mut *guard;
        let m = inner.machines.get(machine).ok_or("no such machine")?;
        let (state, checkpoint) = (m.state, m.checkpoint.is_some());
        let alive = m.process.is_some_and(|p| inner.alive.contains(&p));
        match (state, alive) {
            (VmState::Pausing, true) => {
                inner
                    .machines
                    .get_mut(machine)
                    .ok_or("no such machine")?
                    .state = VmState::Running;
                Ok(())
            }
            (VmState::Running, true) => Ok(()),
            (VmState::Paused, false) if checkpoint => {
                let process = inner.spawn();
                let m = inner.machines.get_mut(machine).ok_or("no such machine")?;
                if m.resume_failures > 0 {
                    m.resume_failures -= 1;
                    inner.alive.remove(&process);
                    return Err("the VM did not start".into());
                }
                m.process = Some(process);
                m.state = VmState::Running;
                m.checkpoint = None;
                m.tso = m.tso_on_resume;
                m.pings = 0;
                Ok(())
            }
            (state, alive) => Err(format!(
                "cannot resume a machine that is {state:?} (alive: {alive})"
            )),
        }
    }

    fn stop_paused(&self, machine: &str) -> Result<(), String> {
        self.call(format!("stop-paused {machine}"));
        let mut inner = lock(&self.inner);
        let m = inner.machines.get(machine).ok_or("no such machine")?;
        if m.state != VmState::Paused {
            return Err("the machine is not paused".into());
        }
        if !m.stuck {
            inner.end(machine);
        }
        Ok(())
    }

    fn stop(&self, machine: &str) -> Result<(), String> {
        self.call(format!("stop {machine}"));
        let mut inner = lock(&self.inner);
        inner.end(machine);
        if let Some(m) = inner.machines.get_mut(machine) {
            m.state = VmState::Stopped;
        }
        Ok(())
    }

    fn delete(&self, machine: &str) -> Result<(), String> {
        self.call(format!("delete {machine}"));
        let mut inner = lock(&self.inner);
        inner.end(machine);
        inner.machines.remove(machine);
        Ok(())
    }

    fn is_alive(&self, process: &VmProcess) -> bool {
        lock(&self.inner).alive.contains(process)
    }

    fn has_tso(&self, machine: &str, process: &VmProcess) -> bool {
        lock(&self.inner)
            .machines
            .get(machine)
            .is_some_and(|m| m.tso && m.process == Some(*process))
    }

    fn wait_running(&self, _machine: &str, _rosetta: bool, _stop: &AtomicBool) -> bool {
        self.runs_early.load(Ordering::SeqCst)
    }

    fn ping(&self, machine: &str) -> Result<(), String> {
        let mut guard = lock(&self.inner);
        let inner = &mut *guard;
        let m = inner.machines.get_mut(machine).ok_or("no such machine")?;
        if !m.process.is_some_and(|p| inner.alive.contains(&p)) {
            return Err("the VM is not running".into());
        }
        m.pings += 1;
        if m.pings <= m.ping_failures {
            return Err("the agent does not answer yet".into());
        }
        Ok(())
    }

    fn set_clock(&self, machine: &str, _host_realtime_ns: i64) -> Result<ClockStep, String> {
        self.call(format!("set-clock {machine}"));
        let skew_ns = lock(&self.inner)
            .machines
            .get(machine)
            .ok_or("no such machine")?
            .skew_ns;
        Ok(ClockStep {
            skew_ns,
            stepped: !(-50_000_000..=5_000_000_000).contains(&skew_ns),
        })
    }

    fn freeze(
        &self,
        machine: &str,
        keep: &[smolvm_protocol::PreservedTcpPeer],
    ) -> Result<u32, String> {
        if keep.is_empty() {
            self.call(format!("freeze {machine}"));
        } else {
            self.call(format!("freeze {machine} keep {keep:?}"));
        }
        Ok(0)
    }

    fn thaw(&self, machine: &str) -> Result<(), String> {
        self.call(format!("thaw {machine}"));
        Ok(())
    }

    fn signal(
        &self,
        machine: &str,
        container: &str,
        exec: Option<&str>,
        signal: u32,
        all: bool,
    ) -> Result<(), String> {
        let target = match exec {
            Some(exec) => format!("{container}/{exec}"),
            None => container.to_string(),
        };
        let all = if all { " all" } else { "" };
        self.call(format!("signal {machine} {target} {signal}{all}"));
        Ok(())
    }
}

/// Hooks that record each call as `<hook> <sandbox id>`.
#[derive(Default)]
pub struct RecordingHooks {
    calls: Mutex<Vec<String>>,
    pub refuse_wake: Mutex<Option<String>>,
    /// Admission waits, for this reason, before it answers.
    pub wait_first: Mutex<Option<String>>,
    pub fail_pausing: Mutex<Option<String>>,
    pub fail_stop_pumps: Mutex<Option<String>>,
    pub fail_release: Mutex<Option<String>>,
    /// What the release hook says about held traffic.
    pub held_for: Mutex<Option<Duration>>,
    /// The warning that the before_release hook returns.
    pub release_warning: Mutex<Option<String>>,
}

impl RecordingHooks {
    pub fn calls(&self) -> Vec<String> {
        lock(&self.calls).clone()
    }

    pub fn clear(&self) {
        lock(&self.calls).clear();
    }

    fn record(&self, hook: &str, pod: &PodInfo) {
        lock(&self.calls).push(format!("{hook} {}", pod.identity.sandbox_id));
    }
}

impl Hooks for RecordingHooks {
    fn may_wake(&self, pod: &PodInfo, waiting: &dyn Fn(&str)) -> Result<(), String> {
        self.record("may-wake", pod);
        if let Some(why) = lock(&self.wait_first).clone() {
            waiting(&why);
        }
        lock(&self.refuse_wake).clone().map_or(Ok(()), Err)
    }

    fn pausing(&self, pod: &PodInfo, reason: &Reason) -> Result<(), String> {
        assert_eq!(pod.state, PodState::Pausing, "pausing hook");
        self.record(&format!("pausing-{}", reason.kind()), pod);
        lock(&self.fail_pausing).clone().map_or(Ok(()), Err)
    }

    fn stop_pumps(&self, pod: &PodInfo) -> Result<(), String> {
        self.record("stop-pumps", pod);
        lock(&self.fail_stop_pumps).clone().map_or(Ok(()), Err)
    }

    fn before_release(&self, pod: &PodInfo) -> Result<Option<String>, String> {
        self.record("before-release", pod);
        Ok(lock(&self.release_warning).clone())
    }

    fn release(&self, pod: &PodInfo) -> Result<Option<Duration>, String> {
        self.record("release", pod);
        match lock(&self.fail_release).clone() {
            Some(e) => Err(e),
            None => Ok(*lock(&self.held_for)),
        }
    }

    fn awake(&self, pod: &PodInfo, cause: AwakeCause) {
        self.record(&format!("awake-{cause:?}"), pod);
    }

    fn asleep(&self, pod: &PodInfo) {
        self.record("asleep", pod);
    }

    fn failed(&self, pod: &PodInfo, _reason: &str) {
        self.record("failed", pod);
    }

    fn deleted(&self, pod: &PodInfo) {
        self.record("deleted", pod);
    }
}

/// VMs that always run, for a lifecycle that only keeps records.
pub struct RunningVms;

impl VmOps for RunningVms {
    fn inspect(&self, _machine: &str) -> Result<VmView, String> {
        Ok(VmView {
            state: VmState::Running,
            process: Some(VmProcess {
                pid: 1,
                start_time: Some(1),
            }),
            alive: true,
            checkpoint: false,
            checkpoint_bytes: None,
        })
    }

    fn machines(&self) -> Result<Vec<String>, String> {
        Ok(Vec::new())
    }

    fn pause(&self, _: &str, _: &Arc<PauseControl>) -> Result<PauseOutcome, String> {
        Err("no pauses here".into())
    }

    fn resume(&self, _: &str) -> Result<(), String> {
        Err("no resumes here".into())
    }

    fn stop_paused(&self, _: &str) -> Result<(), String> {
        Ok(())
    }

    fn stop(&self, _: &str) -> Result<(), String> {
        Ok(())
    }

    fn delete(&self, _: &str) -> Result<(), String> {
        Ok(())
    }

    fn is_alive(&self, _: &VmProcess) -> bool {
        true
    }

    fn has_tso(&self, _: &str, _: &VmProcess) -> bool {
        true
    }

    fn ping(&self, _: &str) -> Result<(), String> {
        Ok(())
    }

    fn set_clock(&self, _: &str, _: i64) -> Result<ClockStep, String> {
        Ok(ClockStep {
            skew_ns: 0,
            stepped: false,
        })
    }

    fn freeze(&self, _: &str, _: &[smolvm_protocol::PreservedTcpPeer]) -> Result<u32, String> {
        Ok(0)
    }

    fn thaw(&self, _: &str) -> Result<(), String> {
        Ok(())
    }

    fn signal(&self, _: &str, _: &str, _: Option<&str>, _: u32, _: bool) -> Result<(), String> {
        Ok(())
    }
}

/// A clock at `now_ms`, in milliseconds since the epoch, which the test moves.
pub fn clock_at(now_ms: &Arc<AtomicU64>) -> Clock {
    let now = now_ms.clone();
    Clock::from_fn(move || UNIX_EPOCH + Duration::from_millis(now.load(Ordering::SeqCst)))
}
