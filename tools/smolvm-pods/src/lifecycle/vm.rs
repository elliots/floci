//! What the lifecycle needs from the VM layer ([`VmOps`]) and from the rest
//! of the node service ([`Hooks`]).
//!
//! Every method is synchronous, as smolvm's embedded API is, and the
//! lifecycle calls each one in `spawn_blocking`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use log::warn;
use serde::{Deserialize, Serialize};

use super::fault::{FaultPoint, Faults};
use super::state::Reason;
use super::PodInfo;
use crate::sync::lock;

/// A VM process: a pid and its start time, which tell it from a later
/// process with the same pid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VmProcess {
    pub pid: i32,
    pub start_time: Option<u64>,
}

/// smolvm's record of a machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VmState {
    Running,
    Pausing,
    Paused,
    /// Created, stopped or failed: no VM process should run.
    Stopped,
    /// smolvm has no such machine.
    Missing,
}

/// smolvm's view of a machine, which recovery compares with the pod's record
/// (see [`super::recovery`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VmView {
    pub state: VmState,
    /// The process smolvm's record names.
    pub process: Option<VmProcess>,
    /// Whether that process still runs (pid and start time).
    pub alive: bool,
    /// Whether the record has a checkpoint that is present and verified.
    pub checkpoint: bool,
    pub checkpoint_bytes: Option<u64>,
}

impl VmView {
    pub fn missing() -> VmView {
        VmView {
            state: VmState::Missing,
            process: None,
            alive: false,
            checkpoint: false,
            checkpoint_bytes: None,
        }
    }

    /// The process, if smolvm says the VM runs and it is alive.
    pub fn running(&self) -> Option<VmProcess> {
        self.process
            .filter(|_| self.state == VmState::Running && self.alive)
    }
}

/// How a pause saved the guest's RAM.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SaveMode {
    /// The whole RAM image.
    Full,
    /// Only the 4 KiB pages that changed since the pod's last pause.
    Delta,
}

impl SaveMode {
    pub fn as_str(self) -> &'static str {
        match self {
            SaveMode::Full => "full",
            SaveMode::Delta => "delta",
        }
    }
}

/// What a pause wrote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Save {
    pub mode: SaveMode,
    pub written_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PauseOutcome {
    /// The checkpoint is durable and the VM process stopped. `save` is none
    /// when the VM was already paused and the pause wrote nothing.
    Paused {
        checkpoint_bytes: Option<u64>,
        save: Option<Save>,
    },
    /// A wake cancelled the pause before it was durable, and smolvm resumed
    /// the frozen VM in place.
    Cancelled,
}

/// The answer of the agent's `SetClock`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClockStep {
    /// Guest realtime minus host realtime, before any step.
    pub skew_ns: i64,
    pub stepped: bool,
}

type Canceller = Arc<dyn Fn() + Send + Sync>;

/// One pause in flight. [`VmOps::pause`] reports smolvm's stages to it and
/// checks it for a cancel.
pub struct PauseControl {
    cancelled: AtomicBool,
    durable: AtomicBool,
    faults: Faults,
    canceller: Mutex<Option<Canceller>>,
}

impl PauseControl {
    pub(super) fn new(faults: Faults) -> PauseControl {
        PauseControl {
            cancelled: AtomicBool::new(false),
            durable: AtomicBool::new(false),
            faults,
            canceller: Mutex::new(None),
        }
    }

    /// Registers what cancels the pause in smolvm. A cancel calls it on a
    /// thread of its own, because smolvm's cancel may wait for the VMM. If
    /// the pause was cancelled already, it is called at once, on this thread.
    pub fn on_cancel(&self, cancel: impl Fn() + Send + Sync + 'static) {
        let cancel: Canceller = Arc::new(cancel);
        let mut slot = lock(&self.canceller);
        if self.is_cancelled() {
            drop(slot);
            cancel();
            return;
        }
        *slot = Some(cancel);
    }

    /// Whether a wake asked to cancel the pause. smolvm checks it before and
    /// while it writes the RAM, and never after the pause is durable.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Call when the VM is frozen and smolvm's record says Pausing.
    pub fn saving(&self) {
        self.faults.hit(FaultPoint::PauseSaving);
    }

    /// Call when smolvm's record says Paused, before the VM process stops.
    pub fn durable(&self) {
        self.durable.store(true, Ordering::SeqCst);
        self.faults.hit(FaultPoint::PauseDurable);
    }

    pub fn is_durable(&self) -> bool {
        self.durable.load(Ordering::SeqCst)
    }

    /// Asks the pause to stop. Returns false if it is already durable, in
    /// which case it finishes. The outcome of the pause is what counts.
    pub(super) fn cancel(&self) -> bool {
        self.cancelled.store(true, Ordering::SeqCst);
        let canceller = lock(&self.canceller).clone();
        if let Some(cancel) = canceller {
            let spawned = std::thread::Builder::new()
                .name("pause-cancel".into())
                .spawn(move || cancel());
            if let Err(e) = spawned {
                warn!("start the thread that cancels a pause: {e}");
            }
        }
        !self.is_durable()
    }
}

/// The VM operations of the lifecycle, over smolvm in the node service and a
/// mock in tests. Errors are messages, as in [`crate::backend::PodBackend`].
pub trait VmOps: Send + Sync + 'static {
    /// smolvm's view of `machine`. A missing machine is [`VmView::missing`],
    /// not an error.
    fn inspect(&self, machine: &str) -> Result<VmView, String>;

    /// The smolvm machines of pod VMs. Recovery deletes any that no record
    /// names.
    fn machines(&self) -> Result<Vec<String>, String>;

    /// A local pause (`pause_machine_local_with`). It calls
    /// [`PauseControl::saving`] once the VM is frozen and
    /// [`PauseControl::durable`] once the pause is durable, and checks
    /// [`PauseControl::is_cancelled`] until then. A cancel resumes the VM in
    /// place and returns [`PauseOutcome::Cancelled`].
    fn pause(&self, machine: &str, control: &Arc<PauseControl>) -> Result<PauseOutcome, String>;

    /// `resume_machine`: restores a paused VM from its checkpoint through our
    /// `_boot-vm`, or thaws a VM that a crash left frozen in Pausing.
    fn resume(&self, machine: &str) -> Result<(), String>;

    /// Stops the VM process of a durable pause (smolvm's `stop_paused`).
    fn stop_paused(&self, machine: &str) -> Result<(), String>;

    /// Stops a running VM.
    fn stop(&self, machine: &str) -> Result<(), String>;

    /// Deletes the machine and its checkpoint, stopping it first, without a
    /// wake. A missing machine is not an error.
    fn delete(&self, machine: &str) -> Result<(), String>;

    /// Whether `process` still runs (pid and start time).
    fn is_alive(&self, process: &VmProcess) -> bool;

    /// Whether `process` turned TSO on (`boot::has_tso`).
    fn has_tso(&self, machine: &str, process: &VmProcess) -> bool;

    /// During a resume: waits until the new VM process runs the guest
    /// (libkrun's control socket answers `OK running`) and, with `rosetta`,
    /// has turned TSO on. Returns false if `stop` is set first, which the
    /// resume does once it returns, or after a timeout.
    fn wait_running(&self, _machine: &str, _rosetta: bool, _stop: &AtomicBool) -> bool {
        false
    }

    /// One ping of the guest agent, with a timeout of its own, so that it
    /// fails fast if the agent does not answer.
    fn ping(&self, machine: &str) -> Result<(), String>;

    /// The agent's `SetClock { host_realtime_ns }`.
    fn set_clock(&self, machine: &str, host_realtime_ns: i64) -> Result<ClockStep, String>;

    /// The agent's `PodFreeze`, before a pause: the pod's containers freeze,
    /// and each TCP connection that leaves the guest is reset. Returns how
    /// many it reset.
    fn freeze(&self, machine: &str) -> Result<u32, String>;

    /// The agent's `PodThaw`: the containers that [`VmOps::freeze`] froze
    /// thaw.
    fn thaw(&self, machine: &str) -> Result<(), String>;

    /// `PodSignal` to a guest process. An exec of `None` is the container's
    /// init, and `all` signals every process of the container.
    fn signal(
        &self,
        machine: &str,
        container: &str,
        exec: Option<&str>,
        signal: u32,
        all: bool,
    ) -> Result<(), String>;
}

/// Why a pod became awake, for [`Hooks::awake`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwakeCause {
    Created,
    /// A resume from the checkpoint.
    Woke,
    /// A wake cancelled the pause.
    PauseCancelled,
    /// The pause failed or never froze the VM.
    PauseFailed,
    /// The node service restarted and adopted the running VM.
    Recovered,
}

/// The work of other parts of the node service at fixed points of the
/// transitions. Each runs in `spawn_blocking`; the defaults do nothing.
pub trait Hooks: Send + Sync + 'static {
    /// The first step of a wake, before the pod leaves `asleep`: admission.
    /// A wake that does not fit at once calls `waiting` once, with the
    /// reason, before it waits. An error refuses the wake.
    fn may_wake(&self, _pod: &PodInfo, _waiting: &dyn Fn(&str)) -> Result<(), String> {
        Ok(())
    }

    /// As a pause starts, before the pumps stop: hold the pod's traffic, then
    /// check that a pause for being idle (`reason`) saw no traffic since its
    /// decision. An error refuses the pause: the pod goes back to awake,
    /// through [`Hooks::release`].
    fn pausing(&self, _pod: &PodInfo, _reason: &Reason) -> Result<(), String> {
        Ok(())
    }

    /// After [`Hooks::pausing`]: stop the output pumps and persist their
    /// cursors. An error refuses the pause.
    fn stop_pumps(&self, _pod: &PodInfo) -> Result<(), String> {
        Ok(())
    }

    /// After the deferred kills: delete in the guest what containerd deleted
    /// while the pod slept, and refresh the containers' trust in Floci's CA.
    /// A warning that it returns, such as stale trust, is the wake event's
    /// `warning`; an error is recorded in the wake's event.
    fn before_release(&self, _pod: &PodInfo) -> Result<Option<String>, String> {
        Ok(None)
    }

    /// The last wake step, after [`Hooks::before_release`] and before the
    /// pod is awake: release the traffic held since the pause. It runs on
    /// every way back to awake from `pausing` or `waking`, also when
    /// [`Hooks::pausing`] never ran or refused, so it must do nothing for a
    /// pod that holds nothing. It returns the time from the capture of the
    /// first held frame to the release, if it delivered any, for the wake's
    /// event. An error is recorded in the wake's event.
    fn release(&self, _pod: &PodInfo) -> Result<Option<Duration>, String> {
        Ok(None)
    }

    /// The pod became awake: start the pumps at their cursors, or reattach
    /// them to an adopted VM.
    fn awake(&self, _pod: &PodInfo, _cause: AwakeCause) {}

    /// The pod is asleep: catch its traffic, and release its room under the
    /// admission caps. Also called for every sleeping pod after a restart,
    /// and after a resume that failed with the checkpoint intact
    /// ([`PodInfo::wake_failures`] counts those).
    fn asleep(&self, _pod: &PodInfo) {}

    /// The pod failed: report exit 255 for every task. Called again for
    /// every failed pod after a restart.
    fn failed(&self, _pod: &PodInfo, _reason: &str) {}

    /// The pod's machine was deleted: remove what smolvm's delete leaves.
    fn deleted(&self, _pod: &PodInfo) {}
}

#[cfg(test)]
pub struct NoHooks;

#[cfg(test)]
impl Hooks for NoHooks {}
