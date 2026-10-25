//! The lifecycle's VM operations over smolvm ([`SmolvmOps`]), and the watch of
//! a VM process ([`exited`]).
//!
//! Every pod VM is a smolvm machine named after its sandbox id. The lifecycle
//! calls each operation in `spawn_blocking`, and never two on one machine at
//! once: its actor runs a pod's transitions one at a time, and while a pause
//! runs, the only other call on the machine is the pause's cancel.

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::{debug, info, warn};
use smolvm::agent::AgentClient;
use smolvm::embedded::{EmbeddedRuntime, PauseStage};
use smolvm::RecordState;
use smolvm_protocol::AgentRequest;
use tokio::io::unix::AsyncFd;

use crate::engine::{delete_sandbox_vm, expect_ok, pod_request, CURSOR_ROOT};
use crate::lifecycle::{
    ClockStep, PauseControl, PauseOutcome, Save, SaveMode, VmOps, VmProcess, VmState, VmView,
};
use crate::proto::valid_id;

/// How long a check of the TSO marker waits for a new VM process to write it.
const TSO_MARKER_WAIT: Duration = Duration::from_secs(2);
/// How long [`VmOps::wait_running`] waits for a resumed VM to run.
const RUNNING_WAIT: Duration = Duration::from_secs(5);

/// The lifecycle's VM operations on the node's smolvm.
pub struct SmolvmOps;

fn runtime() -> Result<Arc<EmbeddedRuntime>, String> {
    smolvm::embedded::runtime().map_err(|e| format!("the smolvm runtime: {e}"))
}

fn agent_socket(machine: &str) -> PathBuf {
    smolvm::agent::vm_data_dir(machine).join("agent.sock")
}

fn vm_state(state: RecordState) -> VmState {
    match state {
        RecordState::Running => VmState::Running,
        RecordState::Pausing => VmState::Pausing,
        RecordState::Paused => VmState::Paused,
        _ => VmState::Stopped,
    }
}

impl VmOps for SmolvmOps {
    fn inspect(&self, machine: &str) -> Result<VmView, String> {
        let inspected = runtime()?
            .inspect_machine(machine)
            .map_err(|e| format!("inspect machine {machine}: {e}"))?;
        let Some(m) = inspected else {
            return Ok(VmView::missing());
        };
        let point = m.resume_point.as_ref();
        Ok(VmView {
            state: vm_state(m.state),
            process: m.pid.map(|pid| VmProcess {
                pid,
                start_time: m.pid_start_time,
            }),
            alive: m.process_alive,
            checkpoint: point.is_some_and(|p| p.verify_error.is_none()),
            checkpoint_bytes: point.map(|p| p.allocated_bytes),
        })
    }

    fn machines(&self) -> Result<Vec<String>, String> {
        let machines = runtime()?
            .list_machines()
            .map_err(|e| format!("list the smolvm machines: {e}"))?;
        Ok(machines
            .into_iter()
            .filter(|m| m.runtime_managed)
            .map(|m| m.name)
            .collect())
    }

    /// smolvm's stages become the control's, and the control's cancel
    /// becomes smolvm's, which resumes a frozen guest in place. The pause
    /// runs in [`crate::logging::pausing`], so smolvm's warning about the VM
    /// process that the pause itself stopped is logged at debug level.
    fn pause(&self, machine: &str, control: &Arc<PauseControl>) -> Result<PauseOutcome, String> {
        let rt = runtime()?;
        let ours = control.clone();
        let theirs = smolvm::embedded::PauseControl::with_observer(move |stage| match stage {
            PauseStage::Saving => ours.saving(),
            PauseStage::Durable => ours.durable(),
        });
        let cancel = theirs.clone();
        let name = machine.to_string();
        control.on_cancel(move || {
            if cancel.cancel() {
                info!("cancelled the pause of machine {name}");
            }
        });
        let outcome = crate::logging::pausing(|| rt.pause_machine_local_with(machine, &theirs))
            .map_err(|e| format!("pause machine {machine}: {e}"))?;
        Ok(match outcome {
            smolvm::embedded::PauseOutcome::Paused {
                save_mode,
                written_bytes,
            } => PauseOutcome::Paused {
                checkpoint_bytes: self.inspect(machine).ok().and_then(|v| v.checkpoint_bytes),
                save: save_mode.map(|mode| Save {
                    mode: match mode {
                        smolvm::embedded::SaveMode::Full => SaveMode::Full,
                        smolvm::embedded::SaveMode::Delta => SaveMode::Delta,
                    },
                    written_bytes,
                }),
            },
            smolvm::embedded::PauseOutcome::Cancelled => PauseOutcome::Cancelled,
        })
    }

    /// Not detached, so that smolvm deletes the consumed checkpoint in the
    /// background. The VM process still outlives the node service: smolvm arms
    /// no parent-death watch, because the node service registers no boot
    /// binary, and `/proc/self/exe _boot-vm` is this binary's entry point.
    fn resume(&self, machine: &str) -> Result<(), String> {
        runtime()?
            .resume_machine(machine)
            .map_err(|e| format!("resume machine {machine}: {e}"))
    }

    fn stop_paused(&self, machine: &str) -> Result<(), String> {
        runtime()?
            .stop_paused_machine(machine)
            .map_err(|e| format!("stop the paused machine {machine}: {e}"))
    }

    fn stop(&self, machine: &str) -> Result<(), String> {
        let rt = runtime()?;
        let paused = rt
            .machine_record(machine)
            .map_err(|e| e.to_string())?
            .is_some_and(|r| r.paused_checkpoint.is_some());
        let stopped = if paused {
            rt.stop_paused_machine(machine)
        } else {
            rt.stop_machine(machine)
        };
        stopped.map_err(|e| format!("stop machine {machine}: {e}"))
    }

    /// Also removes the pod share and the cursors of a sandbox, and what
    /// smolvm's delete leaves of every VM that is gone (see
    /// [`crate::cleanup`]). The pod's bridge is removed by the node service,
    /// which knows its network namespace.
    fn delete(&self, machine: &str) -> Result<(), String> {
        delete_sandbox_vm(&*runtime()?, machine)
            .map_err(|e| format!("delete machine {machine}: {e}"))?;
        if valid_id(machine) {
            let cursors = Path::new(CURSOR_ROOT).join(machine);
            if let Err(e) = std::fs::remove_dir_all(&cursors) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!("remove {}: {e}", cursors.display());
                }
            }
        }
        crate::cleanup::sweep();
        Ok(())
    }

    fn is_alive(&self, process: &VmProcess) -> bool {
        smolvm::process::is_our_process(process.pid, process.start_time)
    }

    /// The VM process writes its marker first thing, before it boots, but
    /// a resume may return before that, so this waits for it a moment.
    fn has_tso(&self, machine: &str, process: &VmProcess) -> bool {
        let dir = smolvm::agent::vm_data_dir(machine);
        let deadline = std::time::Instant::now() + TSO_MARKER_WAIT;
        loop {
            if !self.is_alive(process) {
                return false;
            }
            if crate::boot::has_tso(&dir, process.pid) {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn wait_running(&self, machine: &str, rosetta: bool, stop: &AtomicBool) -> bool {
        let socket = smolvm::agent::fork::control_socket_path(machine);
        let dir = smolvm::agent::vm_data_dir(machine);
        let deadline = Instant::now() + RUNNING_WAIT;
        while !stop.load(Ordering::Acquire) && Instant::now() < deadline {
            let running = smolvm::agent::fork::control_socket_cmd_with_timeout(
                &socket,
                "STATUS",
                Duration::from_millis(100),
            )
            .is_ok_and(|status| status.trim() == "OK running");
            if running && (!rosetta || crate::boot::has_live_tso(&dir)) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        false
    }

    /// With a read timeout of 1 s, which lets a resumed guest's vsock finish
    /// its handshake.
    fn ping(&self, machine: &str) -> Result<(), String> {
        AgentClient::connect_with_clone_ready_timeout(agent_socket(machine))
            .and_then(|mut client| client.ping())
            .map(drop)
            .map_err(|e| format!("ping the guest agent: {e}"))
    }

    fn set_clock(&self, machine: &str, host_realtime_ns: i64) -> Result<ClockStep, String> {
        let host = u64::try_from(host_realtime_ns).map_err(|_| "the host clock is before 1970")?;
        let step = AgentClient::connect(agent_socket(machine))
            .and_then(|mut client| client.set_clock(host))
            .map_err(|e| format!("set the guest clock: {e}"))?;
        Ok(ClockStep {
            skew_ns: step.skew_ns,
            stepped: step.stepped,
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
        let request = AgentRequest::PodSignal {
            id: container.to_string(),
            exec_id: exec.map(str::to_string),
            signal,
            all,
        };
        expect_ok(pod_request(&agent_socket(machine), &request)?, "PodSignal")
    }
}

/// How often a VM process without a pidfd is checked.
const POLL: Duration = Duration::from_secs(1);

/// Returns once `process` has exited. A VM process that this node service
/// adopted after a restart is not its child, so it is watched with a pidfd,
/// which also tells when a child exits.
pub async fn exited(process: VmProcess) {
    match pidfd(process) {
        Ok(Some(fd)) => match AsyncFd::new(fd) {
            Ok(fd) => {
                if let Err(e) = fd.readable().await {
                    warn!("watch VM process {}: {e}", process.pid);
                }
                return;
            }
            Err(e) => warn!("watch VM process {}: {e}", process.pid),
        },
        Ok(None) => return,
        Err(e) => warn!("open a pidfd of VM process {}: {e}", process.pid),
    }
    debug!("polling VM process {}", process.pid);
    while smolvm::process::is_our_process(process.pid, process.start_time) {
        tokio::time::sleep(POLL).await;
    }
}

/// A pidfd of `process`, or `None` if it has exited. The start time, read
/// after the pidfd is open, tells that the pid was not reused before.
fn pidfd(process: VmProcess) -> std::io::Result<Option<OwnedFd>> {
    // SAFETY: pidfd_open takes a pid and flags and returns a new descriptor.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, process.pid, 0) };
    if fd < 0 {
        let e = std::io::Error::last_os_error();
        return match e.raw_os_error() {
            Some(libc::ESRCH) => Ok(None),
            _ => Err(e),
        };
    }
    // SAFETY: the syscall returned a descriptor that nothing else owns.
    let fd = unsafe { OwnedFd::from_raw_fd(fd as i32) };
    debug_assert!(fd.as_raw_fd() >= 0);
    if !smolvm::process::is_our_process(process.pid, process.start_time) {
        return Ok(None);
    }
    Ok(Some(fd))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn process_of(child: &std::process::Child) -> VmProcess {
        let pid = child.id() as i32;
        VmProcess {
            pid,
            start_time: smolvm::process::process_start_time(pid),
        }
    }

    /// The watch returns when the process exits, before it is reaped.
    #[tokio::test]
    async fn the_watch_of_a_vm_process_returns_when_it_exits() {
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        let process = process_of(&child);
        let watch = tokio::spawn(exited(process));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!watch.is_finished(), "the process still runs");
        child.kill().unwrap();
        tokio::time::timeout(Duration::from_secs(5), watch)
            .await
            .expect("the watch did not see the exit")
            .unwrap();
        child.wait().unwrap();
    }

    /// A pid that names another process than the one watched, as after a
    /// reuse, has exited.
    #[tokio::test]
    async fn a_reused_pid_counts_as_exited() {
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        let mut process = process_of(&child);
        process.start_time = process.start_time.map(|t| t + 1);
        tokio::time::timeout(Duration::from_secs(5), exited(process))
            .await
            .expect("a reused pid is not the process");
        child.kill().unwrap();
        child.wait().unwrap();
    }
}
