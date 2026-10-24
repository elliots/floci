//! The recovery table: what to do with a pod, given its record's state and
//! smolvm's view of its VM. Recovery after a restart uses it, and so does a
//! transition whose outcome is uncertain, such as a pause or a resume that
//! returned an error.

use super::state::PodState;
use super::vm::{VmState, VmView};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// The record matches smolvm.
    Keep,
    /// The VM runs: record its process and reattach the pumps.
    Adopt,
    /// Thaw a VM that a pause left frozen, then the post-resume steps.
    ResumeInPlace,
    /// Stop the VM process of a durable pause, then the pod is asleep.
    StopPaused,
    /// The pause is durable and the VM process gone: the pod is asleep.
    Asleep,
    /// The resume never started: run it again.
    Rewake,
    /// The VM runs after a resume or an undone pause: run the post-resume
    /// steps, which are idempotent, then the pod is awake.
    FinishWake,
    Fail(String),
    /// A crash during the boot: delete the machine and drop the record.
    DropRecord,
    FinishDelete,
}

pub fn classify(state: PodState, vm: &VmView) -> Plan {
    let running = vm.state == VmState::Running && vm.alive;
    match state {
        PodState::Creating => Plan::DropRecord,
        PodState::Deleting | PodState::Deleted => Plan::FinishDelete,
        PodState::Failed => Plan::Keep,
        PodState::Awake if running => Plan::Adopt,
        PodState::Awake if vm.state == VmState::Running => {
            Plan::Fail("the VM process exited while the node service was down".into())
        }
        // The freeze never started, or smolvm's guard undid it. The guest
        // clock may be behind by the freeze, so the post-resume steps run.
        PodState::Pausing if running => Plan::FinishWake,
        PodState::Pausing if vm.state == VmState::Pausing && vm.alive => Plan::ResumeInPlace,
        PodState::Pausing if vm.state == VmState::Paused && vm.alive => Plan::StopPaused,
        PodState::Pausing if vm.state == VmState::Paused => paused_and_gone(vm),
        PodState::Pausing if matches!(vm.state, VmState::Pausing | VmState::Running) => {
            Plan::Fail("the VM process exited before the pause was durable".into())
        }
        PodState::Asleep if vm.state == VmState::Paused && !vm.alive && vm.checkpoint => Plan::Keep,
        PodState::Asleep if !vm.alive && !vm.checkpoint => {
            Plan::Fail("the checkpoint is missing".into())
        }
        PodState::Waking if vm.state == VmState::Paused && !vm.alive && vm.checkpoint => {
            Plan::Rewake
        }
        PodState::Waking if vm.state == VmState::Paused && !vm.alive => {
            Plan::Fail("the checkpoint is missing".into())
        }
        PodState::Waking if running => Plan::FinishWake,
        PodState::Waking if vm.state == VmState::Running => {
            Plan::Fail("the VM process exited after the resume".into())
        }
        _ => by_vm(vm),
    }
}

/// For pairs that `classify` does not match, which only a VM changed outside
/// the node service can make: follow smolvm's view.
fn by_vm(vm: &VmView) -> Plan {
    match (vm.state, vm.alive) {
        (VmState::Running, true) => Plan::FinishWake,
        (VmState::Pausing, true) => Plan::ResumeInPlace,
        (VmState::Paused, true) => Plan::StopPaused,
        (VmState::Paused, false) => paused_and_gone(vm),
        (state, _) => Plan::Fail(format!(
            "smolvm has no running or paused VM for the pod ({state:?})"
        )),
    }
}

fn paused_and_gone(vm: &VmView) -> Plan {
    if vm.checkpoint {
        Plan::Asleep
    } else {
        Plan::Fail("the pause left no checkpoint".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::vm::VmProcess;

    fn vm(state: VmState, alive: bool, checkpoint: bool) -> VmView {
        VmView {
            state,
            process: Some(VmProcess {
                pid: 10,
                start_time: Some(1),
            }),
            alive,
            checkpoint,
            checkpoint_bytes: None,
        }
    }

    fn fails(plan: Plan) -> bool {
        matches!(plan, Plan::Fail(_))
    }

    /// Each row of the recovery table, in its order.
    #[test]
    fn the_rows_of_the_recovery_table() {
        use super::PodState::*;
        use super::VmState::{Missing, Paused, Running};
        let pausing_vm = VmState::Pausing;
        assert_eq!(
            classify(Creating, &vm(Running, true, false)),
            Plan::DropRecord
        );
        assert_eq!(classify(Creating, &VmView::missing()), Plan::DropRecord);
        assert_eq!(classify(Awake, &vm(Running, true, false)), Plan::Adopt);
        assert!(fails(classify(Awake, &vm(Running, false, false))));
        assert_eq!(
            classify(Pausing, &vm(Running, true, false)),
            Plan::FinishWake
        );
        assert_eq!(
            classify(Pausing, &vm(pausing_vm, true, true)),
            Plan::ResumeInPlace
        );
        assert_eq!(classify(Pausing, &vm(Paused, true, true)), Plan::StopPaused);
        assert_eq!(classify(Pausing, &vm(Paused, false, true)), Plan::Asleep);
        assert!(fails(classify(Pausing, &vm(Paused, false, false))));
        assert!(fails(classify(Pausing, &vm(pausing_vm, false, true))));
        assert!(fails(classify(Pausing, &vm(Running, false, false))));
        assert_eq!(classify(Asleep, &vm(Paused, false, true)), Plan::Keep);
        assert!(fails(classify(Asleep, &vm(Paused, false, false))));
        assert!(fails(classify(Asleep, &VmView::missing())));
        assert_eq!(classify(Waking, &vm(Paused, false, true)), Plan::Rewake);
        assert!(fails(classify(Waking, &vm(Paused, false, false))));
        assert_eq!(
            classify(Waking, &vm(Running, true, false)),
            Plan::FinishWake
        );
        assert!(fails(classify(Waking, &vm(Running, false, false))));
        assert_eq!(
            classify(Deleting, &vm(Paused, false, true)),
            Plan::FinishDelete
        );
        assert_eq!(classify(Deleting, &VmView::missing()), Plan::FinishDelete);
        assert_eq!(classify(Failed, &vm(Missing, false, false)), Plan::Keep);
    }

    /// Pairs that the table does not list follow smolvm's view.
    #[test]
    fn other_pairs_follow_smolvm() {
        use super::PodState::*;
        use super::VmState::{Paused, Running, Stopped};
        assert_eq!(
            classify(Asleep, &vm(Running, true, false)),
            Plan::FinishWake
        );
        assert_eq!(classify(Asleep, &vm(Paused, true, true)), Plan::StopPaused);
        assert_eq!(
            classify(Awake, &vm(VmState::Pausing, true, true)),
            Plan::ResumeInPlace
        );
        assert_eq!(classify(Awake, &vm(Paused, false, true)), Plan::Asleep);
        assert!(fails(classify(Awake, &vm(Stopped, false, false))));
        assert!(fails(classify(Awake, &VmView::missing())));
        assert!(fails(classify(Waking, &VmView::missing())));
    }
}
