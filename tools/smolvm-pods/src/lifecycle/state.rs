//! Pod states, the reasons for transitions, and what blocks a pause.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::record::ExecStatus;

/// The state of a pod.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PodState {
    /// The sandbox VM boots and the guest is set up.
    Creating,
    /// The agent answers and the post-resume steps are done.
    Awake,
    /// A local pause runs (see [`PausePhase`]).
    Pausing,
    /// The checkpoint is durable and the VM process is verified gone.
    Asleep,
    /// A resume and its post-resume steps run.
    Waking,
    /// The VM is lost, and every task was reported as exited with 255.
    Failed,
    /// The sandbox is being deleted.
    Deleting,
    /// Only in events: the record is gone.
    Deleted,
}

impl PodState {
    pub fn as_str(self) -> &'static str {
        match self {
            PodState::Creating => "creating",
            PodState::Awake => "awake",
            PodState::Pausing => "pausing",
            PodState::Asleep => "asleep",
            PodState::Waking => "waking",
            PodState::Failed => "failed",
            PodState::Deleting => "deleting",
            PodState::Deleted => "deleted",
        }
    }

    /// Whether the pod has not failed and is not being deleted.
    pub fn is_live(self) -> bool {
        !matches!(
            self,
            PodState::Failed | PodState::Deleting | PodState::Deleted
        )
    }
}

impl fmt::Display for PodState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The part of `pausing` a pause is in. Only kept in memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PausePhase {
    /// The RAM is being written; a wake cancels the pause.
    Saving,
    /// The pause is durable and the VM process stops; a wake resumes after it.
    Stopping,
}

/// Why a transition ran.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Reason {
    Api,
    Exec,
    ContainerCreate,
    Signal,
    Traffic {
        from: String,
        port: u16,
    },
    Aws {
        queue: String,
    },
    Idle,
    Error,
    /// The node service's recovery after a restart finished or undid a
    /// transition.
    Recovery,
}

impl Reason {
    /// The reason's kind, as its JSON names it.
    pub fn kind(&self) -> &'static str {
        match self {
            Reason::Api => "api",
            Reason::Exec => "exec",
            Reason::ContainerCreate => "container-create",
            Reason::Signal => "signal",
            Reason::Traffic { .. } => "traffic",
            Reason::Aws { .. } => "aws",
            Reason::Idle => "idle",
            Reason::Error => "error",
            Reason::Recovery => "recovery",
        }
    }
}

/// Why a pod cannot pause now, or why its wake was refused
/// ([`Blocker::Admission`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Blocker {
    /// The pod has the annotation `smolvm-pods.floci.io/hibernate: "false"`.
    Disabled,
    Lease {
        id: String,
        holder: Option<String>,
        expires_ms: u64,
    },
    Exec {
        container: String,
        exec: String,
        status: ExecStatus,
    },
    /// An exec is being created.
    ExecStarting,
    ContainerCreating,
    /// A wake has waiters, or completed less than the grace time ago.
    WakeWaiter {
        until_ms: u64,
    },
    Waking,
    /// The admission hook refused a wake.
    Admission {
        reason: String,
    },
}

impl fmt::Display for Blocker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Blocker::Disabled => write!(
                f,
                "hibernation disabled by {}",
                super::record::ANN_HIBERNATE
            ),
            Blocker::Lease { id, holder, .. } => match holder {
                Some(holder) => write!(f, "active lease {id} of {holder}"),
                None => write!(f, "active lease {id}"),
            },
            Blocker::Exec {
                container,
                exec,
                status: ExecStatus::Created,
            } => write!(f, "exec created: {container}/{exec}"),
            Blocker::Exec {
                container, exec, ..
            } => write!(f, "exec running: {container}/{exec}"),
            Blocker::ExecStarting => f.write_str("exec being created"),
            Blocker::ContainerCreating => f.write_str("container being created"),
            Blocker::WakeWaiter { .. } => f.write_str("wake waiter"),
            Blocker::Waking => f.write_str("the pod is waking"),
            Blocker::Admission { reason } => write!(f, "wake refused: {reason}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reasons_serialize_with_a_kind() {
        let cases = [
            (Reason::Api, r#"{"kind":"api"}"#),
            (Reason::ContainerCreate, r#"{"kind":"container-create"}"#),
            (
                Reason::Traffic {
                    from: "10.42.0.7".into(),
                    port: 8080,
                },
                r#"{"kind":"traffic","from":"10.42.0.7","port":8080}"#,
            ),
            (
                Reason::Aws {
                    queue: "orders".into(),
                },
                r#"{"kind":"aws","queue":"orders"}"#,
            ),
        ];
        for (reason, json) in cases {
            assert_eq!(serde_json::to_string(&reason).unwrap(), json);
            assert_eq!(serde_json::from_str::<Reason>(json).unwrap(), reason);
            assert!(json.contains(&format!(r#""kind":"{}""#, reason.kind())));
        }
    }

    #[test]
    fn states_serialize_in_kebab_case() {
        assert_eq!(
            serde_json::to_string(&PodState::Asleep).unwrap(),
            r#""asleep""#
        );
        assert_eq!(PodState::Waking.to_string(), "waking");
    }
}
