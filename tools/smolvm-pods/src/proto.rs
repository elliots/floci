//! The internal protocol between the front shim and the node service.
//!
//! The front shim keeps one connection to the node service's socket. Each
//! frame is a 4-byte big-endian length and a JSON body, as in the guest
//! agent's protocol. The first frame of a connection says what it is:
//!
//! - [`ToNode::Hello`] opens a session for a containerd namespace. It carries
//!   the front shim's task table, so that the node service can resync after a
//!   restart of either side. The node service answers [`FromNode::Welcome`],
//!   or [`FromNode::Refused`] and closes. Then the front shim sends requests,
//!   each with an id, and the node service sends responses with that id, and
//!   notifications such as [`FromNode::Exit`], which the front shim acks.
//! - [`ToNode::Call`] is a single request, such as the one the shim's
//!   `delete` action sends, answered by one response.
//!
//! Every mutating request is idempotent by its [`OpKey`], so that a request
//! repeated after a reconnect does no harm. The node service accepts protocol
//! versions [`MIN_VERSION`] to [`VERSION`].

use std::io;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub use crate::backend::{ExitInfo as Exit, StatsSample, Stdio};

/// The version this binary speaks.
pub const VERSION: u32 = 1;
/// The oldest front-shim version the node service accepts. The front shim is
/// replaced only when the node restarts, the node service at its own restart.
pub const MIN_VERSION: u32 = 1;

/// The node service's socket.
pub const NODE_SOCKET: &str = "/run/smolvm-pods/node.sock";

/// The largest frame either side accepts: a Hello of thousands of tasks fits.
pub const MAX_FRAME: usize = 64 << 20;

/// Whether `id`, a containerd id, can name a file: containerd's ids are
/// letters, digits, `_`, `.` and `-`.
pub fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 200
        && !id.starts_with(['.', '-'])
        && id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'.' | b'-'))
}

/// A process of a task: the container (or sandbox) id and the exec id, which
/// is empty for the container's init process.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ProcId {
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub exec: String,
}

impl ProcId {
    pub fn init(id: &str) -> Self {
        ProcId {
            id: id.to_string(),
            exec: String::new(),
        }
    }

    pub fn new(id: &str, exec: &str) -> Self {
        ProcId {
            id: id.to_string(),
            exec: exec.to_string(),
        }
    }

    /// The exec id as the backend takes it.
    pub fn exec_opt(&self) -> Option<&str> {
        (!self.exec.is_empty()).then_some(self.exec.as_str())
    }
}

impl std::fmt::Display for ProcId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.exec.is_empty() {
            write!(f, "{}", self.id)
        } else {
            write!(f, "{}/{}", self.id, self.exec)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Sandbox,
    Container,
    Exec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Created,
    Running,
    Stopped,
}

/// One of containerd's rootfs mounts for a container.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mount {
    #[serde(rename = "type")]
    pub kind: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub target: String,
    #[serde(default)]
    pub options: Vec<String>,
}

/// A process in the front shim's task table, as Hello reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskInfo {
    /// The sandbox id of the task's pod.
    pub pod: String,
    pub proc: ProcId,
    pub kind: Kind,
    #[serde(default)]
    pub bundle: String,
    /// The pod's network namespace, for a sandbox.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub netns: Option<String>,
    #[serde(default)]
    pub stdio: Stdio,
    pub pid: u32,
    pub status: Status,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit: Option<Exit>,
}

/// A request to the node service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    CreateSandbox {
        pod: String,
        bundle: String,
        #[serde(default)]
        netns: Option<String>,
    },
    CreateContainer {
        pod: String,
        id: String,
        bundle: String,
        #[serde(default)]
        rootfs: Vec<Mount>,
        #[serde(default)]
        stdio: Stdio,
    },
    /// A container with no CRI annotations, as a pod of its own: the pod's
    /// VM, with the container in it under the pod's id.
    CreateStandalone {
        pod: String,
        bundle: String,
        #[serde(default)]
        rootfs: Vec<Mount>,
        #[serde(default)]
        stdio: Stdio,
    },
    Exec {
        pod: String,
        id: String,
        exec: String,
        /// The OCI process, as JSON.
        spec: String,
        #[serde(default)]
        stdio: Stdio,
    },
    Start {
        pod: String,
        proc: ProcId,
    },
    Kill {
        pod: String,
        proc: ProcId,
        signal: u32,
        #[serde(default)]
        all: bool,
    },
    CloseIo {
        pod: String,
        proc: ProcId,
    },
    ResizePty {
        pod: String,
        proc: ProcId,
        width: u32,
        height: u32,
    },
    Delete {
        pod: String,
        proc: ProcId,
    },
    Pids {
        pod: String,
        id: String,
    },
    Stats {
        pod: String,
        id: String,
    },
    /// Delete the sandbox and its VM, whether or not the node service knows
    /// it: the shim's `delete` action sends it when containerd ends a sandbox
    /// task.
    DropSandbox {
        pod: String,
        #[serde(default)]
        netns: Option<String>,
    },
}

/// What makes a mutating request idempotent: the task, the exec and the
/// operation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OpKey {
    pub pod: String,
    pub proc: ProcId,
    pub op: &'static str,
}

impl Op {
    pub fn name(&self) -> &'static str {
        match self {
            Op::CreateSandbox { .. } => "create_sandbox",
            Op::CreateContainer { .. } => "create_container",
            Op::CreateStandalone { .. } => "create_standalone",
            Op::Exec { .. } => "exec",
            Op::Start { .. } => "start",
            Op::Kill { .. } => "kill",
            Op::CloseIo { .. } => "close_io",
            Op::ResizePty { .. } => "resize_pty",
            Op::Delete { .. } => "delete",
            Op::Pids { .. } => "pids",
            Op::Stats { .. } => "stats",
            Op::DropSandbox { .. } => "drop_sandbox",
        }
    }

    pub fn pod(&self) -> &str {
        match self {
            Op::CreateSandbox { pod, .. }
            | Op::CreateContainer { pod, .. }
            | Op::CreateStandalone { pod, .. }
            | Op::Exec { pod, .. }
            | Op::Start { pod, .. }
            | Op::Kill { pod, .. }
            | Op::CloseIo { pod, .. }
            | Op::ResizePty { pod, .. }
            | Op::Delete { pod, .. }
            | Op::Pids { pod, .. }
            | Op::Stats { pod, .. }
            | Op::DropSandbox { pod, .. } => pod,
        }
    }

    /// The idempotency key of a mutating request; `None` for one that only
    /// reads, or that may run any number of times (signals, resizes).
    pub fn key(&self) -> Option<OpKey> {
        let proc = match self {
            Op::CreateSandbox { pod, .. }
            | Op::CreateStandalone { pod, .. }
            | Op::DropSandbox { pod, .. } => ProcId::init(pod),
            Op::CreateContainer { id, .. } => ProcId::init(id),
            Op::Exec { id, exec, .. } => ProcId::new(id, exec),
            Op::Start { proc, .. } | Op::Delete { proc, .. } => proc.clone(),
            Op::Kill { .. }
            | Op::CloseIo { .. }
            | Op::ResizePty { .. }
            | Op::Pids { .. }
            | Op::Stats { .. } => return None,
        };
        Some(OpKey {
            pod: self.pod().to_string(),
            proc,
            op: self.name(),
        })
    }
}

/// A successful answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reply", rename_all = "snake_case")]
pub enum Reply {
    Done,
    /// The pid of a created or started process.
    Pid {
        pid: u32,
    },
    /// The answer to a kill or a delete: the process's exit, if it is known.
    /// A kill that ended a process at once, such as the nominal sandbox
    /// process, has its exit here.
    Exit {
        #[serde(default)]
        exit: Option<Exit>,
    },
    Pids {
        pids: Vec<u32>,
    },
    Stats {
        #[serde(default)]
        sample: Option<StatsSample>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Code {
    NotFound,
    AlreadyExists,
    FailedPrecondition,
    /// The node service did not answer in time.
    Unavailable,
    /// The node service failed while it handled the request, for example a
    /// panic.
    Internal,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Failure {
    pub code: Code,
    pub message: String,
}

impl Failure {
    pub fn new(code: Code, message: impl std::fmt::Display) -> Self {
        Failure {
            code,
            message: message.to_string(),
        }
    }

    pub fn unknown(message: impl std::fmt::Display) -> Self {
        Self::new(Code::Unknown, message)
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

pub type Outcome = Result<Reply, Failure>;

/// The serialized form of an [`Outcome`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Answer {
    Ok(Reply),
    Error(Failure),
}

impl From<Outcome> for Answer {
    fn from(outcome: Outcome) -> Self {
        match outcome {
            Ok(reply) => Answer::Ok(reply),
            Err(failure) => Answer::Error(failure),
        }
    }
}

impl From<Answer> for Outcome {
    fn from(answer: Answer) -> Self {
        match answer {
            Answer::Ok(reply) => Ok(reply),
            Answer::Error(failure) => Err(failure),
        }
    }
}

/// Frames from the front shim, or from the shim's `delete` action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToNode {
    /// Opens a session: the first frame of the front shim's connection.
    Hello {
        version: u32,
        namespace: String,
        tasks: Vec<TaskInfo>,
    },
    /// A connection for one request.
    Call {
        version: u32,
        id: u64,
        op: Op,
    },
    Request {
        id: u64,
        op: Op,
    },
    /// The front shim recorded this exit, so the node service forgets it.
    Ack {
        pod: String,
        proc: ProcId,
    },
}

/// Frames from the node service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FromNode {
    /// The session is open, with this protocol version.
    Welcome {
        version: u32,
    },
    /// The node service does not accept the session, and closes it.
    Refused {
        message: String,
    },
    Response {
        id: u64,
        answer: Answer,
    },
    /// A process exited. The node service repeats it at every resync until
    /// the front shim acks it.
    Exit {
        pod: String,
        proc: ProcId,
        exit: Exit,
    },
    /// A frame of a later version, which this one ignores.
    #[serde(other)]
    Unknown,
}

/// Encode one frame.
pub fn encode<T: Serialize>(message: &T) -> io::Result<Vec<u8>> {
    let body = serde_json::to_vec(message).map_err(io::Error::other)?;
    if body.len() > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame of {} bytes is too large", body.len()),
        ));
    }
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

pub async fn write_frame<W, T>(w: &mut W, message: &T) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    w.write_all(&encode(message)?).await?;
    w.flush().await
}

/// Read one frame, or `None` at the end of the stream between frames.
pub async fn read_frame<R, T>(r: &mut R) -> io::Result<Option<T>>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut len = [0u8; 4];
    match r.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame of {len} bytes is too large"),
        ));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).await?;
    serde_json::from_slice(&body)
        .map(Some)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frames_round_trip() {
        let hello = ToNode::Hello {
            version: VERSION,
            namespace: "k8s.io".into(),
            tasks: vec![TaskInfo {
                pod: "sb".into(),
                proc: ProcId::new("c", "e"),
                kind: Kind::Exec,
                bundle: String::new(),
                netns: None,
                stdio: Stdio {
                    stdout: "/run/out".into(),
                    ..Stdio::default()
                },
                pid: 1001,
                status: Status::Stopped,
                exit: Some(Exit {
                    status: 3,
                    exited_at_ns: 7,
                    oom: false,
                }),
            }],
        };
        let request = ToNode::Request {
            id: 9,
            op: Op::Kill {
                pod: "sb".into(),
                proc: ProcId::init("c"),
                signal: 15,
                all: false,
            },
        };
        let (mut a, mut b) = tokio::io::duplex(1024);
        write_frame(&mut a, &hello).await.unwrap();
        write_frame(&mut a, &request).await.unwrap();
        drop(a);
        assert_eq!(read_frame::<_, ToNode>(&mut b).await.unwrap(), Some(hello));
        assert_eq!(
            read_frame::<_, ToNode>(&mut b).await.unwrap(),
            Some(request)
        );
        assert_eq!(read_frame::<_, ToNode>(&mut b).await.unwrap(), None);
    }

    #[test]
    fn the_wire_format_is_stable() {
        let response = FromNode::Response {
            id: 4,
            answer: Answer::Ok(Reply::Pid { pid: 1000 }),
        };
        assert_eq!(
            serde_json::to_string(&response).unwrap(),
            r#"{"type":"response","id":4,"answer":{"ok":{"reply":"pid","pid":1000}}}"#
        );
        let start = Op::Start {
            pod: "sb".into(),
            proc: ProcId::init("c"),
        };
        assert_eq!(
            serde_json::to_string(&start).unwrap(),
            r#"{"op":"start","pod":"sb","proc":{"id":"c"}}"#
        );
    }

    #[test]
    fn frames_of_a_later_version_are_ignored() {
        let later = r#"{"type":"pod_state","pod":"sb","state":"asleep"}"#;
        assert_eq!(
            serde_json::from_str::<FromNode>(later).unwrap(),
            FromNode::Unknown
        );
    }

    #[test]
    fn mutating_requests_have_keys() {
        let exec = Op::Exec {
            pod: "sb".into(),
            id: "c".into(),
            exec: "e".into(),
            spec: "{}".into(),
            stdio: Stdio::default(),
        };
        let key = exec.key().unwrap();
        assert_eq!((key.pod.as_str(), key.op), ("sb", "exec"));
        assert_eq!(key.proc, ProcId::new("c", "e"));
        let pids = Op::Pids {
            pod: "sb".into(),
            id: "c".into(),
        };
        assert_eq!(pids.key(), None);
    }

    #[test]
    fn ids_that_name_files() {
        assert!(valid_id("2f9c8d_.-x"));
        for bad in ["-x", ".x", "..", "a/b", ""] {
            assert!(!valid_id(bad), "{bad:?}");
        }
    }

    #[tokio::test]
    async fn an_oversized_frame_is_refused() {
        let (mut a, mut b) = tokio::io::duplex(64);
        a.write_all(&((MAX_FRAME as u32) + 1).to_be_bytes())
            .await
            .unwrap();
        let err = read_frame::<_, ToNode>(&mut b).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
