//! The front shim's connection to the node service.
//!
//! One connection, made again with a backoff from 50 ms to 1 s whenever it
//! breaks. Each connection opens with a Hello that carries the task table, and
//! then sends again every request that has no answer yet: requests are
//! idempotent (see [`crate::proto`]). Exits that the node service reports go
//! to the front shim's table, and are acked once it has them.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use log::{debug, info, warn};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;

use crate::proto::{
    read_frame, write_frame, Code, Exit, Failure, FromNode, Op, Outcome, ProcId, TaskInfo, ToNode,
    VERSION,
};
use crate::sync::lock;

const BACKOFF_MIN: Duration = Duration::from_millis(50);
const BACKOFF_MAX: Duration = Duration::from_secs(1);

/// What the connection needs from the front shim.
#[async_trait]
pub trait Front: Send + Sync + 'static {
    /// The task table, for a Hello.
    async fn tasks(&self) -> Vec<TaskInfo>;

    /// A process exited. The node service repeats an exit until this returns.
    async fn exited(&self, pod: String, proc: ProcId, exit: Exit);
}

/// How long a call waits for an answer.
#[derive(Debug, Clone, Copy)]
pub struct Wait {
    /// When the call gives up, connected or not.
    pub deadline: Option<Instant>,
    /// How long it waits while the node service is unreachable.
    pub outage: Duration,
}

impl Wait {
    pub fn until(deadline: Instant) -> Wait {
        Wait {
            deadline: Some(deadline),
            outage: Duration::MAX,
        }
    }

    pub fn outage(outage: Duration) -> Wait {
        Wait {
            deadline: None,
            outage,
        }
    }
}

#[derive(Clone)]
pub struct NodeClient {
    inner: Arc<Inner>,
}

struct Inner {
    socket: PathBuf,
    namespace: String,
    next_id: AtomicU64,
    state: Mutex<State>,
    connected: watch::Sender<bool>,
    /// Sessions the node service welcomed.
    #[cfg(test)]
    sessions: AtomicU64,
}

#[derive(Default)]
struct State {
    pending: HashMap<u64, Pending>,
    /// The open connection's outgoing frames.
    writer: Option<mpsc::UnboundedSender<ToNode>>,
}

struct Pending {
    op: Op,
    reply: oneshot::Sender<Outcome>,
}

/// Removes a call from the pending calls when it ends, also when its caller
/// gives up on it.
struct PendingGuard<'a> {
    inner: &'a Inner,
    id: u64,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        lock(&self.inner.state).pending.remove(&self.id);
    }
}

impl NodeClient {
    /// A client of the node service at `socket`, for containerd namespace
    /// `namespace`. It connects once [`NodeClient::run`] runs.
    pub fn new(socket: PathBuf, namespace: String) -> NodeClient {
        NodeClient {
            inner: Arc::new(Inner {
                socket,
                namespace,
                next_id: AtomicU64::new(1),
                state: Mutex::new(State::default()),
                connected: watch::channel(false).0,
                #[cfg(test)]
                sessions: AtomicU64::new(0),
            }),
        }
    }

    /// Keep a connection to the node service while `front` exists.
    pub fn run(&self, front: Weak<dyn Front>) {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let mut backoff = BACKOFF_MIN;
            while front.strong_count() > 0 {
                match UnixStream::connect(&inner.socket).await {
                    Ok(stream) => {
                        match crate::panics::catch(session(&inner, stream, &front)).await {
                            Ok(Ok(())) => backoff = BACKOFF_MIN,
                            Ok(Err(e)) => warn!("open a session with the node service: {e}"),
                            Err(panic) => {
                                warn!("the session with the node service panicked: {panic}")
                            }
                        }
                        lock(&inner.state).writer = None;
                        inner.connected.send_replace(false);
                    }
                    Err(e) => debug!("connect to {}: {e}", inner.socket.display()),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(BACKOFF_MAX);
            }
        });
    }

    /// Whether a session with the node service is open.
    #[cfg(test)]
    pub fn is_connected(&self) -> bool {
        *self.inner.connected.borrow()
    }

    /// How many sessions the node service has welcomed.
    #[cfg(test)]
    pub fn sessions(&self) -> u64 {
        self.inner.sessions.load(Ordering::SeqCst)
    }

    /// Send `op` and wait for its outcome, at most as `wait` says. A call that
    /// gives up answers [`Code::Unavailable`].
    pub async fn call(&self, op: Op, wait: Wait) -> Outcome {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (reply, mut answer) = oneshot::channel();
        {
            let mut state = lock(&self.inner.state);
            if let Some(writer) = &state.writer {
                let _ = writer.send(ToNode::Request { id, op: op.clone() });
            }
            state.pending.insert(id, Pending { op, reply });
        }
        let _guard = PendingGuard {
            inner: &self.inner,
            id,
        };
        let mut connected = self.inner.connected.subscribe();
        let mut outage_since: Option<Instant> = None;
        loop {
            if *connected.borrow_and_update() {
                outage_since = None;
            } else if outage_since.is_none() {
                outage_since = Some(Instant::now());
            }
            let outage_end = outage_since.and_then(|since| since.checked_add(wait.outage));
            let limit = match (wait.deadline, outage_end) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            let give_up = async {
                match limit {
                    Some(limit) => tokio::time::sleep_until(limit).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                outcome = &mut answer => {
                    return outcome.unwrap_or_else(|_| {
                        Err(Failure::new(Code::Internal, "the call was dropped"))
                    });
                }
                changed = connected.changed() => {
                    if changed.is_err() {
                        return Err(Failure::new(Code::Internal, "the client is gone"));
                    }
                }
                () = give_up => {
                    let message = if *connected.borrow() {
                        "the node service did not answer in time"
                    } else {
                        "the node service is unavailable"
                    };
                    return Err(Failure::new(Code::Unavailable, message));
                }
            }
        }
    }
}

/// One connection: Hello, then requests out and answers and exits in, until
/// it breaks. An error means that the node service did not welcome it.
async fn session(inner: &Inner, stream: UnixStream, front: &Weak<dyn Front>) -> Result<(), String> {
    let (mut rd, mut wr) = stream.into_split();
    let Some(tasks) = (match front.upgrade() {
        Some(front) => Some(front.tasks().await),
        None => None,
    }) else {
        return Ok(());
    };
    let hello = ToNode::Hello {
        version: VERSION,
        namespace: inner.namespace.clone(),
        tasks,
    };
    write_frame(&mut wr, &hello)
        .await
        .map_err(|e| format!("send Hello: {e}"))?;
    match read_frame::<_, FromNode>(&mut rd).await {
        Ok(Some(FromNode::Welcome { version })) => {
            info!("session with the node service open (protocol {version})")
        }
        Ok(Some(FromNode::Refused { message })) => {
            return Err(format!("the node service refused the session: {message}"))
        }
        Ok(Some(other)) => return Err(format!("the node service answered Hello with {other:?}")),
        Ok(None) => return Err("the node service closed the connection".into()),
        Err(e) => return Err(format!("read the answer to Hello: {e}")),
    }

    let (tx, mut rx) = mpsc::unbounded_channel::<ToNode>();
    let writer = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            if let Err(e) = write_frame(&mut wr, &frame).await {
                debug!("write to the node service: {e}");
                break;
            }
        }
    });
    {
        let mut state = lock(&inner.state);
        for (id, pending) in &state.pending {
            let _ = tx.send(ToNode::Request {
                id: *id,
                op: pending.op.clone(),
            });
        }
        state.writer = Some(tx.clone());
    }
    #[cfg(test)]
    inner.sessions.fetch_add(1, Ordering::SeqCst);
    inner.connected.send_replace(true);

    loop {
        match read_frame::<_, FromNode>(&mut rd).await {
            Ok(Some(FromNode::Response { id, answer })) => {
                if let Some(pending) = lock(&inner.state).pending.remove(&id) {
                    let _ = pending.reply.send(answer.into());
                }
            }
            Ok(Some(FromNode::Exit { pod, proc, exit })) => {
                let Some(front) = front.upgrade() else {
                    break;
                };
                front.exited(pod.clone(), proc.clone(), exit).await;
                let _ = tx.send(ToNode::Ack { pod, proc });
            }
            Ok(Some(_)) => {}
            Ok(None) => {
                warn!("the node service closed the session");
                break;
            }
            Err(e) => {
                warn!("read from the node service: {e}");
                break;
            }
        }
    }
    writer.abort();
    Ok(())
}
