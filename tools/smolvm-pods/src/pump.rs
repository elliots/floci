//! The output pump of each pod process: while the pod is awake, a thread
//! that owns the process's streaming agent connection and copies its output
//! to containerd's fifos. It reattaches after a broken connection, pauses
//! around each hibernation and waits it out without a thread, and marks where
//! the guest dropped output (see [`run_pump`]).
//!
//! Derived from smolvm's crates/smolvm-shim (Apache-2.0), with changes.

use std::collections::VecDeque;
use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use log::{debug, info, warn};
use nix::fcntl::{fcntl, FcntlArg, OFlag};
use tokio::sync::watch;

use smolvm_protocol::{encode_message, AgentRequest, AgentResponse, MAX_FRAME_SIZE};

use crate::backend::{key, now_ns, ExitInfo, Stdio};
use crate::engine::{expect_ok, pod_request, publish_unknown_exit};
use crate::files::ready;
use crate::probe::OutputCapture;
use crate::sync::lock;

/// How long a frame may stall part way, as a busy guest can make it, before
/// the pump drops the connection and attaches again.
const FRAME_STALL: Duration = Duration::from_secs(1);

/// How often a pump that waits for output, or for room in a fifo, looks at
/// its commands. It bounds how long a `Pause` waits.
const TICK: Duration = Duration::from_millis(50);

/// How long the agent may take to answer `PodStart` or `PodAttach`.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a request to the agent may take to send.
const SEND_TIMEOUT: Duration = Duration::from_secs(10);

/// How many times in a row a pump may lose its connection without receiving
/// a frame before it gives up.
const MAX_IDLE_REATTACHES: u32 = 3;

/// How many times in a row a pump tries to connect, or to attach, as smolvm's
/// `AgentClient::connect_with_retry` does: about 3 s in all.
const CONNECT_ATTEMPTS: u32 = 6;

/// The first wait after a failed connect or attach. Each next one doubles.
const RETRY_WAIT: Duration = Duration::from_millis(100);

/// Answers a [`PumpCmd::Pause`].
type Ack = SyncSender<Result<(), String>>;

/// Commands the async side sends to a process's pump, through its
/// [`PumpHandle`]. The pump owns the process's streaming agent connection.
pub(crate) enum PumpCmd {
    /// Sent at once, or after the next attach when the pump has no connection.
    Resize { cols: u16, rows: u16 },
    /// Before a pause: finish the frame being read, close the agent
    /// connection, and wait for `Resume`. `ack` gets `Ok` once the pump holds
    /// no agent connection; it never gets an error. A pump that has ended
    /// holds none either: then the send fails, or `ack` is dropped unanswered.
    Pause { ack: SyncSender<Result<(), String>> },
    /// After a wake: attach again at the cursor. A pump that is not paused
    /// ignores it.
    Resume,
    /// Keep a copy of the first output, for an exec probe's cached result.
    /// Sent before the start, so that it comes before any output.
    Capture(OutputCapture),
}

/// What the async side holds of a process's pump. A command goes to the
/// pump's thread, or, while the pump is parked, to the pump itself: a
/// `Resume` then starts a thread for it.
#[derive(Clone)]
pub(crate) struct PumpHandle {
    cmd_tx: Sender<PumpCmd>,
    parked: Arc<Mutex<Option<Parked>>>,
}

/// The thread's end of a [`PumpHandle`], for [`run_pump`].
pub(crate) struct Inbox {
    cmd_rx: Receiver<PumpCmd>,
    parked: Arc<Mutex<Option<Parked>>>,
}

/// A paused pump that has written all its output, and waits for `Resume`
/// without a thread. It keeps its fifos and its cursor, and takes in the
/// commands that come meanwhile.
struct Parked {
    pump: Pump,
    cmd_rx: Receiver<PumpCmd>,
}

/// The channel of a new pump, whose thread [`run_pump`] runs with the
/// [`Inbox`].
pub(crate) fn pump_channel() -> (PumpHandle, Inbox) {
    let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
    let parked = Arc::new(Mutex::new(None));
    let handle = PumpHandle {
        cmd_tx,
        parked: parked.clone(),
    };
    (handle, Inbox { cmd_rx, parked })
}

impl PumpHandle {
    /// Sends `cmd` to the pump. False when the pump has ended.
    ///
    /// Every command is sent under the lock in which a pump parks, so a
    /// pump never parks with a command that it has not taken in.
    pub(crate) fn send(&self, cmd: PumpCmd) -> bool {
        let mut parked = lock(&self.parked);
        let Some(mut state) = parked.take() else {
            return self.cmd_tx.send(cmd).is_ok();
        };
        if let PumpCmd::Resume = cmd {
            return self.unpark(state);
        }
        if let Some(ack) = state.pump.command(cmd) {
            let _ = ack.send(Ok(()));
        }
        *parked = Some(state);
        true
    }

    /// Starts a thread for a parked pump, which attaches at its cursor.
    fn unpark(&self, state: Parked) -> bool {
        let slot = self.parked.clone();
        let spawned = std::thread::Builder::new()
            .name(format!("pod-io-{}", state.pump.name))
            .spawn(move || {
                let Parked { pump, cmd_rx } = state;
                debug!(
                    "stdio pump for {}: resuming at frame {}",
                    pump.name, pump.cursor
                );
                drive(pump, Link::Detached, cmd_rx, &slot);
            });
        match spawned {
            Ok(_) => true,
            Err(e) => {
                warn!("resume a stdio pump: spawn its thread: {e}");
                false
            }
        }
    }
}

fn open_fifo_writer(path: &str) -> Option<File> {
    if path.is_empty() {
        return None;
    }
    // Blocks until the read end is open; the front shim holds one from the
    // task's create on, so this resolves at once. Writes do not block, so
    // that the pump keeps answering its commands while a fifo is full.
    match std::fs::OpenOptions::new().write(true).open(path) {
        Ok(f) => {
            let flags = fcntl(f.as_raw_fd(), FcntlArg::F_GETFL).map(OFlag::from_bits_truncate);
            if let Err(e) = flags.and_then(|flags| {
                fcntl(f.as_raw_fd(), FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))
            }) {
                warn!("make fifo {path} nonblocking: {e}");
            }
            Some(f)
        }
        Err(e) => {
            warn!("open fifo {path} for write: {e}");
            None
        }
    }
}

/// How a pump begins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Begin {
    /// Start the process with `PodStart`.
    Start,
    /// Attach to a process that runs already, at the frame of its cursor file:
    /// one that a node service before this one started.
    Attach,
    /// As `Attach`, but paused, which parks the pump: attach at the first
    /// `Resume`. For a process of a sleeping pod that a node service before
    /// this one started.
    Paused,
}

/// The process a pump serves, and how it begins.
pub(crate) struct PumpSpec {
    /// The guest agent's socket.
    pub socket: PathBuf,
    pub id: String,
    pub exec_id: Option<String>,
    pub stdio: Stdio,
    pub exit_tx: Arc<watch::Sender<Option<ExitInfo>>>,
    pub begin: Begin,
    /// Where the pump records its cursor, if anywhere.
    pub cursor_file: Option<PathBuf>,
}

/// Per-process stdio pump. Runs on its own blocking thread and owns the
/// process's streaming agent connection while the pod is awake. `inbox` is
/// the thread's end of the [`PumpHandle`] that [`pump_channel`] made:
///
/// ```text
///   async side ──(PumpHandle: Resize/Pause/Resume)─────────────► pump thread
///                                                                   │
///   agent Stdout/Stderr frames ◄── vsock ── PodStart or PodAttach ──┘
///        │                                     │
///        ▼                                     ▼
///   stdout/stderr fifos                Exited → watch<ExitInfo>
/// ```
///
/// The pump connects, sends `PodStart`, waits for `Started`, acks `started_tx`
/// (which unblocks the Task API's `Start`), opens the fifos and enters its
/// loop. The loop drains `PumpCmd`s, writes output the fifos have not taken
/// yet, then waits up to [`TICK`] for one agent frame. Fifo writes never
/// block: a frame a full fifo has not taken waits in the pump, which reads no
/// further frame meanwhile.
///
/// - **Exit.** On `Exited` the pump writes the rest of the output, publishes
///   the exit unless one is recorded already, and ends.
/// - **Reattach.** A broken connection does not end the process in the guest,
///   so the pump attaches again with `PodAttach` after the last frame it
///   received. If that fails, or the agent reports an error, it kills the
///   process and publishes exit 255 so waiters never hang.
/// - **Pause.** At [`PumpCmd::Pause`] the pump closes the connection after the
///   frame it is reading, and acks. While paused it holds no connection,
///   publishes no exit, keeps the fifos open, and keeps the last resize for
///   the next connection. Once it has written all its output, it parks: its
///   thread ends, and the handle keeps the pump. At [`PumpCmd::Resume`] it
///   attaches again at its cursor, on a new thread if it was parked. A
///   process that exits during the pause reports its exit after the wake,
///   from the agent's journal. If the handle is dropped while the pump is
///   paused, the pump ends without an exit: the process is being deleted.
/// - **Gap.** When the agent has dropped the frames at the cursor from its
///   journal (`CURSOR_EXPIRED`), at an attach or on a stream that fell
///   behind, the pump writes a line from [`gap_marker`] and goes on at the
///   oldest frame the agent holds.
///
/// The pump records its cursor in `cursor_file` after each frame it writes to
/// a fifo. A pump of a later node service begins there ([`Begin::Attach`],
/// [`Begin::Paused`]), so that it repeats at most the frame that was being
/// written.
pub(crate) fn run_pump(spec: PumpSpec, inbox: Inbox, started_tx: SyncSender<Result<(), String>>) {
    let Inbox { cmd_rx, parked } = inbox;
    let PumpSpec {
        socket,
        id,
        exec_id,
        stdio,
        exit_tx,
        begin,
        cursor_file,
    } = spec;
    let mut pump = Pump {
        name: key(&id, exec_id.as_deref()),
        socket,
        id,
        exec_id,
        exit_tx,
        io: PumpIo::new(CursorFile::open(cursor_file.as_deref())),
        cursor: 1,
        resize: None,
    };
    let begun = match begin {
        Begin::Start => pump.start(&stdio, &cmd_rx),
        Begin::Attach => {
            pump.io.open(&stdio);
            pump.cursor = pump.io.cursor_file.load();
            info!(
                "stdio pump for {}: attaching at frame {}",
                pump.name, pump.cursor
            );
            pump.attach(&cmd_rx)
        }
        Begin::Paused => {
            pump.io.open(&stdio);
            pump.cursor = pump.io.cursor_file.load();
            debug!(
                "stdio pump for {}: paused at frame {}",
                pump.name, pump.cursor
            );
            Ok(Link::Paused)
        }
    };
    match begun {
        Ok(link) => {
            let _ = started_tx.send(Ok(()));
            pump.io.open(&stdio);
            return drive(pump, link, cmd_rx, &parked);
        }
        Err(e) if begin == Begin::Start => {
            let _ = started_tx.send(Err(e));
        }
        Err(e) => {
            warn!("stdio pump for {}: {e}", pump.name);
            pump.give_up(&cmd_rx);
        }
    }
    answer_pauses(&cmd_rx);
}

/// Runs `pump` on this thread from `link` until it ends, or until it parks
/// in `slot`.
fn drive(mut pump: Pump, link: Link, cmd_rx: Receiver<PumpCmd>, slot: &Mutex<Option<Parked>>) {
    match pump.run(link, &cmd_rx, slot) {
        Some(mut parked) => {
            debug!(
                "stdio pump for {}: parked at frame {}",
                pump.name, pump.cursor
            );
            *parked = Some(Parked { pump, cmd_rx });
        }
        None => answer_pauses(&cmd_rx),
    }
}

/// A pause that came as the pump ended finds it without a connection.
fn answer_pauses(cmd_rx: &Receiver<PumpCmd>) {
    while let Ok(cmd) = cmd_rx.try_recv() {
        if let PumpCmd::Pause { ack } = cmd {
            let _ = ack.send(Ok(()));
        }
    }
}

/// The line a pump writes for the frames from `from` up to `to` (exclusive),
/// which the guest dropped from its journal before the pump read them.
fn gap_marker(from: u64, to: u64) -> String {
    format!(
        "smolvm-pods: output lost: {} frames ({from} to {}) fell out of the guest's journal \
         before they reached the log\n",
        to - from,
        to - 1
    )
}

/// What a pump holds between steps.
enum Link {
    Attached(Conn),
    /// The connection ended: attach again at the cursor.
    Detached,
    /// Paused: no connection until `Resume`.
    Paused,
}

/// How streaming on one connection ended.
enum End {
    Exited(ExitInfo),
    Pause(Ack),
    /// The pump fell behind the agent's journal, which now begins at this
    /// cursor.
    Expired(u64),
    /// The connection broke. The process may still be running.
    Lost(String),
    /// The agent reported an error for the process.
    Failed(String),
}

/// How a paused pump's wait ended.
enum Paused<'s> {
    Resume,
    /// The async side dropped the pump's handle.
    Dropped,
    /// Nothing is left to write and no command waits: the lock on the slot
    /// in which the pump parks.
    Park(MutexGuard<'s, Option<Parked>>),
}

/// Why an attach failed.
enum AttachError {
    /// The agent dropped the frame at the cursor; its journal begins here.
    Expired(u64),
    /// The agent refused the attach.
    Refused(String),
    /// The connection failed before the agent answered.
    Lost(String),
}

struct Pump {
    name: String,
    socket: PathBuf,
    id: String,
    exec_id: Option<String>,
    exit_tx: Arc<watch::Sender<Option<ExitInfo>>>,
    io: PumpIo,
    /// The frames received, as `PodAttach` counts them.
    cursor: u64,
    /// A resize for the next connection.
    resize: Option<(u16, u16)>,
}

impl Pump {
    /// Start the process with `PodStart`. If a node service before this one
    /// sent `PodStart` and stopped before the agent answered, the process runs
    /// already, and the pump attaches to it.
    fn start(&mut self, stdio: &Stdio, cmd_rx: &Receiver<PumpCmd>) -> Result<Link, String> {
        let mut conn = Conn::connect(&self.socket, CONNECT_ATTEMPTS)?;
        conn.send(&AgentRequest::PodStart {
            id: self.id.clone(),
            exec_id: self.exec_id.clone(),
        })?;
        match conn.answer("PodStart")? {
            AgentResponse::Started => {
                self.cursor = 1; // `Started`
                self.io.cursor_file.store(1);
                Ok(Link::Attached(conn))
            }
            AgentResponse::Error { message, .. } if message.contains("already started") => {
                self.io.open(stdio);
                self.cursor = self.io.cursor_file.load();
                self.attach(cmd_rx)
            }
            AgentResponse::Error { message, .. } => Err(format!("PodStart: {message}")),
            other => Err(format!("PodStart: unexpected response {other:?}")),
        }
    }

    /// Pump until the process exits, the pump gives up, or the async side
    /// drops the pump's handle while it is paused: then `None`. Or until the
    /// pump parks: then the lock on `slot`, in which the caller parks it.
    fn run<'s>(
        &mut self,
        mut link: Link,
        cmd_rx: &Receiver<PumpCmd>,
        slot: &'s Mutex<Option<Parked>>,
    ) -> Option<MutexGuard<'s, Option<Parked>>> {
        let mut idle_reattaches = 0;
        loop {
            link = match link {
                Link::Attached(mut conn) => {
                    let before = self.cursor;
                    let end = self.stream(&mut conn, cmd_rx);
                    drop(conn);
                    match end {
                        End::Exited(exit) => {
                            self.exited(exit, cmd_rx);
                            return None;
                        }
                        End::Pause(ack) => {
                            debug!(
                                "stdio pump for {}: paused at frame {}",
                                self.name, self.cursor
                            );
                            let _ = ack.send(Ok(()));
                            Link::Paused
                        }
                        End::Expired(oldest) => match self.skip_to(oldest) {
                            Ok(()) => Link::Detached,
                            Err(e) => {
                                warn!("stdio pump for {}: {e}", self.name);
                                self.give_up(cmd_rx);
                                return None;
                            }
                        },
                        End::Lost(e) => {
                            idle_reattaches = if self.cursor > before {
                                1
                            } else {
                                idle_reattaches + 1
                            };
                            if idle_reattaches > MAX_IDLE_REATTACHES {
                                warn!("stdio pump for {}: {e}", self.name);
                                self.give_up(cmd_rx);
                                return None;
                            }
                            info!(
                                "stdio pump for {}: {e}; reattaching at frame {}",
                                self.name, self.cursor
                            );
                            Link::Detached
                        }
                        End::Failed(e) => {
                            warn!("stdio pump for {}: {e}", self.name);
                            self.give_up(cmd_rx);
                            return None;
                        }
                    }
                }
                Link::Detached => match self.attach(cmd_rx) {
                    Ok(link) => link,
                    Err(e) => {
                        warn!("stdio pump for {}: {e}", self.name);
                        self.give_up(cmd_rx);
                        return None;
                    }
                },
                Link::Paused => match self.paused(cmd_rx, slot) {
                    Paused::Resume => {
                        debug!(
                            "stdio pump for {}: resuming at frame {}",
                            self.name, self.cursor
                        );
                        idle_reattaches = 0;
                        Link::Detached
                    }
                    Paused::Dropped => {
                        debug!("stdio pump for {}: ended while paused", self.name);
                        return None;
                    }
                    Paused::Park(parked) => return Some(parked),
                },
            };
        }
    }

    /// Attach again at the cursor. A connection that fails is tried again
    /// after a wait, in which the pump pauses when told to. An expired cursor
    /// gets a gap marker.
    fn attach(&mut self, cmd_rx: &Receiver<PumpCmd>) -> Result<Link, String> {
        let mut attempt = 1;
        let mut wait = RETRY_WAIT;
        loop {
            match self.try_attach() {
                Ok(conn) => return Ok(Link::Attached(conn)),
                Err(AttachError::Expired(oldest)) => self.skip_to(oldest)?,
                Err(AttachError::Refused(e)) => return Err(e),
                Err(AttachError::Lost(e)) => {
                    if attempt >= CONNECT_ATTEMPTS {
                        return Err(e);
                    }
                    attempt += 1;
                    debug!(
                        "stdio pump for {}: {e}; trying again in {wait:?}",
                        self.name
                    );
                    if let Some(ack) = self.wait_detached(cmd_rx, wait) {
                        debug!(
                            "stdio pump for {}: paused at frame {}",
                            self.name, self.cursor
                        );
                        let _ = ack.send(Ok(()));
                        return Ok(Link::Paused);
                    }
                    wait *= 2;
                }
            }
        }
    }

    fn try_attach(&mut self) -> Result<Conn, AttachError> {
        let mut conn = Conn::connect(&self.socket, 1).map_err(AttachError::Lost)?;
        conn.send(&AgentRequest::PodAttach {
            id: self.id.clone(),
            exec_id: self.exec_id.clone(),
            cursor: self.cursor,
        })
        .map_err(AttachError::Lost)?;
        let answer = match conn.recv(ANSWER_TIMEOUT) {
            Ok(Some(answer)) => answer,
            // An agent that does not answer at all is not tried again.
            Ok(None) => {
                let e = format!("recv PodAttach: no answer in {ANSWER_TIMEOUT:?}");
                return Err(AttachError::Refused(e));
            }
            Err(e) => return Err(AttachError::Lost(format!("recv PodAttach: {e}"))),
        };
        if let Some(oldest) = answer.expired_cursor_oldest() {
            return Err(AttachError::Expired(oldest));
        }
        match answer {
            AgentResponse::Ok { .. } => Ok(conn),
            AgentResponse::Error { message, .. } => {
                Err(AttachError::Refused(format!("PodAttach: {message}")))
            }
            other => Err(AttachError::Refused(format!(
                "PodAttach: unexpected response {other:?}"
            ))),
        }
    }

    /// The agent dropped the frames from the cursor up to `oldest` before the
    /// pump read them: write the gap marker, and go on at `oldest`.
    fn skip_to(&mut self, oldest: u64) -> Result<(), String> {
        if oldest <= self.cursor {
            return Err(format!(
                "the agent expired cursor {} but holds frames from {oldest} on",
                self.cursor
            ));
        }
        warn!(
            "stdio pump for {}: the guest dropped frames {} to {} before the pump read them",
            self.name,
            self.cursor,
            oldest - 1
        );
        self.io.gap(self.cursor, oldest);
        self.cursor = oldest;
        Ok(())
    }

    /// Pump one connection until the process exits, the pump pauses, or the
    /// connection ends.
    fn stream(&mut self, conn: &mut Conn, cmd_rx: &Receiver<PumpCmd>) -> End {
        loop {
            // 1) Commands from the async side. A closed channel means the
            // backend entry was deleted: keep pumping output until the
            // process exits.
            while let Ok(cmd) = cmd_rx.try_recv() {
                if let Some(ack) = self.command(cmd) {
                    return End::Pause(ack);
                }
            }
            if let Some((cols, rows)) = self.resize {
                if let Err(e) = conn.send(&AgentRequest::Resize { cols, rows }) {
                    return End::Lost(e);
                }
                self.resize = None;
            }

            // 2) Output the fifos have not taken yet.
            if !self.io.flush() {
                self.io.wait_for_room(TICK);
                continue;
            }

            // 3) One agent frame.
            let frame = match conn.recv(TICK) {
                Ok(Some(frame)) => frame,
                Ok(None) => continue,
                Err(e) => return End::Lost(format!("streaming connection lost: {e}")),
            };
            if let Some(oldest) = frame.expired_cursor_oldest() {
                return End::Expired(oldest);
            }
            self.cursor += 1;
            match frame {
                AgentResponse::Stdout { data } => self.io.push(Stream::Stdout, data, self.cursor),
                AgentResponse::Stderr { data } => self.io.push(Stream::Stderr, data, self.cursor),
                AgentResponse::Exited { exit_code, oom } => {
                    return End::Exited(ExitInfo {
                        status: exit_code as u32,
                        exited_at_ns: now_ns(),
                        oom,
                    });
                }
                AgentResponse::Error { message, .. } => {
                    return End::Failed(format!("agent error: {message}"));
                }
                _ => self.io.push(Stream::Stdout, Vec::new(), self.cursor),
            }
            self.io.flush();
        }
    }

    /// Take in a command. A resize waits for the stream; a pause returns its
    /// ack.
    fn command(&mut self, cmd: PumpCmd) -> Option<Ack> {
        match cmd {
            PumpCmd::Resize { cols, rows } => self.resize = Some((cols, rows)),
            PumpCmd::Pause { ack } => return Some(ack),
            PumpCmd::Resume => {}
            PumpCmd::Capture(capture) => self.io.capture = Some(capture),
        }
        None
    }

    /// Wait without a connection up to `wait`, taking in commands. Returns the
    /// ack of a `Pause` as soon as one comes.
    fn wait_detached(&mut self, cmd_rx: &Receiver<PumpCmd>, wait: Duration) -> Option<Ack> {
        let deadline = Instant::now() + wait;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return None;
            }
            match cmd_rx.recv_timeout(left) {
                Ok(cmd) => {
                    if let Some(ack) = self.command(cmd) {
                        return Some(ack);
                    }
                }
                Err(RecvTimeoutError::Timeout) => return None,
                Err(RecvTimeoutError::Disconnected) => {
                    std::thread::sleep(left);
                    return None;
                }
            }
        }
    }

    /// Wait without a connection for `Resume`, writing the output the fifos
    /// had not taken. Once all of it is written and no command waits, the
    /// pump parks.
    fn paused<'s>(
        &mut self,
        cmd_rx: &Receiver<PumpCmd>,
        slot: &'s Mutex<Option<Parked>>,
    ) -> Paused<'s> {
        loop {
            let cmd = if self.io.flush() {
                let parked = lock(slot);
                match cmd_rx.try_recv() {
                    Ok(cmd) => cmd,
                    Err(TryRecvError::Empty) => return Paused::Park(parked),
                    Err(TryRecvError::Disconnected) => return Paused::Dropped,
                }
            } else {
                self.io.wait_for_room(TICK);
                match cmd_rx.try_recv() {
                    Ok(cmd) => cmd,
                    Err(TryRecvError::Empty) => continue,
                    Err(TryRecvError::Disconnected) => return Paused::Dropped,
                }
            };
            match cmd {
                PumpCmd::Resume => return Paused::Resume,
                cmd => {
                    if let Some(ack) = self.command(cmd) {
                        let _ = ack.send(Ok(()));
                    }
                }
            }
        }
    }

    /// Write all the output still queued. The pump holds no connection, so it
    /// answers a pause at once.
    fn drain(&mut self, cmd_rx: &Receiver<PumpCmd>) {
        while !self.io.flush() {
            if let Ok(PumpCmd::Pause { ack }) = cmd_rx.try_recv() {
                let _ = ack.send(Ok(()));
            }
            self.io.wait_for_room(TICK);
        }
    }

    /// The process exited: write its output, then publish the exit, unless
    /// one is recorded already.
    fn exited(&mut self, exit: ExitInfo, cmd_rx: &Receiver<PumpCmd>) {
        self.drain(cmd_rx);
        self.exit_tx.send_if_modified(|recorded| {
            let first = recorded.is_none();
            if first {
                *recorded = Some(exit);
            }
            first
        });
    }

    /// The connection is gone for good without a real exit: end the process,
    /// which would otherwise run on unseen, and publish exit 255 so that
    /// waiters never hang.
    fn give_up(&mut self, cmd_rx: &Receiver<PumpCmd>) {
        if self.exit_tx.borrow().is_none() {
            let signal = AgentRequest::PodSignal {
                id: self.id.clone(),
                exec_id: self.exec_id.clone(),
                signal: 9,
                all: true,
            };
            if let Err(e) =
                pod_request(&self.socket, &signal).and_then(|r| expect_ok(r, "PodSignal"))
            {
                warn!("stdio pump for {}: kill the process: {e}", self.name);
            }
        }
        self.drain(cmd_rx);
        publish_unknown_exit(&self.exit_tx);
    }
}

/// A pump's streaming connection to the guest agent, in the agent's framing:
/// a 4-byte big-endian length and a JSON body. The pump owns the socket, so
/// that it can wait for output and still look at its commands.
struct Conn {
    stream: UnixStream,
}

impl Conn {
    /// Connect, at most `attempts` times while the errors are transient.
    fn connect(socket: &Path, attempts: u32) -> Result<Conn, String> {
        let mut wait = RETRY_WAIT;
        let mut attempt = 1;
        let stream = loop {
            match UnixStream::connect(socket) {
                Ok(stream) => break stream,
                Err(e)
                    if attempt < attempts
                        && matches!(
                            e.kind(),
                            ErrorKind::ConnectionRefused
                                | ErrorKind::ConnectionReset
                                | ErrorKind::NotFound
                                | ErrorKind::BrokenPipe
                                | ErrorKind::WouldBlock
                        ) =>
                {
                    std::thread::sleep(wait);
                    wait = (wait * 2).min(Duration::from_secs(2));
                    attempt += 1;
                }
                Err(e) => return Err(format!("connect to sandbox agent: {e}")),
            }
        };
        stream
            .set_read_timeout(Some(FRAME_STALL))
            .and_then(|()| stream.set_write_timeout(Some(SEND_TIMEOUT)))
            .map_err(|e| format!("set the agent socket's timeouts: {e}"))?;
        Ok(Conn { stream })
    }

    fn send(&mut self, request: &AgentRequest) -> Result<(), String> {
        let frame = encode_message(request)
            .map_err(|e| format!("encode {}: {e}", request.log_summary()))?;
        self.stream
            .write_all(&frame)
            .map_err(|e| format!("send {}: {e}", request.log_summary()))
    }

    /// The next frame, or `None` when none begins within `wait`. A frame that
    /// stalls part way for [`FRAME_STALL`] is an error.
    fn recv(&mut self, wait: Duration) -> std::io::Result<Option<AgentResponse>> {
        if !ready(self.stream.as_raw_fd(), libc::POLLIN, wait)? {
            return Ok(None);
        }
        let mut len = [0u8; 4];
        self.read_part(&mut len)?;
        let len = u32::from_be_bytes(len);
        if len > MAX_FRAME_SIZE {
            let e = format!("a frame of {len} bytes is too large");
            return Err(std::io::Error::new(ErrorKind::InvalidData, e));
        }
        let mut body = vec![0u8; len as usize];
        self.read_part(&mut body)?;
        serde_json::from_slice(&body)
            .map(Some)
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))
    }

    fn read_part(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.stream.read_exact(buf).map_err(|e| match e.kind() {
            ErrorKind::WouldBlock | ErrorKind::TimedOut => std::io::Error::new(
                ErrorKind::TimedOut,
                format!("a frame stalled part way for {FRAME_STALL:?}"),
            ),
            ErrorKind::UnexpectedEof => {
                std::io::Error::new(ErrorKind::UnexpectedEof, "the agent closed the connection")
            }
            _ => e,
        })
    }

    /// The agent's answer to a request.
    fn answer(&mut self, what: &str) -> Result<AgentResponse, String> {
        match self.recv(ANSWER_TIMEOUT) {
            Ok(Some(answer)) => Ok(answer),
            Ok(None) => Err(format!("recv {what}: no answer in {ANSWER_TIMEOUT:?}")),
            Err(e) => Err(format!("recv {what}: {e}")),
        }
    }
}

/// A pump's cursor, on tmpfs: the frames it received and wrote. It survives a
/// restart of the node service, not of the node. Without a file a pump begins
/// at frame 1, after `Started`.
struct CursorFile {
    file: Option<File>,
}

impl CursorFile {
    fn open(path: Option<&Path>) -> CursorFile {
        let file = path.and_then(|path| {
            if let Some(dir) = path.parent() {
                let _ = std::fs::create_dir_all(dir);
            }
            let opened = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path);
            opened
                .map_err(|e| warn!("open the pump cursor {}: {e}", path.display()))
                .ok()
        });
        CursorFile { file }
    }

    fn load(&self) -> u64 {
        let mut bytes = [0u8; 8];
        match &self.file {
            Some(file) if file.read_exact_at(&mut bytes, 0).is_ok() => {
                u64::from_le_bytes(bytes).max(1)
            }
            _ => 1,
        }
    }

    fn store(&mut self, cursor: u64) {
        if let Some(file) = &self.file {
            if let Err(e) = file.write_all_at(&cursor.to_le_bytes(), 0) {
                debug!("store the pump cursor: {e}");
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stream {
    Stdout = 0,
    Stderr = 1,
}

/// Output received and not yet written to its fifo.
struct Output {
    stream: Stream,
    data: Vec<u8>,
    written: usize,
    /// The cursor to record once it is written.
    cursor: u64,
}

/// The process's containerd fifos, kept open across reattaches and pauses,
/// the output they have not taken yet, and the cursor file.
struct PumpIo {
    opened: bool,
    stdout: Option<File>,
    stderr: Option<File>,
    cursor_file: CursorFile,
    /// Oldest first: at most a frame, and gap markers.
    out: VecDeque<Output>,
    /// Whether the output queued last for each [`Stream`] left a line open.
    open_line: [bool; 2],
    /// A copy of the first output, for an exec probe.
    capture: Option<OutputCapture>,
}

impl PumpIo {
    fn new(cursor_file: CursorFile) -> Self {
        Self {
            opened: false,
            stdout: None,
            stderr: None,
            cursor_file,
            out: VecDeque::new(),
            open_line: [false; 2],
            capture: None,
        }
    }

    /// Open the fifos, the first time only.
    fn open(&mut self, stdio: &Stdio) {
        if std::mem::replace(&mut self.opened, true) {
            return;
        }
        self.stdout = open_fifo_writer(&stdio.stdout);
        self.stderr = open_fifo_writer(&stdio.stderr);
    }

    /// Queue output for `stream`, and the cursor to record once it is written.
    fn push(&mut self, stream: Stream, data: Vec<u8>, cursor: u64) {
        if let Some(capture) = &self.capture {
            capture.push(&data);
        }
        if let Some(&last) = data.last() {
            self.open_line[stream as usize] = last != b'\n';
        }
        self.out.push_back(Output {
            stream,
            data,
            written: 0,
            cursor,
        });
    }

    /// Queue the gap marker for the frames from `from` up to `to`, on a line
    /// of its own: on stderr, or on stdout when there is no stderr.
    fn gap(&mut self, from: u64, to: u64) {
        let stream = if self.stderr.is_some() {
            Stream::Stderr
        } else {
            Stream::Stdout
        };
        let mut line = Vec::new();
        if self.open_line[stream as usize] {
            line.push(b'\n');
        }
        line.extend_from_slice(gap_marker(from, to).as_bytes());
        self.push(stream, line, to);
    }

    /// Write what the fifos take without blocking, and record the cursor of
    /// each output written. True when nothing is left.
    fn flush(&mut self) -> bool {
        while let Some(head) = self.out.front_mut() {
            let fifo = match head.stream {
                Stream::Stdout => &mut self.stdout,
                Stream::Stderr => &mut self.stderr,
            };
            while let Some(f) = fifo.as_mut() {
                if head.written == head.data.len() {
                    break;
                }
                match f.write(&head.data[head.written..]) {
                    Ok(0) => *fifo = None,
                    Ok(n) => head.written += n,
                    Err(e) if e.kind() == ErrorKind::WouldBlock => return false,
                    Err(e) if e.kind() == ErrorKind::Interrupted => {}
                    Err(e) => {
                        // Reader side gone (EPIPE): stop writing, keep pumping.
                        debug!("write {:?} fifo: {e}", head.stream);
                        *fifo = None;
                    }
                }
            }
            self.cursor_file.store(head.cursor);
            self.out.pop_front();
        }
        true
    }

    /// Wait up to `wait` for room in the fifo of the oldest output not written.
    fn wait_for_room(&self, wait: Duration) {
        let fifo = match self.out.front().map(|o| o.stream) {
            Some(Stream::Stdout) => self.stdout.as_ref(),
            Some(Stream::Stderr) => self.stderr.as_ref(),
            None => None,
        };
        if let Some(f) = fifo {
            if let Err(e) = ready(f.as_raw_fd(), libc::POLLOUT, wait) {
                debug!("wait for room in a fifo: {e}");
                std::thread::sleep(wait);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The next request on `stream`, in the agent's framing, or `None` at
    /// its end.
    fn read_request(stream: &mut UnixStream) -> Option<AgentRequest> {
        let mut len = [0u8; 4];
        stream.read_exact(&mut len).ok()?;
        let mut body = vec![0u8; u32::from_be_bytes(len) as usize];
        stream.read_exact(&mut body).ok()?;
        serde_json::from_slice(&body).ok()
    }

    /// Writes `frame` on `stream` in the agent's framing.
    fn write_frame(stream: &mut UnixStream, frame: &AgentResponse) -> std::io::Result<()> {
        stream.write_all(&encode_message(frame).unwrap())
    }

    /// What a pump of process `c` on the agent socket `socket` needs, and the
    /// receiver of the exit it publishes.
    fn spec_for_c(
        socket: PathBuf,
        begin: Begin,
        stdio: Stdio,
        cursor_file: Option<PathBuf>,
    ) -> (PumpSpec, watch::Receiver<Option<ExitInfo>>) {
        let (exit_tx, exit_rx) = watch::channel(None);
        let spec = PumpSpec {
            socket,
            id: "c".into(),
            exec_id: None,
            stdio,
            exit_tx: Arc::new(exit_tx),
            begin,
            cursor_file,
        };
        (spec, exit_rx)
    }

    /// Runs a pump against an agent whose first connection sends `Started` and
    /// a line, then stops part way through the next frame, after `stall`
    /// bytes, so that the pump drops the connection after [`FRAME_STALL`], as
    /// with a busy guest. Returns the exit status the pump published within
    /// 10 seconds, and the output.
    fn pump_through_a_stall(stall: Vec<u8>) -> (Option<u32>, String) {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("agent.sock");
        let stdout = dir.path().join("stdout");
        std::fs::write(&stdout, b"").unwrap();
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        std::thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            let request = read_request(&mut first).unwrap();
            assert!(
                matches!(request, AgentRequest::PodStart { .. }),
                "{request:?}"
            );
            write_frame(&mut first, &AgentResponse::Started).unwrap();
            let line = b"one\n".to_vec();
            write_frame(&mut first, &AgentResponse::Stdout { data: line }).unwrap();
            first.write_all(&stall).unwrap();

            let (mut second, _) = listener.accept().unwrap();
            let request = read_request(&mut second).unwrap();
            assert!(
                matches!(request, AgentRequest::PodAttach { cursor: 2, .. }),
                "{request:?}"
            );
            write_frame(&mut second, &AgentResponse::ok(None)).unwrap();
            let line = b"two\n".to_vec();
            write_frame(&mut second, &AgentResponse::Stdout { data: line }).unwrap();
            let exited = AgentResponse::Exited {
                exit_code: 3,
                oom: false,
            };
            write_frame(&mut second, &exited).unwrap();
            drop(first);
        });

        let (_pump, inbox) = pump_channel();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let stdio = Stdio {
            stdout: stdout.display().to_string(),
            ..Stdio::default()
        };
        let (spec, exit_rx) = spec_for_c(socket, Begin::Start, stdio, None);
        std::thread::spawn(move || run_pump(spec, inbox, started_tx));
        started_rx.recv().unwrap().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while exit_rx.borrow().is_none() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let status = exit_rx.borrow().as_ref().map(|e| e.status);
        (status, std::fs::read_to_string(&stdout).unwrap())
    }

    #[test]
    fn a_pump_reattaches_when_a_frame_stalls() {
        let stalled = pump_through_a_stall(64u32.to_be_bytes().to_vec());
        assert_eq!(stalled, (Some(3), "one\ntwo\n".to_string()));
    }

    #[test]
    fn a_pump_reattaches_when_a_frame_header_stalls() {
        let stalled = pump_through_a_stall(vec![0, 0]);
        assert_eq!(stalled, (Some(3), "one\ntwo\n".to_string()));
    }

    /// A pump that cannot reattach kills the process before it reports an
    /// exit, so that the process does not run on unseen.
    #[test]
    fn a_pump_kills_and_reports_exit_255_when_it_cannot_reattach() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("agent.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let agent = std::thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            read_request(&mut first).unwrap();
            write_frame(&mut first, &AgentResponse::Started).unwrap();
            drop(first);
            let (mut second, _) = listener.accept().unwrap();
            let request = read_request(&mut second).unwrap();
            assert!(
                matches!(request, AgentRequest::PodAttach { .. }),
                "{request:?}"
            );
            let gone = AgentResponse::error("pod replay cursor outside retained journal", "GONE");
            write_frame(&mut second, &gone).unwrap();
            let (mut third, _) = listener.accept().unwrap();
            let request = read_request(&mut third).unwrap();
            assert!(
                matches!(request, AgentRequest::PodSignal { signal: 9, .. }),
                "{request:?}"
            );
            write_frame(&mut third, &AgentResponse::ok(None)).unwrap();
        });

        let (_pump, inbox) = pump_channel();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let (spec, exit_rx) = spec_for_c(socket, Begin::Start, Stdio::default(), None);
        let pump = std::thread::spawn(move || run_pump(spec, inbox, started_tx));
        started_rx.recv().unwrap().unwrap();
        pump.join().unwrap();
        agent.join().unwrap();

        assert_eq!(exit_rx.borrow().as_ref().map(|e| e.status), Some(255));
    }

    /// Runs a pump that begins as `begin` with the cursor file `cursor` and
    /// output file `stdout`, until it ends. Returns the exit it published.
    fn run_to_the_end(
        socket: PathBuf,
        begin: Begin,
        cursor: &Path,
        stdout: &Path,
    ) -> Option<ExitInfo> {
        let (_pump, inbox) = pump_channel();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let stdio = Stdio {
            stdout: stdout.display().to_string(),
            ..Stdio::default()
        };
        let (spec, exit_rx) = spec_for_c(socket, begin, stdio, Some(cursor.to_path_buf()));
        let pump = std::thread::spawn(move || run_pump(spec, inbox, started_tx));
        started_rx.recv().unwrap().unwrap();
        pump.join().unwrap();
        let exit = *exit_rx.borrow();
        exit
    }

    fn stored(cursor: &Path) -> u64 {
        u64::from_le_bytes(std::fs::read(cursor).unwrap().try_into().unwrap())
    }

    /// A pump records how many frames it wrote, and a pump that attaches to
    /// the process later goes on from there.
    #[test]
    fn a_later_pump_goes_on_at_the_recorded_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("agent.sock");
        let stdout = dir.path().join("stdout");
        let cursor = dir.path().join("cursors/sb/c");
        std::fs::write(&stdout, b"").unwrap();
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let agent = std::thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            let request = read_request(&mut first).unwrap();
            assert!(
                matches!(request, AgentRequest::PodStart { .. }),
                "{request:?}"
            );
            write_frame(&mut first, &AgentResponse::Started).unwrap();
            let line = b"one\n".to_vec();
            write_frame(&mut first, &AgentResponse::Stdout { data: line }).unwrap();
            let line = b"two\n".to_vec();
            write_frame(&mut first, &AgentResponse::Stderr { data: line }).unwrap();
            let exited = AgentResponse::Exited {
                exit_code: 0,
                oom: false,
            };
            write_frame(&mut first, &exited).unwrap();

            let (mut second, _) = listener.accept().unwrap();
            let request = read_request(&mut second).unwrap();
            assert!(
                matches!(request, AgentRequest::PodAttach { cursor: 3, .. }),
                "{request:?}"
            );
            write_frame(&mut second, &AgentResponse::ok(None)).unwrap();
            write_frame(&mut second, &exited).unwrap();
        });
        let exit = run_to_the_end(socket.clone(), Begin::Start, &cursor, &stdout);
        assert_eq!(exit.map(|e| e.status), Some(0));
        // The exit is not recorded, so that a later pump reports it again.
        assert_eq!(stored(&cursor), 3);

        let exit = run_to_the_end(socket, Begin::Attach, &cursor, &stdout);
        assert_eq!(exit.map(|e| e.status), Some(0));
        agent.join().unwrap();
        assert_eq!(std::fs::read_to_string(&stdout).unwrap(), "one\n");
    }

    /// A process that a node service before this one started has its session
    /// already, so the pump attaches to it.
    #[test]
    fn a_pump_attaches_to_a_process_started_already() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("agent.sock");
        let stdout = dir.path().join("stdout");
        let cursor = dir.path().join("cursor");
        std::fs::write(&stdout, b"").unwrap();
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let agent = std::thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            read_request(&mut first).unwrap();
            let started = AgentResponse::error("session already started; use PodAttach", "BAD");
            write_frame(&mut first, &started).unwrap();
            let (mut second, _) = listener.accept().unwrap();
            let request = read_request(&mut second).unwrap();
            assert!(
                matches!(request, AgentRequest::PodAttach { cursor: 1, .. }),
                "{request:?}"
            );
            write_frame(&mut second, &AgentResponse::ok(None)).unwrap();
            let line = b"replayed\n".to_vec();
            write_frame(&mut second, &AgentResponse::Stdout { data: line }).unwrap();
            let exited = AgentResponse::Exited {
                exit_code: 5,
                oom: false,
            };
            write_frame(&mut second, &exited).unwrap();
        });
        let exit = run_to_the_end(socket, Begin::Start, &cursor, &stdout);
        agent.join().unwrap();
        assert_eq!(exit.map(|e| e.status), Some(5));
        assert_eq!(std::fs::read_to_string(&stdout).unwrap(), "replayed\n");
    }

    // A stand-in for the guest agent, for pauses, wakes and gaps.

    /// One process's frames as the guest agent journals them, and what the
    /// stand-in saw.
    #[derive(Default)]
    struct Journal {
        /// The frames from cursor `first` on. `Started` is frame 0.
        frames: Vec<AgentResponse>,
        first: u64,
        /// While held, no frame is sent, so that a subscriber falls behind.
        held: bool,
        requests: Vec<AgentRequest>,
        /// Connections being served.
        open: usize,
        stopped: bool,
    }

    /// Serves one process's journal from any cursor, as the agent's
    /// `PodStart` and `PodAttach` do. A cursor older than the journal gets
    /// the agent's `CURSOR_EXPIRED` answer, at an attach and on a stream that
    /// fell behind.
    struct Agent {
        socket: PathBuf,
        journal: Arc<std::sync::Mutex<Journal>>,
        accepting: Option<std::thread::JoinHandle<()>>,
    }

    impl Agent {
        fn new(socket: PathBuf) -> Agent {
            let mut agent = Agent {
                socket,
                journal: Arc::default(),
                accepting: None,
            };
            agent.journal().frames.push(AgentResponse::Started);
            agent.start();
            agent
        }

        fn journal(&self) -> std::sync::MutexGuard<'_, Journal> {
            self.journal.lock().unwrap()
        }

        /// Listen on the socket, as a VM that boots or wakes does.
        fn start(&mut self) {
            self.journal().stopped = false;
            let listener = std::os::unix::net::UnixListener::bind(&self.socket).unwrap();
            let journal = self.journal.clone();
            self.accepting = Some(std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(stream) = stream else { break };
                    {
                        let mut journal = journal.lock().unwrap();
                        if journal.stopped {
                            break;
                        }
                        journal.open += 1;
                    }
                    let journal = journal.clone();
                    std::thread::spawn(move || {
                        serve(stream, &journal);
                        journal.lock().unwrap().open -= 1;
                    });
                }
            }));
        }

        /// Close every connection and remove the socket, as a VM that pauses
        /// does.
        fn stop(&mut self) {
            self.journal().stopped = true;
            let _ = UnixStream::connect(&self.socket);
            if let Some(accepting) = self.accepting.take() {
                accepting.join().unwrap();
            }
            wait_until("the agent's connections close", || self.journal().open == 0);
            std::fs::remove_file(&self.socket).unwrap();
        }

        fn output(&self, line: &str) {
            let data = line.as_bytes().to_vec();
            self.journal().frames.push(AgentResponse::Stdout { data });
        }

        fn exit(&self, exit_code: i32) {
            let exited = AgentResponse::Exited {
                exit_code,
                oom: false,
            };
            self.journal().frames.push(exited);
        }

        /// Drop the frames before `first`, as a full journal does.
        fn drop_before(&self, first: u64) {
            let mut journal = self.journal();
            let dropped = (first - journal.first) as usize;
            journal.frames.drain(..dropped);
            journal.first = first;
        }

        fn open(&self) -> usize {
            self.journal().open
        }

        /// Each request, in order.
        fn requests(&self) -> Vec<String> {
            self.journal().requests.iter().map(describe).collect()
        }

        fn attaches(&self) -> Vec<u64> {
            let journal = self.journal();
            let cursors = journal.requests.iter().filter_map(|request| match request {
                AgentRequest::PodAttach { cursor, .. } => Some(*cursor),
                _ => None,
            });
            cursors.collect()
        }
    }

    impl Drop for Agent {
        fn drop(&mut self) {
            if let Ok(mut journal) = self.journal.lock() {
                journal.stopped = true;
            }
            let _ = UnixStream::connect(&self.socket);
        }
    }

    fn describe(request: &AgentRequest) -> String {
        match request {
            AgentRequest::PodStart { .. } => "start".into(),
            AgentRequest::PodAttach { cursor, .. } => format!("attach {cursor}"),
            AgentRequest::Resize { cols, rows } => format!("resize {cols}x{rows}"),
            other => other.log_summary(),
        }
    }

    fn serve(mut stream: UnixStream, journal: &std::sync::Mutex<Journal>) {
        let Some(request) = read_request(&mut stream) else {
            return;
        };
        let (mut cursor, attach) = match &request {
            AgentRequest::PodStart { .. } => (0, false),
            AgentRequest::PodAttach { cursor, .. } => (*cursor, true),
            _ => return,
        };
        journal.lock().unwrap().requests.push(request);
        if attach {
            let first = journal.lock().unwrap().first;
            if cursor < first {
                let _ = write_frame(&mut stream, &AgentResponse::cursor_expired(cursor, first));
                return;
            }
            let _ = write_frame(&mut stream, &AgentResponse::ok(None));
        }
        loop {
            let next = {
                let journal = journal.lock().unwrap();
                if journal.stopped {
                    return;
                }
                if journal.held {
                    None
                } else if cursor < journal.first {
                    Some(AgentResponse::cursor_expired(cursor, journal.first))
                } else {
                    journal
                        .frames
                        .get((cursor - journal.first) as usize)
                        .cloned()
                }
            };
            let Some(frame) = next else {
                // Requests on the stream, or the pump closing it.
                if ready(stream.as_raw_fd(), libc::POLLIN, Duration::from_millis(5)).unwrap() {
                    match read_request(&mut stream) {
                        Some(request) => journal.lock().unwrap().requests.push(request),
                        None => return,
                    }
                }
                continue;
            };
            let last = matches!(
                frame,
                AgentResponse::Exited { .. } | AgentResponse::Error { .. }
            );
            if write_frame(&mut stream, &frame).is_err() || last {
                return;
            }
            cursor += 1;
        }
    }

    fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !done() {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting until {what}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn read(path: &Path) -> String {
        std::fs::read_to_string(path).unwrap()
    }

    /// The stdio of a process whose output goes to the files `stdout` and,
    /// with `stderr`, `stderr` in `dir`.
    fn stdio_in(dir: &Path, stderr: bool) -> Stdio {
        let file = |name: &str| {
            let path = dir.join(name);
            std::fs::write(&path, b"").unwrap();
            path.display().to_string()
        };
        Stdio {
            stdout: file("stdout"),
            stderr: if stderr {
                file("stderr")
            } else {
                String::new()
            },
            ..Stdio::default()
        }
    }

    fn mkfifo(path: &Path) {
        nix::unistd::mkfifo(path, nix::sys::stat::Mode::S_IRWXU).unwrap();
    }

    /// A pump on a thread of its own, and what the async side holds of it.
    /// `thread` is the pump's first thread.
    struct Running {
        pump: PumpHandle,
        exit_rx: watch::Receiver<Option<ExitInfo>>,
        thread: std::thread::JoinHandle<()>,
    }

    impl Running {
        fn spawn(socket: &Path, begin: Begin, stdio: Stdio, cursor: Option<&Path>) -> Running {
            Running::spawn_with(socket, begin, stdio, cursor, pump_channel())
        }

        fn spawn_with(
            socket: &Path,
            begin: Begin,
            stdio: Stdio,
            cursor: Option<&Path>,
            (pump, inbox): (PumpHandle, Inbox),
        ) -> Running {
            let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
            let cursor = cursor.map(Path::to_path_buf);
            let (spec, exit_rx) = spec_for_c(socket.to_path_buf(), begin, stdio, cursor);
            let thread = std::thread::spawn(move || run_pump(spec, inbox, started_tx));
            let started = started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            started.unwrap();
            Running {
                pump,
                exit_rx,
                thread,
            }
        }

        fn status(&self) -> Option<u32> {
            self.exit_rx.borrow().as_ref().map(|e| e.status)
        }

        fn send(&self, cmd: PumpCmd) {
            assert!(self.pump.send(cmd), "the pump ended");
        }

        /// Pause the pump as the node service does, and wait for its ack.
        fn pause(&self) -> Result<(), String> {
            let (ack, acked) = std::sync::mpsc::sync_channel(1);
            if !self.pump.send(PumpCmd::Pause { ack }) {
                return Err("the pump ended".into());
            }
            match acked.recv_timeout(Duration::from_secs(2)) {
                Ok(answer) => answer,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    Err("the pump ended".into())
                }
                Err(e) => Err(format!("no ack: {e}")),
            }
        }

        /// Wait until the pump publishes an exit and its first thread ends.
        /// Returns the exit status.
        fn join(self) -> Option<u32> {
            wait_until("the pump publishes an exit", || self.status().is_some());
            wait_until("the pump's first thread ends", || self.thread.is_finished());
            let status = self.status();
            self.thread.join().unwrap();
            status
        }
    }

    /// A pump pauses after the frames before the pause, holds no connection
    /// and reports no exit while the VM is gone, and goes on after the wake
    /// with the frames from during the pause and after it: none is lost and
    /// none repeated.
    #[test]
    fn a_pump_pauses_and_resumes_without_losing_or_repeating_frames() {
        let dir = tempfile::tempdir().unwrap();
        let mut agent = Agent::new(dir.path().join("agent.sock"));
        let stdout = dir.path().join("stdout");
        agent.output("one\n");
        let pump = Running::spawn(
            &agent.socket,
            Begin::Start,
            stdio_in(dir.path(), false),
            None,
        );
        wait_until("one is written", || read(&stdout) == "one\n");

        pump.pause().unwrap();
        pump.pause().unwrap();
        wait_until("the pump closes its connection", || agent.open() == 0);
        agent.stop();
        agent.output("two\n");
        agent.output("three\n");
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(pump.status(), None);
        assert_eq!(read(&stdout), "one\n");
        assert_eq!(agent.requests(), ["start"]);

        agent.start();
        pump.send(PumpCmd::Resume);
        wait_until("two and three are written", || {
            read(&stdout) == "one\ntwo\nthree\n"
        });
        agent.output("four\n");
        agent.exit(0);
        assert_eq!(pump.join(), Some(0));
        assert_eq!(read(&stdout), "one\ntwo\nthree\nfour\n");
        assert_eq!(agent.requests(), ["start", "attach 2"]);
    }

    /// A paused pump that has written all its output parks: its thread ends,
    /// and it answers a pause at once. The wake starts a thread that attaches
    /// at its cursor.
    #[test]
    fn a_paused_pump_holds_no_thread_until_the_wake() {
        let dir = tempfile::tempdir().unwrap();
        let mut agent = Agent::new(dir.path().join("agent.sock"));
        let stdout = dir.path().join("stdout");
        agent.output("one\n");
        let pump = Running::spawn(
            &agent.socket,
            Begin::Start,
            stdio_in(dir.path(), false),
            None,
        );
        wait_until("one is written", || read(&stdout) == "one\n");

        pump.pause().unwrap();
        wait_until("the pump's thread ends", || pump.thread.is_finished());
        agent.stop();
        agent.output("two\n");
        pump.pause().unwrap();
        assert_eq!(pump.status(), None);

        agent.start();
        pump.send(PumpCmd::Resume);
        wait_until("two is written", || read(&stdout) == "one\ntwo\n");
        agent.exit(0);
        assert_eq!(pump.join(), Some(0));
        assert_eq!(agent.requests(), ["start", "attach 2"]);
    }

    /// A pause that comes while a frame arrives lets the frame finish, writes
    /// it and records its cursor before the ack, and the wake attaches after
    /// it.
    #[test]
    fn a_pause_during_a_frame_finishes_the_frame_first() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("agent.sock");
        let cursor = dir.path().join("cursor");
        let stdout = dir.path().join("stdout");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let (half_tx, half_rx) = std::sync::mpsc::channel();
        let agent = std::thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            let request = read_request(&mut first).unwrap();
            assert!(
                matches!(request, AgentRequest::PodStart { .. }),
                "{request:?}"
            );
            write_frame(&mut first, &AgentResponse::Started).unwrap();
            let data = b"whole\n".to_vec();
            let frame = encode_message(&AgentResponse::Stdout { data }).unwrap();
            let (head, tail) = frame.split_at(frame.len() / 2);
            first.write_all(head).unwrap();
            half_tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(300));
            first.write_all(tail).unwrap();
            assert_eq!(first.read(&mut [0u8; 1]).unwrap(), 0, "closed at the pause");

            let (mut second, _) = listener.accept().unwrap();
            let request = read_request(&mut second).unwrap();
            assert!(
                matches!(request, AgentRequest::PodAttach { cursor: 2, .. }),
                "{request:?}"
            );
            write_frame(&mut second, &AgentResponse::ok(None)).unwrap();
            let exited = AgentResponse::Exited {
                exit_code: 0,
                oom: false,
            };
            write_frame(&mut second, &exited).unwrap();
        });
        let stdio = stdio_in(dir.path(), false);
        let pump = Running::spawn(&socket, Begin::Start, stdio, Some(&cursor));
        half_rx.recv().unwrap();
        // The pump reads the first half by now, and waits for the rest.
        std::thread::sleep(Duration::from_millis(100));
        pump.pause().unwrap();
        assert_eq!(read(&stdout), "whole\n");
        assert_eq!(stored(&cursor), 2);

        pump.send(PumpCmd::Resume);
        assert_eq!(pump.join(), Some(0));
        agent.join().unwrap();
    }

    /// A pump whose fifo is full still pauses at once. It keeps the rest of
    /// the frame, writes it while paused, records the frame's cursor once the
    /// fifo took all of it, and then parks.
    #[test]
    fn a_pump_with_a_full_fifo_pauses_at_once_and_writes_the_frame_while_paused() {
        use std::os::unix::fs::OpenOptionsExt;
        let dir = tempfile::tempdir().unwrap();
        let agent = Agent::new(dir.path().join("agent.sock"));
        let cursor = dir.path().join("cursor");
        let fifo = dir.path().join("stdout");
        mkfifo(&fifo);
        let mut reader = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&fifo)
            .unwrap();
        let big = vec![b'x'; 512 * 1024];
        let data = big.clone();
        agent.journal().frames.push(AgentResponse::Stdout { data });
        let stdio = Stdio {
            stdout: fifo.display().to_string(),
            ..Stdio::default()
        };
        let pump = Running::spawn(&agent.socket, Begin::Start, stdio, Some(&cursor));
        // The pump fills the fifo and keeps the rest of the frame.
        std::thread::sleep(Duration::from_millis(300));

        let asked = std::time::Instant::now();
        pump.pause().unwrap();
        assert!(
            asked.elapsed() < Duration::from_millis(500),
            "{:?}",
            asked.elapsed()
        );
        wait_until("the pump closes its connection", || agent.open() == 0);
        assert_eq!(stored(&cursor), 1);
        let mut got = Vec::new();
        wait_until("the paused pump writes the rest of the frame", || {
            let mut buf = [0u8; 65536];
            loop {
                match reader.read(&mut buf) {
                    Ok(n) if n > 0 => got.extend_from_slice(&buf[..n]),
                    _ => break,
                }
            }
            got.len() == big.len()
        });
        assert!(got == big);
        wait_until("the cursor is recorded", || stored(&cursor) == 2);
        wait_until("the pump parks", || pump.thread.is_finished());

        agent.exit(0);
        pump.send(PumpCmd::Resume);
        assert_eq!(pump.join(), Some(0));
        assert_eq!(agent.attaches(), [2]);
    }

    /// A pump whose cursor is older than the agent's journal at its attach
    /// writes the gap marker on stderr and goes on at the oldest frame,
    /// without a false exit.
    #[test]
    fn an_expired_cursor_at_attach_writes_the_gap_marker_and_goes_on() {
        let dir = tempfile::tempdir().unwrap();
        let agent = Agent::new(dir.path().join("agent.sock"));
        let cursor = dir.path().join("cursor");
        std::fs::write(&cursor, 2u64.to_le_bytes()).unwrap();
        for line in ["one\n", "two\n", "three\n", "four\n", "five\n"] {
            agent.output(line);
        }
        agent.exit(0);
        agent.drop_before(4);

        let stdio = stdio_in(dir.path(), true);
        let pump = Running::spawn(&agent.socket, Begin::Attach, stdio, Some(&cursor));
        assert_eq!(pump.join(), Some(0));
        assert_eq!(agent.attaches(), [2, 4]);
        assert_eq!(read(&dir.path().join("stdout")), "four\nfive\n");
        assert_eq!(read(&dir.path().join("stderr")), gap_marker(2, 4));
        assert_eq!(stored(&cursor), 6);
    }

    /// A stream that falls behind the agent's journal ends with the agent's
    /// `CURSOR_EXPIRED` answer. The pump writes the gap marker on a line of
    /// its own and attaches at the oldest frame.
    #[test]
    fn a_stream_that_falls_behind_writes_the_gap_marker_and_goes_on() {
        let dir = tempfile::tempdir().unwrap();
        let agent = Agent::new(dir.path().join("agent.sock"));
        let stdout = dir.path().join("stdout");
        agent.output("one\n");
        agent.output("par");
        let pump = Running::spawn(
            &agent.socket,
            Begin::Start,
            stdio_in(dir.path(), false),
            None,
        );
        wait_until("one and par are written", || read(&stdout) == "one\npar");

        agent.journal().held = true;
        for line in ["two\n", "three\n", "four\n"] {
            agent.output(line);
        }
        agent.exit(0);
        agent.drop_before(5);
        agent.journal().held = false;
        assert_eq!(pump.join(), Some(0));
        let marker = gap_marker(3, 5);
        assert_eq!(read(&stdout), format!("one\npar\n{marker}four\n"));
        assert_eq!(agent.requests(), ["start", "attach 5"]);
        assert_eq!(
            marker,
            "smolvm-pods: output lost: 2 frames (3 to 4) fell out of the guest's journal \
             before they reached the log\n"
        );
    }

    /// While paused, a pump takes resizes without a connection. After the
    /// wake it sends the last one.
    #[test]
    fn a_paused_pump_sends_the_last_resize_after_the_wake() {
        let dir = tempfile::tempdir().unwrap();
        let agent = Agent::new(dir.path().join("agent.sock"));
        let stdio = stdio_in(dir.path(), false);
        let pump = Running::spawn(&agent.socket, Begin::Start, stdio, None);
        pump.pause().unwrap();
        wait_until("the pump closes its connection", || agent.open() == 0);

        pump.send(PumpCmd::Resize { cols: 80, rows: 24 });
        pump.send(PumpCmd::Resize {
            cols: 100,
            rows: 30,
        });
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(agent.requests(), ["start"]);

        pump.send(PumpCmd::Resume);
        let sent = ["start", "attach 1", "resize 100x30"];
        wait_until("the pump sends the resize", || agent.requests() == sent);
        agent.exit(0);
        assert_eq!(pump.join(), Some(0));
    }

    /// An exit that the pump read before a pause is published once, with its
    /// status. A pause after it finds the pump without a connection.
    #[test]
    fn an_exit_read_before_a_pause_is_published_once() {
        let dir = tempfile::tempdir().unwrap();
        let agent = Agent::new(dir.path().join("agent.sock"));
        agent.output("last\n");
        agent.exit(7);
        let pump = Running::spawn(
            &agent.socket,
            Begin::Start,
            stdio_in(dir.path(), false),
            None,
        );
        wait_until("the exit is published", || pump.status().is_some());
        match pump.pause() {
            Ok(()) => {}
            Err(e) => assert_eq!(e, "the pump ended"),
        }
        assert_eq!(pump.join(), Some(7));
        assert_eq!(read(&dir.path().join("stdout")), "last\n");
        assert_eq!(agent.requests(), ["start"]);
    }

    /// An exit that the agent sent and the pump had not read at a pause comes
    /// after the wake, from the journal, never while the pump is paused.
    #[test]
    fn an_exit_not_read_before_a_pause_comes_after_the_wake() {
        let dir = tempfile::tempdir().unwrap();
        let agent = Agent::new(dir.path().join("agent.sock"));
        let stdout = dir.path().join("stdout");
        agent.output("last\n");
        agent.exit(7);
        let (handle, inbox) = pump_channel();
        let (ack, acked) = std::sync::mpsc::sync_channel(1);
        assert!(handle.send(PumpCmd::Pause { ack }));
        let stdio = stdio_in(dir.path(), false);
        let channel = (handle, inbox);
        let pump = Running::spawn_with(&agent.socket, Begin::Start, stdio, None, channel);
        acked.recv_timeout(Duration::from_secs(2)).unwrap().unwrap();
        wait_until("the pump closes its connection", || agent.open() == 0);
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(pump.status(), None);
        assert_eq!(read(&stdout), "");

        pump.send(PumpCmd::Resume);
        assert_eq!(pump.join(), Some(7));
        assert_eq!(read(&stdout), "last\n");
        assert_eq!(agent.requests(), ["start", "attach 1"]);
    }

    /// A pump that is paused when its node service ends leaves its cursor
    /// behind, and reports no exit. The next node service's pump attaches
    /// there at the wake: nothing is lost or repeated.
    #[test]
    fn a_pump_paused_across_a_node_service_restart_attaches_at_its_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let mut agent = Agent::new(dir.path().join("agent.sock"));
        let cursor = dir.path().join("cursors/sb/c");
        let (before, after) = (dir.path().join("before"), dir.path().join("after"));
        std::fs::create_dir_all(&before).unwrap();
        std::fs::create_dir_all(&after).unwrap();
        agent.output("one\n");
        agent.output("two\n");
        let stdio = stdio_in(&before, false);
        let first = Running::spawn(&agent.socket, Begin::Start, stdio, Some(&cursor));
        wait_until("one and two are written", || {
            read(&before.join("stdout")) == "one\ntwo\n"
        });
        first.pause().unwrap();
        agent.stop();

        let Running {
            pump,
            exit_rx,
            thread,
        } = first;
        drop(pump);
        thread.join().unwrap();
        assert_eq!(exit_rx.borrow().as_ref().map(|e| e.status), None);
        assert_eq!(stored(&cursor), 3);

        agent.output("three\n");
        agent.exit(0);
        agent.start();
        let stdio = stdio_in(&after, false);
        let second = Running::spawn(&agent.socket, Begin::Attach, stdio, Some(&cursor));
        assert_eq!(second.join(), Some(0));
        assert_eq!(agent.requests(), ["start", "attach 3"]);
        assert_eq!(read(&after.join("stdout")), "three\n");
    }

    /// A pump of a sleeping pod that begins paused holds no connection until
    /// the wake, then attaches at its cursor.
    #[test]
    fn a_pump_that_begins_paused_attaches_at_the_first_resume() {
        let dir = tempfile::tempdir().unwrap();
        let agent = Agent::new(dir.path().join("agent.sock"));
        let cursor = dir.path().join("cursor");
        std::fs::write(&cursor, 2u64.to_le_bytes()).unwrap();
        agent.output("one\n");
        agent.output("two\n");
        agent.exit(0);
        let stdio = stdio_in(dir.path(), false);
        let pump = Running::spawn(&agent.socket, Begin::Paused, stdio, Some(&cursor));
        std::thread::sleep(Duration::from_millis(200));
        assert!(agent.requests().is_empty());
        pump.pause().unwrap();

        pump.send(PumpCmd::Resume);
        assert_eq!(pump.join(), Some(0));
        assert_eq!(read(&dir.path().join("stdout")), "two\n");
        assert_eq!(agent.requests(), ["attach 2"]);
    }
}
