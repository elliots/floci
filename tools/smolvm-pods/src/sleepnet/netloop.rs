//! The one thread that serves every catcher, with an epoll loop, and
//! [`NetCatchers`], the [`Catchers`] of the node service.
//!
//! The netloop owns each open catcher: it reads its frames, answers ARP,
//! drives its responder, holds frames and asks for wakes, and checks the
//! hold after link events in the pod's namespace. Calls from other threads
//! reach it as commands. A release or a close takes the catcher back off
//! the netloop, so that its netlink calls, the injection into tap0 and the
//! close never stall the other pods. Stopping the netloop closes every
//! catcher it serves at once, on threads of their own, because closing a
//! packet socket waits for an RCU grace period and concurrent closes share
//! one.

use std::collections::HashMap;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Instant;

use crate::netlink::check;
use crate::probe::ResponderFactory;
use crate::sync::lock;

use super::capture::RECV_BUFFER;
use super::catcher::{self, Catcher, Closer, Effects, DRAIN};
use super::{Catchers, Phase, PodNet, Released, TrafficWaker, VnetHeader};

/// The epoll token of the command eventfd. A catcher with key `k` has the
/// tokens `2k` (its packet socket) and `2k + 1` (its netlink socket).
const COMMANDS: u64 = 0;
/// Frames read from one socket before the netloop turns to the others.
const BUDGET: usize = 64;
const CLOSE_THREADS: usize = 2;
/// The threads that close the catchers when the netloop stops.
const STOP_THREADS: usize = 64;

enum Command {
    Add(Box<Catcher>, mpsc::Sender<Result<(), String>>),
    /// Sets the phase of an open catcher and checks its hold; none if there
    /// is no catcher.
    Phase(String, Phase, mpsc::Sender<Option<Result<(), String>>>),
    /// Drains the responder, then hands the catcher back.
    Release(String, mpsc::Sender<Option<Box<Catcher>>>),
    Refuse(String, mpsc::Sender<bool>),
    /// Hands the catcher back at once.
    Remove(String, mpsc::Sender<Option<Box<Catcher>>>),
    #[cfg(test)]
    Counts(String, mpsc::Sender<Option<(catcher::Counts, usize)>>),
    Stop,
}

/// Every pod's catcher, served by one netloop thread.
pub struct NetCatchers {
    commands: mpsc::Sender<Command>,
    wakeup: Arc<OwnedFd>,
    closer: Closer,
    /// One lock per pod, so that the calls for one pod run one at a time.
    pods: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    thread: Mutex<Option<JoinHandle<()>>>,
    stopped: AtomicBool,
}

impl NetCatchers {
    /// Starts the netloop thread. `responders` makes each pod's probe
    /// responder; `waker` is called on the netloop thread when traffic should
    /// wake a pod.
    pub fn start(
        responders: Arc<dyn ResponderFactory>,
        waker: Arc<dyn TrafficWaker>,
    ) -> Result<NetCatchers, String> {
        // SAFETY: plain epoll_create1 call.
        let epoll = check(unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) })
            .map_err(|e| format!("epoll: {e}"))?;
        // SAFETY: `epoll` is a fresh descriptor that nothing else owns.
        let epoll = unsafe { OwnedFd::from_raw_fd(epoll) };
        // SAFETY: plain eventfd call.
        let event = check(unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) })
            .map_err(|e| format!("eventfd: {e}"))?;
        // SAFETY: `event` is a fresh descriptor that nothing else owns.
        let wakeup = Arc::new(unsafe { OwnedFd::from_raw_fd(event) });
        watch(epoll.as_raw_fd(), wakeup.as_raw_fd(), COMMANDS)?;
        let (commands, rx) = mpsc::channel();
        let netloop = Netloop {
            epoll,
            wakeup: wakeup.clone(),
            commands: rx,
            responders,
            waker,
            entries: HashMap::new(),
            keys: HashMap::new(),
            next_key: 1,
        };
        let thread = std::thread::Builder::new()
            .name("sleepnet".into())
            .spawn(move || netloop.run())
            .map_err(|e| format!("spawn the netloop thread: {e}"))?;
        Ok(NetCatchers {
            commands,
            wakeup,
            closer: Closer::start(CLOSE_THREADS)?,
            pods: Mutex::new(HashMap::new()),
            thread: Mutex::new(Some(thread)),
            stopped: AtomicBool::new(false),
        })
    }

    /// Sends a command to the netloop and waits for its answer. After
    /// [`Catchers::stop`] it never returns: the process is ending, and a hook
    /// must not record a failure for a catcher that the stop closed.
    fn ask<T>(&self, command: impl FnOnce(mpsc::Sender<T>) -> Command) -> Result<T, String> {
        if self.stopped.load(Ordering::SeqCst) {
            loop {
                std::thread::park();
            }
        }
        let stopped = || "the sleepnet netloop has stopped".to_string();
        let (tx, rx) = mpsc::channel();
        self.commands.send(command(tx)).map_err(|_| stopped())?;
        signal(self.wakeup.as_raw_fd());
        rx.recv().map_err(|_| stopped())
    }

    /// Runs `f` while no other call for the pod `id` runs.
    fn exclusive<T>(&self, id: &str, f: impl FnOnce() -> T) -> T {
        let pod = lock(&self.pods).entry(id.to_string()).or_default().clone();
        let result = {
            let _guard = lock(&pod);
            f()
        };
        let mut pods = lock(&self.pods);
        if Arc::strong_count(&pod) == 2 {
            pods.remove(id);
        }
        result
    }

    /// What the catcher of `pod` did so far, and how many frames it holds.
    #[cfg(test)]
    fn counts(&self, pod: &PodNet) -> Result<Option<(catcher::Counts, usize)>, String> {
        self.ask(|tx| Command::Counts(pod.sandbox_id.clone(), tx))
    }
}

impl Catchers for NetCatchers {
    fn catch(&self, pod: &PodNet, phase: Phase) -> Result<(), String> {
        let id = &pod.sandbox_id;
        self.exclusive(id, || {
            if let Some(result) = self.ask(|tx| Command::Phase(id.clone(), phase, tx))? {
                return result.map_err(|e| format!("hold {id}: {e}"));
            }
            let started = Instant::now();
            let catcher = Catcher::open(pod, phase).map_err(|e| format!("catch {id}: {e}"))?;
            self.ask(|tx| Command::Add(Box::new(catcher), tx))??;
            log::info!(
                "sleepnet: {id}: caught {} in {:.2} ms",
                pod.ip,
                started.elapsed().as_secs_f64() * 1000.0
            );
            Ok(())
        })
    }

    fn release(&self, pod: &PodNet) -> Result<Released, String> {
        let id = &pod.sandbox_id;
        self.exclusive(id, || {
            match self.ask(|tx| Command::Release(id.clone(), tx))? {
                Some(catcher) => catcher.release(&self.closer),
                None => catcher::forward_without_catcher(pod)
                    .map(|()| Released::default())
                    .map_err(|e| format!("release {id}: {e}")),
            }
        })
    }

    fn refuse(&self, pod: &PodNet) -> Result<(), String> {
        let id = &pod.sandbox_id;
        self.exclusive(id, || {
            self.ask(|tx| Command::Refuse(id.clone(), tx)).map(|_| ())
        })
    }

    fn close(&self, pod: &PodNet) -> Result<(), String> {
        let id = &pod.sandbox_id;
        self.exclusive(id, || {
            match self.ask(|tx| Command::Remove(id.clone(), tx))? {
                Some(catcher) => catcher.close(&self.closer),
                None => {
                    catcher::forward_without_catcher(pod).map_err(|e| format!("close {id}: {e}"))
                }
            }
        })
    }

    fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        if self.commands.send(Command::Stop).is_ok() {
            signal(self.wakeup.as_raw_fd());
        }
        if let Some(thread) = lock(&self.thread).take() {
            let _ = thread.join();
        }
    }
}

impl Drop for NetCatchers {
    fn drop(&mut self) {
        self.stop();
    }
}

fn signal(eventfd: RawFd) {
    let one = 1u64;
    // SAFETY: writes 8 bytes from a live u64 to an eventfd.
    unsafe { libc::write(eventfd, (&one as *const u64).cast(), 8) };
}

fn watch(epoll: RawFd, fd: RawFd, token: u64) -> Result<(), String> {
    let mut event = libc::epoll_event {
        events: libc::EPOLLIN as u32,
        u64: token,
    };
    // SAFETY: adds a live descriptor with a valid event.
    check(unsafe { libc::epoll_ctl(epoll, libc::EPOLL_CTL_ADD, fd, &mut event) })
        .map(|_| ())
        .map_err(|e| format!("epoll_ctl: {e}"))
}

fn unwatch(epoll: RawFd, fd: RawFd) {
    // SAFETY: removes a descriptor; the event is ignored.
    unsafe { libc::epoll_ctl(epoll, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut()) };
}

struct Entry {
    catcher: Box<Catcher>,
    /// A release waits for the responder's connections until this time.
    release: Option<(Instant, mpsc::Sender<Option<Box<Catcher>>>)>,
    /// The last error of a hold check, logged once.
    hold_error: Option<String>,
}

impl Entry {
    fn apply(&mut self, fx: Effects, waker: &dyn TrafficWaker) {
        self.catcher.send_out(&fx.out);
        if let Some((from, port)) = fx.wake {
            if waker.wake(self.catcher.id(), from, port) {
                self.catcher.rules.woke(Instant::now());
            }
        }
    }

    /// Checks the hold and sets it again if it was lost.
    fn check_hold(&mut self) -> Result<(), String> {
        let result = self.catcher.check_hold();
        let id = self.catcher.id().to_string();
        match &result {
            Ok(true) => log::warn!("sleepnet: {id}: the hold on eth0 was lost; it is set again"),
            Ok(false) => {}
            Err(e) if self.hold_error.as_ref() != Some(e) => {
                log::warn!("sleepnet: {id}: check the hold: {e}");
            }
            Err(_) => {}
        }
        self.hold_error = result.as_ref().err().cloned();
        result.map(|_| ())
    }
}

struct Netloop {
    epoll: OwnedFd,
    wakeup: Arc<OwnedFd>,
    commands: mpsc::Receiver<Command>,
    responders: Arc<dyn ResponderFactory>,
    waker: Arc<dyn TrafficWaker>,
    entries: HashMap<u64, Entry>,
    keys: HashMap<String, u64>,
    next_key: u64,
}

impl Netloop {
    fn run(mut self) {
        let mut buf = vec![0u8; RECV_BUFFER];
        let mut events = vec![libc::epoll_event { events: 0, u64: 0 }; 64];
        loop {
            let timeout = self.timeout(Instant::now());
            // SAFETY: waits into a buffer of the length passed.
            let n = unsafe {
                libc::epoll_wait(
                    self.epoll.as_raw_fd(),
                    events.as_mut_ptr(),
                    events.len() as libc::c_int,
                    timeout,
                )
            };
            if n < 0 {
                let e = io::Error::last_os_error();
                if e.kind() != io::ErrorKind::Interrupted {
                    log::error!("sleepnet: epoll_wait: {e}");
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                continue;
            }
            for event in &events[..n as usize] {
                let token = event.u64;
                if token == COMMANDS {
                    if !self.commands() {
                        let catchers: Vec<Catcher> =
                            self.entries.drain().map(|(_, e)| *e.catcher).collect();
                        close_all(catchers);
                        return;
                    }
                } else if token % 2 == 0 {
                    self.frames(token / 2, &mut buf);
                } else {
                    self.link_events(token / 2);
                }
            }
            self.timers(Instant::now());
        }
    }

    /// Milliseconds until the next responder poll or drain deadline, or -1.
    fn timeout(&self, now: Instant) -> libc::c_int {
        let next = self
            .entries
            .values()
            .flat_map(|e| [e.catcher.rules.poll_at, e.release.as_ref().map(|r| r.0)])
            .flatten()
            .min();
        match next {
            None => -1,
            Some(at) => {
                let wait = at.saturating_duration_since(now);
                wait.as_micros().div_ceil(1000).min(60_000) as libc::c_int
            }
        }
    }

    /// Handles every waiting command. Returns false on [`Command::Stop`].
    fn commands(&mut self) -> bool {
        let mut count = [0u8; 8];
        // SAFETY: reads 8 bytes into a live buffer.
        unsafe { libc::read(self.wakeup.as_raw_fd(), count.as_mut_ptr().cast(), 8) };
        while let Ok(command) = self.commands.try_recv() {
            match command {
                Command::Add(catcher, reply) => {
                    let _ = reply.send(self.add(catcher));
                }
                Command::Phase(id, phase, reply) => {
                    let result = self.entry(&id).map(|entry| {
                        entry.catcher.rules.set_phase(phase);
                        entry.check_hold()
                    });
                    let _ = reply.send(result);
                }
                Command::Release(id, reply) => match self.entry(&id) {
                    Some(entry) => {
                        entry.catcher.rules.start_drain();
                        entry.release = Some((Instant::now() + DRAIN, reply));
                    }
                    None => {
                        let _ = reply.send(None);
                    }
                },
                Command::Refuse(id, reply) => {
                    let waker = self.waker.clone();
                    let found = self.entry(&id).map(|entry| {
                        let resets = entry
                            .catcher
                            .rules
                            .refuse(|from, port| waker.is_own_connection(&id, from, port));
                        entry.catcher.send_out(&resets);
                        log::info!(
                            "sleepnet: {id}: refused {} held connections with a RST",
                            resets.len()
                        );
                    });
                    let _ = reply.send(found.is_some());
                }
                Command::Remove(id, reply) => {
                    let catcher = self.keys.get(&id).copied().and_then(|k| self.take(k));
                    let _ = reply.send(catcher);
                }
                #[cfg(test)]
                Command::Counts(id, reply) => {
                    let counts = self.entry(&id).map(|e| {
                        let rules = &e.catcher.rules;
                        (rules.counts, rules.held().len())
                    });
                    let _ = reply.send(counts);
                }
                Command::Stop => return false,
            }
        }
        true
    }

    fn entry(&mut self, id: &str) -> Option<&mut Entry> {
        let key = self.keys.get(id)?;
        self.entries.get_mut(key)
    }

    fn add(&mut self, catcher: Box<Catcher>) -> Result<(), String> {
        let id = catcher.id().to_string();
        if self.keys.contains_key(&id) {
            return Err(format!("catch {id}: the pod has a catcher already"));
        }
        let key = self.next_key;
        self.next_key += 1;
        let epoll = self.epoll.as_raw_fd();
        watch(epoll, catcher.capture.fd(), key * 2)
            .and_then(|()| watch(epoll, catcher.netlink.fd(), key * 2 + 1))
            .map_err(|e| {
                unwatch(epoll, catcher.capture.fd());
                format!("catch {id}: {e}")
            })?;
        self.keys.insert(id, key);
        self.entries.insert(
            key,
            Entry {
                catcher,
                release: None,
                hold_error: None,
            },
        );
        Ok(())
    }

    /// Takes the catcher with key `key` off the netloop.
    fn take(&mut self, key: u64) -> Option<Box<Catcher>> {
        let entry = self.entries.remove(&key)?;
        let epoll = self.epoll.as_raw_fd();
        unwatch(epoll, entry.catcher.capture.fd());
        unwatch(epoll, entry.catcher.netlink.fd());
        self.keys.remove(entry.catcher.id());
        Some(entry.catcher)
    }

    /// Reads the frames waiting on a catcher's packet socket.
    fn frames(&mut self, key: u64, buf: &mut [u8]) {
        let Some(entry) = self.entries.get_mut(&key) else {
            return;
        };
        let (responders, waker) = (&*self.responders, &*self.waker);
        let handled = catch_unwind(AssertUnwindSafe(|| {
            for _ in 0..BUDGET {
                let received = match entry.catcher.capture.recv(buf) {
                    Ok(Some(r)) => r,
                    Ok(None) => break,
                    Err(e) => {
                        log::debug!("sleepnet: {}: read eth0: {e}", entry.catcher.id());
                        break;
                    }
                };
                let Some(vnet) = VnetHeader::parse(&buf[..received.len]) else {
                    continue;
                };
                if received.truncated {
                    entry.catcher.rules.counts.dropped += 1;
                    continue;
                }
                let mut fx = Effects::default();
                entry.catcher.rules.on_frame(
                    vnet,
                    &buf[VnetHeader::LEN..received.len],
                    received.at,
                    Instant::now(),
                    responders,
                    &mut fx,
                );
                entry.apply(fx, waker);
            }
        }));
        if handled.is_err() {
            log::error!(
                "sleepnet: {}: a frame panicked the catcher; its responder is dropped",
                entry.catcher.id()
            );
            entry.catcher.rules.drop_responder();
        }
    }

    /// Checks the hold when a link event names eth0, or events were lost.
    fn link_events(&mut self, key: u64) {
        let Some(entry) = self.entries.get_mut(&key) else {
            return;
        };
        let changes = entry.catcher.netlink.changes();
        if entry.release.is_none() && changes.touch(entry.catcher.eth0) {
            let _ = entry.check_hold();
        }
    }

    /// Polls the responders that are due, and hands back the catchers whose
    /// drain is over.
    fn timers(&mut self, now: Instant) {
        let waker = &*self.waker;
        let mut drained = Vec::new();
        for (key, entry) in &mut self.entries {
            if entry.catcher.rules.poll_at.is_some_and(|at| at <= now) {
                let polled = catch_unwind(AssertUnwindSafe(|| {
                    let mut fx = Effects::default();
                    entry.catcher.rules.poll(now, &mut fx);
                    fx
                }));
                match polled {
                    Ok(fx) => entry.apply(fx, waker),
                    Err(_) => {
                        log::error!(
                            "sleepnet: {}: a poll panicked the responder; it is dropped",
                            entry.catcher.id()
                        );
                        entry.catcher.rules.drop_responder();
                    }
                }
            }
            if let Some((deadline, _)) = &entry.release {
                if *deadline <= now || entry.catcher.rules.drained() {
                    drained.push(*key);
                }
            }
        }
        for key in drained {
            let reply = self.entries.get_mut(&key).and_then(|e| e.release.take());
            if let (Some(catcher), Some((_, reply))) = (self.take(key), reply) {
                if let Err(mpsc::SendError(Some(catcher))) = reply.send(Some(catcher)) {
                    drop(catcher);
                }
            }
        }
    }
}

/// Closes `catchers` at once, on up to [`STOP_THREADS`] threads, so that the
/// RCU grace periods that their packet sockets wait for overlap. The holds
/// stay, as for a restart.
fn close_all(catchers: Vec<Catcher>) {
    let count = catchers.len();
    if count == 0 {
        return;
    }
    let started = Instant::now();
    let queue = Mutex::new(catchers);
    let close = || loop {
        let next = lock(&queue).pop();
        match next {
            Some(catcher) => drop(catcher),
            None => return,
        }
    };
    std::thread::scope(|scope| {
        for i in 0..STOP_THREADS.min(count) {
            let spawned = std::thread::Builder::new()
                .name(format!("sleepnet-stop-{i}"))
                .spawn_scoped(scope, close);
            if let Err(e) = spawned {
                log::warn!("sleepnet: spawn a thread to close the catchers: {e}");
                break;
            }
        }
    });
    close();
    log::info!(
        "sleepnet: closed {count} catchers in {} ms; their holds stay",
        started.elapsed().as_millis()
    );
}

/// The catchers in real network namespaces: a node with cni0 in hairpin
/// mode, a pod whose eth0 is a port of podbr0 as `net.rs` leaves it, a
/// client on cni0, and a guest, a tap in a namespace of its own that a frame
/// copy joins to the pod's tap0, as the VM process does. While the guest
/// sleeps, tap0 is gone, and a new one joins podbr0 at the wake, as at a
/// resume. They need root, /dev/net/tun and `ip`, so they only run when
/// asked for, in a mount namespace of their own (see `net.rs`):
///
/// ```text
/// unshare -m <test binary> --ignored --test-threads 1 sleepnet::netloop::netns_tests
/// ```
#[cfg(test)]
mod netns_tests {
    use super::*;

    use std::io::{Read, Write};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream};
    use std::os::fd::{AsRawFd, OwnedFd};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread::JoinHandle;
    use std::time::{Duration, SystemTime};

    use crate::net::{format_mac, parse_mac};
    use crate::probe::NoResponder;
    use crate::test_netns::{open_tap, Netns};

    const POD_IP: Ipv4Addr = Ipv4Addr::new(10, 42, 0, 17);
    const CLIENT_IP: Ipv4Addr = Ipv4Addr::new(10, 42, 0, 250);
    const PORT: u16 = 8080;

    /// A namespace for these tests.
    fn netns(role: &str) -> Netns {
        Netns::new(&format!("sleepnet-{role}"))
    }

    impl Netns {
        fn port_state(&self, dev: &str) -> String {
            let details: serde_json::Value =
                serde_json::from_str(&self.ip(&["-d", "-j", "link", "show", "dev", dev])).unwrap();
            details[0]["linkinfo"]["info_slave_data"]["state"]
                .as_str()
                .unwrap_or("none")
                .to_string()
        }

        fn mac(&self, dev: &str) -> [u8; 6] {
            let link: serde_json::Value =
                serde_json::from_str(&self.ip(&["-j", "link", "show", "dev", dev])).unwrap();
            parse_mac(link[0]["address"].as_str().unwrap()).unwrap()
        }
    }

    fn dup(fd: &OwnedFd) -> OwnedFd {
        fd.try_clone().unwrap()
    }

    /// Copies frames between tap0 and the guest's tap, as the VM process
    /// does; tap0 goes when the pump does.
    struct Pump {
        stop: Arc<AtomicBool>,
        threads: Vec<JoinHandle<()>>,
    }

    impl Pump {
        fn start(tap0: OwnedFd, guest: &OwnedFd) -> Pump {
            let stop = Arc::new(AtomicBool::new(false));
            let pairs = [(dup(&tap0), dup(guest)), (dup(guest), tap0)];
            let threads = pairs
                .into_iter()
                .map(|(from, to)| {
                    let stop = stop.clone();
                    std::thread::spawn(move || copy_frames(&from, &to, &stop))
                })
                .collect();
            Pump { stop, threads }
        }
    }

    impl Drop for Pump {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            for t in self.threads.drain(..) {
                let _ = t.join();
            }
        }
    }

    fn copy_frames(from: &OwnedFd, to: &OwnedFd, stop: &AtomicBool) {
        let mut buf = vec![0u8; 65536];
        while !stop.load(Ordering::SeqCst) {
            let mut poll = libc::pollfd {
                fd: from.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: one valid pollfd.
            if unsafe { libc::poll(&mut poll, 1, 20) } <= 0 {
                continue;
            }
            // SAFETY: reads into a buffer of the given length.
            let n = unsafe { libc::read(from.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
            if n <= 0 {
                break;
            }
            // SAFETY: writes the bytes just read.
            unsafe { libc::write(to.as_raw_fd(), buf.as_ptr().cast(), n as usize) };
        }
    }

    /// A server in the guest that answers each client with its address.
    struct Server {
        stop: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
    }

    impl Server {
        fn start(netns: &Netns) -> Server {
            let stop = Arc::new(AtomicBool::new(false));
            let (ready_tx, ready) = mpsc::channel();
            let stopping = stop.clone();
            let thread = netns.spawn(move || {
                let listener = TcpListener::bind((POD_IP, PORT)).unwrap();
                listener.set_nonblocking(true).unwrap();
                ready_tx.send(()).unwrap();
                while !stopping.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, peer)) => {
                            stream.set_nonblocking(false).unwrap();
                            let _ = stream.write_all(format!("remote={peer}").as_bytes());
                        }
                        Err(_) => std::thread::sleep(Duration::from_millis(2)),
                    }
                }
            });
            ready.recv().unwrap();
            Server {
                stop,
                thread: Some(thread),
            }
        }
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            if let Some(t) = self.thread.take() {
                let _ = t.join();
            }
        }
    }

    /// What a client saw: how long its connect took, and the answer or the
    /// error.
    type Fetch = (Duration, Result<String, io::ErrorKind>);

    fn fetch(client: &Netns) -> JoinHandle<Fetch> {
        client.spawn(|| {
            let started = Instant::now();
            let addr = SocketAddr::from((POD_IP, PORT));
            let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(4));
            let connected = started.elapsed();
            let reply = stream.and_then(|mut s| {
                s.set_read_timeout(Some(Duration::from_secs(2)))?;
                let mut reply = String::new();
                s.read_to_string(&mut reply)?;
                Ok(reply)
            });
            (connected, reply.map_err(|e| e.kind()))
        })
    }

    #[derive(Default)]
    struct Wakes(Mutex<Vec<(String, SocketAddrV4, u16)>>, std::sync::Condvar);

    impl TrafficWaker for Wakes {
        fn wake(&self, sandbox_id: &str, from: SocketAddrV4, port: u16) -> bool {
            lock(&self.0).push((sandbox_id.to_string(), from, port));
            self.1.notify_all();
            true
        }

        fn is_own_connection(&self, _sandbox_id: &str, _from: SocketAddrV4, _port: u16) -> bool {
            false
        }
    }

    impl Wakes {
        fn wait(&self, count: usize) -> Vec<(String, SocketAddrV4, u16)> {
            let guard = lock(&self.0);
            let (guard, _) = self
                .1
                .wait_timeout_while(guard, Duration::from_secs(3), |w| w.len() < count)
                .unwrap();
            guard.clone()
        }
    }

    struct World {
        node: Netns,
        pod: Netns,
        client: Netns,
        vm: Netns,
        guest: OwnedFd,
        net: PodNet,
    }

    impl World {
        fn new() -> World {
            let node = netns("node");
            let pod = netns("pod");
            let client = netns("client");
            let vm = netns("vm");
            node.ip(&["link", "add", "cni0", "type", "bridge"]);
            node.ip(&["addr", "add", "10.42.0.1/24", "dev", "cni0"]);
            node.ip(&["link", "set", "cni0", "up"]);
            for (peer, ns, addr) in [
                ("vethpod", &pod, "10.42.0.17/24"),
                ("vethcl", &client, "10.42.0.250/24"),
            ] {
                node.ip(&[
                    "link",
                    "add",
                    peer,
                    "type",
                    "veth",
                    "peer",
                    "name",
                    "eth0",
                    "netns",
                    ns.name(),
                ]);
                node.ip(&["link", "set", peer, "master", "cni0", "up"]);
                node.ip(&["link", "set", peer, "type", "bridge_slave", "hairpin", "on"]);
                ns.ip(&["addr", "add", addr, "dev", "eth0"]);
                ns.ip(&["link", "set", "eth0", "up"]);
            }
            // The pod namespace as net.rs leaves it.
            let guest_mac = pod.mac("eth0");
            let mut port_mac = guest_mac;
            port_mac[0] ^= 2;
            let port_mac = format_mac(port_mac);
            pod.run(|| {
                std::fs::write("/proc/sys/net/ipv4/conf/all/arp_ignore", "8").unwrap();
                std::fs::write("/proc/sys/net/ipv4/conf/all/arp_notify", "0").unwrap();
            });
            pod.ip(&[
                "link",
                "add",
                "name",
                "podbr0",
                "address",
                &port_mac,
                "type",
                "bridge",
                "forward_delay",
                "0",
            ]);
            pod.ip(&[
                "link", "set", "dev", "eth0", "address", &port_mac, "master", "podbr0",
            ]);
            pod.ip(&[
                "link",
                "set",
                "dev",
                "eth0",
                "type",
                "bridge_slave",
                "learning",
                "off",
            ]);
            pod.ip(&["link", "set", "dev", "podbr0", "up"]);

            let guest = vm.run(|| open_tap("eth0", false));
            vm.ip(&[
                "link",
                "set",
                "eth0",
                "address",
                &format_mac(guest_mac),
                "up",
            ]);
            vm.ip(&["addr", "add", "10.42.0.17/24", "dev", "eth0"]);
            let net = PodNet {
                sandbox_id: format!("floci-sleepnet-{}", std::process::id()),
                netns: pod.path().display().to_string(),
                iface: "eth0".into(),
                ip: POD_IP,
                mac: guest_mac,
            };
            World {
                node,
                pod,
                client,
                vm,
                guest,
                net,
            }
        }

        /// A new tap0 joined to podbr0 and pumped to the guest, as at a
        /// boot or a resume.
        fn resume(&self) -> Pump {
            let tap0 = self.pod.run(|| open_tap("tap0", false));
            self.pod
                .ip(&["link", "set", "dev", "tap0", "master", "podbr0"]);
            self.pod.ip(&[
                "link",
                "set",
                "dev",
                "tap0",
                "type",
                "bridge_slave",
                "learning",
                "off",
            ]);
            self.pod.ip(&["link", "set", "dev", "tap0", "up"]);
            Pump::start(tap0, &self.guest)
        }

        fn forget_neighbours(&self) {
            for ns in [&self.node, &self.client, &self.vm] {
                ns.ip(&["neigh", "flush", "all"]);
            }
        }

        fn wait_for_tap0_gone(&self) {
            let deadline = Instant::now() + Duration::from_secs(2);
            while self.pod.ip(&["link", "show"]).contains("tap0") {
                assert!(Instant::now() < deadline, "tap0 stays");
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    fn start() -> (NetCatchers, Arc<Wakes>) {
        let wakes = Arc::new(Wakes::default());
        let catchers = NetCatchers::start(Arc::new(NoResponder), wakes.clone()).unwrap();
        (catchers, wakes)
    }

    #[test]
    #[ignore = "needs root, network namespaces and /dev/net/tun"]
    fn a_held_syn_is_replayed_after_a_wake() {
        let world = World::new();
        let _server = Server::start(&world.vm);
        let (catchers, wakes) = start();
        let net = &world.net;

        let pump = world.resume();
        world.forget_neighbours();
        let (took, reply) = fetch(&world.client).join().unwrap();
        assert!(reply.unwrap().starts_with("remote=10.42.0.250:"), "awake");
        eprintln!("awake: connected in {took:?}");

        // Pause: the hold first, then the VM process exits with tap0.
        catchers.catch(net, Phase::Pausing).unwrap();
        assert_eq!(world.pod.port_state("eth0"), "listening");
        drop(pump);
        world.wait_for_tap0_gone();
        catchers
            .catch(net, Phase::Asleep { probes_fail: false })
            .unwrap();

        // A client with no neighbour entry: ARP is answered, the SYN held.
        world.forget_neighbours();
        let client = fetch(&world.client);
        let woken = wakes.wait(1);
        assert_eq!(woken.len(), 1, "one wake");
        let (id, from, port) = &woken[0];
        assert_eq!(
            (id.as_str(), *from.ip(), *port),
            (net.sandbox_id.as_str(), CLIENT_IP, PORT)
        );
        let (counts, held) = catchers.counts(net).unwrap().unwrap();
        assert_eq!(
            (counts.arp_replies, held),
            (1, 2),
            "{counts:?}: the request and the SYN"
        );

        // Resume: a new tap0 joins podbr0, and the hold keeps it cut off.
        catchers.catch(net, Phase::Waking).unwrap();
        let pump = world.resume();
        assert_eq!(world.pod.port_state("eth0"), "listening");
        std::thread::sleep(Duration::from_millis(100));
        let started = Instant::now();
        let released = catchers.release(net).unwrap();
        eprintln!("released {released:?} in {:?}", started.elapsed());
        assert_eq!(released.frames, 2, "the ARP request and the SYN");
        let waited = SystemTime::now()
            .duration_since(released.first_held.unwrap())
            .unwrap();
        assert!(waited >= Duration::from_millis(100), "{waited:?}");
        let (took, reply) = client.join().unwrap();
        eprintln!("held: connected in {took:?}");
        assert!(
            took < Duration::from_millis(900),
            "no SYN retransmit: {took:?}"
        );
        assert!(
            reply.unwrap().starts_with("remote=10.42.0.250:"),
            "the client IP"
        );
        assert_eq!(world.pod.port_state("eth0"), "forwarding");
        assert!(
            catchers.counts(net).unwrap().is_none(),
            "the catcher is gone"
        );
        assert_eq!(catchers.release(net).unwrap(), Released::default());

        // A hold that something else lifts is set again.
        catchers
            .catch(net, Phase::Asleep { probes_fail: false })
            .unwrap();
        world.pod.ip(&[
            "link",
            "set",
            "dev",
            "eth0",
            "type",
            "bridge_slave",
            "state",
            "3",
        ]);
        let deadline = Instant::now() + Duration::from_secs(1);
        while world.pod.port_state("eth0") != "listening" {
            assert!(Instant::now() < deadline, "the hold stays lifted");
            std::thread::sleep(Duration::from_millis(5));
        }

        // A refused wake: the client is refused at once.
        world.forget_neighbours();
        let client = fetch(&world.client);
        assert_eq!(wakes.wait(2).len(), 2);
        catchers.refuse(net).unwrap();
        let (took, reply) = client.join().unwrap();
        assert_eq!(
            reply.unwrap_err(),
            io::ErrorKind::ConnectionRefused,
            "{took:?}"
        );
        assert!(took < Duration::from_millis(900), "{took:?}");
        let held = catchers.counts(net).unwrap().unwrap().1;
        assert_eq!(held, 1, "the ARP request stays for the guest");

        // A close lets the guest answer again; another close is a no-op.
        catchers.close(net).unwrap();
        assert_eq!(world.pod.port_state("eth0"), "forwarding");
        catchers.close(net).unwrap();
        let (_, reply) = fetch(&world.client).join().unwrap();
        assert!(reply.unwrap().starts_with("remote=10.42.0.250:"));
        drop(pump);

        // After a restart the kernel keeps the hold with no catcher: a
        // release without one ends it.
        catchers
            .catch(net, Phase::Asleep { probes_fail: false })
            .unwrap();
        drop(catchers);
        assert_eq!(world.pod.port_state("eth0"), "listening");
        let (catchers, _) = start();
        catchers.release(net).unwrap();
        assert_eq!(world.pod.port_state("eth0"), "forwarding");
    }

    #[test]
    #[ignore = "needs root, network namespaces and /dev/net/tun"]
    fn a_pod_that_is_gone_is_released_and_a_pod_without_a_bridge_is_not_caught() {
        let (catchers, _) = start();
        let gone = PodNet {
            sandbox_id: "gone".into(),
            netns: "/var/run/netns/floci-sleepnet-gone".into(),
            iface: "eth0".into(),
            ip: POD_IP,
            mac: [2, 0, 0, 0, 0, 1],
        };
        catchers.close(&gone).unwrap();
        let err = catchers.catch(&gone, Phase::Pausing).unwrap_err();
        assert!(err.contains("floci-sleepnet-gone"), "{err}");

        let plain = netns("plain");
        plain.ip(&[
            "link", "add", "eth0", "type", "veth", "peer", "name", "peer0",
        ]);
        plain.ip(&["link", "set", "eth0", "up"]);
        let pod = PodNet {
            netns: plain.path().display().to_string(),
            ..gone
        };
        let err = catchers.catch(&pod, Phase::Pausing).unwrap_err();
        assert!(err.contains("not a bridge port"), "{err}");
        catchers.release(&pod).unwrap();
    }

    fn slab_kib() -> u64 {
        meminfo_kib("/proc/meminfo", "Slab:")
    }

    fn meminfo_kib(file: &str, key: &str) -> u64 {
        let text = std::fs::read_to_string(file).unwrap();
        text.lines()
            .find_map(|l| l.strip_prefix(key))
            .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
            .unwrap()
    }

    fn open_fds() -> usize {
        std::fs::read_dir("/proc/self/fd").unwrap().count()
    }

    fn per_pod(after: u64, before: u64, pods: usize) -> f64 {
        (after as f64 - before as f64) / pods as f64
    }

    /// A number from the environment variable `key`, 100 by default.
    fn count(key: &str) -> usize {
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100)
    }

    /// `pods` pod namespaces, each with an eth0 that is a port of podbr0.
    fn bridged_pods(role: &str, pods: usize) -> (Vec<Netns>, Vec<PodNet>) {
        let mut namespaces = Vec::new();
        let mut nets = Vec::new();
        for i in 0..pods {
            let ns = netns(&format!("{role}{i}"));
            ns.ip(&[
                "link", "add", "eth0", "type", "veth", "peer", "name", "peer0",
            ]);
            ns.ip(&["link", "add", "podbr0", "type", "bridge"]);
            ns.ip(&["link", "set", "eth0", "master", "podbr0", "up"]);
            ns.ip(&["link", "set", "peer0", "up"]);
            ns.ip(&["link", "set", "podbr0", "up"]);
            nets.push(PodNet {
                sandbox_id: format!("{role}{i}"),
                netns: ns.path().display().to_string(),
                iface: "eth0".into(),
                ip: Ipv4Addr::new(10, 43, (i / 250) as u8, (i % 250 + 2) as u8),
                mac: [2, 0, 0, 0, (i / 250) as u8, (i % 250) as u8],
            });
            namespaces.push(ns);
        }
        (namespaces, nets)
    }

    /// A stop closes SLEEPNET_STOP_PODS catchers (default 100) at once and
    /// leaves every hold, so that a node service that ends at a SIGTERM does
    /// not wait for one RCU grace period after another. It prints the time
    /// against the same number of packet sockets closed one at a time.
    #[test]
    #[ignore = "needs root and network namespaces"]
    fn a_stop_closes_every_catcher_at_once_and_keeps_the_holds() {
        let pods = count("SLEEPNET_STOP_PODS");
        let (namespaces, nets) = bridged_pods("stop", pods);
        let mut sockets = Vec::new();
        for net in &nets {
            let ip = net.ip;
            let socket = super::super::capture::in_netns(&net.netns, move || {
                let index = super::super::hold::Netlink::open(false)
                    .and_then(|mut nl| nl.link_named("eth0"))
                    .map_err(|e| e.to_string())?
                    .ok_or("no eth0")?
                    .index;
                super::super::capture::Capture::open(index, ip).map_err(|e| e.to_string())
            });
            sockets.push(socket.unwrap());
        }
        let started = Instant::now();
        drop(sockets);
        let one_at_a_time = started.elapsed();

        let (catchers, _) = start();
        for net in &nets {
            catchers
                .catch(net, Phase::Asleep { probes_fail: false })
                .unwrap();
        }
        let fds = open_fds();
        let started = Instant::now();
        catchers.stop();
        let stopped = started.elapsed();
        eprintln!(
            "{pods} packet sockets closed one at a time in {one_at_a_time:?}; a stop closed \
             {pods} catchers in {stopped:?}"
        );
        assert_eq!(
            fds - open_fds(),
            2 * pods + 1,
            "every catcher's two sockets, and the epoll"
        );
        for ns in &namespaces {
            assert_eq!(ns.port_state("eth0"), "listening");
        }
        let (catchers, _) = start();
        for net in &nets {
            catchers.release(net).unwrap();
        }
        for ns in &namespaces {
            assert_eq!(ns.port_state("eth0"), "forwarding");
        }
    }

    /// The kernel memory and descriptors of a sleeping pod's catcher, over
    /// SLEEPNET_COST_PODS pods (default 100). Netlink sockets alone are
    /// measured first and left open, so that the catchers' sockets do not
    /// reuse what they free.
    #[test]
    #[ignore = "needs root and network namespaces"]
    fn the_cost_of_a_catcher() {
        let pods = count("SLEEPNET_COST_PODS");
        let (namespaces, nets) = bridged_pods("cost", pods);
        let (catchers, _) = start();
        let settle = || std::thread::sleep(Duration::from_secs(2));
        // Each catch runs a thread in the pod's namespace; the slab that
        // threads use grows here, not in the measurements.
        for net in nets.iter().chain(&nets) {
            super::super::capture::in_netns(&net.netns, || Ok(())).unwrap();
        }
        settle();

        let slab = slab_kib();
        let mut sockets = Vec::new();
        for net in &nets {
            let socket = super::super::capture::in_netns(&net.netns, || {
                super::super::hold::Netlink::open(false).map_err(|e| e.to_string())
            });
            sockets.push(socket.unwrap());
        }
        settle();
        let netlink = per_pod(slab_kib(), slab, pods);

        let (slab, fds) = (slab_kib(), open_fds());
        let started = Instant::now();
        for net in &nets {
            catchers
                .catch(net, Phase::Asleep { probes_fail: false })
                .unwrap();
        }
        let catch = started.elapsed() / pods as u32;
        settle();
        let (slab_after, fds_after) = (slab_kib(), open_fds());
        let started = Instant::now();
        for net in &nets {
            catchers.close(net).unwrap();
        }
        let close = started.elapsed() / pods as u32;
        eprintln!(
            "cost of {pods} catchers, each: {:.2} KiB of slab, of which {netlink:.2} KiB for \
             the netlink socket; {:.1} descriptors; a catcher of {} bytes and its pod's \
             names; catch {catch:?}, close call {close:?} (the sockets close in the \
             background)",
            per_pod(slab_after, slab, pods),
            per_pod(fds_after as u64, fds as u64, pods),
            std::mem::size_of::<Entry>() + std::mem::size_of::<Catcher>(),
        );
        drop(catchers);
        drop(sockets);
        drop(namespaces);
    }
}

/// The catchers on a real pod of a node: the pod is caught as a pause begins
/// and hibernated with `smolvm-pods ctl`, a client connects, its first SYN
/// asks for the wake, `ctl wake` resumes the pod while the hold stays, and
/// the release replays the held frames into the new tap0. Each client must
/// connect with no SYN retransmit, and the app must see its address. It runs
/// in the node, as root, for a pod whose app answers HTTP at PORT with
/// `remote=ADDRESS` and a client namespace on cni0, with ROUNDS wakes
/// (default 5) of CLIENTS concurrent clients (default 1):
///
/// ```text
/// SLEEPNET_LIVE_POD=SANDBOX SLEEPNET_LIVE_NETNS=/var/run/netns/cni-... \
/// SLEEPNET_LIVE_CLIENT=/var/run/netns/CLIENT SLEEPNET_LIVE_PORT=8080 \
/// SLEEPNET_LIVE_ROUNDS=5 SLEEPNET_LIVE_CLIENTS=10 \
/// <test binary> --ignored --nocapture sleepnet::netloop::live_tests
/// ```
#[cfg(test)]
mod live_tests {
    use super::*;

    use std::io::{Read, Write};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};
    use std::process::Command;
    use std::time::Duration;

    use crate::probe::NoResponder;

    fn env(key: &str) -> String {
        std::env::var(key).unwrap_or_else(|_| panic!("{key} is not set"))
    }

    fn ctl(args: &[&str]) -> String {
        let out = Command::new("smolvm-pods")
            .arg("ctl")
            .args(args)
            .output()
            .expect("run smolvm-pods ctl");
        assert!(
            out.status.success(),
            "ctl {}: {}{}",
            args.join(" "),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    /// The pod's network identity, read from eth0: the guest's MAC is eth0's
    /// with the locally administered bit flipped back.
    fn pod_net(id: &str, netns: &str) -> PodNet {
        let out = Command::new("nsenter")
            .args([
                &format!("--net={netns}"),
                "ip",
                "-j",
                "addr",
                "show",
                "dev",
                "eth0",
            ])
            .output()
            .unwrap();
        let link: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        let mut mac = crate::net::parse_mac(link[0]["address"].as_str().unwrap()).unwrap();
        mac[0] ^= 2;
        let ip: Ipv4Addr = link[0]["addr_info"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["family"] == "inet")
            .and_then(|a| a["local"].as_str())
            .unwrap()
            .parse()
            .unwrap();
        PodNet {
            sandbox_id: id.to_string(),
            netns: netns.to_string(),
            iface: "eth0".into(),
            ip,
            mac,
        }
    }

    struct Waker(Mutex<mpsc::Sender<(SocketAddrV4, u16, Instant)>>);

    impl TrafficWaker for Waker {
        fn wake(&self, _sandbox_id: &str, from: SocketAddrV4, port: u16) -> bool {
            let _ = lock(&self.0).send((from, port, Instant::now()));
            true
        }

        fn is_own_connection(&self, _sandbox_id: &str, _from: SocketAddrV4, _port: u16) -> bool {
            false
        }
    }

    fn state(id: &str) -> String {
        let pod: serde_json::Value = serde_json::from_str(&ctl(&["get", id])).unwrap();
        pod["state"].as_str().unwrap_or("?").to_string()
    }

    #[test]
    #[ignore = "needs a node, a pod and a client namespace"]
    fn a_sleeping_pod_wakes_on_its_first_syn_and_sees_the_client() {
        let id = env("SLEEPNET_LIVE_POD");
        let net = pod_net(&id, &env("SLEEPNET_LIVE_NETNS"));
        let client = env("SLEEPNET_LIVE_CLIENT");
        let port: u16 = env("SLEEPNET_LIVE_PORT").parse().unwrap();
        let rounds: usize = std::env::var("SLEEPNET_LIVE_ROUNDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5);
        let clients: usize = std::env::var("SLEEPNET_LIVE_CLIENTS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);
        let (tx, wakes) = mpsc::channel();
        let catchers =
            NetCatchers::start(Arc::new(NoResponder), Arc::new(Waker(Mutex::new(tx)))).unwrap();
        let target = SocketAddr::from((net.ip, port));
        for round in 1..=rounds {
            assert_eq!(state(&id), "awake", "round {round}");
            catchers.catch(&net, Phase::Pausing).unwrap();
            ctl(&["hibernate", &id]);
            catchers
                .catch(&net, Phase::Asleep { probes_fail: false })
                .unwrap();
            assert_eq!(state(&id), "asleep");

            let flush = Command::new("nsenter")
                .args([&format!("--net={client}"), "ip", "neigh", "flush", "all"])
                .status()
                .unwrap();
            assert!(flush.success());
            let fetches: Vec<_> = (0..clients)
                .map(|_| {
                    let client_ns = client.clone();
                    std::thread::spawn(move || {
                        let ns = std::fs::File::open(&client_ns).unwrap();
                        nix::sched::setns(ns, nix::sched::CloneFlags::CLONE_NEWNET).unwrap();
                        let started = Instant::now();
                        let mut stream =
                            TcpStream::connect_timeout(&target, Duration::from_secs(5))?;
                        let connected = started.elapsed();
                        stream.write_all(b"GET /live HTTP/1.0\r\n\r\n")?;
                        stream.set_read_timeout(Some(Duration::from_secs(3)))?;
                        let mut reply = String::new();
                        stream.read_to_string(&mut reply)?;
                        Ok::<_, io::Error>((started, connected, started.elapsed(), reply))
                    })
                })
                .collect();

            let (from, to_port, asked) = wakes.recv_timeout(Duration::from_secs(5)).unwrap();
            assert_eq!(to_port, port);
            catchers.catch(&net, Phase::Waking).unwrap();
            ctl(&["wake", &id, "--source", "sleepnet-live-test"]);
            let awake = asked.elapsed();
            let (counts, held) = catchers.counts(&net).unwrap().unwrap();
            let started = Instant::now();
            let released = catchers.release(&net).unwrap();
            let release = started.elapsed();
            let results: Vec<_> = fetches.into_iter().map(|f| f.join().unwrap()).collect();
            let body = |reply: &str| {
                let body = reply.split("\r\n\r\n").nth(1).unwrap_or("");
                body.trim().to_string()
            };
            eprintln!(
                "round {round}: the first SYN asked for the wake {:?} after the first connect; \
                 wake to awake {awake:?}; release {release:?}; {} frames held before the \
                 release, {} replayed; {} ARP replies; {} wakes",
                results
                    .iter()
                    .filter_map(|r| r.as_ref().ok())
                    .map(|r| asked.saturating_duration_since(r.0))
                    .max()
                    .unwrap_or_default(),
                held,
                released.frames,
                counts.arp_replies,
                counts.wakes,
            );
            for result in &results {
                match result {
                    Ok((_, connected, answered, reply)) => eprintln!(
                        "  connected in {connected:?}, answered in {answered:?}: {}",
                        body(reply)
                    ),
                    Err(e) => eprintln!("  failed: {e}"),
                }
            }
            assert_eq!(counts.wakes, 1);
            assert!(released.frames >= clients);
            for result in results {
                let (_, connected, _, reply) = result.unwrap();
                assert!(connected < Duration::from_millis(900), "no SYN retransmit");
                let body = body(&reply);
                assert!(
                    body.starts_with(&format!("remote={}:", from.ip())),
                    "the app sees the client: {body}"
                );
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }
}
