//! One pod's catcher: its packet and netlink sockets, the frame rules, the
//! held frames, the responder, and the end of the hold. [`Rules`] has no
//! sockets, so that tests can drive it with frames.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io;
use std::net::SocketAddrV4;
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::net::TAP_IFACE;
use crate::probe::{Responder, ResponderFactory};
use crate::sync::lock;

use super::capture::{self, Capture, RECV_BUFFER};
use super::frame;
use super::hold::{self, Netlink};
use super::{Frame, FrameClass, Phase, PodNet, Released, VnetHeader};

/// The bounds of the held queue.
const MAX_FRAMES: usize = 1024;
const MAX_BYTES: usize = 4 << 20;
/// Client RSTs held for the guest.
const MAX_RESETS: usize = 64;
/// ARP requests held for the guest, which learns its neighbours' MACs from
/// them.
const MAX_ARPS: usize = 64;
/// How long traffic waits before it asks for a wake again, while the pod is
/// not waking.
const WAKE_AGAIN: Duration = Duration::from_secs(1);
/// The longest wait for the responder's connections before a release.
pub(super) const DRAIN: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HeldFrame {
    pub vnet: VnetHeader,
    pub data: Vec<u8>,
    /// When the kernel received it.
    pub at: SystemTime,
    pub class: FrameClass,
}

/// The held frames, in arrival order.
#[derive(Debug, Default)]
pub(super) struct Held {
    frames: Vec<HeldFrame>,
    bytes: usize,
    resets: usize,
    arps: usize,
    digests: HashSet<u64>,
}

fn digest(frame: &HeldFrame) -> u64 {
    let mut hasher = DefaultHasher::new();
    frame.vnet.to_bytes().hash(&mut hasher);
    frame.data.hash(&mut hasher);
    hasher.finish()
}

impl Held {
    /// Holds `frame`, unless it repeats a held frame exactly or the queue is
    /// full: then the newest frame is the one dropped.
    pub fn push(&mut self, frame: HeldFrame) -> bool {
        let is_reset = frame.class == FrameClass::TcpRst;
        let is_arp = frame.class == FrameClass::ArpRequest;
        if self.frames.len() >= MAX_FRAMES
            || self.bytes + frame.data.len() > MAX_BYTES
            || (is_reset && self.resets >= MAX_RESETS)
            || (is_arp && self.arps >= MAX_ARPS)
        {
            return false;
        }
        let digest = digest(&frame);
        let repeat = |f: &HeldFrame| f.vnet == frame.vnet && f.data == frame.data;
        if self.digests.contains(&digest) && self.frames.iter().any(repeat) {
            return false;
        }
        self.digests.insert(digest);
        self.bytes += frame.data.len();
        self.resets += usize::from(is_reset);
        self.arps += usize::from(is_arp);
        self.frames.push(frame);
        true
    }

    pub fn frames(&self) -> &[HeldFrame] {
        &self.frames
    }

    pub fn len(&self) -> usize {
        self.frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    fn retain(&mut self, keep: impl Fn(&HeldFrame) -> bool) {
        let frames = std::mem::take(&mut self.frames);
        *self = Held::default();
        for frame in frames.into_iter().filter(|f| keep(f)) {
            self.push(frame);
        }
    }
}

/// What a catcher did, for its log line.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct Counts {
    pub arp_replies: u64,
    pub to_responder: u64,
    pub dropped: u64,
    pub wakes: u64,
}

/// What the netloop must do after a frame or a poll.
#[derive(Debug, Default)]
pub(super) struct Effects {
    /// Frames to send out of eth0, toward the node.
    pub out: Vec<Vec<u8>>,
    /// Ask the [`super::TrafficWaker`] to wake the pod for traffic from this
    /// source to this port. [`Rules::woke`] is told if it did.
    pub wake: Option<(SocketAddrV4, u16)>,
}

/// The frame rules of one pod.
pub(super) struct Rules {
    pub pod: PodNet,
    phase: Phase,
    held: Held,
    responder: Option<Box<dyn Responder>>,
    draining: bool,
    /// Frames received earlier passed the bridge before the hold was set.
    held_since: SystemTime,
    last_wake: Option<Instant>,
    /// When the responder wants to be polled again.
    pub poll_at: Option<Instant>,
    pub counts: Counts,
}

impl Rules {
    pub fn new(pod: PodNet, phase: Phase, held_since: SystemTime) -> Rules {
        Rules {
            pod,
            phase,
            held: Held::default(),
            responder: None,
            draining: false,
            held_since,
            last_wake: None,
            poll_at: None,
            counts: Counts::default(),
        }
    }

    #[cfg(test)]
    pub fn held(&self) -> &Held {
        &self.held
    }

    fn probes_fail(&self) -> bool {
        matches!(self.phase, Phase::Asleep { probes_fail: true })
    }

    /// While the pod pauses or sleeps again, held UDP goes, and traffic may
    /// ask for a wake at once.
    pub fn set_phase(&mut self, phase: Phase) {
        self.phase = phase;
        if phase != Phase::Waking {
            self.held.retain(|f| f.class != FrameClass::Udp);
            self.last_wake = None;
        }
        let failing = self.probes_fail();
        if let Some(responder) = &mut self.responder {
            responder.set_failing(failing);
        }
    }

    /// Handles a frame to the pod that the kernel received at `at`.
    pub fn on_frame(
        &mut self,
        vnet: VnetHeader,
        data: &[u8],
        at: SystemTime,
        now: Instant,
        responders: &dyn ResponderFactory,
        fx: &mut Effects,
    ) {
        let packet = frame::parse(data, self.pod.ip);
        match packet.class {
            FrameClass::ArpRequest => {
                if let Some(reply) = frame::arp_reply(data, &self.pod) {
                    fx.out.push(reply);
                    self.counts.arp_replies += 1;
                }
            }
            FrameClass::Other => return,
            _ => {}
        }
        if at < self.held_since {
            return;
        }
        if self.offer(vnet, data, packet.class, now, responders, fx) {
            self.counts.to_responder += 1;
            return;
        }
        let hold = match packet.class {
            // The guest learns the sender's MAC from the request at the
            // release, before the sender's held SYN: a pod IP that a new pod
            // took meanwhile has another MAC.
            FrameClass::ArpRequest => true,
            FrameClass::TcpSyn | FrameClass::TcpData => {
                self.ask_wake(packet.from, packet.port, now, fx);
                true
            }
            FrameClass::TcpRst => true,
            FrameClass::Udp => self.phase == Phase::Waking,
            _ => false,
        };
        let held = hold
            && self.held.push(HeldFrame {
                vnet,
                data: data.to_vec(),
                at,
                class: packet.class,
            });
        if !held {
            self.counts.dropped += 1;
        }
    }

    /// Offers the frame to the responder, made on the first SYN or echo
    /// request. During the drain before a release, new SYNs are for the
    /// guest.
    fn offer(
        &mut self,
        vnet: VnetHeader,
        data: &[u8],
        class: FrameClass,
        now: Instant,
        responders: &dyn ResponderFactory,
        fx: &mut Effects,
    ) -> bool {
        let first = match class {
            FrameClass::TcpSyn => !self.draining,
            FrameClass::IcmpEcho => true,
            FrameClass::TcpData | FrameClass::TcpBare | FrameClass::TcpRst => false,
            _ => return false,
        };
        if vnet.is_gso() || (class == FrameClass::TcpSyn && self.draining) {
            return false;
        }
        if self.responder.is_none() {
            if !first {
                return false;
            }
            let mut responder = responders.responder(&self.pod);
            responder.set_failing(self.probes_fail());
            self.responder = Some(responder);
        }
        let Some(responder) = self.responder.as_mut() else {
            return false;
        };
        let taken = responder.offer(&Frame { vnet, data }, now);
        self.poll(now, fx);
        taken
    }

    fn ask_wake(&mut self, from: SocketAddrV4, port: u16, now: Instant, fx: &mut Effects) {
        if self.phase == Phase::Waking || self.draining {
            return;
        }
        if self
            .last_wake
            .is_some_and(|t| now.duration_since(t) < WAKE_AGAIN)
        {
            return;
        }
        fx.wake = Some((from, port));
    }

    /// The waker woke the pod: traffic waits [`WAKE_AGAIN`] before it asks
    /// again. A frame that the waker declines, such as one of the pod's own
    /// connections, delays no wake.
    pub fn woke(&mut self, now: Instant) {
        self.last_wake = Some(now);
        self.counts.wakes += 1;
    }

    /// Moves the responder on.
    pub fn poll(&mut self, now: Instant, fx: &mut Effects) {
        if let Some(responder) = &mut self.responder {
            self.poll_at = responder.poll(now, &mut fx.out);
        }
    }

    /// Drops the responder, after it panicked.
    pub fn drop_responder(&mut self) {
        self.responder = None;
        self.poll_at = None;
    }

    pub fn start_drain(&mut self) {
        self.draining = true;
    }

    /// Whether the responder has no connection open.
    pub fn drained(&self) -> bool {
        self.responder.as_ref().is_none_or(|r| r.is_idle())
    }

    /// Answers each held SYN and data segment on a connection opened to the
    /// pod with a RST and drops it, and drops held UDP. Segments on a
    /// connection that the pod opened, for which `own(from, port)` is true,
    /// stay for the next wake, as do client RSTs and ARP requests. Returns
    /// the RSTs.
    pub fn refuse(&mut self, mut own: impl FnMut(SocketAddrV4, u16) -> bool) -> Vec<Vec<u8>> {
        let ip = self.pod.ip;
        let connection = |f: &HeldFrame| {
            let packet = frame::parse(&f.data, ip);
            (packet.from, packet.port)
        };
        let mut owned = HashMap::new();
        let mut resets: Vec<Vec<u8>> = Vec::new();
        let refusable =
            |f: &&HeldFrame| matches!(f.class, FrameClass::TcpSyn | FrameClass::TcpData);
        for held in self.held.frames().iter().filter(refusable) {
            let (from, port) = connection(held);
            if *owned.entry((from, port)).or_insert_with(|| own(from, port)) {
                continue;
            }
            if let Some(rst) = frame::reset(&held.data, &self.pod) {
                if !resets.contains(&rst) {
                    resets.push(rst);
                }
            }
        }
        self.held.retain(|f| match f.class {
            FrameClass::TcpRst | FrameClass::ArpRequest => true,
            FrameClass::TcpSyn | FrameClass::TcpData => {
                owned.get(&connection(f)).copied().unwrap_or(false)
            }
            _ => false,
        });
        self.last_wake = None;
        resets
    }

    fn take_held(&mut self) -> Held {
        std::mem::take(&mut self.held)
    }

    fn accepts(&self, at: SystemTime) -> bool {
        at >= self.held_since
    }
}

/// A pod's catcher: its sockets in the pod's network namespace, and its
/// rules.
pub(super) struct Catcher {
    pub rules: Rules,
    pub capture: Capture,
    pub netlink: Netlink,
    /// eth0's index.
    pub eth0: i32,
    pub opened: Instant,
}

impl Catcher {
    /// Opens the catcher of `pod` and sets the hold. Frames that passed the
    /// bridge before the hold are not held; when the port was already held,
    /// as after a restart, none did.
    pub fn open(pod: &PodNet, phase: Phase) -> Result<Catcher, String> {
        let (ip, iface) = (pod.ip, pod.iface.clone());
        let (capture, netlink, eth0, held_since) = capture::in_netns(&pod.netns, move || {
            let mut nl = Netlink::open(true).map_err(|e| format!("open a netlink socket: {e}"))?;
            let link = nl
                .link_named(&iface)
                .map_err(|e| format!("read {iface}: {e}"))?
                .ok_or_else(|| format!("the namespace has no {iface}"))?;
            let capture = Capture::open(link.index, ip)
                .map_err(|e| format!("open a packet socket on {iface}: {e}"))?;
            let before = SystemTime::now();
            let changed = hold::set_state(&mut nl, link.index, &iface, hold::LISTENING)?;
            Ok((
                capture,
                nl,
                link.index,
                if changed { before } else { UNIX_EPOCH },
            ))
        })?;
        Ok(Catcher {
            rules: Rules::new(pod.clone(), phase, held_since),
            capture,
            netlink,
            eth0,
            opened: Instant::now(),
        })
    }

    pub fn id(&self) -> &str {
        &self.rules.pod.sandbox_id
    }

    /// Sends the frames out of eth0. A frame that cannot be sent is lost,
    /// as on a wire.
    pub fn send_out(&self, frames: &[Vec<u8>]) {
        for frame in frames {
            if let Err(e) = self.capture.send(self.eth0, VnetHeader::default(), frame) {
                log::debug!("sleepnet: {}: send a frame out of eth0: {e}", self.id());
            }
        }
    }

    /// Sets the hold again if it was lost. Returns whether it was.
    pub fn check_hold(&mut self) -> Result<bool, String> {
        let iface = self.rules.pod.iface.clone();
        hold::set_state(&mut self.netlink, self.eth0, &iface, hold::LISTENING)
    }

    /// The release, after the drain: sets the port to `forwarding`, reads
    /// the socket until it is empty and keeps the frames received up to the
    /// flip, writes every held frame into tap0 in arrival order with its
    /// virtio-net header, and closes the sockets in the background.
    pub fn release(mut self, closer: &Closer) -> Result<Released, String> {
        let iface = self.rules.pod.iface.clone();
        let t0 = SystemTime::now();
        let flip = hold::set_state(&mut self.netlink, self.eth0, &iface, hold::FORWARDING);
        let t1 = SystemTime::now();
        let mut held = self.rules.take_held();
        let (in_window, after) = self.read_rest(&mut held, t0, t1);
        let first_held = held.frames().first().map(|f| f.at);
        let injected = match &flip {
            Ok(_) => self.inject(&held),
            Err(_) => Ok(0),
        };
        let counts = self.rules.counts;
        log::info!(
            "sleepnet: {}: released after {} ms: {} of {} held frames into tap0, the first \
             received {} ms before; {} frames in the flip window, {} after it; {} ARP \
             replies, {} frames to the responder, {} dropped, {} wakes",
            self.id(),
            self.opened.elapsed().as_millis(),
            injected.as_ref().map_or(0, |n| *n),
            held.len(),
            first_held.map_or(0, ms_since),
            in_window,
            after,
            counts.arp_replies,
            counts.to_responder,
            counts.dropped,
            counts.wakes,
        );
        let id = self.id().to_string();
        closer.close(self);
        match flip {
            Ok(false) => {
                log::warn!("sleepnet: {id}: the hold on {iface} was gone at the release");
            }
            Ok(true) => {}
            Err(e) => return Err(format!("release {id}: {e}")),
        }
        let frames = injected.map_err(|e| format!("release {id}: {e}"))?;
        Ok(Released { frames, first_held })
    }

    /// Reads what is left on the socket into `held`: the frames received up
    /// to `t1`. Returns how many came between `t0` and `t1`, which the
    /// bridge may have delivered too, and how many after.
    fn read_rest(&mut self, held: &mut Held, t0: SystemTime, t1: SystemTime) -> (usize, usize) {
        let mut buf = vec![0u8; RECV_BUFFER];
        let (mut in_window, mut after) = (0, 0);
        loop {
            let received = match self.capture.recv(&mut buf) {
                Ok(Some(r)) => r,
                Ok(None) => break,
                Err(e) => {
                    log::warn!("sleepnet: {}: read eth0 at the release: {e}", self.id());
                    break;
                }
            };
            if received.at > t1 {
                after += 1;
                continue;
            }
            let Some(vnet) = VnetHeader::parse(&buf[..received.len]) else {
                continue;
            };
            let data = &buf[VnetHeader::LEN..received.len];
            let class = frame::parse(data, self.rules.pod.ip).class;
            if received.truncated || class == FrameClass::Other || !self.rules.accepts(received.at)
            {
                continue;
            }
            in_window += usize::from(received.at > t0);
            held.push(HeldFrame {
                vnet,
                data: data.to_vec(),
                at: received.at,
                class,
            });
        }
        (in_window, after)
    }

    /// Writes the held frames into tap0. Returns how many it wrote.
    fn inject(&mut self, held: &Held) -> Result<usize, String> {
        if held.is_empty() {
            return Ok(0);
        }
        let tap = self
            .netlink
            .link_named(TAP_IFACE)
            .map_err(|e| format!("read {TAP_IFACE}: {e}"))?
            .ok_or_else(|| format!("the namespace has no {TAP_IFACE} for the held frames"))?;
        let mut sent = 0;
        for frame in held.frames() {
            match send_waiting(&self.capture, tap.index, frame) {
                Ok(()) => sent += 1,
                Err(e) => log::warn!(
                    "sleepnet: {}: write a held frame into {TAP_IFACE}: {e}",
                    self.id()
                ),
            }
        }
        Ok(sent)
    }

    /// Ends the hold for a pod that failed or is deleted: answers held SYNs
    /// and data on every connection with a RST, sets the port to
    /// `forwarding`, and closes the sockets in the background.
    pub fn close(mut self, closer: &Closer) -> Result<(), String> {
        let resets = self.rules.refuse(|_, _| false);
        self.send_out(&resets);
        let iface = self.rules.pod.iface.clone();
        let result = forward(&mut self.netlink, Some(self.eth0), &iface);
        let id = self.id().to_string();
        closer.close(self);
        result.map_err(|e| format!("close the catcher of {id}: {e}"))
    }
}

fn ms_since(at: SystemTime) -> u128 {
    SystemTime::now()
        .duration_since(at)
        .unwrap_or_default()
        .as_millis()
}

/// Sends a held frame into tap0, waiting a little when the socket's send
/// buffer is full.
fn send_waiting(capture: &Capture, ifindex: i32, frame: &HeldFrame) -> io::Result<()> {
    let mut tries = 0;
    loop {
        match capture.send(ifindex, frame.vnet, &frame.data) {
            Err(e) if e.kind() == io::ErrorKind::WouldBlock && tries < 5 => {
                tries += 1;
                std::thread::sleep(Duration::from_millis(2));
            }
            other => return other,
        }
    }
}

/// Sets the port of `iface` (with index `index`, or found by name) to
/// `forwarding` if it is a held bridge port. A link that is gone or not a
/// port holds nothing.
fn forward(nl: &mut Netlink, index: Option<i32>, iface: &str) -> Result<(), String> {
    let link = match index {
        Some(index) => nl.link(index),
        None => nl.link_named(iface),
    }
    .map_err(|e| format!("read {iface}: {e}"))?;
    match link {
        Some(link) if link.port_state.is_some() => {
            hold::set_state(nl, link.index, iface, hold::FORWARDING).map(|_| ())
        }
        _ => Ok(()),
    }
}

/// Ends a hold that no catcher serves, as after a restart. A namespace that
/// is gone holds nothing.
pub(super) fn forward_without_catcher(pod: &PodNet) -> Result<(), String> {
    if !Path::new(&pod.netns).exists() {
        return Ok(());
    }
    let iface = pod.iface.clone();
    capture::in_netns(&pod.netns, move || {
        let mut nl = Netlink::open(false).map_err(|e| format!("open a netlink socket: {e}"))?;
        forward(&mut nl, None, &iface)
    })
}

/// Closes catchers on a few threads of their own: closing a packet socket
/// waits for RCU, 11 to 50 ms in the nested node.
pub(super) struct Closer {
    tx: mpsc::Sender<Catcher>,
}

impl Closer {
    pub fn start(threads: usize) -> Result<Closer, String> {
        let (tx, rx) = mpsc::channel::<Catcher>();
        let rx = Arc::new(Mutex::new(rx));
        for i in 0..threads {
            let rx = rx.clone();
            std::thread::Builder::new()
                .name(format!("sleepnet-close-{i}"))
                .spawn(move || loop {
                    let next = lock(&rx).recv();
                    let Ok(catcher) = next else {
                        return;
                    };
                    let started = Instant::now();
                    let id = catcher.id().to_string();
                    drop(catcher);
                    log::debug!(
                        "sleepnet: {id}: the catcher closed in {} ms",
                        started.elapsed().as_millis()
                    );
                })
                .map_err(|e| format!("spawn a sleepnet close thread: {e}"))?;
        }
        Ok(Closer { tx })
    }

    pub fn close(&self, catcher: Catcher) {
        if let Err(mpsc::SendError(catcher)) = self.tx.send(catcher) {
            drop(catcher);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::frame::build::*;
    use super::super::frame::{ACK, FIN, RST};
    use super::*;

    use std::net::Ipv4Addr;

    /// What a [`TestResponder`] saw, shared with the test.
    #[derive(Default)]
    struct Seen {
        failing: Option<bool>,
        open: bool,
        /// Frames taken and not yet answered.
        pending: usize,
    }

    /// Takes SYNs to port 8080 from the kubelet, the segments that follow
    /// until a FIN, and echo requests; answers each with one frame when
    /// polled.
    struct TestResponder(Arc<Mutex<Seen>>);

    impl Responder for TestResponder {
        fn offer(&mut self, frame: &Frame<'_>, _now: Instant) -> bool {
            let mut seen = lock(&self.0);
            let packet = frame::parse(frame.data, POD_IP);
            let kubelet = packet.from.ip() == KUBELET.ip() && packet.port == 8080;
            let taken = match packet.class {
                FrameClass::IcmpEcho => true,
                FrameClass::TcpSyn if kubelet => {
                    seen.open = true;
                    true
                }
                FrameClass::TcpData | FrameClass::TcpBare if kubelet && seen.open => {
                    if packet.tcp.is_some_and(|t| t.flags & FIN != 0) {
                        seen.open = false;
                    }
                    true
                }
                _ => false,
            };
            seen.pending += usize::from(taken);
            taken
        }

        fn poll(&mut self, now: Instant, out: &mut Vec<Vec<u8>>) -> Option<Instant> {
            let mut seen = lock(&self.0);
            out.extend((0..seen.pending).map(|_| b"answer".to_vec()));
            seen.pending = 0;
            Some(now + Duration::from_millis(200))
        }

        fn is_idle(&self) -> bool {
            !lock(&self.0).open
        }

        fn set_failing(&mut self, failing: bool) {
            lock(&self.0).failing = Some(failing);
        }
    }

    #[derive(Default)]
    struct Factory {
        seen: Arc<Mutex<Seen>>,
        made: Mutex<usize>,
    }

    impl ResponderFactory for Factory {
        fn responder(&self, _pod: &PodNet) -> Box<dyn Responder> {
            *lock(&self.made) += 1;
            Box::new(TestResponder(self.seen.clone()))
        }
    }

    struct Pod {
        rules: Rules,
        factory: Factory,
        now: Instant,
        declines: bool,
    }

    impl Pod {
        fn new(phase: Phase) -> Pod {
            Pod {
                rules: Rules::new(pod(), phase, UNIX_EPOCH),
                factory: Factory::default(),
                now: Instant::now(),
                declines: false,
            }
        }

        /// A frame, and a waker that wakes the pod unless `declines`.
        fn frame_with(&mut self, vnet: VnetHeader, data: &[u8]) -> Effects {
            let mut fx = Effects::default();
            self.rules.on_frame(
                vnet,
                data,
                SystemTime::now(),
                self.now,
                &self.factory,
                &mut fx,
            );
            if fx.wake.is_some() && !self.declines {
                self.rules.woke(self.now);
            }
            fx
        }

        fn frame(&mut self, data: &[u8]) -> Effects {
            self.frame_with(VnetHeader::default(), data)
        }

        fn held(&self) -> Vec<FrameClass> {
            self.rules.held().frames().iter().map(|f| f.class).collect()
        }
    }

    /// Where an ARP request's sender IP is in the frame.
    const ETH_SENDER_IP: usize = 28;

    fn client(port: u16) -> SocketAddrV4 {
        SocketAddrV4::new(*CLIENT.ip(), port)
    }

    #[test]
    fn arp_requests_for_the_pod_are_answered_and_held_for_the_guest() {
        let mut pod = Pod::new(Phase::Asleep { probes_fail: false });
        let request = arp_request(POD_IP);
        let fx = pod.frame(&request);
        assert_eq!(
            fx.out,
            [frame::arp_reply(&request, &pod.rules.pod).unwrap()]
        );
        assert_eq!(fx.wake, None);
        assert_eq!(pod.frame(&request).out.len(), 1, "a repeat is answered");
        let fx = pod.frame(&arp_request(Ipv4Addr::new(10, 42, 0, 9)));
        assert!(fx.out.is_empty());
        assert_eq!(pod.held(), [FrameClass::ArpRequest], "one copy");
        assert_eq!(pod.rules.counts.arp_replies, 2);

        for i in 0..MAX_ARPS {
            let mut other = request.clone();
            other[ETH_SENDER_IP + 3] = i as u8;
            pod.frame(&other);
        }
        assert_eq!(pod.held().len(), MAX_ARPS);
        assert_eq!(pod.rules.counts.arp_replies, 2 + MAX_ARPS as u64);

        pod.frame(&syn(CLIENT, 8080));
        assert_eq!(pod.rules.refuse(|_, _| false).len(), 1);
        assert_eq!(
            pod.held().len(),
            MAX_ARPS,
            "ARP requests stay for the guest"
        );
    }

    #[test]
    fn a_syn_is_held_and_wakes_the_pod_once_a_second() {
        let mut pod = Pod::new(Phase::Asleep { probes_fail: false });
        let fx = pod.frame(&syn(CLIENT, 8080));
        assert_eq!(fx.wake, Some((CLIENT, 8080)));
        assert!(fx.out.is_empty());
        assert_eq!(pod.frame(&syn(client(40001), 8080)).wake, None);
        assert_eq!(
            pod.frame(&tcp(client(40002), 80, ACK, 5, b"data")).wake,
            None
        );
        pod.now += WAKE_AGAIN;
        assert_eq!(
            pod.frame(&syn(client(40003), 8080)).wake,
            Some((client(40003), 8080))
        );
        assert_eq!(
            pod.held(),
            [
                FrameClass::TcpSyn,
                FrameClass::TcpSyn,
                FrameClass::TcpData,
                FrameClass::TcpSyn
            ]
        );
        assert_eq!(pod.rules.counts.wakes, 2);
    }

    #[test]
    fn a_declined_wake_does_not_delay_the_next() {
        let mut pod = Pod::new(Phase::Asleep { probes_fail: false });
        pod.declines = true;
        let push = tcp(client(40001), 8080, ACK, 5, b"push");
        assert_eq!(pod.frame(&push).wake, Some((client(40001), 8080)));
        pod.declines = false;
        assert_eq!(
            pod.frame(&syn(CLIENT, 8080)).wake,
            Some((CLIENT, 8080)),
            "within the second"
        );
        assert_eq!(pod.frame(&syn(client(40002), 8080)).wake, None);
        assert_eq!(pod.rules.counts.wakes, 1);
        assert_eq!(pod.held().len(), 3, "the declined frame is held too");
    }

    #[test]
    fn a_waking_pod_holds_without_asking_for_a_wake() {
        let mut pod = Pod::new(Phase::Waking);
        assert_eq!(pod.frame(&syn(CLIENT, 8080)).wake, None);
        assert_eq!(pod.held(), [FrameClass::TcpSyn]);
    }

    #[test]
    fn udp_is_held_only_while_waking_and_goes_when_the_pod_sleeps_again() {
        let mut pod = Pod::new(Phase::Pausing);
        pod.frame(&udp(53));
        assert!(pod.held().is_empty());
        pod.rules.set_phase(Phase::Waking);
        let fx = pod.frame(&udp(53));
        assert_eq!(fx.wake, None);
        pod.frame(&syn(CLIENT, 8080));
        assert_eq!(pod.held(), [FrameClass::Udp, FrameClass::TcpSyn]);
        pod.rules.set_phase(Phase::Asleep { probes_fail: true });
        assert_eq!(pod.held(), [FrameClass::TcpSyn]);
    }

    #[test]
    fn bare_acks_are_dropped_and_client_resets_are_kept_up_to_a_limit() {
        let mut pod = Pod::new(Phase::Asleep { probes_fail: false });
        let fx = pod.frame(&tcp(CLIENT, 8080, ACK, 1, &[]));
        assert_eq!(fx.wake, None);
        assert!(pod.held().is_empty());
        for port in 0..MAX_RESETS as u16 + 5 {
            let fx = pod.frame(&tcp(client(port), 8080, RST, 1, &[]));
            assert_eq!(fx.wake, None);
        }
        assert_eq!(pod.held().len(), MAX_RESETS);
        assert_eq!(pod.rules.counts.dropped, 6);
    }

    #[test]
    fn exact_repeats_are_dropped_and_the_queue_is_bounded() {
        let mut pod = Pod::new(Phase::Waking);
        let frame = syn(CLIENT, 8080);
        pod.frame(&frame);
        pod.frame(&frame);
        assert_eq!(pod.held().len(), 1);
        let needs_csum = VnetHeader {
            flags: VnetHeader::F_NEEDS_CSUM,
            ..VnetHeader::default()
        };
        pod.frame_with(needs_csum, &frame);
        assert_eq!(pod.held().len(), 2, "another header is another frame");

        for port in 0..MAX_FRAMES as u16 {
            pod.frame(&syn(client(port), 8080));
        }
        assert_eq!(pod.held().len(), MAX_FRAMES);
        let last = pod.rules.held().frames().last().unwrap();
        assert_eq!(
            frame::parse(&last.data, POD_IP).from.port(),
            MAX_FRAMES as u16 - 3
        );

        let mut big = Held::default();
        let payload = vec![0u8; 60_000];
        let mut n = 0;
        while big.push(HeldFrame {
            vnet: VnetHeader::default(),
            data: tcp(client(n), 8080, ACK, 1, &payload),
            at: SystemTime::now(),
            class: FrameClass::TcpData,
        }) {
            n += 1;
        }
        assert_eq!(n, (MAX_BYTES / 60_054) as u16);
    }

    #[test]
    fn kubelet_probes_go_to_the_responder_which_is_made_on_the_first() {
        let mut pod = Pod::new(Phase::Asleep { probes_fail: true });
        pod.frame(&tcp(KUBELET, 8080, ACK, 1, &[]));
        assert_eq!(*lock(&pod.factory.made), 0, "a bare ACK makes no responder");
        let fx = pod.frame(&syn(KUBELET, 8080));
        assert_eq!(fx.out, [b"answer".to_vec()]);
        assert_eq!(fx.wake, None);
        assert_eq!(lock(&pod.factory.seen).failing, Some(true));
        assert!(pod.rules.poll_at.is_some());
        let fx = pod.frame(&tcp(KUBELET, 8080, ACK | FIN, 2, &[]));
        assert_eq!(fx.out.len(), 1);
        assert!(pod.held().is_empty());
        assert_eq!(pod.rules.counts.to_responder, 2);

        let fx = pod.frame(&syn(CLIENT, 8080));
        assert_eq!(fx.wake, Some((CLIENT, 8080)), "not a probe");
        assert_eq!(pod.held(), [FrameClass::TcpSyn]);
        pod.frame(&echo_request());
        assert_eq!(pod.held().len(), 1, "echo answered");
        assert_eq!(*lock(&pod.factory.made), 1);

        pod.rules.set_phase(Phase::Waking);
        assert_eq!(lock(&pod.factory.seen).failing, Some(false));
    }

    #[test]
    fn segmented_frames_never_go_to_the_responder() {
        let mut pod = Pod::new(Phase::Asleep { probes_fail: false });
        let gso = VnetHeader {
            gso_type: 1,
            gso_size: 1448,
            ..VnetHeader::default()
        };
        pod.frame_with(gso, &syn(KUBELET, 8080));
        assert_eq!(*lock(&pod.factory.made), 0);
        assert_eq!(pod.held(), [FrameClass::TcpSyn]);
    }

    #[test]
    fn during_the_drain_probe_connections_finish_and_new_syns_are_for_the_guest() {
        let mut pod = Pod::new(Phase::Waking);
        pod.frame(&syn(KUBELET, 8080));
        pod.rules.start_drain();
        assert!(!pod.rules.drained());
        let fx = pod.frame(&syn(SocketAddrV4::new(*KUBELET.ip(), 50001), 8080));
        assert!(fx.out.is_empty() && fx.wake.is_none());
        assert_eq!(pod.held(), [FrameClass::TcpSyn]);
        pod.frame(&tcp(KUBELET, 8080, ACK | FIN, 2, &[]));
        assert!(pod.rules.drained());
        assert_eq!(pod.held().len(), 1);
    }

    #[test]
    fn a_refusal_resets_held_syns_and_data_once_each_and_keeps_client_resets() {
        let mut pod = Pod::new(Phase::Asleep { probes_fail: false });
        let first = syn(CLIENT, 8080);
        pod.frame(&first);
        pod.frame_with(
            VnetHeader {
                flags: VnetHeader::F_NEEDS_CSUM,
                ..VnetHeader::default()
            },
            &first,
        );
        pod.frame(&tcp(client(40001), 8080, ACK, 9, b"GET /"));
        pod.frame(&tcp(client(40002), 8080, RST, 1, &[]));
        pod.rules.set_phase(Phase::Waking);
        pod.frame(&udp(53));
        let resets = pod.rules.refuse(|_, _| false);
        assert_eq!(resets.len(), 2, "the repeated SYN gets one RST");
        let flags: Vec<u8> = resets
            .iter()
            .map(|r| frame::parse(r, *CLIENT.ip()).tcp.unwrap().flags)
            .collect();
        assert_eq!(flags, [RST | ACK, RST]);
        assert_eq!(pod.held(), [FrameClass::TcpRst]);

        pod.rules.set_phase(Phase::Asleep { probes_fail: false });
        assert!(pod.frame(&syn(CLIENT, 8080)).wake.is_some());
    }

    #[test]
    fn a_refusal_keeps_the_held_data_of_the_pods_own_connections() {
        let mut pod = Pod::new(Phase::Asleep { probes_fail: false });
        let broker = SocketAddrV4::new(Ipv4Addr::new(10, 43, 0, 10), 5672);
        pod.declines = true;
        pod.frame(&tcp(broker, 41000, ACK, 7, b"message"));
        pod.frame(&tcp(broker, 41000, ACK, 14, b"heartbeat"));
        pod.declines = false;
        pod.frame(&syn(CLIENT, 8080));
        pod.frame(&tcp(client(40001), 8080, ACK, 9, b"GET /"));
        let mut asked = Vec::new();
        let resets = pod.rules.refuse(|from, port| {
            asked.push((from, port));
            from == broker && port == 41000
        });
        assert_eq!(asked.len(), 3, "one lookup for each connection");
        let refused: Vec<SocketAddrV4> = resets
            .iter()
            .map(|r| {
                let packet = frame::parse(r, *CLIENT.ip());
                SocketAddrV4::new(*CLIENT.ip(), packet.port)
            })
            .collect();
        assert_eq!(refused, [CLIENT, client(40001)]);
        assert_eq!(pod.held(), [FrameClass::TcpData, FrameClass::TcpData]);
        assert!(pod
            .rules
            .held()
            .frames()
            .iter()
            .all(|f| frame::parse(&f.data, POD_IP).from == broker));
    }

    #[test]
    fn frames_that_passed_before_the_hold_are_not_held() {
        let mut pod = Pod::new(Phase::Pausing);
        pod.rules.held_since = SystemTime::now() + Duration::from_secs(60);
        assert_eq!(pod.frame(&syn(CLIENT, 8080)).wake, None);
        assert!(pod.held().is_empty());
        assert_eq!(pod.frame(&arp_request(POD_IP)).out.len(), 1);
    }

    #[test]
    fn other_frames_are_dropped_without_a_count() {
        let mut pod = Pod::new(Phase::Waking);
        let mut ipv6 = syn(CLIENT, 8080);
        ipv6[12..14].copy_from_slice(&[0x86, 0xdd]);
        pod.frame(&ipv6);
        assert!(pod.held().is_empty());
        assert_eq!(pod.rules.counts, Counts::default());
    }
}
