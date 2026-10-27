//! The smoltcp interface of one pod, which answers probes while the pod is not awake.
//!
//! The catcher makes a pod's responder with [`ProbeResponders`] on the pod's first
//! probe or echo request. It offers the responder each frame to the pod, sends out of the
//! pod's `eth0` the frames that [`Responder::poll`] returns, and polls again when it says.
//! The responder:
//!
//! - answers ICMP echo requests;
//! - takes a SYN that is a kubelet probe: its destination port is a TCP, HTTP or gRPC
//!   probe port, its source is one of the node's addresses, and conntrack's entry for it
//!   shows no NAT. NodePort traffic reaches the pod from `cni0`'s address too, but NATed;
//! - takes the segments of the connections it has open, and no other frame;
//! - makes a listening socket for each probe SYN before smoltcp sees it, completes partial
//!   checksums, and never takes a segmented (GSO) frame;
//! - answers from the probes' last results: a TCP probe is accepted, an HTTP or HTTPS probe
//!   gets the recorded status, a gRPC probe gets `SERVING` (see `http.rs`, `tls.rs` and
//!   `grpc.rs`). A probe whose result failed with no status, and every probe of a failing
//!   pod, gets a RST;
//! - answers an HTTP request that is not kubelet's with a 503 and wakes the pod with the
//!   [`TrafficWaker`].
//!
//! Before a release the catcher stops offering SYNs and polls until [`Responder::is_idle`],
//! for at most 100 ms.

use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::hash::BuildHasher;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use log::{debug, info, warn};
use rustls::ServerConfig;
use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::socket::tcp;
use smoltcp::time::Instant as SmolInstant;
use smoltcp::wire::{
    ArpOperation, ArpPacket, ArpRepr, EthernetAddress, EthernetFrame, EthernetProtocol,
    EthernetRepr, HardwareAddress, Icmpv4Message, Icmpv4Packet, IpAddress, IpCidr, IpProtocol,
    Ipv4Packet, TcpPacket,
};

use super::grpc::GrpcSession;
use super::http::{self, Answer, RequestReader};
use super::tls::{self, TlsSession};
use super::{PodProbes, ProbeAction, ProbeBook, Responder, ResponderFactory};
use crate::activity::{local_addrs, ConntrackLookup, Protocol, Tuple};
use crate::sleepnet::{checksum, Frame, PodNet, TrafficWaker};
use crate::sync::lock;

/// At most this many probe connections are open at once. A SYN past them wakes the pod.
const CONNS_MAX: usize = 32;
/// A probe connection is reset after this long.
const CONN_LIFETIME: Duration = Duration::from_secs(5);
/// A connection's socket stays this long in TIME-WAIT, to acknowledge a repeated FIN.
const TIME_WAIT: Duration = Duration::from_secs(1);
/// Each connection's receive and send buffers.
const BUFFER: usize = 4096;
/// At most this many echo replies wait for a poll.
const ECHOES_MAX: usize = 64;
/// The largest Ethernet frame, without the FCS.
const FRAME_MAX: usize = 1514;
const ETH_HEADER: usize = 14;
/// How often, at most, the node's addresses are read again for a source that is not one.
const ADDRS_REREAD: Duration = Duration::from_secs(1);

/// The node's own addresses. A kubelet probe comes from one.
pub trait LocalAddrs: Send + Sync {
    fn is_local(&self, ip: Ipv4Addr) -> bool;
}

/// The IPv4 addresses of the interfaces in the caller's network namespace, which must be
/// the node's. They are read again for an address that is not among them, at most once per
/// [`ADDRS_REREAD`].
#[derive(Default)]
pub struct InterfaceAddrs {
    cache: Mutex<(HashSet<Ipv4Addr>, Option<Instant>)>,
}

impl LocalAddrs for InterfaceAddrs {
    fn is_local(&self, ip: Ipv4Addr) -> bool {
        let mut cache = lock(&self.cache);
        let (addrs, read_at) = &mut *cache;
        if addrs.contains(&ip) {
            return true;
        }
        if read_at.is_some_and(|at| at.elapsed() < ADDRS_REREAD) {
            return false;
        }
        *read_at = Some(Instant::now());
        match local_addrs() {
            Ok(list) => *addrs = list,
            Err(e) => warn!("read the node's addresses: {e}"),
        }
        addrs.contains(&ip)
    }
}

/// Makes each pod's responder for the catchers, with what they share: the probe book,
/// conntrack, the waker, the node's addresses and its certificate for HTTPS probes.
pub struct ProbeResponders {
    shared: Arc<Shared>,
}

struct Shared {
    book: Arc<dyn ProbeBook>,
    conntrack: Arc<dyn ConntrackLookup>,
    waker: Arc<dyn TrafficWaker>,
    local: Arc<dyn LocalAddrs>,
    tls: Arc<ServerConfig>,
}

impl ProbeResponders {
    /// Makes the node's self-signed certificate. The node's addresses are read on the
    /// thread that offers frames, which must be in the node's network namespace.
    pub fn new(
        book: Arc<dyn ProbeBook>,
        conntrack: Arc<dyn ConntrackLookup>,
        waker: Arc<dyn TrafficWaker>,
    ) -> Result<ProbeResponders, String> {
        let local = Arc::new(InterfaceAddrs::default());
        ProbeResponders::with_local(book, conntrack, waker, local)
    }

    pub fn with_local(
        book: Arc<dyn ProbeBook>,
        conntrack: Arc<dyn ConntrackLookup>,
        waker: Arc<dyn TrafficWaker>,
        local: Arc<dyn LocalAddrs>,
    ) -> Result<ProbeResponders, String> {
        let tls = tls::server_config()?;
        Ok(ProbeResponders {
            shared: Arc::new(Shared {
                book,
                conntrack,
                waker,
                local,
                tls,
            }),
        })
    }
}

impl ResponderFactory for ProbeResponders {
    fn responder(&self, pod: &PodNet) -> Box<dyn Responder> {
        Box::new(PodResponder {
            pod: pod.clone(),
            shared: self.shared.clone(),
            stack: None,
            echoes: Vec::new(),
            failing: false,
        })
    }
}

/// A pod's responder. Its interface is made on the first probe SYN.
pub struct PodResponder {
    pod: PodNet,
    shared: Arc<Shared>,
    stack: Option<Stack>,
    /// Echo replies to send.
    echoes: Vec<Vec<u8>>,
    failing: bool,
}

impl Responder for PodResponder {
    fn offer(&mut self, frame: &Frame<'_>, now: Instant) -> bool {
        if frame.vnet.is_gso() {
            return false;
        }
        match parse(frame.data, self.pod.ip) {
            Some(Inbound::Echo) if self.echoes.len() < ECHOES_MAX => {
                match echo_reply(frame.data, self.pod.mac) {
                    Some(reply) => {
                        self.echoes.push(reply);
                        true
                    }
                    None => false,
                }
            }
            Some(Inbound::Tcp(segment)) => {
                if let Some(stack) = &mut self.stack {
                    if stack.has_conn(&segment) {
                        if let Some(bytes) = complete(frame) {
                            stack.ingest(bytes, now);
                        }
                        return true;
                    }
                }
                segment.syn && !segment.ack && self.take_syn(frame, &segment, now)
            }
            _ => false,
        }
    }

    fn poll(&mut self, now: Instant, out: &mut Vec<Vec<u8>>) -> Option<Instant> {
        out.append(&mut self.echoes);
        let stack = self.stack.as_mut()?;
        let at = stack.at(now);
        stack.iface.poll(at, &mut stack.wire, &mut stack.sockets);
        for conn in &mut stack.conns {
            let socket = stack.sockets.get_mut::<tcp::Socket>(conn.handle);
            conn.serve(socket, now, &self.pod, &self.shared, self.failing);
        }
        stack.iface.poll(at, &mut stack.wire, &mut stack.sockets);
        out.append(&mut stack.wire.tx);
        stack.sweep(now);
        let next = stack
            .iface
            .poll_at(at, &stack.sockets)
            .map(|at| stack.instant(at));
        let deadlines = stack.conns.iter().map(|conn| conn.deadline(now));
        deadlines.chain(next).min()
    }

    fn is_idle(&self) -> bool {
        self.stack.as_ref().is_none_or(|stack| {
            stack.conns.iter().all(|conn| {
                let state = stack.sockets.get::<tcp::Socket>(conn.handle).state();
                matches!(
                    state,
                    tcp::State::TimeWait | tcp::State::Closed | tcp::State::Listen
                )
            })
        })
    }

    fn set_failing(&mut self, failing: bool) {
        self.failing = failing;
    }
}

impl PodResponder {
    /// Takes a SYN if it is a kubelet probe, and accepts it or answers with a RST.
    fn take_syn(&mut self, frame: &Frame<'_>, syn: &Segment, now: Instant) -> bool {
        let id = &self.pod.sandbox_id;
        let Some(probes) = self.shared.book.probes(id) else {
            return false;
        };
        let Some(mode) = Mode::of(&probes, syn.port) else {
            return false;
        };
        if !self.shared.local.is_local(*syn.peer.ip()) || !self.is_kubelet(syn) {
            return false;
        }
        let accept = match mode {
            _ if self.failing => false,
            Mode::Tcp | Mode::Grpc => match mode.passed(&probes, syn.port) {
                Some(passed) => passed,
                None => return false,
            },
            Mode::Http | Mode::Https => true,
        };
        let app = match accept.then(|| App::new(mode, &self.shared.tls)) {
            None => None,
            Some(Ok(app)) => Some(app),
            Some(Err(e)) => {
                warn!("pod {id}: cannot answer the probe from {}: {e}", syn.peer);
                return false;
            }
        };
        let Some(bytes) = complete(frame) else {
            return false;
        };
        if self.stack.is_none() {
            if !EthernetAddress(self.pod.mac).is_unicast() {
                return false;
            }
            self.stack = Some(Stack::new(&self.pod, now));
        }
        let Some(stack) = self.stack.as_mut() else {
            return false;
        };
        if app.is_some() && stack.conns.len() >= CONNS_MAX {
            return false;
        }
        stack.learn(syn, self.pod.ip, now);
        let handle = app.map(|app| stack.listen(syn, self.pod.ip, app, now));
        stack.ingest(bytes, now);
        if let Some(handle) = handle {
            if stack.sockets.get::<tcp::Socket>(handle).state() == tcp::State::Listen {
                stack.remove(handle);
            }
        }
        debug!(
            "pod {id}: {} the {mode:?} probe from {} to port {}",
            if accept { "accepted" } else { "refused" },
            syn.peer,
            syn.port
        );
        true
    }

    /// Whether conntrack shows the SYN with no NAT, so that it is not NodePort traffic.
    fn is_kubelet(&self, syn: &Segment) -> bool {
        let reply = Tuple {
            protocol: Protocol::Tcp,
            src: SocketAddrV4::new(self.pod.ip, syn.port),
            dst: syn.peer,
        };
        match self.shared.conntrack.by_reply(&reply) {
            Ok(entry) => entry.is_some_and(|e| !e.is_nat()),
            Err(e) => {
                debug!(
                    "pod {}: conntrack has no answer for {}: {e}",
                    self.pod.sandbox_id, syn.peer
                );
                false
            }
        }
    }
}

/// How a connection to a probe port is answered, by the kind of probe on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Mode {
    Tcp,
    Grpc,
    Http,
    Https,
}

impl Mode {
    fn of_action(action: &ProbeAction) -> Option<Mode> {
        Some(match action {
            ProbeAction::Tcp { .. } => Mode::Tcp,
            ProbeAction::Grpc { .. } => Mode::Grpc,
            ProbeAction::Http { https: false, .. } => Mode::Http,
            ProbeAction::Http { https: true, .. } => Mode::Https,
            ProbeAction::Exec { .. } => return None,
        })
    }

    /// The mode of `port`. Two kinds of probe on one port are answered as the one that
    /// reads more: an HTTP probe's connection also serves a TCP probe, which sends nothing.
    fn of(probes: &PodProbes, port: u16) -> Option<Mode> {
        probes
            .probes
            .iter()
            .filter(|p| p.action.port() == Some(port))
            .filter_map(|p| Mode::of_action(&p.action))
            .max()
    }

    /// Whether every recorded result of this kind of probe on `port` passed, or none if
    /// none is recorded.
    fn passed(self, probes: &PodProbes, port: u16) -> Option<bool> {
        let mut results = probes
            .probes
            .iter()
            .filter(|p| p.action.port() == Some(port) && Mode::of_action(&p.action) == Some(self))
            .filter_map(|p| p.last.as_ref())
            .peekable();
        results.peek()?;
        Some(results.all(|r| r.passed))
    }
}

/// What a probe connection does with its bytes.
enum App {
    /// A TCP probe: closed when the client closes.
    Tcp,
    Http(RequestReader),
    Https(Box<TlsSession>, RequestReader),
    Grpc(GrpcSession),
    /// Answered: closed when the client closes.
    Done,
}

impl App {
    fn new(mode: Mode, tls: &Arc<ServerConfig>) -> Result<App, String> {
        Ok(match mode {
            Mode::Tcp => App::Tcp,
            Mode::Http => App::Http(RequestReader::default()),
            Mode::Https => App::Https(Box::new(TlsSession::new(tls)?), RequestReader::default()),
            Mode::Grpc => App::Grpc(GrpcSession::default()),
        })
    }
}

/// What a connection does after a step.
enum Step {
    Go,
    /// Send a RST.
    Reset,
}

/// What an HTTP or HTTPS probe connection answers.
enum Reply {
    Bytes(Vec<u8>),
    /// Not kubelet's: [`http::UNAVAILABLE`], and the pod wakes.
    Wake,
    /// No request arrived: close, as for a TCP probe.
    Close,
    Reset,
}

struct Conn {
    handle: SocketHandle,
    peer: SocketAddrV4,
    port: u16,
    opened: Instant,
    app: App,
    /// Bytes for the client that the socket has no room for yet.
    out: Vec<u8>,
    /// Close the socket once `out` is sent.
    closing: bool,
    /// When the socket was first seen in TIME-WAIT.
    time_wait: Option<Instant>,
}

impl Conn {
    /// Moves the connection on: reads what arrived, answers, and closes.
    fn serve(
        &mut self,
        socket: &mut tcp::Socket<'_>,
        now: Instant,
        pod: &PodNet,
        shared: &Shared,
        failing: bool,
    ) {
        let state = socket.state();
        if state == tcp::State::TimeWait {
            self.time_wait.get_or_insert(now);
            return;
        }
        if now >= self.opened + CONN_LIFETIME {
            socket.abort();
            return;
        }
        if matches!(
            state,
            tcp::State::Listen | tcp::State::SynReceived | tcp::State::Closed
        ) {
            return;
        }
        let mut input = Vec::new();
        let mut buf = [0u8; 1024];
        while let Ok(n @ 1..) = socket.recv_slice(&mut buf) {
            input.extend_from_slice(&buf[..n]);
        }
        let peer_closed = matches!(
            state,
            tcp::State::CloseWait | tcp::State::LastAck | tcp::State::Closing
        );
        match self.step(&input, peer_closed, now, pod, shared, failing) {
            Step::Go => self.flush(socket),
            Step::Reset => socket.abort(),
        }
    }

    fn step(
        &mut self,
        input: &[u8],
        peer_closed: bool,
        now: Instant,
        pod: &PodNet,
        shared: &Shared,
        failing: bool,
    ) -> Step {
        let waited = now >= self.opened + http::REQUEST_WAIT;
        let (peer, port) = (self.peer, self.port);
        match &mut self.app {
            App::Tcp | App::Done => {
                self.closing |= peer_closed;
                Step::Go
            }
            App::Grpc(session) => match session.receive(input, &mut self.out) {
                Ok(goaway) => {
                    self.closing |= goaway || peer_closed;
                    Step::Go
                }
                Err(e) => {
                    debug!("pod {}: gRPC probe from {peer}: {e}", pod.sandbox_id);
                    Step::Reset
                }
            },
            App::Http(head) => {
                head.push(input);
                if !(head.is_done() || peer_closed || waited) {
                    return Step::Go;
                }
                let reply = reply(head, port, pod, shared, failing);
                self.app = App::Done;
                self.closing = true;
                match reply {
                    Reply::Bytes(bytes) => self.out.extend_from_slice(&bytes),
                    Reply::Wake => {
                        self.out.extend_from_slice(http::UNAVAILABLE);
                        wake(pod, shared, peer, port);
                    }
                    Reply::Close => {}
                    Reply::Reset => return Step::Reset,
                }
                Step::Go
            }
            App::Https(session, head) => {
                let mut plain = Vec::new();
                if let Err(e) = session.receive(input, &mut plain) {
                    debug!("pod {}: HTTPS probe from {peer}: {e}", pod.sandbox_id);
                    return Step::Reset;
                }
                head.push(&plain);
                if head.is_done() || peer_closed || waited {
                    let bytes = match reply(head, port, pod, shared, failing) {
                        Reply::Bytes(bytes) => bytes,
                        Reply::Wake => {
                            wake(pod, shared, peer, port);
                            http::UNAVAILABLE.to_vec()
                        }
                        Reply::Close => Vec::new(),
                        Reply::Reset => return Step::Reset,
                    };
                    if session.send_and_close(&bytes).is_err() {
                        return Step::Reset;
                    }
                    self.closing = true;
                }
                session.output(&mut self.out);
                if self.closing {
                    self.app = App::Done;
                }
                Step::Go
            }
        }
    }

    /// Sends what the socket has room for, and closes it once all is sent.
    fn flush(&mut self, socket: &mut tcp::Socket<'_>) {
        if !self.out.is_empty() && socket.can_send() {
            if let Ok(sent) = socket.send_slice(&self.out) {
                self.out.drain(..sent);
            }
        }
        if self.closing && self.out.is_empty() {
            socket.close();
        }
    }

    /// When the connection needs a poll after `now`, whatever smoltcp needs.
    fn deadline(&self, now: Instant) -> Instant {
        if let Some(at) = self.time_wait {
            return at + TIME_WAIT;
        }
        let read = self.opened + http::REQUEST_WAIT;
        match self.app {
            App::Http(_) | App::Https(..) if read > now => read,
            _ => self.opened + CONN_LIFETIME,
        }
    }
}

/// The answer to the HTTP request in `head` to `port`.
fn reply(head: &RequestReader, port: u16, pod: &PodNet, shared: &Shared, failing: bool) -> Reply {
    if failing {
        return Reply::Reset;
    }
    if head.is_empty() {
        return Reply::Close;
    }
    let request = head.request();
    let answer = match shared.book.probes(&pod.sandbox_id) {
        Some(probes) => http::answer(request.as_ref(), &probes, port),
        None => Answer::Wake,
    };
    match answer {
        Answer::Status(status) => Reply::Bytes(http::response(status)),
        Answer::Wake => Reply::Wake,
        Answer::Refuse => Reply::Reset,
    }
}

fn wake(pod: &PodNet, shared: &Shared, peer: SocketAddrV4, port: u16) {
    info!(
        "pod {}: an HTTP request from {peer} to port {port} is not kubelet's probe; waking the pod",
        pod.sandbox_id
    );
    shared.waker.wake(&pod.sandbox_id, peer, port);
}

/// The interface, its device and its sockets.
struct Stack {
    iface: Interface,
    wire: Wire,
    sockets: SocketSet<'static>,
    conns: Vec<Conn>,
    /// The time smoltcp counts from.
    epoch: Instant,
}

impl Stack {
    fn new(pod: &PodNet, now: Instant) -> Stack {
        let mut wire = Wire::default();
        let mut config = Config::new(HardwareAddress::Ethernet(EthernetAddress(pod.mac)));
        config.random_seed = RandomState::new().hash_one(&pod.sandbox_id);
        let mut iface = Interface::new(config, &mut wire, SmolInstant::ZERO);
        // Every source is on the link: replies go to the MAC that the probe came from.
        iface.update_ip_addrs(|addrs| {
            let _ = addrs.push(IpCidr::new(IpAddress::Ipv4(pod.ip), 0));
        });
        Stack {
            iface,
            wire,
            sockets: SocketSet::new(Vec::new()),
            conns: Vec::new(),
            epoch: now,
        }
    }

    fn at(&self, now: Instant) -> SmolInstant {
        let micros = now.saturating_duration_since(self.epoch).as_micros();
        SmolInstant::from_micros(i64::try_from(micros).unwrap_or(i64::MAX))
    }

    fn instant(&self, at: SmolInstant) -> Instant {
        self.epoch + Duration::from_micros(u64::try_from(at.total_micros()).unwrap_or(0))
    }

    fn has_conn(&self, segment: &Segment) -> bool {
        self.conns
            .iter()
            .any(|c| c.peer == segment.peer && c.port == segment.port)
    }

    /// Passes a frame to smoltcp, which may answer it at once.
    fn ingest(&mut self, frame: Vec<u8>, now: Instant) {
        let at = self.at(now);
        self.wire.rx = Some(frame);
        self.iface
            .poll_ingress_single(at, &mut self.wire, &mut self.sockets);
        self.wire.rx = None;
    }

    /// Tells smoltcp the MAC of the SYN's source, as an ARP request from it would: smoltcp
    /// learns neighbours only from ARP, which the catcher answers itself. Its reply is
    /// dropped.
    fn learn(&mut self, syn: &Segment, pod_ip: Ipv4Addr, now: Instant) {
        let eth = EthernetRepr {
            src_addr: syn.mac,
            dst_addr: EthernetAddress::BROADCAST,
            ethertype: EthernetProtocol::Arp,
        };
        let arp = ArpRepr::EthernetIpv4 {
            operation: ArpOperation::Request,
            source_hardware_addr: syn.mac,
            source_protocol_addr: *syn.peer.ip(),
            target_hardware_addr: EthernetAddress([0; 6]),
            target_protocol_addr: pod_ip,
        };
        let mut bytes = vec![0; ETH_HEADER + arp.buffer_len()];
        let mut frame = EthernetFrame::new_unchecked(&mut bytes[..]);
        eth.emit(&mut frame);
        arp.emit(&mut ArpPacket::new_unchecked(frame.payload_mut()));
        self.wire.mute = true;
        self.ingest(bytes, now);
        self.wire.mute = false;
    }

    /// A socket listening for the SYN, and its connection.
    fn listen(&mut self, syn: &Segment, pod_ip: Ipv4Addr, app: App, now: Instant) -> SocketHandle {
        let mut socket = tcp::Socket::new(
            tcp::SocketBuffer::new(vec![0; BUFFER]),
            tcp::SocketBuffer::new(vec![0; BUFFER]),
        );
        socket.set_ack_delay(None);
        socket.set_nagle_enabled(false);
        let _ = socket.listen((IpAddress::Ipv4(pod_ip), syn.port));
        let handle = self.sockets.add(socket);
        let mut out = Vec::new();
        if let App::Grpc(_) = app {
            GrpcSession::start(&mut out);
        }
        self.conns.push(Conn {
            handle,
            peer: syn.peer,
            port: syn.port,
            opened: now,
            app,
            out,
            closing: false,
            time_wait: None,
        });
        handle
    }

    fn remove(&mut self, handle: SocketHandle) {
        self.conns.retain(|c| c.handle != handle);
        self.sockets.remove(handle);
    }

    /// Removes the connections that are over.
    fn sweep(&mut self, now: Instant) {
        let sockets = &mut self.sockets;
        self.conns.retain(|conn| {
            let over = match sockets.get::<tcp::Socket>(conn.handle).state() {
                tcp::State::Closed | tcp::State::Listen => true,
                tcp::State::TimeWait => conn.time_wait.is_some_and(|at| now >= at + TIME_WAIT),
                _ => false,
            };
            if over {
                sockets.remove(conn.handle);
            }
            !over
        });
    }
}

/// The interface's device: the frame being passed in, and the frames smoltcp sends.
#[derive(Default)]
struct Wire {
    rx: Option<Vec<u8>>,
    tx: Vec<Vec<u8>>,
    /// Drop what smoltcp sends.
    mute: bool,
}

struct RxFrame(Vec<u8>);

impl RxToken for RxFrame {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        f(&self.0)
    }
}

struct TxFrame<'a>(&'a mut Wire);

impl TxToken for TxFrame<'_> {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut frame = vec![0; len];
        let result = f(&mut frame);
        if !self.0.mute {
            self.0.tx.push(frame);
        }
        result
    }
}

impl Device for Wire {
    type RxToken<'a>
        = RxFrame
    where
        Self: 'a;
    type TxToken<'a>
        = TxFrame<'a>
    where
        Self: 'a;

    fn receive(&mut self, _now: SmolInstant) -> Option<(RxFrame, TxFrame<'_>)> {
        let frame = self.rx.take()?;
        Some((RxFrame(frame), TxFrame(self)))
    }

    fn transmit(&mut self, _now: SmolInstant) -> Option<TxFrame<'_>> {
        Some(TxFrame(self))
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ethernet;
        caps.max_transmission_unit = FRAME_MAX;
        caps
    }
}

/// A frame to the pod that the responder may take.
enum Inbound {
    Echo,
    Tcp(Segment),
}

/// What the responder reads from a TCP segment.
struct Segment {
    peer: SocketAddrV4,
    port: u16,
    syn: bool,
    ack: bool,
    mac: EthernetAddress,
}

fn parse(data: &[u8], pod_ip: Ipv4Addr) -> Option<Inbound> {
    let eth = EthernetFrame::new_checked(data).ok()?;
    if eth.ethertype() != EthernetProtocol::Ipv4 {
        return None;
    }
    let ip = Ipv4Packet::new_checked(eth.payload()).ok()?;
    if ip.dst_addr() != pod_ip || ip.more_frags() || ip.frag_offset() != 0 {
        return None;
    }
    match ip.next_header() {
        IpProtocol::Tcp => {
            let tcp = TcpPacket::new_checked(ip.payload()).ok()?;
            Some(Inbound::Tcp(Segment {
                peer: SocketAddrV4::new(ip.src_addr(), tcp.src_port()),
                port: tcp.dst_port(),
                syn: tcp.syn(),
                ack: tcp.ack(),
                mac: eth.src_addr(),
            }))
        }
        IpProtocol::Icmp => {
            let icmp = Icmpv4Packet::new_checked(ip.payload()).ok()?;
            (icmp.msg_type() == Icmpv4Message::EchoRequest).then_some(Inbound::Echo)
        }
        _ => None,
    }
}

/// The frame's bytes, with the checksum that a partial one (`VIRTIO_NET_HDR_F_NEEDS_CSUM`)
/// still lacks: the field holds the pseudo-header's sum, and the sum from `csum_start` to
/// the end of the IP packet goes in it. None for a frame whose header does not fit it.
fn complete(frame: &Frame<'_>) -> Option<Vec<u8>> {
    let mut bytes = frame.data.to_vec();
    if !frame.vnet.needs_csum() {
        return Some(bytes);
    }
    let ip = Ipv4Packet::new_checked(bytes.get(ETH_HEADER..)?).ok()?;
    let end = (ETH_HEADER + usize::from(ip.total_len())).min(bytes.len());
    let start = usize::from(frame.vnet.csum_start);
    let at = start + usize::from(frame.vnet.csum_offset);
    if start >= end || at + 2 > end {
        return None;
    }
    let sum = checksum(0, &bytes[start..end]);
    bytes[at..at + 2].copy_from_slice(&sum.to_be_bytes());
    Some(bytes)
}

/// The reply to an ICMP echo request, from the guest's MAC.
fn echo_reply(request: &[u8], mac: [u8; 6]) -> Option<Vec<u8>> {
    let ip = Ipv4Packet::new_checked(request.get(ETH_HEADER..)?).ok()?;
    let mut reply = request
        .get(..ETH_HEADER + usize::from(ip.total_len()))?
        .to_vec();
    let mut eth = EthernetFrame::new_unchecked(&mut reply[..]);
    let client = eth.src_addr();
    eth.set_dst_addr(client);
    eth.set_src_addr(EthernetAddress(mac));
    let mut ip = Ipv4Packet::new_unchecked(eth.payload_mut());
    let (src, dst) = (ip.src_addr(), ip.dst_addr());
    ip.set_src_addr(dst);
    ip.set_dst_addr(src);
    ip.set_hop_limit(64);
    ip.fill_checksum();
    let mut icmp = Icmpv4Packet::new_checked(ip.payload_mut()).ok()?;
    icmp.set_msg_type(Icmpv4Message::EchoReply);
    icmp.fill_checksum();
    Some(reply)
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use smoltcp::phy::ChecksumCapabilities;
    use smoltcp::wire::{
        Icmpv4Repr, Ipv4Repr, TcpControl, TcpRepr, TcpSeqNumber, TcpTimestampRepr,
    };

    use super::*;
    use crate::activity::{ConnEntry, MockConntrack};
    use crate::probe::grpc::tests::{
        client_call, client_close, hex, SERVER_PING_ACK, SERVER_REPLY,
    };
    use crate::probe::http::tests::{http_probe, result, KUBELET_REQUEST};
    use crate::probe::tls::tests::kubelet_client;
    use crate::probe::{MockProbeBook, Probe, ProbeResult, ProbeRole};
    use crate::sleepnet::VnetHeader;

    const POD_IP: Ipv4Addr = Ipv4Addr::new(10, 42, 0, 9);
    const POD_MAC: [u8; 6] = [0xc2, 0x3c, 0x2a, 0x92, 0x39, 0x90];
    /// `cni0`, from which kubelet's probes come.
    const NODE_IP: Ipv4Addr = Ipv4Addr::new(10, 42, 0, 1);
    const NODE_MAC: [u8; 6] = [0x0a, 0x58, 0x0a, 0x2a, 0x00, 0x01];

    const TCP_PORT: u16 = 7070;
    const HTTP_PORT: u16 = 8080;
    const HTTPS_PORT: u16 = 8443;
    const GRPC_PORT: u16 = 9090;

    #[derive(Default)]
    struct Wakes(Mutex<Vec<(String, SocketAddrV4, u16)>>);

    impl TrafficWaker for Wakes {
        fn wake(&self, sandbox_id: &str, from: SocketAddrV4, port: u16) -> bool {
            lock(&self.0).push((sandbox_id.to_string(), from, port));
            true
        }

        fn is_own_connection(&self, _sandbox_id: &str, _from: SocketAddrV4, _port: u16) -> bool {
            false
        }
    }

    struct Local(Ipv4Addr);

    impl LocalAddrs for Local {
        fn is_local(&self, ip: Ipv4Addr) -> bool {
            ip == self.0
        }
    }

    fn probe(action: ProbeAction, last: Option<ProbeResult>) -> Probe {
        Probe {
            container: "app".into(),
            role: ProbeRole::Liveness,
            action,
            period_secs: 10,
            timeout_secs: 1,
            last,
        }
    }

    /// A probe of each kind, each passed before the pod slept.
    fn probes() -> Vec<Probe> {
        vec![
            probe(
                ProbeAction::Tcp { port: TCP_PORT },
                Some(result(true, None)),
            ),
            http_probe(HTTP_PORT, "/healthz", false, Some(result(true, Some(200)))),
            http_probe(HTTPS_PORT, "/healthz", true, Some(result(true, Some(200)))),
            probe(
                ProbeAction::Grpc {
                    port: GRPC_PORT,
                    service: None,
                },
                Some(result(true, Some(0))),
            ),
        ]
    }

    type Segment = (VnetHeader, Vec<u8>);

    /// The node: the probe book, conntrack and the waker around one pod's responder.
    struct Node {
        book: Arc<MockProbeBook>,
        conntrack: Arc<MockConntrack>,
        wakes: Arc<Wakes>,
        responder: Box<dyn Responder>,
        now: Instant,
        next_port: u16,
    }

    impl Node {
        fn new(probes: Vec<Probe>) -> Node {
            let book = Arc::new(MockProbeBook::default());
            let pod_probes = PodProbes {
                probes,
                ready: true,
                at_ms: 1,
            };
            lock(&book.pods).insert("pod".into(), pod_probes);
            let conntrack = Arc::new(MockConntrack::default());
            let wakes = Arc::new(Wakes::default());
            let factory = ProbeResponders::with_local(
                book.clone(),
                conntrack.clone(),
                wakes.clone(),
                Arc::new(Local(NODE_IP)),
            )
            .unwrap();
            let pod = PodNet {
                sandbox_id: "pod".into(),
                netns: "/var/run/netns/cni-1".into(),
                iface: "eth0".into(),
                ip: POD_IP,
                mac: POD_MAC,
            };
            Node {
                book,
                conntrack,
                wakes,
                responder: factory.responder(&pod),
                now: Instant::now(),
                next_port: 40000,
            }
        }

        /// A client on the node to `port` of the pod. Conntrack has its entry, NATed for
        /// NodePort traffic.
        fn client(&mut self, from: Ipv4Addr, port: u16, nat: bool) -> Client {
            self.next_port += 1;
            let client = Client::new(SocketAddrV4::new(from, self.next_port), port);
            let reply = Tuple {
                protocol: Protocol::Tcp,
                src: SocketAddrV4::new(POD_IP, port),
                dst: client.addr,
            };
            let original = if nat {
                Tuple {
                    protocol: Protocol::Tcp,
                    src: "100.96.0.1:50000".parse().unwrap(),
                    dst: "100.96.0.2:31080".parse().unwrap(),
                }
            } else {
                reply.inverse()
            };
            lock(&self.conntrack.entries).push(ConnEntry {
                original,
                reply,
                timeout_secs: 119,
            });
            client
        }

        fn kubelet(&mut self, port: u16) -> Client {
            self.client(NODE_IP, port, false)
        }

        fn offer(&mut self, (vnet, data): &Segment) -> bool {
            let frame = Frame { vnet: *vnet, data };
            self.responder.offer(&frame, self.now)
        }

        fn poll(&mut self) -> Vec<Vec<u8>> {
            let mut out = Vec::new();
            self.responder.poll(self.now, &mut out);
            out
        }

        /// Offers the client's segment, which the responder must take, and passes the
        /// answer back.
        fn exchange(&mut self, client: &mut Client, segment: Segment) {
            assert!(self.offer(&segment), "the responder takes the segment");
            client.take(&self.poll());
        }

        fn connect(&mut self, client: &mut Client) {
            let syn = client.syn();
            self.exchange(client, syn);
            assert!(client.synack, "SYN-ACK");
            let ack = client.ack();
            self.exchange(client, ack);
        }

        fn wakes(&self) -> Vec<(String, SocketAddrV4, u16)> {
            lock(&self.wakes.0).clone()
        }
    }

    /// The client of a probe connection, as the node's kernel sends it through a veth:
    /// Linux's SYN options, and partial checksums.
    struct Client {
        addr: SocketAddrV4,
        port: u16,
        seq: u32,
        ack: u32,
        synack: bool,
        fin: bool,
        rst: bool,
        received: Vec<u8>,
    }

    impl Client {
        fn new(addr: SocketAddrV4, port: u16) -> Client {
            Client {
                addr,
                port,
                seq: 0x8000_0000,
                ack: 0,
                synack: false,
                fin: false,
                rst: false,
                received: Vec::new(),
            }
        }

        fn segment(&self, control: TcpControl, payload: &[u8]) -> Segment {
            let syn = control == TcpControl::Syn;
            let repr = TcpRepr {
                src_port: self.addr.port(),
                dst_port: self.port,
                control,
                seq_number: TcpSeqNumber(self.seq as i32),
                ack_number: (!syn).then_some(TcpSeqNumber(self.ack as i32)),
                window_len: if syn { 64240 } else { 502 },
                window_scale: syn.then_some(7),
                max_seg_size: syn.then_some(1410),
                sack_permitted: syn,
                sack_ranges: [None; 3],
                timestamp: Some(TcpTimestampRepr::new(1_000, 0)),
                payload,
            };
            tcp_frame(&repr, *self.addr.ip())
        }

        fn syn(&mut self) -> Segment {
            let segment = self.segment(TcpControl::Syn, &[]);
            self.seq = self.seq.wrapping_add(1);
            segment
        }

        fn ack(&self) -> Segment {
            self.segment(TcpControl::None, &[])
        }

        fn send(&mut self, data: &[u8]) -> Segment {
            let segment = self.segment(TcpControl::Psh, data);
            self.seq = self.seq.wrapping_add(data.len() as u32);
            segment
        }

        fn fin(&mut self) -> Segment {
            let segment = self.segment(TcpControl::Fin, &[]);
            self.seq = self.seq.wrapping_add(1);
            segment
        }

        /// Takes the responder's frames: each must be IPv4 from the guest's MAC to the
        /// node's, with valid checksums. Follows the ones to this client in order.
        fn take(&mut self, frames: &[Vec<u8>]) {
            for bytes in frames {
                let eth = EthernetFrame::new_checked(&bytes[..]).unwrap();
                assert_eq!(eth.ethertype(), EthernetProtocol::Ipv4, "no ARP");
                assert_eq!(eth.src_addr(), EthernetAddress(POD_MAC));
                assert_eq!(eth.dst_addr(), EthernetAddress(NODE_MAC));
                let ip = Ipv4Packet::new_checked(eth.payload()).unwrap();
                assert!(ip.verify_checksum());
                let tcp = TcpPacket::new_checked(ip.payload()).unwrap();
                let to_us = ip.dst_addr() == *self.addr.ip()
                    && tcp.dst_port() == self.addr.port()
                    && tcp.src_port() == self.port;
                if ip.src_addr() != POD_IP || !to_us {
                    continue;
                }
                let (src, dst) = (IpAddress::Ipv4(POD_IP), IpAddress::Ipv4(*self.addr.ip()));
                assert!(tcp.verify_checksum(&src, &dst));
                let seq = tcp.seq_number().0 as u32;
                if tcp.rst() {
                    self.rst = true;
                } else if tcp.syn() {
                    self.synack = tcp.ack() && tcp.ack_number().0 as u32 == self.seq;
                    self.ack = seq.wrapping_add(1);
                } else if seq == self.ack {
                    self.received.extend_from_slice(tcp.payload());
                    self.ack = self.ack.wrapping_add(tcp.payload().len() as u32);
                    if tcp.fin() {
                        self.ack = self.ack.wrapping_add(1);
                        self.fin = true;
                    }
                }
            }
        }
    }

    /// The frame of a segment from `src`, with the partial checksum a local veth leaves:
    /// only the pseudo-header's sum.
    fn tcp_frame(repr: &TcpRepr<'_>, src: Ipv4Addr) -> Segment {
        let ip = Ipv4Repr {
            src_addr: src,
            dst_addr: POD_IP,
            next_header: IpProtocol::Tcp,
            payload_len: repr.buffer_len(),
            hop_limit: 64,
        };
        let csum_start = ETH_HEADER + ip.buffer_len();
        let mut bytes = vec![0; csum_start + repr.buffer_len()];
        let mut eth = EthernetFrame::new_unchecked(&mut bytes[..]);
        node_to_pod().emit(&mut eth);
        let mut packet = Ipv4Packet::new_unchecked(eth.payload_mut());
        ip.emit(&mut packet, &ChecksumCapabilities::default());
        let mut tcp = TcpPacket::new_unchecked(packet.payload_mut());
        let (from, to) = (IpAddress::Ipv4(src), IpAddress::Ipv4(POD_IP));
        repr.emit(&mut tcp, &from, &to, &ChecksumCapabilities::default());
        let mut pseudo = [0u8; 12];
        pseudo[..4].copy_from_slice(&src.octets());
        pseudo[4..8].copy_from_slice(&POD_IP.octets());
        pseudo[9] = 6;
        pseudo[10..].copy_from_slice(&(repr.buffer_len() as u16).to_be_bytes());
        let partial = (!checksum(0, &pseudo)).to_be_bytes();
        bytes[csum_start + 16..csum_start + 18].copy_from_slice(&partial);
        let vnet = VnetHeader {
            flags: VnetHeader::F_NEEDS_CSUM,
            gso_type: VnetHeader::GSO_NONE,
            hdr_len: (csum_start + repr.header_len()) as u16,
            gso_size: 0,
            csum_start: csum_start as u16,
            csum_offset: 16,
        };
        (vnet, bytes)
    }

    fn node_to_pod() -> EthernetRepr {
        EthernetRepr {
            src_addr: EthernetAddress(NODE_MAC),
            dst_addr: EthernetAddress(POD_MAC),
            ethertype: EthernetProtocol::Ipv4,
        }
    }

    #[test]
    fn a_tcp_probe_is_accepted_and_closed() {
        let mut node = Node::new(probes());
        let mut kubelet = node.kubelet(TCP_PORT);
        node.connect(&mut kubelet);
        assert!(!node.responder.is_idle());

        // Kubelet dials and closes, sending nothing.
        let fin = kubelet.fin();
        node.exchange(&mut kubelet, fin);
        assert!(kubelet.fin, "the responder closes too");
        let ack = kubelet.ack();
        node.exchange(&mut kubelet, ack);
        assert!(node.responder.is_idle());
        assert!(kubelet.received.is_empty() && !kubelet.rst);
        assert!(node.wakes().is_empty());
    }

    #[test]
    fn an_http_probe_gets_the_recorded_status() {
        let mut node = Node::new(probes());
        let mut kubelet = node.kubelet(HTTP_PORT);
        node.connect(&mut kubelet);
        let request = kubelet.send(KUBELET_REQUEST);
        node.exchange(&mut kubelet, request);
        assert_eq!(
            kubelet.received,
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        assert!(kubelet.fin);
        assert!(!node.responder.is_idle(), "kubelet has not closed yet");

        let fin = kubelet.fin();
        node.exchange(&mut kubelet, fin);
        assert!(node.responder.is_idle());
        assert!(node.wakes().is_empty());
    }

    #[test]
    fn a_failing_result_is_answered_as_it_was_recorded() {
        let mut node = Node::new(vec![
            probe(
                ProbeAction::Tcp { port: TCP_PORT },
                Some(result(false, None)),
            ),
            http_probe(HTTP_PORT, "/healthz", false, Some(result(false, Some(500)))),
            http_probe(HTTP_PORT, "/refused", false, Some(result(false, None))),
        ]);
        let mut tcp = node.kubelet(TCP_PORT);
        let syn = tcp.syn();
        node.exchange(&mut tcp, syn);
        assert!(tcp.rst && !tcp.synack, "a refused TCP probe gets a RST");

        let mut http = node.kubelet(HTTP_PORT);
        node.connect(&mut http);
        let request = http.send(KUBELET_REQUEST);
        node.exchange(&mut http, request);
        assert_eq!(http.received, http::response(500));

        let mut refused = node.kubelet(HTTP_PORT);
        node.connect(&mut refused);
        let request = refused.send(b"GET /refused HTTP/1.1\r\nUser-Agent: kube-probe/1.35\r\n\r\n");
        node.exchange(&mut refused, request);
        assert!(refused.rst && refused.received.is_empty());
    }

    #[test]
    fn an_http_request_that_is_not_kubelets_gets_a_503_and_wakes_the_pod() {
        let mut node = Node::new(probes());
        let mut curl = node.kubelet(HTTP_PORT);
        node.connect(&mut curl);
        let request = curl.send(
            b"GET /healthz HTTP/1.1\r\nHost: 10.42.0.9:8080\r\nUser-Agent: curl/8.5.0\r\n\
Accept: */*\r\n\r\n",
        );
        node.exchange(&mut curl, request);
        assert_eq!(curl.received, http::UNAVAILABLE);
        assert!(curl.fin);
        assert_eq!(node.wakes(), [("pod".to_string(), curl.addr, HTTP_PORT)]);
    }

    #[test]
    fn a_request_is_answered_after_at_most_a_second() {
        let mut node = Node::new(probes());
        let mut kubelet = node.kubelet(HTTP_PORT);
        node.connect(&mut kubelet);
        let head = kubelet.send(&KUBELET_REQUEST[..KUBELET_REQUEST.len() - 2]);
        assert!(node.offer(&head));
        let mut out = Vec::new();
        let next = node.responder.poll(node.now, &mut out).unwrap();
        kubelet.take(&out);
        assert!(kubelet.received.is_empty());
        assert!(next <= node.now + http::REQUEST_WAIT);

        node.now += http::REQUEST_WAIT;
        kubelet.take(&node.poll());
        assert_eq!(kubelet.received, http::response(200));
    }

    #[test]
    fn a_tcp_probe_of_an_http_port_is_closed_without_an_answer() {
        let mut node = Node::new(vec![
            probe(
                ProbeAction::Tcp { port: HTTP_PORT },
                Some(result(true, None)),
            ),
            http_probe(HTTP_PORT, "/healthz", false, Some(result(true, Some(200)))),
        ]);
        let mut kubelet = node.kubelet(HTTP_PORT);
        node.connect(&mut kubelet);
        let fin = kubelet.fin();
        node.exchange(&mut kubelet, fin);
        assert!(kubelet.fin && kubelet.received.is_empty() && !kubelet.rst);
        assert!(node.wakes().is_empty());
    }

    #[test]
    fn an_https_probe_gets_the_recorded_status_over_tls() {
        let mut node = Node::new(probes());
        let mut kubelet = node.kubelet(HTTPS_PORT);
        node.connect(&mut kubelet);
        let mut tls = kubelet_client();
        tls.writer().write_all(KUBELET_REQUEST).unwrap();
        let mut answer = Vec::new();
        for _ in 0..10 {
            let mut records = Vec::new();
            while tls.wants_write() {
                tls.write_tls(&mut records).unwrap();
            }
            if !records.is_empty() {
                let segment = kubelet.send(&records);
                assert!(node.offer(&segment));
            }
            let seen = kubelet.received.len();
            kubelet.take(&node.poll());
            let records = kubelet.received[seen..].to_vec();
            tls.read_tls(&mut &records[..]).unwrap();
            tls.process_new_packets().unwrap();
            let _ = tls.reader().read_to_end(&mut answer);
            if kubelet.fin {
                break;
            }
        }
        assert_eq!(tls.alpn_protocol(), Some(&b"http/1.1"[..]));
        assert_eq!(answer, http::response(200));
        assert!(kubelet.fin);
    }

    #[test]
    fn a_grpc_probe_gets_the_87_bytes_that_grpc_go_accepted() {
        let mut node = Node::new(probes());
        let mut kubelet = node.kubelet(GRPC_PORT);
        node.connect(&mut kubelet);
        // The server's SETTINGS comes first: grpc-go sends no call before it.
        assert_eq!(kubelet.received, hex(SERVER_REPLY)[..9]);

        let call = kubelet.send(&client_call());
        node.exchange(&mut kubelet, call);
        assert_eq!(kubelet.received, hex(SERVER_REPLY));

        let close = kubelet.send(&client_close());
        node.exchange(&mut kubelet, close);
        assert_eq!(kubelet.received[87..], hex(SERVER_PING_ACK));
        assert!(kubelet.fin, "the responder closes after GOAWAY");
    }

    #[test]
    fn only_kubelets_syns_to_probe_ports_are_taken() {
        let mut node = Node::new(probes());
        // NodePort traffic reaches the pod from cni0's address too, but NATed.
        let mut nodeport = node.client(NODE_IP, HTTP_PORT, true);
        assert!(!node.offer(&nodeport.syn()));
        // Another pod, straight to the pod's IP.
        let mut other_pod = node.client(Ipv4Addr::new(10, 42, 0, 7), HTTP_PORT, false);
        assert!(!node.offer(&other_pod.syn()));
        // No conntrack entry.
        let mut unknown = Client::new(SocketAddrV4::new(NODE_IP, 39999), HTTP_PORT);
        assert!(!node.offer(&unknown.syn()));
        // Not a probe port.
        let mut web = node.kubelet(80);
        assert!(!node.offer(&web.syn()));
        // Not a SYN, and not of an open connection.
        let mut late = node.kubelet(HTTP_PORT);
        assert!(!node.offer(&late.send(b"GET / HTTP/1.1\r\n\r\n")));
        assert!(!node.offer(&late.ack()));
        // A segmented frame.
        let mut big = node.kubelet(HTTP_PORT);
        let (mut vnet, data) = big.syn();
        vnet.gso_type = 1;
        assert!(!node.offer(&(vnet, data)));

        assert!(node.poll().is_empty());
        assert!(node.responder.is_idle());
        assert!(node.wakes().is_empty());
    }

    #[test]
    fn a_probe_without_a_recorded_result_is_not_taken() {
        let mut node = Node::new(vec![
            probe(ProbeAction::Tcp { port: TCP_PORT }, None),
            probe(
                ProbeAction::Grpc {
                    port: GRPC_PORT,
                    service: None,
                },
                None,
            ),
            http_probe(HTTP_PORT, "/healthz", false, None),
        ]);
        let mut tcp = node.kubelet(TCP_PORT);
        assert!(!node.offer(&tcp.syn()));
        let mut grpc = node.kubelet(GRPC_PORT);
        assert!(!node.offer(&grpc.syn()));

        // The path is known only once the request arrives: a 503, and the pod wakes.
        let mut http = node.kubelet(HTTP_PORT);
        node.connect(&mut http);
        let request = http.send(KUBELET_REQUEST);
        node.exchange(&mut http, request);
        assert_eq!(http.received, http::UNAVAILABLE);
        assert_eq!(node.wakes().len(), 1);

        lock(&node.book.pods).clear();
        let mut unrecorded = node.kubelet(HTTP_PORT);
        assert!(!node.offer(&unrecorded.syn()));
    }

    #[test]
    fn a_failing_pod_gets_a_rst_for_every_probe() {
        let mut node = Node::new(probes());
        node.responder.set_failing(true);
        for port in [TCP_PORT, HTTP_PORT, HTTPS_PORT, GRPC_PORT] {
            let mut kubelet = node.kubelet(port);
            let syn = kubelet.syn();
            node.exchange(&mut kubelet, syn);
            assert!(kubelet.rst && !kubelet.synack, "port {port}");
        }
        assert!(node.responder.is_idle());
    }

    #[test]
    fn a_connection_left_open_is_reset() {
        let mut node = Node::new(probes());
        let mut kubelet = node.kubelet(TCP_PORT);
        node.connect(&mut kubelet);
        node.now += CONN_LIFETIME;
        kubelet.take(&node.poll());
        assert!(kubelet.rst);
        assert!(node.responder.is_idle());
    }

    #[test]
    fn a_half_open_connection_is_polled_at_its_end() {
        let mut node = Node::new(probes());
        let mut kubelet = node.kubelet(HTTP_PORT);
        let syn = kubelet.syn();
        node.exchange(&mut kubelet, syn);
        assert!(kubelet.synack);
        let opened = node.now;
        node.now += http::REQUEST_WAIT + Duration::from_millis(500);
        let mut out = Vec::new();
        let next = node.responder.poll(node.now, &mut out).unwrap();
        assert!(
            next > node.now,
            "no poll in a loop while the handshake waits"
        );
        assert!(next <= opened + CONN_LIFETIME);
        node.now = opened + CONN_LIFETIME;
        kubelet.take(&node.poll());
        assert!(kubelet.rst);
        assert!(node.responder.is_idle());
    }

    #[test]
    fn an_echo_request_is_answered() {
        let mut node = Node::new(probes());
        let echo = Icmpv4Repr::EchoRequest {
            ident: 7,
            seq_no: 2,
            data: b"ping from the node",
        };
        let ip = Ipv4Repr {
            src_addr: NODE_IP,
            dst_addr: POD_IP,
            next_header: IpProtocol::Icmp,
            payload_len: echo.buffer_len(),
            hop_limit: 64,
        };
        let caps = ChecksumCapabilities::default();
        let mut request = vec![0; ETH_HEADER + ip.buffer_len() + echo.buffer_len()];
        let mut eth = EthernetFrame::new_unchecked(&mut request[..]);
        node_to_pod().emit(&mut eth);
        let mut packet = Ipv4Packet::new_unchecked(eth.payload_mut());
        ip.emit(&mut packet, &caps);
        echo.emit(
            &mut Icmpv4Packet::new_unchecked(packet.payload_mut()),
            &caps,
        );
        assert!(node.offer(&(VnetHeader::default(), request)));

        let out = node.poll();
        assert_eq!(out.len(), 1);
        let eth = EthernetFrame::new_checked(&out[0][..]).unwrap();
        assert_eq!(eth.src_addr(), EthernetAddress(POD_MAC));
        assert_eq!(eth.dst_addr(), EthernetAddress(NODE_MAC));
        let ip = Ipv4Packet::new_checked(eth.payload()).unwrap();
        assert!(ip.verify_checksum());
        assert_eq!((ip.src_addr(), ip.dst_addr()), (POD_IP, NODE_IP));
        let icmp = Icmpv4Packet::new_checked(ip.payload()).unwrap();
        assert_eq!(
            Icmpv4Repr::parse(&icmp, &caps).unwrap(),
            Icmpv4Repr::EchoReply {
                ident: 7,
                seq_no: 2,
                data: b"ping from the node",
            }
        );
    }

    #[test]
    fn partial_checksums_are_completed() {
        let mut client = Client::new(SocketAddrV4::new(NODE_IP, 40000), HTTP_PORT);
        for payload in [&b"odd"[..], KUBELET_REQUEST] {
            let (vnet, partial) = client.send(payload);
            let frame = Frame {
                vnet,
                data: &partial,
            };
            let full = complete(&frame).unwrap();
            let ip = Ipv4Packet::new_checked(&full[ETH_HEADER..]).unwrap();
            let tcp = TcpPacket::new_checked(ip.payload()).unwrap();
            let (src, dst) = (IpAddress::Ipv4(NODE_IP), IpAddress::Ipv4(POD_IP));
            assert!(tcp.verify_checksum(&src, &dst), "{payload:?}");
            assert_ne!(full, partial);

            let whole = Frame {
                vnet: VnetHeader::default(),
                ..frame
            };
            assert_eq!(complete(&whole).unwrap(), partial);
            let outside = Frame {
                vnet: VnetHeader {
                    csum_start: 2000,
                    ..vnet
                },
                ..frame
            };
            assert_eq!(complete(&outside), None);
        }
    }

    #[test]
    fn the_node_has_its_loopback_address() {
        let addrs = InterfaceAddrs::default();
        assert!(addrs.is_local(Ipv4Addr::LOCALHOST));
        assert!(!addrs.is_local(Ipv4Addr::new(192, 0, 2, 1)));
    }

    /// The responder behind a tap, as the catcher puts it behind the pod's `eth0`, so that
    /// the kernel's own TCP clients connect to it. The tap has checksum offload, so that
    /// the kernel leaves partial checksums, as a veth does.
    mod tap {
        use std::net::{SocketAddr, TcpStream};
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::SystemTime;

        use rustls::StreamOwned;

        use super::*;
        use crate::test_netns::open_tap;

        const TAP: &str = "floci-probe0";
        const HOST_IP: Ipv4Addr = Ipv4Addr::new(10, 99, 0, 1);
        const POD: Ipv4Addr = Ipv4Addr::new(10, 99, 0, 2);
        const WAIT: Duration = Duration::from_secs(2);

        /// Conntrack with no NAT for any connection.
        struct NoNat;

        impl ConntrackLookup for NoNat {
            fn by_reply(&self, reply: &Tuple) -> Result<Option<ConnEntry>, String> {
                Ok(Some(ConnEntry {
                    original: reply.inverse(),
                    reply: *reply,
                    timeout_secs: 119,
                }))
            }

            fn by_original(&self, original: &Tuple) -> Result<Option<ConnEntry>, String> {
                self.by_reply(&original.inverse())
            }

            fn last_activity(&self, _ip: Ipv4Addr) -> Result<Option<SystemTime>, String> {
                Ok(None)
            }
        }

        #[test]
        #[ignore = "needs root and /dev/net/tun"]
        fn kernel_clients_get_answers_through_a_tap() {
            let tap = open_tap(TAP, true);
            configure(TAP, HOST_IP);
            let book = Arc::new(MockProbeBook::default());
            let pod_probes = PodProbes {
                probes: probes(),
                ready: true,
                at_ms: 1,
            };
            lock(&book.pods).insert("pod".into(), pod_probes);
            let wakes = Arc::new(Wakes::default());
            let local = Arc::new(Local(HOST_IP));
            let factory =
                ProbeResponders::with_local(book, Arc::new(NoNat), wakes.clone(), local).unwrap();
            let pod = PodNet {
                sandbox_id: "pod".into(),
                netns: String::new(),
                iface: TAP.into(),
                ip: POD,
                mac: POD_MAC,
            };
            let responder = factory.responder(&pod);
            let stop = Arc::new(AtomicBool::new(false));
            let server = {
                let stop = stop.clone();
                std::thread::spawn(move || serve(&tap, responder, &stop))
            };

            drop(connect(TCP_PORT));
            assert_eq!(exchange(HTTP_PORT, KUBELET_REQUEST), http::response(200));
            let curl = b"GET /healthz HTTP/1.1\r\nUser-Agent: curl/8.5.0\r\n\r\n";
            assert_eq!(exchange(HTTP_PORT, curl), http::UNAVAILABLE);
            assert_eq!(lock(&wakes.0).len(), 1);

            let mut tls = StreamOwned::new(kubelet_client(), connect(HTTPS_PORT));
            tls.write_all(KUBELET_REQUEST).unwrap();
            let mut answer = Vec::new();
            tls.read_to_end(&mut answer).unwrap();
            assert_eq!(answer, http::response(200));
            assert_eq!(tls.conn.alpn_protocol(), Some(&b"http/1.1"[..]));

            let mut grpc = connect(GRPC_PORT);
            let mut reply = vec![0; 87];
            grpc.read_exact(&mut reply[..9]).unwrap();
            grpc.write_all(&client_call()).unwrap();
            grpc.read_exact(&mut reply[9..]).unwrap();
            assert_eq!(reply, hex(SERVER_REPLY));
            grpc.write_all(&client_close()).unwrap();
            let mut rest = Vec::new();
            grpc.read_to_end(&mut rest).unwrap();
            assert_eq!(rest, hex(SERVER_PING_ACK));

            stop.store(true, Ordering::SeqCst);
            server.join().unwrap();
        }

        fn connect(port: u16) -> TcpStream {
            let stream = TcpStream::connect_timeout(&SocketAddr::from((POD, port)), WAIT).unwrap();
            stream.set_read_timeout(Some(WAIT)).unwrap();
            stream
        }

        fn exchange(port: u16, request: &[u8]) -> Vec<u8> {
            let mut stream = connect(port);
            stream.write_all(request).unwrap();
            let mut answer = Vec::new();
            stream.read_to_end(&mut answer).unwrap();
            answer
        }

        /// The catcher's netloop for one pod: ARP answered, every other frame offered.
        fn serve(tap: &OwnedFd, mut responder: Box<dyn Responder>, stop: &AtomicBool) {
            let mut buf = vec![0u8; 65536];
            let mut next: Option<Instant> = None;
            while !stop.load(Ordering::SeqCst) {
                let wait = next.map_or(Duration::from_millis(20), |at| {
                    at.saturating_duration_since(Instant::now())
                });
                let mut poll = libc::pollfd {
                    fd: tap.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                let wait = wait.min(Duration::from_millis(20)).as_millis() as i32;
                // SAFETY: one valid pollfd.
                if unsafe { libc::poll(&mut poll, 1, wait) } > 0 {
                    // SAFETY: reads into a buffer of its length.
                    let n =
                        unsafe { libc::read(tap.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
                    let n = usize::try_from(n).unwrap_or(0);
                    if n > VnetHeader::LEN {
                        let vnet = VnetHeader::parse(&buf).unwrap();
                        let data = &buf[VnetHeader::LEN..n];
                        match arp_reply(data) {
                            Some(reply) => write(tap, &reply),
                            None => {
                                let frame = Frame { vnet, data };
                                responder.offer(&frame, Instant::now());
                            }
                        }
                    }
                }
                let mut out = Vec::new();
                next = responder.poll(Instant::now(), &mut out);
                for frame in out {
                    write(tap, &frame);
                }
            }
        }

        fn write(tap: &OwnedFd, frame: &[u8]) {
            let mut bytes = VnetHeader::default().to_bytes().to_vec();
            bytes.extend_from_slice(frame);
            // SAFETY: writes a buffer of its length.
            let n = unsafe { libc::write(tap.as_raw_fd(), bytes.as_ptr().cast(), bytes.len()) };
            assert_eq!(
                n,
                bytes.len() as isize,
                "write: {}",
                std::io::Error::last_os_error()
            );
        }

        /// The reply to the kernel's ARP request for the pod.
        fn arp_reply(data: &[u8]) -> Option<Vec<u8>> {
            let eth = EthernetFrame::new_checked(data).ok()?;
            if eth.ethertype() != EthernetProtocol::Arp {
                return None;
            }
            let ArpRepr::EthernetIpv4 {
                operation: ArpOperation::Request,
                source_hardware_addr,
                source_protocol_addr,
                target_protocol_addr: POD,
                ..
            } = ArpRepr::parse(&ArpPacket::new_checked(eth.payload()).ok()?).ok()?
            else {
                return None;
            };
            let arp = ArpRepr::EthernetIpv4 {
                operation: ArpOperation::Reply,
                source_hardware_addr: EthernetAddress(POD_MAC),
                source_protocol_addr: POD,
                target_hardware_addr: source_hardware_addr,
                target_protocol_addr: source_protocol_addr,
            };
            let mut reply = vec![0; ETH_HEADER + arp.buffer_len()];
            let mut frame = EthernetFrame::new_unchecked(&mut reply[..]);
            let eth = EthernetRepr {
                src_addr: EthernetAddress(POD_MAC),
                dst_addr: source_hardware_addr,
                ethertype: EthernetProtocol::Arp,
            };
            eth.emit(&mut frame);
            arp.emit(&mut ArpPacket::new_unchecked(frame.payload_mut()));
            Some(reply)
        }

        fn ifname(name: &str) -> [libc::c_char; libc::IFNAMSIZ] {
            let mut ifname = [0; libc::IFNAMSIZ];
            for (dst, src) in ifname.iter_mut().zip(name.bytes()) {
                *dst = src as libc::c_char;
            }
            ifname
        }

        /// Gives the tap `ip`/24 and sets it up.
        fn configure(name: &str, ip: Ipv4Addr) {
            #[repr(C)]
            struct IfreqAddr {
                name: [libc::c_char; libc::IFNAMSIZ],
                addr: libc::sockaddr_in,
                _pad: [u8; 8],
            }
            #[repr(C)]
            struct IfreqFlags {
                name: [libc::c_char; libc::IFNAMSIZ],
                flags: libc::c_short,
                _pad: [u8; 22],
            }
            // SAFETY: a plain socket for interface ioctls.
            let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0) };
            assert!(fd >= 0, "socket: {}", std::io::Error::last_os_error());
            // SAFETY: `fd` is a fresh fd.
            let sock = unsafe { OwnedFd::from_raw_fd(fd) };
            let mask = Ipv4Addr::new(255, 255, 255, 0);
            for (request, addr) in [(libc::SIOCSIFADDR, ip), (libc::SIOCSIFNETMASK, mask)] {
                let mut req = IfreqAddr {
                    name: ifname(name),
                    addr: libc::sockaddr_in {
                        sin_family: libc::AF_INET as libc::sa_family_t,
                        sin_port: 0,
                        sin_addr: libc::in_addr {
                            s_addr: u32::from(addr).to_be(),
                        },
                        sin_zero: [0; 8],
                    },
                    _pad: [0; 8],
                };
                // SAFETY: an address ioctl with a valid ifreq.
                let rc = unsafe { libc::ioctl(sock.as_raw_fd(), request as _, &mut req) };
                assert_eq!(rc, 0, "{request:#x}: {}", std::io::Error::last_os_error());
            }
            let mut req = IfreqFlags {
                name: ifname(name),
                flags: 0,
                _pad: [0; 22],
            };
            // SAFETY: flag ioctls with a valid ifreq.
            unsafe {
                assert_eq!(
                    libc::ioctl(sock.as_raw_fd(), libc::SIOCGIFFLAGS as _, &mut req),
                    0
                );
                req.flags |= libc::IFF_UP as libc::c_short;
                assert_eq!(
                    libc::ioctl(sock.as_raw_fd(), libc::SIOCSIFFLAGS as _, &mut req),
                    0
                );
            }
        }
    }
}
