//! ctnetlink: the node-wide dump, the lookup by reply tuple, and
//! [`is_probe`], which tells kubelet probes from other traffic.
//!
//! This speaks ctnetlink on a raw `NETLINK_NETFILTER` socket: it builds the
//! two requests by hand and parses the IPv4 TCP and UDP entries of the
//! replies into [`Conn`]s. When an entry last saw a packet comes from
//! `CTA_TIMEOUT` and the node's timeout sysctls ([`Timeouts::age`]). With
//! `nf_conntrack_acct` on, which `floci-node` sets, an entry also counts
//! packets and bytes in each direction.

use std::collections::HashSet;
use std::io;
use std::mem;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

use super::{ConnEntry, Protocol, Tuple};
use crate::netlink::{
    self, Message, NLA_F_NESTED, NLMSG_DONE, NLMSG_ERROR, NLM_F_ACK, NLM_F_DUMP, NLM_F_REQUEST,
};
use crate::sync::lock;

// <linux/netfilter/nfnetlink.h> and <linux/netfilter/nfnetlink_conntrack.h>.
// Message types are NFNL_SUBSYS_CTNETLINK (1) << 8 | IPCTNL_MSG_CT_*.
const NFGEN_HDRLEN: usize = 4;
const NFNETLINK_V0: u8 = 0;
/// The `nfgenmsg` of an IPv4 request.
const NFGENMSG: [u8; NFGEN_HDRLEN] = [libc::AF_INET as u8, NFNETLINK_V0, 0, 0];
const CT_NEW: u16 = 0x100;
const CT_GET: u16 = 0x101;
const CTA_TUPLE_ORIG: u16 = 1;
const CTA_TUPLE_REPLY: u16 = 2;
const CTA_STATUS: u16 = 3;
const CTA_PROTOINFO: u16 = 4;
const CTA_TIMEOUT: u16 = 7;
const CTA_COUNTERS_ORIG: u16 = 9;
const CTA_COUNTERS_REPLY: u16 = 10;
const CTA_ID: u16 = 12;
const CTA_TIMESTAMP: u16 = 20;
const CTA_TUPLE_IP: u16 = 1;
const CTA_TUPLE_PROTO: u16 = 2;
const CTA_IP_V4_SRC: u16 = 1;
const CTA_IP_V4_DST: u16 = 2;
const CTA_PROTO_NUM: u16 = 1;
const CTA_PROTO_SRC_PORT: u16 = 2;
const CTA_PROTO_DST_PORT: u16 = 3;
const CTA_PROTOINFO_TCP: u16 = 1;
const CTA_PROTOINFO_TCP_STATE: u16 = 1;
const CTA_COUNTERS_PACKETS: u16 = 1;
const CTA_COUNTERS_BYTES: u16 = 2;
const CTA_COUNTERS32_PACKETS: u16 = 3;
const CTA_COUNTERS32_BYTES: u16 = 4;
const CTA_TIMESTAMP_START: u16 = 1;

// <linux/netfilter/nf_conntrack_common.h>
const IPS_ASSURED: u32 = 1 << 2;

const IPPROTO_TCP: u8 = 6;
const IPPROTO_UDP: u8 = 17;

/// A recv of a dump gets at most one 32 KiB skb, so this never truncates.
const RECV_BUFFER: usize = 64 * 1024;
const RECV_TIMEOUT_SECS: libc::time_t = 5;

const SYSCTLS: &str = "/proc/sys/net/netfilter";

/// Packets and bytes in one direction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Count {
    pub packets: u64,
    pub bytes: u64,
}

/// An entry's counters, from `nf_conntrack_acct`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counters {
    pub original: Count,
    pub reply: Count,
}

/// A conntrack entry as ctnetlink reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Conn {
    /// `timeout_secs` is `u32::MAX` if the kernel did not report it, so that
    /// the entry's age reads 0.
    pub entry: ConnEntry,
    /// `CTA_ID`, which with the original tuple names the entry while it
    /// lives.
    pub id: u32,
    /// `CTA_STATUS`, the `IPS_*` bits.
    pub status: u32,
    /// The TCP state, as `enum tcp_conntrack` numbers it.
    pub tcp_state: Option<u8>,
    /// Only if `nf_conntrack_acct` was on when the entry was made.
    pub counters: Option<Counters>,
    /// When the entry was made, in ns since the epoch, only with
    /// `nf_conntrack_timestamp` on.
    pub start_ns: Option<u64>,
}

/// How a reply ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum End {
    /// `NLMSG_DONE` of a dump, or the ACK of a request.
    Done,
    /// A positive errno.
    Error(i32),
}

impl End {
    fn of(code: i32) -> End {
        match code {
            0 => End::Done,
            code => End::Error(code.saturating_neg()),
        }
    }
}

fn find(buf: &[u8], kind: u16) -> Option<&[u8]> {
    netlink::attrs(buf)
        .find(|(k, _)| *k == kind)
        .map(|(_, v)| v)
}

fn be16(value: &[u8]) -> Option<u16> {
    Some(u16::from_be_bytes(value.get(..2)?.try_into().ok()?))
}

fn be32(value: &[u8]) -> Option<u32> {
    Some(u32::from_be_bytes(value.get(..4)?.try_into().ok()?))
}

fn be64(value: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(value.get(..8)?.try_into().ok()?))
}

fn tuple(buf: &[u8]) -> Option<Tuple> {
    let ip = find(buf, CTA_TUPLE_IP)?;
    let src = Ipv4Addr::from(be32(find(ip, CTA_IP_V4_SRC)?)?);
    let dst = Ipv4Addr::from(be32(find(ip, CTA_IP_V4_DST)?)?);
    let proto = find(buf, CTA_TUPLE_PROTO)?;
    let protocol = match *find(proto, CTA_PROTO_NUM)?.first()? {
        IPPROTO_TCP => Protocol::Tcp,
        IPPROTO_UDP => Protocol::Udp,
        _ => return None,
    };
    Some(Tuple {
        protocol,
        src: SocketAddrV4::new(src, be16(find(proto, CTA_PROTO_SRC_PORT)?)?),
        dst: SocketAddrV4::new(dst, be16(find(proto, CTA_PROTO_DST_PORT)?)?),
    })
}

fn count(buf: &[u8]) -> Option<Count> {
    let (mut packets, mut bytes) = (None, None);
    for (kind, value) in netlink::attrs(buf) {
        match kind {
            CTA_COUNTERS_PACKETS => packets = be64(value),
            CTA_COUNTERS_BYTES => bytes = be64(value),
            CTA_COUNTERS32_PACKETS => packets = be32(value).map(u64::from),
            CTA_COUNTERS32_BYTES => bytes = be32(value).map(u64::from),
            _ => {}
        }
    }
    Some(Count {
        packets: packets?,
        bytes: bytes?,
    })
}

/// The entry in the attributes of a `CT_NEW` message, if it is TCP or UDP.
fn conn(buf: &[u8]) -> Option<Conn> {
    let (mut original, mut reply, mut timeout) = (None, None, None);
    let (mut status, mut id, mut tcp_state, mut start_ns) = (0, 0, None, None);
    let (mut original_count, mut reply_count) = (None, None);
    for (kind, value) in netlink::attrs(buf) {
        match kind {
            CTA_TUPLE_ORIG => original = tuple(value),
            CTA_TUPLE_REPLY => reply = tuple(value),
            CTA_STATUS => status = be32(value).unwrap_or(0),
            CTA_TIMEOUT => timeout = be32(value),
            CTA_PROTOINFO => {
                tcp_state = find(value, CTA_PROTOINFO_TCP)
                    .and_then(|tcp| find(tcp, CTA_PROTOINFO_TCP_STATE))
                    .and_then(|state| state.first().copied())
            }
            CTA_COUNTERS_ORIG => original_count = count(value),
            CTA_COUNTERS_REPLY => reply_count = count(value),
            CTA_ID => id = be32(value).unwrap_or(0),
            CTA_TIMESTAMP => start_ns = find(value, CTA_TIMESTAMP_START).and_then(be64),
            _ => {}
        }
    }
    Some(Conn {
        entry: ConnEntry {
            original: original?,
            reply: reply?,
            timeout_secs: timeout.unwrap_or(u32::MAX),
        },
        id,
        status,
        tcp_state,
        counters: original_count
            .zip(reply_count)
            .map(|(original, reply)| Counters { original, reply }),
        start_ns,
    })
}

/// Parses one recv of netlink messages: appends the IPv4 TCP and UDP
/// entries of messages with sequence number `seq` to `out`, and returns how
/// the reply ended if it did. Messages of other sequence numbers are skipped.
fn parse(buf: &[u8], seq: u32, out: &mut Vec<Conn>) -> Result<Option<End>, String> {
    for message in netlink::messages(buf) {
        let (kind, message_seq, body) = message?;
        if message_seq != seq {
            continue;
        }
        match kind {
            NLMSG_DONE => return Ok(Some(End::of(netlink::error_code(body).unwrap_or(0)))),
            NLMSG_ERROR => {
                let code =
                    netlink::error_code(body).ok_or("a netlink error message with no code")?;
                return Ok(Some(End::of(code)));
            }
            CT_NEW if body.first() == Some(&(libc::AF_INET as u8)) => {
                out.extend(body.get(NFGEN_HDRLEN..).and_then(conn));
            }
            _ => {}
        }
    }
    Ok(None)
}

fn dump_request(seq: u32) -> Vec<u8> {
    Message::new(CT_GET, NLM_F_REQUEST | NLM_F_DUMP, &NFGENMSG).finish(seq)
}

fn tuple_attrs(m: &mut Message, t: &Tuple) {
    m.nest(NLA_F_NESTED | CTA_TUPLE_IP, |m| {
        m.attr(CTA_IP_V4_SRC, &t.src.ip().octets());
        m.attr(CTA_IP_V4_DST, &t.dst.ip().octets());
    });
    let protocol = match t.protocol {
        Protocol::Tcp => IPPROTO_TCP,
        Protocol::Udp => IPPROTO_UDP,
    };
    m.nest(NLA_F_NESTED | CTA_TUPLE_PROTO, |m| {
        m.attr(CTA_PROTO_NUM, &[protocol]);
        m.attr(CTA_PROTO_SRC_PORT, &t.src.port().to_be_bytes());
        m.attr(CTA_PROTO_DST_PORT, &t.dst.port().to_be_bytes());
    });
}

fn get_request(seq: u32, reply: &Tuple) -> Vec<u8> {
    let mut m = Message::new(CT_GET, NLM_F_REQUEST | NLM_F_ACK, &NFGENMSG);
    m.nest(NLA_F_NESTED | CTA_TUPLE_REPLY, |m| tuple_attrs(m, reply));
    m.finish(seq)
}

/// A `NETLINK_NETFILTER` socket.
struct Socket(OwnedFd);

impl Socket {
    fn open() -> Result<Socket, String> {
        let fd = netlink::socket(libc::NETLINK_NETFILTER, 0)
            .map_err(|e| format!("ctnetlink socket: {e}"))?;
        let timeout = libc::timeval {
            tv_sec: RECV_TIMEOUT_SECS,
            tv_usec: 0,
        };
        netlink::set_option(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            &timeout,
        )
        .map_err(|e| format!("ctnetlink receive timeout: {e}"))?;
        Ok(Socket(fd))
    }

    /// Sends `request` to the kernel, and parses the reply into `out` until
    /// it ends. `raw` gets each recv's bytes, for tests that record them.
    fn exchange(
        &self,
        request: &[u8],
        seq: u32,
        out: &mut Vec<Conn>,
        mut raw: impl FnMut(&[u8]),
    ) -> Result<End, String> {
        // SAFETY: an all-zero sockaddr_nl is valid; the kernel is port 0.
        let mut kernel: libc::sockaddr_nl = unsafe { mem::zeroed() };
        kernel.nl_family = libc::AF_NETLINK as libc::sa_family_t;
        // SAFETY: the buffer and the address live for the call.
        let sent = unsafe {
            libc::sendto(
                self.0.as_raw_fd(),
                request.as_ptr().cast(),
                request.len(),
                0,
                (&kernel as *const libc::sockaddr_nl).cast(),
                mem::size_of::<libc::sockaddr_nl>() as libc::socklen_t,
            )
        };
        if sent < 0 {
            return Err(format!("ctnetlink send: {}", io::Error::last_os_error()));
        }
        let mut buf = vec![0u8; RECV_BUFFER];
        loop {
            // SAFETY: recv into buf, which lives for the call. MSG_TRUNC
            // returns the full length of a message that did not fit.
            let n = unsafe {
                libc::recv(
                    self.0.as_raw_fd(),
                    buf.as_mut_ptr().cast(),
                    buf.len(),
                    libc::MSG_TRUNC,
                )
            };
            if n < 0 {
                let e = io::Error::last_os_error();
                if e.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(format!("ctnetlink receive: {e}"));
            }
            let n = n as usize;
            if n > buf.len() {
                return Err(format!("a ctnetlink reply of {n} bytes was truncated"));
            }
            raw(&buf[..n]);
            if let Some(end) = parse(&buf[..n], seq, out)? {
                return Ok(end);
            }
        }
    }
}

/// The node's conntrack table over ctnetlink. Dumps and lookups each have a
/// socket, so that a lookup for a probe SYN never waits for a dump. A socket
/// that fails is closed, and the next call opens another.
pub struct Ctnetlink {
    dumps: Mutex<Option<Socket>>,
    gets: Mutex<Option<Socket>>,
    seq: AtomicU32,
}

impl Default for Ctnetlink {
    fn default() -> Ctnetlink {
        Ctnetlink {
            dumps: Mutex::new(None),
            gets: Mutex::new(None),
            seq: AtomicU32::new(1),
        }
    }
}

impl Ctnetlink {
    /// Every IPv4 TCP and UDP entry of the node's table. The kernel walks the
    /// whole table for any dump, filtered or not.
    pub fn dump(&self) -> Result<Vec<Conn>, String> {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let mut out = Vec::new();
        match exchange(&self.dumps, &dump_request(seq), seq, &mut out, |_| {})? {
            End::Done => Ok(out),
            End::Error(e) => Err(format!(
                "conntrack dump: {}",
                io::Error::from_raw_os_error(e)
            )),
        }
    }

    /// The entry whose reply tuple is `reply`, by one `GET`.
    pub fn get(&self, reply: &Tuple) -> Result<Option<Conn>, String> {
        self.get_raw(reply, |_| {})
    }

    fn get_raw(&self, reply: &Tuple, raw: impl FnMut(&[u8])) -> Result<Option<Conn>, String> {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let mut out = Vec::new();
        match exchange(&self.gets, &get_request(seq, reply), seq, &mut out, raw)? {
            End::Done => Ok(out.pop()),
            End::Error(libc::ENOENT) => Ok(None),
            End::Error(e) => Err(format!(
                "conntrack get: {}",
                io::Error::from_raw_os_error(e)
            )),
        }
    }
}

fn exchange(
    slot: &Mutex<Option<Socket>>,
    request: &[u8],
    seq: u32,
    out: &mut Vec<Conn>,
    raw: impl FnMut(&[u8]),
) -> Result<End, String> {
    let mut slot = lock(slot);
    let socket = match slot.take() {
        Some(socket) => socket,
        None => Socket::open()?,
    };
    let end = socket.exchange(request, seq, out, raw)?;
    *slot = Some(socket);
    Ok(end)
}

/// The node's conntrack timeouts in seconds, from its sysctls, which
/// kube-proxy changes: it sets `established` to a day.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timeouts {
    /// By TCP state, `enum tcp_conntrack` from `SYN_SENT` (1) to
    /// `SYN_SENT2` (9).
    pub tcp: [u32; 10],
    /// `max_retrans` and `unacknowledged`, which replace a longer state
    /// timeout while a connection retransmits or has data in flight.
    pub tcp_retrans: u32,
    pub tcp_unack: u32,
    /// A UDP entry's timeout, and that of a stream (`IPS_ASSURED`).
    pub udp: u32,
    pub udp_stream: u32,
}

impl Default for Timeouts {
    /// The kernel's defaults.
    fn default() -> Timeouts {
        Timeouts {
            tcp: [0, 120, 60, 432_000, 120, 60, 30, 120, 10, 120],
            tcp_retrans: 300,
            tcp_unack: 300,
            udp: 30,
            udp_stream: 120,
        }
    }
}

impl Timeouts {
    pub fn read() -> Timeouts {
        Timeouts::read_from(Path::new(SYSCTLS))
    }

    /// The timeouts in `dir`, with the kernel's default for a sysctl that
    /// cannot be read.
    fn read_from(dir: &Path) -> Timeouts {
        let mut t = Timeouts::default();
        let read = |name: &str, value: &mut u32| {
            let path = dir.join(format!("nf_conntrack_{name}"));
            if let Some(v) = std::fs::read_to_string(path)
                .ok()
                .and_then(|text| text.trim().parse().ok())
            {
                *value = v;
            }
        };
        let states = [
            "syn_sent",
            "syn_recv",
            "established",
            "fin_wait",
            "close_wait",
            "last_ack",
            "time_wait",
            "close",
        ];
        for (i, name) in states.into_iter().enumerate() {
            read(&format!("tcp_timeout_{name}"), &mut t.tcp[i + 1]);
        }
        read("tcp_timeout_max_retrans", &mut t.tcp_retrans);
        read("tcp_timeout_unacknowledged", &mut t.tcp_unack);
        read("udp_timeout", &mut t.udp);
        read("udp_timeout_stream", &mut t.udp_stream);
        t
    }

    /// The timeouts that the entry may run on: each packet sets one of them.
    fn runs(&self, conn: &Conn) -> Vec<u32> {
        let tcp = |state: u32| {
            let mut runs = vec![state];
            runs.extend(
                [self.tcp_retrans, self.tcp_unack]
                    .into_iter()
                    .filter(|&t| t < state),
            );
            runs
        };
        match conn.entry.original.protocol {
            Protocol::Udp if conn.status & IPS_ASSURED != 0 => vec![self.udp_stream],
            Protocol::Udp => vec![self.udp, self.udp_stream],
            Protocol::Tcp => match conn.tcp_state.map(usize::from) {
                Some(state @ 1..=9) => tcp(self.tcp[state]),
                _ => {
                    let mut runs = self.tcp[1..].to_vec();
                    runs.extend([self.tcp_retrans, self.tcp_unack]);
                    runs
                }
            },
        }
    }

    /// Seconds since the entry's last packet, which reset its timeout: the
    /// shortest timeout it may run on that is at least `CTA_TIMEOUT`, less
    /// `CTA_TIMEOUT`, and less the second that `CTA_TIMEOUT` rounds off. Each
    /// choice errs toward a younger age, so toward awake. 0 when no timeout
    /// fits, as after a sysctl was lowered.
    pub fn age(&self, conn: &Conn) -> u32 {
        let left = conn.entry.timeout_secs;
        self.runs(conn)
            .into_iter()
            .filter(|&t| t >= left)
            .min()
            .map_or(0, |t| (t - left).saturating_sub(1))
    }
}

/// The node's IPv4 addresses. Kubelet probes pods from one of them: cni0's,
/// such as 10.42.0.1.
pub fn local_addrs() -> Result<HashSet<Ipv4Addr>, String> {
    let addrs = nix::ifaddrs::getifaddrs().map_err(|e| format!("getifaddrs: {e}"))?;
    Ok(addrs
        .filter_map(|a| a.address?.as_sockaddr_in().map(|a| Ipv4Addr::from(a.ip())))
        .collect())
}

/// Whether `entry` is a kubelet probe of the pod at `pod`, or a pre-sleep
/// check of the node service, which runs from the same place: TCP from one
/// of the node's addresses (`local`) to a probe port of the pod, with no
/// NAT. NodePort traffic also reaches a pod from cni0's address, but NATed,
/// so it is not a probe.
pub fn is_probe(
    entry: &ConnEntry,
    pod: Ipv4Addr,
    local: &HashSet<Ipv4Addr>,
    is_probe_port: impl Fn(u16) -> bool,
) -> bool {
    let original = &entry.original;
    original.protocol == Protocol::Tcp
        && *original.dst.ip() == pod
        && local.contains(original.src.ip())
        && is_probe_port(original.dst.port())
        && !entry.is_nat()
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    // <linux/netfilter/nf_conntrack_common.h>
    pub const IPS_SEEN_REPLY: u32 = 1 << 1;
    // <linux/netlink.h>
    const NLM_F_MULTI: u16 = 0x2;

    pub fn tcp(src: [u8; 4], sport: u16, dst: [u8; 4], dport: u16) -> Tuple {
        Tuple {
            protocol: Protocol::Tcp,
            src: SocketAddrV4::new(src.into(), sport),
            dst: SocketAddrV4::new(dst.into(), dport),
        }
    }

    pub fn udp(src: [u8; 4], sport: u16, dst: [u8; 4], dport: u16) -> Tuple {
        Tuple {
            protocol: Protocol::Udp,
            ..tcp(src, sport, dst, dport)
        }
    }

    /// An established TCP entry, or a UDP one, with no counters.
    pub fn ct(original: Tuple, reply: Tuple, timeout_secs: u32) -> Conn {
        let tcp = original.protocol == Protocol::Tcp;
        Conn {
            entry: ConnEntry {
                original,
                reply,
                timeout_secs,
            },
            id: 7,
            status: IPS_SEEN_REPLY | IPS_ASSURED,
            tcp_state: tcp.then_some(3),
            counters: None,
            start_ns: None,
        }
    }

    pub fn counters(packets: u64, bytes: u64, reply_packets: u64, reply_bytes: u64) -> Counters {
        Counters {
            original: Count { packets, bytes },
            reply: Count {
                packets: reply_packets,
                bytes: reply_bytes,
            },
        }
    }

    /// A `CT_NEW` message of a dump for `conn`, as the kernel writes it,
    /// with the attributes in the kernel's order, and padding and attributes
    /// that the parser skips.
    fn entry_message(conn: &Conn, seq: u32) -> Vec<u8> {
        let mut m = Message::new(CT_NEW, NLM_F_MULTI, &NFGENMSG);
        m.nest(NLA_F_NESTED | CTA_TUPLE_ORIG, |m| {
            tuple_attrs(m, &conn.entry.original)
        });
        m.nest(NLA_F_NESTED | CTA_TUPLE_REPLY, |m| {
            tuple_attrs(m, &conn.entry.reply)
        });
        m.attr(CTA_STATUS, &conn.status.to_be_bytes());
        if let Some(c) = conn.counters {
            for (kind, count) in [
                (CTA_COUNTERS_ORIG, c.original),
                (CTA_COUNTERS_REPLY, c.reply),
            ] {
                m.nest(NLA_F_NESTED | kind, |m| {
                    m.attr(5, &[]); // CTA_COUNTERS_PAD
                    m.attr(CTA_COUNTERS_PACKETS, &count.packets.to_be_bytes());
                    m.attr(5, &[]);
                    m.attr(CTA_COUNTERS_BYTES, &count.bytes.to_be_bytes());
                });
            }
        }
        if let Some(start) = conn.start_ns {
            m.nest(NLA_F_NESTED | CTA_TIMESTAMP, |m| {
                m.attr(CTA_TIMESTAMP_START, &start.to_be_bytes())
            });
        }
        if conn.entry.timeout_secs != u32::MAX {
            m.attr(CTA_TIMEOUT, &conn.entry.timeout_secs.to_be_bytes());
        }
        if let Some(state) = conn.tcp_state {
            m.nest(NLA_F_NESTED | CTA_PROTOINFO, |m| {
                m.nest(NLA_F_NESTED | CTA_PROTOINFO_TCP, |m| {
                    m.attr(CTA_PROTOINFO_TCP_STATE, &[state]);
                    m.attr(2, &[7]); // CTA_PROTOINFO_TCP_WSCALE_ORIGINAL
                });
            });
        }
        m.attr(CTA_ID, &conn.id.to_be_bytes());
        m.attr(11, &1u32.to_be_bytes()); // CTA_USE
        m.finish(seq)
    }

    /// A message of type `kind` that ends a reply with `code`.
    fn end_message(kind: u16, code: i32, seq: u32) -> Vec<u8> {
        Message::new(kind, NLM_F_MULTI, &code.to_ne_bytes()).finish(seq)
    }

    fn hex(text: &str) -> Vec<u8> {
        let digits: Vec<u8> = text
            .chars()
            .filter_map(|c| c.to_digit(16))
            .map(|d| d as u8)
            .collect();
        digits.chunks(2).map(|p| p[0] << 4 | p[1]).collect()
    }

    #[test]
    fn requests_have_the_kernels_layout() {
        assert_eq!(
            dump_request(9),
            hex("14000000 0101 0103 09000000 00000000 02000000"),
            "nlmsghdr: 20 bytes, CT_GET, REQUEST and DUMP, seq 9, pid 0; nfgenmsg: AF_INET"
        );
        let get = get_request(10, &tcp([10, 42, 0, 2], 8080, [10, 42, 0, 1], 40001));
        let want = hex("48000000 0101 0500 0a000000 00000000 02000000
             3400 0280
               1400 0180 0800 0100 0a2a0002 0800 0200 0a2a0001
               1c00 0280 0500 0100 06000000 0600 0200 1f900000 0600 0300 9c410000");
        assert_eq!(get, want);
    }

    #[test]
    fn a_dump_reply_is_parsed_across_recvs() {
        let probe = tcp([10, 42, 0, 1], 40000, [10, 42, 0, 2], 8080);
        let kubelet = Conn {
            counters: Some(counters(6, 412, 4, 337)),
            start_ns: Some(1_760_000_000_000_000_000),
            ..ct(probe, probe.inverse(), 117)
        };
        let nodeport = Conn {
            id: 8,
            tcp_state: Some(7),
            ..ct(
                tcp([100, 96, 0, 1], 50000, [100, 96, 0, 2], 31080),
                tcp([10, 42, 0, 2], 8080, [10, 42, 0, 1], 40001),
                u32::MAX,
            )
        };
        let dns = Conn {
            id: 9,
            status: IPS_SEEN_REPLY,
            ..ct(
                udp([10, 42, 0, 2], 41000, [10, 43, 0, 10], 53),
                udp([10, 42, 0, 3], 53, [10, 42, 0, 2], 41000),
                25,
            )
        };
        let mut icmp = entry_message(&dns, 5);
        // The original tuple's CTA_PROTO_NUM: UDP (17) becomes ICMP (1).
        let at = icmp.windows(5).position(|w| w == [5, 0, 1, 0, 17]).unwrap();
        icmp[at + 4] = 1;

        let first = [
            entry_message(&kubelet, 5),
            entry_message(&dns, 4),
            icmp,
            entry_message(&nodeport, 5),
        ]
        .concat();
        let second = [entry_message(&dns, 5), end_message(NLMSG_DONE, 0, 5)].concat();

        let mut out = Vec::new();
        assert_eq!(parse(&first, 5, &mut out), Ok(None));
        assert_eq!(parse(&second, 5, &mut out), Ok(Some(End::Done)));
        assert_eq!(out, vec![kubelet, nodeport, dns], "no stale or ICMP entry");
    }

    #[test]
    fn errors_and_acks_end_a_reply() {
        let mut out = Vec::new();
        let buf = end_message(NLMSG_ERROR, -libc::ENOENT, 3);
        assert_eq!(parse(&buf, 3, &mut out), Ok(Some(End::Error(libc::ENOENT))));
        let buf = end_message(NLMSG_ERROR, 0, 3);
        assert_eq!(parse(&buf, 3, &mut out), Ok(Some(End::Done)));
        let mut buf = end_message(NLMSG_DONE, -libc::EINTR, 3);
        assert_eq!(parse(&buf, 3, &mut out), Ok(Some(End::Error(libc::EINTR))));
        buf[0] = 200;
        assert!(parse(&buf, 3, &mut out).is_err(), "longer than the recv");
        assert!(out.is_empty());
    }

    #[test]
    fn ages_err_toward_awake() {
        let t = Timeouts {
            tcp: [0, 120, 60, 86_400, 120, 3_600, 30, 120, 10, 120],
            ..Timeouts::default()
        };
        let pod = tcp([10, 42, 0, 9], 41000, [10, 42, 0, 2], 80);
        let with = |state: Option<u8>, left: u32| Conn {
            tcp_state: state,
            ..ct(pod, pod.inverse(), left)
        };
        assert_eq!(t.age(&with(Some(3), 86_300)), 99, "established");
        assert_eq!(t.age(&with(Some(3), 200)), 99, "unacknowledged data");
        assert_eq!(t.age(&with(Some(7), 90)), 29, "TIME_WAIT");
        assert_eq!(t.age(&with(Some(7), 200)), 0, "no timeout fits");
        assert_eq!(t.age(&with(Some(3), 86_400)), 0, "a packet this second");
        assert_eq!(
            t.age(&with(None, 50)),
            9,
            "no state: the shortest that fits"
        );
        assert_eq!(t.age(&with(Some(3), u32::MAX)), 0, "no CTA_TIMEOUT");

        let pod = udp([10, 42, 0, 9], 41000, [10, 42, 0, 2], 53);
        let udp = |status: u32, left: u32| Conn {
            status,
            ..ct(pod, pod.inverse(), left)
        };
        assert_eq!(t.age(&udp(IPS_SEEN_REPLY, 20)), 9, "a DNS answer");
        assert_eq!(
            t.age(&udp(IPS_SEEN_REPLY | IPS_ASSURED, 20)),
            99,
            "a stream"
        );
        assert_eq!(t.age(&udp(0, 100)), 19, "only a stream's timeout fits");
    }

    #[test]
    fn timeouts_come_from_the_sysctls() {
        let dir = tempfile::tempdir().unwrap();
        for (name, value) in [
            ("tcp_timeout_established", "86400\n"),
            ("tcp_timeout_close_wait", "3600\n"),
            ("udp_timeout_stream", "180\n"),
            ("tcp_timeout_unacknowledged", "garbage\n"),
        ] {
            std::fs::write(dir.path().join(format!("nf_conntrack_{name}")), value).unwrap();
        }
        assert_eq!(
            Timeouts::read_from(dir.path()),
            Timeouts {
                tcp: [0, 120, 60, 86_400, 120, 3_600, 30, 120, 10, 120],
                udp_stream: 180,
                ..Timeouts::default()
            }
        );
    }

    #[test]
    fn probes_come_from_the_node_without_nat() {
        let pod = Ipv4Addr::new(10, 42, 0, 2);
        let local: HashSet<Ipv4Addr> = [Ipv4Addr::new(10, 42, 0, 1), Ipv4Addr::LOCALHOST].into();
        let port = |p: u16| p == 8080;
        let entry = |original: Tuple, reply: Tuple| ConnEntry {
            original,
            reply,
            timeout_secs: 100,
        };
        let probe = tcp([10, 42, 0, 1], 40000, [10, 42, 0, 2], 8080);
        assert!(is_probe(&entry(probe, probe.inverse()), pod, &local, port));
        let nodeport = entry(
            tcp([100, 96, 0, 1], 50000, [100, 96, 0, 2], 31080),
            tcp([10, 42, 0, 2], 8080, [10, 42, 0, 1], 40001),
        );
        assert!(!is_probe(&nodeport, pod, &local, port), "NodePort is NATed");
        let other_port = tcp([10, 42, 0, 1], 40000, [10, 42, 0, 2], 80);
        let from_pod = tcp([10, 42, 0, 7], 40000, [10, 42, 0, 2], 8080);
        let over_udp = udp([10, 42, 0, 1], 40000, [10, 42, 0, 2], 8080);
        for t in [other_port, from_pod, over_udp] {
            assert!(
                !is_probe(&entry(t, t.inverse()), pod, &local, port),
                "{t:?}"
            );
        }
        let other_pod = Ipv4Addr::new(10, 42, 0, 3);
        assert!(!is_probe(
            &entry(probe, probe.inverse()),
            other_pod,
            &local,
            port
        ));
    }

    /// The node kernel's (6.18.55) replies to a `get_request` for a loopback
    /// connection, which `live_dump_and_get` recorded: the entry and the
    /// ACK, with `nf_conntrack_acct` off and then on.
    const CAPTURED_GET: &str = "
        d4000000000102000600000075f178bb 02000000340001801400018008000100
        7f000001080002007f0000011c000280 050001000600000006000200b68e0000
        06000300b1df00003400028014000180 080001007f000001080002007f000001
        1c000280050001000600000006000200 b1df000006000300b68e000008000300
        0000018e080008000000000008000c00 ef7087ee08000b000000000208000700
        0001517f300004802c00018005000100 03000000050002000a00000005000300
        0a000000060004002300000006000500 23000000240000000200000106000000
        75f178bb000000004800000001010500 0600000000000000";
    const CAPTURED_GET_ACCT: &str = "
        0c010000000102000600000058f431df 02000000340001801400018008000100
        7f000001080002007f0000011c000280 0500010006000000060002008ec80000
        060003009f1100003400028014000180 080001007f000001080002007f000001
        1c000280050001000600000006000200 9f110000060003008ec8000008000300
        0000018e080008000000000008000c00 2e1980f508000b000000000208000700
        0001517f300004802c00018005000100 03000000050002000a00000005000300
        0a000000060004002300000006000500 230000001c0009800c00010000000000
        000000030c00020000000000000000a8 1c000a800c0001000000000000000002
        0c000200000000000000007024000000 020000010600000058f431df00000000
        48000000010105000600000000000000";

    #[test]
    fn captured_get_replies_are_parsed() {
        let localhost = [127, 0, 0, 1];
        let cases = [
            (CAPTURED_GET, 46734, 45535, 0xef70_87ee, None),
            (
                CAPTURED_GET_ACCT,
                36552,
                40721,
                0x2e19_80f5,
                Some(counters(3, 168, 2, 112)),
            ),
        ];
        for (capture, client, server, id, counters) in cases {
            let raw = hex(capture);
            let seq = u32::from_ne_bytes(raw[8..12].try_into().unwrap());
            let mut out = Vec::new();
            assert_eq!(parse(&raw, seq, &mut out), Ok(Some(End::Done)));
            let original = tcp(localhost, client, localhost, server);
            let want = Conn {
                entry: ConnEntry {
                    original,
                    reply: original.inverse(),
                    timeout_secs: 86_399,
                },
                id,
                // SEEN_REPLY, ASSURED, CONFIRMED, SRC_NAT_DONE and
                // DST_NAT_DONE.
                status: 0x18e,
                tcp_state: Some(3),
                counters,
                start_ns: None,
            };
            assert_eq!(out, vec![want]);
            assert_eq!(timeouts_of_kube_proxy().age(&want), 0);
        }
    }

    fn timeouts_of_kube_proxy() -> Timeouts {
        Timeouts {
            tcp: [0, 120, 60, 86_400, 120, 3_600, 30, 120, 10, 120],
            ..Timeouts::default()
        }
    }

    /// Whether `nf_conntrack_acct` is on, so that new entries count packets
    /// and bytes. Entries made while it was off never do.
    fn accounting() -> bool {
        std::fs::read_to_string(Path::new(SYSCTLS).join("nf_conntrack_acct"))
            .is_ok_and(|v| v.trim() == "1")
    }

    pub fn env_number(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    /// Against the kernel, as root in a namespace where conntrack runs, such
    /// as the node's (see README.md, "Tests"): opens `CONNTRACK_LIVE_TCP`
    /// loopback connections (200), sends to `CONNTRACK_LIVE_UDP` closed UDP
    /// ports (0) for more entries, times five dumps, and gets one connection
    /// by its reply tuple. With `CONNTRACK_CAPTURE=FILE` it writes the raw
    /// reply of the get to FILE in hex.
    #[test]
    #[ignore = "needs root in a namespace where conntrack runs, such as the node"]
    fn live_dump_and_get() {
        use std::io::{Read, Write};
        use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
        use std::time::Instant;

        let v4 = |a: SocketAddr| match a {
            SocketAddr::V4(a) => a,
            SocketAddr::V6(a) => panic!("{a} is not IPv4"),
        };
        let tcp_conns = env_number("CONNTRACK_LIVE_TCP", 200);
        let udp_flows = env_number("CONNTRACK_LIVE_UDP", 0);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let server = v4(listener.local_addr().unwrap());
        let mut streams = Vec::new();
        for _ in 0..tcp_conns {
            let client = TcpStream::connect(server).unwrap();
            streams.push((client, listener.accept().unwrap().0));
        }
        let (client, accepted) = &mut streams[0];
        client.write_all(b"ping").unwrap();
        accepted.read_exact(&mut [0; 4]).unwrap();
        let udp = UdpSocket::bind("127.0.0.1:0").unwrap();
        for port in (20_000..).take(udp_flows) {
            let _ = udp.send_to(b"x", (Ipv4Addr::LOCALHOST, port));
        }

        let ctnetlink = Ctnetlink::default();
        let mut took = Vec::new();
        let mut table = Vec::new();
        for _ in 0..5 {
            let started = Instant::now();
            table = ctnetlink.dump().unwrap();
            took.push(started.elapsed());
        }
        // Not entries of an earlier run that used the same server port.
        let clients: HashSet<SocketAddrV4> = streams
            .iter()
            .map(|(client, _)| v4(client.local_addr().unwrap()))
            .collect();
        let ours = table
            .iter()
            .filter(|c| c.entry.original.dst == server && clients.contains(&c.entry.original.src))
            .count();
        eprintln!(
            "{} entries, {ours} of the {tcp_conns} test connections, accounting {}; \
             dumps took {took:?}",
            table.len(),
            accounting()
        );
        assert_eq!(ours, tcp_conns, "does conntrack run in this namespace?");

        let original = Tuple {
            protocol: Protocol::Tcp,
            src: v4(streams[0].0.local_addr().unwrap()),
            dst: server,
        };
        let mut raw = Vec::new();
        let started = Instant::now();
        let got = ctnetlink
            .get_raw(&original.inverse(), |b| raw.extend_from_slice(b))
            .unwrap()
            .expect("the connection by its reply tuple");
        eprintln!("get took {:?}: {got:?}", started.elapsed());
        assert_eq!(got.entry.original, original);
        assert!(!got.entry.is_nat());
        assert_eq!(got.tcp_state, Some(3));
        let age = Timeouts::read().age(&got);
        assert!(age <= 1, "age {age} s");
        if accounting() {
            let c = got.counters.expect("counters with nf_conntrack_acct on");
            assert!(c.original.packets >= 3 && c.reply.packets >= 2, "{c:?}");
        }
        let unknown = Tuple {
            src: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1),
            ..original
        };
        assert_eq!(ctnetlink.get(&unknown.inverse()), Ok(None));
        assert!(local_addrs().unwrap().contains(&Ipv4Addr::LOCALHOST));
        if let Ok(path) = std::env::var("CONNTRACK_CAPTURE") {
            let lines: Vec<String> = raw
                .chunks(16)
                .map(|line| line.iter().map(|b| format!("{b:02x}")).collect())
                .collect();
            std::fs::write(path, lines.join("\n") + "\n").unwrap();
        }
    }
}
