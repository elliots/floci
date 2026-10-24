//! The hold: `eth0`'s bridge port state. While it is `listening`, `podbr0`
//! forwards nothing between eth0 and tap0, also after a new tap0 joins, but
//! the catcher's packet socket still sees every frame. `forwarding` releases
//! it. With STP off, `disabled` and `blocking` would flip back to
//! `forwarding` at once, and a carrier flap of eth0 resets any state to
//! `forwarding`, so the netloop checks the state again after link events.
//!
//! This speaks route netlink as `ip link set dev eth0 type bridge_slave
//! state N` does: an `RTM_NEWLINK` with `IFLA_LINKINFO {
//! IFLA_INFO_SLAVE_DATA { IFLA_BRPORT_STATE } }`. A netlink socket belongs to
//! the network namespace it is opened in.

use std::io;
use std::mem::{size_of, zeroed};
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::time::{Duration, Instant};

use crate::files;
use crate::netlink::{self, Message, NLMSG_ERROR};

pub(super) const LISTENING: u8 = 1;
pub(super) const FORWARDING: u8 = 3;

const IFLA_BRPORT_STATE: u16 = 1;
const RTEXT_FILTER_SKIP_STATS: u32 = 1 << 3;
const IFINFOMSG: usize = 16;
const ANSWER_TIMEOUT: Duration = Duration::from_secs(2);
/// Room for a link message. A longer datagram is cut short, and the message
/// that does not fit is lost.
const RECV_BUFFER: usize = 32 * 1024;

/// What the catcher needs to know about a link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Link {
    pub index: i32,
    /// Its bridge port state, if it is a bridge port.
    pub port_state: Option<u8>,
}

pub(super) fn state_name(state: u8) -> &'static str {
    match state {
        0 => "disabled",
        1 => "listening",
        2 => "learning",
        3 => "forwarding",
        4 => "blocking",
        _ => "unknown",
    }
}

/// A route netlink socket in a pod's network namespace.
#[derive(Debug)]
pub(super) struct Netlink {
    fd: OwnedFd,
    seq: u32,
    /// Links named by the link events read so far.
    changed: Vec<i32>,
    /// Link events were lost.
    overrun: bool,
}

/// The link events a [`Netlink`] read since the last call.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Changes {
    pub links: Vec<i32>,
    pub overrun: bool,
}

impl Changes {
    /// Whether the link `index` may have changed.
    pub fn touch(&self, index: i32) -> bool {
        self.overrun || self.links.contains(&index)
    }
}

impl Netlink {
    /// Opens the socket in the calling thread's network namespace, joined to
    /// its link events when `events` is set.
    pub fn open(events: bool) -> io::Result<Netlink> {
        let fd = netlink::socket(libc::NETLINK_ROUTE, libc::SOCK_NONBLOCK)?;
        // SAFETY: a zeroed sockaddr_nl is valid, then filled in.
        let mut addr: libc::sockaddr_nl = unsafe { zeroed() };
        addr.nl_family = libc::AF_NETLINK as u16;
        if events {
            addr.nl_groups = libc::RTMGRP_LINK as u32;
        }
        // SAFETY: binds to a valid sockaddr_nl of the size passed.
        let rc = unsafe {
            libc::bind(
                fd.as_raw_fd(),
                (&addr as *const libc::sockaddr_nl).cast(),
                size_of::<libc::sockaddr_nl>() as libc::socklen_t,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Netlink {
            fd,
            seq: 0,
            changed: Vec::new(),
            overrun: false,
        })
    }

    pub fn fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    /// The link named `name`, or none.
    pub fn link_named(&mut self, name: &str) -> io::Result<Option<Link>> {
        let mut msg = link_message(libc::RTM_GETLINK, 0, 0);
        let mut ifname = name.as_bytes().to_vec();
        ifname.push(0);
        msg.attr(libc::IFLA_IFNAME, &ifname);
        msg.attr(libc::IFLA_EXT_MASK, &RTEXT_FILTER_SKIP_STATS.to_ne_bytes());
        self.get(msg)
    }

    /// The link with index `index`, or none.
    pub fn link(&mut self, index: i32) -> io::Result<Option<Link>> {
        let mut msg = link_message(libc::RTM_GETLINK, 0, index);
        msg.attr(libc::IFLA_EXT_MASK, &RTEXT_FILTER_SKIP_STATS.to_ne_bytes());
        self.get(msg)
    }

    fn get(&mut self, msg: Message) -> io::Result<Option<Link>> {
        match self.request(msg, Answer::Link) {
            Ok(payload) => Ok(payload.as_deref().and_then(parse_link)),
            Err(e) if e.raw_os_error() == Some(libc::ENODEV) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Sets the bridge port state of the link with index `index`.
    pub fn set_port_state(&mut self, index: i32, state: u8) -> io::Result<()> {
        let mut msg = link_message(libc::RTM_NEWLINK, netlink::NLM_F_ACK, index);
        msg.nest(libc::IFLA_LINKINFO, |m| {
            m.nest(libc::IFLA_INFO_SLAVE_DATA, |m| {
                m.attr(IFLA_BRPORT_STATE, &[state])
            });
        });
        self.request(msg, Answer::Ack).map(|_| ())
    }

    /// The link events since the last call, reading what is waiting.
    pub fn changes(&mut self) -> Changes {
        let mut buf = vec![0u8; RECV_BUFFER];
        loop {
            match self.recv(&mut buf) {
                Ok(Some(n)) => {
                    self.read_messages(&buf[..n], None);
                }
                Ok(None) => break,
                Err(e) => {
                    log::warn!("sleepnet: read link events: {e}");
                    break;
                }
            }
        }
        Changes {
            links: std::mem::take(&mut self.changed),
            overrun: std::mem::take(&mut self.overrun),
        }
    }

    /// One datagram into `buf`, or none for now. Lost events count as an
    /// overrun.
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<Option<usize>> {
        loop {
            // SAFETY: reads into `buf`, of the length passed.
            let n = unsafe {
                libc::recv(
                    self.fd.as_raw_fd(),
                    buf.as_mut_ptr().cast(),
                    buf.len(),
                    libc::MSG_DONTWAIT,
                )
            };
            if n >= 0 {
                return Ok(Some((n as usize).min(buf.len())));
            }
            let e = io::Error::last_os_error();
            match e.raw_os_error() {
                Some(libc::EAGAIN) => return Ok(None),
                Some(libc::EINTR) => {}
                Some(libc::ENOBUFS) => self.overrun = true,
                _ => return Err(e),
            }
        }
    }

    /// Sends `msg` and reads until its answer, keeping the link events read
    /// meanwhile. The kernel answers before `send` returns.
    fn request(&mut self, msg: Message, answer: Answer) -> io::Result<Option<Vec<u8>>> {
        self.seq = self.seq.wrapping_add(1).max(1);
        let seq = self.seq;
        let bytes = msg.finish(seq);
        // SAFETY: sends `bytes`, of the length passed, to the kernel.
        let n = unsafe { libc::send(self.fd.as_raw_fd(), bytes.as_ptr().cast(), bytes.len(), 0) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        let deadline = Instant::now() + ANSWER_TIMEOUT;
        let mut buf = vec![0u8; RECV_BUFFER];
        loop {
            let Some(n) = self.recv(&mut buf)? else {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "netlink did not answer",
                    ));
                }
                files::ready(self.fd.as_raw_fd(), libc::POLLIN, left)?;
                continue;
            };
            if let Some(result) = self.read_messages(&buf[..n], Some((seq, answer))) {
                return result;
            }
        }
    }

    /// Reads a datagram: the answer to the request `seq`, if it is there,
    /// and link events.
    fn read_messages(
        &mut self,
        datagram: &[u8],
        request: Option<(u32, Answer)>,
    ) -> Option<io::Result<Option<Vec<u8>>>> {
        let mut answer = None;
        for (kind, seq, payload) in netlink::messages(datagram).map_while(Result::ok) {
            let ours = request.filter(|(s, _)| *s == seq).map(|(_, a)| a);
            match (kind, ours) {
                (NLMSG_ERROR, Some(_)) => {
                    let errno = netlink::error_code(payload).unwrap_or(-libc::EPROTO);
                    answer = Some(match errno {
                        0 => Ok(None),
                        e => Err(io::Error::from_raw_os_error(-e)),
                    });
                }
                (libc::RTM_NEWLINK, Some(Answer::Link)) => {
                    answer = Some(Ok(Some(payload.to_vec())));
                }
                (libc::RTM_NEWLINK | libc::RTM_DELLINK, _) => {
                    if let Some(index) = payload.get(4..8) {
                        let index = i32::from_ne_bytes([index[0], index[1], index[2], index[3]]);
                        self.changed.push(index);
                    }
                }
                _ => {}
            }
        }
        answer
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Answer {
    /// An `RTM_NEWLINK` with the link, or an error.
    Link,
    /// An acknowledgement or an error. An `RTM_NEWLINK` with the request's
    /// sequence number is the notification of the change, not the answer.
    Ack,
}

/// A route netlink request about the link `index`.
fn link_message(kind: u16, flags: u16, index: i32) -> Message {
    let mut ifinfomsg = [0u8; IFINFOMSG];
    ifinfomsg[0] = libc::AF_UNSPEC as u8;
    ifinfomsg[4..8].copy_from_slice(&index.to_ne_bytes());
    Message::new(kind, netlink::NLM_F_REQUEST | flags, &ifinfomsg)
}

/// An `RTM_NEWLINK` payload: `ifinfomsg` and attributes.
fn parse_link(payload: &[u8]) -> Option<Link> {
    let index = i32::from_ne_bytes(payload.get(4..8)?.try_into().ok()?);
    let port_state = netlink::attrs(payload.get(IFINFOMSG..)?)
        .filter(|(kind, _)| *kind == libc::IFLA_LINKINFO)
        .flat_map(|(_, info)| netlink::attrs(info))
        .filter(|(kind, _)| *kind == libc::IFLA_INFO_SLAVE_DATA)
        .flat_map(|(_, data)| netlink::attrs(data))
        .find(|(kind, _)| *kind == IFLA_BRPORT_STATE)
        .and_then(|(_, state)| state.first().copied());
    Some(Link { index, port_state })
}

/// Sets the bridge port state of the link `index`, called `name` in errors,
/// to `state` unless it is already, and checks it. Returns whether it
/// changed. A link that is not a bridge port is an error.
pub(super) fn set_state(
    nl: &mut Netlink,
    index: i32,
    name: &str,
    state: u8,
) -> Result<bool, String> {
    let current = nl
        .link(index)
        .map_err(|e| format!("read {name}: {e}"))?
        .ok_or_else(|| format!("{name} is gone"))?
        .port_state
        .ok_or_else(|| format!("{name} is not a bridge port"))?;
    if current == state {
        return Ok(false);
    }
    nl.set_port_state(index, state).map_err(|e| {
        format!(
            "set {name}'s bridge port state from {} to {}: {e}",
            state_name(current),
            state_name(state)
        )
    })?;
    let now = nl
        .link(index)
        .map_err(|e| format!("read {name}: {e}"))?
        .and_then(|l| l.port_state);
    if now != Some(state) {
        return Err(format!(
            "{name}'s bridge port state is {} after it was set to {}",
            now.map_or("gone", state_name),
            state_name(state)
        ));
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::netlink::{NLA_F_NESTED, NLMSG_HDRLEN};

    /// An `RTM_NEWLINK` payload as the kernel sends it for a bridge port, in
    /// part: `ifinfomsg`, `IFLA_IFNAME`, and `IFLA_LINKINFO` with the slave
    /// kind and data, with the nested flag that the kernel sets.
    fn port_payload(index: i32, state: u8) -> Vec<u8> {
        let mut m = link_message(libc::RTM_NEWLINK, 0, index);
        m.attr(libc::IFLA_IFNAME, b"eth0\0");
        m.nest(NLA_F_NESTED | libc::IFLA_LINKINFO, |m| {
            m.attr(libc::IFLA_INFO_SLAVE_KIND, b"bridge\0");
            m.nest(NLA_F_NESTED | libc::IFLA_INFO_SLAVE_DATA, |m| {
                m.attr(2, &[0x20, 0]);
                m.attr(IFLA_BRPORT_STATE, &[state]);
            });
        });
        m.finish(0)[NLMSG_HDRLEN..].to_vec()
    }

    #[test]
    fn a_port_state_is_read_from_the_link_info() {
        assert_eq!(
            parse_link(&port_payload(2, LISTENING)),
            Some(Link {
                index: 2,
                port_state: Some(LISTENING)
            })
        );
        let mut plain = link_message(libc::RTM_NEWLINK, 0, 3);
        plain.attr(libc::IFLA_IFNAME, b"tap0\0");
        let plain = plain.finish(0)[NLMSG_HDRLEN..].to_vec();
        assert_eq!(
            parse_link(&plain),
            Some(Link {
                index: 3,
                port_state: None
            })
        );
        assert_eq!(parse_link(&[0; 6]), None);
    }

    #[test]
    fn a_state_request_nests_the_port_state_in_the_slave_data() {
        let mut msg = link_message(libc::RTM_NEWLINK, netlink::NLM_F_ACK, 7);
        msg.nest(libc::IFLA_LINKINFO, |m| {
            m.nest(libc::IFLA_INFO_SLAVE_DATA, |m| {
                m.attr(IFLA_BRPORT_STATE, &[FORWARDING])
            });
        });
        let bytes = msg.finish(42);
        let parsed: Vec<_> = netlink::messages(&bytes).collect();
        assert_eq!(parsed.len(), 1);
        let (kind, seq, payload) = parsed[0].clone().unwrap();
        assert_eq!((kind, seq), (libc::RTM_NEWLINK, 42));
        let flags = u16::from_ne_bytes([bytes[6], bytes[7]]);
        assert_eq!(flags, (libc::NLM_F_REQUEST | libc::NLM_F_ACK) as u16);
        assert_eq!(
            parse_link(payload),
            Some(Link {
                index: 7,
                port_state: Some(FORWARDING)
            })
        );
        assert_eq!(bytes.len() % 4, 0);
        let len = u32::from_ne_bytes(bytes[0..4].try_into().unwrap());
        assert_eq!(len as usize, bytes.len());
    }

    #[test]
    fn link_events_name_their_links_and_stop_at_a_bad_length() {
        let mut nl = Netlink {
            fd: std::fs::File::open("/dev/null").unwrap().into(),
            seq: 0,
            changed: Vec::new(),
            overrun: false,
        };
        let a = link_message(libc::RTM_NEWLINK, 0, 1).finish(5);
        let b = link_message(libc::RTM_DELLINK, 0, 2).finish(0);
        let mut both = a.clone();
        both.extend_from_slice(&b);
        assert!(nl.read_messages(&both, None).is_none());
        assert_eq!(nl.changed, [1, 2]);
        nl.changed.clear();
        let mut bad = a;
        bad[0..4].copy_from_slice(&1000u32.to_ne_bytes());
        bad.extend_from_slice(&b);
        assert!(nl.read_messages(&bad, None).is_none());
        assert!(nl.changed.is_empty());
    }

    #[test]
    fn changes_touch_a_link_or_every_link_after_an_overrun() {
        let changes = Changes {
            links: vec![3],
            overrun: false,
        };
        assert!(changes.touch(3) && !changes.touch(2));
        let lost = Changes {
            links: vec![],
            overrun: true,
        };
        assert!(lost.touch(2));
    }
}
