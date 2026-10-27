//! Whether an awake pod is idle.
//!
//! `conntrack.rs` reads the node's conntrack table over ctnetlink and tells
//! kubelet probes apart; `idle.rs` keeps the last activity of each pod,
//! probes left out, and its idle timer; and `cpu.rs` reads VM CPU from
//! `/proc`. The probe responder tells kubelet SYNs from NodePort traffic
//! with [`ConntrackLookup::by_reply`], which also finds the source of a
//! wake; the traffic waker tells a sleeping pod's own connections from those
//! opened to it with [`ConntrackLookup::by_original`]; and the node service
//! checks a pod again before it freezes with [`Candidate::recheck`], a fresh
//! [`ConntrackLookup::last_activity`].

mod conntrack;
mod cpu;
mod idle;

pub use conntrack::{local_addrs, Ctnetlink, Direction};
pub use idle::{Candidate, Flow, Monitor, Verdict};

use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Protocol {
    Tcp,
    Udp,
}

/// One direction of a connection, as conntrack keys it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Tuple {
    pub protocol: Protocol,
    pub src: SocketAddrV4,
    pub dst: SocketAddrV4,
}

impl Tuple {
    /// The other direction, with no NAT.
    pub fn inverse(self) -> Tuple {
        Tuple {
            protocol: self.protocol,
            src: self.dst,
            dst: self.src,
        }
    }
}

/// A conntrack entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnEntry {
    pub original: Tuple,
    pub reply: Tuple,
    /// `CTA_TIMEOUT`: the seconds left before the entry expires. Every
    /// packet resets it to its state's timeout.
    pub timeout_secs: u32,
}

impl ConnEntry {
    /// Whether the connection is NATed: its original tuple is not the inverse
    /// of its reply tuple, as for NodePort traffic.
    pub fn is_nat(&self) -> bool {
        self.original != self.reply.inverse()
    }
}

/// The node's conntrack table, over ctnetlink. A lookup finds the entry whose
/// original or reply tuple it names, whichever the lookup names it as: the
/// kernel matches both.
pub trait ConntrackLookup: Send + Sync {
    /// The entry with the tuple `reply`, by one ctnetlink `GET` by
    /// `CTA_TUPLE_REPLY`. For a SYN from `S` to the pod at `P`, `reply` is
    /// `P` to `S`; the SYN is a kubelet probe only if the entry is not NATed.
    fn by_reply(&self, reply: &Tuple) -> Result<Option<ConnEntry>, String>;

    /// The entry with the tuple `original`, by one ctnetlink `GET` by
    /// `CTA_TUPLE_ORIG`. For a frame from `S` to the pod at `P`, after NAT,
    /// `original` is `P` to `S`; the frame is on a connection that the pod
    /// opened only if it is the entry's original tuple, with no NAT, after
    /// SNAT and after DNAT alike.
    fn by_original(&self, original: &Tuple) -> Result<Option<ConnEntry>, String>;

    /// When the pod at `ip` last saw a TCP or UDP packet, either way, on a
    /// connection opened to it, other than a kubelet probe or the node
    /// service's own probe checks, by a fresh dump; none if conntrack has no
    /// such entry. Conntrack counts in seconds, so the time is rounded toward
    /// now, which errs toward awake.
    fn last_activity(&self, ip: Ipv4Addr) -> Result<Option<SystemTime>, String>;
}

/// Conntrack for tests: entries and activity set by the test. A lookup finds
/// an entry by its original or its reply tuple, as the kernel's does.
#[cfg(test)]
#[derive(Default)]
pub struct MockConntrack {
    pub entries: std::sync::Mutex<Vec<ConnEntry>>,
    pub activity: std::sync::Mutex<std::collections::HashMap<Ipv4Addr, SystemTime>>,
}

#[cfg(test)]
impl MockConntrack {
    fn find(&self, tuple: &Tuple) -> Option<ConnEntry> {
        crate::sync::lock(&self.entries)
            .iter()
            .find(|e| e.original == *tuple || e.reply == *tuple)
            .copied()
    }
}

#[cfg(test)]
impl ConntrackLookup for MockConntrack {
    fn by_reply(&self, reply: &Tuple) -> Result<Option<ConnEntry>, String> {
        Ok(self.find(reply))
    }

    fn by_original(&self, original: &Tuple) -> Result<Option<ConnEntry>, String> {
        Ok(self.find(original))
    }

    fn last_activity(&self, ip: Ipv4Addr) -> Result<Option<SystemTime>, String> {
        Ok(crate::sync::lock(&self.activity).get(&ip).copied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tcp(src: [u8; 4], sport: u16, dst: [u8; 4], dport: u16) -> Tuple {
        Tuple {
            protocol: Protocol::Tcp,
            src: SocketAddrV4::new(src.into(), sport),
            dst: SocketAddrV4::new(dst.into(), dport),
        }
    }

    /// Two entries as a node shows them: a kubelet probe, and NodePort
    /// traffic from the Mac that reaches the pod from cni0's address too.
    #[test]
    fn only_a_nated_entry_is_nat() {
        let probe = tcp([10, 42, 0, 1], 40000, [10, 42, 0, 2], 8080);
        let kubelet = ConnEntry {
            original: probe,
            reply: probe.inverse(),
            timeout_secs: 119,
        };
        let nodeport = ConnEntry {
            original: tcp([100, 96, 0, 1], 50000, [100, 96, 0, 2], 31080),
            reply: tcp([10, 42, 0, 2], 8080, [10, 42, 0, 1], 40001),
            timeout_secs: 86399,
        };
        assert!(!kubelet.is_nat());
        assert!(nodeport.is_nat());
    }
}
