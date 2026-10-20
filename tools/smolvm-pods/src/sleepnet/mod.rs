//! The packet path of a pod that is pausing, asleep or waking.
//!
//! While a pod pauses, sleeps or wakes, its **catcher** holds a packet socket
//! on the pod's `eth0`, and `eth0`'s bridge port is `listening`, so that the
//! port forwards nothing to or from the guest but the socket still sees every
//! frame. The catcher answers ARP for the pod IP, hands kubelet probes to the
//! pod's responder ([`crate::probe::Responder`]), holds the TCP SYNs, data
//! and RSTs that the responder does not take, drops bare ACKs, and wakes the
//! pod on the first SYN or data segment. At the end of the wake it sets the
//! port to `forwarding` and writes the held frames into the new `tap0`. One
//! netloop thread serves every catcher.
//!
//! [`NetCatchers`] implements [`Catchers`], which the node service calls from
//! the lifecycle's hooks.

mod capture;
mod catcher;
mod frame;
mod hold;
mod netloop;

pub use frame::checksum;
pub use netloop::NetCatchers;

use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::SystemTime;

use crate::lifecycle::PodIdentity;
use crate::net::{parse_mac, CNI_IFACE};

/// A pod's network identity, which its catcher needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodNet {
    pub sandbox_id: String,
    /// The pod's network namespace, such as `/var/run/netns/cni-...`.
    pub netns: String,
    /// [`CNI_IFACE`] in that namespace.
    pub iface: String,
    pub ip: Ipv4Addr,
    /// The guest's MAC, which ARP replies carry. `eth0` has it with the
    /// locally administered bit flipped (see `net.rs`).
    pub mac: [u8; 6],
}

impl PodNet {
    /// The network identity in a pod's record, which needs its netns, its
    /// IPv4 address and the guest's MAC.
    pub fn of(identity: &PodIdentity) -> Result<PodNet, String> {
        let id = &identity.sandbox_id;
        let netns = identity
            .netns
            .clone()
            .ok_or_else(|| format!("pod sandbox {id} has no network namespace"))?;
        let ip = identity
            .ip
            .as_deref()
            .ok_or_else(|| format!("pod sandbox {id} has no recorded IP"))?;
        let ip = ip
            .parse()
            .map_err(|_| format!("pod sandbox {id} has no IPv4 address but {ip:?}"))?;
        let mac = identity
            .mac
            .as_deref()
            .ok_or_else(|| format!("pod sandbox {id} has no recorded MAC"))?;
        Ok(PodNet {
            sandbox_id: id.clone(),
            netns,
            iface: CNI_IFACE.to_string(),
            ip,
            mac: parse_mac(mac)?,
        })
    }
}

/// The phase of the pod that a catcher serves, which decides what it does
/// with UDP and with probes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// The pod pauses. TCP is held, UDP dropped.
    Pausing,
    /// The pod sleeps. After a failed restore, `probes_fail`: the responder
    /// fails every probe.
    Asleep { probes_fail: bool },
    /// The pod wakes. UDP is held too.
    Waking,
}

/// What a frame to a pod is, as the catcher tells them apart. The catcher's
/// filter passes only ARP requests for the pod IP and IPv4 frames to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameClass {
    /// An ARP request for the pod IP: the catcher answers with the guest's
    /// MAC, and holds a copy for the guest, which learns the sender's MAC
    /// from it at the release. No wake.
    ArpRequest,
    /// A TCP SYN. To the responder if it is a kubelet probe, otherwise held,
    /// and it wakes the pod.
    TcpSyn,
    /// A TCP segment with payload or FIN. To the responder if it belongs to
    /// one of its connections, otherwise held, and it wakes the pod.
    TcpData,
    /// A TCP RST: held in a small side queue for the next wake. No wake.
    TcpRst,
    /// A TCP segment with no payload and no SYN, FIN or RST: an ACK, a
    /// keepalive probe or a window update. To the responder if it belongs to
    /// one of its connections, otherwise dropped. No wake.
    TcpBare,
    /// Dropped while the pod pauses or sleeps, held while it wakes. No wake.
    Udp,
    /// The responder answers. No wake.
    IcmpEcho,
    /// Anything else: dropped.
    Other,
}

/// The `struct virtio_net_hdr` that comes before each frame on a packet
/// socket with `PACKET_VNET_HDR`, in the host's byte order, which is little
/// endian on every node we run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VnetHeader {
    pub flags: u8,
    pub gso_type: u8,
    pub hdr_len: u16,
    pub gso_size: u16,
    pub csum_start: u16,
    pub csum_offset: u16,
}

impl VnetHeader {
    pub const LEN: usize = 10;
    /// `VIRTIO_NET_HDR_F_NEEDS_CSUM`: the checksum is partial, from
    /// `csum_start`, to be written at `csum_start + csum_offset`.
    pub const F_NEEDS_CSUM: u8 = 1;
    /// `VIRTIO_NET_HDR_GSO_NONE`.
    pub const GSO_NONE: u8 = 0;

    pub fn parse(bytes: &[u8]) -> Option<VnetHeader> {
        let b = bytes.get(..Self::LEN)?;
        let u16_at = |i: usize| u16::from_le_bytes([b[i], b[i + 1]]);
        Some(VnetHeader {
            flags: b[0],
            gso_type: b[1],
            hdr_len: u16_at(2),
            gso_size: u16_at(4),
            csum_start: u16_at(6),
            csum_offset: u16_at(8),
        })
    }

    pub fn to_bytes(self) -> [u8; Self::LEN] {
        let mut b = [0u8; Self::LEN];
        b[0] = self.flags;
        b[1] = self.gso_type;
        b[2..4].copy_from_slice(&self.hdr_len.to_le_bytes());
        b[4..6].copy_from_slice(&self.gso_size.to_le_bytes());
        b[6..8].copy_from_slice(&self.csum_start.to_le_bytes());
        b[8..10].copy_from_slice(&self.csum_offset.to_le_bytes());
        b
    }

    pub fn needs_csum(&self) -> bool {
        self.flags & Self::F_NEEDS_CSUM != 0
    }

    /// A segmented frame, which only the guest may take.
    pub fn is_gso(&self) -> bool {
        self.gso_type != Self::GSO_NONE
    }
}

/// A frame the catcher read from the pod's `eth0`.
#[derive(Debug, Clone, Copy)]
pub struct Frame<'a> {
    pub vnet: VnetHeader,
    /// The Ethernet frame, without the virtio-net header.
    pub data: &'a [u8],
}

/// What a catcher and a responder call when traffic may wake the pod. The
/// netloop thread calls it, so it must not block. The node service resolves
/// `from` to the reason `traffic{from, port}`.
pub trait TrafficWaker: Send + Sync {
    /// `from` is the frame's source and `port` its destination port in the
    /// pod. Returns whether it asked for a wake: data on a connection that the
    /// pod opened does not wake it.
    fn wake(&self, sandbox_id: &str, from: SocketAddrV4, port: u16) -> bool;
}

/// What a release delivered.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Released {
    /// The held frames written into `tap0`.
    pub frames: usize,
    /// When the first of them was captured.
    pub first_held: Option<SystemTime>,
}

/// Every pod's catcher. Each call is idempotent and blocking, and works
/// without a catcher too: after a restart, the node service has none for the
/// holds the kernel kept.
pub trait Catchers: Send + Sync {
    /// Opens the pod's catcher and sets the hold: `eth0`'s bridge port is
    /// `listening`. On an open catcher it changes the phase and sets the
    /// hold again if it was lost.
    fn catch(&self, pod: &PodNet, phase: Phase) -> Result<(), String>;

    /// The end of a wake: lets the responder's open connections finish (at
    /// most 100 ms), sets the port to `forwarding`, writes the held frames
    /// into `tap0` in arrival order, and closes the catcher in the
    /// background. With no catcher it only sets the port to `forwarding`.
    fn release(&self, pod: &PodNet) -> Result<Released, String>;

    /// A wake was refused: answers each held SYN and data segment with a RST
    /// and drops it. The hold and the catcher stay.
    fn refuse(&self, pod: &PodNet) -> Result<(), String>;

    /// The pod failed, is deleted, or is awake after a restart: answers held
    /// SYNs and data with a RST, sets the port to `forwarding`, and closes
    /// the catcher.
    fn close(&self, pod: &PodNet) -> Result<(), String>;
}

/// Catchers for tests, which record each call as `<call> <sandbox id>`.
#[cfg(test)]
#[derive(Default)]
pub struct MockCatchers {
    calls: std::sync::Mutex<Vec<String>>,
}

#[cfg(test)]
impl MockCatchers {
    pub fn calls(&self) -> Vec<String> {
        crate::sync::lock(&self.calls).clone()
    }

    fn record(&self, call: String) {
        crate::sync::lock(&self.calls).push(call);
    }
}

#[cfg(test)]
impl Catchers for MockCatchers {
    fn catch(&self, pod: &PodNet, phase: Phase) -> Result<(), String> {
        self.record(format!("catch {} {phase:?}", pod.sandbox_id));
        Ok(())
    }

    fn release(&self, pod: &PodNet) -> Result<Released, String> {
        self.record(format!("release {}", pod.sandbox_id));
        Ok(Released::default())
    }

    fn refuse(&self, pod: &PodNet) -> Result<(), String> {
        self.record(format!("refuse {}", pod.sandbox_id));
        Ok(())
    }

    fn close(&self, pod: &PodNet) -> Result<(), String> {
        self.record(format!("close {}", pod.sandbox_id));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> PodIdentity {
        PodIdentity {
            sandbox_id: "pod".into(),
            machine: "pod".into(),
            netns: Some("/var/run/netns/cni-1".into()),
            ip: Some("10.42.0.9".into()),
            mac: Some("C2:3c:2a:92:39:90".into()),
            ..Default::default()
        }
    }

    #[test]
    fn a_pod_net_comes_from_the_record() {
        let net = PodNet::of(&identity()).unwrap();
        assert_eq!(net.ip, Ipv4Addr::new(10, 42, 0, 9));
        assert_eq!(net.mac, [0xc2, 0x3c, 0x2a, 0x92, 0x39, 0x90]);
        assert_eq!(
            (net.netns.as_str(), net.iface.as_str()),
            ("/var/run/netns/cni-1", "eth0")
        );

        let mut no_ip = identity();
        no_ip.ip = None;
        assert!(PodNet::of(&no_ip).unwrap_err().contains("no recorded IP"));
        let mut v6 = identity();
        v6.ip = Some("fd00::9".into());
        assert!(PodNet::of(&v6).unwrap_err().contains("no IPv4 address"));
    }

    #[test]
    fn a_vnet_header_round_trips() {
        let header = VnetHeader {
            flags: VnetHeader::F_NEEDS_CSUM,
            gso_type: VnetHeader::GSO_NONE,
            hdr_len: 54,
            gso_size: 0,
            csum_start: 34,
            csum_offset: 16,
        };
        let bytes = header.to_bytes();
        assert_eq!(bytes, [1, 0, 54, 0, 0, 0, 34, 0, 16, 0]);
        assert_eq!(VnetHeader::parse(&bytes), Some(header));
        assert!(header.needs_csum() && !header.is_gso());
        assert_eq!(VnetHeader::parse(&bytes[..9]), None);
    }
}
