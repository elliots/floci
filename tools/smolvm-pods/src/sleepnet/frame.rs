//! Frame classes, ARP replies for the pod IP, and the RSTs that refuse held
//! connections. A frame here is an Ethernet frame without its virtio-net
//! header.

use std::net::{Ipv4Addr, SocketAddrV4};

use super::{FrameClass, PodNet};

const ETH_LEN: usize = 14;
const ETHERTYPE_IPV4: u16 = 0x0800;
const ETHERTYPE_ARP: u16 = 0x0806;
const PROTO_ICMP: u8 = 1;
const PROTO_TCP: u8 = 6;
const PROTO_UDP: u8 = 17;
const ICMP_ECHO_REQUEST: u8 = 8;
const ARP_REQUEST: u16 = 1;
const ARP_REPLY: u16 = 2;

pub(super) const FIN: u8 = 0x01;
pub(super) const SYN: u8 = 0x02;
pub(super) const RST: u8 = 0x04;
pub(super) const ACK: u8 = 0x10;

/// What the catcher reads from a frame to the pod.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Packet {
    pub class: FrameClass,
    pub src_mac: [u8; 6],
    /// The sender, with its port for TCP and UDP.
    pub from: SocketAddrV4,
    /// The destination port in the pod, for TCP and UDP.
    pub port: u16,
    /// The TCP header of a segment that is not an IP fragment.
    pub tcp: Option<Segment>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Segment {
    pub seq: u32,
    pub ack: u32,
    pub flags: u8,
    /// Its length in sequence space: the payload, plus one each for SYN and
    /// FIN.
    pub len: u32,
}

fn u16_at(b: &[u8], i: usize) -> u16 {
    u16::from_be_bytes([b[i], b[i + 1]])
}

fn u32_at(b: &[u8], i: usize) -> u32 {
    u32::from_be_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]])
}

fn ip_at(b: &[u8], i: usize) -> Ipv4Addr {
    Ipv4Addr::new(b[i], b[i + 1], b[i + 2], b[i + 3])
}

/// Reads a frame to the pod at `pod_ip`. Anything the catcher does not take
/// apart is [`FrameClass::Other`].
pub(super) fn parse(frame: &[u8], pod_ip: Ipv4Addr) -> Packet {
    let mut packet = Packet {
        class: FrameClass::Other,
        src_mac: [0; 6],
        from: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0),
        port: 0,
        tcp: None,
    };
    if frame.len() < ETH_LEN {
        return packet;
    }
    packet.src_mac.copy_from_slice(&frame[6..12]);
    let body = &frame[ETH_LEN..];
    match u16_at(frame, 12) {
        ETHERTYPE_ARP => parse_arp(body, pod_ip, &mut packet),
        ETHERTYPE_IPV4 => parse_ipv4(body, pod_ip, &mut packet),
        _ => {}
    }
    packet
}

fn parse_arp(arp: &[u8], pod_ip: Ipv4Addr, packet: &mut Packet) {
    let request = arp.len() >= 28
        && arp[..6] == [0, 1, 0x08, 0x00, 6, 4]
        && u16_at(arp, 6) == ARP_REQUEST
        && ip_at(arp, 24) == pod_ip;
    if request {
        packet.class = FrameClass::ArpRequest;
        packet.from = SocketAddrV4::new(ip_at(arp, 14), 0);
    }
}

fn parse_ipv4(ip: &[u8], pod_ip: Ipv4Addr, packet: &mut Packet) {
    if ip.len() < 20 || ip[0] >> 4 != 4 {
        return;
    }
    let header = usize::from(ip[0] & 0x0f) * 4;
    // A segmented frame says 0 when it is longer than 64 KiB (BIG TCP).
    let total = match usize::from(u16_at(ip, 2)) {
        0 => ip.len(),
        n => n.min(ip.len()),
    };
    if header < 20 || total < header || ip_at(ip, 16) != pod_ip {
        return;
    }
    packet.from = SocketAddrV4::new(ip_at(ip, 12), 0);
    let fragment = u16_at(ip, 6) & 0x3fff != 0;
    let first = u16_at(ip, 6) & 0x1fff == 0;
    let l4 = &ip[header..total];
    match ip[9] {
        PROTO_TCP => {
            if first && l4.len() >= 4 {
                packet.from.set_port(u16_at(l4, 0));
                packet.port = u16_at(l4, 2);
            }
            if fragment {
                packet.class = FrameClass::TcpData;
                return;
            }
            let Some(segment) = parse_tcp(l4) else {
                return;
            };
            packet.class = match segment.flags {
                f if f & RST != 0 => FrameClass::TcpRst,
                f if f & SYN != 0 => FrameClass::TcpSyn,
                f if f & FIN != 0 || segment.len > 0 => FrameClass::TcpData,
                _ => FrameClass::TcpBare,
            };
            packet.tcp = Some(segment);
        }
        PROTO_UDP => {
            packet.class = FrameClass::Udp;
            if first && l4.len() >= 4 {
                packet.from.set_port(u16_at(l4, 0));
                packet.port = u16_at(l4, 2);
            }
        }
        PROTO_ICMP if !fragment && l4.first() == Some(&ICMP_ECHO_REQUEST) => {
            packet.class = FrameClass::IcmpEcho;
        }
        _ => {}
    }
}

fn parse_tcp(tcp: &[u8]) -> Option<Segment> {
    if tcp.len() < 20 {
        return None;
    }
    let offset = usize::from(tcp[12] >> 4) * 4;
    if offset < 20 || offset > tcp.len() {
        return None;
    }
    let flags = tcp[13];
    let payload = (tcp.len() - offset) as u32;
    Some(Segment {
        seq: u32_at(tcp, 4),
        ack: u32_at(tcp, 8),
        flags,
        len: payload + u32::from(flags & SYN != 0) + u32::from(flags & FIN != 0),
    })
}

/// The reply to an ARP request for the pod IP: the pod IP is at the guest's
/// MAC.
pub(super) fn arp_reply(request: &[u8], pod: &PodNet) -> Option<Vec<u8>> {
    if parse(request, pod.ip).class != FrameClass::ArpRequest {
        return None;
    }
    let arp = &request[ETH_LEN..];
    let (sender_mac, sender_ip) = (&arp[8..14], &arp[14..18]);
    let mut reply = Vec::with_capacity(42);
    reply.extend_from_slice(sender_mac);
    reply.extend_from_slice(&pod.mac);
    reply.extend_from_slice(&ETHERTYPE_ARP.to_be_bytes());
    reply.extend_from_slice(&[0, 1, 0x08, 0x00, 6, 4]);
    reply.extend_from_slice(&ARP_REPLY.to_be_bytes());
    reply.extend_from_slice(&pod.mac);
    reply.extend_from_slice(&pod.ip.octets());
    reply.extend_from_slice(sender_mac);
    reply.extend_from_slice(sender_ip);
    Some(reply)
}

/// The RST from the pod that refuses a held TCP segment, as a closed port
/// answers it (RFC 9293 section 3.10.7.1).
pub(super) fn reset(segment: &[u8], pod: &PodNet) -> Option<Vec<u8>> {
    let packet = parse(segment, pod.ip);
    let tcp = packet.tcp.filter(|t| t.flags & RST == 0)?;
    let (seq, ack, flags) = if tcp.flags & ACK != 0 {
        (tcp.ack, 0, RST)
    } else {
        (0, tcp.seq.wrapping_add(tcp.len), RST | ACK)
    };
    Some(tcp_frame(
        &TcpFrame {
            dst_mac: packet.src_mac,
            src_mac: pod.mac,
            src: SocketAddrV4::new(pod.ip, packet.port),
            dst: packet.from,
            seq,
            ack,
            flags,
        },
        &[],
    ))
}

/// A TCP segment with no options in an IPv4 frame, with its checksums.
#[derive(Debug, Clone, Copy)]
pub(super) struct TcpFrame {
    pub dst_mac: [u8; 6],
    pub src_mac: [u8; 6],
    pub src: SocketAddrV4,
    pub dst: SocketAddrV4,
    pub seq: u32,
    pub ack: u32,
    pub flags: u8,
}

pub(super) fn tcp_frame(f: &TcpFrame, payload: &[u8]) -> Vec<u8> {
    let tcp_len = 20 + payload.len();
    let mut out = Vec::with_capacity(ETH_LEN + 20 + tcp_len);
    out.extend_from_slice(&f.dst_mac);
    out.extend_from_slice(&f.src_mac);
    out.extend_from_slice(&ETHERTYPE_IPV4.to_be_bytes());

    let mut ip = [0u8; 20];
    ip[0] = 0x45;
    ip[2..4].copy_from_slice(&((20 + tcp_len) as u16).to_be_bytes());
    ip[6] = 0x40;
    ip[8] = 64;
    ip[9] = PROTO_TCP;
    ip[12..16].copy_from_slice(&f.src.ip().octets());
    ip[16..20].copy_from_slice(&f.dst.ip().octets());
    let sum = checksum(0, &ip);
    ip[10..12].copy_from_slice(&sum.to_be_bytes());
    out.extend_from_slice(&ip);

    let mut tcp = vec![0u8; tcp_len];
    tcp[0..2].copy_from_slice(&f.src.port().to_be_bytes());
    tcp[2..4].copy_from_slice(&f.dst.port().to_be_bytes());
    tcp[4..8].copy_from_slice(&f.seq.to_be_bytes());
    tcp[8..12].copy_from_slice(&f.ack.to_be_bytes());
    tcp[12] = 5 << 4;
    tcp[13] = f.flags;
    if f.flags & RST == 0 {
        tcp[14..16].copy_from_slice(&64240u16.to_be_bytes());
    }
    tcp[20..].copy_from_slice(payload);
    let mut pseudo = [0u8; 12];
    pseudo[0..4].copy_from_slice(&f.src.ip().octets());
    pseudo[4..8].copy_from_slice(&f.dst.ip().octets());
    pseudo[9] = PROTO_TCP;
    pseudo[10..12].copy_from_slice(&(tcp_len as u16).to_be_bytes());
    let sum = checksum(sum_words(0, &pseudo), &tcp);
    tcp[16..18].copy_from_slice(&sum.to_be_bytes());
    out.extend_from_slice(&tcp);
    out
}

fn sum_words(mut sum: u32, bytes: &[u8]) -> u32 {
    let (pairs, rest) = bytes.as_chunks::<2>();
    for pair in pairs {
        sum += u32::from(u16::from_be_bytes(*pair));
    }
    if let [last] = rest {
        sum += u32::from(*last) << 8;
    }
    sum
}

/// The Internet checksum of `bytes`, from a partial `sum`.
pub fn checksum(sum: u32, bytes: &[u8]) -> u16 {
    let mut sum = sum_words(sum, bytes);
    while sum > 0xffff {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

/// Test frames to a pod.
#[cfg(test)]
pub(super) mod build {
    use super::*;

    pub const POD_IP: Ipv4Addr = Ipv4Addr::new(10, 42, 0, 17);
    pub const GUEST_MAC: [u8; 6] = [0xc2, 0x3c, 0x2a, 0x92, 0x39, 0x90];
    pub const CLIENT_MAC: [u8; 6] = [0x0a, 0x58, 0x0a, 0x2a, 0x00, 0xfa];
    pub const CLIENT: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(10, 42, 0, 250), 40000);
    pub const KUBELET: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(10, 42, 0, 1), 50000);

    pub fn pod() -> PodNet {
        PodNet {
            sandbox_id: "pod".into(),
            netns: "/var/run/netns/cni-1".into(),
            iface: "eth0".into(),
            ip: POD_IP,
            mac: GUEST_MAC,
        }
    }

    /// A segment from `from` to the pod's `port`.
    pub fn tcp(from: SocketAddrV4, port: u16, flags: u8, seq: u32, payload: &[u8]) -> Vec<u8> {
        tcp_frame(
            &TcpFrame {
                dst_mac: GUEST_MAC,
                src_mac: CLIENT_MAC,
                src: from,
                dst: SocketAddrV4::new(POD_IP, port),
                seq,
                ack: if flags & ACK != 0 { 7000 } else { 0 },
                flags,
            },
            payload,
        )
    }

    pub fn syn(from: SocketAddrV4, port: u16) -> Vec<u8> {
        tcp(from, port, SYN, 1000, &[])
    }

    pub fn arp_request(target: Ipv4Addr) -> Vec<u8> {
        let mut f = vec![0xff; 6];
        f.extend_from_slice(&CLIENT_MAC);
        f.extend_from_slice(&[0x08, 0x06, 0, 1, 0x08, 0x00, 6, 4, 0, 1]);
        f.extend_from_slice(&CLIENT_MAC);
        f.extend_from_slice(&CLIENT.ip().octets());
        f.extend_from_slice(&[0; 6]);
        f.extend_from_slice(&target.octets());
        f
    }

    /// An IPv4 packet of protocol `proto` to `dst`, with `l4` after a
    /// 20-byte header.
    pub fn ipv4(proto: u8, dst: Ipv4Addr, l4: &[u8]) -> Vec<u8> {
        let mut f = GUEST_MAC.to_vec();
        f.extend_from_slice(&CLIENT_MAC);
        f.extend_from_slice(&[0x08, 0x00, 0x45, 0, 0, 0, 0, 0, 0x40, 0, 64, proto, 0, 0]);
        let len = (20 + l4.len()) as u16;
        f[16..18].copy_from_slice(&len.to_be_bytes());
        f.extend_from_slice(&CLIENT.ip().octets());
        f.extend_from_slice(&dst.octets());
        f.extend_from_slice(l4);
        f
    }

    pub fn udp(port: u16) -> Vec<u8> {
        let mut l4 = 53000u16.to_be_bytes().to_vec();
        l4.extend_from_slice(&port.to_be_bytes());
        l4.extend_from_slice(&[0, 12, 0, 0, b'p', b'i', b'n', b'g']);
        ipv4(PROTO_UDP, POD_IP, &l4)
    }

    pub fn echo_request() -> Vec<u8> {
        ipv4(
            PROTO_ICMP,
            POD_IP,
            &[ICMP_ECHO_REQUEST, 0, 0, 0, 0, 1, 0, 1],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::build::*;
    use super::*;

    fn class(frame: &[u8]) -> FrameClass {
        parse(frame, POD_IP).class
    }

    #[test]
    fn frames_fall_into_their_classes() {
        assert_eq!(class(&arp_request(POD_IP)), FrameClass::ArpRequest);
        assert_eq!(
            class(&arp_request(Ipv4Addr::new(10, 42, 0, 18))),
            FrameClass::Other
        );
        assert_eq!(class(&syn(CLIENT, 8080)), FrameClass::TcpSyn);
        assert_eq!(
            class(&tcp(CLIENT, 8080, SYN | ACK, 1, &[])),
            FrameClass::TcpSyn
        );
        assert_eq!(
            class(&tcp(CLIENT, 8080, ACK, 1, b"GET /")),
            FrameClass::TcpData
        );
        assert_eq!(
            class(&tcp(CLIENT, 8080, ACK | FIN, 1, &[])),
            FrameClass::TcpData
        );
        assert_eq!(
            class(&tcp(CLIENT, 8080, RST | ACK, 1, &[])),
            FrameClass::TcpRst
        );
        assert_eq!(class(&tcp(CLIENT, 8080, ACK, 1, &[])), FrameClass::TcpBare);
        assert_eq!(class(&udp(53)), FrameClass::Udp);
        assert_eq!(class(&echo_request()), FrameClass::IcmpEcho);
        let echo_reply = ipv4(PROTO_ICMP, POD_IP, &[0, 0, 0, 0, 0, 1, 0, 1]);
        assert_eq!(class(&echo_reply), FrameClass::Other);
        assert_eq!(class(&ipv4(47, POD_IP, &[0; 8])), FrameClass::Other);
        let mut ipv6 = syn(CLIENT, 8080);
        ipv6[12..14].copy_from_slice(&[0x86, 0xdd]);
        assert_eq!(class(&ipv6), FrameClass::Other);
        let other_pod = ipv4(PROTO_TCP, Ipv4Addr::new(10, 42, 0, 18), &[0; 20]);
        assert_eq!(class(&other_pod), FrameClass::Other);
        assert_eq!(class(&[0u8; 10]), FrameClass::Other);
    }

    #[test]
    fn a_segment_reports_its_sender_port_and_sequence_space() {
        let p = parse(&tcp(CLIENT, 8080, ACK | FIN, 5, b"abc"), POD_IP);
        assert_eq!((p.from, p.port, p.src_mac), (CLIENT, 8080, CLIENT_MAC));
        let t = p.tcp.unwrap();
        assert_eq!((t.seq, t.ack, t.flags, t.len), (5, 7000, ACK | FIN, 4));
        assert_eq!(parse(&syn(CLIENT, 80), POD_IP).tcp.unwrap().len, 1);
    }

    #[test]
    fn short_or_bad_headers_are_other() {
        let mut frame = syn(CLIENT, 8080);
        frame[ETH_LEN + 20 + 12] = 4 << 4;
        assert_eq!(class(&frame), FrameClass::Other, "TCP offset under 20");
        let frame = syn(CLIENT, 8080);
        assert_eq!(class(&frame[..ETH_LEN + 30]), FrameClass::Other);
        let mut frame = syn(CLIENT, 8080);
        frame[ETH_LEN] = 0x44;
        assert_eq!(class(&frame), FrameClass::Other, "IHL under 5");
    }

    #[test]
    fn tcp_fragments_are_data_and_ethernet_padding_is_not_payload() {
        let mut later = syn(CLIENT, 8080);
        later[ETH_LEN + 6..ETH_LEN + 8].copy_from_slice(&0x0010u16.to_be_bytes());
        assert_eq!(class(&later), FrameClass::TcpData);
        let mut first = syn(CLIENT, 8080);
        first[ETH_LEN + 6] |= 0x20;
        let p = parse(&first, POD_IP);
        assert_eq!((p.class, p.port), (FrameClass::TcpData, 8080));

        let mut padded = tcp(CLIENT, 8080, ACK, 1, &[]);
        padded.resize(60, 0);
        assert_eq!(class(&padded), FrameClass::TcpBare);
    }

    #[test]
    fn checksums_verify() {
        let frame = tcp(CLIENT, 8080, ACK, 9, b"odd");
        assert_eq!(checksum(0, &frame[ETH_LEN..ETH_LEN + 20]), 0);
        let mut pseudo = [0u8; 12];
        pseudo[0..4].copy_from_slice(&CLIENT.ip().octets());
        pseudo[4..8].copy_from_slice(&POD_IP.octets());
        pseudo[9] = PROTO_TCP;
        pseudo[10..12].copy_from_slice(&23u16.to_be_bytes());
        assert_eq!(checksum(sum_words(0, &pseudo), &frame[ETH_LEN + 20..]), 0);
    }

    #[test]
    fn an_arp_request_for_the_pod_gets_the_guest_mac() {
        let pod = pod();
        let reply = arp_reply(&arp_request(POD_IP), &pod).unwrap();
        assert_eq!(reply.len(), 42);
        assert_eq!(&reply[0..6], &CLIENT_MAC);
        assert_eq!(&reply[6..12], &GUEST_MAC);
        assert_eq!(&reply[12..22], &[0x08, 0x06, 0, 1, 0x08, 0x00, 6, 4, 0, 2]);
        assert_eq!(&reply[22..28], &GUEST_MAC);
        assert_eq!(&reply[28..32], &POD_IP.octets());
        assert_eq!(&reply[32..38], &CLIENT_MAC);
        assert_eq!(&reply[38..42], &CLIENT.ip().octets());
        assert!(arp_reply(&arp_request(Ipv4Addr::new(10, 42, 0, 1)), &pod).is_none());
        assert!(arp_reply(&syn(CLIENT, 80), &pod).is_none());
    }

    #[test]
    fn a_reset_refuses_a_syn_and_data() {
        let pod = pod();
        let rst = reset(&syn(CLIENT, 8080), &pod).unwrap();
        let p = parse(&rst, *CLIENT.ip());
        assert_eq!(&rst[0..6], &CLIENT_MAC);
        assert_eq!(&rst[6..12], &GUEST_MAC);
        assert_eq!((p.from, p.port), (SocketAddrV4::new(POD_IP, 8080), 40000));
        let t = p.tcp.unwrap();
        assert_eq!((t.flags, t.seq, t.ack), (RST | ACK, 0, 1001));

        let data = tcp(CLIENT, 8080, ACK, 1001, b"GET /");
        let t = parse(&reset(&data, &pod).unwrap(), *CLIENT.ip())
            .tcp
            .unwrap();
        assert_eq!((t.flags, t.seq), (RST, 7000));

        let rst_in = tcp(CLIENT, 8080, RST, 1, &[]);
        assert!(reset(&rst_in, &pod).is_none(), "a RST is never answered");
        assert!(reset(&udp(53), &pod).is_none());
    }
}
