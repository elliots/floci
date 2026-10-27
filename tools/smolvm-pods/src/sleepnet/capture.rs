//! The packet socket on a pod's `eth0`: opened with protocol 0 and bound to
//! `ETH_P_ALL` afterwards, because a socket opened with `ETH_P_ALL` waits
//! about 36 ms for RCU in the nested node; with `PACKET_VNET_HDR`, so
//! that checksum and segmentation metadata survive capture and replay;
//! `SO_TIMESTAMPNS`; and a classic BPF filter that passes only ARP requests
//! for the pod IP and IPv4 frames to it, and no outgoing frames.

use std::fs::File;
use std::io;
use std::mem::{size_of, size_of_val, zeroed};
use std::net::Ipv4Addr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::VnetHeader;
use crate::netlink::{check, set_option};

/// Room for a frame, segmented or not, and its virtio-net header, up to
/// 128 KiB. A longer frame is truncated and dropped.
pub(super) const RECV_BUFFER: usize = 128 * 1024;

/// A packet socket bound to one interface of a pod's network namespace.
#[derive(Debug)]
pub(super) struct Capture {
    fd: OwnedFd,
}

/// A frame read from a [`Capture`]: its virtio-net header and the frame
/// are the first `len` bytes of the buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Received {
    pub len: usize,
    /// When the kernel received it.
    pub at: SystemTime,
    /// The frame did not fit in the buffer.
    pub truncated: bool,
}

impl Capture {
    /// Opens the socket on the interface `ifindex` of the calling thread's
    /// network namespace, for the pod at `ip`.
    pub fn open(ifindex: i32, ip: Ipv4Addr) -> io::Result<Capture> {
        // SAFETY: plain socket call.
        let raw = check(unsafe {
            libc::socket(
                libc::AF_PACKET,
                libc::SOCK_RAW | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                0,
            )
        })?;
        // SAFETY: `raw` is a fresh descriptor that nothing else owns.
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        let one: libc::c_int = 1;
        set_option(raw, libc::SOL_PACKET, libc::PACKET_VNET_HDR, &one)?;
        set_option(raw, libc::SOL_PACKET, libc::PACKET_IGNORE_OUTGOING, &one)?;
        let code = filter(ip);
        let program = libc::sock_fprog {
            len: code.len() as u16,
            filter: code.as_ptr().cast_mut(),
        };
        set_option(raw, libc::SOL_SOCKET, libc::SO_ATTACH_FILTER, &program)?;
        set_option(raw, libc::SOL_SOCKET, libc::SO_TIMESTAMPNS, &one)?;
        // SAFETY: a zeroed sockaddr_ll is valid, then filled in.
        let mut addr: libc::sockaddr_ll = unsafe { zeroed() };
        addr.sll_family = libc::AF_PACKET as u16;
        addr.sll_protocol = (libc::ETH_P_ALL as u16).to_be();
        addr.sll_ifindex = ifindex;
        // SAFETY: binds to a valid sockaddr_ll of the size passed.
        check(unsafe {
            libc::bind(
                raw,
                (&addr as *const libc::sockaddr_ll).cast(),
                size_of::<libc::sockaddr_ll>() as libc::socklen_t,
            )
        })?;
        Ok(Capture { fd })
    }

    pub fn fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    /// The next frame, with its virtio-net header, or none for now.
    pub fn recv(&self, buf: &mut [u8]) -> io::Result<Option<Received>> {
        let mut iov = libc::iovec {
            iov_base: buf.as_mut_ptr().cast(),
            iov_len: buf.len(),
        };
        let mut control = [0u64; 8];
        // SAFETY: a zeroed msghdr is valid, then filled in.
        let mut msg: libc::msghdr = unsafe { zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr().cast();
        msg.msg_controllen = size_of_val(&control) as _;
        // SAFETY: `msg` points at live buffers of the sizes it states.
        let n = unsafe {
            libc::recvmsg(
                self.fd.as_raw_fd(),
                &mut msg,
                libc::MSG_DONTWAIT | libc::MSG_TRUNC,
            )
        };
        if n < 0 {
            let e = io::Error::last_os_error();
            return match e.kind() {
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted => Ok(None),
                _ => Err(e),
            };
        }
        let mut at = None;
        // SAFETY: walks the control messages that recvmsg wrote into
        // `control`, within `msg_controllen`.
        unsafe {
            let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
            while !cmsg.is_null() {
                if (*cmsg).cmsg_level == libc::SOL_SOCKET
                    && (*cmsg).cmsg_type == libc::SCM_TIMESTAMPNS
                {
                    let ts: libc::timespec = std::ptr::read_unaligned(libc::CMSG_DATA(cmsg).cast());
                    at = Some(UNIX_EPOCH + Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32));
                }
                cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
            }
        }
        let len = n as usize;
        Ok(Some(Received {
            len: len.min(buf.len()),
            at: at.unwrap_or_else(SystemTime::now),
            truncated: len > buf.len(),
        }))
    }

    /// Sends `frame` with its virtio-net header out of the interface
    /// `ifindex`: eth0 toward the node, or tap0 toward the guest.
    pub fn send(&self, ifindex: i32, vnet: VnetHeader, frame: &[u8]) -> io::Result<()> {
        let header = vnet.to_bytes();
        let mut iov = [
            libc::iovec {
                iov_base: header.as_ptr().cast_mut().cast(),
                iov_len: header.len(),
            },
            libc::iovec {
                iov_base: frame.as_ptr().cast_mut().cast(),
                iov_len: frame.len(),
            },
        ];
        // SAFETY: a zeroed sockaddr_ll is valid, then filled in.
        let mut addr: libc::sockaddr_ll = unsafe { zeroed() };
        addr.sll_family = libc::AF_PACKET as u16;
        addr.sll_ifindex = ifindex;
        addr.sll_halen = 6;
        if let Some(dst) = frame.get(..6) {
            addr.sll_addr[..6].copy_from_slice(dst);
        }
        // SAFETY: a zeroed msghdr is valid, then filled in.
        let mut msg: libc::msghdr = unsafe { zeroed() };
        msg.msg_name = (&mut addr as *mut libc::sockaddr_ll).cast();
        msg.msg_namelen = size_of::<libc::sockaddr_ll>() as libc::socklen_t;
        msg.msg_iov = iov.as_mut_ptr();
        msg.msg_iovlen = iov.len() as _;
        // SAFETY: `msg` points at live buffers of the sizes it states.
        let n = unsafe { libc::sendmsg(self.fd.as_raw_fd(), &msg, libc::MSG_DONTWAIT) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

/// Runs `f` on a new thread in the network namespace at `netns`, so that the
/// sockets it opens belong to that namespace.
pub(super) fn in_netns<T, F>(netns: &str, f: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    let path = netns.to_string();
    let thread = std::thread::Builder::new()
        .name("sleepnet-netns".into())
        .spawn(move || {
            let ns = File::open(&path).map_err(|e| format!("open {path}: {e}"))?;
            nix::sched::setns(ns, nix::sched::CloneFlags::CLONE_NEWNET)
                .map_err(|e| format!("enter {path}: {e}"))?;
            f()
        })
        .map_err(|e| format!("spawn a thread for {netns}: {e}"))?;
    thread
        .join()
        .map_err(|_| format!("the thread for {netns} panicked"))?
}

fn statement(code: u32, k: u32) -> libc::sock_filter {
    libc::sock_filter {
        code: code as u16,
        jt: 0,
        jf: 0,
        k,
    }
}

fn jump_if_equal(k: u32, jt: u8, jf: u8) -> libc::sock_filter {
    libc::sock_filter {
        code: (libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K) as u16,
        jt,
        jf,
        k,
    }
}

/// The classic BPF program that passes ARP requests for `ip` and IPv4
/// frames to `ip`, whole, and drops outgoing frames and everything else.
pub(super) fn filter(ip: Ipv4Addr) -> Vec<libc::sock_filter> {
    let ip = u32::from(ip);
    let load_word = |at| statement(libc::BPF_LD | libc::BPF_W | libc::BPF_ABS, at);
    let load_half = |at| statement(libc::BPF_LD | libc::BPF_H | libc::BPF_ABS, at);
    let ret = |k| statement(libc::BPF_RET | libc::BPF_K, k);
    // The jump offsets count from the next instruction; 11 passes, 12 drops.
    vec![
        load_word((libc::SKF_AD_OFF + libc::SKF_AD_PKTTYPE) as u32),
        jump_if_equal(libc::PACKET_OUTGOING as u32, 10, 0),
        load_half(12),
        jump_if_equal(0x0806, 0, 4),
        load_half(20),
        jump_if_equal(1, 0, 6),
        load_word(38),
        jump_if_equal(ip, 3, 4),
        jump_if_equal(0x0800, 0, 3),
        load_word(30),
        jump_if_equal(ip, 0, 1),
        ret(u32::MAX),
        ret(0),
    ]
}

#[cfg(test)]
mod tests {
    use super::super::frame::build::*;
    use super::*;

    /// Runs the subset of classic BPF that [`filter`] uses, as the kernel
    /// would on `frame` with packet type `pkttype`.
    fn run(code: &[libc::sock_filter], frame: &[u8], pkttype: u32) -> u32 {
        let pkttype_offset = (libc::SKF_AD_OFF + libc::SKF_AD_PKTTYPE) as u32;
        let mut a = 0u32;
        let mut pc = 0usize;
        loop {
            let i = code[pc];
            pc += 1;
            let code = u32::from(i.code);
            let load = |size: usize| -> Option<u32> {
                let at = i.k as usize;
                let bytes = frame.get(at..at + size)?;
                Some(bytes.iter().fold(0, |v, b| v << 8 | u32::from(*b)))
            };
            match code {
                c if c == libc::BPF_LD | libc::BPF_W | libc::BPF_ABS && i.k == pkttype_offset => {
                    a = pkttype;
                }
                c if c == libc::BPF_LD | libc::BPF_W | libc::BPF_ABS => match load(4) {
                    Some(v) => a = v,
                    None => return 0,
                },
                c if c == libc::BPF_LD | libc::BPF_H | libc::BPF_ABS => match load(2) {
                    Some(v) => a = v,
                    None => return 0,
                },
                c if c == libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K => {
                    pc += usize::from(if a == i.k { i.jt } else { i.jf });
                }
                c if c == libc::BPF_RET | libc::BPF_K => return i.k,
                other => panic!("instruction {other:#x} is not in the subset"),
            }
        }
    }

    const HOST: u32 = libc::PACKET_HOST as u32;
    const OTHERHOST: u32 = libc::PACKET_OTHERHOST as u32;

    #[test]
    fn the_filter_passes_arp_requests_and_ipv4_for_the_pod_only() {
        let code = filter(POD_IP);
        let other = Ipv4Addr::new(10, 42, 0, 18);
        let pass = |frame: &[u8], pkttype| run(&code, frame, pkttype) == u32::MAX;
        assert!(pass(&arp_request(POD_IP), libc::PACKET_BROADCAST as u32));
        assert!(pass(&syn(CLIENT, 8080), OTHERHOST));
        assert!(pass(&udp(53), HOST));
        assert!(pass(&echo_request(), OTHERHOST));

        assert!(!pass(&arp_request(other), libc::PACKET_BROADCAST as u32));
        let mut arp_reply = arp_request(POD_IP);
        arp_reply[21] = 2;
        assert!(!pass(&arp_reply, OTHERHOST), "an ARP reply");
        assert!(!pass(&ipv4(6, other, &[0; 20]), OTHERHOST));
        let mut ipv6 = syn(CLIENT, 8080);
        ipv6[12..14].copy_from_slice(&[0x86, 0xdd]);
        assert!(!pass(&ipv6, OTHERHOST));
        assert!(
            !pass(&syn(CLIENT, 8080), libc::PACKET_OUTGOING as u32),
            "an outgoing frame"
        );
        assert!(!pass(&[0xff; 20], OTHERHOST), "a short frame");
    }

    #[test]
    fn every_jump_lands_inside_the_program() {
        let code = filter(POD_IP);
        for (pc, i) in code.iter().enumerate() {
            if u32::from(i.code) == libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K {
                assert!(pc + 1 + usize::from(i.jt.max(i.jf)) < code.len(), "{pc}");
            }
        }
        let last = code.last().unwrap();
        assert_eq!(
            (u32::from(last.code), last.k),
            (libc::BPF_RET | libc::BPF_K, 0)
        );
    }

    #[test]
    fn a_missing_namespace_is_an_error() {
        let err = in_netns("/var/run/netns/floci-sleepnet-gone", || Ok(())).unwrap_err();
        assert!(err.contains("floci-sleepnet-gone"), "{err}");
    }
}
