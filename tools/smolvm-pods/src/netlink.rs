//! Raw netlink over libc, which the catcher's route netlink socket
//! ([`crate::sleepnet`]) and the activity monitor's ctnetlink socket
//! ([`crate::activity`]) share: the socket, the messages and their
//! attributes. The crate has no netlink dependency, so messages are built
//! and parsed here. [`set_option`] and [`check`] serve the catcher's packet
//! socket and the netloop's epoll too.

use std::io;
use std::mem::size_of;
use std::os::fd::{FromRawFd, OwnedFd, RawFd};

// <linux/netlink.h>
pub const NLMSG_HDRLEN: usize = 16;
pub const NLMSG_ERROR: u16 = 2;
pub const NLMSG_DONE: u16 = 3;
pub const NLM_F_REQUEST: u16 = 0x1;
pub const NLM_F_ACK: u16 = 0x4;
pub const NLM_F_DUMP: u16 = 0x300;
pub const NLA_F_NESTED: u16 = 0x8000;
const NLA_TYPE_MASK: u16 = 0x3fff;

/// A netlink socket of `protocol` in the calling thread's network
/// namespace, with `flags` such as `SOCK_NONBLOCK`.
pub fn socket(protocol: libc::c_int, flags: libc::c_int) -> io::Result<OwnedFd> {
    // SAFETY: plain socket call.
    let fd = unsafe {
        libc::socket(
            libc::AF_NETLINK,
            libc::SOCK_RAW | libc::SOCK_CLOEXEC | flags,
            protocol,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fd` is a fresh descriptor that nothing else owns.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Sets the option `name` at `level` of any socket to `value`.
pub fn set_option<T>(
    fd: RawFd,
    level: libc::c_int,
    name: libc::c_int,
    value: &T,
) -> io::Result<()> {
    // SAFETY: `value` is a live value of the size passed.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            (value as *const T).cast(),
            size_of::<T>() as libc::socklen_t,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// A libc return code as an `io::Result`.
pub fn check(rc: libc::c_int) -> io::Result<libc::c_int> {
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(rc)
    }
}

/// A netlink message being built.
pub struct Message(Vec<u8>);

impl Message {
    /// A message of type `kind` with `flags` whose payload starts with
    /// `fixed`, the fixed header of its family, such as `ifinfomsg`.
    pub fn new(kind: u16, flags: u16, fixed: &[u8]) -> Message {
        let mut b = vec![0u8; NLMSG_HDRLEN];
        b[4..6].copy_from_slice(&kind.to_ne_bytes());
        b[6..8].copy_from_slice(&flags.to_ne_bytes());
        b.extend_from_slice(fixed);
        Message(b)
    }

    /// Appends the attribute `kind` with `value`, padded to 4 bytes.
    pub fn attr(&mut self, kind: u16, value: &[u8]) {
        let len = 4 + value.len();
        self.0.extend_from_slice(&(len as u16).to_ne_bytes());
        self.0.extend_from_slice(&kind.to_ne_bytes());
        self.0.extend_from_slice(value);
        self.0.resize(self.0.len().next_multiple_of(4), 0);
    }

    /// Appends the attribute `kind` holding the attributes that `inner`
    /// appends.
    pub fn nest(&mut self, kind: u16, inner: impl FnOnce(&mut Message)) {
        let start = self.0.len();
        self.attr(kind, &[]);
        inner(self);
        let len = (self.0.len() - start) as u16;
        self.0[start..start + 2].copy_from_slice(&len.to_ne_bytes());
    }

    /// The message, with its length and the sequence number `seq`.
    pub fn finish(mut self, seq: u32) -> Vec<u8> {
        let len = self.0.len() as u32;
        self.0[0..4].copy_from_slice(&len.to_ne_bytes());
        self.0[8..12].copy_from_slice(&seq.to_ne_bytes());
        self.0
    }
}

/// The messages in one recv, as `(type, sequence number, payload)`. A length
/// that does not fit is an error, which ends them.
pub fn messages(mut buf: &[u8]) -> impl Iterator<Item = Result<(u16, u32, &[u8]), String>> {
    std::iter::from_fn(move || {
        if buf.len() < NLMSG_HDRLEN {
            return None;
        }
        let len = u32::from_ne_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if len < NLMSG_HDRLEN || len > buf.len() {
            let error = format!(
                "a netlink message of {len} bytes in a reply of {} bytes",
                buf.len()
            );
            buf = &[];
            return Some(Err(error));
        }
        let kind = u16::from_ne_bytes([buf[4], buf[5]]);
        let seq = u32::from_ne_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let payload = &buf[NLMSG_HDRLEN..len];
        buf = &buf[len.next_multiple_of(4).min(buf.len())..];
        Some(Ok((kind, seq, payload)))
    })
}

/// The code in the payload of an `NLMSG_ERROR` or `NLMSG_DONE` message: 0
/// or a negative errno.
pub fn error_code(payload: &[u8]) -> Option<i32> {
    Some(i32::from_ne_bytes(payload.get(..4)?.try_into().ok()?))
}

/// The attributes in `buf`, as `(type, value)`, the type without the nested
/// and byte order flags. A malformed attribute ends them.
pub fn attrs(mut buf: &[u8]) -> impl Iterator<Item = (u16, &[u8])> {
    std::iter::from_fn(move || {
        let len = usize::from(u16::from_ne_bytes([*buf.first()?, *buf.get(1)?]));
        let kind = u16::from_ne_bytes([*buf.get(2)?, *buf.get(3)?]) & NLA_TYPE_MASK;
        if len < 4 || len > buf.len() {
            return None;
        }
        let value = &buf[4..len];
        buf = &buf[len.next_multiple_of(4).min(buf.len())..];
        Some((kind, value))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attributes_are_padded_and_nests_hold_their_length() {
        let mut m = Message::new(16, NLM_F_REQUEST | NLM_F_ACK, &[7; 4]);
        m.attr(3, b"abc");
        m.nest(NLA_F_NESTED | 4, |m| {
            m.attr(1, &[9]);
            m.attr(2, &[]);
        });
        let bytes = m.finish(42);
        assert_eq!(
            bytes,
            [
                44, 0, 0, 0, 16, 0, 5, 0, 42, 0, 0, 0, 0, 0, 0, 0, 7, 7, 7, 7, // headers
                7, 0, 3, 0, b'a', b'b', b'c', 0, // attribute 3, padded
                16, 0, 4, 0x80, // the nest
                5, 0, 1, 0, 9, 0, 0, 0, // attribute 1, padded
                4, 0, 2, 0, // attribute 2, empty
            ]
        );
        let attributes: Vec<_> = attrs(&bytes[NLMSG_HDRLEN + 4..]).collect();
        assert_eq!(attributes, [(3, &b"abc"[..]), (4, &bytes[32..])]);
        let inner: Vec<_> = attrs(attributes[1].1).collect();
        assert_eq!(inner, [(1, &[9][..]), (2, &[][..])]);
        let mut malformed = bytes[NLMSG_HDRLEN + 4..].to_vec();
        malformed[8] = 100;
        assert_eq!(attrs(&malformed).count(), 1, "a nest longer than the rest");
    }

    #[test]
    fn a_recv_splits_into_messages_and_ends_at_a_bad_length() {
        let a = Message::new(libc::RTM_NEWLINK, 0, &[0; 16]).finish(5);
        let b = Message::new(libc::RTM_DELLINK, 0, &[0; 16]).finish(0);
        let mut both = a.clone();
        both.extend_from_slice(&b);
        let kinds: Vec<_> = messages(&both)
            .map(|m| m.map(|(kind, seq, payload)| (kind, seq, payload.len())))
            .collect();
        assert_eq!(
            kinds,
            [
                Ok((libc::RTM_NEWLINK, 5, 16)),
                Ok((libc::RTM_DELLINK, 0, 16))
            ]
        );
        let mut bad = a;
        bad[0..4].copy_from_slice(&1000u32.to_ne_bytes());
        bad.extend_from_slice(&b);
        let read: Vec<_> = messages(&bad).collect();
        assert_eq!(
            read,
            [Err(
                "a netlink message of 1000 bytes in a reply of 64 bytes".into()
            )]
        );
        assert_eq!(messages(&both[..15]).count(), 0, "a short header");
    }

    #[test]
    fn error_codes_are_the_first_four_bytes() {
        assert_eq!(
            error_code(&(-libc::ENOENT).to_ne_bytes()),
            Some(-libc::ENOENT)
        );
        assert_eq!(error_code(&[0; 20]), Some(0));
        assert_eq!(error_code(&[0; 3]), None);
    }
}
