//! Network namespaces and taps for the tests that need root.

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread::JoinHandle;

/// Runs `program`, an `ip`, with `args` and returns what it printed. A failure
/// fails the test.
fn ip(program: &Path, args: &[&str]) -> String {
    let out = Command::new(program).args(args).output().expect("run ip");
    assert!(
        out.status.success(),
        "ip {}: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn enter(netns: &Path) {
    let ns = std::fs::File::open(netns).unwrap();
    nix::sched::setns(ns, nix::sched::CloneFlags::CLONE_NEWNET).unwrap();
}

/// A network namespace for a test, `floci-ROLE-PID`, deleted with its links
/// on drop.
pub struct Netns {
    name: String,
    ip: PathBuf,
}

impl Netns {
    pub fn new(role: &str) -> Netns {
        Netns::with_ip(Path::new("ip"), role)
    }

    /// A namespace that `program` manages, for a test that puts another `ip`
    /// first on `PATH`.
    pub fn with_ip(program: &Path, role: &str) -> Netns {
        let name = format!("floci-{role}-{}", std::process::id());
        ip(program, &["netns", "add", &name]);
        let netns = Netns {
            name,
            ip: program.to_path_buf(),
        };
        netns.ip(&["link", "set", "lo", "up"]);
        netns
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn path(&self) -> PathBuf {
        Path::new("/var/run/netns").join(&self.name)
    }

    /// Runs `ip` in this namespace and returns what it printed.
    pub fn ip(&self, args: &[&str]) -> String {
        let mut all = vec!["-n", self.name.as_str()];
        all.extend_from_slice(args);
        ip(&self.ip, &all)
    }

    /// Starts `f` on a thread in this namespace.
    pub fn spawn<T: Send + 'static>(
        &self,
        f: impl FnOnce() -> T + Send + 'static,
    ) -> JoinHandle<T> {
        let path = self.path();
        std::thread::spawn(move || {
            enter(&path);
            f()
        })
    }

    pub fn run<T: Send + 'static>(&self, f: impl FnOnce() -> T + Send + 'static) -> T {
        self.spawn(f).join().unwrap()
    }
}

impl Drop for Netns {
    fn drop(&mut self) {
        let _ = Command::new(&self.ip)
            .args(["netns", "del", &self.name])
            .status();
    }
}

/// Opens a tap named `name` without packet information in the calling
/// thread's namespace, as smolvm opens tap0. With `vnet_hdr` its frames carry
/// virtio-net headers, and it takes checksum offload.
pub fn open_tap(name: &str, vnet_hdr: bool) -> OwnedFd {
    // SAFETY: plain open of a device node.
    let fd = unsafe { libc::open(c"/dev/net/tun".as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
    assert!(
        fd >= 0,
        "open /dev/net/tun: {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: `fd` is a fresh descriptor.
    let tun = unsafe { OwnedFd::from_raw_fd(fd) };
    #[repr(C)]
    struct Ifreq {
        name: [libc::c_char; libc::IFNAMSIZ],
        flags: libc::c_short,
        _pad: [u8; 22],
    }
    let mut flags = libc::IFF_TAP | libc::IFF_NO_PI;
    if vnet_hdr {
        flags |= libc::IFF_VNET_HDR;
    }
    let mut req = Ifreq {
        name: [0; libc::IFNAMSIZ],
        flags: flags as libc::c_short,
        _pad: [0; 22],
    };
    for (dst, src) in req.name.iter_mut().zip(name.bytes()) {
        *dst = src as libc::c_char;
    }
    // SAFETY: TUNSETIFF with a valid ifreq.
    let rc = unsafe { libc::ioctl(tun.as_raw_fd(), libc::TUNSETIFF as _, &mut req) };
    assert_eq!(rc, 0, "TUNSETIFF: {}", std::io::Error::last_os_error());
    if vnet_hdr {
        let offload = libc::TUN_F_CSUM as libc::c_ulong;
        // SAFETY: TUNSETOFFLOAD takes its flags by value.
        let rc = unsafe { libc::ioctl(tun.as_raw_fd(), libc::TUNSETOFFLOAD as _, offload) };
        assert_eq!(rc, 0, "TUNSETOFFLOAD: {}", std::io::Error::last_os_error());
    }
    tun
}
