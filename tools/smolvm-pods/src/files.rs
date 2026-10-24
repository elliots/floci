//! File and descriptor helpers: exclusive locks, durable replacement, waits on
//! a descriptor, and the node's boot id.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::time::Duration;

/// Where the kernel gives the id of the node's current boot.
pub const BOOT_ID: &str = "/proc/sys/kernel/random/boot_id";

/// Why [`lock`] failed.
pub enum LockError {
    Open(io::Error),
    Lock(io::Error),
}

impl LockError {
    /// `open PATH: ERROR` or `lock PATH: ERROR`.
    pub fn message(&self, path: &Path) -> String {
        match self {
            LockError::Open(e) => format!("open {}: {e}", path.display()),
            LockError::Lock(e) => format!("lock {}: {e}", path.display()),
        }
    }
}

/// Opens `path` for writing, creating it with `mode` if it is missing, and
/// locks it with [`flock`]. Closing the file unlocks it.
pub fn lock(path: &Path, mode: u32, wait: bool) -> Result<File, LockError> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .mode(mode)
        .open(path)
        .map_err(LockError::Open)?;
    flock(&file, wait).map_err(LockError::Lock)?;
    Ok(file)
}

/// Locks `file` exclusively. With `wait` it waits for the holder; without, a
/// held lock fails with `ErrorKind::WouldBlock`.
pub fn flock(file: &File, wait: bool) -> io::Result<()> {
    let operation = if wait {
        libc::LOCK_EX
    } else {
        libc::LOCK_EX | libc::LOCK_NB
    };
    // SAFETY: flock on a descriptor that `file` owns.
    while unsafe { libc::flock(file.as_raw_fd(), operation) } != 0 {
        let e = io::Error::last_os_error();
        if e.kind() != io::ErrorKind::Interrupted {
            return Err(e);
        }
    }
    Ok(())
}

/// Replaces the file at `path` with `data`, durably: it writes `temporary`,
/// created with `mode`, syncs it, renames it to `path` and syncs the
/// directory.
pub fn replace(path: &Path, temporary: &Path, mode: u32, data: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(mode)
        .open(temporary)?;
    file.write_all(data)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(temporary, path)?;
    match path.parent() {
        Some(dir) => sync_dir(dir),
        None => Ok(()),
    }
}

/// Waits up to `wait` for `events` on `fd`. True when they came, or a hangup
/// or an error, which the next read or write reports.
pub fn ready(fd: RawFd, events: libc::c_short, wait: Duration) -> io::Result<bool> {
    let mut poll = libc::pollfd {
        fd,
        events,
        revents: 0,
    };
    let timeout = wait.as_millis().min(i32::MAX as u128) as i32;
    loop {
        // SAFETY: one valid pollfd.
        let n = unsafe { libc::poll(&mut poll, 1, timeout) };
        if n >= 0 {
            return Ok(n > 0);
        }
        let e = io::Error::last_os_error();
        if e.kind() != io::ErrorKind::Interrupted {
            return Err(e);
        }
    }
}

pub fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// The id of the node's current boot.
pub fn boot_id() -> io::Result<String> {
    std::fs::read_to_string(BOOT_ID).map(|id| id.trim().to_string())
}
