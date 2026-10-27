//! What a deleted pod VM leaves on the node, and its removal.
//!
//! smolvm's delete removes a VM's record and its directory. The pod's bridge
//! stays in the pod network namespace (see [`crate::net`]), and so do smolvm's
//! files about the VM outside its directory, which smolvm's CLI removes after
//! a delete but its library does not:
//!
//! - the fork lock `.NAME.fork-operation.lock` beside the VM's directory,
//!   which every stop and delete takes, so every deleted pod leaves one;
//! - the pause lock `.NAME.fork-operation.pause-operation.lock` beside it,
//!   which every pause and resume takes, and which smolvm's sweep misses;
//! - the ready marker `.smolvm-ready.HASH` in the agent rootfs, when the VM
//!   process did not stop cleanly.
//!
//! [`remove_bridge`] removes the bridge, and [`sweep`] smolvm's files of every
//! VM that is gone, not only the one just deleted.

use std::path::Path;

use log::warn;

use crate::files;

/// smolvm's name for a VM's pause lock, after `.` and the VM's name
/// (`lock_saved_execution` in smolvm's src/agent/fork.rs).
const PAUSE_LOCK_SUFFIX: &str = ".fork-operation.pause-operation.lock";

/// Remove the bridge of the deleted VM of sandbox `id` in the pod network
/// namespace `netns`.
pub fn remove_bridge(id: &str, netns: Option<&str>) {
    if let Some(netns) = netns {
        if let Err(e) = crate::net::detach(netns) {
            warn!("remove the pod network bridge of sandbox {id}: {e}");
        }
    }
}

/// Remove smolvm's files of every VM that is gone.
pub fn sweep() {
    smolvm::agent::fork::prune_orphaned_fork_source_locks();
    prune_pause_locks_in(&smolvm::agent::vm_cache_root());
    smolvm::agent::prune_orphaned_ready_markers();
}

/// Remove the pause locks in `vms_dir`, smolvm's directory of VM directories,
/// of the VMs that are gone. As smolvm's sweep of the fork locks does, it
/// removes a lock only if the VM's directory is gone and nothing holds the
/// lock, which a pause or resume in progress does.
fn prune_pause_locks_in(vms_dir: &Path) {
    let Ok(entries) = std::fs::read_dir(vms_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(name) = file_name
            .to_str()
            .and_then(|f| f.strip_prefix('.'))
            .and_then(|f| f.strip_suffix(PAUSE_LOCK_SUFFIX))
            .filter(|name| !name.is_empty())
        else {
            continue;
        };
        if vms_dir.join(smolvm::agent::vm_dir_hash(name)).is_dir() {
            continue;
        }
        let path = entry.path();
        let Ok(lock) = std::fs::OpenOptions::new().write(true).open(&path) else {
            continue;
        };
        if files::flock(&lock, false).is_ok() {
            let _ = std::fs::remove_file(&path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::os::fd::AsRawFd;
    use std::path::PathBuf;

    fn pause_lock(vms_dir: &Path, name: &str) -> PathBuf {
        let path = vms_dir.join(format!(".{name}{PAUSE_LOCK_SUFFIX}"));
        std::fs::write(&path, b"").unwrap();
        path
    }

    #[test]
    fn the_pause_locks_of_vms_that_are_gone_are_removed() {
        let dir = tempfile::tempdir().unwrap();
        let vms = dir.path();
        let gone = pause_lock(vms, "gone");
        let live = pause_lock(vms, "live");
        std::fs::create_dir(vms.join(smolvm::agent::vm_dir_hash("live"))).unwrap();
        let busy = pause_lock(vms, "busy");
        let held = std::fs::File::open(&busy).unwrap();
        // SAFETY: flock on a descriptor that `held` owns.
        assert_eq!(unsafe { libc::flock(held.as_raw_fd(), libc::LOCK_EX) }, 0);
        let fork_lock = vms.join(".gone.fork-operation.lock");
        std::fs::write(&fork_lock, b"").unwrap();

        prune_pause_locks_in(vms);
        assert!(!gone.exists());
        assert!(live.exists(), "the VM still has its directory");
        assert!(busy.exists(), "a pause or resume holds the lock");
        assert!(fork_lock.exists(), "smolvm's sweep removes the fork locks");

        drop(held);
        // A process that another test starts holds a copy of every descriptor
        // until it execs, and so the lock, so try again for a moment.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        prune_pause_locks_in(vms);
        while busy.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
            prune_pause_locks_in(vms);
        }
        assert!(!busy.exists());
        prune_pause_locks_in(&vms.join("missing"));
    }
}
