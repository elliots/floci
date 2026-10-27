//! Node-side binds of a Docker container's mounts into its pod share.
//!
//! A standalone container's bind mounts, such as Docker's named volumes and
//! its `-v /dir:/x` binds, are bound in place: their source is bound at the
//! mount's path under the pod share, which the guest agent binds into the
//! container, so the VM writes to the source itself. A directory that holds
//! such a bind must never be removed with a plain recursive delete, which
//! would delete the source's contents: [`remove_tree`] detaches every mount
//! under it first.

use std::path::{Path, PathBuf};

use nix::mount::{mount, umount2, MntFlags, MsFlags};

/// Binds `source` at `target`, read-only when `read_only`, creating `target`
/// as a directory or an empty file to match the source.
pub fn bind_in_place(source: &Path, target: &Path, read_only: bool) -> Result<(), String> {
    let meta = std::fs::metadata(source).map_err(|e| format!("stat {}: {e}", source.display()))?;
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    }
    let made = if meta.is_dir() {
        std::fs::create_dir(target)
    } else {
        std::fs::File::create(target).map(drop)
    };
    made.map_err(|e| format!("create {}: {e}", target.display()))?;
    mount(
        Some(source),
        target,
        None::<&str>,
        MsFlags::MS_BIND | MsFlags::MS_REC,
        None::<&str>,
    )
    .map_err(|e| format!("bind {} at {}: {e}", source.display(), target.display()))?;
    if read_only {
        let flags = MsFlags::MS_REMOUNT | MsFlags::MS_BIND | MsFlags::MS_RDONLY;
        if let Err(e) = mount(None::<&str>, target, None::<&str>, flags, None::<&str>) {
            detach(target);
            return Err(format!("make {} read-only: {e}", target.display()));
        }
    }
    Ok(())
}

/// Removes `dir` and everything in it, after detaching each mount at or
/// under it. A mount that stays is an error, and nothing is removed.
pub fn remove_tree(dir: &Path) -> std::io::Result<()> {
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo")?;
    let mounts = mounts_under(&mountinfo, dir);
    if !mounts.is_empty() {
        for mount in &mounts {
            detach(mount);
        }
        let left = mounts_under(&std::fs::read_to_string("/proc/self/mountinfo")?, dir);
        if !left.is_empty() {
            return Err(std::io::Error::other(format!(
                "mounts stay under {}: {left:?}",
                dir.display()
            )));
        }
    }
    std::fs::remove_dir_all(dir)
}

fn detach(target: &Path) {
    if let Err(e) = umount2(target, MntFlags::MNT_DETACH) {
        log::warn!("unmount {}: {e}", target.display());
    }
}

/// The mount points in `mountinfo` (`/proc/self/mountinfo`) at or under
/// `dir`, the deepest first.
fn mounts_under(mountinfo: &str, dir: &Path) -> Vec<PathBuf> {
    let mut mounts: Vec<PathBuf> = mountinfo
        .lines()
        .filter_map(|line| line.split(' ').nth(4))
        .map(|point| PathBuf::from(unescape(point)))
        .filter(|point| point.starts_with(dir))
        .collect();
    mounts.sort_by_key(|point| std::cmp::Reverse(point.components().count()));
    mounts.dedup();
    mounts
}

/// Undoes mountinfo's octal escapes of space, tab, newline and backslash.
fn unescape(field: &str) -> String {
    let mut out = String::with_capacity(field.len());
    let mut rest = field;
    while let Some(at) = rest.find('\\') {
        out.push_str(&rest[..at]);
        let code = rest.get(at + 1..at + 4);
        match code.and_then(|c| u8::from_str_radix(c, 8).ok()) {
            Some(byte) => {
                out.push(char::from(byte));
                rest = &rest[at + 4..];
            }
            None => {
                out.push('\\');
                rest = &rest[at + 1..];
            }
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const MOUNTINFO: &str = "\
22 1 252:0 / / rw,relatime shared:1 - ext4 /dev/vda rw
40 22 252:16 /smolvm-pods /var/lib/containerd-shim-smolvm-pods rw shared:5 - ext4 /dev/vdb rw
51 40 252:16 /docker/volumes/data/_data /var/lib/containerd-shim-smolvm-pods/c1/podshare/c1/mounts/0 rw shared:6 - ext4 /dev/vdb rw
52 40 0:30 / /var/lib/containerd-shim-smolvm-pods/c1/podshare/c1/mounts/0/sub rw - tmpfs tmpfs rw
53 40 252:16 /x /var/lib/containerd-shim-smolvm-pods/c1/podshare/c1/mounts/1\\040b rw - ext4 /dev/vdb rw
54 40 252:16 /y /var/lib/containerd-shim-smolvm-pods/c10/podshare/c10/mounts/0 rw - ext4 /dev/vdb rw
";

    #[test]
    fn the_mounts_under_a_directory_come_deepest_first() {
        let share = "/var/lib/containerd-shim-smolvm-pods/c1/podshare/c1/mounts";
        let mounts = mounts_under(
            MOUNTINFO,
            Path::new("/var/lib/containerd-shim-smolvm-pods/c1"),
        );
        assert_eq!(
            mounts,
            [
                PathBuf::from(format!("{share}/0/sub")),
                PathBuf::from(format!("{share}/0")),
                PathBuf::from(format!("{share}/1 b")),
            ]
        );
    }

    #[test]
    fn a_directory_without_mounts_has_none() {
        let dir = Path::new("/var/lib/containerd-shim-smolvm-pods/c2");
        assert!(mounts_under(MOUNTINFO, dir).is_empty());
    }

    #[test]
    fn mountinfo_escapes_are_undone() {
        assert_eq!(unescape("/a\\040b\\011c\\134d"), "/a b\tc\\d");
        assert_eq!(unescape("/plain"), "/plain");
        assert_eq!(unescape("/odd\\x"), "/odd\\x");
    }

    /// Writes through a bind reach its source, a read-only bind refuses
    /// them, and removing the tree that holds the binds keeps the sources.
    #[test]
    #[ignore = "needs root: run it in the node, in a mount namespace of its own"]
    fn binds_in_place_reach_their_sources_and_their_removal_keeps_them() {
        let root = tempfile::tempdir().unwrap();
        let volume = root.path().join("volume");
        let file = root.path().join("file");
        std::fs::create_dir(&volume).unwrap();
        std::fs::write(volume.join("old"), "kept").unwrap();
        std::fs::write(&file, "one").unwrap();
        let share = root.path().join("share/c1");
        let mounts = share.join("mounts");

        bind_in_place(&volume, &mounts.join("0"), false).unwrap();
        bind_in_place(&volume, &mounts.join("1"), true).unwrap();
        bind_in_place(&file, &mounts.join("2"), false).unwrap();
        std::fs::write(mounts.join("0/new"), "written").unwrap();
        std::fs::write(mounts.join("2"), "two").unwrap();
        let refused = std::fs::write(mounts.join("1/other"), "x").unwrap_err();
        remove_tree(&share).unwrap();

        assert_eq!(refused.raw_os_error(), Some(libc::EROFS));
        assert!(!share.exists());
        assert_eq!(std::fs::read_to_string(volume.join("old")).unwrap(), "kept");
        assert_eq!(
            std::fs::read_to_string(volume.join("new")).unwrap(),
            "written"
        );
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "two");
        assert!(!volume.join("other").exists());
    }

    #[test]
    fn a_tree_without_mounts_is_removed() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("c1");
        std::fs::create_dir_all(dir.join("rootfs/etc")).unwrap();
        std::fs::write(dir.join("rootfs/etc/hosts"), "127.0.0.1 localhost\n").unwrap();
        remove_tree(&dir).unwrap();
        assert!(!dir.exists());
    }
}
