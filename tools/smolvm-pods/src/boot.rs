//! The pod VM's process entry point.
//!
//! smolvm runs this binary as `_boot-vm <config>` for every boot and every
//! resume of a pod VM. A VM with Rosetta runs amd64 code, which needs the x86
//! memory ordering (TSO) that Apple silicon can give each thread. The vCPU
//! threads inherit it from this thread, so it is turned on here, before smolvm
//! starts any of them. It is not part of the saved VM state, and execve turns
//! it off, so every boot and resume sets it again, and records that it did, so
//! that the node service can check that the VM process it started has TSO. The
//! process of a pod VM then enters the pod's network namespace (see
//! [`crate::net`]).

use std::path::Path;
use std::path::PathBuf;

use smolvm::agent::boot_config::BootConfig;

/// `prctl` options from the TSO patches of Asahi Linux, which the libkrunfw
/// kernel of a smolvm node carries.
const PR_GET_MEM_MODEL: libc::c_int = 0x6d4d_444c;
const PR_SET_MEM_MODEL: libc::c_int = 0x4d4d_444c;
const PR_SET_MEM_MODEL_TSO: libc::c_ulong = 1;

/// In the VM's directory, beside the boot config: the pid and start time of
/// the last VM process that turned TSO on.
const TSO_MARKER: &str = "smolvm-pods-tso";

pub fn run(config: PathBuf) {
    if let Err(e) = prepare(&config, set_tso) {
        eprintln!("boot failed: {e}");
        std::process::exit(1);
    }
    if let Err(e) = crate::net::prepare_boot(&config) {
        eprintln!("boot failed: {e}");
        std::process::exit(1);
    }
    if let Err(e) = smolvm::internal_boot::run(config) {
        eprintln!("boot failed: {e}");
        std::process::exit(1);
    }
}

/// Turn TSO on with `set_tso` if the VM needs it. A failure is also written to
/// the VM's startup error log, which smolvm reports when a boot fails.
fn prepare(config: &Path, set_tso: impl FnOnce() -> Result<(), String>) -> Result<(), String> {
    let raw = std::fs::read(config).map_err(|e| format!("read {}: {e}", config.display()))?;
    let boot: BootConfig =
        serde_json::from_slice(&raw).map_err(|e| format!("parse {}: {e}", config.display()))?;
    if !crate::rosetta::is_shared(&boot.mounts) {
        return Ok(());
    }
    set_tso().and_then(|()| mark_tso(config)).map_err(|e| {
        let message = format!("the VM has Rosetta and needs TSO, but {e}");
        let _ = std::fs::write(&boot.startup_error_log, format!("error: {message}\n"));
        message
    })
}

/// Record that this process turned TSO on, for [`has_tso`].
fn mark_tso(config: &Path) -> Result<(), String> {
    let identity = process_identity(std::process::id() as i32)
        .ok_or("the start time of the VM process is unknown")?;
    let marker = config
        .parent()
        .ok_or_else(|| format!("{} has no directory", config.display()))?
        .join(TSO_MARKER);
    std::fs::write(&marker, identity).map_err(|e| format!("write {}: {e}", marker.display()))
}

/// Whether the VM process `pid` of the VM in `vm_dir` turned TSO on, which a
/// VM process started some other way, such as by the smolvm CLI, does not.
pub fn has_tso(vm_dir: &Path, pid: i32) -> bool {
    let marker = std::fs::read_to_string(vm_dir.join(TSO_MARKER));
    marker.is_ok_and(|marker| Some(marker) == process_identity(pid))
}

/// Whether a live VM process of the VM in `vm_dir` turned TSO on: the one
/// that the marker names, if it still runs.
pub fn has_live_tso(vm_dir: &Path) -> bool {
    let Ok(marker) = std::fs::read_to_string(vm_dir.join(TSO_MARKER)) else {
        return false;
    };
    let pid = marker
        .split_whitespace()
        .next()
        .and_then(|pid| pid.parse().ok());
    pid.is_some_and(|pid| Some(marker) == process_identity(pid))
}

/// A pid and the start time of its process, which tell it from a later
/// process with the same pid.
fn process_identity(pid: i32) -> Option<String> {
    smolvm::process::process_start_time(pid).map(|start| format!("{pid} {start}"))
}

fn set_tso() -> Result<(), String> {
    if prctl(PR_SET_MEM_MODEL, PR_SET_MEM_MODEL_TSO) != 0 {
        let e = std::io::Error::last_os_error();
        return Err(format!("prctl(PR_SET_MEM_MODEL, TSO) failed: {e}"));
    }
    let model = prctl(PR_GET_MEM_MODEL, 0);
    if model as libc::c_ulong != PR_SET_MEM_MODEL_TSO {
        return Err(format!("the memory model is {model} after setting TSO"));
    }
    Ok(())
}

/// `prctl` with one argument. The kernel rejects nonzero unused arguments.
fn prctl(option: libc::c_int, arg: libc::c_ulong) -> libc::c_int {
    const UNUSED: libc::c_ulong = 0;
    // SAFETY: prctl with integer arguments only.
    unsafe { libc::prctl(option, arg, UNUSED, UNUSED, UNUSED) }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::Cell;

    /// A boot config as smolvm writes it, with the given guest mounts.
    fn write_config(dir: &Path, targets: &[&str]) -> PathBuf {
        let mounts: Vec<_> = targets
            .iter()
            .map(|t| serde_json::json!({"source": dir, "target": t, "read_only": true}))
            .collect();
        let config = serde_json::json!({
            "rootfs_path": dir.join("rootfs"),
            "storage_disk_path": dir.join("storage.raw"),
            "overlay_disk_path": dir.join("overlay.raw"),
            "vsock_socket": dir.join("agent.sock"),
            "startup_error_log": dir.join("startup-error.log"),
            "storage_size_gb": 1,
            "overlay_size_gb": 1,
            "mounts": mounts,
            "ports": [],
            "resources": {"cpus": 1, "memory_mib": 512, "network": true},
        });
        let path = dir.join("boot-config.json");
        std::fs::write(&path, config.to_string()).unwrap();
        path
    }

    #[test]
    fn a_vm_with_rosetta_turns_tso_on_before_booting() {
        let dir = tempfile::tempdir().unwrap();
        let config = write_config(dir.path(), &["/podshare", "/mnt/rosetta"]);
        let called = Cell::new(false);
        prepare(&config, || {
            called.set(true);
            Ok(())
        })
        .unwrap();
        assert!(called.get());
        assert!(config.exists(), "internal_boot::run still needs the config");
        // This test process is the VM process here.
        assert!(has_tso(dir.path(), std::process::id() as i32));
    }

    #[test]
    fn a_vm_without_rosetta_leaves_tso_off() {
        let dir = tempfile::tempdir().unwrap();
        let config = write_config(dir.path(), &["/podshare"]);
        prepare(&config, || panic!("TSO set for a VM without Rosetta")).unwrap();
        assert!(!has_tso(dir.path(), std::process::id() as i32));
    }

    /// The marker names one process: another VM process with the marker of an
    /// earlier boot, such as one the smolvm CLI started, has no TSO.
    #[test]
    fn the_tso_marker_names_the_process_that_set_it() {
        let dir = tempfile::tempdir().unwrap();
        let config = write_config(dir.path(), &["/mnt/rosetta"]);
        prepare(&config, || Ok(())).unwrap();
        let other = std::process::Command::new("sleep")
            .arg("5")
            .spawn()
            .unwrap();
        let other_pid = other.id() as i32;
        assert!(!has_tso(dir.path(), other_pid));
        std::fs::write(dir.path().join(TSO_MARKER), format!("{other_pid} 0")).unwrap();
        assert!(!has_tso(dir.path(), other_pid), "a reused pid has TSO");
        let mut other = other;
        other.kill().unwrap();
        other.wait().unwrap();
    }

    #[test]
    fn a_tso_failure_fails_the_boot_and_says_why() {
        let dir = tempfile::tempdir().unwrap();
        let config = write_config(dir.path(), &["/podshare", "/mnt/rosetta"]);
        let err = prepare(&config, || Err("prctl failed: EINVAL".into())).unwrap_err();
        assert!(err.contains("needs TSO"), "{err}");
        let log = std::fs::read_to_string(dir.path().join("startup-error.log")).unwrap();
        assert!(log.starts_with("error: "), "{log}");
        assert!(log.contains("EINVAL"), "{log}");
        assert!(!has_tso(dir.path(), std::process::id() as i32));
    }

    #[test]
    fn an_unreadable_config_fails_the_boot() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("boot-config.json");
        assert!(prepare(&missing, || Ok(())).is_err());
        std::fs::write(&missing, "{}").unwrap();
        assert!(prepare(&missing, || Ok(())).is_err());
    }

    /// The share the engine backend adds for a Rosetta pod is what this entry
    /// point finds in the boot config.
    #[test]
    fn the_rosetta_share_is_the_tso_request() {
        let node = tempfile::tempdir().unwrap();
        std::fs::write(node.path().join("rosetta"), b"").unwrap();
        let share = smolvm::agent::HostMount::new(node.path(), "/podshare", false).unwrap();
        let rosetta = crate::rosetta::share_from(node.path()).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let config = write_config(dir.path(), &[]);
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&config).unwrap()).unwrap();
        value["mounts"] = serde_json::to_value([share, rosetta]).unwrap();
        std::fs::write(&config, value.to_string()).unwrap();

        let called = Cell::new(false);
        prepare(&config, || {
            called.set(true);
            Ok(())
        })
        .unwrap();
        assert!(called.get());
    }

    /// Runs the real prctl: a kernel without the TSO patches must refuse it,
    /// which fails a Rosetta VM's boot instead of running it without TSO.
    #[test]
    fn set_tso_either_turns_tso_on_or_fails() {
        std::thread::spawn(|| match set_tso() {
            Ok(()) => assert_eq!(prctl(PR_GET_MEM_MODEL, 0), 1),
            Err(e) => assert!(e.contains("PR_SET_MEM_MODEL"), "{e}"),
        })
        .join()
        .unwrap();
    }
}
