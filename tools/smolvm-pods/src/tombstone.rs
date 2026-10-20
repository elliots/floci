//! Tombstones: sandboxes that containerd ended while the node service could
//! not delete them. The shim's `delete` action writes one when the node
//! service does not answer, and the node service writes one before it deletes
//! a sandbox. The node service deletes the sandbox of each tombstone it finds
//! when it starts, then removes the tombstone.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::files;
use crate::proto::valid_id;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {
    pub pod: String,
    #[serde(default)]
    pub netns: Option<String>,
}

/// The tombstones' directory in the node service's state directory.
pub fn dir(state_dir: &Path) -> PathBuf {
    state_dir.join("tombstones")
}

/// Write a tombstone, durably, through a temporary file whose name starts
/// with a dot.
pub fn write(dir: &Path, tombstone: &Tombstone) -> std::io::Result<()> {
    if !valid_id(&tombstone.pod) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{:?} is not a sandbox id", tombstone.pod),
        ));
    }
    std::fs::create_dir_all(dir)?;
    let path = dir.join(&tombstone.pod);
    let temp = dir.join(format!(".{}.{}", tombstone.pod, std::process::id()));
    let body = serde_json::to_vec(tombstone).map_err(std::io::Error::other)?;
    files::replace(&path, &temp, 0o666, &body)
}

/// Remove the tombstone of sandbox `pod`, if there is one.
pub fn remove(dir: &Path, pod: &str) -> std::io::Result<()> {
    if !valid_id(pod) {
        return Ok(());
    }
    match std::fs::remove_file(dir.join(pod)) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

/// Every tombstone in `dir`, and the names of the files that are not one.
pub fn list(dir: &Path) -> (Vec<Tombstone>, Vec<PathBuf>) {
    let (mut tombstones, mut bad) = (Vec::new(), Vec::new());
    let Ok(entries) = std::fs::read_dir(dir) else {
        return (tombstones, bad);
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            bad.push(entry.path());
            continue;
        };
        // A temporary file of a write that did not finish.
        if name.starts_with('.') {
            bad.push(entry.path());
            continue;
        }
        let read = std::fs::read(entry.path())
            .ok()
            .and_then(|body| serde_json::from_slice::<Tombstone>(&body).ok())
            .filter(|t| t.pod == name);
        match read {
            Some(tombstone) => tombstones.push(tombstone),
            None => bad.push(entry.path()),
        }
    }
    tombstones.sort_by(|a, b| a.pod.cmp(&b.pod));
    (tombstones, bad)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tombstones_round_trip() {
        let state = tempfile::tempdir().unwrap();
        let dir = dir(state.path());
        let a = Tombstone {
            pod: "a1".into(),
            netns: Some("/var/run/netns/cni-1".into()),
        };
        let b = Tombstone {
            pod: "b2".into(),
            netns: None,
        };
        write(&dir, &a).unwrap();
        write(&dir, &b).unwrap();
        write(&dir, &b).unwrap();
        std::fs::write(dir.join(".b2.77"), b"{").unwrap();
        let (found, bad) = list(&dir);
        assert_eq!(found, [a.clone(), b]);
        assert_eq!(bad, [dir.join(".b2.77")]);
        remove(&dir, "b2").unwrap();
        remove(&dir, "b2").unwrap();
        assert_eq!(list(&dir).0, [a]);
    }

    #[test]
    fn a_tombstone_names_a_sandbox_id() {
        let state = tempfile::tempdir().unwrap();
        let bad = Tombstone {
            pod: "../x".into(),
            netns: None,
        };
        assert!(write(&dir(state.path()), &bad).is_err());
    }
}
