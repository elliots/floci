//! The durable state directory:
//!
//! ```text
//! <dir>/node.lock               flock: one node service at a time
//! <dir>/pods/<sandbox-id>.json  one record per pod, replaced atomically
//! <dir>/events.jsonl            the event log, and events.jsonl.1 after a rotation
//! <dir>/faults-fired            kill points that fired (see `fault`)
//! ```

use std::fs::File;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use log::warn;

use super::events::{Event, EventLog, EventPage, EventQuery};
use super::record::PodRecord;
use crate::files::{self, sync_dir, LockError};
use crate::proto::valid_id;
use crate::sync::lock;

const LOCK_FILE: &str = "node.lock";
const PODS_DIR: &str = "pods";

pub(super) struct Store {
    pods: PathBuf,
    lock: Mutex<Option<File>>,
    events: Mutex<EventLog>,
}

impl Store {
    /// Creates `dir` if needed and locks it.
    pub fn open(dir: &Path, events_max_bytes: u64) -> Result<Store, String> {
        std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            .map_err(|e| format!("chmod {}: {e}", dir.display()))?;
        let lock = lock_dir(dir)?;
        let pods = dir.join(PODS_DIR);
        std::fs::create_dir_all(&pods).map_err(|e| format!("create {}: {e}", pods.display()))?;
        remove_temporaries(&pods);
        let events = EventLog::open(dir, events_max_bytes)?;
        Ok(Store {
            pods,
            lock: Mutex::new(Some(lock)),
            events: Mutex::new(events),
        })
    }

    /// Releases the lock on the directory. The store must not be used after.
    pub fn unlock(&self) {
        lock(&self.lock).take();
    }

    /// Every record. A file that does not parse is skipped and left in place.
    pub fn load(&self) -> Result<Vec<PodRecord>, String> {
        let entries = std::fs::read_dir(&self.pods)
            .map_err(|e| format!("read {}: {e}", self.pods.display()))?;
        let mut records = Vec::new();
        for entry in entries {
            let path = entry.map_err(|e| e.to_string())?.path();
            let Some(id) = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_suffix(".json"))
            else {
                continue;
            };
            let parsed = std::fs::read(&path)
                .map_err(|e| e.to_string())
                .and_then(|data| {
                    serde_json::from_slice::<PodRecord>(&data).map_err(|e| e.to_string())
                });
            match parsed {
                Ok(record) if record.identity.sandbox_id == id => records.push(record),
                Ok(record) => warn!(
                    "{} names sandbox {}, so it is skipped",
                    path.display(),
                    record.identity.sandbox_id
                ),
                Err(e) => warn!("skipped the pod record {}: {e}", path.display()),
            }
        }
        records.sort_by(|a, b| a.identity.sandbox_id.cmp(&b.identity.sandbox_id));
        Ok(records)
    }

    pub fn save(&self, record: &PodRecord) -> Result<(), String> {
        let path = self.path(&record.identity.sandbox_id)?;
        let mut data = serde_json::to_vec_pretty(record).map_err(|e| e.to_string())?;
        data.push(b'\n');
        write_atomic(&path, &data).map_err(|e| format!("write {}: {e}", path.display()))
    }

    pub fn remove(&self, sandbox_id: &str) -> Result<(), String> {
        let path = self.path(sandbox_id)?;
        if let Err(e) = std::fs::remove_file(&path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(format!("remove {}: {e}", path.display()));
            }
        }
        sync_dir(&self.pods).map_err(|e| format!("sync {}: {e}", self.pods.display()))
    }

    pub fn append(&self, event: &mut Event) -> Result<(), String> {
        lock(&self.events).append(event)
    }

    pub fn events(&self, query: &EventQuery) -> Result<EventPage, String> {
        lock(&self.events).read(query)
    }

    fn path(&self, sandbox_id: &str) -> Result<PathBuf, String> {
        if !valid_id(sandbox_id) {
            return Err(format!("invalid sandbox id {sandbox_id:?}"));
        }
        Ok(self.pods.join(format!("{sandbox_id}.json")))
    }
}

fn lock_dir(dir: &Path) -> Result<File, String> {
    let path = dir.join(LOCK_FILE);
    files::lock(&path, 0o600, false).map_err(|e| match e {
        LockError::Lock(e) if e.kind() == std::io::ErrorKind::WouldBlock => format!(
            "another node service uses {}: {} is locked",
            dir.display(),
            path.display()
        ),
        e => e.message(&path),
    })
}

/// Replaces the file at `path` with `data`, durably, through `PATH.tmp`.
fn write_atomic(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    files::replace(path, Path::new(&temporary), 0o600, data)
}

/// Removes the temporary files of writes that a crash interrupted.
fn remove_temporaries(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for path in entries.flatten().map(|e| e.path()) {
        if path.extension().is_some_and(|x| x == "tmp") {
            if let Err(e) = std::fs::remove_file(&path) {
                warn!("remove {}: {e}", path.display());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::record::PodIdentity;

    fn record(id: &str) -> PodRecord {
        let identity = PodIdentity {
            sandbox_id: id.into(),
            machine: id.into(),
            ..Default::default()
        };
        PodRecord::new(identity, "boot".into(), 1)
    }

    #[test]
    fn records_are_replaced_atomically_and_load_back() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), 1 << 20).unwrap();
        let mut rec = record("pod");
        store.save(&rec).unwrap();
        rec.generation = 2;
        store.save(&rec).unwrap();
        store.save(&record("other")).unwrap();
        let names: Vec<_> = std::fs::read_dir(dir.path().join(PODS_DIR))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert!(names.iter().all(|n| n.ends_with(".json")), "{names:?}");
        let loaded = store.load().unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[1], rec);
        store.remove("pod").unwrap();
        store.remove("pod").unwrap();
        assert_eq!(store.load().unwrap().len(), 1);
        let mode = std::fs::metadata(dir.path()).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700);
    }

    #[test]
    fn one_store_at_a_time_locks_the_directory() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), 1 << 20).unwrap();
        let error = Store::open(dir.path(), 1 << 20).err().unwrap();
        assert!(error.contains("another node service"), "{error}");
        store.unlock();
        Store::open(dir.path(), 1 << 20).unwrap();
    }

    #[test]
    fn leftovers_and_strangers_are_not_loaded() {
        let dir = tempfile::tempdir().unwrap();
        let pods = dir.path().join(PODS_DIR);
        std::fs::create_dir_all(&pods).unwrap();
        std::fs::write(pods.join("pod.json.tmp"), "{").unwrap();
        std::fs::write(pods.join("bad.json"), "{").unwrap();
        let other = serde_json::to_vec(&record("other")).unwrap();
        std::fs::write(pods.join("renamed.json"), other).unwrap();
        let store = Store::open(dir.path(), 1 << 20).unwrap();
        assert!(store.load().unwrap().is_empty());
        assert!(!pods.join("pod.json.tmp").exists());
        assert!(pods.join("bad.json").exists(), "a bad record is kept");
    }

    #[test]
    fn ids_must_be_one_path_component() {
        for id in ["pod", "a.b-c_d", "0123456789abcdef"] {
            assert!(valid_id(id), "{id}");
        }
        for id in [
            "",
            ".",
            "..",
            "../x",
            "a/b",
            "-a",
            ".hidden",
            &"a".repeat(201),
        ] {
            assert!(!valid_id(id), "{id}");
        }
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), 1 << 20).unwrap();
        assert!(store.save(&record("../escape")).is_err());
    }
}
