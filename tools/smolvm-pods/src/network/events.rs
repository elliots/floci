//! Network events: blocked attempts and relayed connections, counted per pod,
//! kind, destination, port and protocol over a minute, and kept in
//! `.node/network-events.jsonl` (rotated at 8 MiB) with a sequence number.

use std::collections::{BTreeMap, VecDeque};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::broadcast;

use crate::sync::lock;

/// How long an event counts attempts before it is written.
pub const WINDOW: Duration = Duration::from_secs(60);

const ROTATE_AT: u64 = 8 << 20;

/// The events that a page or a follower can still read.
const KEPT: usize = 2000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// A connection or datagram that the sinkhole took.
    Blocked,
    /// A connection that the relay passed to a route's backend.
    Relayed,
    /// A routed connection that the relay could not serve.
    RelayFailed,
}

impl Kind {
    pub const ALL: [Kind; 3] = [Kind::Blocked, Kind::Relayed, Kind::RelayFailed];

    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Blocked => "blocked",
            Kind::Relayed => "relayed",
            Kind::RelayFailed => "relay_failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key {
    pub kind: Kind,
    /// `NAMESPACE/NAME` of the pod, or its address when no record names it.
    pub pod: String,
    pub destination: String,
    pub port: u16,
    pub protocol: &'static str,
}

/// What one attempt adds to its event.
#[derive(Debug, Clone, Default)]
pub struct Detail {
    /// The TLS server name or HTTP `Host` that the attempt sent.
    pub name: Option<String>,
    pub route: Option<String>,
    pub error: Option<String>,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Event {
    pub seq: u64,
    pub kind: Kind,
    pub pod: String,
    pub destination: String,
    pub port: u16,
    pub protocol: &'static str,
    pub count: u64,
    pub first: String,
    pub last: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub names: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "is_zero")]
    pub bytes: u64,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

struct Open {
    first: SystemTime,
    last: SystemTime,
    count: u64,
    names: Vec<String>,
    route: Option<String>,
    error: Option<String>,
    bytes: u64,
}

struct Inner {
    open: BTreeMap<Key, Open>,
    kept: VecDeque<Event>,
    next_seq: u64,
    counters: BTreeMap<Kind, u64>,
}

pub struct Log {
    path: PathBuf,
    inner: Mutex<Inner>,
    sender: broadcast::Sender<Event>,
}

fn rfc3339(t: SystemTime) -> String {
    let ms = t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
    crate::logging::format_utc_ms(ms)
}

impl Log {
    pub fn open(path: &Path) -> Log {
        let next_seq = last_seq(path).map_or(1, |s| s + 1);
        let (sender, _) = broadcast::channel(256);
        Log {
            path: path.to_path_buf(),
            inner: Mutex::new(Inner {
                open: BTreeMap::new(),
                kept: VecDeque::new(),
                next_seq,
                counters: BTreeMap::new(),
            }),
            sender,
        }
    }

    /// Counts one attempt.
    pub fn record(&self, key: Key, detail: Detail) {
        let now = SystemTime::now();
        let mut inner = lock(&self.inner);
        *inner.counters.entry(key.kind).or_default() += 1;
        let open = inner.open.entry(key).or_insert_with(|| Open {
            first: now,
            last: now,
            count: 0,
            names: Vec::new(),
            route: None,
            error: None,
            bytes: 0,
        });
        open.last = now;
        open.count += 1;
        open.bytes += detail.bytes;
        if let Some(name) = detail.name {
            if open.names.len() < 8 && !open.names.contains(&name) {
                open.names.push(name);
            }
        }
        if detail.route.is_some() {
            open.route = detail.route;
        }
        if detail.error.is_some() {
            open.error = detail.error;
        }
    }

    /// Writes every event whose window ended by `now`.
    pub fn flush(&self, now: SystemTime) {
        let mut inner = lock(&self.inner);
        let due: Vec<Key> = inner
            .open
            .iter()
            .filter(|(_, o)| now.duration_since(o.first).unwrap_or_default() >= WINDOW)
            .map(|(k, _)| k.clone())
            .collect();
        for key in due {
            let Some(open) = inner.open.remove(&key) else {
                continue;
            };
            let seq = inner.next_seq;
            inner.next_seq += 1;
            let event = Event {
                seq,
                kind: key.kind,
                pod: key.pod,
                destination: key.destination,
                port: key.port,
                protocol: key.protocol,
                count: open.count,
                first: rfc3339(open.first),
                last: rfc3339(open.last),
                names: open.names,
                route: open.route,
                error: open.error,
                bytes: open.bytes,
            };
            if let Err(e) = self.append(&event) {
                log::warn!("write the network event {seq}: {e}");
            }
            if event.kind == Kind::Blocked {
                log::info!(
                    "blocked {} {} to {}:{} ({} times)",
                    event.pod,
                    event.protocol,
                    event.destination,
                    event.port,
                    event.count
                );
            }
            inner.kept.push_back(event.clone());
            if inner.kept.len() > KEPT {
                inner.kept.pop_front();
            }
            let _ = self.sender.send(event);
        }
    }

    fn append(&self, event: &Event) -> Result<(), String> {
        if std::fs::metadata(&self.path).is_ok_and(|m| m.len() >= ROTATE_AT) {
            let old = self.path.with_extension("jsonl.1");
            std::fs::rename(&self.path, &old)
                .map_err(|e| format!("rotate {}: {e}", self.path.display()))?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| format!("open {}: {e}", self.path.display()))?;
        let mut line = serde_json::to_vec(event).map_err(|e| e.to_string())?;
        line.push(b'\n');
        file.write_all(&line)
            .map_err(|e| format!("append to {}: {e}", self.path.display()))
    }

    /// The kept events after `cursor`, at most `limit`, and the cursor to ask
    /// from next.
    pub fn page(&self, cursor: u64, limit: usize) -> (Vec<Event>, u64) {
        let inner = lock(&self.inner);
        let events: Vec<Event> = inner
            .kept
            .iter()
            .filter(|e| e.seq > cursor)
            .take(limit)
            .cloned()
            .collect();
        let next = events
            .last()
            .map_or(cursor.max(inner.next_seq - 1), |e| e.seq);
        (events, next)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.sender.subscribe()
    }

    /// Attempts counted since the node service started, by kind.
    pub fn counters(&self) -> BTreeMap<Kind, u64> {
        lock(&self.inner).counters.clone()
    }
}

/// The sequence number of the last event in the log at `path`.
fn last_seq(path: &Path) -> Option<u64> {
    let text = std::fs::read_to_string(path).ok()?;
    let line = text.lines().rev().find(|l| !l.trim().is_empty())?;
    serde_json::from_str::<serde_json::Value>(line)
        .ok()?
        .get("seq")?
        .as_u64()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(pod: &str, port: u16) -> Key {
        Key {
            kind: Kind::Blocked,
            pod: pod.into(),
            destination: "1.1.1.1".into(),
            port,
            protocol: "tcp",
        }
    }

    #[test]
    fn attempts_in_a_window_make_one_event() {
        let dir = tempfile::tempdir().unwrap();
        let log = Log::open(&dir.path().join("network-events.jsonl"));
        for _ in 0..3 {
            log.record(
                key("default/orders", 443),
                Detail {
                    name: Some("api.example.com".into()),
                    ..Detail::default()
                },
            );
        }
        log.record(key("default/orders", 80), Detail::default());
        log.flush(SystemTime::now());
        assert_eq!(log.page(0, 10).0.len(), 0, "the window is still open");
        log.flush(SystemTime::now() + WINDOW);
        let (events, cursor) = log.page(0, 10);
        assert_eq!(events.len(), 2);
        let tls = events.iter().find(|e| e.port == 443).unwrap();
        assert_eq!(
            (tls.count, tls.names.as_slice()),
            (3, &["api.example.com".to_string()][..])
        );
        assert_eq!(cursor, 2);
        assert_eq!(log.page(cursor, 10).0.len(), 0);
        assert_eq!(log.counters()[&Kind::Blocked], 4);

        let reopened = Log::open(&dir.path().join("network-events.jsonl"));
        reopened.record(key("default/orders", 22), Detail::default());
        reopened.flush(SystemTime::now() + WINDOW);
        assert_eq!(
            reopened.page(0, 10).0[0].seq,
            3,
            "sequence numbers go on after a restart"
        );
    }
}
