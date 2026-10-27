//! `events.jsonl`: one line per state change, synced, rotated at a size
//! limit. Each event has a sequence number, which is the cursor of
//! [`EventQuery`]: it keeps counting across rotations and restarts.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use log::debug;
use serde::{Deserialize, Serialize};

use super::state::{PodState, Reason};
use super::vm::{ClockStep, Save};
use crate::files::sync_dir;

const EVENTS_FILE: &str = "events.jsonl";
const ROTATED_FILE: &str = "events.jsonl.1";

/// The duration of one step of a transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepTime {
    pub step: String,
    pub ms: f64,
}

/// Milliseconds with microsecond precision.
pub fn ms(d: Duration) -> f64 {
    d.as_micros() as f64 / 1000.0
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct Steps(pub Vec<StepTime>);

impl Steps {
    pub fn push(&mut self, step: &str, took: Duration) {
        self.0.push(StepTime {
            step: step.to_string(),
            ms: ms(took),
        });
    }

    pub fn extend(&mut self, other: Steps) {
        self.0.extend(other.0);
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// The cursor: 1 for the first event of the node.
    pub seq: u64,
    pub time_ms: u64,
    pub sandbox_id: String,
    pub incarnation: String,
    /// The state before, or none for a new record.
    pub from: Option<PodState>,
    pub state: PodState,
    pub generation: u64,
    pub reason: Reason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<StepTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_bytes: Option<u64>,
    /// On `pausing` to `asleep`: how the pause saved the guest's RAM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub save: Option<Save>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clock: Option<ClockStep>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Something the pod runs without, such as Rosetta when the node's TSO
    /// canary failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
    /// On `pausing` to `awake`: a wake cancelled the pause.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub cancelled: bool,
    /// On the way back to `awake`: the time from the capture of the first
    /// frame held for the pod to the release of the held frames.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_to_release_ms: Option<f64>,
    /// On `asleep` to `asleep`: a wake waits for admission, because of this,
    /// such as `awake pods 2/2`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting: Option<String>,
    /// The node service finished or undid this transition after a restart.
    #[serde(default)]
    pub recovered: bool,
}

/// What an event means for its pod, for the metrics and the Kubernetes
/// Events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Change {
    /// `creating` to `awake`.
    Created,
    /// `waking` to `awake`.
    Woke,
    /// `pausing` to `awake`: a wake cancelled the pause.
    PauseCancelled,
    /// `pausing` to `awake` with an error: the pause failed or was refused.
    SleepFailed,
    /// `pausing` to `asleep`.
    Slept,
    /// `asleep` to `asleep` with `waiting`: a wake waits for admission.
    WakeWaiting,
    /// `asleep` to `asleep` with an error: admission refused a wake.
    WakeRefused,
    /// `waking` to `asleep`: the resume failed with the checkpoint intact.
    WakeFailed,
    /// To `failed`.
    Failed,
    /// Anything else, such as a wake starting or a deleted record.
    Other,
}

impl Event {
    pub fn change(&self) -> Change {
        use PodState::*;
        match (self.from, self.state) {
            (_, Failed) if self.from != Some(Failed) => Change::Failed,
            (Some(Creating), Awake) => Change::Created,
            (Some(Waking), Awake) => Change::Woke,
            (Some(Pausing), Awake) if self.cancelled => Change::PauseCancelled,
            (Some(Pausing), Awake) => Change::SleepFailed,
            (Some(Pausing), Asleep) => Change::Slept,
            (Some(Asleep), Asleep) if self.waiting.is_some() => Change::WakeWaiting,
            (Some(Asleep), Asleep) if self.error.is_some() => Change::WakeRefused,
            (Some(Waking), Asleep) => Change::WakeFailed,
            _ => Change::Other,
        }
    }
}

/// Events after a cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventQuery {
    /// Return events with a larger sequence number; 0 for all.
    pub after: u64,
    pub sandbox_id: Option<String>,
    pub limit: usize,
}

#[cfg(test)]
impl EventQuery {
    pub fn after(after: u64) -> EventQuery {
        EventQuery {
            after,
            sandbox_id: None,
            limit: 100,
        }
    }

    pub fn pod(mut self, sandbox_id: &str) -> EventQuery {
        self.sandbox_id = Some(sandbox_id.to_string());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EventPage {
    pub events: Vec<Event>,
    /// The cursor for the next query: the last event returned, or the last
    /// event of the log if none matched.
    pub cursor: u64,
    /// Events after the query's cursor were rotated away.
    pub gap: bool,
}

pub(super) struct EventLog {
    current: PathBuf,
    rotated: PathBuf,
    file: File,
    size: u64,
    next_seq: u64,
    max_bytes: u64,
}

impl EventLog {
    /// Opens the log in `dir`. A crash during an append can leave a partial
    /// last line, which is cut off so that the next event starts a line.
    pub fn open(dir: &Path, max_bytes: u64) -> Result<EventLog, String> {
        let current = dir.join(EVENTS_FILE);
        let rotated = dir.join(ROTATED_FILE);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .mode(0o600)
            .open(&current)
            .map_err(|e| format!("open {}: {e}", current.display()))?;
        let mut data = Vec::new();
        (&file)
            .read_to_end(&mut data)
            .map_err(|e| format!("read {}: {e}", current.display()))?;
        let complete = data.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
        if complete < data.len() {
            file.set_len(complete as u64)
                .and_then(|()| file.sync_all())
                .map_err(|e| format!("cut the partial line of {}: {e}", current.display()))?;
            data.truncate(complete);
        }
        let rotated_data = std::fs::read(&rotated).unwrap_or_default();
        let last = parse(&rotated_data)
            .chain(parse(&data))
            .map(|e| e.seq)
            .max()
            .unwrap_or(0);
        Ok(EventLog {
            current,
            rotated,
            file,
            size: data.len() as u64,
            next_seq: last + 1,
            max_bytes,
        })
    }

    /// Appends `event`, setting its sequence number, and syncs the log. An
    /// event that cannot be written keeps its number, which the next event
    /// does not reuse: followers get it from the broadcast.
    pub fn append(&mut self, event: &mut Event) -> Result<(), String> {
        event.seq = self.next_seq;
        self.next_seq += 1;
        let mut line = serde_json::to_vec(event).map_err(|e| e.to_string())?;
        line.push(b'\n');
        self.file
            .write_all(&line)
            .and_then(|()| self.file.sync_data())
            .map_err(|e| format!("append to {}: {e}", self.current.display()))?;
        self.size += line.len() as u64;
        if self.size > self.max_bytes {
            self.rotate()?;
        }
        Ok(())
    }

    fn rotate(&mut self) -> Result<(), String> {
        std::fs::rename(&self.current, &self.rotated)
            .map_err(|e| format!("rotate {}: {e}", self.current.display()))?;
        self.file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .mode(0o600)
            .open(&self.current)
            .map_err(|e| format!("open {}: {e}", self.current.display()))?;
        self.size = 0;
        if let Some(dir) = self.current.parent() {
            sync_dir(dir).map_err(|e| format!("sync {}: {e}", dir.display()))?;
        }
        Ok(())
    }

    pub fn read(&self, query: &EventQuery) -> Result<EventPage, String> {
        let rotated = std::fs::read(&self.rotated).unwrap_or_default();
        let current = std::fs::read(&self.current)
            .map_err(|e| format!("read {}: {e}", self.current.display()))?;
        let mut events = Vec::new();
        let mut first = None;
        let mut last = 0;
        for event in parse(&rotated).chain(parse(&current)) {
            first.get_or_insert(event.seq);
            last = event.seq;
            if event.seq <= query.after || events.len() >= query.limit {
                continue;
            }
            let matches = query
                .sandbox_id
                .as_ref()
                .is_none_or(|id| *id == event.sandbox_id);
            if matches {
                events.push(event);
            }
        }
        let cursor = match events.last() {
            Some(e) if events.len() >= query.limit => e.seq,
            _ => last.max(query.after),
        };
        let gap = first.is_some_and(|first| first > query.after + 1);
        Ok(EventPage {
            events,
            cursor,
            gap,
        })
    }
}

/// The events of a log, skipping lines that do not parse.
fn parse(data: &[u8]) -> impl Iterator<Item = Event> + '_ {
    data.split(|&b| b == b'\n')
        .enumerate()
        .filter(|(_, line)| !line.is_empty())
        .filter_map(|(i, line)| match serde_json::from_slice(line) {
            Ok(event) => Some(event),
            Err(e) => {
                debug!("skipped line {} of the event log: {e}", i + 1);
                None
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(sandbox_id: &str, state: PodState) -> Event {
        Event {
            seq: 0,
            time_ms: 1,
            sandbox_id: sandbox_id.to_string(),
            incarnation: "inc".to_string(),
            from: None,
            state,
            generation: 1,
            reason: Reason::Api,
            source: None,
            steps: Vec::new(),
            duration_ms: None,
            checkpoint_bytes: None,
            save: None,
            clock: None,
            error: None,
            warning: None,
            cancelled: false,
            capture_to_release_ms: None,
            waiting: None,
            recovered: false,
        }
    }

    fn seqs(page: &EventPage) -> Vec<u64> {
        page.events.iter().map(|e| e.seq).collect()
    }

    #[test]
    fn the_cursor_counts_across_rotations_and_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = EventLog::open(dir.path(), 600).unwrap();
        for _ in 0..6 {
            log.append(&mut event("pod", PodState::Awake)).unwrap();
        }
        assert!(dir.path().join(ROTATED_FILE).exists(), "the log rotated");
        let page = log.read(&EventQuery::after(0)).unwrap();
        assert_eq!(seqs(&page), [1, 2, 3, 4, 5, 6]);
        assert!(!page.gap);
        assert_eq!(page.cursor, 6);
        drop(log);

        let mut log = EventLog::open(dir.path(), 600).unwrap();
        let mut next = event("pod", PodState::Asleep);
        log.append(&mut next).unwrap();
        assert_eq!(next.seq, 7);
        let page = log.read(&EventQuery::after(5)).unwrap();
        assert_eq!(seqs(&page), [6, 7]);
        assert!(!page.gap);
    }

    #[test]
    fn a_rotated_away_cursor_reports_a_gap() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = EventLog::open(dir.path(), 300).unwrap();
        for _ in 0..12 {
            log.append(&mut event("pod", PodState::Awake)).unwrap();
        }
        let page = log.read(&EventQuery::after(0)).unwrap();
        assert!(page.gap);
        assert_eq!(page.cursor, 12);
    }

    #[test]
    fn a_partial_last_line_is_cut_and_bad_lines_are_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(EVENTS_FILE);
        let mut good = event("pod", PodState::Awake);
        good.seq = 4;
        let data = format!(
            "not json\n{}\n{{\"seq\":5,\"sandbox_id\":\"po",
            serde_json::to_string(&good).unwrap()
        );
        std::fs::write(&path, data).unwrap();
        let mut log = EventLog::open(dir.path(), 1 << 20).unwrap();
        let mut next = event("pod", PodState::Asleep);
        log.append(&mut next).unwrap();
        assert_eq!(next.seq, 5);
        let page = log.read(&EventQuery::after(0)).unwrap();
        assert_eq!(seqs(&page), [4, 5]);
        assert!(std::fs::read_to_string(&path).unwrap().ends_with("}\n"));
    }

    #[test]
    fn queries_filter_by_pod_and_page_with_a_limit() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = EventLog::open(dir.path(), 1 << 20).unwrap();
        for id in ["a", "b", "a", "b", "a"] {
            log.append(&mut event(id, PodState::Awake)).unwrap();
        }
        let page = log.read(&EventQuery::after(0).pod("a")).unwrap();
        assert_eq!(seqs(&page), [1, 3, 5]);
        assert_eq!(page.cursor, 5);
        let page = log.read(&EventQuery::after(5).pod("a")).unwrap();
        assert!(page.events.is_empty());
        assert_eq!(page.cursor, 5);
        let mut query = EventQuery::after(0);
        query.limit = 2;
        let page = log.read(&query).unwrap();
        assert_eq!(seqs(&page), [1, 2]);
        assert_eq!(page.cursor, 2);
        let page = log.read(&EventQuery::after(4).pod("a")).unwrap();
        assert_eq!(seqs(&page), [5]);
    }
}
