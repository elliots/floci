//! The node service's metrics, which `GET /metrics` serves in the Prometheus
//! text format, version 0.0.4.
//!
//! Each series is per node, by a bounded label such as a state or a reason
//! kind, never by pod: a sleeping pod adds no series and no memory here. The
//! one exception is the traffic between pods, by pair of pods, which the idle
//! loop reads from conntrack's accounting at each poll and caps (see
//! [`crate::activity::Flow`]).
//!
//! [`Transitions`] counts the lifecycle's events as they are written, the
//! pods by state and the checkpoint bytes are read from the pods at each
//! scrape, and [`process`] reads the node service's own process from
//! `/proc/self`.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::Mutex;

use crate::lifecycle::{Change, Event, SaveMode};
use crate::sync::lock;

/// The bounds of the wake durations, in seconds.
pub const WAKE_BUCKETS: &[f64] = &[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0];

/// The bounds of the pause durations, in seconds.
pub const SLEEP_BUCKETS: &[f64] = &[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0];

/// The bounds of the admission waits, in seconds.
pub const ADMISSION_BUCKETS: &[f64] = &[0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0];

/// A histogram with fixed bucket bounds.
#[derive(Debug, Clone)]
pub struct Histogram {
    bounds: &'static [f64],
    /// The observations at or below each bound, not cumulated.
    counts: Vec<u64>,
    sum: f64,
    count: u64,
}

impl Histogram {
    pub fn new(bounds: &'static [f64]) -> Histogram {
        Histogram {
            bounds,
            counts: vec![0; bounds.len()],
            sum: 0.0,
            count: 0,
        }
    }

    pub fn observe(&mut self, value: f64) {
        if let Some(i) = self.bounds.iter().position(|bound| value <= *bound) {
            self.counts[i] += 1;
        }
        self.sum += value;
        self.count += 1;
    }
}

/// A metric family's type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Counter,
    Gauge,
    Histogram,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Kind::Counter => "counter",
            Kind::Gauge => "gauge",
            Kind::Histogram => "histogram",
        }
    }
}

/// The text of a scrape. Each family is written whole: its `# HELP` and
/// `# TYPE` lines, then its samples.
#[derive(Default)]
pub struct Text {
    out: String,
}

impl Text {
    pub fn new() -> Text {
        Text::default()
    }

    pub fn family(&mut self, name: &str, kind: Kind, help: &str) {
        let _ = writeln!(self.out, "# HELP {name} {}", escape_help(help));
        let _ = writeln!(self.out, "# TYPE {name} {}", kind.as_str());
    }

    pub fn sample(&mut self, name: &str, labels: &[(&str, &str)], value: f64) {
        self.out.push_str(name);
        self.labels(labels, None);
        let _ = writeln!(self.out, " {}", number(value));
    }

    /// The `_bucket`, `_sum` and `_count` samples of a histogram.
    pub fn histogram(&mut self, name: &str, labels: &[(&str, &str)], h: &Histogram) {
        let mut cumulative = 0;
        for (bound, count) in h.bounds.iter().zip(&h.counts) {
            cumulative += count;
            let _ = write!(self.out, "{name}_bucket");
            self.labels(labels, Some(&number(*bound)));
            let _ = writeln!(self.out, " {cumulative}");
        }
        let _ = write!(self.out, "{name}_bucket");
        self.labels(labels, Some("+Inf"));
        let _ = writeln!(self.out, " {}", h.count);
        let _ = write!(self.out, "{name}_sum");
        self.labels(labels, None);
        let _ = writeln!(self.out, " {}", number(h.sum));
        let _ = write!(self.out, "{name}_count");
        self.labels(labels, None);
        let _ = writeln!(self.out, " {}", h.count);
    }

    fn labels(&mut self, labels: &[(&str, &str)], le: Option<&str>) {
        if labels.is_empty() && le.is_none() {
            return;
        }
        self.out.push('{');
        let le = le.map(|le| ("le", le));
        for (i, (name, value)) in labels.iter().copied().chain(le).enumerate() {
            if i > 0 {
                self.out.push(',');
            }
            let _ = write!(self.out, "{name}=\"{}\"", escape_value(value));
        }
        self.out.push('}');
    }

    pub fn finish(self) -> String {
        self.out
    }
}

/// A sample value: whole numbers without a fraction.
fn number(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value > 0.0 { "+Inf" } else { "-Inf" }.to_string()
    } else if value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{}", value as i64)
    } else {
        format!("{value}")
    }
}

fn escape_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn escape_help(help: &str) -> String {
    help.replace('\\', "\\\\").replace('\n', "\\n")
}

/// The outcomes of a wake, as the label `outcome` of
/// `smolvm_pods_wakes_total`.
const WAKE_OUTCOMES: &[&str] = &["awake", "cancelled_pause", "refused", "failed"];

/// The outcomes of a pause, as the label `outcome` of
/// `smolvm_pods_sleeps_total`.
const SLEEP_OUTCOMES: &[&str] = &["asleep", "cancelled", "failed"];

/// The lifecycle's transitions, counted from its events.
#[derive(Default)]
pub struct Transitions {
    inner: Mutex<Counted>,
}

#[derive(Default)]
struct Counted {
    /// By reason kind and outcome.
    wakes: BTreeMap<(&'static str, &'static str), u64>,
    wake_seconds: BTreeMap<&'static str, Histogram>,
    sleeps: BTreeMap<(&'static str, &'static str), u64>,
    sleep_seconds: BTreeMap<&'static str, Histogram>,
    /// By save mode.
    written_bytes: BTreeMap<&'static str, u64>,
    failures: u64,
}

impl Transitions {
    /// Counts an event of the lifecycle.
    pub fn observe(&self, event: &Event) {
        let reason = event.reason.kind();
        let seconds = event
            .duration_ms
            .filter(|_| !event.recovered)
            .map(|ms| ms / 1000.0);
        let mut counted = lock(&self.inner);
        let (wake, sleep) = match event.change() {
            Change::Woke => (Some("awake"), None),
            Change::PauseCancelled => (Some("cancelled_pause"), Some("cancelled")),
            Change::WakeRefused => (Some("refused"), None),
            Change::WakeFailed => (Some("failed"), None),
            Change::Slept => (None, Some("asleep")),
            Change::SleepFailed => (None, Some("failed")),
            Change::Failed => {
                counted.failures += 1;
                (None, None)
            }
            _ => (None, None),
        };
        if let Some(outcome) = wake {
            *counted.wakes.entry((reason, outcome)).or_default() += 1;
            if let (Some(seconds), "awake") = (seconds, outcome) {
                counted
                    .wake_seconds
                    .entry(reason)
                    .or_insert_with(|| Histogram::new(WAKE_BUCKETS))
                    .observe(seconds);
            }
        }
        if let Some(outcome) = sleep {
            // A pause that a wake cancelled is counted by the wake's reason.
            *counted.sleeps.entry((reason, outcome)).or_default() += 1;
            if outcome == "asleep" {
                let (mode, bytes) = match event.save {
                    Some(save) => (save.mode.as_str(), save.written_bytes),
                    None => (SaveMode::Full.as_str(), event.checkpoint_bytes.unwrap_or(0)),
                };
                *counted.written_bytes.entry(mode).or_default() += bytes;
                if let Some(seconds) = seconds {
                    counted
                        .sleep_seconds
                        .entry(reason)
                        .or_insert_with(|| Histogram::new(SLEEP_BUCKETS))
                        .observe(seconds);
                }
            }
        }
    }

    pub fn render(&self, text: &mut Text) {
        let counted = lock(&self.inner);
        text.family(
            "smolvm_pods_wakes_total",
            Kind::Counter,
            "Wakes of sleeping or pausing pods, by reason kind and outcome.",
        );
        render_outcomes(
            text,
            "smolvm_pods_wakes_total",
            &counted.wakes,
            WAKE_OUTCOMES,
        );
        text.family(
            "smolvm_pods_wake_duration_seconds",
            Kind::Histogram,
            "The time from a wake's start to the pod being awake, by reason kind.",
        );
        for (reason, h) in &counted.wake_seconds {
            text.histogram(
                "smolvm_pods_wake_duration_seconds",
                &[("reason", reason)],
                h,
            );
        }
        text.family(
            "smolvm_pods_sleeps_total",
            Kind::Counter,
            "Pauses of awake pods, by reason kind and outcome.",
        );
        render_outcomes(
            text,
            "smolvm_pods_sleeps_total",
            &counted.sleeps,
            SLEEP_OUTCOMES,
        );
        text.family(
            "smolvm_pods_sleep_duration_seconds",
            Kind::Histogram,
            "The time from a pause's start to the pod being asleep, by reason kind.",
        );
        for (reason, h) in &counted.sleep_seconds {
            text.histogram(
                "smolvm_pods_sleep_duration_seconds",
                &[("reason", reason)],
                h,
            );
        }
        text.family(
            "smolvm_pods_checkpoint_written_bytes_total",
            Kind::Counter,
            "The guest RAM that the pauses that completed wrote, by save mode.",
        );
        for mode in [SaveMode::Full.as_str(), SaveMode::Delta.as_str()] {
            let bytes = counted.written_bytes.get(mode).copied().unwrap_or(0);
            text.sample(
                "smolvm_pods_checkpoint_written_bytes_total",
                &[("mode", mode)],
                bytes as f64,
            );
        }
        text.family(
            "smolvm_pods_failures_total",
            Kind::Counter,
            "Pods that failed: their VM was lost.",
        );
        text.sample("smolvm_pods_failures_total", &[], counted.failures as f64);
    }
}

/// The counts by reason and outcome, with every outcome of a reason that was
/// seen, so that a rate of each starts at 0.
fn render_outcomes(
    text: &mut Text,
    name: &str,
    counts: &BTreeMap<(&'static str, &'static str), u64>,
    outcomes: &[&str],
) {
    let mut reasons: Vec<&str> = counts.keys().map(|(reason, _)| *reason).collect();
    reasons.dedup();
    for reason in reasons {
        for outcome in outcomes {
            let n = counts.get(&(reason, *outcome)).copied().unwrap_or(0);
            text.sample(name, &[("reason", reason), ("outcome", outcome)], n as f64);
        }
    }
}

/// The node service's own process, as the standard `process_*` metrics.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Process {
    pub cpu_seconds: f64,
    pub start_time_seconds: f64,
    pub virtual_bytes: u64,
    pub resident_bytes: u64,
    pub open_fds: u64,
}

/// Reads [`Process`] from `/proc/self`.
pub fn process() -> Result<Process, String> {
    let stat = std::fs::read_to_string("/proc/self/stat")
        .map_err(|e| format!("read /proc/self/stat: {e}"))?;
    let boot = std::fs::read_to_string("/proc/stat")
        .ok()
        .and_then(|s| boot_time(&s))
        .unwrap_or(0);
    let open_fds = std::fs::read_dir("/proc/self/fd")
        .map(|dir| dir.count() as u64)
        .unwrap_or(0);
    // SAFETY: sysconf has no preconditions.
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as f64;
    // SAFETY: sysconf has no preconditions.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) }.max(1) as u64;
    parse_stat(&stat, boot, ticks, page, open_fds)
}

/// The boot time, in seconds since the epoch, from `/proc/stat`.
fn boot_time(stat: &str) -> Option<u64> {
    stat.lines()
        .find_map(|line| line.strip_prefix("btime "))
        .and_then(|s| s.trim().parse().ok())
}

/// Parses `/proc/self/stat`, whose second field, the command, may hold
/// spaces and parentheses.
fn parse_stat(
    stat: &str,
    boot: u64,
    ticks: f64,
    page: u64,
    open_fds: u64,
) -> Result<Process, String> {
    let rest = stat
        .rfind(')')
        .map(|i| &stat[i + 1..])
        .ok_or("no command in /proc/self/stat")?;
    // The fields after the command, from field 3 (the state).
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let field = |n: usize| -> Result<u64, String> {
        fields
            .get(n - 3)
            .and_then(|f| f.parse().ok())
            .ok_or_else(|| format!("no field {n} in /proc/self/stat"))
    };
    Ok(Process {
        cpu_seconds: (field(14)? + field(15)?) as f64 / ticks,
        start_time_seconds: boot as f64 + field(22)? as f64 / ticks,
        virtual_bytes: field(23)?,
        resident_bytes: field(24)? * page,
        open_fds,
    })
}

pub fn render_process(text: &mut Text, process: &Process) {
    let families: [(&str, Kind, &str, f64); 5] = [
        (
            "process_cpu_seconds_total",
            Kind::Counter,
            "User and system CPU time of the node service, in seconds.",
            process.cpu_seconds,
        ),
        (
            "process_start_time_seconds",
            Kind::Gauge,
            "When the node service started, in seconds since the epoch.",
            process.start_time_seconds,
        ),
        (
            "process_virtual_memory_bytes",
            Kind::Gauge,
            "The node service's virtual memory size.",
            process.virtual_bytes as f64,
        ),
        (
            "process_resident_memory_bytes",
            Kind::Gauge,
            "The node service's resident memory.",
            process.resident_bytes as f64,
        ),
        (
            "process_open_fds",
            Kind::Gauge,
            "The node service's open file descriptors.",
            process.open_fds as f64,
        ),
    ];
    for (name, kind, help, value) in families {
        text.family(name, kind, help);
        text.sample(name, &[], value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::{PodState, Reason, Save};

    fn event(from: PodState, to: PodState, reason: Reason, duration_ms: f64) -> Event {
        Event {
            seq: 1,
            time_ms: 1,
            sandbox_id: "sb".into(),
            incarnation: "inc".into(),
            from: Some(from),
            state: to,
            generation: 1,
            reason,
            source: None,
            steps: Vec::new(),
            duration_ms: Some(duration_ms),
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

    #[test]
    fn a_histogram_is_written_cumulated_with_its_sum_and_count() {
        let mut h = Histogram::new(&[0.1, 1.0]);
        for value in [0.0625, 0.5, 0.75, 3.0] {
            h.observe(value);
        }
        let mut text = Text::new();
        text.family("t_seconds", Kind::Histogram, "A test.");
        text.histogram("t_seconds", &[("reason", "a\"b")], &h);
        assert_eq!(
            text.finish(),
            "# HELP t_seconds A test.\n\
             # TYPE t_seconds histogram\n\
             t_seconds_bucket{reason=\"a\\\"b\",le=\"0.1\"} 1\n\
             t_seconds_bucket{reason=\"a\\\"b\",le=\"1\"} 3\n\
             t_seconds_bucket{reason=\"a\\\"b\",le=\"+Inf\"} 4\n\
             t_seconds_sum{reason=\"a\\\"b\"} 4.3125\n\
             t_seconds_count{reason=\"a\\\"b\"} 4\n"
        );
    }

    #[test]
    fn transitions_count_wakes_and_sleeps_by_reason_and_outcome() {
        let t = Transitions::default();
        let traffic = Reason::Traffic {
            from: "ns/client".into(),
            via: None,
            port: 8080,
        };
        t.observe(&event(
            PodState::Asleep,
            PodState::Waking,
            traffic.clone(),
            1.0,
        ));
        t.observe(&event(
            PodState::Waking,
            PodState::Awake,
            traffic.clone(),
            300.0,
        ));
        let mut slept = event(PodState::Pausing, PodState::Asleep, Reason::Idle, 2000.0);
        slept.checkpoint_bytes = Some(1 << 20);
        t.observe(&slept);
        let mut delta = event(PodState::Pausing, PodState::Asleep, Reason::Idle, 400.0);
        delta.checkpoint_bytes = Some(1 << 20);
        delta.save = Some(Save {
            mode: SaveMode::Delta,
            written_bytes: 1 << 16,
        });
        t.observe(&delta);
        let mut refused = event(PodState::Asleep, PodState::Asleep, Reason::Api, 50.0);
        refused.error = Some("wake refused: awake pods 2/2".into());
        t.observe(&refused);
        let mut cancelled = event(PodState::Pausing, PodState::Awake, traffic, 20.0);
        cancelled.cancelled = true;
        t.observe(&cancelled);

        let mut text = Text::new();
        t.render(&mut text);
        let text = text.finish();
        for line in [
            "smolvm_pods_wakes_total{reason=\"api\",outcome=\"refused\"} 1",
            "smolvm_pods_wakes_total{reason=\"api\",outcome=\"awake\"} 0",
            "smolvm_pods_wakes_total{reason=\"traffic\",outcome=\"awake\"} 1",
            "smolvm_pods_wakes_total{reason=\"traffic\",outcome=\"cancelled_pause\"} 1",
            "smolvm_pods_wake_duration_seconds_bucket{reason=\"traffic\",le=\"0.25\"} 0",
            "smolvm_pods_wake_duration_seconds_bucket{reason=\"traffic\",le=\"0.5\"} 1",
            "smolvm_pods_wake_duration_seconds_sum{reason=\"traffic\"} 0.3",
            "smolvm_pods_sleeps_total{reason=\"idle\",outcome=\"asleep\"} 2",
            "smolvm_pods_sleeps_total{reason=\"traffic\",outcome=\"cancelled\"} 1",
            "smolvm_pods_sleep_duration_seconds_count{reason=\"idle\"} 2",
            "smolvm_pods_checkpoint_written_bytes_total{mode=\"full\"} 1048576",
            "smolvm_pods_checkpoint_written_bytes_total{mode=\"delta\"} 65536",
            "smolvm_pods_failures_total 0",
        ] {
            assert!(text.lines().any(|l| l == line), "no {line:?} in\n{text}");
        }
        assert!(!text.contains("reason=\"api\"} "), "{text}");
    }

    #[test]
    fn the_process_is_read_from_its_stat_line() {
        let stat = "4242 (node (svc)) S 1 4242 4242 0 -1 4194560 900 0 0 0 250 50 0 0 20 0 \
                    9 0 1000 104857600 2560 18446744073709551615";
        let p = parse_stat(stat, 1_700_000_000, 100.0, 4096, 12).unwrap();
        assert_eq!(p.cpu_seconds, 3.0);
        assert_eq!(p.start_time_seconds, 1_700_000_010.0);
        assert_eq!(p.virtual_bytes, 104_857_600);
        assert_eq!(p.resident_bytes, 2560 * 4096);
        assert_eq!(p.open_fds, 12);
        assert_eq!(
            boot_time("cpu 1 2\nbtime 1700000000\n"),
            Some(1_700_000_000)
        );
    }
}
