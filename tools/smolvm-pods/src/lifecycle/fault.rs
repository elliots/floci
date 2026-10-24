//! Fault points for the end-to-end tests.
//!
//! `SMOLVM_PODS_FAULT` takes a comma-separated list of points, each for every
//! pod or, as `POINT@SANDBOX`, for one pod sandbox. A point does one of three
//! things ([`FaultKind`]):
//!
//! - **kill**, such as `pause:saving`: the node service kills itself with
//!   SIGKILL there, so that a test can check the recovery of the state it
//!   leaves. The point fires once: it is written to `faults-fired` in the
//!   state directory before the process dies, and a restarted node service
//!   with the same environment skips it. Remove the file to arm it again.
//! - **wait** (`idle:decided`): the node service waits [`FAULT_WAIT`] there,
//!   once, then goes on.
//! - **error** (`wake:resume-error`): the step fails, every time.
//!
//! Each writes `fault point POINT reached` to stderr when it fires, followed by
//! ` for pod sandbox SANDBOX` where the pod is known. When the variable is
//! unset, a fault point is one branch on a `None`.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use log::warn;

use crate::sync::lock;

/// The environment variable that arms fault points.
pub const FAULT_ENV: &str = "SMOLVM_PODS_FAULT";

/// How long a wait point waits.
pub const FAULT_WAIT: Duration = Duration::from_secs(5);

/// In the state directory: the kill points that already fired.
const FIRED_FILE: &str = "faults-fired";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FaultPoint {
    /// The VM is frozen and smolvm's record says Pausing.
    PauseSaving,
    /// smolvm's record says Paused, and the VM process has not stopped yet.
    PauseDurable,
    /// The resume returned, before the post-resume steps.
    WakeResumed,
    /// The guest clock was stepped, before the pod is awake.
    WakeClock,
    /// A pause for being idle, after the idle decision and before the
    /// `pausing` hook holds the pod's traffic.
    IdleDecided,
    /// A resume fails before the VM starts and leaves the checkpoint intact,
    /// as a checkpoint that cannot be restored makes it.
    WakeResumeError,
}

/// What a fault point does when the node service reaches it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
    /// Kill the node service with SIGKILL, once.
    Kill,
    /// Wait [`FAULT_WAIT`], once, then go on.
    Wait,
    /// Fail the step, every time.
    Error,
}

impl FaultPoint {
    pub const ALL: [FaultPoint; 6] = [
        FaultPoint::PauseSaving,
        FaultPoint::PauseDurable,
        FaultPoint::WakeResumed,
        FaultPoint::WakeClock,
        FaultPoint::IdleDecided,
        FaultPoint::WakeResumeError,
    ];

    pub fn name(self) -> &'static str {
        match self {
            FaultPoint::PauseSaving => "pause:saving",
            FaultPoint::PauseDurable => "pause:durable",
            FaultPoint::WakeResumed => "wake:resumed",
            FaultPoint::WakeClock => "wake:clock",
            FaultPoint::IdleDecided => "idle:decided",
            FaultPoint::WakeResumeError => "wake:resume-error",
        }
    }

    pub fn kind(self) -> FaultKind {
        match self {
            FaultPoint::IdleDecided => FaultKind::Wait,
            FaultPoint::WakeResumeError => FaultKind::Error,
            _ => FaultKind::Kill,
        }
    }

    pub fn parse(name: &str) -> Option<FaultPoint> {
        FaultPoint::ALL.into_iter().find(|p| p.name() == name)
    }
}

/// One armed point, for every pod or for one pod sandbox.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    point: FaultPoint,
    pod: Option<String>,
}

impl Entry {
    fn parse(text: &str) -> Result<Entry, String> {
        let (name, pod) = match text.split_once('@') {
            Some((name, pod)) if !pod.trim().is_empty() => (name.trim(), Some(pod.trim())),
            Some(_) => return Err(format!("{FAULT_ENV}: {text:?} names no pod sandbox")),
            None => (text, None),
        };
        let point = FaultPoint::parse(name).ok_or_else(|| {
            let known: Vec<_> = FaultPoint::ALL.iter().map(|p| p.name()).collect();
            format!(
                "{FAULT_ENV}: unknown fault point {name:?}; the points are {}",
                known.join(", ")
            )
        })?;
        Ok(Entry {
            point,
            pod: pod.map(str::to_string),
        })
    }

    /// The entry as `SMOLVM_PODS_FAULT` and `faults-fired` write it.
    fn label(&self) -> String {
        match &self.pod {
            Some(pod) => format!("{}@{pod}", self.point.name()),
            None => self.point.name().to_string(),
        }
    }

    fn matches(&self, point: FaultPoint, pod: Option<&str>) -> bool {
        self.point == point && self.pod.as_deref().is_none_or(|p| Some(p) == pod)
    }
}

/// The armed fault points, and the pod they are checked for. Cloning shares
/// the points.
#[derive(Clone, Default)]
pub struct Faults {
    armed: Option<Arc<Armed>>,
    pod: Option<Arc<str>>,
}

struct Armed {
    entries: Vec<Entry>,
    /// The labels of the kill and wait entries that fired.
    fired: Mutex<Vec<String>>,
    action: Action,
}

impl Armed {
    /// The first entry for `point` and `pod` that may still fire.
    fn entry(&self, point: FaultPoint, pod: Option<&str>) -> Option<&Entry> {
        let fired = lock(&self.fired);
        self.entries.iter().find(|e| {
            e.matches(point, pod)
                && (point.kind() == FaultKind::Error || !fired.contains(&e.label()))
        })
    }
}

enum Action {
    /// What the node service does.
    Process { fired_file: PathBuf },
    #[cfg(test)]
    Simulate(Arc<SimulatedCrash>),
}

impl Faults {
    pub fn none() -> Faults {
        Faults::default()
    }

    /// The fault points that `SMOLVM_PODS_FAULT` names, for a node service
    /// whose state directory is `dir`.
    pub fn from_env(dir: &Path) -> Result<Faults, String> {
        match std::env::var(FAULT_ENV) {
            Ok(spec) if !spec.trim().is_empty() => Faults::from_spec(&spec, dir),
            _ => Ok(Faults::none()),
        }
    }

    /// The fault points of `spec`, skipping the kill points that `dir`
    /// records as fired.
    pub fn from_spec(spec: &str, dir: &Path) -> Result<Faults, String> {
        let entries = parse_entries(spec)?;
        let fired_file = dir.join(FIRED_FILE);
        let fired = std::fs::read_to_string(&fired_file)
            .unwrap_or_default()
            .lines()
            .map(|line| line.trim().to_string())
            .filter(|line| !line.is_empty())
            .collect();
        Ok(Faults {
            armed: Some(Arc::new(Armed {
                entries,
                fired: Mutex::new(fired),
                action: Action::Process { fired_file },
            })),
            pod: None,
        })
    }

    /// The same points, checked for pod sandbox `sandbox_id`: a point armed
    /// for another pod does not fire.
    pub fn for_pod(&self, sandbox_id: &str) -> Faults {
        Faults {
            armed: self.armed.clone(),
            pod: Some(Arc::from(sandbox_id)),
        }
    }

    /// Whether `point` would fire.
    #[cfg(test)]
    pub fn armed(&self, point: FaultPoint) -> bool {
        self.armed
            .as_ref()
            .is_some_and(|a| a.entry(point, self.pod.as_deref()).is_some())
    }

    /// Fires a kill or wait point if it is armed. At a kill point the node
    /// service dies, so this never returns.
    pub fn hit(&self, point: FaultPoint) {
        let Some(armed) = &self.armed else {
            return;
        };
        if point.kind() == FaultKind::Error {
            return;
        }
        let Some(entry) = armed.entry(point, self.pod.as_deref()) else {
            return;
        };
        let label = entry.label();
        {
            let mut fired = lock(&armed.fired);
            if fired.contains(&label) {
                return;
            }
            fired.push(label.clone());
        }
        match &armed.action {
            Action::Process { fired_file } => {
                if point.kind() == FaultKind::Kill {
                    kill_self(point, &label, self.pod.as_deref(), fired_file);
                }
                report(point, self.pod.as_deref());
                std::thread::sleep(FAULT_WAIT);
            }
            #[cfg(test)]
            Action::Simulate(crash) => crash.hit(point),
        }
    }

    /// Fails at an error point, every time it is armed.
    pub fn check(&self, point: FaultPoint) -> Result<(), String> {
        let Some(armed) = &self.armed else {
            return Ok(());
        };
        if point.kind() != FaultKind::Error || armed.entry(point, self.pod.as_deref()).is_none() {
            return Ok(());
        }
        if matches!(armed.action, Action::Process { .. }) {
            report(point, self.pod.as_deref());
        }
        Err(format!("fault point {}", point.name()))
    }
}

fn parse_entries(spec: &str) -> Result<Vec<Entry>, String> {
    spec.split(',')
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(Entry::parse)
        .collect()
}

/// The line the end-to-end tests look for in the node service's log.
fn report(point: FaultPoint, pod: Option<&str>) {
    match pod {
        Some(pod) => eprintln!("fault point {} reached for pod sandbox {pod}", point.name()),
        None => eprintln!("fault point {} reached", point.name()),
    }
}

fn kill_self(point: FaultPoint, label: &str, pod: Option<&str>, fired_file: &Path) -> ! {
    let recorded = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(fired_file)
        .and_then(|mut f| {
            writeln!(f, "{label}")?;
            f.sync_all()
        });
    if let Err(e) = recorded {
        warn!(
            "record fault point {label} in {}: {e}",
            fired_file.display()
        );
    }
    report(point, pod);
    // SAFETY: kill with this process's own pid and a constant signal.
    unsafe {
        libc::kill(libc::getpid(), libc::SIGKILL);
    }
    std::process::abort()
}

/// A crash at a fault point, for tests: the thread that hits the point waits
/// until the test releases it, then panics, so the code after the point never
/// runs. The test shuts the lifecycle down meanwhile and opens a new one on
/// the same state directory. At a wait point the thread goes on instead.
#[cfg(test)]
pub struct SimulatedCrash {
    hit: tokio::sync::watch::Sender<Option<FaultPoint>>,
    released: Mutex<bool>,
    wake: std::sync::Condvar,
}

#[cfg(test)]
impl SimulatedCrash {
    fn hit(&self, point: FaultPoint) {
        self.hit.send_replace(Some(point));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        let mut released = lock(&self.released);
        while !*released && std::time::Instant::now() < deadline {
            released = crate::sync::wait_timeout(
                &self.wake,
                released,
                std::time::Duration::from_millis(10),
            );
        }
        drop(released);
        if point.kind() == FaultKind::Wait {
            return;
        }
        panic!("simulated crash at {}", point.name());
    }

    /// Waits until a point fired.
    pub async fn crashed(&self) -> FaultPoint {
        let mut rx = self.hit.subscribe();
        let point = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            rx.wait_for(Option::is_some),
        )
        .await
        .expect("no fault point fired")
        .expect("the crash was dropped");
        point.expect("a fault point")
    }

    pub fn release(&self) {
        *lock(&self.released) = true;
        self.wake.notify_all();
    }
}

#[cfg(test)]
impl Faults {
    pub fn simulated(points: &[FaultPoint]) -> (Faults, Arc<SimulatedCrash>) {
        let crash = Arc::new(SimulatedCrash {
            hit: tokio::sync::watch::channel(None).0,
            released: Mutex::new(false),
            wake: std::sync::Condvar::new(),
        });
        let entries = points
            .iter()
            .map(|&point| Entry { point, pod: None })
            .collect();
        let faults = Faults {
            armed: Some(Arc::new(Armed {
                entries,
                fired: Mutex::new(Vec::new()),
                action: Action::Simulate(crash.clone()),
            })),
            pod: None,
        };
        (faults, crash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn points_parse_from_a_comma_separated_list() {
        let dir = tempfile::tempdir().unwrap();
        let faults = Faults::from_spec(" pause:saving, wake:clock ,", dir.path()).unwrap();
        assert!(faults.armed(FaultPoint::PauseSaving));
        assert!(faults.armed(FaultPoint::WakeClock));
        assert!(!faults.armed(FaultPoint::PauseDurable));
        for point in FaultPoint::ALL {
            assert_eq!(FaultPoint::parse(point.name()), Some(point));
        }
    }

    #[test]
    fn an_unknown_point_is_refused_with_the_known_names() {
        let dir = tempfile::tempdir().unwrap();
        let error = Faults::from_spec("pause:saving,pause:later", dir.path())
            .err()
            .unwrap();
        assert!(error.contains("\"pause:later\""), "{error}");
        assert!(error.contains("wake:resumed"), "{error}");
    }

    #[test]
    fn a_point_that_fired_before_a_restart_stays_disarmed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(FIRED_FILE), "pause:saving\n").unwrap();
        let faults = Faults::from_spec("pause:saving,pause:durable", dir.path()).unwrap();
        assert!(!faults.armed(FaultPoint::PauseSaving));
        assert!(faults.armed(FaultPoint::PauseDurable));
        // A disarmed point returns.
        faults.hit(FaultPoint::PauseSaving);
    }

    #[test]
    fn unset_faults_do_nothing() {
        let faults = Faults::none();
        for point in FaultPoint::ALL {
            assert!(!faults.armed(point));
            faults.hit(point);
        }
    }
    #[test]
    fn a_point_for_one_pod_fires_only_for_that_pod() {
        let dir = tempfile::tempdir().unwrap();
        let faults = Faults::from_spec("idle:decided@pod-a,wake:clock", dir.path()).unwrap();
        assert!(!faults.armed(FaultPoint::IdleDecided));
        assert!(faults.for_pod("pod-a").armed(FaultPoint::IdleDecided));
        assert!(!faults.for_pod("pod-b").armed(FaultPoint::IdleDecided));
        assert!(faults.for_pod("pod-b").armed(FaultPoint::WakeClock));
        let error = Faults::from_spec("idle:decided@", dir.path())
            .err()
            .unwrap();
        assert!(error.contains("names no pod"), "{error}");
    }

    #[test]
    fn a_kill_point_for_one_pod_is_recorded_with_its_pod() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(FIRED_FILE), "pause:saving@pod-a\n").unwrap();
        let faults =
            Faults::from_spec("pause:saving@pod-a,pause:saving@pod-b", dir.path()).unwrap();
        assert!(!faults.for_pod("pod-a").armed(FaultPoint::PauseSaving));
        assert!(faults.for_pod("pod-b").armed(FaultPoint::PauseSaving));
    }

    #[test]
    fn an_error_point_fails_every_time_and_never_kills() {
        let dir = tempfile::tempdir().unwrap();
        let faults = Faults::from_spec("wake:resume-error@pod-a", dir.path()).unwrap();
        let pod = faults.for_pod("pod-a");
        for _ in 0..3 {
            let error = pod.check(FaultPoint::WakeResumeError).unwrap_err();
            assert!(error.contains("wake:resume-error"), "{error}");
            assert!(pod.armed(FaultPoint::WakeResumeError));
        }
        pod.hit(FaultPoint::WakeResumeError);
        assert_eq!(
            faults.for_pod("pod-b").check(FaultPoint::WakeResumeError),
            Ok(())
        );
        assert_eq!(Faults::none().check(FaultPoint::WakeResumeError), Ok(()));
        assert!(!dir.path().join(FIRED_FILE).exists());
    }

    #[test]
    fn the_points_have_their_kinds() {
        assert_eq!(FaultPoint::PauseSaving.kind(), FaultKind::Kill);
        assert_eq!(FaultPoint::WakeClock.kind(), FaultKind::Kill);
        assert_eq!(FaultPoint::IdleDecided.kind(), FaultKind::Wait);
        assert_eq!(FaultPoint::WakeResumeError.kind(), FaultKind::Error);
    }
}
