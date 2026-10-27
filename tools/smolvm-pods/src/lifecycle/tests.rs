//! Tests of the lifecycle over the mock VM layer.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::fault::{FaultPoint, SimulatedCrash};
use super::mock::{clock_at, MockVm, RecordingHooks, CHECKPOINT_BYTES};
use super::record::ANN_HIBERNATE;
use super::store::Store;
use super::*;

use super::PodState::{Asleep, Awake, Creating, Deleted, Deleting, Failed, Pausing, Waking};

const BOOT: &str = "boot-1";
const START_MS: u64 = 1_800_000_000_000;

fn pod(id: &str) -> PodRef {
    PodRef::sandbox(id)
}

fn identity(id: &str) -> PodIdentity {
    PodIdentity {
        sandbox_id: id.into(),
        namespace: "default".into(),
        name: format!("{id}-name"),
        uid: format!("{id}-uid"),
        machine: id.into(),
        netns: Some(format!("/var/run/netns/cni-{id}")),
        ..Default::default()
    }
}

fn kill(container: &str, exec: Option<&str>, signal: u32) -> KillRequest {
    KillRequest {
        container: container.into(),
        exec: exec.map(str::to_string),
        signal,
        all: false,
    }
}

async fn wait_until(mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "timed out");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

struct Harness {
    dir: tempfile::TempDir,
    vm: Arc<MockVm>,
    hooks: Arc<RecordingHooks>,
    now_ms: Arc<AtomicU64>,
    faults: Faults,
    crash: Option<Arc<SimulatedCrash>>,
    tune: Option<fn(&mut Config)>,
    life: Option<Lifecycle>,
}

impl Harness {
    fn unopened() -> Harness {
        Harness {
            dir: tempfile::tempdir().unwrap(),
            vm: Arc::new(MockVm::new()),
            hooks: Arc::new(RecordingHooks::default()),
            now_ms: Arc::new(AtomicU64::new(START_MS)),
            faults: Faults::none(),
            crash: None,
            tune: None,
            life: None,
        }
    }

    async fn new() -> Harness {
        let mut h = Harness::unopened();
        h.open().await;
        h
    }

    /// A harness with the awake pod `id`, and no calls recorded yet.
    async fn awake(id: &str) -> Harness {
        let h = Harness::new().await;
        h.create(id).await.unwrap();
        h.clear();
        h
    }

    async fn asleep(id: &str) -> Harness {
        let h = Harness::awake(id).await;
        h.hibernate(id).await.unwrap();
        h.clear();
        h
    }

    fn crashing(points: &[FaultPoint]) -> (Harness, Arc<SimulatedCrash>) {
        let mut h = Harness::unopened();
        let (faults, crash) = Faults::simulated(points);
        h.faults = faults;
        h.crash = Some(crash.clone());
        (h, crash)
    }

    fn config(&self) -> Config {
        let mut config = Config::defaults(self.dir.path());
        config.boot_id = BOOT.into();
        config.agent_timeout = Duration::from_secs(5);
        config.ping_interval = Duration::from_millis(5);
        config.wake_grace = Duration::ZERO;
        config.faults = self.faults.clone();
        config.clock = clock_at(&self.now_ms);
        if let Some(tune) = self.tune {
            tune(&mut config);
        }
        config
    }

    /// Opens the lifecycle, after shutting down the open one as a crash
    /// would, and waits for its recovery.
    async fn open(&mut self) {
        if let Some(old) = self.life.take() {
            old.shutdown();
        }
        let life = Lifecycle::open(self.config(), self.vm.clone(), self.hooks.clone())
            .await
            .unwrap();
        life.settled().await;
        self.life = Some(life);
    }

    fn life(&self) -> &Lifecycle {
        self.life.as_ref().expect("an open lifecycle")
    }

    fn advance(&self, by: Duration) {
        self.now_ms
            .fetch_add(by.as_millis() as u64, Ordering::SeqCst);
    }

    async fn create(&self, id: &str) -> Result<PodInfo, LifecycleError> {
        self.create_with(identity(id)).await
    }

    async fn create_with(&self, identity: PodIdentity) -> Result<PodInfo, LifecycleError> {
        let vm = self.vm.clone();
        let machine = identity.machine.clone();
        self.life()
            .create(identity, async move {
                vm.add_running(&machine);
                Ok(None)
            })
            .await
    }

    async fn wake(&self, id: &str) -> Result<WakeReport, LifecycleError> {
        self.life()
            .wake(&pod(id), WakeRequest::new(Reason::Api))
            .await
    }

    async fn hibernate(&self, id: &str) -> Result<SleepReport, LifecycleError> {
        self.life()
            .hibernate(&pod(id), HibernateRequest::new(Reason::Idle))
            .await
    }

    fn spawn_wake(&self, id: &str) -> tokio::task::JoinHandle<Result<WakeReport, LifecycleError>> {
        let (life, id) = (self.life().clone(), id.to_string());
        tokio::spawn(async move { life.wake(&pod(&id), WakeRequest::new(Reason::Api)).await })
    }

    fn spawn_hibernate(
        &self,
        id: &str,
    ) -> tokio::task::JoinHandle<Result<SleepReport, LifecycleError>> {
        let (life, id) = (self.life().clone(), id.to_string());
        tokio::spawn(async move {
            life.hibernate(&pod(&id), HibernateRequest::new(Reason::Idle))
                .await
        })
    }

    fn info(&self, id: &str) -> PodInfo {
        self.life().get(&pod(id)).expect("a pod")
    }

    fn state(&self, id: &str) -> Option<PodState> {
        self.life().get(&pod(id)).map(|p| p.state)
    }

    fn events(&self, id: &str) -> Vec<Event> {
        self.life()
            .events(&EventQuery::after(0).pod(id))
            .unwrap()
            .events
    }

    fn states(&self, id: &str) -> Vec<PodState> {
        self.events(id).iter().map(|e| e.state).collect()
    }

    /// The state in the record on disk.
    fn disk_state(&self, id: &str) -> Option<PodState> {
        let path = self.dir.path().join("pods").join(format!("{id}.json"));
        let data = std::fs::read(path).ok()?;
        Some(serde_json::from_slice::<PodRecord>(&data).unwrap().state)
    }

    /// Writes a record, while no lifecycle is open.
    fn write_record(&self, record: &PodRecord) {
        let store = Store::open(self.dir.path(), 1 << 20).unwrap();
        store.save(record).unwrap();
        store.unlock();
    }

    fn clear(&self) {
        self.vm.clear_calls();
        self.hooks.clear();
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(crash) = &self.crash {
            crash.release();
        }
        for gate in [
            &self.vm.pause_gate,
            &self.vm.stop_gate,
            &self.vm.resume_gate,
        ] {
            gate.release();
        }
        if let Some(life) = &self.life {
            life.shutdown();
        }
    }
}

// ----------------------------------- wakes, leases, events and incarnations

#[tokio::test]
async fn concurrent_wakes_resume_once() {
    let h = Harness::asleep("pod").await;
    h.vm.resume_gate.hold();
    let wakes: Vec<_> = (0..12).map(|_| h.spawn_wake("pod")).collect();
    h.vm.resume_gate.entered(1).await;
    wait_until(|| h.info("pod").waiters == 12).await;
    h.vm.resume_gate.release();
    let mut resumed = 0;
    for wake in wakes {
        resumed += usize::from(wake.await.unwrap().unwrap().resumed);
    }
    assert_eq!(resumed, 1, "only the wake that resumed the pod says so");
    assert_eq!(h.vm.calls(), ["resume pod", "set-clock pod"]);
    assert_eq!(
        h.states("pod"),
        [Creating, Awake, Pausing, Asleep, Waking, Awake]
    );
    assert_eq!(h.info("pod").waiters, 0);
}

#[tokio::test]
async fn an_active_lease_prevents_hibernate_and_survives_a_restart() {
    let mut h = Harness::awake("pod").await;
    let request = HoldRequest {
        ttl_secs: Some(60.0),
        ..Default::default()
    };
    let lease = h.life().hold(&pod("pod"), request).unwrap();
    assert_eq!(lease.id.len(), 32);
    assert!(lease.id.bytes().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(lease.expires_ms, START_MS + 60_000);

    h.open().await;
    assert_eq!(
        h.info("pod").leases,
        std::slice::from_ref(&lease),
        "the restart lost the lease"
    );
    let refused = h.hibernate("pod").await.unwrap_err();
    assert_eq!(
        refused,
        LifecycleError::Refused(vec![Blocker::Lease {
            id: lease.id.clone(),
            holder: None,
            expires_ms: lease.expires_ms,
        }])
    );
    assert!(refused.to_string().contains("active lease"), "{refused}");

    h.life().release(&pod("pod"), &lease.id, None).unwrap();
    assert!(h.info("pod").leases.is_empty());
    for ttl in [0.0, 60.5, -1.0, f64::NAN] {
        let request = HoldRequest {
            ttl_secs: Some(ttl),
            ..Default::default()
        };
        let e = h.life().hold(&pod("pod"), request).unwrap_err();
        assert!(e.to_string().contains("lease TTL"), "{ttl}: {e}");
    }
    assert!(h.vm.calls().is_empty());
    h.hibernate("pod").await.unwrap();
}

#[tokio::test]
async fn a_wake_of_an_awake_pod_does_nothing() {
    let h = Harness::awake("pod").await;
    let report = h.wake("pod").await.unwrap();
    assert!(!report.resumed);
    assert!(h.vm.calls().is_empty());
    assert_eq!(h.state("pod"), Some(Awake));
}

#[tokio::test]
async fn a_wake_resumes_an_interrupted_hibernation() {
    let mut h = Harness::unopened();
    let mut record = PodRecord::new(identity("pod"), BOOT.into(), START_MS);
    record.state = Pausing;
    record.vm = Some(h.vm.set("pod", VmState::Pausing, true, true));
    h.write_record(&record);
    h.open().await;
    let report = h.wake("pod").await.unwrap();
    assert!(!report.resumed);
    assert_eq!(h.vm.calls(), ["resume pod", "set-clock pod"]);
    assert_eq!(h.state("pod"), Some(Awake));
}

/// An agent that does not answer yet is retried for the agent timeout.
#[tokio::test]
async fn an_agent_that_does_not_answer_yet_is_retried() {
    let h = Harness::asleep("pod").await;
    h.vm.with("pod", |m| m.ping_failures = 3);
    let report = h.wake("pod").await.unwrap();
    assert!(report.resumed);
    assert_eq!(h.vm.machine("pod").unwrap().pings, 4);
    assert!(report.steps.iter().any(|s| s.step == "ping"));
    assert_eq!(h.state("pod"), Some(Awake));
}

#[tokio::test]
async fn an_agent_that_never_answers_fails_the_pod() {
    let mut h = Harness::unopened();
    h.tune = Some(|c| c.agent_timeout = Duration::from_millis(200));
    h.open().await;
    h.create("pod").await.unwrap();
    h.hibernate("pod").await.unwrap();
    h.clear();
    h.vm.with("pod", |m| m.ping_failures = u32::MAX);
    let e = h.wake("pod").await.unwrap_err();
    assert!(
        matches!(&e, LifecycleError::Failed(why) if why.contains("did not answer")),
        "{e}"
    );
    assert_eq!(h.state("pod"), Some(Failed));
    assert_eq!(h.vm.calls(), ["resume pod", "stop pod"]);
    assert_eq!(h.hooks.calls().last().unwrap(), "failed pod");
    let failed = h.events("pod").pop().unwrap();
    assert_eq!((failed.state, failed.reason), (Failed, Reason::Error));
}

#[tokio::test]
async fn hibernate_checks_that_the_vm_process_exits() {
    // The VM process outlives the pause: it is stopped, then the pod sleeps.
    let h = Harness::awake("pod").await;
    h.vm.with("pod", |m| m.survive_pause = true);
    h.hibernate("pod").await.unwrap();
    assert_eq!(h.vm.calls(), ["pause pod", "stop-paused pod"]);
    assert_eq!(h.state("pod"), Some(Asleep));

    // It survives the stop too: the pod fails.
    let h = Harness::awake("pod").await;
    h.vm.with("pod", |m| {
        m.survive_pause = true;
        m.stuck = true;
    });
    let e = h.hibernate("pod").await.unwrap_err();
    assert!(
        matches!(&e, LifecycleError::Failed(why) if why.contains("survived the pause")),
        "{e}"
    );
    assert_eq!(h.vm.calls(), ["pause pod", "stop-paused pod"]);
    assert_eq!(h.state("pod"), Some(Failed));

    // smolvm reports no running VM process: refused before any pause.
    let h = Harness::awake("pod").await;
    h.vm.kill("pod");
    let e = h.hibernate("pod").await.unwrap_err();
    assert!(e.to_string().contains("no running VM process"), "{e}");
    assert!(h.vm.calls().is_empty());
    assert_eq!(h.state("pod"), Some(Awake));
}

#[tokio::test]
async fn events_have_cursors_filters_and_a_stream() {
    let mut h = Harness::unopened();
    // A crash during an append left a partial line.
    std::fs::write(
        h.dir.path().join("events.jsonl"),
        r#"{"seq":1,"sandbox_id":"po"#,
    )
    .unwrap();
    h.open().await;
    h.create("pod").await.unwrap();
    h.create("other").await.unwrap();
    h.hibernate("pod").await.unwrap();

    let page = h.life().events(&EventQuery::after(0).pod("pod")).unwrap();
    let seqs: Vec<_> = page.events.iter().map(|e| e.seq).collect();
    let states: Vec<_> = page.events.iter().map(|e| e.state).collect();
    assert_eq!(seqs, [1, 2, 5, 6]);
    assert_eq!(states, [Creating, Awake, Pausing, Asleep]);
    assert_eq!(page.cursor, 6);
    let asleep = &page.events[3];
    assert_eq!(asleep.from, Some(Pausing));
    assert_eq!(asleep.reason, Reason::Idle);
    assert_eq!(asleep.checkpoint_bytes, Some(CHECKPOINT_BYTES));
    assert_eq!(
        asleep.save,
        Some(Save {
            mode: SaveMode::Full,
            written_bytes: CHECKPOINT_BYTES,
        })
    );
    assert!(asleep.duration_ms.is_some());
    let steps: Vec<_> = asleep.steps.iter().map(|s| s.step.as_str()).collect();
    assert_eq!(steps, ["slot", "pausing", "stop-pumps", "pause"]);

    let page = h.life().events(&EventQuery::after(2).pod("pod")).unwrap();
    assert_eq!(page.events.len(), 2);
    let page = h.life().events(&EventQuery::after(6)).unwrap();
    assert!(page.events.is_empty());
    assert_eq!(page.cursor, 6);

    let mut stream = h.life().subscribe();
    h.wake("pod").await.unwrap();
    let waking = stream.recv().await.unwrap();
    let awake = stream.recv().await.unwrap();
    assert_eq!((waking.state, awake.state), (Waking, Awake));
    assert_eq!(awake.seq, 8);
    let clock = awake.clock.unwrap();
    assert!(clock.stepped && clock.skew_ns < 0);
    let steps: Vec<_> = awake.steps.iter().map(|s| s.step.as_str()).collect();
    assert_eq!(
        steps,
        [
            "admit",
            "slot",
            "resume",
            "tso",
            "ping",
            "clock",
            "deferred_kills",
            "before_release",
            "release"
        ]
    );
}

/// An incarnation that a call names must be the pod's.
#[tokio::test]
async fn a_stale_incarnation_cannot_mutate() {
    let h = Harness::awake("pod").await;
    let stale = || Some("old".to_string());
    let life = h.life();
    let mut wake = WakeRequest::new(Reason::Api);
    wake.incarnation = stale();
    let mut sleep = HibernateRequest::new(Reason::Api);
    sleep.incarnation = stale();
    let hold = HoldRequest {
        incarnation: stale(),
        ..Default::default()
    };
    let refused = LifecycleError::StaleIncarnation("pod".into());
    assert_eq!(life.wake(&pod("pod"), wake).await.unwrap_err(), refused);
    assert_eq!(
        life.hibernate(&pod("pod"), sleep.clone())
            .await
            .unwrap_err(),
        refused
    );
    assert_eq!(life.hold(&pod("pod"), hold).unwrap_err(), refused);
    assert_eq!(
        life.release(&pod("pod"), "lease", Some("old")).unwrap_err(),
        refused
    );
    assert_eq!(
        life.renew(&pod("pod"), "lease", None, Some("old"))
            .unwrap_err(),
        refused
    );
    assert_eq!(
        life.delete(&pod("pod"), Some("old")).await.unwrap_err(),
        refused
    );
    assert_eq!(h.state("pod"), Some(Awake));
    assert!(h.vm.calls().is_empty());

    sleep.incarnation = Some(h.info("pod").identity.incarnation);
    life.hibernate(&pod("pod"), sleep).await.unwrap();
}

// ------------------------------------------------------- pause and wake

#[tokio::test]
async fn hibernate_writes_the_record_before_and_after_and_pauses_once() {
    let h = Harness::awake("pod").await;
    h.vm.pause_gate.hold();
    let sleeping = h.spawn_hibernate("pod");
    h.vm.pause_gate.entered(1).await;
    assert_eq!(h.disk_state("pod"), Some(Pausing));
    assert_eq!(h.info("pod").phase, Some(PausePhase::Saving));
    h.vm.pause_gate.release();
    let report = sleeping.await.unwrap().unwrap();
    assert!(!report.already_asleep);
    assert_eq!(report.checkpoint_bytes, Some(CHECKPOINT_BYTES));
    assert_eq!(report.save.map(|s| s.mode), Some(SaveMode::Full));
    assert_eq!(h.disk_state("pod"), Some(Asleep));

    assert!(h.hibernate("pod").await.unwrap().already_asleep);
    assert_eq!(h.vm.calls(), ["pause pod"]);
    assert_eq!(
        h.hooks.calls(),
        ["pausing-idle pod", "stop-pumps pod", "asleep pod"]
    );
    let info = h.info("pod");
    assert_eq!((info.state, info.vm, info.phase), (Asleep, None, None));
    assert!(info.last_sleep.is_some());
}

#[tokio::test]
async fn a_wake_cancels_a_pause_that_is_saving() {
    let h = Harness::awake("pod").await;
    let before = h.vm.machine("pod").unwrap().process;
    h.vm.pause_gate.hold();
    let sleeping = h.spawn_hibernate("pod");
    h.vm.pause_gate.entered(1).await;
    // A SIGTERM during the pause is recorded at once.
    let deferred = h.life().kill("pod", kill("app", None, 15)).await.unwrap();
    assert_eq!(deferred, KillOutcome::Deferred { exit_status: 143 });

    let woke = h.wake("pod").await.unwrap();
    assert!(woke.cancelled_pause && !woke.resumed);
    assert_eq!(
        sleeping.await.unwrap().unwrap_err(),
        LifecycleError::Cancelled
    );
    assert_eq!(
        h.vm.calls(),
        ["pause pod", "set-clock pod", "signal pod app 9"]
    );
    assert_eq!(h.vm.machine("pod").unwrap().process, before);
    assert_eq!(h.info("pod").vm, before);
    assert_eq!(h.state("pod"), Some(Awake));
    assert!(h.life().record("pod").unwrap().deferred_kills.is_empty());
    assert_eq!(h.states("pod")[2..], [Pausing, Awake]);
    assert_eq!(
        h.hooks.calls(),
        [
            "pausing-idle pod",
            "stop-pumps pod",
            "before-release pod",
            "release pod",
            "awake-PauseCancelled pod"
        ]
    );
    let last = h.events("pod").pop().unwrap();
    assert_eq!((last.from, last.state), (Some(Pausing), Awake));
    assert!(last.cancelled && last.clock.is_some());
}

/// The pause's cancel reaches smolvm through the canceller that the VM
/// operations register, also when the cancel came first.
#[test]
fn a_cancel_calls_the_registered_canceller_once() {
    let calls = Arc::new(AtomicU64::new(0));
    let control = PauseControl::new(Faults::none());
    let counted = calls.clone();
    control.on_cancel(move || {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    assert!(control.cancel());
    let deadline = Instant::now() + Duration::from_secs(5);
    while calls.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let late = PauseControl::new(Faults::none());
    assert!(late.cancel());
    let counted = calls.clone();
    late.on_cancel(move || {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn wakes_during_a_saving_pause_merge_without_a_resume() {
    let h = Harness::awake("pod").await;
    h.vm.pause_gate.hold();
    let sleeping = h.spawn_hibernate("pod");
    h.vm.pause_gate.entered(1).await;
    let wakes: Vec<_> = (0..12).map(|_| h.spawn_wake("pod")).collect();
    for wake in wakes {
        wake.await.unwrap().unwrap();
    }
    assert!(sleeping.await.unwrap().is_err());
    assert_eq!(h.vm.calls(), ["pause pod", "set-clock pod"]);
    assert_eq!(h.state("pod"), Some(Awake));
}

#[tokio::test]
async fn a_wake_after_the_pause_is_durable_resumes_once_it_stops() {
    let h = Harness::awake("pod").await;
    h.vm.stop_gate.hold();
    let sleeping = h.spawn_hibernate("pod");
    h.vm.stop_gate.entered(1).await;
    assert_eq!(h.info("pod").phase, Some(PausePhase::Stopping));
    let waking: Vec<_> = (0..3).map(|_| h.spawn_wake("pod")).collect();
    wait_until(|| h.info("pod").waiters == 3).await;
    h.vm.stop_gate.release();
    assert!(!sleeping.await.unwrap().unwrap().already_asleep);
    for wake in waking {
        wake.await.unwrap().unwrap();
    }
    assert_eq!(h.vm.calls(), ["pause pod", "resume pod", "set-clock pod"]);
    assert_eq!(h.states("pod")[2..], [Pausing, Asleep, Waking, Awake]);
    assert_eq!(h.state("pod"), Some(Awake));
}

#[tokio::test]
async fn pauses_across_pods_are_limited_by_the_slots() {
    let h = Harness::new().await;
    for id in ["a", "b", "c"] {
        h.create(id).await.unwrap();
    }
    h.vm.pause_gate.hold();
    let sleeping: Vec<_> = ["a", "b", "c"]
        .iter()
        .map(|id| h.spawn_hibernate(id))
        .collect();
    h.vm.pause_gate.entered(2).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(h.vm.pause_gate.count(), 2, "a third pause started");
    h.vm.pause_gate.release();
    for sleep in sleeping {
        sleep.await.unwrap().unwrap();
    }
    assert_eq!(h.vm.max_pausing(), 2);
}

#[tokio::test]
async fn a_failed_pause_leaves_the_pod_awake_with_the_error() {
    let h = Harness::awake("pod").await;
    h.vm.with("pod", |m| m.fail_pause = Some("no space left".into()));
    let e = h.hibernate("pod").await.unwrap_err();
    assert_eq!(e, LifecycleError::Vm("no space left".into()));
    assert_eq!(h.state("pod"), Some(Awake));
    assert_eq!(h.vm.calls(), ["pause pod", "set-clock pod"]);
    assert_eq!(h.hooks.calls().last().unwrap(), "awake-PauseFailed pod");
    let event = h.events("pod").pop().unwrap();
    assert_eq!(event.state, Awake);
    assert_eq!(event.error.as_deref(), Some("no space left"));
}

#[tokio::test]
async fn pumps_that_do_not_stop_refuse_the_pause() {
    let h = Harness::awake("pod").await;
    *lock(&h.hooks.fail_stop_pumps) = Some("a pump is stuck".into());
    let e = h.hibernate("pod").await.unwrap_err();
    assert!(e.to_string().contains("a pump is stuck"), "{e}");
    assert!(!h.vm.calls().contains(&"pause pod".to_string()));
    assert_eq!(h.state("pod"), Some(Awake));
}

#[tokio::test]
async fn the_pausing_hook_runs_before_the_pumps_with_the_reason() {
    let h = Harness::awake("pod").await;
    h.life()
        .hibernate(&pod("pod"), HibernateRequest::new(Reason::Api))
        .await
        .unwrap();
    assert_eq!(
        h.hooks.calls(),
        ["pausing-api pod", "stop-pumps pod", "asleep pod"]
    );
}

#[tokio::test]
async fn a_pausing_hook_that_refuses_keeps_the_pod_awake_and_releases() {
    let h = Harness::awake("pod").await;
    *lock(&h.hooks.fail_pausing) = Some("traffic since the idle decision".into());
    let e = h.hibernate("pod").await.unwrap_err();
    assert_eq!(
        e,
        LifecycleError::PauseRefused("traffic since the idle decision".into())
    );
    assert_eq!(e.to_string(), "pausing: traffic since the idle decision");
    assert_eq!(h.state("pod"), Some(Awake));
    assert_eq!(h.vm.calls(), ["set-clock pod"]);
    assert_eq!(
        h.hooks.calls(),
        [
            "pausing-idle pod",
            "before-release pod",
            "release pod",
            "awake-PauseFailed pod"
        ]
    );
    let event = h.events("pod").pop().unwrap();
    assert_eq!((event.from, event.state), (Some(Pausing), Awake));
    assert!(event.error.unwrap().contains("traffic since"));
}

#[tokio::test]
async fn a_warning_of_the_before_release_hook_is_the_wake_event_s_warning() {
    let h = Harness::asleep("pod").await;
    *lock(&h.hooks.release_warning) = Some("trust stale: containers app".into());
    let report = h.wake("pod").await.unwrap();
    assert!(report.resumed);
    let awake = h.events("pod").pop().unwrap();
    assert_eq!(awake.state, Awake);
    assert_eq!(awake.error, None);
    assert_eq!(
        awake.warning.as_deref(),
        Some("trust stale: containers app")
    );
}

#[tokio::test]
async fn a_wake_releases_last_and_records_a_failed_release() {
    let h = Harness::asleep("pod").await;
    *lock(&h.hooks.fail_release) = Some("tap0 is gone".into());
    let report = h.wake("pod").await.unwrap();
    assert!(report.resumed);
    assert_eq!(report.steps.last().unwrap().step, "release");
    assert_eq!(h.state("pod"), Some(Awake));
    assert_eq!(
        h.hooks.calls(),
        [
            "may-wake pod",
            "before-release pod",
            "release pod",
            "awake-Woke pod"
        ]
    );
    let awake = h.events("pod").pop().unwrap();
    assert_eq!(awake.state, Awake);
    assert_eq!(awake.error.as_deref(), Some("release: tap0 is gone"));
}

/// A wake while the pause waits for its slot cancels it before `pausing`
/// runs, and the release still runs.
#[tokio::test]
async fn a_pause_cancelled_before_its_hold_still_releases() {
    let mut h = Harness::unopened();
    h.tune = Some(|c| c.pause_slots = 1);
    h.open().await;
    h.create("a").await.unwrap();
    h.create("pod").await.unwrap();
    h.vm.pause_gate.hold();
    let first = h.spawn_hibernate("a");
    h.vm.pause_gate.entered(1).await;
    let waiting = h.spawn_hibernate("pod");
    wait_until(|| h.state("pod") == Some(Pausing)).await;
    h.hooks.clear();
    let waking = h.spawn_wake("pod");
    wait_until(|| h.info("pod").waiters == 1).await;
    // The actor takes the wake at once; give it a moment before the slot frees.
    tokio::time::sleep(Duration::from_millis(50)).await;
    h.vm.pause_gate.release();
    first.await.unwrap().unwrap();
    assert!(waking.await.unwrap().unwrap().cancelled_pause);
    assert_eq!(
        waiting.await.unwrap().unwrap_err(),
        LifecycleError::Cancelled
    );
    let calls: Vec<_> = h
        .hooks
        .calls()
        .into_iter()
        .filter(|c| c.ends_with(" pod"))
        .collect();
    assert_eq!(
        calls,
        [
            "before-release pod",
            "release pod",
            "awake-PauseCancelled pod"
        ]
    );
}

#[tokio::test]
async fn a_failed_resume_keeps_the_pod_asleep_until_three_in_a_row() {
    let h = Harness::asleep("pod").await;
    h.vm.with("pod", |m| m.resume_failures = 3);
    for failures in 1..=2 {
        let e = h.wake("pod").await.unwrap_err();
        assert_eq!(e, LifecycleError::Vm("the VM did not start".into()));
        assert_eq!(h.state("pod"), Some(Asleep));
        assert_eq!(h.info("pod").wake_failures, failures);
        assert_eq!(
            h.info("pod").failure.as_deref(),
            Some("the VM did not start")
        );
    }
    let e = h.wake("pod").await.unwrap_err();
    assert!(
        matches!(&e, LifecycleError::Failed(why) if why.contains("3 times in a row")),
        "{e}"
    );
    assert_eq!(h.state("pod"), Some(Failed));

    let h = Harness::asleep("pod").await;
    h.vm.with("pod", |m| m.resume_failures = 1);
    h.wake("pod").await.unwrap_err();
    h.wake("pod").await.unwrap();
    assert_eq!(h.info("pod").wake_failures, 0);
    assert_eq!(h.info("pod").failure, None);
}

#[tokio::test]
async fn a_rosetta_vm_without_its_tso_marker_is_stopped_and_fails() {
    let h = Harness::new().await;
    let mut rosetta = identity("pod");
    rosetta.rosetta = true;
    h.create_with(rosetta).await.unwrap();
    h.hibernate("pod").await.unwrap();
    h.wake("pod").await.unwrap();
    h.hibernate("pod").await.unwrap();
    h.clear();

    h.vm.with("pod", |m| m.tso_on_resume = false);
    let e = h.wake("pod").await.unwrap_err();
    assert!(
        matches!(&e, LifecycleError::Failed(why) if why.contains("TSO")),
        "{e}"
    );
    assert_eq!(h.vm.calls(), ["resume pod", "stop pod"]);
    assert_eq!(h.state("pod"), Some(Failed));
}

#[tokio::test]
async fn the_admission_hook_can_refuse_a_wake() {
    let h = Harness::asleep("pod").await;
    *lock(&h.hooks.refuse_wake) = Some("no memory".into());
    let e = h.wake("pod").await.unwrap_err();
    assert_eq!(
        e,
        LifecycleError::Refused(vec![Blocker::Admission {
            reason: "no memory".into()
        }])
    );
    assert_eq!(h.state("pod"), Some(Asleep));
    assert!(h.vm.calls().is_empty());
    let refused = h.events("pod").pop().unwrap();
    assert_eq!((refused.from, refused.state), (Some(Asleep), Asleep));
    assert_eq!(refused.error.as_deref(), Some("wake refused: no memory"));
}

/// A wake that waits for admission has an event from `asleep` to `asleep`
/// that says why, before the wake's own events.
#[tokio::test]
async fn a_wake_that_waits_for_admission_has_an_event() {
    let h = Harness::asleep("pod").await;
    *lock(&h.hooks.wait_first) = Some("awake pods 2/2".into());
    h.wake("pod").await.unwrap();
    let events = h.events("pod");
    let last: Vec<_> = events[events.len() - 3..]
        .iter()
        .map(|e| (e.from, e.state, e.waiting.as_deref(), e.change()))
        .collect();
    assert_eq!(
        last,
        [
            (
                Some(Asleep),
                Asleep,
                Some("awake pods 2/2"),
                Change::WakeWaiting
            ),
            (Some(Asleep), Waking, None, Change::Other),
            (Some(Waking), Awake, None, Change::Woke),
        ]
    );
    let waiting = &events[events.len() - 3];
    assert_eq!(
        (waiting.reason.kind(), waiting.error.as_deref()),
        ("api", None)
    );
}

#[tokio::test]
async fn the_release_hook_times_the_held_traffic() {
    let h = Harness::asleep("pod").await;
    *lock(&h.hooks.held_for) = Some(Duration::from_micros(182_400));
    h.wake("pod").await.unwrap();
    let awake = h.events("pod").pop().unwrap();
    assert_eq!((awake.from, awake.state), (Some(Waking), Awake));
    assert_eq!(awake.capture_to_release_ms, Some(182.4));
}

#[tokio::test]
async fn a_note_records_why_a_pod_stays_in_its_state() {
    let h = Harness::awake("pod").await;
    let generation = h.info("pod").generation;
    let why = "not sleeping: the pod is not Ready".to_string();
    assert_eq!(
        h.life().note("pod", Awake, Reason::Idle, why.clone()),
        Ok(true)
    );
    assert_eq!(
        h.life().note("pod", Asleep, Reason::Idle, why.clone()),
        Ok(false)
    );
    let note = h.events("pod").pop().unwrap();
    assert_eq!((note.from, note.state), (Some(Awake), Awake));
    assert_eq!((note.reason, note.error), (Reason::Idle, Some(why)));
    assert_eq!(note.generation, generation);
    assert_eq!(h.info("pod").generation, generation);
}

// ------------------------------------------------------ what blocks a pause

#[tokio::test]
async fn execs_and_creates_block_a_pause() {
    let h = Harness::awake("pod").await;
    let exec = ExecRecord {
        container: "app".into(),
        exec: "e1".into(),
        pid: 1001,
        stdio: StdioPaths::default(),
        status: ExecStatus::Running,
        exit_status: None,
    };
    h.life().put_exec("pod", exec).unwrap();
    let e = h.hibernate("pod").await.unwrap_err();
    assert!(e.to_string().contains("exec running: app/e1"), "{e}");
    h.life().exec_exited("pod", "app", "e1", 0).unwrap();

    let starting = h.life().begin("pod", Activity::Exec).unwrap();
    let e = h.hibernate("pod").await.unwrap_err();
    assert_eq!(e, LifecycleError::Refused(vec![Blocker::ExecStarting]));
    assert!(e.to_string().contains("exec being created"), "{e}");
    drop(starting);
    let creating = h.life().begin("pod", Activity::ContainerCreate).unwrap();
    let e = h.hibernate("pod").await.unwrap_err();
    assert!(e.to_string().contains("container being created"), "{e}");
    drop(creating);
    assert!(h.vm.calls().is_empty());
    h.hibernate("pod").await.unwrap();
}

#[tokio::test]
async fn a_recorded_exec_blocks_a_pause_after_a_restart() {
    let mut h = Harness::awake("pod").await;
    let exec = ExecRecord {
        container: "app".into(),
        exec: "e1".into(),
        pid: 1001,
        stdio: StdioPaths::default(),
        status: ExecStatus::Created,
        exit_status: None,
    };
    h.life().put_exec("pod", exec).unwrap();
    h.open().await;
    let e = h.hibernate("pod").await.unwrap_err();
    assert!(e.to_string().contains("exec created: app/e1"), "{e}");
    h.life().remove_exec("pod", "app", "e1").unwrap();
    h.hibernate("pod").await.unwrap();
}

#[tokio::test]
async fn an_exec_wakes_a_sleeping_pod_and_holds_it_awake() {
    let h = Harness::asleep("pod").await;
    let (guard, report) = h.life().wake_for("pod", Activity::Exec).await.unwrap();
    assert!(report.resumed);
    let waking = h.events("pod").into_iter().find(|e| e.state == Waking);
    assert_eq!(waking.unwrap().reason, Reason::Exec);
    assert!(h.hibernate("pod").await.is_err());
    drop(guard);
    h.hibernate("pod").await.unwrap();
}

#[tokio::test]
async fn the_hibernate_annotation_opts_a_pod_out() {
    let h = Harness::new().await;
    let spec = serde_json::json!({ "annotations": { ANN_HIBERNATE: "false" } });
    let mut never = identity("pod");
    never.hibernate_disabled = hibernation_disabled(Some(&spec), false);
    h.create_with(never).await.unwrap();
    let e = h.hibernate("pod").await.unwrap_err();
    assert_eq!(e, LifecycleError::Refused(vec![Blocker::Disabled]));
    assert!(e.to_string().contains(ANN_HIBERNATE), "{e}");
    assert!(h.vm.calls().is_empty());
}

#[tokio::test]
async fn a_wake_holds_off_a_pause_for_the_grace_time() {
    let mut h = Harness::unopened();
    h.tune = Some(|c| c.wake_grace = Duration::from_secs(5));
    h.open().await;
    h.create("pod").await.unwrap();
    h.hibernate("pod").await.unwrap();
    h.wake("pod").await.unwrap();
    let e = h.hibernate("pod").await.unwrap_err();
    assert!(e.to_string().contains("wake waiter"), "{e}");
    h.advance(Duration::from_secs(5));
    h.hibernate("pod").await.unwrap();

    // The grace holds off only a pause for being idle, not one a client
    // asks for.
    h.wake("pod").await.unwrap();
    h.life()
        .hibernate(&pod("pod"), HibernateRequest::new(Reason::Api))
        .await
        .unwrap();
}

#[tokio::test]
async fn expired_leases_are_dropped_when_a_pause_is_considered() {
    let h = Harness::awake("pod").await;
    let request = HoldRequest {
        ttl_secs: Some(1.0),
        ..Default::default()
    };
    h.life().hold(&pod("pod"), request).unwrap();
    assert!(h.hibernate("pod").await.is_err());
    h.advance(Duration::from_secs(2));
    assert!(h.info("pod").leases.is_empty());
    assert_eq!(h.life().record("pod").unwrap().leases.len(), 1);
    h.hibernate("pod").await.unwrap();
    assert!(h.life().record("pod").unwrap().leases.is_empty());
}

#[tokio::test]
async fn a_holder_renews_its_lease_and_a_hold_does_not_wake() {
    let h = Harness::asleep("pod").await;
    let request = HoldRequest {
        ttl_secs: Some(10.0),
        holder: Some("worker".into()),
        ..Default::default()
    };
    let first = h.life().hold(&pod("pod"), request.clone()).unwrap();
    h.advance(Duration::from_secs(5));
    let again = h.life().hold(&pod("pod"), request).unwrap();
    assert_eq!(again.id, first.id);
    assert_eq!(again.expires_ms, first.expires_ms + 5_000);
    let named = HoldRequest {
        id: Some("work".into()),
        ttl_secs: Some(10.0),
        ..Default::default()
    };
    assert_eq!(
        h.life().hold(&pod("pod"), named.clone()).unwrap().id,
        "work"
    );
    assert_eq!(h.life().hold(&pod("pod"), named).unwrap().id, "work");
    assert_eq!(h.info("pod").leases.len(), 2);
    h.life().release(&pod("pod"), "work", None).unwrap();
    let renewed = h
        .life()
        .renew(&pod("pod"), &first.id, Some(20.0), None)
        .unwrap();
    assert_eq!(renewed.expires_ms, START_MS + 25_000);
    assert_eq!(renewed.holder.as_deref(), Some("worker"));
    assert_eq!(h.info("pod").leases.len(), 1);
    assert_eq!(h.state("pod"), Some(Asleep));
    assert!(h.vm.calls().is_empty());

    h.advance(Duration::from_secs(30));
    let e = h
        .life()
        .renew(&pod("pod"), &first.id, None, None)
        .unwrap_err();
    assert!(matches!(e, LifecycleError::NotFound(_)), "{e}");
}

#[tokio::test]
async fn a_wake_with_a_lease_keeps_the_pod_awake() {
    let h = Harness::asleep("pod").await;
    let mut request = WakeRequest::new(Reason::Traffic {
        from: "10.42.0.9".into(),
        via: None,
        port: 8080,
    });
    request.lease_ttl = Some(30.0);
    let report = h.life().wake(&pod("pod"), request).await.unwrap();
    let lease = report.lease.unwrap();
    assert_eq!(h.info("pod").leases, [lease]);
    assert!(h.hibernate("pod").await.is_err());
    let waking = h.events("pod").into_iter().find(|e| e.state == Waking);
    assert!(matches!(
        waking.unwrap().reason,
        Reason::Traffic { port: 8080, .. }
    ));
}

// ----------------------------------------------------- kills and exits

#[tokio::test]
async fn kills_while_asleep_are_deferred_to_the_next_wake() {
    let h = Harness::asleep("pod").await;
    let life = h.life();
    assert_eq!(
        life.kill("pod", kill("app", None, 15)).await.unwrap(),
        KillOutcome::Deferred { exit_status: 143 }
    );
    assert_eq!(
        life.kill("pod", kill("app", Some("e1"), 9)).await.unwrap(),
        KillOutcome::Deferred { exit_status: 137 }
    );
    assert_eq!(h.state("pod"), Some(Asleep));
    assert!(h.vm.calls().is_empty());
    assert_eq!(life.record("pod").unwrap().deferred_kills.len(), 2);

    h.wake("pod").await.unwrap();
    assert_eq!(
        h.vm.calls(),
        [
            "resume pod",
            "set-clock pod",
            "signal pod app 9",
            "signal pod app/e1 9"
        ]
    );
    assert!(life.record("pod").unwrap().deferred_kills.is_empty());

    h.clear();
    let all = KillRequest {
        all: true,
        ..kill("app", None, 15)
    };
    assert_eq!(life.kill("pod", all).await.unwrap(), KillOutcome::Delivered);
    assert_eq!(h.vm.calls(), ["signal pod app 15 all"]);
}

#[tokio::test]
async fn other_signals_wake_a_sleeping_pod() {
    let h = Harness::asleep("pod").await;
    let outcome = h.life().kill("pod", kill("app", None, 10)).await.unwrap();
    assert_eq!(outcome, KillOutcome::Delivered);
    assert_eq!(
        h.vm.calls(),
        ["resume pod", "set-clock pod", "signal pod app 10"]
    );
    let waking = h.events("pod").into_iter().find(|e| e.state == Waking);
    assert_eq!(waking.unwrap().reason, Reason::Signal);

    // With wake-on-terminate, SIGTERM wakes too.
    let h = Harness::new().await;
    let mut wakes = identity("pod");
    wakes.wake_on_terminate = true;
    h.create_with(wakes).await.unwrap();
    h.hibernate("pod").await.unwrap();
    h.clear();
    let outcome = h.life().kill("pod", kill("app", None, 15)).await.unwrap();
    assert_eq!(outcome, KillOutcome::Delivered);
    assert_eq!(h.state("pod"), Some(Awake));
}

#[tokio::test]
async fn a_vm_exit_outside_a_pause_is_a_crash() {
    let h = Harness::awake("pod").await;
    let process = h.info("pod").vm.unwrap();
    let other = VmProcess {
        pid: 1,
        start_time: Some(1),
    };
    assert_eq!(h.life().vm_exited("pod", other).await, VmExit::Stale);
    assert_eq!(h.life().vm_exited("pod", process).await, VmExit::Stale);
    h.vm.kill("pod");
    assert_eq!(h.life().vm_exited("pod", process).await, VmExit::Crash);
    assert_eq!(h.state("pod"), Some(Failed));
    assert_eq!(
        h.info("pod").failure.as_deref(),
        Some("the VM process exited")
    );
    assert_eq!(h.hooks.calls(), ["failed pod"]);
    assert_eq!(h.life().vm_exited("pod", process).await, VmExit::Crash);

    let h = Harness::awake("pod").await;
    let process = h.info("pod").vm.unwrap();
    h.hibernate("pod").await.unwrap();
    assert_eq!(
        h.life().vm_exited("pod", process).await,
        VmExit::Hibernation
    );
    assert_eq!(h.life().vm_exited("gone", process).await, VmExit::Deleted);
}

// ----------------------------------------------------- create and delete

#[tokio::test]
async fn a_failed_boot_deletes_the_machine_and_drops_the_record() {
    let h = Harness::new().await;
    let e = h
        .life()
        .create(identity("pod"), async { Err("boot failed".to_string()) })
        .await
        .unwrap_err();
    assert_eq!(e, LifecycleError::Vm("boot failed".into()));
    assert_eq!(h.vm.calls(), ["delete pod"]);
    assert_eq!(h.hooks.calls(), ["deleted pod"]);
    assert!(h.life().get(&pod("pod")).is_none());
    assert_eq!(h.disk_state("pod"), None);
    assert_eq!(h.states("pod"), [Creating, Deleted]);

    let first = h.create("pod").await.unwrap();
    assert_eq!(first.state, Awake);
    let again = h.create("pod").await.unwrap();
    assert_eq!(again.identity.incarnation, first.identity.incarnation);
    let mut other = identity("pod");
    other.uid = "another".into();
    let e = h.create_with(other).await.unwrap_err();
    assert!(e.to_string().contains("another pod"), "{e}");
    let e = h
        .life()
        .create(identity("../x"), async { Ok(None) })
        .await
        .unwrap_err();
    assert!(matches!(e, LifecycleError::Invalid(_)), "{e}");
}

#[tokio::test]
async fn a_boot_warning_is_in_the_event_of_the_awake_pod() {
    let h = Harness::new().await;
    let vm = h.vm.clone();
    h.life()
        .create(identity("pod"), async move {
            vm.add_running("pod");
            Ok(Some("no Rosetta: the TSO canary failed".into()))
        })
        .await
        .unwrap();
    let awake = h.events("pod").pop().unwrap();
    assert_eq!((awake.from, awake.state), (Some(Creating), Awake));
    assert_eq!(
        awake.warning.as_deref(),
        Some("no Rosetta: the TSO canary failed")
    );
}

#[tokio::test]
async fn a_wake_during_the_boot_waits_for_it() {
    let h = Harness::new().await;
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let (vm, life) = (h.vm.clone(), h.life().clone());
    let creating = tokio::spawn(async move {
        life.create(identity("pod"), async move {
            let _ = released.await;
            vm.add_running("pod");
            Ok(None)
        })
        .await
    });
    wait_until(|| h.life().get(&pod("pod")).is_some()).await;
    assert_eq!(h.state("pod"), Some(Creating));
    let waking = h.spawn_wake("pod");
    wait_until(|| h.info("pod").waiters == 1).await;
    release.send(()).unwrap();
    creating.await.unwrap().unwrap();
    assert!(!waking.await.unwrap().unwrap().resumed);
    assert!(h.vm.calls().is_empty());
}

#[tokio::test]
async fn deleting_a_sleeping_pod_does_not_wake_it() {
    let h = Harness::asleep("pod").await;
    h.life().delete(&pod("pod"), None).await.unwrap();
    assert_eq!(h.vm.calls(), ["delete pod"]);
    assert_eq!(h.hooks.calls(), ["deleted pod"]);
    assert!(h.life().get(&pod("pod")).is_none());
    assert_eq!(h.disk_state("pod"), None);
    assert_eq!(h.states("pod").last(), Some(&Deleted));
    h.life().delete(&pod("pod"), None).await.unwrap();
    assert_eq!(
        h.wake("pod").await.unwrap_err(),
        LifecycleError::NotFound("with sandbox id pod".into())
    );
}

#[tokio::test]
async fn pods_are_found_by_ip_and_listed_by_sandbox_id() {
    let h = Harness::awake("pod").await;
    h.create("other").await.unwrap();
    h.life()
        .set_network(
            "pod",
            Some("10.42.0.9".into()),
            Some("02:00:0a:2a:00:09".into()),
        )
        .unwrap();
    let found = h.life().get(&PodRef::Ip("10.42.0.9".into()));
    assert_eq!(found.map(|p| p.identity.sandbox_id).as_deref(), Some("pod"));
    assert!(h.life().get(&PodRef::Ip("10.42.0.1".into())).is_none());
    let ids: Vec<_> = h
        .life()
        .list()
        .into_iter()
        .map(|p| p.identity.sandbox_id)
        .collect();
    assert_eq!(ids, ["other", "pod"]);
}

#[tokio::test]
async fn tasks_are_kept_in_the_record() {
    let mut h = Harness::awake("pod").await;
    let task = TaskRecord {
        id: "app".into(),
        kind: TaskKind::Container,
        pid: 1002,
        bundle: "/run/containerd/app".into(),
        stdio: StdioPaths {
            stdout: "/run/app.out".into(),
            ..Default::default()
        },
        started: true,
        exit_status: None,
    };
    h.life().put_task("pod", task.clone()).unwrap();
    h.open().await;
    let record = h.life().record("pod").unwrap();
    assert_eq!(record.tasks["app"], task);
    h.life().remove_task("pod", "app").unwrap();
    assert!(h.life().record("pod").unwrap().tasks.is_empty());
}

#[tokio::test]
async fn guest_deletes_are_kept_in_the_record_until_the_guest_did_them() {
    let mut h = Harness::awake("pod").await;
    let task = TaskRecord {
        id: "app".into(),
        kind: TaskKind::Container,
        pid: 1002,
        bundle: String::new(),
        stdio: StdioPaths::default(),
        started: true,
        exit_status: None,
    };
    h.life().put_task("pod", task).unwrap();
    let exec = ExecRecord {
        container: "app".into(),
        exec: "e1".into(),
        pid: 1003,
        stdio: StdioPaths::default(),
        status: ExecStatus::Exited,
        exit_status: Some(0),
    };
    h.life().put_exec("pod", exec).unwrap();
    h.hibernate("pod").await.unwrap();

    let life = h.life();
    life.remove_for_guest("pod", "app", Some("e1")).unwrap();
    life.remove_for_guest("pod", "app", None).unwrap();
    life.remove_for_guest("pod", "app", None).unwrap();
    let container = GuestDelete {
        container: "app".into(),
        exec: None,
    };
    let exec = GuestDelete {
        exec: Some("e1".into()),
        ..container.clone()
    };
    h.open().await;
    let record = h.life().record("pod").unwrap();
    assert!(record.tasks.is_empty() && record.execs.is_empty());
    assert_eq!(record.guest_deletes, [exec.clone(), container.clone()]);

    h.life().guest_deleted("pod", &[exec]).unwrap();
    assert_eq!(h.life().record("pod").unwrap().guest_deletes, [container]);
}

#[tokio::test]
async fn a_second_lifecycle_cannot_open_the_directory() {
    let h = Harness::new().await;
    let e = Lifecycle::open(h.config(), h.vm.clone(), h.hooks.clone())
        .await
        .err()
        .unwrap();
    assert!(e.to_string().contains("another node service"), "{e}");
}

// ------------------------------------------------------------- recovery

struct Row {
    record: PodState,
    /// smolvm's state, whether the VM process runs, and whether a checkpoint
    /// is present; none for no machine.
    vm: Option<(VmState, bool, bool)>,
    /// None: the record is removed.
    want: Option<PodState>,
    calls: &'static [&'static str],
    hooks: &'static [&'static str],
}

/// Every row of the recovery table, in its order.
#[tokio::test]
async fn recovery_after_a_crash_at_every_step() {
    use super::VmState::{Paused, Pausing as Saving, Running};
    let awake_hooks = &["before-release pod", "release pod", "awake-Recovered pod"];
    let rows = [
        Row {
            record: Creating,
            vm: Some((Running, true, false)),
            want: None,
            calls: &["delete pod"],
            hooks: &["deleted pod"],
        },
        Row {
            record: Awake,
            vm: Some((Running, true, false)),
            want: Some(Awake),
            calls: &[],
            hooks: &["awake-Recovered pod"],
        },
        Row {
            record: Awake,
            vm: Some((Running, false, false)),
            want: Some(Failed),
            calls: &[],
            hooks: &["failed pod"],
        },
        Row {
            record: Pausing,
            vm: Some((Running, true, false)),
            want: Some(Awake),
            calls: &["set-clock pod"],
            hooks: awake_hooks,
        },
        Row {
            record: Pausing,
            vm: Some((Saving, true, true)),
            want: Some(Awake),
            calls: &["resume pod", "set-clock pod"],
            hooks: awake_hooks,
        },
        Row {
            record: Pausing,
            vm: Some((Paused, true, true)),
            want: Some(Asleep),
            calls: &["stop-paused pod"],
            hooks: &["asleep pod"],
        },
        Row {
            record: Pausing,
            vm: Some((Paused, false, true)),
            want: Some(Asleep),
            calls: &[],
            hooks: &["asleep pod"],
        },
        Row {
            record: Pausing,
            vm: Some((Paused, false, false)),
            want: Some(Failed),
            calls: &[],
            hooks: &["failed pod"],
        },
        Row {
            record: Pausing,
            vm: Some((Saving, false, true)),
            want: Some(Failed),
            calls: &[],
            hooks: &["failed pod"],
        },
        Row {
            record: Pausing,
            vm: Some((Running, false, false)),
            want: Some(Failed),
            calls: &[],
            hooks: &["failed pod"],
        },
        Row {
            record: Asleep,
            vm: Some((Paused, false, true)),
            want: Some(Asleep),
            calls: &[],
            hooks: &["asleep pod"],
        },
        Row {
            record: Asleep,
            vm: Some((Paused, false, false)),
            want: Some(Failed),
            calls: &[],
            hooks: &["failed pod"],
        },
        Row {
            record: Asleep,
            vm: None,
            want: Some(Failed),
            calls: &[],
            hooks: &["failed pod"],
        },
        Row {
            record: Waking,
            vm: Some((Paused, false, true)),
            want: Some(Awake),
            calls: &["resume pod", "set-clock pod"],
            hooks: &["before-release pod", "release pod", "awake-Woke pod"],
        },
        Row {
            record: Waking,
            vm: Some((Running, true, false)),
            want: Some(Awake),
            calls: &["set-clock pod"],
            hooks: awake_hooks,
        },
        Row {
            record: Waking,
            vm: Some((Running, false, false)),
            want: Some(Failed),
            calls: &[],
            hooks: &["failed pod"],
        },
        Row {
            record: Deleting,
            vm: Some((Paused, false, true)),
            want: None,
            calls: &["delete pod"],
            hooks: &["deleted pod"],
        },
    ];
    for row in rows {
        let name = format!("record {}, smolvm {:?}", row.record, row.vm);
        let mut h = Harness::unopened();
        let mut record = PodRecord::new(identity("pod"), BOOT.into(), START_MS);
        record.state = row.record;
        record.generation = 5;
        record.reason = Some(Reason::Idle);
        if let Some((state, alive, checkpoint)) = row.vm {
            let process = h.vm.set("pod", state, alive, checkpoint);
            record.vm = (state == Running).then_some(process);
        }
        h.write_record(&record);
        h.open().await;
        assert_eq!(h.state("pod"), row.want, "{name}");
        assert_eq!(h.disk_state("pod"), row.want, "{name}");
        assert_eq!(h.vm.calls(), row.calls, "{name}");
        assert_eq!(h.hooks.calls(), row.hooks, "{name}");
        let events = h.events("pod");
        match row.want {
            Some(state) if state == row.record => assert!(events.is_empty(), "{name}"),
            _ => {
                let last = events.last().unwrap();
                assert!(last.recovered, "{name}");
                assert_eq!(last.state, row.want.unwrap_or(Deleted), "{name}");
                if last.state != Failed {
                    assert_eq!(last.reason, Reason::Recovery, "{name}");
                }
            }
        }
        if row.want == Some(Awake) {
            assert_eq!(
                h.info("pod").vm,
                h.vm.machine("pod").unwrap().process,
                "{name}"
            );
            h.hibernate("pod").await.unwrap();
        }
    }
}

#[tokio::test]
async fn records_of_an_earlier_boot_and_unnamed_machines_are_deleted() {
    let mut h = Harness::unopened();
    let mut record = PodRecord::new(identity("old"), "boot-0".into(), START_MS);
    record.state = Asleep;
    h.vm.set("old", VmState::Paused, false, true);
    h.vm.add_running("orphan");
    h.write_record(&record);
    h.open().await;
    let mut calls = h.vm.calls();
    calls.sort();
    assert_eq!(calls, ["delete old", "delete orphan"]);
    assert!(h.life().list().is_empty());
    assert!(h.vm.machine("orphan").is_none());
}

// ------------------------------------------------------------ fault points

#[tokio::test]
async fn a_crash_at_pause_saving_recovers_awake() {
    let (mut h, crash) = Harness::crashing(&[FaultPoint::PauseSaving]);
    h.open().await;
    h.create("pod").await.unwrap();
    let sleeping = h.spawn_hibernate("pod");
    assert_eq!(crash.crashed().await, FaultPoint::PauseSaving);
    assert_eq!(h.vm.machine("pod").unwrap().state, VmState::Pausing);
    h.faults = Faults::none();
    h.open().await;
    sleeping.abort();
    assert_eq!(h.state("pod"), Some(Awake));
    assert_eq!(h.vm.calls(), ["pause pod", "resume pod", "set-clock pod"]);
    let last = h.events("pod").pop().unwrap();
    assert!(last.recovered);
    assert_eq!(last.reason, Reason::Recovery);
    assert!(!h.hooks.calls().contains(&"failed pod".to_string()));
}

#[tokio::test]
async fn a_crash_at_pause_durable_recovers_asleep() {
    let (mut h, crash) = Harness::crashing(&[FaultPoint::PauseDurable]);
    h.open().await;
    h.create("pod").await.unwrap();
    let sleeping = h.spawn_hibernate("pod");
    assert_eq!(crash.crashed().await, FaultPoint::PauseDurable);
    h.faults = Faults::none();
    h.open().await;
    sleeping.abort();
    assert_eq!(h.state("pod"), Some(Asleep));
    assert_eq!(h.vm.calls(), ["pause pod", "stop-paused pod"]);
    assert!(h.events("pod").pop().unwrap().recovered);
    h.wake("pod").await.unwrap();
}

#[tokio::test]
async fn a_crash_at_wake_resumed_recovers_awake() {
    let (mut h, crash) = Harness::crashing(&[FaultPoint::WakeResumed]);
    h.open().await;
    h.create("pod").await.unwrap();
    h.hibernate("pod").await.unwrap();
    h.clear();
    let waking = h.spawn_wake("pod");
    assert_eq!(crash.crashed().await, FaultPoint::WakeResumed);
    h.faults = Faults::none();
    h.open().await;
    waking.abort();
    assert_eq!(h.state("pod"), Some(Awake));
    assert_eq!(h.vm.calls(), ["resume pod", "set-clock pod"]);
    assert!(h.info("pod").last_wake.is_some());
    assert!(!h.hooks.calls().contains(&"failed pod".to_string()));
}

#[tokio::test]
async fn an_idle_pause_waits_at_idle_decided_before_the_hold() {
    let (mut h, crash) = Harness::crashing(&[FaultPoint::IdleDecided]);
    h.open().await;
    h.create("pod").await.unwrap();
    // A pause the API asked for does not stop there.
    h.life()
        .hibernate(&pod("pod"), HibernateRequest::new(Reason::Api))
        .await
        .unwrap();
    h.wake("pod").await.unwrap();
    h.clear();
    let sleeping = h.spawn_hibernate("pod");
    assert_eq!(crash.crashed().await, FaultPoint::IdleDecided);
    assert_eq!(h.state("pod"), Some(Pausing));
    assert!(h.hooks.calls().is_empty());
    crash.release();
    sleeping.await.unwrap().unwrap();
    assert_eq!(
        h.hooks.calls(),
        ["pausing-idle pod", "stop-pumps pod", "asleep pod"]
    );
}

#[tokio::test]
async fn a_resume_error_point_fails_the_wakes_of_its_pod() {
    let mut h = Harness::unopened();
    h.faults = Faults::from_spec("wake:resume-error@pod", h.dir.path()).unwrap();
    h.open().await;
    for id in ["pod", "other"] {
        h.create(id).await.unwrap();
        h.hibernate(id).await.unwrap();
    }
    h.clear();
    assert!(h.wake("other").await.unwrap().resumed);
    for failures in 1..=2 {
        let e = h.wake("pod").await.unwrap_err();
        assert_eq!(
            e,
            LifecycleError::Vm("fault point wake:resume-error".into())
        );
        assert_eq!(h.state("pod"), Some(Asleep));
        assert_eq!(h.info("pod").wake_failures, failures);
    }
    let e = h.wake("pod").await.unwrap_err();
    assert!(
        matches!(&e, LifecycleError::Failed(why) if why.contains("3 times in a row")),
        "{e}"
    );
    assert_eq!(h.state("pod"), Some(Failed));
    assert!(!h.vm.calls().contains(&"resume pod".to_string()));
}

#[tokio::test]
async fn a_crash_at_wake_clock_recovers_awake() {
    let (mut h, crash) = Harness::crashing(&[FaultPoint::WakeClock]);
    h.open().await;
    h.create("pod").await.unwrap();
    h.hibernate("pod").await.unwrap();
    h.clear();
    let waking = h.spawn_wake("pod");
    assert_eq!(crash.crashed().await, FaultPoint::WakeClock);
    h.faults = Faults::none();
    h.open().await;
    waking.abort();
    assert_eq!(h.state("pod"), Some(Awake));
    assert_eq!(
        h.vm.calls(),
        ["resume pod", "set-clock pod", "set-clock pod"]
    );
}

/// A failed sandbox can keep the IP that the network has since given to
/// another pod. The pod map's order changes at each open, so the test opens
/// it a few times.
#[tokio::test]
async fn a_lookup_by_ip_prefers_a_live_pod_then_the_newest() {
    let ip = "10.42.0.9";
    let by_ip = PodRef::Ip(ip.into());
    let mut h = Harness::unopened();
    let created = (START_MS - 7..START_MS).chain([START_MS + 1]);
    for (n, created_ms) in created.enumerate() {
        let mut failed = identity(&format!("failed-{n}"));
        failed.ip = Some(ip.into());
        let mut record = PodRecord::new(failed, BOOT.into(), created_ms);
        record.state = Failed;
        h.write_record(&record);
    }
    h.open().await;
    let found = |h: &Harness| h.life().get(&by_ip).map(|p| p.identity.sandbox_id);
    assert_eq!(found(&h).as_deref(), Some("failed-7"));
    h.create("live").await.unwrap();
    h.life()
        .set_network("live", Some(ip.into()), Some("02:00:0a:2a:00:09".into()))
        .unwrap();
    assert_eq!(found(&h).as_deref(), Some("live"));
    for _ in 0..4 {
        h.open().await;
        assert_eq!(found(&h).as_deref(), Some("live"));
    }
}
