//! The node service's part in wakes on traffic and idle sleep.
//!
//! [`Sleep`] joins these modules to the lifecycle's hooks:
//!
//! - the catchers ([`crate::sleepnet`]) hold a pod's traffic from the start
//!   of a pause (`pausing`) to the end of a wake (`release`), and are closed
//!   when the pod fails or is deleted;
//! - the traffic waker turns a held frame into a wake with the reason
//!   `traffic`, from the source pod's `NAMESPACE/NAME` where it is known;
//! - the idle loop polls the activity monitor ([`crate::activity`]) every
//!   poll interval and at each pod's due time, runs the pre-sleep probe check
//!   of each idle pod ([`crate::probe`]), and hibernates it with the reason
//!   `idle`; the `pausing` hook checks conntrack again once the hold is set;
//! - admission ([`super::admission`]) caps the pods awake.

use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime};

use log::{debug, info, warn};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use super::admission::{Admission, Holder};
use super::settings::Settings;
use crate::activity::{self, Candidate, ConntrackLookup, Monitor, Protocol, Tuple, Verdict};
use crate::engine::SANDBOX_MEMORY_MIB;
use crate::lifecycle::{
    Blocker, Clock, HibernateRequest, Lifecycle, LifecycleError, PodIdentity, PodInfo, PodRef,
    PodState, Reason, WakeRequest,
};
use crate::probe::{NodeProbeBook, ProbeBook, ProbeResponders};
use crate::sleepnet::{Catchers, NetCatchers, Phase, PodNet, TrafficWaker};
use crate::sync::lock;

/// The shortest time between two polls of the idle loop.
const POLL_GAP: Duration = Duration::from_secs(1);

/// Names the pod at an IP, as `NAMESPACE/NAME`.
pub type Names = Arc<dyn Fn(Ipv4Addr) -> Option<String> + Send + Sync>;

/// The node service's parts for wakes on traffic and idle sleep.
pub struct Sleep {
    lifecycle: Lifecycle,
    catchers: Arc<dyn Catchers>,
    book: Arc<dyn ProbeBook>,
    conntrack: Arc<dyn ConntrackLookup>,
    /// The idle loop's monitor; none in tests.
    monitor: Option<Arc<Monitor>>,
    names: Names,
    admission: Admission,
    poll_interval: Duration,
    /// The idle candidates that are being put to sleep, for the recheck.
    candidates: Mutex<HashMap<String, Candidate>>,
    /// Why each awake pod last did not sleep, for an event once per change.
    refusals: Mutex<HashMap<String, String>>,
    /// The IP of each pod that a hook saw, for the traffic waker, which must
    /// not wait for a record.
    ips: Mutex<HashMap<String, Ipv4Addr>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl Drop for Sleep {
    fn drop(&mut self) {
        for task in lock(&self.tasks).drain(..) {
            task.abort();
        }
    }
}

impl Sleep {
    /// Starts the parts in the node's network namespace: the probe book
    /// and its pod watch, the activity monitor, the probe responders, the
    /// catchers' netloop thread and the traffic waker.
    pub fn start(settings: &Settings, lifecycle: Lifecycle) -> Result<Arc<Sleep>, String> {
        let clock = Clock::system();
        let book = Arc::new(NodeProbeBook::new(lifecycle.clone(), clock.clone()));
        let watch = crate::kube::PodWatch::new(crate::kube::KUBELET_KUBECONFIG, book.clone());
        let monitor = Arc::new(Monitor::new(settings, clock));
        let waker = Arc::new(Waker::new());
        let responders = ProbeResponders::new(book.clone(), monitor.clone(), waker.clone())?;
        let catchers = NetCatchers::start(Arc::new(responders), waker.clone())?;
        let names: Names = {
            let book = book.clone();
            Arc::new(move |ip| book.pod_at(ip))
        };
        let mut sleep = Sleep::with(
            settings,
            lifecycle,
            Arc::new(catchers),
            book,
            monitor.clone(),
            names,
        );
        sleep.monitor = Some(monitor);
        let sleep = Arc::new(sleep);
        waker.set(&sleep);
        lock(&sleep.tasks).push(watch.spawn());
        Ok(sleep)
    }

    /// The parts given, with no idle loop: for tests.
    pub fn with(
        settings: &Settings,
        lifecycle: Lifecycle,
        catchers: Arc<dyn Catchers>,
        book: Arc<dyn ProbeBook>,
        conntrack: Arc<dyn ConntrackLookup>,
        names: Names,
    ) -> Sleep {
        Sleep {
            lifecycle,
            catchers,
            book,
            conntrack,
            monitor: None,
            names,
            admission: Admission::new(settings),
            poll_interval: settings.poll_interval,
            candidates: Mutex::new(HashMap::new()),
            refusals: Mutex::new(HashMap::new()),
            ips: Mutex::new(HashMap::new()),
            tasks: Mutex::new(Vec::new()),
        }
    }

    pub fn book(&self) -> &Arc<dyn ProbeBook> {
        &self.book
    }

    /// Starts the idle loop, once recovery has settled every pod.
    pub fn start_idle_loop(self: &Arc<Self>) {
        if self.monitor.is_none() {
            return;
        }
        let task = tokio::spawn(self.clone().idle_loop());
        lock(&self.tasks).push(task);
    }

    /// The network identity of a pod with a network namespace, from its
    /// record, which [`crate::lifecycle::Lifecycle::set_network`] may have
    /// changed since `info` was taken; none for a pod without one.
    fn net(&self, info: &PodInfo) -> Option<Result<PodNet, String>> {
        let id = &info.identity.sandbox_id;
        let identity = self
            .lifecycle
            .record(id)
            .map_or_else(|| info.identity.clone(), |r| r.identity);
        identity.netns.as_ref()?;
        let net = PodNet::of(&identity);
        if let Ok(net) = &net {
            lock(&self.ips).insert(id.clone(), net.ip);
        }
        Some(net)
    }

    // ------------------------------------------------------------ the hooks

    /// `may_wake`: admission, then the catcher holds what comes during the
    /// wake. A refused wake answers the held connections with a RST.
    pub fn may_wake(&self, info: &PodInfo) -> Result<(), String> {
        let id = &info.identity.sandbox_id;
        let admitted = self
            .admission
            .admit(id, SANDBOX_MEMORY_MIB.into(), || self.holders());
        let net = self.net(info).and_then(Result::ok);
        if let Err(reason) = admitted {
            if let Some(net) = &net {
                if let Err(e) = self.catchers.refuse(net) {
                    warn!("pod sandbox {id}: refuse the held connections: {e}");
                }
            }
            return Err(reason);
        }
        if let Some(net) = &net {
            if let Err(e) = self.catchers.catch(net, Phase::Waking) {
                warn!("pod sandbox {id}: hold its traffic during the wake: {e}");
            }
        }
        Ok(())
    }

    /// The pods that hold room for admission.
    fn holders(&self) -> Vec<Holder> {
        self.lifecycle
            .list()
            .into_iter()
            .filter(|p| {
                matches!(
                    p.state,
                    PodState::Creating | PodState::Awake | PodState::Pausing | PodState::Waking
                )
            })
            .map(|p| (p.identity.sandbox_id, SANDBOX_MEMORY_MIB.into()))
            .collect()
    }

    /// `pausing`: the hold, then, for an idle pause, conntrack again, so that
    /// a request that came since the idle decision is answered by the awake
    /// pod. An error refuses the pause.
    pub fn pausing(&self, info: &PodInfo, reason: &Reason) -> Result<(), String> {
        let id = &info.identity.sandbox_id;
        if let Some(net) = self.net(info) {
            self.catchers.catch(&net?, Phase::Pausing)?;
        }
        if *reason == Reason::Idle {
            let candidate = lock(&self.candidates).get(id).cloned();
            if let Some(candidate) = candidate {
                candidate.recheck(&*self.conntrack)?;
            }
        }
        Ok(())
    }

    /// `asleep`: the catcher answers for the pod. After a failed restore its
    /// probes fail.
    pub fn asleep(&self, info: &PodInfo) {
        let id = &info.identity.sandbox_id;
        self.admission.release(id);
        let phase = Phase::Asleep {
            probes_fail: info.wake_failures > 0,
        };
        match self.net(info) {
            Some(Ok(net)) => {
                if let Err(e) = self.catchers.catch(&net, phase) {
                    warn!("pod sandbox {id}: catch its traffic while it sleeps: {e}");
                }
            }
            Some(Err(e)) => warn!("pod sandbox {id}: its traffic is not caught: {e}"),
            None => {}
        }
    }

    /// `release`: the held frames go to the guest. Returns how long the
    /// first of them waited.
    pub fn release(&self, info: &PodInfo) -> Result<Option<Duration>, String> {
        let Some(net) = self.net(info) else {
            return Ok(None);
        };
        let released = self.catchers.release(&net?)?;
        if released.frames > 0 {
            debug!(
                "pod sandbox {}: released {} held frames",
                info.identity.sandbox_id, released.frames
            );
        }
        Ok(released
            .first_held
            .and_then(|at| SystemTime::now().duration_since(at).ok()))
    }

    /// `failed` and `deleted`: held connections get a RST, and the catcher
    /// closes.
    pub fn gone(&self, info: &PodInfo) {
        let id = &info.identity.sandbox_id;
        self.admission.release(id);
        lock(&self.refusals).remove(id);
        if let Some(Ok(net)) = self.net(info) {
            if let Err(e) = self.catchers.close(&net) {
                debug!("pod sandbox {id}: close its catcher: {e}");
            }
        }
        lock(&self.ips).remove(id);
    }

    /// After a restart of the node service, before any hook runs: the
    /// catcher of each pod by its recorded state. An awake or failed pod gets
    /// no catcher, and a hold that a crash left is ended. Blocking.
    pub fn restore(&self, identity: &PodIdentity, state: PodState, wake_failures: u32) {
        let id = &identity.sandbox_id;
        if identity.netns.is_none() {
            return;
        }
        let net = match PodNet::of(identity) {
            Ok(net) => {
                lock(&self.ips).insert(id.clone(), net.ip);
                net
            }
            Err(e) => {
                warn!("pod sandbox {id}: its traffic is not caught: {e}");
                return;
            }
        };
        let phase = match state {
            PodState::Pausing => Some(Phase::Pausing),
            PodState::Asleep => Some(Phase::Asleep {
                probes_fail: wake_failures > 0,
            }),
            PodState::Waking => Some(Phase::Waking),
            _ => None,
        };
        let done = match phase {
            Some(phase) => self.catchers.catch(&net, phase),
            None => self.catchers.close(&net),
        };
        if let Err(e) = done {
            warn!("pod sandbox {id}, {state}: {e}");
        }
    }

    // ------------------------------------------------------- traffic wakes

    /// Whether a frame from `from` to the pod's `port` belongs to a
    /// connection that the pod opened, which does not wake it: conntrack has
    /// the frame's tuple as the reply of an entry whose original source is
    /// the pod. A SYN, or data on a connection opened to the pod, has no such
    /// entry. A failed lookup wakes the pod.
    fn is_own_connection(&self, id: &str, from: SocketAddrV4, port: u16) -> bool {
        let Some(pod_ip) = lock(&self.ips).get(id).copied() else {
            return false;
        };
        let frame = Tuple {
            protocol: Protocol::Tcp,
            src: from,
            dst: SocketAddrV4::new(pod_ip, port),
        };
        match self.conntrack.by_reply(&frame) {
            Ok(Some(entry)) => *entry.original.src.ip() == pod_ip,
            Ok(None) => false,
            Err(e) => {
                debug!("pod sandbox {id}: conntrack has no answer for {from}: {e}");
                false
            }
        }
    }

    /// Wakes the pod for traffic from `from` to its `port`.
    async fn traffic_wake(&self, id: &str, from: SocketAddrV4, port: u16) {
        let started = Instant::now();
        let source = {
            let (lifecycle, conntrack, names) = (
                self.lifecycle.clone(),
                self.conntrack.clone(),
                self.names.clone(),
            );
            let id = id.to_string();
            tokio::task::spawn_blocking(move || {
                source(&lifecycle, &*conntrack, &names, &id, from, port)
            })
            .await
            .unwrap_or_else(|_| from.ip().to_string())
        };
        let reason = Reason::Traffic {
            from: source.clone(),
            port,
        };
        match self
            .lifecycle
            .wake(&PodRef::sandbox(id), WakeRequest::new(reason))
            .await
        {
            Ok(report) if report.resumed => info!(
                "pod sandbox {id} woke for traffic from {source} to port {port} in {:.1} ms",
                started.elapsed().as_secs_f64() * 1000.0
            ),
            Ok(_) => debug!("pod sandbox {id}: traffic from {source} found it awake"),
            Err(e) => info!("pod sandbox {id}: no wake for traffic from {source}: {e}"),
        }
    }

    // ------------------------------------------------------------ idle loop

    async fn idle_loop(self: Arc<Self>) {
        let Some(monitor) = self.monitor.clone() else {
            return;
        };
        let in_flight: Arc<Mutex<HashSet<String>>> = Arc::default();
        loop {
            let pods = self.lifecycle.list();
            let awake: HashSet<String> = pods
                .iter()
                .filter(|p| p.state == PodState::Awake)
                .map(|p| p.identity.sandbox_id.clone())
                .collect();
            lock(&self.refusals).retain(|id, _| awake.contains(id));
            let infos: HashMap<String, PodInfo> = pods
                .iter()
                .map(|p| (p.identity.sandbox_id.clone(), p.clone()))
                .collect();
            let (monitor, book) = (monitor.clone(), self.book.clone());
            let statuses = tokio::task::spawn_blocking(move || monitor.poll(&pods, &*book))
                .await
                .unwrap_or_default();
            let now = SystemTime::now();
            let mut next = now + self.poll_interval;
            for status in statuses {
                match status.verdict {
                    Verdict::Idle(candidate) => {
                        let Some(info) = infos.get(&status.sandbox_id) else {
                            continue;
                        };
                        if lock(&in_flight).insert(status.sandbox_id.clone()) {
                            let (sleep, in_flight, info) =
                                (self.clone(), in_flight.clone(), info.clone());
                            tokio::spawn(async move {
                                let id = info.identity.sandbox_id.clone();
                                sleep.put_to_sleep(info, candidate).await;
                                lock(&in_flight).remove(&id);
                            });
                        }
                    }
                    Verdict::Busy(reasons) => {
                        if status.due > now {
                            next = next.min(status.due.max(now + POLL_GAP));
                        }
                        debug!(
                            "pod sandbox {} is busy: {}",
                            status.sandbox_id,
                            reasons
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join("; ")
                        );
                    }
                }
            }
            let wait = next
                .duration_since(SystemTime::now())
                .unwrap_or_default()
                .max(POLL_GAP);
            tokio::time::sleep(wait).await;
        }
    }

    /// The pre-sleep check of an idle pod, then `hibernate(idle)`.
    async fn put_to_sleep(&self, info: PodInfo, candidate: Candidate) {
        let id = info.identity.sandbox_id.clone();
        if let Some(why) = lasting_blocker(&info) {
            return self.refused(&id, why.clone(), why);
        }
        if !info.leases.is_empty() || !info.execs.is_empty() || info.creates > 0 {
            debug!("pod sandbox {id} is idle, but busy with leases, execs or creates");
            return;
        }
        let book = self.book.clone();
        let checked = {
            let id = id.clone();
            tokio::task::spawn_blocking(move || book.check_before_sleep(&id))
                .await
                .unwrap_or_else(|e| Err(format!("the pre-sleep check ended: {e}")))
        };
        if let Err(why) = checked {
            let key = without_digits(&why);
            return self.refused(&id, why, key);
        }
        lock(&self.candidates).insert(id.clone(), candidate.clone());
        let slept = self
            .lifecycle
            .hibernate(&PodRef::sandbox(&id), HibernateRequest::new(Reason::Idle))
            .await;
        lock(&self.candidates).remove(&id);
        match slept {
            Ok(report) => {
                lock(&self.refusals).remove(&id);
                info!(
                    "pod sandbox {id} slept after {:.0?} idle, in {:.1} ms",
                    SystemTime::now()
                        .duration_since(candidate.idle_since)
                        .unwrap_or_default(),
                    report.duration_ms
                );
            }
            Err(LifecycleError::Refused(blockers)) => {
                match blockers.iter().find(|b| is_lasting(b)) {
                    Some(blocker) => {
                        let why = blocker.to_string();
                        self.refused(&id, why.clone(), kind_of(blocker).into());
                    }
                    None => debug!("pod sandbox {id} did not sleep: {}", describe(&blockers)),
                }
            }
            Err(e) => info!("pod sandbox {id} did not sleep: {e}"),
        }
    }

    /// An idle pod does not sleep: an event from `awake` to `awake` once
    /// per change of the reason, whose `key` leaves out what changes at
    /// every check.
    fn refused(&self, id: &str, why: String, key: String) {
        {
            let mut refusals = lock(&self.refusals);
            if refusals.get(id) == Some(&key) {
                debug!("pod sandbox {id} still does not sleep: {why}");
                return;
            }
            refusals.insert(id.to_string(), key);
        }
        info!("pod sandbox {id} is idle but does not sleep: {why}");
        let error = format!("not sleeping: {why}");
        if let Err(e) = self
            .lifecycle
            .note(id, PodState::Awake, Reason::Idle, error)
        {
            debug!("pod sandbox {id}: record why it does not sleep: {e}");
        }
    }
}

/// Why a pod never sleeps, whatever its activity, if it does not.
fn lasting_blocker(info: &PodInfo) -> Option<String> {
    info.identity
        .hibernate_disabled
        .then(|| Blocker::Disabled.to_string())
}

/// A blocker that stays until someone acts, as opposed to a running exec or
/// a wake's waiters.
fn is_lasting(blocker: &Blocker) -> bool {
    matches!(blocker, Blocker::Disabled | Blocker::Lease { .. })
}

fn kind_of(blocker: &Blocker) -> &'static str {
    match blocker {
        Blocker::Disabled => "disabled",
        Blocker::Lease { .. } => "lease",
        _ => "other",
    }
}

fn describe(blockers: &[Blocker]) -> String {
    blockers
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

/// `text` with its digits left out, so that a reason such as "its last
/// result is 31 s old" is the same reason at the next check.
fn without_digits(text: &str) -> String {
    text.chars().filter(|c| !c.is_ascii_digit()).collect()
}

/// Where traffic to a pod came from, for the wake's reason: the source pod's
/// `NAMESPACE/NAME` when the node knows its IP; for a source on the node, the
/// conntrack entry's original source, such as the host Mac behind a NodePort;
/// otherwise the IP. A live record names the pod before the pod watch does,
/// and the watch before a failed or deleting record, whose IP the network may
/// have given to another pod. Blocking.
fn source(
    lifecycle: &Lifecycle,
    conntrack: &dyn ConntrackLookup,
    names: &Names,
    id: &str,
    from: SocketAddrV4,
    port: u16,
) -> String {
    let mut ip = *from.ip();
    let local = activity::local_addrs().unwrap_or_default();
    if local.contains(&ip) {
        let pod_ip = lifecycle
            .get(&PodRef::sandbox(id))
            .and_then(|p| p.identity.ip)
            .and_then(|ip| ip.parse().ok());
        if let Some(pod_ip) = pod_ip {
            let reply = Tuple {
                protocol: Protocol::Tcp,
                src: SocketAddrV4::new(pod_ip, port),
                dst: from,
            };
            if let Ok(Some(entry)) = conntrack.by_reply(&reply) {
                ip = *entry.original.src.ip();
            }
        }
    }
    let record = lifecycle.get(&PodRef::Ip(ip.to_string()));
    let live = record.as_ref().is_some_and(|p| p.state.is_live());
    let named = record
        .map(|p| format!("{}/{}", p.identity.namespace, p.identity.name))
        .filter(|name| name != "/");
    match named {
        Some(name) if live => name,
        named => names(ip).or(named).unwrap_or_else(|| ip.to_string()),
    }
}

/// The catchers' [`TrafficWaker`]. It runs on the netloop thread, so it only
/// starts the wake on the runtime, one at a time for each pod: the frames
/// that come meanwhile are held, and released or refused with the first.
struct Waker {
    runtime: Handle,
    sleep: OnceLock<Weak<Sleep>>,
    pending: Arc<Mutex<HashSet<String>>>,
}

impl Waker {
    fn new() -> Waker {
        Waker {
            runtime: Handle::current(),
            sleep: OnceLock::new(),
            pending: Arc::default(),
        }
    }

    fn set(&self, sleep: &Arc<Sleep>) {
        let _ = self.sleep.set(Arc::downgrade(sleep));
    }
}

impl TrafficWaker for Waker {
    fn wake(&self, sandbox_id: &str, from: SocketAddrV4, port: u16) -> bool {
        let Some(sleep) = self.sleep.get().and_then(Weak::upgrade) else {
            return false;
        };
        if sleep.is_own_connection(sandbox_id, from, port) {
            debug!("pod sandbox {sandbox_id}: no wake for {from} on a connection the pod opened");
            return false;
        }
        if !lock(&self.pending).insert(sandbox_id.to_string()) {
            return true;
        }
        let (id, pending) = (sandbox_id.to_string(), self.pending.clone());
        self.runtime.spawn(async move {
            sleep.traffic_wake(&id, from, port).await;
            lock(&pending).remove(&id);
        });
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::UNIX_EPOCH;

    use crate::activity::{ConnEntry, MockConntrack};
    use crate::lifecycle::{Config, NoHooks, PodRecord, RunningVms};
    use crate::probe::MockProbeBook;
    use crate::sleepnet::MockCatchers;

    const POD_IP: Ipv4Addr = Ipv4Addr::new(10, 42, 0, 9);

    struct Fixture {
        _dir: tempfile::TempDir,
        life: Lifecycle,
        catchers: Arc<MockCatchers>,
        conntrack: Arc<MockConntrack>,
        sleep: Arc<Sleep>,
    }

    fn identity(id: &str, name: &str, ip: Ipv4Addr) -> PodIdentity {
        PodIdentity {
            sandbox_id: id.into(),
            namespace: "ns".into(),
            name: name.into(),
            machine: id.into(),
            netns: Some(format!("/var/run/netns/cni-{id}")),
            ip: Some(ip.to_string()),
            mac: Some("c2:3c:2a:92:39:90".into()),
            ..Default::default()
        }
    }

    impl Fixture {
        async fn new(settings: Settings) -> Fixture {
            Fixture::with_failed(settings, Vec::new()).await
        }

        /// A fixture whose lifecycle opens with a failed record of each of
        /// `failed`.
        async fn with_failed(settings: Settings, failed: Vec<PodIdentity>) -> Fixture {
            let dir = tempfile::tempdir().unwrap();
            let config = Config::defaults(dir.path());
            let pods = dir.path().join("pods");
            std::fs::create_dir_all(&pods).unwrap();
            for identity in failed {
                let path = pods.join(format!("{}.json", identity.sandbox_id));
                let mut record = PodRecord::new(identity, config.boot_id.clone(), 1);
                record.state = PodState::Failed;
                std::fs::write(path, serde_json::to_vec(&record).unwrap()).unwrap();
            }
            let life = Lifecycle::open(config, Arc::new(RunningVms), Arc::new(NoHooks))
                .await
                .unwrap();
            life.settled().await;
            let catchers = Arc::new(MockCatchers::default());
            let conntrack = Arc::new(MockConntrack::default());
            let names: Names = Arc::new(|ip| {
                (ip == Ipv4Addr::new(10, 42, 0, 30)).then(|| "ns/runc-client".to_string())
            });
            let sleep = Arc::new(Sleep::with(
                &settings,
                life.clone(),
                catchers.clone(),
                Arc::new(MockProbeBook::default()),
                conntrack.clone(),
                names,
            ));
            Fixture {
                _dir: dir,
                life,
                catchers,
                conntrack,
                sleep,
            }
        }

        async fn pod(&self, id: &str, name: &str, ip: Ipv4Addr) -> PodInfo {
            self.life
                .create(identity(id, name, ip), async { Ok(None) })
                .await
                .unwrap()
        }

        /// Runs a hook on a blocking thread, as the lifecycle does.
        async fn hook<T: Send + 'static>(&self, f: impl FnOnce(&Sleep) -> T + Send + 'static) -> T {
            let sleep = self.sleep.clone();
            tokio::task::spawn_blocking(move || f(&sleep))
                .await
                .unwrap()
        }
    }

    fn at(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(1_760_000_000 + secs)
    }

    fn candidate(last_traffic: Option<SystemTime>) -> Candidate {
        Candidate {
            ip: POD_IP,
            last_traffic,
            idle_since: at(0),
            vcpus: None,
        }
    }

    #[tokio::test]
    async fn the_hooks_hold_and_release_the_pods_traffic() {
        let f = Fixture::new(Settings::default()).await;
        let info = f.pod("pod", "web", POD_IP).await;
        let i = info.clone();
        f.hook(move |s| s.pausing(&i, &Reason::Api)).await.unwrap();
        let mut asleep = info.clone();
        asleep.wake_failures = 1;
        f.hook(move |s| s.asleep(&asleep)).await;
        let i = info.clone();
        f.hook(move |s| s.may_wake(&i)).await.unwrap();
        let i = info.clone();
        assert_eq!(f.hook(move |s| s.release(&i)).await, Ok(None));
        let i = info.clone();
        f.hook(move |s| s.gone(&i)).await;
        assert_eq!(
            f.catchers.calls(),
            [
                "catch pod Pausing",
                "catch pod Asleep { probes_fail: true }",
                "catch pod Waking",
                "release pod",
                "close pod"
            ]
        );
    }

    #[tokio::test]
    async fn a_packet_after_the_idle_decision_refuses_the_pause() {
        let f = Fixture::new(Settings::default()).await;
        let info = f.pod("pod", "web", POD_IP).await;
        lock(&f.sleep.candidates).insert("pod".into(), candidate(Some(at(0))));
        lock(&f.conntrack.activity).insert(POD_IP, at(0));
        let i = info.clone();
        f.hook(move |s| s.pausing(&i, &Reason::Idle)).await.unwrap();
        lock(&f.conntrack.activity).insert(POD_IP, at(3));
        let i = info.clone();
        let refused = f.hook(move |s| s.pausing(&i, &Reason::Idle)).await;
        assert_eq!(refused, Err("a packet after the idle decision".into()));
        // A pause through the API is not rechecked.
        let i = info.clone();
        f.hook(move |s| s.pausing(&i, &Reason::Api)).await.unwrap();
    }

    #[tokio::test]
    async fn a_pod_without_a_network_namespace_holds_nothing() {
        let f = Fixture::new(Settings::default()).await;
        let identity = PodIdentity {
            sandbox_id: "ctr".into(),
            machine: "ctr".into(),
            ..Default::default()
        };
        let info = f.life.create(identity, async { Ok(None) }).await.unwrap();
        let i = info.clone();
        f.hook(move |s| s.pausing(&i, &Reason::Idle)).await.unwrap();
        let i = info.clone();
        assert_eq!(f.hook(move |s| s.release(&i)).await, Ok(None));
        assert!(f.catchers.calls().is_empty());
    }

    #[tokio::test]
    async fn a_wake_past_the_cap_is_refused_with_a_reset() {
        let settings = Settings {
            max_awake_pods: 1,
            admission_wait: Duration::from_millis(100),
            ..Settings::default()
        };
        let f = Fixture::new(settings).await;
        f.pod("awake", "web", Ipv4Addr::new(10, 42, 0, 8)).await;
        let info = f.pod("pod", "callee", POD_IP).await;
        let i = info.clone();
        let refused = f.hook(move |s| s.may_wake(&i)).await;
        assert_eq!(refused, Err("awake pods 1/1".into()));
        assert_eq!(f.catchers.calls(), ["refuse pod"]);
    }

    #[tokio::test]
    async fn only_connections_opened_to_the_pod_wake_it() {
        let f = Fixture::new(Settings::default()).await;
        let info = f.pod("pod", "web", POD_IP).await;
        let client = SocketAddrV4::new(Ipv4Addr::new(10, 42, 0, 30), 40000);
        assert!(
            !f.sleep.is_own_connection("pod", client, 8080),
            "no hook saw it"
        );
        assert!(f.sleep.net(&info).is_some_and(|net| net.is_ok()));
        let tcp = |src: SocketAddrV4, dst: SocketAddrV4| Tuple {
            protocol: Protocol::Tcp,
            src,
            dst,
        };
        let pod = |port| SocketAddrV4::new(POD_IP, port);
        let broker = SocketAddrV4::new(Ipv4Addr::new(10, 42, 0, 30), 5672);
        // The pod's own connection to a broker behind a Service.
        lock(&f.conntrack.entries).push(ConnEntry {
            original: tcp(
                pod(41000),
                SocketAddrV4::new(Ipv4Addr::new(10, 43, 0, 20), 5672),
            ),
            reply: tcp(broker, pod(41000)),
            timeout_secs: 86_400,
        });
        assert!(f.sleep.is_own_connection("pod", broker, 41000));
        // A client's connection to the pod.
        lock(&f.conntrack.entries).push(ConnEntry {
            original: tcp(client, pod(8080)),
            reply: tcp(pod(8080), client),
            timeout_secs: 120,
        });
        assert!(!f.sleep.is_own_connection("pod", client, 8080));
        assert!(!f.sleep.is_own_connection(
            "pod",
            SocketAddrV4::new(Ipv4Addr::new(10, 42, 0, 31), 1),
            8080
        ));
    }

    /// A failed sandbox keeps its IP after the network gives it to another
    /// pod, such as the runc pod that only the pod watch knows.
    #[tokio::test]
    async fn the_source_of_traffic_is_named() {
        let failed = vec![
            identity("stale", "stale", Ipv4Addr::new(10, 42, 0, 30)),
            identity("gone", "gone", Ipv4Addr::new(10, 42, 0, 32)),
        ];
        let f = Fixture::with_failed(Settings::default(), failed).await;
        f.pod("pod", "web", POD_IP).await;
        f.pod("caller", "caller", Ipv4Addr::new(10, 42, 0, 12))
            .await;
        let names = f.sleep.names.clone();
        let from = |ip: [u8; 4], port| SocketAddrV4::new(ip.into(), port);
        let named = |from| source(&f.life, &*f.conntrack, &names, "pod", from, 8080);
        assert_eq!(named(from([10, 42, 0, 12], 40000)), "ns/caller");
        assert_eq!(named(from([10, 42, 0, 30], 40000)), "ns/runc-client");
        assert_eq!(named(from([10, 42, 0, 32], 40000)), "ns/gone");
        assert_eq!(named(from([10, 42, 0, 31], 40000)), "10.42.0.31");
    }

    #[test]
    fn a_changing_number_is_the_same_reason() {
        assert_eq!(
            without_digits("its last result is 21 s old"),
            without_digits("its last result is 35 s old")
        );
        assert_ne!(
            without_digits("the pod is not Ready"),
            without_digits("container app is not ready")
        );
    }

    #[test]
    fn leases_and_the_annotation_are_lasting_blockers() {
        let lease = Blocker::Lease {
            id: "work".into(),
            holder: None,
            expires_ms: 1,
        };
        assert!(is_lasting(&lease) && is_lasting(&Blocker::Disabled));
        assert!(!is_lasting(&Blocker::ExecStarting));
        assert!(!is_lasting(&Blocker::WakeWaiter { until_ms: 1 }));
    }
}
