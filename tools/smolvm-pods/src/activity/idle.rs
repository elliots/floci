//! The last activity and idle timer of each pod.
//!
//! [`Monitor::poll`] runs at each poll of the idle loop (`--poll-interval`,
//! 15 s by default, and at a pod's `due` time), on a blocking thread. It
//! dumps the node's conntrack table once, works out when each pod last saw a
//! TCP or UDP packet, either way, on a connection opened to it, other than a
//! kubelet probe, reads each awake pod's VM CPU, and judges each awake pod:
//! [`Verdict::Idle`] with a [`Candidate`], or [`Verdict::Busy`] with every
//! reason. The node service runs the pre-sleep probe check for a candidate
//! and then `hibernate(Reason::Idle)`, and in the `pausing` hook, once the
//! hold is set, calls [`Candidate::recheck`]. Leases, execs and container
//! creates are the lifecycle's pause blockers, which `hibernate` checks, so
//! they are not judged here.
//!
//! **Only connections opened to the pod count**: an entry whose original
//! source is not the pod and whose reply source is, which covers connections
//! to the pod IP and those through a Service or a NodePort after DNAT.
//! Traffic on connections that the pod opened itself, such as its DNS
//! lookups, a broker's pushes or a server's heartbeats, does not keep it
//! awake.
//!
//! **Last activity.** A pod's is the newest of its entries'. An entry's is
//! its age ([`Timeouts::age`]) at the first dump that sees it move: its
//! packet counters change, or, without counters, its TCP state changes or its
//! timeout does not count down by the time between the dumps. The time of an
//! entry that moved is at least just after the previous dump, and an entry
//! that did not move keeps its time, so a pod's last activity grows only
//! when it sent or received a packet, which [`Candidate::recheck`] relies on.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use log::{debug, warn};

use super::conntrack::{self, is_probe, Conn, Counters, Ctnetlink, Timeouts};
use super::cpu::{self, CpuMeter, Sample};
use super::{ConnEntry, ConntrackLookup, Tuple};
use crate::lifecycle::{Clock, PodIdentity, PodInfo, PodState, VmProcess};
use crate::node::settings::Settings;
use crate::probe::{PodProbes, ProbeBook};
use crate::sync::lock;

/// More than this between what an entry's timeout should read and what it
/// reads means that a packet reset it. `CTA_TIMEOUT` rounds to the second.
const COUNTDOWN_SLACK: f64 = 2.0;

/// What the monitor reads from the node. Tests replace it.
pub trait Source: Send + Sync {
    fn dump(&self) -> Result<Vec<Conn>, String>;
    fn get(&self, reply: &Tuple) -> Result<Option<Conn>, String>;
    fn local_addrs(&self) -> Result<HashSet<Ipv4Addr>, String>;
    fn timeouts(&self) -> Timeouts;
    fn cpu(&self, process: VmProcess) -> Result<Sample, String>;
}

/// The node's kernel: ctnetlink, sysctls, interfaces and `/proc`.
#[derive(Default)]
pub struct Kernel(Ctnetlink);

impl Source for Kernel {
    fn dump(&self) -> Result<Vec<Conn>, String> {
        self.0.dump()
    }

    fn get(&self, reply: &Tuple) -> Result<Option<Conn>, String> {
        self.0.get(reply)
    }

    fn local_addrs(&self) -> Result<HashSet<Ipv4Addr>, String> {
        conntrack::local_addrs()
    }

    fn timeouts(&self) -> Timeouts {
        Timeouts::read()
    }

    fn cpu(&self, process: VmProcess) -> Result<Sample, String> {
        cpu::sample(process)
    }
}

/// A pod with an address, whose activity the tracker follows.
struct Watch {
    sandbox_id: String,
    probes: Option<Arc<PodProbes>>,
}

impl Watch {
    fn is_probe_port(&self, port: u16) -> bool {
        self.probes.as_ref().is_some_and(|p| p.is_probe_port(port))
    }
}

/// What the tracker keeps of an entry between dumps.
struct Seen {
    counters: Option<Counters>,
    tcp_state: Option<u8>,
    timeout_secs: u32,
    last: SystemTime,
}

struct PodTrack {
    sandbox_id: String,
    /// The last packet on a connection opened to the pod.
    last: Option<SystemTime>,
}

/// The entries of the watched pods between dumps.
#[derive(Default)]
struct Tracker {
    /// By `CTA_ID` and original tuple.
    seen: HashMap<(u32, Tuple), Seen>,
    dumped: Option<SystemTime>,
    pods: HashMap<Ipv4Addr, PodTrack>,
}

/// When the entry last saw a packet, by its age, rounded toward now.
fn estimate(conn: &Conn, timeouts: &Timeouts, now: SystemTime) -> SystemTime {
    let age = Duration::from_secs(timeouts.age(conn).into());
    let at = now.checked_sub(age).unwrap_or(UNIX_EPOCH);
    let started = conn
        .start_ns
        .map_or(UNIX_EPOCH, |ns| UNIX_EPOCH + Duration::from_nanos(ns));
    at.max(started).min(now)
}

/// Whether the entry saw a packet in the `elapsed` since `old` was seen.
fn moved(old: &Seen, conn: &Conn, elapsed: Duration) -> bool {
    if let (Some(then), Some(now)) = (old.counters, conn.counters) {
        return then != now;
    }
    if old.tcp_state != conn.tcp_state {
        return true;
    }
    let expected = f64::from(old.timeout_secs) - elapsed.as_secs_f64();
    (f64::from(conn.entry.timeout_secs) - expected).abs() > COUNTDOWN_SLACK
}

impl Tracker {
    fn update(
        &mut self,
        conns: &[Conn],
        watched: &HashMap<Ipv4Addr, Watch>,
        local: &HashSet<Ipv4Addr>,
        timeouts: &Timeouts,
        now: SystemTime,
    ) {
        self.pods.retain(|ip, p| {
            watched
                .get(ip)
                .is_some_and(|w| w.sandbox_id == p.sandbox_id)
        });
        for (ip, watch) in watched {
            self.pods.entry(*ip).or_insert_with(|| PodTrack {
                sandbox_id: watch.sandbox_id.clone(),
                last: None,
            });
        }

        let previous = self.dumped.replace(now);
        let mut seen = HashMap::with_capacity(self.seen.len());
        for conn in conns {
            let entry = &conn.entry;
            let concerns_watched_pod = |ip: Ipv4Addr| {
                watched
                    .get(&ip)
                    .is_some_and(|w| !is_probe(entry, ip, local, |port| w.is_probe_port(port)))
            };
            let client = *entry.original.src.ip();
            let server = *entry.reply.src.ip();
            let client_pod = concerns_watched_pod(client);
            let server_pod = server != client && concerns_watched_pod(server);
            let key = (conn.id, entry.original);
            if !(client_pod || server_pod) || seen.contains_key(&key) {
                continue;
            }
            let old = self.seen.remove(&key);
            let estimate = estimate(conn, timeouts, now);
            let last = match (previous, &old) {
                (None, _) => estimate,
                (Some(previous), Some(old))
                    if !moved(old, conn, now.duration_since(previous).unwrap_or_default()) =>
                {
                    old.last
                }
                (Some(previous), _) => estimate.max(previous + Duration::from_millis(1)).min(now),
            };
            // Only the server's pod gets the time: the client opened the
            // connection. An entry that a watched pod opened is tracked all
            // the same, so that its time stays if its server becomes watched.
            if server_pod {
                let pod = self.pod(server);
                pod.last = pod.last.max(Some(last));
            }
            seen.insert(
                key,
                Seen {
                    counters: conn.counters,
                    tcp_state: conn.tcp_state,
                    timeout_secs: entry.timeout_secs,
                    last,
                },
            );
        }
        self.seen = seen;
    }

    fn pod(&mut self, ip: Ipv4Addr) -> &mut PodTrack {
        self.pods.get_mut(&ip).expect("a watched pod has a track")
    }

    fn last(&self, ip: Ipv4Addr) -> Option<SystemTime> {
        self.pods.get(&ip).and_then(|p| p.last)
    }
}

/// Why an awake pod is not idle.
#[derive(Debug, Clone, PartialEq)]
pub enum NotIdle {
    /// Its IP is not recorded, so its traffic cannot be told.
    NoAddress,
    /// Conntrack could not be read.
    Unmeasured(String),
    /// It saw a packet on a connection opened to it less than its idle
    /// timeout ago.
    Traffic { ago: Duration },
    /// It was created or woke less than its idle timeout ago.
    Woke { ago: Duration },
    /// Its VM used more CPU than the threshold over the window.
    Cpu { vcpus: f64, threshold: f64 },
    /// Its VM's CPU is not measured yet, or its process could not be read.
    CpuUnmeasured(String),
}

impl fmt::Display for NotIdle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotIdle::NoAddress => write!(f, "no IP recorded"),
            NotIdle::Unmeasured(e) => write!(f, "traffic not measured: {e}"),
            NotIdle::Traffic { ago } => write!(f, "a packet {ago:.1?} ago"),
            NotIdle::Woke { ago } => write!(f, "awake for {ago:.1?}"),
            NotIdle::Cpu { vcpus, threshold } => {
                write!(f, "VM CPU {vcpus:.2} vCPU, above {threshold}")
            }
            NotIdle::CpuUnmeasured(e) => write!(f, "VM CPU not measured: {e}"),
        }
    }
}

/// An awake pod that is idle.
#[derive(Debug, Clone, PartialEq)]
pub struct Candidate {
    pub ip: Ipv4Addr,
    /// Its last packet other than a probe, if conntrack saw one.
    pub last_traffic: Option<SystemTime>,
    /// The later of that and when it last became awake.
    pub idle_since: SystemTime,
    /// Its VM's CPU in vCPUs over the window; none when no CPU rule applies
    /// to the pod.
    pub vcpus: Option<f64>,
}

impl Candidate {
    /// The recheck in the `pausing` hook, once the hold is set: a fresh dump,
    /// and an error if the pod saw a packet on a connection opened to it
    /// after the dump that made it a candidate. An error refuses the pause.
    pub fn recheck(&self, conntrack: &dyn ConntrackLookup) -> Result<(), String> {
        match conntrack.last_activity(self.ip)? {
            Some(last) if self.last_traffic.is_none_or(|then| last > then) => {
                Err("a packet after the idle decision".into())
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    Idle(Candidate),
    Busy(Vec<NotIdle>),
}

/// The judgement of an awake pod at a poll.
#[derive(Debug, Clone, PartialEq)]
pub struct IdleStatus {
    pub sandbox_id: String,
    pub verdict: Verdict,
    /// When the pod is idle by traffic if it sees no packet until then: its
    /// idle timeout after its last activity. The idle loop polls again then,
    /// before the next interval.
    pub due: SystemTime,
}

struct State {
    tracker: Tracker,
    watched: HashMap<Ipv4Addr, Watch>,
    local: HashSet<Ipv4Addr>,
    cpu: CpuMeter,
}

/// Each pod's activity and idle timer, over the node's conntrack table. It is
/// the node service's [`ConntrackLookup`].
pub struct Monitor {
    source: Box<dyn Source>,
    clock: Clock,
    idle_timeout: Duration,
    /// The node's CPU rule, which a pod's annotations override.
    cpu_threshold: Option<f64>,
    cpu_window: Duration,
    state: Mutex<State>,
}

impl Monitor {
    /// A monitor of the node's kernel with the node service's settings.
    pub fn new(settings: &Settings, clock: Clock) -> Monitor {
        Monitor::with_source(Box::<Kernel>::default(), settings, clock)
    }

    pub fn with_source(source: Box<dyn Source>, settings: &Settings, clock: Clock) -> Monitor {
        Monitor {
            source,
            clock,
            idle_timeout: settings.idle_timeout,
            cpu_threshold: settings.cpu_threshold,
            cpu_window: settings.cpu_window,
            state: Mutex::new(State {
                tracker: Tracker::default(),
                watched: HashMap::new(),
                local: HashSet::new(),
                cpu: CpuMeter::default(),
            }),
        }
    }

    /// One poll (blocking): watches every pod in `pods` that has an address,
    /// with its probe ports from `probes`, dumps conntrack once, and judges
    /// each awake pod.
    pub fn poll(&self, pods: &[PodInfo], probes: &dyn ProbeBook) -> Vec<IdleStatus> {
        let now = self.clock.now();
        let mut state = lock(&self.state);
        state.watched = pods
            .iter()
            .filter(|p| !matches!(p.state, PodState::Failed | PodState::Deleting))
            .filter_map(|p| {
                let ip = p.identity.ip.as_deref()?.parse().ok()?;
                let sandbox_id = p.identity.sandbox_id.clone();
                let probes = probes.probes(&sandbox_id);
                Some((ip, Watch { sandbox_id, probes }))
            })
            .collect();
        let measured = self.refresh(&mut state, now);
        if let Err(e) = &measured {
            warn!("activity: {e}");
        }
        let awake: Vec<&PodInfo> = pods.iter().filter(|p| p.state == PodState::Awake).collect();
        state
            .cpu
            .retain(|id| awake.iter().any(|p| p.identity.sandbox_id == id));
        awake
            .into_iter()
            .map(|pod| self.judge(&mut state, pod, measured.as_ref().err(), now))
            .collect()
    }

    fn refresh(&self, state: &mut State, now: SystemTime) -> Result<(), String> {
        let started = Instant::now();
        let conns = self.source.dump()?;
        let took = started.elapsed();
        match self.source.local_addrs() {
            Ok(local) => state.local = local,
            Err(e) => warn!("activity: the node's addresses: {e}"),
        }
        let timeouts = self.source.timeouts();
        let State {
            tracker,
            watched,
            local,
            ..
        } = state;
        tracker.update(&conns, watched, local, &timeouts, now);
        debug!(
            "activity: {} conntrack entries in {took:?}, {} of watched pods",
            conns.len(),
            tracker.seen.len()
        );
        Ok(())
    }

    fn judge(
        &self,
        state: &mut State,
        pod: &PodInfo,
        unmeasured: Option<&String>,
        now: SystemTime,
    ) -> IdleStatus {
        let sandbox_id = pod.identity.sandbox_id.clone();
        let idle_timeout = pod
            .identity
            .idle_timeout_ms
            .map_or(self.idle_timeout, Duration::from_millis);
        let awake_since = UNIX_EPOCH + Duration::from_millis(pod.since_ms);
        let ip = pod.identity.ip.as_deref().and_then(|ip| ip.parse().ok());
        let last_traffic = ip.and_then(|ip| state.tracker.last(ip));
        let idle_since = last_traffic.map_or(awake_since, |t| t.max(awake_since));
        let due = idle_since + idle_timeout;
        let ago = |t: SystemTime| now.duration_since(t).unwrap_or_default();

        let mut reasons = Vec::new();
        match (ip, unmeasured) {
            (None, _) => reasons.push(NotIdle::NoAddress),
            (Some(_), Some(e)) => reasons.push(NotIdle::Unmeasured(e.clone())),
            (Some(_), None) => {}
        }
        if now < due {
            reasons.push(match last_traffic {
                Some(last) if last > awake_since => NotIdle::Traffic { ago: ago(last) },
                _ => NotIdle::Woke {
                    ago: ago(awake_since),
                },
            });
        }
        let mut vcpus = None;
        if let Some((threshold, window)) = self.cpu_rule(&pod.identity) {
            let reading = match pod.vm {
                None => Err("no VM process".to_string()),
                Some(vm) => self
                    .source
                    .cpu(vm)
                    .map(|sample| state.cpu.record(&sandbox_id, vm, sample, window)),
            };
            match reading {
                Ok(Some(v)) if v > threshold => reasons.push(NotIdle::Cpu {
                    vcpus: v,
                    threshold,
                }),
                Ok(Some(v)) => vcpus = Some(v),
                Ok(None) => reasons.push(NotIdle::CpuUnmeasured("a first reading".into())),
                Err(e) => reasons.push(NotIdle::CpuUnmeasured(e)),
            }
        }
        let verdict = match ip {
            Some(ip) if reasons.is_empty() => Verdict::Idle(Candidate {
                ip,
                last_traffic,
                idle_since,
                vcpus,
            }),
            _ => Verdict::Busy(reasons),
        };
        IdleStatus {
            sandbox_id,
            verdict,
            due,
        }
    }
}

impl Monitor {
    /// The CPU threshold and window of a pod: its annotations', or else the
    /// node's; none when no CPU rule applies to it.
    fn cpu_rule(&self, identity: &PodIdentity) -> Option<(f64, Duration)> {
        let threshold = match identity.cpu_threshold_milli {
            Some(0) => None,
            Some(milli) => Some(f64::from(milli) / 1000.0),
            None => self.cpu_threshold,
        }?;
        let window = identity
            .cpu_window_ms
            .map_or(self.cpu_window, Duration::from_millis);
        Some((threshold, window))
    }
}

impl ConntrackLookup for Monitor {
    fn by_reply(&self, reply: &Tuple) -> Result<Option<ConnEntry>, String> {
        Ok(self.source.get(reply)?.map(|c| c.entry))
    }

    /// By a fresh dump. Only for a pod that the last poll watched, with its
    /// probe ports: an error otherwise.
    fn last_activity(&self, ip: Ipv4Addr) -> Result<Option<SystemTime>, String> {
        let now = self.clock.now();
        let mut state = lock(&self.state);
        if !state.watched.contains_key(&ip) {
            return Err(format!("no pod at {ip} is watched"));
        }
        self.refresh(&mut state, now)?;
        Ok(state.tracker.last(ip))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::conntrack::tests::{counters, ct, tcp, udp, IPS_SEEN_REPLY};
    use crate::lifecycle::PodIdentity;
    use crate::probe::{MockProbeBook, Probe, ProbeAction, ProbeRole};

    const POD: Ipv4Addr = Ipv4Addr::new(10, 42, 0, 2);
    const PEER: Ipv4Addr = Ipv4Addr::new(10, 42, 0, 7);
    const T0: u64 = 1_760_000_000;

    fn at(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(T0 + secs)
    }

    fn local() -> HashSet<Ipv4Addr> {
        [Ipv4Addr::new(10, 42, 0, 1), Ipv4Addr::new(192, 168, 64, 2)].into()
    }

    fn probes(port: u16) -> PodProbes {
        PodProbes {
            probes: vec![Probe {
                container: "app".into(),
                role: ProbeRole::Readiness,
                action: ProbeAction::Http {
                    port,
                    path: "/healthz".into(),
                    https: false,
                    headers: Vec::new(),
                },
                period_secs: 10,
                timeout_secs: 1,
                last: None,
            }],
            ready: true,
            at_ms: 0,
        }
    }

    fn watched() -> HashMap<Ipv4Addr, Watch> {
        HashMap::from([
            (
                POD,
                Watch {
                    sandbox_id: "pod".into(),
                    probes: Some(Arc::new(probes(8080))),
                },
            ),
            (
                PEER,
                Watch {
                    sandbox_id: "peer".into(),
                    probes: None,
                },
            ),
        ])
    }

    /// Kube-proxy's timeouts.
    fn timeouts() -> Timeouts {
        Timeouts {
            tcp: [0, 120, 60, 86_400, 120, 3_600, 30, 120, 10, 120],
            ..Timeouts::default()
        }
    }

    fn established(original: Tuple, reply: Tuple, age: u32) -> Conn {
        ct(original, reply, 86_400 - age)
    }

    struct Fixture {
        tracker: Tracker,
        watched: HashMap<Ipv4Addr, Watch>,
    }

    impl Fixture {
        fn new() -> Fixture {
            Fixture {
                tracker: Tracker::default(),
                watched: watched(),
            }
        }

        fn dump(&mut self, secs: u64, conns: &[Conn]) -> Option<SystemTime> {
            self.tracker
                .update(conns, &self.watched, &local(), &timeouts(), at(secs));
            self.tracker.last(POD)
        }
    }

    #[test]
    fn kubelet_probes_are_not_activity_but_nodeport_is() {
        let mut f = Fixture::new();
        let probe = tcp([10, 42, 0, 1], 40000, [10, 42, 0, 2], 8080);
        let kubelet = established(probe, probe.inverse(), 0);
        assert_eq!(f.dump(0, &[kubelet]), None);
        assert!(f.tracker.seen.is_empty());

        let nodeport = Conn {
            id: 8,
            ..established(
                tcp([100, 96, 0, 1], 50000, [100, 96, 0, 2], 31080),
                tcp([10, 42, 0, 2], 8080, [10, 42, 0, 1], 40001),
                3,
            )
        };
        assert_eq!(f.dump(15, &[kubelet, nodeport]), Some(at(13)));
        // From the node to a port that is not a probe's: not kubelet.
        let other = tcp([10, 42, 0, 1], 40002, [10, 42, 0, 2], 9000);
        let mut f = Fixture::new();
        assert_eq!(
            f.dump(0, &[established(other, other.inverse(), 5)]),
            Some(at(0) - Duration::from_secs(4))
        );
    }

    #[test]
    fn the_first_dump_takes_ages_as_they_are() {
        let mut f = Fixture::new();
        let inbound = tcp([10, 42, 0, 9], 41000, [10, 42, 0, 2], 80);
        let outbound = udp([10, 42, 0, 2], 41001, [10, 43, 0, 10], 53);
        let dns = Conn {
            id: 9,
            status: IPS_SEEN_REPLY,
            ..ct(outbound, udp([10, 42, 0, 3], 53, [10, 42, 0, 2], 41001), 21)
        };
        // (100 - 1) s ago. The DNS lookup, (30 - 21 - 1) = 8 s ago, is the
        // pod's own connection, which does not count.
        let last = f.dump(100, &[established(inbound, inbound.inverse(), 100), dns]);
        assert_eq!(last, Some(at(1)));
    }

    #[test]
    fn counters_tell_whether_an_entry_moved() {
        let mut f = Fixture::new();
        let flow = tcp([10, 42, 0, 9], 41000, [10, 42, 0, 2], 80);
        let with = |left: u32, c| Conn {
            counters: Some(c),
            ..ct(flow, flow.inverse(), left)
        };
        assert_eq!(
            f.dump(0, &[with(86_390, counters(5, 500, 4, 400))]),
            Some(at(0) - Duration::from_secs(9))
        );
        // No packet: the timeout counts down; the time stays.
        let still = f.dump(15, &[with(86_375, counters(5, 500, 4, 400))]);
        assert_eq!(still, Some(at(0) - Duration::from_secs(9)));
        // A packet 3 s ago, in the reply direction only.
        let moved = f.dump(30, &[with(86_397, counters(5, 500, 5, 460))]);
        assert_eq!(moved, Some(at(28)));
        // Counters that moved but a timeout that reads older than the last
        // dump: just after the last dump.
        let late = f.dump(45, &[with(86_370, counters(6, 560, 5, 460))]);
        assert_eq!(late, Some(at(30) + Duration::from_millis(1)));
    }

    #[test]
    fn without_counters_the_countdown_tells() {
        let mut f = Fixture::new();
        let flow = tcp([10, 42, 0, 9], 41000, [10, 42, 0, 2], 80);
        let with = |left: u32, state: u8| Conn {
            tcp_state: Some(state),
            ..ct(flow, flow.inverse(), left)
        };
        let first = Some(at(0) - Duration::from_secs(99));
        assert_eq!(f.dump(0, &[with(86_300, 3)]), first);
        // 15 s later it reads 15 s less, give or take the rounding.
        assert_eq!(f.dump(15, &[with(86_284, 3)]), first);
        assert_eq!(f.dump(30, &[with(86_271, 3)]), first);
        // Reset by a packet 5 s ago.
        assert_eq!(f.dump(45, &[with(86_395, 3)]), Some(at(41)));
        // A FIN: a new state, with a shorter timeout.
        assert_eq!(f.dump(60, &[with(115, 4)]), Some(at(56)));
        // On the 300 s timeout of unacknowledged data, counting down.
        assert_eq!(f.dump(75, &[with(296, 3)]), Some(at(72)));
        assert_eq!(f.dump(90, &[with(281, 3)]), Some(at(72)));
    }

    #[test]
    fn a_new_entry_after_a_dump_is_after_it() {
        let mut f = Fixture::new();
        assert_eq!(f.dump(0, &[]), None);
        let flow = tcp([10, 42, 0, 9], 41000, [10, 42, 0, 2], 443);
        // Its timeout says 10 s ago, before the last dump: a sysctl changed.
        let last = f.dump(5, &[established(flow, flow.inverse(), 11)]);
        assert_eq!(last, Some(at(0) + Duration::from_millis(1)));
        // A duplicate in one dump counts once: an entry that did not move
        // keeps its time.
        let still = established(flow, flow.inverse(), 26);
        assert_eq!(f.dump(20, &[still, still]), last);
    }

    #[test]
    fn a_pod_s_last_activity_only_grows_with_packets() {
        let mut f = Fixture::new();
        let quiet = tcp([10, 42, 0, 9], 41000, [10, 42, 0, 2], 80);
        let busy = tcp([10, 42, 0, 8], 41001, [10, 42, 0, 2], 443);
        let busy = |id: u32| Conn {
            id,
            ..established(busy, busy.inverse(), 600)
        };
        let decided = f.dump(0, &[established(quiet, quiet.inverse(), 400), busy(1)]);
        // The recheck: entries that did not move, and one that ended.
        let later = f.dump(3, &[established(quiet, quiet.inverse(), 403)]);
        assert_eq!(later, decided);
        // A new connection right after the decision.
        let fresh = f.dump(4, &[established(quiet, quiet.inverse(), 404), busy(2)]);
        assert!(fresh > decided);
    }

    #[test]
    fn connections_the_pod_opened_are_not_activity() {
        let mut f = Fixture::new();
        // The pod's own connection to a broker that pushes, and a call that
        // the pod opened to its peer pod through a Service.
        let push = |left: u32, c| Conn {
            id: 1,
            counters: Some(c),
            ..ct(
                tcp([10, 42, 0, 2], 41000, [10, 43, 0, 20], 5672),
                tcp([10, 42, 0, 30], 5672, [10, 42, 0, 2], 41000),
                left,
            )
        };
        let call = Conn {
            id: 2,
            ..established(
                tcp([10, 42, 0, 2], 41001, [10, 43, 0, 50], 80),
                tcp([10, 42, 0, 7], 8080, [10, 42, 0, 2], 41001),
                0,
            )
        };
        assert_eq!(
            f.dump(0, &[push(86_400, counters(5, 300, 9, 900)), call]),
            None
        );
        assert_eq!(
            f.dump(15, &[push(86_400, counters(5, 300, 12, 1_200))]),
            None
        );
        // The peer pod is the server of the call: it counts for the peer.
        assert_eq!(f.tracker.last(PEER), Some(at(0)));
    }

    #[test]
    fn a_pod_that_leaves_is_forgotten() {
        let mut f = Fixture::new();
        let flow = tcp([10, 42, 0, 9], 41000, [10, 42, 0, 2], 80);
        f.dump(0, &[established(flow, flow.inverse(), 0)]);
        assert!(f.tracker.last(POD).is_some());
        f.watched.remove(&POD);
        f.dump(15, &[established(flow, flow.inverse(), 15)]);
        assert!(f.tracker.last(POD).is_none());
        // Its IP, reused by a new pod.
        let new_pod = |id: &str| Watch {
            sandbox_id: id.into(),
            probes: None,
        };
        f.watched.insert(POD, new_pod("new"));
        assert_eq!(f.dump(30, &[]), None);
        f.dump(45, &[established(flow, flow.inverse(), 0)]);
        assert!(f.tracker.last(POD).is_some());
        // Reused again before a dump saw the IP free.
        f.watched.insert(POD, new_pod("newer"));
        assert_eq!(f.dump(60, &[]), None);
    }

    struct Fake {
        conns: Arc<Mutex<Result<Vec<Conn>, String>>>,
        cpu: Arc<Mutex<HashMap<i32, Sample>>>,
    }

    impl Source for Fake {
        fn dump(&self) -> Result<Vec<Conn>, String> {
            lock(&self.conns).clone()
        }

        fn get(&self, reply: &Tuple) -> Result<Option<Conn>, String> {
            Ok(lock(&self.conns)
                .clone()?
                .into_iter()
                .find(|c| c.entry.reply == *reply))
        }

        fn local_addrs(&self) -> Result<HashSet<Ipv4Addr>, String> {
            Ok(local())
        }

        fn timeouts(&self) -> Timeouts {
            timeouts()
        }

        fn cpu(&self, process: VmProcess) -> Result<Sample, String> {
            lock(&self.cpu)
                .get(&process.pid)
                .copied()
                .ok_or_else(|| format!("VM process {}: gone", process.pid))
        }
    }

    struct Node {
        monitor: Monitor,
        /// The CPU annotations of the pod `pod`, in thousandths of a vCPU.
        cpu_threshold_milli: Option<u32>,
        now: Arc<Mutex<SystemTime>>,
        conns: Arc<Mutex<Result<Vec<Conn>, String>>>,
        cpu: Arc<Mutex<HashMap<i32, Sample>>>,
        book: MockProbeBook,
    }

    const VM: VmProcess = VmProcess {
        pid: 100,
        start_time: Some(1),
    };

    impl Node {
        /// A node with a 60 s idle timeout and the default CPU settings.
        fn new() -> Node {
            Node::with_settings(Settings {
                idle_timeout: Duration::from_secs(60),
                ..Settings::default()
            })
        }

        fn with_settings(settings: Settings) -> Node {
            let now = Arc::new(Mutex::new(at(0)));
            let clock = {
                let now = now.clone();
                Clock::from_fn(move || *lock(&now))
            };
            let conns = Arc::new(Mutex::new(Ok(Vec::new())));
            let cpu = Arc::new(Mutex::new(HashMap::new()));
            let source = Fake {
                conns: conns.clone(),
                cpu: cpu.clone(),
            };
            let book = MockProbeBook::default();
            lock(&book.pods).insert("pod".into(), probes(8080));
            Node {
                monitor: Monitor::with_source(Box::new(source), &settings, clock),
                cpu_threshold_milli: None,
                now,
                conns,
                cpu,
                book,
            }
        }

        /// At `secs`, with the VM process, started at 0 s, having used
        /// `cpu` seconds.
        fn poll(&self, secs: u64, cpu: f64, conns: &[Conn]) -> Vec<IdleStatus> {
            self.poll_with(secs, cpu, Ok(conns.to_vec()))
        }

        fn poll_with(
            &self,
            secs: u64,
            cpu: f64,
            conns: Result<Vec<Conn>, String>,
        ) -> Vec<IdleStatus> {
            *lock(&self.now) = at(secs);
            *lock(&self.conns) = conns;
            lock(&self.cpu).insert(
                VM.pid,
                Sample {
                    at: Duration::from_secs(1000 + secs),
                    cpu: Duration::from_secs_f64(cpu),
                    started: Duration::from_secs(1000),
                },
            );
            let mut pod = awake("pod", Some("10.42.0.2"), 0, None);
            pod.identity.cpu_threshold_milli = self.cpu_threshold_milli;
            self.monitor.poll(&[pod], &self.book)
        }
    }

    fn awake(id: &str, ip: Option<&str>, since: u64, idle_timeout: Option<u64>) -> PodInfo {
        PodInfo {
            identity: PodIdentity {
                sandbox_id: id.into(),
                ip: ip.map(str::to_string),
                idle_timeout_ms: idle_timeout.map(|s| s * 1000),
                ..PodIdentity::default()
            },
            state: PodState::Awake,
            phase: None,
            generation: 1,
            created_ms: 0,
            since_ms: (T0 + since) * 1000,
            leases: Vec::new(),
            waiters: 0,
            starting_execs: 0,
            creates: 0,
            execs: Vec::new(),
            last_wake: None,
            last_sleep: None,
            checkpoint_bytes: None,
            wake_failures: 0,
            failure: None,
            vm: Some(VM),
        }
    }

    fn kind(reason: &NotIdle) -> &'static str {
        match reason {
            NotIdle::NoAddress => "no-address",
            NotIdle::Unmeasured(_) => "unmeasured",
            NotIdle::Traffic { .. } => "traffic",
            NotIdle::Woke { .. } => "woke",
            NotIdle::Cpu { .. } => "cpu",
            NotIdle::CpuUnmeasured(_) => "cpu-unmeasured",
        }
    }

    fn reasons(status: &IdleStatus) -> Vec<&'static str> {
        match &status.verdict {
            Verdict::Busy(reasons) => reasons.iter().map(kind).collect(),
            Verdict::Idle(_) => Vec::new(),
        }
    }

    #[test]
    fn a_pod_is_idle_a_timeout_after_its_wake_with_only_probes() {
        let node = Node::new();
        let probe = tcp([10, 42, 0, 1], 40000, [10, 42, 0, 2], 8080);
        let kubelet = |left| ct(probe, probe.inverse(), left);
        let status = &node.poll(15, 0.3, &[kubelet(110)])[0];
        assert_eq!(reasons(status), ["woke"]);
        assert_eq!(status.due, at(60));
        assert_eq!(reasons(&node.poll(59, 0.4, &[kubelet(115)])[0]), ["woke"]);
        let status = &node.poll(60, 0.4, &[kubelet(114)])[0];
        let Verdict::Idle(candidate) = &status.verdict else {
            panic!("{status:?}");
        };
        assert_eq!(candidate.ip, POD);
        assert_eq!(candidate.last_traffic, None);
        assert_eq!(candidate.idle_since, at(0));
        assert!(candidate
            .vcpus
            .is_some_and(|v| (v - 0.4 / 60.0).abs() < 1e-9));
    }

    #[test]
    fn traffic_cpu_and_errors_keep_a_pod_awake() {
        let node = Node::new();
        let flow = tcp([10, 42, 0, 9], 41000, [10, 42, 0, 2], 80);
        let status = &node.poll(50, 0.0, &[established(flow, flow.inverse(), 5)])[0];
        assert_eq!(status.due, at(106));
        assert_eq!(
            status.verdict,
            Verdict::Busy(vec![NotIdle::Traffic {
                ago: Duration::from_secs(4)
            }])
        );
        // The same connection, quiet since.
        let quiet = established(flow, flow.inverse(), 61);
        assert!(matches!(
            node.poll(106, 0.0, &[quiet])[0].verdict,
            Verdict::Idle(_)
        ));
        // A busy loop for the last minute.
        let status = &node.poll(166, 50.0, &[])[0];
        assert_eq!(
            status.verdict,
            Verdict::Busy(vec![NotIdle::Cpu {
                vcpus: 50.0 / 60.0,
                threshold: 0.5
            }])
        );
        // Conntrack cannot be read.
        let error = Err("ctnetlink receive: timed out".to_string());
        let status = &node.poll_with(300, 50.0, error)[0];
        assert_eq!(reasons(status), ["unmeasured"]);
        assert_eq!(
            status.verdict,
            Verdict::Busy(vec![NotIdle::Unmeasured(
                "ctnetlink receive: timed out".into()
            )])
        );
        assert!(matches!(
            node.poll(301, 50.0, &[])[0].verdict,
            Verdict::Idle(_)
        ));
    }

    #[test]
    fn with_the_cpu_rule_off_a_busy_pod_is_idle() {
        let node = Node::with_settings(Settings {
            idle_timeout: Duration::from_secs(60),
            cpu_threshold: None,
            ..Settings::default()
        });
        let status = &node.poll(100, 90.0, &[])[0];
        let Verdict::Idle(candidate) = &status.verdict else {
            panic!("{status:?}");
        };
        assert_eq!(candidate.vcpus, None, "not measured");
        // A pod's annotation turns the rule on for it.
        let mut node = node;
        node.cpu_threshold_milli = Some(500);
        node.poll(120, 90.0, &[]);
        assert_eq!(reasons(&node.poll(160, 130.0, &[])[0]), ["cpu"]);
    }

    #[test]
    fn a_pods_annotation_turns_the_cpu_rule_off_or_moves_it() {
        let mut node = Node::new();
        node.cpu_threshold_milli = Some(0);
        assert!(matches!(
            node.poll(100, 90.0, &[])[0].verdict,
            Verdict::Idle(_)
        ));
        let mut node = Node::new();
        node.cpu_threshold_milli = Some(2000);
        assert_eq!(reasons(&node.poll(100, 0.0, &[])[0]), ["cpu-unmeasured"]);
        // 1.5 vCPU, under the pod's 2.
        assert!(matches!(
            node.poll(130, 45.0, &[])[0].verdict,
            Verdict::Idle(_)
        ));
        node.cpu_threshold_milli = Some(1000);
        let status = &node.poll(160, 90.0, &[])[0];
        assert_eq!(
            status.verdict,
            Verdict::Busy(vec![NotIdle::Cpu {
                vcpus: 1.5,
                threshold: 1.0
            }])
        );
    }

    #[test]
    fn a_pod_with_no_address_or_vm_reading_is_not_idle() {
        let node = Node::new();
        *lock(&node.now) = at(1000);
        let pods = [
            awake("noip", None, 0, None),
            PodInfo {
                vm: Some(VmProcess {
                    pid: 7,
                    start_time: None,
                }),
                ..awake("novm", Some("10.42.0.30"), 0, None)
            },
        ];
        let statuses = node.monitor.poll(&pods, &node.book);
        assert_eq!(reasons(&statuses[0]), ["no-address", "cpu-unmeasured"]);
        assert_eq!(reasons(&statuses[1]), ["cpu-unmeasured"]);
        let Verdict::Busy(r) = &statuses[1].verdict else {
            unreachable!()
        };
        assert_eq!(r[0].to_string(), "VM CPU not measured: VM process 7: gone");
    }

    #[test]
    fn the_annotation_overrides_the_node_timeout() {
        let node = Node::new();
        *lock(&node.now) = at(15);
        lock(&node.cpu).insert(
            VM.pid,
            Sample {
                at: Duration::from_secs(1015),
                cpu: Duration::ZERO,
                started: Duration::from_secs(1000),
            },
        );
        let pods = [
            awake("short", Some("10.42.0.20"), 0, Some(10)),
            awake("long", Some("10.42.0.21"), 0, Some(3600)),
        ];
        let statuses = node.monitor.poll(&pods, &node.book);
        assert!(matches!(statuses[0].verdict, Verdict::Idle(_)));
        assert_eq!(statuses[1].due, at(3600));
        assert_eq!(reasons(&statuses[1]), ["woke"]);
    }

    #[test]
    fn the_recheck_refuses_after_a_packet() {
        let node = Node::new();
        let flow = tcp([10, 42, 0, 9], 41000, [10, 42, 0, 2], 80);
        node.poll(50, 0.0, &[established(flow, flow.inverse(), 20)]);
        let statuses = node.poll(100, 0.0, &[established(flow, flow.inverse(), 70)]);
        let Verdict::Idle(candidate) = &statuses[0].verdict else {
            panic!("{statuses:?}");
        };
        assert_eq!(candidate.last_traffic, Some(at(31)));
        // The hold is set; nothing moved.
        *lock(&node.now) = at(105);
        *lock(&node.conns) = Ok(vec![established(flow, flow.inverse(), 75)]);
        assert_eq!(candidate.recheck(&node.monitor), Ok(()));
        // A request reached the pod before the hold.
        *lock(&node.now) = at(106);
        *lock(&node.conns) = Ok(vec![established(flow, flow.inverse(), 0)]);
        assert_eq!(
            candidate.recheck(&node.monitor),
            Err("a packet after the idle decision".into())
        );
        // A pod no poll watched.
        assert!(node
            .monitor
            .last_activity(Ipv4Addr::new(10, 42, 0, 99))
            .is_err());
    }

    #[test]
    fn the_recheck_works_with_the_mock() {
        let mock = super::super::MockConntrack::default();
        let candidate = Candidate {
            ip: POD,
            last_traffic: Some(at(10)),
            idle_since: at(10),
            vcpus: None,
        };
        assert_eq!(candidate.recheck(&mock), Ok(()));
        lock(&mock.activity).insert(POD, at(10));
        assert_eq!(candidate.recheck(&mock), Ok(()));
        lock(&mock.activity).insert(POD, at(80));
        assert!(candidate.recheck(&mock).is_err());
    }

    #[test]
    fn by_reply_finds_a_nodeport_entry() {
        let node = Node::new();
        let nodeport = ct(
            tcp([100, 96, 0, 1], 50000, [100, 96, 0, 2], 31080),
            tcp([10, 42, 0, 2], 8080, [10, 42, 0, 1], 40001),
            100,
        );
        *lock(&node.conns) = Ok(vec![nodeport]);
        let syn = tcp([10, 42, 0, 1], 40001, [10, 42, 0, 2], 8080);
        let entry = node.monitor.by_reply(&syn.inverse()).unwrap().unwrap();
        assert!(entry.is_nat());
        assert_eq!(node.monitor.by_reply(&syn), Ok(None));
    }

    /// Against the node's kernel (see README.md, "Tests"): watches the pod at
    /// `LIVE_POD_IP`, whose probe ports are `LIVE_PROBE_PORTS`
    /// (comma-separated), dumps conntrack `LIVE_POLLS` times (3),
    /// `LIVE_INTERVAL` seconds apart (15), and prints what each dump found.
    #[test]
    #[ignore = "needs a node and a pod"]
    fn live_watch_a_pod() {
        use super::super::conntrack::tests::env_number;

        let ip: Ipv4Addr = std::env::var("LIVE_POD_IP")
            .expect("LIVE_POD_IP")
            .parse()
            .unwrap();
        let ports: Vec<u16> = std::env::var("LIVE_PROBE_PORTS")
            .unwrap_or_default()
            .split(',')
            .filter_map(|p| p.parse().ok())
            .collect();
        let pod_probes = PodProbes {
            probes: ports
                .iter()
                .map(|&port| Probe {
                    action: ProbeAction::Tcp { port },
                    ..probes(port).probes[0].clone()
                })
                .collect(),
            ready: true,
            at_ms: 0,
        };
        let watched = HashMap::from([(
            ip,
            Watch {
                sandbox_id: "live".into(),
                probes: Some(Arc::new(pod_probes)),
            },
        )]);
        let kernel = Kernel::default();
        let local = kernel.local_addrs().unwrap();
        let mut tracker = Tracker::default();
        for i in 0..env_number("LIVE_POLLS", 3) {
            if i > 0 {
                let interval = env_number("LIVE_INTERVAL", 15) as u64;
                std::thread::sleep(Duration::from_secs(interval));
            }
            let now = SystemTime::now();
            let started = Instant::now();
            let conns = kernel.dump().unwrap();
            let took = started.elapsed();
            tracker.update(&conns, &watched, &local, &kernel.timeouts(), now);
            let tracked = started.elapsed() - took;
            let left_out = conns
                .iter()
                .filter(|c| is_probe(&c.entry, ip, &local, |p| ports.contains(&p)))
                .count();
            let ago = tracker.last(ip).map(|t| now.duration_since(t).unwrap());
            eprintln!(
                "dump {i}: {} entries in {took:?}, tracked in {tracked:?}; {left_out} probe \
                 entries left out, {} tracked; last packet {ago:?} ago",
                conns.len(),
                tracker.seen.len(),
            );
        }
    }
}
