//! Probe specs from the pod watch, and the node's [`ProbeBook`]: each pod's
//! probes as kubelet runs them, with named ports resolved and `$(VAR)`
//! expanded, kept in its record with their last results, so that they
//! survive a restart of the node service.

use std::collections::HashMap;
use std::fmt::Write;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::join_all;
use log::{info, warn};
use tokio::runtime::Handle;

use super::check;
use super::{PodProbes, Probe, ProbeAction, ProbeBook, ProbeResult, ProbeRole, EXEC_OUTPUT_MAX};
use crate::kube::pods::{self, Container, IntOrString};
use crate::kube::{Controller, Labels, Pod, PodSink};
use crate::lifecycle::{Clock, Lifecycle, LifecycleError, PodIdentity};
use crate::sync::lock;

/// Kubelet's defaults, which the API server fills in too.
const PERIOD_SECS: u32 = 10;
const TIMEOUT_SECS: u32 = 1;
/// The annotation of a static pod's mirror, whose uid is not the sandbox's.
const MIRROR_ANNOTATION: &str = "kubernetes.io/config.mirror";

fn role_name(role: ProbeRole) -> &'static str {
    match role {
        ProbeRole::Startup => "startup",
        ProbeRole::Liveness => "liveness",
        ProbeRole::Readiness => "readiness",
    }
}

fn is_sidecar(container: &Container) -> bool {
    container.restart_policy.as_deref() == Some("Always")
}

/// The containers that kubelet probes: the app containers, then the
/// sidecars (init containers that keep running), each with whether it is a
/// sidecar.
fn probed_containers(pod: &Pod) -> impl Iterator<Item = (&Container, bool)> {
    let sidecars = pod.spec.init_containers.iter().filter(|c| is_sidecar(c));
    pod.spec
        .containers
        .iter()
        .map(|c| (c, false))
        .chain(sidecars.map(|c| (c, true)))
}

/// The pod's probes as kubelet runs them, with no results. An error names a
/// probe that the node cannot stand in for while the pod sleeps: one that
/// kubelet cannot run either, or one of another host. Such a pod never
/// sleeps.
fn probes_of(pod: &Pod) -> Result<Vec<Probe>, String> {
    let mut probes = Vec::new();
    for (container, _) in probed_containers(pod) {
        let env = static_env(container);
        let specs = [
            (ProbeRole::Startup, &container.startup_probe),
            (ProbeRole::Liveness, &container.liveness_probe),
            (ProbeRole::Readiness, &container.readiness_probe),
        ];
        for (role, spec) in specs {
            let Some(spec) = spec else {
                continue;
            };
            let action = action(spec, container, &env, &pod.status.pod_ip).map_err(|e| {
                format!(
                    "the {} probe of container {}: {e}",
                    role_name(role),
                    container.name
                )
            })?;
            probes.push(Probe {
                container: container.name.clone(),
                role,
                action,
                period_secs: seconds(spec.period_seconds, PERIOD_SECS),
                timeout_secs: seconds(spec.timeout_seconds, TIMEOUT_SECS),
                last: None,
            });
        }
    }
    Ok(probes)
}

fn seconds(value: Option<i64>, default: u32) -> u32 {
    value
        .filter(|v| *v > 0)
        .map_or(default, |v| u32::try_from(v).unwrap_or(u32::MAX))
}

/// What a probe does, in kubelet's order of handlers.
fn action(
    spec: &pods::Probe,
    container: &Container,
    env: &HashMap<&str, &str>,
    pod_ip: &str,
) -> Result<ProbeAction, String> {
    if let Some(exec) = &spec.exec {
        let command = exec.command.iter().map(|arg| expand(arg, env)).collect();
        return Ok(ProbeAction::Exec { command });
    }
    if let Some(get) = &spec.http_get {
        same_host(&get.host, pod_ip)?;
        let https = match get.scheme.to_ascii_lowercase().as_str() {
            "" | "http" => false,
            "https" => true,
            other => return Err(format!("its scheme {other:?} is not HTTP or HTTPS")),
        };
        return Ok(ProbeAction::Http {
            port: container_port(&get.port, container)?,
            path: request_target(&get.path),
            https,
            headers: get
                .http_headers
                .iter()
                .map(|h| (h.name.clone(), h.value.clone()))
                .collect(),
        });
    }
    if let Some(tcp) = &spec.tcp_socket {
        same_host(&tcp.host, pod_ip)?;
        return Ok(ProbeAction::Tcp {
            port: container_port(&tcp.port, container)?,
        });
    }
    if let Some(grpc) = &spec.grpc {
        return Ok(ProbeAction::Grpc {
            port: valid_port(grpc.port)?,
            service: grpc.service.clone().filter(|s| !s.is_empty()),
        });
    }
    Err("it has no handler".into())
}

fn same_host(host: &str, pod_ip: &str) -> Result<(), String> {
    if host.is_empty() || host == pod_ip {
        Ok(())
    } else {
        Err(format!("it probes the host {host}, not the pod"))
    }
}

/// A probe's port as kubelet resolves it: a number, the name of one of the
/// container's own ports, or a number in a string.
fn container_port(port: &IntOrString, container: &Container) -> Result<u16, String> {
    let number = match port {
        IntOrString::Int(number) => *number,
        IntOrString::String(name) => match container.ports.iter().find(|p| p.name == *name) {
            Some(port) => port.container_port,
            None => name
                .parse()
                .map_err(|_| format!("the container has no port named {name:?}"))?,
        },
    };
    valid_port(number)
}

fn valid_port(port: i64) -> Result<u16, String> {
    u16::try_from(port)
        .ok()
        .filter(|p| *p > 0)
        .ok_or_else(|| format!("{port} is not a port number"))
}

/// The container's `env` values by name, as kubelet expands exec probe
/// commands: the last of a name wins, and one from `valueFrom` is empty.
fn static_env(container: &Container) -> HashMap<&str, &str> {
    container
        .env
        .iter()
        .map(|e| (e.name.as_str(), e.value.as_str()))
        .collect()
}

/// Expands `$(VAR)` as kubelet does (Kubernetes' `expansion.Expand`): `$$`
/// is a `$`, and an unknown or unclosed reference stays as it is.
pub fn expand(input: &str, env: &HashMap<&str, &str>) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let (mut checkpoint, mut cursor) = (0, 0);
    while cursor < bytes.len() {
        if bytes[cursor] == b'$' && cursor + 1 < bytes.len() {
            out.push_str(&input[checkpoint..cursor]);
            let rest = &input[cursor + 1..];
            let advance = match rest.as_bytes()[0] {
                b'$' => {
                    out.push('$');
                    1
                }
                b'(' => match rest.find(')') {
                    Some(end) => {
                        let name = &rest[1..end];
                        match env.get(name) {
                            Some(value) => out.push_str(value),
                            None => {
                                out.push_str("$(");
                                out.push_str(name);
                                out.push(')');
                            }
                        }
                        end + 1
                    }
                    None => {
                        out.push_str("$(");
                        1
                    }
                },
                // A `$` that starts nothing stays, and so does what follows.
                _ => {
                    out.push('$');
                    0
                }
            };
            cursor += advance;
            checkpoint = cursor + 1;
        }
        cursor += 1;
    }
    out.push_str(&input[checkpoint.min(input.len())..]);
    out
}

/// The request target that kubelet sends for an HTTP probe's path, as Go's
/// `url.Parse` and `URL.RequestURI` make it: the fragment dropped, the query
/// kept, and the path escaped unless it is validly escaped already. A path
/// that Go cannot parse is sent whole, escaped.
fn request_target(path: &str) -> String {
    if path.is_empty() {
        return "/".into();
    }
    parsed_target(path).unwrap_or_else(|| escape(path.as_bytes()))
}

fn parsed_target(path: &str) -> Option<String> {
    if path.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return None;
    }
    let (before, fragment) = path.split_once('#').unwrap_or((path, ""));
    unescape(fragment)?;
    let (path, query) = match before.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (before, None),
    };
    let unescaped = unescape(path)?;
    let mut target = if valid_encoded(path) {
        path.to_string()
    } else {
        escape(&unescaped)
    };
    if target.is_empty() {
        target.push('/');
    }
    if let Some(query) = query {
        target.push('?');
        target.push_str(query);
    }
    Some(target)
}

/// Bytes that Go leaves unescaped in a path.
fn path_safe(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"-_.~$&+,/:;=@".contains(&b)
}

fn valid_encoded(path: &str) -> bool {
    path.bytes()
        .all(|b| path_safe(b) || b"!'()*[]%".contains(&b))
}

fn escape(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    for &b in bytes {
        if path_safe(b) {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

fn unescape(text: &str) -> Option<Vec<u8>> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes.get(i + 1..i + 3)?;
            if !hex.iter().all(u8::is_ascii_hexdigit) {
                return None;
            }
            let hex = std::str::from_utf8(hex).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    Some(out)
}

/// Why the API server says the pod is not ready: it is terminating, not
/// running and Ready, or a container has not started or is not ready.
fn not_ready(pod: &Pod) -> Option<String> {
    if pod.metadata.deletion_timestamp.is_some() {
        return Some("the pod is terminating".into());
    }
    if pod.status.phase != "Running" {
        return Some(format!("the pod's phase is {:?}", pod.status.phase));
    }
    let ready = pod
        .status
        .conditions
        .iter()
        .any(|c| c.kind == "Ready" && c.status == "True");
    if !ready {
        return Some("the pod is not Ready".into());
    }
    for (container, sidecar) in probed_containers(pod) {
        let statuses = if sidecar {
            &pod.status.init_container_statuses
        } else {
            &pod.status.container_statuses
        };
        let name = &container.name;
        match statuses.iter().find(|s| s.name == *name) {
            None => return Some(format!("container {name} has no status")),
            Some(s) if s.started != Some(true) => {
                return Some(format!("container {name} has not started"))
            }
            Some(s) if !s.ready => return Some(format!("container {name} is not ready")),
            Some(_) => {}
        }
    }
    None
}

/// Whether two probes are the same probe of the same container, whose last
/// result therefore carries over.
fn same_probe(a: &Probe, b: &Probe) -> bool {
    a.container == b.container && a.role == b.role && a.action == b.action
}

/// The probes without their times, which change at every run and every
/// event: a change of times alone is not written to the record.
fn timeless(probes: &PodProbes) -> PodProbes {
    let mut probes = probes.clone();
    probes.at_ms = 0;
    for last in probes.probes.iter_mut().filter_map(|p| p.last.as_mut()) {
        last.at_ms = 0;
    }
    probes
}

fn truncate(text: &mut String, max: usize) {
    if text.len() > max {
        let mut end = max;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
}

/// Whether an exec probe's last real result passed, recently enough: within
/// twice its period, or twice its timeout if that is longer.
fn exec_result(probe: &Probe, now_ms: u64) -> Result<(), String> {
    let window_ms = 2_000 * u64::from(probe.period_secs.max(probe.timeout_secs));
    match &probe.last {
        None => Err("it has not run since the pod started".into()),
        Some(last) if !last.passed => match last.code {
            Some(code) => Err(format!("it last failed with exit code {code}")),
            None => Err("it last failed".into()),
        },
        Some(last) if now_ms.saturating_sub(last.at_ms) > window_ms => Err(format!(
            "its last result is {} s old",
            now_ms.saturating_sub(last.at_ms) / 1000
        )),
        Some(_) => Ok(()),
    }
}

/// A pod as the API server last told us.
struct KnownPod {
    uid: String,
    namespace: String,
    name: String,
    /// A static pod's mirror, which has a uid of its own.
    mirror: bool,
    ip: Option<Ipv4Addr>,
    /// Its phase is Succeeded or Failed. A finished pod keeps its IP in its
    /// status after the network gives the IP to another pod.
    finished: bool,
    /// Its creation timestamp, whose text sorts by time.
    created: String,
    labels: Labels,
    controller: Option<Controller>,
    /// Its probes, or why the node cannot stand in for them.
    probes: Result<Vec<Probe>, String>,
    /// Why it is not ready, if it is not.
    not_ready: Option<String>,
    at_ms: u64,
}

impl KnownPod {
    fn new(pod: &Pod, at_ms: u64) -> KnownPod {
        KnownPod {
            uid: pod.metadata.uid.clone(),
            namespace: pod.metadata.namespace.clone(),
            name: pod.metadata.name.clone(),
            mirror: pod.metadata.annotations.contains_key(MIRROR_ANNOTATION),
            ip: pod.status.pod_ip.parse().ok(),
            finished: matches!(pod.status.phase.as_str(), "Succeeded" | "Failed"),
            created: pod.metadata.creation_timestamp.clone(),
            labels: Labels::from(&pod.metadata.labels),
            controller: Controller::of(&pod.metadata),
            probes: probes_of(pod),
            not_ready: not_ready(pod),
            at_ms,
        }
    }

    /// Whether this is the pod of a sandbox: by uid, or by namespace and name
    /// if the sandbox has no uid or this is a mirror pod.
    fn owns(&self, identity: &PodIdentity) -> bool {
        if !identity.uid.is_empty() && identity.uid == self.uid {
            return true;
        }
        (identity.uid.is_empty() || self.mirror)
            && identity.namespace == self.namespace
            && identity.name == self.name
    }

    /// A sandbox's probes from this view, with the last results of the
    /// probes that did not change; none if the node cannot stand in for
    /// them.
    fn probes_for(&self, old: Option<&PodProbes>) -> Option<PodProbes> {
        let mut probes = self.probes.clone().ok()?;
        if let Some(old) = old {
            for probe in &mut probes {
                probe.last = old
                    .probes
                    .iter()
                    .find(|o| same_probe(o, probe))
                    .and_then(|o| o.last.clone());
            }
        }
        Some(PodProbes {
            probes,
            ready: self.not_ready.is_none(),
            at_ms: self.at_ms,
        })
    }
}

/// Logs once why a pod never sleeps.
fn note(earlier: Option<&KnownPod>, pod: &KnownPod) {
    if let Err(reason) = &pod.probes {
        if earlier.map(|e| &e.probes) != Some(&pod.probes) {
            info!(
                "pod {}/{} does not sleep: {reason}",
                pod.namespace, pod.name
            );
        }
    }
}

/// The node's [`ProbeBook`]. The pod watch ([`crate::kube::PodWatch`]) feeds
/// it as a [`PodSink`]. It keeps each sandbox's probes in the sandbox's record
/// with [`Lifecycle::set_probes`], and in memory for the responder, and the
/// pod's labels and controller in the record with [`Lifecycle::set_labels`].
pub struct NodeProbeBook {
    lifecycle: Lifecycle,
    clock: Clock,
    /// The runtime of the pre-sleep checks.
    runtime: Handle,
    state: Mutex<State>,
    /// Orders the record writes, so that each record ends with the last.
    writes: Mutex<()>,
}

#[derive(Default)]
struct State {
    /// The pods bound to the node by uid, as the API server told us.
    pods: HashMap<String, Arc<KnownPod>>,
    /// Each sandbox's probes, as its record has them or is about to.
    sandboxes: HashMap<String, Arc<PodProbes>>,
}

impl NodeProbeBook {
    /// A book over the lifecycle's records. Call it on the runtime that the
    /// pre-sleep checks should run on.
    pub fn new(lifecycle: Lifecycle, clock: Clock) -> NodeProbeBook {
        NodeProbeBook {
            lifecycle,
            clock,
            runtime: Handle::current(),
            state: Mutex::new(State::default()),
            writes: Mutex::new(()),
        }
    }

    /// `NAMESPACE/NAME` of the pod bound to this node at `ip`, as the API
    /// server told us: the source of a wake's traffic. Of the pods with that
    /// IP, one that has not finished, then the newest.
    pub fn pod_at(&self, ip: Ipv4Addr) -> Option<String> {
        lock(&self.state)
            .pods
            .values()
            .filter(|p| p.ip == Some(ip))
            .max_by_key(|p| (!p.finished, &p.created, &p.uid))
            .map(|p| format!("{}/{}", p.namespace, p.name))
    }

    /// The pod of a sandbox, as the API server told us.
    fn known(&self, identity: &PodIdentity) -> Option<Arc<KnownPod>> {
        let state = lock(&self.state);
        if let Some(pod) = state.pods.get(&identity.uid) {
            return Some(pod.clone());
        }
        state.pods.values().find(|p| p.owns(identity)).cloned()
    }

    /// Changes a sandbox's probes in memory and, unless only times changed,
    /// in its record.
    fn update(&self, sandbox_id: &str, edit: impl FnOnce(Option<&PodProbes>) -> Option<PodProbes>) {
        let _writing = lock(&self.writes);
        let cached = lock(&self.state).sandboxes.get(sandbox_id).cloned();
        let old = match cached {
            Some(probes) => Some(probes),
            None => match self.lifecycle.record(sandbox_id) {
                Some(record) => record.probes.map(Arc::new),
                None => return,
            },
        };
        let next = edit(old.as_deref());
        let durable = match (old.as_deref(), &next) {
            (Some(old), Some(next)) => timeless(old) != timeless(next),
            (None, None) => false,
            _ => true,
        };
        match &next {
            Some(next) => {
                let next = Arc::new(next.clone());
                lock(&self.state)
                    .sandboxes
                    .insert(sandbox_id.to_string(), next);
            }
            None => {
                lock(&self.state).sandboxes.remove(sandbox_id);
            }
        }
        if !durable {
            return;
        }
        match self.lifecycle.set_probes(sandbox_id, next) {
            Ok(()) => {}
            Err(LifecycleError::NotFound(_)) => {
                lock(&self.state).sandboxes.remove(sandbox_id);
            }
            Err(e) => warn!("record the probes of pod sandbox {sandbox_id}: {e}"),
        }
    }

    /// Gives what the API server says of a pod to its sandboxes.
    fn learn(&self, pod: &KnownPod) {
        let sandboxes: Vec<PodIdentity> = self
            .lifecycle
            .list()
            .into_iter()
            .filter(|info| pod.owns(&info.identity))
            .map(|info| info.identity)
            .collect();
        for identity in sandboxes {
            self.label(&identity, pod);
            self.update(&identity.sandbox_id, |old| pod.probes_for(old));
        }
    }

    /// Writes the pod's labels and controller into a sandbox's record when
    /// they changed.
    fn label(&self, identity: &PodIdentity, pod: &KnownPod) {
        if (&identity.labels, &identity.controller) == (&pod.labels, &pod.controller) {
            return;
        }
        let id = &identity.sandbox_id;
        match self
            .lifecycle
            .set_labels(id, pod.labels.clone(), pod.controller.clone())
        {
            Ok(()) | Err(LifecycleError::NotFound(_)) => {}
            Err(e) => warn!("record the labels of pod sandbox {id}: {e}"),
        }
    }

    /// Writes a sandbox's probes from memory into its record unless the
    /// record has them, apart from times.
    fn persist(&self, sandbox_id: &str) {
        let _writing = lock(&self.writes);
        let Some(cached) = lock(&self.state).sandboxes.get(sandbox_id).cloned() else {
            return;
        };
        let recorded = self.lifecycle.record(sandbox_id).and_then(|r| r.probes);
        if recorded.as_ref().map(timeless) != Some(timeless(&cached)) {
            if let Err(e) = self
                .lifecycle
                .set_probes(sandbox_id, Some((*cached).clone()))
            {
                warn!("record the probes of pod sandbox {sandbox_id}: {e}");
            }
        }
    }
}

impl PodSink for NodeProbeBook {
    fn replace(&self, pods: Vec<Pod>) {
        let now = self.clock.now_ms();
        let known: HashMap<String, Arc<KnownPod>> = pods
            .iter()
            .filter(|p| !p.metadata.uid.is_empty())
            .map(|p| (p.metadata.uid.clone(), Arc::new(KnownPod::new(p, now))))
            .collect();
        let sandboxes = self.lifecycle.list();
        {
            let mut state = lock(&self.state);
            for pod in known.values() {
                note(state.pods.get(&pod.uid).map(|p| &**p), pod);
            }
            state.pods = known.clone();
            state
                .sandboxes
                .retain(|id, _| sandboxes.iter().any(|s| s.identity.sandbox_id == *id));
        }
        for sandbox in sandboxes {
            let identity = &sandbox.identity;
            let pod = known
                .get(&identity.uid)
                .or_else(|| known.values().find(|p| p.owns(identity)));
            if let Some(pod) = pod {
                self.label(identity, pod);
                self.update(&identity.sandbox_id, |old| pod.probes_for(old));
            }
        }
    }

    fn apply(&self, pod: Pod) {
        if pod.metadata.uid.is_empty() {
            return;
        }
        let pod = Arc::new(KnownPod::new(&pod, self.clock.now_ms()));
        let earlier = lock(&self.state).pods.insert(pod.uid.clone(), pod.clone());
        note(earlier.as_deref(), &pod);
        self.learn(&pod);
    }

    fn remove(&self, pod: Pod) {
        lock(&self.state).pods.remove(&pod.metadata.uid);
    }
}

impl ProbeBook for NodeProbeBook {
    fn probes(&self, sandbox_id: &str) -> Option<Arc<PodProbes>> {
        if let Some(probes) = lock(&self.state).sandboxes.get(sandbox_id) {
            return Some(probes.clone());
        }
        let record = self.lifecycle.record(sandbox_id)?;
        let probes = match self.known(&record.identity) {
            Some(pod) => pod.probes_for(record.probes.as_ref()),
            None => record.probes,
        }?;
        let mut state = lock(&self.state);
        let probes = state
            .sandboxes
            .entry(sandbox_id.to_string())
            .or_insert_with(|| Arc::new(probes));
        Some(probes.clone())
    }

    fn check_before_sleep(&self, sandbox_id: &str) -> Result<(), String> {
        let record = self
            .lifecycle
            .record(sandbox_id)
            .ok_or_else(|| format!("no pod sandbox {sandbox_id}"))?;
        let pod = self.known(&record.identity);
        let Some(probes) = self.probes(sandbox_id) else {
            return Err(match pod.as_ref().map(|p| &p.probes) {
                Some(Err(reason)) => reason.clone(),
                _ => "the pod's probes are not recorded yet".into(),
            });
        };
        if !probes.ready {
            return Err(pod
                .as_ref()
                .and_then(|p| p.not_ready.clone())
                .unwrap_or_else(|| "the pod is not ready".into()));
        }

        let mut failures = Vec::new();
        let now = self.clock.now_ms();
        let checked = probes
            .probes
            .iter()
            .filter(|p| p.role != ProbeRole::Startup);
        let (network, execs): (Vec<&Probe>, Vec<&Probe>) =
            checked.partition(|p| p.action.port().is_some());
        for probe in execs {
            if let Err(e) = exec_result(probe, now) {
                failures.push(format!(
                    "the {} probe of container {}: {e}",
                    role_name(probe.role),
                    probe.container
                ));
            }
        }
        if !network.is_empty() {
            let ip = record
                .identity
                .ip
                .as_deref()
                .and_then(|ip| ip.parse().ok())
                .or_else(|| pod.as_ref().and_then(|p| p.ip))
                .ok_or("the pod has no IPv4 address")?;
            let runs = network.iter().map(|probe| {
                let timeout = Duration::from_secs(u64::from(probe.timeout_secs));
                check::run(&probe.action, ip, timeout)
            });
            let outcomes = self.runtime.block_on(join_all(runs));
            let at_ms = self.clock.now_ms();
            self.update(sandbox_id, |old| {
                let mut next = old?.clone();
                for (probe, outcome) in network.iter().zip(&outcomes) {
                    for same in next.probes.iter_mut().filter(|p| same_probe(p, probe)) {
                        same.last = Some(ProbeResult {
                            passed: outcome.passed,
                            code: outcome.code,
                            output: String::new(),
                            at_ms,
                        });
                    }
                }
                Some(next)
            });
            for (probe, outcome) in network.iter().zip(outcomes) {
                if !outcome.passed {
                    failures.push(format!(
                        "the {} probe of container {}: {}",
                        role_name(probe.role),
                        probe.container,
                        outcome.detail
                    ));
                }
            }
        }
        if !failures.is_empty() {
            return Err(failures.join("; "));
        }
        // A pod sleeps only with its probes in its record.
        self.persist(sandbox_id);
        Ok(())
    }

    fn record_exec(
        &self,
        sandbox_id: &str,
        container: &str,
        command: &[String],
        mut result: ProbeResult,
    ) {
        truncate(&mut result.output, EXEC_OUTPUT_MAX);
        self.update(sandbox_id, |old| {
            let mut next = old?.clone();
            next.record_exec(container, command, result);
            Some(next)
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    use tokio::net::TcpListener;

    use crate::lifecycle::{clock_at, Config, NoHooks, RunningVms};

    const START_MS: u64 = 1_800_000_000_000;
    const UID: &str = "6f1c2a3e-1b2c-4d5e-8f90-123456789abc";

    /// A pod with every kind of probe, as k3s's API server returns it.
    const ALL_PROBES_POD: &str = r#"{
      "apiVersion": "v1",
      "kind": "Pod",
      "metadata": {
        "name": "probes-all",
        "namespace": "default",
        "uid": "6f1c2a3e-1b2c-4d5e-8f90-123456789abc",
        "resourceVersion": "4711",
        "creationTimestamp": "2026-10-19T08:00:00Z",
        "labels": {"app": "probes-all"},
        "annotations": {"smolvm-pods.floci.io/idle-timeout": "10s"}
      },
      "spec": {
        "nodeName": "floci-node",
        "runtimeClassName": "smolvm-pods",
        "restartPolicy": "Always",
        "initContainers": [
          {"name": "setup", "image": "busybox", "command": ["true"], "resources": {}},
          {
            "name": "metrics",
            "image": "testapp",
            "restartPolicy": "Always",
            "ports": [{"name": "metrics", "containerPort": 9100, "protocol": "TCP"}],
            "livenessProbe": {
              "tcpSocket": {"port": "metrics"},
              "periodSeconds": 10, "timeoutSeconds": 1,
              "successThreshold": 1, "failureThreshold": 3
            }
          }
        ],
        "containers": [
          {
            "name": "app",
            "image": "testapp",
            "ports": [
              {"name": "http", "containerPort": 8080, "protocol": "TCP"},
              {"name": "https", "containerPort": 8443, "protocol": "TCP"},
              {"name": "grpc", "containerPort": 9090, "protocol": "TCP"}
            ],
            "env": [{"name": "MODE", "value": "probe"}],
            "startupProbe": {
              "httpGet": {"path": "/startup", "port": "http", "scheme": "HTTP"},
              "periodSeconds": 1, "timeoutSeconds": 1,
              "successThreshold": 1, "failureThreshold": 30
            },
            "livenessProbe": {
              "grpc": {"port": 9090, "service": "liveness"},
              "initialDelaySeconds": 5, "periodSeconds": 2, "timeoutSeconds": 3,
              "successThreshold": 1, "failureThreshold": 3
            },
            "readinessProbe": {
              "httpGet": {
                "path": "/ready?full=1", "port": "https", "scheme": "HTTPS",
                "httpHeaders": [{"name": "X-Probe", "value": "readiness"}]
              },
              "periodSeconds": 2, "timeoutSeconds": 1,
              "successThreshold": 1, "failureThreshold": 3
            },
            "resources": {},
            "terminationMessagePath": "/dev/termination-log",
            "terminationMessagePolicy": "File",
            "imagePullPolicy": "IfNotPresent"
          },
          {
            "name": "worker",
            "image": "testapp",
            "env": [
              {"name": "HEALTH_FILE", "value": "/tmp/healthy"},
              {"name": "TOKEN", "valueFrom": {"secretKeyRef": {"name": "worker", "key": "token"}}},
              {"name": "LEVEL", "value": "1"},
              {"name": "LEVEL", "value": "2"}
            ],
            "livenessProbe": {
              "exec": {"command": ["sh", "-c",
                "test -f $(HEALTH_FILE) && echo $(LEVEL) [$(TOKEN)] $(UNSET) $$(HEALTH_FILE)"]},
              "periodSeconds": 10, "timeoutSeconds": 1,
              "successThreshold": 1, "failureThreshold": 3
            },
            "readinessProbe": {
              "tcpSocket": {"port": 5432},
              "periodSeconds": 10, "timeoutSeconds": 1,
              "successThreshold": 1, "failureThreshold": 3
            }
          }
        ]
      },
      "status": {
        "phase": "Running",
        "conditions": [
          {"type": "PodReadyToStartContainers", "status": "True"},
          {"type": "Initialized", "status": "True"},
          {"type": "Ready", "status": "True"},
          {"type": "ContainersReady", "status": "True"},
          {"type": "PodScheduled", "status": "True"}
        ],
        "hostIP": "192.168.64.2",
        "podIP": "10.42.0.9",
        "podIPs": [{"ip": "10.42.0.9"}],
        "qosClass": "BestEffort",
        "initContainerStatuses": [
          {"name": "setup", "ready": true, "restartCount": 0, "started": false,
           "state": {"terminated": {"exitCode": 0, "reason": "Completed"}}},
          {"name": "metrics", "ready": true, "restartCount": 0, "started": true,
           "state": {"running": {"startedAt": "2026-10-19T08:00:03Z"}}}
        ],
        "containerStatuses": [
          {"name": "app", "ready": true, "restartCount": 0, "started": true,
           "state": {"running": {"startedAt": "2026-10-19T08:00:04Z"}}},
          {"name": "worker", "ready": true, "restartCount": 0, "started": true,
           "state": {"running": {"startedAt": "2026-10-19T08:00:04Z"}}}
        ]
      }
    }"#;

    const WORKER_COMMAND: &str = "test -f /tmp/healthy && echo 2 [] $(UNSET) $(HEALTH_FILE)";

    fn all_probes_pod() -> serde_json::Value {
        serde_json::from_str(ALL_PROBES_POD).unwrap()
    }

    fn pod(json: &serde_json::Value) -> Pod {
        serde_json::from_value(json.clone()).unwrap()
    }

    fn set(json: &mut serde_json::Value, pointer: &str, value: serde_json::Value) {
        *json
            .pointer_mut(pointer)
            .unwrap_or_else(|| panic!("{pointer}")) = value;
    }

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|a| a.to_string()).collect()
    }

    fn worker_command() -> Vec<String> {
        argv(&["sh", "-c", WORKER_COMMAND])
    }

    fn probe(
        container: &str,
        role: ProbeRole,
        action: ProbeAction,
        period: u32,
        timeout: u32,
    ) -> Probe {
        Probe {
            container: container.into(),
            role,
            action,
            period_secs: period,
            timeout_secs: timeout,
            last: None,
        }
    }

    #[test]
    fn every_kind_of_probe_is_read_as_kubelet_runs_it() {
        let probes = probes_of(&pod(&all_probes_pod())).unwrap();
        assert_eq!(
            probes,
            [
                probe(
                    "app",
                    ProbeRole::Startup,
                    ProbeAction::Http {
                        port: 8080,
                        path: "/startup".into(),
                        https: false,
                        headers: Vec::new(),
                    },
                    1,
                    1
                ),
                probe(
                    "app",
                    ProbeRole::Liveness,
                    ProbeAction::Grpc {
                        port: 9090,
                        service: Some("liveness".into()),
                    },
                    2,
                    3
                ),
                probe(
                    "app",
                    ProbeRole::Readiness,
                    ProbeAction::Http {
                        port: 8443,
                        path: "/ready?full=1".into(),
                        https: true,
                        headers: vec![("X-Probe".into(), "readiness".into())],
                    },
                    2,
                    1
                ),
                probe(
                    "worker",
                    ProbeRole::Liveness,
                    ProbeAction::Exec {
                        command: worker_command(),
                    },
                    10,
                    1
                ),
                probe(
                    "worker",
                    ProbeRole::Readiness,
                    ProbeAction::Tcp { port: 5432 },
                    10,
                    1
                ),
                probe(
                    "metrics",
                    ProbeRole::Liveness,
                    ProbeAction::Tcp { port: 9100 },
                    10,
                    1
                ),
            ]
        );
        assert_eq!(not_ready(&pod(&all_probes_pod())), None);
    }

    #[test]
    fn missing_fields_take_kubelets_defaults() {
        let json = serde_json::json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "u"},
            "spec": {"containers": [
                {"name": "plain"},
                {"name": "web", "readinessProbe": {"httpGet": {"port": "8081"}}},
                {"name": "rpc", "livenessProbe": {"grpc": {"port": 50051, "service": ""}}}
            ]},
            "status": {"phase": "Running", "podIP": "10.42.0.3"}
        });
        let probes = probes_of(&pod(&json)).unwrap();
        assert_eq!(
            probes,
            [
                probe(
                    "web",
                    ProbeRole::Readiness,
                    ProbeAction::Http {
                        port: 8081,
                        path: "/".into(),
                        https: false,
                        headers: Vec::new(),
                    },
                    PERIOD_SECS,
                    TIMEOUT_SECS
                ),
                probe(
                    "rpc",
                    ProbeRole::Liveness,
                    ProbeAction::Grpc {
                        port: 50051,
                        service: None,
                    },
                    PERIOD_SECS,
                    TIMEOUT_SECS
                ),
            ]
        );
        let none = serde_json::json!({"spec": {"containers": [{"name": "plain"}]}});
        assert_eq!(probes_of(&pod(&none)).unwrap(), []);
    }

    #[test]
    fn probes_the_node_cannot_stand_in_for_are_refused() {
        let mut json = all_probes_pod();
        set(
            &mut json,
            "/spec/containers/0/startupProbe/httpGet/port",
            "admin".into(),
        );
        assert_eq!(
            probes_of(&pod(&json)).unwrap_err(),
            "the startup probe of container app: the container has no port named \"admin\""
        );

        let mut json = all_probes_pod();
        set(
            &mut json,
            "/spec/containers/1/readinessProbe/tcpSocket",
            serde_json::json!({"port": 5432, "host": "10.0.0.1"}),
        );
        assert_eq!(
            probes_of(&pod(&json)).unwrap_err(),
            "the readiness probe of container worker: it probes the host 10.0.0.1, not the pod"
        );
        set(
            &mut json,
            "/spec/containers/1/readinessProbe/tcpSocket/host",
            "10.42.0.9".into(),
        );
        assert!(probes_of(&pod(&json)).is_ok());

        let mut json = all_probes_pod();
        set(
            &mut json,
            "/spec/containers/1/readinessProbe/tcpSocket/port",
            70000.into(),
        );
        assert!(probes_of(&pod(&json))
            .unwrap_err()
            .ends_with("70000 is not a port number"));
    }

    #[test]
    fn readiness_needs_ready_running_and_started_containers() {
        let cases = [
            (
                "/status/phase",
                serde_json::json!("Pending"),
                "the pod's phase is \"Pending\"",
            ),
            (
                "/status/conditions/2/status",
                "False".into(),
                "the pod is not Ready",
            ),
            (
                "/status/containerStatuses/1/started",
                false.into(),
                "container worker has not started",
            ),
            (
                "/status/containerStatuses/0/ready",
                false.into(),
                "container app is not ready",
            ),
            (
                "/status/initContainerStatuses/1/ready",
                false.into(),
                "container metrics is not ready",
            ),
            (
                "/metadata/deletionTimestamp",
                "2026-10-19T09:00:00Z".into(),
                "the pod is terminating",
            ),
        ];
        for (pointer, value, reason) in cases {
            let mut json = all_probes_pod();
            if pointer == "/metadata/deletionTimestamp" {
                json["metadata"]["deletionTimestamp"] = value;
            } else {
                set(&mut json, pointer, value);
            }
            assert_eq!(not_ready(&pod(&json)).as_deref(), Some(reason), "{pointer}");
        }
        let mut json = all_probes_pod();
        json["status"]["containerStatuses"]
            .as_array_mut()
            .unwrap()
            .pop();
        assert_eq!(
            not_ready(&pod(&json)).as_deref(),
            Some("container worker has no status")
        );
    }

    #[test]
    fn variables_expand_as_kubelets() {
        let env: HashMap<&str, &str> = [("A", "x"), ("EMPTY", ""), ("B", "$(A)")]
            .into_iter()
            .collect();
        for (input, output) in [
            ("$(A)", "x"),
            ("<$(A)$(A)>", "<xx>"),
            ("$(EMPTY)", ""),
            ("$(UNSET)", "$(UNSET)"),
            ("$$(A)", "$(A)"),
            ("$$$(A)", "$x"),
            ("$(B)", "$(A)"),
            ("$(A", "$(A"),
            ("a$", "a$"),
            ("$a $ b", "$a $ b"),
            ("$()", "$()"),
            ("é$(A)é", "éxé"),
            ("$é", "$é"),
            ("", ""),
        ] {
            assert_eq!(expand(input, &env), output, "{input}");
        }
    }

    #[test]
    fn http_paths_become_kubelets_request_targets() {
        for (path, target) in [
            ("", "/"),
            ("/healthz", "/healthz"),
            ("/ready?full=1&x=a b", "/ready?full=1&x=a b"),
            ("/p#fragment", "/p"),
            ("/a b", "/a%20b"),
            ("/caf\u{e9}", "/caf%C3%A9"),
            ("/a%2Fb", "/a%2Fb"),
            ("/a(b)!", "/a(b)!"),
            ("/a%20b c", "/a%20b%20c"),
            ("/bad%zz?x", "/bad%25zz%3Fx"),
            ("?q", "/?q"),
            ("/q?", "/q?"),
        ] {
            assert_eq!(request_target(path), target, "{path:?}");
        }
    }

    #[test]
    fn exec_results_must_pass_and_be_recent() {
        let mut probe = probe(
            "worker",
            ProbeRole::Liveness,
            ProbeAction::Exec {
                command: argv(&["true"]),
            },
            10,
            1,
        );
        assert!(exec_result(&probe, START_MS).is_err());
        let result = |passed, at_ms| ProbeResult {
            passed,
            code: Some(if passed { 0 } else { 1 }),
            output: String::new(),
            at_ms,
        };
        probe.last = Some(result(true, START_MS - 20_000));
        assert_eq!(exec_result(&probe, START_MS), Ok(()));
        assert_eq!(
            exec_result(&probe, START_MS + 1),
            Err("its last result is 20 s old".into())
        );
        probe.last = Some(result(false, START_MS));
        assert_eq!(
            exec_result(&probe, START_MS),
            Err("it last failed with exit code 1".into())
        );
        probe.timeout_secs = 30;
        probe.last = Some(result(true, START_MS - 50_000));
        assert_eq!(exec_result(&probe, START_MS), Ok(()));
    }

    #[test]
    fn output_is_cut_at_a_character() {
        let mut text = "aé".repeat(4);
        truncate(&mut text, 5);
        assert_eq!(text, "aéa");
        let mut short = "ok".to_string();
        truncate(&mut short, 5);
        assert_eq!(short, "ok");
    }

    struct Node {
        dir: tempfile::TempDir,
        now_ms: Arc<AtomicU64>,
        life: Lifecycle,
        book: Arc<NodeProbeBook>,
    }

    impl Node {
        async fn new() -> Node {
            let dir = tempfile::tempdir().unwrap();
            let now_ms = Arc::new(AtomicU64::new(START_MS));
            let life = open(dir.path(), &now_ms).await;
            let book = Arc::new(NodeProbeBook::new(life.clone(), clock_at(&now_ms)));
            Node {
                dir,
                now_ms,
                life,
                book,
            }
        }

        /// The node service after a restart, before any word from the API
        /// server.
        async fn restart(self) -> Node {
            self.life.shutdown();
            let life = open(self.dir.path(), &self.now_ms).await;
            let book = Arc::new(NodeProbeBook::new(life.clone(), clock_at(&self.now_ms)));
            Node { life, book, ..self }
        }

        async fn sandbox(&self, id: &str, uid: &str, ip: &str) {
            let identity = PodIdentity {
                sandbox_id: id.into(),
                namespace: "default".into(),
                name: "probes-all".into(),
                uid: uid.into(),
                machine: id.into(),
                ip: Some(ip.into()),
                ..Default::default()
            };
            self.life
                .create(identity, async { Ok(None) })
                .await
                .unwrap();
        }

        fn recorded(&self, id: &str) -> Option<PodProbes> {
            self.life.record(id).unwrap().probes
        }

        fn advance(&self, by: Duration) {
            self.now_ms
                .fetch_add(by.as_millis() as u64, Ordering::SeqCst);
        }

        async fn check(&self, id: &str) -> Result<(), String> {
            let (book, id) = (self.book.clone(), id.to_string());
            tokio::task::spawn_blocking(move || book.check_before_sleep(&id))
                .await
                .unwrap()
        }
    }

    async fn open(dir: &Path, now_ms: &Arc<AtomicU64>) -> Lifecycle {
        let mut config = Config::defaults(dir);
        config.clock = clock_at(now_ms);
        let life = Lifecycle::open(config, Arc::new(RunningVms), Arc::new(NoHooks))
            .await
            .unwrap();
        life.settled().await;
        life
    }

    fn exec_result_of(code: i32, output: &str, at_ms: u64) -> ProbeResult {
        ProbeResult {
            passed: code == 0,
            code: Some(code),
            output: output.into(),
            at_ms,
        }
    }

    #[tokio::test]
    async fn probes_are_recorded_and_survive_a_restart() {
        let node = Node::new().await;
        node.sandbox("sb-1", UID, "10.42.0.9").await;
        node.sandbox("sb-other", "another-uid", "10.42.0.10").await;
        assert!(node.book.probes("sb-1").is_none());
        node.book.replace(vec![pod(&all_probes_pod())]);
        let recorded = node.recorded("sb-1").unwrap();
        assert_eq!(recorded.probes, probes_of(&pod(&all_probes_pod())).unwrap());
        assert!(recorded.ready);
        assert_eq!(recorded.at_ms, START_MS);
        assert_eq!(node.recorded("sb-other"), None);

        let long = "x".repeat(EXEC_OUTPUT_MAX + 10);
        node.book.record_exec(
            "sb-1",
            "worker",
            &worker_command(),
            exec_result_of(0, &long, START_MS),
        );
        let cached = node.book.probes("sb-1").unwrap();
        let result = cached.exec("worker", &worker_command()).unwrap();
        assert_eq!(result.output.len(), EXEC_OUTPUT_MAX);
        assert_eq!(node.recorded("sb-1").as_ref(), Some(&*cached));

        // The same result later changes only times, which stay in memory.
        node.book.record_exec(
            "sb-1",
            "worker",
            &worker_command(),
            exec_result_of(0, &long, START_MS + 10_000),
        );
        let exec_at = |probes: &PodProbes| probes.exec("worker", &worker_command()).unwrap().at_ms;
        assert_eq!(
            exec_at(&node.book.probes("sb-1").unwrap()),
            START_MS + 10_000
        );
        assert_eq!(exec_at(&node.recorded("sb-1").unwrap()), START_MS);
        node.book.record_exec(
            "sb-1",
            "worker",
            &argv(&["sh", "-c", "other"]),
            exec_result_of(0, "", START_MS),
        );

        let node = node.restart().await;
        let probes = node.book.probes("sb-1").unwrap();
        assert_eq!(probes.probes.len(), 6);
        assert_eq!(exec_at(&probes), START_MS);
        assert!(node.book.probes("sb-other").is_none());
        assert_eq!(
            node.book.check_before_sleep("sb-other"),
            Err("the pod's probes are not recorded yet".into())
        );
    }

    #[tokio::test]
    async fn a_sandbox_made_after_the_event_gets_the_pods_probes() {
        let node = Node::new().await;
        node.book.apply(pod(&all_probes_pod()));
        node.sandbox("sb-1", UID, "10.42.0.9").await;
        assert_eq!(node.book.probes("sb-1").unwrap().probes.len(), 6);
        // Kept in memory until the next change is written.
        assert_eq!(node.recorded("sb-1"), None);
        let mut json = all_probes_pod();
        set(&mut json, "/status/containerStatuses/0/ready", false.into());
        node.book.apply(pod(&json));
        assert!(!node.recorded("sb-1").unwrap().ready);
    }

    /// A Deployment's pod, as the API server returns it.
    fn deployment_pod() -> serde_json::Value {
        let mut json = all_probes_pod();
        json["metadata"]["labels"] =
            serde_json::json!({"app": "orders", "pod-template-hash": "7c9d8f6b5"});
        json["metadata"]["ownerReferences"] = serde_json::json!([{
            "apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "orders-7c9d8f6b5",
            "uid": "rs-uid", "controller": true, "blockOwnerDeletion": true,
        }]);
        json
    }

    fn labels_of(identity: &PodIdentity) -> Vec<(&str, &str)> {
        identity.labels.iter().collect()
    }

    #[tokio::test]
    async fn labels_and_the_controller_are_recorded_and_survive_a_restart() {
        let node = Node::new().await;
        let identity = |node: &Node, id: &str| node.life.record(id).unwrap().identity;
        node.sandbox("sb-1", UID, "10.42.0.9").await;
        node.sandbox("sb-bare", "bare-uid", "10.42.0.10").await;
        let mut bare = all_probes_pod();
        set(&mut bare, "/metadata/uid", "bare-uid".into());
        node.book.replace(vec![pod(&deployment_pod()), pod(&bare)]);
        let orders = Some(Controller {
            kind: "Deployment".into(),
            name: "orders".into(),
        });
        let recorded = identity(&node, "sb-1");
        assert_eq!(
            labels_of(&recorded),
            [("app", "orders"), ("pod-template-hash", "7c9d8f6b5")]
        );
        assert_eq!(recorded.controller, orders);
        let recorded = identity(&node, "sb-bare");
        assert_eq!(labels_of(&recorded), [("app", "probes-all")]);
        assert_eq!(recorded.controller, None);

        let mut json = deployment_pod();
        json["metadata"]["labels"]["tier"] = "web".into();
        node.book.apply(pod(&json));
        assert_eq!(identity(&node, "sb-1").labels.get("tier"), Some("web"));

        let node = node.restart().await;
        let recorded = identity(&node, "sb-1");
        assert_eq!(
            labels_of(&recorded),
            [
                ("app", "orders"),
                ("pod-template-hash", "7c9d8f6b5"),
                ("tier", "web")
            ]
        );
        assert_eq!(recorded.controller, orders);
        assert_eq!(identity(&node, "sb-bare").controller, None);
    }

    /// A sandbox made after the pod's event gets the labels at the next
    /// event, such as the pod's IP or readiness, which kubelet reports once
    /// the sandbox runs.
    #[tokio::test]
    async fn a_sandbox_made_after_the_event_gets_the_labels_at_the_next() {
        let node = Node::new().await;
        let mut pending = deployment_pod();
        set(&mut pending, "/status/phase", "Pending".into());
        set(&mut pending, "/status/podIP", "".into());
        node.book.apply(pod(&pending));
        node.sandbox("sb-1", UID, "10.42.0.9").await;
        let recorded = node.life.record("sb-1").unwrap().identity;
        assert!(recorded.labels.is_empty() && recorded.controller.is_none());
        node.book.apply(pod(&deployment_pod()));
        let recorded = node.life.record("sb-1").unwrap().identity;
        assert_eq!(recorded.labels.get("app"), Some("orders"));
        assert_eq!(
            recorded.controller.map(|c| (c.kind, c.name)),
            Some(("Deployment".into(), "orders".into()))
        );
    }

    #[tokio::test]
    async fn a_pod_passes_the_check_only_with_its_probes_recorded() {
        let node = Node::new().await;
        let mut json = all_probes_pod();
        for container in json["spec"]["containers"].as_array_mut().unwrap() {
            let container = container.as_object_mut().unwrap();
            container.retain(|key, _| !key.ends_with("Probe"));
        }
        json["spec"]["initContainers"]
            .as_array_mut()
            .unwrap()
            .truncate(1);
        node.book.apply(pod(&json));
        node.sandbox("sb-1", UID, "10.42.0.9").await;
        assert!(node.book.probes("sb-1").unwrap().probes.is_empty());
        assert_eq!(node.recorded("sb-1"), None);
        assert_eq!(node.check("sb-1").await, Ok(()));
        let recorded = node.recorded("sb-1").unwrap();
        assert!(recorded.probes.is_empty() && recorded.ready);
    }

    #[tokio::test]
    async fn sandboxes_are_matched_by_uid_or_by_name_without_one() {
        let node = Node::new().await;
        node.sandbox("sb-old", "old-uid", "10.42.0.9").await;
        node.sandbox("sb-no-uid", "", "10.42.0.9").await;
        node.book.apply(pod(&all_probes_pod()));
        assert_eq!(node.recorded("sb-old"), None, "a new pod of the same name");
        assert_eq!(node.recorded("sb-no-uid").unwrap().probes.len(), 6);

        let mut mirror = all_probes_pod();
        json_insert(&mut mirror, MIRROR_ANNOTATION, "hash");
        set(&mut mirror, "/metadata/uid", "mirror-uid".into());
        node.book.apply(pod(&mirror));
        assert_eq!(node.recorded("sb-old").unwrap().probes.len(), 6);

        node.book.remove(pod(&mirror));
        node.book.remove(pod(&all_probes_pod()));
        node.sandbox("sb-new", UID, "10.42.0.9").await;
        assert!(node.book.probes("sb-new").is_none());
    }

    /// A finished pod keeps the IP that the network has since given to
    /// another pod. Each replace builds the pod map anew, in another order,
    /// so the test replaces it a few times.
    #[tokio::test]
    async fn the_pod_at_an_ip_has_not_finished_then_is_the_newest() {
        let node = Node::new().await;
        let at = |uid: &str, phase: &str, created: &str| {
            let mut json = all_probes_pod();
            set(&mut json, "/metadata/uid", uid.into());
            set(&mut json, "/metadata/name", uid.into());
            set(&mut json, "/metadata/creationTimestamp", created.into());
            set(&mut json, "/status/phase", phase.into());
            pod(&json)
        };
        let pods = vec![
            at("older", "Running", "2026-10-19T08:00:00Z"),
            at("web", "Running", "2026-10-19T08:30:00Z"),
            at("job", "Succeeded", "2026-10-19T09:00:00Z"),
            at("crashed", "Failed", "2026-10-19T09:30:00Z"),
        ];
        let ip = Ipv4Addr::new(10, 42, 0, 9);
        for _ in 0..8 {
            node.book.replace(pods.clone());
            assert_eq!(node.book.pod_at(ip).as_deref(), Some("default/web"));
        }
        node.book.replace(pods[2..].to_vec());
        assert_eq!(node.book.pod_at(ip).as_deref(), Some("default/crashed"));
        assert_eq!(node.book.pod_at(Ipv4Addr::new(10, 42, 0, 10)), None);
    }

    fn json_insert(json: &mut serde_json::Value, annotation: &str, value: &str) {
        json["metadata"]["annotations"][annotation] = value.into();
    }

    #[tokio::test]
    async fn a_changed_probe_loses_its_result_and_the_others_keep_theirs() {
        let node = Node::new().await;
        node.sandbox("sb-1", UID, "10.42.0.9").await;
        node.book.apply(pod(&all_probes_pod()));
        node.book.record_exec(
            "sb-1",
            "worker",
            &worker_command(),
            exec_result_of(0, "2", START_MS),
        );
        node.advance(Duration::from_secs(1));

        let mut json = all_probes_pod();
        set(
            &mut json,
            "/spec/containers/1/readinessProbe/tcpSocket/port",
            5433.into(),
        );
        node.book.apply(pod(&json));
        let probes = node.recorded("sb-1").unwrap();
        assert!(probes.exec("worker", &worker_command()).is_some());
        assert!(probes.probes.contains(&probe(
            "worker",
            ProbeRole::Readiness,
            ProbeAction::Tcp { port: 5433 },
            10,
            1
        )));
        assert_eq!(probes.at_ms, START_MS + 1000);

        set(
            &mut json,
            "/spec/containers/1/env/0/value",
            "/tmp/ok".into(),
        );
        node.book.apply(pod(&json));
        let probes = node.recorded("sb-1").unwrap();
        assert!(probes.exec("worker", &worker_command()).is_none());
        let changed = argv(&[
            "sh",
            "-c",
            &WORKER_COMMAND.replace("/tmp/healthy", "/tmp/ok"),
        ]);
        assert!(probes.probes.iter().any(|p| p.runs("worker", &changed)));
    }

    #[tokio::test]
    async fn a_pod_whose_probes_the_node_cannot_answer_never_sleeps() {
        let node = Node::new().await;
        node.sandbox("sb-1", UID, "10.42.0.9").await;
        node.book.apply(pod(&all_probes_pod()));
        assert!(node.recorded("sb-1").is_some());
        let mut json = all_probes_pod();
        set(
            &mut json,
            "/spec/containers/1/readinessProbe/tcpSocket/port",
            "db".into(),
        );
        node.book.apply(pod(&json));
        assert_eq!(node.recorded("sb-1"), None);
        assert!(node.book.probes("sb-1").is_none());
        assert_eq!(
            node.check("sb-1").await,
            Err(
                "the readiness probe of container worker: the container has no port named \"db\""
                    .into()
            )
        );
    }

    /// The test pod with probes on local ports: a TCP readiness probe of
    /// `app` and the exec liveness probe of `worker`.
    fn local_pod(port: u16) -> serde_json::Value {
        let mut json = all_probes_pod();
        set(&mut json, "/status/podIP", "127.0.0.1".into());
        let app = &mut json["spec"]["containers"][0];
        app.as_object_mut().unwrap().remove("livenessProbe");
        app["readinessProbe"] = serde_json::json!({"tcpSocket": {"port": port}});
        json["spec"]["containers"][1]
            .as_object_mut()
            .unwrap()
            .remove("readinessProbe");
        json["spec"]["initContainers"]
            .as_array_mut()
            .unwrap()
            .truncate(1);
        json["status"]["initContainerStatuses"]
            .as_array_mut()
            .unwrap()
            .truncate(1);
        json
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_pre_sleep_check_runs_the_probes_and_needs_every_one_to_pass() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let node = Node::new().await;
        node.sandbox("sb-1", UID, "127.0.0.1").await;
        node.book.apply(pod(&local_pod(port)));

        let check = node.check("sb-1").await.unwrap_err();
        assert_eq!(
            check,
            "the liveness probe of container worker: it has not run since the pod started"
        );
        node.book.record_exec(
            "sb-1",
            "worker",
            &worker_command(),
            exec_result_of(0, "", START_MS),
        );
        assert_eq!(node.check("sb-1").await, Ok(()));
        let readiness = |probes: &PodProbes| {
            probes
                .probes
                .iter()
                .find(|p| p.container == "app" && p.role == ProbeRole::Readiness)
                .and_then(|p| p.last.clone())
        };
        let last = readiness(&node.recorded("sb-1").unwrap()).unwrap();
        assert!(last.passed && last.at_ms == START_MS);
        // The startup probe is done once the containers started.
        assert!(node.recorded("sb-1").unwrap().probes[0].last.is_none());

        drop(listener);
        node.advance(Duration::from_secs(5));
        let check = node.check("sb-1").await.unwrap_err();
        assert!(
            check.starts_with(&format!(
                "the readiness probe of container app: TCP 127.0.0.1:{port}: "
            )),
            "{check}"
        );
        let last = readiness(&node.recorded("sb-1").unwrap()).unwrap();
        assert!(!last.passed);
        assert_eq!(last.at_ms, START_MS + 5000);

        node.advance(Duration::from_secs(60));
        let check = node.check("sb-1").await.unwrap_err();
        assert!(
            check.starts_with(
                "the liveness probe of container worker: its last result is 65 s old; "
            ),
            "{check}"
        );

        let mut json = local_pod(port);
        set(&mut json, "/status/containerStatuses/1/ready", false.into());
        node.book.apply(pod(&json));
        assert_eq!(
            node.check("sb-1").await,
            Err("container worker is not ready".into())
        );
    }

    /// Collects the pods of the first list.
    #[derive(Default)]
    struct Listed {
        pods: Mutex<Option<Vec<Pod>>>,
        done: tokio::sync::Notify,
    }

    impl PodSink for Listed {
        fn replace(&self, pods: Vec<Pod>) {
            *lock(&self.pods) = Some(pods);
            self.done.notify_one();
        }

        fn apply(&self, _pod: Pod) {}

        fn remove(&self, _pod: Pod) {}
    }

    /// In a node, as root: the pod watch lists this node's pods, and their
    /// TCP, HTTP and gRPC liveness and readiness probes run from the node.
    #[tokio::test]
    #[ignore = "needs a node: run it there as root"]
    async fn live_the_probes_of_this_nodes_pods_run_from_the_node() {
        let listed = Arc::new(Listed::default());
        let watch = crate::kube::PodWatch::new(crate::kube::KUBELET_KUBECONFIG, listed.clone());
        let task = watch.spawn();
        tokio::time::timeout(Duration::from_secs(30), listed.done.notified())
            .await
            .expect("a list within 30 s");
        task.abort();
        let pods = lock(&listed.pods).take().unwrap();
        let mut ran = 0;
        for pod in &pods {
            let known = KnownPod::new(pod, 0);
            eprintln!(
                "pod {}/{} ({}) at {:?}: {}",
                known.namespace,
                known.name,
                known.uid,
                known.ip,
                known.not_ready.as_deref().unwrap_or("ready")
            );
            let probes = match &known.probes {
                Ok(probes) => probes,
                Err(e) => {
                    eprintln!("  never sleeps: {e}");
                    continue;
                }
            };
            for probe in probes {
                eprintln!("  {probe:?}");
                let Some(ip) = known.ip else { continue };
                if probe.role == ProbeRole::Startup || probe.action.port().is_none() {
                    continue;
                }
                let timeout = Duration::from_secs(u64::from(probe.timeout_secs));
                let outcome = check::run(&probe.action, ip, timeout).await;
                eprintln!("    {outcome:?}");
                assert!(outcome.passed || known.not_ready.is_some(), "{outcome:?}");
                ran += 1;
            }
        }
        assert!(ran > 0, "no probe ran");
    }
}
