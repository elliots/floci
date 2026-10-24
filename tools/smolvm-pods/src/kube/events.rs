//! Kubernetes Events for the pods' sleeps and wakes.
//!
//! [`Reporter`] follows the lifecycle's events and posts a core/v1 Event
//! about the pod for each sleep, wake, wait for admission, refusal and
//! failure, with the kubelet's credentials, which the Node authorizer lets
//! write Events. A [`Limiter`] keeps a busy node from flooding the API
//! server, as client-go's spam filter does for kubelet: a burst per pod and
//! one for the node, each refilled at a steady rate. An Event over either
//! budget is dropped and counted, never queued. Each Event is new: the
//! reporter does not aggregate them into counts.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use log::{debug, warn};
use serde_json::{json, Value};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::client::Client;
use super::pods::host_name;
use crate::lifecycle::{Change, Event, Lifecycle, PodIdentity, PodRef, Reason};
use crate::metrics::{Kind, Text};
use crate::sync::lock;

/// The component that the Events name as their source.
const COMPONENT: &str = "smolvm-pods";

/// The Events one pod may send at once, and how often it gets another.
const POD_BURST: f64 = 10.0;
const POD_REFILL: Duration = Duration::from_secs(30);

/// The Events the node may send at once, and how often it gets another.
const NODE_BURST: f64 = 25.0;
const NODE_REFILL: Duration = Duration::from_secs(1);

/// Above this many pods with a bucket, a check forgets the full buckets.
const PRUNE_ABOVE: usize = 256;

/// The Events waiting to be posted. More are dropped.
const QUEUE: usize = 64;

/// How long the poster waits after a failed post before the next one.
const RETRY_GAP: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens: f64,
    at: Instant,
}

impl Bucket {
    fn full(burst: f64, now: Instant) -> Bucket {
        Bucket {
            tokens: burst,
            at: now,
        }
    }

    fn refill(&mut self, burst: f64, every: Duration, now: Instant) {
        let gained = now.saturating_duration_since(self.at).as_secs_f64() / every.as_secs_f64();
        self.tokens = (self.tokens + gained).min(burst);
        self.at = now;
    }
}

/// Token buckets: one for each pod and one for the node.
pub struct Limiter {
    node: Bucket,
    pods: HashMap<String, Bucket>,
}

impl Limiter {
    pub fn new(now: Instant) -> Limiter {
        Limiter {
            node: Bucket::full(NODE_BURST, now),
            pods: HashMap::new(),
        }
    }

    /// Whether an Event about `pod` may go now, which takes a token from the
    /// pod's bucket and the node's.
    pub fn allow(&mut self, pod: &str, now: Instant) -> bool {
        self.node.refill(NODE_BURST, NODE_REFILL, now);
        if self.pods.len() > PRUNE_ABOVE {
            self.pods.retain(|_, b| {
                b.refill(POD_BURST, POD_REFILL, now);
                b.tokens < POD_BURST
            });
        }
        let bucket = self
            .pods
            .entry(pod.to_string())
            .or_insert_with(|| Bucket::full(POD_BURST, now));
        bucket.refill(POD_BURST, POD_REFILL, now);
        if bucket.tokens < 1.0 || self.node.tokens < 1.0 {
            return false;
        }
        bucket.tokens -= 1.0;
        self.node.tokens -= 1.0;
        true
    }

    /// Forgets a pod's bucket.
    pub fn forget(&mut self, pod: &str) {
        self.pods.remove(pod);
    }
}

/// What an Event says about a pod.
#[derive(Debug, Clone, PartialEq)]
pub struct Note {
    pub warning: bool,
    pub reason: &'static str,
    pub message: String,
}

/// The Event for a lifecycle event, if it has one.
pub fn note_of(event: &Event) -> Option<Note> {
    let why = || why(&event.reason, event.source.as_deref());
    let took = || event.duration_ms.map(took).unwrap_or_default();
    let error = || event.error.clone().unwrap_or_default();
    let (warning, reason, message) = match event.change() {
        Change::Slept => {
            let bytes = event
                .checkpoint_bytes
                .map(|b| format!(", a checkpoint of {}", mib(b)))
                .unwrap_or_default();
            (
                false,
                "Slept",
                format!("slept{} ({}){bytes}", took(), why()),
            )
        }
        Change::Woke => (false, "Woke", format!("woke{} ({})", took(), why())),
        Change::WakeWaiting => {
            let waiting = event.waiting.clone().unwrap_or_default();
            (
                false,
                "WakeWaiting",
                format!("a wake ({}) waits for admission: {waiting}", why()),
            )
        }
        Change::PauseCancelled => (
            false,
            "SleepCancelled",
            format!("a wake ({}) cancelled the sleep", why()),
        ),
        Change::WakeRefused => (true, "WakeRefused", format!("{} ({})", error(), why())),
        Change::WakeFailed => (true, "WakeFailed", format!("the wake failed: {}", error())),
        Change::SleepFailed => (
            true,
            "SleepFailed",
            format!("the sleep failed: {}", error()),
        ),
        Change::Failed => (
            true,
            "VMFailed",
            format!("the pod's VM failed: {}", error()),
        ),
        Change::Created | Change::Other => return None,
    };
    Some(Note {
        warning,
        reason,
        message,
    })
}

/// The reason in words, such as `traffic from ns/web to port 8080`.
fn why(reason: &Reason, source: Option<&str>) -> String {
    let text = match reason {
        Reason::Traffic {
            from,
            via: Some(via),
            port,
        } => format!("traffic from {from} via {via} to port {port}"),
        Reason::Traffic { from, port, .. } => format!("traffic from {from} to port {port}"),
        Reason::Aws { queue } => format!("messages on {queue}"),
        other => other.kind().to_string(),
    };
    match source {
        Some(source) if !source.is_empty() => format!("{text}, by {source}"),
        _ => text,
    }
}

fn took(ms: f64) -> String {
    if ms < 1000.0 {
        format!(" in {ms:.0} ms")
    } else {
        format!(" in {:.1} s", ms / 1000.0)
    }
}

fn mib(bytes: u64) -> String {
    format!("{:.0} MiB", bytes as f64 / f64::from(1 << 20))
}

/// The core/v1 Event of `note` about the pod `identity`, from `node`.
pub fn body(note: &Note, identity: &PodIdentity, node: &str, now: SystemTime) -> Value {
    let since = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    let time = crate::logging::format_utc(since.as_secs(), since.subsec_millis());
    json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {
            "name": format!("{}.{:x}", identity.name, since.as_nanos()),
            "namespace": identity.namespace,
        },
        "involvedObject": {
            "apiVersion": "v1",
            "kind": "Pod",
            "namespace": identity.namespace,
            "name": identity.name,
            "uid": identity.uid,
        },
        "reason": note.reason,
        "message": note.message,
        "type": if note.warning { "Warning" } else { "Normal" },
        "source": {"component": COMPONENT, "host": node},
        "reportingComponent": COMPONENT,
        "reportingInstance": node,
        "firstTimestamp": time,
        "lastTimestamp": time,
        "count": 1,
    })
}

/// The Events posted, and those that did not go, since the node service
/// started.
#[derive(Default)]
struct Counts {
    sent: AtomicU64,
    rate_limited: AtomicU64,
    /// The queue was full, or the reporter fell behind the lifecycle.
    dropped: AtomicU64,
    failed: AtomicU64,
}

/// Posts the Events of the lifecycle's events.
pub struct Reporter {
    counts: Arc<Counts>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl Drop for Reporter {
    fn drop(&mut self) {
        for task in lock(&self.tasks).drain(..) {
            task.abort();
        }
    }
}

/// An Event to post, about the pod with this identity.
struct Outgoing {
    note: Note,
    identity: PodIdentity,
}

impl Reporter {
    /// Follows `lifecycle` and posts with the credentials of `kubeconfig`,
    /// read again after a failure, as k3s renews them.
    pub fn start(kubeconfig: impl Into<PathBuf>, lifecycle: Lifecycle) -> Arc<Reporter> {
        let counts = Arc::new(Counts::default());
        let (queue, outgoing) = mpsc::channel(QUEUE);
        let follow = tokio::spawn(follow(lifecycle, queue, counts.clone()));
        let post = tokio::spawn(post(kubeconfig.into(), outgoing, counts.clone()));
        Arc::new(Reporter {
            counts,
            tasks: Mutex::new(vec![follow, post]),
        })
    }

    pub fn render(&self, text: &mut Text) {
        let name = "smolvm_pods_kubernetes_events_total";
        text.family(
            name,
            Kind::Counter,
            "Kubernetes Events about pods' sleeps and wakes, by result: sent, rate_limited, \
             dropped or failed.",
        );
        let c = &self.counts;
        for (result, n) in [
            ("sent", &c.sent),
            ("rate_limited", &c.rate_limited),
            ("dropped", &c.dropped),
            ("failed", &c.failed),
        ] {
            text.sample(
                name,
                &[("result", result)],
                n.load(Ordering::Relaxed) as f64,
            );
        }
    }
}

/// Turns the lifecycle's events into Events for the poster, within the
/// limits.
async fn follow(lifecycle: Lifecycle, queue: mpsc::Sender<Outgoing>, counts: Arc<Counts>) {
    let mut events = lifecycle.subscribe();
    let mut limiter = Limiter::new(Instant::now());
    loop {
        let event = match events.recv().await {
            Ok(event) => event,
            Err(RecvError::Lagged(n)) => {
                counts.dropped.fetch_add(n, Ordering::Relaxed);
                continue;
            }
            Err(RecvError::Closed) => return,
        };
        if event.state == crate::lifecycle::PodState::Deleted {
            limiter.forget(&event.sandbox_id);
            continue;
        }
        let Some(note) = note_of(&event) else {
            continue;
        };
        let Some(info) = lifecycle.get(&PodRef::sandbox(&event.sandbox_id)) else {
            continue;
        };
        if info.identity.namespace.is_empty() || info.identity.name.is_empty() {
            continue;
        }
        if !limiter.allow(&event.sandbox_id, Instant::now()) {
            counts.rate_limited.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let outgoing = Outgoing {
            note,
            identity: info.identity,
        };
        if queue.try_send(outgoing).is_err() {
            counts.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Posts each Event, with a client made again after a failure.
async fn post(kubeconfig: PathBuf, mut outgoing: mpsc::Receiver<Outgoing>, counts: Arc<Counts>) {
    let mut client: Option<(Client, String)> = None;
    let mut last_error = String::new();
    while let Some(Outgoing { note, identity }) = outgoing.recv().await {
        if client.is_none() {
            client = match Client::from_kubeconfig(&kubeconfig) {
                Ok(c) => {
                    let node = c
                        .node_name()
                        .map(str::to_string)
                        .or_else(host_name)
                        .unwrap_or_default();
                    Some((c, node))
                }
                Err(e) => {
                    counts.failed.fetch_add(1, Ordering::Relaxed);
                    debug!("no Kubernetes Event about pod {}: {e}", identity.name);
                    continue;
                }
            };
        }
        let Some((c, node)) = &client else {
            continue;
        };
        let body = body(&note, &identity, node, SystemTime::now());
        let path = format!("/api/v1/namespaces/{}/events", identity.namespace);
        match c.post_json(&path, &body).await {
            Ok(()) => {
                counts.sent.fetch_add(1, Ordering::Relaxed);
                last_error.clear();
            }
            Err(e) => {
                counts.failed.fetch_add(1, Ordering::Relaxed);
                let e = e.to_string();
                if e != last_error {
                    warn!(
                        "post a Kubernetes Event about pod {}/{}: {e}",
                        identity.namespace, identity.name
                    );
                }
                last_error = e;
                client = None;
                tokio::time::sleep(RETRY_GAP).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::PodState;

    fn event(from: PodState, to: PodState, reason: Reason) -> Event {
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
            duration_ms: Some(230.0),
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
    fn a_pod_gets_a_burst_then_one_event_per_refill() {
        let start = Instant::now();
        let mut limiter = Limiter::new(start);
        let sent = (0..15).filter(|_| limiter.allow("a", start)).count();
        assert_eq!(sent, POD_BURST as usize);
        assert!(!limiter.allow("a", start + POD_REFILL / 2));
        assert!(limiter.allow("a", start + POD_REFILL));
        assert!(!limiter.allow("a", start + POD_REFILL));
        // Another pod has a budget of its own.
        assert!(limiter.allow("b", start + POD_REFILL));
        limiter.forget("a");
        assert!(limiter.allow("a", start + POD_REFILL));
    }

    #[test]
    fn the_node_caps_every_pod_together() {
        let start = Instant::now();
        let mut limiter = Limiter::new(start);
        let sent = (0..100)
            .filter(|i| limiter.allow(&format!("pod-{i}"), start))
            .count();
        assert_eq!(sent, NODE_BURST as usize);
        assert!(!limiter.allow("other", start));
        assert!(limiter.allow("other", start + NODE_REFILL));
        assert!(!limiter.allow("another", start + NODE_REFILL));
    }

    #[test]
    fn full_buckets_are_forgotten_past_the_prune_size() {
        let start = Instant::now();
        let mut limiter = Limiter::new(start);
        let mut at = start;
        for i in 0..=PRUNE_ABOVE {
            at += NODE_REFILL;
            assert!(limiter.allow(&format!("pod-{i}"), at));
        }
        at += POD_REFILL * POD_BURST as u32;
        assert!(limiter.allow("last", at));
        assert_eq!(limiter.pods.len(), 1);
    }

    #[test]
    fn sleeps_wakes_and_refusals_have_events() {
        let traffic = Reason::Traffic {
            from: "ns/client".into(),
            via: None,
            port: 8080,
        };
        let woke = note_of(&event(PodState::Waking, PodState::Awake, traffic.clone())).unwrap();
        assert_eq!(
            (woke.warning, woke.reason, woke.message.as_str()),
            (
                false,
                "Woke",
                "woke in 230 ms (traffic from ns/client to port 8080)"
            )
        );
        let mut slept = event(PodState::Pausing, PodState::Asleep, Reason::Idle);
        slept.duration_ms = Some(1840.0);
        slept.checkpoint_bytes = Some(120 << 20);
        assert_eq!(
            note_of(&slept).unwrap().message,
            "slept in 1.8 s (idle), a checkpoint of 120 MiB"
        );
        let mut waiting = event(PodState::Asleep, PodState::Asleep, Reason::Api);
        waiting.source = Some("floci".into());
        waiting.waiting = Some("awake pods 2/2".into());
        let note = note_of(&waiting).unwrap();
        assert_eq!(
            (note.reason, note.message.as_str()),
            (
                "WakeWaiting",
                "a wake (api, by floci) waits for admission: awake pods 2/2"
            )
        );
        let mut refused = event(PodState::Asleep, PodState::Asleep, traffic);
        refused.error = Some("wake refused: awake pods 2/2".into());
        let note = note_of(&refused).unwrap();
        assert_eq!((note.warning, note.reason), (true, "WakeRefused"));
        assert_eq!(
            note_of(&event(PodState::Asleep, PodState::Waking, Reason::Api)),
            None
        );
        assert_eq!(
            note_of(&event(PodState::Creating, PodState::Awake, Reason::Api)),
            None
        );
    }

    /// The poster POSTs each Event to its pod's namespace with the
    /// kubeconfig's token, and counts what the API server refused.
    #[tokio::test]
    async fn events_are_posted_to_the_pods_namespace() {
        use http_body_util::{BodyExt, Full};
        use hyper::service::service_fn;
        use hyper_util::rt::TokioIo;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (seen, mut requests) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            for status in [201u16, 403] {
                let (stream, _) = listener.accept().await.unwrap();
                let seen = seen.clone();
                let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                    let seen = seen.clone();
                    async move {
                        let (head, body) = request.into_parts();
                        let body = body.collect().await.unwrap().to_bytes();
                        let auth = head.headers.get("authorization").cloned();
                        let _ = seen.send((head.method, head.uri.to_string(), auth, body));
                        hyper::Response::builder()
                            .status(status)
                            .body(Full::new(bytes::Bytes::from_static(b"{}")))
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let kubeconfig = dir.path().join("kubeconfig");
        std::fs::write(
            &kubeconfig,
            format!(
                "clusters:\n- name: c\n  cluster:\n    server: http://{addr}\n\
                 users:\n- name: u\n  user:\n    token: secret\n\
                 contexts:\n- name: x\n  context:\n    cluster: c\n    user: u\n"
            ),
        )
        .unwrap();
        let counts = Arc::new(Counts::default());
        let (queue, outgoing) = mpsc::channel(QUEUE);
        tokio::spawn(post(kubeconfig, outgoing, counts.clone()));
        let identity = PodIdentity {
            sandbox_id: "sb".into(),
            namespace: "ns".into(),
            name: "web".into(),
            uid: "u-1".into(),
            ..Default::default()
        };
        let note = Note {
            warning: false,
            reason: "Woke",
            message: "woke in 230 ms (api)".into(),
        };
        for _ in 0..2 {
            let outgoing = Outgoing {
                note: note.clone(),
                identity: identity.clone(),
            };
            queue.send(outgoing).await.unwrap();
        }
        let (method, path, auth, body) = requests.recv().await.unwrap();
        assert_eq!(
            (method.as_str(), path.as_str()),
            ("POST", "/api/v1/namespaces/ns/events")
        );
        assert_eq!(auth.unwrap(), "Bearer secret");
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            (
                body["reason"].as_str(),
                body["involvedObject"]["name"].as_str()
            ),
            (Some("Woke"), Some("web"))
        );
        requests.recv().await.unwrap();
        for _ in 0..100 {
            if counts.failed.load(Ordering::Relaxed) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(counts.sent.load(Ordering::Relaxed), 1);
        assert_eq!(counts.failed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn the_event_names_the_pod_and_the_node() {
        let identity = PodIdentity {
            sandbox_id: "sb".into(),
            namespace: "ns".into(),
            name: "web".into(),
            uid: "u-1".into(),
            ..Default::default()
        };
        let note = Note {
            warning: true,
            reason: "WakeRefused",
            message: "no room".into(),
        };
        let at = UNIX_EPOCH + Duration::from_millis(1_760_000_000_123);
        let body = body(&note, &identity, "node-1", at);
        assert_eq!(body["metadata"]["namespace"], "ns");
        assert_eq!(
            body["metadata"]["name"],
            format!("web.{:x}", 1_760_000_000_123_000_000u128)
        );
        assert_eq!(
            body["involvedObject"],
            json!({"apiVersion": "v1", "kind": "Pod", "namespace": "ns", "name": "web", "uid": "u-1"})
        );
        assert_eq!(
            (body["type"].as_str(), body["reason"].as_str()),
            (Some("Warning"), Some("WakeRefused"))
        );
        assert_eq!(
            body["source"],
            json!({"component": "smolvm-pods", "host": "node-1"})
        );
        assert_eq!(body["firstTimestamp"], "2025-10-09T08:53:20.123Z");
        assert_eq!(body["count"], 1);
    }
}
