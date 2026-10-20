//! The watch of the pods bound to this node: a list, then a watch from the
//! list's resourceVersion, again from the last version seen whenever a watch
//! ends, and a new list after an error or when the version expired. Every
//! list and watch is node-scoped (`fieldSelector=spec.nodeName=NODE`), which
//! the Node authorizer requires of a kubelet's credentials.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, info, warn};
use serde::Deserialize;
use tokio::task::JoinHandle;

use super::client::{ApiError, Client};

/// The fields of a pod that the node service reads.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct Pod {
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: PodSpec,
    #[serde(default)]
    pub status: PodStatus,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMeta {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub uid: String,
    #[serde(default)]
    pub resource_version: String,
    /// RFC 3339 in UTC to the second, as the API server writes it.
    #[serde(default)]
    pub creation_timestamp: String,
    #[serde(default)]
    pub deletion_timestamp: Option<String>,
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodSpec {
    #[serde(default)]
    pub node_name: String,
    #[serde(default)]
    pub containers: Vec<Container>,
    #[serde(default)]
    pub init_containers: Vec<Container>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Container {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub ports: Vec<ContainerPort>,
    #[serde(default)]
    pub env: Vec<EnvVar>,
    /// `Always` for a sidecar, an init container that keeps running.
    #[serde(default)]
    pub restart_policy: Option<String>,
    #[serde(default)]
    pub startup_probe: Option<Probe>,
    #[serde(default)]
    pub liveness_probe: Option<Probe>,
    #[serde(default)]
    pub readiness_probe: Option<Probe>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerPort {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub container_port: i64,
}

/// An environment variable. One from `valueFrom` has an empty value here.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct EnvVar {
    pub name: String,
    #[serde(default)]
    pub value: String,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Probe {
    #[serde(default)]
    pub exec: Option<ExecAction>,
    #[serde(default)]
    pub http_get: Option<HttpGetAction>,
    #[serde(default)]
    pub tcp_socket: Option<TcpSocketAction>,
    #[serde(default)]
    pub grpc: Option<GrpcAction>,
    #[serde(default)]
    pub period_seconds: Option<i64>,
    #[serde(default)]
    pub timeout_seconds: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct ExecAction {
    #[serde(default)]
    pub command: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpGetAction {
    #[serde(default)]
    pub path: String,
    pub port: IntOrString,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub scheme: String,
    #[serde(default)]
    pub http_headers: Vec<HttpHeader>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct HttpHeader {
    pub name: String,
    #[serde(default)]
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TcpSocketAction {
    pub port: IntOrString,
    #[serde(default)]
    pub host: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct GrpcAction {
    pub port: i64,
    #[serde(default)]
    pub service: Option<String>,
}

/// A port as a number or a container port's name.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum IntOrString {
    Int(i64),
    String(String),
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodStatus {
    #[serde(default)]
    pub phase: String,
    #[serde(default, rename = "podIP")]
    pub pod_ip: String,
    #[serde(default)]
    pub conditions: Vec<PodCondition>,
    #[serde(default)]
    pub container_statuses: Vec<ContainerStatus>,
    #[serde(default)]
    pub init_container_statuses: Vec<ContainerStatus>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct PodCondition {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub status: String,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct ContainerStatus {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub ready: bool,
    /// Absent counts as false, as the API documents.
    #[serde(default)]
    pub started: Option<bool>,
}

/// Where the watch reports the pods. Each call runs in `spawn_blocking`.
pub trait PodSink: Send + Sync + 'static {
    /// Every pod bound to the node, after a list. It replaces what the
    /// earlier calls said.
    fn replace(&self, pods: Vec<Pod>);
    /// A pod was added or changed.
    fn apply(&self, pod: Pod);
    /// A pod was deleted.
    fn remove(&self, pod: Pod);
}

#[derive(Deserialize)]
struct PodList {
    #[serde(default)]
    metadata: ListMeta,
    #[serde(default)]
    items: Vec<Pod>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListMeta {
    #[serde(default)]
    resource_version: String,
}

#[derive(Deserialize)]
struct WatchEvent {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    object: serde_json::Value,
}

/// The `Status` of a watch's `ERROR` event.
#[derive(Default, Deserialize)]
struct Status {
    #[serde(default)]
    code: u16,
    #[serde(default)]
    message: String,
}

/// Why a session of list and watches ended.
enum End {
    /// The resourceVersion expired: list again at once.
    Expired,
    Failed(String),
}

impl From<ApiError> for End {
    fn from(e: ApiError) -> End {
        if e.is_expired() {
            End::Expired
        } else {
            End::Failed(e.to_string())
        }
    }
}

/// The watch of this node's pods with the credentials of a kubeconfig, such
/// as [`super::KUBELET_KUBECONFIG`].
pub struct PodWatch {
    kubeconfig: PathBuf,
    sink: Arc<dyn PodSink>,
    /// The first and the longest wait after a failure.
    backoff: (Duration, Duration),
    /// How long the API server keeps a watch open.
    watch_secs: u64,
}

impl PodWatch {
    pub fn new(kubeconfig: impl Into<PathBuf>, sink: Arc<dyn PodSink>) -> PodWatch {
        PodWatch {
            kubeconfig: kubeconfig.into(),
            sink,
            backoff: (Duration::from_secs(1), Duration::from_secs(30)),
            watch_secs: 300,
        }
    }

    /// Runs the watch on the current runtime until the task is aborted. It
    /// reads the kubeconfig again before every list, because the node
    /// service starts before k3s writes it, and k3s renews its certificates.
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(self.run())
    }

    async fn run(self) {
        let mut wait = self.backoff.0;
        let mut last_error = String::new();
        loop {
            let (listed, end) = self.session().await;
            if listed {
                wait = self.backoff.0;
                last_error.clear();
            }
            match end {
                End::Expired => debug!("the pod watch expired; listing again"),
                End::Failed(e) => {
                    if e != last_error {
                        warn!("the pod watch failed, and retries: {e}");
                    } else {
                        debug!("the pod watch failed again: {e}");
                    }
                    last_error = e;
                    tokio::time::sleep(wait).await;
                    wait = (wait * 2).min(self.backoff.1);
                }
            }
        }
    }

    /// A list, then watches until an error. Returns whether the list was
    /// delivered, and why the session ended.
    async fn session(&self) -> (bool, End) {
        let client = match Client::from_kubeconfig(&self.kubeconfig) {
            Ok(client) => client,
            Err(e) => return (false, End::Failed(e)),
        };
        let node = match client.node_name() {
            Some(node) => node.to_string(),
            None => match host_name() {
                Some(node) => node,
                None => return (false, End::Failed("the node's name is unknown".into())),
            },
        };
        let selector = format!("fieldSelector=spec.nodeName%3D{node}");
        let list: PodList = match client.get_json(&format!("/api/v1/pods?{selector}")).await {
            Ok(list) => list,
            Err(e) => return (false, e.into()),
        };
        info!(
            "the pod watch listed {} pods of node {node} on {}",
            list.items.len(),
            client.server()
        );
        let mut version = list.metadata.resource_version;
        self.deliver(move |sink| sink.replace(list.items)).await;
        loop {
            let path = format!(
                "/api/v1/pods?{selector}&watch=true&allowWatchBookmarks=true\
                 &timeoutSeconds={}&resourceVersion={version}",
                self.watch_secs
            );
            if let Err(end) = self.watch(&client, &path, &mut version).await {
                return (true, end);
            }
        }
    }

    /// One watch, until the API server ends it.
    async fn watch(&self, client: &Client, path: &str, version: &mut String) -> Result<(), End> {
        let mut lines = client.stream(path).await?;
        // A bookmark comes about once a minute, so a quiet minute and the
        // watch's own length mean a dead connection.
        let quiet = Duration::from_secs(self.watch_secs + 60);
        loop {
            let line = match tokio::time::timeout(quiet, lines.next()).await {
                Err(_) => return Err(End::Failed(format!("the watch was quiet for {quiet:?}"))),
                Ok(Err(e)) => return Err(End::Failed(e)),
                Ok(Ok(None)) => return Ok(()),
                Ok(Ok(Some(line))) => line,
            };
            let event: WatchEvent = serde_json::from_slice(&line)
                .map_err(|e| End::Failed(format!("parse a watch event: {e}")))?;
            match event.kind.as_str() {
                "ADDED" | "MODIFIED" | "DELETED" => {
                    let pod: Pod = serde_json::from_value(event.object)
                        .map_err(|e| End::Failed(format!("parse a watched pod: {e}")))?;
                    version.clone_from(&pod.metadata.resource_version);
                    if event.kind == "DELETED" {
                        self.deliver(move |sink| sink.remove(pod)).await;
                    } else {
                        self.deliver(move |sink| sink.apply(pod)).await;
                    }
                }
                "BOOKMARK" => {
                    let pod: Pod = serde_json::from_value(event.object).unwrap_or_default();
                    if !pod.metadata.resource_version.is_empty() {
                        *version = pod.metadata.resource_version;
                    }
                }
                "ERROR" => {
                    let status: Status = serde_json::from_value(event.object).unwrap_or_default();
                    return Err(ApiError {
                        status: Some(status.code),
                        message: format!("watch: {}", status.message),
                    }
                    .into());
                }
                other => debug!("skipped a watch event of type {other}"),
            }
        }
    }

    async fn deliver(&self, call: impl FnOnce(&dyn PodSink) + Send + 'static) {
        let sink = self.sink.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || call(&*sink)).await {
            warn!("the pod watch's sink failed: {e}");
        }
    }
}

/// k3s names its node after the host, in lower case, unless told otherwise.
fn host_name() -> Option<String> {
    let name = std::fs::read_to_string("/proc/sys/kernel/hostname").ok()?;
    let name = name.trim().to_lowercase();
    (!name.is_empty()).then_some(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::convert::Infallible;
    use std::net::SocketAddr;
    use std::path::Path;
    use std::sync::Mutex;

    use bytes::Bytes;
    use http_body_util::{BodyExt, StreamBody};
    use hyper::body::Frame;
    use hyper::service::service_fn;
    use hyper::{Request, Response};
    use hyper_util::rt::TokioIo;
    use rcgen::{BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa, KeyPair};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::server::WebPkiClientVerifier;
    use rustls::{RootCertStore, ServerConfig};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio_rustls::TlsAcceptor;

    /// A pod's JSON, as the API server sends it.
    fn pod_json(name: &str, uid: &str, version: &str) -> serde_json::Value {
        serde_json::json!({
            "metadata": {"name": name, "namespace": "default", "uid": uid,
                         "resourceVersion": version},
            "spec": {"nodeName": "node-1", "containers": [{"name": "app"}]},
            "status": {"phase": "Running", "podIP": "10.42.0.9"},
        })
    }

    /// Records what the watch reports.
    #[derive(Default)]
    struct Recorder {
        calls: Mutex<Vec<String>>,
        notify: Mutex<Option<mpsc::UnboundedSender<()>>>,
    }

    impl Recorder {
        fn push(&self, call: String) {
            crate::sync::lock(&self.calls).push(call);
            if let Some(tx) = &*crate::sync::lock(&self.notify) {
                let _ = tx.send(());
            }
        }
    }

    impl PodSink for Recorder {
        fn replace(&self, pods: Vec<Pod>) {
            let names: Vec<_> = pods.iter().map(|p| p.metadata.name.as_str()).collect();
            self.push(format!("replace {}", names.join(",")));
        }

        fn apply(&self, pod: Pod) {
            self.push(format!(
                "apply {}@{}",
                pod.metadata.name, pod.metadata.resource_version
            ));
        }

        fn remove(&self, pod: Pod) {
            self.push(format!("remove {}", pod.metadata.name));
        }
    }

    /// What the fake API server answers: the body of each request in turn,
    /// as a status and lines.
    type Script = Arc<Mutex<Vec<(u16, Vec<String>)>>>;

    struct Pki {
        ca: String,
        server: (CertificateDer<'static>, PrivateKeyDer<'static>),
        client: (String, String),
    }

    fn pki() -> Pki {
        let ca_key = KeyPair::generate().unwrap();
        let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca.distinguished_name = DistinguishedName::new();
        ca.distinguished_name
            .push(DnType::CommonName, "k3s-server-ca");
        let ca = ca.self_signed(&ca_key).unwrap();
        let server_key = KeyPair::generate().unwrap();
        let server = CertificateParams::new(vec!["127.0.0.1".to_string()])
            .unwrap()
            .signed_by(&server_key, &ca, &ca_key)
            .unwrap();
        let client_key = KeyPair::generate().unwrap();
        let mut client = CertificateParams::new(Vec::<String>::new()).unwrap();
        client.distinguished_name = DistinguishedName::new();
        client
            .distinguished_name
            .push(DnType::OrganizationName, "system:nodes");
        client
            .distinguished_name
            .push(DnType::CommonName, "system:node:node-1");
        let client = client.signed_by(&client_key, &ca, &ca_key).unwrap();
        Pki {
            ca: ca.pem(),
            server: (
                server.der().clone(),
                PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
            ),
            client: (client.pem(), client_key.serialize_pem()),
        }
    }

    /// An API server on 127.0.0.1 that requires a client certificate from
    /// the CA, answers from `script`, and sends each request's path to
    /// `paths`.
    async fn api_server(
        pki: &Pki,
        script: Script,
        paths: mpsc::UnboundedSender<String>,
    ) -> SocketAddr {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut roots = RootCertStore::empty();
        let ca = rustls_pemfile::certs(&mut pki.ca.as_bytes())
            .next()
            .unwrap()
            .unwrap();
        roots.add(ca).unwrap();
        let verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
                .build()
                .unwrap();
        let config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![pki.server.0.clone()], pki.server.1.clone_key())
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    return;
                };
                let (acceptor, script, paths) = (acceptor.clone(), script.clone(), paths.clone());
                tokio::spawn(async move {
                    let Ok(tls) = acceptor.accept(tcp).await else {
                        return;
                    };
                    let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                        let _ = paths.send(request.uri().to_string());
                        let next = {
                            let mut script = crate::sync::lock(&script);
                            (!script.is_empty()).then(|| script.remove(0))
                        };
                        async move {
                            let (status, lines) = next.unwrap_or((500, vec![]));
                            let frames = lines.into_iter().map(|line| {
                                Ok::<_, Infallible>(Frame::data(Bytes::from(line + "\n")))
                            });
                            let body = StreamBody::new(futures_util::stream::iter(frames));
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(status)
                                    .body(body.boxed())
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(tls), service)
                        .await;
                });
            }
        });
        addr
    }

    fn kubeconfig(dir: &Path, pki: &Pki, addr: SocketAddr) -> PathBuf {
        std::fs::write(dir.join("server-ca.crt"), &pki.ca).unwrap();
        std::fs::write(dir.join("client-kubelet.crt"), &pki.client.0).unwrap();
        std::fs::write(dir.join("client-kubelet.key"), &pki.client.1).unwrap();
        let path = dir.join("kubelet.kubeconfig");
        std::fs::write(
            &path,
            format!(
                "clusters:
- cluster: {{certificate-authority: server-ca.crt, server: \"https://{addr}\"}}
  name: local
contexts:
- context: {{cluster: local, user: user}}
  name: Default
current-context: Default
users:
- name: user
  user: {{client-certificate: client-kubelet.crt, client-key: client-kubelet.key}}
"
            ),
        )
        .unwrap();
        path
    }

    fn event(kind: &str, object: serde_json::Value) -> String {
        serde_json::json!({"type": kind, "object": object}).to_string()
    }

    #[tokio::test]
    async fn the_watch_lists_watches_and_lists_again_when_the_version_expires() {
        let pki = pki();
        let list = serde_json::json!({
            "kind": "PodList",
            "metadata": {"resourceVersion": "10"},
            "items": [pod_json("a", "uid-a", "8"), pod_json("b", "uid-b", "9")],
        });
        let bookmark = serde_json::json!({"kind": "Pod", "metadata": {"resourceVersion": "15"}});
        let expired = serde_json::json!({
            "kind": "Status", "status": "Failure", "code": 410, "reason": "Expired",
            "message": "too old resource version: 15 (20)",
        });
        let relist = serde_json::json!({"metadata": {"resourceVersion": "20"}, "items": []});
        let script: Script = Arc::new(Mutex::new(vec![
            (200, vec![list.to_string()]),
            (
                200,
                vec![
                    event("ADDED", pod_json("c", "uid-c", "11")),
                    String::new(),
                    event("MODIFIED", pod_json("a", "uid-a", "12")),
                    event("DELETED", pod_json("b", "uid-b", "13")),
                    event("BOOKMARK", bookmark),
                ],
            ),
            (200, vec![event("ERROR", expired)]),
            (200, vec![relist.to_string()]),
        ]));
        let (paths_tx, mut paths) = mpsc::unbounded_channel();
        let addr = api_server(&pki, script.clone(), paths_tx).await;
        let dir = tempfile::tempdir().unwrap();
        let recorder = Arc::new(Recorder::default());
        let (tx, mut calls) = mpsc::unbounded_channel();
        *crate::sync::lock(&recorder.notify) = Some(tx);
        let mut watch = PodWatch::new(kubeconfig(dir.path(), &pki, addr), recorder.clone());
        watch.watch_secs = 5;
        let task = watch.spawn();
        for _ in 0..5 {
            tokio::time::timeout(Duration::from_secs(10), calls.recv())
                .await
                .expect("the watch reports")
                .unwrap();
        }
        task.abort();
        assert_eq!(
            *crate::sync::lock(&recorder.calls),
            [
                "replace a,b",
                "apply c@11",
                "apply a@12",
                "remove b",
                "replace ",
            ]
        );
        let mut seen = Vec::new();
        while let Ok(path) = paths.try_recv() {
            seen.push(path);
        }
        let selector = "/api/v1/pods?fieldSelector=spec.nodeName%3Dnode-1";
        let watch = |version: &str| {
            format!(
                "{selector}&watch=true&allowWatchBookmarks=true&timeoutSeconds=5\
                 &resourceVersion={version}"
            )
        };
        assert_eq!(
            seen[..4],
            [
                selector.to_string(),
                watch("10"),
                watch("15"),
                selector.to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn a_failed_list_is_retried_with_a_backoff() {
        let pki = pki();
        let forbidden = serde_json::json!({
            "kind": "Status", "code": 403,
            "message": "pods is forbidden: User \"system:node:node-1\" cannot list resource",
        });
        let list = serde_json::json!({"metadata": {"resourceVersion": "1"}, "items": []});
        let script: Script = Arc::new(Mutex::new(vec![
            (403, vec![forbidden.to_string()]),
            (200, vec![list.to_string()]),
        ]));
        let (paths_tx, _paths) = mpsc::unbounded_channel();
        let addr = api_server(&pki, script, paths_tx).await;
        let dir = tempfile::tempdir().unwrap();
        let recorder = Arc::new(Recorder::default());
        let (tx, mut calls) = mpsc::unbounded_channel();
        *crate::sync::lock(&recorder.notify) = Some(tx);
        let mut watch = PodWatch::new(kubeconfig(dir.path(), &pki, addr), recorder.clone());
        watch.backoff = (Duration::from_millis(10), Duration::from_millis(20));
        let task = watch.spawn();
        tokio::time::timeout(Duration::from_secs(10), calls.recv())
            .await
            .expect("the watch lists after a retry")
            .unwrap();
        task.abort();
        assert_eq!(*crate::sync::lock(&recorder.calls), ["replace "]);
    }

    #[test]
    fn a_missing_kubeconfig_ends_the_session() {
        let watch = PodWatch::new(
            "/nonexistent/kubelet.kubeconfig",
            Arc::new(Recorder::default()),
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (listed, end) = runtime.block_on(watch.session());
        assert!(!listed);
        assert!(
            matches!(&end, End::Failed(e) if e.contains("read the kubeconfig")),
            "{}",
            match end {
                End::Failed(e) => e,
                End::Expired => "expired".into(),
            }
        );
    }

    #[test]
    fn a_pod_from_the_api_server_parses() {
        let pod: Pod = serde_json::from_value(pod_json("a", "uid-a", "7")).unwrap();
        assert_eq!(pod.metadata.uid, "uid-a");
        assert_eq!(pod.status.pod_ip, "10.42.0.9");
        assert_eq!(pod.spec.containers[0].name, "app");
    }

    /// In a node, as root: the kubelet's kubeconfig lists this node's pods.
    /// It prints the pods with probes as the API server sends them, and what
    /// the API server says to a list of every pod.
    #[tokio::test]
    #[ignore = "needs a node: run it there as root"]
    async fn live_the_kubelets_credentials_list_this_nodes_pods() {
        let path = Path::new(crate::kube::KUBELET_KUBECONFIG);
        let client = Client::from_kubeconfig(path).unwrap();
        let node = client.node_name().expect("a kubelet's credentials");
        let path = format!("/api/v1/pods?fieldSelector=spec.nodeName%3D{node}");
        let list: serde_json::Value = client.get_json(&path).await.unwrap();
        let items = list["items"].as_array().unwrap();
        eprintln!(
            "node {node} on {}: {} pods at resourceVersion {}",
            client.server(),
            items.len(),
            list["metadata"]["resourceVersion"]
        );
        for item in items {
            let pod: Pod = serde_json::from_value(item.clone()).unwrap();
            let probed = pod
                .spec
                .containers
                .iter()
                .any(|c| c.liveness_probe.is_some() || c.readiness_probe.is_some());
            if probed {
                eprintln!("{}", serde_json::to_string_pretty(item).unwrap());
            }
        }
        assert!(!items.is_empty());
        match client.get_json::<serde_json::Value>("/api/v1/pods").await {
            Ok(all) => eprintln!(
                "a list of every pod is allowed: {} pods",
                all["items"].as_array().map_or(0, Vec::len)
            ),
            Err(e) => eprintln!("a list of every pod is refused: {e}"),
        }
    }
}
