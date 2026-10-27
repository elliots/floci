//! The node service's API: HTTP and JSON on [`API_SOCKET`], which only root
//! may use, and on an optional TCP listener for Floci, where every request
//! needs a bearer token ([`serve_tcp`]). `smolvm-pods ctl` is its client.
//!
//! docs/configuration.md describes the routes, their JSON and the errors. The
//! runtime contract in tests/e2e/README.md lists what the end-to-end tests
//! read of them.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::convert::Infallible;
use std::io::Read;
use std::net::SocketAddr;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use axum::body::{Body, Bytes};
use axum::extract::rejection::QueryRejection;
use axum::extract::{Extension, Path as UrlPath, Query, Request, State};
use axum::http::header::{
    AUTHORIZATION, CACHE_CONTROL, CONTENT_SECURITY_POLICY, CONTENT_TYPE, HOST, WWW_AUTHENTICATE,
};
use axum::http::{HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post, put};
use axum::serve::ListenerExt;
use axum::{Json, Router};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use log::warn;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::broadcast::{self, error::RecvError};

use crate::activity::Flow;
use crate::lifecycle::{
    Blocker, Event, EventPage, EventQuery, HibernateRequest, HoldRequest, Lifecycle,
    LifecycleError, PodIdentity, PodInfo, PodRef, PodState, Reason, StepTime, WakeRequest,
};
use crate::logging::format_utc_ms;
use crate::metrics::{Kind as MetricKind, Text};
use crate::network::events::Kind as NetworkKind;
use crate::node::Node;
use logs::NodeLogs;

pub use docker::Docker;

mod docker;
mod logs;

/// The API's socket.
pub const API_SOCKET: &str = "/run/smolvm-pods/api.sock";

/// Overrides [`API_SOCKET`] for `ctl`, for tests.
const API_SOCKET_ENV: &str = "SMOLVM_PODS_API_SOCKET";

/// The most events one answer carries.
const EVENTS_LIMIT: usize = 500;

/// How long a follow of the events stays quiet before it sends a blank
/// line, which keeps the connection open through port forwards and lets a
/// dead client be noticed.
const FOLLOW_KEEPALIVE: Duration = Duration::from_secs(30);

/// The admin UI's page.
const UI_PATH: &str = "/ui";

/// The admin UI: one page of HTML and script, with no other files.
const UI: &str = include_str!("api/ui.html");

/// The admin UI may run its own inline script and style, and fetch from the
/// node service, and nothing else.
const UI_POLICY: &str = "default-src 'none'; script-src 'unsafe-inline'; \
    style-src 'unsafe-inline'; connect-src 'self'; img-src data:; frame-ancestors 'none'";

/// The shortest bearer token that the TCP listener accepts.
const TOKEN_MIN_LEN: usize = 16;

/// Binds the API's socket, which only root may use.
pub fn listen(socket: &Path) -> Result<tokio::net::UnixListener, String> {
    if let Some(dir) = socket.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    }
    // The node service's lock is held, so a socket there is a leftover.
    let _ = std::fs::remove_file(socket);
    let listener = tokio::net::UnixListener::bind(socket)
        .map_err(|e| format!("listen on {}: {e}", socket.display()))?;
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| format!("chmod {}: {e}", socket.display()))?;
    Ok(listener)
}

/// The routes, with the node's dockerd at `docker` for the Docker proxy and
/// the tunnels.
pub fn router(node: Node, docker: Docker) -> Router {
    routes(node, docker, NodeLogs::default())
}

/// The routes of [`router`], with the log files of `logs`.
fn routes(node: Node, docker: Docker, logs: NodeLogs) -> Router {
    Router::new()
        .route("/v1/pods", get(list))
        .route("/v1/pods/{id}", get(get_pod))
        .route("/v1/pods/{id}/wake", post(wake))
        .route("/v1/pods/{id}/hibernate", post(hibernate))
        .route(
            "/v1/pods/{id}/leases/{lease}",
            put(hold).patch(renew).delete(release),
        )
        .route("/v1/pods/{id}/agent", get(agent))
        .route("/v1/events", get(events))
        .route("/v1/network", get(network_get).put(network_put))
        .route("/v1/network/events", get(network_events))
        .route("/v1/traffic", get(traffic))
        .route("/v1/logs/{log}", get(logs::follow))
        .route("/metrics", get(metrics))
        .route(UI_PATH, get(ui))
        .route("/v1/tunnel", get(docker::tunnel))
        .route(&format!("{}/{{*path}}", docker::PREFIX), any(docker::proxy))
        .layer(Extension(docker))
        .layer(Extension(logs))
        .with_state(node)
}

/// Serves the API until its listener fails.
pub async fn serve(node: Node, listener: tokio::net::UnixListener, docker: Docker) {
    if let Err(e) = axum::serve(listener, router(node, docker)).await {
        warn!("the API stopped: {e}");
    }
}

/// The bearer token of the TCP listener.
#[derive(Clone)]
pub struct Token(Arc<str>);

impl Token {
    /// Reads the token from `path`: one line of at least [`TOKEN_MIN_LEN`]
    /// visible ASCII characters, in a file that only its owner, this
    /// process's user, may read or write.
    pub fn read(path: &Path) -> Result<Token, String> {
        let what = format!("the API token file {}", path.display());
        let mut file = std::fs::File::open(path).map_err(|e| format!("open {what}: {e}"))?;
        let meta = file.metadata().map_err(|e| format!("stat {what}: {e}"))?;
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(format!(
                "{what} has mode {mode:04o}: only its owner may read or write it (0600)"
            ));
        }
        // SAFETY: geteuid has no preconditions.
        let uid = unsafe { libc::geteuid() };
        if meta.uid() != uid {
            return Err(format!(
                "{what} belongs to uid {}, not to the node service's uid {uid}",
                meta.uid()
            ));
        }
        let mut text = String::new();
        file.read_to_string(&mut text)
            .map_err(|e| format!("read {what}: {e}"))?;
        Token::new(text.trim()).ok_or_else(|| {
            format!(
                "{what} must hold one token of at least {TOKEN_MIN_LEN} visible ASCII characters"
            )
        })
    }

    fn new(token: &str) -> Option<Token> {
        (token.len() >= TOKEN_MIN_LEN && token.bytes().all(|b| b.is_ascii_graphic()))
            .then(|| Token(token.into()))
    }

    /// Whether `presented` is the token, in a time that does not depend on
    /// where they differ.
    fn is(&self, presented: &str) -> bool {
        let (a, b) = (self.0.as_bytes(), presented.as_bytes());
        a.len() == b.len() && a.iter().zip(b).fold(0, |diff, (x, y)| diff | (x ^ y)) == 0
    }
}

/// Binds the API's TCP listener.
pub async fn listen_tcp(addr: SocketAddr) -> Result<tokio::net::TcpListener, String> {
    tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| format!("listen on {addr}: {e}"))
}

/// The routes of [`router`], each of which needs the bearer token.
pub fn tcp_router(node: Node, token: Token, docker: Docker) -> Router {
    authorized(router(node, docker), token)
}

/// `router` with every route behind the bearer token.
fn authorized(router: Router, token: Token) -> Router {
    router.layer(middleware::from_fn_with_state(token, authorize))
}

/// Serves the API on a TCP listener until it fails.
pub async fn serve_tcp(
    node: Node,
    listener: tokio::net::TcpListener,
    token: Token,
    docker: Docker,
) {
    let listener = listener.tap_io(|stream| {
        if let Err(e) = stream.set_nodelay(true) {
            warn!("TCP_NODELAY on an API connection: {e}");
        }
    });
    if let Err(e) = axum::serve(listener, tcp_router(node, token, docker)).await {
        warn!("the API's TCP listener stopped: {e}");
    }
}

/// Passes a request with `Authorization: Bearer TOKEN` on, and answers any
/// other with 401. The admin UI's page holds no data and asks for the token
/// itself, so `GET /ui` needs none.
async fn authorize(State(token): State<Token>, request: Request, next: Next) -> Response {
    if request.method() == Method::GET && request.uri().path() == UI_PATH {
        return next.run(request).await;
    }
    let presented = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(bearer);
    if presented.is_some_and(|presented| token.is(presented)) {
        return next.run(request).await;
    }
    let mut response = ApiError::new(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "a request needs Authorization: Bearer TOKEN, with the token of the node service's \
         --api-token-file",
    )
    .into_response();
    response
        .headers_mut()
        .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    response
}

/// The token of an `Authorization` header with the Bearer scheme.
fn bearer(header: &str) -> Option<&str> {
    let (scheme, token) = header.trim().split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| token.trim_start())
}

// ------------------------------------------------------------------- server

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    reasons: Vec<String>,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl ToString) -> ApiError {
        ApiError {
            status,
            code,
            message: message.to_string(),
            reasons: Vec::new(),
        }
    }

    fn refused(message: impl ToString, reasons: Vec<String>) -> ApiError {
        ApiError {
            reasons,
            ..ApiError::new(StatusCode::CONFLICT, "refused", message)
        }
    }

    /// The API error of `e`. For a hibernate call (`hibernate`), a failed pod
    /// is refused with `state failed`, as any state but awake is, and a pause
    /// that the `pausing` hook refused (the hold could not be set, or the
    /// recheck saw traffic) is refused with the hook's reason.
    fn of(e: LifecycleError, hibernate: bool) -> ApiError {
        let message = e.to_string();
        match e {
            LifecycleError::PauseRefused(reason) if hibernate => {
                ApiError::refused(message, vec![reason])
            }
            LifecycleError::NotFound(_) => {
                ApiError::new(StatusCode::NOT_FOUND, "not_found", message)
            }
            LifecycleError::StaleIncarnation(_) => {
                ApiError::new(StatusCode::CONFLICT, "stale_incarnation", message)
            }
            LifecycleError::Invalid(_) => {
                ApiError::new(StatusCode::BAD_REQUEST, "invalid", message)
            }
            LifecycleError::Refused(blockers) => ApiError::refused(message, reasons(&blockers)),
            LifecycleError::State { state, .. } => {
                ApiError::refused(message, vec![format!("state {state}")])
            }
            LifecycleError::Failed(_) if hibernate => {
                ApiError::refused(message, vec!["state failed".to_string()])
            }
            LifecycleError::Failed(_) => ApiError::new(StatusCode::CONFLICT, "failed", message),
            LifecycleError::Cancelled => ApiError::new(StatusCode::CONFLICT, "cancelled", message),
            LifecycleError::Vm(_) | LifecycleError::PauseRefused(_) => {
                ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "vm_error", message)
            }
            LifecycleError::Store(_) => {
                ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "store_error", message)
            }
            LifecycleError::Gone => ApiError::new(StatusCode::GONE, "gone", message),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({"error": {
            "code": self.code,
            "message": self.message,
            "reasons": self.reasons,
        }});
        (self.status, Json(body)).into_response()
    }
}

type Answer = Result<Json<Value>, ApiError>;

/// The short reasons of a refused pause, each once.
fn reasons(blockers: &[Blocker]) -> Vec<String> {
    let mut reasons: Vec<String> = Vec::new();
    for blocker in blockers {
        let reason = match blocker {
            Blocker::Disabled => "hibernation disabled",
            Blocker::Lease { .. } => "lease held",
            Blocker::Exec { .. } | Blocker::ExecStarting => "exec running",
            Blocker::ContainerCreating => "container being created",
            Blocker::WakeWaiter { .. } => "wake waiter",
            Blocker::Waking => "state waking",
            Blocker::Admission { .. } => "wake refused",
        };
        if !reasons.iter().any(|r| r == reason) {
            reasons.push(reason.to_string());
        }
    }
    reasons
}

/// A reason by its kind, such as `"api"`, or in full, such as
/// `{"kind": "traffic", "from": "10.42.0.7", "port": 8080}`.
#[derive(Deserialize)]
#[serde(untagged)]
enum ReasonArg {
    Kind(String),
    Full(Reason),
}

impl ReasonArg {
    fn reason(self) -> Result<Reason, ApiError> {
        match self {
            ReasonArg::Full(reason) => Ok(reason),
            ReasonArg::Kind(kind) => parse_reason(&kind).ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid",
                    format!("unknown reason {kind:?}"),
                )
            }),
        }
    }
}

/// The reasons that need no details.
fn parse_reason(kind: &str) -> Option<Reason> {
    serde_json::from_value(json!({ "kind": kind })).ok()
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct WakeBody {
    reason: Option<ReasonArg>,
    source: Option<String>,
    lease_ttl: Option<f64>,
    incarnation: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct HibernateBody {
    reason: Option<ReasonArg>,
    source: Option<String>,
    incarnation: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct HoldBody {
    ttl_secs: Option<f64>,
    holder: Option<String>,
    incarnation: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct RenewBody {
    ttl_secs: Option<f64>,
    incarnation: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct EventsParams {
    cursor: u64,
    limit: Option<usize>,
    pod: Option<String>,
    follow: bool,
}

/// The pods that `GET /v1/pods` and a path id name: each field given must
/// match.
#[derive(Deserialize, Default, Debug, PartialEq)]
#[serde(default, deny_unknown_fields)]
struct PodFilter {
    ip: Option<String>,
    uid: Option<String>,
    namespace: Option<String>,
    name: Option<String>,
    netns: Option<String>,
}

impl PodFilter {
    /// The filter of a path id other than a sandbox id: `uid:UID`, `ip:IP`,
    /// `netns:PATH` or `NAMESPACE/NAME`.
    fn of(id: &str) -> Option<PodFilter> {
        let mut filter = PodFilter::default();
        match id.split_once(':') {
            Some(("uid", uid)) => filter.uid = Some(uid.to_string()),
            Some(("ip", ip)) => filter.ip = Some(ip.to_string()),
            Some(("netns", netns)) => filter.netns = Some(netns.to_string()),
            _ => {
                let (namespace, name) = id.split_once('/')?;
                filter.namespace = Some(namespace.to_string());
                filter.name = Some(name.to_string());
            }
        }
        Some(filter)
    }

    fn matches(&self, pod: &PodIdentity) -> bool {
        let is = |want: &Option<String>, have: Option<&str>| {
            want.as_deref().is_none_or(|want| have == Some(want))
        };
        is(&self.ip, pod.ip.as_deref())
            && is(&self.uid, Some(&pod.uid))
            && is(&self.namespace, Some(&pod.namespace))
            && is(&self.name, Some(&pod.name))
            && is(&self.netns, pod.netns.as_deref())
    }
}

/// A body that may be empty.
fn body<T: Default + for<'de> Deserialize<'de>>(raw: &[u8]) -> Result<T, ApiError> {
    if raw.iter().all(u8::is_ascii_whitespace) {
        return Ok(T::default());
    }
    serde_json::from_slice(raw)
        .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, "invalid", e.to_string()))
}

fn query<T>(query: Result<Query<T>, QueryRejection>) -> Result<T, ApiError> {
    query
        .map(|Query(params)| params)
        .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, "invalid", e.body_text()))
}

/// The pod that a path id names (see the module's doc).
fn pod_of(node: &Node, id: &str) -> Result<PodInfo, ApiError> {
    let (found, what) = match PodFilter::of(id) {
        None => (
            node.lifecycle().get(&PodRef::sandbox(id)),
            format!("with sandbox id {id}"),
        ),
        Some(filter) => (choose(&filter, node.lifecycle().list()), id.to_string()),
    };
    found.ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "not_found", format!("no pod {what}")))
}

/// Of the pods that match `filter`, one that has not failed and is not
/// being deleted, then the newest.
fn choose(filter: &PodFilter, pods: Vec<PodInfo>) -> Option<PodInfo> {
    pods.into_iter()
        .filter(|pod| filter.matches(&pod.identity))
        .max_by_key(|pod| (pod.state.is_live(), pod.created_ms))
}

/// The sandbox id of the pod that a path id names.
fn sandbox_of(node: &Node, id: &str) -> Result<String, ApiError> {
    Ok(pod_of(node, id)?.identity.sandbox_id)
}

async fn list(
    State(node): State<Node>,
    filter: Result<Query<PodFilter>, QueryRejection>,
) -> Answer {
    let filter = query(filter)?;
    let pods: Vec<Value> = node
        .lifecycle()
        .list()
        .iter()
        .filter(|pod| filter.matches(&pod.identity))
        .map(pod_json)
        .collect();
    Ok(Json(json!({ "pods": pods })))
}

async fn get_pod(State(node): State<Node>, UrlPath(id): UrlPath<String>) -> Answer {
    Ok(Json(pod_json(&pod_of(&node, &id)?)))
}

async fn wake(
    State(node): State<Node>,
    UrlPath(id): UrlPath<String>,
    raw: axum::body::Bytes,
) -> Answer {
    let request: WakeBody = body(&raw)?;
    let reason = request.reason.map_or(Ok(Reason::Api), ReasonArg::reason)?;
    let sandbox = sandbox_of(&node, &id)?;
    let wake = WakeRequest {
        reason,
        source: request.source,
        lease_ttl: request.lease_ttl,
        incarnation: request.incarnation,
    };
    let report = node
        .lifecycle()
        .wake(&PodRef::sandbox(&sandbox), wake)
        .await
        .map_err(|e| ApiError::of(e, false))?;
    let info = pod_of(&node, &sandbox)?;
    Ok(Json(json!({
        "pod": pod_json(&info),
        "resumed": report.resumed,
        "cancelled_pause": report.cancelled_pause,
        "generation": report.generation,
        "duration_ms": report.duration_ms,
        "steps_ms": steps_json(&report.steps),
        "clock": report.clock,
        "lease": report.lease.map(|l| lease_json(&l.id, l.holder.as_deref(), l.expires_ms)),
    })))
}

async fn hibernate(
    State(node): State<Node>,
    UrlPath(id): UrlPath<String>,
    raw: axum::body::Bytes,
) -> Answer {
    let request: HibernateBody = body(&raw)?;
    let reason = request.reason.map_or(Ok(Reason::Api), ReasonArg::reason)?;
    let sandbox = sandbox_of(&node, &id)?;
    let sleep = HibernateRequest {
        reason,
        source: request.source,
        incarnation: request.incarnation,
    };
    let report = node
        .lifecycle()
        .hibernate(&PodRef::sandbox(&sandbox), sleep)
        .await
        .map_err(|e| ApiError::of(e, true))?;
    let info = pod_of(&node, &sandbox)?;
    Ok(Json(json!({
        "pod": pod_json(&info),
        "already_asleep": report.already_asleep,
        "generation": report.generation,
        "duration_ms": report.duration_ms,
        "steps_ms": steps_json(&report.steps),
        "checkpoint_bytes": report.checkpoint_bytes,
        "save": report.save,
    })))
}

async fn hold(
    State(node): State<Node>,
    UrlPath((id, lease)): UrlPath<(String, String)>,
    raw: axum::body::Bytes,
) -> Answer {
    let request: HoldBody = body(&raw)?;
    let sandbox = sandbox_of(&node, &id)?;
    let hold = HoldRequest {
        id: Some(lease),
        ttl_secs: request.ttl_secs,
        holder: request.holder,
        incarnation: request.incarnation,
    };
    let held = node
        .lifecycle()
        .hold(&PodRef::sandbox(&sandbox), hold)
        .map_err(|e| ApiError::of(e, false))?;
    let info = pod_of(&node, &sandbox)?;
    Ok(Json(json!({
        "pod": pod_json(&info),
        "lease": lease_json(&held.id, held.holder.as_deref(), held.expires_ms),
    })))
}

/// Renews a lease that has not expired. Unlike a hold, it never adds a lease
/// to a pod that may have slept since the last one ended.
async fn renew(
    State(node): State<Node>,
    UrlPath((id, lease)): UrlPath<(String, String)>,
    raw: axum::body::Bytes,
) -> Answer {
    let request: RenewBody = body(&raw)?;
    let sandbox = sandbox_of(&node, &id)?;
    let renewed = node
        .lifecycle()
        .renew(
            &PodRef::sandbox(&sandbox),
            &lease,
            request.ttl_secs,
            request.incarnation.as_deref(),
        )
        .map_err(|e| match e {
            LifecycleError::NotFound(_) => ApiError::new(
                StatusCode::NOT_FOUND,
                "lease_not_found",
                format!("pod sandbox {sandbox} has no lease {lease}, or it expired"),
            ),
            e => ApiError::of(e, false),
        })?;
    let info = pod_of(&node, &sandbox)?;
    Ok(Json(json!({
        "pod": pod_json(&info),
        "lease": lease_json(&renewed.id, renewed.holder.as_deref(), renewed.expires_ms),
    })))
}

async fn release(
    State(node): State<Node>,
    UrlPath((id, lease)): UrlPath<(String, String)>,
) -> Answer {
    let sandbox = sandbox_of(&node, &id)?;
    node.lifecycle()
        .release(&PodRef::sandbox(&sandbox), &lease, None)
        .map_err(|e| ApiError::of(e, false))?;
    Ok(Json(json!({ "pod": pod_json(&pod_of(&node, &sandbox)?) })))
}

async fn agent(State(node): State<Node>, UrlPath(id): UrlPath<String>) -> Answer {
    let sandbox = sandbox_of(&node, &id)?;
    let stats = node
        .agent_stats(&sandbox)
        .await
        .map_err(|e| ApiError::of(e, false))?;
    Ok(Json(json!(stats)))
}

async fn events(
    State(node): State<Node>,
    params: Result<Query<EventsParams>, QueryRejection>,
) -> Result<Response, ApiError> {
    let params = query(params)?;
    if params.follow {
        return Ok(follow(node.lifecycle().clone(), params.cursor, params.pod));
    }
    let query = EventQuery {
        after: params.cursor,
        sandbox_id: params.pod,
        limit: params.limit.unwrap_or(EVENTS_LIMIT).clamp(1, EVENTS_LIMIT),
    };
    let page = node
        .lifecycle()
        .events(&query)
        .map_err(|e| ApiError::of(e, false))?;
    let events: Vec<Value> = page.events.iter().map(event_json).collect();
    Ok(Json(json!({
        "events": events,
        "cursor": page.cursor,
        "gap": page.gap,
    }))
    .into_response())
}

/// `GET /ui`: the admin UI.
async fn ui() -> Response {
    (
        [
            (CONTENT_TYPE, "text/html; charset=utf-8"),
            (CONTENT_SECURITY_POLICY, UI_POLICY),
            (CACHE_CONTROL, "no-store"),
        ],
        UI,
    )
        .into_response()
}

/// `GET /v1/traffic`: the traffic between pods that the idle loop counted.
async fn traffic(State(node): State<Node>) -> Answer {
    let flows: Vec<Value> = node
        .flows()
        .iter()
        .map(|flow| {
            let last = flow
                .last
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            json!({
                "from": flow.from,
                "to": flow.to,
                "packets": flow.packets,
                "bytes": flow.bytes,
                "last": format_utc_ms(last),
            })
        })
        .collect();
    Ok(Json(json!({ "flows": flows })))
}

/// `GET /metrics`: the node's metrics in the Prometheus text format.
async fn metrics(State(node): State<Node>) -> Response {
    let text = tokio::task::spawn_blocking(move || metrics_text(&node))
        .await
        .unwrap_or_default();
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        text,
    )
        .into_response()
}

/// The states that `smolvm_pods_pods` counts.
const STATES: [PodState; 7] = [
    PodState::Creating,
    PodState::Awake,
    PodState::Pausing,
    PodState::Asleep,
    PodState::Waking,
    PodState::Failed,
    PodState::Deleting,
];

/// Every metric of the node service. Blocking.
fn metrics_text(node: &Node) -> String {
    let mut text = Text::new();
    let pods = node.lifecycle().list();
    text.family(
        "smolvm_pods_pods",
        MetricKind::Gauge,
        "Pods with a record, by state.",
    );
    for state in STATES {
        let n = pods.iter().filter(|p| p.state == state).count();
        text.sample("smolvm_pods_pods", &[("state", state.as_str())], n as f64);
    }
    text.family(
        "smolvm_pods_checkpoint_bytes",
        MetricKind::Gauge,
        "The size of the checkpoints of the sleeping pods.",
    );
    let bytes: u64 = pods
        .iter()
        .filter(|p| p.state == PodState::Asleep)
        .filter_map(|p| p.checkpoint_bytes)
        .sum();
    text.sample("smolvm_pods_checkpoint_bytes", &[], bytes as f64);
    node.lifecycle().transitions().render(&mut text);
    node.render_metrics(&mut text);
    if let Some(network) = crate::network::get() {
        let counters = network.events.counters();
        text.family(
            "smolvm_pods_network_attempts_total",
            MetricKind::Counter,
            "Connections and datagrams of pods that the sinkhole blocked or the relay \
             passed on, or failed to, by kind.",
        );
        for kind in NetworkKind::ALL {
            let n = counters.get(&kind).copied().unwrap_or(0);
            text.sample(
                "smolvm_pods_network_attempts_total",
                &[("kind", kind.as_str())],
                n as f64,
            );
        }
    }
    render_flows(&mut text, &pods, &node.flows());
    match crate::metrics::process() {
        Ok(process) => crate::metrics::render_process(&mut text, &process),
        Err(e) => warn!("the node service's process metrics: {e}"),
    }
    text.finish()
}

/// The traffic between pods, by the namespace and name of each.
fn render_flows(text: &mut Text, pods: &[PodInfo], flows: &[Flow]) {
    let names: HashMap<&str, &PodIdentity> = pods
        .iter()
        .map(|p| (p.identity.sandbox_id.as_str(), &p.identity))
        .collect();
    let families = [
        (
            "smolvm_pods_pod_traffic_packets_total",
            "Packets, either way, on connections that one pod opened to another.",
        ),
        (
            "smolvm_pods_pod_traffic_bytes_total",
            "Bytes, either way, on connections that one pod opened to another.",
        ),
    ];
    for (i, (name, help)) in families.into_iter().enumerate() {
        text.family(name, MetricKind::Counter, help);
        for flow in flows {
            let (Some(from), Some(to)) =
                (names.get(flow.from.as_str()), names.get(flow.to.as_str()))
            else {
                continue;
            };
            let value = if i == 0 { flow.packets } else { flow.bytes };
            text.sample(
                name,
                &[
                    ("source_namespace", &from.namespace),
                    ("source_pod", &from.name),
                    ("destination_namespace", &to.namespace),
                    ("destination_pod", &to.name),
                ],
                value as f64,
            );
        }
    }
}

fn network() -> Result<Arc<crate::network::Shared>, ApiError> {
    crate::network::get().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "the node's network is not running",
        )
    })
}

async fn network_get() -> Answer {
    Ok(Json(network()?.status()))
}

/// `PUT /v1/network`: checks the profile, then applies it.
async fn network_put(raw: Bytes) -> Answer {
    let shared = network()?;
    let profile: crate::network::profile::Profile = serde_json::from_slice(&raw).map_err(|e| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid",
            format!("the network profile: {e}"),
        )
    })?;
    if let Err(e) = profile.clone().check() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid",
            format!("the network profile: {e}"),
        ));
    }
    tokio::task::spawn_blocking(move || shared.put(profile))
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "apply_error", e))?
        .map(Json)
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "apply_error", e))
}

/// `GET /v1/network/events`: the network events after `cursor`, and with
/// `follow=true` each new one as a line as it is written.
async fn network_events(
    params: Result<Query<EventsParams>, QueryRejection>,
) -> Result<Response, ApiError> {
    let params = query(params)?;
    let shared = network()?;
    let limit = params.limit.unwrap_or(EVENTS_LIMIT).clamp(1, EVENTS_LIMIT);
    if !params.follow {
        let (events, cursor) = shared.events.page(params.cursor, limit);
        return Ok(Json(json!({"events": events, "cursor": cursor})).into_response());
    }
    let receiver = shared.events.subscribe();
    let state = (shared, receiver, params.cursor, VecDeque::new());
    let lines = futures_util::stream::unfold(
        state,
        |(shared, mut rx, mut cursor, mut backlog)| async move {
            loop {
                if let Some(event) = backlog.pop_front() {
                    let mut text = serde_json::to_string(&event).unwrap_or_default();
                    text.push('\n');
                    return Some((
                        Ok::<Bytes, Infallible>(Bytes::from(text)),
                        (shared, rx, cursor, backlog),
                    ));
                }
                let (page, next) = shared.events.page(cursor, EVENTS_LIMIT);
                if !page.is_empty() {
                    cursor = next;
                    backlog.extend(page);
                    continue;
                }
                match tokio::time::timeout(FOLLOW_KEEPALIVE, rx.recv()).await {
                    Err(_) => {
                        return Some((
                            Ok(Bytes::from_static(b"\n")),
                            (shared, rx, cursor, backlog),
                        ));
                    }
                    Ok(Ok(_)) | Ok(Err(RecvError::Lagged(_))) => {}
                    Ok(Err(RecvError::Closed)) => return None,
                }
            }
        },
    );
    Ok((
        [(CONTENT_TYPE, "application/x-ndjson")],
        Body::from_stream(lines),
    )
        .into_response())
}

/// `GET /v1/events?follow=true`: the events after `cursor`, of the pod
/// sandbox `pod` or of all, one JSON object per line, then each new one as
/// it happens, until the client goes. A blank line comes after
/// [`FOLLOW_KEEPALIVE`] without an event.
fn follow(lifecycle: Lifecycle, cursor: u64, pod: Option<String>) -> Response {
    let events = lifecycle.subscribe();
    let log: ReadLog = Arc::new(move |query| lifecycle.events(query).map_err(|e| e.to_string()));
    let follower = Follower::new(events, log, cursor, pod);
    let lines = futures_util::stream::unfold(follower, |mut follower| async move {
        let line = follower.next().await?;
        Some((Ok::<Bytes, Infallible>(line), follower))
    });
    (
        [(CONTENT_TYPE, "application/x-ndjson")],
        Body::from_stream(lines),
    )
        .into_response()
}

/// Reads a page of the event log. Blocking.
type ReadLog = Arc<dyn Fn(&EventQuery) -> Result<EventPage, String> + Send + Sync>;

/// A follower of the events: it reads the event log while it is behind,
/// then takes new events from the lifecycle's broadcast. The log has every
/// event before the broadcast sends it, so a gap in the broadcast's
/// sequence numbers, or a lag that drops events, sends it back to the log.
struct Follower {
    /// Subscribed before the log is first read, so that no event falls
    /// between the two.
    events: broadcast::Receiver<Event>,
    log: ReadLog,
    /// Every event up to this sequence number was sent or filtered out.
    cursor: u64,
    pod: Option<String>,
    /// Events read from the log and not yet sent.
    backlog: VecDeque<Event>,
    /// Read the log next: at the start, after a full page, after a gap.
    behind: bool,
    keepalive: Duration,
}

impl Follower {
    fn new(
        events: broadcast::Receiver<Event>,
        log: ReadLog,
        cursor: u64,
        pod: Option<String>,
    ) -> Follower {
        Follower {
            events,
            log,
            cursor,
            pod,
            backlog: VecDeque::new(),
            behind: true,
            keepalive: FOLLOW_KEEPALIVE,
        }
    }

    /// The next line: an event, or a blank line after a quiet while. None
    /// ends the stream.
    async fn next(&mut self) -> Option<Bytes> {
        loop {
            if let Some(event) = self.backlog.pop_front() {
                return Some(line(&event));
            }
            if self.behind {
                if let Err(e) = self.read_log().await {
                    warn!("an event stream ended: {e}");
                    return None;
                }
                continue;
            }
            let received = match tokio::time::timeout(self.keepalive, self.events.recv()).await {
                Ok(received) => received,
                Err(_) => return Some(Bytes::from_static(b"\n")),
            };
            match received {
                Ok(event) if event.seq <= self.cursor => {}
                Ok(event) if event.seq == self.cursor + 1 => {
                    self.cursor = event.seq;
                    if self.wants(&event) {
                        return Some(line(&event));
                    }
                }
                Ok(_) | Err(RecvError::Lagged(_)) => self.behind = true,
                Err(RecvError::Closed) => return None,
            }
        }
    }

    fn wants(&self, event: &Event) -> bool {
        self.pod.as_ref().is_none_or(|pod| *pod == event.sandbox_id)
    }

    /// Reads one page of the log after the cursor into the backlog.
    async fn read_log(&mut self) -> Result<(), String> {
        let query = EventQuery {
            after: self.cursor,
            sandbox_id: self.pod.clone(),
            limit: EVENTS_LIMIT,
        };
        let log = self.log.clone();
        let page = tokio::task::spawn_blocking(move || log(&query))
            .await
            .map_err(|e| format!("read the event log: {e}"))??;
        self.behind = page.events.len() >= EVENTS_LIMIT;
        self.cursor = self.cursor.max(page.cursor);
        self.backlog.extend(page.events);
        Ok(())
    }
}

/// An event as one line of a follow.
fn line(event: &Event) -> Bytes {
    let mut text = event_json(event).to_string();
    text.push('\n');
    Bytes::from(text)
}

fn lease_json(id: &str, holder: Option<&str>, expires_ms: u64) -> Value {
    json!({
        "id": id,
        "kind": "api",
        "holder": holder,
        "expires_at": format_utc_ms(expires_ms),
    })
}

fn implicit_lease(id: String, kind: &str) -> Value {
    json!({ "id": id, "kind": kind })
}

/// A pod, with its leases: the API's holds, and the implicit leases, by kind
/// `exec`, `create` and `waiter`.
fn pod_json(info: &PodInfo) -> Value {
    let identity = &info.identity;
    let mut leases: Vec<Value> = info
        .leases
        .iter()
        .map(|l| lease_json(&l.id, l.holder.as_deref(), l.expires_ms))
        .collect();
    for exec in &info.execs {
        leases.push(implicit_lease(format!("exec:{exec}"), "exec"));
    }
    if info.starting_execs > 0 {
        leases.push(implicit_lease("exec:starting".into(), "exec"));
    }
    if info.creates > 0 {
        leases.push(implicit_lease("create".into(), "create"));
    }
    if info.waiters > 0 {
        leases.push(implicit_lease("waiter".into(), "waiter"));
    }
    json!({
        "sandbox_id": identity.sandbox_id,
        "namespace": identity.namespace,
        "name": identity.name,
        "uid": identity.uid,
        "labels": identity.labels,
        "controller": identity.controller,
        "ip": identity.ip,
        "mac": identity.mac,
        "netns": identity.netns,
        "machine": identity.machine,
        "state": info.state.as_str(),
        "phase": info.phase,
        "generation": info.generation,
        "incarnation": identity.incarnation,
        "rosetta": identity.rosetta,
        "hibernate": !identity.hibernate_disabled,
        "standalone": identity.standalone,
        "vm_pid": info.vm.map(|vm| vm.pid),
        "leases": leases,
        "created": format_utc_ms(info.created_ms),
        "state_since": format_utc_ms(info.since_ms),
        "last_wake_ms": info.last_wake.map(|t| t.ms),
        "last_pause_ms": info.last_sleep.map(|t| t.ms),
        "checkpoint_bytes": info.checkpoint_bytes,
        "wake_failures": info.wake_failures,
        "failure": info.failure,
    })
}

/// Durations by step name, in milliseconds. A step that ran more than once is
/// the sum of its runs.
fn steps_json(steps: &[StepTime]) -> Value {
    let mut out = serde_json::Map::new();
    for step in steps {
        let total = out.get(&step.step).and_then(Value::as_f64).unwrap_or(0.0) + step.ms;
        out.insert(step.step.clone(), json!(total));
    }
    Value::Object(out)
}

/// An event. A new record's `from` is `none`, and a removed one's `to`
/// `deleted`. The reason is its kind, and details such as a traffic
/// source are in `reason_detail`.
fn event_json(event: &Event) -> Value {
    let reason = &event.reason;
    let detail = matches!(reason, Reason::Traffic { .. } | Reason::Aws { .. }).then_some(reason);
    json!({
        "seq": event.seq,
        "time": format_utc_ms(event.time_ms),
        "pod": event.sandbox_id,
        "incarnation": event.incarnation,
        "from": event.from.map_or("none", |s| s.as_str()),
        "to": event.state.as_str(),
        "generation": event.generation,
        "reason": reason.kind(),
        "reason_detail": detail,
        "source": event.source,
        "steps_ms": steps_json(&event.steps),
        "duration_ms": event.duration_ms,
        "checkpoint_bytes": event.checkpoint_bytes,
        "save": event.save,
        "clock": event.clock,
        "error": event.error,
        "warning": event.warning,
        "cancelled": event.cancelled,
        "capture_to_release_ms": event.capture_to_release_ms,
        "waiting": event.waiting,
        "recovered": event.recovered,
    })
}

// ---------------------------------------------------------------------- ctl

/// The exit statuses of ctl.
const EXIT_ERROR: i32 = 1;
const EXIT_USAGE: i32 = 2;
const EXIT_UNREACHABLE: i32 = 3;

const USAGE: &str = "usage: smolvm-pods ctl COMMAND ARGS
  get POD
  list
  wake POD [--reason REASON] [--source TEXT] [--lease-ttl SECONDS]
  hibernate POD [--reason REASON] [--source TEXT]
  hold POD --lease ID --ttl SECONDS [--holder TEXT]
  release POD --lease ID
  events [--cursor SEQ]
  agent POD
POD is the CRI sandbox id, uid:UID, ip:IP, netns:PATH or NAMESPACE/NAME.
REASON is api, exec, container-create, signal, idle, error or recovery.";

/// A ctl call: the HTTP request it makes.
#[derive(Debug, PartialEq)]
struct Call {
    method: &'static str,
    path: String,
    body: Option<Value>,
    /// `events`: every page from this cursor, one event per line.
    events_from: Option<u64>,
}

/// Parses ctl's command line.
fn parse(args: &[String]) -> Result<Call, String> {
    let (command, rest) = args.split_first().ok_or("no command")?;
    let mut positional = Vec::new();
    let mut flags: BTreeMap<String, String> = BTreeMap::new();
    let mut words = rest.iter();
    while let Some(word) = words.next() {
        match word.strip_prefix("--") {
            Some(flag) => {
                let value = words
                    .next()
                    .ok_or_else(|| format!("--{flag} needs a value"))?;
                flags.insert(flag.to_string(), value.clone());
            }
            None => positional.push(word.clone()),
        }
    }
    let allowed: &[&str] = match command.as_str() {
        "get" | "agent" | "list" => &[],
        "wake" => &["reason", "source", "lease-ttl"],
        "hibernate" => &["reason", "source"],
        "hold" => &["lease", "ttl", "holder"],
        "release" => &["lease"],
        "events" => &["cursor"],
        other => return Err(format!("unknown command {other:?}")),
    };
    if let Some(flag) = flags.keys().find(|f| !allowed.contains(&f.as_str())) {
        return Err(format!("{command} takes no --{flag}"));
    }
    let pods = match command.as_str() {
        "list" | "events" => 0,
        _ => 1,
    };
    if positional.len() != pods {
        return Err(format!("{command} takes {pods} pod argument(s)"));
    }
    let pod = positional.first().map(|p| segment(p)).unwrap_or_default();
    let number = |name: &str| -> Result<Option<f64>, String> {
        flags
            .get(name)
            .map(|v| v.parse::<f64>().map_err(|e| format!("--{name}: {e}")))
            .transpose()
    };
    let reason = match flags.get("reason") {
        Some(kind) if parse_reason(kind).is_none() => {
            return Err(format!("unknown reason {kind:?}"));
        }
        reason => reason.cloned(),
    };
    let required = |name: &str| {
        flags
            .get(name)
            .cloned()
            .ok_or_else(|| format!("{command} needs --{name}"))
    };
    let call = |method, path: String, body| Call {
        method,
        path,
        body,
        events_from: None,
    };
    Ok(match command.as_str() {
        "get" => call("GET", format!("/v1/pods/{pod}"), None),
        "list" => call("GET", "/v1/pods".into(), None),
        "agent" => call("GET", format!("/v1/pods/{pod}/agent"), None),
        "wake" => call(
            "POST",
            format!("/v1/pods/{pod}/wake"),
            Some(json!({
                "reason": reason,
                "source": flags.get("source"),
                "lease_ttl": number("lease-ttl")?,
            })),
        ),
        "hibernate" => call(
            "POST",
            format!("/v1/pods/{pod}/hibernate"),
            Some(json!({ "reason": reason, "source": flags.get("source") })),
        ),
        "hold" => {
            let lease = segment(&required("lease")?);
            let ttl = number("ttl")?.ok_or("hold needs --ttl")?;
            call(
                "PUT",
                format!("/v1/pods/{pod}/leases/{lease}"),
                Some(json!({ "ttl_secs": ttl, "holder": flags.get("holder") })),
            )
        }
        "release" => {
            let lease = segment(&required("lease")?);
            call("DELETE", format!("/v1/pods/{pod}/leases/{lease}"), None)
        }
        _ => {
            let cursor = match flags.get("cursor") {
                Some(c) => c.parse().map_err(|e| format!("--cursor: {e}"))?,
                None => 0,
            };
            Call {
                method: "GET",
                path: String::new(),
                body: None,
                events_from: Some(cursor),
            }
        }
    })
}

/// Percent-encodes a path segment.
fn segment(text: &str) -> String {
    let mut out = String::new();
    for b in text.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// How a ctl request failed to get an answer.
#[derive(Debug)]
struct Unreachable(String);

/// One HTTP/1.1 request on the API socket. Returns the status and body.
fn request(
    socket: &Path,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> Result<(u16, Vec<u8>), Unreachable> {
    let unreachable = |what: &str, e: &dyn std::fmt::Display| Unreachable(format!("{what}: {e}"));
    let request = axum::http::Request::builder()
        .method(method)
        .uri(path)
        .header(HOST, "smolvm-pods")
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(
            body.map(Value::to_string).unwrap_or_default(),
        )))
        .map_err(|e| unreachable("make the request", &e))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .map_err(|e| unreachable("start a runtime", &e))?;
    runtime.block_on(async {
        let stream = tokio::net::UnixStream::connect(socket)
            .await
            .map_err(|e| unreachable(&format!("connect to {}", socket.display()), &e))?;
        let (mut sender, connection) = http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|e| unreachable("start HTTP", &e))?;
        tokio::spawn(connection);
        let response = sender
            .send_request(request)
            .await
            .map_err(|e| unreachable("send the request", &e))?;
        let status = response.status().as_u16();
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|e| unreachable("read the answer", &e))?;
        Ok((status, body.to_bytes().to_vec()))
    })
}

/// `smolvm-pods ctl ARGS`. Prints one JSON document, or one event per line,
/// and returns the exit status: 0, 1 when the API answered with an error,
/// printed as JSON, 2 for a wrong command line, and 3 when the API could not
/// be reached or the connection closed before the answer.
pub fn ctl(args: &[String]) -> i32 {
    let call = match parse(args) {
        Ok(call) => call,
        Err(e) => {
            eprintln!("smolvm-pods ctl: {e}\n{USAGE}");
            return EXIT_USAGE;
        }
    };
    let socket = std::env::var(API_SOCKET_ENV).unwrap_or_else(|_| API_SOCKET.to_string());
    let socket = Path::new(&socket);
    let outcome = match call.events_from {
        Some(cursor) => print_events(socket, cursor),
        None => {
            request(socket, call.method, &call.path, call.body.as_ref()).map(|(status, body)| {
                println!("{}", String::from_utf8_lossy(&body).trim_end());
                status
            })
        }
    };
    match outcome {
        Ok(status) if (200..300).contains(&status) => 0,
        Ok(_) => EXIT_ERROR,
        Err(Unreachable(e)) => {
            eprintln!("smolvm-pods ctl: the node service's API: {e}");
            EXIT_UNREACHABLE
        }
    }
}

/// Prints every event after `cursor`, a page at a time. Returns the status
/// of the last answer.
fn print_events(socket: &Path, mut cursor: u64) -> Result<u16, Unreachable> {
    loop {
        let path = format!("/v1/events?cursor={cursor}&limit={EVENTS_LIMIT}");
        let (status, body) = request(socket, "GET", &path, None)?;
        if !(200..300).contains(&status) {
            println!("{}", String::from_utf8_lossy(&body).trim_end());
            return Ok(status);
        }
        let page: Value = serde_json::from_slice(&body)
            .map_err(|e| Unreachable(format!("an answer that is not JSON: {e}")))?;
        let events = page["events"].as_array().cloned().unwrap_or_default();
        for event in &events {
            println!("{event}");
        }
        let next = page["cursor"].as_u64().unwrap_or(cursor);
        if events.len() < EVENTS_LIMIT || next <= cursor {
            return Ok(status);
        }
        cursor = next;
    }
}

#[cfg(test)]
mod tests;
