use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::lifecycle::PodState;
use crate::node::MockBackends;
use crate::proto::Op;

fn args(line: &str) -> Vec<String> {
    line.split(' ').map(str::to_string).collect()
}

#[test]
fn the_ctl_command_lines_parse() {
    let wake = parse(&args("wake 5f3c --reason api --source e2e")).unwrap();
    assert_eq!(
        (wake.method, wake.path.as_str()),
        ("POST", "/v1/pods/5f3c/wake")
    );
    let body = wake.body.unwrap();
    assert_eq!(body["reason"], "api");
    assert_eq!(body["source"], "e2e");
    assert!(body["lease_ttl"].is_null());

    let hold = parse(&args("hold 5f3c --lease work --ttl 30 --holder e2e")).unwrap();
    assert_eq!(
        (hold.method, hold.path.as_str()),
        ("PUT", "/v1/pods/5f3c/leases/work")
    );
    assert_eq!(hold.body.unwrap()["ttl_secs"], 30.0);
    let release = parse(&args("release 5f3c --lease a/b")).unwrap();
    assert_eq!(release.path, "/v1/pods/5f3c/leases/a%2Fb");
    assert_eq!(
        parse(&args("events --cursor 7")).unwrap().events_from,
        Some(7)
    );
    assert_eq!(parse(&args("events")).unwrap().events_from, Some(0));
    assert_eq!(parse(&args("list")).unwrap().path, "/v1/pods");
    assert_eq!(
        parse(&args("get ip:10.42.0.9")).unwrap().path,
        "/v1/pods/ip%3A10.42.0.9"
    );
    assert_eq!(
        parse(&args("get ns/web")).unwrap().path,
        "/v1/pods/ns%2Fweb"
    );
    assert_eq!(
        parse(&args("agent 5f3c")).unwrap().path,
        "/v1/pods/5f3c/agent"
    );

    for wrong in [
        "",
        "sleep 5f3c",
        "get",
        "get a b",
        "hold 5f3c --ttl 30",
        "hold 5f3c --lease work",
        "wake 5f3c --reason dozing",
        "wake 5f3c --source",
        "hibernate 5f3c --lease-ttl 3",
        "events --cursor x",
    ] {
        let line: Vec<String> = wrong
            .split(' ')
            .filter(|w| !w.is_empty())
            .map(String::from)
            .collect();
        assert!(parse(&line).is_err(), "{wrong:?}");
    }
}

#[test]
fn request_bodies_parse_from_json() {
    let wake: WakeBody = serde_json::from_str(
        r#"{"reason":{"kind":"aws","queue":"orders"},"lease_ttl":10,"source":"floci"}"#,
    )
    .unwrap();
    assert_eq!(
        wake.reason.and_then(|r| r.reason().ok()),
        Some(Reason::Aws {
            queue: "orders".into()
        })
    );
    assert_eq!(
        (wake.lease_ttl, wake.source.as_deref()),
        (Some(10.0), Some("floci"))
    );
    let sleep: HibernateBody = serde_json::from_str(r#"{"reason":{"kind":"idle"}}"#).unwrap();
    assert_eq!(
        sleep.reason.and_then(|r| r.reason().ok()),
        Some(Reason::Idle)
    );
    let hold: HoldBody = serde_json::from_str(r#"{"holder":"worker"}"#).unwrap();
    assert_eq!(
        (hold.ttl_secs, hold.holder.as_deref()),
        (None, Some("worker"))
    );
}

#[test]
fn answers_parse_with_a_length_chunked_or_cut_off() {
    let whole = b"HTTP/1.1 409 Conflict\r\ncontent-length: 2\r\n\r\n{}";
    assert_eq!(parse_response(whole).unwrap(), (409, b"{}".to_vec()));
    let chunked =
        b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n";
    assert_eq!(parse_response(chunked).unwrap(), (200, b"abcde".to_vec()));
    let cut = b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\n{}";
    assert!(parse_response(cut).is_err());
    assert!(parse_response(b"").is_err());
}

#[test]
fn refusals_have_the_contracts_reasons() {
    let blockers = [
        Blocker::Exec {
            container: "c".into(),
            exec: "e".into(),
            status: crate::lifecycle::ExecStatus::Running,
        },
        Blocker::ExecStarting,
        Blocker::Lease {
            id: "work".into(),
            holder: None,
            expires_ms: 1,
        },
        Blocker::Disabled,
        Blocker::ContainerCreating,
        Blocker::WakeWaiter { until_ms: 0 },
    ];
    assert_eq!(
        reasons(&blockers),
        [
            "exec running",
            "lease held",
            "hibernation disabled",
            "container being created",
            "wake waiter"
        ]
    );
    let error = ApiError::of(LifecycleError::Cancelled, true);
    assert_eq!(
        (error.status, error.code),
        (StatusCode::CONFLICT, "cancelled")
    );
    let error = ApiError::of(LifecycleError::Failed("gone".into()), true);
    assert_eq!(
        (error.code, error.reasons),
        ("refused", vec!["state failed".into()])
    );
    let error = ApiError::of(LifecycleError::NotFound("x".into()), false);
    assert_eq!(
        (error.status, error.code),
        (StatusCode::NOT_FOUND, "not_found")
    );
}

#[test]
fn a_refused_pause_is_refused_with_the_hooks_reason() {
    let refusal = LifecycleError::PauseRefused("traffic since the idle decision".into());
    let error = ApiError::of(refusal.clone(), true);
    assert_eq!(
        (error.status, error.code, error.reasons),
        (
            StatusCode::CONFLICT,
            "refused",
            vec!["traffic since the idle decision".to_string()]
        )
    );
    assert_eq!(error.message, "pausing: traffic since the idle decision");
    assert_eq!(ApiError::of(refusal, false).code, "vm_error");
    let failed = LifecycleError::Vm("smolvm reports no running VM process for m".into());
    let error = ApiError::of(failed, true);
    assert_eq!(
        (error.status, error.code),
        (StatusCode::INTERNAL_SERVER_ERROR, "vm_error")
    );
    let admission = LifecycleError::Refused(vec![Blocker::Admission {
        reason: "awake pods 2/2".into(),
    }]);
    let error = ApiError::of(admission, false);
    assert_eq!(
        (error.code, error.reasons),
        ("refused", vec!["wake refused".into()])
    );
    assert!(
        error.message.contains("awake pods 2/2"),
        "{}",
        error.message
    );
}

/// Calls the API at `socket` off the async runtime.
async fn call(
    socket: &Path,
    method: &'static str,
    path: &str,
    body: Option<Value>,
) -> (u16, Value) {
    let (socket, path) = (socket.to_path_buf(), path.to_string());
    tokio::task::spawn_blocking(move || {
        let (status, body) = request(&socket, method, &path, body.as_ref()).unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    })
    .await
    .unwrap()
}

/// A node service with mock pods.
async fn mock_node(dir: &Path) -> Node {
    let backends = Arc::new(MockBackends::default());
    Node::open(backends, dir.join("state"), Duration::from_secs(300))
        .await
        .unwrap()
}

/// Creates the mock pod sandbox `id` of the pod `ns/NAME` with uid `uid`.
async fn create_pod(node: &Node, dir: &Path, id: &str, name: &str, uid: &str) {
    let bundle = dir.join(format!("bundle-{id}"));
    std::fs::create_dir_all(&bundle).unwrap();
    let spec = json!({"annotations": {
        "io.kubernetes.cri.container-type": "sandbox",
        "io.kubernetes.cri.sandbox-namespace": "ns",
        "io.kubernetes.cri.sandbox-name": name,
        "io.kubernetes.cri.sandbox-uid": uid,
    }});
    std::fs::write(bundle.join("config.json"), spec.to_string()).unwrap();
    let create = Op::CreateSandbox {
        pod: id.into(),
        bundle: bundle.display().to_string(),
        netns: None,
    };
    node.handle(Some("k8s.io"), create).await.unwrap();
}

/// The API of a node service with one mock pod, as ctl sees it: the shapes
/// that the end-to-end tests read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_api_answers_in_the_contracts_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let node = mock_node(dir.path()).await;
    create_pod(&node, dir.path(), "sb", "arm64", "u-1").await;

    let socket = dir.path().join("api.sock");
    tokio::spawn(serve(node.clone(), listen(&socket).unwrap()));

    let (status, pod) = call(&socket, "GET", "/v1/pods/sb", None).await;
    assert_eq!(status, 200, "{pod}");
    assert_eq!(pod["sandbox_id"], "sb");
    assert_eq!(
        (pod["namespace"].as_str(), pod["name"].as_str()),
        (Some("ns"), Some("arm64"))
    );
    assert_eq!(pod["state"], "awake");
    assert!(pod["generation"].as_u64().is_some() && pod["vm_pid"].as_u64().is_some());
    assert_eq!(pod["rosetta"], false);
    let (_, pods) = call(&socket, "GET", "/v1/pods", None).await;
    assert_eq!(pods["pods"].as_array().unwrap().len(), 1);

    let (status, missing) = call(&socket, "GET", "/v1/pods/other", None).await;
    assert_eq!(
        (status, missing["error"]["code"].as_str()),
        (404, Some("not_found"))
    );

    let hold = json!({"ttl_secs": 30, "holder": "e2e"});
    let (status, held) = call(&socket, "PUT", "/v1/pods/sb/leases/work", Some(hold)).await;
    assert_eq!(status, 200, "{held}");
    assert_eq!(held["pod"]["leases"][0]["id"], "work");
    assert_eq!(held["pod"]["leases"][0]["kind"], "api");
    let (status, refused) = call(
        &socket,
        "POST",
        "/v1/pods/sb/hibernate",
        Some(json!({"reason": "api"})),
    )
    .await;
    assert_eq!(status, 409, "{refused}");
    assert_eq!(refused["error"]["code"], "refused");
    assert_eq!(refused["error"]["reasons"], json!(["lease held"]));
    let (status, released) = call(&socket, "DELETE", "/v1/pods/sb/leases/work", None).await;
    assert_eq!(status, 200, "{released}");
    assert_eq!(released["pod"]["leases"], json!([]));

    let wake = json!({"reason": "api", "source": "e2e"});
    let (status, woke) = call(&socket, "POST", "/v1/pods/sb/wake", Some(wake)).await;
    assert_eq!(status, 200, "{woke}");
    assert_eq!(
        (woke["pod"]["state"].as_str(), woke["resumed"].as_bool()),
        (Some("awake"), Some(false))
    );
    assert!(woke["duration_ms"].as_f64().is_some());
    let (status, bad) = call(
        &socket,
        "POST",
        "/v1/pods/sb/wake",
        Some(json!({"reason": "dozing"})),
    )
    .await;
    assert_eq!(
        (status, bad["error"]["code"].as_str()),
        (400, Some("invalid"))
    );

    let (status, page) = call(&socket, "GET", "/v1/events?cursor=0", None).await;
    assert_eq!(status, 200, "{page}");
    let events = page["events"].as_array().unwrap();
    let states: Vec<(&str, &str)> = events
        .iter()
        .map(|e| (e["from"].as_str().unwrap(), e["to"].as_str().unwrap()))
        .collect();
    assert_eq!(states, [("none", "creating"), ("creating", "awake")]);
    for event in events {
        assert!(event["seq"].as_u64().is_some() && event["time"].as_str().is_some());
        assert_eq!(event["pod"], "sb");
        assert_eq!(event["reason"], "api");
    }
    assert_eq!(page["cursor"], events.last().unwrap()["seq"]);
    node.shutdown();
}

// ------------------------- token, TCP listener, lookups and followed events

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

/// A mock node service with its API on a Unix socket and on TCP, with
/// [`TOKEN`].
async fn served(dir: &Path) -> (Node, PathBuf, SocketAddr) {
    let node = mock_node(dir).await;
    let socket = dir.join("api.sock");
    tokio::spawn(serve(node.clone(), listen(&socket).unwrap()));
    let listener = listen_tcp("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let token = Token::new(TOKEN).unwrap();
    tokio::spawn(serve_tcp(node.clone(), listener, token));
    (node, socket, addr)
}

/// One request on the TCP listener, with `authorization` as the header.
/// Returns the status, the headers and the body.
async fn tcp_raw(
    addr: SocketAddr,
    method: &str,
    path: &str,
    authorization: Option<&str>,
    body: Option<Value>,
) -> (u16, String, Vec<u8>) {
    let auth = authorization.map_or(String::new(), |a| format!("Authorization: {a}\r\n"));
    let body = body.map(|b| b.to_string()).unwrap_or_default();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: node\r\nConnection: close\r\n{auth}\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    tokio::task::spawn_blocking(move || {
        let mut stream = std::net::TcpStream::connect(addr).unwrap();
        stream.write_all(request.as_bytes()).unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).unwrap();
        let end = response.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
        let head = String::from_utf8_lossy(&response[..end]).to_ascii_lowercase();
        let (status, body) = parse_response(&response).unwrap();
        (status, head, body)
    })
    .await
    .unwrap()
}

/// A request on the TCP listener with the token.
async fn tcp_call(addr: SocketAddr, method: &str, path: &str, body: Option<Value>) -> (u16, Value) {
    let auth = format!("Bearer {TOKEN}");
    let (status, _, body) = tcp_raw(addr, method, path, Some(&auth), body).await;
    (status, serde_json::from_slice(&body).unwrap())
}

#[test]
fn the_token_file_must_be_private_and_hold_a_long_token() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("api-token");
    let write = |text: &str, mode: u32| {
        std::fs::write(&path, text).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
    };
    write(&format!("{TOKEN}\n"), 0o600);
    let token = Token::read(&path).unwrap();
    assert!(token.is(TOKEN));
    assert!(!token.is(&TOKEN[1..]) && !token.is(&TOKEN.to_uppercase()) && !token.is(""));
    for (text, mode, want) in [
        (TOKEN, 0o644, "mode 0644"),
        (TOKEN, 0o620, "mode 0620"),
        ("", 0o600, "at least 16"),
        ("short", 0o600, "at least 16"),
        ("0123456789 abcdef0123", 0o600, "visible ASCII"),
    ] {
        write(text, mode);
        let e = Token::read(&path).err().unwrap();
        assert!(e.contains(want), "{text:?} {mode:o}: {e}");
    }
    let e = Token::read(&dir.path().join("missing")).err().unwrap();
    assert!(e.contains("open the API token file"), "{e}");
}

#[test]
fn bearer_takes_the_token_of_the_bearer_scheme() {
    assert_eq!(bearer("Bearer abc"), Some("abc"));
    assert_eq!(bearer("bearer   abc"), Some("abc"));
    assert_eq!(bearer("BEARER abc "), Some("abc"));
    assert_eq!(bearer("Basic abc"), None);
    assert_eq!(bearer("Bearer"), None);
    assert_eq!(bearer("abc"), None);
}

#[test]
fn path_ids_name_pods_by_uid_ip_netns_or_name() {
    let filter = |uid: Option<&str>,
                  ip: Option<&str>,
                  netns: Option<&str>,
                  ns_name: Option<(&str, &str)>| PodFilter {
        uid: uid.map(String::from),
        ip: ip.map(String::from),
        netns: netns.map(String::from),
        namespace: ns_name.map(|(ns, _)| ns.to_string()),
        name: ns_name.map(|(_, name)| name.to_string()),
    };
    assert_eq!(PodFilter::of("5f3c"), None);
    assert_eq!(
        PodFilter::of("uid:u-1"),
        Some(filter(Some("u-1"), None, None, None))
    );
    assert_eq!(
        PodFilter::of("ip:10.42.0.9"),
        Some(filter(None, Some("10.42.0.9"), None, None))
    );
    assert_eq!(
        PodFilter::of("netns:/var/run/netns/cni-1"),
        Some(filter(None, None, Some("/var/run/netns/cni-1"), None))
    );
    assert_eq!(
        PodFilter::of("ns/web"),
        Some(filter(None, None, None, Some(("ns", "web"))))
    );
}

/// Over TCP, every route needs the bearer token; the Unix socket needs none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_tcp_listener_needs_the_token() {
    let dir = tempfile::tempdir().unwrap();
    let (node, socket, addr) = served(dir.path()).await;
    create_pod(&node, dir.path(), "sb", "web", "u-1").await;
    let wrong = format!("Bearer {}", TOKEN.replace('0', "1"));
    for (auth, path) in [
        (None, "/v1/pods"),
        (Some(wrong.as_str()), "/v1/pods"),
        (Some("Bearer "), "/v1/pods"),
        (Some(&format!("Basic {TOKEN}")), "/v1/pods"),
        (None, "/v1/pods/sb/hibernate"),
        (None, "/no/such/route"),
    ] {
        let (status, head, body) = tcp_raw(addr, "GET", path, auth, None).await;
        assert_eq!(status, 401, "{auth:?} {path}");
        assert!(head.contains("www-authenticate: bearer"), "{head}");
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["code"], "unauthorized", "{body}");
    }
    let (status, _, _) = tcp_raw(addr, "POST", "/v1/pods/sb/wake", None, Some(json!({}))).await;
    assert_eq!(status, 401);
    let (status, pods) = tcp_call(addr, "GET", "/v1/pods", None).await;
    assert_eq!(status, 200, "{pods}");
    assert_eq!(pods["pods"][0]["sandbox_id"], "sb");
    let lower = format!("bearer {TOKEN}");
    let (status, _, _) = tcp_raw(addr, "GET", "/v1/pods/sb", Some(&lower), None).await;
    assert_eq!(status, 200);
    let (status, pod) = call(&socket, "GET", "/v1/pods/sb", None).await;
    assert_eq!((status, pod["name"].as_str()), (200, Some("web")));
    node.shutdown();
}

/// The pod lookups, and wake with a lease, renew and release through them,
/// over TCP.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pods_are_found_by_ip_uid_and_name() {
    let dir = tempfile::tempdir().unwrap();
    let (node, _socket, addr) = served(dir.path()).await;
    create_pod(&node, dir.path(), "sb", "web", "u-1").await;
    create_pod(&node, dir.path(), "sc", "other", "u-2").await;
    let lifecycle = node.lifecycle();
    for (id, ip) in [("sb", "10.42.0.9"), ("sc", "10.42.0.10")] {
        lifecycle
            .set_network(id, Some(ip.into()), Some("c2:3c:2a:92:39:90".into()))
            .unwrap();
    }
    let ids = |pods: &Value| -> Vec<String> {
        pods["pods"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["sandbox_id"].as_str().unwrap().to_string())
            .collect()
    };
    for (query, want) in [
        ("?ip=10.42.0.9", vec!["sb"]),
        ("?ip=10.42.0.1", vec![]),
        ("?namespace=ns", vec!["sb", "sc"]),
        ("?namespace=ns&name=other", vec!["sc"]),
        ("?uid=u-1&ip=10.42.0.10", vec![]),
        ("", vec!["sb", "sc"]),
    ] {
        let (status, pods) = tcp_call(addr, "GET", &format!("/v1/pods{query}"), None).await;
        assert_eq!(
            (status, ids(&pods)),
            (200, want.iter().map(|s| s.to_string()).collect()),
            "{query}"
        );
    }
    let (status, bad) = tcp_call(addr, "GET", "/v1/pods?address=10.42.0.9", None).await;
    assert_eq!(
        (status, bad["error"]["code"].as_str()),
        (400, Some("invalid"))
    );

    for (path, want) in [
        ("ip:10.42.0.9", "sb"),
        ("ip%3A10.42.0.9", "sb"),
        ("uid:u-2", "sc"),
        ("ns%2Fweb", "sb"),
        ("sc", "sc"),
    ] {
        let (status, pod) = tcp_call(addr, "GET", &format!("/v1/pods/{path}"), None).await;
        assert_eq!(
            (status, pod["sandbox_id"].as_str()),
            (200, Some(want)),
            "{path}"
        );
    }
    let (status, missing) = tcp_call(addr, "GET", "/v1/pods/ip:10.9.9.9", None).await;
    assert_eq!(
        (status, missing["error"]["code"].as_str()),
        (404, Some("not_found"))
    );
    assert_eq!(missing["error"]["message"], "no pod ip:10.9.9.9");

    let wake = json!({"reason": "api", "source": "floci", "lease_ttl": 40});
    let (status, woke) = tcp_call(addr, "POST", "/v1/pods/ip:10.42.0.9/wake", Some(wake)).await;
    assert_eq!(status, 200, "{woke}");
    assert_eq!(woke["pod"]["state"], "awake");
    let lease = woke["lease"]["id"].as_str().unwrap().to_string();
    assert_eq!(woke["pod"]["leases"][0]["id"], lease.as_str());
    let expires = woke["lease"]["expires_at"].as_str().unwrap().to_string();

    std::thread::sleep(Duration::from_millis(5));
    let path = format!("/v1/pods/uid:u-1/leases/{lease}");
    let (status, renewed) = tcp_call(addr, "PATCH", &path, Some(json!({"ttl_secs": 50}))).await;
    assert_eq!(status, 200, "{renewed}");
    assert_eq!(renewed["lease"]["id"], lease.as_str());
    assert!(renewed["lease"]["expires_at"].as_str().unwrap() > expires.as_str());
    let (status, gone) = tcp_call(
        addr,
        "PATCH",
        "/v1/pods/sb/leases/unknown",
        Some(json!({"ttl_secs": 5})),
    )
    .await;
    assert_eq!(
        (status, gone["error"]["code"].as_str()),
        (404, Some("lease_not_found"))
    );

    let (status, released) = tcp_call(addr, "DELETE", &path, None).await;
    assert_eq!(status, 200, "{released}");
    assert_eq!(released["pod"]["leases"], json!([]));
    let (status, _) = tcp_call(addr, "PATCH", &path, Some(json!({"ttl_secs": 5}))).await;
    assert_eq!(status, 404);
    let hold = json!({"ttl_secs": 30});
    let (status, held) =
        tcp_call(addr, "PUT", "/v1/pods/ns%2Fother/leases/floci", Some(hold)).await;
    assert_eq!(
        (status, held["pod"]["sandbox_id"].as_str()),
        (200, Some("sc"))
    );
    node.shutdown();
}

/// A uid or a name can match a failed sandbox of the pod beside its new one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lookup_prefers_a_live_pod_then_the_newest() {
    let dir = tempfile::tempdir().unwrap();
    let node = mock_node(dir.path()).await;
    create_pod(&node, dir.path(), "sb", "web", "u-1").await;
    let pod = node.lifecycle().get(&PodRef::sandbox("sb")).unwrap();
    node.shutdown();
    let at = |id: &str, state: PodState, created_ms: u64| {
        let mut pod = pod.clone();
        pod.identity.sandbox_id = id.to_string();
        pod.state = state;
        pod.created_ms = created_ms;
        pod
    };
    let by_uid = PodFilter::of("uid:u-1").unwrap();
    let chosen = |pods: Vec<PodInfo>| choose(&by_uid, pods).map(|p| p.identity.sandbox_id);
    assert_eq!(
        chosen(vec![
            at("old", PodState::Asleep, 1),
            at("failed", PodState::Failed, 2)
        ]),
        Some("old".into())
    );
    assert_eq!(
        chosen(vec![
            at("old", PodState::Awake, 1),
            at("new", PodState::Creating, 2)
        ]),
        Some("new".into())
    );
    assert_eq!(
        chosen(vec![at("failed", PodState::Failed, 1)]),
        Some("failed".into())
    );
    let other = PodFilter::of("uid:u-2").unwrap();
    assert!(choose(&other, vec![at("old", PodState::Awake, 1)]).is_none());
}

/// Reads an answer from `stream` until its chunked body has `n` lines, and
/// returns its head, lower case, and the lines.
fn read_lines(
    stream: &mut std::net::TcpStream,
    n: usize,
    data: &mut Vec<u8>,
) -> (String, Vec<String>) {
    let mut buf = [0u8; 4096];
    loop {
        if let Some(end) = data.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&data[..end]).to_ascii_lowercase();
            let body = complete_chunks(&data[end + 4..]);
            let text = String::from_utf8(body).unwrap();
            let lines: Vec<String> = text
                .split_inclusive('\n')
                .filter(|l| l.ends_with('\n'))
                .map(|l| l.trim_end().to_string())
                .collect();
            if lines.len() >= n {
                return (head, lines);
            }
        }
        let got = stream.read(&mut buf).unwrap();
        assert!(got > 0, "the stream ended");
        data.extend_from_slice(&buf[..got]);
    }
}

/// The data of the chunks that have arrived whole.
fn complete_chunks(mut data: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    while let Some(line_end) = data.windows(2).position(|w| w == b"\r\n") {
        let size =
            usize::from_str_radix(std::str::from_utf8(&data[..line_end]).unwrap(), 16).unwrap();
        let rest = &data[line_end + 2..];
        if size == 0 || rest.len() < size + 2 {
            break;
        }
        body.extend_from_slice(&rest[..size]);
        data = &rest[size + 2..];
    }
    body
}

/// A follow over TCP gets the events after its cursor, then new ones as they
/// happen, one JSON object per line.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_follow_streams_the_events_as_json_lines() {
    let dir = tempfile::tempdir().unwrap();
    let (node, _socket, addr) = served(dir.path()).await;
    create_pod(&node, dir.path(), "sb", "web", "u-1").await;
    let request = format!(
        "GET /v1/events?follow=true&cursor=1 HTTP/1.1\r\nHost: node\r\n\
         Authorization: Bearer {TOKEN}\r\n\r\n"
    );
    let mut stream = std::net::TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    let mut data = Vec::new();
    let (mut stream, data, (head, lines)) = tokio::task::spawn_blocking(move || {
        let read = read_lines(&mut stream, 1, &mut data);
        (stream, data, read)
    })
    .await
    .unwrap();
    assert!(head.starts_with("http/1.1 200"), "{head}");
    assert!(
        head.contains("content-type: application/x-ndjson"),
        "{head}"
    );
    assert!(head.contains("transfer-encoding: chunked"), "{head}");
    let event: Value = serde_json::from_str(&lines[0]).unwrap();
    assert_eq!(
        (
            event["seq"].as_u64(),
            event["from"].as_str(),
            event["to"].as_str()
        ),
        (Some(2), Some("creating"), Some("awake"))
    );

    create_pod(&node, dir.path(), "sc", "other", "u-2").await;
    let mut data = data;
    let (_, lines) = tokio::task::spawn_blocking(move || read_lines(&mut stream, 3, &mut data))
        .await
        .unwrap();
    let events: Vec<(u64, String, String)> = lines
        .iter()
        .map(|l| {
            let e: Value = serde_json::from_str(l).unwrap();
            (
                e["seq"].as_u64().unwrap(),
                e["pod"].as_str().unwrap().to_string(),
                e["to"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        events,
        [
            (2, "sb".into(), "awake".into()),
            (3, "sc".into(), "creating".into()),
            (4, "sc".into(), "awake".into())
        ]
    );
    node.shutdown();
}

/// An event log for a [`Follower`], with the store's paging, which counts
/// its reads.
#[derive(Clone, Default)]
struct FakeLog {
    events: Arc<std::sync::Mutex<Vec<Event>>>,
    reads: Arc<std::sync::atomic::AtomicUsize>,
}

impl FakeLog {
    /// Appends an event of `pod`, and sends it on `sender` if given.
    fn append(&self, pod: &str, sender: Option<&broadcast::Sender<Event>>) -> Event {
        let mut events = self.events.lock().unwrap();
        let seq = events.len() as u64 + 1;
        let event: Event = serde_json::from_value(json!({
            "seq": seq, "time_ms": 1, "sandbox_id": pod, "incarnation": "i",
            "from": "awake", "state": "pausing", "generation": seq, "reason": {"kind": "idle"},
        }))
        .unwrap();
        events.push(event.clone());
        if let Some(sender) = sender {
            let _ = sender.send(event.clone());
        }
        event
    }

    fn reads(&self) -> usize {
        self.reads.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn reader(&self) -> ReadLog {
        let log = self.clone();
        Arc::new(move |query: &EventQuery| {
            log.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let events = log.events.lock().unwrap();
            let page: Vec<Event> = events
                .iter()
                .filter(|e| e.seq > query.after)
                .filter(|e| query.sandbox_id.as_ref().is_none_or(|p| *p == e.sandbox_id))
                .take(query.limit)
                .cloned()
                .collect();
            let cursor = match page.last() {
                Some(e) if page.len() >= query.limit => e.seq,
                _ => events.last().map_or(0, |e| e.seq).max(query.after),
            };
            Ok(EventPage {
                events: page,
                cursor,
                gap: false,
            })
        })
    }
}

/// The sequence numbers of the next `n` lines of a follower.
async fn next_seqs(follower: &mut Follower, n: usize) -> Vec<u64> {
    let mut seqs = Vec::new();
    for _ in 0..n {
        let line = tokio::time::timeout(Duration::from_secs(5), follower.next())
            .await
            .expect("a line within 5 s")
            .expect("the stream goes on");
        let event: Value = serde_json::from_slice(&line).unwrap();
        seqs.push(event["seq"].as_u64().unwrap());
    }
    seqs
}

/// Whether a follower has nothing more to send now.
async fn quiet(follower: &mut Follower) -> bool {
    follower.keepalive = Duration::from_millis(50);
    let line = follower.next().await;
    follower.keepalive = FOLLOW_KEEPALIVE;
    line.as_deref() == Some(b"\n")
}

#[tokio::test]
async fn a_follower_reads_the_log_then_the_broadcast() {
    let log = FakeLog::default();
    let (sender, _) = broadcast::channel(16);
    for pod in ["a", "b", "a"] {
        log.append(pod, None);
    }
    let mut follower = Follower::new(sender.subscribe(), log.reader(), 0, None);
    let mut from_two = Follower::new(sender.subscribe(), log.reader(), 2, None);
    let mut of_a = Follower::new(sender.subscribe(), log.reader(), 0, Some("a".into()));
    assert_eq!(next_seqs(&mut follower, 3).await, [1, 2, 3]);
    assert_eq!(next_seqs(&mut from_two, 1).await, [3]);
    assert_eq!(next_seqs(&mut of_a, 2).await, [1, 3]);
    assert!(
        quiet(&mut follower).await,
        "a blank line when nothing happens"
    );
    for pod in ["b", "a"] {
        log.append(pod, Some(&sender));
    }
    assert_eq!(next_seqs(&mut follower, 2).await, [4, 5]);
    assert_eq!(next_seqs(&mut from_two, 2).await, [4, 5]);
    assert_eq!(next_seqs(&mut of_a, 1).await, [5]);
    assert!(quiet(&mut follower).await && quiet(&mut of_a).await);
    assert_eq!(log.reads(), 3, "each read the log once");
    drop(sender);
    assert_eq!(follower.next().await, None);
}

/// A follower that lags behind the broadcast, or sees its events out of
/// order, reads the log again from its cursor, and sends each event once.
#[tokio::test]
async fn a_lagging_follower_reads_the_log_again_from_its_cursor() {
    let log = FakeLog::default();
    let (sender, _) = broadcast::channel(2);
    let mut follower = Follower::new(sender.subscribe(), log.reader(), 0, None);
    assert!(quiet(&mut follower).await);
    for _ in 0..6 {
        log.append("a", Some(&sender));
    }
    assert_eq!(next_seqs(&mut follower, 6).await, [1, 2, 3, 4, 5, 6]);
    assert!(
        quiet(&mut follower).await,
        "the two kept in the channel are not sent again"
    );
    assert_eq!(log.reads(), 2);

    let seven = log.append("a", None);
    let eight = log.append("b", None);
    sender.send(eight).unwrap();
    sender.send(seven).unwrap();
    assert_eq!(next_seqs(&mut follower, 2).await, [7, 8]);
    log.append("a", Some(&sender));
    assert_eq!(next_seqs(&mut follower, 1).await, [9]);
    assert!(quiet(&mut follower).await);
    assert_eq!(log.reads(), 3);
}

#[tokio::test]
async fn a_long_log_is_read_a_page_at_a_time() {
    let log = FakeLog::default();
    let (sender, _) = broadcast::channel::<Event>(16);
    for _ in 0..(2 * EVENTS_LIMIT + 10) {
        log.append("a", None);
    }
    let mut follower = Follower::new(sender.subscribe(), log.reader(), 5, None);
    let seqs = next_seqs(&mut follower, 2 * EVENTS_LIMIT + 5).await;
    let want: Vec<u64> = (6..=(2 * EVENTS_LIMIT as u64 + 10)).collect();
    assert_eq!(seqs, want);
    assert!(quiet(&mut follower).await);
    assert_eq!(log.reads(), 3);
}
