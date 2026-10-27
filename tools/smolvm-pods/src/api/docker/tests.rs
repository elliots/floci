use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpStream as StdTcpStream};
use std::path::Path;
use std::time::Duration;

use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixListener};

use super::*;
use crate::api::{listen_tcp, serve_tcp, Token};
use crate::node::{MockBackends, Node};

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

/// The fake dockerd's network: the loopback addresses but the gateway.
const SUBNET: &str = "127.0.0.0/8";
const GATEWAY: &str = "127.0.0.1";

/// Where the fake TCP target listens, on the fake network.
const TARGET_IP: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 3);

#[test]
fn a_subnet_allows_its_host_addresses_but_the_gateway() {
    let subnet = Subnet::parse("172.17.0.0/16", Some("172.17.0.1")).unwrap();
    for (ip, allowed) in [
        ("172.17.0.2", true),
        ("172.17.255.254", true),
        ("172.17.0.1", false),
        ("172.17.0.0", false),
        ("172.17.255.255", false),
        ("172.18.0.2", false),
        ("10.0.0.2", false),
        ("fd00::2", false),
    ] {
        assert_eq!(subnet.allows(ip.parse().unwrap()), allowed, "{ip}");
    }
    let v6 = Subnet::parse("fd00:1::/64", Some("fd00:1::1")).unwrap();
    assert!(v6.allows("fd00:1::5".parse().unwrap()));
    assert!(!v6.allows("fd00:1::1".parse().unwrap()));
    assert!(!v6.allows("fd00:2::5".parse().unwrap()));
    assert!(!v6.allows("172.17.0.2".parse().unwrap()));
    let point = Subnet::parse("10.9.0.7/32", None).unwrap();
    assert!(point.allows("10.9.0.7".parse().unwrap()));
    assert!(!point.allows("10.9.0.8".parse().unwrap()));
    for wrong in ["172.17.0.0", "172.17.0.0/33", "x/8", "fd00::/129"] {
        assert_eq!(Subnet::parse(wrong, None), None, "{wrong}");
    }
}

#[test]
fn the_subnets_come_from_the_networks_ipam() {
    let networks = json!([
        {"Name": "bridge", "IPAM": {"Config": [{"Subnet": "172.17.0.0/16", "Gateway": "172.17.0.1"}]}},
        {"Name": "host", "IPAM": {"Config": []}},
        {"Name": "none", "IPAM": {"Config": null}},
        {"Name": "app", "IPAM": {"Config": [
            {"Subnet": "172.20.0.0/24"},
            {"Subnet": "fd00:1::/64", "Gateway": "fd00:1::1"},
            {"Subnet": "broken"}
        ]}}
    ]);
    let subnets = subnets_of(&networks);
    assert_eq!(subnets.len(), 3, "{subnets:?}");
    assert_eq!(subnets[0].gateway, Some("172.17.0.1".parse().unwrap()));
    assert_eq!(subnets[1].prefix, 24);
    assert_eq!(subnets[1].gateway, None);
    assert!(subnets[2].network.is_ipv6());
}

#[test]
fn a_tunnel_names_one_address_and_port() {
    assert_eq!(
        tunnel_target(Some("to=172.17.0.2:5432")).unwrap(),
        "172.17.0.2:5432".parse::<SocketAddr>().unwrap()
    );
    assert_eq!(
        tunnel_target(Some("to=172.17.0.2%3A5432")).unwrap(),
        "172.17.0.2:5432".parse::<SocketAddr>().unwrap()
    );
    assert_eq!(
        tunnel_target(Some("to=%5Bfd00%3A1%3A%3A5%5D%3A3306")).unwrap(),
        "[fd00:1::5]:3306".parse::<SocketAddr>().unwrap()
    );
    for wrong in [
        None,
        Some(""),
        Some("to=172.17.0.2"),
        Some("to=db:5432"),
        Some("to=172.17.0.2:0"),
        Some("to=172.17.0.2:5432&to=172.17.0.3:5432"),
        Some("to=172.17.0.2:5432&via=x"),
    ] {
        assert!(tunnel_target(wrong).is_err(), "{wrong:?}");
    }
}

#[test]
fn the_proxy_drops_hop_by_hop_headers() {
    let mut headers = HeaderMap::new();
    headers.insert(
        CONNECTION,
        HeaderValue::from_static("keep-alive, X-Private"),
    );
    headers.insert("x-private", HeaderValue::from_static("1"));
    headers.insert("keep-alive", HeaderValue::from_static("timeout=5"));
    headers.insert("transfer-encoding", HeaderValue::from_static("chunked"));
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    strip_hop_by_hop(&mut headers, false);
    let names: Vec<&str> = headers.keys().map(|k| k.as_str()).collect();
    assert_eq!(names, ["content-type"]);

    let mut upgrade = HeaderMap::new();
    upgrade.insert(CONNECTION, HeaderValue::from_static("Upgrade"));
    upgrade.insert(UPGRADE, HeaderValue::from_static("tcp"));
    assert!(wants_upgrade(&upgrade));
    strip_hop_by_hop(&mut upgrade, true);
    assert_eq!(upgrade[CONNECTION], "Upgrade");
    assert_eq!(upgrade[UPGRADE], "tcp");
    let mut no_upgrade = HeaderMap::new();
    no_upgrade.insert(UPGRADE, HeaderValue::from_static("tcp"));
    assert!(!wants_upgrade(&no_upgrade));
}

/// A request that the fake dockerd read.
struct FakeRequest {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl FakeRequest {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Reads one request, with a body by Content-Length or chunked.
async fn read_request<R: tokio::io::AsyncBufRead + Unpin>(reader: &mut R) -> Option<FakeRequest> {
    let mut line = String::new();
    if reader.read_line(&mut line).await.ok()? == 0 {
        return None;
    }
    let mut words = line.split_whitespace();
    let method = words.next()?.to_string();
    let target = words.next()?.to_string();
    let mut headers = Vec::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).await.ok()?;
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':')?;
        headers.push((name.trim().to_string(), value.trim().to_string()));
    }
    let mut request = FakeRequest {
        method,
        target,
        headers,
        body: Vec::new(),
    };
    if let Some(length) = request.header("content-length") {
        let mut body = vec![0; length.parse().ok()?];
        reader.read_exact(&mut body).await.ok()?;
        request.body = body;
    } else if request
        .header("transfer-encoding")
        .is_some_and(|t| t.eq_ignore_ascii_case("chunked"))
    {
        loop {
            let mut size = String::new();
            reader.read_line(&mut size).await.ok()?;
            let size = usize::from_str_radix(size.trim(), 16).ok()?;
            let mut chunk = vec![0; size + 2];
            reader.read_exact(&mut chunk).await.ok()?;
            if size == 0 {
                break;
            }
            request.body.extend_from_slice(&chunk[..size]);
        }
    }
    Some(request)
}

fn json_response(body: Value) -> Vec<u8> {
    let body = body.to_string();
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

/// A fake dockerd on a Unix socket, which answers as dockerd does: JSON with
/// a length, a hijacked exec stream that ends when the connection closes,
/// and an attach that upgrades the connection and echoes what it reads.
async fn fake_dockerd(socket: &Path) {
    let listener = UnixListener::bind(socket).unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut stream = BufReader::new(stream);
                while let Some(request) = read_request(&mut stream).await {
                    let route = (request.method.as_str(), request.target.as_str());
                    let reply = match route {
                        ("GET", "/networks") => json_response(json!([
                            {"Name": "bridge", "IPAM": {"Config": [{"Subnet": SUBNET, "Gateway": GATEWAY}]}},
                            {"Name": "host", "IPAM": {"Config": []}},
                        ])),
                        ("POST", "/images/load?quiet=1") => {
                            json_response(json!({"bytes": request.body.len()}))
                        }
                        ("POST", "/exec/e1/start") => {
                            let head = "HTTP/1.1 200 OK\r\n\
                                Content-Type: application/vnd.docker.raw-stream\r\n\r\n";
                            stream.write_all(head.as_bytes()).await.unwrap();
                            for part in ["first ", "second ", "third"] {
                                stream.write_all(part.as_bytes()).await.unwrap();
                                stream.flush().await.unwrap();
                                tokio::time::sleep(Duration::from_millis(20)).await;
                            }
                            return;
                        }
                        ("POST", "/containers/c1/attach?stream=1&stdin=1") => {
                            let upgrade = request.header("upgrade") == Some("tcp");
                            let head = "HTTP/1.1 101 UPGRADED\r\n\
                                Content-Type: application/vnd.docker.raw-stream\r\n\
                                Connection: Upgrade\r\nUpgrade: tcp\r\n\r\n";
                            stream.write_all(head.as_bytes()).await.unwrap();
                            stream.flush().await.unwrap();
                            if !upgrade {
                                return;
                            }
                            let mut buffer = [0; 1024];
                            loop {
                                let n = stream.read(&mut buffer).await.unwrap_or(0);
                                if n == 0 {
                                    let _ = stream.write_all(b"bye").await;
                                    return;
                                }
                                let echo = [b"echo:", &buffer[..n]].concat();
                                stream.write_all(&echo).await.unwrap();
                                stream.flush().await.unwrap();
                            }
                        }
                        _ => json_response(json!({
                            "method": request.method,
                            "target": request.target,
                            "host": request.header("host"),
                            "authorization": request.header("authorization"),
                            "x_private": request.header("x-private"),
                        })),
                    };
                    if stream.write_all(&reply).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
}

/// A TCP target on the fake network that sends a greeting first, as a MySQL
/// server does, then echoes in upper case.
async fn fake_target() -> SocketAddr {
    let listener = TcpListener::bind((TARGET_IP, 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                stream.write_all(b"hello\n").await.unwrap();
                let mut buffer = [0; 1024];
                loop {
                    let n = stream.read(&mut buffer).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    let upper = buffer[..n].to_ascii_uppercase();
                    stream.write_all(&upper).await.unwrap();
                }
            });
        }
    });
    addr
}

/// A mock node service whose API serves the fake dockerd on TCP.
async fn served(dir: &Path) -> (Node, SocketAddr) {
    let socket = dir.join("docker.sock");
    fake_dockerd(&socket).await;
    let backends = Arc::new(MockBackends::default());
    let node = Node::open(backends, dir.join("state"), Duration::from_secs(300))
        .await
        .unwrap();
    let listener = listen_tcp("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let token = Token::new(TOKEN).unwrap();
    tokio::spawn(serve_tcp(
        node.clone(),
        listener,
        token,
        Docker::new(socket),
    ));
    (node, addr)
}

/// Sends `request` on a new connection to the API and reads the response
/// head, byte by byte, leaving what follows it in the stream.
fn send(addr: SocketAddr, request: &str) -> (StdTcpStream, u16, String) {
    let mut stream = StdTcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    let mut head = Vec::new();
    let mut byte = [0; 1];
    while !head.ends_with(b"\r\n\r\n") {
        assert_eq!(stream.read(&mut byte).unwrap(), 1, "the head ended early");
        head.push(byte[0]);
    }
    let head = String::from_utf8(head).unwrap().to_ascii_lowercase();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (stream, status, head)
}

/// The rest of a response: its body, de-chunked when it is chunked.
fn body_of(mut stream: StdTcpStream, head: &str) -> Vec<u8> {
    let mut rest = Vec::new();
    stream.read_to_end(&mut rest).unwrap();
    if head.contains("transfer-encoding: chunked") {
        crate::api::tests::dechunk(&rest).unwrap()
    } else {
        rest
    }
}

fn get(addr: SocketAddr, path: &str, extra: &str) -> (u16, String, Vec<u8>) {
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: node\r\nAuthorization: Bearer {TOKEN}\r\n\
         Connection: close\r\n{extra}\r\n"
    );
    let (stream, status, head) = send(addr, &request);
    let body = body_of(stream, &head);
    (status, head, body)
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(f).await.unwrap()
}

/// The proxy passes a call to dockerd without the prefix and the token, and
/// its answer back; the token is needed, as on every TCP route.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_proxy_passes_calls_to_dockerd() {
    let dir = tempfile::tempdir().unwrap();
    let (node, addr) = served(dir.path()).await;
    let (status, head, body) = blocking(move || {
        get(
            addr,
            "/docker/v1.47/containers/json?all=1",
            "Keep-Alive: timeout=5\r\nX-Private: 1\r\nConnection: X-Private\r\n",
        )
    })
    .await;
    assert_eq!(status, 200, "{head}");
    let answer: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(answer["target"], "/v1.47/containers/json?all=1", "{answer}");
    assert_eq!(answer["host"], "docker");
    assert!(
        answer["authorization"].is_null(),
        "the token reached dockerd"
    );
    assert!(
        answer["x_private"].is_null(),
        "a hop-by-hop header reached dockerd"
    );

    let unauthorized = blocking(move || {
        let request = "GET /docker/v1.47/info HTTP/1.1\r\nHost: node\r\nConnection: close\r\n\r\n";
        send(addr, request).1
    })
    .await;
    assert_eq!(unauthorized, 401);

    let (status, _, body) = blocking(move || get(addr, "/docker", "")).await;
    assert_eq!(status, 404, "{}", String::from_utf8_lossy(&body));
    node.shutdown();
}

/// A request body streams to dockerd, as `docker load` sends an image.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_proxy_streams_a_request_body() {
    let dir = tempfile::tempdir().unwrap();
    let (node, addr) = served(dir.path()).await;
    let (status, body) = blocking(move || {
        let image = vec![7u8; 3 * 1024 * 1024 + 11];
        let mut stream = StdTcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let head = format!(
            "POST /docker/images/load?quiet=1 HTTP/1.1\r\nHost: node\r\n\
             Authorization: Bearer {TOKEN}\r\nConnection: close\r\n\
             Content-Type: application/x-tar\r\nTransfer-Encoding: chunked\r\n\r\n"
        );
        stream.write_all(head.as_bytes()).unwrap();
        for chunk in image.chunks(64 * 1024) {
            write!(stream, "{:x}\r\n", chunk.len()).unwrap();
            stream.write_all(chunk).unwrap();
            stream.write_all(b"\r\n").unwrap();
        }
        stream.write_all(b"0\r\n\r\n").unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).unwrap();
        crate::api::tests::parse_response(&response).unwrap()
    })
    .await;
    assert_eq!(status, 200);
    let answer: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(answer["bytes"], 3 * 1024 * 1024 + 11);
    node.shutdown();
}

/// dockerd's hijacked exec stream, which ends when dockerd closes the
/// connection, reaches the client whole, as it is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_proxy_streams_a_hijacked_answer() {
    let dir = tempfile::tempdir().unwrap();
    let (node, addr) = served(dir.path()).await;
    let (status, head, body) = blocking(move || {
        let request = format!(
            "POST /docker/exec/e1/start HTTP/1.1\r\nHost: node\r\n\
             Authorization: Bearer {TOKEN}\r\nConnection: close\r\n\
             Content-Type: application/json\r\nContent-Length: 2\r\n\r\n{{}}"
        );
        let (stream, status, head) = send(addr, &request);
        let body = body_of(stream, &head);
        (status, head, body)
    })
    .await;
    assert_eq!(status, 200, "{head}");
    assert!(head.contains("application/vnd.docker.raw-stream"), "{head}");
    assert_eq!(String::from_utf8(body).unwrap(), "first second third");
    node.shutdown();
}

/// An attach that asks to upgrade gets dockerd's 101, and then the bytes go
/// both ways until the client closes its side.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_proxy_splices_an_upgraded_connection() {
    let dir = tempfile::tempdir().unwrap();
    let (node, addr) = served(dir.path()).await;
    let (status, head, first, rest) = blocking(move || {
        let request = format!(
            "POST /docker/containers/c1/attach?stream=1&stdin=1 HTTP/1.1\r\nHost: node\r\n\
             Authorization: Bearer {TOKEN}\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\
             Content-Length: 0\r\n\r\n"
        );
        let (mut stream, status, head) = send(addr, &request);
        stream.write_all(b"ping").unwrap();
        let mut first = [0; 9];
        stream.read_exact(&mut first).unwrap();
        stream.shutdown(std::net::Shutdown::Write).unwrap();
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).unwrap();
        (status, head, first.to_vec(), rest)
    })
    .await;
    assert_eq!(status, 101, "{head}");
    assert!(head.contains("upgrade: tcp"), "{head}");
    assert_eq!(first, b"echo:ping");
    assert_eq!(rest, b"bye");
    node.shutdown();
}

/// A tunnel reaches an address on the node's Docker networks, with the bytes
/// that the target sends first, and is refused for any other address.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tunnel_reaches_a_container_address() {
    let dir = tempfile::tempdir().unwrap();
    let (node, addr) = served(dir.path()).await;
    let target = fake_target().await;
    let (status, head, greeting, answer) = blocking(move || {
        let request = format!(
            "GET /v1/tunnel?to={target} HTTP/1.1\r\nHost: node\r\n\
             Authorization: Bearer {TOKEN}\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n"
        );
        let (mut stream, status, head) = send(addr, &request);
        let mut greeting = [0; 6];
        stream.read_exact(&mut greeting).unwrap();
        stream.write_all(b"select 1").unwrap();
        let mut answer = [0; 8];
        stream.read_exact(&mut answer).unwrap();
        (status, head, greeting.to_vec(), answer.to_vec())
    })
    .await;
    assert_eq!(status, 101, "{head}");
    assert_eq!(greeting, b"hello\n");
    assert_eq!(answer, b"SELECT 1");

    let port = target.port();
    let upgrade = "Connection: Upgrade\r\nUpgrade: tcp\r\n";
    for (to, extra, want, code) in [
        (format!("{GATEWAY}:{port}"), upgrade, 403, "forbidden"),
        (format!("10.1.2.3:{port}"), upgrade, 403, "forbidden"),
        (format!("{target}"), "", 400, "invalid"),
        ("db:5432".to_string(), upgrade, 400, "invalid"),
        (format!("{TARGET_IP}:1"), upgrade, 502, "unreachable"),
    ] {
        let path = format!("/v1/tunnel?to={to}");
        let (status, _, body) = blocking(move || get(addr, &path, extra)).await;
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            (status, body["error"]["code"].as_str()),
            (want, Some(code)),
            "{to}: {body}"
        );
    }
    let unauthorized = blocking(move || {
        let request = format!(
            "GET /v1/tunnel?to={target} HTTP/1.1\r\nHost: node\r\n{upgrade}Connection: close\r\n\r\n"
        );
        send(addr, &request).1
    })
    .await;
    assert_eq!(unauthorized, 401);
    node.shutdown();
}

/// Without dockerd, the proxy and the tunnels answer 503.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_dockerd_the_routes_are_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let backends = Arc::new(MockBackends::default());
    let node = Node::open(backends, dir.path().join("state"), Duration::from_secs(300))
        .await
        .unwrap();
    let listener = listen_tcp("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let docker = Docker::new(dir.path().join("missing.sock"));
    tokio::spawn(serve_tcp(
        node.clone(),
        listener,
        Token::new(TOKEN).unwrap(),
        docker,
    ));
    for (path, extra) in [
        ("/docker/_ping", ""),
        (
            "/v1/tunnel?to=172.17.0.2:5432",
            "Connection: Upgrade\r\nUpgrade: tcp\r\n",
        ),
    ] {
        let (status, _, body) = blocking(move || get(addr, path, extra)).await;
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            (status, body["error"]["code"].as_str()),
            (503, Some("unavailable")),
            "{path}"
        );
    }
    node.shutdown();
}
