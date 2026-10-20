//! The pre-sleep checks: a pod's TCP, HTTP(S) and gRPC probes, run from the
//! node's root namespace as kubelet runs them, so that they come from
//! kubelet's source address and conntrack leaves them out of the pod's
//! activity.

use std::net::Ipv4Addr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full, Limited};
use hyper::header::{
    HeaderMap, HeaderName, HeaderValue, ACCEPT, CONNECTION, CONTENT_TYPE, HOST, LOCATION, TE,
    USER_AGENT,
};
use hyper::{Request, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_rustls::TlsConnector;
use url::{Position, Url};

use super::ProbeAction;
use crate::http1::{self, ConnectError};

/// The User-Agent of kubelet's HTTP and gRPC probes.
pub const KUBE_PROBE_USER_AGENT: &str = "kube-probe/1.35";
/// Kubelet makes at most this many requests for one HTTP probe, following
/// redirects to the same host.
const REQUESTS_MAX: usize = 10;
const HEALTH_CHECK_PATH: &str = "/grpc.health.v1.Health/Check";
/// `grpc.health.v1.HealthCheckResponse.SERVING`.
const SERVING: u64 = 1;

/// The result of one run, with a detail that becomes the reason when a sleep
/// is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    pub passed: bool,
    /// The HTTP status, or the gRPC status.
    pub code: Option<i32>,
    pub detail: String,
}

impl Outcome {
    fn failed(detail: String) -> Outcome {
        Outcome {
            passed: false,
            code: None,
            detail,
        }
    }
}

/// Runs a TCP, HTTP or gRPC probe against the pod at `ip` within `timeout`.
pub async fn run(action: &ProbeAction, ip: Ipv4Addr, timeout: Duration) -> Outcome {
    let run = async {
        match action {
            ProbeAction::Tcp { port } => tcp(ip, *port).await,
            ProbeAction::Http {
                port,
                path,
                https,
                headers,
            } => http(ip, *port, path, *https, headers).await,
            ProbeAction::Grpc { port, service } => {
                grpc(ip, *port, service.as_deref().unwrap_or("")).await
            }
            ProbeAction::Exec { command } => {
                Outcome::failed(format!("the exec probe {command:?} runs in the pod"))
            }
        }
    };
    tokio::time::timeout(timeout, run)
        .await
        .unwrap_or_else(|_| {
            Outcome::failed(format!(
                "{} timed out after {timeout:?}",
                describe(action, ip)
            ))
        })
}

fn describe(action: &ProbeAction, ip: Ipv4Addr) -> String {
    match action {
        ProbeAction::Tcp { port } => format!("TCP {ip}:{port}"),
        ProbeAction::Http {
            port, path, https, ..
        } => format!("HTTP GET {}", url(*https, ip, *port, path)),
        ProbeAction::Grpc { port, service } => grpc_name(ip, *port, service.as_deref()),
        ProbeAction::Exec { command } => format!("exec {command:?}"),
    }
}

async fn tcp(ip: Ipv4Addr, port: u16) -> Outcome {
    match TcpStream::connect((ip, port)).await {
        Ok(_) => Outcome {
            passed: true,
            code: None,
            detail: format!("TCP {ip}:{port}: connected"),
        },
        Err(e) => Outcome::failed(format!("TCP {ip}:{port}: {e}")),
    }
}

fn url(https: bool, ip: Ipv4Addr, port: u16, target: &str) -> String {
    let scheme = if https { "https" } else { "http" };
    format!("{scheme}://{ip}:{port}{target}")
}

/// One request of an HTTP probe.
#[derive(Debug, PartialEq, Eq)]
struct Hop {
    https: bool,
    port: u16,
    /// The URL's host and port, as Go's `URL.Host`.
    authority: String,
    target: String,
    /// The probe's own `Host`, while Go still sends it.
    host: Option<String>,
}

/// A GET as kubelet's: 200 to 399 pass, and redirects to the same host are
/// followed as Go's client follows them.
async fn http(
    ip: Ipv4Addr,
    port: u16,
    path: &str,
    https: bool,
    headers: &[(String, String)],
) -> Outcome {
    let mut hop = Hop {
        https,
        port,
        authority: format!("{ip}:{port}"),
        target: path.to_string(),
        host: headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(HOST.as_str()))
            .map(|(_, value)| value.clone())
            .filter(|value| !value.is_empty()),
    };
    for _ in 0..REQUESTS_MAX {
        let url = url(hop.https, ip, hop.port, &hop.target);
        let (status, location) = match get(ip, &hop, headers).await {
            Ok(answer) => answer,
            Err(e) => return Outcome::failed(format!("HTTP GET {url}: {e}")),
        };
        if let (301 | 302 | 303 | 307 | 308, Some(location)) =
            (status, location.filter(|l| !l.is_empty()))
        {
            match redirect(&hop, &location, ip) {
                Ok(Some(next)) => {
                    hop = next;
                    continue;
                }
                Ok(None) => {}
                Err(e) => return Outcome::failed(format!("HTTP GET {url}: {e}")),
            }
        }
        return Outcome {
            passed: (200..400).contains(&status),
            code: Some(i32::from(status)),
            detail: format!("HTTP GET {url}: status {status}"),
        };
    }
    Outcome::failed(format!(
        "HTTP GET {}: stopped after {REQUESTS_MAX} redirects",
        url(https, ip, port, path)
    ))
}

/// Where Go's client goes from `hop` for `location`, resolved by RFC 3986,
/// if kubelet follows it there: only to the same host. Go keeps the probe's
/// `Host` only through a Location without a scheme. An error is a Location
/// that fails the probe.
fn redirect(hop: &Hop, location: &str, ip: Ipv4Addr) -> Result<Option<Hop>, String> {
    let next = Url::parse(&url(hop.https, ip, hop.port, &hop.target))
        .and_then(|from| from.join(location))
        .map_err(|e| format!("Location {location:?}: {e}"))?;
    if next.host_str() != Some(ip.to_string().as_str()) {
        return Ok(None);
    }
    let https = match next.scheme() {
        "http" => false,
        "https" => true,
        other => {
            return Err(format!(
                "Location {location:?}: unsupported scheme {other:?}"
            ))
        }
    };
    let absolute = Url::parse(location).is_ok();
    let authority = if absolute || location.starts_with("//") {
        next[Position::BeforeHost..Position::AfterPort].to_string()
    } else {
        hop.authority.clone()
    };
    Ok(Some(Hop {
        https,
        port: next
            .port_or_known_default()
            .unwrap_or(if https { 443 } else { 80 }),
        authority,
        target: next[Position::BeforePath..Position::AfterQuery].to_string(),
        host: hop
            .host
            .clone()
            .filter(|host| !absolute && *host != hop.authority),
    }))
}

/// Ends a connection's task when the check ends.
struct ConnectionTask(JoinHandle<()>);

impl Drop for ConnectionTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// One GET, with kubelet's headers: its User-Agent and `Accept: */*` unless
/// the probe sets its own, the probe's `Host` while Go sends it, and no
/// keep-alive. Returns the status and the `Location`.
async fn get(
    ip: Ipv4Addr,
    hop: &Hop,
    headers: &[(String, String)],
) -> Result<(u16, Option<String>), String> {
    let (port, target) = (hop.port, hop.target.as_str());
    let mut map = HeaderMap::new();
    for (name, value) in headers {
        let name =
            HeaderName::from_bytes(name.as_bytes()).map_err(|e| format!("header {name:?}: {e}"))?;
        let value = HeaderValue::from_str(value).map_err(|e| format!("header {name}: {e}"))?;
        map.append(name, value);
    }
    map.remove(HOST);
    let host = hop.host.as_deref().unwrap_or(&hop.authority);
    map.insert(
        HOST,
        HeaderValue::from_str(host).map_err(|e| format!("header {HOST}: {e}"))?,
    );
    if !map.contains_key(USER_AGENT) {
        map.insert(USER_AGENT, HeaderValue::from_static(KUBE_PROBE_USER_AGENT));
    }
    if !map.contains_key(ACCEPT) {
        map.insert(ACCEPT, HeaderValue::from_static("*/*"));
    }
    map.insert(CONNECTION, HeaderValue::from_static("close"));
    let mut request = Request::get(target)
        .body(Empty::<Bytes>::new())
        .map_err(|e| format!("request target {target:?}: {e}"))?;
    *request.headers_mut() = map;

    let tls = hop.https.then(|| (tls(), ServerName::IpAddress(ip.into())));
    let (mut sender, connection) = http1::connect((ip, port), tls).await.map_err(|e| match e {
        ConnectError::Tcp(e) => e.to_string(),
        ConnectError::Tls(e) => format!("TLS: {e}"),
        ConnectError::Http(e) => e.to_string(),
    })?;
    let _connection = ConnectionTask(tokio::spawn(async move {
        let _ = connection.await;
    }));
    let response = sender
        .send_request(request)
        .await
        .map_err(|e| e.to_string())?;
    let location = response
        .headers()
        .get(LOCATION)
        .and_then(|l| l.to_str().ok())
        .map(str::to_string);
    Ok((response.status().as_u16(), location))
}

/// The pre-sleep checks' TLS: any certificate, as kubelet's HTTPS probes
/// accept, but offering only `http/1.1`, the only protocol the check speaks.
fn tls() -> &'static TlsConnector {
    static TLS: OnceLock<TlsConnector> = OnceLock::new();
    TLS.get_or_init(|| TlsConnector::from(Arc::new(insecure_client_config(&[b"http/1.1"]))))
}

/// A TLS client that accepts any certificate, as with Go's
/// `InsecureSkipVerify`, and offers `alpn`.
pub(super) fn insecure_client_config(alpn: &[&[u8]]) -> ClientConfig {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("ring has cipher suites for TLS 1.2 and 1.3")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AnyCertificate(provider)))
        .with_no_client_auth();
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    config
}

/// Accepts any server certificate, but still checks the handshake's
/// signatures, as Go does.
#[derive(Debug)]
struct AnyCertificate(Arc<CryptoProvider>);

impl ServerCertVerifier for AnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

fn grpc_name(ip: Ipv4Addr, port: u16, service: Option<&str>) -> String {
    match service.filter(|s| !s.is_empty()) {
        Some(service) => format!("gRPC health check of {service:?} at {ip}:{port}"),
        None => format!("gRPC health check of {ip}:{port}"),
    }
}

/// `grpc.health.v1.Health/Check` over h2c, as kubelet's: it passes on
/// status OK with `SERVING`.
async fn grpc(ip: Ipv4Addr, port: u16, service: &str) -> Outcome {
    let name = grpc_name(ip, port, Some(service));
    match health_check(ip, port, service).await {
        Ok((0, Some(SERVING))) => Outcome {
            passed: true,
            code: Some(0),
            detail: format!("{name}: SERVING"),
        },
        Ok((0, status)) => Outcome {
            passed: false,
            code: Some(0),
            detail: format!("{name}: {}", serving_status(status)),
        },
        Ok((code, _)) => Outcome {
            passed: false,
            code: Some(code),
            detail: format!("{name}: gRPC status {code}"),
        },
        Err(e) => Outcome::failed(format!("{name}: {e}")),
    }
}

fn serving_status(status: Option<u64>) -> String {
    match status {
        Some(0) => "UNKNOWN".into(),
        Some(1) => "SERVING".into(),
        Some(2) => "NOT_SERVING".into(),
        Some(3) => "SERVICE_UNKNOWN".into(),
        Some(other) => format!("status {other}"),
        None => "an answer that is not a HealthCheckResponse".into(),
    }
}

/// Returns the gRPC status, and the health status if the answer has one.
async fn health_check(
    ip: Ipv4Addr,
    port: u16,
    service: &str,
) -> Result<(i32, Option<u64>), String> {
    let tcp = TcpStream::connect((ip, port))
        .await
        .map_err(|e| e.to_string())?;
    let _ = tcp.set_nodelay(true);
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(tcp))
            .await
            .map_err(|e| format!("HTTP/2: {e}"))?;
    let _connection = ConnectionTask(tokio::spawn(async move {
        let _ = connection.await;
    }));
    let request = Request::post(format!("http://{ip}:{port}{HEALTH_CHECK_PATH}"))
        .header(CONTENT_TYPE, "application/grpc")
        .header(TE, "trailers")
        .header(USER_AGENT, KUBE_PROBE_USER_AGENT)
        .body(Full::new(Bytes::from(health_request(service))))
        .map_err(|e| e.to_string())?;
    let response = sender
        .send_request(request)
        .await
        .map_err(|e| e.to_string())?;
    if response.status() != StatusCode::OK {
        return Err(format!("HTTP status {}", response.status().as_u16()));
    }
    // A trailers-only answer, as for an error.
    if let Some(status) = grpc_status(response.headers()) {
        return Ok((status, None));
    }
    let body = Limited::new(response.into_body(), 64 << 10)
        .collect()
        .await
        .map_err(|e| e.to_string())?;
    let status = body
        .trailers()
        .and_then(grpc_status)
        .ok_or("no grpc-status in the answer")?;
    Ok((status, health_status(&body.to_bytes())))
}

fn grpc_status(headers: &HeaderMap) -> Option<i32> {
    headers.get("grpc-status")?.to_str().ok()?.parse().ok()
}

/// A length-prefixed `HealthCheckRequest { service }`.
fn health_request(service: &str) -> Vec<u8> {
    let mut message = Vec::new();
    if !service.is_empty() {
        message.push(0x0a);
        put_varint(service.len() as u64, &mut message);
        message.extend_from_slice(service.as_bytes());
    }
    let mut frame = vec![0];
    frame.extend_from_slice(&(message.len() as u32).to_be_bytes());
    frame.extend(message);
    frame
}

/// The status of a length-prefixed `HealthCheckResponse`; an absent field is
/// 0, `UNKNOWN`.
fn health_status(frame: &[u8]) -> Option<u64> {
    let (&compressed, rest) = frame.split_first()?;
    if compressed != 0 || rest.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes(rest[..4].try_into().ok()?) as usize;
    let mut message = rest.get(4..4 + len)?;
    let mut status = 0;
    while !message.is_empty() {
        let key = take_varint(&mut message)?;
        match key & 7 {
            0 => {
                let value = take_varint(&mut message)?;
                if key >> 3 == 1 {
                    status = value;
                }
            }
            1 => message = message.get(8..)?,
            2 => {
                let len = usize::try_from(take_varint(&mut message)?).ok()?;
                message = message.get(len..)?;
            }
            5 => message = message.get(4..)?,
            _ => return None,
        }
    }
    Some(status)
}

fn put_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn take_varint(input: &mut &[u8]) -> Option<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = input.split_first()?;
        *input = rest;
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Some(value);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::convert::Infallible;
    use std::net::SocketAddr;

    use http_body_util::combinators::BoxBody;
    use http_body_util::StreamBody;
    use hyper::body::{Frame, Incoming};
    use hyper::service::service_fn;
    use hyper::Response;
    use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::ServerConfig;
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;

    const LOCAL: Ipv4Addr = Ipv4Addr::LOCALHOST;
    const TIMEOUT: Duration = Duration::from_secs(5);

    type Body = BoxBody<Bytes, Infallible>;

    fn reply(status: u16, location: Option<&str>) -> Response<Body> {
        let mut response = Response::builder().status(status);
        if let Some(location) = location {
            response = response.header(LOCATION, location);
        }
        response.body(Full::new(Bytes::new()).boxed()).unwrap()
    }

    fn header<'a>(request: &'a Request<Incoming>, name: &str) -> &'a str {
        request
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
    }

    /// The test app's routes.
    fn route(request: &Request<Incoming>, port: u16) -> Response<Body> {
        let target = request.uri().path_and_query().unwrap().as_str();
        match target {
            "/ok" => reply(200, None),
            "/fail" => reply(503, None),
            "/moved" => reply(301, Some("/ok")),
            "/moved-to-fail" => reply(302, Some("/fail")),
            "/dir/relative" => reply(307, Some("ok?x=1")),
            "/dir/ok?x=1" => reply(204, None),
            "/dir/query?q=1" => reply(302, Some("?x=2")),
            "/dir/query?x=2" => reply(200, None),
            "/dir/dots" => reply(302, Some("../ok")),
            "/dir/dot-path" => reply(302, Some("/x/../ok")),
            "/absolute" => reply(308, Some(&format!("http://127.0.0.1:{port}/ok#top"))),
            "/away" => reply(302, Some("http://192.0.2.1/ok")),
            "/loop" => reply(302, Some("/loop")),
            "/host/relative" => reply(302, Some("expect?example.com")),
            "/host/absolute" => reply(
                302,
                Some(&format!(
                    "http://127.0.0.1:{port}/host/expect?127.0.0.1:{port}"
                )),
            ),
            "/not-modified" => reply(304, None),
            "/defaults?q=1" => {
                let ok = header(request, "user-agent") == KUBE_PROBE_USER_AGENT
                    && header(request, "accept") == "*/*"
                    && header(request, "host") == format!("127.0.0.1:{port}")
                    && header(request, "connection") == "close";
                reply(if ok { 200 } else { 400 }, None)
            }
            "/custom" => {
                let ok = header(request, "user-agent") == "custom/1"
                    && header(request, "accept") == "*/*"
                    && header(request, "host") == "example.com"
                    && header(request, "x-custom") == "1";
                reply(if ok { 200 } else { 400 }, None)
            }
            _ if target.starts_with("/host/expect?") => {
                let ok = header(request, "host") == &target["/host/expect?".len()..];
                reply(if ok { 200 } else { 400 }, None)
            }
            _ if target.starts_with("/to/") => reply(
                302,
                Some(&format!(
                    "http://127.0.0.1:{}/loop",
                    &target["/to/".len()..]
                )),
            ),
            _ => reply(404, None),
        }
    }

    /// An HTTP/1.1 server of [`route`] on 127.0.0.1, over TLS with a
    /// self-signed certificate if `tls`.
    async fn http_server(tls: bool) -> u16 {
        let acceptor = tls.then(|| {
            let key = rcgen::KeyPair::generate().unwrap();
            let cert = rcgen::CertificateParams::new(vec!["pod.example".to_string()])
                .unwrap()
                .self_signed(&key)
                .unwrap();
            let key: PrivateKeyDer = PrivatePkcs8KeyDer::from(key.serialize_der()).into();
            let config = ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert.der().clone()], key)
            .unwrap();
            TlsAcceptor::from(Arc::new(config))
        });
        let listener = TcpListener::bind((LOCAL, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<Incoming>| {
                        let response = route(&request, port);
                        async move { Ok::<_, Infallible>(response) }
                    });
                    let http = hyper::server::conn::http1::Builder::new();
                    let _ = match acceptor {
                        Some(acceptor) => match acceptor.accept(tcp).await {
                            Ok(tls) => http.serve_connection(TokioIo::new(tls), service).await,
                            Err(_) => return,
                        },
                        None => http.serve_connection(TokioIo::new(tcp), service).await,
                    };
                });
            }
        });
        port
    }

    /// A gRPC health service over h2c: `SERVING` for the server (no
    /// service), `NOT_SERVING` for "down", and `NOT_FOUND` otherwise, as a
    /// trailers-only answer.
    async fn grpc_server() -> u16 {
        let listener = TcpListener::bind((LOCAL, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let service = service_fn(|request: Request<Incoming>| async move {
                        assert_eq!(request.uri().path(), HEALTH_CHECK_PATH);
                        assert_eq!(header(&request, "content-type"), "application/grpc");
                        assert_eq!(header(&request, "user-agent"), KUBE_PROBE_USER_AGENT);
                        let frame = request.into_body().collect().await.unwrap().to_bytes();
                        let service = match &frame[5..] {
                            [] => String::new(),
                            [0x0a, len, name @ ..] if usize::from(*len) == name.len() => {
                                String::from_utf8(name.to_vec()).unwrap()
                            }
                            other => panic!("unexpected request {other:?}"),
                        };
                        let response = Response::builder().header(CONTENT_TYPE, "application/grpc");
                        let status = match service.as_str() {
                            "" => 1u8,
                            "down" => 2,
                            _ => {
                                let body = Full::new(Bytes::new()).boxed();
                                return Ok::<_, Infallible>(
                                    response.header("grpc-status", "5").body(body).unwrap(),
                                );
                            }
                        };
                        let mut trailers = HeaderMap::new();
                        trailers.insert("grpc-status", HeaderValue::from_static("0"));
                        let frames = vec![
                            Ok::<_, Infallible>(Frame::data(Bytes::from(vec![
                                0, 0, 0, 0, 2, 0x08, status,
                            ]))),
                            Ok(Frame::trailers(trailers)),
                        ];
                        let body = StreamBody::new(futures_util::stream::iter(frames)).boxed();
                        Ok(response.body(body).unwrap())
                    });
                    let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(tcp), service)
                        .await;
                });
            }
        });
        port
    }

    /// A port with nothing listening.
    async fn closed_port() -> u16 {
        let listener = TcpListener::bind((LOCAL, 0)).await.unwrap();
        listener.local_addr().unwrap().port()
    }

    fn http_probe(port: u16, path: &str, https: bool) -> ProbeAction {
        ProbeAction::Http {
            port,
            path: path.into(),
            https,
            headers: Vec::new(),
        }
    }

    async fn check(action: ProbeAction) -> Outcome {
        run(&action, LOCAL, TIMEOUT).await
    }

    #[tokio::test]
    async fn tcp_probes_pass_when_the_port_accepts() {
        let listener = TcpListener::bind((LOCAL, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let outcome = check(ProbeAction::Tcp { port }).await;
        assert!(outcome.passed, "{outcome:?}");
        let port = closed_port().await;
        let outcome = check(ProbeAction::Tcp { port }).await;
        assert!(!outcome.passed);
        assert!(
            outcome
                .detail
                .starts_with(&format!("TCP 127.0.0.1:{port}: ")),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn http_probes_pass_from_200_to_399_and_follow_redirects_to_the_pod() {
        let port = http_server(false).await;
        for (path, code) in [
            ("/ok", 200),
            ("/moved", 200),
            ("/dir/relative", 204),
            ("/dir/query?q=1", 200),
            ("/dir/dots", 200),
            ("/dir/dot-path", 200),
            ("/absolute", 200),
            ("/away", 302),
            ("/not-modified", 304),
            ("/defaults?q=1", 200),
        ] {
            let outcome = check(http_probe(port, path, false)).await;
            assert!(outcome.passed, "{path}: {outcome:?}");
            assert_eq!(outcome.code, Some(code), "{path}");
        }
        for (path, code) in [("/fail", 503), ("/moved-to-fail", 503), ("/missing", 404)] {
            let outcome = check(http_probe(port, path, false)).await;
            assert!(!outcome.passed, "{path}: {outcome:?}");
            assert_eq!(outcome.code, Some(code), "{path}");
        }
        let outcome = check(http_probe(port, "/fail", false)).await;
        assert_eq!(
            outcome.detail,
            format!("HTTP GET http://127.0.0.1:{port}/fail: status 503")
        );
        let outcome = check(http_probe(port, "/loop", false)).await;
        assert!(!outcome.passed && outcome.code.is_none(), "{outcome:?}");
        assert!(outcome.detail.ends_with("stopped after 10 redirects"));
    }

    #[tokio::test]
    async fn a_redirect_loop_is_reported_at_the_probes_url() {
        let (port, other) = (http_server(false).await, http_server(false).await);
        let outcome = check(http_probe(port, &format!("/to/{other}"), false)).await;
        assert_eq!(
            outcome.detail,
            format!("HTTP GET http://127.0.0.1:{port}/to/{other}: stopped after 10 redirects")
        );
    }

    #[tokio::test]
    async fn an_http_probe_sends_its_own_headers() {
        let port = http_server(false).await;
        let headers = [
            ("Host", "example.com"),
            ("User-Agent", "custom/1"),
            ("X-Custom", "1"),
        ];
        let action = ProbeAction::Http {
            port,
            path: "/custom".into(),
            https: false,
            headers: headers
                .iter()
                .map(|(n, v)| (n.to_string(), v.to_string()))
                .collect(),
        };
        let outcome = check(action).await;
        assert!(outcome.passed, "{outcome:?}");
        let outcome = check(http_probe(port, "/custom", false)).await;
        assert_eq!(outcome.code, Some(400));
    }

    #[tokio::test]
    async fn an_http_probe_sends_its_host_until_a_redirect_with_a_scheme() {
        let port = http_server(false).await;
        let probe = |path: &str| ProbeAction::Http {
            port,
            path: path.into(),
            https: false,
            headers: vec![("Host".into(), "example.com".into())],
        };
        for path in ["/host/relative", "/host/absolute"] {
            let outcome = check(probe(path)).await;
            assert_eq!(outcome.code, Some(200), "{path}: {outcome:?}");
        }
        let outcome = check(http_probe(port, "/host/relative", false)).await;
        assert_eq!(outcome.code, Some(400));
    }

    #[tokio::test]
    async fn https_probes_accept_a_self_signed_certificate() {
        let port = http_server(true).await;
        let outcome = check(http_probe(port, "/ok", true)).await;
        assert!(outcome.passed, "{outcome:?}");
        assert_eq!(
            outcome.detail,
            format!("HTTP GET https://127.0.0.1:{port}/ok: status 200")
        );
        let outcome = check(http_probe(port, "/fail", true)).await;
        assert_eq!((outcome.passed, outcome.code), (false, Some(503)));
        let outcome = check(http_probe(port, "/ok", false)).await;
        assert!(!outcome.passed && outcome.code.is_none(), "{outcome:?}");
    }

    #[tokio::test]
    async fn grpc_probes_pass_only_when_serving() {
        let port = grpc_server().await;
        let probe = |service: Option<&str>| ProbeAction::Grpc {
            port,
            service: service.map(str::to_string),
        };
        let outcome = check(probe(None)).await;
        assert_eq!(
            (outcome.passed, outcome.code),
            (true, Some(0)),
            "{outcome:?}"
        );
        let outcome = check(probe(Some("down"))).await;
        assert_eq!((outcome.passed, outcome.code), (false, Some(0)));
        assert_eq!(
            outcome.detail,
            format!("gRPC health check of \"down\" at 127.0.0.1:{port}: NOT_SERVING")
        );
        let outcome = check(probe(Some("other"))).await;
        assert_eq!(
            (outcome.passed, outcome.code),
            (false, Some(5)),
            "{outcome:?}"
        );
        let outcome = check(ProbeAction::Grpc {
            port: closed_port().await,
            service: None,
        })
        .await;
        assert!(!outcome.passed && outcome.code.is_none());
    }

    #[tokio::test]
    async fn a_probe_that_does_not_answer_times_out() {
        let listener = TcpListener::bind((LOCAL, 0)).await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        let _held = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((tcp, _)) = listener.accept().await {
                held.push(tcp);
            }
        });
        let action = http_probe(addr.port(), "/ok", false);
        let outcome = run(&action, LOCAL, Duration::from_millis(200)).await;
        assert!(!outcome.passed);
        assert_eq!(
            outcome.detail,
            format!(
                "HTTP GET http://127.0.0.1:{}/ok timed out after 200ms",
                addr.port()
            )
        );
    }

    #[test]
    fn redirects_resolve_like_gos() {
        let ip = Ipv4Addr::new(10, 42, 0, 9);
        let hop = |https: bool, port: u16, authority: &str, target: &str| Hop {
            https,
            port,
            authority: authority.into(),
            target: target.into(),
            host: None,
        };
        let from = hop(false, 8080, "10.42.0.9:8080", "/a/b?q=1");
        let same = |target: &str| hop(false, 8080, "10.42.0.9:8080", target);
        for (location, next) in [
            ("/c", same("/c")),
            ("c#x", same("/a/c")),
            ("?x=2", same("/a/b?x=2")),
            ("../c", same("/c")),
            ("/c/../d", same("/d")),
            ("#x", same("/a/b?q=1")),
            ("HTTP://10.42.0.9/up", hop(false, 80, "10.42.0.9", "/up")),
            ("https://10.42.0.9/z", hop(true, 443, "10.42.0.9", "/z")),
            ("//10.42.0.9:81", hop(false, 81, "10.42.0.9:81", "/")),
        ] {
            assert_eq!(
                redirect(&from, location, ip),
                Ok(Some(next)),
                "{location:?}"
            );
        }
        assert_eq!(redirect(&from, "http://10.42.0.10/z", ip), Ok(None));
        assert!(redirect(&from, "ftp://10.42.0.9/z", ip).is_err());
        assert!(redirect(&from, "http://[::1/z", ip).is_err());

        let from = Hop {
            host: Some("example.com".into()),
            ..hop(false, 80, "10.42.0.9:80", "/a")
        };
        let next = |location: &str| redirect(&from, location, ip).unwrap().unwrap();
        assert_eq!(next("/b").host.as_deref(), Some("example.com"));
        assert_eq!(next("/b").authority, "10.42.0.9:80");
        assert_eq!(next("//10.42.0.9/b").host.as_deref(), Some("example.com"));
        assert_eq!(next("http://10.42.0.9/b").host, None);
    }

    #[test]
    fn health_messages_round_trip() {
        assert_eq!(health_request(""), [0, 0, 0, 0, 0]);
        assert_eq!(health_request("ab"), [0, 0, 0, 0, 4, 0x0a, 2, b'a', b'b']);
        assert_eq!(health_status(&[0, 0, 0, 0, 2, 0x08, 1]), Some(1));
        assert_eq!(health_status(&[0, 0, 0, 0, 0]), Some(0));
        // An unknown field before the status is skipped.
        assert_eq!(
            health_status(&[0, 0, 0, 0, 5, 0x12, 1, b'x', 0x08, 2]),
            Some(2)
        );
        assert_eq!(health_status(&[1, 0, 0, 0, 0]), None);
        assert_eq!(health_status(&[0, 0, 0, 0, 3, 0x08]), None);
        let mut long = Vec::new();
        put_varint(300, &mut long);
        assert_eq!(take_varint(&mut long.as_slice()), Some(300));
    }
}
