//! The relay on the node address's port [`RELAY_PORT`], where nftables sends
//! the ports of routed origins. For each connection it reads the TLS server
//! name or the HTTP `Host`, picks the route, ends TLS with a leaf for that
//! name from the profile's issuer, and copies both ways to the route's
//! backend, dialled from the node address so that a sleeping simulator wakes
//! on the relay's SYN. The backend sees the original `Host`. While a backend
//! connection is open, [`Calls`] names its caller and route by its local
//! port, so that the wake's reason names them rather than the node address.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::os::fd::AsFd;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use log::{debug, warn};
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyIdMethod, KeyPair, KeyUsagePurpose,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};

use super::events::{Detail, Key, Kind};
use super::profile::{find_route, CheckedRoute, Issuer, Protocol};
use super::sniff::{sniff, Hello};
use super::{dns, original_dst, Shared, RELAY_PORT};
use crate::sync::lock;

/// How long the relay waits for a client's first bytes. A client may connect
/// and send later: a Node event loop that a starved pod stalls, a pod that
/// wakes. A routed port's clients speak first (TLS and HTTP), so waiting is
/// safe, and giving up must not be answered in clear text: on a TLS port that
/// reads as `wrong version number`.
const FIRST_BYTES_FOR: Duration = Duration::from_secs(30);
/// How long the relay reads a connection for its name once the first bytes
/// are there.
const SNIFF_FOR: Duration = Duration::from_secs(2);
/// The most bytes it reads for it.
const SNIFF_MAX: usize = 16 * 1024;
/// How long a backend may take to accept: a wake waits for room up to the
/// node's admission wait, then resumes.
const CONNECT_FOR: Duration = Duration::from_secs(40);
/// The ids of the relay's cluster DNS queries.
static NEXT_ID: AtomicU16 = AtomicU16::new(1);

/// How long a minted leaf is valid.
const LEAF_DAYS: i64 = 7;

/// The caller and route of a connection that the relay dialled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    /// The calling pod's `NAMESPACE/NAME`, or its address.
    pub caller: String,
    pub route: String,
}

/// The relay's open backend connections, by their local port on the node
/// address.
#[derive(Default)]
pub struct Calls {
    by_port: Mutex<HashMap<u16, Call>>,
}

impl Calls {
    pub fn get(&self, port: u16) -> Option<Call> {
        lock(&self.by_port).get(&port).cloned()
    }

    /// Records `call` on `port` until the returned guard drops.
    fn open(&self, port: u16, call: Call) -> OpenCall<'_> {
        lock(&self.by_port).insert(port, call);
        OpenCall { calls: self, port }
    }
}

/// A call in [`Calls`], removed when it drops.
struct OpenCall<'a> {
    calls: &'a Calls,
    port: u16,
}

impl Drop for OpenCall<'_> {
    fn drop(&mut self) {
        lock(&self.calls.by_port).remove(&self.port);
    }
}

/// The leaves minted from the current issuer, by name.
#[derive(Default)]
pub struct Certs {
    leaves: Mutex<HashMap<String, Arc<rustls::ServerConfig>>>,
}

impl Certs {
    pub fn clear(&self) {
        lock(&self.leaves).clear();
    }

    /// The TLS configuration for `name`, with a leaf from `issuer` and the
    /// ALPN protocols of `protocol`.
    fn config(
        &self,
        issuer: &Issuer,
        name: &str,
        protocol: Protocol,
    ) -> Result<Arc<rustls::ServerConfig>, String> {
        let key = format!("{name} {protocol:?}");
        if let Some(config) = lock(&self.leaves).get(&key) {
            return Ok(config.clone());
        }
        let (chain, leaf_key) = mint(issuer, name)?;
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_no_client_auth()
        .with_single_cert(chain, leaf_key)
        .map_err(|e| format!("the leaf for {name}: {e}"))?;
        config.alpn_protocols = match protocol {
            Protocol::Http1 => vec![b"http/1.1".to_vec()],
            Protocol::H2c => vec![b"h2".to_vec()],
            Protocol::Auto => vec![b"h2".to_vec(), b"http/1.1".to_vec()],
        };
        let config = Arc::new(config);
        lock(&self.leaves).insert(key, config.clone());
        Ok(config)
    }
}

fn pem_der(text: &str, what: &str) -> Result<Vec<u8>, String> {
    let mut reader = std::io::BufReader::new(text.as_bytes());
    let item = rustls_pemfile::read_one(&mut reader)
        .map_err(|e| format!("the issuer's {what}: {e}"))?
        .ok_or_else(|| format!("the issuer's {what} has no PEM block"))?;
    Ok(match item {
        rustls_pemfile::Item::X509Certificate(der) => der.to_vec(),
        rustls_pemfile::Item::Pkcs8Key(der) => der.secret_pkcs8_der().to_vec(),
        _ => return Err(format!("the issuer's {what} is not what it should be")),
    })
}

fn hex(text: &str) -> Result<Vec<u8>, String> {
    (0..text.len())
        .step_by(2)
        .map(|i| {
            text.get(i..i + 2)
                .and_then(|b| u8::from_str_radix(b, 16).ok())
                .ok_or_else(|| format!("{text:?} is not hex"))
        })
        .collect()
}

/// A leaf for `name` signed by `issuer`, and the chain to send: the leaf and
/// the issuer.
pub fn mint(
    issuer: &Issuer,
    name: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), String> {
    let issuer_der = pem_der(&issuer.certificate, "certificate")?;
    let issuer_key_der = pem_der(&issuer.key, "key")?;
    let issuer_key = KeyPair::try_from(issuer_key_der.as_slice())
        .map_err(|e| format!("the issuer's key: {e}"))?;
    let mut issuer_params = CertificateParams::default();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, issuer.subject.as_str());
    issuer_params.distinguished_name = dn;
    issuer_params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    issuer_params.key_identifier_method = KeyIdMethod::PreSpecified(hex(&issuer.key_id)?);
    // Only its name and key identifier are used: the chain sends Floci's.
    let issuer_cert = issuer_params
        .self_signed(&issuer_key)
        .map_err(|e| format!("the issuer: {e}"))?;

    let leaf_key = KeyPair::generate().map_err(|e| format!("a leaf key: {e}"))?;
    let mut params =
        CertificateParams::new(vec![name.to_string()]).map_err(|e| format!("{name}: {e}"))?;
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, name);
    params.distinguished_name = dn;
    let now = time::OffsetDateTime::now_utc();
    params.not_before = now - time::Duration::hours(1);
    params.not_after = now + time::Duration::days(LEAF_DAYS);
    params.is_ca = IsCa::ExplicitNoCa;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    params.use_authority_key_identifier_extension = true;
    let leaf = params
        .signed_by(&leaf_key, &issuer_cert, &issuer_key)
        .map_err(|e| format!("sign the leaf for {name}: {e}"))?;
    let chain = vec![
        CertificateDer::from(leaf.der().to_vec()),
        CertificateDer::from(issuer_der),
    ];
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf_key.serialize_der()));
    Ok((chain, key))
}

/// The backend's address: a Service name from the cluster DNS, an address in
/// the cluster's CIDRs, or the Floci host.
async fn resolve(shared: &Shared, route: &CheckedRoute) -> Result<SocketAddr, String> {
    let host = route.backend_host.as_str();
    let port = route.backend_port;
    if host == "host.floci.internal" {
        return Ok(SocketAddr::from((shared.floci_host(), port)));
    }
    if let Ok(ip) = host.parse::<Ipv4Addr>() {
        if shared.env.cluster.iter().any(|c| c.contains(ip)) {
            return Ok(SocketAddr::from((ip, port)));
        }
        return Err(format!("{ip} is not in the cluster's CIDRs"));
    }
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let server = SocketAddr::from((shared.env.cluster_dns, 53));
    let answer = dns::ask_udp(server, &dns::a_query(host, id)).await?;
    let ip = dns::a_records(&answer)
        .into_iter()
        .next()
        .ok_or_else(|| format!("the cluster DNS has no address for {host}"))?;
    Ok(SocketAddr::from((ip, port)))
}

/// Connects to the route's backend for `caller`. The connection is in
/// [`Calls`] from before its SYN until the guard drops.
async fn dial<'a>(
    shared: &'a Shared,
    route: &CheckedRoute,
    caller: &str,
) -> Result<(TcpStream, Option<OpenCall<'a>>), String> {
    let backend = resolve(shared, route).await?;
    let socket = TcpSocket::new_v4().map_err(|e| e.to_string())?;
    // From the node address, so that the waker can ask which call it is.
    let call = if backend.ip().is_loopback() {
        None
    } else {
        socket
            .bind(SocketAddr::from((shared.node.address, 0)))
            .map_err(|e| format!("bind to {}: {e}", shared.node.address))?;
        let port = socket.local_addr().map_err(|e| e.to_string())?.port();
        let call = Call {
            caller: caller.to_string(),
            route: route.name.clone(),
        };
        Some(shared.calls.open(port, call))
    };
    let stream = tokio::time::timeout(CONNECT_FOR, socket.connect(backend))
        .await
        .map_err(|_| format!("{backend} did not accept in {CONNECT_FOR:?}"))?
        .map_err(|e| format!("connect to {backend}: {e}"))?;
    let _ = stream.set_nodelay(true);
    Ok((stream, call))
}

const UNAVAILABLE: &[u8] = b"HTTP/1.1 503 Service Unavailable\r\nRetry-After: 1\r\n\
Content-Type: text/plain\r\nContent-Length: 32\r\nConnection: close\r\n\r\n\
the route's backend is not ready";
const NO_ROUTE: &[u8] = b"HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/plain\r\n\
Content-Length: 22\r\nConnection: close\r\n\r\nno route for this host";

async fn splice<C>(mut client: C, mut backend: TcpStream) -> u64
where
    C: AsyncRead + AsyncWrite + Unpin,
{
    match tokio::io::copy_bidirectional(&mut client, &mut backend).await {
        Ok((up, down)) => up + down,
        Err(e) => {
            debug!("a relayed connection ended: {e}");
            0
        }
    }
}

/// What a connection's first bytes gave.
#[derive(Debug)]
enum Read {
    /// The client closed the connection, or it failed.
    Gone,
    /// The client sent nothing in the time that it was given.
    Silent,
    /// The first bytes. `looks_tls` is true when they start a TLS record,
    /// even when the hello is incomplete or not a ClientHello: the client
    /// then expects TLS back, never text.
    Hello { hello: Hello, looks_tls: bool },
}

/// Peeks at a connection for its name: up to `first_for` for the first
/// bytes, then up to `sniff_for` for the rest of the hello.
async fn read_hello(
    stream: &TcpStream,
    buf: &mut [u8],
    first_for: Duration,
    sniff_for: Duration,
) -> Read {
    let mut n = match tokio::time::timeout(first_for, stream.peek(buf)).await {
        Ok(Ok(n)) if n > 0 => n,
        Ok(_) => return Read::Gone,
        Err(_) => return Read::Silent,
    };
    let deadline = tokio::time::Instant::now() + sniff_for;
    loop {
        let looks_tls = buf[0] == 0x16;
        match sniff(&buf[..n]) {
            Hello::More if n < buf.len() => {
                // A peek returns at once while the bytes already read are
                // waiting, so the deadline is checked here, not on the peek.
                if tokio::time::Instant::now() >= deadline {
                    return Read::Hello {
                        hello: Hello::Other,
                        looks_tls,
                    };
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
                match tokio::time::timeout_at(deadline, stream.peek(buf)).await {
                    Ok(Ok(more)) if more > 0 => n = more,
                    Ok(_) => return Read::Gone,
                    Err(_) => {
                        return Read::Hello {
                            hello: Hello::Other,
                            looks_tls,
                        }
                    }
                }
            }
            hello => return Read::Hello { hello, looks_tls },
        }
    }
}

async fn relay(shared: Arc<Shared>, stream: TcpStream, peer: SocketAddrV4) {
    let Some(dst) = original_dst(stream.as_fd()) else {
        return;
    };
    let Some(applied) = shared.applied() else {
        return;
    };
    let routes = &applied.checked.routes;
    let on_port: Vec<&CheckedRoute> = routes
        .iter()
        .filter(|r| r.origins.iter().any(|o| o.port == dst.port()))
        .collect();
    let mut buf = vec![0u8; SNIFF_MAX];
    let read = read_hello(&stream, &mut buf, FIRST_BYTES_FOR, SNIFF_FOR).await;
    let (hello, looks_tls) = match read {
        Read::Gone => return,
        Read::Silent => {
            // Close without a word: the client may expect TLS.
            shared.events.record(
                Key {
                    kind: Kind::RelayFailed,
                    pod: shared.pod_name(*peer.ip()),
                    destination: dst.ip().to_string(),
                    port: dst.port(),
                    protocol: "tcp",
                },
                Detail {
                    error: Some(format!("the client sent nothing in {FIRST_BYTES_FOR:?}")),
                    ..Detail::default()
                },
            );
            return;
        }
        Read::Hello { hello, looks_tls } => (hello, looks_tls),
    };
    let only = || (on_port.len() == 1).then(|| on_port[0]);
    let (tls, name, route) = match &hello {
        Hello::Tls(Some(name)) => (
            true,
            Some(name.clone()),
            find_route(routes, true, dst.port(), name),
        ),
        Hello::Http(Some(name)) => (
            false,
            Some(name.clone()),
            find_route(routes, false, dst.port(), name),
        ),
        Hello::H2c | Hello::Http(None) => (
            false,
            None,
            only().filter(|r| r.origins.iter().any(|o| !o.tls)),
        ),
        _ => (false, None, None),
    };
    let caller = shared.pod_name(*peer.ip());
    let event = |kind: Kind, route: Option<&CheckedRoute>, error: Option<String>, bytes: u64| {
        shared.events.record(
            Key {
                kind,
                pod: caller.clone(),
                destination: name.clone().unwrap_or_else(|| dst.ip().to_string()),
                port: dst.port(),
                protocol: if tls { "https" } else { "http" },
            },
            Detail {
                name: name.clone(),
                route: route.map(|r| r.name.clone()),
                error,
                bytes,
            },
        )
    };
    let Some(route) = route else {
        event(Kind::RelayFailed, None, Some("no route".into()), 0);
        // Text only to a client that spoke HTTP: a TLS client, or one whose
        // hello stalled or came late, gets a close, not bytes it cannot read.
        if !tls && !looks_tls {
            let mut stream = stream;
            let _ = stream.write_all(NO_ROUTE).await;
        }
        return;
    };
    if tls {
        let (Some(name), Some(issuer)) = (name.as_deref(), applied.checked.profile.issuer.as_ref())
        else {
            return;
        };
        let config = match shared.certs.config(issuer, name, route.protocol) {
            Ok(config) => config,
            Err(e) => {
                warn!("the relay has no leaf for {name}: {e}");
                event(Kind::RelayFailed, Some(route), Some(e), 0);
                return;
            }
        };
        let mut client = match tokio_rustls::TlsAcceptor::from(config).accept(stream).await {
            Ok(client) => client,
            Err(e) => {
                debug!("TLS from {peer} for {name}: {e}");
                event(Kind::RelayFailed, Some(route), Some(format!("TLS: {e}")), 0);
                return;
            }
        };
        let h2 = client.get_ref().1.alpn_protocol() == Some(b"h2");
        match dial(&shared, route, &caller).await {
            Ok((backend, _call)) => {
                let bytes = splice(client, backend).await;
                event(Kind::Relayed, Some(route), None, bytes);
            }
            Err(e) => {
                if !h2 {
                    let _ = client.write_all(UNAVAILABLE).await;
                }
                let _ = client.shutdown().await;
                event(Kind::RelayFailed, Some(route), Some(e), 0);
            }
        }
    } else {
        match dial(&shared, route, &caller).await {
            Ok((backend, _call)) => {
                let bytes = splice(stream, backend).await;
                event(Kind::Relayed, Some(route), None, bytes);
            }
            Err(e) => {
                let mut stream = stream;
                if !matches!(hello, Hello::H2c) {
                    let _ = stream.write_all(UNAVAILABLE).await;
                }
                event(Kind::RelayFailed, Some(route), Some(e), 0);
            }
        }
    }
}

pub async fn serve(shared: Arc<Shared>) {
    let addr = SocketAddrV4::new(shared.node.address, RELAY_PORT);
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            warn!("the relay cannot listen on {addr}: {e}");
            return;
        }
    };
    loop {
        match listener.accept().await {
            Ok((stream, SocketAddr::V4(peer))) => {
                let _ = stream.set_nodelay(true);
                tokio::spawn(relay(shared.clone(), stream, peer));
            }
            Ok((_, SocketAddr::V6(_))) => {}
            Err(e) => {
                warn!("the relay's listener: {e}");
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::client::danger::ServerCertVerifier;

    /// An intermediate like Floci's: `pathLen 0`, its name and key id.
    fn issuer() -> (Issuer, rcgen::Certificate) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::default();
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, "Floci route CA test");
        params.distinguished_name = dn;
        params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let key_id = vec![7u8; 20];
        params.key_identifier_method = KeyIdMethod::PreSpecified(key_id.clone());
        let cert = params.self_signed(&key).unwrap();
        let issuer = Issuer {
            certificate: cert.pem(),
            key: key.serialize_pem(),
            subject: "Floci route CA test".into(),
            key_id: key_id.iter().map(|b| format!("{b:02x}")).collect(),
            not_after: "2099-01-01T00:00:00Z".into(),
        };
        (issuer, cert)
    }

    #[test]
    fn a_minted_leaf_verifies_against_the_issuer() {
        let (issuer, cert) = issuer();
        let (chain, _key) = mint(&issuer, "api.vendor.test").unwrap();
        assert_eq!(chain.len(), 2);
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.der().clone()).unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier =
            rustls::client::WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider)
                .build()
                .unwrap();
        let name = rustls::pki_types::ServerName::try_from("api.vendor.test").unwrap();
        let now = rustls::pki_types::UnixTime::now();
        verifier
            .verify_server_cert(&chain[0], &chain[1..], &name, &[], now)
            .unwrap();
        let other = rustls::pki_types::ServerName::try_from("other.test").unwrap();
        assert!(verifier
            .verify_server_cert(&chain[0], &chain[1..], &other, &[], now)
            .is_err());
    }

    /// A ClientHello from rustls for `name`.
    fn client_hello(name: &str) -> Vec<u8> {
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
        let server = rustls::pki_types::ServerName::try_from(name.to_string()).unwrap();
        let mut conn = rustls::ClientConnection::new(Arc::new(config), server).unwrap();
        let mut out = Vec::new();
        conn.write_tls(&mut out).unwrap();
        out
    }

    /// The relay's end and the client's end of a loopback connection.
    async fn pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (server, client)
    }

    #[tokio::test]
    async fn a_client_that_sends_nothing_gets_silence_not_text() {
        let (server, _client) = pair().await;
        let mut buf = vec![0u8; SNIFF_MAX];
        let read = read_hello(
            &server,
            &mut buf,
            Duration::from_millis(100),
            Duration::from_millis(100),
        )
        .await;
        assert!(matches!(read, Read::Silent), "{read:?}");
    }

    #[tokio::test]
    async fn a_client_hello_later_than_the_sniff_time_is_still_read() {
        let (server, mut client) = pair().await;
        let hello = client_hello("service.example");
        let sender = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            client.write_all(&hello).await.unwrap();
            client
        });
        let mut buf = vec![0u8; SNIFF_MAX];
        // The sniff time is shorter than the client's delay: only the wait
        // for the first bytes covers it.
        let read = read_hello(
            &server,
            &mut buf,
            Duration::from_secs(5),
            Duration::from_millis(100),
        )
        .await;
        match read {
            Read::Hello {
                hello: Hello::Tls(Some(name)),
                looks_tls: true,
            } => assert_eq!(name, "service.example"),
            other => panic!("{other:?}"),
        }
        let _client = sender.await.unwrap();
    }

    #[tokio::test]
    async fn a_stalled_client_hello_is_marked_as_tls() {
        let (server, mut client) = pair().await;
        let hello = client_hello("service.example");
        client.write_all(&hello[..20]).await.unwrap();
        let mut buf = vec![0u8; SNIFF_MAX];
        let read = read_hello(
            &server,
            &mut buf,
            Duration::from_secs(5),
            Duration::from_millis(100),
        )
        .await;
        // The caller must not answer this one with text.
        assert!(
            matches!(
                read,
                Read::Hello {
                    hello: Hello::Other,
                    looks_tls: true
                }
            ),
            "{read:?}"
        );
    }

    #[tokio::test]
    async fn an_http_request_is_not_marked_as_tls() {
        let (server, mut client) = pair().await;
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: Service.Example\r\n\r\n")
            .await
            .unwrap();
        let mut buf = vec![0u8; SNIFF_MAX];
        let read = read_hello(
            &server,
            &mut buf,
            Duration::from_secs(5),
            Duration::from_millis(100),
        )
        .await;
        match read {
            Read::Hello {
                hello: Hello::Http(Some(name)),
                looks_tls: false,
            } => assert_eq!(name, "service.example"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn a_client_that_closes_is_gone() {
        let (server, client) = pair().await;
        drop(client);
        let mut buf = vec![0u8; SNIFF_MAX];
        let read = read_hello(
            &server,
            &mut buf,
            Duration::from_secs(5),
            Duration::from_millis(100),
        )
        .await;
        assert!(matches!(read, Read::Gone), "{read:?}");
    }

    #[test]
    fn a_call_is_known_by_its_port_while_open() {
        let calls = Calls::default();
        let call = Call {
            caller: "default/orders".into(),
            route: "payments".into(),
        };
        {
            let _open = calls.open(40100, call.clone());
            assert_eq!(calls.get(40100), Some(call));
            assert_eq!(calls.get(40101), None);
        }
        assert_eq!(calls.get(40100), None);
    }

    #[test]
    fn alpn_follows_the_protocol() {
        let (issuer, _) = issuer();
        let certs = Certs::default();
        let h2 = certs.config(&issuer, "a.test", Protocol::H2c).unwrap();
        assert_eq!(h2.alpn_protocols, vec![b"h2".to_vec()]);
        let auto = certs.config(&issuer, "a.test", Protocol::Auto).unwrap();
        assert_eq!(auto.alpn_protocols.len(), 2);
        let again = certs.config(&issuer, "a.test", Protocol::Auto).unwrap();
        assert!(Arc::ptr_eq(&auto, &again), "a leaf is minted once per name");
    }
}
