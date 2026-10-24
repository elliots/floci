//! The node's dockerd through the API: its Engine API under [`PREFIX`], and
//! TCP tunnels to the containers on its networks, so that a caller with the
//! API's token reaches them without another published port.
//!
//! The proxy passes each request to dockerd's socket on a connection of its
//! own and streams both bodies, also an upgraded (hijacked) connection, as
//! `docker attach` and `docker exec -i` use. A tunnel is an HTTP Upgrade to
//! raw TCP: the node service connects to the container first, which wakes a
//! sleeping one, and answers 101 once the connection is open.

use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Extension, Request};
use axum::http::header::{AUTHORIZATION, CONNECTION, HOST, UPGRADE};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, Version};
use axum::response::{IntoResponse, Response};
use http_body_util::BodyExt;
use hyper::client::conn::http1;
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::TokioIo;
use log::{debug, warn};
use serde_json::Value;
use tokio::net::{TcpStream, UnixStream};

use super::ApiError;

/// dockerd's socket on the node.
pub const DOCKER_SOCKET: &str = "/run/docker.sock";

/// The path under which the API serves dockerd's Engine API.
pub const PREFIX: &str = "/docker";

/// How long a tunnel waits for its connection to the container, which
/// includes the wake of a sleeping one and a wait for admission.
const TUNNEL_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the tunnel's lookup of dockerd's networks may take.
const NETWORKS_TIMEOUT: Duration = Duration::from_secs(10);

/// The hop-by-hop headers, which a proxy does not pass on.
const HOP_BY_HOP: [&str; 8] = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// Where dockerd listens.
#[derive(Clone)]
pub struct Docker {
    socket: Arc<PathBuf>,
}

impl Default for Docker {
    fn default() -> Docker {
        Docker::new(DOCKER_SOCKET)
    }
}

impl Docker {
    pub fn new(socket: impl Into<PathBuf>) -> Docker {
        Docker {
            socket: Arc::new(socket.into()),
        }
    }

    fn socket(&self) -> &Path {
        &self.socket
    }

    async fn connect(&self) -> Result<http1::SendRequest<Body>, ApiError> {
        let stream = UnixStream::connect(self.socket())
            .await
            .map_err(|e| unavailable(self.socket(), e))?;
        let (sender, connection) = http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|e| unavailable(self.socket(), e))?;
        tokio::spawn(async move {
            if let Err(e) = connection.with_upgrades().await {
                debug!("a connection to dockerd ended: {e}");
            }
        });
        Ok(sender)
    }

    /// The subnets of dockerd's networks.
    async fn subnets(&self) -> Result<Vec<Subnet>, ApiError> {
        let lookup = async {
            let mut sender = self.connect().await?;
            let request = axum::http::Request::get("/networks")
                .header(HOST, "docker")
                .body(Body::empty())
                .map_err(|e| unavailable(self.socket(), e))?;
            let response = sender
                .send_request(request)
                .await
                .map_err(|e| unavailable(self.socket(), e))?;
            let status = response.status();
            let body = response
                .into_body()
                .collect()
                .await
                .map_err(|e| unavailable(self.socket(), e))?
                .to_bytes();
            if !status.is_success() {
                return Err(unavailable(
                    self.socket(),
                    format!("GET /networks answered {status}"),
                ));
            }
            let networks: Value = serde_json::from_slice(&body)
                .map_err(|e| unavailable(self.socket(), format!("GET /networks: {e}")))?;
            Ok(subnets_of(&networks))
        };
        tokio::time::timeout(NETWORKS_TIMEOUT, lookup)
            .await
            .unwrap_or_else(|_| Err(unavailable(self.socket(), "GET /networks timed out")))
    }
}

fn unavailable(socket: &Path, e: impl std::fmt::Display) -> ApiError {
    ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "unavailable",
        format!("dockerd at {} cannot be reached: {e}", socket.display()),
    )
}

/// Passes a request under [`PREFIX`] to dockerd, without the prefix and the
/// API's token, and streams its answer back. A request that asks to upgrade
/// the connection, and that dockerd upgrades, is spliced to dockerd's side.
pub async fn proxy(Extension(docker): Extension<Docker>, mut request: Request) -> Response {
    let target = request
        .uri()
        .path_and_query()
        .map(|p| p.as_str())
        .and_then(|p| p.strip_prefix(PREFIX))
        .filter(|rest| rest.starts_with('/'))
        .and_then(|rest| rest.parse::<Uri>().ok());
    let Some(target) = target else {
        return ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("no Docker API path under {PREFIX}"),
        )
        .into_response();
    };
    let upgrade = wants_upgrade(request.headers());
    let client_side = upgrade.then(|| hyper::upgrade::on(&mut request));
    let (mut parts, body) = request.into_parts();
    parts.uri = target;
    parts.version = Version::HTTP_11;
    strip_hop_by_hop(&mut parts.headers, upgrade);
    parts.headers.remove(AUTHORIZATION);
    parts
        .headers
        .insert(HOST, HeaderValue::from_static("docker"));
    let mut sender = match docker.connect().await {
        Ok(sender) => sender,
        Err(e) => return e.into_response(),
    };
    let method = parts.method.clone();
    let path = parts.uri.path().to_string();
    let mut response = match sender
        .send_request(axum::http::Request::from_parts(parts, body))
        .await
    {
        Ok(response) => response,
        Err(e) => return unavailable(docker.socket(), e).into_response(),
    };
    if response.status() == StatusCode::SWITCHING_PROTOCOLS {
        let Some(client_side) = client_side else {
            return ApiError::new(
                StatusCode::BAD_GATEWAY,
                "unavailable",
                "dockerd upgraded a connection that did not ask for it",
            )
            .into_response();
        };
        let docker_side = hyper::upgrade::on(&mut response);
        tokio::spawn(async move {
            splice(client_side, docker_side, &format!("{method} {path}")).await;
        });
        let (parts, _) = response.into_parts();
        return Response::from_parts(parts, Body::empty());
    }
    let (mut parts, body) = response.into_parts();
    strip_hop_by_hop(&mut parts.headers, false);
    Response::from_parts(parts, Body::new(body))
}

/// Opens a TCP tunnel to `to`, an address on one of dockerd's networks: the
/// node service connects to it, which wakes a sleeping container, then
/// answers 101 and relays the bytes both ways until either side closes.
pub async fn tunnel(Extension(docker): Extension<Docker>, mut request: Request) -> Response {
    match open_tunnel(&docker, &mut request).await {
        Ok(response) => response,
        Err(e) => e.into_response(),
    }
}

async fn open_tunnel(docker: &Docker, request: &mut Request) -> Result<Response, ApiError> {
    let to = tunnel_target(request.uri().query())?;
    if !wants_upgrade(request.headers()) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid",
            "a tunnel needs Connection: Upgrade and Upgrade: tcp",
        ));
    }
    let subnets = docker.subnets().await?;
    if !subnets.iter().any(|subnet| subnet.allows(to.ip())) {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "forbidden",
            format!(
                "{} is not a container address on the node's Docker networks",
                to.ip()
            ),
        ));
    }
    let tcp = match tokio::time::timeout(TUNNEL_CONNECT_TIMEOUT, TcpStream::connect(to)).await {
        Ok(Ok(tcp)) => tcp,
        Ok(Err(e)) => {
            return Err(ApiError::new(
                StatusCode::BAD_GATEWAY,
                "unreachable",
                format!("connect to {to}: {e}"),
            ))
        }
        Err(_) => {
            return Err(ApiError::new(
                StatusCode::GATEWAY_TIMEOUT,
                "unreachable",
                format!(
                    "connect to {to}: no answer in {} s",
                    TUNNEL_CONNECT_TIMEOUT.as_secs()
                ),
            ))
        }
    };
    if let Err(e) = tcp.set_nodelay(true) {
        warn!("TCP_NODELAY on a tunnel to {to}: {e}");
    }
    let client_side = hyper::upgrade::on(request);
    tokio::spawn(async move {
        match client_side.await {
            Ok(upgraded) => {
                let mut client = TokioIo::new(upgraded);
                let mut target = tcp;
                match tokio::io::copy_bidirectional(&mut client, &mut target).await {
                    Ok((up, down)) => {
                        debug!("the tunnel to {to} closed: {up} bytes up, {down} down")
                    }
                    Err(e) => debug!("the tunnel to {to} closed: {e}"),
                }
            }
            Err(e) => warn!("the tunnel to {to} was not upgraded: {e}"),
        }
    });
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    let headers = response.headers_mut();
    headers.insert(CONNECTION, HeaderValue::from_static("Upgrade"));
    headers.insert(UPGRADE, HeaderValue::from_static("tcp"));
    Ok(response)
}

/// The `to=IP:PORT` of a tunnel's query.
fn tunnel_target(query: Option<&str>) -> Result<SocketAddr, ApiError> {
    let invalid = |message: String| ApiError::new(StatusCode::BAD_REQUEST, "invalid", message);
    let mut to = None;
    for pair in query
        .unwrap_or_default()
        .split('&')
        .filter(|p| !p.is_empty())
    {
        match pair.split_once('=') {
            Some(("to", value)) if to.is_none() => to = Some(percent_decode(value)),
            Some(("to", _)) => return Err(invalid("to is given twice".into())),
            _ => return Err(invalid(format!("unknown query parameter {pair:?}"))),
        }
    }
    let to = to.ok_or_else(|| invalid("a tunnel needs to=IP:PORT".into()))?;
    let addr: SocketAddr = to
        .parse()
        .map_err(|_| invalid(format!("to={to:?} is not IP:PORT")))?;
    if addr.port() == 0 {
        return Err(invalid(format!("to={to:?} has port 0")));
    }
    Ok(addr)
}

/// Decodes `%XX` escapes, as a client may write `:` and the brackets of an
/// IPv6 address.
fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = bytes
            .get(i + 1..i + 3)
            .and_then(|h| std::str::from_utf8(h).ok())
            .and_then(|h| u8::from_str_radix(h, 16).ok());
        match (bytes[i], hex) {
            (b'%', Some(byte)) => {
                out.push(byte);
                i += 3;
            }
            (byte, _) => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Whether a request asks to upgrade its connection: `Connection: Upgrade`
/// with an `Upgrade` header.
fn wants_upgrade(headers: &HeaderMap) -> bool {
    let connection_upgrade = headers.get_all(CONNECTION).iter().any(|value| {
        value
            .to_str()
            .unwrap_or_default()
            .split(',')
            .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
    });
    connection_upgrade && headers.contains_key(UPGRADE)
}

/// Removes the hop-by-hop headers and those that `Connection` names; an
/// upgrade keeps `Connection` and `Upgrade`.
fn strip_hop_by_hop(headers: &mut HeaderMap, upgrade: bool) {
    let named: Vec<String> = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|token| token.trim().to_ascii_lowercase())
        .filter(|token| !token.is_empty() && token != "upgrade")
        .collect();
    for name in named {
        headers.remove(name.as_str());
    }
    for name in HOP_BY_HOP {
        if upgrade && (name == "connection" || name == "upgrade") {
            continue;
        }
        headers.remove(name);
    }
    if upgrade {
        headers.insert(CONNECTION, HeaderValue::from_static("Upgrade"));
    }
}

/// Relays an upgraded connection between the API's client and dockerd.
async fn splice(client: OnUpgrade, docker: OnUpgrade, what: &str) {
    let (client, docker) = match tokio::try_join!(client, docker) {
        Ok(both) => both,
        Err(e) => {
            warn!("the upgrade of {what} through the Docker proxy failed: {e}");
            return;
        }
    };
    let (mut client, mut docker) = (TokioIo::new(client), TokioIo::new(docker));
    if let Err(e) = tokio::io::copy_bidirectional(&mut client, &mut docker).await {
        debug!("the upgraded connection of {what} closed: {e}");
    }
}

/// A subnet of a Docker network, and its gateway, which is the node itself.
#[derive(Debug, PartialEq)]
struct Subnet {
    network: IpAddr,
    prefix: u8,
    gateway: Option<IpAddr>,
}

impl Subnet {
    fn parse(cidr: &str, gateway: Option<&str>) -> Option<Subnet> {
        let (address, prefix) = cidr.split_once('/')?;
        let network: IpAddr = address.parse().ok()?;
        let prefix: u8 = prefix.parse().ok()?;
        let bits = if network.is_ipv4() { 32 } else { 128 };
        if prefix > bits {
            return None;
        }
        Some(Subnet {
            network,
            prefix,
            gateway: gateway.and_then(|g| g.parse().ok()),
        })
    }

    /// Whether a tunnel may go to `ip`: a host address in the subnet that is
    /// not its gateway.
    fn allows(&self, ip: IpAddr) -> bool {
        if Some(ip) == self.gateway {
            return false;
        }
        let (bits, host, network) = match (ip, self.network) {
            (IpAddr::V4(ip), IpAddr::V4(net)) => {
                (32, u128::from(ip.to_bits()), u128::from(net.to_bits()))
            }
            (IpAddr::V6(ip), IpAddr::V6(net)) => (128, ip.to_bits(), net.to_bits()),
            _ => return false,
        };
        let host_bits = bits - u32::from(self.prefix);
        let mask = if host_bits >= 128 {
            0
        } else {
            (u128::MAX << host_bits) & (u128::MAX >> (128 - bits))
        };
        if host & mask != network & mask {
            return false;
        }
        let suffix = host & !mask & (u128::MAX >> (128 - bits));
        let all_ones = if host_bits == 0 {
            0
        } else {
            u128::MAX >> (128 - host_bits)
        };
        // The network's own address and, in IPv4, its broadcast address are
        // no container's, except in the subnets too small to have them.
        host_bits < 2 || (suffix != 0 && (bits == 128 || suffix != all_ones))
    }
}

/// The subnets of the networks of `GET /networks`.
fn subnets_of(networks: &Value) -> Vec<Subnet> {
    let mut subnets = Vec::new();
    for network in networks.as_array().into_iter().flatten() {
        for config in network["IPAM"]["Config"].as_array().into_iter().flatten() {
            let Some(cidr) = config["Subnet"].as_str() else {
                continue;
            };
            match Subnet::parse(cidr, config["Gateway"].as_str()) {
                Some(subnet) => subnets.push(subnet),
                None => warn!(
                    "the Docker network {} has a subnet that cannot be read: {cidr}",
                    network["Name"]
                ),
            }
        }
    }
    subnets
}

#[cfg(test)]
mod tests;
