//! The sinkhole on the node address's port [`SINK_PORT`], where nftables
//! sends every connection and datagram that a pod may not make. A TCP
//! connection is accepted, its server name or `Host` read for a moment, and
//! reset; a datagram is dropped. Each attempt is a [`Kind::Blocked`] event
//! with the pod and the original destination.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::os::fd::AsFd;
use std::sync::Arc;
use std::time::Duration;

use log::warn;
use tokio::net::{TcpListener, UdpSocket};

use super::events::{Detail, Key, Kind};
use super::sniff::{sniff, Hello};
use super::{original_dst, Shared, SINK_PORT};
use crate::activity::{ConnEntry, Ctnetlink, Direction, Protocol, Tuple};

/// How long the sinkhole reads a blocked connection for its name.
const SNIFF_FOR: Duration = Duration::from_millis(300);

fn record(
    shared: &Shared,
    src: Ipv4Addr,
    dst: SocketAddrV4,
    protocol: &'static str,
    name: Option<String>,
) {
    shared.events.record(
        Key {
            kind: Kind::Blocked,
            pod: shared.pod_name(src),
            destination: dst.ip().to_string(),
            port: dst.port(),
            protocol,
        },
        Detail {
            name,
            ..Detail::default()
        },
    );
}

pub async fn serve(shared: Arc<Shared>) {
    let addr = SocketAddrV4::new(shared.node.address, SINK_PORT);
    let tcp = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            warn!("the sinkhole cannot listen on TCP {addr}: {e}");
            return;
        }
    };
    match UdpSocket::bind(addr).await {
        Ok(udp) => {
            tokio::spawn(datagrams(shared.clone(), udp));
        }
        Err(e) => warn!("the sinkhole cannot listen on UDP {addr}: {e}"),
    }
    loop {
        let (stream, peer) = match tcp.accept().await {
            Ok(got) => got,
            Err(e) => {
                warn!("the sinkhole's listener: {e}");
                return;
            }
        };
        let shared = shared.clone();
        tokio::spawn(async move {
            let SocketAddr::V4(peer) = peer else {
                return;
            };
            let dst = original_dst(stream.as_fd()).unwrap_or(addr);
            let mut buf = vec![0u8; 4096];
            let deadline = tokio::time::Instant::now() + SNIFF_FOR;
            let mut name = None;
            while let Ok(Ok(n)) = tokio::time::timeout_at(deadline, stream.peek(&mut buf)).await {
                if n == 0 {
                    break;
                }
                match sniff(&buf[..n]) {
                    Hello::Tls(found) | Hello::Http(found) => {
                        name = found;
                        break;
                    }
                    Hello::More if n < buf.len() => {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    _ => break,
                }
            }
            record(&shared, *peer.ip(), dst, "tcp", name);
            // A reset, not a FIN, so that the client fails at once.
            #[allow(deprecated)]
            let _ = stream.set_linger(Some(Duration::ZERO));
        });
    }
}

async fn datagrams(shared: Arc<Shared>, udp: UdpSocket) {
    let ct = Arc::new(Ctnetlink::default());
    let local = SocketAddrV4::new(shared.node.address, SINK_PORT);
    let mut buf = vec![0u8; 2048];
    loop {
        let peer = match udp.recv_from(&mut buf).await {
            Ok((_, SocketAddr::V4(peer))) => peer,
            Ok(_) => continue,
            Err(e) => {
                warn!("the sinkhole's UDP socket: {e}");
                return;
            }
        };
        let (shared, ct) = (shared.clone(), ct.clone());
        tokio::spawn(async move {
            // The reply tuple of the DNATed flow runs from the sinkhole to
            // the pod; its original tuple names where the pod sent it.
            let reply = Tuple {
                protocol: Protocol::Udp,
                src: local,
                dst: peer,
            };
            let entry: Option<ConnEntry> = tokio::task::spawn_blocking(move || {
                ct.get(Direction::Reply, &reply).ok().flatten()
            })
            .await
            .ok()
            .flatten()
            .map(|conn| conn.entry);
            let dst = entry.map_or(local, |e| e.original.dst);
            record(&shared, *peer.ip(), dst, "udp", None);
        });
    }
}
