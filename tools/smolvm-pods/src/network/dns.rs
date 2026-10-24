//! The node's DNS forwarder, on the node address's port [`DNS_PORT`], where
//! CoreDNS and node processes send what the cluster does not answer.
//!
//! | Profile | Isolation | Answers from |
//! | --- | --- | --- |
//! | none | off | the node's original resolver |
//! | none | on | nothing: REFUSED |
//! | one | off | Floci, else the original resolver |
//! | one | on | Floci only, else SERVFAIL |
//!
//! Floci is asked over one kept TCP connection, with the query ids rewritten
//! so that queries from every client share it (RFC 7766).

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use log::{debug, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::oneshot;

use super::{Shared, DNS_PORT};
use crate::sync::lock;

/// How long a query waits for Floci or the original resolver.
const TIMEOUT: Duration = Duration::from_secs(2);

/// Where the node's resolver was before `floci-node` pointed it at the node
/// address.
pub const UPSTREAM_FILE: &str = "/etc/floci-node/upstream-resolv.conf";

pub const RCODE_SERVFAIL: u8 = 2;
#[cfg(test)]
pub const RCODE_NXDOMAIN: u8 = 3;
pub const RCODE_REFUSED: u8 = 5;

/// An answer to `query` with only its header and question, and `rcode`.
pub fn error_answer(query: &[u8], rcode: u8) -> Option<Vec<u8>> {
    if query.len() < 12 {
        return None;
    }
    let question_end = question_end(query)?;
    let mut answer = query[..question_end].to_vec();
    // QR, the query's opcode and RD, RA, and the code.
    answer[2] = 0x80 | (query[2] & 0x79);
    answer[3] = 0x80 | (rcode & 0x0f);
    // One question, no records.
    answer[4..12].copy_from_slice(&[0, 1, 0, 0, 0, 0, 0, 0]);
    Some(answer)
}

/// Where the first question ends, if the message has one.
fn question_end(message: &[u8]) -> Option<usize> {
    if u16::from_be_bytes([message[4], message[5]]) == 0 {
        return Some(12);
    }
    let mut i = 12;
    loop {
        let len = usize::from(*message.get(i)?);
        if len == 0 {
            i += 1;
            break;
        }
        if len & 0xc0 != 0 {
            return None;
        }
        i += 1 + len;
    }
    (i + 4 <= message.len()).then_some(i + 4)
}

/// A query for the A records of `name`, with id `id`.
pub fn a_query(name: &str, id: u16) -> Vec<u8> {
    let mut q = Vec::with_capacity(32 + name.len());
    q.extend_from_slice(&id.to_be_bytes());
    q.extend_from_slice(&[0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0]);
    for label in name.trim_end_matches('.').split('.') {
        q.push(label.len() as u8);
        q.extend_from_slice(label.as_bytes());
    }
    q.extend_from_slice(&[0, 0, 1, 0, 1]);
    q
}

/// The A records of an answer.
pub fn a_records(answer: &[u8]) -> Vec<Ipv4Addr> {
    let mut out = Vec::new();
    if answer.len() < 12 || answer[3] & 0x0f != 0 {
        return out;
    }
    let count = u16::from_be_bytes([answer[6], answer[7]]);
    let Some(mut i) = question_end(answer) else {
        return out;
    };
    for _ in 0..count {
        // The name: labels, or a pointer.
        loop {
            let Some(&len) = answer.get(i) else {
                return out;
            };
            if len & 0xc0 == 0xc0 {
                i += 2;
                break;
            }
            i += 1 + usize::from(len);
            if len == 0 {
                break;
            }
        }
        let Some(rr) = answer.get(i..i + 10) else {
            return out;
        };
        let (kind, len) = (
            u16::from_be_bytes([rr[0], rr[1]]),
            usize::from(u16::from_be_bytes([rr[8], rr[9]])),
        );
        i += 10;
        if kind == 1 && len == 4 {
            if let Some(a) = answer.get(i..i + 4) {
                out.push(Ipv4Addr::new(a[0], a[1], a[2], a[3]));
            }
        }
        i += len;
    }
    out
}

/// Asks `server` over UDP.
pub async fn ask_udp(server: SocketAddr, query: &[u8]) -> Result<Vec<u8>, String> {
    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .map_err(|e| format!("bind: {e}"))?;
    socket
        .send_to(query, server)
        .await
        .map_err(|e| format!("send to {server}: {e}"))?;
    let mut buf = vec![0u8; 4096];
    loop {
        let (n, from) = tokio::time::timeout(TIMEOUT, socket.recv_from(&mut buf))
            .await
            .map_err(|_| format!("{server} did not answer in {TIMEOUT:?}"))?
            .map_err(|e| format!("receive: {e}"))?;
        if from == server && n >= 2 && buf[..2] == query[..2] {
            return Ok(buf[..n].to_vec());
        }
    }
}

/// The first nameserver of a resolv.conf.
pub fn upstream_of(text: &str) -> Option<SocketAddr> {
    text.lines().find_map(|line| {
        let mut words = line.split_whitespace();
        (words.next() == Some("nameserver"))
            .then(|| words.next()?.parse::<Ipv4Addr>().ok())
            .flatten()
            .map(|ip| SocketAddr::from((ip, 53)))
    })
}

type Pending = Arc<Mutex<HashMap<u16, (u16, oneshot::Sender<Vec<u8>>)>>>;

struct Conn {
    server: SocketAddr,
    writer: tokio::net::tcp::OwnedWriteHalf,
    /// Cleared when Floci closes the connection.
    open: Arc<std::sync::atomic::AtomicBool>,
}

/// The kept connection to Floci's node DNS.
#[derive(Default)]
pub struct FlociDns {
    conn: tokio::sync::Mutex<Option<Conn>>,
    pending: Pending,
    next_id: AtomicU16,
}

impl FlociDns {
    /// Floci's answer to `query`, asked at `server`.
    pub async fn ask(&self, server: SocketAddr, query: &[u8]) -> Result<Vec<u8>, String> {
        if query.len() < 12 {
            return Err("a query of less than 12 bytes".into());
        }
        let original = u16::from_be_bytes([query[0], query[1]]);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        lock(&self.pending).insert(id, (original, tx));
        let mut framed = Vec::with_capacity(query.len() + 2);
        framed.extend_from_slice(&(query.len() as u16).to_be_bytes());
        framed.extend_from_slice(&id.to_be_bytes());
        framed.extend_from_slice(&query[2..]);
        let sent = self.send(server, &framed).await;
        if let Err(e) = sent {
            lock(&self.pending).remove(&id);
            return Err(e);
        }
        match tokio::time::timeout(TIMEOUT, rx).await {
            Ok(Ok(answer)) => Ok(answer),
            Ok(Err(_)) => Err(format!("the connection to {server} closed")),
            Err(_) => {
                lock(&self.pending).remove(&id);
                Err(format!("{server} did not answer in {TIMEOUT:?}"))
            }
        }
    }

    async fn send(&self, server: SocketAddr, framed: &[u8]) -> Result<(), String> {
        let mut conn = self.conn.lock().await;
        if let Some(c) = conn.as_mut() {
            if c.server == server
                && c.open.load(Ordering::Relaxed)
                && c.writer.write_all(framed).await.is_ok()
            {
                return Ok(());
            }
        }
        *conn = None;
        let stream = tokio::time::timeout(TIMEOUT, TcpStream::connect(server))
            .await
            .map_err(|_| format!("connect to {server}: timed out"))?
            .map_err(|e| format!("connect to {server}: {e}"))?;
        let _ = stream.set_nodelay(true);
        let (reader, mut writer) = stream.into_split();
        writer
            .write_all(framed)
            .await
            .map_err(|e| format!("write to {server}: {e}"))?;
        let open = Arc::new(std::sync::atomic::AtomicBool::new(true));
        tokio::spawn(read_answers(reader, self.pending.clone(), open.clone()));
        *conn = Some(Conn {
            server,
            writer,
            open,
        });
        Ok(())
    }
}

async fn read_answers(
    mut reader: tokio::net::tcp::OwnedReadHalf,
    pending: Pending,
    open: Arc<std::sync::atomic::AtomicBool>,
) {
    loop {
        let mut len = [0u8; 2];
        if reader.read_exact(&mut len).await.is_err() {
            break;
        }
        let mut answer = vec![0u8; usize::from(u16::from_be_bytes(len))];
        if reader.read_exact(&mut answer).await.is_err() || answer.len() < 12 {
            break;
        }
        let id = u16::from_be_bytes([answer[0], answer[1]]);
        if let Some((original, tx)) = lock(&pending).remove(&id) {
            answer[..2].copy_from_slice(&original.to_be_bytes());
            let _ = tx.send(answer);
        }
    }
    // Queries still waiting on this connection fail at their timeout.
    open.store(false, Ordering::Relaxed);
}

/// The answer to one query from a client of the forwarder.
async fn answer(shared: &Shared, query: &[u8]) -> Option<Vec<u8>> {
    let floci = shared.floci_dns();
    let isolation = shared.effective_isolation();
    let upstream = || async {
        let text = std::fs::read_to_string(UPSTREAM_FILE).ok()?;
        let server = upstream_of(&text)?;
        match ask_udp(server, query).await {
            Ok(answer) => Some(answer),
            Err(e) => {
                debug!("the node's resolver: {e}");
                None
            }
        }
    };
    match (floci, isolation) {
        (None, false) => match upstream().await {
            Some(answer) => Some(answer),
            None => error_answer(query, RCODE_SERVFAIL),
        },
        (None, true) => error_answer(query, RCODE_REFUSED),
        (Some(server), isolation) => match shared.dns.ask(server, query).await {
            Ok(answer) => Some(answer),
            Err(e) if isolation => {
                debug!("Floci's DNS: {e}");
                error_answer(query, RCODE_SERVFAIL)
            }
            Err(e) => {
                debug!("Floci's DNS, so the node's resolver: {e}");
                match upstream().await {
                    Some(answer) => Some(answer),
                    None => error_answer(query, RCODE_SERVFAIL),
                }
            }
        },
    }
}

/// Serves DNS over UDP and TCP on the node address until either listener
/// fails.
pub async fn serve(shared: Arc<Shared>) {
    let addr = SocketAddrV4::new(shared.node.address, DNS_PORT);
    let udp = match UdpSocket::bind(addr).await {
        Ok(socket) => Arc::new(socket),
        Err(e) => {
            warn!("the DNS forwarder cannot listen on UDP {addr}: {e}");
            return;
        }
    };
    let tcp = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            warn!("the DNS forwarder cannot listen on TCP {addr}: {e}");
            return;
        }
    };
    let udp_shared = shared.clone();
    let udp_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 4096];
        loop {
            let (n, client) = match udp.recv_from(&mut buf).await {
                Ok(got) => got,
                Err(e) => {
                    warn!("the DNS forwarder's UDP socket: {e}");
                    return;
                }
            };
            let query = buf[..n].to_vec();
            let (shared, udp) = (udp_shared.clone(), udp.clone());
            tokio::spawn(async move {
                if let Some(answer) = answer(&shared, &query).await {
                    let _ = udp.send_to(&answer, client).await;
                }
            });
        }
    });
    loop {
        let (stream, _) = match tcp.accept().await {
            Ok(got) => got,
            Err(e) => {
                warn!("the DNS forwarder's TCP listener: {e}");
                break;
            }
        };
        let shared = shared.clone();
        tokio::spawn(async move {
            let (mut reader, writer) = stream.into_split();
            let writer = Arc::new(tokio::sync::Mutex::new(writer));
            loop {
                let mut len = [0u8; 2];
                if reader.read_exact(&mut len).await.is_err() {
                    return;
                }
                let mut query = vec![0u8; usize::from(u16::from_be_bytes(len))];
                if reader.read_exact(&mut query).await.is_err() {
                    return;
                }
                let (shared, writer) = (shared.clone(), writer.clone());
                tokio::spawn(async move {
                    if let Some(answer) = answer(&shared, &query).await {
                        let mut framed = (answer.len() as u16).to_be_bytes().to_vec();
                        framed.extend_from_slice(&answer);
                        let _ = writer.lock().await.write_all(&framed).await;
                    }
                });
            }
        });
    }
    udp_task.abort();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_error_answer_keeps_the_id_and_question() {
        let query = a_query("api.vendor.test", 0xbeef);
        let answer = error_answer(&query, RCODE_REFUSED).unwrap();
        assert_eq!(&answer[..2], &[0xbe, 0xef]);
        assert_eq!(answer[2] & 0x80, 0x80, "QR");
        assert_eq!(answer[3] & 0x0f, RCODE_REFUSED);
        assert_eq!(&answer[12..], &query[12..]);
        assert!(a_records(&answer).is_empty());
    }

    #[test]
    fn a_records_are_read_through_pointers() {
        let mut answer = a_query("orders.default.svc.cluster.local", 7);
        answer[2] = 0x81;
        answer[3] = 0x80;
        answer[7] = 2;
        for ip in [[10, 43, 0, 7], [10, 43, 0, 8]] {
            answer.extend_from_slice(&[0xc0, 12, 0, 1, 0, 1, 0, 0, 0, 5, 0, 4]);
            answer.extend_from_slice(&ip);
        }
        assert_eq!(
            a_records(&answer),
            vec![Ipv4Addr::new(10, 43, 0, 7), Ipv4Addr::new(10, 43, 0, 8)]
        );
    }

    #[test]
    fn the_upstream_is_the_first_nameserver() {
        let text = "# from smolvm\nsearch local\nnameserver 100.96.0.1\nnameserver 8.8.8.8\n";
        assert_eq!(upstream_of(text), Some("100.96.0.1:53".parse().unwrap()));
        assert_eq!(upstream_of("search local\n"), None);
    }

    #[tokio::test]
    async fn floci_is_asked_over_one_connection_with_ids_rewritten() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = listener.local_addr().unwrap();
        let accepts = Arc::new(AtomicU16::new(0));
        let counted = accepts.clone();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                counted.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    loop {
                        let mut len = [0u8; 2];
                        if stream.read_exact(&mut len).await.is_err() {
                            return;
                        }
                        let mut q = vec![0u8; usize::from(u16::from_be_bytes(len))];
                        stream.read_exact(&mut q).await.unwrap();
                        let a = error_answer(&q, RCODE_NXDOMAIN).unwrap();
                        let mut framed = (a.len() as u16).to_be_bytes().to_vec();
                        framed.extend_from_slice(&a);
                        stream.write_all(&framed).await.unwrap();
                    }
                });
            }
        });
        let dns = Arc::new(FlociDns::default());
        let mut tasks = Vec::new();
        for id in [1u16, 1, 2, 3] {
            let dns = dns.clone();
            tasks.push(tokio::spawn(async move {
                let answer = dns.ask(server, &a_query("x.test", id)).await.unwrap();
                (
                    id,
                    u16::from_be_bytes([answer[0], answer[1]]),
                    answer[3] & 0x0f,
                )
            }));
        }
        for task in tasks {
            let (id, got, rcode) = task.await.unwrap();
            assert_eq!((got, rcode), (id, RCODE_NXDOMAIN));
        }
        assert_eq!(accepts.load(Ordering::Relaxed), 1);
    }
}
