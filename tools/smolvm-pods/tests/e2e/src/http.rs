//! HTTP on the Mac: a stand-in for Floci, which pods call, a server that
//! pushes a stream to the pods that call it, and a client.

use std::hash::{BuildHasher, RandomState};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// An HTTP server on the Mac's loopback that stands in for Floci. It answers
/// every request with `name=floci-stand-in`, its token and
/// [`StandIn::PADDING`] bytes of padding, one `key=value` pair per line, so
/// that the answer takes full-size TCP segments. Dropping it stops it.
pub struct StandIn {
    port: u16,
    token: String,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl StandIn {
    /// Bytes of padding in each answer.
    pub const PADDING: usize = 64 * 1024;

    /// Starts the server on a free port.
    pub fn start() -> StandIn {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("listen on the Mac's loopback");
        listener
            .set_nonblocking(true)
            .expect("make the listener nonblocking");
        let port = listener.local_addr().expect("the listener's port").port();
        let token = format!("{:016x}", RandomState::new().hash_one(port));
        let body = format!(
            "name=floci-stand-in\ntoken={token}\npadding={}\n",
            "x".repeat(StandIn::PADDING)
        );
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let thread = thread::spawn(move || {
            while !stopped.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let _ = answer(stream, &body);
                    }
                    Err(_) => thread::sleep(Duration::from_millis(20)),
                }
            }
        });
        StandIn {
            port,
            token,
            stop,
            thread: Some(thread),
        }
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn token(&self) -> &str {
        &self.token
    }
}

impl Drop for StandIn {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// An HTTP server on the Mac's loopback that answers every request with a
/// stream: `chunk=N` and `chunk_ms=MS` every `every` for `total`, then
/// `chunks=N`, as the test app's `/stream` does, on a connection that the
/// caller opened. Dropping it stops it and ends the streams.
pub struct Pusher {
    port: u16,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Pusher {
    /// Starts the server on a free port.
    pub fn start(every: Duration, total: Duration) -> Pusher {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("listen on the Mac's loopback");
        listener
            .set_nonblocking(true)
            .expect("make the listener nonblocking");
        let port = listener.local_addr().expect("the listener's port").port();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let thread = thread::spawn(move || {
            let mut streams = Vec::new();
            while !stopped.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let stopped = stopped.clone();
                        streams.push(thread::spawn(move || {
                            let _ = push(stream, every, total, &stopped);
                        }));
                    }
                    Err(_) => thread::sleep(Duration::from_millis(20)),
                }
            }
            for stream in streams {
                let _ = stream.join();
            }
        });
        Pusher {
            port,
            stop,
            thread: Some(thread),
        }
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

impl Drop for Pusher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Reads the head of a request and answers with the stream until `total`
/// passed or `stop` is set.
fn push(
    mut stream: TcpStream,
    every: Duration,
    total: Duration,
    stop: &AtomicBool,
) -> io::Result<()> {
    read_head(&mut stream)?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n"
    )?;
    let start = Instant::now();
    let mut chunks = 0;
    for next in (0..).map(|n| start + every * n) {
        if next > start + total {
            break;
        }
        while Instant::now() < next {
            if stop.load(Ordering::SeqCst) {
                return Ok(());
            }
            thread::sleep((next - Instant::now()).min(Duration::from_millis(100)));
        }
        chunks += 1;
        write!(
            stream,
            "chunk={chunks}\nchunk_ms={}\n",
            start.elapsed().as_millis()
        )?;
    }
    writeln!(stream, "chunks={chunks}")
}

/// Reads the head of a request, blocking.
fn read_head(stream: &mut TcpStream) -> io::Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut head = Vec::new();
    let mut buf = [0; 4096];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        head.extend_from_slice(&buf[..n]);
    }
    Ok(())
}

/// Reads the head of a request and answers with `body`.
fn answer(mut stream: TcpStream, body: &str) -> io::Result<()> {
    read_head(&mut stream)?;
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    )
}

/// GETs `path` from `addr` with HTTP/1.0 and returns the status code and the
/// body.
pub fn http_get(addr: SocketAddr, path: &str, timeout: Duration) -> Result<(u16, String), String> {
    http_get_with(addr, path, &[], timeout)
}

/// Like [`http_get`], with `headers` in the request.
pub fn http_get_with(
    addr: SocketAddr,
    path: &str,
    headers: &[(&str, &str)],
    timeout: Duration,
) -> Result<(u16, String), String> {
    let url = format!("http://{addr}{path}");
    let failed = |e: io::Error| format!("GET {url}: {e}");
    let mut stream = TcpStream::connect_timeout(&addr, timeout).map_err(failed)?;
    stream.set_read_timeout(Some(timeout)).map_err(failed)?;
    stream.set_write_timeout(Some(timeout)).map_err(failed)?;
    let headers: String = headers
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect();
    write!(
        stream,
        "GET {path} HTTP/1.0\r\nHost: {addr}\r\n{headers}\r\n"
    )
    .map_err(failed)?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).map_err(failed)?;
    let response = String::from_utf8_lossy(&response);
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("GET {url}: not an HTTP response: {response:?}"))?;
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|status| status.parse().ok())
        .ok_or_else(|| format!("GET {url}: no status in {head:?}"))?;
    Ok((status, body.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::State;

    #[test]
    fn the_stand_in_answers_with_its_token_and_padding() {
        let floci = StandIn::start();
        let addr = SocketAddr::from(([127, 0, 0, 1], floci.port()));
        for _ in 0..2 {
            let (status, body) = http_get(addr, "/", Duration::from_secs(5)).unwrap();
            assert_eq!(status, 200);
            let state = State::parse(&body);
            assert_eq!(&state["name"], "floci-stand-in");
            assert_eq!(&state["token"], floci.token());
            assert_eq!(state["padding"].len(), StandIn::PADDING);
        }
        drop(floci);
        assert!(http_get(addr, "/", Duration::from_secs(1)).is_err());
    }

    #[test]
    fn the_pusher_streams_chunks_on_each_connection() {
        let pusher = Pusher::start(Duration::from_millis(50), Duration::from_millis(200));
        let addr = SocketAddr::from(([127, 0, 0, 1], pusher.port()));
        for _ in 0..2 {
            let (status, body) = http_get(addr, "/", Duration::from_secs(5)).unwrap();
            assert_eq!(status, 200);
            let chunks: Vec<&str> = body.lines().filter(|l| l.starts_with("chunk=")).collect();
            assert_eq!(
                chunks,
                ["chunk=1", "chunk=2", "chunk=3", "chunk=4", "chunk=5"]
            );
            assert_eq!(body.lines().last(), Some("chunks=5"));
        }
        // A stream still open when the server stops ends.
        let long = Pusher::start(Duration::from_millis(50), Duration::from_secs(60));
        let addr = SocketAddr::from(([127, 0, 0, 1], long.port()));
        let client = thread::spawn(move || http_get(addr, "/", Duration::from_secs(5)));
        thread::sleep(Duration::from_millis(200));
        let stopped = Instant::now();
        drop(long);
        assert!(stopped.elapsed() < Duration::from_secs(1));
        let (_, body) = client.join().unwrap().unwrap();
        assert!(body.starts_with("chunk=1\n"), "{body:?}");
    }
}
