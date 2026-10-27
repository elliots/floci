//! The node's log files through the API: `GET /v1/logs/{log}` follows one
//! of a fixed set of files line by line, from its end, across its rotations.
//! Floci ships the EKS control plane's logs to CloudWatch Logs from it.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::fs::File;
use std::io::{self, ErrorKind, Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{Extension, Path as UrlPath};
use axum::http::header::CONTENT_TYPE;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use log::warn;
use tokio::time::Instant;

use super::{ApiError, FOLLOW_KEEPALIVE};

/// k3s's log, which `k3s server --log` writes and rotates.
pub const K3S_LOG: &str = "/var/log/k3s.log";

/// kube-apiserver's audit log, at the `audit-log-path` that Floci passes.
pub const AUDIT_LOG: &str = "/var/log/kube-apiserver-audit.log";

/// How often a follow looks for new lines and for a rotation.
const POLL: Duration = Duration::from_millis(250);

/// The longest line sent: a longer one is split at this size.
const MAX_LINE: usize = 256 * 1024;

/// The most one read of the file takes before its lines are sent.
const READ_LIMIT: usize = 1024 * 1024;

/// The files that `GET /v1/logs/{log}` follows, by name.
#[derive(Clone)]
pub struct NodeLogs {
    k3s: Arc<PathBuf>,
    audit: Arc<PathBuf>,
}

impl Default for NodeLogs {
    fn default() -> NodeLogs {
        NodeLogs::new(K3S_LOG, AUDIT_LOG)
    }
}

impl NodeLogs {
    pub fn new(k3s: impl Into<PathBuf>, audit: impl Into<PathBuf>) -> NodeLogs {
        NodeLogs {
            k3s: Arc::new(k3s.into()),
            audit: Arc::new(audit.into()),
        }
    }

    fn path(&self, log: &str) -> Option<&Path> {
        match log {
            "k3s" => Some(&self.k3s),
            "kube-apiserver-audit" => Some(&self.audit),
            _ => None,
        }
    }
}

/// `GET /v1/logs/{log}`: each line written to the log from now on, as it is
/// written, until the client goes. A blank line comes after
/// [`FOLLOW_KEEPALIVE`] without a line.
pub async fn follow(
    Extension(logs): Extension<NodeLogs>,
    UrlPath(log): UrlPath<String>,
) -> Result<Response, ApiError> {
    let path = logs.path(&log).ok_or_else(|| {
        ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("no node log {log}: the node logs are k3s and kube-apiserver-audit"),
        )
    })?;
    let tail = Tail::new(path.to_path_buf());
    let lines = futures_util::stream::unfold(tail, |mut tail| async move {
        let line = tail.next().await?;
        Some((Ok::<Bytes, Infallible>(line), tail))
    });
    Ok((
        [(CONTENT_TYPE, "text/plain; charset=utf-8")],
        Body::from_stream(lines),
    )
        .into_response())
}

/// A follower of a log file. Its reads run off the async runtime.
struct Tail {
    /// None while a read runs, and after a failed one.
    reader: Option<Reader>,
    lines: VecDeque<Bytes>,
    keepalive: Duration,
    poll: Duration,
}

impl Tail {
    fn new(path: PathBuf) -> Tail {
        Tail {
            reader: Some(Reader::new(path)),
            lines: VecDeque::new(),
            keepalive: FOLLOW_KEEPALIVE,
            poll: POLL,
        }
    }

    /// The next line, or a blank line after a quiet while. None ends the
    /// stream.
    async fn next(&mut self) -> Option<Bytes> {
        let deadline = Instant::now() + self.keepalive;
        loop {
            if let Some(line) = self.lines.pop_front() {
                return Some(line);
            }
            let mut reader = self.reader.take()?;
            let read = tokio::task::spawn_blocking(move || {
                let mut lines = Vec::new();
                let read = reader.read(&mut lines);
                (reader, lines, read)
            })
            .await;
            let (reader, lines, read) = match read {
                Ok(read) => read,
                Err(e) => {
                    warn!("a follow of a node log ended: {e}");
                    return None;
                }
            };
            self.lines.extend(lines);
            let more = match read {
                Ok(more) => more,
                Err(e) => {
                    warn!("a follow of {} ended: {e}", reader.path.display());
                    return None;
                }
            };
            self.reader = Some(reader);
            if !self.lines.is_empty() || more {
                continue;
            }
            let now = Instant::now();
            if now >= deadline {
                return Some(Bytes::from_static(b"\n"));
            }
            tokio::time::sleep(self.poll.min(deadline - now)).await;
        }
    }
}

/// An open log file.
struct Open {
    file: File,
    /// The file's device and inode.
    id: (u64, u64),
    offset: u64,
}

/// Reads a log file's lines. Blocking.
struct Reader {
    path: PathBuf,
    open: Option<Open>,
    /// Whether the next open starts at the file's end: only the first, so
    /// that a file created later, or after a rotation, is read from its start.
    at_end: bool,
    /// The start of a line whose newline has not been written yet.
    partial: Vec<u8>,
}

impl Reader {
    fn new(path: PathBuf) -> Reader {
        Reader {
            path,
            open: None,
            at_end: true,
            partial: Vec::new(),
        }
    }

    /// Reads what was written since the last read into `lines`. Returns
    /// whether there is more to read now.
    fn read(&mut self, lines: &mut Vec<Bytes>) -> io::Result<bool> {
        let at_end = std::mem::replace(&mut self.at_end, false);
        if self.open.is_none() {
            let mut file = match File::open(&self.path) {
                Ok(file) => file,
                Err(e) if e.kind() == ErrorKind::NotFound => return Ok(false),
                Err(e) => return Err(e),
            };
            let meta = file.metadata()?;
            let offset = if at_end { meta.len() } else { 0 };
            file.seek(SeekFrom::Start(offset))?;
            self.open = Some(Open {
                file,
                id: (meta.dev(), meta.ino()),
                offset,
            });
        }
        let Some(open) = self.open.as_mut() else {
            return Ok(false);
        };
        if open.file.metadata()?.len() < open.offset {
            flush(&mut self.partial, lines);
            open.offset = open.file.seek(SeekFrom::Start(0))?;
        }
        // Looked at before the read, so that the old file is read to its end
        // after the path names a new one.
        let moved = match std::fs::metadata(&self.path) {
            Ok(meta) => (meta.dev(), meta.ino()) != open.id,
            Err(e) if e.kind() == ErrorKind::NotFound => false,
            Err(e) => return Err(e),
        };
        let mut buf = vec![0u8; 64 * 1024];
        let mut read = 0;
        while read < READ_LIMIT {
            let n = open.file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            open.offset += n as u64;
            read += n;
            push(&mut self.partial, &buf[..n], lines);
        }
        if read >= READ_LIMIT {
            return Ok(true);
        }
        if moved {
            flush(&mut self.partial, lines);
            self.open = None;
            return Ok(true);
        }
        Ok(false)
    }
}

/// Adds `data` to the partial line, and moves each complete line, and each
/// [`MAX_LINE`] of a longer one, to `lines`.
fn push(partial: &mut Vec<u8>, data: &[u8], lines: &mut Vec<Bytes>) {
    partial.extend_from_slice(data);
    let mut start = 0;
    while let Some(end) = partial[start..].iter().position(|&b| b == b'\n') {
        emit(&partial[start..start + end], lines);
        start += end + 1;
    }
    while partial.len() - start >= MAX_LINE {
        emit(&partial[start..start + MAX_LINE], lines);
        start += MAX_LINE;
    }
    partial.drain(..start);
}

/// Sends the partial line of a file that has ended.
fn flush(partial: &mut Vec<u8>, lines: &mut Vec<Bytes>) {
    emit(partial, lines);
    partial.clear();
}

/// A line without its newline as lines to send: none for a blank line.
fn emit(line: &[u8], lines: &mut Vec<Bytes>) {
    for chunk in line.chunks(MAX_LINE) {
        let mut text = String::from_utf8_lossy(chunk).into_owned();
        text.push('\n');
        lines.push(Bytes::from(text));
    }
}

#[cfg(test)]
mod tests;
