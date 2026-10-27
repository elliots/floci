//! HTTP probe answers, and the 503 for a request that is not kubelet's.
//!
//! Kubelet sends `GET`, `User-Agent: kube-probe/1.35`, `Accept: */*` and
//! `Connection: close`, and passes a status from 200 to 399. The responder reads at most
//! [`REQUEST_MAX`] bytes or for [`REQUEST_WAIT`], answers the status recorded for the
//! probe's port and path with `Content-Length: 0` and `Connection: close`, and closes. A
//! request that is not kubelet's gets a 503 with `Retry-After: 1`, and wakes the pod.

use std::time::Duration;

use super::{PodProbes, ProbeAction};

/// The most of a request that is read.
pub const REQUEST_MAX: usize = 4096;
/// How long a request may take to arrive.
pub const REQUEST_WAIT: Duration = Duration::from_secs(1);
/// Kubelet's `User-Agent` starts with this, unless the probe sets its own.
pub const KUBELET_AGENT: &str = "kube-probe/";

/// The answer to a request that is not kubelet's, or that no recorded result answers.
pub const UNAVAILABLE: &[u8] = b"HTTP/1.1 503 Service Unavailable\r\nRetry-After: 1\r\n\
Content-Length: 0\r\nConnection: close\r\n\r\n";

/// What a request asks, from its head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub method: String,
    pub target: String,
    pub user_agent: Option<String>,
}

/// What the responder does with a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    /// Kubelet's probe: the recorded status.
    Status(u16),
    /// Not kubelet's, or not a recorded probe: [`UNAVAILABLE`], and the pod wakes.
    Wake,
    /// The probe's recorded result is a refused connection: a RST.
    Refuse,
}

/// The head of a request, as it arrives.
#[derive(Debug, Default)]
pub struct RequestReader {
    head: Vec<u8>,
}

impl RequestReader {
    /// Takes bytes from the client, up to [`REQUEST_MAX`] in all.
    pub fn push(&mut self, bytes: &[u8]) {
        let room = REQUEST_MAX.saturating_sub(self.head.len());
        self.head.extend_from_slice(&bytes[..bytes.len().min(room)]);
    }

    pub fn is_empty(&self) -> bool {
        self.head.is_empty()
    }

    /// Whether the head is complete, or as long as it may be.
    pub fn is_done(&self) -> bool {
        self.head.len() >= REQUEST_MAX || self.head.windows(4).any(|w| w == b"\r\n\r\n")
    }

    /// The request, from the lines that arrived whole. None without a request line.
    pub fn request(&self) -> Option<Request> {
        let text = String::from_utf8_lossy(&self.head);
        let mut lines = text.split("\r\n");
        let mut whole = text.matches("\r\n").count();
        let mut parts = lines.next().filter(|_| whole > 0)?.split(' ');
        let (method, target) = (parts.next()?, parts.next()?);
        if !parts.next()?.starts_with("HTTP/1.") {
            return None;
        }
        let mut user_agent = None;
        for line in lines {
            whole -= 1;
            if whole == 0 || line.is_empty() {
                break;
            }
            if let Some((name, value)) = line.split_once(':') {
                if name.eq_ignore_ascii_case("user-agent") {
                    user_agent = Some(value.trim().to_string());
                }
            }
        }
        Some(Request {
            method: method.to_string(),
            target: target.to_string(),
            user_agent,
        })
    }
}

/// The answer to `request` on `port`, from the pod's probes. A request is
/// kubelet's when its `User-Agent` starts with [`KUBELET_AGENT`], or is the one
/// that the probe sets in its own headers.
pub fn answer(request: Option<&Request>, probes: &PodProbes, port: u16) -> Answer {
    let Some(request) = request else {
        return Answer::Wake;
    };
    if request.method != "GET" {
        return Answer::Wake;
    }
    let agent = request.user_agent.as_deref();
    let recorded = probes
        .probes
        .iter()
        .filter(|p| match &p.action {
            ProbeAction::Http {
                port: on,
                path,
                headers,
                ..
            } => {
                *on == port
                    && request_target(path) == request.target
                    && agent.is_some_and(|agent| is_kubelets(agent, headers))
            }
            _ => false,
        })
        .find_map(|p| p.last.as_ref());
    match recorded {
        None => Answer::Wake,
        Some(result) => match result.code.and_then(|c| u16::try_from(c).ok()) {
            Some(status @ 100..=599) => Answer::Status(status),
            _ if result.passed => Answer::Status(200),
            _ => Answer::Refuse,
        },
    }
}

/// Whether kubelet sends `agent` for a probe with these headers: its own
/// `User-Agent`, or the probe's.
fn is_kubelets(agent: &str, headers: &[(String, String)]) -> bool {
    match headers
        .iter()
        .rfind(|(name, _)| name.eq_ignore_ascii_case("user-agent"))
    {
        Some((_, own)) => agent == own.trim(),
        None => agent.starts_with(KUBELET_AGENT),
    }
}

/// The request target kubelet sends for a probe's path: the URL it builds always has one
/// starting with `/`.
fn request_target(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
}

/// The response with `status` and no body.
pub fn response(status: u16) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status} {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        reason(status)
    )
    .into_bytes()
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        304 => "Not Modified",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "",
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::probe::{Probe, ProbeResult, ProbeRole};

    /// Kubelet 1.35's HTTP probe of `/healthz`, as its prober code makes it, with a pod's
    /// address in `Host`.
    pub const KUBELET_REQUEST: &[u8] = b"GET /healthz HTTP/1.1\r\nHost: 10.42.0.9:8080\r\n\
User-Agent: kube-probe/1.35\r\nAccept: */*\r\nConnection: close\r\n\r\n";

    pub fn http_probe(port: u16, path: &str, https: bool, last: Option<ProbeResult>) -> Probe {
        Probe {
            container: "app".into(),
            role: ProbeRole::Readiness,
            action: ProbeAction::Http {
                port,
                path: path.into(),
                https,
                headers: Vec::new(),
            },
            period_secs: 10,
            timeout_secs: 1,
            last,
        }
    }

    pub fn result(passed: bool, code: Option<i32>) -> ProbeResult {
        ProbeResult {
            passed,
            code,
            output: String::new(),
            at_ms: 1,
        }
    }

    fn read(bytes: &[u8]) -> RequestReader {
        let mut reader = RequestReader::default();
        reader.push(bytes);
        reader
    }

    #[test]
    fn kubelets_request_is_read() {
        let reader = read(KUBELET_REQUEST);
        assert!(reader.is_done());
        assert_eq!(
            reader.request(),
            Some(Request {
                method: "GET".into(),
                target: "/healthz".into(),
                user_agent: Some("kube-probe/1.35".into()),
            })
        );
    }

    #[test]
    fn a_request_is_read_from_whole_lines_up_to_the_limit() {
        let cut = read(&KUBELET_REQUEST[..60]);
        assert!(!cut.is_done());
        let request = cut.request().unwrap();
        assert_eq!(request.target, "/healthz");
        assert_eq!(request.user_agent, None, "the User-Agent line is cut");
        assert_eq!(read(b"GET /healthz HTTP/1.1").request(), None);
        assert_eq!(read(b"\x16\x03\x01\x02\x00\x01\r\n").request(), None);

        let mut long = read(b"GET /healthz HTTP/1.1\r\nX-Pad: ");
        long.push(&[b'a'; 2 * REQUEST_MAX]);
        assert!(long.is_done());
        assert_eq!(long.head.len(), REQUEST_MAX);
    }

    #[test]
    fn kubelet_gets_the_recorded_status_of_its_path() {
        let probes = PodProbes {
            probes: vec![
                http_probe(8080, "/healthz", false, Some(result(true, Some(204)))),
                http_probe(8080, "ready", false, Some(result(true, Some(200)))),
                http_probe(8080, "/broken", false, Some(result(false, Some(500)))),
                http_probe(8080, "/refused", false, Some(result(false, None))),
            ],
            ..Default::default()
        };
        let get = |target: &str| {
            read(format!("GET {target} HTTP/1.1\r\nUser-Agent: kube-probe/1.35\r\n\r\n").as_bytes())
                .request()
        };
        let answer = |target: &str, port| answer(get(target).as_ref(), &probes, port);
        assert_eq!(answer("/healthz", 8080), Answer::Status(204));
        assert_eq!(answer("/ready", 8080), Answer::Status(200));
        assert_eq!(answer("/broken", 8080), Answer::Status(500));
        assert_eq!(answer("/refused", 8080), Answer::Refuse);
        assert_eq!(answer("/other", 8080), Answer::Wake);
        assert_eq!(answer("/healthz", 8081), Answer::Wake);
    }

    #[test]
    fn a_request_that_is_not_kubelets_wakes_the_pod() {
        let probes = PodProbes {
            probes: vec![http_probe(
                8080,
                "/healthz",
                false,
                Some(result(true, Some(200))),
            )],
            ..Default::default()
        };
        for request in [
            &b"GET /healthz HTTP/1.1\r\nUser-Agent: curl/8.5.0\r\n\r\n"[..],
            b"GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n",
            b"POST /healthz HTTP/1.1\r\nUser-Agent: kube-probe/1.35\r\n\r\n",
        ] {
            let request = read(request).request();
            assert_eq!(answer(request.as_ref(), &probes, 8080), Answer::Wake);
        }
        assert_eq!(answer(None, &probes, 8080), Answer::Wake);
    }

    #[test]
    fn a_probe_with_its_own_user_agent_is_kubelets() {
        let mut probe = http_probe(8080, "/healthz", false, Some(result(true, Some(200))));
        if let ProbeAction::Http { headers, .. } = &mut probe.action {
            headers.push(("user-agent".into(), "health-checker/2".into()));
        }
        let probes = PodProbes {
            probes: vec![probe],
            ..Default::default()
        };
        let answer = |agent: &str| {
            let request =
                read(format!("GET /healthz HTTP/1.1\r\nUser-Agent: {agent}\r\n\r\n").as_bytes())
                    .request();
            answer(request.as_ref(), &probes, 8080)
        };
        assert_eq!(answer("health-checker/2"), Answer::Status(200));
        assert_eq!(answer("kube-probe/1.35"), Answer::Wake);
        assert_eq!(answer("curl/8.5.0"), Answer::Wake);
    }

    #[test]
    fn responses_have_no_body_and_close() {
        assert_eq!(
            response(200),
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        assert_eq!(
            response(299),
            b"HTTP/1.1 299 \r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        assert_eq!(
            UNAVAILABLE,
            b"HTTP/1.1 503 Service Unavailable\r\nRetry-After: 1\r\nContent-Length: 0\r\n\
Connection: close\r\n\r\n"
        );
    }
}
