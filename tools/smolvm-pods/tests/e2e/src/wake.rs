//! The tests' adapter to idle sleep, wake on traffic, wake admission, the
//! node service's settings and the TCP API. It builds on `runtime`, which it
//! uses for every ctl call. tests/e2e/README.md ("Runtime contract")
//! documents the same contract for the runtime's side; a change here must
//! change it too.

use std::time::{Duration, Instant};

use crate::json::Json;
use crate::node::{Node, COMMAND_TIMEOUT};
use crate::runtime::{self, parent, quote, Api, Event, Role};
use crate::step;

/// The pod annotation that sets a pod's idle timeout: a Go-style duration
/// from 10 s to 24 h.
pub const IDLE_TIMEOUT: &str = "smolvm-pods.floci.io/idle-timeout";

/// The node service's poll interval (`--poll-interval`, default 15 s): how
/// often it measures each pod's traffic. A pod sleeps at most this long after
/// its idle timeout ends.
pub const POLL_INTERVAL: Duration = Duration::from_secs(15);

/// floci-node adds the words of this file to the node service's arguments
/// each time it starts the node service, while the file exists.
pub const NODE_SERVICE_ARGS: &str = "/run/smolvm-pods/node-service.args";

/// The User-Agent of kubelet's HTTP probes, which the tests' own probes send.
pub const PROBE_USER_AGENT: &str = "kube-probe/1.35";

/// The node service's settings for wake admission.
pub const MAX_AWAKE_PODS: &str = "--max-awake-pods";
pub const ADMISSION_WAIT: &str = "--admission-wait";

/// The node service's settings for the TCP API.
pub const API_LISTEN: &str = "--api-listen";
pub const API_TOKEN_FILE: &str = "--api-token-file";

/// Where a traffic wake came from: the source pod's `namespace/name`, the
/// original source of a node-local connection, or the IP; and the pod's port.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Traffic {
    pub from: String,
    pub port: u16,
}

impl Traffic {
    /// The traffic of an event with the reason `traffic`, from its
    /// `reason_detail`.
    pub fn of(event: &Event) -> Option<Traffic> {
        let detail = event.raw.get("reason_detail");
        if event.reason != "traffic" || detail.get("kind").as_str() != Some("traffic") {
            return None;
        }
        Some(Traffic {
            from: detail.get("from").as_str()?.to_string(),
            port: u16::try_from(detail.get("port").as_u64()?).ok()?,
        })
    }
}

/// A resume that traffic started: `asleep` to `waking` with the reason
/// `traffic`.
pub fn is_traffic_wake(event: &Event) -> bool {
    event.is_resume() && event.reason == "traffic"
}

/// A sleep that the idle timer started: `pausing` to `asleep` with the
/// reason `idle`.
pub fn is_idle_sleep(event: &Event) -> bool {
    event.from == "pausing" && event.to == "asleep" && event.reason == "idle"
}

/// The start of a pause, for whatever reason.
pub fn is_pause(event: &Event) -> bool {
    event.to == "pausing"
}

/// A pause that ended with the VM still running: cancelled by a wake, or
/// refused by the recheck or another error.
pub fn is_aborted_pause(event: &Event) -> bool {
    event.from == "pausing" && event.to == "awake" && (event.cancelled || event.error.is_some())
}

/// Why the node service did not put an idle pod to sleep: an event from
/// `awake` to `awake` with the reason `idle` and the reason in `error`.
pub fn sleep_refusal(event: &Event) -> Option<&str> {
    match (
        event.from.as_str(),
        event.to.as_str(),
        event.reason.as_str(),
    ) {
        ("awake", "awake", "idle") => event.error.as_deref(),
        _ => None,
    }
}

/// Why admission refused a wake: an event from `asleep` to `asleep` whose
/// `error` starts with `wake refused`.
pub fn wake_refusal(event: &Event) -> Option<&str> {
    match (
        event.from.as_str(),
        event.to.as_str(),
        event.error.as_deref(),
    ) {
        ("asleep", "asleep", Some(error)) if error.starts_with("wake refused") => Some(error),
        _ => None,
    }
}

/// On the `waking` to `awake` event of a traffic wake: the node service's
/// time from capturing the first held frame to the release, in ms.
pub fn capture_to_release_ms(event: &Event) -> Option<f64> {
    event.raw.get("capture_to_release_ms").as_f64()
}

/// Starts the node service again with `args` added to its arguments, through
/// [`NODE_SERVICE_ARGS`], and fails unless it runs with them.
pub fn set_args(api: &Api, args: &[&str]) -> Result<u32, String> {
    let words = args.join(" ");
    api.node().try_sh(&format!(
        "mkdir -p {dir} && printf '%s\\n' {line} > {NODE_SERVICE_ARGS}.new && \
         mv {NODE_SERVICE_ARGS}.new {NODE_SERVICE_ARGS}",
        dir = parent(NODE_SERVICE_ARGS),
        line = quote(&words)
    ))?;
    let pid = api.restart_node_service()?;
    let running = api.only(Role::NodeService)?;
    if !has_args(&running.args, args) {
        return Err(format!(
            "the node service runs as {:?}, without {words:?}: floci-node must add the words \
             of {NODE_SERVICE_ARGS} to its arguments",
            running.args.join(" ")
        ));
    }
    step(&format!("node service {pid} runs with {words}"));
    Ok(pid)
}

/// Removes [`NODE_SERVICE_ARGS`] and starts the node service again without
/// the arguments.
pub fn clear_args(api: &Api) -> Result<u32, String> {
    api.node().try_sh(&format!("rm -f {NODE_SERVICE_ARGS}"))?;
    api.restart_node_service()
}

/// Whether `args` holds `words` in order, each one whole.
pub fn has_args(args: &[String], words: &[&str]) -> bool {
    words.is_empty()
        || args
            .windows(words.len())
            .any(|window| window.iter().zip(words).all(|(a, w)| a == w))
}

/// The lines of a log with the level ERROR.
pub fn error_lines(log: &str) -> Vec<&str> {
    log.lines()
        .filter(|line| line.split_whitespace().nth(1) == Some("ERROR"))
        .collect()
}

/// The node service's CPU time and memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Usage {
    pub pid: u32,
    /// User and system time, in clock ticks.
    pub cpu_ticks: u64,
    pub ticks_per_second: u64,
    pub uss_kib: u64,
    pub rss_kib: u64,
}

impl Usage {
    /// The share of one core that the node service used between `self` and
    /// `later`, over `elapsed`.
    pub fn cpu_share(&self, later: &Usage, elapsed: Duration) -> f64 {
        let ticks = later.cpu_ticks.saturating_sub(self.cpu_ticks) as f64;
        ticks / self.ticks_per_second.max(1) as f64 / elapsed.as_secs_f64().max(1e-9)
    }
}

/// The node service's usage now.
pub fn usage(api: &Api) -> Result<Usage, String> {
    let service = api.only(Role::NodeService)?;
    let script = format!(
        "sed 's/.*) //' /proc/{}/stat | cut -d ' ' -f 12,13; getconf CLK_TCK",
        service.pid
    );
    let output = api.node().try_sh(&script)?;
    parse_usage(&output, service.pid, service.uss_kib, service.rss_kib)
}

/// Parses `UTIME STIME` and `CLK_TCK`, the output of [`usage`]'s script.
pub fn parse_usage(text: &str, pid: u32, uss_kib: u64, rss_kib: u64) -> Result<Usage, String> {
    let numbers: Vec<u64> = text
        .split_whitespace()
        .map(|n| n.parse().map_err(|e| format!("{n:?} in {text:?}: {e}")))
        .collect::<Result<_, _>>()?;
    match numbers[..] {
        [utime, stime, ticks_per_second] => Ok(Usage {
            pid,
            cpu_ticks: utime + stime,
            ticks_per_second,
            uss_kib,
            rss_kib,
        }),
        _ => Err(format!("CPU times of the node service: {text:?}")),
    }
}

/// Polls `ctl get POD` every 200 ms until the pod is asleep or `$2` ms
/// pass, and prints when it was first seen pausing and asleep, in ms after
/// the start, or its last state.
const WATCH_SLEEP: &str = r#"pod=$1 limit=$2
start=$(date +%s%N)
pausing=
while :; do
  elapsed=$(( ($(date +%s%N) - start) / 1000000 ))
  state=$(smolvm-pods ctl get "$pod" 2>/dev/null | jq -r .state 2>/dev/null)
  case $state in
    pausing) pausing=${pausing:-$elapsed} ;;
    asleep) echo "pausing_ms=${pausing:-$elapsed}"; echo "asleep_ms=$elapsed"; exit 0 ;;
  esac
  if [ "$elapsed" -ge "$limit" ]; then
    echo "state=${state:-unknown}"; echo "pausing_ms=$pausing"; exit 0
  fi
  sleep 0.2
done"#;

/// When a watched pod went to sleep, after the start of the watch.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Slept {
    pub pausing: Duration,
    pub asleep: Duration,
}

/// Watches the pod until it is asleep, for at most `timeout`. The error
/// names the last state seen.
pub fn watch_sleep(node: &Node, pod: &str, timeout: Duration) -> Result<Slept, String> {
    let limit = timeout.as_millis().to_string();
    let output = node.exec(
        &["sh", "-c", WATCH_SLEEP, "sh", pod, &limit],
        timeout + COMMAND_TIMEOUT,
    );
    if !output.success() {
        return Err(format!("watch pod {pod}: {}", crate::describe(&output)));
    }
    parse_watch(&output.stdout)
}

/// Parses the output of [`WATCH_SLEEP`].
pub fn parse_watch(text: &str) -> Result<Slept, String> {
    let value = |key: &str| {
        text.lines()
            .find_map(|line| line.strip_prefix(key)?.strip_prefix('='))
            .filter(|v| !v.is_empty())
    };
    let ms = |key: &str| value(key).and_then(|v| v.parse::<u64>().ok());
    match (ms("pausing_ms"), ms("asleep_ms")) {
        (Some(pausing), Some(asleep)) => Ok(Slept {
            pausing: Duration::from_millis(pausing),
            asleep: Duration::from_millis(asleep),
        }),
        (pausing, _) => Err(format!(
            "still {}{}",
            value("state").unwrap_or("unknown"),
            pausing.map_or(String::new(), |ms| format!(
                ", though it was pausing after {ms} ms"
            ))
        )),
    }
}

/// The node service's TCP listener, reached from the node with curl.
pub struct TcpApi<'a> {
    node: &'a Node,
    /// `ADDR:PORT` of the listener.
    pub addr: String,
    pub token: String,
}

impl<'a> TcpApi<'a> {
    pub fn new(node: &'a Node, addr: &str, token: &str) -> TcpApi<'a> {
        TcpApi {
            node,
            addr: addr.to_string(),
            token: token.to_string(),
        }
    }

    /// Makes a request with `token` as the bearer token, or none, and returns
    /// the status and the body. An error means that no answer came.
    pub fn request(
        &self,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Option<&str>,
    ) -> Result<(u16, String), String> {
        let url = format!("http://{}{path}", self.addr);
        let auth = token.map(|t| format!("Authorization: Bearer {t}"));
        let mut args = vec![
            "curl",
            "-sS",
            "-m",
            "150",
            "-X",
            method,
            "-w",
            "\n%{http_code}",
        ];
        if let Some(auth) = &auth {
            args.extend(["-H", auth]);
        }
        if let Some(body) = body {
            args.extend(["-H", "Content-Type: application/json", "--data", body]);
        }
        args.push(&url);
        let output = self.node.exec(&args, Duration::from_secs(180));
        if !output.success() {
            return Err(format!(
                "{method} {url}: the TCP API did not answer: {}",
                crate::describe(&output)
            ));
        }
        parse_curl(&output.stdout).ok_or_else(|| format!("{method} {url}: {:?}", output.stdout))
    }

    /// Like [`TcpApi::request`] with the right token, and the body as JSON.
    pub fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> Result<(u16, Json), String> {
        let (status, text) = self.request(method, path, Some(&self.token), body)?;
        let json = Json::parse(&text).map_err(|e| format!("{method} {path}: {e}: {text:?}"))?;
        Ok((status, json))
    }

    /// Starts following the event stream from `cursor` in the background for
    /// `seconds`, into the file `out` in the node.
    pub fn follow(&self, cursor: u64, seconds: u64, out: &str) -> Result<(), String> {
        let url = format!("http://{}/v1/events?follow=true&cursor={cursor}", self.addr);
        let script = r#"out=$1 seconds=$2 token=$3 url=$4
rm -f "$out"
setsid -f curl -sN -m "$seconds" -H "Authorization: Bearer $token" -o "$out" "$url" \
  </dev/null >/dev/null 2>&1"#;
        let seconds = seconds.to_string();
        let output = self.node.exec(
            &["sh", "-c", script, "sh", out, &seconds, &self.token, &url],
            COMMAND_TIMEOUT,
        );
        if output.success() {
            Ok(())
        } else {
            Err(format!("follow {url}: {}", crate::describe(&output)))
        }
    }

    /// The events that the follow into `out` received so far.
    pub fn followed(&self, out: &str) -> Result<Vec<Event>, String> {
        let text = self
            .node
            .try_sh(&format!("cat {out} 2>/dev/null || true"))?;
        runtime::parse_events(&text)
    }
}

/// Splits curl's output, the body then a line with the status.
pub fn parse_curl(text: &str) -> Option<(u16, String)> {
    let (body, status) = text.rsplit_once('\n')?;
    Some((status.trim().parse().ok()?, body.to_string()))
}

/// Polls `check` every `every` until it returns false or `duration` passes,
/// and returns the time it first returned false.
pub fn holds_for(
    duration: Duration,
    every: Duration,
    mut check: impl FnMut() -> Result<bool, String>,
) -> Result<Option<Duration>, String> {
    let start = Instant::now();
    loop {
        if !check()? {
            return Ok(Some(start.elapsed()));
        }
        if start.elapsed() >= duration {
            return Ok(None);
        }
        std::thread::sleep(every.min(duration.saturating_sub(start.elapsed())));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(json: &str) -> Event {
        Event::parse(&Json::parse(json).unwrap()).unwrap()
    }

    #[test]
    fn reads_the_source_of_a_traffic_wake() {
        let wake = event(
            r#"{"seq": 9, "pod": "5f3c", "from": "asleep", "to": "waking", "reason": "traffic",
                "reason_detail": {"kind": "traffic", "from": "ns/client", "port": 8080}}"#,
        );
        assert!(is_traffic_wake(&wake));
        assert_eq!(
            Traffic::of(&wake),
            Some(Traffic {
                from: "ns/client".into(),
                port: 8080
            })
        );
        let api = event(
            r#"{"seq": 10, "pod": "5f3c", "from": "asleep", "to": "waking", "reason": "api",
                "reason_detail": null}"#,
        );
        assert!(!is_traffic_wake(&api));
        assert_eq!(Traffic::of(&api), None);
    }

    #[test]
    fn tells_idle_sleeps_and_aborted_pauses_apart() {
        let idle =
            event(r#"{"seq": 1, "pod": "p", "from": "pausing", "to": "asleep", "reason": "idle"}"#);
        let api =
            event(r#"{"seq": 2, "pod": "p", "from": "pausing", "to": "asleep", "reason": "api"}"#);
        let start =
            event(r#"{"seq": 3, "pod": "p", "from": "awake", "to": "pausing", "reason": "idle"}"#);
        let refused = event(
            r#"{"seq": 4, "pod": "p", "from": "pausing", "to": "awake", "reason": "idle",
                "error": "traffic since the idle decision"}"#,
        );
        let cancelled = event(
            r#"{"seq": 5, "pod": "p", "from": "pausing", "to": "awake", "reason": "traffic",
                "cancelled": true}"#,
        );
        assert!(is_idle_sleep(&idle) && !is_idle_sleep(&api));
        assert!(is_pause(&start) && !is_pause(&idle));
        assert!(is_aborted_pause(&refused) && is_aborted_pause(&cancelled));
        assert!(!is_aborted_pause(&start));
    }

    #[test]
    fn reads_refusals() {
        let sleep = event(
            r#"{"seq": 1, "pod": "p", "from": "awake", "to": "awake", "reason": "idle",
                "error": "not sleeping: readiness probe http :8080/health failed: 500"}"#,
        );
        assert_eq!(
            sleep_refusal(&sleep),
            Some("not sleeping: readiness probe http :8080/health failed: 500")
        );
        let wake = event(
            r#"{"seq": 2, "pod": "p", "from": "asleep", "to": "asleep", "reason": "traffic",
                "error": "wake refused: awake pods 2/2"}"#,
        );
        assert_eq!(wake_refusal(&wake), Some("wake refused: awake pods 2/2"));
        assert_eq!(sleep_refusal(&wake), None);
        let failed = event(
            r#"{"seq": 3, "pod": "p", "from": "waking", "to": "asleep", "reason": "traffic",
                "error": "resume failed"}"#,
        );
        assert_eq!(wake_refusal(&failed), None);
    }

    #[test]
    fn reads_the_capture_to_release_time() {
        let awake = event(
            r#"{"seq": 1, "pod": "p", "from": "waking", "to": "awake", "reason": "traffic",
                "steps_ms": {"resume": 150.5, "release": 1.2}, "capture_to_release_ms": 182.4}"#,
        );
        assert_eq!(capture_to_release_ms(&awake), Some(182.4));
        assert_eq!(awake.steps_ms.len(), 2);
    }

    #[test]
    fn finds_arguments_in_order() {
        let args: Vec<String> = "smolvm-pods node-service --max-awake-pods 2 --admission-wait 10s"
            .split(' ')
            .map(str::to_string)
            .collect();
        assert!(has_args(&args, &[MAX_AWAKE_PODS, "2"]));
        assert!(has_args(
            &args,
            &[MAX_AWAKE_PODS, "2", ADMISSION_WAIT, "10s"]
        ));
        assert!(!has_args(&args, &[MAX_AWAKE_PODS, "3"]));
        assert!(!has_args(&args, &["--max-awake"]));
        assert!(has_args(&args, &[]));
    }

    #[test]
    fn parses_watches() {
        let slept = parse_watch("pausing_ms=12400\nasleep_ms=13650\n").unwrap();
        assert_eq!(slept.pausing, Duration::from_millis(12400));
        assert_eq!(slept.asleep, Duration::from_millis(13650));
        let error = parse_watch("state=awake\npausing_ms=\n").unwrap_err();
        assert_eq!(error, "still awake");
        let error = parse_watch("state=pausing\npausing_ms=500\n").unwrap_err();
        assert!(error.contains("pausing after 500 ms"), "{error}");
    }

    #[test]
    fn parses_curl_output() {
        assert_eq!(
            parse_curl("{\"pods\": []}\n200"),
            Some((200, "{\"pods\": []}".to_string()))
        );
        assert_eq!(parse_curl("\n401"), Some((401, String::new())));
        assert_eq!(parse_curl("no status"), None);
    }

    #[test]
    fn parses_usage() {
        let usage = parse_usage("1500 300\n100\n", 42, 9000, 20000).unwrap();
        assert_eq!(usage.cpu_ticks, 1800);
        let later = Usage {
            cpu_ticks: 1800 + 6000,
            ..usage
        };
        let share = usage.cpu_share(&later, Duration::from_secs(600));
        assert!((share - 0.1).abs() < 1e-9, "{share}");
        assert!(parse_usage("1 2", 1, 0, 0).is_err());
    }

    #[test]
    fn finds_error_lines() {
        let log = "2026-10-19T12:00:00Z INFO smolvm_pods::node: woke\n\
                   2026-10-19T12:00:01Z ERROR smolvm_pods::sleepnet: bad frame\n\
                   2026-10-19T12:00:02Z WARN smolvm_pods::node: ERROR in text\n";
        assert_eq!(
            error_lines(log),
            ["2026-10-19T12:00:01Z ERROR smolvm_pods::sleepnet: bad frame"]
        );
    }

    #[test]
    fn polls_until_a_check_fails() {
        let mut calls = 0;
        let ended = holds_for(Duration::from_secs(5), Duration::from_millis(1), || {
            calls += 1;
            Ok(calls < 3)
        })
        .unwrap();
        assert!(ended.is_some() && calls == 3);
        let held = holds_for(Duration::from_millis(20), Duration::from_millis(5), || {
            Ok(true)
        });
        assert_eq!(held, Ok(None));
    }
}
