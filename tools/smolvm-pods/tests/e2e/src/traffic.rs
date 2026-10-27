//! Traffic for the wake test: the test app's client commands, run from the
//! node in the network namespace of a pod's process, so that they act as that
//! pod without kubectl exec; and what they print.

use std::collections::HashMap;
use std::fmt;
use std::thread;
use std::time::{Duration, Instant};

use crate::cmd::describe;
use crate::node::{Node, COMMAND_TIMEOUT};
use crate::runtime::quote;

/// One request of `testapp fetch`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Fetched {
    pub status: Option<u16>,
    /// From the start of the request until the TCP connection was up.
    pub connect_ms: Option<f64>,
    /// From the start of the request until the first byte of the answer.
    pub first_byte_ms: Option<f64>,
    pub total_ms: f64,
    pub error: Option<String>,
    /// The body's `KEY=VALUE` lines; a key that comes again keeps its last
    /// value.
    pub body: HashMap<String, String>,
    /// The body's other lines.
    pub lines: Vec<String>,
}

impl Fetched {
    /// Whether the request got a 200 and its whole body.
    pub fn ok(&self) -> bool {
        self.status == Some(200) && self.error.is_none()
    }

    /// A value of the body, or an error that describes the request.
    pub fn value(&self, key: &str) -> Result<&str, String> {
        self.body
            .get(key)
            .map(String::as_str)
            .ok_or_else(|| format!("no {key} in the answer: {self}"))
    }
}

impl fmt::Display for Fetched {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.error, self.status) {
            (Some(error), _) => write!(f, "error {error:?}")?,
            (None, Some(status)) => write!(f, "status {status}")?,
            (None, None) => write!(f, "no status")?,
        }
        write!(f, " after {:.1} ms", self.total_ms)?;
        if let Some(ms) = self.connect_ms {
            write!(f, ", connected at {ms:.1} ms")?;
        }
        if let Some(ms) = self.first_byte_ms {
            write!(f, ", first byte at {ms:.1} ms")?;
        }
        Ok(())
    }
}

/// Parses what `testapp fetch` printed: `I.KEY=VALUE` lines for request I.
pub fn parse_fetch(text: &str) -> Result<Vec<Fetched>, String> {
    let mut requests: Vec<Fetched> = Vec::new();
    for line in text.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let Some((index, field)) = key.split_once('.') else {
            continue;
        };
        let Ok(i) = index.parse::<usize>() else {
            continue;
        };
        if requests.len() <= i {
            requests.resize_with(i + 1, Fetched::default);
        }
        let request = &mut requests[i];
        let ms = || value.parse::<f64>().ok();
        match field {
            "status" => request.status = value.parse().ok(),
            "connect_ms" => request.connect_ms = ms(),
            "first_byte_ms" => request.first_byte_ms = ms(),
            "total_ms" => request.total_ms = ms().unwrap_or_default(),
            "error" => request.error = Some(value.to_string()),
            "body" => request.lines.push(value.to_string()),
            _ => {
                if let Some(key) = field.strip_prefix("body.") {
                    request.body.insert(key.to_string(), value.to_string());
                }
            }
        }
    }
    if requests.is_empty() {
        return Err(format!("testapp fetch printed no request: {text:?}"));
    }
    Ok(requests)
}

/// The `KEY=VALUE` lines of the other clients; a key that comes again keeps
/// its last value.
pub fn pairs(text: &str) -> HashMap<String, String> {
    text.lines()
        .filter_map(|line| line.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// The test app in the node, run in the network namespace of process `pid`.
pub struct Client<'a> {
    node: &'a Node,
    testapp: String,
    pid: String,
    /// A directory in the node for the output of background clients.
    dir: String,
}

impl<'a> Client<'a> {
    pub fn new(node: &'a Node, testapp: &str, pid: &str, dir: &str) -> Client<'a> {
        Client {
            node,
            testapp: testapp.to_string(),
            pid: pid.to_string(),
            dir: dir.to_string(),
        }
    }

    /// The command line that runs `testapp ARGS` in the namespace.
    pub fn command(&self, args: &[&str]) -> Vec<String> {
        ["nsenter", "-t", &self.pid, "-n", &self.testapp]
            .iter()
            .chain(args)
            .map(|a| a.to_string())
            .collect()
    }

    /// The same command, quoted for sh.
    pub fn shell(&self, args: &[&str]) -> String {
        self.command(args)
            .iter()
            .map(|a| quote(a))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Runs `testapp ARGS` and returns its stdout, or an error unless it
    /// exits with 0.
    pub fn run(&self, args: &[&str], timeout: Duration) -> Result<String, String> {
        let command = self.command(args);
        let refs: Vec<&str> = command.iter().map(String::as_str).collect();
        let output = self.node.exec(&refs, timeout + COMMAND_TIMEOUT);
        if output.success() {
            Ok(output.stdout)
        } else {
            Err(format!("testapp {}: {}", args.join(" "), describe(&output)))
        }
    }

    /// Makes `count` GETs of `url` at once, each on a new connection.
    pub fn fetch(
        &self,
        url: &str,
        count: usize,
        timeout: Duration,
    ) -> Result<Vec<Fetched>, String> {
        let count = count.to_string();
        let limit = format!("{}ms", timeout.as_millis());
        let out = self.run(&["fetch", "-n", &count, "-timeout", &limit, url], timeout)?;
        parse_fetch(&out)
    }

    /// Makes one GET of each of `urls`, all at once.
    pub fn fetch_all(&self, urls: &[&str], timeout: Duration) -> Result<Vec<Fetched>, String> {
        let limit = format!("{}ms", timeout.as_millis());
        let mut args = vec!["fetch", "-timeout", &limit];
        args.extend(urls);
        let fetched = parse_fetch(&self.run(&args, timeout)?)?;
        if fetched.len() != urls.len() {
            return Err(format!("{} answers for {} URLs", fetched.len(), urls.len()));
        }
        Ok(fetched)
    }

    /// Makes one GET of `url`.
    pub fn fetch_one(&self, url: &str, timeout: Duration) -> Result<Fetched, String> {
        let mut fetched = self.fetch(url, 1, timeout)?;
        Ok(fetched.remove(0))
    }

    /// Makes one GET of `url` and fails unless it got a 200.
    pub fn get(&self, url: &str, timeout: Duration) -> Result<Fetched, String> {
        let fetched = self.fetch_one(url, timeout)?;
        if fetched.ok() {
            Ok(fetched)
        } else {
            Err(format!("GET {url}: {fetched}"))
        }
    }

    /// Starts `testapp ARGS` in the background, in a session of its own.
    pub fn spawn(&self, name: &str, args: &[&str]) -> Result<Background<'a>, String> {
        let out = format!("{}/{name}", self.dir);
        let script = r#"out=$1
shift
rm -f "$out" "$out.status" "$out.go" "$out.pid"
setsid -f sh -c 'out=$1; shift; echo $$ >"$out.pid"; "$@" >"$out" 2>&1; echo $? >"$out.status"' \
  sh "$out" "$@" </dev/null >/dev/null 2>&1
for _ in $(seq 100); do
  if [ -s "$out.pid" ]; then exit 0; fi
  sleep 0.05
done
echo "the client did not start" >&2; exit 1"#;
        let command = self.command(args);
        let mut full = vec!["sh", "-c", script, "sh", &out];
        full.extend(command.iter().map(String::as_str));
        let output = self.node.exec(&full, COMMAND_TIMEOUT);
        if !output.success() {
            return Err(format!(
                "start testapp {}: {}",
                args.join(" "),
                describe(&output)
            ));
        }
        Ok(Background {
            node: self.node,
            out,
        })
    }

    /// The file whose creation a background client waits for, with
    /// `-then-file`.
    pub fn trigger_file(&self, name: &str) -> String {
        format!("{}/{name}.go", self.dir)
    }
}

/// A client running in the background, writing to a file in the node.
pub struct Background<'a> {
    node: &'a Node,
    out: String,
}

impl Background<'_> {
    /// What it printed so far.
    pub fn output(&self) -> Result<String, String> {
        self.node
            .try_sh(&format!("cat {} 2>/dev/null || true", self.out))
    }

    /// Its exit status, once it ended.
    pub fn status(&self) -> Result<Option<i32>, String> {
        let status = self
            .node
            .try_sh(&format!("cat {}.status 2>/dev/null || true", self.out))?;
        Ok(status.trim().parse().ok())
    }

    /// Waits until it printed `text`, and returns what it printed.
    pub fn wait_for(&self, text: &str, timeout: Duration) -> Result<String, String> {
        let deadline = Instant::now() + timeout;
        loop {
            let output = self.output()?;
            if output.contains(text) {
                return Ok(output);
            }
            if self.status()?.is_some() || Instant::now() >= deadline {
                return Err(format!(
                    "the client did not print {text:?} within {timeout:?}: {output:?}"
                ));
            }
            thread::sleep(Duration::from_millis(250));
        }
    }

    /// Waits until it ends, and returns what it printed. An error says how it
    /// ended otherwise.
    pub fn wait(&self, timeout: Duration) -> Result<String, String> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.status()? {
                let output = self.output()?;
                return match status {
                    0 => Ok(output),
                    _ => Err(format!("the client exited with {status}: {output:?}")),
                };
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "the client did not end within {timeout:?}: {:?}",
                    self.output()?
                ));
            }
            thread::sleep(Duration::from_millis(250));
        }
    }

    /// Creates the file that it waits for with `-then-file`.
    pub fn trigger(&self) -> Result<(), String> {
        self.node
            .try_sh(&format!("touch {}.go", self.out))
            .map(drop)
    }
}

impl Drop for Background<'_> {
    fn drop(&mut self) {
        let script = format!(
            "pid=$(cat {out}.pid 2>/dev/null); [ -z \"$pid\" ] || kill -- -\"$pid\" 2>/dev/null; true",
            out = self.out
        );
        if let Err(e) = self.node.try_sh(&script) {
            eprintln!("stop the client writing {}: {e}", self.out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_fetches() {
        let text = "0.total_ms=12.5\n0.connect_ms=0.4\n0.first_byte_ms=12.1\n0.status=200\n\
                    0.body_bytes=60\n0.body.name=web\n0.body.pid=7\n0.body=ok\n\
                    0.body.callee.remote_ip=10.42.0.9\n0.body.k=v=w\n\
                    1.total_ms=1003.2\n1.error=dial tcp 10.42.0.9:8080: connect: connection refused\n\
                    testapp: a line that is not a pair\n";
        let fetched = parse_fetch(text).unwrap();
        assert_eq!(fetched.len(), 2);
        let first = &fetched[0];
        assert!(first.ok());
        assert_eq!(first.connect_ms, Some(0.4));
        assert_eq!(first.first_byte_ms, Some(12.1));
        assert_eq!(first.value("pid"), Ok("7"));
        assert_eq!(first.value("callee.remote_ip"), Ok("10.42.0.9"));
        assert_eq!(first.value("k"), Ok("v=w"));
        assert_eq!(first.lines, ["ok"]);
        let second = &fetched[1];
        assert!(!second.ok());
        assert_eq!(second.status, None);
        assert!(second
            .error
            .as_deref()
            .unwrap()
            .contains("connection refused"));
        assert!(second.to_string().starts_with("error \"dial tcp"));
        assert!(second.value("pid").is_err());
        assert!(parse_fetch("testapp: no such host\n").is_err());
    }

    #[test]
    fn a_failed_status_is_not_ok() {
        let fetched = parse_fetch("0.total_ms=1.0\n0.status=503\n0.body_bytes=0\n").unwrap();
        assert!(!fetched[0].ok());
        assert_eq!(fetched[0].to_string(), "status 503 after 1.0 ms");
    }

    #[test]
    fn reads_pairs() {
        let pairs = pairs("first_conn=3\npings=6\nsecond_reply=conn=3;n=2;text=second\nnoise\n");
        assert_eq!(pairs["first_conn"], "3");
        assert_eq!(pairs["second_reply"], "conn=3;n=2;text=second");
        assert_eq!(pairs.len(), 3);
    }

    #[test]
    fn quotes_client_commands() {
        let node = Node::new("n".into());
        let client = Client::new(&node, "/var/tmp/t/testapp", "42", "/var/tmp/t");
        assert_eq!(
            client.shell(&["fetch", "http://10.42.0.9:8080/call?url=x"]),
            "'nsenter' '-t' '42' '-n' '/var/tmp/t/testapp' 'fetch' \
             'http://10.42.0.9:8080/call?url=x'"
        );
        assert_eq!(client.trigger_file("ws"), "/var/tmp/t/ws.go");
    }
}
