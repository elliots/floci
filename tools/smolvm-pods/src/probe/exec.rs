//! Exec probes answered from the cache while the pod is not awake.
//!
//! Kubelet runs an exec probe through CRI's `ExecSync`, which containerd makes
//! an exec with no terminal and no stdin, whose argv is the probe's command as
//! kubelet expanded it. While the pod is awake, every exec runs in the guest,
//! and the node service records the result of one whose argv is a probe's
//! command with [`super::ProbeBook::record_exec`], with the first
//! [`EXEC_OUTPUT_MAX`] bytes of its output ([`OutputCapture`]). While the pod
//! pauses, sleeps or wakes, such an exec gets that result at once, without a
//! wake ([`cached`]). A user's exec with exactly a probe's argv, no terminal
//! and no stdin gets it too.

use std::fs::OpenOptions;
use std::io::{ErrorKind, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::{PodProbes, ProbeResult, EXEC_OUTPUT_MAX};
use crate::sync::lock;

/// The CRI annotation with a container's name in its pod.
pub const ANN_CONTAINER_NAME: &str = "io.kubernetes.cri.container-name";

/// How long the output of a cached answer may wait for room in its fifo.
const WRITE_WAIT: Duration = Duration::from_secs(1);

/// What an exec runs, from containerd's OCI process spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecCommand {
    pub args: Vec<String>,
    pub terminal: bool,
}

impl ExecCommand {
    pub fn parse(spec: &str) -> Option<ExecCommand> {
        let spec: serde_json::Value = serde_json::from_str(spec).ok()?;
        let args = spec
            .get("args")?
            .as_array()?
            .iter()
            .map(|a| a.as_str().map(str::to_string))
            .collect::<Option<Vec<String>>>()?;
        let terminal = spec
            .get("terminal")
            .and_then(|t| t.as_bool())
            .unwrap_or(false);
        Some(ExecCommand { args, terminal })
    }
}

/// The name of a container in its pod, from the CRI annotation in the spec
/// of its bundle. Blocking.
pub fn container_name(bundle: &str) -> Option<String> {
    let spec = crate::bundle::read_spec(bundle)?;
    crate::bundle::annotation(Some(&spec), ANN_CONTAINER_NAME).map(str::to_string)
}

/// An exec of an exec probe's command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeExec {
    /// The container's name in its pod.
    pub container: String,
    pub command: Vec<String>,
}

/// The probe exec that an exec of `command` in `container` is: one with no
/// terminal and no stdin whose argv is exactly an exec probe's command.
pub fn probe_exec(
    probes: &PodProbes,
    container: &str,
    command: &ExecCommand,
    stdin: bool,
) -> Option<ProbeExec> {
    if command.terminal || stdin {
        return None;
    }
    probes
        .probes
        .iter()
        .any(|p| p.runs(container, &command.args))
        .then(|| ProbeExec {
            container: container.to_string(),
            command: command.args.clone(),
        })
}

/// The answer to a probe exec while the pod is not awake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedAnswer {
    pub exit_status: u32,
    pub output: Vec<u8>,
}

/// The cached answer to `exec`: the last result of its command while the pod
/// was awake, which a frozen app cannot change. After a failed restore, when
/// `failed_wake` says why, exit 1 with that reason. None without a result.
pub fn cached(
    probes: &PodProbes,
    exec: &ProbeExec,
    failed_wake: Option<&str>,
) -> Option<CachedAnswer> {
    if let Some(why) = failed_wake {
        return Some(CachedAnswer {
            exit_status: 1,
            output: format!("smolvm-pods: the pod could not wake: {why}\n").into_bytes(),
        });
    }
    let last = probes.exec(&exec.container, &exec.command)?;
    let exit_status = match last.code {
        Some(code) => code as u32,
        None => u32::from(!last.passed),
    };
    Some(CachedAnswer {
        exit_status,
        output: last.output.clone().into_bytes(),
    })
}

/// The result of a probe exec that exited in the awake pod, for the cache.
pub fn result(exit_status: u32, output: &[u8], at_ms: u64) -> ProbeResult {
    let output = &output[..output.len().min(EXEC_OUTPUT_MAX)];
    ProbeResult {
        passed: exit_status == 0,
        code: Some(exit_status as i32),
        output: String::from_utf8_lossy(output).into_owned(),
        at_ms,
    }
}

/// The first [`EXEC_OUTPUT_MAX`] bytes of a process's output, stdout and
/// stderr in the order they came, as kubelet keeps an exec probe's. The
/// process's output pump fills it.
#[derive(Debug, Clone, Default)]
pub struct OutputCapture(Arc<Mutex<Vec<u8>>>);

impl OutputCapture {
    pub fn push(&self, data: &[u8]) {
        let mut kept = lock(&self.0);
        let room = EXEC_OUTPUT_MAX.saturating_sub(kept.len());
        kept.extend_from_slice(&data[..data.len().min(room)]);
    }

    pub fn bytes(&self) -> Vec<u8> {
        lock(&self.0).clone()
    }
}

/// Writes a cached answer's output to the exec's stdout fifo, which the front
/// shim holds open, then closes it. Blocking, for at most [`WRITE_WAIT`].
pub fn write_output(fifo: &str, output: &[u8]) -> Result<(), String> {
    if fifo.is_empty() || output.is_empty() {
        return Ok(());
    }
    let mut file = OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(fifo)
        .map_err(|e| format!("open {fifo}: {e}"))?;
    let deadline = Instant::now() + WRITE_WAIT;
    let mut written = 0;
    while written < output.len() {
        match file.write(&output[written..]) {
            Ok(0) => return Err(format!("write {fifo}: closed")),
            Ok(n) => written += n,
            Err(e) if e.kind() == ErrorKind::WouldBlock && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(format!("write {fifo}: {e}")),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::probe::{Probe, ProbeAction, ProbeRole};

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|a| a.to_string()).collect()
    }

    const PROBE: [&str; 3] = ["/testapp", "get", "http://127.0.0.1:8080/health"];

    fn probes(last: Option<ProbeResult>) -> PodProbes {
        PodProbes {
            probes: vec![Probe {
                container: "app".into(),
                role: ProbeRole::Liveness,
                action: ProbeAction::Exec {
                    command: argv(&PROBE),
                },
                period_secs: 10,
                timeout_secs: 10,
                last,
            }],
            ready: true,
            at_ms: 1,
        }
    }

    fn command(args: &[&str], terminal: bool) -> ExecCommand {
        ExecCommand {
            args: argv(args),
            terminal,
        }
    }

    #[test]
    fn the_command_comes_from_the_process_spec() {
        let spec = r#"{"args":["/testapp","get","http://127.0.0.1:8080/health"],
            "env":["PATH=/bin"],"cwd":"/","terminal":false}"#;
        assert_eq!(ExecCommand::parse(spec), Some(command(&PROBE, false)));
        assert_eq!(
            ExecCommand::parse(r#"{"args":["sh"],"terminal":true}"#),
            Some(command(&["sh"], true))
        );
        assert_eq!(ExecCommand::parse(r#"{"args":[1]}"#), None);
        assert_eq!(ExecCommand::parse("not json"), None);
    }

    #[test]
    fn only_the_exact_command_without_a_terminal_or_stdin_is_a_probe_exec() {
        let probes = probes(None);
        let probe = command(&PROBE, false);
        let exec = probe_exec(&probes, "app", &probe, false).unwrap();
        assert_eq!(exec.container, "app");
        assert!(probe_exec(&probes, "sidecar", &probe, false).is_none());
        assert!(probe_exec(&probes, "app", &probe, true).is_none(), "stdin");
        let tty = command(&PROBE, true);
        assert!(probe_exec(&probes, "app", &tty, false).is_none());
        let other = command(&["/testapp", "get", "http://127.0.0.1:8080/"], false);
        assert!(probe_exec(&probes, "app", &other, false).is_none());
    }

    #[test]
    fn the_answer_is_the_last_result_or_the_failed_wake() {
        let exec = ProbeExec {
            container: "app".into(),
            command: argv(&PROBE),
        };
        assert_eq!(cached(&probes(None), &exec, None), None);
        let probes = probes(Some(result(0, b"ok\n", 5)));
        assert_eq!(
            cached(&probes, &exec, None),
            Some(CachedAnswer {
                exit_status: 0,
                output: b"ok\n".to_vec()
            })
        );
        let failed = cached(&probes, &exec, Some("the resume failed")).unwrap();
        assert_eq!(failed.exit_status, 1);
        assert_eq!(
            failed.output,
            b"smolvm-pods: the pod could not wake: the resume failed\n"
        );
    }

    #[test]
    fn the_capture_keeps_the_first_10_kib() {
        let capture = OutputCapture::default();
        capture.push(b"ok\n");
        capture.push(&[b'x'; EXEC_OUTPUT_MAX]);
        let bytes = capture.bytes();
        assert_eq!(bytes.len(), EXEC_OUTPUT_MAX);
        assert!(bytes.starts_with(b"ok\nx"));
        let recorded = result(3, &bytes, 9);
        assert!(!recorded.passed);
        assert_eq!(recorded.code, Some(3));
        assert_eq!(recorded.output.len(), EXEC_OUTPUT_MAX);
    }

    #[test]
    fn the_output_goes_to_the_fifo() {
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("stdout");
        let path = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
        // SAFETY: mkfifo with a valid path.
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
        // The front shim's descriptor, open for reading and writing.
        let held = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&fifo)
            .unwrap();
        write_output(fifo.to_str().unwrap(), b"ok\n").unwrap();
        let mut read = [0u8; 16];
        let n = std::io::Read::read(&mut &held, &mut read).unwrap();
        assert_eq!(&read[..n], b"ok\n");
        assert!(write_output("", b"ok\n").is_ok());
    }
}
