//! Running commands with a deadline.

use std::fs::File;
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// What a finished command printed, and how it exited.
#[derive(Debug)]
pub struct Output {
    pub status: ExitStatus,
    pub stdout: String,
    pub stderr: String,
}

impl Output {
    pub fn success(&self) -> bool {
        self.status.success()
    }

    pub fn code(&self) -> Option<i32> {
        self.status.code()
    }
}

/// Describes how a command ended, for an error message.
pub fn describe(output: &Output) -> String {
    format!(
        "{}\nstdout:\n{}\nstderr:\n{}",
        output.status, output.stdout, output.stderr
    )
}

/// Runs `cmd` with stdin closed, and kills it if it runs longer than
/// `timeout`. The result is an error only if the command could not run or
/// timed out.
pub fn run(cmd: &mut Command, timeout: Duration) -> Result<Output, String> {
    run_with_input(cmd, Stdio::null(), timeout)
}

/// Like [`run`], but returns stdout, and an error unless `cmd` exits with 0.
pub fn run_ok(cmd: &mut Command, timeout: Duration) -> Result<String, String> {
    let output = run(cmd, timeout)?;
    checked(cmd, output)
}

/// Like [`run_ok`], but with `input` as stdin.
pub fn run_ok_with_input(
    cmd: &mut Command,
    input: File,
    timeout: Duration,
) -> Result<String, String> {
    let output = run_with_input(cmd, input.into(), timeout)?;
    checked(cmd, output)
}

/// Like [`run`], but with `input` as stdin.
pub fn run_with_stdin(cmd: &mut Command, input: File, timeout: Duration) -> Result<Output, String> {
    run_with_input(cmd, input.into(), timeout)
}

fn checked(cmd: &Command, output: Output) -> Result<String, String> {
    if !output.success() {
        return Err(format!("{cmd:?} failed: {}", describe(&output)));
    }
    Ok(output.stdout)
}

/// The output goes to files rather than pipes, so that a background process
/// that inherits them cannot keep the caller waiting.
fn run_with_input(cmd: &mut Command, input: Stdio, timeout: Duration) -> Result<Output, String> {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let base = std::env::temp_dir().join(format!(
        "floci-e2e-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let (out, err) = (base.with_extension("out"), base.with_extension("err"));
    let result = run_to(cmd, input, timeout, &out, &err);
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(&err);
    result
}

fn run_to(
    cmd: &mut Command,
    input: Stdio,
    timeout: Duration,
    out: &Path,
    err: &Path,
) -> Result<Output, String> {
    let create = |path: &Path| File::create(path).map_err(|e| format!("{}: {e}", path.display()));
    cmd.stdin(input).stdout(create(out)?).stderr(create(err)?);
    let mut child = cmd.spawn().map_err(|e| format!("start {cmd:?}: {e}"))?;
    let deadline = Instant::now() + timeout;
    let mut pause = Duration::from_millis(5);
    let status = loop {
        if let Some(status) = child.try_wait().map_err(|e| format!("wait: {e}"))? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("{cmd:?} did not finish within {timeout:?}"));
        }
        thread::sleep(pause);
        pause = (pause * 2).min(Duration::from_millis(100));
    };
    let read = |path: &Path| {
        std::fs::read(path)
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .map_err(|e| format!("{}: {e}", path.display()))
    };
    Ok(Output {
        status,
        stdout: read(out)?,
        stderr: read(err)?,
    })
}
