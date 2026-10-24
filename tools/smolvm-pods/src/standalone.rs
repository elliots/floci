//! Containers that did not come from CRI, such as Docker's and `ctr run`'s.
//!
//! Each is a pod of its own: the node service boots a pod VM for it and
//! creates the container in that VM from the same bundle, under the task's
//! id, with no sandbox task. Its start, execs, signals, stats and delete then
//! go through the paths of a pod's containers, and its delete deletes the VM.
//!
//! Its OCI spec asks for a new network namespace, with no path. The node
//! service makes it as runc does: a holder process (`_netns-holder`)
//! unshares one, which is bound at `NETNS_DIR/ID`, and the spec's `prestart`
//! and `createRuntime` hooks run with an OCI state whose pid is the
//! holder's. Docker's hook, `libnetwork-setkey`, has dockerd move the
//! container's veth and address into the namespace of that pid. The holder
//! then exits, and the bind keeps the namespace, which is the pod's network
//! namespace from then on, as CNI's is for a Kubernetes pod. A namespace that
//! the hooks left without an `eth0` with an IPv4 address, as `docker run
//! --network none` and `ctr run` leave it, is removed, and the VM has no
//! network. `poststart` hooks run after the container starts, and `poststop`
//! hooks after its delete.

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use log::{info, warn};

/// Where the network namespaces of standalone containers are bound.
pub const NETNS_DIR: &str = "/run/smolvm-pods/netns";

/// How long a hook may run when it sets no timeout of its own.
const HOOK_TIMEOUT: Duration = Duration::from_secs(60);

/// What the holder prints once it is in its new network namespace.
const READY: &str = "ready";

/// The path of the network namespace of standalone container `id`.
pub fn netns_path(id: &str) -> PathBuf {
    Path::new(NETNS_DIR).join(id)
}

/// Refuses a container whose network a pod VM cannot have: one with no
/// network namespace of its own, which shares the node's, and one that joins
/// another's by path, such as `docker run --network container:NAME`.
pub fn check_network(spec: &serde_json::Value) -> Result<(), String> {
    let namespaces = spec
        .pointer("/linux/namespaces")
        .and_then(|n| n.as_array())
        .map(Vec::as_slice)
        .unwrap_or_default();
    let network = namespaces
        .iter()
        .find(|n| n.get("type").and_then(|t| t.as_str()) == Some("network"));
    let Some(network) = network else {
        return Err(
            "the container shares the node's network namespace (host networking), \
                    which a pod VM cannot join"
                .into(),
        );
    };
    match network.get("path").and_then(|p| p.as_str()) {
        Some(path) if !path.is_empty() => Err(format!(
            "the container joins the network namespace {path}, and smolvm-pods runs each \
             container in a network namespace of its own"
        )),
        _ => Ok(()),
    }
}

/// Makes the network namespace of standalone container `id` and runs the
/// spec's `prestart` and `createRuntime` hooks in it. Returns the namespace
/// when it has the pod's `eth0`, and removes it otherwise. Blocking.
pub fn create_netns(
    id: &str,
    bundle: &str,
    spec: &serde_json::Value,
) -> Result<Option<String>, String> {
    let path = netns_path(id);
    remove_netns(id);
    let made = hold_netns(&path).and_then(|holder| {
        let hooked = ["prestart", "createRuntime"].iter().try_for_each(|kind| {
            let state = state(spec, id, bundle, "creating", Some(holder.id()));
            run_hooks(spec, kind, &state)
        });
        release(holder);
        hooked
    });
    if let Err(e) = made {
        remove_netns(id);
        return Err(e);
    }
    let netns = path.to_string_lossy().into_owned();
    match crate::net::pod_network(&netns) {
        Ok((ip, _)) => {
            info!("container {id} has the network namespace {netns}, with {ip}");
            Ok(Some(netns))
        }
        Err(e) => {
            info!("container {id} gets a VM without a network: its namespace has no address: {e}");
            remove_netns(id);
            Ok(None)
        }
    }
}

/// Starts a holder in a new network namespace and binds that namespace at
/// `path`.
fn hold_netns(path: &Path) -> Result<Child, String> {
    let dir = path.parent().ok_or("the namespace path has no directory")?;
    std::fs::create_dir_all(dir).map_err(|e| format!("mkdir {}: {e}", dir.display()))?;
    let exe = std::env::current_exe().map_err(|e| format!("find this binary: {e}"))?;
    let mut holder = Command::new(exe)
        .arg("_netns-holder")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| format!("start the network namespace holder: {e}"))?;
    let mut line = String::new();
    let ready = holder
        .stdout
        .take()
        .map(|out| BufReader::new(out).read_line(&mut line));
    if !matches!(ready, Some(Ok(_))) || line.trim() != READY {
        release(holder);
        return Err(format!(
            "the network namespace holder did not start: {}",
            line.trim()
        ));
    }
    let bound = std::fs::File::create(path)
        .map_err(|e| format!("create {}: {e}", path.display()))
        .and_then(|_| {
            let source = format!("/proc/{}/ns/net", holder.id());
            nix::mount::mount(
                Some(source.as_str()),
                path,
                None::<&str>,
                nix::mount::MsFlags::MS_BIND,
                None::<&str>,
            )
            .map_err(|e| format!("bind {source} at {}: {e}", path.display()))
        });
    match bound {
        Ok(()) => Ok(holder),
        Err(e) => {
            release(holder);
            Err(e)
        }
    }
}

/// Ends a holder: it exits when its stdin closes.
fn release(mut holder: Child) {
    drop(holder.stdin.take());
    if let Err(e) = holder.wait() {
        warn!("wait for the network namespace holder: {e}");
    }
}

/// Removes the network namespace of standalone container `id`, if it has
/// one. Blocking.
pub fn remove_netns(id: &str) {
    let path = netns_path(id);
    if !path.exists() {
        return;
    }
    match nix::mount::umount2(&path, nix::mount::MntFlags::MNT_DETACH) {
        Ok(()) | Err(nix::errno::Errno::EINVAL) => {}
        Err(e) => warn!("unmount {}: {e}", path.display()),
    }
    if let Err(e) = std::fs::remove_file(&path) {
        warn!("remove {}: {e}", path.display());
    }
}

/// The holder's process: unshare a network namespace, say so, and stay in
/// it until stdin closes.
pub fn holder() -> i32 {
    if let Err(e) = nix::sched::unshare(nix::sched::CloneFlags::CLONE_NEWNET) {
        eprintln!("unshare a network namespace: {e}");
        return 1;
    }
    println!("{READY}");
    let _ = std::io::stdout().flush();
    let mut rest = Vec::new();
    let _ = std::io::stdin().read_to_end(&mut rest);
    0
}

/// The OCI state of standalone container `id`, which its hooks read on
/// stdin. The pid is required while the container is created or running.
pub fn state(
    spec: &serde_json::Value,
    id: &str,
    bundle: &str,
    status: &str,
    pid: Option<u32>,
) -> serde_json::Value {
    let mut state = serde_json::json!({
        "ociVersion": spec.get("ociVersion").and_then(|v| v.as_str()).unwrap_or("1.0.2"),
        "id": id,
        "status": status,
        "bundle": bundle,
    });
    if let Some(pid) = pid {
        state["pid"] = pid.into();
    }
    if let Some(annotations) = spec.get("annotations").filter(|a| a.is_object()) {
        state["annotations"] = annotations.clone();
    }
    state
}

/// Runs the spec's hooks of `kind`, one after another, each with `state` on
/// its stdin, its own arguments and environment, and its timeout. The first
/// that fails fails the call. Blocking.
pub fn run_hooks(
    spec: &serde_json::Value,
    kind: &str,
    state: &serde_json::Value,
) -> Result<(), String> {
    let hooks = spec
        .pointer(&format!("/hooks/{kind}"))
        .and_then(|h| h.as_array())
        .map(Vec::as_slice)
        .unwrap_or_default();
    for hook in hooks {
        run_hook(hook, state).map_err(|e| format!("{kind} hook: {e}"))?;
    }
    Ok(())
}

fn strings(value: Option<&serde_json::Value>) -> Vec<String> {
    value
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn run_hook(hook: &serde_json::Value, state: &serde_json::Value) -> Result<(), String> {
    let path = hook
        .get("path")
        .and_then(|p| p.as_str())
        .ok_or("a hook has no path")?;
    let args = strings(hook.get("args"));
    let env = strings(hook.get("env"));
    let timeout = hook
        .get("timeout")
        .and_then(|t| t.as_u64())
        .filter(|t| *t > 0)
        .map_or(HOOK_TIMEOUT, Duration::from_secs);
    let mut command = Command::new(path);
    if let Some((argv0, rest)) = args.split_first() {
        command.arg0(argv0).args(rest);
    }
    command.env_clear();
    for var in &env {
        if let Some((name, value)) = var.split_once('=') {
            command.env(name, value);
        }
    }
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("start {path}: {e}"))?;
    if let Some(mut stdin) = child.stdin.take() {
        // A hook that does not read its state may exit before it is written.
        let _ = stdin.write_all(state.to_string().as_bytes());
    }
    let output = std::thread::scope(|scope| {
        let (stdout, stderr) = (child.stdout.take(), child.stderr.take());
        let out = scope.spawn(move || read_all(stdout));
        let err = scope.spawn(move || read_all(stderr));
        let status = wait_with_timeout(&mut child, timeout);
        let (out, err) = (
            out.join().unwrap_or_default(),
            err.join().unwrap_or_default(),
        );
        status.map(|status| (status, out, err))
    });
    let (status, out, err) = output.map_err(|e| format!("{path}: {e}"))?;
    if status.success() {
        return Ok(());
    }
    let said = [err.trim(), out.trim()]
        .into_iter()
        .find(|s| !s.is_empty())
        .unwrap_or("no output");
    Err(format!("{path} exited with {status}: {said}"))
}

fn read_all(pipe: Option<impl Read>) -> String {
    let mut text = String::new();
    if let Some(mut pipe) = pipe {
        let _ = pipe.read_to_string(&mut text);
    }
    text
}

/// Waits for `child`, and kills it once `timeout` has passed.
fn wait_with_timeout(
    child: &mut Child,
    timeout: Duration,
) -> Result<std::process::ExitStatus, String> {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status),
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("did not exit within {timeout:?}"));
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(10)),
            Err(e) => return Err(format!("wait: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(namespaces: serde_json::Value) -> serde_json::Value {
        serde_json::json!({"ociVersion": "1.2.0", "linux": {"namespaces": namespaces}})
    }

    #[test]
    fn a_new_network_namespace_is_accepted() {
        let new = spec(serde_json::json!([{"type": "pid"}, {"type": "network"}]));
        assert_eq!(check_network(&new), Ok(()));
        let empty = spec(serde_json::json!([{"type": "network", "path": ""}]));
        assert_eq!(check_network(&empty), Ok(()));
    }

    #[test]
    fn host_networking_and_a_joined_namespace_are_refused() {
        let host = check_network(&spec(serde_json::json!([{"type": "pid"}]))).unwrap_err();
        assert!(host.contains("host networking"), "{host}");
        let joined = spec(serde_json::json!([{"type": "network", "path": "/proc/7/ns/net"}]));
        let joined = check_network(&joined).unwrap_err();
        assert!(joined.contains("/proc/7/ns/net"), "{joined}");
    }

    #[test]
    fn the_state_has_the_pid_only_when_given() {
        let spec = serde_json::json!({"ociVersion": "1.2.0", "annotations": {"a": "b"}});
        let creating = state(&spec, "c1", "/b", "creating", Some(42));
        assert_eq!(
            creating,
            serde_json::json!({"ociVersion": "1.2.0", "id": "c1", "status": "creating",
                "bundle": "/b", "pid": 42, "annotations": {"a": "b"}})
        );
        let stopped = state(&serde_json::json!({}), "c1", "/b", "stopped", None);
        assert_eq!(
            stopped,
            serde_json::json!({"ociVersion": "1.0.2", "id": "c1", "status": "stopped", "bundle": "/b"})
        );
    }

    #[test]
    fn hooks_get_their_args_env_and_the_state() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        let script = format!(
            "printf '%s|%s|%s|' \"$0\" \"$1\" \"$HOOK_VAR\" >{0}; cat >>{0}",
            out.display()
        );
        let spec = serde_json::json!({"hooks": {"prestart": [{
            "path": "/bin/sh",
            "args": ["setkey", "-c", script, "zero", "one"],
            "env": ["HOOK_VAR=value"],
        }]}});
        let state = state(&spec, "c1", "/b", "creating", Some(7));
        run_hooks(&spec, "prestart", &state).unwrap();
        let written = std::fs::read_to_string(&out).unwrap();
        let (head, json) = written.rsplit_once('|').unwrap();
        assert_eq!(head, "zero|one|value");
        let read: serde_json::Value = serde_json::from_str(json).unwrap();
        assert_eq!(read["pid"], 7);
        assert_eq!(read["id"], "c1");
    }

    #[test]
    fn a_failing_hook_fails_with_its_output() {
        let spec = serde_json::json!({"hooks": {"createRuntime": [{
            "path": "/bin/sh",
            "args": ["sh", "-c", "echo no network >&2; exit 3"],
        }]}});
        let error = run_hooks(&spec, "createRuntime", &serde_json::json!({})).unwrap_err();
        assert!(error.contains("createRuntime hook"), "{error}");
        assert!(error.contains("no network"), "{error}");
        assert!(run_hooks(&spec, "prestart", &serde_json::json!({})).is_ok());
    }

    #[test]
    fn a_hook_that_runs_past_its_timeout_is_killed() {
        let spec = serde_json::json!({"hooks": {"poststop": [{
            "path": "/bin/sleep",
            "args": ["sleep", "30"],
            "timeout": 1,
        }]}});
        let started = Instant::now();
        let error = run_hooks(&spec, "poststop", &serde_json::json!({})).unwrap_err();
        assert!(error.contains("did not exit within"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(10));
    }
}
