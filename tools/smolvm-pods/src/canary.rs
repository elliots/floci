//! The TSO canary.
//!
//! A Rosetta pod's VM process turns TSO on, and the node kernel confirms it
//! (see [`crate::boot`]), but whether the guest's vCPUs then run with it
//! depends on undocumented macOS behaviour, and a failure is silent. So before
//! the node runs Rosetta pods, the canary runs a litmus test (the one in
//! tests/e2e/workloads/litmus) in two small VMs: in one with the Rosetta share,
//! and so with TSO, it must find no memory-ordering violations, and in one
//! without, it must find some, which shows that it can. The node runs it at
//! boot (`_tso-canary`), and its verdict holds until the node restarts.
//! Unless it passed, no pod gets Rosetta: a pod that requires it is refused,
//! and one without the annotation runs without it (see [`crate::rosetta`]).

use std::path::Path;
use std::time::Duration;

use log::warn;
use serde::{Deserialize, Serialize};
use smolvm::agent::{HostMount, VmResources};
use smolvm::embedded::{EmbeddedRuntime, MachineSpec};

use crate::engine::POD_SHARE_HOST_ROOT;
use crate::files;

/// Where the node image has the litmus test, and where the VMs mount it.
const LITMUS_DIR: &str = "/opt/smolvm-pods/litmus";
const LITMUS_GUEST_DIR: &str = "/litmus";
const LITMUS_TIMEOUT: Duration = Duration::from_secs(120);

/// How often the litmus test runs in a VM before an unclear answer counts.
const ATTEMPTS: usize = 3;

/// The litmus test's exit codes.
const NO_VIOLATIONS: i32 = 0;
const VIOLATIONS: i32 = 3;
const TOO_LITTLE_OVERLAP: i32 = 4;

/// The verdict and the lock under which the canary runs, in
/// [`POD_SHARE_HOST_ROOT`]. Dot files stay when the node removes old pod
/// shares at boot.
const VERDICT_FILE: &str = ".tso-canary";
const LOCK_FILE: &str = ".tso-canary.lock";

/// The canary's verdict in one node boot.
#[derive(Debug, Serialize, Deserialize)]
pub struct Verdict {
    /// The node's boot id: TSO can behave differently after a node restart.
    boot_id: String,
    pub passed: bool,
    pub detail: String,
}

/// Fails, with the reason, unless the canary passed in this node boot, which
/// it runs first if it has not run.
pub fn require_pass() -> Result<(), String> {
    let verdict = ensure().map_err(|e| format!("the TSO canary could not run: {e}"))?;
    if verdict.passed {
        Ok(())
    } else {
        Err(failed(&verdict))
    }
}

/// The reason a failed verdict gives.
fn failed(verdict: &Verdict) -> String {
    format!("the node's TSO canary failed: {}", verdict.detail)
}

/// The reason of [`require_pass`] if the verdict of this node boot is a
/// failure. It only reads the verdict: it neither runs the canary nor waits
/// for one that runs.
pub fn failure() -> Option<String> {
    let boot_id = files::boot_id().ok()?;
    kept_in(Path::new(POD_SHARE_HOST_ROOT), &boot_id)
        .filter(|v| !v.passed)
        .map(|v| failed(&v))
}

/// The canary's verdict in this node boot. If there is none, it runs the
/// canary and keeps the verdict. An error means it could not run, and the
/// next call tries again.
pub fn ensure() -> Result<Verdict, String> {
    let boot_id = files::boot_id().map_err(|e| format!("read the node's boot id: {e}"))?;
    ensure_in(Path::new(POD_SHARE_HOST_ROOT), &boot_id, run)
}

/// The verdict kept in `root` for the node boot `boot_id`. It is written in
/// one rename, so it is read without the lock.
fn kept_in(root: &Path, boot_id: &str) -> Option<Verdict> {
    std::fs::read(root.join(VERDICT_FILE))
        .ok()
        .and_then(|raw| serde_json::from_slice::<Verdict>(&raw).ok())
        .filter(|v| v.boot_id == boot_id)
}

fn ensure_in(
    root: &Path,
    boot_id: &str,
    run: impl FnOnce() -> Result<Result<String, String>, String>,
) -> Result<Verdict, String> {
    std::fs::create_dir_all(root).map_err(|e| format!("create {}: {e}", root.display()))?;
    let lock = root.join(LOCK_FILE);
    let _lock = files::lock(&lock, 0o666, true).map_err(|e| e.message(&lock))?;
    if let Some(verdict) = kept_in(root, boot_id) {
        return Ok(verdict);
    }
    let path = root.join(VERDICT_FILE);
    let (passed, detail) = match run()? {
        Ok(detail) => (true, detail),
        Err(detail) => (false, detail),
    };
    let verdict = Verdict {
        boot_id: boot_id.to_string(),
        passed,
        detail,
    };
    let tmp = path.with_extension("new");
    let raw = serde_json::to_vec(&verdict).map_err(|e| format!("serialize the verdict: {e}"))?;
    std::fs::write(&tmp, raw)
        .and_then(|()| std::fs::rename(&tmp, &path))
        .map_err(|e| format!("write {}: {e}", path.display()))?;
    Ok(verdict)
}

/// Runs the litmus test with and without TSO. The outer error means it could
/// not run; the inner result is the verdict.
fn run() -> Result<Result<String, String>, String> {
    for template in crate::templates::expand()? {
        warn!("no disk template {template}, so smolvm formats each new VM disk");
    }
    let rt = smolvm::embedded::runtime().map_err(|e| e.to_string())?;
    let with_tso = {
        let vm = CanaryVm::start(&rt, true)?;
        match vm.litmus_until(NO_VIOLATIONS)? {
            Ok(result) => result,
            Err((VIOLATIONS, result)) => {
                return Ok(Err(format!(
                    "the litmus test found memory-ordering violations in a VM with TSO: {result}"
                )))
            }
            Err((_, result)) => {
                return Ok(Err(format!(
                    "the litmus test's threads did not run together enough to tell: {result}"
                )))
            }
        }
    };
    let vm = CanaryVm::start(&rt, false)?;
    match vm.litmus_until(VIOLATIONS)? {
        Ok(without_tso) => Ok(Ok(format!(
            "with TSO: {with_tso}; without TSO: {without_tso}"
        ))),
        Err((_, result)) => Ok(Err(format!(
            "the litmus test found no reordering in a VM without TSO, so it cannot \
             check TSO: {result}"
        ))),
    }
}

/// A small VM for the litmus test, deleted on drop.
struct CanaryVm<'a> {
    rt: &'a EmbeddedRuntime,
    name: &'static str,
}

impl<'a> CanaryVm<'a> {
    /// Boots the VM, with TSO through the Rosetta share if `tso`.
    fn start(rt: &'a EmbeddedRuntime, tso: bool) -> Result<Self, String> {
        let name = if tso {
            "smolvm-pods-canary-tso"
        } else {
            "smolvm-pods-canary-plain"
        };
        let litmus = HostMount::new(LITMUS_DIR, LITMUS_GUEST_DIR, true)
            .map_err(|e| format!("share {LITMUS_DIR}: {e}"))?;
        let mut mounts = vec![litmus];
        if tso {
            mounts.push(crate::rosetta::share()?);
        }
        let spec = MachineSpec {
            name: name.to_string(),
            mounts,
            // The litmus test pins its two threads to CPUs 0 and 1.
            resources: VmResources {
                cpus: 2,
                memory_mib: 512,
                network: false,
                ..VmResources::default()
            },
            persistent: true,
            runtime_managed: true,
            ..Default::default()
        };
        // A canary that was interrupted can leave its VM behind.
        if let Err(e) = rt.delete_machine(name) {
            warn!("delete the VM {name} of an earlier canary: {e}");
        }
        rt.create_machine(spec)
            .map_err(|e| format!("create VM {name}: {e}"))?;
        let vm = CanaryVm { rt, name };
        rt.start_machine(name)
            .map_err(|e| format!("start VM {name}: {e}"))?;
        if tso {
            let pid = rt
                .pid(name)
                .ok_or_else(|| format!("VM {name} has no process"))?;
            if !crate::boot::has_tso(&smolvm::agent::vm_data_dir(name), pid) {
                return Err(format!("the process of VM {name} did not turn TSO on"));
            }
        }
        Ok(vm)
    }

    /// Runs the litmus test until it exits with `wanted`, at most [`ATTEMPTS`]
    /// times. Violations with TSO are an answer at once. Otherwise returns the
    /// last exit code and RESULT line.
    fn litmus_until(&self, wanted: i32) -> Result<Result<String, (i32, String)>, String> {
        let mut last = (TOO_LITTLE_OVERLAP, String::new());
        for _ in 0..ATTEMPTS {
            let (code, result) = self.litmus()?;
            if code == wanted {
                return Ok(Ok(result));
            }
            if wanted == NO_VIOLATIONS && code == VIOLATIONS {
                return Ok(Err((code, result)));
            }
            last = (code, result);
        }
        Ok(Err(last))
    }

    /// Runs the litmus test once and returns its exit code and RESULT line.
    fn litmus(&self) -> Result<(i32, String), String> {
        let command = vec![format!("{LITMUS_GUEST_DIR}/litmus")];
        let (code, stdout, stderr) = self
            .rt
            .exec(self.name, command, Vec::new(), None, Some(LITMUS_TIMEOUT))
            .map_err(|e| format!("run the litmus test in VM {}: {e}", self.name))?;
        let stdout = String::from_utf8_lossy(&stdout);
        let result = stdout.lines().last().unwrap_or_default().to_string();
        match code {
            NO_VIOLATIONS | VIOLATIONS | TOO_LITTLE_OVERLAP => Ok((code, result)),
            _ => Err(format!(
                "the litmus test in VM {} exited with {code}: {}",
                self.name,
                String::from_utf8_lossy(&stderr).trim()
            )),
        }
    }
}

impl Drop for CanaryVm<'_> {
    fn drop(&mut self) {
        if let Err(e) = self.rt.delete_machine(self.name) {
            warn!("delete the canary VM {}: {e}", self.name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::Cell;

    #[test]
    fn a_verdict_holds_for_the_node_boot_it_was_made_in() {
        let root = tempfile::tempdir().unwrap();
        let runs = Cell::new(0);
        let run = |verdict: Result<String, String>| {
            let runs = &runs;
            move || {
                runs.set(runs.get() + 1);
                Ok(verdict)
            }
        };

        let first = ensure_in(root.path(), "boot-1", run(Ok("fine".into()))).unwrap();
        assert!(first.passed);
        assert_eq!(first.detail, "fine");
        let again = ensure_in(root.path(), "boot-1", run(Err("bad".into()))).unwrap();
        assert!(again.passed, "a kept verdict is not run again");
        assert_eq!(runs.get(), 1);

        let next = ensure_in(root.path(), "boot-2", run(Err("violations".into()))).unwrap();
        assert!(!next.passed, "a new node boot runs the canary again");
        assert_eq!(next.detail, "violations");
        assert_eq!(runs.get(), 2);
        let kept = ensure_in(root.path(), "boot-2", run(Ok("fine".into()))).unwrap();
        assert!(!kept.passed, "a failure is kept too");
        assert_eq!(runs.get(), 2);
    }

    #[test]
    fn a_failure_is_read_without_running_the_canary() {
        let root = tempfile::tempdir().unwrap();
        assert!(kept_in(root.path(), "boot-1").is_none());
        ensure_in(root.path(), "boot-1", || Ok(Err("violations".into()))).unwrap();
        let kept = kept_in(root.path(), "boot-1").unwrap();
        assert!(!kept.passed);
        assert_eq!(failed(&kept), "the node's TSO canary failed: violations");
        assert!(kept_in(root.path(), "boot-2").is_none());
    }

    #[test]
    fn a_canary_that_could_not_run_runs_again() {
        let root = tempfile::tempdir().unwrap();
        let err = ensure_in(root.path(), "boot-1", || Err("no VM".into())).unwrap_err();
        assert_eq!(err, "no VM");
        assert!(!root.path().join(VERDICT_FILE).exists());
        let verdict = ensure_in(root.path(), "boot-1", || Ok(Ok("fine".into()))).unwrap();
        assert!(verdict.passed);
    }
}
