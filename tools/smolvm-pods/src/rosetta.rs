//! Rosetta for linux/amd64 pods on Apple silicon nodes.
//!
//! On an arm64 node, every pod's VM gets Rosetta unless the pod turns it off
//! with the [`ANN_ROSETTA`] annotation (see [`decide`]). The VM gets the
//! node's Rosetta runtime as a plain read-only share at /mnt/rosetta, not
//! through smolvm's `rosetta` option, because pause refuses a VM with that
//! option. After boot the guest registers Rosetta with binfmt_misc as smolvm's
//! agent does for that option, and every container mounts the share at
//! /mnt/rosetta, where the registered wrapper runs the translator from.
//!
//! The share is also the VM's request for TSO: see [`crate::boot`]. A
//! container whose entrypoint is an x86-64 binary in a VM without Rosetta is
//! refused with the reason the VM has none.

use std::io::Read;
use std::path::{Component, Path, PathBuf};

use log::{info, warn};
use smolvm::agent::HostMount;
use smolvm_protocol::ROSETTA_GUEST_PATH;

use crate::bundle;

/// Pod annotation that turns Rosetta off (`"false"`) or requires it
/// (`"true"`). containerd passes it to the shim, on the sandbox's spec and on
/// each container's, only if the runtime's `pod_annotations` allows it.
pub const ANN_ROSETTA: &str = "smolvm-pods.floci.io/rosetta";

/// Where a node that is itself a smolvm VM with Rosetta has the runtime.
const NODE_ROSETTA_DIR: &str = ROSETTA_GUEST_PATH;

/// The ELF machine of x86-64 binaries.
const EM_X86_64: u16 = 62;

/// The `PATH` of a container whose spec sets none, as in runc and crun.
const DEFAULT_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

/// smolvm's binfmt_misc entry (crates/smolvm-agent/src/rosetta.rs): x86_64 ELF
/// binaries run through the ptrace wrapper in the agent rootfs. `F` opens the
/// wrapper at registration, so containers without the agent rootfs can use it.
const BINFMT_ENTRY: &str = r":rosetta:M::\x7fELF\x02\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x3e\x00:\xff\xff\xff\xff\xff\xfe\xfe\x00\xff\xff\xff\xff\xff\xff\xff\xff\xfe\xff\xff\xff:/usr/bin/rosetta-wrapper:F";

/// What a pod's [`ANN_ROSETTA`] annotation asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Request {
    /// No annotation: Rosetta if the node can give it.
    Default,
    /// `"true"`: Rosetta, or the pod is refused.
    On,
    /// `"false"`: no Rosetta.
    Off,
}

/// The request in an OCI spec's annotations. A value other than true, 1,
/// false or 0 is refused, so that a "no" cannot turn Rosetta on.
fn request(spec: Option<&serde_json::Value>) -> Result<Request, String> {
    let Some(value) = spec
        .and_then(|v| v.pointer("/annotations"))
        .and_then(|a| a.get(ANN_ROSETTA))
    else {
        return Ok(Request::Default);
    };
    match value.as_str().and_then(bundle::parse_flag) {
        Some(true) => Ok(Request::On),
        Some(false) => Ok(Request::Off),
        None => Err(format!(
            "the pod's annotation {ANN_ROSETTA} is {value}, which is neither \"true\" nor \"false\""
        )),
    }
}

/// Whether a pod's VM gets Rosetta.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    /// The VM gets the Rosetta share, and with it TSO.
    pub enabled: bool,
    /// Why a pod without the annotation runs without Rosetta. [`decide`] logs
    /// it, and the pod's event can carry it.
    pub warning: Option<String>,
}

/// Whether the VM of sandbox `id`, with the OCI spec `spec`, gets Rosetta.
/// On an arm64 node:
///
/// | Annotation | Rosetta runtime and TSO canary passed | Otherwise |
/// | --- | --- | --- |
/// | none | Rosetta | no Rosetta, with a warning |
/// | `"true"` | Rosetta | refused with the reason |
/// | `"false"` | no Rosetta | no Rosetta |
///
/// An x86_64 node runs amd64 natively and ignores the annotation. Without a
/// canary verdict in this node boot, the canary runs first.
pub fn decide(id: &str, spec: Option<&serde_json::Value>) -> Result<Decision, String> {
    if !cfg!(target_arch = "aarch64") {
        return Ok(Decision {
            enabled: false,
            warning: None,
        });
    }
    let decision = decide_with(
        request(spec)?,
        Path::new(NODE_ROSETTA_DIR),
        crate::canary::require_pass,
    )?;
    match &decision.warning {
        Some(warning) => warn!("sandbox {id}: {warning}"),
        None if decision.enabled => info!("sandbox {id}: Rosetta and TSO"),
        None => info!("sandbox {id}: no Rosetta: the pod has {ANN_ROSETTA}: \"false\""),
    }
    Ok(decision)
}

/// [`decide`] on an arm64 node whose Rosetta runtime is in `dir`. `canary`
/// fails with the reason unless the TSO canary passed.
fn decide_with(
    request: Request,
    dir: &Path,
    canary: impl FnOnce() -> Result<(), String>,
) -> Result<Decision, String> {
    if request == Request::Off {
        return Ok(Decision {
            enabled: false,
            warning: None,
        });
    }
    let available = match missing_runtime(dir) {
        Some(reason) => Err(reason),
        None => canary(),
    };
    match (available, request) {
        (Ok(()), _) => Ok(Decision {
            enabled: true,
            warning: None,
        }),
        (Err(reason), Request::On) => Err(format!(
            "the pod asks for Rosetta with {ANN_ROSETTA}: \"true\", but {reason}"
        )),
        (Err(reason), _) => Ok(Decision {
            enabled: false,
            warning: Some(format!("the pod runs without Rosetta, because {reason}")),
        }),
    }
}

/// Why a node whose Rosetta runtime would be in `dir` cannot give a VM
/// Rosetta, if it has no runtime there.
fn missing_runtime(dir: &Path) -> Option<String> {
    (!dir.join("rosetta").is_file())
        .then(|| format!("the node has no Rosetta runtime at {}", dir.display()))
}

/// The node's Rosetta runtime as a read-only share at /mnt/rosetta.
pub fn share() -> Result<HostMount, String> {
    share_from(Path::new(NODE_ROSETTA_DIR))
}

pub(crate) fn share_from(dir: &Path) -> Result<HostMount, String> {
    if let Some(reason) = missing_runtime(dir) {
        return Err(reason);
    }
    HostMount::new(dir, ROSETTA_GUEST_PATH, true).map_err(|e| format!("Rosetta share: {e}"))
}

/// Whether a VM's mounts include the Rosetta share.
pub fn is_shared(mounts: &[HostMount]) -> bool {
    mounts
        .iter()
        .any(|m| m.target == Path::new(ROSETTA_GUEST_PATH))
}

/// Shell script that registers Rosetta in the guest kernel and fails if it
/// cannot. Run once after the first boot: the registration is guest memory,
/// so it survives hibernation.
pub fn guest_setup_script() -> String {
    format!(
        "set -e; \
         fail() {{ echo \"$*\" >&2; exit 1; }}; \
         test -f {ROSETTA_GUEST_PATH}/rosetta || fail 'no Rosetta runtime at {ROSETTA_GUEST_PATH}'; \
         test -x /usr/bin/rosetta-wrapper || fail 'the agent rootfs has no /usr/bin/rosetta-wrapper'; \
         d=/proc/sys/fs/binfmt_misc; \
         test -e $d/register || mount -t binfmt_misc binfmt_misc $d; \
         test -e $d/rosetta || printf '%s' '{BINFMT_ENTRY}' > $d/register; \
         grep -qx enabled $d/rosetta || fail 'binfmt_misc entry rosetta is not enabled'"
    )
}

/// Add the VM's Rosetta share, virtiofs tag `tag`, to a container's OCI spec
/// at /mnt/rosetta. The agent binds only pod-share paths into containers, but
/// passes other mount types to crun unchanged.
pub fn add_to_container(spec_json: &str, tag: &str) -> Result<String, String> {
    let mut spec: serde_json::Value =
        serde_json::from_str(spec_json).map_err(|e| format!("parse config.json: {e}"))?;
    let mounts = spec
        .as_object_mut()
        .ok_or("config.json is not an object")?
        .entry("mounts")
        .or_insert_with(|| serde_json::Value::Array(Vec::new()))
        .as_array_mut()
        .ok_or("config.json mounts is not an array")?;
    let taken = mounts
        .iter()
        .any(|m| m.get("destination").and_then(|d| d.as_str()) == Some(ROSETTA_GUEST_PATH));
    if !taken {
        mounts.push(serde_json::json!({
            "destination": ROSETTA_GUEST_PATH,
            "type": "virtiofs",
            "source": tag,
            "options": ["ro", "nosuid", "nodev"],
        }));
    }
    serde_json::to_string(&spec).map_err(|e| format!("reserialize spec: {e}"))
}

/// Fails when the program of a container, with its root file system at
/// `rootfs`, is an x86-64 binary, which a VM without Rosetta cannot run.
pub fn refuse_amd64_entrypoint(rootfs: &Path, spec_json: &str) -> Result<(), String> {
    let spec: serde_json::Value =
        serde_json::from_str(spec_json).map_err(|e| format!("parse config.json: {e}"))?;
    let Some((program, file)) = entrypoint(rootfs, &spec) else {
        return Ok(());
    };
    if elf_machine(&file) == Some(EM_X86_64) {
        let reason = without_rosetta(&spec, Path::new(NODE_ROSETTA_DIR), crate::canary::failure);
        return Err(format!(
            "the container runs {program}, an x86-64 binary, and the pod's VM has no Rosetta, \
             because {reason}"
        ));
    }
    Ok(())
}

/// Why the VM of a container with the OCI spec `spec` has no Rosetta, on a
/// node whose runtime would be in `dir`. containerd copies the pod's
/// annotation onto each container's spec. Without it, the reason is the one
/// [`decide`] warned of, as far as the node still knows it: `canary` is the
/// failure of the TSO canary in this node boot, if it failed.
fn without_rosetta(
    spec: &serde_json::Value,
    dir: &Path,
    canary: impl FnOnce() -> Option<String>,
) -> String {
    if request(Some(spec)) == Ok(Request::Off) {
        return format!("the pod has the annotation {ANN_ROSETTA}: \"false\"");
    }
    missing_runtime(dir).or_else(canary).unwrap_or_else(|| {
        "the node could not give it Rosetta when the pod started (see the node service's log)"
            .to_string()
    })
}

/// The program that the spec's process runs, and the file it is in `rootfs`.
fn entrypoint(rootfs: &Path, spec: &serde_json::Value) -> Option<(String, PathBuf)> {
    let process = spec.get("process")?;
    let program = process.get("args")?.get(0)?.as_str()?;
    let candidates = if program.contains('/') {
        let cwd = process.get("cwd").and_then(|c| c.as_str()).unwrap_or("/");
        vec![Path::new(cwd).join(program)]
    } else {
        let path = process
            .get("env")
            .and_then(|e| e.as_array())
            .and_then(|env| {
                env.iter()
                    .filter_map(|v| v.as_str())
                    .find_map(|v| v.strip_prefix("PATH="))
            })
            .unwrap_or(DEFAULT_PATH);
        path.split(':')
            .map(|dir| Path::new(dir).join(program))
            .collect()
    };
    candidates.into_iter().find_map(|candidate| {
        let file = resolve(rootfs, &candidate)?;
        Some((candidate.display().to_string(), file))
    })
}

/// The regular file that the absolute path `path` in `rootfs` names, after
/// symbolic links at its end, which point inside `rootfs`.
fn resolve(rootfs: &Path, path: &Path) -> Option<PathBuf> {
    let mut path = path.to_path_buf();
    for _ in 0..40 {
        let file = rootfs.join(lexical(&path));
        let meta = std::fs::symlink_metadata(&file).ok()?;
        if !meta.file_type().is_symlink() {
            return meta.is_file().then_some(file);
        }
        let target = std::fs::read_link(&file).ok()?;
        path = path.parent()?.join(target);
    }
    None
}

/// `path` relative to the root it is in, without `.` or `..`, which stop at
/// the root.
fn lexical(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for part in path.components() {
        match part {
            Component::Normal(name) => out.push(name),
            Component::ParentDir => {
                out.pop();
            }
            Component::RootDir | Component::CurDir | Component::Prefix(_) => {}
        }
    }
    out
}

/// The ELF machine of the file at `path`, if it is an ELF file.
fn elf_machine(path: &Path) -> Option<u16> {
    let mut header = [0u8; 20];
    std::fs::File::open(path)
        .ok()?
        .read_exact(&mut header)
        .ok()?;
    if header[..4] != *b"\x7fELF" {
        return None;
    }
    let machine = [header[18], header[19]];
    match header[5] {
        1 => Some(u16::from_le_bytes(machine)),
        2 => Some(u16::from_be_bytes(machine)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec_with(annotations: serde_json::Value) -> serde_json::Value {
        serde_json::json!({ "annotations": annotations })
    }

    #[test]
    fn the_annotation_turns_rosetta_off_or_requires_it() {
        assert_eq!(request(None), Ok(Request::Default));
        assert_eq!(request(Some(&serde_json::json!({}))), Ok(Request::Default));
        let other = spec_with(serde_json::json!({ "other": "false" }));
        assert_eq!(request(Some(&other)), Ok(Request::Default));
        let annotated =
            |v: serde_json::Value| request(Some(&spec_with(serde_json::json!({ ANN_ROSETTA: v }))));
        for v in ["true", "1", "TRUE", " true "] {
            assert_eq!(annotated(v.into()), Ok(Request::On), "{v:?}");
        }
        for v in ["false", "0", "False", " false"] {
            assert_eq!(annotated(v.into()), Ok(Request::Off), "{v:?}");
        }
        for v in ["", "yes", "no", "amd64"] {
            let err = annotated(v.into()).unwrap_err();
            assert!(
                err.contains(ANN_ROSETTA) && err.contains(&format!("{v:?}")),
                "{err}"
            );
        }
        assert!(annotated(serde_json::json!(true)).is_err());
    }

    /// A node directory with a Rosetta runtime.
    fn node_with_rosetta() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("rosetta"), b"").unwrap();
        dir
    }

    fn rosetta() -> Decision {
        Decision {
            enabled: true,
            warning: None,
        }
    }

    fn no_rosetta() -> Decision {
        Decision {
            enabled: false,
            warning: None,
        }
    }

    #[test]
    fn rosetta_is_on_by_default_where_the_canary_passed() {
        let node = node_with_rosetta();
        for request in [Request::Default, Request::On] {
            let decision = decide_with(request, node.path(), || Ok(())).unwrap();
            assert_eq!(decision, rosetta(), "{request:?}");
        }
        let decision = decide_with(Request::Off, node.path(), || panic!("no canary")).unwrap();
        assert_eq!(decision, no_rosetta());
    }

    #[test]
    fn without_a_passed_canary_only_the_default_runs_without_rosetta() {
        let node = node_with_rosetta();
        let failed = || Err("the node's TSO canary failed: violations".to_string());

        let decision = decide_with(Request::Default, node.path(), failed).unwrap();
        assert!(!decision.enabled);
        let warning = decision.warning.unwrap();
        assert!(warning.contains("without Rosetta"), "{warning}");
        assert!(
            warning.contains("TSO canary failed: violations"),
            "{warning}"
        );

        let err = decide_with(Request::On, node.path(), failed).unwrap_err();
        assert!(err.contains("TSO canary failed: violations"), "{err}");
        assert!(err.contains(ANN_ROSETTA), "{err}");

        let decision = decide_with(Request::Off, node.path(), failed).unwrap();
        assert_eq!(decision, no_rosetta());
    }

    #[test]
    fn a_node_without_a_rosetta_runtime_runs_no_canary() {
        let node = tempfile::tempdir().unwrap();
        let canary = || -> Result<(), String> { panic!("the canary ran") };
        let decision = decide_with(Request::Default, node.path(), canary).unwrap();
        assert!(!decision.enabled);
        assert!(
            decision
                .warning
                .as_deref()
                .unwrap()
                .contains("no Rosetta runtime"),
            "{decision:?}"
        );
        let err = decide_with(Request::On, node.path(), canary).unwrap_err();
        assert!(err.contains("no Rosetta runtime"), "{err}");
    }

    #[test]
    fn the_share_needs_a_rosetta_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let err = share_from(dir.path()).unwrap_err();
        assert!(err.contains("no Rosetta runtime"), "{err}");

        std::fs::write(dir.path().join("rosetta"), b"").unwrap();
        let mount = share_from(dir.path()).unwrap();
        assert_eq!(mount.target, Path::new(ROSETTA_GUEST_PATH));
        assert!(mount.read_only);
        assert!(is_shared(&[mount]));
    }

    #[test]
    fn the_guest_script_registers_the_entry_verbatim() {
        let script = guest_setup_script();
        assert!(script.starts_with("set -e;"));
        assert!(script.contains("/usr/bin/rosetta-wrapper"));
        assert!(script.contains(&format!("printf '%s' '{BINFMT_ENTRY}'")));
        assert!(!BINFMT_ENTRY.contains('\''));
        assert!(BINFMT_ENTRY.contains(r"\x7fELF"));
        let syntax = std::process::Command::new("sh")
            .args(["-n", "-c", &script])
            .status()
            .unwrap();
        assert!(syntax.success(), "the guest script is not valid sh");
    }

    #[test]
    fn containers_mount_the_share_once() {
        let spec = serde_json::json!({
            "mounts": [{"destination": "/data", "type": "bind", "source": "/x"}]
        })
        .to_string();
        let once = add_to_container(&spec, "smolvm1").unwrap();
        let twice = add_to_container(&once, "smolvm1").unwrap();
        assert_eq!(once, twice);

        let v: serde_json::Value = serde_json::from_str(&once).unwrap();
        let mounts = v["mounts"].as_array().unwrap();
        assert_eq!(mounts.len(), 2);
        assert_eq!(mounts[1]["destination"], ROSETTA_GUEST_PATH);
        assert_eq!(mounts[1]["type"], "virtiofs");
        assert_eq!(mounts[1]["source"], "smolvm1");
        assert_eq!(mounts[1]["options"][0], "ro");

        let bare = add_to_container("{}", "smolvm1").unwrap();
        let v: serde_json::Value = serde_json::from_str(&bare).unwrap();
        assert_eq!(v["mounts"][0]["source"], "smolvm1");
    }

    /// The start of a 64-bit little-endian ELF header for `machine`.
    fn elf(machine: u16) -> Vec<u8> {
        let mut header = b"\x7fELF\x02\x01\x01".to_vec();
        header.resize(18, 0);
        header.extend_from_slice(&machine.to_le_bytes());
        header.resize(64, 0);
        header
    }

    fn spec(args: &[&str], env: &[&str]) -> String {
        serde_json::json!({"process": {"args": args, "env": env, "cwd": "/work"}}).to_string()
    }

    #[test]
    fn a_refused_amd64_entrypoint_says_why_the_vm_has_no_rosetta() {
        let off = spec_with(serde_json::json!({ ANN_ROSETTA: "false" }));
        let node = node_with_rosetta();
        let reason = without_rosetta(&off, node.path(), || panic!("no canary"));
        assert!(
            reason.contains(&format!("{ANN_ROSETTA}: \"false\"")),
            "{reason}"
        );

        let default = serde_json::json!({});
        let failed = || Some("the node's TSO canary failed: violations".to_string());
        let reason = without_rosetta(&default, node.path(), failed);
        assert_eq!(reason, "the node's TSO canary failed: violations");
        let reason = without_rosetta(&default, node.path(), || None);
        assert!(reason.contains("node service's log"), "{reason}");
        let bare = tempfile::tempdir().unwrap();
        let reason = without_rosetta(&default, bare.path(), failed);
        assert!(reason.contains("no Rosetta runtime"), "{reason}");
    }

    #[test]
    fn an_x86_64_entrypoint_needs_rosetta() {
        let rootfs = tempfile::tempdir().unwrap();
        let root = rootfs.path();
        for dir in ["usr/bin", "work", "opt"] {
            std::fs::create_dir_all(root.join(dir)).unwrap();
        }
        std::fs::write(root.join("usr/bin/amd64"), elf(EM_X86_64)).unwrap();
        std::fs::write(root.join("usr/bin/arm64"), elf(183)).unwrap();
        std::fs::write(root.join("usr/bin/script"), b"#!/bin/sh\n").unwrap();
        // Links that point outside the root resolve inside it.
        std::os::unix::fs::symlink("/usr/bin/amd64", root.join("opt/app")).unwrap();
        std::os::unix::fs::symlink("../../../usr/bin/amd64", root.join("work/app")).unwrap();

        for args in [
            &["/usr/bin/amd64"][..],
            &["amd64", "serve"],
            &["/opt/app"],
            &["./app"],
        ] {
            let err = refuse_amd64_entrypoint(root, &spec(args, &[])).unwrap_err();
            assert!(err.contains("no Rosetta, because"), "{args:?}: {err}");
            assert!(err.contains("x86-64"), "{args:?}: {err}");
        }
        for args in [&["/usr/bin/arm64"][..], &["script"], &["missing"], &[]] {
            refuse_amd64_entrypoint(root, &spec(args, &[])).unwrap();
        }
        // A PATH of the spec's own replaces the default one.
        refuse_amd64_entrypoint(root, &spec(&["amd64"], &["PATH=/opt"])).unwrap();
        assert!(refuse_amd64_entrypoint(root, &spec(&["app"], &["PATH=/opt"])).is_err());
    }
}
