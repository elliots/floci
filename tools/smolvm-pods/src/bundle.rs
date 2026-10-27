//! OCI bundle inspection and rootfs mounting.
//!
//! containerd hands the shim a bundle dir (config.json) and a list of rootfs
//! mounts (e.g. one overlay mount). The CRI plugin marks pod-sandbox vs
//! workload containers with annotations, and passes the pod netns path on the
//! sandbox's config. A container without them, such as Docker's or `ctr
//! run`'s, is a pod of its own ([`crate::standalone`]).
//!
//! Derived from smolvm's crates/smolvm-shim (Apache-2.0), with changes.

use std::path::Path;

use log::warn;
use serde::Deserialize;

/// CRI annotation: "sandbox" (the pause container) or "container".
pub(crate) const ANN_CONTAINER_TYPE: &str = "io.kubernetes.cri.container-type";
/// CRI annotation on sandboxes: host path of the pod's network namespace.
const ANN_SANDBOX_NETNS: &str = "io.kubernetes.cri.sandbox-network-ns";
/// CRI annotation on workload containers: the id of the pod sandbox they belong
/// to.
const ANN_SANDBOX_ID: &str = "io.kubernetes.cri.sandbox-id";

#[derive(Debug, Default)]
pub struct BundleInfo {
    /// A CRI pod sandbox.
    pub is_sandbox: bool,
    /// A container with no CRI annotations: a pod of its own.
    pub standalone: bool,
    pub netns: Option<String>,
    /// For a workload container, its pod's sandbox id.
    pub sandbox_id: Option<String>,
}

#[derive(Deserialize)]
struct MinimalSpec {
    #[serde(default)]
    annotations: std::collections::HashMap<String, String>,
    #[serde(default)]
    linux: Option<MinimalLinux>,
}

#[derive(Deserialize)]
struct MinimalLinux {
    #[serde(default)]
    namespaces: Vec<MinimalNs>,
}

#[derive(Deserialize)]
struct MinimalNs {
    #[serde(rename = "type")]
    ns_type: String,
    #[serde(default)]
    path: Option<String>,
}

/// The OCI spec in `bundle`, if it has a readable one.
pub fn read_spec(bundle: &str) -> Option<serde_json::Value> {
    let raw = std::fs::read(Path::new(bundle).join("config.json")).ok()?;
    serde_json::from_slice(&raw).ok()
}

/// The annotation `key` of an OCI spec, if it is a string.
pub fn annotation<'a>(spec: Option<&'a serde_json::Value>, key: &str) -> Option<&'a str> {
    spec?.pointer("/annotations")?.get(key)?.as_str()
}

/// A yes or no annotation: `true` or `1`, or `false` or `0`, in any case and
/// with spaces around it. Any other value is neither.
pub fn parse_flag(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

/// Read the bundle's config.json and classify it.
pub fn load(bundle: &str) -> Result<BundleInfo, String> {
    let cfg = Path::new(bundle).join("config.json");
    let raw = std::fs::read_to_string(&cfg).map_err(|e| format!("read {}: {e}", cfg.display()))?;
    let spec: MinimalSpec =
        serde_json::from_str(&raw).map_err(|e| format!("parse {}: {e}", cfg.display()))?;

    let container_type = spec.annotations.get(ANN_CONTAINER_TYPE);
    let standalone = container_type.is_none();
    let is_sandbox = container_type.is_some_and(|v| v == "sandbox");

    // Netns: prefer the CRI annotation, else the OCI network namespace path.
    let netns = spec
        .annotations
        .get(ANN_SANDBOX_NETNS)
        .cloned()
        .or_else(|| {
            spec.linux.as_ref().and_then(|l| {
                l.namespaces
                    .iter()
                    .find(|n| n.ns_type == "network")
                    .and_then(|n| n.path.clone())
                    .filter(|p| !p.is_empty())
            })
        });

    // A sandbox's containers find its VM by its id.
    let sandbox_id = if is_sandbox {
        None
    } else {
        spec.annotations.get(ANN_SANDBOX_ID).cloned()
    };

    Ok(BundleInfo {
        is_sandbox,
        standalone,
        netns,
        sandbox_id,
    })
}

/// Mount containerd's rootfs mounts at `<bundle>/rootfs`, returning that path.
/// No mounts (tests, pre-mounted rootfs) is fine: the directory is used as-is.
pub async fn mount_rootfs(bundle: &str, mounts: &[crate::proto::Mount]) -> Result<String, String> {
    let target = Path::new(bundle).join("rootfs");
    tokio::fs::create_dir_all(&target)
        .await
        .map_err(|e| format!("mkdir {}: {e}", target.display()))?;
    for m in mounts {
        containerd_shim::mount::mount_rootfs(
            Some(m.kind.as_str()),
            Some(m.source.as_str()),
            &m.options,
            &target,
        )
        .map_err(|e| format!("mount rootfs ({}): {e}", m.kind))?;
    }
    Ok(target.to_string_lossy().into_owned())
}

/// The host path of a container's root file system: containerd's rootfs
/// mounts at `<bundle>/rootfs`, or, without any, the spec's `root.path` when
/// it is absolute, as Docker's mount of the image is.
pub async fn container_rootfs(
    bundle: &str,
    mounts: &[crate::proto::Mount],
) -> Result<String, String> {
    if mounts.is_empty() {
        let spec = read_spec(bundle);
        let root = spec
            .as_ref()
            .and_then(|s| s.pointer("/root/path"))
            .and_then(|p| p.as_str())
            .filter(|p| Path::new(p).is_absolute());
        if let Some(root) = root {
            return Ok(root.to_string());
        }
    }
    mount_rootfs(bundle, mounts).await
}

/// Unmount `<bundle>/rootfs` (best-effort; may never have been mounted).
pub async fn unmount_rootfs(bundle: &str) {
    let target = Path::new(bundle).join("rootfs");
    if let Err(e) = containerd_shim::mount::umount_recursive(target.to_str(), 0) {
        warn!("umount {}: {e}", target.display());
    }
}
