//! Floci's CA in every container, written into the container's copy of its
//! root file system when it is created, from the network profile's trust
//! material.
//!
//! In mode `system`, the CA is appended to each PEM bundle where Go and
//! OpenSSL look, a bundle is created where the image has none, and each Java
//! trust store is replaced in its own format with Floci's, which holds the
//! public roots and Floci's CA. Every mode but `off` writes
//! `/etc/floci-ca.pem` and `/etc/floci-ca-bundle.pem`, and sets the variables
//! that other clients read, where the container does not set them. Mode `env`
//! writes only those files and variables.
//!
//! `/etc/floci-ca.sha256` records the CA's fingerprint. At each wake,
//! [`refresh`] rewrites the files of a container whose fingerprint is not the
//! profile's, after Floci's CA changed: a process keeps the trust it loaded,
//! so a JVM that runs trusts the old CA until it restarts.
//!
//! Paths are resolved inside the root file system: a symlink never leads out
//! of it.

use std::io::Write;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

use base64::Engine as _;
use log::warn;
use serde_json::Value;

use crate::network::profile::{Trust, TrustMode};

/// The PEM bundles that Go reads (crypto/x509/root_linux.go).
const BUNDLES: [&str; 6] = [
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/ca-bundle.pem",
    "/etc/pki/tls/cacert.pem",
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    "/etc/ssl/cert.pem",
];

const JAVA_STORES: [&str; 3] = [
    "/etc/ssl/certs/java/cacerts",
    "/etc/pki/java/cacerts",
    "/etc/pki/ca-trust/extracted/java/cacerts",
];

/// Directories that hold JVMs, each `*/lib/security/cacerts` or
/// `*/jre/lib/security/cacerts` below them.
const JVM_DIRS: [&str; 3] = ["/usr/lib/jvm", "/opt/java", "/usr/local"];

pub const CA_FILE: &str = "/etc/floci-ca.pem";
pub const BUNDLE_FILE: &str = "/etc/floci-ca-bundle.pem";
/// The fingerprint of the CA written, the profile's `trust.ca_sha256`.
pub const FINGERPRINT_FILE: &str = "/etc/floci-ca.sha256";

const JKS_MAGIC: [u8; 4] = [0xfe, 0xed, 0xfe, 0xed];

/// `path` inside `root`, with symlinks followed inside `root`, or none when
/// it does not exist.
pub fn resolve(root: &Path, path: &str) -> Option<PathBuf> {
    let mut parts: Vec<String> = Vec::new();
    let mut todo: Vec<String> = Path::new(path)
        .components()
        .filter_map(|c| match c {
            Component::Normal(p) => Some(p.to_string_lossy().into_owned()),
            Component::ParentDir => Some("..".into()),
            _ => None,
        })
        .rev()
        .collect();
    let mut hops = 0;
    while let Some(part) = todo.pop() {
        if part == ".." {
            parts.pop();
            continue;
        }
        parts.push(part);
        let here: PathBuf = parts.iter().fold(root.to_path_buf(), |p, c| p.join(c));
        let meta = std::fs::symlink_metadata(&here).ok()?;
        if meta.file_type().is_symlink() {
            hops += 1;
            if hops > 40 {
                return None;
            }
            let target = std::fs::read_link(&here).ok()?;
            parts.pop();
            if target.is_absolute() {
                parts.clear();
            }
            let more: Vec<String> = target
                .components()
                .filter_map(|c| match c {
                    Component::Normal(p) => Some(p.to_string_lossy().into_owned()),
                    Component::ParentDir => Some("..".into()),
                    _ => None,
                })
                .collect();
            todo.extend(more.into_iter().rev());
        }
    }
    let resolved = parts.iter().fold(root.to_path_buf(), |p, c| p.join(c));
    resolved.exists().then_some(resolved)
}

/// Writes `data` over `path`, keeping its owner and mode, or with root and
/// 0644 for a new file.
fn write_file(path: &Path, data: &[u8]) -> Result<(), String> {
    let old = std::fs::metadata(path).ok();
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("mkdir {}: {e}", dir.display()))?;
    }
    let tmp = path.with_extension("floci-tmp");
    // A new file, so that a symlink left at its name is not followed.
    let _ = std::fs::remove_file(&tmp);
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .and_then(|mut file| file.write_all(data))
        .map_err(|e| format!("write {}: {e}", tmp.display()))?;
    let (mode, uid, gid) = old.map_or((0o644, 0, 0), |m| (m.mode() & 0o7777, m.uid(), m.gid()));
    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(mode))
        .map_err(|e| format!("chmod {}: {e}", tmp.display()))?;
    std::os::unix::fs::chown(&tmp, Some(uid), Some(gid))
        .map_err(|e| format!("chown {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("rename to {}: {e}", path.display()))
}

fn with_ca(bundle: &str, ca: &str) -> Option<String> {
    let ca = ca.trim();
    if bundle.contains(ca) {
        return None;
    }
    let mut out = bundle.to_string();
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(ca);
    out.push('\n');
    Some(out)
}

/// The Java trust stores of the root file system at `root`.
fn java_stores(root: &Path, java_home: Option<&str>) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = Vec::new();
    let mut add = |path: Option<PathBuf>| {
        if let Some(p) = path {
            if p.is_file() && !found.contains(&p) {
                found.push(p);
            }
        }
    };
    for store in JAVA_STORES {
        add(resolve(root, store));
    }
    let mut homes: Vec<String> = java_home.map(str::to_string).into_iter().collect();
    for dir in JVM_DIRS {
        if let Some(dir) = resolve(root, dir) {
            if let Ok(entries) = std::fs::read_dir(&dir) {
                for entry in entries.flatten() {
                    let rel = entry.path();
                    if let Ok(rel) = rel.strip_prefix(root) {
                        homes.push(format!("/{}", rel.display()));
                    }
                }
            }
        }
    }
    for home in homes {
        for tail in ["lib/security/cacerts", "jre/lib/security/cacerts"] {
            add(resolve(
                root,
                &format!("{}/{tail}", home.trim_end_matches('/')),
            ));
        }
    }
    found
}

/// The value of variable `name` in an OCI process env.
fn env_has(env: &[Value], name: &str) -> bool {
    env.iter()
        .filter_map(Value::as_str)
        .any(|e| e.split_once('=').is_some_and(|(n, _)| n == name))
}

fn env_get<'a>(env: &'a [Value], name: &str) -> Option<&'a str> {
    env.iter().filter_map(Value::as_str).find_map(|e| {
        e.split_once('=')
            .filter(|(n, _)| *n == name)
            .map(|(_, v)| v)
    })
}

/// Adds the trust variables of `mode` that `env` does not set.
fn add_vars(env: &mut Vec<Value>, mode: TrustMode) {
    let mut vars = vec![
        ("FLOCI_CA_BUNDLE", BUNDLE_FILE),
        ("NODE_EXTRA_CA_CERTS", CA_FILE),
        ("DENO_CERT", CA_FILE),
        ("REQUESTS_CA_BUNDLE", BUNDLE_FILE),
        ("AWS_CA_BUNDLE", BUNDLE_FILE),
    ];
    if mode == TrustMode::Env {
        vars.push(("SSL_CERT_FILE", BUNDLE_FILE));
        vars.push(("CURL_CA_BUNDLE", BUNDLE_FILE));
    }
    for (name, value) in vars {
        if !env_has(env, name) {
            env.push(Value::String(format!("{name}={value}")));
        }
    }
}

/// The OCI process of an exec in a container whose root file system at
/// `root` got Floci's CA, with the trust variables added as its container
/// has them.
pub fn exec_process(root: &Path, process_json: &str, mode: TrustMode) -> String {
    if mode == TrustMode::Off || !root.join(CA_FILE.trim_start_matches('/')).is_file() {
        return process_json.to_string();
    }
    let Ok(mut process) = serde_json::from_str::<Value>(process_json) else {
        return process_json.to_string();
    };
    let mut env: Vec<Value> = process["env"].as_array().cloned().unwrap_or_default();
    add_vars(&mut env, mode);
    process["env"] = Value::Array(env);
    serde_json::to_string(&process).unwrap_or_else(|_| process_json.to_string())
}

/// Writes `trust` into the root file system at `root`, and returns the
/// container's spec with the trust variables added.
pub fn apply(root: &Path, spec_json: &str, trust: &Trust) -> Result<String, String> {
    if trust.mode == TrustMode::Off {
        return Ok(spec_json.to_string());
    }
    let mut spec: Value =
        serde_json::from_str(spec_json).map_err(|e| format!("the container's spec: {e}"))?;
    let env: Vec<Value> = spec["process"]["env"]
        .as_array()
        .cloned()
        .unwrap_or_default();

    write_trust(root, trust, env_get(&env, "JAVA_HOME"), None)?;
    let mut env = env;
    add_vars(&mut env, trust.mode);
    spec["process"]["env"] = Value::Array(env);
    serde_json::to_string(&spec).map_err(|e| e.to_string())
}

/// Rewrites Floci's CA in the root file system at `root` when the one written
/// there is not `trust`'s, taking the old CA out of each PEM bundle. Returns
/// the old CA's fingerprint when it rewrote. A root that never got Floci's
/// CA, and mode `off`, are left alone. A Java trust store under a `JAVA_HOME`
/// outside the usual JVM directories is not found again.
pub fn refresh(root: &Path, trust: &Trust) -> Result<Option<String>, String> {
    if trust.mode == TrustMode::Off {
        return Ok(None);
    }
    let Ok(old_ca) = std::fs::read_to_string(etc_file(root, CA_FILE)) else {
        return Ok(None);
    };
    let recorded = std::fs::read_to_string(etc_file(root, FINGERPRINT_FILE))
        .ok()
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty());
    let current = match &recorded {
        Some(recorded) => recorded.eq_ignore_ascii_case(&trust.ca_sha256),
        None => old_ca.trim() == trust.ca.trim(),
    };
    if current {
        return Ok(None);
    }
    write_trust(root, trust, None, Some(&old_ca))?;
    Ok(Some(recorded.unwrap_or_else(|| "unknown".into())))
}

/// Refreshes the trust of each container root in `roots`, by container id,
/// and returns the wake's warning when any held another CA.
pub fn refresh_all(roots: &[(String, PathBuf)], trust: &Trust) -> Option<String> {
    let mut stale: Vec<String> = Vec::new();
    for (container, root) in roots {
        match refresh(root, trust) {
            Ok(Some(old)) => {
                warn!(
                    "container {container} trusted Floci's CA {old}, not {}: its trust files \
                     are rewritten; a JVM that runs keeps the old CA until it restarts",
                    trust.ca_sha256
                );
                stale.push(container.clone());
            }
            Ok(None) => {}
            Err(e) => {
                warn!("refresh Floci's CA in container {container}: {e}");
                stale.push(container.clone());
            }
        }
    }
    (!stale.is_empty()).then(|| {
        format!(
            "trust stale: containers {} trusted another Floci CA and were given {}; a running \
             JVM keeps the old CA until it restarts",
            stale.join(", "),
            trust.ca_sha256
        )
    })
}

/// A file of `/etc` inside `root`, with `/etc` resolved inside it.
fn etc_file(root: &Path, file: &str) -> PathBuf {
    let name = Path::new(file).file_name().unwrap_or_default();
    resolve(root, "/etc")
        .unwrap_or_else(|| root.join("etc"))
        .join(name)
}

/// Writes `trust`'s files into the root file system at `root`, taking
/// `old_ca` out of each PEM bundle first.
fn write_trust(
    root: &Path,
    trust: &Trust,
    java_home: Option<&str>,
    old_ca: Option<&str>,
) -> Result<(), String> {
    let mut bundle_text: Option<String> = None;
    if trust.mode == TrustMode::System {
        let mut done: Vec<PathBuf> = Vec::new();
        for path in BUNDLES {
            let Some(real) = resolve(root, path) else {
                continue;
            };
            if done.contains(&real) || !real.is_file() {
                continue;
            }
            let text = std::fs::read_to_string(&real).unwrap_or_default();
            let cleaned = old_ca.map_or_else(|| text.clone(), |old| without_ca(&text, old));
            let updated = with_ca(&cleaned, &trust.ca).unwrap_or(cleaned);
            if updated != text {
                write_file(&real, updated.as_bytes())?;
            }
            if bundle_text.is_none() {
                bundle_text = Some(updated);
            }
            done.push(real);
        }
        if done.is_empty() {
            let path = root.join(BUNDLES[0].trim_start_matches('/'));
            write_file(&path, trust.bundle.as_bytes())?;
        }
        let engine = base64::engine::general_purpose::STANDARD;
        let jks = engine
            .decode(&trust.jks)
            .map_err(|e| format!("the JKS trust store: {e}"))?;
        let pkcs12 = engine
            .decode(&trust.pkcs12)
            .map_err(|e| format!("the PKCS12 trust store: {e}"))?;
        for store in java_stores(root, java_home) {
            let head = std::fs::read(&store).map(|b| b.get(..4).map(<[u8]>::to_vec));
            let data = match head {
                Ok(Some(magic)) if magic == JKS_MAGIC => &jks,
                _ => &pkcs12,
            };
            write_file(&store, data)?;
        }
    }
    let bundle = bundle_text.unwrap_or_else(|| trust.bundle.clone());
    write_file(&etc_file(root, CA_FILE), trust.ca.as_bytes())?;
    write_file(
        &etc_file(root, BUNDLE_FILE),
        with_ca(&bundle, &trust.ca).unwrap_or(bundle).as_bytes(),
    )?;
    write_file(
        &etc_file(root, FINGERPRINT_FILE),
        format!("{}\n", trust.ca_sha256).as_bytes(),
    )
}

/// `bundle` without the PEM block `ca`.
fn without_ca(bundle: &str, ca: &str) -> String {
    let ca = ca.trim();
    if ca.is_empty() {
        return bundle.to_string();
    }
    bundle.replace(&format!("{ca}\n"), "").replace(ca, "")
}

#[cfg(test)]
mod tests {
    use super::*;

    const CA: &str = "-----BEGIN CERTIFICATE-----\nFLOCI\n-----END CERTIFICATE-----\n";

    fn trust(mode: TrustMode) -> Trust {
        let engine = base64::engine::general_purpose::STANDARD;
        Trust {
            mode,
            ca: CA.into(),
            ca_sha256: "00".into(),
            bundle: format!("ROOTS\n{CA}"),
            pkcs12: engine.encode(b"P12STORE"),
            jks: engine.encode([0xfe, 0xed, 0xfe, 0xed, b'J']),
        }
    }

    fn spec(env: &[&str]) -> String {
        serde_json::json!({"process": {"env": env}}).to_string()
    }

    #[test]
    fn a_debian_image_gets_the_ca_in_its_bundle_and_a_jks_store() {
        let root = tempfile::tempdir().unwrap();
        let r = root.path();
        std::fs::create_dir_all(r.join("etc/ssl/certs/java")).unwrap();
        std::fs::write(r.join("etc/ssl/certs/ca-certificates.crt"), "PUBLIC\n").unwrap();
        std::os::unix::fs::symlink(
            "/etc/ssl/certs/ca-certificates.crt",
            r.join("etc/ssl/cert.pem"),
        )
        .unwrap();
        std::fs::write(
            r.join("etc/ssl/certs/java/cacerts"),
            [0xfe, 0xed, 0xfe, 0xed, 0],
        )
        .unwrap();
        std::fs::create_dir_all(r.join("usr/lib/jvm/temurin/lib/security")).unwrap();
        std::os::unix::fs::symlink(
            "/etc/ssl/certs/java/cacerts",
            r.join("usr/lib/jvm/temurin/lib/security/cacerts"),
        )
        .unwrap();
        let out = apply(
            r,
            &spec(&["PATH=/bin", "AWS_CA_BUNDLE=/mine.pem"]),
            &trust(TrustMode::System),
        )
        .unwrap();
        let bundle = std::fs::read_to_string(r.join("etc/ssl/certs/ca-certificates.crt")).unwrap();
        assert_eq!(bundle, format!("PUBLIC\n{CA}"));
        assert_eq!(
            std::fs::read(r.join("etc/ssl/certs/java/cacerts")).unwrap(),
            vec![0xfe, 0xed, 0xfe, 0xed, b'J']
        );
        assert!(std::fs::symlink_metadata(r.join("etc/ssl/cert.pem"))
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(
            std::fs::read_to_string(r.join("etc/floci-ca.pem")).unwrap(),
            CA
        );
        let written: Value = serde_json::from_str(&out).unwrap();
        let env: Vec<&str> = written["process"]["env"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .collect();
        assert!(env.contains(&"AWS_CA_BUNDLE=/mine.pem"));
        assert!(!env.contains(&"AWS_CA_BUNDLE=/etc/floci-ca-bundle.pem"));
        assert!(env.contains(&"NODE_EXTRA_CA_CERTS=/etc/floci-ca.pem"));
        assert!(!env.iter().any(|e| e.starts_with("SSL_CERT_FILE=")));

        apply(r, &spec(&[]), &trust(TrustMode::System)).unwrap();
        let again = std::fs::read_to_string(r.join("etc/ssl/certs/ca-certificates.crt")).unwrap();
        assert_eq!(again, bundle, "the CA is appended once");
    }

    #[test]
    fn a_scratch_image_gets_floci_s_bundle() {
        let root = tempfile::tempdir().unwrap();
        apply(root.path(), &spec(&[]), &trust(TrustMode::System)).unwrap();
        let bundle =
            std::fs::read_to_string(root.path().join("etc/ssl/certs/ca-certificates.crt")).unwrap();
        assert_eq!(bundle, format!("ROOTS\n{CA}"));
    }

    #[test]
    fn a_pkcs12_store_stays_pkcs12_and_env_mode_touches_no_store() {
        let root = tempfile::tempdir().unwrap();
        let r = root.path();
        std::fs::create_dir_all(r.join("opt/java/openjdk/lib/security")).unwrap();
        std::fs::write(
            r.join("opt/java/openjdk/lib/security/cacerts"),
            [0x30, 0x82],
        )
        .unwrap();
        apply(r, &spec(&[]), &trust(TrustMode::Env)).unwrap();
        assert_eq!(
            std::fs::read(r.join("opt/java/openjdk/lib/security/cacerts")).unwrap(),
            vec![0x30, 0x82]
        );
        apply(r, &spec(&[]), &trust(TrustMode::System)).unwrap();
        assert_eq!(
            std::fs::read(r.join("opt/java/openjdk/lib/security/cacerts")).unwrap(),
            b"P12STORE"
        );
    }

    #[test]
    fn a_symlink_never_leads_out_of_the_root() {
        let root = tempfile::tempdir().unwrap();
        let r = root.path();
        std::fs::create_dir_all(r.join("etc/ssl")).unwrap();
        std::os::unix::fs::symlink("../../../../../../etc/passwd", r.join("etc/ssl/cert.pem"))
            .unwrap();
        assert_eq!(resolve(r, "/etc/ssl/cert.pem"), None);
        std::os::unix::fs::symlink("/etc/ssl/cert.pem", r.join("loop")).unwrap();
        std::fs::remove_file(r.join("etc/ssl/cert.pem")).unwrap();
        std::os::unix::fs::symlink("/loop", r.join("etc/ssl/cert.pem")).unwrap();
        assert_eq!(resolve(r, "/loop"), None);
    }

    #[test]
    fn an_exec_gets_the_variables_only_in_a_container_with_the_ca() {
        let root = tempfile::tempdir().unwrap();
        let process = serde_json::json!({"args": ["sh"], "env": ["PATH=/bin"]}).to_string();
        assert_eq!(
            exec_process(root.path(), &process, TrustMode::System),
            process
        );
        apply(root.path(), &spec(&[]), &trust(TrustMode::System)).unwrap();
        let out: Value =
            serde_json::from_str(&exec_process(root.path(), &process, TrustMode::System)).unwrap();
        let env: Vec<&str> = out["env"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .collect();
        assert!(
            env.contains(&"PATH=/bin") && env.contains(&"NODE_EXTRA_CA_CERTS=/etc/floci-ca.pem")
        );
        assert_eq!(out["args"][0], "sh");
    }

    #[test]
    fn a_changed_ca_replaces_the_old_one_at_refresh() {
        const NEW: &str = "-----BEGIN CERTIFICATE-----\nNEW\n-----END CERTIFICATE-----\n";
        let root = tempfile::tempdir().unwrap();
        let r = root.path();
        std::fs::create_dir_all(r.join("etc/ssl/certs/java")).unwrap();
        std::fs::write(r.join("etc/ssl/certs/ca-certificates.crt"), "PUBLIC\n").unwrap();
        std::fs::write(r.join("etc/ssl/certs/java/cacerts"), [0x30, 0x82]).unwrap();
        apply(r, &spec(&[]), &trust(TrustMode::System)).unwrap();
        assert_eq!(
            std::fs::read_to_string(r.join("etc/floci-ca.sha256")).unwrap(),
            "00\n"
        );
        assert_eq!(refresh(r, &trust(TrustMode::System)).unwrap(), None);

        let mut changed = trust(TrustMode::System);
        changed.ca = NEW.into();
        changed.ca_sha256 = "11".into();
        changed.bundle = format!("ROOTS\n{NEW}");
        changed.pkcs12 = base64::engine::general_purpose::STANDARD.encode(b"NEWSTORE");
        assert_eq!(refresh(r, &changed).unwrap(), Some("00".into()));
        assert_eq!(
            std::fs::read_to_string(r.join("etc/ssl/certs/ca-certificates.crt")).unwrap(),
            format!("PUBLIC\n{NEW}")
        );
        assert_eq!(
            std::fs::read_to_string(r.join("etc/floci-ca-bundle.pem")).unwrap(),
            format!("PUBLIC\n{NEW}")
        );
        assert_eq!(
            std::fs::read_to_string(r.join("etc/floci-ca.pem")).unwrap(),
            NEW
        );
        assert_eq!(
            std::fs::read(r.join("etc/ssl/certs/java/cacerts")).unwrap(),
            b"NEWSTORE"
        );
        assert_eq!(
            std::fs::read_to_string(r.join("etc/floci-ca.sha256")).unwrap(),
            "11\n"
        );
        assert_eq!(refresh(r, &changed).unwrap(), None);
    }

    #[test]
    fn a_container_without_floci_s_ca_is_not_refreshed() {
        let root = tempfile::tempdir().unwrap();
        assert_eq!(
            refresh(root.path(), &trust(TrustMode::System)).unwrap(),
            None
        );
        assert!(!root.path().join("etc").exists());
        let roots = vec![("app".to_string(), root.path().to_path_buf())];
        assert_eq!(refresh_all(&roots, &trust(TrustMode::System)), None);
    }

    #[test]
    fn a_stale_container_is_a_warning() {
        let (app, sidecar) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        apply(app.path(), &spec(&[]), &trust(TrustMode::System)).unwrap();
        apply(sidecar.path(), &spec(&[]), &trust(TrustMode::System)).unwrap();
        let mut changed = trust(TrustMode::System);
        changed.ca_sha256 = "11".into();
        let roots = vec![
            ("app".to_string(), app.path().to_path_buf()),
            ("sidecar".to_string(), sidecar.path().to_path_buf()),
        ];
        let warning = refresh_all(&roots, &changed).unwrap();
        assert!(warning.starts_with("trust stale: containers app, sidecar "));
        assert_eq!(refresh_all(&roots, &changed), None);
    }

    #[test]
    fn off_changes_nothing() {
        let root = tempfile::tempdir().unwrap();
        let out = apply(root.path(), &spec(&["A=b"]), &trust(TrustMode::Off)).unwrap();
        assert_eq!(out, spec(&["A=b"]));
        assert!(!root.path().join("etc").exists());
    }
}
