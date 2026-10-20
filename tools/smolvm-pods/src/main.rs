//! containerd-shim-smolvm-pods-v2: the containerd shim that runs each pod in a
//! smolvm microVM. One binary, in several modes:
//!
//! - the front shim, which containerd starts (`start`, `delete` and the
//!   daemon, see [`front`]);
//! - `node-service`, the node's process that owns the pod VMs ([`node`]),
//!   with their idle sleep and wake on traffic ([`activity`], [`sleepnet`],
//!   [`probe`] and [`kube`]);
//! - `ctl`, a client of the node service's API ([`api`]);
//! - `_boot-vm`, the entry point of each pod's VM process, and the node's
//!   boot steps `_expand-templates` and `_tso-canary`;
//! - `_build-info`, which prints the smolvm commit, and, by its argv[0],
//!   smolvm's `ip` and `tc`, which a pod's VM process runs (see [`net`]).
//!
//! Derived from smolvm's crates/smolvm-shim (Apache-2.0), with changes. Linux
//! only; a stub on other platforms so the crate checks anywhere.

#[cfg(target_os = "linux")]
mod activity;
#[cfg(target_os = "linux")]
mod api;
#[cfg(target_os = "linux")]
mod backend;
#[cfg(target_os = "linux")]
mod boot;
#[cfg(target_os = "linux")]
mod bundle;
#[cfg(target_os = "linux")]
mod canary;
#[cfg(target_os = "linux")]
mod cleanup;
#[cfg(target_os = "linux")]
mod engine;
#[cfg(target_os = "linux")]
mod files;
#[cfg(target_os = "linux")]
mod front;
#[cfg(target_os = "linux")]
mod http1;
#[cfg(target_os = "linux")]
mod kube;
#[cfg(target_os = "linux")]
mod lifecycle;
#[cfg(target_os = "linux")]
mod logging;
#[cfg(target_os = "linux")]
mod net;
#[cfg(target_os = "linux")]
mod netlink;
#[cfg(target_os = "linux")]
mod node;
#[cfg(target_os = "linux")]
mod panics;
#[cfg(target_os = "linux")]
mod probe;
#[cfg(target_os = "linux")]
mod proto;
#[cfg(target_os = "linux")]
mod pump;
#[cfg(target_os = "linux")]
mod rosetta;
#[cfg(target_os = "linux")]
mod sleepnet;
#[cfg(target_os = "linux")]
mod sync;
#[cfg(target_os = "linux")]
mod templates;
#[cfg(all(target_os = "linux", test))]
mod test_netns;
#[cfg(target_os = "linux")]
mod tombstone;

/// The smolvm commit this binary was built against, which scripts/build.sh
/// passes in.
#[cfg(target_os = "linux")]
const SMOLVM_COMMIT: Option<&str> = option_env!("SMOLVM_PODS_SMOLVM_COMMIT");

#[cfg(target_os = "linux")]
fn main() {
    // A pod VM's process runs this binary as smolvm's `ip` and `tc` (see `net`).
    let mut argv = std::env::args_os();
    if let Some(tool) = argv.next().as_deref().and_then(net::Tool::from_argv0) {
        std::process::exit(tool.run(&argv.collect::<Vec<_>>()));
    }

    let mut args = std::env::args_os().skip(1);
    let command = args.next();
    let command = command.as_deref().and_then(|a| a.to_str());
    // The node image records the smolvm commit (see node/Dockerfile).
    if command == Some("_build-info") {
        let Some(commit) = SMOLVM_COMMIT else {
            eprintln!("the smolvm commit is unknown: build with scripts/build.sh");
            std::process::exit(1);
        };
        println!("smolvm {commit}");
        return;
    }

    // containerd runs the front shim with its flags first (`-namespace ...`),
    // so none of its invocations names a mode below.
    let mode = command.filter(|c| !c.starts_with('-'));
    if !matches!(
        mode,
        Some("_boot-vm" | "node-service" | "ctl" | "_expand-templates" | "_tso-canary")
    ) {
        front::run_shim();
        return;
    }

    // Keep smolvm's state under SMOLVM_DATA_DIR, as the smolvm CLI does for
    // every command: a pause needs the VM data on the same mount as the disk
    // templates there. Before any thread exists, so that it may set HOME. The
    // front shim never calls smolvm, so it does not need it.
    smolvm::process::apply_system_data_root(false);

    match mode {
        // The engine boots each VM by spawning its own executable with
        // `_boot-vm` (see `agent::manager`, which resolves /proc/self/exe),
        // so this binary, which links the engine, serves that subcommand.
        Some("_boot-vm") => {
            // The VM process must not run on with a thread gone.
            panics::abort_on_panic();
            let Some(config) = args.next() else {
                eprintln!("_boot-vm requires a boot-config path");
                std::process::exit(2);
            };
            boot::run(std::path::PathBuf::from(config));
        }
        Some("node-service") => {
            panics::log_panics();
            let args: Vec<String> = args.map(|a| a.to_string_lossy().into_owned()).collect();
            std::process::exit(node::run(&args));
        }
        Some("ctl") => {
            let args: Vec<String> = args.map(|a| a.to_string_lossy().into_owned()).collect();
            std::process::exit(api::ctl(&args));
        }
        // The node runs this at boot, so that its first pods need not wait for it.
        Some("_expand-templates") => match templates::expand() {
            Ok(missing) => {
                for template in missing {
                    eprintln!("no disk template {template}, so smolvm formats each new VM disk");
                }
            }
            Err(e) => {
                eprintln!("expand the disk templates: {e}");
                std::process::exit(1);
            }
        },
        // The node runs this at boot too, after `_expand-templates`.
        Some("_tso-canary") => {
            if !cfg!(target_arch = "aarch64") {
                eprintln!("no TSO canary: this node runs amd64 natively");
                return;
            }
            match canary::ensure() {
                Ok(verdict) if verdict.passed => {
                    eprintln!("TSO canary passed: {}", verdict.detail)
                }
                Ok(verdict) => {
                    eprintln!(
                        "TSO canary failed, so the node runs no Rosetta pods: {}",
                        verdict.detail
                    );
                    std::process::exit(1);
                }
                Err(e) => {
                    eprintln!("TSO canary could not run: {e}");
                    std::process::exit(1);
                }
            }
        }
        _ => unreachable!("every mode is matched above"),
    }
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("containerd-shim-smolvm-pods-v2 only runs on Linux Kubernetes nodes");
    std::process::exit(1);
}
