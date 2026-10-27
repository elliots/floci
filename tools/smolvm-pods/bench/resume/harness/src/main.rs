//! resume-harness drives smolvm machines through `smolvm::embedded`, the way
//! the smolvm-pods runtime does, so that resume can be measured without the
//! smolvm CLI.
//!
//!   resume-harness serve            read commands from stdin, one per line
//!   resume-harness _boot-vm CONFIG  the VM process entry point
//!   resume-harness _build-info      print the smolvm commit it was built from
//!
//! Commands, each answered with one line, `ok MS [VALUE]` or `err MS MESSAGE`,
//! where MS is how long the call took inside this process:
//!
//!   create NAME memhold|petclinic HOST_PORT SHARE_DIR IMAGE
//!   start NAME | stop NAME | resume NAME | delete NAME
//!   pause NAME          a local pause (`pause_machine_local_with`), answered
//!                       with `local save=full|delta|none written=BYTES`
//!   pause-packed NAME   a pause to a packed artifact (`pause_machine`)
//!   inspect NAME        `state=STATE alive=BOOL base_bytes=BYTES
//!                       resume_point_bytes=BYTES` (`inspect_machine`)
//!   delta-saves         `on` or `off`, which SMOLVM_DELTA_SAVES sets
//!   pid NAME | datadir NAME
//!
//! Like the runtime, it boots each VM by running itself as `_boot-vm`.

use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use smolvm::agent::{HostMount, PortMapping, VmResources};
use smolvm::embedded::{MachineSpec, PauseControl, PauseOutcome, SaveMode};
use smolvm::network::NetworkBackend;

/// The smolvm commit, which scripts/build.sh passes in.
const SMOLVM_COMMIT: Option<&str> = option_env!("SMOLVM_PODS_SMOLVM_COMMIT");

fn main() {
    let mut args = std::env::args_os().skip(1);
    let command = args.next();
    let command = command.as_deref().and_then(|a| a.to_str());
    if command == Some("_build-info") {
        let Some(commit) = SMOLVM_COMMIT else {
            eprintln!("the smolvm commit is unknown: build with scripts/build.sh");
            std::process::exit(1);
        };
        println!("smolvm {commit}");
        return;
    }
    // As the runtime and the CLI do, before any thread exists.
    smolvm::process::apply_system_data_root(false);
    match command {
        Some("_boot-vm") => {
            init_logging();
            let Some(config) = args.next() else {
                eprintln!("_boot-vm requires a boot-config path");
                std::process::exit(2);
            };
            if let Err(e) = smolvm::internal_boot::run(PathBuf::from(config)) {
                eprintln!("boot failed: {e}");
                std::process::exit(1);
            }
        }
        Some("serve") => {
            init_logging();
            serve();
        }
        _ => {
            eprintln!("usage: resume-harness serve | _boot-vm CONFIG | _build-info");
            std::process::exit(2);
        }
    }
}

/// The CLI's logging: RUST_LOG, human-readable, to stderr.
fn init_logging() {
    let filter = match std::env::var_os("RUST_LOG") {
        Some(_) => tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("smolvm=warn")),
        None => tracing_subscriber::EnvFilter::new("smolvm=warn"),
    };
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

fn serve() {
    let rt = match smolvm::embedded::runtime() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("open the smolvm runtime: {e}");
            std::process::exit(1);
        }
    };
    // VM processes are children of this process and are never waited for by
    // smolvm itself: reap them, or an exited VM stays a zombie.
    std::thread::spawn(|| loop {
        smolvm::process::reap_vm_children();
        std::thread::sleep(Duration::from_millis(50));
    });
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        let words: Vec<&str> = line.split_whitespace().collect();
        if words.is_empty() {
            continue;
        }
        tracing::info!(command = %line, "harness command start");
        let started = Instant::now();
        let result = run(&rt, &words);
        let ms = started.elapsed().as_secs_f64() * 1000.0;
        let reply = match result {
            Ok(value) => format!("ok {ms:.1} {value}"),
            Err(message) => format!("err {ms:.1} {}", message.replace('\n', " | ")),
        };
        tracing::info!(command = %line, %reply, "harness command done");
        if writeln!(stdout, "{}", reply.trim_end())
            .and_then(|()| stdout.flush())
            .is_err()
        {
            break;
        }
    }
}

fn run(rt: &smolvm::embedded::EmbeddedRuntime, words: &[&str]) -> Result<String, String> {
    let err = |e: smolvm::Error| e.to_string();
    let done = |()| String::new();
    match words {
        ["create", name, workload, port, share, image] => {
            create(rt, name, workload, port, share, image).map(done)
        }
        ["start", name] => rt.start_machine(name).map(done).map_err(err),
        ["stop", name] => rt.stop_machine(name).map(done).map_err(err),
        ["pause", name] => pause(rt, name),
        ["pause-packed", name] => rt.pause_machine(name).map(done).map_err(err),
        ["resume", name] => rt.resume_machine(name).map(done).map_err(err),
        ["delete", name] => rt.delete_machine(name).map(done).map_err(err),
        ["inspect", name] => inspect(rt, name),
        ["delta-saves"] => Ok(if smolvm::embedded::delta_saves_enabled() {
            "on".to_string()
        } else {
            "off".to_string()
        }),
        ["pid", name] => Ok(rt.pid(name).map(|p| p.to_string()).unwrap_or_default()),
        ["datadir", name] => Ok(smolvm::agent::vm_data_dir(name).display().to_string()),
        _ => Err(format!("unknown command: {}", words.join(" "))),
    }
}

/// A local pause, with how it saved guest RAM and how many bytes it wrote.
fn pause(rt: &smolvm::embedded::EmbeddedRuntime, name: &str) -> Result<String, String> {
    match rt.pause_machine_local_with(name, &PauseControl::new()) {
        Ok(PauseOutcome::Paused {
            save_mode,
            written_bytes,
        }) => {
            let save = match save_mode {
                Some(SaveMode::Full) => "full",
                Some(SaveMode::Delta) => "delta",
                None => "none",
            };
            Ok(format!("local save={save} written={written_bytes}"))
        }
        Ok(PauseOutcome::Cancelled) => Err("the pause was cancelled".to_string()),
        Err(e) => Err(e.to_string()),
    }
}

fn inspect(rt: &smolvm::embedded::EmbeddedRuntime, name: &str) -> Result<String, String> {
    let inspection = rt
        .inspect_machine(name)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no machine {name}"))?;
    let resume_point_bytes = inspection
        .resume_point
        .as_ref()
        .map_or(0, |point| point.allocated_bytes);
    Ok(format!(
        "state={:?} alive={} base_bytes={} resume_point_bytes={resume_point_bytes}",
        inspection.state, inspection.process_alive, inspection.base_bytes
    ))
}

/// The workload machines: 2 vCPUs, 3 GiB, virtio-net, the guest's port 8080
/// on HOST_PORT, and SHARE_DIR read-only at /resume-check.
fn create(
    rt: &smolvm::embedded::EmbeddedRuntime,
    name: &str,
    workload: &str,
    port: &str,
    share: &str,
    image: &str,
) -> Result<(), String> {
    let port: u16 = port.parse().map_err(|_| format!("bad port {port}"))?;
    let mount = HostMount::new(share, "/resume-check", true).map_err(|e| e.to_string())?;
    let (command, env, workdir) = match workload {
        "memhold" => (
            vec![
                "/resume-check/memhold".to_string(),
                "-mib".into(),
                "1024".into(),
                "-reserve".into(),
                "512".into(),
            ],
            Vec::new(),
            None,
        ),
        "petclinic" => (
            vec![
                "sh".to_string(),
                "-c".into(),
                "test -d webapps/ROOT || cp -r /resume-check/petclinic/ROOT webapps/; exec catalina.sh run"
                    .into(),
            ],
            vec![(
                "CATALINA_OPTS".to_string(),
                "-Xms1g -Xmx1g -XX:+AlwaysPreTouch".to_string(),
            )],
            Some("/usr/local/tomcat".to_string()),
        ),
        other => return Err(format!("unknown workload {other}")),
    };
    let spec = MachineSpec {
        name: name.to_string(),
        mounts: vec![mount],
        ports: vec![PortMapping::new(port, 8080)],
        image: Some(image.to_string()),
        command,
        resources: VmResources {
            cpus: 2,
            memory_mib: 3072,
            network: true,
            network_backend: Some(NetworkBackend::VirtioNet),
            ..VmResources::default()
        },
        persistent: true,
        ..Default::default()
    };
    rt.create_machine_with_workload(spec, env, workdir, None)
        .map_err(|e| e.to_string())
}
