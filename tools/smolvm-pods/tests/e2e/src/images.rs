//! The test image: the test app and the litmus test from `workloads/`, built
//! for each architecture and saved with Docker.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use crate::cmd;
use crate::pod::Arch;

/// The pinned Go image that builds the workloads, which `scripts/node-vm.sh`
/// also reads. It must be in the local Docker cache: the tests never pull
/// images.
fn go_image() -> &'static str {
    include_str!("../workloads/go-image").trim()
}

/// Builds the workloads as static binaries in `/out/ARCH/`.
const GO_BUILD: &str = "for arch in arm64 amd64; do \
    GOARCH=$arch go build -trimpath -ldflags=-s -o /out/$arch/ ./testapp ./litmus; done";

const TIMEOUT: Duration = Duration::from_secs(600);

/// The Docker tag of the test image for `arch` in the test `PREFIX-ID`.
pub fn tag(prefix: &str, id: &str, arch: Arch) -> String {
    format!("{prefix}/testapp:{}-{id}", arch.name())
}

/// Fails unless the Go image is in the local Docker cache.
pub fn require_go_image() -> Result<(), String> {
    let inspect = ["image", "inspect", "--format", "{{.Id}}", go_image()];
    docker(Command::new("docker").args(inspect))
        .map(drop)
        .map_err(|e| format!("{} must be in the local Docker cache: {e}", go_image()))
}

/// Builds the test image for each architecture, saves it as an archive in
/// `out`, and returns the archives.
pub fn build(prefix: &str, id: &str, out: &Path) -> Result<Vec<(Arch, PathBuf)>, String> {
    let workloads = Path::new(env!("CARGO_MANIFEST_DIR")).join("workloads");
    let name = format!("{prefix}-go-{id}");
    let src = format!("{}:/src:ro", workloads.display());
    let dst = format!("{}:/out", out.display());
    docker(
        Command::new("docker")
            .args(["run", "--rm", "--pull=never", "--network=none"])
            .args(["--name", &name, "-v", &src, "-v", &dst, "-w", "/src"])
            .args(["-e", "CGO_ENABLED=0", go_image(), "sh", "-ec", GO_BUILD]),
    )?;
    let dockerfile = workloads.join("Dockerfile");
    Arch::ALL
        .into_iter()
        .map(|arch| {
            let tag = tag(prefix, id, arch);
            let platform = format!("linux/{}", arch.name());
            let archive = out.join(format!("{}.tar", arch.name()));
            docker(
                Command::new("docker")
                    .args(["build", "--platform", &platform, "--tag", &tag])
                    .args(["--provenance=false", "--sbom=false", "--file"])
                    .arg(&dockerfile)
                    .arg(out.join(arch.name())),
            )?;
            docker(
                Command::new("docker")
                    .args(["save", "--platform", &platform, "--output"])
                    .arg(&archive)
                    .arg(&tag),
            )?;
            Ok((arch, archive))
        })
        .collect()
}

/// Removes the test images from Docker.
pub fn remove(prefix: &str, id: &str) -> Result<(), String> {
    for arch in Arch::ALL {
        match docker(Command::new("docker").args(["image", "rm", &tag(prefix, id, arch)])) {
            Err(e) if !e.contains("No such image") => return Err(e),
            _ => {}
        }
    }
    Ok(())
}

fn docker(cmd: &mut Command) -> Result<String, String> {
    cmd::run_ok(cmd, TIMEOUT)
}
