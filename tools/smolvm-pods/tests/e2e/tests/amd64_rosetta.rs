//! An ordinary Docker container can use the node's Rosetta runtime through a bind mount.
use smolvm_pods_e2e::{describe, Arch, Cluster, COMMAND_TIMEOUT};

#[test]
#[ignore = "needs a Mac with smolvm and Docker: SMOLVM_PODS_E2E=1 cargo test -- --ignored"]
fn amd64_docker_container_with_rosetta_mount() {
    let cluster = Cluster::start("amd64-rosetta");
    let image = cluster.image(Arch::Amd64);
    cluster.node.sh(&format!(
        "docker load -i {} >/dev/null",
        cluster.node_path("amd64.tar")
    ));
    let run = cluster.node.exec(
        &[
            "docker",
            "run",
            "--rm",
            "--platform",
            "linux/amd64",
            "--mount",
            "type=bind,src=/mnt/rosetta,dst=/mnt/rosetta,readonly",
            &image,
            "echo",
            "rosetta-ok",
        ],
        COMMAND_TIMEOUT,
    );
    assert_eq!(run.code(), Some(0), "amd64 container: {}", describe(&run));
    assert!(run.stdout.contains("rosetta-ok"), "{}", describe(&run));
}
