//! Image-owned node DNS must survive the agent preparing an interactive exec.

use smolvm_pods_e2e::{Cluster, COMMAND_TIMEOUT};

#[test]
#[ignore = "needs a Mac with smolvm and Docker: SMOLVM_PODS_E2E=1 cargo test -- --ignored"]
fn node_resolver_survives_interactive_exec() {
    let cluster = Cluster::start("node-dns");
    let expected = cluster.node.sh("cat /etc/floci-node/pod-resolv.conf");
    let upstream = cluster.node.sh("cat /etc/floci-node/upstream-resolv.conf");
    for _ in 0..3 {
        let actual = cluster.node.script(
            &["exec", "-i", "--", "cat", "/etc/resolv.conf"],
            COMMAND_TIMEOUT,
        );
        assert_eq!(actual.trim(), expected.trim());
        assert_eq!(
            cluster.node.sh("cat /etc/floci-node/upstream-resolv.conf"),
            upstream
        );
    }
}
