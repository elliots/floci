//! A host port and a Mac directory share on a node created with extra options.

use std::fs;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use smolvm_pods_e2e::{eventually, http_get, App, Arch, Cluster, NodeSetup, PodSpec, Runtime};

#[test]
#[ignore = "needs a Mac with smolvm and Docker: SMOLVM_PODS_E2E=1 cargo test -- --ignored"]
fn extra_port_and_host_directory_reach_the_node() {
    let host_port = match TcpListener::bind((Ipv4Addr::LOCALHOST, 1023)) {
        Ok(listener) => {
            drop(listener);
            1023
        }
        Err(error) => {
            eprintln!("port 1023 is unavailable to the test user: {error}");
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
            listener.local_addr().unwrap().port()
        }
    };
    let id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let host = std::env::temp_dir().join(format!("floci-node-mapping-{id}"));
    fs::create_dir_all(&host).unwrap();
    fs::write(host.join("marker"), "shared from the Mac\n").unwrap();
    let volume = format!("{}:/mnt/mac-share:ro", host.display());
    let mapping = format!("{host_port}:30443");
    let cluster = Cluster::start_with(
        "mappings",
        NodeSetup {
            create: &["--volume", &volume, "--", "--port", &mapping],
            ..NodeSetup::default()
        },
    );
    assert_eq!(
        cluster.node.sh("cat /mnt/mac-share/marker").trim(),
        "shared from the Mac"
    );
    cluster.run_pods(&[PodSpec {
        name: "web",
        arch: Arch::Arm64,
        runtime: Runtime::Runc,
        app: App::Serve,
        containers: &[("app", 8080)],
        annotations: &[],
    }]);
    cluster.apply(
        "service.yaml",
        &format!(
            "apiVersion: v1\nkind: Service\nmetadata: {{name: web, namespace: {}}}\nspec:\n  type: NodePort\n  selector: {{app: web}}\n  ports: [{{port: 80, targetPort: 8080, nodePort: 30443}}]\n",
            cluster.namespace
        ),
    );
    let address = SocketAddr::from((Ipv4Addr::LOCALHOST, host_port));
    eventually(
        Duration::from_secs(60),
        "the host port to reach the NodePort",
        || {
            let (status, body) = http_get(address, "/healthz", Duration::from_secs(2))?;
            if status == 200 {
                Ok(())
            } else {
                Err(format!("HTTP {status}: {body}"))
            }
        },
    );
    drop(cluster);
    fs::remove_dir_all(host).unwrap();
}
