//! Pod networking. A pod with the smolvm-pods RuntimeClass runs beside a runc
//! pod, each behind a ClusterIP Service, with a NodePort for the VM pod, and a
//! stand-in for Floci listens on the Mac.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use smolvm_pods_e2e::runtime::NODE_SERVICE_LOG;
use smolvm_pods_e2e::{
    assert_all_running_without_restarts, describe, eventually, expect, http_get, run_checks, step,
    App, Arch, Cluster, NetnsHolder, Pod, PodSpec, Runtime, StandIn, State, VmProcess,
    COMMAND_TIMEOUT,
};

const APP: &str = "http://127.0.0.1:8080/";

/// How long a path may take to answer for the first time: a Service until
/// kube-proxy has its rules, a name until CoreDNS is ready.
const FIRST_ANSWER: Duration = Duration::from_secs(60);

/// A host on the internet that answers plain HTTP.
const EXTERNAL_HOST: &str = "example.com";

/// The size of each TCP stream between the pods: 100 MiB, at least 100 MB.
const STREAM_BYTES: u64 = 100 << 20;

/// Prints, one per line, what the node still has of a deleted pod VM: its VM
/// process and shim, its files and smolvm's about it, and the processes, nsfs
/// descriptors and mounts that hold its network namespace, which would keep
/// the bridge and tap0 there. Sockets that hold it are not looked for. The
/// arguments are the pod's sandbox id, the VM's directory, the namespace's
/// name and inode, and the pid of a holder to leave out, or none.
const LEFTOVERS: &str = r#"sandbox=$1 dir=$2 netns=$3 inode=$4 holder=${5:-none}
vms=${dir%/*} hash=${dir##*/}
for p in /proc/[0-9]*; do
  [ "${p#/proc/}" = "$$" ] && continue
  cmd=$(tr '\0' ' ' < "$p/cmdline" 2>/dev/null)
  case $cmd in
    *" _boot-vm $dir/"* | *"containerd-shim-smolvm-pods-v2 "*" -id $sandbox "*)
      echo "process ${p#/proc/}: $cmd" ;;
  esac
done
for f in "$dir" "$vms/.$sandbox.fork-operation.lock" \
    "$vms/.$sandbox.fork-operation.pause-operation.lock" \
    "$SMOLVM_AGENT_ROOTFS/.smolvm-ready.$hash" \
    "/var/lib/containerd-shim-smolvm-pods/$sandbox" "/var/run/netns/$netns"; do
  if [ -e "$f" ]; then echo "file $f"; fi
done
grep -lxF "$hash" "${vms%/*}"/uids/[0-9]* 2>/dev/null | sed 's/^/VM uid record /'
find /proc/[0-9]*/ns/net /proc/[0-9]*/fd -lname "net:\[$inode\]" 2>/dev/null |
  grep -v "^/proc/$holder/" | sed 's/^/network namespace held by /'
grep -l "net:\[$inode\]" /proc/[0-9]*/mountinfo 2>/dev/null | head -n 1 |
  sed 's/^/network namespace mounted in /'
"#;

/// Prints the lines of log `$1`, and of its rotated files, that contain `$2`.
const LOG_LINES: &str = r#"[ -s "$1" ] || { echo "no log $1" >&2; exit 1; }
cat "$1"* | grep -F -- "$2" || true
"#;

/// The log of containerd in the node, which also has the shim's log.
const CONTAINERD_LOG: &str = "/var/lib/rancher/k3s/agent/containerd/containerd.log";

/// What the node service logs, before the sandbox id, when it cannot remove a
/// pod's bridge (`cleanup::after_delete`).
const DETACH_WARNING: &str = "remove the pod network bridge of sandbox ";

#[test]
#[ignore = "needs a Mac with smolvm and Docker: SMOLVM_PODS_E2E=1 cargo test -- --ignored"]
fn network() {
    let cluster = Cluster::start("network");
    let floci = StandIn::start();
    let pods = cluster.run_pods(&[
        PodSpec {
            name: "vm",
            arch: Arch::Arm64,
            runtime: Runtime::SmolvmPods,
            app: App::Serve,
            containers: &[("app", 8080)],
            annotations: &[],
        },
        PodSpec {
            name: "runc",
            arch: Arch::Arm64,
            runtime: Runtime::Runc,
            app: App::Serve,
            containers: &[("app", 8080)],
            annotations: &[],
        },
    ]);
    let [vm, runc] = &pods[..] else {
        unreachable!()
    };
    let services = services(&cluster.namespace, cluster.node_port());
    cluster.apply("services.yaml", &services);
    run_checks(&[
        ("ClusterIP Service", &|| cluster_ip(&cluster, vm, runc)),
        ("NodePort from the Mac", &|| node_port(&cluster, vm)),
        ("DNS", &|| dns(&cluster, vm)),
        ("Service from the VM", &|| service_from_vm(&cluster, vm)),
        ("Floci on the Mac", &|| {
            floci_on_the_mac(&cluster, vm, &floci)
        }),
        ("external host", &|| external_host(vm)),
        ("pod IP and MTU", &|| ip_and_mtu(&cluster, vm, runc)),
        ("TCP streams", &|| streams(vm, runc)),
        ("no restarts", &|| {
            assert_all_running_without_restarts(&pods)
        }),
        ("delete", &|| delete_leaves_nothing(&cluster, vm)),
    ]);
}

/// A ClusterIP Service for each pod, and a NodePort Service for the VM pod on
/// `node_port`, which the node publishes on the Mac.
fn services(namespace: &str, node_port: u16) -> String {
    let service = |name: &str, pod: &str, kind: &str, node_port: &str| {
        format!(
            r#"apiVersion: v1
kind: Service
metadata: {{name: {name}, namespace: {namespace}}}
spec:
  type: {kind}
  selector: {{app: {pod}}}
  ports: [{{port: 80, targetPort: 8080{node_port}}}]
"#
        )
    };
    let node_port = format!(", nodePort: {node_port}");
    [
        service("vm", "vm", "ClusterIP", ""),
        service("vm-node-port", "vm", "NodePort", &node_port),
        service("runc", "runc", "ClusterIP", ""),
    ]
    .join("---\n")
}

#[test]
fn writes_the_services() {
    let expected = r#"apiVersion: v1
kind: Service
metadata: {name: vm, namespace: ns}
spec:
  type: ClusterIP
  selector: {app: vm}
  ports: [{port: 80, targetPort: 8080}]
---
apiVersion: v1
kind: Service
metadata: {name: vm-node-port, namespace: ns}
spec:
  type: NodePort
  selector: {app: vm}
  ports: [{port: 80, targetPort: 8080, nodePort: 32080}]
---
apiVersion: v1
kind: Service
metadata: {name: runc, namespace: ns}
spec:
  type: ClusterIP
  selector: {app: runc}
  ports: [{port: 80, targetPort: 8080}]
"#;
    assert_eq!(services("ns", 32080), expected);
}

/// The boot id of the kernel that the pod's app runs on.
#[track_caller]
fn boot_id(pod: &Pod) -> String {
    pod.get("app", APP)["boot_id"].to_string()
}

/// The runc pod reaches the VM pod through a ClusterIP Service, which
/// kube-proxy sends over the pod network to the VM.
fn cluster_ip(cluster: &Cluster, vm: &Pod, runc: &Pod) {
    let url = format!("http://{}/", cluster.service_ip("vm"));
    let state = eventually(FIRST_ANSWER, "the VM pod's Service to answer", || {
        runc.try_get("app", &url)
    });
    assert_eq!(
        &state["boot_id"],
        boot_id(vm),
        "GET {url} from the runc pod reached another kernel than the VM pod's"
    );
    step(&format!("the runc pod reaches the VM pod at {url}"));
}

/// The Mac reaches the VM pod through a NodePort, which node-vm.sh publishes
/// on the Mac's loopback.
fn node_port(cluster: &Cluster, vm: &Pod) {
    let addr = SocketAddr::from(([127, 0, 0, 1], cluster.node_port()));
    let body = eventually(
        FIRST_ANSWER,
        "the NodePort to answer on the Mac",
        || match http_get(addr, "/", Duration::from_secs(30))? {
            (200, body) => Ok(body),
            (status, body) => Err(format!("status {status}: {body}")),
        },
    );
    assert_eq!(
        &State::parse(&body)["boot_id"],
        boot_id(vm),
        "GET http://{addr}/ on the Mac reached another kernel than the VM pod's"
    );
    step(&format!("the Mac reaches the VM pod at http://{addr}/"));
}

/// In the VM, the cluster's DNS resolves a Service's name to its ClusterIP,
/// and resolves an external name.
fn dns(cluster: &Cluster, vm: &Pod) {
    let service = format!("runc.{}.svc.cluster.local", cluster.namespace);
    let addrs = lookup(vm, &service);
    assert_eq!(addrs, [cluster.service_ip("runc")], "{service} in the VM");
    step(&format!("{service} is {addrs:?} in the VM"));
    let addrs = lookup(vm, EXTERNAL_HOST);
    assert!(
        !addrs.is_empty() && addrs.iter().all(|addr| addr.parse::<IpAddr>().is_ok()),
        "{EXTERNAL_HOST} in the VM: {addrs:?}"
    );
    step(&format!("{EXTERNAL_HOST} is {addrs:?} in the VM"));
}

/// The addresses that `name` resolves to in the pod's app container.
#[track_caller]
fn lookup(pod: &Pod, name: &str) -> Vec<String> {
    let what = format!("{name} to resolve in pod {}", pod.name);
    let state = eventually(FIRST_ANSWER, &what, || {
        let output = pod.exec("app", &["/testapp", "lookup", name]);
        if output.success() {
            Ok(State::parse(&output.stdout))
        } else {
            Err(describe(&output))
        }
    });
    state["addrs"]
        .split_whitespace()
        .map(str::to_string)
        .collect()
}

/// The VM pod calls the runc pod's Service by its name, as apps do.
fn service_from_vm(cluster: &Cluster, vm: &Pod) {
    let url = format!("http://runc.{}.svc.cluster.local/", cluster.namespace);
    let state = eventually(FIRST_ANSWER, "the runc pod's Service to answer", || {
        vm.try_get("app", &url)
    });
    assert_eq!(
        &state["boot_id"],
        cluster.node_kernel().boot_id,
        "GET {url} from the VM pod should reach the runc pod, which runs on the node's kernel"
    );
    step(&format!("the VM pod reaches the runc pod at {url}"));
}

/// The VM pod reaches a stand-in for Floci on the Mac the way the node does:
/// at the node's default gateway, smolvm's, which takes TCP to the Mac's
/// loopback. The answer needs full-size TCP segments, which only pass if the
/// VM uses the pod network's MTU.
fn floci_on_the_mac(cluster: &Cluster, vm: &Pod, floci: &StandIn) {
    let url = format!("http://{}:{}/", cluster.node.gateway(), floci.port());
    let from_node = cluster
        .node
        .sh(&format!("curl -sS --fail --max-time 30 {url}"));
    assert_eq!(
        &State::parse(&from_node)["token"],
        floci.token(),
        "GET {url} from the node"
    );
    let state = eventually(FIRST_ANSWER, "the stand-in to answer the VM pod", || {
        vm.try_get("app", &url)
    });
    assert_eq!(&state["token"], floci.token(), "GET {url} from the VM pod");
    assert_eq!(
        state["padding"].len(),
        StandIn::PADDING,
        "the padding of GET {url} from the VM pod"
    );
    step(&format!(
        "the VM pod reaches the stand-in for Floci at {url}"
    ));
}

/// The VM pod calls a host on the internet.
fn external_host(vm: &Pod) {
    let url = format!("http://{EXTERNAL_HOST}/");
    eventually(FIRST_ANSWER, &format!("{url} to answer the VM pod"), || {
        let output = vm.exec("app", &["/testapp", "get", "-timeout", "30s", &url]);
        if output.success() && output.stdout.contains("Example Domain") {
            Ok(())
        } else {
            Err(describe(&output))
        }
    });
    step(&format!("the VM pod reaches {url}"));
}

/// In the VM, eth0 has the pod IP that Kubernetes reports, and the pod
/// network's MTU: that of the pod's CNI interface and of a runc pod's eth0.
fn ip_and_mtu(cluster: &Cluster, vm: &Pod, runc: &Pod) {
    let guest = interface(vm);
    let ips: Vec<&str> = guest["ipv4"]
        .split_whitespace()
        .filter_map(|addr| addr.split('/').next())
        .collect();
    let pod_ip = vm.ip();
    assert_eq!(
        ips,
        [pod_ip.as_str()],
        "the IPv4 addresses of eth0 in the VM"
    );
    let netns = netns_of(cluster, &vm_process(vm));
    let cni_mtu = cluster.node.sh(&format!(
        r"ip -n {netns} -o link show dev eth0 | sed -n 's/.* mtu \([0-9]*\) .*/\1/p'"
    ));
    assert_eq!(
        &guest["mtu"],
        cni_mtu.trim(),
        "the MTU of eth0 in the VM, and of the pod's CNI interface"
    );
    assert_eq!(
        &guest["mtu"],
        &interface(runc)["mtu"],
        "the MTU of eth0 in the VM, and in the runc pod"
    );
    step(&format!(
        "eth0 in the VM has {} and MTU {}",
        &guest["ipv4"], &guest["mtu"]
    ));
}

/// What the pod's app container sees of its eth0.
#[track_caller]
fn interface(pod: &Pod) -> State {
    let output = pod.exec("app", &["/testapp", "iface", "eth0"]);
    assert!(
        output.success(),
        "eth0 in pod {}: {}",
        pod.name,
        describe(&output)
    );
    State::parse(&output.stdout)
}

/// The pod's one VM process.
#[track_caller]
fn vm_process(pod: &Pod) -> VmProcess {
    let vms = pod.vm_processes();
    let [vm] = &vms[..] else {
        panic!(
            "the node should run one VM process for pod {} (sandbox {}), runs {vms:?}",
            pod.name, pod.sandbox
        );
    };
    vm.clone()
}

/// The name of the network namespace that a VM process is in, which is that
/// of its pod.
#[track_caller]
fn netns_of(cluster: &Cluster, vm: &VmProcess) -> String {
    let names = cluster.node.sh(&format!("ip netns identify {}", vm.pid));
    let names: Vec<&str> = names.split_whitespace().collect();
    let [name] = names[..] else {
        panic!(
            "VM process {} should be in one named network namespace, is in {names:?}",
            vm.pid
        );
    };
    name.to_string()
}

/// A 100 MiB TCP stream from the runc pod to the VM pod, and one back, arrive
/// whole: the receiver hashes the same bytes as the sender. The throughput of
/// each is recorded.
fn streams(vm: &Pod, runc: &Pod) {
    let bytes = STREAM_BYTES.to_string();
    for (from, to) in [(runc, vm), (vm, runc)] {
        let url = format!("http://{}:8080/sink", to.ip());
        let send = [
            "/testapp", "send", "-bytes", &bytes, "-timeout", "240s", &url,
        ];
        let output = from.exec("app", &send);
        assert!(
            output.success(),
            "stream from pod {} to {url}: {}",
            from.name,
            describe(&output)
        );
        let stream = State::parse(&output.stdout);
        let path = format!("the stream from pod {} to pod {}", from.name, to.name);
        assert_eq!(&stream["sent_bytes"], bytes, "the size of {path}");
        assert_eq!(
            &stream["received_bytes"], &stream["sent_bytes"],
            "the bytes received of {path}"
        );
        assert_eq!(
            &stream["received_sha256"], &stream["sent_sha256"],
            "the SHA-256 of {path}"
        );
        let seconds: f64 = stream["seconds"].parse().expect("the stream's duration");
        let mib = (STREAM_BYTES >> 20) as f64;
        step(&format!(
            "{path}: {mib} MiB in {seconds:.2} s, {:.1} MiB/s",
            mib / seconds
        ));
    }
}

/// Deleting the VM pod ends its VM process and its shim, removes the bridge
/// and tap0 from its network namespace, and removes the namespace, its pod
/// share, its VM directory, and smolvm's other files about the VM.
fn delete_leaves_nothing(cluster: &Cluster, vm: &Pod) {
    let process = vm_process(vm);
    let netns = netns_of(cluster, &process);
    let links = cluster.node.sh(&format!("ip -n {netns} -o link show"));
    for link in ["podbr0", "tap0"] {
        assert!(
            links.contains(&format!(": {link}:")),
            "pod {} has no {link} before the delete:\n{links}",
            vm.name
        );
    }
    let inode = cluster
        .node
        .sh(&format!("stat -L -c %i /var/run/netns/{netns}"));
    // Removing the namespace would remove the bridge and tap0 whatever the
    // shim did. A process of the test keeps it, as a socket or process left
    // in it would, so that the shim's own removal is what is checked.
    let holder = NetnsHolder::start(cluster, &netns);
    cluster.delete_pod(vm.name);
    let left_nothing = |holder: &str| {
        let args = [
            "sh",
            "-c",
            LEFTOVERS,
            "sh",
            &vm.sandbox,
            &process.dir,
            &netns,
            inode.trim(),
            holder,
        ];
        eventually(
            Duration::from_secs(120),
            "the VM pod to leave nothing",
            || {
                let output = cluster.node.exec(&args, COMMAND_TIMEOUT);
                if !output.success() {
                    return Err(describe(&output));
                }
                match output.stdout.trim() {
                    "" => Ok(()),
                    left => Err(format!("left in the node:\n{left}")),
                }
            },
        );
    };
    left_nothing(&holder.pid);
    let links = expect(holder.links());
    for link in ["podbr0", "tap0"] {
        assert!(
            !links.contains(&format!(": {link}:")),
            "the network namespace of pod {} has {link} after the delete:\n{links}",
            vm.name
        );
    }
    let warning = format!("{DETACH_WARNING}{}", vm.sandbox);
    for log in [CONTAINERD_LOG, NODE_SERVICE_LOG] {
        let args = ["sh", "-c", LOG_LINES, "sh", log, &warning];
        let output = cluster.node.exec(&args, COMMAND_TIMEOUT);
        assert!(output.success(), "{log}: {}", describe(&output));
        assert_eq!(
            output.stdout.trim(),
            "",
            "{log} on the delete of pod {}",
            vm.name
        );
    }
    drop(holder);
    left_nothing("");
    step(&format!(
        "pod {} left nothing of VM {} and network namespace {netns}",
        vm.name, process.dir
    ));
}
