//! Pods with the smolvm-pods RuntimeClass run as nested VMs in the node VM,
//! amd64 pods under Rosetta with TSO.

use std::collections::HashSet;
use std::env;
use std::path::Path;
use std::time::{Duration, Instant};

use smolvm_pods_e2e::runtime::{BINARY, NODE_SERVICE_LOG, TSO_MARKER};
use smolvm_pods_e2e::{
    assert_all_running_without_restarts, assert_running_without_restarts, describe, eventually,
    run_checks, step, App, Arch, Cluster, Output, Pod, PodSpec, Runtime, State, NO_ROSETTA,
    ROSETTA,
};

const APP: &str = "http://127.0.0.1:8080/";
const SIDECAR: &str = "http://127.0.0.1:8081/";

/// The shim's own test of the pod network bridge, run in the node.
const NETNS_TEST: &str = "net::netns_tests::smolvm_attaches_a_pod_through_the_bridge";

/// The TSO canary's verdict in the node (see the runtime's src/canary.rs).
const CANARY_VERDICT: &str = "/var/lib/containerd-shim-smolvm-pods/.tso-canary";

/// The detail of the canary failure that the test injects, which the
/// refusals and the warning must pass on.
const INJECTED: &str = "a failure the test injected";

/// The files that kubelet and containerd mount into each container, and the
/// line that the check appends to each. The app runs as root without a
/// read-only root filesystem, so it may write all three, as under runc.
const ETC_FILES: [(&str, &str); 3] = [
    ("/etc/hosts", "10.0.0.9 e2e-etc-hosts"),
    ("/etc/resolv.conf", "# e2e"),
    ("/etc/hostname", "# e2e"),
];

#[test]
#[ignore = "needs a Mac with smolvm and Docker: SMOLVM_PODS_E2E=1 cargo test -- --ignored"]
fn pods() {
    let cluster = Cluster::start("pods");
    let pods = cluster.run_pods(&[
        PodSpec {
            name: "arm64",
            arch: Arch::Arm64,
            runtime: Runtime::SmolvmPods,
            app: App::Serve,
            containers: &[("app", 8080), ("sidecar", 8081)],
            annotations: &[NO_ROSETTA],
        },
        // Rosetta by default.
        PodSpec {
            name: "amd64",
            arch: Arch::Amd64,
            runtime: Runtime::SmolvmPods,
            app: App::Serve,
            containers: &[("app", 8080)],
            annotations: &[],
        },
        PodSpec {
            name: "arm64-tso",
            arch: Arch::Arm64,
            runtime: Runtime::SmolvmPods,
            app: App::Serve,
            containers: &[("app", 8080)],
            annotations: &[ROSETTA],
        },
    ]);
    let [arm64, amd64, arm64_tso] = &pods[..] else {
        unreachable!()
    };
    // An amd64 app's first answer runs code under Rosetta for the first time.
    for pod in &pods {
        eventually(Duration::from_secs(120), "a first GET / to answer", || {
            pod.try_get("app", APP)
        });
    }
    run_checks(&[
        ("own VMs", &|| own_vms(&cluster, &pods)),
        ("architectures", &|| architectures(&cluster, arm64, amd64)),
        ("TSO", &|| tso(arm64, amd64, arm64_tso)),
        ("logs and exec", &|| logs_and_exec(arm64)),
        ("sidecar", &|| sidecar(arm64)),
        ("writable /etc files", &|| writable_etc_files(arm64)),
        ("pod network", &|| pod_network(&cluster, &pods)),
        ("bridge in the node", &|| bridge_in_the_node(&cluster)),
        ("stdin refused", &|| stdin_refused(arm64)),
        ("amd64 with Rosetta off", &|| {
            amd64_without_rosetta(&cluster)
        }),
        ("TSO canary", &|| tso_canary(&cluster)),
        ("no restarts", &|| {
            assert_all_running_without_restarts(&pods)
        }),
        ("delete", &|| delete_removes_vms(&cluster, &pods)),
    ]);
}

/// Each pod is Running and Ready with no restarts, its kernel is not the
/// node's or another pod's, and the node runs one VM process for it, whose
/// directory is on the same mount as smolvm's disk templates.
fn own_vms(cluster: &Cluster, pods: &[Pod]) {
    let node = cluster.node_kernel();
    let mut boot_ids = HashSet::from([node.boot_id]);
    for pod in pods {
        assert_running_without_restarts(pod);

        let app = pod.get("app", APP);
        assert_ne!(
            &app["cmdline"], node.cmdline,
            "pod {} sees the node's kernel command line",
            pod.name
        );
        assert!(
            boot_ids.insert(app["boot_id"].to_string()),
            "pod {} shares its kernel boot id {} with the node or another pod",
            pod.name,
            &app["boot_id"]
        );
        let vms = pod.vm_processes();
        let [vm] = &vms[..] else {
            panic!(
                "the node should run one VM process for pod {} (sandbox {}), runs {vms:?}",
                pod.name, pod.sandbox
            );
        };
        let devices = cluster.node.sh(&format!(
            "stat -c %d {} /var/lib/smolvm/.smolvm/storage-template.ext4 \
             /var/lib/smolvm/.smolvm/overlay-template.ext4",
            vm.dir
        ));
        let devices: HashSet<&str> = devices.split_whitespace().collect();
        assert_eq!(
            devices.len(),
            1,
            "the VM directory {} of pod {} and the disk templates are on different mounts",
            vm.dir,
            pod.name
        );
        step(&format!(
            "pod {}: boot id {}, VM process {} in {}",
            pod.name, &app["boot_id"], vm.pid, vm.dir
        ));
    }
}

/// The arm64 app is an arm64 build. The amd64 app is an x86-64 build (ELF
/// machine 62) and runs on an arm64 node, which only Rosetta makes possible.
fn architectures(cluster: &Cluster, arm64: &Pod, amd64: &Pod) {
    assert_eq!(
        cluster.node_kernel().machine,
        "aarch64",
        "the node's machine"
    );
    let app = arm64.get("app", APP);
    assert_eq!(
        (&app["arch"], &app["machine"]),
        ("arm64", "183"),
        "the arm64 app"
    );
    let app = amd64.get("app", APP);
    assert_eq!(
        (&app["arch"], &app["machine"]),
        ("amd64", "62"),
        "the amd64 app"
    );
}

/// The litmus test finds no reordering in the amd64 pod, which runs under
/// Rosetta by default, or in the arm64 pod that requires Rosetta. In the
/// arm64 pod with Rosetta off it must find some; otherwise it could not tell
/// TSO from no TSO.
fn tso(arm64: &Pod, amd64: &Pod, arm64_tso: &Pod) {
    for pod in [amd64, arm64_tso] {
        let output = litmus(pod);
        assert!(
            output.success(),
            "litmus in pod {}: {}",
            pod.name,
            describe(&output)
        );
        step(&format!("pod {}: {}", pod.name, result(&output.stdout)));
    }
    let output = litmus(arm64);
    assert_eq!(
        output.code(),
        Some(3),
        "litmus should find reordering in pod {} without TSO: {}",
        arm64.name,
        describe(&output)
    );
    step(&format!(
        "pod {} without TSO: {}",
        arm64.name,
        result(&output.stdout)
    ));
}

/// Runs the litmus test in `pod`, again if its threads overlapped too little
/// to tell (exit code 4), at most three times, as the TSO canary does.
#[track_caller]
fn litmus(pod: &Pod) -> Output {
    let mut output = pod.exec("app", &["/litmus"]);
    for _ in 1..3 {
        if output.code() != Some(4) {
            break;
        }
        step(&format!(
            "pod {}: {}, again",
            pod.name,
            result(&output.stdout)
        ));
        output = pod.exec("app", &["/litmus"]);
    }
    output
}

/// The RESULT line of the litmus test's output.
fn result(stdout: &str) -> &str {
    stdout.lines().last().unwrap_or_default()
}

/// kubectl exec passes on stdout, stderr and the exit code, and kubectl logs
/// shows what the app printed to stdout and stderr.
fn logs_and_exec(pod: &Pod) {
    let output = pod.exec("app", &["/testapp", "echo", "hello", "from", "exec"]);
    assert!(output.success(), "{}", describe(&output));
    assert_eq!(output.stdout, "hello from exec\n");

    let output = pod.exec("app", &["/testapp", "exit", "7"]);
    assert_eq!(output.code(), Some(7), "{}", describe(&output));
    assert!(
        output.stdout.contains("stdout before exit 7"),
        "{}",
        describe(&output)
    );
    assert!(
        output.stderr.contains("stderr before exit 7"),
        "{}",
        describe(&output)
    );

    let line = format!("GET / request={}", &pod.get("app", APP)["request"]);
    let logs = eventually(Duration::from_secs(30), "the app's log", || {
        let logs = pod.logs("app");
        if logs.contains(&line) {
            Ok(logs)
        } else {
            Err(format!("no {line:?} in:\n{logs}"))
        }
    });
    assert!(
        logs.contains("app listening on :8080"),
        "stdout is missing:\n{logs}"
    );
    assert!(logs.contains("app started"), "stderr is missing:\n{logs}");
}

/// The app and its sidecar reach each other on localhost, and run on the same
/// kernel.
fn sidecar(pod: &Pod) {
    let app = pod.get("sidecar", APP);
    let sidecar = pod.get("app", SIDECAR);
    assert_eq!(&app["name"], "app");
    assert_eq!(&sidecar["name"], "sidecar");
    assert_eq!(
        &app["boot_id"], &sidecar["boot_id"],
        "boot ids of app and sidecar"
    );
}

/// The app appends a line to each of [`ETC_FILES`] and reads it back.
fn writable_etc_files(pod: &Pod) {
    let mut failures = Vec::new();
    for (path, line) in ETC_FILES {
        let output = pod.exec("app", &["/testapp", "append", path, line]);
        if output.success() && output.stdout.ends_with(&format!("{line}\n")) {
            step(&format!("pod {}: appended {line:?} to {path}", pod.name));
        } else {
            failures.push(format!("{path}: {}", describe(&output)));
        }
    }
    assert!(
        failures.is_empty(),
        "the app of pod {} could not append a line to each /etc file and read it back:\n{}",
        pod.name,
        failures.join("\n")
    );
}

/// Each pod's app answers at the pod's IP, from the node and from another
/// pod. Both calls take the pod network path: cni0, the pod's eth0, podbr0,
/// tap0 and the VM's network card.
fn pod_network(cluster: &Cluster, pods: &[Pod]) {
    for (i, pod) in pods.iter().enumerate() {
        let boot_id = pod.get("app", APP)["boot_id"].to_string();
        let url = format!("http://{}:8080/", pod.ip());
        let from_node = cluster
            .node
            .sh(&format!("curl -sS --fail --max-time 30 {url}"));
        let other = &pods[(i + 1) % pods.len()];
        let from_pod = other.get("app", &url);
        for (from, state) in [
            ("the node", State::parse(&from_node)),
            (other.name, from_pod),
        ] {
            assert_eq!(&state["name"], "app", "GET {url} from {from}");
            assert_eq!(
                &state["boot_id"], boot_id,
                "GET {url} from {from} reached another kernel than pod {}",
                pod.name
            );
        }
        step(&format!("pod {} answers at {url}", pod.name));
    }
}

/// The shim's test of the pod network bridge passes against the node's
/// kernel, in network namespaces of its own. SMOLVM_PODS_E2E_NETNS_TEST is
/// the shim's unit test binary (see README.md). It runs in a mount namespace
/// of its own: `ip netns add` makes /run/netns a mount of its own, which
/// would also hold the pods' namespaces, and containerd could then not remove
/// them.
fn bridge_in_the_node(cluster: &Cluster) {
    let binary = env::var_os("SMOLVM_PODS_E2E_NETNS_TEST")
        .expect("SMOLVM_PODS_E2E_NETNS_TEST is not set to the shim's unit test binary");
    let dst = cluster.node_path("netns-test");
    cluster.node.copy_in(Path::new(&binary), &dst);
    let run = format!(
        "chmod +x {dst} && SMOLVM_PODS_BIN={BINARY} \
         unshare --mount --propagation private {dst} --ignored --exact {NETNS_TEST}"
    );
    let output = cluster
        .node
        .exec(&["sh", "-c", &run], Duration::from_secs(300));
    assert!(
        output.success() && output.stdout.contains("1 passed"),
        "{}",
        describe(&output)
    );
}

/// A process with stdin is refused at once, and says why: the guest agent
/// does not pass input to pod processes.
fn stdin_refused(pod: &Pod) {
    let started = Instant::now();
    let output = pod.exec_with_input("app", &["/testapp", "cat"], b"from stdin\n");
    assert!(!output.success(), "{}", describe(&output));
    assert!(
        output.stderr.contains("does not pass stdin"),
        "{}",
        describe(&output)
    );
    step(&format!("kubectl exec -i: {}", output.stderr.trim()));
    assert!(
        started.elapsed() < Duration::from_secs(60),
        "the refusal took {:?}",
        started.elapsed()
    );
}

/// An amd64 pod that turns Rosetta off fails at once, with a reason that
/// names the annotation.
fn amd64_without_rosetta(cluster: &Cluster) {
    let spec = PodSpec {
        name: "amd64-plain",
        arch: Arch::Amd64,
        runtime: Runtime::SmolvmPods,
        app: App::Serve,
        containers: &[("app", 8080)],
        annotations: &[NO_ROSETTA],
    };
    cluster.create_pods(&[spec]);
    let reason = format!("because the pod has the annotation {}", NO_ROSETTA.0);
    let refused = eventually(
        Duration::from_secs(180),
        "the container to be refused",
        || {
            let events = cluster.events("amd64-plain");
            if events.contains("x86-64") && events.contains(&reason) {
                Ok(events)
            } else {
                Err(format!("events: {events}"))
            }
        },
    );
    step(&format!("pod amd64-plain: {refused}"));
    cluster.delete_pod("amd64-plain");
}

/// The node's TSO canary passed at boot. While its verdict is a failure, a
/// pod that requires Rosetta is refused with the canary's reason, and pods
/// without the annotation run without Rosetta: an arm64 app runs in a VM
/// without TSO, with a warning in the node service's log, and an amd64 app is
/// refused with the canary's reason.
fn tso_canary(cluster: &Cluster) {
    let verdict = cluster.node.sh(&format!("cat {CANARY_VERDICT}"));
    let boot_id = cluster.node_kernel().boot_id;
    assert!(
        verdict.contains(&format!("\"boot_id\":\"{boot_id}\"")),
        "the canary's verdict is not from this node boot: {verdict}"
    );
    assert!(
        verdict.contains("\"passed\":true"),
        "the canary failed: {verdict}"
    );
    step(&format!("TSO canary: {verdict}"));

    let failed = format!(r#"{{"boot_id":"{boot_id}","passed":false,"detail":"{INJECTED}"}}"#);
    let _restore = Restore {
        cluster,
        path: CANARY_VERDICT,
        contents: verdict,
    };
    write_verdict(cluster, CANARY_VERDICT, &failed);
    let pod = |name, arch, annotations| PodSpec {
        name,
        arch,
        runtime: Runtime::SmolvmPods,
        app: App::Serve,
        containers: &[("app", 8080)],
        annotations,
    };
    let names = ["amd64-refused", "amd64-fallback", "arm64-fallback"];
    cluster.create_pods(&[
        pod(names[0], Arch::Amd64, &[ROSETTA]),
        pod(names[1], Arch::Amd64, &[]),
        pod(names[2], Arch::Arm64, &[]),
    ]);
    for (name, what, needs) in [
        (names[0], "the sandbox", "TSO canary failed"),
        (names[1], "the container", "x86-64"),
    ] {
        let refused = eventually(
            Duration::from_secs(120),
            &format!("{what} to be refused"),
            || {
                let events = cluster.events(name);
                if events.contains(needs) && events.contains(INJECTED) {
                    Ok(events)
                } else {
                    Err(format!("events: {events}"))
                }
            },
        );
        step(&format!("pod {name}: {refused}"));
    }
    let sandbox = eventually(Duration::from_secs(180), "the pod to be Ready", || {
        cluster.ready_sandbox(names[2])
    });
    let fallback = cluster.ready_pod(names[2]);
    eventually(Duration::from_secs(120), "a first GET / to answer", || {
        fallback.try_get("app", APP)
    });
    let vms = fallback.vm_processes();
    let [vm] = &vms[..] else {
        panic!("pod {} should have one VM process, has {vms:?}", names[2]);
    };
    let marker = format!("{}/{TSO_MARKER}", vm.dir);
    assert!(
        cluster.existing(&[marker.as_str()]).is_empty(),
        "the VM process of pod {} turned TSO on: {marker}",
        names[2]
    );
    let warning = format!("sandbox {sandbox}: the pod runs without Rosetta");
    let logged = cluster.node.sh(&format!(
        "grep -F -- '{warning}' {NODE_SERVICE_LOG} || true"
    ));
    assert!(
        logged.contains(INJECTED),
        "the node service logged no {warning:?} with the canary's reason"
    );
    step(logged.trim());
    let output = litmus(&fallback);
    assert_eq!(
        output.code(),
        Some(3),
        "litmus should find reordering in pod {} without TSO: {}",
        names[2],
        describe(&output)
    );
    for name in names {
        cluster.delete_pod(name);
    }
}

/// Writes a canary verdict in the node in one step, as the shim does.
#[track_caller]
fn write_verdict(cluster: &Cluster, path: &str, contents: &str) {
    let tmp = format!("{path}.e2e");
    cluster.node.write(&tmp, contents);
    cluster.node.sh(&format!("mv {tmp} {path}"));
}

/// Puts back the canary's verdict when the check ends, also when it fails.
struct Restore<'a> {
    cluster: &'a Cluster,
    path: &'static str,
    contents: String,
}

impl Drop for Restore<'_> {
    fn drop(&mut self) {
        write_verdict(self.cluster, self.path, &self.contents);
    }
}

/// Deleting the pods ends their VM processes and removes their VM
/// directories.
fn delete_removes_vms(cluster: &Cluster, pods: &[Pod]) {
    let vms: Vec<_> = pods.iter().flat_map(Pod::vm_processes).collect();
    assert_eq!(
        vms.len(),
        pods.len(),
        "VM processes before the delete: {vms:?}"
    );
    let delete = [
        "delete",
        "pods",
        "--all",
        "-n",
        &cluster.namespace,
        "--timeout=100s",
    ];
    cluster.node.kubectl_ok(&delete);
    let dirs: Vec<&str> = vms.iter().map(|vm| vm.dir.as_str()).collect();
    eventually(Duration::from_secs(120), "the pods' VMs to go", || {
        let running: Vec<_> = cluster
            .vm_processes()
            .into_iter()
            .filter(|p| vms.iter().any(|vm| vm.machine == p.machine))
            .collect();
        let left = cluster.existing(&dirs);
        if running.is_empty() && left.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "VM processes {running:?} and directories {left:?} remain"
            ))
        }
    });
}
