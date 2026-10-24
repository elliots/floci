//! A node with k3s ready, a namespace for the test, and the test image.

use std::env;
use std::hash::{BuildHasher, RandomState};
use std::net::TcpListener;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::Duration;

use crate::cmd::{self, describe};
use crate::node::{Node, COMMAND_TIMEOUT};
use crate::pod::{Arch, Pod, PodSpec, VmProcess};
use crate::{eventually, expect, images, step, RUNTIME_CLASS};

const CREATE_TIMEOUT: Duration = Duration::from_secs(900);
const START_TIMEOUT: Duration = Duration::from_secs(600);
/// How long k3s, and then each pod, may take to become ready.
const READY_TIMEOUT: Duration = Duration::from_secs(300);

/// vCPUs of a node the tests create: the pods' VMs have 2 each, and an amd64
/// app under Rosetta is slow on a node with too few.
const NODE_CPUS: &str = "8";

/// The shim in the node.
const NODE_SHIM: &str = "/usr/local/bin/containerd-shim-smolvm-pods-v2";

/// Prints `PID DIR MACHINE` for each VM process on the node: a process whose
/// arguments are `EXE _boot-vm DIR/boot-config.json`. smolvm records the
/// machine's name in `DIR/name`.
const VM_PROCESSES: &str = r#"for p in /proc/[0-9]*; do
  set -- $(tr '\0' ' ' < "$p/cmdline" 2>/dev/null)
  if [ "$2" = _boot-vm ]; then
    echo "${p#/proc/} ${3%/*} $(cat "${3%/*}/name" 2>/dev/null)"
  fi
done"#;

/// The node's transparent huge page counters and khugepaged settings. The
/// counters show whether khugepaged or MADV_COLLAPSE collapsed memory, which
/// can crash nested VMs under Hypervisor.framework.
const THP_STATE: &str = r#"grep -E '^thp_(collapse_alloc|collapse_alloc_failed|fault_alloc|fault_fallback) ' /proc/vmstat
k=/sys/kernel/mm/transparent_hugepage/khugepaged
for f in pages_to_scan scan_sleep_millisecs alloc_sleep_millisecs defrag max_ptes_none full_scans pages_collapsed; do
  echo "khugepaged/$f $(cat "$k/$f")"
done"#;

/// How a test makes its node, beyond what every test's node has.
#[derive(Default)]
pub struct NodeSetup<'a> {
    /// The node's memory in MiB, or node-vm.sh's default.
    pub memory_mib: Option<u32>,
    /// Publish the node service's API on a free port of the Mac
    /// ([`Cluster::service_port`]).
    pub service_port: bool,
    /// More options for `node-vm.sh create`.
    pub create: &'a [&'a str],
    /// Runs as soon as the node VM has started, before the test waits for
    /// k3s.
    pub started: Option<&'a dyn Fn(&Node)>,
}

impl NodeSetup<'_> {
    /// Whether the test needs a node of its own.
    fn needs_own_node(&self) -> bool {
        self.service_port || !self.create.is_empty() || self.started.is_some()
    }
}

/// What identifies a running kernel.
pub struct Kernel {
    pub boot_id: String,
    pub cmdline: String,
    /// What `uname -m` prints.
    pub machine: String,
}

/// A node with k3s ready, a namespace for the test, and the test image
/// imported for each architecture. Dropping it removes what the test created.
pub struct Cluster {
    pub node: Node,
    pub namespace: String,
    /// `floci-SUITE-e2e`, which with `id` names what the test creates.
    prefix: String,
    id: String,
    /// A NodePort that the node publishes on the Mac.
    node_port: u16,
    /// The Mac port of the node service's API, when the node publishes it.
    service_port: Option<u16>,
    owns_node: bool,
    keep: bool,
    /// Scratch directory on this host.
    work: PathBuf,
    /// Scratch directory in the node.
    node_dir: String,
}

impl Cluster {
    /// Prepares a cluster for the test `suite`, such as "network". What it
    /// creates is named `floci-SUITE-e2e-ID`, or `PREFIX-ID` with
    /// `SMOLVM_PODS_E2E_PREFIX`. Fails unless the end-to-end tests are on, so
    /// that a run cannot pass without a node.
    pub fn start(suite: &str) -> Cluster {
        Cluster::start_with(suite, NodeSetup::default())
    }

    /// Like [`Cluster::start`], but a node that the test creates gets
    /// `memory_mib` MiB of memory.
    pub fn start_with_memory(suite: &str, memory_mib: Option<u32>) -> Cluster {
        Cluster::start_with(
            suite,
            NodeSetup {
                memory_mib,
                ..NodeSetup::default()
            },
        )
    }

    /// Like [`Cluster::start`], but a node that the test creates is made as
    /// `setup` says. A setup that needs more than the memory needs a node of
    /// the test's own, so it fails with `SMOLVM_PODS_E2E_NODE`.
    pub fn start_with(suite: &str, setup: NodeSetup) -> Cluster {
        assert_eq!(
            env::var("SMOLVM_PODS_E2E").as_deref(),
            Ok("1"),
            "set SMOLVM_PODS_E2E=1 to run the smolvm-pods end-to-end tests"
        );
        let prefix = env::var("SMOLVM_PODS_E2E_PREFIX")
            .ok()
            .filter(|prefix| !prefix.is_empty())
            .unwrap_or_else(|| format!("floci-{suite}-e2e"));
        let id = format!("{:06x}", RandomState::new().hash_one(0) & 0xff_ffff);
        let name = format!("{prefix}-{id}");
        let existing = env::var("SMOLVM_PODS_E2E_NODE").ok();
        let existing = existing.filter(|name| !name.is_empty());
        assert!(
            existing.is_none() || !setup.needs_own_node(),
            "the {suite} test creates a node of its own: unset SMOLVM_PODS_E2E_NODE"
        );
        let node_port = if existing.is_some() {
            published_node_port()
        } else {
            free_port(30000..=32767)
        };
        let cluster = Cluster {
            owns_node: existing.is_none(),
            service_port: setup.service_port.then(|| free_port(38000..=38999)),
            node: Node::new(existing.unwrap_or_else(|| name.clone())),
            namespace: name.clone(),
            keep: env::var("SMOLVM_PODS_E2E_KEEP").as_deref() == Ok("1"),
            work: env::temp_dir().join(&name),
            node_dir: format!("/var/tmp/{name}"),
            node_port,
            prefix,
            id,
        };
        // A failure from here on drops the cluster, which removes what exists.
        cluster.prepare(&setup);
        cluster
    }

    fn prepare(&self, setup: &NodeSetup) {
        expect(images::require_go_image());
        if self.owns_node {
            self.create_node(setup);
        }
        let probe = self.node.exec(&["sh", "-c", "exit 3"], COMMAND_TIMEOUT);
        assert_eq!(
            probe.code(),
            Some(3),
            "node-vm.sh exec must pass on the exit status of the command: {}",
            describe(&probe)
        );
        self.wait_for_k3s();
        self.import_images();
        self.node
            .kubectl_ok(&["create", "namespace", &self.namespace]);
        let account = ["get", "serviceaccount", "default", "-n", &self.namespace];
        eventually(READY_TIMEOUT, "the default service account", || {
            self.node.try_kubectl(&account, COMMAND_TIMEOUT)
        });
    }

    /// Creates and starts the node VM, publishing its API, its NodePort and
    /// the node service's API if the setup asks, on free ports so that it
    /// does not clash with other nodes.
    fn create_node(&self, setup: &NodeSetup) {
        step(&format!("creating node VM {}", self.node.name()));
        let api = free_port(40000..=48999).to_string();
        let node_ports = format!("{0}-{0}", self.node_port);
        let mut create = vec![
            "create",
            "--cpus",
            NODE_CPUS,
            "--api-port",
            &api,
            "--node-ports",
            &node_ports,
        ];
        let memory = setup.memory_mib.map(|mib| mib.to_string());
        if let Some(memory) = &memory {
            create.extend(["--mem", memory]);
        }
        let service_port = self.service_port.map(|port| port.to_string());
        if let Some(port) = &service_port {
            create.extend(["--service-port", port]);
        }
        create.extend(setup.create);
        self.node.script(&create, CREATE_TIMEOUT);
        step("starting the node VM");
        match setup.started {
            Some(started) => {
                self.node
                    .script(&["start", "--timeout", "0"], START_TIMEOUT);
                started(&self.node);
            }
            None => {
                self.node.script(&["start"], START_TIMEOUT);
            }
        }
    }

    /// Stops the node VM, starts it again and waits for k3s.
    pub fn restart_node(&self) {
        step(&format!("restarting node VM {}", self.node.name()));
        self.node.script(&["stop"], START_TIMEOUT);
        self.node.script(&["start"], START_TIMEOUT);
        self.wait_for_k3s();
    }

    fn wait_for_k3s(&self) {
        step("waiting for k3s");
        eventually(READY_TIMEOUT, "the Kubernetes API", || {
            self.node
                .try_kubectl(&["get", "--raw", "/readyz"], COMMAND_TIMEOUT)
        });
        let ready = r#"jsonpath={.items[*].status.conditions[?(@.type=="Ready")].status}"#;
        eventually(READY_TIMEOUT, "a Ready node", || {
            let status = self
                .node
                .try_kubectl(&["get", "nodes", "-o", ready], COMMAND_TIMEOUT)?;
            match status.trim() {
                "True" => Ok(()),
                other => Err(format!("node Ready is {other:?}")),
            }
        });
        let handler = [
            "get",
            "runtimeclass",
            RUNTIME_CLASS,
            "-o",
            "jsonpath={.handler}",
        ];
        let handler = eventually(READY_TIMEOUT, "the RuntimeClass", || {
            self.node.try_kubectl(&handler, COMMAND_TIMEOUT)
        });
        assert_eq!(handler, RUNTIME_CLASS, "the handler of the RuntimeClass");
        let info = self.node.sh("cat /etc/floci-node/build-info");
        step(&format!("node build: {}", info.trim().replace('\n', ", ")));
        let thp = self
            .node
            .sh("cat /sys/kernel/mm/transparent_hugepage/enabled");
        assert!(
            thp.contains("[always]"),
            "the node kernel must have transparent huge pages with the policy always \
             (node/kernel/README.md), not {thp:?}"
        );
        self.report_thp("before the test");
        let shim = self
            .node
            .sh(&format!("sha256sum {NODE_SHIM} | cut -d ' ' -f 1"));
        let expected = expect(shim_under_test());
        assert_eq!(
            shim.trim(),
            expected,
            "the node's shim is not the build under test: rebuild the node image \
             with node-vm.sh build, or set SMOLVM_PODS_E2E_SHIM_SHA256"
        );
    }

    /// Builds the test image for each architecture and imports it into the
    /// node's containerd, without a registry.
    fn import_images(&self) {
        step("building the test image");
        let out = self.work.join("images");
        std::fs::create_dir_all(&out).expect("create the scratch directory");
        let out = out.canonicalize().expect("resolve the scratch directory");
        for (arch, archive) in expect(images::build(&self.prefix, &self.id, &out)) {
            self.import_image(&archive, arch, &self.image(arch));
        }
    }

    /// Imports the image archive at `archive` on this host, saved by Docker
    /// for `arch`, into the node's containerd, and waits until the CRI lists
    /// it as `image`.
    #[track_caller]
    pub fn import_image(&self, archive: &Path, arch: Arch, image: &str) {
        self.node.sh(&format!("mkdir -p {}", self.node_dir));
        let name = archive
            .file_name()
            .expect("an archive file")
            .to_string_lossy();
        let dst = format!("{}/{name}", self.node_dir);
        self.node.copy_in(archive, &dst);
        let platform = format!("linux/{}", arch.name());
        self.node.sh(&format!(
            "ctr -n k8s.io images import --platform {platform} {dst}"
        ));
        let inspect = format!("crictl inspecti {image} >/dev/null");
        eventually(READY_TIMEOUT, "the CRI to list the image", || {
            self.node.try_sh(&inspect)
        });
    }

    /// The test's scratch directory on this host, which teardown removes.
    pub fn work_dir(&self) -> &Path {
        &self.work
    }

    /// The binary `name` of the workloads, built for `arch` on this host.
    pub fn workload(&self, arch: Arch, name: &str) -> PathBuf {
        self.work.join("images").join(arch.name()).join(name)
    }

    /// The test image for `arch` in the node.
    pub fn image(&self, arch: Arch) -> String {
        format!("docker.io/{}", images::tag(&self.prefix, &self.id, arch))
    }

    /// The consumers' image in the node, once
    /// [`Cluster::import_consumer_image`] imported it.
    pub fn consumer_image(&self) -> String {
        format!("docker.io/{}", images::consumer_tag(&self.prefix, &self.id))
    }

    /// Builds the consumers' image of `workloads/consumer` for arm64 and
    /// imports it into the node as [`Cluster::consumer_image`]. Teardown
    /// removes it.
    #[track_caller]
    pub fn import_consumer_image(&self) {
        step("building the consumers' image");
        let tag = images::consumer_tag(&self.prefix, &self.id);
        let out = self.work.join("images");
        std::fs::create_dir_all(&out).expect("create the scratch directory");
        let archive = out.join("consumer.tar");
        expect(images::build_consumer(&tag, &archive));
        self.import_image(&archive, Arch::Arm64, &self.consumer_image());
    }

    /// The path of `file` in the test's scratch directory in the node.
    pub fn node_path(&self, file: &str) -> String {
        format!("{}/{file}", self.node_dir)
    }

    /// A NodePort that the node publishes on the same port of the Mac's
    /// loopback.
    pub fn node_port(&self) -> u16 {
        self.node_port
    }

    /// The port of the Mac's loopback on which the node publishes the node
    /// service's API, when the setup asked for one.
    pub fn service_port(&self) -> Option<u16> {
        self.service_port
    }

    /// Writes `yaml` to `file` in the test's scratch directory in the node,
    /// and applies it.
    #[track_caller]
    pub fn apply(&self, file: &str, yaml: &str) {
        let path = self.node_path(file);
        self.node.write(&path, yaml);
        self.node.kubectl_ok(&["apply", "-f", &path]);
    }

    /// Creates the pods, without waiting for them.
    pub fn create_pods(&self, specs: &[PodSpec]) {
        let manifests: Vec<String> = specs
            .iter()
            .map(|spec| spec.manifest(&self.namespace, &self.image(spec.arch)))
            .collect();
        self.apply("pods.yaml", &manifests.join("---\n"));
    }

    /// The ClusterIP of a Service in the test's namespace.
    #[track_caller]
    pub fn service_ip(&self, name: &str) -> String {
        let args = [
            "get",
            "service",
            name,
            "-n",
            &self.namespace,
            "-o",
            "jsonpath={.spec.clusterIP}",
        ];
        self.node.kubectl_ok(&args)
    }

    /// Deletes a pod and waits until it is gone.
    #[track_caller]
    pub fn delete_pod(&self, name: &str) {
        let delete = [
            "delete",
            "pod",
            name,
            "-n",
            &self.namespace,
            "--ignore-not-found",
            "--timeout=120s",
        ];
        self.node.kubectl_ok(&delete);
    }

    /// The messages of the events about a pod.
    #[track_caller]
    pub fn events(&self, pod: &str) -> String {
        let selector = format!("involvedObject.name={pod}");
        let events = [
            "get",
            "events",
            "-n",
            &self.namespace,
            "--field-selector",
            &selector,
            "-o",
            "jsonpath={.items[*].message}",
        ];
        self.node.kubectl_ok(&events)
    }

    /// Creates the pods and waits until all are Ready.
    pub fn run_pods(&self, specs: &[PodSpec]) -> Vec<Pod<'_>> {
        step("creating the test pods");
        self.create_pods(specs);

        let ns = self.namespace.as_str();
        let timeout = format!("--timeout={}s", READY_TIMEOUT.as_secs());
        let pods: Vec<String> = specs
            .iter()
            .map(|spec| format!("pod/{}", spec.name))
            .collect();
        let mut wait = vec!["wait", "-n", ns, "--for=condition=Ready", &timeout];
        wait.extend(pods.iter().map(String::as_str));
        if let Err(e) = self
            .node
            .try_kubectl(&wait, READY_TIMEOUT + COMMAND_TIMEOUT)
        {
            let details = self
                .node
                .try_kubectl(&["describe", "pods", "-n", ns], COMMAND_TIMEOUT);
            panic!(
                "the pods did not become Ready: {e}\n{}",
                details.unwrap_or_else(|e| e)
            );
        }
        specs.iter().map(|spec| self.ready_pod(spec.name)).collect()
    }

    /// Returns a Ready pod, with the id of its CRI sandbox.
    #[track_caller]
    pub fn ready_pod(&self, name: &'static str) -> Pod<'_> {
        let sandbox = expect(self.ready_sandbox(name));
        step(&format!("pod {name} is Ready in sandbox {sandbox}"));
        Pod {
            cluster: self,
            name,
            sandbox,
        }
    }

    /// The id of the pod's one ready CRI sandbox.
    pub fn ready_sandbox(&self, name: &str) -> Result<String, String> {
        let sandboxes = self.node.try_sh(&format!(
            "crictl pods --state ready -q --label io.kubernetes.pod.namespace={} \
             --label io.kubernetes.pod.name={name}",
            self.namespace
        ))?;
        let sandboxes: Vec<&str> = sandboxes.split_whitespace().collect();
        match sandboxes[..] {
            [sandbox] => Ok(sandbox.to_string()),
            _ => Err(format!(
                "pod {name} should have one ready sandbox, has {sandboxes:?}"
            )),
        }
    }

    /// Lists the VM processes on the node.
    pub fn vm_processes(&self) -> Vec<VmProcess> {
        let lines = self.node.sh(VM_PROCESSES);
        lines.lines().filter_map(VmProcess::parse).collect()
    }

    /// Returns those of `paths` that exist in the node.
    pub fn existing(&self, paths: &[&str]) -> Vec<String> {
        let script = format!(
            "for p in {}; do if [ -e \"$p\" ]; then echo \"$p\"; fi; done",
            paths.join(" ")
        );
        let existing = self.node.sh(&script);
        existing.lines().map(str::to_string).collect()
    }

    /// Returns what identifies the node's kernel.
    pub fn node_kernel(&self) -> Kernel {
        let script = "cat /proc/sys/kernel/random/boot_id /proc/cmdline; uname -m";
        let output = self.node.sh(script);
        let mut lines = output.lines().map(|line| line.trim().to_string());
        let mut next = || lines.next().unwrap_or_default();
        Kernel {
            boot_id: next(),
            cmdline: next(),
            machine: next(),
        }
    }

    /// Prints the node's THP counters and khugepaged settings.
    fn report_thp(&self, when: &str) {
        match self.node.try_sh(THP_STATE) {
            Ok(state) => step(&format!(
                "node THP {when}: {}",
                state.trim().replace('\n', ", ")
            )),
            Err(e) => step(&format!("node THP {when}: unknown: {e}")),
        }
    }

    fn teardown(&self) -> Vec<String> {
        self.report_thp("after the test");
        let mut errors = Vec::new();
        let mut images = Arch::ALL.map(|arch| self.image(arch)).to_vec();
        images.push(self.consumer_image());
        if self.keep && self.owns_node {
            step(&format!(
                "kept namespace {} in node {}; delete the node with: SMOLVM_PODS_NODE={} {} delete",
                self.namespace,
                self.node.name(),
                self.node.name(),
                Node::script_path().display()
            ));
        } else if self.keep {
            step(&format!(
                "kept namespace {ns} in node {node}; remove what the test added with: \
                 SMOLVM_PODS_NODE={node} {script} kubectl delete namespace {ns}; \
                 SMOLVM_PODS_NODE={node} {script} exec ctr -n k8s.io images rm {images}; \
                 SMOLVM_PODS_NODE={node} {script} exec rm -rf {dir}",
                ns = self.namespace,
                node = self.node.name(),
                script = Node::script_path().display(),
                images = images.join(" "),
                dir = self.node_dir,
            ));
        } else if self.owns_node {
            step(&format!("deleting node VM {}", self.node.name()));
            errors.extend(self.node.try_script(&["delete"], START_TIMEOUT).err());
        } else {
            let namespace = ["delete", "namespace", &self.namespace, "--ignore-not-found"];
            errors.extend(self.node.try_kubectl(&namespace, READY_TIMEOUT).err());
            let cleanup = format!(
                "ctr -n k8s.io images rm {} >/dev/null 2>&1; rm -rf {}; \
                 ! ctr -n k8s.io images ls -q | grep -F -e {}",
                images.join(" "),
                self.node_dir,
                images.join(" -e ")
            );
            errors.extend(self.node.try_sh(&cleanup).err());
        }
        errors.extend(images::remove(&self.prefix, &self.id).err());
        let consumer = images::consumer_tag(&self.prefix, &self.id);
        errors.extend(images::remove_tag(&consumer).err());
        if let Err(e) = std::fs::remove_dir_all(&self.work) {
            if e.kind() != std::io::ErrorKind::NotFound {
                errors.push(format!("remove {}: {e}", self.work.display()));
            }
        }
        errors
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let errors = self.teardown();
        if errors.is_empty() {
            return;
        }
        let message = format!("teardown failed:\n{}", errors.join("\n"));
        if thread::panicking() {
            eprintln!("{message}");
        } else {
            panic!("{message}");
        }
    }
}

/// The SHA-256 of the shim under test: SMOLVM_PODS_E2E_SHIM_SHA256, or that of
/// the file SMOLVM_PODS_SHIM (default: the release-small build), which
/// node-vm.sh build also takes.
fn shim_under_test() -> Result<String, String> {
    if let Ok(sha) = env::var("SMOLVM_PODS_E2E_SHIM_SHA256") {
        return Ok(sha);
    }
    let shim = env::var_os("SMOLVM_PODS_SHIM")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let pods = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
            pods.join("target/release-small/containerd-shim-smolvm-pods-v2")
        });
    let sum = cmd::run_ok(
        Command::new("shasum").args(["-a", "256"]).arg(&shim),
        COMMAND_TIMEOUT,
    )?;
    let sha = sum.split_whitespace().next().unwrap_or_default();
    Ok(sha.to_string())
}

/// The NodePort that a running node publishes on the Mac, for the tests that
/// need one: SMOLVM_PODS_E2E_NODE_PORT, or the first that node-vm.sh create
/// publishes by default.
fn published_node_port() -> u16 {
    match env::var("SMOLVM_PODS_E2E_NODE_PORT") {
        Ok(port) => port
            .parse()
            .unwrap_or_else(|e| panic!("SMOLVM_PODS_E2E_NODE_PORT={port:?}: {e}")),
        Err(_) => 32080,
    }
}

/// Returns a port in `ports` that nothing on this host listens on.
pub(crate) fn free_port(ports: RangeInclusive<u16>) -> u16 {
    let count = u64::from(ports.end() - ports.start()) + 1;
    let offset = RandomState::new().hash_one(0) % count;
    (0..count)
        .map(|i| ports.start() + ((offset + i) % count) as u16)
        .find(|&port| {
            ["127.0.0.1", "0.0.0.0"]
                .into_iter()
                .all(|ip| TcpListener::bind((ip, port)).is_ok())
        })
        .expect("a free port")
}
