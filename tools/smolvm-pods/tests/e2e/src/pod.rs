//! Test pods. Every container runs the test app from the test image.

use std::collections::HashMap;
use std::ops::Index;
use std::time::Duration;

use crate::cmd::{describe, Output};
use crate::{Cluster, RUNTIME_CLASS};

/// How long a kubectl exec may take. The litmus test is the slowest command.
const EXEC_TIMEOUT: Duration = Duration::from_secs(300);

/// How long the test app's GET may wait for an answer. An amd64 app's first
/// answers run under Rosetta in a nested VM, which can take seconds.
const GET_TIMEOUT: &str = "30s";

/// A CPU architecture of the test image.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Arch {
    Arm64,
    Amd64,
}

impl Arch {
    pub const ALL: [Arch; 2] = [Arch::Arm64, Arch::Amd64];

    pub fn name(self) -> &'static str {
        match self {
            Arch::Arm64 => "arm64",
            Arch::Amd64 => "amd64",
        }
    }
}

/// What runs a pod.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Runtime {
    /// A VM, with the RuntimeClass under test.
    SmolvmPods,
    /// The node's default runtime, runc.
    Runc,
}

/// What each container of a pod runs, from the test image.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum App {
    /// `testapp serve NAME :PORT`, ready once the app answers inside the
    /// container.
    Serve,
    /// `testapp hold -mib MIB NAME :PORT`, the app of the hibernation test,
    /// with no probe: an exec probe would wake a sleeping pod, and an HTTP
    /// or TCP probe would fail while it sleeps.
    Hold { mib: u32 },
}

/// The idle timeout of the VM pods of the tests whose checks expect a pod to
/// stay awake until the test puts it to sleep. An idle pod otherwise sleeps
/// by itself after 5 minutes.
pub const NO_IDLE_SLEEP: (&str, &str) = (crate::wake::IDLE_TIMEOUT, "24h");

/// A pod to run, with `app` in each container from the test image for `arch`.
/// The pod has the label `app: NAME`, for Services. A VM pod gets
/// [`NO_IDLE_SLEEP`] unless its annotations set an idle timeout.
pub struct PodSpec {
    pub name: &'static str,
    pub arch: Arch,
    pub runtime: Runtime,
    pub app: App,
    /// Names and ports of the containers.
    pub containers: &'static [(&'static str, u16)],
    pub annotations: &'static [(&'static str, &'static str)],
}

impl PodSpec {
    /// The pod's manifest, with `image` as the test image. It mounts no
    /// service account token, which the test app does not use.
    pub fn manifest(&self, namespace: &str, image: &str) -> String {
        let mut annotations = self.annotations.to_vec();
        let idle = annotations.iter().any(|(key, _)| *key == NO_IDLE_SLEEP.0);
        if self.runtime == Runtime::SmolvmPods && !idle {
            annotations.push(NO_IDLE_SLEEP);
        }
        let annotations: Vec<String> = annotations
            .iter()
            .map(|(key, value)| format!("{key:?}: {value:?}"))
            .collect();
        let runtime_class = match self.runtime {
            Runtime::SmolvmPods => format!("  runtimeClassName: {RUNTIME_CLASS}\n"),
            Runtime::Runc => String::new(),
        };
        let mut yaml = format!(
            r#"apiVersion: v1
kind: Pod
metadata:
  name: {name}
  namespace: {namespace}
  labels: {{app: {name}}}
  annotations: {{{annotations}}}
spec:
{runtime_class}  automountServiceAccountToken: false
  terminationGracePeriodSeconds: 5
  containers:
"#,
            name = self.name,
            annotations = annotations.join(", "),
        );
        for (container, port) in self.containers {
            yaml += &match self.app {
                App::Serve => format!(
                    r#"  - name: {container}
    image: {image}
    imagePullPolicy: Never
    args: [serve, {container}, ":{port}"]
    readinessProbe:
      exec: {{command: [/testapp, get, "http://127.0.0.1:{port}/healthz"]}}
      periodSeconds: 5
      timeoutSeconds: 10
"#
                ),
                App::Hold { mib } => format!(
                    r#"  - name: {container}
    image: {image}
    imagePullPolicy: Never
    args: [hold, -mib, "{mib}", {container}, ":{port}"]
"#
                ),
            };
        }
        yaml
    }
}

/// A pod that became Ready.
pub struct Pod<'a> {
    pub(crate) cluster: &'a Cluster,
    pub name: &'static str,
    /// The CRI sandbox id, which names the pod's VM.
    pub sandbox: String,
}

impl Pod<'_> {
    /// Runs `args` in `container` through kubectl exec, whatever their exit
    /// status.
    #[track_caller]
    pub fn exec(&self, container: &str, args: &[&str]) -> Output {
        let base = [
            "exec",
            "-n",
            self.namespace(),
            self.name,
            "-c",
            container,
            "--",
        ];
        self.cluster
            .node
            .kubectl(&[&base[..], args].concat(), EXEC_TIMEOUT)
    }

    /// Runs `args` in `container` through kubectl exec -i, with `input` as
    /// stdin, whatever their exit status.
    #[track_caller]
    pub fn exec_with_input(&self, container: &str, args: &[&str], input: &[u8]) -> Output {
        let base = [
            "exec",
            "-i",
            "-n",
            self.namespace(),
            self.name,
            "-c",
            container,
            "--",
        ];
        let args = [&base[..], args].concat();
        self.cluster
            .node
            .kubectl_with_input(&args, input, EXEC_TIMEOUT)
    }

    /// Asks the test app at `url` for its state, from inside `container`.
    #[track_caller]
    pub fn get(&self, container: &str, url: &str) -> State {
        match self.try_get(container, url) {
            Ok(state) => state,
            Err(e) => panic!("{e}"),
        }
    }

    /// Like [`Pod::get`], but returns an error when the GET fails.
    pub fn try_get(&self, container: &str, url: &str) -> Result<State, String> {
        let output = self.exec(
            container,
            &["/testapp", "get", "-timeout", GET_TIMEOUT, url],
        );
        if !output.success() {
            return Err(format!(
                "{}/{container}: GET {url}: {}",
                self.name,
                describe(&output)
            ));
        }
        Ok(State::parse(&output.stdout))
    }

    /// The pod's IP address.
    #[track_caller]
    pub fn ip(&self) -> String {
        self.field("{.status.podIP}")
    }

    /// Returns the output of `container` as kubectl logs shows it.
    #[track_caller]
    pub fn logs(&self, container: &str) -> String {
        let args = ["logs", "-n", self.namespace(), self.name, "-c", container];
        self.cluster.node.kubectl_ok(&args)
    }

    /// Returns a field of the pod object, selected by a kubectl JSONPath.
    #[track_caller]
    pub fn field(&self, path: &str) -> String {
        let jsonpath = format!("jsonpath={path}");
        let args = [
            "get",
            "pod",
            "-n",
            self.namespace(),
            self.name,
            "-o",
            &jsonpath,
        ];
        self.cluster.node.kubectl_ok(&args)
    }

    /// Returns the VM processes on the node that run this pod's sandbox.
    pub fn vm_processes(&self) -> Vec<VmProcess> {
        let processes = self.cluster.vm_processes();
        processes
            .into_iter()
            .filter(|vm| vm.machine.contains(&self.sandbox))
            .collect()
    }

    fn namespace(&self) -> &str {
        &self.cluster.namespace
    }
}

/// A VM process on the node. It runs `EXE _boot-vm DIR/boot-config.json`,
/// and smolvm records the machine's name in `DIR/name`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VmProcess {
    pub pid: u32,
    pub dir: String,
    pub machine: String,
}

impl VmProcess {
    /// Parses a line of `PID DIR MACHINE`.
    pub(crate) fn parse(line: &str) -> Option<VmProcess> {
        let mut fields = line.splitn(3, ' ');
        Some(VmProcess {
            pid: fields.next()?.parse().ok()?,
            dir: fields.next()?.to_string(),
            machine: fields.next().unwrap_or_default().trim().to_string(),
        })
    }
}

/// The state that the test app reports, one `key=value` pair per line.
#[derive(Debug)]
pub struct State(HashMap<String, String>);

impl State {
    pub fn parse(text: &str) -> State {
        let pairs = text.lines().filter_map(|line| line.split_once('='));
        State(pairs.map(|(k, v)| (k.to_string(), v.to_string())).collect())
    }
}

impl Index<&str> for State {
    type Output = str;

    #[track_caller]
    fn index(&self, key: &str) -> &str {
        match self.0.get(key) {
            Some(value) => value,
            None => panic!("the app's state has no {key}: {self:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_a_manifest() {
        let spec = PodSpec {
            name: "pod",
            arch: Arch::Arm64,
            runtime: Runtime::SmolvmPods,
            app: App::Serve,
            containers: &[("app", 8080), ("sidecar", 8081)],
            annotations: &[("example.io/flag", "true")],
        };
        let expected = r#"apiVersion: v1
kind: Pod
metadata:
  name: pod
  namespace: ns
  labels: {app: pod}
  annotations: {"example.io/flag": "true", "smolvm-pods.floci.io/idle-timeout": "24h"}
spec:
  runtimeClassName: smolvm-pods
  automountServiceAccountToken: false
  terminationGracePeriodSeconds: 5
  containers:
  - name: app
    image: docker.io/test:1
    imagePullPolicy: Never
    args: [serve, app, ":8080"]
    readinessProbe:
      exec: {command: [/testapp, get, "http://127.0.0.1:8080/healthz"]}
      periodSeconds: 5
      timeoutSeconds: 10
  - name: sidecar
    image: docker.io/test:1
    imagePullPolicy: Never
    args: [serve, sidecar, ":8081"]
    readinessProbe:
      exec: {command: [/testapp, get, "http://127.0.0.1:8081/healthz"]}
      periodSeconds: 5
      timeoutSeconds: 10
"#;
        assert_eq!(spec.manifest("ns", "docker.io/test:1"), expected);
    }

    #[test]
    fn a_runc_pod_has_no_runtime_class() {
        let spec = PodSpec {
            name: "runc",
            arch: Arch::Arm64,
            runtime: Runtime::Runc,
            app: App::Serve,
            containers: &[("app", 8080)],
            annotations: &[],
        };
        let manifest = spec.manifest("ns", "docker.io/test:1");
        let head =
            "  labels: {app: runc}\n  annotations: {}\nspec:\n  automountServiceAccountToken";
        assert!(manifest.contains(head), "{manifest}");
        assert!(!manifest.contains("runtimeClassName"), "{manifest}");
    }

    #[test]
    fn a_hold_pod_has_no_probe() {
        let spec = PodSpec {
            name: "hold",
            arch: Arch::Amd64,
            runtime: Runtime::SmolvmPods,
            app: App::Hold { mib: 256 },
            containers: &[("app", 8080)],
            annotations: &[],
        };
        let manifest = spec.manifest("ns", "docker.io/test:1");
        let container = r#"  containers:
  - name: app
    image: docker.io/test:1
    imagePullPolicy: Never
    args: [hold, -mib, "256", app, ":8080"]
"#;
        assert!(manifest.ends_with(container), "{manifest}");
        assert!(!manifest.contains("Probe"), "{manifest}");
    }

    #[test]
    fn parses_state() {
        let state = State::parse("name=app\ncmdline=a=b c\nrequest=3\n");
        assert_eq!(&state["name"], "app");
        assert_eq!(&state["cmdline"], "a=b c");
        assert_eq!(&state["request"], "3");
    }

    #[test]
    fn parses_vm_processes() {
        let vm = VmProcess::parse("42 /root/.cache/smolvm/vms/5f25a50f0b46078f abc123");
        assert_eq!(
            vm,
            Some(VmProcess {
                pid: 42,
                dir: "/root/.cache/smolvm/vms/5f25a50f0b46078f".into(),
                machine: "abc123".into(),
            })
        );
        assert_eq!(VmProcess::parse("x y z"), None);
    }
}
