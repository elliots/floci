//! Docker on a smolvm-pods node: dockerd runs on the node's containerd with
//! runc by default, and a container that names the runtime
//! `io.containerd.smolvm-pods.v2` runs in a pod VM of its own, which sleeps
//! only with the annotation `smolvm-pods.floci.io/hibernate: "true"`. The
//! node service's API reaches dockerd and the containers from the Mac.

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use smolvm_pods_e2e::json::Json;
use smolvm_pods_e2e::runtime::CTL;
use smolvm_pods_e2e::{
    eventually, run_checks, step, Arch, Cluster, Node, NodeSetup, COMMAND_TIMEOUT,
};

/// The runtime that puts a container in a pod VM.
const RUNTIME: &str = "io.containerd.smolvm-pods.v2";

/// The idle timeout of the VM containers, set with an annotation.
const IDLE_TIMEOUT: Duration = Duration::from_secs(20);

/// The node service reads conntrack this often.
const POLL_INTERVAL: Duration = Duration::from_secs(15);

/// The node port that the awake VM container publishes with `-p`.
const PUBLISHED_PORT: u16 = 18080;

const APP_PORT: u16 = 8080;

/// What each container runs: the test app holding memory, whose hash shows
/// that the memory survives a sleep.
const HOLD_MIB: &str = "64";

#[test]
#[ignore = "needs a Mac with smolvm and Docker: SMOLVM_PODS_E2E=1 cargo test -- --ignored"]
fn docker() {
    let cluster = Cluster::start_with(
        "docker",
        NodeSetup {
            service_port: true,
            ..NodeSetup::default()
        },
    );
    let node = &cluster.node;
    let image = cluster.image(Arch::Arm64);
    step("loading the test image into dockerd");
    eventually(Duration::from_secs(120), "dockerd to answer", || {
        node.try_sh("docker info --format '{{.ServerVersion}}'")
    });
    node.sh(&format!(
        "docker load -i {} >/dev/null",
        cluster.node_path("arm64.tar")
    ));
    let containers = Containers {
        node,
        names: ["runc", "awake", "sleepy"].map(|role| format!("{}-{role}", cluster.namespace)),
        image: image.clone(),
    };
    let [runc, awake, sleepy] = &containers.names;
    let idle = format!(
        "smolvm-pods.floci.io/idle-timeout={}s",
        IDLE_TIMEOUT.as_secs()
    );
    containers.run(runc, &[], "runc");
    containers.run(
        awake,
        &[
            "--runtime",
            RUNTIME,
            "--annotation",
            &idle,
            "-p",
            &format!("{PUBLISHED_PORT}:{APP_PORT}"),
        ],
        "awake",
    );
    containers.run(
        sleepy,
        &[
            "--runtime",
            RUNTIME,
            "--annotation",
            "smolvm-pods.floci.io/hibernate=true",
            "--annotation",
            &idle,
        ],
        "sleepy",
    );
    let quiet_since = OnceLock::new();
    for name in &containers.names {
        eventually(Duration::from_secs(120), "the app to answer", || {
            containers.get(name, "/ping")
        });
    }
    let hash = containers.get(sleepy, "/hash").expect("the hash of sleepy");
    let id = containers
        .get(sleepy, "/id")
        .expect("the identity of sleepy");

    run_checks(&[
        ("own VMs", &|| own_vms(&containers)),
        ("runc by default", &|| {
            assert_eq!(containers.runtime(runc), "runc", "the runtime of {runc}")
        }),
        ("exec", &|| exec(&containers, awake)),
        ("logs", &|| logs(&containers, awake)),
        ("published port", &|| published_port(node)),
        ("stats", &|| stats(&containers, awake)),
        // The checks before touch the awake container; from here on nothing
        // does until it is checked.
        ("sleeps by itself", &|| {
            quiet_since.get_or_init(Instant::now);
            sleeps(&containers, sleepy)
        }),
        ("stays awake without the annotation", &|| {
            stays_awake(&containers, awake, *quiet_since.get_or_init(Instant::now))
        }),
        ("wakes on traffic", &|| {
            wakes(&containers, sleepy, &hash, &id)
        }),
        ("through the API", &|| {
            through_the_api(&cluster, &containers, sleepy, &hash)
        }),
        ("runc beside", &|| {
            containers
                .get(runc, "/ping")
                .expect("the runc container answers");
            assert!(containers.up(runc), "{runc} is not up");
        }),
        ("volumes in place", &|| {
            volumes(&containers, &format!("{}-data", cluster.namespace))
        }),
        ("stop, start and rm", &|| stop_start_rm(&containers, sleepy)),
    ]);
}

/// The test's containers in the node's dockerd, removed on drop.
struct Containers<'a> {
    node: &'a Node,
    names: [String; 3],
    image: String,
}

impl Containers<'_> {
    fn docker(&self, args: &[&str]) -> Result<String, String> {
        let args = [&["docker"], args].concat();
        let output = self.node.exec(&args, COMMAND_TIMEOUT);
        if output.success() {
            Ok(output.stdout)
        } else {
            Err(smolvm_pods_e2e::describe(&output))
        }
    }

    #[track_caller]
    fn docker_ok(&self, args: &[&str]) -> String {
        self.docker(args)
            .unwrap_or_else(|e| panic!("docker {}: {e}", args.join(" ")))
    }

    #[track_caller]
    fn run(&self, name: &str, options: &[&str], app: &str) {
        step(&format!("docker run {name} {}", options.join(" ")));
        let port = format!(":{APP_PORT}");
        let args = [
            &["run", "-d", "--name", name][..],
            options,
            &[&self.image, "hold", "-mib", HOLD_MIB, app, &port],
        ]
        .concat();
        self.docker_ok(&args);
    }

    #[track_caller]
    fn inspect(&self, name: &str, format: &str) -> String {
        self.docker_ok(&["inspect", "-f", format, name])
            .trim()
            .to_string()
    }

    fn id(&self, name: &str) -> String {
        self.inspect(name, "{{.Id}}")
    }

    fn ip(&self, name: &str) -> String {
        self.inspect(name, "{{.NetworkSettings.IPAddress}}")
    }

    fn runtime(&self, name: &str) -> String {
        self.inspect(name, "{{.HostConfig.Runtime}}")
    }

    fn up(&self, name: &str) -> bool {
        self.inspect(name, "{{.State.Running}}") == "true"
    }

    /// GET `path` of the app at the container's Docker IP, from the node.
    fn get(&self, name: &str, path: &str) -> Result<String, String> {
        let url = format!("http://{}:{APP_PORT}{path}", self.ip(name));
        self.node.try_sh(&format!("curl -sf -m 10 {url}"))
    }

    /// The node service's record of the container's pod.
    fn pod(&self, name: &str) -> Result<Json, String> {
        let id = self.id(name);
        let args = [&CTL[..], &["get", &id]].concat();
        let output = self.node.exec(&args, COMMAND_TIMEOUT);
        if !output.success() {
            return Err(smolvm_pods_e2e::describe(&output));
        }
        Json::parse(&output.stdout)
    }

    #[track_caller]
    fn state(&self, name: &str) -> String {
        let pod = self
            .pod(name)
            .unwrap_or_else(|e| panic!("the pod of {name}: {e}"));
        pod.get("state").as_str().unwrap_or_default().to_string()
    }

    /// The boot id of the kernel that the container's app runs on.
    #[track_caller]
    fn boot_id(&self, name: &str) -> String {
        let id = self
            .get(name, "/id")
            .unwrap_or_else(|e| panic!("the identity of {name}: {e}"));
        id.lines()
            .find_map(|l| l.strip_prefix("boot_id="))
            .unwrap_or_default()
            .to_string()
    }
}

impl Drop for Containers<'_> {
    fn drop(&mut self) {
        let mut rm = vec!["rm", "-f"];
        rm.extend(self.names.iter().map(String::as_str));
        if let Err(e) = self.docker(&rm) {
            eprintln!("remove the test's containers: {e}");
        }
        if let Err(e) = self.docker(&["image", "rm", &self.image]) {
            eprintln!("remove the test image from dockerd: {e}");
        }
    }
}

/// Each VM container is a standalone pod that is awake, at its Docker IP, in a
/// kernel of its own; the runc container is no pod and shares the node's.
fn own_vms(c: &Containers) {
    let [runc, awake, sleepy] = &c.names;
    let node_boot = c.node.sh("cat /proc/sys/kernel/random/boot_id");
    let node_boot = node_boot.trim();
    for name in [awake, sleepy] {
        assert_eq!(c.runtime(name), RUNTIME, "the runtime of {name}");
        let pod = c
            .pod(name)
            .unwrap_or_else(|e| panic!("the pod of {name}: {e}"));
        assert_eq!(pod.get("state").as_str(), Some("awake"), "{name}: {pod:?}");
        assert_eq!(pod.get("standalone").as_bool(), Some(true), "{name}");
        assert_eq!(pod.get("ip").as_str(), Some(c.ip(name).as_str()), "{name}");
        assert_ne!(
            c.boot_id(name),
            node_boot,
            "{name} runs in the node's kernel"
        );
    }
    assert_eq!(
        c.pod(awake).unwrap().get("hibernate").as_bool(),
        Some(false),
        "{awake} without the annotation"
    );
    assert_eq!(
        c.pod(sleepy).unwrap().get("hibernate").as_bool(),
        Some(true),
        "{sleepy} with the annotation"
    );
    assert_ne!(
        c.boot_id(awake),
        c.boot_id(sleepy),
        "two VMs share a kernel"
    );
    assert_eq!(c.boot_id(runc), node_boot, "the runc container's kernel");
    assert!(c.pod(runc).is_err(), "the runc container has a pod record");
}

/// `docker exec` returns the output and the exit code of a process in the VM.
fn exec(c: &Containers, name: &str) {
    let echo = c.docker_ok(&["exec", name, "/testapp", "echo", "hello", "from", "exec"]);
    assert_eq!(echo.trim(), "hello from exec");
    let args = ["docker", "exec", name, "/testapp", "exit", "3"];
    let exit = c.node.exec(&args, COMMAND_TIMEOUT);
    assert_eq!(exit.code(), Some(3), "{}", smolvm_pods_e2e::describe(&exit));
    assert!(
        exit.stdout.contains("stdout before exit 3"),
        "{}",
        exit.stdout
    );
    assert!(
        exit.stderr.contains("stderr before exit 3"),
        "{}",
        exit.stderr
    );
    let iface = c.docker_ok(&["exec", name, "/testapp", "iface", "eth0"]);
    let ip = c.ip(name);
    assert!(
        iface.contains(&format!("ipv4={ip}/")),
        "eth0 in the VM: {iface}"
    );
}

/// `docker logs` shows the app's counter lines.
fn logs(c: &Containers, name: &str) {
    let logs = c.docker_ok(&["logs", "--tail", "5", name]);
    assert!(logs.contains("tick "), "the logs of {name}: {logs}");
}

/// A port published with `-p` reaches the VM from the node.
fn published_port(node: &Node) {
    let ping = node.sh(&format!(
        "curl -sf -m 10 http://127.0.0.1:{PUBLISHED_PORT}/ping"
    ));
    assert_eq!(ping.trim(), "ok");
}

/// `docker stats` reports the VM container's memory, and dockerd still answers.
fn stats(c: &Containers, name: &str) {
    let memory = c.docker_ok(&["stats", "--no-stream", "--format", "{{.MemUsage}}", name]);
    assert!(
        !memory.trim().starts_with("0B"),
        "the memory of {name}: {memory}"
    );
    c.docker_ok(&["info", "--format", "{{.ServerVersion}}"]);
}

/// The container with the hibernate annotation sleeps by itself once idle:
/// its VM process is gone, and Docker still shows it running.
fn sleeps(c: &Containers, name: &str) {
    let limit = IDLE_TIMEOUT + POLL_INTERVAL * 4;
    eventually(limit, "the container to sleep by itself", || {
        match c.state(name).as_str() {
            "asleep" => Ok(()),
            other => Err(format!("{name} is {other}")),
        }
    });
    let pod = c.pod(name).unwrap();
    assert!(
        pod.get("vm_pid").is_null(),
        "a sleeping VM has a process: {pod:?}"
    );
    assert!(c.up(name), "Docker sees {name} stopped while it sleeps");
}

/// The VM container without the annotation is still awake well after its
/// idle timeout with no traffic, and the API does not put it to sleep.
fn stays_awake(c: &Containers, name: &str, quiet_since: Instant) {
    let quiet = IDLE_TIMEOUT + POLL_INTERVAL * 2;
    if let Some(wait) = quiet.checked_sub(quiet_since.elapsed()) {
        std::thread::sleep(wait);
    }
    assert_eq!(
        c.state(name),
        "awake",
        "{name} slept without the annotation"
    );
    let id = c.id(name);
    let args = [&CTL[..], &["hibernate", &id]].concat();
    let refused = c.node.exec(&args, COMMAND_TIMEOUT);
    assert!(
        !refused.success(),
        "an API hibernate of {name} was not refused"
    );
    assert_eq!(c.state(name), "awake");
}

/// A request to the sleeping container's IP wakes it, and its memory and
/// identity are those it had before.
fn wakes(c: &Containers, name: &str, hash: &str, id: &str) {
    let start = Instant::now();
    let after = c.get(name, "/hash").expect("the hash after the wake");
    step(&format!("{name} answered after {:?}", start.elapsed()));
    let sum = |text: &str| {
        text.lines()
            .find(|l| l.starts_with("sha256="))
            .unwrap_or_default()
            .to_string()
    };
    assert_eq!(
        sum(&after),
        sum(hash),
        "the memory of {name} after its wake"
    );
    assert_eq!(c.get(name, "/id").expect("the identity"), id);
    assert_eq!(c.state(name), "awake");
    let pod = c.pod(name).unwrap();
    assert!(
        !pod.get("last_wake_ms").is_null(),
        "no wake recorded: {pod:?}"
    );
}

/// The node service's API on the Mac: its Docker proxy shows the container as
/// dockerd does, and a tunnel to the sleeping container's IP wakes it and
/// carries a request to its app, whose memory is what it was.
fn through_the_api(cluster: &Cluster, c: &Containers, name: &str, hash: &str) {
    let port = cluster.service_port().expect("the node publishes its API");
    let api = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let token = c.node.script(&["token"], COMMAND_TIMEOUT);
    let token = token.trim();
    let id = c.id(name);

    let path = format!("/docker/v1.47/containers/{id}/json");
    let (status, body) = api_get(api, token, &path);
    assert_eq!(status, 200, "GET {path}: {body}");
    let inspect = Json::parse(&body).expect("the inspect answer");
    assert_eq!(
        inspect.get("HostConfig").get("Runtime").as_str(),
        Some(RUNTIME)
    );
    assert_eq!(
        inspect.get("NetworkSettings").get("IPAddress").as_str(),
        Some(c.ip(name).as_str())
    );
    let (status, _) = api_get(api, "wrong-token-0123456789", &path);
    assert_eq!(status, 401, "the proxy without the token");

    let (status, body) = api_get(api, token, &format!("/v1/tunnel?to=10.1.2.3:{APP_PORT}"));
    assert_eq!(status, 400, "a tunnel without an upgrade: {body}");

    step(&format!("putting {name} to sleep through the API"));
    let args = [&CTL[..], &["hibernate", &id]].concat();
    let output = c.node.exec(&args, COMMAND_TIMEOUT);
    assert!(output.success(), "{}", smolvm_pods_e2e::describe(&output));
    assert_eq!(c.state(name), "asleep");

    let to = format!("{}:{APP_PORT}", c.ip(name));
    let start = Instant::now();
    let mut tunnel = TcpStream::connect(api).expect("connect to the API");
    tunnel
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    write!(
        tunnel,
        "GET /v1/tunnel?to={to} HTTP/1.1\r\nHost: node\r\nAuthorization: Bearer {token}\r\n\
         Connection: Upgrade\r\nUpgrade: tcp\r\n\r\n"
    )
    .unwrap();
    let head = read_head(&mut tunnel);
    let upgraded = start.elapsed();
    assert!(
        head.starts_with("HTTP/1.1 101"),
        "the tunnel's answer: {head}"
    );
    tunnel
        .write_all(b"GET /hash HTTP/1.0\r\nHost: app\r\n\r\n")
        .unwrap();
    let mut answer = String::new();
    tunnel
        .read_to_string(&mut answer)
        .expect("the app's answer");
    step(&format!(
        "the tunnel to {to} upgraded after {upgraded:?}, and the app answered after {:?}",
        start.elapsed()
    ));
    assert!(answer.starts_with("HTTP/1.0 200"), "{answer}");
    let sum = |text: &str| {
        text.lines()
            .find(|l| l.starts_with("sha256="))
            .unwrap_or_default()
            .to_string()
    };
    assert_eq!(
        sum(&answer),
        sum(hash),
        "the memory of {name} after the tunnel woke it"
    );
    assert_eq!(c.state(name), "awake");
}

/// A GET on the API with `token`: the status and the body.
fn api_get(api: SocketAddr, token: &str, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(api).expect("connect to the API");
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    write!(
        stream,
        "GET {path} HTTP/1.0\r\nHost: node\r\nAuthorization: Bearer {token}\r\n\r\n"
    )
    .unwrap();
    let head = read_head(&mut stream);
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no status in {head:?}"));
    let mut body = String::new();
    stream.read_to_string(&mut body).expect("the body");
    (status, body)
}

/// Reads a response head byte by byte, so that what follows it stays in the
/// stream.
fn read_head(stream: &mut TcpStream) -> String {
    let mut head = Vec::new();
    let mut byte = [0; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(1) => head.push(byte[0]),
            other => panic!(
                "the head ended early ({other:?}): {}",
                String::from_utf8_lossy(&head)
            ),
        }
    }
    String::from_utf8_lossy(&head).into_owned()
}

/// A named volume and the containers on it, removed on drop.
struct Volume<'a> {
    containers: &'a Containers<'a>,
    name: String,
    writer: String,
    reader: String,
}

impl Drop for Volume<'_> {
    fn drop(&mut self) {
        let c = self.containers;
        if let Err(e) = c.docker(&["rm", "-f", &self.writer, &self.reader]) {
            eprintln!("remove the volume's containers: {e}");
        }
        if let Err(e) = c.docker(&["volume", "rm", &self.name]) {
            eprintln!("remove the volume {}: {e}", self.name);
        }
    }
}

/// A VM container writes into a named volume in place: the node sees the
/// file in the volume's directory, a runc container on the volume reads it,
/// it stays through a sleep and a wake, a new VM container on the volume
/// reads it after `docker rm`, and a read-only mount of the volume refuses
/// writes.
fn volumes(c: &Containers, name: &str) {
    let volume = Volume {
        containers: c,
        name: name.to_string(),
        writer: format!("{name}-writer"),
        reader: format!("{name}-reader"),
    };
    let data = format!("{name}:/data");
    c.docker_ok(&["volume", "create", name]);
    let dir = c.docker_ok(&["volume", "inspect", "-f", "{{.Mountpoint}}", name]);
    let note = format!("{}/note", dir.trim());
    let annotation = "smolvm-pods.floci.io/hibernate=true";
    let vm = [
        "--runtime",
        RUNTIME,
        "--annotation",
        annotation,
        "-v",
        &data,
    ];
    let append = |name: &str, line: &str| {
        c.docker_ok(&["exec", name, "/testapp", "append", "/data/note", line])
    };

    c.run(&volume.writer, &vm, "writer");
    eventually(Duration::from_secs(120), "the writer to answer", || {
        c.get(&volume.writer, "/ping")
    });
    assert_eq!(append(&volume.writer, "vm"), "vm\n");
    assert_eq!(
        c.node.sh(&format!("cat {note}")),
        "vm\n",
        "{note} on the node"
    );
    let runc = c.docker_ok(&[
        "run",
        "--rm",
        "-v",
        &data,
        &c.image,
        "append",
        "/data/note",
        "runc",
    ]);
    assert_eq!(runc, "vm\nrunc\n", "the volume in a runc container");

    let id = c.id(&volume.writer);
    let args = [&CTL[..], &["hibernate", &id]].concat();
    let output = c.node.exec(&args, COMMAND_TIMEOUT);
    assert!(output.success(), "{}", smolvm_pods_e2e::describe(&output));
    assert_eq!(c.state(&volume.writer), "asleep");
    c.node.sh(&format!("echo asleep >> {note}"));
    assert_eq!(append(&volume.writer, "woken"), "vm\nrunc\nasleep\nwoken\n");
    assert_eq!(c.state(&volume.writer), "awake");

    c.docker_ok(&["rm", "-f", &volume.writer]);
    let read_only = format!("{name}:/ro:ro");
    c.run(
        &volume.reader,
        &["--runtime", RUNTIME, "-v", &data, "-v", &read_only],
        "reader",
    );
    eventually(Duration::from_secs(120), "the reader to answer", || {
        c.get(&volume.reader, "/ping")
    });
    assert_eq!(
        append(&volume.reader, "again"),
        "vm\nrunc\nasleep\nwoken\nagain\n",
        "the volume in a new VM container"
    );
    let args = [
        "docker",
        "exec",
        &volume.reader,
        "/testapp",
        "append",
        "/ro/note",
        "x",
    ];
    let refused = c.node.exec(&args, COMMAND_TIMEOUT);
    assert!(
        !refused.success() && refused.stderr.contains("read-only file system"),
        "a write to the read-only mount: {}",
        smolvm_pods_e2e::describe(&refused)
    );
    assert_eq!(c.node.sh(&format!("tail -n 1 {note}")), "again\n");
}

/// `docker stop` ends the pod and its network namespace, `docker start`
/// makes a new one, and `docker rm -f` leaves no pod, namespace or VM.
fn stop_start_rm(c: &Containers, name: &str) {
    let id = c.id(name);
    let netns = format!("/run/smolvm-pods/netns/{id}");
    c.docker_ok(&["stop", "-t", "5", name]);
    assert!(!c.up(name), "{name} runs after docker stop");
    assert!(c.pod(name).is_err(), "{name} has a pod after docker stop");
    assert!(
        c.node.try_sh(&format!("test ! -e {netns}")).is_ok(),
        "{netns} stayed"
    );
    c.docker_ok(&["start", name]);
    eventually(Duration::from_secs(120), "the app to answer again", || {
        c.get(name, "/ping")
    });
    assert_eq!(c.state(name), "awake");
    let mut rm = vec!["rm", "-f"];
    rm.extend(c.names.iter().map(String::as_str));
    c.docker_ok(&rm);
    let list = c
        .node
        .exec(&[&CTL[..], &["list"]].concat(), COMMAND_TIMEOUT);
    let pods = Json::parse(&list.stdout).expect("ctl list");
    let standalone = pods
        .get("pods")
        .as_array()
        .unwrap_or_default()
        .iter()
        .filter(|p| p.get("standalone").as_bool() == Some(true))
        .count();
    assert_eq!(standalone, 0, "pods left after docker rm: {pods:?}");
    let left = c.node.sh("ls /run/smolvm-pods/netns 2>/dev/null | wc -l");
    assert_eq!(left.trim(), "0", "network namespaces left after docker rm");
}
