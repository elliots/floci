//! The node VM, driven through `scripts/node-vm.sh`.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use crate::cmd::{self, Output};
use crate::expect;

/// How long a command in the node may take unless the caller says otherwise.
pub const COMMAND_TIMEOUT: Duration = Duration::from_secs(120);
const COPY_TIMEOUT: Duration = Duration::from_secs(300);

/// One node VM, named for `node-vm.sh`.
pub struct Node {
    name: String,
}

impl Node {
    pub fn new(name: String) -> Node {
        Node { name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// The path of `node-vm.sh`.
    pub fn script_path() -> PathBuf {
        let tests = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        tests.parent().unwrap().join("scripts/node-vm.sh")
    }

    /// Runs `node-vm.sh` with `args` for this node and returns its stdout, or
    /// an error that describes how it failed.
    pub fn try_script(&self, args: &[&str], timeout: Duration) -> Result<String, String> {
        cmd::run_ok(&mut self.command(args), timeout)
    }

    /// Like [`Node::try_script`], but panics when the script fails.
    #[track_caller]
    pub fn script(&self, args: &[&str], timeout: Duration) -> String {
        expect(self.try_script(args, timeout))
    }

    /// Runs `args` in the node, whatever their exit status.
    #[track_caller]
    pub fn exec(&self, args: &[&str], timeout: Duration) -> Output {
        let args = [&["exec", "--"], args].concat();
        expect(cmd::run(&mut self.command(&args), timeout))
    }

    /// Runs a shell script in the node and returns its stdout, or an error
    /// that describes how it failed.
    pub fn try_sh(&self, script: &str) -> Result<String, String> {
        self.try_script(&["exec", "--", "sh", "-c", script], COMMAND_TIMEOUT)
    }

    /// Like [`Node::try_sh`], but panics when the script fails.
    #[track_caller]
    pub fn sh(&self, script: &str) -> String {
        expect(self.try_sh(script))
    }

    /// The node's default gateway, smolvm's, which takes TCP to the Mac's
    /// loopback.
    #[track_caller]
    pub fn gateway(&self) -> String {
        let gateway = self.sh("ip -4 route show default | awk '{print $3; exit}'");
        gateway.trim().to_string()
    }

    /// Runs kubectl in the node, whatever its exit status.
    #[track_caller]
    pub fn kubectl(&self, args: &[&str], timeout: Duration) -> Output {
        let args = [&["kubectl"], args].concat();
        expect(cmd::run(&mut self.command(&args), timeout))
    }

    /// Runs kubectl in the node with `input` as its stdin, whatever its exit
    /// status.
    #[track_caller]
    pub fn kubectl_with_input(&self, args: &[&str], input: &[u8], timeout: Duration) -> Output {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let path = std::env::temp_dir().join(format!(
            "floci-e2e-input-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let open = || {
            std::fs::write(&path, input)?;
            File::open(&path)
        };
        let file = open();
        let _ = std::fs::remove_file(&path);
        let file = file.unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let args = [&["kubectl", "-i", "--"], args].concat();
        expect(cmd::run_with_stdin(&mut self.command(&args), file, timeout))
    }

    /// Runs kubectl in the node and returns its stdout, or an error that
    /// describes how it failed.
    pub fn try_kubectl(&self, args: &[&str], timeout: Duration) -> Result<String, String> {
        self.try_script(&[&["kubectl"], args].concat(), timeout)
    }

    /// Like [`Node::try_kubectl`], but panics when kubectl fails.
    #[track_caller]
    pub fn kubectl_ok(&self, args: &[&str]) -> String {
        expect(self.try_kubectl(args, COMMAND_TIMEOUT))
    }

    /// Writes a small text file in the node.
    #[track_caller]
    pub fn write(&self, path: &str, contents: &str) {
        let script = r#"printf %s "$2" > "$1""#;
        self.script(
            &["exec", "--", "sh", "-c", script, "sh", path, contents],
            COMMAND_TIMEOUT,
        );
    }

    /// Copies the file at `src` on this host to `dst` in the node, through the
    /// stdin of `node-vm.sh exec`.
    #[track_caller]
    pub fn copy_in(&self, src: &Path, dst: &str) {
        let open = |src: &Path| Ok::<_, std::io::Error>((File::open(src)?, src.metadata()?.len()));
        let (input, size) = open(src).unwrap_or_else(|e| panic!("{}: {e}", src.display()));
        let script = r#"cat > "$1" && wc -c < "$1""#;
        let mut cmd = self.command(&["exec", "-i", "--", "sh", "-c", script, "sh", dst]);
        let copied = expect(cmd::run_ok_with_input(&mut cmd, input, COPY_TIMEOUT));
        assert_eq!(
            copied.trim(),
            size.to_string(),
            "bytes of {} copied to {dst} in the node",
            src.display()
        );
    }

    /// The `node-vm.sh` command with `args` for this node.
    pub(crate) fn command(&self, args: &[&str]) -> Command {
        let mut cmd = Command::new(Node::script_path());
        cmd.env("SMOLVM_PODS_NODE", &self.name).args(args);
        cmd
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    use super::*;
    use crate::cmd::describe;
    use crate::json::Json;

    const NODE: &str = "floci-node-vm-test";
    const IMAGE_SHA: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const OTHER_SHA: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";

    /// Stands in for the smolvm CLI: `machine ls` prints `before.json` until
    /// `machine delete` runs, and then `after.json`, failing when it is missing.
    /// `machine create` and `machine update` write their arguments to
    /// `create.args` and `update.args`, one per line. `machine exec` prints
    /// `token`.
    const SMOLVM: &str = r#"#!/bin/sh
dir=$(dirname "$0")
case "$1 $2" in
    "machine ls")
        if [ -e "$dir/deleted" ]; then exec cat "$dir/after.json"; fi
        exec cat "$dir/before.json" ;;
    "machine delete") touch "$dir/deleted" ;;
    "machine exec") exec cat "$dir/token" ;;
    "machine create" | "machine update")
        verb=$2
        shift 2
        printf '%s\n' "$@" > "$dir/$verb.args" ;;
    *) echo "unexpected smolvm $*" >&2; exit 1 ;;
esac
"#;

    /// A home directory with smolvm's copy of image `IMAGE_SHA`, and a smolvm
    /// that lists node `NODE`, made from that image, until it is deleted.
    struct FakeMac {
        dir: PathBuf,
    }

    impl FakeMac {
        /// `after` is the listing once the node is deleted, or `None` for a
        /// listing that fails.
        fn new(after: Option<&str>) -> FakeMac {
            static NEXT: AtomicU32 = AtomicU32::new(0);
            let dir = std::env::temp_dir().join(format!(
                "floci-e2e-node-vm-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let mac = FakeMac { dir };
            fs::create_dir_all(mac.archive()).unwrap();
            fs::write(mac.dir.join("before.json"), listing(&[(NODE, IMAGE_SHA)])).unwrap();
            if let Some(after) = after {
                fs::write(mac.dir.join("after.json"), after).unwrap();
            }
            fs::write(mac.smolvm(), SMOLVM).unwrap();
            fs::set_permissions(mac.smolvm(), fs::Permissions::from_mode(0o755)).unwrap();
            mac
        }

        fn smolvm(&self) -> PathBuf {
            self.dir.join("smolvm")
        }

        fn archive(&self) -> PathBuf {
            self.dir
                .join("home/Library/Caches/smolvm-image-archives")
                .join(IMAGE_SHA)
        }

        /// Makes `machine ls` print `listing` until a delete.
        fn list(&self, listing: &str) {
            fs::write(self.dir.join("before.json"), listing).unwrap();
        }

        /// An image tar for `create --image`.
        fn image(&self) -> String {
            let image = self.dir.join("node-image.tar");
            fs::write(&image, "not an image").unwrap();
            image.display().to_string()
        }

        /// The arguments of the last `machine create` or `machine update`.
        fn args(&self, verb: &str) -> Option<Vec<String>> {
            let args = fs::read_to_string(self.dir.join(format!("{verb}.args"))).ok()?;
            Some(args.lines().map(str::to_string).collect())
        }

        fn run(&self, args: &[&str]) -> Output {
            let mut cmd = Node::new(NODE.to_string()).command(args);
            cmd.env("SMOLVM", self.smolvm())
                .env("HOME", self.dir.join("home"))
                .env_remove("XDG_CONFIG_HOME");
            expect(cmd::run(&mut cmd, COMMAND_TIMEOUT))
        }

        fn delete_node(&self) -> Output {
            self.run(&["delete"])
        }

        /// Runs `create` on a Mac with no machines and returns the arguments
        /// it gave `machine create`.
        #[track_caller]
        fn create_args(&self, options: &[&str]) -> Vec<String> {
            self.list("[]\n");
            let image = self.image();
            let output = self.run(&[&["create", "--image", &image], options].concat());
            assert!(output.success(), "create: {}", describe(&output));
            self.args("create").expect("create ran machine create")
        }
    }

    /// Whether `args` has `option` followed by `value`.
    fn has(args: &[String], option: &str, value: &str) -> bool {
        args.windows(2)
            .any(|pair| pair[0] == option && pair[1] == value)
    }

    impl Drop for FakeMac {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }

    /// What `smolvm machine ls --json` prints for these machines, each made
    /// from the image of its checksum.
    fn listing(machines: &[(&str, &str)]) -> String {
        let entries: Vec<String> = machines
            .iter()
            .map(|(name, sha)| {
                format!(
                    "  {{\n    \"name\": \"{name}\",\n    \"state\": \"stopped\",\n    \
                     \"labels\": {{\n      \"image-sha256\": \"{sha}\"\n    }}\n  }}"
                )
            })
            .collect();
        format!("[\n{}\n]\n", entries.join(",\n"))
    }

    /// What `smolvm machine ls --json` prints for these machines and their
    /// labels.
    fn labelled_listing(machines: &[(&str, &[(&str, &str)])]) -> String {
        let entries: Vec<String> = machines
            .iter()
            .map(|(name, labels)| {
                let labels: Vec<String> = labels
                    .iter()
                    .map(|(key, value)| format!("\"{key}\": \"{value}\""))
                    .collect();
                format!(
                    "  {{\"name\": \"{name}\", \"state\": \"running\", \"labels\": {{{}}}}}",
                    labels.join(", ")
                )
            })
            .collect();
        format!("[\n{}\n]\n", entries.join(",\n"))
    }

    /// `first`, then enough other machines that a reader that stops at
    /// `first` leaves most of the listing unread.
    fn long_listing(first: (&str, &str)) -> String {
        let names: Vec<String> = (0..5000).map(|i| format!("other-{i}")).collect();
        let mut machines = vec![first];
        machines.extend(names.iter().map(|name| (name.as_str(), OTHER_SHA)));
        listing(&machines)
    }

    #[test]
    fn delete_keeps_an_image_that_another_machine_uses() {
        let mac = FakeMac::new(Some(&long_listing(("dev-node", IMAGE_SHA))));
        let output = mac.delete_node();
        assert!(output.success(), "delete: {}", describe(&output));
        assert!(mac.archive().is_dir(), "delete removed the image archive");
    }

    #[test]
    fn delete_removes_an_image_that_no_machine_uses() {
        let mac = FakeMac::new(Some(&long_listing(("dev-node", OTHER_SHA))));
        let output = mac.delete_node();
        assert!(output.success(), "delete: {}", describe(&output));
        assert!(!mac.archive().exists(), "delete kept the image archive");
    }

    #[test]
    fn delete_keeps_the_image_when_the_listing_fails() {
        let mac = FakeMac::new(None);
        let output = mac.delete_node();
        assert!(!output.success(), "delete passed: {}", describe(&output));
        assert!(mac.archive().is_dir(), "delete removed the image archive");
    }

    #[test]
    fn create_publishes_and_labels_the_ports_and_mounts_the_seed() {
        let mac = FakeMac::new(None);
        let seed = mac.dir.join("seed");
        fs::create_dir_all(&seed).unwrap();
        let seed = seed.display().to_string();
        let args = mac.create_args(&["--service-port", "17070", "--seed", &seed]);
        let mount = format!("{seed}:/etc/floci-node/seed:ro");
        for (option, value) in [
            ("--port", "6443:6443"),
            ("--port", "17070:7070"),
            ("--port", "32080-32099:32080-32099"),
            ("--label", "api-port=6443"),
            ("--label", "service-port=17070"),
            ("--label", "node-ports=32080-32099"),
            ("--label", "smolvm-pods-node=true"),
            ("--volume", &mount),
        ] {
            assert!(has(&args, option, value), "no {option} {value}: {args:?}");
        }
        for open in ["--allow-cidr", "--allow-host-pattern", "isolated=true"] {
            assert!(!args.iter().any(|arg| arg == open), "{open}: {args:?}");
        }
    }

    #[test]
    fn create_isolates_a_node_at_its_gateway() {
        for (options, gateway) in [
            (&[][..], "100.96.0.1/32"),
            (
                &["--", "--guest-subnet", "10.200.0.0/30"][..],
                "10.200.0.1/32",
            ),
        ] {
            let mac = FakeMac::new(None);
            let mut create = vec![
                "--node-ports",
                "none",
                "--isolate",
                "--allow-cidr",
                "203.0.113.10/32",
            ];
            create.extend(options);
            let args = mac.create_args(&create);
            for (option, value) in [
                ("--allow-cidr", gateway),
                ("--allow-cidr", "203.0.113.10/32"),
                ("--allow-host-pattern", "*.closed.floci.invalid"),
                ("--env", "FLOCI_NODE_ISOLATION=1"),
                ("--label", "isolated=true"),
                ("--label", "node-ports=none"),
            ] {
                assert!(has(&args, option, value), "no {option} {value}: {args:?}");
            }
            let ports: Vec<&String> = args.iter().filter(|arg| *arg == "--port").collect();
            assert_eq!(ports.len(), 1, "only the API's port: {args:?}");
        }
    }

    #[test]
    fn create_refuses_an_allow_list_without_isolation() {
        let mac = FakeMac::new(None);
        mac.list("[]\n");
        let image = mac.image();
        let output = mac.run(&[
            "create",
            "--image",
            &image,
            "--allow-cidr",
            "203.0.113.10/32",
        ]);
        assert!(!output.success(), "create passed: {}", describe(&output));
        assert!(
            output.stderr.contains("--allow-cidr needs --isolate"),
            "{}",
            describe(&output)
        );
        assert_eq!(mac.args("create"), None, "create ran machine create");
    }

    #[test]
    fn create_refuses_to_isolate_a_node_on_a_named_network() {
        for network in [&["--network", "lab"][..], &["--network=lab"][..]] {
            let mac = FakeMac::new(None);
            mac.list("[]\n");
            let image = mac.image();
            let create = [&["create", "--image", &image, "--isolate", "--"], network].concat();
            let output = mac.run(&create);
            assert!(!output.success(), "create passed: {}", describe(&output));
            assert!(
                output
                    .stderr
                    .contains("--isolate does not work with smolvm's --network"),
                "{}",
                describe(&output)
            );
            assert_eq!(mac.args("create"), None, "create ran machine create");
        }
    }

    #[test]
    fn update_changes_the_allow_list_of_an_isolated_node_only() {
        let change = [
            "update",
            "--allow-cidr",
            "203.0.113.10/32",
            "--remove-allow-cidr",
            "198.51.100.0/24",
        ];
        let isolated = FakeMac::new(None);
        isolated.list(&labelled_listing(&[(
            NODE,
            &[("smolvm-pods-node", "true"), ("isolated", "true")],
        )]));
        let output = isolated.run(&change);
        assert!(output.success(), "update: {}", describe(&output));
        assert_eq!(
            isolated.args("update").expect("update ran machine update"),
            [
                "--name",
                NODE,
                "--allow-cidr",
                "203.0.113.10/32",
                "--remove-allow-cidr",
                "198.51.100.0/24"
            ]
        );

        let open = FakeMac::new(None);
        open.list(&labelled_listing(&[(
            NODE,
            &[("smolvm-pods-node", "true")],
        )]));
        let output = open.run(&change);
        assert!(!output.success(), "update passed: {}", describe(&output));
        assert!(
            output.stderr.contains("is not isolated"),
            "{}",
            describe(&output)
        );
        assert_eq!(open.args("update"), None, "update ran machine update");
    }

    #[test]
    fn status_lists_the_nodes_that_create_made() {
        let mac = FakeMac::new(None);
        mac.list(&labelled_listing(&[
            ("other-machine", &[("owner", "someone")]),
            (NODE, &[("smolvm-pods-node", "true"), ("api-port", "6443")]),
            ("bare-machine", &[]),
        ]));
        let output = mac.run(&["status", "--json"]);
        assert!(output.success(), "status: {}", describe(&output));
        let status = Json::parse(&output.stdout).unwrap();
        let nodes = status.as_array().expect("an array");
        assert_eq!(nodes.len(), 1, "{status}");
        assert_eq!(nodes[0].get("name").as_str(), Some(NODE), "{status}");
        assert_eq!(nodes[0].get("state").as_str(), Some("running"), "{status}");
        assert_eq!(
            nodes[0].get("labels").get("api-port").as_str(),
            Some("6443"),
            "{status}"
        );
    }

    #[test]
    fn docker_env_writes_a_private_config_with_the_token() {
        let mac = FakeMac::new(None);
        mac.list(&labelled_listing(&[(
            NODE,
            &[("smolvm-pods-node", "true"), ("service-port", "17070")],
        )]));
        fs::write(mac.dir.join("token"), "0123456789abcdef\n").unwrap();
        let output = mac.run(&["docker-env"]);
        assert!(output.success(), "docker-env: {}", describe(&output));
        let dir = mac.dir.join("home/.config/smolvm-pods/docker").join(NODE);
        assert_eq!(
            output.stdout,
            format!(
                "export DOCKER_HOST=tcp://127.0.0.1:17070/docker\nexport DOCKER_CONFIG={}\n",
                dir.display()
            )
        );
        let config = Json::parse(&fs::read_to_string(dir.join("config.json")).unwrap()).unwrap();
        assert_eq!(
            config.get("HttpHeaders").get("Authorization").as_str(),
            Some("Bearer 0123456789abcdef"),
            "{config}"
        );
        let mode = |path: &Path| fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&dir), 0o700, "the config directory");
        assert_eq!(mode(&dir.join("config.json")), 0o600, "the config file");
    }

    #[test]
    fn docker_env_needs_a_node_service_port() {
        let mac = FakeMac::new(None);
        mac.list(&labelled_listing(&[(
            NODE,
            &[("smolvm-pods-node", "true")],
        )]));
        fs::write(mac.dir.join("token"), "0123456789abcdef\n").unwrap();
        let output = mac.run(&["docker-env"]);
        assert!(
            !output.success(),
            "docker-env passed: {}",
            describe(&output)
        );
        assert!(
            output.stderr.contains("create --service-port PORT"),
            "{}",
            describe(&output)
        );
        assert!(
            !mac.dir.join("home/.config/smolvm-pods").exists(),
            "docker-env wrote a config"
        );
    }
}
