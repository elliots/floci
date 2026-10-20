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

    const NODE: &str = "floci-node-vm-test";
    const IMAGE_SHA: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const OTHER_SHA: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";

    /// Stands in for the smolvm CLI: `machine ls` prints `before.json` until
    /// `machine delete` runs, and then `after.json`, failing when it is missing.
    const SMOLVM: &str = r#"#!/bin/sh
dir=$(dirname "$0")
case "$1 $2" in
    "machine ls")
        if [ -e "$dir/deleted" ]; then exec cat "$dir/after.json"; fi
        exec cat "$dir/before.json" ;;
    "machine delete") touch "$dir/deleted" ;;
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

        fn delete_node(&self) -> Output {
            let mut cmd = Node::new(NODE.to_string()).command(&["delete"]);
            cmd.env("SMOLVM", self.smolvm())
                .env("HOME", self.dir.join("home"));
            expect(cmd::run(&mut cmd, COMMAND_TIMEOUT))
        }
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
}
