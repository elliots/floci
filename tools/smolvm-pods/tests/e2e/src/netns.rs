//! A process in the node that holds a pod's network namespace, as a process
//! or socket left in it would, so that removing the namespace cannot hide
//! what a delete left in it.

use crate::cmd::describe;
use crate::node::COMMAND_TIMEOUT;
use crate::Cluster;

/// Starts a process that holds network namespace `$1` until it is killed or
/// 10 minutes pass, and prints its pid once it is in the namespace.
const HOLD_NETNS: &str = r#"want="net:[$(stat -L -c %i "/var/run/netns/$1")]"
pidfile=$(mktemp)
setsid -f sh -c 'echo $$ >"$1"; exec nsenter --net="/var/run/netns/$2" sleep 600' \
  sh "$pidfile" "$1" </dev/null >/dev/null 2>&1
for _ in $(seq 100); do
  pid=$(cat "$pidfile")
  if [ -n "$pid" ] && [ "$(readlink "/proc/$pid/ns/net")" = "$want" ]; then
    rm -f "$pidfile"; echo "$pid"; exit 0
  fi
  sleep 0.1
done
rm -f "$pidfile"; echo "no process holds $1" >&2; exit 1
"#;

/// A process in the node that holds a network namespace, ended on drop.
pub struct NetnsHolder<'a> {
    cluster: &'a Cluster,
    pub pid: String,
}

impl NetnsHolder<'_> {
    /// Starts holding the network namespace named `netns`.
    #[track_caller]
    pub fn start<'a>(cluster: &'a Cluster, netns: &str) -> NetnsHolder<'a> {
        let output = cluster
            .node
            .exec(&["sh", "-c", HOLD_NETNS, "sh", netns], COMMAND_TIMEOUT);
        assert!(output.success(), "hold {netns}: {}", describe(&output));
        NetnsHolder {
            cluster,
            pid: output.stdout.trim().to_string(),
        }
    }

    /// The links in the namespace.
    pub fn links(&self) -> Result<String, String> {
        self.cluster.node.try_sh(&format!(
            "nsenter --net=/proc/{}/ns/net ip -o link show",
            self.pid
        ))
    }
}

impl Drop for NetnsHolder<'_> {
    fn drop(&mut self) {
        let kill = format!("kill {}", self.pid);
        if let Err(e) = self.cluster.node.try_sh(&kill) {
            eprintln!("end the holder of a network namespace: {e}");
        }
    }
}
