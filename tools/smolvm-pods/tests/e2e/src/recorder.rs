//! A recorder of containerd's events in the node: `ctr events`, started again
//! whenever it ends, as it does when containerd restarts.

use crate::json::Json;
use crate::node::{Node, COMMAND_TIMEOUT};
use crate::{describe, expect};

/// Starts the recorder loop in a session of its own, appending to file `$1`,
/// and prints the loop's pid once it runs. The loop notes each end of
/// `ctr events` in `$1.err`.
const START: &str = r#"out=$1
rm -f "$out.pid"
setsid -f sh -c 'echo $$ >"$1.pid"
  while :; do
    ctr events >>"$1" 2>>"$1.err"
    echo "ctr events ended at $(date -u +%FT%T.%NZ)" >>"$1.err"
    sleep 0.1
  done' sh "$out" </dev/null >/dev/null 2>&1
for _ in $(seq 100); do
  if [ -s "$out.pid" ]; then cat "$out.pid"; exit 0; fi
  sleep 0.1
done
echo "the recorder did not start" >&2; exit 1
"#;

/// One event that `ctr events` printed.
#[derive(Clone, Debug, PartialEq)]
pub struct TaskEvent {
    /// The line's number in the recording, from 1.
    pub line: usize,
    pub time: String,
    pub namespace: String,
    /// Such as `/tasks/exit`.
    pub topic: String,
    /// The task's container id, which is the CRI container or sandbox id.
    pub container_id: String,
    /// The process: the container id for its init process, or an exec id.
    pub id: String,
    pub exit_status: Option<u64>,
}

impl TaskEvent {
    /// Parses a line of `ctr events`: the time, the namespace, the topic and
    /// the event as JSON, separated by spaces.
    pub fn parse(line: usize, text: &str) -> Option<TaskEvent> {
        let topic_at = text.find(" /")? + 1;
        let (head, rest) = text.split_at(topic_at);
        let (time, namespace) = head.trim_end().rsplit_once(' ')?;
        let (topic, body) = rest.split_once(' ').unwrap_or((rest, "{}"));
        let body = Json::parse(body).ok()?;
        Some(TaskEvent {
            line,
            time: time.to_string(),
            namespace: namespace.to_string(),
            topic: topic.to_string(),
            container_id: body
                .get("container_id")
                .as_str()
                .unwrap_or_default()
                .to_string(),
            id: body.get("id").as_str().unwrap_or_default().to_string(),
            exit_status: body.get("exit_status").as_u64(),
        })
    }

    /// Whether the event reports that a task ended or was removed: the exit
    /// of a container's or sandbox's own process, not of an exec in it, such
    /// as `kubectl exec`, whose exit has the exec's id.
    pub fn is_exit_or_delete(&self) -> bool {
        let exec = !self.id.is_empty() && self.id != self.container_id;
        (self.topic == "/tasks/exit" && !exec) || self.topic == "/tasks/delete"
    }
}

/// The running recorder. Dropping it stops it.
pub struct Recorder<'a> {
    node: &'a Node,
    path: String,
    pid: String,
}

impl<'a> Recorder<'a> {
    /// Starts recording into `path` in the node.
    #[track_caller]
    pub fn start(node: &'a Node, path: &str) -> Recorder<'a> {
        let output = node.exec(&["sh", "-c", START, "sh", path], COMMAND_TIMEOUT);
        assert!(
            output.success(),
            "start the ctr events recorder: {}",
            describe(&output)
        );
        Recorder {
            node,
            path: path.to_string(),
            pid: output.stdout.trim().to_string(),
        }
    }

    /// Fails unless the recorder loop still runs.
    pub fn check_alive(&self) -> Result<(), String> {
        self.node
            .try_sh(&format!("kill -0 {}", self.pid))
            .map(drop)
            .map_err(|e| format!("the ctr events recorder (pid {}) stopped: {e}", self.pid))
    }

    /// The number of lines recorded so far, which marks a point in the
    /// recording.
    #[track_caller]
    pub fn mark(&self) -> usize {
        let count = self.node.sh(&format!("wc -l < {}", self.path));
        count.trim().parse().expect("a line count")
    }

    /// Every event recorded so far.
    #[track_caller]
    pub fn events(&self) -> Vec<TaskEvent> {
        let text = self.node.sh(&format!("cat {}", self.path));
        text.lines()
            .enumerate()
            .filter_map(|(i, line)| TaskEvent::parse(i + 1, line))
            .collect()
    }

    /// When `ctr events` ended and was started again.
    #[track_caller]
    pub fn restarts(&self) -> String {
        expect(
            self.node
                .try_sh(&format!("cat {}.err 2>/dev/null || true", self.path)),
        )
    }
}

impl Drop for Recorder<'_> {
    fn drop(&mut self) {
        if let Err(e) = self.node.try_sh(&format!("kill -- -{}", self.pid)) {
            eprintln!("stop the ctr events recorder: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_task_events() {
        let line = r#"2026-10-19 12:34:56.789012345 +0000 UTC k8s.io /tasks/exit {"container_id":"abc","id":"abc","pid":42,"exit_status":137,"exited_at":"2026-10-19T12:34:56.7Z"}"#;
        let event = TaskEvent::parse(7, line).unwrap();
        assert_eq!(
            event,
            TaskEvent {
                line: 7,
                time: "2026-10-19 12:34:56.789012345 +0000 UTC".into(),
                namespace: "k8s.io".into(),
                topic: "/tasks/exit".into(),
                container_id: "abc".into(),
                id: "abc".into(),
                exit_status: Some(137),
            }
        );
        assert!(event.is_exit_or_delete());
    }

    #[test]
    fn the_exit_of_an_exec_is_not_a_task_exit() {
        let line = r#"2026-10-19 12:34:56 +0000 UTC k8s.io /tasks/exit {"container_id":"abc","id":"e1","pid":43,"exit_status":7}"#;
        let event = TaskEvent::parse(8, line).unwrap();
        assert_eq!((event.id.as_str(), event.exit_status), ("e1", Some(7)));
        assert!(!event.is_exit_or_delete());
    }

    #[test]
    fn parses_events_without_a_container() {
        let line =
            r#"2026-10-19 12:34:56 +0000 UTC k8s.io /images/update {"name":"docker.io/x:1"}"#;
        let event = TaskEvent::parse(1, line).unwrap();
        assert_eq!(event.topic, "/images/update");
        assert_eq!(event.container_id, "");
        assert!(!event.is_exit_or_delete());
        let start =
            r#"2026-10-19 12:34:56 +0000 UTC k8s.io /tasks/start {"container_id":"c1","pid":9}"#;
        let event = TaskEvent::parse(2, start).unwrap();
        assert_eq!(
            (event.topic.as_str(), event.exit_status),
            ("/tasks/start", None)
        );
    }

    #[test]
    fn skips_lines_that_are_not_events() {
        assert_eq!(TaskEvent::parse(1, "ctr: connection refused"), None);
        assert_eq!(TaskEvent::parse(1, "2026 UTC k8s.io /tasks/exit {"), None);
    }
}
