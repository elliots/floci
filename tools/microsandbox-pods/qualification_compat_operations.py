"""Shared Kubernetes lifecycle, resource projection, and log contracts."""
import json
import subprocess
import time


def qualify_compat_operations(*, peer, pod, apply, resource, kubectl, ready,
                              lifecycle, control_get, crash_and_restart, wait, report, command, msb, environment):
    collector = "compat-hook-collector"
    sink = pod(collector, r'''mkdir -p /www/cgi-bin
cat >/www/cgi-bin/event <<'HOOK'
#!/bin/sh
echo "$QUERY_STRING" >>/tmp/events
printf 'Content-Type: text/plain\r\n\r\nok\n'
HOOK
chmod +x /www/cgi-bin/event
exec httpd -f -p 8080 -h /www''', native=False)
    sink["spec"]["containers"][0]["readinessProbe"] = {"tcpSocket": {"port": 8080}, "periodSeconds": 1}
    apply(sink)
    apply({"apiVersion": "v1", "kind": "Service", "metadata": {"name": collector},
           "spec": {"selector": {"app": collector}, "ports": [{"port": 8080, "targetPort": 8080}]}})
    wait("lifecycle event collector", lambda: ready(collector))
    for native in (False, True):
        kind = "native" if native else "docker"
        name = "compat-" + kind
        endpoint = f"http://{collector}:8080/cgi-bin/event"
        script = r'''mkdir -p /data /www/cgi-bin
attempt=0; test ! -f /data/attempt || attempt=$(cat /data/attempt)
attempt=$((attempt+1)); echo "$attempt" >/data/attempt
rm -f /data/post /data/pre /data/exit
cat >/www/cgi-bin/pre <<'HOOK'
#!/bin/sh
touch /data/pre
wget -qO- 'ENDPOINT?pre-NAME' >/dev/null
sleep 1
printf 'Status: 500 Intentional lifecycle response\r\nContent-Type: text/plain\r\n\r\nhook complete\n'
HOOK
chmod +x /www/cgi-bin/pre
httpd -p 8080 -h /www
trap 'test -f /data/pre && wget -qO- "ENDPOINT?term-NAME" >/dev/null; exit 0' TERM
export PROCESS_MARKER=$(cat /proc/sys/kernel/random/uuid)
echo "boot-$attempt"
while ! test -f /data/exit; do echo "tick-$attempt"; sleep 1; done
exit 17'''.replace("ENDPOINT", endpoint).replace("NAME", name)
        value = pod(name, script, native=native)
        value["spec"]["restartPolicy"] = "Always"
        value["spec"]["terminationGracePeriodSeconds"] = 20
        value["spec"]["volumes"] = [{"name": "data", "emptyDir": {}}, {"name": "metadata", "downwardAPI": {"items": [
            {"path": "cpu", "resourceFieldRef": {"containerName": "app", "resource": "requests.cpu", "divisor": "1m"}},
            {"path": "memory", "resourceFieldRef": {"containerName": "app", "resource": "limits.memory", "divisor": "1Mi"}}]}}]
        container = value["spec"]["containers"][0]
        container["volumeMounts"] = [{"name": "data", "mountPath": "/data"}, {"name": "metadata", "mountPath": "/metadata", "readOnly": True}]
        container["resources"] = {"requests": {"cpu": "125m", "memory": "64Mi"}, "limits": {"cpu": "1", "memory": "128Mi"}}
        container["lifecycle"] = {"postStart": {"exec": {"command": ["sh", "-ec",
            "until test -s /data/attempt; do sleep .1; done; echo post >>/data/hooks; touch /data/post"]}},
            "preStop": {"httpGet": {"port": 8080, "path": "/cgi-bin/pre"}}}
        container["readinessProbe"] = {"exec": {"command": ["sh", "-ec", "test -f /data/post; wget -qO- http://127.0.0.1:8080/ >/dev/null 2>&1 || test -d /www"]}, "periodSeconds": 1}
        apply(value)
        try:
            observed = wait(kind + " lifecycle application", lambda: ready(name), 360)
            uid = observed["metadata"]["uid"]
            assert kubectl("exec", name, "--", "cat", "/metadata/cpu", "/metadata/memory") == "125128"
            assert kubectl("exec", name, "--", "cat", "/data/hooks") == "post"
            logs = kubectl("logs", name, "--timestamps")
            assert "T" in logs.split(" ", 1)[0] and "boot-1" in logs, logs
            assert "boot-1" in kubectl("logs", name, "--since=10m")
            assert len(kubectl("logs", name, "--tail=1").splitlines()) == 1
            assert kubectl("logs", name, "--tail=0") == ""
            limited = kubectl("logs", name, "--limit-bytes=4")
            assert len(limited) <= 4, limited
            report["checks"][kind + "_poststart_resources_timestamp_since_tail_limits"] = True
            if native:
                state = lifecycle(uid)
                report["native_log_helper_rss_kib"] = int(command(msb, "exec", "--no-stdin", state["vm"], "--", "sh", "-ec",
                    "for task in /proc/[0-9]*; do test -r \"$task/cmdline\" || continue; "
                    "case \"$(tr '\\0' ' ' <\"$task/cmdline\")\" in '/var/lib/floci-tools/guestfs log '*) "
                    "awk '/^VmRSS:/ {sum += $2} END {print sum+0}' \"$task/status\";; esac; done", env=environment))
                lifecycle(uid, "hibernate", state["generation"])
                sleeping = kubectl("logs", name, "--timestamps")
                assert "boot-1" in sleeping
                assert lifecycle(uid)["state"] == "SLEEPING"
                assert control_get("health")["activeMemoryMiB"] == 0
                crash_and_restart()
                assert kubectl("logs", name, "--timestamps") == sleeping
                state = lifecycle(uid)
                lifecycle(uid, "resume", state["generation"])
                wait("application ready after RAM restore", lambda: ready(name))
                assert kubectl("exec", name, "--", "cat", "/data/hooks") == "post"
                report["checks"]["native_sleeping_logs_recover_without_wake_or_hook_replay"] = True
            follow = subprocess.Popen(["docker", "exec", "-i", peer, "kubectl", "logs", "-f", name, "--tail=1"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                try:
                    streamed, failure = follow.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    follow.terminate()
                    streamed, failure = follow.communicate(timeout=10)
                assert streamed.count(b"tick-1") >= 2, (streamed, failure)
            finally:
                if follow.poll() is None:
                    follow.terminate()
                    follow.communicate(timeout=10)
            report["checks"][kind + "_follow_streams_new_output"] = True
            kubectl("exec", name, "--", "touch", "/data/exit")
            wait(kind + " actual container restart", lambda: any(c.get("restartCount", 0) >= 1 and c.get("ready")
                 for c in resource("pod", name).get("status", {}).get("containerStatuses", [])), 180)
            assert "boot-1" in kubectl("logs", name, "--previous", "--timestamps")
            assert "boot-2" in kubectl("logs", name)
            assert kubectl("exec", name, "--", "cat", "/data/hooks").splitlines() == ["post", "post"]
            report["checks"][kind + "_previous_logs_and_hook_once_per_container_start"] = True
            if native:
                state = lifecycle(uid)
                lifecycle(uid, "hibernate", state["generation"])
                assert lifecycle(uid)["state"] == "SLEEPING"
            kubectl("delete", "pod", name, "--wait=true", timeout=180)
            events = kubectl("exec", collector, "--", "cat", "/tmp/events").splitlines()
            assert events[-2:] == ["pre-"+name, "term-"+name], events
            report["checks"][kind + "_http_prestop_before_term_within_grace"] = True
            if native:
                wait("native delete releases RAM", lambda: control_get("health")["activeMemoryMiB"] == 0)
                report["checks"]["native_sleeping_delete_resumes_only_for_prestop"] = True
        finally:
            kubectl("delete", "pod", name, "--ignore-not-found", "--grace-period=0", "--wait=true", timeout=180)
    kubectl("delete", "pod", collector, "--wait=true", timeout=180)
    kubectl("delete", "service", collector)
