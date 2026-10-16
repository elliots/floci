"""Real Kubernetes checks for independent containers sharing one native Pod VM."""
import copy


def qualify_applications(*, pod, apply, resource, kubectl, ready, lifecycle,
                         control_get, crash_and_restart, wait, command, msb,
                         environment, report):
    name = "native-multi-app"
    value = pod(name, "unused")
    image = value["spec"]["containers"][0]["image"]
    mount = [{"name": "shared", "mountPath": "/shared"}]
    value["spec"]["restartPolicy"] = "Always"
    value["spec"]["volumes"] = [{"name": "shared", "emptyDir": {"medium": "Memory"}}]
    value["spec"]["initContainers"] = [{"name": "initialize", "image": image,
        "command": ["sh", "-ec", "echo once >>/shared/init-runs; cat /proc/sys/kernel/random/uuid >/shared/ram-marker"],
        "volumeMounts": copy.deepcopy(mount)}]
    value["spec"]["containers"] = [
        {"name": "server", "image": image, "command": ["sh", "-ec",
            "echo server >>/shared/server-runs; mkdir -p /www; echo shared-localhost >/www/index.html; "
            "SERVER_MEMORY=$(cat /proc/sys/kernel/random/uuid); export SERVER_MEMORY; "
            "echo server-container-log; exec httpd -f -p 8080 -h /www"],
         "ports": [{"containerPort": 8080}], "volumeMounts": copy.deepcopy(mount),
         "resources": {"requests": {"cpu": "600m", "memory": "512Mi"}},
         "readinessProbe": {"httpGet": {"path": "/", "port": 8080}, "periodSeconds": 1}},
        {"name": "worker", "image": image, "command": ["sh", "-ec",
            "echo worker >>/shared/worker-runs; "
            "until wget -qO /shared/localhost http://127.0.0.1:8080/; do sleep .1; done; "
            "WORKER_MEMORY=$(cat /proc/sys/kernel/random/uuid); export WORKER_MEMORY; "
            "echo worker-container-log; exec sleep 900"],
         "volumeMounts": copy.deepcopy(mount),
         "resources": {"requests": {"cpu": "500m", "memory": "256Mi"}},
         "readinessProbe": {"exec": {"command": ["grep", "-q", "shared-localhost", "/shared/localhost"]}, "periodSeconds": 1}},
    ]
    apply(value)
    observed = wait("both regular applications Ready", lambda: ready(name), 360)
    uid = observed["metadata"]["uid"]
    state = lifecycle(uid)
    assert state["vcpus"] == 2 and state["memoryMiB"] == 1024, state
    assert len(observed["status"]["containerStatuses"]) == 2
    assert kubectl("exec", name, "-c", "worker", "--", "cat", "/shared/init-runs") == "once"
    assert kubectl("exec", name, "-c", "worker", "--", "cat", "/shared/localhost") == "shared-localhost"
    assert "server-container-log" in kubectl("logs", name, "-c", "server")
    assert "worker-container-log" in kubectl("logs", name, "-c", "worker")
    report["checks"]["multiple_applications_share_localhost_and_memory_volume"] = True
    report["checks"]["multiple_application_resource_sum_and_named_logs_exec"] = True

    def identity(container):
        key = container.upper() + "_MEMORY="
        return kubectl("exec", name, "-c", container, "--", "sh", "-ec",
            "tr '\\0' '\\n' </proc/1/environ | grep '^" + key + "'; awk '{print $22}' /proc/1/stat")

    server_before, worker_before = identity("server"), identity("worker")
    ram_marker = kubectl("exec", name, "-c", "server", "--", "cat", "/shared/ram-marker")
    command(msb, "exec", "--no-stdin", state["vm"], "--", "ctr", "--address",
            "/run/containerd/containerd.sock", "tasks", "kill", "--signal", "SIGKILL", "app-1", env=environment)
    wait("only worker restarts", lambda: any(status["name"] == "worker" and status.get("restartCount") == 1
         for status in resource("pod", name).get("status", {}).get("containerStatuses", [])), 180)
    wait("both applications recover readiness", lambda: ready(name), 180)
    assert identity("server") == server_before
    assert identity("worker") != worker_before
    assert kubectl("exec", name, "-c", "worker", "--", "cat", "/shared/init-runs") == "once"
    assert kubectl("exec", name, "-c", "server", "--", "cat", "/shared/server-runs") == "server"
    assert kubectl("exec", name, "-c", "worker", "--", "cat", "/shared/worker-runs") == "worker\nworker"
    report["checks"]["application_restart_preserves_sibling_and_completed_init"] = True

    worker_after = identity("worker")
    for cycle in range(2):
        state = lifecycle(uid)
        lifecycle(uid, "hibernate", state["generation"])
        assert lifecycle(uid)["state"] == "SLEEPING"
        assert control_get("health")["activeMemoryMiB"] == 0
        if cycle == 0:
            crash_and_restart()
        state = lifecycle(uid)
        lifecycle(uid, "resume", state["generation"])
        wait("both applications resume", lambda: ready(name), 180)
        assert identity("server") == server_before
        assert identity("worker") == worker_after
        assert kubectl("exec", name, "-c", "worker", "--", "cat", "/shared/ram-marker") == ram_marker
    crash_and_restart()
    wait("running multi-container Pod adopted", lambda: ready(name), 180)
    assert identity("server") == server_before and identity("worker") == worker_after
    report["checks"]["multiple_applications_preserve_ram_through_cycles_and_recovery"] = True
    kubectl("delete", "pod", name, "--wait=true", timeout=180)
    wait("multi-app fixture releases RAM", lambda: control_get("health")["activeMemoryMiB"] == 0)

    job = pod("native-multi-job", "echo first-finished; exit 0")
    job["spec"]["volumes"] = [{"name": "shared", "emptyDir": {}}]
    job["spec"]["containers"][0]["volumeMounts"] = copy.deepcopy(mount)
    job["spec"]["containers"].append({"name": "second", "image": image,
        "command": ["sh", "-ec", "until test -f /shared/finish; do sleep .1; done; exit 7"],
        "volumeMounts": copy.deepcopy(mount)})
    apply(job)

    def first_completed():
        observed = resource("pod", "native-multi-job")
        statuses = observed.get("status", {}).get("containerStatuses", [])
        return observed if any(s["name"] == "app" and s.get("state", {}).get("terminated", {}).get("exitCode") == 0
            for s in statuses) and any(s["name"] == "second" and "running" in s.get("state", {}) for s in statuses) else None

    observed = wait("first application completes while sibling runs", first_completed, 360)
    assert observed["status"]["phase"] == "Running"
    kubectl("exec", "native-multi-job", "-c", "second", "--", "touch", "/shared/finish")
    wait("Pod fails only after all regular containers exit", lambda: resource("pod", "native-multi-job").get("status", {}).get("phase") == "Failed", 120)
    statuses = resource("pod", "native-multi-job")["status"]["containerStatuses"]
    assert {s["name"]: s["state"]["terminated"]["exitCode"] for s in statuses} == {"app": 0, "second": 7}
    report["checks"]["pod_completion_waits_for_all_regular_containers"] = True
    kubectl("delete", "pod", "native-multi-job", "--wait=true", timeout=180)
    wait("multi-app job releases RAM", lambda: control_get("health")["activeMemoryMiB"] == 0)
