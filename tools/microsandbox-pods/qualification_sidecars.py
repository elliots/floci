"""Real Kubernetes restartable init sidecars, recovery and ordered Job completion."""
import copy
import json
import shlex


def qualify_sidecars(*, pod, apply, resource, kubectl, ready, lifecycle, control_get,
                     crash_and_restart, wait, command, msb, environment, report):
    name = "native-init-sidecars"
    value = pod(name, "unused")
    image = value["spec"]["containers"][0]["image"]
    mounts = [{"name": "shared", "mountPath": "/shared"}]
    value["spec"]["terminationGracePeriodSeconds"] = 20
    value["spec"]["volumes"] = [{"name": "shared", "emptyDir": {"medium": "Memory"}}]

    def sidecar(container, script):
        return {"name": container, "image": image, "restartPolicy": "Always",
                "command": ["sh", "-ec", script], "volumeMounts": copy.deepcopy(mounts)}

    proxy_shutdown = "echo proxy >>/shared/stop-order; printf shutdown-order:; tr '\\n' , </shared/stop-order; echo; exit 0"
    proxy_loop = "trap " + shlex.quote(proxy_shutdown) + " TERM; while true; do sleep .2; done"
    proxy = sidecar("proxy", "echo proxy >>/shared/proxy-runs; "
        "SIDECAR_MEMORY=$(cat /proc/sys/kernel/random/uuid); export SIDECAR_MEMORY; "
        "exec sh -ec " + shlex.quote(proxy_loop))
    proxy["startupProbe"] = {"exec": {"command": ["test", "-f", "/shared/startup-open"]},
                             "periodSeconds": 1, "failureThreshold": 300}
    proxy["readinessProbe"] = {"exec": {"command": ["test", "-f", "/shared/proxy-ready"]},
                               "periodSeconds": 1, "failureThreshold": 1}
    logger_loop = "trap 'echo logger >>/shared/stop-order; echo logger-stopped; exit 0' TERM; while true; do sleep .2; done"
    logger = sidecar("logger", "exec sh -ec " + shlex.quote(logger_loop))
    value["spec"]["initContainers"] = [proxy,
        {"name": "prepare", "image": image,
         "command": ["sh", "-ec", "echo once >>/shared/init-runs"], "volumeMounts": copy.deepcopy(mounts)}, logger]
    value["spec"]["containers"][0].update({
        "command": ["sh", "-ec", "echo app >>/shared/app-runs; "
            "APP_MEMORY=$(cat /proc/sys/kernel/random/uuid); export APP_MEMORY; "
            "exec sh -ec 'until test -f /shared/finish; do sleep .2; done; echo app-finished'"],
        "volumeMounts": copy.deepcopy(mounts)})
    job_name = name
    apply({"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": job_name},
           "spec": {"backoffLimit": 0, "template": {"metadata": {"labels": value["metadata"]["labels"]},
                   "spec": value["spec"]}}})
    children = wait("Job creates its native Pod", lambda: json.loads(kubectl("get", "pods",
        "-l", "job-name=" + job_name, "-o", "json"))["items"])
    assert len(children) == 1, children
    name = children[0]["metadata"]["name"]

    def status(container, init=False):
        field = "initContainerStatuses" if init else "containerStatuses"
        return next((item for item in resource("pod", name).get("status", {}).get(field, [])
                     if item["name"] == container), {})

    wait("first sidecar running before startup gate", lambda: "running" in status("proxy", True).get("state", {}), 360)
    assert not status("proxy", True).get("started", False)
    assert "waiting" in status("prepare", True)["state"]
    assert "waiting" in status("app")["state"]
    kubectl("exec", name, "-c", "proxy", "--", "touch", "/shared/startup-open")
    wait("startup gate admits ordinary init and applications", lambda: "running" in status("app").get("state", {}), 180)
    assert not ready(name), "unready sidecar admitted actual Pod readiness"
    kubectl("exec", name, "-c", "proxy", "--", "touch", "/shared/proxy-ready")
    observed = wait("sidecar readiness admits Pod", lambda: ready(name), 180)
    uid = observed["metadata"]["uid"]
    assert kubectl("exec", name, "-c", "app", "--", "cat", "/shared/init-runs") == "once"
    report["checks"]["sidecar_startup_gates_later_init_and_readiness_gates_pod"] = True

    def identity(container, key):
        return kubectl("exec", name, "-c", container, "--", "sh", "-ec",
            "tr '\\0' '\\n' </proc/1/environ | grep '^" + key + "='; awk '{print $22}' /proc/1/stat")

    app_before, proxy_before = identity("app", "APP_MEMORY"), identity("proxy", "SIDECAR_MEMORY")
    state = lifecycle(uid)
    command(msb, "exec", "--no-stdin", state["vm"], "--", "ctr", "--address",
        "/run/containerd/containerd.sock", "tasks", "kill", "--signal", "SIGKILL", "init-0", env=environment)
    wait("sidecar restarts despite Pod Never", lambda: status("proxy", True).get("restartCount") == 1, 180)
    wait("sidecar recovers readiness", lambda: ready(name), 180)
    assert identity("app", "APP_MEMORY") == app_before
    assert identity("proxy", "SIDECAR_MEMORY") != proxy_before
    assert kubectl("exec", name, "-c", "app", "--", "cat", "/shared/init-runs") == "once"
    assert kubectl("exec", name, "-c", "app", "--", "cat", "/shared/app-runs") == "app"
    assert kubectl("exec", name, "-c", "app", "--", "cat", "/shared/proxy-runs") == "proxy\nproxy"
    report["checks"]["sidecar_restart_under_never_preserves_application_and_completed_init"] = True

    proxy_after = identity("proxy", "SIDECAR_MEMORY")
    state = lifecycle(uid)
    lifecycle(uid, "hibernate", state["generation"])
    assert lifecycle(uid)["state"] == "SLEEPING"
    assert control_get("health")["activeMemoryMiB"] == 0
    crash_and_restart()
    state = lifecycle(uid)
    lifecycle(uid, "resume", state["generation"])
    wait("sidecar and application resume", lambda: ready(name), 180)
    assert identity("app", "APP_MEMORY") == app_before
    assert identity("proxy", "SIDECAR_MEMORY") == proxy_after
    crash_and_restart()
    wait("running sidecar Pod adopted", lambda: ready(name), 180)
    assert identity("app", "APP_MEMORY") == app_before
    assert identity("proxy", "SIDECAR_MEMORY") == proxy_after
    report["checks"]["sidecar_ram_and_init_admission_survive_snapshot_and_controller_recovery"] = True

    kubectl("exec", name, "-c", "app", "--", "touch", "/shared/finish")
    finished = wait("Job completes after reverse sidecar shutdown", lambda:
        resource("pod", name) if resource("pod", name).get("status", {}).get("phase") == "Succeeded" else None, 180)
    assert all("terminated" in item["state"] for item in finished["status"]["initContainerStatuses"])
    assert "shutdown-order:logger,proxy," in kubectl("logs", name, "-c", "proxy")
    assert "logger-stopped" in kubectl("logs", name, "-c", "logger")
    wait("Kubernetes Job controller observes completion", lambda: any(
        condition["type"] == "Complete" and condition["status"] == "True"
        for condition in resource("job", job_name).get("status", {}).get("conditions", [])))
    wait("finished sidecar Job releases RAM", lambda: control_get("health")["activeMemoryMiB"] == 0)
    report["checks"]["sidecars_stop_in_reverse_order_and_allow_job_completion"] = True
    kubectl("delete", "job", job_name, "--wait=true", timeout=180)
