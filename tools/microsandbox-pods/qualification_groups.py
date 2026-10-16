"""The same Kubernetes group and volume assertions run on Docker and native pods."""


def qualify_groups(*, pod, apply, resource, kubectl, ready, lifecycle,
                   control_get, crash_and_restart, wait, report):
    image = "registry.k8s.io/e2e-test-images/agnhost:2.45"
    for native in (False, True):
        backend = "native" if native else "docker"
        for policy in ("Merge", "Strict"):
            name = f"groups-{backend}-{policy.lower()}"
            secret = name + "-secret"
            apply({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": secret},
                   "stringData": {"value": "initial"}})
            value = pod(name, "unused", native=native)
            value["spec"]["securityContext"] = {
                "runAsUser": 1000, "runAsGroup": 2000, "fsGroup": 3000,
                "supplementalGroups": [4000], "supplementalGroupsPolicy": policy,
                "fsGroupChangePolicy": "OnRootMismatch"}
            value["spec"]["volumes"] = [
                {"name": "disk", "emptyDir": {}},
                {"name": "memory", "emptyDir": {"medium": "Memory"}},
                {"name": "secret", "secret": {"secretName": secret, "defaultMode": 0o400}}]
            mounts = [{"name": name, "mountPath": "/"+name} for name in ("disk", "memory", "secret")]
            expected = {2000, 3000, 4000} | ({50000} if policy == "Merge" else set())
            group_check = "test \"$(id -u)\" = 1000; test \"$(id -g)\" = 2000; test -r /secret/value; "
            def container(name, script):
                return {"name": name, "image": image, "command": ["sh", "-ec", group_check+script],
                        "volumeMounts": mounts,
                        "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}}}
            value["spec"]["initContainers"] = [
                container("initialize", "id -G > /disk/init-groups; echo initialized > /memory/initialized"),
                dict(container("sidecar", "id -G >/disk/sidecar-groups; exec sleep 2147483647"), restartPolicy="Always")]
            value["spec"]["containers"] = [container("app",
                "test -f /memory/initialized; id -G >/disk/app-groups; "
                "export GROUP_MARKER=$(cat /proc/sys/kernel/random/uuid); exec sleep 2147483647")]
            value["spec"]["containers"][0]["readinessProbe"] = {
                "exec": {"command": ["sh", "-ec", group_check+"id -G > /disk/probe-groups"]}, "periodSeconds": 1}
            apply(value)
            observed = wait(name+" Ready", lambda: ready(name), 480)
            uid = observed["metadata"]["uid"]

            def check_identity():
                for target in ("app", "sidecar"):
                    actual = set(map(int, kubectl("exec", name, "-c", target, "--", "id", "-G").split()))
                    assert actual == expected, (name, target, actual, expected)
                for source in ("init", "sidecar", "app", "probe"):
                    actual = set(map(int, kubectl("exec", name, "-c", "app", "--", "cat", "/disk/"+source+"-groups").split()))
                    assert actual == expected, (name, source, actual, expected)
                for path in ("/disk/app-groups", "/memory/initialized", "/secret/value"):
                    assert kubectl("exec", name, "-c", "app", "--", "stat", "-Lc", "%g", path) == "3000"
                assert kubectl("exec", name, "-c", "app", "--", "stat", "-Lc", "%a", "/secret/value") == "440"
                status = resource("pod", name)["status"]
                for item in status["containerStatuses"] + status["initContainerStatuses"]:
                    identity = item["user"]["linux"]
                    assert identity["uid"] == 1000 and identity["gid"] == 2000, identity
                    assert set(identity["supplementalGroups"]) == expected, identity
                return True
            wait(name+" initial identity and volume permissions", check_identity, 90)
            apply({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": secret},
                   "stringData": {"value": "rotated"}})
            wait(name+" readable projection rotation", lambda: kubectl("exec", name, "-c", "app", "--", "cat", "/secret/value") == "rotated", 150)
            check_identity()
            report["checks"][f"{backend}_{policy.lower()}_groups_init_sidecar_exec_probe_volumes_rotation"] = True
            if native:
                def marker():
                    return kubectl("exec", name, "-c", "app", "--", "sh", "-ec",
                        "tr '\\000' '\\n' </proc/1/environ | grep '^GROUP_MARKER='")
                before = marker()
                state = lifecycle(uid)
                lifecycle(uid, "hibernate", state["generation"])
                assert control_get("health")["activeMemoryMiB"] == 0
                crash_and_restart()
                state = lifecycle(uid)
                lifecycle(uid, "resume", state["generation"])
                wait(name+" restored Ready", lambda: ready(name))
                assert marker() == before
                check_identity()
                report["checks"][f"native_{policy.lower()}_groups_survive_ram_restore_and_recovery"] = True
            kubectl("delete", "pod", name, "--wait=true", timeout=180)
            kubectl("delete", "secret", secret)
            if native:
                wait(name+" releases memory", lambda: control_get("health")["activeMemoryMiB"] == 0)
