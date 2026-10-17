"""Shared PVC persistence/reclaim contracts, plus native RAM and attachment fencing."""
import json


def qualify_persistent(*, pod, apply, resource, kubectl, ready, lifecycle,
                       control_get, crash_and_restart, wait, command, msb,
                       environment, report):
    classes, claims, pods, volumes = set(), set(), set(), set()
    original_error = None

    def get_optional(kind, name):
        raw = kubectl("get", kind, name, "--ignore-not-found", "-o", "json")
        return json.loads(raw) if raw else None

    def delete_pod(name):
        kubectl("delete", "pod", name, "--ignore-not-found", "--wait=true", timeout=180)
        pods.discard(name)

    def marker(name):
        return kubectl("exec", name, "--", "sh", "-ec",
                       "cat /data/value; tr '\\000' '\\n' </proc/1/environ | grep '^PVC_RAM_MARKER='")

    def workload(name, claim, native, completed=False):
        value = pod(name, "test -s /data/value || cat /proc/sys/kernel/random/uuid >/data/value; "
                    "export PVC_RAM_MARKER=$(cat /proc/sys/kernel/random/uuid); exec sleep 2147483647", native=native)
        if completed:
            value["spec"]["containers"][0]["command"] = ["sh", "-ec", "echo job-write >/data/job-result"]
            value["spec"]["containers"][0].pop("args", None)
        value["spec"]["securityContext"] = {"runAsUser": 1000, "runAsGroup": 2000,
            "fsGroup": 3000, "fsGroupChangePolicy": "OnRootMismatch"}

        value["spec"]["terminationGracePeriodSeconds"] = 2
        value["spec"]["volumes"] = [{"name": "data", "persistentVolumeClaim": {"claimName": claim}}]
        value["spec"]["containers"][0]["volumeMounts"] = [{"name": "data", "mountPath": "/data"}]
        value["spec"]["containers"][0]["readinessProbe"] = {
            "exec": {"command": ["test", "-s", "/data/value"]}, "periodSeconds": 1}
        if completed:
            value["spec"]["containers"][0].pop("readinessProbe", None)
        pods.add(name)
        apply(value)

    def claim(name, storage_class, volume=None):
        value = {"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": {"name": name},
            "spec": {"storageClassName": storage_class, "accessModes": ["ReadWriteOnce"],
                     "volumeMode": "Filesystem", "resources": {"requests": {"storage": "128Mi"}}}}
        if volume:
            value["spec"]["volumeName"] = volume
        claims.add(name)
        apply(value)

    def native_disks():
        return json.loads(command(msb, "volume", "ls", "--format", "json", env=environment))

    def disk_present(name):
        return any(item["name"] == name for item in native_disks())

    try:
        for native in (False, True):
            backend = "native" if native else "docker"
            for policy in ("Delete", "Retain"):
                stem = f"persistent-{backend}-{policy.lower()}"
                storage_class, initial_claim, initial_pod = stem, stem, stem+"-first"
                classes.add(storage_class)
                apply({"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
                    "metadata": {"name": storage_class}, "provisioner": "microsandbox.dev/local" if native else "rancher.io/local-path",
                    "reclaimPolicy": policy, "volumeBindingMode": "WaitForFirstConsumer", "allowVolumeExpansion": False})
                claim(initial_claim, storage_class)
                assert resource("pvc", initial_claim)["status"]["phase"] == "Pending", "WFFC claim bound before any consumer"
                workload(initial_pod, initial_claim, native)
                observed = wait(stem+" initial Pod Ready", lambda: ready(initial_pod), 480)
                bound = resource("pvc", initial_claim)
                assert bound["status"]["phase"] == "Bound", bound
                volume_name = bound["spec"]["volumeName"]
                volumes.add(volume_name)
                pv = resource("pv", volume_name)
                pv_uid = pv["metadata"]["uid"]
                assert pv["spec"]["claimRef"]["uid"] == bound["metadata"]["uid"], "PV lost exact claim identity"
                before = marker(initial_pod).splitlines()
                assert len(before) == 2, before
                if native:
                    assert kubectl("exec", initial_pod, "--", "stat", "-c", "%g", "/data/value") == "3000", "fsGroup not applied"
                    assert pv["spec"]["capacity"]["storage"] == "256Mi", "PV did not advertise supported minimum disk capacity"
                    assert pv["spec"]["local"]["path"] == "/var/lib/floci/persistent-volumes/"+volume_name, pv
                    assert disk_present(volume_name), "PV has no named native disk"
                    uid = observed["metadata"]["uid"]
                    state = lifecycle(uid)
                    lifecycle(uid, "hibernate", state["generation"])
                    assert lifecycle(uid)["state"] == "SLEEPING", "native Pod did not preserve a sleeping checkpoint"
                    assert control_get("health")["activeMemoryMiB"] == 0, "sleeping disk retained guest RAM"
                    if policy == "Delete":
                        contender = stem+"-contender"
                        workload(contender, initial_claim, True)
                        candidate = wait("second writer waits for sleeping owner", lambda:
                            (value if value.get("status", {}).get("reason") == "FailedMount" else None)
                            if (value := resource("pod", contender)) else None, 90)
                        candidate_uid = candidate["metadata"]["uid"]
                        assert lifecycle(candidate_uid)["state"] == "WAITINGVOLUMES", candidate
                        guest_name = "fvp-"+candidate_uid.replace("-", "")[:20]
                        assert not any(item["name"] == guest_name for item in json.loads(command(msb, "ls", "--format", "json", env=environment))), "conflicting writer allocated a VM"
                        crash_and_restart()
                        assert lifecycle(candidate_uid)["state"] == "WAITINGVOLUMES", "contending Pod blocked recovery or lost pending state"
                        delete_pod(contender)
                        report["checks"]["native_sleeping_pvc_rejects_second_writer_without_guest"] = True
                    else:
                        crash_and_restart()
                    state = lifecycle(uid)
                    lifecycle(uid, "resume", state["generation"])
                    wait(stem+" restored Pod Ready", lambda: ready(initial_pod), 180)
                    restored = marker(initial_pod).splitlines()
                    report.setdefault("persistent_observations", []).append({"case": stem, "phase": "ram-restore", "before": before, "after": restored, "pvUID": pv_uid, "volume": volume_name, "disk": native_disks()})
                    assert restored == before, f"RAM restore changed filesystem data or process marker: {before!r} -> {restored!r}"
                    report["checks"][f"native_pvc_{policy.lower()}_ram_restore_controller_recovery"] = True
                    if policy == "Retain":
                        state = lifecycle(uid)
                        lifecycle(uid, "hibernate", state["generation"])
                        assert lifecycle(uid)["state"] == "SLEEPING", "replacement case did not delete a sleeping PVC owner"
                delete_pod(initial_pod)
                replacement = stem+"-replacement"
                workload(replacement, initial_claim, native)
                wait(stem+" replacement Pod Ready", lambda: ready(replacement), 480)
                after = marker(replacement).splitlines()
                report.setdefault("persistent_observations", []).append({"case": stem, "phase": "replacement", "before": before, "after": after, "pvUID": pv_uid, "pv": resource("pv", volume_name), "disk": native_disks() if native else None})
                assert len(after) == 2 and after[0] == before[0] and after[1] != before[1], f"replacement data/RAM mismatch: {before!r} -> {after!r}; PV {pv_uid}, disk {volume_name}"
                kubectl("exec", replacement, "--", "sh", "-ec", "echo replacement-write >>/data/value")
                delete_pod(replacement)
                completed = stem+"-job"
                workload(completed, initial_claim, native, completed=True)
                wait(stem+" completed writer", lambda:
                    (value if value.get("status", {}).get("phase") == "Succeeded" else None)
                    if (value := resource("pod", completed)) else None, 480)
                if native:
                    completed_uid = resource("pod", completed)["metadata"]["uid"]
                    assert lifecycle(completed_uid)["state"] == "TERMINATED", "Job published success before verified terminal storage stop"
                delete_pod(completed)
                reader = stem+"-job-reader"
                workload(reader, initial_claim, native)
                wait(stem+" completed writer replacement", lambda: ready(reader), 480)
                assert kubectl("exec", reader, "--", "cat", "/data/job-result") == "job-write", "completed Job lost its unsynced filesystem write"
                delete_pod(reader)
                report["checks"][f"{backend}_pvc_{policy.lower()}_completed_job_writeback"] = True
                kubectl("delete", "pvc", initial_claim, "--wait=true", timeout=120)
                claims.discard(initial_claim)
                if policy == "Retain":
                    released = wait(stem+" PV Released", lambda:
                        (value if value.get("status", {}).get("phase") == "Released" else None)
                        if (value := resource("pv", volume_name)) else None)
                    assert released["metadata"]["uid"] == pv_uid, "Retain replaced original PV"
                    if native:
                        assert disk_present(volume_name), "Retain deleted native filesystem"
                    kubectl("patch", "pv", volume_name, "--type=merge", "-p", '{"spec":{"claimRef":null}}')
                    rebound = stem+"-rebound"
                    claim(rebound, storage_class, volume_name)
                    workload(rebound, rebound, native)
                    wait(stem+" manually rebound Pod Ready", lambda: ready(rebound), 480)
                    assert kubectl("exec", rebound, "--", "cat", "/data/value") == before[0]+"\nreplacement-write", "manual same-PV rebind lost data"
                    assert resource("pv", volume_name)["metadata"]["uid"] == pv_uid, "rebind created another PV"
                    delete_pod(rebound)
                    kubectl("patch", "pv", volume_name, "--type=merge", "-p", '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}')
                    kubectl("delete", "pvc", rebound, "--wait=true", timeout=120)
                    claims.discard(rebound)
                wait(stem+" Delete reclaim removes PV", lambda: get_optional("pv", volume_name) is None, 180)
                if native:
                    wait(stem+" Delete reclaim removes named disk", lambda: not disk_present(volume_name), 90)
                volumes.discard(volume_name)
                report["checks"][f"{backend}_pvc_{policy.lower()}_replacement_and_reclaim"] = True
                kubectl("delete", "storageclass", storage_class)
                classes.discard(storage_class)
    except BaseException as error:
        original_error = error
        raise
    finally:
        failures = []
        for name in sorted(pods):
            try:
                kubectl("delete", "pod", name, "--ignore-not-found", "--wait=true", timeout=180)
            except Exception as error:
                failures.append(str(error))
        # Resolve names from exact owned PVCs as a failed startup may precede
        # recording the bound PV. Never touch an unrelated volume or disk.
        for name in sorted(claims):
            try:
                value = get_optional("pvc", name)
                if value and value["spec"].get("volumeName"):
                    volumes.add(value["spec"]["volumeName"])
            except Exception as error:
                failures.append(str(error))
        for name in sorted(volumes):
            try:
                if get_optional("pv", name):
                    kubectl("patch", "pv", name, "--type=merge", "-p", '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}')
            except Exception as error:
                failures.append(str(error))
        for name in sorted(claims):
            try:
                kubectl("delete", "pvc", name, "--ignore-not-found", "--wait=true", timeout=120)
            except Exception as error:
                failures.append(str(error))
        for name in sorted(volumes):
            try:
                wait("fixture PV cleanup "+name, lambda: get_optional("pv", name) is None, 180)
                if name.startswith("floci-pvc-"):
                    wait("fixture native disk cleanup "+name, lambda: not disk_present(name), 90)
            except Exception as error:
                failures.append(str(error))
        for name in sorted(classes):
            try:
                kubectl("delete", "storageclass", name, "--ignore-not-found")
            except Exception as error:
                failures.append(str(error))
        report["persistent_fixture_cleanup"] = failures or "passed"
        if failures:
            raise RuntimeError("PVC fixture cleanup failed: "+"; ".join(failures)) from original_error
