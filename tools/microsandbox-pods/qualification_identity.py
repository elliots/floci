"""Real API TLS, Pod-bound token audiences, RBAC and identity refresh across RAM restore."""
import json
import os
from pathlib import Path
import platform
import subprocess
import urllib.request


def qualify_identity(*, peer, temporary, pod, apply, resource, kubectl, ready,
                     lifecycle, control_get, crash_and_restart, wait, command, report):
    name = "native-identity"
    binary = temporary / "qualification-identity"
    architecture = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64"}[platform.machine()]
    command("go", "build", "-o", str(binary), "./qualification/identityserver",
        cwd=Path(__file__).parent,
        env=dict(os.environ, GOOS="linux", GOARCH=architecture, CGO_ENABLED="0"), timeout=300)
    apply({"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": name}})
    apply({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": name+"-config"}, "data": {"value": "authorized"}})
    apply({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": {"name": name}, "rules": [
        {"apiGroups": [""], "resources": ["configmaps"], "resourceNames": [name+"-config"], "verbs": ["get"]},
        {"apiGroups": [""], "resources": ["pods"], "resourceNames": [name], "verbs": ["get"]}]})
    apply({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": {"name": name},
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": name},
        "subjects": [{"kind": "ServiceAccount", "name": name, "namespace": "default"}]})
    value = pod(name, "until test -x /tmp/qualification-identity; do sleep .1; done; exec /tmp/qualification-identity")
    value["metadata"]["annotations"] = {"microsandbox.dev/availability": "proxy", "microsandbox.dev/idle-owner": "external"}
    value["metadata"]["labels"]["identity-version"] = "initial"
    del value["spec"]["automountServiceAccountToken"]
    value["spec"]["serviceAccountName"] = name
    value["spec"]["volumes"] = [
        {"name": "audience", "projected": {"sources": [{"serviceAccountToken": {
            "path": "token", "audience": "qualification.invalid", "expirationSeconds": 600}}]}},
        {"name": "metadata", "projected": {"sources": [{"downwardAPI": {"items": [
            {"path": "label", "fieldRef": {"fieldPath": "metadata.labels['identity-version']"}}]}}]}}]
    app = value["spec"]["containers"][0]
    app["ports"] = [{"containerPort": 8080}]
    app["volumeMounts"] = [{"name": "audience", "mountPath": "/audience", "readOnly": True},
        {"name": "metadata", "mountPath": "/metadata", "readOnly": True}]
    app["readinessProbe"] = {"tcpSocket": {"port": 8080}, "periodSeconds": 1}
    apply(value)
    observed = wait("identity proxy Pod declared", lambda: ready(name))
    uid = observed["metadata"]["uid"]
    assert any("projected" in volume and any("serviceAccountToken" in source for source in volume["projected"]["sources"])
        and volume["name"].startswith("kube-api-access-") for volume in observed["spec"]["volumes"])
    state = lifecycle(uid)
    assert state["state"] == "COLD" and control_get("health")["activeMemoryMiB"] == 0
    lifecycle(uid, "resume", state["generation"])
    wait("identity container running", lambda: any("running" in item.get("state", {})
        for item in resource("pod", name).get("status", {}).get("containerStatuses", [])), 360)
    with binary.open("rb") as source:
        result = subprocess.run(["docker", "exec", "-i", peer, "kubectl", "exec", "-i", name, "--", "sh", "-ec",
            "cat >/tmp/qualification-identity.pending; chmod 700 /tmp/qualification-identity.pending; "
            "mv /tmp/qualification-identity.pending /tmp/qualification-identity"], stdin=source,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
    assert result.returncode == 0, result.stderr[-3000:]

    def identity():
        with urllib.request.urlopen(lifecycle(uid)["proxyUrl"], timeout=180) as response:
            result = json.load(response)
        assert result["uid"] == uid and result["authorized"] == 200 and result["denied"] == 403 and result["wrongAudience"] == 401, result
        return result

    initial = wait("mounted token authenticates over verified Kubernetes TLS", identity)
    assert initial["label"] == "initial"
    report["checks"]["default_automount_pod_token_api_tls_rbac_and_audience"] = True
    kubectl("label", "pod", name, "identity-version=updated", "--overwrite")
    wait("DownwardAPI updates current labels", lambda: identity()["label"] == "updated", 90)
    report["checks"]["projected_downward_metadata_refreshes_while_awake"] = True
    wait("identity request releases proxy activity", lambda: lifecycle(uid)["activeConnections"] == 0)
    state = lifecycle(uid)
    lifecycle(uid, "hibernate", state["generation"])
    assert control_get("health")["activeMemoryMiB"] == 0
    crash_and_restart()
    assert lifecycle(uid)["state"] == "SLEEPING"
    restored = identity()
    assert restored["marker"] == initial["marker"] and restored["tokenHash"] != initial["tokenHash"], restored
    report["checks"]["identity_refreshed_before_forwarded_wake_with_process_ram_preserved"] = True
    crash_and_restart()
    recovered = wait("running controller recovery reissues Pod identity", identity)
    assert recovered["marker"] == initial["marker"] and recovered["tokenHash"] != restored["tokenHash"]
    report["checks"]["running_controller_recovery_refreshes_identity_before_readiness"] = True
    kubectl("delete", "pod", name, "--wait=true", timeout=180)
    wait("identity fixture releases RAM", lambda: control_get("health")["activeMemoryMiB"] == 0)
    for kind, resource_name in [("rolebinding", name), ("role", name), ("serviceaccount", name), ("configmap", name+"-config")]:
        kubectl("delete", kind, resource_name)
