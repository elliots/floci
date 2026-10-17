#!/usr/bin/env python3
"""Prepare and inspect an owned local cluster for the shared native pod controller."""
import argparse
import base64
import collections
import contextlib
import fcntl
import ipaddress
import json
import os
from pathlib import Path
import platform
import secrets
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.request


LABEL = "io.floci.pod-probe"
K3S_IMAGE = "rancher/k3s:v1.35.0-k3s1"
ROOT = Path(__file__).resolve().parents[2]


def command(*args, input_text=None, timeout=120, environment=None, cwd=None):
    result = subprocess.run([str(value) for value in args], input=input_text,
        capture_output=True, text=True, timeout=timeout, env=environment, cwd=cwd)
    if result.returncode:
        # Input may contain certificate authority keys or kubeconfig credentials.
        raise RuntimeError(f"{Path(args[0]).name} failed (exit {result.returncode}): {result.stderr[-2000:]}")
    return result.stdout.strip()


def write_private(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def wait(description, operation, timeout=300):
    deadline = time.monotonic() + timeout
    last = "not ready"
    while time.monotonic() < deadline:
        try:
            if result := operation():
                return result
        except (RuntimeError, OSError, ValueError) as error:
            last = str(error)
        time.sleep(1)
    raise RuntimeError(description+": "+last)


def load(directory):
    directory = Path(directory).resolve()
    record = json.loads((directory / "cluster.json").read_text())
    if record.get("version") != 1 or record.get("directory") != str(directory):
        raise RuntimeError("cluster record belongs to another directory or format")
    return record


@contextlib.contextmanager
def setup_lock(directory):
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if directory.stat().st_mode & 0o077:
        raise RuntimeError("cluster directory must be private (mode 0700)")
    descriptor = os.open(directory / ".manage.lock", os.O_CREAT | os.O_RDWR, 0o600)
    with os.fdopen(descriptor, "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("another cluster setup or cleanup is in progress") from error
        yield lock.fileno()


def controller_running(record):
    try:
        with socket.create_connection(("127.0.0.1", record["controlPort"]), timeout=2):
            return True
    except ConnectionRefusedError:
        return False


def inspect_peer(record):
    result = command("docker", "container", "ls", "-a", "--filter", "name=^/"+record["peer"]+"$", "--format", "{{.ID}}")
    if not result:
        return None
    values = json.loads(command("docker", "inspect", record["peer"]))
    if len(values) != 1 or values[0]["Config"].get("Labels", {}).get(LABEL) != record["owner"]:
        raise RuntimeError("network peer ownership changed; refusing to adopt or modify it")
    if values[0]["Config"]["Image"] != K3S_IMAGE:
        raise RuntimeError("network peer image differs from the recorded pinned cluster version")
    return values[0]


def kubectl(record, *args, **kwargs):
    return command("docker", "exec", "-i", record["peer"], "kubectl", "--request-timeout=20s", *args, **kwargs)


def apply(record, value):
    kubectl(record, "apply", "-f", "-", input_text=json.dumps(value))


def access_resources(record):
    name, user, node = record["accessName"], record["user"], record["node"]
    metadata = {"name": name, "labels": {LABEL: record["owner"]}}
    rules = [
        {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "watch", "delete"]},
        {"apiGroups": [""], "resources": ["pods/status"], "verbs": ["get", "patch", "update"]},
        {"apiGroups": [""], "resources": ["configmaps", "secrets", "services"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": [""], "resources": ["serviceaccounts"], "verbs": ["get"]},
        {"apiGroups": [""], "resources": ["serviceaccounts/token"], "verbs": ["create"]},
        {"apiGroups": [""], "resources": ["namespaces"], "resourceNames": ["kube-system"], "verbs": ["get"]},
        {"apiGroups": [""], "resources": ["nodes", "nodes/status"], "resourceNames": [node], "verbs": ["get", "patch", "update"]},
        {"apiGroups": [""], "resources": ["events"], "verbs": ["create", "patch", "update"]},
        {"apiGroups": ["apps"], "resources": ["deployments", "replicasets"], "verbs": ["get", "list"]},
        {"apiGroups": ["networking.k8s.io"], "resources": ["networkpolicies"], "verbs": ["get", "list"]},
        {"apiGroups": ["authentication.k8s.io"], "resources": ["tokenreviews"], "verbs": ["create"]},
        {"apiGroups": ["authorization.k8s.io"], "resources": ["subjectaccessreviews"], "verbs": ["create"]},
    ]
    subject = [{"kind": "User", "name": user, "apiGroup": "rbac.authorization.k8s.io"}]
    return {"apiVersion": "v1", "kind": "List", "items": [
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": metadata, "rules": rules},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding", "metadata": metadata,
         "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": name}, "subjects": subject},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": dict(metadata, namespace="kube-node-lease"),
         "rules": [
             {"apiGroups": ["coordination.k8s.io"], "resources": ["leases"], "verbs": ["create"]},
             {"apiGroups": ["coordination.k8s.io"], "resources": ["leases"], "resourceNames": [node], "verbs": ["get", "patch", "update"]}]},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": dict(metadata, namespace="kube-node-lease"),
         "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": name}, "subjects": subject},
    ]}


def certificate(record, name, ca_name, common_name, usage, host=None):
    directory = Path(record["directory"])
    with tempfile.TemporaryDirectory(prefix=".certificate-", dir=directory) as temporary:
        temporary = Path(temporary)
        for extension in ("crt", "key"):
            write_private(temporary / ("ca."+extension), command("docker", "exec", record["peer"],
                "cat", "/var/lib/rancher/k3s/server/tls/"+ca_name+"."+extension)+"\n")
        extensions = "extendedKeyUsage="+usage+"\n"
        if host:
            extensions += "subjectAltName=IP:"+str(ipaddress.ip_address(host))+",IP:127.0.0.1\n"
        write_private(temporary / "extensions", extensions)
        command("openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout", temporary/"key",
            "-out", temporary/"request", "-subj", "/CN="+common_name)
        command("openssl", "x509", "-req", "-in", temporary/"request", "-CA", temporary/"ca.crt",
            "-CAkey", temporary/"ca.key", "-set_serial", "0x"+secrets.token_hex(16),
            "-out", temporary/"certificate", "-days", "30", "-extfile", temporary/"extensions")
        write_private(directory / (name+".key"), (temporary/"key").read_text())
        write_private(directory / (name+".crt"), (temporary/"certificate").read_text())


def node_address(record, supplied=None):
    host = supplied or record.get("hostIP")
    if not host:
        hosts = command("docker", "exec", record["peer"], "cat", "/etc/hosts")
        for line in hosts.splitlines():
            fields = line.split("#", 1)[0].split()
            if len(fields) > 1 and "host.docker.internal" in fields[1:]:
                host = fields[0]
                break
    if not host:
        try:
            host = command("docker", "exec", record["peer"], "getent", "hosts", "host.docker.internal").split()[0]
        except (RuntimeError, IndexError):
            host = command("docker", "exec", record["peer"], "sh", "-c",
                "nslookup host.docker.internal | awk '/^Address: / {print $2}' | tail -n1")
    address = ipaddress.IPv4Address(host)
    if address.is_unspecified or address.is_multicast or address.is_loopback or host == "255.255.255.255":
        raise ValueError("node-ip must be an explicit unicast host IPv4 address reachable from the cluster")
    if record.get("hostIP") and host != record["hostIP"]:
        raise RuntimeError("node-ip differs from the recorded cluster routing identity; reuse its original address")
    return str(address)


def prepare(args):
    directory = Path(args.directory).resolve()
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if directory.stat().st_mode & 0o077:
        raise RuntimeError("cluster directory must be private (mode 0700)")
    record_path = directory / "cluster.json"
    if record_path.exists():
        record = load(directory)
        if controller_running(record):
            raise RuntimeError("stop the controller before renewing its cluster credentials")
        for option in ("binaries", "msb", "msb_home"):
            if str(Path(getattr(args, option)).resolve()) != record[option]:
                raise RuntimeError(option+" differs from the recorded cluster; reuse its original paths")
    else:
        if any(path.name != ".manage.lock" for path in directory.iterdir()):
            raise RuntimeError("new cluster directory must be empty")
        owner = secrets.token_hex(8)
        kubelet_port, control_port = free_port(), free_port()
        while control_port == kubelet_port:
            control_port = free_port()
        record = {"version": 1, "directory": str(directory), "owner": owner,
            "peer": "floci-native-"+owner, "node": "native-pods", "user": "floci:microsandbox:"+owner,
            "accessName": "floci-native-"+owner, "kubeletPort": kubelet_port, "controlPort": control_port}
        record.update({option: str(Path(getattr(args, option)).resolve()) for option in ("binaries", "msb", "msb_home")})
        write_private(record_path, json.dumps(record, indent=2)+"\n")
    for name in ("microsandbox-pods", "network-relay", "guestfs"):
        if not os.access(Path(record["binaries"]) / name, os.X_OK):
            raise RuntimeError("missing executable "+name+"; run the build command first")
    version = command(record["msb"], "--version")
    if version.split()[-1] != "0.7.8":
        raise RuntimeError("native microsandbox 0.7.8 is required")
    peer = inspect_peer(record)
    if peer is None:
        if (directory / "controller" / "owner").exists():
            raise RuntimeError("recorded network peer is missing; refusing to replace a cluster with workload state")
        linux_host = ["--add-host", "host.docker.internal:host-gateway"] if platform.system() == "Linux" and not args.node_ip else []
        command("docker", "run", "-d", "--name", record["peer"], "--hostname", record["peer"], *linux_host,
            "--label", LABEL+"="+record["owner"], "--privileged", "-p", "127.0.0.1::6443", "-p", "127.0.0.1::8472/udp",
            K3S_IMAGE, "server", "--disable=traefik", "--disable=servicelb", "--disable=metrics-server",
            "--egress-selector-mode=disabled", timeout=600)
    elif not peer["State"]["Running"]:
        command("docker", "start", record["peer"])
    wait("Kubernetes API ready", lambda: kubectl(record, "get", "--raw=/readyz") == "ok")
    peer = inspect_peer(record)
    api_port = peer["NetworkSettings"]["Ports"]["6443/tcp"][0]["HostPort"]
    host = node_address(record, args.node_ip)
    record["hostIP"] = host
    admin = command("docker", "exec", record["peer"], "cat", "/etc/rancher/k3s/k3s.yaml")
    write_private(directory / "admin.kubeconfig", admin.replace("https://127.0.0.1:6443", "https://127.0.0.1:"+api_port)+"\n")
    certificate(record, "kubelet", "server-ca", record["node"], "serverAuth", host)
    certificate(record, "controller", "client-ca", record["user"], "clientAuth")
    client_ca = command("docker", "exec", record["peer"], "cat", "/var/lib/rancher/k3s/server/tls/client-ca.crt")
    server_ca = command("docker", "exec", record["peer"], "cat", "/var/lib/rancher/k3s/server/tls/server-ca.crt")
    write_private(directory / "client-ca.crt", client_ca+"\n")
    encode = lambda data: base64.b64encode(data.encode()).decode()
    kubeconfig = {"apiVersion": "v1", "kind": "Config", "current-context": "native",
        "clusters": [{"name": "native", "cluster": {"server": "https://127.0.0.1:"+api_port, "certificate-authority-data": encode(server_ca)}}],
        "contexts": [{"name": "native", "context": {"cluster": "native", "user": "controller"}}],
        "users": [{"name": "controller", "user": {"client-certificate-data": encode((directory/"controller.crt").read_text()),
            "client-key-data": encode((directory/"controller.key").read_text())}}]}
    write_private(directory / "controller.kubeconfig", json.dumps(kubeconfig, indent=2)+"\n")
    resources = access_resources(record)
    write_private(directory / "controller-rbac.json", json.dumps(resources, indent=2)+"\n")
    apply(record, resources)
    existing_node = kubectl(record, "get", "node", record["node"], "--ignore-not-found", "-o", "json")
    if existing_node and json.loads(existing_node)["metadata"].get("labels", {}).get(LABEL) != record["owner"]:
        raise RuntimeError("virtual node ownership changed; refusing to replace it")
    apply(record, {"apiVersion": "v1", "kind": "Node", "metadata": {"name": record["node"], "labels": {
        LABEL: record["owner"], "kubernetes.io/hostname": record["node"], "kubernetes.io/os": "linux",
        "kubernetes.io/arch": architecture(), "microsandbox.dev/runtime": "native"}},
        "spec": {"podCIDR": "10.42.200.0/22", "podCIDRs": ["10.42.200.0/22"], "taints": [
            {"key": "microsandbox.dev/runtime", "value": "native", "effect": "NoSchedule"}]}})
    if not (directory / "control-token").exists():
        write_private(directory / "control-token", secrets.token_hex(32)+"\n")
    write_private(record_path, json.dumps(record, indent=2)+"\n")
    print("Prepared native cluster. Workload kubeconfig: "+str(directory / "admin.kubeconfig"))
    print("Run the controller with: python3 "+str(Path(__file__).resolve())+" run --directory "+str(directory))


def architecture():
    names = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64", "amd64": "amd64"}
    if platform.machine().lower() not in names:
        raise RuntimeError("native runtime requires an arm64 or amd64 host")
    return names[platform.machine().lower()]


def build(args):
    output = Path(args.binaries).resolve()
    output.mkdir(parents=True, exist_ok=True)
    for module, package, name, guest in [
        ("microsandbox-pods", ".", "microsandbox-pods", False),
        ("microsandbox-host", ".", "microsandbox-host", False),
        ("microsandbox-pods", "./cmd/network-relay", "network-relay", True),
        ("microsandbox-runtime", "./cmd/guestfs", "guestfs", True),
        ("microsandbox-runtime", "./cmd/guestnet", "guestnet", True),
    ]:
        environment = dict(os.environ)
        environment.update(GOOS={"Darwin": "darwin", "Linux": "linux"}[platform.system()], GOARCH=architecture(), CGO_ENABLED="1")
        if guest:
            environment.update(GOOS="linux", GOARCH=architecture(), CGO_ENABLED="0")
        print("Building "+name, flush=True)
        command("go", "build", "-o", output/name, package, cwd=ROOT/"tools"/module, environment=environment, timeout=600)
    print("Built native runtime helpers in "+str(output))


def controller_arguments(record, args):
    directory, binaries = Path(record["directory"]), Path(record["binaries"])
    return [str(binaries/"microsandbox-pods"), "--kubeconfig", str(directory/"controller.kubeconfig"),
        "--node", record["node"], "--msb", record["msb"], "--msb-home", record["msb_home"],
        "--state", str(directory/"controller"), "--network-peer", record["peer"], "--peer-owner", record["owner"],
        "--network-relay-binary", str(binaries/"network-relay"), "--guest-fs-binary", str(binaries/"guestfs"),
        "--listen", "0.0.0.0:"+str(record["kubeletPort"]), "--node-ip", record["hostIP"],
        "--tls-cert", str(directory/"kubelet.crt"), "--tls-key", str(directory/"kubelet.key"),
        "--client-ca", str(directory/"client-ca.crt"), "--control-listen", "127.0.0.1:"+str(record["controlPort"]),
        "--control-token-file", str(directory/"control-token"), "--cluster-name", record["peer"],
        "--max-active-memory-mib", str(args.memory_mib), "--scheduling-cpu", args.scheduling_cpu,
        "--scheduling-memory", args.scheduling_memory, "--pod-workers", str(args.pod_workers)]


def status(args):
    record = load(args.directory)
    peer = inspect_peer(record)
    directory = Path(record["directory"])
    report = {"peer": record["peer"], "peerRunning": bool(peer and peer["State"]["Running"]),
        "controller": "unavailable", "runtimeHome": record["msb_home"],
        "filesystemFreeBytes": shutil.disk_usage(record["msb_home"]).free}
    states = collections.Counter()
    for path in (directory/"controller").glob("*.json"):
        value = json.loads(path.read_text())
        states[value["view"]["state"]] += 1
    report["journalStates"] = dict(states)
    report["pendingNativeOperation"] = (directory/"controller"/"native-command.pending").exists()
    try:
        token = (directory/"control-token").read_text().strip()
        request = urllib.request.Request("http://127.0.0.1:"+str(record["controlPort"])+"/v1/health",
            headers={"Authorization": "Bearer "+token})
        with urllib.request.urlopen(request, timeout=5) as response:
            report["controller"] = json.load(response)
    except OSError:
        pass  # An absent controller is a normal diagnostic result, not proof of guest failure.
    report["sharedCapacity"] = "unavailable"
    if isinstance(report["controller"], dict):
        request = urllib.request.Request("http://127.0.0.1:"+str(record["controlPort"])+"/v1/capacity",
            headers={"Authorization": "Bearer "+token})
        try:
            with urllib.request.urlopen(request, timeout=35) as response:
                report["sharedCapacity"] = json.load(response)
        except OSError:
            pass  # An unavailable inventory must not be reported as zero usage.
    print(json.dumps(report, indent=2))


def destroy(args):
    record = load(args.directory)
    directory = Path(record["directory"])
    if list((directory/"controller").glob("*.json")) or (directory/"controller"/"native-command.pending").exists():
        raise RuntimeError("controller still owns workload or recovery records; delete Pods and let cleanup finish first")
    if controller_running(record):
        raise RuntimeError("stop the controller before removing its empty cluster")
    peer = inspect_peer(record)
    if peer is not None:
        if not peer["State"]["Running"]:
            raise RuntimeError("start the owned cluster with prepare so cleanup can verify no Pods remain")
        pods = json.loads(kubectl(record, "get", "pods", "-A", "--field-selector", "spec.nodeName="+record["node"], "-o", "json"))
        if pods["items"]:
            raise RuntimeError("Pods still target the native node; delete them and let controller cleanup finish first")
        command("docker", "rm", "-fv", record["peer"])
    print("Removed owned empty cluster; retained private configuration at "+str(directory))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="operation", required=True)
    building = commands.add_parser("build", help="build the host tools and matching static Linux guest helpers")
    building.add_argument("--binaries", required=True)
    preparing = commands.add_parser("prepare", help="create or renew an owned local k3s cluster and restricted controller credentials")
    preparing.add_argument("--directory", required=True)
    preparing.add_argument("--binaries", required=True)
    preparing.add_argument("--msb", required=True)
    preparing.add_argument("--msb-home", required=True)
    preparing.add_argument("--node-ip", help="host IPv4 address reachable from the owned cluster; bypasses automatic Docker host discovery")
    running = commands.add_parser("run", help="run in foreground, retaining guests and cluster on exit")
    running.add_argument("--directory", required=True)
    running.add_argument("--memory-mib", type=int, default=4096)
    running.add_argument("--scheduling-cpu", default="500")
    running.add_argument("--scheduling-memory", default="384Gi")
    running.add_argument("--pod-workers", type=int, default=8)
    for operation in ("status", "destroy"):
        commands.add_parser(operation).add_argument("--directory", required=True)
    args = parser.parse_args()
    try:
        if args.operation == "run":
            record = load(args.directory)
            peer = inspect_peer(record)
            if not peer or not peer["State"]["Running"]:
                raise RuntimeError("owned network peer is not running; run prepare first")
            with setup_lock(args.directory) as descriptor:
                values = controller_arguments(record, args)
                # Keep setup and cleanup excluded for the foreground controller's lifetime.
                os.set_inheritable(descriptor, True)
                os.execv(values[0], values)
        operation = {"build": build, "prepare": prepare, "status": status, "destroy": destroy}[args.operation]
        if args.operation in ("prepare", "destroy"):
            with setup_lock(args.directory):
                operation(args)
        else:
            operation(args)
        return 0
    except (RuntimeError, OSError, ValueError, subprocess.TimeoutExpired) as error:
        parser.exit(1, "native cluster: "+str(error)+"\n")


if __name__ == "__main__":
    raise SystemExit(main())
