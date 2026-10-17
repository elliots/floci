#!/usr/bin/env python3
"""Prepare and inspect an owned local cluster for the shared native pod controller."""
import argparse
import base64
import collections
import contextlib
import fcntl
import ipaddress
import json
import manage_environment
import os
from pathlib import Path
import platform
import secrets
import shutil
import socket
import stat
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
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
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
    record = json.loads(read_private(directory / "cluster.json"))
    if record.get("version") not in (1, 2) or record.get("directory") != str(directory):
        raise RuntimeError("cluster record belongs to another directory or format")
    if record["version"] == 2 and not record.get("environment"):
        raise RuntimeError("protected cluster record is missing its environment identity")
    return record


def read_private(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor) as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid() or info.st_size > 1 << 20:
            raise RuntimeError("invalid private management file: "+str(path))
        return source.read()


def credential_directory(record):
    directory = Path(record["directory"])
    generation = record.get("credentialGeneration")
    if generation is None:
        return directory
    if len(generation) != 32 or any(character not in "0123456789abcdef" for character in generation):
        raise RuntimeError("invalid credential generation")
    path = directory / "credentials" / generation
    if path.resolve() != path or not path.is_dir() or path.stat().st_mode & 0o077:
        raise RuntimeError("credential generation is missing or not a private owned directory")
    return path


@contextlib.contextmanager
def setup_lock(directory):
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if directory.stat().st_mode & 0o077 or directory.stat().st_uid != os.getuid():
        raise RuntimeError("cluster directory must be private (mode 0700)")
    descriptor = os.open(directory / ".manage.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as lock:
        info = os.fstat(lock.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid():
            raise RuntimeError("cluster lock must be a private owned regular file")
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
    manage_environment.verify_peer(record, values[0])
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
        {"apiGroups": [""], "resources": ["persistentvolumeclaims"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": [""], "resources": ["persistentvolumes"], "verbs": ["get", "list", "watch", "create", "update", "delete"]},
        {"apiGroups": ["storage.k8s.io"], "resources": ["storageclasses"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": [""], "resources": ["serviceaccounts"], "verbs": ["get"]},
        {"apiGroups": [""], "resources": ["serviceaccounts/token"], "verbs": ["create"]},
        {"apiGroups": [""], "resources": ["namespaces"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": [""], "resources": ["nodes", "nodes/status"], "resourceNames": [node], "verbs": ["get", "patch", "update"]},
        {"apiGroups": [""], "resources": ["events"], "verbs": ["create", "patch", "update"]},
        {"apiGroups": ["apps"], "resources": ["deployments", "replicasets"], "verbs": ["get", "list"]},
        {"apiGroups": ["networking.k8s.io"], "resources": ["networkpolicies"], "verbs": ["get", "list", "watch"]},
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


def certificate(record, name, ca_name, common_name, usage, host=None, directory=None):
    directory = Path(directory or record["directory"])
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


def prepare_credentials(record):
    peer = inspect_peer(record)
    if peer is None or not peer["State"]["Running"]:
        raise RuntimeError("owned cluster must be running to renew credentials")
    api_port = peer["NetworkSettings"]["Ports"]["6443/tcp"][0]["HostPort"]
    generation = secrets.token_hex(16)
    generations = Path(record["directory"]) / "credentials"
    generations.mkdir(exist_ok=True, mode=0o700)
    if generations.is_symlink() or generations.stat().st_mode & 0o077 or generations.stat().st_uid != os.getuid():
        raise RuntimeError("credential generations require a private owned directory")
    directory = generations / generation
    directory.mkdir(mode=0o700)
    try:
        certificate(record, "kubelet", "server-ca", record["node"], "serverAuth", record["hostIP"], directory)
        certificate(record, "controller", "client-ca", record["user"], "clientAuth", directory=directory)
        client_ca = command("docker", "exec", record["peer"], "cat", "/var/lib/rancher/k3s/server/tls/client-ca.crt")
        server_ca = command("docker", "exec", record["peer"], "cat", "/var/lib/rancher/k3s/server/tls/server-ca.crt")
        write_private(directory / "client-ca.crt", client_ca+"\n")
        encode = lambda data: base64.b64encode(data.encode()).decode()
        kubeconfig = {"apiVersion": "v1", "kind": "Config", "current-context": "native",
            "clusters": [{"name": "native", "cluster": {"server": "https://127.0.0.1:"+api_port,
                "certificate-authority-data": encode(server_ca)}}],
            "contexts": [{"name": "native", "context": {"cluster": "native", "user": "controller"}}],
            "users": [{"name": "controller", "user": {
                "client-certificate-data": encode(read_private(directory/"controller.crt")),
                "client-key-data": encode(read_private(directory/"controller.key"))}}]}
        write_private(directory / "controller.kubeconfig", json.dumps(kubeconfig, indent=2)+"\n")
        write_private(directory / "owner", json.dumps({"owner":record["owner"],"generation":generation})+"\n")
        return dict(record, credentialGeneration=generation, previousCredentialGeneration=record.get("credentialGeneration"))
    except BaseException:
        shutil.rmtree(directory)
        raise


def remove_old_credentials(record):
    keep = {record.get("credentialGeneration"), record.get("previousCredentialGeneration")}
    expected = {"controller.crt","controller.key","kubelet.crt","kubelet.key","client-ca.crt","controller.kubeconfig","owner"}
    root = Path(record["directory"])/"credentials"
    if not root.exists():
        return
    if root.is_symlink() or not root.is_dir() or root.stat().st_mode & 0o077 or root.stat().st_uid != os.getuid():
        raise RuntimeError("credential cleanup requires a private owned directory")
    for directory in root.glob("*"):
        if directory.name in keep or directory.is_symlink() or not directory.is_dir():
            continue
        if len(directory.name) != 32 or any(character not in "0123456789abcdef" for character in directory.name):
            continue
        if directory.stat().st_mode & 0o077 or directory.stat().st_uid != os.getuid():
            continue
        try:
            identity = json.loads(read_private(directory/"owner"))
            if identity != {"owner":record["owner"],"generation":directory.name}:
                continue
            if {path.name for path in directory.iterdir()} != expected:
                continue
            for path in directory.iterdir():
                read_private(path)
        except (OSError, RuntimeError, ValueError):
            continue  # Unknown, partial or legacy directories are retained for inspection.
        shutil.rmtree(directory)


def credentials_due(record, seconds=7*86400):
    directory = credential_directory(record)
    for name in ("controller.crt", "kubelet.crt"):
        # Malformed or missing credentials are renewal failures, not proof of validity.
        certificate = read_private(directory/name)
        command("openssl", "x509", "-noout", input_text=certificate)
        result = subprocess.run(["openssl", "x509", "-checkend", str(seconds), "-noout"],
            input=certificate, text=True, capture_output=True, timeout=15)
        if result.returncode:
            return True
    return False


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
    existing = record_path.exists()
    if existing:
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
    manage_environment.resolve(record, args, command, existing)
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
        command("docker", "create", "--name", record["peer"], "--hostname", record["peer"], *linux_host,
            *manage_environment.peer_options(record), "--label", LABEL+"="+record["owner"], "--privileged", "-p", "127.0.0.1::6443", "-p", "127.0.0.1::8472/udp",
            K3S_IMAGE, "server", "--disable=traefik", "--disable=servicelb", "--disable=metrics-server",
            "--egress-selector-mode=disabled", timeout=600)
        manage_environment.stage_resolver(record, command, write_private)
        command("docker", "start", record["peer"])
    elif not peer["State"]["Running"]:
        manage_environment.stage_resolver(record, command, write_private)
        command("docker", "start", record["peer"])
    wait("Kubernetes API ready", lambda: kubectl(record, "get", "--raw=/readyz") == "ok")
    peer = inspect_peer(record)
    api_port = peer["NetworkSettings"]["Ports"]["6443/tcp"][0]["HostPort"]
    host = node_address(record, args.node_ip)
    record["hostIP"] = host
    admin = command("docker", "exec", record["peer"], "cat", "/etc/rancher/k3s/k3s.yaml")
    write_private(directory / "admin.kubeconfig", admin.replace("https://127.0.0.1:6443", "https://127.0.0.1:"+api_port)+"\n")
    record = prepare_credentials(record)
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
    remove_old_credentials(record)
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
    credentials = credential_directory(record)
    return [str(binaries/"microsandbox-pods"), "--kubeconfig", str(credentials/"controller.kubeconfig"),
        "--node", record["node"], "--msb", record["msb"], "--msb-home", record["msb_home"],
        "--state", str(directory/"controller"), "--network-peer", record["peer"], "--peer-owner", record["owner"],
        "--network-relay-binary", str(binaries/"network-relay"), "--guest-fs-binary", str(binaries/"guestfs"),
        "--listen", "0.0.0.0:"+str(record["kubeletPort"]), "--node-ip", record["hostIP"],
        "--tls-cert", str(credentials/"kubelet.crt"), "--tls-key", str(credentials/"kubelet.key"),
        "--client-ca", str(credentials/"client-ca.crt"), "--control-listen", "127.0.0.1:"+str(record["controlPort"]),
        "--control-token-file", str(directory/"control-token"), "--cluster-name", record["peer"],
        "--max-active-memory-mib", str(args.memory_mib), "--scheduling-cpu", args.scheduling_cpu,
        "--scheduling-memory", args.scheduling_memory, "--pod-workers", str(args.pod_workers),
        "--network-policy=" + str(bool(record.get("environment")) or getattr(args, "network_policy", False)).lower(),
        *manage_environment.controller_options(record)]


def status(args):
    import manage_supervisor
    record = load(args.directory)
    peer = inspect_peer(record)
    directory = Path(record["directory"])
    report = {"peer": record["peer"], "peerRunning": bool(peer and peer["State"]["Running"]),
        "controller": "unavailable", "runtimeHome": record["msb_home"],
        "filesystemFreeBytes": shutil.disk_usage(record["msb_home"]).free}
    report["manager"] = manage_supervisor.running(args.directory)
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
    report["storage"] = "unavailable"
    if isinstance(report["controller"], dict):
        request = urllib.request.Request("http://127.0.0.1:"+str(record["controlPort"])+"/v1/capacity",
            headers={"Authorization": "Bearer "+token})
        try:
            with urllib.request.urlopen(request, timeout=35) as response:
                report["sharedCapacity"] = json.load(response)
        except OSError:
            pass  # An unavailable inventory must not be reported as zero usage.
        try:
            report["storage"] = controller_request(record, "storage")
        except OSError:
            pass  # Storage failures must remain visible separately from an empty inventory.
    print(json.dumps(report, indent=2))


def controller_request(record, path, value=None):
    token = read_private(Path(record["directory"])/"control-token").strip()
    request = urllib.request.Request("http://127.0.0.1:"+str(record["controlPort"])+"/v1/"+path,
        data=json.dumps(value).encode() if value is not None else None,
        headers={"Authorization":"Bearer "+token,"Content-Type":"application/json"})
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.load(response)


def storage(args):
    print(json.dumps(controller_request(load(args.directory), "storage"), indent=2))


def prune(args):
    if args.older_than_seconds < 0:
        raise ValueError("older-than-seconds must be nonnegative")
    value = {"dryRun":not args.apply, "olderThanSeconds":args.older_than_seconds,
        "images":args.images,"retainImages":args.retain_image}
    print(json.dumps(controller_request(load(args.directory), "storage/prune", value), indent=2))


def destroy(args):
    record = load(args.directory)
    directory = Path(record["directory"])
    if list((directory/"controller").glob("*.json")) or (directory/"controller"/"native-command.pending").exists():
        raise RuntimeError("controller still owns workload or recovery records; delete Pods and let cleanup finish first")
    for path in (directory/"controller"/"persistent-volumes").glob("*.json"):
        intent = json.loads(path.read_text())
        if intent.get("version") != 1 or intent.get("phase") != "deleted":
            raise RuntimeError("persistent volume allocations remain; reclaim Delete volumes or preserve the cluster for Retain volumes")
    if controller_running(record):
        raise RuntimeError("stop the controller before removing its empty cluster")
    peer = inspect_peer(record)
    if peer is not None:
        if not peer["State"]["Running"]:
            raise RuntimeError("start the owned cluster with prepare so cleanup can verify no Pods remain")
        pods = json.loads(kubectl(record, "get", "pods", "-A", "--field-selector", "spec.nodeName="+record["node"], "-o", "json"))
        if pods["items"]:
            raise RuntimeError("Pods still target the native node; delete them and let controller cleanup finish first")
        volumes = json.loads(kubectl(record, "get", "pv", "-o", "json"))
        for volume in volumes["items"]:
            if volume.get("metadata", {}).get("annotations", {}).get("pv.kubernetes.io/provisioned-by") != "microsandbox.dev/local":
                continue
            terms = volume.get("spec", {}).get("nodeAffinity", {}).get("required", {}).get("nodeSelectorTerms", [])
            if any(record["node"] in expression.get("values", [])
                   for term in terms for expression in term.get("matchExpressions", [])
                   if expression.get("key") == "kubernetes.io/hostname"):
                raise RuntimeError("native PV objects still target this node; finish reclamation before removing the cluster")
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
    preparing.add_argument("--environment-network", help="existing protected Docker bridge; requires isolation-helper and environment-dns")
    preparing.add_argument("--isolation-helper", help="existing Floci isolation helper protecting that bridge")
    preparing.add_argument("--environment-dns", help="Floci DNS IPv4 address assigned on the protected bridge")
    for operation, description in (("run", "supervise in foreground and renew certificates automatically"),
            ("start", "start an owned background supervisor"), ("_supervise", argparse.SUPPRESS)):
        running = commands.add_parser(operation, help=description)
        running.add_argument("--directory", required=True)
        running.add_argument("--memory-mib", type=int, default=4096)
        running.add_argument("--scheduling-cpu", default="500")
        running.add_argument("--scheduling-memory", default="384Gi")
        running.add_argument("--pod-workers", type=int, default=8)
        running.add_argument("--network-policy", action="store_true", help="require safe capabilities on every native Pod and enforce Kubernetes NetworkPolicies")
    for operation in ("status", "destroy", "stop", "renew", "storage"):
        commands.add_parser(operation).add_argument("--directory", required=True)
    pruning = commands.add_parser("prune", help="preview ownership-aware native storage cleanup; --apply performs it")
    pruning.add_argument("--directory", required=True)
    pruning.add_argument("--apply", action="store_true")
    pruning.add_argument("--images", action="store_true")
    pruning.add_argument("--older-than-seconds", type=int, default=86400)
    pruning.add_argument("--retain-image", action="append", default=[])
    args = parser.parse_args()
    try:
        if args.operation in ("run", "start", "stop", "renew", "_supervise"):
            import manage_supervisor
            operations = {"run": manage_supervisor.serve, "_supervise": manage_supervisor.serve,
                "start": manage_supervisor.start, "stop": manage_supervisor.stop, "renew": manage_supervisor.renew}
            operations[args.operation](args)
            return 0
        operation = {"build": build, "prepare": prepare, "status": status, "destroy": destroy,
            "storage":storage,"prune":prune}[args.operation]
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
