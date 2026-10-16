#!/usr/bin/env python3
"""Qualify a real microsandbox k3s worker against an isolated Docker control plane."""

import argparse
import ipaddress
import json
import os
from pathlib import Path
import platform
import re
import secrets
import socket
import subprocess
import sys
import tempfile
import time


VERSION = "msb 0.7.8"
IMAGE = "rancher/k3s:v1.35.0-k3s1"
POD_IMAGE = "busybox:1.37.0"
OWNER_LABEL = "io.floci.worker-probe"


class PodFailed(RuntimeError):
    """A completed failure cannot become ready by polling the same pod again."""


class Commands:
    def __init__(self, msb, docker, home):
        self.msb = msb
        self.docker = docker
        self.environment = dict(os.environ, MSB_HOME=str(home))
        self.secrets = []

    def redact(self, value):
        text = str(value)
        for secret in sorted(self.secrets, key=len, reverse=True):
            text = text.replace(secret, "[redacted]")
        return re.sub(r"K10[0-9a-fA-F]+::[^\s\"']+", "[redacted]", text)

    def run(self, executable, *args, timeout=30, input_text=None, combined=False):
        try:
            result = subprocess.run(
                [executable, *args], env=self.environment, capture_output=True,
                text=True, input=input_text, timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            # TimeoutExpired includes the command, and must not reach report/log output.
            raise RuntimeError(f"{Path(executable).name} {args[0]} timed out after {timeout}s") from None
        if result.returncode:
            detail = self.redact(result.stderr.strip() or result.stdout.strip())
            raise RuntimeError(
                f"{Path(executable).name} {args[0]} failed (exit {result.returncode}): {detail}"
            )
        return (result.stdout + result.stderr if combined else result.stdout).strip()

    def vm(self, *args, **kwargs):
        return self.run(self.msb, *args, **kwargs)

    def container(self, *args, **kwargs):
        return self.run(self.docker, *args, **kwargs)


def wait_for(description, operation, timeout):
    deadline = time.monotonic() + timeout
    last_error = "not ready"
    while time.monotonic() < deadline:
        try:
            result = operation()
            if result:
                return result
        except PodFailed:
            raise
        except (RuntimeError, ValueError) as error:
            last_error = str(error)
        time.sleep(1)
    raise RuntimeError(f"{description} did not complete within {timeout}s: {last_error}")


class WorkerProbe:
    def __init__(self, commands, args):
        self.commands = commands
        self.args = args
        self.owner = secrets.token_hex(8)
        self.server = f"floci-worker-probe-{self.owner}"
        self.worker = f"fwp-{self.owner}"
        self.workers = [self.worker] + [f"{self.worker}-{i}" for i in range(2, args.workers + 1)]
        self.marker = f"worker-executed-{self.owner}"
        self.server_attempted = False
        self.attempted_workers = []
        self.vxlan_ports = {}
        self.cni_check = "cross_worker_cni_dns_service" if args.workers > 1 else "single_worker_cni_dns_service"
        self.report = {
            "runtime": VERSION,
            "host": platform.platform(),
            "architecture": platform.machine(),
            "image": args.image,
            "pod_image": args.pod_image,
            "control_plane": self.server,
            "workers": self.workers,
            "relay_vxlan": args.relay_vxlan,
            "nodes": [],
            "checks": {},
            "limitations": [
                "Standalone qualification only; no Floci provider is enabled.",
                "AWS identity and hibernation are not tested.",
            ],
        }
        if args.workers == 1:
            self.report["limitations"].append("Cross-worker networking is not tested.")

    def kubectl(self, *args, input_text=None, timeout=30):
        return self.commands.container(
            "exec", "-i", self.server, "kubectl", "--request-timeout=15s", *args,
            input_text=input_text, timeout=timeout,
        )

    def resource(self, kind, name):
        return json.loads(self.kubectl("get", kind, name, "-o", "json"))

    def apply(self, resource):
        self.kubectl("apply", "-f", "-", input_text=json.dumps(resource))

    def pod(self, name, command, host_network=False, labels=None, worker=None):
        return {
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "labels": labels or {}},
            "spec": {
                "nodeName": worker or self.worker,
                "hostNetwork": host_network,
                "dnsPolicy": "Default" if host_network else "ClusterFirst",
                "restartPolicy": "Never",
                "containers": [{
                    "name": "probe", "image": self.args.pod_image,
                    "command": ["sh", "-ec", command],
                    "resources": {"requests": {"cpu": "10m", "memory": "16Mi"}},
                }],
            },
        }

    def completed_pod(self, name):
        pod = self.resource("pod", name)
        phase = pod.get("status", {}).get("phase")
        if phase == "Failed":
            try:
                detail = self.kubectl("logs", name)
            except RuntimeError as error:
                detail = f"logs unavailable: {error}"
            raise PodFailed(f"Pod {name} failed: {detail}")
        return pod if phase == "Succeeded" else None

    def ready_node(self, worker=None):
        worker = worker or self.worker
        node = self.resource("node", worker)
        conditions = node.get("status", {}).get("conditions", [])
        if not any(item.get("type") == "Ready" and item.get("status") == "True"
                   for item in conditions):
            return None
        lease = json.loads(
            self.kubectl("get", "lease", worker, "-n", "kube-node-lease", "-o", "json")
        )
        return node if lease.get("spec", {}).get("holderIdentity") == worker else None

    def qualify(self, temporary):
        commands = self.commands
        if commands.vm("--version") != VERSION:
            raise RuntimeError(f"This probe requires {VERSION}")
        commands.vm("doctor")
        commands.container("info", "--format", "{{.ServerVersion}}")
        existing = json.loads(commands.vm("ls", "--format", "json"))
        if any(item["name"] in self.workers for item in existing):
            raise RuntimeError("Generated worker name already exists; refusing to replace it")

        token_file = temporary / "agent-token"
        agent_token = secrets.token_hex(32)
        commands.secrets.append(agent_token)
        token_file.write_text(agent_token + "\n")
        token_file.chmod(0o600)

        print("Starting isolated Docker k3s control plane...", file=sys.stderr, flush=True)
        self.server_attempted = True
        commands.container(
            "create", "--name", self.server, "--hostname", self.server,
            "--label", f"{OWNER_LABEL}={self.owner}", "--privileged",
            "-p", "127.0.0.1::6443", self.args.image,
            "server", "--disable-agent", "--egress-selector-mode=cluster",
            "--disable=traefik", "--disable=servicelb", "--disable=metrics-server",
            "--tls-san=host.microsandbox.internal",
            "--agent-token-file=/etc/floci-agent-token", timeout=600,
        )
        commands.container("cp", str(token_file), f"{self.server}:/etc/floci-agent-token")
        commands.container("start", self.server)
        inspect = json.loads(commands.container("inspect", self.server))[0]
        port = inspect["NetworkSettings"]["Ports"]["6443/tcp"][0]["HostPort"]
        self.report["control_plane_host_port"] = int(port)
        wait_for("Control plane readiness", lambda: self.kubectl("get", "--raw=/readyz") == "ok",
                 self.args.timeout)
        secure_token = commands.container(
            "exec", self.server, "cat", "/var/lib/rancher/k3s/server/agent-token"
        )
        commands.secrets.append(secure_token)
        token_file.write_text(secure_token + "\n")
        self.report["checks"]["control_plane_ready"] = True

        for worker in self.workers:
            print(f"Starting native microsandbox worker {worker}...", file=sys.stderr, flush=True)
            self.attempted_workers.append(worker)
            started = time.monotonic()
            network_args = []
            if self.args.relay_vxlan:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as listener:
                    listener.bind(("127.0.0.1", 0))
                    self.vxlan_ports[worker] = listener.getsockname()[1]
                network_args = ["--port", f"127.0.0.1:{self.vxlan_ports[worker]}:8472/udp"]
            commands.vm(
                "run", "-d", "--name", worker,
                "--label", f"{OWNER_LABEL}={self.owner}", "--cpus", "2", "--memory", "2G",
                "--root-disk", "flat:8G", "--net", "host,private,public",
                *network_args,
                "--copy-file", f"{token_file}:/etc/floci-agent-token",
                self.args.image, "--", "agent",
                f"--server=https://host.microsandbox.internal:{port}",
                "--token-file=/etc/floci-agent-token", f"--node-name={worker}",
                "--disable-apiserver-lb", timeout=600,
            )
            node = wait_for(f"Worker {worker} Ready and kubelet lease",
                            lambda: self.ready_node(worker), self.args.timeout)
            self.report["nodes"].append({
                "name": node["metadata"]["name"],
                "uid": node["metadata"]["uid"],
                "addresses": node["status"].get("addresses"),
                "node_info": node["status"].get("nodeInfo"),
                "capacity": node["status"].get("capacity"),
                "pod_cidr": node.get("spec", {}).get("podCIDR"),
                "ready_seconds": round(time.monotonic() - started, 3),
            })
            host_pod = f"worker-host-execution-{len(self.report['nodes'])}"
            self.apply(self.pod(host_pod, f"printf '%s\\n' '{self.marker}'", host_network=True,
                                worker=worker))
            wait_for(f"Host-network pod execution on {worker}", lambda: self.completed_pod(host_pod),
                     self.args.timeout)
            if self.marker not in self.kubectl("logs", host_pod):
                raise RuntimeError("Host-network pod logs did not contain its execution marker")
        self.report["checks"]["native_worker_ready_with_lease"] = True
        self.report["checks"]["host_network_pod_execution_and_logs"] = True

        if self.args.skip_cni:
            self.report["checks"][self.cni_check] = "not tested"
            return
        print("Checking ordinary pod networking, DNS and ClusterIP...", file=sys.stderr, flush=True)
        try:
            if self.args.relay_vxlan:
                self.configure_vxlan_relay()
            self.check_cni()
            self.report["checks"][self.cni_check] = True
        except RuntimeError as error:
            self.report["checks"][self.cni_check] = False
            self.report["cni_error"] = commands.redact(error)

    def configure_vxlan_relay(self):
        """Relay only the encapsulated VXLAN transport; leave inner pod addresses intact."""
        self.report["vxlan_host_ports"] = dict(self.vxlan_ports)
        self.report["vxlan_relay"] = []
        for worker in self.workers:
            lookup = self.commands.vm("exec", worker, "--", "sh", "-c",
                                      "getent hosts host.microsandbox.internal || nslookup host.microsandbox.internal")
            gateway = self.host_gateway(lookup)
            for peer in self.workers:
                if worker == peer:
                    continue
                node = self.resource("node", peer)
                address = next((item["address"] for item in node["status"]["addresses"]
                                if item["type"] == "InternalIP"), None)
                peer_ip = str(ipaddress.IPv4Address(address))
                port = self.vxlan_ports[peer]
                self.commands.vm("exec", worker, "--", "sh", "-ec",
                                 'iptables -t nat -C OUTPUT -p udp -d "$1" --dport 8472 '
                                 '-j DNAT --to-destination "$2:$3" 2>/dev/null || '
                                 'iptables -t nat -A OUTPUT -p udp -d "$1" --dport 8472 '
                                 '-j DNAT --to-destination "$2:$3"',
                                 "vxlan-relay", peer_ip, gateway, str(port))
                self.report["vxlan_relay"].append({
                    "source": worker, "peer": peer, "peer_ip": peer_ip,
                    "host_gateway": gateway, "host_port": port,
                })

    @staticmethod
    def host_gateway(lookup):
        addresses = re.findall(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])", lookup)
        if not addresses:
            raise RuntimeError("Cannot resolve the host gateway IPv4 address")
        return str(ipaddress.IPv4Address(addresses[-1]))

    def check_cni(self):
        server_pod = "worker-network-server"
        manifest = self.pod(server_pod,
                            f"mkdir -p /www; printf '%s' '{self.marker}' > /www/index.html; "
                            "exec httpd -f -p 8080 -h /www", labels={"app": server_pod})
        manifest["spec"]["containers"][0]["readinessProbe"] = {
            "tcpSocket": {"port": 8080}, "periodSeconds": 1,
        }
        self.apply(manifest)
        self.apply({
            "apiVersion": "v1", "kind": "Service", "metadata": {"name": server_pod},
            "spec": {"selector": {"app": server_pod},
                     "ports": [{"port": 8080, "targetPort": 8080}]},
        })
        wait_for("Network server pod", lambda: any(
            item.get("type") == "Ready" and item.get("status") == "True"
            for item in self.resource("pod", server_pod).get("status", {}).get("conditions", [])
        ), self.args.timeout)
        address = ipaddress.ip_address(self.resource("pod", server_pod)["status"]["podIP"])
        host = f"[{address}]" if address.version == 6 else str(address)
        self.report["network_pod_nodes"] = {"server": self.worker, "client": self.workers[-1]}
        self.report["network_server_pod_ip"] = str(address)
        self.report["checks"]["direct_pod_ip_http"] = False
        direct_pod = "worker-direct-client"
        self.apply(self.pod(direct_pod, f"wget -T 10 -q -O - http://{host}:8080",
                            worker=self.workers[-1]))
        wait_for("Direct Pod-IP HTTP", lambda: self.completed_pod(direct_pod), self.args.timeout)
        if self.marker not in self.kubectl("logs", direct_pod):
            raise RuntimeError("Direct Pod-IP response did not contain the server pod marker")
        self.report["checks"]["direct_pod_ip_http"] = True
        wait_for("CoreDNS availability", lambda: json.loads(self.kubectl(
            "get", "deployment", "coredns", "-n", "kube-system", "-o", "json"
        )).get("status", {}).get("availableReplicas", 0) > 0, self.args.timeout)
        client_pod = "worker-network-client"
        self.apply(self.pod(client_pod,
                            "nslookup kubernetes.default.svc.cluster.local; "
                            f"wget -T 10 -q -O - http://{server_pod}.default.svc.cluster.local:8080",
                            worker=self.workers[-1]))
        wait_for("Ordinary pod DNS and ClusterIP", lambda: self.completed_pod(client_pod),
                 self.args.timeout)
        if self.marker not in self.kubectl("logs", client_pod):
            raise RuntimeError("ClusterIP response did not contain the server pod marker")

    def diagnostics(self):
        logs = {}
        operations = []
        if self.server_attempted:
            operations.extend([
                ("control-plane.log", lambda: self.commands.container(
                    "logs", "--tail", "300", self.server, combined=True)),
                ("nodes.json", lambda: self.kubectl("get", "nodes", "-o", "json")),
                ("pods.json", lambda: self.kubectl("get", "pods", "-A", "-o", "json")),
                ("events.txt", lambda: self.kubectl("get", "events", "-A", "--sort-by=.metadata.creationTimestamp")),
            ])
        for worker in self.attempted_workers:
            operations.append((f"{worker}.log", lambda name=worker: self.commands.vm(
                "logs", name, "--tail", "300", "--source", "all", "--no-color")))
            if self.args.relay_vxlan:
                operations.append((f"{worker}-network.txt", lambda name=worker: self.commands.vm(
                    "exec", name, "--", "sh", "-ec", "ip -4 addr; ip route; iptables-save -t nat")))
        for name, operation in operations:
            try:
                logs[name] = self.commands.redact(operation())
            except Exception as error:
                logs[name] = self.commands.redact(error)
        return logs

    def cleanup(self):
        errors = []
        for worker in reversed(self.attempted_workers):
            try:
                existing = json.loads(self.commands.vm("ls", "--format", "json"))
                if any(item["name"] == worker for item in existing):
                    inspect = json.loads(self.commands.vm("inspect", "--format", "json", worker))
                    configurations = [inspect.get("config", {})]
                    if inspect.get("active_config") is not None:
                        configurations.append(inspect["active_config"])
                    if any(configuration.get("labels", {}).get(OWNER_LABEL) != self.owner
                           for configuration in configurations):
                        raise RuntimeError("Worker ownership label changed; refusing to remove it")
                    if inspect["status"] != "Stopped":
                        self.commands.vm("stop", "--force", worker)
                    self.commands.vm("rm", worker)
            except Exception as error:
                errors.append(self.commands.redact(f"Worker {worker} cleanup: {error}"))
        if self.server_attempted:
            try:
                containers = self.commands.container(
                    "container", "ls", "-a", "--filter", f"name=^/{self.server}$", "--format", "{{.ID}}"
                )
                if containers:
                    inspect = json.loads(self.commands.container("inspect", self.server))[0]
                    if inspect["Config"].get("Labels", {}).get(OWNER_LABEL) != self.owner:
                        raise RuntimeError("Container ownership label changed; refusing to remove it")
                    self.commands.container("rm", "-f", "-v", self.server)
            except Exception as error:
                errors.append(self.commands.redact(f"Control-plane cleanup: {error}"))
        return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--msb", default="msb")
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--msb-home", type=Path, help="Short runtime path; only uniquely named probe guests are touched")
    parser.add_argument("--image", default=IMAGE)
    parser.add_argument("--pod-image", default=POD_IMAGE)
    parser.add_argument("--workers", type=int, choices=(1, 2), default=1,
                        help="Use two workers to require cross-worker pod networking")
    parser.add_argument("--relay-vxlan", action="store_true",
                        help="Relay cross-worker VXLAN over loopback UDP ports using guest-only DNAT")
    parser.add_argument("--timeout", type=int, default=180, help="Seconds allowed for each readiness check")
    parser.add_argument("--skip-cni", action="store_true", help="Only qualify the agent and a host-network pod")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.timeout < 1:
        parser.error("timeout must be positive")
    home = args.msb_home or Path(tempfile.mkdtemp(prefix="fwp-", dir="/tmp"))
    home = home.resolve()
    if len(str(home).encode()) > 40:
        parser.error("use an MSB_HOME path of at most 40 bytes for macOS Unix socket limits")
    home.mkdir(parents=True, exist_ok=True)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    commands = Commands(args.msb, args.docker, home)
    probe = WorkerProbe(commands, args)
    probe.report["state_directory"] = str(home)
    print(f"MSB_HOME: {home}; owned resources: {probe.server}, {', '.join(probe.workers)}",
          file=sys.stderr, flush=True)
    exit_code = 0
    with tempfile.TemporaryDirectory(prefix="fwp-secret-", dir="/tmp") as temporary:
        try:
            probe.qualify(Path(temporary))
        except (Exception, KeyboardInterrupt) as error:
            probe.report["error"] = commands.redact(error or "Interrupted")
            exit_code = 1
        finally:
            logs = probe.diagnostics()
            errors = probe.cleanup()
            probe.report["cleanup"] = "passed" if not errors else "failed"
            if errors:
                probe.report["cleanup_errors"] = errors
                exit_code = 3
    if not exit_code and probe.report["checks"].get(probe.cni_check) is False:
        exit_code = 2
    probe.report["result"] = "failed" if exit_code in (1, 3) else "partial" if exit_code == 2 else "passed"
    log_dir = args.report.parent / f"{args.report.stem}-{probe.owner}-logs"
    log_dir.mkdir(mode=0o700)
    for name, content in logs.items():
        path = log_dir / name
        path.write_text(content + "\n")
        path.chmod(0o600)
    probe.report["logs"] = str(log_dir.resolve())
    args.report.write_text(commands.redact(json.dumps(probe.report, indent=2)) + "\n")
    print(f"{probe.report['result']}; report: {args.report}; logs: {log_dir}", flush=True)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
