#!/usr/bin/env python3
"""Actual Floci Docker environment, protected managed peer and native egress proof."""
import argparse
import base64
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import secrets
import socket
import traceback
import urllib.request
import urllib.error

import manage
from qualify_managed import ManagedQualification


PYTHON_IMAGE = "python:3.12-alpine"


def free_udp_port():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def unused_public_canary_subnet(networks):
    allocated = [ipaddress.ip_network(config["Subnet"], strict=False)
        for network in networks for config in network.get("IPAM", {}).get("Config", [])
        if config.get("Subnet")]
    for candidate in ("203.0.113.0/24", "198.51.100.0/24", "192.0.2.0/24"):
        subnet = ipaddress.ip_network(candidate)
        if all(subnet.version != existing.version or not subnet.overlaps(existing) for existing in allocated):
            return candidate
    raise RuntimeError("all documentation-range canary subnets overlap existing Docker networks")


class EnvironmentFixture:
    def __init__(self, q):
        self.q = q
        self.owner = "isolation-" + secrets.token_hex(6)
        self.floci_owner = self.owner + "/4566"
        self.networks = []
        self.containers = []
        self.tags = []
        self.floci = None
        self.helper = None
        self.boundary = None
        self.uid = None
        self.source = Path(__file__).with_name("isolation_probe.py")
        self.network_file = q.root / "network.json"
        self.ca = q.root / "ca.crt"

    def command(self, *args, **kwargs):
        return self.q.command(*args, **kwargs)

    def inspect(self, name):
        return json.loads(self.command("docker", "inspect", name))[0]

    def container(self, suffix, network, image, *arguments, ports=()):
        name = self.owner + "-" + suffix
        self.containers.append(name)
        port_args = [value for port in ports for value in ("-p", "127.0.0.1::" + port)]
        self.command("docker", "run", "-d", "--name", name, "--network", network,
            "--label", manage.LABEL + "=" + self.owner, *port_args,
            "-v", str(self.source) + ":/probe.py:ro", image, *arguments, timeout=180)
        return name

    def address(self, container, network):
        return self.inspect(container)["NetworkSettings"]["Networks"][network]["IPAddress"]

    def setup(self):
        q = self.q
        for image in (q.args.floci_image, PYTHON_IMAGE, "registry:2", manage.K3S_IMAGE):
            self.command("docker", "image", "inspect", image)
        network_ids = self.command("docker", "network", "ls", "-q").splitlines()
        existing = json.loads(self.command("docker", "network", "inspect", *network_ids)) if network_ids else []
        canary_subnet = unused_public_canary_subnet(existing)
        protected, outside = self.owner + "-protected", self.owner + "-outside"
        for network in (protected, outside):
            self.networks.append(network)
            ipv6 = ["--subnet", canary_subnet, "--ipv6", "--subnet", "fd" + self.owner[-12:-10] + ":" + self.owner[-10:-6] + ":" + self.owner[-6:-2] + "::/64"] if network == outside else []
            self.command("docker", "network", "create", "--label", manage.LABEL + "=" + self.owner, *ipv6, network)
        self.simulator = self.container("simulator", protected, PYTHON_IMAGE, "python", "/probe.py", "server")
        self.external = self.container("external", outside, PYTHON_IMAGE, "python", "/probe.py", "server", ports=("8080/tcp",))
        self.external_ip = self.address(self.external, outside)
        self.external_ipv6 = self.inspect(self.external)["NetworkSettings"]["Networks"][outside]["GlobalIPv6Address"]
        assert self.external_ipv6
        self.external_host_port = int(self.inspect(self.external)["NetworkSettings"]["Ports"]["8080/tcp"][0]["HostPort"])
        control = [{"name": kind, "kind": kind, "target": ["127.0.0.1", port]}
            for kind, port in (("tcp", 18080), ("udp", 18081), ("dns-tcp", 1053), ("dns-udp", 1053),
                ("dns-tcp", 1054), ("dns-udp", 1054))]
        control += [{"name": "ipv6-" + kind, "kind": kind, "target": ["::1", port]}
            for kind, port in (("tcp", 18080), ("udp", 18081))]
        def servers_ready():
            result = json.loads(self.command("docker", "exec", "-i", self.external, "python", "/probe.py", "probe",
                input_text=json.dumps(control)))
            return all(value["success"] for value in result)
        manage.wait("external TCP UDP DNS canaries ready", servers_ready)
        public = json.loads(self.command("docker", "exec", "-i", self.external, "python", "/probe.py", "probe",
            input_text=json.dumps([{"name": "public-dns", "kind": "resolve", "target": "example.com"}])))
        assert public[0]["success"], public
        with urllib.request.urlopen("http://127.0.0.1:" + str(self.external_host_port), timeout=5) as response:
            assert response.status == 200
        q.report["checks"]["external_canaries_are_live_before_denial_tests"] = True
        registry = self.container("registry", protected, "registry:2", ports=("5000/tcp",))
        registry_port = self.inspect(registry)["NetworkSettings"]["Ports"]["5000/tcp"][0]["HostPort"]
        tag = "127.0.0.1:" + registry_port + "/application:probe"
        self.tags.append(tag)
        self.command("docker", "tag", PYTHON_IMAGE, tag)
        self.command("docker", "push", tag, timeout=240)
        dns_port, peer_port = free_udp_port(), free_udp_port()
        while peer_port == dns_port:
            peer_port = free_udp_port()
        allowed = [{"destination": self.external_ip + "/32", "protocol": protocol, "ports": str(port)}
            for protocol, port in (("tcp", 18082), ("udp", 18083), ("tcp", 1054), ("udp", 1054))]
        config = {"network": {"isolation": {"enabled": True}, "allowed-egress": allowed,
            "native": {"dns-nameservers": ["127.0.0.1:" + str(dns_port)], "ca-file": str(self.ca),
                "allowed-egress": [{"destination": "host", "protocol": "udp", "ports": str(peer_port)}]},
            "routes": {
                "registry": {"origins": ["https://registry.isolation.test"], "target": {"backend-url": "http://" + registry + ":5000"}},
                "simulator": {"origins": ["http://api.vendor.test", "https://api.vendor.test"],
                    "target": {"backend-url": "http://" + self.simulator + ":8080"}},
                "preserved": {"origins": ["https://*.vendor.test"],
                    "target": {"backend-url": "http://" + self.simulator + ":8080", "preserve-host": True}}}}}
        self.network_file.write_text(json.dumps(config, indent=2) + "\n")
        self.floci = self.owner + "-floci"
        self.containers.append(self.floci)
        self.command("docker", "run", "-d", "--name", self.floci, "--network", protected, "--dns", "127.0.0.1",
            "--label", manage.LABEL + "=" + self.owner, "-p", "127.0.0.1::4566",
            "-p", "127.0.0.1:" + str(dns_port) + ":53/udp", "-p", "127.0.0.1:" + str(dns_port) + ":53/tcp",
            "-v", "/var/run/docker.sock:/var/run/docker.sock", "-v", str(self.network_file) + ":/etc/floci/network.json:ro",
            "-e", "FLOCI_NETWORK_CONFIG_FILE=/etc/floci/network.json", "-e", "FLOCI_DOCKER_RESOURCE_NAMESPACE=" + self.owner,
            "-e", "FLOCI_SERVICES_DOCKER_NETWORK=" + protected, "-e", "FLOCI_DNS_CONTAINER_FALLBACK_ENABLED=false",
            "-e", "FLOCI_DNS_SPOOF_AWS_ENDPOINTS=true", "-e", "QUARKUS_HTTP_HOST=0.0.0.0",
            "-e", "FLOCI_SECURITY_ALLOW_UNSAFE_NETWORK_EXPOSURE=true", q.args.floci_image, timeout=180)
        def healthy():
            state = self.inspect(self.floci)["State"]
            if not state["Running"]:
                raise RuntimeError("Floci stopped during isolated environment startup")
            return state.get("Health", {}).get("Status") == "healthy"
        manage.wait("actual Floci isolated environment healthy", healthy, timeout=180)
        self.command("docker", "cp", self.floci + ":/app/data/tls/floci-root-ca.crt", str(self.ca))
        helpers = self.command("docker", "ps", "-aq", "--filter", "label=floci.network.owner=" + self.floci_owner).splitlines()
        assert len(helpers) == 1, helpers
        self.helper = helpers[0]
        self.boundary = json.loads(self.command("docker", "exec", self.helper, "cat", "/etc/floci/network-policy.json"))
        assert self.boundary["fingerprint"] == hashlib.sha256(self.network_file.read_bytes()).hexdigest()
        q.args.environment_network, q.args.isolation_helper = protected, self.helper
        q.args.environment_dns = self.address(self.floci, protected)
        q.args.peer_udp_port = peer_port
        q.args.network_policy = True
        q.manager_args.network_policy = True
        os.environ["FLOCI_NETWORK_CONFIG_FILE"] = str(self.network_file)
        os.environ["FLOCI_MICROSANDBOX_GUEST_AGENT_PATH"] = str(Path(q.args.binaries) / "guestnet")
        q.environment.update(FLOCI_NETWORK_CONFIG_FILE=str(self.network_file),
            FLOCI_MICROSANDBOX_GUEST_AGENT_PATH=str(Path(q.args.binaries) / "guestnet"))
        q.report.update(environmentOwner=self.owner, environmentNetwork=protected,
            environmentConfig=str(self.network_file), peerUDPPort=peer_port,
            externalCanarySubnet=canary_subnet, externalCanaryIPv4=self.external_ip,
            externalCanaryIPv6=self.external_ipv6)

    def probes(self, q, values, phase):
        results = json.loads(q.kubectl("exec", "-i", "isolated-app", "--", "python", "/app/probe.py", "probe",
            input_text=json.dumps(values), timeout=180))
        recorded = {value["name"]: value for value in results}
        q.report[phase + "Probes"] = recorded
        failures = []
        for expected, actual in zip(values, results, strict=True):
            passed = actual["success"] == expected.get("allowed", True)
            q.report["checks"][phase + ":" + expected["name"]] = passed
            if not passed:
                failures.append({"expected": expected, "actual": actual})
        Path(q.args.report).write_text(json.dumps(q.report, indent=2) + "\n")
        print(json.dumps({"phase": phase, "probes": recorded}, indent=2), flush=True)
        assert not failures, failures
        return recorded

    def qualify(self, q):
        try:
            self.scenario(q)
        except Exception:
            diagnostics = {}
            operations = {
                "podStatus": lambda: q.resource("pod", "isolated-app").get("status"),
                "peerRules": lambda: self.command("docker", "exec", q.record["peer"],
                    "iptables-save", "-c", timeout=15),
                "outerRules": lambda: self.command("docker", "exec", self.helper,
                    "nft", "-j", "list", "table", "inet", self.boundary["table"], timeout=15),
            }
            if self.uid:
                operations["lifecycle"] = lambda: q.lifecycle(self.uid)
            for name, operation in operations.items():
                try:
                    diagnostics[name] = operation()
                except Exception as error:
                    diagnostics[name] = {"error": type(error).__name__ + ": " + str(error)}
            q.report["failureDiagnostics"] = diagnostics
            raise

    def scenario(self, q):
        reachable = json.loads(self.command("docker", "exec", "-i", self.external, "python", "/probe.py", "probe",
            input_text=json.dumps([{"name": "host-published", "kind": "http", "target": "http://" + q.record["hostIP"] + ":" + str(self.external_host_port)}])))
        assert reachable[0]["success"], reachable
        q.report["checks"]["exact_host_published_canary_reachable_from_unprotected_control"] = True
        q.apply({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "isolation-probe"},
            "data": {"probe.py": self.source.read_text()}})
        pod = q.pod("isolated-app", "exec python /app/probe.py app")
        pod["metadata"]["annotations"] = {"microsandbox.dev/availability": "proxy", "microsandbox.dev/idle-owner": "external"}
        pod["spec"]["containers"][0].update(image="registry.isolation.test/application:probe", imagePullPolicy="Always",
            ports=[{"name": "http", "containerPort": 8080}],
            readinessProbe={"httpGet": {"path": "/", "port": "http"}, "periodSeconds": 1, "timeoutSeconds": 2}, securityContext={"capabilities": {"drop": ["ALL"]}},
            volumeMounts=[{"name": "probe", "mountPath": "/app", "readOnly": True},
                {"name": "ca", "mountPath": "/etc/floci/certs/ca.crt", "readOnly": True}])
        pod["spec"]["volumes"] = [{"name": "probe", "configMap": {"name": "isolation-probe"}},
            {"name": "ca", "hostPath": {"path": "/etc/floci/certs/ca.crt", "type": "File"}}]
        q.apply(pod)
        q.apply({"apiVersion": "v1", "kind": "Service", "metadata": {"name": "isolated-app"},
            "spec": {"selector": {"app": "isolated-app"}, "ports": [{"name": "http", "port": 8080, "targetPort": "http"}]}})
        q.apply({"apiVersion": "v1", "kind": "Service", "metadata": {"name": "escape-canary"},
            "spec": {"ports": [{"name": "http", "port": 8080, "targetPort": 8080}]}})
        q.apply({"apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice", "metadata": {"name": "escape-canary",
            "labels": {"kubernetes.io/service-name": "escape-canary"}}, "addressType": "IPv4",
            "ports": [{"name": "http", "protocol": "TCP", "port": 8080}],
            "endpoints": [{"addresses": [self.external_ip], "conditions": {"ready": True}}]})
        def cold():
            value = q.resource("pod", "isolated-app")
            return value if any(c["type"] == "Ready" and c["status"] == "True" for c in value.get("status", {}).get("conditions", [])) else None
        observed = manage.wait("isolated native Pod proxy Ready", cold)
        uid = observed["metadata"]["uid"]
        self.uid = uid
        assert q.lifecycle(uid)["state"] == "COLD" and q.control("health")["activeMemoryMiB"] == 0
        q.report["checks"]["protected_deployment_is_ready_without_starting_native_guest"] = True
        def wake():
            try:
                with urllib.request.urlopen(q.lifecycle(uid)["proxyUrl"], timeout=240) as response:
                    return json.load(response)
            except urllib.error.HTTPError as error:
                q.report["wakeFailure"] = {"status": error.code, "body": error.read().decode(), "state": q.lifecycle(uid),
                    "pod": q.resource("pod", "isolated-app").get("status"),
                    "events": json.loads(q.kubectl("get", "events", "--field-selector", "involvedObject.uid=" + uid, "-o", "json"))}
                print(json.dumps(q.report["wakeFailure"], indent=2), flush=True)
                raise
        marker = wake()["marker"]
        q.report["checks"]["first_http_wake_pulls_private_image_through_verified_tls_gateway"] = True
        def matrix(phase):
            outside = self.external_ip
            values = [{"name": "deny-tcp", "kind": "tcp", "target": [outside, 18080], "allowed": False},
                {"name": "deny-udp", "kind": "udp", "target": [outside, 18081], "allowed": False},
                {"name": "deny-ipv6-tcp", "kind": "tcp", "target": [self.external_ipv6, 18080], "allowed": False},
                {"name": "deny-ipv6-udp", "kind": "udp", "target": [self.external_ipv6, 18081], "allowed": False},
                {"name": "deny-dns-udp", "kind": "dns-udp", "target": [outside, 1053], "allowed": False},
                {"name": "deny-dns-tcp", "kind": "dns-tcp", "target": [outside, 1053], "allowed": False},
                {"name": "deny-host-port", "kind": "http", "target": "http://" + q.record["hostIP"] + ":" + str(self.external_host_port), "allowed": False},
                {"name": "deny-sdk-host-port", "kind": "http", "target": "http://host.microsandbox.internal:" + str(self.external_host_port), "allowed": False},
                {"name": "deny-service-escape", "kind": "http", "target": "http://escape-canary.default.svc.cluster.local:8080", "allowed": False},
                {"name": "deny-unknown-dns", "kind": "resolve", "target": "example.com", "allowed": False},
                {"name": "allow-tcp", "kind": "tcp", "target": [outside, 18082]},
                {"name": "allow-udp", "kind": "udp", "target": [outside, 18083]},
                {"name": "allow-dns-udp", "kind": "dns-udp", "target": [outside, 1054]},
                {"name": "allow-dns-tcp", "kind": "dns-tcp", "target": [outside, 1054]},
                {"name": "internal-service", "kind": "http", "target": "http://isolated-app.default.svc.cluster.local:8080"},
                {"name": "aws-dns", "kind": "resolve", "target": "sqs.us-east-1.amazonaws.com"},
                {"name": "simulator-http", "kind": "http", "target": "http://api.vendor.test/path?q=%2F"},
                {"name": "simulator-tls", "kind": "http", "target": "https://api.vendor.test/path?q=%2F",
                    "ca": "/etc/floci/certs/ca.crt", "body": base64.b64encode(b"\0\1\xffpayload").decode(),
                    "headers": {"X-Forwarded-Host": "forged.external"}},
                {"name": "preserve-host", "kind": "http", "target": "https://nested.name.vendor.test/", "ca": "/etc/floci/certs/ca.crt"},
                {"name": "reject-untrusted-tls", "kind": "http", "target": "https://api.vendor.test/", "allowed": False}]
            result = self.probes(q, values, phase)
            assert result["aws-dns"]["value"] == q.args.environment_dns, result["aws-dns"]
            simulator = json.loads(result["simulator-tls"]["value"]["body"])
            assert simulator["originalHost"] == "api.vendor.test" and simulator["host"] == self.simulator + ":8080", simulator
            assert simulator["path"] == "/path?q=%2F", simulator
            assert base64.b64decode(simulator["body"]) == b"\0\1\xffpayload"
            assert "CERTIFICATE_VERIFY_FAILED" in result["reject-untrusted-tls"]["error"], result["reject-untrusted-tls"]
            assert json.loads(result["preserve-host"]["value"]["body"])["host"] == "nested.name.vendor.test"
            assert json.loads(result["internal-service"]["value"]["body"])["marker"] == marker
            return result
        matrix("initial")
        q.report["checks"]["native_direct_and_service_egress_denied_with_exact_allowances"] = True
        q.report["checks"]["controlled_dns_and_simulated_tls_preserve_http_contracts"] = True
        state = q.lifecycle(uid)
        q.lifecycle(uid, "hibernate", state["generation"])
        q.crash_and_restart()
        assert q.lifecycle(uid)["state"] == "SLEEPING"
        assert wake()["marker"] == marker
        matrix("resumed")
        q.report["checks"]["same_ram_after_restart_keeps_dns_routing_and_egress_boundary"] = True
        q.kubectl("delete", "endpointslice", "escape-canary", "--ignore-not-found")

    def cleanup(self):
        if self.floci:
            existing = self.command("docker", "ps", "-aq", "--filter", "name=^/" + self.floci + "$")
            if existing:
                assert self.inspect(self.floci)["Config"]["Labels"].get(manage.LABEL) == self.owner
                self.command("docker", "stop", "--time", "10", self.floci)
        managed = set()
        for label in ("io.floci.owner", "floci.network.owner"):
            managed.update(self.command("docker", "ps", "-aq", "--filter", "label=" + label + "=" + self.floci_owner).splitlines())
        for name in managed:
            value = self.inspect(name)
            labels = value["Config"].get("Labels") or {}
            assert self.floci_owner in (labels.get("io.floci.owner"), labels.get("floci.network.owner"))
            if labels.get("floci.network.table"):
                table = labels["floci.network.table"]
                assert table.startswith("floci_iso_") and table.removeprefix("floci_iso_").isalnum()
                if not value["State"]["Running"]:
                    self.command("docker", "start", name)
                tables = json.loads(self.command("docker", "exec", name, "nft", "-j", "list", "tables"))["nftables"]
                if any(item.get("table", {}).get("name") == table and item["table"].get("family") == "inet" for item in tables):
                    self.command("docker", "exec", name, "nft", "delete", "table", "inet", table)
            self.command("docker", "rm", "-fv", name)
        for name in reversed(self.containers):
            if self.command("docker", "ps", "-aq", "--filter", "name=^/" + name + "$"):
                assert self.inspect(name)["Config"]["Labels"].get(manage.LABEL) == self.owner
                self.command("docker", "rm", "-fv", name)
        for label in ("io.floci.owner", "floci.network.owner"):
            volumes = self.command("docker", "volume", "ls", "-q", "--filter", "label=" + label + "=" + self.floci_owner).splitlines()
            for volume in volumes:
                value = json.loads(self.command("docker", "volume", "inspect", volume))[0]
                assert value["Labels"].get(label) == self.floci_owner
                self.command("docker", "volume", "rm", volume)
        for name in reversed(self.networks):
            value = json.loads(self.command("docker", "network", "inspect", name))[0]
            assert value["Labels"].get(manage.LABEL) == self.owner
            self.command("docker", "network", "rm", name)
        for tag in self.tags:
            self.command("docker", "image", "rm", tag)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binaries", "msb", "msb-home", "peer-images", "floci-image", "report"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--work-directory", default="/tmp")
    parser.add_argument("--node-ip")
    args = parser.parse_args()
    q = ManagedQualification(args)
    fixture = EnvironmentFixture(q)
    try:
        fixture.setup()
        q.run(fixture.qualify)
    except Exception as error:
        q.report.update(failure=type(error).__name__ + ": " + str(error), traceback=traceback.format_exc())
    finally:
        if not q.report.get("cleanupFailure"):
            try:
                fixture.cleanup()
                q.report["checks"]["owned_floci_environment_and_firewall_cleanup"] = True
            except Exception as error:
                q.report["cleanupFailure"] = type(error).__name__ + ": " + str(error)
        q.log.close()
        Path(args.report).write_text(json.dumps(q.report, indent=2) + "\n")
        print(json.dumps(q.report, indent=2))
    return 1 if "failure" in q.report or "cleanupFailure" in q.report else 0


if __name__ == "__main__":
    raise SystemExit(main())
