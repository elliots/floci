"""Real Kubernetes NetworkPolicy contracts on Docker and native Pods."""
import http.server
import hashlib
import json
import threading
import time

from manage import node_address


def qualify_network_policy(*, args, peer, pod, apply, resource, kubectl, ready,
                           lifecycle, control_get, crash_and_restart, wait, report,
                           command, environment, worker_modes=(False, True)):
    clients = ("policy-client-a", "policy-client-b")
    for name in clients:
        value = pod(name, "mkdir -p /www; echo collector >/www/index.html; exec httpd -f -p 8081 -h /www", native=False)
        value["metadata"]["labels"]["access"] = "yes" if name.endswith("a") else "no"
        apply(value)
        wait(name + " ready", lambda name=name: ready(name))

    def fetch(source, address, port=8080, timeout=2):
        return kubectl("exec", source, "--", "sh", "-c",
                       f"wget -T {timeout} -qO- http://{address}:{port}/ 2>/dev/null || printf DENIED")

    def policy(name, direction, rules):
        return {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
                "metadata": {"name": name + "-" + direction.lower()},
                "spec": {"podSelector": {"matchLabels": {"app": name}},
                         "policyTypes": [direction], direction.lower(): rules}}

    for native in worker_modes:
        kind = "native" if native else "docker"
        name = "policy-" + kind
        ingress = [{"from": [{"podSelector": {"matchLabels": {"access": "yes"}}}],
                    "ports": [{"port": "http", "protocol": "TCP"}]}]
        apply(policy(name, "Ingress", ingress))
        value = pod(name, "mkdir -p /www /state; echo policy-$(cat /proc/sys/kernel/random/uuid) >/www/index.html; "
                    "httpd -p 8080 -h /www; while ! test -e /state/restart; do sleep .2; done; rm /state/restart; exit 17", native=native)
        value["spec"]["restartPolicy"] = "Always"
        value["spec"]["volumes"] = [{"name": "state", "emptyDir": {}}]
        container = value["spec"]["containers"][0]
        container.update({"imagePullPolicy": "Always", "ports": [{"name": "http", "containerPort": 8080}],
                          "securityContext": {"capabilities": {"drop": ["ALL"]}},
                          "volumeMounts": [{"name": "state", "mountPath": "/state"}]})
        if native:
            value["metadata"]["annotations"] = {"microsandbox.dev/availability": "proxy", "microsandbox.dev/idle-owner": "external"}
        apply(value)
        try:
            observed = wait(kind + " endpoint ready", lambda: ready(name), 360)
            uid, address = observed["metadata"]["uid"], observed["status"]["podIP"]
            if native:
                assert lifecycle(uid)["state"] == "COLD"
                assert fetch(clients[1], address) == "DENIED"
                assert lifecycle(uid)["state"] == "COLD", "denied ingress woke native workload"
            marker = wait(kind + " selected client allowed", lambda: (result if (result := fetch(clients[0], address, timeout=45)).startswith("policy-") else None), 360)
            assert fetch(clients[1], address) == "DENIED"
            report["checks"][kind + "_selector_named_port_ingress"] = True
            kubectl("label", "pod", clients[0], "access=no", "--overwrite")
            kubectl("label", "pod", clients[1], "access=yes", "--overwrite")
            wait(kind + " source labels revoke old client", lambda: fetch(clients[0], address) == "DENIED")
            wait(kind + " source labels admit new client", lambda: fetch(clients[1], address) == marker)
            report["checks"][kind + "_live_peer_labels_preserve_original_source"] = True

            collector = resource("pod", clients[0])["status"]["podIP"]
            assert fetch(name, collector, 8081) == "collector"
            apply(policy(name, "Egress", []))
            wait(kind + " live egress deny", lambda: fetch(name, collector, 8081) == "DENIED")
            report["checks"][kind + "_live_default_deny_egress"] = True
            kubectl("exec", name, "--", "touch", "/state/restart")
            wait(kind + " Always pull despite application egress deny", lambda: any(c.get("restartCount", 0) >= 1 and c.get("ready")
                 for c in resource("pod", name).get("status", {}).get("containerStatuses", [])), 360)
            assert fetch(name, collector, 8081) == "DENIED"
            report["checks"][kind + "_always_pull_keeps_node_traffic_separate"] = True

            if native:
                wait("native restart application serves", lambda: fetch(clients[1], address).startswith("policy-"))
                marker = fetch(clients[1], address)
                command("docker", "cp", args.policy_probe_binary, peer + ":/tmp/floci-policy-probe")
                kubectl("cp", "/tmp/floci-policy-probe", name + ":/tmp/floci-policy-probe")
                assert "cannot forge" in kubectl("exec", name, "--", "/tmp/floci-policy-probe")
                report["checks"]["native_so_mark_and_raw_packet_escape_denied"] = True
                state = lifecycle(uid)
                ipv6_gateway = command(args.msb, "exec", "--no-stdin", state["vm"], "--", "sh", "-ec", "ip -6 route show default | awk 'NR == 1 { print $3 }'", env=environment)
                assert "IPv6 direct egress denied" in kubectl("exec", name, "--", "/tmp/floci-policy-probe", "egress6", ipv6_gateway)
                report["checks"]["native_ipv6_cannot_bypass_ipv4_egress_isolation"] = True
                lifecycle(uid, "hibernate", state["generation"])
                kubectl("label", "pod", clients[0], "access=yes", "--overwrite")
                kubectl("label", "pod", clients[1], "access=no", "--overwrite")
                time.sleep(2)
                assert fetch(clients[1], address) == "DENIED"
                assert lifecycle(uid)["state"] == "SLEEPING"
                crash_and_restart()
                assert lifecycle(uid)["state"] == "SLEEPING"
                assert fetch(clients[1], address) == "DENIED"
                assert fetch(clients[0], address, timeout=45) == marker
                assert fetch(name, collector, 8081) == "DENIED"
                report["checks"]["native_resume_uses_latest_policy_before_thaw_retains_ram"] = True

                class DirectHandler(http.server.BaseHTTPRequestHandler):
                    def do_GET(self):
                        self.send_response(200); self.end_headers(); self.wfile.write(b"direct-egress")
                    def log_message(self, *_):
                        pass
                server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), DirectHandler)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                try:
                    state = lifecycle(uid)
                    gateway = command(args.msb, "exec", "--no-stdin", state["vm"], "--", "sh", "-ec", "ip -4 route show default | awk 'NR == 1 { print $3 }'", env=environment)
                    port = server.server_address[1]
                    assert fetch(name, gateway, port) == "DENIED"
                    direct = [{"to": [{"ipBlock": {"cidr": gateway + "/32"}}], "ports": [{"port": port, "protocol": "TCP"}]}]
                    apply(policy(name, "Egress", direct))
                    wait("direct guest egress admitted only by exact IP and port", lambda: fetch(name, gateway, port) == "direct-egress")
                    apply(policy(name, "Egress", []))
                    wait("direct guest egress revoked live", lambda: fetch(name, gateway, port) == "DENIED")
                    report["checks"]["native_direct_guest_egress_ipblock_port_and_revocation"] = True
                finally:
                    server.shutdown(); server.server_close(); thread.join(timeout=5)

            dns = [{"to": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "kube-system"}},
                             "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}}}],
                    "ports": [{"port": 53, "protocol": "UDP"}, {"port": 53, "protocol": "TCP"}]}]
            apply(policy(name, "Egress", dns))
            wait(kind + " explicit CoreDNS policy", lambda: "Address" in kubectl("exec", name, "--", "nslookup", "kubernetes.default.svc.cluster.local"))
            assert fetch(name, collector, 8081) == "DENIED"
            report["checks"][kind + "_namespace_selector_and_explicit_dns_allow"] = True
            if native:
                peer_info = json.loads(command("docker", "inspect", peer))[0]
                peer_port = peer_info["NetworkSettings"]["Ports"]["8472/udp"][0]["HostPort"]
                peer_ip = next(iter(peer_info["NetworkSettings"]["Networks"].values()))["IPAddress"]
                kubectl("delete", "networkpolicy", name + "-ingress", name + "-egress")
                time.sleep(2)
                assert "cannot forge" in kubectl("exec", name, "--", "/tmp/floci-policy-probe", "8472", peer_port)
                report["checks"]["native_unselected_udp_transport_escape_denied"] = True
                kubectl("cp", "/tmp/floci-policy-probe", clients[0] + ":/tmp/floci-policy-probe")
                chain = "FNP" + hashlib.sha256(report["owner"].encode()).hexdigest()[:12] + "T"
                def drops(table="raw", name=chain):
                    output = command("docker", "exec", peer, "iptables", "-t", table, "-v", "-x", "-n", "-L", name)
                    return sum(int(line.split()[0]) for line in output.splitlines() if "DROP" in line)
                before = drops()
                kubectl("exec", clients[0], "--", "/tmp/floci-policy-probe", "send", peer_ip, "8472")
                kubectl("exec", clients[0], "--", "/tmp/floci-policy-probe", "send", node_address({"peer": peer}), peer_port)
                assert drops() >= before + 2, "CNI sender reached direct or host-published VXLAN transport"
                report["checks"]["docker_cni_cannot_forge_vxlan_via_peer_or_host_publication"] = True
                origin = chain[:-1] + "S"
                before = drops("raw", origin)
                kubectl("exec", clients[0], "--", "/tmp/floci-policy-probe", "spoof", address, peer_ip)
                kubectl("exec", clients[0], "--", "/tmp/floci-policy-probe", "spoof", peer_ip, address)
                assert drops("raw", origin) >= before + 2, "CNI sender forged native or trusted-node source addresses"
                report["checks"]["docker_cni_cannot_spoof_native_inner_source"] = True
        finally:
            kubectl("delete", "pod", name, "--ignore-not-found", "--grace-period=0", "--wait=true", timeout=180)
            kubectl("delete", "networkpolicy", name + "-ingress", name + "-egress", "--ignore-not-found")
            kubectl("label", "pod", clients[0], "access=yes", "--overwrite")
            kubectl("label", "pod", clients[1], "access=no", "--overwrite")
    unsafe = pod("policy-unsafe", "exec sleep 600")
    apply(unsafe)
    wait("node-wide policy rejects unselected raw capabilities", lambda: "NET_RAW" in kubectl("get", "events", "--field-selector", "involvedObject.name=policy-unsafe", "-o", "json"))
    assert not json.loads(command(args.msb, "ls", "--format", "json", env=environment))
    report["checks"]["native_node_wide_unsafe_capabilities_rejected_before_boot"] = True
    kubectl("delete", "pod", "policy-unsafe", "--wait=true", timeout=180)
    safe = pod("policy-unsupported", "exec sleep 600")
    safe["spec"]["containers"][0]["securityContext"] = {"capabilities": {"drop": ["ALL"]}}
    apply(safe)
    observed = wait("safe unselected Pod ready", lambda: ready("policy-unsupported"))
    uid = observed["metadata"]["uid"]
    apply(policy("policy-unsupported", "Ingress", [{"ports": [{"port": 80, "protocol": "SCTP"}]}]))
    wait("live unsupported policy confirms quarantine", lambda: resource("pod", "policy-unsupported").get("status", {}).get("phase") == "Failed")
    assert "TCP and UDP" in resource("pod", "policy-unsupported")["status"]["message"]
    state = lifecycle(uid)
    inspected = json.loads(command(args.msb, "inspect", state["vm"], "--format", "json", env=environment))
    assert str(inspected["status"]).lower() == "paused", inspected
    report["checks"]["native_live_unsupported_selection_confirms_vm_pause"] = True
    kubectl("delete", "pod", "policy-unsupported", "--wait=true", timeout=180)
    kubectl("delete", "networkpolicy", "policy-unsupported-ingress")
    kubectl("delete", "pod", *clients, "--wait=true", timeout=180)
    wait("policy fixture releases RAM", lambda: control_get("health")["activeMemoryMiB"] == 0)
