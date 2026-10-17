"""Restricted installed policy, resource metrics and storage preview smoke."""
import json
from pathlib import Path
import sys
import time
import uuid

import manage


def qualify_installed_policy(q):
    clients = ("installed-client-a", "installed-client-b")

    def ready(name):
        value = q.resource("pod", name)
        return value if any(c["type"] == "Ready" and c["status"] == "True"
            for c in value.get("status", {}).get("conditions", [])) else None

    for index, name in enumerate(clients):
        value = q.pod(name, "exec sleep 600", native=False)
        value["metadata"]["labels"]["access"] = "yes" if index == 0 else "no"
        value["spec"]["containers"][0]["securityContext"] = {"capabilities": {"drop": ["ALL"]}}
        q.apply(value)
        manage.wait(name + " ready", lambda name=name: ready(name))

    name = "installed-policy"
    q.apply({"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": {"name": name}, "spec": {"podSelector": {"matchLabels": {"app": name}},
        "policyTypes": ["Ingress"], "ingress": [{"from": [{"podSelector": {"matchLabels": {"access": "yes"}}}],
            "ports": [{"protocol": "TCP", "port": "http"}]}]}})
    value = q.pod(name, "mkdir -p /www; cat /proc/sys/kernel/random/uuid >/www/index.html; exec httpd -f -p 8080 -h /www")
    value["metadata"]["annotations"] = {"microsandbox.dev/availability": "proxy", "microsandbox.dev/idle-owner": "external"}
    value["spec"]["containers"][0].update(ports=[{"name": "http", "containerPort": 8080}],
        securityContext={"capabilities": {"drop": ["ALL"]}})
    q.apply(value)
    observed = manage.wait("cold proxy Ready", lambda: ready(name), timeout=120)
    uid, address = observed["metadata"]["uid"], observed["status"]["podIP"]
    assert q.lifecycle(uid)["state"] == "COLD"
    assert q.control("health")["activeMemoryMiB"] == 0

    def metrics(awake):
        for _ in range(2 if awake else 1):
            prefix = "/api/v1/nodes/" + q.record["node"] + "/proxy/"
            text = q.kubectl("get", "--raw", prefix + "metrics/resource")
            stats = json.loads(q.kubectl("get", "--raw", prefix + "stats/summary"))
            if awake:
                assert "container_cpu_usage_seconds_total" in text and "container_memory_working_set_bytes" in text, text
                assert len(stats["pods"]) == 1 and stats["pods"][0]["podRef"]["uid"] == uid, stats
                assert all(c["memory"]["workingSetBytes"] > 0 for c in stats["pods"][0]["containers"]), stats
                time.sleep(1)
            else:
                assert not stats["pods"] and not any(line.startswith("container_") for line in text.splitlines()), stats
                assert q.control("health")["activeMemoryMiB"] == 0

    def fetch(source, timeout=2):
        return q.kubectl("exec", source, "--", "sh", "-c",
            f"wget -T {timeout} -qO- http://{address}:8080/ 2>/dev/null || printf DENIED")

    metrics(False)
    assert fetch(clients[1]) == "DENIED"
    assert q.lifecycle(uid)["state"] == "COLD"
    q.report["checks"]["installed_policy_denial_and_cold_metrics_do_not_start_guest"] = True
    marker = manage.wait("selected peer wakes native HTTP app",
        lambda: result if (result := fetch(clients[0], 45)) != "DENIED" else None, timeout=360)
    uuid.UUID(marker)
    metrics(True)
    q.report["checks"]["installed_kubelet_reports_awake_resources_under_policy"] = True
    q.kubectl("label", "pod", clients[0], "access=no", "--overwrite")
    q.kubectl("label", "pod", clients[1], "access=yes", "--overwrite")
    manage.wait("restricted watch revokes old peer", lambda: fetch(clients[0]) == "DENIED")
    manage.wait("restricted watch admits new peer", lambda: fetch(clients[1]) == marker)
    q.report["checks"]["installed_restricted_policy_watch_reconciles_live_peer_labels"] = True

    state = q.lifecycle(uid)
    q.lifecycle(uid, "hibernate", state["generation"])
    metrics(False)
    q.report["checks"]["installed_sleeping_resource_scrape_omits_samples_without_wake"] = True
    cli = (sys.executable, str(Path(manage.__file__).resolve()))
    before = json.loads(q.command(*cli, "storage", "--directory", str(q.directory)))
    preview = json.loads(q.command(*cli, "prune", "--directory", str(q.directory), "--images", "--older-than-seconds", "0"))
    after = json.loads(q.command(*cli, "storage", "--directory", str(q.directory)))
    assert preview["dryRun"] is True
    assert before["workloads"] == after["workloads"] and before["workloads"][0]["snapshots"], (before, after)
    assert q.lifecycle(uid)["state"] == "SLEEPING" and q.control("health")["activeMemoryMiB"] == 0
    q.report["checks"]["installed_storage_cli_preview_preserves_sleeping_checkpoint"] = True
    q.crash_and_restart()
    assert fetch(clients[0]) == "DENIED"
    assert q.lifecycle(uid)["state"] == "SLEEPING"
    assert fetch(clients[1], 45) == marker
    q.report["checks"]["supervised_restart_retains_policy_and_restores_same_ram"] = True
