#!/usr/bin/env python3
"""Disconnect HTTP during accepted cold/restore mutations in an owned managed cluster."""
import argparse
import json
import select
import socket
import time
import urllib.parse
import urllib.request

import manage
from qualify_managed import ManagedQualification


def disconnect_during_mutation(q, state, operation):
    endpoint = urllib.parse.urlsplit(state["proxyUrl"])
    marker = q.directory/"controller"/"native-command.pending"
    with socket.create_connection((endpoint.hostname, endpoint.port), timeout=10) as client:
        client.sendall(("GET /cgi-bin/data HTTP/1.1\r\nHost: "+endpoint.netloc+
                        "\r\nConnection: close\r\n\r\n").encode())
        deadline = time.monotonic()+120
        while time.monotonic() < deadline:
            try:
                record = json.loads(marker.read_text())
            except (FileNotFoundError, json.JSONDecodeError):
                record = {}
            if record.get("operation") == operation and record.get("phase") == "intent":
                assert record.get("target") == state["vm"], record
                client.shutdown(socket.SHUT_RDWR)
                return
            if select.select([client], [], [], 0)[0]:
                response = client.recv(4096)
                raise AssertionError("HTTP finished before observed "+operation+": "+repr(response))
            time.sleep(0.005)
    raise AssertionError("did not observe accepted "+operation+" before disconnect")


def qualify(q):
    name = "cancelled-wake"
    script = """mkdir -p /www/cgi-bin
export RAM_MARKER="$(cat /proc/sys/kernel/random/uuid)"
cat >/www/cgi-bin/data <<'CGI'
#!/bin/sh
printf 'Content-Type: text/plain\\r\\n\\r\\n'
printf '%s\\n' "$RAM_MARKER"
CGI
chmod +x /www/cgi-bin/data
exec httpd -f -p 8080 -h /www
"""
    pod = q.pod(name, script)
    pod["metadata"]["annotations"] = {"microsandbox.dev/availability": "proxy",
        "microsandbox.dev/idle-owner": "controller", "microsandbox.dev/idle-timeout-seconds": "8"}
    pod["spec"]["containers"][0]["ports"] = [{"name": "http", "containerPort": 8080}]
    q.apply(pod)
    uid = q.resource("pod", name)["metadata"]["uid"]

    def state_is(expected):
        state = q.lifecycle(uid)
        assert state["state"] != "FAILED", state
        return state if state["state"] == expected else None

    manage.wait("Kubernetes proxy Ready", lambda: any(
        condition["type"] == "Ready" and condition["status"] == "True"
        for condition in q.resource("pod", name).get("status", {}).get("conditions", [])))
    state = manage.wait("proxy-ready cold Pod", lambda: state_is("COLD"))
    assert q.control("health")["activeMemoryMiB"] == 0
    disconnect_during_mutation(q, state, "run")
    state = manage.wait("accepted cold wake completed after client disconnect", lambda: state_is("READY"))
    assert not (q.directory/"controller"/"native-command.pending").exists()
    with urllib.request.urlopen(state["proxyUrl"]+"/cgi-bin/data", timeout=180) as response:
        original = response.read().decode().strip()
        assert response.status == 200 and original
    q.report["checks"]["accepted_cold_wake_survives_client_disconnect"] = True
    state = manage.wait("disconnected cold request releases idle activity", lambda: state_is("SLEEPING"))
    assert state["activeRequests"] == 0
    assert q.control("health")["activeMemoryMiB"] == 0
    q.report["checks"]["cancelled_request_releases_activity_and_allows_idle_sleep"] = True
    disconnect_during_mutation(q, state, "rm")
    state = manage.wait("accepted restore completed after client disconnect", lambda: state_is("READY"))
    assert not (q.directory/"controller"/"native-command.pending").exists()
    with urllib.request.urlopen(state["proxyUrl"]+"/cgi-bin/data", timeout=180) as response:
        assert response.status == 200 and response.read().decode().strip() == original
    q.report["checks"]["accepted_restore_survives_client_disconnect_without_reboot"] = True
    state = manage.wait("restored Pod returns to idle sleep", lambda: state_is("SLEEPING"))
    assert state["activeRequests"] == 0
    assert not (q.directory/"controller"/"native-command.pending").exists()
    assert q.control("health")["activeMemoryMiB"] == 0
    q.report["checks"]["restored_request_releases_activity_and_allows_idle_sleep"] = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binaries", "msb", "msb-home", "report"):
        parser.add_argument("--"+name, required=True)
    parser.add_argument("--node-ip")
    parser.add_argument("--work-directory", default="/tmp")
    args = parser.parse_args()
    args.memory_mib = 768
    return ManagedQualification(args).run(qualify)


if __name__ == "__main__":
    raise SystemExit(main())
