"""Real Service wake and HTTP request leases across native Pod hibernation."""
import http.client
import socket
import time
from urllib.parse import urlparse


_ACTIVITY_CGI = r'''set -eu
mkdir -p /www/cgi-bin
cat /proc/sys/kernel/random/uuid >/dev/shm/http-activity-marker
rm -f /tmp/http-activity-release
cat >/www/cgi-bin/http-activity <<'CGI'
#!/bin/sh
set -eu
marker=$(cat /dev/shm/http-activity-marker)
boot=$(cat /proc/sys/kernel/random/boot_id)
started=$(awk '{print $22}' /proc/1/stat)
body=$(printf '%s\n%s\n%s' "$marker" "$boot" "$started")
size=$(printf '%s' "$body" | wc -c)
printf 'Content-Type: text/plain\r\nContent-Length: %s\r\n\r\n' "$size"
printf '%s' "$marker"
if test "${QUERY_STRING:-}" = slow=1; then
    while test ! -f /tmp/http-activity-release; do sleep .1; done
fi
printf '\n%s\n%s' "$boot" "$started"
CGI
chmod 755 /www/cgi-bin/http-activity
'''


def inactive(state):
    return state["activeRequests"] == 0 and state["activeConnections"] == 0


def verify_http_activity(*, name, uid, lifecycle, kubectl, wait, report, idle_seconds):
    """Also used by the larger proxy/scale fixture instead of its old TCP lease check."""
    kubectl("exec", "-i", name, "--", "sh", "-s", input_text=_ACTIVITY_CGI)
    target = urlparse(lifecycle(uid)["proxyUrl"])
    connection = http.client.HTTPConnection(target.hostname, target.port, timeout=240)
    try:
        connection.connect()
        original_socket = connection.sock
        connection.request("GET", "/cgi-bin/http-activity")
        response = connection.getresponse()
        identity = response.read()
        assert response.status == 200 and not response.will_close, {
            "phase": "initial identity", "status": response.status,
            "body": identity.decode(errors="replace"), "willClose": response.will_close}
        parts = identity.splitlines()
        assert len(parts) == 3 and parts[0] and parts[1] and int(parts[2]) > 0, identity
        assert connection.sock is original_socket, "initial response closed frontend keepalive"
        wait("initial HTTP request releases activity", lambda: inactive(lifecycle(uid)), 30)
        before = lifecycle(uid)
        connection.request("GET", "/cgi-bin/http-activity?slow=1")
        response = connection.getresponse()
        if response.status != 200 or response.will_close:
            raise AssertionError({"phase": "slow response", "status": response.status,
                "body": response.read().decode(errors="replace"), "willClose": response.will_close})
        prefix = response.read(len(parts[0]))
        assert prefix == parts[0], {"phase": "partial response", "expected": parts[0], "actual": prefix}
        wait("partial HTTP response retains request activity", lambda: lifecycle(uid)["activeRequests"] == 1, 30)
        state = lifecycle(uid)
        assert state["activeConnections"] == 0, state
        assert state["activitySequence"] > before["activitySequence"], state
        try:
            lifecycle(uid, "hibernate", state["generation"])
        except RuntimeError as error:
            assert "409" in str(error) and "active" in str(error), error
        else:
            raise AssertionError("an in-flight HTTP response allowed manual hibernation")
        deadline = time.monotonic() + idle_seconds + 3
        while time.monotonic() < deadline:
            state = lifecycle(uid)
            assert state["state"] in ("RUNNING", "READY") and state["activeRequests"] == 1, state
            time.sleep(1)
        report["checks"]["http_response_blocks_manual_and_automatic_hibernation"] = True
        kubectl("exec", name, "--", "touch", "/tmp/http-activity-release")
        completed = prefix + response.read()
        assert completed == identity, {"phase": "completed response", "expected": identity, "actual": completed}
        wait("completed HTTP response releases activity", lambda: inactive(lifecycle(uid)), 30)
        sequence = lifecycle(uid)["activitySequence"]
        sleeping = wait("idle HTTP keepalive permits automatic hibernation", lambda:
            (state if (state := lifecycle(uid))["state"] == "SLEEPING" else None), 120)
        assert inactive(sleeping) and connection.sock is original_socket, sleeping
        assert sleeping["activitySequence"] == sequence, "internal probes renewed HTTP activity"
        report["checks"]["idle_http_keepalive_allows_automatic_hibernation"] = True
        wake = {"expectedIdentity": identity.decode(), "sleeping": sleeping}
        report["http_activity_same_connection_wake"] = wake
        try:
            connection.request("GET", "/cgi-bin/http-activity")
            response = connection.getresponse()
            wake["status"] = response.status
            body = response.read()
            wake["body"] = body.decode(errors="replace")
        except Exception as error:
            wake["transportError"] = str(error) or type(error).__name__
            raise
        finally:
            wake["sameConnection"] = connection.sock is original_socket
            try:
                wake["lifecycle"] = lifecycle(uid)
            except Exception as error:
                wake["lifecycleError"] = str(error) or type(error).__name__
        assert response.status == 200 and body == identity, wake
        assert wake["sameConnection"], wake
        assert "lifecycle" in wake, wake
        restored = wake["lifecycle"]
        assert restored["state"] in ("RUNNING", "READY") and restored["generation"] > sleeping["generation"], wake
        report["checks"]["same_http_connection_wakes_preserved_memory_boot_and_process"] = True
        return identity.decode()
    finally:
        connection.close()


def qualify_http_activity(*, pod, apply, resource, kubectl, ready, lifecycle,
                          control_get, wait, report):
    name = "native-http-activity"
    docker_client, native_client = "http-docker-client", "http-native-client"
    apply(pod(docker_client, "exec sleep 900", False))
    wait("HTTP Docker client ready", lambda: ready(docker_client), 300)
    value = pod(name, "mkdir -p /www; echo ready >/www/index.html; exec httpd -f -p 8080 -h /www")
    # Omit proxy-protocol deliberately: the default must use HTTP request activity.
    value["metadata"]["annotations"] = {
        "microsandbox.dev/availability": "proxy", "microsandbox.dev/idle-timeout-seconds": "5"}
    app = value["spec"]["containers"][0]
    app["ports"] = [{"containerPort": 8080}]
    app["readinessProbe"] = {"httpGet": {"path": "/", "port": 8080}, "periodSeconds": 1}
    apply(value)
    apply({"apiVersion": "v1", "kind": "Service", "metadata": {"name": name},
           "spec": {"selector": {"app": name}, "ports": [{"port": 80, "targetPort": 8080}]}})
    observed = wait("HTTP proxy pod Ready while cold", lambda: ready(name), 180)
    uid = observed["metadata"]["uid"]
    state = lifecycle(uid)
    assert state["state"] == "COLD" and control_get("health")["activeMemoryMiB"] == 0, state
    target = urlparse(state["proxyUrl"])
    with socket.create_connection((target.hostname, target.port), timeout=10):
        # Stay below the HTTP header timeout: a connected client has sent no request.
        time.sleep(3)
        state = lifecycle(uid)
        assert state["state"] == "COLD" and inactive(state), state
        health = control_get("health")
        assert health["activeMemoryMiB"] == 0, {"phase": "bare connection", "lifecycle": state, "health": health}
    report["checks"]["bare_idle_http_connection_does_not_wake_cold_pod"] = True
    service_address = resource("service", name)["spec"]["clusterIP"]
    def service_dns_ready():
        # BusyBox nslookup's short-name search results are not a readiness signal.
        # Keep ordinary short Service names in wget below to test application DNS.
        lookup = kubectl("exec", docker_client, "--", "sh", "-c",
            'nslookup "$1" 2>&1; code=$?; printf "\\n__FLOCI_DNS_EXIT__=%s\\n" "$code"',
            "--", name+".default.svc.cluster.local.")
        output, separator, code = lookup.rpartition("\n__FLOCI_DNS_EXIT__=")
        if not separator:
            raise RuntimeError("DNS probe did not return its exit status: "+lookup)
        report["http_activity_dns"] = {"output": output, "exitCode": int(code),
            "expectedServiceIP": service_address}
        if int(code) != 0 or service_address not in output.split():
            raise RuntimeError("Service DNS did not return its ClusterIP: "+lookup)
        return True
    wait("HTTP activity Service DNS", service_dns_ready, 60)
    body = kubectl("exec", docker_client, "--", "wget", "-T", "180", "-qO-", "http://"+name+"/", timeout=210)
    state = lifecycle(uid)
    assert body == "ready" and state["state"] in ("RUNNING", "READY"), {
        "phase": "Docker Service cold wake", "body": body, "lifecycle": state}
    report["checks"]["docker_service_http_request_wakes_cold_default_proxy"] = True
    identity = verify_http_activity(name=name, uid=uid, lifecycle=lifecycle,
        kubectl=kubectl, wait=wait, report=report, idle_seconds=5)
    apply(pod(native_client, "exec sleep 900"))
    wait("HTTP native client ready", lambda: ready(native_client), 300)
    for client in (docker_client, native_client):
        wait(client+" sees target hibernate", lambda: lifecycle(uid)["state"] == "SLEEPING", 120)
        body = kubectl("exec", client, "--", "wget", "-T", "180", "-qO-",
            "http://"+name+"/cgi-bin/http-activity", timeout=210)
        state = lifecycle(uid)
        assert body == identity and state["state"] in ("RUNNING", "READY"), {
            "phase": client+" Service wake", "body": body, "expectedIdentity": identity, "lifecycle": state}
        pod_ip = resource("pod", name)["status"]["podIP"]
        body = kubectl("exec", client, "--", "wget", "-T", "30", "-qO-",
            f"http://{pod_ip}:8080/cgi-bin/http-activity")
        assert body == identity, {"phase": client+" PodIP", "body": body, "expectedIdentity": identity}
    report["checks"]["docker_and_native_service_and_podip_requests_preserve_ram"] = True
    kubectl("delete", "pod", name, docker_client, native_client, "--wait=true", timeout=180)
    kubectl("delete", "service", name)
    wait("HTTP activity fixture releases guest RAM", lambda: control_get("health")["activeMemoryMiB"] == 0, 180)
