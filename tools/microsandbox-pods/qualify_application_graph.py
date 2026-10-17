#!/usr/bin/env python3
"""Shared Docker/native HTTP and SQS applications using a real Aurora microVM."""
import argparse
import functools
import http.server
import json
import os
from pathlib import Path
import platform
import re
import secrets
import signal
import subprocess
import threading
import urllib.error
import urllib.parse
import urllib.request

import manage
from qualify_managed import ManagedQualification


def atomic_json(path, value):
    pending = path.with_suffix(".pending")
    pending.write_text(json.dumps(value))
    pending.replace(path)


def terminate(process):
    if process is None or process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass  # The owned process group exited between the wait and signal.
        process.wait(timeout=30)


class ApplicationGraph:
    def __init__(self, q):
        self.q = q
        self.directory = q.root / "application-graph"
        self.directory.mkdir(mode=0o700)
        self.java = None
        self.host = None
        self.binary_server = None
        self.sequence = 0
        self.created = []
        self.helper_token = secrets.token_hex(32)

    def helper(self, path, data=None):
        request = urllib.request.Request(self.helper_url+path,
            data=json.dumps(data).encode() if data is not None else None,
            headers={"Authorization": "Bearer "+self.helper_token, "Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)

    def command(self, operation, **values):
        self.sequence += 1
        path = self.directory / ("receipt-"+str(self.sequence)+".json")
        atomic_json(self.directory / ("command-"+str(self.sequence)+".json"),
                    dict(operation=operation, **values))
        def receipt():
            if self.java.poll() is not None:
                raise RuntimeError("application graph JVM exited before command receipt")
            return json.loads(path.read_text()) if path.exists() else None
        result = manage.wait("application graph "+operation, receipt, timeout=180)
        assert result.get("success"), result
        return result

    def prepare(self):
        q = self.q
        binaries = self.directory / "binary"
        binaries.mkdir()
        architecture = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64"}[platform.machine()]
        q.command("go", "build", "-o", str(binaries/"probe"), ".",
            cwd=Path(__file__).parent/"qualification"/"applicationgraph",
            env=dict(os.environ, GOOS="linux", GOARCH=architecture, CGO_ENABLED="0"), timeout=300)
        handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(binaries))
        self.binary_server = http.server.ThreadingHTTPServer(("0.0.0.0", 0), handler)
        threading.Thread(target=self.binary_server.serve_forever, daemon=True).start()
        self.endpoint("graph-binary", 80, self.binary_server.server_port)
        self.binary_url = "http://graph-binary.default.svc.cluster.local/probe"
        helper_log = self.directory/"host.log"
        with helper_log.open("w") as output:
            self.host = subprocess.Popen([str(Path(q.args.binaries)/"microsandbox-host")],
                env=dict(q.environment, PORT="0", QUARKUS_HTTP_PORT="0",
                    MICROSANDBOX_EXECUTABLE=q.args.msb, MICROSANDBOX_HOME=q.args.msb_home,
                    MICROSANDBOX_TOKEN=self.helper_token), stdout=output, stderr=output, start_new_session=True)
        def listening():
            if self.host.poll() is not None:
                raise RuntimeError("graph native host helper exited; inspect "+str(helper_log))
            match = re.search(r"address=(127\.0\.0\.1:\d+)", helper_log.read_text())
            return "http://"+match.group(1) if match else None
        self.helper_url = manage.wait("shared native host helper", listening, timeout=30)
        fixture = {"owner": q.record["owner"], "hostIP": q.record["hostIP"], "namespace": "default",
            "controllerURL": "http://127.0.0.1:"+str(q.record["controlPort"]),
            "controllerToken": manage.read_private(q.directory/"control-token").strip(),
            "frontendDeployment": "graph-native-http", "workerDeployment": "graph-native-worker"}
        atomic_json(self.directory/"fixture.json", fixture)
        environment = dict(q.environment, FLOCI_NATIVE_GRAPH_TEST="true",
            FLOCI_NATIVE_GRAPH_DIRECTORY=str(self.directory), FLOCI_TEST_RUNTIME="microsandbox",
            FLOCI_TEST_MICROSANDBOX_URL=self.helper_url, FLOCI_TEST_MICROSANDBOX_TOKEN=self.helper_token)
        if q.args.java_home:
            environment["JAVA_HOME"] = q.args.java_home
        self.java = subprocess.Popen([str(Path(q.args.floci_checkout)/"mvnw"), "-o",
            "-Dtest=NativeApplicationGraphIntegrationTest", "test"], cwd=q.args.floci_checkout,
            env=environment, stdout=q.log, stderr=q.log, start_new_session=True)
        def ready():
            if self.java.poll() is not None:
                raise RuntimeError("graph AWS fixture JVM exited before readiness; inspect qualification log")
            path = self.directory/"ready.json"
            return json.loads(path.read_text()) if path.exists() else None
        self.settings = manage.wait("Aurora and SQS AWS SDK fixture ready", ready, timeout=600)
        self.endpoint("graph-aws", 4566, urllib.parse.urlparse(self.settings["endpoint"]).port)
        self.endpoint("graph-db", 5432, self.settings["dbPort"])
        assert self.helper("/v1/capacity")["memoryMiB"] == 0
        q.report["checks"]["aurora_initially_paused_with_shared_capacity_released"] = True

    def endpoint(self, name, port, target):
        self.q.apply({"apiVersion": "v1", "kind": "Service", "metadata": {"name": name},
            "spec": {"ports": [{"name": "tcp", "port": port, "protocol": "TCP"}]}})
        self.q.apply({"apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice",
            "metadata": {"name": name, "labels": {"kubernetes.io/service-name": name,
                "endpointslice.kubernetes.io/managed-by": "floci-qualification"}}, "addressType": "IPv4",
            "ports": [{"name": "tcp", "protocol": "TCP", "port": target}],
            "endpoints": [{"addresses": [self.q.record["hostIP"]], "conditions": {"ready": True}}]})

    def deploy(self, kind, worker):
        q = self.q
        name = "graph-"+kind+("-worker" if worker else "-http")
        native = kind == "native"
        script = "wget -T 60 -O /tmp/application "+self.binary_url+"; chmod 700 /tmp/application; exec /tmp/application"
        pod = q.pod(name, script, native=native)
        pod["spec"]["restartPolicy"] = "Always"
        pod["spec"]["terminationGracePeriodSeconds"] = 5
        queue = urllib.parse.urlparse(self.settings["queueURL"])
        environment = {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "AWS_REGION": "us-east-1",
            "AWS_ENDPOINT_URL_SQS": "http://graph-aws.default.svc.cluster.local:4566", "AWS_EC2_METADATA_DISABLED": "true",
            "GRAPH_QUEUE_URL": "http://graph-aws.default.svc.cluster.local:4566"+queue.path,
            "GRAPH_WORKER": str(worker).lower(), "PGHOST": "graph-db.default.svc.cluster.local",
            "PGPORT": "5432", "PGUSER": self.settings["dbUser"], "PGPASSWORD": self.settings["dbPassword"],
            "PGDATABASE": self.settings["dbName"], "PGSSLMODE": "disable", "PGCONNECT_TIMEOUT": "150"}
        pod["spec"]["containers"][0].update(ports=[{"name": "http", "containerPort": 8080}],
            env=[{"name": key, "value": value} for key, value in environment.items()],
            readinessProbe={"httpGet": {"port": 8080, "path": "/healthz"}, "periodSeconds": 1})
        metadata = {"labels": {"app": name, "floci-qualification": q.record["owner"]}}
        if native:
            metadata["annotations"] = {"microsandbox.dev/availability": "proxy", "microsandbox.dev/idle-owner": "external"}
        q.apply({"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": name}, "spec": {
            "replicas": 1, "selector": {"matchLabels": {"app": name}}, "template": {"metadata": metadata, "spec": pod["spec"]}}})
        q.apply({"apiVersion": "v1", "kind": "Service", "metadata": {"name": name}, "spec": {
            "selector": {"app": name}, "ports": [{"name": "http", "port": 80, "targetPort": 8080}]}})
        self.created.append(name)
        manage.wait(name+" deployment available", lambda: q.resource("deployment", name).get("status", {}).get("availableReplicas") == 1, timeout=360)
        return name

    def fetch(self, name, path):
        if "native" in name:
            state = manage.wait(name+" discovered", lambda: self.q.state(name))
            with urllib.request.urlopen(state["proxyUrl"]+path, timeout=180) as response:
                return json.load(response)
        pod = json.loads(self.q.kubectl("get", "pods", "-l", "app="+name, "-o", "json"))["items"][0]["metadata"]["name"]
        return json.loads(self.q.kubectl("exec", pod, "--", "wget", "-T", "180", "-qO-", "http://127.0.0.1:8080"+path, timeout=190))

    def wait_row(self, name, row, source):
        def found():
            value = self.fetch(name, "/read")
            rows = {item["id"]: item for item in value["writes"]}
            return rows[row] if row in rows and rows[row]["source"] == source else None
        return manage.wait("SQL application row "+row, found, timeout=180)

    def remove(self, names):
        for name in names:
            self.q.kubectl("delete", "deployment,service", name, "--ignore-not-found", "--wait=true", timeout=180)
            manage.wait(name+" Pods deleted", lambda: not json.loads(self.q.kubectl("get", "pods", "-l", "app="+name, "-o", "json"))["items"], timeout=180)
            self.created.remove(name)

    def sleep_all(self):
        for name in ("graph-native-http", "graph-native-worker"):
            state = self.q.state(name)
            if state and state["state"] in ("RUNNING", "READY"):
                self.q.sleep(name)
        self.command("pauseAurora")
        manage.wait("all graph native memory released", lambda: self.helper("/v1/capacity")["memoryMiB"] == 0, timeout=60)

    def prove_capacity_refusal(self):
        before = self.helper("/v1/capacity")
        inventory = self.q.command(self.q.args.msb, "ls", "--format", "json", env=self.q.environment)
        try:
            accepted = self.helper("/v1/capacity/prepare", {"arguments": ["run", "-d", "--name",
                "graph-over-budget-"+self.q.record["owner"], "--memory", "768M", "postgres:17.11-alpine"]})
        except urllib.error.HTTPError as error:
            failure = json.load(error)
            assert error.code == 409 and failure.get("operationStarted") is False, failure
        else:
            self.helper("/v1/capacity/cancel", {"token": accepted["token"]})
            raise AssertionError("shared ledger admitted a third guest above the Pod/Aurora RAM limit")
        after = self.helper("/v1/capacity")
        assert after["memoryMiB"] == before["memoryMiB"] == 1536
        assert after["reservations"] == before["reservations"]
        assert self.q.command(self.q.args.msb, "ls", "--format", "json", env=self.q.environment) == inventory
        self.q.report["checks"]["shared_pod_aurora_admission_refuses_third_guest_without_dispatch"] = True

    def run_contract(self):
        q = self.q
        http = self.deploy("docker", False)
        worker = self.deploy("docker", True)
        response = self.fetch(http, "/write?id=docker-http-1")
        assert any(row["id"] == "docker-http-1" and row["source"] == "http" for row in response["writes"])
        self.command("enqueue", body="docker-sqs-1")
        self.wait_row(http, "docker-sqs-1", "sqs")
        self.remove([http, worker])
        self.command("pauseAurora")
        q.report["checks"]["docker_http_and_sdk_sqs_worker_write_real_aurora"] = True
        http = self.deploy("native", False)
        worker = self.deploy("native", True)
        assert q.state(http)["state"] == "COLD" and q.state(worker)["state"] == "COLD"
        assert self.helper("/v1/capacity")["memoryMiB"] == 0
        q.report["checks"]["native_graph_deploys_proxy_ready_without_booting_apps_or_aurora"] = True
        markers = {}
        for attempt in (1, 2):
            value = self.fetch(http, "/write?id=native-http-"+str(attempt))
            if attempt == 1:
                markers["http"] = value["marker"]
            assert value["marker"] == markers["http"]
            status = self.helper("/v1/capacity")
            assert status["limits"]["memoryMiB"] == 1536 and status["memoryMiB"] == 1536, status
            self.prove_capacity_refusal()
            assert q.state(worker)["state"] in ("COLD", "SLEEPING")
            q.sleep(http)
            self.command("pauseAurora")
            assert self.helper("/v1/capacity")["memoryMiB"] == 0
            self.command("enqueue", body="native-sqs-"+str(attempt))
            manage.wait("SQS wakes native worker", lambda: q.state(worker)["state"] in ("RUNNING", "READY"), timeout=240)
            row = self.wait_row(worker, "native-sqs-"+str(attempt), "sqs")
            if attempt == 1:
                markers["worker"] = row["marker"]
            assert row["marker"] == markers["worker"]
            assert q.state(http)["state"] == "SLEEPING"
            manage.wait("real coordinator sleeps idle SQS worker", lambda: q.state(worker)["state"] == "SLEEPING", timeout=90)
            manage.wait("coordinator records sleeping worker", lambda: self.command("status")["workerState"] == "SLEEPING", timeout=30)
            self.sleep_all()
            if attempt == 1:
                q.crash_and_restart()
                assert self.helper("/v1/capacity")["memoryMiB"] == 0
        q.report["checks"]["native_http_restores_aurora_and_same_application_process"] = True
        q.report["checks"]["sqs_enqueue_wakes_worker_to_persist_sql_then_idle"] = True
        q.report["checks"]["pod_and_aurora_ram_share_1536_mib_budget_and_release_when_asleep"] = True
        q.report["checks"]["graph_recovery_retains_app_and_database_process_markers"] = True
        q.report["processMarkers"] = markers

    def run(self):
        result = {"success": False}
        try:
            self.prepare()
            self.run_contract()
            self.remove(list(self.created))
            result["success"] = True
        except Exception as error:
            result["error"] = str(error)
            raise
        finally:
            try:
                if self.java is not None:
                    if self.java.poll() is None:
                        atomic_json(self.directory/"result.json", result)
                        try:
                            self.java.wait(timeout=180)
                        except subprocess.TimeoutExpired:
                            terminate(self.java)
                    code = self.java.returncode
                    if result["success"] and code != 0:
                        raise RuntimeError("graph JVM final SQL assertions or owned database cleanup failed")
            finally:
                terminate(self.java)
                terminate(self.host)
                if self.binary_server is not None:
                    self.binary_server.shutdown()
                    self.binary_server.server_close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binaries", "msb", "msb-home", "report", "floci-checkout"):
        parser.add_argument("--"+name, required=True)
    parser.add_argument("--node-ip")
    parser.add_argument("--work-directory", default="/tmp")
    parser.add_argument("--java-home")
    args = parser.parse_args()
    args.memory_mib = 1536
    os.environ["FLOCI_MICROSANDBOX_MEMORY_LIMIT_MIB"] = "1536"
    return ManagedQualification(args).run(lambda q: ApplicationGraph(q).run())


if __name__ == "__main__":
    raise SystemExit(main())
