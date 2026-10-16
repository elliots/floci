#!/usr/bin/env python3
"""Run isolated real Kubernetes-to-native-pod qualification and remove owned resources."""
import argparse
import base64
import json
import ipaddress
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.request
import urllib.error

from qualification_registry import QualificationRegistry
from qualification_applications import qualify_applications
from qualification_operations import qualify_operations

LABEL = "io.floci.pod-probe"


class QualificationFailure(RuntimeError):
    pass


class QualificationComplete(Exception):
    """A selected qualification subset completed; still run shared cleanup."""


def command(*args, timeout=60, input_text=None, env=None, cwd=None):
    result = subprocess.run(args, text=True, input=input_text, capture_output=True, timeout=timeout, env=env, cwd=cwd)
    if result.returncode:
        raise RuntimeError(f"{Path(args[0]).name} {args[1]} failed: {result.stderr[-3000:]}")
    return result.stdout.strip()


def wait(description, operation, timeout=180):
    deadline = time.monotonic() + timeout
    error = "not ready"
    while time.monotonic() < deadline:
        try:
            value = operation()
            if value:
                return value
        except QualificationFailure:
            raise
        except Exception as failure:
            error = str(failure)
        time.sleep(1)
    raise RuntimeError(f"{description}: {error}")


def source_ip(value):
    address = ipaddress.ip_address(value.strip("[]"))
    return str(getattr(address, "ipv4_mapped", None) or address)


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def qualify(args):
    owner = secrets.token_hex(8)
    peer = f"floci-pod-probe-{owner}"
    node = f"native-pods-{owner}"
    environment = dict(os.environ, MSB_HOME=args.msb_home)
    report = {"owner": owner, "peer": peer, "node": node, "checks": {}}
    process = None
    registry = None
    server_name = "native-server"
    log_path = Path(args.report).with_suffix(".controller.log")
    log = log_path.open("w")
    created = False
    def docker(*values, **kw):
        return command("docker", *values, **kw)
    def kubectl(*values, **kw):
        if values and values[0] in ("exec", "logs"):
            values = tuple(server_name if value == "native-server" else value for value in values)
        return docker("exec", "-i", peer, "kubectl", "--request-timeout=20s", *values, **kw)
    def resource(kind, name):
        if kind == "pod" and name == "native-server":
            name = server_name
        return json.loads(kubectl("get", kind, name, "-o", "json"))
    def apply(value):
        return kubectl("apply", "-f", "-", input_text=json.dumps(value))
    def ready(name):
        pod = resource("pod", name)
        if pod.get("status", {}).get("phase") == "Failed":
            raise QualificationFailure(json.dumps(pod["status"]))
        return pod if any(c.get("type") == "Ready" and c.get("status") == "True"
                          for c in pod.get("status", {}).get("conditions", [])) else None
    try:
        directory = tempfile.mkdtemp(prefix="floci-vk-")
        if True:
            temporary = Path(directory)
            docker("run", "-d", "--name", peer, "--hostname", peer, "--label", f"{LABEL}={owner}",
                   "--privileged", "-p", "127.0.0.1::6443", "-p", "127.0.0.1::8472/udp",
                   "rancher/k3s:v1.35.0-k3s1", "server", "--disable=traefik", "--disable=servicelb",
                   "--disable=metrics-server", "--egress-selector-mode=disabled", timeout=600)
            created = True
            wait("control plane ready", lambda: kubectl("get", "--raw=/readyz") == "ok")
            wait("default service account", lambda: resource("serviceaccount", "default"))
            wait("Docker worker ready", lambda: any(c["type"] == "Ready" and c["status"] == "True"
                 for c in resource("node", peer)["status"]["conditions"]))
            def cluster_dns_ready():
                values=json.loads(kubectl("get","pods","-n","kube-system","-l","k8s-app=kube-dns","-o","json"))["items"]
                return values and all(any(c["type"]=="Ready" and c["status"]=="True" for c in value.get("status",{}).get("conditions",[])) for value in values)
            wait("CoreDNS ready",cluster_dns_ready,300)
            report["checks"]["cluster_dns_ready"]=True
            inspect = json.loads(docker("inspect", peer))[0]
            api_port = inspect["NetworkSettings"]["Ports"]["6443/tcp"][0]["HostPort"]
            kubeconfig = docker("exec", peer, "cat", "/etc/rancher/k3s/k3s.yaml")
            config_path = temporary / "kubeconfig"
            config_path.write_text(kubeconfig.replace("https://127.0.0.1:6443", f"https://127.0.0.1:{api_port}"))
            config_path.chmod(0o600)
            host_ip = docker("exec", peer, "sh", "-c", "nslookup host.docker.internal | awk '/^Address: / {print $2}' | tail -n1")
            for filename in ("server-ca.crt", "server-ca.key", "client-ca.crt"):
                path = temporary / filename
                path.write_text(docker("exec", peer, "cat", "/var/lib/rancher/k3s/server/tls/" + filename) + "\n")
                path.chmod(0o600)
            extension = temporary / "ext.cnf"
            extension.write_text(f"subjectAltName=IP:{host_ip},IP:127.0.0.1\nextendedKeyUsage=serverAuth\n")
            command("openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout", str(temporary/"server.key"),
                    "-out", str(temporary/"server.csr"), "-subj", f"/CN={node}")
            command("openssl", "x509", "-req", "-in", str(temporary/"server.csr"), "-CA", str(temporary/"server-ca.crt"),
                    "-CAkey", str(temporary/"server-ca.key"), "-CAcreateserial", "-out", str(temporary/"server.crt"),
                    "-days", "1", "-extfile", str(extension))
            port = free_port()
            control_port = free_port()
            token = secrets.token_hex(32)
            token_path = temporary / "control-token"
            token_path.write_text(token)
            token_path.chmod(0o600)
            controller_args = [args.controller, "--kubeconfig", str(config_path), "--node", node,
                "--msb", args.msb, "--msb-home", args.msb_home, "--state", str(temporary/"state"),
                "--network-peer", peer, "--peer-owner", owner, "--listen", f"0.0.0.0:{port}", "--node-ip", host_ip,
                "--tls-cert", str(temporary/"server.crt"), "--tls-key", str(temporary/"server.key"),
                "--client-ca", str(temporary/"client-ca.crt"), "--control-listen", f"127.0.0.1:{control_port}", "--control-token-file", str(token_path), "--cleanup-on-exit", "--cluster-name", "qualification"]
            if args.network_relay_binary:
                controller_args.extend(["--network-relay-binary",args.network_relay_binary])
            if args.scheduling_cpu:
                controller_args.extend(["--scheduling-cpu", args.scheduling_cpu])
            if args.scheduling_memory:
                controller_args.extend(["--scheduling-memory", args.scheduling_memory])
            process = subprocess.Popen(controller_args, stdout=log, stderr=log)
            def control_get(path):
                request = urllib.request.Request(f"http://127.0.0.1:{control_port}/v1/"+path,headers={"Authorization":"Bearer "+token})
                with urllib.request.urlopen(request,timeout=30) as response: return json.load(response)
            def crash_and_restart():
                nonlocal process
                began = time.monotonic()
                process.kill()
                process.wait(timeout=30)
                process = subprocess.Popen(controller_args,stdout=log,stderr=log)
                wait("controller journal recovery",lambda:control_get("health"),900)
                report.setdefault("controller_recovery_seconds", []).append(round(time.monotonic()-began,3))
            def lifecycle(uid, operation=None, generation=None):
                path = f"http://127.0.0.1:{control_port}/v1/pods/{uid}"
                data = None
                if operation:
                    path += "/" + operation
                    data = json.dumps({"expectedGeneration":generation}).encode()
                request = urllib.request.Request(path, data=data, headers={"Authorization":"Bearer "+token,"Content-Type":"application/json"})
                try:
                    with urllib.request.urlopen(request,timeout=240) as response:
                        return json.load(response)
                except urllib.error.HTTPError as error:
                    raise RuntimeError(f"lifecycle {operation}: {error.code} {error.read().decode()}") from None
            wait("virtual node Ready", lambda: any(c["type"] == "Ready" and c["status"] == "True"
                 for c in resource("node", node)["status"]["conditions"]))
            lease = json.loads(kubectl("get", "lease", node, "-n", "kube-node-lease", "-o", "json"))
            assert lease["spec"]["holderIdentity"] == node
            report["checks"]["real_node_lease"] = True
            def pod(name, script, native=True):
                return {"apiVersion":"v1", "kind":"Pod", "metadata":{"name":name,"labels":{"app":name}}, "spec":{
                    "nodeSelector":{"microsandbox.dev/runtime":"native"} if native else {"kubernetes.io/hostname":peer},
                    "tolerations":[{"key":"microsandbox.dev/runtime","operator":"Equal","value":"native","effect":"NoSchedule"}],
                    "automountServiceAccountToken":False, "restartPolicy":"Never",
                    "containers":[{"name":"app","image":"public.ecr.aws/docker/library/busybox:1.37.0","command":["sh","-ec",script]}]}}
            if not getattr(args, "operations_only", False):
                qualify_applications(pod=pod, apply=apply, resource=resource, kubectl=kubectl,
                    ready=ready, lifecycle=lifecycle, control_get=control_get,
                    crash_and_restart=crash_and_restart, wait=wait, command=command,
                    msb=args.msb, environment=environment, report=report)
            if getattr(args, "applications_only", False):
                raise QualificationComplete()
            qualify_operations(peer=peer, temporary=temporary, pod=pod, apply=apply,
                resource=resource, kubectl=kubectl, ready=ready, lifecycle=lifecycle,
                control_get=control_get, wait=wait, command=command, report=report)
            if getattr(args, "operations_only", False):
                raise QualificationComplete()
            if args.cold_pods:
                if args.cold_pods < 1 or args.cold_pods > 500: raise ValueError("cold-pods must be 0..500")
                cold_pods = [pod(f"cold-{number}","exec sleep 600") for number in range(args.cold_pods)]
                for value in cold_pods:
                    value["metadata"]["annotations"]={"microsandbox.dev/on-demand":"true"}
                    value["metadata"]["labels"]["qualification"]="cold"
                    value["spec"]["containers"][0]["ports"]=[{"containerPort":8080}]
                apply({"apiVersion":"v1","kind":"List","items":cold_pods})
                def all_cold():
                    values=json.loads(kubectl("get","pods","-l","qualification=cold","-o","json"))["items"]
                    return values if len(values)==args.cold_pods and all(p.get("status",{}).get("reason")=="NativeVMCold" for p in values) else None
                values=wait("all declared pods remain cold",all_cold,600)
                assert control_get("health")["activeMemoryMiB"]==0
                names={"fvp-"+p["metadata"]["uid"].replace("-","")[:20] for p in values}
                assert not any(v["name"] in names for v in json.loads(command(args.msb,"ls","--format","json",env=environment)))
                assert all(not any(c["type"]=="Ready" and c["status"]=="True" for c in p["status"].get("conditions",[])) for p in values)
                report["cold_declaration"]={"pods":args.cold_pods,"active_memory_mib":0,"native_vms":0,"controller_rss_bytes":int(command("ps","-o","rss=","-p",str(process.pid)))*1024}
                chosen=values[0]
                uid=chosen["metadata"]["uid"]
                state=lifecycle(uid)
                began=time.monotonic()
                lifecycle(uid,"resume",state["generation"])
                wait("one cold pod really starts",lambda:ready(chosen["metadata"]["name"]),300)
                assert control_get("health")["activeMemoryMiB"]==768
                remaining=json.loads(kubectl("get","pods","-l","qualification=cold","-o","json"))["items"]
                assert sum(p.get("status",{}).get("reason")=="NativeVMCold" for p in remaining)==args.cold_pods-1
                report["cold_declaration"]["first_wake_seconds"]=round(time.monotonic()-began,3)
                report["checks"]["cold_declarations_allocate_only_on_first_wake"]=True
                kubectl("delete","pod","-l","qualification=cold","--wait=true",timeout=600)
                wait("cold fixture releases RAM",lambda:control_get("health")["activeMemoryMiB"]==0)
            server_script = 'mkdir -p /www/cgi-bin; cat >/www/cgi-bin/source <<\'CGI\'\n#!/bin/sh\nprintf \'Content-Type: text/plain\\r\\n\\r\\n\'\nprintf \'%s\\n\' "$REMOTE_ADDR"\nCGI\nchmod +x /www/cgi-bin/source; echo native-pod-executed; exec httpd -f -p 8080 -h /www'
            image_default = pod("image-default", "unused")
            image_default["spec"]["containers"][0] = {
                "name":"app", "image":"public.ecr.aws/docker/library/nginx:1.28.0-alpine",
                "ports":[{"containerPort":80}],
                "readinessProbe":{"httpGet":{"path":"/", "port":80}, "periodSeconds":1}}
            apply(image_default)
            wait("image ENTRYPOINT and CMD start without Pod overrides",lambda:ready("image-default"),300)
            assert "Welcome to nginx!" in kubectl("exec","image-default","--","wget","-qO-","http://127.0.0.1/")
            assert kubectl("exec","image-default","--","printenv","NGINX_VERSION")=="1.28.0"
            report["checks"]["image_entrypoint_cmd_and_environment_defaults"] = True
            kubectl("delete","pod","image-default","--wait=true",timeout=90)
            wait("image-default fixture releases RAM",lambda:control_get("health")["activeMemoryMiB"]==0)
            sized = pod("resource-profile", "exec sleep 600")
            sized["spec"]["containers"][0]["resources"] = {
                "requests":{"cpu":"1500m","memory":"769Mi"}, "limits":{"cpu":"2","memory":"1025Mi"}}
            apply(sized)
            sized_pod = wait("larger resource profile ready",lambda:ready("resource-profile"),300)
            sized_uid = sized_pod["metadata"]["uid"]
            sized_state = lifecycle(sized_uid)
            assert sized_state["vcpus"] == 2 and sized_state["memoryMiB"] == 1282
            assert control_get("health")["activeMemoryMiB"] == 1282
            limits = kubectl("exec","resource-profile","--","sh","-ec",
                'group=$(awk -F: \'$1 == "0" {print $3}\' /proc/self/cgroup); '
                'test "$group" = /; root=/sys/fs/cgroup; '
                'cat "$root/cpu.max" "$root/memory.max"; '
                'if output=$(echo max >"$root/memory.max" 2>&1); then exit 1; fi; '
                'test "$(awk \'/^processor/ {n++} END {print n}\' /proc/cpuinfo)" = 2; '
                'test "$(awk \'/MemTotal/ {print $2}\' /proc/meminfo)" -gt 1049600')
            assert limits.splitlines() == ["200000 100000", str(1025*1024*1024)], limits
            lifecycle(sized_uid,"hibernate",sized_state["generation"])
            assert control_get("health")["activeMemoryMiB"] == 0
            crash_and_restart()
            sized_state = lifecycle(sized_uid)
            assert sized_state["memoryMiB"] == 1282 and sized_state["state"] == "SLEEPING"
            lifecycle(sized_uid,"resume",sized_state["generation"])
            wait("larger guest resumes",lambda:ready("resource-profile"),120)
            assert control_get("health")["activeMemoryMiB"] == 1282
            report["checks"]["variable_guest_cpu_memory_limits_and_recovered_admission"] = True
            kubectl("delete","pod","resource-profile","--wait=true",timeout=90)
            wait("resource profile releases RAM",lambda:control_get("health")["activeMemoryMiB"]==0)
            for name, script, expected_oom in [
                ("limit-oom", "sleep 2; exec awk 'BEGIN { value=\"x\"; while (1) value=value value }'", True),
                ("ordinary-sigkill", "exec sleep 600", False),
            ]:
                value = pod(name,script)
                value["spec"]["containers"][0]["resources"] = {"limits":{"memory":"32Mi", "cpu":"500m"}}
                apply(value)
                if not expected_oom:
                    running = wait("SIGKILL fixture ready",lambda:ready(name),300)
                    vm = lifecycle(running["metadata"]["uid"])["vm"]
                    command(args.msb,"exec","--no-stdin",vm,"--","ctr","--address","/run/containerd/containerd.sock",
                            "tasks","kill","--signal","SIGKILL","application",env=environment)
                def terminal_status():
                    current = resource("pod",name)
                    statuses = current.get("status",{}).get("containerStatuses",[])
                    return statuses[0]["state"].get("terminated") if statuses else None
                status = wait(name+" terminated",terminal_status,300)
                assert status["exitCode"] == 137, status
                assert (status.get("reason")=="OOMKilled") == expected_oom, status
                uid = resource("pod",name)["metadata"]["uid"]
                crash_and_restart()
                recorded = json.loads((temporary/"state"/(uid+".json")).read_text())
                assert recorded["pod"]["status"]["containerStatuses"][0]["state"]["terminated"] == status
                assert lifecycle(uid)["state"] == "TERMINATED"
                kubectl("delete","pod",name,"--wait=true",timeout=90)
                wait(name+" releases RAM",lambda:control_get("health")["activeMemoryMiB"]==0)
            report["checks"]["kernel_oom_attribution_distinguishes_sigkill"] = True
            apply(pod("lost-exit-status", "exec sleep 600"))
            lost_exit = wait("exit-publication fixture ready",lambda:ready("lost-exit-status"),300)
            lost_uid = lost_exit["metadata"]["uid"]
            lost_vm = lifecycle(lost_uid)["vm"]
            command(args.msb,"exec","--no-stdin",lost_vm,"--","sh","-ec",
                    "mkdir /tmp/application.exit.pending; ctr --address /run/containerd/containerd.sock tasks kill --signal SIGKILL application",
                    env=environment)
            wait("missing exit status withdraws readiness",lambda:not any(
                condition.get("type")=="Ready" and condition.get("status")=="True"
                for condition in resource("pod","lost-exit-status").get("status",{}).get("conditions",[])),30)
            def missing_task_failed():
                current = resource("pod","lost-exit-status")
                return current if current.get("status",{}).get("phase")=="Failed" else None
            lost_exit = wait("missing exit status fails within bounded grace",missing_task_failed,60)
            assert lost_exit["status"]["reason"] == "NativeRuntimeFailed", lost_exit["status"]
            assert lifecycle(lost_uid)["state"] == "FAILED"
            crash_and_restart()
            lost_exit = wait("failed missing-task status survives recovery",missing_task_failed,60)
            assert lost_exit["status"]["reason"] == "NativeRuntimeFailed", lost_exit["status"]
            assert lifecycle(lost_uid)["state"] == "FAILED"
            kubectl("delete","pod","lost-exit-status","--wait=true",timeout=90)
            wait("missing-task fixture releases RAM",lambda:control_get("health")["activeMemoryMiB"]==0)
            report["checks"]["failed_exit_publication_cannot_leave_pod_ready"] = True
            volume_binary = b"\x00\xffnative-volume\x00\r\n"
            def volume_sources(version, old_key=False):
                data = {"setting":version,"paired":version}
                if old_key:
                    data["removed"] = "remove-on-update"
                apply({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"native-volume-settings"},
                       "data":data,"binaryData":{"binary":base64.b64encode(volume_binary).decode()}})
                apply({"apiVersion":"v1","kind":"Secret","metadata":{"name":"native-volume-secrets"},
                       "data":{"token":base64.b64encode(("private-"+version).encode()).decode(),
                               "binary":base64.b64encode(volume_binary).decode()}})
            volume_sources("initial", old_key=True)
            apply({"apiVersion":"v1","kind":"Secret","metadata":{"name":"native-volume-optional"},
                   "stringData":{"unselected":"the optional requested key is absent"}})
            def with_volumes(value):
                value["spec"]["volumes"] = [
                    {"name":"disk-cache","emptyDir":{}},
                    {"name":"memory-cache","emptyDir":{"medium":"Memory","sizeLimit":"16Mi"}},
                    {"name":"settings","configMap":{"name":"native-volume-settings","defaultMode":0o440,
                        "items":[{"key":"setting","path":"nested/value"},{"key":"binary","path":"binary","mode":0o400}]}},
                    {"name":"settings-all","configMap":{"name":"native-volume-settings"}},
                    {"name":"secrets","secret":{"secretName":"native-volume-secrets","defaultMode":0o400,
                        "items":[{"key":"token","path":"nested/token"},{"key":"binary","path":"binary"}]}},
                    {"name":"optional-settings","configMap":{"name":"native-volume-optional","optional":True}},
                    {"name":"optional-secrets","secret":{"secretName":"native-volume-optional","optional":True,
                        "items":[{"key":"absent","path":"absent"}]}},
                ]
                value["spec"]["containers"][0]["volumeMounts"] = [
                    {"name":name,"mountPath":path} for name,path in (
                        ("disk-cache","/work/disk"),("memory-cache","/work/memory"),
                        ("settings","/mounted/settings"),("settings-all","/mounted/all"),
                        ("secrets","/mounted/secrets"),("optional-settings","/mounted/optional-settings"),
                        ("optional-secrets","/mounted/optional-secrets"))]
                return value
            initialized = pod("native-init", "test \"$(cat /work/order)\" = first-second; exec sleep 900")
            initialized["spec"]["restartPolicy"] = "Always"
            initialized["spec"]["volumes"] = [
                {"name":"work","emptyDir":{}},
                {"name":"settings","configMap":{"name":"native-volume-settings",
                    "items":[{"key":"setting","path":"nested/value"}]}},
                {"name":"credentials","secret":{"secretName":"native-volume-secrets"}},
            ]
            initialized["spec"]["containers"][0]["env"] = [{"name":"DIRECTORY","value":"nested"}]
            initialized["spec"]["containers"][0]["volumeMounts"] = [
                {"name":"work","mountPath":"/work"},
                {"name":"work","mountPath":"/single-file","subPath":"nested/value"},
                {"name":"work","mountPath":"/expanded","subPathExpr":"$(DIRECTORY)"},
                {"name":"work","mountPath":"/created","subPathExpr":"fresh/$(DIRECTORY)"},
                {"name":"settings","mountPath":"/pinned-config","subPath":"nested/value"},
                {"name":"settings","mountPath":"/pinned-directory","subPath":"nested"},
                {"name":"credentials","mountPath":"/pinned-secret","subPath":"token"},
                {"name":"settings","mountPath":"/settings"},
            ]
            init_image = initialized["spec"]["containers"][0]["image"]
            initialized["spec"]["initContainers"] = [
                {"name":"prepare","image":init_image,
                 "command":["sh","-ec",'mkdir -p /work/nested; chmod 777 /work/nested; printf first >/work/order; echo "$SETTING" >/work/nested/value; echo prepare-completed'],
                 "env":[{"name":"SETTING","valueFrom":{"configMapKeyRef":{"name":"native-volume-settings","key":"setting"}}}],
                 "resources":{"requests":{"cpu":"1500m","memory":"769Mi"},"limits":{"cpu":"2","memory":"1025Mi"}},
                 "volumeMounts":[{"name":"work","mountPath":"/work"}]},
                {"name":"finish","image":init_image,
                 "command":["sh","-ec",'test "$(cat /work/order)" = first; test "$(cat /input)" = initial; printf -- -second >>/work/order; echo finish-completed'],
                 "volumeMounts":[{"name":"work","mountPath":"/work"},
                                 {"name":"work","mountPath":"/input","subPath":"nested/value","readOnly":True}]},
            ]
            apply(initialized)
            init_pod = wait("sequential init containers finish before application",lambda:ready("native-init"),360)
            init_uid = init_pod["metadata"]["uid"]
            init_statuses = init_pod["status"]["initContainerStatuses"]
            assert [value["name"] for value in init_statuses] == ["prepare","finish"], init_statuses
            assert all(value["state"]["terminated"]["exitCode"] == 0 for value in init_statuses), init_statuses
            assert any(value["type"] == "Initialized" and value["status"] == "True"
                       for value in init_pod["status"]["conditions"])
            assert "prepare-completed" in kubectl("logs","native-init","-c","prepare")
            assert "finish-completed" in kubectl("logs","native-init","-c","finish")
            init_state = lifecycle(init_uid)
            assert init_state["vcpus"] == 2 and init_state["memoryMiB"] == 1282, init_state
            assert control_get("health")["activeMemoryMiB"] == 1282
            report["checks"]["sequential_init_status_logs_environment_and_resource_maximum"] = True
            assert kubectl("exec","native-init","--","sh","-ec",
                'test "$(cat /single-file)" = initial; test "$(cat /expanded/value)" = initial; '
                'echo shared >/expanded/value; cat /single-file') == "shared"
            kubectl("exec","native-init","--","sh","-ec",
                'test -d /created; echo created >/created/value; test "$(cat /work/fresh/nested/value)" = created')
            assert kubectl("exec","native-init","--","sh","-ec",
                "if echo forbidden >/pinned-config 2>/dev/null; then exit 1; fi; echo read-only") == "read-only"
            volume_sources("subpath-updated")
            wait("whole projection updates beside pinned subPath",lambda:
                kubectl("exec","native-init","--","cat","/settings/nested/value") == "subpath-updated",120)
            assert kubectl("exec","native-init","--","cat","/pinned-config") == "initial"
            assert kubectl("exec","native-init","--","cat","/pinned-directory/value") == "initial"
            assert kubectl("exec","native-init","--","cat","/pinned-secret") == "private-initial"
            report["checks"]["subpath_file_directory_expression_and_pinned_projection"] = True
            lifecycle(init_uid,"hibernate",init_state["generation"])
            crash_and_restart()
            init_state = lifecycle(init_uid)
            assert init_state["state"] == "SLEEPING"
            lifecycle(init_uid,"resume",init_state["generation"])
            wait("initialized pod resumes after controller recovery",lambda:ready("native-init"),180)
            assert kubectl("exec","native-init","--","cat","/work/order") == "first-second"
            assert kubectl("exec","native-init","--","cat","/single-file") == "shared"
            assert kubectl("exec","native-init","--","cat","/pinned-config") == "initial"
            assert kubectl("exec","native-init","--","cat","/pinned-directory/value") == "initial"
            previous_restarts = resource("pod","native-init")["status"]["containerStatuses"][0]["restartCount"]
            command(args.msb,"exec","--no-stdin",lifecycle(init_uid)["vm"],"--","sh","-ec",
                "ctr --address /run/containerd/containerd.sock tasks kill --signal SIGKILL application",env=environment)
            wait("application restarts after init completion",lambda:
                resource("pod","native-init")["status"]["containerStatuses"][0]["restartCount"] > previous_restarts,120)
            wait("restarted application ready",lambda:ready("native-init"),180)
            assert kubectl("exec","native-init","--","cat","/work/order") == "first-second"
            assert kubectl("exec","native-init","--","cat","/single-file") == "shared"
            assert kubectl("exec","native-init","--","cat","/pinned-config") == "subpath-updated"
            assert kubectl("exec","native-init","--","cat","/pinned-directory/value") == "subpath-updated"
            assert kubectl("exec","native-init","--","cat","/pinned-secret") == "private-subpath-updated"
            assert all(value["restartCount"] == 0 for value in resource("pod","native-init")["status"]["initContainerStatuses"])
            report["checks"]["init_completion_and_subpath_survive_resume_recovery_and_app_restart"] = True
            kubectl("delete","pod","native-init","--wait=true",timeout=120)
            wait("init fixture releases RAM",lambda:control_get("health")["activeMemoryMiB"]==0)
            for policy in ("Never", "OnFailure"):
                name = "init-failure-" + policy.lower()
                retry = pod(name, "exec sleep 900")
                retry["spec"]["restartPolicy"] = policy
                retry["spec"]["volumes"] = [{"name":"work","emptyDir":{}}]
                retry["spec"]["initContainers"] = [{"name":"retry","image":init_image,
                    "command":["sh","-ec",'if test ! -f /work/attempt; then touch /work/attempt; exit 7; fi; echo retry-succeeded'],
                    "volumeMounts":[{"name":"work","mountPath":"/work"}]}]
                apply(retry)
                if policy == "Never":
                    def failed_init():
                        value = resource("pod",name)
                        return value if value.get("status",{}).get("phase") == "Failed" else None
                    failed = wait("Never init failure terminates pod",failed_init,240)
                    assert failed["status"]["initContainerStatuses"][0]["state"]["terminated"]["exitCode"] == 7
                    assert not any(value["state"].get("running") for value in failed["status"]["containerStatuses"])
                    report["checks"]["init_never_failure_prevents_application"] = True
                else:
                    retried = wait("OnFailure retries init and starts app",lambda:ready(name),300)
                    status = retried["status"]["initContainerStatuses"][0]
                    assert status["restartCount"] == 1 and status["state"]["terminated"]["exitCode"] == 0, status
                    assert status["lastState"]["terminated"]["exitCode"] == 7, status
                    report["checks"]["init_onfailure_retries_before_application"] = True
                kubectl("delete","pod",name,"--wait=true",timeout=120)
                wait("failed init fixture releases RAM",lambda:control_get("health")["activeMemoryMiB"]==0)
            escaped = pod("subpath-escape", "exec sleep 900")
            escaped["spec"]["volumes"] = [{"name":"work","emptyDir":{}}]
            escaped["spec"]["containers"][0]["volumeMounts"] = [
                {"name":"work","mountPath":"/escaped","subPath":"escape/passwd"}]
            escaped["spec"]["initContainers"] = [{"name":"prepare","image":init_image,
                "command":["sh","-ec","ln -s /etc /work/escape"],
                "volumeMounts":[{"name":"work","mountPath":"/work"}]}]
            apply(escaped)
            def rejected_escape():
                value = resource("pod","subpath-escape")
                return value if value.get("status",{}).get("phase") == "Failed" else None
            rejected = wait("escaping subPath symlink rejects app launch",rejected_escape,240)
            assert "subPath" in rejected["status"]["message"], rejected["status"]
            assert not any(value["state"].get("running") for value in rejected["status"]["containerStatuses"])
            report["checks"]["subpath_symlink_escape_rejected_before_application"] = True
            kubectl("delete","pod","subpath-escape","--wait=true",timeout=120)
            wait("subPath escape fixture releases RAM",lambda:control_get("health")["activeMemoryMiB"]==0)
            volume_sources("initial", old_key=True)
            registry = QualificationRegistry(command, temporary, owner)
            registry.start()
            apply(registry.secret())
            private = pod("native-private-image", "echo private-registry-executed; exec sleep 900")
            private["spec"]["restartPolicy"] = "Always"
            private["spec"]["imagePullSecrets"] = [{"name":"native-registry"}]
            private["spec"]["containers"][0]["image"] = registry.image
            private["spec"]["containers"][0]["imagePullPolicy"] = "Always"
            private["spec"]["volumes"] = [{"name":"registry-gate","emptyDir":{}}]
            private["spec"]["initContainers"] = [{"name":"registry-gate","image":init_image,
                "command":["sh","-ec","echo once >>/gate/runs; while test ! -f /gate/release; do sleep 1; done"],
                "volumeMounts":[{"name":"registry-gate","mountPath":"/gate"}]}]
            apply(private)
            private_uid = resource("pod","native-private-image")["metadata"]["uid"]
            def registry_gate_ready():
                value = resource("pod","native-private-image")
                statuses = value.get("status",{}).get("initContainerStatuses",[])
                return statuses and statuses[0].get("state",{}).get("running")
            wait("registry fixture waits for its private test CA",registry_gate_ready,300)
            init_started = resource("pod","native-private-image")["status"]["initContainerStatuses"][0]["state"]["running"]["startedAt"]
            private_state = lifecycle(private_uid)
            def running_init_identity():
                return command(args.msb,"exec","--no-stdin",private_state["vm"],"--","sh","-ec",
                    "cat /proc/sys/kernel/random/boot_id; "
                    "ctr --address /run/containerd/containerd.sock tasks list | "
                    "awk '$1 == \"init-0\" && $3 == \"RUNNING\" {print $1, $2, $3}'",env=environment)
            init_identity = running_init_identity()
            assert len(init_identity.splitlines()) == 2, init_identity
            crash_and_restart()
            wait("running init survives controller recovery",registry_gate_ready,120)
            recovered_init = resource("pod","native-private-image")["status"]["initContainerStatuses"][0]
            assert recovered_init["restartCount"] == 0
            assert recovered_init["state"]["running"]["startedAt"] == init_started
            private_state = lifecycle(private_uid)
            assert private_state["state"] == "INITIALIZING", private_state
            assert running_init_identity() == init_identity
            assert command(args.msb,"exec","--no-stdin",private_state["vm"],"--","cat",
                "/var/lib/floci-volumes/registry-gate/runs",env=environment) == "once"
            report["checks"]["controller_recovery_retains_running_init"] = True
            command(args.msb,"exec","--stream",private_state["vm"],"--","sh","-ec",
                "cat >>/etc/ssl/certs/ca-certificates.crt; "
                + "printf '%s\\n' '" + host_ip + " host.microsandbox.internal' >>/etc/hosts; "
                + "touch /var/lib/floci-volumes/registry-gate/release",
                input_text="\n"+registry.certificate.read_text(), env=environment)
            wait("private registry image authenticates and runs",lambda:ready("native-private-image"),300)
            assert "private-registry-executed" in kubectl("logs","native-private-image")
            assert registry.authenticated_requests > 0
            report["checks"]["private_registry_secret_real_tls_authenticated_pull"] = True
            authenticated_before = registry.authenticated_requests
            private_restarts = resource("pod","native-private-image")["status"]["containerStatuses"][0]["restartCount"]
            registry.password = secrets.token_hex(24)
            apply(registry.secret())
            command(args.msb,"exec","--no-stdin",lifecycle(private_uid)["vm"],"--","sh","-ec",
                "ctr --address /run/containerd/containerd.sock tasks kill --signal SIGKILL application",env=environment)
            wait("private application restarts using rotated registry secret",lambda:
                resource("pod","native-private-image")["status"]["containerStatuses"][0]["restartCount"] > private_restarts,120)
            wait("rotated registry credentials permit Always pull",lambda:ready("native-private-image"),180)
            assert registry.authenticated_requests > authenticated_before
            report["checks"]["private_registry_secret_rotation_on_always_restart"] = True
            kubectl("delete","pod","native-private-image","--wait=true",timeout=120)
            wait("registry fixture releases RAM",lambda:control_get("health")["activeMemoryMiB"]==0)
            registry.close()
            registry = None
            def volume_version(version):
                return kubectl("exec","native-server","--","sh","-ec",
                    'printf "%s/%s" "$(cat /mounted/settings/nested/value)" "$(cat /mounted/secrets/nested/token)"') == version+"/private-"+version
            def volume_markers():
                return kubectl("exec","native-server","--","sh","-ec",
                    'printf "%s/%s" "$(cat /work/disk/marker)" "$(cat /work/memory/marker)"') == "disk-"+owner+"/memory-"+owner
            if args.proxy_pods:
                if not args.network_relay_binary or not 1 <= args.proxy_pods <= 500:
                    raise ValueError("proxy-pods requires 1..500 and network-relay-binary")
                lazy = pod("lazy-probe",server_script)
                lazy["spec"]["restartPolicy"]="Always"
                lazy["spec"]["containers"][0]["resources"]={"requests":{"cpu":"1", "memory":"768Mi"}}
                lazy["spec"]["containers"][0]["ports"]=[{"name":"http","containerPort":8080}]
                lazy["spec"]["containers"][0]["readinessProbe"]={"httpGet":{"path":"/cgi-bin/source","port":"http"},"periodSeconds":1,"timeoutSeconds":5,"failureThreshold":1}
                lazy["spec"]["containers"][0]["startupProbe"]={"tcpSocket":{"port":"http"},"periodSeconds":1,"timeoutSeconds":5,"failureThreshold":30}
                lazy["spec"]["containers"][0]["livenessProbe"]={"httpGet":{"path":"/cgi-bin/source","port":"http"},"periodSeconds":1,"timeoutSeconds":5,"failureThreshold":3}
                lazy["spec"]["volumes"]=[{"name":"cold-settings","configMap":{"name":"native-cold-settings"}},
                    {"name":"initialization","emptyDir":{}}]
                lazy["spec"]["containers"][0]["volumeMounts"]=[{"name":"cold-settings","mountPath":"/www/settings"},
                    {"name":"initialization","mountPath":"/www/initialization"}]
                lazy["spec"]["initContainers"]=[{"name":"initialize","image":init_image,
                    "command":["sh","-ec","echo once >>/initialize/runs"],
                    "volumeMounts":[{"name":"initialization","mountPath":"/initialize"}]}]
                apply({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"native-cold-settings"},"data":{"version":"before-cold"}})
                lazy_annotations={"microsandbox.dev/availability":"proxy","microsandbox.dev/idle-timeout-seconds":"20"}
                registration_started=time.monotonic()
                apply({"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"lazy-probe"},"spec":{
                    "replicas":args.proxy_pods,"selector":{"matchLabels":{"app":"lazy-probe"}},"template":{"metadata":{"labels":{"app":"lazy-probe"},"annotations":lazy_annotations},"spec":lazy["spec"]}}})
                def proxy_ready():
                    value=resource("deployment","lazy-probe")
                    return value if value.get("status",{}).get("availableReplicas")==args.proxy_pods else None
                wait("lazy rollout Ready without VMs",proxy_ready,1200)
                discovery=control_get("deployments/default/lazy-probe")
                assert len(discovery["pods"])==args.proxy_pods and all(p["state"]=="COLD" for p in discovery["pods"])
                assert control_get("health")["activeMemoryMiB"]==0
                scheduled=json.loads(kubectl("get","pods","-l","app=lazy-probe","-o","json"))["items"]
                assert len(scheduled) == args.proxy_pods
                assert all(value["spec"]["nodeName"]==node for value in scheduled)
                assert all(any(condition["type"] == "Initialized" and condition["status"] == "False"
                               for condition in value["status"]["conditions"]) for value in scheduled)
                assert all(value["spec"]["containers"][0]["resources"]["requests"]=={"cpu":"1", "memory":"768Mi"} for value in scheduled)
                report["scheduling"]={"capacity":resource("node",node)["status"]["allocatable"],
                    "requested_cpu":args.proxy_pods,"requested_memory_mib":args.proxy_pods*768,
                    "active_memory_mib":0,"active_memory_budget_mib":control_get("health")["maxActiveMemoryMiB"]}
                report["checks"]["scheduler_preserves_cold_pod_requests"]=True
                lazy_vms={p["vm"] for p in discovery["pods"]}
                assert not any(v["name"] in lazy_vms for v in json.loads(command(args.msb,"ls","--format","json",env=environment)))
                apply({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"native-cold-settings"},"data":{"version":"after-cold"}})
                relay_rss=docker("exec",peer,"sh","-ec",r'for probe_pid in $(pidof relay); do awk "/VmRSS/ {print \$2}" /proc/$probe_pid/status; done')
                report["proxy_declaration"]={"fixture_replicas":args.proxy_pods,"pods":args.proxy_pods,"ready":args.proxy_pods,"registration_seconds":round(time.monotonic()-registration_started,3),"active_memory_mib":0,"native_vms":0,"controller_rss_bytes":int(command("ps","-o","rss=","-p",str(process.pid)))*1024,"relay_rss_bytes":sum(int(value) for value in relay_rss.split())*1024}
                report["proxy_declaration"]["combined_controller_relay_rss_bytes"]=report["proxy_declaration"]["controller_rss_bytes"]+report["proxy_declaration"]["relay_rss_bytes"]
                chosen=discovery["pods"][0]
                proxy_uid=chosen["uid"]
                relay_path=f"/tmp/fvp-relay-{owner}"
                listener_id=proxy_uid+"-8080"
                def relay_request(method,path,value=None):
                    return json.loads(docker("exec","-i",peer,relay_path+"/relay","client","--socket",relay_path+"/control.sock","--token-file",relay_path+"/token","--method",method,"--path",path,input_text=json.dumps(value) if value else None))
                relay_request("DELETE","/listeners/"+listener_id)
                wait("missing real relay withdraws proxy readiness",lambda:not ready(chosen["name"]))
                assert lifecycle(proxy_uid)["state"]=="COLD"
                relay_request("PUT","/listeners/"+listener_id,{"listenAddress":chosen["podIP"]+":8080","targetAddress":host_ip+":"+chosen["proxyUrl"].rsplit(":",1)[1]})
                wait("repaired real relay restores proxy readiness",lambda:ready(chosen["name"]))
                report["checks"]["proxy_readiness_requires_live_listener"]=True
                kubectl("label","pod",chosen["name"],"proxy-target=true")
                apply({"apiVersion":"v1","kind":"Service","metadata":{"name":"lazy-probe"},"spec":{"selector":{"proxy-target":"true"},"ports":[{"port":80,"targetPort":8080}]}})
                apply(pod("proxy-docker","exec sleep 900",False))
                wait("proxy Docker client ready",lambda:ready("proxy-docker"))
                apply(pod("proxy-native","exec sleep 900"))
                wait("proxy native client ready",lambda:ready("proxy-native"),300)
                wait("lazy Service DNS propagated",lambda:kubectl("exec","proxy-docker","--","nslookup","lazy-probe.default.svc.cluster.local."),60)
                began=time.monotonic()
                response=kubectl("exec","proxy-docker","--","wget","-T","180","-qO-","http://lazy-probe/cgi-bin/source",timeout=210)
                assert response
                assert kubectl("exec","proxy-docker","--","wget","-T","30","-qO-","http://lazy-probe/settings/version")=="after-cold"
                assert kubectl("exec","proxy-docker","--","wget","-T","30","-qO-","http://lazy-probe/initialization/runs")=="once"
                report["checks"]["proxy_ready_defers_init_until_first_request"]=True
                report["checks"]["cold_volume_reads_latest_config_on_first_wake"]=True
                report["proxy_declaration"]["first_service_wake_seconds"]=round(time.monotonic()-began,3)
                assert lifecycle(proxy_uid)["state"]=="READY"
                assert sum(p["state"]=="COLD" for p in control_get("deployments/default/lazy-probe")["pods"])==args.proxy_pods-1
                for url in [f"http://{chosen['podIP']}:8080/cgi-bin/source", "http://lazy-probe/cgi-bin/source"]:
                    print("Checking native proxy route: "+url,flush=True)
                    assert kubectl("exec","proxy-native","--","wget","-T","30","-qO-",url)
                report["checks"]["lazy_rollout_and_real_clusterip_podip_wake"]=True
                marker="proxy-ram-"+owner
                kubectl("exec",chosen["name"],"--","sh","-ec",f"printf '%s' '{marker}' >/dev/shm/marker")
                boot=kubectl("exec",chosen["name"],"--","cat","/proc/sys/kernel/random/boot_id")
                from urllib.parse import urlparse
                target=urlparse(chosen["proxyUrl"])
                held=socket.create_connection((target.hostname,target.port),timeout=10)
                wait("proxy activity lease",lambda:lifecycle(proxy_uid)["activeConnections"]==1)
                try:
                    state=lifecycle(proxy_uid)
                    try: lifecycle(proxy_uid,"hibernate",state["generation"])
                    except RuntimeError as failure: assert "proxy connections are active" in str(failure)
                    else: raise AssertionError("active TCP stream allowed hibernation")
                    time.sleep(22)
                    assert lifecycle(proxy_uid)["state"]=="READY"
                finally: held.close()
                wait("controller automatically hibernates idle proxy",lambda:lifecycle(proxy_uid)["state"]=="SLEEPING",90)
                assert ready(chosen["name"])
                crash_and_restart()
                assert lifecycle(proxy_uid)["state"]=="SLEEPING"
                wait("proxy route restored after controller crash",lambda:ready(chosen["name"]))
                report["checks"]["sleeping_proxy_controller_crash_recovery"]=True
                began=time.monotonic()
                assert kubectl("exec","proxy-docker","--","wget","-T","60","-qO-","http://lazy-probe/cgi-bin/source",timeout=90)
                report["proxy_declaration"]["service_ram_resume_seconds"]=round(time.monotonic()-began,3)
                assert kubectl("exec",chosen["name"],"--","cat","/dev/shm/marker")==marker
                assert kubectl("exec",chosen["name"],"--","cat","/proc/sys/kernel/random/boot_id")==boot
                assert kubectl("exec",chosen["name"],"--","cat","/www/initialization/runs")=="once"
                report["checks"]["proxy_activity_guard_autoidle_and_ram_resume"]=True
                kubectl("delete","deployment","lazy-probe","--wait=true",timeout=900)
                kubectl("delete","pod","proxy-native","proxy-docker","--wait=true",timeout=120)
                wait("proxy fixture releases RAM",lambda:control_get("health")["activeMemoryMiB"]==0,180)
            server = with_volumes(pod("native-server", "mkdir -p /www; httpd -p 9090 -h /www; " + server_script))
            server["spec"]["containers"][0]["ports"] = [{"containerPort":8080}]
            server["spec"]["containers"][0]["readinessProbe"] = {"httpGet":{"path":"/ready","port":9090},"periodSeconds":1,"timeoutSeconds":5,"failureThreshold":1}
            server["spec"]["containers"][0]["startupProbe"] = {"exec":{"command":["test","-f","/tmp/started"]},"periodSeconds":1,"timeoutSeconds":5,"failureThreshold":120}
            server["spec"]["containers"][0]["livenessProbe"] = {"exec":{"command":["test","-f","/tmp/live"]},"periodSeconds":1,"timeoutSeconds":5,"failureThreshold":2,"terminationGracePeriodSeconds":1}
            server["spec"]["restartPolicy"] = "Always"
            server["spec"]["containers"][0]["envFrom"] = [{"configMapRef":{"name":"native-settings"}},{"secretRef":{"name":"native-secrets"}}]
            command_args = server["spec"]["containers"][0]["command"]
            command_args[2] = 'test "$1" = configured; test "$2" = \'$$(SETTING)\'; test "$3" = \'$(UNDEFINED)\'; ' + command_args[2]
            server["spec"]["containers"][0]["args"] = ["expansion-check", "$(SETTING)", "$$(SETTING)", "$(UNDEFINED)"]
            apply({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"native-settings"},"data":{"SETTING":"configured"}})
            apply({"apiVersion":"v1","kind":"Secret","metadata":{"name":"native-secrets"},"stringData":{"SECRET_SETTING":"private-value"}})
            kubectl("delete","configmap","native-volume-settings","--wait=true")
            apply({"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"native-server"},"spec":{
                "replicas":1,"selector":{"matchLabels":{"app":"native-server"}},"template":{"metadata":{"labels":{"app":"native-server"}},"spec":server["spec"]}}})
            server_name = wait("Deployment pod scheduled",lambda:next((pod["metadata"]["name"] for pod in json.loads(kubectl("get","pods","-l","app=native-server","-o","json"))["items"]),None))
            def waiting_for_volume():
                current = resource("pod","native-server")
                status = current.get("status",{})
                return current if status.get("phase")=="Pending" and status.get("reason")=="FailedMount" else None
            pending = wait("missing required volume keeps pod Pending",waiting_for_volume)
            pending_uid = pending["metadata"]["uid"]
            pending_state = lifecycle(pending_uid)
            assert pending_state["state"] == "WAITINGVOLUMES"
            assert control_get("health")["activeMemoryMiB"] == 0
            assert not any(vm["name"]==pending_state["vm"] for vm in json.loads(command(args.msb,"ls","--format","json",env=environment)))
            crash_and_restart()
            recovered_pending = wait("missing volume remains Pending after controller recovery",waiting_for_volume)
            assert recovered_pending["metadata"]["uid"] == pending_uid
            assert lifecycle(pending_uid)["state"] == "WAITINGVOLUMES"
            assert control_get("health")["activeMemoryMiB"] == 0
            assert not any(vm["name"]==pending_state["vm"] for vm in json.loads(command(args.msb,"ls","--format","json",env=environment)))
            volume_sources("initial", old_key=True)
            wait("native task Running", lambda: resource("pod","native-server").get("status",{}).get("phase") == "Running", 300)
            assert resource("pod","native-server")["metadata"]["uid"] == pending_uid
            report["checks"]["missing_volume_waits_without_allocation_and_recovers"] = True
            assert not ready("native-server"), "startup must gate readiness"
            time.sleep(4)
            status=resource("pod","native-server")["status"]["containerStatuses"][0]
            assert status.get("started") is False and status.get("restartCount",0)==0, "startup must gate failing liveness"
            kubectl("exec","native-server","--","touch","/tmp/started","/tmp/live")
            wait("startup probe completes",lambda:resource("pod","native-server")["status"]["containerStatuses"][0].get("started"))
            assert not ready("native-server"), "HTTP readiness must inspect the undeclared health port"
            report["checks"]["startup_gates_readiness_and_liveness"] = True
            report["checks"]["readiness_from_guest_probe"] = True
            print("Native task running; checking actual logs and exec", flush=True)
            assert "native-pod-executed" in kubectl("logs","native-server")
            kubectl("exec","native-server","--","touch","/www/ready")
            native = wait("native server Ready", lambda: ready("native-server"))
            native_ip = native["status"]["podIP"]
            report["native_pod_ip"] = native_ip
            report["checks"]["native_oci_logs_exec"] = True
            assert kubectl("exec","native-server","--","sh","-ec",'printf "%s/%s" "$SETTING" "$SECRET_SETTING"') == "configured/private-value"
            report["checks"]["configmap_secret_environment"] = True
            report["checks"]["pod_command_environment_expansion_and_escaping"] = True
            assert volume_version("initial")
            for mount_path in ("/mounted/settings/binary","/mounted/secrets/binary"):
                actual = kubectl("exec","native-server","--","sh","-ec",f"od -An -tx1 {mount_path} | tr -d ' \\n'")
                assert actual == volume_binary.hex(), f"binary projection changed at {mount_path}"
            kubectl("exec","native-server","--","sh","-ec",'''
                test "$(stat -Lc %a /mounted/settings/nested/value)" = 440
                test "$(stat -Lc %a /mounted/settings/binary)" = 400
                test "$(stat -Lc %a /mounted/secrets/nested/token)" = 400
                test "$(stat -f -c %T /work/memory)" = tmpfs
                test ! -e /work/disk/marker
                test ! -e /work/memory/marker
                test -d /mounted/optional-settings
                test ! -e /mounted/optional-settings/appeared
                test ! -e /mounted/optional-secrets/absent
                if (printf changed >/mounted/settings/nested/value) 2>/dev/null; then exit 1; fi
                if (printf changed >/mounted/secrets/nested/token) 2>/dev/null; then exit 1; fi
            ''')
            memory_capacity = kubectl("exec","native-server","--","df","-Pk","/work/memory").splitlines()[-1].split()[1]
            assert int(memory_capacity) == 16*1024, f"memory emptyDir limit changed: {memory_capacity} KiB"
            kubectl("exec","native-server","--","sh","-ec",
                    f"printf '%s' 'disk-{owner}' >/work/disk/marker; printf '%s' 'memory-{owner}' >/work/memory/marker")
            report["checks"]["volume_binary_modes_readonly_optional_and_memory_limit"] = True
            volume_sources("awake")
            apply({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"native-volume-optional"},"data":{"appeared":"now-present"}})
            wait("mounted ConfigMap and Secret refresh while awake",lambda:volume_version("awake"),120)
            wait("optional missing ConfigMap appears",lambda:kubectl("exec","native-server","--","cat","/mounted/optional-settings/appeared")=="now-present",120)
            wait("removed ConfigMap key disappears",lambda:kubectl("exec","native-server","--","sh","-ec",
                 'test ! -e /mounted/all/removed; printf removed')=="removed",120)
            report["checks"]["awake_volume_updates_and_removed_keys"] = True
            apply({"apiVersion":"v1","kind":"Service","metadata":{"name":"native-server"},"spec":{
                "selector":{"app":"native-server"},"ports":[{"port":80,"targetPort":8080}]}})
            apply(pod("docker-client", "exec sleep 600", False))
            client = wait("Docker client ready", lambda: ready("docker-client"))
            response = kubectl("exec","docker-client","--","wget","-T","10","-qO-",f"http://{native_ip}:8080/cgi-bin/source")
            assert source_ip(response) == client["status"]["podIP"], f"source Pod IP changed: {response}"
            report["checks"]["docker_to_native_pod_ip_and_source"] = True
            response = kubectl("exec","docker-client","--","wget","-T","10","-qO-","http://native-server/cgi-bin/source")
            assert source_ip(response) == client["status"]["podIP"]
            report["checks"]["dns_clusterip"] = True
            native_client = pod("native-client", "exec sleep 600")
            client_container = native_client["spec"]["containers"][0]
            client_container["args"] = client_container.pop("command")
            client_container["workingDir"] = "/tmp"
            native_client["spec"]["securityContext"] = {"runAsUser":1000, "runAsGroup":2000, "runAsNonRoot":True,
                                                       "seccompProfile":{"type":"RuntimeDefault"}}
            client_container["securityContext"] = {"readOnlyRootFilesystem":True, "allowPrivilegeEscalation":False, "capabilities":{"drop":["ALL"]}}
            apply(native_client)
            second = wait("second native pod ready", lambda: ready("native-client"), 300)
            security = kubectl("exec","native-client","--","sh","-ec",
                'test "$(id -u)" = 1000; test "$(id -g)" = 2000; test "$(pwd)" = /tmp; '
                'test "$(awk \'/NoNewPrivs/ {print $2}\' /proc/self/status)" = 1; '
                'test "$(awk \'/^Seccomp:/ {print $2}\' /proc/self/status)" = 2; '
                'test "$(awk \'/CapEff/ {print $2}\' /proc/self/status)" = 0000000000000000; '
                'if output=$(touch /tmp/floci-readonly-check 2>&1); then exit 1; fi; '
                'case "$output" in *"Read-only file system"*) echo secure;; *) exit 1;; esac')
            assert security == "secure"
            report["checks"]["args_only_and_working_directory_override"] = True
            report["checks"]["nonroot_uid_gid_readonly_root_no_privilege_escalation_and_capabilities"] = True
            report["checks"]["runtime_default_seccomp"] = True
            response = kubectl("exec","native-client","--","wget","-T","10","-qO-","http://native-server/cgi-bin/source")
            assert source_ip(response) == second["status"]["podIP"], f"native source Pod IP changed: {response}"
            report["checks"]["native_to_native_service_and_source"] = True
            uid = native["metadata"]["uid"]
            initial_boot = kubectl("exec","native-server","--","cat","/proc/sys/kernel/random/boot_id")
            kubectl("exec","native-server","--","rm","/tmp/started")
            for cycle in (1,2):
                marker = f"ram-cycle-{cycle}-{owner}"
                kubectl("exec","native-server","--","sh","-ec",f"printf '%s' '{marker}' >/dev/shm/marker")
                state = lifecycle(uid)
                began = time.monotonic()
                state = lifecycle(uid,"hibernate",state["generation"])
                assert state["state"] == "SLEEPING"
                volume_sources(f"sleep-{cycle}")
                hibernate_time = time.monotonic()-began
                health = control_get("health")
                snapshot = next(s for s in json.loads(command(args.msb,"snap","ls","--format","json",env=environment)) if s["group"]+":"+s["name"]==state["snapshot"])
                snapshot_disk_bytes = int(command("du","-sk",snapshot["artifact_path"]).split()[0])*1024
                controller_rss_bytes = int(command("ps","-o","rss=","-p",str(process.pid)))*1024
                if cycle == 1:
                    crash_and_restart()
                    assert lifecycle(uid)["state"] == "SLEEPING"
                    report["checks"]["sleeping_controller_crash_recovery"] = True
                wait("sleep removes readiness",lambda:not ready("native-server"))
                lease_before = json.loads(kubectl("get","lease",node,"-n","kube-node-lease","-o","json"))["spec"]["renewTime"]
                wait("shared node heartbeats during sleep",lambda:json.loads(kubectl("get","lease",node,"-n","kube-node-lease","-o","json"))["spec"]["renewTime"] != lease_before,30)
                began = time.monotonic()
                state = lifecycle(uid,"resume",state["generation"])
                wait("restored application Ready",lambda:lifecycle(uid)["state"]=="READY")
                assert resource("pod","native-server")["metadata"]["uid"] == uid
                assert kubectl("exec","native-server","--","cat","/dev/shm/marker") == marker
                assert kubectl("exec","native-server","--","cat","/proc/sys/kernel/random/boot_id") == initial_boot
                assert volume_markers(), "emptyDir data changed across RAM resume"
                assert volume_version(f"sleep-{cycle}"), "mounted sources were not refreshed on resume"
                response = kubectl("exec","docker-client","--","wget","-T","10","-qO-",f"http://{native_ip}:8080/cgi-bin/source")
                assert source_ip(response) == client["status"]["podIP"]
                with urllib.request.urlopen(state["backendUrl"]+"/cgi-bin/source",timeout=10) as response:
                    assert response.status == 200
                report.setdefault("resume_cycles",[]).append({"cycle":cycle,"hibernate_seconds":round(hibernate_time,3),"resume_seconds":round(time.monotonic()-began,3),
                    "guest_memory_mib":768,"active_memory_mib_while_server_sleeps":health["activeMemoryMiB"],"snapshot_disk_bytes":snapshot_disk_bytes,"controller_rss_bytes":controller_rss_bytes})
                if cycle == 1:
                    crash_and_restart()
                    wait("running restored child adopted",lambda:lifecycle(uid)["state"]=="READY")
                    assert kubectl("exec","native-server","--","cat","/dev/shm/marker") == marker
                    report["checks"]["restored_controller_crash_recovery"] = True
            report["checks"]["two_ram_preserving_resumes_and_live_node_lease"] = True
            report["checks"]["startup_success_retained_across_sleep_and_controller_recovery"] = True
            report["checks"]["emptydir_ram_sleep_and_source_refresh"] = True
            kubectl("exec","native-server","--","rm","/www/ready")
            wait("readiness removal", lambda: not ready("native-server"))
            report["checks"]["readiness_loss_observed"] = True
            kubectl("exec","native-server","--","rm","/tmp/live")
            wait("liveness restarts unhealthy task",lambda:resource("pod","native-server")["status"]["containerStatuses"][0].get("restartCount",0)>=1)
            wait("liveness replacement task running",lambda:resource("pod","native-server")["status"]["containerStatuses"][0].get("state",{}).get("running"))
            assert resource("pod","native-server")["metadata"]["uid"]==uid
            assert resource("pod","native-server")["status"]["containerStatuses"][0]["lastState"]["terminated"]["reason"]=="LivenessProbeFailed"
            kubectl("exec","native-server","--","touch","/tmp/started","/tmp/live","/www/ready")
            wait("liveness replacement ready",lambda:ready("native-server"))
            assert volume_markers(), "emptyDir data changed after liveness restart"
            report["checks"]["liveness_restarts_container_without_replacing_pod"] = True
            kubectl("delete","secret","native-volume-secrets","--wait=true")
            command(args.msb,"exec","--no-stdin",lifecycle(uid)["vm"],"--","sh","-ec","printf 'nameserver 127.0.0.1\\n' >/etc/resolv.conf; ctr --address /run/containerd/containerd.sock tasks kill --signal SIGKILL application",env=environment)
            wait("Always restart observed",lambda:resource("pod","native-server")["status"]["containerStatuses"][0].get("restartCount",0)>=2)
            wait("restarted task running",lambda:resource("pod","native-server")["status"]["containerStatuses"][0].get("state",{}).get("running"))
            kubectl("exec","native-server","--","touch","/tmp/started","/tmp/live","/www/ready")
            wait("restarted application Ready",lambda:ready("native-server"))
            assert resource("pod","native-server")["metadata"]["uid"] == uid
            assert volume_markers(), "emptyDir data changed after container crash"
            assert volume_version("sleep-2")
            report["checks"]["emptydir_preserved_across_container_restarts"] = True
            report["checks"]["last_good_volume_survives_missing_source_and_restart"] = True
            volume_sources("sleep-2")
            report["checks"]["deployment_always_restart_preserves_pod_uid"] = True
            report["checks"]["cached_image_restart_without_registry_dns"] = True
            request = urllib.request.Request(f"http://127.0.0.1:{control_port}/v1/deployments/default/native-server",headers={"Authorization":"Bearer "+token})
            with urllib.request.urlopen(request,timeout=30) as response:
                deployment = json.load(response)
            assert deployment["replicas"] == 1 and deployment["clusterName"] == "qualification" and deployment["pods"][0]["uid"] == uid
            report["checks"]["deployment_owner_discovery"] = True
            failed = pod("startup-failure", "exec sleep 600")
            failed["spec"]["containers"][0]["startupProbe"]={"tcpSocket":{"port":9099},"periodSeconds":1,"timeoutSeconds":2,"failureThreshold":2,"terminationGracePeriodSeconds":1}
            apply(failed)
            status=wait("failed TCP startup terminates Never pod",lambda:(value if (value:=resource("pod","startup-failure")).get("status",{}).get("phase")=="Failed" else None),300)
            assert status["status"]["containerStatuses"][0].get("restartCount",0)==0
            assert status["status"]["containerStatuses"][0]["state"]["terminated"]["reason"]=="StartupProbeFailed"
            report["checks"]["tcp_startup_failure_respects_restart_never"]=True
            kubectl("delete","pod","startup-failure","--wait=true",timeout=180)
            if args.floci_checkout:
                cold_spec=json.loads(json.dumps(server["spec"]))
                cold_spec["initContainers"]=[{"name":"initialize","image":init_image,
                    "command":["sh","-ec","sleep 2; echo initialized >/work/initialized"],
                    "volumeMounts":[{"name":"disk-cache","mountPath":"/work"}]}]
                cold_spec["containers"][0]["command"]=["sh","-ec","mkdir -p /www; touch /www/ready /tmp/started /tmp/live; httpd -p 9090 -h /www; "+server_script]
                cold_annotations={"microsandbox.dev/on-demand":"true"}
                if args.proxy_pods:
                    cold_annotations.update({"microsandbox.dev/availability":"proxy","microsandbox.dev/idle-owner":"external"})
                apply({"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"native-cold"},"spec":{
                    "replicas":1,"selector":{"matchLabels":{"app":"native-cold"}},"template":{"metadata":{"labels":{"app":"native-cold"},"annotations":cold_annotations},"spec":cold_spec}}})
                if args.proxy_pods:
                    wait("external lazy Deployment ready",lambda:resource("deployment","native-cold").get("status",{}).get("availableReplicas")==1)
                cold_deployment=wait("cold Deployment discovery",lambda:control_get("deployments/default/native-cold"))
                assert cold_deployment["pods"][0]["state"]=="COLD"
                java_env = dict(os.environ,JAVA_HOME=args.java_home or os.environ.get("JAVA_HOME",""),FLOCI_NATIVE_POD_TEST="true",FLOCI_NATIVE_POD_CONTROLLER_URL=f"http://127.0.0.1:{control_port}",
                    FLOCI_NATIVE_POD_CONTROLLER_TOKEN=token,FLOCI_NATIVE_POD_NAMESPACE="default",FLOCI_NATIVE_POD_DEPLOYMENT="native-cold",
                    FLOCI_NATIVE_POD_BACKEND_URL=cold_deployment["pods"][0]["backendUrl"],FLOCI_NATIVE_POD_HTTP_PATH="/cgi-bin/source",FLOCI_NATIVE_POD_EXPECT_COLD="true")
                if args.proxy_pods:
                    java_env["FLOCI_NATIVE_POD_PROXY_URL"]=cold_deployment["pods"][0]["proxyUrl"]
                if args.maven_ready_file:
                    print("Waiting for Maven-ready file: "+args.maven_ready_file,flush=True)
                    wait("Maven slot released",lambda:Path(args.maven_ready_file).is_file(),1800)
                print("Starting Java native gateway and contract tests",flush=True)
                completed = subprocess.run([str(Path(args.floci_checkout)/"mvnw"),"-Dtest=NativePodWorkloadIntegrationTest,PodControllerClientServiceTest,KubernetesWorkloadRuntimeServiceTest,OnDemandConfigLoaderServiceTest,ActivationCoordinatorServiceTest","test"],
                    cwd=args.floci_checkout,env=java_env,stdout=log,stderr=log,timeout=900)
                if completed.returncode: raise RuntimeError("Java native workload integration test failed; see controller log")
                report["checks"]["java_on_demand_http_wake"] = True
                cold_name = cold_deployment["pods"][0]["name"]
                assert kubectl("exec",cold_name,"--","cat","/work/disk/initialized")=="initialized"
                kubectl("exec",cold_name,"--","sh","-ec","test ! -e /work/disk/marker; test ! -e /work/memory/marker")
                report["checks"]["new_pod_emptydir_starts_empty"] = True
                kubectl("delete","deployment","native-cold","--wait=true",timeout=90)
            kubectl("delete","deployment","native-server","--wait=true",timeout=90)
            kubectl("delete","pod","native-client","--wait=true",timeout=90)
            owned_vms = {"fvp-"+value["metadata"]["uid"].replace("-","")[:20] for value in (native,second)}
            wait("native VM removal",lambda: not [v for v in json.loads(command(args.msb,"ls","--format","json",env=environment)) if v["name"] in owned_vms])
            report["checks"]["pod_deletion_removes_vms"] = True
            report["result"] = "passed"
    except QualificationComplete:
        report["result"] = "passed"
    except (Exception, KeyboardInterrupt) as error:
        report["result"] = "failed"
        report["error"] = str(error) or type(error).__name__
        try:
            report["peer_network"] = docker("exec", peer, "sh", "-c", "ip -d link show; ip route; ip neigh; iptables-save -c")
            report["pods"] = json.loads(kubectl("get","pods","-o","json"))
            if args.proxy_pods:
                report["relay_inventory"] = json.loads(docker("exec",peer,f"/tmp/fvp-relay-{owner}/relay","client","--socket",f"/tmp/fvp-relay-{owner}/control.sock","--token-file",f"/tmp/fvp-relay-{owner}/token","--method","GET","--path","/listeners"))
            for vm in json.loads(command(args.msb,"ls","--format","json",env=environment)):
                if vm["name"].startswith("fvp-"):
                    inspection = json.loads(command(args.msb,"inspect","--format","json",vm["name"],env=environment))
                    if inspection.get("config",{}).get("labels",{}).get(LABEL) != owner and vm["name"] not in {"fvp-"+p.stem.replace("-","")[:20] for p in (temporary/"state").glob("*.json")}: continue
                    report[vm["name"]+"_pull"] = command(args.msb,"exec","--no-stdin",vm["name"],"--","sh","-c","cat /tmp/image-pull.log 2>/dev/null || true",env=environment)
                    report[vm["name"]] = command(args.msb,"exec",vm["name"],"--","sh","-c","ip -d link show; ip route; bridge fdb show; ip neigh; iptables-save -t nat; cat /tmp/application.log 2>/dev/null; ctr --address /run/containerd/containerd.sock tasks exec --exec-id diagnostic application sh -c 'cat /etc/resolv.conf; nslookup native-server; nslookup native-server.default.svc.cluster.local 10.43.0.10'",env=environment)
        except Exception as diagnostic_error:
            report["diagnostic_error"] = str(diagnostic_error)
    finally:
        if process:
            process.terminate()
            try: process.wait(timeout=120)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait(); report["controller_forced_exit"] = True
        cleanup_errors = []
        if registry:
            try:
                registry.close()
            except Exception as error:
                cleanup_errors.append(str(error))
        try:
            records={}
            if "temporary" in locals():
                for path in (temporary/"state").glob("*.json"):
                    record=json.loads(path.read_text())
                    records[record["view"]["vm"]]=record
            for vm in json.loads(command(args.msb,"ls","--format","json",env=environment)):
                inspection=json.loads(command(args.msb,"inspect","--format","json",vm["name"],env=environment))
                configs=[inspection.get("config",{})]
                if inspection.get("active_config") is not None: configs.append(inspection["active_config"])
                labeled=all(c.get("labels",{}).get(LABEL)==owner for c in configs)
                record=records.get(vm["name"])
                parent=record.get("restoredParent") if record else None
                lineage=parent and all(c.get("snapshot_parent")==parent for c in configs)
                if labeled or lineage:
                    command(args.msb,"rm","--force",vm["name"],env=environment)
            snapshots=json.loads(command(args.msb,"snap","ls","--format","json",env=environment))
            for record in records.values():
                for selector,identity in record.get("snapshotIDs",{}).items():
                    current=next((s for s in snapshots if s["group"]+":"+s["name"]==selector),None)
                    if current is not None:
                        if current["snapshot_id"]!=identity: raise RuntimeError("cleanup snapshot ownership changed")
                        command(args.msb,"snap","rm",selector,env=environment)
            if "temporary" in locals() and (temporary/"state"/"native-command.pending").exists():
                raise RuntimeError("native operation outcome uncertain; retained private state for reconciliation")
        except Exception as error: cleanup_errors.append(str(error))
        if created:
            try:
                inspection = json.loads(docker("inspect",peer))[0]
                if inspection["Config"]["Labels"].get(LABEL) != owner: raise RuntimeError("Docker owner changed")
                docker("rm","-fv",peer)
            except Exception as error: cleanup_errors.append(str(error))
        log.close()
        if "directory" in locals():
            if cleanup_errors:
                report["retained_state_directory"]=directory
            else:
                import shutil
                shutil.rmtree(directory)
        report["cleanup"] = "passed" if not cleanup_errors else cleanup_errors
        report["controller_log"] = str(log_path)
        Path(args.report).write_text(json.dumps(report,indent=2)+"\n")
        print(json.dumps(report,indent=2),flush=True)
    return report["result"] == "passed" and not cleanup_errors

if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--controller",required=True)
    parser.add_argument("--msb",required=True)
    parser.add_argument("--msb-home",required=True)
    parser.add_argument("--report",required=True)
    parser.add_argument("--floci-checkout")
    parser.add_argument("--java-home")
    parser.add_argument("--maven-ready-file")
    parser.add_argument("--cold-pods",type=int,default=2)
    parser.add_argument("--proxy-pods",type=int,default=0)
    parser.add_argument("--applications-only", action="store_true", help="qualify multiple regular containers using the shared owned fixture")
    parser.add_argument("--operations-only", action="store_true", help="qualify TTY, port forwarding and gRPC probes using the shared owned fixture")
    parser.add_argument("--network-relay-binary")
    parser.add_argument("--scheduling-cpu", help="virtual CPU capacity advertised to the Kubernetes scheduler")
    parser.add_argument("--scheduling-memory", help="virtual memory capacity advertised to the Kubernetes scheduler")
    raise SystemExit(0 if qualify(parser.parse_args()) else 1)
