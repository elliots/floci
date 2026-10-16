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

LABEL = "io.floci.pod-probe"


def command(*args, timeout=60, input_text=None, env=None):
    result = subprocess.run(args, text=True, input=input_text, capture_output=True, timeout=timeout, env=env)
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
            raise RuntimeError(json.dumps(pod["status"]))
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
            process = subprocess.Popen(controller_args, stdout=log, stderr=log)
            def control_get(path):
                request = urllib.request.Request(f"http://127.0.0.1:{control_port}/v1/"+path,headers={"Authorization":"Bearer "+token})
                with urllib.request.urlopen(request,timeout=30) as response: return json.load(response)
            def crash_and_restart():
                nonlocal process
                process.kill()
                process.wait(timeout=30)
                process = subprocess.Popen(controller_args,stdout=log,stderr=log)
                wait("controller journal recovery",lambda:control_get("health"),900)
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
                lazy["spec"]["containers"][0]["ports"]=[{"name":"http","containerPort":8080}]
                lazy["spec"]["containers"][0]["readinessProbe"]={"httpGet":{"path":"/cgi-bin/source","port":"http"},"periodSeconds":1,"timeoutSeconds":5,"failureThreshold":1}
                lazy["spec"]["containers"][0]["startupProbe"]={"tcpSocket":{"port":"http"},"periodSeconds":1,"timeoutSeconds":5,"failureThreshold":30}
                lazy["spec"]["containers"][0]["livenessProbe"]={"httpGet":{"path":"/cgi-bin/source","port":"http"},"periodSeconds":1,"timeoutSeconds":5,"failureThreshold":3}
                lazy["spec"]["volumes"]=[{"name":"cold-settings","configMap":{"name":"native-cold-settings"}}]
                lazy["spec"]["containers"][0]["volumeMounts"]=[{"name":"cold-settings","mountPath":"/www/settings"}]
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
            apply(pod("native-client", "exec sleep 600"))
            second = wait("second native pod ready", lambda: ready("native-client"), 300)
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
                kubectl("exec",cold_name,"--","sh","-ec","test ! -e /work/disk/marker; test ! -e /work/memory/marker")
                report["checks"]["new_pod_emptydir_starts_empty"] = True
                kubectl("delete","deployment","native-cold","--wait=true",timeout=90)
            kubectl("delete","deployment","native-server","--wait=true",timeout=90)
            kubectl("delete","pod","native-client","--wait=true",timeout=90)
            owned_vms = {"fvp-"+value["metadata"]["uid"].replace("-","")[:20] for value in (native,second)}
            wait("native VM removal",lambda: not [v for v in json.loads(command(args.msb,"ls","--format","json",env=environment)) if v["name"] in owned_vms])
            report["checks"]["pod_deletion_removes_vms"] = True
            report["result"] = "passed"
    except Exception as error:
        report["result"] = "failed"
        report["error"] = str(error)
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
                docker("rm","-f",peer)
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
    parser.add_argument("--network-relay-binary")
    raise SystemExit(0 if qualify(parser.parse_args()) else 1)
