#!/usr/bin/env python3
"""Qualify the installed supervisor with an owned mixed HTTP graph and TLS identity admission."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.error
import urllib.request
import uuid

import manage
import manage_supervisor
from qualification_aws_identity import qualify_aws_identity


class ManagedQualification:
    def __init__(self, args):
        self.args = args
        self.root = Path(tempfile.mkdtemp(prefix="fmq-", dir=args.work_directory)).resolve()
        self.directory = self.root/"cluster"
        self.report = {"directory":str(self.root), "checks":{}}
        self.record = None
        self.environment = dict(os.environ, MSB_HOME=str(Path(args.msb_home).resolve()))
        self.log = (self.root/"qualification.log").open("w")
        self.manager_args = argparse.Namespace(directory=str(self.directory), memory_mib=getattr(args, "memory_mib", 1536),
            scheduling_cpu="500", scheduling_memory="384Gi", pod_workers=8,
            network_policy=getattr(args, "network_policy", False))

    def command(self, *values, env=None, **kwargs):
        return manage.command(*values, environment=env, **kwargs)

    def kubectl(self, *values, **kwargs):
        return manage.kubectl(self.record, *values, **kwargs)

    def resource(self, kind, name):
        return json.loads(self.kubectl("get", kind, name, "-o", "json"))

    def apply(self, value):
        value.setdefault("metadata", {}).setdefault("labels", {})["floci-qualification"] = self.record["owner"]
        return manage.apply(self.record, value)

    def control(self, path, data=None):
        token = manage.read_private(self.directory/"control-token").strip()
        request = urllib.request.Request("http://127.0.0.1:"+str(self.record["controlPort"])+"/v1/"+path,
            data=json.dumps(data).encode() if data is not None else None,
            headers={"Authorization":"Bearer "+token,"Content-Type":"application/json"})
        with urllib.request.urlopen(request, timeout=240) as response:
            return json.load(response)

    def lifecycle(self, uid, operation=None, generation=None):
        path = "pods/"+uid
        if operation:
            return self.control(path+"/"+operation, {"expectedGeneration":generation})
        return self.control(path)

    def pod(self, name, script, native=True):
        return {"apiVersion":"v1","kind":"Pod","metadata":{"name":name,"labels":{"app":name}},"spec":{
            "nodeSelector":{"microsandbox.dev/runtime":"native"} if native else {"kubernetes.io/hostname":self.record["peer"]},
            "tolerations":[{"key":"microsandbox.dev/runtime","operator":"Equal","value":"native","effect":"NoSchedule"}],
            "automountServiceAccountToken":False,"restartPolicy":"Never","containers":[{
                "name":"app","image":"public.ecr.aws/docker/library/busybox:1.37.0","command":["sh","-ec",script]}]}}

    def prepare(self):
        for operation in (("ls",), ("snapshot", "ls"), ("volume", "ls")):
            assert json.loads(self.command(self.args.msb, *operation, "--format", "json", env=self.environment)) == [], "runtime home must start empty"
        host = self.args.node_ip
        if not host:
            probe = "floci-managed-address-"+secrets.token_hex(8)
            self.command("docker","run","-d","--rm","--name",probe,"--entrypoint","sleep",manage.K3S_IMAGE,"60")
            try:
                host = manage.node_address({"peer":probe})
            finally:
                self.command("docker","rm","-f",probe)
        manage.prepare(argparse.Namespace(directory=str(self.directory), binaries=self.args.binaries,
            msb=self.args.msb, msb_home=self.args.msb_home, node_ip=host,
            **{name: getattr(self.args, name, None) for name in
                ("environment_network", "isolation_helper", "environment_dns", "peer_udp_port", "peer_images")}))
        self.record = manage.load(self.directory)
        self.report.update(owner=self.record["owner"], nodeIP=host, peer=self.record["peer"])
        manage_supervisor.start(self.manager_args)
        manage.wait("default service account", lambda:self.resource("serviceaccount","default"))
        manage.wait("native node Ready", lambda:any(c["type"]=="Ready" and c["status"]=="True"
            for c in self.resource("node",self.record["node"])["status"]["conditions"]))
        def dns():
            pods=json.loads(self.kubectl("get","pods","-n","kube-system","-l","k8s-app=kube-dns","-o","json"))["items"]
            return pods and all(any(c["type"]=="Ready" and c["status"]=="True" for c in p.get("status",{}).get("conditions",[])) for p in pods)
        manage.wait("cluster DNS Ready", dns)
        self.report["checks"]["installed_restricted_controller_and_background_supervisor"] = True

    def crash_and_restart(self):
        before = manage_supervisor.request(self.directory,"status")
        assert before["controllerReady"]
        # The authenticated live manager identifies its current child. No PID file is used.
        os.kill(before["controllerPID"],signal.SIGKILL)
        def recovered():
            value=manage_supervisor.request(self.directory,"status")
            return value if value["controllerReady"] and value["controllerPID"]!=before["controllerPID"] else None
        manage.wait("supervised controller crash recovery",recovered,timeout=180)

    def state(self,name):
        deployment=self.control("deployments/default/"+name)
        return deployment["pods"][0] if len(deployment["pods"])==1 else None

    def fetch(self,name):
        state=self.state(name)
        try:
            with urllib.request.urlopen(state["proxyUrl"]+"/cgi-bin/data",timeout=180) as response:
                return response.status,response.read().decode()
        except urllib.error.HTTPError as error:
            return error.code,error.read().decode()

    def deploy(self,name,body,version="v1"):
        script="""mkdir -p /www/cgi-bin
export RAM_MARKER="$(cat /proc/sys/kernel/random/uuid)"
cat >/www/cgi-bin/data <<'CGI'
#!/bin/sh
set -e
printf 'Content-Type: text/plain\\r\\n\\r\\n'
"""+body+"\nprintf '%s\\n' \"$RAM_MARKER\"\nCGI\nchmod +x /www/cgi-bin/data\nexec httpd -f -p 8080 -h /www"
        pod=self.pod(name,script)
        pod["spec"]["restartPolicy"]="Always"
        pod["spec"]["containers"][0].update(ports=[{"name":"http","containerPort":8080}],env=[{"name":"VERSION","value":version}])
        if name=="catalog":
            pod["spec"]["volumes"]=[{"name":"catalog","configMap":{"name":"catalog"}}]
            pod["spec"]["containers"][0]["volumeMounts"]=[{"name":"catalog","mountPath":"/data","readOnly":True}]
        if name=="counter":
            pod["spec"]["volumes"]=[{"name":"counter","emptyDir":{"medium":"Memory"}}]
            pod["spec"]["containers"][0]["volumeMounts"]=[{"name":"counter","mountPath":"/data"}]
            pod["spec"]["initContainers"]=[{"name":"initialize","image":"public.ecr.aws/docker/library/busybox:1.37.0",
                "command":["sh","-ec","echo 0 >/data/count"],"volumeMounts":[{"name":"counter","mountPath":"/data"}]}]
        self.apply({"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":name},"spec":{
            "replicas":1,"selector":{"matchLabels":{"app":name}},"template":{"metadata":{"labels":{"app":name,"floci-qualification":self.record["owner"]},
                "annotations":{"microsandbox.dev/availability":"proxy","microsandbox.dev/idle-owner":"external"}},"spec":pod["spec"]}}})
        self.apply({"apiVersion":"v1","kind":"Service","metadata":{"name":name},"spec":{
            "selector":{"app":name},"ports":[{"name":"http","port":80,"targetPort":8080}]}})
        manage.wait(name+" cold rollout",lambda:self.resource("deployment",name).get("status",{}).get("availableReplicas")==1)
        manage.wait(name+" single discovered Pod",lambda:self.state(name))

    def sleep(self,name):
        state=self.state(name)
        if state["state"] in ("READY","RUNNING"):
            self.lifecycle(state["uid"],"hibernate",state["generation"])

    def graph(self):
        self.apply({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"catalog"},"data":{"item":"catalog-item"}})
        catalog="cat /data/item; printf '\\n'"
        frontend="wget -T 120 -qO- http://catalog.default.svc.cluster.local/cgi-bin/data; printf 'frontend:%s\\n' \"$VERSION\""
        counter="count=$(cat /data/count); count=$((count+1)); echo \"$count\" >/data/count; printf 'count:%s\\n' \"$count\""
        for name,body in (("catalog",catalog),("frontend",frontend),("counter",counter)):
            self.deploy(name,body)
        assert all(self.state(name)["state"]=="COLD" for name in ("catalog","frontend","counter"))
        assert self.control("health")["activeMemoryMiB"]==0
        assert json.loads(self.command(self.args.msb,"ls","--format","json",env=self.environment))==[]
        self.report["checks"]["different_http_manifests_roll_out_without_guest_boot"] = True
        with ThreadPoolExecutor(max_workers=4) as workers:
            replies=list(workers.map(lambda _:self.fetch("frontend"),range(4)))
        assert all(status==200 and "catalog-item" in body and "frontend:v1" in body for status,body in replies),replies
        initial=replies[0][1]
        assert len(initial.splitlines())==4,initial
        markers=[str(uuid.UUID(initial.splitlines()[index])) for index in (1,3)]
        self.report["graphMarkers"]=markers
        assert all(body==initial for _,body in replies)
        self.report["checks"]["concurrent_nested_service_http_wakes_only_two_guests"] = True
        assert self.control("health")["activeMemoryMiB"]==1536
        assert self.state("counter")["state"]=="COLD"
        for _ in range(2):
            metrics=self.kubectl("get","--raw","/api/v1/nodes/"+self.record["node"]+"/proxy/metrics/resource")
            assert "container_cpu_usage_seconds_total" in metrics and "container_memory_working_set_bytes" in metrics,metrics
            stats=json.loads(self.kubectl("get","--raw","/api/v1/nodes/"+self.record["node"]+"/proxy/stats/summary"))
            assert len(stats["pods"])==2 and all(c["memory"]["workingSetBytes"]>0 for p in stats["pods"] for c in p["containers"]),stats
            time.sleep(1)
        self.report["checks"]["installed_kubelet_reports_awake_container_resources"] = True
        with ThreadPoolExecutor(max_workers=1) as workers:
            waiting=workers.submit(self.fetch,"counter")
            time.sleep(2)
            assert not waiting.done(), "third guest should wait while both memory reservations are held"
            assert self.control("health")["activeMemoryMiB"]==1536
            assert len(json.loads(self.command(self.args.msb,"ls","--format","json",env=self.environment)))==2
            self.sleep("frontend")
            first_counter=waiting.result(timeout=120)
            assert first_counter[0]==200 and "count:1" in first_counter[1],first_counter
        self.sleep("counter")
        self.report["checks"]["third_guest_waits_for_memory_then_wakes_without_eviction"] = True
        self.sleep("catalog")
        before={name:(self.state(name)["uid"],self.state(name)["generation"]) for name in ("catalog","frontend","counter")}
        metrics=self.kubectl("get","--raw","/api/v1/nodes/"+self.record["node"]+"/proxy/metrics/resource")
        stats=json.loads(self.kubectl("get","--raw","/api/v1/nodes/"+self.record["node"]+"/proxy/stats/summary"))
        assert not stats["pods"] and not any(line.startswith("container_") for line in metrics.splitlines()),stats
        assert self.control("health")["activeMemoryMiB"]==0
        self.report["checks"]["cold_and_sleeping_scrapes_omit_samples_without_wake"] = True
        current=manage_supervisor.request(self.directory,"status")
        manage_supervisor.request(self.directory,"renew")
        manage.wait("renewed controller ready",lambda:manage_supervisor.request(self.directory,"status")["controllerReady"])
        renewed=manage_supervisor.request(self.directory,"status")
        assert renewed["credentialGeneration"]!=current["credentialGeneration"] and renewed["controllerPID"]!=current["controllerPID"]
        assert before=={name:(self.state(name)["uid"],self.state(name)["generation"]) for name in before}
        self.report["checks"]["complete_certificate_rotation_preserves_cold_and_sleeping_identities"] = True
        self.crash_and_restart()
        assert self.fetch("frontend")== (200,initial)
        self.report["checks"]["supervised_crash_and_http_resume_preserve_graph_ram"] = True
        self.sleep("frontend")
        self.sleep("catalog")
        first=self.fetch("counter")
        assert first[0]==200 and "count:2" in first[1],first
        uuid.UUID(first[1].splitlines()[-1])
        self.sleep("counter")
        second=self.fetch("counter")
        assert second[0]==200 and "count:3" in second[1] and first[1].splitlines()[-1]==second[1].splitlines()[-1],second
        self.sleep("counter")
        self.report["checks"]["init_and_memory_emptydir_survive_counter_resume"] = True
        old=self.state("frontend")["uid"]
        self.deploy("frontend",frontend,"v2")
        manage.wait("cold replacement rollout",lambda:(value["uid"]!=old and value["state"]=="COLD") if (value:=self.state("frontend")) else False)
        assert self.control("health")["activeMemoryMiB"]==0
        assert "frontend:v2" in self.fetch("frontend")[1]
        self.sleep("frontend")
        self.sleep("catalog")
        self.report["checks"]["deployment_replacement_completes_cold_then_wakes_new_version"] = True

    def identities(self):
        args=self.args
        args.installed_webhook=True
        args.network_relay_binary=str(Path(args.binaries)/"network-relay")
        args.identity_expiry_wait=False
        qualify_aws_identity(args=args,peer=self.record["peer"],host_ip=self.record["hostIP"],temporary=self.root,
            pod=self.pod,apply=self.apply,resource=self.resource,kubectl=self.kubectl,lifecycle=self.lifecycle,
            control_get=self.control,crash_and_restart=self.crash_and_restart,wait=manage.wait,command=self.command,
            report=self.report,log=self.log,failure=RuntimeError)

    def delete_workloads(self):
        label="floci-qualification="+self.record["owner"]
        self.kubectl("delete","deployment,pod,service,configmap,serviceaccount,networkpolicy","-l",label,"--ignore-not-found","--wait=false")
        manage.wait("native Pod journals removed",lambda:not list((self.directory/"controller").glob("*.json")),timeout=240)
        manage.wait("Pod API objects removed",lambda:not json.loads(self.kubectl("get","pods","-A","--field-selector","spec.nodeName="+self.record["node"],"-o","json"))["items"],timeout=180)

    def cleanup(self):
        if self.record is None and (self.directory/"cluster.json").exists():
            self.record=manage.load(self.directory)
        if self.record is None:
            return
        self.delete_workloads()
        for arguments in (("ls",),("snapshot","ls"),("volume","ls")):
            assert json.loads(self.command(self.args.msb,*arguments,"--format","json",env=self.environment))==[],arguments
        manage_supervisor.stop(self.manager_args)
        manage.destroy(argparse.Namespace(directory=str(self.directory)))
        self.report["checks"]["owned_runtime_and_managed_cluster_cleanup"] = True

    def run(self, scenario=None):
        """Run a callback against this installed cluster, then verify owned cleanup."""
        try:
            self.prepare()
            if scenario is not None:
                scenario(self)
            elif getattr(self.args, "installed_policy_only", False):
                from qualification_installed import qualify_installed_policy
                qualify_installed_policy(self)
            elif getattr(self.args, "termination_only", False):
                from qualification_termination import qualify_plain_termination
                qualify_plain_termination(self)
            else:
                if not self.args.identity_only:
                    self.graph()
                if self.args.floci_checkout:
                    self.identities()
        except Exception as error:
            self.report["failure"]=type(error).__name__+": "+str(error)
            self.report["traceback"]=traceback.format_exc()
            print(json.dumps({"failure":self.report["failure"],"traceback":self.report["traceback"]},indent=2),flush=True)
        finally:
            try:
                self.cleanup()
            except Exception as error:
                self.report["cleanupFailure"]=type(error).__name__+": "+str(error)
                self.report["retainedForRecovery"]=str(self.root)
            self.log.close()
            Path(self.args.report).write_text(json.dumps(self.report,indent=2)+"\n")
            print(json.dumps(self.report,indent=2))
        return 1 if "failure" in self.report or "cleanupFailure" in self.report else 0


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ("binaries","msb","msb-home","report"):
        parser.add_argument("--"+name,required=True)
    parser.add_argument("--node-ip")
    parser.add_argument("--work-directory",default="/tmp")
    parser.add_argument("--floci-checkout",help="also run installed TLS webhook and shared AWS SDK identity checks")
    modes=parser.add_mutually_exclusive_group()
    modes.add_argument("--identity-only",action="store_true")
    modes.add_argument("--termination-only",action="store_true",help="shared Docker/native ordinary and zero-grace shutdown checks")
    modes.add_argument("--installed-policy-only",action="store_true",help="restricted installed policy, metrics and storage-preview smoke")
    parser.add_argument("--network-policy",action="store_true",help="explicitly enable native node-wide NetworkPolicy enforcement")
    parser.add_argument("--java-home")
    args=parser.parse_args()
    if args.installed_policy_only and not args.network_policy:
        parser.error("installed-policy-only requires network-policy")
    if args.identity_only and not args.floci_checkout:
        parser.error("identity-only requires floci-checkout")
    return ManagedQualification(args).run()


if __name__=="__main__":
    raise SystemExit(main())
