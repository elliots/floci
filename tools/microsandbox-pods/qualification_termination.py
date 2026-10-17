"""The same ordinary Pod shutdown contract on Docker and native workers."""
import time

import manage


def qualify_plain_termination(installation, worker_modes=(False, True)):
    q = installation
    observer = q.pod("termination-observer", """mkdir -p /www/cgi-bin /data
cat >/www/cgi-bin/record <<'CGI'
#!/bin/sh
printf '%s\\n' "$QUERY_STRING" >>/data/observed
printf 'Content-Type: text/plain\\r\\n\\r\\nrecorded\\n'
CGI
chmod +x /www/cgi-bin/record
exec httpd -f -p 8081 -h /www""", native=False)
    observer["spec"]["containers"][0]["ports"] = [{"containerPort":8081}]
    q.apply(observer)
    q.apply({"apiVersion":"v1", "kind":"Service", "metadata":{"name":"termination-observer"},
        "spec":{"selector":{"app":"termination-observer"},"ports":[{"port":80,"targetPort":8081}]}})

    def ready(name):
        value = q.resource("pod", name)
        return value if any(condition["type"]=="Ready" and condition["status"]=="True"
            for condition in value.get("status",{}).get("conditions",[])) else None

    def observations():
        return q.kubectl("exec","termination-observer","--","sh","-c","cat /data/observed 2>/dev/null || true").splitlines()

    manage.wait("termination observer ready", lambda: ready("termination-observer"))
    destination = "http://termination-observer.default.svc.cluster.local/cgi-bin/record?"
    for native in worker_modes:
        kind = "native" if native else "docker"
        for zero_grace in (False, True):
            name = kind + ("-zero-grace" if zero_grace else "-graceful")
            script = ("trap 'wget -T 4 -qO- " + destination + name + "-started; sleep 1; wget -T 4 -qO- "
                + destination + name + "-finished; exit 0' TERM\n"
                "touch /tmp/trap-ready\nwhile :; do sleep .2; done")
            value = q.pod(name, script, native=native)
            value["spec"]["terminationGracePeriodSeconds"] = 0 if zero_grace else 15
            container = value["spec"]["containers"][0]
            container["readinessProbe"] = {"exec":{"command":["test","-f","/tmp/trap-ready"]},"periodSeconds":1}
            if zero_grace:
                container["lifecycle"] = {"preStop":{"exec":{"command":["wget","-T","4","-qO-",destination+name+"-hook"]}}}
            q.apply(value)
            pod = manage.wait(name+" trap installed", lambda: ready(name))
            uid = pod["metadata"]["uid"]
            began = time.monotonic()
            q.kubectl("delete","pod",name,"--wait=true",timeout=60)
            if native:
                manage.wait(name+" native ownership cleanup",lambda:not (q.directory/"controller"/(uid+".json")).exists(),timeout=45)
                assert q.control("health")["activeMemoryMiB"]==0
            else:
                manage.wait(name+" container exit", lambda:not q.command("docker","exec",q.record["peer"],
                    "crictl","ps","--label","io.kubernetes.pod.uid="+uid,"-q").strip(),timeout=30)
            elapsed = time.monotonic()-began
            observed = observations()
            if zero_grace:
                q.report.setdefault("zeroGraceObservations", {})[kind] = {
                    "elapsedSeconds": elapsed,
                    "preStopRan": name+"-hook" in observed,
                    "sigtermObserved": name+"-started" in observed,
                }
                # Kubelet applies a minimum effective grace and may run preStop
                # even for zero (kubernetes/kubernetes#123408). The native
                # provider intentionally retains its explicit zero-grace skip.
                if native:
                    assert name+"-hook" not in observed,observed
                assert elapsed<12, "configured zero-grace deletion did not finish promptly: "+str(elapsed)
                q.report["checks"][kind+"_configured_zero_grace_cleanup"] = True
            else:
                assert name+"-started" in observed and name+"-finished" in observed,observed
                assert elapsed>=1, "ordinary application was removed before its shutdown work completed"
                q.report["checks"][kind+"_plain_app_sigterm_and_shutdown_completion"] = True
