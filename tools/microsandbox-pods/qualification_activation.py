"""Exercise the Java wake coordinator against multiple cold replicas and one guest's RAM budget."""
import json
import os
from pathlib import Path
import subprocess


def qualify_activation(*, args, pod, apply, resource, control_get, wait, report, log, control_port, token):
    if not args.floci_checkout:
        raise RuntimeError("activation qualification requires --floci-checkout")
    script = """mkdir -p /www
echo ready >/www/ready
cat /proc/sys/kernel/random/boot_id >/www/identity
awk '{print $22}' /proc/1/stat >>/www/identity
cat /proc/sys/kernel/random/uuid >>/www/identity
exec httpd -f -p 8080 -h /www"""
    template = pod("native-activation", script)
    template["spec"]["restartPolicy"] = "Always"
    template["spec"]["containers"][0]["ports"] = [{"containerPort": 8080}]
    annotations = {"microsandbox.dev/on-demand": "true", "microsandbox.dev/availability": "proxy",
                   "microsandbox.dev/idle-owner": "external", "microsandbox.dev/proxy-protocol": "http"}
    apply({"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "native-activation"},
           "spec": {"replicas": 2, "selector": {"matchLabels": {"app": "native-activation"}},
                    "template": {"metadata": {"labels": {"app": "native-activation"}, "annotations": annotations},
                                 "spec": template["spec"]}}})
    wait("two cold proxies Ready", lambda: resource("deployment", "native-activation").get("status", {}).get("availableReplicas") == 2)
    deployment = wait("two cold replica identities", lambda: control_get("deployments/default/native-activation"))
    assert len(deployment["pods"]) == 2 and all(p["state"] == "COLD" for p in deployment["pods"]), deployment
    health = control_get("health")
    assert health["maxActiveMemoryMiB"] == 768 and health["activeMemoryMiB"] == 0, health
    env = dict(os.environ, JAVA_HOME=args.java_home or os.environ.get("JAVA_HOME", ""),
               FLOCI_NATIVE_POD_TEST="true", FLOCI_NATIVE_POD_CONTROLLER_URL=f"http://127.0.0.1:{control_port}",
               FLOCI_NATIVE_POD_CONTROLLER_TOKEN=token, FLOCI_NATIVE_POD_NAMESPACE="default",
               FLOCI_NATIVE_POD_DEPLOYMENT="native-activation", FLOCI_NATIVE_POD_HTTP_PATH="/identity",
               FLOCI_NATIVE_POD_EXPECT_COLD="true", FLOCI_NATIVE_POD_EXPECT_REPLICAS="2", FLOCI_NATIVE_POD_QUEUE_WAKE="true")
    # Inherited fixture settings must not pre-wake an arbitrary replica or select another endpoint.
    env.pop("FLOCI_NATIVE_POD_PROXY_URL", None)
    env.pop("FLOCI_NATIVE_POD_BACKEND_URL", None)
    if args.maven_ready_file:
        wait("Maven slot released", lambda: Path(args.maven_ready_file).is_file(), 1800)
    completed = subprocess.run([str(Path(args.floci_checkout) / "mvnw"), "-Dtest=NativePodWorkloadIntegrationTest", "test"],
                               cwd=args.floci_checkout, env=env, stdout=log, stderr=log, timeout=900)
    if completed.returncode:
        current = control_get("deployments/default/native-activation")
        raise RuntimeError("Java selective activation qualification failed: " + json.dumps(current))
    current = control_get("deployments/default/native-activation")
    assert sum(p["state"] == "READY" for p in current["pods"]) == 1, current
    assert sum(p["state"] == "COLD" for p in current["pods"]) == 1, current
    assert control_get("health")["activeMemoryMiB"] == 768
    report["checks"]["java_http_queue_wake_one_of_two_replicas_with_single_guest_budget"] = True
    report["checks"]["gateway_health_bypasses_pod_activity_and_allows_sleep"] = True
    report["checks"]["selected_replica_retains_memory_across_two_gateway_wakes"] = True
    report["checks"]["unselected_replica_remains_cold_with_unchanged_identity"] = True
