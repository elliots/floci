"""The same real AWS SDK credential-provider checks on Docker and native Kubernetes Pods."""
import base64
import json
import os
from pathlib import Path
import platform
import secrets
import signal
import subprocess
import time
import urllib.request


def apply_patch(value, operations):
    for operation in operations:
        if operation["op"] != "add":
            raise AssertionError("unexpected admission operation")
        parts = [part.replace("~1", "/").replace("~0", "~") for part in operation["path"].split("/")[1:]]
        parent = value
        for part in parts[:-1]:
            parent = parent[int(part)] if isinstance(parent, list) else parent[part]
        key = parts[-1]
        if isinstance(parent, list):
            parent.insert(len(parent) if key == "-" else int(key), operation["value"])
        else:
            parent[key] = operation["value"]
    return value


def qualify_aws_identity(*, args, peer, host_ip, temporary, pod, apply, resource, kubectl,
                         lifecycle, control_get, crash_and_restart, wait, command, report, log, failure):
    if not args.floci_checkout:
        raise RuntimeError("AWS identity qualification requires --floci-checkout")
    if not args.network_relay_binary:
        raise RuntimeError("AWS identity qualification requires --network-relay-binary")
    directory = temporary / "aws-identity"
    directory.mkdir(mode=0o700)
    binary = directory / "probe"
    architecture = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64"}[platform.machine()]
    command("go", "build", "-o", str(binary), ".", cwd=Path(__file__).parent/"qualification"/"awsidentity",
        env=dict(os.environ, GOOS="linux", GOARCH=architecture, CGO_ENABLED="0"), timeout=300)
    issuer = json.loads(kubectl("get", "--raw=/.well-known/openid-configuration"))["issuer"]
    keys = json.loads(kubectl("get", "--raw=/openid/v1/jwks"))["keys"]
    assert len(keys) == 1, "qualification requires one unambiguous disposable cluster signing key"
    cluster = "sdk-"+temporary.name.removeprefix("floci-vk-")
    (directory/"fixture.json").write_text(json.dumps({"cluster": cluster, "peer": peer,
        "hostIP": host_ip, "issuer": issuer, "jwk": keys[0]}))
    environment = dict(os.environ, JAVA_HOME=args.java_home or os.environ.get("JAVA_HOME", ""),
        FLOCI_KUBERNETES_AWS_IDENTITY_TEST="true", FLOCI_KUBERNETES_AWS_IDENTITY_DIRECTORY=str(directory))
    installed = getattr(args, "installed_webhook", False)
    if installed:
        environment["FLOCI_KUBERNETES_INSTALLED_WEBHOOK_TEST"] = "true"
    process = subprocess.Popen([str(Path(args.floci_checkout)/"mvnw"), "-o",
        "-Dtest=KubernetesAwsIdentityIntegrationTest", "test"], cwd=args.floci_checkout,
        env=environment, stdout=log, stderr=log, start_new_session=True)
    result = {"success": False, "providers": {}}
    created = []
    try:
        def fixture_ready():
            if process.poll() is not None:
                raise failure("AWS fixture JVM exited before readiness; inspect qualification log")
            path = directory/"ready.json"
            return json.loads(path.read_text()) if path.is_file() else None
        settings = wait("AWS fixture ready", fixture_ready, 600)
        if installed:
            assert settings.get("webhookManifest"), "installed fixture did not publish its real TLS webhook"
            kubectl("apply", "-f", "-", input_text=settings["webhookManifest"])
            wait("TLS webhook installed", lambda: resource("mutatingwebhookconfiguration", "floci-eks-pod-identity"))
        destination = settings["endpoint"].removeprefix("http://")
        # One node listener forwards the unmodified SDK request, as the existing
        # EKS node relay does. It has no credential or AWS logic of its own.
        relay = "/tmp/floci-aws-identity"
        relay_token = directory/"relay-token"
        relay_token.write_text(secrets.token_hex(32))
        relay_token.chmod(0o600)
        command("docker", "exec", peer, "mkdir", "-m", "700", relay)
        command("docker", "cp", args.network_relay_binary, peer+":"+relay+"/relay")
        command("docker", "cp", str(relay_token), peer+":"+relay+"/token")
        command("docker", "exec", peer, "ip", "addr", "add", "169.254.170.23/32", "dev", "lo")
        command("docker", "exec", "-d", peer, relay+"/relay", "serve", "--owner", "aws-identity",
            "--socket", relay+"/control.sock", "--token-file", relay+"/token",
            "--listen-cidr", "169.254.170.23/32", "--target-host", host_ip)
        def identity_relay():
            return command("docker", "exec", "-i", peer, relay+"/relay", "client",
                "--socket", relay+"/control.sock", "--token-file", relay+"/token", "--method", "PUT",
                "--path", "/listeners/identity", input_text=json.dumps({
                    "listenAddress": "169.254.170.23:80", "targetAddress": destination}))
        wait("node identity relay ready", identity_relay, 30)
        command("docker", "exec", peer, "iptables", "-t", "nat", "-I", "PREROUTING", "1",
            "-s", "10.42.0.0/16", "-d", "169.254.170.23", "-p", "tcp", "--dport", "80",
            "-j", "DNAT", "--to-destination", "169.254.170.23:80")
        # Expose Floci through an ordinary cluster Service. Docker's synthetic
        # host address belongs to the peer network, not the native guest network.
        apply({"apiVersion": "v1", "kind": "Service", "metadata": {"name": "floci-aws"},
            "spec": {"ports": [{"name": "http", "port": 4566, "protocol": "TCP"}]}})
        apply({"apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice",
            "metadata": {"name": "floci-aws", "labels": {"kubernetes.io/service-name": "floci-aws",
                "endpointslice.kubernetes.io/managed-by": "floci-qualification"}}, "addressType": "IPv4",
            "ports": [{"name": "http", "protocol": "TCP", "port": int(destination.rsplit(":", 1)[1])}],
            "endpoints": [{"addresses": [host_ip], "conditions": {"ready": True}}]})
        settings["endpoint"] = "http://floci-aws.default.svc.cluster.local:4566"
        for name in ("aws-irsa", "aws-pod", "aws-unassociated"):
            metadata = {"name": name}
            if name == "aws-irsa":
                metadata["annotations"] = {"eks.amazonaws.com/role-arn": settings["irsaRole"],
                    "eks.amazonaws.com/token-expiration": "600"}
            apply({"apiVersion": "v1", "kind": "ServiceAccount", "metadata": metadata})

        def injected(name, native, kind, forge_projection=False):
            value = pod(name, "until test -x /tmp/aws-identity-probe; do sleep .1; done; exec /tmp/aws-identity-probe", native)
            value["spec"]["serviceAccountName"] = "aws-"+kind
            value["spec"]["automountServiceAccountToken"] = False
            value["metadata"]["annotations"] = {"microsandbox.dev/availability": "proxy", "microsandbox.dev/idle-owner": "external"} if native else {}
            app = value["spec"]["containers"][0]
            app["ports"] = [{"containerPort": 8080}]
            app["env"] = [{"name": key, "value": text} for key, text in {
                "AWS_ENDPOINT_URL_STS": settings["endpoint"], "AWS_ENDPOINT_URL_SSM": settings["endpoint"],
                "AWS_EC2_METADATA_DISABLED": "true", "QUALIFICATION_EXPIRY_WINDOW_SECONDS": "3595",
                "QUALIFICATION_ALLOWED_PARAMETER": settings["allowed"],
                "QUALIFICATION_DENIED_PARAMETER": settings["denied"]}.items()]
            if installed and not forge_projection:
                return value
            review = {"apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview", "request": {
                "uid": name, "operation": "CREATE", "namespace": "default", "object": value}}
            url = settings["localEndpoint"].rstrip("/")+"/_floci/eks/clusters/"+cluster+"/pod-identity-webhook/scope/000000000000"
            request = urllib.request.Request(url, data=json.dumps(review).encode(), headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=15) as response:
                reviewed = json.load(response)["response"]
            assert reviewed["allowed"] and reviewed.get("patch"), reviewed
            value = apply_patch(value, json.loads(base64.b64decode(reviewed["patch"])))
            for volume in value["spec"].get("volumes", []):
                for source in volume.get("projected", {}).get("sources", []):
                    if "serviceAccountToken" in source:
                        source["serviceAccountToken"]["expirationSeconds"] = 600
            return value

        def start(value, native):
            name = value["metadata"]["name"]
            apply(value)
            created.append(name)
            observed = wait("SDK Pod declared", lambda: resource("pod", name))
            if installed and "-denied-" not in name:
                injected_environment = {item["name"] for item in observed["spec"]["containers"][0].get("env", [])}
                expected = "AWS_WEB_IDENTITY_TOKEN_FILE" if "-irsa" in name else "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"
                assert expected in injected_environment, "Kubernetes TLS admission did not inject "+expected
            uid = observed["metadata"]["uid"]
            if native:
                def cold():
                    state = lifecycle(uid)
                    return state if state["state"] == "COLD" else None
                state = wait("SDK Pod cold", cold)
                lifecycle(uid, "resume", state["generation"])
            wait("SDK container running", lambda: any("running" in item.get("state", {})
                for item in resource("pod", name).get("status", {}).get("containerStatuses", [])), 360)
            with binary.open("rb") as source:
                copied = subprocess.run(["docker", "exec", "-i", peer, "kubectl", "exec", "-i", name,
                    "--", "sh", "-ec", "cat >/tmp/aws-identity-probe.new; chmod 700 /tmp/aws-identity-probe.new; mv /tmp/aws-identity-probe.new /tmp/aws-identity-probe"],
                    stdin=source, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
            assert copied.returncode == 0, "SDK probe upload failed"
            return uid

        def observe(name):
            return json.loads(kubectl("exec", name, "--", "wget", "-qO-", "http://127.0.0.1:8080", timeout=90))

        def verify(value, kind):
            expected_source = "WebIdentityCredentials" if kind == "irsa" else "CredentialsEndpointProvider"
            assert value.get("source") == expected_source, value
            assert value.get("allowed") == "authorized" and value.get("denied") in ("AccessDeniedException", "AccessDenied"), value
            role = settings["irsaRole" if kind == "irsa" else "podRole"].split("/")[-1]
            assert ":assumed-role/"+role+"/" in value.get("callerArn", ""), value
            assert value.get("tokenHash") and value.get("credentialHash") and value.get("tokenExpires"), value
            return value

        sleeping = []
        for native in (False, True):
            for kind in ("irsa", "pod"):
                name = ("native" if native else "docker")+"-aws-"+kind
                value = injected(name, native, kind)
                uid = start(value, native)
                initial = wait("AWS SDK authenticates", lambda: verify(observe(name), kind), 120)
                explicit = json.loads(kubectl("exec", name, "--", "env", "AWS_ACCESS_KEY_ID=explicit-fixture",
                    "AWS_SECRET_ACCESS_KEY=explicit-fixture-secret", "/tmp/aws-identity-probe", "--credentials-once"))
                assert explicit["source"] == "EnvConfigCredentials", explicit
                result["providers"][name] = {"callerArn": initial["callerArn"], "source": initial["source"], "explicitPrecedence": True}
                if native:
                    state = lifecycle(uid)
                    lifecycle(uid, "hibernate", state["generation"])
                    sleeping.append((name, uid, kind, initial))
                else:
                    time.sleep(6)
                    renewed = verify(observe(name), kind)
                    assert renewed["credentialHash"] != initial["credentialHash"] and renewed["marker"] == initial["marker"], renewed
                    kubectl("delete", "pod", name, "--wait=true")
                    created.remove(name)
        assert control_get("health")["activeMemoryMiB"] == 0
        crash_and_restart()
        if args.identity_expiry_wait:
            expires = max(initial["tokenExpires"] for _, _, _, initial in sleeping)
            assert expires-time.time() <= 610, "TokenRequest did not honor the requested ten-minute lifetime"
            print("Waiting for projected tokens to expire while both native guests use zero active RAM", flush=True)
            wait("projected tokens expire", lambda: time.time() > expires+2, 620)
        else:
            time.sleep(6)
        for name, uid, kind, initial in sleeping:
            state = lifecycle(uid)
            assert state["state"] == "SLEEPING", state
            with urllib.request.urlopen(state["proxyUrl"], timeout=240) as response:
                restored = verify(json.load(response), kind)
            assert restored["marker"] == initial["marker"], restored
            assert restored["tokenHash"] != initial["tokenHash"] and restored["credentialHash"] != initial["credentialHash"], restored
            result["providers"][name]["restoredIdentity"] = True
            result["providers"][name]["oldTokenExpired"] = time.time() > initial["tokenExpires"]
            kubectl("delete", "pod", name, "--wait=true")
            created.remove(name)
            wait("identity guest cleanup", lambda: control_get("health")["activeMemoryMiB"] == 0)
        for native in (False, True):
            for kind, mismatch in (("irsa", "audience"), ("irsa", "subject"), ("pod", "audience"), ("pod", "subject")):
                name = ("native" if native else "docker")+"-denied-"+kind+"-"+mismatch
                value = injected(name, native, kind, forge_projection=True)
                if mismatch == "subject":
                    value["spec"]["serviceAccountName"] = "aws-unassociated"
                else:
                    for volume in value["spec"]["volumes"]:
                        for source in volume.get("projected", {}).get("sources", []):
                            if "serviceAccountToken" in source:
                                source["serviceAccountToken"]["audience"] = "untrusted.invalid"
                start(value, native)
                rejected = wait("SDK rejects mismatched identity", lambda: observe(name), 90)
                expected = {"AccessDenied", "InvalidIdentityToken"} if kind == "irsa" else {
                    "EndpointHTTP400" if mismatch == "audience" else "EndpointHTTP404"}
                assert rejected.get("error") in expected and "callerArn" not in rejected and "allowed" not in rejected, rejected
                kubectl("delete", "pod", name, "--wait=true")
                created.remove(name)
                wait("denied identity guest cleanup", lambda: control_get("health")["activeMemoryMiB"] == 0)
        result["success"] = True
        if installed:
            report["checks"]["installed_tls_webhook_injects_irsa_and_pod_identity_on_both_workers"] = True
        report["checks"]["shared_aws_sdk_irsa_and_pod_identity_enforce_role_policies"] = True
        report["checks"]["native_first_request_after_resume_uses_refreshed_identity_and_sdk_cache"] = True
        report["checks"]["shared_aws_credentials_reject_wrong_audiences_and_subjects"] = True
        report["checks"]["explicit_aws_credentials_keep_sdk_precedence"] = True
        if args.identity_expiry_wait:
            report["checks"]["expired_pod_tokens_replaced_for_first_native_request"] = True
        report["aws_identity_providers"] = result["providers"]
    except Exception as error:
        result["error"] = str(error)
        raise
    finally:
        pending_result = directory/"result.json.tmp"
        pending_result.write_text(json.dumps(result))
        pending_result.replace(directory/"result.json")
        cleanup_errors = []
        if installed:
            try:
                kubectl("delete", "mutatingwebhookconfiguration", "floci-eks-pod-identity", "--ignore-not-found")
            except Exception as error:
                cleanup_errors.append(str(error))
        for name in created:
            try:
                kubectl("delete", "pod", name, "--ignore-not-found", "--wait=false")
            except Exception as error:
                cleanup_errors.append(str(error))
        try:
            code = process.wait(timeout=120)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass  # The owned process group already exited.
            try:
                code = process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass  # The owned process group exited during the timeout.
                code = process.wait(timeout=30)
        if result["success"] and code != 0:
            raise RuntimeError("AWS fixture JVM assertions failed; inspect qualification log")
        if cleanup_errors:
            raise RuntimeError("AWS identity Pod cleanup failed: " + "; ".join(cleanup_errors))
