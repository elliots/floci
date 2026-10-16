"""Real kubelet exec, localhost forwarding and gRPC health checks."""
import fcntl
import os
from pathlib import Path
import platform
import pty
import select
import signal
import struct
import subprocess
import termios
import time


def terminal_exec(peer, name):
    master, slave = pty.openpty()
    process = None
    output = b""
    try:
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 30, 90, 0, 0))
        process = subprocess.Popen(["docker", "exec", "-it", peer, "kubectl", "exec", "-it", name, "--",
            "sh", "-ec", "test -t 0; test -t 1; echo terminal-ready; read first; stty size; "
            "echo resize-ready; read second; stty size; printf 'received:%s:%s\\n' \"$first\" \"$second\"; exit 23"],
            stdin=slave, stdout=slave, stderr=slave, close_fds=True)
        os.close(slave)
        slave = None

        def receive(marker):
            nonlocal output
            deadline = time.monotonic() + 30
            while marker not in output:
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"TTY output did not reach {marker!r}: {output[-3000:]!r}")
                readable, _, _ = select.select([master], [], [], .2)
                if readable:
                    try:
                        data = os.read(master, 65536)
                    except OSError:
                        data = b""
                    if not data:
                        raise RuntimeError(f"TTY closed early: {output[-3000:]!r}")
                    output += data

        receive(b"terminal-ready")
        os.write(master, b"literal-first\n")
        receive(b"30 90")
        receive(b"resize-ready")
        fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack("HHHH", 44, 132, 0, 0))
        process.send_signal(signal.SIGWINCH)
        # Window updates travel through Docker, kubectl, the kubelet and the SDK.
        time.sleep(1)
        os.write(master, b"literal-second\n")
        receive(b"44 132")
        receive(b"received:literal-first:literal-second")
        assert process.wait(timeout=15) == 23, output
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if slave is not None:
            os.close(slave)
        os.close(master)


def qualify_operations(*, peer, temporary, pod, apply, resource, kubectl, ready,
                       lifecycle, control_get, wait, command, report):
    name = "native-operations"
    binary = temporary / "qualification-health"
    architecture = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64"}[platform.machine()]
    command("go", "build", "-o", str(binary), "./qualification/healthserver",
        cwd=Path(__file__).parent,
        env=dict(os.environ, GOOS="linux", GOARCH=architecture, CGO_ENABLED="0"), timeout=300)
    value = pod(name, "until test -x /tmp/qualification-health; do sleep .1; done; exec /tmp/qualification-health")
    value["spec"]["containers"][0]["readinessProbe"] = {
        "grpc": {"port": 9090, "service": "ready"}, "periodSeconds": 1, "failureThreshold": 1}
    apply(value)
    wait("operations container running", lambda: any("running" in item.get("state", {})
        for item in resource("pod", name).get("status", {}).get("containerStatuses", [])), 360)
    with binary.open("rb") as source:
        result = subprocess.run(["docker", "exec", "-i", peer, "kubectl", "exec", "-i", name, "--", "sh", "-ec",
            "cat >/tmp/qualification-health.pending; chmod 700 /tmp/qualification-health.pending; "
            "mv /tmp/qualification-health.pending /tmp/qualification-health"], stdin=source,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
    assert result.returncode == 0, result.stderr[-3000:]
    wait("loopback-only application starts", lambda: "loopback-only-application" in kubectl("exec", name, "--", "wget", "-qO-", "http://127.0.0.1:8123/"))
    assert not ready(name), "NOT_SERVING gRPC application became Ready"
    kubectl("exec", name, "--", "touch", "/tmp/grpc-ready")
    observed = wait("gRPC service becomes Ready", lambda: ready(name))
    uid = observed["metadata"]["uid"]
    kubectl("exec", name, "--", "rm", "/tmp/grpc-ready")
    wait("gRPC NOT_SERVING withdraws readiness", lambda: not ready(name))
    kubectl("exec", name, "--", "touch", "/tmp/grpc-ready")
    wait("gRPC serving restores readiness", lambda: ready(name))
    report["checks"]["grpc_probe_uses_actual_named_service"] = True
    terminal_exec(peer, name)
    report["checks"]["kubectl_tty_stdin_resize_and_exit_status"] = True

    for cycle in range(2):
        if cycle:
            state = lifecycle(uid)
            lifecycle(uid, "hibernate", state["generation"])
            assert control_get("health")["activeMemoryMiB"] == 0
        forward = subprocess.Popen(["docker", "exec", "-i", peer, "kubectl", "port-forward",
            "--address=127.0.0.1", name, "18123:8123"], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        try:
            response = wait("port-forward reaches Pod loopback", lambda: command("docker", "exec", peer,
                "wget", "-qO-", "http://127.0.0.1:18123/"), 180)
            assert response == "loopback-only-application", response
            assert lifecycle(uid)["state"] in ("RUNNING", "READY")
        finally:
            # Terminate the owned kubectl process inside the peer, not just the
            # Docker client: Docker exec does not forward arbitrary termination.
            command("docker", "exec", peer, "sh", "-ec",
                "for pid in /proc/[0-9]*; do test -r \"$pid/cmdline\" || continue; "
                "case \"$(tr '\\0' ' ' <\"$pid/cmdline\")\" in "
                "'kubectl port-forward --address=127.0.0.1 native-operations 18123:8123 '*) "
                "kill \"${pid##*/}\" 2>/dev/null || true;; esac; done", timeout=20)
            try:
                forward.wait(timeout=10)
            except subprocess.TimeoutExpired:
                forward.kill()
                forward.wait(timeout=5)
            forward.stderr.close()
    report["checks"]["port_forward_reaches_localhost_and_wakes_snapshot"] = True
    wait("forwarding connections release their RAM pin", lambda: lifecycle(uid)["activeConnections"] == 0)
    kubectl("delete", "pod", name, "--wait=true", timeout=180)
    wait("operations fixture releases RAM", lambda: control_get("health")["activeMemoryMiB"] == 0)
