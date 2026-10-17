"""Portable, private ownership of one controller child and its credential rotation."""
import collections
import contextlib
import hmac
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import stat
import subprocess
import sys
import time
import urllib.request

import manage

SOCKET = "manager.sock"
TOKEN = "manager-token"


def request(directory, operation, timeout=120):
    record = manage.load(directory)
    directory = Path(record["directory"])
    token = manage.read_private(directory/TOKEN).strip()
    message = {"owner": record["owner"], "token": token, "operation": operation}
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(timeout)
        connection.connect(str(directory/SOCKET))
        connection.sendall(json.dumps(message).encode()+b"\n")
        with connection.makefile("rb") as stream:
            response = json.loads(stream.readline(16385))
    if response.get("owner") != record["owner"]:
        raise RuntimeError("manager response belongs to another cluster owner")
    if "error" in response:
        raise RuntimeError(response["error"])
    return response


def running(directory):
    try:
        return request(directory, "status", timeout=2)
    except (FileNotFoundError, ConnectionRefusedError):
        return None


def controller_ready(record):
    token = manage.read_private(Path(record["directory"])/"control-token").strip()
    request = urllib.request.Request("http://127.0.0.1:"+str(record["controlPort"])+"/v1/health",
        headers={"Authorization": "Bearer "+token})
    try:
        with urllib.request.urlopen(request, timeout=2) as response:
            return isinstance(json.load(response), dict)
    except OSError:
        return False


def private_log(path):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
    info = os.fstat(descriptor)
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid():
        os.close(descriptor)
        raise RuntimeError("controller log is not a private owned file")
    return os.fdopen(descriptor, "a", buffering=1)


def rotate_log(log, path, limit=10 << 20):
    if os.fstat(log.fileno()).st_size <= limit:
        return
    path = Path(path)
    if path.is_symlink() or path.stat().st_ino != os.fstat(log.fileno()).st_ino:
        raise RuntimeError("active log path ownership changed")
    for backup in (path.with_name(path.name+".1"), path.with_name(path.name+".2")):
        if backup.exists() or backup.is_symlink():
            info = backup.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid():
                raise RuntimeError("log backup is not a private owned file")
    prior = path.with_name(path.name+".1")
    if prior.exists():
        os.replace(prior, path.with_name(path.name+".2"))
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as source:
        if os.fstat(source.fileno()).st_ino != os.fstat(log.fileno()).st_ino:
            raise RuntimeError("active log path ownership changed")
        source.seek(max(0, os.fstat(source.fileno()).st_size-limit))
        recent = source.read(limit).decode(errors="ignore")
    manage.write_private(prior, recent)
    # Keep the child's O_APPEND descriptor valid. Concurrent log lines at the
    # copy/truncate boundary can be lost; workload and recovery files are separate.
    log.flush()
    os.ftruncate(log.fileno(), 0)


class Supervisor:
    def __init__(self, record, args, log):
        self.record, self.args, self.log = record, args, log
        self.child = None
        self.stopping = False
        self.generation = secrets.token_hex(16)
        self.restarts = collections.deque()
        self.next_certificate_check = 0
        self.last_error = None

    def note(self, text):
        print(time.strftime("%Y-%m-%dT%H:%M:%S%z"), text, file=self.log, flush=True)

    def launch(self):
        if manage.controller_running(self.record):
            raise RuntimeError("a controller already occupies the recorded port; refusing to adopt or signal an unknown process")
        peer = manage.inspect_peer(self.record)
        if peer is None or not peer["State"]["Running"]:
            raise RuntimeError("owned network peer is unavailable; preserve state and run prepare after stopping this manager")
        self.child = subprocess.Popen(manage.controller_arguments(self.record, self.args),
            stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log, start_new_session=True)
        self.note("started owned controller child "+str(self.child.pid))

    def stop_child(self):
        if self.child is not None and self.child.poll() is None:
            self.child.terminate()
            try:
                self.child.wait(timeout=45)
            except subprocess.TimeoutExpired:
                self.note("owned controller did not stop in 45s; killing this child and retaining native recovery fences")
                self.child.kill()
                self.child.wait(timeout=15)

    def renew(self):
        # Preparation never changes the active record or signals the healthy child.
        if manage.load(self.record["directory"]) != self.record:
            raise RuntimeError("cluster record changed while this supervisor owned it")
        replacement = manage.prepare_credentials(self.record)
        self.stop_child()
        manage.write_private(Path(self.record["directory"])/"cluster.json", json.dumps(replacement, indent=2)+"\n")
        self.record = replacement
        self.launch()
        manage.remove_old_credentials(self.record)
        self.last_error = None
        self.note("activated credential generation "+replacement["credentialGeneration"])

    def tick(self):
        now = time.monotonic()
        if hasattr(self.log, "name"):
            rotate_log(self.log, Path(self.record["directory"])/"controller.log")
        if self.child is not None and self.child.poll() is not None:
            while self.restarts and self.restarts[0] < now-300:
                self.restarts.popleft()
            if len(self.restarts) >= 3:
                raise RuntimeError("controller exited repeatedly; retained all ownership and pending-operation records")
            self.restarts.append(now)
            self.note("controller exited; retrying journal recovery")
            self.launch()
        if now >= self.next_certificate_check:
            self.next_certificate_check = now+3600
            try:
                if manage.credentials_due(self.record):
                    self.renew()
            except (RuntimeError, OSError, ValueError, subprocess.TimeoutExpired) as error:
                self.last_error = str(error)
                self.next_certificate_check = now+60
                self.note("credential renewal failed; preserving last selected credentials: "+str(error))

    def status(self):
        return {"owner": self.record["owner"], "generation": self.generation,
            "controllerPID": self.child.pid if self.child is not None else None,
            "controllerReady": self.child is not None and self.child.poll() is None and controller_ready(self.record),
            "credentialGeneration": self.record.get("credentialGeneration"), "lastError": self.last_error}

    def dispatch(self, value, token):
        if not isinstance(value, dict) or value.get("owner") != self.record["owner"] or not isinstance(value.get("token"), str) or not hmac.compare_digest(value["token"], token):
            raise RuntimeError("manager ownership authentication failed")
        operation = value.get("operation")
        if operation == "stop":
            self.stopping = True
        elif operation == "renew":
            self.renew()
            self.next_certificate_check = time.monotonic()+3600
        elif operation != "status":
            raise RuntimeError("unknown manager operation")
        return self.status()


def serve(args):
    directory = Path(args.directory).resolve()
    with manage.setup_lock(directory), private_log(directory/"controller.log") as log:
        record = manage.load(directory)
        if manage.controller_running(record):
            raise RuntimeError("a controller is already running without this supervisor; refusing process adoption")
        path = directory/SOCKET
        try:
            info = path.lstat()
        except FileNotFoundError:
            info = None
        if info is not None:
            if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid():
                raise RuntimeError("manager socket path is not an owned socket")
            path.unlink()  # The cluster flock proves no other conforming manager owns it.
        token = secrets.token_hex(32)
        manage.write_private(directory/TOKEN, token+"\n")
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind(str(path))
            path.chmod(0o600)
            identity = path.stat().st_ino
            listener.listen(8)
            listener.settimeout(1)
            supervisor = Supervisor(record, args, log)
            previous = {}
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous[signum] = signal.signal(signum, lambda *_: setattr(supervisor, "stopping", True))
            try:
                if manage.credentials_due(record):
                    replacement = manage.prepare_credentials(record)
                    manage.write_private(directory/"cluster.json", json.dumps(replacement, indent=2)+"\n")
                    supervisor.record = replacement
                supervisor.launch()
                manage.remove_old_credentials(supervisor.record)
                while not supervisor.stopping:
                    supervisor.tick()
                    try:
                        connection, _ = listener.accept()
                    except TimeoutError:
                        continue
                    with connection:
                        connection.settimeout(5)
                        try:
                            with connection.makefile("rb") as stream:
                                line = stream.readline(16385)
                            if len(line) > 16384 or not line.endswith(b"\n"):
                                raise RuntimeError("invalid manager request size")
                            result = supervisor.dispatch(json.loads(line), token)
                        except (RuntimeError, OSError, ValueError, TypeError, subprocess.TimeoutExpired) as error:
                            result = {"owner": record["owner"], "error": str(error)}
                        with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                            connection.sendall(json.dumps(result).encode()+b"\n")
            finally:
                supervisor.stop_child()
                for signum, handler in previous.items():
                    signal.signal(signum, handler)
                if path.exists() and path.lstat().st_ino == identity:
                    path.unlink()


def options(args):
    return ["--memory-mib", str(args.memory_mib), "--scheduling-cpu", args.scheduling_cpu,
        "--scheduling-memory", args.scheduling_memory, "--pod-workers", str(args.pod_workers)]


def start(args):
    current = running(args.directory)
    if current:
        print(json.dumps(current, indent=2))
        return
    directory = Path(args.directory).resolve()
    with private_log(directory/"manager.log") as log:
        rotate_log(log, directory/"manager.log")
        child = subprocess.Popen([sys.executable, str(Path(manage.__file__).resolve()), "_supervise",
            "--directory", str(directory), *options(args)], stdin=subprocess.DEVNULL,
            stdout=log, stderr=log, start_new_session=True)
    def ready():
        if child.poll() is not None:
            raise RuntimeError("manager exited; inspect "+str(directory/"manager.log"))
        state = running(directory)
        return state if state and state["controllerReady"] else None
    deadline = time.monotonic()+300
    while time.monotonic() < deadline:
        value = ready()
        if value:
            print(json.dumps(value, indent=2))
            return
        time.sleep(1)
    raise RuntimeError("manager is still starting; inspect status and controller.log (no resources were deleted)")


def stop(args):
    current = running(args.directory)
    if current is None:
        print("No reachable managed controller; no process was signalled.")
        return
    request(args.directory, "stop")
    manage.wait("manager shutdown", lambda: running(args.directory) is None, timeout=90)
    print("Stopped owned controller; retained workloads and cluster state.")


def renew(args):
    print(json.dumps(request(args.directory, "renew"), indent=2))
