"""Disposable, authenticated TLS registry used only by native qualification."""
import base64
import hmac
import http.server
import json
from pathlib import Path
import secrets
import ssl
import threading
import time
import urllib.error
import urllib.request


class QualificationRegistry:
    def __init__(self, command, directory, owner):
        self.command = command
        self.directory = Path(directory) / "registry"
        self.directory.mkdir(mode=0o700)
        self.owner = owner
        self.name = "floci-registry-probe-" + owner
        self.username = "qualification"
        self.password = secrets.token_hex(24)
        self.creation_attempted = False
        self.tag = None
        self.server = None
        self.thread = None
        self.authenticated_requests = 0

    def start(self):
        self.creation_attempted = True
        self.command("docker", "run", "-d", "--name", self.name,
                     "--label", "io.floci.pod-probe=" + self.owner,
                     "-p", "127.0.0.1::5000", "registry:2", timeout=180)
        inspected = json.loads(self.command("docker", "inspect", self.name))[0]
        port = inspected["NetworkSettings"]["Ports"]["5000/tcp"][0]["HostPort"]
        self.upstream = "http://127.0.0.1:" + port
        for attempt in range(60):
            try:
                with urllib.request.urlopen(self.upstream + "/v2/", timeout=2):
                    break
            except (OSError, urllib.error.URLError):
                if attempt == 59:
                    raise RuntimeError("qualification registry did not start") from None
                time.sleep(1)
        source = "public.ecr.aws/docker/library/busybox:1.37.0"
        try:
            self.command("docker", "image", "inspect", source)
        except RuntimeError:
            self.command("docker", "pull", source, timeout=180)
        self.tag = "127.0.0.1:" + port + "/fixture:probe"
        self.command("docker", "tag", source, self.tag)
        self.command("docker", "push", self.tag, timeout=180)
        self.certificate = self.directory / "certificate.pem"
        key = self.directory / "key.pem"
        self.command("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                     "-keyout", str(key), "-out", str(self.certificate), "-days", "1",
                     "-subj", "/CN=host.microsandbox.internal",
                     "-addext", "subjectAltName=DNS:host.microsandbox.internal",
                     "-addext", "basicConstraints=critical,CA:TRUE")
        key.chmod(0o600)
        fixture = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass  # Request headers can include fixture credentials.

            def do_GET(self):
                self.serve()

            def do_HEAD(self):
                self.serve()

            def serve(self):
                expected = "Basic " + base64.b64encode(
                    (fixture.username + ":" + fixture.password).encode()).decode()
                if not hmac.compare_digest(self.headers.get("Authorization", ""), expected):
                    self.send_response(401)
                    self.send_header("WWW-Authenticate", 'Basic realm="native-qualification"')
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                fixture.authenticated_requests += 1
                headers = {name: self.headers[name] for name in ("Accept", "Range") if name in self.headers}
                request = urllib.request.Request(fixture.upstream + self.path, headers=headers, method=self.command)
                try:
                    response = urllib.request.urlopen(request, timeout=30)
                except urllib.error.HTTPError as error:
                    response = error
                with response:
                    self.send_response(response.status)
                    for name, value in response.headers.items():
                        if name.lower() not in ("connection", "transfer-encoding", "server", "date"):
                            self.send_header(name, value)
                    self.end_headers()
                    if self.command != "HEAD":
                        while chunk := response.read(65536):
                            self.wfile.write(chunk)

        self.server = http.server.ThreadingHTTPServer(("0.0.0.0", 0), Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(self.certificate), str(key))
        self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.host = "host.microsandbox.internal:" + str(self.server.server_port)
        self.image = self.host + "/fixture:probe"
        return self

    def secret(self):
        auth = base64.b64encode((self.username + ":" + self.password).encode()).decode()
        config = json.dumps({"auths": {self.host: {"auth": auth}}})
        return {"apiVersion":"v1", "kind":"Secret", "metadata":{"name":"native-registry"},
                "type":"kubernetes.io/dockerconfigjson", "stringData":{".dockerconfigjson": config}}

    def close(self):
        if self.server:
            if self.thread and self.thread.is_alive():
                self.server.shutdown()
            self.server.server_close()
            if self.thread and self.thread.ident is not None:
                self.thread.join(timeout=5)
            self.server = None
        if self.creation_attempted:
            existing = self.command("docker", "ps", "-aq", "--filter", "name=^/" + self.name + "$")
            if existing:
                inspected = json.loads(self.command("docker", "inspect", self.name))[0]
                if inspected["Config"]["Labels"].get("io.floci.pod-probe") != self.owner:
                    raise RuntimeError("qualification registry ownership changed")
                self.command("docker", "rm", "-fv", self.name)
            self.creation_attempted = False
        if self.tag:
            self.command("docker", "image", "rm", self.tag)
            self.tag = None
