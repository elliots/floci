"""Test application and one-shot standard-library network probes for isolation qualification."""
import base64
import http.server
import json
import os
import secrets
import socket
import socketserver
import ssl
import struct
import sys
import threading
import urllib.request
import uuid

MARKER = str(uuid.uuid4())
RECEIVED = []
LOCK = threading.Lock()


def record(value):
    with LOCK:
        RECEIVED.append(value)


class HTTP(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def do_GET(self):
        self.do_POST()

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        record("http:" + self.path)
        with LOCK:
            received = list(RECEIVED)
        value = {"marker": MARKER, "pid": os.getpid(), "host": self.headers.get("Host"),
            "originalHost": self.headers.get("X-Forwarded-Host"), "forwardedFor": self.headers.get("X-Forwarded-For"),
            "path": self.path, "body": base64.b64encode(body).decode(), "received": received}
        encoded = json.dumps(value).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def dns_response(data):
    record("dns")
    if len(data) < 12:
        return b""
    return data[:2] + b"\x81\x80\x00\x01\x00\x01\x00\x00\x00\x00" + data[12:] + \
        b"\xc0\x0c\x00\x01\x00\x01\x00\x00\x00\x3c\x00\x04\x7f\x00\x00\x01"


class Echo(socketserver.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(8192)
        record("tcp:" + data.decode(errors="replace"))
        self.request.sendall(data)


class UDP(socketserver.BaseRequestHandler):
    def handle(self):
        data, connection = self.request
        record("udp:" + data.decode(errors="replace"))
        connection.sendto(data, self.client_address)


class DNSUDP(socketserver.BaseRequestHandler):
    def handle(self):
        data, connection = self.request
        connection.sendto(dns_response(data), self.client_address)


class DNSTCP(socketserver.BaseRequestHandler):
    def handle(self):
        length = receive(self.request, 2)
        if len(length) != 2:
            return
        data = receive(self.request, struct.unpack("!H", length)[0])
        reply = dns_response(data)
        self.request.sendall(struct.pack("!H", len(reply)) + reply)


class TCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class UDPServer(socketserver.ThreadingUDPServer):
    allow_reuse_address = True
    daemon_threads = True


class IPv6TCP(TCPServer):
    address_family = socket.AF_INET6

    def server_bind(self):
        self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        super().server_bind()


class IPv6UDP(UDPServer):
    address_family = socket.AF_INET6

    def server_bind(self):
        self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        super().server_bind()


def receive(connection, size):
    chunks = bytearray()
    while len(chunks) < size:
        chunk = connection.recv(size - len(chunks))
        if not chunk:
            raise EOFError("connection closed before response")
        chunks.extend(chunk)
    return bytes(chunks)


def serve(application=False):
    listeners = [http.server.ThreadingHTTPServer(("0.0.0.0", 8080), HTTP)]
    if not application:
        listeners += [TCPServer(("0.0.0.0", port), Echo) for port in (18080, 18082)]
        listeners += [UDPServer(("0.0.0.0", port), UDP) for port in (18081, 18083)]
        for port in (1053, 1054):
            listeners += [UDPServer(("0.0.0.0", port), DNSUDP), TCPServer(("0.0.0.0", port), DNSTCP)]
        listeners += [IPv6TCP(("::", 18080), Echo), IPv6UDP(("::", 18081), UDP)]
    for listener in listeners[1:]:
        threading.Thread(target=listener.serve_forever, daemon=True).start()
    listeners[0].serve_forever()


def probe(value):
    kind, target = value["kind"], value["target"]
    timeout = value.get("timeout", 2)
    token = value.get("token", secrets.token_hex(12)).encode()
    if kind == "http":
        context = ssl.create_default_context(cafile=value.get("ca"))
        request = urllib.request.Request(target, data=base64.b64decode(value["body"]) if "body" in value else None,
            headers=value.get("headers", {}))
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=context))
        with opener.open(request, timeout=timeout) as response:
            return {"status": response.status, "body": response.read().decode()}
    if kind == "resolve":
        return socket.gethostbyname(target)
    host, port = target
    if kind in ("tcp", "dns-tcp"):
        connection = socket.create_connection((host, port), timeout=timeout)
    else:
        connection = socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET, socket.SOCK_DGRAM)
        connection.settimeout(timeout)
        connection.connect((host, port))
    with connection:
        if kind.startswith("dns-"):
            question = b"".join(bytes([len(part)]) + part.encode() for part in "canary.isolation.test".split(".")) + b"\0\0\1\0\1"
            data = b"\x12\x34\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00" + question
            connection.sendall((struct.pack("!H", len(data)) if kind == "dns-tcp" else b"") + data)
            reply = receive(connection, struct.unpack("!H", receive(connection, 2))[0]) if kind == "dns-tcp" else connection.recv(8192)
            assert reply[:2] == b"\x12\x34" and len(reply) >= 12 and reply[3] & 15 == 0, reply.hex()
            return "dns-response"
        connection.sendall(token)
        assert connection.recv(8192) == token
        return token.decode()


def probes():
    results = []
    for value in json.load(sys.stdin):
        result = {"name": value["name"], "success": False}
        try:
            result.update(success=True, value=probe(value))
        except Exception as error:
            result.update(success=False, error=type(error).__name__ + ": " + str(error))
        results.append(result)
    print(json.dumps(results))


if __name__ == "__main__":
    if sys.argv[1] == "probe":
        probes()
    else:
        serve(sys.argv[1] == "app")
