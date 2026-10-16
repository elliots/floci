#!/usr/bin/env python3
"""Qualify PostgreSQL memory hibernation without changing Floci's runtime."""

import argparse
import json
import os
from pathlib import Path
import platform
import secrets
import socket
import subprocess
import sys
import tempfile
import time


VERSION = "msb 0.7.8"
IMAGE = "postgres:17.11-alpine"


class Microsandbox:
    def __init__(self, executable, home):
        self.executable = executable
        self.environment = dict(os.environ, MSB_HOME=str(home))
        self.names = []
        self.snapshots = []
        self.secrets = []

    def run(self, *args, timeout=180):
        for arg in args:
            if arg.startswith("POSTGRES_PASSWORD="):
                self.secrets.append(arg.split("=", 1)[1])
        try:
            result = subprocess.run(
                [self.executable, *args], env=self.environment,
                capture_output=True, text=True, timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            # TimeoutExpired includes the entire command, including credentials.
            raise RuntimeError(f"msb {args[0]} timed out after {timeout}s") from None
        if result.returncode:
            detail = result.stderr.strip()
            for secret in self.secrets:
                detail = detail.replace(secret, "[redacted]")
            raise RuntimeError(f"msb {args[0]} failed (exit {result.returncode}): {detail}")
        return result.stdout.strip()

    def inspect(self, name):
        return json.loads(self.run("inspect", "--format", "json", name))

    def hibernate(self, name, snapshot):
        """Never discard the live VM until its full checkpoint verifies."""
        selector = f"{name}:{snapshot}"
        self.run("pause", name)
        try:
            self.run("snap", "create", snapshot, "--sandbox", name, "--full", "--integrity")
            self.snapshots.append(selector)
            self.run("snap", "verify", selector)
        except Exception:
            self.run("resume", name)
            raise
        self.run("stop", "--force", name)
        if self.inspect(name)["status"] != "Stopped":
            raise RuntimeError("Snapshot captured but source VM did not stop")
        return selector

    def restore(self, selector, name, port):
        self.names.append(name)
        self.run("snap", "restore", selector, "--name", name,
                 "-p", f"127.0.0.1:{port}:5432")

    def cleanup(self):
        errors = []
        try:
            existing = {item["name"] for item in json.loads(self.run("ls", "--format", "json"))}
        except Exception as error:
            return [f"Cannot enumerate owned sandboxes: {error}"]
        for name in reversed(self.names):
            if name not in existing:
                continue
            try:
                if self.inspect(name)["status"] != "Stopped":
                    self.run("stop", "--force", name)
                self.run("rm", name)
            except Exception as error:
                errors.append(f"{name}: {error}")
        for selector in reversed(self.snapshots):
            try:
                # Restores leave indexed descendants even after their VMs are removed.
                self.run("snap", "rm", "--force", selector)
            except Exception as error:
                errors.append(f"{selector}: {error}")
        return errors


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def assert_database(connection, expected_start, expected_rows, marker):
    start, rows, memory = connection.execute(
        "SELECT pg_postmaster_start_time()::text, "
        "(SELECT count(*) FROM floci_probe), "
        "pg_read_file('/dev/shm/floci-marker')"
    ).fetchone()
    if (start, rows, memory) != (expected_start, expected_rows, marker):
        raise RuntimeError("Restore lost process identity, committed data, or tmpfs contents")


def qualify(runtime, image, cycles):
    import psycopg

    if runtime.run("--version") != VERSION:
        raise RuntimeError(f"This probe requires {VERSION}")
    runtime.run("doctor")
    name = "floci-probe-0"
    port = free_port()
    password = secrets.token_urlsafe(24)
    marker = secrets.token_hex(16)

    def connect():
        return psycopg.connect(host="127.0.0.1", port=port, user="postgres",
                               password=password, dbname="probe", connect_timeout=3,
                               autocommit=True, options="-c statement_timeout=10000")

    def wait_ready():
        deadline = time.monotonic() + 90
        while True:
            try:
                with connect() as connection:
                    connection.execute("SELECT 1")
                return
            except psycopg.OperationalError:
                if time.monotonic() >= deadline:
                    raise RuntimeError("PostgreSQL did not become reachable through the host TCP port") from None
                time.sleep(0.25)

    runtime.names.append(name)
    print("Starting PostgreSQL microVM...", file=sys.stderr, flush=True)
    runtime.run("run", "-d", "--name", name, "--cpus", "1", "--memory", "512M",
                "--root-disk", "2G", "-e", f"POSTGRES_PASSWORD={password}",
                "-e", "POSTGRES_DB=probe", "-p", f"127.0.0.1:{port}:5432", image)
    wait_ready()
    runtime.run("exec", "--no-tty", name, "--", "sh", "-c",
                f"printf %s {marker} > /dev/shm/floci-marker")
    with connect() as connection:
        start = connection.execute("SELECT pg_postmaster_start_time()::text").fetchone()[0]
        connection.execute("CREATE TABLE floci_probe (id integer PRIMARY KEY)")
        connection.execute("INSERT INTO floci_probe VALUES (0)")
    report = {
        "runtime": VERSION, "host": platform.platform(), "architecture": platform.machine(),
        "image": image, "postmaster_started_at": start, "cycles": [],
    }
    for cycle in range(1, cycles + 1):
        print(f"Hibernation cycle {cycle}/{cycles}...", file=sys.stderr, flush=True)
        before = json.loads(runtime.run("metrics", "--format", "json", name))
        then = time.monotonic()
        selector = runtime.hibernate(name, f"cycle-{cycle}")
        sleep_seconds = time.monotonic() - then
        # No DB connections are held across sleep. Test actual published-port closure.
        with socket.socket() as client:
            client.settimeout(1)
            if client.connect_ex(("127.0.0.1", port)) == 0:
                raise RuntimeError("Host database port is still listening after VM termination")
        name = f"floci-probe-{cycle}"
        then = time.monotonic()
        runtime.restore(selector, name, port)
        wait_ready()
        restore_seconds = time.monotonic() - then
        with connect() as connection:
            assert_database(connection, start, cycle, marker)
            connection.execute("INSERT INTO floci_probe VALUES (%s)", (cycle,))
        # Later cycles snapshot the current generation, including its new writes.
        report["cycles"].append({
            "cycle": cycle, "checkpoint_verify_stop_seconds": round(sleep_seconds, 3),
            "restore_until_sql_ready_seconds": round(restore_seconds, 3),
            "source_status": "Stopped", "source_port_closed": True,
            "host_resident_bytes_before": before["memory_host_resident_bytes"],
            "preserved_postmaster": True, "preserved_tmpfs": True,
            "rows_before_new_write": cycle,
        })
    # Verify the final committed write through another independent TCP connection.
    with connect() as connection:
        assert_database(connection, start, cycles + 1, marker)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--msb", default="msb", help="Path to microsandbox 0.7.8")
    parser.add_argument("--image", default=IMAGE, help="PostgreSQL image, optionally pinned by digest")
    parser.add_argument("--cycles", type=int, default=2)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.cycles < 2:
        parser.error("at least two cycles are required to check writes after a restore")
    # Never operate on the caller's ordinary microsandbox state or existing workloads.
    # macOS's default temporary directory exceeds the runtime's Unix socket path limit.
    home = Path(tempfile.mkdtemp(prefix="floci-msb-", dir="/tmp"))
    print(f"Isolated MSB_HOME: {home}", file=sys.stderr, flush=True)
    runtime = Microsandbox(args.msb, home)
    try:
        report = qualify(runtime, args.image, args.cycles)
    finally:
        errors = runtime.cleanup()
        if errors:
            print("Cleanup incomplete; retained state at " + str(home), file=sys.stderr)
            for error in errors:
                print(error, file=sys.stderr)
    if errors:
        raise RuntimeError("Runtime qualification passed but resource cleanup failed")
    report["cleanup"] = "passed"
    report["state_directory"] = str(home)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Passed; report: {args.report}. Image cache retained at {home}")


if __name__ == "__main__":
    main()
