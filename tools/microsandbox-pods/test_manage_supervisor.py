"""Process ownership, crash limits and transactional credential replacement."""
import argparse
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

import manage
import manage_supervisor as supervision


class SupervisionTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="floci-manager-", dir="/tmp")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name).resolve()
        self.record = {"version": 1, "directory": str(self.directory), "owner": "owned",
            "peer": "native-peer", "node": "native-pods", "user": "native-controller",
            "accessName": "native-access", "controlPort": manage.free_port(), "hostIP": "192.0.2.10"}
        manage.write_private(self.directory/"cluster.json", json.dumps(self.record))
        manage.write_private(self.directory/"control-token", "c"*64)
        self.args = argparse.Namespace(directory=self.directory, memory_mib=1536,
            scheduling_cpu="500", scheduling_memory="384Gi", pod_workers=8)
        self.supervisor = supervision.Supervisor(self.record, self.args, io.StringIO())

    def test_private_management_files_and_generation_reject_symlinks(self):
        path = self.directory/"private"
        manage.write_private(path, "owned")
        link = self.directory/"alias"
        link.symlink_to(path)
        with self.assertRaises(OSError):
            manage.read_private(link)
        path.chmod(0o644)
        with self.assertRaisesRegex(RuntimeError, "private management"):
            manage.read_private(path)
        for name in ("../escape", "/etc", "g"*32):
            with self.assertRaisesRegex(RuntimeError, "credential generation"):
                manage.credential_directory(dict(self.record, credentialGeneration=name))
        root = self.directory/"credentials"
        root.mkdir(mode=0o700)
        (root/("a"*32)).symlink_to(self.directory, target_is_directory=True)
        with self.assertRaisesRegex(RuntimeError, "private owned directory"):
            manage.credential_directory(dict(self.record, credentialGeneration="a"*32))

    def test_failed_certificate_preparation_preserves_active_record_and_process(self):
        child = Mock()
        child.poll.return_value = None
        self.supervisor.child = child
        with patch.object(manage, "prepare_credentials", side_effect=RuntimeError("signing unavailable")):
            with self.assertRaisesRegex(RuntimeError, "signing unavailable"):
                self.supervisor.renew()
        child.terminate.assert_not_called()
        child.kill.assert_not_called()
        self.assertEqual(self.record, manage.load(self.directory))

    def test_partial_bundle_never_replaces_active_credentials(self):
        peer = {"State":{"Running":True},"NetworkSettings":{"Ports":{"6443/tcp":[{"HostPort":"12345"}]}}}
        with patch.object(manage, "inspect_peer", return_value=peer), \
                patch.object(manage, "certificate", side_effect=[None, RuntimeError("second certificate failed")]):
            with self.assertRaisesRegex(RuntimeError, "second certificate"):
                manage.prepare_credentials(self.record)
        self.assertEqual(self.record, manage.load(self.directory))
        self.assertEqual([], list((self.directory/"credentials").iterdir()))

    def test_automatic_renewal_publishes_complete_generation_before_restarting(self):
        replacement = dict(self.record, credentialGeneration="a"*32)
        order = []
        def launch():
            order.append("launch")
            self.assertEqual(replacement, manage.load(self.directory))
            self.assertEqual(replacement, self.supervisor.record)
        with patch.object(manage, "credentials_due", return_value=True), \
                patch.object(manage, "prepare_credentials", side_effect=lambda _: order.append("prepare") or replacement), \
                patch.object(self.supervisor, "stop_child", side_effect=lambda: order.append("stop")), \
                patch.object(self.supervisor, "launch", side_effect=launch):
            self.supervisor.tick()
        self.assertEqual(["prepare", "stop", "launch"], order)
        self.assertIsNone(self.supervisor.last_error)

    def test_crashes_have_bounded_retries_without_deleting_recovery_records(self):
        pending = self.directory/"controller"/"native-command.pending"
        manage.write_private(pending, "uncertain")
        self.supervisor.child = Mock()
        self.supervisor.child.poll.return_value = 1
        with patch.object(self.supervisor, "launch") as launch, patch.object(manage, "credentials_due", return_value=False):
            for _ in range(3):
                self.supervisor.tick()
            with self.assertRaisesRegex(RuntimeError, "exited repeatedly"):
                self.supervisor.tick()
            self.assertEqual(3, launch.call_count)
        self.assertEqual("uncertain", pending.read_text())

    def test_invalid_owner_token_and_operation_cannot_stop_child(self):
        for request in ([], {}, {"owner":"other","token":"secret","operation":"stop"},
                {"owner":"owned","token":"wrong","operation":"stop"},
                {"owner":"owned","token":"secret","operation":"arbitrary"}):
            with self.assertRaises(RuntimeError):
                self.supervisor.dispatch(request, "secret")
        self.assertFalse(self.supervisor.stopping)

    def test_manager_controls_only_its_real_child_and_recovers_child_crash(self):
        worker = self.directory/"worker.py"
        worker.write_text('''from http.server import HTTPServer,BaseHTTPRequestHandler
import sys
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"{}")
    def log_message(self,*args):
        pass
HTTPServer(('127.0.0.1',int(sys.argv[1])),Handler).serve_forever()
''')
        driver = self.directory/"driver.py"
        driver.write_text('''import argparse,json,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
import manage,manage_supervisor
directory=sys.argv[2]
record=manage.load(directory)
manage.credentials_due=lambda _:False
manage.inspect_peer=lambda _: {"State":{"Running":True}}
manage.controller_arguments=lambda *_:[sys.executable,"-u",str(Path(directory)/"worker.py"),str(record["controlPort"])]
manage_supervisor.serve(argparse.Namespace(directory=directory))
''')
        with (self.directory/"driver.log").open("w") as log:
            process = subprocess.Popen([sys.executable, str(driver), str(Path(__file__).parent), str(self.directory)],
                stdout=log, stderr=log)
            self.addCleanup(lambda: process.kill() if process.poll() is None else None)
            def ready():
                if process.poll() is not None:
                    self.fail((self.directory/"driver.log").read_text())
                state = supervision.running(self.directory)
                return state if state and state["controllerReady"] else None
            state = manage.wait("supervisor ready", ready, timeout=20)
            with self.assertRaisesRegex(RuntimeError, "in progress"):
                with manage.setup_lock(self.directory):
                    self.fail("supervisor lost exclusive ownership")
            first = state["controllerPID"]
            # PID comes from the live authenticated manager, never a stale disk record.
            os.kill(first, signal.SIGKILL)
            state = manage.wait("child recovered", lambda: (value if (value := ready()) and value["controllerPID"] != first else None), timeout=20)
            self.assertEqual("owned", state["owner"])
            supervision.request(self.directory, "stop")
            self.assertEqual(0, process.wait(timeout=20))
            self.assertIsNone(supervision.running(self.directory))
            self.assertFalse((self.directory/supervision.SOCKET).exists())


if __name__ == "__main__":
    unittest.main()
