"""Ownership and credential boundaries for repeatable native cluster setup."""
import argparse
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location("manage", Path(__file__).with_name("manage.py"))
manage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(manage)


class ManagementTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.record = {"version": 1, "directory": str(self.directory.resolve()), "owner": "owned",
            "peer": "native-peer", "node": "native-pods", "user": "native-controller",
            "accessName": "native-access", "controlPort": 12345}
        manage.write_private(self.directory / "cluster.json", json.dumps(self.record))
        self.args = argparse.Namespace(directory=self.directory)

    def test_private_atomic_writes_and_directory_identity(self):
        path = self.directory / "credentials"
        manage.write_private(path, "first")
        manage.write_private(path, "second")
        self.assertEqual("second", path.read_text())
        self.assertEqual(0o600, path.stat().st_mode & 0o777)
        self.assertEqual(self.record, manage.load(self.directory))
        self.record["directory"] = "/another/location"
        manage.write_private(self.directory / "cluster.json", json.dumps(self.record))
        with self.assertRaisesRegex(RuntimeError, "another directory"):
            manage.load(self.directory)

    def test_explicit_or_recorded_node_ip_requires_no_special_hostname(self):
        with patch.object(manage, "command") as command:
            self.assertEqual("192.0.2.10", manage.node_address(self.record, "192.0.2.10"))
            record = dict(self.record, hostIP="192.0.2.10")
            self.assertEqual("192.0.2.10", manage.node_address(record))
            with self.assertRaisesRegex(RuntimeError, "routing identity"):
                manage.node_address(record, "192.0.2.11")
            command.assert_not_called()
        for address in ("0.0.0.0", "224.0.0.1", "127.0.0.1", "host.invalid", "::1", "255.255.255.255"):
            with self.assertRaises(ValueError):
                manage.node_address(self.record, address)

    def test_node_discovery_honors_hosts_file_before_dns(self):
        with patch.object(manage, "command", return_value="127.0.0.1 localhost\n192.0.2.10 host.docker.internal # supplied by Docker\n") as command:
            self.assertEqual("192.0.2.10", manage.node_address(self.record))
            command.assert_called_once_with("docker", "exec", self.record["peer"], "cat", "/etc/hosts")

    def test_setup_lock_excludes_concurrent_mutations(self):
        with manage.setup_lock(self.directory):
            with self.assertRaisesRegex(RuntimeError, "in progress"):
                with manage.setup_lock(self.directory):
                    self.fail("lock was acquired twice")
        with manage.setup_lock(self.directory):
            pass

    def test_peer_ownership_and_image_must_match(self):
        peer = {"Config": {"Labels": {manage.LABEL: "another"}, "Image": manage.K3S_IMAGE}}
        with patch.object(manage, "command", side_effect=["id", json.dumps([peer])]):
            with self.assertRaisesRegex(RuntimeError, "ownership changed"):
                manage.inspect_peer(self.record)
        peer["Config"]["Labels"][manage.LABEL] = "owned"
        peer["Config"]["Image"] = "k3s:latest"
        with patch.object(manage, "command", side_effect=["id", json.dumps([peer])]):
            with self.assertRaisesRegex(RuntimeError, "pinned cluster"):
                manage.inspect_peer(self.record)

    def test_destroy_checks_journals_before_touching_cluster(self):
        manage.write_private(self.directory / "controller" / "pod.json", "{}")
        with patch.object(manage, "command") as command:
            with self.assertRaisesRegex(RuntimeError, "workload or recovery records"):
                manage.destroy(self.args)
            command.assert_not_called()

    def test_destroy_refuses_live_controller_and_unrecorded_pods(self):
        with patch.object(manage, "controller_running", return_value=True):
            with self.assertRaisesRegex(RuntimeError, "stop the controller"):
                manage.destroy(self.args)
        with patch.object(manage, "controller_running", return_value=False), \
                patch.object(manage, "inspect_peer", return_value={"State": {"Running": True}}), \
                patch.object(manage, "kubectl", return_value='{"items":[{"metadata":{"name":"cold"}}]}'), \
                patch.object(manage, "command") as command:
            with self.assertRaisesRegex(RuntimeError, "Pods still target"):
                manage.destroy(self.args)
            command.assert_not_called()

    def test_destroy_removes_only_verified_empty_peer(self):
        with patch.object(manage, "controller_running", return_value=False), \
                patch.object(manage, "inspect_peer", return_value={"State": {"Running": True}}), \
                patch.object(manage, "kubectl", return_value='{"items":[]}'), \
                patch.object(manage, "command") as command, patch("builtins.print"):
            manage.destroy(self.args)
            command.assert_called_once_with("docker", "rm", "-fv", "native-peer")
        self.assertTrue((self.directory / "cluster.json").exists())

    def test_controller_role_has_no_cluster_admin_or_workload_creation(self):
        resources = manage.access_resources(self.record)["items"]
        role = next(value for value in resources if value["kind"] == "ClusterRole")
        for rule in role["rules"]:
            self.assertNotIn("*", rule["verbs"])
            self.assertNotIn("*", rule["resources"])
            self.assertNotIn("rbac.authorization.k8s.io", rule["apiGroups"])
            if "pods" in rule["resources"] or "secrets" in rule["resources"]:
                self.assertNotIn("create", rule["verbs"])
                self.assertNotIn("update", rule["verbs"])
            if "nodes" in rule["resources"]:
                self.assertEqual([self.record["node"]], rule["resourceNames"])
        for binding in (value for value in resources if value["kind"].endswith("Binding")):
            self.assertEqual(self.record["accessName"], binding["roleRef"]["name"])
            self.assertEqual(self.record["user"], binding["subjects"][0]["name"])

    def test_status_keeps_unknown_shared_usage_separate_from_zero(self):
        self.record["msb_home"] = str(self.directory)
        manage.write_private(self.directory / "cluster.json", json.dumps(self.record))
        manage.write_private(self.directory / "control-token", "a" * 32)
        for capacity in ({"memoryMiB": 768, "reservations": ["pending-wake"]}, None):
            replies = [io.BytesIO(b'{"activeMemoryMiB":0}'),
                io.BytesIO(json.dumps(capacity).encode()) if capacity else OSError("unavailable")]
            with patch.object(manage, "inspect_peer", return_value=None), \
                    patch.object(manage.urllib.request, "urlopen", side_effect=replies), \
                    patch("builtins.print") as output:
                manage.status(self.args)
                report = json.loads(output.call_args.args[0])
                self.assertEqual(0, report["controller"]["activeMemoryMiB"])
                self.assertEqual(capacity or "unavailable", report["sharedCapacity"])


if __name__ == "__main__":
    unittest.main()
