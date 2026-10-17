import contextlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from qualify_isolation import EnvironmentFixture, unused_public_canary_subnet


class IsolationQualificationTest(unittest.TestCase):
    def test_public_canary_subnet_avoids_existing_supernets_and_subnets(self):
        networks = [{"IPAM": {"Config": [{"Subnet": subnet}]}}
            for subnet in ("203.0.0.0/16", "198.51.100.128/25", "fd00::/64")]
        networks.append({"IPAM": {"Config": None}})
        self.assertEqual("192.0.2.0/24", unused_public_canary_subnet(networks))

    def test_public_canary_subnet_refuses_when_all_candidates_overlap(self):
        with self.assertRaisesRegex(RuntimeError, "overlap"):
            unused_public_canary_subnet([{"IPAM": {"Config": [{"Subnet": "0.0.0.0/0"}]}}])

    def test_all_probe_results_are_saved_before_any_failed_assertion(self):
        results = [
            {"name": "external", "success": False, "error": "TimeoutError"},
            {"name": "internal", "success": False, "error": "TimeoutError"},
            {"name": "simulator", "success": True, "value": {"status": 200}},
        ]
        requests = [
            {"name": "external", "allowed": False},
            {"name": "internal"},
            {"name": "simulator"},
        ]
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            q = SimpleNamespace(root=Path(directory), args=SimpleNamespace(report=str(report)),
                report={"checks": {}}, kubectl=Mock(return_value=json.dumps(results)))
            fixture = EnvironmentFixture(q)
            with contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(AssertionError) as failure:
                    fixture.probes(q, requests, "initial")
            self.assertIn("internal", str(failure.exception))
            saved = json.loads(report.read_text())
            self.assertEqual({value["name"]: value for value in results}, saved["initialProbes"])
            self.assertEqual({"initial:external": True, "initial:internal": False,
                "initial:simulator": True}, saved["checks"])


if __name__ == "__main__":
    unittest.main()
