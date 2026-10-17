"""Protected setup pins ownership and installs DNS before a peer can run."""
import argparse
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import manage
import manage_environment as environment


class EnvironmentTest(unittest.TestCase):
    def setUp(self):
        self.network = {"Id": "a" * 64, "Name": "protected", "Driver": "bridge", "Scope": "local",
            "Options": {"com.docker.network.bridge.name": "owned-bridge"},
            "Containers": {"dns": {"IPv4Address": "172.30.0.2/24"}}}
        self.helper = {"Id": "b" * 64, "Config": {"Labels": {"floci.network.owner": "fixture/4566",
            "floci.network.table": "floci_iso_abcdef"}}, "State": {"Running": True}, "HostConfig": {"NetworkMode": "host"}}
        self.metadata = {"version": 1, "owner": "fixture/4566", "table": "floci_iso_abcdef", "fingerprint": "c" * 64,
            "bridges": [{"name": "protected", "networkId": "a" * 64, "bridge": "owned-bridge"}]}
        self.args = argparse.Namespace(environment_network="protected", isolation_helper="helper", environment_dns="172.30.0.2")

    def command(self, *args):
        if args[:3] == ("docker", "network", "inspect"):
            return json.dumps([self.network])
        if args[:2] == ("docker", "inspect"):
            return json.dumps([self.helper])
        self.assertEqual(("docker", "exec", "b" * 64, "cat", environment.METADATA), args)
        return json.dumps(self.metadata)

    def resolved(self):
        record = {}
        environment.resolve(record, self.args, self.command, False)
        return record

    def test_pins_complete_identities_and_rechecks_when_flags_are_omitted(self):
        record = self.resolved()
        self.assertEqual(2, record["version"])
        self.assertEqual("a" * 64, record["environment"]["networkID"])
        self.assertEqual("b" * 64, record["environment"]["helperID"])
        self.assertEqual("owned-bridge", record["environment"]["bridge"])
        expected = copy.deepcopy(record)
        environment.resolve(record, argparse.Namespace(), self.command, True)
        self.assertEqual(expected, record)
        self.metadata["fingerprint"] = "d" * 64
        with self.assertRaisesRegex(RuntimeError, "identity changed"):
            environment.resolve(record, argparse.Namespace(), self.command, True)
        self.assertEqual(expected, record)

    def test_rejects_partial_options_and_unisolated_cluster_conversion(self):
        for args in (argparse.Namespace(environment_network="protected"),
                argparse.Namespace(environment_dns="172.30.0.2")):
            with self.assertRaisesRegex(ValueError, "supplied together"):
                environment.resolve({}, args, self.command, False)
        with self.assertRaisesRegex(RuntimeError, "existing unisolated"):
            environment.resolve({}, self.args, self.command, True)

    def test_refuses_unassigned_dns_uncovered_bridge_and_bad_helper_metadata(self):
        cases = [(self.network, "Containers", {}), (self.network, "Driver", "overlay"),
            (self.helper["State"], "Running", False), (self.metadata, "version", 2),
            (self.metadata, "owner", "another"), (self.metadata, "table", "another"),
            (self.metadata, "fingerprint", "unknown"), (self.metadata, "bridges", [])]
        for target, key, bad in cases:
            with self.subTest(key=key):
                old = target[key]
                target[key] = bad
                try:
                    with self.assertRaises(RuntimeError):
                        self.resolved()
                finally:
                    target[key] = old
        for address in ("127.0.0.1", "224.0.0.1", "0.0.0.0", "::1"):
            with self.assertRaises(ValueError):
                environment.inspect(self.command, "protected", "helper", address)

    def test_peer_requires_exact_single_network_and_controlled_resolver(self):
        record = self.resolved()
        peer = {"NetworkSettings": {"Networks": {"protected": {"NetworkID": "a" * 64}}},
            "HostConfig": {"Dns": ["172.30.0.2"]}, "Config": {"Env": ["K3S_RESOLV_CONF=" + environment.RESOLVER]}}
        environment.verify_peer(record, peer)
        for change in (
                lambda p: p["NetworkSettings"]["Networks"].update(outside={"NetworkID": "d" * 64}),
                lambda p: p["HostConfig"].update(Dns=["172.30.0.2", "8.8.8.8"]),
                lambda p: p["Config"].update(Env=[])):
            changed = copy.deepcopy(peer)
            change(changed)
            with self.assertRaises(RuntimeError):
                environment.verify_peer(record, changed)

    def test_prepare_creates_protected_peer_and_copies_dns_before_start(self):
        with tempfile.TemporaryDirectory() as temporary:
            args = argparse.Namespace(directory=temporary, binaries=temporary, msb=temporary + "/msb",
                msb_home=temporary, node_ip="192.0.2.10")
            captured = []
            def command(*values, **kwargs):
                captured.append(values)
                if values[-1] == "--version":
                    return "microsandbox 0.7.8"
                return ""
            owned = self.resolved()["environment"]
            with patch.object(manage, "command", side_effect=command), \
                    patch.object(manage, "inspect_peer", return_value=None), \
                    patch.object(manage.os, "access", return_value=True), \
                    patch.object(environment, "resolve", side_effect=lambda record, *_: record.update(environment=owned)), \
                    patch.object(manage, "wait", side_effect=RuntimeError("stop before API")):
                with self.assertRaisesRegex(RuntimeError, "stop before API"):
                    manage.prepare(args)
            operations = [values[1] for values in captured if values[0] == "docker"]
            self.assertEqual(["create", "cp", "start"], operations)
            create = next(values for values in captured if values[:2] == ("docker", "create"))
            self.assertEqual("a" * 64, create[create.index("--network") + 1])
            self.assertEqual("172.30.0.2", create[create.index("--dns") + 1])
            self.assertIn("K3S_RESOLV_CONF=" + environment.RESOLVER, create)
            self.assertFalse(any("io.floci.owner" in str(value) for value in create))
            resolver = Path(temporary) / "environment-resolv.conf"
            self.assertEqual("nameserver 172.30.0.2\noptions timeout:2 attempts:2\n", resolver.read_text())
            self.assertEqual(0o600, resolver.stat().st_mode & 0o777)
            record = manage.load(temporary)
            record.update(node="native", kubeletPort=1000, controlPort=1001, hostIP="192.0.2.10")
            run_args = argparse.Namespace(memory_mib=768, scheduling_cpu="1", scheduling_memory="1Gi", pod_workers=1,
                network_policy=False)
            argv = manage.controller_arguments(record, run_args)
            self.assertIn("--network-policy=true", argv)
            self.assertEqual("a" * 64, argv[argv.index("--environment-network") + 1])
            self.assertEqual("b" * 64, argv[argv.index("--isolation-helper") + 1])


if __name__ == "__main__":
    unittest.main()
