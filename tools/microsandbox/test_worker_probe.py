import argparse
import json
import subprocess
import unittest
from unittest.mock import Mock, patch

from worker_probe import Commands, OWNER_LABEL, PodFailed, WorkerProbe, wait_for


class WorkerProbeSafetyTest(unittest.TestCase):
    def probe(self):
        commands = Commands("msb", "docker", "/tmp/unused")
        return WorkerProbe(commands, argparse.Namespace(
            image="k3s:pinned", pod_image="busybox:pinned", workers=1, relay_vxlan=False))

    def test_command_failure_redacts_token_and_does_not_expose_stdin(self):
        commands = Commands("msb", "docker", "/tmp/unused")
        commands.secrets.append("private-agent-token")
        completed = subprocess.CompletedProcess(
            ["unused"], 1, stdout="", stderr="request failed with private-agent-token"
        )
        with patch("worker_probe.subprocess.run", return_value=completed):
            with self.assertRaises(RuntimeError) as caught:
                commands.vm("exec", "worker", input_text="private-input")
        self.assertNotIn("private-agent-token", str(caught.exception))
        self.assertNotIn("private-input", str(caught.exception))
        self.assertIn("[redacted]", str(caught.exception))

    def test_timeout_does_not_include_command_or_input(self):
        commands = Commands("msb", "docker", "/tmp/unused")
        with patch("worker_probe.subprocess.run", side_effect=subprocess.TimeoutExpired(
                ["msb", "exec", "private-command"], 1, output="private-output")):
            with self.assertRaisesRegex(RuntimeError, "msb exec timed out after 1s") as caught:
                commands.vm("exec", "private-command", timeout=1)
        self.assertNotIn("private", str(caught.exception))

    def test_unknown_secure_tokens_are_redacted_from_diagnostics(self):
        commands = Commands("msb", "docker", "/tmp/unused")
        secure_token = "K10" + "a" * 64 + "::server:unregistered-secret"
        self.assertEqual("token=[redacted]", commands.redact("token=" + secure_token))

    def test_cleanup_only_removes_the_exact_owned_resources(self):
        probe = self.probe()
        probe.attempted_workers.append(probe.worker)
        probe.server_attempted = True
        probe.commands.vm = Mock(side_effect=[
            json.dumps([{"name": "unrelated"}, {"name": probe.worker}]),
            json.dumps({"status": "Running", "config": {"labels": {OWNER_LABEL: probe.owner}}}), "", "",
        ])
        probe.commands.container = Mock(side_effect=[
            "owned-id", json.dumps([{"Config": {"Labels": {OWNER_LABEL: probe.owner}}}]), "",
        ])
        self.assertEqual([], probe.cleanup())
        probe.commands.vm.assert_any_call("stop", "--force", probe.worker)
        probe.commands.vm.assert_any_call("rm", probe.worker)
        probe.commands.container.assert_any_call("rm", "-f", "-v", probe.server)
        for call in probe.commands.vm.call_args_list + probe.commands.container.call_args_list:
            self.assertNotIn("unrelated", call.args)

    def test_cleanup_refuses_container_whose_ownership_changed(self):
        probe = self.probe()
        probe.server_attempted = True
        probe.commands.container = Mock(side_effect=[
            "unexpected-id", json.dumps([{"Config": {"Labels": {OWNER_LABEL: "someone-else"}}}]),
        ])
        errors = probe.cleanup()
        self.assertEqual(1, len(errors))
        self.assertIn("ownership label changed", errors[0])
        self.assertFalse(any(call.args[0] == "rm" for call in probe.commands.container.call_args_list))

    def test_cleanup_refuses_worker_with_changed_active_ownership(self):
        probe = self.probe()
        probe.attempted_workers.append(probe.worker)
        probe.commands.vm = Mock(side_effect=[
            json.dumps([{"name": probe.worker}]),
            json.dumps({"status": "Running", "config": {"labels": {OWNER_LABEL: probe.owner}},
                        "active_config": {"labels": {OWNER_LABEL: "someone-else"}}}),
        ])
        errors = probe.cleanup()
        self.assertEqual(1, len(errors))
        self.assertIn("ownership label changed", errors[0])
        self.assertFalse(any(call.args[0] in ("rm", "stop") for call in probe.commands.vm.call_args_list))

    def test_placeholder_ready_node_without_kubelet_lease_cannot_pass(self):
        probe = self.probe()
        probe.resource = Mock(return_value={
            "status": {"conditions": [{"type": "Ready", "status": "True"}]},
        })
        probe.kubectl = Mock(return_value='{"spec":{"holderIdentity":"different-node"}}')
        self.assertIsNone(probe.ready_node())

    def test_terminal_pod_failure_is_not_retried_even_when_logs_fail(self):
        probe = self.probe()
        probe.resource = Mock(return_value={"status": {"phase": "Failed"}})
        probe.kubectl = Mock(side_effect=RuntimeError("logs request timed out"))
        with patch("worker_probe.time.sleep") as sleep:
            with self.assertRaisesRegex(PodFailed, "logs unavailable"):
                wait_for("Pod", lambda: probe.completed_pod("failed-pod"), 90)
        probe.resource.assert_called_once_with("pod", "failed-pod")
        sleep.assert_not_called()

    def test_transient_kubectl_failure_is_still_retried(self):
        operation = Mock(side_effect=[RuntimeError("connection refused"), {"ready": True}])
        with patch("worker_probe.time.sleep") as sleep:
            self.assertEqual({"ready": True}, wait_for("Pod", operation, 90))
        self.assertEqual(2, operation.call_count)
        sleep.assert_called_once_with(1)

    def test_host_gateway_lookup_accepts_getent_and_busybox_dns_answers(self):
        self.assertEqual("172.16.0.5", WorkerProbe.host_gateway("172.16.0.5 host.microsandbox.internal"))
        self.assertEqual("172.16.0.13", WorkerProbe.host_gateway(
            "Server: 172.16.0.13\nAddress: 172.16.0.13:53\n"
            "Name: host.microsandbox.internal\nAddress: 172.16.0.13\n"))
        with self.assertRaisesRegex(RuntimeError, "Cannot resolve"):
            WorkerProbe.host_gateway("lookup failed")
        with self.assertRaises(ValueError):
            WorkerProbe.host_gateway("Address: 999.16.0.13")

    def test_relay_only_translates_peer_vxlan_and_never_mutates_host_networking(self):
        probe = self.probe()
        peer = probe.worker + "-2"
        probe.workers.append(peer)
        probe.vxlan_ports = {probe.worker: 49100, peer: 49101}
        probe.commands.vm = Mock(side_effect=["172.16.0.5 host.microsandbox.internal", "",
                                             "172.16.0.9 host.microsandbox.internal", ""])
        probe.commands.container = Mock()
        probe.resource = Mock(side_effect=[
            {"status": {"addresses": [{"type": "InternalIP", "address": "172.16.0.10"}]}},
            {"status": {"addresses": [{"type": "InternalIP", "address": "172.16.0.6"}]}},
        ])
        probe.configure_vxlan_relay()
        mutations = [call.args for call in probe.commands.vm.call_args_list if "vxlan-relay" in call.args]
        self.assertEqual(2, len(mutations))
        self.assertEqual(("172.16.0.10", "172.16.0.5", "49101"), mutations[0][-3:])
        self.assertEqual(("172.16.0.6", "172.16.0.9", "49100"), mutations[1][-3:])
        for command in mutations:
            self.assertEqual("exec", command[0])
            self.assertIn(command[1], probe.workers)
            self.assertIn("--dport 8472", command[5])
            self.assertNotIn("MASQUERADE", command[5])
        probe.commands.container.assert_not_called()


if __name__ == "__main__":
    unittest.main()
