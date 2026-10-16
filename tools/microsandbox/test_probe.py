import unittest
from unittest.mock import Mock

from probe import Microsandbox, assert_database


class HibernateTest(unittest.TestCase):
    def runtime(self, failure=None):
        runtime = Microsandbox("unused", "/tmp/unused")

        def run(*args):
            if args == failure:
                raise RuntimeError("injected runtime failure")
            if args[0] == "inspect":
                return '{"status":"Stopped"}'
            return ""

        runtime.run = Mock(side_effect=run)
        return runtime

    def test_checkpoint_failure_keeps_source_and_resumes_it(self):
        runtime = self.runtime(("snap", "create", "s", "--sandbox", "db", "--full", "--integrity"))
        with self.assertRaises(RuntimeError):
            runtime.hibernate("db", "s")
        self.assertFalse(any(call.args[0] == "stop" for call in runtime.run.call_args_list))
        runtime.run.assert_called_with("resume", "db")

    def test_integrity_failure_never_terminates_source(self):
        runtime = self.runtime(("snap", "verify", "db:s"))
        with self.assertRaises(RuntimeError):
            runtime.hibernate("db", "s")
        self.assertFalse(any(call.args[0] == "stop" for call in runtime.run.call_args_list))
        runtime.run.assert_called_with("resume", "db")

    def test_stop_failure_does_not_report_hibernation_success(self):
        runtime = self.runtime(("stop", "--force", "db"))
        with self.assertRaises(RuntimeError):
            runtime.hibernate("db", "s")
        self.assertEqual(["db:s"], runtime.snapshots)

    def test_terminated_source_is_required(self):
        runtime = self.runtime()
        runtime.inspect = Mock(return_value={"status": "Running"})
        with self.assertRaisesRegex(RuntimeError, "did not stop"):
            runtime.hibernate("db", "s")

    def test_cleanup_does_not_touch_unowned_workloads(self):
        runtime = self.runtime()
        runtime.names = ["owned", "failed-before-create"]
        runtime.run.side_effect = lambda *args: (
            '[{"name":"owned"},{"name":"unrelated"}]' if args[0] == "ls"
            else '{"status":"Stopped"}' if args[0] == "inspect" else ""
        )
        self.assertEqual([], runtime.cleanup())
        self.assertFalse(any("unrelated" in call.args for call in runtime.run.call_args_list))
        runtime.run.assert_any_call("rm", "owned")

    def test_restart_or_stale_snapshot_cannot_pass(self):
        connection = Mock()
        for row in [("new-process", 2, "marker"), ("original", 1, "marker"), ("original", 2, "")]:
            with self.subTest(row=row):
                connection.execute.return_value.fetchone.return_value = row
                with self.assertRaises(RuntimeError):
                    assert_database(connection, "original", 2, "marker")


if __name__ == "__main__":
    unittest.main()
