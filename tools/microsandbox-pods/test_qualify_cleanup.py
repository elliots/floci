"""Durability guards for failure diagnostics and fallback fixture cleanup."""
import json
from pathlib import Path
import tempfile
import unittest

from qualify import persistent_cleanup_reason


class PersistentCleanupTest(unittest.TestCase):
    def test_empty_and_deleted_tombstones_allow_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            self.assertIsNone(persistent_cleanup_reason(state))
            (state/"persistent-volumes").mkdir()
            (state/"persistent-volumes"/"volume.json").write_text(json.dumps({"version": 1, "phase": "deleted"}))
            (state/"pod.json").write_text(json.dumps({"pod": {"spec": {"volumes": [{"name": "scratch", "emptyDir": {}}]}}}))
            self.assertIsNone(persistent_cleanup_reason(state))

    def test_live_or_unknown_intents_preserve_whole_fixture(self):
        for contents in ('{"version":1,"phase":"ready"}', '{"version":1,"phase":"deleting"}',
                         '{"version":2,"phase":"deleted"}', '{}', 'null', '{broken'):
            with self.subTest(contents=contents), tempfile.TemporaryDirectory() as directory:
                state = Path(directory)
                (state/"persistent-volumes").mkdir()
                (state/"persistent-volumes"/"volume.json").write_text(contents)
                self.assertIsNotNone(persistent_cleanup_reason(state))

    def test_retained_pod_or_malformed_ownership_blocks_destructive_fallback(self):
        for record in ({"persistentVolumes": [{"name": "data"}]}, {"persistentWriteback": {"phase": "SEALING"}},
                       {"pod": {"spec": {"volumes": [{"persistentVolumeClaim": {"claimName": "db"}}]}}},
                       {"pod": None}, None):
            with self.subTest(record=record), tempfile.TemporaryDirectory() as directory:
                state = Path(directory)
                (state/"pod.json").write_text(json.dumps(record))
                self.assertIsNotNone(persistent_cleanup_reason(state))
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            (state/"pod.json").write_text('{broken')
            self.assertIsNotNone(persistent_cleanup_reason(state))

    def test_symlink_intent_is_not_followed(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            (state/"persistent-volumes").mkdir()
            (state/"persistent-volumes"/"volume.json").symlink_to(state/"missing")
            self.assertIsNotNone(persistent_cleanup_reason(state))


if __name__ == "__main__":
    unittest.main()
