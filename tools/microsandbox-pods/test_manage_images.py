"""Airgap archive ownership and mutation refusal before starting a peer."""
import argparse
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

import manage_images


class ImageArchiveTest(unittest.TestCase):
    def test_archives_are_copied_privately_and_rechecked_before_staging(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "release.tar.zst"
            source.write_bytes(b"owned fixture archive")
            record = {"directory": str(root / "cluster"), "peer": "owned-peer"}
            args = argparse.Namespace(peer_images=str(source))
            manage_images.resolve(record, args, False)
            copied = root / "cluster/peer-images/agent/images/images.tar.zst"
            self.assertEqual(source.read_bytes(), copied.read_bytes())
            self.assertEqual(0o600, copied.stat().st_mode & 0o777)
            source.unlink()
            command = Mock()
            manage_images.stage(record, command)
            command.assert_called_once_with("docker", "cp", str(root / "cluster/peer-images/agent"),
                "owned-peer:/var/lib/rancher/k3s/")
            command.reset_mock()
            copied.write_bytes(b"changed")
            with self.assertRaisesRegex(RuntimeError, "checksum changed"):
                manage_images.stage(record, command)
            command.assert_not_called()

    def test_protected_first_boot_requires_archive_and_symlinks_are_refused(self):
        with tempfile.TemporaryDirectory() as temporary:
            record = {"directory": temporary}
            with self.assertRaisesRegex(ValueError, "requires peer-images"):
                manage_images.resolve(record, argparse.Namespace(environment_network="protected"), False)
            source = Path(temporary) / "image.tar"
            source.write_bytes(b"archive")
            link = Path(temporary) / "alias.tar"
            link.symlink_to(source)
            with self.assertRaises(OSError):
                manage_images.resolve(record, argparse.Namespace(peer_images=str(link)), False)
            self.assertNotIn("peerImages", record)

    def test_reprepare_rejects_another_archive_and_malformed_record_paths(self):
        with tempfile.TemporaryDirectory() as temporary:
            record = {"directory": temporary}
            source = Path(temporary) / "image.tar"
            source.write_bytes(b"archive")
            args = argparse.Namespace(peer_images=str(source))
            manage_images.resolve(record, args, False)
            source.write_bytes(b"replacement")
            with self.assertRaisesRegex(RuntimeError, "differs"):
                manage_images.resolve(record, args, True)
            record["peerImages"]["filename"] = "../../unexpected.tar"
            with self.assertRaisesRegex(RuntimeError, "invalid recorded"):
                manage_images.stage(record, Mock())


if __name__ == "__main__":
    unittest.main()
