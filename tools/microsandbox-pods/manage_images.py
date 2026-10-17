"""Private, checksummed k3s airgap image archive staged before peer startup."""
import hashlib
import os
from pathlib import Path
import shutil
import stat
import tempfile


def digest(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as source:
        if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
            raise RuntimeError("peer-images must be a regular archive")
        return hashlib.file_digest(source, "sha256").hexdigest()


def resolve(record, args, existing):
    source = getattr(args, "peer_images", None)
    previous = record.get("peerImages")
    protected = record.get("environment") or getattr(args, "environment_network", None)
    if not source and not previous:
        if protected:
            raise ValueError("protected setup requires peer-images containing the pinned k3s system images before first boot")
        return
    if previous:
        filename = previous.get("filename", "")
        if filename not in ("images.tar", "images.tar.gz", "images.tar.zst"):
            raise RuntimeError("invalid recorded peer image archive")
        path = Path(record["directory"]) / "peer-images" / "agent" / "images" / filename
        if digest(path) != previous["sha256"]:
            raise RuntimeError("private peer image archive checksum changed")
        if source and digest(Path(source).absolute()) != previous["sha256"]:
            raise RuntimeError("peer-images differs from the recorded cluster archive")
        return
    if existing:
        raise RuntimeError("peer-images must be selected when preparing a new cluster")
    source = Path(source).absolute()
    extension = next((value for value in (".tar.gz", ".tar.zst", ".tar") if source.name.endswith(value)), None)
    if extension is None:
        raise ValueError("peer-images must be a .tar, .tar.gz or .tar.zst archive")
    destination = Path(record["directory"]) / "peer-images" / "agent" / "images"
    destination.mkdir(mode=0o700, parents=True)
    filename = "images" + extension
    descriptor, temporary = tempfile.mkstemp(prefix=".pending-", dir=destination)
    try:
        with os.fdopen(descriptor, "wb") as writer:
            source_descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(source_descriptor, "rb") as reader:
                if not stat.S_ISREG(os.fstat(reader.fileno()).st_mode):
                    raise RuntimeError("peer-images must be a regular archive")
                shutil.copyfileobj(reader, writer, 1 << 20)
                writer.flush()
                os.fsync(writer.fileno())
        os.replace(temporary, destination / filename)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    record["peerImages"] = {"filename": filename, "sha256": digest(destination / filename)}


def stage(record, command):
    if record.get("peerImages"):
        resolve(record, None, True)
        command("docker", "cp", str(Path(record["directory"]) / "peer-images" / "agent"),
            record["peer"] + ":/var/lib/rancher/k3s/")
