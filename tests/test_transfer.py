from __future__ import annotations

import hashlib
from pathlib import Path
from subprocess import CompletedProcess
from typing import Any

import pytest

from runpod_jobrunner.transfer import LocalTransfer, RcloneSFTP, TransferError


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_local_upload_copies_only_manifest_entries(tmp_path: Path) -> None:
    source = tmp_path / "source"
    remote = tmp_path / "remote"
    source.mkdir()
    (source / "allowed.txt").write_bytes(b"allowed")
    (source / "private.txt").write_bytes(b"not declared")

    receipt = LocalTransfer().upload(
        source,
        remote,
        [{"path": "allowed.txt", "size": 7, "sha256": digest(b"allowed")}],
    )

    assert (remote / "allowed.txt").read_bytes() == b"allowed"
    assert not (remote / "private.txt").exists()
    assert receipt.files == 1
    assert receipt.bytes == 7


def test_local_upload_rejects_symlink_and_mutation(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    target = source / "target"
    target.write_bytes(b"changed")
    (source / "link").symlink_to(target)

    with pytest.raises(TransferError, match="symlink"):
        LocalTransfer().upload(
            source,
            tmp_path / "remote",
            [{"path": "link", "size": 7, "sha256": digest(b"changed")}],
        )
    with pytest.raises(TransferError, match="hash"):
        LocalTransfer().upload(
            source,
            tmp_path / "remote",
            [{"path": "target", "size": 7, "sha256": "0" * 64}],
        )


def test_rclone_requires_known_host_and_uses_files_from(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def run(argv: list[str], **kwargs: Any) -> CompletedProcess[str]:
        calls.append(argv)
        return CompletedProcess(argv, 0, stdout="", stderr="")

    known_hosts = tmp_path / "known_hosts"
    key = tmp_path / "id_ed25519"
    known_hosts.write_text("host ssh-ed25519 AAAA\n")
    key.write_text("key")
    transfer = RcloneSFTP(
        host="example",
        port=2222,
        user="root",
        key_file=key,
        known_hosts_file=known_hosts,
        run_command=run,
    )
    root = tmp_path / "source"
    root.mkdir()
    (root / "one").write_bytes(b"1")

    transfer.upload(root, "/workspace/input", [{"path": "one", "size": 1, "sha256": digest(b"1")}])

    argv = calls[0]
    assert "--sftp-known-hosts-file" in argv
    assert str(known_hosts.resolve()) in argv
    assert "--files-from-raw" in argv
    assert "--sftp-use-insecure-cipher" not in argv


def test_rclone_refuses_missing_known_hosts(tmp_path: Path) -> None:
    with pytest.raises(TransferError, match="known-hosts"):
        RcloneSFTP(
            host="example",
            port=22,
            user="root",
            key_file=tmp_path / "key",
            known_hosts_file=tmp_path / "missing",
        )


def test_rclone_downloads_only_declared_files_then_verifies_local_bytes(
    tmp_path: Path,
) -> None:
    calls: list[list[str]] = []
    known_hosts = tmp_path / "known_hosts"
    key = tmp_path / "id_ed25519"
    known_hosts.write_text("host ssh-ed25519 AAAA\n")
    key.write_text("key")
    payload = b"artifact"

    def run(argv: list[str], **kwargs: Any) -> CompletedProcess[str]:
        calls.append(argv)
        destination = Path(argv[3])
        destination.mkdir(parents=True, exist_ok=True)
        (destination / "result.bin").write_bytes(payload)
        return CompletedProcess(argv, 0, stdout="", stderr="")

    transfer = RcloneSFTP(
        host="example",
        port=2222,
        user="root",
        key_file=key,
        known_hosts_file=known_hosts,
        run_command=run,
    )

    receipt = transfer.download(
        "/workspace/artifacts",
        tmp_path / "receipts",
        [{"path": "result.bin", "size": len(payload), "sha256": digest(payload)}],
    )

    assert receipt.files == 1
    assert receipt.bytes == len(payload)
    assert calls[0][0:2] == ["rclone", "copy"]
    assert calls[0][2] == ":sftp:/workspace/artifacts"
    assert "--files-from-raw" in calls[0]


def test_rclone_download_rejects_corrupt_received_file(tmp_path: Path) -> None:
    known_hosts = tmp_path / "known_hosts"
    key = tmp_path / "id_ed25519"
    known_hosts.write_text("host ssh-ed25519 AAAA\n")
    key.write_text("key")

    def run(argv: list[str], **kwargs: Any) -> CompletedProcess[str]:
        destination = Path(argv[3])
        destination.mkdir(parents=True, exist_ok=True)
        (destination / "result.bin").write_bytes(b"corrupt")
        return CompletedProcess(argv, 0, stdout="", stderr="")

    transfer = RcloneSFTP(
        host="example",
        port=2222,
        user="root",
        key_file=key,
        known_hosts_file=known_hosts,
        run_command=run,
    )

    with pytest.raises(TransferError, match="hash mismatch"):
        transfer.download(
            "/workspace/artifacts",
            tmp_path / "receipts",
            [{"path": "result.bin", "size": 7, "sha256": digest(b"expected")}],
        )
