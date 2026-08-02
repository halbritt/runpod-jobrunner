from __future__ import annotations

import hashlib
from pathlib import Path
from subprocess import CompletedProcess, TimeoutExpired
from typing import Any

import pytest

from runpod_jobrunner.transfer import (
    LocalTransfer,
    RcloneSFTP,
    RemoteFile,
    TransferError,
    TransferUnavailable,
    validate_discovery_pattern,
)


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


def test_local_atomic_publish_exposes_only_the_complete_file(tmp_path: Path) -> None:
    source = tmp_path / "launch-token"
    payload = b"1" * 64 + b"\n"
    source.write_bytes(payload)
    destination = tmp_path / "remote" / "control" / "launch-token"

    receipt = LocalTransfer().publish_atomic(
        source,
        destination,
        size=len(payload),
        sha256=digest(payload),
    )

    assert destination.read_bytes() == payload
    assert list(destination.parent.glob(".*.partial-*")) == []
    assert receipt.files == 1
    assert receipt.bytes == len(payload)


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


def test_rclone_atomic_publish_stages_then_server_side_renames(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def run(argv: list[str], **kwargs: Any) -> CompletedProcess[str]:
        calls.append(argv)
        return CompletedProcess(argv, 0, stdout="", stderr="")

    known_hosts = tmp_path / "known_hosts"
    key = tmp_path / "id_ed25519"
    known_hosts.write_text("host ssh-ed25519 AAAA\n")
    key.write_text("key")
    source = tmp_path / "launch-token"
    payload = b"1" * 64 + b"\n"
    source.write_bytes(payload)
    transfer = RcloneSFTP(
        host="example",
        port=2222,
        user="root",
        key_file=key,
        known_hosts_file=known_hosts,
        run_command=run,
    )

    receipt = transfer.publish_atomic(
        source,
        "/workspace/control/launch-token",
        size=len(payload),
        sha256=digest(payload),
    )

    assert calls[0][0:2] == ["rclone", "copyto"]
    assert calls[0][2] == str(source.resolve())
    staging = calls[0][3]
    assert staging.startswith(":sftp:/workspace/control/.launch-token.partial-")
    assert calls[1][0:2] == ["rclone", "moveto"]
    assert calls[1][2] == staging
    assert calls[1][3] == ":sftp:/workspace/control/launch-token"
    assert receipt.files == 1
    assert receipt.bytes == len(payload)


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


def test_local_discovery_is_bounded_safe_and_does_not_follow_symlinks(tmp_path: Path) -> None:
    root = tmp_path / "remote"
    first = root / "checkpoints" / "checkpoint-25" / "checkpoint-complete.json"
    first.parent.mkdir(parents=True)
    first.write_text("{}")
    second = root / "checkpoints" / "checkpoint-50" / "checkpoint-complete.json"
    second.parent.mkdir(parents=True)
    second.write_text("{}")

    discovered = LocalTransfer().discover(
        root,
        "checkpoints/checkpoint-*/checkpoint-complete.json",
        max_matches=2,
    )

    assert [entry.path for entry in discovered] == [
        "checkpoints/checkpoint-25/checkpoint-complete.json",
        "checkpoints/checkpoint-50/checkpoint-complete.json",
    ]
    with pytest.raises(TransferError, match="more than 1"):
        LocalTransfer().discover(
            root,
            "checkpoints/checkpoint-*/checkpoint-complete.json",
            max_matches=1,
        )
    first.unlink()
    first.symlink_to(tmp_path / "outside")
    with pytest.raises(TransferError, match="symlink"):
        LocalTransfer().discover(
            root,
            "checkpoints/checkpoint-*/checkpoint-complete.json",
            max_matches=2,
        )


def test_discovery_plan_uses_fixed_prefix_and_rejects_control_characters() -> None:
    plan = validate_discovery_pattern(
        "checkpoints/checkpoint-*/checkpoint-complete.json"
    )

    assert plan.fixed_prefix == "checkpoints"
    assert plan.relative_pattern == "checkpoint-*/checkpoint-complete.json"
    for unsafe in (
        "checkpoints/checkpoint-*\n/checkpoint-complete.json",
        "checkpoints/checkpoint-\x7f/checkpoint-complete.json",
    ):
        with pytest.raises(TransferError, match="control"):
            validate_discovery_pattern(unsafe)


def test_rclone_discovery_parses_bounded_json_and_rejects_symlink_notice(
    tmp_path: Path,
) -> None:
    calls: list[list[str]] = []
    known_hosts = tmp_path / "known_hosts"
    key = tmp_path / "id_ed25519"
    known_hosts.write_text("host ssh-ed25519 AAAA\n")
    key.write_text("key")

    def run(argv: list[str], **kwargs: Any) -> CompletedProcess[str]:
        calls.append(argv)
        return CompletedProcess(
            argv,
            0,
            stdout=(
                '[{"Path":"checkpoints/checkpoint-25/checkpoint-complete.json",'
                '"Size":123,"ModTime":"2026-07-31T12:00:00Z","IsDir":false}]'
            ),
            stderr="",
        )

    transfer = RcloneSFTP(
        host="example",
        port=2222,
        user="root",
        key_file=key,
        known_hosts_file=known_hosts,
        run_command=run,
    )

    found = transfer.discover(
        "/workspace",
        "checkpoints/checkpoint-*/checkpoint-complete.json",
        max_matches=8,
    )

    assert found == (
        RemoteFile(
            path="checkpoints/checkpoint-25/checkpoint-complete.json",
            size=123,
            modified_at="2026-07-31T12:00:00Z",
        ),
    )
    assert calls[0][:2] == ["rclone", "lsjson"]
    assert "--include" in calls[0]
    assert "--max-depth" in calls[0]
    assert "--sftp-skip-links" in calls[0]

    def symlink_run(argv: list[str], **kwargs: Any) -> CompletedProcess[str]:
        return CompletedProcess(argv, 0, stdout="[]", stderr="NOTICE: cannot follow symlink")

    unsafe = RcloneSFTP(
        host="example",
        port=2222,
        user="root",
        key_file=key,
        known_hosts_file=known_hosts,
        run_command=symlink_run,
    )
    with pytest.raises(TransferError, match="symlink"):
        unsafe.discover(
            "/workspace",
            "checkpoints/checkpoint-*/checkpoint-complete.json",
            max_matches=8,
        )


def test_rclone_list_failure_is_distinctly_retryable(tmp_path: Path) -> None:
    known_hosts = tmp_path / "known_hosts"
    known_hosts.write_text("host ssh-ed25519 AAAA\n")

    def run(argv: list[str], **kwargs: Any) -> CompletedProcess[str]:
        return CompletedProcess(argv, 255, stdout="", stderr="connection closed")

    transfer = RcloneSFTP(
        host="example",
        port=2222,
        user="root",
        key_file=tmp_path / "key",
        known_hosts_file=known_hosts,
        run_command=run,
    )
    with pytest.raises(TransferUnavailable, match="outcome unknown"):
        transfer.discover(
            "/workspace",
            "checkpoints/checkpoint-*/checkpoint-complete.json",
            max_matches=8,
        )


def test_rclone_discovery_timeout_is_bounded_and_retryable(tmp_path: Path) -> None:
    known_hosts = tmp_path / "known_hosts"
    known_hosts.write_text("host ssh-ed25519 AAAA\n")
    observed: dict[str, object] = {}

    def run(argv: list[str], **kwargs: Any) -> CompletedProcess[str]:
        observed.update(kwargs)
        raise TimeoutExpired(argv, kwargs["timeout"])

    transfer = RcloneSFTP(
        host="example",
        port=2222,
        user="root",
        key_file=tmp_path / "key",
        known_hosts_file=known_hosts,
        run_command=run,
    )

    with pytest.raises(TransferUnavailable, match="timed out"):
        transfer.discover(
            "/workspace/checkpoints",
            "checkpoint-*/checkpoint-complete.json",
            max_matches=8,
        )
    assert observed["timeout"] == 60


def test_rclone_missing_fixed_discovery_subtree_is_an_empty_listing(tmp_path: Path) -> None:
    known_hosts = tmp_path / "known_hosts"
    known_hosts.write_text("host ssh-ed25519 AAAA\n")

    def run(argv: list[str], **kwargs: Any) -> CompletedProcess[str]:
        return CompletedProcess(
            argv,
            3,
            stdout="",
            stderr=(
                "2026/08/02 00:10:58 ERROR : : error listing: directory not found\n"
                "2026/08/02 00:10:58 Failed to lsjson with 2 errors: "
                "last error was: error in ListJSON: directory not found\n"
            ),
        )

    transfer = RcloneSFTP(
        host="example",
        port=2222,
        user="root",
        key_file=tmp_path / "key",
        known_hosts_file=known_hosts,
        run_command=run,
    )

    assert (
        transfer.discover(
            "/workspace/checkpoints",
            "checkpoint-*/checkpoint-complete.json",
            max_matches=8,
        )
        == ()
    )
