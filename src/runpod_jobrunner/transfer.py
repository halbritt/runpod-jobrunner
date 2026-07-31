"""Exact-manifest transfer adapters."""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from subprocess import CompletedProcess


class TransferError(RuntimeError):
    """A transfer was unsafe, incomplete, or had an unknown outcome."""


@dataclass(frozen=True)
class TransferReceipt:
    files: int
    bytes: int


RunCommand = Callable[..., CompletedProcess[str]]


class LocalTransfer:
    """Filesystem adapter used by the real local vertical slice."""

    def upload(
        self,
        source_root: Path,
        destination_root: Path | str,
        manifest: Sequence[Mapping[str, object]],
    ) -> TransferReceipt:
        destination = Path(destination_root)
        entries = _validated_entries(source_root, manifest)
        for relative, source, _size, expected_hash in entries:
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(f".{target.name}.partial-{os.getpid()}")
            shutil.copyfile(source, temporary)
            if _sha256(temporary) != expected_hash:
                temporary.unlink(missing_ok=True)
                raise TransferError(f"copied file hash mismatch: {relative}")
            temporary.replace(target)
        return _receipt(entries)


class RcloneSFTP:
    """SFTP bulk transfer with explicit host-key verification."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        user: str,
        key_file: Path,
        known_hosts_file: Path,
        run_command: RunCommand = subprocess.run,
    ) -> None:
        if not known_hosts_file.is_file():
            raise TransferError("a run-scoped known-hosts file is required")
        self._host = host
        self._port = port
        self._user = user
        self._key = key_file.resolve()
        self._known_hosts = known_hosts_file.resolve()
        self._run = run_command

    def upload(
        self,
        source_root: Path,
        destination_root: Path | str,
        manifest: Sequence[Mapping[str, object]],
    ) -> TransferReceipt:
        entries = _validated_entries(source_root, manifest)
        file_list: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", prefix="runpod-jobrunner-files-", delete=False
            ) as handle:
                file_list = handle.name
                for relative, _source, _size, _hash in entries:
                    handle.write(f"{relative.as_posix()}\n")
                handle.flush()
                os.fsync(handle.fileno())
            argv = [
                "rclone",
                "copy",
                str(source_root.resolve()),
                f":sftp:{PurePosixPath(str(destination_root)).as_posix()}",
                "--sftp-host",
                self._host,
                "--sftp-port",
                str(self._port),
                "--sftp-user",
                self._user,
                "--sftp-key-file",
                str(self._key),
                "--sftp-known-hosts-file",
                str(self._known_hosts),
                "--files-from-raw",
                file_list,
                "--checksum",
            ]
            result = self._run(argv, check=False, text=True, capture_output=True)
            if result.returncode != 0:
                raise TransferError("rclone SFTP outcome unknown; reconciliation required")
        finally:
            if file_list is not None:
                Path(file_list).unlink(missing_ok=True)
        return _receipt(entries)

    def download(
        self,
        source_root: Path | str,
        destination_root: Path,
        manifest: Sequence[Mapping[str, object]],
    ) -> TransferReceipt:
        declared = _validated_manifest_entries(manifest)
        destination_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        file_list: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", prefix="runpod-jobrunner-files-", delete=False
            ) as handle:
                file_list = handle.name
                for relative, _size, _hash in declared:
                    handle.write(f"{relative.as_posix()}\n")
                handle.flush()
                os.fsync(handle.fileno())
            argv = [
                "rclone",
                "copy",
                f":sftp:{PurePosixPath(str(source_root)).as_posix()}",
                str(destination_root.resolve()),
                *self._connection_arguments(),
                "--files-from-raw",
                file_list,
            ]
            result = self._run(argv, check=False, text=True, capture_output=True)
            if result.returncode != 0:
                raise TransferError("rclone SFTP outcome unknown; reconciliation required")
        finally:
            if file_list is not None:
                Path(file_list).unlink(missing_ok=True)
        received = _validated_entries(destination_root, manifest)
        return _receipt(received)

    def _connection_arguments(self) -> list[str]:
        return [
            "--sftp-host",
            self._host,
            "--sftp-port",
            str(self._port),
            "--sftp-user",
            self._user,
            "--sftp-key-file",
            str(self._key),
            "--sftp-known-hosts-file",
            str(self._known_hosts),
        ]


Entry = tuple[PurePosixPath, Path, int, str]
DeclaredEntry = tuple[PurePosixPath, int, str]


def _validated_entries(source_root: Path, manifest: Sequence[Mapping[str, object]]) -> list[Entry]:
    root = source_root.resolve()
    entries: list[Entry] = []
    for relative, size, expected_hash in _validated_manifest_entries(manifest):
        value = relative.as_posix()
        source = root.joinpath(*relative.parts)
        if source.is_symlink() or any(part.is_symlink() for part in source.parents if part != root):
            raise TransferError(f"manifest path is a symlink: {value}")
        try:
            source.relative_to(root)
        except ValueError:
            raise TransferError(f"manifest path escapes source root: {value}") from None
        if not source.is_file():
            raise TransferError(f"manifest path is not a regular file: {value}")
        if source.stat().st_size != size:
            raise TransferError(f"manifest size mismatch: {value}")
        if _sha256(source) != expected_hash:
            raise TransferError(f"manifest hash mismatch: {value}")
        entries.append((relative, source, size, expected_hash))
    return entries


def _validated_manifest_entries(
    manifest: Sequence[Mapping[str, object]],
) -> list[DeclaredEntry]:
    entries: list[DeclaredEntry] = []
    seen: set[str] = set()
    for raw in manifest:
        value = raw.get("path")
        size = raw.get("size")
        expected_hash = raw.get("sha256")
        if (
            not isinstance(value, str)
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
            or not isinstance(expected_hash, str)
            or len(expected_hash) != 64
            or any(character not in "0123456789abcdef" for character in expected_hash)
        ):
            raise TransferError("manifest entry is malformed")
        relative = PurePosixPath(value)
        if (
            relative.is_absolute()
            or not value
            or ".." in relative.parts
            or "." in relative.parts
            or relative.as_posix() != value
        ):
            raise TransferError(f"unsafe manifest path: {value}")
        if value in seen:
            raise TransferError(f"duplicate manifest path: {value}")
        seen.add(value)
        entries.append((relative, size, expected_hash))
    return entries


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _receipt(entries: Sequence[Entry]) -> TransferReceipt:
    return TransferReceipt(files=len(entries), bytes=sum(entry[2] for entry in entries))
