"""Exact-manifest transfer adapters."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from subprocess import CompletedProcess, TimeoutExpired
from typing import cast


class TransferError(RuntimeError):
    """A transfer was unsafe, incomplete, or had an unknown outcome."""


class TransferUnavailable(TransferError):
    """The authenticated transfer channel failed without changing workload truth."""


@dataclass(frozen=True)
class TransferReceipt:
    files: int
    bytes: int


@dataclass(frozen=True, order=True)
class RemoteFile:
    """One bounded, regular-file discovery result relative to a remote root."""

    path: str
    size: int
    modified_at: str


@dataclass(frozen=True)
class ContentReceipt:
    """Content identity established after fetching one discovered remote file."""

    path: str
    size: int
    sha256: str


@dataclass(frozen=True)
class DiscoveryPattern:
    """A validated glob split at its longest fixed directory prefix."""

    fixed_prefix: str
    relative_pattern: str


RunCommand = Callable[..., CompletedProcess[str]]
_DISCOVERY_TIMEOUT_SECONDS = 60


def validate_discovery_pattern(pattern: str) -> DiscoveryPattern:
    """Validate a bounded glob and isolate its fixed discovery subtree."""

    _discovery_matcher(pattern)
    parts = PurePosixPath(pattern).parts
    wildcard_index = next(
        (index for index, part in enumerate(parts) if "*" in part),
        len(parts) - 1,
    )
    return DiscoveryPattern(
        fixed_prefix="/".join(parts[:wildcard_index]),
        relative_pattern="/".join(parts[wildcard_index:]),
    )


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

    def publish_atomic(
        self,
        source_file: Path,
        destination_file: Path | str,
        *,
        size: int,
        sha256: str,
    ) -> TransferReceipt:
        """Publish one verified file with a same-directory atomic rename."""

        before = _validated_atomic_source(source_file, size=size, sha256=sha256)
        destination = Path(destination_file)
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if destination.is_symlink():
            raise TransferError("atomic publication destination is a symlink")
        temporary = destination.with_name(
            f".{destination.name}.partial-{uuid.uuid4().hex}"
        )
        try:
            shutil.copyfile(source_file, temporary)
            after = source_file.stat(follow_symlinks=False)
            if _file_identity(before) != _file_identity(after):
                raise TransferUnavailable("atomic publication source changed during transfer")
            if temporary.stat().st_size != size or _sha256(temporary) != sha256:
                raise TransferError("atomic publication staging verification failed")
            descriptor = os.open(temporary, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            temporary.replace(destination)
            _fsync_directory(destination.parent)
        finally:
            temporary.unlink(missing_ok=True)
        return TransferReceipt(files=1, bytes=size)

    def discover(
        self,
        source_root: Path | str,
        pattern: str,
        *,
        max_matches: int,
    ) -> tuple[RemoteFile, ...]:
        matcher = _discovery_matcher(pattern)
        limit = _positive_discovery_limit(max_matches)
        root = Path(source_root).resolve()
        if not root.is_dir():
            return ()
        discovered: list[RemoteFile] = []
        for candidate in root.rglob("*"):
            relative = candidate.relative_to(root).as_posix()
            if not matcher.fullmatch(relative):
                continue
            if candidate.is_symlink():
                raise TransferError(f"discovered path is a symlink: {relative}")
            if not candidate.is_file():
                raise TransferError(f"discovered path is not a regular file: {relative}")
            metadata = candidate.stat()
            discovered.append(
                RemoteFile(
                    path=_safe_remote_relative(relative).as_posix(),
                    size=metadata.st_size,
                    modified_at=str(metadata.st_mtime_ns),
                )
            )
            if len(discovered) > limit:
                raise TransferError(f"discovery returned more than {limit} matches")
        return tuple(sorted(discovered))

    def fetch(
        self,
        source_root: Path | str,
        destination_root: Path,
        remote_file: RemoteFile,
    ) -> ContentReceipt:
        source_root_path = Path(source_root).resolve()
        relative = _validated_remote_file(remote_file)
        source = source_root_path.joinpath(*relative.parts)
        if source.is_symlink() or any(
            parent.is_symlink() for parent in source.parents if parent != source_root_path
        ):
            raise TransferError(f"discovered path is a symlink: {relative.as_posix()}")
        try:
            source.relative_to(source_root_path)
        except ValueError:
            raise TransferError("discovered path escapes source root") from None
        before = source.stat()
        if not source.is_file() or before.st_size != remote_file.size:
            raise TransferError(f"discovered file size changed: {relative.as_posix()}")
        target = destination_root.joinpath(*relative.parts)
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.partial-{os.getpid()}")
        shutil.copyfile(source, temporary)
        after = source.stat()
        if (
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            temporary.unlink(missing_ok=True)
            raise TransferUnavailable("discovered file changed during transfer")
        actual_hash = _sha256(temporary)
        temporary.replace(target)
        return ContentReceipt(path=relative.as_posix(), size=remote_file.size, sha256=actual_hash)


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
                raise TransferUnavailable("rclone SFTP outcome unknown; reconciliation required")
        finally:
            if file_list is not None:
                Path(file_list).unlink(missing_ok=True)
        return _receipt(entries)

    def publish_atomic(
        self,
        source_file: Path,
        destination_file: Path | str,
        *,
        size: int,
        sha256: str,
    ) -> TransferReceipt:
        """Stage one verified file, then expose it with a server-side rename."""

        before = _validated_atomic_source(source_file, size=size, sha256=sha256)
        destination = _safe_remote_absolute_file(destination_file)
        staging = destination.with_name(
            f".{destination.name}.partial-{uuid.uuid4().hex}"
        )
        copy_argv = [
            "rclone",
            "copyto",
            str(source_file.resolve()),
            f":sftp:{staging.as_posix()}",
            *self._connection_arguments(),
            "--checksum",
        ]
        copied = self._run(copy_argv, check=False, text=True, capture_output=True)
        if copied.returncode != 0:
            raise TransferUnavailable("atomic SFTP staging outcome unknown; retry required")
        try:
            after = source_file.stat(follow_symlinks=False)
        except OSError as error:
            raise TransferUnavailable(
                "atomic publication source changed during transfer"
            ) from error
        if _file_identity(before) != _file_identity(after):
            raise TransferUnavailable("atomic publication source changed during transfer")
        move_argv = [
            "rclone",
            "moveto",
            f":sftp:{staging.as_posix()}",
            f":sftp:{destination.as_posix()}",
            *self._connection_arguments(),
        ]
        moved = self._run(move_argv, check=False, text=True, capture_output=True)
        if moved.returncode != 0:
            raise TransferUnavailable("atomic SFTP publication outcome unknown; reconcile status")
        return TransferReceipt(files=1, bytes=size)

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
                "--sftp-skip-links",
            ]
            result = self._run(argv, check=False, text=True, capture_output=True)
            if result.returncode != 0:
                raise TransferUnavailable("rclone SFTP outcome unknown; reconciliation required")
        finally:
            if file_list is not None:
                Path(file_list).unlink(missing_ok=True)
        received = _validated_entries(destination_root, manifest)
        return _receipt(received)

    def discover(
        self,
        source_root: Path | str,
        pattern: str,
        *,
        max_matches: int,
    ) -> tuple[RemoteFile, ...]:
        matcher = _discovery_matcher(pattern)
        limit = _positive_discovery_limit(max_matches)
        argv = [
            "rclone",
            "lsjson",
            f":sftp:{PurePosixPath(str(source_root)).as_posix()}",
            *self._connection_arguments(),
            "--recursive",
            "--files-only",
            "--sftp-skip-links",
            "--include",
            pattern,
            "--max-depth",
            "32",
        ]
        try:
            result = self._run(
                argv,
                check=False,
                text=True,
                capture_output=True,
                timeout=_DISCOVERY_TIMEOUT_SECONDS,
            )
        except TimeoutExpired as error:
            raise TransferUnavailable(
                "rclone SFTP listing timed out; retry required"
            ) from error
        if result.returncode != 0:
            # Rclone reserves exit status 3 for a missing directory. An
            # incremental checkpoint subtree legitimately does not exist
            # before the first completed checkpoint, so this is an empty
            # discovery result rather than a retryable transport failure.
            if result.returncode == 3:
                return ()
            raise TransferUnavailable("rclone SFTP listing outcome unknown; retry required")
        if "symlink" in result.stderr.lower():
            raise TransferError("SFTP discovery encountered a symlink")
        if len(result.stdout.encode("utf-8")) > 4 * 1024 * 1024:
            raise TransferError("SFTP discovery response exceeds the bounded listing size")
        try:
            raw: object = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise TransferError("SFTP discovery returned invalid JSON") from error
        if not isinstance(raw, list):
            raise TransferError("SFTP discovery did not return an array")
        discovered: list[RemoteFile] = []
        seen: set[str] = set()
        for item in cast(list[object], raw):
            if not isinstance(item, Mapping):
                raise TransferError("SFTP discovery entry is malformed")
            raw_mapping = cast(Mapping[object, object], item)
            if not all(isinstance(key, str) for key in raw_mapping):
                raise TransferError("SFTP discovery entry is malformed")
            mapping = cast(Mapping[str, object], raw_mapping)
            value = mapping.get("Path")
            size = mapping.get("Size")
            modified_at = mapping.get("ModTime")
            if (
                not isinstance(value, str)
                or isinstance(size, bool)
                or not isinstance(size, int)
                or size < 0
                or not isinstance(modified_at, str)
                or not modified_at
                or mapping.get("IsDir") is not False
            ):
                raise TransferError("SFTP discovery entry is malformed")
            relative = _safe_remote_relative(value).as_posix()
            if not matcher.fullmatch(relative):
                raise TransferError("SFTP discovery returned a path outside the fixed glob")
            if relative in seen:
                raise TransferError(f"SFTP discovery returned duplicate path: {relative}")
            seen.add(relative)
            discovered.append(RemoteFile(path=relative, size=size, modified_at=modified_at))
            if len(discovered) > limit:
                raise TransferError(f"discovery returned more than {limit} matches")
        return tuple(sorted(discovered))

    def fetch(
        self,
        source_root: Path | str,
        destination_root: Path,
        remote_file: RemoteFile,
    ) -> ContentReceipt:
        relative = _validated_remote_file(remote_file)
        destination_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix=".runpod-jobrunner-fetch-", dir=destination_root.parent
        ) as staging_text:
            staging = Path(staging_text)
            file_list: str | None = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    prefix="runpod-jobrunner-files-",
                    delete=False,
                ) as handle:
                    file_list = handle.name
                    handle.write(f"{relative.as_posix()}\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                argv = [
                    "rclone",
                    "copy",
                    f":sftp:{PurePosixPath(str(source_root)).as_posix()}",
                    str(staging.resolve()),
                    *self._connection_arguments(),
                    "--files-from-raw",
                    file_list,
                    "--sftp-skip-links",
                ]
                result = self._run(argv, check=False, text=True, capture_output=True)
                if result.returncode != 0:
                    raise TransferUnavailable("rclone SFTP fetch outcome unknown; retry required")
            finally:
                if file_list is not None:
                    Path(file_list).unlink(missing_ok=True)
            source = staging.joinpath(*relative.parts)
            if source.is_symlink() or not source.is_file():
                raise TransferError(
                    f"discovered file is absent or a symlink: {relative.as_posix()}"
                )
            if source.stat().st_size != remote_file.size:
                raise TransferError(f"discovered file size changed: {relative.as_posix()}")
            actual_hash = _sha256(source)
            target = destination_root.joinpath(*relative.parts)
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            source.replace(target)
        return ContentReceipt(path=relative.as_posix(), size=remote_file.size, sha256=actual_hash)

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


def _positive_discovery_limit(value: int) -> int:
    if isinstance(value, bool) or not 0 < value <= 4096:
        raise TransferError("discovery max_matches must be between 1 and 4096")
    return value


def _discovery_matcher(pattern: str) -> re.Pattern[str]:
    if not pattern or len(pattern) > 512:
        raise TransferError("incremental manifest glob is invalid")
    if "**" in pattern or any(character in pattern for character in "?[]{}\\\x00"):
        raise TransferError("incremental manifest glob contains unsupported metacharacters")
    if any(ord(character) < 32 or ord(character) == 127 for character in pattern):
        raise TransferError("incremental manifest glob contains control characters")
    path = PurePosixPath(pattern)
    if (
        path.is_absolute()
        or path.as_posix() != pattern
        or any(part in {"", ".", ".."} for part in path.parts)
        or not path.parts
        or "*" in path.name
        or len(path.parts) > 32
        or sum(part.count("*") for part in path.parts) > 4
    ):
        raise TransferError("incremental manifest glob is not a safe fixed glob")
    components = [re.escape(part).replace(r"\*", "[^/]*") for part in path.parts]
    return re.compile("/".join(components))


def _safe_remote_relative(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        not value
        or len(value) > 1024
        or path.is_absolute()
        or path.as_posix() != value
        or "\\" in value
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
        or any(part in {"", ".", ".."} or len(part) > 255 for part in path.parts)
    ):
        raise TransferError(f"unsafe remote path: {value!r}")
    return path


def _safe_remote_absolute_file(value: Path | str) -> PurePosixPath:
    text = str(value)
    path = PurePosixPath(text)
    if (
        not text
        or len(text) > 1024
        or not path.is_absolute()
        or path.as_posix() != text
        or "\\" in text
        or any(ord(character) < 32 or ord(character) == 127 for character in text)
        or not path.name
        or any(part in {"", ".", ".."} or len(part) > 255 for part in path.parts[1:])
    ):
        raise TransferError(f"unsafe remote publication path: {text!r}")
    return path


def _validated_atomic_source(
    source: Path, *, size: int, sha256: str
) -> os.stat_result:
    if (
        isinstance(size, bool)
        or size < 0
        or re.fullmatch(r"[0-9a-f]{64}", sha256) is None
    ):
        raise TransferError("atomic publication receipt is malformed")
    try:
        metadata = source.stat(follow_symlinks=False)
    except OSError as error:
        raise TransferError("atomic publication source is unavailable") from error
    if source.is_symlink() or not stat.S_ISREG(metadata.st_mode):
        raise TransferError("atomic publication source is not a regular file")
    if metadata.st_size != size:
        raise TransferError("atomic publication source size mismatch")
    if _sha256(source) != sha256:
        raise TransferError("atomic publication source hash mismatch")
    return metadata


def _file_identity(metadata: os.stat_result) -> tuple[int, int, int, int]:
    return (
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
        metadata.st_ino,
    )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _validated_remote_file(remote_file: RemoteFile) -> PurePosixPath:
    if remote_file.size < 0 or not remote_file.modified_at:
        raise TransferError("discovered remote file record is malformed")
    return _safe_remote_relative(remote_file.path)


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
