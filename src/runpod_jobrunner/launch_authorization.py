"""Run-scoped launch authorization kept separate from status authentication."""

from __future__ import annotations

import hashlib
import os
import re
import secrets
import stat
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import cast

LAUNCH_PROTOCOL = "launch-authorization/1"
LAUNCH_RELATIVE_PATH = "control/launch-authorization.token"
_TOKEN_PATTERN = re.compile(r"[0-9a-f]{64}\Z")


class LaunchAuthorizationError(ValueError):
    """A launch token or its pinned request contract is unsafe."""


@dataclass(frozen=True, slots=True)
class LaunchToken:
    """Controller-owned launch secret and its public request fields."""

    path: Path
    token: str = field(repr=False)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.token.encode("ascii")).hexdigest()

    @property
    def size(self) -> int:
        return len(self.token.encode("ascii")) + 1

    def request_fields(self, *, timeout_seconds: int) -> dict[str, object]:
        if timeout_seconds <= 0:
            raise LaunchAuthorizationError("launch authorization timeout must be positive")
        return {
            "protocol": LAUNCH_PROTOCOL,
            "path": LAUNCH_RELATIVE_PATH,
            "sha256": self.sha256,
            "size": self.size,
            "timeout_seconds": timeout_seconds,
        }


@dataclass(frozen=True, slots=True)
class LaunchAuthorization:
    """Validated public launch-token contract from one run request."""

    relative_path: PurePosixPath
    sha256: str
    size: int
    timeout_seconds: float


def ensure_launch_token(run_dir: Path, *, forbidden_tokens: tuple[str, ...] = ()) -> LaunchToken:
    """Create or load one durable, mode-0600 launch token for a run."""

    secrets_dir = run_dir / "secrets"
    secrets_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    if secrets_dir.is_symlink() or not secrets_dir.is_dir():
        raise LaunchAuthorizationError("launch authorization secret directory is unsafe")
    secrets_dir.chmod(0o700)
    path = secrets_dir / "launch-authorization.token"
    if path.exists():
        token = _read_token(path)
    else:
        token = secrets.token_hex(32)
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            _write_all(descriptor, f"{token}\n".encode("ascii"))
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        _fsync_directory(secrets_dir)
    if token in forbidden_tokens:
        raise LaunchAuthorizationError("launch authorization token is not independent")
    return LaunchToken(path=path, token=token)


def parse_launch_authorization(candidate: object) -> LaunchAuthorization:
    """Validate the versioned public launch-authorization request field."""

    if not isinstance(candidate, dict):
        raise LaunchAuthorizationError("launch_authorization must be an object")
    value = cast(dict[object, object], candidate)
    if any(not isinstance(key, str) for key in value):
        raise LaunchAuthorizationError("launch_authorization keys must be strings")
    record = cast(dict[str, object], value)
    if record.get("protocol") != LAUNCH_PROTOCOL:
        raise LaunchAuthorizationError("launch authorization protocol is unsupported")
    raw_path = record.get("path")
    if not isinstance(raw_path, str) or raw_path != LAUNCH_RELATIVE_PATH:
        raise LaunchAuthorizationError("launch authorization path is unsupported")
    relative = PurePosixPath(raw_path)
    expected = record.get("sha256")
    if not isinstance(expected, str) or re.fullmatch(r"[0-9a-f]{64}", expected) is None:
        raise LaunchAuthorizationError("launch authorization sha256 is invalid")
    size = record.get("size")
    if isinstance(size, bool) or not isinstance(size, int) or not 1 <= size <= 4096:
        raise LaunchAuthorizationError("launch authorization size is invalid")
    timeout = record.get("timeout_seconds")
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not 0 < float(timeout) <= 3600
    ):
        raise LaunchAuthorizationError("launch authorization timeout is invalid")
    return LaunchAuthorization(
        relative_path=relative,
        sha256=expected,
        size=size,
        timeout_seconds=float(timeout),
    )


def read_launch_token(path: Path) -> str:
    """Read one regular launch-token file without following a final symlink."""

    return _read_token(path)


def _read_token(path: Path) -> str:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise LaunchAuthorizationError("launch authorization token is unavailable") from error
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > 4096:
            raise LaunchAuthorizationError("launch authorization token is not a small regular file")
        data = b""
        while chunk := os.read(descriptor, 4096 - len(data) + 1):
            data += chunk
            if len(data) > 4096:
                raise LaunchAuthorizationError("launch authorization token is too large")
    finally:
        os.close(descriptor)
    try:
        text = data.decode("ascii")
    except UnicodeDecodeError as error:
        raise LaunchAuthorizationError("launch authorization token is not ASCII") from error
    if not text.endswith("\n") or text.count("\n") != 1:
        raise LaunchAuthorizationError("launch authorization token has invalid framing")
    token = text[:-1]
    if _TOKEN_PATTERN.fullmatch(token) is None:
        raise LaunchAuthorizationError("launch authorization token has invalid syntax")
    return token


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written == 0:
            raise OSError("short write while persisting launch token")
        offset += written


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
