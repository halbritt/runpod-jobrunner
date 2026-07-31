"""Durable, run-scoped state storage.

The append-only journal is authoritative. ``state.json`` is a replace-atomic
projection for cheap reads; if a process dies between the journal fsync and the
projection replace, the latest journal projection is used on recovery.
"""

from __future__ import annotations

import copy
import fcntl
import json
import os
import re
import tempfile
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from runpod_jobrunner.protocol import ProtocolValidationError, validate_protocol

_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")


class RunStoreError(RuntimeError):
    """Base class for durable-state errors."""


class InvalidRunIdError(RunStoreError, ValueError):
    """The supplied run ID is unsafe as a directory name."""


class RunNotFoundError(RunStoreError, FileNotFoundError):
    """No durable state exists for the run."""


class RunAlreadyExistsError(RunStoreError):
    """A stable run ID was reused with a different request."""


@dataclass(frozen=True)
class RunPaths:
    """Filesystem paths owned by one run."""

    directory: Path
    request: Path
    state: Path
    events: Path
    provider: Path
    remote_status: Path
    artifact_manifest: Path
    receipts: Path
    closeout_receipt: Path
    lock: Path


class RunStore:
    """Own XDG state and serialize mutations with a run-scoped flock."""

    def __init__(self, runs_root: Path | None = None) -> None:
        if runs_root is None:
            xdg_state = os.environ.get("XDG_STATE_HOME")
            state_home = Path(xdg_state) if xdg_state else Path.home() / ".local" / "state"
            runs_root = state_home / "runpod-jobrunner" / "runs"
        self.runs_root = Path(runs_root)

    def paths(self, run_id: str) -> RunPaths:
        if run_id in {".", ".."} or not _RUN_ID.fullmatch(run_id):
            raise InvalidRunIdError(f"invalid run ID: {run_id!r}")
        directory = self.runs_root / run_id
        return RunPaths(
            directory=directory,
            request=directory / "request.json",
            state=directory / "state.json",
            events=directory / "events.jsonl",
            provider=directory / "provider.json",
            remote_status=directory / "remote-status.json",
            artifact_manifest=directory / "artifact-manifest.json",
            receipts=directory / "receipts",
            closeout_receipt=directory / "receipts" / "closeout-receipt.json",
            lock=directory / ".lock",
        )

    def create_run(
        self, run_id: str, request: Mapping[str, object], initial_state: Mapping[str, object]
    ) -> dict[str, Any]:
        """Create a run idempotently, rejecting request changes for the same ID."""

        with self.transaction(run_id) as transaction:
            paths = transaction.paths
            if paths.request.exists():
                existing_request = _read_json_object(paths.request)
                if existing_request != dict(request):
                    raise RunAlreadyExistsError(
                        f"run {run_id!r} already exists with a different request"
                    )
                try:
                    return transaction.current_state()
                except RunNotFoundError:
                    # A crash after writing request.json but before the first event is safe
                    # to resume with the same immutable request.
                    pass
            else:
                _atomic_write_json(paths.request, request)
            return transaction.commit_state("run_planned", initial_state)

    def read_request(self, run_id: str) -> dict[str, Any]:
        paths = self.paths(run_id)
        if not paths.request.exists():
            raise RunNotFoundError(f"run {run_id!r} has no request")
        return _read_json_object(paths.request)

    def read_state(self, run_id: str) -> dict[str, Any]:
        """Read the effective projection, including a journal entry newer than state.json."""

        paths = self.paths(run_id)
        with _read_lock(paths):
            state = _effective_state(paths)
            if state.get("lifecycle") == "closed":
                _ensure_closeout_receipt(paths, state)
            return state

    def read_closeout_receipt(self, run_id: str) -> dict[str, Any]:
        """Read and validate the durable receipt for a terminal closed run."""

        state = self.read_state(run_id)
        if state.get("lifecycle") != "closed":
            raise RunStoreError(f"run {run_id!r} is not closed")
        return _read_and_validate_closeout_receipt(self.paths(run_id).closeout_receipt)

    def read_events(self, run_id: str) -> list[dict[str, Any]]:
        paths = self.paths(run_id)
        with _read_lock(paths):
            return _read_events(paths.events)

    def active_run_ids(self) -> tuple[str, ...]:
        if not self.runs_root.exists():
            return ()
        active: list[str] = []
        for directory in sorted(self.runs_root.iterdir()):
            if not directory.is_dir() or directory.name.startswith("."):
                continue
            state = self.read_state(directory.name)
            if state.get("lifecycle") != "closed":
                active.append(directory.name)
        return tuple(active)

    @contextmanager
    def admission(self) -> Generator[None, None, None]:
        """Serialize the default one-active-run admission decision."""

        _ensure_directory(self.runs_root)
        lock_path = self.runs_root / ".admission.lock"
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    @contextmanager
    def transaction(self, run_id: str) -> Generator[RunTransaction, None, None]:
        paths = self.paths(run_id)
        _ensure_directory(paths.directory)
        lock_fd = os.open(paths.lock, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            yield RunTransaction(paths)
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)


class RunTransaction:
    """Mutations available while the caller holds one run's exclusive lock."""

    def __init__(self, paths: RunPaths) -> None:
        self.paths = paths

    def current_state(self) -> dict[str, Any]:
        return _effective_state(self.paths)

    def commit_state(
        self,
        kind: str,
        state: Mapping[str, object],
        payload: Mapping[str, object] | None = None,
    ) -> dict[str, Any]:
        """Fsync an event first, then replace and directory-fsync the projection."""

        events = _read_events(self.paths.events)
        sequence = events[-1]["sequence"] + 1 if events else 1
        projection = copy.deepcopy(dict(state))
        projection["event_sequence"] = sequence
        receipt = (
            _make_closeout_receipt(projection)
            if projection.get("lifecycle") == "closed"
            else None
        )
        event: dict[str, object] = {
            "protocol": "run-event/1",
            "sequence": sequence,
            "recorded_at": datetime.now(UTC).isoformat(),
            "kind": kind,
            "payload": copy.deepcopy(dict(payload or {})),
            "projection": projection,
        }
        _append_json_line(self.paths.events, event)
        _atomic_write_json(self.paths.state, projection)
        if receipt is not None:
            _atomic_write_json(self.paths.closeout_receipt, receipt)
        return projection


def _ensure_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)


@contextmanager
def _read_lock(paths: RunPaths) -> Generator[None, None, None]:
    """Exclude appenders while a reader may repair a torn journal tail."""

    if not paths.directory.exists():
        yield
        return
    descriptor = os.open(paths.lock, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _json_bytes(value: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode()


def _atomic_write_json(path: Path, value: Mapping[str, object]) -> None:
    _ensure_directory(path.parent)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(fd, 0o600)
        data = _json_bytes(value)
        offset = 0
        while offset < len(data):
            offset += os.write(fd, data[offset:])
        os.fsync(fd)
        os.close(fd)
        fd = -1
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if fd >= 0:
            os.close(fd)
        temporary.unlink(missing_ok=True)


def _append_json_line(path: Path, value: Mapping[str, object]) -> None:
    _ensure_directory(path.parent)
    data = _json_bytes(value)
    fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        offset = 0
        while offset < len(data):
            offset += os.write(fd, data[offset:])
        os.fsync(fd)
    finally:
        os.close(fd)
    # The directory fsync matters on the first append, when events.jsonl itself is new.
    _fsync_directory(path.parent)


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_fd = os.open(path, flags)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        value = cast(object, json.loads(path.read_text()))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"corrupt JSON state: {path}") from error
    if not isinstance(value, dict):
        raise ValueError(f"JSON state must be an object: {path}")
    return cast(dict[str, Any], value)


def _make_closeout_receipt(state: Mapping[str, object]) -> dict[str, Any]:
    receipt = copy.deepcopy(dict(state))
    receipt["protocol"] = "closeout-receipt/1"
    validate_protocol(receipt, "closeout-receipt/1", subject="closeout receipt")
    return receipt


def _read_and_validate_closeout_receipt(path: Path) -> dict[str, Any]:
    receipt = _read_json_object(path)
    try:
        validate_protocol(receipt, "closeout-receipt/1", subject="closeout receipt")
    except ProtocolValidationError as error:
        raise RunStoreError(f"invalid durable closeout receipt: {path}: {error}") from error
    return receipt


def _ensure_closeout_receipt(paths: RunPaths, state: Mapping[str, object]) -> None:
    expected = _make_closeout_receipt(state)
    if not paths.closeout_receipt.exists():
        _atomic_write_json(paths.closeout_receipt, expected)
        return
    recorded = _read_and_validate_closeout_receipt(paths.closeout_receipt)
    if recorded != expected:
        raise RunStoreError(
            f"durable closeout receipt does not match closed state: {paths.directory}"
        )


def _read_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    try:
        journal_bytes = path.read_bytes()
    except OSError as error:
        raise ValueError(f"cannot read event journal: {path}") from error
    if journal_bytes and not journal_bytes.endswith(b"\n"):
        final_record_offset = journal_bytes.rfind(b"\n") + 1
        fragment = journal_bytes[final_record_offset:]
        if not _is_torn_json_fragment(fragment):
            raise ValueError(f"corrupt event journal: {path}")
        _truncate_and_sync(path, final_record_offset)
        journal_bytes = journal_bytes[:final_record_offset]
    try:
        journal = journal_bytes.decode()
    except UnicodeDecodeError as error:
        raise ValueError(f"corrupt event journal: {path}") from error
    lines = journal.splitlines()
    for expected_sequence, line in enumerate(lines, start=1):
        try:
            decoded = cast(object, json.loads(line))
        except json.JSONDecodeError as error:
            raise ValueError(f"corrupt event journal: {path}") from error
        if not isinstance(decoded, dict):
            raise ValueError(f"corrupt event journal sequence: {path}")
        event = cast(dict[str, Any], decoded)
        if event.get("sequence") != expected_sequence:
            raise ValueError(f"corrupt event journal sequence: {path}")
        projection_value = event.get("projection")
        if not isinstance(projection_value, dict):
            raise ValueError(f"corrupt event journal projection: {path}")
        projection = cast(dict[str, Any], projection_value)
        if projection.get("event_sequence") != expected_sequence:
            raise ValueError(f"corrupt event journal projection: {path}")
        events.append(event)
    return events


def _is_torn_json_fragment(fragment: bytes) -> bool:
    """Return true only when a non-terminated final JSON value ends mid-token."""

    try:
        text = fragment.decode()
    except UnicodeDecodeError as error:
        return error.end == len(fragment) and error.reason == "unexpected end of data"
    try:
        json.loads(text)
    except json.JSONDecodeError as error:
        if error.pos == len(text) or error.msg.startswith("Unterminated string"):
            return True
        suffix = text[error.pos:]
        if error.msg == "Expecting value" and (
            suffix == "-"
            or any(literal.startswith(suffix) for literal in ("true", "false", "null"))
        ):
            return True
        if error.msg == "Invalid \\uXXXX escape" and suffix.startswith("u"):
            return len(suffix) < 5
        return error.msg == "Expecting ',' delimiter" and suffix in {
            ".",
            "e",
            "E",
            "e+",
            "e-",
            "E+",
            "E-",
        }
    return False


def _truncate_and_sync(path: Path, length: int) -> None:
    descriptor = os.open(path, os.O_WRONLY)
    try:
        os.ftruncate(descriptor, length)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _fsync_directory(path.parent)


def _effective_state(paths: RunPaths) -> dict[str, Any]:
    events = _read_events(paths.events)
    snapshot: dict[str, Any] | None = None
    if paths.state.exists():
        snapshot = _read_json_object(paths.state)
    if events:
        projection = cast(dict[str, Any], events[-1]["projection"])
        snapshot_sequence = snapshot.get("event_sequence", 0) if snapshot is not None else 0
        if not isinstance(snapshot_sequence, int):
            raise ValueError(f"corrupt state projection sequence: {paths.state}")
        if snapshot is None or projection["event_sequence"] >= snapshot_sequence:
            return copy.deepcopy(projection)
    if snapshot is not None:
        return snapshot
    raise RunNotFoundError(f"run state does not exist: {paths.directory.name!r}")
