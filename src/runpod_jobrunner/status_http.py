"""Authenticated read-only HTTP projection of remote runner status."""

from __future__ import annotations

import hmac
import json
import threading
import time
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import cast

from runpod_jobrunner.runner import PHASE_ORDER

_STATES = {"ready", "running", "terminal"}
_OUTCOMES = {"succeeded", "failed", "stopped", "timed_out", "limit_exceeded"}
_REASONS = {
    "all_enabled_phases_completed",
    "phase_nonzero_exit",
    "phase_start_failed",
    "phase_timeout",
    "termination_requested",
    "controller_stop_requested",
    "elapsed_cap",
    "cost_cap",
    "storage_unavailable",
    "storage_capacity_below_request",
    "runner_restart_unknown_child",
    "artifact_manifest_missing",
    "artifact_manifest_invalid",
    "artifact_file_missing",
    "artifact_size_mismatch",
    "artifact_hash_mismatch",
}


class StatusHTTPServer:
    """Serve one run's public status projection without exposing runner internals."""

    def __init__(
        self,
        status_dir: Path | str,
        token: str,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
    ) -> None:
        if not token:
            raise ValueError("status token must not be empty")
        handler = _status_handler(Path(status_dir) / "status.json", token)
        self._server = ThreadingHTTPServer((host, port), handler)
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        return int(self._server.server_address[1])

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("status server already started")
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="runpod-jobrunner-status-http",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        if self._thread is None:
            return
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)
        self._thread = None


def _status_handler(status_path: Path, expected_token: str) -> type[BaseHTTPRequestHandler]:
    class RunStatusHandler(BaseHTTPRequestHandler):
        server_version = "runpod-jobrunner-status/1"
        sys_version = ""

        def do_GET(self) -> None:
            if self.path != "/status":
                self._write_json(404, {"error": "not_found"})
                return
            authorization = self.headers.get("Authorization", "")
            prefix = "Bearer "
            supplied_token = (
                authorization[len(prefix) :] if authorization.startswith(prefix) else ""
            )
            if not hmac.compare_digest(supplied_token, expected_token):
                self._write_json(401, {"error": "unauthorized"})
                return
            try:
                stored = json.loads(status_path.read_text())
                public = _public_status(stored)
            except (OSError, json.JSONDecodeError, StatusRecordError):
                self._write_json(503, {"error": "status_unavailable"})
                return
            self._write_json(200, public)

        def do_POST(self) -> None:
            self._write_json(405, {"error": "method_not_allowed"})

        do_PUT = do_POST
        do_PATCH = do_POST
        do_DELETE = do_POST

        def _write_json(self, status_code: int, record: Mapping[str, object]) -> None:
            encoded = (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode()
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.send_header("Cache-Control", "no-store")
            if status_code == 401:
                self.send_header("WWW-Authenticate", "Bearer")
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    return RunStatusHandler


class StatusRecordError(ValueError):
    """A status snapshot cannot be safely projected."""


def _public_status(candidate: object) -> dict[str, object]:
    status = _mapping(candidate)
    if status.get("protocol") != "run-status/1":
        raise StatusRecordError("unsupported status protocol")
    heartbeat_at = _number(status.get("heartbeat_at_unix"))
    state = _string(status.get("state"))
    if state not in _STATES:
        raise StatusRecordError("unknown runner state")
    phase = _optional_string(status.get("phase"))
    if phase is not None and phase not in PHASE_ORDER:
        raise StatusRecordError("unknown phase")
    public: dict[str, object] = {
        "protocol": "run-status/1",
        "run_id": _string(status.get("run_id")),
        "state": state,
        "phase": phase,
        "heartbeat_age_seconds": round(max(0.0, time.time() - heartbeat_at), 6),
        "heartbeat_monotonic_seconds": _number(status.get("heartbeat_monotonic_seconds")),
        "child": _public_child(status.get("child")),
        "latest_event_sequence": _integer(status.get("latest_event_sequence")),
        "terminal_result": _public_terminal(status.get("terminal_result")),
    }
    return public


def _public_child(candidate: object) -> dict[str, object] | None:
    if candidate is None:
        return None
    child = _mapping(candidate)
    running = child.get("running")
    if not isinstance(running, bool):
        raise StatusRecordError("invalid child status")
    exit_code = child.get("exit_code")
    if exit_code is not None:
        exit_code = _integer(exit_code)
    return {
        "pid": _integer(child.get("pid")),
        "running": running,
        "exit_code": exit_code,
    }


def _public_terminal(candidate: object) -> dict[str, object] | None:
    if candidate is None:
        return None
    terminal = _mapping(candidate)
    raw_completed_phases = terminal.get("completed_phases")
    if not isinstance(raw_completed_phases, list):
        raise StatusRecordError("invalid completed phases")
    completed_objects = cast(list[object], raw_completed_phases)
    if not all(isinstance(phase, str) for phase in completed_objects):
        raise StatusRecordError("invalid completed phases")
    completed_phases = cast(list[str], completed_objects)
    if any(phase not in PHASE_ORDER for phase in completed_phases):
        raise StatusRecordError("invalid completed phases")
    raw_exit_codes = _mapping(terminal.get("phase_exit_codes"))
    if any(phase not in PHASE_ORDER for phase in raw_exit_codes):
        raise StatusRecordError("invalid phase exit codes")
    exit_codes = {_string(phase): _integer(code) for phase, code in raw_exit_codes.items()}
    outcome = _string(terminal.get("outcome"))
    reason = _string(terminal.get("reason"))
    if outcome not in _OUTCOMES or reason not in _REASONS:
        raise StatusRecordError("unknown terminal result")
    phase = _optional_string(terminal.get("phase"))
    if phase is not None and phase not in PHASE_ORDER:
        raise StatusRecordError("unknown terminal phase")
    public: dict[str, object] = {
        "outcome": outcome,
        "reason": reason,
        "phase": phase,
        "elapsed_seconds": _number(terminal.get("elapsed_seconds")),
        "estimated_cost_usd": _decimal_string(terminal.get("estimated_cost_usd")),
        "completed_phases": completed_phases,
        "phase_exit_codes": exit_codes,
    }
    manifest_sha256 = terminal.get("artifact_manifest_sha256")
    manifest_size = terminal.get("artifact_manifest_size")
    if manifest_sha256 is not None or manifest_size is not None:
        if (
            not isinstance(manifest_sha256, str)
            or len(manifest_sha256) != 64
            or any(character not in "0123456789abcdef" for character in manifest_sha256)
            or isinstance(manifest_size, bool)
            or not isinstance(manifest_size, int)
            or manifest_size < 0
        ):
            raise StatusRecordError("invalid artifact manifest receipt")
        public["artifact_manifest_sha256"] = manifest_sha256
        public["artifact_manifest_size"] = manifest_size
    return public


def _mapping(candidate: object) -> dict[str, object]:
    if not isinstance(candidate, Mapping):
        raise StatusRecordError("expected object")
    untyped_mapping = cast(Mapping[object, object], candidate)
    if not all(isinstance(key, str) for key in untyped_mapping):
        raise StatusRecordError("object keys must be strings")
    return dict(cast(Mapping[str, object], untyped_mapping))


def _string(candidate: object) -> str:
    if not isinstance(candidate, str):
        raise StatusRecordError("expected string")
    return candidate


def _optional_string(candidate: object) -> str | None:
    if candidate is None:
        return None
    return _string(candidate)


def _integer(candidate: object) -> int:
    if isinstance(candidate, bool) or not isinstance(candidate, int):
        raise StatusRecordError("expected integer")
    return candidate


def _number(candidate: object) -> float:
    if isinstance(candidate, bool) or not isinstance(candidate, (int, float)):
        raise StatusRecordError("expected number")
    return float(candidate)


def _decimal_string(candidate: object) -> str:
    if not isinstance(candidate, str):
        raise StatusRecordError("expected decimal string")
    try:
        parsed = Decimal(candidate)
    except InvalidOperation as error:
        raise StatusRecordError("expected decimal string") from error
    if not parsed.is_finite() or parsed < 0:
        raise StatusRecordError("expected nonnegative decimal string")
    return candidate
