"""Remote fixed-phase job runner."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import signal
import stat
import subprocess
import threading
import time
import uuid
from collections.abc import Mapping, Sequence
from contextlib import suppress
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import cast

from runpod_jobrunner.identity import (
    RunnerIdentity,
    RunnerIdentityError,
    load_runner_identity,
    local_source_identity,
    parse_protocol_majors,
    validate_git_commit,
    validate_runner_version,
)
from runpod_jobrunner.incremental_ack import ACK_NAMESPACE, ACK_PROTOCOL
from runpod_jobrunner.launch_authorization import (
    LaunchAuthorization,
    LaunchAuthorizationError,
    parse_launch_authorization,
    read_launch_token,
)

PHASE_ORDER = ("verify", "preflight", "train", "evaluate", "package")
_RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_BUNDLE_HASH_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_IMAGE_DIGEST_PATTERN = re.compile(r"[^@\s]+@sha256:[0-9a-f]{64}\Z")
_MONEY_PATTERN = re.compile(r"(?:0\.[0-9]*[1-9][0-9]*|[1-9][0-9]*(?:\.[0-9]+)?)\Z")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_STOP_REASONS = {"termination_requested", "controller_stop_requested"}
_MAX_MANIFEST_BYTES = 16 * 1024 * 1024
_PHASE_ENVIRONMENT_NAMES = {
    "HOME",
    "LANG",
    "LC_ALL",
    "LD_LIBRARY_PATH",
    "PATH",
    "PYTHONHOME",
    "PYTHONPATH",
    "TMP",
    "TMPDIR",
    "XDG_CACHE_HOME",
}
_PHASE_ENVIRONMENT_PREFIXES = (
    "ACCELERATE_",
    "CUBLAS_",
    "CUDA_",
    "CUDNN_",
    "FLASH_ATTENTION_",
    "LIGER_",
    "MKL_",
    "NCCL_",
    "NVIDIA_",
    "OMP_",
    "PEFT_",
    "PYTHON",
    "PYTORCH_",
    "TILELANG_",
    "TOKENIZERS_",
    "TORCH_",
    "TRANSFORMERS_",
    "TRITON_",
    "TVM_",
)


class RequestError(ValueError):
    """The remote run request violates the protocol contract."""


class ArtifactVerificationError(ValueError):
    """A declared artifact manifest or one of its files is not trustworthy."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


class RemoteRunner:
    """Execute one validated request and publish its durable process truth."""

    def __init__(
        self,
        request: Mapping[str, object],
        status_dir: Path | str,
        *,
        request_path: Path | str | None = None,
        runner_identity: RunnerIdentity | None = None,
        status_token_sha256: str | None = None,
    ) -> None:
        self.request = _validate_request(request)
        self.launch_authorization = parse_launch_authorization(
            self.request["launch_authorization"]
        )
        self._status_token_sha256 = status_token_sha256
        if runner_identity is not None:
            self.identity = runner_identity
            self._identity_failure = _identity_failure(self.request, self.identity)
        else:
            try:
                self.identity = load_runner_identity()
            except RunnerIdentityError:
                self.identity = local_source_identity()
                self._identity_failure = "runner_identity_unavailable"
            else:
                self._identity_failure = _identity_failure(self.request, self.identity)
        self.status_dir = Path(status_dir)
        self.status_dir.mkdir(parents=True, exist_ok=True)
        self.status_path = self.status_dir / "status.json"
        self.events_path = self.status_dir / "events.jsonl"
        self.terminal_path = self.status_dir / "terminal-result.json"
        self.request_path = Path(request_path).resolve() if request_path is not None else None
        self.run_id = str(self.request["run_id"])
        self._started_monotonic = time.monotonic()
        history = _read_event_history(self.events_path, self.run_id)
        if history:
            last_sequence = history[-1]["sequence"]
            assert isinstance(last_sequence, int) and not isinstance(last_sequence, bool)
            self._sequence = last_sequence
        else:
            self._sequence = 0
        self._prior_elapsed_seconds = _prior_elapsed_seconds(history, self.status_path, self.run_id)
        self._completed_phases: list[str] = []
        self._settled_phases: set[str] = set()
        self._phase_exit_codes: dict[str, int] = {}
        self._current_phase: str | None = None
        self._recovered_failure: tuple[str, str] | None = None
        self._recover_history(history)
        self._launch_authorized = any(
            event.get("kind") == "launch_authorized" for event in history
        )
        self._child: subprocess.Popen[bytes] | None = None
        self._stop_requested = threading.Event()
        self._stop_reason = "termination_requested"

    def request_stop(self, reason: str = "termination_requested") -> None:
        """Ask the runner to stop the active process group."""
        self._stop_reason = reason if reason in _STOP_REASONS else "termination_requested"
        self._stop_requested.set()

    def retain_status(self, maximum_seconds: float) -> None:
        """Keep the terminal status endpoint alive within the original run caps."""
        if maximum_seconds <= 0:
            return
        limits = _mapping(self.request["limits"], "limits")
        elapsed_remaining = max(
            0.0,
            _number_as_float(limits["max_elapsed_seconds"]) - self._elapsed_seconds(),
        )
        cost_seconds = (
            _decimal(limits["max_cost_usd"]) * Decimal(3600) / _decimal(limits["usd_per_hour"])
        )
        cost_remaining = max(0.0, float(cost_seconds) - self._elapsed_seconds())
        deadline = time.monotonic() + min(maximum_seconds, elapsed_remaining, cost_remaining)
        while not self._stop_requested.wait(timeout=0.25):
            if time.monotonic() >= deadline:
                return

    def run(self) -> dict[str, object]:
        """Run enabled phases once, returning the immutable terminal result."""
        existing_terminal = self._read_terminal()
        if existing_terminal is not None:
            self._publish_status("terminal", terminal_result=existing_terminal)
            return existing_terminal

        if self._recovered_failure is not None:
            phase, reason = self._recovered_failure
            self._current_phase = phase
            return self._finish(self._terminal_result("failed", reason, phase))
        if self._current_phase is not None:
            interrupted_phase = self._current_phase
            self._terminate_recorded_process_group()
            return self._finish(
                self._terminal_result("failed", "runner_restart_unknown_child", interrupted_phase)
            )

        if self._identity_failure is not None:
            return self._finish(self._terminal_result("failed", self._identity_failure))

        storage_reason = self._storage_reason()
        if storage_reason is not None:
            return self._finish(self._terminal_result("failed", storage_reason))

        self._append_event("runner_resumed" if self._sequence else "ready")
        self._publish_status("ready")

        launch_failure = self._wait_for_launch_authorization()
        if launch_failure is not None:
            return self._finish(self._terminal_result("failed", launch_failure))

        for phase in PHASE_ORDER:
            if phase in self._settled_phases:
                continue
            phase_request = _mapping(self.request["phases"], "phases")[phase]
            phase_config = _mapping(phase_request, f"phases.{phase}")
            if not phase_config["enabled"]:
                self._append_event("phase_disabled", phase)
                continue

            terminal = self._run_phase(phase, phase_config)
            if terminal is not None:
                return self._finish(terminal)
            self._completed_phases.append(phase)

        terminal = self._terminal_result("succeeded", "all_enabled_phases_completed")
        try:
            terminal.update(self._artifact_receipt())
        except ArtifactVerificationError as error:
            terminal = self._terminal_result("failed", error.reason, "package")
        return self._finish(terminal)

    def _wait_for_launch_authorization(self) -> str | None:
        authorization = self.launch_authorization
        if authorization.sha256 == self._status_token_sha256:
            return "launch_authorization_invalid"
        token_path = self._launch_authorization_path(authorization)
        deadline = time.monotonic() + authorization.timeout_seconds
        heartbeat_interval = _number_as_float(self.request["heartbeat_interval_seconds"])
        next_heartbeat = time.monotonic() + heartbeat_interval
        while True:
            if self._stop_requested.is_set():
                return self._stop_reason
            if token_path.exists() or token_path.is_symlink():
                try:
                    token = read_launch_token(token_path)
                    size = token_path.stat(follow_symlinks=False).st_size
                except (LaunchAuthorizationError, OSError):
                    return "launch_authorization_invalid"
                if (
                    size != authorization.size
                    or hashlib.sha256(token.encode("ascii")).hexdigest()
                    != authorization.sha256
                ):
                    return "launch_authorization_invalid"
                if not self._launch_authorized:
                    self._append_event("launch_authorized")
                    self._launch_authorized = True
                return None
            now = time.monotonic()
            if now >= deadline:
                return "launch_authorization_timeout"
            if now >= next_heartbeat:
                self._publish_status("ready")
                next_heartbeat = now + heartbeat_interval
            self._stop_requested.wait(timeout=min(0.05, max(0.0, deadline - now)))

    def _launch_authorization_path(self, authorization: LaunchAuthorization) -> Path:
        storage = _mapping(self.request["storage"], "storage")
        run_root = (
            Path(str(storage["mount"]))
            / "runpod-jobrunner"
            / "runs"
            / self.run_id
        )
        return run_root.joinpath(*authorization.relative_path.parts)

    def _run_phase(
        self, phase: str, phase_config: Mapping[str, object]
    ) -> dict[str, object] | None:
        limit_reason = self._limit_reason()
        if limit_reason is not None:
            return self._terminal_result("limit_exceeded", limit_reason, phase)

        self._current_phase = phase
        argv = [str(argument) for argument in _sequence(phase_config["argv"], "argv")]
        self._append_event("phase_started", phase)
        try:
            self._child = subprocess.Popen(
                argv,
                start_new_session=True,
                env=self._child_environment(),
            )
        except OSError:
            self._append_event("phase_start_failed", phase)
            return self._terminal_result("failed", "phase_start_failed", phase)
        phase_started = time.monotonic()
        timeout_seconds = _number_as_float(phase_config["timeout_seconds"])
        heartbeat_interval = _number_as_float(self.request["heartbeat_interval_seconds"])
        next_heartbeat = 0.0

        while True:
            exit_code = self._child.poll()
            if exit_code is not None:
                self._phase_exit_codes[phase] = exit_code
                self._publish_status("running")
                self._append_event(
                    "phase_completed" if exit_code == 0 else "phase_failed",
                    phase,
                    exit_code=exit_code,
                )
                self._child = None
                if exit_code != 0:
                    return self._terminal_result("failed", "phase_nonzero_exit", phase)
                return None

            now = time.monotonic()
            if self._stop_requested.is_set():
                self._terminate_process_group()
                return self._terminal_result("stopped", self._stop_reason, phase)
            if now - phase_started >= timeout_seconds:
                self._terminate_process_group()
                return self._terminal_result("timed_out", "phase_timeout", phase)
            limit_reason = self._limit_reason()
            if limit_reason is not None:
                self._terminate_process_group()
                return self._terminal_result("limit_exceeded", limit_reason, phase)
            if now >= next_heartbeat:
                self._publish_status("running")
                next_heartbeat = now + heartbeat_interval
            time.sleep(min(heartbeat_interval, 0.05))

    def _terminate_process_group(self) -> None:
        child = self._child
        if child is None or child.poll() is not None:
            return
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        grace_deadline = time.monotonic() + _number_as_float(
            self.request["termination_grace_seconds"]
        )
        while child.poll() is None and time.monotonic() < grace_deadline:
            time.sleep(0.01)
        if child.poll() is None:
            with suppress(ProcessLookupError):
                os.killpg(child.pid, signal.SIGKILL)
        child.wait()
        if self._current_phase is not None:
            self._phase_exit_codes[self._current_phase] = int(child.returncode)
        self._child = None

    def _terminate_recorded_process_group(self) -> None:
        try:
            stored_status = _mapping(json.loads(self.status_path.read_text()), "status.json")
            child_status = _mapping(stored_status.get("child"), "status.json.child")
        except (OSError, json.JSONDecodeError, RequestError):
            return
        child_pid = child_status.get("pid")
        if (
            isinstance(child_pid, bool)
            or not isinstance(child_pid, int)
            or child_pid <= 1
            or child_pid in {os.getpid(), os.getpgrp()}
        ):
            return
        try:
            os.killpg(child_pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        grace_deadline = time.monotonic() + _number_as_float(
            self.request["termination_grace_seconds"]
        )
        while time.monotonic() < grace_deadline:
            try:
                os.killpg(child_pid, 0)
            except ProcessLookupError:
                return
            time.sleep(0.01)
        with suppress(ProcessLookupError):
            os.killpg(child_pid, signal.SIGKILL)

    def _recover_history(self, history: list[dict[str, object]]) -> None:
        active_phase: str | None = None
        for event in history:
            kind = event.get("kind")
            phase = event.get("phase")
            if kind == "phase_started" and isinstance(phase, str):
                active_phase = phase
            elif kind == "phase_completed" and isinstance(phase, str):
                active_phase = None
                self._settled_phases.add(phase)
                self._completed_phases.append(phase)
                exit_code = event.get("exit_code")
                if isinstance(exit_code, int) and not isinstance(exit_code, bool):
                    self._phase_exit_codes[phase] = exit_code
            elif kind == "phase_disabled" and isinstance(phase, str):
                self._settled_phases.add(phase)
            elif kind in {"phase_failed", "phase_start_failed"} and isinstance(phase, str):
                active_phase = None
                reason = "phase_nonzero_exit" if kind == "phase_failed" else "phase_start_failed"
                self._recovered_failure = (phase, reason)
                exit_code = event.get("exit_code")
                if isinstance(exit_code, int) and not isinstance(exit_code, bool):
                    self._phase_exit_codes[phase] = exit_code
        self._current_phase = active_phase

    def _limit_reason(self) -> str | None:
        elapsed_seconds = self._elapsed_seconds()
        limits = _mapping(self.request["limits"], "limits")
        if elapsed_seconds >= _number_as_float(limits["max_elapsed_seconds"]):
            return "elapsed_cap"
        if self._estimated_cost_decimal(elapsed_seconds) >= _decimal(limits["max_cost_usd"]):
            return "cost_cap"
        return None

    def _child_environment(self) -> dict[str, str]:
        storage = _mapping(self.request["storage"], "storage")
        storage_mount = Path(str(storage["mount"]))
        run_root = storage_mount / "runpod-jobrunner" / "runs" / self.run_id
        environment = {
            key: value
            for key, value in os.environ.items()
            if key in _PHASE_ENVIRONMENT_NAMES or key.startswith(_PHASE_ENVIRONMENT_PREFIXES)
        }
        environment.update(
            {
                "RUNPOD_JOBRUNNER_RUN_ID": self.run_id,
                "RUNPOD_JOBRUNNER_STATUS_DIR": str(self.status_dir.resolve()),
                "RUNPOD_JOBRUNNER_STORAGE_MOUNT": str(storage_mount),
                "RUNPOD_JOBRUNNER_RUN_ROOT": str(run_root),
                "RUNPOD_JOBRUNNER_INPUT_ROOT": str(run_root / "input"),
            }
        )
        if self.request_path is not None:
            environment["RUNPOD_JOBRUNNER_REQUEST_PATH"] = str(self.request_path)
        return environment

    def _storage_reason(self) -> str | None:
        storage = _mapping(self.request["storage"], "storage")
        mount_path = Path(str(storage["mount"]))
        if not mount_path.is_dir():
            return "storage_unavailable"
        try:
            filesystem = os.statvfs(mount_path)
        except OSError:
            return "storage_unavailable"
        available_bytes = filesystem.f_bavail * filesystem.f_frsize
        required_gb = storage["required_gb"]
        assert isinstance(required_gb, int) and not isinstance(required_gb, bool)
        required_bytes = required_gb * 1_000_000_000
        if available_bytes < required_bytes:
            return "storage_capacity_below_request"
        return None

    def _finish(self, terminal_result: dict[str, object]) -> dict[str, object]:
        if (
            terminal_result.get("outcome") != "succeeded"
            and "artifact_manifest_sha256" not in terminal_result
        ):
            with suppress(ArtifactVerificationError):
                terminal_result.update(self._artifact_receipt())
        persisted = self._write_terminal_once(terminal_result)
        self._append_event("terminal", self._current_phase, outcome=persisted["outcome"])
        self._publish_status("terminal", terminal_result=persisted)
        return persisted

    def _terminal_result(
        self, outcome: str, reason: str, phase: str | None = None
    ) -> dict[str, object]:
        elapsed_seconds = self._elapsed_seconds()
        return {
            "outcome": outcome,
            "reason": reason,
            "phase": phase,
            "elapsed_seconds": elapsed_seconds,
            "estimated_cost_usd": _decimal_text(self._estimated_cost_decimal(elapsed_seconds)),
            "completed_phases": list(self._completed_phases),
            "phase_exit_codes": dict(self._phase_exit_codes),
        }

    def _artifact_receipt(self) -> dict[str, object]:
        manifest_value = self.request.get("artifact_manifest_path")
        if manifest_value is None:
            return {}
        assert isinstance(manifest_value, str)
        storage = _mapping(self.request["storage"], "storage")
        storage_root = Path(str(storage["mount"])).resolve()
        run_root = storage_root / "runpod-jobrunner" / "runs" / self.run_id
        manifest_path = _safe_storage_file(run_root, manifest_value)
        try:
            manifest_size = manifest_path.stat().st_size
        except OSError:
            raise ArtifactVerificationError("artifact_manifest_missing") from None
        if manifest_size > _MAX_MANIFEST_BYTES:
            raise ArtifactVerificationError("artifact_manifest_invalid")
        manifest_sha256 = _stable_sha256(manifest_path, "artifact_manifest_invalid")
        try:
            manifest_value_raw: object = json.loads(
                manifest_path.read_text(encoding="utf-8"),
                object_pairs_hook=_reject_duplicate_artifact_keys,
            )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
            raise ArtifactVerificationError("artifact_manifest_invalid") from None
        manifest = _artifact_mapping(manifest_value_raw)
        if manifest.get("protocol") != "artifact-manifest/1":
            raise ArtifactVerificationError("artifact_manifest_invalid")
        manifest_run_id = manifest.get("run_id")
        if manifest_run_id is not None and manifest_run_id != self.run_id:
            raise ArtifactVerificationError("artifact_manifest_invalid")
        files = manifest.get("files")
        if not isinstance(files, list):
            raise ArtifactVerificationError("artifact_manifest_invalid")
        seen: set[str] = set()
        for raw in cast(list[object], files):
            entry = _artifact_mapping(raw)
            path_value = entry.get("path")
            size = entry.get("size")
            expected = entry.get("sha256")
            if (
                not isinstance(path_value, str)
                or isinstance(size, bool)
                or not isinstance(size, int)
                or size < 0
                or not isinstance(expected, str)
                or _SHA256_PATTERN.fullmatch(expected) is None
                or path_value in seen
            ):
                raise ArtifactVerificationError("artifact_manifest_invalid")
            seen.add(path_value)
            artifact_path = _safe_storage_file(run_root, path_value)
            try:
                actual_size = artifact_path.stat().st_size
            except OSError:
                raise ArtifactVerificationError("artifact_file_missing") from None
            if actual_size != size:
                raise ArtifactVerificationError("artifact_size_mismatch")
            if _stable_sha256(artifact_path, "artifact_hash_mismatch") != expected:
                raise ArtifactVerificationError("artifact_hash_mismatch")
        return {
            "artifact_manifest_sha256": manifest_sha256,
            "artifact_manifest_size": manifest_size,
        }

    def _publish_status(
        self, state: str, *, terminal_result: Mapping[str, object] | None = None
    ) -> None:
        elapsed_seconds = self._elapsed_seconds()
        child = self._child
        child_status: dict[str, object] | None = None
        if child is not None:
            return_code = child.poll()
            child_status = {
                "pid": child.pid,
                "running": return_code is None,
                "exit_code": return_code,
            }
        status: dict[str, object] = {
            "protocol": "run-status/1",
            "run_id": self.run_id,
            **self.identity.as_protocol_fields(),
            "state": state,
            "phase": self._current_phase,
            "heartbeat_sequence": self._sequence,
            "heartbeat_at_unix": time.time(),
            "heartbeat_monotonic_seconds": elapsed_seconds,
            "child": child_status,
            "latest_event_sequence": self._sequence,
            "terminal_result": dict(terminal_result) if terminal_result is not None else None,
        }
        _atomic_write_json(self.status_path, status)

    def _append_event(self, kind: str, phase: str | None = None, **fields: object) -> None:
        self._sequence += 1
        event: dict[str, object] = {
            "protocol": "run-event/1",
            "run_id": self.run_id,
            "sequence": self._sequence,
            "kind": kind,
            "phase": phase,
            "monotonic_seconds": self._elapsed_seconds(),
            "recorded_at_unix": time.time(),
        }
        event.update(fields)
        encoded = (json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n").encode()
        descriptor = os.open(self.events_path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
        try:
            _write_all(descriptor, encoded)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _elapsed_seconds(self) -> float:
        current_process_elapsed = time.monotonic() - self._started_monotonic
        return round(self._prior_elapsed_seconds + current_process_elapsed, 6)

    def _estimated_cost_decimal(self, elapsed_seconds: float) -> Decimal:
        usd_per_hour = _decimal(_mapping(self.request["limits"], "limits")["usd_per_hour"])
        return Decimal(str(elapsed_seconds)) * usd_per_hour / Decimal(3600)

    def _read_terminal(self) -> dict[str, object] | None:
        if not self.terminal_path.exists():
            return None
        loaded = json.loads(self.terminal_path.read_text())
        return _mapping(loaded, "terminal-result.json")

    def _write_terminal_once(self, terminal_result: dict[str, object]) -> dict[str, object]:
        terminal_json = json.dumps(terminal_result, sort_keys=True, separators=(",", ":"))
        encoded = (terminal_json + "\n").encode()
        temporary = self.terminal_path.with_name(
            f".{self.terminal_path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
        )
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o400)
        try:
            _write_all(descriptor, encoded)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        try:
            os.link(temporary, self.terminal_path)
        except FileExistsError:
            existing = self._read_terminal()
            assert existing is not None
            return existing
        finally:
            temporary.unlink(missing_ok=True)
        _fsync_directory(self.status_dir)
        return terminal_result


def _validate_request(request: Mapping[str, object]) -> dict[str, object]:
    if request.get("protocol") != "run-request/1":
        raise RequestError("unsupported run request protocol")
    run_id = request.get("run_id")
    if not isinstance(run_id, str) or _RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise RequestError("run_id must be a safe non-empty identifier")
    bundle_hash = request.get("bundle_hash")
    if not isinstance(bundle_hash, str) or _BUNDLE_HASH_PATTERN.fullmatch(bundle_hash) is None:
        raise RequestError("bundle_hash must be a lowercase sha256 digest")
    image_digest = request.get("image_digest")
    if not isinstance(image_digest, str) or _IMAGE_DIGEST_PATTERN.fullmatch(image_digest) is None:
        raise RequestError("image_digest must pin an image by sha256 digest")
    try:
        validate_runner_version(request.get("runner_version"))
        validate_git_commit(request.get("runner_git_commit"))
        parse_protocol_majors(request.get("supported_protocol_majors"))
    except RunnerIdentityError as error:
        raise RequestError(str(error)) from error
    storage = _mapping(request.get("storage"), "storage")
    encrypted = storage.get("encrypted")
    network_volume_id = storage.get("network_volume_id")
    if encrypted is True:
        if network_volume_id is not None:
            raise RequestError("encrypted pod storage cannot declare network_volume_id")
    elif encrypted is False:
        if (
            not isinstance(network_volume_id, str)
            or not network_volume_id.strip()
            or any(character in network_volume_id for character in "\x00\r\n")
        ):
            raise RequestError(
                "unencrypted storage requires a non-empty network_volume_id"
            )
    else:
        raise RequestError("storage.encrypted must be boolean")
    mount_path = storage.get("mount")
    if not isinstance(mount_path, str) or not Path(mount_path).is_absolute():
        raise RequestError("storage.mount must be absolute")
    required_gb = storage.get("required_gb")
    if isinstance(required_gb, bool) or not isinstance(required_gb, int) or required_gb <= 0:
        raise RequestError("storage.required_gb must be a positive integer")
    phases = _mapping(request.get("phases"), "phases")
    if set(phases) != set(PHASE_ORDER):
        raise RequestError("phases must contain exactly the fixed phase names")
    for phase in PHASE_ORDER:
        phase_config = _mapping(phases[phase], f"phases.{phase}")
        if not isinstance(phase_config.get("enabled"), bool):
            raise RequestError(f"phases.{phase}.enabled must be boolean")
        argv = _sequence(phase_config.get("argv"), f"phases.{phase}.argv")
        if phase_config["enabled"] and (
            not argv or any(not isinstance(argument, str) or not argument for argument in argv)
        ):
            raise RequestError(f"phases.{phase}.argv must contain non-empty strings")
        _positive_number(phase_config.get("timeout_seconds"), f"phases.{phase}.timeout_seconds")
    limits = _mapping(request.get("limits"), "limits")
    _positive_number(limits.get("max_elapsed_seconds"), "limits.max_elapsed_seconds")
    for limit_name in ("max_cost_usd", "usd_per_hour"):
        _positive_decimal_string(limits.get(limit_name), f"limits.{limit_name}")
    _positive_number(request.get("heartbeat_interval_seconds"), "heartbeat_interval_seconds")
    _nonnegative_number(request.get("termination_grace_seconds"), "termination_grace_seconds")
    artifact_manifest_path = request.get("artifact_manifest_path")
    if request.get("artifact_path_base") != "run-root":
        raise RequestError("artifact_path_base must be run-root")
    if artifact_manifest_path is not None:
        _safe_relative_path(artifact_manifest_path, "artifact_manifest_path")
    acknowledgement = request.get("incremental_mirror_ack")
    if acknowledgement is not None:
        ack = _mapping(acknowledgement, "incremental_mirror_ack")
        if ack.get("protocol") != ACK_PROTOCOL:
            raise RequestError("incremental mirror acknowledgement protocol is unsupported")
        _safe_relative_path(ack.get("directory"), "incremental_mirror_ack.directory")
        _positive_number(ack.get("timeout_seconds"), "incremental_mirror_ack.timeout_seconds")
        signer = _mapping(ack.get("signer"), "incremental_mirror_ack.signer")
        if (
            signer.get("algorithm") != "ssh-ed25519"
            or signer.get("namespace") != ACK_NAMESPACE
            or not isinstance(signer.get("identity"), str)
            or not isinstance(signer.get("key_id"), str)
            or not isinstance(signer.get("public_key"), str)
        ):
            raise RequestError("incremental mirror acknowledgement signer is invalid")
    try:
        parse_launch_authorization(request.get("launch_authorization"))
    except LaunchAuthorizationError as error:
        raise RequestError(str(error)) from error
    return dict(request)


def _identity_failure(request: Mapping[str, object], identity: RunnerIdentity) -> str | None:
    if request["runner_version"] != identity.version:
        return "runner_version_mismatch"
    if request["runner_git_commit"] != identity.git_commit:
        return "runner_git_commit_mismatch"
    requested_majors = parse_protocol_majors(request["supported_protocol_majors"])
    for protocol in (
        "artifact-manifest",
        "launch-authorization",
        "run-event",
        "run-request",
        "run-status",
    ):
        if (
            1 not in requested_majors.get(protocol, ())
            or 1 not in identity.supported_protocol_majors.get(protocol, ())
        ):
            return "unsupported_protocol_major"
    if request.get("incremental_mirror_ack") is not None and (
        1 not in requested_majors.get("incremental-mirror-ack", ())
        or 1 not in identity.supported_protocol_majors.get("incremental-mirror-ack", ())
    ):
        return "unsupported_protocol_major"
    return None


def _safe_relative_path(candidate: object, name: str) -> str:
    if not isinstance(candidate, str) or not candidate or "\\" in candidate or "\x00" in candidate:
        raise RequestError(f"{name} must be a normalized relative path")
    path = Path(candidate)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise RequestError(f"{name} must be a normalized relative path")
    if path.as_posix() != candidate:
        raise RequestError(f"{name} must be a normalized relative path")
    return candidate


def _safe_storage_file(storage_root: Path, candidate: str) -> Path:
    try:
        relative = _safe_relative_path(candidate, "artifact path")
    except RequestError:
        raise ArtifactVerificationError("artifact_manifest_invalid") from None
    current = storage_root
    for part in Path(relative).parts:
        current = current / part
        if current.is_symlink():
            raise ArtifactVerificationError("artifact_manifest_invalid")
    try:
        resolved = current.resolve(strict=True)
        resolved.relative_to(storage_root)
    except (OSError, ValueError):
        if candidate.endswith("manifest.json"):
            raise ArtifactVerificationError("artifact_manifest_missing") from None
        raise ArtifactVerificationError("artifact_file_missing") from None
    if not resolved.is_file():
        raise ArtifactVerificationError("artifact_file_missing")
    return resolved


def _stable_sha256(path: Path, mismatch_reason: str) -> str:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise ArtifactVerificationError(mismatch_reason) from None
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ArtifactVerificationError(mismatch_reason)
        digest = hashlib.sha256()
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise ArtifactVerificationError(mismatch_reason)
        return digest.hexdigest()
    finally:
        os.close(descriptor)


def _reject_duplicate_artifact_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate artifact manifest key")
        result[key] = value
    return result


def _artifact_mapping(candidate: object) -> dict[str, object]:
    if not isinstance(candidate, Mapping):
        raise ArtifactVerificationError("artifact_manifest_invalid")
    raw = cast(Mapping[object, object], candidate)
    if not all(isinstance(key, str) for key in raw):
        raise ArtifactVerificationError("artifact_manifest_invalid")
    return dict(cast(Mapping[str, object], raw))


def _mapping(candidate: object, name: str) -> dict[str, object]:
    if not isinstance(candidate, Mapping):
        raise RequestError(f"{name} must be an object")
    untyped_mapping = cast(Mapping[object, object], candidate)
    if not all(isinstance(key, str) for key in untyped_mapping):
        raise RequestError(f"{name} keys must be strings")
    return dict(cast(Mapping[str, object], untyped_mapping))


def _sequence(candidate: object, name: str) -> list[object]:
    if not isinstance(candidate, Sequence) or isinstance(candidate, (str, bytes)):
        raise RequestError(f"{name} must be an array")
    return list(cast(Sequence[object], candidate))


def _number_as_float(candidate: object) -> float:
    assert not isinstance(candidate, bool) and isinstance(candidate, (int, float))
    return float(candidate)


def _positive_number(candidate: object, name: str) -> None:
    if isinstance(candidate, bool) or not isinstance(candidate, (int, float)):
        raise RequestError(f"{name} must be a positive finite number")
    if not math.isfinite(float(candidate)) or float(candidate) <= 0:
        raise RequestError(f"{name} must be a positive finite number")


def _nonnegative_number(candidate: object, name: str) -> None:
    if isinstance(candidate, bool) or not isinstance(candidate, (int, float)):
        raise RequestError(f"{name} must be a nonnegative finite number")
    if not math.isfinite(float(candidate)) or float(candidate) < 0:
        raise RequestError(f"{name} must be a nonnegative finite number")


def _positive_decimal_string(candidate: object, name: str) -> None:
    if not isinstance(candidate, str) or _MONEY_PATTERN.fullmatch(candidate) is None:
        raise RequestError(f"{name} must be a positive decimal string")
    try:
        parsed = Decimal(candidate)
    except InvalidOperation as error:
        raise RequestError(f"{name} must be a positive decimal string") from error
    if not parsed.is_finite() or parsed <= 0:
        raise RequestError(f"{name} must be a positive decimal string")


def _decimal(candidate: object) -> Decimal:
    assert isinstance(candidate, str)
    return Decimal(candidate)


def _decimal_text(candidate: Decimal) -> str:
    if candidate.is_zero():
        return "0"
    return format(candidate.normalize(), "f")


def _read_event_history(events_path: Path, run_id: str) -> list[dict[str, object]]:
    if not events_path.exists():
        return []
    try:
        journal = events_path.read_bytes()
    except OSError as error:
        raise RequestError("events.jsonl is unavailable") from error
    if journal and not journal.endswith(b"\n"):
        final_record_offset = journal.rfind(b"\n") + 1
        if not _is_torn_json_fragment(journal[final_record_offset:]):
            raise RequestError("events.jsonl contains an incomplete record terminator")
        descriptor = os.open(events_path, os.O_WRONLY)
        try:
            os.ftruncate(descriptor, final_record_offset)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        _fsync_directory(events_path.parent)
        journal = journal[:final_record_offset]
    try:
        lines = journal.decode().splitlines()
    except UnicodeDecodeError as error:
        raise RequestError("events.jsonl contains invalid JSON") from error
    history: list[dict[str, object]] = []
    expected_sequence = 1
    for line in lines:
        if line.strip():
            try:
                event = _mapping(json.loads(line), "events.jsonl event")
            except json.JSONDecodeError as error:
                raise RequestError("events.jsonl contains invalid JSON") from error
            if event.get("protocol") != "run-event/1" or event.get("run_id") != run_id:
                raise RequestError("events.jsonl does not belong to this run")
            sequence = event.get("sequence")
            if (
                isinstance(sequence, bool)
                or not isinstance(sequence, int)
                or sequence != expected_sequence
            ):
                raise RequestError("events.jsonl sequence is not contiguous")
            kind = event.get("kind")
            if kind not in {
                "ready",
                "runner_resumed",
                "launch_authorized",
                "phase_started",
                "phase_completed",
                "phase_failed",
                "phase_start_failed",
                "phase_disabled",
                "terminal",
            }:
                raise RequestError("events.jsonl contains an unknown event kind")
            phase = event.get("phase")
            if phase is not None and phase not in PHASE_ORDER:
                raise RequestError("events.jsonl contains an unknown phase")
            history.append(event)
            expected_sequence += 1
    return history


def _is_torn_json_fragment(fragment: bytes) -> bool:
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


def _prior_elapsed_seconds(
    history: list[dict[str, object]], status_path: Path, run_id: str
) -> float:
    elapsed = 0.0
    if history:
        recorded_elapsed = history[-1].get("monotonic_seconds")
        if isinstance(recorded_elapsed, (int, float)) and not isinstance(recorded_elapsed, bool):
            elapsed = max(elapsed, float(recorded_elapsed))
    try:
        status = _mapping(json.loads(status_path.read_text()), "status.json")
    except (OSError, json.JSONDecodeError, RequestError):
        return elapsed
    status_elapsed = status.get("heartbeat_monotonic_seconds")
    if (
        status.get("run_id") == run_id
        and isinstance(status_elapsed, (int, float))
        and not isinstance(status_elapsed, bool)
    ):
        elapsed = max(elapsed, float(status_elapsed))
    return elapsed


def _atomic_write_json(path: Path, record: Mapping[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    encoded = (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode()
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        _write_all(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_all(descriptor: int, payload: bytes) -> None:
    written = 0
    while written < len(payload):
        count = os.write(descriptor, payload[written:])
        if count == 0:
            raise OSError("short write while persisting runner state")
        written += count


def _load_request(path: Path) -> dict[str, object]:
    loaded = json.loads(path.read_text())
    return _mapping(loaded, "request")


def _install_signal_handlers(runner: RemoteRunner) -> None:
    for signal_number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signal_number, lambda _number, _frame: runner.request_stop())


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one fixed-phase remote request")
    parser.add_argument("request", type=Path)
    parser.add_argument("status_dir", type=Path)
    parser.add_argument("token_file", type=Path)
    parser.add_argument("--status-host", default="0.0.0.0")
    parser.add_argument("--status-port", type=int, default=8000)
    parser.add_argument("--status-retention-seconds", type=float, default=300.0)
    arguments = parser.parse_args(argv)

    from runpod_jobrunner.status_http import StatusHTTPServer

    token = arguments.token_file.read_text().strip()
    if not token:
        parser.error("token file must contain a non-empty token")
    if arguments.status_retention_seconds < 0:
        parser.error("status retention must be nonnegative")
    runner = RemoteRunner(
        _load_request(arguments.request),
        arguments.status_dir,
        request_path=arguments.request,
        status_token_sha256=hashlib.sha256(token.encode("ascii")).hexdigest(),
    )
    _install_signal_handlers(runner)
    server = StatusHTTPServer(
        arguments.status_dir,
        token,
        host=arguments.status_host,
        port=arguments.status_port,
    )
    server.start()
    try:
        terminal = runner.run()
        runner.retain_status(arguments.status_retention_seconds)
        return 0 if terminal["outcome"] == "succeeded" else 1
    finally:
        server.stop()


if __name__ == "__main__":
    raise SystemExit(main())
