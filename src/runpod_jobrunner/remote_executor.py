"""RunPod workload execution over SFTP plus an SSH-independent status channel."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Protocol, cast

from runpod_jobrunner.application import ApplicationError, ExecutionObservation
from runpod_jobrunner.lifecycle import ArtifactDisposition, WorkloadResult
from runpod_jobrunner.runpod_provider import PodObservation
from runpod_jobrunner.transfer import RcloneSFTP, TransferReceipt


class _PodAPI(Protocol):
    def get_pod(self, pod_id: str) -> PodObservation | None: ...


class _Transfer(Protocol):
    def upload(
        self,
        source_root: Path,
        destination_root: Path | str,
        manifest: Sequence[Mapping[str, object]],
    ) -> TransferReceipt: ...

    def download(
        self,
        source_root: Path | str,
        destination_root: Path,
        manifest: Sequence[Mapping[str, object]],
    ) -> TransferReceipt: ...


TransferFactory = Callable[[str, int, Path, Path], _Transfer]
StatusFetcher = Callable[[str, str], Mapping[str, object]]
HostKeyScanner = Callable[[str, int], str]


class RunPodRemoteExecutor:
    """Deliver one immutable request, follow authenticated status, and recover artifacts."""

    def __init__(
        self,
        api: _PodAPI,
        *,
        ssh_key_file: Path,
        transfer_factory: TransferFactory | None = None,
        host_key_scanner: HostKeyScanner | None = None,
        status_fetcher: StatusFetcher | None = None,
        sleep: Callable[[float], None] = time.sleep,
        poll_interval_seconds: float = 2.0,
    ) -> None:
        self._api = api
        self._key = ssh_key_file
        self._transfer_factory = transfer_factory or _rclone_factory
        self._host_key_scanner = host_key_scanner or _scan_host_key
        self._status_fetcher = status_fetcher or _fetch_status
        self._sleep = sleep
        self._poll_interval = poll_interval_seconds

    def execute(self, request: Mapping[str, object], run_dir: Path) -> ExecutionObservation:
        controller = _mapping(request.get("controller"), "request.controller")
        remote = _mapping(request.get("remote"), "request.remote")
        run_id = _required_string(remote, "run_id")
        resource_id = _resource_id(run_dir)
        deadline = _deadline(_mapping(request.get("provider"), "request.provider"))
        pod = self._wait_for_connectivity(resource_id, deadline)
        if pod is None:
            return _failed("provider resource disappeared before remote delivery")
        assert pod.public_ip is not None and pod.port_mappings is not None
        ssh_port = _ssh_port(pod.port_mappings)
        if ssh_port is None:
            return _failed("provider did not publish an SSH port mapping")

        known_hosts = run_dir / "secrets" / "known-hosts"
        host_key = self._host_key_scanner(pod.public_ip, ssh_port)
        if not host_key.strip():
            raise ApplicationError("SSH host key scan returned no key")
        _atomic_bytes(known_hosts, (host_key.rstrip() + "\n").encode(), mode=0o600)
        transfer = self._transfer_factory(pod.public_ip, ssh_port, self._key, known_hosts)
        remote_root = _required_string(controller, "remote_run_root")
        self._upload_inputs(transfer, controller, remote_root)
        token = _status_token(run_dir, controller)
        self._upload_bootstrap(transfer, remote, token, run_dir, remote_root)

        status_url = f"https://{resource_id}-8080.proxy.runpod.net/status"
        stale_after = max(30.0, float(_positive_number(remote, "heartbeat_interval_seconds")) * 3)
        while datetime.now(UTC) < deadline:
            try:
                status_value = self._status_fetcher(status_url, token)
                status = _mapping(status_value, "remote status")
            except (OSError, ValueError, urllib.error.URLError):
                self._sleep(self._poll_interval)
                continue
            if status.get("protocol") != "run-status/1" or status.get("run_id") != run_id:
                raise ApplicationError("authenticated remote status identity mismatch")
            _atomic_json(run_dir / "remote-status.json", status)
            age = status.get("heartbeat_age_seconds")
            if isinstance(age, bool) or not isinstance(age, (int, float)) or age < 0:
                raise ApplicationError("authenticated remote status has an invalid heartbeat age")
            if float(age) > stale_after:
                return _failed("authenticated remote heartbeat is stale")
            terminal_value = status.get("terminal_result")
            if terminal_value is None:
                self._sleep(self._poll_interval)
                continue
            terminal = _mapping(terminal_value, "remote terminal result")
            outcome = terminal.get("outcome")
            result = WorkloadResult.SUCCEEDED if outcome == "succeeded" else WorkloadResult.FAILED
            disposition = self._recover_artifacts(
                transfer, remote, controller, terminal, run_dir, result
            )
            return ExecutionObservation(
                result=result,
                disposition=disposition,
                detail=f"remote outcome: {outcome}; reason: {terminal.get('reason')}",
            )
        return _failed("provider termination deadline reached before remote terminal status")

    def _wait_for_connectivity(self, resource_id: str, deadline: datetime) -> PodObservation | None:
        while datetime.now(UTC) < deadline:
            pod = self._api.get_pod(resource_id)
            if pod is None:
                return None
            if (
                pod.volume_encrypted is True
                and pod.public_ip
                and pod.port_mappings is not None
                and _ssh_port(pod.port_mappings) is not None
            ):
                return pod
            self._sleep(self._poll_interval)
        return None

    @staticmethod
    def _upload_inputs(
        transfer: _Transfer, controller: Mapping[str, object], remote_root: str
    ) -> None:
        input_root = Path(_required_string(controller, "input_root"))
        input_files = _mapping_sequence(controller.get("input_files"), "input_files")
        transfer.upload(input_root, f"{remote_root}/input", input_files)

    @staticmethod
    def _upload_bootstrap(
        transfer: _Transfer,
        remote: Mapping[str, object],
        token: str,
        run_dir: Path,
        remote_root: str,
    ) -> None:
        bootstrap = run_dir / "bootstrap"
        request_bytes = (
            json.dumps(remote, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
        ).encode()
        token_bytes = f"{token}\n".encode("ascii")
        _atomic_bytes(bootstrap / "request.json", request_bytes, mode=0o600)
        _atomic_bytes(bootstrap / "status-token", token_bytes, mode=0o600)
        manifest = [
            {
                "path": "request.json",
                "size": len(request_bytes),
                "sha256": hashlib.sha256(request_bytes).hexdigest(),
            },
            {
                "path": "status-token",
                "size": len(token_bytes),
                "sha256": hashlib.sha256(token_bytes).hexdigest(),
            },
        ]
        transfer.upload(bootstrap, remote_root, manifest)

    @staticmethod
    def _recover_artifacts(
        transfer: _Transfer,
        remote: Mapping[str, object],
        controller: Mapping[str, object],
        terminal: Mapping[str, object],
        run_dir: Path,
        result: WorkloadResult,
    ) -> ArtifactDisposition:
        manifest_hash = terminal.get("artifact_manifest_sha256")
        manifest_size = terminal.get("artifact_manifest_size")
        if (
            not isinstance(manifest_hash, str)
            or isinstance(manifest_size, bool)
            or not isinstance(manifest_size, int)
        ):
            if result == WorkloadResult.SUCCEEDED:
                raise ApplicationError(
                    "successful remote terminal lacks an artifact manifest receipt"
                )
            return ArtifactDisposition.UNAVAILABLE
        storage = _mapping(remote.get("storage"), "remote.storage")
        storage_mount = PurePosixPath(_required_string(storage, "mount"))
        manifest_relative = _safe_relative(_required_string(controller, "artifact_manifest_path"))
        manifest_parent = manifest_relative.parent
        manifest_name = manifest_relative.name
        manifest_destination = run_dir / "receipts" / "manifest"
        transfer.download(
            (storage_mount / manifest_parent).as_posix(),
            manifest_destination,
            [{"path": manifest_name, "size": manifest_size, "sha256": manifest_hash}],
        )
        local_manifest = manifest_destination / manifest_name
        manifest = _mapping(json.loads(local_manifest.read_text()), "artifact manifest")
        if manifest.get("protocol") != "artifact-manifest/1":
            raise ApplicationError("artifact manifest protocol is unsupported")
        manifest_run_id = manifest.get("run_id")
        if manifest_run_id is not None and manifest_run_id != remote.get("run_id"):
            raise ApplicationError("artifact manifest run identity mismatch")
        raw_entries = _mapping_sequence(manifest.get("files"), "artifact manifest files")
        relative_entries: list[Mapping[str, object]] = []
        for entry in raw_entries:
            path = _safe_relative(_required_string(entry, "path"))
            try:
                relative = path.relative_to(manifest_parent)
            except ValueError:
                raise ApplicationError(
                    "artifact lies outside the declared artifact directory"
                ) from None
            relative_entries.append(
                {
                    "path": relative.as_posix(),
                    "size": entry.get("size"),
                    "sha256": entry.get("sha256"),
                }
            )
        artifacts_destination = run_dir / "receipts" / "artifacts"
        transfer.download(
            (storage_mount / manifest_parent).as_posix(),
            artifacts_destination,
            relative_entries,
        )
        _atomic_bytes(run_dir / "artifact-manifest.json", local_manifest.read_bytes())
        return (
            ArtifactDisposition.VERIFIED
            if result == WorkloadResult.SUCCEEDED
            else ArtifactDisposition.PARTIAL_RECOVERED
        )


def _rclone_factory(host: str, port: int, key: Path, known_hosts: Path) -> _Transfer:
    if not key.is_file():
        raise ApplicationError(f"RunPod SSH key is unavailable: {key}")
    return RcloneSFTP(
        host=host,
        port=port,
        user="root",
        key_file=key,
        known_hosts_file=known_hosts,
    )


def _scan_host_key(host: str, port: int) -> str:
    result = subprocess.run(
        ["ssh-keyscan", "-T", "10", "-p", str(port), host],
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        raise ApplicationError("unable to obtain the ephemeral RunPod SSH host key")
    return result.stdout


def _fetch_status(url: str, token: str) -> Mapping[str, object]:
    request = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            raw: object = json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 401:
            raise ApplicationError("remote status token was rejected") from None
        raise urllib.error.URLError(f"remote status HTTP {error.code}") from None
    if not isinstance(raw, Mapping):
        raise ValueError("remote status response is not an object")
    return cast(Mapping[str, object], raw)


def _resource_id(run_dir: Path) -> str:
    try:
        value: object = json.loads((run_dir / "state.json").read_text())
    except (OSError, json.JSONDecodeError):
        raise ApplicationError("durable resource state is unavailable") from None
    state = _mapping(value, "state")
    resource = _mapping(state.get("resource"), "state.resource")
    return _required_string(resource, "id")


def _status_token(run_dir: Path, controller: Mapping[str, object]) -> str:
    try:
        token = (run_dir / "secrets" / "status-token").read_text(encoding="ascii").strip()
    except OSError:
        raise ApplicationError("run-scoped status token is unavailable") from None
    expected = _required_string(controller, "status_token_sha256")
    if not token or hashlib.sha256(token.encode()).hexdigest() != expected:
        raise ApplicationError("run-scoped status token does not match durable request")
    return token


def _deadline(provider: Mapping[str, object]) -> datetime:
    value = _required_string(provider, "terminate_at")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError:
        raise ApplicationError("provider termination deadline is invalid") from None
    return parsed


def _ssh_port(mappings: Mapping[str, int]) -> int | None:
    for key in ("22", "22/tcp"):
        value = mappings.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and 0 < value < 65536:
            return value
    return None


def _safe_relative(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or not value
        or "\\" in value
        or "\x00" in value
        or any(part in {"", ".", ".."} for part in path.parts)
        or path.as_posix() != value
    ):
        raise ApplicationError("unsafe remote artifact path")
    return path


def _mapping(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ApplicationError(f"{name} must be an object")
    raw = cast(Mapping[object, object], value)
    if not all(isinstance(key, str) for key in raw):
        raise ApplicationError(f"{name} must have string keys")
    return dict(cast(Mapping[str, Any], raw))


def _mapping_sequence(value: object, name: str) -> list[Mapping[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ApplicationError(f"{name} must be an array")
    return [_mapping(item, name) for item in cast(Sequence[object], value)]


def _required_string(value: Mapping[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ApplicationError(f"{key} must be a non-empty string")
    return item


def _positive_number(value: Mapping[str, object], key: str) -> int | float:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, (int, float)) or item <= 0:
        raise ApplicationError(f"{key} must be a positive number")
    return item


def _failed(detail: str) -> ExecutionObservation:
    return ExecutionObservation(
        result=WorkloadResult.FAILED,
        disposition=ArtifactDisposition.UNAVAILABLE,
        detail=detail,
    )


def _atomic_json(path: Path, value: Mapping[str, object]) -> None:
    _atomic_bytes(
        path,
        (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode(),
    )


def _atomic_bytes(path: Path, data: bytes, *, mode: int = 0o600) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, mode)
    try:
        offset = 0
        while offset < len(data):
            offset += os.write(descriptor, data[offset:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
