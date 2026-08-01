"""RunPod workload execution over SFTP plus an SSH-independent status channel."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Protocol, cast

from runpod_jobrunner import __version__
from runpod_jobrunner.application import ApplicationError, ExecutionObservation
from runpod_jobrunner.identity import RunnerIdentityError, parse_protocol_majors
from runpod_jobrunner.incremental_ack import (
    ACK_PROTOCOL,
    IncrementalAckError,
    load_ack_signer,
    sign_ack,
    verify_ack,
)
from runpod_jobrunner.launch_authorization import (
    LaunchAuthorizationError,
    parse_launch_authorization,
    read_launch_token,
)
from runpod_jobrunner.lifecycle import ArtifactDisposition, WorkloadResult
from runpod_jobrunner.runpod_provider import PodObservation
from runpod_jobrunner.transfer import (
    ContentReceipt,
    RcloneSFTP,
    RemoteFile,
    TransferError,
    TransferReceipt,
    TransferUnavailable,
    validate_discovery_pattern,
)

_MAX_INCREMENTAL_MANIFEST_BYTES = 16 * 1024 * 1024
_MAX_INCREMENTAL_MANIFESTS = 512
_MAX_INCREMENTAL_FILES = 16_384
_MAX_INCREMENTAL_TOTAL_FILES = 65_536
_MAX_INCREMENTAL_TOTAL_BYTES = 256 * 1024**3
_MIN_CONTROLLER_FREE_RESERVE_BYTES = 1024**3


class _IncrementalCacheMiss(ApplicationError):
    """A safe local checkpoint cache needs to be populated or replaced."""


class _PodAPI(Protocol):
    def get_pod(self, pod_id: str) -> PodObservation | None: ...


class _Transfer(Protocol):
    def upload(
        self,
        source_root: Path,
        destination_root: Path | str,
        manifest: Sequence[Mapping[str, object]],
    ) -> TransferReceipt: ...

    def publish_atomic(
        self,
        source_file: Path,
        destination_file: Path | str,
        *,
        size: int,
        sha256: str,
    ) -> TransferReceipt: ...

    def download(
        self,
        source_root: Path | str,
        destination_root: Path,
        manifest: Sequence[Mapping[str, object]],
    ) -> TransferReceipt: ...

    def discover(
        self,
        source_root: Path | str,
        pattern: str,
        *,
        max_matches: int,
    ) -> tuple[RemoteFile, ...]: ...

    def fetch(
        self,
        source_root: Path | str,
        destination_root: Path,
        remote_file: RemoteFile,
    ) -> ContentReceipt: ...


TransferFactory = Callable[[str, int, Path, Path], _Transfer]
StatusFetcher = Callable[[str, str], Mapping[str, object]]
HostKeyScanner = Callable[[str, int], str]
FreeSpaceBytes = Callable[[Path], object]


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
        free_space_bytes: FreeSpaceBytes | None = None,
        sleep: Callable[[float], None] = time.sleep,
        poll_interval_seconds: float = 2.0,
    ) -> None:
        self._api = api
        self._key = ssh_key_file
        self._transfer_factory = transfer_factory or _rclone_factory
        self._host_key_scanner = host_key_scanner or _scan_host_key
        self._status_fetcher = status_fetcher or _fetch_status
        self._free_space_bytes = free_space_bytes or _available_bytes
        self._sleep = sleep
        self._poll_interval = poll_interval_seconds
        self._verified_incremental_receipts: set[str] = set()

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
        token = _status_token(run_dir, controller)
        self._upload_bootstrap(transfer, remote, token, run_dir, remote_root)

        status_url = f"https://{resource_id}-8080.proxy.runpod.net/status"
        stale_after = max(30.0, float(_positive_number(remote, "heartbeat_interval_seconds")) * 3)
        while datetime.now(UTC) < deadline:
            try:
                status_value = self._status_fetcher(status_url, token)
                status = _mapping(status_value, "remote status")
            except (OSError, urllib.error.URLError):
                self._sleep(self._poll_interval)
                continue
            except (ApplicationError, ValueError) as error:
                return self._permanent_failure(
                    run_dir, f"authenticated remote status is invalid: {error}"
                )
            if status.get("protocol") != "run-status/1" or status.get("run_id") != run_id:
                return self._permanent_failure(
                    run_dir, "authenticated remote status identity mismatch"
                )
            _atomic_json(run_dir / "remote-status.json", status)
            identity_error = _remote_identity_error(status, remote)
            if identity_error is not None:
                return self._permanent_failure(
                    run_dir,
                    f"authenticated remote runner identity mismatch: {identity_error}",
                )
            age = status.get("heartbeat_age_seconds")
            if isinstance(age, bool) or not isinstance(age, (int, float)) or age < 0:
                return self._permanent_failure(
                    run_dir, "authenticated remote status has an invalid heartbeat age"
                )
            if float(age) > stale_after:
                return _failed("authenticated remote heartbeat is stale")
            state = status.get("state")
            if state == "ready":
                try:
                    self._ensure_inputs_uploaded(transfer, controller, remote_root, run_dir, run_id)
                    self._ensure_launch_published(transfer, remote, remote_root, run_dir, run_id)
                except TransferUnavailable:
                    self._sleep(self._poll_interval)
                    continue
                except (ApplicationError, TransferError) as error:
                    return self._permanent_failure(
                        run_dir, f"launch authorization failed: {error}"
                    )
                self._sleep(self._poll_interval)
                continue
            if state not in {"running", "terminal"}:
                return self._permanent_failure(
                    run_dir, "authenticated remote status has an invalid state"
                )
            terminal_value = status.get("terminal_result")
            try:
                self._mirror_incremental_artifacts(transfer, remote, controller, run_dir)
            except TransferUnavailable:
                # SSH/SFTP is a recoverable data-plane observation failure.  The
                # authenticated status record remains process truth.
                self._sleep(self._poll_interval)
                continue
            except (ApplicationError, TransferError) as error:
                return self._incremental_failure(run_dir, error)
            if terminal_value is None:
                self._sleep(self._poll_interval)
                continue
            try:
                terminal = _mapping(terminal_value, "remote terminal result")
            except ApplicationError as error:
                return self._permanent_failure(
                    run_dir, f"authenticated remote terminal result is invalid: {error}"
                )
            outcome = terminal.get("outcome")
            result = WorkloadResult.SUCCEEDED if outcome == "succeeded" else WorkloadResult.FAILED
            try:
                disposition = self._recover_artifacts(
                    transfer, remote, controller, terminal, run_dir, result
                )
            except TransferUnavailable:
                self._sleep(self._poll_interval)
                continue
            except (ApplicationError, TransferError) as error:
                disposition = (
                    ArtifactDisposition.PARTIAL_RECOVERED
                    if self._verified_incremental_receipts
                    or _has_verified_incremental_artifacts(run_dir)
                    else ArtifactDisposition.UNAVAILABLE
                )
                return ExecutionObservation(
                    result=WorkloadResult.FAILED,
                    disposition=disposition,
                    detail=f"terminal artifact recovery failed: {error}",
                )
            return ExecutionObservation(
                result=result,
                disposition=disposition,
                detail=f"remote outcome: {outcome}; reason: {terminal.get('reason')}",
            )
        return _failed("provider termination deadline reached before remote terminal status")

    def _permanent_failure(self, run_dir: Path, detail: str) -> ExecutionObservation:
        disposition = (
            ArtifactDisposition.PARTIAL_RECOVERED
            if self._verified_incremental_receipts
            or _has_verified_incremental_artifacts(run_dir)
            else ArtifactDisposition.UNAVAILABLE
        )
        return ExecutionObservation(
            result=WorkloadResult.FAILED,
            disposition=disposition,
            detail=detail,
        )

    @staticmethod
    def _ensure_inputs_uploaded(
        transfer: _Transfer,
        controller: Mapping[str, object],
        remote_root: str,
        run_dir: Path,
        run_id: str,
    ) -> None:
        input_files = _mapping_sequence(controller.get("input_files"), "input_files")
        expected = {
            "protocol": "input-publication/1",
            "run_id": run_id,
            "destination_root": f"{remote_root}/input",
            "manifest_sha256": hashlib.sha256(
                json.dumps(
                    input_files,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode()
            ).hexdigest(),
            "files": len(input_files),
            "bytes": sum(_artifact_size(item) for item in input_files),
        }
        receipt_path = run_dir / "receipts" / "launch" / "input-publication.json"
        existing = _optional_mapping(receipt_path, "input publication receipt")
        if existing is not None:
            if existing != expected:
                raise ApplicationError("input publication receipt conflicts with the run request")
            return
        input_root = Path(_required_string(controller, "input_root"))
        receipt = transfer.upload(input_root, f"{remote_root}/input", input_files)
        if receipt.files != expected["files"] or receipt.bytes != expected["bytes"]:
            raise ApplicationError("input transfer receipt differs from the allow-list")
        _atomic_json(receipt_path, expected)

    @staticmethod
    def _ensure_launch_published(
        transfer: _Transfer,
        remote: Mapping[str, object],
        remote_root: str,
        run_dir: Path,
        run_id: str,
    ) -> None:
        try:
            authorization = parse_launch_authorization(remote.get("launch_authorization"))
            token_path = run_dir / "secrets" / "launch-authorization.token"
            token = read_launch_token(token_path)
        except LaunchAuthorizationError as error:
            raise ApplicationError(str(error)) from error
        encoded = f"{token}\n".encode("ascii")
        if (
            len(encoded) != authorization.size
            or hashlib.sha256(token.encode("ascii")).hexdigest() != authorization.sha256
        ):
            raise ApplicationError("local launch token differs from the pinned run request")
        expected = {
            "protocol": "launch-publication/1",
            "run_id": run_id,
            "path": authorization.relative_path.as_posix(),
            "sha256": authorization.sha256,
            "size": authorization.size,
        }
        receipt_path = run_dir / "receipts" / "launch" / "authorization-publication.json"
        existing = _optional_mapping(receipt_path, "launch authorization publication receipt")
        if existing is not None:
            if existing != expected:
                raise ApplicationError("launch authorization publication receipt conflicts")
            return
        receipt = transfer.publish_atomic(
            token_path,
            str(PurePosixPath(remote_root) / authorization.relative_path),
            size=authorization.size,
            sha256=hashlib.sha256(encoded).hexdigest(),
        )
        if receipt.files != 1 or receipt.bytes != authorization.size:
            raise ApplicationError("launch authorization transfer receipt is invalid")
        _atomic_json(receipt_path, expected)

    def _incremental_failure(
        self, run_dir: Path, error: ApplicationError | TransferError
    ) -> ExecutionObservation:
        disposition = (
            ArtifactDisposition.PARTIAL_RECOVERED
            if self._verified_incremental_receipts
            or _has_verified_incremental_artifacts(run_dir)
            else ArtifactDisposition.UNAVAILABLE
        )
        return ExecutionObservation(
            result=WorkloadResult.FAILED,
            disposition=disposition,
            detail=f"incremental artifact contract failed: {error}",
        )

    def _mirror_incremental_artifacts(
        self,
        transfer: _Transfer,
        remote: Mapping[str, object],
        controller: Mapping[str, object],
        run_dir: Path,
    ) -> None:
        raw_pattern = controller.get("incremental_manifest_glob")
        if raw_pattern is None:
            return
        if not isinstance(raw_pattern, str):
            raise ApplicationError("incremental manifest glob must be a string")
        try:
            discovery = validate_discovery_pattern(raw_pattern)
        except TransferError as error:
            raise ApplicationError(str(error)) from error
        storage = _mapping(remote.get("storage"), "remote.storage")
        storage_mount = PurePosixPath(_required_string(storage, "mount"))
        required_gb = storage.get("required_gb")
        if (
            isinstance(required_gb, bool)
            or not isinstance(required_gb, int)
            or required_gb <= 0
        ):
            raise ApplicationError("remote storage capacity is invalid")
        max_total_bytes = min(
            required_gb * 1_000_000_000,
            _MAX_INCREMENTAL_TOTAL_BYTES,
        )
        discovery_root = storage_mount
        if discovery.fixed_prefix:
            discovery_root /= discovery.fixed_prefix
        manifests = transfer.discover(
            discovery_root.as_posix(),
            discovery.relative_pattern,
            max_matches=_MAX_INCREMENTAL_MANIFESTS,
        )
        for discovered_manifest in manifests:
            manifest_path = PurePosixPath(discovered_manifest.path)
            if discovery.fixed_prefix:
                manifest_path = PurePosixPath(discovery.fixed_prefix) / manifest_path
            manifest = RemoteFile(
                path=manifest_path.as_posix(),
                size=discovered_manifest.size,
                modified_at=discovered_manifest.modified_at,
            )
            if manifest.size > _MAX_INCREMENTAL_MANIFEST_BYTES:
                raise ApplicationError(
                    f"incremental completion manifest is too large: {manifest.path}"
                )
            self._mirror_incremental_manifest(
                transfer,
                storage_mount,
                manifest,
                remote,
                controller,
                run_dir,
                max_total_bytes,
            )

    def _mirror_incremental_manifest(
        self,
        transfer: _Transfer,
        storage_mount: PurePosixPath,
        remote_manifest: RemoteFile,
        remote: Mapping[str, object],
        controller: Mapping[str, object],
        run_dir: Path,
        max_total_bytes: int,
    ) -> None:
        manifest_relative = _incremental_relative(remote_manifest.path)
        receipt_key = hashlib.sha256(remote_manifest.path.encode()).hexdigest()
        receipt_path = run_dir / "receipts" / "incremental-state" / f"{receipt_key}.json"
        receipt = _optional_mapping(receipt_path, "incremental mirror receipt")
        fingerprint_matches = receipt is not None and (
            receipt.get("remote_size") == remote_manifest.size
            and receipt.get("remote_modified_at") == remote_manifest.modified_at
        )
        if receipt is not None and receipt.get("status") == "verified" and fingerprint_matches:
            if receipt_key not in self._verified_incremental_receipts:
                _verify_incremental_cache(run_dir, receipt, manifest_relative)
                self._verified_incremental_receipts.add(receipt_key)
            self._publish_incremental_ack(
                transfer,
                remote,
                controller,
                run_dir,
                receipt_key,
                receipt_path,
                receipt,
            )
            return

        staging_parent = _ensure_private_directory(
            run_dir / "receipts" / ".incremental-staging",
            anchor=run_dir,
        )
        with tempfile.TemporaryDirectory(
            prefix=f"{receipt_key}-manifest-", dir=staging_parent
        ) as manifest_staging_text:
            manifest_staging = Path(manifest_staging_text)
            fetched = transfer.fetch(
                storage_mount.as_posix(), manifest_staging, remote_manifest
            )
            fetched_path = manifest_staging.joinpath(*manifest_relative.parts)
            try:
                manifest_bytes = fetched_path.read_bytes()
            except OSError as error:
                raise TransferUnavailable(
                    "fetched incremental manifest is unavailable"
                ) from error
        manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
        if fetched.sha256 != manifest_hash or fetched.size != len(manifest_bytes):
            raise ApplicationError("incremental completion manifest receipt mismatch")
        manifest = _load_incremental_manifest(manifest_bytes)
        entries = _incremental_entries(manifest, manifest_relative)
        existing_hash = receipt.get("manifest_sha256") if receipt is not None else None
        if existing_hash is not None and existing_hash != manifest_hash:
            raise ApplicationError(f"conflicting incremental completion for {remote_manifest.path}")
        _enforce_incremental_run_limits(
            run_dir,
            manifest_relative,
            entries,
            max_total_bytes=max_total_bytes,
        )

        pending: dict[str, object] = {
            "protocol": "incremental-mirror-receipt/1",
            "status": "pending",
            "run_id": remote.get("run_id"),
            "manifest_path": remote_manifest.path,
            "remote_size": remote_manifest.size,
            "remote_modified_at": remote_manifest.modified_at,
            "manifest_sha256": manifest_hash,
            "files": [dict(entry) for entry in entries],
        }
        _atomic_json(receipt_path, pending)

        incremental_root = _ensure_private_directory(
            run_dir / "receipts" / "incremental",
            anchor=run_dir,
        )
        destination_parent = incremental_root.joinpath(*manifest_relative.parent.parts)
        try:
            _verify_incremental_files(destination_parent, entries)
        except _IncrementalCacheMiss:
            declared_bytes = sum(_required_size(entry) for entry in entries)
            available_bytes = self._free_space_bytes(staging_parent)
            if (
                isinstance(available_bytes, bool)
                or not isinstance(available_bytes, int)
                or available_bytes < declared_bytes + _MIN_CONTROLLER_FREE_RESERVE_BYTES
            ):
                raise ApplicationError(
                    "incremental transfer would violate the controller free space reserve"
                ) from None
            destination_parent = _ensure_private_directory(
                destination_parent,
                anchor=incremental_root,
            )
            with tempfile.TemporaryDirectory(
                prefix=f"{receipt_key}-payload-", dir=staging_parent
            ) as payload_staging_text:
                payload_staging = Path(payload_staging_text)
                transfer.download(
                    (storage_mount / manifest_relative.parent).as_posix(),
                    payload_staging,
                    entries,
                )
                _verify_incremental_files(payload_staging, entries)
                _publish_incremental_files(
                    payload_staging,
                    destination_parent,
                    entries,
                )
            _verify_incremental_files(destination_parent, entries)

        final_manifest = destination_parent / manifest_relative.name
        _atomic_bytes(final_manifest, manifest_bytes)
        pending["status"] = "verified"
        pending["verified_at"] = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        _atomic_json(receipt_path, pending)
        self._verified_incremental_receipts.add(receipt_key)
        self._publish_incremental_ack(
            transfer,
            remote,
            controller,
            run_dir,
            receipt_key,
            receipt_path,
            pending,
        )

    def _publish_incremental_ack(
        self,
        transfer: _Transfer,
        remote: Mapping[str, object],
        controller: Mapping[str, object],
        run_dir: Path,
        receipt_key_value: str,
        receipt_path: Path,
        receipt: Mapping[str, object],
    ) -> None:
        raw_ack = controller.get("incremental_mirror_ack")
        if raw_ack is None:
            return
        ack_config = _mapping(raw_ack, "controller.incremental_mirror_ack")
        remote_ack = _mapping(
            remote.get("incremental_mirror_ack"), "remote.incremental_mirror_ack"
        )
        if ack_config != remote_ack or ack_config.get("protocol") != ACK_PROTOCOL:
            raise ApplicationError("incremental acknowledgement configuration mismatch")
        signer_fields = _mapping(ack_config.get("signer"), "incremental acknowledgement signer")
        try:
            signer = load_ack_signer(run_dir, _required_string(remote, "run_id"))
        except IncrementalAckError as error:
            raise ApplicationError(str(error)) from error
        if signer.public_fields() != signer_fields:
            raise ApplicationError("incremental acknowledgement signer differs from run request")
        receipt_bytes = receipt_path.read_bytes()
        entries = _mapping_sequence(receipt.get("files"), "incremental mirror receipt files")
        unsigned: dict[str, object] = {
            "protocol": ACK_PROTOCOL,
            "run_id": remote.get("run_id"),
            "bundle_hash": remote.get("bundle_hash"),
            "image_digest": remote.get("image_digest"),
            "manifest_path": receipt.get("manifest_path"),
            "manifest_size": receipt.get("remote_size"),
            "manifest_sha256": receipt.get("manifest_sha256"),
            "file_count": len(entries),
            "file_bytes": sum(_required_size(entry) for entry in entries),
            "local_receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
            "signer": signer_fields,
        }
        ack_root = _ensure_private_directory(
            run_dir / "receipts" / "incremental-acks", anchor=run_dir
        )
        ack_name = f"{receipt_key_value}.json"
        ack_path = ack_root / ack_name
        if ack_path.exists():
            if ack_path.is_symlink() or not ack_path.is_file():
                raise ApplicationError("local incremental acknowledgement is not a regular file")
            encoded = ack_path.read_bytes()
            try:
                verify_ack(encoded, expected=unsigned, signer=signer_fields)
            except IncrementalAckError as error:
                raise ApplicationError(str(error)) from error
        else:
            try:
                encoded = sign_ack(unsigned, signer)
            except IncrementalAckError as error:
                raise ApplicationError(str(error)) from error
            _atomic_bytes(ack_path, encoded)

        publication_root = _ensure_private_directory(
            run_dir / "receipts" / "incremental-ack-state", anchor=run_dir
        )
        publication_path = publication_root / ack_name
        publication = _optional_mapping(
            publication_path, "incremental acknowledgement publication receipt"
        )
        ack_sha256 = hashlib.sha256(encoded).hexdigest()
        if publication is not None:
            if (
                publication.get("protocol") != "incremental-ack-publication/1"
                or publication.get("status") != "published"
                or publication.get("ack_sha256") != ack_sha256
            ):
                raise ApplicationError("incremental acknowledgement publication is malformed")
            return

        remote_root = PurePosixPath(_required_string(controller, "remote_run_root"))
        ack_directory = _safe_relative(_required_string(ack_config, "directory"))
        remote_ack_root = (remote_root / ack_directory).as_posix()
        found = transfer.discover(remote_ack_root, ack_name, max_matches=1)
        if not found:
            transfer.upload(
                ack_root,
                remote_ack_root,
                [{"path": ack_name, "size": len(encoded), "sha256": ack_sha256}],
            )
            found = transfer.discover(remote_ack_root, ack_name, max_matches=1)
        if len(found) != 1 or found[0].path != ack_name:
            raise TransferUnavailable(
                "incremental acknowledgement publication outcome requires reconciliation"
            )
        with tempfile.TemporaryDirectory(
            prefix=f"{receipt_key_value}-ack-", dir=publication_root
        ) as staging_text:
            fetched = transfer.fetch(remote_ack_root, Path(staging_text), found[0])
            fetched_path = Path(staging_text) / ack_name
            if (
                fetched.size != len(encoded)
                or fetched.sha256 != ack_sha256
                or fetched_path.read_bytes() != encoded
            ):
                raise ApplicationError("remote incremental acknowledgement conflicts")
        _atomic_json(
            publication_path,
            {
                "protocol": "incremental-ack-publication/1",
                "status": "published",
                "run_id": remote.get("run_id"),
                "ack_path": f"{ack_directory.as_posix()}/{ack_name}",
                "ack_sha256": ack_sha256,
            },
        )

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
        try:
            manifest_value: object = json.loads(local_manifest.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ApplicationError("artifact manifest is invalid JSON") from error
        manifest = _mapping(manifest_value, "artifact manifest")
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


def _incremental_relative(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        not value
        or len(value) > 1024
        or path.is_absolute()
        or path.as_posix() != value
        or "\\" in value
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
        or any(part in {"", ".", ".."} or len(part) > 255 for part in path.parts)
        or any("*" in part for part in path.parts)
    ):
        raise ApplicationError(f"unsafe incremental manifest path: {value!r}")
    return path


def _load_incremental_manifest(data: bytes) -> dict[str, Any]:
    try:
        value: object = json.loads(data, object_pairs_hook=_json_object_no_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ApplicationError("incremental completion manifest is invalid JSON") from error
    manifest = _mapping(value, "incremental completion manifest")
    protocol = manifest.get("protocol")
    if (
        not isinstance(protocol, str)
        or not protocol
        or len(protocol) > 128
        or any(ord(character) < 32 or ord(character) == 127 for character in protocol)
    ):
        raise ApplicationError("incremental completion manifest protocol is invalid")
    return manifest


def _json_object_no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ApplicationError(f"incremental completion has duplicate key {key!r}")
        result[key] = value
    return result


def _incremental_entries(
    manifest: Mapping[str, object], manifest_path: PurePosixPath
) -> list[Mapping[str, object]]:
    entries = _mapping_sequence(manifest.get("files"), "incremental completion manifest files")
    if not entries:
        raise ApplicationError("incremental completion manifest has no files")
    if len(entries) > _MAX_INCREMENTAL_FILES:
        raise ApplicationError("incremental completion manifest declares too many files")
    normalized: list[Mapping[str, object]] = []
    seen: set[str] = set()
    for entry in entries:
        raw_path = entry.get("path")
        if not isinstance(raw_path, str):
            raise ApplicationError("incremental artifact path must be a string")
        try:
            relative = _incremental_relative(raw_path)
        except ApplicationError as error:
            raise ApplicationError(f"unsafe incremental artifact path: {raw_path!r}") from error
        if relative.as_posix() == manifest_path.name:
            raise ApplicationError("incremental completion manifest cannot declare itself")
        if relative.as_posix() in seen:
            raise ApplicationError(f"duplicate incremental artifact path: {relative.as_posix()}")
        size = entry.get("size")
        expected_hash = entry.get("sha256")
        if (
            isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
            or not isinstance(expected_hash, str)
            or len(expected_hash) != 64
            or any(character not in "0123456789abcdef" for character in expected_hash)
        ):
            raise ApplicationError(
                f"incremental artifact receipt is malformed: {relative.as_posix()}"
            )
        seen.add(relative.as_posix())
        normalized.append(
            {
                "path": relative.as_posix(),
                "size": size,
                "sha256": expected_hash,
            }
        )
    return normalized


def _required_size(entry: Mapping[str, object]) -> int:
    size = entry.get("size")
    if isinstance(size, bool) or not isinstance(size, int) or size < 0:
        raise ApplicationError("incremental artifact receipt has an invalid size")
    return size


def _enforce_incremental_run_limits(
    run_dir: Path,
    manifest_path: PurePosixPath,
    entries: Sequence[Mapping[str, object]],
    *,
    max_total_bytes: int,
) -> None:
    manifests: dict[str, Sequence[Mapping[str, object]]] = {
        manifest_path.as_posix(): entries
    }
    state_root = run_dir / "receipts" / "incremental-state"
    if state_root.is_symlink():
        raise ApplicationError("incremental mirror receipt directory is a symlink")
    if state_root.exists() and not state_root.is_dir():
        raise ApplicationError("incremental mirror receipt directory is not a directory")
    if state_root.is_dir():
        for receipt_path in sorted(state_root.glob("*.json")):
            if receipt_path.is_symlink() or not receipt_path.is_file():
                raise ApplicationError("incremental mirror receipt is not a regular file")
            receipt = _optional_mapping(receipt_path, "incremental mirror receipt")
            if receipt is None:
                continue
            if (
                receipt.get("protocol") != "incremental-mirror-receipt/1"
                or receipt.get("status") not in {"pending", "verified"}
            ):
                raise ApplicationError("incremental mirror receipt is malformed")
            receipt_manifest = _incremental_relative(
                _required_string(receipt, "manifest_path")
            )
            expected_name = hashlib.sha256(
                receipt_manifest.as_posix().encode()
            ).hexdigest() + ".json"
            if receipt_path.name != expected_name:
                raise ApplicationError("incremental mirror receipt identity mismatch")
            receipt_entries = _incremental_entries(
                {"files": receipt.get("files")}, receipt_manifest
            )
            key = receipt_manifest.as_posix()
            existing = manifests.get(key)
            if existing is not None:
                if [dict(entry) for entry in existing] != [
                    dict(entry) for entry in receipt_entries
                ]:
                    raise ApplicationError(
                        "incremental mirror receipt conflicts with current completion"
                    )
                continue
            manifests[key] = receipt_entries

    if len(manifests) > _MAX_INCREMENTAL_MANIFESTS:
        raise ApplicationError("incremental aggregate manifest count exceeds controller limit")
    total_files = sum(len(manifest_entries) for manifest_entries in manifests.values())
    if total_files > _MAX_INCREMENTAL_TOTAL_FILES:
        raise ApplicationError("incremental aggregate file count exceeds controller limit")
    total_bytes = sum(
        _required_size(entry)
        for manifest_entries in manifests.values()
        for entry in manifest_entries
    )
    if total_bytes > max_total_bytes:
        raise ApplicationError(
            "incremental aggregate bytes exceed encrypted storage capacity"
        )


def _available_bytes(path: Path) -> int:
    try:
        filesystem = os.statvfs(path)
    except OSError as error:
        raise ApplicationError("controller free space is unavailable") from error
    return filesystem.f_bavail * filesystem.f_frsize


def _verify_incremental_files(root: Path, entries: Sequence[Mapping[str, object]]) -> None:
    if root.is_symlink():
        raise ApplicationError("mirrored incremental artifact root is a symlink")
    if not root.exists():
        raise _IncrementalCacheMiss("mirrored incremental artifact root is missing")
    if not root.is_dir():
        raise ApplicationError("mirrored incremental artifact root is not a directory")
    resolved_root = root.resolve()
    for entry in entries:
        relative = _incremental_relative(_required_string(entry, "path"))
        path = root.joinpath(*relative.parts)
        if path.is_symlink() or any(
            parent.is_symlink() for parent in path.parents if parent != root
        ):
            raise ApplicationError(
                f"mirrored incremental artifact is a symlink: {relative.as_posix()}"
            )
        try:
            path.resolve().relative_to(resolved_root)
        except (OSError, ValueError):
            raise ApplicationError(
                f"mirrored incremental artifact escapes its receipt root: {relative.as_posix()}"
            ) from None
        if not path.is_file():
            raise _IncrementalCacheMiss(
                f"mirrored incremental artifact is missing: {relative.as_posix()}"
            )
        size = entry.get("size")
        expected_hash = entry.get("sha256")
        if path.stat().st_size != size:
            raise _IncrementalCacheMiss(
                f"mirrored incremental artifact size mismatch: {relative.as_posix()}"
            )
        if _sha256_file(path) != expected_hash:
            raise _IncrementalCacheMiss(
                f"mirrored incremental artifact hash mismatch: {relative.as_posix()}"
            )


def _publish_incremental_files(
    staging_root: Path,
    destination_root: Path,
    entries: Sequence[Mapping[str, object]],
) -> None:
    for entry in entries:
        relative = _incremental_relative(_required_string(entry, "path"))
        source = staging_root.joinpath(*relative.parts)
        target_parent = _ensure_private_directory(
            destination_root.joinpath(*relative.parent.parts),
            anchor=destination_root,
        )
        target = target_parent / relative.name
        if target.is_symlink():
            raise ApplicationError(
                f"mirrored incremental artifact is a symlink: {relative.as_posix()}"
            )
        if target.exists() and not target.is_file():
            raise ApplicationError(
                f"mirrored incremental artifact is not a regular file: {relative.as_posix()}"
            )
        os.replace(source, target)
        _fsync_file(target)
        _fsync_directory(target_parent)


def _ensure_private_directory(path: Path, *, anchor: Path) -> Path:
    if anchor.is_symlink() or not anchor.is_dir():
        raise ApplicationError("incremental receipt anchor is not a safe directory")
    try:
        relative = path.relative_to(anchor)
    except ValueError:
        raise ApplicationError("incremental receipt directory escapes its anchor") from None
    resolved_anchor = anchor.resolve()
    current = anchor
    for part in relative.parts:
        current /= part
        if current.is_symlink():
            raise ApplicationError(f"incremental receipt directory is a symlink: {current}")
        try:
            current.mkdir(mode=0o700)
        except FileExistsError:
            pass
        except OSError as error:
            raise ApplicationError(
                f"incremental receipt directory is unavailable: {current}"
            ) from error
        if not current.is_dir():
            raise ApplicationError(
                f"incremental receipt path is not a directory: {current}"
            )
        try:
            current.resolve().relative_to(resolved_anchor)
        except (OSError, ValueError):
            raise ApplicationError(
                f"incremental receipt directory escapes its anchor: {current}"
            ) from None
    return current


def _verify_incremental_cache(
    run_dir: Path,
    receipt: Mapping[str, object],
    manifest_relative: PurePosixPath,
) -> None:
    if (
        receipt.get("protocol") != "incremental-mirror-receipt/1"
        or receipt.get("status") != "verified"
        or receipt.get("manifest_path") != manifest_relative.as_posix()
    ):
        raise ApplicationError("incremental mirror receipt is malformed")
    manifest_path = (run_dir / "receipts" / "incremental").joinpath(*manifest_relative.parts)
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ApplicationError("verified incremental completion manifest is unavailable")
    manifest_bytes = manifest_path.read_bytes()
    if hashlib.sha256(manifest_bytes).hexdigest() != receipt.get("manifest_sha256"):
        raise ApplicationError("verified incremental completion manifest hash mismatch")
    manifest = _load_incremental_manifest(manifest_bytes)
    entries = _incremental_entries(manifest, manifest_relative)
    recorded_entries = _mapping_sequence(receipt.get("files"), "incremental mirror receipt files")
    if [dict(entry) for entry in entries] != [dict(entry) for entry in recorded_entries]:
        raise ApplicationError("incremental mirror receipt conflicts with its manifest")
    _verify_incremental_files(manifest_path.parent, entries)


def _has_verified_incremental_artifacts(run_dir: Path) -> bool:
    state_root = run_dir / "receipts" / "incremental-state"
    if state_root.is_symlink() or not state_root.is_dir():
        return False
    for receipt_path in sorted(state_root.glob("*.json")):
        if receipt_path.is_symlink() or not receipt_path.is_file():
            continue
        try:
            receipt = _optional_mapping(receipt_path, "incremental mirror receipt")
            if receipt is None or receipt.get("status") != "verified":
                continue
            manifest_relative = _incremental_relative(
                _required_string(receipt, "manifest_path")
            )
            _verify_incremental_cache(run_dir, receipt, manifest_relative)
        except ApplicationError:
            continue
        return True
    return False


def _optional_mapping(path: Path, name: str) -> dict[str, Any] | None:
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as error:
        raise ApplicationError(f"{name} is unavailable") from error
    try:
        value: object = json.loads(data, object_pairs_hook=_json_object_no_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ApplicationError(f"{name} is invalid JSON") from error
    return _mapping(value, name)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


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
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": f"runpod-jobrunner/{__version__}",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            raw: object = json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 401:
            raise ApplicationError("remote status token was rejected") from None
        raise urllib.error.URLError(f"remote status HTTP {error.code}") from None
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise ApplicationError("remote status response is invalid JSON") from None
    if not isinstance(raw, Mapping):
        raise ApplicationError("remote status response is not an object")
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


def _artifact_size(value: Mapping[str, object]) -> int:
    size = value.get("size")
    if isinstance(size, bool) or not isinstance(size, int) or size < 0:
        raise ApplicationError("input manifest entry has an invalid size")
    return size


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


def _remote_identity_error(
    status: Mapping[str, object], request: Mapping[str, object]
) -> str | None:
    if status.get("runner_version") != request.get("runner_version"):
        return "version"
    if status.get("runner_git_commit") != request.get("runner_git_commit"):
        return "Git commit"
    try:
        runner_majors = parse_protocol_majors(status.get("supported_protocol_majors"))
        controller_majors = parse_protocol_majors(request.get("supported_protocol_majors"))
    except RunnerIdentityError:
        return "protocol capability receipt"
    for protocol in (
        "artifact-manifest",
        "launch-authorization",
        "run-event",
        "run-request",
        "run-status",
    ):
        if 1 not in runner_majors[protocol] or 1 not in controller_majors[protocol]:
            return f"unsupported {protocol} major"
    if request.get("incremental_mirror_ack") is not None and (
        1 not in runner_majors.get("incremental-mirror-ack", ())
        or 1 not in controller_majors.get("incremental-mirror-ack", ())
    ):
        return "unsupported incremental-mirror-ack major"
    return None


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
