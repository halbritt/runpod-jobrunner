"""Application facade and durable supervisor engine."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import secrets
import shutil
import sys
import threading
import time
import tomllib
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn, Protocol, cast

from runpod_jobrunner.budget import BudgetLedger
from runpod_jobrunner.bundle import JobBundle, check_bundle
from runpod_jobrunner.identity import RunnerIdentity, RunnerIdentityError, parse_protocol_majors
from runpod_jobrunner.incremental_ack import (
    ACK_PROTOCOL,
    IncrementalAckError,
    ensure_ack_signer,
)
from runpod_jobrunner.launch_authorization import (
    LaunchAuthorizationError,
    ensure_launch_token,
    parse_launch_authorization,
    read_launch_token,
)
from runpod_jobrunner.lifecycle import (
    ArtifactDisposition,
    LifecycleController,
    LifecycleState,
    WorkloadResult,
)
from runpod_jobrunner.provider import (
    DeleteReceipt,
    Provider,
    ProviderCreateRequest,
    ProviderResource,
)
from runpod_jobrunner.run_store import RunStore
from runpod_jobrunner.runner import RemoteRunner
from runpod_jobrunner.supervision import SupervisionError, SystemdSupervisor
from runpod_jobrunner.transfer import LocalTransfer, TransferError


class ApprovalError(ValueError):
    """A job requests more money than the caller explicitly approved."""


class ApplicationError(RuntimeError):
    """The application cannot safely advance a run."""


class _Supervisor(Protocol):
    def launch(self, run_id: str, executable: Path, run_dir: Path) -> None: ...

    def wake(self, run_id: str) -> None: ...


@dataclass(frozen=True)
class ExecutionObservation:
    result: WorkloadResult
    disposition: ArtifactDisposition
    detail: str


class WorkloadExecutor(Protocol):
    def execute(self, request: Mapping[str, object], run_dir: Path) -> ExecutionObservation: ...


class JobRunner:
    """The small public application interface behind the CLI."""

    def __init__(
        self,
        store: RunStore | None = None,
        supervisor: _Supervisor | None = None,
        *,
        supervisor_executable: Path | None = None,
        run_id_factory: Callable[[], str] | None = None,
        now: Callable[[], datetime] | None = None,
        budget_ledger: BudgetLedger | None = None,
    ) -> None:
        self.store = store or RunStore()
        self.supervisor = supervisor or SystemdSupervisor()
        self.supervisor_executable = supervisor_executable
        self._run_id_factory = run_id_factory or _new_run_id
        self._now = now or (lambda: datetime.now(UTC))
        self._budget_ledger = budget_ledger or BudgetLedger()

    def check(self, bundle_path: Path | str) -> dict[str, object]:
        bundle = check_bundle(bundle_path)
        return _bundle_summary(bundle)

    def run(
        self,
        bundle_path: Path | str,
        *,
        approved_max_usd: Decimal | str,
        budget_scope: str | None = None,
        budget_total_usd: Decimal | str | None = None,
    ) -> str:
        approved = _money(approved_max_usd)
        bundle = check_bundle(bundle_path)
        if bundle.max_cost_usd > approved:
            raise ApprovalError(
                f"job cap ${bundle.max_cost_usd} exceeds explicit approval ${approved}"
            )
        with self.store.admission():
            active = self.store.active_run_ids()
            if active:
                raise ApplicationError(f"an active run already exists: {', '.join(active)}")
            run_id = self._run_id_factory()
            if (budget_scope is None) != (budget_total_usd is None):
                raise ApplicationError("budget scope and aggregate total must be supplied together")
            if budget_scope is not None and budget_total_usd is not None:
                self._budget_ledger.reserve(
                    budget_scope,
                    budget_total_usd,
                    run_id,
                    bundle.max_cost_usd,
                )
            run_dir = self.store.paths(run_id).directory
            token, token_hash = _ensure_status_token(run_dir)
            remote = bundle.to_run_request(run_id)
            remote = _add_incremental_ack(bundle, remote, run_dir)
            try:
                launch_token = ensure_launch_token(run_dir, forbidden_tokens=(token,))
                remote["launch_authorization"] = launch_token.request_fields(
                    timeout_seconds=min(bundle.max_elapsed_seconds, 600)
                )
            except LaunchAuthorizationError as error:
                raise ApplicationError(str(error)) from error
            request = _durable_request(bundle, remote, approved, token_hash, self._now())
            controller = LifecycleController(self.store, _PlanningProvider())
            controller.plan(run_id, request, approved_max_usd=format(approved, "f"))
            self.supervisor.launch(
                run_id,
                self._supervisor_executable(),
                self.store.paths(run_id).directory,
            )
        return run_id

    def status(self, run_id: str) -> dict[str, Any]:
        return self.store.read_state(run_id)

    def stop(self, run_id: str) -> dict[str, Any]:
        state = LifecycleController(self.store, _PlanningProvider()).request_stop(
            run_id, reason="operator_stop"
        )
        self._wake_or_launch(run_id)
        return state

    def recover(self, run_id: str) -> dict[str, Any]:
        self.store.read_state(run_id)
        self._wake_or_launch(run_id)
        return self.store.read_state(run_id)

    def _wake_or_launch(self, run_id: str) -> None:
        try:
            self.supervisor.wake(run_id)
        except SupervisionError:
            self.supervisor.launch(
                run_id,
                self._supervisor_executable(),
                self.store.paths(run_id).directory,
            )

    def _supervisor_executable(self) -> Path:
        return self.supervisor_executable or _installed_supervisor_executable()


class _PlanningProvider:
    """Fail-closed placeholder; planning and stop do not call provider effects."""

    @staticmethod
    def _unavailable(name: str) -> NoReturn:
        raise ApplicationError(f"provider method {name} is unavailable while only planning")

    def find_resources(self, create_operation_id: str) -> tuple[ProviderResource, ...]:
        del create_operation_id
        self._unavailable("find_resources")

    def create(self, request: ProviderCreateRequest) -> ProviderResource:
        del request
        self._unavailable("create")

    def get(self, resource_id: str) -> ProviderResource | None:
        del resource_id
        self._unavailable("get")

    def start(self, resource_id: str, operation_id: str) -> ProviderResource:
        del resource_id, operation_id
        self._unavailable("start")

    def delete(self, resource_id: str, operation_id: str) -> DeleteReceipt:
        del resource_id, operation_id
        self._unavailable("delete")

    def current_spend_usd_per_hour(self, resource_id: str | None) -> Decimal:
        del resource_id
        self._unavailable("current_spend_usd_per_hour")


class SupervisorEngine:
    """Drive provider lifecycle and workload until verified closeout."""

    def __init__(
        self,
        store: RunStore,
        provider: Provider,
        executor: WorkloadExecutor,
        *,
        max_steps: int = 100,
        reconcile_delay_seconds: float = 0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.store = store
        self.controller = LifecycleController(store, provider)
        self.executor = executor
        self.max_steps = max_steps
        self.reconcile_delay_seconds = reconcile_delay_seconds
        self._sleep = sleep

    def drive(self, run_id: str) -> dict[str, Any]:
        observation: ExecutionObservation | None = None
        for _ in range(self.max_steps):
            state = self.controller.status(run_id)
            lifecycle = LifecycleState(state["lifecycle"])
            if lifecycle == LifecycleState.CLOSED:
                return state
            if lifecycle in {
                LifecycleState.PLANNED,
                LifecycleState.PROVISIONING,
                LifecycleState.STARTING,
                LifecycleState.DELETING,
            }:
                self.controller.reconcile(run_id)
                if self.reconcile_delay_seconds:
                    self._sleep(self.reconcile_delay_seconds)
                continue
            if lifecycle == LifecycleState.RUNNING:
                request = self.store.read_request(run_id)
                observation = self.executor.execute(request, self.store.paths(run_id).directory)
                self.controller.record_workload_result(
                    run_id, observation.result, detail=observation.detail
                )
                continue
            if lifecycle == LifecycleState.RECOVERING:
                if _recovery_must_not_dispatch_workload(state):
                    reason = str(state.get("recovery_reason") or "quarantined resources")
                    detail = f"fail-closed recovery: {reason}"
                    self.controller.request_stop(run_id, reason=detail)
                    continue
                if observation is None:
                    request = self.store.read_request(run_id)
                    observation = self.executor.execute(request, self.store.paths(run_id).directory)
                self.controller.record_artifact_disposition(
                    run_id, observation.disposition, detail=observation.detail
                )
                continue
        raise ApplicationError(f"run {run_id} did not close in {self.max_steps} reconcile steps")


def _recovery_must_not_dispatch_workload(state: Mapping[str, object]) -> bool:
    """Return true for provider recovery states that cannot have started work."""

    if state.get("workload_result") is None:
        return True
    if state.get("recovery_reason") in {
        "resource_disappeared_before_start",
        "duplicate_provider_resources",
        "provision_dispatch_unresolved",
    }:
        return True
    quarantined = state.get("quarantined_resource_ids")
    if not isinstance(quarantined, list):
        return False
    return bool(cast(list[object], quarantined))


def _local_ready_identity_error(
    status: Mapping[str, object], request: Mapping[str, object]
) -> str | None:
    """Independently validate the ready runner before publishing private input."""

    if status.get("protocol") != "run-status/1" or status.get("run_id") != request.get(
        "run_id"
    ):
        return "status protocol or run id differs from the request"
    if status.get("runner_version") != request.get("runner_version"):
        return "runner version differs from the request"
    if status.get("runner_git_commit") != request.get("runner_git_commit"):
        return "runner Git commit differs from the request"
    try:
        runner_majors = parse_protocol_majors(status.get("supported_protocol_majors"))
        controller_majors = parse_protocol_majors(request.get("supported_protocol_majors"))
    except RunnerIdentityError as error:
        return str(error)
    for protocol in (
        "artifact-manifest",
        "launch-authorization",
        "run-event",
        "run-request",
        "run-status",
    ):
        if 1 not in runner_majors.get(protocol, ()) or 1 not in controller_majors.get(
            protocol, ()
        ):
            return f"protocol major differs for {protocol}"
    return None


class LocalRunnerExecutor:
    """Run the real remote-runner process model against local filesystem adapters."""

    def __init__(self, *, runner_identity: RunnerIdentity | None = None) -> None:
        self._runner_identity = runner_identity

    def execute(self, request: Mapping[str, object], run_dir: Path) -> ExecutionObservation:
        remote = copy.deepcopy(_mapping(request.get("remote"), "request.remote"))
        controller = _mapping(request.get("controller"), "request.controller")
        remote_root = run_dir / "remote"
        remote_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        local_storage = remote_root / "storage"
        local_storage.mkdir(mode=0o700, parents=True, exist_ok=True)
        remote_storage = _mapping(remote.get("storage"), "request.remote.storage")
        remote_storage["mount"] = str(local_storage)
        remote_storage["required_gb"] = 1
        remote["storage"] = remote_storage
        request_path = remote_root / "request.json"
        _atomic_json(request_path, remote)
        runner = RemoteRunner(
            remote,
            remote_root,
            request_path=request_path,
            runner_identity=self._runner_identity,
            status_token_sha256=_string(controller, "status_token_sha256"),
        )
        terminal_holder: list[dict[str, object]] = []
        runner_errors: list[Exception] = []

        def run_remote() -> None:
            try:
                terminal_holder.append(runner.run())
            except Exception as error:  # pragma: no cover - defensive thread boundary
                runner_errors.append(error)

        thread = threading.Thread(target=run_remote, name=f"runner-{remote['run_id']}")
        thread.start()
        published = False
        try:
            while thread.is_alive():
                if runner.status_path.is_file():
                    status = _read_json_mapping(runner.status_path)
                    state = status.get("state")
                    if state == "ready":
                        identity_error = _local_ready_identity_error(status, remote)
                        if identity_error is not None:
                            runner.request_stop("controller_stop_requested")
                            break
                        if not published:
                            self._publish_local_inputs_and_launch(
                                controller,
                                remote,
                                run_dir,
                                local_storage,
                            )
                            published = True
                        break
                    if state in {"running", "terminal"}:
                        break
                thread.join(timeout=0.01)
            thread.join()
        except (ApplicationError, LaunchAuthorizationError, TransferError):
            runner.request_stop("controller_stop_requested")
            thread.join()
            raise
        if runner_errors:
            raise ApplicationError(f"local remote runner failed: {runner_errors[0]}")
        if not terminal_holder:
            raise ApplicationError("local remote runner produced no terminal result")
        terminal = terminal_holder[0]
        outcome = terminal.get("outcome")
        result = WorkloadResult.SUCCEEDED if outcome == "succeeded" else WorkloadResult.FAILED
        disposition = self._recover_artifacts(
            remote,
            controller,
            local_storage,
            run_dir,
            result,
            terminal,
        )
        return ExecutionObservation(
            result=result,
            disposition=disposition,
            detail=f"remote outcome: {outcome}",
        )

    @staticmethod
    def _publish_local_inputs_and_launch(
        controller: Mapping[str, object],
        remote: Mapping[str, object],
        run_dir: Path,
        storage_root: Path,
    ) -> None:
        run_id = _string(remote, "run_id")
        remote_run_root = storage_root / "runpod-jobrunner" / "runs" / run_id
        input_root = Path(_string(controller, "input_root"))
        input_manifest = [
            cast(Mapping[str, object], item)
            for item in _sequence(controller.get("input_files"), "input_files")
        ]
        transfer = LocalTransfer()
        transfer.upload(input_root, remote_run_root / "input", input_manifest)
        authorization = parse_launch_authorization(remote.get("launch_authorization"))
        token_path = run_dir / "secrets" / "launch-authorization.token"
        token = read_launch_token(token_path)
        encoded = token_path.read_bytes()
        if (
            len(encoded) != authorization.size
            or hashlib.sha256(token.encode("ascii")).hexdigest() != authorization.sha256
        ):
            raise ApplicationError("local launch token differs from the pinned run request")
        transfer.publish_atomic(
            token_path,
            remote_run_root.joinpath(*authorization.relative_path.parts),
            size=authorization.size,
            sha256=hashlib.sha256(encoded).hexdigest(),
        )

    def _recover_artifacts(
        self,
        remote: Mapping[str, object],
        controller: Mapping[str, object],
        storage_root: Path,
        run_dir: Path,
        result: WorkloadResult,
        terminal: Mapping[str, object],
    ) -> ArtifactDisposition:
        manifest_relative = PurePosixPath(_string(controller, "artifact_manifest_path"))
        if manifest_relative.is_absolute() or ".." in manifest_relative.parts:
            raise ApplicationError("artifact manifest path is unsafe")
        artifact_root = storage_root
        if remote.get("artifact_path_base") == "run-root":
            artifact_root = (
                storage_root
                / "runpod-jobrunner"
                / "runs"
                / _string(remote, "run_id")
            )
        elif remote.get("artifact_path_base") is not None:
            raise ApplicationError("remote artifact path base is unsupported")
        manifest_path = artifact_root.joinpath(*manifest_relative.parts)
        if not manifest_path.is_file():
            if result == WorkloadResult.SUCCEEDED:
                raise ApplicationError(
                    "successful workload is missing its declared artifact manifest"
                )
            return ArtifactDisposition.UNAVAILABLE
        expected_manifest_hash = terminal.get("artifact_manifest_sha256")
        expected_manifest_size = terminal.get("artifact_manifest_size")
        if not isinstance(expected_manifest_hash, str) or not isinstance(
            expected_manifest_size, int
        ):
            raise ApplicationError("terminal result lacks an artifact manifest receipt")
        if manifest_path.stat().st_size != expected_manifest_size:
            raise ApplicationError("artifact manifest size differs from terminal receipt")
        if _sha256_file(manifest_path) != expected_manifest_hash:
            raise ApplicationError("artifact manifest hash differs from terminal receipt")
        manifest = _read_json_mapping(manifest_path)
        if manifest.get("protocol") != "artifact-manifest/1":
            raise ApplicationError("artifact manifest protocol is unsupported")
        entries = _sequence(manifest.get("files"), "artifact manifest files")
        artifacts_root = manifest_path.parent
        destination = run_dir / "receipts" / "artifacts"
        transfer_entries: list[Mapping[str, object]] = []
        for raw in entries:
            entry = _mapping(raw, "artifact entry")
            path = PurePosixPath(_string(entry, "path"))
            try:
                relative = path.relative_to(manifest_relative.parent)
            except ValueError:
                raise ApplicationError("artifact lies outside declared artifact root") from None
            transfer_entries.append(
                {
                    "path": relative.as_posix(),
                    "size": entry.get("size"),
                    "sha256": entry.get("sha256"),
                }
            )
        try:
            LocalTransfer().upload(artifacts_root, destination, transfer_entries)
        except TransferError as error:
            raise ApplicationError(str(error)) from error
        _atomic_json(run_dir / "artifact-manifest.json", manifest)
        return ArtifactDisposition.VERIFIED


def _durable_request(
    bundle: JobBundle,
    remote: Mapping[str, object],
    approved: Decimal,
    status_token_hash: str,
    now: datetime,
) -> dict[str, object]:
    spec = bundle.job_spec
    resources = _mapping(spec.get("resources"), "resources")
    gpu_types = _sequence(resources.get("gpu_types"), "resources.gpu_types")
    storage = _mapping(resources.get("storage"), "resources.storage")
    hard_seconds = min(
        bundle.max_elapsed_seconds,
        int(bundle.max_cost_usd / bundle.usd_per_hour * Decimal(3600)),
    )
    terminate_at = now.astimezone(UTC) + timedelta(seconds=hard_seconds)
    manifest = bundle.input_manifest
    inputs = _sequence(manifest.get("files"), "input manifest files")
    artifacts = _mapping(spec.get("artifacts"), "artifacts")
    run_id = _string(remote, "run_id")
    remote_run_root = f"{storage.get('mount')}/runpod-jobrunner/runs/{run_id}"
    return {
        "protocol": "controller-request/1",
        "approved_max_usd": format(approved, "f"),
        "remote": dict(remote),
        "provider": {
            "image": bundle.image_digest,
            "gpu_type_id": _first_string(gpu_types, "resources.gpu_types"),
            "gpu_count": resources.get("gpu_count"),
            "container_disk_gb": resources.get("container_disk_gb", 20),
            **(
                {"network_volume_id": storage["network_volume_id"]}
                if storage.get("network_volume_id") is not None
                else {"volume_gb": storage.get("required_gb")}
            ),
            "volume_mount_path": storage.get("mount"),
            "ports": resources.get("ports", ["22/tcp", "8080/http"]),
            "terminate_at": terminate_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "max_hourly_rate_usd": format(bundle.usd_per_hour, "f"),
            "environment": {
                "RUNPOD_JOBRUNNER_REQUEST_PATH": f"{remote_run_root}/request.json",
                "RUNPOD_JOBRUNNER_STATUS_DIR": f"{remote_run_root}/status",
                "RUNPOD_JOBRUNNER_TOKEN_FILE": f"{remote_run_root}/status-token",
                "RUNPOD_JOBRUNNER_STATUS_RETENTION_SECONDS": str(hard_seconds),
            },
        },
        "controller": {
            "bundle_root": str(bundle.root),
            "input_root": str(bundle.input_root),
            "input_files": [dict(cast(Mapping[str, object], item)) for item in inputs],
            "artifact_manifest_path": artifacts.get("manifest_path"),
            "status_token_sha256": status_token_hash,
            "remote_run_root": remote_run_root,
            **(
                {"incremental_manifest_glob": artifacts["incremental_manifest_glob"]}
                if artifacts.get("incremental_manifest_glob") is not None
                else {}
            ),
            **(
                {"incremental_mirror_ack": remote["incremental_mirror_ack"]}
                if remote.get("incremental_mirror_ack") is not None
                else {}
            ),
        },
    }


def _add_incremental_ack(
    bundle: JobBundle,
    remote: Mapping[str, object],
    run_dir: Path,
) -> dict[str, object]:
    request = dict(remote)
    artifacts = _mapping(bundle.job_spec.get("artifacts"), "artifacts")
    raw_ack = artifacts.get("incremental_mirror_ack")
    if raw_ack is None:
        return request
    ack = _mapping(raw_ack, "artifacts.incremental_mirror_ack")
    try:
        signer = ensure_ack_signer(run_dir, _string(remote, "run_id"))
    except IncrementalAckError as error:
        raise ApplicationError(str(error)) from error
    request["incremental_mirror_ack"] = {
        "protocol": ACK_PROTOCOL,
        "directory": ack.get("directory"),
        "timeout_seconds": ack.get("timeout_seconds"),
        "signer": signer.public_fields(),
    }
    return request


def _bundle_summary(bundle: JobBundle) -> dict[str, object]:
    return {
        "name": bundle.name,
        "bundle_hash": bundle.bundle_hash,
        "image": bundle.image_digest,
        "runner_version": bundle.runner_version,
        "runner_git_commit": bundle.runner_git_commit,
        "input_files": len(bundle.inputs),
        "input_bytes": sum(item.size for item in bundle.inputs),
        "max_cost_usd": format(bundle.max_cost_usd, "f"),
        "max_elapsed_seconds": bundle.max_elapsed_seconds,
    }


def _money(value: object) -> Decimal:
    if isinstance(value, float) or not isinstance(value, (Decimal, str)):
        raise TypeError("approval must be a Decimal or decimal string")
    try:
        result = Decimal(value)
    except InvalidOperation as error:
        raise ValueError("approval is not a decimal amount") from error
    if not result.is_finite() or result <= 0:
        raise ValueError("approval must be positive and finite")
    return result


def _new_run_id() -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    return f"run-{stamp}-{uuid.uuid4().hex[:12]}"


def _installed_supervisor_executable() -> Path:
    executable = shutil.which("runpod-jobrunner-supervise")
    if executable is None:
        sibling = Path(sys.executable).with_name("runpod-jobrunner-supervise")
        if sibling.is_file() and os.access(sibling, os.X_OK):
            executable = str(sibling)
    if executable is None:
        raise ApplicationError("runpod-jobrunner-supervise is not installed on PATH")
    return Path(executable)


def _ensure_status_token(run_dir: Path) -> tuple[str, str]:
    secrets_dir = run_dir / "secrets"
    secrets_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    token_path = secrets_dir / "status-token"
    if token_path.exists():
        token = token_path.read_text(encoding="ascii").strip()
    else:
        token = secrets.token_urlsafe(32)
        descriptor = os.open(token_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            os.write(descriptor, f"{token}\n".encode())
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return token, hashlib.sha256(token.encode()).hexdigest()


def _atomic_json(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    data = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, data)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _read_json_mapping(path: Path) -> dict[str, Any]:
    value: object = json.loads(path.read_text())
    return _mapping(value, str(path))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _mapping(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ApplicationError(f"{name} must be an object")
    return dict(cast(Mapping[str, Any], value))


def _sequence(value: object, name: str) -> list[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ApplicationError(f"{name} must be an array")
    return list(cast(Sequence[object], value))


def _string(value: Mapping[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ApplicationError(f"{key} must be a non-empty string")
    return item


def _first_string(values: Sequence[object], name: str) -> str:
    if not values or not isinstance(values[0], str) or not values[0]:
        raise ApplicationError(f"{name} must contain a string")
    return values[0]


def supervisor_main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    arguments = parser.parse_args(argv)
    run_dir = arguments.run_dir.resolve()
    store = RunStore(run_dir.parent)
    run_id = run_dir.name
    store.read_state(run_id)

    from runpod_jobrunner.remote_executor import RunPodRemoteExecutor
    from runpod_jobrunner.runpod_provider import RunPodHTTP, RunPodProvider

    api = RunPodHTTP(_load_runpod_api_key())
    provider = RunPodProvider(api, secrets_root=run_dir / "secrets" / "provider")
    ssh_key = Path(
        os.environ.get(
            "RUNPOD_JOBRUNNER_SSH_KEY",
            str(Path.home() / ".ssh" / "id_ed25519"),
        )
    )
    executor = RunPodRemoteExecutor(api, ssh_key_file=ssh_key)
    engine = SupervisorEngine(
        store,
        provider,
        executor,
        max_steps=10000,
        reconcile_delay_seconds=2,
    )
    state = engine.drive(run_id)
    return 0 if state.get("lifecycle") == LifecycleState.CLOSED else 1


def _load_runpod_api_key() -> str:
    environment_key = os.environ.get("RUNPOD_API_KEY")
    if environment_key:
        return environment_key
    config_path = Path.home() / ".runpod" / "config.toml"
    try:
        mode = config_path.stat().st_mode & 0o777
        if mode & 0o077:
            raise ApplicationError("RunPod config must not be group- or world-readable")
        value: object = tomllib.loads(config_path.read_text()).get("apikey")
    except (OSError, tomllib.TOMLDecodeError):
        raise ApplicationError("RunPod API credential is unavailable") from None
    if not isinstance(value, str) or not value:
        raise ApplicationError("RunPod API credential is unavailable")
    return value


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(supervisor_main())
