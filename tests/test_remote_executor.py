from __future__ import annotations

import hashlib
import io
import json
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator, Mapping, Sequence
from decimal import Decimal
from email.message import Message
from pathlib import Path

import pytest

from runpod_jobrunner.application import ApplicationError
from runpod_jobrunner.incremental_ack import (
    AckSigner,
    ensure_ack_signer,
    verify_ack,
)
from runpod_jobrunner.launch_authorization import LAUNCH_PROTOCOL, LAUNCH_RELATIVE_PATH
from runpod_jobrunner.lifecycle import ArtifactDisposition, WorkloadResult
from runpod_jobrunner.remote_executor import RunPodRemoteExecutor
from runpod_jobrunner.runpod_provider import PodObservation
from runpod_jobrunner.transfer import (
    ContentReceipt,
    LocalTransfer,
    RemoteFile,
    TransferReceipt,
    TransferUnavailable,
)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


LAUNCH_TOKEN = "1" * 64


class FakeAPI:
    def get_pod(self, pod_id: str) -> PodObservation:
        assert pod_id == "pod-one"
        return PodObservation(
            id=pod_id,
            name="rjr-test",
            desired_status="RUNNING",
            cost_per_hour=Decimal("0.10"),
            volume_encrypted=True,
            public_ip="192.0.2.10",
            port_mappings={"22": 22022},
            environment={
                "RUNPOD_JOBRUNNER_RUN_ID": "run-remote",
                "RUNPOD_JOBRUNNER_OPERATION_ID": "op-create",
            },
        )


class FakeTransfer:
    def __init__(self, remote: Path) -> None:
        self.remote = remote
        self.uploads: list[str] = []
        self.downloads: list[str] = []
        self.discoveries = 0
        self.discovery_requests: list[tuple[str, str]] = []
        self.fetches: list[str] = []

    def upload(
        self,
        source_root: Path,
        destination_root: Path | str,
        manifest: Sequence[Mapping[str, object]],
    ) -> TransferReceipt:
        self.uploads.append(str(destination_root))
        target = self.remote / str(destination_root).lstrip("/")
        return LocalTransfer().upload(source_root, target, manifest)

    def publish_atomic(
        self,
        source_file: Path,
        destination_file: Path | str,
        *,
        size: int,
        sha256: str,
    ) -> TransferReceipt:
        self.uploads.append(str(Path(str(destination_file)).parent))
        target = self.remote / str(destination_file).lstrip("/")
        return LocalTransfer().publish_atomic(
            source_file,
            target,
            size=size,
            sha256=sha256,
        )

    def download(
        self,
        source_root: Path | str,
        destination_root: Path,
        manifest: Sequence[Mapping[str, object]],
    ) -> TransferReceipt:
        self.downloads.append(str(source_root))
        source = self.remote / str(source_root).lstrip("/")
        return LocalTransfer().upload(source, destination_root, manifest)

    def discover(
        self, source_root: Path | str, pattern: str, *, max_matches: int
    ) -> tuple[RemoteFile, ...]:
        self.discoveries += 1
        self.discovery_requests.append((str(source_root), pattern))
        source = self.remote / str(source_root).lstrip("/")
        return LocalTransfer().discover(source, pattern, max_matches=max_matches)

    def fetch(
        self,
        source_root: Path | str,
        destination_root: Path,
        remote_file: RemoteFile,
    ) -> ContentReceipt:
        self.fetches.append(remote_file.path)
        source = self.remote / str(source_root).lstrip("/")
        return LocalTransfer().fetch(source, destination_root, remote_file)


def make_request(
    tmp_path: Path,
    *,
    incremental: bool = False,
    ack_signer: AckSigner | None = None,
    storage_gb: int = 10,
) -> dict[str, object]:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    payload = b"allowed input\n"
    (input_root / "sft.jsonl").write_bytes(payload)
    ack_config: dict[str, object] | None = None
    if ack_signer is not None:
        ack_config = {
            "protocol": "incremental-mirror-ack/1",
            "directory": "control/incremental-acks",
            "timeout_seconds": 30,
            "signer": ack_signer.public_fields(),
        }
    return {
        "protocol": "controller-request/1",
        "remote": {
            "protocol": "run-request/1",
            "run_id": "run-remote",
            "bundle_hash": "a" * 64,
            "image_digest": "example.invalid/image@sha256:" + "b" * 64,
            "runner_version": "0.1.1",
            "runner_git_commit": "a" * 40,
            "supported_protocol_majors": {
                "artifact-manifest": [1],
                **({"incremental-mirror-ack": [1]} if ack_config else {}),
                "launch-authorization": [1],
                "run-event": [1],
                "run-request": [1],
                "run-status": [1],
            },
            "phases": {},
            "limits": {
                "max_elapsed_seconds": 600,
                "max_cost_usd": "0.50",
                "usd_per_hour": "0.10",
            },
            "heartbeat_interval_seconds": 5,
            "termination_grace_seconds": 10,
            "storage": {
                "encrypted": True,
                "mount": "/workspace",
                "required_gb": storage_gb,
            },
            "artifact_manifest_path": "artifacts/manifest.json",
            "launch_authorization": {
                "protocol": LAUNCH_PROTOCOL,
                "path": LAUNCH_RELATIVE_PATH,
                "sha256": digest(LAUNCH_TOKEN.encode()),
                "size": len(LAUNCH_TOKEN) + 1,
                "timeout_seconds": 30,
            },
            **({"incremental_mirror_ack": ack_config} if ack_config else {}),
        },
        "provider": {"terminate_at": "2099-01-01T00:00:00Z"},
        "controller": {
            "input_root": str(input_root),
            "input_files": [{"path": "sft.jsonl", "size": len(payload), "sha256": digest(payload)}],
            "artifact_manifest_path": "artifacts/manifest.json",
            "status_token_sha256": digest(b"run-token"),
            "remote_run_root": "/workspace/runpod-jobrunner/runs/run-remote",
            **(
                {"incremental_manifest_glob": ("checkpoints/checkpoint-*/checkpoint-complete.json")}
                if incremental
                else {}
            ),
            **({"incremental_mirror_ack": ack_config} if ack_config else {}),
        },
    }


def write_incremental_checkpoint(
    remote: Path,
    *,
    checkpoint: str = "checkpoint-25",
    payload: bytes = b"checkpoint state\n",
    declared_path: str = "trainer_state.json",
    declared_hash: str | None = None,
    declared_size: int | None = None,
) -> Path:
    checkpoint_root = remote / "workspace" / "checkpoints" / checkpoint
    checkpoint_root.mkdir(parents=True, exist_ok=True)
    file_path = checkpoint_root / "trainer_state.json"
    file_path.write_bytes(payload)
    manifest = {
        "protocol": "example-checkpoint-completion/1",
        "checkpoint": checkpoint,
        "files": [
            {
                "path": declared_path,
                "size": len(payload) if declared_size is None else declared_size,
                "sha256": declared_hash or digest(payload),
            }
        ],
    }
    manifest_path = checkpoint_root / "checkpoint-complete.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True))
    return manifest_path


def write_terminal_artifact(remote: Path) -> tuple[bytes, dict[str, object]]:
    artifact = b"verified output\n"
    artifact_path = remote / "workspace" / "artifacts" / "result.bin"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_bytes(artifact)
    manifest = {
        "protocol": "artifact-manifest/1",
        "run_id": "run-remote",
        "files": [
            {
                "path": "artifacts/result.bin",
                "size": len(artifact),
                "sha256": digest(artifact),
            }
        ],
    }
    manifest_bytes = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode()
    (artifact_path.parent / "manifest.json").write_bytes(manifest_bytes)
    return artifact, {
        "outcome": "succeeded",
        "reason": "all_enabled_phases_completed",
        "phase": None,
        "elapsed_seconds": 12.0,
        "estimated_cost_usd": "0.01",
        "completed_phases": ["verify", "train", "package"],
        "phase_exit_codes": {"verify": 0, "train": 0, "package": 0},
        "artifact_manifest_sha256": digest(manifest_bytes),
        "artifact_manifest_size": len(manifest_bytes),
    }


def remote_status(
    terminal: Mapping[str, object] | None = None, *, ack: bool = False
) -> dict[str, object]:
    return {
        "protocol": "run-status/1",
        "run_id": "run-remote",
        "runner_version": "0.1.1",
        "runner_git_commit": "a" * 40,
        "supported_protocol_majors": {
            "artifact-manifest": [1],
            **({"incremental-mirror-ack": [1]} if ack else {}),
            "launch-authorization": [1],
            "run-event": [1],
            "run-request": [1],
            "run-status": [1],
        },
        "state": "terminal" if terminal is not None else "running",
        "phase": None if terminal is not None else "train",
        "heartbeat_age_seconds": 0.1,
        "heartbeat_monotonic_seconds": 12.0,
        "child": None if terminal is not None else {"pid": 1, "running": True},
        "latest_event_sequence": 8,
        "terminal_result": terminal,
    }


def ready_status() -> dict[str, object]:
    status = remote_status()
    status.update(state="ready", phase=None, child=None)
    return status


def prepare_run_dir(tmp_path: Path) -> Path:
    run_dir = tmp_path / "runs" / "run-remote"
    (run_dir / "secrets").mkdir(parents=True)
    (run_dir / "secrets").chmod(0o700)
    (run_dir / "secrets" / "status-token").write_text("run-token\n")
    (run_dir / "secrets" / "launch-authorization.token").write_text(
        f"{LAUNCH_TOKEN}\n"
    )
    (run_dir / "state.json").write_text(json.dumps({"resource": {"id": "pod-one"}}))
    return run_dir


def test_identity_mismatch_never_uploads_private_inputs_or_launch_authorization(
    tmp_path: Path,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    remote = tmp_path / "remote"
    transfer = FakeTransfer(remote)
    status = ready_status()
    status["runner_git_commit"] = "b" * 40
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=lambda _url, _token: status,
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path), run_dir)

    assert observation.result == WorkloadResult.FAILED
    assert observation.disposition == ArtifactDisposition.UNAVAILABLE
    assert observation.detail.endswith("identity mismatch: Git commit")
    assert transfer.uploads == ["/workspace/runpod-jobrunner/runs/run-remote"]
    assert not (remote / "workspace/runpod-jobrunner/runs/run-remote/input").exists()
    assert not (
        remote
        / "workspace/runpod-jobrunner/runs/run-remote/control/launch-authorization.token"
    ).exists()


@pytest.mark.parametrize(
    ("field", "value", "detail"),
    [
        ("protocol", "run-status/99", "authenticated remote status identity mismatch"),
        ("run_id", "another-run", "authenticated remote status identity mismatch"),
        (
            "heartbeat_age_seconds",
            True,
            "authenticated remote status has an invalid heartbeat age",
        ),
        (
            "heartbeat_age_seconds",
            -1,
            "authenticated remote status has an invalid heartbeat age",
        ),
        (
            "heartbeat_age_seconds",
            "fresh",
            "authenticated remote status has an invalid heartbeat age",
        ),
    ],
)
def test_permanent_authenticated_status_errors_return_failed_observation(
    tmp_path: Path, field: str, value: object, detail: str
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    transfer = FakeTransfer(tmp_path / "remote")
    status = ready_status()
    status[field] = value
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=lambda _url, _token: status,
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path), run_dir)

    assert observation.result == WorkloadResult.FAILED
    assert observation.disposition == ArtifactDisposition.UNAVAILABLE
    assert observation.detail == detail


def test_rejected_authenticated_status_token_returns_failed_observation(
    tmp_path: Path,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    transfer = FakeTransfer(tmp_path / "remote")

    def rejected(_url: str, _token: str) -> Mapping[str, object]:
        raise ApplicationError("remote status token was rejected")

    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=rejected,
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path), run_dir)

    assert observation.result == WorkloadResult.FAILED
    assert observation.disposition == ArtifactDisposition.UNAVAILABLE
    assert observation.detail.endswith("remote status token was rejected")


def test_permanent_status_error_preserves_verified_incremental_disposition(
    tmp_path: Path,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    remote = tmp_path / "remote"
    write_incremental_checkpoint(remote)
    transfer = FakeTransfer(remote)
    invalid = remote_status()
    invalid["run_id"] = "another-run"
    statuses = iter((remote_status(), invalid))
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=status_sequence(statuses),
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path, incremental=True), run_dir)

    assert observation.result == WorkloadResult.FAILED
    assert observation.disposition == ArtifactDisposition.PARTIAL_RECOVERED
    assert observation.detail == "authenticated remote status identity mismatch"


class _StatusResponse(io.BytesIO):
    def __enter__(self) -> _StatusResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def test_fetch_status_sends_product_user_agent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    observed_headers: dict[str, str] = {}

    def capture(request: object, timeout: float) -> _StatusResponse:
        assert isinstance(request, urllib.request.Request)
        assert timeout == 15
        observed_headers.update(dict(request.header_items()))
        raise urllib.error.HTTPError(
            request.full_url, 401, "unauthorized", Message(), None
        )

    monkeypatch.setattr(
        "runpod_jobrunner.remote_executor.urllib.request.urlopen",
        capture,
    )
    run_dir = prepare_run_dir(tmp_path)
    transfer = FakeTransfer(tmp_path / "remote")
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        sleep=lambda _seconds: None,
    )

    executor.execute(make_request(tmp_path), run_dir)

    assert observed_headers["User-agent"].startswith("runpod-jobrunner/")
    assert observed_headers["Authorization"] == "Bearer run-token"


@pytest.mark.parametrize("body", [b"not JSON", b"[]"])
def test_fetch_status_types_authenticated_malformed_response_as_permanent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, body: bytes
) -> None:
    def malformed(_request: object, timeout: float) -> _StatusResponse:
        del _request, timeout
        return _StatusResponse(body)

    monkeypatch.setattr(
        "runpod_jobrunner.remote_executor.urllib.request.urlopen",
        malformed,
    )
    run_dir = prepare_run_dir(tmp_path)
    transfer = FakeTransfer(tmp_path / "remote")
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path), run_dir)

    assert observation.result == WorkloadResult.FAILED
    assert observation.disposition == ArtifactDisposition.UNAVAILABLE
    assert "remote status response" in observation.detail


def test_fetch_status_types_http_401_as_permanent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def unauthorized(_request: object, timeout: float) -> _StatusResponse:
        del _request, timeout
        raise urllib.error.HTTPError(
            "https://status.invalid", 401, "unauthorized", Message(), None
        )

    monkeypatch.setattr(
        "runpod_jobrunner.remote_executor.urllib.request.urlopen", unauthorized
    )
    run_dir = prepare_run_dir(tmp_path)
    transfer = FakeTransfer(tmp_path / "remote")
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path), run_dir)

    assert observation.result == WorkloadResult.FAILED
    assert observation.disposition == ArtifactDisposition.UNAVAILABLE
    assert observation.detail.endswith("status token was rejected")


def transfer_factory_for(
    transfer: FakeTransfer,
) -> Callable[[str, int, Path, Path], FakeTransfer]:
    def factory(_host: str, _port: int, _key: Path, _known: Path) -> FakeTransfer:
        return transfer

    return factory


def fixed_host_key(_host: str, _port: int) -> str:
    return "host ssh-ed25519 AAAA"


def status_sequence(
    statuses: Iterator[dict[str, object]],
) -> Callable[[str, str], Mapping[str, object]]:
    def fetch(_url: str, _token: str) -> Mapping[str, object]:
        return next(statuses)

    return fetch


def running_status(_url: str, _token: str) -> Mapping[str, object]:
    return remote_status()


def test_remote_executor_uses_status_truth_and_recovers_exact_artifacts(tmp_path: Path) -> None:
    run_dir = prepare_run_dir(tmp_path)
    remote = tmp_path / "remote"
    artifact = b"verified output\n"
    artifact_path = remote / "workspace" / "artifacts" / "result.bin"
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(artifact)
    manifest = {
        "protocol": "artifact-manifest/1",
        "run_id": "run-remote",
        "files": [
            {
                "path": "artifacts/result.bin",
                "size": len(artifact),
                "sha256": digest(artifact),
            }
        ],
    }
    manifest_bytes = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode()
    manifest_path = remote / "workspace" / "artifacts" / "manifest.json"
    manifest_path.write_bytes(manifest_bytes)
    transfer = FakeTransfer(remote)
    ssh_attempts: list[tuple[str, int]] = []

    def transfer_factory(_host: str, _port: int, _key: Path, _known: Path) -> FakeTransfer:
        return transfer

    terminal_status = remote_status(
        {
            "outcome": "succeeded",
            "reason": "all_enabled_phases_completed",
            "phase": None,
            "elapsed_seconds": 12.0,
            "estimated_cost_usd": "0.01",
            "completed_phases": ["verify", "package"],
            "phase_exit_codes": {"verify": 0, "package": 0},
            "artifact_manifest_sha256": digest(manifest_bytes),
            "artifact_manifest_size": len(manifest_bytes),
        }
    )
    statuses = iter((ready_status(), terminal_status))
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory,
        host_key_scanner=lambda host, port: (
            ssh_attempts.append((host, port)) or "[192.0.2.10]:22022 ssh-ed25519 AAAA"
        ),
        status_fetcher=status_sequence(statuses),
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path), run_dir)

    assert observation.result == WorkloadResult.SUCCEEDED
    assert observation.disposition == ArtifactDisposition.VERIFIED
    assert ssh_attempts == [("192.0.2.10", 22022)]
    assert transfer.uploads == [
        "/workspace/runpod-jobrunner/runs/run-remote",
        "/workspace/runpod-jobrunner/runs/run-remote/input",
        "/workspace/runpod-jobrunner/runs/run-remote/control",
    ]
    assert (run_dir / "receipts/launch/input-publication.json").is_file()
    assert (run_dir / "receipts/launch/authorization-publication.json").is_file()
    assert (run_dir / "receipts" / "artifacts" / "result.bin").read_bytes() == artifact
    assert "run-token" not in (run_dir / "remote-status.json").read_text()


def test_controller_restart_after_inputs_before_token_does_not_reupload_inputs(
    tmp_path: Path,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    remote = tmp_path / "remote"
    artifact, terminal = write_terminal_artifact(remote)
    request = make_request(tmp_path)

    class CrashBeforeToken(FakeTransfer):
        publish_attempts = 0

        def publish_atomic(
            self,
            source_file: Path,
            destination_file: Path | str,
            *,
            size: int,
            sha256: str,
        ) -> TransferReceipt:
            self.publish_attempts += 1
            if self.publish_attempts == 1:
                raise SystemExit("controller crashed before launch token publication")
            return super().publish_atomic(
                source_file,
                destination_file,
                size=size,
                sha256=sha256,
            )

    transfer = CrashBeforeToken(remote)
    first = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=lambda _url, _token: ready_status(),
        sleep=lambda _seconds: None,
    )

    with pytest.raises(SystemExit, match="before launch token"):
        first.execute(request, run_dir)

    statuses = iter((ready_status(), remote_status(terminal)))
    second = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=status_sequence(statuses),
        sleep=lambda _seconds: None,
    )

    observation = second.execute(request, run_dir)

    input_root = "/workspace/runpod-jobrunner/runs/run-remote/input"
    control_root = "/workspace/runpod-jobrunner/runs/run-remote/control"
    assert observation.result == WorkloadResult.SUCCEEDED
    assert transfer.uploads.count(input_root) == 1
    assert transfer.uploads.count(control_root) == 1
    assert transfer.publish_attempts == 2
    assert (run_dir / "receipts/artifacts/result.bin").read_bytes() == artifact


def test_controller_restart_after_token_does_not_reupload_inputs_or_token(
    tmp_path: Path,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    remote = tmp_path / "remote"
    _artifact, terminal = write_terminal_artifact(remote)
    request = make_request(tmp_path)
    transfer = FakeTransfer(remote)

    def crash_after_publication(_seconds: float) -> None:
        raise SystemExit("controller crashed after launch token publication")

    first = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=lambda _url, _token: ready_status(),
        sleep=crash_after_publication,
    )

    with pytest.raises(SystemExit, match="after launch token"):
        first.execute(request, run_dir)

    statuses = iter((ready_status(), remote_status(terminal)))
    second = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=status_sequence(statuses),
        sleep=lambda _seconds: None,
    )

    observation = second.execute(request, run_dir)

    assert observation.result == WorkloadResult.SUCCEEDED
    assert transfer.uploads.count(
        "/workspace/runpod-jobrunner/runs/run-remote/input"
    ) == 1
    assert transfer.uploads.count(
        "/workspace/runpod-jobrunner/runs/run-remote/control"
    ) == 1


@pytest.mark.parametrize("initial_state", ["running", "terminal"])
def test_running_or_terminal_restart_never_reuploads_inputs_or_launch_token(
    tmp_path: Path,
    initial_state: str,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    remote = tmp_path / "remote"
    _artifact, terminal = write_terminal_artifact(remote)
    transfer = FakeTransfer(remote)
    statuses = (
        iter((remote_status(), remote_status(terminal)))
        if initial_state == "running"
        else iter((remote_status(terminal),))
    )
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=status_sequence(statuses),
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path), run_dir)

    assert observation.result == WorkloadResult.SUCCEEDED
    assert transfer.uploads == ["/workspace/runpod-jobrunner/runs/run-remote"]


def test_terminal_artifact_transfer_unavailable_retries_before_success(
    tmp_path: Path,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    remote = tmp_path / "remote"
    artifact, terminal = write_terminal_artifact(remote)

    class InterruptingTerminalFetch(FakeTransfer):
        def __init__(self, root: Path) -> None:
            super().__init__(root)
            self.manifest_attempts = 0

        def download(
            self,
            source_root: Path | str,
            destination_root: Path,
            manifest: Sequence[Mapping[str, object]],
        ) -> TransferReceipt:
            if any(item.get("path") == "manifest.json" for item in manifest):
                self.manifest_attempts += 1
                if self.manifest_attempts == 1:
                    raise TransferUnavailable("injected terminal fetch interruption")
            return super().download(source_root, destination_root, manifest)

    transfer = InterruptingTerminalFetch(remote)
    statuses = iter((remote_status(terminal), remote_status(terminal)))
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=status_sequence(statuses),
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path), run_dir)

    assert observation.result == WorkloadResult.SUCCEEDED
    assert observation.disposition == ArtifactDisposition.VERIFIED
    assert transfer.manifest_attempts == 2
    assert (run_dir / "receipts/artifacts/result.bin").read_bytes() == artifact


def test_permanent_terminal_manifest_error_fails_with_unavailable_artifacts(
    tmp_path: Path,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    remote = tmp_path / "remote"
    _artifact, terminal = write_terminal_artifact(remote)
    invalid = b"not JSON\n"
    (remote / "workspace/artifacts/manifest.json").write_bytes(invalid)
    terminal["artifact_manifest_sha256"] = digest(invalid)
    terminal["artifact_manifest_size"] = len(invalid)
    transfer = FakeTransfer(remote)
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=lambda _url, _token: remote_status(terminal),
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path), run_dir)

    assert observation.result == WorkloadResult.FAILED
    assert observation.disposition == ArtifactDisposition.UNAVAILABLE
    assert observation.detail.startswith("terminal artifact recovery failed:")
    assert "manifest is invalid JSON" in observation.detail


def test_remote_executor_treats_stale_authenticated_heartbeat_as_failure(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "runs" / "run-remote"
    (run_dir / "secrets").mkdir(parents=True)
    (run_dir / "secrets" / "status-token").write_text("run-token\n")
    (run_dir / "state.json").write_text(json.dumps({"resource": {"id": "pod-one"}}))
    transfer = FakeTransfer(tmp_path / "remote")

    def transfer_factory(_host: str, _port: int, _key: Path, _known: Path) -> FakeTransfer:
        return transfer

    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory,
        host_key_scanner=lambda _host, _port: "host ssh-ed25519 AAAA",
        status_fetcher=lambda _url, _token: {
            "protocol": "run-status/1",
            "run_id": "run-remote",
            "runner_version": "0.1.1",
            "runner_git_commit": "a" * 40,
            "supported_protocol_majors": {
                "artifact-manifest": [1],
                "launch-authorization": [1],
                "run-event": [1],
                "run-request": [1],
                "run-status": [1],
            },
            "state": "running",
            "phase": "train",
            "heartbeat_age_seconds": 31,
            "heartbeat_monotonic_seconds": 100,
            "child": {"pid": 1, "running": True, "exit_code": None},
            "latest_event_sequence": 10,
            "terminal_result": None,
        },
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path), run_dir)

    assert observation.result == WorkloadResult.FAILED
    assert observation.disposition == ArtifactDisposition.UNAVAILABLE
    assert observation.detail == "authenticated remote heartbeat is stale"


def test_remote_executor_fails_closed_on_authenticated_runner_identity_mismatch(
    tmp_path: Path,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    transfer = FakeTransfer(tmp_path / "remote")
    status = remote_status()
    status["runner_git_commit"] = "b" * 40
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=lambda _url, _token: status,
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path), run_dir)

    assert observation.result == WorkloadResult.FAILED
    assert observation.disposition == ArtifactDisposition.UNAVAILABLE
    assert observation.detail.endswith("identity mismatch: Git commit")


def test_incremental_checkpoint_is_mirrored_before_remote_terminal(
    tmp_path: Path,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    remote = tmp_path / "remote"
    write_incremental_checkpoint(remote)
    artifact, terminal = write_terminal_artifact(remote)
    transfer = FakeTransfer(remote)
    observations = 0

    def fetch_status(_url: str, _token: str) -> Mapping[str, object]:
        nonlocal observations
        observations += 1
        if observations == 1:
            return remote_status()
        mirrored = run_dir / "receipts/incremental/checkpoints/checkpoint-25/trainer_state.json"
        assert mirrored.read_bytes() == b"checkpoint state\n"
        return remote_status(terminal)

    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=fetch_status,
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path, incremental=True), run_dir)

    assert observation.result == WorkloadResult.SUCCEEDED
    assert (run_dir / "receipts/artifacts/result.bin").read_bytes() == artifact
    assert (
        run_dir / "receipts/incremental/checkpoints/checkpoint-25/checkpoint-complete.json"
    ).is_file()
    assert transfer.discovery_requests == [
        ("/workspace/checkpoints", "checkpoint-*/checkpoint-complete.json"),
        ("/workspace/checkpoints", "checkpoint-*/checkpoint-complete.json"),
    ]
    assert "checkpoints/" not in (run_dir / "remote-status.json").read_text()


def test_incremental_ack_is_signed_published_and_reconciled_after_unknown_upload(
    tmp_path: Path,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    signer = ensure_ack_signer(run_dir, "run-remote")
    remote = tmp_path / "remote"
    write_incremental_checkpoint(remote)
    _artifact, terminal = write_terminal_artifact(remote)

    class UnknownAckUploadTransfer(FakeTransfer):
        def __init__(self, root: Path) -> None:
            super().__init__(root)
            self.ack_upload_attempts = 0

        def upload(
            self,
            source_root: Path,
            destination_root: Path | str,
            manifest: Sequence[Mapping[str, object]],
        ) -> TransferReceipt:
            receipt = super().upload(source_root, destination_root, manifest)
            if str(destination_root).endswith("/control/incremental-acks"):
                self.ack_upload_attempts += 1
                if self.ack_upload_attempts == 1:
                    raise TransferUnavailable("injected unknown ack upload outcome")
            return receipt

    transfer = UnknownAckUploadTransfer(remote)
    statuses = iter(
        (
            remote_status(ack=True),
            remote_status(ack=True),
            remote_status(terminal, ack=True),
        )
    )
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=status_sequence(statuses),
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(
        make_request(tmp_path, incremental=True, ack_signer=signer),
        run_dir,
    )

    assert observation.result == WorkloadResult.SUCCEEDED
    assert transfer.ack_upload_attempts == 1
    manifest_path = "checkpoints/checkpoint-25/checkpoint-complete.json"
    ack_name = hashlib.sha256(manifest_path.encode()).hexdigest() + ".json"
    remote_ack = (
        remote
        / "workspace/runpod-jobrunner/runs/run-remote/control/incremental-acks"
        / ack_name
    ).read_bytes()
    signed = json.loads(remote_ack)
    signature = signed.pop("signature")
    assert isinstance(signature, str)
    assert verify_ack(
        remote_ack,
        expected=signed,
        signer=signer.public_fields(),
    )["manifest_path"] == manifest_path
    assert (
        run_dir / "receipts/incremental-ack-state" / ack_name
    ).is_file()


def test_interrupted_incremental_transfer_retries_without_recopy_after_verification(
    tmp_path: Path,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    remote = tmp_path / "remote"
    write_incremental_checkpoint(remote)
    _artifact, terminal = write_terminal_artifact(remote)

    class InterruptingTransfer(FakeTransfer):
        def __init__(self, root: Path) -> None:
            super().__init__(root)
            self.checkpoint_download_attempts = 0

        def download(
            self,
            source_root: Path | str,
            destination_root: Path,
            manifest: Sequence[Mapping[str, object]],
        ) -> TransferReceipt:
            if str(source_root).endswith("/checkpoints/checkpoint-25"):
                self.checkpoint_download_attempts += 1
                if self.checkpoint_download_attempts == 1:
                    raise TransferUnavailable("injected SFTP interruption")
            return super().download(source_root, destination_root, manifest)

    transfer = InterruptingTransfer(remote)
    statuses = iter((remote_status(), remote_status(), remote_status(), remote_status(terminal)))
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=status_sequence(statuses),
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path, incremental=True), run_dir)

    assert observation.result == WorkloadResult.SUCCEEDED
    assert transfer.checkpoint_download_attempts == 2
    assert transfer.fetches.count("checkpoints/checkpoint-25/checkpoint-complete.json") == 2
    # Two later status observations reused the verified receipt without another copy.
    assert transfer.discoveries == 4


def test_ssh_discovery_interruption_does_not_change_authenticated_process_truth(
    tmp_path: Path,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    remote = tmp_path / "remote"
    write_incremental_checkpoint(remote)
    _artifact, terminal = write_terminal_artifact(remote)

    class ListInterruptingTransfer(FakeTransfer):
        def discover(
            self, source_root: Path | str, pattern: str, *, max_matches: int
        ) -> tuple[RemoteFile, ...]:
            if self.discoveries == 0:
                self.discoveries += 1
                raise TransferUnavailable("injected SSH list interruption")
            return super().discover(source_root, pattern, max_matches=max_matches)

    transfer = ListInterruptingTransfer(remote)
    statuses = iter((remote_status(), remote_status(), remote_status(terminal)))
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=status_sequence(statuses),
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path, incremental=True), run_dir)

    assert observation.result == WorkloadResult.SUCCEEDED
    assert observation.detail.startswith("remote outcome: succeeded")
    assert transfer.discoveries == 3


@pytest.mark.parametrize(
    ("declared_path", "declared_hash", "error"),
    (
        ("../private-key", None, "unsafe incremental artifact path"),
        ("trainer_state.json", "0" * 64, "hash mismatch"),
    ),
)
def test_incremental_manifest_rejects_path_escape_and_hash_mismatch(
    tmp_path: Path,
    declared_path: str,
    declared_hash: str | None,
    error: str,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    remote = tmp_path / "remote"
    write_incremental_checkpoint(remote, declared_path=declared_path, declared_hash=declared_hash)
    transfer = FakeTransfer(remote)
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=running_status,
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path, incremental=True), run_dir)

    assert observation.result == WorkloadResult.FAILED
    assert observation.disposition == ArtifactDisposition.UNAVAILABLE
    assert error in observation.detail


def test_incremental_contract_failure_reports_already_verified_checkpoint_as_partial(
    tmp_path: Path,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    remote = tmp_path / "remote"
    write_incremental_checkpoint(remote, checkpoint="checkpoint-25")
    write_incremental_checkpoint(
        remote,
        checkpoint="checkpoint-50",
        declared_hash="0" * 64,
    )
    transfer = FakeTransfer(remote)
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=running_status,
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path, incremental=True), run_dir)

    assert observation.result == WorkloadResult.FAILED
    assert observation.disposition == ArtifactDisposition.PARTIAL_RECOVERED
    assert "hash mismatch" in observation.detail
    assert (
        run_dir / "receipts/incremental/checkpoints/checkpoint-25/trainer_state.json"
    ).is_file()


def test_incremental_declared_bytes_cannot_exceed_encrypted_storage_capacity(
    tmp_path: Path,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    remote = tmp_path / "remote"
    write_incremental_checkpoint(remote, declared_size=1_000_000_001)
    transfer = FakeTransfer(remote)
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=running_status,
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(
        make_request(tmp_path, incremental=True, storage_gb=1),
        run_dir,
    )

    assert observation.result == WorkloadResult.FAILED
    assert "aggregate bytes exceed" in observation.detail
    assert not any(path.endswith("checkpoint-25") for path in transfer.downloads)


def test_incremental_aggregate_cap_includes_durable_pending_receipts(
    tmp_path: Path,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    remote = tmp_path / "remote"
    write_incremental_checkpoint(
        remote,
        checkpoint="checkpoint-50",
        declared_size=600_000_000,
    )
    state_root = run_dir / "receipts" / "incremental-state"
    state_root.mkdir(parents=True)
    prior_path = "checkpoints/checkpoint-25/checkpoint-complete.json"
    prior_key = hashlib.sha256(prior_path.encode()).hexdigest()
    (state_root / f"{prior_key}.json").write_text(
        json.dumps(
            {
                "protocol": "incremental-mirror-receipt/1",
                "status": "pending",
                "run_id": "run-remote",
                "manifest_path": prior_path,
                "manifest_sha256": "f" * 64,
                "files": [
                    {
                        "path": "trainer_state.json",
                        "size": 600_000_000,
                        "sha256": "e" * 64,
                    }
                ],
            }
        )
    )
    transfer = FakeTransfer(remote)
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=running_status,
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(
        make_request(tmp_path, incremental=True, storage_gb=1),
        run_dir,
    )

    assert observation.result == WorkloadResult.FAILED
    assert "aggregate bytes exceed" in observation.detail
    assert not any(path.endswith("checkpoint-50") for path in transfer.downloads)


def test_incremental_transfer_reserves_controller_free_space_before_copy(
    tmp_path: Path,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    remote = tmp_path / "remote"
    write_incremental_checkpoint(remote)
    transfer = FakeTransfer(remote)
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=running_status,
        free_space_bytes=lambda _path: 0,
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path, incremental=True), run_dir)

    assert observation.result == WorkloadResult.FAILED
    assert "controller free space reserve" in observation.detail
    assert not any(path.endswith("checkpoint-25") for path in transfer.downloads)


def test_incremental_manifest_symlink_is_rejected(tmp_path: Path) -> None:
    run_dir = prepare_run_dir(tmp_path)
    remote = tmp_path / "remote"
    manifest = write_incremental_checkpoint(remote)
    target = manifest.with_name("real-completion.json")
    manifest.replace(target)
    manifest.symlink_to(target.name)
    transfer = FakeTransfer(remote)
    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=running_status,
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path, incremental=True), run_dir)

    assert observation.result == WorkloadResult.FAILED
    assert observation.disposition == ArtifactDisposition.UNAVAILABLE
    assert "symlink" in observation.detail


def test_incremental_publish_never_writes_through_a_local_symlink(
    tmp_path: Path,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    remote = tmp_path / "remote"
    manifest_path = write_incremental_checkpoint(remote)
    checkpoint_root = manifest_path.parent
    payload = (checkpoint_root / "trainer_state.json").read_bytes()
    nested_payload = checkpoint_root / "nested" / "trainer_state.json"
    nested_payload.parent.mkdir()
    nested_payload.write_bytes(payload)
    manifest = json.loads(manifest_path.read_text())
    manifest["files"][0]["path"] = "nested/trainer_state.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True))
    _artifact, terminal = write_terminal_artifact(remote)

    destination = run_dir / "receipts/incremental/checkpoints/checkpoint-25"
    destination.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "trainer_state.json"
    sentinel.write_bytes(b"outside sentinel\n")
    (destination / "nested").symlink_to(outside, target_is_directory=True)

    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(FakeTransfer(remote)),
        host_key_scanner=fixed_host_key,
        status_fetcher=lambda _url, _token: remote_status(terminal),
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path, incremental=True), run_dir)

    assert observation.result == WorkloadResult.FAILED
    assert "symlink" in observation.detail
    assert sentinel.read_bytes() == b"outside sentinel\n"


def test_conflicting_checkpoint_completion_after_interruption_is_rejected(
    tmp_path: Path,
) -> None:
    run_dir = prepare_run_dir(tmp_path)
    remote = tmp_path / "remote"
    manifest_path = write_incremental_checkpoint(remote)

    class PendingTransfer(FakeTransfer):
        def download(
            self,
            source_root: Path | str,
            destination_root: Path,
            manifest: Sequence[Mapping[str, object]],
        ) -> TransferReceipt:
            if str(source_root).endswith("/checkpoints/checkpoint-25"):
                raise TransferUnavailable("injected interruption")
            return super().download(source_root, destination_root, manifest)

    transfer = PendingTransfer(remote)
    calls = 0

    def fetch_status(_url: str, _token: str) -> Mapping[str, object]:
        nonlocal calls
        calls += 1
        if calls == 2:
            changed = json.loads(manifest_path.read_text())
            changed["unexpected_mutation"] = True
            manifest_path.write_text(json.dumps(changed, sort_keys=True))
        return remote_status()

    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory_for(transfer),
        host_key_scanner=fixed_host_key,
        status_fetcher=fetch_status,
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path, incremental=True), run_dir)

    assert observation.result == WorkloadResult.FAILED
    assert observation.disposition == ArtifactDisposition.UNAVAILABLE
    assert "conflicting incremental completion" in observation.detail
