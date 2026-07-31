from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from decimal import Decimal
from pathlib import Path

from runpod_jobrunner.lifecycle import ArtifactDisposition, WorkloadResult
from runpod_jobrunner.remote_executor import RunPodRemoteExecutor
from runpod_jobrunner.runpod_provider import PodObservation
from runpod_jobrunner.transfer import LocalTransfer, TransferReceipt


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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

    def upload(
        self,
        source_root: Path,
        destination_root: Path | str,
        manifest: Sequence[Mapping[str, object]],
    ) -> TransferReceipt:
        self.uploads.append(str(destination_root))
        target = self.remote / str(destination_root).lstrip("/")
        return LocalTransfer().upload(source_root, target, manifest)

    def download(
        self,
        source_root: Path | str,
        destination_root: Path,
        manifest: Sequence[Mapping[str, object]],
    ) -> TransferReceipt:
        self.downloads.append(str(source_root))
        source = self.remote / str(source_root).lstrip("/")
        return LocalTransfer().upload(source, destination_root, manifest)


def make_request(tmp_path: Path) -> dict[str, object]:
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    payload = b"allowed input\n"
    (input_root / "sft.jsonl").write_bytes(payload)
    return {
        "protocol": "controller-request/1",
        "remote": {
            "protocol": "run-request/1",
            "run_id": "run-remote",
            "bundle_hash": "a" * 64,
            "image_digest": "example.invalid/image@sha256:" + "b" * 64,
            "runner_version": "0.1.0",
            "phases": {},
            "limits": {
                "max_elapsed_seconds": 600,
                "max_cost_usd": "0.50",
                "usd_per_hour": "0.10",
            },
            "heartbeat_interval_seconds": 5,
            "termination_grace_seconds": 10,
            "storage": {"encrypted": True, "mount": "/workspace", "required_gb": 10},
            "artifact_manifest_path": "artifacts/manifest.json",
        },
        "provider": {"terminate_at": "2099-01-01T00:00:00Z"},
        "controller": {
            "input_root": str(input_root),
            "input_files": [{"path": "sft.jsonl", "size": len(payload), "sha256": digest(payload)}],
            "artifact_manifest_path": "artifacts/manifest.json",
            "status_token_sha256": digest(b"run-token"),
            "remote_run_root": "/workspace/runpod-jobrunner/runs/run-remote",
        },
    }


def test_remote_executor_uses_status_truth_and_recovers_exact_artifacts(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "run-remote"
    (run_dir / "secrets").mkdir(parents=True)
    (run_dir / "secrets" / "status-token").write_text("run-token\n")
    (run_dir / "state.json").write_text(json.dumps({"resource": {"id": "pod-one"}}))
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

    executor = RunPodRemoteExecutor(
        FakeAPI(),
        ssh_key_file=tmp_path / "key",
        transfer_factory=transfer_factory,
        host_key_scanner=lambda host, port: (
            ssh_attempts.append((host, port)) or "[192.0.2.10]:22022 ssh-ed25519 AAAA"
        ),
        status_fetcher=lambda url, token: {
            "protocol": "run-status/1",
            "run_id": "run-remote",
            "state": "terminal",
            "phase": "package",
            "heartbeat_age_seconds": 0.1,
            "heartbeat_monotonic_seconds": 12.0,
            "child": None,
            "latest_event_sequence": 8,
            "terminal_result": {
                "outcome": "succeeded",
                "reason": "all_enabled_phases_completed",
                "phase": None,
                "elapsed_seconds": 12.0,
                "estimated_cost_usd": "0.01",
                "completed_phases": ["verify", "package"],
                "phase_exit_codes": {"verify": 0, "package": 0},
                "artifact_manifest_sha256": digest(manifest_bytes),
                "artifact_manifest_size": len(manifest_bytes),
            },
        },
        sleep=lambda _seconds: None,
    )

    observation = executor.execute(make_request(tmp_path), run_dir)

    assert observation.result == WorkloadResult.SUCCEEDED
    assert observation.disposition == ArtifactDisposition.VERIFIED
    assert ssh_attempts == [("192.0.2.10", 22022)]
    assert transfer.uploads == [
        "/workspace/runpod-jobrunner/runs/run-remote/input",
        "/workspace/runpod-jobrunner/runs/run-remote",
    ]
    assert (run_dir / "receipts" / "artifacts" / "result.bin").read_bytes() == artifact
    assert "run-token" not in (run_dir / "remote-status.json").read_text()


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
