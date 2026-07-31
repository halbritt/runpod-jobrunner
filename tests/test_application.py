from __future__ import annotations

import hashlib
import json
import sys
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml

from runpod_jobrunner.application import (
    ApplicationError,
    ApprovalError,
    ExecutionObservation,
    JobRunner,
    LocalRunnerExecutor,
    SupervisorEngine,
)
from runpod_jobrunner.bundle import compute_bundle_hash
from runpod_jobrunner.lifecycle import ArtifactDisposition, LifecycleController, LifecycleState
from runpod_jobrunner.protocol import validate_protocol
from runpod_jobrunner.provider import MemoryRunPod, ProviderCreateRequest
from runpod_jobrunner.run_store import RunStore

PHASES = ("verify", "preflight", "train", "evaluate", "package")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def make_bundle(root: Path, *, fail: bool = False) -> Path:
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    payload = b"exact input\n"
    (inputs / "input.txt").write_bytes(payload)
    manifest: dict[str, Any] = {
        "protocol": "input-manifest/1",
        "root": "inputs",
        "files": [{"path": "input.txt", "size": len(payload), "sha256": sha256(payload)}],
    }
    artifact_script = (
        "import hashlib,json,os,pathlib;"
        "root=pathlib.Path(os.environ['RUNPOD_JOBRUNNER_STORAGE_MOUNT']);"
        "d=root/'artifacts';d.mkdir(parents=True,exist_ok=True);"
        "p=d/'result.txt';p.write_bytes(b'ok\\n');"
        "m={'protocol':'artifact-manifest/1','files':[{'path':'artifacts/result.txt',"
        "'size':3,'sha256':hashlib.sha256(b'ok\\n').hexdigest()}]};"
        "(d/'manifest.json').write_text(json.dumps(m))"
    )
    phase_map = {
        phase: {
            "enabled": phase == ("train" if fail else "package"),
            "argv": (
                [sys.executable, "-c", "import sys;sys.exit(17)"]
                if fail and phase == "train"
                else [sys.executable, "-c", artifact_script]
                if phase == "package"
                else []
            ),
            "timeout_seconds": 30,
        }
        for phase in PHASES
    }
    spec: dict[str, Any] = {
        "protocol": "job-spec/1",
        "name": "local-failure" if fail else "local-noop",
        "image": "ghcr.io/example/noop@sha256:" + "a" * 64,
        "runner": {"version": "0.1.0"},
        "resources": {
            "gpu_types": ["NVIDIA RTX 2000 Ada Generation"],
            "gpu_count": 1,
            "container_disk_gb": 10,
            "ports": ["22/tcp", "8080/http"],
            "storage": {"encrypted": True, "mount": "/workspace", "required_gb": 10},
        },
        "inputs": {"manifest": "input-manifest.json"},
        "phases": phase_map,
        "limits": {
            "max_elapsed_seconds": 60,
            "max_cost_usd": "0.50",
            "usd_per_hour": "0.24",
        },
        "heartbeat_interval_seconds": 1,
        "termination_grace_seconds": 1,
        "artifacts": {"manifest_path": "artifacts/manifest.json"},
        "lifecycle": {"delete_after_terminal": True},
    }
    spec["bundle_hash"] = compute_bundle_hash(spec, manifest)
    (root / "job.yaml").write_text(yaml.safe_dump(spec, sort_keys=False))
    (root / "input-manifest.json").write_text(json.dumps(manifest) + "\n")
    return root


class RecordingSupervisor:
    def __init__(self, store: RunStore) -> None:
        self.store = store
        self.launched: list[str] = []

    def launch(self, run_id: str, executable: Path, run_dir: Path) -> None:
        assert self.store.read_state(run_id)["lifecycle"] == LifecycleState.PLANNED
        assert self.store.paths(run_id).request.is_file()
        self.launched.append(run_id)

    def wake(self, run_id: str) -> None:
        self.launched.append(run_id)


class RecordingExecutor:
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, request: Mapping[str, object], run_dir: Path) -> ExecutionObservation:
        del request, run_dir
        self.calls += 1
        raise AssertionError("a fail-closed recovery must not dispatch the workload")


def test_check_does_not_require_a_supervisor_executable(tmp_path: Path) -> None:
    bundle = make_bundle(tmp_path / "bundle")
    store = RunStore(tmp_path / "runs")
    app = JobRunner(store, RecordingSupervisor(store))

    result = app.check(bundle)

    assert result["name"] == "local-noop"


def test_run_persists_intent_before_launch_and_never_uses_full_approval(tmp_path: Path) -> None:
    bundle = make_bundle(tmp_path / "bundle")
    store = RunStore(tmp_path / "runs")
    supervisor = RecordingSupervisor(store)
    app = JobRunner(
        store,
        supervisor,
        supervisor_executable=Path("/bin/true"),
        run_id_factory=lambda: "run-fixed",
    )

    run_id = app.run(bundle, approved_max_usd=Decimal("2.00"))

    request = store.read_request(run_id)
    assert supervisor.launched == ["run-fixed"]
    assert request["approved_max_usd"] == "2.00"
    assert request["remote"]["limits"]["max_cost_usd"] == "0.50"
    assert request["provider"]["terminate_at"].endswith("Z")
    assert request["provider"]["image"].endswith("@sha256:" + "a" * 64)


def test_run_rejects_job_cap_above_explicit_approval(tmp_path: Path) -> None:
    bundle = make_bundle(tmp_path / "bundle")
    store = RunStore(tmp_path / "runs")
    app = JobRunner(
        store,
        RecordingSupervisor(store),
        supervisor_executable=Path("/bin/true"),
    )

    with pytest.raises(ApprovalError, match="exceeds"):
        app.run(bundle, approved_max_usd=Decimal("0.49"))

    assert not store.runs_root.exists()


def test_run_rejects_a_second_active_run_by_default(tmp_path: Path) -> None:
    first_bundle = make_bundle(tmp_path / "first")
    second_bundle = make_bundle(tmp_path / "second")
    store = RunStore(tmp_path / "runs")
    run_ids = iter(("run-first", "run-second"))
    app = JobRunner(
        store,
        RecordingSupervisor(store),
        supervisor_executable=Path("/bin/true"),
        run_id_factory=lambda: next(run_ids),
    )
    app.run(first_bundle, approved_max_usd=Decimal("0.50"))

    with pytest.raises(ApplicationError, match="active run"):
        app.run(second_bundle, approved_max_usd=Decimal("0.50"))

    assert not store.paths("run-second").directory.exists()


def test_local_noop_reaches_verified_closed_state(tmp_path: Path) -> None:
    bundle = make_bundle(tmp_path / "bundle")
    store = RunStore(tmp_path / "runs")
    app = JobRunner(
        store,
        RecordingSupervisor(store),
        supervisor_executable=Path("/bin/true"),
        run_id_factory=lambda: "run-local",
    )
    run_id = app.run(bundle, approved_max_usd=Decimal("0.50"))
    provider = MemoryRunPod()
    engine = SupervisorEngine(store, provider, LocalRunnerExecutor())

    state = engine.drive(run_id)

    assert state["lifecycle"] == LifecycleState.CLOSED
    assert state["workload_result"] == "succeeded"
    assert state["closeout"]["artifact_disposition"]["status"] == "verified"
    assert provider.resources == {}
    recovered = store.paths(run_id).receipts / "artifacts" / "result.txt"
    assert recovered.read_bytes() == b"ok\n"
    receipt = json.loads(store.paths(run_id).closeout_receipt.read_text())
    validate_protocol(receipt, "closeout-receipt/1", subject="receipt")
    assert receipt["closeout"] == state["closeout"]
    assert receipt["event_sequence"] == state["event_sequence"]


def test_missing_resource_before_start_closes_without_dispatching_workload(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "runs")
    provider = MemoryRunPod()
    controller = LifecycleController(store, provider)
    controller.plan("run-missing", {"job": "noop"}, approved_max_usd="0.50")
    controller.reconcile("run-missing")
    starting = controller.reconcile("run-missing")
    provider.resources.pop(starting["resource"]["id"])
    executor = RecordingExecutor()

    closed = SupervisorEngine(store, provider, executor).drive("run-missing")

    assert executor.calls == 0
    assert closed["lifecycle"] == LifecycleState.CLOSED
    assert closed["workload_result"] == "cancelled"
    assert closed["recovery_reason"] == "resource_disappeared_before_start"
    assert closed["closeout"]["artifact_disposition"] == {
        "status": ArtifactDisposition.UNAVAILABLE,
        "detail": "provider resource disappeared before workload start",
    }


def test_legacy_fail_closed_recovery_never_dispatches_workload(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "runs")
    provider = MemoryRunPod()
    controller = LifecycleController(store, provider)
    controller.plan("run-legacy", {"job": "noop"}, approved_max_usd="0.50")
    controller.reconcile("run-legacy")
    starting = controller.reconcile("run-legacy")
    provider.resources.pop(starting["resource"]["id"])
    with store.transaction("run-legacy") as transaction:
        state = transaction.current_state()
        state["lifecycle"] = LifecycleState.RECOVERING
        state["recovery_reason"] = "resource_disappeared_before_start"
        transaction.commit_state("legacy_recovery_fixture", state)
    executor = RecordingExecutor()

    closed = SupervisorEngine(store, provider, executor).drive("run-legacy")

    assert executor.calls == 0
    assert closed["lifecycle"] == LifecycleState.CLOSED
    assert closed["workload_result"] == "cancelled"
    assert closed["closeout"]["artifact_disposition"]["status"] == "unavailable"


def test_duplicate_quarantine_closes_without_dispatching_workload(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "runs")
    provider = MemoryRunPod()
    controller = LifecycleController(store, provider)
    controller.plan("run-duplicate", {"job": "noop"}, approved_max_usd="0.50")
    intent = controller.reconcile("run-duplicate")
    operation_id = intent["operations"]["provision"]["id"]
    create = ProviderCreateRequest(
        run_id="run-duplicate",
        operation_id=operation_id,
        resource_name="duplicate-test",
        spec={},
    )
    provider.create(create)
    provider.create(create)
    executor = RecordingExecutor()

    closed = SupervisorEngine(store, provider, executor).drive("run-duplicate")

    assert executor.calls == 0
    assert closed["lifecycle"] == LifecycleState.CLOSED
    assert closed["workload_result"] == "cancelled"
    assert provider.resources == {}


def test_local_failed_phase_still_deletes_and_closes(tmp_path: Path) -> None:
    bundle = make_bundle(tmp_path / "bundle", fail=True)
    store = RunStore(tmp_path / "runs")
    app = JobRunner(
        store,
        RecordingSupervisor(store),
        supervisor_executable=Path("/bin/true"),
        run_id_factory=lambda: "run-failed",
    )
    run_id = app.run(bundle, approved_max_usd=Decimal("0.50"))
    provider = MemoryRunPod()

    state = SupervisorEngine(store, provider, LocalRunnerExecutor()).drive(run_id)

    assert state["lifecycle"] == LifecycleState.CLOSED
    assert state["workload_result"] == "failed"
    assert state["closeout"]["artifact_disposition"]["status"] == "unavailable"
    assert provider.resources == {}
