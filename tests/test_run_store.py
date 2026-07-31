from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from runpod_jobrunner.lifecycle import (
    ArtifactDisposition,
    LifecycleController,
    WorkloadResult,
)
from runpod_jobrunner.protocol import validate_protocol
from runpod_jobrunner.provider import MemoryRunPod
from runpod_jobrunner.run_store import (
    InvalidRunIdError,
    RunAlreadyExistsError,
    RunStore,
)


def test_default_root_uses_xdg_state_home(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    state_home = tmp_path / "state-home"
    monkeypatch.setenv("XDG_STATE_HOME", str(state_home))

    store = RunStore()

    assert store.runs_root == state_home / "runpod-jobrunner" / "runs"


@pytest.mark.parametrize("run_id", ["", ".", "..", "../escape", "with/slash", "white space"])
def test_run_ids_cannot_escape_the_state_root(tmp_path: Path, run_id: str) -> None:
    store = RunStore(tmp_path / "runs")

    with pytest.raises(InvalidRunIdError):
        store.paths(run_id)


def test_request_is_immutable_for_a_stable_run_identity(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "runs")
    state = {"run_id": "run-1", "lifecycle": "planned"}
    store.create_run("run-1", {"job": "first"}, state)

    assert store.create_run("run-1", {"job": "first"}, state) == store.read_state("run-1")
    with pytest.raises(RunAlreadyExistsError):
        store.create_run("run-1", {"job": "different"}, state)


def test_event_is_durable_before_projection_replace_and_recovers_projection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    store = RunStore(tmp_path / "runs")
    store.create_run("run-1", {"job": "noop"}, {"run_id": "run-1", "value": 1})
    raw_before = json.loads(store.paths("run-1").state.read_text())
    real_replace = __import__("os").replace

    def fail_projection_replace(source: str | Path, destination: str | Path) -> None:
        destination_path = Path(destination)
        if destination_path == store.paths("run-1").state:
            events = store.paths("run-1").events.read_text().splitlines()
            assert json.loads(events[-1])["projection"]["value"] == 2
            raise OSError("simulated crash before projection replace")
        real_replace(source, destination)

    monkeypatch.setattr("runpod_jobrunner.run_store.os.replace", fail_projection_replace)
    with (
        pytest.raises(OSError, match="simulated crash"),
        store.transaction("run-1") as transaction,
    ):
        transaction.commit_state("value_changed", {"run_id": "run-1", "value": 2})

    # The replace did not happen, so state.json is stale. The fsynced event journal is
    # authoritative and reconstructs the effective projection after restart.
    assert json.loads(store.paths("run-1").state.read_text()) == raw_before
    assert store.read_state("run-1")["value"] == 2


def test_run_lock_serializes_transactions(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "runs")
    store.create_run("run-1", {"job": "noop"}, {"run_id": "run-1"})
    attempted = threading.Event()
    acquired = threading.Event()

    def contender() -> None:
        attempted.set()
        with store.transaction("run-1"):
            acquired.set()

    with store.transaction("run-1"):
        thread = threading.Thread(target=contender, daemon=True)
        thread.start()
        assert attempted.wait(timeout=1)
        assert not acquired.wait(timeout=0.05)

    thread.join(timeout=1)
    assert acquired.is_set()


def test_corrupt_event_fails_closed(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "runs")
    store.create_run("run-1", {"job": "noop"}, {"run_id": "run-1"})
    with store.paths("run-1").events.open("ab") as events:
        events.write(b'{"sequence":')

    with pytest.raises(ValueError, match="corrupt event journal"):
        store.read_state("run-1")


def test_complete_json_without_record_terminator_fails_closed(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "runs")
    store.create_run("run-1", {"job": "noop"}, {"run_id": "run-1"})
    journal = store.paths("run-1").events
    journal.write_bytes(journal.read_bytes().removesuffix(b"\n"))

    with pytest.raises(ValueError, match="corrupt event journal"):
        store.read_state("run-1")


def test_closed_projection_repairs_a_missing_durable_closeout_receipt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    store = RunStore(tmp_path / "runs")
    controller = LifecycleController(store, MemoryRunPod())
    controller.plan("run-closed", {"job": "noop"}, approved_max_usd="1")
    controller.reconcile("run-closed")
    controller.reconcile("run-closed")
    controller.reconcile("run-closed")
    controller.record_workload_result("run-closed", WorkloadResult.FAILED)
    controller.record_artifact_disposition("run-closed", ArtifactDisposition.UNAVAILABLE)
    controller.reconcile("run-closed")
    controller.reconcile("run-closed")
    receipt_path = store.paths("run-closed").closeout_receipt
    real_replace = __import__("os").replace
    fail_once = True

    def fail_receipt_replace(source: str | Path, destination: str | Path) -> None:
        nonlocal fail_once
        if fail_once and Path(destination) == receipt_path:
            fail_once = False
            raise OSError("simulated crash before closeout receipt replace")
        real_replace(source, destination)

    monkeypatch.setattr("runpod_jobrunner.run_store.os.replace", fail_receipt_replace)
    with pytest.raises(OSError, match="simulated crash"):
        controller.reconcile("run-closed")

    assert not receipt_path.exists()
    closed = store.read_state("run-closed")
    receipt = json.loads(receipt_path.read_text())

    assert closed["lifecycle"] == "closed"
    validate_protocol(receipt, "closeout-receipt/1", subject="receipt")
    assert receipt["event_sequence"] == closed["event_sequence"]
