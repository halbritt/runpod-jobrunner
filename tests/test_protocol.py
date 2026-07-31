import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from runpod_jobrunner.lifecycle import ArtifactDisposition, LifecycleController, WorkloadResult
from runpod_jobrunner.protocol import (
    CanonicalJSONError,
    canonical_json,
    canonical_sha256,
    load_schema,
    validate_protocol,
)
from runpod_jobrunner.provider import MemoryRunPod
from runpod_jobrunner.run_store import RunStore
from runpod_jobrunner.runner import PHASE_ORDER, RemoteRunner

PROTOCOLS = (
    "artifact-manifest/1",
    "closeout-receipt/1",
    "input-manifest/1",
    "job-spec/1",
    "run-event/1",
    "run-request/1",
    "run-status/1",
)


def test_canonical_json_sorts_keys_and_preserves_exact_decimal_numbers() -> None:
    left = {"z": [Decimal("1.2300"), True, None], "a": {"b": 2}}
    right = {"a": {"b": 2}, "z": [Decimal("1.23"), True, None]}

    assert canonical_json(left) == b'{"a":{"b":2},"z":[1.23,true,null]}'
    assert canonical_json(left) == canonical_json(right)
    assert canonical_sha256(left) == canonical_sha256(right)


@pytest.mark.parametrize("value", [1.25, float("nan"), float("inf")])
def test_canonical_json_rejects_binary_floats(value: float) -> None:
    with pytest.raises(CanonicalJSONError, match="float"):
        canonical_json({"money": value})


def test_canonical_json_rejects_non_string_object_keys() -> None:
    with pytest.raises(CanonicalJSONError, match="string keys"):
        canonical_json({1: "ambiguous"})


@pytest.mark.parametrize("protocol", PROTOCOLS)
def test_protocol_registry_loads_valid_packaged_draft_2020_12_schema(protocol: str) -> None:
    schema = load_schema(protocol)

    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    Draft202012Validator.check_schema(schema)


def test_packaged_run_request_schema_accepts_the_current_runner_contract() -> None:
    record: dict[str, Any] = {
        "protocol": "run-request/1",
        "run_id": "run-schema-001",
        "bundle_hash": "a" * 64,
        "image_digest": "example.invalid/runner@sha256:" + "b" * 64,
        "runner_version": "0.1.0",
        "phases": {
            phase: {
                "enabled": phase == "verify",
                "argv": ["/opt/verify"] if phase == "verify" else [],
                "timeout_seconds": 30,
            }
            for phase in ("verify", "preflight", "train", "evaluate", "package")
        },
        "limits": {
            "max_elapsed_seconds": 600,
            "max_cost_usd": "0.50",
            "usd_per_hour": "0.24",
        },
        "heartbeat_interval_seconds": 5,
        "termination_grace_seconds": 10,
        "storage": {"encrypted": True, "mount": "/workspace", "required_gb": 10},
        "artifact_manifest_path": "artifacts/manifest.json",
    }

    schema = load_schema("run-request/1")

    Draft202012Validator.check_schema(schema)
    validate_protocol(record, "run-request/1", subject="request")

    record["phases"]["surprise"] = {
        "enabled": True,
        "argv": ["/opt/surprise"],
        "timeout_seconds": 30,
    }
    with pytest.raises(ValueError, match=r"request\.phases"):
        validate_protocol(record, "run-request/1", subject="request")


def test_run_event_schema_accepts_remote_and_controller_journal_records() -> None:
    remote_event = {
        "protocol": "run-event/1",
        "run_id": "run-schema-001",
        "sequence": 3,
        "kind": "phase_completed",
        "phase": "verify",
        "monotonic_seconds": 1.25,
        "recorded_at_unix": 1785513601.25,
        "exit_code": 0,
    }
    controller_event = {
        "protocol": "run-event/1",
        "sequence": 4,
        "recorded_at": "2026-07-31T12:00:01+00:00",
        "kind": "resource_running",
        "payload": {"resource_id": "pod-one"},
        "projection": {
            "protocol": "run-status/1",
            "run_id": "run-schema-001",
            "lifecycle": "running",
        },
    }

    schema = load_schema("run-event/1")

    Draft202012Validator.check_schema(schema)
    validate_protocol(remote_event, "run-event/1", subject="remote_event")
    validate_protocol(controller_event, "run-event/1", subject="controller_event")


def test_run_status_schema_accepts_remote_and_lifecycle_projections() -> None:
    remote_status = {
        "protocol": "run-status/1",
        "run_id": "run-schema-001",
        "state": "terminal",
        "phase": "package",
        "heartbeat_sequence": 8,
        "heartbeat_at_unix": 1785513601.25,
        "heartbeat_monotonic_seconds": 12.5,
        "child": None,
        "latest_event_sequence": 8,
        "terminal_result": {
            "outcome": "succeeded",
            "reason": "all_enabled_phases_completed",
            "phase": None,
            "elapsed_seconds": 12.5,
            "estimated_cost_usd": "0.01",
            "completed_phases": ["verify", "package"],
            "phase_exit_codes": {"verify": 0, "package": 0},
            "artifact_manifest_sha256": "c" * 64,
            "artifact_manifest_size": 4321,
        },
    }
    public_status = dict(remote_status)
    public_status.pop("heartbeat_at_unix")
    public_status.pop("heartbeat_sequence")
    public_status["heartbeat_age_seconds"] = 0.1
    lifecycle_status = {
        "protocol": "run-status/1",
        "run_id": "run-schema-001",
        "lifecycle": "closed",
        "workload_result": "succeeded",
        "approved_max_usd": "0.50",
        "resource": {
            "id": "pod-one",
            "run_id": "run-schema-001",
            "create_operation_id": "op-create",
            "name": "rjr-run-schema-001",
            "state": "running",
            "hourly_rate_usd": "0.24",
        },
        "operations": {
            "delete": {"id": "op-delete", "status": "acknowledged_all", "attempts": 1}
        },
        "recovery_reason": None,
        "closeout": {
            "artifact_disposition": {"status": "verified", "detail": "hashes matched"},
            "delete_acknowledged": True,
            "delete_already_absent": False,
            "provider_not_found": True,
            "current_spend_usd_per_hour": "0",
            "delete_acknowledged_resource_ids": ["pod-one"],
        },
        "event_sequence": 9,
    }

    schema = load_schema("run-status/1")

    Draft202012Validator.check_schema(schema)
    validate_protocol(remote_status, "run-status/1", subject="remote_status")
    validate_protocol(public_status, "run-status/1", subject="public_status")
    validate_protocol(lifecycle_status, "run-status/1", subject="lifecycle_status")


def test_artifact_manifest_schema_accepts_the_runner_verified_record() -> None:
    manifest: dict[str, Any] = {
        "protocol": "artifact-manifest/1",
        "run_id": "run-schema-001",
        "files": [
            {
                "path": "artifacts/adapter/model.safetensors",
                "size": 123456,
                "sha256": "d" * 64,
            }
        ],
    }

    schema = load_schema("artifact-manifest/1")

    Draft202012Validator.check_schema(schema)
    validate_protocol(manifest, "artifact-manifest/1", subject="manifest")

    manifest["files"][0]["path"] = "/workspace/private-output"
    with pytest.raises(ValueError, match=r"manifest.files.0.path"):
        validate_protocol(manifest, "artifact-manifest/1", subject="manifest")


def test_closeout_receipt_schema_requires_the_lifecycle_closeout_proof() -> None:
    receipt: dict[str, Any] = {
        "protocol": "closeout-receipt/1",
        "run_id": "run-schema-001",
        "lifecycle": "closed",
        "workload_result": "succeeded",
        "approved_max_usd": "0.50",
        "resource": {"id": "pod-one"},
        "operations": {
            "delete": {"id": "op-delete", "status": "acknowledged_all", "attempts": 1}
        },
        "recovery_reason": None,
        "closeout": {
            "artifact_disposition": {"status": "verified", "detail": "hashes matched"},
            "delete_acknowledged": True,
            "delete_already_absent": False,
            "provider_not_found": True,
            "current_spend_usd_per_hour": "0",
        },
        "event_sequence": 9,
    }

    schema = load_schema("closeout-receipt/1")

    Draft202012Validator.check_schema(schema)
    validate_protocol(receipt, "closeout-receipt/1", subject="receipt")

    receipt["closeout"]["current_spend_usd_per_hour"] = "0.01"
    with pytest.raises(ValueError, match="current_spend_usd_per_hour"):
        validate_protocol(receipt, "closeout-receipt/1", subject="receipt")


def test_current_remote_runner_records_conform_to_the_packaged_schemas(tmp_path: Path) -> None:
    request: dict[str, object] = {
        "protocol": "run-request/1",
        "run_id": "run-schema-runtime",
        "bundle_hash": "e" * 64,
        "image_digest": "example.invalid/runner@sha256:" + "f" * 64,
        "phases": {
            phase: {"enabled": False, "argv": [], "timeout_seconds": 1}
            for phase in PHASE_ORDER
        },
        "limits": {
            "max_elapsed_seconds": 30,
            "max_cost_usd": "1.00",
            "usd_per_hour": "1.00",
        },
        "heartbeat_interval_seconds": 1,
        "termination_grace_seconds": 1,
        "storage": {"encrypted": True, "mount": str(tmp_path), "required_gb": 1},
    }
    status_dir = tmp_path / "status"

    validate_protocol(request, "run-request/1", subject="request")
    RemoteRunner(request, status_dir).run()

    status = json.loads((status_dir / "status.json").read_text())
    validate_protocol(status, "run-status/1", subject="status")
    for sequence, event in enumerate(
        (json.loads(line) for line in (status_dir / "events.jsonl").read_text().splitlines()),
        start=1,
    ):
        validate_protocol(event, "run-event/1", subject=f"event[{sequence}]")


def test_current_closed_lifecycle_maps_to_the_closeout_receipt_schema(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "runs")
    controller = LifecycleController(store, MemoryRunPod())
    run_id = "run-schema-closeout"
    controller.plan(run_id, {"job": "schema-test"}, approved_max_usd="1.00")
    controller.reconcile(run_id)
    controller.reconcile(run_id)
    controller.reconcile(run_id)
    controller.record_workload_result(run_id, WorkloadResult.FAILED, detail="test failure")
    controller.record_artifact_disposition(
        run_id,
        ArtifactDisposition.UNAVAILABLE,
        detail="no artifact expected",
    )
    controller.reconcile(run_id)
    controller.reconcile(run_id)
    closed = controller.reconcile(run_id)

    validate_protocol(closed, "run-status/1", subject="closed")
    for sequence, event in enumerate(store.read_events(run_id), start=1):
        validate_protocol(event, "run-event/1", subject=f"event[{sequence}]")
    receipt = dict(closed)
    receipt["protocol"] = "closeout-receipt/1"
    validate_protocol(receipt, "closeout-receipt/1", subject="receipt")
