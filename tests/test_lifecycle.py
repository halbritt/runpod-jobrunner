from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from runpod_jobrunner.lifecycle import (
    ArtifactDisposition,
    LifecycleController,
    LifecycleState,
    WorkloadResult,
)
from runpod_jobrunner.provider import (
    MemoryRunPod,
    ProviderCreateRequest,
    ProviderOutcomeUnknown,
    ProviderRejected,
)
from runpod_jobrunner.run_store import RunStore, RunTransaction


def make_controller(tmp_path: Path) -> tuple[LifecycleController, RunStore, MemoryRunPod]:
    store = RunStore(tmp_path / "runs")
    provider = MemoryRunPod()
    return LifecycleController(store, provider), store, provider


class DelayedVisibilityRunPod(MemoryRunPod):
    def __init__(self, *, hidden_observations_after_create: int) -> None:
        super().__init__()
        self.hidden_observations_after_create = hidden_observations_after_create
        self.create_completed = False

    def find_resources(self, create_operation_id: str):  # type: ignore[no-untyped-def]
        if self.create_completed and self.hidden_observations_after_create > 0:
            self.hidden_observations_after_create -= 1
            return ()
        return super().find_resources(create_operation_id)

    def create(self, request: ProviderCreateRequest):  # type: ignore[no-untyped-def]
        try:
            return super().create(request)
        finally:
            self.create_completed = True


def reach_running(controller: LifecycleController, run_id: str = "run-1") -> dict[str, object]:
    controller.plan(
        run_id,
        {"job": "noop", "provider": {"image": "example.invalid/noop@sha256:abc"}},
        approved_max_usd="2.50",
    )
    assert controller.reconcile(run_id)["lifecycle"] == LifecycleState.PROVISIONING
    assert controller.reconcile(run_id)["lifecycle"] == LifecycleState.STARTING
    state = controller.reconcile(run_id)
    assert state["lifecycle"] == LifecycleState.RUNNING
    return state


def test_money_is_decimal_string_and_float_input_is_rejected(tmp_path: Path) -> None:
    controller, _, _ = make_controller(tmp_path)

    state = controller.plan("run-1", {"job": "noop"}, approved_max_usd=Decimal("2.50"))

    assert state["approved_max_usd"] == "2.50"
    with pytest.raises(TypeError, match="Decimal or decimal string"):
        controller.plan("run-2", {"job": "noop"}, approved_max_usd=2.5)  # type: ignore[arg-type]


def test_provision_and_start_intents_are_durable_before_provider_effects(tmp_path: Path) -> None:
    controller, store, provider = make_controller(tmp_path)
    observed: list[tuple[str, str]] = []

    def before_effect(kind: str, operation_id: str) -> None:
        events = [json.loads(line) for line in store.paths("run-1").events.read_text().splitlines()]
        observed.append((kind, events[-1]["kind"]))
        assert events[-1]["projection"]["operations"][kind]["id"] == operation_id

    provider.before_effect = before_effect
    controller.plan("run-1", {"job": "noop"}, approved_max_usd="1.00")

    provision_intent = controller.reconcile("run-1")
    assert provision_intent["lifecycle"] == LifecycleState.PROVISIONING
    assert provider.effects == []

    created = controller.reconcile("run-1")
    assert created["lifecycle"] == LifecycleState.STARTING
    assert [effect.kind for effect in provider.effects] == ["provision"]

    running = controller.reconcile("run-1")
    assert running["lifecycle"] == LifecycleState.RUNNING
    assert [effect.kind for effect in provider.effects] == ["provision", "start"]
    assert observed == [
        ("provision", "provider_operation_dispatched"),
        ("start", "resource_provisioned"),
    ]


def test_unknown_create_waits_for_delayed_exact_resource_visibility(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "runs")
    provider = DelayedVisibilityRunPod(hidden_observations_after_create=3)
    controller = LifecycleController(store, provider)
    controller.plan("run-1", {"job": "noop"}, approved_max_usd="1")
    controller.reconcile("run-1")
    provider.fail_next_after_effect("provision")

    with pytest.raises(ProviderOutcomeUnknown):
        controller.reconcile("run-1")

    for _ in range(3):
        pending = controller.reconcile("run-1")
        assert pending["lifecycle"] == LifecycleState.PROVISIONING
        assert pending["operations"]["provision"]["attempts"] == 1
        assert [effect.kind for effect in provider.effects].count("provision") == 1

    adopted = controller.reconcile("run-1")

    assert adopted["lifecycle"] == LifecycleState.STARTING
    assert adopted["resource"]["id"] == "memory-pod-000001"
    assert [effect.kind for effect in provider.effects].count("provision") == 1


def test_unknown_create_is_reconciled_without_a_duplicate(tmp_path: Path) -> None:
    controller, _, provider = make_controller(tmp_path)
    controller.plan("run-1", {"job": "noop"}, approved_max_usd="1")
    controller.reconcile("run-1")
    provider.fail_next_after_effect("provision")

    with pytest.raises(ProviderOutcomeUnknown):
        controller.reconcile("run-1")
    state = controller.status("run-1")
    assert state["lifecycle"] == LifecycleState.PROVISIONING
    assert state["operations"]["provision"]["status"] == "outcome_unknown"

    reconciled = controller.reconcile("run-1")
    assert reconciled["lifecycle"] == LifecycleState.STARTING
    assert len(provider.resources) == 1
    assert [effect.kind for effect in provider.effects].count("provision") == 1


def test_process_crash_after_create_is_reconciled_without_a_duplicate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    controller, _, provider = make_controller(tmp_path)
    controller.plan("run-1", {"job": "noop"}, approved_max_usd="1")
    controller.reconcile("run-1")
    original_commit = RunTransaction.commit_state
    crash_once = True

    def crash_before_observation_is_journaled(
        transaction: RunTransaction,
        kind: str,
        state: dict[str, object],
        payload: dict[str, object] | None = None,
    ) -> dict[str, object]:
        nonlocal crash_once
        if crash_once and kind == "resource_provisioned":
            crash_once = False
            raise SystemExit("simulated controller death")
        return original_commit(transaction, kind, state, payload)

    monkeypatch.setattr(RunTransaction, "commit_state", crash_before_observation_is_journaled)

    with pytest.raises(SystemExit, match="controller death"):
        controller.reconcile("run-1")
    assert len(provider.resources) == 1

    reconciled = controller.reconcile("run-1")
    assert reconciled["lifecycle"] == LifecycleState.STARTING
    assert len(provider.resources) == 1
    assert [effect.kind for effect in provider.effects].count("provision") == 1


def test_process_crash_after_create_does_not_redispatch_during_visibility_lag(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    store = RunStore(tmp_path / "runs")
    provider = DelayedVisibilityRunPod(hidden_observations_after_create=2)
    controller = LifecycleController(store, provider)
    controller.plan("run-1", {"job": "noop"}, approved_max_usd="1")
    controller.reconcile("run-1")
    original_commit = RunTransaction.commit_state
    crash_once = True

    def crash_before_observation_is_journaled(
        transaction: RunTransaction,
        kind: str,
        state: dict[str, object],
        payload: dict[str, object] | None = None,
    ) -> dict[str, object]:
        nonlocal crash_once
        if crash_once and kind == "resource_provisioned":
            crash_once = False
            raise SystemExit("simulated controller death")
        return original_commit(transaction, kind, state, payload)

    monkeypatch.setattr(RunTransaction, "commit_state", crash_before_observation_is_journaled)

    with pytest.raises(SystemExit, match="controller death"):
        controller.reconcile("run-1")

    durable = controller.status("run-1")
    assert durable["operations"]["provision"]["status"] == "dispatched"
    assert durable["operations"]["provision"]["attempts"] == 1
    assert [event["kind"] for event in store.read_events("run-1")][-1] == (
        "provider_operation_dispatched"
    )

    for _ in range(2):
        assert controller.reconcile("run-1")["lifecycle"] == LifecycleState.PROVISIONING
        assert [effect.kind for effect in provider.effects].count("provision") == 1

    assert controller.reconcile("run-1")["lifecycle"] == LifecycleState.STARTING
    assert [effect.kind for effect in provider.effects].count("provision") == 1


def test_dispatched_create_without_visible_resource_enters_bounded_fail_closed_recovery(
    tmp_path: Path,
) -> None:
    class Clock:
        value = datetime(2026, 7, 31, tzinfo=UTC)

        def now(self) -> datetime:
            return self.value

        def sleep(self, seconds: float) -> None:
            self.value += timedelta(seconds=seconds)

    clock = Clock()
    store = RunStore(tmp_path / "runs")
    provider = MemoryRunPod()
    controller = LifecycleController(
        store,
        provider,
        now=clock.now,
        provision_visibility_grace_seconds=5,
    )
    controller.plan("run-1", {"job": "noop"}, approved_max_usd="1")
    controller.reconcile("run-1")

    def crash_before_create(_kind: str, _operation_id: str) -> None:
        raise SystemExit("simulated death before provider effect")

    provider.before_effect = crash_before_create
    with pytest.raises(SystemExit, match="before provider effect"):
        controller.reconcile("run-1")
    provider.before_effect = None
    assert controller.status("run-1")["operations"]["provision"]["status"] == "dispatched"

    clock.sleep(6)
    recovering = controller.reconcile("run-1")

    assert recovering["lifecycle"] == LifecycleState.RECOVERING
    assert recovering["recovery_reason"] == "provision_dispatch_unresolved"
    assert recovering["workload_result"] == WorkloadResult.CANCELLED
    assert recovering["closeout"]["artifact_disposition"]["status"] == (
        ArtifactDisposition.UNAVAILABLE
    )
    assert provider.effects == []


def test_unknown_start_is_reconciled_from_resource_state(tmp_path: Path) -> None:
    controller, _, provider = make_controller(tmp_path)
    controller.plan("run-1", {"job": "noop"}, approved_max_usd="1")
    controller.reconcile("run-1")
    controller.reconcile("run-1")
    provider.fail_next_after_effect("start")

    with pytest.raises(ProviderOutcomeUnknown):
        controller.reconcile("run-1")
    state = controller.status("run-1")
    assert state["lifecycle"] == LifecycleState.STARTING
    assert state["operations"]["start"]["status"] == "outcome_unknown"

    running = controller.reconcile("run-1")
    assert running["lifecycle"] == LifecycleState.RUNNING
    assert [effect.kind for effect in provider.effects].count("start") == 1


def test_resource_missing_before_start_immediately_enters_fail_closed_deletion(
    tmp_path: Path,
) -> None:
    controller, _, provider = make_controller(tmp_path)
    controller.plan("run-1", {"job": "noop"}, approved_max_usd="1")
    controller.reconcile("run-1")
    starting = controller.reconcile("run-1")
    provider.resources.pop(starting["resource"]["id"])

    deleting = controller.reconcile("run-1")

    assert deleting["lifecycle"] == LifecycleState.DELETING
    assert deleting["workload_result"] == WorkloadResult.CANCELLED
    assert deleting["recovery_reason"] == "resource_disappeared_before_start"
    assert deleting["closeout"]["artifact_disposition"] == {
        "status": ArtifactDisposition.UNAVAILABLE,
        "detail": "provider resource disappeared before workload start",
    }
    assert deleting["operations"]["delete"]["status"] == "intended"


def test_failed_workload_can_close_but_result_is_not_lifecycle_state(tmp_path: Path) -> None:
    controller, _, provider = make_controller(tmp_path)
    reach_running(controller)

    recovering = controller.record_workload_result("run-1", WorkloadResult.FAILED)
    assert recovering["lifecycle"] == LifecycleState.RECOVERING
    assert recovering["workload_result"] == WorkloadResult.FAILED

    deleting = controller.record_artifact_disposition(
        "run-1",
        ArtifactDisposition.PARTIAL_RECOVERED,
        detail="terminal record and stderr recovered",
    )
    assert deleting["lifecycle"] == LifecycleState.DELETING
    assert deleting["closeout"]["artifact_disposition"]["status"] == (
        ArtifactDisposition.PARTIAL_RECOVERED
    )

    # Delete acknowledgement, provider not-found, and spend-zero are independent durable steps.
    assert controller.reconcile("run-1")["closeout"]["delete_acknowledged"] is True
    assert controller.reconcile("run-1")["closeout"]["provider_not_found"] is True
    closed = controller.reconcile("run-1")

    assert closed["lifecycle"] == LifecycleState.CLOSED
    assert closed["workload_result"] == WorkloadResult.FAILED
    assert closed["closeout"]["current_spend_usd_per_hour"] == "0"
    assert [effect.kind for effect in provider.effects][-1] == "delete"


def test_nonzero_current_spend_prevents_closeout(tmp_path: Path) -> None:
    controller, _, provider = make_controller(tmp_path)
    reach_running(controller)
    controller.record_workload_result("run-1", WorkloadResult.SUCCEEDED)
    controller.record_artifact_disposition("run-1", ArtifactDisposition.VERIFIED)
    controller.reconcile("run-1")  # delete acknowledgement
    controller.reconcile("run-1")  # provider not-found
    provider.forced_current_spend_usd_per_hour = Decimal("0.01")

    still_deleting = controller.reconcile("run-1")
    assert still_deleting["lifecycle"] == LifecycleState.DELETING
    assert still_deleting["closeout"]["current_spend_usd_per_hour"] == "0.01"

    provider.forced_current_spend_usd_per_hour = Decimal("0")
    assert controller.reconcile("run-1")["lifecycle"] == LifecycleState.CLOSED


def test_terminal_replay_does_not_regress_deletion_or_overwrite_artifacts(tmp_path: Path) -> None:
    controller, _, _ = make_controller(tmp_path)
    reach_running(controller)
    controller.record_workload_result("run-1", WorkloadResult.FAILED)
    deleting = controller.record_artifact_disposition("run-1", ArtifactDisposition.VERIFIED)

    replayed = controller.record_workload_result("run-1", WorkloadResult.FAILED)
    stopped = controller.request_stop("run-1", reason="late operator stop")

    assert replayed["lifecycle"] == LifecycleState.DELETING
    assert stopped["lifecycle"] == LifecycleState.DELETING
    assert (
        stopped["closeout"]["artifact_disposition"] == deleting["closeout"]["artifact_disposition"]
    )


def test_negative_zero_money_is_canonicalized(tmp_path: Path) -> None:
    controller, _, _ = make_controller(tmp_path)

    state = controller.plan("run-1", {"job": "noop"}, approved_max_usd=Decimal("-0"))

    assert state["approved_max_usd"] == "0"


def test_duplicate_resources_for_one_operation_are_quarantined_for_deletion(
    tmp_path: Path,
) -> None:
    controller, _, provider = make_controller(tmp_path)
    controller.plan("run-1", {"job": "noop"}, approved_max_usd="1")
    state = controller.reconcile("run-1")
    operation_id = state["operations"]["provision"]["id"]
    request = ProviderCreateRequest(
        run_id="run-1",
        operation_id=operation_id,
        resource_name="duplicate-test",
        spec={},
    )
    provider.create(request)
    provider.create(request)

    deleting = controller.reconcile("run-1")

    assert deleting["lifecycle"] == LifecycleState.DELETING
    assert deleting["quarantined_resource_ids"] == [
        "memory-pod-000001",
        "memory-pod-000002",
    ]
    assert deleting["workload_result"] == "cancelled"


def test_unknown_delete_is_retried_as_an_acknowledged_absence(tmp_path: Path) -> None:
    controller, _, provider = make_controller(tmp_path)
    reach_running(controller)
    controller.record_workload_result("run-1", WorkloadResult.FAILED)
    controller.record_artifact_disposition("run-1", ArtifactDisposition.UNAVAILABLE)
    provider.fail_next_after_effect("delete")

    with pytest.raises(ProviderOutcomeUnknown):
        controller.reconcile("run-1")

    # The resource is gone, but closeout still requires an explicit delete acknowledgement.
    acknowledged = controller.reconcile("run-1")
    assert acknowledged["closeout"]["delete_acknowledged"] is True
    assert acknowledged["closeout"]["delete_already_absent"] is True


def test_rejected_created_resource_moves_to_deletion_without_retry(
    tmp_path: Path,
) -> None:
    controller, _, provider = make_controller(tmp_path)
    controller.plan("run-1", {"job": "noop", "provider": {}}, approved_max_usd="1")
    controller.reconcile("run-1")
    create_calls = 0

    def reject(kind: str, operation_id: str) -> None:
        nonlocal create_calls
        if kind == "provision":
            create_calls += 1
            raise ProviderRejected(
                "provider rate exceeded admission cap",
                resource_id="rejected-pod",
            )

    provider.before_effect = reject

    deleting = controller.reconcile("run-1")

    assert deleting["lifecycle"] == LifecycleState.DELETING
    assert deleting["workload_result"] == "cancelled"
    assert deleting["resource"] == {"id": "rejected-pod"}
    assert deleting["closeout"]["artifact_disposition"]["status"] == "unavailable"
    assert create_calls == 1


def test_rejected_resource_found_after_unknown_create_never_creates_again(
    tmp_path: Path,
) -> None:
    class RejectingReconcileProvider(MemoryRunPod):
        reject_match = False

        def find_resources(self, create_operation_id: str):  # type: ignore[no-untyped-def]
            matches = super().find_resources(create_operation_id)
            if self.reject_match and matches:
                resource_id = matches[0].id
                self.resources.pop(resource_id)
                raise ProviderRejected(
                    "reconciled resource violated encryption invariant",
                    resource_id=resource_id,
                )
            return matches

    store = RunStore(tmp_path / "runs")
    provider = RejectingReconcileProvider()
    controller = LifecycleController(store, provider)
    controller.plan("run-1", {"job": "noop", "provider": {}}, approved_max_usd="1")
    controller.reconcile("run-1")
    provider.fail_next_after_effect("provision")

    with pytest.raises(ProviderOutcomeUnknown):
        controller.reconcile("run-1")
    provider.reject_match = True

    deleting = controller.reconcile("run-1")

    assert deleting["lifecycle"] == LifecycleState.DELETING
    assert deleting["resource"] == {"id": "memory-pod-000001"}
    assert [effect.kind for effect in provider.effects].count("provision") == 1


def test_stop_after_unknown_create_adopts_and_deletes_late_resource(tmp_path: Path) -> None:
    controller, _, provider = make_controller(tmp_path)
    controller.plan("run-1", {"job": "noop", "provider": {}}, approved_max_usd="1")
    controller.reconcile("run-1")
    provider.fail_next_after_effect("provision")
    with pytest.raises(ProviderOutcomeUnknown):
        controller.reconcile("run-1")

    stopped = controller.request_stop("run-1", reason="operator stop during unknown create")
    assert stopped["lifecycle"] == LifecycleState.DELETING
    assert stopped["resource"] is None

    adopted = controller.reconcile("run-1")

    assert adopted["lifecycle"] == LifecycleState.DELETING
    assert adopted["resource"]["id"] == "memory-pod-000001"
    assert adopted["closeout"]["delete_acknowledged"] is False
    assert [effect.kind for effect in provider.effects].count("provision") == 1
    controller.reconcile("run-1")
    assert provider.resources == {}


def test_stop_quarantines_and_deletes_every_duplicate_resource(tmp_path: Path) -> None:
    controller, _, provider = make_controller(tmp_path)
    controller.plan("run-1", {"job": "noop", "provider": {}}, approved_max_usd="1")
    intent = controller.reconcile("run-1")
    operation_id = intent["operations"]["provision"]["id"]
    request = ProviderCreateRequest(
        run_id="run-1",
        operation_id=operation_id,
        resource_name="duplicate-test",
        spec={},
    )
    provider.create(request)
    provider.create(request)
    controller.request_stop("run-1", reason="quarantine duplicate resources")

    quarantined = controller.reconcile("run-1")

    assert quarantined["quarantined_resource_ids"] == [
        "memory-pod-000001",
        "memory-pod-000002",
    ]
    state = quarantined
    for _ in range(6):
        state = controller.reconcile("run-1")
        if state["lifecycle"] == LifecycleState.CLOSED:
            break
    assert state["lifecycle"] == LifecycleState.CLOSED
    assert provider.resources == {}
    assert [effect.kind for effect in provider.effects].count("delete") == 2
