"""Crash-recoverable provider lifecycle reconciliation."""

from __future__ import annotations

import copy
import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, NoReturn, cast

from runpod_jobrunner.provider import (
    DeleteReceipt,
    Provider,
    ProviderCreateRequest,
    ProviderError,
    ProviderOutcomeUnknown,
    ProviderProtocolError,
    ProviderRejected,
    ProviderResource,
    ProviderResourceState,
)
from runpod_jobrunner.run_store import RunStore, RunTransaction

_OPERATION_NAMESPACE = uuid.UUID("c2ad7d61-943e-42c8-9353-b864fd25f7f0")
_PROVISION_VISIBILITY_GRACE_SECONDS = 30.0
_PROVISION_ABSENCE_GRACE_SECONDS = 5.0
_UNCERTAIN_PROVISION_STATUSES = frozenset({"dispatched", "outcome_unknown", "unresolved"})


class LifecycleState(StrEnum):
    PLANNED = "planned"
    PROVISIONING = "provisioning"
    STARTING = "starting"
    RUNNING = "running"
    RECOVERING = "recovering"
    DELETING = "deleting"
    CLOSED = "closed"


class WorkloadResult(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ArtifactDisposition(StrEnum):
    VERIFIED = "verified"
    PARTIAL_RECOVERED = "partial_recovered"
    UNAVAILABLE = "unavailable"
    NOT_APPLICABLE = "not_applicable"


class LifecycleError(RuntimeError):
    """Base class for lifecycle contract failures."""


class LifecycleConflictError(LifecycleError):
    """Observed provider state cannot be reconciled safely."""


class InvalidTransitionError(LifecycleError):
    """The requested lifecycle mutation is not valid from the current state."""


class LifecycleController:
    """Advance one run by one durable reconciliation step per call."""

    def __init__(
        self,
        store: RunStore,
        provider: Provider,
        *,
        now: Callable[[], datetime] | None = None,
        provision_visibility_grace_seconds: float = _PROVISION_VISIBILITY_GRACE_SECONDS,
    ) -> None:
        if provision_visibility_grace_seconds <= 0:
            raise ValueError("provision visibility grace must be positive")
        self.store = store
        self.provider = provider
        self._now = now or (lambda: datetime.now(UTC))
        self._provision_visibility_grace_seconds = provision_visibility_grace_seconds

    def plan(
        self,
        run_id: str,
        request: Mapping[str, object],
        *,
        approved_max_usd: str | Decimal,
    ) -> dict[str, Any]:
        approved = _money(approved_max_usd)
        initial_state: dict[str, object] = {
            "protocol": "run-status/1",
            "run_id": run_id,
            "lifecycle": LifecycleState.PLANNED,
            "workload_result": None,
            "approved_max_usd": approved,
            "resource": None,
            "operations": {},
            "recovery_reason": None,
            "closeout": {
                "artifact_disposition": None,
                "delete_acknowledged": False,
                "delete_already_absent": False,
                "provider_not_found": False,
                "current_spend_usd_per_hour": None,
                "current_spend_scope": None,
            },
        }
        stored_request = copy.deepcopy(dict(request))
        stored_request["approved_max_usd"] = approved
        return self.store.create_run(run_id, stored_request, initial_state)

    def status(self, run_id: str) -> dict[str, Any]:
        return self.store.read_state(run_id)

    def reconcile(self, run_id: str) -> dict[str, Any]:
        """Run exactly one durable lifecycle step.

        External mutations occur while the run flock is held. Every mutation has an
        intent committed by an earlier step, and unknown outcomes are journaled before
        being re-raised to the supervisor.
        """

        with self.store.transaction(run_id) as transaction:
            state = transaction.current_state()
            lifecycle = LifecycleState(state["lifecycle"])
            if lifecycle == LifecycleState.PLANNED:
                return self._intend_provision(transaction, state)
            if lifecycle == LifecycleState.PROVISIONING:
                return self._reconcile_provision(transaction, state)
            if lifecycle == LifecycleState.STARTING:
                return self._reconcile_start(transaction, state)
            if lifecycle in {
                LifecycleState.RUNNING,
                LifecycleState.RECOVERING,
                LifecycleState.CLOSED,
            }:
                return state
            if lifecycle == LifecycleState.DELETING:
                return self._reconcile_delete(transaction, state)
        raise AssertionError("unreachable lifecycle")

    def record_workload_result(
        self, run_id: str, result: WorkloadResult, *, detail: str | None = None
    ) -> dict[str, Any]:
        result = WorkloadResult(result)
        with self.store.transaction(run_id) as transaction:
            state = transaction.current_state()
            existing = state.get("workload_result")
            if existing == result:
                return state
            if existing is not None:
                raise LifecycleConflictError(
                    f"terminal workload result already recorded as {existing!r}"
                )
            if state["lifecycle"] == LifecycleState.CLOSED:
                raise InvalidTransitionError("cannot change workload result after closeout")
            state["workload_result"] = result
            state["lifecycle"] = LifecycleState.RECOVERING
            return transaction.commit_state(
                "workload_terminal",
                state,
                {"result": result, "detail": detail},
            )

    def record_artifact_disposition(
        self,
        run_id: str,
        disposition: ArtifactDisposition,
        *,
        detail: str | None = None,
    ) -> dict[str, Any]:
        disposition = ArtifactDisposition(disposition)
        with self.store.transaction(run_id) as transaction:
            state = transaction.current_state()
            closeout = _object(state, "closeout")
            recorded = {"status": disposition, "detail": detail}
            existing = closeout.get("artifact_disposition")
            if existing == recorded:
                return state
            if existing is not None:
                raise LifecycleConflictError("artifact disposition is already recorded")
            if state["lifecycle"] == LifecycleState.CLOSED:
                raise InvalidTransitionError("cannot change artifacts after closeout")
            closeout["artifact_disposition"] = recorded
            operations = _object(state, "operations")
            operations.setdefault("delete", _new_operation(run_id, "delete"))
            state["lifecycle"] = LifecycleState.DELETING
            return transaction.commit_state(
                "artifact_disposition_recorded",
                state,
                {"status": disposition, "detail": detail},
            )

    def request_stop(self, run_id: str, *, reason: str) -> dict[str, Any]:
        """Move any non-closed run toward deletion with an explicit disposition."""

        with self.store.transaction(run_id) as transaction:
            state = transaction.current_state()
            if state["lifecycle"] == LifecycleState.CLOSED:
                return state
            if state.get("workload_result") is None:
                state["workload_result"] = WorkloadResult.CANCELLED
            closeout = _object(state, "closeout")
            if closeout.get("artifact_disposition") is None:
                closeout["artifact_disposition"] = {
                    "status": ArtifactDisposition.UNAVAILABLE,
                    "detail": reason,
                }
            operations = _object(state, "operations")
            operations.setdefault("delete", _new_operation(run_id, "delete"))
            state["lifecycle"] = LifecycleState.DELETING
            return transaction.commit_state("stop_requested", state, {"reason": reason})

    def _intend_provision(
        self, transaction: RunTransaction, state: dict[str, Any]
    ) -> dict[str, Any]:
        operations = _object(state, "operations")
        operations.setdefault("provision", _new_operation(state["run_id"], "provision"))
        state["lifecycle"] = LifecycleState.PROVISIONING
        operation = _object(operations, "provision")
        return transaction.commit_state(
            "provider_operation_intended",
            state,
            {"kind": "provision", "operation_id": operation["id"]},
        )

    def _reconcile_provision(
        self, transaction: RunTransaction, state: dict[str, Any]
    ) -> dict[str, Any]:
        operations = _object(state, "operations")
        operation = _object(operations, "provision")
        operation_id = _string(operation, "id")
        try:
            matches = self.provider.find_resources(operation_id)
        except ProviderRejected as error:
            return self._record_provider_rejection(
                transaction, state, operation, operation_id, error
            )
        if len(matches) > 1:
            state["recovery_reason"] = "duplicate_provider_resources"
            operation["status"] = "conflict"
            operation["resource_ids"] = [resource.id for resource in matches]
            state["quarantined_resource_ids"] = list(operation["resource_ids"])
            state["resource"] = _resource_projection(matches[0])
            state["workload_result"] = WorkloadResult.CANCELLED
            closeout = _object(state, "closeout")
            closeout["artifact_disposition"] = {
                "status": ArtifactDisposition.UNAVAILABLE,
                "detail": "duplicate provider resources quarantined",
            }
            operations.setdefault("delete", _new_operation(state["run_id"], "delete"))
            state["lifecycle"] = LifecycleState.DELETING
            return transaction.commit_state(
                "provider_reconciliation_conflict_quarantined",
                state,
                {
                    "operation_id": operation_id,
                    "resource_ids": operation["resource_ids"],
                },
            )
        if matches:
            return self._record_provisioned(transaction, state, matches[0])

        operation_status = _string(operation, "status")
        if operation_status != "intended":
            return self._record_provision_visibility_pending(
                transaction, state, operation, operation_id
            )

        request = self.store.read_request(state["run_id"])
        provider_spec = request.get("provider", {})
        if not isinstance(provider_spec, dict):
            raise ProviderProtocolError("request.provider must be an object")
        create_request = ProviderCreateRequest(
            run_id=state["run_id"],
            operation_id=operation_id,
            resource_name=f"rjr-{state['run_id']}-{operation_id[-8:]}",
            spec=cast(dict[str, object], provider_spec),
        )
        operation["attempts"] = _integer(operation.get("attempts", 0)) + 1
        operation["status"] = "dispatched"
        operation["dispatched_at"] = self._now().isoformat()
        transaction.commit_state(
            "provider_operation_dispatched",
            state,
            {"kind": "provision", "operation_id": operation_id},
        )
        try:
            resource = self.provider.create(create_request)
        except ProviderRejected as error:
            return self._record_provider_rejection(
                transaction, state, operation, operation_id, error
            )
        except ProviderOutcomeUnknown as error:
            operation["status"] = "outcome_unknown"
            operation["last_error"] = str(error)
            transaction.commit_state(
                "provider_outcome_unknown",
                state,
                {"kind": "provision", "operation_id": operation_id},
            )
            raise
        except ProviderError as error:
            self._record_provider_error(transaction, state, operation, "provision", error)
        return self._record_provisioned(transaction, state, resource)

    def _record_provision_visibility_pending(
        self,
        transaction: RunTransaction,
        state: dict[str, Any],
        operation: dict[str, Any],
        operation_id: str,
    ) -> dict[str, Any]:
        """Observe a dispatched create without ever issuing it a second time."""

        now = self._now()
        first_value = operation.get("dispatched_at") or operation.get(
            "visibility_first_observed_at"
        )
        if not isinstance(first_value, str):
            first_value = now.isoformat()
            operation["visibility_first_observed_at"] = first_value
        try:
            first = datetime.fromisoformat(first_value)
        except ValueError:
            raise LifecycleConflictError("provision dispatch timestamp is invalid") from None
        if first.tzinfo is None or now.tzinfo is None:
            raise LifecycleConflictError("provision dispatch timestamp must be timezone-aware")
        elapsed = (now - first).total_seconds()
        if elapsed < 0:
            raise LifecycleConflictError("provision dispatch timestamp is in the future")
        operation["visibility_observations"] = (
            _integer(operation.get("visibility_observations", 0)) + 1
        )
        operation["last_visibility_observed_at"] = now.isoformat()
        if elapsed < self._provision_visibility_grace_seconds:
            return transaction.commit_state(
                "provider_resource_visibility_pending",
                state,
                {"operation_id": operation_id, "elapsed_seconds": elapsed},
            )

        detail = "provider create was dispatched but its exact resource did not become visible"
        operation["status"] = "unresolved"
        state["recovery_reason"] = "provision_dispatch_unresolved"
        state["workload_result"] = WorkloadResult.CANCELLED
        closeout = _object(state, "closeout")
        closeout["artifact_disposition"] = {
            "status": ArtifactDisposition.UNAVAILABLE,
            "detail": detail,
        }
        operations = _object(state, "operations")
        operations.setdefault("delete", _new_operation(state["run_id"], "delete"))
        state["lifecycle"] = LifecycleState.RECOVERING
        return transaction.commit_state(
            "provider_create_visibility_exhausted",
            state,
            {"operation_id": operation_id, "elapsed_seconds": elapsed},
        )

    def _record_provisioned(
        self,
        transaction: RunTransaction,
        state: dict[str, Any],
        resource: ProviderResource,
    ) -> dict[str, Any]:
        operations = _object(state, "operations")
        provision = _object(operations, "provision")
        if resource.run_id != state["run_id"]:
            raise ProviderProtocolError("provider resource run ID mismatch")
        if resource.create_operation_id != provision["id"]:
            raise ProviderProtocolError("provider resource operation ID mismatch")
        provision["status"] = "observed"
        provision["resource_id"] = resource.id
        operations.setdefault("start", _new_operation(state["run_id"], "start"))
        state["resource"] = _resource_projection(resource)
        state["lifecycle"] = LifecycleState.STARTING
        return transaction.commit_state(
            "resource_provisioned",
            state,
            {"resource_id": resource.id, "start_operation_id": operations["start"]["id"]},
        )

    def _reconcile_start(
        self, transaction: RunTransaction, state: dict[str, Any]
    ) -> dict[str, Any]:
        resource_projection = _object_value(state.get("resource"), "resource")
        resource_id = _string(resource_projection, "id")
        operations = _object(state, "operations")
        operation = _object(operations, "start")
        operation_id = _string(operation, "id")
        try:
            observed = self.provider.get(resource_id)
        except ProviderRejected as error:
            return self._record_provider_rejection(
                transaction, state, operation, operation_id, error
            )
        if observed is None:
            state["recovery_reason"] = "resource_disappeared_before_start"
            state["workload_result"] = WorkloadResult.CANCELLED
            operation["status"] = "resource_not_found"
            closeout = _object(state, "closeout")
            closeout["artifact_disposition"] = {
                "status": ArtifactDisposition.UNAVAILABLE,
                "detail": "provider resource disappeared before workload start",
            }
            operations.setdefault("delete", _new_operation(state["run_id"], "delete"))
            state["lifecycle"] = LifecycleState.DELETING
            return transaction.commit_state(
                "provider_resource_missing",
                state,
                {"resource_id": resource_id, "operation_id": operation_id},
            )
        state["resource"] = _resource_projection(observed)
        if observed.state == ProviderResourceState.RUNNING:
            operation["status"] = "observed_running"
            state["lifecycle"] = LifecycleState.RUNNING
            return transaction.commit_state("resource_running", state, {"resource_id": resource_id})

        operation["attempts"] = _integer(operation.get("attempts", 0)) + 1
        try:
            started = self.provider.start(resource_id, operation_id)
        except ProviderRejected as error:
            return self._record_provider_rejection(
                transaction, state, operation, operation_id, error
            )
        except ProviderOutcomeUnknown as error:
            operation["status"] = "outcome_unknown"
            operation["last_error"] = str(error)
            transaction.commit_state(
                "provider_outcome_unknown",
                state,
                {"kind": "start", "operation_id": operation_id},
            )
            raise
        except ProviderError as error:
            self._record_provider_error(transaction, state, operation, "start", error)
        state["resource"] = _resource_projection(started)
        operation["status"] = "acknowledged"
        if started.state == ProviderResourceState.RUNNING:
            state["lifecycle"] = LifecycleState.RUNNING
            return transaction.commit_state("resource_running", state, {"resource_id": resource_id})
        return transaction.commit_state("resource_starting", state, {"resource_id": resource_id})

    def _reconcile_delete(
        self, transaction: RunTransaction, state: dict[str, Any]
    ) -> dict[str, Any]:
        closeout = _object(state, "closeout")
        if closeout.get("artifact_disposition") is None:
            raise InvalidTransitionError("deletion requires an artifact disposition")
        operations = _object(state, "operations")
        operation = _object(operations, "delete")
        operation_id = _string(operation, "id")
        resource_value = state.get("resource")
        provision_value = operations.get("provision")
        if resource_value is None and isinstance(provision_value, dict):
            provision = cast(dict[str, Any], provision_value)
            provision_attempts = _integer(provision.get("attempts", 0))
            deadline_resolution_required = (
                provision_attempts > 0
                and provision.get("status") in _UNCERTAIN_PROVISION_STATUSES
            )
            termination_deadline: datetime | None = None
            termination_deadline_text: str | None = None
            if not closeout.get("provision_absence_confirmed"):
                provision_operation_id = _string(provision, "id")
                try:
                    matches = self.provider.find_resources(provision_operation_id)
                except ProviderRejected as error:
                    if error.resource_ids:
                        state["quarantined_resource_ids"] = list(error.resource_ids)
                        state["resource"] = {"id": error.resource_ids[0]}
                        closeout["provision_absence_first_observed_at"] = None
                        return transaction.commit_state(
                            "stopped_rejected_resource_adopted",
                            state,
                            {"resource_ids": list(error.resource_ids)},
                        )
                    raise
                if len(matches) > 1:
                    state["quarantined_resource_ids"] = [resource.id for resource in matches]
                    state["resource"] = _resource_projection(matches[0])
                    return transaction.commit_state(
                        "stopped_duplicate_resources_quarantined",
                        state,
                        {"resource_ids": state["quarantined_resource_ids"]},
                    )
                if matches:
                    state["resource"] = _resource_projection(matches[0])
                    closeout["provision_absence_first_observed_at"] = None
                    return transaction.commit_state(
                        "stopped_resource_adopted",
                        state,
                        {"resource_id": matches[0].id},
                    )
                if provision_attempts == 0:
                    closeout["provision_absence_confirmed"] = True
                    return transaction.commit_state(
                        "stopped_provision_not_dispatched",
                        state,
                        {"operation_id": provision_operation_id},
                    )
                if deadline_resolution_required:
                    termination_deadline_text, termination_deadline = (
                        self._provider_termination_deadline(state)
                    )
                now = self._now()
                if now.tzinfo is None:
                    raise LifecycleConflictError("current time must be timezone-aware")
                if termination_deadline is not None and now < termination_deadline:
                    closeout["provision_absence_first_observed_at"] = None
                    return transaction.commit_state(
                        "stopped_provision_termination_pending",
                        state,
                        {
                            "operation_id": provision_operation_id,
                            "termination_deadline": termination_deadline_text,
                        },
                    )
                first_value = closeout.get("provision_absence_first_observed_at")
                if not isinstance(first_value, str):
                    closeout["provision_absence_first_observed_at"] = now.isoformat()
                    return transaction.commit_state(
                        "stopped_provision_absence_observed",
                        state,
                        {"operation_id": provision_operation_id},
                    )
                try:
                    first = datetime.fromisoformat(first_value)
                except ValueError:
                    raise LifecycleConflictError("provision absence timestamp is invalid") from None
                if first.tzinfo is None:
                    raise LifecycleConflictError(
                        "provision absence timestamp must be timezone-aware"
                    )
                if termination_deadline is not None and first < termination_deadline:
                    closeout["provision_absence_first_observed_at"] = now.isoformat()
                    return transaction.commit_state(
                        "stopped_provision_absence_observed",
                        state,
                        {"operation_id": provision_operation_id},
                    )
                if (now - first).total_seconds() < _PROVISION_ABSENCE_GRACE_SECONDS:
                    return transaction.commit_state(
                        "stopped_provision_absence_pending",
                        state,
                        {"operation_id": provision_operation_id},
                    )
                closeout["provision_absence_confirmed"] = True
                if termination_deadline is not None:
                    closeout["provision_resolution"] = {
                        "source": "termination_deadline_elapsed",
                        "operation_id": provision_operation_id,
                        "termination_deadline": termination_deadline_text,
                        "absence_first_observed_at": first.isoformat(),
                        "confirmed_at": now.isoformat(),
                    }
                return transaction.commit_state(
                    "stopped_provision_absence_confirmed",
                    state,
                    {"operation_id": provision_operation_id},
                )
        resource_value = state.get("resource")
        resource_id = None
        if resource_value is not None:
            resource_id = _string(_object_value(resource_value, "resource"), "id")
        quarantined_value = state.get("quarantined_resource_ids")
        quarantined_ids: list[str] = []
        if isinstance(quarantined_value, list):
            quarantined_objects = cast(list[object], quarantined_value)
            if not all(isinstance(item, str) and item for item in quarantined_objects):
                raise LifecycleConflictError("quarantined resource IDs are invalid")
            quarantined_ids = cast(list[str], quarantined_objects)
        delete_ids = quarantined_ids or ([resource_id] if resource_id is not None else [])
        acknowledged_value = closeout.get("delete_acknowledged_resource_ids", [])
        if not isinstance(acknowledged_value, list):
            raise LifecycleConflictError("delete acknowledgement IDs are invalid")
        acknowledged_objects = cast(list[object], acknowledged_value)
        if not all(isinstance(item, str) for item in acknowledged_objects):
            raise LifecycleConflictError("delete acknowledgement IDs are invalid")
        acknowledged_ids = cast(list[str], acknowledged_objects)

        if not closeout.get("delete_acknowledged"):
            pending_ids = [item for item in delete_ids if item not in acknowledged_ids]
            if not pending_ids:
                closeout["delete_acknowledged"] = True
                closeout["delete_already_absent"] = not delete_ids
                operation["status"] = (
                    "acknowledged_all" if delete_ids else "not_required_no_resource"
                )
                return transaction.commit_state(
                    "delete_acknowledged",
                    state,
                    {
                        "operation_id": operation_id,
                        "reason": "all_resources_acknowledged"
                        if delete_ids
                        else "no_resource_was_created",
                    },
                )
            delete_resource_id = pending_ids[0]
            operation["attempts"] = _integer(operation.get("attempts", 0)) + 1
            try:
                receipt = self.provider.delete(delete_resource_id, operation_id)
            except ProviderOutcomeUnknown as error:
                operation["status"] = "outcome_unknown"
                operation["last_error"] = str(error)
                transaction.commit_state(
                    "provider_outcome_unknown",
                    state,
                    {"kind": "delete", "operation_id": operation_id},
                )
                raise
            except ProviderError as error:
                self._record_provider_error(transaction, state, operation, "delete", error)
            return self._record_delete_receipt(
                transaction, state, operation, receipt, delete_resource_id
            )

        if not closeout.get("provider_not_found"):
            if quarantined_ids:
                provision = _object(operations, "provision")
                remaining = self.provider.find_resources(_string(provision, "id"))
                absent = not remaining
            else:
                absent = resource_id is None or self.provider.get(resource_id) is None
            if absent:
                closeout["provider_not_found"] = True
                return transaction.commit_state(
                    "provider_absence_confirmed", state, {"resource_id": resource_id}
                )
            return transaction.commit_state(
                "provider_deletion_pending", state, {"resource_id": resource_id}
            )

        spend_scope = self._current_spend_scope(state, closeout, resource_id)
        existing_spend_scope = closeout.get("current_spend_scope")
        if existing_spend_scope is not None and existing_spend_scope != spend_scope:
            raise LifecycleConflictError("current spend scope differs from closeout proof")
        spend = (
            Decimal("0")
            if spend_scope["kind"] in {"provision_operation", "run"}
            else self.provider.current_spend_usd_per_hour(resource_id)
        )
        if not spend.is_finite() or spend < 0:
            raise ProviderProtocolError("provider current spend must be a non-negative Decimal")
        closeout["current_spend_usd_per_hour"] = _format_money(spend)
        closeout["current_spend_scope"] = spend_scope
        if spend == 0:
            self._assert_closeout_ready(state)
            state["lifecycle"] = LifecycleState.CLOSED
            return transaction.commit_state("closeout_completed", state)
        return transaction.commit_state(
            "provider_spend_observed",
            state,
            {
                "current_spend_usd_per_hour": _format_money(spend),
                "current_spend_scope": spend_scope,
            },
        )

    def _provider_termination_deadline(
        self, state: Mapping[str, object]
    ) -> tuple[str, datetime]:
        request = self.store.read_request(_string(state, "run_id"))
        provider_value = request.get("provider")
        if not isinstance(provider_value, Mapping):
            raise LifecycleConflictError("provider termination deadline is missing")
        provider = cast(Mapping[str, object], provider_value)
        deadline_text = provider.get("terminate_at")
        if not isinstance(deadline_text, str) or not deadline_text:
            raise LifecycleConflictError("provider termination deadline is missing")
        try:
            deadline = datetime.fromisoformat(deadline_text)
        except ValueError:
            raise LifecycleConflictError("provider termination deadline is invalid") from None
        if deadline.tzinfo is None:
            raise LifecycleConflictError(
                "provider termination deadline must be timezone-aware"
            )
        return deadline_text, deadline

    def _current_spend_scope(
        self,
        state: Mapping[str, object],
        closeout: Mapping[str, object],
        resource_id: str | None,
    ) -> dict[str, object]:
        if resource_id is not None:
            return {"kind": "resource", "resource_id": resource_id}
        request = self.store.read_request(_string(state, "run_id"))
        provider_value = request.get("provider")
        network_volume_id = (
            cast(Mapping[str, object], provider_value).get("network_volume_id")
            if isinstance(provider_value, Mapping)
            else None
        )
        if isinstance(network_volume_id, str) and network_volume_id:
            operations = _object_value(state.get("operations"), "operations")
            provision_value = operations.get("provision")
            if provision_value is None:
                return {"kind": "run", "proof": "provision_not_dispatched"}
            provision = _object_value(provision_value, "provision")
            if _integer(provision.get("attempts", 0)) == 0:
                return {
                    "kind": "provision_operation",
                    "operation_id": _string(provision, "id"),
                    "proof": "provision_not_dispatched",
                }
            if (
                provision.get("status") not in _UNCERTAIN_PROVISION_STATUSES
                or not self._provision_resolution_is_complete(state, closeout)
            ):
                return {"kind": "account"}
            return {
                "kind": "provision_operation",
                "operation_id": _string(provision, "id"),
                "proof": "termination_deadline_absence",
            }
        return {"kind": "account"}

    def _record_delete_receipt(
        self,
        transaction: RunTransaction,
        state: dict[str, Any],
        operation: dict[str, Any],
        receipt: DeleteReceipt,
        resource_id: str,
    ) -> dict[str, Any]:
        if not receipt.acknowledged:
            raise ProviderProtocolError("provider delete was not acknowledged")
        if receipt.operation_id != operation["id"]:
            raise ProviderProtocolError("provider delete receipt operation ID mismatch")
        operation["status"] = "acknowledged"
        closeout = _object(state, "closeout")
        acknowledged_value = closeout.setdefault("delete_acknowledged_resource_ids", [])
        if not isinstance(acknowledged_value, list):
            raise LifecycleConflictError("delete acknowledgement IDs are invalid")
        acknowledged_ids = cast(list[object], acknowledged_value)
        if resource_id not in acknowledged_ids:
            acknowledged_ids.append(resource_id)
        quarantined_value = state.get("quarantined_resource_ids")
        if isinstance(quarantined_value, list):
            all_resource_ids = cast(list[object], quarantined_value)
        else:
            resource_value = state.get("resource")
            all_resource_ids = (
                [_string(_object_value(resource_value, "resource"), "id")]
                if resource_value is not None
                else []
            )
        if all(isinstance(item, str) and item in acknowledged_ids for item in all_resource_ids):
            closeout["delete_acknowledged"] = True
            operation["status"] = "acknowledged_all"
        closeout["delete_already_absent"] = receipt.already_absent
        return transaction.commit_state(
            "delete_acknowledged",
            state,
            {
                "operation_id": receipt.operation_id,
                "already_absent": receipt.already_absent,
                "resource_id": resource_id,
            },
        )

    def _record_provider_error(
        self,
        transaction: RunTransaction,
        state: dict[str, Any],
        operation: dict[str, Any],
        kind: str,
        error: ProviderError,
    ) -> NoReturn:
        operation["status"] = "failed"
        operation["last_error"] = str(error)
        transaction.commit_state(
            "provider_operation_failed",
            state,
            {"kind": kind, "operation_id": operation["id"], "error": str(error)},
        )
        raise error

    def _record_provider_rejection(
        self,
        transaction: RunTransaction,
        state: dict[str, Any],
        operation: dict[str, Any],
        operation_id: str,
        error: ProviderRejected,
    ) -> dict[str, Any]:
        operation["status"] = "rejected"
        operation["last_error"] = str(error)
        state["workload_result"] = WorkloadResult.CANCELLED
        closeout = _object(state, "closeout")
        closeout["artifact_disposition"] = {
            "status": ArtifactDisposition.UNAVAILABLE,
            "detail": str(error),
        }
        operations = _object(state, "operations")
        operations.setdefault("delete", _new_operation(state["run_id"], "delete"))
        if error.resource_ids:
            state["quarantined_resource_ids"] = list(error.resource_ids)
            state["resource"] = {"id": error.resource_ids[0]}
        state["lifecycle"] = LifecycleState.DELETING
        return transaction.commit_state(
            "provider_resource_rejected",
            state,
            {
                "operation_id": operation_id,
                "resource_id": error.resource_id,
                "resource_ids": list(error.resource_ids),
                "reason": str(error),
            },
        )

    def _assert_closeout_ready(self, state: Mapping[str, object]) -> None:
        closeout = _object_value(state.get("closeout"), "closeout")
        resource_value = state.get("resource")
        resource_id = (
            None
            if resource_value is None
            else _string(_object_value(resource_value, "resource"), "id")
        )
        expected_spend_scope = self._current_spend_scope(
            state,
            closeout,
            resource_id,
        )
        required = (
            closeout.get("artifact_disposition") is not None,
            closeout.get("delete_acknowledged") is True,
            closeout.get("provider_not_found") is True,
            closeout.get("current_spend_usd_per_hour") == "0",
            closeout.get("current_spend_scope") == expected_spend_scope,
            self._provision_resolution_is_complete(state, closeout),
        )
        if not all(required):
            raise InvalidTransitionError("closeout proof is incomplete")

    def _provision_resolution_is_complete(
        self,
        state: Mapping[str, object],
        closeout: Mapping[str, object],
    ) -> bool:
        if state.get("resource") is not None:
            return True
        operations = _object_value(state.get("operations"), "operations")
        provision_value = operations.get("provision")
        if not isinstance(provision_value, Mapping):
            return True
        provision = cast(Mapping[str, object], provision_value)
        if (
            _integer(provision.get("attempts", 0)) == 0
            or provision.get("status") not in _UNCERTAIN_PROVISION_STATUSES
        ):
            return True
        resolution_value = closeout.get("provision_resolution")
        if not isinstance(resolution_value, Mapping):
            return False
        resolution = cast(Mapping[str, object], resolution_value)
        deadline_text, deadline = self._provider_termination_deadline(state)
        first = _aware_datetime_or_none(resolution.get("absence_first_observed_at"))
        confirmed = _aware_datetime_or_none(resolution.get("confirmed_at"))
        return (
            closeout.get("provision_absence_confirmed") is True
            and resolution.get("source") == "termination_deadline_elapsed"
            and resolution.get("operation_id") == provision.get("id")
            and resolution.get("termination_deadline") == deadline_text
            and first is not None
            and confirmed is not None
            and first >= deadline
            and (confirmed - first).total_seconds() >= _PROVISION_ABSENCE_GRACE_SECONDS
        )


def _new_operation(run_id: str, kind: str) -> dict[str, object]:
    stable = uuid.uuid5(_OPERATION_NAMESPACE, f"runpod-jobrunner/{run_id}/{kind}/1")
    return {"id": f"op-{stable}", "status": "intended", "attempts": 0}


def _aware_datetime_or_none(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _resource_projection(resource: ProviderResource) -> dict[str, object]:
    return {
        "id": resource.id,
        "run_id": resource.run_id,
        "create_operation_id": resource.create_operation_id,
        "name": resource.name,
        "state": resource.state,
        "hourly_rate_usd": _format_money(resource.hourly_rate_usd),
    }


def _money(value: object) -> str:
    if isinstance(value, float) or not isinstance(value, (str, Decimal)):
        raise TypeError("money must be a Decimal or decimal string")
    try:
        money = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"invalid money value: {value!r}") from error
    if not money.is_finite() or money < 0:
        raise ValueError(f"invalid money value: {value!r}")
    return _format_money(money)


def _format_money(money: Decimal) -> str:
    return "0" if money == 0 else format(money, "f")


def _object(parent: Mapping[str, object], key: str) -> dict[str, Any]:
    return _object_value(parent.get(key), key)


def _object_value(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise LifecycleConflictError(f"state field {name!r} must be an object")
    return cast(dict[str, Any], value)


def _string(parent: Mapping[str, object], key: str) -> str:
    value = parent.get(key)
    if not isinstance(value, str):
        raise LifecycleConflictError(f"state field {key!r} must be a string")
    return value


def _integer(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise LifecycleConflictError("operation attempts must be an integer")
    return value
