"""Internal provider contract and an observable in-memory RunPod fake."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Protocol, runtime_checkable


class ProviderError(RuntimeError):
    """A provider operation failed without a fabricated fallback."""


class ProviderOutcomeUnknown(ProviderError):
    """The effect may have happened; callers must reconcile before retrying."""

    def __init__(self, kind: str, operation_id: str) -> None:
        self.kind = kind
        self.operation_id = operation_id
        super().__init__(f"provider {kind} outcome is unknown for operation {operation_id}")


class ProviderResourceNotFound(ProviderError):
    """The requested provider resource is absent."""


class ProviderProtocolError(ProviderError):
    """The provider returned a response that violates the internal contract."""


class ProviderRejected(ProviderError):
    """A created or discovered resource violated a non-negotiable admission invariant."""

    def __init__(self, message: str, *, resource_id: str | None) -> None:
        self.resource_id = resource_id
        super().__init__(message)


class ProviderResourceState(StrEnum):
    PROVISIONED = "provisioned"
    STARTING = "starting"
    RUNNING = "running"
    STOPPED = "stopped"


@dataclass(frozen=True)
class ProviderCreateRequest:
    run_id: str
    operation_id: str
    resource_name: str
    spec: Mapping[str, object]


@dataclass(frozen=True)
class ProviderResource:
    id: str
    run_id: str
    create_operation_id: str
    name: str
    state: ProviderResourceState
    hourly_rate_usd: Decimal


@dataclass(frozen=True)
class DeleteReceipt:
    operation_id: str
    acknowledged: bool
    already_absent: bool = False


@dataclass(frozen=True)
class ProviderEffect:
    kind: str
    operation_id: str
    resource_id: str | None


@runtime_checkable
class Provider(Protocol):
    """The private provider seam used by lifecycle reconciliation."""

    def find_resources(self, create_operation_id: str) -> tuple[ProviderResource, ...]: ...

    def create(self, request: ProviderCreateRequest) -> ProviderResource: ...

    def get(self, resource_id: str) -> ProviderResource | None: ...

    def start(self, resource_id: str, operation_id: str) -> ProviderResource: ...

    def delete(self, resource_id: str, operation_id: str) -> DeleteReceipt: ...

    def current_spend_usd_per_hour(self, resource_id: str | None) -> Decimal: ...


class MemoryRunPod:
    """Deterministic provider fake with explicit unknown-outcome injection.

    It intentionally creates a new resource on every ``create`` call. This makes
    lifecycle tests prove that reconciliation, rather than fake idempotency, prevents
    duplicates after an unknown outcome.
    """

    def __init__(self) -> None:
        self.resources: dict[str, ProviderResource] = {}
        self.effects: list[ProviderEffect] = []
        self.before_effect: Callable[[str, str], None] | None = None
        self.forced_current_spend_usd_per_hour: Decimal | None = None
        self._fail_after_effect: list[str] = []
        self._next_resource_number = 1

    def fail_next_after_effect(self, kind: str) -> None:
        if kind not in {"provision", "start", "delete"}:
            raise ValueError(f"unsupported provider effect: {kind}")
        self._fail_after_effect.append(kind)

    def find_resources(self, create_operation_id: str) -> tuple[ProviderResource, ...]:
        return tuple(
            resource
            for resource in self.resources.values()
            if resource.create_operation_id == create_operation_id
        )

    def create(self, request: ProviderCreateRequest) -> ProviderResource:
        resource_id = f"memory-pod-{self._next_resource_number:06d}"
        self._next_resource_number += 1
        self._before("provision", request.operation_id)
        effect = ProviderEffect("provision", request.operation_id, resource_id)
        self.effects.append(effect)
        rate = _decimal_from_spec(request.spec.get("hourly_rate_usd", "1"))
        resource = ProviderResource(
            id=resource_id,
            run_id=request.run_id,
            create_operation_id=request.operation_id,
            name=request.resource_name,
            state=ProviderResourceState.PROVISIONED,
            hourly_rate_usd=rate,
        )
        self.resources[resource_id] = resource
        self._raise_if_injected("provision", request.operation_id)
        return resource

    def get(self, resource_id: str) -> ProviderResource | None:
        return self.resources.get(resource_id)

    def start(self, resource_id: str, operation_id: str) -> ProviderResource:
        resource = self.resources.get(resource_id)
        if resource is None:
            raise ProviderResourceNotFound(f"provider resource is absent: {resource_id}")
        self._before("start", operation_id)
        self.effects.append(ProviderEffect("start", operation_id, resource_id))
        resource = replace(resource, state=ProviderResourceState.RUNNING)
        self.resources[resource_id] = resource
        self._raise_if_injected("start", operation_id)
        return resource

    def delete(self, resource_id: str, operation_id: str) -> DeleteReceipt:
        self._before("delete", operation_id)
        self.effects.append(ProviderEffect("delete", operation_id, resource_id))
        already_absent = resource_id not in self.resources
        self.resources.pop(resource_id, None)
        self._raise_if_injected("delete", operation_id)
        return DeleteReceipt(
            operation_id=operation_id,
            acknowledged=True,
            already_absent=already_absent,
        )

    def current_spend_usd_per_hour(self, resource_id: str | None) -> Decimal:
        if self.forced_current_spend_usd_per_hour is not None:
            return self.forced_current_spend_usd_per_hour
        if resource_id is None:
            return sum(
                (resource.hourly_rate_usd for resource in self.resources.values()), Decimal("0")
            )
        resource = self.resources.get(resource_id)
        if resource is None or resource.state == ProviderResourceState.STOPPED:
            return Decimal("0")
        return resource.hourly_rate_usd

    def _before(self, kind: str, operation_id: str) -> None:
        if self.before_effect is not None:
            self.before_effect(kind, operation_id)

    def _raise_if_injected(self, kind: str, operation_id: str) -> None:
        if kind in self._fail_after_effect:
            self._fail_after_effect.remove(kind)
            raise ProviderOutcomeUnknown(kind, operation_id)


def _decimal_from_spec(value: object) -> Decimal:
    if isinstance(value, float) or not isinstance(value, (str, Decimal, int)):
        raise ProviderProtocolError("provider money must be an integer, Decimal, or decimal string")
    try:
        result = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise ProviderProtocolError(f"invalid provider money: {value!r}") from error
    if not result.is_finite() or result < 0:
        raise ProviderProtocolError(f"invalid provider money: {value!r}")
    return result
