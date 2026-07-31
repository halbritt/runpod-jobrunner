from __future__ import annotations

import io
import json
from decimal import Decimal
from email.message import Message
from pathlib import Path
from typing import Any
from urllib.error import HTTPError

import pytest

from runpod_jobrunner.provider import (
    ProviderCreateRequest,
    ProviderOutcomeUnknown,
    ProviderRejected,
    ProviderResourceState,
)
from runpod_jobrunner.runpod_provider import (
    PodCreateRequest,
    PodObservation,
    ProviderProtocolError,
    RunPodHTTP,
    RunPodProvider,
    RunPodTransportOutcomeUnknown,
    SecurityInvariantError,
)


class Response(io.BytesIO):
    status = 200

    def __enter__(self) -> Response:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


class RecordingOpener:
    def __init__(self, replies: list[dict[str, Any] | list[Any] | None]) -> None:
        self.replies = list(replies)
        self.requests: list[Any] = []

    def open(self, request: Any, timeout: float) -> Response:
        assert timeout > 0
        self.requests.append(request)
        reply = self.replies.pop(0)
        return Response(b"" if reply is None else json.dumps(reply).encode())


def create_request() -> PodCreateRequest:
    return PodCreateRequest(
        name="rj-run-01-create-01",
        image="ghcr.io/example/runner@sha256:" + "a" * 64,
        gpu_type_id="NVIDIA RTX 2000 Ada Generation",
        gpu_count=1,
        container_disk_gb=10,
        volume_gb=20,
        volume_mount_path="/workspace",
        ports=("22/tcp", "8080/http"),
        environment={"RUNPOD_JOBRUNNER_RUN_ID": "run-01"},
        terminate_at="2026-08-01T00:00:00Z",
    )


def test_create_uses_encryption_key_and_provider_termination() -> None:
    opener = RecordingOpener(
        [
            {
                "data": {
                    "podFindAndDeployOnDemand": {
                        "id": "pod-1",
                        "name": "rj-run-01-create-01",
                        "desiredStatus": "RUNNING",
                        "costPerHr": 0.24,
                        "volumeEncrypted": True,
                    }
                }
            }
        ]
    )
    api = RunPodHTTP("secret", opener=opener)

    pod = api.create_encrypted_pod(create_request(), volume_key="a1" * 15)

    assert pod.id == "pod-1"
    assert pod.volume_encrypted is True
    sent = json.loads(opener.requests[0].data)
    values = sent["variables"]["input"]
    assert values["volumeKey"] == "a1" * 15
    assert values["terminateAfter"] == "2026-08-01T00:00:00Z"
    assert values["cloudType"] == "SECURE"
    assert opener.requests[0].get_header("Authorization") == "Bearer secret"
    assert "secret" not in opener.requests[0].full_url


@pytest.mark.parametrize("key", ["", "x" * 31, "not-a-key"])
def test_create_rejects_invalid_volume_key_before_network(key: str) -> None:
    opener = RecordingOpener([])
    api = RunPodHTTP("secret", opener=opener)

    with pytest.raises(ValueError, match="volume key"):
        api.create_encrypted_pod(create_request(), volume_key=key)

    assert opener.requests == []


def test_unencrypted_create_is_deleted_before_returning_error() -> None:
    opener = RecordingOpener(
        [
            {
                "data": {
                    "podFindAndDeployOnDemand": {
                        "id": "pod-unsafe",
                        "name": "rj-run-01-create-01",
                        "desiredStatus": "RUNNING",
                        "costPerHr": 0.24,
                        "volumeEncrypted": False,
                    }
                }
            },
            None,
        ]
    )
    api = RunPodHTTP("secret", opener=opener)

    with pytest.raises(SecurityInvariantError, match="unencrypted"):
        api.create_encrypted_pod(create_request(), volume_key="z" * 30)

    assert opener.requests[1].method == "DELETE"
    assert opener.requests[1].full_url.endswith("/pods/pod-unsafe")


def test_graphql_errors_are_typed_and_do_not_leak_key() -> None:
    opener = RecordingOpener(
        [{"errors": [{"message": "no capacity"}], "data": {"podFindAndDeployOnDemand": None}}]
    )
    api = RunPodHTTP("very-secret", opener=opener)

    with pytest.raises(ProviderProtocolError, match="no capacity") as caught:
        api.create_encrypted_pod(create_request(), volume_key="z" * 30)

    assert "very-secret" not in str(caught.value)


def test_exact_name_reconciliation_and_spend() -> None:
    opener = RecordingOpener(
        [
            [
                {"id": "one", "name": "rj-run-01-create-01", "volumeEncrypted": True},
                {"id": "two", "name": "rj-run-01-create-010", "volumeEncrypted": True},
            ],
            {"data": {"myself": {"currentSpendPerHr": 0.24}}},
        ]
    )
    api = RunPodHTTP("secret", opener=opener)

    matches = api.find_pods_by_exact_name("rj-run-01-create-01")

    assert [pod.id for pod in matches] == ["one"]
    assert str(api.current_spend_per_hour()) == "0.24"


def test_get_404_is_absent() -> None:
    class MissingOpener:
        def open(self, request: Any, timeout: float) -> Response:
            raise HTTPError(request.full_url, 404, "not found", Message(), None)

    assert RunPodHTTP("secret", opener=MissingOpener()).get_pod("gone") is None


class FakeAPI:
    def __init__(self) -> None:
        self.pods: dict[str, PodObservation] = {}
        self.created: tuple[PodCreateRequest, str] | None = None
        self.spend = Decimal("0")

    def create_encrypted_pod(self, request: PodCreateRequest, *, volume_key: str) -> PodObservation:
        self.created = (request, volume_key)
        pod = PodObservation(
            id="pod-1",
            name=request.name,
            desired_status="RUNNING",
            cost_per_hour=Decimal("0.24"),
            volume_encrypted=True,
            image=request.image,
            environment=request.environment,
        )
        self.pods[pod.id] = pod
        return pod

    def find_pods_by_exact_name(self, name: str) -> tuple[PodObservation, ...]:
        return tuple(pod for pod in self.pods.values() if pod.name == name)

    def list_pods(self) -> tuple[PodObservation, ...]:
        return tuple(self.pods.values())

    def get_pod(self, pod_id: str) -> PodObservation | None:
        return self.pods.get(pod_id)

    def start_pod(self, pod_id: str) -> PodObservation:
        return self.pods[pod_id]

    def delete_pod(self, pod_id: str) -> bool:
        return self.pods.pop(pod_id, None) is not None

    def current_spend_per_hour(self) -> Decimal:
        return self.spend


def provider_request() -> ProviderCreateRequest:
    return ProviderCreateRequest(
        run_id="run-01",
        operation_id="op-11111111-2222-3333-4444-555555555555",
        resource_name="rjr-run-01-55555555",
        spec={
            "image": "ghcr.io/example/runner@sha256:" + "a" * 64,
            "gpu_type_id": "NVIDIA RTX 2000 Ada Generation",
            "gpu_count": 1,
            "container_disk_gb": 10,
            "volume_gb": 20,
            "volume_mount_path": "/workspace",
            "ports": ["22/tcp", "8080/http"],
            "terminate_at": "2026-08-01T00:00:00Z",
            "max_hourly_rate_usd": "0.24",
        },
    )


def test_production_adapter_creates_encrypted_resource_with_durable_key(tmp_path: Path) -> None:
    api = FakeAPI()
    provider = RunPodProvider(api, secrets_root=tmp_path / "secrets")

    resource = provider.create(provider_request())

    assert resource.state == ProviderResourceState.RUNNING
    assert api.created is not None
    sent, key = api.created
    assert sent.environment["RUNPOD_JOBRUNNER_OPERATION_ID"] == provider_request().operation_id
    assert len(key) == 30 and key.isalnum()
    key_path = tmp_path / "secrets" / provider_request().operation_id / "volume-key"
    assert key_path.read_text().strip() == key
    assert key_path.stat().st_mode & 0o777 == 0o600


def test_production_adapter_reconciles_by_operation_environment(tmp_path: Path) -> None:
    api = FakeAPI()
    provider = RunPodProvider(api, secrets_root=tmp_path / "secrets")
    created = provider.create(provider_request())

    matches = provider.find_resources(provider_request().operation_id)

    assert matches == (created,)


def test_production_adapter_maps_unknown_create_outcome(tmp_path: Path) -> None:
    class UnknownAPI(FakeAPI):
        def create_encrypted_pod(
            self, request: PodCreateRequest, *, volume_key: str
        ) -> PodObservation:
            raise RunPodTransportOutcomeUnknown("POST")

    provider = RunPodProvider(UnknownAPI(), secrets_root=tmp_path / "secrets")

    with pytest.raises(ProviderOutcomeUnknown) as caught:
        provider.create(provider_request())

    assert caught.value.operation_id == provider_request().operation_id


def test_production_adapter_delete_receipt_distinguishes_absence(tmp_path: Path) -> None:
    api = FakeAPI()
    provider = RunPodProvider(api, secrets_root=tmp_path / "secrets")

    receipt = provider.delete("already-gone", "op-delete")

    assert receipt.acknowledged is True
    assert receipt.already_absent is True


def test_production_adapter_deletes_resource_above_admitted_hourly_rate(
    tmp_path: Path,
) -> None:
    api = FakeAPI()
    original_create = api.create_encrypted_pod

    def expensive_create(request: PodCreateRequest, *, volume_key: str) -> PodObservation:
        original = original_create(request, volume_key=volume_key)
        expensive = PodObservation(
            id=original.id,
            name=original.name,
            desired_status=original.desired_status,
            cost_per_hour=Decimal("0.25"),
            volume_encrypted=original.volume_encrypted,
            environment=original.environment,
        )
        api.pods[expensive.id] = expensive
        return expensive

    api.create_encrypted_pod = expensive_create  # type: ignore[method-assign]
    provider = RunPodProvider(api, secrets_root=tmp_path / "secrets")

    with pytest.raises(ProviderRejected, match="hourly rate"):
        provider.create(provider_request())

    assert api.pods == {}


def test_reconciliation_deletes_unencrypted_matching_resource(tmp_path: Path) -> None:
    api = FakeAPI()
    request = provider_request()
    provider = RunPodProvider(api, secrets_root=tmp_path / "secrets")
    provider.create(request)
    original = api.pods["pod-1"]
    api.pods["pod-1"] = PodObservation(
        id=original.id,
        name=original.name,
        desired_status=original.desired_status,
        cost_per_hour=original.cost_per_hour,
        volume_encrypted=False,
        environment=original.environment,
    )

    with pytest.raises(ProviderRejected, match="encrypted"):
        provider.find_resources(request.operation_id)

    assert api.pods == {}


def test_reconciliation_applies_durable_rate_cap_after_unknown_create(
    tmp_path: Path,
) -> None:
    class UnknownExpensiveAPI(FakeAPI):
        def create_encrypted_pod(
            self, request: PodCreateRequest, *, volume_key: str
        ) -> PodObservation:
            pod = super().create_encrypted_pod(request, volume_key=volume_key)
            expensive = PodObservation(
                id=pod.id,
                name=pod.name,
                desired_status=pod.desired_status,
                cost_per_hour=Decimal("0.25"),
                volume_encrypted=True,
                environment=pod.environment,
            )
            self.pods[pod.id] = expensive
            raise RunPodTransportOutcomeUnknown("POST")

    api = UnknownExpensiveAPI()
    request = provider_request()
    provider = RunPodProvider(api, secrets_root=tmp_path / "secrets")
    with pytest.raises(ProviderOutcomeUnknown):
        provider.create(request)

    with pytest.raises(ProviderRejected, match="hourly rate"):
        provider.find_resources(request.operation_id)

    assert api.pods == {}


def test_reconciliation_deletes_resource_with_unattested_image(tmp_path: Path) -> None:
    api = FakeAPI()
    request = provider_request()
    provider = RunPodProvider(api, secrets_root=tmp_path / "secrets")
    provider.create(request)
    original = api.pods["pod-1"]
    api.pods["pod-1"] = PodObservation(
        id=original.id,
        name=original.name,
        desired_status=original.desired_status,
        cost_per_hour=original.cost_per_hour,
        volume_encrypted=True,
        image="ghcr.io/attacker/wrong@sha256:" + "f" * 64,
        environment=original.environment,
    )

    with pytest.raises(ProviderRejected, match="image"):
        provider.find_resources(request.operation_id)

    assert api.pods == {}


def test_reconciliation_uses_stable_exact_name_when_provider_omits_environment(
    tmp_path: Path,
) -> None:
    api = FakeAPI()
    request = provider_request()
    provider = RunPodProvider(api, secrets_root=tmp_path / "secrets")
    created = provider.create(request)
    original = api.pods["pod-1"]
    api.pods["pod-1"] = PodObservation(
        id=original.id,
        name=original.name,
        desired_status=original.desired_status,
        cost_per_hour=original.cost_per_hour,
        volume_encrypted=True,
        image=original.image,
        environment=None,
    )

    assert provider.find_resources(request.operation_id) == (created,)
