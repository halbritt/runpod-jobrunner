"""Narrow RunPod control-plane adapter.

Creation uses the official GraphQL API because it is the Pod API surface that
supports both a caller-supplied encrypted-volume key and provider-side
``terminateAfter``. Reads and deletion use the official v1 REST API.
"""

from __future__ import annotations

import json
import os
import re
import secrets
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, NoReturn, Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, build_opener

from runpod_jobrunner.provider import (
    DeleteReceipt,
    ProviderCreateRequest,
    ProviderOutcomeUnknown,
    ProviderRejected,
    ProviderResource,
    ProviderResourceState,
)
from runpod_jobrunner.provider import (
    ProviderProtocolError as InternalProviderProtocolError,
)

GRAPHQL_URL = "https://api.runpod.io/graphql"
REST_PODS_URL = "https://rest.runpod.io/v1/pods"
_VOLUME_KEY = re.compile(r"[A-Za-z0-9]{1,30}\Z")


class ProviderProtocolError(RuntimeError):
    """The provider returned an unsuccessful or malformed response."""


class SecurityInvariantError(RuntimeError):
    """A provider resource violated a required security property."""

    def __init__(self, message: str, *, resource_id: str | None = None) -> None:
        self.resource_id = resource_id
        super().__init__(message)


class RunPodTransportOutcomeUnknown(ProviderProtocolError):
    """A mutating HTTP request returned without proving whether its effect happened."""


class _Opener(Protocol):
    def open(self, request: Request, timeout: float) -> Any: ...


@dataclass(frozen=True)
class PodCreateRequest:
    name: str
    image: str
    gpu_type_id: str
    gpu_count: int
    container_disk_gb: int
    volume_gb: int
    volume_mount_path: str
    ports: tuple[str, ...]
    environment: Mapping[str, str]
    terminate_at: str
    cloud_type: str = "SECURE"


@dataclass(frozen=True)
class PodObservation:
    id: str
    name: str
    desired_status: str | None
    cost_per_hour: Decimal | None
    volume_encrypted: bool | None
    image: str | None = None
    public_ip: str | None = None
    port_mappings: Mapping[str, int] | None = None
    environment: Mapping[str, str] | None = None


class RunPodHTTP:
    """Authenticated RunPod API calls with secret-safe errors."""

    def __init__(
        self,
        api_key: str,
        *,
        opener: _Opener | None = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        if not api_key:
            raise ValueError("RunPod API key is required")
        self._api_key = api_key
        self._opener = opener or build_opener()
        self._timeout = timeout_seconds

    def create_encrypted_pod(self, request: PodCreateRequest, *, volume_key: str) -> PodObservation:
        if _VOLUME_KEY.fullmatch(volume_key) is None:
            raise ValueError("volume key must be 1-30 alphanumeric characters")
        if request.cloud_type != "SECURE":
            raise SecurityInvariantError("encrypted jobs require Secure Cloud")
        variables = {
            "input": {
                "name": request.name,
                "imageName": request.image,
                "gpuTypeId": request.gpu_type_id,
                "gpuCount": request.gpu_count,
                "cloudType": request.cloud_type,
                "containerDiskInGb": request.container_disk_gb,
                "volumeInGb": request.volume_gb,
                "volumeMountPath": request.volume_mount_path,
                "volumeKey": volume_key,
                "ports": ",".join(request.ports),
                "startSsh": True,
                "startJupyter": False,
                "terminateAfter": request.terminate_at,
                "env": [
                    {"key": key, "value": value}
                    for key, value in sorted(request.environment.items())
                ],
            }
        }
        query = """
          mutation CreatePod($input: PodFindAndDeployOnDemandInput!) {
            podFindAndDeployOnDemand(input: $input) {
              id name imageName desiredStatus costPerHr volumeEncrypted env
              publicIp portMappings volumeInGb volumeMountPath
              machine { dataCenterId secureCloud gpuTypeId currentPricePerGpu }
            }
          }
        """
        try:
            result = self._graphql(query, variables)
            raw = result.get("podFindAndDeployOnDemand")
            if not isinstance(raw, Mapping):
                raise ProviderProtocolError("RunPod create returned no pod")
            pod = _parse_pod(cast(Mapping[str, Any], raw))
        except RunPodTransportOutcomeUnknown:
            raise
        except ProviderProtocolError as error:
            raise RunPodTransportOutcomeUnknown(
                f"RunPod create outcome unknown: {error}"
            ) from error
        if pod.volume_encrypted is not True:
            cleanup_error: str | None = None
            try:
                self.delete_pod(pod.id)
            except ProviderProtocolError as error:
                cleanup_error = str(error)
            suffix = "" if cleanup_error is None else f"; cleanup also failed: {cleanup_error}"
            raise SecurityInvariantError(
                f"RunPod created an unencrypted pod; deletion requested{suffix}",
                resource_id=pod.id,
            )
        return pod

    def find_pods_by_exact_name(self, name: str) -> tuple[PodObservation, ...]:
        return tuple(pod for pod in self.list_pods() if pod.name == name)

    def list_pods(self) -> tuple[PodObservation, ...]:
        raw = self._json_request("GET", REST_PODS_URL)
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
            raise ProviderProtocolError("RunPod pod list was not an array")
        observations: list[PodObservation] = []
        for item in cast(Sequence[object], raw):
            if not isinstance(item, Mapping):
                raise ProviderProtocolError("RunPod pod list item was not an object")
            observations.append(_parse_pod(cast(Mapping[str, Any], item)))
        return tuple(observations)

    def get_pod(self, pod_id: str) -> PodObservation | None:
        raw = self._json_request("GET", f"{REST_PODS_URL}/{pod_id}", allow_not_found=True)
        if raw is None:
            return None
        if not isinstance(raw, Mapping):
            raise ProviderProtocolError("RunPod pod response was not an object")
        return _parse_pod(cast(Mapping[str, Any], raw))

    def start_pod(self, pod_id: str) -> PodObservation:
        raw = self._json_request("POST", f"{REST_PODS_URL}/{pod_id}/start", payload={})
        if not isinstance(raw, Mapping):
            raise ProviderProtocolError("RunPod start response was not an object")
        return _parse_pod(cast(Mapping[str, Any], raw))

    def delete_pod(self, pod_id: str) -> bool:
        response = self._json_request("DELETE", f"{REST_PODS_URL}/{pod_id}", allow_not_found=True)
        return response is not None

    def current_spend_per_hour(self) -> Decimal:
        data = self._graphql("query CurrentSpend { myself { currentSpendPerHr } }", {})
        myself = data.get("myself")
        if not isinstance(myself, Mapping) or "currentSpendPerHr" not in myself:
            raise ProviderProtocolError("RunPod spend response was malformed")
        myself_record = cast(Mapping[str, object], myself)
        return Decimal(str(myself_record["currentSpendPerHr"]))

    def _graphql(self, query: str, variables: Mapping[str, object]) -> Mapping[str, Any]:
        raw = self._json_request(
            "POST", GRAPHQL_URL, payload={"query": query, "variables": variables}
        )
        if not isinstance(raw, Mapping):
            raise ProviderProtocolError("RunPod GraphQL response was not an object")
        response = cast(Mapping[str, object], raw)
        errors = response.get("errors")
        if isinstance(errors, Sequence) and errors:
            first = cast(Sequence[object], errors)[0]
            if isinstance(first, Mapping):
                message = cast(Mapping[str, object], first).get("message", "unknown error")
            else:
                message = "unknown error"
            raise ProviderProtocolError(f"RunPod GraphQL error: {message}")
        data = response.get("data")
        if not isinstance(data, Mapping):
            raise ProviderProtocolError("RunPod GraphQL response had no data")
        return cast(Mapping[str, Any], data)

    def _json_request(
        self,
        method: str,
        url: str,
        *,
        payload: Mapping[str, object] | None = None,
        allow_not_found: bool = False,
    ) -> Any:
        body = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
        request = Request(url, data=body, method=method)
        request.add_header("Authorization", f"Bearer {self._api_key}")
        request.add_header("Accept", "application/json")
        if body is not None:
            request.add_header("Content-Type", "application/json")
        try:
            with self._opener.open(request, timeout=self._timeout) as response:
                raw = response.read()
        except HTTPError as error:
            if allow_not_found and error.code == 404:
                return None
            raise ProviderProtocolError(f"RunPod HTTP error {error.code}") from None
        except (URLError, TimeoutError, OSError) as error:
            raise RunPodTransportOutcomeUnknown(
                f"RunPod transport outcome unknown ({type(error).__name__})"
            ) from None
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ProviderProtocolError("RunPod returned invalid JSON") from None


def _parse_pod(raw: Mapping[str, Any]) -> PodObservation:
    pod_id = raw.get("id")
    name = raw.get("name")
    if not isinstance(pod_id, str) or not isinstance(name, str):
        raise ProviderProtocolError("RunPod pod record lacks string id/name")
    cost = raw.get("costPerHr")
    try:
        parsed_cost = None if cost is None else Decimal(str(cost))
    except (InvalidOperation, ValueError):
        raise ProviderProtocolError("RunPod pod record has an invalid hourly rate") from None
    if parsed_cost is not None and (not parsed_cost.is_finite() or parsed_cost < 0):
        raise ProviderProtocolError("RunPod pod record has an invalid hourly rate")
    mappings = raw.get("portMappings")
    environment = _parse_environment(raw.get("env"))
    return PodObservation(
        id=pod_id,
        name=name,
        desired_status=_optional_string(raw.get("desiredStatus")),
        cost_per_hour=parsed_cost,
        volume_encrypted=(
            raw.get("volumeEncrypted") if isinstance(raw.get("volumeEncrypted"), bool) else None
        ),
        image=_optional_string(raw.get("imageName", raw.get("image"))),
        public_ip=_optional_string(raw.get("publicIp")),
        port_mappings=(
            cast(Mapping[str, int], mappings) if isinstance(mappings, Mapping) else None
        ),
        environment=environment,
    )


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _parse_environment(value: object) -> Mapping[str, str] | None:
    if isinstance(value, Mapping):
        items = cast(Mapping[object, object], value).items()
        return {
            str(key): str(item)
            for key, item in items
            if isinstance(key, str) and isinstance(item, str)
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        parsed: dict[str, str] = {}
        for item in cast(Sequence[object], value):
            if not isinstance(item, Mapping):
                continue
            record = cast(Mapping[str, object], item)
            key = record.get("key")
            item_value = record.get("value")
            if isinstance(key, str) and isinstance(item_value, str):
                parsed[key] = item_value
        return parsed
    return None


class _RunPodAPI(Protocol):
    def create_encrypted_pod(
        self, request: PodCreateRequest, *, volume_key: str
    ) -> PodObservation: ...

    def list_pods(self) -> tuple[PodObservation, ...]: ...

    def get_pod(self, pod_id: str) -> PodObservation | None: ...

    def start_pod(self, pod_id: str) -> PodObservation: ...

    def delete_pod(self, pod_id: str) -> bool: ...

    def current_spend_per_hour(self) -> Decimal: ...


class RunPodProvider:
    """Translate the lifecycle provider seam to the narrow HTTP client."""

    def __init__(self, api: _RunPodAPI, *, secrets_root: Path) -> None:
        self._api = api
        self._secrets_root = secrets_root

    def find_resources(self, create_operation_id: str) -> tuple[ProviderResource, ...]:
        try:
            pods = self._api.list_pods()
        except ProviderProtocolError as error:
            raise InternalProviderProtocolError(str(error)) from error
        try:
            expected_name = self._read_operation_text(create_operation_id, "resource-name")
        except InternalProviderProtocolError:
            expected_name = None
        matches = tuple(
            pod
            for pod in pods
            if (
                pod.environment is not None
                and pod.environment.get("RUNPOD_JOBRUNNER_OPERATION_ID") == create_operation_id
            )
            or (expected_name is not None and pod.name == expected_name)
        )
        checked = tuple(self._checked_observation(pod, create_operation_id) for pod in matches)
        run_id = self._read_operation_text(create_operation_id, "run-id") if checked else ""
        resources: list[ProviderResource] = []
        for pod in checked:
            self._persist_operation_text(create_operation_id, "resource-id", pod.id)
            resources.append(
                self._resource(
                    pod,
                    create_operation_id=create_operation_id,
                    observed_run_id=run_id,
                )
            )
        return tuple(resources)

    def create(self, request: ProviderCreateRequest) -> ProviderResource:
        spec = request.spec
        environment = _string_mapping(spec.get("environment", {}), "environment")
        environment.update(
            {
                "RUNPOD_JOBRUNNER_RUN_ID": request.run_id,
                "RUNPOD_JOBRUNNER_OPERATION_ID": request.operation_id,
            }
        )
        pod_request = PodCreateRequest(
            name=request.resource_name,
            image=_required_string(spec, "image"),
            gpu_type_id=_required_string(spec, "gpu_type_id"),
            gpu_count=_positive_integer(spec, "gpu_count"),
            container_disk_gb=_positive_integer(spec, "container_disk_gb"),
            volume_gb=_positive_integer(spec, "volume_gb"),
            volume_mount_path=_required_string(spec, "volume_mount_path"),
            ports=_string_tuple(spec.get("ports"), "ports"),
            environment=environment,
            terminate_at=_required_string(spec, "terminate_at"),
        )
        maximum_rate = _required_decimal(spec, "max_hourly_rate_usd")
        self._persist_maximum_rate(request.operation_id, maximum_rate)
        self._persist_operation_text(request.operation_id, "resource-name", request.resource_name)
        self._persist_operation_text(request.operation_id, "run-id", request.run_id)
        self._persist_operation_text(request.operation_id, "expected-image", pod_request.image)
        volume_key = self._volume_key(request.operation_id)
        try:
            pod = self._api.create_encrypted_pod(pod_request, volume_key=volume_key)
        except RunPodTransportOutcomeUnknown as error:
            raise ProviderOutcomeUnknown("provision", request.operation_id) from error
        except SecurityInvariantError as error:
            raise ProviderRejected(str(error), resource_id=error.resource_id) from error
        except ProviderProtocolError as error:
            raise ProviderOutcomeUnknown("provision", request.operation_id) from error
        if pod.cost_per_hour is None:
            self._delete_rejected_pod(pod.id, "RunPod pod has no hourly rate")
        assert pod.cost_per_hour is not None
        if pod.cost_per_hour > maximum_rate:
            self._delete_rejected_pod(
                pod.id,
                f"RunPod hourly rate {pod.cost_per_hour} exceeds admitted {maximum_rate}",
            )
        pod = self._checked_observation(pod, request.operation_id)
        self._persist_operation_text(request.operation_id, "resource-id", pod.id)
        return self._resource(
            pod,
            create_operation_id=request.operation_id,
            observed_run_id=request.run_id,
        )

    def _delete_rejected_pod(self, pod_id: str, message: str) -> NoReturn:
        cleanup_suffix = ""
        try:
            self._api.delete_pod(pod_id)
        except ProviderProtocolError as error:
            cleanup_suffix = f"; cleanup outcome unknown: {error}"
        raise ProviderRejected(f"{message}; deletion requested{cleanup_suffix}", resource_id=pod_id)

    def get(self, resource_id: str) -> ProviderResource | None:
        try:
            pod = self._api.get_pod(resource_id)
        except ProviderProtocolError as error:
            raise InternalProviderProtocolError(str(error)) from error
        if pod is None:
            return None
        operation_id = self._operation_id_for_pod(pod)
        pod = self._checked_observation(pod, operation_id)
        return self._resource(
            pod,
            create_operation_id=operation_id,
            observed_run_id=self._read_operation_text(operation_id, "run-id"),
        )

    def start(self, resource_id: str, operation_id: str) -> ProviderResource:
        try:
            pod = self._api.start_pod(resource_id)
        except RunPodTransportOutcomeUnknown as error:
            raise ProviderOutcomeUnknown("start", operation_id) from error
        except ProviderProtocolError as error:
            raise InternalProviderProtocolError(str(error)) from error
        operation_id_from_pod = self._operation_id_for_pod(pod)
        pod = self._checked_observation(pod, operation_id_from_pod)
        return self._resource(
            pod,
            create_operation_id=operation_id_from_pod,
            observed_run_id=self._read_operation_text(operation_id_from_pod, "run-id"),
        )

    def delete(self, resource_id: str, operation_id: str) -> DeleteReceipt:
        try:
            existed = self._api.delete_pod(resource_id)
        except RunPodTransportOutcomeUnknown as error:
            raise ProviderOutcomeUnknown("delete", operation_id) from error
        except ProviderProtocolError as error:
            raise InternalProviderProtocolError(str(error)) from error
        return DeleteReceipt(
            operation_id=operation_id,
            acknowledged=True,
            already_absent=not existed,
        )

    def current_spend_usd_per_hour(self, resource_id: str | None) -> Decimal:
        del resource_id
        try:
            return self._api.current_spend_per_hour()
        except ProviderProtocolError as error:
            raise InternalProviderProtocolError(str(error)) from error

    def _volume_key(self, operation_id: str) -> str:
        directory = self._secrets_root / operation_id
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        path = directory / "volume-key"
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            key = path.read_text(encoding="ascii").strip()
            if _VOLUME_KEY.fullmatch(key) is None:
                raise InternalProviderProtocolError("stored volume key is invalid") from None
            return key
        key = secrets.token_hex(15)
        try:
            os.write(descriptor, f"{key}\n".encode())
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        directory_descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
        return key

    def _persist_maximum_rate(self, operation_id: str, maximum_rate: Decimal) -> None:
        directory = self._secrets_root / operation_id
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        path = directory / "max-hourly-rate-usd"
        encoded = f"{format(maximum_rate, 'f')}\n".encode("ascii")
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            existing = self._read_maximum_rate(operation_id)
            if existing != maximum_rate:
                raise InternalProviderProtocolError(
                    "durable maximum hourly rate changed for one operation"
                ) from None
            return
        try:
            os.write(descriptor, encoded)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        directory_descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)

    def _persist_operation_text(self, operation_id: str, name: str, value: str) -> None:
        if not value or "\n" in value or "\r" in value:
            raise InternalProviderProtocolError(f"operation policy {name} is invalid")
        directory = self._secrets_root / operation_id
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        path = directory / name
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            if self._read_operation_text(operation_id, name) != value:
                raise InternalProviderProtocolError(
                    f"durable operation policy {name} changed"
                ) from None
            return
        try:
            os.write(descriptor, f"{value}\n".encode())
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        directory_descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)

    def _read_operation_text(self, operation_id: str, name: str) -> str:
        path = self._secrets_root / operation_id / name
        try:
            value = path.read_text(encoding="utf-8").strip()
        except OSError:
            raise InternalProviderProtocolError(
                f"durable operation policy {name} is unavailable"
            ) from None
        if not value:
            raise InternalProviderProtocolError(f"durable operation policy {name} is empty")
        return value

    def _read_maximum_rate(self, operation_id: str) -> Decimal:
        path = self._secrets_root / operation_id / "max-hourly-rate-usd"
        try:
            value = path.read_text(encoding="ascii").strip()
        except OSError:
            raise InternalProviderProtocolError(
                "durable maximum hourly rate is unavailable during reconciliation"
            ) from None
        return _required_decimal({"maximum": value}, "maximum")

    def _checked_observation(self, pod: PodObservation, operation_id: str) -> PodObservation:
        if pod.volume_encrypted is not True:
            self._delete_rejected_pod(
                pod.id, "RunPod reconciliation could not prove encrypted storage"
            )
        maximum_rate = self._read_maximum_rate(operation_id)
        if pod.cost_per_hour is None:
            self._delete_rejected_pod(pod.id, "RunPod pod has no hourly rate")
        assert pod.cost_per_hour is not None
        if not pod.cost_per_hour.is_finite() or pod.cost_per_hour <= 0:
            self._delete_rejected_pod(pod.id, "RunPod pod hourly rate is invalid")
        if pod.cost_per_hour > maximum_rate:
            self._delete_rejected_pod(
                pod.id,
                f"RunPod hourly rate {pod.cost_per_hour} exceeds admitted {maximum_rate}",
            )
        expected_image = self._read_operation_text(operation_id, "expected-image")
        if pod.image != expected_image:
            self._delete_rejected_pod(
                pod.id, "RunPod image identity does not match the admitted digest"
            )
        return pod

    def _operation_id_for_pod(self, pod: PodObservation) -> str:
        environment = pod.environment or {}
        operation_id = environment.get("RUNPOD_JOBRUNNER_OPERATION_ID")
        if operation_id is not None:
            return operation_id
        if not self._secrets_root.is_dir():
            raise InternalProviderProtocolError("RunPod pod lacks create operation identity")
        matches: list[str] = []
        for directory in self._secrets_root.iterdir():
            if not directory.is_dir():
                continue
            try:
                resource_id = self._read_operation_text(directory.name, "resource-id")
            except InternalProviderProtocolError:
                continue
            if resource_id == pod.id:
                matches.append(directory.name)
        if len(matches) != 1:
            raise InternalProviderProtocolError("RunPod pod lacks unique operation identity")
        return matches[0]

    @staticmethod
    def _resource(
        pod: PodObservation,
        *,
        create_operation_id: str,
        observed_run_id: str | None = None,
    ) -> ProviderResource:
        environment = pod.environment or {}
        run_id = observed_run_id or environment.get("RUNPOD_JOBRUNNER_RUN_ID")
        if run_id is None:
            raise InternalProviderProtocolError("RunPod pod lacks run identity")
        if pod.cost_per_hour is None:
            raise InternalProviderProtocolError("RunPod pod has no hourly rate")
        return ProviderResource(
            id=pod.id,
            run_id=run_id,
            create_operation_id=create_operation_id,
            name=pod.name,
            state=_provider_state(pod.desired_status),
            hourly_rate_usd=pod.cost_per_hour,
        )


def _provider_state(desired_status: str | None) -> ProviderResourceState:
    if desired_status == "RUNNING":
        return ProviderResourceState.RUNNING
    if desired_status in {"RESTARTING"}:
        return ProviderResourceState.STARTING
    if desired_status in {"EXITED", "PAUSED", "DEAD", "TERMINATED"}:
        return ProviderResourceState.STOPPED
    return ProviderResourceState.PROVISIONED


def _required_string(spec: Mapping[str, object], key: str) -> str:
    value = spec.get(key)
    if not isinstance(value, str) or not value:
        raise InternalProviderProtocolError(f"provider spec {key} must be a string")
    return value


def _required_decimal(spec: Mapping[str, object], key: str) -> Decimal:
    value = spec.get(key)
    if not isinstance(value, str):
        raise InternalProviderProtocolError(f"provider spec {key} must be a decimal string")
    try:
        result = Decimal(value)
    except Exception:
        raise InternalProviderProtocolError(
            f"provider spec {key} must be a decimal string"
        ) from None
    if not result.is_finite() or result <= 0:
        raise InternalProviderProtocolError(f"provider spec {key} must be positive")
    return result


def _positive_integer(spec: Mapping[str, object], key: str) -> int:
    value = spec.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InternalProviderProtocolError(f"provider spec {key} must be a positive integer")
    return value


def _string_tuple(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise InternalProviderProtocolError(f"provider spec {name} must be a string array")
    result = tuple(cast(Sequence[object], value))
    if not all(isinstance(item, str) and item for item in result):
        raise InternalProviderProtocolError(f"provider spec {name} must be a string array")
    return cast(tuple[str, ...], result)


def _string_mapping(value: object, name: str) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise InternalProviderProtocolError(f"provider spec {name} must be a string map")
    items = cast(Mapping[object, object], value).items()
    result: dict[str, str] = {}
    for key, item in items:
        if not isinstance(key, str) or not isinstance(item, str):
            raise InternalProviderProtocolError(f"provider spec {name} must be a string map")
        result[key] = item
    return result
