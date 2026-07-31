"""Versioned record validation and deterministic JSON hashing.

The project deliberately uses one canonical encoder for intent hashes, manifests,
and operation records.  Binary floating-point values are rejected: lifecycle money
values enter the system as :class:`~decimal.Decimal` and remain exact.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from decimal import Decimal
from importlib import resources
from typing import Any, NoReturn, cast

from jsonschema import Draft202012Validator


class ProtocolValidationError(ValueError):
    """A versioned protocol record does not satisfy its declared schema."""


class CanonicalJSONError(ValueError):
    """A value cannot be represented by the project's canonical JSON encoding."""


_SCHEMA_FILES = {
    "artifact-manifest/1": "artifact-manifest-1.json",
    "closeout-receipt/1": "closeout-receipt-1.json",
    "job-spec/1": "job-spec-1.json",
    "input-manifest/1": "input-manifest-1.json",
    "run-event/1": "run-event-1.json",
    "run-request/1": "run-request-1.json",
    "run-status/1": "run-status-1.json",
}


def _fail(message: str) -> NoReturn:
    raise CanonicalJSONError(message)


def _decimal_text(value: Decimal) -> str:
    if not value.is_finite():
        _fail("canonical JSON does not support non-finite Decimal values")
    if value.is_zero():
        return "0"
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def canonical_json(value: object) -> bytes:
    """Encode *value* as deterministic UTF-8 JSON.

    Object keys are sorted, insignificant whitespace is removed, and ``Decimal``
    values are emitted as normalized JSON numbers.  Python ``float`` is rejected so
    a monetary value cannot silently acquire binary rounding before it is hashed.
    """

    active_containers: set[int] = set()

    def encode(item: object) -> str:
        if item is None:
            return "null"
        if item is True:
            return "true"
        if item is False:
            return "false"
        if isinstance(item, int):
            return str(item)
        if isinstance(item, Decimal):
            return _decimal_text(item)
        if isinstance(item, float):
            _fail("canonical JSON rejects float values; use Decimal for exact numbers")
        if isinstance(item, str):
            try:
                return json.dumps(item, ensure_ascii=False, separators=(",", ":"))
            except UnicodeEncodeError as exc:
                raise CanonicalJSONError("canonical JSON requires valid Unicode strings") from exc
        if isinstance(item, Mapping):
            mapping = cast(Mapping[object, object], item)
            container_id = id(mapping)
            if container_id in active_containers:
                _fail("canonical JSON cannot encode a cyclic mapping")
            string_mapping: dict[str, object] = {}
            for key, member in mapping.items():
                if not isinstance(key, str):
                    _fail("canonical JSON mappings require string keys")
                string_mapping[key] = member
            active_containers.add(container_id)
            try:
                members = (
                    f"{encode(key)}:{encode(string_mapping[key])}" for key in sorted(string_mapping)
                )
                return "{" + ",".join(members) + "}"
            finally:
                active_containers.remove(container_id)
        if isinstance(item, Sequence) and not isinstance(item, (str, bytes, bytearray)):
            sequence = cast(Sequence[object], item)
            container_id = id(sequence)
            if container_id in active_containers:
                _fail("canonical JSON cannot encode a cyclic sequence")
            active_containers.add(container_id)
            try:
                return "[" + ",".join(encode(member) for member in sequence) + "]"
            finally:
                active_containers.remove(container_id)
        _fail(f"canonical JSON does not support {type(item).__name__}")

    try:
        return encode(value).encode("utf-8")
    except UnicodeEncodeError as exc:
        raise CanonicalJSONError("canonical JSON requires valid Unicode strings") from exc


def canonical_sha256(value: object) -> str:
    """Return the lowercase SHA-256 digest of :func:`canonical_json`."""

    return hashlib.sha256(canonical_json(value)).hexdigest()


def load_schema(protocol: str) -> Mapping[str, Any]:
    """Load a packaged JSON schema for a supported protocol identifier."""

    try:
        filename = _SCHEMA_FILES[protocol]
    except KeyError as exc:
        supported = ", ".join(sorted(_SCHEMA_FILES))
        raise ProtocolValidationError(
            f"unsupported protocol {protocol!r}; supported protocols: {supported}"
        ) from exc
    schema_resource = resources.files("runpod_jobrunner.schemas").joinpath(filename)
    with schema_resource.open("r", encoding="utf-8") as handle:
        loaded_schema: object = json.load(handle)
    if not isinstance(loaded_schema, Mapping):  # pragma: no cover - packaged developer error
        raise RuntimeError(f"packaged schema {filename} is not a JSON object")
    return cast(Mapping[str, Any], loaded_schema)


def validate_protocol(record: Mapping[str, Any], expected_protocol: str, *, subject: str) -> None:
    """Validate *record* and raise one stable, path-oriented error on failure."""

    actual_protocol = record.get("protocol")
    if actual_protocol != expected_protocol:
        raise ProtocolValidationError(
            f"{subject}.protocol must be {expected_protocol!r}, got {actual_protocol!r}"
        )
    validator = Draft202012Validator(load_schema(expected_protocol))
    errors = sorted(
        validator.iter_errors(cast(Any, record)),  # pyright: ignore[reportUnknownMemberType]
        key=lambda error: list(error.absolute_path),
    )
    if not errors:
        return
    error = errors[0]
    path = ".".join(str(part) for part in error.absolute_path)
    location = f"{subject}.{path}" if path else subject
    raise ProtocolValidationError(f"{location}: {error.message}")
