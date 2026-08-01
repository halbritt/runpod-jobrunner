"""Load and verify immutable local job bundles.

Only manifest-declared files below the manifest root become runtime inputs.  Bundle
checking opens each input without following symlinks and verifies the exact size and
SHA-256 declared by ``input-manifest/1``.
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn, cast

import yaml
from yaml.constructor import ConstructorError
from yaml.nodes import MappingNode, ScalarNode

from runpod_jobrunner.identity import supported_protocol_majors
from runpod_jobrunner.protocol import (
    CanonicalJSONError,
    ProtocolValidationError,
    canonical_json,
    canonical_sha256,
    validate_protocol,
)

FIXED_PHASES = ("verify", "preflight", "train", "evaluate", "package")
_RUN_ID = re.compile(r"^run-[A-Za-z0-9][A-Za-z0-9_-]{0,126}$")


class BundleValidationError(ValueError):
    """A bundle is unsafe, incomplete, or inconsistent with its declarations."""


class _DecimalSafeLoader(yaml.SafeLoader):
    """Safe YAML loader with duplicate-key rejection and exact decimal parsing."""


def _construct_mapping(
    loader: _DecimalSafeLoader, node: MappingNode, deep: bool = False
) -> dict[object, object]:
    loader.flatten_mapping(node)
    result: dict[object, object] = {}
    for key_node, value_node in node.value:
        key = cast(
            object,
            loader.construct_object(  # pyright: ignore[reportUnknownMemberType]
                key_node, deep=deep
            ),
        )
        try:
            duplicate = key in result
        except TypeError as exc:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                "found an unhashable mapping key",
                key_node.start_mark,
            ) from exc
        if duplicate:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"found duplicate key {key!r}",
                key_node.start_mark,
            )
        result[key] = cast(
            object,
            loader.construct_object(  # pyright: ignore[reportUnknownMemberType]
                value_node, deep=deep
            ),
        )
    return result


def _construct_decimal(loader: _DecimalSafeLoader, node: ScalarNode) -> Decimal:
    scalar = loader.construct_scalar(node).replace("_", "")
    try:
        value = Decimal(scalar)
    except InvalidOperation as exc:
        raise ConstructorError(
            "while constructing a decimal",
            node.start_mark,
            f"invalid finite decimal {scalar!r}",
            node.start_mark,
        ) from exc
    if not value.is_finite():
        raise ConstructorError(
            "while constructing a decimal",
            node.start_mark,
            "non-finite decimal values are not allowed",
            node.start_mark,
        )
    return value


_DecimalSafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping
)
_DecimalSafeLoader.add_constructor("tag:yaml.org,2002:float", _construct_decimal)


@dataclass(frozen=True, slots=True)
class PhaseSpec:
    """One member of the fixed v1 phase sequence."""

    name: str
    enabled: bool
    argv: tuple[str, ...]
    timeout_seconds: int


@dataclass(frozen=True, slots=True)
class InputFile:
    """One verified member of the runtime transfer allow-list."""

    relative_path: str
    source_path: Path
    sha256: str
    size: int


@dataclass(frozen=True, slots=True)
class StorageSpec:
    """Persistent storage required before any phase can execute."""

    encrypted: bool
    mount: str
    required_gb: int
    network_volume_id: str | None = None


@dataclass(frozen=True, slots=True)
class JobBundle:
    """A checked, normalized job bundle safe to hand to the controller.

    Mutable source mappings are retained only as canonical bytes.  Accessors return
    fresh decoded records, while phases and input entries are immutable tuples.
    """

    root: Path
    protocol: str
    name: str
    bundle_hash: str
    image_digest: str
    runner_version: str
    runner_git_commit: str
    phases: tuple[PhaseSpec, ...]
    storage: StorageSpec
    input_root: Path
    inputs: tuple[InputFile, ...]
    max_elapsed_seconds: int
    max_cost_usd: Decimal
    usd_per_hour: Decimal
    heartbeat_interval_seconds: int
    termination_grace_seconds: int
    job_spec_hash: str
    input_manifest_hash: str
    _job_spec_json: bytes = field(repr=False)
    _input_manifest_json: bytes = field(repr=False)

    @property
    def job_spec(self) -> dict[str, Any]:
        """Return a detached copy of the validated job specification."""

        value: object = json.loads(self._job_spec_json, parse_float=Decimal)
        assert isinstance(value, dict)  # guaranteed by schema validation
        return cast(dict[str, Any], value)

    @property
    def input_manifest(self) -> dict[str, Any]:
        """Return a detached copy of the validated input manifest."""

        value: object = json.loads(self._input_manifest_json, parse_float=Decimal)
        assert isinstance(value, dict)  # guaranteed by schema validation
        return cast(dict[str, Any], value)

    def to_run_request(self, run_id: str) -> dict[str, Any]:
        """Build the normalized controller-to-runner request core.

        The controller may add version/build negotiation fields, but it must not
        reinterpret these bundle-derived lifecycle and spend limits.
        """

        if not _RUN_ID.fullmatch(run_id):
            raise ValueError("run_id must start with 'run-' and contain only stable ID characters")
        request: dict[str, Any] = {
            "protocol": "run-request/1",
            "run_id": run_id,
            "bundle_hash": self.bundle_hash,
            "image_digest": self.image_digest,
            "runner_version": self.runner_version,
            "runner_git_commit": self.runner_git_commit,
            "supported_protocol_majors": supported_protocol_majors(),
            "phases": {
                phase.name: {
                    "enabled": phase.enabled,
                    "argv": list(phase.argv),
                    "timeout_seconds": phase.timeout_seconds,
                }
                for phase in self.phases
            },
            "limits": {
                "max_elapsed_seconds": self.max_elapsed_seconds,
                "max_cost_usd": format(self.max_cost_usd, "f"),
                "usd_per_hour": format(self.usd_per_hour, "f"),
            },
            "heartbeat_interval_seconds": self.heartbeat_interval_seconds,
            "termination_grace_seconds": self.termination_grace_seconds,
            "artifact_path_base": "run-root",
            "storage": {
                "encrypted": self.storage.encrypted,
                "mount": self.storage.mount,
                "required_gb": self.storage.required_gb,
                **(
                    {"network_volume_id": self.storage.network_volume_id}
                    if self.storage.network_volume_id is not None
                    else {}
                ),
            },
        }
        artifacts_value: object = self.job_spec.get("artifacts")
        if isinstance(artifacts_value, Mapping):
            artifacts = cast(Mapping[str, object], artifacts_value)
            if artifacts.get("manifest_path") is not None:
                request["artifact_manifest_path"] = artifacts["manifest_path"]
        return request


def _bundle_error(message: str) -> NoReturn:
    raise BundleValidationError(message)


def _json_object_no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            _bundle_error(f"input-manifest.json contains duplicate key {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> NoReturn:
    _bundle_error(f"input-manifest.json contains non-finite number {value}")


def _read_regular_file(path: Path, *, label: str) -> str:
    if path.is_symlink():
        _bundle_error(f"{label} must not be a symlink")
    try:
        metadata = path.stat()
    except FileNotFoundError:
        _bundle_error(f"missing required {label}")
    if not stat.S_ISREG(metadata.st_mode):
        _bundle_error(f"{label} must be a regular file")
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise BundleValidationError(f"{label} must be valid UTF-8") from exc


def _load_job_spec(path: Path) -> dict[str, Any]:
    text = _read_regular_file(path, label="job.yaml")
    try:
        value: object = yaml.load(text, Loader=_DecimalSafeLoader)
    except yaml.YAMLError as exc:
        raise BundleValidationError(f"job.yaml is not valid safe YAML: {exc}") from exc
    if not isinstance(value, dict):
        _bundle_error("job.yaml must contain one mapping")
    return cast(dict[str, Any], value)


def _load_input_manifest(path: Path) -> dict[str, Any]:
    text = _read_regular_file(path, label="input-manifest.json")
    try:
        value: object = json.loads(
            text,
            parse_float=Decimal,
            parse_constant=_reject_json_constant,
            object_pairs_hook=_json_object_no_duplicates,
        )
    except json.JSONDecodeError as exc:
        raise BundleValidationError(f"input-manifest.json is not valid JSON: {exc.msg}") from exc
    if not isinstance(value, dict):
        _bundle_error("input-manifest.json must contain one object")
    return cast(dict[str, Any], value)


def compute_bundle_hash(job_spec: Mapping[str, Any], input_manifest: Mapping[str, Any]) -> str:
    """Compute the bundle hash, excluding its self-declaration from ``job_spec``."""

    spec_without_self_hash = dict(job_spec)
    spec_without_self_hash.pop("bundle_hash", None)
    return canonical_sha256({"input_manifest": input_manifest, "job_spec": spec_without_self_hash})


def _safe_relative_path(raw_path: object, *, field_name: str) -> PurePosixPath:
    if not isinstance(raw_path, str):  # schema normally catches this; keeps helper total
        _bundle_error(f"{field_name} must be a string path")
    if "\\" in raw_path or "\x00" in raw_path:
        _bundle_error(f"{field_name} contains an unsafe path separator or NUL")
    path = PurePosixPath(raw_path)
    if (
        path.is_absolute()
        or not path.parts
        or path.as_posix() != raw_path
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        _bundle_error(f"{field_name} must be a normalized relative path without '.' or '..'")
    return path


def _safe_incremental_manifest_glob(raw_value: object) -> str:
    field_name = "artifacts.incremental_manifest_glob"
    if not isinstance(raw_value, str) or not raw_value or len(raw_value) > 512:
        _bundle_error(f"{field_name} must be a non-empty string of at most 512 characters")
    if "**" in raw_value or any(character in raw_value for character in "?[]{}\\\x00"):
        _bundle_error(f"{field_name} contains unsupported glob metacharacters")
    if any(ord(character) < 32 or ord(character) == 127 for character in raw_value):
        _bundle_error(f"{field_name} contains control characters")
    path = PurePosixPath(raw_value)
    if (
        path.is_absolute()
        or path.as_posix() != raw_value
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
        or "*" in path.name
        or len(path.parts) > 32
        or sum(part.count("*") for part in path.parts) > 4
    ):
        _bundle_error(
            f"{field_name} must be a normalized relative fixed glob with a fixed filename"
        )
    return raw_value


def _reject_symlink_components(root: Path, relative_path: PurePosixPath, *, label: str) -> Path:
    current = root
    for part in relative_path.parts:
        current = current / part
        if current.is_symlink():
            _bundle_error(f"{label} traverses symlink {current}")
    try:
        resolved = current.resolve(strict=True)
    except FileNotFoundError:
        _bundle_error(f"missing {label}: {relative_path.as_posix()}")
    try:
        resolved.relative_to(root)
    except ValueError:
        _bundle_error(f"{label} escapes bundle root: {relative_path.as_posix()}")
    return resolved


def _hash_open_regular_file(path: Path, *, label: str) -> tuple[int, str]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            _bundle_error(f"{label} must not be a symlink")
        if exc.errno == errno.ENOENT:
            _bundle_error(f"missing {label}")
        raise BundleValidationError(f"cannot open {label}: {exc.strerror}") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            _bundle_error(f"{label} must be a regular file")
        digest = hashlib.sha256()
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
        after = os.fstat(descriptor)
        stable_fields = (
            "st_dev",
            "st_ino",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        if any(getattr(before, name) != getattr(after, name) for name in stable_fields):
            _bundle_error(f"{label} mutated while it was being verified")
        return before.st_size, digest.hexdigest()
    finally:
        os.close(descriptor)


def _inventory_input_root(input_root: Path) -> set[str]:
    inventory: set[str] = set()
    for directory, child_directories, filenames in os.walk(input_root, followlinks=False):
        directory_path = Path(directory)
        for child in child_directories:
            child_path = directory_path / child
            if child_path.is_symlink():
                _bundle_error(f"input root contains symlink {child_path.relative_to(input_root)}")
        for filename in filenames:
            file_path = directory_path / filename
            relative = file_path.relative_to(input_root).as_posix()
            if file_path.is_symlink():
                _bundle_error(f"input root contains symlink {relative}")
            try:
                mode = file_path.stat().st_mode
            except FileNotFoundError:
                _bundle_error(f"input file disappeared during inventory: {relative}")
            if not stat.S_ISREG(mode):
                _bundle_error(f"input root contains non-regular file {relative}")
            inventory.add(relative)
    return inventory


def _verify_inputs(root: Path, manifest: Mapping[str, Any]) -> tuple[Path, tuple[InputFile, ...]]:
    input_root_relative = _safe_relative_path(manifest["root"], field_name="manifest.root")
    input_root = _reject_symlink_components(root, input_root_relative, label="manifest input root")
    if not input_root.is_dir():
        _bundle_error("manifest input root must be a directory")

    declared: set[str] = set()
    verified: list[InputFile] = []
    for index, entry in enumerate(manifest["files"]):
        relative = _safe_relative_path(entry["path"], field_name=f"manifest.files[{index}].path")
        relative_text = relative.as_posix()
        if relative_text in declared:
            _bundle_error(f"manifest declares input path more than once: {relative_text}")
        declared.add(relative_text)
        source = _reject_symlink_components(
            input_root, relative, label=f"declared input {relative_text}"
        )
        actual_size, actual_sha256 = _hash_open_regular_file(
            source, label=f"declared input {relative_text}"
        )
        expected_size = entry["size"]
        expected_sha256 = entry["sha256"]
        if actual_size != expected_size:
            _bundle_error(
                f"declared input {relative_text} size is {actual_size}, expected {expected_size}"
            )
        if actual_sha256 != expected_sha256:
            _bundle_error(
                f"declared input {relative_text} sha256 is {actual_sha256}, "
                f"expected {expected_sha256}"
            )
        verified.append(
            InputFile(
                relative_path=relative_text,
                source_path=source,
                sha256=actual_sha256,
                size=actual_size,
            )
        )

    inventory = _inventory_input_root(input_root)
    undeclared = sorted(inventory - declared)
    if undeclared:
        _bundle_error(f"input root contains undeclared files: {', '.join(undeclared)}")
    missing_after_inventory = sorted(declared - inventory)
    if missing_after_inventory:
        _bundle_error(
            "declared inputs disappeared during verification: " + ", ".join(missing_after_inventory)
        )
    return input_root, tuple(verified)


def _money(value: object, *, field_name: str) -> Decimal:
    if not isinstance(value, str):
        _bundle_error(f"{field_name} must be an exact decimal string")
    try:
        money = Decimal(value)
    except InvalidOperation as exc:  # schema normally catches this
        raise BundleValidationError(f"{field_name} is not a decimal amount") from exc
    if not money.is_finite() or money <= 0:
        _bundle_error(f"{field_name} must be a positive finite decimal amount")
    return money


def check_bundle(bundle_root: str | os.PathLike[str]) -> JobBundle:
    """Validate a job bundle and return its immutable normalized representation."""

    lexical_root = Path(bundle_root)
    if lexical_root.is_symlink():
        _bundle_error("job bundle root must not be a symlink")
    try:
        root = lexical_root.resolve(strict=True)
    except FileNotFoundError:
        _bundle_error(f"job bundle does not exist: {lexical_root}")
    if not root.is_dir():
        _bundle_error(f"job bundle is not a directory: {lexical_root}")

    spec = _load_job_spec(root / "job.yaml")
    manifest = _load_input_manifest(root / "input-manifest.json")
    try:
        validate_protocol(spec, "job-spec/1", subject="job.yaml")
        validate_protocol(manifest, "input-manifest/1", subject="input-manifest.json")
    except ProtocolValidationError as exc:
        raise BundleValidationError(str(exc)) from exc

    artifacts_value = spec.get("artifacts")
    if isinstance(artifacts_value, Mapping):
        artifacts_mapping = cast(Mapping[str, object], artifacts_value)
        incremental_glob = artifacts_mapping.get("incremental_manifest_glob")
        if incremental_glob is not None:
            _safe_incremental_manifest_glob(incremental_glob)
        incremental_ack = artifacts_mapping.get("incremental_mirror_ack")
        if incremental_ack is not None:
            if incremental_glob is None or not isinstance(incremental_ack, Mapping):
                _bundle_error(
                    "artifacts.incremental_mirror_ack requires an incremental manifest glob"
                )
            ack_mapping = cast(Mapping[str, object], incremental_ack)
            directory = _safe_relative_path(
                ack_mapping.get("directory"),
                field_name="artifacts.incremental_mirror_ack.directory",
            )
            if any(
                ord(character) < 32 or ord(character) == 127
                for character in directory.as_posix()
            ):
                _bundle_error(
                    "artifacts.incremental_mirror_ack.directory contains control characters"
                )

    try:
        actual_bundle_hash = compute_bundle_hash(spec, manifest)
        job_spec_json = canonical_json(spec)
        input_manifest_json = canonical_json(manifest)
    except CanonicalJSONError as exc:
        raise BundleValidationError(f"bundle is not canonically encodable: {exc}") from exc
    if spec["bundle_hash"] != actual_bundle_hash:
        _bundle_error(
            f"job.yaml.bundle_hash is {spec['bundle_hash']}, expected {actual_bundle_hash}"
        )

    input_root, inputs = _verify_inputs(root, manifest)
    phases = tuple(
        PhaseSpec(
            name=name,
            enabled=spec["phases"][name]["enabled"],
            argv=tuple(spec["phases"][name]["argv"]),
            timeout_seconds=spec["phases"][name]["timeout_seconds"],
        )
        for name in FIXED_PHASES
    )
    limits = spec["limits"]
    storage = spec["resources"]["storage"]
    return JobBundle(
        root=root,
        protocol=spec["protocol"],
        name=spec["name"],
        bundle_hash=actual_bundle_hash,
        image_digest=spec["image"],
        runner_version=spec["runner"]["version"],
        runner_git_commit=spec["runner"]["git_commit"],
        phases=phases,
        storage=StorageSpec(
            encrypted=storage["encrypted"],
            mount=storage["mount"],
            required_gb=storage["required_gb"],
            network_volume_id=storage.get("network_volume_id"),
        ),
        input_root=input_root,
        inputs=inputs,
        max_elapsed_seconds=limits["max_elapsed_seconds"],
        max_cost_usd=_money(limits["max_cost_usd"], field_name="limits.max_cost_usd"),
        usd_per_hour=_money(limits["usd_per_hour"], field_name="limits.usd_per_hour"),
        heartbeat_interval_seconds=spec["heartbeat_interval_seconds"],
        termination_grace_seconds=spec["termination_grace_seconds"],
        job_spec_hash=canonical_sha256(spec),
        input_manifest_hash=canonical_sha256(manifest),
        _job_spec_json=job_spec_json,
        _input_manifest_json=input_manifest_json,
    )
