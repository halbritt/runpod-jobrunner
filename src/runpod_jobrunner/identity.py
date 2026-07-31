"""Fail-closed identity and protocol capability receipt for the remote runner."""

from __future__ import annotations

import json
import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from runpod_jobrunner import __version__

_SEMANTIC_VERSION = re.compile(
    r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"
    r"(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?\Z"
)
_GIT_COMMIT = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_REQUIRED_PROTOCOLS = (
    "artifact-manifest",
    "launch-authorization",
    "run-event",
    "run-request",
    "run-status",
)
_LOCAL_SOURCE_COMMIT = "0" * 40
_PUBLISHED_ROOT = Path("/opt/runpod-jobrunner")
_PUBLISHED_RECEIPT = _PUBLISHED_ROOT / "release.json"


class RunnerIdentityError(ValueError):
    """The installed runner cannot prove its executable identity."""


@dataclass(frozen=True, slots=True)
class RunnerIdentity:
    """The executable identity exchanged across the controller/runner seam."""

    version: str
    git_commit: str
    supported_protocol_majors: Mapping[str, tuple[int, ...]]

    def as_protocol_fields(self) -> dict[str, object]:
        """Return a detached JSON-safe identity projection."""

        return {
            "runner_version": self.version,
            "runner_git_commit": self.git_commit,
            "supported_protocol_majors": {
                protocol: list(majors)
                for protocol, majors in self.supported_protocol_majors.items()
            },
        }


def local_source_identity() -> RunnerIdentity:
    """Return a deterministic identity for source-only local execution.

    Published containers always set ``RUNPOD_JOBRUNNER_RELEASE_PATH`` and therefore
    cannot use this fallback.
    """

    return RunnerIdentity(
        version=__version__,
        git_commit=_LOCAL_SOURCE_COMMIT,
        supported_protocol_majors={
            "artifact-manifest": (1,),
            "incremental-mirror-ack": (1,),
            "launch-authorization": (1,),
            "run-event": (1,),
            "run-request": (1,),
            "run-status": (1,),
        },
    )


def load_runner_identity(path: Path | str | None = None) -> RunnerIdentity:
    """Load and validate the immutable build receipt, if one is configured."""

    configured = path
    if configured is None:
        configured = os.environ.get("RUNPOD_JOBRUNNER_RELEASE_PATH")
    if configured is None:
        if _PUBLISHED_RECEIPT.exists() or Path(sys.prefix).is_relative_to(_PUBLISHED_ROOT):
            configured = _PUBLISHED_RECEIPT
        else:
            return local_source_identity()
    receipt_path = Path(configured)
    try:
        candidate: object = json.loads(
            receipt_path.read_text(encoding="utf-8"),
            object_pairs_hook=_object_without_duplicates,
        )
    except (OSError, json.JSONDecodeError) as error:
        raise RunnerIdentityError(f"runner release receipt is unavailable: {error}") from error
    if not isinstance(candidate, Mapping):
        raise RunnerIdentityError("runner release receipt must be an object")
    receipt = cast(Mapping[str, object], candidate)
    if receipt.get("protocol") != "runner-release/1":
        raise RunnerIdentityError("runner release receipt protocol is unsupported")
    version = validate_runner_version(receipt.get("runner_version"))
    if version != __version__:
        raise RunnerIdentityError("runner release receipt version differs from installed package")
    git_commit = validate_git_commit(receipt.get("runner_git_commit"))
    majors = _protocol_majors(receipt.get("supported_protocol_majors"))
    return RunnerIdentity(
        version=version,
        git_commit=git_commit,
        supported_protocol_majors=majors,
    )


def parse_protocol_majors(candidate: object) -> dict[str, tuple[int, ...]]:
    """Validate a peer's declared supported protocol major versions."""

    return _protocol_majors(candidate)


def supported_protocol_majors() -> dict[str, list[int]]:
    """Return the controller's current protocol-major offer."""

    return {
        "artifact-manifest": [1],
        "incremental-mirror-ack": [1],
        "launch-authorization": [1],
        "run-event": [1],
        "run-request": [1],
        "run-status": [1],
    }


def validate_runner_version(candidate: object) -> str:
    """Return an exact semantic version or reject it."""

    if not isinstance(candidate, str) or _SEMANTIC_VERSION.fullmatch(candidate) is None:
        raise RunnerIdentityError("runner identity needs an exact semantic version")
    return candidate


def validate_git_commit(candidate: object) -> str:
    """Return a normalized Git commit identifier or reject it."""

    if not isinstance(candidate, str) or _GIT_COMMIT.fullmatch(candidate) is None:
        raise RunnerIdentityError("runner identity needs an exact Git commit")
    if not candidate.strip("0"):
        raise RunnerIdentityError("runner identity Git commit must not be all zero")
    return candidate


def _protocol_majors(candidate: object) -> dict[str, tuple[int, ...]]:
    if not isinstance(candidate, Mapping):
        raise RunnerIdentityError("supported protocol majors must be an object")
    raw = cast(Mapping[object, object], candidate)
    parsed: dict[str, tuple[int, ...]] = {}
    for protocol, versions in raw.items():
        if not isinstance(protocol, str) or not protocol:
            raise RunnerIdentityError("supported protocol major names must be non-empty strings")
        if not isinstance(versions, list) or not versions:
            raise RunnerIdentityError(f"supported protocol majors for {protocol} must be an array")
        version_items = cast(list[object], versions)
        if any(
            isinstance(item, bool) or not isinstance(item, int) or item <= 0
            for item in version_items
        ):
            raise RunnerIdentityError(
                f"supported protocol majors for {protocol} must be positive integers"
            )
        normalized = tuple(cast(list[int], version_items))
        if len(set(normalized)) != len(normalized) or tuple(sorted(normalized)) != normalized:
            raise RunnerIdentityError(
                f"supported protocol majors for {protocol} must be sorted and unique"
            )
        parsed[protocol] = normalized
    for required in _REQUIRED_PROTOCOLS:
        if required not in parsed:
            raise RunnerIdentityError(f"supported protocol majors must include {required}")
    return parsed


def _object_without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    record: dict[str, object] = {}
    for key, value in pairs:
        if key in record:
            raise RunnerIdentityError(f"runner release receipt has duplicate key {key!r}")
        record[key] = value
    return record
