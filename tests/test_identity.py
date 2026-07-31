from __future__ import annotations

import json
from pathlib import Path

import pytest

from runpod_jobrunner import __version__
from runpod_jobrunner.identity import (
    RunnerIdentityError,
    load_runner_identity,
)


def test_release_receipt_binds_version_commit_and_protocol_majors(tmp_path: Path) -> None:
    receipt = tmp_path / "release.json"
    receipt.write_text(
        json.dumps(
            {
                "protocol": "runner-release/1",
                "runner_version": __version__,
                "runner_git_commit": "a" * 40,
                "supported_protocol_majors": {
                    "artifact-manifest": [1],
                    "launch-authorization": [1],
                    "run-event": [1],
                    "run-request": [1],
                    "run-status": [1],
                },
            }
        )
    )

    identity = load_runner_identity(receipt)

    assert identity.version == __version__
    assert identity.git_commit == "a" * 40
    assert identity.supported_protocol_majors == {
        "artifact-manifest": (1,),
        "launch-authorization": (1,),
        "run-event": (1,),
        "run-request": (1,),
        "run-status": (1,),
    }


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("runner_version", "0.1", "semantic version"),
        ("runner_git_commit", "not-a-commit", "Git commit"),
        ("runner_git_commit", "0" * 40, "must not be all zero"),
        (
            "supported_protocol_majors",
            {
                "artifact-manifest": [1],
                "launch-authorization": [1],
                "run-event": [1],
                "run-request": [1],
            },
            "run-status",
        ),
    ],
)
def test_release_receipt_rejects_ambiguous_identity(
    tmp_path: Path, field: str, value: object, match: str
) -> None:
    receipt = tmp_path / "release.json"
    record: dict[str, object] = {
        "protocol": "runner-release/1",
        "runner_version": __version__,
        "runner_git_commit": "a" * 40,
        "supported_protocol_majors": {
            "artifact-manifest": [1],
            "launch-authorization": [1],
            "run-event": [1],
            "run-request": [1],
            "run-status": [1],
        },
    }
    record[field] = value
    receipt.write_text(json.dumps(record))

    with pytest.raises(RunnerIdentityError, match=match):
        load_runner_identity(receipt)


def test_release_receipt_rejects_duplicate_keys(tmp_path: Path) -> None:
    receipt = tmp_path / "release.json"
    receipt.write_text(
        '{"protocol":"runner-release/1","runner_version":"0.1.1",'
        '"runner_version":"0.1.1","runner_git_commit":"'
        + "a"
        * 40
        + '","supported_protocol_majors":{"artifact-manifest":[1],'
        '"launch-authorization":[1],"run-event":[1],'
        '"run-request":[1],"run-status":[1]}}'
    )

    with pytest.raises(RunnerIdentityError, match="duplicate key"):
        load_runner_identity(receipt)
