from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from pathlib import Path

from runpod_jobrunner.status_http import StatusHTTPServer


def write_status(status_dir: Path) -> None:
    status_dir.mkdir()
    status = {
        "protocol": "run-status/1",
        "run_id": "run-http-001",
        "runner_version": "0.1.1",
        "runner_git_commit": "a" * 40,
        "supported_protocol_majors": {
            "artifact-manifest": [1],
            "future-safe-protocol": [2],
            "launch-authorization": [1],
            "run-event": [1],
            "run-request": [1],
            "run-status": [1],
        },
        "state": "running",
        "phase": "train",
        "heartbeat_at_unix": time.time() - 0.05,
        "heartbeat_monotonic_seconds": 12.5,
        "child": {
            "pid": 4321,
            "running": True,
            "exit_code": None,
            "command": ["python", "/private/train.py", "--token", "secret-child"],
        },
        "latest_event_sequence": 7,
        "terminal_result": None,
        "log": "secret-log-text",
        "request_path": "/private/request.json",
        "provider_token": "secret-provider-token",
    }
    (status_dir / "status.json").write_text(json.dumps(status))


def request_json(url: str, token: str) -> dict[str, object]:
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(request, timeout=2) as response:
        assert response.status == 200
        return json.load(response)


def test_authenticated_status_is_read_only_and_redacts_internal_fields(tmp_path: Path) -> None:
    status_dir = tmp_path / "status"
    write_status(status_dir)
    server = StatusHTTPServer(status_dir, "correct-run-token", host="127.0.0.1", port=0)
    server.start()
    url = f"http://127.0.0.1:{server.port}/status"

    try:
        with urllib.request.urlopen(url, timeout=2):
            raise AssertionError("unauthenticated request unexpectedly succeeded")
    except urllib.error.HTTPError as error:
        assert error.code == 401

    try:
        exposed = request_json(url, "correct-run-token")
    finally:
        server.stop()

    assert exposed == {
        "protocol": "run-status/1",
        "run_id": "run-http-001",
        "runner_version": "0.1.1",
        "runner_git_commit": "a" * 40,
        "supported_protocol_majors": {
            "artifact-manifest": [1],
            "future-safe-protocol": [2],
            "launch-authorization": [1],
            "run-event": [1],
            "run-request": [1],
            "run-status": [1],
        },
        "state": "running",
        "phase": "train",
        "heartbeat_age_seconds": exposed["heartbeat_age_seconds"],
        "heartbeat_monotonic_seconds": 12.5,
        "child": {"pid": 4321, "running": True, "exit_code": None},
        "latest_event_sequence": 7,
        "terminal_result": None,
    }
    assert 0 <= float(str(exposed["heartbeat_age_seconds"])) < 2
    encoded = json.dumps(exposed)
    assert "secret" not in encoded
    assert "/private" not in encoded


def test_terminal_projection_redacts_artifact_paths_and_rejects_writes(tmp_path: Path) -> None:
    status_dir = tmp_path / "status"
    write_status(status_dir)
    status_path = status_dir / "status.json"
    status = json.loads(status_path.read_text())
    status.update(
        state="terminal",
        phase="package",
        child=None,
        terminal_result={
            "outcome": "succeeded",
            "reason": "all_enabled_phases_completed",
            "phase": None,
            "elapsed_seconds": 42.25,
            "estimated_cost_usd": "0.01",
            "completed_phases": ["verify", "package"],
            "phase_exit_codes": {"verify": 0, "package": 0},
            "artifact_manifest_sha256": "a" * 64,
            "artifact_manifest_size": 4321,
            "artifact_path": "/private/model/adapter",
            "secret": "must-not-leak",
        },
    )
    status_path.write_text(json.dumps(status))
    server = StatusHTTPServer(status_dir, "correct-run-token", host="127.0.0.1", port=0)
    server.start()
    url = f"http://127.0.0.1:{server.port}/status"

    try:
        exposed = request_json(url, "correct-run-token")
        write_request = urllib.request.Request(
            url,
            data=b"{}",
            method="POST",
            headers={"Authorization": "Bearer correct-run-token"},
        )
        try:
            urllib.request.urlopen(write_request, timeout=2)
            raise AssertionError("write request unexpectedly succeeded")
        except urllib.error.HTTPError as error:
            assert error.code == 405
    finally:
        server.stop()

    assert exposed["terminal_result"] == {
        "outcome": "succeeded",
        "reason": "all_enabled_phases_completed",
        "phase": None,
        "elapsed_seconds": 42.25,
        "estimated_cost_usd": "0.01",
        "completed_phases": ["verify", "package"],
        "phase_exit_codes": {"verify": 0, "package": 0},
        "artifact_manifest_sha256": "a" * 64,
        "artifact_manifest_size": 4321,
    }
    assert "/private" not in json.dumps(exposed)
    assert "must-not-leak" not in json.dumps(exposed)
