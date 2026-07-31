from __future__ import annotations

import hashlib
import json
import sys
import threading
import time
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from runpod_jobrunner.runner import PHASE_ORDER, RemoteRunner


def make_request(tmp_path: Path, **overrides: object) -> dict[str, object]:
    trace_path = tmp_path / "phase-trace.txt"
    phases = {
        phase: {
            "enabled": True,
            "argv": [
                sys.executable,
                "-c",
                (
                    "from pathlib import Path; "
                    f"p = Path({str(trace_path)!r}); "
                    f"p.write_text(p.read_text() + {phase + chr(10)!r} if p.exists() "
                    f"else {phase + chr(10)!r})"
                ),
            ],
            "timeout_seconds": 5,
        }
        for phase in reversed(PHASE_ORDER)
    }
    request: dict[str, object] = {
        "protocol": "run-request/1",
        "run_id": "run-test-001",
        "bundle_hash": "1" * 64,
        "image_digest": "example.invalid/runner@sha256:" + "2" * 64,
        "storage": {
            "encrypted": True,
            "mount": str(tmp_path),
            "required_gb": 1,
        },
        "phases": phases,
        "limits": {
            "max_elapsed_seconds": 30,
            "max_cost_usd": "1.00",
            "usd_per_hour": "1.00",
        },
        "heartbeat_interval_seconds": 0.02,
        "termination_grace_seconds": 0.2,
    }
    request.update(overrides)
    return request


def read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text())


def test_runner_executes_enabled_phases_in_fixed_order(tmp_path: Path) -> None:
    request = make_request(tmp_path)
    status_dir = tmp_path / "status"

    terminal = RemoteRunner(request, status_dir).run()

    assert terminal["outcome"] == "succeeded"
    assert (tmp_path / "phase-trace.txt").read_text().splitlines() == list(PHASE_ORDER)
    status = read_json(status_dir / "status.json")
    assert status["state"] == "terminal"
    assert status["terminal_result"] == terminal
    events = [json.loads(line) for line in (status_dir / "events.jsonl").read_text().splitlines()]
    assert [event["sequence"] for event in events] == list(range(1, len(events) + 1))


def test_success_with_declared_artifacts_publishes_verified_manifest_hash(
    tmp_path: Path,
) -> None:
    request = make_request(tmp_path)
    request["artifact_manifest_path"] = "artifacts/manifest.json"
    phases = cast(dict[str, object], request["phases"])
    payload = b"verified artifact\n"
    package_script = (
        "import hashlib,json,os,pathlib;"
        "root=pathlib.Path(os.environ['RUNPOD_JOBRUNNER_STORAGE_MOUNT']);"
        "p=root/'artifacts'/'result.txt';p.parent.mkdir(parents=True,exist_ok=True);"
        f"p.write_bytes({payload!r});"
        "m={'protocol':'artifact-manifest/1','run_id':os.environ['RUNPOD_JOBRUNNER_RUN_ID'],"
        "'files':[{'path':'artifacts/result.txt','size':p.stat().st_size,"
        "'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}]};"
        "(p.parent/'manifest.json').write_text(json.dumps(m,sort_keys=True,separators=(',',':'))+'\\n')"
    )
    phases["package"] = {
        "enabled": True,
        "argv": [sys.executable, "-c", package_script],
        "timeout_seconds": 5,
    }

    terminal = RemoteRunner(request, tmp_path / "status").run()

    manifest = tmp_path / "artifacts" / "manifest.json"
    assert terminal["outcome"] == "succeeded"
    assert terminal["artifact_manifest_sha256"] == hashlib.sha256(manifest.read_bytes()).hexdigest()
    assert terminal["artifact_manifest_size"] == manifest.stat().st_size


def test_success_fails_closed_when_declared_artifact_manifest_is_missing(
    tmp_path: Path,
) -> None:
    request = make_request(tmp_path)
    request["artifact_manifest_path"] = "artifacts/manifest.json"

    terminal = RemoteRunner(request, tmp_path / "status").run()

    assert terminal["outcome"] == "failed"
    assert terminal["reason"] == "artifact_manifest_missing"


def test_success_fails_closed_when_declared_artifact_hash_is_wrong(tmp_path: Path) -> None:
    request = make_request(tmp_path)
    request["artifact_manifest_path"] = "artifacts/manifest.json"
    phases = cast(dict[str, object], request["phases"])
    script = (
        "import json,os,pathlib;"
        "root=pathlib.Path(os.environ['RUNPOD_JOBRUNNER_STORAGE_MOUNT']);"
        "p=root/'artifacts'/'result.txt';p.parent.mkdir(parents=True,exist_ok=True);"
        "p.write_bytes(b'actual');"
        "m={'protocol':'artifact-manifest/1','files':[{'path':'artifacts/result.txt',"
        "'size':6,'sha256':'0'*64}]};"
        "(p.parent/'manifest.json').write_text(json.dumps(m)+'\\n')"
    )
    phases["package"] = {
        "enabled": True,
        "argv": [sys.executable, "-c", script],
        "timeout_seconds": 5,
    }

    terminal = RemoteRunner(request, tmp_path / "status").run()

    assert terminal["outcome"] == "failed"
    assert terminal["reason"] == "artifact_hash_mismatch"


def test_runner_never_interprets_phase_argv_as_a_shell_command(tmp_path: Path) -> None:
    request = make_request(tmp_path)
    injected_path = tmp_path / "must-not-exist"
    phases = request["phases"]
    assert isinstance(phases, dict)
    phases["verify"] = {
        "enabled": True,
        "argv": [f"touch {injected_path}"],
        "timeout_seconds": 5,
    }

    terminal = RemoteRunner(request, tmp_path / "status").run()

    assert terminal["outcome"] == "failed"
    assert terminal["reason"] == "phase_start_failed"
    assert not injected_path.exists()


def test_phase_timeout_kills_a_process_group_after_bounded_grace(tmp_path: Path) -> None:
    request = make_request(tmp_path)
    phases = request["phases"]
    assert isinstance(phases, dict)
    phases["verify"] = {
        "enabled": True,
        "argv": [
            sys.executable,
            "-c",
            "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(10)",
        ],
        "timeout_seconds": 0.15,
    }
    request["termination_grace_seconds"] = 0.1

    started = time.monotonic()
    terminal = RemoteRunner(request, tmp_path / "status").run()

    assert time.monotonic() - started < 1
    assert terminal["outcome"] == "timed_out"
    assert terminal["reason"] == "phase_timeout"
    assert terminal["phase_exit_codes"] == {"verify": -9}
    assert not (tmp_path / "phase-trace.txt").exists()


def test_runner_enforces_its_own_elapsed_and_cost_caps(tmp_path: Path) -> None:
    cases: tuple[tuple[dict[str, object], str], ...] = (
        ({"max_elapsed_seconds": 0.08}, "elapsed_cap"),
        ({"max_cost_usd": "0.00008", "usd_per_hour": "3.60"}, "cost_cap"),
    )
    for limit_overrides, expected_reason in cases:
        case_dir = tmp_path / expected_reason
        case_dir.mkdir()
        request = make_request(case_dir)
        phases = cast(dict[str, object], request["phases"])
        limits = cast(dict[str, object], request["limits"])
        phases["verify"] = {
            "enabled": True,
            "argv": [sys.executable, "-c", "import time; time.sleep(10)"],
            "timeout_seconds": 5,
        }
        limits.update(limit_overrides)

        started = time.monotonic()
        terminal = RemoteRunner(request, case_dir / "status").run()

        assert time.monotonic() - started < 1
        assert terminal["outcome"] == "limit_exceeded"
        assert terminal["reason"] == expected_reason
        assert Decimal(str(terminal["estimated_cost_usd"])) >= 0


def test_status_heartbeats_remain_parseable_while_a_child_runs(tmp_path: Path) -> None:
    request = make_request(tmp_path)
    phases = cast(dict[str, object], request["phases"])
    phases["verify"] = {
        "enabled": True,
        "argv": [sys.executable, "-c", "import time; time.sleep(0.25)"],
        "timeout_seconds": 5,
    }
    for phase in PHASE_ORDER[1:]:
        phase_config = cast(dict[str, object], phases[phase])
        phase_config["enabled"] = False
    status_dir = tmp_path / "status"
    runner = RemoteRunner(request, status_dir)
    terminals: list[dict[str, object]] = []
    thread = threading.Thread(target=lambda: terminals.append(runner.run()))
    thread.start()
    observed_heartbeats: list[float] = []

    while thread.is_alive():
        if (status_dir / "status.json").exists():
            status = read_json(status_dir / "status.json")
            observed_heartbeats.append(float(str(status["heartbeat_monotonic_seconds"])))
        time.sleep(0.005)
    thread.join()

    assert terminals[0]["outcome"] == "succeeded"
    assert len(set(observed_heartbeats)) >= 2


def test_existing_terminal_result_is_immutable_and_prevents_rerun(tmp_path: Path) -> None:
    request = make_request(tmp_path)
    status_dir = tmp_path / "status"
    first_terminal = RemoteRunner(request, status_dir).run()
    original_terminal_bytes = (status_dir / "terminal-result.json").read_bytes()
    original_event_bytes = (status_dir / "events.jsonl").read_bytes()
    phases = request["phases"]
    assert isinstance(phases, dict)
    phases["verify"] = {
        "enabled": True,
        "argv": [sys.executable, "-c", "raise SystemExit('must not rerun')"],
        "timeout_seconds": 5,
    }

    second_terminal = RemoteRunner(request, status_dir).run()

    assert second_terminal == first_terminal
    assert (status_dir / "terminal-result.json").read_bytes() == original_terminal_bytes
    assert (status_dir / "events.jsonl").read_bytes() == original_event_bytes


def test_phase_receives_only_run_scoped_path_context_as_explicit_environment(
    tmp_path: Path,
) -> None:
    request = make_request(tmp_path)
    phases = cast(dict[str, object], request["phases"])
    environment_path = tmp_path / "phase-environment.json"
    request_path = tmp_path / "request.json"
    request_path.write_text("{}")
    phases["verify"] = {
        "enabled": True,
        "argv": [
            sys.executable,
            "-c",
            (
                "import json,os,pathlib; "
                f"pathlib.Path({str(environment_path)!r}).write_text(json.dumps({{"
                "key: os.environ[key] for key in ("
                "'RUNPOD_JOBRUNNER_RUN_ID','RUNPOD_JOBRUNNER_STATUS_DIR',"
                "'RUNPOD_JOBRUNNER_STORAGE_MOUNT','RUNPOD_JOBRUNNER_RUN_ROOT',"
                "'RUNPOD_JOBRUNNER_INPUT_ROOT','RUNPOD_JOBRUNNER_REQUEST_PATH')}))"
            ),
        ],
        "timeout_seconds": 5,
    }
    for phase in PHASE_ORDER[1:]:
        cast(dict[str, object], phases[phase])["enabled"] = False
    status_dir = tmp_path / "status"

    terminal = RemoteRunner(
        request,
        status_dir,
        request_path=request_path,
    ).run()

    assert terminal["outcome"] == "succeeded"
    assert json.loads(environment_path.read_text()) == {
        "RUNPOD_JOBRUNNER_RUN_ID": "run-test-001",
        "RUNPOD_JOBRUNNER_STATUS_DIR": str(status_dir.resolve()),
        "RUNPOD_JOBRUNNER_STORAGE_MOUNT": str(tmp_path),
        "RUNPOD_JOBRUNNER_RUN_ROOT": str(tmp_path / "runpod-jobrunner" / "runs" / "run-test-001"),
        "RUNPOD_JOBRUNNER_INPUT_ROOT": str(
            tmp_path / "runpod-jobrunner" / "runs" / "run-test-001" / "input"
        ),
        "RUNPOD_JOBRUNNER_REQUEST_PATH": str(request_path),
    }


def test_phase_environment_does_not_inherit_controller_style_credentials(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("RUNPOD_API_KEY", "must-not-reach-phase")
    monkeypatch.setenv("WANDB_API_KEY", "must-not-reach-phase")
    request = make_request(tmp_path)
    phases = cast(dict[str, object], request["phases"])
    observed = tmp_path / "credential-observation.json"
    phases["verify"] = {
        "enabled": True,
        "argv": [
            sys.executable,
            "-c",
            (
                "import json,os,pathlib;"
                f"pathlib.Path({str(observed)!r}).write_text(json.dumps({{"
                "'runpod':os.environ.get('RUNPOD_API_KEY'),"
                "'wandb':os.environ.get('WANDB_API_KEY')}))"
            ),
        ],
        "timeout_seconds": 5,
    }
    for phase in PHASE_ORDER[1:]:
        cast(dict[str, object], phases[phase])["enabled"] = False

    terminal = RemoteRunner(request, tmp_path / "status").run()

    assert terminal["outcome"] == "succeeded"
    assert json.loads(observed.read_text()) == {"runpod": None, "wandb": None}


def test_restart_resumes_after_durably_completed_phase_without_rerunning_it(
    tmp_path: Path,
) -> None:
    request = make_request(tmp_path)
    phases = cast(dict[str, object], request["phases"])
    for phase in PHASE_ORDER[1:]:
        cast(dict[str, object], phases[phase])["enabled"] = False
    status_dir = tmp_path / "status"
    status_dir.mkdir()
    events = [
        {
            "protocol": "run-event/1",
            "run_id": "run-test-001",
            "sequence": 1,
            "kind": "ready",
            "phase": None,
            "monotonic_seconds": 0.1,
        },
        {
            "protocol": "run-event/1",
            "run_id": "run-test-001",
            "sequence": 2,
            "kind": "phase_started",
            "phase": "verify",
            "monotonic_seconds": 0.2,
        },
        {
            "protocol": "run-event/1",
            "run_id": "run-test-001",
            "sequence": 3,
            "kind": "phase_completed",
            "phase": "verify",
            "exit_code": 0,
            "monotonic_seconds": 0.3,
        },
    ]
    (status_dir / "events.jsonl").write_text("".join(json.dumps(event) + "\n" for event in events))

    terminal = RemoteRunner(request, status_dir).run()

    assert terminal["outcome"] == "succeeded"
    assert terminal["completed_phases"] == ["verify"]
    assert not (tmp_path / "phase-trace.txt").exists()


def test_restart_fails_closed_instead_of_repeating_a_phase_with_unknown_outcome(
    tmp_path: Path,
) -> None:
    request = make_request(tmp_path)
    status_dir = tmp_path / "status"
    status_dir.mkdir()
    events = [
        {
            "protocol": "run-event/1",
            "run_id": "run-test-001",
            "sequence": 1,
            "kind": "ready",
            "phase": None,
            "monotonic_seconds": 0.1,
        },
        {
            "protocol": "run-event/1",
            "run_id": "run-test-001",
            "sequence": 2,
            "kind": "phase_started",
            "phase": "verify",
            "monotonic_seconds": 0.2,
        },
    ]
    (status_dir / "events.jsonl").write_text("".join(json.dumps(event) + "\n" for event in events))

    terminal = RemoteRunner(request, status_dir).run()

    assert terminal["outcome"] == "failed"
    assert terminal["reason"] == "runner_restart_unknown_child"
    assert not (tmp_path / "phase-trace.txt").exists()
