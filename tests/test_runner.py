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

from runpod_jobrunner.identity import RunnerIdentity
from runpod_jobrunner.launch_authorization import LAUNCH_PROTOCOL, LAUNCH_RELATIVE_PATH
from runpod_jobrunner.runner import PHASE_ORDER, RemoteRunner, RequestError

TEST_IDENTITY = RunnerIdentity(
    version="0.1.1",
    git_commit="a" * 40,
    supported_protocol_majors={
        "artifact-manifest": (1,),
        "launch-authorization": (1,),
        "run-event": (1,),
        "run-request": (1,),
        "run-status": (1,),
    },
)


def make_request(
    tmp_path: Path, *, authorize: bool = True, **overrides: object
) -> dict[str, object]:
    launch_token = "1" * 64
    launch_path = (
        tmp_path / "runpod-jobrunner" / "runs" / "run-test-001" / LAUNCH_RELATIVE_PATH
    )
    if authorize:
        launch_path.parent.mkdir(parents=True, exist_ok=True)
        launch_path.write_text(f"{launch_token}\n", encoding="ascii")
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
        "runner_version": TEST_IDENTITY.version,
        "runner_git_commit": TEST_IDENTITY.git_commit,
        "supported_protocol_majors": {
            "artifact-manifest": [1],
            "launch-authorization": [1],
            "run-event": [1],
            "run-request": [1],
            "run-status": [1],
        },
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
        "artifact_path_base": "run-root",
        "launch_authorization": {
            "protocol": LAUNCH_PROTOCOL,
            "path": LAUNCH_RELATIVE_PATH,
            "sha256": hashlib.sha256(launch_token.encode()).hexdigest(),
            "size": len(launch_token) + 1,
            "timeout_seconds": 2,
        },
    }
    request.update(overrides)
    return request


def runner(
    request: dict[str, object],
    status_dir: Path,
    *,
    request_path: Path | None = None,
) -> RemoteRunner:
    return RemoteRunner(
        request,
        status_dir,
        request_path=request_path,
        runner_identity=TEST_IDENTITY,
    )


@pytest.mark.parametrize(
    ("identity", "reason"),
    [
        (
            RunnerIdentity(
                "0.2.0",
                "a" * 40,
                {
                    "artifact-manifest": (1,),
                    "launch-authorization": (1,),
                    "run-event": (1,),
                    "run-request": (1,),
                    "run-status": (1,),
                },
            ),
            "runner_version_mismatch",
        ),
        (
            RunnerIdentity(
                "0.1.1",
                "b" * 40,
                {
                    "artifact-manifest": (1,),
                    "launch-authorization": (1,),
                    "run-event": (1,),
                    "run-request": (1,),
                    "run-status": (1,),
                },
            ),
            "runner_git_commit_mismatch",
        ),
        (
            RunnerIdentity(
                "0.1.1",
                "a" * 40,
                {
                    "artifact-manifest": (1,),
                    "launch-authorization": (1,),
                    "run-event": (1,),
                    "run-request": (2,),
                    "run-status": (1,),
                },
            ),
            "unsupported_protocol_major",
        ),
    ],
)
def test_runner_rejects_identity_mismatch_before_any_phase(
    tmp_path: Path, identity: RunnerIdentity, reason: str
) -> None:
    request = make_request(tmp_path)

    terminal = RemoteRunner(
        request,
        tmp_path / "status",
        runner_identity=identity,
    ).run()

    assert terminal["outcome"] == "failed"
    assert terminal["reason"] == reason
    assert not (tmp_path / "phase-trace.txt").exists()
    status = read_json(tmp_path / "status" / "status.json")
    assert status["runner_version"] == identity.version
    assert status["runner_git_commit"] == identity.git_commit


def test_runner_requires_the_explicit_run_root_artifact_base(tmp_path: Path) -> None:
    request = make_request(tmp_path)
    request.pop("artifact_path_base")

    with pytest.raises(RequestError, match="artifact_path_base must be run-root"):
        runner(request, tmp_path / "status")


def test_missing_published_release_receipt_fails_with_authenticated_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request = make_request(tmp_path)
    monkeypatch.setenv(
        "RUNPOD_JOBRUNNER_RELEASE_PATH",
        str(tmp_path / "missing-release.json"),
    )

    terminal = RemoteRunner(request, tmp_path / "status").run()

    assert terminal["outcome"] == "failed"
    assert terminal["reason"] == "runner_identity_unavailable"
    assert not (tmp_path / "phase-trace.txt").exists()
    status = read_json(tmp_path / "status" / "status.json")
    assert status["runner_git_commit"] == "0" * 40


def read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text())


def test_runner_executes_enabled_phases_in_fixed_order(tmp_path: Path) -> None:
    request = make_request(tmp_path)
    status_dir = tmp_path / "status"

    terminal = runner(request, status_dir).run()

    assert terminal["outcome"] == "succeeded"
    assert (tmp_path / "phase-trace.txt").read_text().splitlines() == list(PHASE_ORDER)
    status = read_json(status_dir / "status.json")
    assert status["state"] == "terminal"
    assert status["terminal_result"] == terminal
    events = [json.loads(line) for line in (status_dir / "events.jsonl").read_text().splitlines()]
    assert [event["sequence"] for event in events] == list(range(1, len(events) + 1))


def test_failed_phase_publishes_durable_diagnostic_artifact_manifest(
    tmp_path: Path,
) -> None:
    request = make_request(tmp_path)
    phases = cast(dict[str, dict[str, object]], request["phases"])
    for phase in PHASE_ORDER:
        phases[phase]["enabled"] = phase == "preflight"
    phases["preflight"]["argv"] = [
        sys.executable,
        "-u",
        "-c",
        "import sys; print('hopper probe failed'); "
        "print('exact cause', file=sys.stderr); sys.exit(7)",
    ]
    request["artifact_manifest_path"] = "artifact-manifest.json"

    terminal = runner(request, tmp_path / "status").run()

    assert terminal["outcome"] == "failed"
    assert terminal["reason"] == "phase_nonzero_exit"
    assert terminal["phase_exit_codes"] == {"preflight": 7}
    phase_log = (
        tmp_path
        / "runpod-jobrunner/runs/run-test-001/diagnostics/phases/preflight.log"
    )
    assert phase_log.read_text() == "hopper probe failed\nexact cause\n"
    manifest_path = tmp_path / "runpod-jobrunner/runs/run-test-001/artifact-manifest.json"
    manifest = read_json(manifest_path)
    assert manifest["protocol"] == "artifact-manifest/1"
    assert manifest["run_id"] == "run-test-001"
    assert manifest["files"] == [
        {
            "path": "diagnostics/phases/preflight.log",
            "size": phase_log.stat().st_size,
            "sha256": hashlib.sha256(phase_log.read_bytes()).hexdigest(),
        }
    ]
    assert terminal["artifact_manifest_size"] == manifest_path.stat().st_size
    assert terminal["artifact_manifest_sha256"] == hashlib.sha256(
        manifest_path.read_bytes()
    ).hexdigest()


def test_runner_accepts_an_existing_network_volume_request(tmp_path: Path) -> None:
    request = make_request(
        tmp_path,
        storage={
            "encrypted": False,
            "network_volume_id": "network-volume-123",
            "mount": str(tmp_path),
            "required_gb": 1,
        },
    )

    terminal = runner(request, tmp_path / "status").run()

    assert terminal["outcome"] == "succeeded"


@pytest.mark.parametrize(
    "storage",
    [
        {"encrypted": False, "mount": "/workspace", "required_gb": 1},
        {
            "encrypted": False,
            "network_volume_id": "   ",
            "mount": "/workspace",
            "required_gb": 1,
        },
        {
            "encrypted": True,
            "network_volume_id": "network-volume-123",
            "mount": "/workspace",
            "required_gb": 1,
        },
    ],
)
def test_runner_rejects_ambiguous_or_missing_network_volume_identity(
    tmp_path: Path,
    storage: dict[str, object],
) -> None:
    request = make_request(tmp_path, storage=storage)

    with pytest.raises(RequestError, match="network_volume_id"):
        runner(request, tmp_path / "status")


def test_runner_publishes_ready_and_waits_for_matching_launch_authorization(
    tmp_path: Path,
) -> None:
    request = make_request(tmp_path, authorize=False)
    status_dir = tmp_path / "status"
    remote_runner = runner(request, status_dir)
    terminals: list[dict[str, object]] = []
    thread = threading.Thread(target=lambda: terminals.append(remote_runner.run()))
    thread.start()

    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if (status_dir / "status.json").exists():
            status = read_json(status_dir / "status.json")
            if status["state"] == "ready":
                break
        time.sleep(0.005)
    else:
        raise AssertionError("runner did not publish ready")
    time.sleep(0.1)
    assert thread.is_alive()
    assert not (tmp_path / "phase-trace.txt").exists()
    launch = cast(dict[str, object], request["launch_authorization"])
    launch_path = (
        tmp_path / "runpod-jobrunner" / "runs" / "run-test-001" / str(launch["path"])
    )
    launch_path.parent.mkdir(parents=True, exist_ok=True)
    launch_path.write_text(f"{'1' * 64}\n", encoding="ascii")
    thread.join(timeout=3)

    assert not thread.is_alive()
    assert terminals[0]["outcome"] == "succeeded"
    assert (tmp_path / "phase-trace.txt").read_text().splitlines() == list(PHASE_ORDER)


def test_runner_times_out_without_launch_authorization_and_starts_no_phase(
    tmp_path: Path,
) -> None:
    request = make_request(tmp_path, authorize=False)
    launch = cast(dict[str, object], request["launch_authorization"])
    launch["timeout_seconds"] = 0.05

    terminal = runner(request, tmp_path / "status").run()

    assert terminal["outcome"] == "failed"
    assert terminal["reason"] == "launch_authorization_timeout"
    assert not (tmp_path / "phase-trace.txt").exists()


def test_runner_rejects_tampered_launch_authorization_and_starts_no_phase(
    tmp_path: Path,
) -> None:
    request = make_request(tmp_path, authorize=False)
    launch_path = (
        tmp_path / "runpod-jobrunner" / "runs" / "run-test-001" / LAUNCH_RELATIVE_PATH
    )
    launch_path.parent.mkdir(parents=True)
    launch_path.write_text(f"{'2' * 64}\n", encoding="ascii")

    terminal = runner(request, tmp_path / "status").run()

    assert terminal["outcome"] == "failed"
    assert terminal["reason"] == "launch_authorization_invalid"
    assert not (tmp_path / "phase-trace.txt").exists()


def test_runner_rejects_status_token_reuse_as_launch_authorization(
    tmp_path: Path,
) -> None:
    request = make_request(tmp_path)
    launch = cast(dict[str, object], request["launch_authorization"])

    terminal = RemoteRunner(
        request,
        tmp_path / "status",
        runner_identity=TEST_IDENTITY,
        status_token_sha256=str(launch["sha256"]),
    ).run()

    assert terminal["outcome"] == "failed"
    assert terminal["reason"] == "launch_authorization_invalid"
    assert not (tmp_path / "phase-trace.txt").exists()


def test_success_with_declared_artifacts_publishes_verified_manifest_hash(
    tmp_path: Path,
) -> None:
    request = make_request(tmp_path)
    request["artifact_manifest_path"] = "artifacts/manifest.json"
    phases = cast(dict[str, object], request["phases"])
    payload = b"verified artifact\n"
    package_script = (
        "import hashlib,json,os,pathlib;"
        "root=pathlib.Path(os.environ['RUNPOD_JOBRUNNER_RUN_ROOT']);"
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

    terminal = runner(request, tmp_path / "status").run()

    run_root = tmp_path / "runpod-jobrunner" / "runs" / "run-test-001"
    manifest = run_root / "artifacts" / "manifest.json"
    assert terminal["outcome"] == "succeeded"
    assert terminal["artifact_manifest_sha256"] == hashlib.sha256(manifest.read_bytes()).hexdigest()
    assert terminal["artifact_manifest_size"] == manifest.stat().st_size


def test_success_fails_closed_when_declared_artifact_manifest_is_missing(
    tmp_path: Path,
) -> None:
    request = make_request(tmp_path)
    request["artifact_manifest_path"] = "artifacts/manifest.json"

    terminal = runner(request, tmp_path / "status").run()

    assert terminal["outcome"] == "failed"
    assert terminal["reason"] == "artifact_manifest_missing"


def test_success_does_not_reuse_a_declared_manifest_from_a_sibling_run(
    tmp_path: Path,
) -> None:
    request = make_request(tmp_path)
    request["artifact_manifest_path"] = "artifacts/manifest.json"
    sibling_artifacts = (
        tmp_path
        / "runpod-jobrunner"
        / "runs"
        / "run-stale"
        / "artifacts"
    )
    sibling_artifacts.mkdir(parents=True)
    payload = b"stale artifact\n"
    (sibling_artifacts / "result.txt").write_bytes(payload)
    (sibling_artifacts / "manifest.json").write_text(
        json.dumps(
            {
                "protocol": "artifact-manifest/1",
                "run_id": "run-stale",
                "files": [
                    {
                        "path": "artifacts/result.txt",
                        "size": len(payload),
                        "sha256": hashlib.sha256(payload).hexdigest(),
                    }
                ],
            }
        )
    )

    terminal = runner(request, tmp_path / "status").run()

    assert terminal["outcome"] == "failed"
    assert terminal["reason"] == "artifact_manifest_missing"


def test_success_fails_closed_when_declared_artifact_hash_is_wrong(tmp_path: Path) -> None:
    request = make_request(tmp_path)
    request["artifact_manifest_path"] = "artifacts/manifest.json"
    phases = cast(dict[str, object], request["phases"])
    script = (
        "import json,os,pathlib;"
        "root=pathlib.Path(os.environ['RUNPOD_JOBRUNNER_RUN_ROOT']);"
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

    terminal = runner(request, tmp_path / "status").run()

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

    terminal = runner(request, tmp_path / "status").run()

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
    terminal = runner(request, tmp_path / "status").run()

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
        terminal = runner(request, case_dir / "status").run()

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
    remote_runner = runner(request, status_dir)
    terminals: list[dict[str, object]] = []
    thread = threading.Thread(target=lambda: terminals.append(remote_runner.run()))
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
    first_terminal = runner(request, status_dir).run()
    original_terminal_bytes = (status_dir / "terminal-result.json").read_bytes()
    original_event_bytes = (status_dir / "events.jsonl").read_bytes()
    phases = request["phases"]
    assert isinstance(phases, dict)
    phases["verify"] = {
        "enabled": True,
        "argv": [sys.executable, "-c", "raise SystemExit('must not rerun')"],
        "timeout_seconds": 5,
    }

    second_terminal = runner(request, status_dir).run()

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

    terminal = runner(request, status_dir, request_path=request_path).run()

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

    terminal = runner(request, tmp_path / "status").run()

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

    terminal = runner(request, status_dir).run()

    assert terminal["outcome"] == "succeeded"
    assert terminal["completed_phases"] == ["verify"]
    assert not (tmp_path / "phase-trace.txt").exists()


@pytest.mark.parametrize("fragment", [b'{"protocol":', b'{"exit_code":-'])
def test_restart_truncates_only_a_torn_final_event_record(
    tmp_path: Path, fragment: bytes
) -> None:
    request = make_request(tmp_path)
    status_dir = tmp_path / "status"
    status_dir.mkdir()
    event = {
        "protocol": "run-event/1",
        "run_id": "run-test-001",
        "sequence": 1,
        "kind": "ready",
        "phase": None,
        "monotonic_seconds": 0.1,
    }
    durable_prefix = (json.dumps(event) + "\n").encode()
    journal = status_dir / "events.jsonl"
    journal.write_bytes(durable_prefix + fragment)

    runner(request, status_dir)

    assert journal.read_bytes() == durable_prefix


def test_restart_rejects_a_complete_malformed_event_record(tmp_path: Path) -> None:
    request = make_request(tmp_path)
    status_dir = tmp_path / "status"
    status_dir.mkdir()
    (status_dir / "events.jsonl").write_bytes(b'{"protocol":}\n')

    with pytest.raises(RequestError, match="invalid JSON"):
        runner(request, status_dir)


def test_restart_rejects_nonterminated_but_complete_malformed_event(tmp_path: Path) -> None:
    request = make_request(tmp_path)
    status_dir = tmp_path / "status"
    status_dir.mkdir()
    (status_dir / "events.jsonl").write_bytes(b'{"protocol":}')

    with pytest.raises(RequestError, match="incomplete record terminator"):
        runner(request, status_dir)


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

    terminal = runner(request, status_dir).run()

    assert terminal["outcome"] == "failed"
    assert terminal["reason"] == "runner_restart_unknown_child"
    assert not (tmp_path / "phase-trace.txt").exists()
