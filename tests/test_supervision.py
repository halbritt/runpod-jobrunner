from __future__ import annotations

from pathlib import Path
from subprocess import CompletedProcess
from typing import Any

from runpod_jobrunner.supervision import SystemdSupervisor, unit_name_for_run


def test_unit_name_is_deterministic_and_sanitized() -> None:
    assert unit_name_for_run("Run/ABC_123") == "runpod-jobrunner-run-abc-123.service"


def test_launch_uses_transient_service_with_restart_and_no_secrets(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def run(argv: list[str], **kwargs: Any) -> CompletedProcess[str]:
        calls.append(argv)
        return CompletedProcess(argv, 0, stdout="", stderr="")

    state = tmp_path / "runs" / "run-1"
    state.mkdir(parents=True)
    supervisor = SystemdSupervisor(run_command=run)

    supervisor.launch("run-1", Path("/opt/jobrunner/bin/supervise"), state)

    argv = calls[0]
    assert argv[:3] == ["systemd-run", "--user", "--no-block"]
    assert "--collect" in argv
    assert "--property=Type=exec" in argv
    assert "--property=Restart=on-failure" in argv
    assert "--property=RestartSec=5s" in argv
    assert argv[-2:] == ["--run-dir", str(state.resolve())]
    assert not any("token" in item.lower() or "secret" in item.lower() for item in argv)


def test_wake_restarts_existing_unit(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def run(argv: list[str], **kwargs: Any) -> CompletedProcess[str]:
        calls.append(argv)
        return CompletedProcess(argv, 0, stdout="", stderr="")

    supervisor = SystemdSupervisor(run_command=run)
    supervisor.wake("run-1")

    assert calls == [["systemctl", "--user", "restart", "runpod-jobrunner-run-1.service"]]
