"""Local durable supervision through transient user-systemd services."""

from __future__ import annotations

import re
import subprocess
from collections.abc import Callable
from pathlib import Path
from subprocess import CompletedProcess

_SAFE_UNIT = re.compile(r"[^a-z0-9]+")


class SupervisionError(RuntimeError):
    """The local service manager rejected a supervision operation."""


RunCommand = Callable[..., CompletedProcess[str]]


def unit_name_for_run(run_id: str) -> str:
    safe = _SAFE_UNIT.sub("-", run_id.lower()).strip("-")
    if not safe:
        raise ValueError("run id has no service-safe characters")
    return f"runpod-jobrunner-{safe}.service"


class SystemdSupervisor:
    def __init__(self, *, run_command: RunCommand = subprocess.run) -> None:
        self._run = run_command

    def launch(self, run_id: str, executable: Path, run_dir: Path) -> None:
        argv = [
            "systemd-run",
            "--user",
            "--no-block",
            "--collect",
            f"--unit={unit_name_for_run(run_id)}",
            "--property=Type=exec",
            "--property=Restart=on-failure",
            "--property=RestartSec=5s",
            "--property=StartLimitIntervalSec=0",
            "--property=TimeoutStopSec=30s",
            str(executable.resolve()),
            "--run-dir",
            str(run_dir.resolve()),
        ]
        self._checked(argv)

    def wake(self, run_id: str) -> None:
        self._checked(["systemctl", "--user", "restart", unit_name_for_run(run_id)])

    def state(self, run_id: str) -> dict[str, str]:
        argv = [
            "systemctl",
            "--user",
            "show",
            unit_name_for_run(run_id),
            "--property=LoadState,ActiveState,SubState,Result,NRestarts",
        ]
        result = self._checked(argv)
        pairs: dict[str, str] = {}
        for line in result.stdout.splitlines():
            key, separator, value = line.partition("=")
            if separator:
                pairs[key] = value
        return pairs

    def _checked(self, argv: list[str]) -> CompletedProcess[str]:
        result = self._run(argv, check=False, text=True, capture_output=True)
        if result.returncode != 0:
            detail = result.stderr.strip() or "service-manager failure"
            raise SupervisionError(detail)
        return result
