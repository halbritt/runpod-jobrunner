from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

from runpod_jobrunner.cli import main


class FakeApplication:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def check(self, path: Path | str) -> dict[str, object]:
        self.calls.append(("check", Path(path)))
        return {"name": "noop", "max_cost_usd": "0.50"}

    def run(
        self,
        path: Path | str,
        *,
        approved_max_usd: Decimal | str,
        budget_scope: str | None = None,
        budget_total_usd: Decimal | str | None = None,
    ) -> str:
        self.calls.append(("run", Path(path), approved_max_usd, budget_scope, budget_total_usd))
        return "run-1"

    def status(self, run_id: str) -> dict[str, Any]:
        self.calls.append(("status", run_id))
        return {"run_id": run_id, "lifecycle": "closed"}

    def stop(self, run_id: str) -> dict[str, Any]:
        self.calls.append(("stop", run_id))
        return {"run_id": run_id, "lifecycle": "deleting"}

    def recover(self, run_id: str) -> dict[str, Any]:
        self.calls.append(("recover", run_id))
        return {"run_id": run_id, "lifecycle": "running"}


def test_run_requires_and_passes_exact_decimal_approval(capsys: Any) -> None:
    app = FakeApplication()

    result = main(["run", "/tmp/bundle", "--approve-max-usd", "0.50"], application=app)

    assert result == 0
    assert app.calls == [("run", Path("/tmp/bundle"), Decimal("0.50"), None, None)]
    assert json.loads(capsys.readouterr().out) == {"run_id": "run-1"}


def test_run_passes_aggregate_budget_reservation(capsys: Any) -> None:
    app = FakeApplication()

    result = main(
        [
            "run",
            "/tmp/bundle",
            "--approve-max-usd",
            "47",
            "--budget-scope",
            "striatum-2026-07-31",
            "--budget-total-usd",
            "50",
        ],
        application=app,
    )

    assert result == 0
    assert app.calls == [
        (
            "run",
            Path("/tmp/bundle"),
            Decimal("47"),
            "striatum-2026-07-31",
            Decimal("50"),
        )
    ]
    capsys.readouterr()


def test_check_and_lifecycle_commands_emit_json(capsys: Any) -> None:
    app = FakeApplication()

    for argv in (
        ["check", "/tmp/bundle"],
        ["status", "run-1"],
        ["stop", "run-1"],
        ["recover", "run-1"],
    ):
        assert main(argv, application=app) == 0
        assert json.loads(capsys.readouterr().out)
