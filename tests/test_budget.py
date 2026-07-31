from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from runpod_jobrunner.budget import BudgetExceeded, BudgetLedger


def test_budget_ledger_conservatively_reserves_aggregate_caps(tmp_path: Path) -> None:
    ledger = BudgetLedger(tmp_path / "budgets")

    ledger.reserve("striatum-2026-07-31", "50.00", "run-smoke", "0.50")
    ledger.reserve("striatum-2026-07-31", "50.00", "run-training", "47.00")

    state = ledger.status("striatum-2026-07-31")
    assert state["total_authorized_usd"] == "50.00"
    assert state["reserved_usd"] == "47.50"
    assert state["remaining_usd"] == "2.50"


def test_budget_ledger_rejects_cap_sum_above_authority_without_writing_it(
    tmp_path: Path,
) -> None:
    ledger = BudgetLedger(tmp_path / "budgets")
    ledger.reserve("scope", Decimal("50"), "run-one", Decimal("47"))

    with pytest.raises(BudgetExceeded, match="exceed"):
        ledger.reserve("scope", Decimal("50"), "run-two", Decimal("4"))

    stored = json.loads((tmp_path / "budgets" / "scope.json").read_text())
    assert stored["reservations"] == {"run-one": "47"}


def test_budget_reservation_is_idempotent_but_cannot_change_one_run_cap(
    tmp_path: Path,
) -> None:
    ledger = BudgetLedger(tmp_path / "budgets")
    first = ledger.reserve("scope", "50", "run-one", "1.25")
    second = ledger.reserve("scope", "50", "run-one", "1.25")

    assert second == first
    with pytest.raises(BudgetExceeded, match="already reserved"):
        ledger.reserve("scope", "50", "run-one", "2")
