"""Conservative aggregate authorization ledger for multiple paid runs."""

from __future__ import annotations

import fcntl
import json
import os
import re
import tempfile
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, cast

_SCOPE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")


class BudgetExceeded(ValueError):
    """A reservation would exceed or mutate the principal's durable authority."""


class BudgetLedger:
    """Reserve full per-run caps; never infer refunds from lifecycle completion."""

    def __init__(self, root: Path | None = None) -> None:
        if root is None:
            xdg_state = os.environ.get("XDG_STATE_HOME")
            state_home = Path(xdg_state) if xdg_state else Path.home() / ".local" / "state"
            root = state_home / "runpod-jobrunner" / "budgets"
        self.root = root

    def reserve(
        self,
        scope: str,
        total_authorized_usd: Decimal | str,
        run_id: str,
        run_cap_usd: Decimal | str,
    ) -> dict[str, object]:
        path, lock_path = self._paths(scope)
        total = _money(total_authorized_usd, "total authorization")
        cap = _money(run_cap_usd, "run cap")
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            state: dict[str, Any]
            if path.exists():
                state = _read(path)
                existing_total = _money(
                    state.get("total_authorized_usd"), "stored total authorization"
                )
                if existing_total != total:
                    raise BudgetExceeded("budget total authority cannot change in place")
            else:
                state = {
                    "protocol": "budget-ledger/1",
                    "scope": scope,
                    "total_authorized_usd": format(total, "f"),
                    "reservations": cast(object, {}),
                }
            reservations_value = state.get("reservations")
            if not isinstance(reservations_value, dict):
                raise ValueError("budget reservations are corrupt")
            reservations = cast(dict[str, object], reservations_value)
            existing = reservations.get(run_id)
            if existing is not None:
                if _money(existing, "stored run reservation") != cap:
                    raise BudgetExceeded("run already reserved with a different cap")
                return _projection(state)
            reserved = sum(
                (_money(value, "stored run reservation") for value in reservations.values()),
                Decimal("0"),
            )
            if reserved + cap > total:
                raise BudgetExceeded(f"run cap {cap} would exceed aggregate authority {total}")
            reservations[run_id] = format(cap, "f")
            _atomic_json(path, state)
            return _projection(state)
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def status(self, scope: str) -> dict[str, object]:
        path, _lock = self._paths(scope)
        return _projection(_read(path))

    def _paths(self, scope: str) -> tuple[Path, Path]:
        if _SCOPE.fullmatch(scope) is None:
            raise ValueError("budget scope must be a safe identifier")
        return self.root / f"{scope}.json", self.root / f".{scope}.lock"


def _projection(state: Mapping[str, object]) -> dict[str, object]:
    if state.get("protocol") != "budget-ledger/1":
        raise ValueError("unsupported budget ledger protocol")
    total_value = state.get("total_authorized_usd")
    total = _money(total_value, "stored total authorization")
    reservations_value = state.get("reservations")
    if not isinstance(reservations_value, Mapping):
        raise ValueError("budget reservations are corrupt")
    reservations = cast(Mapping[object, object], reservations_value)
    if not all(isinstance(key, str) for key in reservations):
        raise ValueError("budget reservation IDs are corrupt")
    reserved = sum(
        (_money(value, "stored run reservation") for value in reservations.values()),
        Decimal("0"),
    )
    return {
        "protocol": "budget-ledger/1",
        "scope": state.get("scope"),
        "total_authorized_usd": str(total_value),
        "reserved_usd": format(reserved, "f"),
        "remaining_usd": format(total - reserved, "f"),
        "reservations": dict(cast(Mapping[str, object], reservations)),
    }


def _money(value: object, name: str) -> Decimal:
    if isinstance(value, float) or not isinstance(value, (Decimal, str)):
        raise TypeError(f"{name} must be a Decimal or decimal string")
    try:
        result = Decimal(value)
    except InvalidOperation as error:
        raise ValueError(f"{name} is invalid") from error
    if not result.is_finite() or result <= 0:
        raise ValueError(f"{name} must be positive and finite")
    return result


def _read(path: Path) -> dict[str, Any]:
    try:
        value: object = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        raise ValueError("budget ledger is unavailable or corrupt") from None
    if not isinstance(value, dict):
        raise ValueError("budget ledger is corrupt")
    return cast(dict[str, Any], value)


def _atomic_json(path: Path, state: Mapping[str, object]) -> None:
    data = (
        json.dumps(state, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode()
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        offset = 0
        while offset < len(data):
            offset += os.write(descriptor, data[offset:])
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
