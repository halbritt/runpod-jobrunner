"""Command-line adapter for :class:`runpod_jobrunner.application.JobRunner`."""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Protocol

from runpod_jobrunner.application import JobRunner


class _Application(Protocol):
    def check(self, path: Path | str) -> Mapping[str, object]: ...

    def run(
        self,
        path: Path | str,
        *,
        approved_max_usd: Decimal | str,
        budget_scope: str | None = None,
        budget_total_usd: Decimal | str | None = None,
    ) -> str: ...

    def status(self, run_id: str) -> Mapping[str, object]: ...

    def stop(self, run_id: str) -> Mapping[str, object]: ...

    def recover(self, run_id: str) -> Mapping[str, object]: ...


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="runpod-jobrunner")
    subcommands = parser.add_subparsers(dest="command", required=True)

    check = subcommands.add_parser("check")
    check.add_argument("job_bundle", type=Path)

    run = subcommands.add_parser("run")
    run.add_argument("job_bundle", type=Path)
    run.add_argument("--approve-max-usd", required=True, type=_decimal)
    run.add_argument("--budget-scope")
    run.add_argument("--budget-total-usd", type=_decimal)

    status = subcommands.add_parser("status")
    status.add_argument("run_id")
    status.add_argument("--follow", action="store_true")

    stop = subcommands.add_parser("stop")
    stop.add_argument("run_id")

    recover = subcommands.add_parser("recover")
    recover.add_argument("run_id")
    return parser


def _decimal(value: str) -> Decimal:
    try:
        result = Decimal(value)
    except InvalidOperation as error:
        raise argparse.ArgumentTypeError("must be a decimal amount") from error
    if not result.is_finite() or result <= 0:
        raise argparse.ArgumentTypeError("must be positive and finite")
    return result


def main(
    argv: Sequence[str] | None = None,
    *,
    application: _Application | None = None,
) -> int:
    arguments = _parser().parse_args(argv)
    app = application or JobRunner()
    command = str(arguments.command)
    if command == "check":
        _print_json(app.check(arguments.job_bundle))
        return 0
    if command == "run":
        run_id = app.run(
            arguments.job_bundle,
            approved_max_usd=arguments.approve_max_usd,
            budget_scope=arguments.budget_scope,
            budget_total_usd=arguments.budget_total_usd,
        )
        _print_json({"run_id": run_id})
        return 0
    if command == "status":
        while True:
            state = app.status(str(arguments.run_id))
            _print_json(state)
            if not bool(arguments.follow) or state.get("lifecycle") == "closed":
                return 0
            time.sleep(1)
    if command == "stop":
        _print_json(app.stop(str(arguments.run_id)))
        return 0
    if command == "recover":
        _print_json(app.recover(str(arguments.run_id)))
        return 0
    raise AssertionError("argparse accepted an unknown command")


def _print_json(value: Mapping[str, object]) -> None:
    json.dump(value, sys.stdout, sort_keys=True, separators=(",", ":"))
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
