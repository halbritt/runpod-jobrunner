#!/usr/bin/env python3
"""Deterministic phase command used by the first no-op integration bundle."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import time
from pathlib import Path

PHASES = frozenset({"verify", "preflight", "train", "evaluate", "package"})


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def _encoded_json(record: object) -> bytes:
    return (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _parse_sleep_seconds(argv: list[str]) -> int:
    if len(argv) == 2:
        return 0
    if len(argv) != 4 or argv[2] != "--sleep-seconds":
        raise ValueError
    try:
        seconds = int(argv[3])
    except ValueError as error:
        raise ValueError from error
    if not 0 <= seconds <= 300:
        raise ValueError
    return seconds


def main(argv: list[str]) -> int:
    if len(argv) < 2 or argv[1] not in PHASES:
        print(
            "usage: noop.py {verify|preflight|train|evaluate|package} "
            "[--sleep-seconds 0..300]",
            file=sys.stderr,
        )
        return 64

    try:
        sleep_seconds = _parse_sleep_seconds(argv)
    except ValueError:
        print(
            "usage: noop.py {verify|preflight|train|evaluate|package} "
            "[--sleep-seconds 0..300]",
            file=sys.stderr,
        )
        return 64

    phase = argv[1]
    run_id = os.environ.get("RUNPOD_JOBRUNNER_RUN_ID", "")
    storage_mount = os.environ.get("RUNPOD_JOBRUNNER_STORAGE_MOUNT", "")
    run_root = os.environ.get("RUNPOD_JOBRUNNER_RUN_ROOT", "")
    if not run_id or not storage_mount or not run_root:
        print("run-scoped environment is incomplete", file=sys.stderr)
        return 65

    if sleep_seconds:
        time.sleep(sleep_seconds)

    if phase == "package":
        artifacts_root = Path(run_root).resolve()
        artifact_path = artifacts_root / "artifacts" / "noop-result.json"
        artifact_bytes = _encoded_json({"result": "noop", "run_id": run_id})
        _atomic_write(artifact_path, artifact_bytes)
        manifest = {
            "protocol": "artifact-manifest/1",
            "run_id": run_id,
            "files": [
                {
                    "path": "artifacts/noop-result.json",
                    "sha256": hashlib.sha256(artifact_bytes).hexdigest(),
                    "size": len(artifact_bytes),
                }
            ],
        }
        _atomic_write(
            artifacts_root / "artifacts" / "manifest.json",
            _encoded_json(manifest),
        )

    print(json.dumps({"phase": phase, "run_id": run_id}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
