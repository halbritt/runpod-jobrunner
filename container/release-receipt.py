#!/usr/bin/env python3
"""Write the immutable runner build identity into a container layer."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

SEMVER = re.compile(
    r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"
    r"(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?\Z"
)
COMMIT = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")


def main() -> int:
    if len(sys.argv) != 4:
        raise SystemExit("usage: release-receipt.py OUTPUT VERSION GIT_COMMIT")
    output = Path(sys.argv[1])
    version = sys.argv[2]
    commit = sys.argv[3]
    if SEMVER.fullmatch(version) is None:
        raise SystemExit("VERSION must be an exact semantic version")
    if COMMIT.fullmatch(commit) is None:
        raise SystemExit("GIT_COMMIT must be a full SHA-1 or SHA-256 object ID")
    if not commit.strip("0"):
        raise SystemExit("GIT_COMMIT must not be all zero")
    receipt = {
        "protocol": "runner-release/1",
        "runner_version": version,
        "runner_git_commit": commit,
        "supported_protocol_majors": {
            "artifact-manifest": [1],
            "incremental-mirror-ack": [1],
            "launch-authorization": [1],
            "run-event": [1],
            "run-request": [1],
            "run-status": [1],
        },
    }
    output.write_text(json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n")
    output.chmod(0o444)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
