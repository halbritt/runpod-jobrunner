from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

from runpod_jobrunner import __version__

ROOT = Path(__file__).resolve().parents[1]
CONTAINER = ROOT / "container"


def test_image_uses_a_digest_pinned_official_runpod_base_and_exec_form_init() -> None:
    dockerfile = (CONTAINER / "Dockerfile").read_text()

    from_lines = [line for line in dockerfile.splitlines() if line.startswith("FROM ")]
    assert from_lines
    assert all(
        re.fullmatch(r"FROM runpod/base:[^\s@]+@sha256:[0-9a-f]{64}", line) for line in from_lines
    )
    assert 'ENTRYPOINT ["/usr/bin/tini", "--", "/opt/runpod-jobrunner/entrypoint"]' in dockerfile
    assert "EXPOSE 22 8080" in dockerfile
    assert not re.search(r"^CMD\s+[^[]", dockerfile, flags=re.MULTILINE)
    assert not re.search(r"^ENTRYPOINT\s+[^[]", dockerfile, flags=re.MULTILINE)


def test_image_copies_only_declared_source_and_never_bakes_credentials() -> None:
    dockerfile = (CONTAINER / "Dockerfile").read_text()
    dockerignore = (CONTAINER / "Dockerfile.dockerignore").read_text().splitlines()
    normalized = dockerfile.lower()

    assert not re.search(r"^copy\s+(?:--\S+\s+)*\.\s", normalized, flags=re.MULTILINE)
    assert not re.search(
        r"^(?:arg|env)\s+[^\n]*(?:secret|token|password|credential|api[_-]?key)",
        normalized,
        flags=re.MULTILINE,
    )
    assert ".ssh" not in normalized
    assert ".env" not in normalized
    assert dockerignore[0] == "**"
    assert set(dockerignore[1:]) == {
        "!pyproject.toml",
        "!README.md",
        "!LICENSE",
        "!src",
        "!src/**",
        "!container",
        "!container/Dockerfile",
        "!container/entrypoint.sh",
        "!container/noop.py",
        "!container/release-receipt.py",
        "!container/requirements-runtime.lock",
    }


def test_entrypoint_preserves_ssh_bootstrap_and_has_a_bounded_request_wait() -> None:
    entrypoint = (CONTAINER / "entrypoint.sh").read_text()

    subprocess.run(["bash", "-n", str(CONTAINER / "entrypoint.sh")], check=True)
    start_offset = entrypoint.index("/start.sh &")
    exec_offset = entrypoint.index("exec runpod-jobrunner-remote")
    assert start_offset < exec_offset
    assert "RUNPOD_JOBRUNNER_REQUEST_WAIT_SECONDS" in entrypoint
    assert "exit 124" in entrypoint
    assert "/workspace/runpod-jobrunner/request.json" in entrypoint
    assert "/workspace/runpod-jobrunner/status-token" in entrypoint
    assert "--status-port 8080" in entrypoint
    assert "sh -c" not in entrypoint
    assert "RUNPOD_JOBRUNNER_RELEASE_PATH=/opt/runpod-jobrunner/release.json" in entrypoint
    assert "runner state path must be below the declared /workspace storage mount" in entrypoint
    assert "encrypted /workspace" not in entrypoint


def test_image_embeds_a_full_build_identity_receipt(tmp_path: Path) -> None:
    dockerfile = (CONTAINER / "Dockerfile").read_text()
    assert "install -m 0444 LICENSE /opt/runpod-jobrunner/LICENSE" in dockerfile
    assert 'org.opencontainers.image.version="${RUNNER_VERSION}"' in dockerfile

    output = tmp_path / "release.json"
    subprocess.run(
        [
            sys.executable,
            str(CONTAINER / "release-receipt.py"),
            str(output),
            __version__,
            "a" * 40,
        ],
        check=True,
    )

    assert json.loads(output.read_text()) == {
        "protocol": "runner-release/1",
        "runner_version": __version__,
        "runner_git_commit": "a" * 40,
        "supported_protocol_majors": {
            "artifact-manifest": [1],
            "incremental-mirror-ack": [1],
            "launch-authorization": [1],
            "run-event": [1],
            "run-request": [1],
            "run-status": [1],
        },
    }
    assert output.stat().st_mode & 0o777 == 0o444


def test_noop_package_phase_creates_a_small_content_verified_manifest(
    tmp_path: Path,
) -> None:
    run_root = tmp_path / "runpod-jobrunner/runs/run-container-contract-001"
    environment = os.environ.copy()
    environment.update(
        RUNPOD_JOBRUNNER_RUN_ID="run-container-contract-001",
        RUNPOD_JOBRUNNER_STORAGE_MOUNT=str(tmp_path),
        RUNPOD_JOBRUNNER_RUN_ROOT=str(run_root),
    )

    completed = subprocess.run(
        [str(CONTAINER / "noop.py"), "package"],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )

    manifest_path = run_root / "artifacts" / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    artifact = run_root / str(manifest["files"][0]["path"])
    assert json.loads(completed.stdout) == {
        "phase": "package",
        "run_id": "run-container-contract-001",
    }
    assert manifest == {
        "protocol": "artifact-manifest/1",
        "run_id": "run-container-contract-001",
        "files": [
            {
                "path": "artifacts/noop-result.json",
                "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
                "size": artifact.stat().st_size,
            }
        ],
    }
    assert manifest_path.stat().st_size < 512
    assert artifact.stat().st_size < 256


def test_noop_optional_delay_is_bounded_and_argument_checked(tmp_path: Path) -> None:
    environment = os.environ.copy()
    environment.update(
        RUNPOD_JOBRUNNER_RUN_ID="run-container-contract-002",
        RUNPOD_JOBRUNNER_STORAGE_MOUNT=str(tmp_path),
        RUNPOD_JOBRUNNER_RUN_ROOT=str(
            tmp_path / "runpod-jobrunner/runs/run-container-contract-002"
        ),
    )
    started = time.monotonic()
    completed = subprocess.run(
        [str(CONTAINER / "noop.py"), "preflight", "--sleep-seconds", "1"],
        capture_output=True,
        text=True,
        env=environment,
    )

    assert completed.returncode == 0
    assert time.monotonic() - started >= 1

    rejected = subprocess.run(
        [str(CONTAINER / "noop.py"), "preflight", "--sleep-seconds", "301"],
        capture_output=True,
        text=True,
        env=environment,
    )
    assert rejected.returncode == 64


def test_publish_workflow_and_script_never_use_floating_action_or_image_tags() -> None:
    workflow = (ROOT / ".github" / "workflows" / "container.yml").read_text()
    publish_script = (CONTAINER / "build-publish.sh").read_text()

    action_refs = re.findall(r"uses:\s*[^@\s]+@([^\s]+)", workflow)
    assert action_refs
    assert all(re.fullmatch(r"[0-9a-f]{40}", ref) for ref in action_refs)
    assert "docker buildx create" in workflow
    assert "--driver docker-container" in workflow
    assert ":latest" not in workflow
    assert ":latest" not in publish_script
    assert "docker buildx build" in publish_script
    assert "docker buildx imagetools inspect" in publish_script
    assert "git status --porcelain=v1 --untracked-files=all" in publish_script
    assert '--build-arg "SOURCE_REVISION=${source_revision}"' in publish_script
    assert '--build-arg "RUNNER_VERSION=${version}"' in publish_script
    assert "--require-hashes -r container/requirements-ci.lock" in workflow
    assert "uv export --frozen" in workflow
    dockerfile = (CONTAINER / "Dockerfile").read_text()
    assert "--require-hashes -r requirements-runtime.lock" in dockerfile
    assert "--no-deps --no-build-isolation ." in dockerfile


def _publish_script_repository(tmp_path: Path) -> Path:
    repository = tmp_path / "repository"
    (repository / "container").mkdir(parents=True)
    shutil.copy2(ROOT / "pyproject.toml", repository / "pyproject.toml")
    shutil.copy2(
        CONTAINER / "build-publish.sh",
        repository / "container" / "build-publish.sh",
    )
    subprocess.run(["git", "init", "-q", str(repository)], check=True)
    subprocess.run(
        ["git", "-C", str(repository), "config", "user.email", "test@example.invalid"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repository), "config", "user.name", "Container Test"],
        check=True,
    )
    subprocess.run(["git", "-C", str(repository), "add", "."], check=True)
    subprocess.run(
        ["git", "-C", str(repository), "commit", "-qm", "fixture"],
        check=True,
    )
    return repository


def test_publish_script_rejects_dirty_source_and_version_mismatch(tmp_path: Path) -> None:
    repository = _publish_script_repository(tmp_path)
    script = repository / "container" / "build-publish.sh"

    mismatch = subprocess.run(
        [str(script), "example.invalid/runner", "9.9.9"],
        cwd=repository,
        capture_output=True,
        text=True,
    )
    assert mismatch.returncode == 65
    assert "differs from project version" in mismatch.stderr

    (repository / "uncommitted.txt").write_text("dirty\n")
    dirty = subprocess.run(
        [str(script), "example.invalid/runner", __version__],
        cwd=repository,
        capture_output=True,
        text=True,
    )
    assert dirty.returncode == 65
    assert "dirty build tree" in dirty.stderr
