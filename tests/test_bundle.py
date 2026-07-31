from __future__ import annotations

import hashlib
import json
from dataclasses import FrozenInstanceError
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml

from runpod_jobrunner.bundle import BundleValidationError, check_bundle, compute_bundle_hash

PHASE_NAMES = ("verify", "preflight", "train", "evaluate", "package")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _write_bundle(root: Path, *, image: str | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    payload = b"hello from the no-op bundle\n"
    (inputs / "message.txt").write_bytes(payload)

    manifest: dict[str, Any] = {
        "protocol": "input-manifest/1",
        "root": "inputs",
        "files": [
            {
                "path": "message.txt",
                "sha256": _sha256(payload),
                "size": len(payload),
            }
        ],
    }
    spec: dict[str, Any] = {
        "protocol": "job-spec/1",
        "name": "noop",
        "image": image or "ghcr.io/halbritt/runpod-jobrunner-noop@sha256:" + "a" * 64,
        "runner": {"version": "0.1.0"},
        "inputs": {"manifest": "input-manifest.json"},
        "phases": {
            name: {
                "enabled": name in {"verify", "package"},
                "argv": ["/opt/runpod-jobrunner/noop", name],
                "timeout_seconds": 60,
            }
            for name in PHASE_NAMES
        },
        "limits": {
            "max_elapsed_seconds": 600,
            "max_cost_usd": "0.50",
            "usd_per_hour": "0.24",
        },
        "heartbeat_interval_seconds": 5,
        "termination_grace_seconds": 10,
        "resources": {
            "gpu_types": ["NVIDIA GeForce RTX 2000 Ada Generation"],
            "gpu_count": 1,
            "storage": {"encrypted": True, "mount": "/workspace", "required_gb": 10},
        },
        "artifacts": {"manifest_path": "artifacts/manifest.json"},
        "lifecycle": {"delete_after_terminal": True},
    }
    spec["bundle_hash"] = compute_bundle_hash(spec, manifest)
    (root / "job.yaml").write_text(yaml.safe_dump(spec, sort_keys=False), encoding="utf-8")
    (root / "input-manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    return spec, manifest


def _rewrite_manifest(root: Path, manifest: dict[str, Any]) -> None:
    (root / "input-manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )


def _rewrite_spec(root: Path, spec: dict[str, Any], manifest: dict[str, Any]) -> None:
    spec["bundle_hash"] = compute_bundle_hash(spec, manifest)
    (root / "job.yaml").write_text(yaml.safe_dump(spec, sort_keys=False), encoding="utf-8")


def test_check_returns_immutable_normalized_bundle(tmp_path: Path) -> None:
    _write_bundle(tmp_path)

    bundle = check_bundle(tmp_path)

    assert bundle.protocol == "job-spec/1"
    assert bundle.name == "noop"
    assert bundle.image_digest.endswith("@sha256:" + "a" * 64)
    assert bundle.max_cost_usd == Decimal("0.50")
    assert bundle.usd_per_hour == Decimal("0.24")
    assert tuple(phase.name for phase in bundle.phases) == PHASE_NAMES
    assert bundle.phases[0].argv == ("/opt/runpod-jobrunner/noop", "verify")
    assert bundle.inputs[0].relative_path == "message.txt"

    with pytest.raises(FrozenInstanceError):
        bundle.name = "changed"  # type: ignore[misc]

    request = bundle.to_run_request("run-01K3Y2GB5CJ1FV3NCB7KQ9Z8ZX")
    assert request == {
        "protocol": "run-request/1",
        "run_id": "run-01K3Y2GB5CJ1FV3NCB7KQ9Z8ZX",
        "bundle_hash": bundle.bundle_hash,
        "image_digest": bundle.image_digest,
        "runner_version": "0.1.0",
        "phases": {
            name: {
                "enabled": name in {"verify", "package"},
                "argv": ["/opt/runpod-jobrunner/noop", name],
                "timeout_seconds": 60,
            }
            for name in PHASE_NAMES
        },
        "limits": {
            "max_elapsed_seconds": 600,
            "max_cost_usd": "0.50",
            "usd_per_hour": "0.24",
        },
        "heartbeat_interval_seconds": 5,
        "termination_grace_seconds": 10,
        "storage": {"encrypted": True, "mount": "/workspace", "required_gb": 10},
        "artifact_manifest_path": "artifacts/manifest.json",
    }


def test_tag_only_image_is_rejected(tmp_path: Path) -> None:
    _write_bundle(tmp_path, image="ghcr.io/halbritt/runpod-jobrunner-noop:latest")

    with pytest.raises(BundleValidationError, match="image"):
        check_bundle(tmp_path)


def test_phase_mapping_is_exact_and_argv_is_an_array(tmp_path: Path) -> None:
    spec, manifest = _write_bundle(tmp_path)
    del spec["phases"]["evaluate"]
    spec["phases"]["other"] = {
        "enabled": True,
        "argv": "echo unsafe shell",
        "timeout_seconds": 1,
    }
    _rewrite_spec(tmp_path, spec, manifest)

    with pytest.raises(BundleValidationError, match="phases"):
        check_bundle(tmp_path)


def test_missing_declared_input_is_rejected(tmp_path: Path) -> None:
    _write_bundle(tmp_path)
    (tmp_path / "inputs" / "message.txt").unlink()

    with pytest.raises(BundleValidationError, match="missing"):
        check_bundle(tmp_path)


@pytest.mark.parametrize("mutation", [b"same byte length but changed!\n", b"short\n"])
def test_mutated_declared_input_is_rejected(tmp_path: Path, mutation: bytes) -> None:
    _write_bundle(tmp_path)
    (tmp_path / "inputs" / "message.txt").write_bytes(mutation)

    with pytest.raises(BundleValidationError, match=r"(size|sha256)"):
        check_bundle(tmp_path)


def test_undeclared_input_is_rejected(tmp_path: Path) -> None:
    _write_bundle(tmp_path)
    (tmp_path / "inputs" / "surprise.txt").write_text("not allow-listed\n", encoding="utf-8")

    with pytest.raises(BundleValidationError, match="undeclared"):
        check_bundle(tmp_path)


def test_symlinked_input_is_rejected(tmp_path: Path) -> None:
    spec, manifest = _write_bundle(tmp_path)
    outside = tmp_path / "outside.txt"
    outside.write_text("outside\n", encoding="utf-8")
    source = tmp_path / "inputs" / "message.txt"
    source.unlink()
    source.symlink_to(outside)
    manifest["files"][0].update(sha256=_sha256(b"outside\n"), size=len(b"outside\n"))
    _rewrite_manifest(tmp_path, manifest)
    _rewrite_spec(tmp_path, spec, manifest)

    with pytest.raises(BundleValidationError, match="symlink"):
        check_bundle(tmp_path)


def test_manifest_path_escape_is_rejected(tmp_path: Path) -> None:
    spec, manifest = _write_bundle(tmp_path)
    manifest["files"][0]["path"] = "../job.yaml"
    _rewrite_manifest(tmp_path, manifest)
    _rewrite_spec(tmp_path, spec, manifest)

    with pytest.raises(BundleValidationError, match="path"):
        check_bundle(tmp_path)


def test_bundle_hash_detects_job_spec_mutation(tmp_path: Path) -> None:
    _write_bundle(tmp_path)
    job_path = tmp_path / "job.yaml"
    job_path.write_text(
        job_path.read_text(encoding="utf-8").replace(
            "max_elapsed_seconds: 600", "max_elapsed_seconds: 601"
        ),
        encoding="utf-8",
    )

    with pytest.raises(BundleValidationError, match="bundle_hash"):
        check_bundle(tmp_path)


def test_manifest_requires_exact_sha256_and_size(tmp_path: Path) -> None:
    spec, manifest = _write_bundle(tmp_path)
    manifest["files"][0]["sha256"] = "not-a-digest"
    _rewrite_manifest(tmp_path, manifest)
    _rewrite_spec(tmp_path, spec, manifest)

    with pytest.raises(BundleValidationError, match="sha256"):
        check_bundle(tmp_path)
