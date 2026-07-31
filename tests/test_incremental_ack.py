from __future__ import annotations

import json
from pathlib import Path

import pytest

from runpod_jobrunner.incremental_ack import (
    ACK_PROTOCOL,
    IncrementalAckError,
    ensure_ack_signer,
    load_ack_signer,
    sign_ack,
    verify_ack,
)


def _unsigned(signer_fields: dict[str, str]) -> dict[str, object]:
    return {
        "protocol": ACK_PROTOCOL,
        "run_id": "run-ack-test",
        "bundle_hash": "a" * 64,
        "image_digest": "example.invalid/image@sha256:" + "b" * 64,
        "manifest_path": "checkpoints/checkpoint-25/checkpoint-complete.json",
        "manifest_size": 321,
        "manifest_sha256": "c" * 64,
        "file_count": 4,
        "file_bytes": 1234,
        "local_receipt_sha256": "d" * 64,
        "signer": signer_fields,
    }


def test_run_scoped_signer_is_private_stable_and_verifiable(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    signer = ensure_ack_signer(run_dir, "run-ack-test")

    assert signer.private_key.stat().st_mode & 0o777 == 0o600
    assert signer.private_key.parent.stat().st_mode & 0o777 == 0o700
    assert load_ack_signer(run_dir, "run-ack-test") == signer
    assert ensure_ack_signer(run_dir, "run-ack-test") == signer

    expected = _unsigned(signer.public_fields())
    encoded = sign_ack(expected, signer)
    assert verify_ack(
        encoded,
        expected=expected,
        signer=signer.public_fields(),
    ) == expected
    assert str(signer.private_key) not in encoded.decode()


def test_ack_verification_rejects_tampering_and_missing_original_key(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "run"
    signer = ensure_ack_signer(run_dir, "run-ack-test")
    expected = _unsigned(signer.public_fields())
    encoded = sign_ack(expected, signer)
    tampered = json.loads(encoded)
    tampered["manifest_sha256"] = "e" * 64

    with pytest.raises(IncrementalAckError, match="authority binding mismatch"):
        verify_ack(
            json.dumps(tampered).encode(),
            expected=expected,
            signer=signer.public_fields(),
        )

    signer.private_key.unlink()
    with pytest.raises(IncrementalAckError, match="key file is unavailable"):
        load_ack_signer(run_dir, "run-ack-test")

