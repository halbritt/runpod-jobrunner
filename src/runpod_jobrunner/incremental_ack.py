"""Controller-authenticated acknowledgements for durably mirrored artifacts."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from subprocess import CompletedProcess
from typing import cast

ACK_PROTOCOL = "incremental-mirror-ack/1"
ACK_NAMESPACE = "runpod-jobrunner-incremental-mirror"
ACK_KEY_DIRECTORY = "incremental-mirror-ack-key"


class IncrementalAckError(RuntimeError):
    """An acknowledgement key or signature is unavailable or invalid."""


RunCommand = Callable[..., CompletedProcess[bytes]]


@dataclass(frozen=True, slots=True)
class AckSigner:
    private_key: Path
    public_key: str
    key_id: str
    identity: str
    namespace: str = ACK_NAMESPACE

    def public_fields(self) -> dict[str, str]:
        return {
            "algorithm": "ssh-ed25519",
            "identity": self.identity,
            "key_id": self.key_id,
            "namespace": self.namespace,
            "public_key": self.public_key,
        }


def ensure_ack_signer(
    run_dir: Path,
    run_id: str,
    *,
    run_command: RunCommand = subprocess.run,
) -> AckSigner:
    """Create or recover one run-scoped Ed25519 signing key before provisioning."""

    if not run_id or any(character.isspace() for character in run_id):
        raise IncrementalAckError("acknowledgement signer needs a stable run ID")
    secrets_root = run_dir / "secrets"
    secrets_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    _require_private_directory(secrets_root)
    key_directory = secrets_root / ACK_KEY_DIRECTORY
    if not key_directory.exists():
        staging = secrets_root / f".{ACK_KEY_DIRECTORY}.{uuid.uuid4().hex}.tmp"
        staging.mkdir(mode=0o700)
        private_key = staging / "key"
        try:
            try:
                result = run_command(
                    [
                        "ssh-keygen",
                        "-q",
                        "-t",
                        "ed25519",
                        "-N",
                        "",
                        "-C",
                        "",
                        "-f",
                        str(private_key),
                    ],
                    check=False,
                    capture_output=True,
                    text=False,
                )
            except OSError as error:
                raise IncrementalAckError(
                    "ssh-keygen is required to generate the acknowledgement key"
                ) from error
            if result.returncode != 0:
                raise IncrementalAckError("could not generate the run-scoped Ed25519 key")
            _fsync_regular(private_key, expected_mode=0o600)
            _fsync_regular(private_key.with_suffix(".pub"))
            _fsync_directory(staging)
            try:
                os.rename(staging, key_directory)
            except FileExistsError:
                shutil.rmtree(staging)
            _fsync_directory(secrets_root)
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise
    return load_ack_signer(run_dir, run_id)


def load_ack_signer(run_dir: Path, run_id: str) -> AckSigner:
    """Load the original run-scoped signer; never replace a missing key."""

    key_directory = run_dir / "secrets" / ACK_KEY_DIRECTORY
    _require_private_directory(key_directory)
    private_key = key_directory / "key"
    public_path = key_directory / "key.pub"
    _require_regular(private_key, expected_mode=0o600)
    _require_regular(public_path)
    try:
        public_key = _normalized_public_key(public_path.read_text(encoding="ascii"))
    except (OSError, UnicodeDecodeError) as error:
        raise IncrementalAckError("acknowledgement public key is unreadable") from error
    return AckSigner(
        private_key=private_key,
        public_key=public_key,
        key_id=_public_key_id(public_key),
        identity=f"runpod-jobrunner:{run_id}",
    )


def sign_ack(
    unsigned: Mapping[str, object],
    signer: AckSigner,
    *,
    run_command: RunCommand = subprocess.run,
) -> bytes:
    """Sign one canonical acknowledgement and return canonical JSON bytes."""

    record = dict(unsigned)
    if record.get("protocol") != ACK_PROTOCOL or "signature" in record:
        raise IncrementalAckError("acknowledgement statement is malformed")
    expected_signer = signer.public_fields()
    if record.get("signer") != expected_signer:
        raise IncrementalAckError("acknowledgement signer fields do not match the local key")
    statement = _canonical(record)
    with tempfile.TemporaryDirectory(prefix="runpod-jobrunner-ack-sign-") as directory_text:
        directory = Path(directory_text)
        statement_path = directory / "statement.json"
        statement_path.write_bytes(statement)
        try:
            result = run_command(
                [
                    "ssh-keygen",
                    "-Y",
                    "sign",
                    "-f",
                    str(signer.private_key),
                    "-n",
                    ACK_NAMESPACE,
                    str(statement_path),
                ],
                check=False,
                capture_output=True,
                text=False,
            )
        except OSError as error:
            raise IncrementalAckError(
                "ssh-keygen is required to sign the acknowledgement"
            ) from error
        signature_path = statement_path.with_suffix(".json.sig")
        if result.returncode != 0 or not signature_path.is_file():
            raise IncrementalAckError("could not sign the incremental mirror acknowledgement")
        signature = signature_path.read_bytes()
    signed = {
        **record,
        "signature": base64.b64encode(signature).decode("ascii"),
    }
    return _canonical(signed) + b"\n"


def verify_ack(
    encoded: bytes,
    *,
    expected: Mapping[str, object],
    signer: Mapping[str, object],
    run_command: RunCommand = subprocess.run,
) -> dict[str, object]:
    """Verify the signature and exact authority bindings of an acknowledgement."""

    try:
        candidate: object = json.loads(encoded, object_pairs_hook=_object_without_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise IncrementalAckError("acknowledgement is not valid JSON") from error
    if not isinstance(candidate, Mapping):
        raise IncrementalAckError("acknowledgement must be an object")
    record = dict(cast(Mapping[str, object], candidate))
    signature_value = record.pop("signature", None)
    if not isinstance(signature_value, str):
        raise IncrementalAckError("acknowledgement signature is absent")
    if record != dict(expected):
        raise IncrementalAckError("acknowledgement authority binding mismatch")
    public_key, identity = _validated_signer_fields(signer)
    if record.get("signer") != dict(signer):
        raise IncrementalAckError("acknowledgement signer binding mismatch")
    try:
        signature = base64.b64decode(signature_value, validate=True)
    except ValueError as error:
        raise IncrementalAckError("acknowledgement signature encoding is invalid") from error
    with tempfile.TemporaryDirectory(prefix="runpod-jobrunner-ack-verify-") as directory_text:
        directory = Path(directory_text)
        allowed_signers = directory / "allowed_signers"
        signature_path = directory / "signature"
        allowed_signers.write_text(f"{identity} {public_key}\n", encoding="ascii")
        signature_path.write_bytes(signature)
        try:
            result = run_command(
                [
                    "ssh-keygen",
                    "-Y",
                    "verify",
                    "-f",
                    str(allowed_signers),
                    "-I",
                    identity,
                    "-n",
                    ACK_NAMESPACE,
                    "-s",
                    str(signature_path),
                ],
                input=_canonical(record),
                check=False,
                capture_output=True,
                text=False,
            )
        except OSError as error:
            raise IncrementalAckError(
                "ssh-keygen is required to verify the acknowledgement"
            ) from error
    if result.returncode != 0:
        raise IncrementalAckError("acknowledgement signature verification failed")
    return record


def receipt_key(manifest_path: str) -> str:
    return hashlib.sha256(manifest_path.encode("utf-8")).hexdigest()


def _validated_signer_fields(signer: Mapping[str, object]) -> tuple[str, str]:
    if signer.get("algorithm") != "ssh-ed25519" or signer.get("namespace") != ACK_NAMESPACE:
        raise IncrementalAckError("acknowledgement signer algorithm is unsupported")
    public_value = signer.get("public_key")
    identity_value = signer.get("identity")
    key_id = signer.get("key_id")
    if not isinstance(public_value, str) or not isinstance(identity_value, str):
        raise IncrementalAckError("acknowledgement signer identity is malformed")
    public_key = _normalized_public_key(public_value)
    if key_id != _public_key_id(public_key):
        raise IncrementalAckError("acknowledgement signer key ID mismatch")
    return public_key, identity_value


def _normalized_public_key(value: str) -> str:
    fields = value.strip().split()
    if len(fields) < 2 or fields[0] != "ssh-ed25519":
        raise IncrementalAckError("acknowledgement public key must be Ed25519")
    try:
        base64.b64decode(fields[1], validate=True)
    except ValueError as error:
        raise IncrementalAckError("acknowledgement public key is malformed") from error
    return f"ssh-ed25519 {fields[1]}"


def _public_key_id(public_key: str) -> str:
    encoded = public_key.split()[1]
    key_blob = base64.b64decode(encoded, validate=True)
    digest = base64.b64encode(hashlib.sha256(key_blob).digest()).decode("ascii").rstrip("=")
    return f"SHA256:{digest}"


def _canonical(value: Mapping[str, object]) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise IncrementalAckError("acknowledgement is not canonically encodable") from error


def _object_without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise IncrementalAckError(f"acknowledgement contains duplicate key {key!r}")
        result[key] = value
    return result


def _require_private_directory(path: Path) -> None:
    if path.is_symlink() or not path.is_dir() or path.stat().st_mode & 0o077:
        raise IncrementalAckError(f"acknowledgement key directory is not private: {path}")


def _require_regular(path: Path, *, expected_mode: int | None = None) -> None:
    if path.is_symlink() or not path.is_file():
        raise IncrementalAckError(f"acknowledgement key file is unavailable: {path}")
    if expected_mode is not None and path.stat().st_mode & 0o777 != expected_mode:
        raise IncrementalAckError(f"acknowledgement private key mode is unsafe: {path}")


def _fsync_regular(path: Path, *, expected_mode: int | None = None) -> None:
    _require_regular(path, expected_mode=expected_mode)
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
