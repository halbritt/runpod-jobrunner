# runpod-jobrunner

`runpod-jobrunner` runs one bounded, recoverable job on RunPod. It records intent
before provider effects, executes a fixed five-phase remote protocol, verifies
declared artifacts, and deletes the worker before declaring the lifecycle closed.

The public interface is deliberately small:

```text
runpod-jobrunner check JOB_BUNDLE
runpod-jobrunner run JOB_BUNDLE --approve-max-usd USD
runpod-jobrunner status RUN_ID [--follow]
runpod-jobrunner stop RUN_ID
runpod-jobrunner recover RUN_ID
```

Several runs under one grant can reserve their full caps atomically:

```text
runpod-jobrunner run JOB_BUNDLE --approve-max-usd 47 \
  --budget-scope striatum-2026-07-31 --budget-total-usd 50
```

The first release is intentionally model-agnostic. See `docs/adr/` for the
lifecycle contract and `docs/security-and-operations.md` for the failure and
credential boundaries.

Worker launch has a two-step authorization barrier. The controller first sends
only `request.json` and a run-scoped status token. The worker publishes its exact
version, source commit, and protocol capabilities over the authenticated status
channel. Only after those fields match the durable request does the controller
upload the allow-listed job inputs and a separate random launch token. Identity
failure therefore reaches deletion without disclosing job input bytes or starting
a phase. Durable publication receipts make controller restarts idempotent.

Long-running jobs can opt into incremental, content-verified artifact recovery:

```yaml
artifacts:
  manifest_path: artifacts/manifest.json
  incremental_manifest_glob: checkpoints/checkpoint-*/checkpoint-complete.json
  incremental_mirror_ack:
    required: true
    directory: control/incremental-acks
    timeout_seconds: 900
```

Each matching completion manifest declares files relative to its own directory
with exact `path`, `size`, and `sha256` fields. The controller mirrors verified
checkpoints while the authenticated remote status still reports the workload as
running. Discovery is confined to the glob's fixed prefix, links are skipped, and
files are verified in private staging before atomic publication. Permanent
contract failures fail the workload and still preserve any checkpoints already
verified for recovery.

The optional acknowledgement contract adds hard backpressure. Before
provisioning, the controller creates a run-scoped Ed25519 key and sends only its
public fields to the worker. After a checkpoint is mirrored and verified, the
controller publishes a signed `incremental-mirror-ack/1` record into the selected
run directory. A participating save callback must verify that run, bundle, image,
manifest, file inventory, signer, and namespace binding before it returns.

Published runner images are built only from a clean committed tree. Runtime and
CI dependencies are installed from hash-locked exports, the image carries a
release receipt bound to its semantic version and Git commit, and release CI runs
the full test, lint, type, wheel, and container-contract gates before publication.
