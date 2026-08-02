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

Starting with 0.1.7, a job declares exactly one storage mode. Encrypted pod
storage keeps the original contract:

```yaml
resources:
  storage:
    encrypted: true
    mount: /workspace
    required_gb: 120
```

Non-sensitive jobs may attach one existing RunPod network volume:

```yaml
resources:
  storage:
    encrypted: false
    network_volume_id: YOUR_RUNPOD_VOLUME_ID
    mount: /workspace
    required_gb: 120
```

The second form sends `networkVolumeId` at Pod creation and does not request a
pod volume disk or create a volume key. Creation and every later provider
observation must report the same attached volume ID. An absent or different ID
fails closed and sends the Pod through deletion. The existing network volume is
not deleted by job closeout. `required_gb` is the free-space requirement checked
at the mount before any phase starts.

Closeout spend proof is scoped to the exact created Pod when its provider ID is
known. The controller observes that Pod's hourly rate while it exists and records
zero only after an exact-ID lookup reports it absent. Retained network-volume
charges and unrelated account workloads therefore do not prevent that run from
closing. If a network-volume create has no known Pod ID, the controller can record
operation-scoped zero compute only after the provider deadline and fresh absence
window prove that operation created no surviving Pod. The receipt records whether
zero spend used resource, provision-operation, or conservative account scope.
A stop before provider creation uses a run- or provision-operation-scoped
`provision_not_dispatched` proof and is also independent of the retained-volume
baseline.

Worker launch has a two-step authorization barrier. The controller first sends
only `request.json` and a run-scoped status token. The worker publishes its exact
version, source commit, and protocol capabilities over the authenticated status
channel. Only after those fields match the durable request does the controller
upload the allow-listed job inputs and a separate random launch token. Identity
failure therefore reaches deletion without disclosing job input bytes or starting
a phase. Durable publication receipts make controller restarts idempotent.

Starting with 0.1.8, the controller also bounds the interval from provider
creation to the first authenticated worker status by the launch-authorization
timeout. A broken entrypoint or restart loop therefore fails into lifecycle
cleanup instead of consuming the entire workload deadline before any phase starts.

Starting with 0.1.9, each phase writes combined stdout and stderr to a durable
run-scoped diagnostic log. A failed run creates a hash-verified fallback artifact
manifest when the job package phase did not produce one. Authenticated immutable
terminal status remains recoverable during its retention window even after the
live heartbeat becomes stale; heartbeat staleness still fails ready and running
workers.

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

The terminal `manifest_path`, every path in that manifest, and the incremental
glob are relative to the run-owned root
`<mount>/runpod-jobrunner/runs/<run_id>`. They are never resolved from the shared
storage mount, so files left by another run cannot satisfy the current run's
artifact contract. Incremental manifest file entries remain relative to the
completion manifest's directory.

New requests pin this interpretation with `artifact_path_base: run-root`, and a
0.1.7 runner refuses to start without it. Controller recovery treats the field's
absence as the legacy 0.1.6 storage-mount base, so an already durable older run is
not reinterpreted during takeover.

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
controller atomically publishes a signed `incremental-mirror-ack/1` record into
the selected run directory. A participating save callback must verify that run, bundle, image,
manifest, file inventory, signer, and namespace binding before it returns.
The acknowledgement's `manifest_path` remains relative to the storage mount, for
example `runpod-jobrunner/runs/<run_id>/checkpoints/checkpoint-25/checkpoint-complete.json`.

Published runner images are built only from a clean committed tree. Runtime and
CI dependencies are installed from hash-locked exports, the image carries a
release receipt bound to its semantic version and Git commit, and release CI runs
the full test, lint, type, wheel, and container-contract gates before publication.
