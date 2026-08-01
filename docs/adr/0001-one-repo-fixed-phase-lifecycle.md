# ADR 0001: One repository and one recoverable fixed-phase lifecycle

Status: Accepted

Date: 2026-07-31

## Context

The previous training workflow separated worker creation from workload startup.
That gap allowed an H100 to remain billable while no useful work ran. Terminal
polling and SSH observations also could not prove remote process state or safe
resource deletion.

The first consumer is a private PEFT tuning job. The lifecycle tool must remain
model-agnostic and must never expand the consumer's explicit file allow-list.
The controller and remote runner share a wire protocol and are released together.

## Decision

Build `runpod-jobrunner` as one private GitHub repository owned by `halbritt`.
Expose only:

```text
check JOB_BUNDLE
run JOB_BUNDLE --approve-max-usd USD
status RUN_ID [--follow]
stop RUN_ID
recover RUN_ID
```

`run` durably records the request before it starts a transient user-systemd
supervisor. That supervisor owns one lifecycle:

```text
planned -> provisioning -> starting -> running -> recovering -> deleting -> closed
```

Workload outcome is a separate axis. The runner executes the fixed protocol
`verify -> preflight -> train -> evaluate -> package`; a phase may be disabled,
but the bundle cannot define an arbitrary graph.

Hide RunPod behind an internal provider seam and transfer behind an internal
transfer seam. Those seams are earned by the production and in-memory/local
adapters used by the same application behavior. Each external effect has a
stable operation ID and durable intent. An unknown outcome is reconciled before
retry.

The local controller owns lifecycle closure. The remote runner owns workload
process truth and publishes a token-protected read-only status record independent
of SSH. The runner has no provider-management credential.

Private inputs and working artifacts require encrypted pod storage. A job whose
data policy permits unencrypted storage may instead name one existing RunPod
network volume. The controller must attach that exact provider ID, must not
request a pod volume disk, and must reject an absent or different attachment
during creation and reconciliation. The upload contains only files declared by
an exact size-and-SHA-256 manifest. A lifecycle is closed only after artifact
disposition, Pod delete acknowledgement, provider not-found, and zero-current-spend
observations are recorded. An existing network volume remains outside the run's
deletion lifecycle.

Artifact declarations are namespaced to
`<mount>/runpod-jobrunner/runs/<run_id>`. Terminal manifest paths, terminal file
paths, and incremental discovery globs cannot resolve from the shared mount or a
sibling run. Signed incremental acknowledgements keep their established
storage-mount-relative manifest binding, including the run namespace prefix.

For a known Pod ID, closeout uses an exact-resource spend observation rather than
RunPod's account-wide spend total. This keeps retained network-volume charges and
unrelated workloads outside the run's teardown proof. Account scope remains the
conservative fallback when no Pod ID was established, except that a network-volume
create may use provision-operation scope after its provider deadline and bounded
absence proof are complete. The selected scope is durable receipt evidence.

## Consequences

- A caller cannot create an idle worker through the normal interface.
- Controller and runner compatibility can be tested and released atomically.
- New training jobs normally require only a bundle and pinned image.
- Provider and transfer failures remain explicit, inspectable states rather than
  being converted into success or silently routed to fakes.
- Local evidence uses append-only events plus atomic projections; systemd unit
  state is supervision evidence, not run truth.
- Public API growth is deferred until a second workload proves a missing need.

## Residual risk

A local systemd supervisor survives shells and agent sessions, but it cannot
delete a worker while this host or the RunPod control plane is unreachable. The
provider-side termination deadline and the runner's elapsed/cost self-cap bound
compute, but a control-plane outage may leave storage billing until RunPod's
deadline acts. The first release records this limitation rather than giving the
remote runner broad provider credentials.

## Revisit when

- a second provider is accepted;
- controller and runner require independent ownership or release cadence;
- the first two real training jobs cannot fit the fixed phase protocol;
- RunPod operations cannot be reconciled by stable run identity;
- authenticated status or verified deletion cannot be established.
