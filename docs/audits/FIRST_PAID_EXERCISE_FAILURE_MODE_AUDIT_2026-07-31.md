# First paid exercise failure-mode audit

Date: 2026-07-31 UTC  
Scope: first no-private-data RunPod vertical slice  
Verdict: admitted only after the blocking rows below are verified by tests

| Failure path | Initial severity | Policy and evidence |
| --- | --- | --- |
| Stop after an unknown create strands a late pod | Blocker | Deletion keeps reconciling the stable provision operation. Without an authoritative `not_created` receipt, it cannot accept absence until `terminateAfter` has elapsed and a fresh five-second absence window passes. A late match is adopted and deleted; the deadline proof is part of closeout. Covered by the hidden-resource, no-effect, restart, and malformed-deadline lifecycle tests. |
| A create response is lost or malformed and a second paid pod is created | Blocker | Dispatch intent is durable and fenced before the request. A 5xx, empty body, invalid JSON, or protocol-invalid response is `ProviderOutcomeUnknown`; reconciliation by the stable run identity must resolve it before any further create. Delayed list visibility remains unknown rather than becoming permission to retry. |
| Encryption, rate, or image rejection is retried as another create | Blocker | The adapter deletes immediately and returns `ProviderRejected`; reconciliation persists the rejected resource ID and enters deletion. The admitted rate, expected image, stable name, and run identity are durable operation policy. |
| Several paid runs each consume the full principal authority | Blocker | `budget-ledger/1` reserves full run caps under one aggregate authority. Reservations are conservative and are not refunded merely because a lifecycle closes. |
| Missing provider environment causes duplicate create | Serious | Reconciliation uses the exact durable resource name as well as the operation environment. The production adapter preserves or elects one immutable primary ID while returning every duplicate. The lifecycle quarantines the set and deletes every resource, including a surviving non-primary pod, before not-found and spend-zero closeout. |
| Wrong worker image or runner receives private inputs | Blocker | Provider image evidence must equal the admitted digest. Bootstrap contains only the request and status token. The controller requires authenticated `ready` status with the exact version, source commit, and protocol offer before uploading input or publishing the independent launch token. A mismatch produces zero input bytes and zero phase starts. |
| Controller restart replays launch or duplicates work | Blocker | Input and launch publications have durable run-bound receipts. The worker verifies the hash-pinned random launch token and persists authorization before phase execution. Waiting, running, and terminal restart paths do not start a second workload. |
| Artifact copy succeeds but bytes are wrong | Blocker | The runner hashes a versioned manifest and every declared file. The controller first binds the downloaded manifest to the terminal hash, then downloads only its file list and rehashes every receipt before deletion. |
| SSH failure is mistaken for process exit | Serious | SSH is used only for transfer. Process truth comes from the run-token-protected HTTP status projection. A stale authenticated heartbeat is an explicit workload failure; an SSH error alone remains unknown and is retried. |
| Permanent authenticated-status failure causes a systemd restart loop | Blocker | Authentication rejection and structurally invalid authenticated status become durable failed workload observations. The supervisor proceeds through recovery and deletion; it does not depend on process restart to reinterpret the same response. |
| A torn or corrupt event journal wedges every restart | Serious | Recovery may remove only an incomplete final JSONL fragment. A complete malformed record, wrong run identity, duplicate sequence, or invalid event remains a hard error and cannot be silently rewritten. |
| A release silently resolves different dependencies | Serious | Runtime and CI installs use committed hash-locked exports. Release CI regenerates and compares those exports, then runs lint, strict type checking, the full test suite, a wheel build, and the container contract before publishing. |
| Controller or provider control plane is unreachable | Residual | The user-systemd supervisor restarts without a burst limit, the remote process has its own elapsed/cost caps, and RunPod receives an independent `terminateAfter`. A simultaneous local-host and provider-control-plane outage can still delay deletion until the provider deadline. |

The remote runner receives no RunPod management credential. Phase children use an
environment allow-list; controller-style tokens and API keys are not inherited.
The first paid exercise must contain no private SFT data, use the cheapest admitted
Secure Cloud GPU, reserve its full cap in the shared budget ledger, and finish with
artifact verification, delete acknowledgement, provider not-found, and zero current
spend. Its public image must also pass an anonymous registry read before provider
creation. Provider creation starts the billable elapsed interval; image download is
inside that interval.
