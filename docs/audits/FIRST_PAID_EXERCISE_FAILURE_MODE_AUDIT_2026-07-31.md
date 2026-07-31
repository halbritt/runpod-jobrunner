# First paid exercise failure-mode audit

Date: 2026-07-31 UTC  
Scope: first no-private-data RunPod vertical slice  
Verdict: admitted only after the blocking rows below are verified by tests

| Failure path | Initial severity | Policy and evidence |
| --- | --- | --- |
| Stop after an unknown create strands a late pod | Blocker | Deletion first reconciles the stable provision operation, adopts a matching resource, and requires delayed absence evidence before accepting that no resource exists. Covered by `test_stop_after_unknown_create_adopts_and_deletes_late_resource`. |
| Encryption, rate, or image rejection is retried as another create | Blocker | The adapter deletes immediately and returns `ProviderRejected`; reconciliation persists the rejected resource ID and enters deletion. The admitted rate, expected image, stable name, and run identity are durable operation policy. |
| Several paid runs each consume the full principal authority | Blocker | `budget-ledger/1` reserves full run caps under one aggregate authority. Reservations are conservative and are not refunded merely because a lifecycle closes. |
| Missing provider environment causes duplicate create | Serious | Reconciliation uses the exact durable resource name as well as the operation environment. Duplicate matches are quarantined as a set and every resource is deleted before not-found and spend-zero closeout. |
| Wrong worker image receives private inputs | Serious | Provider image evidence must exactly equal the admitted digest before input transfer. Missing evidence is rejection, not a warning. |
| Artifact copy succeeds but bytes are wrong | Blocker | The runner hashes a versioned manifest and every declared file. The controller first binds the downloaded manifest to the terminal hash, then downloads only its file list and rehashes every receipt before deletion. |
| SSH failure is mistaken for process exit | Serious | SSH is used only for transfer. Process truth comes from the run-token-protected HTTP status projection. A stale authenticated heartbeat is an explicit workload failure; an SSH error alone remains unknown and is retried. |
| Controller or provider control plane is unreachable | Residual | The user-systemd supervisor restarts without a burst limit, the remote process has its own elapsed/cost caps, and RunPod receives an independent `terminateAfter`. A simultaneous local-host and provider-control-plane outage can still delay deletion until the provider deadline. |

The remote runner receives no RunPod management credential. Phase children use an
environment allow-list; controller-style tokens and API keys are not inherited.
The first paid exercise must contain no private SFT data, use the cheapest admitted
Secure Cloud GPU, reserve its full cap in the shared budget ledger, and finish with
artifact verification, delete acknowledgement, provider not-found, and zero current
spend.
