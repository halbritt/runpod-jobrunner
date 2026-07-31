# Security and operations boundary

The controller first sends a minimal versioned run request and a status token.
It sends no job input until authenticated status proves the exact runner version,
Git commit, image-bound request identity, and required protocol majors. It then
uploads only files named by the validated input manifest and publishes a separate
run-scoped launch token whose path, size, and hash are pinned in the request. The
worker cannot start a phase without that token. The controller must not copy a
repository, undisclosed files, credentials, or a parent directory. Symlinks are
rejected.

Provider credentials remain on the controller host. A remote runner receives a
random run-scoped status token and only the transfer authority it needs. The
status token and launch token are independently generated. Status
contains identifiers, phase, heartbeat, child state, sequence, and terminal
result; it contains no logs, paths, input contents, model contents, or secrets.

The launch barrier is restart-safe. Durable input and launch-publication receipts
prevent a controller restart from widening the input set or minting a second
authorization. The runner records authorization before phase execution; a restart
may resume a waiting, running, or terminal state, but it never treats token replay
as authority for a second workload.

RunPod's provider termination deadline is mandatory and independent of the local
supervisor. A stopped workload is not a deleted worker. Operators should use
`status` or `recover`; never infer closeout from an SSH failure or a systemd unit
exit.

Provider creation is an uncertainty boundary. A transport error, 5xx response,
empty body, or invalid response after dispatch has an unknown outcome. The
controller fences that operation and reconciles the stable run identity; it does
not issue another create merely because a list result is briefly empty. If no
operation-specific `not_created` receipt exists, an uncertain create cannot close
until the immutable `terminateAfter` deadline has elapsed and a new five-second
absence window has passed. Closeout records that deadline proof and still requires
zero current spend. A pod that becomes visible before then is adopted and deleted.
Duplicate matches preserve one durable primary identity but all remain visible to
the deletion loop. Permanent authenticated-status failures become a failed
workload observation followed by normal deletion instead of a supervisor restart
loop. A torn final JSONL fragment may be removed during recovery; a complete
malformed event remains a hard error.

When several paid runs share one principal grant, pass one budget scope and total
to every `run` command. The durable ledger reserves each job's full cap and does
not release it from lifecycle state alone:

```text
--budget-scope striatum-2026-07-31 --budget-total-usd 50
```

The first release uses trust on first use for each ephemeral pod SSH host key.
SSH is a transfer channel, not process truth. A correctly authenticated HTTP
status record remains authoritative when SSH is unavailable. A simultaneous
controller-host and RunPod control-plane outage can delay deletion until the
provider-side termination deadline; the worker never receives broad provider
credentials as a workaround.

Treat all time after provider creation as billable, including image download and
startup. An image pull is not excluded from elapsed or dollar admission merely
because no phase has started.

## Incremental artifact mirroring

A job may declare `artifacts.incremental_manifest_glob`, for example
`checkpoints/checkpoint-*/checkpoint-complete.json`. The glob is a normalized
relative path with bounded single-component `*` matching and a fixed completion
filename. It is retained in the controller request only; it is not sent to the
runner or returned by the status endpoint.

After each authenticated status observation, the controller uses the run-scoped
SFTP channel to discover regular completion manifests. Listing starts at the
glob's longest fixed directory prefix instead of the storage root, and the SFTP
backend is instructed to skip links. Each manifest must contain a non-empty
`files` array of normalized relative paths, exact sizes, and lowercase SHA-256
hashes. Declared files are confined to the completion manifest's directory and
mirrored below `receipts/incremental/`.

Manifest and payload downloads use fresh, mode-0700 staging directories. Payload
files are verified in staging before safe atomic replacement in the receipt tree;
the completion manifest and `verified` receipt are published only after every
declared file verifies. Retries reuse a matching verified receipt, replace an
incomplete cache from fresh staging, and reject a changed completion record for
the same remote path. Local symlink or non-regular-file sentinels are permanent
contract failures and are never followed or overwritten.

One manifest may declare at most 16,384 files. Durable pending and verified
receipts count toward per-run limits of 512 manifests and 65,536 files. Aggregate
declared bytes may not exceed the smaller of the encrypted storage allocation and
256 GiB. Before each payload transfer, the controller also requires enough local
free space for the declared payload plus a 1 GiB reserve.

Transient SFTP listing or copy failure defers mirroring and does not change
authenticated workload truth. Path escapes, unsupported glob syntax, control
characters, symlinks, duplicate paths, conflicting completions, exceeded limits,
and size or hash mismatches fail the workload. Any prior verified checkpoint is
reported as `partial_recovered`; the supervisor then deletes the provider resource
and completes normal fail-closed closeout.

### Signed mirror acknowledgements

Jobs that cannot tolerate more than one unmirrored checkpoint interval may add
`artifacts.incremental_mirror_ack`. The controller generates a unique Ed25519 key
under the local run's mode-0700 secret directory before provider provisioning;
the mode-0600 private key never enters the run request, worker environment,
status, or recovered artifacts. The request carries protocol
`incremental-mirror-ack/1`, an acknowledgement directory and timeout, and the
public key, fingerprint, run-bound identity, and pinned OpenSSH signature
namespace.

For each verified completion manifest, the controller signs the run ID, bundle
hash, image digest, manifest path/size/hash, declared file count/bytes, and local
receipt hash. Publication uses the existing atomic transfer path and a durable
local publication receipt. An unknown upload result is reconciled by discovery
and exact readback; a conflicting remote acknowledgement is permanent failure.
Workers must wait only after publishing a closed completion manifest, verify the
signature and every known binding, and refuse resume from a checkpoint without a
valid acknowledgement. `ssh-keygen -Y verify` supplies the Ed25519 verifier.

Terminal artifact recovery follows the same failure classification: transient
`TransferUnavailable` observations retry while provider time remains, while
malformed manifests and permanent transfer contract errors return a failed
observation so the supervisor proceeds to provider deletion instead of relying
on a process restart.
