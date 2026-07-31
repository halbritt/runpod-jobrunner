# Security and operations boundary

The controller may send a minimal versioned run request and files named by the
validated input manifest. It must not copy a repository, undisclosed files,
credentials, or a parent directory. Symlinks are rejected.

Provider credentials remain on the controller host. A remote runner receives a
random run-scoped status token and only the transfer authority it needs. Status
contains identifiers, phase, heartbeat, child state, sequence, and terminal
result; it contains no logs, paths, input contents, model contents, or secrets.

RunPod's provider termination deadline is mandatory and independent of the local
supervisor. A stopped workload is not a deleted worker. Operators should use
`status` or `recover`; never infer closeout from an SSH failure or a systemd unit
exit.

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
