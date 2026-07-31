# Domain context

## Mission

Run one bounded remote training job as a recoverable transaction: persist intent,
provision and start immediately, observe process truth independently of SSH,
recover only verified artifacts, and remove the billable resource.

## Glossary

- **job bundle**: immutable local specification plus an exact input manifest. The
  directory itself is never uploaded wholesale.
- **run**: one accepted job request with a stable identity, immutable approval,
  and durable event history.
- **operation**: one provider or transfer effect. Intent is durable before the
  effect and carries a stable ID across retries.
- **lifecycle phase**: provider-control progress from `planned` through `closed`.
- **workload result**: remote computation outcome, separate from lifecycle
  closure. A failed workload can still close cleanly.
- **artifact disposition**: either verified local recovery or an explicit record
  that no artifact was recoverable or required.
- **terminal record**: the runner's single immutable workload result and final
  artifact-manifest hash.
- **closeout**: proof of artifact disposition, delete acknowledgement, provider
  absence, and zero current spend. Only then is the lifecycle `closed`.
- **unknown outcome**: an external call whose effect cannot be established. It
  must be reconciled by operation identity before retry.

## Invariants

1. No provider create occurs before the durable run and operation intent exist.
2. A retry never creates a second resource for an unresolved create operation.
3. SSH is transport, not workload truth.
4. Money is represented as decimal strings; limits can only become tighter.
5. The remote runner has no provider-management credential.
6. `closed` is evidence, not a synonym for process exit or delete request.

## Non-goals

This project is not a scheduler, workflow DAG, model trainer, model registry,
multi-cloud API, multi-tenant service, or source of model-acceptance decisions.

