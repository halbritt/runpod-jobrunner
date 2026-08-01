# First paid exercise result

Date: 2026-08-01 UTC  
Scope: no-private-data RunPod vertical slice  
Outcome: the fourth run completed and closed without interactive repair or a
supervisor restart. It satisfies the strict clean-run exit criterion while the
third run remains the recovery-path exercise.

## Attempts

| Run | Result | Provider resource | Closeout |
| --- | --- | --- | --- |
| `run-20260731T231952-8659465e69c3` | GraphQL authentication and client-signature failure | None observed | Closed at `23:30:04Z`; no-resource delete acknowledgement, provider absence, and current spend `0` |
| `run-20260731T234153-ca572eb66b02` | GraphQL response selection rejected unsupported fields | None observed | Closed at `23:52:04Z`; no-resource delete acknowledgement, provider absence, and current spend `0` |
| `run-20260801T000705-9e655643753b` | Succeeded after one controller hotfix and two supervised restarts | Pod `04wnw6l8l7u8xp` | Closed at `00:15:54Z`; verified artifact, acknowledged delete, provider absence, and current spend `0` |
| `run-20260801T003338-3d00352cd1c8` | Succeeded without interactive repair or a supervisor restart | Pod `oz54kuxvzd529w` | Closed at `00:36:42Z`; verified artifact, acknowledged delete, provider absence, and current spend `0` |

Each attempt durably recorded exactly one provision dispatch. The first two
dispatches received provider rejections before a resource ID existed. Their
supervisors retained the unknown-outcome fence until their independent provider
termination deadlines elapsed, confirmed absence, and closed without retrying
create.

The third run exercised supervised recovery. The fourth run followed the normal
`run` path from launch through closeout: no SSH session, signal, service action,
CLI recovery, or live code change was used.

## Third-run identity

- Image: `ghcr.io/halbritt/runpod-jobrunner-noop@sha256:d6b9f31996c37dc3a627eb3fede9d9a7ae09a86f58108a2ffeeb6a4ea69f6fde`
- Remote runner: version `0.1.5`, Git commit
  `cfd725e7eea9cd07aeb2286563fb35d20200705e`
- Controller at launch and bundle stamp:
  `a6303764f0317ea8827994721bb9d3463c22ac0c`
- Controller after the status-channel fix:
  `0655c629b184bb76ca82c8c2f2d36ae11b49236c`
- Provider create operation:
  `op-f23700a7-f1f2-5f42-b069-81c2dc3ac5e7`
- Provider delete operation:
  `op-a20cc8bf-e294-52a4-a8bd-17d28ee261b8`
- Provider rate: `$0.24/hour`
- Independent provider `terminateAfter`: `2026-08-01T00:17:05Z`

Each recorded provider-list observation reported one pod with the exact image
digest.
The durable provision attempt count remained one before and after both local
supervisor restarts. Deletion was acknowledged at `00:15:49Z`, provider absence
was confirmed at `00:15:51Z`, and the provider pod list was empty after closeout.
Deletion therefore completed before the independent termination deadline.

## SSH-independent process truth and controller recovery

A bounded SSH inspection connected at `00:10:45Z`, observed the remote runner
under `tini`, and then disconnected. The authenticated HTTP status channel later
reported the same run and pinned runner identity in these states:

- `ready` at `00:14:29Z`;
- `running`, phase `preflight`, child PID `392`, `running: true` at `00:14:52Z`;
- `terminal`, phase `package`, outcome `succeeded` at `00:15:36Z`.

The SSH disconnect did not become a false process-exit observation.

The original local supervisor was PID `2955800`. At `00:14:12Z` it was killed
after the controller status-channel fix was committed so systemd could load the
corrected code. Systemd restarted it at `00:14:17Z` as PID `2972748`, with
`NRestarts=1`.

The deliberate recovery exercise occurred during the live preflight. At
`00:15:03Z`, authenticated status still showed phase `preflight` and child PID
`392` running. Only supervisor PID `2972748` was sent `SIGKILL`. Systemd restarted
the supervisor at `00:15:12Z` as PID `2975287`, with `NRestarts=2`. The provider
resource ID remained `04wnw6l8l7u8xp`, provision attempts remained one, and the
provider still listed exactly one pod. The remote child completed without a CLI
`recover` command. After closeout, the transient unit was `inactive/dead` with
`Result=success` and no main PID.

## Third-run artifact and closeout proof

The remote terminal record reported all enabled phases complete with exit code
zero: `verify`, `preflight`, and `package`. Its artifact manifest SHA-256 was
`b3a60bb73b571dcbf886d0540ac83976444f1c3ad07099f3a6e753aae1bc18b8`.
The recovered manifest matched that hash, and the recovered
`noop-result.json` matched the declared SHA-256
`25d10f149b2b9e2c611b1df077462e47d3dad0cdc366161e0d9c42a780794415`.

The closeout receipt records:

- workload result `succeeded`;
- artifact disposition `verified`;
- one acknowledged delete for pod `04wnw6l8l7u8xp`;
- provider not-found `true`;
- current spend per hour `0`.

The local evidence is under
`~/.local/state/runpod-jobrunner/runs/run-20260801T000705-9e655643753b/`.
That run directory also contains secrets and must not be published wholesale.

## Clean fourth-run acceptance

Release commit `c682e91186b38e5a1aebc242252fa35ea8a7fcfd` cut runner
version `0.1.6`. GitHub Actions run `30675645766` succeeded for that exact
commit. Anonymous registry reads resolved both the `0.1.6` tag and digest URL to
OCI index
`sha256:d83de7e79ad5ca1c19ad5cd335ed7f79f9549ecbcae849927cdb40585605594c`.
The index contains the Linux/amd64 image
`sha256:8753af807fbb193b205a2005a58f94091a166521043452c0271671000d120ea0`
and a linked attestation. Anonymous config and attestation reads also verified
the release version and revision labels, SLSA provenance, and SBOM.
Bundle-stamp commit
`640a1b4dbca3b699559ba05a38412beaf395de03` pins the index digest, runner
version, and release commit. The bundle hash is
`5c5f19cb94a6d92f24710b891d3e4a09b418229c4b5a035afcd0e4d413e25795`.

The fourth run recorded one provision dispatch and one provision attempt under
operation `op-08ccf845-8460-59e7-9667-eaaccd73342a`. It observed only pod
`oz54kuxvzd529w`, at `$0.24/hour`, with provider `terminateAfter`
`2026-08-01T00:43:38Z`. The remote terminal record identifies runner `0.1.6`
at commit `c682e91186b38e5a1aebc242252fa35ea8a7fcfd` and reports:

- outcome `succeeded` with reason `all_enabled_phases_completed`;
- exit code zero for `verify`, the 45-second `preflight`, and `package`;
- elapsed time `65.577147` seconds and estimated cost `$0.0043718098`;
- artifact-manifest SHA-256
  `1ca38722b11f9c8c6d10c581bd235f8690e683b8c0557493759f9bd6dafc4b88`;
- `noop-result.json` SHA-256
  `db540b6ebfeb08f43c46d1c3baad9ffbf34cac6394200443167881733d466f55`.

The recovered manifest matches the remote manifest byte for byte, and the
recovered result matches its declared hash. Deletion was acknowledged once at
`00:36:37Z`, provider absence was confirmed at `00:36:40Z`, and closeout
completed at `00:36:42Z`, almost seven minutes before `terminateAfter`. The
closeout receipt records artifact disposition `verified`, delete status
`acknowledged_all`, provider not-found `true`, and current spend per hour `0`.
The post-closeout provider list was empty.

No supervisor restart was recorded: systemd reported `NRestarts=0`, and the
journal contains one start with no restart or service failure. The transient
unit was collected as `inactive/dead` with `Result=success`. As an operator
observation, no `recover` or `stop` command, SSH session, process signal,
service restart, or code edit occurred between launch and closeout. The run
protocol also records no recovery reason. The local evidence is under
`~/.local/state/runpod-jobrunner/runs/run-20260801T003338-3d00352cd1c8/`;
that directory contains secrets and must not be published wholesale.

## Time and spend

Provision dispatch occurred at `00:07:07.861Z`; deletion was acknowledged at
`00:15:49.191Z`, an interval of about 8 minutes 41 seconds. The remote terminal
estimated `$0.0280214424`. The account balance changed from `$164.2273624798` to
`$164.1969185909`, a `$0.0304438889` decrease over the exercise window. RunPod's
resource billing history had not yet emitted a line item at closeout, so the
balance delta is the best available provider-side charge observation and is not
claimed as a finalized invoice line.

For the clean fourth run, provision dispatch occurred at `00:33:41.069Z` and
deletion was acknowledged at `00:36:37.926Z`, an interval of about 2 minutes
57 seconds. Account-wide balance snapshots changed from `$164.1920680428`
immediately before launch to `$164.1879144872` at the first post-closeout read,
a `$0.0041535556` difference. A later zero-pod, zero-spend snapshot read
`$164.1797463391`, which demonstrates that account billing was still moving.
The remote run estimate was `$0.0043718098`. The balance differences are not
attributed solely to this run, and none of these values is claimed as a
finalized invoice line.

The two budget ledgers currently contain:

- scope `striatum-2026-07-31`: `$50.00` total authority, with two retained
  `$0.50` reservations for the first two attempts and `$49.00` remaining;
- scope `striatum-2026-07-31-amendment-1`: `$49.50` total authority, with one
  retained `$0.50` reservation for each successful no-op and `$48.50`
  remaining.

`budget-ledger/1` enforces each scope independently. It has no field or check
that links an amendment scope to a parent scope. The formal sum of the two
per-scope totals is therefore `$99.50`, not a mechanically enforced `$51.00`
aggregate.

The principal expressly authorized the final `$0.50` no-op in addition to the
earlier allocation and designated `$50` as a soft spend target. The delegated
reservation plan is now `$1.00` for the first two attempts, `$1.00` for the two
successful no-ops, `$2.00` for the future Qwen preflight, and `$47.00` for the
future full run: a planned reserved-cap envelope of `$51.00`. Reserved caps are
conservative ceilings, not projected charges; the added no-op's remote estimate
and first account-wide balance difference were both less than one cent. The
planned envelope exceeds the soft target by `$1.00`. The future Qwen
reservations must use only the original
`striatum-2026-07-31` scope's remaining `$49.00`. The amendment scope's
remaining `$48.50` is intentionally unused. This allocation is an operator
constraint; v1 does not enforce it across the two ledgers. Reservations are not
refunded from observed spend.

## Defects found and corrected

1. RunPod GraphQL requires its API key in the query string, and Cloudflare
   rejected Python's default client signature. Commit
   `e0df1bd99314867d25c37fa486b9f159c98c0728` corrected GraphQL authentication,
   added a product user agent, and preserved REST bearer authentication.
2. The create mutation selected unsupported `publicIp` and `portMappings` fields.
   Commit `9a93f9dae9a9b40670ebb14c11d87f4449f48272` narrowed the mutation response.
3. Review of that response fix found deep-JSON recursion and HTTP-error custody
   gaps. Commit `cfd725e7eea9cd07aeb2286563fb35d20200705e` closed them and became remote
   runner release `0.1.5`.
4. The independent status fetcher still used Python's default user agent and
   received HTTP `403`. Commit
   `0655c629b184bb76ca82c8c2f2d36ae11b49236c` added the product user agent. Its
   gate passed 244 tests, Pyright with zero errors, and Ruff.

## Remaining limits

- The third run needed the status-fetcher hotfix and supervised restarts. The
  fourth run closes the clean-run criterion; the third run remains the evidence
  for reconciliation and exact-one-create behavior across controller recovery.
- The live SSH exercise used a bounded inspection connection that disconnected.
  It did not simulate a prolonged network partition or packet loss.
- The transient systemd unit exposed `NRestarts=2` while live, but systemd reset
  that counter after collecting the third run's inactive unit. The run protocol
  does not currently persist supervisor restart events.
- Provider billing history lagged closeout. The final per-resource line item
  should be reconciled later against the recorded balance delta.
- `budget-ledger/1` does not enforce a parent/amendment relationship or a total
  across scope names. The operator must keep the amendment scope's remaining
  `$48.50` unused and place future Qwen reservations only in the original scope;
  otherwise the ledgers permit reservations beyond the delegated `$51.00`
  plan.
- Registry tags are mutable. The accepted bundle therefore pins the immutable
  OCI index digest instead of relying on the `0.1.6` tag.
- A simultaneous controller-host and RunPod control-plane outage can still delay
  deletion until the provider deadline. The remote elapsed cap and provider
  `terminateAfter` bound compute exposure but do not remove that residual risk.
