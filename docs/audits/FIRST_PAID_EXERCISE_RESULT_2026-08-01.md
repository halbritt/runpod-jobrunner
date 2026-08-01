# First paid exercise result

Date: 2026-08-01 UTC  
Scope: no-private-data RunPod vertical slice  
Outcome: the third run completed and closed safely; the strict no-interactive-repair
exit criterion remains unproven because a controller HTTP fix was applied during
that run.

## Attempts

| Run | Result | Provider resource | Closeout |
| --- | --- | --- | --- |
| `run-20260731T231952-8659465e69c3` | GraphQL authentication and client-signature failure | None observed | Closed at `23:30:04Z`; no-resource delete acknowledgement, provider absence, and current spend `0` |
| `run-20260731T234153-ca572eb66b02` | GraphQL response selection rejected unsupported fields | None observed | Closed at `23:52:04Z`; no-resource delete acknowledgement, provider absence, and current spend `0` |
| `run-20260801T000705-9e655643753b` | Succeeded after one controller hotfix and two supervised restarts | Pod `04wnw6l8l7u8xp` | Closed at `00:15:54Z`; verified artifact, acknowledged delete, provider absence, and current spend `0` |

Each attempt durably recorded exactly one provision dispatch. The first two
dispatches received provider rejections before a resource ID existed. Their
supervisors retained the unknown-outcome fence until their independent provider
termination deadlines elapsed, confirmed absence, and closed without retrying
create.

## Successful run identity

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

## Artifact and closeout proof

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

## Time and spend

Provision dispatch occurred at `00:07:07.861Z`; deletion was acknowledged at
`00:15:49.191Z`, an interval of about 8 minutes 41 seconds. The remote terminal
estimated `$0.0280214424`. The account balance changed from `$164.2273624798` to
`$164.1969185909`, a `$0.0304438889` decrease over the exercise window. RunPod's
resource billing history had not yet emitted a line item at closeout, so the
balance delta is the best available provider-side charge observation and is not
claimed as a finalized invoice line.

Budget admission was additive and conservative:

- scope `striatum-2026-07-31`: `$50.00` total authority, with two retained
  `$0.50` reservations for the first two attempts;
- scope `striatum-2026-07-31-amendment-1`: `$49.50` total authority, with one
  retained `$0.50` reservation for the successful attempt.

The aggregate worst-case authority was `$50.50`; the principal explicitly
accepted that as the soft-cap interpretation. Reservations are not refunded
from observed spend, leaving `$49.00` unreserved in the amendment scope.

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

- This run needed the status-fetcher hotfix and a supervisor restart. It proves
  reconciliation, exact-one-create behavior, verified recovery, and closeout,
  but it does not satisfy the stricter milestone exit of completing a paid run
  without interactive repair. A future already-authorized workload can provide
  that evidence; no extra pod was created for this report.
- The live SSH exercise used a bounded inspection connection that disconnected.
  It did not simulate a prolonged network partition or packet loss.
- The transient systemd unit exposed `NRestarts=2` while live, but systemd reset
  that counter after collecting the inactive unit. The run protocol does not
  currently persist supervisor restart events.
- Provider billing history lagged closeout. The final per-resource line item
  should be reconciled later against the recorded balance delta.
- Commit `0655c629b184bb76ca82c8c2f2d36ae11b49236c` is pushed on `main`, but the
  controller status fix has not been cut as a new packaged release.
- A simultaneous controller-host and RunPod control-plane outage can still delay
  deletion until the provider deadline. The remote elapsed cap and provider
  `terminateAfter` bound compute exposure but do not remove that residual risk.
