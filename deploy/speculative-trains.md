# Depth-two train canary

N is deployed **disabled**. Do not enable until TauCeti issues #16231 (B),
#16232 (E), and #16234 (F) are CLOSED with genuine `rollout-verified:` evidence,
and the trusted TauCeti immutable-ref workflow change has landed and passed a
NEW production dispatch. Required human infrastructure approval is mandatory.
There is no 24-hour baseline prerequisite for this or unrelated releases.

## Scheduling and identity

The durable repository executor remains the sole scheduler. It may test A on
main and B on the exact bound A commit. B's batch contains B's own immutable
members, preserving bundle atomicity; its base includes A. There are at most two
current train candidates. No branching or depth-three in-flight train is allowed.
One successor reservation per parent generation bounds wasted work. Existing
legacy candidates drain serially before a fresh root can speculate.

Assembly requires local squash construction, known exact toolchain bytes, and
unchanged effective base policy. The helper records a complete exact base/head
Git-tree diff independently of optional diagnostics. Incomplete or 300-path
immutable comparisons, mutable PR 3000-file ceilings, more than
64 members, or a pin/policy/tooling path barrier stops speculative admission.
Barriers include both sides of renames of lean-toolchain, lake-manifest.json,
lakefiles, bors.toml, scripts/, and .github/. Such roots still run serially.
Speculative successors never fall back to the shared mutable staging branch.

Every tracked generation publishes only `staging-candidate-ID-GENERATION` with
an absent-ref lease. Retry/restart checks the bound commit and reconstructs only
that exact Git object from the durable assembly recipe. The trusted workflow
checks bors's live requested batch (repository, branch, batch ID, state, head,
base, candidate and generation), derives the immutable ref, and verifies its
GitHub object and base ancestry. Payload fields cannot select a ref or authority.
The existing workflow concurrency key isolates repository/batch/head/base/branch.

The required exact-commit statuses authorize normal promotion checks. The trusted
CI finalizer reports green only after audits, exact public-cache confirmation
when configured, and current-generation validation. A status-only recovery can
deliver that verdict even though the original workflow remains failed; workflow
housekeeping is not an additional promotion gate.
After dispatch intent has been recorded, a train never resends an ambiguous
request merely because Actions has not exposed the run yet. An operator must
reconcile a persistently unconfirmed dispatch; do not delete its reservation or
redispatch without proving no accepted workflow exists.

B's terminal verdict is held until the exact predecessor generation has a
recorded promotion and main equals that predecessor's bound head. Main is read
again before promotion, and Git uses `--force-with-lease` against the exact tested
base after proving ancestry. This closes the read/push race, including an
unexpected main move to an intermediate ancestor of the candidate. A lost push
response is recovered through the existing promotion journal. An unexpected
advance beyond A invalidates B even if the advance contains A.

Failure, withdrawal, changed membership/head, generation changes, and unexpected
main advance invalidate descendants. Their own exact reviewer/head/manual approval
snapshots move into sealed native retry batches once; predecessor members are
never annexed. Immutable history survives. Stale approval/cancel commands carry
the captured head and approval token, allocated atomically at durable command
receipt. Reapproval/withdrawal supersedes older intent even when cancellation
priority reorders execution; ordinary worker writes cannot overwrite that token.
Green statuses do not bypass these checks. Old canceled generations cannot
promote successors.

## Hard limits and cost

Three durable repository staging reservations cover construction, dispatch and
unfinished invalidated workflows. Only authoritative completion of the exact
workflow releases a dispatched reservation. Cancellation in bors does not free
runner capacity. Before new dispatch, bounded Actions queries also count queued,
in-progress, waiting, requested and pending staging workflows, including legacy
runs. Incomplete/rate-limited observations block dispatch. At most two valid train
candidates plus one diagnostic/retry job can execute; leftover invalidated work
uses these same slots. Existing diagnostic isolation remains serial within its
available slot. No speculative candidate, retry, or ordinary PR gains cache or
publication privileges.

The initial lifetime canary budget is **one successor reservation**. Operators may
set `BORS_SPECULATION_CANARY_SUCCESSORS` to 1–10; the hard maximum is ten for this
implementation, counted durably across restarts. Failed construction consumes a
reservation too. Exhausting this budget returns scheduling to serial operation,
while all exact generation/promotion and capacity guards keep draining. Existing
workflow CI deadlines and infrastructure retry backoff remain in force. The
three-workflow bound is a staging cap, not a cap on unrelated PR workflows or a
promise about dollars/runner sizes; B/F measurements determine the enablement
cost decision. External administrator-triggered reruns consume capacity and must
not be used to bypass the scheduler.

## Activation and verification

Keep `src/index.js`'s `BORS_SPECULATIVE_TRAINS=false` until all gates above pass.
Activation is a separate reviewed deployment configuring:

- `BORS_SPECULATIVE_TRAINS=true`, async orchestration and local Git mode enabled.
- `BORS_SPECULATION_GATE_16231`, `_16232`, `_16234`: exact GitHub issue-comment
  evidence URLs for each CLOSED, rollout-verified prerequisite. The URL shape is
  validated; the deployer must verify the evidence itself, not just supply a URL.
- `BORS_SPECULATION_CANARY_SUCCESSORS=1` initially; never raise it to compensate
  for an identity or approval failure.

The initial canary advances the Durable Object's one-shot startup epoch from
`outbox-v1` to `outbox-v2-train-canary-1`. A warm container receives a bounded
graceful stop before the new epoch is acknowledged; a failed stop retries without
clearing durable work or reservations. Later requests at the same epoch do not
restart the container. Confirm `/health/` returns `x-bors-speculative-trains: true`:
that header reads the backend process environment, not the Worker's configuration.

Capture before/after health, observation/v1 and candidate journal snapshots;
verify TauCetiReview's current observation validator and account for every
approval change. Observe an A/B pair with distinct exact refs and matching trusted
workflow runs; verify out-of-order results cannot land B early. After A lands,
check B's actual target update equals its tested head and has exactly one promotion
and dispatch. Compare required build/lint/axiom/publication checks with the serial
path. Use B's trusted host observations for runner cost, throughput and wasted
work; status receipt time is not runner execution time. Record all links on
TauCeti#16243 before writing `rollout-verified:` and closing it.

Stop immediately on incorrect promotion, approval loss, duplicate dispatch or a
capacity violation. Disable the flag in a reviewed deployment; existing tracked
candidates keep exact guards and reservations, successors drain in order, and new
work runs serially within the remaining capacity. Do not clear reservations to
accelerate rollback. Publication secrets remain isolated, trusted scripts stay
workflow-pinned, and ordinary PR-produced compiled files never enter shared
staging caches.
