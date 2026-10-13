# Queue recovery

Bors retains batches, their CI status rows, and exact-head approvals when its
batcher crashes. Restart recovery resumes the database state instead of deleting
waiting work. Recovery does not grant scope authority: human scope approval is
still revalidated at admission and immediately before promotion.

Each accepted approval records its head, reviewer and desired queue membership.
Withdrawals, cancel-all, changed revisions and confirmed individual build or
conflict failures stop that intent. Healthy members of a failed parent batch
retain it. Retry children are inserted transactionally; repeated creation reuses
existing exact-head memberships and preserves member priorities and manual scope
approval snapshots. Atomic bundles remain indivisible.

The existing heartbeat recovers missing memberships through normal live checks.
It offers at most four orphan approvals and four eligibility checks per tick;
existing held approvals are recovered in batches of at most four. Already waiting
approvals do not get another prerun loop. GitHub PR-list discovery reads one page
per tick, retaining the last complete snapshot while refreshing it. Observations
expose snapshot time and whether the scan is complete. Webhooks remain the fast
path. These limits bound offered work and list-page requests; admission still
requires its ordinary GitHub reads, which can paginate or retry. No per-worker
repository scan or LLM session is added.

A green staged commit that cannot be pushed keeps its running slot and retries
promotion after five minutes. After six unsuccessful attempts it alerts and
backs off to hourly. Stored retry deadlines apply after restarts and to duplicate
status notifications. Repeated process crashes back off from 30 seconds to one
hour and produce a dedicated alert at six failures. A delayed running batch still
owns staging; waiting batches cannot overwrite its build. These are infrastructure
holds, not failed proofs. The observation API exposes deadlines and counts.

For the configured staging-dispatch project, a failed `build` status is checked
against its trusted GitHub Actions source before it becomes an individual proof
failure. This failure-only classification reads at most one exact-head status
page and one Actions run per attempt; ordinary green CI adds no requests. The
source must match the repository, workflow, dispatch event, branch, batch, head
and base. Classification covers `build`, `scope`, and `bump-guard`, including
guard failures posted before the build finalizer. An in-progress finalizer or
newer pending/successful status gets a one-minute propagation hold without
incrementing the failure count or extending its CI deadline. The poll consumes
the updated verdict before bisection or promotion; a held failure never posts
a successful Bors status. Running holds arm the existing batch recheck message
for their deadline, bypassing the coarse regular poll. Startup re-arms these
timers from durable deadlines; stale rechecks cannot revive a terminal attempt.
Transient database or status-read failures retain a prompt retry instead of
losing the wake-up or breaking worker startup. Infrastructure retries report
their members as waiting in queue; repeated-problem alerts remain separate.

A confirmed cancelled build retains approval and creates a native retry child
with the same exact-head membership through the existing transactional divider.
The old attempt retains historical links and CI records while releasing the
unique staging SHA; the child's distinct batch ID keeps an old
cancellation from cancelling a newer attempt on the same SHA. Statuses for
older matching attempts are propagation holds until the current verdict arrives.
The child returns to normal waiting-batch preflight. The existing infrastructure
failure count drives backoff from five
minutes to hourly; prior process failures can make the initial delay longer.
Restart never shortens an existing deadline. Other due waiting batches can use
the slot during cancellation backoff. Withdrawals, changed heads and holds
remain subject to preflight; no AI edit or rebase is requested. Conclusive
build failures and workflow timeouts still bisect normally.

Unavailable API reads or an unverifiable source briefly retain the running
batch for another classification. Three infrastructure observations or the
original CI deadline release its staging slot and retry through normal
preflight, preserving approval instead of fabricating a failed singleton.
Repeated inconclusive CI emits an operator alert at counts three, six, and
every subsequent sixth observation. Inspect CI and API access: these retries
are not evidence that the PRs passed integration. The configured public
TauCeti repository's Actions metadata is readable with the existing Bors App;
a private repository would also need the appropriate Actions read permission.

## Deployment and verification

Merge the reviewed change into master after CI passes. The existing Cloudflare
Builds trigger deploys master and runs the additive migration before serving.
Confirm the deployed revision, the migration, health and active batches before
restoring legacy intent. Rollback uses the prior application revision; leave the
additive columns and indexes intact so approvals remain auditable.

The migration seeds only currently active exact-head memberships. Historical
stranded approvals require a one-time audited bootstrap because the previous
release did not record withdrawal tombstones. Use `scripts/bootstrap_queue_approvals.py`
with a JSON list of `{pr, head_sha, batch_id}` records. Run with `--limit 1` and
without `--apply` first. The script requires psycopg 3 and authenticated gh. It
requires `--output` for its results. Applying also requires `--audit` pointing to
a manifest with `recoverable` exact-head records and `withdrawals_checked: true`.
The latter attests to the withdrawal audit described below.
rejects changed/draft/closed PRs, holds, active memberships, bundles, separate held
or manual approvals, and confirmed singleton failures. The source audit must
establish crash/race provenance and exclude dashboard cancel-all withdrawals,
which the old release did not record in its command log. Never treat arbitrary
old green checks as permission to retry.

After deployment, apply one approved canary. The conditional update restores its
previous automatic approval with bootstrap provenance; it creates no batch and
posts no command. Normal Bors recovery performs the actual admission. Verify one
active exact-head membership, preserved sibling work, normal staging CI and
landing. A repeated bootstrap must change no rows. Then extend to the remaining
audited recovery candidates. Confirmed integration failures remain stopped for
repair; they are not part of this bootstrap.

## Stuck recovery

Inspect `promotion_retry_at`, `promotion_failures`, `recovery_after` and
`recovery_failures` in the existing batch observation. Fix the infrastructure
cause first; the next due retry resumes automatically. Permanent permission or
configuration problems retain approval but can block the project queue, so the
six-failure alert requires operator attention. A maintainer can issue `bors r-`
on the affected PR, which withdraws its intent and preserves unrelated siblings,
then explicitly review and queue it again once fixed. Bundles withdraw as units.
Human scope approvals must be granted again by the authorized human after
withdrawal. Do not clear database records or replay historical approval comments.

## Dependency bumps after a confirmed integration failure

The existing heartbeat offers the known open dependency bump every five minutes,
including when GraphQL candidate discovery fails. This reserves one of the four
eligibility reads; it adds no scheduler or branch updates. The serialized batcher
may retry an unchanged, automatically reviewed singleton bump when current main
is a strict descendant of its failed staging base. Legacy batches resolve that
base from the staging commit's first parent. The normal current-head eligibility,
CI, backend, label, and merge-base checks still apply. Pending approvals, active
membership, explicit withdrawals, bundles and human approvals are excluded.

Three retries are allowed per head, each requiring a new main revision. The
counter survives restarts and new check IDs and resets on a genuinely new head.
A failed observation spends no retry. The decision and base are persisted with
the existing exact-head approval before normal queue admission.

A confirmed singleton failure hands off to proof repair only when main has not
advanced, ancestry definitively prevents retry, or the three-retry budget is
exhausted. API failures defer. Repair ownership is stored in exact-head failed
queue intent before publishing `bump-integration`; later main advances cannot
requeue that head while an editor owns it. A retry stopped before admission
restores failed intent. Timeouts refund its attempt; definitive failures retain
the charge. Both retain the attempted main revision so they cannot loop on the
same main. Multi-PR batch failures and cancelled CI do not publish it. The public
observation's `bump_repairs` lists only current stopped singleton heads, excluding
pending/active work and withdrawals. Worker bump selection can therefore repair
an integration-only proof failure even when standalone CI is green. It rereads
both the live PR and this handoff before launching an editor. Observation outages
skip this candidate without stalling unrelated worker work. A missed status
publication is retried by the same heartbeat. Recovery that succeeds unchanged
requires neither a branch push nor another AI editing or review session.

## Dependency bump scheduling

The serialized queue poll gives an already-approved dependency singleton
priority 50 without changing the PR priority. Explicit nonzero maintainer
priorities are respected. This applies to existing waiting memberships after deployment and
requires the bot author/reviewer, current accepted single-head policy proof and
exact-head approved intent. Withdrawals, drafts, bundles, manual approvals and
multi-member batches are excluded. The scheduler adds no GitHub reads or AI work.
Running batches retain their sentinel priority 100 and finish normally. The
promotion changes queue order without the manual priority command's preemption
side effect. Ordinary priority-zero batches follow the bump; explicitly higher
maintainer priorities remain respected. New bump heads and bisected children use
the same policy on each normal queue poll.
