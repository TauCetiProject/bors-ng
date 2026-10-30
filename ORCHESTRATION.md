# Durable asynchronous orchestration

K uses A's `CandidateAssembler` identity validation and append-only candidate
journal. J can replace construction independently. The feature is disabled by
default (`BORS_ASYNC_ORCHESTRATION=false`). The Cloudflare deployment opts in with
**one executing operation globally** (`BORS_ASYNC_WORKERS=1`). Other installations
may select 1–4 slots; the default is two. This release enables no speculative
trains and keeps the existing one-staging-slot scheduling policy.

The batcher is a scheduler and durable mailbox: it records casts, poll requests,
rechecks and recovery events without performing their GitHub operations. Public
command/status callers receive acknowledgement only after outbox insertion. A
supervised dispatcher claims at most 32 eligible rows per tick, offering at most
the configured number of executing operations. A database project lock serializes
claims across every branch of a repository. Each repository executor retains
legacy process state between jobs. Timers scheduled by an executor become durable
future mailbox rows; approval retry generations also survive process restarts.
The polling path considers every existing running batch, rather than just the
first. New waiting work cannot overwrite any running staging slot. Priority
changes reorder waiting work and do not preempt an active build during rollout.

## Construction and dispatch

Construction creates an immutable Git object before touching staging. The
candidate's exact identity and membership are validated before binding. Its
binding, running batch commit/base, and unique `dispatch:<batch>` outbox row commit
in one database transaction. A crash before that commit can reconstruct with a
new candidate generation; a crash after it replays dispatch for the bound SHA.
The local assembler stores the immutable metadata/date recipe in the outbox and
reads exact toolchain bytes as data before publication. If its disposable mirror
is lost, reconstruction must reproduce the same bound commit and tree before a
leased push is authorized. An already published SHA is reconciled without a push.
A queued withdrawal vetoes construction publication, dispatch and promotion;
normal serialized cancellation then releases the slot and retries surviving
members. Changed-head dispatch results are rejected and release their active
candidate through native cancellation/retry semantics.

Dispatch first verifies current generation, membership and commit. Force pushing
the same SHA is idempotent; staging push recovery does not create a new commit.
For trusted repository dispatch, the request is recorded before one API attempt.
After a lost response, a bounded page of the existing trusted workflow's Actions
runs created since the request (with a 60-second clock margin) is matched to
its exact branch/batch/head/base run name. An accepted run is
acknowledged without another dispatch. An unavailable or truncated lookup defers;
it never means absence. An unobserved request may retry after durable backoff.
GitHub provides no dispatch idempotency key or atomic transaction with bors:
visibility delayed beyond the retry interval can produce another workflow run.
The existing trusted concurrency group and exact batch/commit guards retain a
single visible candidate outcome; this release does not claim exactly-once HTTP
requests. It never allows two active bors candidates for the same staging slot.
The CI deadline starts at acknowledged staging publication, rather than charging
construction or outbox delay to the build.

## Promotion, notifications and recovery

The same repository lane serializes main promotion with dispatch and recovery.
Live PR heads, immutable membership, scope approvals, drafts and required statuses
remain gates. Terminal status webhooks refresh the authoritative current commit
verdict; duplicate unchanged receipts add no GitHub calls. A non-fast-forward
main advance uses the existing sealed retry child. If main already equals the
staged SHA after a lost push response, or ancestry proves it includes that SHA
after a recorded promotion request, recovery commits the successful outcome
without another push or fresh promotion authority. The promotion event and
terminal state commit together before notifications, so replay cannot revive or
promote the candidate twice.

Comments are separate outbox jobs with permanent idempotency keys and hidden
markers. One write is attempted initially; replay checks a bounded comment page updated since the notice was created
(with the same clock margin) for its marker. An unavailable/truncated reconciliation defers instead of
posting a duplicate. Commit statuses and labels retain their existing same-value
update semantics. Notification jobs do not grant queue or publication authority.

Preflight rejection generation also retains one latest fingerprint on each patch,
bound to its head, target branch and reason. PR-status reasons include the sorted
failed required contexts. Repeated evaluations of an unchanged blocker enqueue
no additional notice, even across executor restarts. A changed blocker or a
conclusive recovery followed by rejection gets a new marker; unavailable reads
and incomplete status sets do not count as recovery. The fingerprint and async
notice intent commit together. This adds constant storage per patch, while real
transitions retain ordinary outbox delivery/replay tombstones. Existing notices
are not rewritten or removed. Legacy synchronous delivery records the fingerprint
after an acknowledged write and retains its existing HTTP ambiguity on a crash.

Claims use random tokens. Completion, failure and lease renewal update only the
matching token; old results cannot complete a new owner. Active leases renew;
an application/dispatcher restart stops all old repository executors before
recovery. Expired leases become eligible after five minutes. Worker crashes
receive at least five minutes of quiet time for an in-flight GitHub mutation to
settle. Other failures back off from one minute to one hour. A watchdog bounds
one operation to 1,100 seconds. One API attempt may still complete after its
caller dies, so replay always reconciles external state. Deployment runs one
container; the slot cap is per application instance, while repository claim
serialization is shared in the database. Multiple replicas are outside this
rollout's global-cap guarantee.

On an orderly supervisor stop, the dispatcher stops claiming work and drains
matching active results for at most four seconds before the executors stop.
Completed commands use the existing token-fenced completion/retry paths. An
unfinished command retains its lease: a shared HTTP client or Git child may
still be executing, so stopping the caller cannot authorize an early replay.
Abrupt death and commands exceeding the drain window still require normal
lease expiry and external-state reconciliation. The drain does not shorten
leases or clear another dispatcher's work. See [restart recovery](deploy/restart-recovery.md)
for the timing evidence and rollout limits.

## Operation and rollback

Inspect `orchestration_jobs` for kind, project, state, attempts, available_at,
lease_until and sanitized last_error. Pending/running rows are work intent;
candidate diagnostics remain evidence with `queue_authority=false`. Completed
dispatch/notification keys are retained as deduplication tombstones. Payloads are
internally produced Erlang terms decoded with `[:safe]`; they contain installation
references, never installation tokens or publication credentials.

`attempt_timing.recent` retains at most eight numeric/category events per row:
claim time and availability (including the prior claim/completion timestamps),
lease-expiry recovery time and deadline, and results received during orderly
shutdown. Current `claimed_at_ms`/`completed_at_ms` describe the current attempt;
completion is cleared on a new claim. A recovery interval is abandoned-lease
waiting, not measured operation service. These events contain no payload,
claim token, credentials or exception text. Shutdown and expiry logs also carry
job/attempt identifiers and timing, without tokens.

Before disabling the feature, stop new admission and drain pending/running outbox
operations; then restart with the flag false. Do not switch execution modes live
or discard pending withdrawals. Application rollback leaves additive tables and
the approval token column intact. Existing queue observations and candidate
schemas are unchanged. Cache producer trust, trusted workflow pins, publisher
secrets and all build/lint/axiom gates are unchanged.

The bounded time filters use GitHub's documented [workflow run `created` filter](https://docs.github.com/en/rest/actions/workflow-runs#list-workflow-runs-for-a-workflow)
and [issue comment `since` filter](https://docs.github.com/en/rest/issues/comments#list-issue-comments).


Cloudflare image rollout can preserve a running instance's old startup environment.
The owning Durable Object applies `outbox-v1` once before serving requests: it
sends SIGTERM, waits at most ten seconds, destroys a remaining process, then
records the activation marker. A failure leaves the marker unset for retry.
The next start uses the deployed async environment. This stops only the singleton
Container process; its Durable Object storage, webhook Queue and PostgreSQL queue
state remain intact. Later Worker constructors do not repeat the restart.

## Disabled depth-two canary

N adds durable staging reservations and immutable generation refs to this same
repository lane. Production still sets `BORS_SPECULATIVE_TRAINS=false`; deployment
alone grants no extra staging capacity. B/E/F evidence and the trusted workflow
change must be verified before a separate small canary activation. See
[depth-two scheduling, exact promotion, caps and rollback](deploy/speculative-trains.md).

## Dispatch priority and latency

The mailbox has three stable FIFO classes: candidate commands (0), lane progress
(1), and background work (2). Dispatch, poll/recheck restoration, promotion retries
and every status observation of a recorded candidate use class 1. Both
red and green observations share the class; priority does not select verdicts.
Cancellation uses class 0. Other commands have base class 2, with effective
class 0 for a running member and class 1 for a waiting member; ordinary PR status,
unrelated eligibility/admission/recovery and notices use class 2. Classification is recorded at receipt; pending commands are also upgraded when
their subject joins an incomplete batch, with a current recheck under the claim
lock. The additive migration classifies
existing pending/running rows in bounded pages. A late observation after a
candidate becomes terminal cannot grant it new authority. No observations are
coalesced or removed by this policy.

After eight critical claims in a repository, its oldest due background operation
gets one turn. This fairness promise is per repository, not a cross-repository
service guarantee under a global single-worker cap. The project row stores this counter under the existing claim lock,
so restarts, retries and multiple dispatcher claimers retain fairness. Backoff
still determines eligibility. FIFO within each class and the configured global
worker cap remain in force. A running repository is excluded before the bounded
32-row result selection, so its backlog cannot hide another free repository.
The joined priority query still sorts eligible rows; the bound limits returned
work, not total database scan cost. Webhook tick bursts are debounced to at most
ten drains per second, with completion always triggering an immediate drain.

Pending candidate commands, including policy eligibility, fence construction,
dispatch and promotion even during retry backoff. An eligibility wakeup received
before its PR has a patch row conservatively fences the whole project until read.
Within a priority class, polls and dispatches previously deferred by the
withdrawal fence yield to other ready jobs. This lets the waiting member commands
that block publication drain after defer backoff expires, even when the poll has
an older FIFO ID. Candidate status casts keep their verdict order, and every
publication still rechecks pending withdrawals before acting.

Fence hits defer without increasing failure attempts. Their next check respects
the earliest pending-command deadline, with completion of a command waking
blocked operations immediately; they do not cause exponential failure backoff.
All other errors retain infrastructure backoff, and six failed attempts emit a
stuck-operation warning. A failed command is retained and keeps fencing rather
than being silently discarded. Receipt-time approval tokens still fence
r+/r-/cancel-all generations. Dispatch now also reads every exact live PR head,
open/draft/base identity before publication; promotion uses the same check.
The pending-command/lease fence is repeated immediately before external mutation.
Unavailable identity reads defer with ordinary durable retry semantics. No
staging/main lock or train reservation is bypassed.

For each attempt, `enqueued_at_ms`, `claimed_at_ms` and `completed_at_ms` retain
wall-clock milliseconds;
`outbox_start`/`outbox_finish` log job/project/kind/priority/attempt, ready queue wait
and monotonic operation service milliseconds. `total_wait_ms` also includes all
prior deferrals/backoff from original insertion; pre-upgrade insertion times
have only one-second resolution. The `[:bors, :outbox, :operation]`
telemetry event carries both durations separately. Ready wait starts at the later
of insertion and `available_at`; intentional infrastructure backoff is excluded.
An operation exceeding the **10-second service target** emits
`outbox_service_budget_exceeded`. These measurements never decide correctness.
A pending dispatch is queued work, not running CI; its existing publication
acknowledgement still starts the CI deadline.

The operational target is **claim within two seconds after repository/global
capacity is free and input fences are clear**, independent of ordinary backlog
length, allowing one background fairness turn (target at most ten seconds) when
the eight-claim burst has been consumed. Existing non-preemptible operations and
candidate commands can still add service time. The existing **1,100-second hard
watchdog** remains the upper operation limit; a ten-second warning is not a forced
cancellation. Large preparation, recovery and live-read operations remain service
bottlenecks to measure and split at durable boundaries within #16811. This first
increment establishes queue priority and observable service budgets; it does not
claim an achieved production latency or throughput gain before rollout evidence.

## Authoritative wakeups and status ingress

Per-PR eligibility reconsideration, ordinary poll wakeups, queue recovery and
backend-hold recovery now use separate durable keys. The payload requests a read
of current state; it does not carry a verdict. Enqueue increments `wake_version`
under a row lock. Claim snapshots it in `claimed_version`. Completion clears the
key only when those versions still match. A receipt during execution increments
the version and records the earliest requested `rerun_at`, so completion re-pends
one follow-up read in a new FIFO row, atomically completing the old owner and
transferring the key. This puts a late read after commands queued during the
previous read. A pending wakeup separated by a command also gets a new row,
retaining the earlier read and preserving command order. The comparison and key
transfer hold the same row lock as receipt, preventing a lost wakeup on either
side of completion. A receipt that
races key removal retries insertion instead of disappearing.

Short mailbox transactions use the same project-then-job lock order for
receipt, completion, recovery and leases on both adapters. They never hold that
lock over GitHub work. This prevents MySQL unique-key/gap-lock deadlocks between
producers and a completing/claiming consumer.

Repeated pending wakeups share one read. Only immediately due wakeups coalesce. Future timers retain their original
per-deadline keys: an earlier read cannot satisfy a later admission deadline.
Fresh receipts cannot shorten infrastructure-error backoff. A retry/expired-lease
claim snapshots the latest durable version; stale tokens cannot complete it.
No approval, withdrawal, head-change, bundle/priority command or candidate status
verdict uses this coalescing mechanism. Existing exact status-payload deduplication
and immutable check/candidate observation receipts retain their prior semantics.

In async mode, webhook ingress avoids durable stage-status jobs for SHAs that
match no recorded batch, outside an assembling candidate's conservative fallback.
Trying receipts still go directly to their independent serialized actor. Async dispatch binds its commit before publication. An unbound candidate
on a waiting batch retains unmatched observations to cover temporary-ref assembly
races only for the latest generation while construction can still publish before
binding. Stopped/bound/superseded generations cannot keep that fallback alive.
An append-only `local_assembly_selected` fact disables the fallback for async
local assembly, whose remote publication follows binding. Unknown and shadow
assemblers remain conservative; native mode always retains publication races.
A database/read failure fails the webhook for its ordinary durable retry.
Legacy mode still forwards every status because its publication can precede its
record write. Trying delivery remains unconditional in the controller. Candidate
check receipts are journaled and ordinary PR eligibility notifications are queued
before filtering the stage-status route. The filter uses identities, without
assuming a context called `bors` can never be required. The
`[:bors, :webhook, :status_filtered]` counter measures avoided stage-status jobs.
Ordinary PR admission still reads its current GitHub statuses normally.

Before rolling this release back to code without wake versions, stop new intake
and drain pending/running versioned wakeups, including dirty follow-ups. Do not
clear a key or delete a row to force drainage. Additive columns may remain after
rollback. Deployment/restart on this release retains the same lease and version
fences; restart does not require replaying approval comments.

## Cooperative service boundaries and advisory delivery

In async mode, a terminal completion finishes its durable mutation and enqueues
an immediate completion poll instead of constructing the next batch recursively.
Its per-batch key is separate from ordinary poll retries, so an unrelated failed
poll cannot impose its infrastructure backoff on fresh completion. Queue and
backend-hold recovery handle one PR per operation, with at most four distinct PRs
per original wakeup. The remaining bounded budget and visited IDs travel in
durable continuation messages; a short queue cannot wrap and repeatedly read the
same PR in that wakeup. Native recovery retains its existing four-PR pass.
Restart may revisit a PR; live identity, approval and active-membership guards
still decide whether recovery may admit it.

Advisory notices and PR-head `bors` status writes have one separate global
notification slot, with one durable lease per repository. The mutation lane
retains one executor per repository and its existing global worker cap. Only
`notice` and `projection` payloads enter the notification executor; it rejects
merge work and ignores legacy poll messages. Project-row locking still serializes
all short mailbox transitions. Neither lane holds a database transaction during
GitHub calls. This permits advisory HTTP work alongside a merge mutation; it
never permits two staging/main mutators.

Notification jobs keep repository FIFO across retry deadlines. Each notification operation
has an eight-second shared deadline across reconciliation and delivery, including
advisory API queue time, with a ten-second maximum caller timeout. A separate
single API client and token cache keep advisory HTTP off the merge API server.
A request task is terminated at its deadline, including slowly streaming HTTP;
this bounds local service, without claiming to revoke an already accepted remote
write. Expired queued requests do no HTTP work; advisory HTTP requests
share the remaining deadline across token acquisition and delivery, without
middleware retries or token refresh replay. An error or lost response waits at
least five minutes before another attempt. Permanent HTTP4xx (except403/429)
and a truncated notice reconciliation terminate visibly; other advisory failures
terminate after six attempts. Terminal rows retain their tombstones and a
`notification_terminal:` error, and emit a warning before releasing FIFO.
Exact merge/approval commands and verdict observations have no such attempt cap.
A401 evicts that advisory installation token without replaying the write.
Notice retries reconcile their stable marker before posting again.
Status retries may repeat the same logical advisory status. Completed source-job
projection keys remain tombstones. Delivery rechecks the batch and current
candidate before publishing green; canceled/error/conflict or superseded
candidates cannot publish delayed green, and completed batches cannot publish a
late non-green status. A missing/deleted batch can still publish its cancellation
red, while green is suppressed. A projection is skipped if its SHA has become a
recorded tested commit; the synchronous tested status owns that identity. Candidate/trying verdict observations remain unchanged.
The tested commit's `bors` status remains synchronous before promotion because
branch protection may require it. Label reconciliation also remains synchronous
to preserve merged-label history.

These boundaries reduce completion and recovery occupancy; candidate preparation,
live dispatch validation/publication and label reconciliation may still exceed
the ten-second service target. The 1,100-second watchdog is an infrastructure
safety bound, not a latency target. Keep measuring ready wait, free-lane claim gap
and service separately; a fast free-lane claim does not meet the user-visible
ready-to-dispatch target while another operation occupies the lane.

Rollback to code without these payload handlers requires stopping new intake and
draining pending/running `projection`, `completion_poll`, `recover_queue_slice`
and `backend_holds_slice` work as well as versioned wakeups. Keep the forward
worker available for drainage and retain all terminal errors/tombstones. Do not
mutate queue rows to force a rollback. A rollback during a live lease must wait
for its ordinary fence; it is not evidence of running CI.

Phase telemetry `[:bors, :outbox, :phase]` and `outbox_phase` logs separate metadata
preparation, local Git assembly, assembly live-head reads, dispatch live-member
validation and publication. Metadata includes job/project/batch IDs and a fixed
phase name, with monotonic service time; it carries no tokens or status payloads.
Statistics never decide candidate validity. Separate notification kinds when
comparing merge-lane ready wait and service distributions.

Durable payload decoding preloads the trusted Batcher and CandidateAssembler
module literals before `binary_to_term(..., [:safe])`. A cold VM can claim a
continuation or publisher before its producer has run; safe decoding must not
mistake that known atom for an untrusted stored symbol. Unknown atoms remain
rejected and are never interned from queue data. This is module loading only;
leases, identities, command order and candidate validation still apply normally.

## Post-merge PR bookkeeping

Async successful promotion commits the promoted event, completed batch state,
approval retirement and an immutable cleanup snapshot in one transaction. This
also applies when recovery observes the tested commit already on the base.
The snapshot binds batch, tested commit, candidate, PR head and approval token.
Native and legacy serial delivery retain their existing behavior.

A background turn expands at most four independent PR-close children and keyed
historical merge notices. Continuations reference the retained parent snapshot;
they do not duplicate every remaining member. Each close job reads one live PR,
checks its head/base/draft, current local approval token and pending candidate
commands, and patches only its title and state. Already closed PRs need no write;
an existing title prefix is preserved. A changed head or fresh approval cannot
be closed by old cleanup. The historical notice identifies the exact merged
revision and remains deliverable when later PR intent prevents closing it.

Cleanup has its own single API client and token cache, independent of notices.
A shared eight-second deadline covers the live read and close, with no hidden
HTTP retry; an expired deadline cannot initiate a late close. Missing legacy
policy is inferred as non-squash, with bounded root/.github config lookup.
Retries wait at least five minutes. Permanent 4xx except401/403/429 and invalid
policy terminate visibly; other failures terminate after six attempts. Terminal
rows retain their keys and `cleanup_terminal:` error. Each failed child releases
its siblings and the merge lane. No cleanup job modifies staging or main.

Parent/child/continuation keys remain durable tombstones after completion;
expansion replay cannot regenerate an already completed child or notice. A
rollback to code without these handlers must drain `post_merge_cleanup`,
`expand_post_merge_cleanup` and `close_merged_pr` jobs using forward code first,
as well as earlier versioned wakeups, projections and service continuations.
Do not delete queued work or clear keys to manufacture drainage.

## Recoverable live read batches

TauCeti's public-GitHub async lane groups metadata and live PR identities into
GraphQL requests of at most 20 PRs. Reads run independently in monitored tasks,
using the normal HTTP transport timeout, retries and installation-token refresh.
There is no deadline shared by the entire collection or by unrelated callers.
The coordinator caches tokens, never PR identity or merge authority.

Healthy aliases and completed chunks survive a partial response. Missing or
malformed aliases use fresh REST reads; a failed GraphQL request falls back for
that chunk only. Metadata recovery retains every commit page in chronological
order, including histories beyond 100 commits and unsupported GraphQL author
types. Author profiles are optional enrichment: a failed lookup uses the PR's
known login and numeric ID for GitHub's canonical noreply attribution. A public
profile is reused once per distinct author within an operation.

Required identity evidence still validates repository, PR number, full head SHA,
base, open/closed state and draft flag. Failure of both read paths defers the
operation with the affected PR and underlying error recorded. Dispatch read deferrals
back off from 30 seconds to one hour without dropping approvals; rate-limited
GraphQL responses (including HTTP 403 with rate-limit headers) defer without
fanning out into REST requests. Promotion failures defer each batch independently
so other running candidates are still polled. The CI timeout begins on successful
dispatch; pending dispatches cannot time out proof CI. Dispatch and
promotion retain separate fresh identity reads and all generation, approval,
pending-command and withdrawal fences. No successful earlier read authorizes a
later operation, and no status verdict, approval decision or command is combined
by these queries.

The dispatcher skips additional rows for a repository already active in the same
lane during its current launch turn. A single due page can contain32rows from
one previously idle repository; after its first launch, repeated claims would
only acquire project/job locks and find the existing lease. Skipping those rows
avoids redundant database work without replacing `claim/1`'s durable cross-node
lock/token checks. Notification and mutation occupancy remain independent.
