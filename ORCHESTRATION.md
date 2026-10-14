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

## Operation and rollback

Inspect `orchestration_jobs` for kind, project, state, attempts, available_at,
lease_until and sanitized last_error. Pending/running rows are work intent;
candidate diagnostics remain evidence with `queue_authority=false`. Completed
dispatch/notification keys are retained as deduplication tombstones. Payloads are
internally produced Erlang terms decoded with `[:safe]`; they contain installation
references, never installation tokens or publication credentials.

Before disabling the feature, stop new admission and drain pending/running outbox
operations; then restart with the flag false. Do not switch execution modes live
or discard pending withdrawals. Application rollback leaves additive tables and
the approval token column intact. Existing queue observations and candidate
schemas are unchanged. Cache producer trust, trusted workflow pins, publisher
secrets and all build/lint/axiom gates are unchanged.

The bounded time filters use GitHub's documented [workflow run `created` filter](https://docs.github.com/en/rest/actions/workflow-runs#list-workflow-runs-for-a-workflow)
and [issue comment `since` filter](https://docs.github.com/en/rest/issues/comments#list-issue-comments).
