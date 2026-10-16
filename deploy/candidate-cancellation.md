# Explicit withdrawal of an exact Actions candidate

The tau-ceti-bors App5172678 and installation167441900 were inspected live on
2026-10-16. Both have Workflows:write and Contents:write, and neither has
Actions permission. A fresh installation token can read public Actions runs.
The cancellation endpoint requires Actions:write. This implementation does not
change App permissions, credentials or repository settings.

The companion TauCeti main-defined `cancel-bors-candidate.yml` workflow supplies
Actions:write only to its cancellation job. It has a dedicated repository_dispatch
subscription, authenticated bot sender gate, workflow-pinned sparse tool checkout,
no candidate code, and no publication credentials. Its human-owned paths require
human review before landing. Deploying Bors alone cannot exercise that workflow;
a new genuine cancellation dispatch after the companion lands is required.
Before dispatch Bors reads both reviewed workflow/helper blobs from main and
requires their exact Git blob IDs. An absent or changed sink retries within the
same expiry without emitting a dispatch. Update the pins with reviewed companion
changes; mismatches disable dispatch rather than silently trusting new code.

Bors persists `cancel_run` in the existing durable outbox in the transaction that
marks an explicitly withdrawn running batch canceled. It captures the repository,
batch, immutable candidate ID/generation, staged head, tested base, branch and
recorded dispatch time. Permanent deduplication survives completion and restart.
Only explicit membership withdrawal queues this operation; CI failure, backend
handoff and speculative descendant invalidation do not grant cancellation authority.

The mutation lane discovers the target with one request-scoped Actions page,
limited to100 runs since its recorded dispatch. A truncated or ambiguous response
cannot authorize cancellation. The run must match the exact title emitted by
pr-build, repository ID/name, workflow ID/path, repository_dispatch event, bot ID,
login/type for both actor and triggering actor, creation window and run_attempt1.
`run.head_sha` is the workflow SHA for repository_dispatch and is deliberately
not treated as the candidate SHA. The journal must bind that dispatch title to
exactly one construction generation; same-head/base rebuilds with indistinguishable
titles are refused. The current generation and single dispatch-request record must
still match. The first accepted run ID/attempt is journaled durably and cannot be
replaced by a later lookup. Reruns are refused.

Immediately before sending the cancellation dispatch, the GitHub adapter rereads
durable authority after token acquisition and mailbox wait. Bors exposes that same
read-only authority at `/api/candidate-cancellations/:intent_id`, with no-store
headers and404 for revoked, expired, completed or missing intents. Promotion intent,
promoted candidates, active/requeued batches and changed generations revoke it.
The trusted helper validates the dispatch and reads the bound run twice, then
rereads Bors authority immediately before POSTing normal cancellation. There is
no force cancellation, VM mutation, candidate checkout or required-status write.
GitHub's ordinary cancellation preserves its normal always()/cleanup behavior;
this change leaves publishers/finalizers and build/lint/axiom/cache trust untouched.

GitHub does not offer an attempt-conditional cancellation endpoint. The helper
refuses every observed changed attempt and performs the last run read immediately
before its final authority read and cancellation request. A human rerun racing
between that read and POST cannot be fenced atomically by GitHub's run-ID API;
this API limitation must not be represented as a stronger guarantee. No automatic
reruns are introduced by this change.

A dispatch204 is an acknowledgment, not a CI-terminal observation. Bors retries
through the durable outbox with60/120/240/480/960-second backoff, at most six
claims and an absolute30-minute authority expiry. Missing runs and transient API
errors retry within those bounds, allowing cancellation before run discovery.
Completed/cancelled/failed runs terminate delivery without another cancellation.
Malformed/ambiguous binding, changed run identity, denied/missing API resources
and revoked authority fail closed. Exhaustion is retained as
`cancellation_exhausted` on the outbox row; terminal and request observations are
also candidate journal events. A crash retains the existing lease and is reconciled
by normal outbox recovery. Known204 acceptance is journaled and only reconciled,
never redispatched. Ambiguous API/worker failure allows at most one replay (two
send attempts total); remaining claims only reconcile. Thus one accepted dispatch
does not emit six redundant workflows while the target is still running. No HTTP
runs inside the cancellation transaction.

Release requires both normally reviewed upstream PRs, required exact-head CI,
actual source/image/instance/health evidence, and a NEW natural withdrawal.
The coordinator owns staging run watches. Do not synthesize a production PR,
withdrawal, approval or dispatch to obtain acceptance. Record cancellation
request-to-CI-terminal and runner-retirement-to-online timing only where observed;
coordinate Fleet classification with#17125. Keep TauCeti#17167 open until that
acceptance passes. The old868 build failed298seconds after Bors canceled it;
that elapsed lifetime is neither measured CPU nor evidence of saved compute.
