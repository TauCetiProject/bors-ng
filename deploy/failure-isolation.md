# Failure isolation and quarantine

Failure evidence and retry decisions append to the existing candidates/v1 journal.
There is no data migration or historical inference. Existing observation/v1, queue
intent, approval, staging, promotion and cache publication gates remain authoritative.

For trusted dispatched staging, bors binds the latest failing status creator and
run URL to the exact repository, branch, batch, head, base and workflow. Cancellation
retains its existing native sealed retry and backoff. For completed runs, one bounded
GitHub jobs request names the exact run attempt. Canceled runs and known primary
setup/cache failures are infrastructure. A green sandbox followed by publisher or
finalizer failure is also infrastructure, not individual proof evidence. Verdict
reporters deliberately fail for red build/configuration verdicts too: a
reporter-only failure therefore retains ordinary deterministic bisection.
An entirely green workflow with a red required scope status also retains native
failure isolation: the scope status remains an independent merge gate.
Bump cache completeness/archive compatibility failures also bisect. Driver/watchdog
and dependency fetch failures are infrastructure only when the candidate pin
validator did not run; on a bump they can be deterministic compatibility failures.
A known failed candidate validation/build step
permits ordinary failure isolation even if a sibling job is canceled. Terminal
timeouts retain native failure semantics. Unknown terminal failures retain
ordinary deterministic bisection, so a renamed step cannot silently cause
unbounded whole-batch infrastructure retries. Incomplete/mismatched job pages
retain intent and back off. No candidate-written metrics, telemetry, logs or artifacts classify
the failure. Optional [candidate-bound compiler diagnostics](compiler-diagnostics.md)
can subsequently guide retry scheduling, with no proof or merge authority.
A failed offline build includes the required audits; the classification does not
claim that a particular theorem or proof is wrong.

The trusted local Git helper reads immutable source blobs as data. It records
per-head changed paths, simple local import edges, missing provider paths and
direct dependency paths. It never executes a candidate parser. Its deliberately
limited header reader accepts ordinary imports, module headers and nested comments;
unusual syntax, resource limits or read errors produce unknown diagnostics. The
workflow-pinned Lean preflight remains the source validity authority. For
successful construction, header/provider reads use the exact assembled tree,
so a provider supplied by another member is present and one removed by the base
is absent. Successful assembly reuses the exact ordered squash chain to validate
each parent/tree with one batched commit read. Ownership compares each immutable
head with its fork point against the frozen base, retaining inherited stacked and
identical edits that reappear when heads are retried separately. Git lacks a
pairwise merge-base batch interface, so ancestry still uses one bounded query per
member; all head tree diffs run in one process. Conflicting construction has no
assembled chain and reads optional imports at exact member heads under the same
total limits, preserving conflict quarantine/reconsideration behavior.
These are interaction hints, not failure verdicts.

Diagnostics get at most 15 seconds within the existing assembly deadline, 256
changed paths per member (1,024 total), 128 KiB of serialized ownership, 256 source
blobs total, 256 KiB per source, 3 MiB of source content and 256 KiB of total
serialized hints. Optional dependency collection uses a separate five-second cap
inside that same 15 seconds, one bounded tree inventory and one batched blob read.
The helper's existing subprocess output/mirror budgets still apply. Unavailable
diagnostics cannot fail assembly; the helper restores its outer deadline afterward.
`ownership_complete` and `dependencies_complete` are independent. Optional limits,
timeouts and ambiguous headers retain complete owners but publish no partial
dependency or missing-source findings. Only complete optional data can trigger
missing-import preflight or automatic quarantine reconsideration. Located compiler
errors can guide conservative retry partitions using complete ownership alone.
Their hints must match the journal's exact candidate/generation/base/digest,
commit/tree and ordered heads. Unknown locations, ownership or log collection keep
the existing fallback. Old complete journals remain readable; old incomplete
journals are never upgraded by inference.
Paths outside the supported ASCII path subset, including non-source paths with
spaces, conservatively make ownership unknown. Shadow-mode local objects cannot
guide terminal compiler isolation when the authoritative GitHub commit differs;
only hints matching the actual constructed commit/tree are consumed.

Retry selection uses atomic bundle units, immutable head identity, import provider
relationships and shared changed paths. Missing-import components are tested apart
from unrelated units; structured local conflicts isolate the offending unit;
otherwise a connected component can be tested apart from the rest. Each diagnostic
partition strictly shrinks the failed unit set and retains every member exactly
once. A single-only member takes its whole bundle into a separate retry. Unusable,
ambiguous or non-narrowing evidence falls back to deterministic bundle-aware
bisection. Nonlocal/disabled diagnostics and lone conflicts keep the existing
mergeability/bundle isolation semantics.
Split ordering follows the immutable candidate's recorded assembly inputs;
historical batches without a snapshot use durable link IDs. Unspecified SQL
row order cannot alter the split after restart.
Retries use the existing sealed clone/admission path, preserve exact heads,
reviewer/manual approval, requested priority and lineage, and reconcile replayed
children after restart. Multi-member diagnostic suspicions never revoke approval.

## Sending one suspect to repair before confirmation

When a conclusive failed candidate's complete, immutable diagnostic ownership
selects exactly one unbundled PR, bors may send that head to the existing
`needs-rebase` worker path instead of immediately dispatching a singleton
confirmation run. The request says **provisional suspect**, not individual build
failure. It includes the actual candidate, generation, run/attempt, diagnostic
paths, exact head and original tested main, under `provisional-diagnostic-v1`.
No existing head-CI status is changed. Generic bisection, multiple/unmapped
suspects, connected multi-member components, tracked train reservations, manual
scope approvals, pause labels, stale main/head, withdrawals and unavailable API
bindings retain the conservative retry path.

Parent retirement, all remainder admissions, suspect withdrawal and durable
notice are committed together. The remainder keeps exact heads, approvals,
original scheduling age and parent lineage. Duplicate notifications/restarts do
not create a second suspect admission or repair request. A failed notice delivery
uses the existing outbox retry; labels alone authenticate nothing. Worker ownership,
fork permissions, claims, per-PR attempt caps, full build/axiom validation and
compare-and-swap push remain mandatory. This feature grants no ownership exception.

A worker must report attribution **unconfirmed** if this PR passes alone; no-change
exit or cheap import PASS is not a validation receipt. There is no new worker
receipt service. If no changed head is observed after the existing default
90-minute worker round plus five minutes' grace, ordinary bounded source
reconciliation can clone **one sealed isolated fallback** from the original
admission. Fresh head/main/open/pause/withdrawal checks and normal dispatch gates
still apply. This delay is a recovery tradeoff, not a guaranteed completion time;
fair reconciliation and higher-priority dispatch may defer it further.
The authenticated request remains active for an in-flight worker; its claim and
safe push are not invalidated. The fallback cannot combine with the original
remainder or be renewed as main advances. A clear local conflict or missing-source
finding still prevents dispatch. Actual isolated failure upgrades provenance to
individual evidence and publishes its run, without renewing the worker budget;
success follows normal promotion. A changed head supersedes the old request and
requires normal fresh CI/review. No confirmation-run saving is claimed until a
genuine production case demonstrates it.

Inspect `provisional_repair` and `provisional_unconfirmed` in the existing candidate
journal and the exact-head `provisional` metadata for the consumed fallback batch.
`BORS_SOURCE_COMPATIBILITY=off` stops new handoffs/reconciliation; already committed
requests and admissions retain their existing recovery semantics.

A genuinely failed singleton or indivisible bundle retains native terminal failure
semantics. Its failed queue approval now includes an explainable quarantine record:
head, immutable failed base, candidate ID, reason, changed/import dependency paths
and reviewer. The journal records `head_quarantined`; this does not identify an
individual culprit within an inseparable bundle. Head changes cannot reuse the
old quarantine or approval.

For ordinary automatically reviewed source PRs, a new eligible and review-safe
App decision may reconsider an unchanged quarantined head when an exact-base
GitHub comparison finds a changed relevant source/dependency or Lake/toolchain
path. The base must be a descendant of the failed base; comparisons of 300 or
more files are uncertain and cannot authorize a retry. Unknown/incomplete
quarantine diagnostics hold. Dependency paths describe direct imports of changed
sources; unobserved transitive dependencies remain unknown and may require manual
reconsideration. Reconsideration is recorded, then normal live head,
review policy, labels, scope, merge-base, CI and admission checks still apply.
It does not restore approval by itself. Explicit withdrawals, human scope approval
and terminal bundles cannot be overridden. Existing bounded bump recovery remains
authoritative for bump PRs and is unchanged. Same-base and repeated same-base
reconsideration are blocked; successful admission persists the reconsidered base
and count, capped at three automatic reconsiderations per head. Duplicate
head/base reconsideration evidence is recorded once when admission is blocked.

The default rollout enables conservative hints for newly assembled local candidates.
Set `BORS_FAILURE_DIAGNOSTICS=off` in the bors container environment and restart to
disable collection, diagnostic partitioning and automatic reconsideration. Trusted
failure classification, native infrastructure recovery and existing singleton failure
semantics remain. Existing candidates keep their immutable journal facts. Inspect
`failure_classified`, `failure_partition`, `head_quarantined` and
`quarantine_reconsidered` through `/repositories/1/candidates?base=main&batch_id=...`.
`queue_authority=false` still applies; no journal evidence grants review, merge or
cache publication authority. No workflow pins, publisher secrets or compiled cache
producer trust change.

## Historical source-only rehearsal

Candidate 213 / batch 790 / generation 10 is a 54-member diagnostic replay of
[run 38824781960](https://github.com/TauCetiProject/TauCeti/actions/runs/38824781960).
The original helper timed out and used deterministic bisection. With the exact
recorded assembly chain and heads on a local warm Git repository, ownership took
141 ms and all hints took 547 ms. The one located primary compiler error maps to PR #16883;
the known direct-import component keeps #16897 with it, yielding groups of 2 and
52. All 54 original snapshots are preserved. Bad-import/cancellation cascade lines
do not establish additional missing-source findings. With optional dependencies
unknown the direct owner still permits a retry-only 1/53 partition. Neither replay
proves individual guilt, a live CI outcome, or improved accepted throughput.

To reproduce using a single-candidate journal capture, a local bare repository
containing its objects, and the captured job log:

```sh
python3 scripts/replay_diagnostic_ownership.py candidate.json replay.git job.log replay.json
MIX_ENV=test BORS_DATABASE=postgresql mix run --no-start scripts/replay_diagnostic_partition.exs replay.json partition.json
```

Use `--gh-log` for a `gh run view --log-failed` capture with presentation columns.
The replay only reads Git objects and writes the named report files; it never
fetches, starts the application, changes the queue or dispatches CI. Measured
timings are local warm-repository results, not production latency promises.

[Recorded immutable inputs and results](failure-isolation-evidence.json) cover
batches 639 (15 PRs), 634 (9), 631 (52) and 622 (2). Direct diagnostics completed
in 0.6–2.0 seconds locally. At batch 631's exact tested tree
`d47d151d948453dad5e41ec3d4d9bade150baa54`, they identify PR #15876's
`PathBimodule.lean` import of the absent `Moore/Between.lean` provider, matching
[the historical failed run](https://github.com/TauCetiProject/TauCeti/actions/runs/38462342074).
The provider is checked at the assembled tree, not the PR's old base/head.
Batches 634 and 622 supply no direct import/path interaction explaining their
proof failure; ordinary deterministic bisection remains the fallback. These
are source diagnostics, not proof replay or production latency promises.
