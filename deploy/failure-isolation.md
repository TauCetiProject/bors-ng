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
retain intent and back off. No candidate-written metrics, telemetry, logs or artifacts are used.
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
is absent. Conflicting construction has no assembled tree and uses exact member
heads. These are interaction hints, not failure verdicts.

Diagnostics get at most 15 seconds within the existing assembly deadline, 256
changed paths per member (1,024 total), 256 source reads total, 256 KiB per source
and 256 KiB of serialized hints. The helper's existing subprocess output/mirror budgets still apply. Unavailable diagnostics
cannot fail assembly or consume its remaining deadline. Old candidates without
these fields retain deterministic bisection.

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
