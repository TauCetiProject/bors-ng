# Exact source compatibility and bounded worker repair (#16813)

This reuses the `needs-rebase` projection and authenticated `tauceti-rebase:v1`
current-head marker. The exact Bors App identity is accepted by Worker#266 and
Review#176. No new repair queue, service, mandatory status, compiled artifact or
publication credential is introduced. A green head status stays green.

The direct rebase worker integrates one current main snapshot, repairs actual
imports/APIs/proofs, passes build and axiom checks, then compare-and-swap pushes.
A mechanical update-branch followed by red-CI repair would first push the known
incompatible tree and consume another build. The direct path does both in one
claimed, budgeted task. Rebase alone is sufficient only when validation passes.

## Cheap checks and scheduling

`LocalGit.compatibility` reads immutable Git objects as data. Each singleton
prospective merge tree contains main plus that PR, including PR-added modules.
It checks textual conflicts and every project-local source header, including
unchanged dependencies. Missing-import pairs already present on main are not
attributed to a PR. A conservative parser supports module/private/public headers,
multiline imports and nested/line comments. Unknown syntax, non-text, unsupported
paths, symlinks, limits or incomplete objects yield explicit unknown coverage and
retain authoritative validation. This is a local-source scheduling check, not
Lean proof or general header validity authority. The trusted Lean-header CI and
all build/audit/policy/pin/size guards remain mandatory. External Mathlib inventory,
import cycles and declaration-name grep conviction are out of scope.

Parsed headers are reused only by immutable Git blob plus `local-source-v1`
checker policy. Each new base still inventories the exact main and prospective
trees; passing results are keyed to full main/head/policy identity and never
relabelled fresh across main commits. Bump the policy/cache version when changing
the parser semantics. Helper limits: 30 seconds total, 32 selected heads, 4 MiB
per inventory/batch read, 256 KiB per source, existing 2 GiB mirror budget and
root flock. It executes no candidate code and writes no remote refs.

Successful promotion wakes one deduplicated existing project Outbox operation.
The existing heartbeat recovers missed/out-of-band main advances (five-minute
main reconciliation; bounded backlog chunks progress approximately once/minute).
Four queued/ready heads per chunk use a durable rotating project cursor. Repeated
notifications coalesce; latest actual main is read by the executor. Main moving
mid-check discards results while retaining cursor progress and a follow-up.
Background chunks use the existing ordinary priority class; dispatch/promotion
and withdrawals retain priority. The existing durable eight-critical-claim fairness
rule guarantees bounded background progress without changing scheduler priority.
Selected heads
get a bounded just-in-time check against the frozen actual base before candidate
construction/reservation; they do not wait for the backlog sweep. Speculative
successor bases, explicit bundles and excess/unknown coverage retain existing full
candidate validation and diagnostic fallback; no multi-PR group is convicted.
Current singleton and tracked-root candidates therefore no longer bypass a
conclusive individual check. Existing tracked partition/reservation logic remains
unchanged. Existing main policy/pin/size preflight guards still apply at admission
and authoritative CI; the background pass does not claim to reimplement them.

## Request and withdrawal lifecycle

A concrete failure records exact evidence and queues one existing durable notice
per head/evidence base. The notice carries importer/module and tested main, or the
link to a genuinely conclusive exact single-PR CI build. Only trusted individual
compiler/build evidence triggers API/proof/duplicate repair; suspect partitions,
quarantine labels, timeouts and infrastructure failures cannot. Automatic Mathlib
bumps retain their existing dedicated recovery path.

Admission removes the diagnosed current head before reservation and retains
innocent member head/reviewer/manual approval snapshots. Background withdrawal
uses the existing serialized cancellation path. Hold/human labels suppress worker
requests and withdraw diagnosed held heads; untended/unwritable heads require
their author/maintainer rather than new permissions. Worker ownership, fork scope,
claims, CAS push, caps/backoff and fresh-head CI/review remain unchanged. Durable
candidate events expose exact cheap coverage and unknown reasons; labels do not
represent a passing validation.

A new head supersedes the old request. If another main restores local-source
compatibility, the executor cancels an undelivered local notice or edits only its
own local-source marker, removes the projection and re-offers ordinary automatic
admission. It mutates no PR source, recreates no human approval and does not cancel
an authoritative individual compiler request or another App's request. History
coverage is bounded to fewer than 100 comments/four owned matching requests;
incomplete/API-failed resolution retains the known request and retries through
bounded reconciliation, requiring maintainer attention if coverage stays unknown.

## Evidence, rollout and rollback

`source-compatibility-evidence.json` is read-only historical batch727 evidence:
exact head dedc6f8a9464710a0ab92fa76475942b7eb16d31 on main
5fa17650a559a7bd9764fcc11cb306d8f8edf286 finds NonsplitReduction importing deleted
VariableChange. Cold pass: 7.68 s wall / 4.23 s helper CPU. Parsed-inventory reuse:
0.23 s / 0.10 s. These local measurements do not claim production latency or
throughput improvement and involve no build, push, status or PR mutation.

Deploy Worker#266/shared facts, Review#176 and its trusted TauCeti#16818 pin;
TauCeti#16818 requires human review and normal trusted CI. A NEW merge-sweep
dispatch after the pin lands must exercise it. Deploy this bors companion normally
with its additive map migration. `BORS_SOURCE_COMPATIBILITY=off` stops new checks
and handoffs while retaining authoritative staging validation and existing valid
repair requests. Git/header caches are disposable and contain no compiled PR
artifacts. Roll back code without dropping historical evidence columns.

Acceptance is still pending: genuine deployed request, actual tending-worker
pickup and validated changed-head outcome, normal new-head CI/review/re-admission,
production latency/cost and exact innocent membership comparison. Do not claim
rollout-verified or close #16813 on fixture/PR/CI evidence alone.
