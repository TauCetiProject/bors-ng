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
attributed to a PR. A conservative parser follows Lean's one-module-per-import header grammar,
including module/public/meta prefixes, multiline whitespace and nested/line
comments. Private declarations are body commands; private imports and escaped
names retain unknown coverage. Body macros cannot become alleged imports. Unknown syntax, non-text, unsupported
paths, symlinks, limits or incomplete objects yield explicit unknown coverage and
retain authoritative validation. This is a local-source scheduling check, not
Lean proof or general header validity authority. The trusted Lean-header CI and
all build/audit/policy/pin/size guards remain mandatory. External Mathlib inventory,
import cycles and declaration-name grep conviction are out of scope.

Parsed headers are reused only by immutable Git blob plus `local-source-v2`
header-parser policy. Scheduling results use `local-source-v3`, so old timeout
results are revisited lazily with the existing fair cursor after this rollout.
The parser/cache itself is unchanged. Each new base still inventories the exact main and prospective
trees; passing results are keyed to full main/head/policy identity and never
relabelled fresh across main commits. Bump the policy/cache version when changing
the parser semantics. Helper limits: 8 seconds for ordinary background turns, 30 seconds
for selected-head admission, 32 selected heads, 4 MiB
per inventory/batch read, 256 KiB per source, existing 2 GiB mirror budget and
root flock. Previous exact Git pins remain until the next fetch succeeds, so
Git can reuse the downloaded history during negotiation. Only then are the old
pins atomically replaced with the verified new inputs; a failed fetch preserves
them. An empty mirror may use the existing 30-second allowance once per
object generation to complete its initial fetch; a consumed marker prevents a
failed download from claiming it again for another PR/main revision. Later
background checks stay at eight seconds. If that bootstrap fails, background
reports explicit unknown without repeating the cold fetch until ordinary
admission/assembly seeds the mirror or the mirror is recreated. A successful
fetch resets the marker for a future object generation emptied by GC/eviction.
The deadline starts at helper construction; Python startup and final cleanup add
overhead. Git-header reads are bounded by listed source byte lengths (1 MiB/512
objects per batch). Timeout evidence retains phase/operation and a preceding
base-header failure, without raw Git diagnostics. It executes no candidate code
and writes no remote refs.

Successful promotion wakes one deduplicated existing project Outbox operation.
The existing heartbeat recovers missed/out-of-band main advances (five-minute
main reconciliation; bounded backlog chunks progress approximately once/minute).
One queued/ready head per turn uses a durable rotating project cursor, inspecting
at most 16 lightweight metadata rows before yielding. Stored identities are
compared as decoded values, avoiding JSON string quoting/collation differences
between PostgreSQL and MySQL 8.4. Repeated
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
per head/evidence base. Partial label publication records a pending projection and
retries it from the saved diagnosis without duplicating the request or source
work. The bounded notice carries up to eight importer/module findings and tested main, or the
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
Retries reuse the exact cached passing inventory and only retry request/label
resolution; they do not repeat Git. Held findings also stay cached for identical
inputs. If a hold is removed, admission revalidates the live head/main and labels
before requesting repair from that existing diagnosis, without another source
scan. Neither cache path can clear an authoritative individual build request.

## Evidence, rollout and rollback

`source-compatibility-evidence.json` is read-only historical batch727 evidence:
exact head dedc6f8a9464710a0ab92fa76475942b7eb16d31 on main
5fa17650a559a7bd9764fcc11cb306d8f8edf286 finds NonsplitReduction importing deleted
VariableChange. Cold pass: 7.28 s wall / 4.11 s helper CPU. Parsed-inventory reuse:
0.20 s / 0.11 s. These local measurements do not claim production latency or
throughput improvement and involve no build, push, status or PR mutation.

Deploy Worker#266/shared facts, Review#176 and its trusted TauCeti#16818 pin;
TauCeti#16818 requires human review and normal trusted CI. A NEW merge-sweep
dispatch after the pin lands must exercise it. Deploy this bors companion normally
with its additive map migration. `BORS_SOURCE_COMPATIBILITY=off` stops new checks
and handoffs while retaining authoritative staging validation and existing valid
repair requests. Git/header caches are disposable and contain no compiled PR
artifacts. Roll back code without dropping historical evidence columns.

Whole-helper failures (including bounded fetch/lock timeouts) are persisted as
explicit exact-main/head/policy unknown results, with an allowlisted reason.
They never create a request/hold or turn into a passing result. Authoritative
staging remains available; the same inputs do not trigger an unbounded retry
loop. Later main/head identities are checked normally. The first production
chunks advanced their cursor without storing these failures; this observability
gap was caught during rollout rather than reported as a successful source pass.

Production background turns exhausted the original 30-second helper budget
(31.745–32.722 seconds total observed service) without conclusive coverage.
Background turns now use an 8-second helper budget, including Git and root-lock
waits, to yield the mutator sooner. GitHub/token/metadata overhead is additional;
this is not a claim of a hard 8-second total operation deadline. Selected-head
admission retains its separate 30-second budget when source work is needed. An
exact cached unknown falls back to authoritative CI without a costlier cheap
retry. Held evidence waits for re-admission or changed main/head inputs; simply
removing a hold does not start a new Git scan. Cold cache timeouts stay unknown
and use authoritative validation; ordinary candidate assembly may warm the same
Git mirror. Validated immutable header entries are also retained atomically after
an interrupted partial inventory; a later turn can continue parsing other blobs.
Incomplete inventory still yields unknown, never a cached pass. No warm-cache
success or reduced production latency is claimed until
measured, and no extra warmup service, repeated CI or faster poll is introduced.

Acceptance is still pending: genuine deployed request, actual tending-worker
pickup and validated changed-head outcome, normal new-head CI/review/re-admission,
production latency/cost and exact innocent membership comparison. Do not claim
rollout-verified or close #16813 on fixture/PR/CI evidence alone.
