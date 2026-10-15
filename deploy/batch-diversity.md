# Roadmap diversity: retain current scheduling

[Issue #16887](https://github.com/TauCetiProject/TauCeti/issues/16887), following
[the deployed #16244 advisory](https://github.com/TauCetiProject/TauCeti/issues/16244).
This release collects scheduling evidence and ships an **offline, disabled**
fresh-formation planner. There is no live planner call, environment switch or
change to concurrency, batch cap, speculative budget, approvals or publication.
The current policy remains in production. No live throughput gain is claimed.

The useful immediate change is bounded admission evidence from label reads that
preflight already performs. No extra GitHub requests or Lean builds are made.
New journal snapshots record exact local project/PR/head, observation time,
read source, explicit Unix-second timestamp units, priority, singleton status and labels. The record explicitly says
that labels are mutable PR metadata, not an atomic head/label response. These
facts grant no approval, merge, source-compatibility or cache authority.

## Evidence and decision

The committed [fixture](../test/fixtures/diversity/workload.json) reduces the
[#16244 eligibility archives](https://github.com/TauCetiProject/TauCetiCI/tree/92e8b391eeec7334fd1c7d976e6006b12b7579dc/records/observations/TauCeti)
and [the roadmap investigation](https://github.com/TauCetiProject/TauCeti/issues/16226#issuecomment-6181540526).
It contains 891 observed head versions in 1,428 snapshots ending Oct 14 22:42 UTC,
including 240 initially present heads with unknown prior age. New first-observed
heads arrive at approximately 27.14/hour; this is eligibility, not admission.
Fourfold time compression tests approximately 108.55/hour without duplicating
heads or raising concurrency. A missing item in a bounded scan is not treated
as a withdrawal. Explicit not-ready observations and changed heads invalidate
waiting inputs. First-seen admission bounds are the preceding/current sample.

The labels were collected retrospectively at Oct 15 00:27 UTC: 571 exact heads
match metadata, and 538 have a usable roadmap area. Strict as-of replay permits
only hints available at admission and unexpired at formation. **All 80 strict
scenarios select the FIFO reference membership.** Retrospective full-workload
comparisons also fall back on coverage uncertainty. A separately named
retrospective known-label subset tests the scoring direction; it is neither
prospective replay nor a representative filtered production workload.

[Summary JSON](batch-diversity-evidence.json) includes 240 scenarios, input/trace
SHA-256, cohort wait observations, reference comparisons and trace composition
digests. Full traces include every selected exact head, actual area composition,
size, imputed work, dispatch time/wait, fallback and unobserved failure cause.
Counterfactual accepted PRs/hour, retry amplification, runner minutes/accepted PR
and CPU/accepted PR are **null**, with an explicit reason. Assigning failures to
all pairs in failed groups would fabricate an oracle. There are no failure draws.

For the diagnostic known-label subset, upper admission bound, fixed **assumed**
600-second serial service and one **imputed** work unit/member:

| Cap | Arrival multiplier | Policy | Formations | Same-area pairs | p90 dispatch wait, min |
| --- | --- | --- | --- | --- | --- |
| 8 | 1 | FIFO | 133 | 55 | 51.55 |
| 8 | 1 | diversify | 133 | 23 | 56.51 |
| 8 | 1 | group | 133 | 224 | 51.56 |
| 16 | 4 | FIFO | 36 | 163 | 54.63 |
| 16 | 4 | diversify | 36 | 32 | 78.88 |
| 16 | 4 | group | 36 | 1137 | 72.88 |
| 128 | 1 | all five policies | 132 | 170 | 8.02 |
| 128 | 4 | all five policies | 36 | 274 | 9.12 |

All rows in this table attempt the same 538 heads with no invalidations. Reduced
concentration can worsen tail wait; at the production cap of 128 the membership
sets agree. These are formation/service sensitivities, not CI throughput or
runner costs. Lower caps cannot be justified by this table: fixed startup cost,
real work, accepted outcomes and isolation/retry cost are unidentified. Soft
caps 2 and 3 are also included, always with a fill-preserving fallback.

The observed 84 candidates contain 19/155 same-area exact-head pairs with shared
changed-file/import-edge hints versus 4/3435 different-area pairs. This is
proximity, not pair-caused failure probability. Successful candidates also
contain connected same-area pairs. Size/time/observed admission-age controls
and shared-exact-head lineage components are recorded in the summary; physical
work control is unavailable. Repeated lineages are not independent trials.
The two temporal halves and PR-partitioned lineage holdout waits are descriptive;
no parameters are trained on future outcomes or tuned on either holdout.

The [pinned interaction reproduction](batch-diversity-interaction.json) verifies
both ModularCurves heads merge individually onto the recorded base, then conflict
in three files when combined. The [Git-only script](../scripts/reproduce_diversity_interaction.py)
reproduces it without a checkout or compilation. Its result applies only to
those heads and that base. Once either lands, the other can remain incompatible
with main; separation does not repair that incompatibility. Changed files and
import edges are therefore diagnostics here, never dependencies or an oracle.

## Planner and preservation rules

`Diversity.select/2` defaults to FIFO unless explicitly enabled by its offline
caller. It accepts already eligible fresh atomic units for one repository/base
lane, refuses immutable inputs and duplicate PR membership, and compares FIFO,
soft marginal shared-area-pair penalty, grouping and soft caps 2/3. It anchors
the oldest unit at the highest **exact** priority, scores at most 256 units, never
fills across priority boundaries and never waits for diversity. It preserves
atomic bundles and singleton isolation; an oversized oldest unit runs alone.
It reverts to the reference formation if alternative atomic packing underfills.
Selected units retain FIFO relative construction order. Count and supplied work budgets apply to every policy; physical work is unknown
in this captured workload. No labels or import edges create bundles/stacks.

Each reference-eligible unit can be bypassed at most three formations for the
policy score. The caller supplies returned counters; a due reference unit forces
FIFO fallback if scoring omitted it. Unavoidable atomic packing gaps are not
policy bypasses. Independently, every formation serves the oldest eligible unit:
a unit with r older same-priority units waits at most r+1 service opportunities
once higher-priority work drains. Elapsed bounds require bounded service and
bounded higher-priority arrivals; overload/outages cannot guarantee wall time.

Missing/none/Unknown areas, mismatched provenance, stale (>900 seconds) or
future metadata anywhere in the lookahead revert the window to FIFO. Multi-label
members count a shared member pair once. The process-local scratch buffer holds
at most 256 observations, expires on restart, and is expendable. It records read
receipt time before other preflight calls. Admission journals persist copies;
retry admissions copy the original scheduling record, never newer labels.
Legacy/missing facts remain explicitly unknown. Runtime mutators, eligibility,
withdrawals/reapprovals, source holds, intent fencing and constructed/sealed
memberships continue through their existing code, with their existing tests.

## Reproduction

In a configured bors checkout, with normal dependencies installed:

```sh
MIX_ENV=test BORS_DATABASE=postgresql mix run --no-start scripts/replay_diversity.exs \
  test/fixtures/diversity/workload.json /tmp/diversity-replay.json
python3 scripts/summarize_diversity.py \
  --workload test/fixtures/diversity/workload.json \
  --replay /tmp/diversity-replay.json --out /tmp/diversity-summary.json
```

The command starts no database, worker or GitHub client. To regenerate the
fixture, `scripts/capture_diversity.py --records CHECKOUT/records --evidence DIR
--out OUTPUT` accepts a checkout of the pinned TauCetiCI commit and the four
supplementary issue inputs (metadata, investigation, exact-head reproduction and
candidate-journal capture). Every archive path/hash is included; the committed
fixture makes the experiment portable without private paths. The summary
includes fixture and trace digests. Tests cover deterministic selection/replay,
priority, fairness, fill/work, atomicity, immutable retries, metadata provenance,
restart, process isolation and rollback; existing full queue suites exercise
concurrent admission, withdrawals, reapproval and source/intent fencing.

## Rollout and next canary gate

Deploy this evidence-only release with scheduling disabled. Verify the exact
merged revision, health, and **new** admissions carrying scheduling facts; inspect
new candidate membership and unchanged sealed-retry ancestry. No active candidate
is repacked and no admission is delayed. Missing hints remain safe after restart.
The evidence does not support enabling a scheduling canary in this release.

If admission coverage and trustworthy work features later permit a separately
reviewed integration, compare paired blocks of at most six new formations per
arm, one serial lane, at the existing cap/work budget and exact priority. Anchor
oldest units, persist bypass counts or fall back to FIFO on counter loss, and
expire the policy after 12 new formations or two hours, whichever comes first.
Do not recruit retries, existing memberships, singletons or explicit bundles to
change policy exposure; their existing semantics must be retained. Record exact
heads/base/areas, queue wait, formation fill/work, dispatch latency, accepted
PRs/hour, failure causes, retry amplification and CPU/runner minutes per accepted
PR. Stratify arrivals, age, work, cache state and priority, and expose unknowns.

Stop immediately on stale approval/head, membership/intent/source invariant
failure, any fill delay/fragmentation versus the reference, bypass-limit breach,
or a >10% increase in matched p90 wait or runner minutes/accepted PR. API/metadata
uncertainty, expiration or rollback must resume FIFO for **fresh** work; sealed
retries and constructed membership remain immutable. An inconclusive or harmful
paired result keeps the flag off. No 24-hour baseline gate is required and no
review, exact-candidate CI/audit or publication gate may be bypassed.
