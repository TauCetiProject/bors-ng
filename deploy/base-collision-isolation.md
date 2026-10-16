# Direct declaration collisions against unchanged base source

Refs TauCetiProject/TauCeti#17106. Roadmap: none.

The exception preserves one directly located duplicate-declaration owner before
advisory import-component widening. It changes retry grouping, not merge or
individual-failure authority. The ordinary `compiler_diagnostics` route remains
in use, including the existing provisional singleton safeguards from #16813.

## Exact rule

Every observation in every completely retrieved failed build-job log must be
`duplicate_declaration` with the same unordered pair of distinct project paths.
Repeated observations of that same pair are allowed; another pair, an ordinary
compiler error, an unknown environment finding or an empty observation set is
ineligible. Both complete ownership and complete dependency metadata are required;
missing-import findings are ineligible.

Exactly one path has exactly one direct changed-file PR owner. The other has no
changed-file or attributed-dependency owner. Dependency/provider-only ownership,
shared collision paths, two changed owners, both paths owned by one PR, and
unmapped ordinary diagnostics retain existing handling. An approved bundle is
one atomic unit. The selected unit must not share any changed path with another
unit; such sharing retains existing component grouping.

A separate `collision-source-identity-v1` observation must prove:

- Both exact paths exist as regular blob files in the immutable candidate.
- The ownerless path exists at base with exactly the same blob and byte length.
- The owned path has a different candidate blob, or is absent at base.
- Every selected candidate/base blob was read completely within the source bound.

An absent ownership entry alone proves none of this. Source identities are
separate from weak provider evidence. Provider fields cannot supply this proof.

The original log, source result and ownership provenance must bind the same
candidate ID, generation, member digest, ordered PR/heads, base, commit, tree,
workflow, run, attempt and failed job IDs. Consumption checks the latest trusted
classifier, fresh run creation time, live parent commit/base, latest candidate,
and unchanged member heads/reviewers/manual approvals. Stale, incomplete or
unverifiable records retain existing conservative handling.

When eligible, the directly owned atomic unit forms the first planner group and
all other units remain in the remainder, subject to existing singleton scheduling
constraints. Advisory import edges cannot widen this one suspect; their neighbors
remain admitted and receive all current-base gates. Unrelated diagnostics disable
the exception. Other components do not establish individual blame. No queue
priority or child execution order is promised.

`compiler_diagnostics` may then enter the *existing* provisional handoff only if
all its current checks pass: correct project/main, enabled policy/outbox, no
manual approval, bundle, automatic bump, prior request, withdrawal or actual
speculative predecessor/successor, exact current main/head and unheld PR. Existing
atomic parent retirement, remainder admissions, durable repair/outbox, one-repair
and fallback budgets remain unchanged. Blocked handoffs use ordinary full-gate
retries; no new repair protocol or authoritative singleton conviction is added.

## Bounds and trust

`collision_sources` is an optional offline operation on the existing mirror. It
never initializes/fetches/checks out a repository, builds an index, interprets
headers, evaluates Lean, requests credentials, changes refs, writes source or
compiled caches, inventories the project or evicts mirrors. A busy or missing
mirror is immediately unavailable. Exactly two paths share one five-second total
deadline, including lock time; at most 256 KiB per unique blob, 512 KiB total
unique candidate/base source, 64 KiB per tree listing and 64 KiB result. Missing,
partial, unsupported, over-budget or expired inputs retain conservative handling.

## Real frozen replay, not a measured saved run

Candidate233/batch853 generation1, run38849701621 attempt1,
sandboxed-build116808025136, commit
`0f6ed531945489e7e33e1510e0cdb60c940a6f78`, base
`3924e3ef4afb8102b6a8e9893043c2c1a303f0d1`:

- Actual event3290 has one direct changed owner, #16504's new `Manifold.lean`.
  `ChartedSpace.lean` is identical to base, blob
  `068f488a6ff92ae87e5000a20a24284a601e20fa`.
- #16650 imports the already-existing `Complex/Chart.lean`, modified by #16504.
  This advisory edge is not evidence of stacking or a missing source dependency.
- Actual event3292 selected `[16504,16650]/33`, with children854/855.
- A **new local** source-identity assessment reads 16,759 unique source bytes;
  the planner counterfactual is `[16504]/34`. All 35 frozen snapshots are kept,
  including heads, reviewers, manual approvals, original ages, bundles,
  scheduling and lineage. #16650 remains in the remainder.

`test/fixtures/candidate233-base-collision.json` contains the real frozen inputs
and clearly labels the added local identity assessment. It does not retrofit an
observation into the historical journal. Reproduce with:

```
MIX_ENV=test BORS_DATABASE=postgresql mix run --no-start \
  scripts/replay_diagnostic_partition.exs \
  test/fixtures/candidate233-base-collision.json /tmp/candidate233-partition.json
```

Removing `collision_sources` reproduces the historical 2/33 baseline. Portable
source-helper tests use synthetic Git trees; those are separate from the real
journal replay. Local replay establishes neither a measured avoided build nor a
production handoff. Exact-head checks/review, automatic deployment image/health,
and a new naturally matching production outcome are required before closure.
