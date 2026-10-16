# Bounded import-provider retry coverage

Refs TauCetiProject/TauCeti#17103. Policy `bounded_depth_v2` permits a weak
`bounded_provider_hint` to guide retry grouping after precise, bound compiler
errors name only unchanged, unmapped project files. It does not establish an
individual failure. Direct diagnostic ownership remains a separate, stronger
route. The original direct lookup and legacy observations retain their contract.

## Coverage contract

Diagnostic roots have depth zero. A deterministic breadth-first traversal parses
all reachable project headers at depths zero through four, stopping expansion
immediately at each frozen changed-provider path. Providers discovered at depth
five are checked for blob availability and recorded as changed frontiers or
unexpanded depth-limit residuals. A root shared with another root is visited once
at its shortest distance. Cycles are guarded, imports are sorted and deduplicated.
Every root must reach at least one changed frontier for the mapping to guide a
retry. All frozen owners of all reached changed frontier paths are included.

`bounded_complete: true` means this declared search finished with every discovered
path accounted for. It deliberately allows unresolved deeper providers, including
other owners beyond the boundary. `complete: false` and
`full_closure_complete: false` remain explicit, even if the residual is empty:
this policy never certifies the entire import closure. `authority: retry_only`
and `proof_authority: false` accompany the evidence. A depth boundary is distinct
from a resource limit. Time, byte, header, path, output, missing-object or parser
failures never earn bounded coverage, even if an owner was already found. The
original `bounded_depth_v1` observations retain their existing semantics and are
never retroactively given a direct checkpoint.

The structured lookup records the versioned limits, roots, ordered graph nodes
with depths/blob identities/sizes, candidate/base blob equality for each expanded
unchanged file, imports, changed frontier, residual frontier, header/source-byte
counts, cache misses, elapsed milliseconds and Python CPU milliseconds. Error
results retain partial progress and pending paths when available; output overflow
explicitly marks evidence truncation. CPU excludes Git-child CPU. The binding
retains candidate/generation/commit/base/member digest/workflow/run/attempt/jobs.
The latest trusted classifier selects only its exact immutable observation. A
previous observation, including UNKNOWN, is never rerun or backfilled after a
policy change. Ownership/provenance/current frozen membership are checked before
reading Git. The consumer reconstructs the graph to reject partial or inconsistent
coverage before intersecting with complete frozen ownership.

## Complete direct checkpoint before deeper expansion

The v2 helper first parses **every** diagnostic root, verifies its exact base
identity, resolves **all** direct project providers and checks their ordinary
blob type, per-file size and availability. Only then does it record a separately
versioned `direct_checkpoint` (`direct_depth_v1`, `complete: true`) with the same
full candidate/run binding. It includes every root, sorted providers with blob
identities/sizes, base blob equality, total distinct paths, root header/source
accounting and completion elapsed time. A missing or malformed second root or
direct provider prevents completion; a discovered changed owner alone does not
suffice. The checkpoint itself fits within the combined evidence budget.

The deeper BFS reuses these root parses, queued imports and immutable metadata.
There is **one** helper/lock/deadline, not a second direct lookup after a cap.
Roots count once toward source/header limits; cache hits remain charged. Every
root/direct/provider/deeper path shares the existing 128-path bound. The 64KiB
limit covers the **combined** envelope, checkpoint and deep progress; partial
deep progress is explicitly marked truncated if necessary to retain the complete
checkpoint. No Git read, parse, cache write or fallback helper runs after a cap.
Error formatting only returns already established evidence. Header/source
counters record accepted reads, with the rejected next path recorded separately.

If bounded coverage succeeds, the consumer validates the checkpoint against the
full graph and uses the bounded policy. If v2 explicitly reports a deep error,
it may independently validate the complete checkpoint through the existing
direct mapper. Every root must have a changed direct owner, every frozen owner
is retained, and ordinary component/bundle widening applies. This fallback uses
`import_provider_hint`, **authority retry_only / proof_authority false**. The
outer deep result remains `complete: false`, `bounded_complete: false`, and
`full_closure_complete: false` with its actual error reason. It does not become a
completed traversal because direct evidence exists. Successful but non-narrowing
bounded results are not opportunistically downgraded to direct coverage.

Legacy v1 errors, unknown policy versions, mismatched bindings, partial root/
provider data, a checkpoint completed after the deadline, or an oversized combined
envelope cannot use this fallback. No checkpoint can enter `diagnosis/2`, the
provisional singleton branch, automatic repair, approval, withdrawal, bundling,
component bypass or individual blame. Existing durable observations are final;
the old capped event is never backfilled and old retries are never rewritten.

## Limits and trust

All roots together share one five-second helper deadline, including lock, parsing,
cache access and Git reads. The limit is never reset for another root. Bounds are:
eight diagnostic roots; 64 expanded header paths; 256 KiB per file; 512 KiB total
expanded source, including cache hits; 128 distinct output paths **including the
roots**; 64 KiB total evidence. Terminal nodes are not parsed; their immutable blob
type/size/availability are checked. Every expanded unowned source must exactly
match its base blob and size. Source-only parsed-header cache access is capped at
4 MiB. A cold parsed cache may read existing immutable objects; missing/cold Git
objects, a busy mirror, partial reads or unsupported headers fall back immediately.
There is no inventory, fetch, checkout, source evaluation or Lean execution.

The existing mirror lock and data-only header-cache trust model are reused.
No publication/API credentials are requested for either offline provider operation.
No compiled artifacts, persistent candidate verdicts, services, runner capacity,
model/account/worker ownership or production stage budgets change.

## Scheduling and authority

The planner uses `bounded_provider_suspects` and records `bounded_provider_hint`.
Existing bundles/shared paths/import components can widen the selected group.
A non-narrowing or unavailable hint preserves component/bisection fallback. Every
input is retained in a sealed retry with its original head, reviewer/manual
approval, original admission age, bundle/scheduling constraints and parent lineage.
The current queue ordering is unchanged; insertion order promises no larger-group
priority. Full exact-candidate builds and existing human/head/CAS/status checks
remain mandatory for every promotion.

Neither bounded nor direct providers enter `diagnosis/2` or the strong
`compiler_diagnostics` provisional singleton branch. Even one weak owner cannot
authorize provisional eviction, automatic repair/withdrawal/approval, a bundle
between PRs, an individual failure or a stacking allegation. A later actual
isolated build is separate evidence under existing rules.

## Frozen natural regressions

The candidate230 and candidate232 JSON fixtures preserve full frozen snapshots,
ownership, logs, exact production binding and actual baseline partition events.
Companion compressed source fixtures contain the exact immutable source blobs
reached by the production helper. Python regressions construct portable minimal
Git trees from those blobs and exercise the real helper/parser; these test trees
have synthetic commits. The captured lookup itself was replayed against the
original local candidate/base Git objects, without fetching or executing code.

| Candidate | Headers | Source bytes | Frontier owner | Depth residuals | Counterfactual groups | Actual groups |
| --- | ---: | ---: | --- | ---: | --- | --- |
| 230 | 37 | 493244 | 16291 (two paths) | 15 | 1/40 | 3/38 |
| 232 | 36 | 457279 | 16625 (two paths) | 5 | 2/35 | 2/35 |

Candidate232 has **no narrowing gain** because the existing import component
widens its owner to [15980,16625]. Candidate230's earlier external recovery is
not attributed to provider hints. These are scheduling counterfactuals, not
individual blame, a saved run or deployed benefit. Local helper cost with existing
Git/OS caches was 371/249 ms for candidate230 and 329/191 ms for candidate232
(cold/warm parsed cache), with Python CPU 106/72 and 86/41 ms respectively. These
exclude interpreter startup and Elixir orchestration and are not one-vCPU
production service measurements. An eight-header cap would remain UNKNOWN.

Replay the pure production planner without starting the application:

```sh
MIX_ENV=test BORS_DATABASE=postgresql mix run --no-start scripts/replay_diagnostic_partition.exs test/fixtures/bounded-providers-candidate230.json partition230.json
MIX_ENV=test BORS_DATABASE=postgresql mix run --no-start scripts/replay_diagnostic_partition.exs test/fixtures/bounded-providers-candidate232.json partition232.json
```

## Natural candidate254 cap regression

Actual candidate254/batch867 (46 frozen inputs, run38873489266) failed in unchanged
Isogeny/FiniteCenter.lean. Production event3773 under v1 correctly returned
UNKNOWN/provider_header_limit in616ms,139ms Python CPU; attempted accounting was
65headers/523204bytes, including the rejected65th header before its parse. Actual
3775 selected deterministic bisection23/23. This is one supplied natural attempt,
not an aggregate UNKNOWN-rate or benefit claim.

The root has six direct imports. Center/Functoriality.lean **and** Isogeny/Basic.lean
are frozen changed paths of PR16848. The existing complete direct helper and v2
checkpoint replay against original hash-verified candidate/base objects both give
an eligible **counterfactual1/45** partition, preserving all46 full snapshots. The
v2 deep walk still returns UNKNOWN at the same header boundary; it records64
accepted headers/519495bytes and a separately completed one-root4499-byte direct
checkpoint. No owner is inferred from incomplete deep nodes. Original production
logs reparse to the same primary compiler diagnostic. Companion exact source
fixtures exercise this cap in portable minimal Git trees with synthetic commits;
the captured lookup uses the original production identities.

Bounded immutable read-only API preparation of sparse source objects occurred
outside the offline helper, with each commit/tree/blob hash verified. No network,
credentials, project inventory or execution was added to the helper. Local replay
cost is not production helper cost. The coordinator's independent PR16848
withdrawal and its retained22-member retry are unrelated recovery, not checkpoint
or provisional-repair success. NEW post-release natural evidence remains required.

## Rollout and rollback

The default new-observation output policy is bounded_depth_v2. The existing
`BORS_IMPORT_PROVIDER_POLICY=bounded_depth_v1` selector is retained for deployment
compatibility; it selects the current bounded implementation. Set
`BORS_IMPORT_PROVIDER_POLICY=direct` to restore depth-one lookups for new
observations; any unrecognized value also selects direct. The Cloudflare Worker
explicitly forwards this variable into the Container environment, defaulting to
bounded_depth_v1 when absent. Operators can set the Worker variable to direct
and use the normal deployment path; no runtime setting is changed by this PR.
Existing immutable observations keep their recorded policy. `BORS_FAILURE_DIAGNOSTICS=off` retains
the broader existing rollback. No migration or workflow pin change is needed.

Required review, hosted checks and normal automatic deployment must precede
runtime verification. Record the exact deployed image and health, then observe
NEW natural compiler diagnostics, partition, retries and promotion outcomes in
the public candidate journal. Measure helper cost and unknown rate from actual
observations; report denominators and reason distribution without extrapolating
savings. No deliberately failed production batch or benchmark series is needed.
Keep the issue open until this evidence exists; replay alone does not satisfy
rollout acceptance.
