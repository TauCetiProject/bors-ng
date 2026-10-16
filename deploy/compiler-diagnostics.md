# Candidate compiler and audit diagnostics

Refs TauCetiProject/TauCeti#16781. This extends the existing
[failure isolation contract](failure-isolation.md). Diagnostics have retry
scheduling authority only. An emitted or forged compiler error cannot convict a
PR, revoke approval, skip checks, merge, publish or change cache trust.

The trusted run/jobs classifier still decides candidate versus infrastructure
failure. Only a terminal failed `sandboxed-build` with a known failed compiler or
import-preflight step is eligible for log collection. Cancellation, timeouts,
publisher failures and unknown classifier outcomes retain existing semantics.
The existing asynchronous repository executor performs collection; neither the
shared GitHub API server nor the scheduler downloads logs. The serial fallback
does not collect logs.

The durable `failure_diagnostics` event binds candidate ID, generation, immutable
base, ordered member digest, tested commit, workflow, run ID/attempt and failed
job IDs. Run creation must follow the generation's construction start. A stale
head, generation, run attempt or job cannot reuse another observation. The
latest trusted failure classification selects the matching observation. Complete
and unavailable results both survive restart and suppress repeated downloads for
the same identity. A crash before observation persistence may repeat this bounded
read; native sealed retry creation remains idempotent.

The trusted Python helper reads at most two exact job logs, with one GitHub
request and at most one storage request per job. It strips credentials before an
allowlisted HTTPS log-storage redirect and follows no further redirect. Limits:
16 MiB per log, nine seconds for the entire helper, 128 diagnostic records,
8 KiB per input line and 512 characters per source path/module. Invalid UTF-8,
truncation, API errors, oversized logs or parser limits produce unavailable
evidence. Raw logs, arbitrary log messages and signed URLs are never journaled.
The event records byte counts and elapsed/CPU milliseconds for available logs.

ANSI escapes and GitHub timestamps are stripped. Only anchored Lean/Lake source
error locations and the audit driver's specific duplicate-declaration message
are recognized. Shell commands, annotations, status-like text, traversal and
external package paths do not match. A syntactically valid forged location can
still affect scheduling; it has exactly the same limited authority as any other
untrusted diagnostic. Every selected retry receives all authoritative checks.

Paths map to exact changed files and attributed direct project imports. New
assembly hints record direct dependency paths per member as well as the existing
aggregate inventory. Historical hints without this attribution cannot map an
unchanged dependency error. Audit collisions retain both module paths and suspect
all changed/importing owners; a declaration already in the immutable base has no
changed owner. Unknown paths or non-narrowing sets retain deterministic fallback.
Atomic bundles, shared-file owners and provider/importer components stay in the
existing planner; every member appears in exactly one sealed retry. Multiple
suspects are tested together, followed by a fully checked remainder. Only a
genuine indivisible failed CI retry can reach existing quarantine semantics.

Exact local assembly also avoids a dispatch when complete inventory already
records a missing project import and the existing planner yields a strict
multi-unit partition. It records `local_import_failure`, then uses the same
sealed native retry path, preserving heads, manual/reviewer approval, lineage
and requested priorities. It never quarantines from inventory. Singletons,
indivisible/non-narrowing groups, unavailable hints and candidates with
speculative reservations retain dispatch. No workflow pin or workflow file
changes are required.

The shortcut commits evidence, all retry children and the parent's terminal
state in one database transaction. A crash after that commit cannot reconstruct
the waiting parent against an advanced main beside its live children. Failure to
create any child rolls back the evidence and every new child, retaining the
waiting parent for ordinary recovery. Compiler-log retries retain their existing
running-CI binding and native replay path.

[Historical production replay](compiler-diagnostics-evidence.json) shows batch
729 selecting #15908 versus three other heads, 731 selecting #15936 versus ten,
and the joint audit failure in 733 selecting #15955 versus four. Batch 727 retains
its existing #16175 versus seventeen missing-import split; this inventory can now
avoid the original multi-member dispatch. Logs were 0.35–9.35 MB; local parsing
took 3–102 ms. One actual bounded download of batch 733 read 7,056,186 bytes in
756 ms with 76 ms process CPU. These are local observations, not deployed latency
or throughput gains. Production partition, approval/membership comparison and a
full passing remainder are required before marking rollout verified.

Rollback: `BORS_FAILURE_DIAGNOSTICS=off` disables collection, diagnostic partitions
and the missing-import dispatch shortcut using the existing feature switch.
There is no migration, backfill, continuous PR revalidation or moving-main rebase.

## Environment-linter locations

Refs TauCetiProject/TauCeti#17037. The TauCeti reporter can now emit one
`LINT-ENV-LOCATIONS` version-1 JSON envelope after its existing report, nonce and
NEW-versus-baseline validation. Findings contain `linter`, `declaration`, a
project-local `path` and positive `line`/`column`, or an explicit `unknown` reason.
The reporter obtains locations from declaration origin/range metadata in the
existing exact candidate linter environment. It never infers an owner from the
declaration's name. The raw `LINT-ENV-ORIGINS` driver output is not consumed here.

The optional envelope is capped at 1 MiB and 128 findings, within the existing
16 MiB log/128 observation/time bounds. Duplicate envelopes or JSON keys,
malformed/incomplete/truncated records, unsupported fields/versions, unsafe paths
and invalid ranges make log evidence unavailable. Unknown findings retain empty
paths, forcing the existing mapper to retain deterministic fallback rather than
drop a potentially global interaction. Older logs remain supported.

These remain candidate-generated, untrusted scheduling observations, including
when a well-formed record is forged. No identity or authority is accepted from the
envelope: the existing authoritative failed workflow/run/attempt/job and immutable
candidate/generation/base/ordered-head digest/commit bindings select the logs.
Unchanged dependency paths still need existing complete attribution; bundles,
multiple owners and provider/importer groups stay intact. Every retry and its
remainder require full authoritative CI; a hint cannot convict, hold, close,
revoke approval, skip a check, merge or grant cache authority.

Validation includes the actual batch-816 `simpNF` explanation for
`TauCeti.element_annihilator_flat_baseChange`. A bounded exact-source replay
imports its module together with `Mathlib.Algebra.Module.Torsion.Basic`, reproduces
the failure, and emits `TauCeti/RingTheory/Flat/Annihilator.lean:77:9`. Appending that
locally collected record to the frozen original native job log lets the production
pure planner select #16385 and the remaining 34 original snapshots. Ownership and
dependency replay took 442 ms locally. This is fixture replay, not individual
guilt, a deployed partition or a passing remainder. The unchanged source/metadata
join took 0.109 ms per local call, 17.195 ms including Python startup; native
one-finding origin collection measured 0 ms at millisecond resolution. These are
bounded-fixture measurements, not whole-library benchmarks.

The companion has Python parsing and parser-to-Elixir/DB retry tests, including
Unicode/escaped names, unknown/private/generated origins, multiple findings,
unchanged dependencies, forged/malformed/truncated records and stale bindings.
The production producer is workflow-pinned: after it lands with required human
review and full CI, a NEW ordinary dispatch must exercise it. A naturally occurring
failed audit and fully passing remainder are still needed to claim real diagnostic
partition benefit. Do not manufacture a failed production run or mark the issue
verified while review/deployment/production evidence remains outstanding.

### Lake failures reported only as failed targets

The same retry-only parser recognizes Lake's final, anchored
`Some required targets logged failures:` block, followed by one or more exact
`- TauCeti.Module` targets and the closing `error: build failed`. It maps those
module targets to project source paths without guessing warning positions or
declaration origins. Warnings alone produce no observations. Missing/duplicate
headers, truncated or empty blocks, unsupported/mixed external targets, facets,
unsafe paths or more than 128 observations invalidate that fallback evidence.

The target summary is a fallback only. The parser first scans the entire bounded
log for precise source/audit observations and all environment-linter records.
When any such records exist, it retains those records and does not consume the
optional summary. A malformed, truncated or oversized cascade cannot erase a
precise finding or add unrelated retry owners. Unknown environment origins still
retain empty paths and force the mapper's conservative fallback; malformed or
duplicate environment envelopes still make evidence unavailable. The 16 MiB log,
128 observation, path, line and total-time bounds are unchanged. Warning-only
failures still use the strict complete target-summary fallback.

This covers the real batch82548 failure in
[run38829250757](https://github.com/TauCetiProject/TauCeti/actions/runs/38829250757),
job116743924558. `lake build --iofail` failed after a deprecated import warning;
the final block names `TauCeti.Combinatorics.Young.KnuthCorrespondence` without a
source error diagnostic. The original 7,287,202-byte native log now yields that
single exact failed-module path in 61 ms locally. The unrelated warning text and
line9 are not used. This is parsing of a genuine frozen failure, not a new
production partition or proof that a PR independently fails.

Failed-target observations use the identical authoritative run/candidate and
ownership path. They cannot revoke approval or change holds, source branches,
checks or cache trust. This consumer improvement can deploy before the TauCeti
reporter: it needs no new producer. Root still owns deployment and new natural
production verification; the reporter separately requires a NEW dispatch pinned
to its merged trusted code. Run38831374735 was dispatched on older trusted code
and cannot demonstrate reporter rollout.

### Batch827 primary-diagnostic regression

The original #47 parser coupled target-summary validation to primary diagnostics.
In [run38835437044](https://github.com/TauCetiProject/TauCeti/actions/runs/38835437044),
job116763406898, two exact source errors at
`TauCeti/FieldTheory/FunctionField/ConstantExtension/RiemannRoch.lean:320:0` and
`:327:0` precede 1,378 final failed targets. The entire 7,869,800-byte native log
produces one deduplicated compiler path before #47 (`2210cde`), but #47 raises
`observation_limit` and discards that useful path. Journal event2981 records
unavailable diagnostics and event2983 selects a dependency-component retry.

The correction restores the primary path without increasing any limit or
promoting cascading bad imports into missing-source or stacking evidence. On the
same complete frozen log, local parsing measured 68 ms before #47, 66 ms before
failing in deployed #47, and 63 ms after the correction; these are single warm
local measurements, not production latency claims. The existing #825 warning-only
fallback and #816 environment-location cases remain covered.

[Frozen replay evidence](primary-diagnostics-regression-evidence.json) binds exact
candidate219/generation1, base, candidate commit/tree, ordered-head digest, all 36
heads and the original run/attempt/job. The existing ownership replay verifies
the exact assembly chain; the production pure planner selects `[16812]` and the
other 35 original snapshots with every snapshot unchanged. This is fixture replay
only, not individual guilt, a new production partition or a passing remainder.
No historical journal row, source branch, hold, approval, queue or lease is
modified. The one-CPU deployment setting from master6096172 is unchanged.

Reproduce with `scripts/replay_diagnostic_ownership.py` on the frozen candidate219
journal capture, a local bare mirror containing its exact objects and the complete
native job log, followed by `scripts/replay_diagnostic_partition.exs`. Python
regressions combine the exact primary error text with 1,400 distinct cascading
targets and malformed/truncated summaries in either order. Parser-to-Elixir/DB
tests verify stale-attempt rejection, immutable checked retry groups, preserved
approvals and unknown global lint findings retaining fallback. Natural production
collection/partition verification remains the coordinator's responsibility after
normal required review, full CI and deployment; the TauCeti reporter's human
review and new trusted dispatch requirements are separate and unchanged.
