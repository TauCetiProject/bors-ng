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
