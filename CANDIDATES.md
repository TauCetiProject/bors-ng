# Candidate assembly contract

`BorsNG.Worker.CandidateAssembler` defines the assembly callback and exact result
identity validation. `BorsNG.Worker.CandidateJournal` records inputs and lifecycle
facts for independent assemblers J and K. The existing scheduler, approval checks, live head validation,
merge-time checks and promotion rules remain the authority. The journal and its
public read API never grant admission, dispatch, promotion or cache publication.
Speculation remains disabled; recording a predecessor does not enable it.

1. Insert approved queue links with `insert_member/1` (or `/3` for a retry), in
   the same transaction as the admission snapshot. Preserve exact approved head,
   reviewer and manual approval. Retry snapshots retain source batch, source
   candidate and source admission IDs, original admission receipt time, and
   reason. Missing historical ancestry stays null. Replay of a committed retry
   reuses its existing links and snapshots.
2. After eligibility filtering, order links in the actual merge order and call
   `begin/4` with the exact base commit. The batch lock allocates a monotonically
   increasing generation. Every reconstruction, including restart after a
   partial construction, allocates another generation; never rewrite an old
   generation. Snapshots retain the ordered member/approval/head inputs even
   after withdrawal, changed heads or deletion of queue links and batches.
3. An optional speculative predecessor must be a recorded candidate in the same
   project and target branch whose bound commit equals the exact assembly base.
   Assemblers must independently enforce scheduling limits and eligibility.
   This API validates identity only. The current serial assembler supplies null.
4. Call `bind/7` once after GitHub returns the assembled commit. Record exact
   commit, tree and base, effective policy and its read ref, and toolchain bytes
   fetched at the exact constructed commit. Binding is serialized and rejects a
   second bind or a result from an abandoned generation. The assembler result
   envelope binds candidate ID, generation, base SHA and ordered-member digest,
   and returns commit/tree or structured conflict/error details. Validate it
   before any async result becomes visible. Live eligibility and main advance
   checks remain the caller’s responsibility. The effective-policy hash uses
   canonical JSON with recursively sorted map keys and preserved array order; the toolchain hash is SHA-256 of the file bytes. Policy refs
   may name the temporary branch for existing non-scoped repositories: the
   effective policy snapshot, rather than that mutable ref, is the durable fact.
   An absent/failed toolchain read is explicitly unknown. Digests establish byte
   identity only, never proof correctness or producer trust.
5. Append sourced lifecycle events. Record dispatch request before workflow
   dispatch and its acknowledgement afterwards. Staging push acknowledgement is
   recorded separately by source when push-triggered CI is used. Promotion is
   recorded on successful target-branch push, before notifications. No status
   receipt is a runner timestamp. Existing trusted workflows and credential
   boundaries are unchanged.

## Timestamps and status binding

`check_run_observed` retains check ID, suite/app IDs and GitHub’s authoritative
`started_at`/`completed_at` as `check_started_at`/`check_finished_at` in Unix
microseconds. Missing or invalid GitHub timestamps remain null. These describe
the named check execution, with commit-only generation binding; they do not
establish runner assignment or aggregate candidate completion. Raw status
processing and queue state are unchanged.

All `observed_at` and `original_admitted_at` values are Unix microseconds from
bors's clock. They are receipt/write times with named sources, not inferred
GitHub or runner event times. Construction starts after base discovery and stops
or binds after the corresponding API response. The legacy synthesis operation
creates and pushes together, so `constructed` and push acknowledgement describe
receipt of that combined operation. If a process dies after an external write
but before journaling, that fact stays unknown; the next attempt gets a new ID.

Status observations bind to the latest generation only when its bound commit
matches the batch's current commit. Old commits cannot annotate newer
candidates. Commit-only status payloads cannot distinguish repeated execution of
an identical SHA: events explicitly report `generation_binding=commit_only` and
`runner_lifecycle_authority=false`. They cannot establish run identity, runner
assignment, actual job start or finish. These timestamps stay null. A terminal
bors status summary is an observation, not a job finish time. Existing status
processing is unchanged; downstream consumers must not use journal events as
queue intent or as proof that a particular runner execution succeeded.

## Additive read API and migration

`GET /repositories/:id/candidates?base=main&batch_id=123` returns
`tauceti-bors.candidates/v1`, snapshots, admissions and sourced events. Without a
batch filter it reads the latest 100 candidates for the requested project/base,
and latest 500 admissions. A batch filter reads all generations and up to 500
admissions. Empty history means unknown, not an empty historical batch. Runner
times remain explicit nulls. `queue_authority` is false and responses are
`no-store`. The existing `tauceti-bors.observation/v1` endpoint, fields and state
projection are unchanged, including its active member arrays.

The migration creates three tables and indexes without updating existing
batches, links, approvals or timestamps. Table/index creation tolerates replay
following interrupted MySQL DDL. It deliberately does not backfill historical
facts. Journal rows have no cascading foreign keys and survive queue deletion.
Rolling back removes the journal; rolling forward cannot reconstruct lost facts.
