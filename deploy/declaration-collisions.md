# Defer batch-mates that add the same declaration

On 4 November the 2598 lineage spent three consecutive staging rounds on
duplicate declarations between batch-mates (candidates 1949, 1950, 1951): each
compile passed and the audit then failed on one name declared in two modules,
about 40 minutes of the serial slot per round. The compile cannot see this class
of failure, so a complete build does not collapse the rounds.

## What changed

Each PR's trusted build prints one `DECLARATION-NAMES` record: the names the PR
adds relative to its merge base, read from compiled module headers, with
reserved, generated and private names excluded. With
`BORS_DECLARATION_COLLISIONS` set to `record` or `on`, each planning pass
queues, for every member whose admission has no record yet, a background outbox
job (`{:declaration_names, admission_id}`, class 2) that finds the run that
posted the head's `build` status, reads the `sandboxed-build` job log through
the bounded collector in `priv/failure_diagnostics.py`, validates the record
strictly, and stores it on the admission snapshot as `declaration_names` with
state `known`, `empty`, `incomplete` or `unknown`. Admission is untouched and
nothing waits for that job: the pass that queued it treats the member as
unknown and the next pass sees the record. Every failure stores `unknown`, the
head is not fetched again, and retries carry the record of the same head.

With `BORS_DECLARATION_COLLISIONS=on`, `ConflictSelection.select/4` walks the
units in order before the Git probe. A non-bundle unit with a known record that
adds a name an already accepted unit also adds, in a different module, is
skipped with kind `declaration_collision`, naming the constant, both modules and
the earlier PR. Those skips join the Git selection's own skips in the one
`merge_conflict_deferred` group, so deferral, readmission and the audit's final
verdict are unchanged paths. The check runs under its own two-second budget;
on exhaustion the remaining units are unchecked and
`declaration_check_cut_short` is logged. Same-module matches, unknown or
incomplete records and bundles are never collisions. Each deferral records a
`declaration_collision` candidate event and posts one neutral PR comment with
an idempotency marker; the PR stays approved and is tried again.

The audit remains the authority. This check can only remove rounds: a miss
leaves the candidate to the audit exactly as before, and the only way it can
defer a unit is a name the audit would have rejected.

## Rollout

1. Producer first (TauCeti `scripts/DeclarationNames.lean` and the
   TauCetiTools build-script step), verified on a few PR builds.
2. Deploy this change and set `BORS_DECLARATION_COLLISIONS=record`. Confirm
   admissions of planned members carry `declaration_names` with state `known`
   on most heads and `unknown` reasons that are expected
   (`build_run_unavailable` for heads built before the producer).
3. Set `BORS_DECLARATION_COLLISIONS=on`. Verification is the first
   `declaration_collision` event followed by a candidate whose audit no longer
   reports that pair, and a lineage without consecutive audit-collision rounds.

## Rollback

Set the switch to `record` to stop deferrals at once while records keep
being stored, or unset it to stop both. An ordinary revert removes the fetch
job; pending jobs complete as unknown.
