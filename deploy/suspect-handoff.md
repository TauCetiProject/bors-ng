# Hand attributed suspects and conflicts to repair, never to solo CI

The 3 November staging audit (TauCetiWorker `docs/redesign/specs/staging-triage.md`)
found two ways the single staging slot was spent without landing anything:
a PR peeled out of a batch by a construction merge conflict got its own sealed
retry batch and a full staging run (eleven in one day, all green), and a batch
with several broken members lost one suspect per CI round because the handoff
accepted exactly one. The 95-minute provisional fallback added a third solo run
for suspects whose worker made no change.

## What changed

`FailureIsolation.split/4` hands off every directly attributed owner at once.
`SourceCompatibility.provisional_failure/7` takes the list of suspect links:
one transaction locks the parent, rechecks each suspect's exact head and
approval, admits the remainder once as `provisional_remainder` (no child when
every member is attributed), persists one request and withdrawal per suspect,
retires the parent and records one `provisional_repair` event with `prs`,
`suspects` and `remainder`. Labels are projected afterwards per suspect.
Component expansion through shared paths and import edges no longer selects who
is handed off; it still shapes the retry groups when a handoff is unavailable.

A construction conflict now passes the repository connection to the split and
hands the conflicting head off with a `conflict` request under the ordinary
`local-source-v3` policy, with the conflict's paths and merge head, instead of
cloning it into a one-member retry batch. Because the policy is the ordinary
one, the existing source sweep rechecks that head when `main` moves and
`maybe_resolve` lifts the request without author work if the branch then merges
cleanly; a conflict with landed `main` stays requested. The request text says
the conflict appeared while combining queued PRs. Bundled conflicting heads,
speculative successors with an unlanded predecessor, manual approvals, pause
labels and stale bindings keep the previous retry-batch behaviour.

Each `suspects` entry of the `provisional_repair` event also records the
suspect's `patch_id`, `link_id`, `admission_id`, `original_admitted_at` and
`queue_age_retries`, read from its admission in the retired parent. When the
same PR is next admitted after the handoff, at a repaired head or at the
unchanged head once `maybe_resolve` has lifted a conflict request,
`CandidateJournal.admit` takes that age and counter for the new admission,
marks the snapshot with `inherited_handoff` and appends `suspect_readmitted` to
the handoff candidate, so the PR returns to the queue position it held instead
of behind everything approved during the repair. Each handoff grants this once;
a head whose request is still active, a second fresh admission and retry clones
inherit nothing new. The rule, the record choice and why an admission of a
still-requested head cannot spend the grant are described in deploy/queue-age.md.

`provisional_fallback/4`, `fallback_due?`, `fallback_active?`, `fallback_member?`
and the 95-minute grace are removed, as is the request sentence promising an
unchanged PR a run of its own. An unchanged suspect stays withdrawn until its
head changes and passes ordinary readmission.

## Rollout and verification

Deploy live through the normal upstream merge and automatic Cloudflare build;
keep admissions and running staging builds enabled. Existing one-member retry
batches drain under the old rules. Verify in the candidate journal that the next
`failure_partition` with several compiler owners is followed by one
`provisional_repair` whose `prs` lists all of them and one `provisional_remainder`
child, that the next `local_conflict` is followed by `provisional_repair` with
`conflict.paths` and no one-member retry batch, and that no `provisional_unconfirmed`
event appears again. For readmission age, verify that the `provisional_repair`
carries `original_admitted_at` for each suspect and that the suspect's next
fresh admission after the handoff has `inherited_handoff` naming that event, the
same `original_admitted_at`, and a `suspect_readmitted` event on the handoff
candidate; the TauCetiWorker journal report measures handoff-to-readmission
time from these fields.

## Rollback

Ordinary reviewed revert. Requests and withdrawals already committed keep their
existing recovery semantics; `BORS_SOURCE_COMPATIBILITY=off` stops new handoffs.
The readmission fields (the extended `suspects` entries, `inherited_handoff` and
`suspect_readmitted`) are additive and ignored by older code.
