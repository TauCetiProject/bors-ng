# Waiting queue age

Eligible waiting batches retain state and explicit descending priority ordering.
Within a priority, use the **oldest immutable original admission timestamp among
all current members** (microseconds). A bundle or sealed retry stays one atomic
batch: this chooses a group, never rearranges members or changes approvals.
Withdrawal deletes a live link, so that member no longer supplies age. Retries
retain original admission and lineage across generations; a changed current head
cannot inherit the old head's scheduling evidence, with one exception: a suspect
handed to repair keeps its admission age once, at its next admission after the
handoff, whether at a repaired head or at the unchanged head once its conflict
request has been lifted (see "Readmission after a repair handoff" below).

Every member must have a matching admission for project, branch, batch, link,
patch, exact admitted/current head and reviewer. Manual approval is mutable at
the same head (a human can take over queued work); it is never an age input or
an authorization output. Existing dispatch and promotion checks validate it.
Original age must be a positive integer no later than its observation, which cannot be in the
future. Missing, malformed, duplicate or inconsistent records, and empty groups,
use the batch's current `last_polled` as an operational fallback for the **whole
group**. Unknown age is never inferred from PR creation, parent batches or other
members. A missing/invalid poll timestamp sorts at the pass's current time.
Equal effective ages use `last_polled`, then ascending durable batch ID; input/SQL
row order cannot affect the result. Running state always precedes waiting,
including explicit user priorities above the running priority100 sentinel.
Clones continue using member priorities, never the running sentinel.

Age preference has a finite retry allowance. Fresh admissions start with
`queue_age_retries=0`; failed/inconclusive clones increment and saturate at3.
At3 the whole group returns to poll-time FIFO. Neutral train invalidation, stale dispatch, member
removal/cancellation, non-fast-forward promotion and provisional remainder or
isolated fallback clones carry the counter unchanged. Unknown reasons consume
the allowance conservatively. Registry crash counts do not spend it. Thus repeated
inconclusive/failing clones cannot inherit an old position indefinitely and repeatedly overtake existing waiting peers. Original
age, head approval and retry/repair budgets remain intact. Historical snapshots
without a counter start at0 for a bounded rollout grace; a missing original
admission or malformed counter gets no new inherited preference. This is a
scheduling allowance, not a new repair authorization or terminal failure verdict.
Existing narrowing bisection, indivisible terminal failure, recovery backoff and
per-head worker attempt limits continue to apply.

The scheduler reads current waiting links, current heads and their admissions in
one bulk join, returning only current waiting membership. A PostgreSQL batch-ID
index supports membership seeks. It
performs no historical ancestry query, per-batch/member admission lookup or
GitHub request for ordering. Serial running polls defer the metadata read to
successor selection, if train eligibility permits one.
The same helper is used for serial and speculative successor selection. No batch,
link or approval is written by age calculation. `last_polled` is never backdated.
Poll delay, `recovery_after` and `promotion_retry_at` all gate waiting eligibility;
running promotion/recovery logic and predecessor/train gates remain authoritative.
An older waiting group cannot preempt running work. The existing explicit
priority hold path may put a running batch back into waiting with its priority100
and promotion deadline intact; it must respect that deadline before resuming.

Conflict isolation deliberately creates unmergeable units first; deterministic
bisection and diagnostic partitions also have deliberate creation order. Durable
batch ID preserves that order when age and poll time tie. Different original
ages now follow FIFO within the existing priority, rather than silently treating
creation order as a new diagnostic priority. Diagnostic partition membership,
strict narrowing, suspect repair and quarantine authority are unchanged.

## Readmission after a repair handoff

A PR attributed as a culprit, or peeled from a batch by a construction
conflict, is withdrawn by `SourceCompatibility.provisional_failure` with a
head-bound repair request (deploy/suspect-handoff.md). It comes back through
ordinary re-approval at a new head, and until now that new head was a fresh
admission that re-entered behind everything approved while the author was
repairing it. The suspect now inherits its withdrawn admission's
`original_admitted_at` once per handoff, so the repaired head takes the place
the PR already held. Approvals, authorization, dispatch and promotion are
unchanged; this is only an age input.

**Record.** The handoff's `provisional_repair` event
(source `bors.failure_isolation`) carries, for each entry of `suspects`, the
suspect's `patch_id`, `link_id`, `admission_id`, `original_admitted_at` (the
age the withdrawn admission carried, itself possibly inherited through retry
generations) and `queue_age_retries`, beside the existing `pr` and `head_sha`.
A missing or mismatched parent admission records `nil` and passes nothing on.
The event was chosen over the persisted request on the patch
(`patch.source_compatibility`) because the request is rewritten by the source
sweep as soon as the new head is checked, which may happen before or after the
re-approval, while the journal is append-only and survives head changes,
withdrawals and batch deletion. It is also the record the TauCetiWorker journal
report already reads for handoff-to-readmission time. No new table or column.

**Rule.** When a fresh admission (`source` `bors.admission`, never a retry
clone) is written for a PR, `CandidateJournal.admit` looks up the latest
`provisional_repair` event of the same repository and branch naming that PR.
`QueueAge.readmission/3` accepts its record only if the retained age is a
positive integer no later than the new observation and the retained counter is
a non-negative integer below the saturation value 3. The head may be the
handed-off one: a PR peeled from a batch by a construction conflict was never
broken, and when `main` moves and the head merges cleanly the ordinary sweep's
`maybe_resolve` lifts its `local-source-v3` request and re-offers admission at
the unchanged head; that admission inherits like a repaired one. A head whose
request is still active inherits nothing: `CandidateJournal.admit` checks
`SourceCompatibility.requested?` for the admitted head. Automatic admission
never offers such a head (`MergeEligibility.apply_decision` sees an unchanged
decision id until `maybe_resolve` clears it), and a human `r+` of it does reach
`insert_member` but is ejected at assembly by `SourceCompatibility.admit`, which
deletes the link and withdraws the approval; the `requested?` check keeps that
ejected admission from consuming the grant. The handoff is
consumed by a `suspect_readmitted` event (source `bors.admission`) appended to
the handoff candidate with `pr`, `patch_id`, `head_sha`, `handoff_head_sha`,
`handoff_event_id`, `admission_id`, `link_id`, `batch_id`,
`original_admitted_at`, `queue_age_retries` and `readmitted_at`; a later fresh
admission of the same PR finds that marker and is ordinary. A later handoff of
the readmitted head records the inherited age again, so each handoff grants
one readmission. The lookup is one read at approval time over the repository's
`provisional_repair` events plus the handoff candidate's own events; the
scheduler path is untouched and still validates the inherited value against
the current admission record (exact head, reviewer, patch, branch, batch and
observation) like any other age.

**Allowance.** The readmitted admission carries `queue_age_retries` unchanged
from the admission it inherits from; it does not start at 0. The counter bounds
how often one age can overtake peers through clone generations, and it belongs
with that age. The handoff spends none of it, so a repaired suspect is not
punished, and renews none of it, so a handoff cannot refresh an allowance the
way the policy forbids neutral clones from doing. A record whose counter is
already saturated has no preference left to pass on and gives an ordinary
admission with a fresh age and counter 0.

**Journal.** The inheriting admission snapshot gains `inherited_handoff` with
`candidate_id`, `event_id`, `head_sha` (the handed-off head), `admission_id`
and `handed_off_at`. Ordinary admissions keep their exact previous shape.
Candidates freeze the field with the rest of the member snapshot. Older code
ignores all of these fields.

## Replay and rollout

Run without Mix, Ecto, application or database startup:

```sh
elixir -r lib/worker/batcher/queue_age.ex -e \
  'ExUnit.start(); Code.require_file("test/batcher/queue_age_test.exs")'
```

The frozen2026-10-16 06:50UTC aggregate counterexample replays old waiting order
`854,856,859,860,861,862,863` and new order
`856,854,863,859,861,860,862`; running852 remains first. Aggregate minima/counts
are real; per-member identities in this pure test are synthetic. The fixture
checks all132 waiting members survive unchanged, including heads and approvals;
it does not claim to export production per-head identities or measure time saved.
Transactional tests exercise actual admission/clone/withdrawal metadata.

After required review and exact-head CI, merge into upstream master. Verify the
automatic Cloudflare build's exact source, deployed image/instance and health.
The coordinating session owns staging watches. Use the next naturally eligible
selection's journal/construction event and read-only current queue/admission
snapshot to replay the decision with this helper; capture priority, eligibility,
running state, unchanged heads/approvals and group membership. A retry counter
and source-linked queue-order receipt distinguish original age from fallback.
Do not cancel work or dispatch synthetic production CI to obtain evidence.
Production verification and unobserved performance benefits remain pending until
actual receipts exist. Rollback is the normal reviewed code revert; journal
counter fields are additive and ignored by older code.

For readmission age, verify in the journal that the next `provisional_repair`
carries `original_admitted_at` and `queue_age_retries` for each suspect, and
that the suspect's next fresh admission after the handoff, at a repaired head
or at the unchanged head after a lifted conflict request, has
`inherited_handoff.event_id` naming that event, `original_admitted_at` equal to
the retained value, and a `suspect_readmitted` event on the handoff candidate.
Replay the next natural selection with the helper above to confirm the returned
batch is ordered by the inherited age ahead of work approved during the repair.
Rollback is the same ordinary revert; `inherited_handoff`, the extended
`suspects` entries and `suspect_readmitted` are additive and ignored by older
code, and admissions already written keep their inherited age as a plain
`original_admitted_at`.
