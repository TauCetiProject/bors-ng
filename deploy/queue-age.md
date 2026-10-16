# Waiting queue age

Eligible waiting batches retain state and explicit descending priority ordering.
Within a priority, use the **oldest immutable original admission timestamp among
all current members** (microseconds). A bundle or sealed retry stays one atomic
batch: this chooses a group, never rearranges members or changes approvals.
Withdrawal deletes a live link, so that member no longer supplies age. Retries
retain original admission and lineage across generations; a changed current head
cannot inherit the old head's scheduling evidence.

Every member must have a matching admission for project, branch, batch, link,
patch, exact admitted/current head, reviewer and manual approval. Original age
must be a positive integer no later than its observation, which cannot be in the
future. Missing, malformed, duplicate or inconsistent records, and empty groups,
use the batch's current `last_polled` as an operational fallback for the **whole
group**. Unknown age is never inferred from PR creation, parent batches or other
members. A missing/invalid poll timestamp sorts at the pass's current time.
Equal effective ages use `last_polled`, then ascending durable batch ID; input/SQL
row order cannot affect the result. Running state always precedes waiting,
including explicit user priorities above the running priority100 sentinel.
Clones continue using member priorities, never the running sentinel.

Age preference has a finite retry allowance. Fresh admissions start with
`queue_age_retries=0`; each sealed clone increments and saturates at3. At3,
or when infrastructure `recovery_failures` reaches3, the whole group returns to
poll-time FIFO. Thus repeated inconclusive/failing clones cannot inherit an old
position indefinitely and repeatedly overtake existing waiting peers. Original
age, head approval and retry/repair budgets remain intact. Historical snapshots
without a counter start at0 for a bounded rollout grace; a missing original
admission or malformed counter gets no new inherited preference. This is a
scheduling allowance, not a new repair authorization or terminal failure verdict.
Existing narrowing bisection, indivisible terminal failure, recovery backoff and
per-head worker attempt limits continue to apply.

The scheduler reads current waiting links, current heads and their admissions in
one bulk join, bounded by current waiting membership. It performs no historical
ancestry scan, per-batch/member admission lookup or GitHub request for ordering.
The same helper is used for serial and speculative successor selection. No batch,
link or approval is written by age calculation. `last_polled` is never backdated.
Poll delay, `recovery_after` and `promotion_retry_at` all gate waiting eligibility;
running promotion/recovery logic and predecessor/train gates remain authoritative.
An older waiting group cannot preempt running work.

Conflict isolation deliberately creates unmergeable units first; deterministic
bisection and diagnostic partitions also have deliberate creation order. Durable
batch ID preserves that order when age and poll time tie. Different original
ages now follow FIFO within the existing priority, rather than silently treating
creation order as a new diagnostic priority. Diagnostic partition membership,
strict narrowing, suspect repair and quarantine authority are unchanged.

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
