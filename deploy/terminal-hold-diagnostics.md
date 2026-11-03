# Diagnose completed staging results held inside Bors

Batch 2582 / candidate 1918 failed its workflow at 17:13:02 UTC on 3 November.
Bors persisted `build=error` at 17:13:06–07, but classified/split the batch only
at 17:43:13–22. The configured project running-batch poll interval is 1,800 seconds.

## Evidence and reproduction

The failure-status job 560973 was claimed at 17:13:06.146 and completed
successfully at 17:13:08.035. During that execution, eligibility job 560977 for
batch member #25749 (patch 14410) was enqueued at **17:13:07.301**. It was not
claimed until 17:13:08.217 and completed at 17:13:11.311.

The exact guard predicate treats pending member `cast` jobs as withdrawal
barriers; it includes this eligibility refresh. The final refreshed status was
journaled at 17:13:07.519, after that job was enqueued. The subsequent guard
could therefore see it. The guard's hold branch updates `last_polled`, schedules
no short recheck, and returns successfully. Finishing an eligibility job wakes
jobs explicitly deferred in the outbox, but this status job has already finished
successfully. Ordinary polls skip the held batch until its next 1,800-second poll.

A database integration regression reproduces this sequence with a real pending
eligibility job, exact candidate, red status, train guard, status callback and
ordinary subsequent poll. Clearing the blocker restores guard eligibility but
does not advance completion before the thirty-minute deadline. This explains
the recorded timing; a direct historical hold-reason record was not retained,
so it is strong causal reconstruction rather than a recovered execution trace.

Regular polls continued during the gap. No per-batch retry was dispatched in
the exported 17:10–17:55 mailbox. Two `BadBooleanError` jobs, 561263 and 561273,
first executed at **17:43:07.893** and **17:43:43.286** respectively, with the
first failures recorded at 17:43:35.739 and 17:45:22.594. Their timings exclude
them as causes of the original 17:13–17:43 delay; they may affect subsequent
queue progress. Their old records retain the class but no stack location.

The exception is independently reproducible in `Zulip.send_message/1` line 15:
production does not set `:is_test`, so `Application.get_env(:bors, :is_test)`
returns nil. Its use as the left operand of strict `or` raises `BadBooleanError`
before notification delivery. Tests set `:is_test=true`, masking the condition.
This runs immediately after `terminal_observed` and before queue-label cleanup
and the next completion poll are enqueued. That matches the first historical
exception timestamp. The patch supplies a false default and tests production's
missing flag with disabled/incomplete notification configuration, without network
delivery. The original historical stack was not retained, but this exact failure
is now reproduced and fixed.

## Added diagnostics

- Candidate events `completion_deferred` record terminal status summary, hold
  reason, job/attempt identity, recovery/promotion deadlines, explicit next retry
  when present, and the normal fallback poll time. Withdrawal barriers include
  up to eight job IDs, kinds, subject IDs, fixed command categories and readiness
  timestamps, with an explicit truncation flag.
- Each candidate/reason is reported at most once per five minutes. Normal
  in-progress builds produce no such events. Diagnostics never enqueue work,
  alter approvals or change terminal-result scheduling.
- Executor exceptions log and persist up to eight function/module/arity and
  basename/line frames in the existing `attempt_timing.last_exception` field.
  Messages, actual arguments, command payloads and local directory prefixes are
  excluded. The latest exception survives subsequent retry success. Writes
  require the current durable job owner; stale attempts cannot overwrite them.
  Retry/error semantics remain unchanged, and diagnostic failures are best effort.

No schema migration or new per-PR workflow is required. Use normal hosted checks,
merge and automatic live deployment with admissions enabled. Verify running image,
health and useful completions; record any naturally occurring diagnostic events.

## Follow-up informed by this investigation

The smallest direct repair is a durable short recheck whenever train guarding
holds a completed result, respecting real recovery deadlines. A separate decision
is whether ordinary eligibility refreshes should count as withdrawal barriers;
do not weaken that predicate merely to avoid its missing-wakeup consequence.
The reproduced notification boolean bug is fixed here; persisted exception
locations will distinguish any additional boolean failures after rollout.
