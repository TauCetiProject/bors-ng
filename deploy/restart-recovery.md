# Bounded orderly-stop recovery

Refs TauCetiProject/TauCeti#17067.

The dispatcher previously exited on supervisor shutdown without recording
active results delivered during shutdown. A live claim could therefore block
the project until its 300-second lease expired, even if the command completed
while the supervisor stopped. The change traps orderly shutdown and accepts
only results matching its stored active worker/token for up to four seconds.
It claims no new work in that interval. Executors and API clients stop after
the dispatcher; the database stops after the orchestration supervisor.

This is a conservative completion drain. It does not release interrupted work,
shorten leases, or clear claims on startup. Incomplete work and abrupt death
retain their existing expiry/reconciliation behavior. A GitHub HTTP request can
complete after its caller dies; an already accepted dispatch must be reconciled
on retry. An exhausted supervisor or container stop budget can still interrupt
the drain. The five-second dispatcher shutdown budget also bounds slow database
completion; the four-second mailbox deadline cannot be extended by stale traffic.

## Before and local verification

Read-only production observation at 2026-10-16 02:45:58 UTC, before this change:

| Job | Kind | Ready wait | Recorded service | Limit |
| --- | --- | --- | --- | --- |
| 55792 | compatibility | 264679 ms | 22018 ms | Availability was one minute after enqueue. |
| 55795 | cast | 344913 ms | 2138 ms | Second attempt only; initial claimant was overwritten. |
| 55803 | cancel | 283409 ms | 611 ms | Unrelated cancellation subject5458. |

The rollout at 02:13 UTC and these waits are consistent with an abandoned
lease, but do not prove restart causality or identify the initial claimant.
Candidate221/batch830 was legitimately cancelled by another owner; its journal
and the batch840 cancellation retry group remain untouched.

Of 60 recent completed jobs sampled at the same observation, 57 had recorded
service below four seconds (median806ms, maximum7371ms). This is a short sample,
not a fleet capacity estimate or a promise that longer commands will drain.

A local PostgreSQL regression at 02:47:50 UTC used a real supervisor stop with
an already accepted external dispatch and a second dispatcher. The drain took
12ms; the queued cancellation was claimed after1002ms and ran5ms while the old
claim still had more than250s left. The dispatch ran18ms and occurred exactly
once. These are deterministic mock-service timings, not production throughput.
Tests also cover continued stale messages at the deadline, late completion after
reclaim, stale renewal/failure, abrupt death, bounded timing history and existing
accepted-mutation reconciliation. Reproduce with:

```sh
mix test test/batcher/orchestration_test.exs test/application_test.exs
```

## Rollout acceptance

Use normal CI, required review, merge and automatic Cloudflare deployment. Do
not restart production or generate staging work solely for evidence. The first
deployment replaces older code, so it cannot itself demonstrate a drain by the
new outgoing dispatcher. On a subsequent normal stop, inspect bounded
`attempt_timing.recent` for `shutdown_result`, the row's completion and successor
claim times, and the normal container lifecycle. Alternatively use retained
`outbox_shutdown_*` logs. Confirm cancellation order and dispatch reconciliation
from the existing candidate journal without changing it.

Keep the issue open until a real production observation establishes reduced
delay with no lost command, bypassed cancellation or duplicate dispatch. Record
the exact deployment, timings and limitations in the PR and issue. No historical
attempt can be reconstructed from timing records introduced by this release.
