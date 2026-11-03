# Wake completed results held by a train guard

A completed root or speculative child can temporarily be held by a pending
member command or unfinished predecessor. Moving `last_polled` without a
dedicated wakeup left some finished results waiting for the thirty-minute
fallback, even after the original blocker cleared.

When a train guard holds a batch whose required status summary is success or
failure, enqueue the existing retry message for about sixty seconds later and
persist that deadline in `recovery_after`. Queue the durable message before
changing polling timestamps, so a crash or failed batch update cannot strand the
result. Existing recovery/promotion deadlines are respected. No counters or
approvals change, and running CI gets no new timers from this path.

Reuse the existing `retry_promotion` handler: its batch/project/head/state checks
discard stale messages, it respects any later retry deadlines, and it rereads
current CI and train/base/withdrawal guards before doing work. Startup's existing
recheck restoration uses the persisted deadline. Repeated status notifications
during the hold do not keep pushing the deadline out. If the guard still holds
at the retry, another bounded recheck is queued.

Completion diagnostics now report the scheduled wakeup with the hold reason.
There is no new schema, scheduler, polling loop, configurable timeout, or relaxed
admission/merge gate. Deploy through normal checks and the automatic live update;
keep admissions and running staging builds enabled.
