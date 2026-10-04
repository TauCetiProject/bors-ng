# Switching Tau Ceti merge backends

## Operation

`MERGE_BACKEND` in TauCetiProject/TauCeti is the sole switch. An authenticated successful listing without it selects `queue`. Set exactly `queue` or `bors`; other values and unavailable observations defer fresh admissions.

```sh
gh variable set MERGE_BACKEND --repo TauCetiProject/TauCeti --body bors
gh variable set MERGE_BACKEND --repo TauCetiProject/TauCeti --body queue
```

New admissions to the incoming queue wait for the outgoing queue's **waiting and running** work on `main` to disappear. Existing bors batches, retry children and rebuilds continue after selecting queue; their starts still wait for GitHub's queue to be empty. Build deadlines start with builds, not with backend waiting. Switching does not cancel work, change timeouts or stop ordinary CI/reviews.

There is no ownership record. Observations are checked immediately before admission and before a bors batch starts. A rare check/act race can overlap queues. Bors fast-forwards only its tested commit; an intervening main update makes it rebuild instead of overwriting main. Repeated switches while draining can delay progress; allow a handoff to finish for interpretable experiments.

The live endpoint is `/repositories/1/active-batches?base=main`. It includes waiting/running batch IDs, immutable member heads, current-head outcomes, held approvals, and the latest heartbeat observation. Pilot/try branches and ordinary PR CI do not block handoffs. Missing or malformed observations defer admission.

Unsafe review decisions revoke approvals in both engines regardless of selection. A live new review delays admission while preserving a previously green standing approval. Auto-admission never retries a terminal bors failure at the same head; fix/push or use the existing explicit bors retry.

## Liveness

Cloudflare's existing minute cron observes queue counts and ready-to-merge labels. Labels are hints only: the trusted Review sweep rechecks CI, scope, exact head, merge base and every review rubric. When pending hinted work exists and the outgoing queue is empty, heartbeat dispatches `tauceti-merge-reconcile` at most once per five minutes. The hourly sweep remains a backstop. Held approvals are recovered after container restarts. Restarting the singleton can reset the cost throttle; it cannot grant queue ownership.

The internal reconcile route uses the existing webhook secret and is blocked on the public Worker. Measurements are archived as immutable timestamped objects under `merge-observations/` in the existing R2 bucket. They do not control admission. `/api/merge-observations?day=YYYY-MM-DD` returns paginated public measurements for TauCetiCI.

## Rollout

1. Both Apps and installations need Variables read (`actions_variables: read`). Bors also retains contents write for repository dispatch. No new Cloudflare service, account tier or secret is needed.
2. Deploy the bors PR (nullable approved-head migration included). Keep GitHub selected. Verify health, the main observation schema, minute archives, and that no main bors batch starts.
3. Merge the Review PR. Its tokens request Variables read explicitly. Re-pin all TauCeti reusable workflow callers and the checkout-merge-policy action together to that immutable commit; merge the TauCeti workflow PR. Its main sweep receives repository_dispatch and supports both engines.
4. Deploy the TauCetiCI collector/report updates. Run the trusted sweep with `dry-run=true`. Confirm it defers correctly while outgoing work exists and reports eligible PRs without mutations.
5. Before the first production trial, add the tau-ceti-bors GitHub App (5172678) as an always bypass actor to main ruleset 17824807. Keep that authority while alternating so outgoing bors work can finish. Retain required CI statuses and all review policy.
6. Manually select bors, observe several normal batches, then select queue and verify automatic drain/recovery. Do not replay historical webhook DLQ deliveries. Do not schedule automatic alternation yet.

## Experiment interpretation

Compare total merge-validation job minutes per actually merged PR, including failures, bisection, rebuilds, reruns, cancellations and publication jobs. Keep ordinary PR CI separate; stratify runner sizes and cache state. Engine attribution follows `merge_group` or the trusted bors staging dispatch, never the current variable. Bors Actions run.head_sha is the workflow source, so the collector uses the trusted telemetry's actual tested head/base and batch members.

Minute observations capture the setting's updated timestamp, eligible heads, pending count, both queues' counts, and overlaps. They support arrival rates, backlog and first-observed eligibility latency. Missing samples are gaps, not empty queues. Drain periods belong to the outgoing engine. Compare complete windows with enough successful merges; unequal traffic, runner/cache changes and the first-window backlog can confound a short trial.
