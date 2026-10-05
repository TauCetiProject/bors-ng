# Tau Ceti bors deployment

The Cloudflare Worker verifies GitHub webhooks and queues them durably. One
Container runs the bors release and stores state in PostgreSQL. R2 holds large
webhook payloads and delivery markers for 14 days. The account is
`tauceti` (`ec2169bdf033f56b009956d4b64ba8ef`); the Worker name and domain
are in `wrangler.jsonc`.

Cloudflare Builds cannot install Erlang and Elixir from a root `.tool-versions`.
Their asdf pins live in `.tool-versions.dev`; local developers copy that file
to the ignored `.tool-versions`. The Container's Dockerfile pins the production
toolchain separately.

## Provisioning status

Workers Paid is active. The queues `tauceti-bors-webhooks` and
`tauceti-bors-webhooks-dlq`, and the R2 bucket `tauceti-bors-webhooks` with
14-day payload and marker expiration, have been created in that account.
An existing Neon direct endpoint was supplied on 2026-09-27. Its credentials
successfully connected over verified TLS to PostgreSQL 18. The direct URI,
without libpq-only query parameters, is installed as the bootstrap Worker's
`DATABASE_URL` secret. The Neon organization `TauCetiProject`
(`org-summer-darkness-64936821`) is on Launch. Its existing project
`dawn-wildflower-89938913` has seven-day history, a 0.25–2 CU autoscaling
range, and scale-to-zero disabled. The direct connection was checked after
these settings were applied. The previous settings are saved locally in
`/home/kim/.config/tauceti-bors/neon-settings-before-launch.json`.
Do not create another Neon project for this deployment.

## Pilot status and next steps

Neon is ready. Bors uses its own small connection pool instead of Neon's
transaction pooler. The `provision_neon.py` script is retained for a future
clean installation; it must not be run for the already-supplied database.

The private GitHub App `tau-ceti-bors` is registered and installed only on
`TauCetiProject/TauCeti`. Its App ID is `5172678`; the webhook URL is
`https://bors.taucetiproject.org/webhook/github`. All seven Worker secrets
have been installed. Their source files and the Cloudflare Builds API token
are under `/home/kim/.config/tauceti-bors/` with private permissions.

Cloudflare Builds is connected to `TauCetiProject/bors-ng` and automatically
builds pushes to `master`. The build command is `npm ci`, the deploy command is
`npx wrangler deploy`, and the root directory is `/`. Workers Builds has Docker
available for the image; local deployment requires a usable Docker daemon.
Check the existing trigger with
`python3 deploy/cloudflare/configure_builds.py --token-file /home/kim/.config/tauceti-bors/cloudflare-builds-token --branch master`.
Use `--apply` only when the branch filter needs updating.

The public `/health/` route returns HTTP 200. A signed `ping` delivered to the
public Worker returned HTTP 202 and later had its exact delivery marker in R2,
confirming the Worker, Queue, bors Container, and R2 processing path. An
installation resync registered `TauCetiProject/TauCeti` in PostgreSQL with the
default `staging` and `trying` branches; the database's open patch count
matched GitHub's 111 open PRs on 2026-10-03.

One Queue consumer handles batches of ten. It forwards only completed check
runs and check suites on branches with `staging` or `trying` prefixes; the same
filter prevents new unrelated check events from entering the Queue. If the
bors project branch names change, update `src/webhook-filter.mjs` with them.
The dead letter queue retains 244 deliveries from the initial failed startup;
do not replay old approval comments blindly. The synthetic installation
resync restored repository and open PR state without replaying them.

Hosted pilots have completed successfully: batch 10 landed PRs 12062 and
11953 together as tested commit `725ce1d2d7f645819e2075192b268de44ea6b9c9`,
with staging CI and exact revision cache publication passing. The bors App has
the required legacy and ruleset push allowances; build and bump-guard checks
remain required. TauCeti PR 12207 (issue 12085) supports large batch lint
comparison while preserving whole-library axiom and dot audits.

`MERGE_BACKEND` controls admission. Both queues observe the other queue and
wait for its existing work to drain before admitting new work. A selected
backend change does not cancel running batches. Timed comparisons below use
this existing handoff mechanism.

The review App (`3947238`) writes exact-head `bors r+ sha=<head>` (or
`bors r+ single sha=<head>` for Lake pin changes) and `bors r- sha=<head>`
comments only when `MERGE_BACKEND=bors`. The bors webhook
checks GitHub's issuing App ID and re-fetches the live PR head before granting
reviewer authority. The fork dispatches staging commits to TauCeti's trusted
`pr-build` workflow. Bors then waits for `build`, `bump-guard`, and `scope` on
the combined staging SHA and bisects failures.

## Timed merge-backend comparison

The existing minute cron can run one 24-hour bors window followed by one 24-hour
GitHub merge-queue window. Arm it with the TauCeti repository variable
`MERGE_EXPERIMENT` containing:

```json
{"schema":"tauceti-merge.experiment/v1","id":"unique-experiment-id","duration_hours":24,"created_at":"CURRENT-UTC-ISO-TIME","enabled":true}
```

Use a unique ID and a creation time within the last fifteen minutes. Start with
`MERGE_BACKEND=queue`. The Worker selects bors, waits for fresh evidence that the
GitHub queue is empty, then starts the first clock. After 24 hours it selects
queue, waits for bors to drain, and starts the second clock. It finishes with
queue selected. Each handoff has a six-hour deadline; the absolute limit is
72 hours from creation. Expiring a handoff selects queue and lets existing work
drain through the normal admission gates; it does not cancel builds or shorten
CI timeouts.

State and write intents live in the existing singleton Durable Object's SQLite
storage. The controller runs before the Container health check, so restoring
queue does not require the Elixir process to be healthy. GitHub API failures
are retried by later cron invocations. Restoration depends on GitHub API and
Cloudflare cron availability. The existing GitHub App needs Variables: write;
the Worker requests an installation token restricted to TauCeti and that
permission. No new infrastructure or public mutation endpoint is needed.

Read current state at `/api/merge-experiment`. Minute observation archives carry
an `experiment` snapshot; TauCetiCI reports the measured windows and handoffs
separately. Each clock starts with the first fresh archived observation proving
that the outgoing queue is empty, at minute sampling resolution.

Disable by setting `MERGE_EXPERIMENT` to `{"enabled":false}` or deleting it. The
controller restores queue if it still owns the selection. Any change to
`MERGE_BACKEND`, including a rewrite to the same value with a new `updated_at`,
aborts automation and preserves that operator selection. Editing an active
plan also cancels it. Two distinct fresh observations with work in both queues
abort the experiment and restore queue when the selection is still owned.
The API has no atomic compare-and-swap: the controller rechecks the setting
immediately before writing, but a concurrent manual edit in that small interval
can race the write. Completed/aborted IDs never restart; a new run needs a new ID.

The controller leaves batching limits and CI timeouts as configured. Record
those settings and runner sizes when arming a comparison. Compare completed
windows using total recorded CI minutes, throughput, arrivals, backlog and
coverage; a single pair of days cannot separate all workload changes.
