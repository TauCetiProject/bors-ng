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
builds pushes to `master`. Bors PR #1 merged on 2026-10-03 at
`ddddbc77b6357796d656b638d2ce6c8f562f8d08`. The build command is `npm ci`,
the deploy command is `npx wrangler deploy`, and the root directory is `/`.
Workers Builds has Docker available for the image; this host has no usable
Docker daemon. To repair the existing trigger, use
`python3 deploy/cloudflare/configure_builds.py --token-file /home/kim/.config/tauceti-bors/cloudflare-builds-token --branch master --apply`.
The script updates the existing trigger instead of creating a second one.

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

TauCeti staging CI PR #8935 also merged. The trusted staging workflow and
public cache pilot passed on 2026-10-03:
[run 37121450575](https://github.com/TauCetiProject/TauCeti/actions/runs/37121450575)
tested staging SHA `e6df9b1c64087b269c93f4bc73cdcb217d22b676`. Anonymous
readback returned HTTP 200 for the revision map (10,025 mappings) and a
referenced artifact. The publisher checked the public map against its staged
outputs. This pilot assembled staging with GitHub's merge API; it establishes
the CI/cache path, not the bors batcher's merge behavior.

The full-library lint exposed three existing simp normal-form violations on
main. [TauCeti PR #11336](https://github.com/TauCetiProject/TauCeti/pull/11336)
repairs them; the successful pilot includes its functional repair. Its current
head passed CI and all review rubrics. Admission to the existing merge queue
is waiting for the reservation held by Mathlib bump PR #11090. The repair must
land through the normal review pipeline before production batches use main.

The actual bors batcher also passed an isolated test:
[run 37123267072](https://github.com/TauCetiProject/TauCeti/actions/runs/37123267072).
Diagnostic PRs #11346 and #11347 copied already-reviewed exact heads into a
bundle targeting `bors-pilot-base`. Bors created one batch (database ID 1) at
`8c4337f63aca7090e784300e7d8a906a9f4be673`, dispatched trusted CI, waited for
all three statuses, advanced the isolated branch to that exact commit, and
closed both diagnostic PRs. Anonymous readback confirmed its public revision
map (10,027 mappings) and a referenced artifact. Build/audit took 7m38s;
publication took 2m47s. These are pilot timings, not a capacity estimate.
The temporary pilot branches were removed after verification; neither PR
targeted main. This tests successful batching and merge completion. Failure
bisection and sustained load have not been exercised in production.

Production still uses GitHub's merge queue: `MERGE_BACKEND` is unset, and the
bors App has no main-ruleset bypass. The queue's build concurrency was
temporarily reduced from five to one for the pilot and restored to five after
the batch finished.

Before cutover, require a healthy Container, working webhooks, a successful
actual bors batch, public exact-revision cache, and the lint repair on main.
Then coordinate queue drainage, grant bors the required main update authority,
and set `MERGE_BACKEND=bors` so approvals arrive from the review App. Verify
the first production batch before retiring the old queue path.

The review App (`3947238`) writes exact-head `bors r+ sha=<head>` (or
`bors r+ single sha=<head>` for Lake pin changes) and `bors r- sha=<head>`
comments only when `MERGE_BACKEND=bors`. The bors webhook
checks GitHub's issuing App ID and re-fetches the live PR head before granting
reviewer authority. The fork dispatches staging commits to TauCeti's trusted
`pr-build` workflow. Bors then waits for `build`, `bump-guard`, and `scope` on
the combined staging SHA and bisects failures.
