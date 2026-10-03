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
builds pushes to `feat/cloudflare-bors`, the branch of draft PR #1. The build
command is `npm ci`, the deploy command is `npx wrangler deploy`, and the root
directory is `/`. Workers Builds has Docker available for the image;
`wrangler deploy` on this host cannot build it because this host has no usable
Docker daemon. After PR #1 is reviewed and merged, move the existing Builds
trigger to `master` with
`python3 deploy/cloudflare/configure_builds.py --token-file /home/kim/.config/tauceti-bors/cloudflare-builds-token --branch master --apply`.
The script updates the existing trigger instead of creating a second one.

GitHub webhook delivery to the Worker has returned HTTP 202. The public
health route and end-to-end webhook processing are still being verified.
Then test a staging batch and its exact revision cache before switching the
review App or changing `MERGE_BACKEND`.

Do not enable bors merge authority or set `MERGE_BACKEND=bors` until the
container is healthy, the App receives webhooks, staging CI succeeds on a
pilot batch, and its exact revision cache is public. The existing merge queue
continues running while the code and infrastructure are staged.

The review App (`3947238`) writes exact-head `bors r+ sha=<head>` (or
`bors r+ single sha=<head>` for Lake pin changes) and `bors r- sha=<head>`
comments only when `MERGE_BACKEND=bors`. The bors webhook
checks GitHub's issuing App ID and re-fetches the live PR head before granting
reviewer authority. The fork dispatches staging commits to TauCeti's trusted
`pr-build` workflow. Bors then waits for `build`, `bump-guard`, and `scope` on
the combined staging SHA and bisects failures.
