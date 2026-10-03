# Tau Ceti bors deployment

The Cloudflare Worker verifies GitHub webhooks and queues them durably. One
Container runs the bors release and stores state in PostgreSQL. R2 holds large
webhook payloads and delivery markers for 14 days. The account is
`tauceti` (`ec2169bdf033f56b009956d4b64ba8ef`); the Worker name and domain
are in `wrangler.jsonc`.

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

## Remaining one-time setup

1. Neon is ready. Bors uses its own small connection pool instead of Neon's
   transaction pooler. The `provision_neon.py` script is retained for a future
   clean installation; it must not be run for the already-supplied database.
2. Open `https://tauceti-bors.tauceti-ec2.workers.dev/github-app/setup` and
   register the private App under TauCetiProject. The temporary setup Worker
   displays a one-time code after GitHub redirects back. Within one hour, pass
   that code on stdin to
   `python3 deploy/cloudflare/exchange_github_manifest.py`; the script stores
   the App ID, client secret, PEM key, and GitHub-generated webhook secret in
   local mode-0600 files. Install the App **only** on `TauCetiProject/TauCeti`
   for the pilot, using the URL the script prints. Its URL is
   `https://bors.taucetiproject.org/`, webhook is `/webhook/github`, and OAuth
   callback is `/auth/github/callback`. GitHub may report a failed initial ping
   before the bors Container is deployed. The older manual App setup URL from
   `github-app-url.mjs` remains a fallback.
3. Authorize the Cloudflare Workers Builds GitHub connection for the fork
   `TauCetiProject/bors-ng` only. A bootstrap Worker project named
   `tauceti-bors` already exists. Provide a user-scoped Cloudflare API token
   with Workers Builds Configuration Edit and Workers Scripts Read in a local
   private file. Create one Worker build token in Settings > Builds > API token,
   then run `python3 deploy/cloudflare/configure_builds.py --token-file PATH`
   to check prerequisites, followed by the same command with `--apply`.
   The script configures the reviewed `master` branch, repository root `/`,
   build command `npm ci`, and deploy command `npx wrangler deploy`; it does
   not start the first build. Workers Builds has Docker available for the
   Dockerfile image; `wrangler deploy` on this host cannot build it because this
   host has no usable Docker daemon.
4. Put the webhook secret, client secret, and GitHub App private key in separate
   local private files, outside the repository. The database URL is already
   installed and should be supplied in a private file for repeatable uploads. Run
   `python3 deploy/cloudflare/upload_secrets.py --help` for the upload command's
   arguments. It creates a reusable random `SECRET_KEY_BASE` file with mode
   0600 and sends all seven Worker secrets to Wrangler over stdin. It encodes
   the downloaded PEM as required by bors. The webhook secret must match the
   GitHub App UI. No secret belongs in Git.

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
