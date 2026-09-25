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

## Remaining one-time setup

1. Create a Neon organization on the Scale plan with billing, and provide a
   temporary organization API key through a local private file. The project
   should be in AWS `us-east-1`, with one primary PostgreSQL compute at 0.25 CU,
   scale-to-zero disabled, and 30-day history retention. The deployment uses a
   direct TLS connection string (not Neon's transaction pooler); bors uses its
   own small connection pool.
2. Register an organization-owned GitHub App using the prefilled URL printed by
   `node deploy/cloudflare/github-app-url.mjs`. Set a randomly generated webhook
   secret in the App UI; URL parameters cannot prefill it. Generate a private
   key and client secret. Install the App **only** on `TauCetiProject/TauCeti`
   for the pilot. Its URL is `https://bors.taucetiproject.org/`, webhook is
   `/webhook/github`, and OAuth callback is `/auth/github/callback`. GitHub may
   report a failed initial ping before the Worker is deployed.
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
4. Put the database URL, webhook secret, client secret, and GitHub App private
   key in separate local private files, outside the repository. Run
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
