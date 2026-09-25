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
   `TauCetiProject/bors-ng` only. Create a Worker project named `tauceti-bors`
   on the `tauceti` account, with repository root `/`, build command `npm ci`,
   and deploy command `npx wrangler deploy`. Set its production branch to the
   reviewed deployment branch. Workers Builds has Docker available for the
   Dockerfile image; `wrangler deploy` on this host cannot build it because this
   host has no usable Docker daemon.
4. Put these Worker secrets in the Cloudflare dashboard (or `wrangler secret
   put`), never in Git: `DATABASE_URL`, `SECRET_KEY_BASE`,
   `GITHUB_WEBHOOK_SECRET`, `GITHUB_CLIENT_ID`, `GITHUB_CLIENT_SECRET`,
   `GITHUB_INTEGRATION_ID`, and `GITHUB_INTEGRATION_PEM`. The PEM value is the
   base64 encoding of the downloaded GitHub private key. `SECRET_KEY_BASE` is
   a random 64-byte value. The webhook secret must match the GitHub App UI.

Do not enable bors merge authority or set `MERGE_BACKEND=bors` until the
container is healthy, the App receives webhooks, staging CI succeeds on a
pilot batch, and its exact revision cache is public. The existing merge queue
continues running while the code and infrastructure are staged.

The review App (`3947238`) writes exact-head `bors r+ sha=<head>` or
`bors r- sha=<head>` comments only when `MERGE_BACKEND=bors`. The bors webhook
checks GitHub's issuing App ID and re-fetches the live PR head before granting
reviewer authority. The fork dispatches staging commits to TauCeti's trusted
`pr-build` workflow. Bors then waits for `build`, `bump-guard`, and `scope` on
the combined staging SHA and bisects failures.
