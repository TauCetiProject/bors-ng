# Direct cancellation of withdrawn Actions candidates

Bors cancels the exact Actions run for an explicitly withdrawn staging batch.
The GitHub App installation requires Actions write permission. Check that the
installation has accepted this permission before deploying this path. No workflow
relay, public cancellation endpoint or pins to downstream helper files are needed.

The transaction marking a running batch canceled also writes its cancellation
intent to the existing outbox. Waiting batches do not create cancellation intents.
The intent records the repository, batch, candidate generation, staged head, base,
branch and dispatch time, and expires after thirty minutes. Deduplication survives
completion and restart. Surviving members retain their approvals and are requeued
through normal preflight.

Run discovery uses the recorded dispatch window and exact workflow title. Repository,
workflow, bot actor, candidate identity and the first attempt must match. Ambiguous
same-head/base generations cannot authorize cancellation. The discovered run ID is
saved once and cannot be replaced by a later lookup.

The API adapter rechecks the saved withdrawal and fetches the bound run immediately
before normal `POST /actions/runs/{id}/cancel`. It then checks withdrawal authority
again. Promotion intent, a changed generation or a reactivated batch revoke that
authority. An observed newer attempt or completed run prevents the POST. GitHub has
no attempt-conditional cancellation endpoint, so a human rerun between the final
GET and POST remains a small unavoidable race. No automatic reruns or force
cancellation are introduced.

HTTP 202 records acceptance; completion is checked on subsequent existing outbox
retries. HTTP 409 is terminal. Permission denial, missing resources and invalid
identity are explicit terminal outcomes. Transient errors retry with the existing
60/120/240/480/960-second backoff, six-claim maximum and absolute thirty-minute
expiry. Repeating a normal cancellation after an ambiguous transport failure is
safe and does not dispatch another workflow. Once acceptance is recorded, retries
only observe completion.

No HTTP runs inside the withdrawal transaction. Cancellation does not grant merge
approval or change build, audit, publication or promotion requirements. Observe the
next natural withdrawal after deployment to verify request-to-terminal timing and
runner release; do not synthesize production approvals or withdrawals for testing.
