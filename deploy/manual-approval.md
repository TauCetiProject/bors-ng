# Human approval of restricted TauCeti paths

After reviewing a TauCeti PR, a repository admin can issue the existing `bors r+`
or `bors merge` command. Bors checks the actual authenticated command author's
current GitHub repository admin role, records their user ID and the live head and
merge base when accepting the command, and requests a rerun of TauCeti's trusted
`pr-build` workflow. Bors confirms the accepted head and merge base in a comment.
Plain comments follow bors's live-head semantics,
with an additional check that the comment does not predate a head or target change
observed by bors. Review and inline review commands also pin the reviewed commit ID.
A claimed reviewer name
in `bors r=an-admin` does not grant this authority. Other users and repositories keep
the existing policy. `bors retry` reuses ordinary review intent and never extends
human scope authority to a new revision.

The workflow reads approval from bors's HTTPS observation endpoint. Approval waives
only the restricted-path rule. Build, axiom/lint audits, Lake configuration
attestation, pin validation, size guards and incomplete-read failures retain their
existing behavior. In particular, a Lakefile edit still fails configuration
attestation; this mechanism does not authorize executing proposed build tooling.
Bors posts pending `scope` when granting or revoking approval, and waits for an
actual green verdict from trusted CI rather than skipping the check. New red scope
verdicts reject admission normally. TauCeti main uses the trusted base's
`bors.toml` requirements, so candidate changes cannot remove its required checks.

The observation exposes `scope_approvals` entries containing `pr`, `actor_id`,
`head_sha` and `merge_base_sha`. Staging observations also expose the immutable
`base_sha`. TauCeti verifies the batch ID, staging head, base and membership
against that endpoint, and requires approval for every member with restricted
paths. Every restricted path in the combined diff must also belong to an approved
member's diff. File listings must be complete and include rename sources and deletions.
Both bors and TauCeti CI resolve admin authority from GitHub's live repository
effective permissions, including roles inherited through teams, rather than a
list of identities in the source. Removing the approving
account's admin role invalidates scope authority; an unavailable role lookup
fails closed. Dispatch payloads cannot authorize a scope exception.
The corresponding TauCeti CI verifier must also be deployed before additional
admins can approve restricted paths under this policy.

Approval survives a negative automatic `merge eligibility` verdict. TauCetiReview
continues publishing its usual policy verdict without knowing about this mechanism.
Explicit `bors r-`, cancellation, pushes, retargets, closure, draft conversion and
changed merge bases invalidate the human approval. A normal advance of main which
leaves the merge base unchanged does not require reapproval. Bors checks the live
revision and merge base at admission and immediately before pushing a green batch;
unavailable reads defer the merge. Membership snapshots preserve this check across
restarts and bisection. An explicit human approval can also take ownership of an
already queued automatic approval.

## Rollout

1. Merge and deploy the bors change, including its additive database migration.
   The standard deployment runs migrations before serving requests.
2. Merge TauCeti's CI change. Until that workflow is on main, the new dispatch is
   ignored and restricted-path PRs continue to fail scope; no bypass is enabled.
3. After reviewing a restricted-path PR, issue `bors merge` as a repository admin.
   Observe the requested PR CI rerun, green scope, staging CI and the eventual bors merge.

The initial CI change itself is subject to the previous scope policy. The new
approval cannot bootstrap it: both PR and staging CI still use main's old workflow.
Use the existing human infrastructure merge procedure while bors is drained, with
build and bump-guard still green. Select `MERGE_BACKEND=queue` to defer fresh bors
admissions and release unstarted batches, wait for the observation endpoint to show
no waiting or running bors batches, merge the reviewed CI change, then restore
`MERGE_BACKEND=bors`. Do not try to override staging scope after the old workflow
reports failure: bors has already failed that batch. After this rollout, ordinary
restricted-path PRs use only the existing human bors command.

Authority is managed through the repository admin role. Adding or removing admins
requires no source edits, secrets, labels or new commands.
