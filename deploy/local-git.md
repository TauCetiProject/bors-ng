# Local candidate assembly

Scope: TauCetiProject/TauCeti `main`, squash policy read at the immutable base,
with the existing `staging` branch. Other projects, branches and merge policies
continue using GitHub assembly.

The repository Actions variable `BORS_CANDIDATE_ASSEMBLER` selects `off` (also
missing, invalid or unreadable), `shadow`, or `local`. Read it once per
construction; changing it cannot replace an already running candidate. Shadow
constructs locally without pushing and records `local_assembled` followed by
`shadow_compared` with both trees. GitHub remains authoritative in shadow mode.
An unavailable shadow is recorded and does not block normal staging. On the next
normal poll, a running recorded candidate may also be replayed once: this reads
the exact GitHub commit tree, allocates no generation, pushes no ref and changes
no CI, status or promotion state.

The assembler consumes the candidate journal's ordered immutable heads. Metadata
reads check the live PR head before and after fetching commits and attribution.
The source SHA always comes from the immutable input. Bundles retain the existing
ordering, admission, cancellation and conflict/bisection behavior. Tree assembly
retains intermediate merge ancestry, while the landed chain has one single-parent
squash per PR with the existing message generator and PR author/committer. Commit
dates are construction time; commit IDs need not equal historical IDs.

The trusted Python helper uses `merge-tree --write-tree` and `commit-tree` in a
bare repository; it never checks out or executes PR code. Hooks, inherited Git
configuration, external credentials, replacement objects, file transport,
submodules and automatic background maintenance are disabled. Only the existing
bors installation credential enters the Git subprocess environment. No token is
written to disk, a remote URL, command arguments, logs, or candidate metadata.
Errors retain operation/status and bounded Git stderr/porcelain output, with
installation credentials, authorization headers and URL credentials redacted.
Promotion reports those details instead of replacing them with a generic 503;
helper startup/exit, invalid JSON and credential lookup failures remain distinct.

Mirrors live at `BORS_GIT_MIRROR_ROOT` (default `/tmp/bors-git-mirrors`) and persist
across assembler/BEAM restarts on that filesystem. They are an expendable source
object cache: container replacement or host loss cold-fetches exact objects again.
No durability of Cloudflare container disk is assumed. A root-wide OS flock
serializes fetch, construction, maintenance and pushes across processes and
releases automatically when the helper/Git process tree exits. Git inherits the
lock descriptor, and a parent-death signal stops its direct child after an
interrupted helper. Stale Git lock files are recovered under the root lock. The retained root budget is 2 GiB, with LRU mirror
eviction and synchronous pruning under the same lock. During Git commands the
helper checks the budget every 250 ms and kills the process group if exceeded;
a single sampling interval can transiently exceed the budget. Oversized mirrors
are removed on failure. Deploy with enough disk headroom for that interval and
Git's transient repack files. Each Git operation has a 120-second deadline and
each request, including lock acquisition, has a 600-second deadline. Only exact
missing SHAs are fetched; force-pushed/unavailable objects never fall back to a
PR's live ref. Local refs retain current inputs/result; they are replaced at the
next construction. Missing/evicted objects during publication defer safely.

Assembly accepts at most 128 ordered members, matching the current candidate
policy maximum. Existing waiting batches of 33–128 members need no migration:
the next normal attempt uses their immutable approvals and heads. Larger local
formations use the existing durable bundle-aware bisection path before metadata
or Git work; sealed child retries retain admission lineage and priority. An
indivisible oversized bundle stops for maintainer attention instead of repeatedly
attempting an unsupported formation. Lower policy admission limits still apply.
The helper's compatibility limit remains independently bounded at 32 members;
source checks chunk larger supplied lists into separate requests. Admission still
selects at most 32 singleton heads, and background reconciliation still checks
one stale head at a time. The existing time, disk, output and diagnostic bounds
apply to larger assemblies unchanged; optional diagnostics may remain incomplete.

Publication is separate from assembly. The caller validates the current candidate
identity and live heads, persists CI intent, then pushes exactly the assembled
commit to `refs/heads/staging` with an explicit expected old SHA lease. No other
remote ref is permitted by the helper, and no temporary remote refs can leak on
restart. A stale lease, timeout, auth/network failure or missing object leaves the
batch waiting with its approval snapshots and a five-minute recovery hold.
An ambiguous push may have delivered the SHA, but does not grant dispatch or
promotion authority: reconstruction gets a new generation. The existing trusted
staging dispatch, exact-candidate CI, current reviews, live policy checks and
fast-forward main promotion remain authoritative. The journal records local
construction and push sources separately. Ordinary PR compiled files never enter
shared staging caches; source mirror objects grant no publication/proof authority.

## Rollout

1. Run the full database suites and real Git acceptance checks. Replay historical
   candidates against their exact landed trees, including the measured batch 636.
2. Land the PR on upstream `master` after required checks/review and verify the
   deployed release image includes Git >= 2.38 and Python 3.
3. Set `BORS_CANDIDATE_ASSEMBLER=shadow` in **TauCetiProject/TauCeti**. Observe a
   newly constructed production candidate with matching local/GitHub trees and
   unchanged observation, approval and exact-head contracts.
4. Set the variable to `local`. Verify a new production candidate's immutable
   members, local tree, leased staging push, trusted dispatch and exact CI result.
   Verify its normal main promotion with current approvals before closing #16238.
5. Roll back by setting `off` or deleting the variable. Existing running commits
   retain their CI/promotion rules. Delete expendable mirrors only while no helper
   owns the root lock; no database or queue reset is required.
