# Direct import-provider retry hints

Refs TauCetiProject/TauCeti#17084. When complete, exact-run compiler diagnostics
name only unmapped project files, Bors can inspect their direct imports at the
failed candidate commit. Every inspected file must have the same blob and size
in the bound base. Provider paths are intersected with complete frozen changed
ownership. A changed provider is a scheduling suspect, possibly unrelated to the
error; this does not establish individual failure.

The `failure_diagnostics` event retains the separate `provider_lookup`, including
candidate/generation/head/base/member digest/run/attempt/jobs, diagnostic source
blobs, direct provider blobs, parsed-header cache misses and helper elapsed/CPU
milliseconds. Only the matching latest trusted failure classification can reuse
it. Unavailable results are final for that immutable observation across restarts.
Legacy observations keep their existing behavior.

The existing component planner consumes `provider_suspects` and records
`import_provider_hint`. Every original input remains in one sealed retry with
its frozen head, approval, age and retry lineage. Bundles, shared paths and known
import components still widen groups. Non-narrowing provider sets retain the
existing component/bisection behavior. Mixed mapped/unmapped diagnostics,
non-compiler findings and unknown paths are outside this initial fallback.

`FailureDiagnostics.diagnosis/2` deliberately excludes this evidence. Provider
hints cannot invoke PR52's provisional repair shortcut. There is no singleton
quarantine, repair request, review invalidation, closure, compatibility PASS,
dispatch approval, merge authority or compiled-cache publication from a lookup.
A later real isolated failure follows the existing authoritative path.

The lookup reads existing local Git objects without credentials, initialization,
fetch, checkout or candidate execution. It uses the existing full source header
parser and data-only blob cache, without building any project inventory. Bounds:
eight diagnostic paths, 512 characters per path, 256 KiB per file, 512 KiB total
source, 128 distinct direct providers, 64 KiB output, five seconds including lock
acquisition. A busy mirror is immediately unavailable. Missing objects/headers,
unsupported syntax, malformed output, stale identities and budget exhaustion
retain conservative retries. Cache reads/writes are capped at 4 MiB. The lookup
never waits for a backlog scan or performs mirror eviction.

## Exact candidate225 replay

The checked-in [capture](../test/fixtures/import-providers-candidate225.json)
contains the complete 22 frozen input snapshots, production ownership and parsed
diagnostics, exact binding, production baseline event3121, and the new lookup's
blob evidence. Production log parsing against the original 11,455,886-byte job
log reproduced the recorded observation exactly. Replaying the production helper
and pure planner selects PR16713 versus 21 members, preserving all 22 snapshots.
Without provider evidence the same planner returns fallback, and the unchanged
divider bisects 22 atomic units 11/11, as deployed event3121 did.

The failing Scheme.lean blob is `680466dc91e561fc87def21098cd2eceb4673e42`
(15,860 bytes) in both candidate and base. Four direct project imports exist;
Character.lean and KernelPoints.lean have PR16713 as their changed owner. The
other two have no changed owner. This is counterfactual scheduling evidence,
not proof that PR16713 failed alone, a deployed benefit, or measured time saved.
The earlier candidate223 is not a benefit claim.

The local replay measured 32 ms with one uncached parsed header and 11 ms with
its parsed header cached (helper-process CPU 5/4 ms, excluding Git child CPU).
Git objects and OS caches were already local in both cases. These timings exclude
Python startup, Elixir orchestration and production contention; production helper
service/cold/warm latency is unmeasured. The production helper has one vCPU; no
full-core throughput or saved-build-time extrapolation is justified.

Reproduce offline with a disposable bare repository holding the exact objects:

```sh
python3 scripts/replay_import_provider_lookup.py candidate225.json replay.git batch834-failed.log replay225.json
MIX_ENV=test BORS_DATABASE=postgresql mix run --no-start scripts/replay_diagnostic_partition.exs replay225.json partition225.json
```

Required CI/review and the normal root-coordinated deployment remain mandatory.
Keep #17084 open until a genuine new production case records a lookup, its retry
partition and resulting retries/merge outcome in the public candidate journal:
https://bors.taucetiproject.org/repositories/1/candidates?base=main . A replay is
not rollout acceptance. Root retains staging watches and release coordination.
Rollback uses the existing `BORS_FAILURE_DIAGNOSTICS=off` switch. No migration,
service, workflow pin, new dispatch or deployment restart is introduced here.
