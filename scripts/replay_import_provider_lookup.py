#!/usr/bin/env python3
"""Replay production depth-one lookup against exact local objects, without fetch.

Usage: python3 scripts/replay_import_provider_lookup.py JOURNAL GIT_DIR JOB_LOG OUTPUT
Then run scripts/replay_diagnostic_partition.exs on OUTPUT. Accepts gh-rendered logs.
Measurements are local helper time, not production service latency or time saved.
"""
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import time


def helper(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).resolve().parents[1] / 'priv' / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    journal, git_dir, log, output = map(Path, sys.argv[1:])
    candidate = json.loads(journal.read_text())
    assembly = next(e['details'] for e in reversed(candidate['events']) if e['kind'] == 'local_assembled')
    diagnostic = next(e['details'] for e in reversed(candidate['events']) if e['kind'] == 'failure_diagnostics')
    baseline = next(e for e in reversed(candidate['events']) if e['kind'] == 'failure_partition')
    binding = diagnostic['binding']
    members = candidate['snapshot']['members']
    digest = hashlib.sha256(json.dumps(members, ensure_ascii=False, sort_keys=True,
                                       separators=(',', ':')).encode()).hexdigest()
    assert binding['members_digest'] == assembly['members_digest'] == digest
    assert binding['candidate_id'] == assembly['candidate_id'] == candidate['id']
    assert binding['generation'] == assembly['generation'] == candidate['generation']
    assert binding['base_sha'] == assembly['base_sha'] == candidate['snapshot']['base_sha']
    assert binding['commit'] == assembly['commit']
    assert all(m['head_sha'] == m['assembly_head_sha'] for m in members)
    text = log.read_text()
    if text.startswith('sandboxed-build\t'):
        text = '\n'.join(line.split('\t', 2)[2] for line in text.splitlines()
                         if line.startswith('sandboxed-build\t') and len(line.split('\t', 2)) == 3)
    observations = helper('failure_diagnostics').parse(text)
    assert len(diagnostic['result']['jobs']) == 1
    assert observations == diagnostic['result']['jobs'][0]['observations']
    assert all(o['kind'] == 'compiler_error' and o['paths'] for o in observations)
    paths = sorted({p for o in observations for p in o['paths']})
    local = helper('local_git')
    request = dict(root=str(output.resolve().parent / 'replay-mirror'), repository='replay/local',
                   operation='import_providers', binding=binding, paths=paths, timeout=5)
    local.validate(request)
    # Point the read operation at existing objects; isolate lock and header-cache
    # writes in the supplied disposable bare replay repository.
    measurements = []
    lookup = None
    for _ in range(2):
        mirror = local.Mirror(request)
        mirror.repo = git_dir.resolve()
        started = time.monotonic()
        lookup = mirror.run()
        measurements.append(dict(helper_wall_ms=round((time.monotonic() - started) * 1000),
                                 **{k: lookup[k] for k in ['elapsed_ms', 'cpu_ms', 'cold_blobs']}))
    result = dict(candidate_id=candidate['id'], members=members,
                  hints=assembly['evidence']['diagnostic_hints'], logs=diagnostic['result'],
                  binding=binding, provider_lookup=lookup, recorded_baseline=baseline,
                  local_measurements=measurements, counterfactual_only=True)
    output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(dict(candidate_id=candidate['id'], measurements=measurements,
                          files=lookup['files'], recorded_baseline=baseline)))


if __name__ == '__main__':
    main()
