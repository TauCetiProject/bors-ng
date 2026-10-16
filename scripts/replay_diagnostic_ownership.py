#!/usr/bin/env python3
"""Read an immutable journal capture and local Git objects; never fetch or dispatch."""
import argparse
import importlib.util
import json
from pathlib import Path


def helper(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).resolve().parents[1] / 'priv' / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('journal', type=Path, help='single candidate object from the read-only journal API')
parser.add_argument('git_dir', type=Path, help='local bare repository containing the exact candidate chain')
parser.add_argument('log', type=Path)
parser.add_argument('output', type=Path)
parser.add_argument('--gh-log', action='store_true', help='remove gh run view presentation columns')
args = parser.parse_args()
candidate = json.loads(args.journal.read_text())
assembly = next(e['details'] for e in reversed(candidate['events']) if e['kind'] == 'local_assembled')
assert assembly['candidate_id'] == candidate['id']
assert assembly['generation'] == candidate['generation']
assert assembly['base_sha'] == candidate['snapshot']['base_sha']
members = candidate['snapshot']['members']
assert all(m['head_sha'] == m['assembly_head_sha'] for m in members)
local = helper('local_git')
request = dict(root=str(args.git_dir.parent), repository='replay/local', timeout=15,
               identity={k: assembly[k] for k in ['candidate_id', 'generation', 'base_sha', 'members_digest']})
mirror = local.Mirror(request)
mirror.repo = args.git_dir.resolve()
hints = mirror.diagnostic_hints(assembly['base_sha'], members, assembly['commit'], assembly['evidence']['commits'])
text = args.log.read_text()
if args.gh_log:
    text = '\n'.join(line.split('\t', 2)[2] for line in text.splitlines()
                     if line.startswith('sandboxed-build\t') and len(line.split('\t', 2)) == 3)
observations = helper('failure_diagnostics').parse(text)
result = dict(candidate_id=candidate['id'], members=members, hints=hints,
              logs=dict(complete=True, jobs=[dict(observations=observations)]))
args.output.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(dict(members=len(members), ownership_complete=hints['ownership_complete'],
                     dependencies_complete=hints['dependencies_complete'], elapsed_ms=hints['elapsed_ms'],
                     observations=observations)))
