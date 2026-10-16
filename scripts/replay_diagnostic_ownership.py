#!/usr/bin/env python3
"""Read an immutable journal capture and local Git objects; never fetch or dispatch."""
import argparse
import hashlib
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
parser.add_argument('--max-bytes', type=int, default=2 * 1024 ** 3, help='local repository byte bound')
args = parser.parse_args()
if args.max_bytes <= 0:
    parser.error('--max-bytes must be positive')
candidate = json.loads(args.journal.read_text())
assembly = next(e['details'] for e in reversed(candidate['events']) if e['kind'] == 'local_assembled')
members = candidate['snapshot']['members']
digest = hashlib.sha256(json.dumps(members, ensure_ascii=False, sort_keys=True,
                                   separators=(',', ':')).encode()).hexdigest()
if (assembly['candidate_id'] != candidate['id'] or assembly['generation'] != candidate['generation']
        or assembly['base_sha'] != candidate['snapshot']['base_sha'] or assembly['members_digest'] != digest
        or any(m['head_sha'] != m['assembly_head_sha'] for m in members)):
    raise ValueError('journal identity mismatch')
local = helper('local_git')
request = dict(root=str(args.git_dir.resolve()), repository='replay/local', timeout=15, max_bytes=args.max_bytes,
               identity={k: assembly[k] for k in ['candidate_id', 'generation', 'base_sha', 'members_digest']})
mirror = local.Mirror(request)
mirror.repo = args.git_dir.resolve()
hints = mirror.diagnostic_hints(assembly['base_sha'], members, assembly['commit'], assembly['evidence']['commits'])
if hints['ownership_complete'] and hints['provenance']['tree'] != assembly['tree']:
    raise ValueError('journal tree mismatch')
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
