#!/usr/bin/env python3
"""Read-only portable reduction of #16244 archives and #16887 evidence.

No API calls, no builds, no writes outside the requested output. Paths are
explicit inputs so a reviewer can reproduce from the linked source checkout.
"""
import argparse
import datetime as dt
import gzip
import hashlib
import json
from pathlib import Path


def stamp(s):
    return dt.datetime.fromisoformat(s.replace('Z', '+00:00')).timestamp()


def capture(records, evidence):
    sources, observations = [], {}
    for p in sorted(records.glob('observations/TauCeti/**/*.ndjson.gz')):
        sources.append({'path': str(p.relative_to(records)), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()})
        for line in gzip.open(p, 'rt'):
            row = json.loads(line)
            observations[row['observed_at']] = row
    end = max(map(stamp, observations))
    rows = [(stamp(at), r) for at, r in sorted(observations.items()) if stamp(at) >= end - 86400]
    heads, current = {}, {}
    first = rows[0][0]
    previous = first
    for at, row in rows:
        for member in sorted(row['eligible'], key=lambda m: (m['pr'], m['head_sha'])):
            pr, head = member['pr'], member['head_sha']
            key = f'{pr}@{head}'
            old = current.get(pr)
            if old and old != key and old in heads:
                heads[old]['stale_at'] = at
            current[pr] = key
            if member.get('ready') is False:
                if key in heads:
                    heads[key]['stale_at'] = at
                continue
            if key not in heads:
                heads[key] = {'pr': pr, 'head_sha': head, 'lo': previous, 'hi': at, 'stale_at': None}
        previous = at
    metadata = json.loads((evidence / 'roadmap-metadata-all.json').read_text())
    analysis = json.loads((evidence / 'roadmap-batching-analysis.json').read_text())
    labels_at = stamp(analysis['observed_at'])
    candidates = json.loads((evidence / 'capacity-candidates-latest.json').read_text())['candidates']
    index = {c['id']: c for c in candidates}
    for row in analysis['rows']:
        c = index[row['candidate']]
        row['observed_at'] = c['observed_at'] / 1_000_000
        row['exact_heads'] = [f"{m['pr']}@{m['head_sha']}" for m in c['snapshot']['members']]
        ages = [(c['observed_at'] - m['original_admitted_at']) / 1_000_000
                for m in c['snapshot']['members'] if m.get('original_admitted_at')]
        row['max_admission_age_s'] = max(ages) if ages else None
    proximity = {}
    for same in [True, False]:
        pairs = [p for p in analysis['pairs'] if p['same'] == same]
        proximity['same_area' if same else 'different_area'] = {
            'exact_head_pairs': len(pairs),
            'shared_changed_file_or_import_edge': sum(p['overlap'] or p['import_edge'] for p in pairs)}
    for h in heads.values():
        m = metadata.get(str(h['pr']), {})
        h['labels'] = sorted(n['name'] for n in m.get('labels', {}).get('nodes', [])) if m.get('headRefOid') == h['head_sha'] else None
    return {
        'schema': 'bors.diversity-workload/v1', 'source_commit': '92e8b391eeec7334fd1c7d976e6006b12b7579dc',
        'source_repository': 'https://github.com/TauCetiProject/TauCetiCI',
        'evidence_source': 'https://github.com/TauCetiProject/TauCeti/issues/16226#issuecomment-6181540526',
        'sources': sources, 'start': first, 'end': end, 'labels_observed_at': labels_at,
        'snapshots': len(rows), 'heads': sorted(heads.values(), key=lambda h: (h['hi'], h['pr'], h['head_sha'])),
        'observed_candidates': analysis['rows'], 'interaction_proximity': proximity,
        'concrete_interaction': json.loads((evidence / 'roadmap-batch757-exact-replay.json').read_text()),
        'limits': [
            'Eligibility samples are not exact admission timestamps; initial backlog age is unknown.',
            'A missing item in a bounded snapshot is not a withdrawal. Only explicit ready=false or changed head invalidates.',
            'Labels are retrospective and head matching only; no historical label index exists.',
            'Priority, atomic bundles and physical build work are unknown in these eligibility observations.',
            'Candidate outcomes and changed-file/import hints are retrospective, never features or failure oracles.',
            'All selected formations have unknown acceptance, retry and runner/CPU cost outcomes.'
        ]}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--records', type=Path, required=True)
    p.add_argument('--evidence', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    a = p.parse_args()
    a.out.write_text(json.dumps(capture(a.records, a.evidence), sort_keys=True, indent=2) + '\n')


if __name__ == '__main__':
    main()
