#!/usr/bin/env python3
"""Reduce deterministic replay traces to a reviewable advisory (no oracle)."""
import argparse
import collections
import hashlib
import json
import statistics
from pathlib import Path


def summary(workload, replay, trace_digest, fixture_digest):
    lookup = {f"{h['pr']}@{h['head_sha']}": h for h in workload['heads']}
    midpoint = (workload['start'] + workload['end']) / 2
    scenarios = []
    strict_changes = 0
    for row in replay['scenarios']:
        base = next(r for r in replay['scenarios'] if all(r[k] == row[k] for k in
            ['coverage', 'admission_bound', 'cap', 'arrival_multiplier']) and r['mode'] == 'fifo')
        formations = row['formations']
        changed = [r['members'] for r in formations] != [r['members'] for r in base['formations']]
        strict_changes += row['coverage'] == 'as_of' and changed
        cohorts = collections.defaultdict(list)
        for formation in formations:
            for key, wait in zip(formation['members'], formation['waits_s'], strict=True):
                h = lookup[key]
                cohorts['early' if h['hi'] < midpoint else 'temporal_holdout'].append(wait)
                # All versions of a PR stay on one side; no future outcome or
                # label score is fitted on either side. Descriptive only.
                cohorts['lineage_holdout' if h['pr'] % 2 else 'lineage_reference'].append(wait)
        out = {k: row[k] for k in ['coverage', 'admission_bound', 'cap', 'arrival_multiplier',
            'mode', 'input_heads', 'attempted_heads', 'invalidated_heads', 'batches', 'same_area_pairs',
            'p90_dispatch_wait_s', 'max_dispatch_wait_s', 'simulated_lane_minutes']}
        out.update({'changed_composition': changed,
                    'formation_sha256': hashlib.sha256(json.dumps(formations, sort_keys=True).encode()).hexdigest(),
                    'fallback_counts': dict(collections.Counter(r['fallback'] or 'none' for r in formations)),
                    'cohort_dispatch': {k: {'heads': len(v), 'mean_wait_s': statistics.mean(v), 'max_wait_s': max(v)} for k, v in sorted(cohorts.items())}})
        assert row['attempted_heads'] + row['invalidated_heads'] == row['input_heads']
        scenarios.append(out)
    controls = []
    rows = sorted(workload['observed_candidates'], key=lambda r: r['observed_at'])
    split = rows[len(rows) // 2]['observed_at']
    for low, high in [(1, 1), (2, 8), (9, 16), (17, 32), (33, 128)]:
        for holdout in [False, True]:
            subset = [r for r in rows if low <= r['n'] <= high and (r['observed_at'] >= split) == holdout]
            ages = [r['max_admission_age_s'] for r in subset if r['max_admission_age_s'] is not None]
            controls.append({'size': [low, high], 'temporal_holdout': holdout,
                             'candidates': len(subset), 'outcomes': dict(collections.Counter(r['outcome'] for r in subset)),
                             'same_area_pairs': sum(r['same_pairs'] for r in subset),
                             'median_max_admission_age_s': statistics.median(ages) if ages else None,
                             'physical_work_control': 'unavailable'})
    components = []
    for row in rows:
        heads = set(row['exact_heads'])
        joined = [c for c in components if c & heads]
        components = [c for c in components if not c & heads]
        components.append(set.union(heads, *joined))
    return {'schema': 'bors.diversity-evidence/v1', 'production_enabled': False,
            'recommendation': 'retain production policy; no identified accepted-throughput benefit',
            'fixture_sha256': fixture_digest,
            'trace_sha256': trace_digest, 'strict_as_of_composition_changes': strict_changes,
            'captured_head_versions': len(workload['heads']), 'eligibility_snapshots': workload['snapshots'],
            'retrospective_exact_head_label_matches': sum(h['labels'] is not None for h in workload['heads']),
            'interaction_proximity': workload['interaction_proximity'],
            'observed_size_time_age_controls': controls,
            'shared_exact_head_lineage_component_sizes': sorted(map(len, components), reverse=True),
            'limits': workload['limits'], 'causal_metrics': replay['causal_metrics'], 'scenarios': scenarios}


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--workload', required=True)
    p.add_argument('--replay', required=True)
    p.add_argument('--out', required=True)
    a = p.parse_args()
    trace = Path(a.replay).read_bytes()
    result = summary(json.loads(Path(a.workload).read_text()), json.loads(trace), hashlib.sha256(trace).hexdigest(), hashlib.sha256(Path(a.workload).read_bytes()).hexdigest())
    Path(a.out).write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    print(f"{len(result['scenarios'])} accounted scenarios; {result['strict_as_of_composition_changes']} strict as-of composition changes")
