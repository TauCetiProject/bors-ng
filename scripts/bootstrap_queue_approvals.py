"""One-time, exact-head bootstrap of audited legacy approvals; dry-run by default."""
import argparse
import json
import subprocess
from pathlib import Path
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

REPO = 'TauCetiProject/TauCeti'
HOLDS = {'keep', 'hold', 'wip', 'human', 'do-not-close'}
REVIEW_APP = 3947238
REVIEWER = 'tauceti-review-bot[bot]'

def gh(path, *, pages=False):
    args = ['gh', 'api', path]
    if pages:
        args += ['--paginate', '--slurp']
    result = subprocess.run(args, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError('GitHub read unavailable')
    return json.loads(result.stdout)

def inspect(conn, candidate):
    pr, head = candidate['pr'], candidate['head_sha']
    row = conn.execute('''SELECT id,commit AS head,open,is_draft,bundle_id,bundle_reviewer,manual_approval,queue_approval,merge_eligibility_id,merge_eligibility
        FROM patches WHERE project_id=1 AND pr_xref=%s''', (pr,)).fetchone()
    if not row or row['head'] != head or not row['open'] or row['is_draft']:
        return None, 'revision changed or PR unavailable'
    if row['queue_approval'] is not None:
        return None, 'durable intent already recorded; native recovery owns this head'
    if row['bundle_id'] or row['bundle_reviewer'] or row['manual_approval']:
        return None, 'bundle or separate held/manual approval needs its existing recovery path'
    proof = row['merge_eligibility'] or {}
    if not (proof.get('eligible') and proof.get('review_safe') and proof.get('head_sha') == head):
        return None, 'no accepted exact-head automatic approval'
    if conn.execute('''SELECT 1 FROM link_patch_batch l JOIN batches b ON b.id=l.batch_id
        WHERE l.patch_id=%s AND b.state IN (0,1) LIMIT 1''', (row['id'],)).fetchone():
        return None, 'already represented'
    # Legacy withdrawals had no durable tombstone. Do not infer consent from
    # eligibility if the most recent recorded queue command withdrew it.
    logs = conn.execute('SELECT cmd FROM log WHERE patch_id=%s ORDER BY id DESC', (row['id'],)).fetchall()
    for log in logs:
        cmd = bytes(log['cmd'])
        if b'deactivate' in cmd or b'cancel_all' in cmd:
            return None, 'recorded withdrawal requires fresh human approval'
        if b'activate' in cmd or b'reviewed' in cmd:
            break
    # A failed singleton with a failed staging status is genuine integration
    # evidence, unlike a failed parent batch whose retry child was deleted.
    terminal = conn.execute('''SELECT b.id,b.state,
         (SELECT count(*) FROM link_patch_batch x WHERE x.batch_id=b.id) AS members,
         EXISTS(SELECT 1 FROM statuses s WHERE s.batch_id=b.id) AS had_ci
         FROM link_patch_batch l JOIN batches b ON b.id=l.batch_id
         WHERE l.patch_id=%s AND l.head_sha=%s ORDER BY b.id DESC LIMIT 1''', (row['id'], head)).fetchone()
    if terminal and terminal['id'] != candidate.get('batch_id'):
        return None, 'batch history changed since the audit'
    if terminal and (terminal['state'] == 2 or
            (terminal['members'] == 1 and terminal['state'] == 3 and
             (terminal['had_ci'] or terminal['id'] != 475))):
        return None, 'terminal individual failure or already merged'
    live = gh(f'repos/{REPO}/pulls/{pr}')
    if live['state'] != 'open' or live['draft'] or live['head']['sha'] != head or live['base']['ref'] != 'main':
        return None, 'live PR changed'
    if HOLDS & {label['name'].lower() for label in live['labels']}:
        return None, 'live hold'
    pages = gh(f'repos/{REPO}/commits/{head}/check-runs?filter=all&per_page=100', pages=True)
    checks = [c for page in pages for c in page['check_runs']
              if c['name'] == 'merge eligibility' and c['app']['id'] == REVIEW_APP]
    if not checks:
        return None, 'no trusted live eligibility check'
    check = max(checks, key=lambda c: c['id'])
    data = json.loads(check.get('external_id') or '{}')
    if not (check['head_sha'] == head and check['status'] == 'completed' and check['conclusion'] == 'success'
            and data.get('schema') == 'tauceti-merge.eligibility/v1' and data.get('repo') == REPO
            and data.get('pr') == pr and data.get('eligible') and data.get('review_safe')):
        return None, 'live eligibility is no longer valid'
    row['pr'] = pr
    return row, 'eligible for native Bors recovery'

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidates', type=Path, required=True)
    parser.add_argument('--database-url-file', type=Path,
                        default=Path('/home/kim/.config/tauceti-bors/database-url'))
    parser.add_argument('--limit', type=int, default=1)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--audit', type=Path, help='reviewed exact-head crash/race provenance manifest; required for apply')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    url = args.database_url_file.read_text().strip()
    results = []
    qualified = []
    candidates = json.loads(args.candidates.read_text())
    if args.apply:
        if args.audit is None:
            parser.error('--apply requires an audited provenance manifest')
        audit = json.loads(args.audit.read_text())
        approved = {(r['pr'], r['head_sha'], r['batch_id']) for r in audit['recoverable']}
        if not audit.get('withdrawals_checked'):
            parser.error('audit must explicitly check legacy withdrawals')
        candidates = [r for r in candidates if (r['pr'],r['head_sha'],r.get('batch_id')) in approved]
    with psycopg.connect(url, options='-c default_transaction_read_only=on -c statement_timeout=30000',
                         row_factory=dict_row) as conn:
        project = conn.execute('SELECT id FROM projects WHERE name=%s', (REPO,)).fetchone()
        if not project or project['id'] != 1:
            raise RuntimeError('configured project ID does not match the production repository')
        for candidate in candidates:
            try:
                row, reason = inspect(conn, candidate)
            except Exception as error:
                row, reason = None, f'read deferred ({type(error).__name__})'
            results.append({'pr': candidate['pr'], 'head_sha': candidate['head_sha'], 'reason': reason})
            if row:
                qualified.append(row)
                if len(qualified) >= args.limit:
                    break
    if args.apply:
        backend = gh(f'repos/{REPO}/actions/variables/MERGE_BACKEND')
        if backend['value'] != 'bors':
            raise RuntimeError('Bors is not the selected backend')
        # Native Bors still performs all normal live admission/preflight checks.
        # This only restores the durable approval accepted before the bug lost it.
        with psycopg.connect(url, options='-c statement_timeout=30000', row_factory=dict_row) as conn:
            for row in qualified:
                approval = {'head_sha': row['head'], 'reviewer': REVIEWER, 'state': 'approved', 'source': 'legacy-bootstrap-2026-10-12'}
                changed = conn.execute('''UPDATE patches p SET queue_approval=%s,queue_pending=true
                    WHERE p.id=%s AND p.commit=%s AND p.open AND NOT p.is_draft
                      AND p.queue_approval IS NULL AND NOT p.queue_pending
                      AND p.bundle_id IS NULL AND p.bundle_reviewer IS NULL AND p.manual_approval IS NULL
                      AND p.merge_eligibility_id=%s AND p.merge_eligibility=%s
                      AND NOT EXISTS(SELECT 1 FROM link_patch_batch l JOIN batches b ON b.id=l.batch_id
                                     WHERE l.patch_id=p.id AND b.state IN (0,1))
                    RETURNING p.pr_xref''',
                    (Jsonb(approval), row['id'], row['head'], row['merge_eligibility_id'], Jsonb(row['merge_eligibility']))).fetchone()
                for result in results:
                    if result['pr'] == row['pr']:
                        result['restored_intent'] = changed is not None
    args.output.write_text(json.dumps({'applied': args.apply, 'results': results}, indent=2)+'\n')
    print(json.dumps({'applied': args.apply, 'checked': len(results), 'qualified': len(qualified),
                      'restored': sum(bool(r.get('restored_intent')) for r in results)}))

if __name__ == '__main__':
    main()
