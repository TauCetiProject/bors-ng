"""Real Git acceptance checks, invoked by ExUnit on every supported CI matrix."""
import concurrent.futures
import importlib.util
import json
import multiprocessing
import os
from pathlib import Path
import subprocess
import shutil
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('local_git', Path(__file__).parents[1] / 'priv/local_git.py')
local = importlib.util.module_from_spec(spec)
spec.loader.exec_module(local)


def git(path, *args, input=None):
    env = os.environ | dict(GIT_AUTHOR_NAME='Author', GIT_AUTHOR_EMAIL='author@example.invalid',
                            GIT_COMMITTER_NAME='Author', GIT_COMMITTER_EMAIL='author@example.invalid')
    return subprocess.check_output(['git', '-C', str(path), *args], input=input, text=True, env=env).strip()


class TestMirror(local.Mirror):
    def __init__(self, request, upstream):
        super().__init__(request)
        self.url = str(upstream)

    def git(self, *args, **kwargs):
        # Only this test adapter permits a file transport; production forbids it.
        return super().git('-c', 'protocol.file.allow=always', *args, **kwargs)


def assemble_worker(request, upstream):
    return TestMirror(request, upstream).run()


class LocalGitTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.upstream = self.root / 'upstream'
        self.upstream.mkdir()
        git(self.upstream, 'init', '-q')
        (self.upstream / 'file').write_text('one\ntwo\nthree\n')
        git(self.upstream, 'add', '.')
        git(self.upstream, 'commit', '-qm', 'base')
        self.base = git(self.upstream, 'rev-parse', 'HEAD')
        self.request = dict(operation='assemble', root=str(self.root / 'mirrors'),
                            repository='example/repository', timeout=10, max_bytes=100 * 1024 ** 2,
                            identity=dict(candidate_id=1, generation=1, base_sha=self.base,
                                          members_digest='immutable'),
                            date='2026-10-14T00:00:00Z', members=[])

    def tearDown(self):
        self.tmp.cleanup()

    def commit(self, start, changes, message='change'):
        git(self.upstream, 'checkout', '-q', '--detach', start)
        for name, contents in changes.items():
            path = self.upstream / name
            if contents is None:
                path.unlink()
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(contents)
        git(self.upstream, 'add', '-A')
        git(self.upstream, 'commit', '-qm', message)
        return git(self.upstream, 'rev-parse', 'HEAD')

    def member(self, pr, sha):
        return dict(pr=pr, head_sha=sha, committer=dict(name='PR Author', email='pr@example.invalid'),
                    commit_message=f'PR {pr}\n\nCo-authored-by: Other <other@example.invalid>\n')

    def run_mirror(self):
        local.validate(self.request)
        return TestMirror(self.request, self.upstream).run()

    def test_order_squash_attribution_and_restart(self):
        one = self.commit(self.base, {'a': 'a'})
        two = self.commit(self.base, {'b': 'b'})
        self.request['members'] = [self.member(2, two), self.member(1, one)]
        result = self.run_mirror()
        mirror = Path(self.request['root']) / 'example--repository.git'
        head = result['result']['commit']
        self.assertEqual(git(mirror, 'rev-list', '--count', self.base + '..' + head), '2')
        self.assertEqual(git(mirror, 'show', '-s', '--format=%B', head), self.member(1, one)['commit_message'].strip())
        self.assertEqual(git(mirror, 'show', '-s', '--format=%an <%ae>|%cn <%ce>', head),
                         'PR Author <pr@example.invalid>|PR Author <pr@example.invalid>')
        self.assertEqual([m['pr'] for m in result['commits']], [2, 1])
        self.assertEqual(self.run_mirror()['result'], result['result'])
        self.assertEqual(git(mirror, 'for-each-ref', '--format=%(refname)').splitlines(),
                         ['refs/bors/input/0', 'refs/bors/input/1', 'refs/bors/input/2', 'refs/bors/result'])

    def test_toolchain_bytes_and_reconstruction_after_mirror_loss(self):
        content = 'leanprover/lean4:v4.25.0\n'
        head = self.commit(self.base, {'lean-toolchain': content})
        self.request['members'] = [self.member(1, head)]
        first = self.run_mirror()
        self.assertEqual(first['toolchain']['content'], content)
        shutil.rmtree(self.request['root'])
        rebuilt = self.run_mirror()
        self.assertEqual(rebuilt['result'], first['result'])
        self.assertEqual(rebuilt['toolchain'], first['toolchain'])

    def test_stacked_changes_and_rename_delete_parity(self):
        one = self.commit(self.base, {'file': None, 'renamed': 'one\ntwo\nthree\n'})
        two = self.commit(one, {'renamed': 'one\nchanged\nthree\n', 'new': 'new'})
        self.request['members'] = [self.member(1, one), self.member(2, two)]
        result = self.run_mirror()
        self.assertEqual(result['result']['tree'], git(self.upstream, 'rev-parse', two + '^{tree}'))

    def test_interacting_independent_edits(self):
        one = self.commit(self.base, {'file': 'ONE\ntwo\nthree\n'})
        two = self.commit(self.base, {'file': 'one\ntwo\nTHREE\n'})
        self.request['members'] = [self.member(1, one), self.member(2, two)]
        result = self.run_mirror()
        mirror = Path(self.request['root']) / 'example--repository.git'
        self.assertEqual(git(mirror, 'show', result['result']['commit'] + ':file'), 'ONE\ntwo\nTHREE')

    def test_structured_conflict_no_result_ref(self):
        one = self.commit(self.base, {'file': 'ONE\ntwo\nthree\n'})
        two = self.commit(self.base, {'file': 'OTHER\ntwo\nthree\n'})
        self.request['members'] = [self.member(1, one), self.member(2, two)]
        result = self.run_mirror()
        self.assertEqual(result['status'], 'conflict')
        self.assertEqual(result['details']['pr'], 2)
        self.assertEqual(result['details']['paths'], ['file'])
        mirror = Path(self.request['root']) / 'example--repository.git'
        self.assertNotIn('refs/bors/result', git(mirror, 'for-each-ref', '--format=%(refname)'))

    def test_import_hints_use_exact_heads_and_include_providers(self):
        importer = self.commit(self.base, {'Importer.lean': 'module\npublic import TauCeti.Provider\n'})
        # A provider in another PR is an interaction hint, not proof of failure.
        provider = self.commit(self.base, {'TauCeti/Provider.lean': 'def x := 1\n'})
        self.request['members'] = [self.member(1, importer), self.member(2, provider)]
        result = self.run_mirror()
        hints = result['diagnostic_hints']
        self.assertTrue(hints['complete'])
        self.assertIn([1, 2], hints['import_edges'])
        self.assertEqual(hints['missing_imports'], [])
        self.assertIn('TauCeti/Provider.lean', hints['dependency_paths'])
        self.assertEqual(hints['changed_files'][0]['head_sha'], importer)
        self.assertEqual(local.local_imports('/- import TauCeti.Fake /- nested -/ -/\nmodule\nimport TauCeti.Real -- comment\ndef x := 1'), ['TauCeti.Real'])
        self.request['members'] = [self.member(1, importer)]
        self.assertEqual(self.run_mirror()['diagnostic_hints']['missing_imports'][0]['provider_path'], 'TauCeti/Provider.lean')

    def test_provider_removed_by_base_is_checked_at_assembled_tree(self):
        old_base = self.commit(self.base, {'TauCeti/Provider.lean': 'def x := 1\n'})
        importer = self.commit(old_base, {'Importer.lean': 'module\nimport TauCeti.Provider\n'})
        advanced_base = self.commit(old_base, {'TauCeti/Provider.lean': None})
        self.request['identity']['base_sha'] = advanced_base
        self.request['members'] = [self.member(1, importer)]
        hints = self.run_mirror()['diagnostic_hints']
        self.assertTrue(hints['complete'])
        self.assertEqual(hints['changed_files'][0]['head_sha'], importer)
        self.assertEqual(hints['missing_imports'][0]['provider_path'], 'TauCeti/Provider.lean')

    def test_import_limits_and_ambiguous_headers_do_not_fail_assembly(self):
        head = self.commit(self.base, {'Broken.lean': 'module\nimport «TauCeti.Quoted»\n'})
        self.request['members'] = [self.member(1, head)]
        result = self.run_mirror()
        self.assertEqual(result['status'], 'ok')
        self.assertFalse(result['diagnostic_hints']['complete'])
        self.request['diagnostics'] = False
        self.assertFalse(self.run_mirror()['diagnostic_hints']['complete'])

    def test_missing_object_never_substitutes_force_pushed_head(self):
        new = self.commit(self.base, {'a': 'a'})
        self.request['members'] = [self.member(1, '0' * 40)]
        with self.assertRaises(local.Failure):
            self.run_mirror()
        self.assertEqual(git(self.upstream, 'rev-parse', 'HEAD'), new)

    def test_restart_recovers_interrupted_git_lock(self):
        one = self.commit(self.base, {'a': 'a'})
        self.request['members'] = [self.member(1, one)]
        result = self.run_mirror()
        mirror = Path(self.request['root']) / 'example--repository.git'
        (mirror / 'refs/bors/result.lock').write_text('interrupted update-ref')
        self.assertEqual(self.run_mirror()['result'], result['result'])
        self.assertFalse((mirror / 'refs/bors/result.lock').exists())

    def test_concurrent_processes_share_mirror(self):
        one = self.commit(self.base, {'a': 'a'})
        self.request['members'] = [self.member(1, one)]
        with concurrent.futures.ProcessPoolExecutor(4, mp_context=multiprocessing.get_context('fork')) as pool:
            results = list(pool.map(assemble_worker, [self.request] * 8, [self.upstream] * 8))
        self.assertTrue(all(r['result'] == results[0]['result'] for r in results))

    def test_budget_failure_removes_oversized_mirror(self):
        one = self.commit(self.base, {'a': 'a'})
        self.request['members'] = [self.member(1, one)]
        self.request['max_bytes'] = 1
        with self.assertRaises(local.Failure):
            self.run_mirror()
        self.assertLessEqual(sum(local.size(p) for p in Path(self.request['root']).glob('*.git')), 1)

    def test_leased_staging_push_and_stale_lease(self):
        one = self.commit(self.base, {'a': 'a'})
        self.request['members'] = [self.member(1, one)]
        result = self.run_mirror()
        git(self.upstream, 'update-ref', 'refs/heads/staging', self.base)
        push = self.request | dict(operation='publish', ref='refs/heads/staging',
                                    commit=result['result']['commit'], expected=self.base)
        local.validate(push)
        self.assertEqual(TestMirror(push, self.upstream).run()['status'], 'ok')
        self.assertEqual(git(self.upstream, 'rev-parse', 'staging'), push['commit'])
        with self.assertRaises(local.Failure):
            TestMirror(push | dict(commit=one), self.upstream).run()
        self.assertEqual(git(self.upstream, 'rev-parse', 'staging'), push['commit'])
        self.assertEqual(git(self.upstream, 'for-each-ref', '--format=%(refname)', 'refs/heads/bors-candidate'), '')

    def test_publication_ref_and_input_validation(self):
        for ref in ['refs/heads/main', 'refs/heads/master', 'refs/heads/staging.tmp']:
            with self.assertRaises(local.Failure):
                local.validate(self.request | dict(operation='publish', ref=ref, commit=self.base, expected=self.base))
        with self.assertRaises(local.Failure):
            local.validate(self.request | dict(repository='../bad', members=[self.member(1, self.base)]))

    def test_network_auth_error_redacts_diagnostics(self):
        fake = self.root / 'bin'
        fake.mkdir()
        program = fake / 'git'
        program.write_text('#!/bin/sh\necho "Authorization: secret" >&2\nexit 128\n')
        program.chmod(0o755)
        mirror = local.Mirror(self.request)
        mirror.env['PATH'] = str(fake)
        with self.assertRaises(local.Failure) as failure:
            mirror.git('fetch', mirror.url, self.base)
        self.assertEqual(failure.exception.details['exit_status'], 128)
        self.assertNotIn('secret', json.dumps(failure.exception.details))


if __name__ == '__main__':
    unittest.main()
