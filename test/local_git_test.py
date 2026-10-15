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

    def compatibility(self, base, *heads):
        request = self.request | dict(operation='compatibility', base_sha=base,
            members=[dict(pr=n + 1, head_sha=head) for n, head in enumerate(heads)])
        local.validate(request)
        return TestMirror(request, self.upstream).run()['results']

    def test_compatibility_deleted_source_and_unchanged_importer(self):
        fork = self.commit(self.base, {'TauCeti/Old.lean': 'def old := 1',
            'TauCeti/Dependency.lean': 'import TauCeti.Old\ndef dependency := 1'})
        main = self.commit(fork, {'TauCeti/Old.lean': None})
        head = self.commit(fork, {'TauCeti/Consumer.lean': 'import TauCeti.Dependency\ndef consumer := 1'})
        # Main's pre-existing missing import is not attributed to this PR.
        result = self.compatibility(main, head)[0]
        self.assertEqual(result['status'], 'pass')
        # Main deletes the provider AND fixes its own importer; head reintroduces
        # the unchanged dependency through an otherwise clean textual merge.
        main = self.commit(fork, {'TauCeti/Old.lean': None, 'TauCeti/Dependency.lean': None})
        head = self.commit(fork, {'TauCeti/Consumer.lean': 'import TauCeti.Old\ndef consumer := 1'})
        result = self.compatibility(main, head)[0]
        self.assertEqual(result['status'], 'missing_source')
        self.assertEqual(result['missing_imports'][0]['module'], 'TauCeti.Old')
        self.assertEqual(result['base_sha'], main)

    def test_compatibility_pr_added_sources_and_multiline(self):
        head = self.commit(self.base, {'TauCeti/Added.lean': 'def added := 1',
            'TauCeti/Consumer.lean': 'module\n/- outer /- inner -/ -/\npublic import\n TauCeti.Added\n-- comment\npublic import Mathlib\nnamespace TauCeti\n'})
        result = self.compatibility(self.base, head)[0]
        self.assertEqual(result['status'], 'pass')
        self.assertEqual(result['coverage'], 'all_project_local_headers')

    def test_header_body_commands_cannot_become_imports(self):
        self.assertEqual(local.compatibility_imports(
            'module\nimport Mathlib\nassert_not_exists TauCeti.NotAModule\n'), [])
        self.assertEqual(local.compatibility_imports(
            'module\npublic import\n TauCeti.Real\nprivate def localName := 1\n'), ['TauCeti.Real'])
        for header in ['private import TauCeti.Missing', 'import «TauCeti.Hidden»',
                       'importFoo TauCeti.Hidden', 'import', 'module module import TauCeti.X', 'import TauCeti.' + 'x' * 513, 'import\u00a0TauCeti.Hidden']:
            with self.assertRaises(local.Failure, msg=header):
                local.compatibility_imports(header)

    def test_partial_inventory_caches_only_parsed_blobs_without_passing(self):
        from unittest.mock import patch
        head = self.commit(self.base, {f'TauCeti/F{i}.lean': f'def value{i} := {i}\n'
                                      for i in range(15)})
        request = self.request | dict(operation='compatibility', base_sha=head,
                                      members=[dict(pr=1, head_sha=head)])

        class Interrupted(TestMirror):
            batches = 0

            def git(self, *args, **kwargs):
                if args[:2] == ('cat-file', '--batch'):
                    self.batches += 1
                    if self.batches == 2:
                        raise local.Failure('timeout')
                return super().git(*args, **kwargs)

        interrupted = Interrupted(request, self.upstream)
        result = interrupted.run()['results'][0]
        self.assertEqual(result['status'], 'unknown')
        cache = interrupted.repo / 'bors-headers-local-source-v2.json'
        self.assertEqual(len(json.loads(cache.read_text())), 12)

        with patch.object(local, 'compatibility_imports', wraps=local.compatibility_imports) as parse:
            complete = TestMirror(request, self.upstream).run()['results'][0]
            self.assertEqual(complete['status'], 'pass')
            self.assertEqual(parse.call_count, 3)
        self.assertEqual(len(json.loads(cache.read_text())), 15)

    def test_compatibility_individual_conflicts_and_unknown(self):
        main = self.commit(self.base, {'file': 'main\n'})
        conflict = self.commit(self.base, {'file': 'head\n'})
        malformed = self.commit(self.base, {'TauCeti/Bad.lean': 'import «unusual.module»\n'})
        results = self.compatibility(main, conflict, malformed)
        self.assertEqual(results[0]['status'], 'conflict')
        self.assertEqual(results[1]['status'], 'unknown')
        self.assertEqual(results[0]['paths'], ['file'])

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
        self.assertEqual(result['barriers'], dict(complete=True, paths=['file', 'new', 'renamed']))

    def test_barriers_are_exact_even_when_optional_diagnostics_are_disabled(self):
        one = self.commit(self.base, {'scripts/new-audit.py': 'trusted tooling'})
        self.request.update(members=[self.member(1, one)], diagnostics=False)
        result = self.run_mirror()
        self.assertEqual(result['barriers'], dict(complete=True, paths=['scripts/new-audit.py']))
        self.assertFalse(result['diagnostic_hints']['complete'])

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
        self.assertIn('TauCeti/Provider.lean', hints['changed_files'][0]['dependency_paths'])
        self.assertEqual(hints['changed_files'][1]['dependency_paths'], [])
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

    def test_depth_two_exact_bases_and_ordered_leased_promotion(self):
        one = self.commit(self.base, {'a': 'a'})
        two = self.commit(self.base, {'b': 'b'})
        self.request['members'] = [self.member(1, one)]
        first = self.run_mirror()['result']['commit']
        push = self.request | dict(operation='publish', ref='refs/heads/staging-candidate-1-1',
                                   commit=first, expected='0' * 40)
        local.validate(push)
        TestMirror(push, self.upstream).run()
        second_request = self.request | dict(identity=self.request['identity'] | dict(
            candidate_id=2, base_sha=first), members=[self.member(2, two)])
        second = TestMirror(second_request, self.upstream).run()['result']['commit']
        TestMirror(push | dict(ref='refs/heads/staging-candidate-2-1', commit=second), self.upstream).run()
        mirror = Path(self.request['root']) / 'example--repository.git'
        self.assertEqual(git(mirror, 'rev-parse', second + '^'), first)
        git(self.upstream, 'update-ref', 'refs/heads/main', self.base)
        promote = self.request | dict(operation='promote', ref='refs/heads/main',
                                      commit=second, expected=first)
        local.validate(promote)
        # Green B cannot land before A: exact-base lease refuses it.
        with self.assertRaises(local.Failure):
            TestMirror(promote, self.upstream).run()
        TestMirror(promote | dict(commit=first, expected=self.base), self.upstream).run()
        self.assertEqual(git(self.upstream, 'rev-parse', 'main'), first)
        shutil.rmtree(self.request['root'])
        # Exact commits survive disposable mirror loss; refetch only trusted objects.
        git(self.upstream, 'update-ref', 'refs/heads/staging-candidate-2-1', second)
        TestMirror(promote, self.upstream).run()
        self.assertEqual(git(self.upstream, 'rev-parse', 'main'), second)
        self.assertEqual(git(self.upstream, 'show', 'main:a'), 'a')
        self.assertEqual(git(self.upstream, 'show', 'main:b'), 'b')

    def test_main_change_between_read_and_push_cannot_promote(self):
        one = self.commit(self.base, {'a': 'a'})
        two = self.commit(self.base, {'b': 'b'})
        self.request['members'] = [self.member(1, one), self.member(2, two)]
        result = self.run_mirror()['result']['commit']
        mirror = Path(self.request['root']) / 'example--repository.git'
        intermediate = git(mirror, 'rev-parse', result + '^')
        TestMirror(self.request | dict(operation='publish', ref='refs/heads/staging',
                   commit=result, expected='0' * 40), self.upstream).run()
        # Even an intermediate ancestor of the candidate is an unexpected base.
        git(self.upstream, 'update-ref', 'refs/heads/main', intermediate)
        push = self.request | dict(operation='promote', ref='refs/heads/main',
                                   commit=result, expected=self.base)
        with self.assertRaises(local.Failure):
            TestMirror(push, self.upstream).run()
        self.assertEqual(git(self.upstream, 'rev-parse', 'main'), intermediate)

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
