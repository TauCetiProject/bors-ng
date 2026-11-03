#!/usr/bin/env python3
import importlib.util
import io
import gzip
import json
import time
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('diagnostics', Path(__file__).parents[1] / 'priv/failure_diagnostics.py')
diagnostics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diagnostics)


class DiagnosticsTest(unittest.TestCase):
    # Actual primary diagnostics from batch827/run38835437044/job116763406898.
    RIEMANN_ROCH = 'TauCeti/FieldTheory/FunctionField/ConstantExtension/RiemannRoch.lean'
    PRIMARY = (
        '2026-10-16T01:44:30.9389087Z error: ' + RIEMANN_ROCH + ':320:0: '
        "`[FiniteDimensional F F']` did not match any variables in the current scope\n"
        '2026-10-16T01:44:30.9418784Z error: ' + RIEMANN_ROCH + ':327:0: '
        'automatically included section variable(s) unused in theorem '
        '`TauCeti.Divisor.dim_nsmul_poles_le_of_linearDisjoint`:\n')

    def target_block(self, *modules):
        return '\n'.join([diagnostics.FAILED_TARGETS_HEADER,
                          *('- ' + module for module in modules), 'error: build failed'])

    def test_iofail_warning_only_failure_uses_final_exact_target_not_warning(self):
        warning = ('warning: TauCeti/Combinatorics/Young/KnuthCorrespondence.lean:9:0: '
                   "'Mathlib.Data.Prod.Lex' has been deprecated")
        self.assertEqual(diagnostics.parse(warning), [])
        module = 'TauCeti.Combinatorics.Young.KnuthCorrespondence'
        result = diagnostics.parse(warning + '\n' + self.target_block(module))
        self.assertEqual(result, [dict(kind='lake_failed_target', module=module,
            paths=['TauCeti/Combinatorics/Young/KnuthCorrespondence.lean'])])
        # Unrelated warnings are not added as owners or range guesses.
        self.assertEqual(result, diagnostics.parse('warning: TauCeti/Unrelated.lean:1:1: warning\n'
                                                  + self.target_block(module)))
        self.assertNotIn('line', result[0])

    def test_failed_targets_retain_multiple_global_targets_with_ansi_timestamps(self):
        block = self.target_block('TauCeti.Provider', "TauCeti.Consumer'")
        text = '\n'.join('2026-10-16T00:42:44.123Z \x1b[31m' + line + '\x1b[0m'
                         for line in block.splitlines())
        self.assertEqual([o['paths'] for o in diagnostics.parse(text)],
                         [['TauCeti/Provider.lean'], ["TauCeti/Consumer'.lean"]])

    def test_failed_targets_missing_malformed_truncated_mixed_and_overflow_fall_back(self):
        good = self.target_block('TauCeti.Provider')
        bad = [self.target_block(), good.rsplit('\n', 1)[0],
               good + '\n' + good, self.target_block('TauCeti.Provider', 'Mathlib.Other'),
               self.target_block('TauCeti.Provider', 'TauCeti../Other'),
               self.target_block('TauCeti.Provider:olean'), self.target_block('TauCeti.' + 'A' * 512),
               good.replace('error: build failed', 'error: unrelated failure')]
        for text in bad:
            with self.subTest(text=text[:120]), self.assertRaises(ValueError):
                diagnostics.parse(text)
        self.assertEqual(diagnostics.parse('- TauCeti.Provider\nerror: build failed'), [])
        self.assertEqual(diagnostics.parse('echo ' + good.replace('\n', '\\n')), [])
        self.assertEqual(diagnostics.parse('warning: ' + good.replace('\n', '\\n')), [])

    def test_batch827_primary_error_survives_cascading_oversized_target_summary(self):
        # The native log has >1300 failed targets after these exact two errors.
        # Distinct fixture targets exercise the bound without shipping an 8MB log.
        cascade = self.target_block(*(f'TauCeti.Cascade{i}' for i in range(1400)))
        self.assertEqual(diagnostics.parse(cascade + '\n' + self.envelope()),
                         diagnostics.parse(self.envelope()))
        expected = [dict(kind='compiler_error', paths=[self.RIEMANN_ROCH])]
        for text in [self.PRIMARY + cascade, cascade + '\n' + self.PRIMARY]:
            self.assertEqual(diagnostics.parse(text), expected)
        # Collection stays available for the primary path without extra downloads.
        text = self.PRIMARY + cascade
        with patch.object(diagnostics, 'read_log', return_value=(text, len(text))), \
                patch.dict(diagnostics.os.environ, BORS_DIAGNOSTICS_TOKEN='test-only'):
            result = diagnostics.collect(dict(repository_id=1, job_ids=[2]))
        self.assertTrue(result['complete'])
        self.assertEqual(result['jobs'][0]['observations'], expected)

    def test_target_summary_is_only_fallback_and_cannot_erase_source_or_audit_findings(self):
        audit = ("audit-driver: import TauCeti.Left failed, environment already contains "
                 "'TauCeti.name' from TauCeti.Right")
        summaries = [self.target_block('TauCeti.Unrelated'), self.target_block(),
                     self.target_block('Mathlib.Unsupported'),
                     self.target_block('TauCeti.Provider').rsplit('\n', 1)[0],
                     self.target_block('TauCeti.Provider') * 2,
                     diagnostics.FAILED_TARGETS_HEADER + '\n' + 'x' * 9000]
        for summary in summaries:
            for primary in [self.PRIMARY, audit]:
                expected = diagnostics.parse(primary)
                self.assertEqual(diagnostics.parse(primary + '\n' + summary), expected)
                self.assertEqual(diagnostics.parse(summary + '\n' + primary), expected)

    def test_environment_unknowns_and_invalid_records_are_never_hidden_by_precise_errors(self):
        unknown = dict(linter='simpNF', declaration='_private.Probe', unknown='missing_range')
        cascade = self.target_block(*(f'TauCeti.Cascade{i}' for i in range(1400)))
        for summary in [cascade, diagnostics.FAILED_TARGETS_HEADER + '\ninvalid']:
            text = self.PRIMARY + summary + '\n' + self.envelope([unknown])
            result = diagnostics.parse(text)
            self.assertEqual(result[0]['paths'], [self.RIEMANN_ROCH])
            self.assertEqual(result[1]['paths'], [])
            self.assertEqual(result[1]['unknown'], 'missing_range')
            # An unknown alone must not be replaced by apparently located targets.
            result = diagnostics.parse(summary + '\n' + self.envelope([unknown]))
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]['paths'], [])
        for envelope in [self.envelope(complete=False), self.envelope()[:-2],
                         self.envelope() + '\n' + self.envelope()]:
            with self.assertRaises(ValueError):
                diagnostics.parse(self.PRIMARY + cascade + '\n' + envelope)
        self.assertEqual(diagnostics.parse(self.PRIMARY * 1000), diagnostics.parse(self.PRIMARY))


    def envelope(self, findings=None, **changes):
        finding = dict(linter='simpNF', declaration='TauCeti.element_annihilator_flat_baseChange',
                       path='TauCeti/RingTheory/Flat/Annihilator.lean', line=77, column=9)
        data = dict(version=1, complete=True, findings=[finding] if findings is None else findings)
        data.update(changes)
        return diagnostics.ENV_LOCATIONS + json.dumps(data, ensure_ascii=True)

    def test_real_simpnf_origin_and_escaped_unicode_names(self):
        for name in ['TauCeti.element_annihilator_flat_baseChange', 'TauCeti.λ雪',
                     'TauCeti.«quoted.name»', 'TauCeti.«a\\b"c»']:
            finding = dict(linter='simpNF', declaration=name,
                           path='TauCeti/RingTheory/Flat/Annihilator.lean', line=77, column=9)
            result = diagnostics.parse('2026-10-16T00:19:22.123Z ' + self.envelope([finding]))
            self.assertEqual(result, [dict(kind='environment_linter', linter='simpNF',
                declaration=name, paths=[finding['path']], line=77, column=9)])

    def test_unknown_and_multiple_global_findings_are_not_silently_dropped(self):
        finding = dict(linter='simpNF', declaration='TauCeti.Probe',
                       path='TauCeti/Other.lean', line=1, column=2)
        for reason in ['private_or_generated', 'missing_origin', 'missing_range']:
            unknown = dict(linter='checkType', declaration='_private.Probe', unknown=reason)
            result = diagnostics.parse(self.envelope([finding, unknown]))
            self.assertEqual(len(result), 2)
            self.assertEqual(result[1]['paths'], [])
            self.assertEqual(result[1]['unknown'], reason)
        second = {**finding, 'declaration': 'TauCeti.Other', 'path': 'TauCeti/Provider.lean'}
        self.assertEqual(len(diagnostics.parse(self.envelope([finding, second]))), 2)

    def test_malformed_truncated_duplicate_oversized_and_external_environment_records(self):
        good = self.envelope()
        data = json.loads(good[len(diagnostics.ENV_LOCATIONS):])
        finding = data['findings'][0]
        bad = [good[:-2], diagnostics.ENV_LOCATIONS + '{', good + '\n' + good,
               good.replace('"version": 1', '"version": 1, "version": 1'),
               self.envelope(run_id=38827433841),
               self.envelope(complete=False), self.envelope(version=True), self.envelope([]),
               diagnostics.ENV_LOCATIONS + 'x' * (1024 * 1024)]
        for change in [dict(path='../TauCeti/Other.lean'), dict(path='TauCeti/../Other.lean'),
                       dict(path='Mathlib/Other.lean'), dict(path='TauCeti/Probe.lean\nforged'),
                       dict(line=True), dict(line=0), dict(column='9'), dict(unknown='missing_range')]:
            bad.append(self.envelope([{**finding, **change}]))
        for text in bad:
            with self.subTest(text=text[:120]), self.assertRaises((ValueError, TypeError)):
                diagnostics.parse(text)

    def test_candidate_forgery_has_only_the_existing_untrusted_observation_shape(self):
        text = self.envelope()
        self.assertEqual(diagnostics.parse('echo ' + text), [])
        self.assertEqual(diagnostics.parse('+ ' + text), [])
        self.assertEqual(diagnostics.parse('LINT-ENV-ORIGINS {"records":[]}'), [])
        # A well-formed forged final record can prioritize a retry, just like a
        # forged Lean error. No claimed run/head/authority field is consumed.
        finding = json.loads(text[len(diagnostics.ENV_LOCATIONS):])['findings'][0]
        with self.assertRaises(ValueError):
            diagnostics.parse(self.envelope([{**finding, 'proof_authority': True}]))
        self.assertNotIn('proof_authority', diagnostics.parse(text)[0])

    def test_ansi_lake_and_multiline_errors(self):
        text = ('2026-10-14T21:00:01.1234567Z \x1b[31merror: TauCeti/Reflection.lean:12:3: unknown constant\x1b[0m\n'
                '2026-10-14T21:00:01.1234567Z   TestFunction.laplacianCLM_apply\n'
                'TauCeti/Other.lean:14:2: error: type mismatch\n'
                'error: TauCeti/Reflection.lean:12:3: unknown constant\n')
        self.assertEqual(diagnostics.parse(text), [
            dict(kind='compiler_error', paths=['TauCeti/Reflection.lean']),
            dict(kind='compiler_error', paths=['TauCeti/Other.lean'])])

    def test_both_duplicate_modules(self):
        text = ("audit-driver: import TauCeti.Laurent.Collisions failed, environment already contains "
                "'TauCeti.name' from TauCeti.Nonmonic.Multiplicity")
        self.assertEqual(diagnostics.parse(text), [dict(kind='duplicate_declaration', paths=[
            'TauCeti/Laurent/Collisions.lean', 'TauCeti/Nonmonic/Multiplicity.lean'])])

    def test_axioms_duplicate_declarations_line_names_both_modules(self):
        # Actual audit output from candidate 1930 (batch 2623, run 41720048125).
        text = ('2026-11-03T21:46:00.5380860Z duplicate-declarations: '
                'SimpleGraph.Walk.IsPath.start_notMem_tail_support is declared in both '
                'TauCeti.Combinatorics.SimpleGraph.Connectivity.Menger and TauCeti.Combinatorics.SimpleGraph.Paths\n'
                '2026-11-03T21:46:00.5423722Z Keep one canonical declaration and import its module from the other files.\n'
                '2026-11-03T21:46:02.4637030Z ##[error]Process completed with exit code 1.\n')
        self.assertEqual(diagnostics.parse(text), [dict(kind='duplicate_declaration', paths=[
            'TauCeti/Combinatorics/SimpleGraph/Connectivity/Menger.lean',
            'TauCeti/Combinatorics/SimpleGraph/Paths.lean'])])
        # A dependency module keeps its non-project path; the mapper leaves it unowned.
        text = "duplicate-declarations: Foo.bar' is declared in both Mathlib.Order.Basic and TauCeti.Order.Basic"
        self.assertEqual(diagnostics.parse(text), [dict(kind='duplicate_declaration', paths=[
            'Mathlib/Order/Basic.lean', 'TauCeti/Order/Basic.lean'])])
        for text in ['duplicate-declarations: Foo.bar is declared in both Mathlib.A and Mathlib.B',
                     'duplicate-declarations: checked 23614 TauCeti modules and 9083 dependency modules; '
                     'no duplicate declarations.',
                     'duplicate-declarations: Foo.bar is declared in both TauCeti.A and TauCeti.B extra',
                     'duplicate-declarations: Foo.bar is declared in both TauCeti.A and ../TauCeti.B',
                     'duplicate-declarations: Foo.bar is declared in both TauCeti.' + 'A' * 600 + ' and TauCeti.B']:
            self.assertEqual(diagnostics.parse(text), [], text[:80])

    def test_unmatched_failure_lines_are_sampled_only_when_nothing_is_attributed(self):
        lines = ['2026-11-03T21:46:00.1Z error: something the parser does not know\n',
                 '2026-11-03T21:46:00.2Z error: build failed\n',
                 "error: TauCeti/A.lean: bad import 'TauCeti.B'\n",
                 '\u2716 [1/2] Building TauCeti.B (cancelled)\n',
                 '\u2716 [2/2] Running TauCeti.C\n',
                 'lint-env: 1 (linter, declaration) violation(s).\n', 'ordinary output\n']
        sample = []
        self.assertEqual(diagnostics.parse_lines(iter(lines), sample), [])
        self.assertEqual(sample, ['error: something the parser does not know',
                                  '\u2716 [2/2] Running TauCeti.C',
                                  'lint-env: 1 (linter, declaration) violation(s).'])
        sample = []
        many = ['error: unknown %d %s\n' % (i, 'x' * 600) for i in range(20)]
        diagnostics.parse_lines(iter(many), sample)
        self.assertEqual(len(sample), diagnostics.UNMATCHED_SAMPLE)
        self.assertTrue(all(len(line) == diagnostics.UNMATCHED_BYTES for line in sample))
        self.assertEqual(diagnostics.parse_lines(iter(lines)), [])

        def read(repo, job, token, deadline, sample):
            text = self.PRIMARY if job == 1 else 'error: unknown failure\n'
            return diagnostics.parse_lines(iter(text.splitlines(True)), sample), len(text)

        with patch.object(diagnostics, 'read_log', side_effect=read), \
                patch.dict('os.environ', {'BORS_DIAGNOSTICS_TOKEN': 'secret'}):
            result = diagnostics.collect(dict(repository_id=1, job_ids=[1, 2]))
        self.assertNotIn('unmatched_sample', result['jobs'][0])
        self.assertEqual(result['jobs'][1]['unmatched_sample'], ['error: unknown failure'])
        self.assertEqual(result['jobs'][1]['observations'], [])

    def test_module_only_environment_locations_keep_the_path_without_a_range(self):
        finding = dict(linter='unusedArguments',
                       declaration='_private.TauCeti.LowDimTopology.DehnSurgery.Slope.0.TauCeti.slopeSetoid',
                       path='TauCeti/LowDimTopology/DehnSurgery/Slope.lean')
        self.assertEqual(diagnostics.parse(self.envelope([finding])), [dict(
            kind='environment_linter', linter='unusedArguments', declaration=finding['declaration'],
            paths=[finding['path']])])
        for change in [dict(line=3), dict(column=3), dict(path='Mathlib/Other.lean'),
                       dict(path='TauCeti/../Other.lean'), dict(unknown='missing_range')]:
            with self.subTest(change=change), self.assertRaises(ValueError):
                diagnostics.parse(self.envelope([{**finding, **change}]))

    def test_primary_located_error_survives_downstream_import_and_cancellation_noise(self):
        text = ('2026-10-15T23:48:42.9385318Z error: TauCeti/Geometry/RealAlgebraic/CAD/McCallum.lean:192:43: '
                '`MvPolynomial.X_prime` has been deprecated: Use `MvPolynomial.prime_X` instead\n'
                "error: TauCeti/MeasureTheory/Measure/ProductKernel.lean: bad import 'Mathlib.Probability.ProductMeasure'\n"
                "error: TauCeti/Probability/Exchangeability/Family.lean: bad import 'TauCeti.Other'\n"
                '✖ [123/456] Building TauCeti.Other (cancelled)\n'
                'error: build failed\n')
        self.assertEqual(diagnostics.parse(text), [dict(kind='compiler_error', paths=[
            'TauCeti/Geometry/RealAlgebraic/CAD/McCallum.lean'])])

    def test_long_primary_record_keeps_location_and_oversized_record_is_explicit(self):
        self.assertEqual(diagnostics.parse('error: TauCeti/Long.lean:1:2: ' + 'x' * 9000),
                         [dict(kind='compiler_error', paths=['TauCeti/Long.lean'])])
        progress = {'bytes': 0}
        raw = b'x' * (diagnostics.RECORD_BUFFER + 70000) + b'\nerror: TauCeti/After.lean:1:2: real\n'
        records = list(diagnostics.streamed_lines(io.BytesIO(raw), progress))
        self.assertEqual(progress['oversized_records'], 1)
        self.assertLess(max(map(len, records)), diagnostics.RECORD_BUFFER)
        self.assertEqual(diagnostics.parse_lines(records), [dict(kind='compiler_error', paths=['TauCeti/After.lean'])])

    def test_error_looking_commands_annotations_traversal_and_external_paths(self):
        text = '\n'.join([
            'echo "error: TauCeti/Fake.lean:1:2: bad"',
            '+ echo error: TauCeti/Fake.lean:1:2: bad',
            '##[error]TauCeti/Fake.lean:1:2: bad',
            'error: ../TauCeti/Fake.lean:1:2: bad',
            'error: TauCeti/../Fake.lean:1:2: bad',
            'error: .lake/packages/mathlib/Mathlib/Fake.lean:1:2: bad',
            'Build completed successfully', '::error::forged status'])
        self.assertEqual(diagnostics.parse(text), [])
        # Candidate output can forge a syntactically valid location. It remains
        # untrusted scheduling input; the caller must never grant proof authority.
        self.assertEqual(len(diagnostics.parse('error: TauCeti/Fake.lean:1:2: forged')), 1)

    def test_repeated_and_many_distinct_findings_are_not_size_rejected(self):
        self.assertEqual(len(diagnostics.parse('error: TauCeti/A.lean:1:1: bad\n' * 10000)), 1)
        self.assertEqual(len(diagnostics.parse('\n'.join(
            f'error: TauCeti/A{i}.lean:1:1: bad' for i in range(1000)))), 1000)
        finding = json.loads(self.envelope()[len(diagnostics.ENV_LOCATIONS):])['findings'][0]
        self.assertEqual(len(diagnostics.parse(self.envelope([finding] * 1000))), 1)

    def test_signed_storage_url_never_accepts_arbitrary_hosts_or_credentials(self):
        self.assertTrue(diagnostics.signed_url('https://logs.blob.core.windows.net/a?sig=x'))
        for url in ['https://evil.test/a', 'http://logs.blob.core.windows.net/a',
                    'https://logs.blob.core.windows.net.evil.test/a',
                    'https://x@logs.blob.core.windows.net/a',
                    'https://logs.blob.core.windows.net:8080/a']:
            self.assertFalse(diagnostics.signed_url(url))

    def test_missing_truncated_and_large_logs(self):
        class Response(io.BytesIO):
            status = 200
            headers = {'Content-Length': '20'}

        class Opener:
            def open(self, *_args, **_kwargs):
                return Response(b'short')

        with patch.object(diagnostics.urllib.request, 'build_opener', return_value=Opener()):
            with self.assertRaisesRegex(ValueError, 'truncated_response'):
                diagnostics.read_log(1, 2, 'secret', time.monotonic() + 5)

    def test_redirect_strips_token_and_rejects_second_redirect(self):
        class Response(io.BytesIO):
            status = 200
            headers = {'Content-Length': '4'}

        requests = []

        class Opener:
            def open(self, request, **_kwargs):
                requests.append(request)
                if len(requests) == 1:
                    raise diagnostics.urllib.error.HTTPError(request.full_url, 302, '',
                        {'Location': 'https://logs.blob.core.windows.net/a?sig=x'}, None)
                return Response(b'test')

        with patch.object(diagnostics.urllib.request, 'build_opener', return_value=Opener()):
            self.assertEqual(diagnostics.read_log(1, 2, 'secret', time.monotonic() + 5), ([], 4))
        self.assertEqual(requests[0].get_header('Authorization'), 'Bearer secret')
        self.assertIsNone(requests[1].get_header('Authorization'))

    def test_streams_over_16mib_and_keeps_errors_at_both_ends(self):
        text = ('error: TauCeti/First.lean:1:1: bad\n' + 'unrelated output\n' * 1100000
                + 'error: TauCeti/Last.lean:1:1: bad\n')
        data = text.encode()

        class Response(io.BytesIO):
            status = 200
            headers = {'Content-Length': str(len(data))}

            def read(self, n=-1):
                self.assert_bounded(n)
                return super().read(n)

            @staticmethod
            def assert_bounded(n):
                if not 0 < n <= 65536:
                    raise AssertionError('reader attempted an unbounded read')

        class Opener:
            def open(self, *_args, **_kwargs):
                return Response(data)

        with patch.object(diagnostics.urllib.request, 'build_opener', return_value=Opener()):
            observations, size = diagnostics.read_log(1, 2, 'secret')
        self.assertGreater(size, 16 * 1024 * 1024)
        self.assertEqual([o['paths'] for o in observations],
                         [['TauCeti/First.lean'], ['TauCeti/Last.lean']])

    def test_sampled_targets_require_raw_fallback_without_primary_diagnostics(self):
        text = (diagnostics.FAILED_TARGETS_HEADER + '\n- TauCeti.A\n'
                'TAUCETI-DIAGNOSTICS-SAMPLED failed-targets total=100 shown=1\nerror: build failed\n')
        with self.assertRaisesRegex(ValueError, 'sampled_diagnostics'):
            diagnostics.parse(text)
        self.assertEqual(diagnostics.parse(self.PRIMARY + text), diagnostics.parse(self.PRIMARY))

    def test_raw_artifact_fallback_keeps_complete_targets_bound_to_run_attempt(self):
        raw = self.target_block('TauCeti.First', 'TauCeti.Last').encode()
        output = io.BytesIO()
        with zipfile.ZipFile(output, 'w') as archive:
            archive.writestr('lake-build.log.gz', gzip.compress(raw))
        listing = json.dumps({'artifacts': [{'id': 55, 'name': 'ci-failure-logs-7-2',
            'expired': False, 'workflow_run': {'id': 7}}]}).encode()

        class Response(io.BytesIO):
            status = 200
            headers = {}

        calls = []
        def respond(url, token=None):
            calls.append((url, token))
            return Response(output.getvalue() if url.endswith('/zip') else listing)

        with patch.object(diagnostics, 'artifact_response', side_effect=respond), \
                patch.object(diagnostics, 'read_log', side_effect=diagnostics.CollectionError('sampled_diagnostics')), \
                patch.dict(diagnostics.os.environ, BORS_DIAGNOSTICS_TOKEN='test-only'):
            result = diagnostics.collect(dict(repository_id=1, job_ids=[2], run_id=7, run_attempt=2))
        self.assertTrue(result['complete'])
        self.assertEqual(result['jobs'][0]['source'], 'raw_artifact')
        self.assertEqual([o['paths'] for o in result['jobs'][0]['observations']],
                         [['TauCeti/First.lean'], ['TauCeti/Last.lean']])
        self.assertIn('/actions/runs/7/artifacts?', calls[0][0])
        with patch.object(diagnostics, 'artifact_response', return_value=Response(listing)), \
                self.assertRaisesRegex(ValueError, 'raw_artifact_missing'):
            diagnostics.read_raw_artifact(1, dict(run_id=7, run_attempt=3), 'test-only', time.monotonic()+5)

    def test_chunk_boundaries_unicode_and_invalid_encoding(self):
        data = self.envelope([dict(linter='simpNF', declaration='TauCeti.雪',
            path='TauCeti/Unicode.lean', line=1, column=2)]).replace('\\u96ea', '雪').encode()
        class Response(io.BytesIO):
            headers = {}
            def read(self, n=-1):
                return super().read(min(n, 5))
        lines = diagnostics.streamed_lines(Response(data), {'bytes': 0})
        self.assertEqual(diagnostics.parse_lines(lines)[0]['declaration'], 'TauCeti.雪')
        with self.assertRaises(UnicodeError):
            list(diagnostics.streamed_lines(Response(b'bad\xff'), {'bytes': 0}))

    def test_deadline_incomplete_read_and_artifact_transport_reasons(self):
        with self.assertRaisesRegex(ValueError, 'collection_timeout'):
            list(diagnostics.streamed_lines(io.BytesIO(b'content'), {'bytes': 0}, time.monotonic()-1))
        class Opener:
            def open(self, *_args, **_kwargs):
                raise diagnostics.http.client.IncompleteRead(b'partial', 100)
        with patch.object(diagnostics.urllib.request, 'build_opener', return_value=Opener()):
            with self.assertRaisesRegex(ValueError, 'truncated_response'):
                diagnostics.read_log(1, 2, 'secret')
        for error, reason in [(TimeoutError(), 'transport_timeout'),
                              (diagnostics.http.client.IncompleteRead(b'', 10), 'truncated_response'),
                              (diagnostics.urllib.error.URLError('private URL'), 'transport_failure')]:
            with patch.object(diagnostics, 'artifact_response', side_effect=error):
                with self.assertRaisesRegex(ValueError, reason):
                    diagnostics.read_raw_artifact(1, dict(run_id=7, run_attempt=2), 'secret', time.monotonic()+5)

    def test_http_reasons_distinguish_quota_permissions_and_missing_logs(self):
        self.assertEqual(diagnostics.http_reason(403, {'X-RateLimit-Remaining': '0'}), 'rate_limited')
        self.assertEqual(diagnostics.http_reason(429, {}), 'rate_limited')
        self.assertEqual(diagnostics.http_reason(403, {}), 'permission_denied')
        self.assertEqual(diagnostics.http_reason(404, {}), 'log_not_available')
        self.assertEqual(diagnostics.http_reason(302, {}), 'unexpected_redirect')


if __name__ == '__main__':
    unittest.main()
