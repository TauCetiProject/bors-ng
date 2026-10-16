#!/usr/bin/env python3
import importlib.util
import io
import json
import time
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('diagnostics', Path(__file__).parents[1] / 'priv/failure_diagnostics.py')
diagnostics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diagnostics)


class DiagnosticsTest(unittest.TestCase):
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
               self.envelope([finding] * 129), diagnostics.ENV_LOCATIONS + 'x' * (1024 * 1024)]
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

    def test_primary_located_error_survives_downstream_import_and_cancellation_noise(self):
        text = ('2026-10-15T23:48:42.9385318Z error: TauCeti/Geometry/RealAlgebraic/CAD/McCallum.lean:192:43: '
                '`MvPolynomial.X_prime` has been deprecated: Use `MvPolynomial.prime_X` instead\n'
                "error: TauCeti/MeasureTheory/Measure/ProductKernel.lean: bad import 'Mathlib.Probability.ProductMeasure'\n"
                "error: TauCeti/Probability/Exchangeability/Family.lean: bad import 'TauCeti.Other'\n"
                '✖ [123/456] Building TauCeti.Other (cancelled)\n'
                'error: build failed\n')
        self.assertEqual(diagnostics.parse(text), [dict(kind='compiler_error', paths=[
            'TauCeti/Geometry/RealAlgebraic/CAD/McCallum.lean'])])

    def test_error_looking_commands_annotations_traversal_and_external_paths(self):
        text = '\n'.join([
            'echo "error: TauCeti/Fake.lean:1:2: bad"',
            '+ echo error: TauCeti/Fake.lean:1:2: bad',
            '##[error]TauCeti/Fake.lean:1:2: bad',
            'error: ../TauCeti/Fake.lean:1:2: bad',
            'error: TauCeti/../Fake.lean:1:2: bad',
            'error: .lake/packages/mathlib/Mathlib/Fake.lean:1:2: bad',
            'Build completed successfully', '::error::forged status',
            'error: TauCeti/Fake.lean:1:2: ' + 'x' * 9000])
        self.assertEqual(diagnostics.parse(text), [])
        # Candidate output can forge a syntactically valid location. It remains
        # untrusted scheduling input; the caller must never grant proof authority.
        self.assertEqual(len(diagnostics.parse('error: TauCeti/Fake.lean:1:2: forged')), 1)

    def test_limits_fail_closed(self):
        with self.assertRaises(ValueError):
            diagnostics.parse('error: TauCeti/A.lean:1:1: bad\n' * 129)
        with patch.object(diagnostics, 'MAX_BYTES', 8), self.assertRaises(ValueError):
            diagnostics.parse('0123456789')

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
            with self.assertRaisesRegex(ValueError, 'truncated_log'):
                diagnostics.read_log(1, 2, 'secret', time.monotonic() + 5)
            with patch.object(diagnostics, 'MAX_BYTES', 8), self.assertRaisesRegex(ValueError, 'log_limit'):
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
            self.assertEqual(diagnostics.read_log(1, 2, 'secret', time.monotonic() + 5), ('test', 4))
        self.assertEqual(requests[0].get_header('Authorization'), 'Bearer secret')
        self.assertIsNone(requests[1].get_header('Authorization'))


if __name__ == '__main__':
    unittest.main()
