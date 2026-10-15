#!/usr/bin/env python3
import importlib.util
import io
import time
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('diagnostics', Path(__file__).parents[1] / 'priv/failure_diagnostics.py')
diagnostics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diagnostics)


class DiagnosticsTest(unittest.TestCase):
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
