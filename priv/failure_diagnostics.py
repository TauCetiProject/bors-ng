#!/usr/bin/env python3
"""Read terminal job logs as untrusted scheduling hints, never execute them."""
import codecs
from contextlib import closing
import io
import http.client
import zlib
import gzip
import json
import socket
import sqlite3
import tempfile
import os
import re
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

PARSER_VERSION = "streaming-v2"
RECORD_BUFFER = 1024 * 1024
ANSI = re.compile(r'\x1b\[[0-?]*[ -/]*[@-~]')
STAMP = re.compile(r'^\d{4}-\d\d-\d\dT[0-9:.]+Z ')
PATH = r'TauCeti(?:/[A-Za-z0-9_\-]+)+\.lean'
MODULE = r'TauCeti(?:\.[A-Za-z0-9_]+)+'
SOURCE = re.compile(r'^(?:error: )?(' + PATH + r'):\d+:\d+:\s*(?:error:|error\b)')
LAKE = re.compile(r'^error: (' + PATH + r'):\d+:\d+:')
AUDIT = re.compile(r"^audit-driver: import (" + MODULE +
                   r") failed, environment already contains '[^'\r\n]{1,512}' from (" + MODULE + r')$')
# scripts/Axioms.lean prints one line per colliding declaration; at least one
# module is a project module, the other may belong to a dependency.
ANY_MODULE = r"[A-Za-z_][A-Za-z0-9_']*(?:\.[A-Za-z_][A-Za-z0-9_']*)*"
DUPLICATE = re.compile(r'^duplicate-declarations: (\S{1,1024}) is declared in both ('
                       + ANY_MODULE + r') and (' + ANY_MODULE + r')$')
UNMATCHED_SAMPLE = 8
UNMATCHED_BYTES = 512
ENV_LOCATIONS = 'LINT-ENV-LOCATIONS '
ENV_PATH = re.compile(r"TauCeti(?:/[A-Za-z_][A-Za-z0-9_']*)+\.lean")
FAILED_TARGET = re.compile(r"- (TauCeti(?:\.[A-Za-z_][A-Za-z0-9_']*)+)")
FAILED_TARGETS_HEADER = 'Some required targets logged failures:'


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate_environment_key')
        result[key] = value
    return result


def environment_locations(payload):
    """Untrusted origin hints: same retry-only authority as compiler locations.

    Keep unknown findings (empty paths), so a partial global failure cannot look
    fully located. The existing mapper will retain deterministic fallback.
    """
    data = json.loads(payload, object_pairs_hook=unique_object)
    if (not isinstance(data, dict) or set(data) != {'version', 'complete', 'findings'}
            or type(data.get('version')) is not int
            or data['version'] != 1 or data.get('complete') is not True
            or not isinstance(data.get('findings'), list)
            or not data['findings']):
        raise ValueError('invalid_environment_locations')
    observations = []
    for finding in data['findings']:
        if not isinstance(finding, dict):
            raise ValueError('invalid_environment_finding')
        linter, declaration = finding.get('linter'), finding.get('declaration')
        if (not isinstance(linter, str) or not re.fullmatch(r'[A-Za-z0-9_]{1,128}', linter)
                or not isinstance(declaration, str) or not 1 <= len(declaration) <= 1024
                or any(ord(c) < 32 or ord(c) == 127 for c in declaration)):
            raise ValueError('invalid_environment_name')
        observation = dict(kind='environment_linter', linter=linter, declaration=declaration)
        if 'unknown' in finding:
            if (set(finding) != {'linter', 'declaration', 'unknown'}
                    or not isinstance(finding['unknown'], str)
                    or not re.fullmatch(r'[a-z_]{1,64}', finding['unknown'])):
                raise ValueError('invalid_environment_unknown')
            observation.update(paths=[], unknown=finding['unknown'])
        else:
            # A module-only location (no range) still names the defining file.
            path = finding.get('path')
            ranged = set(finding) == {'linter', 'declaration', 'path', 'line', 'column'}
            if (not (ranged or set(finding) == {'linter', 'declaration', 'path'})
                    or not isinstance(path, str) or len(path) > 512 or not ENV_PATH.fullmatch(path)
                    or (ranged and any(type(finding.get(k)) is not int
                                       or not 1 <= finding[k] <= 10_000_000
                                       for k in ('line', 'column')))):
                raise ValueError('invalid_environment_path_or_range')
            observation.update(paths=[path])
            if ranged:
                observation.update(line=finding['line'], column=finding['column'])
        observations.append(observation)
    return observations


class CollectionError(ValueError):
    def __init__(self, reason, *, http_status=None, bytes_read=0):
        super().__init__(reason)
        self.reason, self.http_status, self.bytes_read = reason, http_status, bytes_read


def http_reason(status, headers, missing='log_not_available'):
    if status == 429 or headers.get('X-RateLimit-Remaining') == '0':
        return 'rate_limited'
    if status in (401, 403):
        return 'permission_denied'
    if status in (404, 410):
        return missing
    if status == 302:
        return 'unexpected_redirect'
    return 'http_failure'


def unmatched_failure_line(line):
    """A failure-looking line no pattern consumed; cancelled-job fallout is not one."""
    if line == 'error: build failed' or " bad import '" in line or line.endswith('(cancelled)'):
        return False
    return line.startswith(('error:', 'duplicate-declarations:', 'audit-driver:', 'lint-env:')) \
        or '\u2716' in line


def parse_lines(lines, sample=None):
    """Read every record; spill deduplicated observations instead of retaining logs.

    `sample`, when a list, receives at most UNMATCHED_SAMPLE unmatched failure
    lines so an unattributed failure leaves evidence of what the parser missed.
    """
    with tempfile.TemporaryDirectory(prefix='bors-diagnostic-') as directory:
        with closing(sqlite3.connect(str(Path(directory) / 'observations.sqlite'))) as db:
            db.execute('CREATE TABLE observations (kind TEXT, value TEXT, UNIQUE(kind,value))')
            environment_seen = targets_seen = in_targets = False
            target_error = None
            target_count = 0
            sampled = False

            def add(kind, observation):
                db.execute('INSERT OR IGNORE INTO observations VALUES (?,?)',
                           (kind, json.dumps(observation, sort_keys=True)))

            for raw in lines:
                line = STAMP.sub('', ANSI.sub('', raw.rstrip('\r\n').lstrip('\ufeff')))
                if line.startswith(ENV_LOCATIONS):
                    if environment_seen:
                        raise CollectionError('duplicate_environment_locations')
                    environment_seen = True
                    for observation in environment_locations(line[len(ENV_LOCATIONS):]):
                        add('primary', observation)
                    continue
                audit = AUDIT.fullmatch(line)
                duplicate = DUPLICATE.fullmatch(line)
                source = SOURCE.match(line) or LAKE.match(line)
                if audit and all(len(module) <= 512 for module in audit.groups()):
                    add('primary', dict(kind='duplicate_declaration', paths=[
                        module.replace('.', '/') + '.lean' for module in audit.groups()]))
                elif (duplicate and any(module.startswith('TauCeti.') for module in duplicate.groups()[1:])
                        and all(len(module) <= 508 for module in duplicate.groups()[1:])):
                    add('primary', dict(kind='duplicate_declaration', paths=[
                        module.replace('.', '/') + '.lean' for module in duplicate.groups()[1:]]))
                elif source and len(source[1]) <= 512:
                    add('primary', dict(kind='compiler_error', paths=[source[1]]))
                elif sample is not None and len(sample) < UNMATCHED_SAMPLE and unmatched_failure_line(line):
                    sample.append(line[:UNMATCHED_BYTES])
                if line.startswith('TAUCETI-DIAGNOSTICS-SAMPLED '):
                    sampled = True
                if line == FAILED_TARGETS_HEADER:
                    if targets_seen:
                        target_error = 'duplicate_failed_targets'
                    targets_seen = in_targets = True
                    continue
                if in_targets:
                    target = FAILED_TARGET.fullmatch(line)
                    if target and len(target[1].replace('.', '/') + '.lean') <= 512:
                        add('fallback', dict(kind='lake_failed_target', module=target[1],
                            paths=[target[1].replace('.', '/') + '.lean']))
                        target_count += 1
                    elif line == 'error: build failed' and target_count:
                        in_targets = False
                    else:
                        target_error = 'unsupported_or_incomplete_failed_targets'
                        in_targets = False
            primary = list(db.execute("SELECT value FROM observations WHERE kind='primary' ORDER BY rowid"))
            if primary:
                return [json.loads(row[0]) for row in primary]
            if sampled:
                raise CollectionError('sampled_diagnostics')
            if in_targets:
                raise CollectionError('truncated_failed_targets')
            if target_error:
                raise CollectionError(target_error)
            return [json.loads(row[0]) for row in db.execute(
                "SELECT value FROM observations WHERE kind='fallback' ORDER BY rowid")]


def parse(text):
    return parse_lines(io.StringIO(text))


def streamed_lines(response, progress, deadline=None):
    decoder = codecs.getincrementaldecoder('utf-8')()
    buffer = ''
    oversized = False
    prefix = ''
    while True:
        if deadline is not None and time.monotonic() >= deadline:
            raise CollectionError('collection_timeout', bytes_read=progress['bytes'])
        chunk = response.read(64 * 1024)
        if not chunk:
            text = decoder.decode(b'', final=True)
        else:
            progress['bytes'] += len(chunk)
            text = decoder.decode(chunk)
        for piece in text.splitlines(keepends=True):
            if not oversized:
                buffer += piece
                if len(buffer) > RECORD_BUFFER:
                    prefix, buffer, oversized = buffer[:8192], '', True
                    progress['oversized_records'] = progress.get('oversized_records', 0) + 1
            if piece.endswith('\n'):
                yield prefix if oversized else buffer
                buffer, prefix, oversized = '', '', False
        if not chunk:
            if oversized or buffer:
                yield prefix if oversized else buffer
            break
    expected = getattr(response, 'headers', {}).get('Content-Length')
    if expected is not None and progress['bytes'] != int(expected):
        raise CollectionError('truncated_response', bytes_read=progress['bytes'])


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def signed_url(url):
    parsed = urllib.parse.urlsplit(url)
    return (parsed.scheme == 'https' and parsed.port in (None, 443)
            and not parsed.username and not parsed.password
            and any((parsed.hostname or '').endswith(suffix) for suffix in (
                '.blob.core.windows.net', '.actions.githubusercontent.com')))


def read_log(repo, job, token, deadline=None, sample=None):
    opener = urllib.request.build_opener(NoRedirect)
    request = urllib.request.Request(
        f'https://api.github.com/repositories/{repo}/actions/jobs/{job}/logs',
        headers={'Authorization': 'Bearer ' + token, 'User-Agent': 'bors-failure-diagnostics',
                 'Accept': 'application/vnd.github+json'})
    progress = {'bytes': 0}
    try:
        try:
            response = opener.open(request, timeout=10)
        except urllib.error.HTTPError as error:
            location = error.headers.get('Location', '')
            status = error.code
            error.close()
            if status != 302:
                reason = http_reason(status, error.headers)
                raise CollectionError(reason, http_status=status) from None
            if not signed_url(location):
                raise CollectionError('unexpected_redirect', http_status=status) from None
            # The authenticated request and signed-storage request have separate headers.
            response = opener.open(urllib.request.Request(location), timeout=10)
        with response:
            if response.status != 200:
                raise CollectionError('http_failure', http_status=response.status)
            observations = parse_lines(streamed_lines(response, progress, deadline), sample)
        if progress.get('oversized_records'):
            raise CollectionError('unsupported_diagnostic_record', bytes_read=progress['bytes'])
        return observations, progress['bytes']
    except CollectionError as error:
        error.bytes_read = progress['bytes']
        raise
    except http.client.IncompleteRead:
        raise CollectionError('truncated_response', bytes_read=progress['bytes']) from None
    except UnicodeError:
        raise CollectionError('decoding_failure', bytes_read=progress['bytes']) from None
    except (TimeoutError, socket.timeout):
        raise CollectionError('transport_timeout', bytes_read=progress['bytes']) from None
    except urllib.error.HTTPError as error:
        status = error.code
        error.close()
        raise CollectionError('http_failure', http_status=status, bytes_read=progress['bytes']) from None
    except (OSError, urllib.error.URLError):
        raise CollectionError('transport_failure', bytes_read=progress['bytes']) from None


def artifact_response(url, token=None):
    opener = urllib.request.build_opener(NoRedirect)
    headers = {'User-Agent': 'bors-failure-diagnostics', 'Accept': 'application/vnd.github+json'}
    if token:
        headers['Authorization'] = 'Bearer ' + token
    try:
        response = opener.open(urllib.request.Request(url, headers=headers), timeout=10)
    except urllib.error.HTTPError as error:
        location, status = error.headers.get('Location', ''), error.code
        error.close()
        if status == 302 and token and signed_url(location):
            return artifact_response(location)
        reason = http_reason(status, error.headers, 'raw_artifact_missing')
        raise CollectionError(reason, http_status=status) from None
    if response.status != 200:
        status = response.status
        response.close()
        raise CollectionError('artifact_http_failure', http_status=status)
    return response


def _read_raw_artifact(repo, request, token, deadline):
    run, attempt = request.get('run_id'), request.get('run_attempt')
    if type(run) is not int or type(attempt) is not int or run <= 0 or attempt <= 0:
        raise CollectionError('raw_artifact_missing')
    name = f'ci-failure-logs-{run}-{attempt}'
    found = []
    page = 1
    while True:
        with artifact_response(f'https://api.github.com/repositories/{repo}/actions/runs/{run}/artifacts?per_page=100&page={page}', token) as response:
            data = json.load(response)
        artifacts = data.get('artifacts', [])
        found.extend(a for a in artifacts if a.get('name') == name and not a.get('expired')
                     and a.get('workflow_run', {}).get('id') == run)
        if len(artifacts) < 100:
            break
        page += 1
        if time.monotonic() >= deadline:
            raise CollectionError('collection_timeout')
    if len(found) != 1:
        raise CollectionError('raw_artifact_missing' if not found else 'ambiguous_raw_artifact')
    artifact = found[0]['id']
    with tempfile.TemporaryFile() as archive:
        with artifact_response(f'https://api.github.com/repositories/{repo}/actions/artifacts/{artifact}/zip', token) as response:
            progress = {'bytes': 0}
            while True:
                if time.monotonic() >= deadline:
                    raise CollectionError('collection_timeout', bytes_read=progress['bytes'])
                chunk = response.read(65536)
                if not chunk:
                    break
                progress['bytes'] += len(chunk)
                archive.write(chunk)
            expected = response.headers.get('Content-Length')
            if expected is not None and int(expected) != progress['bytes']:
                raise CollectionError('truncated_response', bytes_read=progress['bytes'])
        archive.seek(0)
        try:
            with zipfile.ZipFile(archive) as bundle:
                names = [n for n in bundle.namelist() if n == 'lake-build.log.gz']
                if len(names) != 1:
                    raise CollectionError('raw_artifact_missing')
                with bundle.open(names[0]) as member, gzip.GzipFile(fileobj=member) as raw:
                    raw_progress = {'bytes': 0}
                    observations = parse_lines(streamed_lines(raw, raw_progress, deadline))
            if raw_progress.get('oversized_records'):
                raise CollectionError('unsupported_diagnostic_record', bytes_read=raw_progress['bytes'])
            return observations, raw_progress['bytes']
        except (zipfile.BadZipFile, gzip.BadGzipFile, EOFError, zlib.error):
            raise CollectionError('invalid_raw_artifact') from None


def read_raw_artifact(repo, request, token, deadline):
    try:
        return _read_raw_artifact(repo, request, token, deadline)
    except CollectionError:
        raise
    except http.client.IncompleteRead:
        raise CollectionError('truncated_response') from None
    except (TimeoutError, socket.timeout):
        raise CollectionError('transport_timeout') from None
    except (json.JSONDecodeError, UnicodeError, AttributeError, KeyError, TypeError):
        raise CollectionError('invalid_raw_artifact') from None
    except (OSError, urllib.error.URLError):
        raise CollectionError('transport_failure') from None


def collect(request):
    repo, jobs = request['repository_id'], request['job_ids']
    if type(repo) is not int or repo <= 0 or not 1 <= len(jobs) <= 2 or any(
            type(j) is not int or j <= 0 for j in jobs):
        raise ValueError('invalid_identity')
    deadline = time.monotonic() + request.get('time_budget', 60)
    result = []
    for job in jobs:
        source = 'job_log'
        sample = []
        try:
            observations, size = read_log(repo, job, os.environ['BORS_DIAGNOSTICS_TOKEN'], deadline, sample)
        except CollectionError as error:
            if error.reason != 'sampled_diagnostics':
                raise
            observations, size = read_raw_artifact(repo, request, os.environ['BORS_DIAGNOSTICS_TOKEN'], deadline)
            source = 'raw_artifact'
        if isinstance(observations, str):  # compatibility with recorded replay fixtures
            observations = parse(observations)
        record = dict(job_id=job, bytes=size, observations=observations, source=source)
        if not observations and sample:
            record['unmatched_sample'] = sample
        result.append(record)
    return dict(complete=True, jobs=result, parser_version=PARSER_VERSION)


def main():
    started = time.monotonic()
    cpu_started = time.process_time()
    signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(CollectionError("collection_timeout")))
    try:
        request = json.loads(Path(sys.argv[1]).read_text())
        budget = request.get('time_budget', 60)
        if type(budget) not in (int, float) or not 0 < budget <= 60:
            raise ValueError('invalid_budget')
        signal.setitimer(signal.ITIMER_REAL, budget + 1)
        result = collect(request)
    except CollectionError as error:
        result = dict(complete=False, reason=error.reason, bytes=error.bytes_read, http_status=error.http_status)
    except TimeoutError:
        result = dict(complete=False, reason='collection_timeout')
    except ValueError:
        result = dict(complete=False, reason='parser_failure')
    except Exception:
        # No URLs, token, log content or exception text crosses this boundary.
        result = dict(complete=False, reason='collector_failure')
    result['parser_version'] = PARSER_VERSION
    result['elapsed_ms'] = round((time.monotonic() - started) * 1000)
    result['cpu_ms'] = round((time.process_time() - cpu_started) * 1000)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
