#!/usr/bin/env python3
"""Read terminal job logs as untrusted scheduling hints, never execute them."""
import json
import os
import re
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

MAX_BYTES = 16 * 1024 * 1024
MAX_OBSERVATIONS = 128
ANSI = re.compile(r'\x1b\[[0-?]*[ -/]*[@-~]')
STAMP = re.compile(r'^\d{4}-\d\d-\d\dT[0-9:.]+Z ')
PATH = r'TauCeti(?:/[A-Za-z0-9_\-]+)+\.lean'
MODULE = r'TauCeti(?:\.[A-Za-z0-9_]+)+'
SOURCE = re.compile(r'^(?:error: )?(' + PATH + r'):\d+:\d+:\s*(?:error:|error\b)')
LAKE = re.compile(r'^error: (' + PATH + r'):\d+:\d+:')
AUDIT = re.compile(r"^audit-driver: import (" + MODULE +
                   r") failed, environment already contains '[^'\r\n]{1,512}' from (" + MODULE + r')$')
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
            or not 1 <= len(data['findings']) <= MAX_OBSERVATIONS):
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
            path = finding.get('path')
            if (set(finding) != {'linter', 'declaration', 'path', 'line', 'column'}
                    or not isinstance(path, str) or len(path) > 512 or not ENV_PATH.fullmatch(path)
                    or any(type(finding.get(k)) is not int or not 1 <= finding[k] <= 10_000_000
                           for k in ('line', 'column'))):
                raise ValueError('invalid_environment_path_or_range')
            observation.update(paths=[path], line=finding['line'], column=finding['column'])
        observations.append(observation)
    return observations


def parse(text):
    observations = []
    environment_seen = False
    targets_seen = False
    targets = None
    if len(text.encode('utf-8')) > MAX_BYTES:
        raise ValueError('log_limit')
    for raw in text.splitlines():
        # The single envelope is bounded independently of ordinary log lines.
        # Duplicate, malformed or incomplete records invalidate optional evidence.
        line = STAMP.sub('', ANSI.sub('', raw))
        if targets is not None:
            target = FAILED_TARGET.fullmatch(line) if len(raw) <= 8192 else None
            if target and len(target[1].replace('.', '/') + '.lean') <= 512:
                targets.append(dict(kind='lake_failed_target', module=target[1],
                                    paths=[target[1].replace('.', '/') + '.lean']))
                if len(observations) + len(targets) > MAX_OBSERVATIONS:
                    raise ValueError('observation_limit')
            elif line == 'error: build failed' and targets:
                observations.extend(targets)
                targets = None
            else:
                # Never keep only the supported subset of a failed-target list.
                raise ValueError('unsupported_or_incomplete_failed_targets')
            continue
        if line == FAILED_TARGETS_HEADER:
            if targets_seen:
                raise ValueError('duplicate_failed_targets')
            targets_seen = True
            targets = []
            continue
        if line.startswith(ENV_LOCATIONS):
            if environment_seen or len(line.encode('utf-8')) > 1024 * 1024:
                raise ValueError('environment_location_limit')
            environment_seen = True
            observations.extend(environment_locations(line[len(ENV_LOCATIONS):]))
            if len(observations) > MAX_OBSERVATIONS:
                raise ValueError('observation_limit')
            continue
        if len(raw) > 8192:
            continue
        audit = AUDIT.fullmatch(line)
        source = SOURCE.match(line) or LAKE.match(line)
        if audit and all(len(module) <= 512 for module in audit.groups()):
            observations.append(dict(kind='duplicate_declaration', paths=[
                m.replace('.', '/') + '.lean' for m in audit.groups()]))
        elif source and len(source[1]) <= 512:
            observations.append(dict(kind='compiler_error', paths=[source[1]]))
        if len(observations) > MAX_OBSERVATIONS:
            raise ValueError('observation_limit')
    if targets is not None:
        raise ValueError('truncated_failed_targets')
    # Deduplicate repeated Lake diagnostics without losing distinct offenders.
    return list({json.dumps(o, sort_keys=True): o for o in observations}.values())


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def signed_url(url):
    parsed = urllib.parse.urlsplit(url)
    return (parsed.scheme == 'https' and parsed.port in (None, 443)
            and not parsed.username and not parsed.password
            and any((parsed.hostname or '').endswith(suffix) for suffix in (
                '.blob.core.windows.net', '.actions.githubusercontent.com')))


def read_log(repo, job, token, deadline):
    opener = urllib.request.build_opener(NoRedirect)
    request = urllib.request.Request(
        f'https://api.github.com/repositories/{repo}/actions/jobs/{job}/logs',
        headers={'Authorization': 'Bearer ' + token, 'User-Agent': 'bors-failure-diagnostics',
                 'Accept': 'application/vnd.github+json'})
    try:
        response = opener.open(request, timeout=3)
    except urllib.error.HTTPError as error:
        location = error.headers.get('Location', '')
        error.close()
        if error.code != 302 or not signed_url(location):
            raise ValueError('log_unavailable') from None
        # Never forward the installation token to log storage.
        response = opener.open(urllib.request.Request(location), timeout=3)
    with response:
        if response.status != 200 or int(response.headers.get('Content-Length', '0')) > MAX_BYTES:
            raise ValueError('log_limit_or_status')
        chunks, size = [], 0
        while True:
            if time.monotonic() >= deadline:
                raise ValueError('log_deadline')
            chunk = response.read(64 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if size > MAX_BYTES:
                raise ValueError('log_limit')
            chunks.append(chunk)
        expected = response.headers.get('Content-Length')
        if expected is not None and size != int(expected):
            raise ValueError('truncated_log')
    return b''.join(chunks).decode('utf-8'), size


def collect(request):
    repo, jobs = request['repository_id'], request['job_ids']
    if type(repo) is not int or repo <= 0 or not 1 <= len(jobs) <= 2 or any(
            type(j) is not int or j <= 0 for j in jobs):
        raise ValueError('invalid_identity')
    deadline = time.monotonic() + 8
    result = []
    for job in jobs:
        text, size = read_log(repo, job, os.environ['BORS_DIAGNOSTICS_TOKEN'], deadline)
        result.append(dict(job_id=job, bytes=size, observations=parse(text)))
    return dict(complete=True, jobs=result)


def main():
    started = time.monotonic()
    cpu_started = time.process_time()
    signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(TimeoutError()))
    signal.alarm(9)
    try:
        result = collect(json.loads(Path(sys.argv[1]).read_text()))
    except Exception:
        # No URLs, token, log content or exception text crosses this boundary.
        result = dict(complete=False, reason='logs_unavailable_or_bounded')
    result['elapsed_ms'] = round((time.monotonic() - started) * 1000)
    result['cpu_ms'] = round((time.process_time() - cpu_started) * 1000)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
