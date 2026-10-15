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


def parse(text):
    observations = []
    if len(text.encode('utf-8')) > MAX_BYTES:
        raise ValueError('log_limit')
    for raw in text.splitlines():
        if len(raw) > 8192:
            continue
        line = STAMP.sub('', ANSI.sub('', raw))
        audit = AUDIT.fullmatch(line)
        source = SOURCE.match(line) or LAKE.match(line)
        if audit and all(len(module) <= 512 for module in audit.groups()):
            observations.append(dict(kind='duplicate_declaration', paths=[
                m.replace('.', '/') + '.lean' for m in audit.groups()]))
        elif source and len(source[1]) <= 512:
            observations.append(dict(kind='compiler_error', paths=[source[1]]))
        if len(observations) > MAX_OBSERVATIONS:
            raise ValueError('observation_limit')
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
