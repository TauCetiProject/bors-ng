"""Trusted Git plumbing only: never check out or execute candidate code.

A root-wide flock protects fetch, assembly, eviction and upload across processes.
The mirror is expendable; immutable candidate identity lives in the database.
"""
import base64
import ctypes
import fcntl
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time

SHA = re.compile(r"[0-9a-f]{40}\Z")
REPO = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")
OUTPUT_LIMIT = 1024 * 1024


class Failure(Exception):
    def __init__(self, kind, **details):
        self.details = dict(kind=kind, **details)


def child_guard(owner):
    # Linux release images: a killed helper must not leave an unlocked Git
    # writer behind. The signal also covers process/container restart.
    ctypes.CDLL(None).prctl(1, signal.SIGKILL)
    if os.getppid() != owner:
        os.kill(os.getpid(), signal.SIGKILL)


def size(path):
    return sum(p.stat().st_size for p in path.rglob('*') if p.is_file())


class Mirror:
    def __init__(self, request):
        self.request = request
        self.root = Path(request['root'])
        self.repo = self.root / (request['repository'].replace('/', '--') + '.git')
        self.deadline = time.monotonic() + min(request.get('timeout', 600), 600)
        self.budget = request.get('max_bytes', 2 * 1024 ** 3)
        self.env = {k: v for k, v in os.environ.items()
                    if k in ['PATH', 'LANG', 'SSL_CERT_FILE', 'SSL_CERT_DIR', 'TMPDIR']}
        self.env.update(GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL='/dev/null',
                        GIT_TERMINAL_PROMPT='0', GIT_NO_REPLACE_OBJECTS='1',
                        GIT_CONFIG_COUNT='1',
                        GIT_CONFIG_KEY_0='http.https://github.com/.extraHeader',
                        GIT_CONFIG_VALUE_0='Authorization: Basic ' + base64.b64encode(
                            ('x-access-token:' + os.environ.get('BORS_GIT_TOKEN', '')).encode()).decode())
        self.url = 'https://github.com/' + request['repository'] + '.git'

    def git(self, *args, input=None, extra_env=None, check=True, binary=False):
        # No inherited configuration, hooks, credentials, replacement objects,
        # alternates, checkout, submodules, filters or external merge drivers.
        argv = ['git', '--git-dir=' + str(self.repo), '-c', 'core.hooksPath=/dev/null',
                '-c', 'credential.helper=', '-c', 'gc.auto=0', '-c', 'maintenance.auto=false',
                '-c', 'core.logAllRefUpdates=false', '-c', 'protocol.file.allow=never',
                '-c', 'fetch.fsckObjects=true', '-c', 'pack.threads=1',
                '-c', 'pack.windowMemory=32m', *args]
        remaining = min(120, self.deadline - time.monotonic())
        if remaining <= 0:
            raise Failure('timeout')
        with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
            owner = os.getpid()
            proc = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=out, stderr=err,
                                    env=self.env | (extra_env or {}), start_new_session=True,
                                    preexec_fn=lambda: child_guard(owner),
                                    pass_fds=(self.lock_fd,) if hasattr(self, 'lock_fd') else ())
            sent = False
            until = time.monotonic() + remaining
            try:
                while True:
                    try:
                        proc.communicate(input.encode() if input is not None and not sent else None,
                                         timeout=min(0.25, max(0.001, until - time.monotonic())))
                        break
                    except subprocess.TimeoutExpired:
                        sent = True
                        if time.monotonic() >= until:
                            raise Failure('timeout', operation=args[0])
                        if os.fstat(out.fileno()).st_size > OUTPUT_LIMIT or os.fstat(err.fileno()).st_size > OUTPUT_LIMIT:
                            raise Failure('output_limit', operation=args[0])
                        if size(self.root) > self.budget:
                            raise Failure('mirror_limit', max_bytes=self.budget)
            except Failure:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
                raise
            out.seek(0)
            result = out.read(OUTPUT_LIMIT + 1)
            if len(result) > OUTPUT_LIMIT:
                raise Failure('output_limit', operation=args[0])
            if check and proc.returncode:
                # Never emit stderr: Git/HTTP diagnostics may contain credentials.
                raise Failure('git_failed', operation=args[0], exit_status=proc.returncode)
            return (result if binary else result.decode('utf-8', errors='replace')), proc.returncode

    def enforce_budget(self):
        mirrors = sorted(self.root.glob('*.git'), key=lambda p: p.stat().st_mtime)
        total = sum(size(p) for p in mirrors)
        for mirror in mirrors:
            if total <= self.budget:
                break
            n = size(mirror)
            shutil.rmtree(mirror)
            total -= n
        if not self.repo.exists():
            raise Failure('mirror_limit', max_bytes=self.budget)

    def initialize(self):
        # Only helper-owned mirrors live here; flock plus the child death guard
        # excludes a surviving writer. Recover Git locks after interrupted work.
        if self.repo.exists():
            for lock in self.repo.rglob('*.lock'):
                lock.unlink()
            (self.repo / 'gc.pid').unlink(missing_ok=True)
        if not self.repo.exists():
            self.git('init', '--bare', '--template=')
        # Pin only the current inputs/result. Earlier attempt refs are disposable
        # and no remote-tracking branches or reflogs accumulate.
        refs, _ = self.git('for-each-ref', '--format=%(refname)', 'refs/bors/')
        for ref in refs.splitlines():
            self.git('update-ref', '-d', ref)
        # Synchronous GC under the same lock, never a detached background writer.
        if size(self.repo) > self.budget // 2:
            self.git('gc', '--prune=now')
        self.enforce_budget()

    def fetch(self, shas):
        missing = [sha for sha in shas if self.git('cat-file', '-e', sha + '^{commit}', check=False)[1]]
        if missing:
            self.git('fetch', '--quiet', '--no-tags', '--no-write-fetch-head', '--no-auto-gc',
                     self.url, *dict.fromkeys(missing))
        for index, sha in enumerate(shas):
            self.git('cat-file', '-e', sha + '^{commit}')
            self.git('update-ref', 'refs/bors/input/' + str(index), sha)
        self.enforce_budget()

    def assemble(self):
        members = self.request['members']
        base = self.request['identity']['base_sha']
        self.initialize()
        self.fetch([base] + [m['head_sha'] for m in members])
        prev = base
        # Retain merge ancestry exactly as the legacy second GitHub pass does.
        # Squash parents alone lose ancestry of earlier (possibly stacked) PRs.
        merge_head = base
        commits = []
        hints = self.diagnostic_hints(base, members) if self.request.get('diagnostics', True) else dict(complete=False, reason='disabled')
        for member in members:
            if self.git('merge-base', '--is-ancestor', member['head_sha'], merge_head, check=False)[1] == 0:
                return dict(status='conflict', details=dict(kind='already_merged',
                            pr=member['pr'], head_sha=member['head_sha'], paths=[]))
            output, status = self.git('merge-tree', '--write-tree', '--name-only', '-z',
                                      merge_head, member['head_sha'], check=False)
            fields = output.split('\0')
            if status == 1:
                paths = fields[1:fields.index('', 1)] if '' in fields[1:] else fields[1:]
                return dict(status='conflict', details=dict(kind='merge_conflict',
                            pr=member['pr'], head_sha=member['head_sha'], paths=paths,
                            merge_head=merge_head, base_sha=base, hints=hints))
            if status:
                raise Failure('git_failed', operation='merge-tree', exit_status=status, pr=member['pr'])
            tree = fields[0].strip()
            attribution = member['committer']
            # GitHub's legacy API field named committer actually sets author.
            env = dict(GIT_AUTHOR_NAME=attribution['name'], GIT_AUTHOR_EMAIL=attribution['email'],
                       GIT_COMMITTER_NAME=attribution['name'],
                       GIT_COMMITTER_EMAIL=attribution['email'],
                       GIT_AUTHOR_DATE=self.request['date'], GIT_COMMITTER_DATE=self.request['date'])
            prev = self.git('commit-tree', tree, '-p', prev,
                            input=member['commit_message'], extra_env=env)[0].strip()
            if merge_head != member['head_sha']:
                merge_head = self.git('commit-tree', tree, '-p', merge_head, '-p', member['head_sha'],
                                      input='[ci skip][skip ci] bors local merge ancestry\n',
                                      extra_env=env)[0].strip()
            commits.append(dict(pr=member['pr'], head_sha=member['head_sha'], commit=prev, tree=tree))
        tree = self.git('rev-parse', prev + '^{tree}')[0].strip()
        self.git('update-ref', 'refs/bors/result', prev)
        self.enforce_budget()
        os.utime(self.repo, None)
        toolchain_bytes, missing = self.git('show', prev + ':lean-toolchain', check=False, binary=True)
        try:
            content = toolchain_bytes.decode('utf-8') if not missing and len(toolchain_bytes) <= 4096 else None
        except UnicodeDecodeError:
            content = None
        toolchain = dict(content=content, unknown_reason=None if content is not None else 'absent or non-text/oversize')
        return dict(status='ok', result=self.request['identity'] | dict(commit=prev, tree=tree),
                    diagnostic_hints=hints, toolchain=toolchain,
                    commits=commits, git_version=self.git('--version')[0].strip(),
                    mirror_bytes=size(self.repo))

    def diagnostic_hints(self, base, members):
        # These are scheduling hints, never proof verdicts. Read immutable Git
        # blobs as data; no candidate parser, executable, metrics or cache.
        previous_deadline = self.deadline
        self.deadline = min(self.deadline, time.monotonic() + 15)
        try:
            changed = []
            owners = {}
            for member in members:
                ancestor = self.git('merge-base', base, member['head_sha'])[0].strip()
                paths = self.git('diff', '--name-only', '-z', '--no-renames',
                                 ancestor, member['head_sha'])[0].split('\0')
                paths = [p for p in paths if p]
                if len(paths) > 256 or sum(len(c['paths']) for c in changed) + len(paths) > 1024:
                    raise Failure('diagnostic_limit')
                changed.append(dict(pr=member['pr'], head_sha=member['head_sha'], paths=paths))
                for path in paths:
                    owners.setdefault(path, []).append(member['pr'])
            edges, missing, dependencies = [], [], set()
            reads = 0
            for member, change in zip(members, changed):
                pending = [p for p in change['paths'] if p.endswith('.lean')]
                seen = set()
                while pending:
                    path = pending.pop()
                    if path in seen:
                        continue
                    seen.add(path)
                    reads += 1
                    if reads > 256:
                        raise Failure('diagnostic_limit')
                    content, absent = self.git('show', member['head_sha'] + ':' + path, check=False)
                    if absent:
                        continue
                    if len(content) > 256 * 1024:
                        raise Failure('diagnostic_limit')
                    imports = local_imports(content)
                    for module in imports:
                        provider = module.replace('.', '/') + '.lean'
                        dependencies.add(provider)
                        for owner in owners.get(provider, []):
                            if owner != member['pr']:
                                edges.append([member['pr'], owner])
                        exists = self.git('cat-file', '-e', member['head_sha'] + ':' + provider,
                                          check=False)[1] == 0
                        if not exists:
                            missing.append(dict(pr=member['pr'], path=path, module=module,
                                                provider_path=provider))
                        else:
                            pending.append(provider)
            hints = dict(complete=True, changed_files=changed, import_edges=edges,
                         missing_imports=missing, dependency_paths=sorted(dependencies))
            if len(json.dumps(hints)) > 256 * 1024:
                raise Failure('diagnostic_limit')
            return hints
        except (Failure, UnicodeError):
            # Missing/large/ambiguous diagnostics cannot fail construction.
            return dict(complete=False, reason='bounded_diagnostics_unavailable')
        finally:
            self.deadline = previous_deadline

    def publish(self):
        commit = self.request['commit']
        ref = self.request['ref']
        self.git('cat-file', '-e', commit + '^{commit}')
        # Caller authorizes only staging. An explicit expected SHA prevents
        # overwriting a ref moved by another process between read and push.
        self.git('push', '--porcelain', '--force-with-lease=' + ref + ':' + self.request['expected'],
                 self.url, commit + ':' + ref)
        return dict(status='ok', ref=ref, commit=commit)

    def run(self):
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        with (self.root / '.lock').open('a') as lock:
            # Keep the flock alive in Git's process tree even if the helper dies.
            # A surviving index-pack/HTTP child cannot become an unlocked writer.
            self.lock_fd = lock.fileno()
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= self.deadline:
                        raise Failure('lock_timeout')
                    time.sleep(0.05)
            try:
                return getattr(self, self.request['operation'])()
            finally:
                # Failed fetches/assembly may leave unreachable objects; bound
                # retained bytes on both success and failure, under the lock.
                mirrors = sorted(self.root.glob('*.git'), key=lambda p: p.stat().st_mtime)
                total = sum(size(p) for p in mirrors)
                for mirror in mirrors:
                    if total <= self.budget:
                        break
                    total -= size(mirror)
                    shutil.rmtree(mirror)


def local_imports(content):
    # A deliberately small subset; the pinned Lean preflight remains authority.
    clean, index, depth = [], 0, 0
    while index < len(content):
        pair = content[index:index + 2]
        if pair == '/-':
            depth += 1
            index += 2
        elif pair == '-/' and depth:
            depth -= 1
            index += 2
            clean.append(' ')
        elif depth:
            if content[index] == '\n':
                clean.append('\n')
            index += 1
        elif pair == '--':
            end = content.find('\n', index)
            index = len(content) if end < 0 else end
        else:
            clean.append(content[index])
            index += 1
    if depth:
        raise Failure('ambiguous_header')
    imports = []
    for line in ''.join(clean).splitlines():
        line = line.strip()
        if not line or line in ['prelude', 'module']:
            continue
        match = re.fullmatch(r'(?:(?:public|private|meta)\s+)*import\s+([A-Za-z0-9_. ]+)', line)
        if match:
            for module in match[1].split():
                if module == 'TauCeti' or module.startswith('TauCeti.'):
                    imports.append(module)
        elif re.match(r'(?:(?:public|private|meta)\s+)*import\b', line):
            raise Failure('ambiguous_header')
        else:
            break
    return imports


def validate(request):
    if not REPO.fullmatch(request['repository']) or '..' in request['repository']:
        raise Failure('invalid_repository')
    if request['operation'] not in ['assemble', 'publish']:
        raise Failure('invalid_operation')
    if request['operation'] == 'assemble':
        if not request['members']:
            raise Failure('empty_members')
        shas = [request['identity']['base_sha']] + [m['head_sha'] for m in request['members']]
    else:
        shas = [request['commit'], request['expected']]
        if request['ref'] != 'refs/heads/staging':
            raise Failure('invalid_ref')
    if any(not SHA.fullmatch(sha) for sha in shas):
        raise Failure('invalid_sha')


def main():
    try:
        request = json.loads(Path(sys.argv[1]).read_text())
        validate(request)
        result = Mirror(request).run()
    except Failure as error:
        result = dict(status='error', details=error.details)
    except Exception:
        result = dict(status='error', details=dict(kind='helper_failed'))
    print(json.dumps(result))


if __name__ == '__main__':
    main()
