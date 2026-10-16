"""Trusted Git plumbing only: never check out or execute candidate code.

A root-wide flock protects fetch, assembly, eviction and upload across processes.
The mirror is expendable; immutable candidate identity lives in the database.
"""
import base64
import ctypes
from collections import deque
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
ASSEMBLY_MEMBER_LIMIT = 128
COMPATIBILITY_MEMBER_LIMIT = 32
PROVIDER_OPERATIONS = ('import_providers', 'bounded_import_providers')
OFFLINE_OPERATIONS = (*PROVIDER_OPERATIONS, 'collision_sources')
PROVIDER_PATH = re.compile(r"TauCeti(?:/[A-Za-z_][A-Za-z0-9_'\-]*)+\.lean\Z")
BOUNDED_PROVIDER_POLICY = 'bounded_depth_v1'
PROVIDER_LIMITS = dict(max_depth=5, max_headers=64, max_file_bytes=256 * 1024,
                       max_source_bytes=512 * 1024, max_paths=128,
                       max_evidence_bytes=64 * 1024, total_timeout_ms=5000)


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
        self.started = time.monotonic()
        self.cpu_started = time.process_time()
        self.deadline = self.started + min(request.get('timeout', 600), 600)
        self.phase = 'lock'
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

    def git(self, *args, input=None, extra_env=None, check=True, binary=False, output_limit=OUTPUT_LIMIT):
        # No inherited configuration, hooks, credentials, replacement objects,
        # alternates, checkout, submodules, filters or external merge drivers.
        argv = ['git', '--git-dir=' + str(self.repo), '-c', 'core.hooksPath=/dev/null',
                '-c', 'credential.helper=', '-c', 'gc.auto=0', '-c', 'maintenance.auto=false',
                '-c', 'core.logAllRefUpdates=false', '-c', 'protocol.file.allow=never',
                '-c', 'fetch.fsckObjects=true', '-c', 'pack.threads=1',
                '-c', 'pack.windowMemory=32m', *args]
        remaining = min(120, self.deadline - time.monotonic())
        if remaining <= 0:
            raise Failure('timeout', operation=args[0], phase=self.phase)
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
                            raise Failure('timeout', operation=args[0], phase=self.phase)
                        if os.fstat(out.fileno()).st_size > output_limit or os.fstat(err.fileno()).st_size > output_limit:
                            raise Failure('output_limit', operation=args[0])
                        if self.request['operation'] not in OFFLINE_OPERATIONS and size(self.root) > self.budget:
                            raise Failure('mirror_limit', max_bytes=self.budget)
            except Failure:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
                raise
            out.seek(0)
            result = out.read(output_limit + 1)
            if len(result) > output_limit:
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
        # Keep the previous exact pins until the next fetch completes. Git
        # advertises these commits as HAVE tips; deleting them before fetch
        # makes a warm mirror download the entire reachable history again.
        # fetch() atomically replaces them only after verifying all new inputs.
        refs, _ = self.git('for-each-ref', '--format=%(refname)', 'refs/bors/')
        self.previous_refs = refs.splitlines()
        # Synchronous GC under the same lock, never a detached background writer.
        if size(self.repo) > self.budget // 2:
            self.git('gc', '--prune=now')
        self.enforce_budget()

    def fetch(self, shas):
        objects, _ = self.git('cat-file', '--batch-check', input=''.join(sha + '^{commit}\n' for sha in shas))
        missing = [sha for sha, line in zip(shas, objects.splitlines()) if line.endswith(' missing')]
        if missing:
            if self.request['operation'] == 'compatibility' and self.request.get('bootstrap_timeout', 0) > 0:
                counts, _ = self.git('count-objects', '-v')
                counts = dict(line.split(': ', 1) for line in counts.splitlines())
                bootstrap = self.repo / 'bors-source-bootstrap-v1'
                if counts.get('count') == '0' and counts.get('in-pack') == '0':
                    if bootstrap.exists():
                        raise Failure('cold_mirror_unavailable', operation='fetch', phase='fetch')
                    # An empty mirror cannot retain a half-fetched pack after
                    # Git is killed. Allow the existing 30s admission budget
                    # once to seed it, then keep ordinary background work at 8s.
                    # The total includes lock/startup time, not 30s per input.
                    bootstrap.touch(exist_ok=False)
                    self.deadline = self.started + min(self.request['bootstrap_timeout'], 30)
            self.git('fetch', '--quiet', '--no-tags', '--no-write-fetch-head', '--no-auto-gc',
                     self.url, *dict.fromkeys(missing))
        verified, _ = self.git('cat-file', '--batch-check', input=''.join(sha + '^{commit}\n' for sha in shas))
        lines = verified.splitlines()
        if len(lines) != len(shas) or any(not re.fullmatch(re.escape(sha) + r' commit [0-9]+', line)
                                        for sha, line in zip(shas, lines)):
            raise Failure('missing_commit')
        # A completed fetch starts a warm object generation. If ordinary GC
        # later empties it, that new cold generation may bootstrap once again.
        (self.repo / 'bors-source-bootstrap-v1').unlink(missing_ok=True)
        # Pin all exact inputs in one atomic transaction. Checking each SHA and
        # deleting/updating each ref in a separate process dominates tiny turns
        # on the production container. The batch check above rejects missing/non-commit inputs.
        targets = {'refs/bors/input/' + str(index): sha for index, sha in enumerate(shas)}
        self.git('update-ref', '--stdin',
                 input=''.join('delete ' + ref + '\n' for ref in self.previous_refs if ref not in targets)
                       + ''.join('update ' + ref + ' ' + sha + '\n' for ref, sha in targets.items()))
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
        for member in members:
            if self.git('merge-base', '--is-ancestor', member['head_sha'], merge_head, check=False)[1] == 0:
                return dict(status='conflict', details=dict(kind='already_merged',
                            pr=member['pr'], head_sha=member['head_sha'], paths=[]))
            output, status = self.git('merge-tree', '--write-tree', '--name-only', '-z',
                                      merge_head, member['head_sha'], check=False)
            fields = output.split('\0')
            if status == 1:
                hints = self.diagnostic_hints(base, members) if self.request.get('diagnostics', True) else dict(complete=False, reason='disabled')
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
        hints = self.diagnostic_hints(base, members, prev, commits) if self.request.get('diagnostics', True) else dict(complete=False, reason='disabled')
        # Scheduling barriers use exact immutable trees, independently of
        # optional failure hints and mutable PR file-list endpoints.
        paths = self.git('diff', '--name-only', '-z', '--no-renames', base, prev)[0].split('\0')
        paths = [p for p in paths if p]
        barriers = dict(complete=len(paths) < 300, paths=paths if len(paths) < 300 else [])
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
                            diagnostic_hints=hints, barriers=barriers, toolchain=toolchain,
                    commits=commits, git_version=self.git('--version')[0].strip(),
                    mirror_bytes=size(self.repo))

    def diagnostic_hints(self, base, members, candidate=None, commits=None):
        # These are scheduling hints, never proof verdicts. Read immutable Git
        # blobs as data; no candidate parser, executable, metrics or cache.
        started = time.monotonic()
        previous_deadline = self.deadline
        self.deadline = min(self.deadline, started + 15)
        hints = dict(complete=False, ownership_complete=False, dependencies_complete=False)
        try:
            changed = self.diagnostic_owners(base, members, candidate, commits)
            if time.monotonic() >= self.deadline:
                raise Failure('timeout')
            hints.update(ownership_complete=True, changed_files=changed,
                         ownership_elapsed_ms=round((time.monotonic() - started) * 1000))
            if candidate:
                hints['provenance'] = self.request['identity'] | dict(
                    commit=candidate, tree=commits[-1]['tree'],
                    members=[dict(pr=m['pr'], head_sha=m['head_sha']) for m in members])
            # Optional imports get their own five-second cap inside the unchanged
            # total 15s budget. Publish them atomically; partial absence is unknown.
            self.deadline = min(self.deadline, time.monotonic() + 5)
            optional = self.diagnostic_dependencies(changed, candidate)
            if time.monotonic() >= self.deadline:
                raise Failure('timeout')
            hints.update(optional, dependencies_complete=True, complete=True)
            if len(json.dumps(hints)) > 256 * 1024:
                raise Failure('diagnostic_limit')
            return hints
        except Failure as error:
            hints.update(complete=False, dependencies_complete=False, reason=error.details['kind'])
            if hints['ownership_complete']:
                hints['changed_files'] = changed
            # No partial optional evidence escapes, even on the final size bound.
            for key in ['import_edges', 'missing_imports', 'dependency_paths']:
                hints.pop(key, None)
            return hints
        except UnicodeError:
            hints.update(complete=False, reason='non_text_source')
            return hints
        finally:
            hints['elapsed_ms'] = round((time.monotonic() - started) * 1000)
            self.deadline = previous_deadline

    def diagnostic_owners(self, base, members, candidate, commits):
        if candidate:
            # Verify the exact chain just assembled before reusing its provenance.
            if (not commits or len(commits) != len(members)
                    or [(c['pr'], c['head_sha']) for c in commits] !=
                       [(m['pr'], m['head_sha']) for m in members]
                    or commits[-1]['commit'] != candidate):
                raise Failure('diagnostic_provenance')
            objects = self.diagnostic_objects([c['commit'] for c in commits], 'commit')
            parent = base
            for commit, content in zip(commits, objects):
                headers = content.decode('utf-8').split('\n\n', 1)[0].splitlines()
                if ([h[5:] for h in headers if h.startswith('tree ')] != [commit['tree']]
                        or [h[7:] for h in headers if h.startswith('parent ')] != [parent]):
                    raise Failure('diagnostic_provenance')
                parent = commit['commit']
        # Ownership describes each exact head relative to its fork point, not
        # just its squash delta: stacked or identical edits reappear on retry.
        # Git has no pairwise merge-base batch interface. Keep these bounded
        # ancestry queries, then collect all head diffs in one process.
        pairs = [(m['head_sha'], self.git('merge-base', base, m['head_sha'])[0].strip())
                 for m in members]
        output = self.git('diff-tree', '--stdin', '--raw', '--no-abbrev', '--no-renames',
                          '-r', '-z', '--always',
                          input=''.join(head + ' ' + ancestor + '\n' for head, ancestor in pairs))[0]
        fields = output.split('\0')
        changes, position = [], 0
        for head, _ in pairs:
            if position >= len(fields) or fields[position] != head:
                raise Failure('diagnostic_provenance')
            position += 1
            paths = []
            while position < len(fields) and fields[position].startswith(':'):
                if (not re.fullmatch(r':[0-7]{6} [0-7]{6} [0-9a-f]{40} [0-9a-f]{40} [AMDT]', fields[position])
                        or position + 1 >= len(fields)):
                    raise Failure('diagnostic_provenance')
                paths.append(fields[position + 1])
                position += 2
            changes.append(paths)
        if position != len(fields) - 1 or fields[-1] != '':
            raise Failure('diagnostic_provenance')
        if (any(len(paths) > 256 or any(not re.fullmatch(r'[A-Za-z0-9_./-]{1,512}', p)
                                      for p in paths) for paths in changes)
                or sum(map(len, changes)) > 1024):
            raise Failure('diagnostic_limit')
        changed = [dict(pr=m['pr'], head_sha=m['head_sha'], paths=paths)
                   for m, paths in zip(members, changes)]
        if len(json.dumps(changed)) > 128 * 1024:
            raise Failure('diagnostic_limit')
        return changed

    def diagnostic_objects(self, oids, kind):
        data = self.git('cat-file', '--batch', input=''.join(oid + '\n' for oid in oids),
                        binary=True, output_limit=4 * 1024 * 1024)[0]
        objects, position = [], 0
        for oid in oids:
            end = data.find(b'\n', position)
            header = data[position:end].decode('ascii').split()
            if len(header) != 3 or header[:2] != [oid, kind] or not header[2].isdigit():
                raise Failure('diagnostic_provenance')
            length = int(header[2])
            position = end + 1
            if length > 256 * 1024 or data[position + length:position + length + 1] != b'\n':
                raise Failure('diagnostic_limit')
            objects.append(data[position:position + length])
            position += length + 1
        if position != len(data):
            raise Failure('diagnostic_provenance')
        return objects

    def diagnostic_dependencies(self, changed, candidate, owners=None, budget=None):
        if owners is None:
            owners = {}
            for change in changed:
                for path in change['paths']:
                    owners.setdefault(path, []).append(change['pr'])
        if budget is None:
            budget = dict(reads=0, bytes=0)
        if not candidate:
            # Conflicts have no assembled tree. Preserve the legacy exact-head
            # import coverage, under the same independent total byte/time caps.
            result = dict(changed_files=[], import_edges=[], missing_imports=[], dependency_paths=[])
            for change in changed:
                part = self.diagnostic_dependencies([change], change['head_sha'], owners, budget)
                for key in result:
                    result[key].extend(part[key])
                if len(json.dumps(result)) > 192 * 1024:
                    raise Failure('diagnostic_limit')
            result['dependency_paths'] = sorted(set(result['dependency_paths']))
            return result
        listing = self.git('ls-tree', '-r', '-l', '-z', candidate,
                           output_limit=4 * 1024 * 1024)[0]
        files = {}
        for entry in listing.split('\0'):
            if entry:
                meta, path = entry.split('\t', 1)
                files[path] = meta.split()
        paths = sorted({p for c in changed for p in c['paths'] if p.endswith('.lean') and p in files})
        if any(files[p][0] != '100644' or files[p][1] != 'blob' for p in paths):
            raise Failure('diagnostic_limit')
        budget['reads'] += len(paths)
        budget['bytes'] += sum(int(files[p][3]) for p in paths)
        if (budget['reads'] > 256 or any(int(files[p][3]) > 256 * 1024 for p in paths)
                or budget['bytes'] > 3 * 1024 * 1024):
            raise Failure('diagnostic_limit')
        oids = list(dict.fromkeys(files[p][2] for p in paths))
        imports = {oid: local_imports(blob.decode('utf-8'))
                   for oid, blob in zip(oids, self.diagnostic_objects(oids, 'blob'))}
        edges, missing, dependencies, per_member = set(), [], set(), []
        for change in changed:
            member_dependencies = set()
            for path in change['paths']:
                for module in imports.get(files.get(path, ['', '', None])[2], []):
                    provider = module.replace('.', '/') + '.lean'
                    member_dependencies.add(provider)
                    for owner in owners.get(provider, []):
                        if owner != change['pr']:
                            edges.add((change['pr'], owner))
                    if provider not in files:
                        missing.append(dict(pr=change['pr'], path=path, module=module, provider_path=provider))
            dependencies.update(member_dependencies)
            per_member.append(change | dict(dependency_paths=sorted(member_dependencies)))
        result = dict(changed_files=per_member, import_edges=[list(e) for e in sorted(edges)],
                      missing_imports=missing, dependency_paths=sorted(dependencies))
        if len(json.dumps(result)) > 192 * 1024:
            raise Failure('diagnostic_limit')
        return result

    def collision_sources(self):
        """Identity of exactly two audit paths, offline, without interpreting source.

        An ownerless path is not presumed to exist on base. Return both exact
        tree entries, and read each unique blob to reject missing/partial objects.
        No checkout, fetch, source evaluation, header cache or publication.
        """
        binding, paths = self.request['binding'], self.request['paths']
        self.deadline = min(self.deadline, self.started + 5)
        started_cpu = time.process_time()

        def entries(commit):
            raw = self.git('ls-tree', '-l', '-z', commit, '--', *paths, output_limit=64 * 1024)[0]
            found = {}
            for entry in raw.split('\0'):
                if not entry:
                    continue
                meta, path = entry.split('\t', 1)
                mode, kind, oid, length = meta.split()
                if (path not in paths or mode != '100644' or kind != 'blob'
                        or not SHA.fullmatch(oid) or not length.isdigit()):
                    raise Failure('unsupported_source')
                found[path] = dict(blob=oid, bytes=int(length))
            return found

        current, base = entries(binding['commit']), entries(binding['base_sha'])
        if set(current) != set(paths):
            raise Failure('missing_source')
        blobs = {entry['blob']: entry['bytes'] for entry in [*current.values(), *base.values()]}
        if any(n > 256 * 1024 for n in blobs.values()) or sum(blobs.values()) > 512 * 1024:
            raise Failure('source_byte_limit')
        contents = self.diagnostic_objects(list(blobs), 'blob')
        if len(contents) != len(blobs) or any(len(content) != n for content, n in zip(contents, blobs.values())):
            raise Failure('partial_source')
        result = dict(complete=True, policy='collision-source-identity-v1', binding=binding,
                      files=[dict(path=p, candidate=current[p], base=base.get(p)) for p in paths],
                      source_bytes=sum(blobs.values()),
                      cpu_ms=round((time.process_time() - started_cpu) * 1000),
                      elapsed_ms=round((time.monotonic() - self.started) * 1000),
                      authority='retry_only', proof_authority=False)
        if len(json.dumps(result)) > 64 * 1024:
            raise Failure('source_output_limit')
        if time.monotonic() >= self.deadline:
            raise Failure('timeout')
        return result

    def import_providers(self):
        """Depth-one retry hints from a few unchanged diagnostic files, offline.

        Never initializes/fetches a mirror or scans a project inventory. The
        existing source-only header cache is optional; a cold cache reads only
        the requested immutable blobs with the same full header parser.
        """
        cpu_started = time.process_time()
        binding = self.request['binding']
        paths = self.request['paths']
        self.deadline = min(self.deadline, self.started + 5)

        def entries(commit, selected):
            listing = self.git('ls-tree', '-l', '-z', commit, '--', *selected,
                               output_limit=64 * 1024)[0]
            result = {}
            for entry in listing.split('\0'):
                if not entry:
                    continue
                meta, path = entry.split('\t', 1)
                mode, kind, oid, length = meta.split()
                if (path not in selected or mode != '100644' or kind != 'blob'
                        or not SHA.fullmatch(oid) or not length.isdigit()):
                    raise Failure('unsupported_source')
                result[path] = (oid, int(length))
            if set(result) != set(selected):
                raise Failure('missing_source')
            return result

        files = entries(binding['commit'], paths)
        if files != entries(binding['base_sha'], paths):
            raise Failure('changed_diagnostic_source')
        if (any(length > 256 * 1024 for _, length in files.values())
                or sum(length for _, length in files.values()) > 512 * 1024):
            raise Failure('provider_byte_limit')
        cache = self.repo / 'bors-headers-local-source-v2.json'
        stored = {}
        if cache.exists() and cache.stat().st_size <= 4 * 1024 * 1024:
            try:
                stored = json.loads(cache.read_text())
                if not isinstance(stored, dict):
                    stored = {}
            except (OSError, ValueError, UnicodeError):
                pass
        headers, cold = {}, []
        for oid, _ in files.values():
            modules = stored.get(oid)
            if (isinstance(modules, list) and len(modules) <= 128
                    and all(isinstance(m, str) and len(m) <= 512
                            and re.fullmatch(r'TauCeti(?:\.[A-Za-z_][A-Za-z0-9_\']*)*', m)
                            for m in modules)):
                headers[oid] = modules
            elif oid not in cold:
                cold.append(oid)
        for oid, content in zip(cold, self.diagnostic_objects(cold, 'blob') if cold else []):
            headers[oid] = compatibility_imports(content.decode('utf-8'))
        providers = {path: sorted({m.replace('.', '/') + '.lean' for m in headers[oid]})
                     for path, (oid, _) in files.items()}
        all_providers = sorted({p for values in providers.values() for p in values})
        if len(all_providers) > 128:
            raise Failure('provider_count_limit')
        provider_files = entries(binding['commit'], all_providers) if all_providers else {}
        # Cache only source data, never verdicts or compiled candidate artifacts.
        if cold:
            stored.update(headers)
            encoded = json.dumps(stored)
            if len(encoded) <= 4 * 1024 * 1024:
                temp = cache.with_suffix('.tmp')
                temp.write_text(encoded)
                temp.replace(cache)
        result = dict(complete=True, binding=binding, files=[dict(
            path=path, blob=files[path][0], bytes=files[path][1], providers=[dict(
                path=p, blob=provider_files[p][0]) for p in providers[path]]) for path in paths],
            cpu_ms=round((time.process_time() - cpu_started) * 1000),
            cold_blobs=len(cold), elapsed_ms=round((time.monotonic() - self.started) * 1000),
            authority='retry_only', proof_authority=False)
        if len(json.dumps(result)) > 64 * 1024:
            raise Failure('provider_output_limit')
        if time.monotonic() >= self.deadline:
            raise Failure('timeout')
        return result

    def bounded_import_providers(self):
        """Complete the declared depth, not the closure. All limits span all roots.

        Frozen changed paths terminate expansion. Depth-five unowned paths are
        explicit residuals; missing objects or partial work never earn coverage.
        """
        cpu_started = time.process_time()
        binding, roots = self.request['binding'], sorted(self.request['paths'])
        changed = set(self.request['changed_paths'])
        limits = PROVIDER_LIMITS
        cache = self.repo / 'bors-headers-local-source-v2.json'
        stored = {}
        if cache.exists() and cache.stat().st_size <= 4 * 1024 * 1024:
            try:
                stored = json.loads(cache.read_text())
                if not isinstance(stored, dict):
                    stored = {}
            except (OSError, ValueError, UnicodeError):
                pass
        nodes, seen = [], set(roots)
        queue = deque((p, 0) for p in roots)
        source_bytes = header_count = cold_blobs = 0
        self.provider_progress = dict(nodes=nodes)
        self.provider_queue = queue

        def check():
            if time.monotonic() >= self.deadline:
                raise Failure('timeout')
            if len(json.dumps(nodes).encode()) > limits['max_evidence_bytes']:
                raise Failure('provider_output_limit')

        def entry(commit, path):
            listing = self.git('ls-tree', '-l', '-z', commit, '--', path,
                               output_limit=2048)[0].split('\0')
            if len(listing) != 2 or listing[-1] != '' or '\t' not in listing[0]:
                raise Failure('missing_source')
            meta, actual = listing[0].split('\t', 1)
            fields = meta.split()
            if (actual != path or len(fields) != 4 or fields[:2] != ['100644', 'blob']
                    or not SHA.fullmatch(fields[2]) or not fields[3].isdigit()):
                raise Failure('unsupported_source')
            if int(fields[3]) > limits['max_file_bytes']:
                raise Failure('provider_byte_limit')
            return fields[2], int(fields[3])

        while queue:
            check()
            path, depth = queue.popleft()
            self.provider_progress['active_path'] = path
            blob, length = entry(binding['commit'], path)
            node = dict(path=path, blob=blob, bytes=length, depth=depth)
            if path in changed:
                if depth == 0:
                    raise Failure('changed_diagnostic_source')
                node['stop'] = 'changed_frontier'
            elif depth == limits['max_depth']:
                node['stop'] = 'depth_limit'
            else:
                if (blob, length) != entry(binding['base_sha'], path):
                    raise Failure('unowned_changed_source')
                header_count += 1
                source_bytes += length
                self.provider_progress.update(headers=header_count, source_bytes=source_bytes)
                if header_count > limits['max_headers']:
                    raise Failure('provider_header_limit')
                if source_bytes > limits['max_source_bytes']:
                    raise Failure('provider_byte_limit')
                modules = stored.get(blob)
                if not (isinstance(modules, list) and len(modules) <= 128
                        and all(isinstance(m, str) and len(m) <= 512 and
                                re.fullmatch(r"TauCeti(?:\.[A-Za-z_][A-Za-z0-9_']*)*", m)
                                for m in modules)):
                    content = self.diagnostic_objects([blob], 'blob')[0]
                    if len(content) != length:
                        raise Failure('incomplete_source')
                    try:
                        modules = compatibility_imports(content.decode('utf-8'))
                    except UnicodeError:
                        raise Failure('non_text_source')
                    stored[blob] = modules
                    cold_blobs += 1
                    self.provider_progress['cold_blobs'] = cold_blobs
                providers = sorted({m.replace('.', '/') + '.lean' for m in modules})
                if any(len(p) > 512 or not PROVIDER_PATH.fullmatch(p) for p in providers):
                    raise Failure('unsupported_provider')
                node.update(base_blob=blob, providers=providers)
                for provider in providers:
                    if provider not in seen:
                        seen.add(provider)
                        if len(seen) > limits['max_paths']:
                            raise Failure('provider_count_limit')
                        queue.append((provider, depth + 1))
            nodes.append(node)
            self.provider_progress.pop('active_path', None)
            check()
        # ls-tree alone cannot establish availability of a terminal blob.
        oids = list(dict.fromkeys(n['blob'] for n in nodes))
        checked = self.git('cat-file', '--batch-check', input='\n'.join(oids) + '\n',
                           output_limit=16 * 1024)[0].splitlines()
        expected = {n['blob']: n['bytes'] for n in nodes}
        if checked != [f"{oid} blob {expected[oid]}" for oid in oids]:
            raise Failure('missing_source')
        result = dict(complete=False, bounded_complete=True, policy=BOUNDED_PROVIDER_POLICY,
                      full_closure_complete=False, binding=binding, roots=roots, nodes=nodes,
                      residual_frontier=[n['path'] for n in nodes if n.get('stop') == 'depth_limit'],
                      changed_frontier=[n['path'] for n in nodes if n.get('stop') == 'changed_frontier'],
                      limits=limits, headers=header_count, source_bytes=source_bytes,
                      cold_blobs=cold_blobs, cpu_ms=round((time.process_time() - cpu_started) * 1000),
                      authority='retry_only', proof_authority=False)
        # Cache only validated immutable source headers, never coverage or verdicts.
        encoded = json.dumps(stored)
        if cold_blobs and len(encoded.encode()) <= 4 * 1024 * 1024:
            check()
            temp = cache.with_suffix('.tmp')
            temp.write_text(encoded)
            temp.replace(cache)
        result['elapsed_ms'] = round((time.monotonic() - self.started) * 1000)
        if len(json.dumps(result).encode()) > limits['max_evidence_bytes']:
            raise Failure('provider_output_limit')
        check()
        return result

    def compatibility(self):
        """Exact singleton trees, source data only; unknown coverage never holds a PR."""
        base = self.request['base_sha']
        members = self.request['members']
        self.phase = 'initialize'
        self.initialize()
        self.phase = 'fetch'
        self.fetch([base] + [m['head_sha'] for m in members])
        results, headers = [], {}
        base_error = None
        try:
            self.phase = 'base_headers'
            baseline = self.source_inventory(base, headers)
            baseline_missing = {(path, module) for path, modules in baseline.items() for module in modules
                                if module.replace('.', '/') + '.lean' not in baseline}
        except (Failure, UnicodeError) as error:
            baseline_missing = None
            base_error = error.details if isinstance(error, Failure) else dict(kind='non_text_source')
        for member in members:
            identity = dict(pr=member['pr'], head_sha=member['head_sha'], base_sha=base,
                            policy='local-source-v3')
            try:
                self.phase = 'merge_tree'
                output, status = self.git('merge-tree', '--write-tree', '--name-only', '-z',
                                          base, member['head_sha'], check=False)
                fields = output.split('\0')
                if status == 1:
                    paths = fields[1:fields.index('', 1)] if '' in fields[1:] else fields[1:]
                    if any(not re.fullmatch(r'[A-Za-z0-9_./-]{1,512}', path) for path in paths):
                        raise Failure('unsupported_path')
                    results.append(identity | dict(status='conflict', paths=paths[:64]))
                    continue
                if status:
                    raise Failure('merge_unavailable')
                tree = fields[0].strip()
                if baseline_missing is None:
                    raise Failure('unknown_base_headers')
                self.phase = 'candidate_headers'
                inventory = self.source_inventory(tree, headers)
                missing = [dict(path=path, module=module,
                                provider_path=module.replace('.', '/') + '.lean')
                           for path, modules in inventory.items() for module in modules
                           if module.replace('.', '/') + '.lean' not in inventory
                           and (path, module) not in baseline_missing]
                results.append(identity | dict(status='missing_source' if missing else 'pass',
                                                tree=tree, missing_imports=missing[:64],
                                                coverage='all_project_local_headers'))
            except (Failure, UnicodeError) as error:
                reason = error.details['kind'] if isinstance(error, Failure) else 'non_text_source'
                unknown = identity | dict(status='unknown', reason=reason, phase=self.phase,
                                           operation=error.details.get('operation') if isinstance(error, Failure) else None)
                if base_error:
                    unknown['base_headers_reason'] = base_error['kind']
                    unknown['base_headers_operation'] = base_error.get('operation')
                results.append(unknown)
        return dict(status='ok', results=results)

    def source_inventory(self, tree, headers):
        # Cache parsed headers by immutable blob, not main identity. Every pass
        # still inventories the exact prospective tree (including PR additions).
        listing = self.git('ls-tree', '-r', '-l', '-z', tree, output_limit=4 * 1024 * 1024)[0]
        files, lengths = {}, {}
        for entry in listing.split('\0'):
            if not entry:
                continue
            meta, path = entry.split('\t', 1)
            mode, kind, oid, length = meta.split()
            if path == 'TauCeti.lean' or (path.startswith('TauCeti/') and path.endswith('.lean')):
                if (kind != 'blob' or mode != '100644' or int(length) > 256 * 1024
                        or not re.fullmatch(r'[A-Za-z0-9_./]{1,512}', path)):
                    raise Failure('unsupported_source')
                files[path] = oid
                lengths[oid] = int(length)
        cache = self.repo / 'bors-headers-local-source-v2.json'
        try:
            stored = json.loads(cache.read_text())
        except (OSError, ValueError):
            stored = {}
        headers.update({oid: modules for oid, modules in stored.items()
                        if isinstance(modules, list) and all(isinstance(m, str) for m in modules)})
        needed = list(dict.fromkeys(oid for oid in files.values() if oid not in headers))
        try:
            # Bound by actual ls-tree byte lengths, rather than launching one
            # Git process per twelve tiny files (~1,230 for current TauCeti).
            # Keep each output below 1 MiB plus framing, and <=512 objects.
            batches, oids, total = [], [], 0
            for oid in needed:
                if oids and (total + lengths[oid] > 1024 * 1024 or len(oids) >= 512):
                    batches.append(oids)
                    oids, total = [], 0
                oids.append(oid)
                total += lengths[oid]
            if oids:
                batches.append(oids)
            for oids in batches:
                data = self.git('cat-file', '--batch', input='\n'.join(oids) + '\n', binary=True, output_limit=4 * 1024 * 1024)[0]
                position = 0
                for oid in oids:
                    end = data.index(b'\n', position)
                    actual, kind, length = data[position:end].decode('ascii').split()
                    length = int(length)
                    position = end + 1
                    if (actual != oid or kind != 'blob' or length != lengths[oid]
                            or len(data) < position + length + 1 or data[position + length] != 10):
                        raise Failure('incomplete_source')
                    content = data[position:position + length]
                    position += length + 1
                    headers[oid] = compatibility_imports(content.decode('utf-8'))
                if position != len(data):
                    raise Failure('incomplete_source')
        finally:
            # Retain validated immutable headers even after bounded partial work.
            # Completeness is still required below; this never caches a PR pass.
            retained = {oid: headers[oid] for oid in set(files.values()) if oid in headers}
            encoded = json.dumps(retained)
            if len(encoded) <= 4 * 1024 * 1024:
                temp = cache.with_suffix('.tmp')
                temp.write_text(encoded)
                temp.replace(cache)
        return {path: headers[oid] for path, oid in files.items()}

    def promote(self):
        self.initialize()
        self.fetch([self.request['expected'], self.request['commit']])
        if self.git('merge-base', '--is-ancestor', self.request['expected'],
                    self.request['commit'], check=False)[1]:
            raise Failure('non_descendant_promotion')
        return self.publish()

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
        if self.request['operation'] in OFFLINE_OPERATIONS:
            self.deadline = min(self.deadline, self.started + 5)
            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                raise Failure('timeout')
            previous_handler = signal.getsignal(signal.SIGALRM)
            def timeout(signum, frame):
                raise Failure('timeout')
            signal.signal(signal.SIGALRM, timeout)
            signal.setitimer(signal.ITIMER_REAL, remaining)
            try:
                return self.run_locked()
            finally:
                signal.setitimer(signal.ITIMER_REAL, 0)
                signal.signal(signal.SIGALRM, previous_handler)
        return self.run_locked()

    def run_locked(self):
        if self.request['operation'] in OFFLINE_OPERATIONS:
            self.deadline = min(self.deadline, self.started + 5)
            if not self.repo.is_dir():
                raise Failure('mirror_unavailable')
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
                    if self.request['operation'] in OFFLINE_OPERATIONS:
                        raise Failure('mirror_busy')
                    if time.monotonic() >= self.deadline:
                        raise Failure('lock_timeout', phase='lock')
                    time.sleep(0.05)
            try:
                self.phase = self.request['operation']
                return getattr(self, self.request['operation'])()
            except Failure as error:
                error.details.setdefault('phase', self.phase)
                if self.request['operation'] == 'bounded_import_providers':
                    progress = getattr(self, 'provider_progress', {}).copy()
                    progress['pending_paths'] = [p for p, _ in getattr(self, 'provider_queue', [])]
                    error.details.update(policy=BOUNDED_PROVIDER_POLICY, bounded_complete=False,
                        full_closure_complete=False, limits=PROVIDER_LIMITS,
                        elapsed_ms=round((time.monotonic() - self.started) * 1000),
                        cpu_ms=round((time.process_time() - self.cpu_started) * 1000),
                        progress=progress)
                    if len(json.dumps(error.details).encode()) > 60 * 1024:
                        error.details['progress'] = dict(evidence_truncated=True,
                            headers=progress.get('headers', 0), source_bytes=progress.get('source_bytes', 0),
                            nodes_recorded=len(progress.get('nodes', [])),
                            pending_paths_count=len(progress['pending_paths']))
                raise
            finally:
                # A read-only optional lookup never evicts another lane's mirror.
                # Failed fetches/assembly may leave unreachable objects; bound
                # retained bytes on both success and failure, under the lock.
                if self.request['operation'] not in OFFLINE_OPERATIONS:
                    mirrors = sorted(self.root.glob('*.git'), key=lambda p: p.stat().st_mtime)
                    total = sum(size(p) for p in mirrors)
                    for mirror in mirrors:
                        if total <= self.budget:
                            break
                        total -= size(mirror)
                        shutil.rmtree(mirror)


def compatibility_imports(content):
    """Conservative subset of Lean.Parser.Module.Syntax, never body commands.

    Lean has ONE module identifier per import command; whitespace/comments may
    span lines. Unsupported/escaped names or header prefixes mean unknown.
    """
    if content.startswith('\ufeff'):
        raise Failure('ambiguous_header')
    i, imports = 0, []

    def skip(index):
        while index < len(content):
            if content[index] in ' \t\r\n':
                index += 1
            elif content.startswith('--', index):
                end = content.find('\n', index)
                index = len(content) if end < 0 else end
            elif content.startswith('/-', index):
                index, depth = index + 2, 1
                while depth and index < len(content):
                    if content.startswith('/-', index):
                        index, depth = index + 2, depth + 1
                    elif content.startswith('-/', index):
                        index, depth = index + 2, depth - 1
                    else:
                        index += 1
                if depth:
                    raise Failure('ambiguous_header')
            else:
                break
        return index

    def word(index):
        match = re.match(r'[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*', content[index:])
        return match[0] if match else None

    i = skip(i)
    if word(i) == 'module':
        i = skip(i + len('module'))
    if word(i) == 'prelude':
        i = skip(i + len('prelude'))
    while True:
        start = i
        token = word(i)
        if token == 'public':
            i = skip(i + len(token))
            token = word(i)
        if token == 'meta':
            i = skip(i + len(token))
            token = word(i)
        if token != 'import':
            # private import / malformed keyword prefixes are outside this
            # subset. Body macros (including namespace-like arguments) cannot
            # become alleged imports. The trusted CI parser remains authority.
            first = word(start)
            if (first in ['module', 'prelude'] or token in ['public', 'meta']
                    or first == 'private' and word(skip(start + len('private'))) == 'import'
                    or first and any(first.startswith(k) and first != k
                                     for k in ['import', 'module', 'prelude', 'public', 'private', 'meta'])):
                raise Failure('ambiguous_header')
            return imports
        i = skip(i + len('import'))
        if word(i) == 'all':
            i = skip(i + len('all'))
        module = word(i)
        if module is None or len(module) > 512 or module[0].islower() or module in ['import', 'public', 'meta', 'private', 'module', 'prelude']:
            raise Failure('ambiguous_header')
        i += len(module)
        if i < len(content) and content[i] not in ' \t\r\n' and not content.startswith('/-', i) and not content.startswith('--', i):
            raise Failure('ambiguous_header')
        if module == 'TauCeti' or module.startswith('TauCeti.'):
            imports.append(module)
        i = skip(i)


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
    if request['operation'] not in ['assemble', 'publish', 'promote', 'compatibility', *OFFLINE_OPERATIONS]:
        raise Failure('invalid_operation')
    if request['operation'] in OFFLINE_OPERATIONS:
        binding, paths = request['binding'], request['paths']
        if (not isinstance(binding, dict)
                or any(type(binding.get(k)) is not int or binding[k] <= 0
                       for k in ['candidate_id', 'generation', 'run_id', 'run_attempt'])
                or not re.fullmatch(r'[0-9a-f]{64}', binding.get('members_digest', ''))
                or binding.get('workflow') != '.github/workflows/pr-build.yml'
                or not isinstance(binding.get('job_ids'), list)
                or not 1 <= len(binding['job_ids']) <= 2
                or any(type(j) is not int or j <= 0 for j in binding['job_ids'])
                or not isinstance(paths, list) or not 1 <= len(paths) <= 8
                or len(set(paths)) != len(paths)
                or any(not isinstance(p, str) or len(p) > 512
                       or not re.fullmatch(r"TauCeti(?:/[A-Za-z_][A-Za-z0-9_'-]*)+\.lean", p)
                       for p in paths)):
            raise Failure('invalid_provider_request')
        if request['operation'] == 'bounded_import_providers':
            changed = request.get('changed_paths')
            if (not isinstance(changed, list) or len(changed) > 1024
                    or any(not isinstance(p, str) or len(p) > 512
                           or not PROVIDER_PATH.fullmatch(p) for p in changed)
                    or changed != sorted(set(changed))):
                raise Failure('invalid_changed_frontier')
        if request['operation'] == 'collision_sources' and len(paths) != 2:
            raise Failure('invalid_collision_request')
        shas = [binding['commit'], binding['base_sha']]
    elif request['operation'] in ['assemble', 'compatibility']:
        if not request['members']:
            raise Failure('empty_members')
        limit = (ASSEMBLY_MEMBER_LIMIT if request['operation'] == 'assemble'
                 else COMPATIBILITY_MEMBER_LIMIT)
        if len(request['members']) > limit:
            raise Failure('member_limit', max_members=limit, members=len(request['members']))
        base = request['base_sha'] if request['operation'] == 'compatibility' else request['identity']['base_sha']
        shas = [base] + [m['head_sha'] for m in request['members']]
    else:
        shas = [request['commit'], request['expected']]
        if request['operation'] == 'promote':
            if request['ref'] != 'refs/heads/main':
                raise Failure('invalid_ref')
        elif not re.fullmatch(r'refs/heads/staging(?:-candidate-[1-9][0-9]*-[1-9][0-9]*)?', request['ref']):
            raise Failure('invalid_ref')
    if any(not SHA.fullmatch(sha) for sha in shas):
        raise Failure('invalid_sha')


def main():
    try:
        request = json.loads(Path(sys.argv[1]).read_text())
        validate(request)
        mirror = Mirror(request)
        result = mirror.run()
    except Failure as error:
        result = dict(status='error', details=error.details)
    except Exception:
        result = dict(status='error', details=dict(kind='helper_failed'))
    if (isinstance(locals().get('request'), dict)
            and request.get('operation') == 'bounded_import_providers'
            and result.get('status') == 'error'):
        result.update(complete=False, bounded_complete=False, full_closure_complete=False,
                      policy=BOUNDED_PROVIDER_POLICY, limits=PROVIDER_LIMITS,
                      authority='retry_only', proof_authority=False)
        if 'mirror' in locals():
            result.update(elapsed_ms=round((time.monotonic() - mirror.started) * 1000),
                          cpu_ms=round((time.process_time() - mirror.cpu_started) * 1000))
    print(json.dumps(result))


if __name__ == '__main__':
    main()
