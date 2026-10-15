#!/usr/bin/env python3
"""Reproduce the one demonstrated pair interaction using Git only (no builds).

Use --git-dir with an object store containing the three pinned commits. Output
is local diagnostic evidence, not an oracle for any other base/head combination.
Fetch missing objects from https://github.com/TauCetiProject/TauCeti.git into an
isolated bare repository if needed. Does not check out or alter refs/branches.
"""
import argparse
import json
import os
import subprocess

BASE = '4f1a681d10b8371c56bc49158b0481d18bd949f5'
A = 'af6446b5cc924497adda5b107e12c39745359b7b'
B = '4cb41effd279d6774e2b4ae43d4bea96f496edc7'


def reproduce(git_dir):
    env = dict(os.environ, GIT_AUTHOR_NAME='Replay', GIT_AUTHOR_EMAIL='replay@example.invalid',
               GIT_COMMITTER_NAME='Replay', GIT_COMMITTER_EMAIL='replay@example.invalid',
               GIT_AUTHOR_DATE='2026-10-15T00:00:00+00:00', GIT_COMMITTER_DATE='2026-10-15T00:00:00+00:00')

    def git(*args, input=None):
        return subprocess.run(['git', '--git-dir', git_dir, *args], input=input,
                              capture_output=True, text=True, env=env, check=False)

    for sha in [BASE, A, B]:
        git('cat-file', '-e', sha + '^{commit}').check_returncode()
    first = git('merge-tree', '--write-tree', BASE, A)
    second = git('merge-tree', '--write-tree', BASE, B)
    first.check_returncode()
    second.check_returncode()
    tree = first.stdout.splitlines()[0]
    synthetic = git('commit-tree', tree, '-p', BASE, '-p', A, input='Exact-head interaction replay\n')
    synthetic.check_returncode()
    pair = git('merge-tree', '--write-tree', synthetic.stdout.strip(), B)
    if pair.returncode != 1 or 'CONFLICT' not in pair.stdout:
        raise RuntimeError('Expected the pinned pair conflict, got: ' + pair.stdout + pair.stderr)
    return {'base': BASE, 'earlier_head': A, 'later_head': B,
            'individual_merges': [first.returncode, second.returncode],
            'pair_exit': pair.returncode, 'diagnostic': pair.stdout,
            'scope': 'Only this base and these heads. Separating the heads does not repair incompatibility after either lands.'}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--git-dir', required=True)
    a = p.parse_args()
    print(json.dumps(reproduce(a.git_dir), indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
