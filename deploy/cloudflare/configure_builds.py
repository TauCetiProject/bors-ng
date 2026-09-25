#!/usr/bin/env python3
"""Connect the bors Worker to its fork for Cloudflare-hosted builds.

The Cloudflare GitHub App must first be authorized for TauCetiProject/bors-ng.
The token is read from a private file so it is never passed on a command line.
This script deliberately does not trigger a deployment: runtime secrets and
the reviewed production branch must be ready before that step.
"""

import argparse
import json
import pathlib
import subprocess
import urllib.error
import urllib.request

ACCOUNT = "ec2169bdf033f56b009956d4b64ba8ef"
WORKER = "tauceti-bors"
OWNER = "TauCetiProject"
REPO = "bors-ng"
API = f"https://api.cloudflare.com/client/v4/accounts/{ACCOUNT}"


def github(path):
    return json.loads(subprocess.check_output(["gh", "api", path], text=True))


def cloudflare(token, path, *, method="GET", data=None):
    body = json.dumps(data).encode() if data is not None else None
    request = urllib.request.Request(
        API + path,
        data=body,
        method=method,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            result = json.load(response)
    except urllib.error.HTTPError as error:
        # An API error body is safe to show here; the token is never included.
        raise SystemExit(f"Cloudflare {method} {path}: HTTP {error.code}: "
                         f"{error.read().decode()[:1000]}") from error
    if not result.get("success"):
        raise SystemExit(f"Cloudflare {method} {path}: {result.get('errors')}")
    return result["result"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token-file", type=pathlib.Path, required=True)
    parser.add_argument("--branch", default="master",
                        help="reviewed production branch (default: master)")
    parser.add_argument("--apply", action="store_true",
                        help="create the repo connection and production trigger")
    args = parser.parse_args()
    token = args.token_file.read_text().strip()
    if not token:
        parser.error("token file is empty")

    owner_id = github(f"users/{OWNER}")["id"]
    repo_id = github(f"repos/{OWNER}/{REPO}")["id"]
    scripts = cloudflare(token, "/workers/scripts")
    worker = next((script for script in scripts if script["id"] == WORKER), None)
    if worker is None:
        raise SystemExit(f"Worker {WORKER} is missing from account {ACCOUNT}")
    worker_tag = worker["tag"]
    tokens = cloudflare(token, "/builds/tokens")
    triggers = cloudflare(token, f"/builds/workers/{worker_tag}/triggers")
    production = next((trigger for trigger in triggers
                       if args.branch in trigger.get("branch_includes", [])), None)
    if production:
        print(f"Production trigger already exists: {production['trigger_uuid']}")
        return
    if not tokens:
        raise SystemExit("No Workers build token exists. Create one in Worker Settings > Builds > API token.")
    if len(tokens) != 1:
        raise SystemExit("Several build tokens exist; select one in Worker Settings > Builds first.")

    print(f"Ready to connect {OWNER}/{REPO} ({repo_id}) to {WORKER} ({worker_tag})")
    print(f"Production branch: {args.branch}; build token: {tokens[0]['build_token_name']}")
    if not args.apply:
        print("Read-only check; pass --apply to create the connection and trigger.")
        return

    connection = cloudflare(token, "/builds/repos/connections", method="PUT", data={
        "provider_type": "github",
        "provider_account_id": str(owner_id),
        "provider_account_name": OWNER,
        "repo_id": str(repo_id),
        "repo_name": REPO,
    })
    trigger = cloudflare(token, "/builds/triggers", method="POST", data={
        "external_script_id": worker_tag,
        "repo_connection_uuid": connection["repo_connection_uuid"],
        "build_token_uuid": tokens[0]["build_token_uuid"],
        "trigger_name": "Deploy reviewed bors",
        "build_command": "npm ci",
        "deploy_command": "npx wrangler deploy",
        "root_directory": "/",
        "branch_includes": [args.branch],
        "branch_excludes": [],
        "path_includes": ["*"],
        "path_excludes": [],
    })
    print(f"Created production trigger {trigger['trigger_uuid']}; no build was started.")


if __name__ == "__main__":
    main()
