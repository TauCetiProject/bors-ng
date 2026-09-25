#!/usr/bin/env python3
"""Provision the single Neon Postgres project needed by Tau Ceti bors.

Create a billed Neon organization and an API key in the Neon console first.
The returned connection URI is written to a private local file, never stdout.
"""

import argparse
import json
import os
import pathlib
import urllib.error
import urllib.request

API = "https://console.neon.tech/api/v2"
NAME = "tauceti-bors"


def request(token, path, *, method="GET", data=None):
    body = json.dumps(data).encode() if data is not None else None
    req = urllib.request.Request(API + path, data=body, method=method, headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        # Avoid echoing an API response that may contain a connection string.
        raise SystemExit(f"Neon {method} {path}: HTTP {error.code}") from error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token-file", type=pathlib.Path, required=True)
    parser.add_argument("--database-url-file", type=pathlib.Path, required=True)
    parser.add_argument("--org-id", required=True,
                        help="the billed Neon Scale organization, never a personal project")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    token = args.token_file.read_text().strip()
    if not token:
        parser.error("token file is empty")
    if args.database_url_file.exists():
        raise SystemExit(f"Output already exists: {args.database_url_file}")
    if not args.database_url_file.parent.is_dir():
        raise SystemExit(f"Output directory does not exist: {args.database_url_file.parent}")
    organization = request(token, f"/organizations/{args.org_id}")
    organization = organization.get("organization", organization)
    if organization.get("id") != args.org_id or "scale" not in organization.get("plan", "").lower():
        raise SystemExit("Neon organization ID or Scale plan does not match; refusing to create project")
    query = "/projects?limit=100&search=" + NAME
    query += "&org_id=" + args.org_id
    existing = [project for project in request(token, query)["projects"]
                if project["name"] == NAME]
    if existing:
        raise SystemExit(f"Project {NAME} already exists ({existing[0]['id']}); "
                         "retrieve its direct connection URI rather than creating another")

    project = {
        "name": NAME,
        "region_id": "aws-us-east-1",
        "pg_version": 17,
        "history_retention_seconds": 30 * 24 * 3600,
        "default_endpoint_settings": {
            "autoscaling_limit_min_cu": 0.25,
            "autoscaling_limit_max_cu": 0.25,
            "suspend_timeout_seconds": -1,
        },
    }
    project["org_id"] = args.org_id
    print("Ready to create tauceti-bors in AWS us-east-1: PostgreSQL 17, "
          "0.25 CU fixed, always active, 30-day history.")
    if not args.apply:
        print("Read-only check; pass --apply to provision it.")
        return
    created = request(token, "/projects", method="POST", data={"project": project})
    connection = next((item["connection_uri"] for item in created["connection_uris"]
                       if "-pooler." not in item["connection_uri"]), None)
    if not connection:
        raise SystemExit("Project created, but no direct connection URI was returned; "
                         f"retrieve it from Neon project {created['project']['id']}")
    descriptor = os.open(args.database_url_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as output:
        output.write(connection + "\n")
    print(f"Created Neon project {created['project']['id']}; "
          f"direct connection URI saved to {args.database_url_file}")


if __name__ == "__main__":
    main()
