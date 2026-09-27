#!/usr/bin/env python3
"""Upload bors runtime secrets from local private files to its existing Worker.

Run from the repository root. Secret values travel to Wrangler over stdin,
never through command arguments, logs, or a temporary JSON file.
"""

import argparse
import base64
import json
import os
import pathlib
import secrets
import subprocess
import urllib.parse


def read(path):
    value = path.read_text().strip()
    if not value:
        raise SystemExit(f"Empty secret file: {path}")
    return value


def key_base(path):
    if not path.exists():
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as output:
            output.write(base64.b64encode(secrets.token_bytes(64)).decode() + "\n")
    return read(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url-file", type=pathlib.Path, required=True)
    parser.add_argument("--webhook-secret-file", type=pathlib.Path, required=True)
    parser.add_argument("--github-client-id", required=True)
    parser.add_argument("--github-client-secret-file", type=pathlib.Path, required=True)
    parser.add_argument("--github-app-id", required=True)
    parser.add_argument("--github-private-key-file", type=pathlib.Path, required=True)
    parser.add_argument("--secret-key-base-file", type=pathlib.Path, required=True)
    args = parser.parse_args()

    database_url = read(args.database_url_file)
    parsed = urllib.parse.urlsplit(database_url)
    if parsed.scheme not in ("postgresql", "postgres") or not parsed.hostname:
        parser.error("database URL must be a PostgreSQL URI")
    if "-pooler." in parsed.hostname:
        parser.error("use Neon's direct endpoint, not its transaction pooler")
    # Neon emits libpq-specific URL parameters. Ecto/Postgrex receives TLS
    # through DATABASE_USE_SSL=true in src/index.js, so do not pass those
    # parameters to an Elixir driver that does not interpret them as libpq.
    params = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
    if params and (params.get("sslmode") != ["require"] or
                   set(params) - {"sslmode", "channel_binding"} or
                   params.get("channel_binding", ["require"]) != ["require"]):
        parser.error("database URL has unsupported query parameters")
    database_url = parsed._replace(query="", fragment="").geturl()
    pem = args.github_private_key_file.read_bytes()
    if b"-----BEGIN RSA PRIVATE KEY-----" not in pem and b"-----BEGIN PRIVATE KEY-----" not in pem:
        parser.error("GitHub private key file is not PEM")
    values = {
        "DATABASE_URL": database_url,
        "SECRET_KEY_BASE": key_base(args.secret_key_base_file),
        "GITHUB_WEBHOOK_SECRET": read(args.webhook_secret_file),
        "GITHUB_CLIENT_ID": args.github_client_id,
        "GITHUB_CLIENT_SECRET": read(args.github_client_secret_file),
        "GITHUB_INTEGRATION_ID": args.github_app_id,
        "GITHUB_INTEGRATION_PEM": base64.b64encode(pem).decode(),
    }
    result = subprocess.run(
        ["npx", "wrangler", "secret", "bulk", "--name", "tauceti-bors"],
        input=json.dumps(values), text=True, capture_output=True, check=False,
    )
    if result.returncode:
        raise SystemExit(f"Wrangler secret upload failed (exit {result.returncode}); "
                         "output withheld because it may contain secrets")
    print("Uploaded seven runtime secrets to tauceti-bors.")


if __name__ == "__main__":
    main()
