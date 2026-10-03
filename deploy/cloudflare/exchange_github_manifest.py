#!/usr/bin/env python3
"""Exchange a GitHub App manifest code and save its credentials privately.

The one-time code is read from stdin. No credential or code is printed.
"""

import json
import os
import pathlib
import re
import subprocess
import sys


OUTPUT = pathlib.Path.home() / ".config/tauceti-bors"


def save(name, value):
    destination = OUTPUT / name
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as output:
        output.write(value)
        if not value.endswith("\n"):
            output.write("\n")


def main():
    if (OUTPUT / "github-app.json").exists():
        raise SystemExit("App credentials already saved; refusing to overwrite them")
    recovery = OUTPUT / "github-app-credentials.json"
    if recovery.exists():
        app = json.loads(recovery.read_text())
    else:
        code = sys.stdin.readline().strip()
        if not re.fullmatch(r"[a-fA-F0-9]{40}", code):
            raise SystemExit("Expected a 40-character GitHub manifest code on stdin")
        result = subprocess.run(
            ["gh", "api", "-X", "POST", f"app-manifests/{code}/conversions"],
            text=True, capture_output=True, check=False,
        )
        if result.returncode:
            raise SystemExit(f"GitHub manifest exchange failed (exit {result.returncode}); "
                             "response withheld because it may contain credentials")
        app = json.loads(result.stdout)
    if app.get("owner", {}).get("login") != "TauCetiProject":
        raise SystemExit("GitHub returned an App outside TauCetiProject")
    if not all(app.get(key) for key in ("id", "slug", "client_id", "client_secret",
                                       "webhook_secret", "pem")):
        raise SystemExit("GitHub did not return all required App credentials")
    if not app["pem"].startswith("-----BEGIN "):
        raise SystemExit("GitHub returned an invalid private key")
    OUTPUT.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not recovery.exists():
        save("github-app-credentials.json", result.stdout)
    save("github-private-key.pem", app["pem"])
    save("github-client-secret", app["client_secret"])
    # The manifest-generated secret supersedes the unused locally generated one.
    save("github-webhook-secret", app["webhook_secret"])
    save("github-app.json", json.dumps({
        "id": app["id"], "slug": app["slug"], "client_id": app["client_id"],
        "owner": app["owner"]["login"],
    }, indent=2))
    print(f"Saved TauCetiProject GitHub App {app['slug']} ({app['id']}) credentials to {OUTPUT}")
    print(f"Install it for TauCeti only: https://github.com/apps/{app['slug']}/installations/new")


if __name__ == "__main__":
    main()
