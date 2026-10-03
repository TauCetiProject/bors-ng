// Temporary setup page for GitHub's App Manifest flow. Deploy only while
// registering the organization-owned bors App; all other paths return 503.
const ORIGIN = "https://tauceti-bors.tauceti-ec2.workers.dev";
const MANIFEST = {
  name: "Tau Ceti Bors",
  url: "https://bors.taucetiproject.org/",
  description: "Serial staging and bisecting merge queue for Tau Ceti",
  hook_attributes: { url: "https://bors.taucetiproject.org/webhook/github", active: true },
  redirect_url: `${ORIGIN}/github-app/callback`,
  callback_urls: ["https://bors.taucetiproject.org/auth/github/callback"],
  public: false,
  default_permissions: {
    contents: "write", issues: "write", pull_requests: "write",
    statuses: "write", checks: "write", members: "read",
  },
  default_events: [
    "repository", "status", "issue_comment", "pull_request",
    "pull_request_review", "pull_request_review_comment", "check_run",
    "check_suite", "team", "member", "membership", "organization",
  ],
};

function page(body, status = 200, cookie = undefined) {
  const headers = {
    "content-type": "text/html; charset=utf-8",
    "cache-control": "no-store",
    "referrer-policy": "no-referrer",
    "x-content-type-options": "nosniff",
    "content-security-policy": "default-src 'none'; style-src 'unsafe-inline'; form-action https://github.com; base-uri 'none'",
  };
  if (cookie) headers["set-cookie"] = cookie;
  return new Response(`<!doctype html><meta charset="utf-8"><title>Tau Ceti Bors setup</title>
    <style>body{font:16px system-ui;max-width:42rem;margin:4rem auto;line-height:1.5;padding:0 1rem}
    button{font:inherit;padding:.7rem 1rem}code{overflow-wrap:anywhere}</style>${body}`, { status, headers });
}

export default {
  fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/github-app/setup" && request.method === "GET") {
      const state = crypto.randomUUID();
      const action = new URL("https://github.com/organizations/TauCetiProject/settings/apps/new");
      action.searchParams.set("state", state);
      const manifest = JSON.stringify(MANIFEST).replaceAll("&", "&amp;").replaceAll('"', "&quot;");
      return page(`<h1>Create Tau Ceti Bors</h1><p>This registers a private GitHub App under
        TauCetiProject. GitHub will generate its credentials and return a one-time code.</p>
        <form method="post" action="${action}"><input type="hidden" name="manifest"
        value="${manifest}"><button>Create GitHub App</button></form>`, 200,
      `bors_setup_state=${state}; Secure; HttpOnly; SameSite=Lax; Max-Age=3600; Path=/github-app/callback`);
    }
    if (url.pathname === "/github-app/callback" && request.method === "GET") {
      const state = request.headers.get("cookie")?.match(/(?:^|;\s*)bors_setup_state=([^;]+)/)?.[1];
      const code = url.searchParams.get("code");
      if (!state || state !== url.searchParams.get("state") || !/^[a-z0-9]{40}$/i.test(code || "")) {
        return page("<h1>Invalid setup callback</h1>", 400);
      }
      return page(`<h1>App registered</h1><p>Send this one-time code to Codex within one hour.
        It will exchange the code for the App credentials and store them locally.</p>
        <p><code>${code}</code></p>`);
    }
    return new Response("Bors is not deployed", { status: 503 });
  },
};
