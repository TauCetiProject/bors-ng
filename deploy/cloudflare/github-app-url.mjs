// Print a prefilled organization GitHub App registration URL. GitHub does not
// accept webhook secrets through URL parameters; set that in the App UI.
const url = new URL("https://github.com/organizations/TauCetiProject/settings/apps/new");
const values = {
  name: "TauCeti Bors",
  description: "Serial staging and bisecting merge queue for Tau Ceti",
  url: "https://bors.taucetiproject.org/",
  public: "false",
  webhook_active: "true",
  webhook_url: "https://bors.taucetiproject.org/webhook/github",
  contents: "write",
  issues: "write",
  pull_requests: "write",
  statuses: "write",
  checks: "write",
  members: "read",
};
for (const [name, value] of Object.entries(values)) url.searchParams.set(name, value);
url.searchParams.append("callback_urls[]", "https://bors.taucetiproject.org/auth/github/callback");
for (const event of ["repository", "status", "issue_comment", "pull_request",
  "pull_request_review", "pull_request_review_comment", "check_run", "check_suite",
  "team", "member", "membership", "organization"]) {
  url.searchParams.append("events[]", event);
}
process.stdout.write(`${url}\n`);
