import assert from "node:assert/strict";
import test from "node:test";
import { needsBors } from "./webhook-filter.mjs";

const body = (payload) => new TextEncoder().encode(JSON.stringify(payload));

test("forwards only completed check runs on bors branches", () => {
  const check = (status, branch) => body({
    check_run: { status, check_suite: { head_branch: branch } },
  });
  assert.equal(needsBors("check_run", check("completed", "staging")), true);
  assert.equal(needsBors("check_run", check("completed", "trying.tmp")), true);
  assert.equal(needsBors("check_run", check("queued", "staging")), false);
  assert.equal(needsBors("check_run", check("completed", "main")), false);
  assert.equal(needsBors("check_run", check("completed", null)), false);
});

test("forwards only completed check suites on bors branches", () => {
  const suite = (action, branch) => body({ action, check_suite: { head_branch: branch } });
  assert.equal(needsBors("check_suite", suite("completed", "staging")), true);
  assert.equal(needsBors("check_suite", suite("requested", "staging")), false);
  assert.equal(needsBors("check_suite", suite("completed", "feature/one")), false);
});

test("preserves other and malformed events for bors", () => {
  assert.equal(needsBors("ping", body({})), true);
  assert.equal(needsBors("pull_request", body({})), true);
  assert.equal(needsBors("check_run", new TextEncoder().encode("{bad")), true);
});

test("eligibility notifications reach bors for fork heads and all decision transitions", () => {
  const payload = {
    repository: { full_name: "TauCetiProject/TauCeti" },
    action: "completed",
    check_run: { name: "merge eligibility", app: { id: 3947238 },
      status: "completed", conclusion: "success", pull_requests: [], check_suite: { head_branch: null } },
  };
  for (const conclusion of ["success", "neutral", "failure"]) {
    payload.check_run.conclusion = conclusion;
    assert.equal(needsBors("check_run", body(payload)), true);
  }
  payload.check_run.app.id = 7;
  assert.equal(needsBors("check_run", body(payload)), false);
  payload.check_run.app.id = 3947238;
  payload.repository.full_name = "another/repo";
  assert.equal(needsBors("check_run", body(payload)), false);
});
