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
