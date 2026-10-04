import test from "node:test";
import assert from "node:assert/strict";
import { archiveObservation, readObservations } from "./merge-observations.mjs";

test("archive uses an immutable timestamp, and pagination preserves continuation", async () => {
  let stored;
  const observation = { schema: "tauceti-merge.observation/v1", observed_at: "2026-10-04T12:00:00.000Z", backend: "queue" };
  const bucket = {
    async put(key, body) { stored = { key, body }; },
    async list(options) {
      assert.equal(options.prefix, "merge-observations/2026-10-04/");
      assert.equal(options.cursor, "page2");
      return { objects: [{ key: stored.key }], truncated: true, cursor: "page3" };
    },
    async get() { return { json: async () => JSON.parse(stored.body) }; },
  };
  await archiveObservation(observation, bucket);
  const response = await readObservations(new Request("https://bors/api/merge-observations?day=2026-10-04&cursor=page2"), bucket);
  assert.deepEqual(await response.json(), { observations: [observation], truncated: true, cursor: "page3" });
  assert.equal(response.headers.get("Cache-Control"), "no-store");
});

test("invalid dates and schemas cannot escape the measurement prefix", async () => {
  assert.equal((await readObservations(new Request("https://bors/api/merge-observations?day=../../deliveries"), {})).status, 400);
  await assert.rejects(archiveObservation({ schema: "bad", observed_at: "2026-10-04" }, {}));
});
