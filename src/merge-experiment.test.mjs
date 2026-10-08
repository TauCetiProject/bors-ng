import test from 'node:test';
import assert from 'node:assert/strict';
import { tickExperiment } from './merge-experiment.mjs';

const HOUR = 3600000;
const start = Date.parse('2026-10-05T23:00:00Z');
const iso = t => new Date(t).toISOString();
function fixture(mode) {
  const data = new Map();
  let now = start;
  let backend = { value: 'queue', updated_at: iso(start - HOUR) };
  let plan = { schema: 'tauceti-merge.experiment/v1', id: 'test', duration_hours: 24, created_at: iso(start) };
  if (mode !== undefined) plan.mode = mode;
  let loseResponse = false;
  const writes = [];
  const storage = { async get(k) { return structuredClone(data.get(k)); }, async put(k, v) { data.set(k, structuredClone(v)); } };
  const variables = {
    async get(name) { return structuredClone(name === 'MERGE_BACKEND' ? backend : plan && {value: JSON.stringify(plan)}); },
    async select(value, expected) {
      assert.deepEqual(expected, backend);
      backend = {value, updated_at: iso(now)};
      writes.push(value);
      if (loseResponse) { loseResponse = false; throw new Error('response lost'); }
      return structuredClone(backend);
    }
  };
  return { data, storage, variables, writes,
    set time(t) { now = t; }, get backend() { return backend; }, set backend(v) { backend = v; },
    set plan(v) { plan = v; }, lose() { loseResponse = true; },
    observe(github_count = 0, bors_count = 0, at = now) {
      data.set('merge-experiment-observation', {schema:'tauceti-merge.observation/v1',
        backend:backend.value, updated_at:backend.updated_at, observed_at:iso(at), github_count, bors_count});
    }, async tick() { return tickExperiment(storage, variables, now); }
  };
}

test('two full days measured after drains, then remains on queue without restarting', async () => {
  const f = fixture();
  assert.equal((await f.tick()).phase, 'draining_to_bors');
  f.time = start + HOUR; f.observe(5, 0);
  assert.equal((await f.tick()).phase, 'draining_to_bors');
  f.time = start + 2*HOUR; f.observe();
  assert.equal((await f.tick()).bors_started_at, iso(start + 2*HOUR));
  f.time = start + 26*HOUR;
  assert.equal((await f.tick()).phase, 'draining_to_queue');
  f.time = start + 27*HOUR; f.observe(0, 1);
  assert.equal((await f.tick()).phase, 'draining_to_queue');
  f.time = start + 28*HOUR; f.observe();
  assert.equal((await f.tick()).queue_started_at, iso(start + 28*HOUR));
  f.time = start + 52*HOUR;
  assert.equal((await f.tick()).phase, 'complete');
  assert.equal((await f.tick()).phase, 'complete');
  assert.deepEqual(f.writes, ['bors', 'queue']);
});

test('bors-only measures a full day after drainage and stays on bors after completion', async () => {
  const f = fixture('bors_only');
  assert.equal((await f.tick()).phase, 'draining_to_bors');
  f.time = start + HOUR; f.observe(5, 0);
  assert.equal((await f.tick()).phase, 'draining_to_bors');
  f.time = start + 2*HOUR; f.observe();
  assert.equal((await f.tick()).bors_started_at, iso(start + 2*HOUR));
  f.time = start + 26*HOUR - 1;
  assert.equal((await f.tick()).phase, 'bors');
  f.data.delete('merge-experiment-observation'); f.time = start + 26*HOUR;
  const done = await f.tick();
  assert.equal(done.phase, 'complete');
  assert.equal(done.bors_ended_at, iso(start + 26*HOUR));
  assert.equal(done.finished_at, done.bors_ended_at);
  assert.equal(done.queue_requested_at, undefined);
  f.time = start + 80*HOUR;
  assert.deepEqual(await f.tick(), done);
  assert.equal(f.backend.value, 'bors');
  assert.deepEqual(f.writes, ['bors']);
});
test('bors-only aborts preserve bors on disabled plans and stalled handoffs', async () => {
  const disabled = fixture('bors_only'); await disabled.tick();
  disabled.plan = {enabled:false}; disabled.time = start + 60000;
  assert.equal((await disabled.tick()).phase, 'aborted');
  assert.equal(disabled.backend.value, 'bors'); assert.deepEqual(disabled.writes, ['bors']);
  const stalled = fixture('bors_only'); await stalled.tick(); stalled.time = start + 6*HOUR;
  assert.equal((await stalled.tick()).phase, 'aborted');
  assert.equal(stalled.backend.value, 'bors'); assert.deepEqual(stalled.writes, ['bors']);
});
test('bors-only overlap abort stops measurement without changing backend', async () => {
  const f = fixture('bors_only'); await f.tick();
  f.time = start + 60000; f.observe(1, 1); await f.tick();
  f.time = start + 120000; f.observe(1, 1);
  const state = await f.tick();
  assert.equal(state.phase, 'aborted'); assert.match(state.reason, /overlap/);
  assert.equal(f.backend.value, 'bors'); assert.deepEqual(f.writes, ['bors']);
});
test('bors-only respects a manual backend change', async () => {
  const f = fixture('bors_only'); await f.tick();
  f.time = start + 60000; f.backend = {value:'queue', updated_at:iso(start + 60000)};
  assert.equal((await f.tick()).phase, 'aborted');
  assert.equal(f.backend.value, 'queue'); assert.deepEqual(f.writes, ['bors']);
});
test('invalid measurement mode is refused without selecting a backend', async () => {
  const f = fixture('bors_onyl');
  await assert.rejects(f.tick(), /Invalid experiment plan/);
  assert.deepEqual(f.writes, []);
});
test('stale or pre-switch observations cannot start a measurement window', async () => {
  const f = fixture(); f.observe(); await f.tick();
  assert.equal((await f.tick()).phase, 'draining_to_bors');
  f.time = start + HOUR; f.observe(0, 0, start);
  assert.equal((await f.tick()).phase, 'draining_to_bors');
});
test('deadline restoration does not require healthy container or fresh observations', async () => {
  const f = fixture(); await f.tick(); f.time = start + 60000; f.observe(); await f.tick();
  f.data.delete('merge-experiment-observation'); f.time = start + 24*HOUR + 60000;
  assert.equal((await f.tick()).phase, 'draining_to_queue');
  assert.equal(f.backend.value, 'queue');
});
test('manual change, including same-value rewrite, ends automation and preserves operator selection', async () => {
  for (const value of ['queue', 'bors']) {
    const f = fixture(); await f.tick();
    f.backend = {value, updated_at:iso(start + 60000)}; f.time = start + 60000;
    assert.equal((await f.tick()).phase, 'aborted');
    assert.equal(f.backend.value, value); assert.deepEqual(f.writes, ['bors']);
  }
});
test('disabled plan restores queue; lost restore response recovers and finalizes abort', async () => {
  const f = fixture(); await f.tick(); f.plan = {enabled:false}; f.time = start + 60000; f.lose();
  await assert.rejects(f.tick(), /response lost/);
  assert.equal(f.backend.value, 'queue');
  assert.equal((await f.tick()).phase, 'aborted');
  assert.deepEqual(f.writes, ['bors', 'queue']);
});
test('lost initial response is adopted from durable intent after restart', async () => {
  const f = fixture(); f.lose(); await assert.rejects(f.tick(), /response lost/);
  f.time = start + 60000; f.observe();
  assert.equal((await f.tick()).phase, 'bors'); assert.deepEqual(f.writes, ['bors']);
});
test('failed write before mutation is retried from durable intent', async () => {
  const f = fixture(); const select = f.variables.select;
  f.variables.select = async () => {throw new Error('offline');};
  await assert.rejects(f.tick(), /offline/);
  f.variables.select = select; f.time = start + 60000;
  assert.equal((await f.tick()).phase, 'draining_to_bors'); assert.equal(f.backend.value, 'bors');
});
test('stale plan never switches, and six-hour stalled drain restores queue', async () => {
  const stale = fixture(); stale.time = start + HOUR;
  assert.equal((await stale.tick()).phase, 'refused'); assert.deepEqual(stale.writes, []);
  const f = fixture(); await f.tick(); f.time = start + 6*HOUR;
  assert.equal((await f.tick()).phase, 'aborted'); assert.equal(f.backend.value, 'queue');
});
test('one repeated overlap sample does not abort; two distinct fresh samples do', async () => {
  const f = fixture(); await f.tick(); f.time = start + 60000; f.observe(1, 1);
  assert.equal((await f.tick()).phase, 'draining_to_bors');
  assert.equal((await f.tick()).phase, 'draining_to_bors');
  f.time = start + 120000; f.observe(1, 1);
  assert.equal((await f.tick()).phase, 'aborted'); assert.equal(f.backend.value, 'queue');
});
test('malformed edit cancels but transient plan read failure leaves durable state for retry', async () => {
  const f = fixture(); await f.tick();
  const get = f.variables.get;
  f.variables.get = async name => { if(name === 'MERGE_EXPERIMENT') throw new Error('offline'); return get(name); };
  await assert.rejects(f.tick(), /offline/); assert.equal(f.backend.value, 'bors');
  f.variables.get = async name => name === 'MERGE_EXPERIMENT' ? {value:'{invalid'} : get(name);
  f.time = start + 60000;
  assert.equal((await f.tick()).phase, 'aborted'); assert.equal(f.backend.value, 'queue');
});
