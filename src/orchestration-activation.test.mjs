import test from 'node:test';
import assert from 'node:assert/strict';
import {activateOrchestration} from './orchestration-activation.mjs';

function state() {
  const data = new Map();
  return {get: async key => data.get(key), put: async (key, value) => data.set(key, value)};
}

test('activation stops a running old environment once before acknowledging the marker', async () => {
  const storage = state();
  const calls = [];
  let stopped;
  const container = {running: true, monitor: () => new Promise(resolve => {stopped = resolve;}),
    signal: signal => {calls.push(signal); container.running = false; stopped();},
    destroy: async () => calls.push('destroy')};
  await activateOrchestration(storage, container, async () => {});
  container.running = true;
  await activateOrchestration(storage, container, async () => {});
  assert.deepEqual(calls, [15]);
});

test('a fresh stopped container needs no restart', async () => {
  const storage = state();
  const container = {running: false};
  await activateOrchestration(storage, container);
  container.running = true;
  await activateOrchestration(storage, container);
});

test('bounded graceful shutdown falls back to destroy and retries failed shutdown', async () => {
  const storage = state();
  let signals = 0;
  let destroys = 0;
  let fail = true;
  const container = {running: true, monitor: () => new Promise(() => {}), signal: () => signals++,
    destroy: async () => {destroys++; if (fail) throw new Error('unavailable'); container.running = false;}};
  await assert.rejects(activateOrchestration(storage, container, async ms => assert.equal(ms, 10_000)), /unavailable/);
  fail = false;
  await activateOrchestration(storage, container, async () => {});
  assert.equal(signals, 2);
  assert.equal(destroys, 2);
  container.running = true;
  await activateOrchestration(storage, container, async () => {});
  assert.equal(signals, 2);
});
