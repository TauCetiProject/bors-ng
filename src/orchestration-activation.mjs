// Container image rollout may retain an instance's old startup environment.
// Apply the async settings once, through the owning Durable Object lifecycle.
const KEY = 'bors-async-orchestration-activation';
const VERSION = 'outbox-v1';

export async function activateOrchestration(storage, container, wait = ms => new Promise(resolve => setTimeout(resolve, ms))) {
  if (await storage.get(KEY) === VERSION) return;
  if (container.running) {
    const stopped = container.monitor().catch(() => {});
    container.signal(15);
    await Promise.race([stopped, wait(10_000)]);
    if (container.running) await container.destroy();
  }
  // A failed restart must retry; do not acknowledge it before stopping.
  await storage.put(KEY, VERSION);
}
