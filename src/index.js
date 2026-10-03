import { Container, getContainer } from "@cloudflare/containers";
import { env as workerEnv } from "cloudflare:workers";
import { needsBors } from "./webhook-filter.mjs";

const INSTANCE = "singleton";
// JSON encodes a byte array at up to four characters per byte. Keep Queue
// messages below its 128 KB limit even for an unlucky payload.
const INLINE_LIMIT = 30_000;

export class BorsContainer extends Container {
  defaultPort = 4000;
  sleepAfter = "24h";

  envVars = {
    PORT: "4000",
    PUBLIC_HOST: "bors.taucetiproject.org",
    PUBLIC_PROTOCOL: "https",
    PUBLIC_PORT: "443",
    // The Container port serves HTTP behind Cloudflare's public TLS endpoint.
    FORCE_SSL: "false",
    DATABASE_AUTO_MIGRATE: "true",
    DATABASE_URL: workerEnv.DATABASE_URL,
    DATABASE_USE_SSL: "true",
    POOL_SIZE: "10",
    SECRET_KEY_BASE: workerEnv.SECRET_KEY_BASE,
    GITHUB_WEBHOOK_SECRET: workerEnv.GITHUB_WEBHOOK_SECRET,
    GITHUB_CLIENT_ID: workerEnv.GITHUB_CLIENT_ID,
    GITHUB_CLIENT_SECRET: workerEnv.GITHUB_CLIENT_SECRET,
    GITHUB_INTEGRATION_ID: workerEnv.GITHUB_INTEGRATION_ID,
    GITHUB_INTEGRATION_PEM: workerEnv.GITHUB_INTEGRATION_PEM,
    COMMAND_TRIGGER: "bors",
    BORS_STAGE_DISPATCH_PROJECT: "TauCetiProject/TauCeti",
    TAUCETI_REVIEW_APP_ID: "3947238",
  };
}

function hexBytes(hex) {
  if (!/^[a-f0-9]{64}$/i.test(hex)) return null;
  return Uint8Array.from(hex.match(/../g), (byte) => Number.parseInt(byte, 16));
}

async function validSignature(body, signature, secret) {
  if (!signature?.startsWith("sha256=") || !secret) return false;
  const expected = hexBytes(signature.slice(7));
  if (!expected) return false;
  const key = await crypto.subtle.importKey(
    "raw", new TextEncoder().encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["verify"]
  );
  return crypto.subtle.verify("HMAC", key, expected, body);
}

function container(env) {
  return getContainer(env.BORS, INSTANCE);
}

export default {
  async fetch(request, env) {
    const path = new URL(request.url).pathname;
    if (path !== "/webhook/github") {
      const url = new URL(request.url);
      const publicProtocol = url.protocol.slice(0, -1);
      url.protocol = "http:";
      const forwarded = new Request(url, request);
      forwarded.headers.set("x-forwarded-proto", publicProtocol);
      return container(env).fetch(forwarded);
    }
    if (request.method !== "POST") return new Response("Method not allowed", { status: 405 });

    const body = await request.arrayBuffer();
    const signature = request.headers.get("x-hub-signature-256");
    if (!(await validSignature(body, signature, env.GITHUB_WEBHOOK_SECRET))) {
      return new Response("Invalid signature", { status: 401 });
    }
    const delivery = request.headers.get("x-github-delivery");
    const event = request.headers.get("x-github-event");
    if (!delivery || !event) return new Response("Missing GitHub headers", { status: 400 });
    if (!needsBors(event, body)) return new Response("Accepted", { status: 202 });

    const item = { delivery, event, signature };
    if (body.byteLength <= INLINE_LIMIT) {
      item.body = Array.from(new Uint8Array(body));
    } else {
      item.object = `deliveries/${delivery}`;
      await env.WEBHOOK_BODIES.put(item.object, body);
    }
    await env.WEBHOOKS.send(item);
    return new Response("Accepted", { status: 202 });
  },

  async queue(batch, env) {
    const backend = container(env);
    for (const message of batch.messages) {
      const item = message.body;
      const processedKey = `processed/${item.delivery}`;
      let body;
      if (item.object) {
        const object = await env.WEBHOOK_BODIES.get(item.object);
        if (!object) throw new Error(`Missing webhook payload ${item.delivery}`);
        body = await object.arrayBuffer();
      } else {
        body = Uint8Array.from(item.body);
      }
      if (!needsBors(item.event, body)) {
        if (item.object) await env.WEBHOOK_BODIES.delete(item.object);
        message.ack();
        continue;
      }
      if (await env.WEBHOOK_BODIES.head(processedKey)) {
        message.ack();
        continue;
      }
      const response = await backend.fetch(new Request("http://bors/webhook/github", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-hub-signature-256": item.signature,
          "x-github-event": item.event,
          "x-github-delivery": item.delivery,
        },
        body,
      }));
      // Bors responds 404 for GitHub event types it does not use. Retrying
      // those deliveries only fills the queue with permanent failures.
      if (!response.ok && response.status !== 404) {
        throw new Error(`Bors webhook returned ${response.status}`);
      }
      await env.WEBHOOK_BODIES.put(processedKey, "1");
      if (item.object) await env.WEBHOOK_BODIES.delete(item.object);
      message.ack();
    }
  },

  async scheduled(_event, env) {
    const response = await container(env).fetch(new Request("http://bors/health/"));
    if (!response.ok) throw new Error(`Bors health returned ${response.status}`);
  },
};
