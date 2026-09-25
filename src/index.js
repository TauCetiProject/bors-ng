import { Container, getContainer } from "@cloudflare/containers";

const INSTANCE = "singleton";
// JSON encodes a byte array at up to four characters per byte. Keep Queue
// messages below its 128 KB limit even for an unlucky payload.
const INLINE_LIMIT = 30_000;

export class BorsContainer extends Container {
  defaultPort = 4000;
  sleepAfter = "24h";

  get envVars() {
    const env = this.env;
    return {
      PORT: "4000",
      PUBLIC_HOST: "bors.taucetiproject.org",
      PUBLIC_PROTOCOL: "https",
      PUBLIC_PORT: "443",
      DATABASE_AUTO_MIGRATE: "true",
      DATABASE_URL: env.DATABASE_URL,
      DATABASE_USE_SSL: "true",
      POOL_SIZE: "10",
      SECRET_KEY_BASE: env.SECRET_KEY_BASE,
      GITHUB_WEBHOOK_SECRET: env.GITHUB_WEBHOOK_SECRET,
      GITHUB_CLIENT_ID: env.GITHUB_CLIENT_ID,
      GITHUB_CLIENT_SECRET: env.GITHUB_CLIENT_SECRET,
      GITHUB_INTEGRATION_ID: env.GITHUB_INTEGRATION_ID,
      GITHUB_INTEGRATION_PEM: env.GITHUB_INTEGRATION_PEM,
      COMMAND_TRIGGER: "bors",
      BORS_STAGE_DISPATCH_PROJECT: "TauCetiProject/TauCeti",
      TAUCETI_REVIEW_APP_ID: "3947238",
    };
  }
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
    if (path !== "/webhook/github") return container(env).fetch(request);
    if (request.method !== "POST") return new Response("Method not allowed", { status: 405 });

    const body = await request.arrayBuffer();
    const signature = request.headers.get("x-hub-signature-256");
    if (!(await validSignature(body, signature, env.GITHUB_WEBHOOK_SECRET))) {
      return new Response("Invalid signature", { status: 401 });
    }
    const delivery = request.headers.get("x-github-delivery");
    const event = request.headers.get("x-github-event");
    if (!delivery || !event) return new Response("Missing GitHub headers", { status: 400 });

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
      if (await env.WEBHOOK_BODIES.head(processedKey)) {
        message.ack();
        continue;
      }
      let body;
      if (item.object) {
        const object = await env.WEBHOOK_BODIES.get(item.object);
        if (!object) throw new Error(`Missing webhook payload ${item.delivery}`);
        body = await object.arrayBuffer();
      } else {
        body = Uint8Array.from(item.body);
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
      if (!response.ok) throw new Error(`Bors webhook returned ${response.status}`);
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
