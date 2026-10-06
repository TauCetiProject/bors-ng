// Restricted installation tokens for the existing App. No credentials leave the
// Worker; the controller can change only the two experiment-related variables.
const ROOT = "https://api.github.com";
const REPO = "TauCetiProject/TauCeti";
let cached;

function base64url(bytes) {
  return btoa(String.fromCharCode(...bytes)).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
}

function der(tag, bytes) {
  let size = bytes.length;
  const length = [];
  while (size) { length.unshift(size & 255); size >>>= 8; }
  return Uint8Array.from([tag, ...(bytes.length < 128 ? [bytes.length] : [128 + length.length, ...length]), ...bytes]);
}

export async function appJwt(env, now = Date.now()) {
  const pem = atob(env.GITHUB_INTEGRATION_PEM);
  let bytes = Uint8Array.from(atob(pem.replace(/-----[^\n]+-----|\s/g, "")), c => c.charCodeAt(0));
  if (pem.includes("BEGIN RSA PRIVATE KEY")) {
    // GitHub downloads PKCS#1; WebCrypto requires the PKCS#8 envelope.
    const algorithm = [0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01, 0x05, 0x00];
    bytes = der(0x30, Uint8Array.from([0x02, 0x01, 0x00, ...algorithm, ...der(0x04, bytes)]));
  } else if (!pem.includes("BEGIN PRIVATE KEY")) {
    throw new Error("Unsupported GitHub private key encoding");
  }
  const key = await crypto.subtle.importKey("pkcs8", bytes,
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" }, false, ["sign"]);
  const encode = value => base64url(new TextEncoder().encode(JSON.stringify(value)));
  const issued = Math.floor(now / 1000);
  const input = `${encode({ alg: "RS256", typ: "JWT" })}.${encode({ iat: issued - 60, exp: issued + 540, iss: String(env.GITHUB_INTEGRATION_ID) })}`;
  const signature = await crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, new TextEncoder().encode(input));
  return `${input}.${base64url(new Uint8Array(signature))}`;
}

async function request(path, token, method = "GET", body) {
  const response = await fetch(`${ROOT}${path}`, { method, redirect: "manual",
    signal: AbortSignal.timeout(5000), headers: { Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28",
      "User-Agent": "TauCetiMergeExperiment/1.0", "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body) });
  // Workers accepts follow/manual only. Never follow a redirect with an App token.
  if (response.status >= 300 && response.status < 400) {
    throw new Error(`GitHub experiment API ${method} ${path}: unexpected redirect`);
  }
  if (response.status === 404 && method === "GET") return null;
  if (!response.ok) throw new Error(`GitHub experiment API ${method} ${path}: HTTP ${response.status}`);
  return response.status === 204 ? null : response.json();
}

async function token(env) {
  if (cached && cached.expires > Date.now() + 60_000) return cached.value;
  const result = await request("/app/installations/167441900/access_tokens", await appJwt(env), "POST",
    { repositories: ["TauCeti"], permissions: { actions_variables: "write" } });
  if (!result?.token || result.permissions?.actions_variables !== "write") {
    throw new Error("Experiment requires Variables write on the existing bors installation");
  }
  cached = { value: result.token, expires: Date.parse(result.expires_at) };
  return cached.value;
}

export function githubVariables(env) {
  return {
    async get(name) {
      if (!["MERGE_BACKEND", "MERGE_EXPERIMENT"].includes(name)) throw new Error("Variable outside experiment scope");
      return request(`/repos/${REPO}/actions/variables/${name}`, await token(env));
    },
    async select(value, expected) {
      if (!["queue", "bors"].includes(value)) throw new Error("Invalid merge backend");
      const current = await this.get("MERGE_BACKEND");
      if (!current || current.value !== expected.value || current.updated_at !== expected.updated_at) {
        throw new Error("Backend changed before scheduled write; selection preserved");
      }
      await request(`/repos/${REPO}/actions/variables/MERGE_BACKEND`, await token(env), "PATCH",
        { name: "MERGE_BACKEND", value });
      return this.get("MERGE_BACKEND");
    },
  };
}
