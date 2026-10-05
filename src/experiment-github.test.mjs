import test from 'node:test';
import assert from 'node:assert/strict';
import {generateKeyPairSync, verify} from 'node:crypto';
import {appJwt, githubVariables} from './experiment-github.mjs';
const {privateKey, publicKey} = generateKeyPairSync('rsa', {modulusLength:2048});
const environment = type => ({GITHUB_INTEGRATION_ID:'5172678',
  GITHUB_INTEGRATION_PEM:Buffer.from(privateKey.export({type,format:'pem'})).toString('base64')});
for (const type of ['pkcs1', 'pkcs8']) test(`signs verifiable GitHub JWT from ${type}`, async () => {
  const now = 1760000000000;
  const jwt = await appJwt(environment(type), now);
  const [header, claims, signature] = jwt.split('.');
  assert.deepEqual(JSON.parse(Buffer.from(claims,'base64url')), {iat:1759999940,exp:1760000540,iss:'5172678'});
  assert.equal(verify('RSA-SHA256',Buffer.from(`${header}.${claims}`),publicKey,Buffer.from(signature,'base64url')),true);
});
test('installation token is scoped and writes recheck live backend', async () => {
  const original = globalThis.fetch;
  let backend = {value:'queue',updated_at:'2026-10-05T20:00:00Z'};
  const calls = [];
  globalThis.fetch = async (url, options) => {
    calls.push({url, options});
    assert.equal(options.redirect,'error');
    if (url.endsWith('/access_tokens')) {
      assert.deepEqual(JSON.parse(options.body),{repositories:['TauCeti'],permissions:{actions_variables:'write'}});
      return Response.json({token:'test-only-token',expires_at:new Date(Date.now()+3600000).toISOString(),permissions:{actions_variables:'write'}});
    }
    if (options.method === 'PATCH') {
      backend={value:JSON.parse(options.body).value,updated_at:'2026-10-05T23:00:00Z'};
      return new Response(null,{status:204});
    }
    return Response.json(backend);
  };
  try {
    const variables = githubVariables(environment('pkcs1'));
    await assert.rejects(variables.get('SECRET'),/outside experiment scope/);
    await assert.rejects(variables.select('invalid',backend),/Invalid merge backend/);
    const selected = await variables.select('bors', {...backend});
    assert.equal(selected.value,'bors');
    await assert.rejects(variables.select('queue',{value:'queue',updated_at:'old'}),/selection preserved/);
    assert.equal(calls.filter(c=>c.options.method==='PATCH').length,1);
  } finally {globalThis.fetch=original;}
});
