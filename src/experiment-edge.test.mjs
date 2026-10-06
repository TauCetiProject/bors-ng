import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {generateKeyPairSync, verify} from 'node:crypto';
import {Miniflare, convertV4MiniflareOptions, Response} from 'miniflare';

test('Workers runtime signs App requests, reads variables and refuses redirects', async () => {
  const {privateKey,publicKey}=generateKeyPairSync('rsa',{modulusLength:2048});
  const root=process.cwd();
  let redirect=false;
  let calls=0;
  const mf=new Miniflare(convertV4MiniflareOptions({workers:[{
    name:'experiment-edge-test',modulesRoot:root,compatibilityDate:'2026-09-25',
    modules:[{type:'ESModule',path:root+'/edge-test-entry.mjs',contents:`
      import {githubVariables} from './src/experiment-github.mjs';
      export default {async fetch(request,env) {
        try {return Response.json(await githubVariables(env).get('MERGE_BACKEND'));}
        catch(error) {return new Response(error.message,{status:502});}
      }};`},
      {type:'ESModule',path:root+'/src/experiment-github.mjs',contents:fs.readFileSync('src/experiment-github.mjs','utf8')}],
    bindings:{GITHUB_INTEGRATION_ID:'5172678',
      GITHUB_INTEGRATION_PEM:Buffer.from(privateKey.export({type:'pkcs1',format:'pem'})).toString('base64')},
    outboundService:async request=>{
      calls++;
      const url=new URL(request.url);
      assert.equal(url.host,'api.github.com');
      if(url.pathname.endsWith('/access_tokens')) {
        const jwt=request.headers.get('Authorization').slice(7);
        const [header,claims,signature]=jwt.split('.');
        assert.equal(JSON.parse(Buffer.from(claims,'base64url')).iss,'5172678');
        assert.ok(verify('RSA-SHA256',Buffer.from(`${header}.${claims}`),publicKey,Buffer.from(signature,'base64url')));
        assert.deepEqual(await request.json(),{repositories:['TauCeti'],permissions:{actions_variables:'write'}});
        return Response.json({token:'test-token',permissions:{actions_variables:'write'},
          expires_at:new Date(Date.now()+3600000).toISOString()});
      }
      assert.equal(request.headers.get('Authorization'),'Bearer test-token');
      if(redirect)return new Response(null,{status:302,headers:{Location:'https://other.example/steal'}});
      return Response.json({value:'queue',updated_at:'2026-10-05T20:00:00Z'});
    },
  }]}));
  try {
    const first=await mf.dispatchFetch('http://edge-test/');
    assert.equal(first.status,200); assert.equal((await first.json()).value,'queue');
    redirect=true;
    const second=await mf.dispatchFetch('http://edge-test/');
    assert.equal(second.status,502); assert.match(await second.text(),/unexpected redirect/);
    assert.equal(calls,3);
  } finally {await mf.dispose();}
});
