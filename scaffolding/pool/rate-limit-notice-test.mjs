// Verifies the api-server surfaces an upstream rate-limit MEANINGFULLY instead of
// a generic api_error / "empty response N times":
//   1. A run that errors with resource_exhausted → SSE `rate_limit_error` event
//      with a clear message (not api_error).
//   2. After a model was rate-limited, an empty turn on the SAME model → a
//      "rate-limited upstream" notice AND the empty-turn retries are SKIPPED.
//   3. Control: an empty turn with NO prior rate-limit → normal retries fire +
//      the enriched "empty response" notice (regression guard).
import net from 'node:net';
import http from 'node:http';
import { spawn } from 'node:child_process';
import path from 'node:path';
import os from 'node:os';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.join(HERE, '..', '..');
const PORT = 14655;
const SOCK = path.join(os.tmpdir(), `ratlc-rl-${process.pid}.sock`);
let fail = 0; const a = (n, c, d) => { console.log((c ? '  ✓ ' : '  ✗ ') + n + (c ? '' : ` — ${d}`)); if (!c) fail++; };

// Mock pool. Per-model behavior; tracks hits per reqId so we can prove retries
// fired (or didn't). Retries replay the same reqId.
const hitsByReqId = new Map();
const firstReqForModel = new Map(); // model → the reqId that should ERROR (rl-then-empty)
function startMockPool() {
  try { fs.unlinkSync(SOCK); } catch {}
  const server = net.createServer((conn) => {
    let buf = '';
    conn.on('data', (chunk) => {
      buf += chunk.toString('utf8'); let i;
      while ((i = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, i); buf = buf.slice(i + 1); if (!line.trim()) continue;
        let msg; try { msg = JSON.parse(line); } catch { continue; }
        if (msg.type !== 'request') continue;
        const reqId = msg.requestId; const model = msg.model || 'm';
        hitsByReqId.set(reqId, (hitsByReqId.get(reqId) || 0) + 1);
        const send = (o) => { try { conn.write(JSON.stringify({ ...o, requestId: reqId }) + '\n'); } catch {} };
        send({ type: 'route_decision', channelId: 'ch-1', servedModel: model, fallback: false });
        if (model === 'rl-error') {
          send({ type: 'error', message: "Connect error resource_exhausted: You've reached the rate limit. Please wait a bit" });
        } else if (model === 'rl-then-empty') {
          if (!firstReqForModel.has(model)) firstReqForModel.set(model, reqId);
          if (firstReqForModel.get(model) === reqId) send({ type: 'error', message: 'Connect error resource_exhausted: rate limit' });
          else send({ type: 'yield' }); // empty turn on a now-known-rate-limited model
        } else { // 'plain-empty' control
          send({ type: 'yield' });
        }
      }
    });
    conn.on('error', () => {});
  });
  return new Promise((r) => server.listen(SOCK, () => r(server)));
}
function startApi() {
  const env = { ...process.env, POOL_SOCK: SOCK, RATLC_API_PORT: String(PORT), RATLC_API_HOST: '127.0.0.1',
    RATLC_RETRY_EMPTY_TURN_MAX: '3', RATLC_RETRY_UPSTREAM_SILENT_MAX: '0', RATLC_RETRY_UPSTREAM_ABORT_MAX: '0',
    RATLC_KEEPALIVE_PING_MS: '0', POOL_REINJECT_THINKING: '0', RATLC_STATS_DISABLE: '1' };
  const proc = spawn(process.execPath, [path.join(HERE, 'api-server.mjs')], { cwd: REPO_ROOT, env, stdio: ['ignore', 'pipe', 'pipe'] });
  return new Promise((resolve, reject) => {
    let out = ''; const on = (d) => { out += d; if (/api-server listening/.test(out)) { clearTimeout(to); resolve(proc); } };
    const to = setTimeout(() => reject(new Error('api start timeout:\n' + out)), 8000);
    proc.stdout.on('data', on); proc.stderr.on('data', on);
  });
}
function ask(model) {
  return new Promise((resolve) => {
    const body = JSON.stringify({ model, max_tokens: 100, stream: true, messages: [{ role: 'user', content: 'hi' }] });
    const req = http.request({ host: '127.0.0.1', port: PORT, path: '/v1/messages', method: 'POST', headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) } }, (res) => {
      let sse = ''; res.on('data', (c) => sse += c); res.on('end', () => resolve(sse));
    });
    req.on('error', () => resolve('')); req.write(body); req.end();
    setTimeout(() => { try { req.destroy(); } catch {} resolve(''); }, 20000);
  });
}

const pool = await startMockPool();
const api = await startApi();
try {
  // 1. run errors with rate-limit → rate_limit_error event.
  console.log('=== 1. run errors with resource_exhausted ===');
  const r1 = await ask('rl-error');
  a('emits a proper rate_limit_error (not api_error)', /"type":"rate_limit_error"/.test(r1) && !/"type":"api_error"/.test(r1), r1.slice(0, 200));
  a('message names the upstream Cursor rate limit + the model', /Upstream Cursor rate limit for model rl-error/.test(r1), r1.slice(0, 200));

  // 2. rate-limit THEN empty turn on the same model → rate-limit notice + no retries.
  console.log('=== 2. empty turn on a just-rate-limited model ===');
  const r2a = await ask('rl-then-empty');            // errors → notes the model
  a('first req (error) emits rate_limit_error', /"type":"rate_limit_error"/.test(r2a), r2a.slice(0, 120));
  const r2b = await ask('rl-then-empty');            // empty turn → should read as rate-limit
  const emptyReqId = [...hitsByReqId.keys()].find((k) => firstReqForModel.get('rl-then-empty') !== k && hitsByReqId.get(k) >= 1 && k !== undefined);
  a('empty turn reports it as rate-limited upstream (not "empty response")', /rate-limited upstream by Cursor/.test(r2b) && !/empty response \d+ times/.test(r2b), r2b.slice(0, 220));
  // the 2nd (empty) request's reqId should have been hit exactly once → retries skipped.
  const secondReqHits = Math.max(...[...hitsByReqId.entries()].filter(([k]) => k !== firstReqForModel.get('rl-then-empty')).map(([, v]) => v), 0);
  a('empty-turn retries SKIPPED for a rate-limited model (1 pool hit, not 4)', secondReqHits === 1, `maxHits=${secondReqHits}`);

  // 3. Control: plain empty turn (never rate-limited) → retries fire + enriched notice.
  console.log('=== 3. control: plain empty turn (no rate-limit) ===');
  hitsByReqId.clear();
  const r3 = await ask('plain-empty');
  const plainHits = Math.max(...[...hitsByReqId.values()], 0);
  a('plain empty turn DID retry (4 pool hits = 1 + 3)', plainHits === 4, `hits=${plainHits}`);
  a('exhaustion notice is the enriched empty-response message', /empty response 4 times/.test(r3) && /rate-limited or not provisioned/.test(r3), r3.slice(0, 260));
  a('plain empty does NOT falsely claim rate-limited-upstream', !/rate-limited upstream by Cursor/.test(r3), 'false positive');
} catch (e) { console.log('  ✗ harness error:', e.message); fail++; }
try { api.kill('SIGKILL'); } catch {}
await new Promise((r) => pool.close(r));
try { fs.unlinkSync(SOCK); } catch {}
console.log(fail === 0 ? '\nrate-limit-notice-test: OK' : `\nrate-limit-notice-test: FAIL (${fail})`);
process.exit(fail === 0 ? 0 : 1);
