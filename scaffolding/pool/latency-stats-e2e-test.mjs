// E2E: drive real requests through api-server (mock pool), then verify the
// /v1/_stats endpoint records per-model latency + that `ratlc stats` renders it.
import net from 'node:net';
import http from 'node:http';
import { spawn } from 'node:child_process';
import path from 'node:path';
import os from 'node:os';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.join(HERE, '..', '..');
const PORT = 14620;
const SOCK = path.join(os.tmpdir(), `ratlc-statse2e-${process.pid}.sock`);
const STATS_FILE = path.join(os.tmpdir(), `ratlc-stats-e2e-${process.pid}.json`);
const MODEL = 'claude-test-max-fast';   // -fast → fast type
const ENV = {
  POOL_SOCK: SOCK, RATLC_API_PORT: String(PORT), RATLC_API_HOST: '127.0.0.1',
  RATLC_RETRY_UPSTREAM_SILENT_MAX: '0', RATLC_RETRY_EMPTY_TURN_MAX: '0', RATLC_KEEPALIVE_PING_MS: '0',
  POOL_REINJECT_THINKING: '0',
  RATLC_STATS_FILE: STATS_FILE, RATLC_STATS_MIN_SUGGEST_SAMPLES: '5', RATLC_STATS_MIN_HOUR_SAMPLES: '5',
  RATLC_ADAPTIVE_TIMEOUTS: '1',
};
let fail = 0; const a = (n, c, d) => { console.log((c ? '  ✓ ' : '  ✗ ') + n + (c ? '' : ` — ${d}`)); if (!c) fail++; };

function startMockPool() {
  try { fs.unlinkSync(SOCK); } catch {}
  const server = net.createServer((conn) => {
    let buf = '';
    conn.on('data', (chunk) => {
      buf += chunk.toString('utf8'); let i;
      while ((i = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, i); buf = buf.slice(i + 1); if (!line.trim()) continue;
        let msg; try { msg = JSON.parse(line); } catch { continue; }
        if (msg.type === 'request') {
          const reqId = msg.requestId;
          const send = (o) => { try { conn.write(JSON.stringify({ ...o, requestId: reqId }) + '\n'); } catch {} };
          send({ type: 'route_decision', channelId: 'ch-1', servedModel: msg.model || MODEL, fallback: false });
          setTimeout(() => send({ type: 'text_delta', text: 'answer ' }), 60 + Math.floor(Math.random() * 40));
          setTimeout(() => send({ type: 'text_delta', text: 'here.' }), 120);
          setTimeout(() => send({ type: 'yield' }), 180);
        }
      }
    });
    conn.on('error', () => {});
  });
  return new Promise((r) => server.listen(SOCK, () => r(server)));
}
function startApiServer() {
  const proc = spawn(process.execPath, [path.join(HERE, 'api-server.mjs')], { cwd: REPO_ROOT, env: { ...process.env, ...ENV }, stdio: ['ignore', 'pipe', 'pipe'] });
  return new Promise((resolve, reject) => {
    let out = '';
    const onData = (d) => { out += d.toString('utf8'); if (/api-server listening/.test(out)) { cleanup(); resolve(proc); } };
    const to = setTimeout(() => { cleanup(); reject(new Error('api-server did not start:\n' + out)); }, 8000);
    function cleanup() { clearTimeout(to); proc.stdout.off('data', onData); proc.stderr.off('data', onData); }
    proc.stdout.on('data', onData); proc.stderr.on('data', onData);
  });
}
function oneRequest() {
  return new Promise((resolve) => {
    const body = JSON.stringify({ model: MODEL, max_tokens: 100, stream: true, messages: [{ role: 'user', content: 'hello world please answer' }] });
    const req = http.request({ host: '127.0.0.1', port: PORT, path: '/v1/messages', method: 'POST', headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) } }, (res) => {
      res.on('data', () => {}); res.on('end', () => resolve());
    });
    req.on('error', () => resolve()); req.write(body); req.end();
    setTimeout(() => { try { req.destroy(); } catch {} resolve(); }, 5000);
  });
}
function getJSON(p) {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('timeout')), 5000);
    http.get({ host: '127.0.0.1', port: PORT, path: p }, (res) => { let b = ''; res.on('data', (c) => b += c); res.on('end', () => { clearTimeout(t); try { resolve(JSON.parse(b)); } catch (e) { reject(e); } }); }).on('error', (e) => { clearTimeout(t); reject(e); });
  });
}
function runRatlcStats() {
  return new Promise((resolve) => {
    const proc = spawn(process.execPath, [path.join(HERE, 'ratlc.mjs'), 'stats'], { cwd: REPO_ROOT, env: { ...process.env, RATLC_API_URL: `http://127.0.0.1:${PORT}` } });
    let out = ''; proc.stdout.on('data', (d) => out += d); proc.stderr.on('data', (d) => out += d);
    proc.on('exit', () => resolve(out.replace(/\x1b\[[0-9;]*m/g, '')));
  });
}

const pool = await startMockPool();
const api = await startApiServer();
try {
  console.log('driving 12 requests…');
  for (let i = 0; i < 12; i++) await oneRequest();
  await new Promise((r) => setTimeout(r, 300));

  console.log('=== GET /v1/_stats ===');
  const snap = await getJSON('/v1/_stats');
  const m = (snap.models || []).find((x) => x.model === MODEL);
  a('endpoint returns the served model', !!m, 'models=' + JSON.stringify((snap.models || []).map((x) => x.model)));
  a('model type classified fast', m && m.type === 'fast', m && m.type);
  a('overall count ~12', m && m.overall.count >= 10 && m.overall.count <= 13, m && m.overall.count);
  const b = m && m.buckets[0];
  a('bucket <=8KB present', b && b.bucket === '<=8KB', b && b.bucket);
  a('fb p50 finite & ~60-120ms', b && Number.isFinite(b.fb.p50) && b.fb.p50 >= 40 && b.fb.p50 <= 200, b && b.fb.p50);
  a('total p50 finite & >= fb', b && Number.isFinite(b.total.p50) && b.total.p50 >= b.fb.p50, b && `${b && b.total.p50}/${b && b.fb.p50}`);
  a('adaptiveTimeouts=true surfaced', snap.adaptiveTimeouts === true, snap.adaptiveTimeouts);
  a('currentRegime present', typeof snap.currentRegime === 'string', snap.currentRegime);
  a('suggested gap present (>=5 samples)', b && Number.isFinite(b.currentRegimeSuggestGapMs), b && b.currentRegimeSuggestGapMs);
  a('regimeOf has 24 hours', snap.regimeOf && Object.keys(snap.regimeOf).length === 24, snap.regimeOf && Object.keys(snap.regimeOf).length);

  console.log('=== ratlc stats (rendered) ===');
  const rendered = await runRatlcStats();
  console.log(rendered.split('\n').slice(0, 8).join('\n'));
  a('renders the model', rendered.includes(MODEL), 'missing model');
  a('renders FBp50 header', /FBp50/.test(rendered), 'no header');
  a('renders a regime line', /regime=/.test(rendered), 'no regime line');
  a('renders the <=8KB bucket row', /<=8KB/.test(rendered), 'no bucket row');
} catch (e) { console.log('  ✗ harness error:', e.message); fail++; }
try { api.kill('SIGKILL'); } catch {}
await new Promise((r) => pool.close(r));
try { fs.unlinkSync(SOCK); } catch {}
try { fs.unlinkSync(STATS_FILE); } catch {}
console.log(fail === 0 ? '\nlatency-stats-e2e-test: OK' : `\nlatency-stats-e2e-test: FAIL (${fail})`);
process.exit(fail === 0 ? 0 : 1);
