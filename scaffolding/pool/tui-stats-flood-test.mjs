// Acceptance test for the "TUI floods the api log with /v1/_stats calls" bug.
//
// Reproduces the original condition — a busy api log (frequent tail activity →
// frequent TUI re-renders) — and asserts BOTH fixes hold:
//   Fix A: the api-server does NOT log introspection GETs (/v1/_stats), so the
//          log the TUI tails is never flooded by the TUI's own polling.
//   Fix B: the TUI throttles its /v1/_stats fetch (cache), so even under heavy
//          render pressure it hits the endpoint a bounded number of times.
// Plus a regression check that the compact latency band still renders.
import net from 'node:net';
import http from 'node:http';
import { spawn } from 'node:child_process';
import path from 'node:path';
import os from 'node:os';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.join(HERE, '..', '..');
const PORT = 14644;          // api-server
const PROXY_PORT = 14645;    // counting proxy in front of the api-server
const SOCK = path.join(os.tmpdir(), `ratlc-flood-${process.pid}.sock`);
const STATS_FILE = path.join(os.tmpdir(), `ratlc-flood-stats-${process.pid}.json`);
const API_LOG = path.join(os.tmpdir(), `ratlc-flood-api-${process.pid}.log`);
const POOL_LOG = path.join(os.tmpdir(), `ratlc-flood-pool-${process.pid}.log`);
const MODEL = 'claude-opus-4-8-thinking-max-fast';
let fail = 0; const a = (n, c, d) => { console.log((c ? '  ✓ ' : '  ✗ ') + n + (c ? '' : ` — ${d}`)); if (!c) fail++; };

const snapshot = () => ({
  pool: { actualSize: 2, configuredSize: 2, readyCount: 2, busyCount: 0, openingCount: 0, deadCount: 0,
    pendingRequests: 0, toolUseIndex: 0, channels: [], groups: [], tokens: [] },
  config: { toolMode: 'translate', model: MODEL, concurrentOpens: 1,
    watchdog: { livenessGapMs: 90000, ceilingMs: 300000, busyStuckMs: 360000,
      fast: { livenessGapMs: 90000, ceilingMs: 300000, busyStuckMs: 360000 },
      slow: { livenessGapMs: 150000, ceilingMs: 600000, busyStuckMs: 660000 }, adaptive: false } },
});

function startMockPool() {
  try { fs.unlinkSync(SOCK); } catch {}
  const server = net.createServer((conn) => {
    let buf = '';
    conn.on('data', (chunk) => {
      buf += chunk.toString('utf8'); let i;
      while ((i = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, i); buf = buf.slice(i + 1); if (!line.trim()) continue;
        let msg; try { msg = JSON.parse(line); } catch { continue; }
        if (msg.type === 'status') { try { conn.write(JSON.stringify(snapshot()) + '\n'); } catch {} continue; }
        if (msg.type === 'request') {
          const reqId = msg.requestId;
          const send = (o) => { try { conn.write(JSON.stringify({ ...o, requestId: reqId }) + '\n'); } catch {} };
          send({ type: 'route_decision', channelId: 'ch-1', servedModel: msg.model || MODEL, fallback: false });
          setTimeout(() => send({ type: 'text_delta', text: 'hi' }), 50);
          setTimeout(() => send({ type: 'yield' }), 100);
        }
      }
    });
    conn.on('error', () => {});
  });
  return new Promise((r) => server.listen(SOCK, () => r(server)));
}
function startApiServer(stdoutBuf) {
  const env = { ...process.env, POOL_SOCK: SOCK, RATLC_API_PORT: String(PORT), RATLC_API_HOST: '127.0.0.1',
    RATLC_STATS_FILE: STATS_FILE, RATLC_STATS_MIN_SUGGEST_SAMPLES: '5', RATLC_STATS_MIN_HOUR_SAMPLES: '5',
    RATLC_KEEPALIVE_PING_MS: '0', POOL_REINJECT_THINKING: '0' };
  const proc = spawn(process.execPath, [path.join(HERE, 'api-server.mjs')], { cwd: REPO_ROOT, env, stdio: ['ignore', 'pipe', 'pipe'] });
  return new Promise((resolve, reject) => {
    let started = false;
    const on = (d) => { stdoutBuf.s += d.toString('utf8'); if (!started && /api-server listening/.test(stdoutBuf.s)) { started = true; clearTimeout(to); resolve(proc); } };
    const to = setTimeout(() => reject(new Error('api start timeout:\n' + stdoutBuf.s)), 8000);
    proc.stdout.on('data', on); proc.stderr.on('data', on);
  });
}
// Counting proxy: forwards every request to the api-server, tallies /v1/_stats.
function startProxy(counter) {
  const server = http.createServer((creq, cres) => {
    if ((creq.url || '').split('?')[0] === '/v1/_stats') counter.n++;
    const preq = http.request({ host: '127.0.0.1', port: PORT, path: creq.url, method: creq.method, headers: creq.headers }, (pres) => {
      cres.writeHead(pres.statusCode || 200, pres.headers); pres.pipe(cres);
    });
    preq.on('error', () => { try { cres.writeHead(502); cres.end(); } catch {} });
    creq.pipe(preq);
  });
  return new Promise((r) => server.listen(PROXY_PORT, '127.0.0.1', () => r(server)));
}
function oneRequest() {
  return new Promise((resolve) => {
    const body = JSON.stringify({ model: MODEL, max_tokens: 50, stream: true, messages: [{ role: 'user', content: 'hello answer me please' }] });
    const req = http.request({ host: '127.0.0.1', port: PORT, path: '/v1/messages', method: 'POST', headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) } }, (res) => { res.on('data', () => {}); res.on('end', resolve); });
    req.on('error', resolve); req.write(body); req.end(); setTimeout(() => { try { req.destroy(); } catch {} resolve(); }, 3000);
  });
}

const apiOut = { s: '' };
const counter = { n: 0 };
const pool = await startMockPool();
const api = await startApiServer(apiOut);
const proxy = await startProxy(counter);
try {
  // Populate stats so the band has data to render.
  for (let i = 0; i < 8; i++) await oneRequest();
  await new Promise((r) => setTimeout(r, 200));
  const apiOutAfterDrive = apiOut.s;     // snapshot of api log after the real traffic

  // Spawn the REAL tui (non-TTY → just renders frames). Point its getStats at the
  // counting proxy; isolate its log tails to temp files we control.
  fs.writeFileSync(API_LOG, ''); fs.writeFileSync(POOL_LOG, '');
  const tui = spawn(process.execPath, [path.join(HERE, 'ratlc.mjs'), 'tui'], {
    cwd: REPO_ROOT, env: { ...process.env, POOL_SOCK: SOCK, RATLC_API_URL: `http://127.0.0.1:${PROXY_PORT}`, RATLC_API_LOG: API_LOG, RATLC_POOL_LOG: POOL_LOG } });
  let frames = ''; tui.stdout.on('data', (d) => frames += d.toString('utf8')); tui.stderr.on('data', (d) => frames += d.toString('utf8'));

  // SPAM the api log the TUI tails — this forces frequent dirty flips → frequent
  // re-renders, the exact condition that produced the flood. Throttle (Fix B)
  // must keep the /v1/_stats hit count bounded despite this.
  const spam = setInterval(() => { try { fs.appendFileSync(API_LOG, `[spam] ${'x'.repeat(20)}\n`); } catch {} }, 50);
  const counterAtStart = counter.n;
  await new Promise((r) => setTimeout(r, 4000));
  clearInterval(spam);
  try { tui.kill('SIGKILL'); } catch {}
  const statsHitsDuringTui = counter.n - counterAtStart;

  // (Fix A) The api-server must NOT log /v1/_stats — scan its whole stdout.
  const statsLogLines = (apiOut.s.match(/\/v1\/_stats/g) || []).length;
  a('api-server never logs /v1/_stats (Fix A: no flood at source)', statsLogLines === 0, `found ${statsLogLines} log lines`);
  // Real traffic IS still logged (didn't over-suppress).
  a('api-server still logs POST /v1/messages', /POST \/v1\/messages/.test(apiOutAfterDrive), 'no request log line');

  // (Fix B) Despite ~80 dirty-flips (4s of 50ms spam), the throttle (3s TTL)
  // bounds stats fetches to a handful. Without it this would be dozens.
  a('TUI stats fetches bounded under render pressure (Fix B: throttle)', statsHitsDuringTui <= 4, `hits=${statsHitsDuringTui} in 4s`);
  a('TUI did fetch stats at least once (band is live)', statsHitsDuringTui >= 1, `hits=${statsHitsDuringTui}`);

  // Regression: the compact band still renders (through the cache) with data.
  const clean = frames.replace(/\x1b\[[0-9;]*m/g, '').replace(/\x1b\[[0-9]*[A-Za-z]/g, '').replace(/\x1bc/g, '');
  a('compact latency band still renders in split view', /Latency regime=/.test(clean), 'no band');
  a('band shows the model line with first-byte stats', clean.includes(MODEL) && /FB /.test(clean), 'no model line');

  // ── Version-skew (adversarial Gap #1): TUI relaunched against an OLD api-server
  // that has no /v1/_stats → 404 with a VALID-JSON {"error":"not found"} body.
  // Must show an ACTIONABLE signal, never regime=? / hour=undefined garbage.
  console.log('=== version skew: old api-server (404 on /v1/_stats) ===');
  const OLD_PORT = 14646;
  const oldApi = http.createServer((req, res) => { res.writeHead(404, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: 'not found' })); });
  await new Promise((r) => oldApi.listen(OLD_PORT, '127.0.0.1', r));
  // (a) one-shot `ratlc stats` → clean error + exit 1, no garbage.
  const oneShot = await new Promise((resolve) => {
    const p = spawn(process.execPath, [path.join(HERE, 'ratlc.mjs'), 'stats'], { cwd: REPO_ROOT, env: { ...process.env, RATLC_API_URL: `http://127.0.0.1:${OLD_PORT}` } });
    let o = ''; p.stdout.on('data', (d) => o += d); p.stderr.on('data', (d) => o += d);
    p.on('exit', (code) => resolve({ o: o.replace(/\x1b\[[0-9;]*m/g, ''), code }));
  });
  a('one-shot `ratlc stats` reports unavailable on old server', /unavailable/i.test(oneShot.o) && oneShot.code === 1, `code=${oneShot.code} out=${oneShot.o.slice(0, 80)}`);
  a('one-shot shows no regime=?/undefined garbage', !/regime=\?/.test(oneShot.o) && !/undefined/.test(oneShot.o), oneShot.o.slice(0, 80));
  // (b) the live TUI against the old server → actionable hint in the band, no garbage.
  const tui2 = spawn(process.execPath, [path.join(HERE, 'ratlc.mjs'), 'tui'], {
    cwd: REPO_ROOT, env: { ...process.env, POOL_SOCK: SOCK, RATLC_API_URL: `http://127.0.0.1:${OLD_PORT}`, RATLC_API_LOG: API_LOG, RATLC_POOL_LOG: POOL_LOG } });
  let f2 = ''; tui2.stdout.on('data', (d) => f2 += d.toString('utf8')); tui2.stderr.on('data', (d) => f2 += d.toString('utf8'));
  await new Promise((r) => setTimeout(r, 1600));
  try { tui2.kill('SIGKILL'); } catch {}
  const c2 = f2.replace(/\x1b\[[0-9;]*m/g, '').replace(/\x1b\[[0-9]*[A-Za-z]/g, '').replace(/\x1bc/g, '');
  a('TUI band shows actionable "./launch.sh up" hint on old server', /launch\.sh up/.test(c2), 'no hint');
  a('TUI shows NO regime=?/hour=undefined garbage on old server', !/regime=\?/.test(c2) && !/hour=undefined/.test(c2), 'garbage rendered');
  await new Promise((r) => oldApi.close(r));
} catch (e) { console.log('  ✗ harness error:', e.message); fail++; }
try { api.kill('SIGKILL'); } catch {}
await new Promise((r) => proxy.close(r));
await new Promise((r) => pool.close(r));
for (const f of [SOCK, STATS_FILE, API_LOG, POOL_LOG]) { try { fs.unlinkSync(f); } catch {} }
console.log(fail === 0 ? '\ntui-stats-flood-test: OK' : `\ntui-stats-flood-test: FAIL (${fail})`);
process.exit(fail === 0 ? 0 : 1);
