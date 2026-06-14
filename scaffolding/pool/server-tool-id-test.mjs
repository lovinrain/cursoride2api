// Verifies the api-server emits server_tool_use ids that match Anthropic's
// required pattern ^srvtoolu_[a-zA-Z0-9_]+$ (NO dashes), so a session that used
// Cursor WebSearch/WebFetch can be RESUMED against the real Anthropic API without
// a 400 on the stored server_tool_use.id. Also checks the started/result pairing
// stays identical, and idempotency for an already-valid id.
import net from 'node:net';
import http from 'node:http';
import { spawn } from 'node:child_process';
import path from 'node:path';
import os from 'node:os';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.join(HERE, '..', '..');
const PORT = 14666;
const SOCK = path.join(os.tmpdir(), `ratlc-stid-${process.pid}.sock`);
const PATTERN = /^srvtoolu_[a-zA-Z0-9_]+$/;       // Anthropic's exact rule
let fail = 0; const a = (n, c, d) => { console.log((c ? '  ✓ ' : '  ✗ ') + n + (c ? '' : ` — ${d}`)); if (!c) fail++; };

// id the mock pool sends for each model (mimics Cursor's native tool ids).
const ID_FOR = {
  'dashy': 'ws-7f3a9c21-4b1e-4d2a-9c8e-abc123def456', // UUID-ish WITH dashes (the bug trigger)
  'valid': 'srvtoolu_already_valid_123',               // already conforms → must round-trip
  'noid': undefined,                                    // missing → must still produce a valid id
};
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
        const reqId = msg.requestId; const id = ID_FOR[msg.model];
        const send = (o) => { try { conn.write(JSON.stringify({ ...o, requestId: reqId }) + '\n'); } catch {} };
        send({ type: 'route_decision', channelId: 'ch-1', servedModel: msg.model, fallback: false });
        const stu = (phase, extra) => send({ type: 'server_tool_use', name: 'web_search', phase, ...(id !== undefined ? { id } : {}), ...extra });
        setTimeout(() => stu('started', { input: { query: 'who won' } }), 30);
        setTimeout(() => stu('completed', { content: [{ type: 'web_search_result', url: 'https://example.com', title: 'X' }] }), 80);
        setTimeout(() => send({ type: 'text_delta', text: 'done.' }), 120);
        setTimeout(() => send({ type: 'yield' }), 160);
      }
    });
    conn.on('error', () => {});
  });
  return new Promise((r) => server.listen(SOCK, () => r(server)));
}
function startApi() {
  const env = { ...process.env, POOL_SOCK: SOCK, RATLC_API_PORT: String(PORT), RATLC_API_HOST: '127.0.0.1',
    RATLC_RETRY_EMPTY_TURN_MAX: '0', RATLC_RETRY_UPSTREAM_SILENT_MAX: '0', RATLC_KEEPALIVE_PING_MS: '0',
    POOL_REINJECT_THINKING: '0', RATLC_STATS_DISABLE: '1', RATLC_RENDER_SERVER_TOOL_TEXT: '0' };
  const proc = spawn(process.execPath, [path.join(HERE, 'api-server.mjs')], { cwd: REPO_ROOT, env, stdio: ['ignore', 'pipe', 'pipe'] });
  return new Promise((resolve, reject) => {
    let out = ''; const on = (d) => { out += d; if (/api-server listening/.test(out)) { clearTimeout(to); resolve(proc); } };
    const to = setTimeout(() => reject(new Error('api start timeout:\n' + out)), 8000);
    proc.stdout.on('data', on); proc.stderr.on('data', on);
  });
}
// Returns { stuId, resultToolUseId } parsed from the SSE.
function ask(model) {
  return new Promise((resolve) => {
    const body = JSON.stringify({ model, max_tokens: 100, stream: true, messages: [{ role: 'user', content: 'hi' }] });
    const req = http.request({ host: '127.0.0.1', port: PORT, path: '/v1/messages', method: 'POST', headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) } }, (res) => {
      let sse = ''; res.on('data', (c) => sse += c); res.on('end', () => {
        let stuId = null, resultToolUseId = null;
        for (const m of sse.split('\n')) {
          if (!m.startsWith('data:')) continue;
          let j; try { j = JSON.parse(m.slice(5).trim()); } catch { continue; }
          const cb = j && j.content_block;
          if (cb && cb.type === 'server_tool_use') stuId = cb.id;
          if (cb && (cb.type === 'web_search_tool_result' || cb.type === 'web_fetch_tool_result')) resultToolUseId = cb.tool_use_id;
        }
        resolve({ stuId, resultToolUseId });
      });
    });
    req.on('error', () => resolve({})); req.write(body); req.end();
    setTimeout(() => { try { req.destroy(); } catch {} resolve({}); }, 8000);
  });
}

const pool = await startMockPool();
const api = await startApi();
try {
  console.log('=== 1. dashy Cursor UUID id → conforms + no dashes ===');
  const d = await ask('dashy');
  a('server_tool_use.id matches ^srvtoolu_[a-zA-Z0-9_]+$', PATTERN.test(d.stuId || ''), `id=${d.stuId}`);
  a('id has NO dashes (the original UUID dashes were sanitized)', d.stuId && !d.stuId.includes('-'), `id=${d.stuId}`);
  a('web_search_tool_result.tool_use_id matches the server_tool_use.id (pairing intact)', d.resultToolUseId === d.stuId && PATTERN.test(d.resultToolUseId || ''), `stu=${d.stuId} result=${d.resultToolUseId}`);

  console.log('=== 2. already-valid id → round-trips unchanged (idempotent) ===');
  const v = await ask('valid');
  a('already-valid id is preserved (not double-prefixed)', v.stuId === 'srvtoolu_already_valid_123', `id=${v.stuId}`);
  a('still matches the pattern', PATTERN.test(v.stuId || ''), `id=${v.stuId}`);

  console.log('=== 3. missing id → still a valid srvtoolu_ id ===');
  const n = await ask('noid');
  a('missing upstream id → synthesized valid id', PATTERN.test(n.stuId || ''), `id=${n.stuId}`);
  a('pairing still intact for synthesized id', n.resultToolUseId === n.stuId, `stu=${n.stuId} result=${n.resultToolUseId}`);
} catch (e) { console.log('  ✗ harness error:', e.message); fail++; }
try { api.kill('SIGKILL'); } catch {}
await new Promise((r) => pool.close(r));
try { fs.unlinkSync(SOCK); } catch {}
console.log(fail === 0 ? '\nserver-tool-id-test: OK' : `\nserver-tool-id-test: FAIL (${fail})`);
process.exit(fail === 0 ? 0 : 1);
