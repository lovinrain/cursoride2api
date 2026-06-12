#!/usr/bin/env node
// Integration test for POOL_PROXY_THINKING_BLOCKS=1 — forwarding upstream
// reasoning to the client as proxy-local `thinking` content blocks, plus the
// watchdog coupling that forwarding introduces.
//
// Drives the REAL api-server.mjs against a scripted mock pool over the Unix
// socket (same harness as liveness-watchdog-test.mjs) and reconstructs the
// client-facing Anthropic SSE content blocks to assert:
//   F1 basic forward     → thinking deltas become ONE ordered thinking block
//                          (proxy-local signature) before the text block
//   F2 interleave (A1)   → thinking AFTER text opens a NEW thinking block
//                          instead of being silently dropped
//   F3 thinking-only (A2)→ a thinking-only turn is NOT retried as "empty"
//   F4 hang w/ forward(C) → post-thinking silence with visible thinking →
//                          clean error + reap, NO transparent replay
//   F5 hang w/o forward(C)→ same silence WITHOUT forwarding → transparent
//                          replay recovers on a fresh channel
//   F6 byte cap (A4)     → forwarding stops after POOL_PROXY_THINKING_MAX_BYTES
//
// Run: node scaffolding/pool/proxy-thinking-forward-test.mjs

import net from 'node:net';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import os from 'node:os';
import fs from 'node:fs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const API_SERVER = path.join(HERE, 'api-server.mjs');
const REPO_ROOT = path.join(HERE, '..', '..');

let failures = 0;
function check(name, cond, detail) {
  if (cond) { console.log(`  ✓ ${name}`); }
  else { console.log(`  ✗ ${name} — ${detail}`); failures++; }
}

// Mock pool: on {type:'request'} send route_decision then run script(send, reqId, attempt).
function startMockPool(sockPath, script) {
  try { fs.unlinkSync(sockPath); } catch { /* ignore */ }
  const attempts = new Map();
  const server = net.createServer((conn) => {
    let buf = '';
    conn.on('data', (chunk) => {
      buf += chunk.toString('utf8');
      let idx;
      while ((idx = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, idx); buf = buf.slice(idx + 1);
        if (!line.trim()) continue;
        let msg; try { msg = JSON.parse(line); } catch { continue; }
        if (msg.type === 'request') {
          const reqId = msg.requestId;
          const attempt = (attempts.get(reqId) || 0) + 1;
          attempts.set(reqId, attempt);
          server._requestCount = (server._requestCount || 0) + 1;
          const send = (o) => { try { conn.write(JSON.stringify({ ...o, requestId: reqId }) + '\n'); } catch { /* ignore */ } };
          send({ type: 'route_decision', channelId: `ch-test-${attempt}`, servedModel: msg.model || 'm', fallback: false });
          script(send, reqId, attempt);
        }
        // cancel_request / others: ignore
      }
    });
    conn.on('error', () => {});
  });
  server._requestCount = 0;
  return new Promise((resolve) => server.listen(sockPath, () => resolve(server)));
}

function startApiServer(sockPath, port, extraEnv) {
  const env = {
    ...process.env,
    POOL_SOCK: sockPath,
    RATLC_API_PORT: String(port),
    RATLC_API_HOST: '127.0.0.1',
    RATLC_RETRY_UPSTREAM_SILENT_MAX: '0',
    RATLC_RETRY_EMPTY_TURN_MAX: '0',
    RATLC_KEEPALIVE_PING_MS: '0',
    POOL_REINJECT_THINKING: '0',
    RATLC_STATS_DISABLE: '1',   // isolate: never record into production /tmp/ratlc-stats.json
    ...extraEnv,
  };
  const proc = spawn(process.execPath, [API_SERVER], { cwd: REPO_ROOT, env, stdio: ['ignore', 'pipe', 'pipe'] });
  return new Promise((resolve, reject) => {
    let out = '';
    const onData = (d) => { out += d.toString('utf8'); if (/api-server listening/.test(out)) { cleanup(); resolve(proc); } };
    const onErr = (d) => { out += d.toString('utf8'); };
    const to = setTimeout(() => { cleanup(); reject(new Error('api-server did not start:\n' + out)); }, 8000);
    function cleanup() { clearTimeout(to); proc.stdout.off('data', onData); proc.stderr.off('data', onErr); }
    proc.stdout.on('data', onData); proc.stderr.on('data', onErr);
    proc.on('exit', (c) => reject(new Error('api-server exited early code=' + c + '\n' + out)));
  });
}

// POST /v1/messages (stream) and collect the full SSE event stream.
function streamRequest(port, model, { thinking = false, collectMs }) {
  return new Promise((resolve) => {
    const bodyObj = { model, max_tokens: 200, stream: true, messages: [{ role: 'user', content: 'hi' }] };
    if (thinking) bodyObj.thinking = { type: 'enabled', budget_tokens: 8000 };
    const body = JSON.stringify(bodyObj);
    const t0 = Date.now();
    let raw = '';
    const sse = []; // {t, event, data}
    const req = http.request({ host: '127.0.0.1', port, path: '/v1/messages', method: 'POST',
      headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) } }, (res) => {
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        raw += chunk;
        let idx;
        while ((idx = raw.indexOf('\n\n')) !== -1) {
          const block = raw.slice(0, idx); raw = raw.slice(idx + 2);
          let event = null, dataStr = '';
          for (const line of block.split('\n')) {
            if (line.startsWith('event:')) event = line.slice(6).trim();
            else if (line.startsWith('data:')) dataStr += line.slice(5).trim();
          }
          if (!event) continue;
          let data; try { data = JSON.parse(dataStr); } catch { data = dataStr; }
          sse.push({ t: Date.now() - t0, event, data });
        }
      });
      res.on('end', () => resolve({ sse, endedMs: Date.now() - t0 }));
    });
    req.on('error', () => resolve({ sse, endedMs: Date.now() - t0, errored: true }));
    req.write(body);
    req.end();
    setTimeout(() => { try { req.destroy(); } catch {} resolve({ sse, endedMs: Date.now() - t0, timedOut: true }); }, collectMs);
  });
}

// Reconstruct ordered content blocks from the SSE stream.
// Returns [{ index, type, text, signature }] in emission order.
function reconstructBlocks(sse) {
  const blocks = []; const byIndex = new Map();
  for (const { event, data } of sse) {
    if (typeof data !== 'object' || data == null) continue;
    if (event === 'content_block_start') {
      const b = { index: data.index, type: data.content_block && data.content_block.type, text: '', signature: null };
      byIndex.set(data.index, b); blocks.push(b);
    } else if (event === 'content_block_delta') {
      const b = byIndex.get(data.index); if (!b) continue;
      const d = data.delta || {};
      if (d.type === 'text_delta') b.text += d.text || '';
      else if (d.type === 'thinking_delta') b.text += d.thinking || '';
      else if (d.type === 'signature_delta') b.signature = d.signature || null;
    }
  }
  return blocks;
}
const allText = (sse) => reconstructBlocks(sse).filter((b) => b.type === 'text').map((b) => b.text).join('');
const thinkingBlocks = (sse) => reconstructBlocks(sse).filter((b) => b.type === 'thinking');

let portCursor = 14400;
async function runScenario({ script, env = {}, thinking = true, model = 'thinktest', collectMs }) {
  const idx = portCursor++;
  const sockPath = path.join(os.tmpdir(), `ratlc-fwdtest-${process.pid}-${idx}.sock`);
  const pool = await startMockPool(sockPath, script);
  const api = await startApiServer(sockPath, idx, env);
  const result = await streamRequest(idx, model, { thinking, collectMs });
  result.requestCount = pool._requestCount || 0;
  try { api.kill('SIGKILL'); } catch {}
  await new Promise((r) => pool.close(r));
  try { fs.unlinkSync(sockPath); } catch {}
  return result;
}

async function main() {
  console.log('proxy-thinking-forward-test');

  // F1: basic forward — thinking deltas → one ordered thinking block, then text.
  {
    const r = await runScenario({ collectMs: 1500, env: { POOL_PROXY_THINKING_BLOCKS: '1' },
      script: (send) => {
        setTimeout(() => send({ type: 'thinking_delta', text: 'Let me think. ' }), 80);
        setTimeout(() => send({ type: 'thinking_delta', text: 'Step two. ' }), 160);
        setTimeout(() => send({ type: 'thinking_delta', text: 'Conclusion. ' }), 240);
        setTimeout(() => send({ type: 'thinking_completed', durationMs: 300 }), 320);
        setTimeout(() => send({ type: 'text_delta', text: 'The answer is 42.' }), 400);
        setTimeout(() => send({ type: 'yield' }), 520);
      } });
    const blocks = reconstructBlocks(r.sse);
    const th = blocks.filter((b) => b.type === 'thinking');
    const tx = blocks.filter((b) => b.type === 'text');
    console.log('F1 basic forward:', { blocks: blocks.map((b) => b.type), reqs: r.requestCount });
    check('F1 exactly one thinking block', th.length === 1, `got ${th.length}`);
    check('F1 thinking text is the concatenated reasoning', th[0] && th[0].text === 'Let me think. Step two. Conclusion. ', `text=${JSON.stringify(th[0] && th[0].text)}`);
    check('F1 thinking carries a proxy-local signature', th[0] && typeof th[0].signature === 'string' && th[0].signature.startsWith('proxy-local-thinking-v1.'), `sig=${th[0] && th[0].signature}`);
    check('F1 answer text streamed', allText(r.sse).includes('The answer is 42.'), `text=${JSON.stringify(allText(r.sse))}`);
    check('F1 thinking precedes text (ordering)', th[0] && tx[0] && th[0].index < tx[0].index, `thinkIdx=${th[0] && th[0].index} textIdx=${tx[0] && tx[0].index}`);
    check('F1 no retry', r.requestCount === 1, `reqs=${r.requestCount}`);
  }

  // F2: interleave (A1) — thinking AFTER text must open a new block, not vanish.
  {
    const r = await runScenario({ collectMs: 1500, env: { POOL_PROXY_THINKING_BLOCKS: '1' },
      script: (send) => {
        setTimeout(() => send({ type: 'thinking_delta', text: 'FIRST-THINK' }), 80);
        setTimeout(() => send({ type: 'text_delta', text: 'FIRST-TEXT' }), 160);
        setTimeout(() => send({ type: 'thinking_delta', text: 'SECOND-THINK' }), 240);
        setTimeout(() => send({ type: 'text_delta', text: 'SECOND-TEXT' }), 320);
        setTimeout(() => send({ type: 'yield' }), 440);
      } });
    const th = thinkingBlocks(r.sse);
    const joinedThink = th.map((b) => b.text).join('|');
    console.log('F2 interleave:', { thinking: th.map((b) => b.text), text: allText(r.sse) });
    check('F2 second thinking was NOT dropped (two thinking blocks)', th.length === 2, `got ${th.length}: ${joinedThink}`);
    check('F2 both reasoning segments present', joinedThink.includes('FIRST-THINK') && joinedThink.includes('SECOND-THINK'), `joined=${joinedThink}`);
    check('F2 both text segments present', allText(r.sse).includes('FIRST-TEXT') && allText(r.sse).includes('SECOND-TEXT'), `text=${allText(r.sse)}`);
  }

  // F3: thinking-only turn (A2) — must NOT be retried as an empty turn.
  {
    const r = await runScenario({ collectMs: 1500,
      env: { POOL_PROXY_THINKING_BLOCKS: '1', RATLC_RETRY_EMPTY_TURN_MAX: '2', RATLC_RETRY_EMIT_NOTICE: '1' },
      script: (send) => {
        setTimeout(() => send({ type: 'thinking_delta', text: 'reasoning ' }), 80);
        setTimeout(() => send({ type: 'thinking_delta', text: 'only' }), 160);
        setTimeout(() => send({ type: 'thinking_completed', durationMs: 100 }), 240);
        setTimeout(() => send({ type: 'yield' }), 320);
      } });
    const th = thinkingBlocks(r.sse);
    console.log('F3 thinking-only:', { reqs: r.requestCount, thinking: th.map((b) => b.text) });
    check('F3 NOT retried as empty (one pool request)', r.requestCount === 1, `reqs=${r.requestCount} (>1 means forwarded thinking did not count as visible)`);
    check('F3 the reasoning was shown', th.some((b) => b.text.includes('reasoning only')), `thinking=${JSON.stringify(th.map((b) => b.text))}`);
  }

  // F4: post-thinking silence WITH forwarding (C) — clean error, NO replay.
  {
    const r = await runScenario({ collectMs: 1800,
      env: { POOL_PROXY_THINKING_BLOCKS: '1', RATLC_NO_VISIBLE_LIVENESS_GRACE_MS: '500', RATLC_RETRY_UPSTREAM_SILENT_MAX: '2' },
      script: (send) => {
        setTimeout(() => send({ type: 'thinking_delta', text: 'thinking hard then hang' }), 80);
        setTimeout(() => send({ type: 'thinking_completed', durationMs: 100 }), 160);
        // then silence — the gap (500ms) should fire ~660ms
      } });
    const th = thinkingBlocks(r.sse);
    const full = allText(r.sse);
    console.log('F4 hang-with-forward:', { reqs: r.requestCount, recycled: full.includes('recycled') });
    check('F4 did NOT replay over visible thinking (one pool request)', r.requestCount === 1, `reqs=${r.requestCount} (>1 means it replayed and would duplicate thinking)`);
    check('F4 the reasoning was shown before the hang', th.some((b) => b.text.includes('thinking hard then hang')), `thinking=${JSON.stringify(th.map((b) => b.text))}`);
    check('F4 surfaced a clean resumable error notice', full.includes('went silent') && full.includes('recycled'), `text=${JSON.stringify(full)}`);
  }

  // F5: post-thinking silence WITHOUT forwarding (C) — transparent replay recovers.
  {
    const r = await runScenario({ collectMs: 2500,
      env: { POOL_PROXY_THINKING_BLOCKS: '0', RATLC_NO_VISIBLE_LIVENESS_GRACE_MS: '500', RATLC_RETRY_UPSTREAM_SILENT_MAX: '2', RATLC_RETRY_EMIT_NOTICE: '1' },
      script: (send, reqId, attempt) => {
        if (attempt === 1) {
          setTimeout(() => send({ type: 'thinking_delta', text: 'hidden-reasoning' }), 80);
          setTimeout(() => send({ type: 'thinking_completed', durationMs: 100 }), 160);
          // then silence → gap fires → replay (nothing visible was sent)
        } else {
          setTimeout(() => send({ type: 'text_delta', text: 'recovered-answer' }), 100);
          setTimeout(() => send({ type: 'yield' }), 300);
        }
      } });
    const full = allText(r.sse);
    const th = thinkingBlocks(r.sse);
    console.log('F5 hang-without-forward:', { reqs: r.requestCount, recovered: full.includes('recovered-answer') });
    check('F5 transparently replayed (>=2 pool requests)', r.requestCount >= 2, `reqs=${r.requestCount} (==1 means the silent hang was not retried)`);
    check('F5 recovered the answer on a fresh channel', full.includes('recovered-answer'), `text=${JSON.stringify(full)}`);
    check('F5 hidden reasoning was NOT forwarded (no real thinking text)', !th.some((b) => b.text.includes('hidden-reasoning')), `thinking=${JSON.stringify(th.map((b) => b.text))}`);
  }

  // F6: byte cap (A4) — forwarding stops once POOL_PROXY_THINKING_MAX_BYTES is hit.
  {
    const r = await runScenario({ collectMs: 1500,
      env: { POOL_PROXY_THINKING_BLOCKS: '1', POOL_PROXY_THINKING_MAX_BYTES: '10' },
      script: (send) => {
        setTimeout(() => send({ type: 'thinking_delta', text: '12345678' }), 80);   // 8B  → forwarded (0<10)
        setTimeout(() => send({ type: 'thinking_delta', text: 'ABCDEFGH' }), 160);   // 8B  → forwarded (8<10) → total 16
        setTimeout(() => send({ type: 'thinking_delta', text: 'xxxxxxxx' }), 240);   // blocked (16>=10)
        setTimeout(() => send({ type: 'text_delta', text: 'done' }), 320);
        setTimeout(() => send({ type: 'yield' }), 440);
      } });
    const th = thinkingBlocks(r.sse);
    const joined = th.map((b) => b.text).join('');
    console.log('F6 byte-cap:', { forwarded: joined, len: joined.length });
    check('F6 forwarded up to the crossing delta then stopped', joined === '12345678ABCDEFGH', `forwarded=${JSON.stringify(joined)} (expected first two deltas only)`);
    check('F6 over-cap reasoning was dropped', !joined.includes('xxxxxxxx'), 'third delta leaked past the cap');
    check('F6 answer still streamed after the cap', allText(r.sse).includes('done'), `text=${allText(r.sse)}`);
  }

  console.log('');
  if (failures === 0) { console.log('proxy-thinking-forward-test: OK'); process.exit(0); }
  else { console.log(`proxy-thinking-forward-test: FAIL (${failures} checks)`); process.exit(1); }
}

main().catch((e) => { console.error('test harness error:', e); process.exit(2); });
