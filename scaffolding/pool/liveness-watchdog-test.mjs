#!/usr/bin/env node
// Integration test for Option B — liveness-gated first-byte watchdog.
//
// Drives the REAL api-server.mjs against a scripted mock pool over the Unix
// socket, and asserts the two-timer behavior:
//   S1 silence            → liveness-gap fires at ~grace (not ceiling)
//   S2 progress-then-stop → gap RESET by progress, fires ~grace after LAST frame
//   S3 frames-forever     → gap never fires; absolute CEILING fires as backstop
//   S4 text_delta (alive) → first visible event disarms both → NO timeout notice
//   S5 grace=0 (disabled) → only the ceiling fires (pure Option A)
//
// The api-server connects to POOL_SOCK as a client and sends
// {type:'request', requestId, action, model, ...}; we reply with route_decision
// then a per-scenario event script. RETRY budgets are 0 so a fired watchdog
// just emits the [proxy_notice] + finishes, which we observe in the SSE stream.
//
// Run: node scaffolding/pool/liveness-watchdog-test.mjs

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

// NOTE: api-server clamps the ceiling to Math.max(5_000, env) — 5s is the hard
// floor — so the ceiling-backstop scenarios (S3/S5) necessarily run to ~5s.
const CEILING_MS = 5000;
const GRACE_MS = 500;

let failures = 0;
function check(name, cond, detail) {
  if (cond) { console.log(`  ✓ ${name}`); }
  else { console.log(`  ✗ ${name} — ${detail}`); failures++; }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Mock pool: on {type:'request'} send route_decision then run script(conn, reqId).
function startMockPool(sockPath, script) {
  try { fs.unlinkSync(sockPath); } catch { /* ignore */ }
  const attempts = new Map(); // reqId -> count (a retry replays the same reqId)
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
    RATLC_NO_VISIBLE_EVENT_TIMEOUT_MS: String(CEILING_MS),
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

// POST /v1/messages (stream) and collect SSE deltas with arrival timestamps.
function streamRequest(port, model, collectMs) {
  return new Promise((resolve) => {
    const body = JSON.stringify({ model, max_tokens: 100, stream: true, messages: [{ role: 'user', content: 'hi' }] });
    const t0 = Date.now();
    const events = []; // {t, text}
    const req = http.request({ host: '127.0.0.1', port, path: '/v1/messages', method: 'POST',
      headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) } }, (res) => {
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        for (const m of chunk.matchAll(/"text":"((?:[^"\\]|\\.)*)"/g)) {
          let txt; try { txt = JSON.parse('"' + m[1] + '"'); } catch { txt = m[1]; }
          events.push({ t: Date.now() - t0, text: txt });
        }
      });
      res.on('end', () => resolve({ events, endedMs: Date.now() - t0 }));
    });
    req.on('error', () => resolve({ events, endedMs: Date.now() - t0, errored: true }));
    req.write(body);
    req.end();
    setTimeout(() => { try { req.destroy(); } catch {} resolve({ events, endedMs: Date.now() - t0, timedOut: true }); }, collectMs);
  });
}

// Find the proxy timeout notice + the "within Nms" value it reports.
function findTimeoutNotice(events) {
  const e = events.find((x) => x.text.includes('[proxy_notice]') && x.text.includes('did not emit'));
  if (!e) return null;
  const m = e.text.match(/within (\d+)ms/);
  return { t: e.t, withinMs: m ? parseInt(m[1], 10) : null };
}

async function runScenario({ name, model, script, env = {}, idx, collectMs }) {
  const sockPath = path.join(os.tmpdir(), `ratlc-livetest-${process.pid}-${idx}.sock`);
  const port = 14300 + idx;
  const pool = await startMockPool(sockPath, script);
  const api = await startApiServer(sockPath, port, env);
  const result = await streamRequest(port, model, collectMs);
  result.requestCount = pool._requestCount || 0; // # of pool requests (>1 ⇒ a retry replayed)
  try { api.kill('SIGKILL'); } catch {}
  await new Promise((r) => pool.close(r));
  try { fs.unlinkSync(sockPath); } catch {}
  return result;
}

async function main() {
  console.log(`liveness-watchdog-test (ceiling=${CEILING_MS}ms grace=${GRACE_MS}ms)`);

  // S1: silence after route_decision → gap fires at ~grace.
  {
    const r = await runScenario({ idx: 1, name: 'S1 silence', model: 's1', collectMs: 1800,
      env: { RATLC_NO_VISIBLE_LIVENESS_GRACE_MS: String(GRACE_MS) },
      script: () => { /* send nothing */ } });
    const n = findTimeoutNotice(r.events);
    console.log('S1 silence (expect gap fire ~%dms, within=%d):', GRACE_MS, GRACE_MS, n);
    check('S1 notice fired', !!n, 'no proxy_notice seen');
    check('S1 reports gap window', n && n.withinMs === GRACE_MS, `withinMs=${n && n.withinMs}`);
    check('S1 fired near grace, before ceiling', n && n.t >= GRACE_MS - 150 && n.t < CEILING_MS - 100, `t=${n && n.t}`);
  }

  // S2: progress at 200 & 400ms then stop → gap reset, fires ~grace after LAST frame (~900ms), before ceiling.
  {
    const r = await runScenario({ idx: 2, name: 'S2 progress-then-stop', model: 's2', collectMs: 2200,
      env: { RATLC_NO_VISIBLE_LIVENESS_GRACE_MS: String(GRACE_MS) },
      script: (send) => {
        setTimeout(() => send({ type: 'progress', kind: 'upstream_frame' }), 200);
        setTimeout(() => send({ type: 'progress', kind: 'upstream_frame' }), 400);
      } });
    const n = findTimeoutNotice(r.events);
    console.log('S2 progress-then-stop (expect fire ~900ms = 400+grace):', n);
    check('S2 notice fired', !!n, 'no proxy_notice seen');
    check('S2 reports gap window', n && n.withinMs === GRACE_MS, `withinMs=${n && n.withinMs}`);
    check('S2 gap was RESET (fired after last frame+grace, not at first grace)', n && n.t >= 400 + GRACE_MS - 150 && n.t < CEILING_MS, `t=${n && n.t} (a non-reset gap would fire ~${GRACE_MS})`);
  }

  // S3: progress every 200ms up to 2s → gap never fires; ceiling backstop fires ~1500ms.
  {
    // progress every 300ms (< grace 500) past the ceiling: gap keeps resetting
    // and never wins; the absolute ceiling must fire as the backstop.
    const timers = [];
    const r = await runScenario({ idx: 3, name: 'S3 frames-forever', model: 's3', collectMs: CEILING_MS + 1500,
      env: { RATLC_NO_VISIBLE_LIVENESS_GRACE_MS: String(GRACE_MS) },
      script: (send) => { for (let t = 300; t <= CEILING_MS + 1000; t += 300) timers.push(setTimeout(() => send({ type: 'progress', kind: 'upstream_frame' }), t)); } });
    timers.forEach(clearTimeout);
    const n = findTimeoutNotice(r.events);
    console.log('S3 frames-forever (expect CEILING fire ~%dms, within=%d):', CEILING_MS, CEILING_MS, n);
    check('S3 notice fired', !!n, 'no proxy_notice seen');
    check('S3 reports CEILING window (gap never won)', n && n.withinMs === CEILING_MS, `withinMs=${n && n.withinMs} (gap=${GRACE_MS} should NOT appear)`);
    check('S3 fired near ceiling', n && n.t >= CEILING_MS - 300 && n.t < CEILING_MS + 900, `t=${n && n.t}`);
  }

  // S4: text_delta at 300ms then yield → visible event disarms both → no timeout notice.
  {
    const r = await runScenario({ idx: 4, name: 'S4 alive', model: 's4', collectMs: 1500,
      env: { RATLC_NO_VISIBLE_LIVENESS_GRACE_MS: String(GRACE_MS) },
      script: (send) => {
        setTimeout(() => send({ type: 'text_delta', text: 'hello world' }), 300);
        setTimeout(() => send({ type: 'yield' }), 600);
      } });
    const n = findTimeoutNotice(r.events);
    const gotText = r.events.some((e) => e.text.includes('hello world'));
    console.log('S4 alive (expect NO timeout notice, got text):', { notice: n, gotText });
    check('S4 streamed the real text', gotText, 'did not see text_delta content');
    check('S4 NO timeout notice (both timers disarmed)', !n, `unexpected notice t=${n && n.t} within=${n && n.withinMs}`);
  }

  // S5: grace=0 (disabled) + silence → only ceiling fires (~1500ms), NOT at grace.
  {
    const r = await runScenario({ idx: 5, name: 'S5 grace-disabled', model: 's5', collectMs: CEILING_MS + 1500,
      env: { RATLC_NO_VISIBLE_LIVENESS_GRACE_MS: '0' },
      script: () => { /* silence */ } });
    const n = findTimeoutNotice(r.events);
    console.log('S5 grace-disabled (expect ONLY ceiling ~%dms):', CEILING_MS, n);
    check('S5 notice fired', !!n, 'no proxy_notice seen');
    check('S5 reports CEILING window', n && n.withinMs === CEILING_MS, `withinMs=${n && n.withinMs}`);
    check('S5 did NOT fire early at grace', n && n.t >= CEILING_MS - 300, `t=${n && n.t} (fired too early → gap not disabled)`);
  }

  // ── Option C: upstream_abort retry (transient error before content) ──

  // S6: transient abort BEFORE content → retry on a fresh channel → success.
  {
    const r = await runScenario({ idx: 6, name: 'S6 abort-before-content', model: 's6', collectMs: 2500,
      env: { RATLC_RETRY_UPSTREAM_ABORT_MAX: '1', RATLC_RETRY_EMIT_NOTICE: '1' },
      script: (send, reqId, attempt) => {
        if (attempt === 1) {
          // first channel aborts before any content
          setTimeout(() => send({ type: 'error', message: 'Response error: aborted' }), 200);
        } else {
          // replay channel succeeds
          setTimeout(() => send({ type: 'text_delta', text: 'recovered after abort' }), 150);
          setTimeout(() => send({ type: 'yield' }), 400);
        }
      } });
    const gotText = r.events.some((e) => e.text.includes('recovered after abort'));
    const notice = r.events.some((e) => e.text.includes('[proxy_notice]') && e.text.includes('upstream_abort'));
    const empty = r.events.some((e) => e.text.includes('did not emit') || e.text.includes('api_error'));
    console.log('S6 abort-before-content (expect retry→recovered, %d pool reqs):', r.requestCount, { gotText, notice, empty });
    check('S6 retried on a fresh channel (2 pool requests)', r.requestCount === 2, `requestCount=${r.requestCount}`);
    check('S6 recovered text streamed', gotText, 'did not see recovered text');
    check('S6 emitted upstream_abort breadcrumb', notice, 'no upstream_abort notice');
    check('S6 no empty/error surfaced to client', !empty, 'an error/empty leaked to client');
  }

  // S7: abort AFTER partial content → NO retry; preserve shown output + notice.
  {
    const r = await runScenario({ idx: 7, name: 'S7 abort-after-content', model: 's7', collectMs: 2200,
      env: { RATLC_RETRY_UPSTREAM_ABORT_MAX: '1' },
      script: (send, reqId, attempt) => {
        setTimeout(() => send({ type: 'text_delta', text: 'partial answer so far' }), 150);
        setTimeout(() => send({ type: 'error', message: 'Response error: aborted' }), 400);
      } });
    const gotPartial = r.events.some((e) => e.text.includes('partial answer so far'));
    const dropNotice = r.events.some((e) => e.text.includes('Upstream connection dropped mid-response'));
    console.log('S7 abort-after-content (expect NO retry, preserve+notice, %d pool reqs):', r.requestCount, { gotPartial, dropNotice });
    check('S7 did NOT retry (1 pool request)', r.requestCount === 1, `requestCount=${r.requestCount} (after-content must not retry)`);
    check('S7 preserved shown output', gotPartial, 'lost the partial answer');
    check('S7 emitted dropped-mid-response notice', dropNotice, 'no preserve-output notice');
  }

  // S8: persistent error (rate-limit) before content → NO retry.
  {
    const r = await runScenario({ idx: 8, name: 'S8 persistent-no-retry', model: 's8', collectMs: 2000,
      env: { RATLC_RETRY_UPSTREAM_ABORT_MAX: '1' },
      script: (send) => { setTimeout(() => send({ type: 'error', message: "Connect error resource_exhausted: You've reached the rate limit" }), 200); } });
    console.log('S8 persistent-no-retry (expect NO retry, %d pool reqs):', r.requestCount);
    check('S8 did NOT retry a persistent error (1 pool request)', r.requestCount === 1, `requestCount=${r.requestCount} (rate-limit must not retry)`);
  }

  // ── continue_after_abort: self-driven continuation of a mid-text abort ──

  // S9: abort mid-text → continuation streams into the SAME message, no notice.
  {
    const r = await runScenario({ idx: 9, name: 'S9 continue-after-abort', model: 's9', collectMs: 3000,
      env: { RATLC_RETRY_CONTINUE_AFTER_ABORT_MAX: '2', RATLC_RETRY_UPSTREAM_ABORT_MAX: '0' },
      script: (send, reqId, attempt) => {
        if (attempt === 1) {
          setTimeout(() => send({ type: 'text_delta', text: 'The answer is ' }), 150);
          setTimeout(() => send({ type: 'error', message: 'Response error: aborted' }), 350);
        } else {
          setTimeout(() => send({ type: 'text_delta', text: 'forty-two.' }), 150);
          setTimeout(() => send({ type: 'yield' }), 400);
        }
      } });
    const full = r.events.map((e) => e.text).join('');
    const gotPartial = full.includes('The answer is ');
    const gotContinuation = full.includes('forty-two.');
    const notice = full.includes('Send a new message to continue');
    console.log('S9 continue-after-abort (expect both texts, no notice, %d pool reqs):', r.requestCount, { gotPartial, gotContinuation, notice });
    check('S9 self-drove a continuation (2 pool requests)', r.requestCount === 2, `requestCount=${r.requestCount}`);
    check('S9 preserved the partial text', gotPartial, 'lost the partial');
    check('S9 streamed the continuation into the same message', gotContinuation, 'no continuation text');
    check('S9 did NOT show the "send a new message" notice', !notice, 'dead-end notice leaked');
  }

  // S10: continuation budget exhausted → falls back to the notice.
  {
    const r = await runScenario({ idx: 10, name: 'S10 continue-budget-exhausted', model: 's10', collectMs: 3000,
      env: { RATLC_RETRY_CONTINUE_AFTER_ABORT_MAX: '1', RATLC_RETRY_UPSTREAM_ABORT_MAX: '0' },
      script: (send, reqId, attempt) => {
        // every attempt aborts mid-text → after 1 continuation, budget is spent
        setTimeout(() => send({ type: 'text_delta', text: `chunk${attempt} ` }), 150);
        setTimeout(() => send({ type: 'error', message: 'Response error: aborted' }), 350);
      } });
    const full = r.events.map((e) => e.text).join('');
    const notice = full.includes('Send a new message to continue');
    console.log('S10 continue-budget-exhausted (expect 2 reqs then notice):', r.requestCount, { notice });
    check('S10 tried exactly one continuation (2 pool requests)', r.requestCount === 2, `requestCount=${r.requestCount}`);
    check('S10 preserved both partials', full.includes('chunk1 ') && full.includes('chunk2 '), `full=${JSON.stringify(full).slice(0,120)}`);
    check('S10 fell back to the notice when budget exhausted', notice, 'no fallback notice');
  }

  // ── empty_assistant_turn: breadcrumb must NOT cap retries at 1 ──

  // S11: empty, empty, then content → must retry TWICE (the breadcrumb emitted
  // on retry #1 used to fool the empty-check and block retry #2). requestCount
  // === 3 proves the fix; without it the 2nd empty would not retry (==2).
  {
    const r = await runScenario({ idx: 11, name: 'S11 empty-retries-until-content', model: 's11', collectMs: 4000,
      env: { RATLC_RETRY_EMPTY_TURN_MAX: '3', RATLC_RETRY_EMIT_NOTICE: '1' },
      script: (send, reqId, attempt) => {
        if (attempt < 3) send({ type: 'yield' });               // empty turn
        else { setTimeout(() => send({ type: 'text_delta', text: 'finally answered' }), 100); setTimeout(() => send({ type: 'yield' }), 300); }
      } });
    const full = r.events.map((e) => e.text).join('');
    console.log('S11 empty-retries-until-content (expect 3 reqs, real text):', r.requestCount, { gotText: full.includes('finally answered') });
    check('S11 retried TWICE despite the breadcrumb (3 pool requests)', r.requestCount === 3, `requestCount=${r.requestCount} (==2 means breadcrumb still caps at 1)`);
    check('S11 eventually streamed real content', full.includes('finally answered'), 'no real content');
    check('S11 no misleading "retries exhausted" (it succeeded)', !full.includes('auto-retry exhausted'), 'wrongly reported exhaustion');
  }

  // S12: persistent empty → budget exhausts → CLEAR message, not a bare breadcrumb.
  {
    const r = await runScenario({ idx: 12, name: 'S12 empty-exhausted', model: 's12', collectMs: 4000,
      env: { RATLC_RETRY_EMPTY_TURN_MAX: '2', RATLC_RETRY_EMIT_NOTICE: '1' },
      script: (send) => send({ type: 'yield' }) });  // every attempt is empty
    const full = r.events.map((e) => e.text).join('');
    console.log('S12 empty-exhausted (expect 3 reqs + clear exhaustion msg):', r.requestCount, { exhausted: full.includes('auto-retry exhausted') });
    check('S12 used the full budget (3 pool requests = 1 + 2 retries)', r.requestCount === 3, `requestCount=${r.requestCount}`);
    check('S12 showed a CLEAR exhaustion message', full.includes('auto-retry exhausted') && full.includes('send your message again'), `full=${JSON.stringify(full).slice(0,160)}`);
  }

  // S13: thinking frames keep the channel alive (Workstream C). A thinking_delta
  // RESETS the liveness gap AND retires the absolute ceiling — so a post-thinking
  // silence fires at ~grace after the LAST thinking frame, reporting the GAP
  // window (not the ceiling). Forwarding is off here, so the gap-fire falls
  // through to the standard silent notice (SILENT_MAX=0).
  {
    const r = await runScenario({ idx: 13, name: 'S13 thinking-keeps-alive', model: 's13', collectMs: 2600,
      env: { RATLC_NO_VISIBLE_LIVENESS_GRACE_MS: String(GRACE_MS) },
      script: (send) => { for (const t of [100, 400, 700, 1000]) setTimeout(() => send({ type: 'thinking_delta', text: 'reasoning… ' }), t); } });
    const n = findTimeoutNotice(r.events);
    console.log('S13 thinking-keeps-alive (expect gap ~%dms = 1000+grace):', 1000 + GRACE_MS, n);
    check('S13 notice fired', !!n, 'no proxy_notice seen');
    check('S13 reports GAP window (ceiling retired by thinking)', n && n.withinMs === GRACE_MS, `withinMs=${n && n.withinMs} (CEILING ${CEILING_MS} would mean thinking did not reset the gap)`);
    check('S13 gap RESET by thinking (fired after last frame+grace, not earlier/ceiling)', n && n.t >= 1000 + GRACE_MS - 200 && n.t < CEILING_MS, `t=${n && n.t}`);
  }

  console.log('');
  if (failures === 0) { console.log('liveness-watchdog-test: OK'); process.exit(0); }
  else { console.log(`liveness-watchdog-test: FAIL (${failures} checks)`); process.exit(1); }
}

main().catch((e) => { console.error('test harness error:', e); process.exit(2); });
