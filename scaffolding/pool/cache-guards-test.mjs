#!/usr/bin/env node
// cache-guards-test.mjs
//
// Exercises the lightweight RATLC cache/session guards without touching
// Cursor:
//   1. default-group queue timeout returns a normal pool error instead of
//      hanging forever when no channel can become ready.
//   2. consumed tool_use ids are retained briefly so duplicate/replayed
//      tool_result requests can be recognized distinctly from unknown ids.
//   3. hybrid session affinity keeps stable continuations on the same ready
//      channel.

import net from 'node:net';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const MGR = path.join(__dirname, 'pool-manager.mjs');

let seq = 0;
const failures = [];

function pass(msg) { console.log(`  OK ${msg}`); }
function fail(msg) { console.log(`  FAIL ${msg}`); failures.push(msg); }
function assert(cond, msg) { (cond ? pass : fail)(msg); }
function step(label) { seq++; console.log(`\nSTEP ${seq}: ${label}`); }

function tempName(label) {
  return `/tmp/ratlc-${label}-${process.pid}-${Date.now()}`;
}

async function startPool(envExtra = {}, waitMs = 200) {
  const sock = tempName('cache-guards') + '.sock';
  const log = tempName('cache-guards') + '.log';
  const env = {
    ...process.env,
    POOL_SOCK: sock,
    POOL_MODEL: 'mock-default-model',
    POOL_SIZE: '1',
    POOL_TEST_MOCK_CHANNELS: '1',
    POOL_TOOL_MODE: 'translate',
    POOL_BRIDGE_PROTOCOL: 'h1',
    POOL_CONTEXT_MODE: 'full',
    POOL_CONCURRENT_OPENS: '1',
    RATLC_QUEUE_TIMEOUT_MS: '250',
    RATLC_CONSUMED_TOOL_TTL_MS: '60000',
    ...envExtra,
  };
  const fd = fs.openSync(log, 'a');
  const proc = spawn('node', [MGR], { env, stdio: ['ignore', fd, fd] });
  await new Promise((r) => setTimeout(r, waitMs));
  return { proc, sock, log };
}

async function stopPool(pool) {
  if (!pool?.proc) return;
  try { pool.proc.kill('SIGTERM'); } catch {}
  await new Promise((r) => setTimeout(r, 250));
  try { pool.proc.kill('SIGKILL'); } catch {}
  try { fs.unlinkSync(pool.sock); } catch {}
}

function streamRequest(sockPath, obj, timeoutMs = 2000, terminal = (m) => m.type === 'yield' || m.type === 'error') {
  return new Promise((resolve, reject) => {
    const events = [];
    const sock = net.createConnection(sockPath);
    let buf = '';
    const t = setTimeout(() => {
      try { sock.destroy(); } catch {}
      resolve(events);
    }, timeoutMs);
    sock.on('connect', () => sock.write(JSON.stringify(obj) + '\n'));
    sock.on('data', (c) => {
      buf += c.toString();
      let idx;
      while ((idx = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, idx);
        buf = buf.slice(idx + 1);
        if (!line.trim()) continue;
        try {
          const m = JSON.parse(line);
          events.push(m);
          if (terminal(m)) {
            clearTimeout(t);
            sock.end();
            return resolve(events);
          }
        } catch {}
      }
    });
    sock.on('error', (e) => { clearTimeout(t); reject(e); });
  });
}

async function waitForReady(sockPath, timeoutMs = 2000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const events = await streamRequest(
      sockPath,
      { type: 'status' },
      500,
      (m) => m.type === 'status',
    );
    const snap = events.find((e) => e.type === 'status');
    if (snap?.pool?.readyCount > 0) return snap;
    await new Promise((r) => setTimeout(r, 50));
  }
  return null;
}

(async () => {
  step('queue timeout on default group with no ready channels');
  {
    const pool = await startPool({ MOCK_READY_DELAY_MS: '5000', RATLC_QUEUE_TIMEOUT_MS: '200' }, 200);
    try {
      const started = Date.now();
      const events = await streamRequest(pool.sock, {
        type: 'request',
        requestId: 'req-queue-timeout',
        action: 'send_user_message',
        model: 'mock-default-model',
        text: 'hello',
        system: '',
        tools: [],
      }, 1500);
      const err = events.find((e) => e.type === 'error');
      assert(!!err, 'received queue timeout error');
      assert(/no ready RATLC channel/.test(err?.message || ''), `error message is specific (${err?.message})`);
      const elapsed = Date.now() - started;
      assert(elapsed >= 150 && elapsed < 1200, `timeout elapsed is bounded (${elapsed}ms)`);
    } finally {
      await stopPool(pool);
    }
  }

  step('consumed tool_use id is recognized on duplicate tool_result');
  {
    const pool = await startPool({}, 300);
    try {
      const ready = await waitForReady(pool.sock);
      assert(!!ready, 'mock channel ready');

      const toolEvents = await streamRequest(pool.sock, {
        type: 'request',
        requestId: 'req-tool-use',
        action: 'send_user_message',
        model: 'mock-default-model',
        text: '__MOCK_TOOL_USE__',
        system: '',
        tools: [],
      }, 2000, (m) => m.type === 'tool_use');
      const tool = toolEvents.find((e) => e.type === 'tool_use');
      assert(!!tool?.anthropic_id, `received tool_use (${tool?.anthropic_id || 'none'})`);

      const first = await streamRequest(pool.sock, {
        type: 'request',
        requestId: 'req-tool-result-1',
        action: 'send_tool_result',
        model: 'mock-default-model',
        anthropic_tool_use_id: tool.anthropic_id,
        content: 'first result',
      }, 2000);
      assert(first.some((e) => e.type === 'yield'), 'first tool_result is accepted');

      const second = await streamRequest(pool.sock, {
        type: 'request',
        requestId: 'req-tool-result-2',
        action: 'send_tool_result',
        model: 'mock-default-model',
        anthropic_tool_use_id: tool.anthropic_id,
        content: 'duplicate result',
      }, 2000);
      const dup = second.find((e) => e.type === 'error');
      assert(!!dup, 'duplicate tool_result returns an error event');
      assert(/already consumed anthropic_tool_use_id/.test(dup?.message || ''), `duplicate error is classified (${dup?.message})`);

      const statusEvents = await streamRequest(pool.sock, { type: 'status' }, 500, (m) => m.type === 'status');
      const status = statusEvents.find((e) => e.type === 'status');
      assert(status?.pool?.consumedToolUseIndex >= 1, `status exposes consumedToolUseIndex=${status?.pool?.consumedToolUseIndex}`);
    } finally {
      await stopPool(pool);
    }
  }

  step('hybrid session affinity prefers the same channel');
  {
    const pool = await startPool({ POOL_SIZE: '2', POOL_CONTEXT_MODE: 'hybrid', POOL_CONCURRENT_OPENS: '2' }, 400);
    try {
      const ready = await waitForReady(pool.sock);
      assert(!!ready && ready.pool.readyCount >= 1, `mock pool readyCount=${ready?.pool?.readyCount}`);

      const sessionKey = 'sid:mock-default-model:test-session';
      const first = await streamRequest(pool.sock, {
        type: 'request',
        requestId: 'req-hybrid-1',
        action: 'send_user_message',
        model: 'mock-default-model',
        sessionKey,
        contextMode: 'full',
        hybridReason: 'new-session',
        text: '=== FULL CONVERSATION CONTEXT ===\n[user] (RESPOND TO THIS):\nfirst',
        system: '',
        tools: [],
      }, 2000);
      const firstRoute = first.find((e) => e.type === 'route_decision');
      assert(!!firstRoute?.channelId, `first routed to ${firstRoute?.channelId || 'none'}`);

      const second = await streamRequest(pool.sock, {
        type: 'request',
        requestId: 'req-hybrid-2',
        action: 'send_user_message',
        model: 'mock-default-model',
        sessionKey,
        contextMode: 'last',
        hybridReason: 'stable-session',
        text: 'second',
        system: '',
        tools: [],
      }, 2000);
      const secondRoute = second.find((e) => e.type === 'route_decision');
      assert(!!secondRoute?.channelId, `second routed to ${secondRoute?.channelId || 'none'}`);
      assert(secondRoute?.channelId === firstRoute?.channelId,
        `second uses sticky channel (${firstRoute?.channelId} -> ${secondRoute?.channelId})`);

      const statusEvents = await streamRequest(pool.sock, { type: 'status' }, 500, (m) => m.type === 'status');
      const status = statusEvents.find((e) => e.type === 'status');
      assert(status?.pool?.sessionAffinity >= 1, `status exposes sessionAffinity=${status?.pool?.sessionAffinity}`);
    } finally {
      await stopPool(pool);
    }
  }

  if (failures.length) {
    console.error(`\nFAILURES (${failures.length}):`);
    for (const f of failures) console.error(`- ${f}`);
    process.exit(1);
  }
  console.log('\ncache-guards: all checks passed');
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
