#!/usr/bin/env node

import assert from 'node:assert/strict';
import fs from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, '../..');
const MGR = path.join(ROOT, 'scaffolding/pool/pool-manager.mjs');
const SOCK = `/tmp/ratlc-partial-tools-${process.pid}.sock`;
const LOG = `/tmp/ratlc-partial-tools-${process.pid}.log`;

let mgrProc = null;

async function startPool() {
  try { fs.unlinkSync(SOCK); } catch {}
  try { fs.unlinkSync(LOG); } catch {}
  const env = {
    ...process.env,
    POOL_SOCK: SOCK,
    POOL_MODEL: 'mock-default-model',
    POOL_SIZE: '1',
    POOL_TEST_MOCK_CHANNELS: '1',
    POOL_GROUP_WAIT_MS: '100',
    POOL_CONCURRENT_OPENS: '1',
    POOL_TOOL_MODE: 'translate',
    POOL_BRIDGE_PROTOCOL: 'h1',
    POOL_CONTEXT_MODE: 'full',
  };
  const fd = fs.openSync(LOG, 'a');
  mgrProc = spawn('node', [MGR], { env, stdio: ['ignore', fd, fd] });
  await new Promise((resolve) => setTimeout(resolve, 700));
}

async function stopPool() {
  if (!mgrProc) return;
  try { mgrProc.kill('SIGTERM'); } catch {}
  await new Promise((resolve) => setTimeout(resolve, 200));
  try { mgrProc.kill('SIGKILL'); } catch {}
  try { fs.unlinkSync(SOCK); } catch {}
  mgrProc = null;
}

function streamRequest(obj, terminal, timeoutMs = 4000) {
  return new Promise((resolve, reject) => {
    const events = [];
    const sock = net.createConnection(SOCK);
    let buf = '';
    const timer = setTimeout(() => {
      try { sock.destroy(); } catch {}
      resolve(events);
    }, timeoutMs);
    sock.on('connect', () => sock.write(JSON.stringify(obj) + '\n'));
    sock.on('data', (chunk) => {
      buf += chunk.toString('utf8');
      let idx;
      while ((idx = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, idx);
        buf = buf.slice(idx + 1);
        if (!line.trim()) continue;
        const msg = JSON.parse(line);
        events.push(msg);
        if (terminal(msg, events)) {
          clearTimeout(timer);
          sock.end();
          resolve(events);
          return;
        }
      }
    });
    sock.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });
}

function oneShot(obj, timeoutMs = 4000) {
  return new Promise((resolve, reject) => {
    const sock = net.createConnection(SOCK);
    let buf = '';
    const timer = setTimeout(() => {
      try { sock.destroy(); } catch {}
      reject(new Error('timeout'));
    }, timeoutMs);
    sock.on('connect', () => sock.write(JSON.stringify(obj) + '\n'));
    sock.on('data', (chunk) => {
      buf += chunk.toString('utf8');
      const idx = buf.indexOf('\n');
      if (idx === -1) return;
      clearTimeout(timer);
      sock.end();
      resolve(JSON.parse(buf.slice(0, idx)));
    });
    sock.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });
}

try {
  await startPool();

  const firstEvents = await streamRequest({
    type: 'request',
    requestId: 'req-two-tools',
    action: 'send_user_message',
    model: 'mock-default-model',
    text: '__MOCK_TWO_TOOL_USES__',
    system: '',
    tools: [],
  }, (_msg, events) => events.filter((e) => e.type === 'tool_use').length >= 2);

  const toolUses = firstEvents.filter((e) => e.type === 'tool_use');
  assert.equal(toolUses.length, 2, 'mock worker should emit two tool_use events');
  assert.notEqual(toolUses[0].anthropic_id, toolUses[1].anthropic_id, 'tool_use ids should be distinct');

  let status = await oneShot({ type: 'status' });
  assert.equal(status.pool.toolUseIndex, 2, 'manager tracks both pending tool uses');
  assert.equal(status.pool.pendingToolUseChannels, 1, 'one channel has pending tool uses');

  const secondEvents = await streamRequest({
    type: 'request',
    requestId: 'req-one-result',
    action: 'send_tool_results',
    model: 'mock-default-model',
    results: [{
      anthropic_tool_use_id: toolUses[0].anthropic_id,
      content: 'only first result returned by client',
    }],
  }, (msg) => msg.type === 'tool_use');

  const lateToolUses = secondEvents.filter((e) => e.type === 'tool_use');
  assert.equal(lateToolUses.length, 1, 'partial result request should expose the missing tool_use');
  assert.equal(lateToolUses[0].anthropic_id, toolUses[1].anthropic_id, 'missing tool_use id is re-emitted');
  assert.equal(lateToolUses[0].late, true, 'missing tool_use is marked late for diagnostics');

  status = await oneShot({ type: 'status' });
  assert.equal(status.pool.toolUseIndex, 2, 'pending tool uses remain held until the missing result returns');
  assert.equal(status.pool.pendingToolUseChannels, 1, 'pending channel remains tracked while holding partial results');

  const thirdEvents = await streamRequest({
    type: 'request',
    requestId: 'req-second-result',
    action: 'send_tool_results',
    model: 'mock-default-model',
    results: [{
      anthropic_tool_use_id: toolUses[1].anthropic_id,
      content: 'second result returned after late re-emit',
    }],
  }, (msg) => msg.type === 'yield');

  assert(thirdEvents.some((e) => e.type === 'route_decision'), 'complete result request should be routed');
  assert(thirdEvents.some((e) => e.type === 'yield'), 'worker should yield after receiving the complete held batch');

  const logBeforeStatus = fs.readFileSync(LOG, 'utf8');
  assert.match(logBeforeStatus, /routing 2 result\(s\)/, 'held first result and later second result are routed as one complete batch');
  assert.doesNotMatch(logBeforeStatus, /routing 1 result\(s\)/, 'manager must not dispatch partial result subsets upstream');

  status = await oneShot({ type: 'status' });
  assert.equal(status.pool.toolUseIndex, 0, 'all pending tool uses are consumed after complete batch dispatch');
  assert.equal(status.pool.pendingToolUseChannels, 0, 'pending channel set is cleared after complete batch dispatch');

  const log = fs.readFileSync(LOG, 'utf8');
  assert.match(log, /hold partial tool_result batch/, 'pool log records the partial batch hold');

  console.log('partial-tool-results-test: OK');
} finally {
  await stopPool();
}
