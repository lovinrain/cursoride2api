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
const API = path.join(ROOT, 'scaffolding/pool/api-server.mjs');
const SOCK = `/tmp/ratlc-api-image-${process.pid}.sock`;
const MGR_LOG = `/tmp/ratlc-api-image-mgr-${process.pid}.log`;
const API_LOG = `/tmp/ratlc-api-image-api-${process.pid}.log`;

let mgrProc = null;
let apiProc = null;

async function getFreePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const port = server.address().port;
      server.close(() => resolve(port));
    });
  });
}

async function waitFor(fn, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  let lastError;
  while (Date.now() < deadline) {
    try {
      const value = await fn();
      if (value) return value;
    } catch (e) {
      lastError = e;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw lastError || new Error(`timeout after ${timeoutMs}ms`);
}

async function startPool() {
  try { fs.unlinkSync(SOCK); } catch {}
  try { fs.unlinkSync(MGR_LOG); } catch {}
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
  const fd = fs.openSync(MGR_LOG, 'a');
  mgrProc = spawn('node', [MGR], { env, stdio: ['ignore', fd, fd] });
  await waitFor(() => fs.existsSync(SOCK), 5000);
  await waitFor(() => fs.existsSync(MGR_LOG) && fs.readFileSync(MGR_LOG, 'utf8').includes('READY'), 5000);
}

async function startApi(port) {
  try { fs.unlinkSync(API_LOG); } catch {}
  const env = {
    ...process.env,
    POOL_SOCK: SOCK,
    RATLC_API_HOST: '127.0.0.1',
    RATLC_API_PORT: String(port),
    POOL_CONTEXT_MODE: 'full',
    POOL_TOOL_MODE: 'translate',
    RATLC_NO_VISIBLE_EVENT_TIMEOUT_MS: '5000',
  };
  const fd = fs.openSync(API_LOG, 'a');
  apiProc = spawn('node', [API], { env, stdio: ['ignore', fd, fd] });
  await waitFor(async () => {
    const res = await fetch(`http://127.0.0.1:${port}/health`);
    return res.ok;
  }, 5000);
}

async function stopAll() {
  for (const proc of [apiProc, mgrProc]) {
    if (!proc) continue;
    try { proc.kill('SIGTERM'); } catch {}
  }
  await new Promise((resolve) => setTimeout(resolve, 200));
  for (const proc of [apiProc, mgrProc]) {
    if (!proc) continue;
    try { proc.kill('SIGKILL'); } catch {}
  }
  try { fs.unlinkSync(SOCK); } catch {}
}

try {
  await startPool();
  const port = await getFreePort();
  await startApi(port);

  const pngBase64 = Buffer.from('png-bytes').toString('base64');
  const res = await fetch(`http://127.0.0.1:${port}/v1/messages`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model: 'mock-default-model',
      max_tokens: 64,
      stream: true,
      messages: [{
        role: 'user',
        content: [
          { type: 'text', text: 'Describe this image briefly.' },
          { type: 'image', source: { type: 'base64', media_type: 'image/png', data: pngBase64 } },
        ],
      }],
    }),
  });

  assert.equal(res.status, 200);
  const sse = await res.text();
  assert.match(sse, /message_stop/, 'API response should complete');
  assert.match(sse, /\[mock [^\]]+\] ack/, 'mock pool response should be streamed');

  await new Promise((resolve) => setTimeout(resolve, 100));
  const apiLog = fs.readFileSync(API_LOG, 'utf8');
  const mgrLog = fs.readFileSync(MGR_LOG, 'utf8');

  assert.match(apiLog, /pool send_user_message .*images=1/, 'API should route image requests over send_user_message');
  assert.doesNotMatch(apiLog, /pool send_native_image_message/, 'API should not default image requests to native one-shot');
  assert.match(mgrLog, /routed action=send_user_message/, 'pool-manager should dispatch send_user_message');
  assert.doesNotMatch(mgrLog, /routed action=send_native_image_message/, 'pool-manager should not dispatch native image action');

  console.log('api-image-bajie-yield-routing-test: OK');
} finally {
  await stopAll();
}
