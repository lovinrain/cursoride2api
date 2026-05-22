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
const SOCK = `/tmp/ratlc-native-image-${process.pid}.sock`;
const LOG = `/tmp/ratlc-native-image-${process.pid}.log`;

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

try {
  await startPool();
  const events = await streamRequest({
    type: 'request',
    requestId: 'req-native-image',
    action: 'send_native_image_message',
    model: 'mock-default-model',
    text: 'describe this',
    content: {
      items: [
        { kind: 'text', text: 'describe this' },
        { kind: 'image', mediaType: 'image/png', dataBase64: Buffer.from('png-bytes').toString('base64') },
      ],
    },
    system: '',
    tools: [],
  }, (msg) => msg.type === 'yield');

  assert(events.some((e) => e.type === 'route_decision'), 'native image request should be routed');
  assert(events.some((e) => e.type === 'text_delta'), 'mock worker should answer native image request');
  assert(events.some((e) => e.type === 'yield'), 'native image request should finish');

  const log = fs.readFileSync(LOG, 'utf8');
  assert.match(log, /send_native_image_message/, 'pool log should show native image action dispatch');

  console.log('native-image-routing-test: OK');
} finally {
  await stopPool();
}
