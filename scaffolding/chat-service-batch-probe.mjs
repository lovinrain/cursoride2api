#!/usr/bin/env node
// Batch probe: systematically test ChatService auth bypass vectors.
//
// Tests multiple combinations of:
//   - RPC paths (StreamUnifiedChatWithTools, Idempotent variants)
//   - Request fields (isChat, isHeadless, isBackgroundComposer, isAgentic, ...)
//   - maxMode, model, forceIsNotDev, enableYoloMode
//
// Each probe opens a fresh H2 connection, sends one Connect-proto frame,
// reads the response, and classifies the result.
//
// Usage:
//   node scaffolding/chat-service-batch-probe.mjs
//   node scaffolding/chat-service-batch-probe.mjs --delay=2000 --timeout=15000

import crypto from 'node:crypto';
import fs from 'node:fs';
import http2 from 'node:http2';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { create, toBinary, fromBinary } from '@bufbuild/protobuf';
import { v4 as uuidv4 } from 'uuid';

const REPO_ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const BASE_URL = process.env.CURSOR_API_BASE_URL || 'https://api2.cursor.sh';
const CLIENT_VERSION = process.env.CURSOR_CLIENT_VERSION || '2.6.20';
const CLIENT_COMMIT = process.env.CURSOR_COMMIT || 'd5c0e77a0214208f36b56d42e8e787de88d02ea4';
const CHAT_PROTO_CACHE = process.env.CURSOR_CHAT_PROTO_CACHE || '/tmp/cursoride2api-chat-probe/chat_pb.mjs';

function parseArgs(argv) {
  const out = { delay: 1500, timeout: 20000 };
  for (const a of argv.slice(2)) {
    const m = /^--([^=]+)=(.*)$/.exec(a);
    if (m) out[m[1]] = m[2];
  }
  out.delay = Number(out.delay);
  out.timeout = Number(out.timeout);
  return out;
}

function loadToken() {
  const p = path.resolve(REPO_ROOT, 'token.json');
  const j = JSON.parse(fs.readFileSync(p, 'utf8'));
  const tok = (j.tokens && j.tokens[0]) || j;
  if (!tok.accessToken) throw new Error('token.json missing accessToken');
  return tok;
}

function generateChecksum(machineId, macMachineId) {
  let k = 165;
  const t = Math.floor(Date.now() / 1e6);
  const b = new Uint8Array([(t >> 40) & 255, (t >> 32) & 255, (t >> 24) & 255, (t >> 16) & 255, (t >> 8) & 255, t & 255]);
  for (let i = 0; i < b.length; i++) { b[i] = ((b[i] ^ k) + (i % 256)) & 0xff; k = b[i]; }
  const prefix = Buffer.from(b).toString('base64');
  return macMachineId ? `${prefix}${machineId}/${macMachineId}` : `${prefix}${machineId}`;
}

function encodeConnectFrame(payload, flags = 0) {
  const frame = Buffer.alloc(5 + payload.length);
  frame[0] = flags;
  frame.writeUInt32BE(payload.length, 1);
  payload.copy(frame, 5);
  return frame;
}

function cursorOs() {
  if (process.platform === 'darwin') return 'darwin';
  if (process.platform === 'win32') return 'windows_nt';
  return 'linux';
}

function buildHeaders(token, rpcPath) {
  return {
    ':method': 'POST',
    ':path': rpcPath,
    'content-type': 'application/connect+proto',
    'connect-protocol-version': '1',
    te: 'trailers',
    authorization: `Bearer ${token.accessToken}`,
    'x-cursor-checksum': generateChecksum(token.machineId || '', token.macMachineId || ''),
    'x-cursor-client-version': CLIENT_VERSION,
    'x-cursor-timezone': 'UTC',
    'x-request-id': uuidv4(),
    'x-session-id': uuidv4(),
    'x-ghost-mode': 'false',
    'x-cursor-client-type': 'ide',
    'x-cursor-client-os': cursorOs(),
    'x-cursor-client-arch': process.arch === 'x64' ? 'x64' : process.arch,
    'x-cursor-client-device-type': 'desktop',
    'x-cursor-client-os-version': os.release(),
    'x-cursor-commit': CLIENT_COMMIT,
    'x-client-key': crypto.randomBytes(32).toString('hex'),
  };
}

function parseConnectFrames(buffer) {
  const frames = [];
  let offset = 0;
  while (buffer.length - offset >= 5) {
    const flags = buffer[offset];
    const len = buffer.readUInt32BE(offset + 1);
    if (buffer.length - offset < 5 + len) break;
    frames.push({ flags, payload: buffer.subarray(offset + 5, offset + 5 + len) });
    offset += 5 + len;
  }
  return { frames, rest: buffer.subarray(offset) };
}

function classify(text, statusCode) {
  if (/unpaid invoice/i.test(text)) return 'BILLING_BLOCKED';
  if (/ERROR_RATE_LIMITED|rate.?limit|resource_exhausted/i.test(text)) return 'RATE_LIMITED';
  if (/not authorized to use cloud agents|ERROR_UNAUTHORIZED/i.test(text)) return 'CLOUD_AGENTS_UNAUTH';
  if (/unauthenticated/i.test(text)) return 'UNAUTHENTICATED';
  if (/permission|entitlement/i.test(text)) return 'ENTITLEMENT_ERR';
  if (/Max Mode Required/i.test(text)) return 'MAX_MODE_REQ';
  if (/invalid|parse|decode|malformed|missing/i.test(text)) return 'SHAPE_ERROR';
  if (/not found|no such|unknown|unimplemented/i.test(text)) return 'NOT_FOUND';
  if (/TIMEOUT/i.test(text)) return 'TIMEOUT';
  if (statusCode && statusCode >= 400 && statusCode < 500) return `HTTP_${statusCode}`;
  if (statusCode && statusCode >= 500) return `HTTP_${statusCode}`;
  if (text.includes('[decoded]') || text.includes('thinking=')) return 'SUCCESS_STREAM';
  if (text.includes('[frame') && statusCode === 200) return 'RESPONSE_200';
  if (text.trim()) return 'OTHER_RESPONSE';
  return 'NO_PAYLOAD';
}

async function loadChatProto() {
  return import(pathToFileURL(CHAT_PROTO_CACHE).href);
}

// ── Probe variants ───────────────────────────────────────────────────

function defineVariants(chat) {
  const MT = chat.ConversationMessage_MessageType;
  const baseModel = 'claude-4.6-opus-max-thinking-fast';
  const prompt = 'Reply with exactly: "probe-ok"';

  function makeUser() {
    return create(chat.ConversationMessageSchema, { text: prompt, type: MT.HUMAN, bubbleId: uuidv4() });
  }
  function makeModel(model, maxMode = true) {
    return create(chat.ModelDetailsSchema, { modelName: model, maxMode });
  }
  function wrap(inner) {
    const w = create(chat.StreamUnifiedChatRequestWithToolsSchema, { streamUnifiedChatRequest: inner });
    return Buffer.from(toBinary(chat.StreamUnifiedChatRequestWithToolsSchema, w));
  }
  function wrapIdempotent(inner) {
    const w = create(chat.StreamUnifiedChatRequestWithToolsSchema, { streamUnifiedChatRequest: inner });
    const idem = create(chat.StreamUnifiedChatRequestWithToolsIdempotentSchema, { streamUnifiedChatRequestWithTools: w });
    return Buffer.from(toBinary(chat.StreamUnifiedChatRequestWithToolsIdempotentSchema, idem));
  }

  const variants = [];
  const RPC = '/aiserver.v1.ChatService/StreamUnifiedChatWithTools';

  // Group 1: isBackgroundComposer × isHeadless matrix
  for (const bg of [undefined, false, true]) {
    for (const hl of [undefined, false, true]) {
      variants.push({
        id: `bg${bg ?? 'U'}_hl${hl ?? 'U'}`,
        label: `bgComposer=${bg ?? 'unset'} headless=${hl ?? 'unset'}`,
        rpcPath: RPC,
        build: () => {
          const f = { conversation: [makeUser()], modelDetails: makeModel(baseModel), conversationId: uuidv4(), isChat: true };
          if (bg !== undefined) f.isBackgroundComposer = bg;
          if (hl !== undefined) f.isHeadless = hl;
          return wrap(create(chat.StreamUnifiedChatRequestSchema, f));
        },
      });
    }
  }

  // Group 2: isAgentic combos
  for (const ag of [false, true]) {
    variants.push({
      id: `agentic_${ag}`,
      label: `isAgentic=${ag} chat=false bg=false hl=false`,
      rpcPath: RPC,
      build: () => wrap(create(chat.StreamUnifiedChatRequestSchema, {
        conversation: [makeUser()], modelDetails: makeModel(baseModel), conversationId: uuidv4(),
        isChat: false, isHeadless: false, isBackgroundComposer: false, isAgentic: ag,
      })),
    });
  }

  // Group 3: Minimal request
  variants.push({
    id: 'minimal',
    label: 'minimal: conv + model + convId only',
    rpcPath: RPC,
    build: () => wrap(create(chat.StreamUnifiedChatRequestSchema, {
      conversation: [makeUser()], modelDetails: makeModel(baseModel), conversationId: uuidv4(),
    })),
  });

  // Group 4: maxMode=false
  variants.push({
    id: 'no_maxmode',
    label: 'maxMode=false bg=false hl=false',
    rpcPath: RPC,
    build: () => wrap(create(chat.StreamUnifiedChatRequestSchema, {
      conversation: [makeUser()], modelDetails: makeModel(baseModel, false), conversationId: uuidv4(),
      isChat: true, isHeadless: false, isBackgroundComposer: false,
    })),
  });

  // Group 5: Smaller models
  for (const model of ['claude-4.6-sonnet-medium', 'claude-4.5-sonnet', 'composer-2-fast']) {
    variants.push({
      id: `model_${model.replace(/[^a-z0-9]/g, '_')}`,
      label: `model=${model} bg=false hl=false`,
      rpcPath: RPC,
      build: () => wrap(create(chat.StreamUnifiedChatRequestSchema, {
        conversation: [makeUser()], modelDetails: makeModel(model, false), conversationId: uuidv4(),
        isChat: true, isHeadless: false, isBackgroundComposer: false,
      })),
    });
  }

  // Group 6: Special flags
  for (const [flag, desc] of [['forceIsNotDev', 'forceIsNotDev=true'], ['enableYoloMode', 'yoloMode=true'], ['useUnifiedChatPrompt', 'unifiedPrompt=true']]) {
    variants.push({
      id: `flag_${flag}`,
      label: `${desc} bg=false hl=false`,
      rpcPath: RPC,
      build: () => {
        const f = { conversation: [makeUser()], modelDetails: makeModel(baseModel), conversationId: uuidv4(), isChat: true, isHeadless: false, isBackgroundComposer: false };
        f[flag] = true;
        return wrap(create(chat.StreamUnifiedChatRequestSchema, f));
      },
    });
  }

  // Group 7: Empty frame
  variants.push({
    id: 'empty_frame',
    label: 'empty Connect-proto frame',
    rpcPath: RPC,
    build: () => Buffer.alloc(0),
  });

  // Group 8: Idempotent RPC paths
  for (const suffix of ['', 'SSE', 'Poll']) {
    const rpc = `/aiserver.v1.ChatService/StreamUnifiedChatWithToolsIdempotent${suffix}`;
    variants.push({
      id: `idempotent${suffix || '_base'}`,
      label: `Idempotent${suffix} bg=false hl=false`,
      rpcPath: rpc,
      build: () => {
        const inner = create(chat.StreamUnifiedChatRequestSchema, {
          conversation: [makeUser()], modelDetails: makeModel(baseModel), conversationId: uuidv4(),
          isChat: true, isHeadless: false, isBackgroundComposer: false,
        });
        return wrapIdempotent(inner);
      },
    });
  }

  // Group 9: AgentService RunSSE (thinking field check baseline)
  variants.push({
    id: 'agent_runsse_baseline',
    label: 'AgentService/RunSSE (baseline - should work)',
    rpcPath: '/agent.v1.AgentService/RunSSE',
    build: () => {
      const inner = create(chat.StreamUnifiedChatRequestSchema, {
        conversation: [makeUser()], modelDetails: makeModel(baseModel), conversationId: uuidv4(),
        isChat: true, isHeadless: false,
      });
      return wrap(inner);
    },
  });

  return variants;
}

// ── Single probe execution ───────────────────────────────────────────

function runSingleProbe(token, variant, timeoutMs, chat) {
  return new Promise((resolve) => {
    const headers = buildHeaders(token, variant.rpcPath);
    const payload = variant.build();
    const body = encodeConnectFrame(payload);
    const startedAt = Date.now();
    let statusCode = null;
    let collected = '';
    let frameCount = 0;
    let resolved = false;

    function finish() {
      if (resolved) return;
      resolved = true;
      const ms = Date.now() - startedAt;
      const result = classify(collected, Number(statusCode || 0));
      resolve({ id: variant.id, label: variant.label, rpcPath: variant.rpcPath, statusCode, result, ms, collected: collected.slice(0, 800) });
    }

    const client = http2.connect(BASE_URL);
    client.unref();
    const timeout = setTimeout(() => { collected += '\n[TIMEOUT]'; try { client.close(); } catch {} finish(); }, timeoutMs);
    client.on('error', (e) => { collected += `\n[h2-error] ${e.message}`; clearTimeout(timeout); finish(); });

    const req = client.request(headers);
    req.on('response', (h) => { statusCode = h[':status']; });

    let buffer = Buffer.alloc(0);
    req.on('data', (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      const parsed = parseConnectFrames(buffer);
      buffer = parsed.rest;
      for (const frame of parsed.frames) {
        frameCount++;
        const isEnd = (frame.flags & 0x02) !== 0;
        let summary;
        if (!isEnd && chat) {
          try {
            const resp = fromBinary(chat.StreamUnifiedChatResponseWithToolsSchema, new Uint8Array(frame.payload));
            const inner = resp.streamUnifiedChatResponse;
            if (inner) {
              const parts = ['[decoded]'];
              if (inner.text) parts.push(`text=${JSON.stringify(inner.text.slice(0, 80))}`);
              if (inner.thinking?.text) parts.push(`thinking=${inner.thinking.text.length}b`);
              if (inner.thinking?.signature) parts.push(`SIG=${inner.thinking.signature.length}b`);
              summary = parts.join(' ');
            }
          } catch {}
        }
        if (!summary) {
          const text = frame.payload.toString('utf8').replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/g, '.').slice(0, 300);
          summary = `[frame flags=0x${frame.flags.toString(16)} len=${frame.payload.length}] ${text}`;
        }
        collected += `\n${summary}`;
        if (frameCount >= 3) {
          clearTimeout(timeout);
          try { req.close(); client.close(); } catch {}
          finish();
          return;
        }
      }
    });

    req.on('trailers', (t) => { collected += `\n[trailers] ${JSON.stringify(t)}`; });
    req.on('end', () => { clearTimeout(timeout); try { client.close(); } catch {} finish(); });
    req.on('error', (e) => { collected += `\n[stream-error] ${e.message}`; clearTimeout(timeout); try { client.close(); } catch {} finish(); });
    req.write(body);
    req.end();
  });
}

// ── Main ─────────────────────────────────────────────────────────────

async function main() {
  const args = parseArgs(process.argv);
  const token = loadToken();
  const chat = await loadChatProto();
  const variants = defineVariants(chat);

  console.log(`\n${'='.repeat(72)}`);
  console.log(`  ChatService Auth Bypass Batch Probe`);
  console.log(`  ${variants.length} variants | delay=${args.delay}ms | timeout=${args.timeout}ms`);
  console.log(`${'='.repeat(72)}\n`);

  const results = [];

  for (let i = 0; i < variants.length; i++) {
    const v = variants[i];
    process.stdout.write(`[${String(i + 1).padStart(2)}/${variants.length}] ${v.id.padEnd(32)} `);
    const r = await runSingleProbe(token, v, args.timeout, chat);
    results.push(r);

    const icon = r.result === 'SUCCESS_STREAM' || r.result === 'RESPONSE_200' ? 'OK'
      : r.result === 'CLOUD_AGENTS_UNAUTH' ? 'LOCKED'
      : r.result === 'RATE_LIMITED' ? 'RATELIM'
      : r.result === 'NOT_FOUND' ? 'NOTFOUND'
      : r.result === 'TIMEOUT' ? 'TIMEOUT'
      : r.result.startsWith('HTTP_') ? r.result
      : 'ERR';
    console.log(`${icon.padEnd(12)} ${r.result} (${r.ms}ms) HTTP=${r.statusCode || '?'}`);

    if (r.result === 'SUCCESS_STREAM' || r.result === 'RESPONSE_200') {
      console.log(`    >>> BREAKTHROUGH: ${v.label}`);
      console.log(`    >>> ${r.collected.slice(0, 300)}`);
    }

    if (i < variants.length - 1) {
      await new Promise(r => setTimeout(r, args.delay));
    }
  }

  // Summary
  console.log(`\n${'='.repeat(72)}`);
  console.log('  RESULTS SUMMARY');
  console.log(`${'='.repeat(72)}`);

  const byResult = new Map();
  for (const r of results) {
    const list = byResult.get(r.result) || [];
    list.push(r);
    byResult.set(r.result, list);
  }

  for (const [result, items] of [...byResult.entries()].sort()) {
    console.log(`\n  ${result} (${items.length}):`);
    for (const item of items) {
      console.log(`    - ${item.id}: ${item.label}`);
    }
  }

  const hits = results.filter(r => r.result === 'SUCCESS_STREAM' || r.result === 'RESPONSE_200');
  if (hits.length > 0) {
    console.log(`\n${'='.repeat(72)}`);
    console.log(`  BREAKTHROUGHS: ${hits.length}`);
    console.log(`${'='.repeat(72)}`);
    for (const b of hits) {
      console.log(`\n  id:     ${b.id}`);
      console.log(`  label:  ${b.label}`);
      console.log(`  rpc:    ${b.rpcPath}`);
      console.log(`  http:   ${b.statusCode}`);
      console.log(`  body:   ${b.collected.slice(0, 400)}`);
    }
  }

  const nonStd = results.filter(r => r.result !== 'CLOUD_AGENTS_UNAUTH' && r.result !== 'SUCCESS_STREAM' && r.result !== 'RESPONSE_200');
  if (nonStd.length > 0) {
    console.log(`\n  Non-standard responses (investigate):`);
    for (const r of nonStd) {
      console.log(`    ${r.id}: ${r.result} | ${r.collected.slice(0, 150).replace(/\n/g, ' ')}`);
    }
  }

  const outPath = path.resolve(REPO_ROOT, 'scaffolding', 'chat-probe-results.json');
  fs.writeFileSync(outPath, JSON.stringify(results, null, 2), 'utf8');
  console.log(`\n  Results: ${outPath}`);
  console.log(`${'='.repeat(72)}\n`);
}

main().catch(e => { console.error(`Fatal: ${e.message}`); process.exit(1); });
