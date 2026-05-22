#!/usr/bin/env node
// Probe Cursor's aiserver.v1.ChatService transport without changing the
// production 3001/4242 routes.
//
// Modes:
//   --mode=empty    Send one empty Connect-proto frame. Cheapest auth probe.
//   --mode=request  Build a minimal StreamUnifiedChatRequestWithTools using
//                   the historical chat_pb.mjs from commit 868aa95.

import { execFileSync } from 'node:child_process';
import crypto from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, symlinkSync, writeFileSync } from 'node:fs';
import http2 from 'node:http2';
import os from 'node:os';
import { dirname, resolve as pathResolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { create, toBinary } from '@bufbuild/protobuf';
import { fromBinary } from '@bufbuild/protobuf';
import { v4 as uuidv4 } from 'uuid';

const REPO_ROOT = pathResolve(dirname(new URL(import.meta.url).pathname), '..');
const CHAT_RPC_PATH = process.env.CURSOR_CHAT_RPC_PATH || '/aiserver.v1.ChatService/StreamUnifiedChatWithTools';
const BASE_URL = process.env.CURSOR_API_BASE_URL || 'https://api2.cursor.sh';
const CLIENT_VERSION = process.env.CURSOR_CLIENT_VERSION || '2.6.20';
const CLIENT_COMMIT = process.env.CURSOR_COMMIT || 'd5c0e77a0214208f36b56d42e8e787de88d02ea4';
const CHAT_PROTO_COMMIT = process.env.CURSOR_CHAT_PROTO_COMMIT || '868aa95';
const CHAT_PROTO_CACHE = process.env.CURSOR_CHAT_PROTO_CACHE || '/tmp/cursoride2api-chat-probe/chat_pb.mjs';

function parseArgs(argv) {
  const out = {
    mode: 'empty',
    prompt: 'Hello. Reply with one short sentence.',
    model: process.env.CURSOR_CHAT_PROBE_MODEL || 'claude-4.6-opus-max-thinking-fast',
    maxMode: process.env.CURSOR_CHAT_PROBE_MAX_MODE || 'true',
    timeoutMs: Number(process.env.CURSOR_CHAT_PROBE_TIMEOUT_MS || 30000),
  };
  for (const a of argv.slice(2)) {
    const m = /^--([^=]+)(?:=(.*))?$/.exec(a);
    if (!m) continue;
    out[m[1]] = m[2] ?? true;
  }
  if (!['empty', 'request'].includes(out.mode)) {
    throw new Error(`unsupported --mode=${out.mode}; use empty or request`);
  }
  return out;
}

function loadToken() {
  const p = pathResolve(REPO_ROOT, 'token.json');
  const raw = readFileSync(p, 'utf8');
  const j = JSON.parse(raw);
  const tok = (j.tokens && j.tokens[0]) || j;
  if (!tok.accessToken) throw new Error('token.json missing accessToken');
  return tok;
}

function generateChecksum(machineId, macMachineId) {
  let k = 165;
  const t = Math.floor(Date.now() / 1e6);
  const b = new Uint8Array([
    (t >> 40) & 255, (t >> 32) & 255, (t >> 24) & 255,
    (t >> 16) & 255, (t >> 8) & 255, t & 255,
  ]);
  for (let i = 0; i < b.length; i++) {
    b[i] = ((b[i] ^ k) + (i % 256)) & 0xff;
    k = b[i];
  }
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

function truthy(v) {
  return !/^(0|false|off|no)$/i.test(String(v ?? '').trim());
}

function parseConnectFrames(buffer) {
  const frames = [];
  let offset = 0;
  while (buffer.length - offset >= 5) {
    const flags = buffer[offset];
    const len = buffer.readUInt32BE(offset + 1);
    if (buffer.length - offset < 5 + len) break;
    frames.push({
      flags,
      payload: buffer.subarray(offset + 5, offset + 5 + len),
    });
    offset += 5 + len;
  }
  return { frames, rest: buffer.subarray(offset) };
}

function buildHeaders(token) {
  const sessionId = uuidv4();
  return {
    ':method': 'POST',
    ':path': CHAT_RPC_PATH,
    'content-type': 'application/connect+proto',
    'connect-protocol-version': '1',
    'connect-accept-encoding': 'gzip',
    te: 'trailers',
    authorization: `Bearer ${token.accessToken}`,
    'x-cursor-checksum': generateChecksum(token.machineId || '', token.macMachineId || ''),
    'x-cursor-client-version': CLIENT_VERSION,
    'x-cursor-timezone': safeTimezone(),
    'x-request-id': uuidv4(),
    'x-session-id': sessionId,
    'x-ghost-mode': 'false',
    'x-cursor-client-type': process.env.CURSOR_CLIENT_TYPE || 'ide',
    'x-cursor-client-os': process.env.CURSOR_CLIENT_OS || cursorOs(),
    'x-cursor-client-arch': process.env.CURSOR_CLIENT_ARCH || cursorArch(),
    'x-cursor-client-device-type': process.env.CURSOR_CLIENT_DEVICE_TYPE || 'desktop',
    'x-cursor-client-os-version': process.env.CURSOR_CLIENT_OS_VERSION || os.release(),
    'x-cursor-commit': CLIENT_COMMIT,
    'user-agent': 'connect-es/1.6.1',
    'x-amzn-trace-id': `Root=${uuidv4()}`,
    // Cursor IDE sends an opaque 32-byte hex client key. Do not derive a
    // stable fingerprint from accessToken here; a random value is enough for
    // this reachability probe and avoids secret-derived logs/artifacts.
    'x-client-key': crypto.randomBytes(32).toString('hex'),
  };
}

function safeTimezone() {
  try { return Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC'; }
  catch { return 'UTC'; }
}

function cursorOs() {
  if (process.platform === 'darwin') return 'darwin';
  if (process.platform === 'win32') return 'windows_nt';
  return 'linux';
}

function cursorArch() {
  if (process.arch === 'x64') return 'x64';
  if (process.arch === 'arm64') return 'arm64';
  return process.arch;
}

async function loadHistoricalChatProto() {
  const cacheDir = dirname(CHAT_PROTO_CACHE);
  mkdirSync(cacheDir, { recursive: true });
  const cacheNodeModules = pathResolve(cacheDir, 'node_modules');
  if (!existsSync(cacheNodeModules)) {
    try {
      symlinkSync(pathResolve(REPO_ROOT, 'node_modules'), cacheNodeModules, 'dir');
    } catch {
      // If another probe created it between existsSync and symlinkSync, the
      // import below can still proceed.
    }
  }
  if (!existsSync(CHAT_PROTO_CACHE)) {
    const code = execFileSync('git', [
      '-C', REPO_ROOT,
      'show',
      `${CHAT_PROTO_COMMIT}:src/proto/chat_pb.mjs`,
    ], { encoding: 'utf8', maxBuffer: 20 * 1024 * 1024 });
    writeFileSync(CHAT_PROTO_CACHE, code, 'utf8');
  }
  return import(pathToFileURL(CHAT_PROTO_CACHE).href);
}

async function buildRequestPayload(args) {
  const chat = await loadHistoricalChatProto();
  const user = create(chat.ConversationMessageSchema, {
    text: args.prompt,
    type: chat.ConversationMessage_MessageType.HUMAN,
    bubbleId: uuidv4(),
  });
  const modelDetails = create(chat.ModelDetailsSchema, {
    modelName: args.model,
    maxMode: truthy(args.maxMode),
  });
  const inner = create(chat.StreamUnifiedChatRequestSchema, {
    conversation: [user],
    modelDetails,
    conversationId: uuidv4(),
    isChat: true,
    isHeadless: true,
    thinkingLevel: chat.StreamUnifiedChatRequest_ThinkingLevel.HIGH,
  });
  const wrapped = create(chat.StreamUnifiedChatRequestWithToolsSchema, {
    streamUnifiedChatRequest: inner,
  });
  return Buffer.from(toBinary(chat.StreamUnifiedChatRequestWithToolsSchema, wrapped));
}

function summarizePayload(payload, chat = null) {
  if (chat && payload.length > 0) {
    const decoded = decodeChatResponse(payload, chat);
    if (decoded) return decoded;
  }
  const text = payload.toString('utf8');
  const printable = text.replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/g, '.');
  return printable.slice(0, 1600);
}

function decodeChatResponse(payload, chat) {
  try {
    const resp = fromBinary(chat.StreamUnifiedChatResponseWithToolsSchema, new Uint8Array(payload));
    const inner = resp.streamUnifiedChatResponse || resp.stream_unified_chat_response;
    if (!inner) return '[decoded] StreamUnifiedChatResponseWithTools without inner response';
    const parts = ['[decoded]'];
    if (inner.text) parts.push(`text=${JSON.stringify(inner.text.slice(0, 300))}${inner.text.length > 300 ? '...' : ''}`);
    if (inner.thinking) {
      const t = inner.thinking;
      parts.push(
        `thinking.text_len=${(t.text || '').length}`,
        `thinking.signature_len=${(t.signature || '').length}`,
        `thinking.redacted_len=${(t.redactedThinking || '').length}`,
        `thinking.last=${!!t.isLastThinkingChunk}`,
      );
      if (t.text) parts.push(`thinking.preview=${JSON.stringify(t.text.slice(0, 220))}${t.text.length > 220 ? '...' : ''}`);
    }
    const usage = inner.usage || inner.usageInfo || null;
    if (usage) parts.push(`usage=${JSON.stringify(usage)}`);
    const keys = Object.keys(inner).filter(k => inner[k] != null && inner[k] !== '' && !(Array.isArray(inner[k]) && inner[k].length === 0));
    if (keys.length) parts.push(`keys=${keys.join(',')}`);
    return parts.join(' ');
  } catch {
    return null;
  }
}

function classify(text, statusCode) {
  if (/unpaid invoice/i.test(text)) {
    return 'account_billing_blocked_unpaid_invoice';
  }
  if (/ERROR_RATE_LIMITED|rate.?limit|resource_exhausted/i.test(text)) {
    return 'rate_limited_or_resource_exhausted';
  }
  if (/not authorized to use cloud agents|ERROR_UNAUTHORIZED|Unauthorized request/i.test(text)) {
    return 'chat_service_unauthorized';
  }
  if (/unauthenticated|permission|entitlement/i.test(text)) {
    return 'auth_or_entitlement_error';
  }
  if (/Max Mode Required/i.test(text)) {
    return 'model_requires_max_mode';
  }
  if (/invalid|parse|decode|malformed|missing/i.test(text)) {
    return 'request_shape_error';
  }
  if (statusCode && statusCode >= 400) {
    return 'http_error';
  }
  if (text.trim()) {
    return 'stream_or_backend_response';
  }
  return 'no_payload_seen';
}

async function runProbe(args) {
  const token = loadToken();
  const headers = buildHeaders(token);
  const chat = args.mode === 'request' ? await loadHistoricalChatProto() : null;
  const payload = args.mode === 'request' ? await buildRequestPayload(args) : Buffer.alloc(0);
  const body = encodeConnectFrame(payload);

  const client = http2.connect(BASE_URL, { settings: { initialWindowSize: 8 * 1024 * 1024 } });
  client.unref();

  let statusCode = null;
  let responseHeaders = {};
  let buffer = Buffer.alloc(0);
  let collected = '';
  let frameCount = 0;
  let endStreamSeen = false;
  const startedAt = Date.now();

  console.log(`[chat-probe] mode=${args.mode} model=${args.model} maxMode=${truthy(args.maxMode)}`);
  console.log(`[chat-probe] path=${CHAT_RPC_PATH}`);
  console.log(`[chat-probe] request_id=${headers['x-request-id']} session_id=${headers['x-session-id']}`);

  await new Promise((resolve) => {
    const timeout = setTimeout(() => {
      collected += '\n[probe-timeout]';
      try { req.close(); } catch {}
      try { client.close(); } catch {}
      resolve();
    }, Number(args.timeoutMs) || 30000);

    client.on('error', (e) => {
      collected += `\n[h2-client-error] ${e.message}`;
      clearTimeout(timeout);
      resolve();
    });

    const req = client.request(headers);
    req.on('response', (h) => {
      responseHeaders = h;
      statusCode = h[':status'] || null;
      console.log(`[chat-probe] status=${statusCode} ms=${Date.now() - startedAt}`);
      const interesting = ['content-type', 'grpc-status', 'grpc-message', 'x-cursor-server-region', 'server'];
      for (const k of interesting) {
        if (h[k] != null) console.log(`[chat-probe] header ${k}: ${h[k]}`);
      }
    });
    req.on('data', (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      const parsed = parseConnectFrames(buffer);
      buffer = parsed.rest;
      for (const frame of parsed.frames) {
        frameCount += 1;
        const isEnd = (frame.flags & 0x02) !== 0;
        if (isEnd) endStreamSeen = true;
        const summary = summarizePayload(frame.payload, (isEnd ? null : chat));
        collected += `\n[frame ${frameCount} flags=0x${frame.flags.toString(16)} len=${frame.payload.length} end=${isEnd}] ${summary}`;
        console.log(`[chat-probe] frame=${frameCount} flags=0x${frame.flags.toString(16)} len=${frame.payload.length} end=${isEnd}`);
        if (summary) console.log(summary);
      }
    });
    req.on('trailers', (trailers) => {
      const safe = JSON.stringify(trailers);
      collected += `\n[trailers] ${safe}`;
      console.log(`[chat-probe] trailers=${safe}`);
    });
    req.on('end', () => {
      clearTimeout(timeout);
      try { client.close(); } catch {}
      resolve();
    });
    req.on('error', (e) => {
      collected += `\n[h2-stream-error] ${e.message}`;
      clearTimeout(timeout);
      try { client.close(); } catch {}
      resolve();
    });
    req.write(body);
    req.end();
  });

  if (buffer.length > 0) {
    collected += `\n[partial-buffer len=${buffer.length}] ${summarizePayload(buffer)}`;
  }
  const classification = classify(collected, Number(statusCode || 0));
  console.log(`[chat-probe] done ms=${Date.now() - startedAt} frames=${frameCount} end_stream=${endStreamSeen}`);
  console.log(`[chat-probe] classification=${classification}`);
  if (Object.keys(responseHeaders).length === 0) {
    console.log('[chat-probe] no response headers received');
  }
}

runProbe(parseArgs(process.argv)).catch((e) => {
  console.error(`[chat-probe] fatal: ${e.message}`);
  process.exit(1);
});
