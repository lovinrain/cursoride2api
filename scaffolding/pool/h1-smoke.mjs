#!/usr/bin/env node
// scaffolding/pool/h1-smoke.mjs
//
// Smoke test for src/cursor-agent-h1.js — a single-channel one-shot turn
// over HTTP/1.1 (BidiAppend + RunSSE pair, no pool involvement).
//
// USAGE
//   node scaffolding/pool/h1-smoke.mjs
//   CURSOR_AGENT_DEBUG=1 node scaffolding/pool/h1-smoke.mjs   # verbose
//
// EXPECTED OUTPUT (on a healthy quota; varies with model)
// ───────────────────────────────────────────────────────
//   [h1-smoke] model=claude-opus-4-7-thinking-max-fast prompt="What is 2+2?"
//   [cursor-agent-h1] new conv id=... model=... hasState=false tools=0
//   [cursor-agent-h1] runRequest tools=0 toolBytes=0 totalBytes=... maxMode=true rid=...
//   [h1-smoke] textDelta: 2 + 2
//   [h1-smoke] textDelta:  = 4
//   [h1-smoke] turn ended in=... out=... — PASS
//
// On a rate-limited / unpaid quota the script prints the Connect error
// detail and exits 2 (treated as a known-environmental fail, not a code
// regression).

import { createRequire } from 'node:module';
import fs from 'node:fs';

const require = createRequire(import.meta.url);
const { startConversation } = require('../../src/cursor-agent-h1.js');

const MODEL = process.env.RATLC_MODEL || 'claude-opus-4-7-thinking-max-fast';
const PROMPT = process.env.H1_SMOKE_PROMPT || 'What is 2+2?';
const TIMEOUT_MS = parseInt(process.env.H1_SMOKE_TIMEOUT_MS || '60000', 10);

const TOKEN_PATH = new URL('../../token.json', import.meta.url);
const tokenFile = JSON.parse(fs.readFileSync(TOKEN_PATH, 'utf8'));
const token = tokenFile.tokens ? tokenFile.tokens[0] : tokenFile;

console.log(`[h1-smoke] model=${MODEL} prompt=${JSON.stringify(PROMPT)} timeout=${TIMEOUT_MS}ms`);

let textBuffer = '';
let thinkingDeltaCount = 0;
let thinkingCharCount = 0;
let thinkingCompletedCount = 0;
let thinkingDurationMs = null;
let resolved = false;
let bridge = null;

const failureTimer = setTimeout(() => {
  if (resolved) return;
  resolved = true;
  console.error(`[h1-smoke] TIMEOUT after ${TIMEOUT_MS}ms`);
  try { bridge && bridge.close(); } catch { /* ignore */ }
  process.exit(1);
}, TIMEOUT_MS);

bridge = startConversation(token, {
  prompt: PROMPT,
  modelId: MODEL,
  maxMode: true,
  onTextDelta: (t) => {
    textBuffer += t;
    process.stdout.write(`[h1-smoke] textDelta: ${JSON.stringify(t)}\n`);
  },
  onThinkingDelta: (t) => {
    thinkingDeltaCount++;
    thinkingCharCount += Buffer.byteLength(String(t || ''), 'utf8');
    process.stdout.write(`[h1-smoke] thinkingDelta#${thinkingDeltaCount}: ${JSON.stringify(String(t || '').slice(0, 160))}\n`);
  },
  onThinkingCompleted: (info) => {
    thinkingCompletedCount++;
    thinkingDurationMs = info?.thinkingDurationMs ?? info?.thinking_duration_ms ?? info?.durationMs ?? null;
    process.stdout.write(`[h1-smoke] thinkingCompleted#${thinkingCompletedCount}: ${JSON.stringify(info || {})}\n`);
  },
  onMcpCall: (info) => {
    console.log(`[h1-smoke] (unexpected) mcpCall: ${info.toolName}`);
    // Reply with empty so the model can finish
    try { bridge.sendToolResult(info.id, info.execId, ''); } catch { /* ignore */ }
  },
  onStepCompleted: () => {},
  onTurnEnded: (stats) => {
    if (resolved) return;
    resolved = true;
    clearTimeout(failureTimer);
    console.log(`[h1-smoke] turn ended in=${stats.inputTokens} out=${stats.outputTokens} — PASS`);
    console.log(`[h1-smoke] thinking summary: deltas=${thinkingDeltaCount} bytes=${thinkingCharCount} completed=${thinkingCompletedCount} durationMs=${thinkingDurationMs ?? 'n/a'}`);
    console.log(`[h1-smoke] full response: ${textBuffer.replace(/\s+/g, ' ').trim().slice(0, 200)}`);
    try { bridge.close(); } catch { /* ignore */ }
    setTimeout(() => process.exit(0), 200);
  },
  onError: (err) => {
    if (resolved) return;
    resolved = true;
    clearTimeout(failureTimer);
    const msg = String(err?.message || err || 'unknown');
    console.error(`[h1-smoke] ERROR: ${msg}`);
    try { bridge.close(); } catch { /* ignore */ }
    // Treat known billing/quota errors as environmental (exit 2) so caller
    // can distinguish them from code regressions (exit 1).
    if (/unpaid invoice|RATE_LIMIT|cursor\.com\/dashboard/i.test(msg)) {
      process.exit(2);
    }
    process.exit(1);
  },
});
