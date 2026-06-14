#!/usr/bin/env node
// API server — stateless HTTP front for the RATLC pool.
// Speaks Anthropic Messages on the client side; speaks Protocol B
// (newline-delimited JSON over Unix socket) to pool-manager.
//
// Restart-safe: holds no Cursor state. Pool manager owns the warm channels.

import http from 'node:http';
import net from 'node:net';
import { randomUUID, createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { cursorToAnthropic, isInternalTool } from './tool-translator.mjs';
import { isFastModel, typeThresholdMs } from './model-utils.mjs';
import * as latencyMetrics from './latency-metrics.mjs';
import { getPoolLocalToolDecision, isPoolLocalToolName, runPoolLocalTool } from './local-tool-executor.mjs';
import {
  consumeClientToolResult,
  getConsumedClientToolResult,
  rememberClientToolUse,
  buildPoolToolResultContentFromClientResult,
  resolveClientBridgeToolUse,
} from './client-tool-bridge.mjs';
import * as thinkingBuffer from './thinking-buffer.mjs';
import {
  performWebSearch,
  extractQuery,
  formatSearchResults,
  fallbackNoticeBody,
  resultsLookGeneric,
} from './spoof-mitigation.mjs';
import {
  appendImageAttachments,
  cursorMcpContentImageCount,
  cursorMcpContentPayloadBytes,
  cursorMcpContentToText,
  extractImageAttachmentsFromMessages,
  normalizeAnthropicContentForCursorMcp,
  prependTextContent,
} from './multimodal-content.mjs';
import { createSubagentModelPin } from './subagent-model-pin.mjs';

// Bridge to the existing CommonJS anthropic-tools helpers so we can reuse
// `deriveConversationKey` and `extractClientSessionId` instead of porting
// them. The helpers depend on Node `crypto` only — no ESM coupling.
const _require = createRequire(import.meta.url);
const anthropicTools = _require('../../src/anthropic-tools.js');
const agentToolsStore = _require('../../src/agent-tools-virtual-store.js');
const { StreamingHallucinationFilter } = _require('../../src/streaming-hallucination-filter.js');
const {
  ProxyThinkingBlockAdapter,
  makeProxyThinkingSignature,
  isProxyLocalThinkingBlock,
} = _require('../../src/proxy-thinking-adapter.js');

const PORT = parseInt(process.env.RATLC_API_PORT || process.env.PORT || '4242', 10);
const HOST = process.env.RATLC_API_HOST || process.env.HOST || '127.0.0.1';
const POOL_SOCK = process.env.POOL_SOCK || '/tmp/ratlc-pool.sock';
const POOL_TOOL_MODE = (process.env.POOL_TOOL_MODE || 'contract').toLowerCase();
// Opt-in: pin a Task sub-agent to the same model/group as the main agent that
// spawned it (RATLC_SUBAGENT_INHERIT_PARENT_MODEL=1). Default off — no routing
// change unless enabled. See ./subagent-model-pin.mjs.
const subagentModelPin = createSubagentModelPin();
// POOL_REINJECT_THINKING — opt-in symmetry with CURSOR_REINJECT_THINKING.
// When set, every thinking_delta arriving from the pool is appended to a
// per-convKey buffer; on subsequent turns the captured text is rendered
// back into the outbound prompt as `<thinking>...</thinking>` blocks.
// Default OFF (no behavior change vs. legacy). See thinking-buffer.mjs.
const POOL_REINJECT_THINKING = process.env.POOL_REINJECT_THINKING === '1';
const POOL_PROXY_THINKING_BLOCKS = process.env.POOL_PROXY_THINKING_BLOCKS === '1';
// Upper bound on how many bytes of upstream reasoning we forward to the client
// as thinking deltas per turn when POOL_PROXY_THINKING_BLOCKS=1. A pathological
// reasoning stream shouldn't flood the SSE channel; once over, we stop
// forwarding (the text is still captured for detection/reinjection). Unset →
// generous 256KB default (well above MAX_THINKING_TOKENS worth of text); a
// positive value is the exact cap; <=0 disables the cap (unlimited).
const POOL_PROXY_THINKING_MAX_BYTES = (() => {
  const raw = process.env.POOL_PROXY_THINKING_MAX_BYTES;
  if (raw == null || raw === '') return 262144;
  const n = parseInt(raw, 10);
  if (!Number.isFinite(n) || n <= 0) return Infinity;
  return n;
})();
// Claude Code currently does not render Anthropic `server_tool_use` blocks in
// the same visible way it renders client-side `tool_use` blocks. Keep emitting
// the official blocks for protocol consumers, and add a small text trace unless
// explicitly disabled.
const RENDER_SERVER_TOOL_TEXT = process.env.RATLC_RENDER_SERVER_TOOL_TEXT !== '0';
// POOL_CONTEXT_MODE selects how multi-turn conversations are forwarded
// to the pool channel:
//   last (default) — only the last user message text is sent. Backwards-
//                    compatible. Pool channels accumulate per-conversation
//                    state inside the model's context window, so multi-turn
//                    coherence requires every turn of one conversation to
//                    land on the SAME channel. LRU rotation breaks this.
//   full           — every POST renders the entire messages[] history into
//                    one self-contained prompt. The channel is treated as
//                    a stateless carrier — each `bajie_yield` result is a
//                    complete fresh request. Channel rotation is now safe.
//   hybrid         — first turn / divergent turns send `full`; stable
//                    continuations send `last` and ask the pool-manager to
//                    prefer the same channel for this client session.
const POOL_CONTEXT_MODE = (process.env.POOL_CONTEXT_MODE || 'last').toLowerCase();
if (!['full', 'last', 'hybrid'].includes(POOL_CONTEXT_MODE)) {
  console.error(`invalid POOL_CONTEXT_MODE=${POOL_CONTEXT_MODE} (must be full|last|hybrid)`);
  process.exit(1);
}
const HYBRID_SESSION_TTL_MS = Math.max(60_000, parseInt(process.env.POOL_HYBRID_SESSION_TTL_MS || '1800000', 10));
// Safety valve for clients such as Claude Code that echo complete messages[]
// history, tool results, and optional thinking back on every turn. PR #2's
// author observed that very large bajie_yield payloads could make Cursor
// close or stall the live session, and set this default to 98304.
//
// Default changed to 0 (disabled) on 2026-05-22 after the probe in
// scaffolding/pool/test_bidi_payload_limit.mjs measured 100% success up to
// 8 MB on claude-4.6 with no stalls — see BIDI_PAYLOAD_LIMIT.md for the
// data. The previous default silently truncated any conversation >96 KB,
// destroying memory. Operators who hit the original problem in their
// environment can re-enable explicitly: RATLC_CONTEXT_MAX_BYTES=N.
//
// When > 0, the guard fires only in POOL_CONTEXT_MODE=hybrid (gated by
// `guardActive` below) so `full` mode always honors its name.
const CONTEXT_MAX_BYTES = Math.max(0, parseInt(process.env.RATLC_CONTEXT_MAX_BYTES || process.env.POOL_CONTEXT_MAX_BYTES || '0', 10));

const log = (...args) => console.log(`[${new Date().toISOString().slice(11, 23)}] [api]`, ...args);
log(`POOL_CONTEXT_MODE=${POOL_CONTEXT_MODE}  POOL_TOOL_MODE=${POOL_TOOL_MODE}  POOL_REINJECT_THINKING=${POOL_REINJECT_THINKING ? 1 : 0}  POOL_PROXY_THINKING_BLOCKS=${POOL_PROXY_THINKING_BLOCKS ? 1 : 0}  CONTEXT_MAX_BYTES=${CONTEXT_MAX_BYTES}  SUBAGENT_MODEL_PIN=${subagentModelPin.enabled ? 1 : 0}`);

const REQUEST_LOG_MAX = Math.max(100, parseInt(process.env.RATLC_REQUEST_LOG_MAX || '500', 10));
const requestLog = [];
const requestLogById = new Map();

function rememberRequest(entry) {
  if (!entry || !entry.requestId) return entry;
  requestLogById.set(entry.requestId, entry);
  requestLog.unshift(entry);
  while (requestLog.length > REQUEST_LOG_MAX) {
    const old = requestLog.pop();
    if (old?.requestId) requestLogById.delete(old.requestId);
  }
  return entry;
}

function patchRequest(requestId, patch) {
  const entry = requestLogById.get(requestId);
  if (!entry) return null;
  Object.assign(entry, patch);
  return entry;
}

function finishRequestLog(requestId, patch = {}) {
  const now = Date.now();
  const entry = requestLogById.get(requestId);
  if (!entry) return null;
  Object.assign(entry, patch, {
    endedAt: patch.endedAt || now,
    durationMs: now - entry.startedAt,
  });
  if (entry.firstByteAt && entry.firstByteMs == null) {
    entry.firstByteMs = entry.firstByteAt - entry.startedAt;
  }
  // Single accumulation hook for per-model latency metrics (both the graceful
  // and hard-error finish paths funnel through here). Never let it break a turn.
  try { latencyMetrics.record(entry); } catch { /* metrics are best-effort */ }
  return entry;
}

// ── Per-model-type watchdog thresholds + adaptive timeouts ──────────────────
// Fast models (the `-fast` accelerator) can use a different silent-timeout than
// non-fast ones. RATLC_<KEY>_FAST / RATLC_<KEY>_SLOW override the global
// RATLC_<KEY>; an explicit "" or unset falls through to the global, then dflt.
const ADAPTIVE_TIMEOUTS = process.env.RATLC_ADAPTIVE_TIMEOUTS === '1';
const ADAPTIVE_MIN_GAP_MS = Math.max(1000, parseInt(process.env.RATLC_ADAPTIVE_MIN_GAP_MS || '10000', 10));

// Upstream rate-limit awareness. Cursor throttles per-model quota (e.g. a new/
// premium model like Fable 5 on accounts without allowance): the run errors with
// "resource_exhausted … rate limit", or the channel accepts the turn but returns
// nothing. Without this, the former surfaced as a generic api_error and the
// latter as "empty response N times" — both hiding the real cause. We (a) tag a
// rate-limit error as a proper rate_limit_error to the client, and (b) remember
// which models were just rate-limited so a subsequent empty turn on the same
// model is reported as a rate-limit (and not retried into the same wall).
const RATE_LIMIT_MEMORY_MS = Math.max(10_000, parseInt(process.env.RATLC_RATE_LIMIT_MEMORY_MS || '90000', 10));
const _rateLimitedModelAt = new Map(); // model → last rate-limit ms
function isUpstreamRateLimit(message) {
  return /resource_exhausted|rate[ _-]?limit|RATE_LIMITED|reached the rate limit|API usage limit|too many computers/i.test(String(message || ''));
}
function noteUpstreamRateLimit(model) { if (model) _rateLimitedModelAt.set(String(model), Date.now()); }
function recentlyRateLimited(model) {
  const t = _rateLimitedModelAt.get(String(model || ''));
  return t != null && (Date.now() - t) < RATE_LIMIT_MEMORY_MS;
}

function normalizeModelForRouting(model) {
  return String(model || '').trim().replace(/\[[^\]]+\]$/g, '');
}

function stableHash(value) {
  let raw;
  if (typeof value === 'string') raw = value;
  else {
    try { raw = JSON.stringify(value || null); }
    catch { raw = String(value || ''); }
  }
  return createHash('sha256').update(raw).digest('hex').slice(0, 16);
}

function toolsSignature(tools) {
  if (!Array.isArray(tools)) return '';
  return tools
    .filter((t) => t && t.name)
    .map((t) => `${t.name}:${stableHash(t.input_schema || t.jsonSchema || {})}`)
    .sort()
    .join('|');
}

function messagesFingerprint(messages) {
  return stableHash(Array.isArray(messages) ? messages : []);
}

const hybridSessions = new Map();

function evictHybridSessions(now = Date.now()) {
  for (const [k, v] of hybridSessions) {
    if (now - v.lastAccessMs > HYBRID_SESSION_TTL_MS) hybridSessions.delete(k);
  }
}
setInterval(evictHybridSessions, 5 * 60_000).unref();

function makeSessionKey({ clientSessionId, convKey, routingModel }) {
  return clientSessionId
    ? `sid:${routingModel || ''}:${clientSessionId}`
    : `conv:${routingModel || ''}:${convKey}`;
}

function decideHybridContext({ clientSessionId, convKey, routingModel, system, tools, messages }) {
  const sessionKey = makeSessionKey({ clientSessionId, convKey, routingModel });
  const now = Date.now();
  const sysHash = stableHash(extractSystemPrompt(system));
  const toolHash = toolsSignature(tools);
  const msgCount = Array.isArray(messages) ? messages.length : 0;
  const currentTranscriptHash = messagesFingerprint(messages);
  const prev = hybridSessions.get(sessionKey);
  const reasons = [];
  let sendMode = 'full';

  if (!prev) reasons.push('new-session');
  if (prev && prev.model !== routingModel) reasons.push('model-changed');
  if (prev && prev.systemHash !== sysHash) reasons.push('system-changed');
  if (prev && prev.toolsHash !== toolHash) reasons.push('tools-changed');
  if (prev && msgCount <= prev.messageCount) reasons.push('non-monotonic-messages');

  if (prev && reasons.length === 0) sendMode = 'last';

  hybridSessions.set(sessionKey, {
    model: routingModel || '',
    systemHash: sysHash,
    toolsHash: toolHash,
    messageCount: msgCount,
    transcriptHash: currentTranscriptHash,
    previousTranscriptHash: prev?.transcriptHash || null,
    lastAccessMs: now,
  });

  return {
    sessionKey,
    sendMode,
    reason: reasons.length ? reasons.join(',') : 'stable-session',
    messageCount: msgCount,
  };
}

// ── Pool socket connection ──────────────────────────────────────────────
let poolSock = null;
let poolBuf = '';
const reqHandlers = new Map();      // requestId -> { onEvent }
let reconnectTimer = null;

// Spoof-mitigation playbook: when the model emits the empty Write→
// agent-tools/<uuid>.txt pattern, we kick off a real Bing RSS search
// asynchronously and record the resulting PROMISE keyed by tool_use_id.
// When claude-code POSTs the corresponding tool_result back, we
// `await` the promise (with timeout) and REPLACE the short "File
// created at..." ack with the real search payload before forwarding
// to the pool. The model sees actual web data inline in its next
// assistant turn — without needing to re-read the file.
//
// The tool_use forwarding is NOT blocked by the search: we mutate
// msg.args.content to a static proxy_notice (so claude-code's Write
// succeeds with non-empty content) and let the SSE flow proceed. The
// search runs in parallel; by the time claude-code's Write finishes
// (~10ms) and POSTs the tool_result, the search (~200ms) is usually
// still in flight — we await it briefly there.
//
// Earlier attempt (commit 0867440 → reverted 88f7c70) failed because
// of duplicate `message_start` SSE events and missing interleaved-
// thinking blocks on the route_decision error path. Both fixed in
// commits 9485e8e and 65310a6, so the playbook is safe to re-enable.
//
// Bounded FIFO with SPOOF_PLAYBOOK_MAX entries — prevents unbounded
// growth on long-running api-server processes.
const SPOOF_PLAYBOOK_MAX = 256;
const SPOOF_SEARCH_WAIT_MS = 5000;
// Gap-1 (WEB_RESEARCH_GAPS.md): when Cursor's WebSearch completes but the
// proxy can't extract result metadata from the bidi stream, fall back to
// Bing RSS on the proxy host and substitute those as the result content.
// Set to 0 to disable (then the original error_code:'unavailable' block
// is emitted as before).
const WEBSEARCH_FALLBACK_ENABLED = (process.env.RATLC_WEBSEARCH_FALLBACK || '1') === '1';
const WEBSEARCH_FALLBACK_TIMEOUT_MS = parseInt(process.env.RATLC_WEBSEARCH_FALLBACK_TIMEOUT_MS || '4500', 10);

// ── Auto-retry on transient upstream failures ──────────────────────────────
// Two distinct symptoms get separately-budgeted retry:
//
//   upstream_silent_timeout — pool routed, Cursor accepted the bidi frame, then
//     >NO_VISIBLE_EVENT_TIMEOUT_MS passed with no text/thinking/tool_use/yield/
//     error. Usually a transient (Cursor's vendored proto evolves; an unknown
//     exec message makes the inner model wait forever). A different channel
//     usually succeeds. Retry cancels the stuck channel + replays the original
//     send_user_message payload; pool round-robins to a fresh channel.
//
//   empty_assistant_turn — Cursor cleanly ended the turn (yield/step_completed)
//     but the model produced ZERO visible content. Less likely to be transient
//     since the same prompt against the same model often gets the same answer;
//     retry overrides sessionKey to null so the pool routes to a DIFFERENT
//     channel. Optionally skipped if substantive thinking was captured (the
//     model may have legitimately decided to say nothing).
//
// Both default off. Recommended starting point: silent=2, empty=1 with
// thinking-aware default. Each retry burns one Cursor turn worth of quota +
// adds latency, so cap conservatively. Only fires for send_user_message
// payloads — send_tool_results retries are unsafe (consumed tool_use_id state).
const UPSTREAM_SILENT_RETRY_MAX = Math.max(0, parseInt(process.env.RATLC_RETRY_UPSTREAM_SILENT_MAX || '0', 10));
const EMPTY_TURN_RETRY_MAX = Math.max(0, parseInt(process.env.RATLC_RETRY_EMPTY_TURN_MAX || '0', 10));
const RETRY_DELAY_MS = Math.max(0, parseInt(process.env.RATLC_RETRY_DELAY_MS || '500', 10));
const RETRY_EMIT_NOTICE = (process.env.RATLC_RETRY_EMIT_NOTICE || '1') === '1';
// `already consumed` / `unknown` / `span multiple channels` tool_use_id errors
// mean the client (re)sent a tool_result the pool can't honor on the bound
// channel — typically a claude-code DOUBLE-SUBMIT (the first submit consumed
// the id; the duplicate or a later "continue" hits this) or a watchdog-
// finalized turn whose channel never produced output. The old behavior was a
// dead-end text notice ("send a fresh user message"), but claude-code's
// conversation still has an unanswered tool_use, so it just re-sends the same
// (now-consumed) tool_result forever. Instead we recover by replaying the
// pre-built full-context send_user_message on a FRESH channel (same machinery
// as the silent-timeout fallback) so the model actually continues. Defaults
// ON (1) because the dead-end is strictly worse; set 0 to restore the notice.
const STALE_TOOL_RESULT_RETRY_MAX = Math.max(0, parseInt(process.env.RATLC_RETRY_STALE_TOOL_RESULT_MAX || '1', 10));

// upstream_abort — a transient upstream error (e.g. Cursor "Response error:
// aborted", a channel that died, an NGHTTP2 reset) arrived BEFORE any
// client-visible content. Without recovery this dead-ends as either an
// "empty or malformed response (HTTP 200)" (the clean-error path) or a
// manual-retry notice (the softened path). Retrying the original payload on a
// FRESH channel (sessionKey nulled, to dodge a stale sticky channel) recovers
// it transparently. Default 0 (opt-in). Only fires before content + for
// transient errors (never auth/quota/rate-limit). See isRetryableUpstreamAbort.
const UPSTREAM_ABORT_RETRY_MAX = Math.max(0, parseInt(process.env.RATLC_RETRY_UPSTREAM_ABORT_MAX || '0', 10));

// continue_after_abort — when the upstream aborts mid-TEXT (after some visible
// text was already streamed to the client), we can't replay from scratch
// (that would duplicate the shown text). Instead, self-drive a CONTINUATION:
// keep the SSE message open, ask a fresh channel to continue seamlessly from
// the partial text, and stream the continuation into the same message — so the
// user never sees "send a new message to continue". Default 0 (opt-in).
const CONTINUE_AFTER_ABORT_MAX = Math.max(0, parseInt(process.env.RATLC_RETRY_CONTINUE_AFTER_ABORT_MAX || '0', 10));
const CONTINUE_AFTER_ABORT_INSTRUCTION =
  'Your previous reply (shown immediately above as the assistant turn) was cut off mid-stream by an upstream connection error. ' +
  'Continue it seamlessly from the exact point where it stopped. Do NOT repeat, re-summarize, or rephrase any text already written; ' +
  'do NOT restart from the beginning; do NOT add any preamble, greeting, apology, or meta-comment about the interruption. ' +
  'Output only the remaining continuation text, as if no interruption had occurred.';

// ── Keep-alive ping cadence ────────────────────────────────────────────────
// After message_start we may sit silent for many seconds waiting on Cursor's
// first token (large `full`-mode payloads are slow to first-byte). The wire
// goes quiet and the client's read-idle timeout fires FIRST — before our 25s
// no-visible-event watchdog — so claude-code aborts with "API returned an
// empty or malformed response (HTTP 200)" and we never get to retry. A
// recurring SSE `ping` (the same event Anthropic interleaves) resets the
// client's read timer at the transport layer regardless of how the client
// treats the event semantically. Armed in startMsg, cleared in finishMessage.
// Set to 0 to disable.
const KEEPALIVE_PING_MS = Math.max(0, parseInt(process.env.RATLC_KEEPALIVE_PING_MS || '3000', 10));

// ── Local-tool-adapter dedup ───────────────────────────────────────────────
// The bridge-worker can deliver the SAME tool_use IPC twice — Cursor's
// backend sometimes re-emits an unhandled tool_use after the parallel-tools
// watchdog finalizes the outer turn and a follow-up request comes in on
// the same channel. The local-tool-adapter would then run twice, both
// would send_tool_results, and the second one would hit the pool's
// `already consumed anthropic_tool_use_id` check (which softens to a
// proxy_notice but is ugly + wastes a tool execution).
//
// This dedup tracks in-flight + recently-completed tool_use_ids module-wide
// (the duplicate can cross HTTP requests, so per-request scope wouldn't
// catch it). On duplicate arrival: skip the second invocation entirely;
// the first invocation's send_tool_results stands.
const localToolInFlight = new Map();    // tool_use_id -> { startedAt, name }
const localToolCompleted = new Map();   // tool_use_id -> completedAt
const LOCAL_TOOL_DEDUP_TTL_MS = 30_000;
setInterval(() => {
  const cutoff = Date.now() - LOCAL_TOOL_DEDUP_TTL_MS;
  for (const [id, ts] of localToolCompleted) {
    if (ts < cutoff) localToolCompleted.delete(id);
  }
}, 10_000).unref();

const spoofResultPlaybook = new Map();
function recordSpoofResult(toolUseId, promise) {
  if (!toolUseId || !promise) return;
  if (spoofResultPlaybook.size >= SPOOF_PLAYBOOK_MAX) {
    const firstKey = spoofResultPlaybook.keys().next().value;
    if (firstKey !== undefined) spoofResultPlaybook.delete(firstKey);
  }
  spoofResultPlaybook.set(toolUseId, promise);
}
async function consumeSpoofResult(toolUseId) {
  const p = spoofResultPlaybook.get(toolUseId);
  if (p === undefined) return undefined;
  spoofResultPlaybook.delete(toolUseId);
  try {
    return await Promise.race([
      p,
      new Promise((resolve) => setTimeout(() => resolve(null), SPOOF_SEARCH_WAIT_MS)),
    ]);
  } catch {
    return null;
  }
}

function connectPool() {
  poolSock = net.createConnection(POOL_SOCK);
  poolSock.on('connect', () => {
    log(`connected to pool at ${POOL_SOCK}`);
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  });
  poolSock.on('data', (chunk) => {
    poolBuf += chunk.toString('utf8');
    let idx;
    while ((idx = poolBuf.indexOf('\n')) !== -1) {
      const line = poolBuf.slice(0, idx);
      poolBuf = poolBuf.slice(idx + 1);
      if (!line.trim()) continue;
      try {
        const msg = JSON.parse(line);
        const h = msg.requestId ? reqHandlers.get(msg.requestId) : null;
        if (h) h.onEvent(msg);
      } catch (e) {
        log('bad json from pool:', e.message);
      }
    }
  });
  poolSock.on('error', (e) => log('pool socket error:', e.message));
  poolSock.on('close', () => {
    log('pool socket closed; will retry in 2s');
    poolSock = null;
    // Fail any in-flight handlers
    for (const [reqId, h] of reqHandlers.entries()) {
      h.onEvent({ type: 'error', requestId: reqId, message: 'pool socket disconnected' });
    }
    reqHandlers.clear();
    if (!reconnectTimer) reconnectTimer = setTimeout(connectPool, 2000);
  });
}
connectPool();

function poolWrite(obj) {
  if (!poolSock || poolSock.destroyed) return false;
  try { poolSock.write(JSON.stringify(obj) + '\n'); return true; }
  catch { return false; }
}

// ── Anthropic SSE encoder ───────────────────────────────────────────────
function sseWrite(res, event, data) {
  if (!res || res.writableEnded) return;
  try { res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`); }
  catch (e) { /* client disconnect */ }
}

// ── Request handler ─────────────────────────────────────────────────────
async function readJsonBody(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

function extractTextFromContent(content) {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content.filter((c) => c.type === 'text').map((c) => c.text || '').join('\n');
}

function buildLastUserCursorMcpContent(content, thinkingTurns) {
  const normalized = normalizeAnthropicContentForCursorMcp(content);
  const preamble = renderThinkingPreamble(thinkingTurns);
  return prependTextContent(normalized, preamble);
}

function buildFullContextCursorMcpContent({ messages, system, tools, thinkingTurns }) {
  const text = renderFullContext({ messages, system, tools, thinkingTurns });
  const attachments = extractImageAttachmentsFromMessages(messages);
  return appendImageAttachments({ items: [{ kind: 'text', text }] }, attachments);
}

function findAllToolResults(content) {
  if (!Array.isArray(content)) return [];
  const out = [];
  for (const c of content) {
    if (c.type === 'tool_result') {
      const text = typeof c.content === 'string' ? c.content :
        Array.isArray(c.content) ? c.content.map((p) => p.type === 'text' ? p.text : JSON.stringify(p)).join('\n') : '';
      out.push({ tool_use_id: c.tool_use_id, text, content: c.content, isError: c.is_error === true });
    }
  }
  return out;
}

function makeClientBridgeToolUseId(poolToolUseId) {
  // Must match Anthropic's tool_use pattern ^toolu_[a-zA-Z0-9_]+$ (NO dashes) so
  // the block survives a session resume against the real API. The pool id is
  // arbitrary (often a UUID with dashes); strip dashes too (the prior class
  // `[^A-Za-z0-9_-]` kept them). Correlation back to the pool id is via a stored
  // map (poolToolUseId), not by decoding this suffix, so sanitizing is safe.
  const suffix = String(poolToolUseId || randomUUID()).replace(/[^A-Za-z0-9_]/g, '').slice(-16);
  return `toolu_client_${suffix || randomUUID().replace(/-/g, '').slice(0, 16)}`;
}

function isClientBridgeToolUseId(toolUseId) {
  return typeof toolUseId === 'string' && toolUseId.startsWith('toolu_client_');
}

function isSyntheticToolUseId(toolUseId) {
  if (!toolUseId || typeof toolUseId !== 'string') return false;
  if (toolUseId.startsWith('toolu_synth_')) return true;
  const decoded = anthropicTools.decodeToolUseId(toolUseId);
  return !!decoded && !decoded.execId && String(decoded.toolCallId || '').startsWith('toolu_synth_');
}

function extractSystemPrompt(system) {
  if (!system) return '';
  if (typeof system === 'string') return system;
  if (Array.isArray(system)) return system.map((p) => typeof p === 'string' ? p : p.text || '').join('\n');
  return '';
}

// Render an Anthropic content block array as a flat string, stable across
// nesting shapes. Used by renderFullContext to expand both top-level message
// content and the inner content of tool_result blocks.
function renderContentBlocks(blocks) {
  if (typeof blocks === 'string') return blocks;
  if (!Array.isArray(blocks)) return '';
  const out = [];
  for (const c of blocks) {
    if (!c || typeof c !== 'object') continue;
    if (c.type === 'text') {
      out.push(c.text || '');
    } else if (c.type === 'tool_use') {
      // Show the assistant's tool call: name + JSON args.
      const args = c.input == null ? {} : c.input;
      out.push(`<tool_use name="${c.name || '?'}" id="${c.id || ''}">\n${JSON.stringify(args, null, 2)}\n</tool_use>`);
    } else if (c.type === 'tool_result') {
      // Recursively render nested content blocks. Anthropic SDK allows the
      // result body to be either a string or an array of {type:text|image}
      // entries; both shapes are handled.
      const inner = typeof c.content === 'string'
        ? c.content
        : (Array.isArray(c.content) ? renderContentBlocks(c.content) : '');
      const err = c.is_error ? ' is_error="true"' : '';
      out.push(`<tool_result tool_use_id="${c.tool_use_id || ''}"${err}>\n${inner}\n</tool_result>`);
    } else if (c.type === 'image') {
      out.push('<image/>');
    } else if (c.type === 'thinking') {
      // claude-code echoes thinking blocks back into messages on the next
      // turn (interleaved-thinking beta). Proxy-local thinking is UI-only and
      // should not silently become Cursor prompt context; explicit
      // POOL_REINJECT_THINKING remains the opt-in path for that.
      if (isProxyLocalThinkingBlock(c)) continue;
      const t = typeof c.thinking === 'string' ? c.thinking : '';
      if (t) out.push(`<thinking>\n${t}\n</thinking>`);
    } else if (c.type === 'redacted_thinking') {
      // No plaintext to render — a redacted_thinking block carries only an
      // encrypted blob. Safe to drop here: this transport never PRODUCES them
      // (we emit proxy-local `thinking`, not redacted), so one could only appear
      // if a client echoed a real-Anthropic block in, which doesn't occur when
      // claude-code points at this proxy. Nothing actionable to forward upstream.
    } else if (typeof c.text === 'string') {
      // Tolerate untyped {text:"..."} entries (older SDKs).
      out.push(c.text);
    } else {
      // Unknown block type — dump as JSON so nothing is silently dropped.
      out.push(`<unknown type="${c.type || '?'}">${JSON.stringify(c).slice(0, 500)}</unknown>`);
    }
  }
  return out.join('\n');
}

// Render the entire messages[] history into a single self-contained prompt.
// Used when POOL_CONTEXT_MODE=full so the pool channel (which is stateless
// across conversation turns under LRU rotation) gets the full context every
// turn. Format design goals:
//   - Clearly delimit user vs assistant turns
//   - Expand tool_use blocks (tool name + args) and tool_result blocks
//   - End with the latest user turn marked as the one to respond to
//   - Stable across content-shape variations (string vs array, nested
//     tool_result.content of either shape)
//
// When `thinkingTurns` is provided (POOL_REINJECT_THINKING=1), each entry
// `{turnIndex, text}` is keyed to the assistant message at that ordinal
// (0-indexed by assistant role appearances in messages[]). We prepend
// `<thinking>...</thinking>` to that turn's body so the model can
// reference its own prior reasoning in the next turn. Mirrors
// src/anthropic-converter.js:499-518 (server.js's converter).
function renderFullContext({ messages, system, tools, thinkingTurns }) {
  const lines = [];
  lines.push('=== FULL CONVERSATION CONTEXT ===');
  lines.push('You are receiving the complete conversation history for ONE self-contained request. Respond to the FINAL user turn below. Do not assume any continuity with prior bajie_yield results — each delivery is independent and the history below is the only context you have.');
  lines.push('');

  const sys = extractSystemPrompt(system);
  if (sys) {
    lines.push('--- SYSTEM ---');
    lines.push(sys);
    lines.push('');
  }

  if (Array.isArray(tools) && tools.length > 0) {
    lines.push('--- AVAILABLE TOOLS (for reference; use the live tool list bound to this stream) ---');
    const clientMcpNames = [];
    for (const t of tools) {
      if (!t || !t.name) continue;
      const desc = t.description ? ` — ${String(t.description).slice(0, 200)}` : '';
      lines.push(`* ${t.name}${desc}`);
      if (String(t.name).startsWith('mcp__')) clientMcpNames.push(t.name);
    }
    if (clientMcpNames.length > 0) {
      lines.push('');
      lines.push(
        'Client MCP tools above are not registered one-by-one in this warm Cursor pool. ' +
        'To use any tool named `mcp__server__tool`, call `client_mcp_call` with ' +
        '`tool_name` set to that exact name and `input` set to the tool arguments.'
      );
    }
    lines.push('');
  }

  // Build a per-assistant-turn-ordinal lookup so we can attach captured
  // thinking text to the matching assistant message in messages[].
  const thinkingByAssistantIdx = new Map();
  if (Array.isArray(thinkingTurns)) {
    for (const e of thinkingTurns) {
      if (e && Number.isFinite(e.turnIndex) && typeof e.text === 'string' && e.text.length > 0) {
        thinkingByAssistantIdx.set(e.turnIndex, e.text);
      }
    }
  }

  lines.push('--- CONVERSATION ---');
  const arr = Array.isArray(messages) ? messages : [];
  let assistantIdx = -1;
  for (let i = 0; i < arr.length; i++) {
    const m = arr[i];
    if (!m || !m.role) continue;
    const isLastUser = (i === arr.length - 1) && m.role === 'user';
    const tag = isLastUser ? `[user] (RESPOND TO THIS)` : `[${m.role}]`;
    lines.push(tag + ':');
    let body = typeof m.content === 'string'
      ? m.content
      : renderContentBlocks(m.content);
    if (m.role === 'assistant') {
      assistantIdx++;
      const priorThinking = thinkingByAssistantIdx.get(assistantIdx);
      if (priorThinking) {
        body = `<thinking>\n${priorThinking}\n</thinking>\n${body || ''}`;
      }
    }
    lines.push(body || '(empty)');
    lines.push('');
  }

  lines.push('--- END CONVERSATION ---');
  lines.push('Respond to the final user turn now. Then call bajie_yield to wait for the next request.');
  return lines.join('\n');
}

// Render a stack of captured thinking turns as a leading sequence of
// `<thinking>...</thinking>` blocks, oldest-first. Used in `last`-mode
// re-injection: we have only the latest user text to send, so the stored
// thinking history goes in front of it. Returns '' when no turns.
function renderThinkingPreamble(thinkingTurns) {
  if (!Array.isArray(thinkingTurns) || thinkingTurns.length === 0) return '';
  const blocks = thinkingTurns
    .filter((e) => e && typeof e.text === 'string' && e.text.length > 0)
    .map((e) => `<thinking>\n${e.text}\n</thinking>`);
  if (blocks.length === 0) return '';
  return blocks.join('\n\n') + '\n\n';
}

function looksLikeAgentToolPlaceholderWrite(toolName, args) {
  // Used only by the hallucinated-tool-call rescue path (where a model
  // writes a Write tool call as TEXT and we parse it out). The general
  // intercept for agent-tools/<uuid>.txt is now handled by the virtual
  // store (see handleMessagesRequest's Write/Read handlers below). Here
  // we only suppress the legacy "model wrote empty placeholder as text"
  // rescue — non-empty agent-tools writes go through the virtual store
  // path which always intercepts.
  const normalizedTool = anthropicTools.normalizeClientToolNameForPolicy(toolName);
  if (normalizedTool !== 'write') return false;
  const a = args && typeof args === 'object' ? args : {};
  const p = agentToolsStore.getPathFromArgs(a);
  const c = String(a.content ?? a.file_text ?? a.text ?? a.body ?? a.data ?? '').trim();
  return agentToolsStore.isAgentToolsArtifactPath(p) && (c === '' || c === '(No content)');
}

async function handleMessagesRequest(req, res) {
  let body;
  try { body = await readJsonBody(req); }
  catch (e) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ type: 'error', error: { type: 'invalid_request_error', message: 'bad json' } }));
  }
  const { tools } = body;
  // messages/system are mutable: a trailing (or embedded) role:system message
  // gets hoisted into the system field below (see the hoist block before
  // validation). Some clients append MCP-server-instructions / skills-list
  // content as a `{role:"system"}` element in messages[] — illegal in the
  // Anthropic schema (system belongs in the top-level field) — which would
  // otherwise 400 with "last message must be user".
  let { messages, system, model } = body;
  let routingModel = normalizeModelForRouting(model);
  // claude-code (and other clients) enable `interleaved-thinking-2025-05-14`
  // beta plus `thinking: {type:'enabled'}` in the body when talking to
  // thinking models. With that beta on, the client REQUIRES a `thinking`
  // content block to precede any `tool_use` block in the assistant response.
  // If we emit only the tool_use, claude-code rejects the whole stream with
  // "API returned an empty or malformed response (HTTP 200)". We cannot
  // produce Anthropic-signed thinking from Cursor's AgentService stream, so
  // this server either emits real current-turn Cursor `thinking_delta` as a
  // proxy-local block (POOL_PROXY_THINKING_BLOCKS=1) or falls back to a
  // proxy-local placeholder block. Both are stripped from prompt rendering.
  // Pre-emit guard is on body.thinking.type, NOT the beta
  // header — claude-code v2.1.143 sends both together, and the body
  // field is the authoritative signal.
  const clientThinkingEnabled =
    body && body.thinking && (body.thinking.type === 'enabled' || body.thinking === 'enabled');
  if (process.env.LOG_REQUEST_TOOLS === '1') {
    log(`incoming /v1/messages: tools=${Array.isArray(tools) ? tools.length : 0} [${(tools || []).map((t) => t.name).slice(0, 30).join(', ')}]  system=${typeof system === 'string' ? system.length + 'c' : Array.isArray(system) ? 'array(' + system.length + ')' : 'none'}  model=${model || '(default)'}`);
  }
  // Body summary — every POST gets a one-liner showing the LAST message's
  // shape. This is the ONE log line you need to see whether a POST is a
  // tool_result round-trip or a fresh user turn.
  if (Array.isArray(messages) && messages.length > 0) {
    const last = messages[messages.length - 1];
    let summary;
    if (typeof last.content === 'string') {
      summary = `text="${last.content.slice(0, 80).replace(/\n/g, '\\n')}"`;
    } else if (Array.isArray(last.content)) {
      const parts = last.content.map((c) => {
        if (c.type === 'tool_result') return `tool_result(id=${c.tool_use_id}, ${typeof c.content === 'string' ? c.content.length + 'c' : 'blocks=' + (Array.isArray(c.content) ? c.content.length : '?')}${c.is_error ? ', is_error=true' : ''})`;
        if (c.type === 'text') return `text(${(c.text || '').length}c)`;
        if (c.type === 'image') return 'image';
        return c.type;
      });
      summary = parts.join(', ');
    } else {
      summary = `content type=${typeof last.content}`;
    }
    log(`  body: lastMsg.role=${last.role} content=[${summary}] msgCount=${messages.length}`);
  }
  // ── Hoist role:system messages out of messages[] ──────────────────────────
  // The Anthropic Messages API only permits `user`/`assistant` roles inside
  // messages[]; system content belongs in the top-level `system` field. Some
  // clients (observed: claude-code appending "# MCP Server Instructions" /
  // "The following skills are available…" as a trailing element) violate this,
  // which used to 400 with "last message must be user". Instead of rejecting,
  // fold each system message's text into the system field (in order) and drop
  // it from messages[]. The cleaned array then flows through the normal
  // validation below — a request that is STILL malformed after hoisting (e.g.
  // genuinely empty, or ending in assistant) is still correctly rejected.
  if (Array.isArray(messages) && messages.some((m) => m && m.role === 'system')) {
    const hoisted = [];
    const kept = [];
    for (const m of messages) {
      if (m && m.role === 'system') {
        const t = extractTextFromContent(m.content);
        if (t) hoisted.push(t);
      } else {
        kept.push(m);
      }
    }
    if (hoisted.length > 0) {
      const hoistedText = hoisted.join('\n\n');
      if (Array.isArray(system)) {
        system = [...system, { type: 'text', text: hoistedText }];
      } else if (typeof system === 'string' && system.length > 0) {
        system = `${system}\n\n${hoistedText}`;
      } else {
        system = hoistedText;
      }
      log(`  → hoisted ${hoisted.length} role:system message(s) out of messages[] into system field (${hoistedText.length}c); msgCount ${messages.length}→${kept.length}`);
    }
    messages = kept;
    // Keep body in sync for any downstream reader (e.g. extractClientSessionId).
    body.messages = messages;
    body.system = system;
  }
  if (!Array.isArray(messages) || messages.length === 0) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ type: 'error', error: { type: 'invalid_request_error', message: 'messages required' } }));
  }
  const lastMsg = messages[messages.length - 1];
  if (lastMsg.role !== 'user') {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ type: 'error', error: { type: 'invalid_request_error', message: 'last message must be user' } }));
  }

  // Derive convKey for the thinking-buffer (and any future per-conv
  // state). Prefer the claude-code session UUID — it's stable across
  // continuations of one CLI invocation and cannot collide across
  // distinct sessions on the same machine. Falls back to the legacy
  // (modelId, system, first-user-text, remoteAddr, remotePort, tools)
  // salt when the header / body.metadata.user_id are absent.
  req.body = body; // expose for extractClientSessionId's body-fallback path
  const clientSessionId = anthropicTools.extractClientSessionId(req);
  const convKey = anthropicTools.deriveConversationKey(
    messages, model, system, tools,
    req.socket?.remoteAddress, req.socket?.remotePort,
    clientSessionId,
  );
  if (POOL_REINJECT_THINKING) {
    log(`  convKey=${convKey} clientSessionId=${clientSessionId ? clientSessionId.slice(0, 8) + '…' : '(none)'}`);
  }

  // Sub-agent model pin (opt-in, RATLC_SUBAGENT_INHERIT_PARENT_MODEL=1). A
  // claude-code sub-agent shares the parent's session id but gets a distinct
  // convKey; if its routing model drifted from the main agent's (e.g. Cursor
  // proposed a cheaper model, or inherit resolved elsewhere), re-pin it to the
  // parent session's primary model HERE — before the pool routes on model — so
  // multi-agent stays on the same model/group. No-op unless the flag is set.
  {
    const pin = subagentModelPin.decide({
      clientSessionId,
      convKey,
      model,
      hasTools: Array.isArray(tools) && tools.length > 0,
    });
    if (pin.overridden) {
      log(`  subagent-model-pin: ${model || '(default)'} → ${pin.model} (session=${(clientSessionId || '').slice(0, 8)}… subagent convKey=${convKey})`);
      model = pin.model;
      routingModel = normalizeModelForRouting(model);
    }
  }

  // Decide what to send: tool_result(s) or user message.
  // Parallel-tool fix: a single POST may carry N tool_result blocks (one
  // per parallel tool_use the model emitted in its previous assistant
  // turn). All N must be forwarded to the same pool channel.
  const toolResults = findAllToolResults(lastMsg.content);
  const requestId = 'req-' + randomUUID().replace(/-/g, '').slice(0, 16);
  const clientBridgeToolResults = [];
  const replayedClientBridgeToolResults = [];
  const regularToolResults = [];
  for (const r of toolResults) {
    const entry = consumeClientToolResult(r.tool_use_id);
    if (entry) clientBridgeToolResults.push({ result: r, entry });
    else if (isClientBridgeToolUseId(r.tool_use_id)) {
      const consumed = getConsumedClientToolResult(r.tool_use_id);
      if (consumed) replayedClientBridgeToolResults.push({ result: r, entry: consumed });
      else regularToolResults.push(r);
    }
    else regularToolResults.push(r);
  }
  const forwardedFor = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  const requestIp = forwardedFor || req.headers['x-real-ip'] || req.socket?.remoteAddress || '';
  const reqLog = rememberRequest({
    requestId,
    startedAt: Date.now(),
    status: 'queued',
    kind: toolResults.length > 0 ? 'tool_result' : 'user_message',
    model: model || null,
    routeModel: routingModel || null,
    requestIp,
    forwardedFor: req.headers['x-forwarded-for'] || null,
    clientSessionId: clientSessionId || null,
    convKey,
    messageCount: messages.length,
    toolCount: Array.isArray(tools) ? tools.length : 0,
    toolResultCount: toolResults.length,
    clientBridgeToolResultCount: clientBridgeToolResults.length,
    replayedClientBridgeToolResultCount: replayedClientBridgeToolResults.length,
    inputTokensEstimate: Math.ceil(JSON.stringify(messages || []).length / 4),
    outputTokens: 0,
    serverWebSearchRequests: 0,
  });

  // HTTP header write is deferred until we receive the pool's route_decision
  // event so we can stamp x-ratlc-routed-to / x-ratlc-channel / x-ratlc-fallback
  // before sending the SSE preamble. writeHeadersOnce() is idempotent and
  // also called from the early-error path with no x-ratlc-* fields.
  let headersWritten = false;
  let routedTo = null;
  let routedChannel = null;
  let routeFallback = false;
  let routeFallbackReason = null;
  function writeHeadersOnce(extra) {
    if (headersWritten) return;
    headersWritten = true;
    const hdrs = {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
    };
    if (extra) Object.assign(hdrs, extra);
    try { res.writeHead(200, hdrs); } catch { /* client gone */ }
  }

  // Anthropic message bookkeeping
  const messageId = 'msg_' + randomUUID().replace(/-/g, '').slice(0, 24);
  let blockIdx = -1;
  let textBlockOpen = false;
  let outputTokens = 0;
  let stopReason = 'end_turn';
  let done = false;
  let finalStatusOverride = null;
  let finalErrorMessage = null;
  let toolUseEmitted = false;
  let thinkingBlockOpen = false;
  let thinkingBlockStarted = false;
  let thinkingBlockIndex = null;
  let thinkingAdapter = null;
  let emittedTextForDetection = '';
  let emittedThinkingForDetection = '';
  // True once GENUINE upstream visible content has reached the client — model
  // text, a server tool use, OR (when POOL_PROXY_THINKING_BLOCKS is on) real
  // reasoning forwarded as thinking deltas — as opposed to proxy-injected
  // notices, which also go through emitTextDelta and bump outputTokens/
  // textBlockOpen. Two consumers key off THIS:
  //   1. empty-assistant-turn detection (so emitting the "auto-retrying"
  //      breadcrumb doesn't fool the check into thinking the next still-empty
  //      turn produced content), and
  //   2. the silent-timeout recovery (a transparent replay is only safe when
  //      the client has seen nothing yet; once real thinking/text is out, a
  //      replay would duplicate it, so we surface a clean error instead).
  let realVisibleEmitted = false;
  // Bytes of upstream reasoning actually forwarded to the client this turn, and
  // a one-shot flag so we log the cap only once. Used by emitThinkingDelta to
  // enforce POOL_PROXY_THINKING_MAX_BYTES.
  let forwardedThinkingBytes = 0;
  let forwardedThinkingCapped = false;
  let thinkingCompletedCount = 0;
  let thinkingDurationMs = null;
  let rescuedHitCount = 0;
  const emittedToolUseKeys = new Set();
  const hallucinationFilter = new StreamingHallucinationFilter();
  const serverToolBlocks = new Map();
  const openServerTools = new Set();
  // Maps toolId to the server tool name ('web_search' or 'web_fetch') so
  // completion-path code (completeOpenServerTools) can pick the right
  // result_error type.
  const openServerToolNames = new Map();
  const visibleServerToolTraces = new Set();
  // Gap-1 fix (WEB_RESEARCH_GAPS.md): cache the query at started phase so
  // the completed phase has it available for the Bing-RSS fallback when
  // extraction fails.
  const serverToolQueries = new Map();  // toolId -> query string
  let serverWebSearchRequestCount = 0;
  let serverWebFetchRequestCount = 0;
  // Auto-retry state (see RATLC_RETRY_UPSTREAM_SILENT_MAX / RATLC_RETRY_EMPTY_TURN_MAX).
  // Snapshot of the original poolWrite payload (send_user_message OR
  // send_tool_results) — captured for replay on retry. Both action types
  // are retryable as of the release_consumed_ids IPC; tool_result retries
  // first ask pool-manager to forget the consumed-id entries so the
  // replay isn't rejected with "already consumed".
  let lastPoolRetryPayload = null;
  let silentRetryCount = 0;
  let emptyTurnRetryCount = 0;
  let staleToolResultRetryCount = 0;
  let abortRetryCount = 0;
  let continueRetryCount = 0;
  let pendingRetryTimer = null;
  let keepalivePingTimer = null;
  // Set true by cancelForClientDisconnect before it calls finishMessage(), so
  // the empty_assistant_turn retry path knows not to replay a turn for a
  // client that already hung up (would burn a fresh channel + quota for
  // output nobody reads).
  let clientGone = false;
  // `messageStarted` gates startMsg() so it can only fire once per request.
  // Was previously gated on `blockIdx === -1`, but startMsg doesn't bump
  // blockIdx — so the route_decision branch AND the error branch would
  // both call startMsg(), emitting two `message_start` SSE events. That
  // shape is malformed enough that claude-code rejects the response with
  // "API returned an empty or malformed response (HTTP 200)".
  let messageStarted = false;
  let visibleUpstreamEventSeen = false;
  let noVisibleEventTimer = null;
  // Per-served-model-type (fast vs non-fast). Initialized from the REQUESTED
  // model; recomputed from the SERVED model at arm time (resolveWatchdogThresholds).
  let noVisibleCeilingMs = typeThresholdMs(routingModel, 'NO_VISIBLE_EVENT_TIMEOUT_MS', 25000, 5000);
  // Option B — liveness-gated patience. The no-visible timer above is the
  // ABSOLUTE CEILING (max total wait for the first visible event, even while
  // upstream frames keep arriving). This second timer is the LIVENESS GAP: it
  // resets every time the worker forwards a `progress` event (a raw upstream
  // frame — the same signal the HTTP stall detector trusts), and fires if no
  // such frame arrives for the grace window, i.e. the channel has gone silent
  // at the wire and is treated as hung. Whichever fires first triggers the
  // same retry/notice path. Default 0 = disabled → pure ceiling behavior
  // (Option A). See project_channel_timeout_stack memory + UPSTREAM_RETRY_DESIGN.md.
  let livenessGapTimer = null;
  let noVisibleGraceMs = typeThresholdMs(routingModel, 'NO_VISIBLE_LIVENESS_GRACE_MS', 0, 0);
  // Parallel-tool-calls fix: after each tool_use, arm a *watchdog* timer.
  //
  // In theory the primary finalize signal during a tool_use turn is
  // `step_completed` from the worker (which mirrors cursor-agent's
  // `interactionUpdate.stepCompleted`). EMPIRICALLY though, the
  // `*-thinking-fast` Cursor variants we use don't emit stepCompleted
  // reliably — observed 0 step_completed events across many turns
  // 2026-05-15 — so the watchdog is the de-facto primary signal in
  // practice, not just a fallback. Each new tool_use resets the timer,
  // so as long as parallel tool_uses arrive within this window of each
  // other they're all batched into the same SSE message.
  //
  // Default 1000ms: observed inter-tool_use gaps within a single step
  // range from 150ms to 480ms; 1s gives comfortable headroom. If you
  // see "WATCHDOG" log lines AND find that the model still had more
  // tool_uses to emit after firing (the orphan bug), bump this higher
  // via POOL_TOOL_USE_WATCHDOG_MS. The busy-watchdog in pool-manager
  // (240s default) is the ultimate safety net for any channels that
  // do get stuck.
  //
  // POOL_TOOL_USE_DEBOUNCE_MS is kept as an env-var fallback for
  // backwards compat but should be retired in favor of
  // POOL_TOOL_USE_WATCHDOG_MS.
  let toolUseFinishTimer = null;
  const TOOL_USE_WATCHDOG_MS = parseInt(
    process.env.POOL_TOOL_USE_WATCHDOG_MS
      || process.env.POOL_TOOL_USE_DEBOUNCE_MS
      || '1000',
    10,
  );
  function armToolUseFinalizer() {
    if (toolUseFinishTimer) clearTimeout(toolUseFinishTimer);
    toolUseFinishTimer = setTimeout(() => {
      toolUseFinishTimer = null;
      if (done) return;
      log(`  → finalize tool_use turn (WATCHDOG @${TOOL_USE_WATCHDOG_MS}ms — step_completed never arrived) requestId=${requestId}`);
      stopReason = 'tool_use';
      finishMessage();
    }, TOOL_USE_WATCHDOG_MS);
  }
  function disarmToolUseFinalizer() {
    if (toolUseFinishTimer) {
      clearTimeout(toolUseFinishTimer);
      toolUseFinishTimer = null;
    }
  }
  // Shared fire path for both the absolute-ceiling timer and the liveness-gap
  // timer. `source` is 'ceiling' | 'liveness_gap' for the log line. Clears the
  // sibling timer so only one fires, then runs the existing retry/notice path.
  function _fireSilentTimeout(source) {
    if (noVisibleEventTimer) { clearTimeout(noVisibleEventTimer); noVisibleEventTimer = null; }
    if (livenessGapTimer) { clearTimeout(livenessGapTimer); livenessGapTimer = null; }
    if (done || visibleUpstreamEventSeen) return;
    const waitedMs = source === 'liveness_gap' ? noVisibleGraceMs : noVisibleCeilingMs;
    // If the client has already seen real reasoning forwarded as thinking
    // (POOL_PROXY_THINKING_BLOCKS) — or any other visible bytes — a transparent
    // replay would re-stream that output on a fresh channel, duplicating what
    // the caller already has. In that case do NOT replay: cancel the hung
    // channel so the pool recycles it, then surface a clean, resumable error.
    // Otherwise (nothing visible yet) take the existing transparent-retry path:
    // returns true if a retry was scheduled (the replay re-arms this watchdog),
    // false if retry is disabled/exhausted/unsafe.
    const clientSawBytes = realVisibleEmitted || toolUseEmitted;
    if (!clientSawBytes && tryRetryRequest('upstream_silent_timeout')) return;
    log(`  → no upstream frame timeout (${source} @${waitedMs}ms) requestId=${requestId} clientSawBytes=${clientSawBytes}${silentRetryCount ? ` (retries exhausted ${silentRetryCount}/${UPSTREAM_SILENT_RETRY_MAX})` : ''}`);
    finalStatusOverride = 'upstream_no_visible_event_timeout';
    finalErrorMessage = `No upstream frame (${source}) within ${waitedMs}ms${clientSawBytes ? ' after thinking/partial output' : ' after routing'}`;
    if (clientSawBytes) {
      // Reap the silent channel promptly instead of leaving it busy until the
      // pool-manager busy-watchdog (minutes later).
      poolWrite({ type: 'cancel_request', requestId, reason: `silent_after_visible:${source}` });
      emitTextDelta(
        `[proxy_notice] Cursor streamed reasoning but then went silent for ${waitedMs}ms without producing an answer; ` +
        'the channel was recycled. Please send your message again to continue.\n'
      );
    } else {
      emitTextDelta(
        `[proxy_notice] Cursor upstream accepted the request but did not emit text, thinking, tool_use, yield, or error within ${waitedMs}ms. ` +
        'The RATLC channel was likely waiting on an unrecognized Cursor exec message. Please retry after the channel is recycled.\n'
      );
    }
    stopReason = 'end_turn';
    finishMessage();
  }
  // Resolve the silent-timeout thresholds from the SERVED model's type (fast vs
  // non-fast) at arm time — routedTo is known by route_decision; falls back to
  // the requested model. With RATLC_ADAPTIVE_TIMEOUTS=1 the GAP is instead
  // derived from the current time-of-day regime's observed first-byte p99 for
  // this (model, payload-size), clamped to [ADAPTIVE_MIN_GAP_MS, ceiling].
  function resolveWatchdogThresholds() {
    const m = routedTo || routingModel;
    noVisibleCeilingMs = typeThresholdMs(m, 'NO_VISIBLE_EVENT_TIMEOUT_MS', 25000, 5000);
    noVisibleGraceMs = typeThresholdMs(m, 'NO_VISIBLE_LIVENESS_GRACE_MS', 0, 0);
    if (ADAPTIVE_TIMEOUTS && noVisibleGraceMs > 0) {
      let sug = null;
      try { sug = latencyMetrics.suggestGapMs(m, reqLog && reqLog.contentBytes); } catch { sug = null; }
      if (sug && Number.isFinite(sug.gapMs)) {
        noVisibleGraceMs = Math.min(noVisibleCeilingMs, Math.max(ADAPTIVE_MIN_GAP_MS, sug.gapMs));
        if (reqLog) { reqLog.adaptiveGapMs = noVisibleGraceMs; reqLog.adaptiveRegime = sug.regime; }
      }
    }
  }
  // Liveness-gap timer (Option B): clears + re-sets on each call. Re-armed by
  // the `progress` handler so it only fires after noVisibleGraceMs of true wire
  // silence. No-op when disabled (grace<=0) → ceiling-only.
  function armLivenessGapTimer() {
    if (noVisibleGraceMs <= 0 || done || visibleUpstreamEventSeen) return;
    if (livenessGapTimer) clearTimeout(livenessGapTimer);
    livenessGapTimer = setTimeout(() => {
      livenessGapTimer = null;
      _fireSilentTimeout('liveness_gap');
    }, noVisibleGraceMs);
  }
  function armNoVisibleEventTimer() {
    if (done || visibleUpstreamEventSeen) return;
    resolveWatchdogThresholds();   // pick per-type / adaptive values for the served model
    if (!noVisibleEventTimer) {
      noVisibleEventTimer = setTimeout(() => {
        noVisibleEventTimer = null;
        _fireSilentTimeout('ceiling');
      }, noVisibleCeilingMs);
    }
    // Arm the liveness-gap clock alongside the ceiling (no-op when disabled).
    armLivenessGapTimer();
  }
  function disarmNoVisibleEventTimer() {
    if (noVisibleEventTimer) {
      clearTimeout(noVisibleEventTimer);
      noVisibleEventTimer = null;
    }
    if (livenessGapTimer) {
      clearTimeout(livenessGapTimer);
      livenessGapTimer = null;
    }
  }
  function markVisibleUpstreamEvent() {
    visibleUpstreamEventSeen = true;
    disarmNoVisibleEventTimer();
  }
  // Thinking-frame liveness (Workstream C). A thinking frame proves the channel
  // is alive and producing real model output, so RETIRE the absolute first-byte
  // ceiling (the "did it ever produce content" question is answered) — but keep
  // the LIVENESS GAP armed and reset it, so a post-thinking silence (the
  // documented "went quiet after thinkingCompleted before the answer" hang) is
  // still caught. Deliberately does NOT set visibleUpstreamEventSeen: unlike
  // text/tool, thinking must leave the gap able to fire. When the gap is
  // disabled (grace<=0) clearing the ceiling reduces to the legacy behavior
  // where the first thinking token fully disarms the per-request watchdog.
  function markThinkingLiveness() {
    if (done || visibleUpstreamEventSeen) return;
    if (noVisibleEventTimer) {
      clearTimeout(noVisibleEventTimer);
      noVisibleEventTimer = null;
    }
    armLivenessGapTimer();
  }

  // Auto-retry helper. Returns true if a retry was scheduled, false if not.
  // Safe to call from both the watchdog (upstream_silent_timeout) and the
  // empty-turn detection in finishMessage (empty_assistant_turn).
  //
  // Safety invariants checked in order:
  //   1. The relevant per-symptom retry budget must allow it.
  //   2. No MODEL CONTENT can have been emitted yet (tool_use, text deltas,
  //      thinking deltas). The bare `message_start` SSE event from startMsg()
  //      is fine — it's a wrapper that's idempotent on the replay path. The
  //      callers ensure this invariant holds: the watchdog only fires when
  //      `!visibleUpstreamEventSeen` (so no text/thinking/tool_use has been
  //      forwarded yet); the empty-turn detection only fires when
  //      `outputTokens === 0 && !textBlockOpen`.
  //   3. We must have captured a send_user_message-shape payload as
  //      lastPoolRetryPayload. For send_user_message POSTs this is the
  //      original payload. For send_tool_results POSTs this is a
  //      pre-rendered fallback (full conversation history serialized
  //      via mode=full) — replaying THIS on a fresh channel gives the
  //      model a complete transcript including the tool_results that
  //      were stuck, and the model continues with fresh tool_use_ids.
  //      Replaying the raw send_tool_results would fail with "unknown
  //      anthropic_tool_use_id" (tool_use_ids are channel-bound and the
  //      original channel was killed by cancel_request); the fallback
  //      sidesteps that by re-establishing context on a new channel.
  //
  // For `empty_assistant_turn` the sticky channel that produced the empty
  // turn is still alive and would be selected again under session affinity;
  // we override sessionKey to null on the replay payload so the pool routes
  // round-robin to a different channel. For `upstream_silent_timeout` the
  // channel got killed by the cancel_request, so a fresh routing decision
  // is implicit.
  //
  // Note: an earlier version checked `messageStarted` and never fired for
  // upstream_silent_timeout because messageStarted is set the moment
  // route_decision arrives (long before the 25s watchdog). The real
  // invariant is "no content emitted", which the callers already ensure.
  function tryRetryRequest(symptom) {
    // No socket to deliver a replay into once the client hung up. Covers all
    // symptoms and any future call site (the finishMessage guard is the
    // primary one; this is belt-and-suspenders).
    if (clientGone) return false;
    if (done && symptom !== 'empty_assistant_turn') return false;
    if (!lastPoolRetryPayload) return false;
    if (toolUseEmitted) return false;
    // Forwarded thinking (POOL_PROXY_THINKING_BLOCKS) is client-visible output;
    // replaying would re-stream it. The silent-timeout caller already branches
    // on this, but guard here too so no symptom can blind-replay over content
    // the client has already seen.
    if (realVisibleEmitted) return false;
    // lastPoolRetryPayload is ALWAYS a send_user_message-shape payload:
    //   - For send_user_message POSTs: the original payload itself.
    //   - For send_tool_results POSTs: a fallback payload pre-rendered
    //     in the tool_results branch (full conversation history rendered
    //     into one bajie_yield text via mode=full). The fresh channel
    //     gets the complete transcript including the tool_results that
    //     would otherwise have been lost; the model continues from
    //     there with fresh tool_use_ids.
    // No action-type check needed — replay always send_user_message.
    const replayAction = lastPoolRetryPayload.action || 'send_user_message';
    const budget = symptom === 'upstream_silent_timeout'
      ? UPSTREAM_SILENT_RETRY_MAX
      : symptom === 'stale_tool_result'
      ? STALE_TOOL_RESULT_RETRY_MAX
      : symptom === 'upstream_abort'
      ? UPSTREAM_ABORT_RETRY_MAX
      : EMPTY_TURN_RETRY_MAX;
    if (budget <= 0) return false;
    const counterBefore = symptom === 'upstream_silent_timeout'
      ? silentRetryCount
      : symptom === 'stale_tool_result'
      ? staleToolResultRetryCount
      : symptom === 'upstream_abort'
      ? abortRetryCount
      : emptyTurnRetryCount;
    if (counterBefore >= budget) return false;
    const counterAfter = counterBefore + 1;
    if (symptom === 'upstream_silent_timeout') silentRetryCount = counterAfter;
    else if (symptom === 'stale_tool_result') staleToolResultRetryCount = counterAfter;
    else if (symptom === 'upstream_abort') abortRetryCount = counterAfter;
    else emptyTurnRetryCount = counterAfter;
    log(`  → retry: ${symptom} attempt ${counterAfter}/${budget} action=${replayAction} requestId=${requestId}`);
    patchRequest(requestId, {
      retryCount: silentRetryCount + emptyTurnRetryCount + staleToolResultRetryCount + abortRetryCount,
      lastRetrySymptom: symptom,
      lastRetryAt: Date.now(),
    });
    if (RETRY_EMIT_NOTICE && (silentRetryCount + emptyTurnRetryCount + staleToolResultRetryCount + abortRetryCount) === 1) {
      // Only emit on the first overall retry so the client gets ONE breadcrumb,
      // not a wall of notices on repeated retries.
      emitTextDelta(`[proxy_notice] ${symptom} — auto-retrying through a fresh channel...\n`);
    }
    // For upstream_silent_timeout: cancel kills the stuck channel, releasing
    // it for recycling. For empty_assistant_turn: the channel is fine but we
    // want to break affinity, so we cancel anyway (no-op on already-finished
    // request) and override sessionKey in the replay. For upstream_abort: the
    // channel already died/aborted (cancel is a no-op cleanup), and we null
    // sessionKey so routing avoids re-picking the same stale sticky channel.
    poolWrite({ type: 'cancel_request', requestId, reason: `retry:${symptom}` });
    const replayPayload = (symptom === 'empty_assistant_turn' || symptom === 'upstream_abort')
      ? { ...lastPoolRetryPayload, sessionKey: null }
      : lastPoolRetryPayload;
    // Reset per-request state so the new attempt looks like a fresh route.
    // We DO NOT clear lastPoolRetryPayload — successive retries replay
    // the same input. We DO clear watchdog state.
    disarmNoVisibleEventTimer();
    disarmToolUseFinalizer();
    visibleUpstreamEventSeen = false;
    if (symptom === 'empty_assistant_turn') {
      // finishMessage was about to commit — reverse the done flag so the
      // replay path can re-issue and the new response can stream.
      done = false;
      stopReason = null;
      finalStatusOverride = null;
      finalErrorMessage = null;
    }
    if (pendingRetryTimer) clearTimeout(pendingRetryTimer);
    pendingRetryTimer = setTimeout(() => {
      pendingRetryTimer = null;
      if (done) return;  // client disconnected during delay
      log(`  → retry: ${symptom} firing replay requestId=${requestId} attempt=${counterAfter} action=${replayAction}`);
      poolWrite(replayPayload);
      armNoVisibleEventTimer();
    }, RETRY_DELAY_MS);
    return true;
  }

  // Self-driven continuation after a mid-TEXT upstream abort. The partial text
  // is already on the client's screen, so we can't replay from scratch (that
  // would duplicate it). Instead we keep the SSE message OPEN, hand a fresh
  // channel the full conversation + the partial assistant text + an
  // instruction to continue from exactly where it stopped, and stream that
  // continuation into the same text block. Returns true if a continuation was
  // scheduled (caller must NOT finishMessage), false to fall back to the notice.
  function tryContinueAfterAbort() {
    if (clientGone || done) return false;
    if (CONTINUE_AFTER_ABORT_MAX <= 0) return false;
    if (continueRetryCount >= CONTINUE_AFTER_ABORT_MAX) return false;
    const partial = (emittedTextForDetection || '').trim();
    if (!partial) return false;  // nothing shown to continue from → use the notice
    let continuationPayload;
    try {
      // Render: [full history] + [assistant: partial so far] + [user: continue].
      // The model treats the partial as its own prior turn and continues it.
      const contMessages = [
        ...messages,
        { role: 'assistant', content: partial },
        { role: 'user', content: CONTINUE_AFTER_ABORT_INSTRUCTION },
      ];
      const thinkingTurns = POOL_REINJECT_THINKING ? thinkingBuffer.getForConvKey(convKey) : [];
      const content = buildFullContextCursorMcpContent({ messages: contMessages, system, tools, thinkingTurns });
      continuationPayload = {
        type: 'request', requestId, action: 'send_user_message',
        model: routingModel || null,
        requestedModel: model || null,
        text: cursorMcpContentToText(content),
        content,
        system: extractSystemPrompt(system),
        tools: tools || [],
        sessionKey: null,           // fresh channel — the aborted one is dead
        contextMode: 'full',
        hybridReason: 'continue-after-abort',
      };
    } catch (e) {
      log(`  ↪ continue-after-abort build failed: ${e.message || e}`);
      return false;
    }
    continueRetryCount++;
    log(`  → continue-after-abort attempt ${continueRetryCount}/${CONTINUE_AFTER_ABORT_MAX} requestId=${requestId} partialChars=${partial.length}`);
    patchRequest(requestId, {
      retryCount: silentRetryCount + emptyTurnRetryCount + staleToolResultRetryCount + abortRetryCount + continueRetryCount,
      lastRetrySymptom: 'continue_after_abort',
      lastRetryAt: Date.now(),
    });
    // Keep the message OPEN and preserve emitted content (textBlockOpen,
    // blockIdx, outputTokens). Clear only the error/watchdog state. Point the
    // silent-timeout retry payload at the CONTINUATION so a silent continuation
    // re-continues (instead of replaying the original → duplication).
    finalStatusOverride = null;
    finalErrorMessage = null;
    stopReason = null;
    lastPoolRetryPayload = continuationPayload;
    poolWrite({ type: 'cancel_request', requestId, reason: 'continue_after_abort' });
    disarmNoVisibleEventTimer();
    disarmToolUseFinalizer();
    visibleUpstreamEventSeen = false;
    if (pendingRetryTimer) clearTimeout(pendingRetryTimer);
    pendingRetryTimer = setTimeout(() => {
      pendingRetryTimer = null;
      if (done) return;  // client disconnected during delay
      log(`  → continue-after-abort firing replay requestId=${requestId} attempt=${continueRetryCount}`);
      poolWrite(continuationPayload);
      armNoVisibleEventTimer();
    }, RETRY_DELAY_MS);
    return true;
  }

  function startMsg() {
    if (messageStarted) return;
    messageStarted = true;
    writeHeadersOnce();
    // Cumulative input-token estimate for /context display.
    //
    // The previous version reported only the LAST user message's char/4
    // count — which for short messages came out as e.g. "4/600k tokens
    // (0%)" in claude-code's /context bar even though the actual sent
    // payload was hundreds of thousands of tokens. Claude-code reads
    // this value from message_start.usage.input_tokens and uses it as
    // the "used" number in the top line of /context.
    //
    // `text` is the full rendered payload (system + tools + entire
    // messages history) we sent to the pool, captured via closure from
    // the outer handleMessagesRequest scope. By the time startMsg() is
    // called via route_decision, the payload has been built. The /3.5
    // chars-per-token ratio matches what handleCountTokens uses so the
    // category breakdown (computed via count_tokens RPCs) and the top
    // number stay consistent.
    //
    // Fallback to the old last-message estimate if `text` somehow isn't
    // set yet (defensive — shouldn't happen with the route_decision
    // sequencing but keeps the message_start well-formed).
    const inputTokensEstimate = (typeof text === 'string' && text.length > 0)
      ? Math.ceil(text.length / 3.5)
      : (extractTextFromContent(lastMsg.content).length / 4 | 0);
    sseWrite(res, 'message_start', {
      type: 'message_start',
      message: {
        id: messageId, type: 'message', role: 'assistant',
        content: [], model: model || 'claude-opus-4-7',
        stop_reason: null, stop_sequence: null,
        usage: {
          input_tokens: inputTokensEstimate,
          output_tokens: 0,
          cache_creation_input_tokens: 0,
          cache_read_input_tokens: 0,
          server_tool_use: serverWebSearchRequestCount > 0 || serverWebFetchRequestCount > 0
            ? { web_search_requests: serverWebSearchRequestCount, web_fetch_requests: serverWebFetchRequestCount }
            : null,
          service_tier: 'standard',
        },
      },
    });
    sseWrite(res, 'ping', { type: 'ping' });
    armKeepalivePing();
  }

  // Recurring SSE ping so a slow first-byte (or any mid-stream silent gap)
  // doesn't trip the client's read-idle timeout. Idempotent; cleared by
  // disarmKeepalivePing(). The ping is harmless interleaved with real
  // content blocks — it matches Anthropic's own keep-alive cadence.
  function armKeepalivePing() {
    if (KEEPALIVE_PING_MS <= 0 || keepalivePingTimer || done) return;
    keepalivePingTimer = setInterval(() => {
      if (done || res.writableEnded) { disarmKeepalivePing(); return; }
      sseWrite(res, 'ping', { type: 'ping' });
    }, KEEPALIVE_PING_MS);
    if (keepalivePingTimer.unref) keepalivePingTimer.unref();
  }
  function disarmKeepalivePing() {
    if (keepalivePingTimer) {
      clearInterval(keepalivePingTimer);
      keepalivePingTimer = null;
    }
  }

  function startTextBlock() {
    // Same ordering invariant as emitToolUseBlock — thinking block first
    // when the client asked for thinking. See emitPlaceholderThinkingBlock.
    stopThinkingBlock();
    emitPlaceholderThinkingBlock();
    blockIdx++;
    sseWrite(res, 'content_block_start', {
      type: 'content_block_start', index: blockIdx,
      content_block: { type: 'text', text: '' },
    });
    textBlockOpen = true;
  }

  function emitTextDelta(text) {
    if (!textBlockOpen) startTextBlock();
    outputTokens += Math.ceil(text.length / 4);
    sseWrite(res, 'content_block_delta', {
      type: 'content_block_delta', index: blockIdx,
      delta: { type: 'text_delta', text },
    });
  }

  function stopTextBlock() {
    if (!textBlockOpen) return;
    sseWrite(res, 'content_block_stop', { type: 'content_block_stop', index: blockIdx });
    textBlockOpen = false;
  }

  function startThinkingBlock() {
    if (thinkingBlockOpen) return;
    stopTextBlock();
    blockIdx++;
    thinkingBlockIndex = blockIdx;
    thinkingBlockStarted = true;
    thinkingAdapter = new ProxyThinkingBlockAdapter({
      source: 'pool',
      convKey,
      requestId,
      blockIndex: thinkingBlockIndex,
    });
    sseWrite(res, 'content_block_start', {
      type: 'content_block_start',
      index: thinkingBlockIndex,
      content_block: { type: 'thinking', thinking: '' },
    });
    thinkingBlockOpen = true;
    // A real proxy-local thinking block satisfies the same ordering
    // requirement as the placeholder; do not emit both in one assistant turn.
    placeholderThinkingEmitted = true;
  }

  function emitThinkingDelta(text) {
    if (!text) return;
    // A4: cap total forwarded reasoning per turn so a pathological thinking
    // stream can't flood the client SSE. Over the cap we stop forwarding; the
    // text is still captured upstream for detection/reinjection.
    if (forwardedThinkingBytes >= POOL_PROXY_THINKING_MAX_BYTES) {
      if (!forwardedThinkingCapped) {
        forwardedThinkingCapped = true;
        log(`  thinking-forward cap hit (${POOL_PROXY_THINKING_MAX_BYTES}B) req=${requestId} — further reasoning not forwarded`);
      }
      return;
    }
    // A1: (re)open a thinking block whenever one isn't currently open — this
    // covers the first thinking block AND re-opening after text. Cursor can
    // interleave thinking↔text within a turn, and Anthropic SSE permits ordered
    // thinking/text/thinking blocks. The prior guards (`blockIdx >= 0` /
    // `thinkingBlockStarted`) silently dropped any thinking that arrived after a
    // block had opened, losing reasoning mid-stream with no trace.
    if (!thinkingBlockOpen) {
      if (thinkingBlockStarted && blockIdx >= 0) {
        log(`  thinking re-open after prior block req=${requestId} (interleaved thinking/text)`);
      }
      startThinkingBlock();
    }
    forwardedThinkingBytes += Buffer.byteLength(text, 'utf8');
    if (thinkingAdapter) thinkingAdapter.append(text);
    sseWrite(res, 'content_block_delta', {
      type: 'content_block_delta',
      index: thinkingBlockIndex,
      delta: { type: 'thinking_delta', thinking: text },
    });
  }

  function stopThinkingBlock() {
    if (!thinkingBlockOpen) return;
    const signature = thinkingAdapter
      ? thinkingAdapter.signature({ blockIndex: thinkingBlockIndex })
      : null;
    if (signature) {
      sseWrite(res, 'content_block_delta', {
        type: 'content_block_delta',
        index: thinkingBlockIndex,
        delta: { type: 'signature_delta', signature },
      });
    }
    sseWrite(res, 'content_block_stop', { type: 'content_block_stop', index: thinkingBlockIndex });
    thinkingBlockOpen = false;
    thinkingBlockIndex = null;
    thinkingAdapter = null;
  }

  // Whether we've emitted a placeholder thinking block already on this
  // assistant turn. Anthropic emits at most one thinking block per
  // assistant turn (before any tool_use); we mirror that.
  let placeholderThinkingEmitted = false;
  function emitPlaceholderThinkingBlock() {
    if (placeholderThinkingEmitted) return;
    if (!clientThinkingEnabled) return;
    stopTextBlock();
    blockIdx++;
    sseWrite(res, 'content_block_start', {
      type: 'content_block_start', index: blockIdx,
      content_block: { type: 'thinking', thinking: '' },
    });
    // Single short thinking_delta with placeholder text so the block has
    // content. The text doesn't need to mean anything to the client; it
    // just satisfies the parser's "thinking block must exist" requirement.
    sseWrite(res, 'content_block_delta', {
      type: 'content_block_delta', index: blockIdx,
      delta: { type: 'thinking_delta', thinking: '(thinking captured upstream; not forwarded by proxy)' },
    });
    // Signature delta: mark this as proxy-local so echoed placeholder blocks
    // stay UI/protocol-only and are stripped from future prompt rendering.
    const signature = makeProxyThinkingSignature({
      source: 'pool-placeholder',
      convKey,
      requestId,
      blockIndex: blockIdx,
      text: '(thinking captured upstream; not forwarded by proxy)',
    });
    sseWrite(res, 'content_block_delta', {
      type: 'content_block_delta', index: blockIdx,
      delta: { type: 'signature_delta', signature },
    });
    sseWrite(res, 'content_block_stop', { type: 'content_block_stop', index: blockIdx });
    placeholderThinkingEmitted = true;
  }

  function emitToolUseBlock(anthropicId, toolName, args) {
    // claude-code with interleaved-thinking beta REQUIRES a thinking
    // block before any tool_use block, otherwise it rejects the whole
    // SSE as "empty or malformed". Emit a placeholder once per turn.
    stopThinkingBlock();
    emitPlaceholderThinkingBlock();
    stopTextBlock();
    blockIdx++;
    sseWrite(res, 'content_block_start', {
      type: 'content_block_start', index: blockIdx,
      content_block: { type: 'tool_use', id: anthropicId, name: toolName, input: {} },
    });
    sseWrite(res, 'content_block_delta', {
      type: 'content_block_delta', index: blockIdx,
      delta: { type: 'input_json_delta', partial_json: '' },
    });
    sseWrite(res, 'content_block_delta', {
      type: 'content_block_delta', index: blockIdx,
      delta: { type: 'input_json_delta', partial_json: JSON.stringify(args || {}) },
    });
    sseWrite(res, 'content_block_stop', { type: 'content_block_stop', index: blockIdx });
    toolUseEmitted = true;
    markVisibleUpstreamEvent();
    try { emittedToolUseKeys.add(`${toolName}|${JSON.stringify(args || {})}`); }
    catch { emittedToolUseKeys.add(`${toolName}|?`); }
  }

  function normalizeServerToolId(id) {
    // Anthropic requires server_tool_use ids to match ^srvtoolu_[a-zA-Z0-9_]+$
    // — note: NO dashes. Cursor's native tool id is arbitrary (typically a UUID
    // WITH dashes), so we must (a) strip dashes/any other char to underscores,
    // and (b) guarantee the srvtoolu_ prefix. Without this, a session that used
    // Cursor WebSearch/WebFetch can't be RESUMED against the real Anthropic API:
    // claude-code replays the stored server_tool_use block and the API 400s with
    // "server_tool_use.id: String should match pattern '^srvtoolu_[a-zA-Z0-9_]+$'".
    // Idempotent: an already-normalized id round-trips unchanged (the result
    // block reuses this same id as tool_use_id, and completeOpenServerTools
    // re-feeds it, so the started/completed pair must stay identical).
    const base = String(id || '').replace(/^srvtoolu_/i, '').replace(/[^A-Za-z0-9_]/g, '_');
    return 'srvtoolu_' + (base || randomUUID().replace(/-/g, ''));
  }

  async function emitServerToolUseEvent(event) {
    if (done) return;
    if (!event || (event.name !== 'web_search' && event.name !== 'web_fetch')) return;
    startMsg();
    stopThinkingBlock();
    emitPlaceholderThinkingBlock();
    stopTextBlock();
    const toolId = normalizeServerToolId(event.id);
    const isWebFetch = event.name === 'web_fetch';
    const blockName = isWebFetch ? 'web_fetch' : 'web_search';

    if (event.phase === 'started') {
      if (serverToolBlocks.has(toolId)) return;
      // Gap-1 (WebSearch only): remember the query so the completed phase
      // can fall back to Bing-RSS if Cursor's content extraction fails.
      const traceValue = String(isWebFetch ? event.input?.url || '' : event.input?.query || '')
        .replace(/\s+/g, ' ')
        .trim();
      if (!isWebFetch && traceValue) serverToolQueries.set(toolId, traceValue);
      if (RENDER_SERVER_TOOL_TEXT && !visibleServerToolTraces.has(toolId)) {
        visibleServerToolTraces.add(toolId);
        emitTextDelta(`[Cursor ${isWebFetch ? 'WebFetch' : 'WebSearch'}] ${traceValue || '(input unavailable)'}\n`);
        stopTextBlock();
      }
      blockIdx++;
      const idx = blockIdx;
      serverToolBlocks.set(toolId, idx);
      openServerTools.add(toolId);
      openServerToolNames.set(toolId, blockName);
      sseWrite(res, 'content_block_start', {
        type: 'content_block_start',
        index: idx,
        content_block: {
          type: 'server_tool_use',
          id: toolId,
          name: blockName,
        },
      });
      sseWrite(res, 'content_block_delta', {
        type: 'content_block_delta',
        index: idx,
        delta: {
          type: 'input_json_delta',
          partial_json: JSON.stringify(event.input || {}),
        },
      });
      sseWrite(res, 'content_block_stop', { type: 'content_block_stop', index: idx });
      if (isWebFetch) serverWebFetchRequestCount++;
      else serverWebSearchRequestCount++;
      const logInput = isWebFetch ? event.input?.url || '' : event.input?.query || '';
      log(`→ server_tool_use to client: name=${blockName} input=${JSON.stringify(logInput).slice(0, 160)} id=${toolId}`);
      return;
    }

    if (event.phase === 'completed') {
      if (!serverToolBlocks.has(toolId)) {
        await emitServerToolUseEvent({ ...event, phase: 'started' });
      }
      openServerTools.delete(toolId);
      openServerToolNames.delete(toolId);
      // Capture blockIdx BEFORE any await — emitServerToolUseEvent awaits
      // Bing-RSS in the WebSearch fallback path, and concurrent emits could
      // bump blockIdx out from under us if we held it as a closure ref.
      blockIdx++;
      const idx = blockIdx;
      let content = Array.isArray(event.content) ? event.content : (event.content || null);
      let resultSource = 'cursor';
      // Gap-1 fix: Bing-RSS fallback only applies to WebSearch (not WebFetch
      // — for fetches, if extraction fails the URL-specific response is what
      // the model wanted; a search wouldn't substitute meaningfully).
      if (!isWebFetch && !Array.isArray(content)) {
        const cachedQuery = serverToolQueries.get(toolId) || '';
        if (cachedQuery && WEBSEARCH_FALLBACK_ENABLED) {
          try {
            const results = await performWebSearch(cachedQuery, {
              timeoutMs: WEBSEARCH_FALLBACK_TIMEOUT_MS,
              maxResults: 5,
            });
            if (results && results.length > 0 && !resultsLookGeneric(results)) {
              content = results.map((r) => ({
                type: 'web_search_result',
                url: r.url,
                title: r.title || '',
                encrypted_content: r.snippet || '',
                page_age: null,
              }));
              resultSource = 'proxy_bing_fallback';
              log(`  ↪ websearch fallback: substituted ${results.length} Bing results for failed extraction (q="${cachedQuery.slice(0, 80)}")`);
            } else {
              log(`  ↪ websearch fallback: Bing returned ${results?.length || 0} results (generic=${resultsLookGeneric(results)}) — emitting error block`);
            }
          } catch (e) {
            log(`  ↪ websearch fallback: Bing failed (${e.message || e}) — emitting error block`);
          }
        }
      }
      // Final fallback: if we still don't have a content array, emit the
      // original error block (separate types for web_search vs web_fetch).
      if (!Array.isArray(content)) {
        content = {
          type: isWebFetch ? 'web_fetch_tool_result_error' : 'web_search_tool_result_error',
          error_code: 'unavailable',
        };
      }
      sseWrite(res, 'content_block_start', {
        type: 'content_block_start',
        index: idx,
        content_block: {
          type: isWebFetch ? 'web_fetch_tool_result' : 'web_search_tool_result',
          tool_use_id: toolId,
          content,
        },
      });
      sseWrite(res, 'content_block_stop', { type: 'content_block_stop', index: idx });
      log(`→ ${blockName}_tool_result to client: id=${toolId} results=${Array.isArray(content) ? content.length : 'error'} source=${resultSource}`);
      serverToolQueries.delete(toolId);
    }
  }

  function completeOpenServerTools(reason) {
    if (done || openServerTools.size === 0) return;
    for (const toolId of [...openServerTools]) {
      const name = openServerToolNames.get(toolId) || 'web_search';
      emitServerToolUseEvent({
        phase: 'completed',
        name,
        id: toolId,
        content: {
          type: name === 'web_fetch' ? 'web_fetch_tool_result_error' : 'web_search_tool_result_error',
          error_code: 'unavailable',
        },
        error: reason || `Cursor backend ${name === 'web_fetch' ? 'WebFetch' : 'WebSearch'} completed without exposing result metadata to the proxy.`,
      });
    }
  }

  function tryRescueHallucinatedToolCalls() {
    const combined = emittedTextForDetection + (emittedThinkingForDetection ? '\n' + emittedThinkingForDetection : '');
    if (!combined) return 0;
    const hits = anthropicTools.parseHallucinatedToolCalls(combined);
    if (hits.length <= rescuedHitCount) return 0;

    const registered = new Set(
      (tools || []).flatMap((t) => [t && t.name, t && t.toolName]).filter(Boolean),
    );

    let added = 0;
    for (const hit of hits.slice(rescuedHitCount)) {
      if (anthropicTools.shouldDropClientWebLookupToolName(hit.name)) {
        log(`  hallucinated-tool-call ignored: ${hit.name} (web lookup is Cursor-native only)`);
        continue;
      }

      const canonical = anthropicTools.canonicalizeHallucinatedToolName(hit.name, registered);
      const normalizedArgs = anthropicTools.normalizeHallucinatedToolArgs(canonical, hit.args || {});
      if (anthropicTools.shouldDropClientWebLookupToolName(canonical)) {
        log(`  hallucinated-tool-call ignored: ${hit.name} → ${canonical} (web lookup is Cursor-native only)`);
        continue;
      }
      if (looksLikeAgentToolPlaceholderWrite(canonical, normalizedArgs)) {
        log(`  hallucinated-tool-call ignored: ${hit.name} (empty agent-tools placeholder write)`);
        continue;
      }

      const key = (() => {
        try { return `${canonical}|${JSON.stringify(normalizedArgs || {})}`; }
        catch { return `${canonical}|?`; }
      })();
      if (emittedToolUseKeys.has(key)) continue;
      emittedToolUseKeys.add(key);

      const synthId = anthropicTools.encodeToolUseId(
        convKey,
        '',
        `toolu_synth_${randomUUID().replace(/-/g, '').slice(0, 16)}`,
        clientSessionId || '',
      );
      emitToolUseBlock(synthId, canonical, normalizedArgs);
      log(`  🩹 hallucinated-tool-call rescued: ${hit.name}${canonical !== hit.name ? ` → ${canonical}` : ''} id=${synthId}`);
      added++;
    }

    rescuedHitCount = hits.length;
    return added;
  }

  // Is this pool error a TRANSIENT upstream failure worth retrying on a fresh
  // channel? Used by the before-content abort-retry. Conservative: returns
  // false for anything that looks persistent (auth / quota / rate-limit / no
  // ready channel — retrying those amplifies the problem or can't help), and
  // true only for known transient transport/stream failures. Default false.
  function isRetryableUpstreamAbort(message, code) {
    const t = String(message || '');
    // Persistent — never retry (would hammer the upstream or loop forever).
    if (/not.?logged.?in|unauthor|auth[_ ]?error|forbidden|quota|resource_exhausted|rate.?limit|payment|unpaid|insufficient|no ready RATLC channel|\b40[13]\b|\b429\b/i.test(t)) {
      return false;
    }
    // Transient transport/stream failures that left no usable content.
    return /abort|reset|ECONNRESET|EPIPE|socket hang ?up|nghttp2|stall|no longer alive|channel .* died|busy-watchdog|unhandled Cursor exec|ERR_STREAM|stream error|premature|unexpected_turn_ended/i.test(t);
  }

  function classifySoftenedPoolError(message, code) {
    const text = String(message || '');
    if (code === 'no_ready_timeout' || /no ready RATLC channel/i.test(text)) {
      return {
        status: 'no_ready_timeout',
        error: text,
        text:
          '[proxy_notice] RATLC pool currently has no ready Cursor channel for this model. ' +
          'The request reached the server, but Cursor upstream did not provide a usable Agent session before the queue timeout. ' +
          'Check /ratlc/dashboard for Ready/Opening state and upstream resource_exhausted/rate-limit logs.\n',
      };
    }
    if (/already consumed anthropic_tool_use_id/i.test(text)) {
      return {
        status: 'stale_tool_result',
        error: text,
        text: `[proxy_notice] ${text}. The client replayed a tool_result that the proxy had already consumed. Please send a fresh user message to continue.\n`,
      };
    }
    if (/unknown anthropic_tool_use_id/i.test(text) || /tool_use_ids span multiple channels/i.test(text)) {
      return {
        status: 'stale_tool_result',
        error: text,
        text: `[proxy_notice] ${text}. The tool_result does not match the active RATLC channel. Please send a fresh user message to continue.\n`,
      };
    }
    if (/channel .* (died|no longer alive)/i.test(text)) {
      return {
        status: 'channel_died',
        error: text,
        text: `[proxy_notice] ${text}. The active RATLC channel exited while handling the request. Please retry after the pool opens a ready channel.\n`,
      };
    }
    if (/busy-watchdog timeout/i.test(text)) {
      return {
        status: 'channel_timeout',
        error: text,
        text: `[proxy_notice] ${text}. The active RATLC channel stopped making progress and was recycled. Please retry after the pool opens a ready channel.\n`,
      };
    }
    if (/unhandled Cursor exec message/i.test(text)) {
      return {
        status: 'unhandled_cursor_exec',
        error: text,
        text: `[proxy_notice] ${text}. Cursor upstream sent an exec message this proxy version cannot decode. The channel was closed instead of leaving the outer API stream malformed; retry after the pool opens a replacement channel.\n`,
      };
    }
    return null;
  }

  function finishMessage() {
    if (done) return;
    try {
      const rescued = tryRescueHallucinatedToolCalls();
      if (rescued > 0) stopReason = 'tool_use';
    } catch { /* never let textual tool-call rescue crash finalization */ }
    // Auto-retry hook for empty_assistant_turn. Detected BEFORE we set done
    // so the retry can re-issue cleanly. Empty visible output is always a
    // degraded outcome from the caller's POV — retry up to budget regardless
    // of whether thinking was captured. Captured thinking carries forward to
    // the next request via thinkingBuffer either way.
    //
    // Skip entirely when the client already disconnected (clientGone): the
    // turn looks "empty" only because we never got to stream it, and there's
    // no open socket to deliver a replay into — retrying would burn a fresh
    // channel + quota for output nobody reads.
    if (!clientGone && !toolUseEmitted && !realVisibleEmitted && EMPTY_TURN_RETRY_MAX > 0
        && !recentlyRateLimited(routedTo || routingModel)) {  // a rate-limited model won't fill on retry — don't burn channels/quota
      if (tryRetryRequest('empty_assistant_turn')) {
        // tryRetryRequest already reset done=false and scheduled the replay.
        // Do NOT proceed with the rest of finishMessage — let the replay take
        // over. The next finishMessage call (from the replayed turn) will
        // commit/close as normal.
        return;
      }
    }
    done = true;
    // Disarm watchdog centrally so the bookkeeping is symmetric across all
    // exit paths (step_completed / yield / error / watchdog / disconnect).
    // The watchdog-fired path used to leave a dangling reference because
    // the disarm was at the call sites of the other paths only.
    disarmToolUseFinalizer();
    disarmNoVisibleEventTimer();
    disarmKeepalivePing();
    if (!toolUseEmitted && !realVisibleEmitted) {
      const emptyModel = routedTo || routingModel || '';
      if (recentlyRateLimited(emptyModel)) {
        // We just saw this model rate-limited upstream; an empty turn now is the
        // same throttle wearing a different hat. Report it as such (not "empty").
        emitTextDelta(`\n[proxy_notice] Model "${emptyModel}" is being rate-limited upstream by Cursor (recent resource_exhausted on this model) — it accepted the request but returned no content. This is a per-model quota on the Cursor account(s) behind the proxy, not a proxy error. Wait and retry, or switch to a model that has quota (e.g. your opus/4.6 groups).\n`);
      } else if (emptyTurnRetryCount > 0) {
        // We retried on fresh channels and still got nothing. Replace the bare
        // "auto-retrying" breadcrumb (which misleadingly implies we're still
        // trying) with a clear, actionable exhaustion message — and name the most
        // common real cause for a brand-new/premium model.
        emitTextDelta(`\n[proxy_notice] The model returned an empty response ${emptyTurnRetryCount + 1} times in a row (auto-retry exhausted after ${emptyTurnRetryCount} fresh-channel attempt(s)). For a new or premium model (e.g. Fable 5) this usually means it is rate-limited or not provisioned on the upstream Cursor account — check \`ratlc tui\` token-health, try a different model, or resend if it was just transient load.\n`);
      } else {
        emitTextDelta('[proxy_notice] Cursor ended this turn without visible text or tool calls. Any upstream thinking was captured for the next request, but there is no assistant-visible content to display.\n');
      }
    }
    const trailing = hallucinationFilter.flush();
    if (trailing) emitTextDelta(trailing);
    stopThinkingBlock();
    stopTextBlock();
    // Commit any accumulated thinking text into a stored turn under this
    // convKey BEFORE emitting message_stop. Each /v1/messages POST maps
    // to exactly one assistant message in the client's messages[]
    // history, so one commit per finishMessage is correct (regardless of
    // whether stopReason was end_turn or tool_use). Mirrors server.js's
    // onTurnEnded → thinkingHistory.recordTurnThinking path.
    if (POOL_REINJECT_THINKING) thinkingBuffer.commitTurn(convKey);
    finishRequestLog(requestId, {
      status: finalStatusOverride || (stopReason === 'tool_use' ? 'waiting_tool_result' : 'completed'),
      stopReason,
      outputTokens,
      serverWebSearchRequests: serverWebSearchRequestCount,
      serverWebFetchRequests: serverWebFetchRequestCount,
      thinkingCompletedCount,
      thinkingDurationMs,
      channelId: routedChannel,
      servedModel: routedTo,
      fallback: routeFallback,
      fallbackReason: routeFallbackReason,
      error: finalErrorMessage,
    });
    sseWrite(res, 'message_delta', {
      type: 'message_delta',
      delta: { stop_reason: stopReason, stop_sequence: null },
      usage: {
        input_tokens: 0,
        output_tokens: outputTokens,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
        server_tool_use: serverWebSearchRequestCount > 0 || serverWebFetchRequestCount > 0
          ? { web_search_requests: serverWebSearchRequestCount, web_fetch_requests: serverWebFetchRequestCount }
          : null,
      },
    });
    sseWrite(res, 'message_stop', { type: 'message_stop' });
    try { res.end(); } catch { /* ignore */ }
    reqHandlers.delete(requestId);
  }

  // startMsg() is deferred until headers are written (after route_decision
  // arrives). For the rare case the pool never emits route_decision (e.g.
  // socket error), the error handler below will call writeHeadersOnce()
  // with no x-ratlc-* fields and then startMsg() + finishMessage().

  reqHandlers.set(requestId, {
    onEvent: (msg) => {
      if (msg.type === 'route_decision') {
        routedTo = msg.servedModel || null;
        routedChannel = msg.channelId || null;
        routeFallback = !!msg.fallback;
        routeFallbackReason = msg.fallbackReason || null;
        patchRequest(requestId, {
          status: 'routed',
          routedAt: Date.now(),
          channelId: routedChannel,
          servedModel: routedTo,
          fallback: routeFallback,
          fallbackReason: routeFallbackReason,
        });
        const extra = {};
        if (routedTo) extra['x-ratlc-routed-to'] = routedTo;
        if (routedChannel) extra['x-ratlc-channel'] = routedChannel;
        extra['x-ratlc-fallback'] = routeFallback ? '1' : '0';
        if (routeFallback && routeFallbackReason) extra['x-ratlc-fallback-reason'] = routeFallbackReason;
        writeHeadersOnce(extra);
        if (!toolUseEmitted && !done && blockIdx === -1) {
          startMsg();
          armNoVisibleEventTimer();
        }
        log(`  route_decision req=${requestId} → ${routedChannel} group=${routedTo}${routeFallback ? ` (FALLBACK ${msg.requestedModel} → ${routedTo} reason=${routeFallbackReason})` : ''}`);
        return;
      }
      if (msg.type === 'text_delta') {
        if (done) return;
        markVisibleUpstreamEvent();
        if (!reqLog.firstByteAt) {
          reqLog.firstByteAt = Date.now();
          reqLog.firstByteMs = reqLog.firstByteAt - reqLog.startedAt;
          reqLog.status = 'streaming';
        }
        completeOpenServerTools('Cursor backend WebSearch result was consumed by the model; result metadata was not exposed on this transport.');
        emittedTextForDetection += msg.text || '';
        if ((msg.text || '').length) realVisibleEmitted = true;
        const forwardedText = hallucinationFilter.feed(msg.text || '');
        if (forwardedText) emitTextDelta(forwardedText);
        // Re-arm the tool_use watchdog on any model-originated stream
        // activity. The watchdog measures "model has gone silent" — text
        // and thinking deltas between tool_uses within the same step are
        // normal model output and should keep the timer alive. Without
        // this re-arm, a single text_delta or long-thinking gap >
        // WATCHDOG_MS would fire the watchdog mid-step, finalize the
        // turn, and orphan any subsequent tool_use the model emits (model
        // waits forever for results that claude-code never gets — channel
        // sits busy until the 240s busy-watchdog reaps it). See
        // WATCHDOG_REARM_REVIEW.md Issue 1.
        if (toolUseEmitted) armToolUseFinalizer();
      } else if (msg.type === 'thinking_delta') {
        if (done) return;
        // C: thinking is liveness, not a watchdog-disarming "visible event".
        // markThinkingLiveness retires the first-byte ceiling but keeps + resets
        // the liveness gap, so a post-thinking silence still trips the watchdog.
        markThinkingLiveness();
        if (!reqLog.firstByteAt) {
          reqLog.firstByteAt = Date.now();
          reqLog.firstByteMs = reqLog.firstByteAt - reqLog.startedAt;
          reqLog.status = 'thinking';
        }
        // Always capture for hallucination detection and (opt-in) re-injection
        // into the NEXT turn's prompt. Forwarding the reasoning to the client as
        // a proxy-local thinking block is separately gated on
        // POOL_PROXY_THINKING_BLOCKS + the client having asked for thinking.
        emittedThinkingForDetection += msg.text || '';
        if (POOL_REINJECT_THINKING) thinkingBuffer.append(convKey, msg.text || '');
        if (POOL_PROXY_THINKING_BLOCKS && clientThinkingEnabled && msg.text) {
          emitThinkingDelta(msg.text);
          // A2/C: the client now sees real reasoning. Mark visible content so
          // (a) the empty-turn retry can't fire and duplicate the thinking, and
          // (b) a post-thinking silent-timeout surfaces a clean error instead of
          // a transparent replay that would re-stream it. Guard on
          // forwardedThinkingBytes so a fully-capped turn doesn't count.
          if (forwardedThinkingBytes > 0) realVisibleEmitted = true;
        }
        if (toolUseEmitted) armToolUseFinalizer();
      } else if (msg.type === 'thinking_completed') {
        if (done) return;
        // The thinking→answer boundary is exactly where the "went silent before
        // the answer" hang occurs. Reset the gap here so the answer gets a fresh
        // grace window; if it never comes, the gap fires (Workstream C).
        markThinkingLiveness();
        thinkingCompletedCount++;
        const dur = Number.isFinite(msg.durationMs) ? msg.durationMs : Number(msg.durationMs);
        if (Number.isFinite(dur)) thinkingDurationMs = dur;
        patchRequest(requestId, {
          thinkingCompletedCount,
          thinkingDurationMs,
        });
        if (POOL_PROXY_THINKING_BLOCKS && thinkingBlockOpen) stopThinkingBlock();
        if (toolUseEmitted) armToolUseFinalizer();
      } else if (msg.type === 'server_tool_use') {
        if (done) return;
        markVisibleUpstreamEvent();
        if (!reqLog.firstByteAt) {
          reqLog.firstByteAt = Date.now();
          reqLog.firstByteMs = reqLog.firstByteAt - reqLog.startedAt;
        }
        reqLog.status = 'server_tool';
        realVisibleEmitted = true;
        emitServerToolUseEvent(msg);
      } else if (msg.type === 'progress') {
        if (done) return;
        // Liveness breadcrumb: the channel is alive at the wire. Reset ONLY
        // the liveness-gap timer (Option B) — leave the absolute ceiling
        // running so a channel that emits frames forever without ever
        // producing a visible event still gives up eventually. No-op when
        // liveness gating is disabled (grace<=0) → pure ceiling behavior.
        if (noVisibleEventTimer && !visibleUpstreamEventSeen) armLivenessGapTimer();
        patchRequest(requestId, {
          status: 'upstream_progress',
          upstreamProgressKind: msg.kind || null,
          upstreamProgressAt: Date.now(),
          upstreamProgressAttempt: msg.attempt || null,
        });
      } else if (msg.type === 'tool_use') {
        if (done) {
          // The current SSE response is already closed. Do not synthesize a
          // tool_result here: pool-manager owns the complete pending tool set
          // and will hold partial client results, then re-emit any missing
          // tool_use on the next continuation request.
          log(`⚠ ignoring tool_use after SSE finalize: name=${msg.name} anthropic_id=${msg.anthropic_id} reqId=${requestId} late=${msg.late ? 1 : 0}`);
          return;
        }

        if (POOL_TOOL_MODE === 'translate' && msg.name === 'Write') {
          const fp = agentToolsStore.getPathFromArgs(msg.args || {});
          if (agentToolsStore.isAgentToolsArtifactPath(fp)) {
            markVisibleUpstreamEvent();
            const content = agentToolsStore.contentForAgentToolsWrite(
              agentToolsStore.getContentFromArgs(msg.args || {}),
            );
            const entry = agentToolsStore.putAgentToolsArtifact(fp, content, { source: 'ratlc-write' });
            log(`→ virtual agent-tools Write captured: path=${fp} bytes=${entry?.bytes || 0} id=${msg.anthropic_id}`);
            poolWrite({
              type: 'request',
              requestId,
              action: 'send_tool_results',
              model: routingModel || null,
              requestedModel: model || null,
              results: [{
                anthropic_tool_use_id: msg.anthropic_id,
                content: agentToolsStore.makeVirtualWriteResultText(entry),
              }],
            });
            return;
          }
        }

        if (POOL_TOOL_MODE === 'translate' && msg.name === 'Read') {
          const fp = agentToolsStore.getPathFromArgs(msg.args || {});
          if (agentToolsStore.isAgentToolsArtifactPath(fp)) {
            markVisibleUpstreamEvent();
            const entry = agentToolsStore.getAgentToolsArtifact(fp);
            const content = entry ? entry.content : agentToolsStore.makeVirtualReadMissingText(fp);
            log(`→ virtual agent-tools Read served: path=${fp} bytes=${Buffer.byteLength(content)} found=${entry ? 1 : 0} id=${msg.anthropic_id}`);
            poolWrite({
              type: 'request',
              requestId,
              action: 'send_tool_results',
              model: routingModel || null,
              requestedModel: model || null,
              results: [{
                anthropic_tool_use_id: msg.anthropic_id,
                content,
              }],
            });
            return;
          }
        }
        // Parallel-tool-calls fix: emit the tool_use block but DO NOT finish
        // the message here. The model may emit several tool_uses in a single
        // assistant turn — each must get its own content_block_start with a
        // distinct index. We only finish the response when:
        //   (a) the pool reports `step_completed` (the model has finished
        //       emitting this step's tool_uses and is now waiting on results) —
        //       PRIMARY signal, finalize immediately, OR
        //   (b) the watchdog (default 1 s) fires — empirically primary
        //       on *-thinking-fast variants that don't emit
        //       step_completed at all. Re-armed on every text_delta /
        //       thinking_delta too, so the timer measures "model went
        //       silent" rather than "no more tool_uses". Firing without
        //       a step_completed is normal for these models; firing
        //       with subsequent tool_uses still pending would orphan
        //       them, hence the re-arm. OR
        //   (c) the pool reports `yield` (end_turn case — the model never
        //       called any tool, only text).
        //
        // In contract mode, names are passed through unchanged.
        // In translate mode, the model emitted a Cursor name (e.g. Shell);
        // we map to the Anthropic name (Bash) and adapt args. If the
        // Cursor tool has no Anthropic equivalent, we silently reject
        // back to the inner agent by sending a tool_error result via
        // the pool socket — the agent picks a different approach.

        // Write-spoof MITIGATION (rewrite, not reject).
        //
        // History:
        //   3c2f017: rejected the spoof with [proxy_error] tool_result
        //            → killed ch-43 (4 min HTTP stall)
        //   4984888: softened to success-shaped tool_result with hint
        //            → killed ch-58 + ch-84 (240s busy-watchdog SIGTERM)
        //   00ad168: reverted both → spoof completes silently, model
        //            confabulates downstream
        //
        // Both rejection attempts failed because the model's downstream
        // plan is "I wrote a file, now I'll read it and quote." Any
        // synthetic tool_result from the proxy breaks that plan and
        // stalls the model for the rest of its thinking budget.
        //
        // The mitigation: don't reject. INSTEAD rewrite the `content`
        // arg in-flight so claude-code writes a useful proxy notice into
        // the file. The Write goes through normally, claude-code returns
        // a real (non-synthetic) success result, the model stays alive.
        // When the model later Reads the file (its trained next step),
        // it gets our notice in the read content — context-injected
        // guidance that may redirect the model toward a real fetch.
        //
        // Failure modes & their fates:
        //   - Model reads notice → calls Cursor-native WebSearch  ✅ ideal
        //   - Model reads notice → quotes it verbatim to user   ⚠ ugly
        //     but at least it's not a confidently-wrong fake
        //   - Model ignores notice → narrates around it         ⚠ same
        //     as pre-rejection baseline; we did no harm
        //   - Model hangs                                        N/A —
        //     this is impossible because the tool_use & tool_result
        //     come from claude-code in the normal shape; we never
        //     synthesize one from the proxy.
        //
        // See AGENT_TOOLS_SPOOF_OBSERVATION.md for the spoof pattern
        // diagnosis and DEVLOG.md for the iteration history.
        //
        // NOTE (2026-05-23): in POOL_TOOL_MODE=translate (the default config)
        // this Write-spoof block is DEAD CODE for agent-tools/<uuid>.txt
        // paths — the virtual-store handler above (line ~1404) intercepts
        // those Writes and returns before execution reaches here. This block
        // remains as a fallback for `contract` mode operators who don't get
        // the virtual store. The Bing-RSS injection here was the primary
        // protection before we ported the colleague's webFetch + virtual
        // store; with both of those in place, this is belt-and-suspenders.
        if (msg.name === 'Write') {
          const fp = msg.args?.file_path || '';
          const content = msg.args?.content || '';
          const uuidV4Path = /^agent-tools\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.txt$/i;
          // Gap-3 broadening (WEB_RESEARCH_GAPS.md): trigger the mitigation
          // on ANY write to agent-tools/<uuid>.txt regardless of content —
          // empty placeholder OR pre-filled hallucinated content. Kept here
          // for contract mode; translate mode gets the cleaner virtual store
          // path that fires earlier.
          if (uuidV4Path.test(fp)) {
            const isPreFilled = String(content).trim().length > 0
              && String(content).trim() !== '(No content)';
            const noticeBody =
              '[proxy_notice — read this carefully]\n\n' +
              'This file was created by a CLIENT-SIDE Write tool call, not by a real ' +
              'web fetch. The path `agent-tools/<uuid>.txt` is the convention Cursor\'s ' +
              'backend uses to write WebSearch results to disk on its OWN filesystem — ' +
              'when you (the model) emit Write to this path, you are NOT triggering a ' +
              'web fetch, you are just creating an empty file locally and getting a ' +
              'success response.\n\n' +
              'This is a known confabulation pattern. If you proceed to narrate web ' +
              'content as if you had fetched it, you will be fabricating facts.\n\n' +
              'WHAT TO DO INSTEAD:\n' +
              '  - To search or look up public web information: use Cursor-native WebSearch.\n' +
              '  - For a user-explicit URL fetch, use WebFetch/Fetch when available; do not use Bash/curl unless the user specifically asks for a shell command.\n' +
              '  - Do NOT use WebFetch/Fetch as a broad-search substitute for Cursor-native WebSearch.\n' +
              '  - If you cannot fulfill the user request without web access, tell the ' +
              'user that and call `bajie_yield`.\n\n' +
              'DO NOT quote this proxy_notice as if it were search results. DO NOT ' +
              'fabricate web content.';
            log(`⚠ Write-spoof intercept: rewriting ${isPreFilled ? 'PRE-FILLED' : 'empty'} Write→${fp} ${isPreFilled ? `(${String(content).length}B hallucinated content)` : ''}with proxy_notice (${noticeBody.length}B) requestId=${requestId}`);
            // Mutate the args in place so the normal forwarding path
            // below picks up the new content. The shape stays identical
            // to a regular Write — file_path unchanged, content now
            // non-empty. claude-code writes the notice to the file and
            // returns its standard success result.
            msg.args = { ...msg.args, content: noticeBody };
            // ALSO kick off a real Bing RSS search in parallel and
            // record the promise keyed by the tool_use_id. When
            // claude-code POSTs the tool_result back (~10ms later for
            // a local Write), the send_tool_results path will await
            // the search briefly and REPLACE the "File created" ack
            // with the real search results before forwarding to the
            // pool. The model then sees actual web data in its next
            // assistant turn. See spoofResultPlaybook above.
            //
            // Tool_use id we record under: emitToolUseBlock sends
            // msg.anthropic_id to the client, and claude-code echoes
            // it back as tool_result.tool_use_id. Cover both ids in
            // case a translation step preserves msg.id instead.
            const query = extractQuery(messages);
            log(`  ↪ spoof: launching async search query="${query.slice(0, 120).replace(/\n/g, ' ')}" toolUseId=${msg.anthropic_id}`);
            const searchPromise = (async () => {
              try {
                if (!query) throw new Error('no extractable query');
                const results = await performWebSearch(query, { timeoutMs: 4500, maxResults: 5 });
                if (resultsLookGeneric(results)) {
                  throw new Error(`results look generic (top hosts: ${results.slice(0, 3).map((r) => { try { return new URL(r.url).hostname; } catch { return '?'; } }).join(', ')})`);
                }
                const body = formatSearchResults(query, results, { fetchedAtIso: new Date().toISOString() });
                log(`  ↪ spoof: search OK, ${results.length} results (${body.length}B) toolUseId=${msg.anthropic_id}`);
                return body;
              } catch (err) {
                log(`  ↪ spoof: search failed (${err.message || err}) toolUseId=${msg.anthropic_id} — will fall back to proxy_notice in tool_result`);
                return null;
              }
            })();
            recordSpoofResult(msg.anthropic_id, searchPromise);
            if (msg.id && msg.id !== msg.anthropic_id) recordSpoofResult(msg.id, searchPromise);
            // Intentionally fall through to the standard translate /
            // contract forwarding logic — DO NOT return early.
          }
        }

        if (POOL_TOOL_MODE === 'translate' && !isInternalTool(msg.name)) {
          const xlated = cursorToAnthropic(msg.name, msg.args || {});
          if (!xlated.ok) {
            // Rejection — feed the error back through the pool to the inner
            // agent. The api-server's request stream stays open; the inner
            // agent will keep generating after seeing this tool_result.
            // Use the batch shape with a single entry so the pool path
            // remains consistent (manager + worker only know the new
            // `send_tool_results` action after the parallel-tools fix).
            poolWrite({
              type: 'request',
              requestId,
              action: 'send_tool_results',
              model: model || null,
              results: [{
                anthropic_tool_use_id: msg.anthropic_id,
                content: `[proxy_error] ${xlated.error}`,
              }],
            });
            // Don't emit anything to the client — pretend the tool_use
            // never happened from claude-code's POV.
            return;
          }
          if (isPoolLocalToolName(xlated.name)) {
            const localDecision = getPoolLocalToolDecision(xlated.name, xlated.input || {});
            if (!localDecision.canRun && localDecision.retryOnClient) {
              log(`→ local tool adapter skip: name=${xlated.name} reason=${localDecision.reason || 'not runnable locally'}; forwarding to client id=${msg.anthropic_id}`);
            } else {
              // Dedup: suppress duplicate invocations for the same tool_use_id.
              // The bridge-worker can deliver the same tool_use IPC twice
              // (Cursor backend re-emits unhandled tool_use after our watchdog
              // finalizes the outer turn). Without this check, we'd run the
              // local tool twice, send two tool_results, and the second would
              // trip the pool's `already consumed anthropic_tool_use_id` guard.
              const tid = msg.anthropic_id;
              if (tid && localToolInFlight.has(tid)) {
                log(`  ↪ local tool adapter dedup: in-flight id=${tid} name=${xlated.name} — suppressing duplicate`);
                return;
              }
              if (tid && localToolCompleted.has(tid)) {
                const agoMs = Date.now() - localToolCompleted.get(tid);
                log(`  ↪ local tool adapter dedup: already completed id=${tid} name=${xlated.name} ${agoMs}ms ago — suppressing duplicate`);
                return;
              }
              if (tid) localToolInFlight.set(tid, { startedAt: Date.now(), name: xlated.name });
              log(`→ local tool adapter: name=${xlated.name} args=${JSON.stringify(xlated.input).slice(0, 200)} id=${msg.anthropic_id}`);
              reqLog.status = 'local_tool';
              reqLog.lastToolName = xlated.name;
              const markCompleted = () => {
                if (tid) {
                  localToolInFlight.delete(tid);
                  localToolCompleted.set(tid, Date.now());
                }
              };
              runPoolLocalTool(xlated.name, xlated.input || {})
                .then((result) => {
                  markCompleted();
                  if (!result.ok) {
                    log(`  ↪ local tool adapter error: name=${result.name || xlated.name} id=${msg.anthropic_id} ${String(result.content || '').slice(0, 200)}`);
                  } else {
                    log(`  ↪ local tool adapter ok: name=${result.name || xlated.name} id=${msg.anthropic_id} bytes=${Buffer.byteLength(String(result.content || ''))}`);
                  }
                  poolWrite({
                    type: 'request',
                    requestId,
                    action: 'send_tool_results',
                    model: routingModel || null,
                    requestedModel: model || null,
                    results: [{
                      anthropic_tool_use_id: msg.anthropic_id,
                      content: result.content || '',
                    }],
                  });
                })
                .catch((e) => {
                  markCompleted();
                  const message = e && e.message ? e.message : String(e);
                  log(`  ↪ local tool adapter threw: name=${xlated.name} id=${msg.anthropic_id} ${message}`);
                  poolWrite({
                    type: 'request',
                    requestId,
                    action: 'send_tool_results',
                    model: routingModel || null,
                    requestedModel: model || null,
                    results: [{
                      anthropic_tool_use_id: msg.anthropic_id,
                      content: `[proxy_error] ${message}`,
                    }],
                  });
                });
              return;
            }
          }
          const clientBridge = resolveClientBridgeToolUse(tools, xlated.name, xlated.input || {});
          if (clientBridge.handled && !clientBridge.ok) {
            log(`→ client MCP bridge reject: name=${xlated.name} requested=${clientBridge.clientToolName || '(none)'} id=${msg.anthropic_id} error=${clientBridge.error}`);
            reqLog.status = 'client_tool_bridge_rejected';
            reqLog.lastToolName = clientBridge.clientToolName || xlated.name;
            poolWrite({
              type: 'request',
              requestId,
              action: 'send_tool_results',
              model: routingModel || null,
              requestedModel: model || null,
              results: [{
                anthropic_tool_use_id: msg.anthropic_id,
                content: `[proxy_error] ${clientBridge.error}`,
              }],
            });
            return;
          }
          if (clientBridge.ok) {
            const clientToolUseId = makeClientBridgeToolUseId(msg.anthropic_id);
            rememberClientToolUse({
              clientToolUseId,
              poolToolUseId: msg.anthropic_id,
              requestId,
              toolName: clientBridge.clientToolName,
              input: clientBridge.clientInput || {},
            });
            log(`→ client MCP bridge tool_use: name=${clientBridge.clientToolName} via=${xlated.name} poolId=${msg.anthropic_id} clientId=${clientToolUseId} args=${JSON.stringify(clientBridge.clientInput || {}).slice(0, 200)}`);
            reqLog.status = 'client_tool_bridge';
            reqLog.lastToolName = clientBridge.clientToolName;
            emitToolUseBlock(clientToolUseId, clientBridge.clientToolName, clientBridge.clientInput || {});
            stopReason = 'tool_use';
            armToolUseFinalizer();
            return;
          }
          log(`→ tool_use to client (translated): name=${xlated.name} args=${JSON.stringify(xlated.input).slice(0, 200)}`);
          reqLog.status = 'tool_use';
          reqLog.lastToolName = xlated.name;
          emitToolUseBlock(msg.anthropic_id, xlated.name, xlated.input);
        } else {
          log(`→ tool_use to client: name=${msg.name} args=${JSON.stringify(msg.args).slice(0, 200)}`);
          reqLog.status = 'tool_use';
          reqLog.lastToolName = msg.name;
          emitToolUseBlock(msg.anthropic_id, msg.name, msg.args);
        }
        // Mark that we should end with stop_reason='tool_use' when the
        // turn finalizes. Arm the watchdog after every tool_use (each new
        // one resets the timer — more may still arrive). The watchdog
        // only fires if step_completed never comes; on the common path
        // step_completed fires first and cancels it.
        stopReason = 'tool_use';
        armToolUseFinalizer();
      } else if (msg.type === 'step_completed') {
        markVisibleUpstreamEvent();
        // The pool's bridge-worker observed `interactionUpdate.stepCompleted`
        // from cursor-agent. If any tool_uses have been emitted on this
        // turn, the model is now paused waiting for the tool_result(s).
        // This is the DETERMINISTIC, PRIMARY finalize signal — the
        // watchdog above is only a fallback for the case this never fires.
        if (toolUseEmitted && !done) {
          log(`  → finalize tool_use turn (step_completed) requestId=${requestId}`);
          stopReason = 'tool_use';
          finishMessage();  // disarms the watchdog centrally
        }
      } else if (msg.type === 'yield') {
        markVisibleUpstreamEvent();
        completeOpenServerTools('Cursor backend WebSearch completed before the model yielded; result metadata was not exposed on this transport.');
        // The model called bajie_yield. If any tool_uses were emitted this
        // turn (rare — usually finalize happens earlier via step_completed
        // or the watchdog), stop_reason='tool_use'. Otherwise the model
        // sent pure-text and then yielded — that's stop_reason='end_turn'.
        stopReason = toolUseEmitted ? 'tool_use' : 'end_turn';
        finishMessage();
      } else if (msg.type === 'error') {
        // Transparent recovery for a transient upstream error that arrived
        // BEFORE any client-visible content (e.g. Cursor "Response error:
        // aborted" or a stale sticky channel dying during a throttle storm).
        // Left unhandled, this dead-ends as "empty or malformed response
        // (HTTP 200)" (clean-error path) or a manual-retry notice (softened
        // path); instead we replay on a fresh channel — same mechanism as the
        // silent-timeout retry. Guards: not already retrying (a stale post-
        // cancel error must not double-fire), before-content (never duplicate
        // shown output — an abort AFTER partial content falls through to the
        // preserve-shown-output path below), and transient-only.
        if (!pendingRetryTimer && !textBlockOpen && !toolUseEmitted && outputTokens === 0
            && isRetryableUpstreamAbort(msg.message, msg.code)
            && tryRetryRequest('upstream_abort')) {
          return;
        }
        markVisibleUpstreamEvent();
        completeOpenServerTools('The response ended with an error before Cursor exposed WebSearch result metadata.');
        const softened = classifySoftenedPoolError(msg.message, msg.code);
        if (softened) {
          log(`  → soften pool error as text requestId=${requestId}: ${String(msg.message || '').slice(0, 180)}`);
          finalStatusOverride = softened.status;
          finalErrorMessage = softened.error || msg.message || null;
          // Recover stale tool_result errors (already consumed / unknown id /
          // span-multiple-channels) by replaying the pre-built full-context
          // send_user_message on a fresh channel, instead of dead-ending with
          // a notice the client can't act on (it just re-sends the same
          // consumed tool_result). startMsg() first so the retry breadcrumb
          // and the replayed turn stream into a well-formed message (there was
          // no route_decision for a pool-rejected duplicate, so message_start
          // hasn't been sent yet). tryRetryRequest re-arms the watchdog itself.
          if (softened.status === 'stale_tool_result' && STALE_TOOL_RESULT_RETRY_MAX > 0) {
            startMsg();
            if (tryRetryRequest('stale_tool_result')) return;
          }
          startMsg();
          emitTextDelta(softened.text);
          stopReason = 'end_turn';
          finishMessage();
          return;
        }
        // If we've ALREADY streamed client-visible content (text or tool_use),
        // a bare `error` event orphans the partial message — no
        // content_block_stop / message_stop — and claude-code DISCARDS the
        // answer the user already saw as "empty or malformed response (HTTP
        // 200)". This is the disappearing-answer bug seen during throttle
        // storms (Cursor aborts the bidi stream mid-response → pool forwards a
        // `type:error` → here). Preserve what was shown by finalizing the
        // message cleanly instead of emitting a raw error event. For a text
        // turn, append a brief truncation notice; for a tool_use turn, leave
        // the emitted tool_use blocks intact so the client can still act on
        // them. finishMessage() closes the open blocks and emits
        // message_delta + message_stop, so the SSE stays well-formed.
        if (textBlockOpen || toolUseEmitted || outputTokens > 0) {
          log(`  → upstream error AFTER partial content requestId=${requestId}; finalizing gracefully to preserve shown output: ${String(msg.message || '').slice(0, 140)}`);
          if (toolUseEmitted) {
            // Tool_use turn: the emitted tool_use blocks are intact, so the
            // client continues automatically by executing them. No notice.
            finalStatusOverride = 'error_after_partial_content';
            finalErrorMessage = msg.message || null;
            stopReason = 'tool_use';
          } else if (tryContinueAfterAbort()) {
            // Text turn cut off mid-response: self-drive a continuation into the
            // same open message instead of dead-ending. Message stays open; the
            // continuation streams in and finishMessage runs on its yield.
            return;
          } else {
            finalStatusOverride = 'error_after_partial_content';
            finalErrorMessage = msg.message || null;
            emitTextDelta('\n\n[proxy_notice] Upstream connection dropped mid-response — the answer above may be incomplete. Send a new message to continue.\n');
            stopReason = 'end_turn';
          }
          finishMessage();
          return;
        }
        // No client-visible content yet — emit a clean `error` event and close.
        // Anthropic's real SSE for errors emits ONLY `event: error` (no
        // message_delta/message_stop afterwards); claude-code treats an error
        // event followed by message_delta/message_stop as malformed. So here,
        // with nothing streamed to lose, we emit the error event, close the
        // stream, and skip finishMessage.
        writeHeadersOnce({ 'x-ratlc-fallback': '0' });
        // An upstream rate-limit (per-model Cursor quota) is NOT a generic
        // api_error — tag it as rate_limit_error so the client backs off, and
        // remember the model so a follow-up empty turn on it reads as rate-limit.
        const rateLimited = isUpstreamRateLimit(msg.message);
        const rlModel = routedTo || routingModel || 'this model';
        if (rateLimited) noteUpstreamRateLimit(routedTo || routingModel);
        finishRequestLog(requestId, {
          status: rateLimited ? 'upstream_rate_limit' : 'error',
          error: msg.message,
          stopReason: 'error',
          outputTokens,
        });
        sseWrite(res, 'error', rateLimited
          ? { type: 'error', error: { type: 'rate_limit_error', message: `Upstream Cursor rate limit for model ${rlModel}: ${String(msg.message || '').replace(/^Connect error\s*/i, '')}. This is a per-model quota on the Cursor account(s) behind the proxy, not a proxy error — wait and retry, or switch to a model that has quota (e.g. your opus/4.6 groups).` } }
          : { type: 'error', error: { type: 'api_error', message: msg.message } });
        done = true;
        disarmToolUseFinalizer();
        stopThinkingBlock();
        if (POOL_REINJECT_THINKING) thinkingBuffer.commitTurn(convKey);
        try { res.end(); } catch { /* ignore */ }
        reqHandlers.delete(requestId);
      }
    },
  });

  // Send to pool
  if (toolResults.length > 0) {
    if (replayedClientBridgeToolResults.length > 0 && clientBridgeToolResults.length === 0 && regularToolResults.length === 0) {
      log(`  → ignore replayed client-tool bridge results requestId=${requestId} count=${replayedClientBridgeToolResults.length} ids=[${replayedClientBridgeToolResults.map(r => r.result.tool_use_id).join(', ')}]`);
      patchRequest(requestId, {
        status: 'ignored_replayed_client_tool_bridge_result',
        forwardedAt: Date.now(),
        toolResultCount: replayedClientBridgeToolResults.length,
        toolResultIds: replayedClientBridgeToolResults.map(r => r.result.tool_use_id),
      });
      startMsg();
      emitTextDelta('[proxy_notice] Duplicate client MCP tool_result replay ignored; the result was already forwarded to the Cursor pool. Continue with a fresh user message if needed.\n');
      stopReason = 'end_turn';
      finishMessage();
      return;
    }

    if (replayedClientBridgeToolResults.length > 0) {
      log(`  → reject mixed replayed client-bridge/other tool_result batch requestId=${requestId} replayed=${replayedClientBridgeToolResults.length} bridged=${clientBridgeToolResults.length} regular=${regularToolResults.length}`);
      finalStatusOverride = 'error';
      finalErrorMessage = 'mixed replayed client-bridge and other tool_result batch';
      startMsg();
      emitTextDelta('[proxy_error] Replayed client MCP tool results were mixed with new tool results. Retry with a fresh user message so the proxy can rebuild from the current conversation history.\n');
      stopReason = 'end_turn';
      finishMessage();
      return;
    }

    if (clientBridgeToolResults.length > 0 && regularToolResults.length === 0) {
      const bridged = clientBridgeToolResults.map(({ result, entry }) => ({
        anthropic_tool_use_id: entry.poolToolUseId,
        content: buildPoolToolResultContentFromClientResult(result),
      }));
      log(`  → pool client-tool bridge results requestId=${requestId} count=${bridged.length} ids=[${bridged.map(r => r.anthropic_tool_use_id).join(', ')}]`);
      patchRequest(requestId, {
        status: 'forwarded_client_tool_bridge_result',
        forwardedAt: Date.now(),
        toolResultCount: bridged.length,
        toolResultIds: bridged.map(r => r.anthropic_tool_use_id),
      });
      poolWrite({
        type: 'request', requestId, action: 'send_tool_results',
        model: routingModel || null,
        requestedModel: model || null,
        results: bridged,
      });
      return;
    }

    if (clientBridgeToolResults.length > 0 && regularToolResults.length > 0) {
      // Mixed batch: the assistant turn emitted PARALLEL tool calls spanning
      // BOTH a client-bridge MCP tool (executed by claude-code — e.g. a
      // playwright browser tool) AND a regular pool tool, so claude-code
      // returns all of their results in one user turn. Both kinds were emitted
      // by the SAME assistant turn → the SAME pool channel, and both serialize
      // to identical `send_tool_results` entries ({anthropic_tool_use_id,
      // content}), so we MERGE them into a single batch — which is exactly what
      // the channel is waiting for. (Rejecting instead dead-ended the client
      // AND stranded the channel holding a partial tool_result batch until the
      // busy-watchdog reaped it.)
      //
      // Exception: a SYNTHETIC regular result (a textual "[Tool call]" marker
      // with no real pending execId) needs the full-context rebuild path
      // (send_user_message), which can't be merged with real bridged execIds —
      // so that rare combination is still rejected.
      const syntheticRegular = regularToolResults.filter((r) => isSyntheticToolUseId(r.tool_use_id));
      if (syntheticRegular.length > 0) {
        log(`  → reject mixed client-bridge/synthetic tool_result batch requestId=${requestId} bridged=${clientBridgeToolResults.length} synthetic=${syntheticRegular.length} regular=${regularToolResults.length}`);
        finalStatusOverride = 'error';
        finalErrorMessage = 'mixed client-bridge and synthetic tool_result batch';
        startMsg();
        emitTextDelta('[proxy_error] Mixed client-bridge and synthetic tool results arrived in one batch. Retry the last request so the proxy can rebuild the turn from full message history.\n');
        stopReason = 'end_turn';
        finishMessage();
        return;
      }
      const bridgedResults = clientBridgeToolResults.map(({ result, entry }) => ({
        anthropic_tool_use_id: entry.poolToolUseId,
        content: buildPoolToolResultContentFromClientResult(result),
      }));
      // Same enrichment as the regular send_tool_results path (spoof-result
      // injection + error/normalization), so a regular result inside a mixed
      // batch is handled identically to one in a homogeneous batch.
      const enrichedRegular = await Promise.all(regularToolResults.map(async (r) => {
        const injected = await consumeSpoofResult(r.tool_use_id);
        if (injected) {
          log(`  ↪ spoof-result injection: tool_use_id=${r.tool_use_id} replacing ${r.text ? r.text.length + 'B ack' : 'empty ack'} with ${injected.length}B search payload`);
          return { anthropic_tool_use_id: r.tool_use_id, content: injected };
        }
        if (r.isError) return { anthropic_tool_use_id: r.tool_use_id, content: { error: r.text || 'Tool failed' } };
        return { anthropic_tool_use_id: r.tool_use_id, content: normalizeAnthropicContentForCursorMcp(r.content === undefined ? r.text : r.content) };
      }));
      const merged = [...bridgedResults, ...enrichedRegular];
      // Retry snapshot (parity with the regular path): a full-context
      // send_user_message fallback in case this batch silent-times-out.
      try {
        const retryThinkingTurns = POOL_REINJECT_THINKING ? thinkingBuffer.getForConvKey(convKey) : [];
        const retryContent = buildFullContextCursorMcpContent({ messages, system, tools, thinkingTurns: retryThinkingTurns });
        lastPoolRetryPayload = {
          type: 'request', requestId, action: 'send_user_message',
          model: routingModel || null,
          requestedModel: model || null,
          text: cursorMcpContentToText(retryContent),
          content: retryContent,
          system: extractSystemPrompt(system),
          tools: tools || [],
          sessionKey: null,
          contextMode: 'full',
          hybridReason: 'retry-fallback-from-mixed-tool-results',
        };
      } catch (e) {
        log(`  ↪ retry fallback build failed (mixed batch): ${e.message || e}`);
        lastPoolRetryPayload = null;
      }
      log(`  → pool merged client-bridge+regular tool_results requestId=${requestId} bridged=${bridgedResults.length} regular=${enrichedRegular.length} ids=[${merged.map(r => r.anthropic_tool_use_id).join(', ')}]`);
      patchRequest(requestId, {
        status: 'forwarded_mixed_tool_result',
        forwardedAt: Date.now(),
        toolResultCount: merged.length,
        toolResultIds: merged.map((r) => r.anthropic_tool_use_id),
      });
      poolWrite({
        type: 'request', requestId, action: 'send_tool_results',
        model: routingModel || null,
        requestedModel: model || null,
        results: merged,
      });
      return;
    }

    const syntheticToolResults = regularToolResults.filter((r) => isSyntheticToolUseId(r.tool_use_id));
    if (syntheticToolResults.length > 0 && syntheticToolResults.length !== regularToolResults.length) {
      log(`  → reject mixed synthetic/real tool_result batch requestId=${requestId} synthetic=${syntheticToolResults.length} total=${regularToolResults.length}`);
      finalStatusOverride = 'error';
      finalErrorMessage = 'mixed synthetic and real tool_result batch';
      startMsg();
      emitTextDelta('[proxy_error] Mixed synthetic and real tool results arrived in one batch. Retry the last request so the proxy can rebuild the turn from full message history.');
      stopReason = 'end_turn';
      finishMessage();
      return;
    }

    if (syntheticToolResults.length === regularToolResults.length) {
      // These tool_use blocks were synthesized from textual `[Tool call ...]`
      // markers, so Cursor is not paused on a real pending execId. Claude Code
      // will still execute the client-side tool and POST a tool_result. Feeding
      // that ID through pool-manager would fail `unknown anthropic_tool_use_id`;
      // instead rebuild a fresh request from the full messages[] history so the
      // inner model sees the synthetic tool_use + real tool_result as context.
      const thinkingTurns = POOL_REINJECT_THINKING ? thinkingBuffer.getForConvKey(convKey) : [];
      const content = buildFullContextCursorMcpContent({ messages, system, tools, thinkingTurns });
      const imageCount = cursorMcpContentImageCount(content);
      const poolAction = 'send_user_message';
      const text = cursorMcpContentToText(content);
      const contentBytes = cursorMcpContentPayloadBytes(content);
      const sessionKey = makeSessionKey({ clientSessionId, convKey, routingModel });
      log(`  → pool synthetic tool_result rebuild ${poolAction} requestId=${requestId} count=${regularToolResults.length} model=${model || '(default)'} routeModel=${routingModel || '(default)'} sessionKey=${sessionKey || '(none)'} textBytes=${text.length} contentBytes=${contentBytes} images=${imageCount} msgCount=${messages.length} reinjectTurns=${thinkingTurns.length}`);
      patchRequest(requestId, {
        status: 'forwarded_synthetic_tool_result_rebuild',
        forwardedAt: Date.now(),
        contextMode: 'synthetic-rebuild',
        effectiveContextMode: 'full',
        sessionKey: sessionKey || null,
        textBytes: text.length,
        contentBytes,
        imageCount,
        reinjectTurns: thinkingTurns.length,
        toolResultIds: regularToolResults.map((r) => r.tool_use_id),
      });
      poolWrite({
        type: 'request', requestId, action: poolAction,
        model: routingModel || null,
        requestedModel: model || null,
        text,
        content,
        system: extractSystemPrompt(system),
        tools: tools || [],
        sessionKey: sessionKey || null,
        contextMode: 'full',
        hybridReason: 'synthetic-tool-result-rebuild',
      });
      return;
    }

    // Batch send: pool-manager + bridge-worker both understand
    // `send_tool_results` (plural) with an array of entries. All N entries
    // must resolve to the same channel — the manager defensively checks
    // this and errors out if not (which shouldn't happen by construction,
    // since they were all emitted by one channel in one assistant turn).
    //
    // Spoof-mitigation injection: for any tool_use_id we recorded in the
    // playbook (because the model emitted the Write-spoof pattern and
    // we kicked off an async Bing search), await the search briefly and
    // REPLACE the tool_result content with the real search payload.
    // claude-code wrote a real file with proxy_notice but the model
    // doesn't necessarily read the file back — putting results inline
    // in the tool_result is the reliable channel.
    const enriched = await Promise.all(regularToolResults.map(async (r) => {
      const injected = await consumeSpoofResult(r.tool_use_id);
      if (injected) {
        log(`  ↪ spoof-result injection: tool_use_id=${r.tool_use_id} replacing ${r.text ? r.text.length + 'B ack' : 'empty ack'} with ${injected.length}B search payload`);
        return { anthropic_tool_use_id: r.tool_use_id, content: injected };
      }
      // Either no playbook entry (normal tool_result) or search failed/
      // timed out — forward whatever claude-code's Write actually
      // returned (the proxy_notice ack).
      if (r.isError) {
        return {
          anthropic_tool_use_id: r.tool_use_id,
          content: { error: r.text || 'Tool failed' },
        };
      }
      return {
        anthropic_tool_use_id: r.tool_use_id,
        content: normalizeAnthropicContentForCursorMcp(r.content === undefined ? r.text : r.content),
      };
    }));
    log(`  → pool send_tool_results requestId=${requestId} count=${enriched.length} model=${model || '(default)'} routeModel=${routingModel || '(default)'} ids=[${enriched.map(r => r.anthropic_tool_use_id).join(', ')}]`);
    patchRequest(requestId, {
      status: 'forwarded_tool_result',
      forwardedAt: Date.now(),
      toolResultCount: enriched.length,
      toolResultIds: enriched.map(r => r.anthropic_tool_use_id),
    });
    // Retry snapshot for upstream_silent_timeout on send_tool_results:
    // tool_use_ids are channel-bound, so we CANNOT replay the same
    // send_tool_results on a fresh channel ("unknown anthropic_tool_use_id").
    // Instead we pre-render a send_user_message fallback that includes the
    // ENTIRE messages[] history (with tool_result blocks inlined via
    // mode=full). On retry, the fresh channel receives the complete
    // transcript and the model continues from there, emitting NEW
    // tool_use_ids on the new channel. The original stuck channel is
    // cancelled (killed); its stale ids are no longer referenced.
    //
    // The fallback uses sessionKey=null (force fresh routing) and
    // contextMode='full' (always full transcript — retry is recovery,
    // not steady-state, so we want the full conversation regardless of
    // hybrid optimizations).
    try {
      const retryThinkingTurns = POOL_REINJECT_THINKING ? thinkingBuffer.getForConvKey(convKey) : [];
      const retryContent = buildFullContextCursorMcpContent({ messages, system, tools, thinkingTurns: retryThinkingTurns });
      const retryText = cursorMcpContentToText(retryContent);
      lastPoolRetryPayload = {
        type: 'request', requestId, action: 'send_user_message',
        model: routingModel || null,
        requestedModel: model || null,
        text: retryText,
        content: retryContent,
        system: extractSystemPrompt(system),
        tools: tools || [],
        sessionKey: null,  // force fresh channel
        contextMode: 'full',
        hybridReason: 'retry-fallback-from-tool-results',
      };
    } catch (e) {
      log(`  ↪ retry fallback build failed: ${e.message || e}; tool_result silent timeouts on this request won't be retried`);
      lastPoolRetryPayload = null;
    }
    poolWrite({
      type: 'request', requestId, action: 'send_tool_results',
      model: routingModel || null,
      requestedModel: model || null,
      results: enriched,
    });
  } else {
    // Mode selection: in `full` mode, render the ENTIRE messages[] into
    // one self-contained prompt; in `last` mode (default, backwards-
    // compatible), forward only the last user message text. The pool
    // socket frame is identical in both — only the `text` payload changes.
    //
    // POOL_REINJECT_THINKING: in `full` mode the captured thinking turns
    // are attached to their matching assistant messages inside the
    // rendered history. In `last` mode the captured turns are prepended
    // to the user-message text as a leading sequence of `<thinking>`
    // blocks, since there's no history to attach to.
    const thinkingTurns = POOL_REINJECT_THINKING ? thinkingBuffer.getForConvKey(convKey) : [];
    const hybrid = POOL_CONTEXT_MODE === 'hybrid'
      ? decideHybridContext({ clientSessionId, convKey, routingModel, system, tools, messages })
      : null;
    let effectiveContextMode = hybrid ? hybrid.sendMode : POOL_CONTEXT_MODE;
    let contextGuardReason = null;
    let text;
    let content;
    // Bidi-stream payload guard. Cursor's live agent stream has an empirical
    // payload-size limit around ~96 KB; PR #2 added CONTEXT_MAX_BYTES to back
    // off in that case. We gate the guard on `hybrid` mode only: hybrid has
    // session affinity, so the sticky channel already holds prior turns in
    // its native memory and falling back to `last` is a legitimate
    // continuation. In pure `full` mode the caller explicitly opted into
    // complete history; honoring that contract is more important than
    // sidestepping Cursor's payload limit (silent context loss is worse than
    // a visible upstream error). See BIDI_PAYLOAD_LIMIT.md for the full
    // rationale and the planned follow-up workstream.
    const guardActive = (POOL_CONTEXT_MODE === 'hybrid') && CONTEXT_MAX_BYTES > 0;
    const lastUserContent = buildLastUserCursorMcpContent(lastMsg.content, thinkingTurns);
    if (cursorMcpContentImageCount(lastUserContent) > 0) {
      content = buildFullContextCursorMcpContent({ messages, system, tools, thinkingTurns });
      text = cursorMcpContentToText(content);
      const imageFullContentBytes = cursorMcpContentPayloadBytes(content);
      if (guardActive && imageFullContentBytes > CONTEXT_MAX_BYTES) {
        contextGuardReason = `image-full-context-too-large:${imageFullContentBytes}>${CONTEXT_MAX_BYTES}`;
        content = lastUserContent;
        text = cursorMcpContentToText(content);
        effectiveContextMode = 'last';
      } else {
        effectiveContextMode = 'full';
      }
    } else if (effectiveContextMode === 'full') {
      content = buildFullContextCursorMcpContent({ messages, system, tools, thinkingTurns });
      text = cursorMcpContentToText(content);
      if (guardActive && text.length > CONTEXT_MAX_BYTES) {
        contextGuardReason = `full-context-too-large:${text.length}>${CONTEXT_MAX_BYTES}`;
        content = lastUserContent;
        text = cursorMcpContentToText(content);
        effectiveContextMode = 'last';
      }
    } else {
      content = lastUserContent;
      text = cursorMcpContentToText(content);
    }
    const sessionKey = hybrid?.sessionKey || makeSessionKey({ clientSessionId, convKey, routingModel });
    const imageCount = cursorMcpContentImageCount(content);
    const poolAction = 'send_user_message';
    const contentBytes = cursorMcpContentPayloadBytes(content);
    log(`  → pool ${poolAction} requestId=${requestId} model=${model || '(default)'} routeModel=${routingModel || '(default)'} mode=${POOL_CONTEXT_MODE}${hybrid ? '/' + hybrid.sendMode + ' reason=' + hybrid.reason : ''}${contextGuardReason ? ' guard=' + contextGuardReason : ''} sessionKey=${sessionKey || '(none)'} textBytes=${text.length} contentBytes=${contentBytes} images=${imageCount} msgCount=${messages.length} tools=${(tools || []).length} reinjectTurns=${thinkingTurns.length}`);
    patchRequest(requestId, {
      status: 'forwarded',
      forwardedAt: Date.now(),
      contextMode: POOL_CONTEXT_MODE,
      effectiveContextMode,
      hybridReason: hybrid?.reason || null,
      contextGuardReason,
      sessionKey: sessionKey || null,
      textBytes: text.length,
      contentBytes,
      imageCount,
      reinjectTurns: thinkingTurns.length,
    });
    const poolReqPayload = {
      type: 'request', requestId, action: poolAction,
      model: routingModel || null,
      requestedModel: model || null,
      text,
      content,
      system: extractSystemPrompt(system),
      tools: tools || [],
      sessionKey: sessionKey || null,
      contextMode: effectiveContextMode,
      hybridReason: contextGuardReason || hybrid?.reason || null,
    };
    // Snapshot for potential retry on upstream_silent_timeout / empty_assistant_turn.
    // Both send_user_message AND send_tool_results are retryable as of the
    // release_consumed_ids IPC; for tool_results, tryRetryRequest emits
    // release_consumed_ids before replaying so the pool doesn't reject
    // with "already consumed".
    lastPoolRetryPayload = poolReqPayload;
    poolWrite(poolReqPayload);
  }

	  function cancelForClientDisconnect() {
	    if (done) return;
	    clientGone = true;  // suppress the now-pointless empty_assistant_turn retry
	    log(`client disconnected mid-stream for ${requestId}`);
	    patchRequest(requestId, { clientDisconnectedAt: Date.now() });
	    poolWrite({ type: 'cancel_request', requestId, reason: 'client_disconnected' });
	    if (pendingRetryTimer) { clearTimeout(pendingRetryTimer); pendingRetryTimer = null; }
	    disarmKeepalivePing();
	    finishMessage();  // disarms the watchdog centrally
	  }

	  // req.close can fire after the request body is fully read, before the SSE
	  // response has actually finished. res.close is the reliable signal for a
	  // streaming client disconnect.
	  res.on('close', () => {
	    if (!res.writableEnded) cancelForClientDisconnect();
	  });
}

function handleModels(req, res) {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    data: [
      { id: 'claude-opus-4-8-thinking-max-fast', type: 'model', display_name: 'Claude Opus 4.8 Thinking Max (Fast)', created_at: '2026-01-01T00:00:00Z' },
      { id: 'claude-4.6-opus-max-thinking-fast', type: 'model', display_name: 'Claude Opus 4.6 Max Thinking (Fast)', created_at: '2026-01-01T00:00:00Z' },
    ],
  }));
}

// GET /v1/_stats — per-model latency percentiles (first-byte + total) bucketed
// by payload size, with time-of-day regime classification and a suggested
// silent-timeout for the current regime. Backs `ratlc stats` and the TUI.
function handleStats(req, res) {
  let snap;
  // adaptiveMinGapMs: the floor the api-server actually enforces on the adaptive
  // gap. The per-bucket suggestion is floored at only 1s internally, so under
  // adaptive the UI must clamp the displayed gap to this to match enforcement (M2).
  try { snap = latencyMetrics.snapshot(); snap.adaptiveTimeouts = ADAPTIVE_TIMEOUTS; snap.adaptiveMinGapMs = ADAPTIVE_MIN_GAP_MS; }
  catch (e) {
    res.writeHead(500, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ error: String((e && e.message) || e) }));
  }
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(snap));
}

function handleMetrics(req, res) {
  // Prometheus-style text exposition. Pull from pool's status snapshot,
  // augment with api-server-local counters (TODO).
  const sock = net.createConnection(POOL_SOCK);
  let buf = '';
  const t = setTimeout(() => { try { sock.destroy(); } catch {} ; if (!res.writableEnded) { res.writeHead(503); res.end(''); } }, 5000);
  sock.on('connect', () => sock.write(JSON.stringify({ type: 'status' }) + '\n'));
  sock.on('data', (c) => {
    buf += c.toString('utf8');
    const idx = buf.indexOf('\n');
    if (idx === -1) return;
    try {
      const m = JSON.parse(buf.slice(0, idx));
      clearTimeout(t); sock.end();
      const lines = [];
      const p = m.pool || {};
      const cfg = m.config || {};
      lines.push('# HELP ratlc_pool_channels_total Channels alive in the pool.');
      lines.push('# TYPE ratlc_pool_channels_total gauge');
      lines.push(`ratlc_pool_channels_total{model="${cfg.model || ''}",mode="${cfg.toolMode || ''}"} ${p.actualSize || 0}`);
      lines.push('# HELP ratlc_pool_channels_target Target channel count.');
      lines.push('# TYPE ratlc_pool_channels_target gauge');
      lines.push(`ratlc_pool_channels_target ${p.configuredSize || 0}`);
      lines.push('# HELP ratlc_pool_channels_by_state Channels by state.');
      lines.push('# TYPE ratlc_pool_channels_by_state gauge');
      lines.push(`ratlc_pool_channels_by_state{state="ready"} ${p.readyCount || 0}`);
      lines.push(`ratlc_pool_channels_by_state{state="busy"} ${p.busyCount || 0}`);
      lines.push(`ratlc_pool_channels_by_state{state="opening"} ${p.openingCount || 0}`);
      lines.push(`ratlc_pool_channels_by_state{state="dead"} ${p.deadCount || 0}`);
      lines.push('# HELP ratlc_pool_pending_requests Requests queued awaiting a ready channel.');
      lines.push('# TYPE ratlc_pool_pending_requests gauge');
      lines.push(`ratlc_pool_pending_requests ${p.pendingRequests || 0}`);
      lines.push('# HELP ratlc_pool_tool_use_held Tool_use round-trips currently held awaiting tool_result.');
      lines.push('# TYPE ratlc_pool_tool_use_held gauge');
      lines.push(`ratlc_pool_tool_use_held ${p.toolUseIndex || 0}`);
      lines.push('# HELP ratlc_pool_tool_use_consumed Cached consumed tool_use ids kept for replay detection.');
      lines.push('# TYPE ratlc_pool_tool_use_consumed gauge');
      lines.push(`ratlc_pool_tool_use_consumed ${p.consumedToolUseIndex || 0}`);
      lines.push('# HELP ratlc_channel_rounds Successful rounds served per channel.');
      lines.push('# TYPE ratlc_channel_rounds counter');
      for (const ch of (p.channels || [])) {
        const grpLbl = ch.group ? `,group="${ch.group}"` : '';
        lines.push(`ratlc_channel_rounds{channel="${ch.id}"${grpLbl}} ${ch.roundsServed || 0}`);
        lines.push(`ratlc_channel_open_attempts{channel="${ch.id}"${grpLbl}} ${ch.openAttempts || 0}`);
      }
      lines.push('# HELP ratlc_group_channels Channels in a group, broken down by state.');
      lines.push('# TYPE ratlc_group_channels gauge');
      for (const g of (p.groups || [])) {
        const isDflt = g.isDefault ? '1' : '0';
        lines.push(`ratlc_group_channels{group="${g.model}",default="${isDflt}",state="ready"} ${g.ready || 0}`);
        lines.push(`ratlc_group_channels{group="${g.model}",default="${isDflt}",state="busy"} ${g.busy || 0}`);
        lines.push(`ratlc_group_channels{group="${g.model}",default="${isDflt}",state="opening"} ${g.opening || 0}`);
        lines.push(`ratlc_group_channels{group="${g.model}",default="${isDflt}",state="dead"} ${g.dead || 0}`);
        lines.push(`ratlc_group_target{group="${g.model}",default="${isDflt}"} ${g.target || 0}`);
        lines.push(`ratlc_group_rounds{group="${g.model}",default="${isDflt}"} ${g.rounds || 0}`);
      }
      res.writeHead(200, { 'Content-Type': 'text/plain; version=0.0.4' });
      res.end(lines.join('\n') + '\n');
    } catch { /* keep accumulating */ }
  });
  sock.on('error', (e) => { clearTimeout(t); if (!res.writableEnded) { res.writeHead(503); res.end(`pool socket error: ${e.message}`); } });
}

// Debug endpoint: dump the in-process thinking buffer. Used by
// scaffolding/pool/reinject-thinking-test.mjs to verify capture without
// having to grep truncated logs. Off-by-default — only enabled when
// POOL_REINJECT_THINKING_DEBUG=1.
//   GET /v1/_debug/thinking_buffer        → all keys + sizes
//   GET /v1/_debug/thinking_buffer?convKey=XXXXXXXXXXXXXXXX
//                                          → the stored turns for one key
function handleThinkingBufferDebug(req, res) {
  if (process.env.POOL_REINJECT_THINKING_DEBUG !== '1') {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'not found' }));
    return;
  }
  const url = new URL(req.url, 'http://localhost');
  const convKey = url.searchParams.get('convKey');
  if (convKey) {
    const turns = thinkingBuffer.getForConvKey(convKey);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      enabled: thinkingBuffer.isEnabled(),
      convKey,
      turns,
      turnCount: turns.length,
    }));
    return;
  }
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    enabled: thinkingBuffer.isEnabled(),
    maxBytesPerTurn: thinkingBuffer.maxBytesPerTurn(),
    maxTurns: thinkingBuffer.maxTurns(),
    size: thinkingBuffer.size(),
  }));
}

// Debug endpoint: render the outbound prompt text for a given (synthetic)
// message body without actually sending it to the pool. Lets the E2E
// test verify the `<thinking>` block placement in the rendered prompt.
//   POST /v1/_debug/render  body = { messages, system, tools, model, convKey? }
function handleRenderDebug(req, res) {
  if (process.env.POOL_REINJECT_THINKING_DEBUG !== '1') {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'not found' }));
    return;
  }
  (async () => {
    let body;
    try { body = await readJsonBody(req); }
    catch (e) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ error: 'bad json' }));
    }
    const { messages, system, tools, model, convKey: convKeyOverride, mode: modeOverride } = body || {};
    let convKey = convKeyOverride;
    if (!convKey) {
      req.body = body;
      const sid = anthropicTools.extractClientSessionId(req);
      convKey = anthropicTools.deriveConversationKey(
        messages || [], model, system, tools,
        req.socket?.remoteAddress, req.socket?.remotePort, sid,
      );
    }
    const thinkingTurns = POOL_REINJECT_THINKING ? thinkingBuffer.getForConvKey(convKey) : [];
    const mode = (modeOverride === 'last' || modeOverride === 'full' || modeOverride === 'hybrid') ? modeOverride : POOL_CONTEXT_MODE;
    const effectiveMode = mode === 'hybrid'
      ? decideHybridContext({ clientSessionId: 'debug', convKey, routingModel: normalizeModelForRouting(model), system, tools, messages: messages || [] }).sendMode
      : mode;
    let rendered;
    if (effectiveMode === 'full') {
      rendered = renderFullContext({ messages: messages || [], system, tools, thinkingTurns });
    } else {
      const lastMsg = (messages || []).slice(-1)[0];
      const userText = lastMsg ? extractTextFromContent(lastMsg.content) : '';
      rendered = renderThinkingPreamble(thinkingTurns) + userText;
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      enabled: POOL_REINJECT_THINKING,
      mode,
      effectiveMode,
      convKey,
      thinkingTurnCount: thinkingTurns.length,
      rendered,
    }));
  })();
}

// ── POST /v1/messages/count_tokens (Anthropic Messages API) ────────────
//
// claude-code's `/context` command and Anthropic SDK fit-check paths
// POST here to ask "how many input tokens would this body cost?" Without
// the endpoint claude-code 404s and falls back to a rougher client-side
// char-count estimator. Anthropic's real endpoint returns
//   { "input_tokens": <int> }
// We approximate via character-count heuristic at ~3.5 chars/token (a
// decent estimate for English code/text on the Claude tokenizer). Good
// enough for fit checks; not for billing. Ported from the legacy
// server.js on `feat/anthropic-api-support` (lines 1478–1545 there).
function countCharsRecursive(value) {
  if (value == null) return 0;
  if (typeof value === 'string') return value.length;
  if (typeof value === 'number') return String(value).length;
  if (Array.isArray(value)) {
    let n = 0;
    for (const item of value) n += countCharsRecursive(item);
    return n;
  }
  if (typeof value === 'object') {
    let n = 0;
    for (const k of Object.keys(value)) {
      n += k.length; // count field-name overhead too
      n += countCharsRecursive(value[k]);
    }
    return n;
  }
  return 0;
}

async function handleCountTokens(req, res) {
  let body;
  try { body = await readJsonBody(req); }
  catch (e) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ type: 'error', error: { type: 'invalid_request_error', message: 'bad json' } }));
  }
  const { messages, system, tools } = body || {};
  if (!Array.isArray(messages)) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ type: 'error', error: { type: 'invalid_request_error', message: 'messages is required' } }));
  }
  let chars = 0;
  if (system != null) chars += countCharsRecursive(system);
  chars += countCharsRecursive(messages);
  if (Array.isArray(tools)) chars += countCharsRecursive(tools);
  // Round up so we never under-report (under-reporting risks claude-code
  // thinking a fork fits when it actually doesn't).
  const inputTokens = Math.ceil(chars / 3.5);
  log(`  count_tokens: messages=${messages.length} tools=${Array.isArray(tools) ? tools.length : 0} system=${system != null} chars=${chars} → ${inputTokens} tokens`);
  // Return the full Anthropic Messages count_tokens response shape. Newer
  // claude-code versions (2.1.x) check for the cache fields and treat
  // responses missing them as malformed → /context display falls back to
  // its client-side estimator. Zero values are correct for this proxy
  // since Cursor's backend doesn't expose prompt-caching to us.
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    input_tokens: inputTokens,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
  }));
}

function handleHealth(req, res) {
  // Open a one-shot socket to the pool — keeps administrative requests
  // off the main streaming socket.
  const sock = net.createConnection(POOL_SOCK);
  let buf = '';
  const timer = setTimeout(() => {
    try { sock.destroy(); } catch { /* ignore */ }
    if (!res.writableEnded) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'pool status timeout' }));
    }
  }, 5000);
  sock.on('connect', () => sock.write(JSON.stringify({ type: 'status' }) + '\n'));
  sock.on('data', (chunk) => {
    buf += chunk.toString('utf8');
    const idx = buf.indexOf('\n');
    if (idx === -1) return;
    try {
      const m = JSON.parse(buf.slice(0, idx));
      clearTimeout(timer);
      sock.end();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(m));
    } catch { /* not json yet — keep waiting */ }
  });
  sock.on('error', (e) => {
    clearTimeout(timer);
    if (!res.writableEnded) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: e.message }));
    }
  });
}

function handleRequests(req, res) {
  const url = new URL(req.url, 'http://localhost');
  const limitRaw = parseInt(url.searchParams.get('limit') || '100', 10);
  const limit = Math.min(Math.max(Number.isFinite(limitRaw) ? limitRaw : 100, 1), REQUEST_LOG_MAX);
  const now = Date.now();
  const items = requestLog.slice(0, limit).map((r) => ({
    ...r,
    ageMs: now - r.startedAt,
  }));
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    ok: true,
    max: REQUEST_LOG_MAX,
    count: requestLog.length,
    items,
  }));
}

// ── Server ──────────────────────────────────────────────────────────────
// High-frequency introspection/polling GETs — NOT logged. The TUI polls
// /v1/_stats every refresh; logging it floods the very api log the TUI tails,
// and the log-tail's dirty flag re-triggers the next poll (self-amplifying
// loop). Real traffic (POST /v1/messages) and one-shot endpoints stay logged.
const QUIET_LOG_GETS = new Set(['/v1/_stats', '/health', '/metrics', '/requests']);
const server = http.createServer(async (req, res) => {
  const path = (req.url || '').split('?')[0];
  if (!(req.method === 'GET' && QUIET_LOG_GETS.has(path))) log(`${req.method} ${req.url}`);
  if (req.method === 'POST' && path === '/v1/messages') return handleMessagesRequest(req, res);
  if (req.method === 'POST' && path === '/v1/messages/count_tokens') return handleCountTokens(req, res);
  if (req.method === 'GET' && (path === '/v1/models' || path === '/models')) return handleModels(req, res);
  if (req.method === 'GET' && path === '/health') return handleHealth(req, res);
  if (req.method === 'GET' && path === '/requests') return handleRequests(req, res);
  if (req.method === 'GET' && path === '/metrics') return handleMetrics(req, res);
  if (req.method === 'GET' && path === '/v1/_stats') return handleStats(req, res);
  if (req.method === 'GET' && path === '/v1/_debug/thinking_buffer') return handleThinkingBufferDebug(req, res);
  if (req.method === 'POST' && path === '/v1/_debug/render') return handleRenderDebug(req, res);
  if (req.method === 'HEAD') { res.writeHead(200); return res.end(); }
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'not found' }));
});

server.listen(PORT, HOST, () => {
  log(`api-server listening on http://${HOST}:${PORT}`);
  log(`pool socket: ${POOL_SOCK}`);
});

// Latency-metrics persistence: reload history at boot, flush periodically + on exit.
try { latencyMetrics.restore(); } catch { /* fresh start on corrupt/missing */ }
const _statsPersistTimer = setInterval(() => { try { latencyMetrics.persist(); } catch { /* ignore */ } }, 30_000);
if (_statsPersistTimer.unref) _statsPersistTimer.unref();
process.on('SIGINT', () => { try { latencyMetrics.persist(); } catch { /* ignore */ } try { server.close(); } catch { /* ignore */ } process.exit(0); });
process.on('SIGTERM', () => { try { latencyMetrics.persist(); } catch { /* ignore */ } try { server.close(); } catch { /* ignore */ } process.exit(0); });
