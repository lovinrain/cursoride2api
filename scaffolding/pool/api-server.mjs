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
import { isPoolLocalToolName, runPoolLocalTool } from './local-tool-executor.mjs';
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

// Bridge to the existing CommonJS anthropic-tools helpers so we can reuse
// `deriveConversationKey` and `extractClientSessionId` instead of porting
// them. The helpers depend on Node `crypto` only — no ESM coupling.
const _require = createRequire(import.meta.url);
const anthropicTools = _require('../../src/anthropic-tools.js');
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
// POOL_REINJECT_THINKING — opt-in symmetry with CURSOR_REINJECT_THINKING.
// When set, every thinking_delta arriving from the pool is appended to a
// per-convKey buffer; on subsequent turns the captured text is rendered
// back into the outbound prompt as `<thinking>...</thinking>` blocks.
// Default OFF (no behavior change vs. legacy). See thinking-buffer.mjs.
const POOL_REINJECT_THINKING = process.env.POOL_REINJECT_THINKING === '1';
const POOL_PROXY_THINKING_BLOCKS = process.env.POOL_PROXY_THINKING_BLOCKS === '1';
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
// history, tool results, and optional thinking back on every turn. Sending a
// very large full-context payload as a bajie_yield tool_result can make Cursor
// close or stall the live session. When full rendering crosses this byte-ish
// character limit, fall back to sticky last-turn delivery for that request.
// Set to 0 to disable.
const CONTEXT_MAX_BYTES = Math.max(0, parseInt(process.env.RATLC_CONTEXT_MAX_BYTES || process.env.POOL_CONTEXT_MAX_BYTES || '98304', 10));

const log = (...args) => console.log(`[${new Date().toISOString().slice(11, 23)}] [api]`, ...args);
log(`POOL_CONTEXT_MODE=${POOL_CONTEXT_MODE}  POOL_TOOL_MODE=${POOL_TOOL_MODE}  POOL_REINJECT_THINKING=${POOL_REINJECT_THINKING ? 1 : 0}  POOL_PROXY_THINKING_BLOCKS=${POOL_PROXY_THINKING_BLOCKS ? 1 : 0}  CONTEXT_MAX_BYTES=${CONTEXT_MAX_BYTES}`);

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
  return entry;
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
  const suffix = String(poolToolUseId || randomUUID()).replace(/[^A-Za-z0-9_-]/g, '').slice(-16);
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
      // No plaintext to render on the Cursor AgentService path.
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
  const normalizedTool = anthropicTools.normalizeClientToolNameForPolicy(toolName);
  if (normalizedTool !== 'write') return false;
  const a = args && typeof args === 'object' ? args : {};
  const p = String(a.file_path || a.path || a.filename || '').replace(/\\/g, '/');
  const c = String(a.content ?? a.file_text ?? a.text ?? a.body ?? a.data ?? '').trim();
  return /^agent-tools\/[^/]+\.txt$/i.test(p) && (c === '' || c === '(No content)');
}

async function handleMessagesRequest(req, res) {
  let body;
  try { body = await readJsonBody(req); }
  catch (e) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ type: 'error', error: { type: 'invalid_request_error', message: 'bad json' } }));
  }
  const { messages, system, tools, model } = body;
  const routingModel = normalizeModelForRouting(model);
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
  let thinkingCompletedCount = 0;
  let thinkingDurationMs = null;
  let rescuedHitCount = 0;
  const emittedToolUseKeys = new Set();
  const hallucinationFilter = new StreamingHallucinationFilter();
  const serverToolBlocks = new Map();
  const openServerTools = new Set();
  const visibleServerToolTraces = new Set();
  let serverWebSearchRequestCount = 0;
  // `messageStarted` gates startMsg() so it can only fire once per request.
  // Was previously gated on `blockIdx === -1`, but startMsg doesn't bump
  // blockIdx — so the route_decision branch AND the error branch would
  // both call startMsg(), emitting two `message_start` SSE events. That
  // shape is malformed enough that claude-code rejects the response with
  // "API returned an empty or malformed response (HTTP 200)".
  let messageStarted = false;
  let visibleUpstreamEventSeen = false;
  let noVisibleEventTimer = null;
  const NO_VISIBLE_EVENT_TIMEOUT_MS = Math.max(5_000, parseInt(
    process.env.RATLC_NO_VISIBLE_EVENT_TIMEOUT_MS || '25000',
    10,
  ));
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
  function armNoVisibleEventTimer() {
    if (noVisibleEventTimer || done || visibleUpstreamEventSeen) return;
    noVisibleEventTimer = setTimeout(() => {
      noVisibleEventTimer = null;
      if (done || visibleUpstreamEventSeen) return;
      log(`  → no visible upstream event timeout @${NO_VISIBLE_EVENT_TIMEOUT_MS}ms requestId=${requestId}`);
      finalStatusOverride = 'upstream_no_visible_event_timeout';
      finalErrorMessage = `No visible Cursor event within ${NO_VISIBLE_EVENT_TIMEOUT_MS}ms after routing`;
      emitTextDelta(
        `[proxy_notice] Cursor upstream accepted the request but did not emit text, thinking, tool_use, yield, or error within ${NO_VISIBLE_EVENT_TIMEOUT_MS}ms. ` +
        'The RATLC channel was likely waiting on an unrecognized Cursor exec message. Please retry after the channel is recycled.\n'
      );
      stopReason = 'end_turn';
      finishMessage();
    }, NO_VISIBLE_EVENT_TIMEOUT_MS);
  }
  function disarmNoVisibleEventTimer() {
    if (noVisibleEventTimer) {
      clearTimeout(noVisibleEventTimer);
      noVisibleEventTimer = null;
    }
  }
  function markVisibleUpstreamEvent() {
    visibleUpstreamEventSeen = true;
    disarmNoVisibleEventTimer();
  }

  function startMsg() {
    if (messageStarted) return;
    messageStarted = true;
    writeHeadersOnce();
    sseWrite(res, 'message_start', {
      type: 'message_start',
      message: {
        id: messageId, type: 'message', role: 'assistant',
        content: [], model: model || 'claude-opus-4-7',
        stop_reason: null, stop_sequence: null,
        usage: {
          input_tokens: extractTextFromContent(lastMsg.content).length / 4 | 0,
          output_tokens: 0,
          cache_creation_input_tokens: 0,
          cache_read_input_tokens: 0,
          server_tool_use: serverWebSearchRequestCount > 0
            ? { web_search_requests: serverWebSearchRequestCount }
            : null,
          service_tier: 'standard',
        },
      },
    });
    sseWrite(res, 'ping', { type: 'ping' });
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
    if (!thinkingBlockOpen && thinkingBlockStarted) return;
    if (!thinkingBlockOpen && blockIdx >= 0) return;
    startThinkingBlock();
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
    return String(id || ('srv_' + randomUUID().replace(/-/g, '').slice(0, 16))).replace(/[^A-Za-z0-9_-]/g, '_');
  }

  function emitServerToolUseEvent(event) {
    if (done) return;
    if (!event || event.name !== 'web_search') return;
    startMsg();
    stopThinkingBlock();
    emitPlaceholderThinkingBlock();
    stopTextBlock();
    const toolId = normalizeServerToolId(event.id);

    if (event.phase === 'started') {
      if (serverToolBlocks.has(toolId)) return;
      if (RENDER_SERVER_TOOL_TEXT && !visibleServerToolTraces.has(toolId)) {
        visibleServerToolTraces.add(toolId);
        const query = String(event.input?.query || '').replace(/\s+/g, ' ').trim();
        emitTextDelta(`[Cursor WebSearch] ${query || '(query unavailable)'}\n`);
        stopTextBlock();
      }
      blockIdx++;
      const idx = blockIdx;
      serverToolBlocks.set(toolId, idx);
      openServerTools.add(toolId);
      sseWrite(res, 'content_block_start', {
        type: 'content_block_start',
        index: idx,
        content_block: {
          type: 'server_tool_use',
          id: toolId,
          name: 'web_search',
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
      serverWebSearchRequestCount++;
      log(`→ server_tool_use to client: name=web_search query=${JSON.stringify(event.input?.query || '').slice(0, 160)} id=${toolId}`);
      return;
    }

    if (event.phase === 'completed') {
      if (!serverToolBlocks.has(toolId)) {
        emitServerToolUseEvent({ ...event, phase: 'started' });
      }
      openServerTools.delete(toolId);
      blockIdx++;
      const content = Array.isArray(event.content) ? event.content : (event.content || {
        type: 'web_search_tool_result_error',
        error_code: 'unavailable',
      });
      sseWrite(res, 'content_block_start', {
        type: 'content_block_start',
        index: blockIdx,
        content_block: {
          type: 'web_search_tool_result',
          tool_use_id: toolId,
          content,
        },
      });
      sseWrite(res, 'content_block_stop', { type: 'content_block_stop', index: blockIdx });
      log(`→ web_search_tool_result to client: id=${toolId} results=${Array.isArray(content) ? content.length : 'error'}`);
    }
  }

  function completeOpenServerTools(reason) {
    if (done || openServerTools.size === 0) return;
    for (const toolId of [...openServerTools]) {
      emitServerToolUseEvent({
        phase: 'completed',
        name: 'web_search',
        id: toolId,
        content: {
          type: 'web_search_tool_result_error',
          error_code: 'unavailable',
        },
        error: reason || 'Cursor backend WebSearch completed without exposing result metadata to the proxy.',
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
    if (/channel .* died/i.test(text)) {
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
    done = true;
    // Disarm watchdog centrally so the bookkeeping is symmetric across all
    // exit paths (step_completed / yield / error / watchdog / disconnect).
    // The watchdog-fired path used to leave a dangling reference because
    // the disarm was at the call sites of the other paths only.
    disarmToolUseFinalizer();
    disarmNoVisibleEventTimer();
    if (!toolUseEmitted && outputTokens === 0 && !textBlockOpen) {
      emitTextDelta('[proxy_notice] Cursor ended this turn without visible text or tool calls. Any upstream thinking was captured for the next request, but there is no assistant-visible content to display.\n');
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
        server_tool_use: serverWebSearchRequestCount > 0
          ? { web_search_requests: serverWebSearchRequestCount }
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
        markVisibleUpstreamEvent();
        if (!reqLog.firstByteAt) {
          reqLog.firstByteAt = Date.now();
          reqLog.firstByteMs = reqLog.firstByteAt - reqLog.startedAt;
          reqLog.status = 'thinking';
        }
        // Capture thinking text into the per-convKey buffer for re-injection
        // on the NEXT turn. Do NOT forward to the client SSE — Anthropic's
        // signed thinking blocks need a signature we can't produce, and
        // emitting unsigned blocks poisons claude-code's session against
        // direct-Anthropic resume. POOL_PROXY_THINKING_BLOCKS explicitly opts
        // into proxy-local display blocks with non-Anthropic signatures.
        emittedThinkingForDetection += msg.text || '';
        if (POOL_REINJECT_THINKING) thinkingBuffer.append(convKey, msg.text || '');
        if (POOL_PROXY_THINKING_BLOCKS && clientThinkingEnabled && msg.text) {
          emitThinkingDelta(msg.text);
        }
        if (toolUseEmitted) armToolUseFinalizer();
      } else if (msg.type === 'thinking_completed') {
        if (done) return;
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
        emitServerToolUseEvent(msg);
      } else if (msg.type === 'progress') {
        if (done) return;
        if (noVisibleEventTimer) {
          disarmNoVisibleEventTimer();
          armNoVisibleEventTimer();
        }
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
        if (msg.name === 'Write') {
          const fp = msg.args?.file_path || '';
          const content = msg.args?.content || '';
          const uuidV4Path = /^agent-tools\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.txt$/i;
          if (uuidV4Path.test(fp) && String(content).trim() === '') {
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
              '  - For a user-explicit URL fetch, WebFetch/Fetch may be used; Bash/curl is allowed only when the environment permits it.\n' +
              '  - Do NOT use WebFetch/Fetch as a broad-search substitute for Cursor-native WebSearch.\n' +
              '  - If you cannot fulfill the user request without web access, tell the ' +
              'user that and call `bajie_yield`.\n\n' +
              'DO NOT quote this proxy_notice as if it were search results. DO NOT ' +
              'fabricate web content.';
            log(`⚠ Write-spoof intercept: rewriting empty Write→${fp} content with proxy_notice (${noticeBody.length}B) requestId=${requestId}`);
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
            log(`→ local tool adapter: name=${xlated.name} args=${JSON.stringify(xlated.input).slice(0, 200)} id=${msg.anthropic_id}`);
            reqLog.status = 'local_tool';
            reqLog.lastToolName = xlated.name;
            runPoolLocalTool(xlated.name, xlated.input || {})
              .then((result) => {
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
        markVisibleUpstreamEvent();
        completeOpenServerTools('The response ended with an error before Cursor exposed WebSearch result metadata.');
        const softened = classifySoftenedPoolError(msg.message, msg.code);
        if (softened) {
          log(`  → soften pool error as text requestId=${requestId}: ${String(msg.message || '').slice(0, 180)}`);
          finalStatusOverride = softened.status;
          finalErrorMessage = softened.error || msg.message || null;
          startMsg();
          emitTextDelta(softened.text);
          stopReason = 'end_turn';
          finishMessage();
          return;
        }
        // Anthropic's real SSE for errors emits ONLY `event: error` and
        // closes the stream. NO message_delta + message_stop afterwards.
        // claude-code's parser treats an SSE that contains an `error`
        // event followed by message_delta/message_stop as malformed and
        // surfaces a second "API returned an empty or malformed response
        // (HTTP 200)" error on top of the original error message. So we
        // emit the error event, close the stream, and skip finishMessage.
        writeHeadersOnce({ 'x-ratlc-fallback': '0' });
        finishRequestLog(requestId, {
          status: 'error',
          error: msg.message,
          stopReason: 'error',
          outputTokens,
        });
        sseWrite(res, 'error', { type: 'error', error: { type: 'api_error', message: msg.message } });
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
      log(`  → reject mixed client-bridge/regular tool_result batch requestId=${requestId} bridged=${clientBridgeToolResults.length} regular=${regularToolResults.length}`);
      finalStatusOverride = 'error';
      finalErrorMessage = 'mixed client-bridge and regular tool_result batch';
      startMsg();
      emitTextDelta('[proxy_error] Mixed client-bridge and regular tool results arrived in one batch. Send these tool results in separate turns.\n');
      stopReason = 'end_turn';
      finishMessage();
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
    poolWrite({
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
    });
  }

	  function cancelForClientDisconnect() {
	    if (done) return;
	    log(`client disconnected mid-stream for ${requestId}`);
	    patchRequest(requestId, { clientDisconnectedAt: Date.now() });
	    poolWrite({ type: 'cancel_request', requestId, reason: 'client_disconnected' });
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
      { id: 'claude-opus-4-7-thinking-max-fast', type: 'model', display_name: 'Claude Opus 4.7 Thinking Max (Fast)', created_at: '2026-01-01T00:00:00Z' },
      { id: 'claude-4.6-opus-max-thinking-fast', type: 'model', display_name: 'Claude Opus 4.6 Max Thinking (Fast)', created_at: '2026-01-01T00:00:00Z' },
    ],
  }));
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
const server = http.createServer(async (req, res) => {
  const path = (req.url || '').split('?')[0];
  log(`${req.method} ${req.url}`);
  if (req.method === 'POST' && path === '/v1/messages') return handleMessagesRequest(req, res);
  if (req.method === 'POST' && path === '/v1/messages/count_tokens') return handleCountTokens(req, res);
  if (req.method === 'GET' && (path === '/v1/models' || path === '/models')) return handleModels(req, res);
  if (req.method === 'GET' && path === '/health') return handleHealth(req, res);
  if (req.method === 'GET' && path === '/requests') return handleRequests(req, res);
  if (req.method === 'GET' && path === '/metrics') return handleMetrics(req, res);
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

process.on('SIGINT', () => { try { server.close(); } catch { /* ignore */ } process.exit(0); });
process.on('SIGTERM', () => { try { server.close(); } catch { /* ignore */ } process.exit(0); });
