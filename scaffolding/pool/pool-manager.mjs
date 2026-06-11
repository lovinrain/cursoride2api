#!/usr/bin/env node
// Pool manager — long-lived daemon that forks N bridge-worker children,
// tracks state, routes requests, pings idle workers, auto-respawns dead
// ones. Exposes Unix socket /tmp/ratlc-pool.sock to api-server and
// ratlc-ctl. See ./IPC.md for the wire format.
//
// Multi-group: channels are partitioned into named GROUPS, each keyed by
// model id. The DEFAULT group is sized by POOL_MODEL/POOL_SIZE. Extra
// groups can be declared at boot via POOL_GROUPS or added/removed at
// runtime via the add_group/remove_group socket ops. Per-request routing
// uses the requested model to pick the matching group; on miss, falls
// back to the default group. See MULTI_GROUP_PLAN.md for the design.

import { fork } from 'node:child_process';
import net from 'node:net';
import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { defaultTranslateModeTools } from './tool-translator.mjs';
import { isFastModel, typeThresholds } from './model-utils.mjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const POOL_SIZE = parseInt(process.env.POOL_SIZE || '2', 10);
const POOL_SOCK = process.env.POOL_SOCK || '/tmp/ratlc-pool.sock';
// POOL_MODEL accepts two forms:
//   1. Single model name (legacy):    POOL_MODEL=claude-opus-4-7-thinking-max-fast
//      → Sized by POOL_SIZE. The first entry is always the DEFAULT group.
//   2. CSV form:                       POOL_MODEL=opus,haiku:3
//                                      POOL_MODEL=opus:5,haiku:3,composer-2-fast:1
//      → First entry is the default group. Each entry is `model` (using
//        POOL_SIZE) or `model:size` (explicit per-group size). Subsequent
//        entries become additional groups, mirroring POOL_GROUPS semantics.
//
// Both forms work in combination with POOL_GROUPS — entries from POOL_GROUPS
// are merged in the same way (add to existing group's targetSize, or create
// new group), so `POOL_MODEL=opus,haiku:3` is equivalent to
// `POOL_MODEL=opus POOL_GROUPS=haiku:3`.
const POOL_MODEL_RAW = process.env.POOL_MODEL || 'claude-opus-4-7-thinking-max-fast';
// parsePoolModelEnv: returns [{model, size}, ...] in declaration order. First
// entry is the default group. `size` is null when not explicitly given —
// callers default to POOL_SIZE for the default group, or treat it as a soft
// add-to-existing for later groups.
function parsePoolModelEnv(raw) {
  const trimmed = String(raw || '').trim();
  if (!trimmed) return [];
  const out = [];
  for (const piece of trimmed.split(',')) {
    const s = piece.trim();
    if (!s) continue;
    // Accept either "model" or "model:N" (N must be a non-negative int).
    const colonIdx = s.lastIndexOf(':');
    let model, size;
    if (colonIdx === -1) {
      model = s;
      size = null;
    } else {
      const sizeStr = s.slice(colonIdx + 1).trim();
      const sizeNum = parseInt(sizeStr, 10);
      if (/^\d+$/.test(sizeStr) && Number.isFinite(sizeNum) && sizeNum >= 0) {
        model = s.slice(0, colonIdx).trim();
        size = sizeNum;
      } else {
        // Treat as a model name that legitimately contains ':' (none of
        // Cursor's current model ids do, but be permissive).
        model = s;
        size = null;
      }
    }
    if (!model) continue;
    out.push({ model, size });
  }
  return out;
}
const poolModelEntries = parsePoolModelEnv(POOL_MODEL_RAW);
if (poolModelEntries.length === 0) {
  console.error(`invalid POOL_MODEL=${JSON.stringify(POOL_MODEL_RAW)} (parsed zero entries)`);
  process.exit(1);
}
const POOL_MODEL = poolModelEntries[0].model;
const POOL_MODEL_DEFAULT_SIZE = poolModelEntries[0].size != null ? poolModelEntries[0].size : POOL_SIZE;
const IDLE_PING_MS = parseInt(process.env.IDLE_PING_MS || '1200000', 10);
const PING_TIMEOUT_MS = parseInt(process.env.PING_TIMEOUT_MS || '45000', 10);
const STAGGER_OPEN_MS = parseInt(process.env.STAGGER_OPEN_MS || '5000', 10);
const POOL_TOOL_MODE = (process.env.POOL_TOOL_MODE || 'contract').toLowerCase();
const POOL_BRIDGE_PROTOCOL = (process.env.POOL_BRIDGE_PROTOCOL || 'h2').toLowerCase();
if (!['h1', 'h2'].includes(POOL_BRIDGE_PROTOCOL)) {
  console.error(`invalid POOL_BRIDGE_PROTOCOL=${POOL_BRIDGE_PROTOCOL} (must be h1|h2)`);
  process.exit(1);
}
const POOL_CONTEXT_MODE = (process.env.POOL_CONTEXT_MODE || 'last').toLowerCase();
if (!['full', 'last', 'hybrid'].includes(POOL_CONTEXT_MODE)) {
  console.error(`invalid POOL_CONTEXT_MODE=${POOL_CONTEXT_MODE} (must be full|last|hybrid)`);
  process.exit(1);
}
const POOL_REINJECT_THINKING = process.env.POOL_REINJECT_THINKING === '1';
// Global rate-limit budget shared across all groups (one account quota
// against Cursor's /Run endpoint). At most this many channels can be in
// the spawning/opening retry lottery at once.
const POOL_CONCURRENT_OPENS = Math.max(1, parseInt(process.env.POOL_CONCURRENT_OPENS || '1', 10));
// How long a request will wait for its target group to surface a ready
// channel before being eligible for fallback to the default group. Used
// when the target group exists but all its channels are opening / busy.
// 0 = fall back immediately.
const POOL_GROUP_WAIT_MS = Math.max(0, parseInt(process.env.POOL_GROUP_WAIT_MS || '5000', 10));
const RATLC_QUEUE_TIMEOUT_MS = Math.max(0, parseInt(process.env.RATLC_QUEUE_TIMEOUT_MS || process.env.POOL_QUEUE_TIMEOUT_MS || '120000', 10));
const RATLC_CONSUMED_TOOL_TTL_MS = Math.max(60_000, parseInt(process.env.RATLC_CONSUMED_TOOL_TTL_MS || '1800000', 10));
const RATLC_SESSION_TTL_MS = Math.max(60_000, parseInt(process.env.RATLC_SESSION_TTL_MS || process.env.POOL_HYBRID_SESSION_TTL_MS || '1800000', 10));
// Test hook: fork mock-worker.mjs instead of bridge-worker.mjs so the
// multi-group test suite can exercise routing without paying Cursor's
// retry lottery. NEVER set this outside tests.
const POOL_TEST_MOCK_CHANNELS = process.env.POOL_TEST_MOCK_CHANNELS === '1';
const WORKER_SCRIPT = path.join(__dirname, POOL_TEST_MOCK_CHANNELS ? 'mock-worker.mjs' : 'bridge-worker.mjs');

if (!['contract', 'translate'].includes(POOL_TOOL_MODE)) {
  console.error(`invalid POOL_TOOL_MODE=${POOL_TOOL_MODE} (must be contract|translate)`);
  process.exit(1);
}

const log = (...args) => console.log(`[${new Date().toISOString().slice(11, 23)}] [pool]`, ...args);

function normalizeModelForRouting(model) {
  return String(model || '').trim().replace(/\[[^\]]+\]$/g, '');
}

function rememberConsumedToolUse(id, entry) {
  if (!id) return;
  consumedToolUseIndex.set(id, {
    consumedAt: Date.now(),
    channelId: entry?.channelId || null,
    execId: entry?.execId || null,
  });
}

function getConsumedToolUse(id) {
  if (!id) return null;
  const entry = consumedToolUseIndex.get(id);
  if (!entry) return null;
  if (Date.now() - entry.consumedAt > RATLC_CONSUMED_TOOL_TTL_MS) {
    consumedToolUseIndex.delete(id);
    return null;
  }
  return entry;
}

function evictConsumedToolUses(now = Date.now()) {
  for (const [id, entry] of consumedToolUseIndex) {
    if (now - entry.consumedAt > RATLC_CONSUMED_TOOL_TTL_MS) consumedToolUseIndex.delete(id);
  }
}
setInterval(evictConsumedToolUses, 5 * 60_000).unref();

function getPendingToolUseIdsForChannel(channelId) {
  if (!channelId) return [];
  const ids = pendingToolUseIdsByChannel.get(channelId);
  return ids ? [...ids] : [];
}

function rememberPendingToolUse(channelId, anthropicId) {
  if (!channelId || !anthropicId) return;
  let ids = pendingToolUseIdsByChannel.get(channelId);
  if (!ids) {
    ids = new Set();
    pendingToolUseIdsByChannel.set(channelId, ids);
  }
  ids.add(anthropicId);
}

function forgetPendingToolUse(channelId, anthropicId) {
  if (!channelId || !anthropicId) return;
  const ids = pendingToolUseIdsByChannel.get(channelId);
  if (!ids) return;
  ids.delete(anthropicId);
  if (ids.size === 0) pendingToolUseIdsByChannel.delete(channelId);
}

function clearPendingToolUsesForChannel(channelId) {
  if (!channelId) return;
  const ids = pendingToolUseIdsByChannel.get(channelId);
  if (ids) {
    for (const id of ids) toolUseIndex.delete(id);
    pendingToolUseIdsByChannel.delete(channelId);
  }
  heldToolResultsByChannel.delete(channelId);
}

function getHeldToolResultsForChannel(channelId) {
  if (!channelId) return null;
  return heldToolResultsByChannel.get(channelId) || null;
}

function rememberHeldToolResults(channelId, requestId, providedById) {
  if (!channelId || !providedById || providedById.size === 0) return;
  heldToolResultsByChannel.set(channelId, {
    requestId,
    heldAt: Date.now(),
    providedById: new Map(providedById),
  });
}

function mergeHeldToolResults(channelId, requestId, providedById) {
  const held = getHeldToolResultsForChannel(channelId);
  if (!held) return providedById;
  const merged = new Map(held.providedById);
  for (const [id, value] of providedById) merged.set(id, value);
  heldToolResultsByChannel.set(channelId, {
    requestId,
    heldAt: held.heldAt || Date.now(),
    providedById: merged,
  });
  return merged;
}

function clearHeldToolResults(channelId) {
  if (!channelId) return;
  heldToolResultsByChannel.delete(channelId);
}

function buildResolvedToolResults(channelId, providedById) {
  const pendingIds = getPendingToolUseIdsForChannel(channelId);
  const pending = [];
  const missing = [];
  for (const id of pendingIds) {
    const provided = providedById.get(id);
    const entry = provided?.entry || toolUseIndex.get(id);
    if (!entry) continue;
    pending.push({
      anthropic_tool_use_id: id,
      execId: entry.execId,
      toolUseKey: entry.toolUseKey,
      content: provided?.content,
      provided: providedById.has(id),
    });
    if (!providedById.has(id)) missing.push(id);
  }
  return { pendingIds, pending, missing };
}

function routeCompleteToolResults({ client, requestId, model, channelId, resolved }) {
  const ch = channels.get(channelId);
  if (!ch || ch.state === 'dead') {
    log(`  ❌ channel ${channelId} no longer alive (state=${ch?.state})`);
    writeToClient(client, { type: 'error', requestId, message: `channel ${channelId} no longer alive` });
    return false;
  }
  for (const r of resolved) {
    const entry = toolUseIndex.get(r.anthropic_tool_use_id);
    toolUseIndex.delete(r.anthropic_tool_use_id);
    forgetPendingToolUse(channelId, r.anthropic_tool_use_id);
    rememberConsumedToolUse(r.anthropic_tool_use_id, entry);
  }
  clearHeldToolResults(channelId);
  log(`  ✅ routing ${resolved.length} result(s) to ${channelId} (group=${ch.group}) execIds=[${resolved.map(r => r.execId).join(', ')}] (state was ${ch.state})`);
  ch.currentRequestId = requestId;
  ch.state = 'busy';
  ch.busyAt = Date.now();
  ch.lastActivityAt = Date.now();
  requestClient.set(requestId, client);
  writeToClient(client, {
    type: 'route_decision', requestId, channelId: ch.id,
    servedModel: ch.group, requestedModel: model || ch.group,
    fallback: false, fallbackReason: null,
  });
  ch.proc.send({
    type: 'send_tool_results', requestId,
    results: resolved.map((r) => ({ execId: r.execId, toolUseKey: r.toolUseKey, content: r.content })),
  });
  return true;
}

function writeRouteDecisionForChannel(client, requestId, ch, model) {
  if (!client || !ch) return;
  writeToClient(client, {
    type: 'route_decision', requestId, channelId: ch.id,
    servedModel: ch.group, requestedModel: model || ch.group,
    fallback: false, fallbackReason: null,
  });
}

function holdPartialToolResults({ client, requestId, model, channelId, providedById, pendingIds, missing }) {
  rememberHeldToolResults(channelId, requestId, providedById);
  const ch = channels.get(channelId);
  if (!ch || ch.state === 'dead') {
    log(`  ❌ channel ${channelId} no longer alive while holding partial tool results (state=${ch?.state})`);
    writeToClient(client, { type: 'error', requestId, message: `channel ${channelId} no longer alive` });
    return false;
  }
  log(`  ⏳ hold partial tool_result batch for ${channelId}; provided=[${[...providedById.keys()].join(', ')}] pending=[${pendingIds.join(', ')}] missing=[${missing.join(', ')}]`);
  requestClient.set(requestId, client);
  ch.currentRequestId = requestId;
  ch.lastActivityAt = Date.now();
  writeRouteDecisionForChannel(client, requestId, ch, model);
  for (const missingId of missing) {
    const missingEntry = toolUseIndex.get(missingId);
    if (!missingEntry) continue;
    writeToClient(client, {
      type: 'tool_use',
      requestId,
      anthropic_id: missingId,
      name: missingEntry.toolName,
      args: missingEntry.args || {},
      late: true,
    });
  }
  return true;
}

function getSessionAffinity(sessionKey) {
  if (!sessionKey) return null;
  const entry = sessionAffinity.get(sessionKey);
  if (!entry) return null;
  if (Date.now() - entry.lastAccessMs > RATLC_SESSION_TTL_MS) {
    sessionAffinity.delete(sessionKey);
    return null;
  }
  return entry;
}

function rememberSessionAffinity(sessionKey, ch, model) {
  if (!sessionKey || !ch) return;
  sessionAffinity.set(sessionKey, {
    channelId: ch.id,
    group: ch.group,
    model: model || ch.group,
    lastAccessMs: Date.now(),
  });
}

function evictSessionAffinity(now = Date.now()) {
  for (const [key, entry] of sessionAffinity) {
    if (now - entry.lastAccessMs > RATLC_SESSION_TTL_MS) sessionAffinity.delete(key);
  }
}
setInterval(evictSessionAffinity, 5 * 60_000).unref();

// ── Groups ───────────────────────────────────────────────────────────────
// A Group is a named partition of the pool keyed by `model`. Channels in
// the group are all opened with that model. The DEFAULT group is keyed
// by POOL_MODEL.
//
//   { model, isDefault, targetSize, channels: Set<channelId>, draining: bool }
//
// `draining` is set by remove_group — refuse new requests, let in-flight
// finish, then evict the group entry when channel count hits 0.
const groups = new Map();

function makeGroup(model, targetSize, isDefault = false) {
  return { model, isDefault, targetSize: Math.max(0, targetSize | 0), channels: new Set(), draining: false };
}

function getDefaultGroup() {
  return groups.get(POOL_MODEL);
}

function parsePoolGroupsEnv() {
  const raw = (process.env.POOL_GROUPS || '').trim();
  if (!raw) return [];
  const out = [];
  for (const piece of raw.split(',')) {
    const s = piece.trim();
    if (!s) continue;
    const m = s.match(/^([^:]+):(\d+)$/);
    if (!m) {
      log(`POOL_GROUPS: ignoring malformed entry "${s}" (want "model:N")`);
      continue;
    }
    const model = m[1].trim();
    const size = parseInt(m[2], 10);
    if (!model || !Number.isFinite(size) || size < 0) {
      log(`POOL_GROUPS: ignoring entry "${s}" (invalid model or size)`);
      continue;
    }
    out.push({ model, size, foldIntoDefault: model === POOL_MODEL });
  }
  return out;
}

// ── Worker (channel) record ──────────────────────────────────────────────
let nextChannelSeq = 0;
const channels = new Map();

// ── Token rotation + health ──────────────────────────────────────────────
// token.json is shaped { tokens: [{name, accessToken, machineId, macMachineId}, ...] }.
// Round-robin assignment at spawn time: channel N gets the next live token.
// Spreads soft-rate-limit pressure across multiple accounts when more than
// one token is present. With a single token in the file (the typical case)
// every channel still uses tokens[0] — identical to the previous behavior.
//
// Health tracking: each spawned bridge-worker reports `token_validated` the
// first time it gets a post-auth response from Cursor (opened / unpaid /
// rate_limit_* / no_yield — anything that isn't `other_error`). If a token's
// workers die with `errorKind: 'other_error'` N consecutive times without
// EVER reporting validation, the token is marked dead and skipped in the
// rotation. This is fast detection: at typical retry rates, 3 strikes
// surface within 15-90s.
const _tokenPath = path.resolve(__dirname, '..', '..', 'token.json');
let _tokenCount = 1;
let _tokenNames = ['(default)'];
try {
  const _tokenFile = JSON.parse(fs.readFileSync(_tokenPath, 'utf8'));
  if (Array.isArray(_tokenFile.tokens) && _tokenFile.tokens.length > 0) {
    _tokenCount = _tokenFile.tokens.length;
    _tokenNames = _tokenFile.tokens.map((t, i) => t.name || `token-${i}`);
  }
} catch (e) {
  log(`WARN: failed to read ${_tokenPath} for rotation count: ${e.message} — assuming single token`);
}
const TOKEN_DEATH_THRESHOLD = parseInt(process.env.RATLC_TOKEN_DEATH_THRESHOLD || '3', 10);
const _tokenValidated = new Array(_tokenCount).fill(false);
const _tokenOtherErrors = new Array(_tokenCount).fill(0);
const _tokenDead = new Array(_tokenCount).fill(false);
const _tokenLastError = new Array(_tokenCount).fill(null);
log(`token rotation: ${_tokenCount} token(s) loaded — [${_tokenNames.join(', ')}], death-threshold=${TOKEN_DEATH_THRESHOLD}`);
let _nextTokenIdx = 0;
function nextTokenIndex() {
  // Try up to _tokenCount steps to find a live token. If all are dead, fall
  // through to index 0 anyway with a critical log — better to keep trying
  // than to freeze the pool.
  for (let i = 0; i < _tokenCount; i++) {
    const idx = _nextTokenIdx % _tokenCount;
    _nextTokenIdx = (_nextTokenIdx + 1) % _tokenCount;
    if (!_tokenDead[idx]) return idx;
  }
  log('CRITICAL: every token is marked dead; falling back to index 0 anyway');
  return 0;
}
function markTokenValidated(idx) {
  if (idx < 0 || idx >= _tokenCount) return;
  if (!_tokenValidated[idx]) {
    log(`token[${idx}]=${_tokenNames[idx]} validated (reached Cursor past auth)`);
  }
  _tokenValidated[idx] = true;
  _tokenOtherErrors[idx] = 0;  // reset strike count
}
function recordTokenOtherError(idx, errMsg) {
  if (idx < 0 || idx >= _tokenCount) return;
  _tokenLastError[idx] = errMsg ? String(errMsg).slice(0, 200) : null;
  if (_tokenValidated[idx]) return;  // proven good before, this is a transient
  _tokenOtherErrors[idx]++;
  if (_tokenOtherErrors[idx] >= TOKEN_DEATH_THRESHOLD && !_tokenDead[idx]) {
    _tokenDead[idx] = true;
    log(`⚠ TOKEN DEAD: token[${idx}]=${_tokenNames[idx]} marked dead after ${_tokenOtherErrors[idx]} consecutive other_error failures with no validation. lastError="${_tokenLastError[idx]}"`);
    log(`  → future channel spawns will skip this token. ratlc down + fix token.json + ratlc up to revive.`);
  }
}
// Definite-fatal kinds (auth_error, quota_exhausted) — Cursor explicitly
// told us this token is permanently broken (invalid login / account quota
// exhausted). No retry threshold; mark dead on first strike.
function killTokenImmediately(idx, errorKind, errMsg) {
  if (idx < 0 || idx >= _tokenCount) return;
  _tokenLastError[idx] = errMsg ? String(errMsg).slice(0, 200) : null;
  _tokenOtherErrors[idx]++;
  if (!_tokenDead[idx]) {
    _tokenDead[idx] = true;
    log(`⚠ TOKEN DEAD (${errorKind}): token[${idx}]=${_tokenNames[idx]} marked dead on first strike. Cursor returned ${errorKind === 'auth_error' ? 'auth failure (invalid/expired token)' : 'account quota exhausted'}. lastError="${_tokenLastError[idx]}"`);
    log(`  → future channel spawns will skip this token. ratlc down + fix token.json + ratlc up to revive.`);
  }
}

const requestQueue = [];
const toolUseIndex = new Map();
const consumedToolUseIndex = new Map();
const sessionAffinity = new Map();
const requestClient = new Map();
const pendingToolUseIdsByChannel = new Map();
const heldToolResultsByChannel = new Map();

// Empirical: Cursor only reads tools from requestContextResult ONCE per
// stream. Tools are pinned at worker open. The contract is GLOBAL across
// groups (claude-code uses consistent tools regardless of model).
let poolTools = null;
let poolSystem = null;
let toolsSignature = '';

if (POOL_TOOL_MODE === 'translate') {
  poolTools = defaultTranslateModeTools();
  poolSystem = '';
  toolsSignature = 'translate-mode-static';
}

const POOL_SIG_MODE = (process.env.POOL_SIG_MODE || 'name').toLowerCase();

function signatureOf(tools) {
  if (!Array.isArray(tools)) return '';
  const filtered = tools.filter((t) => t && t.name);
  if (POOL_SIG_MODE === 'schema') {
    return filtered
      .map((t) => `${t.name}:${JSON.stringify(t.input_schema || t.jsonSchema || {})}`)
      .sort().join('|');
  }
  return filtered.map((t) => t.name).sort().join(',');
}

function setPoolContract(system, tools) {
  poolSystem = system || '';
  poolTools = tools || [];
  toolsSignature = signatureOf(tools);
}

function poolNeedsReopen(tools) {
  return signatureOf(tools) !== toolsSignature;
}

function reopenAllChannels() {
  log(`recycling all channels (new tools sig=[${toolsSignature.slice(0, 80)}])`);
  for (const ch of channels.values()) {
    try { ch.proc.send({ type: 'shutdown' }); } catch { /* ignore */ }
  }
}

// ── Channel management ──────────────────────────────────────────────────
function spawnChannel(group) {
  const channelId = `ch-${nextChannelSeq++}`;
  const tokenIdx = nextTokenIndex();
  const tokenName = _tokenNames[tokenIdx];
  const env = {
    ...process.env,
    RATLC_CHANNEL_ID: channelId,
    RATLC_MODEL: group.model,
    RATLC_TOKEN_INDEX: String(tokenIdx),
    BRIDGE_PROTOCOL: POOL_BRIDGE_PROTOCOL,
    POOL_CONTEXT_MODE,
    POOL_REINJECT_THINKING: POOL_REINJECT_THINKING ? '1' : '0',
    RATLC_PASSTHROUGH_NATIVE: POOL_TOOL_MODE === 'translate' ? '1' : '0',
    ...(POOL_TOOL_MODE === 'translate' ? {
      CURSOR_STALL_TIMEOUT_MS_WITH_CONTENT: '1800000',
      CURSOR_STALL_TIMEOUT_MS: '600000',
      CURSOR_LOG_NATIVE_EXEC: '1',
    } : {}),
  };
  const proc = fork(WORKER_SCRIPT, [], { env, silent: false });
  const ch = {
    id: channelId,
    proc,
    pid: proc.pid,
    group: group.model,
    tokenIdx,
    tokenName,
    state: 'spawning',
    openAttempts: 0,
    openedAt: 0,
    lastActivityAt: Date.now(),
    spawnedAt: Date.now(),
    busyAt: null,
    // Wall-clock of the last forward-progress frame (text/thinking/tool/progress).
    // Distinct from lastActivityAt (which also moves on state/heartbeat); the TUI
    // uses the gap since this to split a busy channel into "thinking" (frames
    // flowing) vs "busy" (silent — the truly-suspect state).
    lastProgressAt: null,
    currentRequestId: null,
    pendingExecId: null,
    pendingAnthropicId: null,
    roundsServed: 0,
    error: null,
  };
  channels.set(channelId, ch);
  group.channels.add(channelId);

  proc.on('message', (msg) => handleWorkerMessage(ch, msg));
  proc.on('exit', (code, signal) => handleWorkerExit(ch, code, signal));
  proc.on('error', (err) => {
    log(`channel ${channelId} proc error:`, err.message);
  });

  if (poolTools !== null) {
    proc.send({ type: 'open', model: group.model, tools: poolTools, system: poolSystem });
  }
  log(`spawned ${channelId} (pid=${proc.pid}, group=${group.model}, token[${tokenIdx}]=${tokenName}); pool size=${channels.size}`);
  return ch;
}

function handleWorkerMessage(ch, msg) {
  if (msg.type === 'yield' && ch._pingTimer && ch.currentRequestId &&
      String(ch.currentRequestId).startsWith('ping-')) {
    clearTimeout(ch._pingTimer);
    ch._pingTimer = null;
    log(`ping ${ch.currentRequestId} OK on ${ch.id} (idle reset)`);
    ch.currentRequestId = null;
    ch.lastActivityAt = Date.now();
    ch.state = 'ready';
    setImmediate(drainQueue);
    return;
  }
  if (msg.type === 'error' && ch._pingTimer && ch.currentRequestId &&
      String(ch.currentRequestId).startsWith('ping-')) {
    clearTimeout(ch._pingTimer);
    ch._pingTimer = null;
    log(`ping ${ch.currentRequestId} FAILED on ${ch.id}: ${msg.message}`);
    try { ch.proc.kill('SIGTERM'); } catch { /* ignore */ }
    return;
  }

  switch (msg.type) {
    case 'state':
      ch.state = msg.state;
      ch.openAttempts = msg.openAttempts || ch.openAttempts;
      ch.openedAt = msg.openedAt || ch.openedAt;
      // Defensive: lastActivityAt must only move forward. A stale IPC msg
      // (e.g. setState called before lastActivityAt was bumped in the
      // worker) could otherwise roll the clock back and trick the
      // busy-watchdog into killing freshly-routed channels.
      if (msg.lastActivityAt && msg.lastActivityAt > (ch.lastActivityAt || 0)) {
        ch.lastActivityAt = msg.lastActivityAt;
      }
      ch.error = msg.error || null;
      if (msg.errorKind) ch.errorKind = msg.errorKind;
      // Worker-driven state change: if it just left 'busy', reset busyAt
      // so the TUI's BUSY column collapses back to '-'.
      if (msg.state !== 'busy') ch.busyAt = null;
      if (msg.state === 'ready') {
        log(`channel ${ch.id} (group=${ch.group}) READY after ${ch.openAttempts} attempts (${((Date.now() - ch.spawnedAt) / 1000).toFixed(1)}s)`);
        const g = groups.get(ch.group);
        if (g && g.draining) {
          log(`channel ${ch.id} reached READY but group ${ch.group} is draining — killing`);
          try { ch.proc.send({ type: 'shutdown' }); } catch { /* ignore */ }
          return;
        }
        setImmediate(drainQueue);
        setImmediate(maybeSpawnNext);
      }
      break;

    case 'heartbeat':
      if (msg.lastActivityAt && msg.lastActivityAt > (ch.lastActivityAt || 0)) {
        ch.lastActivityAt = msg.lastActivityAt;
      }
      break;

    case 'progress':
      // Liveness breadcrumb (Option B): a raw upstream frame arrived before
      // the first visible event. Treat it as channel activity so the
      // busy-watchdog (RATLC_BUSY_STUCK_TIMEOUT_MS) doesn't reap a channel
      // that's slow-to-first-byte but demonstrably alive, then forward it so
      // the api-server can reset its liveness-gap timer.
      ch.lastActivityAt = Date.now();
      ch.lastProgressAt = Date.now();
      forwardToClient(ch, msg);
      break;

    case 'text_delta':
    case 'thinking_delta':
    case 'thinking_completed':
    case 'server_tool_use':
    case 'tool_use':
    case 'step_completed':
      // Streaming model output. Stamp lastProgressAt (powers the TUI's
      // thinking/busy-silent split) AND bump lastActivityAt so the busy-watchdog
      // never reaps a channel that is actively producing reasoning or text —
      // these frames, unlike `progress`, previously updated neither, so a long
      // (>BUSY_STUCK) thinking or answer stream could be SIGTERMed mid-flight.
      ch.lastProgressAt = Date.now();
      ch.lastActivityAt = Date.now();
      forwardToClient(ch, msg);
      break;

    case 'yield':
    case 'error':
      forwardToClient(ch, msg);
      break;

    case 'log':
      log(`[${ch.id}] ${msg.level}: ${msg.message}`);
      break;

    case 'token_validated':
      // Worker saw a post-auth response (opened / unpaid / rate_limit_* /
      // no_yield) for the first time — the token is proven good, regardless
      // of whether the channel reaches READY.
      markTokenValidated(typeof msg.tokenIdx === 'number' ? msg.tokenIdx : ch.tokenIdx);
      break;
  }
}

function handleWorkerExit(ch, code, signal) {
  log(`channel ${ch.id} (group=${ch.group}) exited code=${code} signal=${signal} state=${ch.state}${ch.errorKind ? ` errorKind=${ch.errorKind}` : ''}${ch.error ? ` error="${String(ch.error).slice(0, 120)}"` : ''}`);
  // Feed token health: an other_error death on a token that hasn't been
  // validated counts as a strike. auth_error and quota_exhausted are
  // explicit signals from Cursor that the token is broken — mark dead
  // immediately, no threshold. Once dead, future spawns skip this token.
  if (typeof ch.tokenIdx === 'number') {
    if (ch.errorKind === 'auth_error' || ch.errorKind === 'quota_exhausted') {
      killTokenImmediately(ch.tokenIdx, ch.errorKind, ch.error);
    } else if (ch.errorKind === 'other_error') {
      recordTokenOtherError(ch.tokenIdx, ch.error);
    }
  }
  channels.delete(ch.id);
  clearPendingToolUsesForChannel(ch.id);
  const g = groups.get(ch.group);
  if (g) g.channels.delete(ch.id);
  if (ch.currentRequestId) {
    const client = requestClient.get(ch.currentRequestId);
    if (client) {
      writeToClient(client, {
        type: 'error',
        requestId: ch.currentRequestId,
        message: `channel ${ch.id} died (code=${code} signal=${signal})`,
      });
    }
    requestClient.delete(ch.currentRequestId);
  }
  if (g && g.draining && g.channels.size === 0) {
    groups.delete(g.model);
    log(`group ${g.model} fully drained — removed`);
  }
  setTimeout(maybeSpawnNext, 500);
}

function countOpening() {
  let n = 0;
  for (const ch of channels.values()) {
    if (ch.state === 'spawning' || ch.state === 'opening') n++;
  }
  return n;
}

function maybeSpawnNext() {
  while (countOpening() < POOL_CONCURRENT_OPENS) {
    let spawned = false;
    for (const g of groups.values()) {
      if (g.draining) continue;
      if (g.channels.size < g.targetSize) {
        spawnChannel(g);
        spawned = true;
        if (countOpening() >= POOL_CONCURRENT_OPENS) return;
      }
    }
    if (!spawned) return;
  }
}

function forwardToClient(ch, msg) {
  const reqId = msg.requestId;
  if (!reqId) return;
  const client = requestClient.get(reqId);
  if (!client) return;

  if (msg.type === 'tool_use') {
    const anthropic_id = 'toolu_' + randomUUID().replace(/-/g, '').slice(0, 16);
    toolUseIndex.set(anthropic_id, {
      channelId: ch.id,
      execId: msg.execId,
      toolUseKey: anthropic_id,
      toolName: msg.name,
      args: msg.args,
    });
    rememberPendingToolUse(ch.id, anthropic_id);
    try {
      ch.proc.send({
        type: 'remember_tool_use',
        toolUseKey: anthropic_id,
        info: {
          id: msg.id,
          execId: msg.execId,
          toolCallId: msg.toolCallId,
          toolName: msg.name,
          args: msg.args,
          ...(msg.origin ? { origin: msg.origin } : {}),
          ...(msg.originKey ? { originKey: msg.originKey } : {}),
        },
      });
    } catch { /* worker may have exited */ }
      if (!msg.origin) {
        ch.pendingExecId = msg.execId;
        ch.pendingAnthropicId = anthropic_id;
      }
    writeToClient(client, {
      type: 'tool_use',
      requestId: reqId,
      anthropic_id,
        name: msg.name,
        args: msg.args,
        late: ch.currentRequestId !== reqId,
      });
    return;
  }

  if (msg.type === 'yield') {
    clearPendingToolUsesForChannel(ch.id);
    ch.currentRequestId = null;
    ch.roundsServed = (ch.roundsServed || 0) + 1;
    requestClient.delete(reqId);
    writeToClient(client, { type: 'yield', requestId: reqId });
    setImmediate(drainQueue);
    const g = groups.get(ch.group);
    if (g && g.draining) {
      log(`channel ${ch.id} idle on draining group ${ch.group} — killing`);
      try { ch.proc.send({ type: 'shutdown' }); } catch { /* ignore */ }
    }
    return;
  }

  if (msg.type === 'error') {
    clearPendingToolUsesForChannel(ch.id);
    requestClient.delete(reqId);
    ch.currentRequestId = null;
    writeToClient(client, { type: 'error', requestId: reqId, message: msg.message });
    return;
  }

  writeToClient(client, msg);
}

// ── Routing ─────────────────────────────────────────────────────────────
function pickReadyChannelInGroup(g) {
  let best = null;
  for (const channelId of g.channels) {
    const ch = channels.get(channelId);
    if (!ch || ch.state !== 'ready') continue;
    if (!best || ch.lastActivityAt < best.lastActivityAt) best = ch;
  }
  return best;
}

function pickStickyReadyChannel(job, g) {
  const sticky = getSessionAffinity(job.sessionKey);
  if (!sticky || sticky.group !== g.model) return null;
  const ch = channels.get(sticky.channelId);
  if (!ch || ch.state !== 'ready' || ch.group !== g.model) return null;
  sticky.lastAccessMs = Date.now();
  return ch;
}

function tryPickForJob(job) {
  const dflt = getDefaultGroup();
  if (!dflt) return null;
  const requestedModel = job.routeModel || job.model;
  if (requestedModel) {
    const g = groups.get(requestedModel);
    if (g && !g.draining) {
      const ch = pickStickyReadyChannel(job, g) || pickReadyChannelInGroup(g);
      if (ch) {
        return { channel: ch, servedModel: g.model, fallback: false, fallbackReason: null };
      }
      // Known target group but no ready channel yet — caller decides
      // whether to wait or fall back via job.fallbackArmed.
      return null;
    }
    if (!job.fallbackArmed) {
      job.fallbackArmed = true;
      job.fallbackReason = g ? 'group-draining' : 'unknown-model';
    }
  }
  const ch = pickStickyReadyChannel(job, dflt) || pickReadyChannelInGroup(dflt);
  if (ch) {
    const fallback = !!requestedModel && requestedModel !== dflt.model;
    return {
      channel: ch,
      servedModel: dflt.model,
      fallback,
      fallbackReason: fallback ? (job.fallbackReason || 'unknown-model') : null,
    };
  }
  return null;
}

function armFallbackTimer(job) {
  const targetModel = job.routeModel || job.model;
  if (job.waitTimer || !targetModel) return;
  const g = groups.get(targetModel);
  if (!g || g.draining) return;
  if (targetModel === POOL_MODEL) return;
  job.waitTimer = setTimeout(() => {
    job.waitTimer = null;
    job.fallbackArmed = true;
    job.fallbackReason = 'group-no-ready';
    log(`req ${job.requestId}: target group ${job.routeModel || job.model} had no ready channel within ${POOL_GROUP_WAIT_MS}ms — eligible for default fallback`);
    setImmediate(drainQueue);
  }, POOL_GROUP_WAIT_MS);
}

function armQueueTimeoutTimer(job) {
  if (job.queueTimer || RATLC_QUEUE_TIMEOUT_MS <= 0) return;
  job.queueTimer = setTimeout(() => {
    job.queueTimer = null;
    const idx = requestQueue.indexOf(job);
    if (idx === -1) return;
    requestQueue.splice(idx, 1);
    clearJobTimers(job);
    const waitedMs = Date.now() - (job.queuedAt || Date.now());
    const target = job.routeModel || job.model || POOL_MODEL;
    log(`req ${job.requestId}: queue timeout after ${waitedMs}ms target=${target}`);
    writeToClient(job.client, {
      type: 'error',
      requestId: job.requestId,
      code: 'no_ready_timeout',
      targetModel: target,
      waitedMs,
      message: `no ready RATLC channel for ${target} within ${RATLC_QUEUE_TIMEOUT_MS}ms`,
    });
  }, RATLC_QUEUE_TIMEOUT_MS);
}

function clearJobTimers(job) {
  if (job.waitTimer) { clearTimeout(job.waitTimer); job.waitTimer = null; }
  if (job.queueTimer) { clearTimeout(job.queueTimer); job.queueTimer = null; }
}

function cancelRequest(requestId, reason = 'cancelled') {
  if (!requestId) return false;
  for (let i = 0; i < requestQueue.length; i++) {
    const job = requestQueue[i];
    if (job.requestId !== requestId) continue;
    requestQueue.splice(i, 1);
    clearJobTimers(job);
    requestClient.delete(requestId);
    log(`req ${requestId}: cancelled while queued (${reason})`);
    return true;
  }
  for (const ch of channels.values()) {
    if (ch.currentRequestId !== requestId) continue;
    requestClient.delete(requestId);
    clearPendingToolUsesForChannel(ch.id);
    log(`req ${requestId}: cancelling active channel ${ch.id} (${reason})`);
    try { ch.proc.kill('SIGTERM'); } catch { /* ignore */ }
    return true;
  }
  requestClient.delete(requestId);
  return false;
}

function drainQueue() {
  let progress = true;
  while (progress) {
    progress = false;
    for (let i = 0; i < requestQueue.length; i++) {
      const job = requestQueue[i];
      let pick;
      if (job.fallbackArmed) {
        const dflt = getDefaultGroup();
        const ch = dflt ? pickReadyChannelInGroup(dflt) : null;
        if (ch) {
          pick = { channel: ch, servedModel: dflt.model, fallback: true, fallbackReason: job.fallbackReason || 'unknown-model' };
        }
      } else {
        pick = tryPickForJob(job);
      }
      if (!pick) continue;
      requestQueue.splice(i, 1);
      clearJobTimers(job);
      routeRequest(job, pick);
      progress = true;
      break;
    }
  }
}

function routeRequest(job, pick) {
  const ch = pick.channel;
  rememberSessionAffinity(job.sessionKey, ch, pick.servedModel);
  ch.currentRequestId = job.requestId;
  ch.state = 'busy';
  ch.busyAt = Date.now();
  ch.lastActivityAt = Date.now();
  requestClient.set(job.requestId, job.client);
  writeToClient(job.client, {
    type: 'route_decision',
    requestId: job.requestId,
    channelId: ch.id,
    servedModel: pick.servedModel,
    requestedModel: job.requestedModel || job.model || null,
    fallback: !!pick.fallback,
    fallbackReason: pick.fallbackReason || null,
  });
  if (pick.fallback) {
    log(`req ${job.requestId}: routed action=${job.action} to ${ch.id} (group=${pick.servedModel}, FALLBACK from ${job.requestedModel || job.model}, reason=${pick.fallbackReason})`);
  } else if (job.routeModel || job.model) {
    log(`req ${job.requestId}: routed action=${job.action} to ${ch.id} (group=${pick.servedModel}${job.sessionKey ? `, sticky=${job.sessionKey.slice(0, 48)}` : ''}${job.contextMode ? `, ctx=${job.contextMode}` : ''}${job.hybridReason ? `, reason=${job.hybridReason}` : ''})`);
  }
  if (job.action === 'send_user_message' || job.action === 'send_native_image_message') {
    ch.proc.send({
      type: job.action,
      requestId: job.requestId,
      text: job.payload.text,
      content: job.payload.content,
      model: job.routeModel || job.model || null,
      requestedModel: job.requestedModel || job.model || null,
      system: job.payload.system || '',
      tools: job.payload.tools || [],
    });
  } else if (job.action === 'send_tool_result') {
    ch.proc.send({
      type: 'send_tool_result',
      requestId: job.requestId,
      execId: ch.pendingExecId,
      content: job.payload.content,
    });
  } else {
    writeToClient(job.client, { type: 'error', requestId: job.requestId, message: 'unknown action: ' + job.action });
    return;
  }
}

// ── Idle ping ────────────────────────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (const ch of channels.values()) {
    if (ch.state !== 'ready') continue;
    if (now - ch.lastActivityAt < IDLE_PING_MS) continue;
    const pingReqId = 'ping-' + randomUUID().slice(0, 8);
    log(`pinging idle ${ch.id} (group=${ch.group}, idle for ${Math.floor((now - ch.lastActivityAt) / 1000)}s)`);
    ch.currentRequestId = pingReqId;
    ch.state = 'busy';
    ch.busyAt = now;
    ch.lastActivityAt = now;
    const pingTimer = setTimeout(() => {
      log(`ping timeout on ${ch.id}; killing for respawn`);
      try { ch.proc.kill('SIGTERM'); } catch { /* ignore */ }
    }, PING_TIMEOUT_MS);
    ch._pingTimer = pingTimer;
    ch.proc.send({ type: 'ping', requestId: pingReqId });
  }
}, 30_000);

// ── Busy-watchdog ────────────────────────────────────────────────────────
// A channel that sits in `busy` state past this many ms with no activity
// (no text_delta / thinking_delta / tool_use / heartbeat — see worker
// `lastActivityAt` updates in bridge-worker.mjs) is considered stuck. We
// notify any waiting client with an error and SIGTERM the worker so the
// existing exit handler refills the slot via maybeSpawnNext.
//
// Why this exists: HTTP-layer stall detection in cursor-agent-h1.js and
// the worker's onTurnEnded handler cover most hang modes (Cursor silent
// mid-stream, model ends without yield), but a stream that keeps the
// connection alive without ever delivering a bajie_yield — or an IPC
// pipe that's silently stalled — would leave the channel busy forever.
// Idle-ping above only watches `ready` channels, so it can't recover
// this state.
//
// Default 240 s: well above any legitimate single-turn latency we've
// observed (600k-token NIAH inferences top out around 40 s plus
// streaming). Tune via RATLC_BUSY_STUCK_TIMEOUT_MS.
// Per-model-type (fast vs non-fast). RATLC_BUSY_STUCK_TIMEOUT_MS_{FAST,SLOW}
// override the global RATLC_BUSY_STUCK_TIMEOUT_MS.
const BUSY_STUCK = typeThresholds('BUSY_STUCK_TIMEOUT_MS', 240000, 0);
setInterval(() => {
  const now = Date.now();
  for (const ch of channels.values()) {
    if (ch.state !== 'busy') continue;
    const idleMs = now - (ch.lastActivityAt || 0);
    const threshold = isFastModel(ch.group) ? BUSY_STUCK.fast : BUSY_STUCK.slow;
    if (idleMs < threshold) continue;
    log(`busy-watchdog: ${ch.id} (group=${ch.group}) stuck busy ${Math.floor(idleMs / 1000)}s reqId=${ch.currentRequestId} — killing for respawn`);
    if (ch.currentRequestId) {
      const client = requestClient.get(ch.currentRequestId);
      if (client) {
        writeToClient(client, {
          type: 'error',
          requestId: ch.currentRequestId,
          message: `busy-watchdog timeout: channel ${ch.id} stuck busy ${Math.floor(idleMs / 1000)}s`,
        });
      }
      requestClient.delete(ch.currentRequestId);
    }
    try { ch.proc.kill('SIGTERM'); } catch { /* ignore */ }
  }
}, 30_000);

function writeToClient(client, obj) {
  if (!client || client.destroyed) return;
  try {
    client.write(JSON.stringify(obj) + '\n');
  } catch (e) {
    log('writeToClient failed:', e.message);
  }
}

function handleClientMessage(client, msg) {
  if (msg.type === 'request') {
    const { requestId, action, text, content, anthropic_tool_use_id, system, tools, results, model, requestedModel, sessionKey, contextMode, hybridReason } = msg;

    if (action === 'send_user_message' || action === 'send_native_image_message') {
      if (POOL_TOOL_MODE === 'contract') {
        const incomingTools = tools || [];
        const isEmptyToolsProbe = incomingTools.length === 0;
        if (!isEmptyToolsProbe) {
          if (poolTools === null) {
            setPoolContract(system, incomingTools);
            log(`pool contract set: tools=${incomingTools.length} (${(incomingTools.map(t=>t.name).join(',')).slice(0, 80)})`);
            for (const ch of channels.values()) {
              if (ch.state === 'spawning') {
                ch.proc.send({ type: 'open', model: ch.group, tools: poolTools, system: poolSystem });
              }
            }
          } else if (poolNeedsReopen(incomingTools)) {
            log(`tools mismatch — recycling pool (have=[${toolsSignature.slice(0, 60)}] want=[${signatureOf(incomingTools).slice(0, 60)}])`);
            setPoolContract(system, incomingTools);
            reopenAllChannels();
            writeToClient(client, {
              type: 'error', requestId,
              message: 'pool recycling for new tools contract — retry in 30-180s',
            });
            return;
          }
        }
      }

      const job = {
        requestId, action,
        payload: { text, content: content || null, system: system || '', tools: tools || [] },
        client,
        model: model || null,
        routeModel: normalizeModelForRouting(model),
        requestedModel: requestedModel || model || null,
        sessionKey: sessionKey || null,
        contextMode: contextMode || null,
        hybridReason: hybridReason || null,
        queuedAt: Date.now(),
        waitTimer: null,
        queueTimer: null,
        fallbackArmed: false,
        fallbackReason: null,
      };
      requestQueue.push(job);
      armFallbackTimer(job);
      armQueueTimeoutTimer(job);
      drainQueue();
      const stillQueued = requestQueue.includes(job);
      if (stillQueued) {
        log(`no ready channel for req=${requestId} model=${requestedModel || model || '(default)'} routeModel=${normalizeModelForRouting(model) || '(default)'} — queued (queue depth ${requestQueue.length})`);
      }
      return;
    }

    if (action === 'send_tool_result') {
      const entry = toolUseIndex.get(anthropic_tool_use_id);
      log(`route send_tool_result requestId=${requestId} anthropic_tool_use_id=${anthropic_tool_use_id} found=${!!entry} indexSize=${toolUseIndex.size}`);
      if (!entry) {
        const consumed = getConsumedToolUse(anthropic_tool_use_id);
        if (consumed) {
          log(`  ↪ duplicate consumed anthropic_tool_use_id=${anthropic_tool_use_id} consumedAgoMs=${Date.now() - consumed.consumedAt}`);
          writeToClient(client, {
            type: 'error',
            requestId,
            message: `already consumed anthropic_tool_use_id: ${anthropic_tool_use_id}`,
          });
          return;
        }
        log(`  ❌ unknown anthropic_tool_use_id — known ids: [${[...toolUseIndex.keys()].slice(0, 5).join(', ')}${toolUseIndex.size > 5 ? '…' : ''}]`);
        writeToClient(client, { type: 'error', requestId, message: `unknown anthropic_tool_use_id: ${anthropic_tool_use_id}` });
        return;
      }
      const channelId = entry.channelId;
      const providedById = mergeHeldToolResults(channelId, requestId, new Map([
        [anthropic_tool_use_id, { entry, content }],
      ]));
      const { pendingIds, pending, missing } = buildResolvedToolResults(channelId, providedById);
      if (missing.length > 0) {
        holdPartialToolResults({ client, requestId, model, channelId, providedById, pendingIds, missing });
        return;
      }
      routeCompleteToolResults({
        client, requestId, model, channelId,
        resolved: pending.map((r) => ({
          anthropic_tool_use_id: r.anthropic_tool_use_id,
          execId: r.execId,
          toolUseKey: r.toolUseKey,
          content: r.content,
        })),
      });
      return;
    }

    if (action === 'send_tool_results') {
      const rs = Array.isArray(results) ? results : [];
      if (rs.length === 0) {
        writeToClient(client, { type: 'error', requestId, message: 'send_tool_results: empty results array' });
        return;
      }
      log(`route send_tool_results requestId=${requestId} count=${rs.length} ids=[${rs.map(r => r.anthropic_tool_use_id).join(', ')}] indexSize=${toolUseIndex.size}`);
      let providedById = new Map();
      let channelId = null;
      for (const r of rs) {
        const entry = toolUseIndex.get(r.anthropic_tool_use_id);
        if (!entry) {
          const consumed = getConsumedToolUse(r.anthropic_tool_use_id);
          if (consumed) {
            log(`  ↪ duplicate consumed anthropic_tool_use_id=${r.anthropic_tool_use_id} consumedAgoMs=${Date.now() - consumed.consumedAt}`);
            writeToClient(client, {
              type: 'error', requestId,
              message: `already consumed anthropic_tool_use_id: ${r.anthropic_tool_use_id}`,
            });
            return;
          }
          log(`  ❌ unknown anthropic_tool_use_id=${r.anthropic_tool_use_id} — known ids: [${[...toolUseIndex.keys()].slice(0, 5).join(', ')}${toolUseIndex.size > 5 ? '…' : ''}]`);
          writeToClient(client, { type: 'error', requestId, message: `unknown anthropic_tool_use_id: ${r.anthropic_tool_use_id}` });
          return;
        }
        if (channelId === null) channelId = entry.channelId;
        else if (entry.channelId !== channelId) {
          log(`  ❌ tool_use_ids span multiple channels: ${channelId} vs ${entry.channelId} (impossible by construction)`);
          writeToClient(client, {
            type: 'error', requestId,
            message: `tool_use_ids span multiple channels (${channelId} vs ${entry.channelId}) — possibly stale conversation`,
          });
          return;
        }
        providedById.set(r.anthropic_tool_use_id, { entry, content: r.content });
      }
      providedById = mergeHeldToolResults(channelId, requestId, providedById);
      const { pendingIds, pending, missing } = buildResolvedToolResults(channelId, providedById);
      if (missing.length > 0) {
        holdPartialToolResults({ client, requestId, model, channelId, providedById, pendingIds, missing });
        return;
      }
      routeCompleteToolResults({
        client, requestId, model, channelId,
        resolved: pending.map((r) => ({
          anthropic_tool_use_id: r.anthropic_tool_use_id,
          execId: r.execId,
          toolUseKey: r.toolUseKey,
          content: r.content,
        })),
      });
      return;
    }

    writeToClient(client, { type: 'error', requestId, message: 'unknown action: ' + action });
    return;
  }

	  if (msg.type === 'status') {
	    writeToClient(client, statusSnapshot());
	    return;
	  }

	  if (msg.type === 'cancel_request') {
	    cancelRequest(msg.requestId, msg.reason || 'cancel_request');
	    return;
	  }

	  // Used by api-server's send_tool_results retry path. After a
	  // cancel_request, the channel that held the in-flight tool_use_ids
	  // got killed and its toolUseIndex entries are gone — but the ids
	  // remain in consumedToolUseIndex (the 30-min dedup cache). Without
	  // release, a retry that re-delivers the same tool_use_id would hit
	  // "already consumed". Release-then-replay is safe because cancel
	  // already killed the channel before it could produce a real result.
	  if (msg.type === 'release_consumed_ids') {
	    const ids = Array.isArray(msg.ids) ? msg.ids : [];
	    let released = 0;
	    for (const id of ids) {
	      if (consumedToolUseIndex.has(id)) {
	        consumedToolUseIndex.delete(id);
	        released++;
	      }
	    }
	    log(`release_consumed_ids: released=${released}/${ids.length} reason=${msg.reason || '(none)'}`);
	    return;
	  }

	  if (msg.type === 'list_groups') {
    writeToClient(client, { type: 'groups', groups: groupsSnapshot() });
    return;
  }

  if (msg.type === 'add_group') {
    const m = String(msg.model || '').trim();
    const size = Math.max(0, parseInt(msg.size || 0, 10));
    if (!m) {
      writeToClient(client, { type: 'error', message: 'add_group: model required' });
      return;
    }
    if (groups.has(m)) {
      const g = groups.get(m);
      if (g.draining) {
        writeToClient(client, { type: 'error', message: `group ${m} is draining; wait for it to disappear first` });
        return;
      }
      g.targetSize = size;
      log(`add_group: existing group ${m} resized to ${size}`);
      writeToClient(client, { type: 'ack', message: `group ${m} resized to ${size}` });
      setImmediate(maybeSpawnNext);
      return;
    }
    groups.set(m, makeGroup(m, size, false));
    log(`add_group: ${m} targetSize=${size}`);
    writeToClient(client, { type: 'ack', message: `group ${m} added (target=${size})` });
    setImmediate(maybeSpawnNext);
    return;
  }

  if (msg.type === 'remove_group') {
    const m = String(msg.model || '').trim();
    if (!m) {
      writeToClient(client, { type: 'error', message: 'remove_group: model required' });
      return;
    }
    const g = groups.get(m);
    if (!g) {
      writeToClient(client, { type: 'error', message: `group ${m} not found` });
      return;
    }
    if (g.isDefault) {
      writeToClient(client, { type: 'error', message: 'cannot remove default group' });
      return;
    }
    g.draining = true;
    g.targetSize = 0;
    log(`remove_group: ${m} marked for draining (${g.channels.size} channels)`);
    for (const cid of g.channels) {
      const ch = channels.get(cid);
      if (!ch) continue;
      if (ch.state === 'ready' || ch.state === 'spawning' || ch.state === 'opening') {
        try { ch.proc.send({ type: 'shutdown' }); } catch { /* ignore */ }
      }
    }
    writeToClient(client, { type: 'ack', message: `group ${m} draining (${g.channels.size} channels to evict)` });
    return;
  }

  if (msg.type === 'ramp_up') {
    const n = Math.max(1, parseInt(msg.count || 1, 10));
    const targetModel = (msg.group && String(msg.group).trim()) || POOL_MODEL;
    const g = groups.get(targetModel);
    if (!g) {
      writeToClient(client, { type: 'error', message: `ramp_up: unknown group ${targetModel} (use add_group first)` });
      return;
    }
    if (g.draining) {
      writeToClient(client, { type: 'error', message: `ramp_up: group ${targetModel} is draining` });
      return;
    }
    g.targetSize += n;
    log(`ramp_up by ${n} on group ${targetModel} → target=${g.targetSize}`);
    setImmediate(maybeSpawnNext);
    writeToClient(client, { type: 'ack', message: `ramping up ${n} on ${targetModel} (target=${g.targetSize}, sequential)` });
    return;
  }

  if (msg.type === 'ramp_down') {
    const n = Math.max(1, parseInt(msg.count || 1, 10));
    const targetModel = (msg.group && String(msg.group).trim()) || POOL_MODEL;
    const g = groups.get(targetModel);
    if (!g) {
      writeToClient(client, { type: 'error', message: `ramp_down: unknown group ${targetModel}` });
      return;
    }
    g.targetSize = Math.max(0, g.targetSize - n);
    log(`ramp_down by ${n} on group ${targetModel} → target=${g.targetSize}`);
    const inGroup = Array.from(g.channels)
      .map((cid) => channels.get(cid))
      .filter(Boolean)
      .sort((a, b) => (a.state === 'ready' ? 0 : 1) - (b.state === 'ready' ? 0 : 1))
      .slice(0, n);
    for (const ch of inGroup) {
      try { ch.proc.send({ type: 'shutdown' }); } catch { /* ignore */ }
    }
    writeToClient(client, { type: 'ack', message: `ramping down ${inGroup.length} on ${targetModel} (target=${g.targetSize})` });
    return;
  }

  if (msg.type === 'restart_channel') {
    const ch = channels.get(msg.channelId);
    if (!ch) {
      writeToClient(client, { type: 'error', message: `channel not found: ${msg.channelId}` });
      return;
    }
    try { ch.proc.kill('SIGTERM'); } catch { /* ignore */ }
    writeToClient(client, { type: 'ack', message: `killing ${msg.channelId} for respawn` });
    return;
  }

  if (msg.type === 'shutdown') {
    log('shutdown requested');
    for (const ch of channels.values()) {
      try { ch.proc.send({ type: 'shutdown' }); } catch { /* ignore */ }
    }
    setTimeout(() => process.exit(0), 1500);
    writeToClient(client, { type: 'ack', message: 'shutting down' });
    return;
  }

  writeToClient(client, { type: 'error', message: 'unknown command: ' + msg.type });
}

function groupsSnapshot() {
  const out = [];
  for (const g of groups.values()) {
    let ready = 0, busy = 0, opening = 0, dead = 0, rounds = 0;
    for (const cid of g.channels) {
      const ch = channels.get(cid);
      if (!ch) continue;
      if (ch.state === 'ready') ready++;
      else if (ch.state === 'busy') busy++;
      else if (ch.state === 'opening' || ch.state === 'spawning') opening++;
      else if (ch.state === 'dead') dead++;
      rounds += ch.roundsServed || 0;
    }
    out.push({
      model: g.model,
      isDefault: !!g.isDefault,
      draining: !!g.draining,
      target: g.targetSize,
      actual: g.channels.size,
      ready, busy, opening, dead,
      rounds,
    });
  }
  out.sort((a, b) => (b.isDefault ? 1 : 0) - (a.isDefault ? 1 : 0) || a.model.localeCompare(b.model));
  return out;
}

function statusSnapshot() {
  const list = [];
  let readyCount = 0, busyCount = 0, openingCount = 0, deadCount = 0;
  const now = Date.now();
  for (const ch of channels.values()) {
    list.push({
      id: ch.id,
      pid: ch.pid,
      group: ch.group,
      tokenIdx: ch.tokenIdx ?? 0,
      tokenName: ch.tokenName ?? '(default)',
      state: ch.state,
      openAttempts: ch.openAttempts,
      openedAt: ch.openedAt,
      openedAgoMs: ch.openedAt ? now - ch.openedAt : null,
      lastActivityAt: ch.lastActivityAt,
      idleMs: ch.lastActivityAt ? now - ch.lastActivityAt : null,
      busyAt: ch.busyAt,
      busyForMs: ch.state === 'busy' && ch.busyAt ? now - ch.busyAt : null,
      lastProgressAt: ch.lastProgressAt,
      progressGapMs: ch.lastProgressAt ? now - ch.lastProgressAt : null,
      roundsServed: ch.roundsServed,
      currentRequestId: ch.currentRequestId,
      pendingToolUseIds: getPendingToolUseIdsForChannel(ch.id),
      error: ch.error,
    });
    if (ch.state === 'ready') readyCount++;
    else if (ch.state === 'busy') busyCount++;
    else if (ch.state === 'opening' || ch.state === 'spawning') openingCount++;
    else if (ch.state === 'dead') deadCount++;
  }
  list.sort((a, b) => {
    if (a.group !== b.group) return a.group.localeCompare(b.group);
    return a.id.localeCompare(b.id);
  });
  let configuredSize = 0;
  for (const g of groups.values()) configuredSize += g.targetSize;
  const tokens = [];
  for (let i = 0; i < _tokenCount; i++) {
    tokens.push({
      idx: i,
      name: _tokenNames[i],
      validated: _tokenValidated[i],
      dead: _tokenDead[i],
      otherErrorCount: _tokenOtherErrors[i],
      lastError: _tokenLastError[i],
    });
  }
  return {
    type: 'status',
    pool: {
      configuredSize,
      actualSize: channels.size,
      channels: list,
      groups: groupsSnapshot(),
      tokens,
      defaultGroup: POOL_MODEL,
      readyCount, busyCount, openingCount, deadCount,
      pendingRequests: requestQueue.length,
      toolUseIndex: toolUseIndex.size,
      pendingToolUseChannels: pendingToolUseIdsByChannel.size,
      consumedToolUseIndex: consumedToolUseIndex.size,
      sessionAffinity: sessionAffinity.size,
    },
    config: {
      model: POOL_MODEL,
      toolMode: POOL_TOOL_MODE,
      bridgeProtocol: POOL_BRIDGE_PROTOCOL,
      contextMode: POOL_CONTEXT_MODE,
      reinjectThinking: POOL_REINJECT_THINKING ? 1 : 0,
      concurrentOpens: POOL_CONCURRENT_OPENS,
      groupWaitMs: POOL_GROUP_WAIT_MS,
      queueTimeoutMs: RATLC_QUEUE_TIMEOUT_MS,
      consumedToolTtlMs: RATLC_CONSUMED_TOOL_TTL_MS,
      sessionTtlMs: RATLC_SESSION_TTL_MS,
      idlePingMs: IDLE_PING_MS,
      pingTimeoutMs: PING_TIMEOUT_MS,
      poolToolsContractCount: poolTools ? poolTools.length : null,
      poolToolsSignature: toolsSignature.slice(0, 80),
      // Watchdog thresholds (same env the api-server reads) so the TUI can render
      // a silence countdown against them. livenessGapMs is the silent-timeout the
      // SILENT column counts toward; busyStuckMs is the pool-side reap backstop.
      watchdog: (() => {
        const gap = typeThresholds('NO_VISIBLE_LIVENESS_GRACE_MS', 0, 0);
        const ceil = typeThresholds('NO_VISIBLE_EVENT_TIMEOUT_MS', 25000, 5000);
        const busy = typeThresholds('BUSY_STUCK_TIMEOUT_MS', 240000, 0);
        return {
          // Flat values = the "slow"/global tier (back-compat for any reader that
          // ignores model type; equals the global when no _FAST/_SLOW are set).
          livenessGapMs: gap.slow, ceilingMs: ceil.slow, busyStuckMs: busy.slow,
          // Per-model-type, so the TUI SILENT countdown uses the right threshold
          // for each channel (fast channels can have a tighter silent-timeout).
          fast: { livenessGapMs: gap.fast, ceilingMs: ceil.fast, busyStuckMs: busy.fast },
          slow: { livenessGapMs: gap.slow, ceilingMs: ceil.slow, busyStuckMs: busy.slow },
          // When the api-server runs RATLC_ADAPTIVE_TIMEOUTS=1 the silent-timeout
          // gap is derived live (regime p99 × margin) and changes per request, so
          // a fixed countdown denominator would mislead — the TUI shows the gap as
          // dynamic (`~`) instead. Same env both processes read under launch.sh.
          adaptive: process.env.RATLC_ADAPTIVE_TIMEOUTS === '1',
        };
      })(),
    },
  };
}

// ── Listen on Unix socket ────────────────────────────────────────────────
try { fs.unlinkSync(POOL_SOCK); } catch { /* ignore */ }
const server = net.createServer((socket) => {
  let buf = '';
  socket.on('data', (chunk) => {
    buf += chunk.toString('utf8');
    let idx;
    while ((idx = buf.indexOf('\n')) !== -1) {
      const line = buf.slice(0, idx);
      buf = buf.slice(idx + 1);
      if (!line.trim()) continue;
      try {
        const msg = JSON.parse(line);
        handleClientMessage(socket, msg);
      } catch (e) {
        writeToClient(socket, { type: 'error', message: 'bad json: ' + e.message });
      }
    }
  });
  socket.on('error', () => {});
	  socket.on('close', () => {
	    for (const [reqId, c] of requestClient.entries()) {
	      if (c !== socket) continue;
	      const ch = Array.from(channels.values()).find((x) => x.currentRequestId === reqId);
	      if (ch && getPendingToolUseIdsForChannel(ch.id).length > 0) {
	        requestClient.delete(reqId);
	      } else {
	        cancelRequest(reqId, 'client_socket_closed');
	      }
	    }
	  });
});
server.listen(POOL_SOCK, () => {
  const groupSummary = Array.from(groups.values()).map((g) => `${g.model}:${g.targetSize}${g.isDefault ? '(default)' : ''}`).join(', ');
  log(`listening on ${POOL_SOCK}, groups=[${groupSummary}], protocol=${POOL_BRIDGE_PROTOCOL}, contextMode=${POOL_CONTEXT_MODE}, reinjectThinking=${POOL_REINJECT_THINKING ? 1 : 0}, groupWaitMs=${POOL_GROUP_WAIT_MS}`);
});

// ── Bootstrap groups from env ────────────────────────────────────────────
// Default group: first entry of POOL_MODEL_RAW (CSV form) or the legacy
// single-value POOL_MODEL. Sized by the entry's explicit size if given,
// else POOL_SIZE.
const defaultGroup = makeGroup(POOL_MODEL, POOL_MODEL_DEFAULT_SIZE, true);
groups.set(POOL_MODEL, defaultGroup);

// CSV-form POOL_MODEL trailing entries — merged the same way as POOL_GROUPS.
// Entries with no explicit size default to POOL_SIZE (mirrors the default-
// group fallback).
for (let i = 1; i < poolModelEntries.length; i++) {
  const e = poolModelEntries[i];
  const size = e.size != null ? e.size : POOL_SIZE;
  if (e.model === POOL_MODEL) {
    defaultGroup.targetSize += size;
    continue;
  }
  if (groups.has(e.model)) {
    groups.get(e.model).targetSize += size;
    continue;
  }
  groups.set(e.model, makeGroup(e.model, size, false));
}

for (const entry of parsePoolGroupsEnv()) {
  if (entry.foldIntoDefault) {
    defaultGroup.targetSize += entry.size;
    continue;
  }
  if (groups.has(entry.model)) {
    const existing = groups.get(entry.model);
    existing.targetSize += entry.size;
    continue;
  }
  groups.set(entry.model, makeGroup(entry.model, entry.size, false));
}

let totalTarget = 0;
for (const g of groups.values()) totalTarget += g.targetSize;
log(`bringing up initial pool: ${groups.size} group(s), total target=${totalTarget}, up to ${POOL_CONCURRENT_OPENS} concurrent opens`);
maybeSpawnNext();

// ── Shutdown ─────────────────────────────────────────────────────────────
function shutdown(signal) {
  log(`received ${signal}, shutting down`);
  for (const ch of channels.values()) {
    try { ch.proc.send({ type: 'shutdown' }); } catch { /* ignore */ }
    setTimeout(() => { try { ch.proc.kill('SIGKILL'); } catch { /* ignore */ } }, 2000);
  }
  try { server.close(); } catch { /* ignore */ }
  try { fs.unlinkSync(POOL_SOCK); } catch { /* ignore */ }
  setTimeout(() => process.exit(0), 3000);
}
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
