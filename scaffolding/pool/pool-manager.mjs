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
if (!['full', 'last'].includes(POOL_CONTEXT_MODE)) {
  console.error(`invalid POOL_CONTEXT_MODE=${POOL_CONTEXT_MODE} (must be full|last)`);
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
const requestClient = new Map();

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

    case 'text_delta':
    case 'thinking_delta':
    case 'server_tool_use':
    case 'tool_use':
    case 'yield':
    case 'step_completed':
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
    toolUseIndex.set(anthropic_id, { channelId: ch.id, execId: msg.execId });
    ch.pendingExecId = msg.execId;
    ch.pendingAnthropicId = anthropic_id;
    writeToClient(client, {
      type: 'tool_use',
      requestId: reqId,
      anthropic_id,
      name: msg.name,
      args: msg.args,
    });
    return;
  }

  if (msg.type === 'yield') {
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

function tryPickForJob(job) {
  const dflt = getDefaultGroup();
  if (!dflt) return null;
  const requestedModel = job.routeModel || job.model;
  if (requestedModel) {
    const g = groups.get(requestedModel);
    if (g && !g.draining) {
      const ch = pickReadyChannelInGroup(g);
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
  const ch = pickReadyChannelInGroup(dflt);
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

function clearJobTimers(job) {
  if (job.waitTimer) { clearTimeout(job.waitTimer); job.waitTimer = null; }
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
    log(`req ${job.requestId}: routed to ${ch.id} (group=${pick.servedModel}, FALLBACK from ${job.requestedModel || job.model}, reason=${pick.fallbackReason})`);
  } else if (job.routeModel || job.model) {
    log(`req ${job.requestId}: routed to ${ch.id} (group=${pick.servedModel})`);
  }
  if (job.action === 'send_user_message') {
    ch.proc.send({
      type: 'send_user_message',
      requestId: job.requestId,
      text: job.payload.text,
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
const BUSY_STUCK_TIMEOUT_MS = parseInt(process.env.RATLC_BUSY_STUCK_TIMEOUT_MS || '240000', 10);
setInterval(() => {
  const now = Date.now();
  for (const ch of channels.values()) {
    if (ch.state !== 'busy') continue;
    const idleMs = now - (ch.lastActivityAt || 0);
    if (idleMs < BUSY_STUCK_TIMEOUT_MS) continue;
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
    const { requestId, action, text, content, anthropic_tool_use_id, system, tools, results, model, requestedModel } = msg;

    if (action === 'send_user_message') {
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
        payload: { text },
        client,
        model: model || null,
        routeModel: normalizeModelForRouting(model),
        requestedModel: requestedModel || model || null,
        queuedAt: Date.now(),
        waitTimer: null,
        fallbackArmed: false,
        fallbackReason: null,
      };
      requestQueue.push(job);
      armFallbackTimer(job);
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
        log(`  ❌ unknown anthropic_tool_use_id — known ids: [${[...toolUseIndex.keys()].slice(0, 5).join(', ')}${toolUseIndex.size > 5 ? '…' : ''}]`);
        writeToClient(client, { type: 'error', requestId, message: `unknown anthropic_tool_use_id: ${anthropic_tool_use_id}` });
        return;
      }
      toolUseIndex.delete(anthropic_tool_use_id);
      const ch = channels.get(entry.channelId);
      if (!ch || ch.state === 'dead') {
        log(`  ❌ channel ${entry.channelId} no longer alive (state=${ch?.state})`);
        writeToClient(client, { type: 'error', requestId, message: `channel ${entry.channelId} no longer alive` });
        return;
      }
      log(`  ✅ routing to ${entry.channelId} (group=${ch.group}) execId=${entry.execId} (state was ${ch.state})`);
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
      ch.proc.send({ type: 'send_tool_result', requestId, execId: entry.execId, content });
      return;
    }

    if (action === 'send_tool_results') {
      const rs = Array.isArray(results) ? results : [];
      if (rs.length === 0) {
        writeToClient(client, { type: 'error', requestId, message: 'send_tool_results: empty results array' });
        return;
      }
      log(`route send_tool_results requestId=${requestId} count=${rs.length} ids=[${rs.map(r => r.anthropic_tool_use_id).join(', ')}] indexSize=${toolUseIndex.size}`);
      const resolved = [];
      let channelId = null;
      for (const r of rs) {
        const entry = toolUseIndex.get(r.anthropic_tool_use_id);
        if (!entry) {
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
        resolved.push({ anthropic_tool_use_id: r.anthropic_tool_use_id, execId: entry.execId, content: r.content });
      }
      const ch = channels.get(channelId);
      if (!ch || ch.state === 'dead') {
        log(`  ❌ channel ${channelId} no longer alive (state=${ch?.state})`);
        writeToClient(client, { type: 'error', requestId, message: `channel ${channelId} no longer alive` });
        return;
      }
      for (const r of resolved) toolUseIndex.delete(r.anthropic_tool_use_id);
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
        results: resolved.map((r) => ({ execId: r.execId, content: r.content })),
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
      roundsServed: ch.roundsServed,
      currentRequestId: ch.currentRequestId,
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
    },
    config: {
      model: POOL_MODEL,
      toolMode: POOL_TOOL_MODE,
      bridgeProtocol: POOL_BRIDGE_PROTOCOL,
      contextMode: POOL_CONTEXT_MODE,
      reinjectThinking: POOL_REINJECT_THINKING ? 1 : 0,
      concurrentOpens: POOL_CONCURRENT_OPENS,
      groupWaitMs: POOL_GROUP_WAIT_MS,
      idlePingMs: IDLE_PING_MS,
      pingTimeoutMs: PING_TIMEOUT_MS,
      poolToolsContractCount: poolTools ? poolTools.length : null,
      poolToolsSignature: toolsSignature.slice(0, 80),
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
      if (c === socket) requestClient.delete(reqId);
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
