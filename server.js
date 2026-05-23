// ═══════════════════════════════════════════════════
//  CursorIDE2API v2 - 极简版
//  token.json → 反代 Cursor API → OpenAI 兼容接口
// ═══════════════════════════════════════════════════

const express = require('express');
const cors = require('cors');
const fs = require('fs');
const crypto = require('crypto');
const http = require('http');
const https = require('https');
const path = require('path');
const { Writable } = require('stream');
const { URL } = require('url');
const { v4: uuidv4 } = require('./src/uuid');
const converter = require('./src/converter');
const anthropicConverter = require('./src/anthropic-converter');
const config = require('./src/config');
const CURSOR_DOWNSTREAM_PROTOCOL = String(
  process.env.CURSOR_DOWNSTREAM_PROTOCOL || process.env.EXTERNAL_HTTP_PROTOCOL || 'http/1.1'
).toLowerCase();
const CURSOR_UPSTREAM_TRANSPORT = String(process.env.CURSOR_UPSTREAM_TRANSPORT || 'h2').toLowerCase() === 'h1' ? 'h1' : 'h2';
const cursorAgent = CURSOR_UPSTREAM_TRANSPORT === 'h1'
  ? require('./src/cursor-agent-h1')
  : require('./src/cursor-agent');
const thinkingHistory = require('./src/thinking-history');
const anthropicTools = require('./src/anthropic-tools');
const preprocess = require('./src/preprocess');
const debugLog = require('./src/debug-log');
const stallThresholds = require('./src/stall-thresholds');
const runtimeStats = require('./src/runtime-stats');
const { StreamingHallucinationFilter } = require('./src/streaming-hallucination-filter');
const { getCursorToolMatrix } = require('./src/cursor-tool-matrix');
const { ProxyThinkingBlockAdapter } = require('./src/proxy-thinking-adapter');
const agentToolsStore = require('./src/agent-tools-virtual-store');
const serverLocalTools = require('./src/server-local-tool-adapter');

const PUBLIC_DIR = path.join(__dirname, 'public');

// Proxy-local thinking-block emission. Off by default: Cursor AgentService
// exposes only plaintext thinkingDelta frames, not Anthropic signed thinking
// blocks. When enabled, we wrap those real Cursor deltas in Anthropic-shaped
// thinking blocks with a clearly proxy-local signature so Claude Code can show
// the thinking UI. These blocks are for this proxy only; they are not portable
// to direct Anthropic sessions.
const PROXY_THINKING_BLOCKS = process.env.CURSOR_PROXY_THINKING_BLOCKS === '1';
debugLog.init();

function looksLikeAgentToolPlaceholderWrite(toolName, args) {
  const normalizedTool = anthropicTools.normalizeClientToolNameForPolicy(toolName);
  if (normalizedTool !== 'write') return false;
  const a = args && typeof args === 'object' ? args : {};
  const p = agentToolsStore.getPathFromArgs(a);
  const c = String(a.content ?? a.file_text ?? a.text ?? a.body ?? a.data ?? '').trim();
  return agentToolsStore.isAgentToolsArtifactPath(p) && (c === '' || c === '(No content)');
}

// Configurable "small model" used for warmup pings, compaction summarization,
// and (optionally) subagent traffic — costs much less than a full Sonnet/Opus
// turn. Default to the smallest real Claude on Cursor.
const SMALL_MODEL = process.env.SMALL_MODEL || 'claude-sonnet-4-6';
// Whether to also downgrade subagent traffic to SMALL_MODEL. Off by default
// (subagents may legitimately need full reasoning capability).
const SUBAGENT_USE_SMALL_MODEL = /^1|true|yes$/i.test(process.env.SUBAGENT_USE_SMALL_MODEL || '');

// ── 配置 ──
const PORT = parseInt(process.env.PORT || '3000');
const HOST = process.env.HOST || '0.0.0.0';
const API_KEY = process.env.API_KEY || '';  // 留空 = 不校验
const TOKEN_FILE = process.env.TOKEN_FILE || path.join(__dirname, 'token.json');
const DEFAULT_MODEL = process.env.DEFAULT_MODEL || 'claude-4.5-sonnet';
const CLIENT_VERSION = process.env.CURSOR_CLIENT_VERSION || '2.6.20';
const PASSTHROUGH_NATIVE_TOOLS = /^(1|true|yes)$/i.test(process.env.CURSOR_PASSTHROUGH_NATIVE_TOOLS || '');
const CURSOR_UPSTREAM_H1_ONLY = CURSOR_UPSTREAM_TRANSPORT === 'h1';
const RATLC_POOL_URL = String(process.env.CURSOR_RATLC_POOL_URL || process.env.RATLC_POOL_URL || '').replace(/\/+$/, '');
const RATLC_ROUTE_MESSAGES = /^(1|true|yes)$/i.test(process.env.CURSOR_RATLC_ROUTE_MESSAGES || '');
const RATLC_ROUTE_COUNT_TOKENS = /^(1|true|yes)$/i.test(process.env.CURSOR_RATLC_ROUTE_COUNT_TOKENS || process.env.CURSOR_RATLC_ROUTE_MESSAGES || '');
const RATLC_FALLBACK_TO_DIRECT = /^(1|true|yes)$/i.test(process.env.CURSOR_RATLC_FALLBACK_TO_DIRECT || '');
const RATLC_TIMEOUT_MS = parseInt(process.env.CURSOR_RATLC_TIMEOUT_MS || '600000', 10);
const RATLC_MODELS = String(process.env.CURSOR_RATLC_MODELS || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);
const RATLC_MODEL_SET = new Set(RATLC_MODELS.map(s => s.toLowerCase()));
const RATLC_ENABLED = !!(RATLC_POOL_URL && RATLC_MODEL_SET.size > 0);
let _cursorClient = null;
function getCursorClient() {
  if (!_cursorClient) _cursorClient = require('./src/cursor-client');
  return _cursorClient;
}

// ── Bridge / conversation caches (Anthropic tool-use flow) ──
//
// activeBridges:    bridgeKey  -> { bridge, lastAccessMs, mcpTools, pendingExecs, convKey, conversationId, sessionId, requestedModel, cursorModel, clientThinkingEnabled }
//                   pendingExecs: [{ execMsgId, execId, toolCallId, toolName, args, anthropicToolUseId, blockIndex }]
// bridgesBySessionId: sessionId -> same entry as above (alternate index — same
//                   object, two pointers). Used to find the bridge across TCP
//                   socket changes: the client's tool_use_id encodes the
//                   sessionId, which stays stable through keepAliveTimeout
//                   reconnects on a new remotePort.
// conversationStates: convKey  -> { conversationId, state: Buffer, lastAccessMs }
const activeBridges = new Map();
const bridgesBySessionId = new Map();
const conversationStates = new Map();
const responsesStore = new Map();

const CONVERSATION_TTL_MS = 30 * 60 * 1000; // 30 min

// Tear an entry down completely: close the bridge, drop every bridgeKey
// alias the entry was ever stored under, and drop the sessionId index. We
// track aliases on the entry itself (entry.bridgeKeys, a Set) because a
// continuation that arrives on a new TCP socket re-caches under a different
// bridgeKey while reusing the same entry; without alias tracking the old
// key would dangle and a future eviction pass could double-close the bridge.
function dropBridgeEntry(entry, reason) {
  if (!entry || entry._dropped) return;
  entry._dropped = true;
  try { entry.bridge && entry.bridge.close(); } catch { /* ignore */ }
  if (entry.bridgeKeys) {
    for (const bk of entry.bridgeKeys) {
      // Only delete if the map still points at THIS entry — a different
      // entry may have legitimately taken the same bridgeKey since.
      if (activeBridges.get(bk) === entry) activeBridges.delete(bk);
    }
  }
  if (entry.sessionId && bridgesBySessionId.get(entry.sessionId) === entry) {
    bridgesBySessionId.delete(entry.sessionId);
  }
  if (process.env.CURSOR_AGENT_DEBUG && reason) {
    console.log(`  🧹 dropped bridge entry sessionId=${(entry.sessionId || '').slice(0, 8)} reason=${reason}`);
  }
}

// Insert/refresh a bridge entry under a given bridgeKey. Maintains the
// entry.bridgeKeys alias set so dropBridgeEntry can clean up everything.
function indexBridgeEntry(entry, bridgeKey) {
  if (!entry.bridgeKeys) entry.bridgeKeys = new Set();
  entry.bridgeKeys.add(bridgeKey);
  activeBridges.set(bridgeKey, entry);
  if (entry.sessionId) bridgesBySessionId.set(entry.sessionId, entry);
}

function evictStale() {
  const now = Date.now();
  for (const [k, v] of conversationStates) {
    if (now - v.lastAccessMs > CONVERSATION_TTL_MS) conversationStates.delete(k);
  }
  for (const [k, v] of responsesStore) {
    if (now - v.lastAccessMs > CONVERSATION_TTL_MS) responsesStore.delete(k);
  }
  // Collect victims first; drop them outside the iteration so we never
  // mutate activeBridges while iterating.
  const victims = new Set();
  for (const [, v] of activeBridges) {
    if (now - v.lastAccessMs > CONVERSATION_TTL_MS) victims.add(v);
  }
  for (const v of victims) dropBridgeEntry(v, 'ttl');
}
setInterval(evictStale, 60 * 1000).unref();

// ── Token pool ──
//
// Health-aware replacement for the old round-robin picker. State per token:
//
//   { accessToken, machineId, macMachineId,
//     activeRequests, lastUsed, rateLimitUntil,
//     totalRequests, totalErrors, totalRateLimits,
//     released /* internal — set by pick(), cleared by release() */ }
//
// pick() prefers tokens with fewer in-flight requests, breaking ties by
// least-recently-used. Tokens that recently 429'd are parked until their
// rateLimitUntil expires (default 60 s, override via TOKEN_RATE_LIMIT_PARK_MS).
// If every token is parked, we still hand out the one recovering soonest —
// the upstream call will likely fail again, but blocking unconditionally would
// hide the situation from the caller.
//
// release() is idempotent: each "ticket" returned by pick() carries a unique
// `released` flag, so duplicate calls (e.g. both a try/finally and an
// res.on('close') hook) don't double-decrement activeRequests.
const TOKEN_RATE_LIMIT_PARK_MS = parseInt(process.env.TOKEN_RATE_LIMIT_PARK_MS || '60000');

class TokenPool {
  constructor(rawTokens = []) {
    this._tokens = [];
    this.replace(rawTokens);
  }

  replace(rawTokens) {
    // Preserve counters across hot-reloads by matching on accessToken.
    const prev = new Map(this._tokens.map(t => [t.accessToken, t]));
    this._tokens = rawTokens
      .filter(t => t && t.accessToken)
      .map(t => {
        const carry = prev.get(t.accessToken);
        return {
          accessToken: t.accessToken,
          machineId: t.machineId || '',
          macMachineId: t.macMachineId || '',
          activeRequests: carry ? carry.activeRequests : 0,
          lastUsed: carry ? carry.lastUsed : 0,
          rateLimitUntil: carry ? carry.rateLimitUntil : 0,
          totalRequests: carry ? carry.totalRequests : 0,
          totalErrors: carry ? carry.totalErrors : 0,
          totalRateLimits: carry ? carry.totalRateLimits : 0,
        };
      });
  }

  size() { return this._tokens.length; }

  pick() {
    if (this._tokens.length === 0) return null;
    const now = Date.now();
    const live = this._tokens.filter(t => t.rateLimitUntil <= now);
    let chosen;
    if (live.length > 0) {
      live.sort((a, b) =>
        (a.activeRequests - b.activeRequests) || (a.lastUsed - b.lastUsed));
      chosen = live[0];
    } else {
      // Every token is parked — pick the one recovering soonest so we at
      // least try, rather than failing closed.
      chosen = this._tokens.slice()
        .sort((a, b) => a.rateLimitUntil - b.rateLimitUntil)[0];
    }
    chosen.activeRequests++;
    chosen.lastUsed = now;
    chosen.totalRequests++;
    // Each pick returns a fresh ticket object so release() can be idempotent
    // per call site without sharing a mutable flag across concurrent requests
    // that happen to draw the same underlying token.
    return {
      accessToken: chosen.accessToken,
      machineId: chosen.machineId,
      macMachineId: chosen.macMachineId,
      _slot: chosen,
      _released: false,
    };
  }

  release(ticket, info = {}) {
    if (!ticket || ticket._released || !ticket._slot) return;
    ticket._released = true;
    const slot = ticket._slot;
    if (slot.activeRequests > 0) slot.activeRequests--;
    if (info.error) slot.totalErrors++;
    if (info.rateLimited) {
      slot.totalRateLimits++;
      slot.rateLimitUntil = Date.now() + TOKEN_RATE_LIMIT_PARK_MS;
    }
  }

  stats() {
    const now = Date.now();
    return {
      tokens: this._tokens.map(t => {
        const parked = t.rateLimitUntil > now;
        return {
          accessToken_suffix: t.accessToken.slice(-6),
          activeRequests: t.activeRequests,
          totalRequests: t.totalRequests,
          totalErrors: t.totalErrors,
          totalRateLimits: t.totalRateLimits,
          parked,
          parkedUntilMs: parked ? t.rateLimitUntil : null,
        };
      }),
    };
  }
}

// Heuristic 429 detection. We accept several shapes because Connect/H2 errors
// surface differently depending on where they originated (TLS layer, Cursor's
// envelope, or the upstream Anthropic provider Cursor proxies to).
function looksLikeRateLimit(err) {
  if (!err) return false;
  if (err.status === 429 || err.statusCode === 429 || err.code === 429) return true;
  if (typeof err.code === 'string' && /resource_exhausted/i.test(err.code)) return true;
  const msg = (typeof err === 'string') ? err : (err.message || '');
  if (/\b429\b/.test(msg)) return true;
  if (/RESOURCE_EXHAUSTED/i.test(msg)) return true;
  if (/rate.?limit|too.many.requests/i.test(msg)) return true;
  return false;
}

const tokenPool = new TokenPool([]);

function loadTokens() {
  try {
    if (!fs.existsSync(TOKEN_FILE)) {
      console.error(`  ❌ token.json not found: ${TOKEN_FILE}`);
      console.error('  📝 Create token.json with your Cursor credentials');
      process.exit(1);
    }
    const data = JSON.parse(fs.readFileSync(TOKEN_FILE, 'utf8'));
    const valid = (data.tokens || []).filter(t => t.accessToken && t.accessToken !== 'your-cursor-access-token-here');
    if (valid.length === 0) {
      console.error('  ❌ No valid tokens in token.json');
      console.error('  📝 Add at least one token with a valid accessToken');
      process.exit(1);
    }
    tokenPool.replace(valid);
    return tokenPool.size();
  } catch (e) {
    console.error(`  ❌ Failed to load token.json: ${e.message}`);
    process.exit(1);
  }
}

// 监听 token.json 变化, 自动热更新
function watchTokenFile() {
  try {
    fs.watchFile(TOKEN_FILE, { interval: 5000 }, () => {
      try {
        const data = JSON.parse(fs.readFileSync(TOKEN_FILE, 'utf8'));
        const newTokens = (data.tokens || []).filter(t => t.accessToken && t.accessToken !== 'your-cursor-access-token-here');
        if (newTokens.length > 0) {
          tokenPool.replace(newTokens);
          console.log(`  🔄 token.json reloaded: ${tokenPool.size()} token(s)`);
        }
      } catch {}
    });
  } catch {}
}

// ── Express 应用 ──
const app = express();
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use((req, res, next) => {
  const incoming = req.headers['x-request-id'];
  const requestId = typeof incoming === 'string' && incoming.trim()
    ? incoming.trim()
    : `req_${uuidv4().replace(/-/g, '').slice(0, 24)}`;
  req.requestId = requestId;
  res.setHeader('x-request-id', requestId);
  next();
});
app.use('/assets', express.static(path.join(PUBLIC_DIR, 'assets'), {
  immutable: true,
  maxAge: '1h',
}));

// ── API Key 简单校验 ──
function checkApiKey(req, res, next) {
  if (!API_KEY) return next();
  const auth = req.headers.authorization;
  const xApiKey = req.headers['x-api-key'];
  const key = typeof xApiKey === 'string' && xApiKey
    ? xApiKey
    : (auth ? auth.replace(/^Bearer\s+/i, '') : '');
  if (!key) return res.status(401).json({ error: { message: 'Missing API key', type: 'auth_error' } });
  if (key !== API_KEY) return res.status(401).json({ error: { message: 'Invalid API key', type: 'auth_error' } });
  next();
}

function shouldRouteAnthropicToRatlc(model) {
  if (!RATLC_ENABLED || !RATLC_ROUTE_MESSAGES) return false;
  const requested = normalizeModelForRouting(model || DEFAULT_MODEL || '').toLowerCase();
  if (RATLC_MODEL_SET.has(requested)) return true;
  const mapped = anthropicConverter.mapAnthropicModel(
    anthropicConverter.stripNoThinkingSuffix(normalizeModelForRouting(model || DEFAULT_MODEL)),
    config.anthropicModelMapping
  );
  return RATLC_MODEL_SET.has(normalizeModelForRouting(mapped || '').toLowerCase());
}

function normalizeModelForRouting(model) {
  return String(model || '').trim().replace(/\[[^\]]+\]$/g, '');
}

function bodyWithRatlcRoutingModel(body) {
  const out = { ...(body || {}) };
  if (typeof out.model === 'string') {
    const normalized = normalizeModelForRouting(out.model);
    if (normalized) out.model = normalized;
  }
  return out;
}

function buildRatlcUnavailableResponse(message) {
  return anthropicConverter.buildAnthropicErrorResponse(message, 'api_error');
}

function safeJoinPublic(relPath) {
  const p = path.resolve(PUBLIC_DIR, relPath);
  return p.startsWith(PUBLIC_DIR + path.sep) || p === PUBLIC_DIR ? p : null;
}

function sendDashboard(req, res) {
  const p = safeJoinPublic('ratlc-dashboard.html');
  if (!p || !fs.existsSync(p)) return res.status(404).send('dashboard not found');
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(fs.readFileSync(p, 'utf8'));
}

async function proxyRatlcGet(req, res, targetPath, timeoutMs = 2500) {
  if (!RATLC_POOL_URL) return res.status(503).json({ ok: false, error: 'RATLC pool URL is not configured' });
  let target;
  try { target = new URL(targetPath, RATLC_POOL_URL + '/'); }
  catch (e) { return res.status(500).json({ ok: false, error: e.message }); }
  const transport = target.protocol === 'https:' ? https : http;
  const upstream = transport.request({
    protocol: target.protocol,
    hostname: target.hostname,
    port: target.port || (target.protocol === 'https:' ? 443 : 80),
    method: 'GET',
    path: target.pathname + target.search,
    timeout: timeoutMs,
  }, (r) => {
    let text = '';
    r.setEncoding('utf8');
    r.on('data', c => { text += c; });
    r.on('end', () => {
      res.status(r.statusCode || 502);
      res.setHeader('Content-Type', r.headers['content-type'] || 'application/json');
      res.send(text);
    });
  });
  upstream.on('timeout', () => upstream.destroy(new Error('RATLC pool request timeout')));
  upstream.on('error', (e) => res.status(503).json({ ok: false, error: e.message }));
  upstream.end();
}

function proxyToRatlc(req, res, targetPath, body, opts = {}) {
  return new Promise((resolve) => {
    if (!RATLC_POOL_URL) {
      if (!res.headersSent) {
        res.status(503).json(buildRatlcUnavailableResponse('RATLC pool URL is not configured'));
      }
      return resolve({ proxied: false, error: 'missing_ratlc_url' });
    }

    let target;
    try {
      target = new URL(targetPath, RATLC_POOL_URL + '/');
    } catch (e) {
      if (!res.headersSent) {
        res.status(500).json(buildRatlcUnavailableResponse(`Invalid RATLC pool URL: ${e.message}`));
      }
      return resolve({ proxied: false, error: e.message });
    }

    const payloadBody = bodyWithRatlcRoutingModel(body);
    const payload = Buffer.from(JSON.stringify(payloadBody || {}), 'utf8');
    const headers = { ...req.headers };
    delete headers.host;
    delete headers.connection;
    delete headers['content-length'];
    delete headers['transfer-encoding'];
    delete headers['accept-encoding'];
    headers['content-type'] = headers['content-type'] || 'application/json';
    headers['content-length'] = String(payload.length);
    headers['x-cursoride-ratlc-proxy'] = '1';
    const remoteIp = req.headers['x-forwarded-for'] || req.socket?.remoteAddress || '';
    if (remoteIp) headers['x-forwarded-for'] = String(remoteIp);
    if (req.socket?.remoteAddress) headers['x-real-ip'] = req.socket.remoteAddress;

    const transport = target.protocol === 'https:' ? https : http;
    const upstreamReq = transport.request({
      protocol: target.protocol,
      hostname: target.hostname,
      port: target.port || (target.protocol === 'https:' ? 443 : 80),
      method: req.method || 'POST',
      path: target.pathname + target.search,
      headers,
      timeout: Number.isFinite(RATLC_TIMEOUT_MS) && RATLC_TIMEOUT_MS > 0 ? RATLC_TIMEOUT_MS : 600000,
    }, (upstreamRes) => {
      const outHeaders = { ...(upstreamRes.headers || {}) };
      delete outHeaders.connection;
      delete outHeaders['keep-alive'];
      delete outHeaders['transfer-encoding'];
      delete outHeaders['content-length'];
      outHeaders['x-cursoride-route'] = 'ratlc-pool';
      outHeaders['x-cursoride-ratlc-url'] = RATLC_POOL_URL;
      if (opts.model) outHeaders['x-cursoride-ratlc-requested-model'] = String(opts.model);
      res.writeHead(upstreamRes.statusCode || 502, outHeaders);
      upstreamRes.pipe(res);
      upstreamRes.on('end', () => resolve({ proxied: true, statusCode: upstreamRes.statusCode || 0 }));
      upstreamRes.on('error', (e) => {
        if (!res.writableEnded) res.end();
        resolve({ proxied: true, error: e.message });
      });
    });

    upstreamReq.on('timeout', () => upstreamReq.destroy(new Error('RATLC pool request timeout')));
    upstreamReq.on('error', (e) => {
      if (!res.headersSent) {
        res.status(503).json(buildRatlcUnavailableResponse(`RATLC pool unavailable: ${e.message}`));
      } else if (!res.writableEnded) {
        try { res.end(); } catch { /* ignore */ }
      }
      resolve({ proxied: false, error: e.message });
    });
    upstreamReq.end(payload);
  });
}

async function getRatlcHealth(timeoutMs = 1500) {
  if (!RATLC_POOL_URL) return { reachable: false, error: 'not_configured' };
  return new Promise((resolve) => {
    let target;
    try { target = new URL('/health', RATLC_POOL_URL + '/'); }
    catch (e) { return resolve({ reachable: false, error: e.message }); }
    const transport = target.protocol === 'https:' ? https : http;
    const req = transport.request({
      protocol: target.protocol,
      hostname: target.hostname,
      port: target.port || (target.protocol === 'https:' ? 443 : 80),
      method: 'GET',
      path: target.pathname + target.search,
      timeout: timeoutMs,
    }, (r) => {
      let text = '';
      r.setEncoding('utf8');
      r.on('data', c => { text += c; });
      r.on('end', () => {
        try {
          const body = JSON.parse(text);
          const pool = body.pool || {};
          resolve({
            reachable: r.statusCode >= 200 && r.statusCode < 300,
            statusCode: r.statusCode,
            ready: pool.readyCount || 0,
            busy: pool.busyCount || 0,
            opening: pool.openingCount || 0,
            dead: pool.deadCount || 0,
            actualSize: pool.actualSize || 0,
            configuredSize: pool.configuredSize || 0,
            pendingRequests: pool.pendingRequests || 0,
            groups: Array.isArray(pool.groups) ? pool.groups : [],
            channels: Array.isArray(pool.channels) ? pool.channels : [],
            config: body.config || null,
          });
        } catch (e) {
          resolve({ reachable: false, statusCode: r.statusCode, error: `bad_json: ${e.message}` });
        }
      });
    });
    req.on('timeout', () => req.destroy(new Error('RATLC health timeout')));
    req.on('error', e => resolve({ reachable: false, error: e.message }));
    req.end();
  });
}

// ── GET /v1/models ──
//
// Cursor's GetUsableModels endpoint opens a fresh H2 connection per call
// (cursor-client.js:328). Claude Code polls /v1/models on startup and
// occasionally afterwards; without caching every call pays a TLS+H2
// handshake. Cache the response for 5 minutes — the model list is stable
// over hours, not minutes.
const _modelsCache = { ts: 0, body: null };
const MODELS_CACHE_TTL_MS = 5 * 60 * 1000;
const STATIC_CURSOR_MODELS = [
  'default',
  'composer-2-fast',
  'composer-2',
  'composer-2.5',
  'composer-2.5-fast',
];

function buildStaticModelsResponse() {
  const seen = new Set();
  const models = [];
  for (const modelId of [...STATIC_CURSOR_MODELS, ...RATLC_MODELS]) {
    const key = String(modelId || '').toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    models.push({ modelId });
  }
  return anthropicConverter.buildModelsResponseWithAnthropicAliases(
    models,
    config.anthropicModelMapping
  );
}

app.get('/v1/models', checkApiKey, async (req, res) => {
  const now = Date.now();
  if (_modelsCache.body && (now - _modelsCache.ts) < MODELS_CACHE_TTL_MS) {
    return res.json(_modelsCache.body);
  }
  if (CURSOR_UPSTREAM_H1_ONLY) {
    const body = buildStaticModelsResponse();
    _modelsCache.ts = now;
    _modelsCache.body = body;
    return res.json(body);
  }
  const token = tokenPool.pick();
  if (!token) return res.json({ object: 'list', data: [] });
  try {
    const cursorClient = getCursorClient();
    const result = await cursorClient.getModels(token);
    const body = anthropicConverter.buildModelsResponseWithAnthropicAliases(
      result.models || [], config.anthropicModelMapping
    );
    _modelsCache.ts = now;
    _modelsCache.body = body;
    tokenPool.release(token, { success: true });
    res.json(body);
  } catch (err) {
    tokenPool.release(token, {
      error: true,
      rateLimited: looksLikeRateLimit(err),
    });
    // On error, serve stale cache if we have one rather than empty list.
    if (_modelsCache.body) return res.json(_modelsCache.body);
    res.json({ object: 'list', data: [] });
  }
});

// ── GET /v1/tools ──
//
// Introspection endpoint for the two tool layers this proxy handles:
// request-declared client tools (third-party/local functions) and Cursor's
// native protocol tools (built into agent_pb.mjs).
app.get(['/v1/tools', '/tools'], checkApiKey, (req, res) => {
  res.json(getCursorToolMatrix({
    passthroughNativeTools: PASSTHROUGH_NATIVE_TOOLS,
    clientTools: [],
  }));
});

app.get(['/ratlc', '/ratlc/dashboard'], sendDashboard);
app.get('/ratlc/requests', checkApiKey, (req, res) => {
  const qs = req.url.includes('?') ? req.url.slice(req.url.indexOf('?')) : '';
  proxyRatlcGet(req, res, '/requests' + qs, 3000);
});
app.get('/ratlc/health', checkApiKey, (req, res) => {
  proxyRatlcGet(req, res, '/health', 3000);
});

// ── POST /v1/responses (OpenAI Responses API MVP) ──
app.post('/v1/responses', checkApiKey, async (req, res) => {
  const body = req.body || {};
  const requestedModel = body.model || DEFAULT_MODEL;
  const cursorModel = mapOpenAICompatibleModel(requestedModel);
  const isStream = body.stream === true;
  const hasTools = isResponsesToolRequest(body);

  if (isStream) {
    return res.status(400).json(buildResponsesError(
      'Streaming /v1/responses is not implemented in this bridge yet; use stream=false.',
      'invalid_request_error',
      400
    ));
  }

  const previous = body.previous_response_id ? responsesStore.get(body.previous_response_id) : null;
  const previousItems = previous ? [...(previous.inputItems || []), ...(previous.outputItems || [])] : [];
  if (previous) previous.lastAccessMs = Date.now();

  if (hasTools) {
    if (!isToolCapableModel(cursorModel)) {
      return res.status(400).json(buildResponsesError(
        `Model ${requestedModel} is not routed to the tool-capable bridge; use composer-2.5-fast, composer-2.5, or a Claude model for tool calls.`,
        'invalid_request_error',
        400
      ));
    }

    const inputItems = normalizeResponsesInputItems(body.input);
    const anthropicMessages = responsesInputToAnthropicMessages(body.input, previousItems);
    if (anthropicMessages.length === 0) {
      return res.status(400).json(buildResponsesError('input is required', 'invalid_request_error', 400));
    }

    const anthropicBody = {
      model: cursorModel,
      max_tokens: body.max_output_tokens || body.max_tokens || 1024,
      stream: false,
      system: body.instructions,
      messages: anthropicMessages,
      tools: responsesToolsToAnthropicTools(body.tools),
    };
    console.log(
      `  🧭 route=bridge-tools /v1/responses | ${requestedModel} → ${cursorModel} | ` +
      `tools=${anthropicBody.tools.length} | prev=${body.previous_response_id ? 'yes' : 'no'}`
    );

    try {
      const upstream = await invokeAnthropicMessages(req, anthropicBody, handleAnthropicMessagesRequest);
      if (upstream.statusCode >= 400) {
        const errBody = upstream.body || (() => {
          try { return JSON.parse(upstream.bodyText); } catch { return null; }
        })();
        return res.status(upstream.statusCode).json(errBody || buildResponsesError(upstream.bodyText || 'Upstream bridge error', 'api_error', upstream.statusCode));
      }

      const response = anthropicToResponses(upstream.body, requestedModel, {
        route: responseRouteFromUpstream(upstream, 'bridge-tools'),
      });
      rememberResponse(response, [...previousItems, ...inputItems]);
      return res.json(response);
    } catch (e) {
      console.error(`  ❌ responses tool bridge error: ${e.message}`);
      return res.status(500).json(buildResponsesError(e.message, 'api_error', 500));
    }
  }

  const inputItems = normalizeResponsesInputItems(body.input);
  const anthropicMessages = responsesInputToAnthropicMessages(body.input, previousItems);
  if (anthropicMessages.length === 0) {
    return res.status(400).json(buildResponsesError('input is required', 'invalid_request_error', 400));
  }

  if (CURSOR_UPSTREAM_H1_ONLY) {
    const anthropicBody = {
      model: cursorModel,
      max_tokens: body.max_output_tokens || body.max_tokens || 1024,
      stream: false,
      system: body.instructions,
      messages: anthropicMessages,
      tools: [],
    };
    console.log(
      `  🧭 route=h1-bridge-responses /v1/responses | ${requestedModel} → ${cursorModel} | ` +
      `prev=${body.previous_response_id ? 'yes' : 'no'}`
    );

    try {
      const upstream = await runAnthropicBridgeOnce(req, anthropicBody);
      if (upstream.statusCode >= 400) {
        const errBody = upstream.body || (() => {
          try { return JSON.parse(upstream.bodyText); } catch { return null; }
        })();
        return res.status(upstream.statusCode).json(errBody || buildResponsesError(upstream.bodyText || 'Upstream bridge error', 'api_error', upstream.statusCode));
      }

      const response = anthropicToResponses(upstream.body, requestedModel, {
        route: responseRouteFromUpstream(upstream, 'h1-bridge-responses'),
      });
      rememberResponse(response, [...previousItems, ...inputItems]);
      return res.json(response);
    } catch (e) {
      console.error(`  ❌ h1 bridge responses error: ${e.message}`);
      return res.status(500).json(buildResponsesError(e.message, 'api_error', 500));
    }
  }

  const token = tokenPool.pick();
  if (!token) {
    return res.status(503).json(buildResponsesError('No available tokens', 'server_error', 503));
  }
  res.on('close', () => tokenPool.release(token, { success: true }));

  const prompt = anthropicConverter.anthropicMessagesToPrompt(anthropicMessages, body.instructions);
  console.log(
    `  📨 [${new Date().toLocaleTimeString()}] route=native-chat /v1/responses ${requestedModel} → ${cursorModel} | ` +
    `prev=${body.previous_response_id ? 'yes' : 'no'} | ${prompt.substring(0, 80)}...`
  );

  try {
    const cursorClient = getCursorClient();
    const result = await cursorClient.chat(token, prompt, cursorModel, { stream: false });
    if (result.error) {
      console.error(`  ❌ ${result.error}`);
      tokenPool.release(token, {
        error: true,
        rateLimited: looksLikeRateLimit(result.error),
      });
      return res.status(500).json(buildResponsesError(result.error, 'api_error', 500));
    }

    tokenPool.release(token, { success: true });
    const response = {
      id: responseId(),
      object: 'response',
      created_at: Math.floor(Date.now() / 1000),
      status: 'completed',
      model: requestedModel,
      output: [{
        type: 'message',
        id: outputMessageId(),
        status: 'completed',
        role: 'assistant',
        content: [{
          type: 'output_text',
          text: result.text || '',
          annotations: [],
        }],
      }],
      output_text: result.text || '',
      usage: {
        input_tokens: result.inputTokens || 0,
        output_tokens: result.outputTokens || 0,
        total_tokens: (result.inputTokens || 0) + (result.outputTokens || 0),
      },
      cursoride_route: 'native-chat',
    };
    rememberResponse(response, [...previousItems, ...inputItems]);
    return res.json(response);
  } catch (e) {
    console.error(`  ❌ responses native error: ${e.message}`);
    tokenPool.release(token, { error: true, rateLimited: looksLikeRateLimit(e) });
    return res.status(500).json(buildResponsesError(e.message, 'api_error', 500));
  }
});

// ── SSE helper ──
function setSSEHeaders(res) {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();
}

// ── OpenAI Responses / Chat tool bridge helpers ──
//
// Keep the transport split explicit:
//   native-chat  = Cursor's simple AgentService chat path, lowest overhead
//   bridge-tools = /v1/messages Cursor Agent bridge, required for tool_use
function isToolCapableModel(model) {
  const id = String(model || '').toLowerCase();
  return id.includes('composer') || id.includes('claude');
}

function mapOpenAICompatibleModel(model) {
  const requested = model || DEFAULT_MODEL;
  const mapped = converter.mapModel(requested);
  if (mapped !== requested || config.modelMapping[requested]) return mapped;

  const anthropicBase = anthropicConverter.stripNoThinkingSuffix(requested);
  const anthropicMapped = anthropicConverter.mapAnthropicModel(anthropicBase, config.anthropicModelMapping);
  if (anthropicMapped && anthropicMapped !== anthropicBase) return anthropicMapped;

  return mapped;
}

function isOpenAIToolRequest(body = {}) {
  const messages = Array.isArray(body.messages) ? body.messages : [];
  const hasToolMessages = messages.some(msg => {
    if (!msg) return false;
    if (Array.isArray(msg.tool_calls) && msg.tool_calls.length > 0) return true;
    if (msg.role === 'tool') return true;
    if (msg.function_call) return true;
    return false;
  });
  if (hasToolMessages) return true;
  if (body.tool_choice === 'none') return false;
  const tools = Array.isArray(body.tools) ? body.tools : [];
  if (tools.length > 0) return true;
  return !!body.tool_choice;
}

function isResponsesToolRequest(body = {}) {
  const input = body.input;
  const hasToolOutput = (item) => {
    if (!item || typeof item !== 'object') return false;
    if (item.type === 'function_call_output') return true;
    if (item.type === 'function_call') return true;
    if (item.type === 'tool_result') return true;
    if (Array.isArray(item.content)) return item.content.some(hasToolOutput);
    return false;
  };
  if (Array.isArray(input) && input.some(hasToolOutput)) return true;
  if (body.tool_choice === 'none') return false;
  const tools = Array.isArray(body.tools) ? body.tools : [];
  if (tools.length > 0) return true;
  return !!body.tool_choice;
}

function responseId() {
  return `resp_${uuidv4().replace(/-/g, '')}`;
}

function outputMessageId() {
  return `msg_${uuidv4().replace(/-/g, '')}`;
}

function functionCallId() {
  return `fc_${uuidv4().replace(/-/g, '')}`;
}

function normalizeTextContent(content) {
  if (content == null) return '';
  if (typeof content === 'string') return content;
  if (typeof content === 'number' || typeof content === 'boolean') return String(content);
  if (Array.isArray(content)) {
    return content.map(part => {
      if (part == null) return '';
      if (typeof part === 'string') return part;
      if (typeof part.text === 'string') return part.text;
      if (typeof part.output_text === 'string') return part.output_text;
      if (typeof part.input_text === 'string') return part.input_text;
      if (part.type === 'text' && typeof part.content === 'string') return part.content;
      return '';
    }).filter(Boolean).join('\n');
  }
  try { return JSON.stringify(content); } catch { return String(content); }
}

function openAIContentToAnthropic(content) {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return normalizeTextContent(content);

  const blocks = [];
  for (const part of content) {
    if (part == null) continue;
    if (typeof part === 'string') {
      blocks.push({ type: 'text', text: part });
      continue;
    }
    if (typeof part.text === 'string') {
      blocks.push({ type: 'text', text: part.text });
      continue;
    }
    if (typeof part.input_text === 'string') {
      blocks.push({ type: 'text', text: part.input_text });
      continue;
    }
    if (typeof part.output_text === 'string') {
      blocks.push({ type: 'text', text: part.output_text });
      continue;
    }
    if (part.type === 'image' && part.source) {
      blocks.push(part);
      continue;
    }
    if (part.type === 'image_url' && part.image_url && part.image_url.url) {
      blocks.push({ type: 'text', text: `[image: ${part.image_url.url}]` });
    }
  }
  return blocks.length === 1 && blocks[0].type === 'text' ? blocks[0].text : blocks;
}

function stringifyArguments(args) {
  if (typeof args === 'string') return args;
  try { return JSON.stringify(args || {}); } catch { return '{}'; }
}

function parseArguments(args) {
  if (args == null) return {};
  if (typeof args === 'object') return args;
  if (typeof args !== 'string') return {};
  try { return JSON.parse(args); } catch { return {}; }
}

function openAIToolsToAnthropicTools(tools) {
  if (!Array.isArray(tools)) return [];
  const out = [];
  for (const tool of tools) {
    if (!tool || typeof tool !== 'object') continue;
    if (tool.type && tool.type !== 'function') continue;
    const fn = tool.function && typeof tool.function === 'object' ? tool.function : tool;
    const name = fn.name || tool.name;
    if (!name) continue;
    out.push({
      name,
      description: fn.description || tool.description || '',
      input_schema: fn.parameters || tool.parameters || { type: 'object', properties: {}, required: [] },
    });
  }
  return out;
}

function chatMessagesToAnthropic(messages) {
  const out = [];
  if (!Array.isArray(messages)) return out;

  for (const msg of messages) {
    if (!msg || typeof msg !== 'object') continue;
    const role = msg.role || 'user';

    if (role === 'system') continue;

    if (role === 'assistant') {
      const blocks = [];
      const text = normalizeTextContent(msg.content);
      if (text) blocks.push({ type: 'text', text });
      const calls = Array.isArray(msg.tool_calls) ? msg.tool_calls : [];
      for (const call of calls) {
        const fn = call && call.function ? call.function : null;
        if (!fn || !fn.name) continue;
        blocks.push({
          type: 'tool_use',
          id: call.id || functionCallId(),
          name: fn.name,
          input: parseArguments(fn.arguments),
        });
      }
      if (msg.function_call && msg.function_call.name) {
        blocks.push({
          type: 'tool_use',
          id: msg.function_call.id || functionCallId(),
          name: msg.function_call.name,
          input: parseArguments(msg.function_call.arguments),
        });
      }
      if (blocks.length > 0) out.push({ role: 'assistant', content: blocks });
      continue;
    }

    if (role === 'tool') {
      out.push({
        role: 'user',
        content: [{
          type: 'tool_result',
          tool_use_id: msg.tool_call_id || msg.id || '',
          content: normalizeTextContent(msg.content),
        }],
      });
      continue;
    }

    if (role === 'function') {
      out.push({
        role: 'user',
        content: [{
          type: 'tool_result',
          tool_use_id: msg.tool_call_id || msg.name || '',
          content: normalizeTextContent(msg.content),
        }],
      });
      continue;
    }

    out.push({ role: 'user', content: openAIContentToAnthropic(msg.content) });
  }

  return out;
}

function chatSystemToAnthropic(messages) {
  if (!Array.isArray(messages)) return undefined;
  const parts = messages
    .filter(m => m && m.role === 'system')
    .map(m => normalizeTextContent(m.content))
    .filter(Boolean);
  return parts.length > 0 ? parts.join('\n\n') : undefined;
}

function anthropicUsageToOpenAI(usage = {}) {
  const input = usage.input_tokens || usage.prompt_tokens || 0;
  const output = usage.output_tokens || usage.completion_tokens || 0;
  return {
    prompt_tokens: input,
    completion_tokens: output,
    total_tokens: input + output,
  };
}

function anthropicToChatCompletion(body, requestedModel) {
  const content = Array.isArray(body.content) ? body.content : [];
  const text = content
    .filter(block => block && block.type === 'text')
    .map(block => block.text || '')
    .join('');
  const toolCalls = content
    .filter(block => block && block.type === 'tool_use')
    .map(block => ({
      id: block.id || functionCallId(),
      type: 'function',
      function: {
        name: block.name || '',
        arguments: stringifyArguments(block.input || {}),
      },
    }));

  const message = { role: 'assistant', content: text || null };
  if (toolCalls.length > 0) message.tool_calls = toolCalls;

  return {
    id: `chatcmpl-${uuidv4().replace(/-/g, '').substring(0, 24)}`,
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model: requestedModel,
    choices: [{
      index: 0,
      message,
      finish_reason: toolCalls.length > 0 ? 'tool_calls' : 'stop',
    }],
    usage: anthropicUsageToOpenAI(body.usage || {}),
  };
}

function buildResponsesError(message, type = 'invalid_request_error', code = 400) {
  return {
    error: {
      message,
      type,
      param: null,
      code,
    },
  };
}

async function runAnthropicBridgeOnce(req, anthropicBody) {
  return invokeAnthropicMessages(req, anthropicBody, handleAnthropicMessagesRequest);
}

function responseInputItemToText(item) {
  if (item == null) return '';
  if (typeof item === 'string') return item;
  if (typeof item === 'number' || typeof item === 'boolean') return String(item);
  if (typeof item.content === 'string') return item.content;
  if (typeof item.text === 'string') return item.text;
  if (typeof item.output_text === 'string') return item.output_text;
  if (typeof item.input_text === 'string') return item.input_text;
  if (Array.isArray(item.content)) return normalizeTextContent(item.content);
  return '';
}

function responsesInputToAnthropicMessages(input, previousItems = []) {
  const source = [];
  if (Array.isArray(previousItems)) source.push(...previousItems);
  if (Array.isArray(input)) source.push(...input);
  else if (input != null) source.push({ role: 'user', content: input });

  const messages = [];
  for (const item of source) {
    if (item == null) continue;
    if (typeof item === 'string') {
      messages.push({ role: 'user', content: item });
      continue;
    }
    if (typeof item !== 'object') {
      messages.push({ role: 'user', content: String(item) });
      continue;
    }

    if (item.type === 'function_call_output') {
      messages.push({
        role: 'user',
        content: [{
          type: 'tool_result',
          tool_use_id: item.call_id || item.id || '',
          content: item.output == null ? '' : normalizeTextContent(item.output),
          is_error: item.status === 'failed' || item.is_error === true,
        }],
      });
      continue;
    }

    if (item.type === 'function_call') {
      messages.push({
        role: 'assistant',
        content: [{
          type: 'tool_use',
          id: item.call_id || item.id || functionCallId(),
          name: item.name || '',
          input: parseArguments(item.arguments),
        }],
      });
      continue;
    }

    if (item.type === 'message' || item.role) {
      const role = item.role === 'assistant' ? 'assistant' : 'user';
      const content = Array.isArray(item.content)
        ? item.content.map(part => {
            if (!part || typeof part !== 'object') return null;
            if (part.type === 'output_text' || part.type === 'input_text' || part.type === 'text') {
              return { type: 'text', text: part.text || '' };
            }
            if (part.type === 'tool_result') return part;
            if (part.type === 'tool_use') return part;
            return null;
          }).filter(Boolean)
        : responseInputItemToText(item);
      if (Array.isArray(content) && content.length === 0) continue;
      if (!Array.isArray(content) && !content) continue;
      messages.push({ role, content });
      continue;
    }

    const text = responseInputItemToText(item);
    if (text) messages.push({ role: 'user', content: text });
  }

  return messages;
}

function normalizeResponsesInputItems(input) {
  if (input == null) return [];
  if (Array.isArray(input)) return input.slice();
  if (typeof input === 'string') {
    return [{
      type: 'message',
      role: 'user',
      content: [{ type: 'input_text', text: input }],
    }];
  }
  if (typeof input === 'object') return [input];
  return [{
    type: 'message',
    role: 'user',
    content: [{ type: 'input_text', text: String(input) }],
  }];
}

function responsesToolsToAnthropicTools(tools) {
  return openAIToolsToAnthropicTools(tools);
}

function anthropicToResponses(body, requestedModel, opts = {}) {
  const id = opts.id || responseId();
  const createdAt = Math.floor(Date.now() / 1000);
  const content = Array.isArray(body.content) ? body.content : [];
  const output = [];
  let outputText = '';

  const text = content
    .filter(block => block && block.type === 'text')
    .map(block => block.text || '')
    .join('');
  if (text) {
    outputText += text;
    output.push({
      type: 'message',
      id: outputMessageId(),
      status: 'completed',
      role: 'assistant',
      content: [{
        type: 'output_text',
        text,
        annotations: [],
      }],
    });
  }

  for (const block of content) {
    if (!block || block.type !== 'tool_use') continue;
    output.push({
      type: 'function_call',
      id: functionCallId(),
      call_id: block.id || functionCallId(),
      name: block.name || '',
      arguments: stringifyArguments(block.input || {}),
      status: 'completed',
    });
  }

  const inputTokens = body.usage?.input_tokens || 0;
  const outputTokens = body.usage?.output_tokens || 0;
  const resp = {
    id,
    object: 'response',
    created_at: createdAt,
    status: 'completed',
    model: requestedModel,
    output,
    output_text: outputText,
    usage: {
      input_tokens: inputTokens,
      output_tokens: outputTokens,
      total_tokens: inputTokens + outputTokens,
    },
  };
  if (opts.route) resp.cursoride_route = opts.route;
  return resp;
}

function responseRouteFromUpstream(upstream, fallback) {
  const headers = upstream && upstream.headers ? upstream.headers : {};
  const route = String(headers['x-cursoride-route'] || '').toLowerCase();
  if (route === 'ratlc-pool') return 'ratlc-responses';
  return fallback;
}

function responseOutputToInputItems(response) {
  if (!response || !Array.isArray(response.output)) return [];
  const items = [];
  for (const item of response.output) {
    if (!item || typeof item !== 'object') continue;
    if (item.type === 'message') {
      items.push({
        type: 'message',
        role: item.role || 'assistant',
        content: Array.isArray(item.content) ? item.content : [],
      });
    } else if (item.type === 'function_call') {
      items.push({
        type: 'function_call',
        id: item.id,
        call_id: item.call_id,
        name: item.name,
        arguments: item.arguments,
      });
    }
  }
  return items;
}

function rememberResponse(response, inputItems) {
  if (!response || !response.id) return;
  responsesStore.set(response.id, {
    lastAccessMs: Date.now(),
    inputItems: Array.isArray(inputItems) ? inputItems : [],
    outputItems: responseOutputToInputItems(response),
  });
}

function parseSseEvents(text) {
  const events = [];
  let event = 'message';
  let dataLines = [];
  const flush = () => {
    if (dataLines.length === 0) {
      event = 'message';
      return;
    }
    const raw = dataLines.join('\n');
    dataLines = [];
    const name = event;
    event = 'message';
    if (!raw || raw === '[DONE]') return;
    try {
      events.push({ event: name, data: JSON.parse(raw) });
    } catch {
      events.push({ event: name, data: raw });
    }
  };

  for (const line of String(text || '').replace(/\r\n/g, '\n').split('\n')) {
    if (line === '') {
      flush();
      continue;
    }
    if (line.startsWith(':')) continue;
    if (line.startsWith('event:')) {
      event = line.slice(6).trim() || 'message';
      continue;
    }
    if (line.startsWith('data:')) {
      let data = line.slice(5);
      if (data.startsWith(' ')) data = data.slice(1);
      dataLines.push(data);
    }
  }
  flush();
  return events;
}

function compactAnthropicBlock(block) {
  if (!block || typeof block !== 'object') return block;
  const out = { ...block };
  if (out._inputJson != null) {
    if (out.type === 'tool_use' || out.type === 'server_tool_use') {
      try { out.input = out._inputJson ? JSON.parse(out._inputJson) : (out.input || {}); }
      catch { out.input = out.input || {}; }
    }
    delete out._inputJson;
  }
  delete out._stopped;
  return out;
}

function parseAnthropicSseMessage(text) {
  const events = parseSseEvents(text);
  let message = null;
  const blocks = new Map();
  const content = [];
  let finalUsage = null;
  let finalDelta = null;

  const pushBlock = (idx) => {
    const block = blocks.get(idx);
    if (!block || block._stopped) return;
    block._stopped = true;
    content.push(compactAnthropicBlock(block));
  };

  for (const evt of events) {
    const data = evt.data;
    if (!data || typeof data !== 'object') continue;
    if (data.type === 'message_start' && data.message) {
      message = { ...data.message, content: [] };
      continue;
    }
    if (data.type === 'content_block_start') {
      const idx = Number(data.index);
      const block = { ...(data.content_block || {}) };
      if (block.type === 'text') block.text = block.text || '';
      if (block.type === 'thinking') block.thinking = block.thinking || '';
      if (block.type === 'tool_use' || block.type === 'server_tool_use') {
        block.input = block.input || {};
        block._inputJson = '';
      }
      blocks.set(idx, block);
      continue;
    }
    if (data.type === 'content_block_delta') {
      const idx = Number(data.index);
      const block = blocks.get(idx) || {};
      const delta = data.delta || {};
      if (delta.type === 'text_delta') block.text = String(block.text || '') + String(delta.text || '');
      else if (delta.type === 'input_json_delta') block._inputJson = String(block._inputJson || '') + String(delta.partial_json || '');
      else if (delta.type === 'thinking_delta') block.thinking = String(block.thinking || '') + String(delta.thinking || '');
      else if (delta.type === 'signature_delta') block.signature = String(block.signature || '') + String(delta.signature || '');
      blocks.set(idx, block);
      continue;
    }
    if (data.type === 'content_block_stop') {
      pushBlock(Number(data.index));
      continue;
    }
    if (data.type === 'message_delta') {
      finalDelta = data.delta || null;
      finalUsage = data.usage || null;
    }
  }

  for (const idx of [...blocks.keys()].sort((a, b) => a - b)) pushBlock(idx);

  const usage = {
    ...(message?.usage || {}),
    ...(finalUsage || {}),
  };
  if (message?.usage?.input_tokens != null && (!usage.input_tokens || usage.input_tokens === 0)) {
    usage.input_tokens = message.usage.input_tokens;
  }

  return {
    id: message?.id || `msg_${uuidv4().replace(/-/g, '').substring(0, 24)}`,
    type: 'message',
    role: 'assistant',
    content,
    model: message?.model || DEFAULT_MODEL,
    stop_reason: finalDelta?.stop_reason || message?.stop_reason || 'end_turn',
    stop_sequence: finalDelta?.stop_sequence || message?.stop_sequence || null,
    usage,
  };
}

function parseInvocationBody(bodyText, headers = {}) {
  const text = String(bodyText || '');
  if (!text) return null;
  const contentType = String(headers['content-type'] || headers['Content-Type'] || '').toLowerCase();
  if (contentType.includes('text/event-stream') || /^event:/m.test(text)) {
    return parseAnthropicSseMessage(text);
  }
  try { return JSON.parse(text); } catch { return null; }
}

async function invokeAnthropicMessages(req, anthropicBody, responseAdapter) {
  return new Promise((resolve, reject) => {
    let statusCode = 200;
    const headers = {};
    const chunks = [];
    const closeHandlers = [];
    let settled = false;
    function finish(payload) {
      if (settled) return;
      settled = true;
      for (const cb of closeHandlers) {
        try { cb(); } catch { /* ignore */ }
      }
      resolve(payload);
    }
    const proxyRes = new Writable({
      write(chunk, encoding, cb) {
        proxyRes.headersSent = true;
        chunks.push(Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk));
        cb();
      },
      final(cb) {
        if (!proxyRes.writableEnded) finishResponse();
        cb();
      },
    });
    proxyRes.headersSent = false;
    const finishResponse = (chunk) => {
      if (chunk) proxyRes.write(chunk);
      const bodyText = chunks.join('');
      finish({
        statusCode,
        headers,
        bodyText,
        body: parseInvocationBody(bodyText, headers),
      });
    };
    Object.assign(proxyRes, {
      status(code) {
        statusCode = code;
        return this;
      },
      setHeader(name, value) {
        headers[String(name).toLowerCase()] = value;
      },
      writeHead(code, outHeaders) {
        statusCode = code || statusCode;
        if (outHeaders && typeof outHeaders === 'object') {
          for (const [k, v] of Object.entries(outHeaders)) headers[String(k).toLowerCase()] = v;
        }
        this.headersSent = true;
        return this;
      },
      flushHeaders() {
        this.headersSent = true;
      },
      end(chunk) {
        finishResponse(chunk);
      },
      json(obj) {
        this.headersSent = true;
        this.writableEnded = true;
        finish({
          statusCode,
          headers,
          bodyText: JSON.stringify(obj),
          body: obj,
        });
      },
    });
    const originalOn = proxyRes.on.bind(proxyRes);
    proxyRes.on = (event, cb) => {
      if (event === 'close' && typeof cb === 'function') closeHandlers.push(cb);
      return originalOn(event, cb);
    };

    const proxyReq = Object.create(req);
    proxyReq.body = anthropicBody;
    proxyReq.path = '/v1/messages';
    proxyReq.method = 'POST';
    proxyReq.on = () => proxyReq;

    Promise.resolve(responseAdapter(proxyReq, proxyRes)).catch(reject);
  });
}

// ── Build the per-turn callback set used by handleFreshTurn / handleContinuation ──
//
// Captures the response stream, the per-turn state, and the cache identifiers
// so the same callback shape can drive both the initial turn and any
// re-entered turns (after sendToolResult). The returned `closeOpenBlock()` is
// also handy for the caller to invoke before terminating the response.
function buildTurnCallbacks(ctx) {
  const {
    res, isStream, turnState,
    convKey, bridgeKey, conversationId, sessionId,
    requestedModel, cursorModel, mcpTools,
    getBridge, requestId,
    clientThinkingEnabled = false,
    // Optional — present for continuation turns. When set we update the
    // existing entry in place (preserving its bridgeKey alias set) instead
    // of creating a new one.
    cachedEntry,
    // Whether this turn is a continuation (claude-code POSTed tool_result
    // blocks). Fed to runtime-stats so we can break down latency by mode.
    isContinuation = false,
    // Timings object — populated by callbacks; logged on turn end so the
    // user can see where time went (local proxy work vs upstream Cursor).
    timings,
    // Optional — the TokenPool ticket for this fresh turn. Continuations
    // reuse the bridge's already-released ticket and pass null. Callbacks
    // that signal a true terminal outcome (success/error) call
    // tokenPool.release(token, ...); release() is idempotent so the
    // res.on('close') backstop on the original handler is still safe.
    token,
    // Position of the assistant message this turn produces in the
    // conversation history. Used to key recorded thinking content so it
    // can be re-injected against the matching assistant message on later
    // turns. Only meaningful for fresh-turn handlers; continuations reuse
    // the previous assistant message slot.
    assistantTurnIdx = null,
  } = ctx;
  function stamp(name) {
    if (timings && timings.t0 != null && timings[name] == null) {
      timings[name] = Date.now() - timings.t0;
    }
  }

  function closeOpenBlock() {
    if (turnState.textBlockOpen) {
      // Flush any text the hallucination filter is still holding (typically
      // an incomplete `[Tool call:` prefix the model started but never
      // finished). Leak it as plain text rather than silently swallowing —
      // better visible noise than swallowed user content.
      const trailing = turnState.hallucinationFilter
        ? turnState.hallucinationFilter.flush()
        : '';
      if (trailing) {
        if (!isStream) turnState.accumulatedText += trailing;
        if (isStream && !res.writableEnded) {
          res.write(anthropicConverter.buildContentBlockDelta(turnState.nextBlockIndex - 1, trailing));
        }
      }
      if (isStream && !res.writableEnded) {
        res.write(anthropicConverter.buildContentBlockStop(turnState.nextBlockIndex - 1));
      }
      turnState.textBlockOpen = false;
    }
    if (turnState.thinkingBlockOpen) {
      if (isStream && !res.writableEnded) {
        const signature = turnState.thinkingAdapter
          ? turnState.thinkingAdapter.signature({ blockIndex: turnState.thinkingBlockIndex })
          : null;
        if (signature) {
          res.write(anthropicConverter.buildContentBlockDeltaSignature(turnState.thinkingBlockIndex, signature));
        }
        res.write(anthropicConverter.buildContentBlockStop(turnState.thinkingBlockIndex));
      }
      turnState.thinkingBlockOpen = false;
      turnState.thinkingBlockIndex = null;
      turnState.thinkingAdapter = null;
    }
  }

  // Scan the buffered text + thinking content for `[Tool call: NAME({...})]`
  // patterns the model emitted as TEXT instead of as structured tool_use,
  // and synthesize tool_use blocks for the ones not already represented in
  // pendingToolCalls. Safe to call multiple times per turn — uses
  // turnState.rescuedHitCount as a watermark so identical hits aren't
  // re-rescued and dedups against real tool calls already in the list.
  //
  // Must be called BEFORE writing the final message_delta / message_stop in
  // a tool_use finalize, so the synthesized content_block_* deltas can ride
  // out on the still-open response.
  //
  // Returns the number of newly rescued calls (informational).
  function tryRescueHallucinatedToolCalls() {
    if (turnState.toolUseFinished) {
      // After finalize, res is closed — we couldn't emit blocks anyway.
      return 0;
    }
    const textBuf = turnState.emittedTextForDetection || '';
    const thinkBuf = turnState.emittedThinkingForDetection || '';
    if (!textBuf && !thinkBuf) return 0;
    // Concatenating with a newline separator is fine for the parser — it
    // scans for the `[Tool call: ` literal which doesn't cross the boundary.
    const combined = textBuf + (thinkBuf ? '\n' + thinkBuf : '');
    const allHits = anthropicTools.parseHallucinatedToolCalls(combined);
    if (allHits.length <= turnState.rescuedHitCount) return 0;
    const newHits = allHits.slice(turnState.rescuedHitCount);

    const registered = new Set(
      (mcpTools || []).flatMap(t => [t && t.name, t && t.toolName]).filter(Boolean)
    );

    // Dedup keys for already-pending calls (real or previously rescued).
    const pendingKeys = new Set(turnState.pendingToolCalls.map(tc => {
      try { return tc.toolName + '|' + JSON.stringify(tc.args || {}); }
      catch { return tc.toolName + '|?'; }
    }));

    let added = 0;
    for (const hit of newHits) {
      if (anthropicTools.shouldDropClientWebLookupToolName(hit.name)) {
        console.log(`  hallucinated-tool-call ignored: ${hit.name} (web lookup is Cursor-native only)`);
        continue;
      }
      const canonical = anthropicTools.canonicalizeHallucinatedToolName(hit.name, registered);
      const normalizedArgs = anthropicTools.normalizeHallucinatedToolArgs(canonical, hit.args || {});
      if (anthropicTools.shouldDropClientWebLookupToolName(canonical)) {
        console.log(`  hallucinated-tool-call ignored: ${hit.name} → ${canonical} (web lookup is Cursor-native only)`);
        continue;
      }
      if (looksLikeAgentToolPlaceholderWrite(canonical, normalizedArgs)) {
        console.log(`  hallucinated-tool-call ignored: ${hit.name} (empty agent-tools placeholder write)`);
        continue;
      }
      const dupKey = (() => {
        try { return canonical + '|' + JSON.stringify(normalizedArgs); }
        catch { return canonical + '|?'; }
      })();
      if (pendingKeys.has(dupKey)) {
        // The model emitted both a real tool_use AND a `[Tool call: ...]`
        // textual narration of the same call — don't double-execute.
        continue;
      }
      pendingKeys.add(dupKey);

      const argsJson = (() => {
        try { return JSON.stringify(normalizedArgs); }
        catch { return '{}'; }
      })();
      // The rescue path writes tool_use blocks directly, so close any open
      // text/thinking block before allocating the synthetic block index.
      closeOpenBlock();
      const synthExecId = '';
      const synthToolCallId = `toolu_synth_${uuidv4().replace(/-/g, '').slice(0, 16)}`;
      const anthropicToolUseId = anthropicTools.encodeToolUseId(convKey, synthExecId, synthToolCallId, sessionId);
      const blockIndex = turnState.nextBlockIndex++;
      turnState.pendingToolCalls.push({
        execMsgId: 0,
        execId: synthExecId,
        toolCallId: synthToolCallId,
        toolName: canonical,
        args: normalizedArgs,
        anthropicToolUseId,
        blockIndex,
        synthetic: true,
      });
      if (isStream && !res.writableEnded) {
        res.write(anthropicConverter.buildContentBlockStartToolUse(blockIndex, anthropicToolUseId, canonical));
        res.write(anthropicConverter.buildContentBlockDeltaInputJson(blockIndex, argsJson));
        res.write(anthropicConverter.buildContentBlockStop(blockIndex));
      }
      const normalizedKeys = Object.keys(normalizedArgs);
      const originalKeys = Object.keys(hit.args || {});
      const argsRenamed = normalizedKeys.length === originalKeys.length &&
        normalizedKeys.some(k => !originalKeys.includes(k));
      console.log(
        `  🩹 hallucinated-tool-call rescued: ${hit.name}` +
        (canonical !== hit.name ? ` → ${canonical}` : '') +
        (argsRenamed ? ` (args normalized: ${originalKeys.join(',')} → ${normalizedKeys.join(',')})` : '')
      );
      added++;
    }
    turnState.rescuedHitCount = allHits.length;
    return added;
  }

  function finalizeToolUseTurn() {
    if (turnState.toolUseFinished) return;
    if (turnState.toolUseFinishTimer) {
      clearTimeout(turnState.toolUseFinishTimer);
      turnState.toolUseFinishTimer = null;
    }
    // Mixed-mode rescue: if the model emitted real tool_use AND
    // `[Tool call: ...]` text in the same turn (a known failure mode of
    // Cursor's thinking-max with long contexts), this is our last chance
    // to synthesize the hallucinated calls before the response closes.
    // Must run BEFORE setting toolUseFinished + writing message_delta so
    // the synthesized content_block_* deltas can still go out on the wire.
    try { tryRescueHallucinatedToolCalls(); } catch { /* never let rescue crash finalize */ }
    turnState.toolUseFinished = true;
    closeOpenBlock();

    // Cache the bridge for the client's follow-up tool_result POST. For a
    // continuation turn we mutate the existing entry in place so the alias
    // set (entry.bridgeKeys) accumulates across socket reconnects. For a
    // fresh turn we create a new entry. Either way indexBridgeEntry adds
    // the current bridgeKey to the alias set and refreshes both indices.
    const bridge = getBridge();
    const entry = cachedEntry || {
      bridge,
      mcpTools,
      convKey,
      conversationId,
      sessionId,
      requestedModel,
      cursorModel,
      clientThinkingEnabled,
    };
    entry.lastAccessMs = Date.now();
    entry.pendingExecs = turnState.pendingToolCalls.slice();
    entry.requestId = requestId;
    entry.clientThinkingEnabled = !!clientThinkingEnabled;
    indexBridgeEntry(entry, bridgeKey);

    if (isStream) {
      if (!res.writableEnded) {
        // Read latest known token counts from the bridge — checkpoint
        // updates may have populated input_tokens before the early
        // finalize fires. Better to emit the real number when we have
        // it than to leave claude-code's /context counter at 0.
        const stats = (bridge && typeof bridge.getStats === 'function') ? bridge.getStats() : null;
        const inTok = stats ? stats.inputTokens : 0;
        const outTok = stats ? stats.outputTokens : 0;
        res.write(anthropicConverter.buildMessageDelta('tool_use', outTok || 0, inTok || 0));
        res.write(anthropicConverter.buildMessageStop());
        res.end();
      }
    } else if (!res.headersSent) {
      const toolUses = turnState.pendingToolCalls.map(tc => ({
        id: tc.anthropicToolUseId,
        name: tc.toolName,
        input: tc.args,
      }));
      res.json(anthropicConverter.buildAnthropicResponse(
        turnState.accumulatedText,
        requestedModel,
        0, 0,
        { stopReason: 'tool_use', toolUses }
      ));
    }

    stamp('turnEnded');
    stamp('respEnded');
    console.log(
      `  ✅ turn ended (tool_use finalize) | toolCalls=${turnState.pendingToolCalls.length}` +
      (timings ? ` | ${formatTimings(timings)}` : '')
    );
  }

  // Backstop debounce — fires if no `stepCompleted` arrives. The common
  // case (stepCompleted comes ~10ms after the last mcpArgs) finalizes
  // immediately via onStepCompleted; this 250ms is just a safety net.
  function armToolUseFinalizer() {
    if (turnState.toolUseFinishTimer) clearTimeout(turnState.toolUseFinishTimer);
    turnState.toolUseFinishTimer = setTimeout(finalizeToolUseTurn, 250);
  }

  return {
    closeOpenBlock,

    onTextDelta: (text) => {
      stamp('firstFrame');
      stamp('firstText');
      // ALWAYS append to the detection buffer first — the structural rescue
      // path reads from this buffer at finalize/onTurnEnded to find
      // `[Tool call: ...]` patterns and synthesize tool_use blocks. The
      // filter below only suppresses the bracketed text from the wire; the
      // rescue still needs the original text to find what to rescue.
      turnState.emittedTextForDetection += text;
      // Filter out hallucinated `[Tool call: ...]` patterns from outbound
      // text. Returns only the safe-to-forward portion (whole pattern
      // ranges suppressed; partial pattern starts may be held back for
      // the next delta).
      const forwarded = turnState.hallucinationFilter.feed(text);
      if (!forwarded) return;  // entirely suppressed / held back

      if (turnState.thinkingBlockOpen) closeOpenBlock();
      if (!turnState.textBlockOpen) {
        const idx = turnState.nextBlockIndex++;
        if (isStream && !res.writableEnded) {
          res.write(anthropicConverter.buildContentBlockStart(idx));
        }
        turnState.textBlockOpen = true;
      }
      // Only accumulate for non-streaming responses. In stream mode the
      // delta is already on the wire; appending to a JS string per chunk
      // is O(n²) for long generations because strings are immutable.
      if (!isStream) {
        turnState.accumulatedText += forwarded;
      }
      if (isStream && !res.writableEnded) {
        res.write(anthropicConverter.buildContentBlockDelta(turnState.nextBlockIndex - 1, forwarded));
      }
    },

    onThinkingDelta: (text) => {
      // ALWAYS append to the detection buffer so the hallucination rescue
      // path can find `[Tool call: ...]` patterns the model may have
      // emitted inside the thinking block. This buffer is internal-only.
      turnState.emittedThinkingForDetection += text;

      // Drop thinking content from the API response by default. When explicitly
      // enabled, emit a proxy-local thinking block for Claude Code UI display.
      if (!PROXY_THINKING_BLOCKS || !clientThinkingEnabled) return;
      if (!turnState.thinkingBlockOpen && turnState.thinkingBlockStarted) return;
      if (!turnState.thinkingBlockOpen && turnState.nextBlockIndex > 0) return;

      if (turnState.textBlockOpen) closeOpenBlock();
      if (!turnState.thinkingBlockOpen) {
        const idx = turnState.nextBlockIndex++;
        turnState.thinkingBlockIndex = idx;
        turnState.thinkingBlockStarted = true;
        turnState.thinkingAdapter = new ProxyThinkingBlockAdapter({
          source: 'server',
          convKey,
          requestId,
          turnIndex: assistantTurnIdx,
          blockIndex: idx,
        });
        if (isStream && !res.writableEnded) {
          res.write(anthropicConverter.buildContentBlockStartThinking(idx));
        }
        turnState.thinkingBlockOpen = true;
      }
      if (turnState.thinkingAdapter) turnState.thinkingAdapter.append(text);
      // Same rationale as onTextDelta: skip the in-memory accumulator when
      // we're streaming — it's not used for the response, only for diagnostics.
      if (!isStream) {
        turnState.accumulatedThinking += text;
      }
      if (isStream && !res.writableEnded) {
        res.write(anthropicConverter.buildContentBlockDeltaThinking(turnState.thinkingBlockIndex, text));
      }
    },

    onMcpCall: ({ id, execId, toolCallId, toolName, args }) => {
      stamp('firstFrame');
      stamp('firstTool');
      const registeredNames = new Set(
        (mcpTools || []).flatMap(t => [t && t.name, t && t.toolName]).filter(Boolean)
      );
      const wireToolName = anthropicTools.normalizeMcpWireToolNameForClient(toolName, registeredNames);
      const normalizedToolName = anthropicTools.canonicalizeHallucinatedToolName(wireToolName, registeredNames);
      const normalizedArgs = anthropicTools.normalizeHallucinatedToolArgs(normalizedToolName, { ...(args || {}) });
      const artifactPath = agentToolsStore.getPathFromArgs(normalizedArgs || {});
      if (normalizedToolName === 'Write' && agentToolsStore.isAgentToolsArtifactPath(artifactPath)) {
        const content = agentToolsStore.contentForAgentToolsWrite(
          agentToolsStore.getContentFromArgs(normalizedArgs || {})
        );
        const entry = agentToolsStore.putAgentToolsArtifact(artifactPath, content, { source: 'server-write' });
        console.log(`  virtual agent-tools Write captured: ${artifactPath} (${entry?.bytes || 0} bytes)`);
        const bridge = getBridge();
        if (bridge && typeof bridge.sendToolResult === 'function') {
          bridge.sendToolResult(id, execId, agentToolsStore.makeVirtualWriteResultText(entry));
          return;
        }
      }
      if (normalizedToolName === 'Read' && agentToolsStore.isAgentToolsArtifactPath(artifactPath)) {
        const entry = agentToolsStore.getAgentToolsArtifact(artifactPath);
        const content = entry ? entry.content : agentToolsStore.makeVirtualReadMissingText(artifactPath);
        console.log(`  virtual agent-tools Read served: ${artifactPath} (${Buffer.byteLength(content)} bytes, found=${entry ? 1 : 0})`);
        const bridge = getBridge();
        if (bridge && typeof bridge.sendToolResult === 'function') {
          bridge.sendToolResult(id, execId, content);
          return;
        }
      }
      if (serverLocalTools.isServerLocalToolName(normalizedToolName)) {
        const localToolName = serverLocalTools.normalizeServerLocalToolName(normalizedToolName);
        const bridge = getBridge();
        if (bridge && typeof bridge.sendToolResult === 'function') {
          const localDecision = serverLocalTools.getServerLocalToolDecision(localToolName, normalizedArgs || {});
          if (!localDecision.canRun && localDecision.retryOnClient) {
            console.log(`  local tool adapter skip: ${localToolName}: ${localDecision.reason || 'not runnable locally'}; forwarding to client`);
          } else {
            let argsPreview = '';
            try { argsPreview = JSON.stringify(normalizedArgs || {}).slice(0, 160); } catch { argsPreview = '{...}'; }
            console.log(`  local tool adapter: ${localToolName}(${argsPreview})`);
            serverLocalTools.runServerLocalTool(localToolName, normalizedArgs || {})
              .then((result) => {
                const content = result && result.content != null ? String(result.content) : '';
                if (!result || !result.ok) {
                  console.log(`  local tool adapter error: ${localToolName}: ${content.slice(0, 200)}`);
                } else {
                  console.log(`  local tool adapter ok: ${localToolName} bytes=${Buffer.byteLength(content)}`);
                }
                bridge.sendToolResult(id, execId, content);
              })
              .catch((err) => {
                const message = err && err.message ? err.message : String(err);
                console.log(`  local tool adapter threw: ${localToolName}: ${message}`);
                bridge.sendToolResult(id, execId, `[proxy_error] ${message}`);
              });
            return;
          }
        }
      }
      closeOpenBlock();
      const blockIndex = turnState.nextBlockIndex++;
      const anthropicToolUseId = anthropicTools.encodeToolUseId(convKey, execId, toolCallId, sessionId);

      turnState.pendingToolCalls.push({
        execMsgId: id, execId, toolCallId, toolName: normalizedToolName, args: normalizedArgs,
        anthropicToolUseId, blockIndex,
      });

      if (isStream && !res.writableEnded) {
        res.write(anthropicConverter.buildContentBlockStartToolUse(blockIndex, anthropicToolUseId, normalizedToolName));
        try {
          res.write(anthropicConverter.buildContentBlockDeltaInputJson(blockIndex, JSON.stringify(normalizedArgs || {})));
        } catch {
          res.write(anthropicConverter.buildContentBlockDeltaInputJson(blockIndex, '{}'));
        }
        res.write(anthropicConverter.buildContentBlockStop(blockIndex));
      }

      let argsPreview = '';
      try { argsPreview = JSON.stringify(normalizedArgs || {}).slice(0, 80); } catch { argsPreview = '{...}'; }
      console.log(`  🔧 tool call: ${normalizedToolName}(${argsPreview}) → ${anthropicToolUseId}`);
      debugLog.logToolCall(
        { request_id: requestId, conv_key: convKey, model_cursor: cursorModel },
        normalizedToolName, normalizedArgs
      );

      // The Cursor stream is now paused waiting for our mcpResult — Cursor
      // will NOT emit turnEnded until we send tool results. So we synthesize
      // a turn-end with stop_reason=tool_use either:
      //   - immediately on `stepCompleted` (the model has finished emitting
      //     tool calls for this step), or
      //   - after a 250 ms backstop debounce in case stepCompleted is
      //     delayed or missing.
      // The first signal wins; the other is no-op'd by `toolUseFinished`.
      armToolUseFinalizer();
    },

    onStepCompleted: () => {
      // Cursor finished a step; if we have pending tool calls, this is the
      // signal that the model is done emitting them and is now waiting for
      // the result. Fire the finalize immediately — saves the 250 ms debounce.
      if (turnState.pendingToolCalls.length > 0 && !turnState.toolUseFinished) {
        finalizeToolUseTurn();
      }
    },

    onTurnEnded: ({
      inputTokens, outputTokens, conversationState: newState,
      maxIdleMs, stallThresholdSource,
      turnRetries, turnTransportErrors, turnStalls, turnCascadeDetected,
    }) => {
      closeOpenBlock();

      // NOTE: we deliberately do NOT cache `newState` to conversationStates
      // anymore. Empirically, Cursor's KV blob store appears to be scoped
      // per-H2-stream — replaying a saved checkpoint on a fresh stream
      // triggers `Connect error internal: Blob not found` because the
      // referenced blobs only existed in the closed stream. Each fresh
      // /v1/messages request starts with empty state; Cursor rebuilds its
      // blob store from setBlobArgs and the client re-supplies the message
      // history anyway. The conversationStates Map is kept for the future
      // case where we share an H2 client across requests.

      // If we already finalized this turn via the tool-use debounce, ignore
      // (the model has paused waiting for tool results — turnEnded won't fire
      // until we sendToolResult, but if it does we don't want to double-send).
      if (turnState.toolUseFinished) {
        console.log(`  (turnEnded after tool_use finalize, in=${inputTokens} out=${outputTokens})`);
        return;
      }

      // Hallucinated-tool-call rescue: scans text + thinking content for
      // `[Tool call: NAME({...})]` patterns the model emitted instead of
      // structured tool_use, synthesizes tool_use blocks for any not
      // already represented. Idempotent — safe even if finalizeToolUseTurn
      // already ran the same scan. (In practice that path early-returns
      // above via toolUseFinished, but the helper guards anyway.)
      try { tryRescueHallucinatedToolCalls(); } catch { /* never let rescue crash end_turn */ }

      // Record this turn's thinking content into the per-conversation
      // store so the NEXT turn can re-inject it as <thinking> context.
      // No-op when CURSOR_REINJECT_THINKING is off.
      if (thinkingHistory.isEnabled() && assistantTurnIdx != null && turnState.emittedThinkingForDetection) {
        try {
          thinkingHistory.recordTurnThinking(convKey, assistantTurnIdx, turnState.emittedThinkingForDetection);
        } catch { /* ignore */ }
      }

      const hasTools = turnState.pendingToolCalls.length > 0;
      const stopReason = hasTools ? 'tool_use' : 'end_turn';
      // If every pending tool call is synthetic (rescued from hallucinated
      // text), Cursor's stream wasn't actually waiting on these. Don't keep
      // the bridge — force the client's continuation to cache-miss into a
      // fresh-turn rebuild that includes the synthetic tool_use + tool_result
      // in the message history.
      const allSynthetic = hasTools && turnState.pendingToolCalls.every(tc => tc.synthetic);

      const bridge = getBridge();

      if (hasTools && !allSynthetic) {
        const entry = cachedEntry || {
          bridge,
          mcpTools,
          convKey,
          conversationId,
          sessionId,
          requestedModel,
          cursorModel,
          clientThinkingEnabled,
        };
        entry.lastAccessMs = Date.now();
        entry.pendingExecs = turnState.pendingToolCalls.slice();
        entry.requestId = requestId;
        entry.clientThinkingEnabled = !!clientThinkingEnabled;
        indexBridgeEntry(entry, bridgeKey);
      } else {
        // Either a clean end_turn OR a fully-synthetic-tools turn that we
        // can't serve via the existing bridge. Drop the cached entry (if
        // any) so the next continuation cache-misses and rebuilds.
        if (cachedEntry) {
          dropBridgeEntry(cachedEntry, allSynthetic ? 'synthetic-tool-use' : 'end_turn');
        } else {
          try { bridge && bridge.close(); } catch {}
        }
      }

      if (isStream) {
        if (!res.writableEnded) {
          res.write(anthropicConverter.buildMessageDelta(stopReason, outputTokens || 0, inputTokens || 0));
          res.write(anthropicConverter.buildMessageStop());
          res.end();
        }
      } else if (!res.headersSent) {
        const toolUses = turnState.pendingToolCalls.map(tc => ({
          id: tc.anthropicToolUseId,
          name: tc.toolName,
          input: tc.args,
        }));
        res.json(anthropicConverter.buildAnthropicResponse(
          turnState.accumulatedText,
          requestedModel,
          inputTokens,
          outputTokens,
          { stopReason, toolUses }
        ));
      }

      stamp('turnEnded');
      stamp('respEnded');
      const maxIdleStr = (typeof maxIdleMs === 'number')
        ? ` | maxIdle=${Math.round(maxIdleMs)}ms src=${stallThresholdSource || 'baseline'}`
        : '';
      console.log(
        `  ✅ turn ended | in=${inputTokens} out=${outputTokens} | ` +
        `stopReason=${stopReason} | toolCalls=${turnState.pendingToolCalls.length}` +
        (timings ? ` | ${formatTimings(timings)}` : '') +
        maxIdleStr
      );
      // Feed runtime-stats. Duration = total turn wall time (from t0 to
      // turnEnded). firstFrame/firstText/firstTool come from the per-turn
      // timing object. Tool count comes from the turnState's pending list.
      try {
        runtimeStats.recordTurnEnd({
          model: cursorModel,
          outcome: 'success',
          isStream: !!isStream,
          isContinuation: !!isContinuation,
          durationMs:   timings ? timings.turnEnded : null,
          firstFrameMs: timings ? timings.firstFrame : null,
          firstTextMs:  timings ? timings.firstText : null,
          firstToolMs:  timings ? timings.firstTool : null,
          maxIdleMs: typeof maxIdleMs === 'number' ? maxIdleMs : null,
          retries: turnRetries || 0,
          transportErrors: turnTransportErrors || 0,
          stalls: turnStalls || 0,
          cascadeDetected: !!turnCascadeDetected,
          toolCount: turnState.pendingToolCalls.length,
          inputTokens: typeof inputTokens === 'number' ? inputTokens : null,
          outputTokens: typeof outputTokens === 'number' ? outputTokens : null,
        });
      } catch (e) { /* never let stats crash a turn */ }
      debugLog.logTurnEnded(
        { request_id: requestId, conv_key: convKey, model_cursor: cursorModel },
        { inputTokens, outputTokens },
        stopReason, turnState.pendingToolCalls.length
      );
    },


    onError: (errMsg) => {
      console.error(`  ❌ ${errMsg}`);
      // Record the failed turn — pull the in-flight counters off the bridge.
      try {
        const bridge = getBridge && getBridge();
        const bs = bridge && typeof bridge.getStats === 'function' ? bridge.getStats() : {};
        runtimeStats.recordTurnEnd({
          model: cursorModel,
          outcome: 'fail',
          isStream: !!isStream,
          isContinuation: !!isContinuation,
          durationMs: timings && timings.t0 ? Date.now() - timings.t0 : null,
          firstFrameMs: timings ? timings.firstFrame : null,
          firstTextMs:  timings ? timings.firstText : null,
          firstToolMs:  timings ? timings.firstTool : null,
          maxIdleMs: typeof bs.maxIdleMs === 'number' ? bs.maxIdleMs : null,
          retries: bs.turnRetries || 0,
          transportErrors: bs.turnTransportErrors || 0,
          stalls: bs.turnStalls || 0,
          cascadeDetected: !!bs.turnCascadeDetected,
          toolCount: turnState.pendingToolCalls.length,
          inputTokens: typeof bs.inputTokens === 'number' ? bs.inputTokens : null,
          outputTokens: typeof bs.outputTokens === 'number' ? bs.outputTokens : null,
        });
      } catch (e) { /* never let stats crash error path */ }
      if (token) {
        tokenPool.release(token, {
          error: true,
          rateLimited: looksLikeRateLimit(errMsg),
        });
      }
      if (cachedEntry) {
        dropBridgeEntry(cachedEntry, 'error');
      } else {
        // Fresh-turn error before any entry was indexed — nothing in the
        // maps to clean up. We still try to delete bridgeKey/sessionId in
        // case a partial entry slipped in.
        activeBridges.delete(bridgeKey);
        if (sessionId) bridgesBySessionId.delete(sessionId);
      }

      debugLog.logCursorError({
        request_id: requestId,
        conv_key: convKey,
        bridge_key: bridgeKey,
        model_requested: requestedModel,
        model_cursor: cursorModel,
        is_stream: isStream,
        accumulated_text_len: (turnState.accumulatedText || '').length,
        accumulated_thinking_len: (turnState.accumulatedThinking || '').length,
        pending_tool_calls: turnState.pendingToolCalls.length,
        next_block_index: turnState.nextBlockIndex,
      }, errMsg);

      // "Blob not found" means our cached conversationState references blobs
      // Cursor's KV store has evicted. The state is no longer usable — drop
      // it so the NEXT request from this client re-derives a fresh context
      // instead of hitting the same error again.
      if (/blob not found/i.test(errMsg)) {
        conversationStates.delete(convKey);
        if (process.env.CURSOR_AGENT_DEBUG) {
          console.log(`  ↩️  evicted stale conversationState for convKey=${convKey} (blob expired upstream)`);
        }
      }

      const bridge = getBridge();
      try { bridge && bridge.close(); } catch {}

      // Classify the error — transient upstream failures (stalls, GOAWAY
      // cascades, REFUSED_STREAM) get an Anthropic-style mid-stream
      // `error` event so claude-code's SDK can recognize them as
      // retryable rather than treating the partial response as a
      // completed turn. Hard errors (auth, permission, validation, etc)
      // still land as content blocks the user can read directly.
      const isTransientUpstream =
        /Upstream stalled/i.test(errMsg) ||
        /Upstream stream ended before turnEnded/i.test(errMsg) ||
        /REFUSED_STREAM|INTERNAL_ERROR|GOAWAY|client (goaway|closed)/i.test(errMsg);

      if (isStream) {
        if (!res.writableEnded) {
          closeOpenBlock();
          if (isTransientUpstream && process.env.STALL_AS_OVERLOADED !== '0') {
            // Mid-stream `error` event — terminal per Anthropic SSE spec.
            // No message_delta/message_stop after this; the partial
            // response is implicitly discarded by the client.
            res.write(anthropicConverter.buildSseErrorEvent(errMsg, 'overloaded_error'));
            res.end();
          } else {
            // Legacy path: append error as content + end_turn.
            const idx = turnState.nextBlockIndex++;
            res.write(anthropicConverter.buildContentBlockStart(idx));
            res.write(anthropicConverter.buildContentBlockDelta(idx, `\n\n[Error: ${errMsg}]`));
            res.write(anthropicConverter.buildContentBlockStop(idx));
            res.write(anthropicConverter.buildMessageDelta('end_turn', 0));
            res.write(anthropicConverter.buildMessageStop());
            res.end();
          }
        }
      } else if (!res.headersSent) {
        const errType = isTransientUpstream ? 'overloaded_error' : 'api_error';
        const status = isTransientUpstream ? 529 : 500;
        res.status(status).json(anthropicConverter.buildAnthropicErrorResponse(errMsg, errType));
      }
    },
  };
}

function makeTurnState() {
  return {
    textBlockOpen: false,
    thinkingBlockOpen: false,
    thinkingBlockStarted: false,
    thinkingBlockIndex: null,
    thinkingAdapter: null,
    // Buffer of every text delta we've forwarded this turn — kept regardless
    // of isStream because we need it at end_turn to detect hallucinated
    // tool calls (`[Tool call: NAME({...})]`) the model emitted as text
    // instead of as structured tool_use. See parseHallucinatedToolCalls.
    emittedTextForDetection: '',
    // Same idea for thinking content. The model occasionally narrates tool
    // calls inside the thinking block instead of the response, and those
    // would otherwise escape the rescuer.
    emittedThinkingForDetection: '',
    // Tracks how many tool_use blocks we've already rescued out of the text
    // buffers. Lets the helper run multiple times per turn (e.g. at finalize
    // time AND at onTurnEnded) without re-rescuing the same hit.
    rescuedHitCount: 0,
    // Streams text-delta content through here before writing to the wire,
    // so `[Tool call: ...]` patterns the model emits as text never reach
    // claude-code (the rescue path still synthesizes structured tool_use
    // for them — this just hides the leftover bracketed text from the UI).
    hallucinationFilter: new StreamingHallucinationFilter(),
    nextBlockIndex: 0,
    pendingToolCalls: [],   // { execMsgId, execId, toolCallId, toolName, args, anthropicToolUseId, blockIndex }
    accumulatedText: '',
    accumulatedThinking: '',
  };
}

// Format a timings object as `⏱ key=ms,…`. Skips unstamped entries so the
// output stays compact even when only a subset of milestones fire (e.g. a
// no-text response has no firstText stamp).
function formatTimings(t) {
  if (!t || t.t0 == null) return '';
  const order = ['firstFrame', 'firstText', 'firstTool', 'turnEnded', 'respEnded'];
  const parts = [];
  for (const k of order) if (t[k] != null) parts.push(`${k}=${t[k]}ms`);
  return `⏱ ${parts.join(' ')}`;
}

// ── Path A: Resume the cached bridge with tool results ──
async function handleContinuation(req, res, cached, messages, requestedModel, cursorModel, isStream, bridgeKey) {
  const timings = { t0: Date.now() };
  const toolResults = anthropicTools.extractToolResults(messages);

  if (toolResults.length === 0) {
    return res.status(400).json(
      anthropicConverter.buildAnthropicErrorResponse('Continuation request has no tool_result blocks', 'invalid_request_error')
    );
  }

  console.log(`  🔄 continuation | toolResults=${toolResults.length} | pendingExecs=${cached.pendingExecs.length}`);

  if (isStream) {
    setSSEHeaders(res);
    res.write(anthropicConverter.buildMessageStart(requestedModel, 0));
    res.write(anthropicConverter.buildPing());
  }

  const turnState = makeTurnState();

  const callbacks = buildTurnCallbacks({
    res, isStream, turnState,
    convKey: cached.convKey,
    // Use the EXACT bridgeKey the caller computed (with system + remoteAddr
    // already mixed in) so re-caching after another tool round-trip matches
    // the original entry.
    bridgeKey,
    conversationId: cached.conversationId,
    sessionId: cached.sessionId,
    requestedModel, cursorModel,
    mcpTools: cached.mcpTools,
    getBridge: () => cached.bridge,
    requestId: cached.requestId,
    clientThinkingEnabled: !!cached.clientThinkingEnabled,
    // Pass the existing entry so finalize/onTurnEnded mutate it in place
    // (preserving the bridgeKeys alias set) instead of creating a new one.
    cachedEntry: cached,
    isContinuation: true,  // continuation handler
    timings,
  });

  // Re-bind bridge callbacks so events from this resumed turn drive the
  // new HTTP response, not the closed one from the previous request.
  cached.bridge.setCallbacks({
    onTextDelta: callbacks.onTextDelta,
    onThinkingDelta: callbacks.onThinkingDelta,
    onMcpCall: callbacks.onMcpCall,
    onStepCompleted: callbacks.onStepCompleted,
    onTurnEnded: callbacks.onTurnEnded,
    onError: callbacks.onError,
  });

  // Build a quick lookup for tool_use_id → result and feed each pending exec.
  const resultById = new Map();
  for (const r of toolResults) resultById.set(r.toolUseId, r);

  for (const exec of cached.pendingExecs) {
    const matching = resultById.get(exec.anthropicToolUseId);
    let payload;
    if (matching) {
      if (matching.isError) {
        payload = { error: matching.content || 'Tool execution failed' };
      } else if (Array.isArray(matching.contentItems) && matching.contentItems.length > 0) {
        // Pass the structured items so image blocks survive the round-trip
        // (otherwise screenshots/PNG content from Read or Bash get dropped).
        payload = { items: matching.contentItems };
      } else {
        payload = matching.content;
      }
    } else {
      payload = { error: 'Tool result not provided by client' };
    }
    cached.bridge.sendToolResult(exec.execMsgId, exec.execId, payload);
  }

  // Clear the cached pending list — the new turn may add more.
  cached.pendingExecs = [];
  cached.lastAccessMs = Date.now();

  // Cleanup if the caller bails before turnEnded.
  req.on('close', () => {
    if (res.writableEnded) return;
    // Don't close the bridge on disconnect — the model may still be running
    // and the next request will re-bind callbacks. Eviction will reap it.
  });
}

// ── Path B: Open a fresh Cursor conversation ──
async function handleFreshTurn(req, res, token, params) {
  const {
    messages, system, requestedModel, cursorModel, isStream,
    convKey, bridgeKey, conversationId, tools,
  } = params;
  const requestBody = params.body || req.body || {};

  const timings = { t0: Date.now() };
  // Pull any prior-turn thinking we've stored for this conversation, so
  // anthropicMessagesToPrompt can inject it back as `<thinking>` context
  // (opt-in via CURSOR_REINJECT_THINKING=1; otherwise empty array).
  const thinkingHist = thinkingHistory.getHistory(convKey);
  const prompt = anthropicConverter.anthropicMessagesToPrompt(messages, system, {
    thinkingHistory: thinkingHist,
    clientTools: tools,
  });
  // The assistant-message index this new turn will land at — used by the
  // onTurnEnded handler to key the recorded thinking by position.
  const newAssistantTurnIdx = (messages || []).filter(m => m && m.role === 'assistant').length;
  const mcpTools = anthropicTools.anthropicToolsToMcpTools(tools, 'cursoride2api');

  // Diagnostic-only: dump the EXACT flattened prompt we send to Cursor so we
  // can verify our tagging / framing actually reaches the model. Off by default;
  // set DUMP_PROMPTS=1 to enable. Each file is timestamped + reqId-tagged for
  // easy correlation with /tmp/v1messages-*.json bodies.
  if (process.env.DUMP_PROMPTS) {
    try {
      const reqId = params.requestId || 'unk';
      const dumpFile = `/tmp/prompt-${Date.now()}-${reqId}.txt`;
      const header =
        `# request_id=${reqId}\n` +
        `# conv_key=${convKey}\n` +
        `# bridge_key=${bridgeKey}\n` +
        `# requested_model=${requestedModel}\n` +
        `# cursor_model=${cursorModel}\n` +
        `# message_count=${messages.length}\n` +
        `# prompt_bytes=${prompt.length}\n` +
        `# tool_count=${(tools||[]).length}\n` +
        `# ─── BEGIN PROMPT ───\n`;
      require('fs').writeFileSync(dumpFile, header + prompt);
      console.log(`  📋 dumped prompt to ${dumpFile} | bytes=${prompt.length} | msgs=${messages.length}`);
    } catch (e) { /* ignore */ }
  }

  // Do NOT load cached state — see onTurnEnded note. Cursor's blob store is
  // per-H2-stream and replaying a stale checkpoint triggers Blob not found.
  const conversationState = null;

  if (isStream) {
    setSSEHeaders(res);
    res.write(anthropicConverter.buildMessageStart(requestedModel, 0));
    res.write(anthropicConverter.buildPing());
  }

  const turnState = makeTurnState();
  const clientThinkingEnabled =
    requestBody && requestBody.thinking &&
    (requestBody.thinking.type === 'enabled' || requestBody.thinking === 'enabled');

  // sessionId is a per-bridge uuid baked into every tool_use_id we mint. It
  // lets continuations find this bridge across TCP socket reconnects (the
  // bridgeKey lookup misses when the client's keepAliveTimeout expires and
  // it reconnects on a new remotePort).
  const sessionId = uuidv4();

  // Forward declaration — `bridge` is assigned below but the callbacks need
  // a stable getter so `getBridge()` returns the right object.
  let bridge = null;
  const callbacks = buildTurnCallbacks({
    res, isStream, turnState,
    convKey, bridgeKey, conversationId, sessionId,
    requestedModel, cursorModel, mcpTools,
    getBridge: () => bridge,
    requestId: params.requestId,
    isContinuation: false,  // fresh-turn handler
    timings,
    token,
    // The position this turn's assistant message will occupy in the
    // conversation history — used to key recorded thinking content.
    assistantTurnIdx: newAssistantTurnIdx,
    clientThinkingEnabled,
  });

  bridge = cursorAgent.startConversation(token, {
    prompt,
    modelId: cursorModel,
    conversationId,
    conversationState,
    tools: mcpTools,
    // Reuse the per-bridge sessionId we already minted for tool_use_id
    // encoding: it now also serves as the IDE-style x-session-id header
    // so Cursor's backend can schedule this stream independently.
    sessionId,
    onTextDelta: callbacks.onTextDelta,
    onThinkingDelta: callbacks.onThinkingDelta,
    onMcpCall: callbacks.onMcpCall,
    onStepCompleted: callbacks.onStepCompleted,
    onTurnEnded: callbacks.onTurnEnded,
    onError: callbacks.onError,
    passthroughNativeTools: PASSTHROUGH_NATIVE_TOOLS,
  });

  // NOTE: We intentionally do NOT register req.on('close') here. In some Node
  // versions the 'close' event on the incoming request can fire as soon as the
  // body has been fully consumed by express.json(), which would close our
  // outgoing H2 stream prematurely and kill the in-flight Cursor request.
  // The TTL eviction loop is responsible for reaping stale bridges.
}

// Shared Anthropic Messages handler. /v1/messages uses it directly; OpenAI
// Chat/Responses tool-compatible routes call it through a local adapter.
async function handleAnthropicMessagesRequest(req, res) {
  const body = req.body || {};
  const { messages, model, system, max_tokens, stream, temperature, top_p, stop_sequences, tools } = body;

  if (shouldRouteAnthropicToRatlc(model)) {
    console.log(`  🧭 route=ratlc-pool /v1/messages | ${model || DEFAULT_MODEL} → ${RATLC_POOL_URL}`);
    const result = await proxyToRatlc(req, res, '/v1/messages', body, { model: model || DEFAULT_MODEL });
    if (result.proxied || !RATLC_FALLBACK_TO_DIRECT) return;
    if (res.headersSent) return;
    console.warn(`  ⚠️ RATLC proxy failed (${result.error}); falling back to direct H1`);
  }

  // Optional debug dump
  if (process.env.DUMP_REQUESTS) {
    try {
      const dumpFile = `/tmp/v1messages-${Date.now()}-${Math.random().toString(36).slice(2,6)}.json`;
      require('fs').writeFileSync(dumpFile, JSON.stringify(body, null, 2));
      console.log(`  📋 dumped request to ${dumpFile} | bytes=${JSON.stringify(body).length} | tools=${(tools||[]).length} | msgs=${(messages||[]).length}`);
    } catch (e) { /* ignore */ }
  }

  if (!messages || !Array.isArray(messages) || messages.length === 0) {
    debugLog.warn('rejected_request', {
      reason: 'messages is required',
      method: req.method, path: req.path, remote: req.ip,
      body_keys: Object.keys(body),
    });
    return res.status(400).json(anthropicConverter.buildAnthropicErrorResponse('messages is required', 'invalid_request_error'));
  }

  const requestedModel = model || DEFAULT_MODEL;

  // Run all preprocessing (compaction detect, subagent marker, IDE-tool
  // sanitize) in one pass so server.js only sees a single decision object.
  const pre = preprocess.preprocessAnthropicRequest(body);

  // Cursor has no Claude Haiku model. We previously routed warmup pings to
  // `composer-2-fast` (Cursor's own Kimi-K2.5 Composer 2, not Claude). All
  // claude-haiku-* requests now upgrade to a real Claude — adjustable via
  // SMALL_MODEL env var.
  let effectiveModel = requestedModel;
  let routingReason = null;
  if (/^claude-haiku/i.test(requestedModel)) {
    effectiveModel = SMALL_MODEL;
    routingReason = 'haiku-upgrade';
  }
  // Compaction calls (Claude Code's /compact, OpenCode's anchor summarizer)
  // don't need full Sonnet/Opus. Route to small model and log.
  if (pre.compactType === preprocess.COMPACT_REQUEST) {
    effectiveModel = SMALL_MODEL;
    routingReason = 'compact-request';
  } else if (pre.compactType === preprocess.COMPACT_AUTO_CONTINUE) {
    // Auto-continue happens AFTER a compact summary lands; the model is
    // re-attaching to the truncated history. Real model semantics matter
    // here (it has to keep coding), so we leave the model as-is but mark it.
    routingReason = 'compact-auto-continue';
  }
  // Subagent traffic — opt-in downgrade.
  if (pre.subagentMarker) {
    if (SUBAGENT_USE_SMALL_MODEL) {
      effectiveModel = SMALL_MODEL;
      routingReason = `subagent (${pre.subagentMarker.agent_type})`;
    } else if (!routingReason) {
      routingReason = `subagent passthrough (${pre.subagentMarker.agent_type})`;
    }
  }
  // Strip the `-no-thinking` client-side marker before mapping. claude-code's
  // CLI lacks a --no-thinking flag, so users embed the signal in --model.
  // extractModelOverrides below also detects it (and sets thinkingType=disabled);
  // here we just normalize the lookup name so e.g. claude-opus-4-7-no-thinking
  // hits the claude-opus-4-7 entry in anthropicModelMapping.
  const modelForLookup = anthropicConverter.stripNoThinkingSuffix(effectiveModel);
  let cursorModel = anthropicConverter.mapAnthropicModel(modelForLookup, config.anthropicModelMapping);
  // Honor claude-code's --effort flag (carried in body.output_config.effort)
  // and the Anthropic-API thinking parameter. Without this hook every
  // request gets the static thinking-max mapping regardless of what the
  // user asked for.
  const overrides = anthropicConverter.extractModelOverrides(body);
  if (overrides.effort || overrides.thinkingType) {
    const before = cursorModel;
    cursorModel = anthropicConverter.applyModelOverrides(cursorModel, overrides);
    if (before !== cursorModel) {
      const segs = [];
      if (overrides.effort) segs.push(`effort=${overrides.effort}`);
      if (overrides.thinkingType) segs.push(`thinking=${overrides.thinkingType}`);
      routingReason = (routingReason ? routingReason + ' | ' : '') + `override(${segs.join(', ')}): ${before} → ${cursorModel}`;
    }
  }
  const isStream = stream === true;

  // Conversation/bridge cache keys.
  //
  // Preferred path: pull claude-code's stable per-conversation UUID from
  // `x-claude-code-session-id` (or its `body.metadata.user_id.session_id`
  // mirror) — this is the only signal we have that survives two clients
  // sharing remoteAddr+remotePort+firstUserText, which DOES happen in
  // practice (e.g. two simultaneous `claude -p 'foo'` invocations on the
  // same host can both reach us through one keep-alive socket).
  //
  // Fallback path (no session header): salt with remoteAddr + remotePort +
  // tool-list hash + first-user-text. Less robust but the historical
  // behavior — preserved for non-claude-code callers and to keep this
  // change additive. Without the salt, two concurrent same-prompt sessions
  // would clobber each other's H2 stream (the second's `continuation=false`
  // overwrites the first's bridge entry; the first then routes its
  // tool_result onto the wrong stream and hangs).
  const remoteAddr = (req.ip || req.socket?.remoteAddress || '').toString();
  const remotePort = req.socket?.remotePort;
  req.body = body;
  const clientSessionId = anthropicTools.extractClientSessionId(req);
  const convKey = anthropicTools.deriveConversationKey(messages, cursorModel, system, tools, remoteAddr, remotePort, clientSessionId);
  const bridgeKey = anthropicTools.deriveBridgeKey(cursorModel, messages, system, tools, remoteAddr, remotePort, clientSessionId);
  const conversationId = anthropicTools.deterministicConversationId(convKey);

  const isContinuation = anthropicTools.hasToolResults(messages);

  const requestId = uuidv4().slice(0, 8);
  const reasonLabel = routingReason ? ` | ${routingReason}` : '';
  console.log(
    `  📨 [${new Date().toLocaleTimeString()}] (Anthropic) ${requestedModel} → ${cursorModel} | ` +
    `stream=${isStream} | continuation=${isContinuation} | convKey=${convKey} | ` +
    `clientSessionId=${clientSessionId ? clientSessionId.slice(0, 8) + '…' : '(none)'} | reqId=${requestId}${reasonLabel}`
  );

  debugLog.logRequest(req, body, {
    requestId,
    requestedModel, effectiveModel, cursorModel,
    convKey, bridgeKey, isContinuation,
    compactType: pre.compactType,
    subagentMarker: pre.subagentMarker,
    routingReason,
  });

  // Path A: continuation — caller is delivering tool_result blocks.
  if (isContinuation) {
    // Try sessionId-based lookup first. The client's tool_use_id encodes the
    // bridge's sessionId — stable across TCP reconnects (undici closes idle
    // sockets after 4 s, the next request lands on a new remotePort, which
    // would otherwise miss the bridgeKey-indexed cache).
    let cached = null;
    let cacheHitVia = '';
    const probeResults = anthropicTools.extractToolResults(messages);
    if (probeResults.length > 0 && probeResults[0].sessionId) {
      cached = bridgesBySessionId.get(probeResults[0].sessionId);
      if (cached) cacheHitVia = `sessionId=${probeResults[0].sessionId.slice(0,8)}`;
    }
    if (!cached) {
      cached = activeBridges.get(bridgeKey);
      if (cached) cacheHitVia = `bridgeKey=${bridgeKey}`;
    }
    if (cached) {
      // User-injected-content rescue. handleContinuation forwards only
      // tool_result blocks to Cursor's open stream; any text/image/etc.
      // content the user attached alongside the tool_results would be
      // silently dropped. Detect that case and force a cache miss so the
      // request flows through handleFreshTurn with the full message
      // history. Cursor then sees the new instruction in the rebuilt
      // prompt. Verified failure mode: model continues prior task,
      // ignoring the new user instruction.
      const lastUser = anthropicTools.findLatestUserMessage(messages);
      const hasUserInjection =
        lastUser && Array.isArray(lastUser.content) &&
        lastUser.content.some(b => b && b.type && b.type !== 'tool_result');
      if (hasUserInjection) {
        const injectedTypes = [...new Set(lastUser.content.map(b => b && b.type).filter(t => t && t !== 'tool_result'))];
        console.log(`  ↪️  user-injected content alongside tool_result (${injectedTypes.join(',')}); forcing fresh-turn so the new instruction reaches the model`);
        dropBridgeEntry(cached, 'user-injected-content');
        cached = null;
      }
    }
    if (cached) {
      cached.lastAccessMs = Date.now();
      cached.requestId = requestId;
      if (process.env.CURSOR_AGENT_DEBUG) console.log(`  ↪️  continuation cache hit via ${cacheHitVia}`);
      return handleContinuation(req, res, cached, messages, requestedModel, cursorModel, isStream, bridgeKey);
    }
    // Cache miss — bridge died, was evicted, or we just dropped it on
    // purpose because the user injected new content alongside the
    // tool_results. Fall through to a fresh turn using the full message
    // history (system + alternations) as the prompt.
    console.log('  ⚠️  Bridge cache miss for continuation; starting fresh');
    debugLog.warn('continuation_cache_miss', { request_id: requestId, conv_key: convKey, bridge_key: bridgeKey });
  }

  // Path B: fresh turn — only NOW do we burn a token. Cache-hit continuations
  // reuse the bridge's existing connection; calling tokenPool.pick() upfront
  // would inflate the chosen slot's activeRequests without using it and skew
  // health-aware load distribution for unrelated fresh requests.
  const token = tokenPool.pick();
  if (!token) {
    debugLog.error('no_tokens_available', { reason: 'No available tokens', method: req.method, path: req.path });
    return res.status(503).json(anthropicConverter.buildAnthropicErrorResponse('No available tokens', 'api_error'));
  }
  // Release on response close. release() is idempotent, so the bridge's
  // onError/onTurnEnded callbacks below can also call it with a more accurate
  // {error, rateLimited} verdict — whichever fires first wins, the second is
  // a no-op. This guarantees we never leak activeRequests even if a bridge
  // callback is somehow missed (e.g. abrupt H2 client tear-down).
  res.on('close', () => tokenPool.release(token, { success: true }));
  return handleFreshTurn(req, res, token, {
    messages, system, requestedModel, cursorModel, isStream,
    convKey, bridgeKey, conversationId, tools, requestId, body,
  });
}

// ── POST /v1/messages (Anthropic Messages API, full tool-use) ──
app.post('/v1/messages', checkApiKey, handleAnthropicMessagesRequest);

// ── POST /v1/messages/count_tokens (Anthropic Messages API) ──
//
// Claude Code calls this to plan things like whether a /btw fork can fit
// in the context window. Without it the call 404s and Claude Code can
// silently abort the operation. Anthropic's real endpoint returns
//   { "input_tokens": <int> }
// We approximate by character-count heuristic (~3.5 chars per token for
// Claude). Good enough for fit-check purposes; not for billing.
function _countCharsRecursive(value) {
  if (value == null) return 0;
  if (typeof value === 'string') return value.length;
  if (typeof value === 'number') return String(value).length;
  if (Array.isArray(value)) {
    let n = 0;
    for (const item of value) n += _countCharsRecursive(item);
    return n;
  }
  if (typeof value === 'object') {
    let n = 0;
    for (const k of Object.keys(value)) {
      // Field-name overhead matters slightly — count it
      n += k.length;
      n += _countCharsRecursive(value[k]);
    }
    return n;
  }
  return 0;
}

app.post('/v1/messages/count_tokens', checkApiKey, async (req, res) => {
  const body = req.body || {};
  const { messages, system, tools } = body;

  if (RATLC_ENABLED && RATLC_ROUTE_COUNT_TOKENS && shouldRouteAnthropicToRatlc(body.model)) {
    console.log(`  🧭 route=ratlc-pool /v1/messages/count_tokens | ${body.model || DEFAULT_MODEL} → ${RATLC_POOL_URL}`);
    const result = await proxyToRatlc(req, res, '/v1/messages/count_tokens', body, { model: body.model || DEFAULT_MODEL });
    if (result.proxied || !RATLC_FALLBACK_TO_DIRECT) return;
    if (res.headersSent) return;
    console.warn(`  ⚠️ RATLC count_tokens proxy failed (${result.error}); using local estimator`);
  }

  if (!messages || !Array.isArray(messages)) {
    return res.status(400).json(anthropicConverter.buildAnthropicErrorResponse(
      'messages is required', 'invalid_request_error'
    ));
  }

  // Sum chars across system, messages, tools.
  let chars = 0;
  if (system != null) chars += _countCharsRecursive(system);
  chars += _countCharsRecursive(messages);
  if (Array.isArray(tools)) chars += _countCharsRecursive(tools);

  // ~3.5 chars per token for Claude is a decent estimate for English code/text.
  // Round up so we never under-report (which could cause Claude Code to
  // think a fork fits when it doesn't).
  const inputTokens = Math.ceil(chars / 3.5);

  if (debugLog.isEnabled()) {
    debugLog.info('count_tokens', {
      msg_count: messages.length,
      tool_count: Array.isArray(tools) ? tools.length : 0,
      has_system: system != null,
      chars,
      input_tokens: inputTokens,
    });
  }

  res.json({ input_tokens: inputTokens });
});

// ── 健康检查 ──
app.get('/health', async (req, res) => {
  const conn = runtimeStats.getConnectionStats({ recent: 0 });
  const ratlcHealth = RATLC_ENABLED ? await getRatlcHealth() : { reachable: false, error: RATLC_POOL_URL ? 'no_models_configured' : 'not_configured' };
  res.json({
    ok: true,
    status: 'ok',
    tokens: tokenPool.stats(),
    tokenCount: tokenPool.size(),
    defaultModel: DEFAULT_MODEL,
    passthroughNativeTools: PASSTHROUGH_NATIVE_TOOLS,
    upstreamTransport: CURSOR_UPSTREAM_TRANSPORT,
    downstreamProtocol: CURSOR_DOWNSTREAM_PROTOCOL,
    directServerProtocol: 'http/1.1',
    ratlc: {
      enabled: RATLC_ENABLED,
      poolUrl: RATLC_POOL_URL || null,
      routeMessages: RATLC_ROUTE_MESSAGES,
      routeCountTokens: RATLC_ROUTE_COUNT_TOKENS,
      fallbackToDirect: RATLC_FALLBACK_TO_DIRECT,
      models: RATLC_MODELS,
      ...ratlcHealth,
    },
    stallThresholds: stallThresholds.getStats(),
    runtime: runtimeStats.getStats({ window: 'last1h' }).totals,
    connections: {
      currentlyOpen: conn.currentlyOpen,
      totalOpens: conn.totalOpens,
      totalCloses: conn.totalCloses,
      closeReasonCounts: conn.closeReasonCounts,
    },
    version: '2.0.0',
  });
});

// ── Runtime stats endpoint ──
//
// Per-model aggregates with t-digest distributions. Supports time windows
// (?window=lifetime|last1h|last24h|<ms>), model filtering (?model=...), and
// grouping (?groupBy=model|modelMode|mode).
// Persists across restarts via logs/runtime-stats.json (override path with
// RUNTIME_STATS_FILE).
app.get('/stats', checkApiKey, (req, res) => {
  const windowParam = req.query.window;
  const window = windowParam == null
    ? 'lifetime'
    : /^\d+$/.test(windowParam) ? parseInt(windowParam, 10) : windowParam;
  const model = req.query.model || undefined;
  const groupBy = req.query.groupBy || 'model';
  res.json(runtimeStats.getStats({ window, model, groupBy }));
});

// ── Connection lifecycle stats ──
//
// Per-pool-client lifetimes, streams-served distribution, close-reason
// breakdown, per-slot churn. Useful for diagnosing connection rot from
// Cursor's LB rotating connections, GOAWAY frequency, etc.
app.get('/stats/connections', checkApiKey, (req, res) => {
  const recentN = req.query.recent != null ? parseInt(req.query.recent, 10) : 50;
  res.json(runtimeStats.getConnectionStats({ recent: Number.isFinite(recentN) ? recentN : 50 }));
});

// ── Live in-flight bridge state ──
//
// Answers the question "right now, is this turn thinking or stuck?" without
// having to grep proxy logs. For each active bridge whose turn isn't
// already ended, returns: model, idle time since last useful frame, current
// stall threshold, when the watchdog will trip, plus delta counts and byte
// flow so you can tell heartbeats-only (stuck) apart from active progress.
//
// Heuristic for the JSON consumer: if `thinkingDeltaCount` or `textDeltaCount`
// is rising between polls, the model is producing. If those counts are flat
// and `bytesInSinceLastUsefulFrame` is also flat, only heartbeats are flowing
// — the upstream is silent and `willTripStallInMs` is the budget left.
app.get('/stats/inflight', checkApiKey, (req, res) => {
  const seen = new Set();   // dedupe entries that have multiple bridgeKey aliases
  const bridges = [];
  for (const [bridgeKey, entry] of activeBridges) {
    if (!entry || seen.has(entry)) continue;
    seen.add(entry);
    let s = null;
    try { s = entry.bridge && typeof entry.bridge.getStats === 'function' ? entry.bridge.getStats() : null; }
    catch { /* ignore */ }
    if (!s) continue;
    // "In-flight" = not closed AND turn isn't ended (or the bridge is mid-
    // continuation idle window — useful to surface either way).
    const inFlight = !s.closed && !s.turnEndedFired;
    bridges.push({
      bridgeKey,
      sessionId: entry.sessionId ? entry.sessionId.slice(0, 8) : null,
      requestId: entry.requestId || null,
      model: s.modelId || entry.cursorModel,
      requestedModel: entry.requestedModel,
      inFlight,
      closed: s.closed,
      turnEndedFired: s.turnEndedFired,
      hasEmittedContent: s.hasEmittedContent,
      openedMsAgo: s.openedMsAgo,
      idleMsSinceLastUsefulFrame: s.idleMsSinceLastUsefulFrame,
      currentThresholdMs: s.currentThresholdMs,
      currentThresholdKind: s.currentThresholdKind,
      willTripStallInMs: s.willTripStallInMs,
      stallThresholdSource: s.stallThresholdSource,
      retryAttempts: s.retryAttempts,
      bytesInTotal: s.bytesInTotal,
      bytesInSinceLastUsefulFrame: s.bytesInSinceLastUsefulFrame,
      bytesOutTotal: s.bytesOutTotal,
      textDeltaCount: s.textDeltaCount,
      thinkingDeltaCount: s.thinkingDeltaCount,
      mcpCallCount: s.mcpCallCount,
      inputTokens: s.inputTokens,
      outputTokens: s.outputTokens,
      maxIdleMs: s.maxIdleMs,
      turnRetries: s.turnRetries,
      turnStalls: s.turnStalls,
      turnTransportErrors: s.turnTransportErrors,
      turnCascadeDetected: s.turnCascadeDetected,
      lastAccessMsAgo: entry.lastAccessMs ? (Date.now() - entry.lastAccessMs) : null,
    });
  }
  // Sort: in-flight first, then by largest idleMs so the most concerning
  // bridge surfaces at the top of the list.
  bridges.sort((a, b) => {
    if (a.inFlight !== b.inFlight) return a.inFlight ? -1 : 1;
    return (b.idleMsSinceLastUsefulFrame || 0) - (a.idleMsSinceLastUsefulFrame || 0);
  });
  res.json({
    now: Date.now(),
    totalBridges: bridges.length,
    inFlightCount: bridges.filter(b => b.inFlight).length,
    bridges,
  });
});

// ── 启动 ──
const count = loadTokens();
watchTokenFile();

// Pre-warm the shared H2 client + proto schemas so the first /v1/messages
// request doesn't pay the TLS handshake / proto load latency. The H1
// RunSSE/BidiAppend transport opens per-turn streams, so it has no shared
// client prewarm hook.
if (typeof cursorAgent.prewarmSharedClient === 'function') {
  cursorAgent.prewarmSharedClient();
}

// Start runtime-stats persistence (snapshot to logs/runtime-stats.json
// every minute and on graceful shutdown).
runtimeStats.startPersistence();

// Periodic stats summary line. Off by default; set RUNTIME_STATS_LOG_INTERVAL_MS
// to a positive number of ms to enable. Useful on long-running proxies for
// at-a-glance health without polling /stats.
const _statsLogIntervalMs = parseInt(process.env.RUNTIME_STATS_LOG_INTERVAL_MS || '0', 10);
if (_statsLogIntervalMs > 0) {
  setInterval(() => {
    const s = runtimeStats.getStats({ window: 'last1h' });
    if (!s.totals.total) return;
    const t = s.totals;
    const succPct = ((t.successRate || 0) * 100).toFixed(1);
    const firstPct = ((t.firstTryRate || 0) * 100).toFixed(1);
    // Pick the busiest model in the last hour for a model-specific slice.
    const top = Object.entries(s.models).sort((a, b) => b[1].total - a[1].total)[0];
    const topStr = top ? ` | top ${top[0]}: ${top[1].total} (p95dur=${top[1].durationMs.p95}ms p95first=${top[1].firstFrameMs.p95}ms)` : '';
    console.log(
      `  📈 runtime/last1h | turns=${t.total} succ=${succPct}% firstTry=${firstPct}% retries=${t.totalRetries} stalls=${t.totalStalls} cascades=${t.totalCascades}${topStr}`
    );
  }, _statsLogIntervalMs).unref();
}

app.listen(PORT, HOST, () => {
  console.log('');
  console.log('  ╔═══════════════════════════════════════════╗');
  console.log('  ║       CursorIDE2API v2.0 (Lite)           ║');
  console.log('  ╠═══════════════════════════════════════════╣');
  console.log(`  ║  🌐 http://${HOST}:${PORT}                     ║`);
  console.log(`  ║  🔌 /v1/messages (Anthropic)               ║`);
  console.log(`  ║  🔌 /v1/messages/count_tokens             ║`);
  console.log(`  ║  🔌 /v1/responses                         ║`);
  console.log(`  ║  🔌 /v1/tools                             ║`);
  console.log(`  ║  📋 /v1/models                            ║`);
  console.log('  ╠═══════════════════════════════════════════╣');
  console.log(`  ║  🔑 Tokens: ${String(count).padEnd(30)}║`);
  console.log(`  ║  🤖 Default: ${DEFAULT_MODEL.padEnd(29)}║`);
  console.log(`  ║  🔐 API Key: ${(API_KEY ? 'SET' : 'OPEN (no key)').padEnd(29)}║`);
  console.log(`  ║  🔁 Cursor upstream: ${CURSOR_UPSTREAM_TRANSPORT.toUpperCase().padEnd(21)}║`);
  console.log(`  ║  ↕️ Downstream: ${CURSOR_DOWNSTREAM_PROTOCOL.padEnd(27)}║`);
  console.log(`  ║  🖥️ Direct server: ${'http/1.1'.padEnd(25)}║`);
  console.log(`  ║  🧭 RATLC route: ${(RATLC_ENABLED && RATLC_ROUTE_MESSAGES ? 'ON' : 'OFF').padEnd(26)}║`);
  if (RATLC_ENABLED) {
    console.log(`  ║  🧭 RATLC models: ${String(RATLC_MODELS.length).padEnd(25)}║`);
  }
  console.log(`  ║  🧰 Native tools: ${(PASSTHROUGH_NATIVE_TOOLS ? 'PASSTHROUGH' : 'SAFE-REJECT').padEnd(24)}║`);
  if (debugLog.isEnabled()) {
    console.log(`  ║  📝 Debug: ${(debugLog.isVerbose() ? 'verbose' : 'on').padEnd(31)}║`);
  }
  console.log(`  ║  💭 Proxy thinking: ${(PROXY_THINKING_BLOCKS ? 'ON (proxy-local)' : 'OFF').padEnd(24)}║`);
  console.log('  ╚═══════════════════════════════════════════╝');
  console.log('');
});

// ── 优雅退出 ──
process.on('SIGINT', () => { console.log('\n  Bye!'); process.exit(0); });
process.on('SIGTERM', () => process.exit(0));
