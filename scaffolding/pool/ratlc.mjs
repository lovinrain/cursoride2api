#!/usr/bin/env node
// ratlc — unified CLI for the Retry-based Agentic Tool-Looping Conversation pool.
//
// Subcommands:
//   up [size]            Start pool + api-server (replaces start.sh)
//   down                 Stop pool + api-server cleanly
//   status               One-shot snapshot
//   watch [interval]     Refreshing status (existing behavior)
//   tui                  Full-screen dashboard with hotkeys (r/R/k/q)
//   tail                 Live-tail pool log, filtered to significant events
//   ramp <N> [--group=M] Add N channels (positive) or remove |N| (negative); --group defaults to default group
//   restart [<ch>]       Restart specific channel (or any-stuck one if omitted)
//   metrics              JSON metrics snapshot
//   claude [--model X] [args...]   Spawn claude-code with auto-wait-for-ready + ANTHROPIC_MODEL forwarded
//   logs                 Print log file paths
//   groups               Print per-group breakdown
//   add-group <model> <N>   Register a new model group with target N channels
//   remove-group <model>    Drain & remove a non-default model group
//
// Env vars (forwarded when up):
//   POOL_SIZE                Default 2
//   POOL_TOOL_MODE           contract|translate (default contract)
//   POOL_CONCURRENT_OPENS    1..3 (default 1; raise for faster bring-up)
//   POOL_MODEL               default claude-opus-4-7-thinking-max-fast
//   TOOL_INCLUDE             comma-separated tool allowlist (contract mode)
//   IDLE_PING_MS             default 1200000 (20 min)

import net from 'node:net';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { spawn, exec } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { isThinking, isWaitTool, resolveWatchdog, stateLabel, silentCell, countSplit } from './tui-format.mjs';
const execp = promisify(exec);

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO = path.resolve(__dirname, '..', '..');

const POOL_SOCK = process.env.POOL_SOCK || '/tmp/ratlc-pool.sock';
const POOL_PID = '/tmp/ratlc-pool.pid';
const API_PID = '/tmp/ratlc-api.pid';
const POOL_LOG = process.env.RATLC_POOL_LOG || '/tmp/ratlc-pool.log';
const API_LOG = process.env.RATLC_API_LOG || '/tmp/ratlc-api.log';
const RATLC_API_HOST = process.env.RATLC_API_HOST || '127.0.0.1';
const RATLC_API_PORT = process.env.RATLC_API_PORT || '4242';
const API_URL = process.env.RATLC_API_URL || `http://${RATLC_API_HOST}:${RATLC_API_PORT}`;

// ── ANSI helpers ─────────────────────────────────────────────────────────
const ANSI = {
  reset: '\x1b[0m', bold: '\x1b[1m', dim: '\x1b[2m',
  red: '\x1b[31m', green: '\x1b[32m', yellow: '\x1b[33m',
  blue: '\x1b[34m', cyan: '\x1b[36m', gray: '\x1b[90m',
  clear: '\x1b[2J\x1b[H', clearLine: '\x1b[2K',
  hideCursor: '\x1b[?25l', showCursor: '\x1b[?25h',
  altScreen: '\x1b[?1049h', restoreScreen: '\x1b[?1049l',
};
function color(s, c) { return c + s + ANSI.reset; }
// STATE_COLOR, isThinking, isWaitTool, resolveWatchdog, stateLabel, silentCell,
// and countSplit moved to ./tui-format.mjs — the single source of truth shared
// with ratlc-ctl.mjs so the three views can never drift apart.

// ── IPC: ask the pool for status ─────────────────────────────────────────
function poolRequest(obj, timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    const sock = net.createConnection(POOL_SOCK);
    let buf = '';
    const t = setTimeout(() => { try { sock.destroy(); } catch {} reject(new Error('timeout')); }, timeoutMs);
    sock.on('connect', () => sock.write(JSON.stringify(obj) + '\n'));
    sock.on('data', (c) => {
      buf += c.toString();
      const idx = buf.indexOf('\n');
      if (idx === -1) return;
      try {
        const m = JSON.parse(buf.slice(0, idx));
        clearTimeout(t); sock.end(); resolve(m);
      } catch { /* incomplete */ }
    });
    sock.on('error', (e) => { clearTimeout(t); reject(e); });
  });
}

async function getStatus() { return poolRequest({ type: 'status' }); }
async function getGroups() {
  const r = await poolRequest({ type: 'list_groups' });
  return r?.groups || [];
}

async function getHealth() {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('timeout')), 5000);
    http.get(API_URL + '/health', (res) => {
      let buf = '';
      res.on('data', (c) => { buf += c.toString(); });
      res.on('end', () => { clearTimeout(t); try { resolve(JSON.parse(buf)); } catch (e) { reject(e); } });
    }).on('error', (e) => { clearTimeout(t); reject(e); });
  });
}

async function getStats() {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('timeout')), 5000);
    http.get(API_URL + '/v1/_stats', (res) => {
      let buf = '';
      res.on('data', (c) => { buf += c.toString(); });
      res.on('end', () => {
        clearTimeout(t);
        // An api-server too old to have /v1/_stats answers 404 with a VALID-JSON
        // error body ({"error":"not found"}). Reject on any non-2xx so callers
        // see "unavailable" instead of a truthy error object masquerading as a
        // snapshot (which renders regime=? hour=undefined). See adversarial Gap #1.
        if ((res.statusCode || 0) >= 300) { reject(new Error('HTTP ' + res.statusCode)); return; }
        try {
          const j = JSON.parse(buf);
          if (!j || typeof j !== 'object' || j.error) { reject(new Error(String((j && j.error) || 'bad stats body'))); return; }
          resolve(j);
        } catch (e) { reject(e); }
      });
    }).on('error', (e) => { clearTimeout(t); reject(e); });
  });
}

// Recent request log (already collected by the api-server: status/error/model/
// routeModel/clientSessionId). Mirrors getStats's strict non-2xx handling.
async function getRequests(limit = 200) {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('timeout')), 5000);
    http.get(API_URL + '/requests?limit=' + limit, (res) => {
      let buf = '';
      res.on('data', (c) => { buf += c.toString(); });
      res.on('end', () => {
        clearTimeout(t);
        if ((res.statusCode || 0) >= 300) { reject(new Error('HTTP ' + res.statusCode)); return; }
        try {
          const j = JSON.parse(buf);
          if (!j || typeof j !== 'object' || j.error) { reject(new Error(String((j && j.error) || 'bad requests body'))); return; }
          resolve(j);
        } catch (e) { reject(e); }
      });
    }).on('error', (e) => { clearTimeout(t); reject(e); });
  });
}

// ── process management ───────────────────────────────────────────────────
async function isProcAlive(pidFile) {
  try {
    const pid = parseInt(fs.readFileSync(pidFile, 'utf8'), 10);
    if (!Number.isFinite(pid)) return false;
    process.kill(pid, 0);
    return pid;
  } catch { return false; }
}

async function killAllRatlc() {
  // Kill anything matching our process names (covers stale or orphaned ones).
  try { await execp(`pkill -9 -f "pool/pool-manager.mjs"`); } catch {}
  try { await execp(`pkill -9 -f "pool/bridge-worker.mjs"`); } catch {}
  try { await execp(`pkill -9 -f "pool/api-server.mjs"`); } catch {}
  // And anything still hogging port 4242.
  try {
    const { stdout } = await execp('lsof -ti :4242');
    if (stdout.trim()) await execp(`echo ${stdout.trim().split('\n').join(' ')} | xargs kill -9`);
  } catch {}
  // Clean stale files.
  for (const f of [POOL_SOCK, POOL_LOG, API_LOG, POOL_PID, API_PID]) {
    try { fs.unlinkSync(f); } catch {}
  }
  await new Promise((r) => setTimeout(r, 1500));
}

async function cmdUp(args) {
  const size = args[0] ? parseInt(args[0], 10) : parseInt(process.env.POOL_SIZE || '2', 10);
  console.log(color(`▸ stopping any stale ratlc processes...`, ANSI.gray));
  await killAllRatlc();

  const env = {
    ...process.env,
    POOL_SIZE: String(size),
    HOST: RATLC_API_HOST,
    PORT: RATLC_API_PORT,
  };
  // Defaults we always want exposed:
  env.LOG_REQUEST_TOOLS = '1';

  console.log(color(`▸ starting pool-manager (size=${size}, mode=${env.POOL_TOOL_MODE || 'contract'}, concurrent_opens=${env.POOL_CONCURRENT_OPENS || '1'})...`, ANSI.gray));
  const poolLogFd = fs.openSync(POOL_LOG, 'a');
  const poolProc = spawn('node', [path.join(__dirname, 'pool-manager.mjs')], {
    env, detached: true, stdio: ['ignore', poolLogFd, poolLogFd],
  });
  fs.writeFileSync(POOL_PID, String(poolProc.pid));
  poolProc.unref();
  await new Promise((r) => setTimeout(r, 800));

  console.log(color(`▸ starting api-server...`, ANSI.gray));
  const apiLogFd = fs.openSync(API_LOG, 'a');
  const apiProc = spawn('node', [path.join(__dirname, 'api-server.mjs')], {
    env, detached: true, stdio: ['ignore', apiLogFd, apiLogFd],
  });
  fs.writeFileSync(API_PID, String(apiProc.pid));
  apiProc.unref();
  await new Promise((r) => setTimeout(r, 1500));

  // Verify api-server is responsive.
  try {
    const h = await getHealth();
    console.log(color(`✅ api-server up at ${API_URL}`, ANSI.green));
    console.log(color(`   pool:  ${POOL_LOG}`, ANSI.dim));
    console.log(color(`   api:   ${API_LOG}`, ANSI.dim));
    console.log('');
    console.log(`Mode: ${color(h.config.toolMode, ANSI.bold)}  Model: ${h.config.model}  Concurrent opens: ${h.config.concurrentOpens || 1}`);
    console.log('');
    console.log(`Watch:    ${color('ratlc tui', ANSI.cyan)}`);
    console.log(`Status:   ${color('ratlc status', ANSI.cyan)}`);
    console.log(`Claude:   ${color('ratlc claude', ANSI.cyan)}        ${color('(auto-waits for pool ready)', ANSI.dim)}`);
    console.log(`Shutdown: ${color('ratlc down', ANSI.cyan)}`);
  } catch (e) {
    console.error(color(`❌ api-server didn't come up: ${e.message}`, ANSI.red));
    console.error(`Check log: tail -50 ${API_LOG}`);
    process.exit(1);
  }
}

async function cmdDown() {
  console.log(color(`▸ shutting down...`, ANSI.gray));
  // Best-effort graceful shutdown via pool socket
  try { await poolRequest({ type: 'shutdown' }, 3000); } catch {}
  await new Promise((r) => setTimeout(r, 1500));
  await killAllRatlc();
  console.log(color(`✅ all stopped`, ANSI.green));
}

async function cmdStatus() {
  let snap;
  try { snap = await getStatus(); }
  catch (e) { console.error(color(`pool not reachable: ${e.message}`, ANSI.red)); process.exit(1); }
  printStatus(snap);
}

function fmtAgo(ts) {
  if (!ts) return '-';
  const ms = Date.now() - ts;
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(0)}s`;
  if (ms < 3_600_000) return `${(ms / 60_000).toFixed(1)}m`;
  return `${(ms / 3_600_000).toFixed(1)}h`;
}

// ── latency-stats rendering (shared by `ratlc stats` and the TUI 'stats' view) ─
function fmtMsDur(ms) {
  if (ms == null) return '-';
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 10_000) return `${(ms / 1000).toFixed(1)}s`;
  if (ms < 60_000) return `${Math.round(ms / 1000)}s`;
  return `${(ms / 60_000).toFixed(1)}m`;
}
function padAnsi(s, n) { const raw = String(s).replace(/\x1b\[[0-9;]*m/g, ''); return s + ' '.repeat(Math.max(0, n - raw.length)); }
const STATS_W = [9, 6, 7, 7, 7, 7, 7, 7, 6, 8];
const STATS_HDR = ['BUCKET', 'N', 'FBp50', 'FBp90', 'FBp99', 'TOp50', 'TOp90', 'TOp99', 'err%', 'sugGap'];
function statRow(b, opts = {}) {
  const thin = (b.count || 0) < 5;
  const errC = b.errPct >= 5 ? ANSI.red : b.errPct > 0 ? ANSI.yellow : ANSI.gray;
  // sugGap column: when adaptive enforcement is ON, show the value the api-server
  // would actually arm (clamped up to adaptiveMinGapMs) so the display matches
  // enforcement (M2). When OFF, show the raw data-driven suggestion (advisory for
  // manually tuning the *_FAST/_SLOW thresholds).
  let sug = color('-', ANSI.gray);
  if (b.currentRegimeSuggestGapMs != null) {
    const eff = opts.adaptive ? Math.max(opts.minGapMs || 0, b.currentRegimeSuggestGapMs) : b.currentRegimeSuggestGapMs;
    sug = color(fmtMsDur(eff), ANSI.cyan);
  }
  return [
    padAnsi(b.bucket, STATS_W[0]),
    padAnsi(String(b.count), STATS_W[1]),
    padAnsi(fmtMsDur(b.fb.p50), STATS_W[2]),
    padAnsi(fmtMsDur(b.fb.p90), STATS_W[3]),
    padAnsi(color(fmtMsDur(b.fb.p99), thin ? ANSI.dim : ANSI.bold), STATS_W[4]),
    padAnsi(fmtMsDur(b.total.p50), STATS_W[5]),
    padAnsi(fmtMsDur(b.total.p90), STATS_W[6]),
    padAnsi(fmtMsDur(b.total.p99), STATS_W[7]),
    padAnsi(color(String(b.errPct), errC), STATS_W[8]),
    padAnsi(sug, STATS_W[9]),
  ].join(' ');
}
function formatStatsLines(stats) {
  const out = [];
  // null (unreachable / non-2xx) OR a truthy error object both mean "no usable
  // snapshot" — show the actionable line, never render regime=? hour=undefined.
  if (!stats || stats.error || !Array.isArray(stats.models)) {
    out.push(color('stats unavailable — api-server down or too old; restart it: ./launch.sh up', ANSI.red));
    return out;
  }
  const reg = { fast: ANSI.green, medium: ANSI.yellow, slow: ANSI.red };
  const cr = stats.currentRegime || '?';
  const adaptive = stats.adaptiveTimeouts
    ? color('ADAPTIVE on', ANSI.green + ANSI.bold) + color(' (gap≥' + fmtMsDur(stats.adaptiveMinGapMs || 0) + ', ≤ceiling)', ANSI.dim)
    : color('adaptive off', ANSI.dim);
  out.push('Latency  regime=' + color(cr, (reg[cr] || ANSI.bold) + ANSI.bold)
    + '  hour=' + stats.currentHour + ' (UTC' + (stats.hourOffset >= 0 ? '+' : '') + stats.hourOffset + ')  '
    + (stats.regimesLearned ? color('regimes:learned', ANSI.green) : color('regimes:learning…', ANSI.dim))
    + '  ' + adaptive + '  fb-margin=×' + stats.fbMargin);
  let clock = '';
  for (let h = 0; h < 24; h++) {
    const rg = (stats.regimeOf || {})[h] || 'medium';
    clock += color(h === stats.currentHour ? '▮' : (rg === 'fast' ? '▁' : rg === 'medium' ? '▄' : '█'), reg[rg] || ANSI.gray);
  }
  out.push('  ' + clock + '  ' + color('hours 0-23 · ▁fast ▄med █slow ▮now', ANSI.dim));
  if (!stats.models || !stats.models.length) { out.push(''); out.push(color('  (no requests recorded yet — make some, then check back)', ANSI.dim)); return out; }
  for (const m of stats.models) {
    out.push('');
    out.push(color('▸ ' + m.model, ANSI.cyan + ANSI.bold) + ' ' + color('[' + m.type + ']', m.type === 'fast' ? ANSI.green : ANSI.yellow)
      + color('  N=' + m.overall.count + '  err=' + m.overall.errPct + '%  timeouts=' + m.overall.timeouts + '  retried=' + m.overall.retried, ANSI.dim));
    out.push('  ' + STATS_HDR.map((h, i) => color(padAnsi(h, STATS_W[i]), ANSI.bold)).join(' '));
    for (const b of m.buckets) out.push('  ' + statRow(b, { adaptive: stats.adaptiveTimeouts, minGapMs: stats.adaptiveMinGapMs }));
    out.push('  ' + color(statRow({ bucket: 'all', count: m.overall.count, errPct: m.overall.errPct, fb: m.overall.fb, total: m.overall.total, currentRegimeSuggestGapMs: null }), ANSI.dim));
  }
  return out;
}

// Compact latency band for the default (split) view + `ratlc stats --compact`:
// one regime line (with the 24h clock) + one line per model (N, first-byte
// p50/p99, suggested gap, err). The full per-bucket table lives in view 5.
// Returns [] when stats are unreachable/disabled so the default view stays clean.
function formatStatsCompact(stats) {
  const out = [];
  // null/error = api-server unreachable or pre-`/v1/_stats` (version skew): show
  // an actionable one-liner instead of a silent gap — this is the "no stats"
  // symptom when the TUI was relaunched but the api-server wasn't. Distinct from
  // a healthy-but-empty snapshot (models=[]), which shows "learning…" below.
  if (!stats || stats.error) {
    out.push(color('Latency', ANSI.bold) + color('  stats unavailable — restart the api-server: ./launch.sh up', ANSI.yellow));
    return out;
  }
  if (!Array.isArray(stats.models)) return out;
  const reg = { fast: ANSI.green, medium: ANSI.yellow, slow: ANSI.red };
  const cr = stats.currentRegime || '?';
  let clock = '';
  for (let h = 0; h < 24; h++) {
    const rg = (stats.regimeOf || {})[h] || 'medium';
    clock += color(h === stats.currentHour ? '▮' : (rg === 'fast' ? '▁' : rg === 'medium' ? '▄' : '█'), reg[rg] || ANSI.gray);
  }
  out.push(color('Latency', ANSI.bold) + ' regime=' + color(cr, (reg[cr] || ANSI.bold) + ANSI.bold)
    + ' ' + clock + ' '
    + (stats.adaptiveTimeouts ? color('adaptive', ANSI.green + ANSI.bold) : color('static', ANSI.dim))
    + color('  [5] detail', ANSI.dim));
  if (!stats.models.length) { out.push(color('  (no latency recorded yet — learning…)', ANSI.dim)); return out; }
  for (const m of stats.models) {
    const o = m.overall;
    let sug = m.suggestedGapMs;
    if (sug != null && stats.adaptiveTimeouts) sug = Math.max(stats.adaptiveMinGapMs || 0, sug);
    const errC = o.errPct >= 5 ? ANSI.red : o.errPct > 0 ? ANSI.yellow : ANSI.gray;
    out.push('  ' + color('▸ ' + m.model, ANSI.cyan) + ' ' + color('[' + m.type + ']', m.type === 'fast' ? ANSI.green : ANSI.yellow)
      + color(' N=', ANSI.dim) + o.count
      + color('  FB ', ANSI.dim) + fmtMsDur(o.fb.p50) + color('/', ANSI.dim) + color(fmtMsDur(o.fb.p99), ANSI.bold)
      + color('  sug ', ANSI.dim) + (sug != null ? color(fmtMsDur(sug), ANSI.cyan) : color('—', ANSI.gray))
      + color('  err ', ANSI.dim) + color(o.errPct + '%', errC)
      + (o.timeouts ? color('  t/o ' + o.timeouts, ANSI.red) : ''));
  }
  return out;
}

function printStatus(snap) {
  if (!snap?.pool) { console.log(JSON.stringify(snap, null, 2)); return; }
  const { pool, config } = snap;
  const { thinking: thinkingCount, waitTool: waitToolCount, busy: busySilent } = countSplit(pool.channels, pool.busyCount);
  const deadToks = (pool.tokens || []).filter((t) => t.dead).length;
  const counts = [
    `ready=${color(pool.readyCount, ANSI.green)}`,
    `thinking=${color(thinkingCount, ANSI.cyan)}`,
    `wait-tool=${color(waitToolCount, ANSI.blue)}`,
    `busy=${color(busySilent, ANSI.yellow)}`,
    `opening=${color(pool.openingCount, ANSI.cyan)}`,
    `dead=${color(pool.deadCount, pool.deadCount ? ANSI.red : ANSI.gray)}`,
  ];
  // A dead token will fail every channel that rotates onto it — surface it in the
  // header so token exhaustion is visible at a glance, not buried in the token table.
  if (deadToks) counts.push(color(`⚠tok-dead=${deadToks}/${pool.tokens.length}`, ANSI.red));
  console.log(`Pool: ${color(pool.actualSize + '/' + pool.configuredSize, ANSI.bold)} channels  ${counts.join('  ')}  pending=${pool.pendingRequests}  tool_use_index=${pool.toolUseIndex}`);
  const groupCount = (pool.groups || []).length;
  console.log(`Mode: ${color(config.toolMode, ANSI.bold)}  groups=${groupCount} (default=${pool.defaultGroup || config.model})  concurrent_opens=${config.concurrentOpens || 1}  group_wait_ms=${config.groupWaitMs ?? '-'}  contract=${config.toolMode === 'translate' ? 'cursor defaults' : (config.poolToolsContractCount ?? 'unset')}  subagent=${config.subagentSupport == null ? color('?', ANSI.gray) : (config.subagentSupport ? color('on', ANSI.green) : color('off', ANSI.red))}`);
  const wd0 = resolveWatchdog(config);
  if (wd0.livenessGapMs) console.log(color(`SILENT n/${Math.round(wd0.livenessGapMs / 1000)}s = upstream silent → retry near threshold (empty turn may fire sooner); wait-tool = waiting on client tool; reap ${Math.round((wd0.busyStuckMs || 0) / 1000)}s`, ANSI.dim));
  if (pool.groups?.length) {
    console.log('');
    const ghdr = ['GROUP', 'TARGET', 'READY', 'BUSY', 'OPEN', 'DEAD', 'ROUNDS'];
    const gw = [42, 7, 7, 6, 6, 6, 7];
    console.log(ghdr.map((h, i) => color(h.padEnd(gw[i]), ANSI.bold)).join(' '));
    for (const g of pool.groups) {
      const tag = g.isDefault ? '* ' : '  ';
      const drain = g.draining ? color(' [drain]', ANSI.red) : '';
      const row = [
        (tag + g.model).padEnd(gw[0] - 0) + drain,
        String(g.target).padEnd(gw[1]),
        color(String(g.ready).padEnd(gw[2]), ANSI.green),
        color(String(g.busy).padEnd(gw[3]), ANSI.yellow),
        color(String(g.opening).padEnd(gw[4]), ANSI.cyan),
        color(String(g.dead).padEnd(gw[5]), g.dead ? ANSI.red : ANSI.gray),
        String(g.rounds).padEnd(gw[6]),
      ];
      console.log(row.join(' '));
    }
  }
  console.log('');
  if (!pool.channels?.length) { console.log(color('  (no channels)', ANSI.dim)); return; }
  const headers = ['CHANNEL', 'STATE', 'SILENT', 'GROUP', 'PID', 'ATTEMPTS', 'AGE', 'IDLE', 'ROUNDS', 'CURRENT', 'ERROR'];
  const widths = [10, 9, 11, 36, 7, 9, 8, 8, 8, 22, 30];
  console.log(headers.map((h, i) => h.padEnd(widths[i])).join('  '));
  console.log('-'.repeat(widths.reduce((a, b) => a + b + 2, 0)));
  for (const ch of pool.channels) {
    const row = [
      ch.id, stateLabel(ch), silentCell(ch, wd0), ch.group || '-', String(ch.pid || '-'),
      String(ch.openAttempts || 0), fmtAgo(ch.openedAt), fmtAgo(ch.lastActivityAt),
      String(ch.roundsServed || 0),
      ch.currentRequestId ? ch.currentRequestId.slice(0, 20) : '-',
      (() => {
        if (ch.state === 'dead' && ch.deathReason) return color((ch.deathReason + (ch.error ? ' ' + String(ch.error).slice(0, 12) : '')).slice(0, 28), ANSI.red);
        if (ch.error) return String(ch.error).slice(0, 28);
        // wait-tool: the SILENT cell only fits the count (0/2) for a batch — name
        // the outstanding tools HERE (the ERROR column is otherwise blank for a
        // live channel) so you can see WHAT it's blocked on, not just how many.
        const pt = Array.isArray(ch.pendingTools) ? ch.pendingTools : null;
        if (pt && pt.length > 1) {
          const missing = pt.filter((t) => !t.provided).map((t) => t.toolName || '?');
          let label = 'waiting: ' + missing.join(', ');
          if (label.length > 28) label = label.slice(0, 27) + '…';
          return color(label, ANSI.blue);
        }
        return '';
      })(),
    ];
    const out = row.map((v, i) => {
      const raw = String(v).replace(/\x1b\[[0-9;]*m/g, '');
      return v + ' '.repeat(Math.max(0, widths[i] - raw.length));
    }).join('  ');
    console.log(out);
  }
}

async function cmdWatch(interval = 2) {
  process.stdout.write(ANSI.hideCursor);
  process.on('SIGINT', () => { process.stdout.write(ANSI.showCursor); process.exit(0); });
  while (true) {
    process.stdout.write(ANSI.clear);
    console.log(color(`[${new Date().toISOString()}]  ratlc watch ${interval}s (Ctrl+C to stop)`, ANSI.dim));
    console.log('');
    try { printStatus(await getStatus()); }
    catch (e) { console.error(color(`pool unreachable: ${e.message}`, ANSI.red)); }
    await new Promise((r) => setTimeout(r, interval * 1000));
  }
}

// ── TUI mode (split-screen + ':'-style command bar) ─────────────────────
async function cmdTui() {
  process.stdout.write(ANSI.altScreen + ANSI.hideCursor);

  const apiLines = [];
  const poolLines = [];
  let dirty = true;
  let viewMode = 'split';        // 'split' | 'api' | 'pool' | 'status' | 'stats'
  let showHelp = false;          // '?' toggles a full keymap overlay
  // Throttle the /v1/_stats fetch: render fires on every `dirty` flip (log
  // activity, keypresses, the 1s tick) — up to ~4×/s — but latency percentiles
  // don't need sub-second refresh. Cache so the band/detail fetch is decoupled
  // from the render rate (and we never hammer the endpoint). null is cached too.
  let _statsCache = null, _statsCacheAt = 0;
  const STATS_TTL_MS = 3000;
  async function getStatsCached() {
    const now = Date.now();
    if (now - _statsCacheAt < STATS_TTL_MS) return _statsCache;
    _statsCacheAt = now;
    try { _statsCache = await getStats(); } catch { _statsCache = null; }
    return _statsCache;
  }
  let cmdMode = false;           // vim-style ':' command input
  let cmdBuffer = '';
  let cmdHistory = [];
  let cmdHistoryIdx = -1;
  let cmdResult = '';            // last-action feedback shown below the bar
  let pendingEsc = false;        // track escape sequence parse

  const tailApi = spawn('tail', ['-F', '-n', '50', API_LOG]);
  tailApi.stdout.on('data', (chunk) => {
    for (const line of chunk.toString().split('\n')) {
      if (!line.trim()) continue;
      apiLines.push(line);
      if (apiLines.length > 500) apiLines.shift();
      dirty = true;
    }
  });
  const tailPool = spawn('tail', ['-F', '-n', '30', POOL_LOG]);
  tailPool.stdout.on('data', (chunk) => {
    for (const line of chunk.toString().split('\n')) {
      if (!line.trim()) continue;
      poolLines.push(line);
      if (poolLines.length > 500) poolLines.shift();
      dirty = true;
    }
  });

  function exitTui(code = 0) {
    tailApi.kill(); tailPool.kill();
    process.stdout.write(ANSI.showCursor + ANSI.restoreScreen);
    process.exit(code);
  }

  process.on('SIGINT', () => exitTui(0));
  if (process.stdin.isTTY) {
    process.stdin.setRawMode(true);
    process.stdin.resume();
    process.stdin.setEncoding('utf8');
  } else {
    console.log(color('(stdin not a TTY — running view-only, no hotkeys)', ANSI.dim));
  }

  async function executeCommand(line) {
    const parts = line.split(/\s+/).filter(Boolean);
    const sub = parts[0];
    const args = parts.slice(1);
    cmdResult = '▸ ' + line;
    dirty = true;
    const ratlcBin = process.argv[1];
    function spawnRatlc(subArgs, env) {
      const child = spawn(process.execPath, [ratlcBin, ...subArgs], {
        env: env || process.env, stdio: ['ignore', 'pipe', 'pipe'],
      });
      let out = '';
      child.stdout?.on('data', (d) => { out += d.toString(); });
      child.stderr?.on('data', (d) => { out += d.toString(); });
      child.on('exit', (code) => {
        const tail = out.trim().split('\n').slice(-1)[0] || '';
        cmdResult = (code === 0 ? '✓ ' : '✗ exit ' + code + ' ') + line + (tail ? ' — ' + tail.slice(0, 80) : '');
        dirty = true;
      });
    }
    if (sub === 'down' || sub === 'stop') { spawnRatlc(['down']); return; }
    if (sub === 'up' || sub === 'start') {
      const env = { ...process.env };
      let size = null;
      for (const a of args) {
        if (a === 'translate' || a === 'contract') env.POOL_TOOL_MODE = a;
        else if (/^\d+$/.test(a)) size = a;
        else if (a.startsWith('mode=')) env.POOL_TOOL_MODE = a.split('=')[1];
        else if (a.startsWith('concurrent=') || a.startsWith('parallel=')) env.POOL_CONCURRENT_OPENS = a.split('=')[1];
        else if (a.startsWith('model=')) env.POOL_MODEL = a.split('=')[1];
        else if (a.startsWith('include=')) env.TOOL_INCLUDE = a.split('=')[1];
      }
      spawnRatlc(size ? ['up', size] : ['up'], env);
      return;
    }
    if (sub === 'ramp') {
      if (!args.length) { cmdResult = 'usage: ramp <±N>'; dirty = true; return; }
      spawnRatlc(['ramp', args[0]]);
      return;
    }
    if (sub === 'restart') { spawnRatlc(['restart', ...args]); return; }
    if (sub === 'help' || sub === '?') {
      cmdResult = 'cmds: down · up [N] [translate|contract] · ramp ±N · subagent on|off · restart [ch-N] · help · q  (press ? for full keymap)';
      dirty = true;
      return;
    }
    if (sub === 'subagent' || sub === 'subagents') {
      const a = (args[0] || 'toggle').toLowerCase();
      if (a === 'status') {
        getStatus().then((s) => { const v = s?.config?.subagentSupport; cmdResult = 'subagent: ' + (v == null ? '?' : (v ? 'on' : 'off')); dirty = true; }).catch((e) => { cmdResult = '✗ ' + e.message; dirty = true; });
        return;
      }
      const value = a === 'on' ? true : a === 'off' ? false : 'toggle';
      poolRequest({ type: 'set_subagent_support', value }).then((r) => { cmdResult = '✓ ' + (r.message || 'subagent ' + a); dirty = true; }).catch((e) => { cmdResult = '✗ subagent: ' + e.message; dirty = true; });
      return;
    }
    if (sub === 'claude') { cmdResult = '(run `ratlc claude` from a separate terminal)'; dirty = true; return; }
    cmdResult = '? unknown: ' + sub + ' (try :help)'; dirty = true;
  }

  function onKey(key) {
    if (cmdMode) {
      // Handle ANSI escape sequences for arrow keys / Esc
      if (pendingEsc) {
        pendingEsc = false;
        if (key === '[A') { // up
          if (cmdHistoryIdx < cmdHistory.length - 1) { cmdHistoryIdx++; cmdBuffer = cmdHistory[cmdHistoryIdx] || ''; dirty = true; }
          return;
        }
        if (key === '[B') { // down
          if (cmdHistoryIdx > 0) { cmdHistoryIdx--; cmdBuffer = cmdHistory[cmdHistoryIdx] || ''; }
          else if (cmdHistoryIdx === 0) { cmdHistoryIdx = -1; cmdBuffer = ''; }
          dirty = true;
          return;
        }
        // ESC pressed alone — cancel
        cmdMode = false; cmdBuffer = ''; cmdHistoryIdx = -1; dirty = true;
        return;
      }
      if (key === '') { pendingEsc = true; return; }
      if (key === '\r' || key === '\n') {
        const line = cmdBuffer.trim();
        cmdMode = false; cmdBuffer = ''; cmdHistoryIdx = -1; dirty = true;
        if (line) { cmdHistory.unshift(line); cmdHistory = cmdHistory.slice(0, 50); executeCommand(line); }
        return;
      }
      if (key === '' || key === '\b') { cmdBuffer = cmdBuffer.slice(0, -1); dirty = true; return; }
      if (key.length === 1 && key >= ' ' && key < '') { cmdBuffer += key; dirty = true; return; }
      return;
    }
    // Normal-mode hotkeys
    // Keymap overlay: '?' opens it; while open, ANY key just closes it (modal).
    if (showHelp) { showHelp = false; dirty = true; return; }
    if (key === '?') { showHelp = true; dirty = true; return; }
    if (key === 'q' || key === '') return exitTui(0);
    if (key === ':') { cmdMode = true; cmdBuffer = ''; cmdHistoryIdx = -1; dirty = true; return; }
    if (key === '1') { viewMode = 'split'; dirty = true; return; }
    if (key === '2') { viewMode = 'api'; dirty = true; return; }
    if (key === '3') { viewMode = 'pool'; dirty = true; return; }
    if (key === '4') { viewMode = 'status'; dirty = true; return; }
    if (key === '5') { viewMode = 'stats'; dirty = true; return; }
    if (key === 'r') { poolRequest({ type: 'ramp_up', count: 1 }).then(() => { cmdResult = '✓ ramp +1'; dirty = true; }).catch((e) => { cmdResult = '✗ ramp+1: ' + e.message; dirty = true; }); return; }
    if (key === 'R') { poolRequest({ type: 'ramp_down', count: 1 }).then(() => { cmdResult = '✓ ramp -1'; dirty = true; }).catch((e) => { cmdResult = '✗ ramp-1: ' + e.message; dirty = true; }); return; }
    if (key === 'g') { poolRequest({ type: 'set_subagent_support', value: 'toggle' }).then((r) => { cmdResult = '✓ ' + (r.message || 'subagent toggled'); dirty = true; }).catch((e) => { cmdResult = '✗ subagent: ' + e.message; dirty = true; }); return; }
    if (key === 'k') {
      getStatus().then((s) => {
        const c = s.pool.channels.find((c) => c.state === 'opening' || c.state === 'dead') || s.pool.channels[0];
        if (c) return poolRequest({ type: 'restart_channel', channelId: c.id }).then(() => { cmdResult = '✓ restart ' + c.id; });
      }).catch((e) => { cmdResult = '✗ restart: ' + e.message; }).finally(() => { dirty = true; });
      return;
    }
  }
  if (process.stdin.isTTY) process.stdin.on('data', onKey);

  function stripAnsi(s) { return String(s).replace(/\x1b\[[0-9;]*m/g, ''); }
  function rpad(s, n) { return s + ' '.repeat(Math.max(0, n - stripAnsi(s).length)); }

  function buildStatusLines(snap) {
    const out = [];
    if (!snap?.pool) { out.push(color('pool unreachable', ANSI.red)); return out; }
    const { pool, config } = snap;
    // Split busy into actively-thinking (frames flowing) vs busy-silent (the
    // truly-suspect state). thinking + busy = pool.busyCount.
    const { thinking: thinkingCount, waitTool: waitToolCount, busy: busySilent } = countSplit(pool.channels, pool.busyCount);
    const counts = [
      'ready=' + color(pool.readyCount, ANSI.green),
      'thinking=' + color(thinkingCount, ANSI.cyan),
      'wait-tool=' + color(waitToolCount, ANSI.blue),
      'busy=' + color(busySilent, ANSI.yellow),
      'opening=' + color(pool.openingCount, ANSI.cyan),
      'dead=' + color(pool.deadCount, pool.deadCount ? ANSI.red : ANSI.gray),
    ].join('  ');
    out.push('Pool ' + color(pool.actualSize + '/' + pool.configuredSize, ANSI.bold) + '  ' + counts + '  pending=' + pool.pendingRequests + '  tool_use_held=' + pool.toolUseIndex);
    const groups = Array.isArray(pool.groups) ? pool.groups : [];
    const multiGroup = groups.length > 1;
    const subagentCell = '  subagent ' + (config.subagentSupport == null ? color('?', ANSI.gray) : (config.subagentSupport ? color('on', ANSI.green) : color('off', ANSI.red)));
    if (multiGroup) {
      out.push('Mode ' + color(config.toolMode, ANSI.bold) + '  groups=' + color(String(groups.length), ANSI.bold) + ' (default=' + (config.model || groups.find((g) => g.isDefault)?.model || '?') + ')  parallel-opens=' + (config.concurrentOpens || 1) + subagentCell);
    } else {
      out.push('Mode ' + color(config.toolMode, ANSI.bold) + '  model=' + config.model + '  parallel-opens=' + (config.concurrentOpens || 1) + subagentCell);
    }
    const wd = resolveWatchdog(config);
    if (wd.livenessGapMs) {
      out.push(color(`SILENT n/${Math.round(wd.livenessGapMs / 1000)}s = upstream silent → auto-retry near threshold (empty turn may fire sooner); pool reaps at ${Math.round((wd.busyStuckMs || 0) / 1000)}s`, ANSI.dim));
    }
    if (pool.readyCount >= 1) {
      out.push(color('▶ READY — you can run: ratlc claude', ANSI.green + ANSI.bold));
    } else if (pool.openingCount > 0) {
      const opening = (pool.channels || []).filter((c) => c.state === 'opening' || c.state === 'spawning');
      const maxAttempts = Math.max(0, ...opening.map((c) => c.openAttempts || 0));
      out.push(color('▶ WARMING UP — wait for ready≥1 before launching claude-code', ANSI.yellow + ANSI.bold) + color('  (best attempt: ' + maxAttempts + ')', ANSI.dim));
    } else {
      out.push(color('▶ NOT READY — no channels opening; check status', ANSI.red + ANSI.bold));
    }

    // Per-group summary table (only shown when there's more than one group;
    // single-group setups stay visually unchanged).
    if (multiGroup) {
      out.push('');
      const gw = [42, 7, 7, 6, 6, 6, 7];
      const ghdr = ['GROUP', 'TARGET', 'READY', 'BUSY', 'OPEN', 'DEAD', 'ROUNDS'];
      out.push('  ' + ghdr.map((h, i) => color(rpad(h, gw[i]), ANSI.bold)).join(' '));
      for (const g of groups) {
        const label = g.model + (g.isDefault ? color(' (default)', ANSI.dim) : '') + (g.draining ? color(' (draining)', ANSI.yellow) : '');
        out.push('  ' + [
          rpad(label, gw[0]),
          rpad(String(g.target || 0), gw[1]),
          rpad(color(String(g.ready || 0), ANSI.green), gw[2]),
          rpad(color(String(g.busy || 0), ANSI.yellow), gw[3]),
          rpad(color(String(g.opening || 0), ANSI.cyan), gw[4]),
          rpad(color(String(g.dead || 0), g.dead ? ANSI.red : ANSI.gray), gw[5]),
          rpad(String(g.rounds || 0), gw[6]),
        ].join(' '));
      }
    }

    // Token-health table (only shown when there's more than one token).
    const tokens = Array.isArray(pool.tokens) ? pool.tokens : [];
    if (tokens.length > 1) {
      out.push('');
      // ERROR_WIDTH: fit the message in the remaining terminal columns.
      // Conservative default 80; truncated cleanly per-row below.
      const termCols = process.stdout.columns || 132;
      const tw = [4, 18, 12, 6, 9];
      const fixedWidth = tw.reduce((a, b) => a + b, 0) + 5 /* separators */ + 3 /* '  ' indent */;
      const errWidth = Math.max(20, Math.min(120, termCols - fixedWidth - 2));
      const thdr = ['IDX', 'NAME', 'VALIDATED', 'DEAD', 'OTHERERR', 'LAST_ERROR'];
      out.push('  ' + thdr.slice(0, 5).map((h, i) => color(rpad(h, tw[i]), ANSI.bold)).join(' ') + ' ' + color(thdr[5], ANSI.bold));
      for (const t of tokens) {
        const valTxt = t.validated ? color('✓ yes', ANSI.green) : color('✗ no', ANSI.yellow);
        const deadTxt = t.dead ? color('YES', ANSI.red + ANSI.bold) : color('no', ANSI.gray);
        const errCount = t.otherErrorCount || 0;
        const errCntTxt = errCount > 0
          ? color(String(errCount), t.dead ? ANSI.red : ANSI.yellow)
          : color('0', ANSI.gray);
        const lastErrRaw = t.lastError ? String(t.lastError).replace(/\s+/g, ' ').slice(0, errWidth) : '';
        const lastErrTxt = !lastErrRaw
          ? color('-', ANSI.gray)
          : t.dead
            ? color(lastErrRaw, ANSI.red)
            : color(lastErrRaw, ANSI.yellow);
        out.push('  ' + [
          rpad(String(t.idx), tw[0]),
          rpad(t.name || '?', tw[1]),
          rpad(valTxt, tw[2]),
          rpad(deadTxt, tw[3]),
          rpad(errCntTxt, tw[4]),
        ].join(' ') + ' ' + lastErrTxt);
      }
    }

    out.push('');
    if (pool.channels?.length) {
      // Channel table — sectioned by group when there's more than one group,
      // flat (today's behavior) when only one.
      const w = [10, 10, 5, 7, 11, 8, 9, 8, 8, 7, 18];
      const hdr = ['CHANNEL', 'STATE', 'TOK', 'BUSY', 'SILENT', 'PID', 'ATTEMPTS', 'AGE', 'IDLE', 'ROUNDS', 'CURRENT'];

      function emitRow(ch) {
        const stateCell = stateLabel(ch);
        // BUSY column: time in current turn. Colored yellow at >3 min, red
        // at >4 min (busy-watchdog default kill threshold).
        let busyTxt = '-';
        if (ch.state === 'busy' && ch.busyAt) {
          const busyMs = Date.now() - ch.busyAt;
          const fmt = fmtAgo(ch.busyAt);
          if (busyMs > 240_000) busyTxt = color(fmt, ANSI.red);
          else if (busyMs > 180_000) busyTxt = color(fmt, ANSI.yellow);
          else busyTxt = fmt;
        }
        // SILENT column — see tui-format.mjs silentCell (single source of truth).
        const silentTxt = silentCell(ch, wd);
        return [
          rpad(ch.id, w[0]),
          rpad(stateCell, w[1]),
          rpad(String(ch.tokenIdx ?? 0), w[2]),
          rpad(busyTxt, w[3]),
          rpad(silentTxt, w[4]),
          rpad(String(ch.pid || '-'), w[5]),
          rpad(String(ch.openAttempts || 0), w[6]),
          rpad(fmtAgo(ch.openedAt), w[7]),
          rpad(fmtAgo(ch.lastActivityAt), w[8]),
          rpad(String(ch.roundsServed || 0), w[9]),
          rpad(ch.currentRequestId ? ch.currentRequestId.slice(0, 16) : '-', w[10]),
        ].join(' ');
      }

      if (multiGroup) {
        // Bucket channels by group; preserve group order from the groups array
        // (default first, then alts). Channels with an unknown group ID land
        // in a synthetic "(orphan)" bucket — shouldn't happen in normal
        // operation but visible if it does.
        const buckets = new Map();
        for (const g of groups) buckets.set(g.model, []);
        const orphans = [];
        for (const ch of pool.channels) {
          if (ch.group && buckets.has(ch.group)) buckets.get(ch.group).push(ch);
          else orphans.push(ch);
        }
        let first = true;
        for (const g of groups) {
          const rows = buckets.get(g.model) || [];
          if (rows.length === 0) continue;
          if (!first) out.push('');
          first = false;
          const tag = g.model + (g.isDefault ? color(' (default)', ANSI.dim) : '') + (g.draining ? color(' (draining)', ANSI.yellow) : '');
          out.push(color('── ' + tag + ' ──', ANSI.dim));
          out.push(hdr.map((h, i) => color(rpad(h, w[i]), ANSI.bold)).join(' '));
          for (const ch of rows) out.push(emitRow(ch));
        }
        if (orphans.length) {
          out.push('');
          out.push(color('── (no group / unknown) ──', ANSI.red));
          out.push(hdr.map((h, i) => color(rpad(h, w[i]), ANSI.bold)).join(' '));
          for (const ch of orphans) out.push(emitRow(ch));
        }
      } else {
        out.push(hdr.map((h, i) => color(rpad(h, w[i]), ANSI.bold)).join(' '));
        for (const ch of pool.channels) out.push(emitRow(ch));
      }
    }
    return out;
  }

  function header(ts) {
    const tabs = (key, label, active) => {
      const tag = key + ':' + label;
      return active ? color('[' + tag + ']', ANSI.cyan + ANSI.bold) : color(' ' + tag + ' ', ANSI.dim);
    };
    return [
      color('ratlc tui', ANSI.bold) + '  ' + color(ts, ANSI.dim) +
      '   views: ' + tabs('1', 'split', viewMode === 'split') + tabs('2', 'api', viewMode === 'api') + tabs('3', 'pool', viewMode === 'pool') + tabs('4', 'status', viewMode === 'status') + tabs('5', 'stats', viewMode === 'stats') +
      '   actions: ' + color('[r]', ANSI.cyan) + '+1 ' + color('[R]', ANSI.cyan) + '-1 ' + color('[k]', ANSI.cyan) + ' restart-stuck ' + color('[g]', ANSI.cyan) + ' subagent ' + color('[:]', ANSI.cyan) + ' cmd ' + color('[?]', ANSI.cyan) + ' keymap ' + color('[q]', ANSI.cyan) + ' quit',
      color('─'.repeat(Math.max(1, (process.stdout.columns || 100) - 1)), ANSI.dim),
    ];
  }

  function buildHelpLines() {
    const k = (s) => color(s, ANSI.cyan + ANSI.bold);
    const d = (s) => color(s, ANSI.dim);
    return [
      color('  RATLC TUI — keymap', ANSI.bold),
      '',
      '  ' + d('views  ') + '  ' + k('1') + ' split   ' + k('2') + ' api   ' + k('3') + ' pool   ' + k('4') + ' status   ' + k('5') + ' stats',
      '  ' + d('keys   ') + '  ' + k('r') + ' ramp +1   ' + k('R') + ' ramp -1   ' + k('k') + ' restart a stuck channel',
      '           ' + k('g') + ' toggle sub-agents on/off   ' + k('?') + ' this keymap   ' + k('q') + ' quit',
      '  ' + d('command') + '  ' + k(':') + ' ' + d('up [N] [translate|contract] · down · ramp ±N [--group=M]'),
      '             ' + d('subagent on|off|status · restart [ch-N] · help'),
      '',
      '  ' + d('CLI / launch.sh (outside the TUI)'),
      '    ' + d('ratlc status · failures [N] · subagent on|off|status · ramp ±N · tail · metrics'),
      '    ' + d('./launch.sh up|down|status|tui · ./launch.sh subagent on|off · ./launch.sh edit'),
      '',
      d('  press ? or any key to close'),
    ];
  }

  function logPaneLines(buf, label, height) {
    const cols = process.stdout.columns || 200;
    const out = [color('── ' + label + ' (live tail) ──', ANSI.dim + ANSI.bold)];
    const lines = buf.slice(-height + 1);
    for (const l of lines) out.push(color('  ' + l.slice(0, cols - 4), ANSI.dim));
    while (out.length < height) out.push('');
    return out;
  }

  function drawCmdBar(cols) {
    console.log(color('─'.repeat(Math.max(1, cols - 1)), ANSI.dim));
    if (cmdMode) {
      process.stdout.write(color(':', ANSI.cyan + ANSI.bold) + cmdBuffer + color('▎', ANSI.cyan) + '\n');
    } else {
      const hint = cmdResult
        ? color(cmdResult, ANSI.green)
        : color("press ':' for command (e.g. :up 10 translate, :down, :ramp +3, :help)", ANSI.dim);
      console.log(hint);
    }
  }

  async function render() {
    if (!dirty) return;
    dirty = false;
    let snap;
    try { snap = await getStatus(); } catch (e) { snap = null; }
    process.stdout.write(ANSI.clear);
    const cols = process.stdout.columns || 100;
    const rows = process.stdout.rows || 30;
    const ts = new Date().toISOString().slice(11, 19);
    const hdr = header(ts);
    for (const line of hdr) console.log(line);

    if (showHelp) {
      for (const l of buildHelpLines()) console.log(l);
      drawCmdBar(cols); return;
    }

    // command bar takes 2 lines at the bottom
    const cmdBarLines = 2;

    if (viewMode === 'status') {
      for (const l of buildStatusLines(snap)) console.log(l);
      drawCmdBar(cols); return;
    }
    if (viewMode === 'api') {
      for (const l of logPaneLines(apiLines, API_LOG, rows - hdr.length - cmdBarLines)) console.log(l);
      drawCmdBar(cols); return;
    }
    if (viewMode === 'pool') {
      for (const l of logPaneLines(poolLines, POOL_LOG, rows - hdr.length - cmdBarLines)) console.log(l);
      drawCmdBar(cols); return;
    }
    if (viewMode === 'stats') {
      for (const l of formatStatsLines(await getStatsCached())) console.log(l);
      drawCmdBar(cols); return;
    }
    // split — status panel + a compact latency band (always visible by default;
    // full per-bucket table is view 5) + the api log tail.
    const statusLines = buildStatusLines(snap);
    const compact = formatStatsCompact(await getStatsCached());
    if (compact.length) { statusLines.push(color('─'.repeat(Math.max(1, Math.min(cols, 60)) - 1), ANSI.dim)); for (const l of compact) statusLines.push(l); }
    const minStatusHeight = Math.max(statusLines.length, 12);
    for (let i = 0; i < minStatusHeight; i++) console.log(statusLines[i] ?? '');
    console.log(color('─'.repeat(Math.max(1, cols - 1)), ANSI.dim));
    const apiPaneHeight = Math.max(5, rows - hdr.length - minStatusHeight - cmdBarLines - 2);
    for (const l of logPaneLines(apiLines, API_LOG, apiPaneHeight)) console.log(l);
    drawCmdBar(cols);
  }

  setInterval(() => { dirty = true; }, 1000);
  setInterval(render, 250);
  await render();
}
// ── tail (filtered) ──────────────────────────────────────────────────────
async function cmdTail() {
  console.log(color(`tailing ${POOL_LOG} + ${API_LOG} (Ctrl+C to stop)`, ANSI.dim));
  const t = spawn('tail', ['-F', POOL_LOG, API_LOG], { stdio: 'inherit' });
  process.on('SIGINT', () => { t.kill(); process.exit(0); });
}

// ── ramp ────────────────────────────────────────────────────────────────
async function cmdRamp(args) {
  let group = null;
  const positional = [];
  for (const a of args) {
    if (a.startsWith('--group=')) group = a.slice('--group='.length);
    else positional.push(a);
  }
  const n = parseInt(positional[0] || '1', 10);
  if (!Number.isFinite(n) || n === 0) {
    console.error('usage: ratlc ramp <\u00b1N> [--group=<model>]');
    process.exit(1);
  }
  const req = n > 0
    ? { type: 'ramp_up', count: n }
    : { type: 'ramp_down', count: -n };
  if (group) req.group = group;
  const r = await poolRequest(req);
  if (r?.type === 'error') {
    console.error(color('error: ' + (r.message || 'unknown'), ANSI.red));
    process.exit(1);
  }
  console.log(r.message || JSON.stringify(r));
}

// ratlc subagent <on|off|status> — toggle native Task sub-agent support at runtime.
async function cmdSubagent(args) {
  const action = String(args[0] || 'status').toLowerCase();
  if (action === 'status') {
    const s = await getStatus();
    const on = s?.config?.subagentSupport;
    console.log(`subagent support: ${on ? color('ON', ANSI.green) : color('OFF', ANSI.red)}`);
    return;
  }
  let value;
  if (['on', 'enable', 'enabled', '1', 'true', 'yes'].includes(action)) value = true;
  else if (['off', 'disable', 'disabled', '0', 'false', 'no'].includes(action)) value = false;
  else { console.error('usage: ratlc subagent <on|off|status>'); process.exit(1); }
  const r = await poolRequest({ type: 'set_subagent_support', value });
  if (r?.type === 'error') { console.error(color('error: ' + (r.message || 'unknown'), ANSI.red)); process.exit(1); }
  console.log(r.message || JSON.stringify(r));
}

// ratlc failures [N] — recent not-ok requests (rate-limits, empty turns, errors,
// invalid params) straight from /requests, so you don't grep api.log.
async function cmdFailures(args) {
  const limit = parseInt(args[0] || '200', 10);
  let data;
  try { data = await getRequests(Number.isFinite(limit) ? limit : 200); }
  catch (e) { console.error(color('requests unavailable: ' + e.message + ' (is the api-server up?)', ANSI.red)); process.exit(1); }
  const all = data.items || [];
  // Show anything that is NOT a normal/in-progress state (exclusion is robust to
  // the status vocabulary growing) — catches error, upstream_rate_limit,
  // empty_assistant_turn, stale_tool_result, timeouts, and any unknown status.
  const NORMAL = new Set(['ok', 'completed', 'queued', 'streaming', 'waiting_tool_result', 'done', 'sent']);
  // Only flag FINISHED requests (endedAt set). The api-server mutates a request's
  // status through transient mid-stream states (thinking/tool_use/forwarded/…) that
  // aren't in NORMAL; without this guard a healthy in-flight request would be
  // mis-reported as a failure under load. endedAt is null while in-flight.
  const bad = all.filter((r) => (r.error || (r.status && !NORMAL.has(r.status))) && r.endedAt != null);
  const waiting = all.filter((r) => r.status === 'waiting_tool_result').length;
  console.log(`requests: ${data.count} logged · scanned last ${all.length} · ${color(String(bad.length), bad.length ? ANSI.red : ANSI.green)} not-ok · ${waiting} waiting-on-tool`);
  if (!bad.length) { console.log(color('  ✓ no failures in the window', ANSI.green)); return; }
  console.log(color('  AGE    STATUS              REQUEST           MODEL→ROUTE                     ERROR', ANSI.dim));
  for (const r of bad) {
    const age = r.ageMs < 60000 ? Math.round(r.ageMs / 1000) + 's' : Math.round(r.ageMs / 60000) + 'm';
    const route = (r.model || '?') + (r.routeModel && r.routeModel !== r.model ? '→' + r.routeModel : '');
    const sc = /rate|limit|throttl/i.test(String(r.status || '')) ? ANSI.yellow : ANSI.red;
    console.log(`  ${age.padStart(5)}  ${color(String(r.status || '?').padEnd(18), sc)}  ${String(r.requestId || '-').slice(0, 16).padEnd(16)}  ${route.slice(0, 30).padEnd(30)}  ${String(r.error || '').slice(0, 48)}`);
  }
}

// ratlc inspect <ch> — "why is THIS channel stuck right now": joins the pool
// snapshot + /requests + token into one dump, incl. the outstanding tools' args.
async function cmdInspect(args) {
  const id = args[0];
  if (!id) { console.error('usage: ratlc inspect <ch-id>'); process.exit(1); }
  let snap;
  try { snap = await getStatus(); } catch (e) { console.error(color('pool unreachable: ' + e.message, ANSI.red)); process.exit(1); }
  let ch = (snap.pool?.channels || []).find((c) => c.id === id);
  let dead = false;
  if (!ch) {
    // Live channel gone — fall back to the dead-channel tombstone so we can still
    // post-mortem WHY it died (the bar scenario). Tombstone carries deathReason +
    // the tools it was still waiting on.
    ch = (snap.pool?.recentDeaths || []).find((t) => t.id === id);
    dead = !!ch;
    if (!ch) { console.error(`channel ${id} not present, and no recent-death tombstone (raise RATLC_DEAD_TOMBSTONE_MAX, or it died long ago)`); process.exit(1); }
  }
  const reqId = ch.currentRequestId || ch.deathRequestId;
  let reqs; try { reqs = await getRequests(500); } catch { reqs = { items: [] }; }
  const req = reqId ? (reqs.items || []).find((r) => r.requestId === reqId) : null;
  const tok = (snap.pool?.tokens || [])[ch.tokenIdx];
  const s = (ms) => ms == null ? '-' : Math.round(ms / 1000) + 's';
  if (dead) {
    console.log(color(ch.id, ANSI.bold) + '  ' + color('DEAD', ANSI.red) + `  ${s(ch.deathAgoMs)} ago  group=${ch.group}  rounds=${ch.roundsServed || 0}`);
  } else {
    console.log(color(ch.id, ANSI.bold) + `  ${stateLabel(ch)}  silent=${s(ch.progressGapMs)}  busy=${s(ch.busyForMs)}  age=${s(ch.openedAgoMs)}  group=${ch.group}`);
  }
  if (ch.deathReason) console.log('  death: ' + color(ch.deathReason, ANSI.red) + (ch.error ? '  (' + String(ch.error).slice(0, 80) + ')' : ''));
  const pt = ch.pendingTools || [];
  if (pt.length) {
    console.log(`  waiting on ${color(String(pt.length), ANSI.blue)} tool_result(s):`);
    for (const t of pt.slice(0, 10)) console.log(`    ${t.provided ? color('✓', ANSI.green) : color('·', ANSI.yellow)} ${t.toolName}${t.subagentType ? ':' + t.subagentType : ''}${t.argPreview ? '  ' + color(t.argPreview, ANSI.dim) : ''}`);
    if (pt.length > 10) console.log(`    … +${pt.length - 10} more`);
  }
  if (req) {
    const mb = req.textBytes ? (req.textBytes / 1048576).toFixed(1) + 'MB' : '-';
    const trouble = (req.retryCount > 0 || req.lastRetrySymptom) ? color(`retries=${req.retryCount || 0}${req.lastRetrySymptom ? ' (' + req.lastRetrySymptom + ')' : ''}`, ANSI.yellow) : `retries=0`;
    console.log(`  request ${req.requestId}: status=${req.status} ${trouble} payload=${mb} firstByte=${s(req.firstByteMs)} reinject=${req.reinjectTurns || 0}`);
  } else if (reqId) {
    console.log(`  request ${reqId}: (not in recent /requests window)`);
  }
  if (tok) console.log(`  token[${ch.tokenIdx}] ${tok.name || ''}: ${tok.dead ? color('DEAD', ANSI.red) : (tok.validated ? color('ok', ANSI.green) : '?')}${tok.otherErrorCount ? ' errs=' + tok.otherErrorCount : ''}${tok.lastError ? ' last=' + String(tok.lastError).slice(0, 50) : ''}`);
}

// ratlc deaths [N] — recent channel deaths (they vanish from the live view in ms).
async function cmdDeaths(args) {
  const n = parseInt(args[0] || '30', 10);
  let snap; try { snap = await getStatus(); } catch (e) { console.error(color('pool unreachable: ' + e.message, ANSI.red)); process.exit(1); }
  const d = (snap.pool?.recentDeaths || []).slice(0, Number.isFinite(n) ? n : 30);
  if (!d.length) { console.log('(no recent channel deaths retained — old pool, or none yet)'); return; }
  const sd = (ms) => ms == null ? '-' : (ms < 60000 ? Math.round(ms / 1000) + 's' : Math.round(ms / 60000) + 'm');
  console.log(`recent channel deaths (${d.length}, newest first) — inspect one with \`ratlc inspect <ch>\`:`);
  for (const t of d) {
    const sc = /quota|auth|stall|client-wait/i.test(t.deathReason || '') ? ANSI.red : ANSI.yellow;
    console.log(`  ${sd(t.deathAgoMs).padStart(5)}  ${color(String(t.deathReason || '?').padEnd(22), sc)}  ${String(t.id).padEnd(8)} tok[${t.tokenIdx}]  ${t.deathRequestId || ''}`);
  }
}

async function cmdRestart(args) {
  const id = args[0];
  if (!id) {
    const s = await getStatus();
    const stuck = s.pool.channels.find((c) => c.state === 'busy' && (Date.now() - (c.lastActivityAt || 0)) > 60_000);
    if (!stuck) { console.error('no stuck channel to restart; pass a channel id'); process.exit(1); }
    console.log(`restarting ${stuck.id} (busy for >60s)`);
    const r = await poolRequest({ type: 'restart_channel', channelId: stuck.id });
    return console.log(r.message || JSON.stringify(r));
  }
  const r = await poolRequest({ type: 'restart_channel', channelId: id });
  console.log(r.message || JSON.stringify(r));
}

// ── groups ──────────────────────────────────────────────────────────────
async function cmdGroups() {
  let r;
  try { r = await poolRequest({ type: 'list_groups' }); }
  catch (e) { console.error(color('pool unreachable: ' + e.message, ANSI.red)); process.exit(1); }
  const list = r?.groups || [];
  if (!list.length) { console.log(color('(no groups)', ANSI.dim)); return; }
  const headers = ['GROUP', 'TARGET', 'READY', 'BUSY', 'OPEN', 'DEAD', 'ROUNDS', ''];
  const w = [44, 7, 7, 6, 6, 6, 7, 8];
  console.log(headers.map((h, i) => color(h.padEnd(w[i]), ANSI.bold)).join(' '));
  for (const g of list) {
    const tag = g.isDefault ? color('default', ANSI.cyan) : '       ';
    const drain = g.draining ? color('drain', ANSI.red) : '';
    console.log([
      (g.model + (g.isDefault ? ' (default)' : '')).padEnd(w[0]),
      String(g.target).padEnd(w[1]),
      color(String(g.ready).padEnd(w[2]), ANSI.green),
      color(String(g.busy).padEnd(w[3]), ANSI.yellow),
      color(String(g.opening).padEnd(w[4]), ANSI.cyan),
      color(String(g.dead).padEnd(w[5]), g.dead ? ANSI.red : ANSI.gray),
      String(g.rounds).padEnd(w[6]),
      drain,
    ].join(' '));
  }
}

async function cmdAddGroup(args) {
  const model = args[0];
  const size = parseInt(args[1] || '0', 10);
  if (!model || !Number.isFinite(size) || size < 0) {
    console.error('usage: ratlc add-group <model> <N>');
    process.exit(1);
  }
  const r = await poolRequest({ type: 'add_group', model, size });
  if (r?.type === 'error') {
    console.error(color('error: ' + (r.message || 'unknown'), ANSI.red));
    process.exit(1);
  }
  console.log(r.message || JSON.stringify(r));
}

async function cmdRemoveGroup(args) {
  const model = args[0];
  if (!model) {
    console.error('usage: ratlc remove-group <model>');
    process.exit(1);
  }
  const r = await poolRequest({ type: 'remove_group', model });
  if (r?.type === 'error') {
    console.error(color('error: ' + (r.message || 'unknown'), ANSI.red));
    process.exit(1);
  }
  console.log(r.message || JSON.stringify(r));
}

// ── metrics ─────────────────────────────────────────────────────────────
async function cmdMetrics() {
  // For now we just return getStatus output as JSON. /metrics endpoint
  // (Prometheus-style) added in api-server separately.
  try {
    const s = await getStatus();
    console.log(JSON.stringify(s, null, 2));
  } catch (e) {
    try {
      const r = await getHealth();
      console.log(JSON.stringify(r, null, 2));
    } catch (e2) {
      console.error('neither pool socket nor api /health reachable');
      process.exit(1);
    }
  }
}

async function cmdStats(args = []) {
  let stats;
  try { stats = await getStats(); }
  catch (e) { console.error(color('stats unavailable: ' + e.message + ' (is the api-server up?)', ANSI.red)); process.exit(1); }
  if (args.includes('--json')) { console.log(JSON.stringify(stats, null, 2)); return; }
  const lines = args.includes('--compact') ? formatStatsCompact(stats) : formatStatsLines(stats);
  for (const line of lines) console.log(line);
}

// ── claude wrapper ──────────────────────────────────────────────────────
async function cmdClaude(args) {
  // Pull out --model X (or --model=X) from the args we forward. If
  // present, refuse to launch unless a matching group exists (decision
  // §9.4 in MULTI_GROUP_PLAN.md). The flag is also forwarded to claude
  // and used to set ANTHROPIC_MODEL so the request body actually carries
  // the model.
  let targetModel = null;
  const passArgs = [];
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === '--model' && i + 1 < args.length) { targetModel = args[i + 1]; passArgs.push(a, args[i + 1]); i++; }
    else if (a.startsWith('--model=')) { targetModel = a.slice('--model='.length); passArgs.push(a); }
    else passArgs.push(a);
  }
  if (targetModel) {
    let known;
    try { known = await getGroups(); } catch (e) {
      console.error(color('error: cannot reach pool to verify --model ' + targetModel + ': ' + e.message, ANSI.red));
      process.exit(1);
    }
    const match = known.find((g) => g.model === targetModel);
    if (!match) {
      const list = known.map((g) => g.model + (g.isDefault ? ' (default)' : '')).join(', ');
      console.error(color('error: no group for model ' + targetModel + '. Known: ' + (list || '(none)'), ANSI.red));
      console.error(color('hint: ratlc add-group ' + targetModel + ' 2', ANSI.dim));
      process.exit(1);
    }
  }
  console.log(color('▸ waiting for at least 1 channel ready' + (targetModel ? ' on group ' + targetModel : '') + '...', ANSI.gray));
  let waited = 0;
  while (true) {
    try {
      const h = await getHealth();
      const allReady = h?.pool?.readyCount || 0;
      let groupReady = allReady;
      if (targetModel) {
        const g = (h?.pool?.groups || []).find((x) => x.model === targetModel);
        groupReady = g ? g.ready : 0;
      }
      if (groupReady >= 1) {
        console.log(color('✅ pool ready (' + groupReady + ' ready' + (targetModel ? ' on ' + targetModel : '') + '), launching claude...', ANSI.green));
        break;
      }
    } catch {}
    if (waited > 600) {
      console.error(color('❌ pool did not reach ready>=1 in 10 minutes', ANSI.red));
      process.exit(1);
    }
    process.stdout.write('.');
    await new Promise((r) => setTimeout(r, 2000));
    waited += 2;
  }
  console.log('');
  const env = {
    HOME: process.env.HOME, PATH: process.env.PATH, TERM: process.env.TERM || 'xterm',
    ANTHROPIC_BASE_URL: API_URL,
    ANTHROPIC_API_KEY: 'ratlc-pool',
  };
  if (targetModel) env.ANTHROPIC_MODEL = targetModel;
  const child = spawn(process.env.CLAUDE_BIN || 'claude', passArgs, {
    env, stdio: 'inherit',
  });
  child.on('exit', (code) => process.exit(code || 0));
}

async function cmdLogs() {
  console.log(`pool: ${POOL_LOG}`);
  console.log(`api:  ${API_LOG}`);
}

// ── dispatch ────────────────────────────────────────────────────────────
const [, , cmd, ...rest] = process.argv;
(async () => {
  try {
    switch (cmd) {
      case 'up': return await cmdUp(rest);
      case 'down': return await cmdDown();
      case 'status': return await cmdStatus();
      case 'watch': return await cmdWatch(rest[0] ? parseInt(rest[0], 10) : 2);
      case 'tui': return await cmdTui();
      case 'tail': return await cmdTail();
      case 'ramp': return await cmdRamp(rest);
      case 'subagent': case 'subagents': return await cmdSubagent(rest);
      case 'failures': case 'requests': return await cmdFailures(rest);
      case 'inspect': return await cmdInspect(rest);
      case 'deaths': return await cmdDeaths(rest);
      case 'restart': case 'restart-channel': return await cmdRestart(rest);
      case 'metrics': return await cmdMetrics();
      case 'stats': return await cmdStats(rest);
      case 'groups': return await cmdGroups();
      case 'add-group': return await cmdAddGroup(rest);
      case 'remove-group': return await cmdRemoveGroup(rest);
      case 'claude': return await cmdClaude(rest);
      case 'logs': return await cmdLogs();
      default:
        console.log(`Usage:
  ratlc up [size]             Start pool + api (replaces start.sh)
  ratlc down                  Stop everything cleanly
  ratlc status                One-shot snapshot
  ratlc watch [interval]      Auto-refreshing snapshot
  ratlc tui                   Full-screen dashboard (hotkeys r/R/k/q)
  ratlc tail                  Live-tail pool + api logs
  ratlc ramp <±N> [--group=M]   Add/remove channels on group M (default group if omitted)
  ratlc restart [<ch>]        Restart specific channel (or any stuck one)
  ratlc metrics               JSON metrics
  ratlc stats [--json|--compact]  Per-model latency percentiles + time-of-day regimes
                             (--compact = the one-line-per-model band shown in tui)
  ratlc groups                Per-group breakdown
  ratlc add-group <model> <N> Register a new model group with target N channels
  ratlc remove-group <model>  Drain & remove a non-default model group
  ratlc claude [--model X] [args...]  Auto-wait then spawn claude-code (ANTHROPIC_MODEL forwarded)
  ratlc logs                  Show log file paths

Env vars for 'up':
  POOL_SIZE=2                                 default-group channel target
  POOL_TOOL_MODE=contract|translate           default contract
  POOL_CONCURRENT_OPENS=1                     bump to 2-3 for faster bring-up
  POOL_MODEL=claude-opus-4-7-thinking-max-fast  default group model
  POOL_GROUPS=modelA:3,modelB:2               extra groups at boot (comma-separated model:size)
  POOL_GROUP_WAIT_MS=5000                     ms a request waits for its target group before fallback
  TOOL_INCLUDE=Bash,Read,Edit,...             contract mode tool filter
`);
        process.exit(cmd ? 1 : 0);
    }
  } catch (e) {
    console.error(color(`error: ${e.message}`, ANSI.red));
    process.exit(1);
  }
})();
