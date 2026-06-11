#!/usr/bin/env node
// ratlc-ctl — CLI for inspecting and steering the RATLC pool manager.
//
// Commands:
//   status              — one-shot snapshot
//   watch [interval=2]  — refresh status every N seconds
//   ramp-up N           — add N channels
//   ramp-down N         — remove N channels
//   restart-channel ID  — kill a channel for respawn
//   shutdown            — shut down the pool manager

import net from 'node:net';

const POOL_SOCK = process.env.POOL_SOCK || '/tmp/ratlc-pool.sock';

function send(obj, timeoutMs = 8000) {
  return new Promise((resolve, reject) => {
    const sock = net.createConnection(POOL_SOCK);
    let buf = '';
    const t = setTimeout(() => {
      try { sock.destroy(); } catch { /* ignore */ }
      reject(new Error('timeout'));
    }, timeoutMs);
    sock.on('connect', () => sock.write(JSON.stringify(obj) + '\n'));
    sock.on('data', (c) => {
      buf += c.toString('utf8');
      const idx = buf.indexOf('\n');
      if (idx === -1) return;
      try {
        const m = JSON.parse(buf.slice(0, idx));
        clearTimeout(t);
        sock.end();
        resolve(m);
      } catch { /* keep waiting */ }
    });
    sock.on('error', (e) => { clearTimeout(t); reject(e); });
  });
}

function fmtTimeAgo(ts) {
  if (!ts) return '-';
  const ms = Date.now() - ts;
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(0)}s`;
  if (ms < 3_600_000) return `${(ms / 60_000).toFixed(1)}m`;
  return `${(ms / 3_600_000).toFixed(1)}h`;
}

const STATE_COLOR = {
  ready: '\x1b[32m',     // green
  busy: '\x1b[33m',      // yellow
  opening: '\x1b[36m',   // cyan
  spawning: '\x1b[36m',
  dead: '\x1b[31m',      // red
};
const RESET = '\x1b[0m';
const CYAN = '\x1b[36m';
// A busy channel with a forward-progress frame within this window is actively
// producing output → "thinking"; busy with a longer gap is "busy" (silent).
const isThinking = (ch) => ch.state === 'busy' && ch.progressGapMs != null && ch.progressGapMs < 3000;
// SILENT cell: for a busy channel, silence since the last useful frame (or busy
// start if none) vs the silent-timeout threshold — the "retry coming" countdown.
function fmtSilent(ch, gapThr) {
  if (ch.state !== 'busy') return '-';
  const since = ch.lastProgressAt || ch.busyAt;
  if (!since) return '·';
  const silentMs = Date.now() - since;
  if (silentMs < 3000) return `${CYAN}live${RESET}`;
  const s = Math.round(silentMs / 1000);
  if (!gapThr) return `${STATE_COLOR.busy}${s}s${RESET}`;
  const ratio = silentMs / gapThr;
  const c = ratio >= 0.85 ? STATE_COLOR.dead : STATE_COLOR.busy;
  return `${c}${s}s/${Math.round(gapThr / 1000)}s${ratio >= 1 ? '!' : ''}${RESET}`;
}

function printStatus(snapshot) {
  if (!snapshot || !snapshot.pool) {
    console.log(JSON.stringify(snapshot, null, 2));
    return;
  }
  const { pool, config } = snapshot;
  const thinkingCount = (pool.channels || []).filter(isThinking).length;
  const busySilent = Math.max(0, (pool.busyCount || 0) - thinkingCount);
  const counts = `ready=${pool.readyCount} thinking=${thinkingCount} busy=${busySilent} opening=${pool.openingCount} dead=${pool.deadCount}`;
  console.log(`Pool: ${pool.actualSize}/${pool.configuredSize} channels  ${counts}  pending=${pool.pendingRequests}  tool_use_index=${pool.toolUseIndex}`);
  const modeStr = `mode=${config.toolMode || 'contract'}`;
  const contractStr = config.toolMode === 'translate'
    ? 'translate (Cursor defaults)'
    : (config.poolToolsContractCount === null ? 'unset' : `${config.poolToolsContractCount} tools`);
  console.log(`Model: ${config.model}  ${modeStr}  contract=${contractStr}  idle_ping=${(config.idlePingMs / 60000).toFixed(0)}min`);
  const gapThr = (config && config.watchdog && config.watchdog.livenessGapMs) || 0;
  if (gapThr) console.log(`SILENT n/${Math.round(gapThr / 1000)}s = upstream silent → auto-retry near threshold (empty turn may fire sooner); reap at ${Math.round(((config.watchdog && config.watchdog.busyStuckMs) || 0) / 1000)}s`);
  console.log('');
  if (!pool.channels || pool.channels.length === 0) {
    console.log('  (no channels)');
    return;
  }
  const headers = ['CHANNEL', 'STATE', 'SILENT', 'PID', 'OPEN_ATT', 'AGE', 'IDLE', 'ROUNDS', 'CURRENT', 'ERROR'];
  const widths = [10, 9, 11, 7, 9, 8, 8, 8, 22, 40];
  console.log(headers.map((h, i) => h.padEnd(widths[i])).join('  '));
  console.log('─'.repeat(widths.reduce((a, b) => a + b + 2, 0)));
  for (const ch of pool.channels) {
    const color = STATE_COLOR[ch.state] || '';
    const row = [
      ch.id,
      isThinking(ch) ? `${CYAN}thinking${RESET}`
        : ch.state === 'busy' ? `${STATE_COLOR.busy}busy${RESET}`
        : `${color}${ch.state}${RESET}`,
      fmtSilent(ch, gapThr),
      String(ch.pid || '-'),
      String(ch.openAttempts || 0),
      fmtTimeAgo(ch.openedAt),
      fmtTimeAgo(ch.lastActivityAt),
      String(ch.roundsServed || 0),
      ch.currentRequestId ? ch.currentRequestId.slice(0, 20) : '-',
      ch.error ? String(ch.error).slice(0, 38) : '',
    ];
    // pad with raw lengths (subtract ANSI escapes from width calc when colorized)
    const out = row.map((v, i) => {
      // Strip ANSI for length calc
      const raw = String(v).replace(/\x1b\[[0-9;]*m/g, '');
      const pad = Math.max(0, widths[i] - raw.length);
      return v + ' '.repeat(pad);
    }).join('  ');
    console.log(out);
  }
  console.log('');
}

async function cmdStatus() {
  const snap = await send({ type: 'status' });
  printStatus(snap);
}

async function cmdWatch(interval = 2) {
  while (true) {
    process.stdout.write('\x1b[2J\x1b[H');  // clear screen
    console.log(`[${new Date().toISOString()}]  (refresh every ${interval}s, Ctrl+C to stop)\n`);
    try {
      const snap = await send({ type: 'status' });
      printStatus(snap);
    } catch (e) {
      console.error('error:', e.message);
    }
    await new Promise((r) => setTimeout(r, interval * 1000));
  }
}

async function cmdRampUp(n) {
  const r = await send({ type: 'ramp_up', count: parseInt(n, 10) });
  console.log(r.message || JSON.stringify(r));
}

async function cmdRampDown(n) {
  const r = await send({ type: 'ramp_down', count: parseInt(n, 10) });
  console.log(r.message || JSON.stringify(r));
}

async function cmdRestartChannel(id) {
  const r = await send({ type: 'restart_channel', channelId: id });
  console.log(r.message || JSON.stringify(r));
}

async function cmdShutdown() {
  const r = await send({ type: 'shutdown' });
  console.log(r.message || JSON.stringify(r));
}

const args = process.argv.slice(2);
const cmd = args[0];
(async () => {
  try {
    switch (cmd) {
      case 'status': await cmdStatus(); break;
      case 'watch':  await cmdWatch(args[1] ? parseInt(args[1], 10) : 2); break;
      case 'ramp-up': await cmdRampUp(args[1] || 1); break;
      case 'ramp-down': await cmdRampDown(args[1] || 1); break;
      case 'restart-channel': await cmdRestartChannel(args[1]); break;
      case 'shutdown': await cmdShutdown(); break;
      default:
        console.log('Usage:');
        console.log('  ratlc-ctl status');
        console.log('  ratlc-ctl watch [interval_seconds]');
        console.log('  ratlc-ctl ramp-up <N>');
        console.log('  ratlc-ctl ramp-down <N>');
        console.log('  ratlc-ctl restart-channel <CHANNEL_ID>');
        console.log('  ratlc-ctl shutdown');
        process.exit(1);
    }
  } catch (e) {
    console.error('error:', e.message);
    process.exit(1);
  }
})();
