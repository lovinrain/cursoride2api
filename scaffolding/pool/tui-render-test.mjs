// Integration test: drives all THREE renderers (ratlc-ctl status, ratlc status,
// ratlc tui) against a crafted snapshot and asserts each renders STATE/SILENT via
// the shared tui-format module — including the SLIP-1 fix (stale lastProgressAt
// must not inflate a fresh turn) and the env-fallback threshold. Consistency
// across views is now structural (one shared module); this proves the wiring.
import net from 'node:net';
import { spawn } from 'node:child_process';
import fs from 'node:fs';

const POOL_DIR = '/root/git_farm/cursoride2api_ratlc/cursoride2api/scaffolding/pool';
const now = Date.now();
const ch = (id, state, gapAgo, busyAgo, pending) => ({
  id, pid: 7, group: 'm', state, openAttempts: 1, openedAt: now - 600000,
  lastActivityAt: now - 1000, tokenIdx: 0,
  busyAt: state === 'busy' ? now - (busyAgo ?? 60000) : null,
  busyForMs: state === 'busy' ? (busyAgo ?? 60000) : null,
  lastProgressAt: gapAgo == null ? null : now - gapAgo,
  progressGapMs: gapAgo == null ? null : gapAgo,
  roundsServed: 1, currentRequestId: state === 'busy' ? 'req-x' : null, pendingToolUseIds: pending || [], error: null,
});
const snapshot = {
  pool: {
    actualSize: 5, configuredSize: 5, readyCount: 1, busyCount: 4, openingCount: 0, deadCount: 0,
    pendingRequests: 0, toolUseIndex: 0, defaultGroup: 'm', groups: [],
    channels: [
      ch('ch-think',  'busy', 500,    5000,  []),          // thinking / live
      ch('ch-silent', 'busy', 47000,  50000, []),          // busy / 47s/90s
      ch('ch-tool',   'busy', 50000,  60000, ['t']),       // wait-tool / tool 50s
      ch('ch-stale',  'busy', 600000, 2000,  []),          // SLIP 1: fresh turn, stale frame → busy / 2s, NOT 600s
      ch('ch-ready',  'ready', null,  null,  []),          // ready / -
    ],
  },
  config: { toolMode: 'translate', model: 'm', poolToolsContractCount: null, idlePingMs: 1200000, concurrentOpens: 1, groupWaitMs: 5000 },
  // NO config.watchdog → exercises the env-fallback below.
};
const ENV = { RATLC_NO_VISIBLE_LIVENESS_GRACE_MS: '90000', RATLC_BUSY_STUCK_TIMEOUT_MS: '360000' };

function run(file, args, tuiMs) {
  return new Promise((resolve) => {
    const sock = `/tmp/ratlc-rendertest-${process.pid}-${Math.floor(now % 100000)}-${file}.sock`;
    try { fs.unlinkSync(sock); } catch {}
    const server = net.createServer((conn) => { let b = ''; conn.on('data', (c) => { b += c; if (b.includes('\n')) conn.write(JSON.stringify(snapshot) + '\n'); }); });
    server.listen(sock, () => {
      const proc = spawn('node', [file, ...args], { cwd: POOL_DIR, env: { ...process.env, POOL_SOCK: sock, ...ENV } });
      let raw = ''; proc.stdout.on('data', (d) => raw += d); proc.stderr.on('data', (d) => raw += d);
      if (tuiMs) setTimeout(() => { try { proc.kill('SIGKILL'); } catch {} }, tuiMs);
      proc.on('exit', () => { try { fs.unlinkSync(sock); } catch {} server.close(); resolve(raw.replace(/\x1b\[[0-9;?]*[a-zA-Z]/g, '')); });
    });
  });
}

let fail = 0; const a = (n, c, d) => { console.log((c ? '  ✓ ' : '  ✗ ') + n + (c ? '' : ` — ${d}`)); if (!c) fail++; };
const rowOf = (out, id) => (out.split('\n').find((l) => l.trimStart().startsWith(id)) || '');

function checkRenderer(label, out) {
  console.log(`=== ${label} ===`);
  a(`${label}: legend (env-fallback threshold)`, /SILENT n\/90s/.test(out), 'no legend → env-fallback failed');
  a(`${label}: counts thinking=1 wait-tool=1 busy=2`, /thinking=1\b/.test(out) && /wait-tool=1\b/.test(out) && /busy=2\b/.test(out), out.split('\n').find((l) => /ready=/.test(l)) || '?');
  a(`${label}: ch-think → thinking + live`, /\bthinking\b/.test(rowOf(out, 'ch-think')) && /\blive\b/.test(rowOf(out, 'ch-think')), rowOf(out, 'ch-think').trim());
  a(`${label}: ch-silent → busy + 47s/90s`, /\bbusy\b/.test(rowOf(out, 'ch-silent')) && /47s\/90s/.test(rowOf(out, 'ch-silent')), rowOf(out, 'ch-silent').trim());
  a(`${label}: ch-tool → wait-tool + tool 50s`, /\bwait-tool\b/.test(rowOf(out, 'ch-tool')) && /tool 50s/.test(rowOf(out, 'ch-tool')), rowOf(out, 'ch-tool').trim());
  // SLIP 1: stale frame (600s) must NOT inflate the fresh (2s) turn.
  const stale = rowOf(out, 'ch-stale');
  a(`${label}: ch-stale (SLIP 1) → small countdown ~2s, NOT 600s`, /\b[0-9]s\/90s/.test(stale) && !/600s/.test(stale), stale.trim());
  a(`${label}: ch-stale → STATE busy (not thinking on a ghost frame)`, /\bbusy\b/.test(stale) && !/thinking/.test(stale), stale.trim());
  a(`${label}: ch-ready → ready, SILENT "-"`, /\bready\b/.test(rowOf(out, 'ch-ready')), rowOf(out, 'ch-ready').trim());
}

checkRenderer('ratlc-ctl status', await run('ratlc-ctl.mjs', ['status']));
checkRenderer('ratlc status', await run('ratlc.mjs', ['status']));
// tui: alt-screen frames; just confirm the key tokens render (wiring check).
const tui = await run('ratlc.mjs', ['tui'], 1600);
console.log('=== ratlc tui (headless) ===');
a('tui: thinking+live present', /thinking/.test(tui) && /live/.test(tui), 'missing');
a('tui: wait-tool + tool 50s present', /wait-tool/.test(tui) && /tool 50s/.test(tui), 'missing');
a('tui: 47s/90s threshold present (env-fallback)', /47s\/90s/.test(tui), 'missing');
a('tui: SLIP-1 stale NOT shown as 600s', !/600s\/90s/.test(tui), '600s leaked');

console.log(fail === 0 ? '\ntui-render-test: OK' : `\ntui-render-test: FAIL (${fail})`);
process.exit(fail === 0 ? 0 : 1);
