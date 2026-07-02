// Verifies the channel-death audit renderer (buildDeathsLines) used by BOTH
// `ratlc deaths` and TUI view 7. Drives a mock pool that returns crafted
// recentDeaths tombstones and asserts the rendered post-mortem: churn summary,
// reason tally, per-channel rows with lifetime/rounds/error, and held-tool flag.
import net from 'node:net';
import { spawn } from 'node:child_process';
import path from 'node:path';
import os from 'node:os';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.join(HERE, '..', '..');
const SOCK = path.join(os.tmpdir(), `ratlc-deaths-${process.pid}.sock`);
let fail = 0; const a = (n, c, d) => { console.log((c ? '  ✓ ' : '  ✗ ') + n + (c ? '' : ` — ${d}`)); if (!c) fail++; };

const NOW_LIKE = 7_200_000; // 2h in ms — a plausible channel lifetime
const snapshot = () => ({
  type: 'status',
  pool: {
    actualSize: 2, configuredSize: 2, readyCount: 2, busyCount: 0, openingCount: 0, deadCount: 0,
    pendingRequests: 0, toolUseIndex: 0, channels: [], groups: [], tokens: [],
    neverReadyDeaths: 7,   // channels that never reached ready (open failures) — excluded from the tab
    recentDeaths: [
      // served turns, then Cursor aborted mid-follow-up-turn: retryable TYPE but 0 retries (turn-1-only)
      { id: 'ch-11', group: 'claude-opus-4-8-thinking-max-fast', tokenIdx: 0, tokenName: 'flaky@outlook.com', deathReason: 'worker:error', error: 'Response error: aborted', roundsServed: 5, openedAt: 1000, deathAt: 1000 + NOW_LIKE, deathAgoMs: 45000, pendingTools: [], retryable: true, retries: 0 },
      // died on its FIRST turn after retrying 4× (retryable, exhausted)
      { id: 'ch-14', group: 'claude-opus-4-8-thinking-max-fast', tokenIdx: 0, tokenName: 'flaky@outlook.com', deathReason: 'worker:error', error: 'RunSSE non-200: 503 Service Unavailable', roundsServed: 0, openedAt: 1500, deathAt: 1500 + 30000, deathAgoMs: 30000, pendingTools: [], retryable: true, retries: 4 },
      // died holding client tools (wait-tool) — not a retryable stream error
      { id: 'ch-12', group: 'claude-opus-4-8-thinking-max-fast', tokenIdx: 0, tokenName: 'flaky@outlook.com', deathReason: 'reap:wait-tool@1800s', error: null, roundsServed: 2, openedAt: 2000, deathAt: 2000 + 60000, deathAgoMs: 20000, pendingTools: [{ toolName: 'Bash' }, { toolName: 'Task' }], retryable: false, retries: 0 },
      // never reached ready (open exhausted) — MUST be excluded from the tab
      { id: 'ch-13', group: 'claude-opus-4-8-thinking-max-fast', tokenIdx: 0, tokenName: 'flaky@outlook.com', deathReason: 'worker:exhausted', error: 'open exhausted', roundsServed: 0, openedAt: null, deathAt: 5000, deathAgoMs: 600000, pendingTools: [] },
    ],
  },
  config: { toolMode: 'translate', model: 'claude-opus-4-8-thinking-max-fast' },
});
function startMockPool() {
  try { fs.unlinkSync(SOCK); } catch {}
  const server = net.createServer((conn) => {
    let buf = '';
    conn.on('data', (chunk) => {
      buf += chunk.toString('utf8'); let i;
      while ((i = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, i); buf = buf.slice(i + 1); if (!line.trim()) continue;
        let msg; try { msg = JSON.parse(line); } catch { continue; }
        if (msg.type === 'status') { try { conn.write(JSON.stringify(snapshot()) + '\n'); } catch {} }
      }
    });
    conn.on('error', () => {});
  });
  return new Promise((r) => server.listen(SOCK, () => r(server)));
}
function runDeaths() {
  return new Promise((resolve) => {
    const p = spawn(process.execPath, [path.join(HERE, 'ratlc.mjs'), 'deaths', '30'], { cwd: REPO_ROOT, env: { ...process.env, POOL_SOCK: SOCK } });
    let out = ''; p.stdout.on('data', (d) => out += d); p.stderr.on('data', (d) => out += d);
    p.on('exit', () => resolve(out.replace(/\x1b\[[0-9;]*m/g, '')));
  });
}

const pool = await startMockPool();
try {
  const r = await runDeaths();
  console.log(r.split('\n').slice(0, 9).join('\n'));
  a('renders the death-audit header', /Channel deaths/.test(r), 'no header');
  // (1) never-ready channel EXCLUDED + noted
  a('never-ready channel (ch-13) is EXCLUDED from the tab', !/ch-13/.test(r), 'ch-13 leaked in');
  a('notes the excluded never-ready count (7)', /7 channel\(s\) died before ever reaching ready/.test(r), r.match(/note:.*/)?.[0]);
  a('churn counts only ready-then-dead (3 shown, not 4)', /churn: 3 in last 1m · 3 in last 5m · 3 ready-then-dead shown/.test(r), r.match(/churn:.*/)?.[0]);
  a('reason tally excludes worker:exhausted', /by reason:.*worker:error 2/.test(r) && /reap:wait-tool 1/.test(r) && !/by reason:.*worker:exhausted/.test(r), r.match(/by reason:.*/)?.[0]);
  // (2) RETRY column
  a('served-then-aborted row: retryable but 0 retries → y/0 (follow-up turn)', /ch-11.*\b5\b.*2\.0h.*y\/0.*worker:error.*Response error: aborted/.test(r), r.match(/ch-11.*/)?.[0]);
  a('first-turn 503 death: retried 4× → ↻4', /ch-14.*↻4.*worker:error.*503/.test(r), r.match(/ch-14.*/)?.[0]);
  a('wait-tool death: not retryable → fatal + held tools', /ch-12.*fatal.*reap:wait-tool.*held 2 tool\(s\): Bash,Task/.test(r), r.match(/ch-12.*/)?.[0]);
} catch (e) { console.log('  ✗ harness error:', e.message); fail++; }
await new Promise((r) => pool.close(r));
try { fs.unlinkSync(SOCK); } catch {}
console.log(fail === 0 ? '\ndeaths-audit-test: OK' : `\ndeaths-audit-test: FAIL (${fail})`);
process.exit(fail === 0 ? 0 : 1);
