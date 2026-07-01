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
    recentDeaths: [
      // served turns then Cursor aborted it (the "went away after a successful turn" case)
      { id: 'ch-11', group: 'claude-opus-4-8-thinking-max-fast', tokenIdx: 0, tokenName: 'flaky@outlook.com', deathReason: 'worker:error', error: 'Response error: aborted', roundsServed: 5, openedAt: 1000, deathAt: 1000 + NOW_LIKE, deathAgoMs: 45000, pendingTools: [] },
      // died holding client tools (wait-tool)
      { id: 'ch-12', group: 'claude-opus-4-8-thinking-max-fast', tokenIdx: 0, tokenName: 'flaky@outlook.com', deathReason: 'reap:wait-tool@1800s', error: null, roundsServed: 2, openedAt: 2000, deathAt: 2000 + 60000, deathAgoMs: 20000, pendingTools: [{ toolName: 'Bash' }, { toolName: 'Task' }] },
      // never opened (open exhausted)
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
  a('churn summary: 2 in last 1m, 2 in last 5m, 3 shown', /churn: 2 in last 1m · 2 in last 5m · 3 shown/.test(r), r.match(/churn:.*/)?.[0]);
  a('reason tally present', /by reason:.*worker:error 1/.test(r) && /worker:exhausted 1/.test(r) && /reap:wait-tool 1/.test(r), r.match(/by reason:.*/)?.[0]);
  a('served-then-aborted row shows rounds + lifetime + error', /ch-11.*\b5\b.*2\.0h.*worker:error.*Response error: aborted/.test(r), r.match(/ch-11.*/)?.[0]);
  a('wait-tool death flags the held tools', /ch-12.*reap:wait-tool.*held 2 tool\(s\): Bash,Task/.test(r), r.match(/ch-12.*/)?.[0]);
  a('never-opened row shows LIFE as — (died before ready)', /ch-13.*—.*worker:exhausted.*open exhausted/.test(r), r.match(/ch-13.*/)?.[0]);
} catch (e) { console.log('  ✗ harness error:', e.message); fail++; }
await new Promise((r) => pool.close(r));
try { fs.unlinkSync(SOCK); } catch {}
console.log(fail === 0 ? '\ndeaths-audit-test: OK' : `\ndeaths-audit-test: FAIL (${fail})`);
process.exit(fail === 0 ? 0 : 1);
