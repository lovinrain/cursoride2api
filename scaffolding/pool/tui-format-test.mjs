// Unit tests for tui-format.mjs — the single source of truth for STATE/SILENT
// cells. Covers every issue the adversarial review surfaced.
import { isThinking, isWaitTool, resolveWatchdog, stateLabel, silentCell, countSplit, THINK_GAP_MS } from './tui-format.mjs';

const strip = (s) => s.replace(/\x1b\[[0-9;]*m/g, '');
let fail = 0;
const eq = (name, got, want) => { const g = strip(got); const ok = g === want; console.log((ok ? '  ✓ ' : '  ✗ ') + name + (ok ? '' : ` — got "${g}" want "${want}"`)); if (!ok) fail++; };
const ok = (name, cond, d) => { console.log((cond ? '  ✓ ' : '  ✗ ') + name + (cond ? '' : ` — ${d}`)); if (!cond) fail++; };
const now = Date.now();
const ch = (o) => ({ state: 'busy', lastProgressAt: null, busyAt: null, pendingToolUseIds: [], ...o });
const WD = { livenessGapMs: 90000, busyStuckMs: 360000, ceilingMs: 300000 };

console.log('=== SLIP 1: stale lastProgressAt must NOT inflate silence (false-alarm bug) ===');
// Served a request 600s ago; freshly routed 1s ago; no frame yet this turn.
const stale = ch({ lastProgressAt: now - 600000, busyAt: now - 1000 });
eq('fresh turn with stale prior frame → counts from busyAt (1s), not 600s', silentCell(stale, WD), '1s/90s');
ok('stale prior frame → NOT thinking (STATE busy)', !isThinking(stale), 'isThinking true on a ghost frame');
eq('stale → STATE busy', stateLabel(stale), 'busy');

console.log('=== SLIP 2: ghost frame from prior turn must not show thinking/live ===');
const ghost = ch({ lastProgressAt: now - 1000, busyAt: now - 500 }); // prev frame older than this turn's start
ok('ghost frame (lastProgressAt<busyAt) → not thinking', !isThinking(ghost), 'thinking on a prior-turn frame');
ok('ghost → SILENT counts from busyAt (small), not live', /^[0-9]+s\/90s$/.test(strip(silentCell(ghost, WD))), strip(silentCell(ghost, WD)));

console.log('=== real thinking: a frame THIS turn within 3s ===');
const think = ch({ busyAt: now - 5000, lastProgressAt: now - 500 });
ok('frame this turn <3s → thinking', isThinking(think), 'not thinking');
eq('thinking → SILENT live', silentCell(think, WD), 'live');
eq('thinking → STATE thinking', stateLabel(think), 'thinking');

console.log('=== gap countdown + color ramp ===');
eq('47s busy-silent → 47s/90s', silentCell(ch({ busyAt: now - 47000, lastProgressAt: now - 47000 }), WD), '47s/90s');
ok('47/90 (<0.85) is yellow not red', !silentCell(ch({ busyAt: now - 47000, lastProgressAt: now - 47000 }), WD).includes('\x1b[31m'), 'unexpected red');
ok('83/90 (>=0.85) is red', silentCell(ch({ busyAt: now - 83000, lastProgressAt: now - 83000 }), WD).includes('\x1b[31m'), 'not red near threshold');
eq('95s over threshold → 95s/90s!', silentCell(ch({ busyAt: now - 95000, lastProgressAt: now - 95000 }), WD), '95s/90s!');

console.log('=== wait-tool ===');
const tool = ch({ busyAt: now - 60000, lastProgressAt: now - 50000, pendingToolUseIds: ['t'] });
eq('wait-tool → STATE wait-tool', stateLabel(tool), 'wait-tool');
eq('wait-tool → tool 50s (calm)', silentCell(tool, WD), 'tool 50s');
ok('wait-tool calm = blue not red', silentCell(tool, WD).includes('\x1b[34m'), 'not blue');
const toolNear = ch({ busyAt: now - 320000, lastProgressAt: now - 320000, pendingToolUseIds: ['t'] });
eq('wait-tool near reap → tool 320s!', silentCell(toolNear, WD), 'tool 320s!');
ok('wait-tool near reap = red', silentCell(toolNear, WD).includes('\x1b[31m'), 'not red');

console.log('=== SLIP 3: negative clock skew clamps to 0 (→ live), never negative ===');
const skew = ch({ busyAt: now + 5000, lastProgressAt: now + 5000 }); // since in the future
const sc = strip(silentCell(skew, WD));
ok('clock-skew never shows negative seconds', !/-\d/.test(sc), `got ${sc}`);

console.log('=== SLIP 4 / edge: busy with no stamps → "·", not a crash ===');
eq('busy, no busyAt/lastProgressAt → ·', silentCell(ch({}), WD), '·');
eq('non-array pendingToolUseIds → not wait-tool', stateLabel(ch({ busyAt: now - 5000, lastProgressAt: now - 5000, pendingToolUseIds: undefined })), 'busy');
eq('unknown state → rendered raw, no crash', stateLabel({ state: 'weird' }), 'weird');

console.log('=== resolveWatchdog precedence (EDGE-1) ===');
ok('snapshot watchdog present → used', resolveWatchdog({ watchdog: { livenessGapMs: 1234, busyStuckMs: 9, ceilingMs: 9 } }).livenessGapMs === 1234, 'snapshot ignored');
ok('snapshot watchdog {} (half-empty) → env fallback, not shadowed', (() => { process.env.RATLC_NO_VISIBLE_LIVENESS_GRACE_MS = '77000'; const r = resolveWatchdog({ watchdog: {} }); delete process.env.RATLC_NO_VISIBLE_LIVENESS_GRACE_MS; return r.livenessGapMs === 77000; })(), '{} shadowed the env');
ok('no watchdog + env set → env used', (() => { process.env.RATLC_NO_VISIBLE_LIVENESS_GRACE_MS = '90000'; const r = resolveWatchdog({}); delete process.env.RATLC_NO_VISIBLE_LIVENESS_GRACE_MS; return r.livenessGapMs === 90000; })(), 'env not used');
ok('no watchdog + env unset → gap 0 (Option B off, no threshold)', (() => { delete process.env.RATLC_NO_VISIBLE_LIVENESS_GRACE_MS; return resolveWatchdog({}).livenessGapMs === 0; })(), 'gap not 0');
ok('negative env clamps (EDGE-3)', (() => { process.env.RATLC_BUSY_STUCK_TIMEOUT_MS = '-5000'; const r = resolveWatchdog({}); delete process.env.RATLC_BUSY_STUCK_TIMEOUT_MS; return r.busyStuckMs === 0; })(), 'negative busyStuck not clamped');

console.log('=== gap=0 (Option B off) → bare Ns, no threshold ===');
eq('gap 0 → bare 47s (no /90s)', silentCell(ch({ busyAt: now - 47000, lastProgressAt: now - 47000 }), { livenessGapMs: 0, busyStuckMs: 360000 }), '47s');

console.log('=== countSplit (disjoint) ===');
const chans = [think, tool, ch({ busyAt: now - 47000, lastProgressAt: now - 47000 }), { state: 'ready' }];
const cs = countSplit(chans, 3);
ok('countSplit thinking=1 waitTool=1 busy=1', cs.thinking === 1 && cs.waitTool === 1 && cs.busy === 1, JSON.stringify(cs));

console.log('=== SLIP 5: worst realistic widths fit the 11-col SILENT budget ===');
const longest = [silentCell(ch({ busyAt: now - 390000, lastProgressAt: now - 390000 }), WD), silentCell(toolNear, WD)].map(strip);
ok('longest realistic SILENT (post-reap ~390s) ≤ 11 chars', longest.every((x) => x.length <= 11), JSON.stringify(longest.map((x) => [x, x.length])));

console.log(fail === 0 ? '\ntui-format-test: OK' : `\ntui-format-test: FAIL (${fail})`);
process.exit(fail === 0 ? 0 : 1);
