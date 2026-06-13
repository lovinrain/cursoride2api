// Shared cell formatting for the RATLC monitor views — the SINGLE source of
// truth for how a channel's STATE and SILENT cells render, so `ratlc tui`,
// `ratlc status`, and `ratlc-ctl status` can never drift apart (the repeated
// 3-copy divergence is what kept slipping). Emits raw ANSI; callers pad using
// ANSI-stripped width math.
import { isFastModel, typeThresholds } from './model-utils.mjs';

const C = {
  green: '\x1b[32m', yellow: '\x1b[33m', red: '\x1b[31m',
  blue: '\x1b[34m', cyan: '\x1b[36m', gray: '\x1b[90m', reset: '\x1b[0m',
};
const STATE_COLOR = { ready: C.green, busy: C.yellow, opening: C.cyan, spawning: C.cyan, dead: C.red };

export const THINK_GAP_MS = 3000;

// "Thinking" = a frame from THIS turn arrived within THINK_GAP_MS. Keyed off the
// RAW timestamps (lastProgressAt / busyAt), NOT the snapshot's precomputed
// progressGapMs, so a frame left over from a PRIOR turn (lastProgressAt < busyAt
// — lastProgressAt is never reset per-turn by the pool) never counts. This is
// what stops a freshly-routed channel from flashing "thinking"/"live" on a ghost
// frame, and keeps STATE and SILENT measuring the SAME (current) turn.
export function isThinking(ch) {
  if (!ch || ch.state !== 'busy' || ch.lastProgressAt == null) return false;
  if (ch.busyAt != null && ch.lastProgressAt < ch.busyAt) return false;
  return (Date.now() - ch.lastProgressAt) < THINK_GAP_MS;
}
export const isWaitTool = (ch) => !!ch && ch.state === 'busy' && Array.isArray(ch.pendingToolUseIds) && ch.pendingToolUseIds.length > 0;

// Prefer the snapshot's watchdog config, but fall back to this process's env
// (launch.sh exports launch.yaml to the TUI) so the SILENT threshold shows
// without a pool restart. Only trust the snapshot if it actually carries
// livenessGapMs — a half-empty {} must not shadow the env fallback. Mirrors the
// pool-manager's own defaults/clamps.
export function resolveWatchdog(config) {
  const wd = config && config.watchdog;
  // adaptive: the api-server derives the gap live (regime p99 × margin) so there's
  // no fixed countdown denominator. Trust the snapshot's flag; else read this
  // process's env (same RATLC_ADAPTIVE_TIMEOUTS under launch.sh).
  const adaptive = (wd && typeof wd.adaptive === 'boolean') ? wd.adaptive : (process.env.RATLC_ADAPTIVE_TIMEOUTS === '1');
  if (wd && wd.fast && wd.slow) return { ...wd, adaptive };  // snapshot already per-type
  if (wd && wd.livenessGapMs != null) {              // older pool: flat → both types
    const flat = { livenessGapMs: wd.livenessGapMs, ceilingMs: wd.ceilingMs, busyStuckMs: wd.busyStuckMs };
    return { ...flat, fast: flat, slow: flat, adaptive };
  }
  // Fall back to this process's env (launch.sh exports launch.yaml), per type.
  const gap = typeThresholds('NO_VISIBLE_LIVENESS_GRACE_MS', 0, 0);
  const ceil = typeThresholds('NO_VISIBLE_EVENT_TIMEOUT_MS', 25000, 5000);
  const busy = typeThresholds('BUSY_STUCK_TIMEOUT_MS', 240000, 0);
  const waitTool = typeThresholds('WAIT_TOOL_STUCK_TIMEOUT_MS', 1800000, 1800000);
  return {
    livenessGapMs: gap.slow, ceilingMs: ceil.slow, busyStuckMs: busy.slow,
    fast: { livenessGapMs: gap.fast, ceilingMs: ceil.fast, busyStuckMs: busy.fast, waitToolStuckMs: waitTool.fast },
    slow: { livenessGapMs: gap.slow, ceilingMs: ceil.slow, busyStuckMs: busy.slow, waitToolStuckMs: waitTool.slow },
    adaptive,
  };
}

// Pick the threshold set for a channel's model type. Accepts a per-type wd
// (with .fast/.slow) or a flat wd (back-compat) and returns a {livenessGapMs,
// busyStuckMs, ceilingMs}-shaped object.
function pickWd(ch, wd) {
  if (wd && wd.fast && wd.slow) return isFastModel(ch && ch.group) ? wd.fast : wd.slow;
  return wd || {};
}

// Silence of the CURRENT turn: time since its last frame, or since it went busy
// if no frame yet. max() of the two stamps so a stale lastProgressAt from a prior
// turn never inflates it — busyAt (reset on every route) wins for a fresh turn.
// Clamped at 0 against renderer-vs-pool clock skew. null when neither stamp set.
function silenceMs(ch) {
  const since = Math.max(ch.lastProgressAt || 0, ch.busyAt || 0);
  return since > 0 ? Math.max(0, Date.now() - since) : null;
}

// Abbreviate a tool name to ≤4 chars so the SILENT cell stays within budget
// (`WebFetch`→`WebF`), while keeping the high-signal names readable (Task/Bash/Read).
function abbrevTool(name) {
  const n = String(name || 'tool');
  return n.length <= 4 ? n : n.slice(0, 4);
}

export function stateLabel(ch) {
  if (ch && ch.state === 'busy') {
    if (isThinking(ch)) return C.cyan + 'thinking' + C.reset;
    if (isWaitTool(ch)) return C.blue + 'wait-tool' + C.reset;
    return C.yellow + 'busy' + C.reset;
  }
  const st = (ch && ch.state) || '?';
  return (STATE_COLOR[st] || '') + st + C.reset;
}

// SILENT cell. `wd` from resolveWatchdog. "live" is tied to isThinking so STATE
// and SILENT can never disagree.
export function silentCell(ch, wd) {
  if (!ch || ch.state !== 'busy') return C.gray + '-' + C.reset;
  if (isThinking(ch)) return C.cyan + 'live' + C.reset;
  const ms = silenceMs(ch);
  if (ms == null) return C.gray + '·' + C.reset;
  const s = Math.round(ms / 1000);
  const t = pickWd(ch, wd);   // per-model-type threshold (fast vs non-fast)
  if (isWaitTool(ch)) {
    // wait-tool is reaped on its OWN (much longer) timer post the wait-tool
    // watchdog split — fall back to busyStuckMs only for pre-split snapshots.
    const reapMs = (t && (t.waitToolStuckMs ?? t.busyStuckMs)) || 0;
    const near = reapMs > 0 && ms >= 0.85 * reapMs;
    // Finer granularity, from the snapshot's pendingTools detail when present:
    //  • lone tool → NAME it (`Task 280s`, `Bash 12s`) — a lone sub-agent wait is
    //    no longer an opaque `tool 280s`.
    //  • parallel batch → `provided/total` (`1/3 120s`) — watch provided climb /
    //    pending shrink to see the wait PROGRESSING (siblings returning), vs stuck.
    // Falls back to the count-only form for pre-detail snapshots.
    const detail = Array.isArray(ch.pendingTools) ? ch.pendingTools : null;
    const pending = detail ? detail.length : ch.pendingToolUseIds.length;
    let body;
    if (detail && pending === 1) body = `${abbrevTool(detail[0].toolName)} ${s}s`;
    else if (detail && pending > 1) body = `${detail.filter((d) => d.provided).length}/${pending} ${s}s`;
    else body = pending > 1 ? `${pending}×${s}s` : `tool ${s}s`;
    return (near ? C.red : C.blue) + body + (near ? '!' : '') + C.reset;
  }
  if (wd && wd.adaptive) {
    // Adaptive gap is dynamic (regime p99 × margin, recomputed per request) — no
    // fixed denominator to show. Render elapsed silence + `~`; ramp red only near
    // the absolute ceiling, the upper bound that still holds under adaptive.
    const ceilMs = (t && t.ceilingMs) || 0;
    const near = ceilMs > 0 && ms >= 0.85 * ceilMs;
    return (near ? C.red : C.yellow) + `${s}s~${near ? '!' : ''}` + C.reset;
  }
  const gapThr = (t && t.livenessGapMs) || 0;
  if (gapThr > 0) {
    const ratio = ms / gapThr;
    return (ratio >= 0.85 ? C.red : C.yellow) + `${s}s/${Math.round(gapThr / 1000)}s${ratio >= 1 ? '!' : ''}` + C.reset;
  }
  return C.yellow + `${s}s` + C.reset;
}

// Header counts: split busy into thinking / wait-tool / busy-silent (disjoint).
export function countSplit(channels, busyCount) {
  const thinking = (channels || []).filter(isThinking).length;
  const waitTool = (channels || []).filter((c) => isWaitTool(c) && !isThinking(c)).length;
  const busy = Math.max(0, (busyCount || 0) - thinking - waitTool);
  return { thinking, waitTool, busy };
}
