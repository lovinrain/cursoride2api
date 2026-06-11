// Shared cell formatting for the RATLC monitor views — the SINGLE source of
// truth for how a channel's STATE and SILENT cells render, so `ratlc tui`,
// `ratlc status`, and `ratlc-ctl status` can never drift apart (the repeated
// 3-copy divergence is what kept slipping). Emits raw ANSI; callers pad using
// ANSI-stripped width math.
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
  if (wd && wd.livenessGapMs != null) return wd;
  const n = (k, d) => { const v = parseInt(process.env[k] || '', 10); return Number.isFinite(v) ? v : d; };
  return {
    livenessGapMs: Math.max(0, n('RATLC_NO_VISIBLE_LIVENESS_GRACE_MS', 0)),
    busyStuckMs: Math.max(0, n('RATLC_BUSY_STUCK_TIMEOUT_MS', 240000)),
    ceilingMs: Math.max(5000, n('RATLC_NO_VISIBLE_EVENT_TIMEOUT_MS', 25000)),
  };
}

// Silence of the CURRENT turn: time since its last frame, or since it went busy
// if no frame yet. max() of the two stamps so a stale lastProgressAt from a prior
// turn never inflates it — busyAt (reset on every route) wins for a fresh turn.
// Clamped at 0 against renderer-vs-pool clock skew. null when neither stamp set.
function silenceMs(ch) {
  const since = Math.max(ch.lastProgressAt || 0, ch.busyAt || 0);
  return since > 0 ? Math.max(0, Date.now() - since) : null;
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
  if (isWaitTool(ch)) {
    const reapMs = (wd && wd.busyStuckMs) || 0;
    const near = reapMs > 0 && ms >= 0.85 * reapMs;
    return (near ? C.red : C.blue) + `tool ${s}s${near ? '!' : ''}` + C.reset;
  }
  const gapThr = (wd && wd.livenessGapMs) || 0;
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
