// Per-conversation thinking-history store for the RATLC pool's api-server.
//
// Why this exists:
//
// Cursor's transport strips Anthropic's signed thinking blocks at the
// boundary. The pool relays whatever the model emits through the
// `bajie_yield` tool-result pump; on every fresh turn the model sees the
// flattened conversation text but not its prior reasoning. Compounded by
// LRU channel rotation in `POOL_CONTEXT_MODE=full`, the model's natural
// server-side thinking context is dropped between turns.
//
// When `POOL_REINJECT_THINKING=1`, the api-server captures every
// `thinking_delta` event flowing back from the bridge (already forwarded
// via IPC by bridge-worker → pool-manager) and re-renders it as
// `<thinking>...</thinking>` text inside the prompt that goes back to the
// bridge on the next turn. The model treats it as visible reasoning
// context rather than as its own signed thinking — different mechanism,
// similar effect for reasoning-heavy multi-turn workflows.
//
// Cost: extra prompt tokens on every continuation, capped by:
//   - POOL_REINJECT_THINKING_MAX_BYTES_PER_TURN  (default 4096 bytes per stored turn)
//   - POOL_REINJECT_THINKING_MAX_TURNS           (default 5 stored turns per conversation)
//
// Off by default. Opt in for symmetry with CURSOR_REINJECT_THINKING on
// server.js. See DEVLOG.md "Proxy-side thinking re-injection (opt-in
// approximation)" for the deeper motivation.
//
// Mirrors src/thinking-history.js (the CommonJS module used by
// server.js). Ported as a fresh ESM module here because:
//   - api-server is ESM; bridging CJS dynamically is annoying.
//   - The pool's env-var names differ (POOL_* vs CURSOR_*), keeping
//     enable/disable controls cleanly separate.
//   - The module is small enough that duplicating beats coupling.

const _enabled = process.env.POOL_REINJECT_THINKING === '1';

const MAX_BYTES_PER_TURN = (() => {
  const raw = process.env.POOL_REINJECT_THINKING_MAX_BYTES_PER_TURN;
  if (raw == null || raw === '') return 4096;
  const n = parseInt(raw, 10);
  return Number.isFinite(n) && n > 0 ? n : 4096;
})();

const MAX_TURNS = (() => {
  const raw = process.env.POOL_REINJECT_THINKING_MAX_TURNS;
  if (raw == null || raw === '') return 5;
  const n = parseInt(raw, 10);
  return Number.isFinite(n) && n > 0 ? n : 5;
})();

// 30-minute TTL — matches src/thinking-history.js and the bridge cache.
const TTL_MS = 30 * 60_000;

// Strip out textual tool-call markers — these are the model's
// hallucinated tool-call markers we suppress from the visible response.
// Don't include them in re-injected thinking either; the model would see
// noise. (Same scrub as src/thinking-history.js.)
function _scrubThinking(text) {
  if (!text) return '';
  return text
    .replace(/\[Tool call: [^\]]*\]/g, '')
    .replace(/\[Tool call\]\s+[A-Za-z_][\w.-]*(?:\([^]*?\))?/g, '')
    .trim();
}

// Per-conversation store. Map<convKey, {
//   turns:         [{turnIndex, text}],     // committed turns, oldest first
//   currentTurnText: string,                // accumulator for the in-progress turn
//   currentTurnIndex: number,               // assistant-turn ordinal for the next commit
//   lastAccessMs:  number,                  // TTL timestamp
// }>
const _store = new Map();

function _slot(convKey) {
  let s = _store.get(convKey);
  if (!s) {
    s = { turns: [], currentTurnText: '', currentTurnIndex: 0, lastAccessMs: Date.now() };
    _store.set(convKey, s);
  }
  return s;
}

export function isEnabled() { return _enabled; }
export function maxBytesPerTurn() { return MAX_BYTES_PER_TURN; }
export function maxTurns() { return MAX_TURNS; }

// Append a chunk of thinking text to the in-progress turn for this
// convKey. Respects MAX_BYTES_PER_TURN — once the accumulator hits the
// cap, further appends for the same turn are dropped (truncation marker
// is added on commit if needed). No-op when disabled.
export function append(convKey, text) {
  if (!_enabled) return;
  if (!convKey || !text) return;
  const s = _slot(convKey);
  if (s.currentTurnText.length >= MAX_BYTES_PER_TURN) return;
  const remaining = MAX_BYTES_PER_TURN - s.currentTurnText.length;
  const chunk = text.length > remaining ? text.slice(0, remaining) : text;
  s.currentTurnText += chunk;
  s.lastAccessMs = Date.now();
}

// Commit the in-progress turn buffer as a stored turn and reset the
// accumulator for the next turn. Triggers FIFO eviction once turns
// exceeds MAX_TURNS. No-op when disabled or when the in-progress buffer
// is empty / scrub-stripped down to nothing.
export function commitTurn(convKey) {
  if (!_enabled) return;
  if (!convKey) return;
  const s = _store.get(convKey);
  if (!s) return;
  const scrubbed = _scrubThinking(s.currentTurnText);
  const turnIdx = s.currentTurnIndex;
  s.currentTurnIndex += 1;
  s.currentTurnText = '';
  s.lastAccessMs = Date.now();
  if (!scrubbed) return;
  s.turns.push({ turnIndex: turnIdx, text: scrubbed });
  if (s.turns.length > MAX_TURNS) {
    s.turns.splice(0, s.turns.length - MAX_TURNS);
  }
}

// Fetch the stored turns for a conversation, ordered oldest-first.
// Returns an empty array when disabled or absent — caller is allowed to
// call unconditionally.
export function getForConvKey(convKey) {
  if (!_enabled) return [];
  if (!convKey) return [];
  const s = _store.get(convKey);
  if (!s) return [];
  s.lastAccessMs = Date.now();
  return s.turns.slice();
}

// Total number of conversations currently in the buffer. Used by tests
// and the status snapshot.
export function size() {
  return _store.size;
}

// Drop the in-progress accumulator for a conversation without committing
// it. Used when a turn errored out and we don't want to taint future
// reinjections with partial reasoning that never produced a real reply.
export function discardCurrent(convKey) {
  if (!_enabled) return;
  if (!convKey) return;
  const s = _store.get(convKey);
  if (!s) return;
  s.currentTurnText = '';
  s.lastAccessMs = Date.now();
}

// Background TTL eviction. Cheap; only fires when there's actual state.
function _evictStale(nowOverride) {
  const now = nowOverride ?? Date.now();
  for (const [k, v] of _store) {
    if (now - v.lastAccessMs > TTL_MS) _store.delete(k);
  }
}
const _evictTimer = setInterval(_evictStale, 5 * 60_000);
_evictTimer.unref();

// Test hooks — let unit tests bypass real time and clear state between
// cases without process-restart hackery.
export function _resetForTests() {
  _store.clear();
}
export function _evictForTests(nowOverride) {
  _evictStale(nowOverride);
}
export function _ttlMs() { return TTL_MS; }
