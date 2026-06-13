// Sub-agent model pin (opt-in, RATLC_SUBAGENT_INHERIT_PARENT_MODEL=1).
//
// Goal: keep a Task sub-agent on the SAME model/group as the MAIN agent that
// spawned it. In POOL_TOOL_MODE=translate the Cursor model proposes its own
// (often cheaper, e.g. composer-2.5-fast) model for native sub-agents; even
// after the proxy drops that from the emitted Task call, the sub-agent's fresh
// /v1/messages request can route to a different group than the parent. The pool
// routes purely on the requested model, so we correct that at the proxy by
// overriding a sub-agent request's routing model to the parent session's primary.
//
// How a sub-agent is identified WITHOUT any plugin/marker: claude-code sends the
// SAME x-claude-code-session-id for the parent and its sub-agents (DEVLOG #8),
// and the proxy's convKey (firstUserText+toolHash salt) is DIFFERENT for the
// sub-agent's distinct first user message. So within one known session:
//   - same convKey as the recorded primary  → main-thread continuation (no-op)
//   - different convKey + carries tools      → a sub-agent turn (pin candidate)
// The `hasTools` gate keeps claude-code's tool-less background small-model calls
// (title/topic/quota) off the strong model. The override only fires when the
// sub-agent's model actually differs from the parent's, so a client that already
// inherits correctly is untouched.

export function createSubagentModelPin(opts = {}) {
  const enabled = opts.enabled !== undefined
    ? !!opts.enabled
    : process.env.RATLC_SUBAGENT_INHERIT_PARENT_MODEL === '1';
  const maxSessions = opts.maxSessions || 2000;
  const now = opts.now || (() => Date.now());
  // clientSessionId -> { model, convKey, ts }
  const sessions = new Map();

  function prune() {
    if (sessions.size <= maxSessions) return;
    let oldestKey = null;
    let oldestTs = Infinity;
    for (const [k, v] of sessions) {
      if (v.ts < oldestTs) { oldestTs = v.ts; oldestKey = k; }
    }
    if (oldestKey !== null) sessions.delete(oldestKey);
  }

  // Returns { overridden, model?, role }. `role` is for logging/tests:
  // 'disabled' | 'no-session' | 'primary' | 'continuation' | 'background' |
  // 'subagent-match' | 'subagent'.
  function decide({ clientSessionId, convKey, model, hasTools }) {
    if (!enabled) return { overridden: false, role: 'disabled' };
    if (!clientSessionId || !convKey) return { overridden: false, role: 'no-session' };

    const prior = sessions.get(clientSessionId);
    if (!prior) {
      // First request seen for this session = the main agent. Record as primary.
      sessions.set(clientSessionId, { model: model || '', convKey, ts: now() });
      prune();
      return { overridden: false, role: 'primary' };
    }
    prior.ts = now();

    if (convKey === prior.convKey) return { overridden: false, role: 'continuation' };
    if (!hasTools) return { overridden: false, role: 'background' };
    if (!prior.model || prior.model === model) return { overridden: false, role: 'subagent-match' };
    return { overridden: true, model: prior.model, role: 'subagent' };
  }

  return {
    enabled,
    decide,
    _size: () => sessions.size,
    _peek: (k) => sessions.get(k),
  };
}
