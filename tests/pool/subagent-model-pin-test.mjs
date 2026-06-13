#!/usr/bin/env node
// Unit tests for the sub-agent model pin (scaffolding/pool/subagent-model-pin.mjs).
import assert from 'node:assert/strict';
import { createSubagentModelPin } from '../../scaffolding/pool/subagent-model-pin.mjs';

const PARENT = 'claude-opus-4-8-thinking-max-fast';

// 1. Disabled → never overrides, even for an obvious sub-agent.
{
  const pin = createSubagentModelPin({ enabled: false });
  pin.decide({ clientSessionId: 's1', convKey: 'parent', model: PARENT, hasTools: true });
  const d = pin.decide({ clientSessionId: 's1', convKey: 'child', model: 'haiku', hasTools: true });
  assert.equal(d.overridden, false);
  assert.equal(d.role, 'disabled');
}

// 2. First request of a session is recorded as primary (no override).
{
  const pin = createSubagentModelPin({ enabled: true });
  const d = pin.decide({ clientSessionId: 's1', convKey: 'parent', model: PARENT, hasTools: true });
  assert.equal(d.overridden, false);
  assert.equal(d.role, 'primary');
  assert.equal(pin._peek('s1').model, PARENT);
}

// 3. Main-thread continuation (same convKey) → no override.
{
  const pin = createSubagentModelPin({ enabled: true });
  pin.decide({ clientSessionId: 's1', convKey: 'parent', model: PARENT, hasTools: true });
  const d = pin.decide({ clientSessionId: 's1', convKey: 'parent', model: PARENT, hasTools: true });
  assert.equal(d.overridden, false);
  assert.equal(d.role, 'continuation');
}

// 4. Sub-agent (distinct convKey, has tools, drifted model) → pinned to parent.
{
  const pin = createSubagentModelPin({ enabled: true });
  pin.decide({ clientSessionId: 's1', convKey: 'parent', model: PARENT, hasTools: true });
  const d = pin.decide({ clientSessionId: 's1', convKey: 'child-A', model: 'haiku', hasTools: true });
  assert.equal(d.overridden, true);
  assert.equal(d.role, 'subagent');
  assert.equal(d.model, PARENT);
}

// 5. Sub-agent that already matches the parent model → no override (no-op).
{
  const pin = createSubagentModelPin({ enabled: true });
  pin.decide({ clientSessionId: 's1', convKey: 'parent', model: PARENT, hasTools: true });
  const d = pin.decide({ clientSessionId: 's1', convKey: 'child-A', model: PARENT, hasTools: true });
  assert.equal(d.overridden, false);
  assert.equal(d.role, 'subagent-match');
}

// 6. Tool-less background small-model call (e.g. title/topic/quota) → never pinned.
{
  const pin = createSubagentModelPin({ enabled: true });
  pin.decide({ clientSessionId: 's1', convKey: 'parent', model: PARENT, hasTools: true });
  const d = pin.decide({ clientSessionId: 's1', convKey: 'bg', model: 'claude-3-5-haiku', hasTools: false });
  assert.equal(d.overridden, false);
  assert.equal(d.role, 'background');
}

// 7. No session id → cannot pin (non-claude-code caller).
{
  const pin = createSubagentModelPin({ enabled: true });
  const d = pin.decide({ clientSessionId: '', convKey: 'x', model: PARENT, hasTools: true });
  assert.equal(d.overridden, false);
  assert.equal(d.role, 'no-session');
}

// 8. Multiple distinct sub-agents in one session all pin to the same parent model.
{
  const pin = createSubagentModelPin({ enabled: true });
  pin.decide({ clientSessionId: 's1', convKey: 'parent', model: PARENT, hasTools: true });
  for (const ck of ['child-A', 'child-B', 'child-C']) {
    const d = pin.decide({ clientSessionId: 's1', convKey: ck, model: 'composer-2.5-fast', hasTools: true });
    assert.equal(d.overridden, true);
    assert.equal(d.model, PARENT);
  }
}

// 9. Distinct sessions don't cross-contaminate.
{
  const pin = createSubagentModelPin({ enabled: true });
  pin.decide({ clientSessionId: 's1', convKey: 'p1', model: PARENT, hasTools: true });
  pin.decide({ clientSessionId: 's2', convKey: 'p2', model: 'claude-4.6-opus-max-thinking-fast', hasTools: true });
  const d1 = pin.decide({ clientSessionId: 's1', convKey: 'c', model: 'haiku', hasTools: true });
  const d2 = pin.decide({ clientSessionId: 's2', convKey: 'c', model: 'haiku', hasTools: true });
  assert.equal(d1.model, PARENT);
  assert.equal(d2.model, 'claude-4.6-opus-max-thinking-fast');
}

// 10. LRU prune respects maxSessions (oldest by ts evicted first).
{
  let t = 0;
  const pin = createSubagentModelPin({ enabled: true, maxSessions: 2, now: () => ++t });
  pin.decide({ clientSessionId: 'a', convKey: 'pa', model: PARENT, hasTools: true }); // ts=1
  pin.decide({ clientSessionId: 'b', convKey: 'pb', model: PARENT, hasTools: true }); // ts=2
  pin.decide({ clientSessionId: 'c', convKey: 'pc', model: PARENT, hasTools: true }); // ts=3 → evicts 'a'
  assert.equal(pin._size(), 2);
  assert.equal(pin._peek('a'), undefined);
  assert.ok(pin._peek('b'));
  assert.ok(pin._peek('c'));
}

// 11. Env-driven enable (RATLC_SUBAGENT_INHERIT_PARENT_MODEL=1) when no explicit opt.
{
  const prev = process.env.RATLC_SUBAGENT_INHERIT_PARENT_MODEL;
  process.env.RATLC_SUBAGENT_INHERIT_PARENT_MODEL = '1';
  try {
    const pin = createSubagentModelPin();
    assert.equal(pin.enabled, true);
    pin.decide({ clientSessionId: 's1', convKey: 'parent', model: PARENT, hasTools: true });
    const d = pin.decide({ clientSessionId: 's1', convKey: 'child', model: 'haiku', hasTools: true });
    assert.equal(d.overridden, true);
  } finally {
    if (prev === undefined) delete process.env.RATLC_SUBAGENT_INHERIT_PARENT_MODEL;
    else process.env.RATLC_SUBAGENT_INHERIT_PARENT_MODEL = prev;
  }
}

console.log('subagent-model-pin-test: OK');
