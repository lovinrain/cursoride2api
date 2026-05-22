#!/usr/bin/env node
// mock-worker.mjs — a no-op bridge-worker stand-in used by the test
// harness. Forked by pool-manager when POOL_TEST_MOCK_CHANNELS=1.
//
// Speaks Protocol A (see ./IPC.md) but never touches Cursor. Lifecycle:
//   spawning → ready (after ~50ms) → busy → ready → ...
//
// Behavior on each request:
//   send_user_message / send_native_image_message → emits one text_delta + yield within ~25ms.
//   send_tool_result   → emits one text_delta + yield within ~25ms.
//   send_tool_results  → same.
//   ping               → emits yield within ~10ms.
//   shutdown           → exit 0.
//
// Purpose: lets the multi-group routing tests verify pool-manager's
// dispatch logic without paying the Cursor /Run retry lottery.

const CHANNEL_ID = process.env.RATLC_CHANNEL_ID || 'ch-?';
const MODEL = process.env.RATLC_MODEL || 'mock-model';
const READY_DELAY_MS = parseInt(process.env.MOCK_READY_DELAY_MS || '50', 10);
const TURN_DELAY_MS = parseInt(process.env.MOCK_TURN_DELAY_MS || '25', 10);

let openedAt = 0;

function send(msg) {
  if (process.send) process.send(msg);
}

function setState(state, extra = {}) {
  send({
    type: 'state',
    channelId: CHANNEL_ID,
    state,
    openAttempts: 1,
    openedAt,
    lastActivityAt: Date.now(),
    model: MODEL,
    ...extra,
  });
}

setState('spawning');

setTimeout(() => {
  openedAt = Date.now();
  setState('opening');
}, 5);

setTimeout(() => {
  setState('ready');
}, READY_DELAY_MS);

setInterval(() => {
  send({ type: 'heartbeat', lastActivityAt: Date.now() });
}, 10_000).unref();

process.on('message', (msg) => {
  if (!msg || typeof msg !== 'object') return;
  if (msg.type === 'open') {
    // No-op; we go straight to ready via the timer above.
    return;
  }
  if (msg.type === 'shutdown') {
    setState('dead');
    setTimeout(() => process.exit(0), 5);
    return;
  }
  if (msg.type === 'ping') {
    setState('busy');
    setTimeout(() => {
      send({ type: 'yield', requestId: msg.requestId });
      setState('ready');
    }, 10);
    return;
  }
  if (msg.type === 'send_user_message' || msg.type === 'send_native_image_message' || msg.type === 'send_tool_result' || msg.type === 'send_tool_results') {
    setState('busy');
    setTimeout(() => {
      if (msg.type === 'send_user_message' && String(msg.text || '').includes('__MOCK_TOOL_USE__')) {
        send({
          type: 'tool_use',
          requestId: msg.requestId,
          execId: `exec-${CHANNEL_ID}`,
          name: 'MockTool',
          args: { ok: true },
        });
        return;
      }
      if (msg.type === 'send_user_message' && String(msg.text || '').includes('__MOCK_TWO_TOOL_USES__')) {
        send({
          type: 'tool_use',
          requestId: msg.requestId,
          execId: `exec-${CHANNEL_ID}-a`,
          name: 'MockTool',
          args: { n: 1 },
        });
        send({
          type: 'tool_use',
          requestId: msg.requestId,
          execId: `exec-${CHANNEL_ID}-b`,
          name: 'MockTool',
          args: { n: 2 },
        });
        return;
      }
      send({ type: 'text_delta', requestId: msg.requestId, text: `[mock ${CHANNEL_ID}/${MODEL}] ack` });
      send({ type: 'yield', requestId: msg.requestId });
      setState('ready');
    }, TURN_DELAY_MS);
    return;
  }
});

process.on('SIGINT', () => process.exit(0));
process.on('SIGTERM', () => process.exit(0));
