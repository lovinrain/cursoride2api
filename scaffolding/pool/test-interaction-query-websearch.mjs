#!/usr/bin/env node
// Unit test for handleInteractionQuery WebSearch approval.
//
// Verifies the two branches that translate-mode added:
//   passthroughNativeTools=false → Rejected (legacy behavior preserved)
//   passthroughNativeTools=true  → Approved (Cursor backend will do the search)
//
// Also confirms CURSOR_LOG_INTERACTION=1 emits the expected trace line so
// we can grep for "action=approve" / "action=reject" in real proxy logs.

import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const require = createRequire(import.meta.url);

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, '../..');
const ca = require(path.join(ROOT, 'src/cursor-agent.js'));

function protoVarint(value) {
  let n = Number(value);
  const out = [];
  while (n > 0x7f) {
    out.push((n & 0x7f) | 0x80);
    n = Math.floor(n / 128);
  }
  out.push(n);
  return Buffer.from(out);
}

function protoFieldBytes(fieldNo, bytes) {
  const b = Buffer.from(bytes || []);
  return Buffer.concat([protoVarint(fieldNo * 8 + 2), protoVarint(b.length), b]);
}

function protoFieldString(fieldNo, value) {
  return protoFieldBytes(fieldNo, Buffer.from(String(value || ''), 'utf8'));
}

function protoFieldUInt32(fieldNo, value) {
  return Buffer.concat([protoVarint(fieldNo * 8), protoVarint(value)]);
}

function protoMessage(fields) {
  return Buffer.concat(fields.filter(Boolean).map((f) => Buffer.from(f)));
}

function unknownLengthDelimited(fieldNo, body) {
  return { no: fieldNo, wireType: 2, data: Buffer.concat([protoVarint(body.length), Buffer.from(body)]) };
}

function readDelimited(data, offset) {
  let pos = offset;
  let len = 0;
  let shift = 0;
  while (pos < data.length) {
    const b = data[pos++];
    len += (b & 0x7f) * Math.pow(2, shift);
    if ((b & 0x80) === 0) break;
    shift += 7;
  }
  return { bytes: data.subarray(pos, pos + len), offset: pos + len };
}

function parseFields(bytes) {
  const data = Buffer.from(bytes || []);
  const out = [];
  let offset = 0;
  while (offset < data.length) {
    const tag = data[offset++];
    const no = tag >> 3;
    const wireType = tag & 7;
    if (wireType === 0) {
      const start = offset;
      while (offset < data.length && (data[offset++] & 0x80)) {}
      out.push({ no, wireType, data: data.subarray(start, offset) });
    } else if (wireType === 2) {
      const value = readDelimited(data, offset);
      out.push({ no, wireType, data: value.bytes });
      offset = value.offset;
    } else {
      throw new Error(`unsupported test wire type ${wireType}`);
    }
  }
  return out;
}

async function run() {
  // Load the proto module the same way startConversation does.
  await ca.loadProto();
  const { create, toBinary, fromBinary } = require('@bufbuild/protobuf');
  const agent = await import(path.join(ROOT, 'src/proto/agent_pb.mjs'));

  // Build a synthetic webSearchRequestQuery.
  const args = create(agent.WebSearchArgsSchema, { searchTerm: 'latest Claude 5 release', toolCallId: 'tc_test_123' });
  const query = create(agent.WebSearchRequestQuerySchema, { args });
  const iq = create(agent.InteractionQuerySchema, {
    id: 4242,
    query: { case: 'webSearchRequestQuery', value: query },
  });

  // Run each scenario, capturing what gets sent on the wire.
  function runOnce(passthroughNativeTools) {
    const captured = [];
    const sendBinaryFrame = (bytes) => captured.push(bytes);
    // Capture console.log to verify the trace line.
    const realLog = console.log;
    const lines = [];
    console.log = (...a) => lines.push(a.join(' '));
    process.env.CURSOR_LOG_INTERACTION = '1';
    try {
      ca.handleInteractionQuery(iq, sendBinaryFrame, { passthroughNativeTools });
    } finally {
      console.log = realLog;
      delete process.env.CURSOR_LOG_INTERACTION;
    }
    if (captured.length !== 1) throw new Error(`expected 1 frame, got ${captured.length}`);
    const wrapper = fromBinary(agent.AgentClientMessageSchema, captured[0]);
    if (wrapper.message?.case !== 'interactionResponse') {
      throw new Error(`expected interactionResponse wrapper, got ${wrapper.message?.case}`);
    }
    const ir = wrapper.message.value;
    if (ir.id !== iq.id) throw new Error(`response.id mismatch: ${ir.id} !== ${iq.id}`);
    const resultCase = ir.result?.case;
    if (resultCase !== 'webSearchRequestResponse') {
      throw new Error(`expected webSearchRequestResponse, got ${resultCase}`);
    }
    const innerCase = ir.result.value?.result?.case;
    return { innerCase, trace: lines.find((l) => l.startsWith('[cursor-agent] interactionQuery')) };
  }

  // Scenario 1: passthrough off → reject.
  const off = runOnce(false);
  if (off.innerCase !== 'rejected') throw new Error(`passthrough=off should reject, got ${off.innerCase}`);
  if (!off.trace || !off.trace.includes('action=reject')) {
    throw new Error(`passthrough=off trace missing 'action=reject': ${off.trace}`);
  }
  console.log('OK passthrough=false → rejected; trace:', off.trace);

  // Scenario 2: passthrough on → approve.
  const on = runOnce(true);
  if (on.innerCase !== 'approved') throw new Error(`passthrough=on should approve, got ${on.innerCase}`);
  if (!on.trace || !on.trace.includes('action=approve')) {
    throw new Error(`passthrough=on trace missing 'action=approve': ${on.trace}`);
  }
  if (!on.trace.includes('search_term="latest Claude 5 release"')) {
    throw new Error(`passthrough=on trace missing search_term: ${on.trace}`);
  }
  console.log('OK passthrough=true → approved; trace:', on.trace);

  // Scenario 3: ExaSearch (passthrough on) should still reject — we only
  // approve WebSearch.
  const exaIq = create(agent.InteractionQuerySchema, {
    id: 9999,
    query: { case: 'exaSearchRequestQuery', value: create(agent.ExaSearchRequestQuerySchema, {}) },
  });
  const captured = [];
  ca.handleInteractionQuery(exaIq, (b) => captured.push(b), { passthroughNativeTools: true });
  const exaWrap = fromBinary(agent.AgentClientMessageSchema, captured[0]);
  const exaInner = exaWrap.message?.value?.result?.value?.result?.case;
  if (exaInner !== 'rejected') throw new Error(`exa with passthrough=on should still reject, got ${exaInner}`);
  console.log('OK exaSearch passthrough=true → still rejected (scope guard)');

  // Scenario 4: Cursor-native WebFetch is field 9 in newer proto builds.
  // The vendored proto does not know this oneof yet, so it arrives in
  // `$unknown`. We must approve/reject with raw field 9 instead of sending
  // a bare abandoned response that makes the model fall back to MCP.
  const webFetchArgs = protoMessage([
    protoFieldString(1, 'https://example.com/'),
    protoFieldString(2, 'toolu_wire_fetch'),
  ]);
  const webFetchQueryPayload = protoMessage([
    protoFieldBytes(1, webFetchArgs),
  ]);
  const webFetchIq = create(agent.InteractionQuerySchema, { id: 777 });
  webFetchIq.$unknown = [unknownLengthDelimited(9, webFetchQueryPayload)];

  function runWebFetchWire(passthroughNativeTools) {
    const captured = [];
    const serverEvents = [];
    const lines = [];
    const realLog = console.log;
    console.log = (...a) => lines.push(a.join(' '));
    process.env.CURSOR_LOG_INTERACTION = '1';
    try {
      ca.handleInteractionQuery(webFetchIq, (b) => captured.push(b), {
        passthroughNativeTools,
        onServerToolUse: (event) => serverEvents.push(event),
      });
    } finally {
      console.log = realLog;
      delete process.env.CURSOR_LOG_INTERACTION;
    }
    if (captured.length !== 1) throw new Error(`webFetch expected 1 frame, got ${captured.length}`);
    const wrapped = fromBinary(agent.AgentClientMessageSchema, captured[0]);
    if (wrapped.message?.case !== 'interactionResponse') {
      throw new Error(`webFetch expected interactionResponse, got ${wrapped.message?.case}`);
    }
    const ir = wrapped.message.value;
    if (ir.id !== 777) throw new Error(`webFetch response.id mismatch: ${ir.id}`);
    const raw = ir.$unknown?.find((u) => u.no === 9 && u.wireType === 2);
    if (!raw) throw new Error('webFetch response missing raw field 9');
    const responsePayload = readDelimited(Buffer.from(raw.data), 0).bytes;
    const responseFields = parseFields(responsePayload);
    return {
      responseCase: responseFields[0]?.no,
      serverEvents,
      trace: lines.find((l) => l.startsWith('[cursor-agent] interactionQuery')),
      abandoned: lines.find((l) => l.includes('abandoning so model falls back to MCP')),
    };
  }

  const wfOn = runWebFetchWire(true);
  if (wfOn.responseCase !== 1) throw new Error(`webFetch passthrough=on should approve field 1, got ${wfOn.responseCase}`);
  if (wfOn.abandoned) throw new Error(`webFetch passthrough=on must not abandon: ${wfOn.abandoned}`);
  if (!wfOn.trace || !wfOn.trace.includes('case=webFetchRequestQuery') || !wfOn.trace.includes('action=approve')) {
    throw new Error(`webFetch approve trace wrong: ${wfOn.trace}`);
  }
  if (wfOn.serverEvents[0]?.name !== 'web_fetch' || wfOn.serverEvents[0]?.input?.url !== 'https://example.com/') {
    throw new Error(`webFetch approve server event wrong: ${JSON.stringify(wfOn.serverEvents[0])}`);
  }
  console.log('OK webFetch field=9 passthrough=true → approved; trace:', wfOn.trace);

  const wfOff = runWebFetchWire(false);
  if (wfOff.responseCase !== 2) throw new Error(`webFetch passthrough=off should reject field 2, got ${wfOff.responseCase}`);
  if (wfOff.abandoned) throw new Error(`webFetch passthrough=off must not abandon: ${wfOff.abandoned}`);
  if (!wfOff.trace || !wfOff.trace.includes('action=reject')) {
    throw new Error(`webFetch reject trace wrong: ${wfOff.trace}`);
  }
  console.log('OK webFetch field=9 passthrough=false → rejected; trace:', wfOff.trace);

  // Scenario 5: Cursor's WebFetch tool completion is also field 37 on
  // ToolCall in the newer proto. Decode it from $unknown for observability.
  const webFetchSuccess = protoMessage([
    protoFieldString(1, 'https://example.com/'),
    protoFieldString(2, 'Example markdown body'),
  ]);
  const webFetchResult = protoMessage([
    protoFieldBytes(1, webFetchSuccess),
  ]);
  const webFetchToolCall = protoMessage([
    protoFieldBytes(1, webFetchArgs),
    protoFieldBytes(2, webFetchResult),
  ]);
  const toolCall = create(agent.ToolCallSchema, {});
  toolCall.$unknown = [unknownLengthDelimited(37, webFetchToolCall)];
  const wfEvent = ca.extractWebFetchServerToolEvent('toolCallCompleted', {
    callId: 'call_wire_fetch',
    toolCall,
  });
  if (!wfEvent || wfEvent.name !== 'web_fetch' || wfEvent.phase !== 'completed') {
    throw new Error(`webFetch field=37 event missing: ${JSON.stringify(wfEvent)}`);
  }
  if (wfEvent.content?.[0]?.content !== 'Example markdown body') {
    throw new Error(`webFetch field=37 markdown missing: ${JSON.stringify(wfEvent)}`);
  }
  console.log('OK webFetch ToolCall field=37 → server_tool_use completion decoded');

  console.log('\nAll assertions passed.');
}

run().catch((e) => {
  console.error('FAIL:', e.message);
  console.error(e.stack);
  process.exit(1);
});
