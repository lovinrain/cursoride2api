#!/usr/bin/env node

import assert from 'node:assert/strict';
import {
  CLIENT_MCP_DISPATCH_TOOL_NAME,
  buildPoolToolResultContentFromClientResult,
  consumedClientToolCount,
  consumeClientToolResult,
  findClientToolDefinition,
  getConsumedClientToolResult,
  isClientBridgeToolName,
  pendingClientToolCount,
  rememberClientToolUse,
  resolveClientBridgeToolUse,
  normalizeAnthropicToolResultContentForCursorMcp,
} from '../../scaffolding/pool/client-tool-bridge.mjs';

assert.equal(isClientBridgeToolName('mcp__browser-devtools__click'), true);
assert.equal(isClientBridgeToolName('mcp__playwright__browser_snapshot'), true);
assert.equal(isClientBridgeToolName('mcp_Glob'), false);
assert.equal(isClientBridgeToolName('Glob'), false);
assert.equal(isClientBridgeToolName('mcp__broken'), false);

const tools = [
  { name: 'Bash' },
  { name: 'mcp__browser-devtools__click', input_schema: { type: 'object' } },
];
assert.equal(findClientToolDefinition(tools, 'mcp__browser-devtools__click'), tools[1]);
assert.equal(findClientToolDefinition(tools, 'mcp__browser-devtools__missing'), null);

{
  const resolved = resolveClientBridgeToolUse(tools, 'mcp__browser-devtools__click', { selector: '#ok' });
  assert.equal(resolved.ok, true);
  assert.equal(resolved.handled, true);
  assert.equal(resolved.clientToolName, 'mcp__browser-devtools__click');
  assert.deepEqual(resolved.clientInput, { selector: '#ok' });
}

{
  const resolved = resolveClientBridgeToolUse(tools, CLIENT_MCP_DISPATCH_TOOL_NAME, {
    tool_name: 'mcp__browser-devtools__click',
    input: { selector: '#ok' },
  });
  assert.equal(resolved.ok, true);
  assert.equal(resolved.clientToolName, 'mcp__browser-devtools__click');
  assert.deepEqual(resolved.clientInput, { selector: '#ok' });
}

{
  const resolved = resolveClientBridgeToolUse(tools, CLIENT_MCP_DISPATCH_TOOL_NAME, {
    server_name: 'browser-devtools',
    tool_name: 'click',
    input: { selector: '#ok' },
  });
  assert.equal(resolved.ok, true);
  assert.equal(resolved.clientToolName, 'mcp__browser-devtools__click');
  assert.deepEqual(resolved.clientInput, { selector: '#ok' });
}

{
  const resolved = resolveClientBridgeToolUse(tools, CLIENT_MCP_DISPATCH_TOOL_NAME, {
    tool_name: 'mcp__browser-devtools__missing',
    input: {},
  });
  assert.equal(resolved.ok, false);
  assert.equal(resolved.handled, true);
  assert.match(resolved.error, /not declared/);
}

rememberClientToolUse({
  clientToolUseId: 'toolu_client_abc',
  poolToolUseId: 'toolu_pool_xyz',
  requestId: 'req-1',
  toolName: 'mcp__browser-devtools__click',
  input: { selector: '#ok' },
});
assert.equal(pendingClientToolCount(), 1);

const entry = consumeClientToolResult('toolu_client_abc');
assert.equal(entry.poolToolUseId, 'toolu_pool_xyz');
assert.equal(entry.toolName, 'mcp__browser-devtools__click');
assert.deepEqual(entry.input, { selector: '#ok' });
assert.equal(consumedClientToolCount(), 1);
const consumed = getConsumedClientToolResult('toolu_client_abc');
assert.equal(consumed.poolToolUseId, 'toolu_pool_xyz');
assert.equal(consumeClientToolResult('toolu_client_abc'), null);
assert.equal(pendingClientToolCount(), 0);

{
  const pngBase64 = Buffer.from('png-bytes').toString('base64');
  const normalized = normalizeAnthropicToolResultContentForCursorMcp([
    { type: 'text', text: 'screenshot captured' },
    { type: 'image', source: { type: 'base64', media_type: 'image/png', data: pngBase64 } },
  ]);
  assert.deepEqual(normalized.items[0], { kind: 'text', text: 'screenshot captured' });
  assert.equal(normalized.items[1].kind, 'image');
  assert.equal(normalized.items[1].mediaType, 'image/png');
  assert.equal(normalized.items[1].dataBase64, pngBase64);
  assert.equal(Object.hasOwn(normalized.items[1], 'data'), false);
}

{
  const payload = buildPoolToolResultContentFromClientResult({
    tool_use_id: 'toolu_client_screenshot',
    text: 'fallback text',
    content: [
      { type: 'text', text: 'devtools screenshot' },
      { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'cG5n' } },
    ],
    isError: false,
  });
  assert.equal(payload.items.length, 2);
  assert.deepEqual(payload.items[0], { kind: 'text', text: 'devtools screenshot' });
  assert.deepEqual(payload.items[1], {
    kind: 'image',
    mediaType: 'image/png',
    dataBase64: 'cG5n',
  });
}

{
  const payload = buildPoolToolResultContentFromClientResult({
    text: 'permission denied',
    isError: true,
  });
  assert.deepEqual(payload, { error: 'permission denied' });
}

console.log('client-tool-bridge-test: OK');
