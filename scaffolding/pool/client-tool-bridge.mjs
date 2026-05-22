import {
  normalizeAnthropicContentForCursorMcp,
} from './multimodal-content.mjs';

const MAX_PENDING = Math.max(16, parseInt(process.env.RATLC_CLIENT_TOOL_BRIDGE_MAX || '512', 10));
const TTL_MS = Math.max(60_000, parseInt(process.env.RATLC_CLIENT_TOOL_BRIDGE_TTL_MS || '1800000', 10));

export const CLIENT_MCP_DISPATCH_TOOL_NAME = 'client_mcp_call';

const pendingByClientId = new Map();
const consumedByClientId = new Map();

function nowMs() {
  return Date.now();
}

function evictExpired(now = nowMs()) {
  for (const [id, entry] of pendingByClientId) {
    if (now - entry.createdAt > TTL_MS) pendingByClientId.delete(id);
  }
  for (const [id, entry] of consumedByClientId) {
    if (now - entry.consumedAt > TTL_MS) consumedByClientId.delete(id);
  }
  while (pendingByClientId.size > MAX_PENDING) {
    const first = pendingByClientId.keys().next().value;
    if (first === undefined) break;
    pendingByClientId.delete(first);
  }
  while (consumedByClientId.size > MAX_PENDING) {
    const first = consumedByClientId.keys().next().value;
    if (first === undefined) break;
    consumedByClientId.delete(first);
  }
}

export function isClientBridgeToolName(name) {
  const raw = String(name || '').trim();
  if (!raw.startsWith('mcp__')) return false;
  const parts = raw.split('__');
  return !!(parts.length >= 3 && parts[1] && parts.slice(2).join('__'));
}

export function findClientToolDefinition(tools, name) {
  if (!Array.isArray(tools)) return null;
  return tools.find((t) => t && t.name === name) || null;
}

export function listClientBridgeToolNames(tools) {
  if (!Array.isArray(tools)) return [];
  return tools
    .map((t) => t && t.name)
    .filter((name) => isClientBridgeToolName(name));
}

function isPlainObject(value) {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function normalizeDispatchedToolName(rawName, args = {}, tools = []) {
  const raw = String(rawName || '').trim();
  if (isClientBridgeToolName(raw)) return raw;

  const server = String(
    args.server_name || args.server || args.mcp_server || args.mcpServer || ''
  ).trim();
  if (server && raw) return `mcp__${server}__${raw}`;

  if (raw.includes('/')) {
    const parts = raw.split('/').map((s) => s.trim()).filter(Boolean);
    if (parts.length >= 2) return `mcp__${parts[0]}__${parts.slice(1).join('__')}`;
  }

  if (raw.includes('.')) {
    const parts = raw.split('.').map((s) => s.trim()).filter(Boolean);
    if (parts.length >= 2) return `mcp__${parts[0]}__${parts.slice(1).join('__')}`;
  }

  const suffixMatches = listClientBridgeToolNames(tools)
    .filter((name) => name.endsWith(`__${raw}`));
  if (suffixMatches.length === 1) return suffixMatches[0];

  return raw;
}

function extractDispatcherInput(args = {}) {
  if (isPlainObject(args.input)) return args.input;
  if (isPlainObject(args.arguments)) return args.arguments;
  if (isPlainObject(args.args)) return args.args;

  const meta = new Set([
    'tool_name', 'toolName', 'name', 'tool', 'mcp_tool_name', 'mcpToolName',
    'server_name', 'serverName', 'server', 'mcp_server', 'mcpServer',
    'input', 'arguments', 'args',
  ]);
  const out = {};
  for (const [key, value] of Object.entries(args || {})) {
    if (!meta.has(key)) out[key] = value;
  }
  return out;
}

export function resolveClientBridgeToolUse(tools, emittedToolName, emittedInput = {}) {
  const name = String(emittedToolName || '').trim();
  const input = isPlainObject(emittedInput) ? emittedInput : {};

  if (isClientBridgeToolName(name)) {
    const definition = findClientToolDefinition(tools, name);
    if (!definition) {
      return {
        ok: false,
        handled: true,
        clientToolName: name,
        error: `Client MCP tool '${name}' was not declared by this request.`,
      };
    }
    return { ok: true, handled: true, clientToolName: name, clientInput: input, definition };
  }

  if (name !== CLIENT_MCP_DISPATCH_TOOL_NAME) {
    return { ok: false, handled: false, error: 'not a client MCP bridge tool' };
  }

  const requested = input.tool_name || input.toolName || input.name || input.tool || input.mcp_tool_name || input.mcpToolName;
  const clientToolName = normalizeDispatchedToolName(requested, input, tools);
  if (!isClientBridgeToolName(clientToolName)) {
    return {
      ok: false,
      handled: true,
      clientToolName,
      error:
        `client_mcp_call requires a client MCP tool name like ` +
        `'mcp__server__tool' plus an input object.`,
    };
  }

  const definition = findClientToolDefinition(tools, clientToolName);
  if (!definition) {
    const available = listClientBridgeToolNames(tools).slice(0, 30).join(', ');
    return {
      ok: false,
      handled: true,
      clientToolName,
      error:
        `Client MCP tool '${clientToolName}' was not declared by this request.` +
        (available ? ` Available client MCP tools: ${available}` : ' No client MCP tools were declared.'),
    };
  }

  return {
    ok: true,
    handled: true,
    clientToolName,
    clientInput: extractDispatcherInput(input),
    definition,
  };
}

export function rememberClientToolUse({ clientToolUseId, poolToolUseId, requestId, toolName, input }) {
  if (!clientToolUseId || !poolToolUseId) return null;
  evictExpired();
  const entry = {
    clientToolUseId,
    poolToolUseId,
    requestId: requestId || '',
    toolName: toolName || '',
    input: input || {},
    createdAt: nowMs(),
  };
  pendingByClientId.set(clientToolUseId, entry);
  return entry;
}

export function consumeClientToolResult(clientToolUseId) {
  if (!clientToolUseId) return null;
  evictExpired();
  const entry = pendingByClientId.get(clientToolUseId);
  if (!entry) return null;
  pendingByClientId.delete(clientToolUseId);
  consumedByClientId.set(clientToolUseId, {
    ...entry,
    consumedAt: nowMs(),
  });
  return entry;
}

export function getConsumedClientToolResult(clientToolUseId) {
  if (!clientToolUseId) return null;
  evictExpired();
  return consumedByClientId.get(clientToolUseId) || null;
}

export function pendingClientToolCount() {
  evictExpired();
  return pendingByClientId.size;
}

export function consumedClientToolCount() {
  evictExpired();
  return consumedByClientId.size;
}

export function normalizeAnthropicToolResultContentForCursorMcp(content) {
  return normalizeAnthropicContentForCursorMcp(content);
}

export function buildPoolToolResultContentFromClientResult(result) {
  if (!result) return { items: [{ kind: 'text', text: '' }] };
  if (result.isError) return { error: result.text || 'Tool execution failed' };
  return normalizeAnthropicToolResultContentForCursorMcp(
    result.content === undefined ? result.text : result.content,
  );
}
