// Cursor native/tool bridge capability matrix.
//
// There are three different "tool" sources in this proxy:
//   1. Client-declared function/MCP tools supplied on each OpenAI/Anthropic
//      request. These can be third-party tools from Codex/Claude clients.
//   2. Cursor native protocol tools from agent_pb.mjs. These are built into
//      Cursor's agent protocol, even when the client declares zero tools.
//   3. Cursor internal bookkeeping surfaces such as KV/blob/state helpers.
//
// Keep this file data-only so server.js can expose it without loading proto
// descriptors or reaching Cursor upstream.

const EXEC_RUNTIME_TOOLS = [
  { field: 'id', source: 'cursor-native-exec-metadata', status: 'metadata' },
  { field: 'execId', source: 'cursor-native-exec-metadata', status: 'metadata' },
  { field: 'spanContext', source: 'cursor-native-exec-metadata', status: 'metadata' },
  { field: 'shellArgs', source: 'cursor-native-exec', status: 'passthrough-optional', bridgeTool: 'Bash' },
  { field: 'shellStreamArgs', source: 'cursor-native-exec', status: 'passthrough-optional', bridgeTool: 'Bash' },
  { field: 'backgroundShellSpawnArgs', source: 'cursor-native-exec', status: 'passthrough-optional', bridgeTool: 'Bash' },
  { field: 'writeShellStdinArgs', source: 'cursor-native-exec', status: 'reject', reason: 'No persistent shell stdin channel is exposed to the client.' },
  { field: 'readArgs', source: 'cursor-native-exec', status: 'passthrough-optional', bridgeTool: 'Read' },
  { field: 'writeArgs', source: 'cursor-native-exec', status: 'passthrough-optional', bridgeTool: 'Write' },
  { field: 'deleteArgs', source: 'cursor-native-exec', status: 'reject', reason: 'Destructive native file deletion is not executed by the headless proxy.' },
  { field: 'grepArgs', source: 'cursor-native-exec', status: 'passthrough-optional', bridgeTool: 'Grep' },
  { field: 'lsArgs', source: 'cursor-native-exec', status: 'reject', reason: 'No stable native LS result bridge is implemented yet.' },
  { field: 'diagnosticsArgs', source: 'cursor-native-exec', status: 'empty-success', reason: 'Headless proxy has no IDE diagnostics engine.' },
  { field: 'requestContextArgs', source: 'cursor-native-exec', status: 'handled-internal', reason: 'Returns environment and client-declared MCP/function tools.' },
  { field: 'mcpArgs', source: 'client-declared-third-party-or-local', status: 'forwarded', reason: 'Forwards the current request tools back to the API client.' },
  { field: 'listMcpResourcesExecArgs', source: 'cursor-native-exec', status: 'empty-success', reason: 'No MCP resource servers are mounted inside the proxy.' },
  { field: 'readMcpResourceExecArgs', source: 'cursor-native-exec', status: 'reject', reason: 'No MCP resource servers are mounted inside the proxy.' },
  { field: 'fetchArgs', source: 'cursor-native-exec', status: 'reject-by-default', bridgeTool: 'WebFetch', reason: 'Local server WebFetch is disabled unless CURSOR_SERVER_WEBFETCH=1; public web lookup should use Cursor backend WebSearch.' },
  { field: 'recordScreenArgs', source: 'cursor-native-exec', status: 'reject', reason: 'Headless service has no screen recorder.' },
  { field: 'computerUseArgs', source: 'cursor-native-exec', status: 'reject', reason: 'Headless service has no browser/desktop computer-use executor.' },
];

const INTERACTION_RUNTIME_TOOLS = [
  { field: 'id', source: 'cursor-native-interaction-metadata', status: 'metadata' },
  { field: 'webSearchRequestQuery', source: 'cursor-native-backend-search', status: 'approve-optional', bridgeTool: 'Cursor backend WebSearch', reason: 'When passthrough is enabled Cursor performs search server-side; otherwise rejected.' },
  { field: 'askQuestionInteractionQuery', source: 'cursor-native-interaction', status: 'reject', reason: 'No interactive user prompt loop is available inside API request handling.' },
  { field: 'switchModeRequestQuery', source: 'cursor-native-interaction', status: 'reject', reason: 'API clients choose mode/model at request routing time.' },
  { field: 'exaSearchRequestQuery', source: 'cursor-native-third-party-backend', status: 'reject', reason: 'Exa is a Cursor backend/third-party integration, not a client-declared local tool.' },
  { field: 'exaFetchRequestQuery', source: 'cursor-native-third-party-backend', status: 'reject', reason: 'Exa is a Cursor backend/third-party integration, not a client-declared local tool.' },
  { field: 'createPlanRequestQuery', source: 'cursor-native-interaction', status: 'reject', reason: 'Planning artifacts are not persisted by the headless proxy.' },
  { field: 'setupVmEnvironmentArgs', source: 'cursor-native-interaction', status: 'empty-success', reason: 'Cursor proto exposes no rejection variant; the proxy acknowledges without provisioning a VM.' },
];

const TOOL_CALL_HISTORY_FIELDS = [
  'shellToolCall',
  'deleteToolCall',
  'globToolCall',
  'grepToolCall',
  'readToolCall',
  'updateTodosToolCall',
  'readTodosToolCall',
  'editToolCall',
  'lsToolCall',
  'readLintsToolCall',
  'mcpToolCall',
  'semSearchToolCall',
  'createPlanToolCall',
  'webSearchToolCall',
  'taskToolCall',
  'listMcpResourcesToolCall',
  'readMcpResourceToolCall',
  'applyAgentDiffToolCall',
  'askQuestionToolCall',
  'fetchToolCall',
  'switchModeToolCall',
  'exaSearchToolCall',
  'exaFetchToolCall',
  'generateImageToolCall',
  'recordScreenToolCall',
  'computerUseToolCall',
  'writeShellStdinToolCall',
  'reflectToolCall',
  'setupVmEnvironmentToolCall',
  'truncatedToolCall',
  'startGrindExecutionToolCall',
  'startGrindPlanningToolCall',
];

const ARG_SCHEMA_TO_SOURCE = [
  { schema: 'AbortArgsSchema', tool: 'Abort', source: 'cursor-internal', status: 'internal' },
  { schema: 'ApplyAgentDiffArgsSchema', tool: 'ApplyAgentDiff', source: 'cursor-native-history', status: 'telemetry-only' },
  { schema: 'AskQuestionArgsSchema', tool: 'AskQuestion', source: 'cursor-native-interaction', status: 'reject' },
  { schema: 'BackgroundShellSpawnArgsSchema', tool: 'BackgroundShellSpawn', source: 'cursor-native-exec', status: 'passthrough-optional', bridgeTool: 'Bash' },
  { schema: 'ComputerUseArgsSchema', tool: 'ComputerUse', source: 'cursor-native-exec', status: 'reject' },
  { schema: 'CreatePlanArgsSchema', tool: 'CreatePlan', source: 'cursor-native-interaction', status: 'reject' },
  { schema: 'DeleteArgsSchema', tool: 'Delete', source: 'cursor-native-exec', status: 'reject' },
  { schema: 'DiagnosticsArgsSchema', tool: 'Diagnostics', source: 'cursor-native-exec', status: 'empty-success' },
  { schema: 'EditArgsSchema', tool: 'Edit', source: 'cursor-native-history', status: 'telemetry-only' },
  { schema: 'ExaFetchArgsSchema', tool: 'ExaFetch', source: 'cursor-native-third-party-backend', status: 'reject' },
  { schema: 'ExaSearchArgsSchema', tool: 'ExaSearch', source: 'cursor-native-third-party-backend', status: 'reject' },
  { schema: 'FetchArgsSchema', tool: 'Fetch', source: 'cursor-native-exec', status: 'reject-by-default', bridgeTool: 'WebFetch', reason: 'Local server WebFetch is disabled unless CURSOR_SERVER_WEBFETCH=1; public web lookup should use Cursor backend WebSearch.' },
  { schema: 'GenerateImageArgsSchema', tool: 'GenerateImage', source: 'cursor-native-backend-generation', status: 'unsupported' },
  { schema: 'GetBlobArgsSchema', tool: 'GetBlob', source: 'cursor-internal-kv', status: 'handled-internal' },
  { schema: 'GrepArgsSchema', tool: 'Grep', source: 'cursor-native-exec', status: 'passthrough-optional', bridgeTool: 'Grep' },
  { schema: 'ListMcpResourcesExecArgsSchema', tool: 'ListMcpResources', source: 'cursor-native-exec', status: 'empty-success' },
  { schema: 'LsArgsSchema', tool: 'Ls', source: 'cursor-native-exec', status: 'reject' },
  { schema: 'McpArgsSchema', tool: 'Mcp', source: 'client-declared-third-party-or-local', status: 'forwarded' },
  { schema: 'ReadArgsSchema', tool: 'Read', source: 'cursor-native-exec', status: 'passthrough-optional', bridgeTool: 'Read' },
  { schema: 'ReadLintsToolArgsSchema', tool: 'ReadLintsTool', source: 'cursor-native-history', status: 'telemetry-only' },
  { schema: 'ReadMcpResourceExecArgsSchema', tool: 'ReadMcpResource', source: 'cursor-native-exec', status: 'reject' },
  { schema: 'ReadTodosArgsSchema', tool: 'ReadTodos', source: 'cursor-native-history', status: 'telemetry-only' },
  { schema: 'ReadToolArgsSchema', tool: 'ReadTool', source: 'cursor-native-history', status: 'telemetry-only' },
  { schema: 'RecordScreenArgsSchema', tool: 'RecordScreen', source: 'cursor-native-exec', status: 'reject' },
  { schema: 'ReflectArgsSchema', tool: 'Reflect', source: 'cursor-native-history', status: 'telemetry-only' },
  { schema: 'RequestContextArgsSchema', tool: 'RequestContext', source: 'cursor-native-exec', status: 'handled-internal' },
  { schema: 'SemSearchToolArgsSchema', tool: 'SemSearchTool', source: 'cursor-native-history', status: 'telemetry-only' },
  { schema: 'SetBlobArgsSchema', tool: 'SetBlob', source: 'cursor-internal-kv', status: 'handled-internal' },
  { schema: 'SetupVmEnvironmentArgsSchema', tool: 'SetupVmEnvironment', source: 'cursor-native-interaction', status: 'empty-success' },
  { schema: 'ShellArgsSchema', tool: 'Shell', source: 'cursor-native-exec', status: 'passthrough-optional', bridgeTool: 'Bash' },
  { schema: 'StartGrindExecutionArgsSchema', tool: 'StartGrindExecution', source: 'cursor-native-history', status: 'telemetry-only' },
  { schema: 'StartGrindPlanningArgsSchema', tool: 'StartGrindPlanning', source: 'cursor-native-history', status: 'telemetry-only' },
  { schema: 'SwitchModeArgsSchema', tool: 'SwitchMode', source: 'cursor-native-interaction', status: 'reject' },
  { schema: 'TaskArgsSchema', tool: 'Task', source: 'cursor-native-history', status: 'telemetry-only' },
  { schema: 'TruncatedToolCallArgsSchema', tool: 'TruncatedToolCall', source: 'cursor-native-history', status: 'telemetry-only' },
  { schema: 'UpdateTodosArgsSchema', tool: 'UpdateTodos', source: 'cursor-native-history', status: 'telemetry-only' },
  { schema: 'WebSearchArgsSchema', tool: 'WebSearch', source: 'cursor-native-backend-search', status: 'approve-optional', bridgeTool: 'Cursor backend WebSearch' },
  { schema: 'WriteArgsSchema', tool: 'Write', source: 'cursor-native-exec', status: 'passthrough-optional', bridgeTool: 'Write' },
  { schema: 'WriteShellStdinArgsSchema', tool: 'WriteShellStdin', source: 'cursor-native-exec', status: 'reject' },
];

function summarizeStatus(items) {
  const out = {};
  for (const item of items) {
    const key = item.status || 'unknown';
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function getCursorToolMatrix(options = {}) {
  const passthroughNativeTools = options.passthroughNativeTools === true;
  const clientTools = Array.isArray(options.clientTools) ? options.clientTools : [];
  return {
    passthroughNativeTools,
    explanation: {
      fourTools: 'If a request shows tools=4, that is the number of client-declared function/MCP tools in that request, not the Cursor native protocol surface.',
      cursorNative: 'Cursor native tools come from src/proto/agent_pb.mjs and may be requested by Cursor upstream even when the API client declares no tools.',
      thirdParty: 'Only client-declared tools and Cursor backend integrations such as Exa/WebSearch should be considered third-party. Shell/read/write/fetch/grep/etc. are Cursor native protocol tools.',
      webLookupPolicy: 'By default, client-declared WebSearch/Search/WebFetch/Fetch tools are not forwarded into RequestContext.tools. Public web lookup is expected to use Cursor native WebSearch; local WebFetch is enabled only with CURSOR_SERVER_WEBFETCH=1.',
    },
    requestDeclaredTools: {
      count: clientTools.length,
      source: 'client-declared-third-party-or-local',
      tools: clientTools.map((tool) => ({
        name: tool && (tool.name || tool.toolName || tool.function?.name) || '',
        type: tool && (tool.type || (tool.function ? 'function' : 'mcp')) || 'unknown',
      })),
    },
    counts: {
      argSchemas: ARG_SCHEMA_TO_SOURCE.length,
      execRuntimeFields: EXEC_RUNTIME_TOOLS.length,
      execRuntimeToolCases: EXEC_RUNTIME_TOOLS.filter((t) => t.status !== 'metadata').length,
      interactionRuntimeFields: INTERACTION_RUNTIME_TOOLS.length,
      interactionRuntimeToolCases: INTERACTION_RUNTIME_TOOLS.filter((t) => t.status !== 'metadata').length,
      toolCallHistoryFields: TOOL_CALL_HISTORY_FIELDS.length,
    },
    statusCounts: {
      argSchemas: summarizeStatus(ARG_SCHEMA_TO_SOURCE),
      execRuntime: summarizeStatus(EXEC_RUNTIME_TOOLS),
      interactionRuntime: summarizeStatus(INTERACTION_RUNTIME_TOOLS),
    },
    execRuntimeTools: EXEC_RUNTIME_TOOLS,
    interactionRuntimeTools: INTERACTION_RUNTIME_TOOLS,
    toolCallHistoryFields: TOOL_CALL_HISTORY_FIELDS,
    argSchemas: ARG_SCHEMA_TO_SOURCE,
  };
}

module.exports = {
  EXEC_RUNTIME_TOOLS,
  INTERACTION_RUNTIME_TOOLS,
  TOOL_CALL_HISTORY_FIELDS,
  ARG_SCHEMA_TO_SOURCE,
  getCursorToolMatrix,
};
