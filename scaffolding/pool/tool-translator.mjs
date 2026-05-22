// tool-translator.mjs — bidirectional name + arg translation between
// Cursor's built-in agent tools and Anthropic/claude-code's tool conventions.
//
// Empirical context (probed 2026-05-13 from the running pool):
//
//   When AgentService.Run is called with a NON-empty `tools` field,
//   Cursor's backend auto-injects its full default agent toolset into the
//   model's prompt:
//
//     Shell, Glob, Grep, Read, Delete, StrReplace, Write, EditNotebook,
//     TodoWrite, ReadLints, WebSearch, WebFetch, GenerateImage,
//     AskQuestion, Task, ListMcpResources, FetchMcpResource, SwitchMode
//
//   Plus whatever caller-defined tools we registered via mcpToolDefs
//   (always including bajie_yield).
//
//   When `tools` is EMPTY, Cursor injects nothing — the model only sees
//   bajie_yield.
//
// So in TRANSLATE mode the pool always opens with at least bajie_yield +
// a tiny placeholder tool to force Cursor to expose its defaults. The model
// then has the full Cursor agent toolset, and we translate names/args on
// the wire between it and the API caller (claude-code).
//
// Direction conventions:
//   cursorToAnthropic — model emitted a tool_use with Cursor's name; we
//     translate to the Anthropic-style name + args that the caller expects.
//     Used in api-server's on-tool_use path.
//   anthropicToCursor — caller sent a tool_result keyed to an Anthropic
//     name; we strip nothing on the way back (the tool_use_id is opaque
//     and the result content is free-form). Mostly a no-op; included for
//     symmetry.

// ── 1. Name table ───────────────────────────────────────────────────────
// Cursor name → Anthropic/claude-code name.
// Tools with the same name on both sides aren't listed here — passthrough.
const CURSOR_TO_ANTHROPIC_NAME = {
  Shell: 'Bash',
  StrReplace: 'Edit',
  EditNotebook: 'NotebookEdit',
  AskQuestion: 'AskUserQuestion',
};

const ANTHROPIC_TO_CURSOR_NAME = Object.fromEntries(
  Object.entries(CURSOR_TO_ANTHROPIC_NAME).map(([c, a]) => [a, c])
);

const MCP_WIRE_ALIAS_TOOL_NAMES = new Set([
  'AskQuestion', 'Delete', 'Edit', 'EditNotebook', 'FetchMcpResource',
  'GenerateImage', 'Glob', 'Grep', 'ListMcpResources', 'Read',
  'ReadLints', 'Shell', 'StrReplace', 'SwitchMode', 'Task',
  'TodoWrite', 'WebFetch', 'WebSearch', 'Write',
]);

function normalizeCursorToolName(cursorName) {
  const name = String(cursorName || '');
  if (name.startsWith('mcp_') && !name.startsWith('mcp__')) {
    const unprefixed = name.slice(4);
    if (MCP_WIRE_ALIAS_TOOL_NAMES.has(unprefixed)) return unprefixed;
  }
  return name;
}

// Tools Cursor provides natively but with no equivalent on the
// claude-code side. When the model calls these, the proxy returns a
// structured tool_error result rather than forwarding to the client.
const CURSOR_ONLY_TOOLS = new Set([
  'Delete',            // claude-code uses Bash `rm`
  'ReadLints',         // editor diagnostic — no claude-code analog
  'GenerateImage',     // image generation — out of scope
  'SwitchMode',        // Cursor IDE mode toggle — no analog
  'ListMcpResources',  // Cursor-internal MCP host stuff
  'FetchMcpResource',
]);

// ── 2. Arg adapters (Phase 2) ───────────────────────────────────────────
// Cursor's tool args → claude-code's tool args, when they differ.
// If a tool has identical arg names+shapes on both sides, no adapter is
// needed (we just rename via the table and pass args through verbatim).
//
// Schemas captured empirically by asking the inner agent for its
// JSON Schema. Adapters are best-effort: if a field's name is unknown,
// we fall through to passthrough.

const ARG_CURSOR_TO_ANTHROPIC = {
  // Shell({command, ...}) → Bash({command, description, timeout?, run_in_background?})
  // The Cursor and Anthropic shapes are very close; both have `command`.
  // We pass through and let claude-code shrug at unknown fields.
  Shell: (args) => {
    const out = { command: args.command || args.cmd || '' };
    if (args.description) out.description = args.description;
    if (args.timeout) out.timeout = args.timeout;
    if (args.run_in_background !== undefined) out.run_in_background = args.run_in_background;
    if (args.cwd && !out.command) out.command = `cd ${JSON.stringify(args.cwd)} && ${args.command || ''}`;
    return out;
  },

  // StrReplace → Edit: both take a file path + an old-string/new-string pair.
  // Cursor names the path field unknown-but-likely `file_path` or `path`.
  // Cursor names the strings unknown — could be old_str/new_str, old_string/new_string.
  // We accept either and emit claude-code's exact shape.
  StrReplace: (args) => {
    const file_path = args.file_path || args.path || args.target_file || '';
    const old_string = args.old_string || args.old_str || args.find || '';
    const new_string = args.new_string || args.new_str || args.replace || '';
    const replace_all = args.replace_all === true;
    return { file_path, old_string, new_string, ...(replace_all ? { replace_all: true } : {}) };
  },

  // EditNotebook → NotebookEdit. Likely takes notebook_path + cell_id + new_source.
  // Best-effort field mapping.
  EditNotebook: (args) => {
    const notebook_path = args.notebook_path || args.path || '';
    const cell_id = args.cell_id || args.cellId;
    const new_source = args.new_source || args.source || args.content || '';
    const edit_mode = args.edit_mode || args.mode || 'replace';
    const cell_type = args.cell_type || args.cellType;
    const out = { notebook_path, new_source };
    if (cell_id) out.cell_id = cell_id;
    if (edit_mode) out.edit_mode = edit_mode;
    if (cell_type) out.cell_type = cell_type;
    return out;
  },

  // AskQuestion → AskUserQuestion. claude-code's AskUserQuestion expects an
  // array of `questions`; Cursor's AskQuestion may use a flatter shape.
  AskQuestion: (args) => {
    if (Array.isArray(args.questions)) return { questions: args.questions };
    // Flatten: build one question from the args we see
    const q = {
      question: args.question || args.text || args.prompt || '',
      header: args.header || 'Question',
      multiSelect: !!args.multiSelect,
      options: Array.isArray(args.options)
        ? args.options.map((o) => (typeof o === 'string'
            ? { label: o, description: '' }
            : { label: o.label || String(o), description: o.description || '' }))
        : [
            { label: 'Yes', description: '' },
            { label: 'No', description: '' },
          ],
    };
    return { questions: [q] };
  },
};

// ── 3. Public surface ───────────────────────────────────────────────────

// Translate a Cursor-emitted tool_use into the Anthropic shape the caller expects.
// Returns either:
//   { ok: true, name, input }                   — forward to client
//   { ok: false, error, name }                  — return tool_error to inner agent
export function cursorToAnthropic(cursorName, cursorArgs) {
  cursorName = normalizeCursorToolName(cursorName);
  if (CURSOR_ONLY_TOOLS.has(cursorName)) {
    return {
      ok: false,
      name: cursorName,
      error: `Tool '${cursorName}' is a Cursor-only built-in with no claude-code equivalent. Use a different approach (e.g. Bash for file deletion).`,
    };
  }
  const anthropicName = CURSOR_TO_ANTHROPIC_NAME[cursorName] || cursorName;
  const adapter = ARG_CURSOR_TO_ANTHROPIC[cursorName];
  const input = adapter ? adapter(cursorArgs || {}) : (cursorArgs || {});
  return { ok: true, name: anthropicName, input };
}

// Forward a tool_result from claude-code back to the inner agent. The
// inner agent will see whatever string content we pass — Cursor doesn't
// re-validate the result against the tool's schema. Mostly a no-op.
export function anthropicResultToCursor(anthropicName, content) {
  return content;
}

// ── 4. Pool-open tool list for TRANSLATE mode ───────────────────────────
// Empirical observation: Cursor's backend injects its full default agent
// toolset into the model's prompt only when the caller registers at least
// one non-trivial-looking tool via mcpToolDefs. Tools whose names start
// with our internal `bajie_` prefix don't seem to trigger the injection
// (Cursor likely treats them as internal). Tools with empty schemas may
// also be skipped.
//
// We register Edit/Glob/NotebookEdit/TodoWrite under their Anthropic
// (claude-code) names directly so when the caller asks for `Edit`/`Glob`
// the model has a *matching* tool in its prompt with the EXACT schema
// claude-code documents. This avoids the model falling back to Cursor's
// `StrReplace`/Cursor-`Glob` (whose arg names differ — `path` vs
// `file_path`, etc.) and saying "X unavailable" when it can't reconcile
// schemas. Each tool whose name conflicts with a Cursor built-in gets the
// `mcp_` prefix on the wire (see cursor-agent.js CURSOR_NATIVE_TOOL_NAMES);
// the `toolName` field stays unprefixed so mcpArgs round-trips cleanly.
//
// search_codebase is kept as the placeholder that proves "tools.length>0"
// to Cursor's default-toolset injector.
export const CLIENT_MCP_DISPATCH_TOOL_NAME = 'client_mcp_call';

export function defaultTranslateModeTools() {
  return [
    {
      name: 'search_codebase',
      description:
        'Search across the codebase. (Proxy-internal placeholder — do not call this; ' +
        'use the appropriate Cursor-native tool like Grep or Read instead.)',
      input_schema: {
        type: 'object',
        properties: { query: { type: 'string', description: 'search query' } },
        required: ['query'],
      },
    },
    {
      name: CLIENT_MCP_DISPATCH_TOOL_NAME,
      description:
        'Call a client-declared MCP tool listed in the current request context. ' +
        'Use this for tools named like mcp__server__tool, including browser-devtools. ' +
        'Arguments: tool_name is the exact mcp__server__tool name; input is the JSON object to pass.',
      input_schema: {
        type: 'object',
        properties: {
          tool_name: {
            type: 'string',
            description: 'Exact client MCP tool name, for example mcp__browser-devtools__click.',
          },
          input: {
            type: 'object',
            description: 'JSON arguments for the selected client MCP tool.',
            additionalProperties: true,
          },
        },
        required: ['tool_name', 'input'],
      },
    },
    // Anthropic `Edit` — alias for Cursor's `StrReplace` but with the exact
    // schema claude-code emits. Receives via mcpArgs(toolName="Edit").
    {
      name: 'Edit',
      description:
        'Find-and-replace edit on a file. Required: file_path, old_string, new_string. ' +
        'old_string must match exactly (including indentation). Set replace_all=true to ' +
        'replace every occurrence. The new_string must differ from old_string.',
      input_schema: {
        type: 'object',
        properties: {
          file_path: { type: 'string', description: 'Absolute path to the file to modify.' },
          old_string: { type: 'string', description: 'Text to find. Must match exactly.' },
          new_string: { type: 'string', description: 'Replacement text. Must differ from old_string.' },
          replace_all: { type: 'boolean', description: 'Replace all occurrences (default false).' },
        },
        required: ['file_path', 'old_string', 'new_string'],
      },
    },
    // Anthropic `Glob` — file-pattern search.
    {
      name: 'Glob',
      description: 'Find files by glob pattern (e.g. "**/*.ts"). Returns matching paths sorted by modification time.',
      input_schema: {
        type: 'object',
        properties: {
          pattern: { type: 'string', description: 'The glob pattern to match against file paths.' },
          path: { type: 'string', description: 'Optional directory to search in (defaults to cwd).' },
        },
        required: ['pattern'],
      },
    },
    // Anthropic `NotebookEdit` — Cursor calls this `EditNotebook` natively;
    // by registering the Anthropic name we let claude-code's calls land
    // through mcpArgs directly without name translation.
    {
      name: 'NotebookEdit',
      description: 'Edit a Jupyter notebook cell. Args mirror claude-code: notebook_path, new_source, optional cell_id, cell_type, edit_mode.',
      input_schema: {
        type: 'object',
        properties: {
          notebook_path: { type: 'string', description: 'Absolute path to the .ipynb file.' },
          cell_id: { type: 'string', description: 'ID of the cell to edit (optional for insert).' },
          new_source: { type: 'string', description: 'New cell source text.' },
          edit_mode: { type: 'string', enum: ['replace', 'insert', 'delete'], description: 'Edit mode (default replace).' },
          cell_type: { type: 'string', enum: ['code', 'markdown'], description: 'Cell type for insert mode.' },
        },
        required: ['notebook_path', 'new_source'],
      },
    },
    // Anthropic `TodoWrite` — a popular built-in in claude-code workflows.
    // Cursor also has `TodoWrite` natively but its arg shape differs.
    {
      name: 'TodoWrite',
      description: 'Write or update the todo list for the current session. Pass an array of {subject, description, activeForm?} objects.',
      input_schema: {
        type: 'object',
        properties: {
          todos: {
            type: 'array',
            description: 'Array of todo items.',
            items: {
              type: 'object',
              properties: {
                subject: { type: 'string' },
                description: { type: 'string' },
                activeForm: { type: 'string' },
                status: { type: 'string', enum: ['pending', 'in_progress', 'completed'] },
              },
              required: ['subject'],
            },
          },
        },
        required: ['todos'],
      },
    },
  ];
}

// Names the model should never directly invoke (proxy-internal).
// `search_codebase` is internal scaffolding; `bajie_yield` is the relay
// signal. Edit/Glob/NotebookEdit/TodoWrite/client_mcp_call are registered
// as MCP tools (above) and DO bubble up to the API layer when called — they
// aren't internal.
export function isInternalTool(name) {
  return name === 'bajie_yield' || name === 'search_codebase';
}
