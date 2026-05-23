#!/usr/bin/env node

import assert from 'node:assert/strict';
import {
  CLIENT_MCP_DISPATCH_TOOL_NAME,
  cursorToAnthropic,
  defaultTranslateModeTools,
} from '../../scaffolding/pool/tool-translator.mjs';

{
  const out = cursorToAnthropic('Shell', { command: 'pwd' });
  assert.equal(out.ok, true);
  assert.equal(out.name, 'Bash');
  assert.deepEqual(out.input, { command: 'pwd' });
}

{
  const out = cursorToAnthropic('mcp_Glob', { pattern: '/tmp/*.txt' });
  assert.equal(out.ok, true);
  assert.equal(out.name, 'Glob');
  assert.deepEqual(out.input, { pattern: '/tmp/*.txt' });
}

{
  const out = cursorToAnthropic('mcp_TodoWrite', { todos: [{ subject: 'fix tools' }] });
  assert.equal(out.ok, true);
  assert.equal(out.name, 'TodoWrite');
  assert.deepEqual(out.input, { todos: [{ subject: 'fix tools' }] });
}

{
  const out = cursorToAnthropic('mcp_WebFetch', { url: 'https://example.com/' });
  assert.equal(out.ok, true);
  assert.equal(out.name, 'WebFetch');
  assert.deepEqual(out.input, { url: 'https://example.com/' });
}

{
  const out = cursorToAnthropic('mcp_Write', { file_path: '/tmp/a.txt', content: 'hello' });
  assert.equal(out.ok, true);
  assert.equal(out.name, 'Write');
  assert.deepEqual(out.input, { file_path: '/tmp/a.txt', content: 'hello' });
}

{
  const out = cursorToAnthropic('StrReplace', {
    path: '/tmp/a.txt',
    oldString: 'old',
    replacement: 'new',
    replaceAll: true,
  });
  assert.equal(out.ok, true);
  assert.equal(out.name, 'Edit');
  assert.deepEqual(out.input, {
    file_path: '/tmp/a.txt',
    old_string: 'old',
    new_string: 'new',
    replace_all: true,
  });
}

{
  const out = cursorToAnthropic('Grep', { pattern: 'TODO', path: '/tmp' });
  assert.equal(out.ok, true);
  assert.equal(out.name, 'Grep');
  assert.deepEqual(out.input, {
    pattern: 'TODO',
    path: '/tmp',
    output_mode: 'files_with_matches',
  });
}

{
  const out = cursorToAnthropic('mcp__github__search', { q: 'repo' });
  assert.equal(out.ok, true);
  assert.equal(out.name, 'mcp__github__search');
  assert.deepEqual(out.input, { q: 'repo' });
}

{
  const tools = defaultTranslateModeTools();
  assert.ok(tools.some((t) => t.name === CLIENT_MCP_DISPATCH_TOOL_NAME));
  assert.ok(tools.some((t) => t.name === 'WebFetch'));
}

console.log('tool-translator-test: OK');
