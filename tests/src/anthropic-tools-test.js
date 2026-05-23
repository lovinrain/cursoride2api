#!/usr/bin/env node

const assert = require('node:assert/strict');
const tools = require('../../src/anthropic-tools');

const oldServerWebFetch = process.env.CURSOR_SERVER_WEBFETCH;

{
  const registered = new Set(['Glob', 'TodoWrite']);
  assert.equal(tools.normalizeMcpWireToolNameForClient('mcp_Glob', registered), 'Glob');
  assert.equal(tools.normalizeMcpWireToolNameForClient('mcp_TodoWrite', registered), 'TodoWrite');
}

{
  const registered = new Set(['Glob']);
  assert.equal(tools.normalizeMcpWireToolNameForClient('mcp_Write', registered), 'Write');
  assert.equal(tools.normalizeMcpWireToolNameForClient('mcp_WebFetch', registered), 'WebFetch');
  assert.equal(tools.normalizeMcpWireToolNameForClient('mcp_Unknown', registered), 'mcp_Unknown');
  assert.equal(tools.normalizeMcpWireToolNameForClient('mcp__github__search', registered), 'mcp__github__search');
}

{
  const registered = new Set(['Glob']);
  assert.equal(tools.canonicalizeHallucinatedToolName('mcp_Glob', registered), 'Glob');
}

{
  const registered = new Set(['Edit']);
  assert.equal(tools.canonicalizeHallucinatedToolName('StrReplace', registered), 'Edit');
  assert.deepEqual(tools.normalizeHallucinatedToolArgs('Edit', {
    path: '/tmp/a.txt',
    oldString: 'old',
    replacement: 'new',
    replaceAll: true,
  }), {
    file_path: '/tmp/a.txt',
    old_string: 'old',
    new_string: 'new',
    replace_all: true,
  });
}

{
  assert.deepEqual(tools.normalizeHallucinatedToolArgs('Grep', {
    pattern: 'TODO',
    path: '/tmp',
  }), {
    pattern: 'TODO',
    path: '/tmp',
    output_mode: 'files_with_matches',
  });
}

{
  delete process.env.CURSOR_SERVER_WEBFETCH;
  assert.equal(tools.shouldDropClientWebLookupToolName('WebFetch'), false);
  assert.equal(tools.shouldDropClientWebLookupToolName('Fetch'), false);
  process.env.CURSOR_SERVER_WEBFETCH = '0';
  assert.equal(tools.shouldDropClientWebLookupToolName('WebFetch'), true);
  assert.equal(tools.shouldDropClientWebLookupToolName('Fetch'), true);
  if (oldServerWebFetch == null) delete process.env.CURSOR_SERVER_WEBFETCH;
  else process.env.CURSOR_SERVER_WEBFETCH = oldServerWebFetch;
}

console.log('anthropic-tools-test: OK');
