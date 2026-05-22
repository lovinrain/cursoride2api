#!/usr/bin/env node

import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {
  isPoolLocalToolName,
  normalizePoolLocalToolName,
  runPoolLocalTool,
} from '../../scaffolding/pool/local-tool-executor.mjs';

const root = await fs.mkdtemp(path.join(os.tmpdir(), 'ratlc-local-tools-'));

try {
  await fs.mkdir(path.join(root, 'src'), { recursive: true });
  await fs.mkdir(path.join(root, 'notes'), { recursive: true });
  await fs.writeFile(path.join(root, 'src', 'a.js'), 'const TODO = 1;\nconsole.log(TODO);\n');
  await fs.writeFile(path.join(root, 'src', 'b.txt'), 'plain text\nTODO second\n');
  await fs.writeFile(path.join(root, 'notes', 'c.md'), '# TODO\n');

  assert.equal(normalizePoolLocalToolName('mcp_Grep'), 'Grep');
  assert.equal(normalizePoolLocalToolName('mcp_Glob'), 'Glob');
  assert.equal(normalizePoolLocalToolName('mcp_WebFetch'), 'WebFetch');
  assert.equal(normalizePoolLocalToolName('mcp__github__search'), '');
  assert.equal(isPoolLocalToolName('Fetch'), true);
  assert.equal(isPoolLocalToolName('Bash'), false);

  {
    const res = await runPoolLocalTool('Glob', { pattern: '**/*.js', path: root });
    assert.equal(res.ok, true);
    assert.match(res.content, /src\/a\.js/);
    assert.doesNotMatch(res.content, /b\.txt/);
  }

  {
    const res = await runPoolLocalTool('Grep', {
      pattern: 'TODO',
      path: root,
      glob: '**/*.js',
      output_mode: 'files_with_matches',
    });
    assert.equal(res.ok, true);
    assert.match(res.content, /src\/a\.js/);
    assert.doesNotMatch(res.content, /b\.txt/);
  }

  {
    const res = await runPoolLocalTool('Grep', {
      pattern: 'TODO',
      path: root,
      glob: '**/*.txt',
      output_mode: 'content',
    });
    assert.equal(res.ok, true);
    assert.match(res.content, /b\.txt:2:TODO second/);
  }

  {
    const res = await runPoolLocalTool('Grep', {
      pattern: 'TODO',
      path: root,
      output_mode: 'count',
    });
    assert.equal(res.ok, true);
    assert.match(res.content, /a\.js:2/);
    assert.match(res.content, /b\.txt:1/);
    assert.match(res.content, /c\.md:1/);
  }

  {
    const res = await runPoolLocalTool('WebFetch', { url: 'http://127.0.0.1/' });
    assert.equal(res.ok, false);
    assert.match(res.content, /Private or local IP URLs are blocked|Localhost URLs are blocked/);
  }

  console.log('local-tool-executor-test: OK');
} finally {
  await fs.rm(root, { recursive: true, force: true });
}
