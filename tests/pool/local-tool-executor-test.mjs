#!/usr/bin/env node

import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {
  getPoolLocalToolDecision,
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
  assert.equal(normalizePoolLocalToolName('StrReplace'), 'Edit');
  assert.equal(normalizePoolLocalToolName('mcp_Edit'), 'Edit');
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

  {
    const editFile = path.join(root, 'src', 'edit.txt');
    await fs.writeFile(editFile, 'alpha\nbeta\n');
    const res = await runPoolLocalTool('StrReplace', {
      file_path: editFile,
      old_string: 'beta',
      new_string: 'gamma',
    });
    assert.equal(res.ok, true);
    assert.match(res.content, /Replacements: 1/);
    assert.equal(await fs.readFile(editFile, 'utf8'), 'alpha\ngamma\n');
  }

  {
    const oldRoots = process.env.RATLC_WINDOWS_DRIVE_ROOTS;
    const missingDriveRoot = path.join(root, 'missing-drive-d');
    process.env.RATLC_WINDOWS_DRIVE_ROOTS = `${missingDriveRoot}`;
    try {
      const decision = getPoolLocalToolDecision('Grep', {
        pattern: 'Testing',
        path: 'D:/XM/Nx/test_tools.txt',
      });
      assert.equal(decision.canRun, false);
      assert.equal(decision.retryOnClient, true);
      assert.match(decision.reason, /not visible to the proxy process/);

      const grep = await runPoolLocalTool('Grep', {
        pattern: 'Testing',
        path: 'D:/XM/Nx/test_tools.txt',
        output_mode: 'content',
      });
      assert.equal(grep.ok, false);
      assert.equal(grep.retryOnClient, true);
      assert.match(grep.content, /should be forwarded to the outer client/);

      const editDecision = getPoolLocalToolDecision('Edit', {
        file_path: 'D:/XM/Nx/test_tools.txt',
        old_string: 'Testing',
        new_string: 'Checked',
      });
      assert.equal(editDecision.canRun, false);
      assert.equal(editDecision.retryOnClient, true);
    } finally {
      if (oldRoots == null) delete process.env.RATLC_WINDOWS_DRIVE_ROOTS;
      else process.env.RATLC_WINDOWS_DRIVE_ROOTS = oldRoots;
    }
  }

  {
    const driveRoot = path.join(root, 'drive-d');
    const mappedFile = path.join(driveRoot, 'XM', 'Nx', 'test_tools.txt');
    await fs.mkdir(path.dirname(mappedFile), { recursive: true });
    await fs.writeFile(mappedFile, 'Testing windows path\n');
    const oldRoots = process.env.RATLC_WINDOWS_DRIVE_ROOTS;
    process.env.RATLC_WINDOWS_DRIVE_ROOTS = `${driveRoot}`;
    try {
      const grep = await runPoolLocalTool('Grep', {
        pattern: 'Testing',
        path: 'D:/XM/Nx/test_tools.txt',
        output_mode: 'content',
      });
      assert.equal(grep.ok, true);
      assert.match(grep.content, /test_tools\.txt:1:Testing windows path/);

      const edit = await runPoolLocalTool('Edit', {
        file_path: 'D:/XM/Nx/test_tools.txt',
        old_string: 'windows',
        new_string: 'mapped',
      });
      assert.equal(edit.ok, true);
      assert.equal(await fs.readFile(mappedFile, 'utf8'), 'Testing mapped path\n');
    } finally {
      if (oldRoots == null) delete process.env.RATLC_WINDOWS_DRIVE_ROOTS;
      else process.env.RATLC_WINDOWS_DRIVE_ROOTS = oldRoots;
    }
  }

  console.log('local-tool-executor-test: OK');
} finally {
  await fs.rm(root, { recursive: true, force: true });
}
