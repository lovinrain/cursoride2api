#!/usr/bin/env node

const assert = require('node:assert/strict');
const { create } = require('@bufbuild/protobuf');
const {
  loadProto,
  buildNativeReadResult,
  buildNativeWriteResult,
  buildNativeDeleteResult,
  buildNativeGrepResult,
  buildListMcpResourcesResult,
} = require('../../src/cursor-agent');

(async () => {
  const { agent } = await loadProto();

  const read = buildNativeReadResult(create, agent, '/tmp/example.txt', 'hello\nworld');
  assert.equal(read.result.case, 'success');
  assert.equal(read.result.value.path, '/tmp/example.txt');
  assert.equal(read.result.value.output.case, 'content');
  assert.equal(read.result.value.output.value, 'hello\nworld');
  assert.equal(read.result.value.totalLines, 2);
  assert.equal(read.result.value.fileSize, 11n);

  const write = buildNativeWriteResult(
    create,
    agent,
    { path: '/tmp/write.txt', content: 'alpha\nbeta\n' },
    'File created successfully at: /tmp/write.txt'
  );
  assert.equal(write.result.case, 'success');
  assert.equal(write.result.value.path, '/tmp/write.txt');
  assert.equal(write.result.value.linesCreated, 2);
  assert.equal(write.result.value.fileSize, 11);
  assert.equal(write.result.value.fileContentAfterWrite, 'alpha\nbeta\n');

  const del = buildNativeDeleteResult(
    create,
    agent,
    { path: '/tmp/delete.txt' },
    '{"__cursoride2apiDeleteResult":1,"path":"/tmp/delete.txt","case":"success","deletedFile":"/tmp/delete.txt","fileSize":5,"prevContent":"hello"}\n'
  );
  assert.equal(del.result.case, 'success');
  assert.equal(del.result.value.path, '/tmp/delete.txt');
  assert.equal(del.result.value.deletedFile, '/tmp/delete.txt');
  assert.equal(del.result.value.fileSize, 5);
  assert.equal(del.result.value.prevContent, 'hello');

  const delMissing = buildNativeDeleteResult(
    create,
    agent,
    { path: '/tmp/missing.txt' },
    '{"__cursoride2apiDeleteResult":1,"path":"/tmp/missing.txt","case":"fileNotFound"}\n'
  );
  assert.equal(delMissing.result.case, 'fileNotFound');
  assert.equal(delMissing.result.value.path, '/tmp/missing.txt');

  const grep = buildNativeGrepResult(
    create,
    agent,
    { pattern: 'TODO', path: '/tmp', outputMode: 'files_with_matches' },
    '/tmp/a.txt\n/tmp/b.txt\n'
  );
  assert.equal(grep.result.case, 'success');
  assert.equal(grep.result.value.pattern, 'TODO');
  assert.equal(grep.result.value.path, '/tmp');
  assert.equal(grep.result.value.outputMode, 'files_with_matches');
  assert.ok(grep.result.value.workspaceResults['/tmp']);
  assert.equal(grep.result.value.workspaceResults['/tmp'].result.case, 'files');
  assert.deepEqual(
    grep.result.value.workspaceResults['/tmp'].result.value.files,
    ['/tmp/a.txt', '/tmp/b.txt']
  );
  assert.equal(grep.result.value.workspaceResults['/tmp'].result.value.totalFiles, 2);

  const grepDefault = buildNativeGrepResult(
    create,
    agent,
    { pattern: 'TODO', path: '/tmp' },
    '/tmp/default.txt\n'
  );
  assert.equal(grepDefault.result.value.outputMode, 'files_with_matches');
  assert.equal(grepDefault.result.value.workspaceResults['/tmp'].result.case, 'files');

  const grepContent = buildNativeGrepResult(
    create,
    agent,
    { pattern: 'TODO', path: '/tmp', outputMode: 'content' },
    '/tmp/a.txt:3:TODO item\n'
  );
  assert.equal(grepContent.result.value.outputMode, 'content');
  assert.equal(grepContent.result.value.workspaceResults['/tmp'].result.case, 'content');
  assert.equal(
    grepContent.result.value.workspaceResults['/tmp'].result.value.matches[0].matches[0].content,
    'TODO item'
  );

  const listMcp = buildListMcpResourcesResult(create, agent, []);
  assert.equal(listMcp.result.case, 'success');
  assert.deepEqual(listMcp.result.value.resources, []);

  console.log('native-tool-result-test: OK');
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
