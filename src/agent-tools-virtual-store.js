'use strict';

const MAX_ARTIFACTS = Math.max(16, parseInt(process.env.RATLC_AGENT_TOOLS_MAX_ARTIFACTS || '512', 10));
const MAX_BYTES = Math.max(1024, parseInt(process.env.RATLC_AGENT_TOOLS_MAX_BYTES || '5242880', 10));
const artifacts = new Map();

function normalizeAgentToolPath(value) {
  const p = String(value || '').replace(/\\/g, '/').replace(/^\.\/+/, '');
  return p.replace(/\/+/g, '/');
}

function isAgentToolsArtifactPath(value) {
  const p = normalizeAgentToolPath(value);
  return /^agent-tools\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.txt$/i.test(p);
}

function getPathFromArgs(args = {}) {
  const a = args && typeof args === 'object' ? args : {};
  return normalizeAgentToolPath(a.file_path || a.path || a.filename || '');
}

function getContentFromArgs(args = {}) {
  const a = args && typeof args === 'object' ? args : {};
  return String(a.content ?? a.file_text ?? a.text ?? a.body ?? a.data ?? '');
}

function trimToMaxBytes(text) {
  const raw = String(text || '');
  const bytes = Buffer.byteLength(raw);
  if (bytes <= MAX_BYTES) return { text: raw, truncated: false, bytes };
  let out = raw;
  while (Buffer.byteLength(out) > MAX_BYTES && out.length > 0) {
    out = out.slice(0, Math.max(0, Math.floor(out.length * 0.9)));
  }
  return {
    text: out + `\n\n[proxy_notice] agent-tools artifact truncated to ${MAX_BYTES} bytes from ${bytes} bytes.`,
    truncated: true,
    bytes,
  };
}

function putAgentToolsArtifact(path, content, meta = {}) {
  const normalizedPath = normalizeAgentToolPath(path);
  if (!isAgentToolsArtifactPath(normalizedPath)) return null;
  const trimmed = trimToMaxBytes(content);
  if (artifacts.size >= MAX_ARTIFACTS && !artifacts.has(normalizedPath)) {
    const firstKey = artifacts.keys().next().value;
    if (firstKey !== undefined) artifacts.delete(firstKey);
  }
  const entry = {
    path: normalizedPath,
    content: trimmed.text,
    originalBytes: trimmed.bytes,
    bytes: Buffer.byteLength(trimmed.text),
    truncated: trimmed.truncated,
    createdAt: Date.now(),
    updatedAt: Date.now(),
    source: meta.source || 'cursor-agent',
  };
  artifacts.set(normalizedPath, entry);
  return entry;
}

function contentForAgentToolsWrite(content) {
  const raw = String(content ?? '');
  if (raw.trim() && raw.trim() !== '(No content)') return raw;
  return (
    '[proxy_notice - read this carefully]\n\n' +
    'This agent-tools artifact was created by a Write call with no useful ' +
    'content. The proxy did not create a local workspace file. If you intended ' +
    'to look up public web information, use Cursor-native WebSearch. Do not ' +
    'fabricate web content from this placeholder.'
  );
}

function getAgentToolsArtifact(path) {
  const normalizedPath = normalizeAgentToolPath(path);
  const entry = artifacts.get(normalizedPath);
  if (!entry) return null;
  entry.lastAccessAt = Date.now();
  return entry;
}

function makeVirtualWriteResultText(entry) {
  const e = entry || {};
  return `Virtual agent-tools artifact stored at ${e.path || 'agent-tools/(unknown).txt'} (${e.bytes || 0} bytes).`;
}

function makeVirtualReadMissingText(path) {
  return `[proxy_error] agent-tools artifact not found in the proxy virtual store: ${normalizeAgentToolPath(path)}`;
}

module.exports = {
  getAgentToolsArtifact,
  getContentFromArgs,
  getPathFromArgs,
  contentForAgentToolsWrite,
  isAgentToolsArtifactPath,
  makeVirtualReadMissingText,
  makeVirtualWriteResultText,
  normalizeAgentToolPath,
  putAgentToolsArtifact,
};
