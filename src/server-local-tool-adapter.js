let localToolsPromise = null;
const fs = require('node:fs');
const path = require('node:path');

const DEFAULT_CWD = process.env.RATLC_LOCAL_TOOL_CWD || process.cwd();

function normalizeServerLocalToolName(name) {
  const raw = String(name || '').trim();
  if (!raw) return '';
  const unprefixed = raw.startsWith('mcp_') && !raw.startsWith('mcp__')
    ? raw.slice(4)
    : raw;
  const lower = unprefixed.toLowerCase();
  if (lower === 'grep') return 'Grep';
  if (lower === 'glob') return 'Glob';
  if (lower === 'edit' || lower === 'strreplace') return 'Edit';
  if (lower === 'webfetch') return 'WebFetch';
  if (lower === 'fetch') return 'Fetch';
  return '';
}

function isServerLocalToolName(name) {
  return normalizeServerLocalToolName(name) !== '';
}

function loadLocalTools() {
  if (!localToolsPromise) {
    localToolsPromise = import('../scaffolding/pool/local-tool-executor.mjs');
  }
  return localToolsPromise;
}

async function runServerLocalTool(name, args = {}, opts = {}) {
  const normalized = normalizeServerLocalToolName(name);
  if (!normalized) {
    return {
      ok: false,
      name: String(name || ''),
      content: `[proxy_error] Unsupported local tool: ${name || '(empty)'}`,
    };
  }
  const mod = await loadLocalTools();
  return mod.runPoolLocalTool(normalized, args, opts);
}

function getServerLocalToolDecision(name, args = {}, opts = {}) {
  const normalized = normalizeServerLocalToolName(name);
  if (!normalized) {
    return {
      canRun: false,
      retryOnClient: false,
      reason: `Unsupported local tool: ${name || '(empty)'}`,
    };
  }
  const cwd = opts.cwd || DEFAULT_CWD;
  if (normalized === 'WebFetch' || normalized === 'Fetch') {
    return { canRun: true, retryOnClient: false, reason: 'proxy-local tool' };
  }
  if (normalized === 'Grep') {
    const searchPath = args.path || deriveGlobSearchRoot(args.glob || '', '', cwd);
    const detail = inspectResolvablePath(searchPath, cwd);
    if (!detail.exists) {
      return {
        canRun: false,
        retryOnClient: true,
        reason: `Grep path is not visible to the proxy process: ${detail.requested}`,
        detail,
      };
    }
    return { canRun: true, retryOnClient: false, detail };
  }
  if (normalized === 'Glob') {
    const pattern = String(args.pattern || args.glob || '*').trim() || '*';
    const root = deriveGlobSearchRoot(pattern, args.path || '', cwd);
    const detail = inspectResolvablePath(root, cwd);
    if (!detail.exists) {
      return {
        canRun: false,
        retryOnClient: true,
        reason: `Glob search root is not visible to the proxy process: ${detail.requested}`,
        detail,
      };
    }
    if (!detail.isDirectory) {
      return {
        canRun: false,
        retryOnClient: true,
        reason: `Glob search root is not a directory in the proxy process: ${detail.requested}`,
        detail,
      };
    }
    return { canRun: true, retryOnClient: false, detail };
  }
  if (normalized === 'Edit') {
    const requestedPath = String(extractEditPath(args) || '').trim();
    if (!requestedPath) return { canRun: true, retryOnClient: false, reason: 'invalid Edit request; let tool return schema error' };
    const detail = inspectResolvablePath(requestedPath, cwd);
    if (!detail.exists) {
      return {
        canRun: false,
        retryOnClient: true,
        reason: `Edit file is not visible to the proxy process: ${detail.requested}`,
        detail,
      };
    }
    if (!detail.isFile) {
      return {
        canRun: false,
        retryOnClient: true,
        reason: `Edit path is not a file in the proxy process: ${detail.requested}`,
        detail,
      };
    }
    return { canRun: true, retryOnClient: false, detail };
  }
  return { canRun: false, retryOnClient: false, reason: `Unsupported local tool: ${normalized}` };
}

function normalizeSlash(value) {
  return String(value || '').replace(/\\/g, '/');
}

function windowsDrivePathParts(value) {
  const raw = normalizeSlash(String(value || '').trim());
  const m = /^([A-Za-z]):\/?(.*)$/.exec(raw);
  if (!m) return null;
  return {
    drive: m[1].toLowerCase(),
    driveUpper: m[1].toUpperCase(),
    rest: String(m[2] || '').replace(/^\/+/, ''),
  };
}

function windowsDriveRootTemplates() {
  return String(process.env.RATLC_WINDOWS_DRIVE_ROOTS || '/{drive},/mnt/{drive},/{DRIVE},/mnt/{DRIVE}')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
}

function resolveWindowsDrivePathDetailed(value) {
  const parts = windowsDrivePathParts(value);
  if (!parts) return null;
  const candidateRoots = windowsDriveRootTemplates().map((template) => template
    .replace(/\{drive\}/g, parts.drive)
    .replace(/\{DRIVE\}/g, parts.driveUpper));
  const candidates = candidateRoots.map((root) => path.resolve(root, parts.rest));
  for (let i = 0; i < candidates.length; i++) {
    if (fs.existsSync(candidates[i])) {
      return {
        kind: 'windows-drive',
        requested: normalizeSlash(String(value || '').trim()),
        resolved: candidates[i],
        root: path.resolve(candidateRoots[i]),
        candidateRoots: candidateRoots.map((r) => path.resolve(r)),
        candidates,
      };
    }
  }
  for (let i = 0; i < candidates.length; i++) {
    const root = path.resolve(candidateRoots[i]);
    if (fs.existsSync(root)) {
      return {
        kind: 'windows-drive',
        requested: normalizeSlash(String(value || '').trim()),
        resolved: candidates[i],
        root,
        candidateRoots: candidateRoots.map((r) => path.resolve(r)),
        candidates,
      };
    }
  }
  const root = candidateRoots[0] || '';
  return {
    kind: 'windows-drive',
    requested: normalizeSlash(String(value || '').trim()),
    resolved: candidates[0] || '',
    root: root ? path.resolve(root) : '',
    candidateRoots: candidateRoots.map((r) => path.resolve(r)),
    candidates,
  };
}

function inspectResolvablePath(value, cwd = DEFAULT_CWD) {
  const raw = String(value || '').trim();
  const requested = raw || '.';
  let detail;
  if (!raw) {
    detail = { kind: 'cwd', requested, resolved: path.resolve(cwd) };
  } else {
    const windowsPath = resolveWindowsDrivePathDetailed(raw);
    if (windowsPath) detail = windowsPath;
    else detail = { kind: path.isAbsolute(raw) ? 'absolute' : 'relative', requested, resolved: path.resolve(cwd, raw) };
  }
  let st = null;
  try {
    st = fs.statSync(detail.resolved);
  } catch {
    st = null;
  }
  return {
    ...detail,
    exists: !!st,
    isFile: !!st && st.isFile(),
    isDirectory: !!st && st.isDirectory(),
  };
}

function firstGlobMagicIndex(value) {
  const s = String(value || '');
  const matches = [s.indexOf('*'), s.indexOf('?'), s.indexOf('[')].filter((n) => n >= 0);
  return matches.length ? Math.min(...matches) : -1;
}

function deriveGlobSearchRoot(pattern, pathArg, cwd = DEFAULT_CWD) {
  if (pathArg) return inspectResolvablePath(pathArg, cwd).resolved;
  const raw = String(pattern || '').trim();
  if (!raw) return path.resolve(cwd);
  const magicAt = firstGlobMagicIndex(raw);
  if (windowsDrivePathParts(raw)) {
    if (magicAt < 0) return path.dirname(inspectResolvablePath(raw, cwd).resolved);
    const staticPart = normalizeSlash(raw.slice(0, magicAt));
    const dir = staticPart.endsWith('/')
      ? staticPart
      : path.posix.dirname(staticPart);
    return inspectResolvablePath(dir, cwd).resolved;
  }
  if (path.isAbsolute(raw)) {
    if (magicAt < 0) return path.dirname(raw);
    const staticPart = raw.slice(0, magicAt);
    const root = staticPart.endsWith(path.sep) || staticPart.endsWith('/')
      ? staticPart
      : path.dirname(staticPart);
    return root || path.parse(raw).root;
  }
  if (magicAt < 0) {
    const dir = path.dirname(raw);
    return dir && dir !== '.' ? path.resolve(cwd, dir) : path.resolve(cwd);
  }
  const staticPart = raw.slice(0, magicAt);
  const dir = staticPart.endsWith(path.sep) || staticPart.endsWith('/')
    ? staticPart
    : path.dirname(staticPart);
  return dir && dir !== '.' ? path.resolve(cwd, dir) : path.resolve(cwd);
}

function extractEditPath(args = {}) {
  return args.file_path || args.filePath || args.path || args.filename || '';
}

module.exports = {
  getServerLocalToolDecision,
  isServerLocalToolName,
  normalizeServerLocalToolName,
  runServerLocalTool,
};
