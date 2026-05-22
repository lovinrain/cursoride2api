import fs from 'node:fs/promises';
import path from 'node:path';
import dns from 'node:dns/promises';
import net from 'node:net';

const DEFAULT_CWD = process.env.RATLC_LOCAL_TOOL_CWD || process.cwd();
const MAX_RESULTS = Math.max(1, parseInt(process.env.RATLC_LOCAL_TOOL_MAX_RESULTS || '2000', 10));
const MAX_FILES_SCANNED = Math.max(1, parseInt(process.env.RATLC_LOCAL_TOOL_MAX_FILES || '20000', 10));
const MAX_FILE_BYTES = Math.max(1, parseInt(process.env.RATLC_LOCAL_GREP_MAX_FILE_BYTES || '1048576', 10));
const FETCH_TIMEOUT_MS = Math.max(1, parseInt(process.env.RATLC_LOCAL_WEBFETCH_TIMEOUT_MS || '15000', 10));
const FETCH_MAX_BYTES = Math.max(1, parseInt(process.env.RATLC_LOCAL_WEBFETCH_MAX_BYTES || '1500000', 10));
const SKIP_DIRS = new Set(
  String(process.env.RATLC_LOCAL_TOOL_SKIP_DIRS || '.git,node_modules,.hg,.svn,.cache')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
);

export function normalizePoolLocalToolName(name) {
  const raw = String(name || '').trim();
  if (!raw) return '';
  const unprefixed = raw.startsWith('mcp_') && !raw.startsWith('mcp__')
    ? raw.slice(4)
    : raw;
  const lower = unprefixed.toLowerCase();
  if (lower === 'grep') return 'Grep';
  if (lower === 'glob') return 'Glob';
  if (lower === 'webfetch') return 'WebFetch';
  if (lower === 'fetch') return 'Fetch';
  return '';
}

export function isPoolLocalToolName(name) {
  return normalizePoolLocalToolName(name) !== '';
}

export async function runPoolLocalTool(name, args = {}, opts = {}) {
  const normalized = normalizePoolLocalToolName(name);
  try {
    if (normalized === 'Grep') {
      return { ok: true, name: normalized, content: await runLocalGrep(args, opts) };
    }
    if (normalized === 'Glob') {
      return { ok: true, name: normalized, content: await runLocalGlob(args, opts) };
    }
    if (normalized === 'WebFetch' || normalized === 'Fetch') {
      return { ok: true, name: normalized, content: await runLocalWebFetch(args) };
    }
    return { ok: false, name: normalized || String(name || ''), content: `[proxy_error] Unsupported local tool: ${name || '(empty)'}` };
  } catch (e) {
    const message = e && e.message ? e.message : String(e);
    return { ok: false, name: normalized || String(name || ''), content: `[proxy_error] ${message}` };
  }
}

function resolveFromCwd(value, cwd = DEFAULT_CWD) {
  const raw = String(value || '').trim();
  if (!raw) return path.resolve(cwd);
  return path.resolve(cwd, raw);
}

function normalizeSlash(value) {
  return String(value || '').replace(/\\/g, '/');
}

function hasGlobMagic(value) {
  return /[*?\[]/.test(String(value || ''));
}

function firstGlobMagicIndex(value) {
  const s = String(value || '');
  const matches = [s.indexOf('*'), s.indexOf('?'), s.indexOf('[')].filter((n) => n >= 0);
  return matches.length ? Math.min(...matches) : -1;
}

function deriveGlobSearchRoot(pattern, pathArg, cwd = DEFAULT_CWD) {
  if (pathArg) return resolveFromCwd(pathArg, cwd);
  const raw = String(pattern || '').trim();
  if (!raw) return path.resolve(cwd);
  const magicAt = firstGlobMagicIndex(raw);
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

function escapeRegex(value) {
  return String(value).replace(/[|\\{}()[\]^$+?.]/g, '\\$&');
}

function globToRegExp(glob) {
  const s = normalizeSlash(glob || '*').replace(/^\.\//, '');
  let re = '^';
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === '*') {
      if (s[i + 1] === '*') {
        while (s[i + 1] === '*') i++;
        if (s[i + 1] === '/') {
          re += '(?:.*\\/)?';
          i++;
        } else {
          re += '.*';
        }
      } else {
        re += '[^/]*';
      }
    } else if (ch === '?') {
      re += '[^/]';
    } else if (ch === '[') {
      const end = s.indexOf(']', i + 1);
      if (end > i + 1) {
        re += s.slice(i, end + 1);
        i = end;
      } else {
        re += '\\[';
      }
    } else {
      re += escapeRegex(ch);
    }
  }
  re += '$';
  return new RegExp(re);
}

function makeGlobMatcher(pattern, root, cwd = DEFAULT_CWD) {
  const raw = String(pattern || '*').trim() || '*';
  const absolutePattern = path.isAbsolute(raw);
  const rootAbs = path.resolve(cwd, root || cwd);
  const regex = globToRegExp(absolutePattern ? normalizeSlash(path.resolve(cwd, raw)) : raw);
  return (absPath) => {
    const candidate = absolutePattern
      ? normalizeSlash(absPath)
      : normalizeSlash(path.relative(rootAbs, absPath));
    return regex.test(candidate);
  };
}

async function* walkFiles(root, opts = {}) {
  const maxFiles = opts.maxFiles || MAX_FILES_SCANNED;
  const state = opts.state || { files: 0 };
  let st;
  try {
    st = await fs.lstat(root);
  } catch {
    return;
  }
  if (st.isSymbolicLink()) return;
  if (st.isFile()) {
    if (state.files++ < maxFiles) yield { absPath: root, stat: st };
    return;
  }
  if (!st.isDirectory()) return;
  let entries;
  try {
    entries = await fs.readdir(root, { withFileTypes: true });
  } catch {
    return;
  }
  entries.sort((a, b) => a.name.localeCompare(b.name));
  for (const ent of entries) {
    if (state.files >= maxFiles) return;
    if (ent.isDirectory() && SKIP_DIRS.has(ent.name)) continue;
    if (ent.isSymbolicLink()) continue;
    yield* walkFiles(path.join(root, ent.name), { ...opts, state });
  }
}

function makeSearchRegex(pattern) {
  const raw = String(pattern || '');
  if (!raw) return null;
  try {
    return new RegExp(raw);
  } catch {
    return new RegExp(escapeRegex(raw));
  }
}

function normalizeGrepOutputMode(mode) {
  const raw = String(mode || '').trim();
  if (raw === 'content') return 'content';
  if (raw === 'count') return 'count';
  return 'files';
}

async function runLocalGrep(args = {}, opts = {}) {
  const cwd = opts.cwd || DEFAULT_CWD;
  const pattern = String(args.pattern ?? args.query ?? '');
  const outputMode = normalizeGrepOutputMode(args.output_mode || args.outputMode);
  const searchPath = args.path || deriveGlobSearchRoot(args.glob || '', '', cwd);
  const root = resolveFromCwd(searchPath, cwd);
  const matcher = args.glob ? makeGlobMatcher(args.glob, root, cwd) : () => true;
  const regex = makeSearchRegex(pattern);
  const out = [];
  const seenFiles = new Set();

  for await (const { absPath, stat } of walkFiles(root)) {
    if (out.length >= MAX_RESULTS) break;
    if (!matcher(absPath)) continue;
    if (!regex) {
      if (outputMode !== 'content') out.push(absPath);
      continue;
    }
    if (stat.size > MAX_FILE_BYTES) continue;
    let text;
    try {
      text = await fs.readFile(absPath, 'utf8');
    } catch {
      continue;
    }
    const lines = text.split(/\r?\n/);
    let count = 0;
    for (let i = 0; i < lines.length; i++) {
      regex.lastIndex = 0;
      if (!regex.test(lines[i])) continue;
      count++;
      if (outputMode === 'content') {
        out.push(`${absPath}:${i + 1}:${lines[i]}`);
        if (out.length >= MAX_RESULTS) break;
      }
    }
    if (count > 0 && outputMode === 'count') out.push(`${absPath}:${count}`);
    if (count > 0 && outputMode === 'files' && !seenFiles.has(absPath)) {
      seenFiles.add(absPath);
      out.push(absPath);
    }
  }
  return out.slice(0, MAX_RESULTS).join('\n') + (out.length ? '\n' : '');
}

async function runLocalGlob(args = {}, opts = {}) {
  const cwd = opts.cwd || DEFAULT_CWD;
  const pattern = String(args.pattern || args.glob || '*').trim() || '*';
  const root = deriveGlobSearchRoot(pattern, args.path || '', cwd);
  const matcher = makeGlobMatcher(pattern, root, cwd);
  const matches = [];
  for await (const item of walkFiles(root)) {
    if (!matcher(item.absPath)) continue;
    matches.push(item);
    if (matches.length >= MAX_RESULTS) break;
  }
  matches.sort((a, b) => (b.stat.mtimeMs - a.stat.mtimeMs) || a.absPath.localeCompare(b.absPath));
  return matches.map((m) => m.absPath).join('\n') + (matches.length ? '\n' : '');
}

function isPrivateIp(ip) {
  if (!ip) return true;
  if (net.isIPv4(ip)) {
    const parts = ip.split('.').map(Number);
    const [a, b] = parts;
    return (
      a === 0 ||
      a === 10 ||
      a === 127 ||
      (a === 169 && b === 254) ||
      (a === 172 && b >= 16 && b <= 31) ||
      (a === 192 && b === 168) ||
      a >= 224
    );
  }
  if (net.isIPv6(ip)) {
    const normalized = ip.toLowerCase();
    return (
      normalized === '::1' ||
      normalized === '::' ||
      normalized.startsWith('fc') ||
      normalized.startsWith('fd') ||
      normalized.startsWith('fe8') ||
      normalized.startsWith('fe9') ||
      normalized.startsWith('fea') ||
      normalized.startsWith('feb') ||
      normalized.startsWith('ff')
    );
  }
  return true;
}

async function assertPublicFetchUrl(rawUrl) {
  let parsed;
  try {
    parsed = new URL(rawUrl);
  } catch {
    throw new Error('Invalid URL');
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new Error('Only http and https URLs are supported');
  }
  if (!parsed.hostname) throw new Error('URL hostname is required');
  if (parsed.hostname === 'localhost' || parsed.hostname.endsWith('.localhost')) {
    throw new Error('Localhost URLs are blocked');
  }
  if (net.isIP(parsed.hostname)) {
    if (isPrivateIp(parsed.hostname)) throw new Error('Private or local IP URLs are blocked');
    return parsed;
  }
  let records;
  try {
    records = await dns.lookup(parsed.hostname, { all: true, verbatim: true });
  } catch (e) {
    throw new Error(`DNS lookup failed: ${e.message}`);
  }
  if (!records || records.length === 0) throw new Error('DNS lookup returned no addresses');
  for (const rec of records) {
    if (isPrivateIp(rec.address)) throw new Error('DNS resolves to a private or local address');
  }
  return parsed;
}

function decodeHtmlEntities(text) {
  return String(text || '')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#x([0-9a-f]+);/gi, (_, hex) => {
      const n = parseInt(hex, 16);
      return Number.isFinite(n) ? String.fromCodePoint(n) : ' ';
    })
    .replace(/&#(\d+);/g, (_, dec) => {
      const n = parseInt(dec, 10);
      return Number.isFinite(n) ? String.fromCodePoint(n) : ' ';
    });
}

function htmlToReadableText(html) {
  const raw = String(html || '');
  const title = raw.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  const metadata = title && title[1] ? [`Title: ${decodeHtmlEntities(title[1]).replace(/\s+/g, ' ').trim()}`] : [];
  const body = decodeHtmlEntities(raw)
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, ' ')
    .replace(/<noscript\b[^>]*>[\s\S]*?<\/noscript>/gi, ' ')
    .replace(/<!--[\s\S]*?-->/g, ' ')
    .replace(/<\/(p|div|section|article|header|footer|main|aside|nav|li|tr|h[1-6]|blockquote)>/gi, '\n')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/[ \t\f\v]+/g, ' ')
    .replace(/\n\s+/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
  return [...metadata, body].filter(Boolean).join('\n\n').trim() ||
    '[No readable text was found in the HTML. The page may require JavaScript rendering.]';
}

function isRedirectStatus(status) {
  return status === 301 || status === 302 || status === 303 || status === 307 || status === 308;
}

function extractFetchToolUrl(args) {
  if (!args || typeof args !== 'object') return '';
  return args.url || args.uri || args.href || args.URL || '';
}

async function runLocalWebFetch(args = {}) {
  if (typeof fetch !== 'function') throw new Error('Global fetch is not available in this Node runtime');
  const url = extractFetchToolUrl(args);
  if (!url) throw new Error('WebFetch requires a url');
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(new Error('Fetch timed out')), FETCH_TIMEOUT_MS);
  try {
    let currentUrl = url;
    let response = null;
    let parsed = null;
    for (let redirects = 0; redirects <= 5; redirects++) {
      parsed = await assertPublicFetchUrl(currentUrl);
      response = await fetch(parsed.toString(), {
        signal: ac.signal,
        redirect: 'manual',
        headers: {
          'user-agent': 'Mozilla/5.0 (compatible; cursoride2api-ratlc; +https://cursor.sh)',
          accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,text/plain;q=0.8,application/json;q=0.8,*/*;q=0.5',
          'accept-language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
        },
      });
      if (!isRedirectStatus(response.status)) break;
      const location = response.headers.get('location');
      if (!location) break;
      currentUrl = new URL(location, parsed).toString();
      if (redirects === 5) throw new Error('Too many redirects');
    }
    const contentType = response.headers.get('content-type') || 'application/octet-stream';
    const reader = response.body && response.body.getReader ? response.body.getReader() : null;
    const chunks = [];
    let total = 0;
    let truncated = false;
    if (reader) {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        if (!value) continue;
        const remaining = FETCH_MAX_BYTES - total;
        if (remaining <= 0) {
          truncated = true;
          try { await reader.cancel(); } catch { /* ignore */ }
          break;
        }
        const chunk = value.length > remaining ? value.slice(0, remaining) : value;
        chunks.push(Buffer.from(chunk));
        total += chunk.length;
        if (value.length > remaining) {
          truncated = true;
          try { await reader.cancel(); } catch { /* ignore */ }
          break;
        }
      }
    } else {
      const ab = await response.arrayBuffer();
      const buf = Buffer.from(ab);
      chunks.push(buf.subarray(0, FETCH_MAX_BYTES));
      truncated = buf.length > FETCH_MAX_BYTES;
    }
    let content = Buffer.concat(chunks).toString('utf8');
    if (/html/i.test(contentType)) content = htmlToReadableText(content);
    if (truncated) content += `\n\n[Content truncated at ${FETCH_MAX_BYTES} bytes by cursoride2api]`;
    return [
      `URL: ${response.url || parsed.toString()}`,
      `Status: ${response.status}`,
      `Content-Type: ${contentType}`,
      '',
      content || '',
    ].join('\n');
  } finally {
    clearTimeout(timer);
  }
}
