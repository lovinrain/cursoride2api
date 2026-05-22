// Spoof mitigation: when the model emits the WebSearch counterfeit
// pattern (empty Write to agent-tools/<uuid>.txt), the proxy fetches
// REAL web search results and injects them into the Write content.
// Model reads the file later → gets actual data instead of
// writing-then-confabulating from training data.
//
// Engine selection: Bing's RSS endpoint
// (`bing.com/search?q=...&format=rss`) is the cleanest backend that
// doesn't require an API key, returns structured XML, and doesn't
// CAPTCHA Linux server IPs. DuckDuckGo's HTML endpoint was tried first
// but consistently returns the anomaly modal (HTTP 202, no results)
// from server origins. The RSS feed parses with a trivial regex and
// gives us title / link / description per item, which is exactly the
// shape we need to inject into the Write content.
//
// See AGENT_TOOLS_SPOOF_OBSERVATION.md for the spoof pattern itself
// and the iteration history in DEVLOG.md.

const BING_RSS = 'https://www.bing.com/search';

const UA =
  'Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0';

// Natural-language preambles to strip off before sending to the search
// engine. We accept either an explicit separator (`-`, `:`, `,`, `—`)
// OR a small set of connector phrases (`and tell me`, `for`, `to find`,
// `for me`). Without this, queries like "search online and tell me the
// top 3 LLMs on lmarena" reach Bing as-is and Bing latches onto
// "search online" as the dominant keyword, returning generic search-
// engine homepages.
const STRIP_PREFIXES = [
  /^search\s+(online|web|google|duckduckgo|the\s+web|please)\s*([:,\-—]+|\s+(?:and\s+(?:tell\s+(?:me|us)|find|show\s+(?:me|us))|for|to\s+(?:find|tell|show|see|check|learn))\b)\s*/i,
  /^(can\s+you\s+)?(go\s+)?look\s+(this\s+)?up\s*([:,\-—]+|\s+(?:for|and\s+tell\s+(?:me|us))\b)\s*/i,
  /^check\s+(online|the\s+web)\s*([:,\-—]+|\s+(?:for|and\s+tell\s+(?:me|us))\b)\s*/i,
  /^(please\s+)?(go\s+)?(do\s+a\s+)?(quick\s+)?(web\s+)?search\s+(for|about)\s+/i,
  /^(tell|show)\s+(me|us)\s+/i,
];

const HTML_ENTITY = {
  '&amp;': '&',
  '&lt;': '<',
  '&gt;': '>',
  '&quot;': '"',
  '&#x27;': "'",
  '&#39;': "'",
  '&#x2F;': '/',
  '&nbsp;': ' ',
  '&apos;': "'",
};

function decodeEntities(s) {
  return s
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => {
      try { return String.fromCodePoint(parseInt(h, 16)); } catch { return _; }
    })
    .replace(/&#(\d+);/g, (_, n) => {
      try { return String.fromCodePoint(parseInt(n, 10)); } catch { return _; }
    })
    .replace(/&(amp|lt|gt|quot|nbsp|apos);/g, (m) => HTML_ENTITY[m] || m);
}

function stripTags(s) {
  return s.replace(/<[^>]+>/g, '');
}

// Bing wraps Wikipedia / aggregator redirects only in tracking links
// when accessed via the HTML SERP, not the RSS feed — the RSS `<link>`
// is the real destination. Kept as a no-op pass-through for parity
// with any future engine that does wrap URLs.
function unwrapRedirect(url) {
  return url || '';
}

// Pull the most likely search query out of the conversation's
// user-role messages. Walks backward through messages and within each
// user message walks all text blocks, skipping content that's clearly
// not a user-authored search query (claude-code's `<system-reminder>`
// injections, `<command-name>` tags, tool_result blocks, empty text).
// Returns the first such text it finds, lightly cleaned.
export function extractQuery(messages) {
  if (!Array.isArray(messages) || messages.length === 0) return '';
  const stripSystemBlocks = (text) => {
    // claude-code wraps its injected metadata as <system-reminder>...
    // </system-reminder> blocks that can appear before AND/OR after
    // the user's actual prompt. Drop every such block; what remains
    // is the user-authored text (possibly empty).
    let s = String(text || '');
    s = s.replace(/<system-reminder>[\s\S]*?<\/system-reminder>/g, ' ');
    s = s.replace(/<system>[\s\S]*?<\/system>/g, ' ');
    s = s.replace(/<command-(name|message|args)>[\s\S]*?<\/command-\1>/g, ' ');
    s = s.replace(/<local-command-stdout>[\s\S]*?<\/local-command-stdout>/g, ' ');
    return s.replace(/\s+/g, ' ').trim();
  };
  const isUsable = (t) => stripSystemBlocks(t).length > 0;
  const cleanQuery = (text) => {
    let q = stripSystemBlocks(text);
    for (const re of STRIP_PREFIXES) {
      const cleaned = q.replace(re, '');
      if (cleaned.length < q.length) q = cleaned.trim();
    }
    // Drop leading stop-words left behind by preamble stripping
    // ("the top..." → "top...", "me the latest..." → "latest..."),
    // but only if the result still has at least 2 non-stop tokens —
    // we don't want to evaporate a one-word query like "lmarena".
    q = q.replace(
      /^(the|a|an|me|us|please|right\s+now|currently|today|some|any)\s+/i,
      ''
    ).trim();
    if (q.length > 300) q = q.slice(0, 300);
    return q;
  };

  for (let i = messages.length - 1; i >= 0; i--) {
    const m = messages[i];
    if (!m || m.role !== 'user') continue;
    if (typeof m.content === 'string') {
      if (isUsable(m.content)) return cleanQuery(m.content);
      continue;
    }
    if (!Array.isArray(m.content)) continue;
    // Walk blocks LAST→FIRST inside this message — claude-code emits
    // its system-reminder block AFTER the user's text, so the user
    // text is usually earlier in the array. But within-message order
    // varies, so we check every text block and pick the last usable
    // one (the most recently authored).
    for (let b = m.content.length - 1; b >= 0; b--) {
      const block = m.content[b];
      if (!block || block.type !== 'text') continue;
      if (isUsable(block.text)) return cleanQuery(block.text);
    }
  }
  return '';
}

// Bing RSS scrape. Returns [{url, title, snippet}, ...]. Throws on
// network failure / HTTP non-2xx / parse-zero-results — caller decides
// whether to fall back.
export async function performWebSearch(query, options = {}) {
  const timeoutMs = options.timeoutMs || 12000;
  const maxResults = options.maxResults || 5;
  if (!query || !query.trim()) {
    throw new Error('empty query');
  }

  const url = `${BING_RSS}?q=${encodeURIComponent(query)}&format=rss`;
  let res;
  try {
    res = await fetch(url, {
      method: 'GET',
      headers: {
        'User-Agent': UA,
        Accept: 'application/rss+xml,application/xml,text/xml',
        'Accept-Language': 'en-US,en;q=0.5',
      },
      signal: AbortSignal.timeout(timeoutMs),
    });
  } catch (e) {
    throw new Error(`Bing fetch failed: ${e.message}`);
  }
  if (!res.ok) {
    throw new Error(`Bing returned HTTP ${res.status}`);
  }

  const xml = await res.text();

  // Bing RSS item shape:
  //   <item>
  //     <title>TITLE</title>
  //     <link>URL</link>
  //     <description>DESCRIPTION</description>
  //     <pubDate>...</pubDate>
  //   </item>
  // Title/link/description appear in that order in every item we've
  // observed. We deliberately match each field with its own non-greedy
  // capture so a missing <description> in one item doesn't shift the
  // matches across items.
  const results = [];
  const itemRe = /<item>([\s\S]*?)<\/item>/g;
  let im;
  while ((im = itemRe.exec(xml)) !== null && results.length < maxResults) {
    const block = im[1];
    const tm = /<title>([\s\S]*?)<\/title>/.exec(block);
    const lm = /<link>([\s\S]*?)<\/link>/.exec(block);
    const dm = /<description>([\s\S]*?)<\/description>/.exec(block);
    const title = tm ? decodeEntities(stripTags(tm[1])).replace(/\s+/g, ' ').trim() : '';
    const link = lm ? unwrapRedirect(decodeEntities(lm[1]).trim()) : '';
    const desc = dm ? decodeEntities(stripTags(dm[1])).replace(/\s+/g, ' ').trim() : '';
    if (!title || !link) continue;
    results.push({ url: link, title, snippet: desc });
  }
  if (results.length === 0) {
    throw new Error('parsed zero results from Bing RSS');
  }
  return results;
}

// Bing's RSS sometimes responds to a verbose / stop-word-heavy query
// with generic "what is the web" homepages — none of which contain
// the user's actual answer. Feeding these to the model is worse than
// admitting we couldn't fetch real results: the model treats them as
// authoritative and quotes them, which can mislead. Detect the failure
// mode by checking how many results land on a known generic-search /
// dictionary host. If more than half do, treat the whole batch as a
// miss and let the caller fall back to the proxy_notice.
const GENERIC_HOST_PATTERNS = [
  /^(www\.)?(google|bing|yahoo|duckduckgo|startpage|brave|ecosia|baidu|yandex|ask|aol)\.com$/i,
  /^(www\.)?(google|bing|yahoo|duckduckgo)\.[a-z.]+$/i,
  /^(www\.)?(merriam-webster|dictionary|thefreedictionary|wordreference|collinsdictionary|britannica)\.com$/i,
  /^(www\.)?(en|simple)\.wiktionary\.org$/i,
  /^dictionary\./i,
];
export function resultsLookGeneric(results) {
  if (!results || results.length === 0) return true;
  let generic = 0;
  for (const r of results) {
    try {
      const host = new URL(r.url).hostname.toLowerCase();
      if (GENERIC_HOST_PATTERNS.some((re) => re.test(host))) generic++;
    } catch {
      generic++;
    }
  }
  return generic >= Math.ceil(results.length / 2);
}

// Render a results list into the file content the model will read.
// Tries to look enough like "real search output" that the model
// trusts/uses it, while explicitly marking provenance so the model
// (and any human reviewing) knows where it came from.
export function formatSearchResults(query, results, opts = {}) {
  const lines = [];
  lines.push(`[proxy_websearch — REAL results retrieved by the proxy]`);
  lines.push(`Query: ${query}`);
  lines.push(`Source: Bing RSS (${results.length} result${results.length === 1 ? '' : 's'})`);
  if (opts.fetchedAtIso) lines.push(`Fetched: ${opts.fetchedAtIso}`);
  lines.push('');
  for (let i = 0; i < results.length; i++) {
    const r = results[i];
    lines.push(`[${i + 1}] ${r.title}`);
    lines.push(`    URL: ${r.url}`);
    if (r.snippet) lines.push(`    Snippet: ${r.snippet}`);
    lines.push('');
  }
  lines.push(
    'You may quote these results directly. If you need the full page contents, ' +
      'use Cursor-native WebSearch again with a more specific query, or WebFetch/Fetch for a user-explicit URL. Do NOT use WebFetch/Fetch as a broad-search substitute for Cursor-native WebSearch. Do NOT ' +
      'fabricate facts beyond what these results contain.'
  );
  return lines.join('\n');
}

// Fallback content when the search fails. Same as the previous
// proxy_notice in api-server.mjs — model gets explicit guidance to use
// WebSearch / Bash directly instead of trusting the file.
export function fallbackNoticeBody(reason) {
  return (
    '[proxy_notice — read this carefully]\n\n' +
    'This file was created by a CLIENT-SIDE Write tool call. The proxy attempted to ' +
    'fetch real web search results to inject here, but the fetch failed' +
    (reason ? ` (${reason})` : '') +
    '. The path `agent-tools/<uuid>.txt` is the convention Cursor\'s backend uses ' +
    'to write WebSearch results on its OWN filesystem — writing to it from the ' +
    'client side does NOT perform a web fetch.\n\n' +
    'WHAT TO DO INSTEAD:\n' +
    '  - To search or look up public web information: use Cursor-native WebSearch.\n' +
    '  - For a user-explicit URL fetch, WebFetch/Fetch may be used; Bash/curl is allowed only when the environment permits it.\n' +
    '  - Do NOT use WebFetch/Fetch as a broad-search substitute for Cursor-native WebSearch.\n' +
    '  - If you cannot fulfill the user request without web access, tell the user ' +
    'that and call `bajie_yield`.\n\n' +
    'DO NOT narrate web content as if you had fetched it. DO NOT quote this ' +
    'proxy_notice as if it were search results.'
  );
}
