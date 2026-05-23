# Web Research Pipeline — Three Gaps That Compound

## TL;DR

Three independent fixes (PR #1, PR #2, and our spoof-mitigation) each
addressed a piece of the model's "research the web" capability. A
specific failure pattern falls through the gaps in all three and
produces a **plausible-looking but hallucinated** result that
claude-code renders as a successful `Write` operation. The user sees
no error.

This doc:
1. Records the trace of how the failure happens (so it's reproducible
   and debuggable).
2. Identifies the three gaps precisely with code locations.
3. Proposes a concrete fix for each, ranked by blast radius and value.

**Status as of 2026-05-23**: documented, not fixed. The simplest of the
three fixes (broaden spoof-mitigation) is recommended for the next
implementation pass and closes the user-visible symptom regardless of
whether the other two gaps are ever addressed.

## The symptom users see

In a claude-code session, the model is asked something that requires
web research. The TUI shows roughly:

```
⏺ [Cursor WebSearch] ZenomTrader Premium whop.com review 2026
  ⎿  API Error: Response error: aborted

(model retries with more queries)

⏺ Write     agent-tools/da9e2364-...txt        (200 bytes)
⏺ Write     agent-tools/fdeb9f20-...txt        (350 bytes)
⏺ Write     agent-tools/f9e98369-...txt        (350 bytes)
⏺ Write     agent-tools/931f9f0d-...txt        (350 bytes)
```

The `[Cursor WebSearch]` line with `aborted` looks like a transient
error. The subsequent `Write` operations appear successful. The user
naturally assumes the model recovered.

In reality, **every byte written to those `agent-tools/*.txt` files is
fabricated** — the model is hallucinating content that looks like a
fetched webpage. Sampled content from one real session:

| File hash prefix | First chars of fabricated content |
|------------------|-----------------------------------|
| `da9e2364`       | "Whop Review: Legit Way To Make Money? [2026 Update]..." |
| `fdeb9f20`       | "EQUITIES FIXED INCOME MULTI ASSET STRUCTURED MONEY MARKET — Goldman Sachs Romania Equit..." |
| `f9e98369`       | "EQUITIES FIXED INCOME MULTI ASSET STRUCTURED MONEY MARKET — Goldman Sachs Romania Equit..." |
| `931f9f0d`       | "EQUITIES FIXED INCOME MULTI ASSET STRUCTURED MONEY MARKET — Goldman Sachs Romania Equit..." |

Three identical Goldman Sachs Romania factsheet pages, in response to a
ZenomTrader query. That is a classic LLM "fabricate something plausible
when blocked" pattern — the model drew "what a financial-services
webpage might look like" from training data and wrote it to disk as
if it were a real fetch.

## The three layers of "web research" support

| Layer | What it does | Code |
|-------|-------------|------|
| **Cursor native WebSearch** | The inner Cursor model can ask its backend to run a web search; result text comes back to the model in-band. PR #1 added the surfacing so claude-code sees a `server_tool_use` content block when this happens. | `src/cursor-agent.js handleInteractionQuery` for `webSearchToolCall`; `scaffolding/pool/api-server.mjs` for the SSE emit |
| **Cursor native WebFetch** | Same backend mechanism, but fetches a specific URL. Used by the inner model as a research follow-up — "I found these URLs in the search results, now fetch them." | `src/cursor-agent.js handleInteractionQuery` for `webFetchRequestQuery` (NOT IMPLEMENTED) |
| **Client-side WebFetch tool** | When claude-code registers `WebFetch` as one of its tools, the inner model can invoke it via MCP and the proxy runs the fetch locally (PR #2's `local-tool-executor.mjs`). | `scaffolding/pool/local-tool-executor.mjs` |

The naming overlap between "Cursor native WebFetch" and "client-side
WebFetch tool" is genuinely confusing. They are two different code
paths that look identical from the model's POV: both let the model
fetch a URL. But they enter our proxy through different mechanisms,
and we only implement one.

| Path | Mechanism | Status |
|------|-----------|--------|
| Cursor native WebFetch | Cursor sends `webFetchRequestQuery` interaction frame; proxy must respond | **NOT HANDLED** (the gap) |
| Client-side WebFetch tool | Inner model emits `mcp_call` for the `WebFetch` tool name | Handled by `local-tool-executor.mjs` (PR #2) |

Plus the safety net we built:

| Layer | What it does | Code |
|-------|-------------|------|
| **Spoof-mitigation** | Intercepts the model's known fallback pattern (writing empty `agent-tools/<uuid>.txt`), runs real Bing RSS search, injects the actual results into both the Write content and the eventual tool_result body. | `scaffolding/pool/spoof-mitigation.mjs`, fires from `looksLikeAgentToolPlaceholderWrite()` in `api-server.mjs:533-540` |

## The trace through the gaps

A real session from 2026-05-23, `/tmp/ratlc-api.log` and
`/tmp/ratlc-pool.log`, request `req-e3190f49342b4982`:

```
16:06:48.807  POST /v1/messages → routed to ch-1294
16:06:52.443  → server_tool_use to client
              name=web_search query="ZenomTrader Premium whop.com review 2026"
              id=cursor-websearch-1
              ← PR #1 surfacing working: visible "[Cursor WebSearch]" in TUI

16:06:55.864  → web_search_tool_result to client
              id=cursor-websearch-1  results=error
              ← Gap 1: proxy couldn't extract result content from Cursor's stream
              ← claude-code renders this as "API Error: Response error: aborted"

(pool log around the same window)
              [cursor-agent] interactionQuery case=webFetchRequestQuery id=0
                not handled in vendored proto; abandoning so model falls back to MCP
              [cursor-agent][unknown-iq] id=0 bytes(114)=4a700a6e0a4c68747470733a2f2f...
              ← Gap 2: Cursor wanted to fetch
                "https://whop.com/joined/zenomtrader-premium/products/zenomtrader-premium-fb/"
                Decoded URL field from the hex; tool_use_id was "toolu_01ETLYBc5ck9cGLskT9Ygmcn"
              Five more abandoned webFetchRequestQuery in the next ~30s:
                id=3 → https://x.com/ZenomTrader
                id=6,7,8 → (more URLs, same outcome)

16:07:06.169  Model retried with more search queries; all also returned results=error.

16:07:14.021  → tool_use to client (translated): name=Write
              file_path="agent-tools/da9e2364-0b46-4a27-b412-ce5666c4ca43.txt"
              content="Whop Review: Legit Way To Make Money? [2026 Update]..."
              ← Gap 3: looksLikeAgentToolPlaceholderWrite checks content === '',
                content was non-empty (model pre-filled with hallucination),
                so mitigation skipped. Content reaches claude-code unmodified.

16:08:35.663  Three more Write calls to agent-tools/<uuid>.txt with hallucinated
              Goldman Sachs Romania factsheet content. Same gap-3 skip.
```

Each gap individually is recoverable; the model would either get a real
search result, a real fetched page, or have the spoof-mitigation
substitute real Bing data. With all three gaps active simultaneously,
the model has no way to recover and the hallucination reaches disk.

## Gap 1: WebSearch result extraction returns `error_code: 'unavailable'`

### Diagnosis

`scaffolding/pool/api-server.mjs:1017-1031` builds the
`web_search_tool_result` content block:

```js
const content = Array.isArray(event.content) ? event.content : (event.content || {
  type: 'web_search_tool_result_error',
  error_code: 'unavailable',
});
```

When `event.content` is empty or non-array, the proxy emits an error
result block. The comment at line 1046 admits the actual cause:

> "Cursor backend WebSearch completed without exposing result metadata
> to the proxy."

Translation: Cursor's backend ran the search and got results, but
encoded them in a structure the proxy doesn't decode. The proxy treats
absence-of-decoded-content as a failure.

### Proposed fix

Three options in increasing order of completeness:

**Fix 1A (instrumentation only)** — when extraction fails, log the raw
event bytes so we can build a corpus of unknown response shapes:

```js
if (extractedContent.length === 0) {
  log(`  ↪ websearch extract failed: raw=${Buffer.from(event.raw || []).toString('hex').slice(0, 400)}`);
}
```

Estimate: 10 minutes. Not a fix in itself; lets us collect data for
fix 1B/1C.

**Fix 1B (proto decode)** — reverse-engineer Cursor's WebSearch result
field layout from the captured bytes. Add a decoder. Surface real
result text.

Estimate: 2-4 hours plus one observation session.

**Fix 1C (proxy-side fallback search)** — when extraction fails, re-run
the captured query against Bing RSS on the proxy host and emit those
as the result.

Estimate: ~1 hour. The Bing-RSS helper already exists in
`scaffolding/pool/spoof-mitigation.mjs` (`performWebSearch(query)`).
We'd just call it from the failed-extraction branch and shape the
output as a `web_search_tool_result` content array.

### Recommendation

**Do 1A immediately** (10 min) to gather data. **Do 1C as the
shipping fix** (1 hour) because it produces a real, usable result for
the model. **Defer 1B** unless 1C reveals problems (e.g. quality
mismatch).

## Gap 2: `webFetchRequestQuery` interaction case unhandled

### Diagnosis

When Cursor's inner model decides to fetch a URL as a research
follow-up, it sends an `interactionQuery` frame with the
`webFetchRequestQuery` oneof case set. Our proxy in
`src/cursor-agent.js handleInteractionQuery` falls through to the
"unknown case" branch and abandons:

```
[cursor-agent] interactionQuery case=webFetchRequestQuery id=X
  not handled in vendored proto; abandoning so model falls back to MCP
```

The comment is optimistic: "falls back to MCP" assumes the model has
client-side `WebFetch` registered and will retry through MCP. In
practice the model usually falls back to the Write-spoof pattern
described in Gap 3.

We have empirical data on the wire format from
`[cursor-agent][unknown-iq]` hex dumps. Two examples decoded:

```
id=0: 4a700a6e0a4c<url-bytes>121e<tool_use_id-bytes>
  field 9 (length-delimited) wraps inner message:
    field 1 (length-delimited): URL string
      Example: "https://whop.com/joined/zenomtrader-premium/products/zenomtrader-premium-fb/"
    field 2 (length-delimited): tool_use_id string
      Example: "toolu_01ETLYBc5ck9cGLskT9Ygmcn"

id=3: 08034a3d0a3b0a19<url>121e<tool_use_id>
  field 1 (varint): 3 (this is the interaction id)
  field 9 (length-delimited) wraps inner message with URL + tool_use_id
```

So the proto structure is reasonably clear from observation alone.

### Proposed fix

Three options:

**Fix 2A (translate to claude-code WebFetch)** — when
`webFetchRequestQuery` arrives:
1. Decode the URL and tool_use_id from the wire bytes.
2. Emit a `tool_use` SSE block to claude-code with `name=WebFetch`,
   `input={url}`.
3. Wait for claude-code's `tool_result` POST.
4. Translate that back into the appropriate interaction response and
   send to Cursor.

Pros: uses claude-code's existing WebFetch implementation (which has
network access, page parsing, etc).
Cons: requires correctly synthesizing the response interaction
message — we don't know the response field layout yet.

Estimate: 1-2 days.

**Fix 2B (run fetch on the proxy host)** — like Fix 1C, run an HTTP
GET on the proxy host and synthesize the result back to Cursor.
Reuses existing fetch logic from `src/cursor-agent.js`
`fetchUrlForCursor()` (used for the `fetchArgs` exec case).

Pros: no claude-code round trip; faster. Cons: still need to know
Cursor's response interaction shape.

Estimate: 1 day plus probing time for the response shape.

**Fix 2C (clean rejection)** — synthesize a structured "URL fetch not
available" response so the model gets a definite NO instead of
silence. Stops the model from retrying-and-fabricating; forces it to
tell the user the request can't be fulfilled.

Pros: smallest code, no protocol reverse-engineering. Cons: degrades
capability (model can no longer follow up URLs from search results).

Estimate: 2-3 hours.

### Recommendation

**2C first** as a near-term improvement (the model fails honestly
instead of fabricating). **2B as the proper fix** when someone has a
quiet afternoon to probe the response shape — start by sending the
same shape we'd send for any `interactionQueryResult`, with the
fetched body in a likely-named field, and iterate against Cursor's
behavior.

## Gap 3: Spoof-mitigation only fires on empty writes

### Diagnosis

`scaffolding/pool/api-server.mjs:533-540`:

```js
function looksLikeAgentToolPlaceholderWrite(toolName, args) {
  const normalizedTool = anthropicTools.normalizeClientToolNameForPolicy(toolName);
  if (normalizedTool !== 'write') return false;
  const a = args && typeof args === 'object' ? args : {};
  const p = String(a.file_path || a.path || a.filename || '').replace(/\\/g, '/');
  const c = String(a.content ?? a.file_text ?? a.text ?? a.body ?? a.data ?? '').trim();
  return /^agent-tools\/[^/]+\.txt$/i.test(p) && (c === '' || c === '(No content)');
}
```

The path check is correct: `agent-tools/<uuid>.txt` is Cursor's backend
convention for staging WebSearch result text on Cursor's OWN
filesystem. There is no legitimate reason a model running through us
should ever Write to it from the client side — the model is always
either being intercepted (empty placeholder) or fabricating
(non-empty content).

But the content check `c === '' || c === '(No content)'` exempts the
fabrication case. Today the model is increasingly likely to pre-fill
content with hallucinated text instead of writing empty placeholders,
which means the mitigation has a falling hit rate as the model's
fabrication strategy evolves.

### Proposed fix

Three options:

**Fix 3A (broaden the predicate, always intercept)** — drop the content
check. Any write to `agent-tools/<uuid>.txt` triggers the existing
mitigation path: run real Bing RSS, replace content.

```js
return /^agent-tools\/[^/]+\.txt$/i.test(p);
```

Pros: smallest possible change, preserves all existing behavior for
the empty case, adds protection for the non-empty case. Reuses the
existing Bing-RSS infrastructure.
Cons: the proxy now overwrites whatever the model wrote, even on the
off chance the model had something useful in there. The path semantics
make this acceptable but worth flagging.

Estimate: 5 minutes (one-line change) + the existing extractQuery
function may need a tweak — currently it pulls the query from message
context, but for a pre-filled fabrication we should also accept the
fabricated content as a hint for what query to actually run.

**Fix 3B (reject loudly)** — return a structured tool_error for all
`agent-tools/<uuid>.txt` writes, telling the model that path is
backend-only and to use Cursor-native WebSearch instead.

Pros: honest, doesn't run any fallback. Cons: burns a turn; model may
end up in a retry loop.

Estimate: 5 minutes.

**Fix 3C (hybrid)** — empty writes get the existing real-search
injection. Non-empty writes get rejected with a clear error message.

Pros: distinguishes "model knew it had nothing" (intercept) from
"model fabricated something" (reject) — different signals, different
remedies.
Cons: marginally more code.

Estimate: 15 minutes.

### Recommendation

**Do 3A** — broadens the predicate and reuses the most proven part of
the mitigation. The "model had something useful in there" risk is
minimal because Cursor's backend convention is clear: this path is
Cursor's own staging area, not a user-visible artifact. Even if we
overwrite something the model had, the model sees real Bing results in
the next tool_result body and adapts.

## Priority ordering

If only one fix is implemented:

| Priority | Fix | Rationale |
|----------|-----|-----------|
| 1 | **Gap 3 Fix 3A** | Smallest code, closes the user-visible "fabrication reaches disk" symptom, regardless of whether the other two gaps are ever addressed. The model still gets real Bing results via the mitigation. |
| 2 | **Gap 1 Fix 1A + 1C** | Improves the model's experience when it DOES use Cursor-native WebSearch — gets real results instead of an error, doesn't have to fall back at all. |
| 3 | **Gap 2 Fix 2C** | Stops the model from getting silence; makes failures honest. Important for the model's behavior modeling but only matters if the model is leaning on `webFetchRequestQuery` heavily. |
| 4 | **Gap 2 Fix 2B** | The "proper" fix — Cursor's native research turns work end-to-end. Largest engineering investment; defer until the others are in place. |

All four together produce a robust web-research pipeline:

```
Model needs to research →
  Try Cursor native WebSearch  ──┐
    success → real results to model
    extract failed → fall back to proxy Bing  ──┐  (gap 1)
                                                │
  Try Cursor native WebFetch on URLs ──────────┤
    success → real page content                │
    abandoned → fall back to claude-code WebFetch tool  (gap 2)
                                                │
  If all of above fail and model writes agent-tools/<uuid>.txt:
    intercept (any content) → run real Bing → inject results  (gap 3)
                                                │
                                                ↓
                              Model never sees hallucination as success
```

## Testing strategy

Each fix needs an end-to-end test that demonstrates the failure mode
BEFORE the fix and the recovery AFTER. Suggested probes:

### Gap 1 test

Send a request that triggers Cursor's native WebSearch but with a
query that produces an extraction-failure result. Easiest reproduction
is to find a query from the live logs that produced
`results=error` and replay it. Verify:

- Without fix: log shows `error_code: 'unavailable'`, claude-code sees
  the error
- With Fix 1C: log shows `websearch fallback to proxy bing q="..."`,
  claude-code receives a real `web_search_tool_result` content array

### Gap 2 test

Find a query that produces `webFetchRequestQuery` interactions in the
pool log. Replay through the same client and check for behavior
change. Until Fix 2B is in place, you can directly inspect that the
abandoned interactions stop happening (Fix 2C synthesizes a response
that ends the interaction cleanly).

### Gap 3 test

Most reproducible. Send a `/v1/messages` request that asks about
recent news (forces web research). Watch `/tmp/ratlc-api.log` for
`tool_use to client (translated): name=Write
file_path="agent-tools/...txt"` lines. Verify:

- Without fix: content matches what the model fabricated, no
  spoof-intercept log
- With Fix 3A: log shows `spoof: intercepted non-empty
  agent-tools write`, content is replaced with real Bing results

## See also

- `BIDI_PAYLOAD_LIMIT.md` — same doc style (TL;DR + measurements +
  decisions + open follow-ups). Different topic.
- `TOKEN_REFRESH_DESIGN.md` — same doc style; planning doc for a
  different feature.
- `MERGE_NOTES.md` — when implementing any of these fixes, follow the
  rules for `src/cursor-agent.js` / `scaffolding/pool/api-server.mjs`
  changes (both files appear in PR #1 / PR #2 cherry-pick regions).

## Memory pointer

Future Claude Code sessions on this repo will see a one-line reference
in `MEMORY.md` pointing here.
