# Cursor Bidi Stream Payload Limit — Known Issue

This doc records what we know about Cursor's per-payload size limit on
the live agent stream, why PR #2's mitigation attempt was incomplete,
and the design decision to confine truncation to `POOL_CONTEXT_MODE=hybrid`
so that `full` mode honors its name.

This is a separate workstream from the immediate fix — we log it here
so the underlying constraint can be properly characterized and addressed
later instead of being papered over with a silent guard.

## The constraint

Cursor's bidi agent stream has an empirical payload-size limit somewhere
around 96–100 KB per `bajie_yield` tool_result. Above that, Cursor's
backend closes or stalls the live session. This was discovered by the
author of PR #2 (huaerye23) and recorded in their inline comment at
`scaffolding/pool/api-server.mjs:88-93`:

> Safety valve for clients such as Claude Code that echo complete
> messages[] history, tool results, and optional thinking back on every
> turn. Sending a very large full-context payload as a bajie_yield
> tool_result can make Cursor close or stall the live session.

PR #2 also added `scaffolding/pool/CACHE_DESIGN.md` which lists
"compaction/windowing" as roadmap item 5 (cap full-context bytes,
summarize old messages, truncate large tool results).

## What we DON'T know

- Exact threshold. Is it 96K, 100K, 128K? Does it depend on UTF-8
  byte length vs character count? PR #2's default is 98304 bytes;
  whether that's the actual limit or a conservative guess is unclear.
- Hard vs soft. Does the stream close immediately, or stall and
  eventually recover? Does Cursor send any explicit signal?
- Stream-level vs content-level. Is this an HTTP/2 / h1 frame size
  limit, or a Cursor backend processing-budget limit?
- Per-payload vs cumulative. The comment says per-tool_result. Have we
  tested whether splitting one large payload into multiple smaller
  bajie_yield tool_results works?
- Variance. Does the limit depend on model, account tier, beta-flag
  combinations, or upstream load?

These are open empirical questions. Until any of them is answered, the
98 KB threshold is an educated guess inherited from PR #2.

## PR #2's mitigation and why it's wrong

PR #2 added a byte-cap guard in `api-server.mjs`:

```js
if (CONTEXT_MAX_BYTES > 0 && text.length > CONTEXT_MAX_BYTES) {
  contextGuardReason = `full-context-too-large:${text.length}>${CONTEXT_MAX_BYTES}`;
  content = lastUserContent;            // ← drops ALL prior history
  text = cursorMcpContentToText(content);
  effectiveContextMode = 'last';
}
```

Three problems compound:

1. **Silent contract violation.** When the user sets
   `POOL_CONTEXT_MODE=full`, they're explicitly opting into full-history
   delivery. The guard fires silently — same `mode=full` label in the
   log line, no SSE notice, no header. The model receives a fresh
   conversation, and the client perceives this as a model defect
   ("Claude forgot what we discussed").
2. **Catastrophic recovery.** Dropping everything except the last user
   message means a single oversize turn destroys 100% of conversation
   memory, not 10%. A sliding-window approach (drop oldest first) would
   keep recency intact.
3. **Wrong default mode.** PR #2 ships `POOL_CONTEXT_MODE=full` as the
   typical config, but the guard logic was actually designed for
   `hybrid` mode (where session affinity means the sticky channel
   already holds prior turns natively, so falling back to `last` is a
   legitimate continuation). In pure `full` mode there is no such
   guarantee.

Live evidence collected from `154.9.227.226` on 2026-05-22: 6 of 18
sessions hit the guard, with 38 individual guard events. One session
sent 30 bytes of context after 31 messages of history were dropped —
the user asked "do you still remember what we wrote?" and the model
correctly answered "I don't retain memory between sessions" because the
proxy had stripped all 30 prior turns.

## Design decision

`POOL_CONTEXT_MODE=full` MUST be true to its name. The user opted into
full history; we send full history. If Cursor's stream closes on
oversize payload, that surfaces as a visible upstream error — the user
sees the constraint and can react (switch to hybrid, summarize on the
client side, raise an issue). Silent truncation is worse than visible
failure.

`POOL_CONTEXT_MODE=hybrid` is the right home for any byte-cap logic.
Hybrid mode maintains per-session state (`hybridSessions` Map keyed by
`sid:<model>:<clientSessionId>`) and decides full-vs-last per request
based on: new-session, model-changed, system-changed, tools-changed,
or non-monotonic-messages. When hybrid decides to send `full`, the
sticky channel will route to the same channel that handled prior turns,
which already holds those turns in its native conversation memory.
Truncating to `last` in that case is a real continuation, not memory
loss.

Caveat: hybrid mode has its own silent-failure paths — `hybridSessions`
is in-process memory, lost on api-server restart, and evicted after
`HYBRID_SESSION_TTL_MS` (30 min default). Channel-affinity is also
load-bearing: if routing lands on a different channel, the `last`
payload reaches a fresh-context channel and the model sees only the
latest turn. These are separate issues to address (visible proxy_notice
on guard fires, session-state persistence, tighter affinity guarantees).

## Status: PATCH APPLIED

The patch described below was applied as part of the cherry-pick of
PR #2 (commit `60fc2aa`) into `feat/ratlc-mvp` on 2026-05-22. The
guard-only-in-hybrid behavior is live in our branch. This section is
kept as documentation of WHAT was changed and WHY, for the next time
upstream evolves this region.

## The patch (applied during cherry-pick conflict resolution)

In `scaffolding/pool/api-server.mjs`, inside the `handleMessagesRequest`
rendering block (around line 1796), the change was:

```js
// Bidi-stream payload guard. Cursor's live agent stream has an
// empirical payload-size limit around ~96 KB. The cap is gated on
// hybrid mode only — see BIDI_PAYLOAD_LIMIT.md. In `full` mode the
// caller explicitly opted into complete history; honor that contract
// even if Cursor stalls on oversize payload (better a visible upstream
// error than silent context loss).
const guardActive = (POOL_CONTEXT_MODE === 'hybrid') && CONTEXT_MAX_BYTES > 0;
```

Then in the two existing guard sites, replace `CONTEXT_MAX_BYTES > 0`
with `guardActive`:

```diff
-      if (CONTEXT_MAX_BYTES > 0 && imageFullContentBytes > CONTEXT_MAX_BYTES) {
+      if (guardActive && imageFullContentBytes > CONTEXT_MAX_BYTES) {
         contextGuardReason = `image-full-context-too-large:${imageFullContentBytes}>${CONTEXT_MAX_BYTES}`;

...

-      if (CONTEXT_MAX_BYTES > 0 && text.length > CONTEXT_MAX_BYTES) {
+      if (guardActive && text.length > CONTEXT_MAX_BYTES) {
         contextGuardReason = `full-context-too-large:${text.length}>${CONTEXT_MAX_BYTES}`;
```

Net change: one new local variable + two find-replaces. No new files,
no API surface change, no env-var change. Behavior delta:

| Mode | Before this patch | After this patch |
|------|-------------------|------------------|
| `full` | Silently truncates above 96 KB | Sends full history; Cursor errors propagate visibly |
| `last` | Unchanged | Unchanged |
| `hybrid` | Cap fires on `full`-render path | Cap fires on `full`-render path (identical) |

## Future work (separate workstream)

To resolve the underlying constraint rather than route around it:

1. **Measure the actual limit.** Write a probe in `scaffolding/pool/`
   that grows `bajie_yield` payload size by 16 KB per step against a
   live channel, records when Cursor closes/stalls the stream, and
   reports the threshold per model + account tier. Until we have this
   number, 98304 is a guess.
2. **Test payload splitting.** If the limit is per-tool_result, sending
   one logical context across multiple `bajie_yield` messages may sidestep
   it. Worth probing.
3. **Smarter recovery for hybrid.** Sliding-window drop-oldest is the
   minimum upgrade over drop-all-but-last. Summarization, RAG-style
   compaction, or proxy-side memory store are more involved options.
4. **Visible proxy_notice when truncating.** Any time the proxy delivers
   less than the caller asked for, the SSE stream should carry a
   `[proxy_notice] context truncated: ...` text_delta or an
   `x-ratlc-context-truncated` header. The current silent failure mode
   is the root cause of "model forgot" support burden.
5. **Document hybrid as the recommended mode** in `CACHE_DESIGN.md`
   alongside `full`, so deployers don't default to `full` thinking it's
   safe (it is — but only with this patch in place).

## See also

- `scaffolding/pool/CACHE_DESIGN.md` (PR #2) — the broader cache/session
  design that introduced hybrid mode.
- `MERGE_NOTES.md` — references this doc under the rules for merging
  PR #2 from huaerye23.
- `api-server.mjs:176-213` (post-merge) — `decideHybridContext`
  implementation.
- Memory: `project_bidi_payload_limit.md` — short-form pointer for
  future Claude Code sessions.
