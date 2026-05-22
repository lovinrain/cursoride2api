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

## Empirical measurements (2026-05-22)

We probed the actual behavior with `scaffolding/pool/test_bidi_payload_limit.mjs`.
The probe sweeps payload sizes by sending a single user message with N bytes
of filler, then records SSE outcomes (message_start, message_stop, error,
timeout). Run conditions:

- Model: `claude-4.6-opus-max-thinking-fast`
- Token pool: 5 accounts, round-robin
- `POOL_CONTEXT_MODE=full` (no truncation guard active per our patch)
- Inter-probe delay: 5-10s
- Reps per size: 2

### Results (all 32 attempts succeeded)

| Size       | n | Success | Duration (min/max) | TTFB (min/max) | Notes |
|------------|---|---------|--------------------|----------------|-------|
| 32 KB      | 2 | 100%    | 1.6s / 1.9s        | 6 / 64 ms      | |
| 64 KB      | 2 | 100%    | 3.0s / 3.0s        | 9 / 12 ms      | |
| **96 KB**  | 2 | 100%    | 4.7s / 5.1s        | 12 / 12 ms     | **crosses PR #2's 98304 threshold** |
| 128 KB     | 2 | 100%    | 7.5s / 7.5s        | 15 / 15 ms     | |
| 192 KB     | 2 | 100%    | 15.4s / 15.5s      | 19 / 21 ms     | |
| 256 KB     | 2 | 100%    | 25.0s / 25.1s      | 23 / 44 ms     | latency plateau begins |
| 384 KB     | 2 | 100%    | 25.0s / 25.0s      | 33 / 34 ms     | |
| 512 KB     | 2 | 100%    | 25.0s / 25.1s      | 46 / 54 ms     | |
| 768 KB     | 2 | 100%    | 25.1s / 25.1s      | 91 / 142 ms    | |
| 1 MB       | 2 | 100%    | 25.1s / 25.1s      | 125 / 126 ms   | |
| 1.5 MB     | 2 | 100%    | 25.2s / 25.2s      | 215 / 233 ms   | |
| 2 MB       | 2 | 100%    | 25.3s / 25.4s      | 317 / 363 ms   | |
| 3 MB       | 2 | 100%    | 25.6s / 25.7s      | 611 / 681 ms   | |
| 4 MB       | 2 | 100%    | 26.1s / 26.2s      | 1135 / 1176 ms | |
| 6 MB       | 2 | 100%    | 27.3s / **122.4s** | 2.3s / **122.4s** | **variance grows sharply** |
| 8 MB       | 2 | 100%    | 29.0s / 50.3s      | 4.0s / 25.3s   | variance grows sharply |

### Findings

1. **PR #2's 98304-byte default appears unjustified for our environment.**
   Crossing the threshold at 96-128 KB shows zero behavior change. The
   guard fires silently and corrupts conversations, but the threshold
   it's protecting against doesn't manifest in our setup.

2. **No hard wall observed up to 8 MB.** Cursor accepted payloads roughly
   80x larger than PR #2's guard threshold without closing the stream
   or returning errors. The "stall" PR #2 was protecting against did
   not reproduce.

3. **Soft signal at 6 MB+**: success rate stays 100%, but latency variance
   grows dramatically. Same payload, two attempts: 27.3s vs 122.4s. At
   8 MB, similar variance (29.0s vs 50.3s). The upstream is feeling the
   size but isn't failing.

4. **TTFB scales roughly linearly with payload above 256 KB**: from ~50ms
   at 256K to ~700ms at 3MB to ~25s at 8MB (in the worst case). This
   matches network transmission + Cursor parse overhead.

5. **Duration plateau at ~25s from 256KB to 4MB** is most likely Cursor's
   "max-fast" thinking-budget ceiling, not a payload-limit signal. The
   model thinks for ~25s regardless of payload in that range.

### Possible explanations for PR #2's original observation

PR #2's author reported "Sending a very large full-context payload as a
bajie_yield tool_result can make Cursor close or stall the live session."
Our probe could not reproduce. Hypotheses:

- Misdiagnosis: an unrelated failure (network, throttle, channel state)
  was attributed to payload size.
- Account-tier variance: enterprise or free accounts may behave differently.
- Time-of-day load: Cursor under upstream load may have different limits.
- Prompt-structure dependency: many short turns vs one long string may
  exercise different code paths. Our probe uses a single big user message.
- Limit exists but at higher sizes than we probed (>8 MB).
- Limit is on cumulative per-channel state, not per-payload.

The first hypothesis is the most likely given the data. The threshold
98304 appears to be a guess that didn't match observation.

### What we still don't know

- Behavior with multi-turn message structure (many user/assistant/tool_use
  pairs totaling N bytes vs one user message of N bytes). Worth a separate
  probe variant if needed.
- Behavior at sizes >8 MB. Did not probe (impractical for typical loads).
- Behavior under sustained high load on the upstream side.
- Behavior on `claude-opus-4-7-thinking-max-fast` — the 4.7 group was
  too throttled to probe today; could differ.

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

## Decisions taken from the measurements

Based on the data above:

1. **Default `CONTEXT_MAX_BYTES` set to 0.** Since the threshold doesn't
   manifest in our environment up to 8 MB, defaulting the guard to active
   at 98 KB is solving a phantom problem. Operators who hit the issue in
   their own environment can re-enable explicitly with
   `RATLC_CONTEXT_MAX_BYTES=N`. This eliminates the silent context loss
   entirely for the default config.

2. **Hybrid-mode guard still gated on env var.** When CONTEXT_MAX_BYTES=0,
   guardActive=false even in hybrid mode. If a future measurement reveals
   a real limit, the env-var path remains available without code changes.

3. **Probe script kept in tree** at `scaffolding/pool/test_bidi_payload_limit.mjs`
   so the empirical measurement is reproducible by any operator on any
   environment. Useful baseline before deciding to enable the guard.

## Open follow-ups (lower priority now)

The original "future work" list assumed the limit was real and needed
mitigation. With the limit unmeasurable in our environment, several
items become exploratory rather than necessary:

1. **Probe `claude-opus-4-7-thinking-max-fast` ceiling.** Today's run
   was throttled. Worth a fresh probe in a low-load window. May reveal
   a lower ceiling than 4.6 (consistent with NIAH findings of 600K vs 900K
   for the model's own context).
2. **Probe multi-turn payload structure.** Same total bytes, but as N
   user/assistant/tool_use/tool_result turns instead of one big text
   message. Tests whether prompt structure (not just bytes) affects
   Cursor's behavior.
3. **Probe under sustained load.** Today's run was after a throttle had
   just cleared. Running during normal load may reveal different behavior.
4. **Sliding-window recovery for hybrid.** If/when the guard is ever
   needed, drop-oldest is still preferable to drop-all-but-last. Cheap
   to implement when motivated.
5. **Visible proxy_notice when truncating.** Same — relevant only if
   the guard actually fires in some user's environment.

These are now nice-to-have rather than blocking. The original "smoking
gun" — silent context loss in `full` mode — is gone by default.

## See also

- `scaffolding/pool/CACHE_DESIGN.md` (PR #2) — the broader cache/session
  design that introduced hybrid mode.
- `MERGE_NOTES.md` — references this doc under the rules for merging
  PR #2 from huaerye23.
- `api-server.mjs:176-213` (post-merge) — `decideHybridContext`
  implementation.
- Memory: `project_bidi_payload_limit.md` — short-form pointer for
  future Claude Code sessions.
