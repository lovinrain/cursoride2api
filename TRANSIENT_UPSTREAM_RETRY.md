# Transient-upstream in-place retry — rationale & correctness

**Status:** LIVE (default on). Kill-switch: `RATLC_RETRY_TRANSIENT_UPSTREAM=0`.
**Commit:** `3e34ac2` · **Code:** `src/cursor-agent-h1.js` (`isRecoverableUpstreamError`, `failOrRetry`).
**Test:** `tests/src/recoverable-upstream-test.js`.
**Related:** `UPSTREAM_RETRY_DESIGN.md` (the *api-server* cross-channel retry layer — a different, complementary layer).

---

## TL;DR

When a live turn hits a **transient Cursor backend blip** (RunSSE `502/503/504`,
`Response error: aborted`, `BidiAppend` fetch/5xx, socket reset) *before any
client-visible output*, we now **re-issue the same turn on a fresh HTTP stream to
the same Cursor conversation** — instead of killing the whole channel and paying a
(throttle-expensive) re-open. Fatal faults (auth / quota / rate-limit) are **not**
retried; they still tear the channel down.

This does **not** break the "never-ending conversation" design (see Correctness).
If it misbehaves, flip the kill-switch — no code revert needed (see Revert).

---

## Why the channel matters (the design constraint we must not break)

A RATLC **channel = one long-lived Cursor Agent conversation** (a stable
`conversationId`) that serves **many turns**. This is deliberate — it mimics an
MCP-style *never-ending conversation*: open the conversation once, then stream
turn after turn into it. Opening a conversation is the **expensive** operation
(under Cursor throttle we've observed 200+ open attempts / ~8 min for one channel).
So the whole point of the pool is to **keep conversations open and reuse them**.

The bug we're fixing directly attacks that: the bridge used to `process.exit(1)`
the worker on **any** live-turn error, discarding a warm conversation over a
transient hiccup and forcing a fresh, expensive open. Under Cursor instability
(502/503 storms) this churned the pool (`ready → opening`) continuously. See the
death audit (`ratlc deaths` / TUI view 7): the dominant reasons were
`worker:error (Response error: aborted)`, `RunSSE 502/503`, `BidiAppend 503`.

## What changed

`cursor-agent-h1.js` `failOrRetry()` already re-issues a turn on a fresh stream
with exponential backoff, gated by `!hasEmittedContent`. Its `isTransient`
classifier only matched NGHTTP2 / socket errors — so the Cursor *backend* errors
above fell through to `fail()` → `onError` → `setState('dead') + process.exit(1)`.

We broadened it with `isRecoverableUpstreamError(msg, code)`:
- **Retries:** `HTTP_50[234]`, `ERR_BIDI_APPEND[_50x]`, `ERR_RES`, `ERR_REQ`, and
  `ERR_STREAM` when the message is an abort/reset/socket/timeout.
- **Never retries (fatal, checked FIRST so it wins even under a 5xx code):**
  `ERROR_NOT_LOGGED_IN`/`unauthenticated`, `ERROR_RATE_LIMITED_CHANGEABLE`/`API
  usage limit`, `resource_exhausted`/`rate limit`/`too many requests`/`too many
  computers`, `unpaid invoice`.

Bounded by `MAX_REQUEST_RETRIES` (5) + backoff `[100,250,750,2000,5000]ms`
(doubled in a detected LB cascade, capped 8s). Toggle: `RATLC_RETRY_TRANSIENT_UPSTREAM=0`.

---

## Correctness: does the retry preserve the never-ending conversation?

**Yes.** The concern is: "does a retry silently start a *new* conversation and lose
the accumulated multi-turn context?" It does not. Three facts (all in
`cursor-agent-h1.js`):

1. **`conversationId` is stable across a retry.** It's set once at
   `startConversation` (line ~202) and is a closure variable reused verbatim in the
   re-issued `runRequest` (line ~1060). `failOrRetry` → `attemptConnection` never
   regenerates it. Cursor therefore treats the retry as a **continuation of the
   same conversation**, not a new one.
2. **The exact same turn payload is replayed.** The initial encoded
   `AgentRunRequest` is cached (`cachedInitialEncoded`, line ~1082) and re-sent
   byte-for-byte on retry (line ~1148-1149). Same `conversationId`, same
   `conversationState`, same user message.
3. **`appendSeqno = 0n` resets the STREAM, not the conversation.** `seqno` is
   per-RunSSE-stream (a fresh HTTP POST starts at 0); the *conversation* is
   identified by `conversationId`, which is unchanged. So it's "new socket, same
   conversation."

And it's belt-and-suspenders across context modes:

| `POOL_CONTEXT_MODE` | Multi-turn context lives in… | In-place retry safe? |
|---|---|---|
| **`full`** (active; `launch.yaml:22`) | the payload — the api-server re-renders the **entire** history every turn | ✅ Yes — the full history is *in the replayed payload*, so context can't be lost regardless of conversation state |
| `last` | Cursor's **server-side** conversation state (only the delta is sent) | ✅ Yes — the retry reuses the **same `conversationId` on the same channel**, so the server-side state is intact |

So in `full` mode (what we run) it's doubly safe; in `last` mode it's still safe
because this retry is **in-place on the same conversation** (it is *not* a
cross-channel retry).

**Not new behavior.** This retry *mechanism* (re-`attemptConnection` on the same
conversation) already existed and ran for NGHTTP2/`REFUSED_STREAM` errors. We only
widened *which errors* trigger it. If it were going to corrupt multi-turn state, it
would already have done so on every LB-cascade retry. It hasn't.

### Where context COULD be lost (and why it isn't our change)

The *api-server* has a separate, higher layer that retries on a **different**
channel (`upstream_abort` / `empty_assistant_turn` / silent-timeout — see
`UPSTREAM_RETRY_DESIGN.md`). In `last` mode a *cross-channel* retry could land on a
channel without this conversation's server-side state — but that path already
rebuilds a **full-context** payload as a fallback (`api-server.mjs`, the
`send_tool_results` retry builds `buildFullContextCursorMcpContent` with
`contextMode:'full'`). That is pre-existing and unaffected by this change. Our
change is the *lower* layer (same conversation, same channel) and is the safest
place to retry.

---

## Risks & the "is it doing well?" signals

Watch these after enabling (all visible without extra tooling):

- **Good (expected):** in `ratlc deaths` / TUI **view 7**, the
  `worker:error (Response error: aborted)`, `RunSSE 502/503`, and `BidiAppend`
  death rows should **drop sharply**; what remains should be genuinely fatal
  (`worker:quota_exhausted`, `auth`). The death-churn breadcrumb on the split view
  should shrink.
- **The retry cost:** turns that hit a blip take slightly longer (up to a few
  seconds of backoff) instead of erroring. Watch `ratlc stats` first-byte p99 —
  a modest rise is the expected trade for far fewer channel deaths.
- **Cursor genuinely down (not transient):** you'll see repeated
  `[cursor-agent-h1] retrying after RunSSE non-200: 503 (N/5)` in the pool log,
  then a death after 5 attempts. That's correct (bounded) — it degrades to the old
  teardown behavior after exhausting retries, and the api-server's cross-channel
  retry is the backstop.

**BAD signals → revert:**
- **Duplicated or incoherent responses.** This is the failure mode the design
  worries about (a retry double-sending a turn, or losing context). It should be
  impossible (the `!hasEmittedContent` gate blocks retry after any output, and the
  conversation is preserved) — but if you see the model repeat itself or "forget"
  the immediately-prior turn on a request that visibly retried, **flip the
  kill-switch and tell us.**
- **A rise in `unexpected_turn_ended` or empty turns** correlated with retries
  (would suggest the re-issued stream confuses Cursor's conversation state).
- **Retry storms** that don't converge (pool log full of retry lines, latency
  spiking) — Cursor is hard-down and we're hammering it; kill-switch to fall back
  to fast teardown + cross-channel retry.

---

## Revert

**Fast, no deploy of code (preferred first step):** disable the widened retry via
the kill-switch. Set in `scaffolding/pool/launch.yaml`:

```yaml
RATLC_RETRY_TRANSIENT_UPSTREAM: 0
```

then `./launch.sh up` (or just let channels respawn — each new bridge-worker reads
the env). This reverts to the **old behavior** (transient backend errors tear the
channel down immediately) while keeping the NGHTTP2/socket retry that predates this
change. No code change, instantly reversible.

**Full code revert:** `git revert 3e34ac2` then `./launch.sh up`.

**Note on deployment:** each bridge-worker is a fresh process that `require`s the
on-disk `cursor-agent-h1.js`, so **respawned channels pick up both the fix and the
kill-switch automatically**; a full `./launch.sh up` applies it to all channels at
once.
