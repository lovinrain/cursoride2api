# Auto-Retry on Transient Upstream Failures

## TL;DR

Two distinct failure patterns where Cursor accepted the request but didn't
produce a useful response now have server-side auto-retry, controlled by
two independent env-var budgets. Both are **disabled by default** —
opt-in by setting the budget knobs > 0.

| Symptom (internal name) | Trigger | Default | Recommended |
|---|---|---|---|
| `upstream_silent_timeout` | Pool routed, Cursor accepted bidi frame, then >25s with no text/thinking/tool_use/yield/error | `RATLC_UPSTREAM_SILENT_RETRY_MAX=0` (off) | `=2` |
| `empty_assistant_turn` | Cursor cleanly ended the turn (yield/step_completed) but model emitted zero visible content | `RATLC_EMPTY_TURN_RETRY_MAX=0` (off) | `=1` |

Status as of 2026-05-24: implemented in `scaffolding/pool/api-server.mjs`,
committed, api-server-only restart sufficient (no bridge-worker changes).

## Why retry these

### `upstream_silent_timeout`

The proxy emits this when nothing comes back from Cursor's bidi stream
within `RATLC_NO_VISIBLE_EVENT_TIMEOUT_MS` (default 25000) after a
successful routing. The user sees:

> `[proxy_notice] Cursor upstream accepted the request but did not emit
> text, thinking, tool_use, yield, or error within 25000ms. The RATLC
> channel was likely waiting on an unrecognized Cursor exec message.
> Please retry after the channel is recycled.`

Empirically usually caused by:
- Cursor's backend issued an exec message our vendored proto doesn't
  decode → bridge-worker abandons → inner Cursor model hangs waiting
- Slow Cursor backend under load
- Channel state corruption

A different channel almost always succeeds because it's bound to a
different token + different account + fresh conversation state.

### `empty_assistant_turn`

The proxy emits this in `finishMessage` when the turn ended cleanly but
`!toolUseEmitted && outputTokens === 0 && !textBlockOpen`. The user sees:

> `[proxy_notice] Cursor ended this turn without visible text or tool
> calls. Any upstream thinking was captured for the next request, but
> there is no assistant-visible content to display.`

Causes are more mixed:
- Cursor backend dropped output frames (wire-level issue)
- Model thought-then-decided-no-response (legitimate)
- Pre-emptive content filter / refusal that surfaced as silence
- Long-context confusion

Retry can recover the wire-level case; the "model legitimately said
nothing" case would re-fire on retry. We mitigate this by:
1. Defaulting `RATLC_EMPTY_TURN_RETRY_MAX=0` (opt-in only)
2. Defaulting to thinking-aware behavior — if substantive thinking was
   captured, skip retry (the model thought and chose silence; respect it)
3. Recommended starting value `=1` (one retry, not aggressive)
4. Forcing a different channel via `sessionKey: null` on the replay
   (otherwise session affinity routes back to the same channel which
   would produce the same empty output)

## Behavior

```
T+0       POST /v1/messages → pool routes to ch-X
T+0       Proxy snapshots the send_user_message payload for replay
T+25s     For upstream_silent_timeout:
            → tryRetryRequest('upstream_silent_timeout')
            → cancel ch-X (kills it, releases for recycling)
            → wait RATLC_RETRY_DELAY_MS (default 500)
            → poolWrite same payload, same requestId
            → pool round-robins to a fresh channel
            → watchdog re-armed
T+25.5s+  Either succeeds → user never sees the retry (except for the
          one [proxy_notice] line on first retry if RATLC_RETRY_EMIT_NOTICE=1)
          OR times out again → retry++ until cap → falls through to the
          existing exhaustion notice
```

For `empty_assistant_turn`:
- Detected synchronously inside `finishMessage`
- `done = false` reverted, state reset
- Same cancel + delay + replay, but with `sessionKey: null` to break
  session affinity (otherwise same channel + same input → same empty)
- New turn streams in, replaces the would-be empty turn

## Safety invariants

`tryRetryRequest()` checks ALL of these before scheduling a replay:

1. **Retry budget not exhausted** for the specific symptom
2. **No client-visible content emitted yet** (`!messageStarted &&
   !toolUseEmitted`). If we already streamed something, retry would
   produce duplicates in the same SSE response.
3. **lastSendUserMessagePayload is captured** — only the initial
   `send_user_message` path is retryable. `send_tool_results` retries
   are unsafe because the consumed `tool_use_id` state can't cleanly
   be replayed (the bridge would see "already consumed" or worse).

If any check fails, returns `false` and the caller falls through to
the existing notice + finishMessage path.

## Cost model

Each retry burns:
- One real Cursor channel-turn (account quota + Cursor backend time)
- `NO_VISIBLE_EVENT_TIMEOUT_MS` (25s default) of client wait for
  upstream_silent_timeout
- ~the model's normal turn latency for empty_assistant_turn

Worst-case client wait before final exhaustion notice (defaults: silent=2,
empty=1, retry_delay=500ms):

| Trigger sequence | Total wait |
|---|---|
| Success on first attempt | normal latency |
| 1 silent retry → success | 25s + 0.5s + normal |
| 2 silent retries → exhausted | 25s + 0.5s + 25s + 0.5s + 25s = 76s |
| 1 empty retry → success | normal + 0.5s + normal |
| 1 empty retry → exhausted | normal + 0.5s + normal |

Pick max values according to your latency-vs-recovery tradeoff. Setting
either to 0 disables that symptom's retry entirely.

## Env-var contract

| Variable | Default | Range | Effect |
|---|---|---|---|
| `RATLC_UPSTREAM_SILENT_RETRY_MAX` | `0` | 0–N | 0 disables; N enables up to N silent-timeout retries per request |
| `RATLC_EMPTY_TURN_RETRY_MAX` | `0` | 0–N | 0 disables; N enables up to N empty-turn retries per request |
| `RATLC_EMPTY_TURN_RETRY_IGNORE_THINKING` | `0` | 0 or 1 | 1 = retry even if model captured thinking; 0 (default) = respect silent thinking decisions |
| `RATLC_RETRY_DELAY_MS` | `500` | ms | Backoff between cancel and replay |
| `RATLC_RETRY_EMIT_NOTICE` | `1` | 0 or 1 | 1 (default) = emit a single `[proxy_notice]` to the client on first retry; 0 = silent |

## Observability

Every retry adds to `request_log`:
- `retryCount` — total across both symptoms
- `lastRetrySymptom` — which fired most recently
- `lastRetryAt` — timestamp

Log lines from the api-server stdout (`/tmp/ratlc-api.log`):
- `→ retry: <symptom> attempt N/MAX requestId=...` — when retry decided
- `→ retry: <symptom> firing replay requestId=... attempt=N` — when replay sent
- `→ no visible upstream event timeout @25000ms requestId=... (retries exhausted N/MAX)` — final fallback after retries fail

Grep `/tmp/ratlc-api.log` for `retry:` to see all retry activity.

## What's NOT retried

- `send_tool_results` failures — too much state (consumed tool_use_id,
  channel mid-conversation) to safely replay
- Auth errors (`ERROR_NOT_LOGGED_IN`), quota exhaustion, rate-limit
  errors — these aren't transient, they're persistent and retrying
  would amplify the problem. Existing pool-manager handles them via
  `killTokenImmediately` / channel cooldown
- Requests where `messageStarted` or `toolUseEmitted` is true at
  timeout — partial SSE content can't be undone

## Open follow-ups (not blocking)

1. **Same-channel-twice avoidance** — if a retry happens to land on
   the same channel that just timed out (e.g. small pool), it'll
   likely fail again. Worth tracking the channel-id per attempt and
   excluding it from the next pick. Currently round-robin makes this
   unlikely but not impossible.
2. **Adaptive backoff** — pure 500ms delay is fine for v1. Could
   add exponential if we see retry-storms exhausting the pool.
3. **Per-token retry counter** — if one token keeps producing silent
   timeouts, mark it suspect. Right now retries are per-request, not
   per-token.
4. **Telemetry on `/metrics`** — counter `upstream_retries_total{symptom=...,outcome=...}`.
5. **Auto-retry for `send_tool_results`** — requires deeper plumbing
   to release consumed-id state before replay. Worth exploring if we
   see the failure pattern there in practice.

## See also

- `BIDI_PAYLOAD_LIMIT.md` — separate symptom (payload too large for
  bidi stream); not currently retryable because it's reproducible
- `WEB_RESEARCH_GAPS.md` — closes most of the failure modes that
  USED to cause silent timeouts (webFetchRequestQuery, etc.); retry
  is the residual safety net for what's still unknown
- `MERGE_NOTES.md` — when this region of `api-server.mjs` is touched
  in a merge, the retry helper and per-call-site integration must be
  preserved together; cherry-picking only the helper without the
  call-site wiring leaves it dead code
