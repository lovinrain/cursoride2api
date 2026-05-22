# RATLC Cache Design

This document records the cache/session design for the 3001 + 4242 bridge.
It is intentionally narrower than full gateway projects such as sub2api,
CLIProxyAPI/CPA, or new-api: cursoride2api does not own user billing or an
admin panel, but it does need stable conversation routing, replay tolerance,
and controlled memory growth.

## Reference Patterns

- sub2api: PostgreSQL for durable account/config state, Redis for cache,
  queues, rate limits, and sticky-session routing. The important lesson for
  this project is not the billing model; it is that session affinity must be
  externalized when there are multiple accounts or worker processes.
- CLIProxyAPI/CPA: CLI-account proxying, model groups, and account scheduling.
  The useful pattern is separating account/channel health from the external
  OpenAI/Anthropic-compatible API surface.
- new-api: local memory cache plus Redis-backed shared cache with DB fallback.
  The useful pattern is L1 memory for hot reads and L2 shared cache for
  restart/multi-process continuity.

## Current State

3001 (`server.js`) owns:

- `activeBridges`: direct bridge sessions for non-RATLC tool flow.
- `bridgesBySessionId`: alternate index for reconnect-safe continuation.
- `conversationStates`: currently reserved for Cursor opaque state.
- `responsesStore`: `/v1/responses previous_response_id` memory history.
- `_modelsCache`: five-minute model-list cache.
- `thinkingHistory`: 30-minute thinking reinjection history.

4242 (`scaffolding/pool/*`) owns:

- channel pool state: model group, channel state, token health, open attempts.
- `toolUseIndex`: live `anthropic_tool_use_id -> channel/execId` mapping.
- `consumedToolUseIndex`: 30-minute duplicate/replay detection for already
  consumed `tool_use` ids.
- `requestQueue`: pending requests waiting for a ready channel.
- `thinking-buffer`: 30-minute per-conversation thinking reinjection buffer.
- spoof mitigation playbook: short-lived async result replacement for blocked
  empty `agent-tools/*.txt` placeholder writes.

All current caches are in memory. A process restart intentionally drops them.

## Cursor Cache Semantics vs Claude Code

Cursor IDE and Claude Code do not cache the same thing.

Cursor IDE behaves like a long-lived workspace agent:

- the IDE owns local workspace state, tool state, and the visible transcript;
- Cursor's backend owns a live agent stream keyed by `x-request-id` and
  `x-session-id`;
- the stream exchanges `conversationCheckpointUpdate` frames containing a
  `ConversationStateStructure`;
- the checkpoint may reference KV blobs created through Cursor's stream-local
  KV messages (`setBlobArgs` / `getBlobArgs`);
- native tools such as WebSearch can run inside Cursor's backend and may not
  map one-to-one to client-visible Anthropic tool cards.

Claude Code behaves like a stateless Anthropic-compatible client:

- the client owns the complete `messages[]` transcript;
- every `/v1/messages` request re-sends enough history for the model to answer;
- `tool_use_id` is the stable join key between assistant `tool_use` and the
  next user `tool_result`;
- prompt caching fields (`cache_creation_input_tokens`,
  `cache_read_input_tokens`) are accounting fields from the API protocol, not
  a guarantee that this proxy can reuse Cursor's internal checkpoint;
- client retries can replay a stale `tool_result` after the proxy has already
  consumed the original id.

This mismatch is why the bridge cannot treat Cursor checkpoint state like
Claude Code transcript cache. `server.js` deliberately does not store and
replay Cursor `conversationState`: Cursor's KV blob store has been observed to
be stream-scoped, and replaying a checkpoint on a fresh stream can fail with
`Blob not found`. For compatibility clients, the safer strategy is to rebuild
context from the client's transcript and only cache replay-safe metadata.

### Practical Consequences

- Cursor-native path: preserve stream/session affinity where possible. Cache
  only live stream objects, tool ids, health/cooldown, and small metadata.
- Claude Code path: accept full transcript semantics. Add windowing,
  summarization, tool-result truncation, and duplicate `tool_result`
  protection.
- RATLC pool path: warm Cursor channels are useful for latency, but the model
  context inside a channel must not be treated as the single source of truth
  when the external client is Claude Code.
- WebSearch/tool observability: Cursor-native server tools may be real even
  when Claude Code does not render a local tool card. The bridge should expose
  protocol-compliant `server_tool_use` events plus a visible text trace, but
  it should not invent fake local WebSearch/WebFetch executions unless the
  client explicitly provided those tools.

### Do Not Cache

- stream-scoped Cursor `conversationState` for reuse on a fresh stream;
- Cursor KV blob ids outside their original stream;
- local filesystem side effects claimed by model text unless a real client
  tool_result confirmed them;
- model fallback decisions without headers/metrics exposing the fallback.

### Safe To Cache

- client session id to routing preference;
- model cooldown/high-load windows;
- consumed `tool_use_id` ids for replay detection;
- `/v1/responses` previous response history;
- thinking summaries or bounded text reinjection;
- model list and tool matrix metadata.

## Target Cache Layers

### L1: In-Process Memory

Use for state tied to a single Node process or child channel:

- live bridge/channel objects
- request handlers and stream clients
- pending `tool_use` mappings
- consumed `tool_use` replay cache
- short thinking buffers
- small model-list cache

Properties:

- fastest path
- TTL bounded
- lost on restart
- no new dependency

### L2: Shared Redis Or SQLite

Use only for state that must survive restart or be shared by multiple 3001/4242
processes:

- session affinity: `clientSessionId -> model group/channel preference`
- consumed `tool_use` ids across restart
- per-model upstream pressure: rate-limit/high-load cool-down windows
- request idempotency keys for client retries
- `/v1/responses previous_response_id` history when using multiple workers

Redis is better for multi-instance deployment and queues. SQLite is acceptable
for a single host, simpler ops, and moderate traffic.

### Durable DB

Not currently needed unless this project grows into account management:

- API users and quotas
- token/account inventory
- audit logs
- billing records

## Cache Keys

Prefer stable client-provided session ids:

- `x-claude-code-session-id`
- `body.metadata.user_id.session_id`

Fallback hashes should include model, first user text, system fingerprint, tool
list hash, and remote address/port. This is already implemented in
`src/anthropic-tools.js`, but `conv-v2:model:sessionId` is the preferred path.

Suggested L2 key layout:

```text
ratlc:session:<sessionId>                 -> { model, group, lastChannelId, lastAccessMs }
ratlc:tool:live:<toolUseId>               -> { channelId, execId, requestId }
ratlc:tool:consumed:<toolUseId>           -> { consumedAt, channelId, execId }
ratlc:model:cooldown:<model>              -> { untilMs, reason, lastError }
ratlc:responses:<responseId>              -> { inputItems, outputItems, lastAccessMs }
ratlc:idempotency:<hash(request body)>     -> { responseId or final text, createdAt }
```

## TTL Defaults

- live bridge/channel state: process lifetime, evicted on channel death
- conversation/response state: 30 minutes
- thinking buffers: 30 minutes
- consumed tool ids: 30 minutes
- spoof playbook promises: bounded FIFO plus short wait timeout
- model list: 5 minutes
- queue timeout: 120 seconds by default
- model cooldown: 5-15 minutes after `resource_exhausted`/high load

## Failure Policy

Cache misses must be safe:

- missing `responsesStore` entry: treat `previous_response_id` as absent or
  return a clear invalid-request error if the client requires strict resume.
- missing live `toolUseIndex`: check consumed ids before returning an error.
  Duplicates should become a soft proxy notice, not malformed SSE.
- no ready channel: return a normal SSE text notice after queue timeout instead
  of leaving the client hanging indefinitely.
- high-load/rate-limit: drain or cool down the affected model group instead of
  opening many retrying channels.

## Implementation Roadmap

1. Keep L1 memory but harden it:
   - consumed `tool_use` TTL cache.
   - queue timeout.
   - metrics for held and consumed tool ids.
   - model cooldown counters.
2. Add a small cache abstraction:
   - memory backend first.
   - optional Redis backend behind env vars.
   - optional SQLite backend for single-host persistence.
3. Move resumable state into the abstraction:
   - `responsesStore`.
   - consumed tool ids.
   - model cooldown windows.
4. Add session affinity:
   - store `clientSessionId -> preferred group`.
   - avoid cross-model continuation unless explicit fallback is enabled and
     visible in headers.
5. Add compaction/windowing:
   - cap full-context bytes.
   - summarize old messages.
   - truncate large tool results.

## Current Config Knobs

```env
POOL_CONTEXT_MODE=hybrid
POOL_REINJECT_THINKING=1
POOL_REINJECT_THINKING_MAX_TURNS=2
RATLC_QUEUE_TIMEOUT_MS=120000
RATLC_CONSUMED_TOOL_TTL_MS=1800000
RATLC_SESSION_TTL_MS=1800000
POOL_GROUP_WAIT_MS=3000
POOL_CONCURRENT_OPENS=1
RATLC_RETRY_MODE=constant
RATLC_CONSTANT_INTERVAL_MS=60000
```

During upstream high load, prefer smaller pools and slower retry. A pool of 30
channels with 500ms retry can prolong account-level rate limits.

## Hybrid Context Mode

`POOL_CONTEXT_MODE=hybrid` implements the target bridge policy:

```text
External protocol remains Claude Code / Anthropic compatible.
Internal execution stays as close as possible to Cursor native live Agent sessions.
The bridge cache checks consistency and falls back to full transcript rebuilds.
```

The api-server decides the payload shape per request:

- `full` for new sessions, model changes, system changes, tool-list changes,
  or non-monotonic message counts.
- `last` for stable continuations of the same client session.

The pool-manager receives a `sessionKey` and prefers the same ready channel for
that session. If the sticky channel is busy/dead/missing, normal group routing
continues. This keeps Claude Code retries and transcript semantics intact while
allowing Cursor's live channel context to carry stable continuations.
