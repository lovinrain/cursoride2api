# IPC contracts for the RATLC pool stack

Three processes, two protocols.

```
┌─────────────────────────────────────┐
│  Pool Manager (parent process)      │           PROTOCOL B
│  - /tmp/ratlc-pool.sock (listen)    │ ←──────── Unix socket, line-delimited
│  - forks N bridge-worker children   │           JSON
│                                     │              │
│   ┌──────────┐  ┌──────────┐  ...   │              │
│   │worker 0  │  │worker 1  │        │              │
│   └──────────┘  └──────────┘        │              │
│       ↑              ↑              │              │
│       └── PROTOCOL A ┘              │              │
│           process.send                              │
└─────────────────────────────────────┘              │
                                                     │
                                  ┌──────────────────┴────────────────┐
                                  │                                   │
                          ┌───────┴────────┐               ┌──────────┴──────────┐
                          │  api-server    │               │  ratlc-ctl          │
                          │  HTTP :4242    │               │  (CLI)              │
                          └────────────────┘               └─────────────────────┘
```

## Protocol A — Pool Manager ↔ Bridge Worker (via `process.send` IPC, JSON objects)

### Manager → Worker

```jsonc
{ "type": "open", "model": "claude-opus-4-7-thinking-max-fast",
  "tools": [{"name":"Read", "input_schema":{...}}, ...], "system": "<caller system>" }

{ "type": "send_user_message", "requestId": "req-abc", "text": "What is 2+2?" }

// User message with optional image attachments. This is the default
// /v1/messages path: the worker answers the currently parked bajie_yield by
// sending content.items as the MCP tool-result payload. Reusing a ready
// channel avoids opening a fresh Cursor Run for each image request.
{ "type": "send_user_message", "requestId": "req-abc",
  "text": "What is in this image?<image/>",
  "content": { "items": [
    { "kind": "text", "text": "What is in this image?" },
    { "kind": "image", "mediaType": "image/png", "dataBase64": "..." }
  ] }
}

// Optional/native image path. This opens a native Cursor image Run where
// UserMessage.selectedContext.selectedImages carries the images. It is not
// used by the default /v1/messages image route because it consumes upstream
// Run admission capacity.
{ "type": "send_native_image_message", "requestId": "req-abc",
  "text": "What is in this image?",
  "content": { "items": [
    { "kind": "text", "text": "What is in this image?" },
    { "kind": "image", "mediaType": "image/png", "dataBase64": "..." }
  ] }
}

{ "type": "send_tool_result", "requestId": "req-abc", "execId": "<from previous tool_use>", "content": "..." }

{ "type": "ping", "requestId": "ping-1234" }       // a no-op user message just for keepalive

{ "type": "shutdown" }                              // graceful close
```

### Worker → Manager

```jsonc
// State updates — sent on every transition.
{ "type": "state", "state": "spawning"|"opening"|"ready"|"busy"|"dead",
  "openAttempts": 231, "openedAt": 1730000000000, "lastActivityAt": 1730000000000,
  "model": "...", "error": "..." }

// Streaming response events — keyed to requestId from the matching send_*.
{ "type": "text_delta", "requestId": "req-abc", "text": "Hello" }

{ "type": "tool_use", "requestId": "req-abc", "execId": "<cursor exec id>",
  "name": "Read", "args": {"file_path":"/foo"} }

{ "type": "yield", "requestId": "req-abc" }        // round complete (model called bajie_yield)

{ "type": "error", "requestId": "req-abc"|null, "message": "..." }

// Heartbeat (sent every 30s so manager can detect hung workers)
{ "type": "heartbeat", "now": 1730000000000 }
```

### Worker lifecycle

```
spawning → opening (retry lottery) → ready → busy → ready → ... → dead
```

- `spawning` — process forked, hasn't yet called `startConversation`
- `opening` — running the retry-until-success lottery
- `ready` — bridge open, parked in `bajie_yield`, awaiting user message
- `busy` — currently servicing a request OR holding a tool_use waiting for tool_result
- `dead` — fatal error; process should exit so manager can respawn

## Protocol B — Pool Manager ↔ Clients (api-server, ratlc-ctl) over Unix socket

Listen path: `/tmp/ratlc-pool.sock` (override with `POOL_SOCK`).

Wire format: newline-delimited JSON. One line = one message.

### Client → Manager — request types

```jsonc
// Open a request — manager picks a ready channel.
// For new conversation turn:
{ "type": "request", "requestId": "req-abc",
  "action": "send_user_message", "text": "...",
  "system": "<caller system>", "tools": [...],   // first request's tools become the pool's tools
  "model": "claude-haiku-4-5-fast"               // optional — names the target group;
                                                  // omitted/unknown → default group
}

// Same request shape as send_user_message, but with multimodal content.items.
// This is the default Anthropic image route and is delivered as a bajie_yield
// MCP tool result on an already-ready channel.
{ "type": "request", "requestId": "req-abc",
  "action": "send_user_message", "text": "...<image/>", "content": {"items":[...]},
  "system": "<caller system>", "tools": [...],
  "model": "claude-haiku-4-5-fast" }

// Optional/native image route for explicit experiments. Opens a fresh Cursor
// Run and carries images through native selectedImages instead of bajie_yield.
{ "type": "request", "requestId": "req-abc",
  "action": "send_native_image_message", "text": "...", "content": {"items":[...]},
  "system": "<caller system>", "tools": [...],
  "model": "claude-haiku-4-5-fast" }

// For tool_result follow-up:
{ "type": "request", "requestId": "req-abc",
  "action": "send_tool_result",
  "anthropic_tool_use_id": "toolu_xyz", "content": "..." }

// For N parallel tool_results from a single assistant turn:
{ "type": "request", "requestId": "req-abc",
  "action": "send_tool_results",
  "model": "claude-haiku-4-5-fast",              // optional — informational; routing is
                                                  // forced to the channel that emitted
                                                  // the matching tool_use
  "results": [
    { "anthropic_tool_use_id": "toolu_aaa", "content": "..." },
    { "anthropic_tool_use_id": "toolu_bbb", "content": "..." }
  ]
}

// Snapshot pool state. Response includes pool.groups[] and per-channel group field.
{ "type": "status" }

// List groups only (lighter snapshot).
{ "type": "list_groups" }
// → { "type": "groups", "groups": [{ model, isDefault, draining, target, actual,
//                                    ready, busy, opening, dead, rounds }, ...] }

// Register a new model group (or resize an existing one).
{ "type": "add_group", "model": "claude-haiku-4-5-fast", "size": 3 }
// → { "type": "ack", "message": "group <m> added (target=3)" }
//   or { "type": "error", "message": "..." } if model missing / already draining

// Drain a non-default group: refuses new dispatches, lets in-flight finish,
// kills channels once idle, removes the group entry when channel count hits 0.
// Refused on the default group.
{ "type": "remove_group", "model": "claude-haiku-4-5-fast" }
// → { "type": "ack", "message": "group <m> draining (N channels to evict)" }
//   or { "type": "error", "message": "cannot remove default group" }

// Scale a single group (defaults to default group).
{ "type": "ramp_up",   "count": 3, "group": "claude-haiku-4-5-fast" }
{ "type": "ramp_down", "count": 2, "group": "claude-haiku-4-5-fast" }
// group field is optional; omitted = POOL_MODEL.

// Force-restart a specific channel.
{ "type": "restart_channel", "channelId": "ch-3" }

// Shutdown the whole pool gracefully.
{ "type": "shutdown" }
```

### Manager → Client — streamed events on a request

```jsonc
// First event after request acceptance: tells the client which channel +
// group the request was routed to. api-server uses this to stamp the
// x-ratlc-* response headers BEFORE writing the SSE preamble.
{ "type": "route_decision", "requestId": "req-abc",
  "channelId": "ch-7", "servedModel": "claude-haiku-4-5-fast",
  "requestedModel": "claude-haiku-4-5-fast",     // what the client asked for
  "fallback": false,                              // true when servedModel ≠ requestedModel
  "fallbackReason": null                          // "unknown-model" | "group-draining" | "group-no-ready"
}

// Routed to whichever channel won the request.
{ "type": "text_delta", "requestId": "req-abc", "text": "Hello" }

// Tool_use events — manager mints anthropic_id and remembers (channelId, execId).
{ "type": "tool_use", "requestId": "req-abc", "anthropic_id": "toolu_abc...",
  "name": "Read", "args": {"file_path":"/foo"} }

// Round complete — channel goes back to `ready`.
{ "type": "yield", "requestId": "req-abc" }

// Errors — manager may downgrade the channel to dead and pick another.
{ "type": "error", "requestId": "req-abc"|null, "message": "..." }
```

### Status response shape

```jsonc
{
  "type": "status",
  "pool": {
    "configuredSize": 5,                 // sum of all groups' targetSize
    "channels": [
      { "id": "ch-0", "state": "ready",  "group": "claude-opus-4-7-thinking-max-fast",
        "openedAt": ..., "openAttempts": 87, "lastActivityAt": ..., "idleMs": 240000,
        "roundsServed": 14, "pid": 12345 },
      { "id": "ch-1", "state": "opening", "group": "claude-haiku-4-5-fast",
        "openAttempts": 123, "pid": 12346 },
      { "id": "ch-2", "state": "busy",   "group": "claude-haiku-4-5-fast",
        "currentRequestId": "req-abc",
        "openAttempts": 92, "pid": 12347, "roundsServed": 7 },
      // ...
    ],
    "readyCount": 3, "busyCount": 1, "openingCount": 1, "deadCount": 0,
    "pendingRequests": 0,
    "toolUseIndex": 0,
    "defaultGroup": "claude-opus-4-7-thinking-max-fast",
    "groups": [
      { "model": "claude-opus-4-7-thinking-max-fast", "isDefault": true,
        "draining": false,
        "target": 3, "actual": 3,
        "ready": 2, "busy": 1, "opening": 0, "dead": 0,
        "rounds": 14 },
      { "model": "claude-haiku-4-5-fast", "isDefault": false,
        "draining": false,
        "target": 2, "actual": 2,
        "ready": 1, "busy": 0, "opening": 1, "dead": 0,
        "rounds": 7 }
    ]
  },
  "config": {
    "model": "claude-opus-4-7-thinking-max-fast",   // default group
    "toolMode": "translate", "bridgeProtocol": "h1",
    "contextMode": "full", "reinjectThinking": 1,
    "concurrentOpens": 5,
    "groupWaitMs": 5000,                            // POOL_GROUP_WAIT_MS
    "idlePingMs": 1200000, "pingTimeoutMs": 45000,
    "poolToolsContractCount": 14,                   // contract-mode tool count
    "poolToolsSignature": "Bash,Edit,Read,..."
  }
}
```

## Key invariants

1. **Tool-round-trip stickiness.** When a worker emits a `tool_use`, the manager records
   `(anthropic_id) → (channelId, execId)`. The next request's `send_tool_result` MUST be
   routed back to that exact channel — otherwise the inner agent has no pending tool to
   respond to and the round breaks.

2. **One request per channel.** Each channel services at most one request at a time
   (a request being either a `send_user_message` or `send_tool_result` cycle that ends
   with `yield`). Channels in `busy` state are skipped by the round-robin picker.

3. **Tool list changes trigger a full reopen.** If a request comes in with a tool list
   that differs from what the pool was opened with, the manager closes and reopens
   ALL workers (because they need to be in sync). For MVP this is acceptable; in
   practice claude-code's tool list is stable per session.

4. **Channel respawn is automatic.** When a worker exits (`process.on('exit')`) or
   emits a `dead` state, the manager replaces it with a fresh fork. The new worker
   starts in `spawning` → `opening` and joins the rotation when `ready`.

5. **Idle ping.** A `ready` channel that has been idle ≥ `idlePingMs` (default 20 min)
   gets a `ping` message (round-trip through `bajie_yield` with content like
   `[health-check] reply with exactly OK`). If the round doesn't complete within
   30s OR returns an error, the channel is marked dead and respawned.

6. **API server is stateless.** All persistent state lives in the pool manager.
   Restarting api-server.mjs reconnects to the same pool, loses no warm channels.

7. **Groups are pool-side only.** Each channel is owned by exactly one group
   (its `group` field is the group's model id). LRU pick + idle-ping +
   auto-respawn + ramp are per-group operations. The default group is
   always defined by `POOL_MODEL`/`POOL_SIZE` and cannot be removed.

8. **Global open budget.** `POOL_CONCURRENT_OPENS` is a single rate-limit
   budget across all groups against Cursor's `/Run` endpoint. The opener
   round-robins through groups that still need channels, but the total
   number of channels in `spawning`/`opening` state at any moment is
   capped by `POOL_CONCURRENT_OPENS`.

9. **Channel ids are globally monotonic.** `ch-0`, `ch-1`, ... are minted
   from a single counter regardless of which group owns the channel.
   Restart by channel id still works the same way.

10. **Fallback to default.** A `request` with `model` set to an unknown or
    draining group falls back to the default group. If the named group
    exists but has zero ready channels, the request waits up to
    `POOL_GROUP_WAIT_MS` for one to surface, then falls back. The
    `route_decision` event carries `fallback: true` and `fallbackReason`
    so the api-server can stamp the response headers.
