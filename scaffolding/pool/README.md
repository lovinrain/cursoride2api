# RATLC pool — user guide

A four-process stack that serves the Anthropic Messages API by holding a
**pool of pre-warmed Cursor agent streams**. Pay the probabilistic
`unpaid_invoice` retry lottery once per channel; reap fast responses for
the rest of each channel's lifetime.

> **Relationship to `server.js` (repo root):** completely separate. The
> root `server.js` is the *original* per-request proxy and uses features
> like `CURSOR_REINJECT_THINKING`. The RATLC pool (this directory) is a
> different code path that maintains warm conversations. They share
> protobuf and HTTP helpers via `src/cursor-agent*.js` but the rest of
> the stack is independent.

## Processes

| Process | What it does | Restart cost |
|---|---|---|
| **`pool-manager.mjs`** | Forks N `bridge-worker` children, maintains the pool, pings idle channels every 20 min, auto-respawns dead ones. Listens on `/tmp/ratlc-pool.sock`. | High — restarting loses all warm channels |
| **`bridge-worker.mjs`** *(child)* | Owns one Cursor `RunSSE` stream + matching `BidiAppend` POSTs. Managed by the pool. | Auto-respawn |
| **`api-server.mjs`** | HTTP `:4242` serving `/v1/messages` (Anthropic format). Stateless. | **Free** — restart anytime, pool stays up |
| **`ratlc`** (unified CLI) | Single entry point: `up`, `down`, `status`, `watch`, `tui`, `ramp`, `restart`, `subagent on\|off\|status`, `failures [N]`, `inspect <ch>`, `deaths [N]`, `claude`, `tail`, `metrics`, `logs` | n/a |

## Recommended launch (for claude-code use)

The cleanest path is via `launch.sh` + `launch.yaml`:

```bash
./scaffolding/pool/launch.sh up           # bring pool up using config from launch.yaml
./scaffolding/pool/launch.sh status       # snapshot
./scaffolding/pool/launch.sh tui          # interactive TUI
./scaffolding/pool/launch.sh edit         # edit the config in $EDITOR (defaults vim)
./scaffolding/pool/launch.sh down         # tear it all down
```

`launch.yaml` holds the env-var configuration in one place (pool size,
model groups, retry tuning, native-tool passthrough, etc.). Override any
single var at the command line without editing the file:

```bash
POOL_SIZE=20 ./scaffolding/pool/launch.sh up
```

Or use a different config:

```bash
RATLC_LAUNCH_CONFIG=/path/to/other.yaml ./scaffolding/pool/launch.sh up
```

Then point claude-code at the proxy:

```bash
./scaffolding/pool/ratlc claude
# or manually:
ANTHROPIC_BASE_URL=http://127.0.0.1:4242 \
  claude --dangerously-skip-permissions --effort max
```

The first launch takes 1–10 min (probabilistic gate). Subsequent restarts
are faster as long as you didn't kill `pool-manager.mjs`.

### Legacy inline-env launch (still works)

If you prefer not to use `launch.sh`:

```bash
POOL_BRIDGE_PROTOCOL=h1 \
POOL_TOOL_MODE=translate \
POOL_CONTEXT_MODE=full \
POOL_CONCURRENT_OPENS=5 \
RATLC_PASSTHROUGH_NATIVE=1 \
  ./scaffolding/pool/ratlc up 10
```

`RATLC_PASSTHROUGH_NATIVE=1` is required for Cursor's native WebSearch /
ExaSearch to fire — without it the bridge abandons every InteractionQuery
and the model falls back to confabulation. `launch.yaml` sets this by
default; if you launch inline make sure it's there.

### Sub-agents (Task / multi-agent teams)

In `translate` mode the Cursor model can spawn its own **native sub-agents**.
The proxy forwards each one to claude-code as a `Task` tool_use so claude-code
runs it as a real sub-agent. Cursor's native frame carries *Cursor's* values
(`subagent_type` like `explore`/`shell`, `model` like `composer-2.5-fast`),
which claude-code's `Task` schema rejects with **`Invalid tool parameters`** —
so the proxy **normalizes** them (`buildSubagentToolArgsFromWire`):

- `subagent_type` → clamped to `general-purpose` (the agent type present in
  every claude-code install). Restore fidelity by mapping specific Cursor types
  to your installed agents via `RATLC_SUBAGENT_TYPE_MAP="explore=Explore,plan=Plan"`.
- `model` → kept only if it's a claude-code model keyword (`sonnet`/`opus`/`haiku`);
  Cursor backend slugs are dropped so the sub-agent inherits the parent model.
- `resume` → dropped unless `RATLC_SUBAGENT_FORWARD_RESUME=1`.

Set `CURSOR_LOG_NATIVE_EXEC=1` to see each `subagent passthrough … type:X→Y model:X→Y`
line. **Operational note:** a native sub-agent holds the *parent* channel in
`wait-tool` while claude-code runs the child as a **separate conversation that
needs its own free channel** — keep **≥2 free channels** (parent + child) or the
child queues against `POOL_GROUP_WAIT_MS`. The parent sits in `wait-tool` for the
sub-agent's whole lifetime; it is reaped on the generous `RATLC_WAIT_TOOL_STUCK_TIMEOUT_MS`
ceiling (default 30 min), **not** the aggressive `BUSY_STUCK` reap — so a long
sub-agent no longer gets its parent SIGTERMed mid-run. In `ratlc tui` / `ratlc status`
a wait-tool channel shows `tool Ns` (lone tool) or `N×Ms` for a parallel batch —
**watch `N` shrink to confirm the wait is progressing**, not stuck.

**Disabling sub-agents.** Set `RATLC_SUBAGENT_SUPPORT=0` (or `ratlc subagent off`
/ the `[g]` key in the TUI at runtime) to make the proxy reject native subagent
frames: the Cursor model gets a "sub-agents disabled — do the work inline" error
and completes the task in the main agent, so no Task is launched and no channel is
left waiting. `ratlc subagent status` shows the current state.

**Pinning sub-agents to the main agent's model.** Dropping the sub-agent `model`
makes claude-code *inherit* the parent's model — but that's the client's
behaviour, and in a **multi-group** pool a sub-agent's fresh request can still
route to a different group. Set `RATLC_SUBAGENT_INHERIT_PARENT_MODEL=1` to make
the proxy **guarantee** it: each claude-code session's first (main-agent) model
is remembered, and any sub-agent turn in that session — identified natively by
the shared `x-claude-code-session-id` + a distinct `convKey` + carrying tools —
is re-routed to that model/group *before* the pool routes. So multi-agent always
runs on your strong model instead of Cursor's cheap Composer default, even across
groups. Tool-less background calls (title/topic/quota) are left untouched. Default
off; the `subagent-model-pin: X → Y` log line shows each override.

## Diagnosing stuck channels (no log grep)

`ratlc status` / `ratlc tui` are meant to answer "what is this channel doing?" at a
glance. The **SILENT** column on a `wait-tool` channel tells you what it's blocked on:

| SILENT cell | meaning |
|---|---|
| `Bash 12s` / `Read 8s` | one client tool outstanding, named (fast tools — should clear quickly) |
| `Task 280s` | blocked on a sub-agent (Task) for 280s |
| `1/3 120s` | a 3-tool parallel batch, 1 result back, 2 pending — **watch the count climb = progressing** |
| `0/5 1650s!` | 5 pending, **nothing returned** for 27 min — almost always an abandoned client (red `!` = near the reap ceiling) |

The **ERROR** column is dual-use: on a dead channel it shows *why* it died
(`reap:wait-tool@361s`, `reap:busy@250s`, `worker:quota_exhausted`, red); on a
live `wait-tool` channel with a **multi-tool batch** it names the outstanding
tools (`waiting: Read, Bash`) — because the narrow SILENT cell only has room for
the count (`0/2`). A *lone* pending tool is already named in SILENT (`Bash 12s`).
What it CANNOT show: the tool's args (which file / command) or the client's
execution — a wait-tool channel is idle, blocked on claude-code running the tool,
which is opaque to the proxy. The header shows `⚠tok-dead=N/total` when tokens are
quota-dead (your main capacity limiter).

- **`ratlc inspect <ch>`** — "why is THIS channel stuck right now": one dump joining
  the pool snapshot + `/requests` + token — state/silence/death, the outstanding
  tools **with arg preview** (the actual Bash command / Read path), the request's
  trouble (`retries`, payload MB, symptom, first-byte), and the token. Works on a
  **dead** channel too (falls back to a death tombstone), so you can post-mortem a
  channel that just died.
- **`ratlc deaths [N]`** — recent channel deaths with their reason (`reap:wait-tool@Ns`,
  `reap:client-wait@Ns`, `stall:upstream@Ns`, `worker:quota_exhausted`). Dead rows
  vanish from the live table in milliseconds; this retains the last ~60.
- **`ratlc failures [N]`** — recent not-ok requests (rate-limit, empty turn, stale
  tool_result, errors) straight from `/requests`, so you don't tail `api.log`.
- **`ratlc tui` → press `?`** — full keymap overlay.

Death-reason vocabulary: `reap:wait-tool@Ns` (pool gave up waiting on the client),
`reap:client-wait@Ns` (the bridge stall fired with a client tool_result still
outstanding — **not** a Cursor fault, despite the raw "Upstream stalled" string),
`stall:upstream@Ns` (a real Cursor stall — silent with no tool outstanding),
`worker:quota_exhausted` / `worker:auth_error` (the Cursor account is broken).

**Most common stuck pattern:** a channel parked in `wait-tool` for minutes with
`0/N` and nothing returned = the **claude-code client abandoned the conversation
mid-tool-batch** (closed / Ctrl-C'd after the model emitted tool calls). The proxy
correctly holds it until `RATLC_WAIT_TOOL_STUCK_TIMEOUT_MS` (or the busy clock when
sub-agents are off). Clear it now with `ratlc restart ch-N` (or the TUI `k` key).

## Testing toolchain

Three scripts in this directory verify the proxy's behavior end-to-end:

| script | what it checks | needs Cursor backend? |
|---|---|---|
| `test_niah.sh` | model can retrieve a needle injected at arbitrary depth in a multi-turn conversation; effective context ceiling | yes |
| `test_render.sh` | `renderFullContext()` preserves every message + content-block faithfully; isolates rendering from model behavior | no (proxy-internal) |
| `niah-test.mjs` | older Node-side NIAH harness, structured size × depth matrix | yes |

See `NIAH_RESULTS.md` for measured effective context windows per Cursor
model variant (4-6 [1m] ~900K tokens, 4-7 plain ~600K).

Recommended verification flow after any proxy change:

```bash
./scaffolding/pool/launch.sh up                       # bring pool up
./scaffolding/pool/test_niah.sh                       # smoke: depth=10, ~1s
DEPTH=300 ./scaffolding/pool/test_niah.sh             # mid: ~64KB body
DEPTH=2000 ./scaffolding/pool/test_niah.sh            # heavy: ~430KB body
```

All three should PASS within seconds. If render correctness is in question:

```bash
# Toggle the debug endpoint on in launch.yaml's optional toggles section:
#   POOL_REINJECT_THINKING_DEBUG: 1
./scaffolding/pool/launch.sh up
./scaffolding/pool/test_render.sh
```

## Configuration (env vars)

All settings are env vars on the pool-manager (it forwards everything to
the children + api-server).

| Var | Values | Default | What it controls |
|---|---|---|---|
| `POOL_BRIDGE_PROTOCOL` | `h1` \| `h2` | `h2` | HTTP version of the Cursor bridge. **`h1` strongly recommended** — bypasses the per-account hard rate limit on `/Run` that bites HTTP/2 under concurrency. See `H1_RESULTS.md`. |
| `POOL_TOOL_MODE` | `translate` \| `contract` | `contract` | `translate` (recommended for claude-code): bridge auto-injects Cursor's native tools and we translate them to Anthropic names. `contract`: pool's tool list is locked to the first request's `tools` field. |
| `POOL_CONTEXT_MODE` | `full` \| `last` | `last` | How multi-turn conversations are forwarded. **`full` strongly recommended for claude-code.** See [§ Context modes](#context-modes-fullvslast) below. |
| `POOL_CONCURRENT_OPENS` | `1`–`5` | `1` | How many channels open in parallel. `1` is safe but slow; `5` is faster but more rate-limit pressure. **At `>=5`, H2 trips the per-account rate limit**; H1 is fine. |
| `POOL_SIZE` | integer | `1` (cli `up N` overrides) | Target channel count. `./scaffolding/pool/ratlc up N` is the easy way. |
| `POOL_MODEL` | model id, or CSV | `claude-opus-4-7-thinking-max-fast` | Which Cursor model to drive (the **default group**). Accepts two forms: `opus` (single model, sized by `POOL_SIZE`) or CSV `opus,haiku:3` / `opus:5,haiku:3,composer-2-fast:1`. With CSV, the **first entry is the default group**; subsequent entries become additional groups (equivalent to `POOL_GROUPS`). Each entry is `model` (uses `POOL_SIZE`) or `model:size`. |
| `POOL_GROUPS` | `modelA:N,modelB:M,...` | unset | Extra **named groups** at boot. Each `model:N` declares a group of N channels pinned to that Cursor model. Channels are partitioned across groups; per-request `body.model` picks which group serves. Groups can also be added/removed at runtime via `ratlc add-group`/`remove-group`. **Equivalent** to putting the same entries in CSV-form `POOL_MODEL` — the two are merged identically. |
| `POOL_GROUP_WAIT_MS` | milliseconds | `5000` | When a request names a known group whose channels are all opening/busy, wait this long for one to surface before falling back to the default group. Set to `0` for immediate fallback. |
| `POOL_REINJECT_THINKING` | `0` \| `1` | `0` | Captures the model's `thinking_delta` per `convKey`; on the next turn for the same conversation, prepends `<thinking>…</thinking>` text into the outbound prompt. Pool-side symmetry with `server.js`'s `CURSOR_REINJECT_THINKING`. See [§ Thinking continuity](#thinking-continuity) below. |
| `POOL_PROXY_THINKING_BLOCKS` | `0` \| `1` | code `0`, **shipped `launch.yaml` `1`** | Streams Cursor's real current-turn `thinking_delta` to Claude Code as proxy-local `thinking` blocks (`proxy-local-thinking-v1.*` signatures) → live reasoning UI, **visual parity with Cursor**. Not Anthropic-signed (such sessions can't be resumed against `api.anthropic.com` directly) and separate from prompt reinjection. Coupling: once reasoning is visible, a post-thinking hang surfaces as a clean error + reap, not a silent replay. Interleaved thinking/text and a per-turn byte cap (`POOL_PROXY_THINKING_MAX_BYTES`) are handled. E2E: `proxy-thinking-forward-test.mjs`. |
| `POOL_PROXY_THINKING_MAX_BYTES` | int | `262144` | Cap on reasoning forwarded to the client per turn when `POOL_PROXY_THINKING_BLOCKS=1` (further thinking is still captured upstream). `<=0` → unlimited. |
| `POOL_REINJECT_THINKING_MAX_BYTES_PER_TURN` | int | `4096` | Cap on captured bytes per assistant turn (truncates further deltas in the same turn). Matches server.js's default. |
| `POOL_REINJECT_THINKING_MAX_TURNS` | int | `5` | Number of past assistant turns kept per `convKey`; FIFO-evicts older. |
| `POOL_REINJECT_THINKING_DEBUG` | `1` | unset | Exposes `/v1/_debug/thinking_buffer` and `/v1/_debug/render` for buffer inspection. Off in normal operation. |
| `CURSOR_AGENT_DEBUG` | `1` | unset | Per-line wire debug from cursor-agent (verbose) |
| `CURSOR_LOG_SERVER_MSG` | `1` | unset | Log every `AgentServerMessage` case received from Cursor |
| `CURSOR_LOG_NATIVE_EXEC` | `1` | unset | Log every native exec passthrough event (incl. `subagent passthrough type:X→Y model:X→Y`) |
| `RATLC_SUBAGENT_TYPE_MAP` | `cursorType=clientAgent,...` | unset | Map specific Cursor sub-agent types to your installed claude-code agents (e.g. `explore=Explore,plan=Plan`). Unmapped types fall to the default. See [§ Sub-agents](#sub-agents-task--multi-agent-teams). |
| `RATLC_SUBAGENT_TYPE_DEFAULT` | agent name | `general-purpose` | Fallback `subagent_type` for any Cursor type not in the map. Must be an agent that exists in the target claude-code. |
| `RATLC_SUBAGENT_MODEL_KEYWORDS` | CSV | `sonnet,opus,haiku` | The claude-code `Task` `model` enum to keep as-is (anything else is dropped → inherit parent). Extend only to track future client enum additions. |
| `RATLC_SUBAGENT_FORWARD_MODEL` | `0` | unset | `0` forces the sub-agent `model` to always be dropped (inherit parent), even for keyword matches. |
| `RATLC_SUBAGENT_FORWARD_RESUME` | `1` | unset | `1` forwards Cursor's `resume`/agent-id to the `Task` call. Off by default (not in every client's Task schema). |
| `RATLC_SUBAGENT_INHERIT_PARENT_MODEL` | `1` | unset | Pin every Task sub-agent to the **main agent's** model/group (keyed on claude-code session id), so multi-agent stays on your strong model instead of routing to a cheaper/other group. See [§ Sub-agents](#sub-agents-task--multi-agent-teams). |
| `RATLC_SUBAGENT_SUPPORT` | `0` | `1` (on) | Master switch for sub-agent support. `0` → the proxy **rejects** native subagent frames (the Cursor model does the work inline; no Task reaches the client). Toggle at runtime with `ratlc subagent on/off` or the `[g]` key in `ratlc tui` — no restart needed. `ratlc subagent status` / the TUI header show the current state. |
| `RATLC_WAIT_TOOL_STUCK_TIMEOUT_MS` | ms (`_FAST`/`_SLOW` too) | `1800000` | Reap ceiling for a channel in **wait-tool** (blocked on the client returning tool_results, e.g. a slow sub-agent). Separate from `BUSY_STUCK` because there's no stuck upstream to detect — a long sub-agent is normal. `0` = never reap while waiting on the client (client-disconnect still frees it). |
| `LOG_REQUEST_TOOLS` | `1` | unset | api-server logs incoming tool list per request |
| `LOG_REQUEST_BODY` | n/a | n/a | Body summary (last-message role + content shape) is always on. |
| `CURSOR_LOG_INTERACTION` | `1` | unset | Log every `interactionQuery` decision (WebSearch approve/reject) and backend `webSearchToolCall` byte counts |

### Retry / watchdog tuning

| Var | Values | Default | What it controls |
|---|---|---|---|
| `RATLC_RETRY_MODE` | `constant` \| `aimd` | `constant` | How a bridge-worker paces channel-open retries. `constant` fires every `RATLC_CONSTANT_INTERVAL_MS` regardless of response — fastest in practice. `aimd` is TCP-style adaptive (multiplicative back-off on rate-limit, additive ramp on success) — useful for sustained-pressure scenarios. |
| `RATLC_CONSTANT_INTERVAL_MS` | int | `500` | Fixed retry interval in `constant` mode |
| `RATLC_WAIT_FLOOR_MS` | int | `300` | AIMD floor (most aggressive wait) |
| `RATLC_WAIT_CEILING_MS` | int | `30000` | AIMD ceiling (slowest wait) |
| `RATLC_WAIT_DECREASE_MS` | int | `50` | AIMD linear decrement per non-rate-limit |
| `RATLC_WAIT_INCREASE_FACTOR` | float | `2.0` | AIMD multiplier per rate-limit hit |
| `RATLC_INITIAL_WAIT_MS` | int | `1000` | AIMD starting wait |
| `RATLC_OPEN_RETRY_MAX` | int | `500` | Hard cap on open attempts per worker before process recycle |
| `POOL_TOOL_USE_WATCHDOG_MS` | int | `1000` | Tool-use turn finalize watchdog. Re-armed on every `text_delta` / `thinking_delta` / `tool_use`, so it measures "model went silent" rather than "no more tool_uses." See [STEP_COMPLETED_INVESTIGATION.md](./STEP_COMPLETED_INVESTIGATION.md) and [WATCHDOG_REARM_REVIEW.md](./WATCHDOG_REARM_REVIEW.md) for why this design. |
| `POOL_TOOL_USE_DEBOUNCE_MS` | int | unset | Legacy alias for `POOL_TOOL_USE_WATCHDOG_MS`; kept for backwards compat |
| `RATLC_BUSY_STUCK_TIMEOUT_MS` | int | `240000` | Pool-manager scans for channels stuck in `busy` state with no activity longer than this and SIGTERMs the worker. Replacement spawns automatically. The ultimate safety net beyond the tool-use watchdog. |

### Multi-token (per-account) configuration

| Var | Default | What it controls |
|---|---|---|
| `RATLC_TOKEN_DEATH_THRESHOLD` | `3` | Consecutive `other_error` strikes (with no token validation) before pool-manager marks a token dead and skips it in round-robin. `auth_error` and `quota_exhausted` always mark dead on first strike regardless of this. |
| `CURSOR_CLIENT_OS` | auto-detected | Forces the `x-cursor-client-os` header. Auto-derives `darwin` when `token.macMachineId` is set and host isn't darwin (Mac-minted token spoofing). Override here to force a value. |
| `CURSOR_CLIENT_OS_VERSION` | `os.release()` or `23.5.0` for Mac-spoof | Forces `x-cursor-client-os-version` header. |
| `CURSOR_CLIENT_ARCH` | `process.arch` or `arm64` for Mac-spoof | Forces `x-cursor-client-arch` header. |

## Multi-group model pools

A single pool can host multiple **named groups**, each pinned to a
different Cursor model. The `body.model` field of `/v1/messages`
routes the request to the matching group; unknown / missing models
fall back to the default group. Useful when you want one stack to
serve several models at once (Opus for hard work, Sonnet for cheap
throughput, etc.) without running multiple `ratlc up` invocations on
multiple ports.

### Bring up multiple groups at boot

```bash
POOL_MODEL=claude-opus-4-7-thinking-max-fast POOL_SIZE=10 \
POOL_GROUPS="claude-4.6-sonnet-medium-fast:3" \
POOL_BRIDGE_PROTOCOL=h1 POOL_TOOL_MODE=translate POOL_CONTEXT_MODE=full \
POOL_CONCURRENT_OPENS=5 \
  ./scaffolding/pool/ratlc up
```

This brings up 13 channels total: 10 on the default Opus group + 3 on
the Sonnet group. `POOL_CONCURRENT_OPENS` is a **global** open budget
shared across all groups, so opens are interleaved at the
rate-limit-safe pace, not per-group.

Equivalently, you can declare every group inline via the CSV form of
`POOL_MODEL` — handy when you want a single env line:

```bash
POOL_MODEL="claude-opus-4-7-thinking-max-fast:10,claude-4.6-sonnet-medium-fast:3" \
POOL_BRIDGE_PROTOCOL=h1 POOL_TOOL_MODE=translate POOL_CONTEXT_MODE=full \
POOL_CONCURRENT_OPENS=5 \
  ./scaffolding/pool/ratlc up
```

The first CSV entry is the default group; subsequent entries become
additional groups. Entries with no `:N` suffix fall back to `POOL_SIZE`.
CSV-form `POOL_MODEL` and `POOL_GROUPS` are merged identically — pick
whichever reads better for your config.

### Mutate groups at runtime

```bash
ratlc groups                                    # per-group status table
ratlc add-group claude-4.6-sonnet-medium-fast 3 # register + spawn
ratlc remove-group claude-4.6-sonnet-medium-fast # drain + evict
ratlc ramp +2 --group=claude-4.6-sonnet-medium-fast  # grow named group
ratlc ramp -1 --group=claude-4.6-sonnet-medium-fast  # shrink named group
```

`remove-group` is **drain semantics**: in-flight requests finish
normally, new requests to that group fall back to default with
`x-ratlc-fallback-reason: group-draining`, and channels are killed as
they go idle. The default group cannot be removed.

### Route per-request via `body.model`

```bash
curl -s http://127.0.0.1:4242/v1/messages -i \
  -H 'content-type: application/json' \
  -d '{"model":"claude-4.6-sonnet-medium-fast","max_tokens":256,
       "messages":[{"role":"user","content":"hi"}]}'

HTTP/1.1 200 OK
x-ratlc-routed-to: claude-4.6-sonnet-medium-fast   # the actual serving group
x-ratlc-channel: ch-7                              # which channel ran it
x-ratlc-fallback: 0
```

Unknown models silently fall back to default (200 OK with the headers
flipped):

```
x-ratlc-routed-to: claude-opus-4-7-thinking-max-fast
x-ratlc-channel: ch-2
x-ratlc-fallback: 1
x-ratlc-fallback-reason: unknown-model
```

Other fallback reasons:
- `group-draining` — the target group is being removed
- `group-no-ready` — the target group has zero ready channels even
  after waiting `POOL_GROUP_WAIT_MS` (default 5s) for one to surface

### claude-code with a specific group

```bash
ratlc claude --model claude-4.6-sonnet-medium-fast
# ANTHROPIC_MODEL=claude-4.6-sonnet-medium-fast forwarded; refuses if
# no matching group exists. Run `ratlc add-group MODEL N` first.
```

### Per-group observability

- `ratlc status` adds a `GROUP` column on the per-channel table and a
  per-group summary above the table.
- `ratlc tui` (both the `1:split` and `4:status` views): when more than
  one group is registered, the header line shows `groups=N (default=X)`
  in place of the single-model line, a per-group summary table sits
  above the channel table, and the channel table is **sectioned by
  group** with one `── <model> ──` separator per group. Single-group
  setups render unchanged.
- `/health` returns `pool.groups[]` (per-group ready/busy/opening/dead
  counts + targets).
- `/metrics` emits `ratlc_group_channels{group="...",state="..."}`
  series plus a per-channel `group=` label on existing counters.

### Constraints

- The default group is always `POOL_MODEL` and cannot be removed.
- All groups share `token.json` (channels distribute across tokens via
  round-robin — see [§ Multi-account token rotation](#multi-account-token-rotation)).
- Channel ids stay globally monotonic (`ch-0`, `ch-1`, ...) regardless
  of group ownership. `ratlc restart ch-N` works the same.
- `POOL_TOOL_MODE` / `POOL_CONTEXT_MODE` / `POOL_REINJECT_THINKING`
  apply pool-wide (no per-group overrides in v1).

## Multi-account token rotation

`token.json` holds an array of accounts. Each new channel spawn gets a
token via round-robin (`channelSeq % tokenCount`). With N accounts, soft-
rate-limit pressure that Cursor applies per-account is distributed
across all N — typically 2-3× sustained throughput vs a single account.

### token.json shape

```jsonc
{
  "tokens": [
    {
      "name": "account-1",              // human label; appears in logs
      "accessToken": "...",             // Cursor bearer token
      "machineId": "...",               // 64-hex from Cursor IDE install
      "macMachineId": "..."             // 128-hex; presence triggers Mac-OS spoofing on non-Mac hosts
    },
    {
      "name": "account-2",
      "accessToken": "...",
      "machineId": "...",
      "macMachineId": "..."
    }
  ]
}
```

Single-token deployments work unchanged (every channel gets index 0).
With N > 1, the pool boot log says `token rotation: N token(s) loaded —
[name1, name2, ...]` and each spawn log line includes
`token[idx]=name`.

### Token health detection

Bad tokens are detected automatically via the response-kind classifier
in `bridge-worker.mjs onError`. Three definite-fatal kinds:

| Kind | Pattern | When marked dead |
|---|---|---|
| `auth_error` | `ERROR_NOT_LOGGED_IN`, `unauthenticated` | First strike |
| `quota_exhausted` | `ERROR_RATE_LIMITED_CHANGEABLE`, `API usage limit reached` | First strike |
| `other_error` | Anything we don't classify | After `RATLC_TOKEN_DEATH_THRESHOLD` strikes (default 3), only if the token has never reached a post-auth response |

A token is marked **validated** the first time it gets any of: `opened`,
`unpaid`, `rate_limit_soft`, `rate_limit_hard`, `no_yield` — these all
prove the token authenticated past Cursor's gate. Validated tokens are
exempt from `other_error` strikes (treated as transients).

Dead tokens are skipped in `nextTokenIndex` round-robin. They never get
new channels until pool restart. To revive: fix the underlying account
(re-login, refill quota), `ratlc down`, `ratlc up`.

### Observing token health

```bash
ratlc tui
# new TOK column on the channel table shows token index per channel
# new "token health" panel below the group table (only with N > 1 tokens)
# shows IDX / NAME / VALIDATED / DEAD / OTHERERR / LAST_ERROR per token

ratlc metrics
# JSON snapshot now includes `pool.tokens[]` with full per-token state
```

The pool log emits clear events for every transition:
- `token rotation: N token(s) loaded — [name1, name2, ...]`
- `spawned ch-K (..., token[idx]=name)`
- `token[idx]=name validated (reached Cursor past auth)`
- `⚠ TOKEN DEAD (auth_error): token[idx]=name marked dead on first strike. ...`
- `⚠ TOKEN DEAD: token[idx]=name marked dead after N consecutive other_error failures ...`

### Constraints

- All tokens share the same set of model groups. Per-token model
  affinity isn't in v1.
- Each token's machine fingerprint (machineId / macMachineId) is used
  individually for header generation — the per-token
  `resolveClientFingerprint(token)` correctly handles a mix of Mac-
  minted and Linux-minted tokens.
- Adding a token requires pool restart to pick up. Hot-reload of
  `token.json` is out of scope for v1.

## Context modes (`full` vs `last`)

This is the most important setting for claude-code multi-turn coherence.
Pool channels are picked **least-recently-used** for each new POST, so a
multi-turn conversation can hop between channels — which means model
context handling depends on this flag.

### `last` (default, backward-compatible)

- Only the **last user message text** is forwarded to the bridge.
- Channels accumulate per-conversation state **server-side** inside
  Cursor's model context.
- Multi-turn coherence requires every turn of one conversation to land
  on the **same channel** — but LRU rotation breaks this.
- ✅ Cheap (no token re-send overhead).
- ❌ Cross-channel drift on multi-turn: turn 2 lands on a different
  channel that has no memory of turn 1.

### `full` (recommended for claude-code)

- The **entire `messages[]` history** is rendered into one self-contained
  prompt and fed via `bajie_yield` on every POST.
- Channels are stateless carriers — each `bajie_yield` result is a
  complete request.
- ✅ Multi-turn coherence preserved across channel rotation.
- ❌ Quadratic token cost as conversations grow (each turn re-sends the
  full prior history). Fine for typical claude-code sessions (10-30
  turns); gets expensive at 100+.

## Thinking continuity

Enable with `POOL_REINJECT_THINKING=1`. Captures Cursor's `thinking_delta`
events into a per-`convKey` server-side buffer, then renders them as
`<thinking>…</thinking>` text into the next turn's outbound prompt for
that same conversation. Pool-side counterpart of `server.js`'s
`CURSOR_REINJECT_THINKING`; same mechanism, different process boundary.

### Why this exists

The model's *own* prior reasoning ordinarily lives in the server-side
Cursor model context for the open channel. Two situations break that:

- **`full` mode + LRU rotation** — turn 2 of a conversation may land on a
  different channel from turn 1. The new channel has no memory of what
  turn 1's model thought.
- **`last` mode + same-channel-different-conversation** — across truly
  unrelated conversations served by the same channel, prior thinking is
  noise rather than help (this case already works without reinjection;
  no change).

Either path, the captured-then-rendered `<thinking>…</thinking>` is the
only way to give the model a useful reasoning carry-over within the
existing wire constraints (Cursor's transport strips signed extended-
thinking blocks regardless of source — see `DEVLOG.md` "Proxy-side
thinking re-injection" for that constraint).

`POOL_PROXY_THINKING_BLOCKS=1` (enabled in the shipped `launch.yaml`) is
separate. It does not feed thinking back to Cursor; it only wraps the current
turn's real upstream `thinking_delta` in Claude-compatible `thinking` SSE blocks
so the client can display the reasoning live (visual parity with Cursor IDE).
The generated signatures are explicitly proxy-local and are stripped from
full-context prompt rendering (so combining it with `POOL_REINJECT_THINKING`
does not double-feed the model). It also makes thinking count as channel
liveness in the watchdog: an actively-reasoning channel is never reaped, but a
post-thinking silence is caught — and because the reasoning is already visible,
that case surfaces a clean error + channel reap rather than a transparent replay
(which would duplicate the shown thinking). See `proxy-thinking-forward-test.mjs`.

### How conversations are identified

This depends on the convKey identity fix that landed earlier in this
project: `extractClientSessionId(req)` pulls a stable per-conversation
UUID from either the `x-claude-code-session-id` header or
`body.metadata.user_id`'s `session_id` field. `deriveConversationKey`
hashes only `(modelId, sessionId)` when that UUID is present, producing
a `conv-v2:` key with zero collision risk across distinct conversations
even when prompts and tools are identical. Non-claude-code callers
fall back to the legacy circumstantial hash.

### Trade-off

- ✅ Survives both LRU rotation in `full` mode and channel reuse across
  conversations in `last` mode.
- ✅ Bounded: `MAX_BYTES_PER_TURN × MAX_TURNS` = 4 KB × 5 = 20 KB cap on
  injected thinking per `convKey`.
- ✅ Default off — opt-in symmetry with `CURSOR_REINJECT_THINKING`.
- ❌ Text-form continuity, NOT native signed extended-thinking. The model
  sees prior reasoning as inline `<thinking>` tags, treats it as
  reference context — does not run it through extended-thinking re-
  validation logic on Cursor's side. Same approximation `server.js`
  ships, same caveat.
- ❌ Some extra prompt bytes per continuation; bounded by the env caps.

### Verifying it works

```bash
node scaffolding/pool/thinking-buffer-test.mjs      # 34 unit assertions
node scaffolding/pool/reinject-thinking-test.mjs    # 2-turn E2E
```

The E2E test asserts on the **outbound prompt to the bridge** containing
a `<thinking>` block on turn 2 — model-output coherence is a separate
concern verified by `multi-turn-test.mjs`.

If `POOL_REINJECT_THINKING_DEBUG=1`, the api-server exposes:

| Endpoint | What it returns |
|---|---|
| `GET /v1/_debug/thinking_buffer` | Live buffer contents per convKey |
| `POST /v1/_debug/render` | Render a fake POST body through the same pipeline to inspect what would be sent |

### Verifying it works

```bash
# Single-turn round-trip (both modes)
node scaffolding/pool/tool-roundtrip-test.mjs

# Multi-turn coherence — "I have three apples" / "How many do I have?"
node scaffolding/pool/multi-turn-test.mjs

# Parallel tools — model calls 2 Bash tools at once
node scaffolding/pool/parallel-tools-test.mjs
```

All three should PASS in the recommended config (`h1 + translate + full`).

## What works (current state, 2026-05-15)

- ✅ **HTTP/1.1 transport** via `BidiAppend` + `RunSSE` pair. Bypasses
  the per-account rate-limit ceiling that capped H2 at ~2-3 channels.
- ✅ **Stateless full-context forwarding** (`POOL_CONTEXT_MODE=full`).
- ✅ **Parallel tool calls** — model can fire N tool_uses in one
  response; all N round-trip back correctly.
- ✅ **Anthropic SSE wire compliance** — claude-code parses our
  responses correctly (cache_creation_input_tokens et al., proper
  model id, empty `input_json_delta` preamble).
- ✅ **`ExecClientControlMessage(streamClose)`** sent after every tool
  result — matches Cursor IDE's bundle pattern.
- ✅ **Thinking continuity** (`POOL_REINJECT_THINKING=1`, opt-in) —
  captured per `convKey` (using `x-claude-code-session-id` for ironclad
  attribution), reinjected as `<thinking>…</thinking>` text on
  subsequent turns. Mirrors `server.js`'s `CURSOR_REINJECT_THINKING`.
- ✅ **Multi-group model pools** — one pool, multiple groups, one
  group per model. Per-request routing via `body.model`. Runtime
  `add-group`/`remove-group`/`ramp --group`. `x-ratlc-*` response
  headers expose routing decisions + fallback reasons. See
  [§ Multi-group model pools](#multi-group-model-pools).
- ✅ **CSV `POOL_MODEL`** — declare the default group AND additional
  groups in one env var: `POOL_MODEL="opus:5,haiku:3,composer-2-fast:1"`.
  Equivalent to `POOL_MODEL=opus POOL_SIZE=5 POOL_GROUPS="haiku:3,composer-2-fast:1"`.
- ✅ **Claude-code tool coverage** in `translate` mode — the pool now
  registers `Edit`, `Glob`, `NotebookEdit`, `TodoWrite` as MCP tools
  under their **Anthropic-native names** (matching what claude-code
  emits). The model sees both Cursor's native surface (`StrReplace`,
  Cursor-`Glob`, ...) and the Anthropic-named variants, so a
  claude-code session that calls `Edit` lands on a matching tool
  without falling back to "X unavailable" hallucinations or Bash
  heredocs. Round-trip validated by `tool-coverage-test.mjs` and
  `live-claude-sim-test.mjs` against a real pool.
- ✅ **Multi-account token rotation** — `token.json` accepts an array
  of accounts; pool-manager round-robins token assignment per spawn.
  Spreads soft-rate-limit pressure across N accounts. See
  [§ Multi-account token rotation](#multi-account-token-rotation).
- ✅ **Token health detection** — bad tokens (invalid auth or
  account-quota-exhausted) are flagged on first strike via the
  response-kind classifier; dead tokens are skipped in rotation.
  Validated tokens (those that reached any post-auth response) are
  exempt from `other_error` strikes. TUI shows full per-token state.
- ✅ **Header fingerprint auto-detection** — Mac-minted tokens running
  from a non-Mac host now automatically claim macOS in headers
  (matching the checksum bundle). Was a hard-rate-limit pitfall pre-`d65a0cb`.
- ✅ **Busy-watchdog** — pool-manager kills channels stuck in `busy`
  state with no activity longer than `RATLC_BUSY_STUCK_TIMEOUT_MS`
  (default 240 s). Last-resort safety net beyond the tool-use
  watchdog and HTTP-layer stall detection.
- ✅ **tool_use watchdog re-arms on every model delta** — finalize
  timer measures "model went silent" not "no more tool_uses"; works
  correctly even on `*-thinking-fast` Cursor variants that don't
  emit `step_completed`. See [STEP_COMPLETED_INVESTIGATION.md](./STEP_COMPLETED_INVESTIGATION.md).
- ✅ **WebSearch Write-spoof rejected** — model emitting empty `Write`
  to `agent-tools/<uuid>.txt` (the cheap-confabulation pattern) gets
  a synthetic `tool_error` instead of a silent success. See
  [AGENT_TOOLS_SPOOF_OBSERVATION.md](./AGENT_TOOLS_SPOOF_OBSERVATION.md).
- ✅ **WebFetch passthrough no longer pre-summarizes** — hardcoded
  `"Summarize this content."` prompt replaced with a neutral
  "return content as-is" instruction so models doing structured
  extraction get raw text.
- ✅ **AIMD self-tuning retry** — `RATLC_RETRY_MODE=aimd` available
  for high-throttle scenarios. Default `constant` 500 ms is fastest
  in typical use.
- ✅ **NIAH context measured** — `claude-opus-4-7-max-fast` reliably
  retrieves at all positions up through 600 k tokens; falls off a
  cliff at 650-700 k. See [NIAH_RESULTS.md](./NIAH_RESULTS.md) — the
  test script is committed and re-runnable.

See `H1_RESULTS.md` for the scale-test results (10 channels @ H1: 0
hard rate-limit hits vs 108 on H2). See `TOOL_USE_HANG_FINDINGS.md`
for the diagnosis trail that found the streamClose requirement.

## Bring it up — basics

```bash
# Default (size=1, contract mode, h2) — minimal but slow
./scaffolding/pool/ratlc up

# Recommended for claude-code (size=10, h1, translate, full context)
POOL_BRIDGE_PROTOCOL=h1 POOL_TOOL_MODE=translate POOL_CONTEXT_MODE=full \
  POOL_CONCURRENT_OPENS=5 ./scaffolding/pool/ratlc up 10
```

## Watch it warm up

```bash
./scaffolding/pool/ratlc watch       # status refresh every 2s
./scaffolding/pool/ratlc tui         # split-screen TUI with logs + status
./scaffolding/pool/ratlc status      # one-shot snapshot
```

Channels transition `spawning` → `opening` → `ready`. Many will retry
30-300 times each before clearing the probabilistic `unpaid_invoice`
gate. Expected.

## Use claude-code against it

```bash
./scaffolding/pool/ratlc claude
# auto-waits for ready≥1, then launches claude with the right env vars
```

Or manually:

```bash
ANTHROPIC_BASE_URL=http://127.0.0.1:4242 \
ANTHROPIC_API_KEY=ratlc-pool \
  claude --dangerously-skip-permissions --effort max
```

`./scaffolding/pool/ratlc claude` is the easier path — it adds the env
vars + waits for the pool to be ready before exec'ing claude.

## Scale at runtime

```bash
./scaffolding/pool/ratlc ramp +3        # add 3 channels
./scaffolding/pool/ratlc ramp -2        # remove 2 (idle first)
./scaffolding/pool/ratlc restart ch-0   # respawn one channel
```

## Restart the API server without losing the pool

```bash
pkill -f "node.*api-server.mjs"
LOG_REQUEST_TOOLS=1 nohup node scaffolding/pool/api-server.mjs >> /tmp/ratlc-api.log 2>&1 &
disown
```

The api-server holds no Cursor state. The pool keeps its warm channels.
Useful for picking up new env vars (e.g. switching `POOL_CONTEXT_MODE`)
without re-paying the probabilistic gate.

## Stop everything

```bash
./scaffolding/pool/ratlc down
```

## Logs

| File | What's in it |
|---|---|
| `/tmp/ratlc-pool.log` | Pool manager + worker stdout/stderr (state transitions, BidiAppend results, tool routing, native exec passthrough) |
| `/tmp/ratlc-api.log` | API server (HTTP requests, per-POST body summary, pool socket events) |

Live-tail both: `./scaffolding/pool/ratlc tail`

## Observability surface

| Layer | Logged automatically | Opt-in (env var) |
|---|---|---|
| api-server | POST URL, tool list, body summary (`role + content shape`), pool dispatch (`mode=full\|last`, requestId, bytes) | — |
| pool-manager | Channel state transitions, `route send_tool_result` outcomes, attempt counters | — |
| bridge-worker | IPC arrival (`type, requestId, state, pendingYield/pendingMcp`), BEFORE/AFTER around `bridge.sendToolResult` | — |
| cursor-agent-h1 | `sendToolResult` entry (id, execId, kind, size), BidiAppend OK/FAIL/EXCEPTION | `CURSOR_LOG_SERVER_MSG=1` for receive-side msgCase trace |
| metrics | Prometheus exposition at `:4242/metrics` | — |

## Health behaviors

- **Idle ping every 20 min** — any `ready` channel idle that long gets a
  tiny health-check round-trip. Reset on success; channel killed on
  failure.
- **Heartbeat every 30s** — workers report state to manager.
- **Auto-respawn on exit** — worker exit triggers replacement to maintain
  `currentTargetSize`.
- **Rate-limit handling** — soft "please wait" returns retry with
  exponential backoff (5s → 60s). Hard `ERROR_PRO_USER_RATE_LIMIT`
  doesn't fire on H1 path.

## Status fields

```
CHANNEL   ch-N         logical id, monotonically increasing
STATE     spawning     forked, not yet started open
          opening      running the probabilistic gate retry lottery
          ready        parked in bajie_yield, awaiting request
          busy         serving a request OR holding a tool_use
          dead         fatal error, will be respawned
TOK                    index into token.json's tokens[] for this channel
BUSY                   time in current turn (yellow >180 s, red >240 s)
PID                    OS pid of the worker process
ATTEMPTS               how many retry attempts the lottery has taken
AGE                    time since the channel opened (first lottery win)
IDLE                   time since the last activity
ROUNDS                 successful user-message→yield cycles served
CURRENT                request id currently in flight on this channel
ERROR                  most recent error message (if any)
```

Token-health panel (only shown when more than one token in token.json):

```
IDX                    token index
NAME                   token's `name` field from token.json
VALIDATED              ✓ if the token has reached any post-auth response
DEAD                   YES if pool-manager has marked dead; skipped in rotation
OTHERERR               count of unclassified `other_error` strikes
LAST_ERROR             the actual Cursor error message (colored)
```

## Common diagnostic paths

| Symptom | Where to look |
|---|---|
| claude-code hangs mid-conversation | `/tmp/ratlc-api.log` for "→ tool_use to client" then check `/tmp/ratlc-pool.log` for the matching `sendToolResult` and `BidiAppend OK seqno=…` |
| "API returned an empty or malformed response" | Likely parallel-tool-call bug if the model fires multiple in one turn. We support this now; if it surfaces, check `pendingMcpInfo` map state |
| Channel stuck `opening` forever | Probabilistic gate or hard rate-limit — `stream-summary-h1 code=fail reason="…"` entries reveal which. If reason is `unauthenticated` or `API usage limit reached`, see [§ Token health detection](#token-health-detection) — the token will get marked dead on first strike. |
| Channel stuck `busy` with high `IDLE` | Pool-manager's busy-watchdog will SIGTERM it at `RATLC_BUSY_STUCK_TIMEOUT_MS` (default 240 s). If you're seeing this routinely, check [WATCHDOG_REARM_REVIEW.md](./WATCHDOG_REARM_REVIEW.md) — the tool_use watchdog might be finalizing turns prematurely (re-armed in `3c2f017` to mitigate). |
| Pool slowly shrinks: token count drops, no new spawns | A token has been marked dead (see [§ Multi-account token rotation](#multi-account-token-rotation)). `ratlc tui` → token-health panel shows which, and the `LAST_ERROR` column shows why. Fix the upstream account, then `ratlc down` + `ratlc up`. |
| Model returns "READY" instead of answering a long-context question | You're above the model's effective context window. See [NIAH_RESULTS.md](./NIAH_RESULTS.md) — `claude-opus-4-7-max-fast` tops out around 600 k tokens (Cursor truncates from the tail, leaving only the priming "Reply with READY" instruction). |
| Model writes empty files to `agent-tools/<uuid>.txt` then narrates fake WebSearch results | Known model confabulation pattern. The proxy now rejects these Writes (`3c2f017`); see [AGENT_TOOLS_SPOOF_OBSERVATION.md](./AGENT_TOOLS_SPOOF_OBSERVATION.md) for diagnosis. |
| Linux pool gets 100% rate-limit even on first attempt | Mac-minted `token.json` + Linux host. `d65a0cb` auto-detects this via `token.macMachineId` and claims `darwin` headers. If you've overridden `CURSOR_CLIENT_OS=linux` for some reason, unset it. |

See `TOOL_USE_HANG_FINDINGS.md`, `H1_RESULTS.md`, `FOCUS.md`,
`STEP_COMPLETED_INVESTIGATION.md`, `NIAH_RESULTS.md`, and the
`WEBSEARCH_WEBFETCH_REVIEW.md` / `WATCHDOG_REARM_REVIEW.md` /
`AGENT_TOOLS_SPOOF_OBSERVATION.md` review docs for underlying details.
