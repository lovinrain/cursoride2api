# Needle-in-Haystack — effective context per Cursor model variant

NIAH (needle-in-a-haystack) verification of the proxy's long-context handling.
Each test sends a single `POST /v1/messages` carrying a multi-turn conversation
with a unique random needle injected at a configurable depth, then checks that
the model retrieves the exact needle string.

The wall in every variant is the same: the model returns its priming
acknowledgment string (`READY`, set at channel-open in `bridge-worker.mjs`)
instead of an answer. That happens because Cursor truncates oversize inputs
from the tail, leaving only the priming context at the start; the model
answers THAT instead of the user's actual question.

---

## `claude-4.6-opus-max-thinking-fast[1m]` — effective ~900K tokens

Measured 2026-05-20 via `scaffolding/pool/test_niah.sh` against the live pool.

| msgs | body | ~tokens | needle position | latency | result |
|---|---|---|---|---|---|
| 33 | 3.6 KB | 1K | 60% | 1s | PASS |
| 213 | 22 KB | 6K | 94% | 1s | PASS |
| 213 | 22 KB | 6K | **16%** (early) | 2s | PASS |
| 1013 | ~100 KB | 30K | **2%** (early) | 9s | PASS |
| 4013 | 426 KB | 122K | 99% (late) | 5s | PASS |
| 4013 | 426 KB | 122K | **0.5%** (early) | 3s | PASS |
| 10003 | 1.06 MB | 290K | 50% | 18s | PASS |
| 20003 | 2.13 MB | 580K | 50% | 10s | PASS |
| 24003 | 2.55 MB | 720K | 50% | 25s | PASS |
| 30003 | 3.20 MB | 900K | 50% | 15s | **PASS (max working)** |
| 34003 | 3.60 MB | 1.03M | 50% | 29s | **FAIL — model returned "READY"** |
| 36003 | 3.80 MB | 1.09M | 50% | 65s | FAIL — "READY" |
| 40003 | 4.27 MB | 1.16M | 50% | 60s | FAIL — "READY" |

**Effective ceiling: ~900K tokens.** Above 1.03M the model falls back to
returning the priming string `READY`. Needle position within the working
window is largely irrelevant — retrieved correctly at positions 0.5%, 2%,
16%, 50%, 60%, 80%, 94%, 99%, 99.7%. No "lost-in-the-middle" failure
observed below the wall.

`CLAUDE_CODE_AUTO_COMPACT_WINDOW=600000` (typical user setup) keeps rendered
context at ~67% of the ceiling — comfortable headroom; auto-compaction
kicks in well before the failure mode.

The 4-6 variant lives up to most of its advertised 1M context, unlike the
4-7 sibling below.

---

## `claude-opus-4-7-max-fast` — effective ~600K tokens

Measured 2026-05-15 via `scaffolding/pool/niah-test.mjs` against the same
pool. 24 calls across 8 context sizes × 3 needle-depth positions
(10% / 50% / 90% through the haystack).

| target tokens | actual input_tokens | depth 10% | depth 50% | depth 90% | latency (s) |
|---:|---:|:-:|:-:|:-:|:-:|
| 10,000 | 10,094 | ✓ | ✓ | ✓ | 6.7 / 7.1 / 6.8 |
| 50,000 | 50,094 | ✓ | ✓ | ✓ | 8.2 / 8.4 / 8.7 |
| 200,000 | 200,094 | ✓ | ✓ | ✓ | 16.8 / 20.5 / 16.6 |
| 500,000 | 500,094 | ✓ | ✓ | ✓ | 34.5 / 27.3 / 40.9 |
| **600,000** | **600,094** | **✓** | **✓** | **✓** | **34.5 / 30.2 / 30.6** |
| 650,000 | 650,094 | ✓ | ✓ | **✗** | 31.8 / 32.2 / 30.9 |
| 700,000 | 700,094 | ✗ | ✗ | ✗ | 138.8 / 117.2 / 36.5 |
| 950,000 | 950,094 | ✗ | ✗ | ✗ | 22.1 / 91.5 / 19.5 |

21/24 hits overall. The wall is **between 650k and 700k**:

- ≤ 600k: 100% retrieval at every position tested.
- 650k: edge failure — needle at depth 90% (near the end of the haystack)
  gets cut off, the other two positions still succeed.
- ≥ 700k: complete failure at every position.

**Practical budget: ~598K tokens** including priming and tool overhead
(~94 tokens in this pool's configuration). Far below the model identifier's
implied 1M.

---

## Common failure-mode analysis

Above each variant's cliff, every miss returns the literal string `READY`.
That's what the channel is primed to emit at open time (see
`bridge-worker.mjs` `buildPrimingPrompt`):

```js
lines.push('Reply with exactly "READY" to acknowledge, then call bajie_yield.');
```

The priming context lives at the START of the conversation; the user's
haystack + question is what gets truncated. The model is literally
answering the priming prompt because that's what's left after Cursor
truncates the input.

Latency corroborates: at-or-below the ceiling, response time scales
roughly linearly with input tokens (~55 µs/token marginal for the 4-7,
slightly higher for 4-6 since [1m] context routing has more overhead).
Above the ceiling, latency is erratic (19-139 s) consistent with Cursor
handling oversize inputs through different code paths depending on
internal state.

Position-sensitivity within the failing band also matches the truncate-
from-tail hypothesis: in the 4-7 variant, 650k @ depth 90% fails first
because a needle near the END of the haystack is exactly what gets
chopped if Cursor truncates from the tail to fit a fixed window.

---

## Implications for callers

- **Treat ~600K as the safe budget for any `*-max-fast` variant** unless
  you've specifically verified higher headroom on the variant you use.
  The 4-6 `[1m]` variant comfortably handles ~900K, the 4-7 plain variant
  caps at ~600K.
- **The model identifier suffix `[1m]` reflects the underlying Claude
  capability, not what Cursor's `max-fast` family actually delivers.**
  Each Cursor variant has its own effective window — run NIAH against
  the variant you'll use in production.
- **Silent failure is the danger:** the model doesn't error out, it
  just answers a different (the priming) question. Callers above the
  cliff would see "model returned a one-word response that doesn't
  address my prompt." Worth a length-guard at the api-server if this
  becomes a problem in practice.
- **`CLAUDE_CODE_AUTO_COMPACT_WINDOW` should be set BELOW the ceiling**
  of your model. For 4-6 `[1m]`, 600K is safe (67% of ceiling). For
  4-7 plain, 400K is safer (67% of 600K ceiling).
- **Needle-position effects are minimal below the ceiling.** No need to
  worry about "lost in the middle" within the working window; the model
  attends to deep history correctly.

---

## Test harnesses

Two scripts live in this directory:

### `test_niah.sh` (shell, simple)

Bash script that builds a multi-turn body with a configurable depth + tail,
POSTs to `/v1/messages`, parses the SSE response, and exits 0 (PASS) or 1
(FAIL). Output formatted for at-a-glance human verification. Recommended
for ad-hoc spot checks and regression tests.

```bash
./test_niah.sh                                       # default DEPTH=10, TAIL=5
DEPTH=100 ./test_niah.sh                             # needle at 99% with 100 turns before
DEPTH=10 TAIL=2000 ./test_niah.sh                    # needle at 0.5%, 2000 turns after
DEPTH=5000 TAIL=5000 ./test_niah.sh                  # needle at 50%, ~580K tokens
MODEL='claude-opus-4-7-thinking-max-fast' ./test_niah.sh
BASE_URL=http://other-host:4242 ./test_niah.sh
```

### `niah-test.mjs` (Node, more sophisticated)

Node script with structured size/depth matrix. Tests multiple sizes ×
multiple depth positions in one run, prints a results table at the end.
Used for the 4-7 measurement above.

```bash
node niah-test.mjs                                                  # default 4 sizes × 3 depths
NIAH_SIZES=600000,650000,700000 node niah-test.mjs
NIAH_DEPTHS=0.05,0.25,0.5,0.75,0.95 node niah-test.mjs
NIAH_MODEL=claude-opus-4-7-thinking-max-fast node niah-test.mjs
```

### `test_render.sh` (debug-side verification)

Hits the `POST /v1/_debug/render` endpoint (gated on
`POOL_REINJECT_THINKING_DEBUG=1`) to check the proxy's render pipeline
without invoking Cursor. PASS = every sentinel sentence from each turn
appears in the rendered output. Use this to isolate "proxy is dropping
turns" from "model can't attend." Instant.

```bash
# Enable the endpoint via launch.yaml's commented toggle then:
./launch.sh up
./test_render.sh
```

## Pre-requisites

For any of these tests:

1. Pool must be up with at least one channel ready in the target model
   group. Use `./launch.sh up`, wait until `./launch.sh status` shows
   `ready >= 1`.
2. The `RATLC_PASSTHROUGH_NATIVE=1` env (set by default in `launch.yaml`)
   must be on for native-tool roundtrip tests; not required for NIAH or
   render tests.
3. `CURSOR_CLIENT_OS=darwin` must be set on Linux hosts or every channel
   open will be rate-limited (see
   `project_token_machine_os_fingerprint` memory).
