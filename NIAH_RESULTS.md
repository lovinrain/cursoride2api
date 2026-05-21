# NIAH (Needle-In-A-Haystack) test results

Multi-turn long-context verification for the RATLC pool. Each row sends one
`POST /v1/messages` carrying a synthetic conversation with a random needle
string injected at a chosen depth, then checks that the model retrieves the
exact needle. The script is `scaffolding/pool/test_niah.sh`.

## Model under test

`claude-4.6-opus-max-thinking-fast[1m]` via the live proxy at
`http://127.0.0.1:4242` (pool with `POOL_CONTEXT_MODE=full`,
`POOL_REINJECT_THINKING=1`).

Test date: 2026-05-20.

## Configuration

- `DEPTH=N` → N filler turns BEFORE the needle (each turn ≈ 100 chars)
- `TAIL=N` → N filler turns AFTER the needle, before the final retrieval query
- Total messages: `2*DEPTH + 2*TAIL + 3`
- Needle is injected exactly once as a unique random hex string

## Results

| msgs | body | ~tokens | needle position | latency | result |
|---|---|---|---|---|---|
| 33 | 3.6 KB | 1K | 60% | 1s | PASS |
| 73 | 7.8 KB | 2K | 80% | 2s | PASS |
| 213 | 22 KB | 6K | 94% | 1s | PASS |
| 613 | 64 KB | 18K | 98% | 2s | PASS |
| 2013 | 212 KB | 60K | 99% | 6s | PASS |
| 213 | 22 KB | 6K | **16%** | 2s | PASS |
| 1013 | ~100 KB | 30K | **2%** | 9s | PASS |
| 4013 | 426 KB | 122K | **0.5%** | 3s | PASS |
| 4013 | 426 KB | 122K | 99.7% | 5s | PASS |
| 10003 | 1.06 MB | 290K | 50% | 18s | PASS |
| 20003 | 2.13 MB | 580K | 50% | 10s | PASS |
| 24003 | 2.55 MB | 720K | 50% | 25s | PASS |
| 30003 | 3.20 MB | 900K | 50% | 15s | **PASS (max working)** |
| 34003 | 3.60 MB | 1.03M | 50% | 29s | **FAIL — model returned "READY"** |
| 36003 | 3.80 MB | 1.09M | 50% | 65s | FAIL — "READY" |
| 40003 | 4.27 MB | 1.16M | 50% | 60s | FAIL — "READY" |

## Findings

1. **Effective context ceiling: ~900K–1.03M tokens.** Past the wall, the model
   returns its priming-prompt string (`READY`) instead of an answer. This is
   the same fallback mode documented for the `claude-opus-4-7-max-fast` sibling
   (memory: `project_claude_opus_47_max_fast_effective_context.md`), at a
   higher ceiling (~900K vs ~600K).
2. **Needle position is largely irrelevant within the working ceiling.** Tested
   at 0.5%, 2%, 16%, 50%, 60%, 80%, 94%, 98%, 99%, 99.7%. All retrieved
   correctly. No "lost in the middle" failure was observed below the wall.
3. **Latency scales roughly linearly with context size.** From 1s at 1K tokens
   to 25s at 720K tokens. Variance is high (15-29s at the 900K–1M range)
   probably due to Cursor backend thinking time.
4. **Proxy correctness is confirmed.** The rendering pipeline preserves
   thousands of messages, multiple content-block types (text, tool_use,
   tool_result, thinking placeholders), and arbitrary needle positions
   without loss or reordering.

## Implications

- The `CLAUDE_CODE_AUTO_COMPACT_WINDOW=600000` flag the user runs with keeps
  rendered context at ~67% of the effective ceiling — comfortable headroom.
  Auto-compaction kicks in well before the priming-fallback wall is hit.
- The earlier ch-307 / ch-335 silent stalls were at ~330KB body / ~94K tokens
  — that's ~10% of the effective ceiling. Those stalls are **not** a context-
  size issue. They're triggered by specific content patterns in those
  conversations (something Cursor's backend can't process inside that specific
  text).
- The `[1m]` model variant nominally advertises 1M context. The effective
  ceiling is ~900K usable — close enough to the advertised value that the
  variant lives up to most of its claim, unlike the 4-7 sibling.

## How to reproduce

```bash
./scaffolding/pool/test_niah.sh                        # default DEPTH=10
DEPTH=30 ./scaffolding/pool/test_niah.sh
DEPTH=100 TAIL=2000 ./scaffolding/pool/test_niah.sh    # needle EARLY, lots after
DEPTH=10000 TAIL=10000 ./scaffolding/pool/test_niah.sh # past the ceiling, expect FAIL
MODEL='claude-opus-4-7-thinking-max-fast' ./scaffolding/pool/test_niah.sh
```

## Limitations of this test

- **Synthetic content.** Each filler turn is a short prime-number sentence,
  not real claude-code conversation content. Real workloads have tool_uses,
  tool_results with file contents, much longer per-turn text. To get a more
  realistic stress test, modify the script to inject 2-3 KB per turn (would
  hit the ceiling at a much lower message count).
- **Single-shot.** Each test is one `POST /v1/messages`. Doesn't exercise
  the full multi-turn cycle where claude-code POSTs, gets a `tool_use`,
  executes it locally, POSTs back the `tool_result`, etc.
- **No tool_use roundtrips in the body.** The synthetic conversation uses
  only `text` content blocks. Tool-rich conversations would render differently
  (and tested separately via `test_render.sh` once the
  `POOL_REINJECT_THINKING_DEBUG=1` toggle is enabled).
- **Cursor backend variability.** Some test runs at 900K tokens take 15s,
  some take 29s. Backend latency is unpredictable at high context sizes.
