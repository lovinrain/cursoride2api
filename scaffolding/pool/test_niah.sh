#!/usr/bin/env bash
# NIAH (Needle In A Haystack) test for the RATLC pool's long-context handling.
#
# Builds a multi-turn conversation in memory, injects a unique random "needle"
# string at a configurable depth, then asks the model in a final user turn to
# retrieve it. The full multi-turn messages array is sent to /v1/messages in
# ONE POST (matching how claude-code sends its full history each turn).
#
# PASS means the proxy correctly rendered every turn (including the needle
# turn deep in history) and the model retrieved the needle from context.
# FAIL means either the proxy dropped/reordered something, the rendered
# prompt overflowed the model's effective context, or the model failed to
# attend to the needle. Use the api log to disambiguate.
#
# Usage:
#   ./test_niah.sh                                      # defaults: DEPTH=10, model=claude-4.6-opus-max-thinking-fast
#   DEPTH=30 ./test_niah.sh                             # deeper needle
#   MODEL='claude-4.6-opus-max-thinking-fast[1m]' ./test_niah.sh
#   BASE_URL=http://127.0.0.1:4242 ./test_niah.sh

set -euo pipefail

DEPTH="${DEPTH:-10}"
TAIL="${TAIL:-5}"
MODEL="${MODEL:-claude-4.6-opus-max-thinking-fast[1m]}"
BASE_URL="${BASE_URL:-http://127.0.0.1:4242}"
NEEDLE="${NEEDLE:-NIAH-$(openssl rand -hex 8 2>/dev/null || cat /proc/sys/kernel/random/uuid | tr -d '-' | head -c 16)}"

echo "=== NIAH test ==="
echo "  model:   $MODEL"
echo "  depth:   $DEPTH turns of fluff before the needle, $TAIL after"
echo "  needle:  $NEEDLE"
echo "  base:    $BASE_URL"
echo ""

BODY_FILE="$(mktemp -t niah_body_XXXXXX.json)"
SSE_FILE="$(mktemp -t niah_sse_XXXXXX.out)"
trap 'rm -f "$BODY_FILE" "$SSE_FILE"' EXIT

# Build the multi-turn messages array via python3
python3 - "$NEEDLE" "$DEPTH" "$TAIL" "$MODEL" > "$BODY_FILE" <<'PYEOF'
import json, sys
needle, depth, tail, model = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
msgs = []
# Filler turns before the needle — each turn adds ~100 chars of mostly-unique text
for i in range(depth):
    msgs.append({'role': 'user', 'content': f'Q{i+1}: Tell me one short sentence about prime number {i*7+11}.'})
    msgs.append({'role': 'assistant', 'content': f'A{i+1}: The prime {i*7+11} is divisible only by 1 and itself, with no factors in between.'})
# Needle turn
msgs.append({'role': 'user', 'content': f'Important context: the magic phrase for later retrieval is exactly "{needle}". I will quiz you on it at the end of our conversation. For now, please just acknowledge briefly.'})
msgs.append({'role': 'assistant', 'content': f'Acknowledged. I will remember the magic phrase exactly as you provided it.'})
# Filler turns after the needle
for i in range(tail):
    msgs.append({'role': 'user', 'content': f'Filler{i+1}: One short sentence about composite number {i*13+22}.'})
    msgs.append({'role': 'assistant', 'content': f'FillerA{i+1}: The composite number {i*13+22} has multiple factors beyond 1 and itself.'})
# Final retrieval question — assistant must produce the needle
msgs.append({
    'role': 'user',
    'content': 'Quiz time: what was the exact magic phrase I gave you earlier? Reply with ONLY the phrase, no preamble, no quotes, no explanation.',
})

body = {
    'model': model,
    'max_tokens': 128,
    'messages': msgs,
    'stream': True,
}
print(json.dumps(body))
PYEOF

BODY_BYTES=$(wc -c < "$BODY_FILE")
echo "  request body: $BODY_BYTES bytes, $(jq '.messages | length' < "$BODY_FILE") messages"
echo ""

# POST and stream the SSE response
echo "  posting to $BASE_URL/v1/messages?beta=true ..."
START=$(date +%s)
HTTP=$(curl -sN -o "$SSE_FILE" -w '%{http_code}' \
  --max-time 180 \
  -X POST "$BASE_URL/v1/messages?beta=true" \
  -H 'Content-Type: application/json' \
  -H 'Accept: text/event-stream' \
  --data @"$BODY_FILE")
ELAPSED=$(( $(date +%s) - START ))
echo "  http status: $HTTP  (elapsed: ${ELAPSED}s)"
echo ""

# Extract assembled text from content_block_delta events
ANSWER=$(python3 - <<PYEOF
import json
text = ''
with open('$SSE_FILE') as f:
  for line in f:
    line = line.strip()
    if not line.startswith('data: '): continue
    try:
      obj = json.loads(line[6:])
      if obj.get('type') == 'content_block_delta':
        delta = obj.get('delta', {})
        if delta.get('type') == 'text_delta':
          text += delta.get('text', '')
    except Exception:
      pass
print(text)
PYEOF
)

# Also extract any error event for triage
ERROR=$(python3 - <<PYEOF
import json
with open('$SSE_FILE') as f:
  for line in f:
    line = line.strip()
    if not line.startswith('data: '): continue
    try:
      obj = json.loads(line[6:])
      if obj.get('type') == 'error':
        print(obj.get('error', {}).get('message', ''))
        break
    except Exception:
      pass
PYEOF
)

echo "=== result ==="
echo "  answer:  $(printf '%s' "$ANSWER" | head -c 400)"
if [ -n "$ERROR" ]; then
  echo "  error:   $ERROR"
fi
echo ""

if [ -z "$ANSWER" ]; then
  echo "FAIL: empty response (likely Cursor backend stall or proxy error)"
  exit 1
fi

if echo "$ANSWER" | grep -qF "$NEEDLE"; then
  echo "PASS: needle retrieved exactly from depth=$DEPTH"
  exit 0
else
  echo "FAIL: needle not in answer (model attended to a different part of context)"
  exit 1
fi
