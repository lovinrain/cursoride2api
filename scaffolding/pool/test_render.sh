#!/usr/bin/env bash
# Render-debug test: confirms the proxy's renderFullContext() faithfully
# preserves every message + tool_use + tool_result in a multi-turn body.
# Uses /v1/_debug/render which is gated on POOL_REINJECT_THINKING_DEBUG=1.
#
# Builds a fake multi-turn body, sends it to /v1/_debug/render, then greps
# the rendered output for distinctive strings from each turn. PASS = every
# turn's distinctive substring appears in the rendered text.
#
# Run this BEFORE test_niah.sh if you suspect the proxy is dropping turns.
# It's instant (no Cursor backend involvement) and isolates rendering from
# model behavior.
#
# Usage:
#   POOL_REINJECT_THINKING_DEBUG=1 ./launch.sh up      # toggle endpoint on
#   ./test_render.sh                                    # run the test
#   POOL_REINJECT_THINKING_DEBUG=  ./launch.sh up      # toggle back off
#
# Or, while pool is already running, set the env on the api-server only:
#   pkill -f api-server.mjs
#   POOL_REINJECT_THINKING_DEBUG=1 ... node scaffolding/pool/api-server.mjs &

set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:4242}"

BODY_FILE="$(mktemp -t render_body_XXXXXX.json)"
RESP_FILE="$(mktemp -t render_resp_XXXXXX.json)"
trap 'rm -f "$BODY_FILE" "$RESP_FILE"' EXIT

# Build a small multi-turn body with distinctive sentinels per turn that we
# can grep for in the rendered output.
python3 - > "$BODY_FILE" <<'PYEOF'
import json
sentinels = [f'SENTINEL_{chr(65+i)}_{i*37+101}' for i in range(8)]
msgs = []
# Mix text, tool_use, and tool_result blocks so we exercise every renderer branch
msgs.append({'role': 'user', 'content': f'Turn 1 user: {sentinels[0]}. What is 2+2?'})
msgs.append({'role': 'assistant', 'content': f'Turn 1 assistant: {sentinels[1]}. The answer is 4.'})
msgs.append({'role': 'user', 'content': f'Turn 2 user: {sentinels[2]}. Run ls.'})
msgs.append({'role': 'assistant', 'content': [
    {'type': 'text', 'text': f'Turn 2 assistant text: {sentinels[3]}. I will use Bash.'},
    {'type': 'tool_use', 'id': 'toolu_test01', 'name': 'Bash', 'input': {'command': f'ls # {sentinels[4]}'}},
]})
msgs.append({'role': 'user', 'content': [
    {'type': 'tool_result', 'tool_use_id': 'toolu_test01', 'content': f'a.txt\nb.txt\n{sentinels[5]}'},
]})
msgs.append({'role': 'assistant', 'content': f'Turn 3 assistant: {sentinels[6]}. Got 2 files.'})
msgs.append({'role': 'user', 'content': f'Final user turn: {sentinels[7]}. Quiz me on all sentinels.'})
body = {
    'model': 'claude-4.6-opus-max-thinking-fast',
    'messages': msgs,
    'system': 'You are a helpful assistant.',
    'tools': [{'name': 'Bash', 'description': 'Run shell', 'input_schema': {'type':'object','properties':{'command':{'type':'string'}}}}],
}
out = {'body': body, 'sentinels': sentinels}
print(json.dumps(out))
PYEOF

# Extract just the body portion to POST
jq -c .body < "$BODY_FILE" > /tmp/render_body_only.json
SENTINELS=($(jq -r '.sentinels[]' < "$BODY_FILE"))

echo "=== render-debug test ==="
echo "  sentinels (${#SENTINELS[@]}):  ${SENTINELS[*]}"
echo "  body size: $(wc -c < /tmp/render_body_only.json) bytes"
echo ""

HTTP=$(curl -s -o "$RESP_FILE" -w '%{http_code}' \
  -X POST "$BASE_URL/v1/_debug/render" \
  -H 'Content-Type: application/json' \
  --data @/tmp/render_body_only.json)
echo "  http status: $HTTP"

if [ "$HTTP" = "404" ]; then
  echo ""
  echo "  → /v1/_debug/render is OFF. Enable with POOL_REINJECT_THINKING_DEBUG=1"
  echo "    in launch.yaml and re-run after './launch.sh up'."
  exit 1
fi

if [ "$HTTP" != "200" ]; then
  echo ""
  echo "  → unexpected status. Response body:"
  cat "$RESP_FILE" | head -c 500
  exit 1
fi

# Extract rendered text and check for each sentinel
RENDERED=$(jq -r '.rendered // .text // empty' < "$RESP_FILE")
RENDERED_SIZE=$(printf '%s' "$RENDERED" | wc -c)
echo "  rendered text size: $RENDERED_SIZE chars"
echo ""

MISSING=()
for s in "${SENTINELS[@]}"; do
  if ! printf '%s' "$RENDERED" | grep -qF "$s"; then
    MISSING+=("$s")
  fi
done

if [ ${#MISSING[@]} -eq 0 ]; then
  echo "PASS: all ${#SENTINELS[@]} sentinels found in rendered output"
  echo ""
  echo "=== rendered preview (first 800 chars) ==="
  printf '%s\n' "$RENDERED" | head -c 800
  echo ""
  exit 0
else
  echo "FAIL: ${#MISSING[@]} sentinels missing from rendered output:"
  for s in "${MISSING[@]}"; do echo "  - $s"; done
  echo ""
  echo "=== rendered preview (first 1500 chars) ==="
  printf '%s\n' "$RENDERED" | head -c 1500
  echo ""
  exit 1
fi
