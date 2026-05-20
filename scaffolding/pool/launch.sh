#!/usr/bin/env bash
# Read launch.yaml, export every flat `KEY: value` pair as an env var, then
# exec `node ratlc.mjs` with whatever args the caller passed. Lets you keep
# the env var list in one place (launch.yaml) instead of a long inline
# `KEY=value KEY=value ... node ...` command.
#
# Usage:
#   ./launch.sh up [size]      # bring the pool up (size overrides POOL_SIZE)
#   ./launch.sh down           # tear it down
#   ./launch.sh status         # status snapshot
#   ./launch.sh tui            # interactive TUI
#
# Override a single var without editing the YAML:
#   POOL_SIZE=20 ./launch.sh up
#   (env-set values win because we only export YAML values if the var is unset)
#
# Use a different config file:
#   RATLC_LAUNCH_CONFIG=/path/to/other.yaml ./launch.sh up

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="${RATLC_LAUNCH_CONFIG:-$HERE/launch.yaml}"

if [ ! -f "$CONFIG" ]; then
  echo "launch.sh: config not found: $CONFIG" >&2
  exit 1
fi

# Parse the YAML with sed: strip comments + blank lines, extract `KEY: value`
# pairs where KEY is an unindented uppercase identifier. Only exports a var if
# it isn't already set in the environment (so command-line overrides win).
while IFS= read -r line; do
  # Skip blank lines and comments
  [[ -z "${line// }" ]] && continue
  [[ "$line" =~ ^[[:space:]]*# ]] && continue
  # Only flat top-level KEY: value (no indentation)
  if [[ "$line" =~ ^([A-Z_][A-Z0-9_]*)[[:space:]]*:[[:space:]]*(.*)$ ]]; then
    key="${BASH_REMATCH[1]}"
    val="${BASH_REMATCH[2]}"
    # Strip trailing inline comment + whitespace
    val="${val%%#*}"
    val="${val%"${val##*[![:space:]]}"}"
    # Strip optional surrounding quotes
    if [[ "$val" =~ ^\"(.*)\"$ ]]; then val="${BASH_REMATCH[1]}"; fi
    if [[ "$val" =~ ^\'(.*)\'$ ]]; then val="${BASH_REMATCH[1]}"; fi
    # Don't overwrite an already-set env var (lets callers override)
    if [ -z "${!key:-}" ]; then
      export "$key=$val"
    fi
  fi
done < "$CONFIG"

# Pick up POOL_SIZE override from the second arg of `up <N>` if given
exec node "$HERE/ratlc.mjs" "$@"
