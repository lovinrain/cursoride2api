# Merge / rebase notes for `feat/ratlc-mvp`

This branch was forked off `feat/anthropic-api-support` at commit
`1548106` ("docs: cross-implementation study on thinking-block continuity")
and developed a separate architecture in `scaffolding/pool/` (the RATLC
warm-channel pool) plus extensions to shared files (`src/cursor-agent.js`,
`src/anthropic-tools.js`, etc.).

When pulling upstream `feat/anthropic-api-support` into this branch
(either via merge or future rebase), you'll see conflicts. **Most are
NOT real semantic disagreements** — they're git flagging proximity
because both branches happened to edit the same region of a file in
additive ways. Use the rules below to resolve quickly.

## Rules of thumb

### 1. DEVLOG.md, *.md docs → keep both sides

Both branches independently appended notes to the same regions of
DEVLOG and similar docs. There is no semantic competition — our pool
work and upstream's single-channel work are different topics. Resolve
by concatenating: keep our section, then upstream's, with a
horizontal-rule separator (`---`) between them. Order chronologically
if dates are visible.

### 2. `module.exports` blocks → keep both sides additively

Multiple commits on each side add exports to the same `module.exports
= { ... }` block. These are always additive (we're never removing the
same name the other side keeps). Resolve by listing both sides'
entries, deduplicating any shared names. Specifically:

- **`src/cursor-agent.js` exports MUST include** `buildMcpToolDefinitions`,
  `handleExecMessage`, `handleKvMessage`, `handleInteractionQuery`,
  `extractWebSearchServerToolEvent`, `sendExecClientMessage`,
  `sendExecClientControlMessage`, `sendExecClientMessageAndClose`,
  `sendKvResponse`, `frameConnectMessage`, `resolveClientFingerprint`,
  `loadProto`, `prewarmSharedClient`. These are consumed by
  `src/cursor-agent-h1.js` (the H1 transport variant we maintain that
  upstream doesn't have). Removing any of them breaks the pool.
- Test/debug aliases from upstream (e.g., `_handleExecMessage`) are
  fine to keep — additive.

### 3. Shared functions in `src/anthropic-tools.js`, `src/cursor-agent.js` → assess by architecture relevance

When both sides modified the same FUNCTION BODY, this IS a real
semantic conflict — read both implementations:

- **If upstream's change is a fix for a bug that affects our pool
  too** (e.g., `decodeMcpArgs` Value-format proto change, header
  client-fingerprint fix, listMcpResources handling): **take
  upstream's version**.
- **If upstream's change is a defense against a bug that doesn't
  manifest in our pool architecture** (e.g., subagent collision in
  the single-channel state cache — irrelevant when
  `POOL_CONTEXT_MODE=full` makes every turn self-contained): **keep
  ours** unless adopting upstream's version is harmless and reduces
  future-merge friction.
- **If unsure**: take ours and document the decision in the merge
  commit message, so the next merge knows why.

Special note on `deriveConversationKey` in `src/anthropic-tools.js`:
upstream's v2 (`hash(modelId, sessionId, firstUserText, toolHash)`)
adds defenses our pool doesn't need (we render full history each turn;
no per-conversation channel state to collide). The v2 was adopted in
the 2026-05-22 merge for future-merge-friction reasons. If a future
upstream version evolves convKey further, re-evaluate whether to keep
following or revert to our simpler v1.

Special note on PR #2's context byte-cap guard (from huaerye23,
commit `0139d20`): when merging PR #2, the guard MUST be gated on
`POOL_CONTEXT_MODE === 'hybrid'` so that `full` mode honors its name.
PR #2 ships the guard active in all modes, which silently violates
the `full` contract — see `BIDI_PAYLOAD_LIMIT.md` for the full design
rationale and the exact patch to apply during merge resolution. The
patch is two find-replaces against PR #2's `api-server.mjs`; do not
take PR #2's version verbatim.

### 4. Files that only OUR branch has → no conflict

The entire `scaffolding/pool/` tree, `src/cursor-agent-h1.js`,
`src/cursor-tool-matrix.js`, `MERGE_NOTES.md` (this file), and our
top-level docs (`NIAH_RESULTS.md` if reintroduced, etc.) only exist
on this branch. Upstream never had them. Merges will not touch them —
they pass through cleanly.

### 5. Files that only UPSTREAM has → adopt them

Upstream may add files we don't have (e.g., a new `src/` helper or
new top-level doc). These typically don't conflict; the merge brings
them in. Review them post-merge to decide whether to use them in our
pool architecture or leave dormant.

## Preferred workflow

Default to **`git merge upstream/feat/anthropic-api-support`** rather
than rebasing our 90+ commits onto upstream. Rationale:

- Merge resolves all conflicts ONCE. Rebase replays each of our
  commits and may re-hit the same conflict region many times.
- Merge preserves our linear history wrt PRs and the upstream's
  linear history wrt its own contributors.
- Force-push not needed for merge; required for rebase.
- The pool architecture's many small commits represent real
  exploration history we don't want to flatten.

If a future rebase IS desired (e.g., to publish a clean linear branch
without the messy fix-then-revert dance in our history), squash our
commits down to ~10 logical commits FIRST, then rebase the small set.
Squashing is reversible (we keep the original branch as
`backup/pre-squash` tag); rebasing 90+ commits one at a time isn't.

## Procedure for the next merge

```bash
# 1. Sync upstream
git fetch origin feat/anthropic-api-support

# 2. Tag current state so any mistake is recoverable
git tag -f backup/pre-merge $(git branch --show-current)

# 3. Attempt the merge — examine conflicts before resolving
git merge origin/feat/anthropic-api-support

# 4. For each conflict, apply the rules above:
#    - DEVLOG / docs:        keep both sides + separator
#    - module.exports:       keep both, dedupe
#    - shared function body: assess via rule 3
#    - everything else:      git checkout --ours <file> or --theirs <file>

# 5. Run syntax checks before committing
node --check src/cursor-agent.js
node --check src/cursor-agent-h1.js
node --check src/anthropic-tools.js
node --check scaffolding/pool/api-server.mjs
node --check scaffolding/pool/pool-manager.mjs
node --check scaffolding/pool/bridge-worker.mjs

# 6. Commit with a message documenting WHICH side won for each
#    real semantic conflict (rule 3) and WHY. Future merges read
#    that as historical justification.
git commit
git push
```

## History

Initial merge that established these rules: `be11f0d` (2026-05-22).

Resolved conflicts:
- `DEVLOG.md` — kept both sides verbatim (rule 1)
- `src/cursor-agent.js` exports block — kept ours + upstream's test alias (rule 2)
- `src/anthropic-tools.js` `deriveConversationKey` — took upstream's v2
  to reduce future-merge friction; our v1 was sufficient for our
  pool architecture (rule 3, defensive-but-harmless category)

Files upstream added or substantially modified that auto-merged
cleanly: `src/proto/agent_pb.mjs` (regen with WebFetch Value support),
`REFERENCES.md` (new), `server.js`, `HANDOVER_LOCAL_MODE.md`,
`README.md`. None required intervention.

---

PR #2 cherry-pick from huaerye23 (`0139d20`): `60fc2aa` (2026-05-22).

Resolved conflicts:
- `server.js` — simple additive: took upstream's `req.body = body;`
  (rule 2)
- `src/cursor-agent.js` near `decodeMcpArgs` — took upstream's new
  forward-compatible protobuf wire helpers (subagent_args=28,
  execute_hook_args=27, etc.) AND kept our richer `decodeMcpArgs`
  comment (rule 2, additive merge of comment + helpers)
- `src/cursor-agent.js` `module.exports` block — kept both sides:
  upstream's native result builders (`buildNativeReadResult`,
  `buildNativeWriteResult`, `buildNativeDeleteResult`,
  `buildNativeGrepResult`, `buildListMcpResourcesResult`,
  `buildSelectedContextForImages`, `describeUnknownFields`) AND our
  `_handleExecMessage` test alias. Deduplicated `resolveClientFingerprint`
  which appeared on both sides (rule 2)
- `scaffolding/pool/api-server.mjs` context guard — APPLIED THE
  BIDI_PAYLOAD_LIMIT.md PATCH: added `guardActive` const gated on
  `POOL_CONTEXT_MODE === 'hybrid'`, replaced both `CONTEXT_MAX_BYTES > 0`
  checks at guard sites with `guardActive`. Rationale: PR #2's guard
  silently violated the `full`-mode contract. See
  `BIDI_PAYLOAD_LIMIT.md` for the full design rationale. (Rule 3,
  defending-against-bug-irrelevant-to-our-mode category — we modified
  upstream rather than took it verbatim.)
