# Token Refresh Design

Standalone planning doc for adding access-token refresh to the RATLC
pool. **Not yet implemented.** Current state: access tokens work, no
refresh logic, manual intervention required if any token expires.

This doc is self-contained — anyone reading it on this branch should be
able to implement without external references.

## Why this exists

`token.json` holds Cursor authentication tokens that the pool uses for
every channel-open and every model request. Two failure modes today:

1. **Expiration** — every token has a JWT `exp` claim. Once `Date.now()
   > exp * 1000`, the token is rejected by Cursor with
   `ERROR_NOT_LOGGED_IN` and the channel dies.
2. **Revocation** — Cursor can revoke a session server-side before
   `exp` (account compromise, manual logout, ToS action). Same wire
   symptom as expiration.

Currently both manifest the same way: `bridge-worker` reports
`errorKind: 'auth_error'` to the `pool-manager`, the pool-manager calls
`killTokenImmediately(...)`, and the token is marked dead until an
operator edits `token.json` and restarts.

This design adds **automatic refresh from a long-lived session token**
so the pool can recover without operator intervention.

## Token taxonomy

Cursor uses a two-tier auth model issued by WorkOS as the identity
provider. Both tokens are HS256-signed JWTs from
`iss: https://authentication.cursor.sh`.

### Session token (long-term)

- Stored shape: `user_XXXXXXXXXXXXX::eyJhbGciOiJIUzI1NiIs...`
  (some sources URL-encode the `::` as `%3A%3A`).
- The leading `user_...` prefix is a WorkOS user ID. The JWT body after
  `::` carries the actual auth claims.
- JWT payload includes `type: "web"`, `aud: "https://cursor.com"`,
  `scope: "openid profile email offline_access"`, and `exp`.
- Empirically `exp` is set ~60 days from issuance in the samples we
  observed. Not guaranteed for all accounts.
- **Used for**: cookie auth on Cursor's web/REST endpoints, AND as the
  refresh credential to mint new access tokens.

### Access token (short-term)

- Stored shape: bare JWT, `eyJhbGciOiJIUzI1NiIs...` (no `user_` prefix).
- JWT payload similar to session token but with `type: "session"`.
- Empirically `exp` was also ~60 days in our samples — but this is a
  per-account artifact, not a guarantee. Treat access tokens as
  short-lived for design purposes.
- **Used for**: `Authorization: Bearer <accessToken>` on Cursor's gRPC
  (AgentService) endpoints we hit from `cursor-agent.js` /
  `cursor-agent-h1.js`. This is the only path our pool exercises today.

### Validation rule (matches what UIs do)

A token is valid iff:

1. The trailing `eyJ...` segment parses as a JWT (header/payload base64
   decode + JSON.parse succeed).
2. The decoded payload has a numeric `exp`.
3. `exp * 1000 > Date.now()`.

Signature verification is not done client-side anywhere we've seen
(WorkOS signs with a key we don't have). This is consistent with
treating Cursor as the authoritative validator at request time.

### Extraction (one regex handles both shapes)

```js
// Pulls the JWT off either a bare `eyJ...` or `user_XXX::eyJ...` form.
const match = String(input).match(/ey[^ \s]*$/);
const jwt = match ? match[0] : null;
```

Anchor at end-of-string. Useful for any code that wants to decode the
JWT without caring whether the storage shape is bare-JWT or session-token.

## Exchange protocol (session token → access token)

There is a third-party service that exchanges a session token for a
fresh access token. As of this writing it is NOT operated by Cursor —
this is an external trust dependency. See "Trust considerations" below.

### Endpoint

```
GET https://token.cursorpro.com.cn/reftoken?token=<sessionToken>
```

- Method: `GET`
- Query parameter `token`: the full session-token string
  (`user_XXX::eyJ...`, URL-encoded as needed by HTTP).
- Headers: none required, but standard `Accept: application/json` is
  polite.

### Response (success)

```json
{
  "code": 0,
  "data": {
    "accessToken": "eyJhbGciOiJIUzI1NiIs...",
    "refreshToken": "..."
  }
}
```

- Success is signaled by `data.code === 0`, NOT by HTTP 200 alone.
  Always check the JSON shape, not just `res.ok`.
- `data.accessToken` is the new JWT to use for `Authorization: Bearer`.
- `data.refreshToken` is returned but the existing fly-cursor-free
  implementation does not use it for anything. We can store it for
  forward compatibility but don't have to.

### Response (failure)

```json
{ "code": <non-zero>, "msg": "..." }
```

Treat any `data.code !== 0`, non-2xx HTTP status, or non-JSON response
as a refresh failure. Do not retry indefinitely — fall back to error
handling described below.

### Cost / rate-limit assumption

The third-party endpoint's rate limits are undocumented. Assume:
- Refreshing on every channel spawn is too aggressive.
- Refreshing on actual auth failure is appropriate.
- Refreshing preemptively at most once per hour per token is safe.

## Trust considerations (do not skip)

`token.cursorpro.com.cn` is a **third-party service**, not run by
Cursor. Sending session tokens to it implies trusting that operator
not to log, sell, or replay credentials.

### Mitigations to consider

1. **Replace with Cursor's official refresh endpoint if/when discovered.**
   The WorkOS OAuth2 flow likely has a `/token` endpoint that accepts
   the session token's `offline_access` scope and returns a new access
   token. We have not located it. A short investigation should precede
   implementation — if Cursor exposes their own endpoint, prefer that.
2. **Use the third-party endpoint with explicit operator opt-in.**
   Default to off; require setting `RATLC_TOKEN_REFRESH_PROVIDER=cursorpro`
   to enable. Document the trust trade-off in the env-var description.
3. **Try direct session-token authentication first.** Cursor's web/REST
   endpoints accept session token via cookie. The gRPC `AgentService`
   path we use takes `Authorization: Bearer <jwt>`. It is unknown
   whether sending the trailing JWT portion of a session token as the
   Bearer works — if it does, no exchange is needed. Worth a probe before
   committing to the third-party path.

### Decision deferred

The first implementation can pick any of the three mitigations above.
The doc records all three so a future implementer can compare.

## Current state of our code

### Storage (`token.json`)

```json
{
  "tokens": [
    {
      "name": "account-1@example.com",
      "sessionToken": "user_01XXX::eyJhbGciOi...",
      "accessToken": "eyJhbGciOi...",
      "machineId": "<hex>",
      "macMachineId": "<hex>"
    }
  ]
}
```

- `name`: human-readable label, used in logs (e.g. token-rotation events).
- `sessionToken`: optional today; present on accounts where we have it.
  This is the refresh credential.
- `accessToken`: the JWT actually sent on every request today.
- `machineId`, `macMachineId`: fingerprint values; orthogonal to auth.

### Consumers

| File | What it does |
|------|--------------|
| `scaffolding/pool/pool-manager.mjs` | Reads `token.json` once at startup, round-robins index assignment, marks tokens dead on `auth_error` |
| `scaffolding/pool/bridge-worker.mjs` | Reads `token.json` once at module init, picks the entry indexed by `RATLC_TOKEN_INDEX`, exits on auth failure |
| `src/cursor-agent.js` and `src/cursor-agent-h1.js` | Build request headers: `'authorization': 'Bearer ' + token.accessToken` |

### Failure mode today

1. Cursor rejects with `unauthenticated` / `ERROR_NOT_LOGGED_IN`.
2. `bridge-worker.mjs` resolves the open-attempt as `{ kind: 'auth_error' }`.
3. `pool-manager.mjs` receives that, calls `killTokenImmediately(idx, 'auth_error', ...)`.
4. Token slot marked dead; round-robin skips it.
5. If all tokens die, pool log:
   `CRITICAL: every token is marked dead; falling back to index 0 anyway`
6. Operator notices via dashboard or 401s, edits `token.json` by hand,
   restarts the pool via `launch.sh down && launch.sh up`.

## Design

### Goals

1. **Reactive refresh.** When a channel observes `auth_error`, attempt
   refresh before marking the token dead. If refresh succeeds, retry
   with the new access token. If refresh fails, fall back to current
   behavior (mark dead).
2. **Preemptive refresh.** Decode the access-token JWT, compute time to
   `exp`. If under a configurable threshold (default 24h), refresh in
   the background. Avoids in-flight failures.
3. **Atomic persistence.** After refresh, write the new access token
   to `token.json` so a pool restart doesn't lose it.
4. **Single-flight concurrency.** Multiple channels seeing auth_error
   simultaneously must not trigger N parallel exchange calls. One
   in-flight refresh per token; concurrent requesters wait on the same
   promise.
5. **No regression.** Tokens without `sessionToken` (legacy single-token
   accounts) continue working exactly as today.
6. **Operator opt-in.** Refresh is off by default until we resolve the
   trust question. Enable with `RATLC_TOKEN_REFRESH_PROVIDER=<name>`.

### Architecture

Centralize all token state and refresh in `pool-manager.mjs`. The
manager already owns the rotation state and is a single process —
ideal for this responsibility.

```
┌───────────────────────────┐         ┌──────────────────────────┐
│  pool-manager.mjs         │         │  bridge-worker.mjs       │
│                           │   IPC   │  (one per channel)       │
│  TokenStore               │◄────────│  Reports auth_error      │
│  ─ refreshSessionToken()  │ refresh │  Reads its own token     │
│  ─ inFlightByTokenIdx     │  req    │  via env var              │
│  ─ atomic writeTokenFile  │         │                          │
│  Preemptive timer:        │         │                          │
│   ─ decode JWT exp        │         │                          │
│   ─ refresh if < 24h      │         │                          │
└───────────────────────────┘         └──────────────────────────┘
```

#### Reactive path

```
bridge-worker spawn fails with kind=auth_error
  └─► IPC to pool-manager: { type:'channel_exited', errorKind:'auth_error', tokenIdx }
        └─► pool-manager.handleAuthFailure(tokenIdx):
              ├─ if sessionToken absent → killTokenImmediately (current behavior)
              ├─ if refresh already in flight for this tokenIdx → await its promise
              ├─ else → start refresh, cache the promise, write result on success
              ├─ on success: update _tokens[tokenIdx].accessToken in memory + on disk
              ├─ on failure: killTokenImmediately
              └─ either way: do NOT respawn channel until promise resolves
```

#### Preemptive path

A timer in pool-manager, fired every 10 minutes:

```
for each tokenIdx where _tokenDead[tokenIdx] is false:
  if no sessionToken → skip
  decode current accessToken JWT
  remaining = exp*1000 - Date.now()
  if remaining < PREEMPTIVE_THRESHOLD_MS (default 24h):
    start refresh if not already in flight
```

The 24h threshold is conservative — gives us a full day of warning
before any in-flight failure could happen.

#### State

```js
// In pool-manager.mjs (alongside existing _tokenValidated etc.)
const _tokenSessionTokens = new Array(_tokenCount).fill(null);  // populated from token.json at startup
const _tokenRefreshInFlight = new Map();  // tokenIdx -> Promise<{ok, accessToken, error}>
const _tokenLastRefreshAt = new Array(_tokenCount).fill(0);
const _tokenRefreshCount = new Array(_tokenCount).fill(0);
```

#### Atomic persistence

```js
function writeTokenFileAtomic(updated) {
  const tmp = _tokenPath + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(updated, null, 2));
  fs.renameSync(tmp, _tokenPath);
}
```

Always read-modify-write the full file; never partial-write a single
token. The write happens under the in-flight promise so no two refreshes
race on the same file.

#### IPC additions

Two new message types between bridge-worker and pool-manager (the IPC
contract is documented in `scaffolding/pool/IPC.md`):

```
// bridge-worker → pool-manager
{ type: 'request_token_refresh', tokenIdx, reason: 'auth_error' | 'preemptive' }

// pool-manager → bridge-worker (only needed if we want hot reload)
{ type: 'token_refreshed', tokenIdx, accessToken }
```

Option A: bridge-worker exits on auth_error; pool-manager refreshes;
next spawn picks up the new token from disk. No `token_refreshed` IPC
needed. Simpler. Trade-off: every refresh = one channel respawn.

Option B: bridge-worker stays alive, receives `token_refreshed`, and
retries with the new token in-place. More complex. Avoids respawn cost.

Recommend Option A for v1.

### Concurrency safety

The single-flight pattern:

```js
async function refreshTokenSingleFlight(tokenIdx) {
  if (_tokenRefreshInFlight.has(tokenIdx)) {
    return _tokenRefreshInFlight.get(tokenIdx);
  }
  const promise = doRefresh(tokenIdx).finally(() => {
    _tokenRefreshInFlight.delete(tokenIdx);
  });
  _tokenRefreshInFlight.set(tokenIdx, promise);
  return promise;
}
```

`doRefresh()`:

1. Pull `sessionToken` from `_tokens[tokenIdx]`.
2. Issue HTTP GET to the configured refresh provider.
3. Parse response; if `code === 0`, take `data.accessToken`.
4. Update `_tokens[tokenIdx].accessToken` in memory.
5. Call `writeTokenFileAtomic(...)`.
6. Reset `_tokenOtherErrors[tokenIdx]` and `_tokenLastError[tokenIdx]`.
7. Log `token[idx] refreshed (provider=..., new exp=...)`.
8. Return `{ ok: true, accessToken: '...' }`.

On any failure, return `{ ok: false, error: msg }`. The caller decides
whether to `killTokenImmediately` based on context.

### Env vars

| Name | Default | Purpose |
|------|---------|---------|
| `RATLC_TOKEN_REFRESH_PROVIDER` | `(unset)` = disabled | `cursorpro` enables the third-party endpoint. Reserved values: `direct-session` (try session token as Bearer; experimental), `cursor-official` (if we ever discover an official refresh endpoint). |
| `RATLC_TOKEN_REFRESH_URL` | provider default | Override the provider URL (per-provider). For `cursorpro`: `https://token.cursorpro.com.cn/reftoken?token={sessionToken}`. |
| `RATLC_TOKEN_PREEMPTIVE_THRESHOLD_MS` | `86400000` (24h) | Refresh access token preemptively when this much time remains before `exp`. |
| `RATLC_TOKEN_PREEMPTIVE_INTERVAL_MS` | `600000` (10min) | How often the preemptive timer fires. |
| `RATLC_TOKEN_REFRESH_TIMEOUT_MS` | `15000` | HTTP timeout for refresh requests. |
| `RATLC_TOKEN_REFRESH_MAX_PER_HOUR` | `4` | Per-token rate limit on refreshes. Above this, treat further refreshes as no-op (returns cached). |

### Failure semantics

| Scenario | Behavior |
|----------|----------|
| Refresh provider down (5xx, timeout) | Return `{ok:false}`. Caller (reactive path) calls `killTokenImmediately`. Preemptive caller just logs and continues; next preemptive fire will retry. |
| Session token also expired/revoked | Provider returns `code != 0`. Same as above. |
| Refresh succeeds but returned access token is itself expired | Should be impossible from a working provider. Treat as `{ok:false}`. |
| Provider returns malformed JSON | Treat as `{ok:false}`. |
| Disk write of token.json fails | Log critically, abort the refresh (do NOT update in-memory token, since in-memory + on-disk would diverge — and a restart would lose the fresh token anyway). Return `{ok:false}`. |
| Concurrent token.json edit by operator | Atomic rename ensures the file is always valid JSON or the prior valid file. Operator edits between read and write would be lost — this is acceptable since operators shouldn't be editing while the pool is running. |

## Implementation plan

Recommended ordering:

1. **(Spike, < 1 day)** Try direct session-token use. Send the JWT
   portion of `sessionToken` as `Authorization: Bearer` to one of our
   gRPC endpoints and observe. If Cursor accepts, the entire third-party
   exchange may be unnecessary — design simplifies dramatically. Write
   findings into this doc as a new section.
2. **(Spike, < 1 day)** Search Cursor's UI / Cursor's documented APIs
   for an official refresh endpoint (likely under
   `authentication.cursor.sh` or `api2.cursor.sh`). If found, prefer it.
3. **(0.5 day)** Add `extractJwt(input)` and `getJwtExp(jwt)` helpers
   in a new `src/jwt-utils.js`. Pure functions, easy unit tests.
4. **(0.5 day)** Add `refreshTokenSingleFlight(tokenIdx)` in
   `pool-manager.mjs` with the third-party provider as v1. Stays
   disabled until env var is set.
5. **(0.5 day)** Add atomic write helper + per-token state arrays.
6. **(0.5 day)** Wire reactive path: in the `channel_exited` handler,
   when `errorKind === 'auth_error'` AND refresh is enabled AND
   `sessionToken` exists, defer `killTokenImmediately` until refresh
   resolves.
7. **(0.5 day)** Wire preemptive timer.
8. **(0.5 day)** Tests — see Testing section.
9. **(0.5 day)** Documentation: env-var list in `README.md` and
   `scaffolding/pool/launch.yaml` comments. Update `IPC.md` if Option B
   is taken.

Estimated total: **3-4 person-days** including the two spikes. If both
spikes find better paths, the third-party piece can be removed and the
total drops to 2-3 days.

## Open questions / decisions to make

1. **Provider choice**. Third-party `cursorpro.com.cn` vs. direct
   session-token use vs. discovering Cursor's official endpoint. The
   two spikes resolve this.
2. **Option A vs Option B for IPC**. Respawn channels on refresh
   (simpler, current state) vs. hot-reload tokens in living
   bridge-workers (more efficient, more code). Default: Option A.
3. **Preemptive threshold default**. 24h is conservative but generous.
   If preemptive refresh is cheap, could go to 7 days. If expensive
   (third-party rate limits), could go to 6h.
4. **Failure escalation timing**. Today a single `auth_error` kills the
   token. With refresh enabled, do we retry once before killing?
   Default proposal: yes — retry once with the new access token, kill
   on second auth_error within 60s.
5. **Logging verbosity**. Refresh events visible at INFO. Failures
   visible at WARN. Token contents NEVER logged.
6. **Metrics**. Expose per-token `last_refresh_at`, `refresh_count`,
   `last_refresh_error` on `/ratlc/status`. Useful for monitoring.

## Testing strategy

1. **Unit tests** (no network):
   - `extractJwt` handles bare JWT, `user_X::JWT`, URL-encoded `::`,
     malformed input.
   - `getJwtExp` returns null on garbage, returns ms on valid.
   - `refreshTokenSingleFlight` mocked: two parallel calls share one
     in-flight promise; success/failure paths.
   - Atomic write: simulate fs failure at rename → original file
     untouched.

2. **Integration tests** (against a fake refresh provider):
   - Spin up a local HTTP server that mimics the exchange protocol.
   - Verify pool recovers when access token is replaced with an
     expired JWT.
   - Verify preemptive timer fires when remaining time < threshold.
   - Verify operator-disabled (no env var) path = current behavior.

3. **Manual production-ish test**:
   - Set `accessToken` in `token.json` to a JWT with `exp` 10 seconds
     in the future. Watch channels open, then expire, then auto-refresh
     when reactive path triggers.
   - Inspect `token.json` after the test — verify it contains the
     newly-minted access token, NOT the manually-set expired one.

4. **Negative test**:
   - Set `sessionToken` to garbage. Verify pool falls back to current
     behavior (mark token dead).

## See also

- `BIDI_PAYLOAD_LIMIT.md` — same doc style: TL;DR + measurements +
  decisions + open follow-ups.
- `MERGE_NOTES.md` — when implementing, follow the merge rules for any
  `src/cursor-agent.js` or `pool-manager.mjs` changes.
- `scaffolding/pool/IPC.md` — IPC contract between pool-manager and
  bridge-worker, must be updated if Option B is taken.
