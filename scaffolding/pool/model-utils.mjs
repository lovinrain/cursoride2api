// Shared model-type classifier. "fast" is a Cursor naming convention — the
// `-fast` accelerator token (e.g. claude-opus-4-8-thinking-max-fast,
// claude-4.6-opus-max-thinking-fast). Used by api-server (per-request watchdog),
// pool-manager (per-channel busy-watchdog), and the TUI (per-channel SILENT
// threshold) so all three agree on a model's type. Single source of truth.
export function isFastModel(modelId) {
  // `-fast` followed by a word boundary (so it matches the trailing accelerator
  // token in `…-max-fast` / `…-fast-v2`, but NOT a mid-word "fastish").
  return /-fast\b/i.test(String(modelId || ''));
}

// Coarse type label for display/keying.
export function modelType(modelId) {
  return isFastModel(modelId) ? 'fast' : 'slow';
}

function readEnvInt(name, fallback) {
  const v = process.env[name];
  if (v == null || v === '') return fallback;
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : fallback;
}

// Resolve a watchdog threshold for BOTH model types. Precedence per type:
//   RATLC_<KEY>_{FAST,SLOW}  ??  RATLC_<KEY> (global)  ??  dflt   (then floored).
// Single source shared by api-server (per-request), pool-manager (busy-watchdog
// + snapshot), and the TUI (SILENT countdown). `key` is the env suffix, e.g.
// 'NO_VISIBLE_LIVENESS_GRACE_MS'.
export function typeThresholds(key, dflt, floor = 0) {
  const global = readEnvInt(`RATLC_${key}`, dflt);
  const fast = Math.max(floor, readEnvInt(`RATLC_${key}_FAST`, global));
  const slow = Math.max(floor, readEnvInt(`RATLC_${key}_SLOW`, global));
  return { fast, slow };
}

// Resolve a threshold for one specific model.
export function typeThresholdMs(model, key, dflt, floor = 0) {
  const t = typeThresholds(key, dflt, floor);
  return isFastModel(model) ? t.fast : t.slow;
}
