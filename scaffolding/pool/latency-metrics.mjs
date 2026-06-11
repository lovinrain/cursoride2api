// Per-model latency metrics with time-of-day regimes.
//
// Goal: accumulate historical request latency (time-to-first-byte + total) per
// (model, payload-size bucket, hour-of-day), then compute live percentiles via
// t-digest. Because load varies by time of day, we LEARN 3 regimes (fast /
// medium / slow) by ranking the 24 hours by their median first-byte latency and
// splitting into terciles — the current regime is whichever tercile the current
// hour falls in. The suggested timeout is the CURRENT regime's p99 × margin, so
// it adapts to load instead of being one-size-fits-all.
//
// Why key by fixed hour-of-day (not by regime): regime boundaries shift as data
// accumulates; bucketing samples by regime at WRITE time would mis-file history
// when the boundaries move. Keying by hour and classifying regimes at READ time
// keeps history correct under reassignment.
//
// State is in-memory + persisted to /tmp/ratlc-stats.json (survives restarts =
// "historical"). Single-process: ONE api-server per RATLC_STATS_FILE — two
// sharing a file last-writer-wins (no locking/merge). persist() is atomic
// (tmp+rename) so a crash can't truncate; restore() is load-once (guarded).
//
// Metric semantics (so the numbers aren't misread):
//  • firstByteMs is time to the first visible frame of ANY kind — text,
//    thinking, or server-tool. For thinking-heavy models this is time-to-first-
//    THINKING, which can be well under time-to-first-answer; the suggested gap
//    (fb p99 × margin) is sized for first-frame silence, which is the right
//    thing for the liveness watchdog (it only cares that SOMETHING is flowing).
//  • Only requests that produced a first byte contribute an fb sample. A request
//    that times out before any byte has no fb to record (survivorship); the
//    `timeouts`/`errors` counts surface those instead. So read fb percentiles as
//    "latency given the stream started" and the timeout count as "how often it
//    didn't" — together they tell you if a gap is too tight.
//  • `timeouts` counts TERMINAL silent-timeouts (the request ended as one). A
//    transient timeout that a retry recovered is reflected in `retried`, not
//    `timeouts` (the recovered turn records its successful latency).

import fs from 'node:fs';
import { createRequire } from 'node:module';
import { isFastModel } from './model-utils.mjs';

const _require = createRequire(import.meta.url);
const { TDigest } = _require('tdigest');

const DISABLED = process.env.RATLC_STATS_DISABLE === '1';
const STATS_FILE = process.env.RATLC_STATS_FILE || '/tmp/ratlc-stats.json';
// Hour-of-day offset applied to UTC for the hour key (display alignment only;
// regimes are relative so correctness is offset-independent). Default UTC.
const HOUR_OFFSET = parseInt(process.env.RATLC_STATS_HOUR_OFFSET || '0', 10) || 0;
// Min samples before an hour participates in regime ranking / a suggestion is made.
const MIN_HOUR_SAMPLES = Math.max(1, parseInt(process.env.RATLC_STATS_MIN_HOUR_SAMPLES || '20', 10));
const MIN_SUGGEST_SAMPLES = Math.max(1, parseInt(process.env.RATLC_STATS_MIN_SUGGEST_SAMPLES || '30', 10));
const FB_MARGIN = clampFloat(process.env.RATLC_ADAPTIVE_FB_MARGIN, 1.5);
const REGIME_CACHE_MS = 60_000;

function clampFloat(v, d) { const n = parseFloat(v); return Number.isFinite(n) && n > 0 ? n : d; }

// ── payload-size buckets (by rendered Cursor content bytes) ─────────────────
const BUCKETS = [
  { label: '<=8KB', max: 8 * 1024 },
  { label: '<=64KB', max: 64 * 1024 },
  { label: '<=512KB', max: 512 * 1024 },
  { label: '>512KB', max: Infinity },
];
export const BUCKET_LABELS = BUCKETS.map((b) => b.label);
function bucketFor(bytes) {
  const n = Number.isFinite(bytes) ? bytes : 0;
  for (const b of BUCKETS) if (n <= b.max) return b.label;
  return BUCKETS[BUCKETS.length - 1].label;
}

function hourOf(ts) {
  const d = new Date(ts || Date.now());
  return (d.getUTCHours() + HOUR_OFFSET + 24) % 24;
}

// store: Map<model, Map<bucket, Map<hour, record>>>
const store = new Map();
// Cap distinct model keys so a typo'd/garbage model id (L1) can't leak forever.
const MAX_MODELS = Math.max(8, parseInt(process.env.RATLC_STATS_MAX_MODELS || '64', 10));
function newRecord() {
  return { count: 0, errors: 0, timeouts: 0, retried: 0, fb: new TDigest(), total: new TDigest() };
}
function getRec(model, bucket, hour) {
  let bm = store.get(model);
  if (!bm) { if (store.size >= MAX_MODELS) return null; bm = new Map(); store.set(model, bm); }
  let hm = bm.get(bucket); if (!hm) { hm = new Map(); bm.set(bucket, hm); }
  let rec = hm.get(hour); if (!rec) { rec = newRecord(); hm.set(hour, rec); }
  return rec;
}

// ── digest helpers ──────────────────────────────────────────────────────────
function dsize(d) { return d && typeof d.size === 'function' ? d.size() : 0; }
function mergeDigests(digests) {
  const m = new TDigest();
  for (const d of digests) { if (dsize(d) > 0) m.push_centroid(d.toArray()); }
  return m;
}
function pcts(digest, ps) {
  if (dsize(digest) === 0) return ps.map(() => null);
  const r = digest.percentile(ps);
  const arr = Array.isArray(r) ? r : [r];
  return arr.map((v) => (Number.isFinite(v) ? Math.round(v) : null));
}

// ── recording (the one hook, called from finishRequestLog) ──────────────────
export function record(entry) {
  if (DISABLED || !entry) return;
  const model = entry.servedModel || entry.routeModel || entry.model || 'unknown';
  const bytes = Number.isFinite(entry.contentBytes) ? entry.contentBytes
    : (Number.isFinite(entry.textBytes) ? entry.textBytes : 0);
  const bucket = bucketFor(bytes);
  const hour = hourOf(entry.endedAt);
  const rec = getRec(model, bucket, hour);
  if (!rec) return; // model-key cap reached (L1) — drop rather than leak
  rec.count++;
  const status = String(entry.status || '');
  const isTimeout = status === 'upstream_no_visible_event_timeout';
  const isError = isTimeout || status === 'error' || (!!entry.error && status !== 'completed' && status !== 'waiting_tool_result');
  if (isTimeout) rec.timeouts++;
  if (isError) rec.errors++;
  if ((entry.retryCount || 0) > 0) rec.retried++;
  if (Number.isFinite(entry.firstByteMs) && entry.firstByteMs >= 0) rec.fb.push(entry.firstByteMs);
  if (Number.isFinite(entry.durationMs) && entry.durationMs >= 0) rec.total.push(entry.durationMs);
}

// ── regime classification (read-time, cached) ───────────────────────────────
let _regimeCache = null;
let _regimeCacheAt = 0;
function hourGlobalFb(hour) {
  const ds = []; let n = 0;
  for (const bm of store.values()) for (const hm of bm.values()) {
    const rec = hm.get(hour); if (rec) { ds.push(rec.fb); n += rec.count; }
  }
  return { merged: ds.length ? mergeDigests(ds) : null, n };
}
function computeRegimes() {
  const hourP50 = {}; const ranked = [];
  for (let h = 0; h < 24; h++) {
    const { merged, n } = hourGlobalFb(h);
    const p50 = (n >= MIN_HOUR_SAMPLES && dsize(merged) > 0) ? merged.percentile(0.5) : null;
    hourP50[h] = { p50: Number.isFinite(p50) ? Math.round(p50) : null, n };
    if (p50 != null && Number.isFinite(p50)) ranked.push({ h, p50 });
  }
  const regimeOf = {};
  if (ranked.length < 3) {
    for (let h = 0; h < 24; h++) regimeOf[h] = 'medium';
  } else {
    // Stable tie-break by hour so equal-p50 hours classify deterministically.
    ranked.sort((a, b) => a.p50 - b.p50 || a.h - b.h);
    // Fractional tercile boundaries guarantee all three regimes are non-empty
    // for n>=3 (a `ceil(n/3)` split leaves `slow` empty at n=4, e.g. only 4
    // active hours during early rollout — see adversarial M3).
    const n = ranked.length, lo = n / 3, hi = (2 * n) / 3;
    ranked.forEach((r, i) => { regimeOf[r.h] = i < lo ? 'fast' : (i < hi ? 'medium' : 'slow'); });
    for (let h = 0; h < 24; h++) if (!(h in regimeOf)) regimeOf[h] = 'medium';
  }
  return { regimeOf, hourP50, learned: ranked.length >= 3 };
}
function getRegimes() {
  const now = Date.now();
  if (!_regimeCache || now - _regimeCacheAt > REGIME_CACHE_MS) {
    _regimeCache = computeRegimes(); _regimeCacheAt = now;
  }
  return _regimeCache;
}
export function currentHour() { return hourOf(Date.now()); }
export function currentRegime() { return getRegimes().regimeOf[currentHour()] || 'medium'; }
export function invalidateRegimeCache() { _regimeCache = null; }

// Merge a (model,bucket)'s digests across all hours that belong to `regime`.
function regimeMerged(model, bucket, regime, which) {
  const { regimeOf } = getRegimes();
  const bm = store.get(model); if (!bm) return null;
  const hm = bm.get(bucket); if (!hm) return null;
  const ds = [];
  for (const [hour, rec] of hm) if ((regimeOf[hour] || 'medium') === regime) ds.push(rec[which]);
  return ds.length ? mergeDigests(ds) : null;
}
function allHoursMerged(model, bucket, which) {
  const bm = store.get(model); if (!bm) return null;
  const hm = bm.get(bucket); if (!hm) return null;
  const ds = []; for (const rec of hm.values()) ds.push(rec[which]);
  return ds.length ? mergeDigests(ds) : null;
}
function allBucketsRegimeMerged(model, regime, which) {
  const { regimeOf } = getRegimes();
  const bm = store.get(model); if (!bm) return null;
  const ds = [];
  for (const hm of bm.values()) for (const [hour, rec] of hm) if ((regimeOf[hour] || 'medium') === regime) ds.push(rec[which]);
  return ds.length ? mergeDigests(ds) : null;
}

// Suggested silent-timeout gap (ms) for a (model, payloadBytes), derived from the
// CURRENT regime's first-byte p99 × margin. Falls back through coarser slices.
// Returns null when there isn't enough data (caller keeps its static threshold).
export function suggestGapMs(model, payloadBytes) {
  if (DISABLED) return null;
  const bucket = bucketFor(payloadBytes);
  const regime = currentRegime();
  const tries = [
    () => regimeMerged(model, bucket, regime, 'fb'),
    () => allHoursMerged(model, bucket, 'fb'),
    () => allBucketsRegimeMerged(model, regime, 'fb'),
  ];
  for (let i = 0; i < tries.length; i++) {
    const d = tries[i]();
    if (dsize(d) > 0 && d.n >= MIN_SUGGEST_SAMPLES) {
      const p99 = d.percentile(0.99);
      if (Number.isFinite(p99)) {
        return { gapMs: Math.max(1000, Math.round(p99 * FB_MARGIN)), regime, p99: Math.round(p99), samples: d.n, basis: ['regime+bucket', 'bucket', 'regime'][i] };
      }
    }
  }
  return null;
}

// ── snapshot for the UI / /v1/_stats ────────────────────────────────────────
function statBlock(d) { const [p50, p90, p99] = pcts(d, [0.5, 0.9, 0.99]); return { p50, p90, p99, n: dsize(d) > 0 ? d.n : 0 }; }
export function snapshot() {
  const { regimeOf, hourP50, learned } = getRegimes();
  const ch = currentHour();
  const cr = regimeOf[ch] || 'medium';
  const models = [];
  for (const [model, bm] of store) {
    const buckets = [];
    const overallFb = []; const overallTotal = []; let oCount = 0, oErr = 0, oTimeout = 0, oRetry = 0;
    for (const label of BUCKET_LABELS) {
      const hm = bm.get(label); if (!hm) continue;
      const fbs = []; const tots = []; let count = 0, errors = 0, timeouts = 0, retried = 0;
      for (const rec of hm.values()) { fbs.push(rec.fb); tots.push(rec.total); count += rec.count; errors += rec.errors; timeouts += rec.timeouts; retried += rec.retried; }
      if (count === 0) continue;
      const fbAll = mergeDigests(fbs); const totAll = mergeDigests(tots);
      overallFb.push(fbAll); overallTotal.push(totAll); oCount += count; oErr += errors; oTimeout += timeouts; oRetry += retried;
      const byRegime = {};
      for (const rg of ['fast', 'medium', 'slow']) {
        const d = regimeMerged(model, label, rg, 'fb');
        byRegime[rg] = dsize(d) > 0 ? { ...statBlock(d) } : { p50: null, p90: null, p99: null, n: 0 };
      }
      const sug = suggestGapMs(model, BUCKETS.find((b) => b.label === label).max === Infinity ? 1024 * 1024 : BUCKETS.find((b) => b.label === label).max);
      buckets.push({
        bucket: label, count, errors, timeouts, retried,
        errPct: count ? Math.round((errors / count) * 1000) / 10 : 0,
        retryPct: count ? Math.round((retried / count) * 1000) / 10 : 0,
        fb: statBlock(fbAll), total: statBlock(totAll),
        byRegimeFb: byRegime,
        currentRegimeSuggestGapMs: sug ? sug.gapMs : null,
      });
    }
    if (oCount === 0) continue;
    models.push({
      model, type: isFastModel(model) ? 'fast' : 'slow',
      overall: { count: oCount, errors: oErr, timeouts: oTimeout, retried: oRetry,
        errPct: oCount ? Math.round((oErr / oCount) * 1000) / 10 : 0,
        fb: statBlock(mergeDigests(overallFb)), total: statBlock(mergeDigests(overallTotal)) },
      buckets,
    });
  }
  models.sort((a, b) => a.model.localeCompare(b.model));
  return {
    generatedAt: Date.now(), hourOffset: HOUR_OFFSET, currentHour: ch, currentRegime: cr,
    regimesLearned: learned, regimeOf, hourP50, bucketLabels: BUCKET_LABELS,
    minHourSamples: MIN_HOUR_SAMPLES, fbMargin: FB_MARGIN, models,
  };
}

// ── persistence ─────────────────────────────────────────────────────────────
function serialize() {
  const out = { v: 1, savedAt: Date.now(), records: [] };
  for (const [model, bm] of store) for (const [bucket, hm] of bm) for (const [hour, rec] of hm) {
    out.records.push({ model, bucket, hour, count: rec.count, errors: rec.errors, timeouts: rec.timeouts, retried: rec.retried, fb: rec.fb.toArray(), total: rec.total.toArray() });
  }
  return out;
}
export function persist(file = STATS_FILE) {
  if (DISABLED) return;
  // Atomic: write a sibling tmp then rename, so a crash mid-write can't leave a
  // truncated file that loses ALL history (DR-3). rename(2) is atomic on POSIX.
  try {
    const tmp = `${file}.tmp.${process.pid}`;
    fs.writeFileSync(tmp, JSON.stringify(serialize()));
    fs.renameSync(tmp, file);
  } catch { /* best effort */ }
}
// restore() is additive (push_centroid sums weights), so calling it twice would
// DOUBLE every sample (H1). It's a boot-time load-once: guard against re-entry.
let _restored = false;
export function restore(file = STATS_FILE) {
  if (DISABLED || _restored) return;
  _restored = true;
  try {
    const data = JSON.parse(fs.readFileSync(file, 'utf8'));
    if (!data || !Array.isArray(data.records)) return;
    for (const e of data.records) {
      if (e == null || e.model == null || e.bucket == null || !(e.hour >= 0 && e.hour < 24)) continue;
      const count = Math.max(0, e.count | 0); // negative counts in a forged file can't corrupt err%
      const rec = getRec(String(e.model), String(e.bucket), e.hour | 0);
      if (!rec) continue; // model-key cap reached
      rec.count += count; rec.errors += Math.max(0, e.errors | 0); rec.timeouts += Math.max(0, e.timeouts | 0); rec.retried += Math.max(0, e.retried | 0);
      if (Array.isArray(e.fb) && e.fb.length) rec.fb.push_centroid(e.fb);
      if (Array.isArray(e.total) && e.total.length) rec.total.push_centroid(e.total);
    }
    invalidateRegimeCache();
  } catch { /* corrupt/missing → start fresh */ }
}

// For tests.
export const _internal = {
  bucketFor, hourOf, computeRegimes, store, mergeDigests,
  reset() { store.clear(); invalidateRegimeCache(); _restored = false; },
};
