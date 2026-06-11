import fs from 'node:fs';
import { record, snapshot, persist, restore, invalidateRegimeCache, suggestGapMs, _internal } from './latency-metrics.mjs';

let fail = 0;
const a = (n, c, d) => { console.log((c ? '  ✓ ' : '  ✗ ') + n + (c ? '' : ` — ${d}`)); if (!c) fail++; };
const near = (x, y, tol) => Math.abs(x - y) <= tol;
// Deterministic hour timestamps (UTC; HOUR_OFFSET defaults 0).
const base = Date.UTC(2026, 0, 1, 0, 0, 0);
const hourTs = (h) => base + h * 3600_000;
const reset = () => { _internal.reset(); };

console.log('=== record → percentiles ===');
reset();
// 200 first-byte samples 100..2000ms (uniform-ish), small payload, hour 0.
for (let i = 0; i < 200; i++) record({ servedModel: 'm', contentBytes: 100, endedAt: hourTs(0), firstByteMs: 100 + i * 10, durationMs: 1000 + i * 20, status: 'completed' });
{
  const snap = snapshot();
  const m = snap.models.find((x) => x.model === 'm');
  const b = m.buckets[0];
  a('bucket is <=8KB', b.bucket === '<=8KB', b.bucket);
  a('count = 200', b.count === 200, b.count);
  a('fb p50 ~ 1100ms', near(b.fb.p50, 1100, 120), `p50=${b.fb.p50}`);
  a('fb p99 ~ 1990ms', near(b.fb.p99, 1990, 150), `p99=${b.fb.p99}`);
  a('fb p50 < p90 < p99', b.fb.p50 < b.fb.p90 && b.fb.p90 <= b.fb.p99, `${b.fb.p50}/${b.fb.p90}/${b.fb.p99}`);
}

console.log('=== payload buckets ===');
reset();
record({ servedModel: 'm', contentBytes: 4 * 1024, endedAt: hourTs(0), firstByteMs: 100, durationMs: 200, status: 'completed' });
record({ servedModel: 'm', contentBytes: 40 * 1024, endedAt: hourTs(0), firstByteMs: 100, durationMs: 200, status: 'completed' });
record({ servedModel: 'm', contentBytes: 300 * 1024, endedAt: hourTs(0), firstByteMs: 100, durationMs: 200, status: 'completed' });
record({ servedModel: 'm', contentBytes: 2 * 1024 * 1024, endedAt: hourTs(0), firstByteMs: 100, durationMs: 200, status: 'completed' });
{
  const labels = snapshot().models[0].buckets.map((b) => b.bucket).sort();
  a('four payload buckets populated', JSON.stringify(labels) === JSON.stringify(['<=512KB', '<=64KB', '<=8KB', '>512KB']), JSON.stringify(labels));
}

console.log('=== regime classification (rank 24 hours by median TTFB → terciles) ===');
reset();
// hours 0-7: fast (~100ms), 8-15: medium (~1000ms), 16-23: slow (~5000ms); 30 samples each.
for (let h = 0; h < 24; h++) {
  const fb = h < 8 ? 100 : h < 16 ? 1000 : 5000;
  for (let i = 0; i < 30; i++) record({ servedModel: 'm', contentBytes: 100, endedAt: hourTs(h), firstByteMs: fb + (i % 5), durationMs: fb * 2, status: 'completed' });
}
{
  const { regimeOf, learned } = _internal.computeRegimes();
  a('regimes learned', learned, 'not enough data');
  a('hours 0-7 → fast', [0, 3, 7].every((h) => regimeOf[h] === 'fast'), JSON.stringify([0, 3, 7].map((h) => regimeOf[h])));
  a('hours 8-15 → medium', [8, 11, 15].every((h) => regimeOf[h] === 'medium'), JSON.stringify([8, 11, 15].map((h) => regimeOf[h])));
  a('hours 16-23 → slow', [16, 20, 23].every((h) => regimeOf[h] === 'slow'), JSON.stringify([16, 20, 23].map((h) => regimeOf[h])));
  // regime merge: the 'fast' regime's fb p50 ≈ 100, 'slow' ≈ 5000
  const snap = snapshot();
  const bucket = snap.models[0].buckets[0];
  a('byRegime fast p50 ~100', near(bucket.byRegimeFb.fast.p50, 100, 30), `=${bucket.byRegimeFb.fast.p50}`);
  a('byRegime slow p50 ~5000', near(bucket.byRegimeFb.slow.p50, 5000, 60), `=${bucket.byRegimeFb.slow.p50}`);
}

console.log('=== thin / empty guards ===');
reset();
{
  const snap = snapshot();
  a('empty store → no models', snap.models.length === 0, JSON.stringify(snap.models));
  a('empty → regimes not learned (all medium)', snap.regimeOf[0] === 'medium' && !snap.regimesLearned, JSON.stringify(snap.regimeOf[0]));
  record({ servedModel: 'm', contentBytes: 100, endedAt: hourTs(0), firstByteMs: 500, durationMs: 800, status: 'completed' });
  a('1 sample < MIN_SUGGEST → suggestGapMs null', suggestGapMs('m', 100) === null, 'unexpected suggestion');
}

console.log('=== error / timeout / retry accounting ===');
reset();
for (let i = 0; i < 10; i++) record({ servedModel: 'm', contentBytes: 100, endedAt: hourTs(0), firstByteMs: 100, durationMs: 200, status: 'completed' });
record({ servedModel: 'm', contentBytes: 100, endedAt: hourTs(0), status: 'upstream_no_visible_event_timeout', error: 'x' });
record({ servedModel: 'm', contentBytes: 100, endedAt: hourTs(0), status: 'error', error: 'boom' });
record({ servedModel: 'm', contentBytes: 100, endedAt: hourTs(0), firstByteMs: 100, durationMs: 200, status: 'completed', retryCount: 2 });
{
  const b = snapshot().models[0].buckets[0];
  a('count=13', b.count === 13, b.count);
  a('errors=2 (timeout+error)', b.errors === 2, b.errors);
  a('timeouts=1', b.timeouts === 1, b.timeouts);
  a('retried=1', b.retried === 1, b.retried);
}

console.log('=== persistence round-trip (centroids) ===');
reset();
for (let i = 0; i < 300; i++) record({ servedModel: 'm', contentBytes: 100, endedAt: hourTs(5), firstByteMs: 200 + (i % 100) * 7, durationMs: 500 + i, status: 'completed' });
const file = `/tmp/ratlc-stats-test-${process.pid}.json`;
const before = snapshot().models[0].buckets[0].fb;
persist(file);
reset();
a('after reset → empty', snapshot().models.length === 0, 'not empty');
restore(file);
{
  const after = snapshot().models[0].buckets[0].fb;
  a('round-trip p50 preserved', near(before.p50, after.p50, Math.max(20, before.p50 * 0.05)), `${before.p50} vs ${after.p50}`);
  a('round-trip p99 preserved', near(before.p99, after.p99, Math.max(40, before.p99 * 0.05)), `${before.p99} vs ${after.p99}`);
}
try { fs.unlinkSync(file); } catch {}
restore('/tmp/does-not-exist-xyz.json'); // must not throw
a('restore(missing) is a no-op, no throw', true);

console.log('=== H1: double restore() must NOT double-count ===');
reset();
for (let i = 0; i < 120; i++) record({ servedModel: 'm', contentBytes: 100, endedAt: hourTs(5), firstByteMs: 300 + i, durationMs: 600 + i, status: 'completed' });
const file2 = `/tmp/ratlc-stats-test2-${process.pid}.json`;
persist(file2);
reset();
restore(file2);
const afterFirst = snapshot().models[0].buckets[0];
restore(file2); // second restore — guard must make this a no-op
const afterSecond = snapshot().models[0].buckets[0];
a('count stable across double restore (120, not 240)', afterFirst.count === 120 && afterSecond.count === 120, `${afterFirst.count} → ${afterSecond.count}`);
a('p99 stable across double restore', afterFirst.fb.p99 === afterSecond.fb.p99, `${afterFirst.fb.p99} vs ${afterSecond.fb.p99}`);
try { fs.unlinkSync(file2); } catch {}

console.log('=== M3: exactly 4 active hours → all three regimes non-empty ===');
reset();
// Only 4 distinct hours reach MIN_HOUR_SAMPLES; rest stay thin. Tercile split
// must still produce a non-empty `slow` regime (ceil(4/3)=2 would empty it).
[[0, 100], [6, 400], [12, 1600], [18, 6400]].forEach(([h, fb]) => {
  for (let i = 0; i < 30; i++) record({ servedModel: 'm', contentBytes: 100, endedAt: hourTs(h), firstByteMs: fb + (i % 5), durationMs: fb * 2, status: 'completed' });
});
{
  const { regimeOf, learned } = _internal.computeRegimes();
  const active = [0, 6, 12, 18].map((h) => regimeOf[h]);
  a('4-hour regimes learned', learned, 'not learned');
  a('fastest active hour → fast', regimeOf[0] === 'fast', regimeOf[0]);
  a('slowest active hour → slow (slow not empty)', regimeOf[18] === 'slow', `regimes=${JSON.stringify(active)}`);
  a('all 3 regimes appear among active hours', new Set(active).size === 3, JSON.stringify(active));
}

console.log(fail === 0 ? '\nlatency-metrics-test: OK' : `\nlatency-metrics-test: FAIL (${fail})`);
process.exit(fail === 0 ? 0 : 1);
