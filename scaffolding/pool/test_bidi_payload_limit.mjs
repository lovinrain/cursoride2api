#!/usr/bin/env node
// test_bidi_payload_limit.mjs
//
// Probe Cursor's bidi-stream payload-size limit empirically.
//
// For each target payload size in PROBE_SIZES_KB, send a /v1/messages
// request with a user message padded to that size, record outcome,
// repeat REPS times. The proxy is expected to be in POOL_CONTEXT_MODE=full
// so no truncation guard interferes — every byte we put on the wire is
// what the inner Cursor model receives.
//
// Outcomes:
//   ok            — message_start AND message_stop both arrived
//   error_sse     — `event: error` SSE block received
//   stalled       — message_start arrived but no message_stop within TIMEOUT_MS
//   no_response   — nothing from server before TIMEOUT_MS
//   http_error    — non-200 HTTP response
//   network_error — fetch threw
//
// CSV columns: ts,size_bytes,attempt,model,outcome,duration_ms,
//              time_to_first_byte_ms,message_start_at_ms,message_stop_at_ms,
//              error_reason,bytes_received
//
// Usage:
//   node scaffolding/pool/test_bidi_payload_limit.mjs
//   API_URL=http://localhost:4242 node test_bidi_payload_limit.mjs
//   PROBE_SIZES_KB=32,64,96,128 REPS=3 node test_bidi_payload_limit.mjs

import fs from 'node:fs';

const API = process.env.API_URL || 'http://127.0.0.1:4242';
const MODEL = process.env.MODEL || 'claude-4.6-opus-max-thinking-fast';
const REPS = parseInt(process.env.REPS || '2', 10);
const TIMEOUT_MS = parseInt(process.env.PROBE_TIMEOUT_MS || '90000', 10);
const INTER_PROBE_MS = parseInt(process.env.PROBE_DELAY_MS || '5000', 10);
const DEFAULT_SIZES_KB = '32,64,96,128,192,256,384,512';
const SIZES_KB = (process.env.PROBE_SIZES_KB || DEFAULT_SIZES_KB)
  .split(',')
  .map((s) => parseInt(s.trim(), 10))
  .filter((n) => Number.isFinite(n) && n > 0);

const ts = new Date().toISOString().replace(/[:.]/g, '-');
const csvPath = `/tmp/bidi-probe-${ts}.csv`;
const csvHeader = [
  'ts', 'size_bytes', 'attempt', 'model', 'outcome', 'duration_ms',
  'time_to_first_byte_ms', 'message_start_at_ms', 'message_stop_at_ms',
  'error_reason', 'bytes_received',
].join(',');
fs.writeFileSync(csvPath, csvHeader + '\n');

function appendCsv(row) {
  const line = [
    row.ts,
    row.size_bytes,
    row.attempt,
    row.model,
    row.outcome,
    row.duration_ms,
    row.time_to_first_byte_ms ?? '',
    row.message_start_at_ms ?? '',
    row.message_stop_at_ms ?? '',
    JSON.stringify(row.error_reason ?? '').replace(/^"|"$/g, '').replace(/,/g, ';'),
    row.bytes_received,
  ].join(',');
  fs.appendFileSync(csvPath, line + '\n');
}

function buildPayload(sizeBytes) {
  // Reserve some characters for the natural prompt prefix.
  const prefix =
    "Reply with just the single word OK and nothing else.\n\n" +
    "Below is filler content to test payload-size handling. " +
    "Please ignore it entirely:\n\n";
  const fillerSize = Math.max(0, sizeBytes - prefix.length);
  const filler = 'A'.repeat(fillerSize);
  return {
    model: MODEL,
    max_tokens: 16,
    stream: true,
    messages: [{ role: 'user', content: prefix + filler }],
  };
}

async function probe(sizeBytes, attempt) {
  const probeStart = Date.now();
  const body = buildPayload(sizeBytes);
  const actualBytes = JSON.stringify(body).length;

  let messageStartAt = null;
  let messageStopAt = null;
  let firstByteAt = null;
  let bytesReceived = 0;
  let errorReason = '';
  let outcome = 'pending';

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), TIMEOUT_MS);

  try {
    const res = await fetch(`${API}/v1/messages`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-probe-id': `bidi-probe-${sizeBytes}-${attempt}-${Date.now()}`,
      },
      body: JSON.stringify(body),
      signal: controller.signal,
    });

    if (!res.ok) {
      clearTimeout(timeoutId);
      const text = await res.text().catch(() => '');
      return {
        ts: new Date().toISOString(),
        size_bytes: actualBytes,
        attempt,
        model: MODEL,
        outcome: 'http_error',
        duration_ms: Date.now() - probeStart,
        time_to_first_byte_ms: null,
        message_start_at_ms: null,
        message_stop_at_ms: null,
        error_reason: `HTTP ${res.status} ${text.slice(0, 200)}`,
        bytes_received: text.length,
      };
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (firstByteAt === null) firstByteAt = Date.now() - probeStart;
      buf += decoder.decode(value, { stream: true });
      bytesReceived += value.length;

      // Parse SSE: blank line terminates an event
      let parts = buf.split('\n\n');
      buf = parts.pop() || '';
      for (const part of parts) {
        const eventMatch = part.match(/event:\s*([^\n]+)/);
        const dataMatch = part.match(/data:\s*([^\n]+)/);
        if (!eventMatch) continue;
        const eventName = eventMatch[1].trim();
        if (eventName === 'message_start' && messageStartAt === null) {
          messageStartAt = Date.now() - probeStart;
        } else if (eventName === 'message_stop' && messageStopAt === null) {
          messageStopAt = Date.now() - probeStart;
          outcome = 'ok';
        } else if (eventName === 'error') {
          outcome = 'error_sse';
          errorReason = (dataMatch?.[1] || '').slice(0, 200);
        }
      }
    }

    clearTimeout(timeoutId);
    if (outcome === 'pending') {
      // Stream closed without message_stop and without explicit error
      outcome = messageStartAt !== null ? 'stalled' : 'no_response';
      errorReason = errorReason || `stream ended without message_stop (bytes=${bytesReceived})`;
    }

    return {
      ts: new Date().toISOString(),
      size_bytes: actualBytes,
      attempt,
      model: MODEL,
      outcome,
      duration_ms: Date.now() - probeStart,
      time_to_first_byte_ms: firstByteAt,
      message_start_at_ms: messageStartAt,
      message_stop_at_ms: messageStopAt,
      error_reason: errorReason,
      bytes_received: bytesReceived,
    };
  } catch (e) {
    clearTimeout(timeoutId);
    const aborted = e.name === 'AbortError';
    return {
      ts: new Date().toISOString(),
      size_bytes: actualBytes,
      attempt,
      model: MODEL,
      outcome: aborted ? (messageStartAt !== null ? 'stalled' : 'no_response') : 'network_error',
      duration_ms: Date.now() - probeStart,
      time_to_first_byte_ms: firstByteAt,
      message_start_at_ms: messageStartAt,
      message_stop_at_ms: messageStopAt,
      error_reason: aborted ? `timeout after ${TIMEOUT_MS}ms` : (e.message || String(e)),
      bytes_received: bytesReceived,
    };
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

(async () => {
  console.log(`# bidi-stream payload limit probe`);
  console.log(`# api=${API} model=${MODEL} reps=${REPS} timeout=${TIMEOUT_MS}ms inter_probe=${INTER_PROBE_MS}ms`);
  console.log(`# sizes_kb=[${SIZES_KB.join(', ')}]`);
  console.log(`# csv=${csvPath}`);
  console.log('');
  console.log(`size_kb\tattempt\toutcome\tduration_ms\tttfb_ms\terror_reason`);

  const results = [];
  for (const sizeKb of SIZES_KB) {
    const sizeBytes = sizeKb * 1024;
    for (let attempt = 1; attempt <= REPS; attempt++) {
      const r = await probe(sizeBytes, attempt);
      results.push(r);
      appendCsv(r);
      const summary = `${sizeKb}\t${attempt}\t${r.outcome}\t${r.duration_ms}\t${r.time_to_first_byte_ms ?? ''}\t${(r.error_reason || '').slice(0, 80)}`;
      console.log(summary);
      if (attempt < REPS || sizeKb !== SIZES_KB[SIZES_KB.length - 1]) {
        await sleep(INTER_PROBE_MS);
      }
    }
  }

  console.log('');
  console.log(`# === Summary by size ===`);
  console.log(`size_kb\tn\tok\tstalled\terror_sse\thttp_err\tnet_err\tno_resp\tok_rate`);
  const grouped = new Map();
  for (const r of results) {
    const kb = Math.round(r.size_bytes / 1024);
    if (!grouped.has(kb)) grouped.set(kb, []);
    grouped.get(kb).push(r);
  }
  const sizes = [...grouped.keys()].sort((a, b) => a - b);
  for (const kb of sizes) {
    const arr = grouped.get(kb);
    const counts = { ok: 0, stalled: 0, error_sse: 0, http_error: 0, network_error: 0, no_response: 0 };
    for (const r of arr) counts[r.outcome] = (counts[r.outcome] || 0) + 1;
    const okRate = (counts.ok / arr.length * 100).toFixed(0);
    console.log(`${kb}\t${arr.length}\t${counts.ok}\t${counts.stalled}\t${counts.error_sse}\t${counts.http_error}\t${counts.network_error}\t${counts.no_response}\t${okRate}%`);
  }
  console.log(`# csv saved to ${csvPath}`);
})();
