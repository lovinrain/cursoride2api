#!/usr/bin/env node
// Verifies the recoverable-vs-fatal upstream-error classifier that lets a warm
// channel RETRY a transient Cursor blip in-place (failOrRetry) instead of being
// torn down. Uses the exact error strings/codes seen in real channel-death
// tombstones. Recoverable → retry (channel survives). Fatal → teardown.
const assert = require('node:assert/strict');
const { isRecoverableUpstreamError, isFatalUpstreamFault, isRetryableTransientType } = require('../../src/cursor-agent-h1');

let fail = 0;
const ok = (name, cond) => { if (cond) { console.log('  ✓ ' + name); } else { console.log('  ✗ ' + name); fail++; } };

console.log('=== RECOVERABLE (retry the turn on a fresh stream, keep the channel) ===');
const recoverable = [
  ['RunSSE 502 Bad Gateway', 'RunSSE non-200: 502 <html>\r\n<head><title>502 Bad Gateway</title></head>', 'HTTP_502'],
  ['RunSSE 503 Service Unavailable', 'RunSSE non-200: 503 {"error":"Service Unavailable"}', 'HTTP_503'],
  ['RunSSE 504', 'RunSSE non-200: 504 gateway timeout', 'HTTP_504'],
  ['Response error: aborted', 'Response error: aborted', 'ERR_STREAM'],
  ['BidiAppend returned 503', 'BidiAppend seqno=0 returned 503: {"error":"Service Unavailable"}', 'ERR_BIDI_APPEND_503'],
  ['BidiAppend fetch error', 'BidiAppend seqno=0 fetch error: BidiAppend request failed', 'ERR_BIDI_APPEND'],
  ['RunSSE response error (socket)', 'RunSSE response error: socket hang up', 'ERR_RES'],
  ['Request error ECONNRESET', 'Request error: read ECONNRESET', 'ERR_REQ'],
];
for (const [name, msg, code] of recoverable) ok(name + ' → recoverable', isRecoverableUpstreamError(msg, code) === true);

console.log('=== FATAL (do NOT retry — needs a different token / real backoff → teardown) ===');
const fatal = [
  ['rate limit (soft)', "Connect error resource_exhausted: You've reached the rate limit. Please wait a bit", 'HTTP_429'],
  ['rate limit via dashboard', 'Connect error resource_exhausted: Visit [cursor.com/dashboard]', 'ERR_STREAM'],
  ['quota exhausted', 'Connect error resource_exhausted: Switched to composer-2.5 after reaching API limit [ERROR_RATE_LIMITED_CHANGEABLE]', 'HTTP_503'],
  ['auth / not logged in', 'Connect error unauthenticated: ... [ERROR_NOT_LOGGED_IN]', 'ERR_STREAM'],
  ['BidiAppend 401 unauthenticated', 'BidiAppend seqno=0 returned 401: {"code":"unauthenticated"}', 'ERR_BIDI_APPEND_401'],
  ['too many computers', 'Connect error resource_exhausted: Too many computers used within the last 24 hours', 'ERR_STREAM'],
];
for (const [name, msg, code] of fatal) {
  ok(name + ' → NOT recoverable', isRecoverableUpstreamError(msg, code) === false);
  ok(name + ' → flagged fatal', isFatalUpstreamFault(msg) === true);
}

console.log('=== a rate-limit body wins even under a 5xx code (fatal guard first) ===');
ok('503 code but resource_exhausted body → fatal', isRecoverableUpstreamError('resource_exhausted: rate limit', 'HTTP_503') === false);

console.log('=== unrelated / non-transient → not classified as recoverable-upstream ===');
ok('unexpected_turn_ended → not recoverable (handled elsewhere)', isRecoverableUpstreamError('unexpected_turn_ended', 'ERR_TURN') === false);
ok('plain empty → not recoverable', isRecoverableUpstreamError('', '') === false);

console.log('=== isRetryableTransientType (labels a death: transport OR recoverable-upstream, not fatal) ===');
ok('NGHTTP2 refused → retryable type', isRetryableTransientType('NGHTTP2_REFUSED_STREAM', 'ERR_HTTP2_STREAM_ERROR') === true);
ok('socket reset → retryable type', isRetryableTransientType('read ECONNRESET', 'ERR_REQ') === true);
ok('RunSSE 503 → retryable type', isRetryableTransientType('RunSSE non-200: 503', 'HTTP_503') === true);
ok('rate limit → NOT retryable type (fatal)', isRetryableTransientType("resource_exhausted: rate limit", 'HTTP_429') === false);
ok('auth → NOT retryable type (fatal)', isRetryableTransientType('ERROR_NOT_LOGGED_IN', 'ERR_STREAM') === false);

console.log(fail === 0 ? '\nrecoverable-upstream-test: OK' : `\nrecoverable-upstream-test: FAIL (${fail})`);
process.exit(fail === 0 ? 0 : 1);
