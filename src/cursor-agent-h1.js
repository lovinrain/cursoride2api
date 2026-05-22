// ═══════════════════════════════════════════════
//  CursorIDE2API - Cursor Agent Protocol Client (HTTP/1.1)
//  BidiAppend + RunSSE pair, application/connect+proto
// ═══════════════════════════════════════════════
//
//  Drop-in replacement for src/cursor-agent.js that uses Cursor's
//  HTTP/1.1-compatible BiDi-emulator path:
//
//    • client→server messages: POST /aiserver.v1.BidiService/BidiAppend
//      (unary, one per AgentClientMessage, monotonically increasing
//       append_seqno, all sharing the same x-request-id UUID)
//    • server→client stream:   POST /agent.v1.AgentService/RunSSE
//      (server-streaming, Connect-Web envelopes inside chunked HTTP/1.1)
//
//  See scaffolding/pool/RUNSSE.md for the reverse-engineered protocol
//  notes. The HTTP/1.1 path bypasses the ALB's per-target-group HTTP/2
//  rate-limit on /agent.v1.AgentService/Run, which today caps the
//  RATLC pool at ~2-3 channels before tripping
//  ERROR_PRO_USER_RATE_LIMIT_EXCEEDED.
//
//  Public surface MATCHES src/cursor-agent.js — bridge-worker.mjs reads
//  the same {sendToolResult, setCallbacks, setTools, close, getStats}
//  methods. Switch protocols by importing this module instead.
//
//  Transport:
//    • RunSSE: Node's `https.request` (NOT fetch). We need streaming
//      response body reads and the ability to detect socket-level
//      disconnects mid-stream. fetch's WHATWG ReadableStream loses this.
//    • BidiAppend: Node's `https.request`, also pinned to HTTP/1.1 so the
//      whole service→Cursor path is auditable as H1-only.
//
//  Shared with cursor-agent.js (imported below):
//    buildMcpToolDefinitions, handleExecMessage, handleKvMessage,
//    handleInteractionQuery, sendExecClientMessage, sendKvResponse,
//    frameConnectMessage, decodeMcpArgs, loadProto

const https = require('node:https');
const crypto = require('node:crypto');
const { URL } = require('node:url');
const { v4: uuidv4 } = require('./uuid');
const config = require('./config');
const { generateChecksum } = require('./cursor-client');
const stallThresholds = require('./stall-thresholds');
const {
  loadProto,
  buildMcpToolDefinitions,
  handleExecMessage,
  handleKvMessage,
  handleInteractionQuery,
  extractWebSearchServerToolEvent,
  sendExecClientMessage,
  sendExecClientControlMessage,
  sendExecClientMessageAndClose,
  sendKvResponse,
  frameConnectMessage,
  buildNativeReadResult,
  buildNativeWriteResult,
  buildNativeDeleteResult,
  buildNativeGrepResult,
  sendForwardCompatibleSubagentResult,
  buildSelectedContextForImages,
  resolveClientFingerprint,
} = require('./cursor-agent');

// Connect-protocol "end stream" frame flag
const CONNECT_END_STREAM_FLAG = 0b00000010;

// ── Module-load: kick off proto load so the first startConversation()
//   call is fast. cursor-agent.js already pre-warms, but if this module
//   is imported standalone it doesn't hurt to nudge again.
loadProto().catch((e) => {
  console.error(`[cursor-agent-h1] proto load failed: ${e.message}`);
});

// ── Cursor backend constants ──
const _baseUrl = config.cursor.baseUrl;
const _baseUrlParsed = new URL(_baseUrl);
const _host = _baseUrlParsed.hostname;
const _port = _baseUrlParsed.port ? parseInt(_baseUrlParsed.port, 10) : 443;

// ── Header fingerprint (mirror cursor-agent.js exactly) ──
const _clientType = process.env.CURSOR_CLIENT_TYPE || 'ide';
const _clientDevice = process.env.CURSOR_CLIENT_DEVICE_TYPE || 'desktop';
const _clientCommit = process.env.CURSOR_COMMIT || 'd5c0e77a0214208f36b56d42e8e787de88d02ea4';
// OS/arch/version are resolved per-token via cursor-agent.js so a Mac-minted
// token sent from a non-Mac host claims darwin instead of leaking the real
// platform — Cursor's anti-abuse gate rejects mismatched bundles.
const _cursorTimezone = (() => {
  try { return Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC'; }
  catch { return 'UTC'; }
})();

// Headers shared across BidiAppend and RunSSE. The x-request-id UUID
// joins the two halves into one logical BiDi stream on the server side
// — same UUID for the RunSSE open AND every BidiAppend in that stream.
function _commonHeaders(token, requestId, sessionId) {
  const fp = resolveClientFingerprint(token);
  return {
    'authorization': `Bearer ${token.accessToken}`,
    'x-cursor-checksum': generateChecksum(token.machineId || '', token.macMachineId || ''),
    'x-cursor-client-version': config.cursor.clientVersion,
    'x-cursor-timezone': _cursorTimezone,
    'x-request-id': requestId,
    'x-session-id': sessionId,
    'x-ghost-mode': 'false',
    'x-cursor-client-type': _clientType,
    'x-cursor-client-os': fp.clientOs,
    'x-cursor-client-arch': fp.clientArch,
    'x-cursor-client-device-type': _clientDevice,
    'x-cursor-client-os-version': fp.clientOsVersion,
    'x-cursor-commit': _clientCommit,
    'x-cursor-streaming': 'true',
    'connect-protocol-version': '1',
  };
}

// ── Connect-Web envelope helper: wrap a binary BidiRequestId proto for
//   the RunSSE request body. Connect-Web client-stream framing is
//   [flags(1)][len(4 BE)][payload(len)] just like the response side.
function _wrapConnectRequest(payload) {
  const buf = Buffer.alloc(5 + payload.length);
  buf[0] = 0;
  buf.writeUInt32BE(payload.length, 1);
  if (payload.length > 0) Buffer.from(payload).copy(buf, 5);
  return buf;
}

function httpsRequestText(opts, body) {
  return new Promise((resolve, reject) => {
    const req = https.request(opts, (res) => {
      let text = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => { text += chunk; });
      res.on('end', () => resolve({
        statusCode: res.statusCode || 0,
        statusMessage: res.statusMessage || '',
        headers: res.headers || {},
        text,
      }));
      res.on('error', reject);
    });
    req.setTimeout(config.cursor.requestTimeout, () => {
      req.destroy(new Error('BidiAppend request timeout'));
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

// ── Deterministic conversation UUID (same as cursor-agent.js) ──
function deterministicConversationId(convKey) {
  const hex = crypto.createHash('sha256')
    .update(`cursor-conv-id:${convKey}`)
    .digest('hex')
    .slice(0, 32);
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    `4${hex.slice(13, 16)}`,
    `${(0x8 | (parseInt(hex[16], 16) & 0x3)).toString(16)}${hex.slice(17, 20)}`,
    hex.slice(20, 32),
  ].join('-');
}

// ═══════════════════════════════════════════════
//  startConversation (H1) — mirrors src/cursor-agent.js
// ═══════════════════════════════════════════════

function startConversation(token, options = {}) {
  const {
    prompt = '',
    modelId,
    conversationId = uuidv4(),
    conversationState = null,
    tools = [],
    sessionId = uuidv4(),
    onTextDelta,
    onThinkingDelta,
    onThinkingCompleted,
    onMcpCall,
    onServerToolUse,
    onStepCompleted,
    onTurnEnded,
    onError,
  } = options;

  const currentCallbacks = {
    onTextDelta: onTextDelta || (() => {}),
    onThinkingDelta: onThinkingDelta || (() => {}),
    onThinkingCompleted: onThinkingCompleted || (() => {}),
    onMcpCall: onMcpCall || (() => {}),
    onServerToolUse: onServerToolUse || (() => {}),
    onStepCompleted: onStepCompleted || (() => {}),
    onTurnEnded: onTurnEnded || (() => {}),
    onError: onError || (() => {}),
  };

  function setCallbacks(newCallbacks) {
    if (!newCallbacks || typeof newCallbacks !== 'object') return;
    for (const k of ['onTextDelta', 'onThinkingDelta', 'onThinkingCompleted', 'onMcpCall', 'onServerToolUse', 'onStepCompleted', 'onTurnEnded', 'onError']) {
      if (typeof newCallbacks[k] === 'function') {
        currentCallbacks[k] = newCallbacks[k];
      }
    }
  }

  console.log(
    `[cursor-agent-h1] new conv id=${conversationId} model=${modelId} ` +
    `hasState=${!!conversationState} tools=${(tools || []).length}`
  );

  // Each H1 channel needs its own request_id UUID — that's what joins
  // BidiAppend ↔ RunSSE on the server. New session = new request_id.
  const requestId = uuidv4();

  // ── State ──────────────────────────────────────────────────────────
  let sseReq = null;         // https.ClientRequest for the streaming response
  let sseRes = null;         // https.IncomingMessage for the streaming response
  let closed = false;
  let connectionStarted = false;
  const pendingWrites = [];  // Uint8Arrays queued before proto load completes
  let appendSeqno = 0n;      // BidiAppend.append_seqno; monotonically increases
  let appendInFlight = Promise.resolve();  // serializes BidiAppend POSTs
  let buffer = Buffer.alloc(0);
  let inputTokens = 0;
  let outputTokens = 0;
  let capturedState = null;
  let turnEndedFired = false;
  const blobStore = new Map();
  const _nativeExecKinds = new Map();

  // Telemetry (mirrors cursor-agent.js for stats compatibility)
  let streamOpenedAt = 0;
  let streamBytesIn = 0;
  let streamBytesOut = 0;
  let streamMcpCallCount = 0;
  let streamSummaryEmitted = false;
  let lastUsefulFrameAt = 0;
  let maxIdleMs = 0;
  let _turnRetries = 0;
  let _turnTransportErrors = 0;
  let _turnStalls = 0;
  let _turnCascadeDetected = false;
  let _turnTextDeltaCount = 0;
  let _turnThinkingDeltaCount = 0;
  let _bytesInAtLastUsefulFrame = 0;
  let watchdog = null;
  let heartbeat = null;
  let retryAttempts = 0;
  let hasReceivedData = false;
  let hasEmittedContent = false;
  let lastErrorWasTransport = false;
  const MAX_REQUEST_RETRIES = 5;
  let cachedInitialEncoded = null;

  // Stall thresholds (per-model, adaptive) — same module as cursor-agent.js
  let _stallPreMs = 0;
  let _stallPostMs = 0;
  let _stallSource = 'baseline';
  function _recomputeStallThresholds() {
    const t = stallThresholds.getThreshold(modelId);
    _stallPreMs = t.pre;
    _stallPostMs = t.post;
    _stallSource = t.source;
  }
  function markUsefulFrame() {
    const now = Date.now();
    if (lastUsefulFrameAt > 0) {
      const idle = now - lastUsefulFrameAt;
      if (idle > maxIdleMs) maxIdleMs = idle;
    }
    lastUsefulFrameAt = now;
    _bytesInAtLastUsefulFrame = streamBytesIn;
  }
  function dumpStreamSummary(reason, code) {
    if (streamSummaryEmitted) return;
    streamSummaryEmitted = true;
    const ageMs = streamOpenedAt ? (Date.now() - streamOpenedAt) : -1;
    console.log(
      `  📊 stream-summary-h1 ` +
      `code=${code || 'none'} ` +
      `ageMs=${ageMs} ` +
      `bytesIn=${streamBytesIn} ` +
      `bytesOut=${streamBytesOut} ` +
      `mcpCalls=${streamMcpCallCount} ` +
      `hasContent=${hasEmittedContent} ` +
      `retries=${retryAttempts} ` +
      `model=${modelId} ` +
      `sid=${(sessionId || '').slice(0, 8)} ` +
      `rid=${requestId.slice(0, 8)} ` +
      `reason="${(reason || '').slice(0, 80)}"`
    );
  }

  function fail(msg) {
    if (closed) return;
    dumpStreamSummary(msg, 'fail');
    currentCallbacks.onError(msg);
    close();
  }

  function close() {
    if (closed) return;
    closed = true;
    if (heartbeat) { try { clearInterval(heartbeat); } catch { /* ignore */ } heartbeat = null; }
    if (watchdog) { try { clearInterval(watchdog); } catch { /* ignore */ } watchdog = null; }
    try { if (sseRes) sseRes.destroy(); } catch { /* ignore */ }
    try { if (sseReq) sseReq.destroy(); } catch { /* ignore */ }
  }

  // ── BidiAppend: unary POST sending one AgentClientMessage at a time ──
  // We serialize sends through `appendInFlight` so seqnos are strictly
  // monotonic on the wire (the bundle uses Bigint++ in the same way).
  function sendBinaryFrame(payload) {
    if (closed) return;
    // The H1 path is NOT framed at the wire (BidiAppend is unary, not
    // streaming). The hex of the raw binary AgentClientMessage IS the
    // BidiAppend.data field. So `payload` here is the bare proto bytes,
    // and we hex-encode it for the POST body.
    const dataHex = Buffer.from(payload).toString('hex');
    streamBytesOut += payload.length;
    const seqno = appendSeqno++;

    // Serialize on the in-flight chain so the network requests fire
    // in seqno order. If a previous append failed, we still queue ours
    // — the server will mismatch and end the stream, which our SSE
    // reader will surface via end-stream envelope as expected.
    appendInFlight = appendInFlight.then(() => _doBidiAppend(seqno, dataHex));
  }

  async function _doBidiAppend(seqno, dataHex) {
    if (closed) return;
    try {
      const headers = {
        ..._commonHeaders(token, requestId, sessionId),
        'content-type': 'application/json',
      };
      const body = JSON.stringify({
        data: dataHex,
        request_id: { request_id: requestId },
        // proto int64 → Connect-JSON encodes as string
        append_seqno: String(seqno),
      });
      headers['content-length'] = String(Buffer.byteLength(body));
      const res = await httpsRequestText({
        method: 'POST',
        host: _host,
        port: _port,
        path: '/aiserver.v1.BidiService/BidiAppend',
        headers,
        ALPNProtocols: ['http/1.1'],
      }, body);
      if (res.statusCode < 200 || res.statusCode >= 300) {
        const text = res.text || '';
        // Always log — silent failures cause the channel to hang
        // because the SSE side keeps producing heartbeats while the
        // conversation can't progress.
        console.log(`[cursor-agent-h1] BidiAppend FAIL seqno=${seqno} status=${res.statusCode} body=${text.slice(0, 256)}`);
        // Surface to the bridge so it can either retry (if no content
        // has been emitted yet — initial runRequest case) or fail-fast
        // (mid-conversation case). failOrRetry's hasEmittedContent
        // gate handles both.
        if (_protoCached) {
          failOrRetry(_protoCached, `BidiAppend seqno=${seqno} returned ${res.statusCode}: ${text.slice(0, 200)}`, `ERR_BIDI_APPEND_${res.statusCode}`);
        } else {
          fail(`BidiAppend seqno=${seqno} returned ${res.statusCode} before stream opened`);
        }
        return;
      }
      // Always log success too — when seeing 200s but no stream progress
      // we need to know the server got the payload but didn't act on it.
      console.log(`[cursor-agent-h1] BidiAppend OK seqno=${seqno} body=${String(res.text || '').slice(0, 64)}`);
    } catch (e) {
      console.log(`[cursor-agent-h1] BidiAppend EXCEPTION seqno=${seqno} error: ${e.message}`);
      if (_protoCached) {
        failOrRetry(_protoCached, `BidiAppend seqno=${seqno} fetch error: ${e.message}`, 'ERR_BIDI_APPEND');
      } else {
        fail(`BidiAppend seqno=${seqno} fetch error before stream opened: ${e.message}`);
      }
    }
  }

  // ── sendToolResult — same surface as cursor-agent.js ──
  // Builds either an mcpResult or a native result (shell/read/write/etc.)
  // and dispatches via sendExecClientMessage → sendBinaryFrame.
  // (Logic is identical to cursor-agent.js; lifted here so it works with
  // our H1 sendBinaryFrame closure.)
  function sendToolResult(id, execId, content) {
    if (closed) return;
    const _kind = _nativeExecKinds.get(execId) || 'mcp';
    const _contentSize = typeof content === 'string' ? content.length
      : (content == null ? 0 : JSON.stringify(content).length);
    const _idDesc = id == null ? 'null'
      : (id instanceof Uint8Array ? `bytes[${id.length}]=${Buffer.from(id).toString('hex').slice(0, 16)}`
        : typeof id === 'string' ? `str("${id.slice(0, 16)}")`
        : `${typeof id}=${String(id).slice(0, 32)}`);
    console.log(`[cursor-agent-h1] sendToolResult id=${_idDesc} execId="${String(execId || '').slice(0, 16)}" kind=${_kind} contentSize=${_contentSize}`);
    turnEndedFired = false;
    lastUsefulFrameAt = Date.now();
    maxIdleMs = 0;
    _turnRetries = 0;
    _turnTransportErrors = 0;
    _turnStalls = 0;
    _turnCascadeDetected = false;
    _turnTextDeltaCount = 0;
    _turnThinkingDeltaCount = 0;
    _bytesInAtLastUsefulFrame = streamBytesIn;
    const proto = _protoRequire();
    const { create, agent } = proto;

    function buildContentItems(items) {
      const out = [];
      for (const it of items || []) {
        if (!it) continue;
        let imageData = it.data;
        if ((!imageData || imageData.length === 0) && typeof it.dataBase64 === 'string' && it.dataBase64) {
          try { imageData = Buffer.from(it.dataBase64, 'base64'); } catch { imageData = null; }
        }
        if (it.kind === 'image' && imageData && imageData.length > 0) {
          out.push(create(agent.McpToolResultContentItemSchema, {
            content: {
              case: 'image',
              value: create(agent.McpImageContentSchema, {
                mimeType: it.mediaType || 'image/png',
                data: imageData instanceof Uint8Array ? imageData : new Uint8Array(imageData),
              }),
            },
          }));
        } else if (it.kind === 'text' && it.text) {
          out.push(create(agent.McpToolResultContentItemSchema, {
            content: { case: 'text', value: create(agent.McpTextContentSchema, { text: it.text }) },
          }));
        }
      }
      if (out.length === 0) {
        out.push(create(agent.McpToolResultContentItemSchema, {
          content: { case: 'text', value: create(agent.McpTextContentSchema, { text: '' }) },
        }));
      }
      return out;
    }

    // Native passthrough dispatch — mirrors src/cursor-agent.js. nativeKindRaw
    // can be either a string (legacy) or {kind, path?, url?} so we can echo
    // back the original tool args (path/url) instead of an empty string.
    const nativeKindRaw = _nativeExecKinds.get(execId);
    if (nativeKindRaw) {
      _nativeExecKinds.delete(execId);
      const nativeKind = typeof nativeKindRaw === 'string' ? nativeKindRaw : nativeKindRaw.kind;
      const nativePath = typeof nativeKindRaw === 'string' ? '' : (nativeKindRaw.path || '');
      const nativeUrl = typeof nativeKindRaw === 'string' ? '' : (nativeKindRaw.url || '');
      let text;
      if (typeof content === 'string') text = content;
      else if (content && Array.isArray(content.items)) {
        text = content.items.filter((i) => i?.kind === 'text').map((i) => i.text || '').join('\n');
      } else if (content && typeof content === 'object' && content.error) {
        text = `[tool_error] ${String(content.error)}`;
      } else if (content == null) {
        text = '';
      } else {
        text = JSON.stringify(content);
      }

      if (nativeKind === 'shell') {
        const result = create(agent.ShellResultSchema, {
          result: {
            case: 'success',
            value: create(agent.ShellSuccessSchema, {
              command: '', workingDirectory: '',
              exitCode: 0, signal: '',
              stdout: text, stderr: '',
              executionTime: 0,
            }),
          },
        });
        sendExecClientMessageAndClose(id, execId, 'shellResult', result, sendBinaryFrame);
        return;
      }
      if (nativeKind === 'shellStream') {
        // Multi-event stream. After stdout + exit, the IDE's executor loop
        // (workbench.desktop.main.js, $jb/c1c.handle) writes
        // ExecClientControlMessage(streamClose{id}) — without it Cursor's
        // backend keeps the exec stream "open" and the model stalls
        // mid-second-tool-call (see scaffolding/pool/TOOL_USE_HANG_FINDINGS.md).
        const stdoutEvt = create(agent.ShellStreamSchema, {
          event: { case: 'stdout', value: create(agent.ShellStreamStdoutSchema, { data: text }) },
        });
        sendExecClientMessage(id, execId, 'shellStream', stdoutEvt, sendBinaryFrame);
        const exitEvt = create(agent.ShellStreamSchema, {
          event: { case: 'exit', value: create(agent.ShellStreamExitSchema, { code: 0, cwd: '', aborted: false }) },
        });
        sendExecClientMessage(id, execId, 'shellStream', exitEvt, sendBinaryFrame);
        sendExecClientControlMessage(id, 'streamClose', sendBinaryFrame);
        return;
      }
      if (nativeKind === 'backgroundShell') {
        const sid = (Math.random().toString(36).slice(2, 10));
        const result = create(agent.BackgroundShellSpawnResultSchema, {
          result: {
            case: 'success',
            value: create(agent.BackgroundShellSpawnSuccessSchema, {
              shellId: sid, command: '', workingDirectory: '', pid: 0,
            }),
          },
        });
        sendExecClientMessageAndClose(id, execId, 'backgroundShellSpawnResult', result, sendBinaryFrame);
        return;
      }
      if (nativeKind === 'read') {
        const result = buildNativeReadResult(create, agent, nativePath, text);
        sendExecClientMessageAndClose(id, execId, 'readResult', result, sendBinaryFrame);
        return;
      }
      if (nativeKind === 'write') {
        const result = buildNativeWriteResult(create, agent, nativeKindRaw, text);
        sendExecClientMessageAndClose(id, execId, 'writeResult', result, sendBinaryFrame);
        return;
      }
      if (nativeKind === 'delete') {
        const result = buildNativeDeleteResult(create, agent, nativeKindRaw, text);
        sendExecClientMessageAndClose(id, execId, 'deleteResult', result, sendBinaryFrame);
        return;
      }
      if (nativeKind === 'fetch') {
        const result = create(agent.FetchResultSchema, {
          result: {
            case: 'success',
            value: create(agent.FetchSuccessSchema, {
              url: nativeUrl, content: text,
              statusCode: 200, contentType: 'text/plain',
            }),
          },
        });
        sendExecClientMessageAndClose(id, execId, 'fetchResult', result, sendBinaryFrame);
        return;
      }
      if (nativeKind === 'grep') {
        const result = buildNativeGrepResult(create, agent, nativeKindRaw, text);
        sendExecClientMessageAndClose(id, execId, 'grepResult', result, sendBinaryFrame);
        return;
      }
      if (nativeKind === 'subagent') {
        sendForwardCompatibleSubagentResult(id, execId, content, sendBinaryFrame);
        return;
      }
    }

    let mcpResult;
    let summary = 'ok';
    if (content && typeof content === 'object' && content.error) {
      mcpResult = create(agent.McpResultSchema, {
        result: { case: 'error', value: create(agent.McpErrorSchema, { error: String(content.error) }) },
      });
      summary = `error: ${String(content.error).slice(0, 60)}`;
    } else if (content && typeof content === 'object' && Array.isArray(content.items)) {
      const items = buildContentItems(content.items);
      mcpResult = create(agent.McpResultSchema, {
        result: {
          case: 'success',
          value: create(agent.McpSuccessSchema, { content: items, isError: false }),
        },
      });
      const textN = content.items.filter(i => i.kind === 'text').length;
      const imageN = content.items.filter(i => i.kind === 'image').length;
      summary = `text=${textN} image=${imageN}`;
    } else {
      const text = typeof content === 'string' ? content
        : (content == null ? '' : JSON.stringify(content));
      mcpResult = create(agent.McpResultSchema, {
        result: {
          case: 'success',
          value: create(agent.McpSuccessSchema, {
            content: [
              create(agent.McpToolResultContentItemSchema, {
                content: { case: 'text', value: create(agent.McpTextContentSchema, { text }) },
              }),
            ],
            isError: false,
          }),
        },
      });
    }
    console.log(`[cursor-agent-h1] sending tool result execId=${execId} ${summary}`);
    sendExecClientMessageAndClose(id, execId, 'mcpResult', mcpResult, sendBinaryFrame);
  }

  function buildUserMessage(create, agent, text, images) {
    const fields = {
      text: String(text || ''),
      messageId: uuidv4(),
    };
    const selectedContext = buildSelectedContextForImages(create, agent, images);
    if (selectedContext) fields.selectedContext = selectedContext;
    return create(agent.UserMessageSchema, fields);
  }

  function resetTurnStateForClientMessage() {
    turnEndedFired = false;
    lastUsefulFrameAt = Date.now();
    maxIdleMs = 0;
    _turnRetries = 0;
    _turnTransportErrors = 0;
    _turnStalls = 0;
    _turnCascadeDetected = false;
    _turnTextDeltaCount = 0;
    _turnThinkingDeltaCount = 0;
    _bytesInAtLastUsefulFrame = streamBytesIn;
  }

  function sendUserMessage(text, images) {
    if (closed) return;
    resetTurnStateForClientMessage();
    const { create, toBinary, agent } = _protoRequire();
    const userMsg = buildUserMessage(create, agent, text, images);
    const action = create(agent.ConversationActionSchema, {
      action: {
        case: 'userMessageAction',
        value: create(agent.UserMessageActionSchema, { userMessage: userMsg }),
      },
    });
    const wrapper = create(agent.AgentClientMessageSchema, {
      message: { case: 'conversationAction', value: action },
    });
    const encoded = toBinary(agent.AgentClientMessageSchema, wrapper);
    const imageCount = Array.isArray(images) ? images.length : 0;
    console.log(`[cursor-agent-h1] sending native user message textBytes=${String(text || '').length} images=${imageCount}`);
    sendBinaryFrame(encoded);
  }

  // Top-level server message dispatch (mirrors cursor-agent.js)
  function handleServerMessage(msg) {
    const msgCase = msg.message?.case;
    if (process.env.CURSOR_LOG_SERVER_MSG === '1') {
      let suffix = '';
      if (msgCase === 'interactionUpdate') suffix = `:${msg.message.value?.message?.case || '?'}`;
      else if (msgCase === 'execServerMessage') suffix = `:${msg.message.value?.message?.case || '?'}`;
      if (!suffix.includes('heartbeat')) {
        console.log(`[cursor-agent-h1 RX] msgCase=${msgCase}${suffix}`);
      }
    }

    if (msgCase === 'execServerMessage') {
      markUsefulFrame();
      const exec = msg.message.value;
      const mcpToolDefs = state.mcpToolDefs;
      handleExecMessage(exec, mcpToolDefs, sendBinaryFrame, (info) => {
        hasEmittedContent = true;
        streamMcpCallCount++;
        currentCallbacks.onMcpCall(info);
      }, {
        passthroughNativeTools: !!options.passthroughNativeTools,
        nativeExecKinds: _nativeExecKinds,
        onUnhandledExec: (info) => {
          currentCallbacks.onError(info?.detail || 'unhandled Cursor exec message');
        },
      });
      return;
    }
    if (msgCase === 'kvServerMessage') {
      markUsefulFrame();
      handleKvMessage(msg.message.value, blobStore, sendBinaryFrame);
      return;
    }
    if (msgCase === 'interactionUpdate') {
      const iu = msg.message.value;
      const iuCase = iu.message?.case;
      const iuVal = iu.message?.value;

      if (iuCase === 'heartbeat') return;
      markUsefulFrame();
      if (iuCase === 'textDelta') {
        const t = iuVal?.text || '';
        if (t) {
          hasEmittedContent = true;
          _turnTextDeltaCount++;
          currentCallbacks.onTextDelta(t);
        }
        return;
      }
      if (iuCase === 'thinkingDelta') {
        const t = iuVal?.text || '';
        if (t) {
          hasEmittedContent = true;
          _turnThinkingDeltaCount++;
          currentCallbacks.onThinkingDelta(t);
        }
        return;
      }
      if (iuCase === 'thinkingCompleted') {
        try { currentCallbacks.onThinkingCompleted(iuVal || {}); } catch { /* ignore */ }
        return;
      }
      if (iuCase === 'tokenDelta') {
        outputTokens += iuVal?.tokens || 0;
        return;
      }
      if (iuCase === 'stepStarted') return;
      if (iuCase === 'stepCompleted') {
        try { currentCallbacks.onStepCompleted && currentCallbacks.onStepCompleted(); }
        catch (e) { /* ignore */ }
        return;
      }
      if (iuCase === 'turnEnded') {
        console.log(
          `[cursor-agent-h1] turn ended in=${inputTokens} out=${outputTokens} ` +
          `state=${capturedState ? capturedState.length + 'B' : 'null'}`
        );
        turnEndedFired = true;
        try { stallThresholds.recordTurn(modelId, maxIdleMs); } catch { /* ignore */ }
        try {
          currentCallbacks.onTurnEnded({
            inputTokens, outputTokens, conversationState: capturedState,
            maxIdleMs,
            stallThresholdSource: _stallSource,
            turnRetries: _turnRetries,
            turnTransportErrors: _turnTransportErrors,
            turnStalls: _turnStalls,
            turnCascadeDetected: _turnCascadeDetected,
          });
        } catch (e) {
          console.log(`[cursor-agent-h1] onTurnEnded threw: ${e.message}`);
        }
        return;
      }
      const serverToolEvent = extractWebSearchServerToolEvent(iuCase, iuVal);
      if (serverToolEvent) {
        try { currentCallbacks.onServerToolUse(serverToolEvent); }
        catch (e) { console.log(`[cursor-agent-h1] onServerToolUse threw: ${e.message}`); }
      }
      return;
    }
    if (msgCase === 'conversationCheckpointUpdate') {
      markUsefulFrame();
      const stateStruct = msg.message.value;
      if (stateStruct?.tokenDetails) {
        const used = Number(stateStruct.tokenDetails.usedTokens || 0);
        const probeOutput = outputTokens || 0;
        inputTokens = Math.max(0, used - probeOutput);
      }
      try {
        const { toBinary, agent } = _protoRequire();
        capturedState = Buffer.from(toBinary(agent.ConversationStateStructureSchema, stateStruct));
      } catch (e) {
        if (process.env.CURSOR_AGENT_DEBUG) console.log(`[cursor-agent-h1][debug] checkpoint encode failed: ${e.message}`);
      }
      return;
    }
    if (msgCase === 'interactionQuery') {
      markUsefulFrame();
      handleInteractionQuery(msg.message.value, sendBinaryFrame, {
        passthroughNativeTools: !!options.passthroughNativeTools,
        onServerToolUse: currentCallbacks.onServerToolUse,
      });
      return;
    }
    if (msgCase === 'execServerControlMessage') {
      markUsefulFrame();
      return;
    }

    if (process.env.CURSOR_AGENT_DEBUG) {
      console.log(`[cursor-agent-h1][debug] unhandled server case=${msgCase}`);
    }
  }

  // Synchronous proto-module accessor (loadProto() pre-warms; if we beat
  // the pre-warm we hit the fall-back in startConnection).
  let _protoCached = null;
  function _protoRequire() {
    if (_protoCached) return _protoCached;
    throw new Error('proto module not loaded yet');
  }

  const state = { mcpToolDefs: [] };

  const _emptyBuf = Buffer.alloc(0);
  function parseFrames(chunk) {
    if (process.env.CURSOR_AGENT_DEBUG) console.log(`[cursor-agent-h1][debug] data chunk ${chunk.length}B`);
    const work = (buffer.length > 0) ? Buffer.concat([buffer, chunk]) : chunk;
    let offset = 0;
    while (offset + 5 <= work.length) {
      const flags = work[offset];
      const len = work.readUInt32BE(offset + 1);
      if (offset + 5 + len > work.length) break;
      const payload = work.slice(offset + 5, offset + 5 + len);
      offset += 5 + len;

      if (flags & CONNECT_END_STREAM_FLAG) {
        // Connect end-stream — payload is JSON describing trailers
        try {
          const json = JSON.parse(payload.toString('utf8'));
          if (json && json.error) {
            const code = json.error.code || 'unknown';
            let message = json.error.message || 'Unknown error';
            const dets = Array.isArray(json.error.details) ? json.error.details : [];
            for (const d of dets) {
              const debug = d && d.debug;
              if (!debug) continue;
              const innerDetail = debug.details && (debug.details.detail || debug.details.title);
              const innerErr = debug.error;
              if (innerDetail) {
                message = innerDetail + (debug.details.title && debug.details.title !== innerDetail ? ` (${debug.details.title})` : '');
                if (innerErr && innerErr !== 'ERROR_UNKNOWN') message += ` [${innerErr}]`;
                break;
              }
              if (innerErr) {
                message = `${innerErr}`;
                break;
              }
            }
            fail(`Connect error ${code}: ${message}`);
          }
        } catch (e) {
          if (process.env.CURSOR_AGENT_DEBUG) console.log(`[cursor-agent-h1][debug] end-stream parse: ${e.message}`);
        }
        continue;
      }

      try {
        const { fromBinary, agent } = _protoRequire();
        const msg = fromBinary(agent.AgentServerMessageSchema, new Uint8Array(payload));
        handleServerMessage(msg);
      } catch (e) {
        if (process.env.CURSOR_AGENT_DEBUG) console.log(`[cursor-agent-h1][debug] decode failed: ${e.message}`);
      }
    }
    buffer = (offset < work.length) ? work.slice(offset) : _emptyBuf;
  }

  // ── Open the RunSSE stream via Node's https.request ──
  function attemptConnection(proto) {
    _protoCached = proto;
    streamOpenedAt = Date.now();
    streamBytesIn = 0;
    streamBytesOut = 0;
    streamMcpCallCount = 0;
    streamSummaryEmitted = false;
    buffer = Buffer.alloc(0);

    // Build the RunSSE request body: a single Connect-Web envelope
    // wrapping BidiRequestId{request_id}. Connect-Web client-stream
    // framing is identical to the response side.
    const { create, toBinary, agent } = proto;
    const bidiReq = create(agent.BidiRequestIdSchema, { requestId });
    const bidiBytes = toBinary(agent.BidiRequestIdSchema, bidiReq);
    const body = _wrapConnectRequest(bidiBytes);

    const headers = {
      ..._commonHeaders(token, requestId, sessionId),
      // application/connect+proto matches the IDE. If the server rejects
      // with a parse error, the user can flip CURSOR_H1_CONTENT_TYPE.
      'content-type': process.env.CURSOR_H1_CONTENT_TYPE || 'application/connect+proto',
      'accept': process.env.CURSOR_H1_CONTENT_TYPE || 'application/connect+proto',
      'content-length': String(body.length),
    };

    const opts = {
      method: 'POST',
      host: _host,
      port: _port,
      path: '/agent.v1.AgentService/RunSSE',
      headers,
      // Force HTTP/1.1 at TLS ALPN as well as at the Node API layer.
      ALPNProtocols: ['http/1.1'],
    };

    sseReq = https.request(opts, (res) => {
      sseRes = res;
      if (process.env.CURSOR_AGENT_DEBUG) {
        console.log(`[cursor-agent-h1][debug] RunSSE status=${res.statusCode}`);
        for (const [k, v] of Object.entries(res.headers || {})) {
          console.log(`[cursor-agent-h1][debug]   ${k}: ${v}`);
        }
      }
      if (res.statusCode !== 200) {
        // Non-200 — drain body and surface as error
        let buf = '';
        res.on('data', (c) => { buf += c.toString('utf8'); });
        res.on('end', () => {
          failOrRetry(proto, `RunSSE non-200: ${res.statusCode} ${buf.slice(0, 256)}`, `HTTP_${res.statusCode}`);
        });
        res.on('error', (e) => failOrRetry(proto, `RunSSE response error: ${e.message}`, 'ERR_RES'));
        return;
      }

      res.on('data', (chunk) => {
        hasReceivedData = true;
        streamBytesIn += chunk.length;
        parseFrames(chunk);
      });
      res.on('end', () => {
        if (closed) return;
        if (turnEndedFired) {
          if (heartbeat) { try { clearInterval(heartbeat); } catch { /* ignore */ } heartbeat = null; }
          if (watchdog) { try { clearInterval(watchdog); } catch { /* ignore */ } watchdog = null; }
          closed = true;
          return;
        }
        // Premature end
        fail('Upstream stream ended before turnEnded');
      });
      res.on('error', (e) => failOrRetry(proto, `Response error: ${e.message}`, 'ERR_STREAM'));
      res.on('close', () => {
        if (closed || turnEndedFired) return;
        // Socket-level close without end-stream envelope
        failOrRetry(proto, 'RunSSE socket closed', 'ERR_SOCKET_CLOSED');
      });
    });
    sseReq.setTimeout(config.cursor.requestTimeout, () => {
      // setTimeout on https request fires only if NO data received for that long
      fail('RunSSE request timeout');
    });
    sseReq.on('error', (e) => failOrRetry(proto, `Request error: ${e.message}`, 'ERR_REQ'));
    sseReq.write(body);
    sseReq.end();

    // Heartbeat — sent as a clientHeartbeat AgentClientMessage via BidiAppend.
    if (heartbeat) { try { clearInterval(heartbeat); } catch { /* ignore */ } }
    heartbeat = setInterval(() => {
      if (closed) return;
      const { create, toBinary, agent } = proto;
      const hb = create(agent.AgentClientMessageSchema, {
        message: { case: 'clientHeartbeat', value: create(agent.ClientHeartbeatSchema, {}) },
      });
      sendBinaryFrame(toBinary(agent.AgentClientMessageSchema, hb));
    }, config.cursor.heartbeatInterval);

    // Stall watchdog
    lastUsefulFrameAt = Date.now();
    maxIdleMs = 0;
    _recomputeStallThresholds();
    if (watchdog) { try { clearInterval(watchdog); } catch { /* ignore */ } }
    watchdog = setInterval(() => {
      if (closed) return;
      if (turnEndedFired) return;
      const idle = Date.now() - lastUsefulFrameAt;
      const threshold = hasEmittedContent ? _stallPostMs : _stallPreMs;
      if (idle > threshold) {
        try { clearInterval(watchdog); } catch { /* ignore */ }
        watchdog = null;
        try { stallThresholds.recordStall(modelId); } catch { /* ignore */ }
        _turnStalls++;
        const m = `Upstream stalled — no progress for ${Math.round(idle / 1000)}s`;
        failOrRetry(proto, `NGHTTP2_INTERNAL_ERROR (${m})`, 'ERR_H1_STALL');
      }
    }, Math.min(15000, Math.max(5000, Math.floor(_stallPreMs / 4))));
  }

  function startConnection(proto) {
    if (connectionStarted) return;
    connectionStarted = true;
    _protoCached = proto;

    // Build mcpTools once
    state.mcpToolDefs = buildMcpToolDefinitions(tools || []);

    // Build the initial runRequest (mirrors cursor-agent.js exactly)
    const { create, toBinary, agent } = proto;
    let stateStruct;
    if (conversationState) {
      let bytes;
      if (Buffer.isBuffer(conversationState)) bytes = new Uint8Array(conversationState);
      else if (conversationState instanceof Uint8Array) bytes = conversationState;
      else if (typeof conversationState === 'string') {
        try { bytes = new Uint8Array(Buffer.from(conversationState, 'base64')); } catch { bytes = null; }
      }
      if (bytes && bytes.length > 0) {
        try {
          stateStruct = proto.fromBinary(agent.ConversationStateStructureSchema, bytes);
        } catch (e) {
          if (process.env.CURSOR_AGENT_DEBUG) console.log(`[cursor-agent-h1][debug] state decode failed: ${e.message}`);
        }
      }
    }
    if (!stateStruct) {
      stateStruct = create(agent.ConversationStateStructureSchema, {
        rootPromptMessagesJson: [],
        turns: [],
        todos: [],
        pendingToolCalls: [],
        previousWorkspaceUris: [],
        fileStates: {},
        fileStatesV2: {},
        summaryArchives: [],
        turnTimings: [],
        subagentStates: {},
        selfSummaryCount: 0,
        readPaths: [],
      });
    }

    const userMsg = buildUserMessage(create, agent, prompt, options.images);
    const action = create(agent.ConversationActionSchema, {
      action: {
        case: 'userMessageAction',
        value: create(agent.UserMessageActionSchema, { userMessage: userMsg }),
      },
    });
    const enableMaxMode = !!options.maxMode;
    const modelDetails = create(agent.ModelDetailsSchema, {
      modelId,
      displayModelId: modelId,
      displayName: modelId,
      displayNameShort: modelId,
      maxMode: enableMaxMode,
    });
    const runRequestFields = {
      conversationState: stateStruct,
      action,
      modelDetails,
      conversationId,
    };
    runRequestFields.requestedModel = create(agent.RequestedModelSchema, {
      modelId,
      maxMode: enableMaxMode,
    });
    if (options.customSystemPrompt) {
      runRequestFields.customSystemPrompt = String(options.customSystemPrompt);
    }
    const runRequest = create(agent.AgentRunRequestSchema, runRequestFields);
    const wrapper = create(agent.AgentClientMessageSchema, {
      message: { case: 'runRequest', value: runRequest },
    });
    const encoded = toBinary(agent.AgentClientMessageSchema, wrapper);
    let toolBytes = 0;
    for (const td of state.mcpToolDefs) {
      try { toolBytes += toBinary(agent.McpToolDefinitionSchema, td).length; } catch { /* ignore */ }
    }
    console.log(
      `[cursor-agent-h1] runRequest tools=${state.mcpToolDefs.length} ` +
      `toolBytes=${toolBytes} totalBytes=${encoded.length} maxMode=${enableMaxMode} rid=${requestId.slice(0, 8)}`
    );
    cachedInitialEncoded = encoded;

    // ORDER MATTERS: open the RunSSE stream BEFORE sending the first
    // BidiAppend(runRequest). Otherwise the server could discard the
    // append if no matching RunSSE has connected yet. (In practice
    // BidiAppend appears to be stateful regardless, but matching the
    // IDE's order is safest.)
    attemptConnection(proto);
    sendBinaryFrame(encoded);

    // Drain anything queued before we got here
    if (pendingWrites.length > 0) {
      const drain = pendingWrites.splice(0);
      for (const p of drain) sendBinaryFrame(p);
    }
    if (process.env.CURSOR_AGENT_DEBUG) {
      console.log(`[cursor-agent-h1][debug] runRequest sent`);
    }
  }

  // Same backoff schedule as cursor-agent.js failOrRetry. Mid-stream
  // errors after content emitted bubble up unchanged.
  function failOrRetry(proto, msg, code) {
    if (closed) return;
    const isTransient =
      /NGHTTP2_REFUSED_STREAM|REFUSED_STREAM/i.test(msg) ||
      /NGHTTP2_INTERNAL_ERROR|INTERNAL_ERROR/i.test(msg) ||
      /socket hang up|ECONNRESET|EPIPE|ETIMEDOUT/i.test(msg) ||
      code === 'ERR_H1_STALL' || code === 'ERR_SOCKET_CLOSED';

    const safeToRetry = !hasEmittedContent && retryAttempts < MAX_REQUEST_RETRIES && isTransient;
    dumpStreamSummary(msg, code || 'stream-error');
    if (safeToRetry) {
      retryAttempts++;
      _turnRetries++;
      console.log(`[cursor-agent-h1] retrying after ${msg} (${retryAttempts}/${MAX_REQUEST_RETRIES}) hasReceivedData=${hasReceivedData}`);
      streamSummaryEmitted = false;
      // Tear down the old request cleanly
      try { if (sseRes) sseRes.removeAllListeners(); } catch { /* ignore */ }
      try { if (sseRes) sseRes.destroy(); } catch { /* ignore */ }
      try { if (sseReq) sseReq.removeAllListeners(); } catch { /* ignore */ }
      try { if (sseReq) sseReq.destroy(); } catch { /* ignore */ }
      sseReq = null; sseRes = null;
      if (heartbeat) { try { clearInterval(heartbeat); } catch { /* ignore */ } heartbeat = null; }
      if (watchdog) { try { clearInterval(watchdog); } catch { /* ignore */ } watchdog = null; }
      // Reset seqno — a new RunSSE session is a new logical stream
      appendSeqno = 0n;
      appendInFlight = Promise.resolve();
      const isTransportError = /REFUSED_STREAM|GOAWAY|INTERNAL_ERROR|socket|ECONN|EPIPE|ETIMEDOUT/i.test(msg);
      const inCascade = isTransportError && lastErrorWasTransport;
      if (isTransportError) _turnTransportErrors++;
      if (inCascade) _turnCascadeDetected = true;
      const baseBackoffs = [100, 250, 750, 2000, 5000];
      const baseMs = baseBackoffs[Math.min(retryAttempts - 1, baseBackoffs.length - 1)];
      const backoffMs = Math.min(8000, baseMs * (inCascade ? 2 : 1));
      lastErrorWasTransport = isTransportError;
      setTimeout(() => {
        if (closed) return;
        try { attemptConnection(proto); } catch (e) {
          fail(`Retry failed: ${e.message}`);
          return;
        }
        // Re-send the initial runRequest on the new stream
        if (cachedInitialEncoded) {
          sendBinaryFrame(cachedInitialEncoded);
        }
        if (pendingWrites.length > 0) {
          const drain = pendingWrites.splice(0);
          for (const p of drain) sendBinaryFrame(p);
        }
      }, backoffMs);
      return;
    }
    const stallMatch = /^NGHTTP2_INTERNAL_ERROR \((Upstream stalled — [^)]+)\)$/.exec(msg);
    fail(stallMatch ? stallMatch[1] : msg);
  }

  function setTools(newTools) {
    state.mcpToolDefs = buildMcpToolDefinitions(newTools || []);
  }

  // Kick off
  loadProto().then((proto) => {
    if (closed) return;
    startConnection(proto);
  }).catch((e) => fail(`proto load failed: ${e.message}`));

  return {
    conversationId,
    sendToolResult,
    sendUserMessage,
    setCallbacks,
    setTools,
    close,
    getStats: () => {
      const now = Date.now();
      const idleMs = lastUsefulFrameAt > 0 ? now - lastUsefulFrameAt : 0;
      const thresholdMs = hasEmittedContent ? _stallPostMs : _stallPreMs;
      return {
        inputTokens, outputTokens,
        maxIdleMs,
        turnRetries: _turnRetries,
        turnTransportErrors: _turnTransportErrors,
        turnStalls: _turnStalls,
        turnCascadeDetected: _turnCascadeDetected,
        modelId,
        closed,
        turnEndedFired,
        hasEmittedContent,
        streamOpenedAt,
        openedMsAgo: streamOpenedAt > 0 ? now - streamOpenedAt : null,
        lastUsefulFrameAt,
        idleMsSinceLastUsefulFrame: lastUsefulFrameAt > 0 ? idleMs : null,
        currentThresholdMs: thresholdMs,
        currentThresholdKind: hasEmittedContent ? 'post-content' : 'pre-content',
        willTripStallInMs: lastUsefulFrameAt > 0 ? Math.max(0, thresholdMs - idleMs) : null,
        stallThresholdSource: _stallSource,
        retryAttempts,
        bytesInTotal: streamBytesIn,
        bytesInSinceLastUsefulFrame: streamBytesIn - _bytesInAtLastUsefulFrame,
        bytesOutTotal: streamBytesOut,
        textDeltaCount: _turnTextDeltaCount,
        thinkingDeltaCount: _turnThinkingDeltaCount,
        mcpCallCount: streamMcpCallCount,
        transport: 'h1',
        requestId,
      };
    },
  };
}

module.exports = {
  startConversation,
  deterministicConversationId,
};
