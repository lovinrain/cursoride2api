const assert = require('assert');
const anthropicConverter = require('../../src/anthropic-converter');
const {
  PROXY_THINKING_SIGNATURE_PREFIX,
  ProxyThinkingBlockAdapter,
  isProxyThinkingSignature,
  isProxyLocalThinkingBlock,
} = require('../../src/proxy-thinking-adapter');

function parseSsePayload(sse) {
  const line = String(sse).split('\n').find((l) => l.startsWith('data: '));
  assert(line, 'SSE data line exists');
  return JSON.parse(line.slice('data: '.length));
}

const adapter = new ProxyThinkingBlockAdapter({
  source: 'test',
  convKey: 'conv123',
  requestId: 'req123',
  blockIndex: 0,
});
adapter.append('first ');
adapter.append('second');
const signature = adapter.signature();

assert(signature.startsWith(PROXY_THINKING_SIGNATURE_PREFIX), 'signature has proxy-local prefix');
assert(isProxyThinkingSignature(signature), 'signature is recognized');
assert(isProxyLocalThinkingBlock({
  type: 'thinking',
  thinking: 'first second',
  signature,
}), 'thinking block is recognized as proxy-local');
assert(!isProxyLocalThinkingBlock({
  type: 'thinking',
  thinking: 'first second',
  signature: 'real-anthropic-looking-signature',
}), 'non-proxy thinking block is not recognized');

const sigEvent = parseSsePayload(anthropicConverter.buildContentBlockDeltaSignature(3, signature));
assert.strictEqual(sigEvent.type, 'content_block_delta');
assert.strictEqual(sigEvent.index, 3);
assert.deepStrictEqual(sigEvent.delta, { type: 'signature_delta', signature });

const sseEvents = [
  anthropicConverter.buildMessageStart('claude-test', 0),
  anthropicConverter.buildContentBlockStartThinking(0),
  anthropicConverter.buildContentBlockDeltaThinking(0, 'thinking'),
  anthropicConverter.buildContentBlockDeltaSignature(0, signature),
  anthropicConverter.buildContentBlockStop(0),
  anthropicConverter.buildContentBlockStart(1),
  anthropicConverter.buildContentBlockDelta(1, 'answer'),
  anthropicConverter.buildContentBlockStop(1),
  anthropicConverter.buildMessageDelta('end_turn', 1, 1),
  anthropicConverter.buildMessageStop(),
].join('');
const ssePayloads = sseEvents
  .split('\n\n')
  .filter(Boolean)
  .map(parseSsePayload);
assert.deepStrictEqual(
  ssePayloads.map((e) => e.type),
  [
    'message_start',
    'content_block_start',
    'content_block_delta',
    'content_block_delta',
    'content_block_stop',
    'content_block_start',
    'content_block_delta',
    'content_block_stop',
    'message_delta',
    'message_stop',
  ],
  'SSE emits thinking signature before closing thinking block and starting text',
);
assert.strictEqual(ssePayloads[1].index, 0);
assert.strictEqual(ssePayloads[5].index, 1);

const prompt = anthropicConverter.anthropicMessagesToPrompt([
  {
    role: 'user',
    content: 'question',
  },
  {
    role: 'assistant',
    content: [
      { type: 'thinking', thinking: 'proxy-local hidden', signature },
      { type: 'text', text: 'visible answer' },
    ],
  },
  {
    role: 'user',
    content: 'next',
  },
]);
assert(!prompt.includes('proxy-local hidden'), 'proxy-local thinking is not rendered into prompt');
assert(prompt.includes('visible answer'), 'assistant text is still rendered');

const realThinkingPrompt = anthropicConverter.anthropicMessagesToPrompt([
  {
    role: 'assistant',
    content: [
      { type: 'thinking', thinking: 'non proxy thinking', signature: 'non-proxy' },
      { type: 'text', text: 'answer' },
    ],
  },
  {
    role: 'user',
    content: 'next',
  },
]);
assert(realThinkingPrompt.includes('<thinking>\nnon proxy thinking\n</thinking>'), 'non-proxy thinking keeps previous rendering behavior');

console.log('proxy-thinking-adapter-test: OK');
