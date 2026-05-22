import assert from 'node:assert/strict';
import {
  appendImageAttachments,
  cursorMcpContentImageCount,
  cursorMcpContentToNativeUserText,
  cursorMcpContentToText,
  extractImageAttachmentsFromMessages,
  normalizeAnthropicContentForCursorMcp,
} from '../../scaffolding/pool/multimodal-content.mjs';

const pngBase64 = Buffer.from('png-bytes').toString('base64');

{
  const normalized = normalizeAnthropicContentForCursorMcp([
    { type: 'text', text: 'look at this' },
    { type: 'image', source: { type: 'base64', media_type: 'image/png', data: pngBase64 } },
  ]);

  assert.equal(normalized.items.length, 2);
  assert.deepEqual(normalized.items[0], { kind: 'text', text: 'look at this' });
  assert.deepEqual(normalized.items[1], {
    kind: 'image',
    mediaType: 'image/png',
    dataBase64: pngBase64,
  });
  assert.equal(cursorMcpContentImageCount(normalized), 1);
  assert.equal(cursorMcpContentToText(normalized), 'look at this<image/>');
  assert.equal(cursorMcpContentToNativeUserText(normalized), 'look at this');
}

{
  const normalized = normalizeAnthropicContentForCursorMcp([
    { type: 'image_url', image_url: { url: `data:image/jpeg;base64,${pngBase64}` } },
  ]);

  assert.deepEqual(normalized.items[0], {
    kind: 'image',
    mediaType: 'image/jpeg',
    dataBase64: pngBase64,
  });
}

{
  const attachments = extractImageAttachmentsFromMessages([
    { role: 'user', content: [{ type: 'text', text: 'first' }] },
    {
      role: 'assistant',
      content: [{
        type: 'tool_result',
        tool_use_id: 'toolu_screenshot',
        content: [
          { type: 'text', text: 'screenshot' },
          { type: 'image', source: { type: 'base64', media_type: 'image/png', data: pngBase64 } },
        ],
      }],
    },
    {
      role: 'user',
      content: [
        { type: 'text', text: 'what is in this image?' },
        { type: 'image', source: { type: 'base64', media_type: 'image/png', data: pngBase64 } },
      ],
    },
  ]);

  assert.equal(attachments.length, 2);
  assert.match(attachments[0].label, /assistant turn 2 tool_result toolu_screenshot image 1/);
  assert.match(attachments[1].label, /user turn 3 image 1/);

  const withAttachments = appendImageAttachments({ items: [{ kind: 'text', text: 'context' }] }, attachments);
  assert.equal(cursorMcpContentImageCount(withAttachments), 2);
  assert.match(cursorMcpContentToText(withAttachments), /IMAGE ATTACHMENTS/);
}

console.log('multimodal-content-test: OK');
