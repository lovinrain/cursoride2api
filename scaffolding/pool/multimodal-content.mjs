function stringifyUnknownContentBlock(block) {
  try { return JSON.stringify(block); }
  catch { return String(block); }
}

export function normalizeBase64Payload(value) {
  if (!value) return '';
  if (typeof value === 'string') {
    const comma = value.indexOf(',');
    if (value.startsWith('data:') && comma !== -1) return value.slice(comma + 1);
    return value;
  }
  if (value instanceof Uint8Array || Array.isArray(value)) {
    return Buffer.from(value).toString('base64');
  }
  if (value && typeof value === 'object' && value.type === 'Buffer' && Array.isArray(value.data)) {
    return Buffer.from(value.data).toString('base64');
  }
  return '';
}

function parseDataUrl(value) {
  if (typeof value !== 'string' || !value.startsWith('data:')) return null;
  const comma = value.indexOf(',');
  if (comma === -1) return null;
  const meta = value.slice(5, comma);
  const data = value.slice(comma + 1);
  if (!/;base64(?:;|$)/i.test(meta)) return null;
  const mediaType = (meta.split(';')[0] || 'image/png').trim() || 'image/png';
  return { mediaType, dataBase64: data };
}

export function normalizeAnthropicImageBlock(block) {
  if (!block || typeof block !== 'object') return null;

  if (block.type === 'image_url') {
    const url = typeof block.image_url === 'string'
      ? block.image_url
      : (block.image_url && typeof block.image_url.url === 'string' ? block.image_url.url : '');
    const parsed = parseDataUrl(url);
    if (!parsed) return null;
    return { kind: 'image', mediaType: parsed.mediaType, dataBase64: parsed.dataBase64 };
  }

  if (block.type !== 'image') return null;
  const source = block.source || {};
  if (source.type === 'url') {
    const parsed = parseDataUrl(source.url);
    if (!parsed) return null;
    return { kind: 'image', mediaType: parsed.mediaType, dataBase64: parsed.dataBase64 };
  }
  if (source.type && source.type !== 'base64') return null;

  const mediaType = source.media_type || source.mediaType || block.media_type || block.mediaType || 'image/png';
  const dataBase64 = normalizeBase64Payload(source.data ?? block.dataBase64 ?? block.base64 ?? block.data);
  if (!dataBase64) return null;
  return { kind: 'image', mediaType, dataBase64 };
}

export function normalizeAnthropicContentForCursorMcp(content) {
  const items = [];
  const pushText = (text) => {
    const s = String(text ?? '');
    if (s) items.push({ kind: 'text', text: s });
  };

  if (content == null) return { items: [{ kind: 'text', text: '' }] };
  if (typeof content === 'string') return { items: [{ kind: 'text', text: content }] };
  if (!Array.isArray(content)) return { items: [{ kind: 'text', text: stringifyUnknownContentBlock(content) }] };

  for (const block of content) {
    if (!block || typeof block !== 'object') continue;
    if (block.type === 'text') {
      pushText(block.text);
      continue;
    }
    const image = normalizeAnthropicImageBlock(block);
    if (image) {
      items.push(image);
      continue;
    }
    pushText(stringifyUnknownContentBlock(block));
  }

  if (items.length === 0) items.push({ kind: 'text', text: '' });
  return { items };
}

function extractImageAttachmentsFromContent(content, labelPrefix) {
  const out = [];
  if (!Array.isArray(content)) return out;
  let imageIndex = 0;
  for (const block of content) {
    if (!block || typeof block !== 'object') continue;
    const image = normalizeAnthropicImageBlock(block);
    if (image) {
      imageIndex++;
      out.push({ label: `${labelPrefix} image ${imageIndex}`, item: image });
      continue;
    }
    if (block.type === 'tool_result' && Array.isArray(block.content)) {
      const toolLabel = `${labelPrefix} tool_result ${block.tool_use_id || ''}`.trim();
      out.push(...extractImageAttachmentsFromContent(block.content, toolLabel));
    }
  }
  return out;
}

export function extractImageAttachmentsFromMessages(messages) {
  const out = [];
  const arr = Array.isArray(messages) ? messages : [];
  for (let i = 0; i < arr.length; i++) {
    const msg = arr[i];
    if (!msg || typeof msg !== 'object') continue;
    const role = msg.role || 'message';
    out.push(...extractImageAttachmentsFromContent(msg.content, `${role} turn ${i + 1}`));
  }
  return out;
}

export function appendImageAttachments(content, attachments) {
  const baseItems = content && Array.isArray(content.items) ? [...content.items] : [];
  const imgs = Array.isArray(attachments) ? attachments.filter((a) => a && a.item) : [];
  if (imgs.length === 0) return { items: baseItems.length > 0 ? baseItems : [{ kind: 'text', text: '' }] };
  baseItems.push({
    kind: 'text',
    text: '\n\n--- IMAGE ATTACHMENTS ---\nThe binary images below are part of the conversation context above.\n',
  });
  imgs.forEach((a, idx) => {
    baseItems.push({ kind: 'text', text: `\n[Image ${idx + 1}: ${a.label || 'attached image'}]\n` });
    baseItems.push(a.item);
  });
  return { items: baseItems };
}

export function prependTextContent(content, text) {
  const s = String(text || '');
  if (!s) return content && Array.isArray(content.items) ? content : { items: [{ kind: 'text', text: '' }] };
  const items = content && Array.isArray(content.items) ? content.items : [];
  return { items: [{ kind: 'text', text: s }, ...items] };
}

export function cursorMcpContentToText(content) {
  if (typeof content === 'string') return content;
  if (!content || !Array.isArray(content.items)) return '';
  return content.items.map((item) => {
    if (!item || typeof item !== 'object') return '';
    if (item.kind === 'text') return item.text || '';
    if (item.kind === 'image') return '<image/>';
    return '';
  }).join('');
}

export function cursorMcpContentToNativeUserText(content) {
  if (typeof content === 'string') return content;
  if (!content || !Array.isArray(content.items)) return '';
  return content.items.map((item) => {
    if (!item || typeof item !== 'object') return '';
    if (item.kind === 'text') return item.text || '';
    return '';
  }).join('');
}

export function cursorMcpContentImageCount(content) {
  if (!content || !Array.isArray(content.items)) return 0;
  return content.items.filter((item) => item && item.kind === 'image').length;
}

export function cursorMcpContentPayloadBytes(content) {
  try { return Buffer.byteLength(JSON.stringify(content || {})); }
  catch { return 0; }
}
