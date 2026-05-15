export function toPublicErrorMessage(message?: string | null): string {
  const normalized = (message || '').toLowerCase();

  if (
    normalized === 'invalid or unsupported media file' ||
    normalized === 'file exceeds allowed limits' ||
    normalized === 'processing timed out' ||
    normalized === 'processing failed'
  ) {
    return message as string;
  }

  if (
    normalized.includes('invalid job id') ||
    normalized.includes('file extension') ||
    normalized.includes('file signature') ||
    normalized.includes('mime and extension mismatch') ||
    normalized.includes('unsupported') ||
    normalized.includes('codec') ||
    normalized.includes('invalid jpeg header') ||
    normalized.includes('invalid png header') ||
    normalized.includes('invalid gif header') ||
    normalized.includes('invalid webp header') ||
    normalized.includes('invalid pdf header') ||
    normalized.includes('invalid mp4 header') ||
    normalized.includes('does not contain a video stream') ||
    normalized.includes('malformed') ||
    normalized.includes('encrypted')
  ) {
    return 'Invalid or unsupported media file';
  }

  if (
    normalized.includes('allowed size') ||
    normalized.includes('safety size limit') ||
    normalized.includes('pixel limit') ||
    normalized.includes('page count exceeds') ||
    normalized.includes('page dimensions exceed')
  ) {
    return 'File exceeds allowed limits';
  }

  if (
    normalized.includes('timeout') ||
    normalized.includes('stalled')
  ) {
    return 'Processing timed out';
  }

  return 'Processing failed';
}
