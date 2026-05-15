import fs from 'fs';
import { normalizeExtension } from './media';

function hasPrefix(buffer: Buffer, signature: number[]): boolean {
  return signature.every((value, index) => buffer[index] === value);
}

export async function assertBasicFileHeader(
  filePath: string,
  detectedMime?: string | null,
  detectedExt?: string | null
): Promise<void> {
  const ext = normalizeExtension(detectedExt);
  const header = Buffer.alloc(32);
  const handle = await fs.promises.open(filePath, 'r');

  try {
    const { bytesRead } = await handle.read(header, 0, header.length, 0);
    const slice = header.subarray(0, bytesRead);

    if (detectedMime === 'image/jpeg' || ext === '.jpg' || ext === '.jpeg') {
      if (!hasPrefix(slice, [0xff, 0xd8, 0xff])) throw new Error('Invalid JPEG header');
      return;
    }

    if (detectedMime === 'image/png' || ext === '.png') {
      if (!hasPrefix(slice, [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a])) {
        throw new Error('Invalid PNG header');
      }
      return;
    }

    if (detectedMime === 'image/gif' || ext === '.gif') {
      const gifHeader = slice.subarray(0, 6).toString('ascii');
      if (gifHeader !== 'GIF87a' && gifHeader !== 'GIF89a') {
        throw new Error('Invalid GIF header');
      }
      return;
    }

    if (detectedMime === 'image/webp' || ext === '.webp') {
      if (slice.subarray(0, 4).toString('ascii') !== 'RIFF' || slice.subarray(8, 12).toString('ascii') !== 'WEBP') {
        throw new Error('Invalid WEBP header');
      }
      return;
    }

    if (detectedMime === 'application/pdf' || ext === '.pdf') {
      if (slice.subarray(0, 5).toString('ascii') !== '%PDF-') {
        throw new Error('Invalid PDF header');
      }
      return;
    }

    if (detectedMime === 'video/mp4' || ext === '.mp4' || ext === '.m4v') {
      if (slice.length < 12 || slice.subarray(4, 8).toString('ascii') !== 'ftyp') {
        throw new Error('Invalid MP4 header');
      }
    }
  } finally {
    await handle.close();
  }
}
