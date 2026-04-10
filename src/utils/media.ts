import path from 'path';
import { FILE_TYPE } from '../constants';
import { FileJob } from '../types';

const IMAGE_EXTENSIONS = new Set([
  '.jpg',
  '.jpeg',
  '.png',
  '.webp',
  '.gif',
  '.avif',
  '.heic',
  '.heif',
  '.tif',
  '.tiff',
  '.bmp',
  '.svg',
]);

const VIDEO_EXTENSIONS = new Set([
  '.mp4',
  '.m4v',
  '.mov',
  '.mkv',
  '.webm',
  '.avi',
  '.wmv',
  '.flv',
  '.mpg',
  '.mpeg',
  '.m2v',
  '.ts',
  '.mts',
  '.m2ts',
  '.3gp',
  '.ogv',
  '.mxf',
]);

export function normalizeExtension(ext?: string | null): string | null {
  if (!ext) return null;
  const normalized = ext.startsWith('.') ? ext.toLowerCase() : `.${ext.toLowerCase()}`;
  return normalized === '.' ? null : normalized;
}

export function inferExtensionFromValue(value?: string | null): string | null {
  if (!value) return null;

  try {
    if (/^[a-z]+:\/\//i.test(value)) {
      return normalizeExtension(path.extname(new URL(value).pathname));
    }
  } catch {
    // Fall through to raw path parsing
  }

  return normalizeExtension(path.extname(value.split('?')[0]));
}

export function classifyFileType(
  mime?: string | null,
  extOrValue?: string | null
): FileJob['fileType'] {
  const ext = inferExtensionFromValue(extOrValue) ?? normalizeExtension(extOrValue);
  const normalizedMime = mime?.toLowerCase() ?? null;

  if (normalizedMime?.startsWith('image/')) return FILE_TYPE.IMAGE;
  if (normalizedMime?.startsWith('video/')) return FILE_TYPE.VIDEO;

  if (ext && IMAGE_EXTENSIONS.has(ext)) return FILE_TYPE.IMAGE;
  if (ext && VIDEO_EXTENSIONS.has(ext)) return FILE_TYPE.VIDEO;

  return FILE_TYPE.OTHER;
}

export function isLikelyMatroska(
  details: {
    formatName?: string | null;
    mime?: string | null;
    ext?: string | null;
  }
): boolean {
  const formatName = details.formatName?.toLowerCase() ?? '';
  const mime = details.mime?.toLowerCase() ?? '';
  const ext = normalizeExtension(details.ext);

  return (
    formatName.includes('matroska') ||
    mime === 'video/x-matroska' ||
    ext === '.mkv'
  );
}
