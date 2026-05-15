import path from 'path';
import { FILE_TYPE } from '../constants';
import { FileJob } from '../types';

const IMAGE_EXTENSIONS = new Set([
  '.jpg',
  '.jpeg',
  '.png',
  '.webp',
  '.gif',
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

const PDF_EXTENSIONS = new Set([
  '.pdf',
]);

const IMAGE_MIME_TYPES = new Set([
  'image/jpeg',
  'image/png',
  'image/webp',
  'image/gif',
]);

const SAFE_MEDIA_MIME_TO_EXTENSIONS = new Map<string, { fileType: FileJob['fileType']; exts: Set<string> }>([
  ['image/jpeg', { fileType: FILE_TYPE.IMAGE, exts: new Set(['.jpg', '.jpeg']) }],
  ['image/png', { fileType: FILE_TYPE.IMAGE, exts: new Set(['.png']) }],
  ['image/webp', { fileType: FILE_TYPE.IMAGE, exts: new Set(['.webp']) }],
  ['image/gif', { fileType: FILE_TYPE.IMAGE, exts: new Set(['.gif']) }],
  ['video/mp4', { fileType: FILE_TYPE.VIDEO, exts: new Set(['.mp4', '.m4v']) }],
  ['video/quicktime', { fileType: FILE_TYPE.VIDEO, exts: new Set(['.mov']) }],
  ['video/webm', { fileType: FILE_TYPE.VIDEO, exts: new Set(['.webm']) }],
  ['video/x-matroska', { fileType: FILE_TYPE.VIDEO, exts: new Set(['.mkv']) }],
  ['video/vnd.avi', { fileType: FILE_TYPE.VIDEO, exts: new Set(['.avi']) }],
  ['video/x-msvideo', { fileType: FILE_TYPE.VIDEO, exts: new Set(['.avi']) }],
  ['video/x-ms-wmv', { fileType: FILE_TYPE.VIDEO, exts: new Set(['.wmv']) }],
  ['video/x-ms-asf', { fileType: FILE_TYPE.VIDEO, exts: new Set(['.wmv']) }],
  ['video/x-flv', { fileType: FILE_TYPE.VIDEO, exts: new Set(['.flv']) }],
  ['video/mpeg', { fileType: FILE_TYPE.VIDEO, exts: new Set(['.mpg', '.mpeg', '.m2v']) }],
  ['video/mp2t', { fileType: FILE_TYPE.VIDEO, exts: new Set(['.ts', '.mts', '.m2ts']) }],
  ['video/3gpp', { fileType: FILE_TYPE.VIDEO, exts: new Set(['.3gp']) }],
  ['video/ogg', { fileType: FILE_TYPE.VIDEO, exts: new Set(['.ogv']) }],
  ['application/ogg', { fileType: FILE_TYPE.VIDEO, exts: new Set(['.ogv']) }],
  ['application/mxf', { fileType: FILE_TYPE.VIDEO, exts: new Set(['.mxf']) }],
  ['application/pdf', { fileType: FILE_TYPE.PDF, exts: new Set(['.pdf']) }],
  ['video/matroska', { fileType: FILE_TYPE.VIDEO, exts: new Set(['.mkv']) }]
]);

const VIDEO_MIME_TYPES = new Set([
  'video/mp4',
  'video/quicktime',
  'video/webm',
  'video/x-matroska',
  'video/x-msvideo',
  'video/vnd.avi',
  'video/x-ms-wmv',
  'video/x-ms-asf',
  'video/x-flv',
  'video/mpeg',
  'video/mp2t',
  'video/3gpp',
  'video/3gpp2',
  'video/ogg',
  'application/ogg',
  'application/mxf',
  'video/x-matroska',
  'video/matroska',
]);

const PDF_MIME_TYPES = new Set([
  'application/pdf',
]);

const DANGEROUS_EXTENSIONS = new Set([
  '.bat',
  '.cmd',
  '.com',
  '.cpl',
  '.dll',
  '.exe',
  '.hta',
  '.htm',
  '.html',
  '.jar',
  '.js',
  '.jse',
  '.lnk',
  '.mjs',
  '.msi',
  '.php',
  '.ps1',
  '.scr',
  '.sh',
  '.svg',
  '.vbe',
  '.vbs',
  '.wsf',
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
  const safeMime = normalizedMime
    ? SAFE_MEDIA_MIME_TO_EXTENSIONS.get(normalizedMime)
    : null;

  if (safeMime) return safeMime.fileType;

  if (ext && IMAGE_EXTENSIONS.has(ext)) return FILE_TYPE.IMAGE;
  if (ext && VIDEO_EXTENSIONS.has(ext)) return FILE_TYPE.VIDEO;
  if (ext && PDF_EXTENSIONS.has(ext)) return FILE_TYPE.PDF;

  return FILE_TYPE.OTHER;
}

export function assertAllowedMediaInput(
  fileType: FileJob['fileType'],
  mime?: string | null,
  extOrValue?: string | null
): void {
  const ext = inferExtensionFromValue(extOrValue) ?? normalizeExtension(extOrValue);
  const normalizedMime = mime?.toLowerCase() ?? null;

  if (ext && DANGEROUS_EXTENSIONS.has(ext)) {
    throw new Error(`Blocked dangerous file extension: ${ext}`);
  }

  if (!ext) {
    throw new Error('Missing file extension');
  }

  const allowedByMime = normalizedMime
    ? SAFE_MEDIA_MIME_TO_EXTENSIONS.get(normalizedMime)
    : null;

  if (normalizedMime && (!allowedByMime || allowedByMime.fileType !== fileType || !allowedByMime.exts.has(ext))) {
    throw new Error(`MIME and extension mismatch: ${normalizedMime} ${ext}`);
  }

  if (fileType === FILE_TYPE.IMAGE) {
    if (!IMAGE_EXTENSIONS.has(ext) || (normalizedMime && !IMAGE_MIME_TYPES.has(normalizedMime))) {
      throw new Error(`Unsupported image extension: ${ext}`);
    }
    return;
  }

  if (fileType === FILE_TYPE.VIDEO) {
    if (!VIDEO_EXTENSIONS.has(ext) || (normalizedMime && !VIDEO_MIME_TYPES.has(normalizedMime))) {
      throw new Error(`Unsupported video extension: ${ext}`);
    }
    return;
  }

  if (fileType === FILE_TYPE.PDF) {
    if (!PDF_EXTENSIONS.has(ext) || (normalizedMime && !PDF_MIME_TYPES.has(normalizedMime))) {
      throw new Error(`Unsupported PDF extension: ${ext}`);
    }
    return;
  }

  throw new Error(`Unsupported file type: ${fileType}`);
}

export function assertDetectedMediaMatchesDeclaration(
  fileType: FileJob['fileType'],
  declaredValue: string,
  detectedMime?: string | null,
  detectedExt?: string | null
): void {
  const normalizedDetectedMime = detectedMime?.toLowerCase() ?? null;
  const normalizedDetectedExt = normalizeExtension(detectedExt);

  if (!normalizedDetectedMime || !normalizedDetectedExt) {
    throw new Error('Unable to detect trusted file signature');
  }

  const allowed = SAFE_MEDIA_MIME_TO_EXTENSIONS.get(normalizedDetectedMime);

  if (!allowed || allowed.fileType !== fileType || !allowed.exts.has(normalizedDetectedExt)) {
    throw new Error(`Unsupported detected media type: ${normalizedDetectedMime} ${normalizedDetectedExt}`);
  }

  assertAllowedMediaInput(fileType, normalizedDetectedMime, declaredValue);
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
