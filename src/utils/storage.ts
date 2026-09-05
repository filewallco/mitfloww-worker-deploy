import fs from "node:fs/promises";
import path from "node:path";
import {
  headR2Object,
  downloadFromR2,
  uploadToR2,
  uploadJsonToR2,
  deleteR2Object,
  contentTypeFromKey,
} from "./r2";
import { logger } from "./logger";
import { config } from "../config";
import { tryAcquireUploadSlot, releaseUploadSlot } from "../worker/resourceManager";

function isLocal() {
  return process.env.STORAGE_PROVIDER === "local";
}

function getLocalPath(bucket: string, key: string) {
  const basePath = process.env.LOCAL_STORAGE_PATH || "/storage";
  const resolvedBase = path.resolve(process.cwd(), basePath);
  return path.join(resolvedBase, bucket, key);
}

export async function headObject(input: {
  bucket: string;
  key: string;
}): Promise<{ contentLength: number | null; contentType: string | null } | null> {
  if (isLocal()) {
    try {
      const p = getLocalPath(input.bucket, input.key);
      const stat = await fs.stat(p);
      return { contentLength: stat.size, contentType: null };
    } catch {
      return null;
    }
  }
  return headR2Object(input);
}

export async function downloadObject(input: {
  bucket: string;
  key: string;
  dest: string;
  expectedBytes?: number | null;
  maxBytes?: number;
  onProgress?: (bytes: number, total?: number) => void;
}) {
  if (isLocal()) {
    const src = getLocalPath(input.bucket, input.key);
    logger.info("Local download started", { bucket: input.bucket, key: input.key });
    
    await fs.mkdir(path.dirname(input.dest), { recursive: true });
    
    // Using copyFile for local-to-local is much faster and bypasses streams.
    // If we need progress, we could stream it, but for local it's nearly instantaneous.
    await fs.copyFile(src, input.dest);
    const stat = await fs.stat(input.dest);
    
    if (input.onProgress) {
      input.onProgress(stat.size, stat.size);
    }
    
    if (input.expectedBytes && stat.size !== input.expectedBytes) {
      throw new Error(`Downloaded bytes mismatch. expected=${input.expectedBytes} actual=${stat.size}`);
    }
    
    logger.info("Local download complete", { bucket: input.bucket, key: input.key, writtenBytes: stat.size });
    return;
  }
  return downloadFromR2(input);
}

export async function uploadObject(input: {
  bucket: string;
  key: string;
  filePath: string;
  contentType?: string;
  holderId: string;
  signal?: AbortSignal;
  onProgress?: (bytes: number, total: number) => void;
}) {
  if (isLocal()) {
    const stat = await fs.stat(input.filePath);

    if (!stat.isFile() || stat.size > config.security.maxOutputBytes) {
      logger.error('Local upload rejected: file exceeds size limit or is not a file', {
        filePath: input.filePath,
        size: stat.isFile() ? stat.size : 'not a file',
        maxOutputBytes: config.security.maxOutputBytes,
      });
      throw new Error('Local upload file is too large or not a valid file.');
    }

    // Still acquire upload slot to respect concurrency limits locally
    await tryAcquireUploadSlot(input.holderId);

    try {
      if (input.signal?.aborted) {
      throw new Error('Local upload file aborted: cancellation requested');
    }
    const dest = getLocalPath(input.bucket, input.key);
      await fs.mkdir(path.dirname(dest), { recursive: true });
      await fs.copyFile(input.filePath, dest);
      if (input.onProgress) {
        input.onProgress(stat.size, stat.size);
      }
      logger.info('Local upload complete', { bucket: input.bucket, key: input.key });
      return {
        bucket: input.bucket,
        key: input.key,
        sizeBytes: stat.size,
        contentType: input.contentType ?? contentTypeFromKey(input.key)
      };
    } finally {
      await releaseUploadSlot(input.holderId);
    }
  }
  return uploadToR2(input);
}

export async function uploadJsonObject(input: {
  bucket: string;
  key: string;
  payload: unknown;
}) {
  if (isLocal()) {
    const dest = getLocalPath(input.bucket, input.key);
    await fs.mkdir(path.dirname(dest), { recursive: true });
    const bodyStr = JSON.stringify(input.payload, null, 2);
    await fs.writeFile(dest, bodyStr);
    return {
      bucket: input.bucket,
      key: input.key,
      sizeBytes: Buffer.byteLength(bodyStr),
    };
  }
  return uploadJsonToR2(input);
}

export async function deleteObject(bucket: string, key: string): Promise<void> {
  if (isLocal()) {
    try {
      const dest = getLocalPath(bucket, key);
      await fs.unlink(dest);
    } catch {}
    return;
  }
  return deleteR2Object(bucket, key);
}
