import fs from 'fs';
import https from 'https';
import path from 'path';
import { Readable } from 'stream';
import {
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { config } from '../config';
import { logger } from './logger';
import { tryAcquireUploadSlot, releaseUploadSlot } from '../worker/resourceManager';
import { NodeHttpHandler } from "@smithy/node-http-handler";

let client: S3Client | null = null;
let httpsAgent: https.Agent | null = null;

function getHttpsAgent() {
  if (!httpsAgent) {
    httpsAgent = new https.Agent({
      keepAlive: true,
      keepAliveMsecs: 10_000,
      maxSockets: 50,
      timeout: 120_000,
    });
  }
  return httpsAgent;
}

function getClient() {
  if (!client) {
    const accountId = process.env.R2_ACCOUNT_ID?.trim();
    const accessKeyId = process.env.R2_ACCESS_KEY_ID?.trim();
    const secretAccessKey = process.env.R2_SECRET_ACCESS_KEY?.trim();

    if (!accountId || !accessKeyId || !secretAccessKey) {
      throw new Error("R2 credentials are missing.");
    }

    const endpoint = `https://${accountId}.r2.cloudflarestorage.com`;
    
    client = new S3Client({
      region: "auto",
      endpoint,
      forcePathStyle: true,
      credentials: {
        accessKeyId,
        secretAccessKey,
      },
      maxAttempts: 5,
      requestHandler: new NodeHttpHandler({
        connectionTimeout: 30_000,
        requestTimeout: 120_000,
        httpsAgent: getHttpsAgent(),
      }),
    });
  }

  return client;
}

export function contentTypeFromKey(key: string) {
  const ext = path.extname(key).toLowerCase();
  if (ext === '.jpg' || ext === '.jpeg') return 'image/jpeg';
  if (ext === '.png') return 'image/png';
  if (ext === '.webp') return 'image/webp';
  if (ext === '.gif') return 'image/gif';
  if (ext === '.pdf') return 'application/pdf';
  if (ext === '.mp4') return 'video/mp4';
  if (ext === '.mp3') return 'audio/mpeg';
  if (ext === '.wav') return 'audio/wav';
  if (ext === '.ogg') return 'audio/ogg';
  if (ext === '.m4a') return 'audio/mp4';
  if (ext === '.aac') return 'audio/aac';
  if (ext === '.flac') return 'audio/flac';
  return 'application/octet-stream';
}

function isSourceMissingError(error: unknown): boolean {
  const anyErr = error as any;
  const name = String(anyErr?.name || '');
  const code = String(anyErr?.Code || anyErr?.code || '');
  const statusCode = Number(anyErr?.$metadata?.httpStatusCode || anyErr?.statusCode || 0);
  return (
    name === 'NotFound' ||
    name === 'NoSuchKey' ||
    code === 'NoSuchKey' ||
    statusCode === 404
  );
}

export async function headR2Object(input: {
  bucket: string;
  key: string;
}): Promise<{ contentLength: number | null; contentType: string | null } | null> {
  try {
    const result = await getClient().send(
      new HeadObjectCommand({
        Bucket: input.bucket,
        Key: input.key,
      }),
    );

    return {
      contentLength: typeof result.ContentLength === 'number' ? result.ContentLength : null,
      contentType: result.ContentType || null,
    };
  } catch (error) {
    if (isSourceMissingError(error)) return null;
    throw error;
  }
}

export async function downloadFromR2(input: {
  bucket: string;
  key: string;
  dest: string;
  expectedBytes?: number | null;
  maxBytes?: number;
  onProgress?: (bytes: number, total?: number) => void;
}) {
  logger.info('R2 download started', { bucket: input.bucket, key: input.key, expectedBytes: input.expectedBytes });

  let expectedBytes =
    Number.isFinite(input.expectedBytes) && (input.expectedBytes as number) > 0
      ? Number(input.expectedBytes)
      : null;

  if (!expectedBytes) {
    try {
      const head = await headR2Object({ bucket: input.bucket, key: input.key });
      if (head?.contentLength) {
        expectedBytes = head.contentLength;
      }
    } catch (err) {
      logger.warn('Could not determine object size via HEAD before download', { bucket: input.bucket, key: input.key, err });
    }
  }

  const maxBytes = input.maxBytes ?? config.security.maxUploadBytes;
  await fs.promises.mkdir(path.dirname(input.dest), { recursive: true });

  let writtenBytes = 0;
  const maxDownloadAttempts = 8;
  let attempt = 0;
  let lastError: unknown = null;

  // Clear any existing partial file before starting fresh
  try {
    await fs.promises.unlink(input.dest);
  } catch {}

  while (attempt < maxDownloadAttempts) {
    attempt++;
    const isResuming = writtenBytes > 0;
    const rangeHeader = isResuming ? `bytes=${writtenBytes}-` : undefined;

    if (isResuming) {
      logger.info('Resuming R2 download using Range request', {
        bucket: input.bucket,
        key: input.key,
        attempt,
        startByte: writtenBytes,
        expectedBytes,
      });
    }

    try {
      await new Promise<void>(async (resolve, reject) => {
        let isDone = false;
        let bodyStream: Readable | null = null;
        let fileStream: fs.WriteStream | null = null;

        const cleanup = (error?: unknown) => {
          if (isDone) return;
          isDone = true;
          try {
            bodyStream?.destroy();
          } catch {}
          try {
            fileStream?.destroy();
          } catch {}
          if (error) {
            reject(error);
          } else {
            resolve();
          }
        };

        try {
          const result = await getClient().send(
            new GetObjectCommand({
              Bucket: input.bucket,
              Key: input.key,
              Range: rangeHeader,
            }),
          );

          bodyStream = result.Body as Readable;
          if (!(bodyStream instanceof Readable)) {
            cleanup(new Error('R2 object body is not readable.'));
            return;
          }

          // When resuming, open with 'a' (append) to continue writing to disk
          fileStream = fs.createWriteStream(input.dest, {
            flags: isResuming ? 'a' : 'w',
            highWaterMark: 4 * 1024 * 1024, // 4MB buffer prevents excessive backpressure stalls
          });

          bodyStream.on('data', (chunk: Buffer) => {
            writtenBytes += chunk.length;

            if (input.onProgress) {
              input.onProgress(writtenBytes, expectedBytes ?? undefined);
            }

            if (writtenBytes > maxBytes) {
              const err = new Error('Download exceeds maximum upload size');
              logger.error('R2 download exceeded maxBytes', { bucket: input.bucket, key: input.key, writtenBytes, maxBytes });
              cleanup(err);
              return;
            }

            if (expectedBytes && writtenBytes > expectedBytes) {
              const err = new Error('Download exceeds expected content length');
              logger.error('R2 download exceeded expectedBytes', { bucket: input.bucket, key: input.key, writtenBytes, expectedBytes });
              cleanup(err);
              return;
            }

            if (!fileStream?.write(chunk)) {
              bodyStream?.pause();
              fileStream?.once('drain', () => bodyStream?.resume());
            }
          });

          bodyStream.once('error', (err) => {
            cleanup(err);
          });

          fileStream.once('error', (err) => {
            cleanup(err);
          });

          bodyStream.once('end', () => {
            if (!fileStream) {
              cleanup();
              return;
            }
            fileStream.end(() => {
              cleanup();
            });
          });
        } catch (fetchErr) {
          cleanup(fetchErr);
        }
      });

      // Verify on-disk size
      const stats = await fs.promises.stat(input.dest);
      writtenBytes = stats.size;

      if (expectedBytes && writtenBytes < expectedBytes) {
        throw new Error(`Incomplete download stream. expected=${expectedBytes} actual=${writtenBytes}`);
      }

      logger.info('R2 download complete', { bucket: input.bucket, key: input.key, writtenBytes });
      return;
    } catch (err: any) {
      lastError = err;
      logger.warn('R2 download stream interrupted, will attempt range resumption', {
        bucket: input.bucket,
        key: input.key,
        writtenBytes,
        expectedBytes,
        attempt,
        maxDownloadAttempts,
        error: err?.message || String(err),
      });

      // Synchronize writtenBytes with actual disk state
      try {
        const stats = await fs.promises.stat(input.dest);
        writtenBytes = stats.size;
      } catch {}

      if (expectedBytes && writtenBytes >= expectedBytes) {
        logger.info('R2 download complete despite stream interruption', { bucket: input.bucket, key: input.key, writtenBytes });
        return;
      }

      if (attempt >= maxDownloadAttempts) {
        break;
      }

      // Backoff before resuming: 1s, 2s, 4s, up to 8s
      const backoffMs = Math.min(1000 * Math.pow(2, attempt - 1), 8000);
      await new Promise((resolve) => setTimeout(resolve, backoffMs));
    }
  }

  logger.error('R2 download failed after all resume attempts', {
    bucket: input.bucket,
    key: input.key,
    expectedBytes,
    writtenBytes,
    attempts: attempt,
    error: lastError,
  });

  throw lastError instanceof Error ? lastError : new Error(String(lastError || 'Download failed'));
}

export async function uploadToR2(input: {
  bucket: string;
  key: string;
  filePath: string;
  contentType?: string;
  holderId: string;
  signal?: AbortSignal;
  onProgress?: (bytes: number, total: number) => void;
}) {
  const stat = await fs.promises.stat(input.filePath);

  if (!stat.isFile() || stat.size > config.security.maxOutputBytes) {
    logger.error('R2 upload rejected: file exceeds size limit or is not a file', {
      filePath: input.filePath,
      size: stat.isFile() ? stat.size : 'not a file',
      maxOutputBytes: config.security.maxOutputBytes,
    });
    throw new Error('Output file exceeds allowed size');
  }

  logger.info('R2 upload starting', {
    bucket: input.bucket,
    key: input.key,
    sizeBytes: stat.size,
    filePath: input.filePath,
  });

  const holderId = input.holderId;
  while (!(await tryAcquireUploadSlot(holderId))) {
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  try {
    const fileStream = fs.createReadStream(input.filePath);

    // Use 10MB parts for large files, 5MB minimum (R2 minimum is 5MB per part)
    const partSizeBytes = Math.max(5 * 1024 * 1024, Math.ceil(stat.size / 1000));

    const upload = new Upload({
      client: getClient(),
      queueSize: 4,           // 4 concurrent part uploads
      partSize: partSizeBytes, // dynamic part size
      leavePartsOnError: false,
      params: {
        Bucket: input.bucket,
        Key: input.key,
        Body: fileStream,
        ContentType: input.contentType ?? contentTypeFromKey(input.key),
      },
    });

    upload.on('httpUploadProgress', (progress) => {
      logger.info('R2 upload progress', {
        key: input.key,
        loaded: progress.loaded,
        total: progress.total ?? stat.size,
        part: progress.part,
      });
      if (input.onProgress && progress.loaded) {
        input.onProgress(progress.loaded, stat.size);
      }
    });

    logger.info("Calling upload.done()", {
      bucket: input.bucket,
      key: input.key,
    });

    const abortListener = () => {
      try {
        upload.abort();
      } catch {}
      try {
        fileStream.destroy();
      } catch {}
    };

    if (input.signal) {
      if (input.signal.aborted) {
        abortListener();
        throw new Error('Upload aborted: cancellation requested');
      }
      input.signal.addEventListener('abort', abortListener, { once: true });
    }

    try {
      await upload.done();
    } catch (error) {
      logger.error("upload.done() failed", {
        bucket: input.bucket,
        key: input.key,
        endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
        error,
      });

      throw error;
    }

    if (input.signal) input.signal.removeEventListener('abort', abortListener);
    logger.info('R2 upload complete', {
      bucket: input.bucket,
      key: input.key,
      sizeBytes: stat.size,
    });

    return {
      bucket: input.bucket,
      key: input.key,
      sizeBytes: stat.size,
      contentType: input.contentType ?? contentTypeFromKey(input.key),
    };
  } catch (err) {
    logger.error('R2 upload failed', {
      bucket: input.bucket,
      key: input.key,
      sizeBytes: stat.size,
      filePath: input.filePath,
      error: err,
    });
    throw err;
  } finally {
    await releaseUploadSlot(holderId);
  }
}


export async function deleteR2Object(bucket: string, key: string): Promise<void> {
  try {
    await getClient().send(
      new DeleteObjectCommand({
        Bucket: bucket,
        Key: key,
      }),
    );
    logger.info('Deleted R2 object', { bucket, key });
  } catch (err) {
    logger.warn('Failed to delete R2 object', { bucket, key, err });
  }
}

export async function uploadJsonToR2(input: {
  bucket: string;
  key: string;
  payload: unknown;
}) {
  const body = JSON.stringify(input.payload, null, 2);

  await getClient().send(
    new PutObjectCommand({
      Bucket: input.bucket,
      Key: input.key,
      Body: body,
      ContentLength: Buffer.byteLength(body),
      ContentType: 'application/json',
    }),
  );

  return {
    bucket: input.bucket,
    key: input.key,
    sizeBytes: Buffer.byteLength(body),
  };
}

export async function upload(
  filePath: string,
  key: string,
  holderId: string,
  onProgress?: (bytes: number, total: number) => void
): Promise<string> {
  if (config.mode === 'local') {
    const outputDir = path.resolve('./outputs');
    const dest = path.resolve(outputDir, key);

    if (!dest.startsWith(`${outputDir}${path.sep}`) && dest !== outputDir) {
      throw new Error('Invalid output key');
    }

    await fs.promises.mkdir(path.dirname(dest), { recursive: true });
    
    if (onProgress) {
      const stat = await fs.promises.stat(filePath);
      const readStream = fs.createReadStream(filePath);
      const writeStream = fs.createWriteStream(dest);
      let uploadedBytes = 0;
      
      readStream.on('data', (chunk) => {
        uploadedBytes += chunk.length;
        onProgress(uploadedBytes, stat.size);
      });

      await new Promise((resolve, reject) => {
        readStream.pipe(writeStream);
        writeStream.on('finish', resolve);
        writeStream.on('error', reject);
        readStream.on('error', reject);
      });
    } else {
      await fs.promises.copyFile(filePath, dest);
    }
    return dest;
  }

  const bucket = process.env.R2_BUCKET_NAME;
  if (!bucket) {
    throw new Error('R2_BUCKET_NAME is required.');
  }

  await uploadToR2({ bucket, key, filePath, holderId, onProgress });
  return key;
}

export async function download(
  inputUrl: string,
  dest: string,
  options?: { expectedBytes?: number | null; maxBytes?: number; onProgress?: (bytes: number, total?: number) => void; },
): Promise<void> {
  await fs.promises.mkdir(path.dirname(dest), { recursive: true });
  const maxBytes = options?.maxBytes ?? config.security.maxUploadBytes;

  if (inputUrl.startsWith('file://')) {
    if (process.env.ALLOW_LOCAL_FILE_INPUTS !== 'true') {
      throw new Error('Local file inputs are disabled. Use R2 bucket/key input.');
    }

    const sourcePath = decodeURIComponent(new URL(inputUrl).pathname);
    const stat = await fs.promises.stat(sourcePath);
    if (stat.size > maxBytes) {
      throw new Error('Download exceeds maximum upload size');
    }
    await fs.promises.copyFile(sourcePath, dest);
    return;
  }

  if (!/^https?:\/\//i.test(inputUrl)) {
    throw new Error('Unsupported input URL protocol');
  }

  if (process.env.ALLOW_REMOTE_INPUT_URLS !== 'true') {
    throw new Error('Remote input URLs are disabled. Use R2 bucket/key input.');
  }

  const response = await fetch(inputUrl);
  if (!response.ok || !response.body) {
    throw new Error(`Download failed: ${response.status}`);
  }

  const contentLength = Number(response.headers.get('content-length') ?? '0');
  if (contentLength > maxBytes) {
    throw new Error('Download exceeds maximum upload size');
  }

  const expectedBytes =
    Number.isFinite(options?.expectedBytes) && (options?.expectedBytes as number) > 0
      ? Number(options?.expectedBytes)
      : contentLength > 0
        ? contentLength
        : null;

  const file = fs.createWriteStream(dest);
  let downloadedBytes = 0;

  await new Promise<void>((resolve, reject) => {
    const reader = response.body!.getReader();

    async function pump(): Promise<void> {
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            file.end(resolve);
            return;
          }

          downloadedBytes += value.byteLength;

          if (options?.onProgress) {
            options.onProgress(downloadedBytes, expectedBytes ?? undefined);
          }

          if (downloadedBytes > maxBytes) {
            file.destroy();
            reject(new Error('Download exceeds maximum upload size'));
            return;
          }

          if (expectedBytes && downloadedBytes > expectedBytes) {
            file.destroy();
            reject(new Error('Download exceeds expected content length'));
            return;
          }

          if (!file.write(Buffer.from(value))) {
            await new Promise((resume) => file.once('drain', resume));
          }
        }
      } catch (error) {
        reject(error);
      }
    }

    file.on('error', reject);
    void pump();
  });

  if (expectedBytes && downloadedBytes !== expectedBytes) {
    throw new Error(`Downloaded bytes mismatch. expected=${expectedBytes} actual=${downloadedBytes}`);
  }
}
