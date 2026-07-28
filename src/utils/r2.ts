import fs from 'fs';
import path from 'path';
import { Readable } from 'stream';
import {
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

function getClient() {
  if (!client) {
    const accountId = process.env.R2_ACCOUNT_ID?.trim();
    const accessKeyId = process.env.R2_ACCESS_KEY_ID?.trim();
    const secretAccessKey = process.env.R2_SECRET_ACCESS_KEY?.trim();

    if (!accountId || !accessKeyId || !secretAccessKey) {
      throw new Error("R2 credentials are missing.");
    }

    const endpoint = `https://${accountId}.r2.cloudflarestorage.com`;

      // TODO: temp remove
    logger.info("R2 Configuration", {
      endpoint,
      accountId,
      bucket: process.env.R2_BUCKET_NAME,
      node: process.version,
      accessKeyPresent: !!accessKeyId,
      secretPresent: !!secretAccessKey,
    });

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
        requestTimeout: 300_000,
      }),
    });
  }

  return client;
}

function contentTypeFromKey(key: string) {
  const ext = path.extname(key).toLowerCase();
  if (ext === '.jpg' || ext === '.jpeg') return 'image/jpeg';
  if (ext === '.png') return 'image/png';
  if (ext === '.webp') return 'image/webp';
  if (ext === '.gif') return 'image/gif';
  if (ext === '.pdf') return 'application/pdf';
  if (ext === '.mp4') return 'video/mp4';
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

  const result = await getClient().send(
    new GetObjectCommand({
      Bucket: input.bucket,
      Key: input.key,
    }),
  );

  const body = result.Body;
  if (!(body instanceof Readable)) {
    throw new Error('R2 object body is not readable.');
  }

  await fs.promises.mkdir(path.dirname(input.dest), { recursive: true });
  const file = fs.createWriteStream(input.dest, { flags: 'w' });
  const maxBytes = input.maxBytes ?? config.security.maxUploadBytes;
  const expectedBytes =
    Number.isFinite(input.expectedBytes) && (input.expectedBytes as number) > 0
      ? Number(input.expectedBytes)
      : null;
  let writtenBytes = 0;

  await new Promise<void>((resolve, reject) => {
    const fail = (error: unknown) => {
      logger.error('R2 download stream error', { bucket: input.bucket, key: input.key, writtenBytes, error });
      file.destroy();
      reject(error);
    };

    body.on('data', (chunk: Buffer) => {
      writtenBytes += chunk.length;

      if (input.onProgress) {
        input.onProgress(writtenBytes, expectedBytes ?? undefined);
      }

      if (writtenBytes > maxBytes) {
        const err = new Error('Download exceeds maximum upload size');
        logger.error('R2 download exceeded maxBytes', { bucket: input.bucket, key: input.key, writtenBytes, maxBytes });
        body.destroy(err);
        return;
      }

      if (expectedBytes && writtenBytes > expectedBytes) {
        const err = new Error('Download exceeds expected content length');
        logger.error('R2 download exceeded expectedBytes', { bucket: input.bucket, key: input.key, writtenBytes, expectedBytes });
        body.destroy(err);
        return;
      }

      if (!file.write(chunk)) {
        body.pause();
        file.once('drain', () => body.resume());
      }
    });

    body.once('error', fail);
    file.once('error', fail);
    body.once('end', () => file.end(() => {
      logger.info('R2 download complete', { bucket: input.bucket, key: input.key, writtenBytes });
      resolve();
    }));
  });

  if (expectedBytes && writtenBytes !== expectedBytes) {
    const msg = `Downloaded bytes mismatch. expected=${expectedBytes} actual=${writtenBytes}`;
    logger.error('R2 download byte mismatch', { bucket: input.bucket, key: input.key, expectedBytes, writtenBytes });
    throw new Error(msg);
  }
}

export async function uploadToR2(input: {
  bucket: string;
  key: string;
  filePath: string;
  contentType?: string;
  holderId: string;
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
