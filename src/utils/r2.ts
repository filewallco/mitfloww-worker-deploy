import fs from "fs";
import path from "path";
import { Readable } from "stream";
import { GetObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { config } from "../config";
import { connection } from "../queue/connection";

const UPLOAD_KEY = "global:upload_slots";
const MAX_UPLOADS = Number(process.env.MAX_PARALLEL_UPLOADS || 6);

/**
 * This module provides utility functions for interacting with Cloudflare R2, a scalable object storage service. It includes functions for downloading files from R2, uploading files to R2, and uploading JSON data as files to R2. The module also implements a Redis-based locking mechanism to manage concurrent uploads and prevent resource exhaustion. These utilities are essential for handling file processing workflows that involve storing and retrieving files from R2, ensuring efficient and reliable interactions with the storage service.
 * Key functions include:
 * - getClient: Initializes and returns a singleton S3Client instance configured for R2.
 * - acquireUploadSlot: Implements a locking mechanism using Redis to limit concurrent uploads.
 * - releaseUploadSlot: Releases the upload slot after an upload is completed or fails.
 * - contentTypeFromKey: Determines the MIME type of a file based on its extension.
 * - downloadFromR2: Downloads a file from R2 to a local destination.
 * - uploadToR2: Uploads a file from the local filesystem to R2, with size validation and concurrency control.
 * - uploadJsonToR2: Uploads a JSON payload to R2 as a file, ensuring proper formatting and content type.
 * These functions are designed to work together to facilitate seamless file handling in applications that utilize R2 for storage.
 */
const ACQUIRE_UPLOAD_LUA = `
local current = tonumber(redis.call("GET", KEYS[1]) or "0")
local limit = tonumber(ARGV[1])

if current < limit then
  redis.call("INCR", KEYS[1])
  redis.call("EXPIRE", KEYS[1], 60)
  return 1
else
  return 0
end
`;

let client: S3Client | null = null;

/**
 * getClient initializes and returns a singleton instance of the S3Client for interacting with R2. It reads the necessary credentials and configuration from environment variables and sets up the client with the appropriate endpoint and settings for R2. This function ensures that the client is only created once and reused across the application, optimizing resource usage and performance when making requests to R2.
 */
function getClient() {
  if (!client) {
    const accountId = process.env.R2_ACCOUNT_ID;
    const accessKeyId = process.env.R2_ACCESS_KEY_ID;
    const secretAccessKey = process.env.R2_SECRET_ACCESS_KEY;

    if (!accountId || !accessKeyId || !secretAccessKey) {
      throw new Error("R2 credentials are missing.");
    }

    client = new S3Client({
      credentials: { accessKeyId, secretAccessKey },
      endpoint: `https://${accountId}.r2.cloudflarestorage.com`,
      forcePathStyle: true,
      region: "auto",
    });
  }

  return client;
}

/**
 * acquireUploadSlot attempts to acquire an upload slot by incrementing a counter in Redis. It uses a Lua script to ensure atomicity and checks against the configured maximum number of parallel uploads. If the limit is reached, it waits and retries until a slot becomes available. This mechanism helps manage concurrent uploads effectively, preventing resource exhaustion and ensuring that the system can handle the load without degradation.
 */
async function acquireUploadSlot(): Promise<void> {
  while (true) {
    const ok = await connection.eval(ACQUIRE_UPLOAD_LUA, 1, UPLOAD_KEY, MAX_UPLOADS);
    if (ok === 1) return;
    await new Promise((r) => setTimeout(r, 50));
  }
}

/**
 * releaseUploadSlot decrements the upload slot counter in Redis, allowing other uploads to proceed. If the counter reaches zero or below, it deletes the key to reset the state. This function is called after an upload is completed or fails, ensuring that the system can manage concurrent uploads effectively without exceeding the configured limit.
 */
async function releaseUploadSlot(): Promise<void> {
  try {
    const val = await connection.decr(UPLOAD_KEY);
    if (val <= 0) await connection.del(UPLOAD_KEY);
  } catch {}
}

/**
 * contentTypeFromKey is a helper function that determines the MIME type of a file based on its extension. It uses the path module to extract the file extension and maps common extensions to their corresponding MIME types. If the extension is not recognized, it defaults to "application/octet-stream". This function is useful for setting the correct Content-Type header when uploading files to R2, ensuring that they are handled appropriately when accessed later.
 */
function contentTypeFromKey(key: string) {
  const ext = path.extname(key).toLowerCase();

  if (ext === ".jpg" || ext === ".jpeg") return "image/jpeg";
  if (ext === ".png") return "image/png";
  if (ext === ".webp") return "image/webp";
  if (ext === ".gif") return "image/gif";
  if (ext === ".pdf") return "application/pdf";
  if (ext === ".mp4") return "video/mp4";

  return "application/octet-stream";
}

/**
 * downloadFromR2 downloads a file from R2 to a specified local destination. It retrieves the object from R2 using the GetObjectCommand and streams it directly to the local filesystem. The function ensures that the destination directory exists and handles any errors that may occur during the download process. This is essential for processing files that are stored in R2, allowing them to be accessed locally for further processing or analysis.
 */
export async function downloadFromR2(input: {
  bucket: string;
  key: string;
  dest: string;
}) {
  const result = await getClient().send(
    new GetObjectCommand({
      Bucket: input.bucket,
      Key: input.key,
    }),
  );

  const body = result.Body;

  if (!(body instanceof Readable)) {
    throw new Error("R2 object body is not readable.");
  }

  await fs.promises.mkdir(path.dirname(input.dest), { recursive: true });

  await new Promise<void>((resolve, reject) => {
    const file = fs.createWriteStream(input.dest);
    body.pipe(file);
    body.on("error", reject);
    file.on("error", reject);
    file.on("finish", resolve);
  });
}

/**
 * uploadToR2 uploads a file from the local filesystem to R2. It checks the file size against the configured maximum output size to prevent uploading excessively large files. The function uses a Redis-based locking mechanism to limit the number of concurrent uploads, ensuring that system resources are not overwhelmed. Upon successful upload, it returns the bucket, key, size in bytes, and content type of the uploaded file for reference.
 * This function is essential for handling the output of processed files, allowing them to be stored in R2 and accessed later for delivery or further processing.
 */
export async function uploadToR2(input: {
  bucket: string;
  key: string;
  filePath: string;
  contentType?: string;
}) {
  const stat = await fs.promises.stat(input.filePath);

  if (!stat.isFile() || stat.size > config.security.maxOutputBytes) {
    throw new Error("Output file exceeds allowed size");
  }

  await acquireUploadSlot();

  try {
    await getClient().send(
      new PutObjectCommand({
        Bucket: input.bucket,
        Key: input.key,
        Body: fs.createReadStream(input.filePath),
        ContentLength: stat.size,
        ContentType: input.contentType ?? contentTypeFromKey(input.key),
      }),
    );

    return {
      bucket: input.bucket,
      key: input.key,
      sizeBytes: stat.size,
      contentType: input.contentType ?? contentTypeFromKey(input.key),
    };
  } finally {
    await releaseUploadSlot();
  }
}

/**
 * uploadJsonToR2 uploads a JSON payload to R2 as a file. It takes care of stringifying the payload and setting the appropriate content type. This is useful for storing metadata, logs, or any structured data related to file processing jobs.
 * The function ensures that the JSON is properly formatted and calculates the content length for efficient uploading. It also returns the bucket, key, and size of the uploaded JSON for reference.
 */
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
      ContentType: "application/json",
    }),
  );

  return {
    bucket: input.bucket,
    key: input.key,
    sizeBytes: Buffer.byteLength(body),
  };
}

/**
 * Backward-compatible local upload.
 */
export async function upload(filePath: string, key: string): Promise<string> {
  if (config.mode === "local") {
    const outputDir = path.resolve("./outputs");
    const dest = path.resolve(outputDir, key);

    if (!dest.startsWith(`${outputDir}${path.sep}`) && dest !== outputDir) {
      throw new Error("Invalid output key");
    }

    await fs.promises.mkdir(path.dirname(dest), { recursive: true });
    await fs.promises.copyFile(filePath, dest);
    return dest;
  }

  const bucket = process.env.R2_BUCKET_NAME;

  if (!bucket) {
    throw new Error("R2_BUCKET_NAME is required.");
  }

  await uploadToR2({ bucket, key, filePath });
  return key;
}

export async function download(inputUrl: string, dest: string): Promise<void> {
  await fs.promises.mkdir(path.dirname(dest), { recursive: true });

  if (inputUrl.startsWith("file://")) {
    if (process.env.ALLOW_LOCAL_FILE_INPUTS !== "true") {
      throw new Error("Local file inputs are disabled. Use R2 bucket/key input.");
    }

    const sourcePath = decodeURIComponent(new URL(inputUrl).pathname);
    await fs.promises.copyFile(sourcePath, dest);
    return;
  }

  if (!/^https?:\/\//i.test(inputUrl)) {
    throw new Error("Unsupported input URL protocol");
  }

  if (process.env.ALLOW_REMOTE_INPUT_URLS !== "true") {
    throw new Error("Remote input URLs are disabled. Use R2 bucket/key input.");
  }

  const response = await fetch(inputUrl);

  if (!response.ok || !response.body) {
    throw new Error(`Download failed: ${response.status}`);
  }

  const contentLength = Number(response.headers.get("content-length") ?? "0");

  if (contentLength > config.security.maxUploadBytes) {
    throw new Error("Download exceeds maximum upload size");
  }

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

          if (downloadedBytes > config.security.maxUploadBytes) {
            file.destroy();
            reject(new Error("Download exceeds maximum upload size"));
            return;
          }

          if (!file.write(Buffer.from(value))) {
            await new Promise((resume) => file.once("drain", resume));
          }
        }
      } catch (error) {
        reject(error);
      }
    }

    file.on("error", reject);
    void pump();
  });
}