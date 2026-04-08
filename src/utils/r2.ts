import fs from 'fs';
import https from 'https';
import path from 'path';
import { config } from '../config';
import { connection } from '../queue/connection';

/**
 * Downloads a file from a URL or local file path.
 * Handles both local file copying and HTTPS downloads.
 *
 * @param url - File URL or local path (prefixed with file://)
 * @param dest - Destination path to save the file
 */
export async function download(url: string, dest: string) {
  if (config.mode === 'local' && url.startsWith('file://')) {
    // For local testing, just copy the file from local path
    const localPath = url.replace('file://', '');
    await fs.promises.copyFile(localPath, dest);
    return;
  }

  // Download file over HTTPS
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);

    const req = https.get(url, (res) => {
      if (res.statusCode !== 200) {
        return reject(new Error(`HTTP ${res.statusCode}`));
      }

      res.pipe(file);

      file.on('finish', () => {
        file.close();
        resolve(true);
      });
    });

    // Timeout after 30 seconds
    req.setTimeout(30000, () => {
      req.destroy();
      reject(new Error('Download timeout'));
    });

    req.on('error', reject);
  });
}

/**
 * Distributed upload limiter using Redis.
 * Prevents multi-worker overload and ensures global fairness.
 */
const UPLOAD_KEY = 'global:upload_slots';
const MAX_UPLOADS = Number(process.env.MAX_PARALLEL_UPLOADS || 6);

/**
 * Atomic upload slot acquisition (Redis Lua)
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

/**
 * Acquire upload slot (blocking with retry)
 */
async function acquireUploadSlot(): Promise<void> {
  while (true) {
    const ok = await connection.eval(
      ACQUIRE_UPLOAD_LUA,
      1,
      UPLOAD_KEY,
      MAX_UPLOADS
    );

    if (ok === 1) return;

    await new Promise(r => setTimeout(r, 50));
  }
}

/**
 * Release upload slot
 */
async function releaseUploadSlot(): Promise<void> {
  try {
    const val = await connection.decr(UPLOAD_KEY);
    if (val <= 0) await connection.del(UPLOAD_KEY);
  } catch {
    // never throw in cleanup
  }
}

/**
 * Uploads a file either to local storage or a remote service (R2 placeholder here).
 * Returns the path or URL of the uploaded file.
 *
 * @param filePath - Path to the local file to upload
 * @param key - Unique key or filename for the destination
 * @returns Path or URL of the uploaded file
 */
export async function upload(filePath: string, key: string): Promise<string> {
  await acquireUploadSlot();
  try {
    if (config.mode === 'local') {
      const outputDir = './outputs';

      // Ensure output directory exists
      if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir);
      }

      const dest = path.join(outputDir, key);

      // Ensure destination directory structure exists
      await fs.promises.mkdir(path.dirname(dest), { recursive: true });

      // Copy the file
      await fs.promises.copyFile(filePath, dest);

      console.log(`Saved locally: ${dest}`);
      return dest; // Return local path
    }

    // Placeholder for real R2 upload
    const url = `https://your-r2-domain/${key}`;
    console.log(`Uploading ${filePath} to ${url}`);
    return url; // Return URL
  } finally {
    await releaseUploadSlot();
  }
}