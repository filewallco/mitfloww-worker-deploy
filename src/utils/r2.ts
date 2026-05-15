import fs from 'fs';
import dns from 'dns';
import https from 'https';
import net from 'net';
import path from 'path';
import { config } from '../config';
import { connection } from '../queue/connection';

function isPrivateAddress(address: string): boolean {
  const normalized = address.replace(/^::ffff:/, '');

  if (net.isIPv4(normalized)) {
    const [a, b, c] = normalized.split('.').map(Number);

    return (
      a === 0 ||
      a === 10 ||
      a === 127 ||
      a >= 224 ||
      (a === 100 && b >= 64 && b <= 127) ||
      (a === 169 && b === 254) ||
      (a === 172 && b >= 16 && b <= 31) ||
      (a === 192 && b === 168) ||
      (a === 192 && b === 0 && c === 0) ||
      (a === 192 && b === 0 && c === 2) ||
      (a === 198 && (b === 18 || b === 19)) ||
      (a === 198 && b === 51 && c === 100) ||
      (a === 203 && b === 0 && c === 113)
    );
  }

  if (net.isIPv6(address)) {
    const lower = address.toLowerCase();
    return (
      lower === '::' ||
      lower === '::1' ||
      lower.startsWith('fc') ||
      lower.startsWith('fd') ||
      /^fe[89ab]/.test(lower) ||
      lower.startsWith('ff') ||
      lower.startsWith('2001:db8')
    );
  }

  return true;
}

function parseDownloadUrl(rawUrl: string): URL {
  const parsed = new URL(rawUrl);
  const hostname = parsed.hostname.toLowerCase();

  if (parsed.protocol !== 'https:') {
    throw new Error('Only HTTPS downloads are allowed');
  }

  if (parsed.username || parsed.password) {
    throw new Error('Download URL credentials are not allowed');
  }

  if (
    hostname === 'localhost' ||
    hostname.endsWith('.localhost') ||
    (net.isIP(hostname) !== 0 && isPrivateAddress(hostname))
  ) {
    throw new Error('Blocked private download host');
  }

  return parsed;
}

function guardedLookup(hostname: string, options: any, callback?: any) {
  const lookupOptions = typeof options === 'function' ? {} : options;
  const done = typeof options === 'function' ? options : callback;

  dns.lookup(hostname, { ...lookupOptions, all: true } as dns.LookupAllOptions, (err, addresses) => {
    if (err) return done(err);

    const allowed = addresses.filter((address: dns.LookupAddress) => !isPrivateAddress(address.address));

    if (allowed.length !== addresses.length || allowed.length === 0) {
      return done(new Error('Blocked private download address'));
    }

    if (lookupOptions.all) return done(null, allowed);

    return done(null, allowed[0].address, allowed[0].family);
  });
}

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
    const stat = await fs.promises.stat(localPath);

    if (stat.size > config.security.maxUploadBytes) {
      throw new Error('Local input exceeds maximum upload size');
    }

    await fs.promises.copyFile(localPath, dest);
    return;
  }

  const downloadUrl = parseDownloadUrl(url);

  // Download file over HTTPS
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);
    let req: ReturnType<typeof https.get> | null = null;
    let settled = false;
    let downloadedBytes = 0;

    function fail(err: Error) {
      if (settled) return;
      settled = true;
      req?.destroy();
      file.destroy();
      void fs.promises.rm(dest, { force: true });
      reject(err);
    }

    function succeed(value: boolean) {
      if (settled) return;
      settled = true;
      resolve(value);
    }

    req = https.get(downloadUrl, { lookup: guardedLookup }, (res) => {
      if (res.statusCode !== 200) {
        res.resume();
        return fail(new Error(`HTTP ${res.statusCode}`));
      }

      const contentLength = Number(res.headers['content-length'] || 0);

      if (contentLength > config.security.maxUploadBytes) {
        res.resume();
        return fail(new Error('Download exceeds maximum upload size'));
      }

      res.on('data', (chunk: Buffer) => {
        downloadedBytes += chunk.length;

        if (downloadedBytes > config.security.maxUploadBytes) {
          fail(new Error('Download exceeds maximum upload size'));
        }
      });

      res.pipe(file);

      file.on('finish', () => {
        file.close(() => succeed(true));
      });
    });

    // Timeout after 30 seconds
    req.setTimeout(30000, () => {
      fail(new Error('Download timeout'));
    });

    req.on('error', fail);
    file.on('error', fail);
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
  const stat = await fs.promises.stat(filePath);

  if (!stat.isFile() || stat.size > config.security.maxOutputBytes) {
    throw new Error('Output file exceeds allowed size');
  }

  await acquireUploadSlot();
  try {
    if (config.mode === 'local') {
      const outputDir = path.resolve('./outputs');

      // Ensure output directory exists
      if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir);
      }

      const dest = path.resolve(outputDir, key);

      if (!dest.startsWith(`${outputDir}${path.sep}`) && dest !== outputDir) {
        throw new Error('Invalid output key');
      }

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
